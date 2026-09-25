// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Account-wide current reports with public USD-M instrument discovery.

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
};

use anyhow::Context;
use nautilus_binance::{
    common::{
        enums::{BinanceAlgoStatus, BinanceOrderStatus, BinancePositionSide},
        parse::parse_usdm_instrument,
        symbol::format_binance_symbol,
    },
    futures::http::models::BinanceFuturesUsdExchangeInfo,
};
use nautilus_model::{
    identifiers::InstrumentId,
    reports::{OrderStatusReport, PositionStatusReport},
};
use parking_lot::Mutex;

use super::{
    InstrumentScope, ReportCollector, SymbolReports,
    history::{PapiConsistencyError, PapiSchemaError},
    insert_algo, insert_consistent,
    models::{AlgoRow, OrderRow, PositionRow},
    parse::{position_report, validate_symbol},
    validate_client_identities,
};
use crate::{
    consts::BINANCE_PAPI_VENUE,
    http::query::{PapiPublicRequest, PapiRequest},
    observations::ObservationSource,
};

impl ReportCollector<'_> {
    pub(crate) async fn open_orders(
        mut self,
        instrument_id: Option<InstrumentId>,
        instruments: &Mutex<InstrumentScope>,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        if let Some(id) = instrument_id {
            self.prepare_instrument(id, instruments).await?;
            return self.scoped_open_orders(Some(id)).await;
        }

        self.ensure_mode().await?;
        let orders: Vec<OrderRow> = self
            .rows(&PapiRequest::Observation(ObservationSource::UmOpenOrders))
            .await?;
        let algos: Vec<AlgoRow> = self
            .rows(&PapiRequest::Observation(ObservationSource::UmOpenAlgos))
            .await?;
        let mut books: BTreeMap<String, SymbolReports> = BTreeMap::new();

        for row in orders {
            validate_symbol(&row.symbol)?;
            let book = books.entry(row.symbol.clone()).or_default();
            anyhow::ensure!(
                matches!(
                    row.status,
                    BinanceOrderStatus::New
                        | BinanceOrderStatus::PendingNew
                        | BinanceOrderStatus::PartiallyFilled
                        | BinanceOrderStatus::PendingCancel
                ) && book.open_orders.insert(row.order_id),
                PapiConsistencyError
            );
            insert_consistent(&mut book.orders, row.order_id, row)?;
        }

        for row in algos {
            validate_symbol(&row.symbol)?;
            let book = books.entry(row.symbol.clone()).or_default();
            anyhow::ensure!(
                matches!(
                    row.algo_status,
                    BinanceAlgoStatus::New
                        | BinanceAlgoStatus::Triggering
                        | BinanceAlgoStatus::Triggered
                ) && book.open_algos.insert(row.algo_id),
                PapiConsistencyError
            );
            insert_algo(&mut book.algos, row)?;
        }

        self.prepare_instruments(&books.keys().cloned().collect(), instruments)
            .await?;
        let mut reports = Vec::new();

        for (symbol, mut book) in books {
            self.resolve_children(&symbol, &mut book).await?;
            let (orders, _) = self.build_reports(&symbol, &book, None)?;
            reports.extend(
                orders
                    .into_iter()
                    .filter(|row| row.order_status.is_open() || row.order_status.is_inflight()),
            );
        }

        validate_client_identities(&reports)?;
        self.budget.check()?;
        Ok(reports)
    }

    pub(crate) async fn positions(
        mut self,
        instrument_id: Option<InstrumentId>,
        instruments: &Mutex<InstrumentScope>,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        if let Some(id) = instrument_id {
            self.prepare_instrument(id, instruments).await?;
            return self.scoped_positions(Some(id)).await;
        }

        self.ensure_mode().await?;
        let rows: Vec<PositionRow> = self.rows(&PapiRequest::Positions { symbol: None }).await?;
        let mut identities = BTreeSet::new();
        let mut positions = Vec::new();

        for row in rows {
            validate_symbol(&row.symbol)?;
            anyhow::ensure!(
                row.position_side == BinancePositionSide::Both
                    && identities.insert(row.symbol.clone()),
                "PAPI account position response has duplicate symbols or unsupported position sides"
            );

            if !row.position_amt.value().is_zero() {
                positions.push(row);
            }
        }

        self.prepare_instruments(
            &positions.iter().map(|row| row.symbol.clone()).collect(),
            instruments,
        )
        .await?;
        let reports = positions
            .iter()
            .map(|row| position_report(row, self.context(&row.symbol)?))
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.budget.check()?;
        Ok(reports)
    }

    async fn prepare_instrument(
        &mut self,
        id: InstrumentId,
        instruments: &Mutex<InstrumentScope>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            id.venue == *BINANCE_PAPI_VENUE,
            "PAPI instrument must use the BINANCE venue"
        );
        let symbol = format_binance_symbol(&id);
        validate_symbol(&symbol)?;
        self.prepare_instruments(&BTreeSet::from([symbol]), instruments)
            .await?;
        self.scope.symbols(Some(id))?;
        Ok(())
    }

    async fn prepare_instruments(
        &mut self,
        symbols: &BTreeSet<String>,
        instruments: &Mutex<InstrumentScope>,
    ) -> anyhow::Result<()> {
        self.scope = Cow::Owned(instruments.lock().clone());

        if symbols
            .iter()
            .all(|symbol| self.scope.instruments.contains_key(symbol))
        {
            return Ok(());
        }

        let response = self
            .http
            .get_public(&PapiPublicRequest::ExchangeInfo, &self.budget, self.cancel)
            .await?;
        self.responses.push(response.metadata.clone());
        let info: BinanceFuturesUsdExchangeInfo =
            serde_json::from_str(response.body.get()).map_err(|_| PapiSchemaError)?;
        self.budget.charge_rows(info.symbols.len())?;
        let mut discovered = BTreeSet::new();
        let mut loaded = Vec::new();

        for definition in info.symbols {
            let symbol = definition.symbol.as_str();
            anyhow::ensure!(
                discovered.insert(symbol.to_owned()),
                "Duplicate USD-M instrument metadata"
            );

            if !symbols.contains(symbol) || self.scope.instruments.contains_key(symbol) {
                continue;
            }

            let instrument = parse_usdm_instrument(
                &definition,
                response.metadata.ts_received,
                self.clock.get_time_ns(),
            )
            .with_context(|| format!("Cannot resolve PAPI instrument metadata for {symbol}"))?;
            loaded.push(instrument);
        }

        let loaded = InstrumentScope::new(loaded)?;
        self.scope.to_mut().instruments.extend(loaded.instruments);

        for symbol in symbols {
            anyhow::ensure!(
                self.scope.instruments.contains_key(symbol),
                "Unresolved PAPI instrument metadata for {symbol}"
            );
        }

        self.budget.check()?;
        instruments
            .lock()
            .instruments
            .extend(self.scope.instruments.clone());
        Ok(())
    }
}
