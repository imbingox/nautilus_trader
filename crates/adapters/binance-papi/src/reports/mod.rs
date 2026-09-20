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

//! Scoped report collection; REST observations never imply atomic or verified historical coverage.

pub(crate) mod history;
pub(crate) mod models;
pub(crate) mod parse;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use nautilus_binance::common::{
    enums::{BinanceAlgoStatus, BinanceOrderStatus},
    symbol::format_binance_symbol,
};
use nautilus_core::time::AtomicTime;
use nautilus_model::{
    enums::PositionSide,
    identifiers::{AccountId, ClientOrderId, InstrumentId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::Quantity,
};
use rust_decimal::Decimal;
use serde::{Serialize, de::DeserializeOwned};
use tokio_util::sync::CancellationToken;

use self::{
    history::{HistoryWindow, PapiConsistencyError, PapiCoverageError, PapiSchemaError},
    models::{AlgoRow, OrderRow, PositionMode, PositionRow, TradeRow},
    parse::{
        OrderFamily, ReportContext, algo_child_id, algo_report, commission, decode_order_id,
        fill_report, order_report, position_report,
    },
};
use crate::{
    consts::{BINANCE_PAPI_CLIENT_ID, BINANCE_PAPI_VENUE},
    http::{
        BinancePapiResponseMetadata, PapiHttpClient, RawResponse, RequestBudget,
        error::PapiHttpError,
        query::{HistoryEndpoint, PapiRequest},
    },
    observations::{fields::Field, models::JsonObject},
    read_only::BinancePapiReadOnlySnapshot,
};

#[derive(Debug)]
pub(crate) struct InstrumentScope {
    instruments: BTreeMap<String, InstrumentAny>,
}

impl InstrumentScope {
    pub(crate) fn new(instruments: Vec<InstrumentAny>) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !instruments.is_empty() && instruments.len() <= 256,
            "PAPI requires 1 to 256 explicitly scoped instruments"
        );
        let mut result = BTreeMap::new();

        for instrument in instruments {
            let symbol = format_binance_symbol(&instrument.id());
            parse::validate_symbol(&symbol)?;
            anyhow::ensure!(
                instrument.id().venue == *BINANCE_PAPI_VENUE
                    && matches!(
                        instrument,
                        InstrumentAny::CryptoPerpetual(_) | InstrumentAny::CryptoFuture(_)
                    )
                    && !instrument.is_inverse()
                    && instrument.settlement_currency() == instrument.quote_currency()
                    && instrument.raw_symbol().as_str() == symbol,
                "PAPI requires Binance linear UM futures metadata"
            );
            anyhow::ensure!(
                result.insert(symbol, instrument).is_none(),
                "Duplicate PAPI instrument scope"
            );
        }

        Ok(Self {
            instruments: result,
        })
    }

    pub(crate) fn symbols(
        &self,
        instrument_id: Option<InstrumentId>,
    ) -> anyhow::Result<Vec<String>> {
        if let Some(id) = instrument_id {
            let symbol = format_binance_symbol(&id);
            anyhow::ensure!(
                self.instrument(&symbol)?.id() == id,
                "Instrument is outside PAPI scope"
            );
            Ok(vec![symbol])
        } else {
            Ok(self.instruments.keys().cloned().collect())
        }
    }

    pub(crate) fn instrument(&self, symbol: &str) -> anyhow::Result<&InstrumentAny> {
        self.instruments
            .get(symbol)
            .ok_or_else(|| anyhow::anyhow!("PAPI response has no in-scope instrument metadata"))
    }

    pub(crate) fn instrument_ids(&self) -> Vec<InstrumentId> {
        self.instruments.values().map(Instrument::id).collect()
    }
}

pub(crate) struct ReportCollector<'a> {
    pub(crate) http: &'a PapiHttpClient,
    pub(crate) scope: &'a InstrumentScope,
    pub(crate) account_id: AccountId,
    pub(crate) clock: &'a AtomicTime,
    pub(crate) cancel: &'a CancellationToken,
    pub(crate) budget: RequestBudget,
    pub(crate) responses: Vec<BinancePapiResponseMetadata>,
}

impl ReportCollector<'_> {
    pub(crate) async fn mass_status(
        mut self,
        window: HistoryWindow,
    ) -> anyhow::Result<BinancePapiReadOnlySnapshot> {
        self.ensure_mode().await?;
        let symbols = self.scope.symbols(None)?;
        let positions = self.positions_inner(&symbols).await?;
        let mut books = BTreeMap::new();

        // Required current sources succeed before any optional historical leg is attempted
        for symbol in &symbols {
            books.insert(symbol.clone(), self.current_orders(symbol).await?);
        }

        let mut issues = vec![
            "Historical retention, time selection and algo linkage await authenticated validation"
                .to_owned(),
        ];

        for (symbol, book) in &mut books {
            match self
                .history::<OrderRow>(HistoryEndpoint::Orders, symbol, window)
                .await
            {
                Ok(rows) => {
                    for row in rows {
                        insert_consistent(&mut book.orders, row.order_id, row)?;
                    }
                }
                Err(e) => record_history_failure(&mut issues, symbol, HistoryEndpoint::Orders, e)?,
            }

            match self
                .history::<AlgoRow>(HistoryEndpoint::Algos, symbol, window)
                .await
            {
                Ok(rows) => {
                    for row in rows {
                        insert_algo(&mut book.algos, row)?;
                    }
                }
                Err(e) => record_history_failure(&mut issues, symbol, HistoryEndpoint::Algos, e)?,
            }

            match self
                .history::<TradeRow>(HistoryEndpoint::Trades, symbol, window)
                .await
            {
                Ok(rows) => {
                    for row in rows {
                        // Millisecond query bounds enclose the exact requested nanosecond window
                        if window.contains(row.time.nanoseconds) {
                            insert_consistent(&mut book.trades, row.id, row)?;
                        }
                    }
                }
                Err(e) => record_history_failure(&mut issues, symbol, HistoryEndpoint::Trades, e)?,
            }
        }

        let mut orders = Vec::new();
        let mut fills = Vec::new();

        for (symbol, mut book) in books {
            self.resolve_children(&symbol, &mut book).await?;

            for trade in book.trades.values() {
                commission(trade)?;

                if !book.orders.contains_key(&trade.order_id) {
                    let row = self
                        .lookup_ordinary(&symbol, Some(trade.order_id), None)
                        .await?;
                    book.orders.insert(row.order_id, row);
                }
            }

            let (symbol_orders, symbol_fills) = self.build_reports(&symbol, &book, Some(window))?;
            orders.extend(symbol_orders);
            fills.extend(symbol_fills);
        }

        validate_client_identities(&orders)?;
        let mut mass_status = ExecutionMassStatus::new(
            *BINANCE_PAPI_CLIENT_ID,
            self.account_id,
            *BINANCE_PAPI_VENUE,
            self.clock.get_time_ns(),
            None,
        );
        // Exhausted pages alone cannot prove the venue's retention/selection contract
        mass_status.set_report_window(Some(window.start), false);
        mass_status.add_order_reports(orders);
        mass_status.add_fill_reports(fills);
        mass_status.add_position_reports(positions);

        let snapshot = BinancePapiReadOnlySnapshot {
            mass_status,
            window_end: window.end,
            instrument_ids: self.scope.instrument_ids(),
            issues,
            responses: self.responses,
        };
        self.budget.check()?;
        Ok(snapshot)
    }

    pub(crate) async fn open_orders(
        mut self,
        instrument_id: Option<InstrumentId>,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        let symbols = self.scope.symbols(instrument_id)?;
        self.ensure_mode().await?;
        let mut reports = Vec::new();

        for symbol in symbols {
            let mut book = self.current_orders(&symbol).await?;
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
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let symbols = self.scope.symbols(instrument_id)?;
        self.ensure_mode().await?;
        let result = self.positions_inner(&symbols).await?;
        self.budget.check()?;
        Ok(result)
    }

    pub(crate) async fn single_order(
        &mut self,
        instrument_id: InstrumentId,
        venue_id: Option<VenueOrderId>,
        client_id: Option<ClientOrderId>,
    ) -> anyhow::Result<OrderStatusReport> {
        let symbols = self.scope.symbols(Some(instrument_id))?;
        let symbol = &symbols[0];

        if let Some(client_id) = client_id {
            parse::client_order_id(client_id.as_str())?;
        }

        anyhow::ensure!(
            venue_id.is_some() || client_id.is_some(),
            "PAPI order query requires an identity"
        );
        let identity = venue_id.map(|id| decode_order_id(id, symbol)).transpose()?;
        self.ensure_mode().await?;

        let report = match identity {
            Some((OrderFamily::Algo, id)) => {
                let row = self.lookup_algo(symbol, id).await?;
                let child = if let Some(id) = algo_child_id(&row)? {
                    Some(self.lookup_ordinary(symbol, Some(id), None).await?)
                } else {
                    None
                };
                algo_report(&row, child.as_ref(), self.context(symbol)?)?
            }
            other => {
                let row = self
                    .lookup_ordinary(
                        symbol,
                        other.map(|(_, id)| id),
                        client_id.map(|id| id.to_string()),
                    )
                    .await?;
                order_report(&row, self.context(symbol)?, false)?
            }
        };

        anyhow::ensure!(
            venue_id.is_none_or(|id| report.venue_order_id == id)
                && client_id.is_none_or(|id| report.client_order_id == Some(id)),
            PapiConsistencyError
        );
        self.budget.check()?;
        Ok(report)
    }

    async fn ensure_mode(&mut self) -> anyhow::Result<()> {
        let response = self.raw(&PapiRequest::PositionMode).await?;
        let JsonObject(mode): JsonObject<PositionMode> =
            serde_json::from_str(response.body.get()).map_err(|_| PapiSchemaError)?;
        anyhow::ensure!(
            !mode.dual_side_position,
            "PAPI hedge mode is unsupported; one-way mode is required"
        );
        Ok(())
    }

    async fn positions_inner(
        &mut self,
        symbols: &[String],
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        let mut reports = Vec::with_capacity(symbols.len());

        for symbol in symbols {
            let response = self
                .raw(&PapiRequest::Positions {
                    symbol: symbol.clone(),
                })
                .await?;
            let rows: Vec<JsonObject<PositionRow>> =
                serde_json::from_str(response.body.get()).map_err(|_| PapiSchemaError)?;
            self.budget.charge_rows(rows.len())?;
            anyhow::ensure!(
                rows.len() <= 1 && rows.first().is_none_or(|row| row.0.symbol == *symbol),
                "PAPI position response does not uniquely cover the requested symbol"
            );

            if let Some(JsonObject(row)) = rows.first() {
                reports.push(position_report(row, self.context(symbol)?)?);
            } else {
                let context = self.context(symbol)?;
                reports.push(PositionStatusReport::new(
                    context.account_id,
                    context.instrument.id(),
                    PositionSide::Flat,
                    Quantity::zero(context.instrument.size_precision()),
                    response.metadata.ts_received,
                    context.ts_init,
                    None,
                    None,
                    None,
                ));
            }
        }

        Ok(reports)
    }

    async fn current_orders(&mut self, symbol: &str) -> anyhow::Result<SymbolReports> {
        let mut book = SymbolReports::default();
        let orders: Vec<OrderRow> = self
            .rows(&PapiRequest::OpenOrders {
                symbol: symbol.to_owned(),
            })
            .await?;

        for row in orders {
            anyhow::ensure!(
                row.symbol == symbol
                    && matches!(
                        row.status,
                        BinanceOrderStatus::New
                            | BinanceOrderStatus::PendingNew
                            | BinanceOrderStatus::PartiallyFilled
                            | BinanceOrderStatus::PendingCancel
                    )
                    && book.open_orders.insert(row.order_id),
                PapiConsistencyError
            );
            insert_consistent(&mut book.orders, row.order_id, row)?;
        }

        let algos: Vec<AlgoRow> = self
            .rows(&PapiRequest::OpenAlgos {
                symbol: symbol.to_owned(),
            })
            .await?;

        for row in algos {
            anyhow::ensure!(
                row.symbol == symbol
                    && matches!(
                        row.algo_status,
                        BinanceAlgoStatus::New
                            | BinanceAlgoStatus::Triggering
                            | BinanceAlgoStatus::Triggered
                    )
                    && book.open_algos.insert(row.algo_id),
                PapiConsistencyError
            );
            insert_algo(&mut book.algos, row)?;
        }

        Ok(book)
    }

    async fn resolve_children(
        &mut self,
        symbol: &str,
        book: &mut SymbolReports,
    ) -> anyhow::Result<()> {
        let ids: Vec<_> = book.algos.keys().copied().collect();

        for id in ids {
            let row = &book.algos[&id];

            if matches!(
                row.algo_status,
                BinanceAlgoStatus::Triggered | BinanceAlgoStatus::Finished
            ) && algo_child_id(row)?.is_none()
            {
                let full = self.lookup_algo(symbol, id).await?;
                insert_algo(&mut book.algos, full)?;
            }

            if let Some(child_id) = algo_child_id(&book.algos[&id])? {
                anyhow::ensure!(
                    book.children.insert(child_id, id).is_none(),
                    PapiConsistencyError
                );

                // A targeted child lookup supplies its current state even when history already has it
                let child = self.lookup_ordinary(symbol, Some(child_id), None).await?;
                insert_consistent(&mut book.orders, child.order_id, child)?;
            }
        }

        Ok(())
    }

    async fn lookup_ordinary(
        &mut self,
        symbol: &str,
        order_id: Option<i64>,
        client_order_id: Option<String>,
    ) -> anyhow::Result<OrderRow> {
        let response = self
            .raw(&PapiRequest::Order {
                symbol: symbol.to_owned(),
                order_id,
                client_order_id: client_order_id.clone(),
            })
            .await?;

        let JsonObject(row): JsonObject<OrderRow> =
            serde_json::from_str(response.body.get()).map_err(|_| PapiSchemaError)?;
        self.budget.charge_rows(1)?;
        anyhow::ensure!(
            row.symbol == symbol
                && order_id.is_none_or(|id| row.order_id == id)
                && client_order_id.is_none_or(|id| row.client_order_id == id),
            PapiConsistencyError
        );
        Ok(row)
    }

    async fn lookup_algo(&mut self, symbol: &str, algo_id: i64) -> anyhow::Result<AlgoRow> {
        let mut rows: Vec<AlgoRow> = self
            .rows(&PapiRequest::Algo {
                symbol: symbol.to_owned(),
                algo_id,
            })
            .await?;
        anyhow::ensure!(
            rows.len() == 1 && rows[0].symbol == symbol && rows[0].algo_id == algo_id,
            "PAPI algo lookup did not resolve the exact requested identity"
        );
        Ok(rows.remove(0))
    }

    fn build_reports(
        &self,
        symbol: &str,
        book: &SymbolReports,
        window: Option<HistoryWindow>,
    ) -> anyhow::Result<(Vec<OrderStatusReport>, Vec<FillReport>)> {
        let ctx = self.context(symbol)?;
        let mut orders = BTreeMap::new();
        let mut child_orders = BTreeMap::new();
        let mut clients = BTreeSet::new();

        for row in book.algos.values() {
            let child = algo_child_id(row)?.and_then(|id| book.orders.get(&id));
            let report = algo_report(row, child, ctx)?;

            if let Some(child) = child {
                child_orders.insert(child.order_id, report.venue_order_id);
            }

            anyhow::ensure!(clients.insert(report.client_order_id), PapiConsistencyError);
            orders.insert(report.venue_order_id, report);
        }

        for row in book.orders.values() {
            if book.children.contains_key(&row.order_id) {
                continue;
            }

            let report = order_report(row, ctx, false)?;
            anyhow::ensure!(clients.insert(report.client_order_id), PapiConsistencyError);
            orders.insert(report.venue_order_id, report);
        }

        let mut fills = Vec::new();
        let mut quantities = BTreeMap::new();
        let mut linked_orders = BTreeSet::new();

        for row in book.trades.values() {
            let venue_id =
                child_orders
                    .get(&row.order_id)
                    .copied()
                    .unwrap_or(parse::venue_order_id(
                        symbol,
                        OrderFamily::Ordinary,
                        row.order_id,
                    )?);
            let order = orders
                .get(&venue_id)
                .ok_or_else(|| anyhow::anyhow!("PAPI fill has no linked order report"))?;
            let source_order = book
                .orders
                .get(&row.order_id)
                .ok_or_else(|| anyhow::anyhow!("PAPI fill has no source order"))?;
            anyhow::ensure!(
                row.time.nanoseconds >= source_order.time.nanoseconds
                    && row.time.nanoseconds <= source_order.update_time.nanoseconds,
                "PAPI fill is outside its source order's execution interval"
            );
            let fill = fill_report(row, ctx, order)?;
            let total = quantities.entry(venue_id).or_insert(Decimal::ZERO);
            *total = total
                .checked_add(fill.last_qty.as_decimal())
                .ok_or_else(|| anyhow::anyhow!("PAPI fill quantity overflow"))?;
            anyhow::ensure!(
                *total <= order.filled_qty.as_decimal(),
                "PAPI fills exceed the linked order's executed quantity"
            );
            linked_orders.insert(venue_id);
            fills.push(fill);
        }

        let orders = orders
            .into_values()
            .filter(|row| {
                row.order_status.is_open()
                    || row.order_status.is_inflight()
                    || linked_orders.contains(&row.venue_order_id)
                    || window.is_none_or(|window| window.contains(row.ts_last))
            })
            .collect();

        Ok((orders, fills))
    }

    fn context(&self, symbol: &str) -> anyhow::Result<ReportContext<'_>> {
        Ok(ReportContext {
            account_id: self.account_id,
            instrument: self.scope.instrument(symbol)?,
            ts_init: self.clock.get_time_ns(),
        })
    }

    pub(crate) async fn raw(&mut self, request: &PapiRequest) -> anyhow::Result<RawResponse> {
        let response = self.http.get(request, &self.budget, self.cancel).await?;
        self.responses.push(response.metadata.clone());
        Ok(response)
    }

    async fn rows<T: DeserializeOwned>(&mut self, request: &PapiRequest) -> anyhow::Result<Vec<T>> {
        let response = self.raw(request).await?;
        let rows: Vec<JsonObject<T>> =
            serde_json::from_str(response.body.get()).map_err(|_| PapiSchemaError)?;
        self.budget.charge_rows(rows.len())?;
        Ok(rows.into_iter().map(|row| row.0).collect())
    }
}

#[derive(Default)]
struct SymbolReports {
    orders: BTreeMap<i64, OrderRow>,
    algos: BTreeMap<i64, AlgoRow>,
    trades: BTreeMap<i64, TradeRow>,
    open_orders: BTreeSet<i64>,
    open_algos: BTreeSet<i64>,
    children: BTreeMap<i64, i64>,
}

fn validate_client_identities(reports: &[OrderStatusReport]) -> anyhow::Result<()> {
    let mut clients = BTreeSet::new();

    for report in reports {
        anyhow::ensure!(clients.insert(report.client_order_id), PapiConsistencyError);
    }

    Ok(())
}

fn insert_consistent<T: Serialize>(
    map: &mut BTreeMap<i64, T>,
    id: i64,
    row: T,
) -> anyhow::Result<()> {
    anyhow::ensure!(id > 0, PapiConsistencyError);

    if let Some(previous) = map.get(&id) {
        anyhow::ensure!(
            serde_json::to_string(previous)? == serde_json::to_string(&row)?,
            PapiConsistencyError
        );
    } else {
        map.insert(id, row);
    }

    Ok(())
}

fn insert_algo(map: &mut BTreeMap<i64, AlgoRow>, row: AlgoRow) -> anyhow::Result<()> {
    anyhow::ensure!(row.algo_id > 0, PapiConsistencyError);

    if let Some(previous) = map.get(&row.algo_id) {
        let previous_id = algo_child_id(previous)?;
        let next_id = algo_child_id(&row)?;
        anyhow::ensure!(
            previous_id.is_none() || next_id.is_none() || previous_id == next_id,
            PapiConsistencyError
        );
        let mut previous_terms = previous.clone();
        let mut next_terms = row.clone();
        previous_terms.actual_order_id = Field::Missing;
        next_terms.actual_order_id = Field::Missing;
        anyhow::ensure!(
            serde_json::to_string(&previous_terms)? == serde_json::to_string(&next_terms)?,
            PapiConsistencyError
        );

        if previous_id.is_some() && next_id.is_none() {
            return Ok(());
        }
    }

    map.insert(row.algo_id, row);
    Ok(())
}

fn record_history_failure(
    issues: &mut Vec<String>,
    symbol: &str,
    endpoint: HistoryEndpoint,
    e: anyhow::Error,
) -> anyhow::Result<()> {
    let partial = e.is::<PapiCoverageError>()
        || e.downcast_ref::<PapiHttpError>().is_some_and(|e| {
            matches!(
                e,
                PapiHttpError::Server(_)
                    | PapiHttpError::Timeout
                    | PapiHttpError::Sdk
                    | PapiHttpError::Rejected { .. }
            )
        });

    if !partial {
        return Err(e);
    }

    issues.push(format!("{} for {symbol}: {e}", endpoint.path()));
    Ok(())
}
