// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Authenticated, unit-checked admission evidence for ordinary UM trading.

use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use nautilus_binance::common::enums::{BinanceFuturesOrderType, BinanceOrderStatus, BinanceSide};
use nautilus_model::{
    enums::PositionSide,
    events::AccountState,
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
    reports::{OrderStatusReport, PositionStatusReport},
    types::{Currency, Quantity},
};
use rust_decimal::Decimal;
use serde::Deserialize;

use super::{BinancePapiProjectionStatus, BinancePapiReadOnlyClient};
use crate::{
    config::BinancePapiTradingConfig,
    http::query::{PapiPublicRequest, PapiRequest},
    observations::{ObservationData, ObservationSource, fields::Amount, models::JsonObject},
    reports::{
        models::{OrderRow, PositionMode, PositionRow},
        parse::{ReportContext, client_order_id, ensure_one_way, order_report, position_report},
    },
    trading::{
        coordinator::{
            PapiAccountStatusEvidence, PapiMarginRuleEvidence, PapiPositionModeEvidence,
            PapiRiskUnitEvidence, PapiVerifiedInstrumentRisk, PapiVerifiedInstrumentRules,
            PapiVerifiedOpenOrder, PapiVerifiedRiskSnapshot,
        },
        journal::PapiIntentSide,
    },
};

const BASIS_POINTS: u32 = 10_000;
const ACCOUNT_ENDPOINT: &str = "/papi/v1/account";
const POSITION_MODE_ENDPOINT: &str = "/papi/v1/um/positionSide/dual";
const LEVERAGE_ENDPOINT: &str = "/papi/v1/um/leverageBracket";
const MARK_PRICE_ENDPOINT: &str = "/fapi/v1/premiumIndex";
const EXCHANGE_INFO_ENDPOINT: &str = "/fapi/v1/exchangeInfo";
const RISK_UNIT_SOURCE: &str = "/papi/v1/account + /sapi/v1/portfolio/asset-index-price";

pub(crate) struct PapiVerifiedRiskRebaseline {
    pub(crate) snapshot: PapiVerifiedRiskSnapshot,
    pub(crate) account_state: AccountState,
    pub(crate) order_reports: Vec<OrderStatusReport>,
    pub(crate) position_reports: Vec<PositionStatusReport>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarkPrice {
    symbol: String,
    mark_price: Amount,
    time: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssetIndex {
    asset: String,
    asset_index_price: Amount,
    time: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExchangeInfo {
    server_time: i64,
    symbols: Vec<ExchangeSymbol>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExchangeSymbol {
    symbol: String,
    status: String,
    margin_asset: String,
    filters: Vec<ExchangeFilter>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExchangeFilter {
    filter_type: String,
    tick_size: Option<Amount>,
    step_size: Option<Amount>,
    min_price: Option<Amount>,
    max_price: Option<Amount>,
    min_qty: Option<Amount>,
    max_qty: Option<Amount>,
    notional: Option<Amount>,
    min_notional: Option<Amount>,
    max_notional: Option<Amount>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LeverageRow {
    symbol: String,
    brackets: Vec<LeverageBracket>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LeverageBracket {
    bracket: i64,
    initial_leverage: i64,
    notional_floor: Decimal,
    notional_cap: Decimal,
}

impl BinancePapiReadOnlyClient {
    pub(crate) async fn collect_verified_risk_snapshot(
        &self,
        config: &BinancePapiTradingConfig,
    ) -> anyhow::Result<PapiVerifiedRiskSnapshot> {
        Ok(self
            .collect_verified_risk_rebaseline(config)
            .await?
            .snapshot)
    }

    pub(crate) async fn collect_verified_risk_rebaseline(
        &self,
        config: &BinancePapiTradingConfig,
    ) -> anyhow::Result<PapiVerifiedRiskRebaseline> {
        let started = Instant::now();
        self.refresh_account_observations().await?;
        let max_age = std::time::Duration::from_millis(config.max_risk_age_ms);
        let max_span = std::time::Duration::from_millis(config.max_risk_collection_span_ms);
        let projection = self.account_projection(max_age, max_span)?;
        anyhow::ensure!(
            projection.wallet_status == BinancePapiProjectionStatus::Available
                && projection.risk_status == BinancePapiProjectionStatus::Available
                && projection.wallet_issues.is_empty()
                && projection.risk_issues.is_empty(),
            "PAPI account observations are not valid for trading admission"
        );
        let account_state = projection
            .account_state
            .ok_or_else(|| anyhow::anyhow!("PAPI wallet projection produced no account state"))?;
        let (generation, summary, um_account) = {
            let observations = self.inner.observations.lock();
            let account = observations
                .sources
                .iter()
                .find(|stored| stored.slot.source() == &ObservationSource::Account)
                .and_then(|stored| stored.slot.last_response())
                .ok_or_else(|| anyhow::anyhow!("PAPI account risk observation is unavailable"))?;
            let um_account = observations
                .sources
                .iter()
                .find(|stored| stored.slot.source() == &ObservationSource::UmAccountV2)
                .and_then(|stored| stored.slot.last_response())
                .ok_or_else(|| anyhow::anyhow!("PAPI UM account observation is unavailable"))?;
            anyhow::ensure!(
                account.generation == observations.generation
                    && um_account.generation == observations.generation,
                "PAPI risk observations do not share the current generation"
            );
            let ObservationData::Account(summary) = &account.data else {
                anyhow::bail!("PAPI account risk source contained unexpected data")
            };
            let ObservationData::UmAccount(um_account) = &um_account.data else {
                anyhow::bail!("PAPI UM account source contained unexpected data")
            };
            (
                observations.generation,
                (**summary).clone(),
                um_account.clone(),
            )
        };

        anyhow::ensure!(
            summary.account_status.require("accountStatus")? == "NORMAL",
            "PAPI account status does not permit increase-risk trading"
        );
        let available_usd = summary
            .total_available_balance
            .require("totalAvailableBalance")?
            .value();
        let initial_margin_usd = summary
            .account_initial_margin
            .require("accountInitialMargin")?
            .value();
        anyhow::ensure!(
            available_usd >= Decimal::ZERO && initial_margin_usd >= Decimal::ZERO,
            "PAPI account margin values are outside supported bounds"
        );

        let budget = self.budget()?;
        let mode: JsonObject<PositionMode> = serde_json::from_str(
            self.inner
                .http
                .get(&PapiRequest::PositionMode, &budget, &self.inner.cancel)
                .await?
                .body
                .get(),
        )?;
        anyhow::ensure!(
            !mode.0.dual_side_position,
            "PAPI hedge mode is unsupported; one-way mode is required"
        );

        let exchange_info: ExchangeInfo = serde_json::from_str(
            self.inner
                .http
                .get_public(
                    &PapiPublicRequest::ExchangeInfo,
                    &budget,
                    &self.inner.cancel,
                )
                .await?
                .body
                .get(),
        )?;
        anyhow::ensure!(exchange_info.server_time > 0, "Invalid FAPI exchange time");
        let asset_code = config.risk_currency.code.as_str();
        let indexes: Vec<JsonObject<AssetIndex>> = serde_json::from_str(
            self.inner
                .http
                .get_public(
                    &PapiPublicRequest::AssetIndex {
                        asset: asset_code.to_owned(),
                    },
                    &budget,
                    &self.inner.cancel,
                )
                .await?
                .body
                .get(),
        )?;
        anyhow::ensure!(
            indexes.len() == 1
                && indexes[0].0.asset == asset_code
                && indexes[0].0.time > 0
                && indexes[0].0.asset_index_price.value() > Decimal::ZERO,
            "PAPI risk-currency asset index is missing or invalid"
        );
        let conversion = indexes[0].0.asset_index_price.value().max(Decimal::ONE);
        let available_initial_margin = available_usd
            .checked_div(conversion)
            .ok_or_else(|| anyhow::anyhow!("PAPI available-margin conversion overflow"))?;

        let allowed: HashSet<_> = config
            .instrument_limits
            .iter()
            .map(|limits| limits.instrument_id)
            .collect();
        self.ensure_no_unallowlisted_orders(&allowed)?;

        let mut instruments = HashMap::new();
        let mut open_orders = Vec::new();
        let mut order_reports = Vec::new();
        let mut position_reports = Vec::with_capacity(config.instrument_limits.len());
        let mut max_initial_margin_rate = Decimal::ZERO;
        let mut account_exposure = Decimal::ZERO;
        let ts_init = self.now();

        for limits in &config.instrument_limits {
            let instrument = self
                .inner
                .scope
                .instrument_ids()
                .into_iter()
                .find(|id| *id == limits.instrument_id)
                .and_then(|id| {
                    let symbol = nautilus_binance::common::symbol::format_binance_symbol(&id);
                    self.inner.scope.instrument(&symbol).ok()
                })
                .ok_or_else(|| {
                    anyhow::anyhow!("PAPI trading instrument is outside report scope")
                })?;
            let symbol = instrument.raw_symbol().as_str().to_owned();
            anyhow::ensure!(
                instrument.settlement_currency() == config.risk_currency,
                "PAPI instrument settlement currency does not match the risk currency"
            );

            let mark: JsonObject<MarkPrice> = serde_json::from_str(
                self.inner
                    .http
                    .get_public(
                        &PapiPublicRequest::MarkPrice {
                            symbol: symbol.clone(),
                        },
                        &budget,
                        &self.inner.cancel,
                    )
                    .await?
                    .body
                    .get(),
            )?;
            anyhow::ensure!(
                mark.0.symbol == symbol
                    && mark.0.time > 0
                    && mark.0.mark_price.value() > Decimal::ZERO,
                "FAPI mark-price response does not cover the requested symbol"
            );
            let price_observed_at = Instant::now();
            let reference_price = mark.0.mark_price.value();

            let (signed_position_quantity, position_report) = self
                .target_position(&symbol, &um_account, &budget, instrument, ts_init)
                .await?;
            position_reports.push(position_report);
            let exposure = signed_position_quantity
                .abs()
                .checked_mul(reference_price)
                .ok_or_else(|| anyhow::anyhow!("PAPI position exposure overflow"))?;
            account_exposure = account_exposure
                .checked_add(exposure)
                .ok_or_else(|| anyhow::anyhow!("PAPI account exposure overflow"))?;

            let rules = verified_rules(
                limits.instrument_id,
                config.risk_currency,
                generation,
                &symbol,
                &exchange_info,
            )?;
            let leverage_rate = self.max_initial_margin_rate(&symbol, &budget).await?;
            max_initial_margin_rate = max_initial_margin_rate.max(leverage_rate);
            let (instrument_orders, instrument_reports) = self
                .target_open_orders(
                    &symbol,
                    reference_price,
                    config,
                    &budget,
                    instrument,
                    ts_init,
                )
                .await?;
            open_orders.extend(instrument_orders);
            order_reports.extend(instrument_reports);
            instruments.insert(
                limits.instrument_id,
                PapiVerifiedInstrumentRisk {
                    signed_position_quantity,
                    exposure,
                    reference_price,
                    price_source: MARK_PRICE_ENDPOINT,
                    price_generation: generation,
                    price_observed_at,
                    rules,
                },
            );
        }

        budget.check()?;
        anyhow::ensure!(
            self.inner.observations.lock().generation == generation,
            "PAPI account observations changed during risk collection"
        );
        let observed_at = Instant::now();
        Ok(PapiVerifiedRiskRebaseline {
            snapshot: PapiVerifiedRiskSnapshot {
                account_id: self.inner.account_id,
                generation,
                observed_at,
                collection_span: observed_at.duration_since(started),
                status: PapiAccountStatusEvidence::Normal {
                    endpoint: ACCOUNT_ENDPOINT,
                    generation,
                },
                position_mode: PapiPositionModeEvidence::OneWay {
                    endpoint: POSITION_MODE_ENDPOINT,
                    generation,
                },
                units: PapiRiskUnitEvidence {
                    currency: config.risk_currency,
                    source: RISK_UNIT_SOURCE,
                },
                margin_rule: PapiMarginRuleEvidence {
                    source: LEVERAGE_ENDPOINT,
                    generation,
                    max_initial_margin_rate,
                },
                available_initial_margin,
                account_exposure,
                instruments,
                open_orders,
            },
            account_state,
            order_reports,
            position_reports,
        })
    }

    fn ensure_no_unallowlisted_orders(
        &self,
        allowed: &HashSet<InstrumentId>,
    ) -> anyhow::Result<()> {
        let observations = self.inner.observations.lock();

        for source in [
            ObservationSource::UmOpenOrders,
            ObservationSource::UmOpenAlgos,
        ] {
            let rows = observations
                .sources
                .iter()
                .find(|stored| stored.slot.source() == &source)
                .and_then(|stored| stored.slot.last_response())
                .and_then(|observation| match &observation.data {
                    ObservationData::ScopeRows(rows) => Some(rows),
                    _ => None,
                })
                .ok_or_else(|| anyhow::anyhow!("PAPI open-order observation is unavailable"))?;

            for row in rows {
                let symbol = row.symbol.require("symbol")?;
                let instrument = self.inner.scope.instrument(symbol)?;
                anyhow::ensure!(
                    allowed.contains(&instrument.id()),
                    "PAPI account has UM orders outside the trading allowlist"
                );
            }
            anyhow::ensure!(
                source != ObservationSource::UmOpenAlgos || rows.is_empty(),
                "PAPI account has unsupported open algo orders"
            );
        }
        Ok(())
    }

    async fn target_position(
        &self,
        symbol: &str,
        um_account: &crate::observations::models::UmAccount,
        budget: &crate::http::RequestBudget,
        instrument: &InstrumentAny,
        ts_init: nautilus_core::UnixNanos,
    ) -> anyhow::Result<(Decimal, PositionStatusReport)> {
        let response = self
            .inner
            .http
            .get(
                &PapiRequest::Positions {
                    symbol: Some(symbol.to_owned()),
                },
                budget,
                &self.inner.cancel,
            )
            .await?;
        let rows: Vec<JsonObject<PositionRow>> = serde_json::from_str(response.body.get())?;
        anyhow::ensure!(rows.len() <= 1, "PAPI position response is ambiguous");
        let account_rows: Vec<_> = um_account
            .positions
            .iter()
            .filter(|position| position.symbol == symbol)
            .collect();
        anyhow::ensure!(
            account_rows.len() <= 1,
            "PAPI UM account contains duplicate position identities"
        );
        let account_quantity = account_rows
            .first()
            .map(|position| {
                position
                    .position_amt
                    .require("positionAmt")
                    .map(Amount::value)
            })
            .transpose()?;

        let context = ReportContext {
            account_id: self.inner.account_id,
            instrument,
            ts_init,
        };
        let (quantity, report) = match rows.first() {
            Some(JsonObject(row)) => {
                anyhow::ensure!(row.symbol == symbol, "PAPI position symbol does not match");
                ensure_one_way(row.position_side)?;
                (row.position_amt.value(), position_report(row, context)?)
            }
            None => {
                anyhow::ensure!(
                    account_quantity.is_none_or(|quantity| quantity.is_zero()),
                    "PAPI scoped position response omitted an account position"
                );
                (
                    Decimal::ZERO,
                    PositionStatusReport::new(
                        self.inner.account_id,
                        instrument.id(),
                        PositionSide::Flat,
                        Quantity::zero(instrument.size_precision()),
                        response.metadata.ts_received,
                        ts_init,
                        None,
                        None,
                        None,
                    ),
                )
            }
        };
        anyhow::ensure!(
            account_quantity.is_none_or(|account| account == quantity),
            "PAPI scoped and account position quantities disagree"
        );
        Ok((quantity, report))
    }

    async fn max_initial_margin_rate(
        &self,
        symbol: &str,
        budget: &crate::http::RequestBudget,
    ) -> anyhow::Result<Decimal> {
        let response = self
            .inner
            .http
            .get(
                &PapiRequest::LeverageBrackets {
                    symbol: symbol.to_owned(),
                },
                budget,
                &self.inner.cancel,
            )
            .await?;
        let rows: Vec<JsonObject<LeverageRow>> = serde_json::from_str(response.body.get())?;
        anyhow::ensure!(
            rows.len() == 1 && rows[0].0.symbol == symbol && !rows[0].0.brackets.is_empty(),
            "PAPI leverage brackets do not cover the requested symbol"
        );
        let mut previous_cap = Decimal::ZERO;
        let mut maximum = Decimal::ZERO;

        for (index, bracket) in rows[0].0.brackets.iter().enumerate() {
            anyhow::ensure!(
                bracket.bracket == i64::try_from(index + 1)?
                    && bracket.initial_leverage > 0
                    && bracket.notional_floor == previous_cap
                    && bracket.notional_cap > bracket.notional_floor,
                "PAPI leverage brackets are incomplete or invalid"
            );
            previous_cap = bracket.notional_cap;
            let rate = Decimal::ONE
                .checked_div(Decimal::from(bracket.initial_leverage))
                .ok_or_else(|| anyhow::anyhow!("PAPI leverage conversion overflow"))?;
            maximum = maximum.max(rate);
        }
        Ok(maximum)
    }

    async fn target_open_orders(
        &self,
        symbol: &str,
        reference_price: Decimal,
        config: &BinancePapiTradingConfig,
        budget: &crate::http::RequestBudget,
        instrument: &InstrumentAny,
        ts_init: nautilus_core::UnixNanos,
    ) -> anyhow::Result<(Vec<PapiVerifiedOpenOrder>, Vec<OrderStatusReport>)> {
        let response = self
            .inner
            .http
            .get(
                &PapiRequest::OpenOrders {
                    symbol: symbol.to_owned(),
                },
                budget,
                &self.inner.cancel,
            )
            .await?;
        let rows: Vec<JsonObject<OrderRow>> = serde_json::from_str(response.body.get())?;
        let algos = self
            .inner
            .http
            .get(
                &PapiRequest::OpenAlgos {
                    symbol: symbol.to_owned(),
                },
                budget,
                &self.inner.cancel,
            )
            .await?;
        let algo_rows: Vec<serde_json::Value> = serde_json::from_str(algos.body.get())?;
        anyhow::ensure!(
            algo_rows.is_empty(),
            "PAPI symbol has unsupported open algo orders"
        );
        let market_buffer = rate_amount(reference_price, config.market_order_price_buffer_bps)?;
        let buffered_reference = reference_price
            .checked_add(market_buffer)
            .ok_or_else(|| anyhow::anyhow!("PAPI buffered price overflow"))?;
        let mut result = Vec::with_capacity(rows.len());
        let mut reports = Vec::with_capacity(rows.len());
        let context = ReportContext {
            account_id: self.inner.account_id,
            instrument,
            ts_init,
        };

        for JsonObject(row) in rows {
            anyhow::ensure!(
                row.symbol == symbol
                    && row.order_id > 0
                    && matches!(
                        row.status,
                        BinanceOrderStatus::New
                            | BinanceOrderStatus::PendingNew
                            | BinanceOrderStatus::PartiallyFilled
                            | BinanceOrderStatus::PendingCancel
                    )
                    && row.order_type == BinanceFuturesOrderType::Limit,
                "PAPI open order is outside the supported admission scope"
            );
            ensure_one_way(row.position_side)?;
            let quantity = row.orig_qty.value();
            anyhow::ensure!(quantity > Decimal::ZERO, "Invalid PAPI open-order quantity");
            let price = row.price.value().max(buffered_reference);
            let notional = quantity
                .checked_mul(price)
                .ok_or_else(|| anyhow::anyhow!("PAPI open-order notional overflow"))?;
            let worst_case_exposure = if row.reduce_only {
                Decimal::ZERO
            } else {
                notional
                    .checked_add(rate_amount(notional, config.fee_buffer_bps)?)
                    .ok_or_else(|| anyhow::anyhow!("PAPI open-order exposure overflow"))?
            };
            result.push(PapiVerifiedOpenOrder {
                instrument_id: instrument.id(),
                client_order_id: client_order_id(&row.client_order_id)?,
                venue_order_id: row.order_id,
                side: match row.side {
                    BinanceSide::Buy => PapiIntentSide::Buy,
                    BinanceSide::Sell => PapiIntentSide::Sell,
                },
                quantity,
                reduce_only: row.reduce_only,
                worst_case_exposure,
            });
            reports.push(order_report(&row, context, false)?);
        }
        Ok((result, reports))
    }
}

fn verified_rules(
    instrument_id: InstrumentId,
    risk_currency: Currency,
    generation: u64,
    symbol: &str,
    exchange_info: &ExchangeInfo,
) -> anyhow::Result<PapiVerifiedInstrumentRules> {
    let rows: Vec<_> = exchange_info
        .symbols
        .iter()
        .filter(|row| row.symbol == symbol)
        .collect();
    anyhow::ensure!(
        rows.len() == 1,
        "FAPI exchange info does not uniquely cover the symbol"
    );
    let row = rows[0];
    anyhow::ensure!(
        row.margin_asset == risk_currency.code.as_str(),
        "FAPI margin asset does not match the configured risk currency"
    );
    let price = filter(&row.filters, "PRICE_FILTER")?;
    let lot = filter(&row.filters, "LOT_SIZE")?;
    let market_lot = filter(&row.filters, "MARKET_LOT_SIZE")?;
    let price_increment = positive(price.tick_size.as_ref(), "tickSize")?;
    let quantity_increment = positive(lot.step_size.as_ref(), "stepSize")?;
    anyhow::ensure!(
        positive(market_lot.step_size.as_ref(), "market stepSize")? == quantity_increment,
        "FAPI limit and market quantity increments differ"
    );
    let min_quantity = maximum_optional(
        positive_optional(lot.min_qty.as_ref()),
        positive_optional(market_lot.min_qty.as_ref()),
    );
    let max_quantity = minimum_optional(
        positive_optional(lot.max_qty.as_ref()),
        positive_optional(market_lot.max_qty.as_ref()),
    );
    let min_notional_filter = row
        .filters
        .iter()
        .find(|filter| filter.filter_type == "MIN_NOTIONAL");
    let notional_filter = row
        .filters
        .iter()
        .find(|filter| filter.filter_type == "NOTIONAL");
    let min_notional = maximum_optional(
        min_notional_filter
            .and_then(|filter| filter.notional.as_ref())
            .map(Amount::value)
            .filter(|value| *value > Decimal::ZERO),
        notional_filter
            .and_then(|filter| filter.min_notional.as_ref())
            .map(Amount::value)
            .filter(|value| *value > Decimal::ZERO),
    );
    let max_notional = notional_filter
        .and_then(|filter| filter.max_notional.as_ref())
        .map(Amount::value)
        .filter(|value| *value > Decimal::ZERO);
    let rules = PapiVerifiedInstrumentRules {
        source: EXCHANGE_INFO_ENDPOINT,
        generation,
        settlement_currency: risk_currency,
        trading: row.status == "TRADING",
        price_increment,
        quantity_increment,
        min_price: positive_optional(price.min_price.as_ref()),
        max_price: positive_optional(price.max_price.as_ref()),
        min_quantity,
        max_quantity,
        min_notional,
        max_notional,
    };
    anyhow::ensure!(
        rules
            .min_quantity
            .is_none_or(|minimum| { rules.max_quantity.is_none_or(|maximum| minimum <= maximum) }),
        "FAPI quantity bounds are inconsistent for {instrument_id}"
    );
    Ok(rules)
}

fn filter<'a>(filters: &'a [ExchangeFilter], name: &str) -> anyhow::Result<&'a ExchangeFilter> {
    let rows: Vec<_> = filters
        .iter()
        .filter(|filter| filter.filter_type == name)
        .collect();
    anyhow::ensure!(
        rows.len() == 1,
        "FAPI symbol filter {name} is missing or duplicated"
    );
    Ok(rows[0])
}

fn positive(value: Option<&Amount>, name: &str) -> anyhow::Result<Decimal> {
    let value = value
        .ok_or_else(|| anyhow::anyhow!("FAPI symbol filter is missing {name}"))?
        .value();
    anyhow::ensure!(
        value > Decimal::ZERO,
        "FAPI symbol filter {name} is invalid"
    );
    Ok(value)
}

fn positive_optional(value: Option<&Amount>) -> Option<Decimal> {
    value
        .map(Amount::value)
        .filter(|value| *value > Decimal::ZERO)
}

fn maximum_optional(left: Option<Decimal>, right: Option<Decimal>) -> Option<Decimal> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    }
}

fn minimum_optional(left: Option<Decimal>, right: Option<Decimal>) -> Option<Decimal> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    }
}

fn rate_amount(value: Decimal, basis_points: u32) -> anyhow::Result<Decimal> {
    value
        .checked_mul(Decimal::from(basis_points))
        .and_then(|value| value.checked_div(Decimal::from(BASIS_POINTS)))
        .ok_or_else(|| anyhow::anyhow!("PAPI rate calculation overflow"))
}

#[cfg(test)]
mod tests {
    use nautilus_model::{identifiers::InstrumentId, types::Currency};
    use rstest::rstest;
    use rust_decimal_macros::dec;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        config::{BinancePapiInstrumentTradingConfig, BinancePapiTradingConfig},
        testing::{self, MockServer},
    };

    fn trading_config(path: std::path::PathBuf) -> BinancePapiTradingConfig {
        BinancePapiTradingConfig {
            command_journal_path: path,
            risk_currency: Currency::USDT(),
            instrument_limits: vec![BinancePapiInstrumentTradingConfig {
                instrument_id: InstrumentId::from("BTCUSDT-PERP.BINANCE"),
                max_order_quantity: dec!(0.001),
                max_order_notional: dec!(200),
                max_position_quantity: dec!(0.001),
                max_instrument_exposure: dec!(200),
            }],
            max_account_exposure: dec!(200),
            max_in_flight_operations: 4,
            max_risk_age_ms: 30_000,
            max_risk_collection_span_ms: 30_000,
            max_recovery_requests: 32,
            max_recovery_rounds: 3,
            recovery_recheck_interval_ms: 10,
            market_order_price_buffer_bps: 100,
            fee_buffer_bps: 10,
        }
    }

    #[rstest]
    #[tokio::test]
    async fn current_sources_build_conservative_verified_risk_snapshot() {
        let server = MockServer::new(testing::supported_trading).await;
        let client = testing::client(&server, &["BTCUSDT", "ETHUSDT"]);
        let directory = TempDir::new().unwrap();
        let config = trading_config(directory.path().join("commands.journal"));

        let rebaseline = client
            .collect_verified_risk_rebaseline(&config)
            .await
            .unwrap();
        let snapshot = &rebaseline.snapshot;

        assert_eq!(snapshot.generation, 1);
        assert_eq!(snapshot.available_initial_margin, dec!(1000));
        assert_eq!(snapshot.account_exposure, Decimal::ZERO);
        assert!(snapshot.open_orders.is_empty());
        assert_eq!(snapshot.margin_rule.max_initial_margin_rate, dec!(0.1));
        let risk = &snapshot.instruments[&InstrumentId::from("BTCUSDT-PERP.BINANCE")];
        assert_eq!(risk.signed_position_quantity, Decimal::ZERO);
        assert_eq!(risk.reference_price, dec!(100000));
        assert_eq!(risk.rules.min_notional, Some(dec!(5)));
        assert!(!rebaseline.account_state.total_only_balances.is_empty());
        assert!(rebaseline.order_reports.is_empty());
        assert_eq!(rebaseline.position_reports.len(), 1);
        assert_eq!(
            rebaseline.position_reports[0].instrument_id,
            InstrumentId::from("BTCUSDT-PERP.BINANCE")
        );
        assert!(server.requests().iter().all(|request| !matches!(
            request.path.as_str(),
            "/papi/v1/um/allOrders" | "/papi/v1/um/algo/allAlgoOrders" | "/papi/v1/um/userTrades"
        )));
        let asset_index = server
            .requests()
            .into_iter()
            .find(|request| request.path == "/sapi/v1/portfolio/asset-index-price")
            .unwrap();
        assert_eq!(asset_index.api_key.as_deref(), Some(testing::API_KEY));
        assert!(!asset_index.params.contains_key("signature"));
    }
}
