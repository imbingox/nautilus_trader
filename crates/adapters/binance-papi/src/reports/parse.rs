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

use nautilus_binance::common::enums::{
    BinanceAlgoStatus, BinanceAlgoType, BinanceFuturesOrderType, BinanceOrderStatus,
    BinancePositionSide, BinanceSide, BinanceTimeInForce, BinanceWorkingType,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{LiquiditySide, OrderStatus, OrderType, PositionSide, TimeInForce, TriggerType},
    identifiers::{AccountId, ClientOrderId, TradeId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;
use thiserror::Error;

use super::models::{AlgoRow, OrderRow, PositionRow, TradeRow};
use crate::observations::fields::{Field, VenueTime};

#[derive(Clone, Copy, Debug)]
pub(crate) struct ReportContext<'a> {
    pub(crate) account_id: AccountId,
    pub(crate) instrument: &'a InstrumentAny,
    pub(crate) ts_init: UnixNanos,
}

pub(crate) fn order_report(
    row: &OrderRow,
    ctx: ReportContext<'_>,
    is_algo_child: bool,
) -> anyhow::Result<OrderStatusReport> {
    validate_context(&row.symbol, ctx)?;
    ensure_one_way(row.position_side)?;
    validate_times(row.time, row.update_time)?;
    let client_order_id = client_order_id(&row.client_order_id)?;

    anyhow::ensure!(
        matches!(
            row.order_type,
            BinanceFuturesOrderType::Limit | BinanceFuturesOrderType::Market
        ),
        "Unsupported PAPI ordinary order type"
    );
    anyhow::ensure!(
        is_algo_child || row.orig_type == row.order_type,
        "PAPI conditional child requires its algo parent"
    );

    let quantity = parse_quantity(row.orig_qty.value(), ctx.instrument)?;
    let filled_qty = parse_quantity(row.executed_qty.value(), ctx.instrument)?;
    anyhow::ensure!(
        quantity.is_positive() && filled_qty <= quantity,
        "Invalid PAPI order quantities"
    );

    let order_status = match row.status {
        BinanceOrderStatus::New
        | BinanceOrderStatus::PartiallyFilled
        | BinanceOrderStatus::Filled
        | BinanceOrderStatus::Canceled
        | BinanceOrderStatus::Rejected
        | BinanceOrderStatus::Expired
        | BinanceOrderStatus::ExpiredInMatch
        | BinanceOrderStatus::PendingCancel => row.status.to_nautilus_order_status(false)?,
        BinanceOrderStatus::PendingNew => OrderStatus::Submitted,
        _ => anyhow::bail!("Unsupported PAPI order status"),
    };
    validate_filled_status(order_status, quantity, filled_qty)?;
    let (time_in_force, post_only) = time_in_force(row.time_in_force)?;
    let order_type = OrderType::try_from(row.order_type)?;
    anyhow::ensure!(
        !post_only || order_type == OrderType::Limit,
        "PAPI post-only order is not a limit"
    );

    let mut report = OrderStatusReport::new(
        ctx.account_id,
        ctx.instrument.id(),
        Some(client_order_id),
        venue_order_id(&row.symbol, OrderFamily::Ordinary, row.order_id)?,
        Some(row.side.into()),
        order_type,
        time_in_force,
        order_status,
        quantity,
        filled_qty,
        row.time.nanoseconds,
        row.update_time.nanoseconds,
        ctx.ts_init,
        None,
    );
    report.reduce_only = row.reduce_only;
    report.post_only = post_only;
    report.expire_time = expiry(row.time_in_force, &row.good_till_date, row.time.nanoseconds)?;

    if order_type == OrderType::Limit {
        report.price = Some(parse_price(row.price.value(), ctx.instrument)?);
    } else {
        anyhow::ensure!(
            row.price.value() == Decimal::ZERO,
            "Unexpected PAPI market order price"
        );
    }

    if filled_qty.is_positive() {
        anyhow::ensure!(
            row.avg_price.value() > Decimal::ZERO,
            "PAPI filled order has no average price"
        );
        report.avg_px = Some(row.avg_price.value());
    } else {
        anyhow::ensure!(
            row.avg_price.value() == Decimal::ZERO,
            "PAPI unfilled order has an execution average"
        );
    }

    Ok(report)
}

pub(crate) fn algo_report(
    row: &AlgoRow,
    child: Option<&OrderRow>,
    ctx: ReportContext<'_>,
) -> anyhow::Result<OrderStatusReport> {
    validate_context(&row.symbol, ctx)?;
    ensure_one_way(row.position_side)?;
    validate_times(row.create_time, row.update_time)?;
    anyhow::ensure!(
        row.algo_type == BinanceAlgoType::Conditional,
        "Unsupported PAPI algo type"
    );
    anyhow::ensure!(
        !row.close_position,
        "PAPI closePosition algo quantity mapping is unverified"
    );
    anyhow::ensure!(
        matches!(
            row.order_type,
            BinanceFuturesOrderType::Stop
                | BinanceFuturesOrderType::StopMarket
                | BinanceFuturesOrderType::TakeProfit
                | BinanceFuturesOrderType::TakeProfitMarket
        ),
        "Unsupported PAPI conditional order type"
    );
    let quantity = parse_quantity(row.quantity.value(), ctx.instrument)?;
    anyhow::ensure!(
        quantity.is_positive(),
        "PAPI algo quantity must be positive"
    );
    let client_order_id = client_order_id(&row.client_algo_id)?;
    let (time_in_force, post_only) = time_in_force(row.time_in_force)?;
    let order_type = OrderType::try_from(row.order_type)?;
    let is_limit = matches!(order_type, OrderType::StopLimit | OrderType::LimitIfTouched);
    anyhow::ensure!(!post_only || is_limit, "PAPI post-only algo is not a limit");
    let child_id = algo_child_id(row)?;

    let (order_status, filled_qty, avg_px, ts_last) = if let Some(child) = child {
        anyhow::ensure!(
            child_id == Some(child.order_id)
                && child.symbol == row.symbol
                && child.side == row.side
                && child.reduce_only == row.reduce_only
                && child.time_in_force == row.time_in_force
                && (!is_limit || child.price.value() == row.price.value())
                && (child.orig_type == row.order_type || child.orig_type == child.order_type)
                && matches!(
                    row.algo_status,
                    BinanceAlgoStatus::Triggered | BinanceAlgoStatus::Finished
                ),
            "Inconsistent PAPI algo child identity or execution terms"
        );
        let child_report = order_report(child, ctx, true)?;
        let trigger = row.trigger_time.require("triggerTime")?.nanoseconds;
        anyhow::ensure!(
            trigger > UnixNanos::default()
                && trigger >= row.create_time.nanoseconds
                && trigger <= row.update_time.nanoseconds
                && child.time.nanoseconds >= trigger
                && child_report.quantity == quantity
                && child_report.expire_time
                    == expiry(
                        row.time_in_force,
                        &row.good_till_date,
                        row.create_time.nanoseconds
                    )?
                && ((is_limit && child_report.order_type == OrderType::Limit)
                    || (!is_limit && child_report.order_type == OrderType::Market)),
            "Inconsistent PAPI algo trigger or child quantity"
        );
        anyhow::ensure!(
            row.algo_status != BinanceAlgoStatus::Finished || child_report.order_status.is_closed(),
            "Finished PAPI algo has an active child"
        );

        (
            child_report.order_status,
            child_report.filled_qty,
            child_report.avg_px,
            child_report.ts_last.max(row.update_time.nanoseconds),
        )
    } else {
        anyhow::ensure!(child_id.is_none(), "PAPI algo child order was not queried");
        let status = match row.algo_status {
            BinanceAlgoStatus::New | BinanceAlgoStatus::Triggering => OrderStatus::Accepted,
            BinanceAlgoStatus::Canceled => OrderStatus::Canceled,
            BinanceAlgoStatus::Expired => OrderStatus::Expired,
            BinanceAlgoStatus::Rejected => OrderStatus::Rejected,
            BinanceAlgoStatus::Triggered | BinanceAlgoStatus::Finished => {
                anyhow::bail!("Triggered PAPI algo requires its actual child order");
            }
            BinanceAlgoStatus::Unknown => anyhow::bail!("Unsupported PAPI algo status"),
        };

        (
            status,
            Quantity::zero(ctx.instrument.size_precision()),
            None,
            row.update_time.nanoseconds,
        )
    };

    let mut report = OrderStatusReport::new(
        ctx.account_id,
        ctx.instrument.id(),
        Some(client_order_id),
        venue_order_id(&row.symbol, OrderFamily::Algo, row.algo_id)?,
        Some(row.side.into()),
        order_type,
        time_in_force,
        order_status,
        quantity,
        filled_qty,
        row.create_time.nanoseconds,
        ts_last,
        ctx.ts_init,
        None,
    );
    report.reduce_only = row.reduce_only;
    report.post_only = post_only;
    report.avg_px = avg_px;
    report.expire_time = expiry(
        row.time_in_force,
        &row.good_till_date,
        row.create_time.nanoseconds,
    )?;
    report.trigger_price = Some(parse_price(row.trigger_price.value(), ctx.instrument)?);
    report.trigger_type = Some(match row.working_type {
        BinanceWorkingType::ContractPrice => TriggerType::LastPrice,
        BinanceWorkingType::MarkPrice => TriggerType::MarkPrice,
        BinanceWorkingType::Unknown => anyhow::bail!("Unsupported PAPI trigger source"),
    });

    if is_limit {
        report.price = Some(parse_price(row.price.value(), ctx.instrument)?);
    } else {
        anyhow::ensure!(
            row.price.value() == Decimal::ZERO,
            "Unexpected PAPI market algo price"
        );
    }

    match &row.trigger_time {
        Field::Invalid(_) => anyhow::bail!("Invalid PAPI trigger time"),
        Field::Value(time) if time.nanoseconds > UnixNanos::default() => {
            anyhow::ensure!(
                row.algo_status != BinanceAlgoStatus::New
                    && time.nanoseconds >= row.create_time.nanoseconds
                    && time.nanoseconds <= row.update_time.nanoseconds,
                "Invalid PAPI trigger time"
            );
            report.ts_triggered = Some(time.nanoseconds);
        }
        _ => {}
    }

    Ok(report)
}

pub(crate) fn fill_report(
    row: &TradeRow,
    ctx: ReportContext<'_>,
    order: &OrderStatusReport,
) -> anyhow::Result<FillReport> {
    // Commission is checked before other fields so a required fee cannot become incomplete history
    let commission = commission(row)?;
    validate_context(&row.symbol, ctx)?;
    ensure_one_way(row.position_side)?;
    anyhow::ensure!(
        row.id > 0 && row.order_id > 0 && row.time.nanoseconds > UnixNanos::default(),
        "Invalid PAPI trade identity or timestamp"
    );
    anyhow::ensure!(
        row.buyer == (row.side == BinanceSide::Buy)
            && order.account_id == ctx.account_id
            && order.instrument_id == ctx.instrument.id()
            && order.order_side == Some(row.side.into())
            && row.time.nanoseconds >= order.ts_accepted
            && order
                .ts_triggered
                .is_none_or(|trigger| row.time.nanoseconds >= trigger)
            && row.time.nanoseconds <= order.ts_last,
        "Inconsistent PAPI trade and order evidence"
    );
    let last_qty = parse_quantity(row.qty.value(), ctx.instrument)?;
    anyhow::ensure!(
        last_qty.is_positive(),
        "PAPI fill quantity must be positive"
    );

    Ok(FillReport::new(
        ctx.account_id,
        ctx.instrument.id(),
        order.venue_order_id,
        TradeId::new_checked(row.id.to_string())?,
        row.side.into(),
        last_qty,
        parse_price(row.price.value(), ctx.instrument)?,
        commission,
        if row.maker {
            LiquiditySide::Maker
        } else {
            LiquiditySide::Taker
        },
        order.client_order_id,
        None,
        row.time.nanoseconds,
        ctx.ts_init,
        None,
    ))
}

pub(crate) fn position_report(
    row: &PositionRow,
    ctx: ReportContext<'_>,
) -> anyhow::Result<PositionStatusReport> {
    validate_context(&row.symbol, ctx)?;
    ensure_one_way(row.position_side)?;
    let amount = row.position_amt.value();
    let side = if amount > Decimal::ZERO {
        PositionSide::Long
    } else if amount < Decimal::ZERO {
        PositionSide::Short
    } else {
        PositionSide::Flat
    };
    let avg_px_open = if amount == Decimal::ZERO {
        anyhow::ensure!(
            row.entry_price.value() == Decimal::ZERO,
            "Flat PAPI position has a nonzero entry price"
        );
        None
    } else {
        anyhow::ensure!(
            row.entry_price.value() > Decimal::ZERO
                && row.update_time.nanoseconds > UnixNanos::default(),
            "PAPI open position has no valid entry price or timestamp"
        );
        Some(row.entry_price.value())
    };

    Ok(PositionStatusReport::new(
        ctx.account_id,
        ctx.instrument.id(),
        side,
        parse_quantity(amount.abs(), ctx.instrument)?,
        row.update_time.nanoseconds,
        ctx.ts_init,
        None,
        None,
        avg_px_open,
    ))
}

pub(crate) fn commission(row: &TradeRow) -> anyhow::Result<Money> {
    let result = (|| {
        let amount = row.commission.require("commission")?.value();
        let code = row.commission_asset.require("commissionAsset")?;
        let currency = Currency::try_from_str(code)
            .ok_or_else(|| anyhow::anyhow!("Unknown commission currency"))?;
        let value = Money::from_decimal(amount, currency)?;
        anyhow::ensure!(value.as_decimal() == amount, "Inexact commission");
        Ok::<_, anyhow::Error>(value)
    })();

    result.map_err(|_| PapiCommissionError.into())
}

#[derive(Debug, Error)]
#[error("PAPI commission is missing, invalid, or not exactly representable in its currency")]
pub(crate) struct PapiCommissionError;

pub(crate) fn algo_child_id(row: &AlgoRow) -> anyhow::Result<Option<i64>> {
    match &row.actual_order_id {
        Field::Missing | Field::Null | Field::Empty => Ok(None),
        Field::Value(id) if id == "0" => Ok(None),
        Field::Value(id) => {
            anyhow::ensure!(
                id.bytes().all(|b| b.is_ascii_digit()),
                "Invalid PAPI actualOrderId"
            );
            let id: i64 = id
                .parse()
                .map_err(|_| anyhow::anyhow!("PAPI actualOrderId overflow"))?;
            anyhow::ensure!(id > 0, "Invalid PAPI actualOrderId");
            Ok(Some(id))
        }
        Field::Invalid(_) => anyhow::bail!("Invalid PAPI actualOrderId"),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OrderFamily {
    Ordinary,
    Algo,
}

pub(crate) fn venue_order_id(
    symbol: &str,
    family: OrderFamily,
    id: i64,
) -> anyhow::Result<VenueOrderId> {
    validate_symbol(symbol)?;
    anyhow::ensure!(id > 0, "PAPI order ID must be positive");
    let family = match family {
        OrderFamily::Ordinary => "O",
        OrderFamily::Algo => "A",
    };
    Ok(VenueOrderId::new_checked(format!(
        "PAPI:{family}:{symbol}:{id}"
    ))?)
}

pub(crate) fn decode_order_id(
    value: VenueOrderId,
    symbol: &str,
) -> anyhow::Result<(OrderFamily, i64)> {
    let parts: Vec<_> = value.as_str().split(':').collect();
    anyhow::ensure!(
        parts.len() == 4 && parts[0] == "PAPI" && parts[2] == symbol,
        "PAPI venue order ID has the wrong scope"
    );
    let family = match parts[1] {
        "O" => OrderFamily::Ordinary,
        "A" => OrderFamily::Algo,
        _ => anyhow::bail!("Unknown PAPI order ID family"),
    };
    let id: i64 = parts[3]
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid PAPI venue order ID"))?;
    anyhow::ensure!(
        venue_order_id(symbol, family, id)? == value,
        "Noncanonical PAPI venue order ID"
    );
    Ok((family, id))
}

pub(crate) fn client_order_id(value: &str) -> anyhow::Result<ClientOrderId> {
    anyhow::ensure!(
        (1..=36).contains(&value.len())
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._:/-".contains(&b)),
        "Invalid PAPI client order ID"
    );
    Ok(ClientOrderId::new_checked(value)?)
}

pub(crate) fn validate_symbol(symbol: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        (1..=32).contains(&symbol.len())
            && symbol
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'),
        "Invalid PAPI symbol"
    );
    Ok(())
}

pub(crate) fn ensure_one_way(side: BinancePositionSide) -> anyhow::Result<()> {
    anyhow::ensure!(
        side == BinancePositionSide::Both,
        "PAPI hedge mode is unsupported; one-way BOTH is required"
    );
    Ok(())
}

fn parse_quantity(value: Decimal, instrument: &InstrumentAny) -> anyhow::Result<Quantity> {
    let result = Quantity::from_decimal_dp(value, instrument.size_precision())?;
    anyhow::ensure!(
        result.as_decimal() == value,
        "PAPI quantity exceeds instrument precision"
    );
    Ok(result)
}

fn parse_price(value: Decimal, instrument: &InstrumentAny) -> anyhow::Result<Price> {
    anyhow::ensure!(value > Decimal::ZERO, "PAPI price must be positive");
    let result = Price::from_decimal_dp(value, instrument.price_precision())?;
    anyhow::ensure!(
        result.as_decimal() == value,
        "PAPI price exceeds instrument precision"
    );
    Ok(result)
}

fn validate_times(created: VenueTime, updated: VenueTime) -> anyhow::Result<()> {
    anyhow::ensure!(
        created.nanoseconds > UnixNanos::default() && updated.nanoseconds >= created.nanoseconds,
        "Invalid PAPI order timestamps"
    );
    Ok(())
}

fn validate_context(symbol: &str, ctx: ReportContext<'_>) -> anyhow::Result<()> {
    anyhow::ensure!(
        ctx.instrument.raw_symbol().as_str() == symbol,
        "PAPI report symbol does not match instrument metadata"
    );
    Ok(())
}

fn time_in_force(value: BinanceTimeInForce) -> anyhow::Result<(TimeInForce, bool)> {
    Ok(match value {
        BinanceTimeInForce::Gtc => (TimeInForce::Gtc, false),
        BinanceTimeInForce::Gtx => (TimeInForce::Gtc, true),
        BinanceTimeInForce::Ioc => (TimeInForce::Ioc, false),
        BinanceTimeInForce::Fok => (TimeInForce::Fok, false),
        BinanceTimeInForce::Gtd => (TimeInForce::Gtd, false),
        BinanceTimeInForce::Rpi | BinanceTimeInForce::Unknown => {
            anyhow::bail!("Unsupported PAPI timeInForce")
        }
    })
}

fn expiry(
    tif: BinanceTimeInForce,
    value: &Field<VenueTime>,
    accepted: UnixNanos,
) -> anyhow::Result<Option<UnixNanos>> {
    if tif == BinanceTimeInForce::Gtd {
        let expiry = value.require("goodTillDate")?.nanoseconds;
        anyhow::ensure!(expiry > accepted, "Invalid PAPI order expiry");
        Ok(Some(expiry))
    } else {
        match value {
            Field::Invalid(_) => anyhow::bail!("Invalid PAPI order expiry"),
            Field::Value(value) => {
                anyhow::ensure!(
                    value.nanoseconds == UnixNanos::default(),
                    "Unexpected PAPI order expiry"
                );
            }
            _ => {}
        }

        Ok(None)
    }
}

fn validate_filled_status(
    status: OrderStatus,
    quantity: Quantity,
    filled: Quantity,
) -> anyhow::Result<()> {
    let valid = match status {
        OrderStatus::Accepted | OrderStatus::Submitted | OrderStatus::Rejected => filled.is_zero(),
        OrderStatus::PartiallyFilled => filled.is_positive() && filled < quantity,
        OrderStatus::Filled => filled == quantity,
        _ => filled < quantity,
    };
    anyhow::ensure!(valid, "PAPI order status contradicts filled quantity");
    Ok(())
}
