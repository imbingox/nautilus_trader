// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License. You may obtain a copy of the
//  License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Exact projection of retained private-stream order deltas into Nautilus reports.

use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{LiquiditySide, OrderStatus},
    identifiers::{AccountId, TradeId},
    instruments::{Instrument, InstrumentAny},
    reports::{FillReport, OrderStatusReport},
    types::{Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;

use super::state::OrderDelta;
use crate::reports::parse::{OrderFamily, client_order_id, venue_order_id};

pub(super) fn incremental_reports(
    delta: &OrderDelta,
    account_id: AccountId,
    instrument: &InstrumentAny,
    ts_init: UnixNanos,
) -> anyhow::Result<(OrderStatusReport, Vec<FillReport>)> {
    let order = &delta.order;
    anyhow::ensure!(
        instrument.raw_symbol().as_str() == order.symbol,
        "PAPI stream order symbol does not match instrument metadata"
    );
    anyhow::ensure!(
        order.position_side == "BOTH",
        "PAPI stream order is not one-way"
    );
    let client_order_id = client_order_id(&order.client_order_id)?;
    let venue_order_id = venue_order_id(&order.symbol, OrderFamily::Ordinary, order.order_id)?;
    let quantity = exact_quantity(order.quantity, instrument)?;
    let filled_qty = exact_quantity(order.accumulated_qty, instrument)?;
    let order_status = order_status(&order.status)?;
    validate_status_quantities(order_status, quantity, filled_qty)?;
    let ts_accepted = milliseconds(order.accepted_time_ms)?;
    let mut ts_last = milliseconds(order.transaction_time_ms.max(order.event_time_ms))?;

    if let Some(fill) = &delta.fill {
        ts_last = ts_last.max(milliseconds(fill.trade_time_ms)?);
    }

    let mut report = OrderStatusReport::new(
        account_id,
        instrument.id(),
        Some(client_order_id),
        venue_order_id,
        Some(order.side),
        order.order_type,
        order.time_in_force,
        order_status,
        quantity,
        filled_qty,
        ts_accepted,
        ts_last,
        ts_init,
        None,
    );
    report.reduce_only = order.reduce_only;
    report.post_only = order.post_only;
    report.price = order
        .price
        .map(|price| exact_price(price, instrument))
        .transpose()?;
    report.avg_px = order.average_price;

    let fills = delta
        .fill
        .as_ref()
        .map(|fill| {
            let currency = Currency::try_from_str(&fill.commission_asset)
                .ok_or_else(|| anyhow::anyhow!("Unknown PAPI stream commission currency"))?;
            let commission = Money::from_decimal(fill.commission, currency)?;
            anyhow::ensure!(
                commission.as_decimal() == fill.commission,
                "Inexact PAPI stream commission"
            );
            let mut report = FillReport::new(
                account_id,
                instrument.id(),
                venue_order_id,
                TradeId::new_checked(fill.trade_id.to_string())?,
                order.side,
                exact_quantity(fill.quantity, instrument)?,
                exact_price(fill.price, instrument)?,
                commission,
                if fill.maker {
                    LiquiditySide::Maker
                } else {
                    LiquiditySide::Taker
                },
                Some(client_order_id),
                None,
                milliseconds(fill.trade_time_ms)?,
                ts_init,
                None,
            );
            report.avg_px = order.average_price;
            Ok::<_, anyhow::Error>(report)
        })
        .transpose()?
        .into_iter()
        .collect();

    Ok((report, fills))
}

fn order_status(value: &str) -> anyhow::Result<OrderStatus> {
    Ok(match value {
        "PENDING_NEW" => OrderStatus::Submitted,
        "NEW" => OrderStatus::Accepted,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "PENDING_CANCEL" => OrderStatus::PendingCancel,
        "CANCELED" => OrderStatus::Canceled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" | "EXPIRED_IN_MATCH" => OrderStatus::Expired,
        _ => anyhow::bail!("Unsupported PAPI stream order status"),
    })
}

fn validate_status_quantities(
    status: OrderStatus,
    quantity: Quantity,
    filled: Quantity,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        filled <= quantity,
        "PAPI stream filled quantity exceeds order quantity"
    );

    match status {
        OrderStatus::PartiallyFilled => anyhow::ensure!(
            filled.is_positive() && filled < quantity,
            "Invalid PAPI stream partial-fill quantity"
        ),
        OrderStatus::Filled => anyhow::ensure!(
            filled == quantity,
            "Invalid PAPI stream filled order quantity"
        ),
        OrderStatus::Submitted | OrderStatus::Accepted | OrderStatus::Rejected => anyhow::ensure!(
            filled.is_zero(),
            "Unexpected PAPI stream fill quantity for order status"
        ),
        OrderStatus::PendingCancel | OrderStatus::Canceled | OrderStatus::Expired => {}
        _ => anyhow::bail!("Unsupported PAPI stream order status"),
    }
    Ok(())
}

fn exact_quantity(value: Decimal, instrument: &InstrumentAny) -> anyhow::Result<Quantity> {
    let quantity = Quantity::from_decimal_dp(value, instrument.size_precision())?;
    anyhow::ensure!(
        quantity.as_decimal() == value,
        "Inexact PAPI stream quantity"
    );
    Ok(quantity)
}

fn exact_price(value: Decimal, instrument: &InstrumentAny) -> anyhow::Result<Price> {
    anyhow::ensure!(value > Decimal::ZERO, "Invalid PAPI stream price");
    let price = Price::from_decimal_dp(value, instrument.price_precision())?;
    anyhow::ensure!(price.as_decimal() == value, "Inexact PAPI stream price");
    Ok(price)
}

fn milliseconds(value: i64) -> anyhow::Result<UnixNanos> {
    let value = u64::try_from(value)?;
    Ok(UnixNanos::from(value.checked_mul(1_000_000).ok_or_else(
        || anyhow::anyhow!("PAPI stream timestamp overflow"),
    )?))
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::{OrderSide, OrderType, TimeInForce},
        identifiers::ClientOrderId,
    };
    use rstest::rstest;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{
        testing,
        websocket::messages::{FillFact, OrderFact},
    };

    fn delta() -> OrderDelta {
        let fill = FillFact {
            trade_id: 7,
            quantity: dec!(0.001),
            price: dec!(42000.10),
            commission: dec!(-0.00000123),
            commission_asset: "BNB".to_string(),
            maker: false,
            trade_time_ms: 1_700_000_000_001,
        };
        OrderDelta {
            order: OrderFact {
                symbol: "BTCUSDT".to_string(),
                client_order_id: "client-1".to_string(),
                order_id: 42,
                side: OrderSide::Buy,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::Gtc,
                post_only: false,
                quantity: dec!(0.010),
                price: Some(dec!(42000.10)),
                average_price: Some(dec!(42000.10)),
                reduce_only: false,
                position_side: "BOTH".to_string(),
                execution_type: "TRADE".to_string(),
                status: "PARTIALLY_FILLED".to_string(),
                accumulated_qty: dec!(0.002),
                accepted_time_ms: 1_700_000_000_000,
                event_time_ms: 1_700_000_000_002,
                transaction_time_ms: 1_700_000_000_001,
                fill: Some(fill.clone()),
            },
            fill: Some(fill),
        }
    }

    #[rstest]
    fn order_and_fill_projection_preserves_exact_stream_economics() {
        let instrument = testing::instrument("BTCUSDT");
        let (order, fills) = incremental_reports(
            &delta(),
            AccountId::from("BINANCE-PAPI-001"),
            &instrument,
            UnixNanos::from(1_700_000_000_003_000_000),
        )
        .unwrap();

        assert_eq!(order.client_order_id, Some(ClientOrderId::from("client-1")));
        assert_eq!(order.order_status, OrderStatus::PartiallyFilled);
        assert_eq!(order.quantity.as_decimal(), dec!(0.010));
        assert_eq!(order.filled_qty.as_decimal(), dec!(0.002));
        assert_eq!(order.price.unwrap().as_decimal(), dec!(42000.10));
        assert_eq!(order.avg_px, Some(dec!(42000.10)));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].trade_id.as_str(), "7");
        assert_eq!(fills[0].commission.as_decimal(), dec!(-0.00000123));
        assert_eq!(fills[0].commission.currency, Currency::BNB());
        assert_eq!(fills[0].liquidity_side, LiquiditySide::Taker);
    }

    #[rstest]
    fn inexact_stream_values_fail_projection() {
        let instrument = testing::instrument("BTCUSDT");
        let mut delta = delta();
        delta.order.quantity = dec!(0.0100001);

        assert!(
            incremental_reports(
                &delta,
                AccountId::from("BINANCE-PAPI-001"),
                &instrument,
                UnixNanos::from(1_700_000_000_003_000_000),
            )
            .is_err()
        );
    }
}
