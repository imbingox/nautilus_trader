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

use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{
        LiquiditySide, OrderSide, OrderStatus, OrderType, PositionSide, TimeInForce, TriggerType,
    },
    identifiers::{AccountId, ClientOrderId, InstrumentId, TradeId},
    instruments::Instrument,
    reports::OrderStatusReport,
    types::Currency,
};
use rstest::rstest;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::{models::*, parse::*};
use crate::{
    observations::models::JsonObject,
    testing::{self, TRADE_TIME, ms},
};

fn decode<T: DeserializeOwned>(value: &Value) -> anyhow::Result<T> {
    let JsonObject(row) = serde_json::from_str(&value.to_string())?;
    Ok(row)
}

fn order(value: &Value) -> anyhow::Result<OrderStatusReport> {
    let instrument = testing::instrument("BTCUSDT");
    order_report(
        &decode(value)?,
        ReportContext {
            account_id: AccountId::from("BINANCE-PAPI-001"),
            instrument: &instrument,
            ts_init: ms(TRADE_TIME + 1_000),
        },
        false,
    )
}

#[rstest]
fn ordinary_order_preserves_execution_terms_and_zeros_are_not_prices() {
    let report = order(&testing::order()).unwrap();
    assert_eq!(report.account_id, AccountId::from("BINANCE-PAPI-001"));
    assert_eq!(
        report.instrument_id,
        InstrumentId::from("BTCUSDT-PERP.BINANCE")
    );
    assert_eq!(report.venue_order_id.as_str(), "PAPI:O:BTCUSDT:270093109");
    assert_eq!(report.client_order_id, Some(ClientOrderId::from("abc")));
    assert_eq!(report.order_type, OrderType::Limit);
    assert_eq!(report.order_side, Some(OrderSide::Sell));
    assert_eq!(report.order_status, OrderStatus::Accepted);
    assert_eq!(report.time_in_force, TimeInForce::Gtc);
    assert_eq!(report.quantity.as_decimal(), dec!(0.010));
    assert_eq!(report.filled_qty.as_decimal(), dec!(0));
    assert_eq!(report.price.unwrap().as_decimal(), dec!(28511));
    assert_eq!(report.avg_px, None);
    assert_eq!(report.ts_accepted, ms(TRADE_TIME - 1_000));
    assert_eq!(report.ts_last, ms(TRADE_TIME - 1_000));
    assert_eq!(report.ts_init, ms(TRADE_TIME + 1_000));
    assert!(!report.post_only);
    assert!(!report.reduce_only);
    assert_eq!(report.expire_time, None);
    assert_eq!(report.trigger_price, None);

    let mut market = testing::order();
    market["type"] = json!("MARKET");
    market["origType"] = json!("MARKET");
    market["price"] = json!("0");
    let report = order(&market).unwrap();
    assert_eq!(report.order_type, OrderType::Market);
    assert_eq!(report.price, None);
    assert_eq!(report.avg_px, None);
}

#[rstest]
#[case("NEW", "0", "0", OrderStatus::Accepted)]
#[case("PENDING_NEW", "0", "0", OrderStatus::Submitted)]
#[case("PARTIALLY_FILLED", "0.005", "28511.125", OrderStatus::PartiallyFilled)]
#[case("FILLED", "0.010", "28511", OrderStatus::Filled)]
#[case("CANCELED", "0", "0", OrderStatus::Canceled)]
#[case("REJECTED", "0", "0", OrderStatus::Rejected)]
#[case("EXPIRED", "0", "0", OrderStatus::Expired)]
#[case("EXPIRED_IN_MATCH", "0", "0", OrderStatus::Expired)]
fn ordinary_statuses_are_explicit(
    #[case] status: &str,
    #[case] filled: &str,
    #[case] average: &str,
    #[case] expected: OrderStatus,
) {
    let mut value = testing::order();
    value["status"] = json!(status);
    value["executedQty"] = json!(filled);
    value["avgPrice"] = json!(average);
    let report = order(&value).unwrap();
    assert_eq!(report.order_status, expected);
}

#[rstest]
fn post_only_reduce_only_and_expiry_are_retained() {
    let mut value = testing::order();
    value["timeInForce"] = json!("GTX");
    value["reduceOnly"] = json!(true);
    let report = order(&value).unwrap();
    assert_eq!(report.time_in_force, TimeInForce::Gtc);
    assert!(report.post_only);
    assert!(report.reduce_only);

    value["timeInForce"] = json!("GTD");
    value["goodTillDate"] = json!(TRADE_TIME + 86_400_000);
    let report = order(&value).unwrap();
    assert_eq!(report.expire_time, Some(ms(TRADE_TIME + 86_400_000)));
    assert_eq!(report.time_in_force, TimeInForce::Gtd);
}

#[rstest]
#[case("origQty", json!("0.00001"))]
#[case("origQty", json!("-0.1"))]
#[case("origQty", json!("0"))]
#[case("origQty", json!("79228162514264337593543950335"))]
#[case("price", json!("28511.0001"))]
#[case("price", json!("79228162514264337593543950336"))]
#[case("price", json!("0"))]
#[case("executedQty", json!("1"))]
#[case("status", json!("UNDOCUMENTED"))]
#[case("side", json!("UNKNOWN"))]
#[case("positionSide", json!("LONG"))]
#[case("timeInForce", json!("RPI"))]
#[case("time", json!(0))]
#[case("updateTime", json!(-1))]
#[case("updateTime", json!(i64::MAX))]
#[case("origType", json!("STOP_MARKET"))]
#[case("clientOrderId", json!(""))]
#[case("orderId", json!(0))]
#[case("goodTillDate", json!(-1))]
#[case("goodTillDate", json!("invalid"))]
fn invalid_order_fields_fail_conversion(#[case] field: &str, #[case] value: Value) {
    let mut row = testing::order();
    row[field] = value;
    assert!(order(&row).is_err(), "{field}");
}

#[rstest]
fn missing_order_terms_and_positional_arrays_are_rejected() {
    for field in [
        "symbol",
        "side",
        "reduceOnly",
        "timeInForce",
        "clientOrderId",
        "executedQty",
    ] {
        let mut row = testing::order();
        row.as_object_mut().unwrap().remove(field);
        assert!(order(&row).is_err(), "{field}");
    }

    assert!(serde_json::from_str::<JsonObject<OrderRow>>("[]").is_err());
    let mut raw = testing::order().to_string();
    raw.insert_str(1, "\"orderId\":99,");
    assert!(serde_json::from_str::<JsonObject<OrderRow>>(&raw).is_err());
}

#[rstest]
fn trailing_decimal_zero_padding_is_exactly_equivalent() {
    let mut row = testing::order();
    row["price"] = json!("28511.00000000000000000000000000000000000");
    row["origQty"] = json!("0.010000000000000000000000000000000000000");
    let report = order(&row).unwrap();
    assert_eq!(report.quantity.as_decimal(), dec!(0.01));
    assert_eq!(report.price.unwrap().as_decimal(), dec!(28511));
}

#[rstest]
fn documented_hedge_mode_order_example_is_not_silently_accepted() {
    let raw: Value =
        serde_json::from_str(include_str!("../../test_data/reports/order.json")).unwrap();
    assert!(order(&raw).is_err());
}

#[rstest]
fn official_fill_example_preserves_fee_liquidity_and_linkage() {
    let instrument = testing::instrument("BTCUSDT");
    let ctx = ReportContext {
        account_id: AccountId::from("BINANCE-PAPI-001"),
        instrument: &instrument,
        ts_init: ms(TRADE_TIME + 1_000),
    };
    let order = order(&testing::filled_order()).unwrap();
    let row: TradeRow = decode(&testing::trade()).unwrap();
    let report = fill_report(&row, ctx, &order).unwrap();
    assert_eq!(report.trade_id, TradeId::from("67880589"));
    assert_eq!(report.venue_order_id, order.venue_order_id);
    assert_eq!(report.client_order_id, order.client_order_id);
    assert_eq!(report.instrument_id, instrument.id());
    assert_eq!(report.order_side, OrderSide::Sell);
    assert_eq!(report.last_qty.as_decimal(), dec!(0.010));
    assert_eq!(report.last_px.as_decimal(), dec!(28511));
    assert_eq!(report.commission.as_decimal(), dec!(0.114044));
    assert_eq!(report.commission.currency, Currency::USDT());
    assert_eq!(report.liquidity_side, LiquiditySide::Taker);
    assert_eq!(report.ts_event, ms(TRADE_TIME));
    assert_eq!(report.ts_init, ctx.ts_init);
}

#[rstest]
fn signed_third_currency_rebate_is_not_absolutized_or_relabelled() {
    let mut value = testing::trade();
    value["commission"] = json!("-0.00001234");
    value["commissionAsset"] = json!("BNB");
    let row: TradeRow = decode(&value).unwrap();
    let money = commission(&row).unwrap();
    assert_eq!(money.currency, Currency::BNB());
    assert_eq!(money.as_decimal(), dec!(-0.00001234));
}

#[rstest]
#[case(json!(null), json!("USDT"))]
#[case(json!(""), json!("USDT"))]
#[case(json!("invalid"), json!("USDT"))]
#[case(json!("0.000000001"), json!("USDT"))]
#[case(json!("79228162514264337593543950335"), json!("USDT"))]
#[case(json!("0"), json!("UNKNOWN_CURRENCY"))]
#[case(json!("0"), json!(null))]
fn invalid_commission_is_a_fatal_typed_error(#[case] amount: Value, #[case] asset: Value) {
    let mut value = testing::trade();
    value["commission"] = amount;
    value["commissionAsset"] = asset;
    let row: TradeRow = decode(&value).unwrap();
    assert!(commission(&row).unwrap_err().is::<PapiCommissionError>());
}

#[rstest]
fn missing_commission_and_maker_never_receive_defaults() {
    let mut value = testing::trade();
    value.as_object_mut().unwrap().remove("commission");
    let row: TradeRow = decode(&value).unwrap();
    assert!(commission(&row).unwrap_err().is::<PapiCommissionError>());
    value.as_object_mut().unwrap().remove("maker");
    assert!(decode::<TradeRow>(&value).is_err());
}

#[rstest]
#[case("0.010", "28511.125", PositionSide::Long, dec!(0.010))]
#[case("-0.010", "28511.125", PositionSide::Short, dec!(-0.010))]
#[case("0.000", "0.00000", PositionSide::Flat, dec!(0))]
fn one_way_positions_preserve_sign_and_average(
    #[case] amount: &str,
    #[case] entry: &str,
    #[case] side: PositionSide,
    #[case] signed: Decimal,
) {
    let instrument = testing::instrument("BTCUSDT");
    let mut value = testing::position("BTCUSDT");
    value["positionAmt"] = json!(amount);
    value["entryPrice"] = json!(entry);
    let row: PositionRow = decode(&value).unwrap();
    let report = position_report(
        &row,
        ReportContext {
            account_id: AccountId::from("BINANCE-PAPI-001"),
            instrument: &instrument,
            ts_init: ms(TRADE_TIME),
        },
    )
    .unwrap();
    assert_eq!(report.position_side, side);
    assert_eq!(report.signed_decimal_qty, signed);
    assert_eq!(report.quantity.as_decimal(), signed.abs());
    assert_eq!(
        report.avg_px_open,
        if signed.is_zero() {
            None
        } else {
            Some(dec!(28511.125))
        }
    );
    assert_eq!(report.ts_last, row.update_time.nanoseconds);
}

#[rstest]
fn zero_position_time_is_retained_only_for_explicit_flat_rows() {
    let instrument = testing::instrument("BTCUSDT");
    let ctx = ReportContext {
        account_id: AccountId::from("BINANCE-PAPI-001"),
        instrument: &instrument,
        ts_init: ms(TRADE_TIME),
    };
    let mut value = testing::position("BTCUSDT");
    value["updateTime"] = json!(0);
    let report = position_report(&decode(&value).unwrap(), ctx).unwrap();
    assert_eq!(report.ts_last, UnixNanos::default());
    value["positionAmt"] = json!("0.010");
    value["entryPrice"] = json!("28511");
    assert!(position_report(&decode(&value).unwrap(), ctx).is_err());
}

#[rstest]
fn order_ids_are_reversible_and_distinct_across_symbol_and_family() {
    let ordinary = venue_order_id("BTCUSDT", OrderFamily::Ordinary, 42).unwrap();
    let algo = venue_order_id("BTCUSDT", OrderFamily::Algo, 42).unwrap();
    let other = venue_order_id("ETHUSDT", OrderFamily::Ordinary, 42).unwrap();
    assert_ne!(ordinary, algo);
    assert_ne!(ordinary, other);
    assert_eq!(
        decode_order_id(ordinary, "BTCUSDT").unwrap(),
        (OrderFamily::Ordinary, 42)
    );
    assert_eq!(
        decode_order_id(algo, "BTCUSDT").unwrap(),
        (OrderFamily::Algo, 42)
    );
    assert!(decode_order_id(other, "BTCUSDT").is_err());
    assert!(decode_order_id("PAPI:O:BTCUSDT:042".into(), "BTCUSDT").is_err());
    assert!(decode_order_id("PAPI:O:BTCUSDT:-42".into(), "BTCUSDT").is_err());
    assert!(client_order_id(&"x".repeat(37)).is_err());
}

#[rstest]
fn official_open_algo_preserves_trigger_terms_and_parent_identity() {
    let instrument = testing::instrument("BNBUSDT");
    let row: AlgoRow = decode(&testing::algo()).unwrap();
    let report = algo_report(
        &row,
        None,
        ReportContext {
            account_id: AccountId::from("BINANCE-PAPI-001"),
            instrument: &instrument,
            ts_init: ms(1_800_000_000_000),
        },
    )
    .unwrap();
    assert_eq!(report.venue_order_id.as_str(), "PAPI:A:BNBUSDT:2146760");
    assert_eq!(
        report.client_order_id.unwrap().as_str(),
        "6B2I9XVcJpCjqPAJ4YoFX7"
    );
    assert_eq!(report.order_status, OrderStatus::Accepted);
    assert_eq!(report.order_type, OrderType::LimitIfTouched);
    assert_eq!(report.trigger_type, Some(TriggerType::LastPrice));
    assert_eq!(report.trigger_price.unwrap().as_decimal(), dec!(750));
    assert_eq!(report.price.unwrap().as_decimal(), dec!(750));
    assert_eq!(report.quantity.as_decimal(), dec!(0.01));
    assert_eq!(report.filled_qty.as_decimal(), dec!(0));
    assert_eq!(report.ts_triggered, None);
    assert_eq!(report.avg_px, None);
}

#[rstest]
#[case("TRIGGERED")]
#[case("FINISHED")]
#[case("UNKNOWN")]
fn algo_status_without_child_never_invents_a_fill(#[case] status: &str) {
    let instrument = testing::instrument("BNBUSDT");
    let mut row = testing::algo();
    row["algoStatus"] = json!(status);
    assert!(
        algo_report(
            &decode(&row).unwrap(),
            None,
            ReportContext {
                account_id: AccountId::from("BINANCE-PAPI-001"),
                instrument: &instrument,
                ts_init: ms(1_800_000_000_000),
            }
        )
        .is_err()
    );
}

#[rstest]
fn close_position_and_trailing_algos_are_explicitly_unsupported() {
    let instrument = testing::instrument("BNBUSDT");
    let ctx = ReportContext {
        account_id: AccountId::from("BINANCE-PAPI-001"),
        instrument: &instrument,
        ts_init: ms(1_800_000_000_000),
    };
    let mut row = testing::algo();
    row["closePosition"] = json!(true);
    row["quantity"] = json!("0");
    assert!(algo_report(&decode(&row).unwrap(), None, ctx).is_err());
    row = testing::algo();
    row["orderType"] = json!("TRAILING_STOP_MARKET");
    assert!(algo_report(&decode(&row).unwrap(), None, ctx).is_err());
}

#[rstest]
#[case("price", json!("28512.00"))]
#[case("origQty", json!("0.020"))]
#[case("side", json!("BUY"))]
#[case("positionSide", json!("SHORT"))]
#[case("timeInForce", json!("IOC"))]
#[case("reduceOnly", json!(true))]
#[case("orderId", json!(7))]
#[case("time", json!(TRADE_TIME - 751))]
fn algo_child_execution_terms_must_agree(#[case] field: &str, #[case] value: Value) {
    let instrument = testing::instrument("BTCUSDT");
    let (parent, mut child) = testing::triggered_algo();
    child[field] = value;
    let result = algo_report(
        &decode(&parent).unwrap(),
        Some(&decode(&child).unwrap()),
        ReportContext {
            account_id: AccountId::from("BINANCE-PAPI-001"),
            instrument: &instrument,
            ts_init: ms(TRADE_TIME + 1_000),
        },
    );
    assert!(result.is_err(), "{field}");
}

#[rstest]
#[case("triggerTime", json!(-1))]
#[case("triggerTime", json!("invalid"))]
#[case("triggerTime", json!(1_750_485_492_077_u64))]
#[case("goodTillDate", json!(-1))]
fn untriggered_algo_cannot_hide_invalid_optional_times(#[case] field: &str, #[case] value: Value) {
    let instrument = testing::instrument("BNBUSDT");
    let mut row = testing::algo();
    row[field] = value;
    assert!(
        algo_report(
            &decode(&row).unwrap(),
            None,
            ReportContext {
                account_id: AccountId::from("BINANCE-PAPI-001"),
                instrument: &instrument,
                ts_init: ms(1_800_000_000_000),
            }
        )
        .is_err()
    );
}

#[rstest]
fn triggered_algo_update_cannot_precede_its_trigger() {
    let instrument = testing::instrument("BTCUSDT");
    let (mut parent, child) = testing::triggered_algo();
    parent["updateTime"] = json!(TRADE_TIME - 1_000);
    assert!(
        algo_report(
            &decode(&parent).unwrap(),
            Some(&decode(&child).unwrap()),
            ReportContext {
                account_id: AccountId::from("BINANCE-PAPI-001"),
                instrument: &instrument,
                ts_init: ms(TRADE_TIME + 1_000),
            }
        )
        .is_err()
    );
}
