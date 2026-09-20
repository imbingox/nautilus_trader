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

use rstest::rstest;
use rust_decimal::Decimal;
use serde::Deserialize;

use super::{
    fields::{Amount, Field, VenueTime},
    *,
};

const BALANCES: &str = include_str!("../../test_data/observations/balances.json");
const ACCOUNT: &str = include_str!("../../test_data/observations/account.json");
const UM_ACCOUNT: &str = include_str!("../../test_data/observations/um_account.json");

#[rstest]
#[case("{}", "missing")]
#[case(r#"{"amount":null}"#, "null")]
#[case(r#"{"amount":""}"#, "empty")]
#[case(r#"{"amount":"bad"}"#, "invalid")]
#[case(r#"{"amount":"0"}"#, "value")]
fn test_field_states(#[case] body: &str, #[case] expected: &str) {
    #[derive(Deserialize)]
    struct Row {
        #[serde(default)]
        amount: Field<Amount>,
    }

    let row: Row = serde_json::from_str(body).unwrap();
    let encoded = serde_json::to_value(&row.amount).unwrap();
    assert_eq!(encoded["state"], expected);
    assert_eq!(row.amount.require("amount").is_ok(), expected == "value");
}

#[rstest]
#[case("-0.00000001")]
#[case("12345678901234567890.12345678")]
#[case("0.0000000000000000000000000001")]
#[case("79228162514264337593543950335")]
#[case("-79228162514264337593543950335")]
#[case("-0.00000000")]
fn test_amount_exact_round_trip(#[case] text: &str) {
    let json = serde_json::to_string(text).unwrap();
    let field: Field<Amount> = serde_json::from_str(&json).unwrap();
    let amount = field.require("amount").unwrap();
    assert_eq!(amount.value(), Decimal::from_str_exact(text).unwrap());
    assert_eq!(serde_json::to_string(amount).unwrap(), json);
}

#[rstest]
#[case(r#""0.00000000000000000000000000001""#)]
#[case(r#""1.00000000000000000000000000001""#)]
#[case(r#""79228162514264337593543950336""#)]
#[case(r#""NaN""#)]
#[case(r#""Infinity""#)]
#[case(r#""1e-8""#)]
#[case(r#""1_000""#)]
#[case(r#"" 1""#)]
#[case(r#""1 ""#)]
#[case(r#""+1""#)]
#[case(r#"".5""#)]
#[case(r#""1.""#)]
#[case(r#""1.2.3""#)]
#[case("0.123456789012345678901234567890123456789")]
#[case("1e999")]
#[case("true")]
#[case("{}")]
#[case("[]")]
fn test_invalid_amount_retains_exact_wire_value(#[case] json: &str) {
    let field: Field<Amount> = serde_json::from_str(json).unwrap();
    assert!(field.require("amount").is_err());

    let Field::Invalid(raw) = field else {
        panic!("Expected invalid field")
    };
    assert_eq!(raw.get(), json);
}

#[rstest]
#[case("1.0000000000000000000000000000000000", "1")]
#[case(
    "79228162514264337593543950335.00000000",
    "79228162514264337593543950335"
)]
#[case(
    "0.0000000000000000000000000001000000",
    "0.0000000000000000000000000001"
)]
fn test_amount_fractional_zero_padding(#[case] text: &str, #[case] significant: &str) {
    let json = serde_json::to_string(text).unwrap();
    let field: Field<Amount> = serde_json::from_str(&json).unwrap();
    let amount = field.require("amount").unwrap();
    assert_eq!(
        amount.value(),
        Decimal::from_str_exact(significant).unwrap()
    );
    assert_eq!(serde_json::to_string(amount).unwrap(), json);
}

#[rstest]
#[case("0", Some(0))]
#[case("1617939110373", Some(1_617_939_110_373_000_000))]
#[case("18446744073709", Some(18_446_744_073_709_000_000))]
#[case("18446744073710", None)]
#[case("-1", None)]
#[case("1.5", None)]
#[case(r#""1617939110373""#, None)]
fn test_venue_time(#[case] json: &str, #[case] expected: Option<u64>) {
    let field: Field<VenueTime> = serde_json::from_str(json).unwrap();
    assert_eq!(
        field
            .require("updateTime")
            .ok()
            .map(|value| value.nanoseconds.as_u64()),
        expected,
    );
}

#[rstest]
fn test_balance_components_are_not_projected_or_added() {
    let ObservationData::Balances(rows) =
        parse_response(&ObservationSource::Balance { asset: None }, BALANCES).unwrap()
    else {
        panic!("Expected balances")
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].asset, "BTC");
    assert_eq!(
        rows[0]
            .total_wallet_balance
            .require("total")
            .unwrap()
            .value(),
        Decimal::ONE
    );
    assert_eq!(
        rows[1]
            .total_wallet_balance
            .require("total")
            .unwrap()
            .value(),
        Decimal::from(-10)
    );
    assert_eq!(
        rows[1]
            .cross_margin_borrowed
            .require("borrowed")
            .unwrap()
            .value(),
        Decimal::from(20)
    );
    assert_eq!(
        rows[1]
            .negative_balance
            .require("negativeBalance")
            .unwrap()
            .value(),
        Decimal::from(10)
    );
    assert!(matches!(rows[0].cm_unrealized_pnl, Field::Empty));
    assert!(matches!(rows[1].cross_margin_free, Field::Null));
    assert!(matches!(rows[1].cross_margin_locked, Field::Missing));
}

#[rstest]
#[case(r#"{"asset":"BTC","totalWalletBalance":"1"}"#)]
#[case(r#"[{"asset":"BTC","totalWalletBalance":"1"}]"#)]
fn test_scoped_balance_shapes(#[case] body: &str) {
    let source = ObservationSource::Balance {
        asset: Some("BTC".to_string()),
    };
    assert!(parse_response(&source, body).is_ok());
}

#[rstest]
#[case(r#"[{"asset":"BTC"},{"asset":"BTC"}]"#)]
#[case(r#"[{"asset":""}]"#)]
#[case(r#"[{"asset":" BTC"}]"#)]
#[case(r#"[{"asset":null}]"#)]
#[case("[{}]")]
#[case(r#"[{"asset":"BTC","asset":"USDT"}]"#)]
#[case(r#"[{"asset":"BTC","totalWalletBalance":"1","totalWalletBalance":"2"}]"#)]
#[case(r#"{"asset":"BTC"}"#)]
#[case(r#"{"code":-2015,"msg":"fixture rejection"}"#)]
#[case("null")]
#[case("[{")]
fn test_balance_shape_and_identity_errors(#[case] body: &str) {
    assert!(parse_response(&ObservationSource::Balance { asset: None }, body).is_err());
}

#[rstest]
#[case("[]")]
#[case(r#"{"asset":"USDT"}"#)]
#[case(r#"[{"asset":"BTC"},{"asset":"USDT"}]"#)]
fn test_balance_scope_mismatch(#[case] body: &str) {
    assert!(
        parse_response(
            &ObservationSource::Balance {
                asset: Some("BTC".to_string())
            },
            body,
        )
        .is_err()
    );
}

#[rstest]
fn test_empty_full_balance_response() {
    let result = parse_response(&ObservationSource::Balance { asset: None }, "[]").unwrap();
    assert!(matches!(result, ObservationData::Balances(rows) if rows.is_empty()));
}

#[rstest]
#[case(ObservationSource::Balance { asset: None }, r#"[["BTC"]]"#)]
#[case(ObservationSource::Account, r#"[null,null,null,null,null,"NORMAL"]"#)]
#[case(ObservationSource::UmAccountV1, "[[],[]]")]
#[case(ObservationSource::UmAccountV2, "[[],[]]")]
#[case(
    ObservationSource::UmAccountV2,
    r#"{"assets":[["USDT"]],"positions":[]}"#
)]
#[case(
    ObservationSource::UmAccountV2,
    r#"{"assets":[],"positions":[["BTCUSDT","BOTH"]]}"#
)]
fn test_rejects_array_shaped_objects(#[case] source: ObservationSource, #[case] body: &str) {
    assert!(parse_response(&source, body).is_err());
}

#[rstest]
fn test_summary_preserves_unknown_status_and_unavailable_capacity() {
    let ObservationData::Account(summary) =
        parse_response(&ObservationSource::Account, ACCOUNT).unwrap()
    else {
        panic!("Expected account summary")
    };
    assert_eq!(
        summary.account_status.require("status").unwrap(),
        "FUTURE_STATUS"
    );
    assert_eq!(
        summary.account_equity.require("equity").unwrap().value(),
        Decimal::from_str_exact("1234.12345678").unwrap(),
    );
    assert!(matches!(summary.total_available_balance, Field::Empty));
    assert!(matches!(summary.total_margin_open_loss, Field::Null));
}

#[rstest]
#[case(ObservationSource::UmAccountV1)]
#[case(ObservationSource::UmAccountV2)]
fn test_um_component_observations(#[case] source: ObservationSource) {
    let ObservationData::UmAccount(account) = parse_response(&source, UM_ACCOUNT).unwrap() else {
        panic!("Expected UM account")
    };
    assert_eq!(account.assets[0].asset, "USDT");
    assert_eq!(account.positions[0].symbol, "BTCUSDT");
    assert_eq!(account.positions[0].position_side, "BOTH");
    assert_eq!(
        account.positions[0]
            .position_amt
            .require("amount")
            .unwrap()
            .value(),
        Decimal::from(-2)
    );
    assert!(matches!(account.positions[0].entry_price, Field::Missing));
    assert_eq!(
        account.positions[0]
            .update_time
            .require("time")
            .unwrap()
            .milliseconds,
        0
    );
}

#[rstest]
#[case(r#"{"assets":[],"positions":null}"#)]
#[case(r#"{"assets":[]}"#)]
#[case(r#"{"assets":[{"asset":"USDT"},{"asset":"USDT"}],"positions":[]}"#)]
#[case(r#"{"assets":[],"positions":[{"symbol":"BTCUSDT","positionSide":"BOTH"},{"symbol":"BTCUSDT","positionSide":"BOTH"}]}"#)]
fn test_um_invalid_coverage(#[case] body: &str) {
    assert!(parse_response(&ObservationSource::UmAccountV2, body).is_err());
}

#[rstest]
fn test_um_side_identity_and_sparse_response() {
    let body = r#"{"assets":[],"positions":[{"symbol":"BTCUSDT","positionSide":"LONG"},{"symbol":"BTCUSDT","positionSide":"SHORT"}]}"#;
    assert!(parse_response(&ObservationSource::UmAccountV2, body).is_ok());
    assert!(
        parse_response(
            &ObservationSource::UmAccountV2,
            r#"{"assets":[],"positions":[]}"#
        )
        .is_ok()
    );
}

#[rstest]
fn test_failed_refresh_preserves_observation_and_monotonic_age() {
    let start = Instant::now();
    let max_age = Duration::from_secs(10);
    let mut slot = ObservationSlot::new(
        AccountId::from("BINANCE-PAPI-001"),
        ObservationSource::Account,
    );
    assert_eq!(slot.receipt_status(start, max_age), ReceiptStatus::Missing);
    slot.record_response(ACCOUNT, 1, UnixNanos::from(100), start, start)
        .unwrap();
    assert_eq!(
        slot.receipt_status(start + max_age, max_age),
        ReceiptStatus::Recent
    );
    assert_eq!(
        slot.receipt_status(start + max_age + Duration::from_nanos(1), max_age),
        ReceiptStatus::Stale
    );
    assert_eq!(
        slot.receipt_status(start - Duration::from_nanos(1), max_age),
        ReceiptStatus::Stale
    );
    slot.record_failure("Fixture request failed".to_string());
    assert_eq!(slot.receipt_status(start, max_age), ReceiptStatus::Failed);
    assert_eq!(slot.last_response().unwrap().generation, 1);
    assert!(
        slot.record_response("{}", 2, UnixNanos::from(90), start, start)
            .is_err()
    );
    assert_eq!(slot.receipt_status(start, max_age), ReceiptStatus::Failed);
    assert_eq!(slot.last_response().unwrap().generation, 1);

    // Wall-clock rollback does not affect monotonic receipt age
    slot.record_response(
        ACCOUNT,
        3,
        UnixNanos::from(90),
        start + Duration::from_secs(1),
        start + Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(
        slot.receipt_status(start + Duration::from_secs(2), max_age),
        ReceiptStatus::Recent
    );
    assert_eq!(
        slot.last_response().unwrap().ts_received,
        UnixNanos::from(90)
    );
}

#[rstest]
fn test_source_isolation_and_risk_only_updates() {
    let start = Instant::now();
    let account_id = AccountId::from("BINANCE-PAPI-001");
    let mut balances = ObservationSlot::new(account_id, ObservationSource::Balance { asset: None });
    let mut risk = ObservationSlot::new(account_id, ObservationSource::Account);
    balances
        .record_response(BALANCES, 1, UnixNanos::from(100), start, start)
        .unwrap();
    risk.record_response(ACCOUNT, 1, UnixNanos::from(100), start, start)
        .unwrap();
    let changed = ACCOUNT.replace("FUTURE_STATUS", "REDUCE_ONLY");
    risk.record_response(&changed, 2, UnixNanos::from(200), start, start)
        .unwrap();
    balances.record_failure("Fixture request failed".to_string());
    assert_eq!(balances.last_response().unwrap().generation, 1);
    assert_eq!(risk.last_response().unwrap().generation, 2);
    assert_eq!(
        risk.receipt_status(start, Duration::from_secs(1)),
        ReceiptStatus::Recent
    );
    let ObservationData::Account(summary) = &risk.last_response().unwrap().data else {
        panic!("Expected account summary")
    };
    assert_eq!(
        summary.account_status.require("status").unwrap(),
        "REDUCE_ONLY"
    );
}

#[rstest]
fn test_out_of_order_response_preserves_latest_observation() {
    let start = Instant::now();
    let mut slot = ObservationSlot::new(
        AccountId::from("BINANCE-PAPI-001"),
        ObservationSource::Account,
    );
    slot.record_response(ACCOUNT, 2, UnixNanos::from(100), start, start)
        .unwrap();
    assert!(
        slot.record_response(ACCOUNT, 1, UnixNanos::from(200), start, start)
            .is_err()
    );
    assert!(
        slot.record_response(ACCOUNT, 2, UnixNanos::from(200), start, start)
            .is_err()
    );
    assert!(
        slot.record_response(
            ACCOUNT,
            3,
            UnixNanos::from(200),
            start - Duration::from_secs(1),
            start - Duration::from_secs(1)
        )
        .is_err()
    );
    assert_eq!(slot.last_response().unwrap().generation, 2);
    assert_eq!(
        slot.receipt_status(start, Duration::from_secs(1)),
        ReceiptStatus::Failed
    );
}

#[rstest]
fn test_observation_serialization_preserves_provenance_and_raw_body() {
    let body = ACCOUNT.replace("FUTURE_STATUS", "NEW_STATUS");
    let mut slot = ObservationSlot::new(
        AccountId::from("BINANCE-PAPI-002"),
        ObservationSource::Account,
    );
    let now = Instant::now();
    slot.record_response(&body, 42, UnixNanos::from(123), now, now)
        .unwrap();
    let observation = slot.last_response().unwrap();
    assert_eq!(observation.source.endpoint(), "/papi/v1/account");
    assert_eq!(observation.raw.get(), body.trim());
    let encoded = serde_json::to_value(observation).unwrap();
    assert_eq!(encoded["account_id"], "BINANCE-PAPI-002");
    assert_eq!(encoded["generation"], 42);
    assert_eq!(encoded["raw"]["futureField"], "retained");
    assert_eq!(
        encoded["data"]["Account"]["accountEquity"]["value"],
        "1234.12345678"
    );
}

#[rstest]
fn test_oversized_response_preserves_previous_observation() {
    let start = Instant::now();
    let mut slot = ObservationSlot::new(
        AccountId::from("BINANCE-PAPI-001"),
        ObservationSource::Account,
    );
    slot.record_response(ACCOUNT, 1, UnixNanos::from(100), start, start)
        .unwrap();
    let oversized = " ".repeat(MAX_RESPONSE_BYTES + 1);
    assert!(
        slot.record_response(&oversized, 2, UnixNanos::from(200), start, start)
            .is_err()
    );
    assert_eq!(slot.last_response().unwrap().generation, 1);
    assert_eq!(
        slot.receipt_status(start, Duration::from_secs(1)),
        ReceiptStatus::Failed
    );
}

#[rstest]
fn test_request_timing_and_refresh_lifecycle_preserve_diagnostic_values() {
    let requested = Instant::now();
    let received = requested + Duration::from_millis(25);
    let mut slot = ObservationSlot::new(
        AccountId::from("BINANCE-PAPI-001"),
        ObservationSource::Account,
    );
    let missing = slot.timing(requested);
    assert_eq!(missing.receipt_age_ns, None);
    assert_eq!(missing.collection_span_ns, None);
    slot.record_response(ACCOUNT, 1, UnixNanos::from(100), requested, received)
        .unwrap();
    let initial = slot.timing(received);
    assert_eq!(initial.receipt_age_ns, Some(0));
    assert_eq!(initial.collection_span_ns, Some(25_000_000));
    let later = slot.timing(received + Duration::from_secs(2));
    assert_eq!(later.receipt_age_ns, Some(2_000_000_000));
    assert_eq!(later.collection_span_ns, initial.collection_span_ns);
    assert_eq!(slot.timing(requested).receipt_age_ns, None);

    slot.record_refresh_started();
    assert_eq!(
        slot.receipt_status(received, Duration::from_secs(1)),
        ReceiptStatus::Refreshing
    );
    slot.record_canceled();
    assert_eq!(
        slot.receipt_status(received, Duration::from_secs(1)),
        ReceiptStatus::Canceled
    );
    assert_eq!(slot.last_response().unwrap().generation, 1);
    assert_eq!(slot.timing(received).collection_span_ns, Some(25_000_000));
    slot.record_response(ACCOUNT, 3, UnixNanos::from(90), received, received)
        .unwrap();
    assert_eq!(
        slot.receipt_status(received, Duration::from_secs(1)),
        ReceiptStatus::Recent
    );
    assert!(slot.failure().is_none());
    assert_eq!(slot.timing(received).collection_span_ns, Some(0));
}

#[rstest]
#[case(10, 9, "PAPI observation receipt precedes its request")]
#[case(-1, 11, "Out-of-order PAPI observation")]
fn test_invalid_request_timing_atomically_rejects_response(
    #[case] request_offset_ms: i64,
    #[case] receipt_offset_ms: u64,
    #[case] expected: &str,
) {
    let requested = Instant::now();
    let received = requested + Duration::from_millis(10);
    let mut slot = ObservationSlot::new(
        AccountId::from("BINANCE-PAPI-001"),
        ObservationSource::Account,
    );
    slot.record_response(ACCOUNT, 1, UnixNanos::from(100), requested, received)
        .unwrap();
    let next_request = if request_offset_ms < 0 {
        requested - Duration::from_millis(request_offset_ms.unsigned_abs())
    } else {
        requested + Duration::from_millis(request_offset_ms.unsigned_abs())
    };
    let next_receipt = requested + Duration::from_millis(receipt_offset_ms);
    let e = slot
        .record_response(ACCOUNT, 2, UnixNanos::from(200), next_request, next_receipt)
        .unwrap_err();
    assert_eq!(e.to_string(), expected);
    assert_eq!(slot.last_response().unwrap().generation, 1);
    assert_eq!(slot.timing(received).collection_span_ns, Some(10_000_000));
    assert_eq!(slot.timing(received).receipt_age_ns, Some(0));
    let failure = serde_json::to_value(slot.failure()).unwrap();
    assert_eq!(failure["state"], "failed");
    assert_eq!(failure["reason"], expected);
}
