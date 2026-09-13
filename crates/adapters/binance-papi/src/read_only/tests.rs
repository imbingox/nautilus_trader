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

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use nautilus_model::{
    enums::{OrderStatus, PositionSide},
    identifiers::InstrumentId,
};
use rstest::rstest;
use serde_json::{Value, json};

use super::*;
use crate::{
    http::error::PapiHttpError,
    reports::parse::PapiCommissionError,
    testing::{self, MockServer, Reply, TRADE_TIME, ms},
};

#[tokio::test]
async fn test_dropped_refresh_marks_every_unread_source_failed() {
    let delay = Arc::new(AtomicBool::new(false));
    let trigger = Arc::clone(&delay);
    let server = MockServer::new(move |request| {
        let mut reply = testing::quiet(request);

        if request.path == "/papi/v1/balance" && trigger.load(Ordering::Acquire) {
            reply.delay = Duration::from_secs(10);
        }

        reply
    })
    .await;
    let client = testing::client(&server, &["BTCUSDT"]);
    client.refresh_account_observations().await.unwrap();
    delay.store(true, Ordering::Release);
    let refreshing = client.clone();

    // This task must stay on the test runtime with the mock server
    let task = tokio::spawn(async move { refreshing.refresh_account_observations().await }); // tokio-import-ok
    server.wait_for_requests(5).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let retained: Value = serde_json::from_str(
        &client
            .account_observations_json(Duration::from_secs(30))
            .unwrap(),
    )
    .unwrap();

    for source in retained.as_array().unwrap() {
        assert_eq!(source["receipt_status"], "failed");
        assert_eq!(source["observation"]["generation"], 1);
    }

    delay.store(false, Ordering::Release);
    client.refresh_account_observations().await.unwrap();
    let recovered: Value = serde_json::from_str(
        &client
            .account_observations_json(Duration::from_secs(30))
            .unwrap(),
    )
    .unwrap();
    assert!(recovered.as_array().unwrap().iter().all(|source| {
        source["receipt_status"] == "recent" && source["observation"]["generation"] == 3
    }));
}

#[tokio::test]
async fn test_snapshot_json_retains_bounds_coverage_and_safe_request_metadata() {
    let server = MockServer::new(testing::quiet).await;
    let client = testing::client(&server, &["BTCUSDT"]);
    let snapshot = client
        .generate_mass_status(ms(TRADE_TIME - 2_000), ms(TRADE_TIME))
        .await
        .unwrap();
    let serialized = snapshot.to_json().unwrap();
    let evidence: Value = serde_json::from_str(&serialized).unwrap();

    assert_eq!(evidence["window_end"], TRADE_TIME * 1_000_000);
    assert_eq!(evidence["mass_status"]["reports_complete"], false);
    assert_eq!(
        evidence["mass_status"]["lookback_start"],
        (TRADE_TIME - 2_000) * 1_000_000
    );
    assert_eq!(evidence["instrument_ids"], json!(["BTCUSDT-PERP.BINANCE"]));
    assert!(!evidence["issues"].as_array().unwrap().is_empty());
    assert!(
        evidence["responses"]
            .as_array()
            .unwrap()
            .iter()
            .all(|response| {
                response["status"] == 200
                    && response["endpoint"].as_str().unwrap().starts_with("/papi/")
            })
    );
    assert!(!serialized.contains(testing::API_KEY));
    assert!(!serialized.contains(testing::API_SECRET));
    assert!(!serialized.contains("signature"));
}

#[tokio::test]
async fn test_partial_account_refresh_retains_failed_source_and_updates_other_sources() {
    let fail = Arc::new(AtomicBool::new(false));
    let trigger = Arc::clone(&fail);
    let server = MockServer::new(move |request| {
        if request.path == "/papi/v1/account" && trigger.load(Ordering::Acquire) {
            Reply::raw(503, "{}")
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let client = testing::client(&server, &["BTCUSDT"]);
    client.refresh_account_observations().await.unwrap();
    let first: Value = serde_json::from_str(
        &client
            .account_observations_json(Duration::from_secs(30))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(first[1]["receipt_status"], "recent");
    fail.store(true, Ordering::Release);
    assert!(client.refresh_account_observations().await.is_err());
    let next: Value = serde_json::from_str(
        &client
            .account_observations_json(Duration::from_secs(30))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(next[1]["receipt_status"], "failed");
    assert_eq!(next[1]["observation"], first[1]["observation"]);
    assert_eq!(next[0]["observation"]["generation"], 2);
    assert_eq!(next[0]["receipt_status"], "recent");
    assert_eq!(next[2]["observation"]["generation"], 2);
    assert_eq!(next[3]["observation"]["generation"], 2);
    assert_eq!(next[3]["endpoint"], "/papi/v2/um/account");
}

#[rstest]
#[case("http://example.com")]
#[case("https://user:secret@example.com")]
#[case("https://example.com/?signature=secret")]
#[case("https://example.com/private")]
fn test_invalid_origins_fail_before_transport(#[case] url: &str) {
    let config = testing::config(url);
    let e =
        BinancePapiReadOnlyClient::new(&config, vec![testing::instrument("BTCUSDT")]).unwrap_err();
    assert!(!e.to_string().contains("secret"));
}

#[rstest]
fn test_construction_requires_exact_scope_and_redacts_credentials() {
    let config = testing::config("https://papi.binance.com");
    let rendered = format!("{config:?}");
    assert!(!rendered.contains(testing::API_KEY));
    assert!(!rendered.contains(testing::API_SECRET));
    assert!(BinancePapiReadOnlyClient::new(&config, vec![]).is_err());
    assert!(
        BinancePapiReadOnlyClient::new(&config, vec![testing::instrument("BTCUSDT"); 2]).is_err()
    );
    let client =
        BinancePapiReadOnlyClient::new(&config, vec![testing::instrument("BTCUSDT")]).unwrap();
    assert!(!format!("{client:?}").contains(testing::API_KEY));
}

#[rstest]
#[case(json!({"dualSidePosition": true}))]
#[case(json!({"dualSidePosition": null}))]
#[case(json!({}))]
#[case(json!([false]))]
#[tokio::test]
async fn test_position_mode_must_be_explicitly_one_way(#[case] mode: Value) {
    let server = MockServer::new(move |request| {
        if request.path.ends_with("/positionSide/dual") {
            Reply::json(&mode)
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let client = testing::client(&server, &["BTCUSDT"]);
    assert!(client.generate_position_status_reports(None).await.is_err());
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn test_absent_position_is_an_error_and_explicit_flat_is_a_report() {
    let missing = Arc::new(AtomicBool::new(true));
    let state = Arc::clone(&missing);
    let server = MockServer::new(move |request| {
        if request.path.ends_with("/positionRisk") && state.load(Ordering::Acquire) {
            Reply::json(&json!([]))
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let client = testing::client(&server, &["BTCUSDT"]);
    assert!(client.generate_position_status_reports(None).await.is_err());
    missing.store(false, Ordering::Release);
    let reports = client.generate_position_status_reports(None).await.unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].position_side, PositionSide::Flat);
    assert_eq!(
        reports[0].quantity.as_decimal(),
        rust_decimal_macros::dec!(0)
    );
}

#[tokio::test]
async fn test_client_order_lookup_without_venue_id_and_mismatch_detection() {
    let server = MockServer::new(|request| {
        if request.path == "/papi/v1/um/order" {
            Reply::json(&testing::order())
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let client = testing::client(&server, &["BTCUSDT"]);
    let instrument = InstrumentId::from("BTCUSDT-PERP.BINANCE");
    let report = client
        .generate_order_status_report(instrument, None, Some("abc".into()))
        .await
        .unwrap();
    assert_eq!(report.venue_order_id.as_str(), "PAPI:O:BTCUSDT:270093109");
    let requests = server.requests();
    assert_eq!(requests[1].params["origClientOrderId"], "abc");
    assert!(!requests[1].params.contains_key("orderId"));
    assert!(
        client
            .generate_order_status_report(instrument, Some("PAPI:O:BTCUSDT:99".into()), None)
            .await
            .is_err()
    );
}

#[rstest]
#[case(400, -2013)]
#[case(404, -2013)]
#[case(401, -2015)]
#[tokio::test]
async fn test_failed_or_expired_lookup_never_returns_absence(
    #[case] status: u16,
    #[case] code: i64,
) {
    let server = MockServer::new(move |request| {
        if request.path == "/papi/v1/um/order" {
            Reply::raw(status, format!(r#"{{"code":{code},"msg":"missing"}}"#))
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let client = testing::client(&server, &["BTCUSDT"]);
    assert!(
        client
            .generate_order_status_report(
                InstrumentId::from("BTCUSDT-PERP.BINANCE"),
                Some("PAPI:O:BTCUSDT:99".into()),
                None,
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_mass_status_scans_closed_only_symbols_and_declares_unverified_history() {
    let server = MockServer::new(|request| {
        if request.params.get("symbol").map(String::as_str) == Some("BTCUSDT") {
            match request.path.as_str() {
                "/papi/v1/um/allOrders" => return Reply::json(&json!([testing::filled_order()])),
                "/papi/v1/um/userTrades" => return Reply::json(&json!([testing::trade()])),
                _ => {}
            }
        }
        testing::quiet(request)
    })
    .await;
    let client = testing::client(&server, &["BTCUSDT", "BNBUSDT"]);
    let start = ms(TRADE_TIME - 2_000);
    let snapshot = client
        .generate_mass_status(start, ms(TRADE_TIME + 1))
        .await
        .unwrap();
    assert!(!snapshot.mass_status.reports_complete());
    assert_eq!(snapshot.mass_status.lookback_start(), Some(start));
    assert_eq!(snapshot.mass_status.order_reports().len(), 1);
    assert_eq!(
        snapshot
            .mass_status
            .fill_reports()
            .values()
            .map(Vec::len)
            .sum::<usize>(),
        1
    );
    assert_eq!(snapshot.mass_status.position_reports().len(), 2);
    assert_eq!(snapshot.instrument_ids.len(), 2);
    assert_eq!(snapshot.issues.len(), 1);

    for symbol in ["BTCUSDT", "BNBUSDT"] {
        for path in [
            "/papi/v1/um/allOrders",
            "/papi/v1/um/algo/allAlgoOrders",
            "/papi/v1/um/userTrades",
        ] {
            assert!(
                server
                    .requests()
                    .iter()
                    .any(|r| r.path == path && r.params["symbol"] == symbol)
            );
        }
    }
}

#[tokio::test]
async fn test_old_active_order_ignores_history_lower_bound() {
    let server = MockServer::new(|request| {
        if request.path == "/papi/v1/um/openOrders" {
            Reply::json(&json!([testing::order()]))
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let client = testing::client(&server, &["BTCUSDT"]);
    let start = ms(TRADE_TIME);
    let snapshot = client
        .generate_mass_status(start, ms(TRADE_TIME + 1))
        .await
        .unwrap();
    let reports = snapshot.mass_status.order_reports();
    assert_eq!(reports.len(), 1);
    assert!(reports.values().next().unwrap().ts_accepted < start);
    assert!(
        server
            .requests()
            .iter()
            .filter(|r| r.path.ends_with("/openOrders"))
            .all(|r| !r.params.contains_key("startTime"))
    );
}

#[tokio::test]
async fn test_fills_backfill_orders_older_than_the_window() {
    let server = MockServer::new(|request| match request.path.as_str() {
        "/papi/v1/um/userTrades" => Reply::json(&json!([testing::trade()])),
        "/papi/v1/um/order" => {
            let mut row = testing::filled_order();
            row["time"] = json!(TRADE_TIME - 86_400_000);
            Reply::json(&row)
        }
        _ => testing::quiet(request),
    })
    .await;
    let client = testing::client(&server, &["BTCUSDT"]);
    let snapshot = client
        .generate_mass_status(ms(TRADE_TIME - 1), ms(TRADE_TIME))
        .await
        .unwrap();
    assert_eq!(snapshot.mass_status.order_reports().len(), 1);
    assert_eq!(snapshot.mass_status.fill_reports().len(), 1);
    let request = server
        .requests()
        .into_iter()
        .find(|r| r.path == "/papi/v1/um/order")
        .unwrap();
    assert_eq!(request.params["orderId"], "270093109");
    assert!(!request.params.contains_key("startTime"));
}

#[tokio::test]
async fn test_missing_linked_order_fails_instead_of_inventing_a_lifecycle() {
    let server = MockServer::new(|request| {
        if request.path == "/papi/v1/um/userTrades" {
            Reply::json(&json!([testing::trade()]))
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let client = testing::client(&server, &["BTCUSDT"]);
    assert!(
        client
            .generate_mass_status(ms(TRADE_TIME - 1), ms(TRADE_TIME))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_history_failure_preserves_current_reports_with_explicit_incompleteness() {
    let server = MockServer::new(|request| match request.path.as_str() {
        "/papi/v1/um/openOrders" => Reply::json(&json!([testing::order()])),
        "/papi/v1/um/allOrders" => Reply::raw(503, "{}"),
        _ => testing::quiet(request),
    })
    .await;
    let snapshot = testing::client(&server, &["BTCUSDT"])
        .generate_mass_status(ms(TRADE_TIME - 2_000), ms(TRADE_TIME))
        .await
        .unwrap();
    assert!(!snapshot.mass_status.reports_complete());
    assert_eq!(snapshot.mass_status.order_reports().len(), 1);
    assert_eq!(snapshot.issues.len(), 2);
    assert!(snapshot.issues[1].contains("/papi/v1/um/allOrders"));
}

#[tokio::test]
async fn test_commission_failure_is_fatal_even_with_other_incomplete_history() {
    let server = MockServer::new(|request| match request.path.as_str() {
        "/papi/v1/um/allOrders" => Reply::raw(503, "{}"),
        "/papi/v1/um/userTrades" => {
            let mut trade = testing::trade();
            trade.as_object_mut().unwrap().remove("commission");
            Reply::json(&json!([trade]))
        }
        _ => testing::quiet(request),
    })
    .await;
    let e = testing::client(&server, &["BTCUSDT"])
        .generate_mass_status(ms(TRADE_TIME - 2_000), ms(TRADE_TIME))
        .await
        .unwrap_err();
    assert!(e.is::<PapiCommissionError>());
}

#[tokio::test]
async fn test_current_source_failure_and_cancellation_cannot_return_empty_success() {
    let server = MockServer::new(|request| {
        if request.path == "/papi/v1/um/openOrders" {
            Reply::raw(503, "{}")
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let client = testing::client(&server, &["BTCUSDT"]);
    assert!(
        client
            .generate_open_order_status_reports(None)
            .await
            .is_err()
    );
    let clone = client.clone();
    clone.cancel();
    let before = server.requests().len();
    let e = client
        .generate_position_status_reports(None)
        .await
        .unwrap_err();
    assert_eq!(
        e.downcast_ref::<PapiHttpError>(),
        Some(&PapiHttpError::Canceled)
    );
    assert_eq!(server.requests().len(), before);
}

#[tokio::test]
async fn test_full_pages_are_bisected_and_deduplicated_without_timestamp_gaps() {
    let start_ms = TRADE_TIME - 10_000;
    let server = MockServer::new(move |request| {
        if request.path == "/papi/v1/um/allOrders" {
            let lo: u64 = request.params["startTime"].parse().unwrap();
            let hi: u64 = request.params["endTime"].parse().unwrap();
            let rows: Vec<_> = (0..1_001_u64)
                .filter(|i| (lo..=hi).contains(&(start_ms + i)))
                .take(1_000)
                .map(|i| {
                    let mut row = testing::order();
                    row["orderId"] = json!(i + 1);
                    row["clientOrderId"] = json!(format!("page-{i}"));
                    row["status"] = json!("CANCELED");
                    row["time"] = json!(start_ms + i);
                    row["updateTime"] = row["time"].clone();
                    row
                })
                .collect();
            Reply::json(&json!(rows))
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let snapshot = testing::client(&server, &["BTCUSDT"])
        .generate_mass_status(ms(start_ms), ms(start_ms + 1_000))
        .await
        .unwrap();
    assert_eq!(snapshot.mass_status.order_reports().len(), 1_001);
    assert_eq!(snapshot.issues.len(), 1);
    let requests: Vec<_> = server
        .requests()
        .into_iter()
        .filter(|r| r.path == "/papi/v1/um/allOrders")
        .collect();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[1].params["startTime"], start_ms.to_string());
    assert_eq!(
        requests[2].params["endTime"],
        (start_ms + 1_000).to_string()
    );
    assert_eq!(
        requests[1].params["endTime"].parse::<u64>().unwrap() + 1,
        requests[2].params["startTime"].parse::<u64>().unwrap()
    );
}

#[tokio::test]
async fn test_single_millisecond_saturation_is_explicitly_incomplete() {
    let server = MockServer::new(|request| {
        if request.path == "/papi/v1/um/allOrders" {
            let rows: Vec<_> = (1..=1_000)
                .map(|id| {
                    let mut row = testing::order();
                    row["orderId"] = json!(id);
                    row["clientOrderId"] = json!(format!("saturated-{id}"));
                    row["time"] = json!(TRADE_TIME);
                    row["updateTime"] = json!(TRADE_TIME);
                    row
                })
                .collect();
            Reply::json(&json!(rows))
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let snapshot = testing::client(&server, &["BTCUSDT"])
        .generate_mass_status(ms(TRADE_TIME), ms(TRADE_TIME))
        .await
        .unwrap();
    assert!(!snapshot.mass_status.reports_complete());
    assert!(
        snapshot
            .issues
            .iter()
            .any(|s| s.contains("saturated millisecond"))
    );
}

#[tokio::test]
async fn test_large_windows_use_fixed_subwindows_shorter_than_seven_days() {
    let server = MockServer::new(testing::quiet).await;
    let end = TRADE_TIME;
    let start = end - 8 * 86_400_000;
    testing::client(&server, &["BTCUSDT"])
        .generate_mass_status(ms(start), ms(end))
        .await
        .unwrap();
    let requests: Vec<_> = server
        .requests()
        .into_iter()
        .filter(|r| r.path == "/papi/v1/um/userTrades")
        .collect();
    assert_eq!(requests.len(), 2);

    for request in &requests {
        assert!(!request.params.contains_key("fromId"));
        assert!(
            request.params["endTime"].parse::<u64>().unwrap()
                - request.params["startTime"].parse::<u64>().unwrap()
                < 7 * 86_400_000
        );
        assert_eq!(request.params["limit"], "1000");
    }

    assert_eq!(
        requests[0].params["endTime"].parse::<u64>().unwrap() + 1,
        requests[1].params["startTime"].parse::<u64>().unwrap()
    );
}

#[tokio::test]
async fn test_exact_nanosecond_history_filter_excludes_enclosing_millisecond() {
    let server = MockServer::new(|request| {
        if request.path == "/papi/v1/um/userTrades" {
            Reply::json(&json!([testing::trade()]))
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let snapshot = testing::client(&server, &["BTCUSDT"])
        .generate_mass_status(
            UnixNanos::from(ms(TRADE_TIME).as_u64() + 1),
            ms(TRADE_TIME + 1),
        )
        .await
        .unwrap();
    assert!(snapshot.mass_status.fill_reports().is_empty());
    assert!(
        !server
            .requests()
            .iter()
            .any(|r| r.path == "/papi/v1/um/order")
    );
}

#[tokio::test]
async fn test_request_and_row_budgets_bound_the_entire_collection() {
    let server = MockServer::new(testing::quiet).await;
    let mut config = testing::config(&server.url);
    config.max_requests = 3;
    let client = BinancePapiReadOnlyClient::from_parts(
        &config,
        vec![testing::instrument("BTCUSDT")],
        testing::gate(),
        Arc::new(AtomicTime::new(false, ms(1_800_000_000_000))),
    )
    .unwrap();
    let e = client
        .generate_mass_status(ms(TRADE_TIME - 2_000), ms(TRADE_TIME))
        .await
        .unwrap_err();
    assert_eq!(
        e.downcast_ref::<PapiHttpError>(),
        Some(&PapiHttpError::Budget)
    );
    assert_eq!(server.requests().len(), 3);
}

#[rstest]
#[case(true)]
#[case(false)]
#[tokio::test]
async fn test_client_order_ids_must_be_unique_across_symbols(#[case] mass_status: bool) {
    let server = MockServer::new(|request| {
        if request.path == "/papi/v1/um/openOrders" {
            let mut row = testing::order();
            row["symbol"] = json!(request.params["symbol"]);
            Reply::json(&json!([row]))
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let client = testing::client(&server, &["BTCUSDT", "ETHUSDT"]);
    let result = if mass_status {
        client
            .generate_mass_status(ms(TRADE_TIME - 2_000), ms(TRADE_TIME))
            .await
            .map(|_| ())
    } else {
        client
            .generate_open_order_status_reports(None)
            .await
            .map(|_| ())
    };
    assert!(result.is_err());
}

#[tokio::test]
async fn test_same_numeric_order_ids_remain_distinct_across_symbols_and_families() {
    let server = MockServer::new(|request| match request.path.as_str() {
        "/papi/v1/um/openOrders" => {
            let mut row = testing::order();
            row["symbol"] = json!(request.params["symbol"]);
            row["orderId"] = json!(42);
            row["clientOrderId"] = json!(format!("ordinary-{}", request.params["symbol"]));
            Reply::json(&json!([row]))
        }
        "/papi/v1/um/algo/openAlgoOrders" => {
            let mut row = testing::algo();
            row["symbol"] = json!(request.params["symbol"]);
            row["algoId"] = json!(42);
            row["clientAlgoId"] = json!(format!("algo-{}", request.params["symbol"]));
            Reply::json(&json!([row]))
        }
        _ => testing::quiet(request),
    })
    .await;
    let snapshot = testing::client(&server, &["BTCUSDT", "ETHUSDT"])
        .generate_mass_status(ms(TRADE_TIME - 2_000), ms(TRADE_TIME))
        .await
        .unwrap();
    let reports = snapshot.mass_status.order_reports();
    assert_eq!(reports.len(), 4);

    for symbol in ["BTCUSDT", "ETHUSDT"] {
        for family in ["O", "A"] {
            assert!(
                reports
                    .keys()
                    .any(|id| id.as_str() == format!("PAPI:{family}:{symbol}:42"))
            );
        }
    }
}

#[rstest]
#[case("FINISHED", "FILLED", "0.010", OrderStatus::Filled)]
#[case("FINISHED", "CANCELED", "0.005", OrderStatus::Canceled)]
#[case("TRIGGERED", "PARTIALLY_FILLED", "0.005", OrderStatus::PartiallyFilled)]
#[tokio::test]
async fn test_algo_parent_child_and_fills_form_one_lifecycle(
    #[case] algo_status: &'static str,
    #[case] child_status: &'static str,
    #[case] filled: &'static str,
    #[case] expected: OrderStatus,
) {
    let server = MockServer::new(move |request| {
        let (mut parent, mut child) = testing::triggered_algo();
        parent["algoStatus"] = json!(algo_status);
        child["status"] = json!(child_status);
        child["executedQty"] = json!(filled);
        match request.path.as_str() {
            "/papi/v1/um/algo/allAlgoOrders" => Reply::json(&json!([parent])),
            "/papi/v1/um/allOrders" => Reply::json(&json!([child])),
            "/papi/v1/um/order" => Reply::json(&child),
            "/papi/v1/um/userTrades" => {
                let mut trade = testing::trade();
                trade["qty"] = json!(filled);
                Reply::json(&json!([trade]))
            }
            _ => testing::quiet(request),
        }
    })
    .await;
    let snapshot = testing::client(&server, &["BTCUSDT"])
        .generate_mass_status(ms(TRADE_TIME - 2_000), ms(TRADE_TIME))
        .await
        .unwrap();
    let orders = snapshot.mass_status.order_reports();
    let fills = snapshot.mass_status.fill_reports();
    assert_eq!(orders.len(), 1);
    assert_eq!(fills.len(), 1);
    let order = orders.values().next().unwrap();
    let fill = &fills[&order.venue_order_id][0];
    assert_eq!(order.venue_order_id.as_str(), "PAPI:A:BTCUSDT:2146760");
    assert_eq!(order.order_status, expected);
    assert_eq!(
        order.client_order_id.unwrap().as_str(),
        "6B2I9XVcJpCjqPAJ4YoFX7"
    );
    assert_eq!(order.filled_qty.as_decimal(), filled.parse().unwrap());
    assert_eq!(order.ts_triggered, Some(ms(TRADE_TIME - 750)));
    assert_eq!(fill.venue_order_id, order.venue_order_id);
    assert_eq!(fill.client_order_id, order.client_order_id);
    assert_eq!(fill.last_qty, order.filled_qty);
    assert_eq!(fills[&order.venue_order_id].len(), 1);
    assert!(!snapshot.mass_status.reports_complete());
}

#[tokio::test]
async fn test_open_algo_fetches_parent_link_and_child_without_a_second_order() {
    let server = MockServer::new(|request| {
        let (mut parent, mut child) = testing::triggered_algo();
        parent["algoStatus"] = json!("TRIGGERED");
        child["status"] = json!("NEW");
        child["executedQty"] = json!("0");
        child["avgPrice"] = json!("0");

        match request.path.as_str() {
            "/papi/v1/um/algo/openAlgoOrders" => {
                parent.as_object_mut().unwrap().remove("actualOrderId");
                Reply::json(&json!([parent]))
            }
            "/papi/v1/um/algo/allAlgoOrders" => Reply::json(&json!([parent])),
            "/papi/v1/um/openOrders" => Reply::json(&json!([child])),
            "/papi/v1/um/order" => Reply::json(&child),
            _ => testing::quiet(request),
        }
    })
    .await;
    let reports = testing::client(&server, &["BTCUSDT"])
        .generate_open_order_status_reports(None)
        .await
        .unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].venue_order_id.as_str(), "PAPI:A:BTCUSDT:2146760");
    assert_eq!(reports[0].order_status, OrderStatus::Accepted);
    assert_eq!(
        reports[0].filled_qty.as_decimal(),
        rust_decimal_macros::dec!(0)
    );
    let requests = server.requests();
    let parent = requests
        .iter()
        .find(|r| r.path == "/papi/v1/um/algo/allAlgoOrders")
        .unwrap();
    assert_eq!(parent.params["algoId"], "2146760");
    assert!(!parent.params.contains_key("startTime"));
    assert!(
        requests
            .iter()
            .any(|r| r.path == "/papi/v1/um/order" && r.params["orderId"] == "270093109")
    );
}

#[rstest]
#[case("missing_child")]
#[case("mismatched_child")]
#[case("shared_child")]
#[case("fill_before_trigger")]
#[case("fill_after_child_update")]
#[case("excess_fill_quantity")]
#[tokio::test]
async fn test_contradictory_algo_lifecycles_fail(#[case] scenario: &'static str) {
    let server = MockServer::new(move |request| {
        let (parent, mut child) = testing::triggered_algo();

        match request.path.as_str() {
            "/papi/v1/um/algo/allAlgoOrders" => {
                if scenario == "shared_child" {
                    let mut second = parent.clone();
                    second["algoId"] = json!(2146761);
                    second["clientAlgoId"] = json!("another-parent");
                    Reply::json(&json!([parent, second]))
                } else {
                    Reply::json(&json!([parent]))
                }
            }
            "/papi/v1/um/order" if scenario != "missing_child" => {
                if scenario == "mismatched_child" {
                    child["orderId"] = json!(99);
                }

                if scenario == "fill_after_child_update" {
                    child["updateTime"] = json!(TRADE_TIME - 1);
                }
                Reply::json(&child)
            }
            "/papi/v1/um/userTrades" => {
                let mut trade = testing::trade();
                if scenario == "fill_before_trigger" {
                    trade["time"] = json!(TRADE_TIME - 1_000);
                }

                if scenario == "excess_fill_quantity" {
                    trade["qty"] = json!("0.020");
                }
                Reply::json(&json!([trade]))
            }
            _ => testing::quiet(request),
        }
    })
    .await;
    assert!(
        testing::client(&server, &["BTCUSDT"])
            .generate_mass_status(ms(TRADE_TIME - 2_000), ms(TRADE_TIME))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_invalid_scope_fails_before_any_request() {
    let server = MockServer::new(testing::quiet).await;
    let client = testing::client(&server, &["BTCUSDT"]);
    let outside = Some(InstrumentId::from("ETHUSDT-PERP.BINANCE"));
    assert!(
        client
            .generate_open_order_status_reports(outside)
            .await
            .is_err()
    );
    assert!(
        client
            .generate_position_status_reports(outside)
            .await
            .is_err()
    );
    assert!(server.requests().is_empty());
}

#[rstest]
#[case(json!([]))]
#[case(json!({}))]
#[case(json!([testing::position("BTCUSDT"), testing::position("BTCUSDT")]))]
#[case(json!([testing::position("ETHUSDT")]))]
#[case(json!([{"symbol":"BTCUSDT", "positionSide":"LONG", "positionAmt":"1", "entryPrice":"28511", "updateTime":TRADE_TIME}]))]
#[tokio::test]
async fn test_invalid_position_coverage_never_synthesizes_flat(#[case] response: Value) {
    let server = MockServer::new(move |request| {
        if request.path == "/papi/v1/um/positionRisk" {
            Reply::json(&response)
        } else {
            testing::quiet(request)
        }
    })
    .await;
    assert!(
        testing::client(&server, &["BTCUSDT"])
            .generate_position_status_reports(None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_duplicate_history_identity_is_fatal() {
    let server = MockServer::new(|request| {
        if request.path == "/papi/v1/um/allOrders" {
            Reply::json(&json!([testing::order(), testing::order()]))
        } else {
            testing::quiet(request)
        }
    })
    .await;
    assert!(
        testing::client(&server, &["BTCUSDT"])
            .generate_mass_status(ms(TRADE_TIME - 2_000), ms(TRADE_TIME))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_nonprogressing_pages_end_with_explicit_coverage_failure() {
    let server = MockServer::new(|request| {
        if request.path == "/papi/v1/um/allOrders" {
            let rows: Vec<_> = (1..=1_000)
                .map(|id| {
                    let mut row = testing::order();
                    row["orderId"] = json!(id);
                    row["clientOrderId"] = json!(format!("stuck-{id}"));
                    row["time"] = json!(TRADE_TIME);
                    row["updateTime"] = json!(TRADE_TIME);
                    row
                })
                .collect();
            Reply::json(&json!(rows))
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let snapshot = testing::client(&server, &["BTCUSDT"])
        .generate_mass_status(ms(TRADE_TIME - 1), ms(TRADE_TIME))
        .await
        .unwrap();
    assert!(!snapshot.mass_status.reports_complete());
    assert!(
        snapshot
            .issues
            .iter()
            .any(|issue| issue.contains("nonprogressing page"))
    );
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|r| r.path == "/papi/v1/um/allOrders")
            .count(),
        2
    );
}

#[tokio::test]
async fn test_row_budget_cannot_return_a_truncated_success() {
    let server = MockServer::new(|request| {
        if request.path == "/papi/v1/um/openOrders" {
            Reply::json(&json!([testing::order()]))
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let mut config = testing::config(&server.url);
    config.max_rows = 1;
    let client = BinancePapiReadOnlyClient::from_parts(
        &config,
        vec![testing::instrument("BTCUSDT")],
        testing::gate(),
        Arc::new(AtomicTime::new(false, ms(1_800_000_000_000))),
    )
    .unwrap();
    let e = client
        .generate_mass_status(ms(TRADE_TIME - 2_000), ms(TRADE_TIME))
        .await
        .unwrap_err();
    assert_eq!(
        e.downcast_ref::<PapiHttpError>(),
        Some(&PapiHttpError::Budget)
    );
    assert_eq!(server.requests().len(), 3);
}
