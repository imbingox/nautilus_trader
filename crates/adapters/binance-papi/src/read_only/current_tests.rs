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

//! Synthetic account-wide scenarios derived from the Binance and PAPI documented fixtures.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use nautilus_model::{
    enums::{OrderStatus, PositionSide},
    identifiers::InstrumentId,
};
use rstest::rstest;
use rust_decimal_macros::dec;
use serde_json::{Value, json};

use super::BinancePapiReadOnlyClient;
use crate::{
    http::{error::PapiHttpError, query::PapiRequest},
    observations::ObservationSource,
    testing::{self, MockServer, Reply, TRADE_TIME},
};

fn exchange_info(symbols: &[&str]) -> Value {
    let mut info: Value = serde_json::from_str(include_str!(
        "../../../binance/test_data/futures/http_json/exchange_info_usdm.json"
    ))
    .unwrap();
    let definition = info["symbols"][0].clone();
    info["symbols"] = symbols
        .iter()
        .map(|symbol| {
            let mut row = definition.clone();
            row["symbol"] = json!(symbol);
            row["pair"] = json!(symbol);
            row["baseAsset"] = json!(symbol.strip_suffix("USDT").unwrap());
            row
        })
        .collect();
    info
}

fn position(symbol: &str) -> Value {
    let mut row = testing::position(symbol);
    row["positionAmt"] = json!("-0.125");
    row["entryPrice"] = json!("28511.00");
    row
}

#[tokio::test]
async fn account_positions_discover_symbols_and_refresh_metadata_without_expanding_recovery_scope()
{
    let generation = Arc::new(AtomicUsize::new(0));
    let state = Arc::clone(&generation);
    let server = MockServer::new(move |request| match request.path.as_str() {
        "/papi/v1/um/positionRisk" if !request.params.contains_key("symbol") => {
            let rows = match state.load(Ordering::Acquire) {
                0 => json!([position("BTCUSDT")]),
                1 => json!([position("BTCUSDT"), position("ETHUSDT")]),
                _ => json!([]),
            };
            Reply::json(&rows)
        }
        "/fapi/v1/exchangeInfo" => Reply::json(&exchange_info(&["BTCUSDT", "ETHUSDT"])),
        _ => testing::quiet(request),
    })
    .await;
    let client = testing::client(&server, &["BNBUSDT"]);
    let reports = client.generate_position_status_reports(None).await.unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0].instrument_id,
        InstrumentId::from("BTCUSDT-PERP.BINANCE")
    );
    assert_eq!(reports[0].position_side, PositionSide::Short);
    assert_eq!(reports[0].quantity.as_decimal(), dec!(0.125));
    assert_eq!(reports[0].avg_px_open, Some(dec!(28511.00)));
    client
        .clone()
        .generate_position_status_reports(None)
        .await
        .unwrap();
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|r| r.path == "/fapi/v1/exchangeInfo")
            .count(),
        1
    );
    generation.store(1, Ordering::Release);
    assert_eq!(
        client
            .generate_position_status_reports(None)
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|r| r.path == "/fapi/v1/exchangeInfo")
            .count(),
        2
    );
    generation.store(2, Ordering::Release);
    assert!(
        client
            .generate_position_status_reports(None)
            .await
            .unwrap()
            .is_empty()
    );
    let snapshot = client
        .generate_mass_status(testing::ms(TRADE_TIME - 1000), testing::ms(TRADE_TIME))
        .await
        .unwrap();
    assert_eq!(
        snapshot.instrument_ids,
        vec![InstrumentId::from("BNBUSDT-PERP.BINANCE")]
    );
    assert!(server.requests().iter().all(|r| r.method == "GET"));
    assert!(
        server
            .requests()
            .iter()
            .filter(|r| r.path == "/fapi/v1/exchangeInfo")
            .all(|r| r.api_key.is_none() && !r.params.contains_key("signature"))
    );
}

#[tokio::test]
async fn account_orders_include_external_ordinary_and_algo_orders_and_reflect_terminal_state() {
    let generation = Arc::new(AtomicUsize::new(0));
    let state = Arc::clone(&generation);
    let server = MockServer::new(move |request| {
        let ended = state.load(Ordering::Acquire) > 0;
        match request.path.as_str() {
            "/papi/v1/um/openOrders" => {
                assert!(!request.params.contains_key("symbol"));
                let mut order = testing::order();
                order["status"] = json!("PARTIALLY_FILLED");
                order["executedQty"] = json!("0.005");
                order["avgPrice"] = json!("28511.00");
                Reply::json(&if ended { json!([]) } else { json!([order]) })
            }
            "/papi/v1/um/algo/openAlgoOrders" => {
                assert!(!request.params.contains_key("symbol"));
                let algos: Value =
                    serde_json::from_str(include_str!("../../test_data/reports/open_algos.json"))
                        .unwrap();
                Reply::json(&if ended { json!([]) } else { algos })
            }
            "/fapi/v1/exchangeInfo" => Reply::json(&exchange_info(&["BTCUSDT", "BNBUSDT"])),
            _ => testing::quiet(request),
        }
    })
    .await;
    let client = testing::client(&server, &[]);
    let reports = client
        .generate_order_status_reports(None, true)
        .await
        .unwrap();
    assert_eq!(reports.len(), 2);
    let ordinary = reports
        .iter()
        .find(|r| r.instrument_id == InstrumentId::from("BTCUSDT-PERP.BINANCE"))
        .unwrap();
    assert_eq!(ordinary.order_status, OrderStatus::PartiallyFilled);
    assert_eq!(ordinary.filled_qty.as_decimal(), dec!(0.005));
    assert_eq!(ordinary.quantity.as_decimal(), dec!(0.010));
    assert_eq!(ordinary.venue_order_id.as_str(), "PAPI:O:BTCUSDT:270093109");
    let algo = reports
        .iter()
        .find(|r| r.instrument_id == InstrumentId::from("BNBUSDT-PERP.BINANCE"))
        .unwrap();
    assert_eq!(algo.venue_order_id.as_str(), "PAPI:A:BNBUSDT:2146760");
    assert_eq!(algo.order_status, OrderStatus::Accepted);
    generation.store(1, Ordering::Release);
    assert!(
        client
            .generate_order_status_reports(None, true)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn complete_empty_account_and_zero_positions_need_no_metadata() {
    let server = MockServer::new(|request| {
        if request.path == "/papi/v1/um/positionRisk" {
            return Reply::json(&json!([testing::position("UNKNOWNUSDT")]));
        }
        testing::quiet(request)
    })
    .await;
    let client = testing::client(&server, &[]);
    assert!(
        client
            .generate_position_status_reports(None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        client
            .generate_order_status_reports(None, true)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        server
            .requests()
            .iter()
            .all(|r| !r.path.contains("exchangeInfo") && !r.params.contains_key("symbol"))
    );
}

#[rstest]
#[case("/papi/v1/um/positionRisk", 403)]
#[case("/papi/v1/um/openOrders", 403)]
#[case("/papi/v1/um/algo/openAlgoOrders", 503)]
#[case("/papi/v1/um/openOrders", 429)]
#[tokio::test]
async fn failed_source_is_never_an_empty_or_partial_success(
    #[case] endpoint: &'static str,
    #[case] status: u16,
) {
    let server = MockServer::new(move |request| {
        if request.path == endpoint {
            Reply::raw(status, r#"{"code":-1003,"msg":"synthetic failure"}"#)
        } else if request.path == "/papi/v1/um/openOrders" {
            Reply::json(&json!([testing::order()]))
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let client = testing::client(&server, &["BTCUSDT"]);
    if endpoint.ends_with("positionRisk") {
        assert!(client.generate_position_status_reports(None).await.is_err());
    } else {
        assert!(
            client
                .generate_order_status_reports(None, true)
                .await
                .is_err()
        );
    }
}

#[rstest]
#[case(false)]
#[case(true)]
#[tokio::test]
async fn unresolved_active_metadata_returns_error(#[case] orders: bool) {
    let server = MockServer::new(move |request| match request.path.as_str() {
        "/papi/v1/um/positionRisk" => Reply::json(&json!([position("BTCUSDT")])),
        "/papi/v1/um/openOrders" => Reply::json(&json!([testing::order()])),
        "/fapi/v1/exchangeInfo" => Reply::json(&exchange_info(&["ETHUSDT"])),
        _ => testing::quiet(request),
    })
    .await;
    let client = testing::client(&server, &[]);
    let error = if orders {
        client
            .generate_order_status_reports(None, true)
            .await
            .unwrap_err()
    } else {
        client
            .generate_position_status_reports(None)
            .await
            .unwrap_err()
    };
    assert!(
        error
            .to_string()
            .contains("Unresolved PAPI instrument metadata for BTCUSDT")
    );
}

#[tokio::test]
async fn account_reads_enforce_request_and_row_budgets() {
    let server = MockServer::new(|request| {
        if request.path == "/papi/v1/um/positionRisk" {
            Reply::json(&json!([position("BTCUSDT"), position("ETHUSDT")]))
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let mut config = testing::config(&server.url);
    config.max_rows = 1;
    let client = BinancePapiReadOnlyClient::new(&config, vec![]).unwrap();
    let error = client
        .generate_position_status_reports(None)
        .await
        .unwrap_err();
    assert!(error.downcast_ref::<PapiHttpError>().is_some());
    config.max_requests = 1;
    let client = BinancePapiReadOnlyClient::new(&config, vec![]).unwrap();
    assert!(
        client
            .generate_order_status_reports(None, true)
            .await
            .is_err()
    );
    assert!(
        !server
            .requests()
            .iter()
            .any(|r| r.path == "/papi/v1/um/openOrders")
    );
}

#[rstest]
fn account_query_weights_and_parameters_match_endpoint_scope() {
    let positions = PapiRequest::Positions { symbol: None };
    assert!(!positions.params().contains_key("symbol"));
    assert_eq!(positions.weight(), 5);
    for source in [
        ObservationSource::UmOpenOrders,
        ObservationSource::UmOpenAlgos,
    ] {
        let request = PapiRequest::Observation(source);
        assert!(!request.params().contains_key("symbol"));
        assert_eq!(request.weight(), 40);
    }
}

#[tokio::test]
async fn account_positions_cover_more_than_the_historical_scope_limit() {
    let symbols: Vec<_> = (0..257).map(|index| format!("BTC{index}USDT")).collect();
    let rows: Vec<_> = symbols.iter().map(|symbol| position(symbol)).collect();
    let info = exchange_info(&symbols.iter().map(String::as_str).collect::<Vec<_>>());
    let server = MockServer::new(move |request| match request.path.as_str() {
        "/papi/v1/um/positionRisk" => Reply::json(&json!(rows)),
        "/fapi/v1/exchangeInfo" => Reply::json(&info),
        _ => testing::quiet(request),
    })
    .await;
    let reports = testing::client(&server, &[])
        .generate_position_status_reports(None)
        .await
        .unwrap();
    assert_eq!(reports.len(), 257);
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|r| r.path == "/papi/v1/um/positionRisk")
            .count(),
        1
    );
    assert!(
        server
            .requests()
            .iter()
            .all(|r| !r.params.contains_key("symbol"))
    );
}

#[rstest]
#[case(json!({}))]
#[case(json!([position("BTCUSDT"), position("BTCUSDT")]))]
#[case(json!([{"symbol": "BTCUSDT", "positionSide": "LONG", "positionAmt": "1", "entryPrice": "28511", "updateTime": TRADE_TIME}]))]
#[case(json!([{"symbol": "BTCUSDT", "positionSide": "BOTH", "positionAmt": "0.0001", "entryPrice": "28511", "updateTime": TRADE_TIME}]))]
#[tokio::test]
async fn invalid_account_positions_never_return_partial_success(#[case] rows: Value) {
    let server = MockServer::new(move |request| match request.path.as_str() {
        "/papi/v1/um/positionRisk" => Reply::json(&rows),
        "/fapi/v1/exchangeInfo" => Reply::json(&exchange_info(&["BTCUSDT"])),
        _ => testing::quiet(request),
    })
    .await;
    assert!(
        testing::client(&server, &[])
            .generate_position_status_reports(None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn explicit_current_instrument_loads_metadata_without_becoming_a_recovery_scope() {
    let server = MockServer::new(|request| match request.path.as_str() {
        "/fapi/v1/exchangeInfo" => Reply::json(&exchange_info(&["BTCUSDT"])),
        _ => testing::quiet(request),
    })
    .await;
    let client = testing::client(&server, &[]);
    let reports = client
        .generate_position_status_reports(Some(InstrumentId::from("BTCUSDT-PERP.BINANCE")))
        .await
        .unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].position_side, PositionSide::Flat);
    let count = server.requests().len();
    assert!(
        client
            .generate_mass_status(testing::ms(TRADE_TIME - 1000), testing::ms(TRADE_TIME))
            .await
            .is_err()
    );
    assert_eq!(server.requests().len(), count);
}
