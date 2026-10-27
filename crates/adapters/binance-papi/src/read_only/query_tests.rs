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
    identifiers::{InstrumentId, Symbol},
    instruments::InstrumentAny,
    types::Currency,
};
use rstest::rstest;
use serde_json::{Value, json};

use super::*;
use crate::testing::{self, MockServer, Reply, ms};

#[rstest]
#[case(vec!["BTCUSDT"])]
#[case(vec![])]
#[tokio::test]
async fn account_query_observes_positions_and_orders_outside_scope(#[case] symbols: Vec<&str>) {
    let server = MockServer::new(|request| match request.path.as_str() {
        "/papi/v1/um/account" | "/papi/v2/um/account" => Reply::json(&json!({
            "assets": [], "positions": [{
                "symbol": "ETHUSDT", "positionSide": "SHORT", "positionAmt": "-2.500",
                "leverage": "10", "unrealizedProfit": "-0.11000000", "updateTime": 1617939110373_i64,
            }],
        })),
        "/papi/v1/um/openOrders" | "/papi/v1/um/algo/openAlgoOrders" => {
            Reply::json(&json!([{"symbol": "ETHUSDT"}]))
        }
        _ => testing::supported_trading(request),
    }).await;
    let client = testing::client(&server, &symbols);
    let state = client.query_account_state().await.unwrap();
    let info = &state.info.as_ref().unwrap()["papi"];
    let snapshot: Value = serde_json::from_str(
        &client
            .account_snapshot_json(Duration::from_secs(30), Duration::from_secs(30))
            .unwrap(),
    )
    .unwrap();

    assert_eq!(info["portfolio_margin"]["actual_equity"], "1000");
    assert_eq!(info["position_metrics"].as_array().unwrap().len(), 2);

    for source in info["position_metrics"].as_array().unwrap() {
        assert_eq!(source["coverage"], "returned_positions");
        assert_eq!(source["generation"], 1);
        let receipt = info["sources"]
            .as_array()
            .unwrap()
            .iter()
            .find(|receipt| receipt["endpoint"] == source["source"])
            .unwrap();
        assert_eq!(source["ts_received"], receipt["ts_received"]);
        let row = &source["positions"][0];
        assert_eq!(row["symbol"], "ETHUSDT");
        assert_eq!(row["position_side"], "SHORT");
        assert_eq!(row["position_side_recognized"], true);
        assert_eq!(
            row["position_amount"],
            json!({"state": "value", "value": "-2.500"})
        );
        assert_eq!(row["leverage"], json!({"state": "value", "value": "10"}));
        assert_eq!(
            row["unrealized_pnl"],
            json!({"state": "value", "value": "-0.11000000"})
        );
        assert_eq!(row["metadata_status"], "missing");
        assert!(row["instrument_id"].is_null());
        assert!(row["unrealized_pnl_currency"].is_null());
        assert_eq!(row["ts_source"], 1_617_939_110_373_000_000_u64);
    }
    assert_eq!(snapshot["wallet"]["status"], "unsupported");
    assert_eq!(snapshot["trading_authorized"], false);
    assert_eq!(client.inner.scope.instrument_ids().len(), symbols.len());
    assert_eq!(server.requests().len(), 9);
    assert!(
        server
            .requests()
            .iter()
            .all(|request| request.path.starts_with("/papi/"))
    );
}

#[tokio::test]
async fn account_query_keeps_versions_rows_and_currencies_separate() {
    let server = MockServer::new(|request| match request.path.as_str() {
        "/papi/v1/um/account" => Reply::json(&json!({"assets": [], "positions": [
            {"symbol": "BTCUSDT", "positionSide": "BOTH", "positionAmt": "0.100",
             "leverage": "10", "unrealizedProfit": "0.11000000", "updateTime": 1000},
            {"symbol": "ETHUSDC", "positionSide": "LONG", "positionAmt": "2",
             "leverage": "5", "unrealizedProfit": "0.0000000000000000000000000001"},
            {"symbol": "BTCUSDT", "positionSide": "SHORT", "positionAmt": "0.000",
             "leverage": "20", "unrealizedProfit": "0.000", "updateTime": 0}
        ]})),
        "/papi/v2/um/account" => Reply::json(&json!({"assets": [], "positions": [
            {"symbol": "BTCUSDT", "positionSide": "BOTH", "positionAmt": "0.200",
             "unrealizedProfit": "0.22000000", "updateTime": 2000}
        ]})),
        _ => testing::supported_trading(request),
    })
    .await;
    let client = testing::client(&server, &["BTCUSDT"]);
    let InstrumentAny::CryptoPerpetual(mut eth) = testing::instrument("ETHUSDT") else {
        panic!()
    };
    eth.id = InstrumentId::from("ETHUSDC-PERP.BINANCE");
    eth.raw_symbol = Symbol::from("ETHUSDC");
    eth.quote_currency = Currency::USDC();
    eth.settlement_currency = Currency::USDC();
    let client = BinancePapiReadOnlyClient::from_parts(
        &testing::config(&server.url),
        vec![
            testing::instrument("BTCUSDT"),
            InstrumentAny::CryptoPerpetual(eth),
        ],
        testing::gate(),
        Arc::clone(&client.inner.clock),
    )
    .unwrap();
    let state = client.query_account_state().await.unwrap();
    let sources = &state.info.as_ref().unwrap()["papi"]["position_metrics"];
    let v1 = &sources[0];
    let v2 = &sources[1];
    assert_eq!(v1["source"], "/papi/v1/um/account");
    assert_eq!(v2["source"], "/papi/v2/um/account");
    assert_eq!(v1["positions"].as_array().unwrap().len(), 3);
    assert_eq!(v2["positions"].as_array().unwrap().len(), 1);
    assert_eq!(v1["positions"][0]["instrument_id"], "BTCUSDT-PERP.BINANCE");
    assert_eq!(v1["positions"][0]["unrealized_pnl_currency"], "USDT");
    assert_eq!(v1["positions"][0]["ts_source"], 1_000_000_000_u64);
    assert_eq!(v1["positions"][1]["metadata_status"], "available");
    assert_eq!(v1["positions"][1]["unrealized_pnl_currency"], "USDC");
    assert_eq!(
        v1["positions"][1]["unrealized_pnl"]["value"],
        "0.0000000000000000000000000001"
    );
    assert!(v1["positions"][1]["ts_source"].is_null());
    assert_eq!(v1["positions"][2]["position_amount"]["value"], "0.000");
    assert_eq!(v1["positions"][2]["ts_source"], 0);
    assert_eq!(v2["positions"][0]["leverage"], json!({"state": "missing"}));
    assert_eq!(v2["positions"][0]["position_amount"]["value"], "0.200");
    assert_eq!(v2["positions"][0]["unrealized_pnl"]["value"], "0.22000000");
    assert_eq!(v2["positions"][0]["ts_source"], 2_000_000_000_u64);
    assert_eq!(server.requests().len(), 9);
}

#[rstest]
#[case(json!({}), "missing")]
#[case(json!({"leverage": null, "unrealizedProfit": null, "positionAmt": null}), "null")]
#[case(json!({"leverage": "", "unrealizedProfit": "", "positionAmt": ""}), "empty")]
#[case(json!({"leverage": "NaN", "unrealizedProfit": "Infinity", "positionAmt": "bad"}), "invalid")]
#[case(json!({"leverage": 10, "unrealizedProfit": 0.1, "positionAmt": 1}), "invalid")]
#[case(json!({"leverage": "0", "unrealizedProfit": "79228162514264337593543950336", "positionAmt": "1e2"}), "invalid")]
#[case(json!({"leverage": "-10", "unrealizedProfit": "0.00000000000000000000000000001", "positionAmt": []}), "invalid")]
#[case(json!({"leverage": "1.5", "unrealizedProfit": {}, "positionAmt": false}), "invalid")]
#[tokio::test]
async fn account_query_marks_unavailable_metrics_without_reusing_values(
    #[case] fields: Value,
    #[case] expected: &str,
) {
    let partial = Arc::new(AtomicBool::new(false));
    let response_partial = Arc::clone(&partial);
    let server = MockServer::new(move |request| {
        if matches!(request.path.as_str(), "/papi/v1/um/account" | "/papi/v2/um/account") {
            let mut row = if response_partial.load(Ordering::Acquire) { fields.clone() } else {
                json!({"leverage": "10", "unrealizedProfit": "1.00", "positionAmt": "2.00", "updateTime": 1000})
            };
            row["symbol"] = json!("BTCUSDT");
            row["positionSide"] = json!("FUTURE_SIDE");
            return Reply::json(&json!({"assets": [], "positions": [row]}));
        }
        testing::supported_trading(request)
    }).await;
    let client = testing::client(&server, &["BTCUSDT"]);
    let first = client.query_account_state().await.unwrap();
    partial.store(true, Ordering::Release);
    client.inner.clock.set_time(ms(1_800_000_001_000));
    let second = client.query_account_state().await.unwrap();
    let prior = &first.info.as_ref().unwrap()["papi"]["position_metrics"][0];
    let current = &second.info.as_ref().unwrap()["papi"]["position_metrics"][0];
    assert_eq!(prior["positions"][0]["leverage"]["value"], "10");
    assert!(current["ts_received"].as_u64().unwrap() > prior["ts_received"].as_u64().unwrap());
    assert_eq!(current["generation"], 2);

    for row in second.info.as_ref().unwrap()["papi"]["position_metrics"]
        .as_array()
        .unwrap()
    {
        let position = &row["positions"][0];
        for field in ["leverage", "unrealized_pnl", "position_amount"] {
            assert_eq!(position[field]["state"], expected);
        }
        assert!(position["ts_source"].is_null());
        assert_eq!(position["position_side"], "FUTURE_SIDE");
        assert_eq!(position["position_side_recognized"], false);
    }
}

#[tokio::test]
async fn account_query_empty_positions_have_explicit_returned_coverage() {
    let server = MockServer::new(testing::supported_trading).await;
    let client = testing::client(&server, &[]);
    let state = client.query_account_state().await.unwrap();

    for source in state.info.as_ref().unwrap()["papi"]["position_metrics"]
        .as_array()
        .unwrap()
    {
        assert_eq!(source["coverage"], "returned_positions");
        assert_eq!(source["positions"], json!([]));
    }
}

#[tokio::test]
async fn account_query_retains_metric_times_across_failure_delta_and_absence() {
    use nautilus_core::UUID4;
    use nautilus_model::{
        accounts::{Account, MarginAccount},
        types::Money,
    };

    let fail = Arc::new(AtomicBool::new(false));
    let absent = Arc::new(AtomicBool::new(false));
    let response_fail = Arc::clone(&fail);
    let response_absent = Arc::clone(&absent);
    let server = MockServer::new(move |request| {
        if request.path == "/papi/v1/um/account" && response_fail.load(Ordering::Acquire) {
            return Reply::raw(403, r#"{"code":-2015,"msg":"Invalid permissions"}"#);
        }

        if matches!(
            request.path.as_str(),
            "/papi/v1/um/account" | "/papi/v2/um/account"
        ) && !response_absent.load(Ordering::Acquire)
        {
            return Reply::json(&json!({"assets": [], "positions": [{
                "symbol": "BTCUSDT", "positionSide": "BOTH", "positionAmt": "0.001",
                "leverage": "10", "unrealizedProfit": "0.11000000", "updateTime": 1000,
            }]}));
        }
        testing::supported_trading(request)
    })
    .await;
    let client = testing::client(&server, &["BTCUSDT"]);
    let first = client.query_account_state().await.unwrap();
    let original = first.info.as_ref().unwrap()["papi"]["position_metrics"].clone();
    fail.store(true, Ordering::Release);
    client.inner.clock.set_time(ms(1_800_000_001_000));
    assert!(client.query_account_state().await.is_err());
    let retained: Value = serde_json::from_str(
        &client
            .account_observations_json(Duration::from_secs(30))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(retained[2]["receipt_status"], "failed");
    assert_eq!(
        retained[2]["observation"]["ts_received"],
        original[0]["ts_received"]
    );

    let mut account = MarginAccount::new(first.clone(), false);
    let mut delta = first.clone();
    delta.event_id = UUID4::new();
    delta.info = None;
    delta.ts_event = ms(1_800_000_002_000);
    delta.ts_init = delta.ts_event;
    delta.total_only_balances = vec![Money::from("1 USDT")];
    account.apply(delta).unwrap();
    assert!(account.last_event().unwrap().info.is_none());
    assert_eq!(
        first.info.as_ref().unwrap()["papi"]["position_metrics"],
        original
    );

    fail.store(false, Ordering::Release);
    absent.store(true, Ordering::Release);
    client.inner.clock.set_time(ms(1_800_000_003_000));
    let next = client.query_account_state().await.unwrap();
    for source in next.info.as_ref().unwrap()["papi"]["position_metrics"]
        .as_array()
        .unwrap()
    {
        assert_eq!(source["positions"], json!([]));
        assert_eq!(source["coverage"], "returned_positions");
        assert_eq!(source["generation"], 3);
    }
    assert_eq!(
        original[0]["positions"][0]["position_amount"]["value"],
        "0.001"
    );
}
