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

//! Synthetic REST scenarios built from the official examples in test_data/reports.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use axum::{
    Router,
    body::Body,
    extract::State,
    http::{Request, Response, StatusCode},
};
use nautilus_core::{UnixNanos, string::secret::SecretString, time::AtomicTime};
use nautilus_model::{
    identifiers::{AccountId, InstrumentId, Symbol},
    instruments::{InstrumentAny, stubs::crypto_perpetual_ethusdt},
    types::Currency,
};
use nautilus_network::ratelimiter::quota::Quota;
use parking_lot::Mutex;
use serde_json::{Value, json};

use crate::{
    http::RequestGate,
    read_only::{BinancePapiReadOnlyClient, BinancePapiReadOnlyConfig},
};

pub(crate) const API_KEY: &str = "OfflinePapiKey";
pub(crate) const API_SECRET: &str = "OfflinePapiSecret";
pub(crate) const TRADE_TIME: u64 = 1_680_688_557_875;

pub(crate) struct MockServer {
    pub(crate) url: String,
    state: Arc<ServerState>,
    task: tokio::task::JoinHandle<()>,
}

impl MockServer {
    pub(crate) async fn new(
        handler: impl Fn(&RecordedRequest) -> Reply + Send + Sync + 'static,
    ) -> Self {
        let state = Arc::new(ServerState {
            requests: Mutex::new(Vec::new()),
            notify: tokio::sync::Notify::new(),
            handler: Box::new(handler),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let router = Router::new()
            .fallback(respond)
            .with_state(Arc::clone(&state));

        let serve = async move { axum::serve(listener, router).await.unwrap() };

        // Test teardown owns this server and its runtime
        let task = tokio::spawn(serve); // tokio-import-ok
        Self { url, state, task }
    }

    pub(crate) fn requests(&self) -> Vec<RecordedRequest> {
        self.state.requests.lock().clone()
    }

    pub(crate) async fn wait_for_requests(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let notified = self.state.notify.notified();

                if self.state.requests.lock().len() >= count {
                    break;
                }

                notified.await;
            }
        })
        .await
        .unwrap();
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct ServerState {
    requests: Mutex<Vec<RecordedRequest>>,
    notify: tokio::sync::Notify,
    handler: Box<dyn Fn(&RecordedRequest) -> Reply + Send + Sync>,
}

#[derive(Clone, Debug)]
pub(crate) struct RecordedRequest {
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) params: BTreeMap<String, String>,
    pub(crate) query: String,
    pub(crate) api_key: Option<String>,
}

pub(crate) struct Reply {
    pub(crate) status: u16,
    pub(crate) body: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) delay: Duration,
}

impl Reply {
    pub(crate) fn json(value: &Value) -> Self {
        Self::raw(200, value.to_string())
    }

    pub(crate) fn raw(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            body: body.into(),
            headers: Vec::new(),
            delay: Duration::ZERO,
        }
    }
}

async fn respond(State(state): State<Arc<ServerState>>, request: Request<Body>) -> Response<Body> {
    let query = request.uri().query().unwrap_or_default().to_owned();
    let recorded = RecordedRequest {
        method: request.method().to_string(),
        path: request.uri().path().to_owned(),
        params: url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect(),
        query,
        api_key: request
            .headers()
            .get("x-mbx-apikey")
            .map(|v| v.to_str().unwrap().to_owned()),
    };
    let reply = (state.handler)(&recorded);
    state.requests.lock().push(recorded);
    state.notify.notify_waiters();

    if !reply.delay.is_zero() {
        tokio::time::sleep(reply.delay).await;
    }

    let mut response = Response::builder().status(StatusCode::from_u16(reply.status).unwrap());

    for (name, value) in reply.headers {
        response = response.header(name, value);
    }

    response.body(Body::from(reply.body)).unwrap()
}

pub(crate) fn instrument(symbol: &str) -> InstrumentAny {
    let mut instrument = crypto_perpetual_ethusdt();
    instrument.id = InstrumentId::from(format!("{symbol}-PERP.BINANCE"));
    instrument.raw_symbol = Symbol::from(symbol);
    instrument.base_currency = match symbol {
        "BNBUSDT" => Currency::BNB(),
        "ETHUSDT" => Currency::ETH(),
        "BTCUSDT" => Currency::BTC(),
        _ => panic!("Unsupported test instrument {symbol}"),
    };
    InstrumentAny::CryptoPerpetual(instrument)
}

pub(crate) fn gate() -> Arc<RequestGate> {
    Arc::new(RequestGate::new(
        Quota::per_minute(std::num::NonZeroU32::new(3_000).unwrap())
            .allow_burst(std::num::NonZeroU32::new(40).unwrap()),
    ))
}

pub(crate) fn config(url: &str) -> BinancePapiReadOnlyConfig {
    let mut config = BinancePapiReadOnlyConfig::new(
        AccountId::from("BINANCE-PAPI-001"),
        API_KEY.into(),
        API_SECRET.into(),
    );
    config.base_url = SecretString::from(url);
    config.request_timeout = Duration::from_millis(500);
    config.operation_timeout = Duration::from_secs(30);
    config
}

pub(crate) fn client(server: &MockServer, symbols: &[&str]) -> BinancePapiReadOnlyClient {
    BinancePapiReadOnlyClient::from_parts(
        &config(&server.url),
        symbols.iter().map(|s| instrument(s)).collect(),
        gate(),
        Arc::new(AtomicTime::new(false, ms(1_800_000_000_000))),
    )
    .unwrap()
}

pub(crate) fn ms(value: u64) -> UnixNanos {
    UnixNanos::from(value * 1_000_000)
}

/// A docs-derived one-way order scenario; the canonical order example is hedge-mode with zero price.
pub(crate) fn order() -> Value {
    let mut row: Value =
        serde_json::from_str(include_str!("../test_data/reports/order.json")).unwrap();
    row["positionSide"] = json!("BOTH");
    row["price"] = json!("28511.00");
    row["side"] = json!("SELL");
    row["origQty"] = json!("0.010");
    row["orderId"] = json!(270_093_109_i64);
    row["time"] = json!(TRADE_TIME - 1_000);
    row["updateTime"] = row["time"].clone();
    row
}

pub(crate) fn filled_order() -> Value {
    let mut row = order();
    row["status"] = json!("FILLED");
    row["executedQty"] = json!("0.010");
    row["avgPrice"] = json!("28511.00");
    row["updateTime"] = json!(TRADE_TIME);
    row
}

pub(crate) fn trade() -> Value {
    let rows: Vec<Value> =
        serde_json::from_str(include_str!("../test_data/reports/trades.json")).unwrap();
    rows[0].clone()
}

pub(crate) fn position(symbol: &str) -> Value {
    let mut rows: Vec<Value> =
        serde_json::from_str(include_str!("../test_data/reports/positions.json")).unwrap();
    rows[0]["symbol"] = json!(symbol);
    rows.remove(0)
}

pub(crate) fn supported_balances() -> Value {
    let mut rows: Value =
        serde_json::from_str(include_str!("../test_data/observations/balances.json")).unwrap();
    let usdt = rows
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|row| row["asset"] == "USDT")
        .unwrap();
    usdt["crossMarginBorrowed"] = json!("0.00000000");
    usdt["crossMarginInterest"] = json!("0.00000000");
    rows
}

pub(crate) fn algo() -> Value {
    let rows: Vec<Value> =
        serde_json::from_str(include_str!("../test_data/reports/open_algos.json")).unwrap();
    rows[0].clone()
}

/// A synthetic triggered parent and its filled child, derived from the official examples.
pub(crate) fn triggered_algo() -> (Value, Value) {
    let mut parent = algo();
    parent["symbol"] = json!("BTCUSDT");
    parent["price"] = json!("28511.00");
    parent["triggerPrice"] = json!("28511.00");
    parent["algoStatus"] = json!("FINISHED");
    parent["actualOrderId"] = json!("270093109");
    parent["createTime"] = json!(TRADE_TIME - 2_000);
    parent["triggerTime"] = json!(TRADE_TIME - 750);
    parent["updateTime"] = json!(TRADE_TIME);
    let mut child = filled_order();
    child["origType"] = json!("TAKE_PROFIT");
    child["time"] = json!(TRADE_TIME - 750);
    (parent, child)
}

/// Default quiet-account GET responses; each test overrides only its relevant scenario.
pub(crate) fn quiet(request: &RecordedRequest) -> Reply {
    match request.path.as_str() {
        "/papi/v1/listenKey" if request.method == "POST" => {
            Reply::json(&json!({"listenKey": "offline-listen-key"}))
        }
        "/papi/v1/listenKey" if matches!(request.method.as_str(), "PUT" | "DELETE") => {
            Reply::raw(200, "")
        }
        "/papi/v1/um/positionSide/dual" => Reply::json(&json!({"dualSidePosition": false})),
        "/papi/v1/um/positionRisk" => Reply::json(&json!([position(&request.params["symbol"])])),
        "/papi/v1/um/openOrders"
        | "/papi/v1/um/algo/openAlgoOrders"
        | "/papi/v1/cm/positionRisk"
        | "/papi/v1/cm/openOrders"
        | "/papi/v1/margin/openOrders"
        | "/papi/v1/um/allOrders"
        | "/papi/v1/um/algo/allAlgoOrders"
        | "/papi/v1/um/userTrades" => Reply::json(&json!([])),
        "/papi/v1/balance" => {
            Reply::raw(200, include_str!("../test_data/observations/balances.json"))
        }
        "/papi/v1/account" => {
            Reply::raw(200, include_str!("../test_data/observations/account.json"))
        }
        "/papi/v1/um/account" | "/papi/v2/um/account" => Reply::raw(
            200,
            include_str!("../test_data/observations/um_account.json"),
        ),
        "/papi/v1/rateLimit/order" => Reply::raw(
            200,
            include_str!("../test_data/reports/order_rate_limit.json"),
        ),
        _ => Reply::raw(400, r#"{"code":-2013,"msg":"Order does not exist"}"#),
    }
}
