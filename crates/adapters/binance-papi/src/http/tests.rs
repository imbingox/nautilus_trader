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
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use aws_lc_rs::hmac;
use binance_sdk::common::errors::ConnectorError;
use nautilus_core::time::AtomicTime;
use nautilus_live::execution::failure::CommandFailure;
use nautilus_model::identifiers::ClientOrderId;
use nautilus_network::ratelimiter::quota::Quota;
use rstest::rstest;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;
use crate::{
    http::command::{
        CancelUmOrderRequest, PapiCommandBuildError, PapiUmOrderSide, PapiUmTimeInForce,
        SubmitUmOrderRequest,
    },
    observations::ObservationSource,
    testing::{self, API_KEY, API_SECRET, MockServer, Reply},
};

fn http(server: &MockServer, gate: Arc<RequestGate>) -> PapiHttpClient {
    PapiHttpClient::new(
        &testing::config(&server.url),
        gate,
        Arc::new(AtomicTime::default()),
    )
    .unwrap()
}

fn budget() -> RequestBudget {
    RequestBudget::new(Duration::from_secs(5), 30, 10_000).unwrap()
}

fn request() -> PapiRequest {
    PapiRequest::Observation(ObservationSource::Account)
}

fn assert_signature(request: &testing::RecordedRequest) {
    assert_command_signature(request, "GET");
}

fn assert_command_signature(request: &testing::RecordedRequest, method: &str) {
    let (canonical, signature) = request.query.rsplit_once("&signature=").unwrap();
    let key = hmac::Key::new(hmac::HMAC_SHA256, API_SECRET.as_bytes());
    let tag = hmac::sign(&key, canonical.as_bytes());
    let expected: String = tag
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert_eq!(signature, expected);
    assert_eq!(request.method, method);
    assert_eq!(request.api_key.as_deref(), Some(API_KEY));
    assert_eq!(request.params["recvWindow"], "5000");
}

fn market_request() -> SubmitUmOrderRequest {
    SubmitUmOrderRequest::market(
        "BTCUSDT",
        PapiUmOrderSide::Buy,
        dec!(0.0100),
        ClientOrderId::new("strategy/A:1"),
        false,
    )
    .unwrap()
}

fn limit_request() -> SubmitUmOrderRequest {
    SubmitUmOrderRequest::limit(
        "BTCUSDT",
        PapiUmOrderSide::Sell,
        dec!(0.0100),
        dec!(28511.2300),
        PapiUmTimeInForce::Gtx,
        ClientOrderId::new("strategy/A:2"),
        true,
    )
    .unwrap()
}

fn command_reply(request: &testing::RecordedRequest) -> Reply {
    let order_id = request
        .params
        .get("orderId")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(270_093_109);
    let client_order_id = request
        .params
        .get("newClientOrderId")
        .or_else(|| request.params.get("origClientOrderId"))
        .cloned()
        .unwrap_or_else(|| "venue-client-id".to_owned());
    Reply::json(&serde_json::json!({
        "symbol": request.params["symbol"],
        "orderId": order_id,
        "clientOrderId": client_order_id,
    }))
}

#[rstest]
#[case::symbol(
    SubmitUmOrderRequest::market(
        "BTC-USDT",
        PapiUmOrderSide::Buy,
        dec!(1),
        ClientOrderId::new("valid"),
        false,
    ),
    PapiCommandBuildError::Symbol
)]
#[case::client_order_id(
    SubmitUmOrderRequest::market(
        "BTCUSDT",
        PapiUmOrderSide::Buy,
        dec!(1),
        ClientOrderId::new("invalid#id"),
        false,
    ),
    PapiCommandBuildError::ClientOrderId
)]
#[case::quantity(
    SubmitUmOrderRequest::market(
        "BTCUSDT",
        PapiUmOrderSide::Buy,
        Decimal::ZERO,
        ClientOrderId::new("valid"),
        false,
    ),
    PapiCommandBuildError::Quantity
)]
#[case::price(
    SubmitUmOrderRequest::limit(
        "BTCUSDT",
        PapiUmOrderSide::Buy,
        dec!(1),
        Decimal::ZERO,
        PapiUmTimeInForce::Gtc,
        ClientOrderId::new("valid"),
        false,
    ),
    PapiCommandBuildError::Price
)]
fn command_construction_rejects_unsupported_wire_values(
    #[case] result: Result<SubmitUmOrderRequest, PapiCommandBuildError>,
    #[case] expected: PapiCommandBuildError,
) {
    assert_eq!(result.unwrap_err(), expected);
}

#[rstest]
fn cancel_construction_requires_a_valid_identity() {
    assert_eq!(
        CancelUmOrderRequest::by_order_id("BTCUSDT", 0).unwrap_err(),
        PapiCommandBuildError::OrderId
    );
    assert_eq!(
        CancelUmOrderRequest::by_client_order_id("BTCUSDT", ClientOrderId::new("invalid#id"))
            .unwrap_err(),
        PapiCommandBuildError::ClientOrderId
    );
}

#[tokio::test]
async fn signed_limit_submit_preserves_exact_decimals_and_fixed_capabilities() {
    let server = MockServer::new(command_reply).await;
    let client = http(&server, testing::gate());
    let response = client
        .submit_um_order(&limit_request(), &budget(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(response.raw.metadata.endpoint, "/papi/v1/um/order");
    assert_eq!(response.raw.metadata.symbol.as_deref(), Some("BTCUSDT"));
    assert_eq!(response.raw.metadata.status, 200);
    assert_eq!(response.acknowledgement.venue_order_id, 270_093_109);
    assert_eq!(response.acknowledgement.client_order_id, "strategy/A:2");

    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_command_signature(request, "POST");
    assert_eq!(request.path, "/papi/v1/um/order");
    assert_eq!(request.params["symbol"], "BTCUSDT");
    assert_eq!(request.params["side"], "SELL");
    assert_eq!(request.params["positionSide"], "BOTH");
    assert_eq!(request.params["type"], "LIMIT");
    assert_eq!(request.params["timeInForce"], "GTX");
    assert_eq!(request.params["quantity"], "0.0100");
    assert_eq!(request.params["price"], "28511.2300");
    assert_eq!(request.params["newClientOrderId"], "strategy/A:2");
    assert_eq!(request.params["newOrderRespType"], "ACK");
    assert_eq!(request.params["reduceOnly"], "true");
    assert!(!request.params.contains_key("priceMatch"));
    assert!(!request.params.contains_key("selfTradePreventionMode"));
    assert!(!request.params.contains_key("goodTillDate"));
}

#[tokio::test]
async fn signed_market_submit_omits_limit_only_parameters() {
    let server = MockServer::new(command_reply).await;
    let client = http(&server, testing::gate());
    client
        .submit_um_order(&market_request(), &budget(), &CancellationToken::new())
        .await
        .unwrap();

    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_command_signature(request, "POST");
    assert_eq!(request.params["type"], "MARKET");
    assert_eq!(request.params["quantity"], "0.0100");
    assert_eq!(request.params["reduceOnly"], "false");
    assert!(!request.params.contains_key("price"));
    assert!(!request.params.contains_key("timeInForce"));
}

#[tokio::test]
async fn signed_cancel_encodes_exactly_one_order_identity() {
    let server = MockServer::new(command_reply).await;
    let client = http(&server, testing::gate());
    let cancel = CancellationToken::new();
    client
        .cancel_um_order(
            &CancelUmOrderRequest::by_order_id("BTCUSDT", 9_007_199_254_740_993).unwrap(),
            &budget(),
            &cancel,
        )
        .await
        .unwrap();
    client
        .cancel_um_order(
            &CancelUmOrderRequest::by_client_order_id(
                "BTCUSDT",
                ClientOrderId::new("strategy/A:2"),
            )
            .unwrap(),
            &budget(),
            &cancel,
        )
        .await
        .unwrap();

    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    assert_command_signature(&requests[0], "DELETE");
    assert_eq!(requests[0].params["orderId"], "9007199254740993");
    assert!(!requests[0].params.contains_key("origClientOrderId"));
    assert_command_signature(&requests[1], "DELETE");
    assert_eq!(requests[1].params["origClientOrderId"], "strategy/A:2");
    assert!(!requests[1].params.contains_key("orderId"));
}

#[rstest]
#[case(-1000)]
#[case(-1001)]
#[case(-1006)]
#[case(-1007)]
#[tokio::test]
async fn uncertain_venue_codes_are_ambiguous_and_not_retried(#[case] code: i64) {
    let server =
        MockServer::new(move |_| Reply::raw(400, format!(r#"{{"code":{code},"msg":"unknown"}}"#)))
            .await;
    let failure = http(&server, testing::gate())
        .submit_um_order(&market_request(), &budget(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(
        failure.classification,
        CommandFailure::Ambiguous(_)
    ));
    assert!(matches!(
        failure.error,
        PapiHttpError::Rejected {
            status: Some(400),
            code: Some(actual),
        } if actual == code
    ));
    assert_eq!(server.requests().len(), 1);
}

#[rstest]
#[case(-2011)]
#[case(-2013)]
#[tokio::test]
async fn cancel_not_found_codes_are_ambiguous_and_not_retried(#[case] code: i64) {
    let server = MockServer::new(move |_| {
        Reply::raw(400, format!(r#"{{"code":{code},"msg":"cancel rejected"}}"#))
    })
    .await;
    let failure = http(&server, testing::gate())
        .cancel_um_order(
            &CancelUmOrderRequest::by_order_id("BTCUSDT", 270_093_109).unwrap(),
            &budget(),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        failure.classification,
        CommandFailure::Ambiguous(_)
    ));
    assert!(matches!(
        failure.error,
        PapiHttpError::Rejected {
            status: Some(400),
            code: Some(actual),
        } if actual == code
    ));
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn explicit_submit_rejection_is_classified_from_typed_venue_evidence() {
    let server =
        MockServer::new(|_| Reply::raw(400, r#"{"code":-2010,"msg":"NEW_ORDER_REJECTED"}"#)).await;
    let failure = http(&server, testing::gate())
        .submit_um_order(&market_request(), &budget(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(
        failure.classification,
        CommandFailure::VenueRejected(_)
    ));
    assert_eq!(server.requests().len(), 1);
}

#[rstest]
#[case(r#"{"msg":"Unknown error, please check your request or try again later."}"#)]
#[case(r#"{"msg":"Service Unavailable."}"#)]
#[case(r#"{"msg":"Internal error; unable to process your request."}"#)]
#[tokio::test]
async fn sdk_erases_503_variants_so_every_command_outcome_is_ambiguous(#[case] body: &str) {
    let body = body.to_string();
    let server = MockServer::new(move |_| Reply::raw(503, body.clone())).await;
    let failure = http(&server, testing::gate())
        .submit_um_order(&market_request(), &budget(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(
        failure.classification,
        CommandFailure::Ambiguous(_)
    ));
    assert_eq!(failure.error, PapiHttpError::Server(503));
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn command_decode_failure_after_2xx_is_ambiguous_and_not_retried() {
    let server = MockServer::new(|_| Reply::raw(200, "{")).await;
    let failure = http(&server, testing::gate())
        .submit_um_order(&market_request(), &budget(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(
        failure.classification,
        CommandFailure::Ambiguous(_)
    ));
    assert_eq!(failure.error, PapiHttpError::Decode);
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn command_response_loss_after_dispatch_is_ambiguous_and_not_retried() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = testing::config(&format!("http://{}", listener.local_addr().unwrap()));
    let serve = async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut data = [0; 4_096];
        let received = stream.read(&mut data).await.unwrap();
        assert!(received > 0);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 200\r\nConnection: close\r\n\r\n{}")
            .await
            .unwrap();
        stream.shutdown().await.unwrap();
    };

    let task = tokio::spawn(serve); // tokio-import-ok
    let client =
        PapiHttpClient::new(&config, testing::gate(), Arc::new(AtomicTime::default())).unwrap();
    let failure = client
        .submit_um_order(&market_request(), &budget(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(
        failure.classification,
        CommandFailure::Ambiguous(_)
    ));
    assert_eq!(failure.error, PapiHttpError::Sdk);
    task.await.unwrap();
}

#[tokio::test]
async fn command_timeout_after_dispatch_is_ambiguous_and_not_retried() {
    let server = MockServer::new(|_| {
        let mut reply = Reply::raw(200, "{}");
        reply.delay = Duration::from_secs(2);
        reply
    })
    .await;
    let mut config = testing::config(&server.url);
    config.request_timeout = Duration::from_millis(50);
    let client =
        PapiHttpClient::new(&config, testing::gate(), Arc::new(AtomicTime::default())).unwrap();
    let failure = client
        .submit_um_order(&market_request(), &budget(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(
        failure.classification,
        CommandFailure::Ambiguous(_)
    ));
    assert_eq!(failure.error, PapiHttpError::Timeout);
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn command_cancellation_before_dispatch_is_not_sent() {
    let server = MockServer::new(|_| Reply::raw(200, "{}")).await;
    let token = CancellationToken::new();
    token.cancel();
    let failure = http(&server, testing::gate())
        .submit_um_order(&market_request(), &budget(), &token)
        .await
        .unwrap_err();
    assert!(matches!(failure.classification, CommandFailure::NotSent(_)));
    assert_eq!(failure.error, PapiHttpError::Canceled);
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn failed_durable_barrier_sends_nothing() {
    let server = MockServer::new(|_| Reply::raw(200, "{}")).await;
    let invoked = Arc::new(AtomicUsize::new(0));
    let barrier_invoked = Arc::clone(&invoked);
    let error = http(&server, testing::gate())
        .submit_um_order_with_barrier(
            &market_request(),
            &budget(),
            &CancellationToken::new(),
            move || {
                barrier_invoked.fetch_add(1, Ordering::SeqCst);
                anyhow::bail!("offline durability fault")
            },
        )
        .await
        .unwrap_err();

    assert!(matches!(error, PapiCommandDispatchError::Barrier(_)));
    assert_eq!(invoked.load(Ordering::SeqCst), 1);
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn durable_barrier_runs_only_after_all_quota_waits() {
    let server = MockServer::new(|_| Reply::raw(200, "{}")).await;
    let gate = Arc::new(RequestGate::with_order_quota(
        Quota::per_minute(std::num::NonZeroU32::new(100).unwrap())
            .allow_burst(std::num::NonZeroU32::new(10).unwrap()),
        Quota::with_period(Duration::from_secs(10))
            .unwrap()
            .allow_burst(std::num::NonZeroU32::new(1).unwrap()),
    ));
    gate.order_limiter.await_keys_ready(Some(&[()])).await;
    let client = http(&server, gate);
    let token = CancellationToken::new();
    let cancel = token.clone();
    let invoked = Arc::new(AtomicUsize::new(0));
    let barrier_invoked = Arc::clone(&invoked);
    let request = market_request();
    let budget = budget();
    let future = client.submit_um_order_with_barrier(&request, &budget, &token, move || {
        barrier_invoked.fetch_add(1, Ordering::SeqCst);
        Ok(())
    });
    tokio::pin!(future);

    tokio::select! {
        biased;
        result = &mut future => panic!("Unexpected command completion: {result:?}"),
        () = tokio::time::sleep(Duration::from_millis(5)) => {}
    }

    assert_eq!(invoked.load(Ordering::SeqCst), 0);
    cancel.cancel();
    let error = future.await.unwrap_err();
    assert!(matches!(
        error,
        PapiCommandDispatchError::Command(PapiCommandFailure {
            classification: CommandFailure::NotSent(_),
            ..
        })
    ));
    assert_eq!(invoked.load(Ordering::SeqCst), 0);
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn command_cancellation_after_dispatch_is_ambiguous_and_not_retried() {
    let server = MockServer::new(|_| {
        let mut reply = Reply::raw(200, "{}");
        reply.delay = Duration::from_secs(2);
        reply
    })
    .await;
    let client = http(&server, testing::gate());
    let token = CancellationToken::new();
    let cancel = token.clone();

    let operation = async move {
        client
            .submit_um_order(&market_request(), &budget(), &token)
            .await
    };

    let task = tokio::spawn(operation); // tokio-import-ok
    server.wait_for_requests(1).await;
    cancel.cancel();
    let failure = task.await.unwrap().unwrap_err();
    assert!(matches!(
        failure.classification,
        CommandFailure::Ambiguous(_)
    ));
    assert_eq!(failure.error, PapiHttpError::Canceled);
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn submit_waiting_for_account_order_quota_can_be_canceled_without_dispatch() {
    let server = MockServer::new(|_| Reply::raw(200, "{}")).await;
    let gate = Arc::new(RequestGate::with_order_quota(
        Quota::per_minute(std::num::NonZeroU32::new(100).unwrap())
            .allow_burst(std::num::NonZeroU32::new(10).unwrap()),
        Quota::with_period(Duration::from_secs(10))
            .unwrap()
            .allow_burst(std::num::NonZeroU32::new(1).unwrap()),
    ));
    gate.order_limiter.await_keys_ready(Some(&[()])).await;
    let client = http(&server, gate);
    let token = CancellationToken::new();
    let cancel = token.clone();
    let request = market_request();
    let budget = budget();
    let future = client.submit_um_order(&request, &budget, &token);
    tokio::pin!(future);

    tokio::select! {
        biased;
        result = &mut future => panic!("Unexpected command completion: {result:?}"),
        () = tokio::time::sleep(Duration::from_millis(5)) => {}
    }

    cancel.cancel();
    let failure = future.await.unwrap_err();
    assert!(matches!(failure.classification, CommandFailure::NotSent(_)));
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn cancel_does_not_consume_the_new_order_quota() {
    let server = MockServer::new(|_| Reply::json(&testing::order())).await;
    let gate = Arc::new(RequestGate::with_order_quota(
        Quota::per_minute(std::num::NonZeroU32::new(100).unwrap())
            .allow_burst(std::num::NonZeroU32::new(10).unwrap()),
        Quota::with_period(Duration::from_secs(10))
            .unwrap()
            .allow_burst(std::num::NonZeroU32::new(1).unwrap()),
    ));
    gate.order_limiter.await_keys_ready(Some(&[()])).await;
    let client = http(&server, gate);
    client
        .cancel_um_order(
            &CancelUmOrderRequest::by_order_id("BTCUSDT", 270_093_109).unwrap(),
            &budget(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn rejected_submit_conservatively_consumes_the_account_order_quota() {
    let server =
        MockServer::new(|_| Reply::raw(400, r#"{"code":-2010,"msg":"NEW_ORDER_REJECTED"}"#)).await;
    let gate = Arc::new(RequestGate::with_order_quota(
        Quota::per_minute(std::num::NonZeroU32::new(100).unwrap())
            .allow_burst(std::num::NonZeroU32::new(10).unwrap()),
        Quota::with_period(Duration::from_secs(10))
            .unwrap()
            .allow_burst(std::num::NonZeroU32::new(1).unwrap()),
    ));
    let client = http(&server, gate);
    let first = client
        .submit_um_order(&market_request(), &budget(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(
        first.classification,
        CommandFailure::VenueRejected(_)
    ));

    let token = CancellationToken::new();
    let cancel = token.clone();
    let request = market_request();
    let budget = budget();
    let future = client.submit_um_order(&request, &budget, &token);
    tokio::pin!(future);

    tokio::select! {
        biased;
        result = &mut future => panic!("Unexpected command completion: {result:?}"),
        () = tokio::time::sleep(Duration::from_millis(5)) => {}
    }

    cancel.cancel();
    let second = future.await.unwrap_err();
    assert!(matches!(second.classification, CommandFailure::NotSent(_)));
    assert_eq!(server.requests().len(), 1);
}

#[rstest]
#[case(418)]
#[case(429)]
#[tokio::test]
async fn command_throttle_is_ambiguous_and_latches_the_shared_gate(#[case] status: u16) {
    let server =
        MockServer::new(move |_| Reply::raw(status, r#"{"code":-1003,"msg":"Too many requests"}"#))
            .await;
    let gate = testing::gate();
    let client = http(&server, Arc::clone(&gate));
    let first = client
        .submit_um_order(&market_request(), &budget(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(first.classification, CommandFailure::Ambiguous(_)));
    assert!(matches!(first.error, PapiHttpError::Throttled { .. }));

    let second = http(&server, gate)
        .cancel_um_order(
            &CancelUmOrderRequest::by_order_id("BTCUSDT", 270_093_109).unwrap(),
            &budget(),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(second.classification, CommandFailure::NotSent(_)));
    assert_eq!(second.error, PapiHttpError::GateClosed);
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn listen_key_lifecycle_uses_api_key_without_signed_parameters() {
    let server = MockServer::new(testing::quiet).await;
    let client = http(&server, testing::gate());
    let cancel = CancellationToken::new();
    let key = client.create_listen_key(&budget(), &cancel).await.unwrap();
    assert_eq!(key.expose_secret(), "offline-listen-key");
    client
        .keepalive_listen_key(&budget(), &cancel)
        .await
        .unwrap();
    client.close_listen_key(&budget(), &cancel).await.unwrap();

    let requests = server.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[1].method, "PUT");
    assert_eq!(requests[2].method, "DELETE");

    for request in requests {
        assert_eq!(request.path, "/papi/v1/listenKey");
        assert_eq!(request.api_key.as_deref(), Some(API_KEY));
        assert!(request.params.is_empty());
        assert!(!request.query.contains("signature"));
        assert!(!request.query.contains("timestamp"));
    }
}

#[tokio::test]
async fn listen_key_expiry_is_typed_and_not_retried() {
    let server = MockServer::new(|request| {
        if request.path == "/papi/v1/listenKey" && request.method == "PUT" {
            Reply::raw(
                400,
                r#"{"code":-1125,"msg":"This listenKey does not exist."}"#,
            )
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let client = http(&server, testing::gate());
    let e = client
        .keepalive_listen_key(&budget(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e, PapiHttpError::ListenKeyExpired);
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn listen_key_throttle_closes_the_shared_gate() {
    let server =
        MockServer::new(|_| Reply::raw(429, r#"{"code":-1003,"msg":"Too many requests"}"#)).await;
    let gate = testing::gate();
    let first = http(&server, Arc::clone(&gate));
    let second = http(&server, gate);
    let e = first
        .create_listen_key(&budget(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(e, PapiHttpError::Throttled { .. }));
    assert_eq!(
        second
            .keepalive_listen_key(&budget(), &CancellationToken::new())
            .await
            .unwrap_err(),
        PapiHttpError::GateClosed
    );
    assert_eq!(server.requests().len(), 1);
}

#[rstest]
#[case(ObservationSource::UmOpenOrders, 40)]
#[case(ObservationSource::UmOpenAlgos, 40)]
#[case(ObservationSource::CmPositions, 1)]
#[case(ObservationSource::CmOpenOrders, 40)]
#[case(ObservationSource::MarginOpenOrders, 5)]
fn account_wide_observation_uses_documented_ip_weight(
    #[case] source: ObservationSource,
    #[case] expected: usize,
) {
    assert_eq!(PapiRequest::Observation(source).weight(), expected);
}

#[tokio::test]
async fn signed_get_preserves_raw_json_and_quota_metadata() {
    let body =
        r#"{"absentPeer":null,"amount":"0.1234567890123456789012345678","id":9007199254740993}"#;
    let server = MockServer::new(move |_| {
        let mut reply = Reply::raw(200, body);
        reply.headers = vec![
            ("x-mbx-used-weight-1m".into(), "71".into()),
            ("x-mbx-order-count-1m".into(), "3".into()),
        ];
        reply
    })
    .await;
    let client = http(&server, testing::gate());
    let response = client
        .get(&request(), &budget(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(response.body.get(), body);
    assert_eq!(response.metadata.status, 200);
    assert_eq!(response.metadata.used_weight_1m, Some(71));
    assert_eq!(response.metadata.order_count_1m, Some(3));
    assert_eq!(response.metadata.retry_after_seconds, None);
    assert!(response.metadata.ts_received >= response.metadata.ts_requested);
    assert!(response.received_at >= response.requested_at);
    assert_signature(&server.requests()[0]);
}

#[tokio::test]
async fn signing_encodes_client_identity_and_keeps_large_order_ids_exact() {
    let server = MockServer::new(|_| Reply::raw(200, "{}")).await;
    let client = http(&server, testing::gate());
    client
        .get(
            &PapiRequest::Order {
                symbol: "BTCUSDT".into(),
                order_id: Some(9_007_199_254_740_993),
                client_order_id: Some("client/a:b.c_-1".into()),
            },
            &budget(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    let request = &server.requests()[0];
    assert_signature(request);
    assert_eq!(request.params["orderId"], "9007199254740993");
    assert_eq!(request.params["origClientOrderId"], "client/a:b.c_-1");
}

#[tokio::test]
async fn transient_retry_gets_a_new_signature_and_consumes_another_attempt() {
    let count = AtomicUsize::new(0);
    let first_attempt_at = Arc::new(parking_lot::Mutex::new(None));
    let observed = Arc::clone(&first_attempt_at);
    let server = MockServer::new(move |_| {
        if count.fetch_add(1, Ordering::SeqCst) == 0 {
            *observed.lock() = Some(Instant::now());
            Reply::raw(503, r#"{"msg":"unavailable"}"#)
        } else {
            Reply::raw(200, "{}")
        }
    })
    .await;
    let client = http(&server, testing::gate());
    let response = client
        .get(&request(), &budget(), &CancellationToken::new())
        .await
        .unwrap();
    assert!(response.requested_at <= first_attempt_at.lock().unwrap());
    assert!(response.received_at >= first_attempt_at.lock().unwrap());
    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    assert_signature(&requests[0]);
    assert_signature(&requests[1]);
    assert!(
        requests[1].params["timestamp"].parse::<u64>().unwrap()
            > requests[0].params["timestamp"].parse::<u64>().unwrap()
    );
    assert_ne!(
        requests[0].params["signature"],
        requests[1].params["signature"]
    );
}

#[rstest]
#[case(401, -2014)]
#[case(403, -2015)]
#[case(400, -1022)]
#[case(400, -1021)]
#[case(404, -2013)]
#[case(400, -1100)]
#[tokio::test]
async fn permanent_failure_is_not_retried_or_exposed_as_absence(
    #[case] status: u16,
    #[case] code: i64,
) {
    let server = MockServer::new(move |_| {
        Reply::raw(
            status,
            format!(r#"{{"code":{code},"msg":"https://venue.invalid/?signature=SecretMarker"}}"#),
        )
    })
    .await;
    let client = http(&server, testing::gate());
    let e = client
        .get(&request(), &budget(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert!(!e.retryable());
    assert!(!format!("{e:?} {e}").contains("SecretMarker"));
    assert!(!format!("{e:?} {e}").contains("https://"));
    assert_eq!(server.requests().len(), 1);
}

#[rstest]
#[case(429)]
#[case(418)]
#[tokio::test]
async fn header_only_throttle_latches_across_independent_clients(#[case] status: u16) {
    let server = MockServer::new(move |_| {
        let mut reply = Reply::raw(status, "{}");
        reply.headers.push(("retry-after".into(), "1".into()));
        reply
    })
    .await;
    let gate = testing::gate();
    let first = http(&server, Arc::clone(&gate));
    let second = http(&server, gate);
    assert!(matches!(
        first.get(&request(), &budget(), &CancellationToken::new()).await,
        Err(PapiHttpError::Throttled { status: Some(s), code: None }) if s == status
    ));
    drop(first);

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(
        second
            .get(&request(), &budget(), &CancellationToken::new())
            .await
            .unwrap_err(),
        PapiHttpError::GateClosed
    );
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn throttling_code_on_bad_request_also_closes_gate() {
    let server = MockServer::new(|_| Reply::raw(400, r#"{"code":-1003,"msg":"throttled"}"#)).await;
    let client = http(&server, testing::gate());
    assert!(matches!(
        client
            .get(&request(), &budget(), &CancellationToken::new())
            .await,
        Err(PapiHttpError::Throttled {
            code: Some(-1003),
            ..
        })
    ));
    assert!(client.is_throttled());
}

#[tokio::test]
async fn malformed_json_is_a_decode_failure_without_retries() {
    let server = MockServer::new(|_| Reply::raw(200, "{")).await;
    let e = http(&server, testing::gate())
        .get(&request(), &budget(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e, PapiHttpError::Decode);
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn oversized_json_is_rejected_before_domain_parsing() {
    let server =
        MockServer::new(|_| Reply::raw(200, format!("\"{}\"", "x".repeat(MAX_RESPONSE_BYTES))))
            .await;
    let e = http(&server, testing::gate())
        .get(&request(), &budget(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e, PapiHttpError::ResponseTooLarge);
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn sdk_body_read_failure_is_not_guessed_from_error_text_or_retried() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = testing::config(&format!("http://{}", listener.local_addr().unwrap()));
    let serve = async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut data = [0; 4_096];
        let received = stream.read(&mut data).await.unwrap();
        assert!(received > 0);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 200\r\nConnection: close\r\n\r\n{}")
            .await
            .unwrap();
        stream.shutdown().await.unwrap();
    };

    // Keep the mock connection on the runtime owned by this test
    let task = tokio::spawn(serve); // tokio-import-ok
    let client =
        PapiHttpClient::new(&config, testing::gate(), Arc::new(AtomicTime::default())).unwrap();
    let e = client
        .get(&request(), &budget(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e, PapiHttpError::Sdk);
    task.await.unwrap();
}

#[tokio::test]
async fn redirect_does_not_forward_credentials() {
    let sink = MockServer::new(|_| Reply::raw(200, "{}")).await;
    let location = sink.url.clone();
    let server = MockServer::new(move |_| {
        let mut reply = Reply::raw(302, "{}");
        reply.headers.push(("location".into(), location.clone()));
        reply
    })
    .await;
    let e = http(&server, testing::gate())
        .get(&request(), &budget(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(
        e,
        PapiHttpError::Rejected {
            status: Some(302),
            code: None
        }
    );
    assert!(sink.requests().is_empty());
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn attempt_budget_bounds_sdk_and_adapter_retries_together() {
    let server = MockServer::new(|_| Reply::raw(503, "{}")).await;
    let budget = RequestBudget::new(Duration::from_secs(5), 2, 100).unwrap();
    let e = http(&server, testing::gate())
        .get(&request(), &budget, &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e, PapiHttpError::Budget);
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn shared_weight_blocks_the_next_client_within_its_total_deadline() {
    let server = MockServer::new(|_| Reply::raw(200, "{}")).await;
    let gate = Arc::new(RequestGate::new(
        Quota::with_period(Duration::from_secs(10))
            .unwrap()
            .allow_burst(std::num::NonZeroU32::new(40).unwrap()),
    ));
    let first = http(&server, Arc::clone(&gate));
    let second = http(&server, gate);
    first
        .get(
            &PapiRequest::PositionMode,
            &budget(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    tokio::time::pause();
    let e = second
        .get(&request(), &budget(), &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(e, PapiHttpError::Budget);
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn cancel_during_quota_wait_sends_no_request() {
    let server = MockServer::new(|_| Reply::raw(200, "{}")).await;
    let gate = Arc::new(RequestGate::new(
        Quota::with_period(Duration::from_secs(10))
            .unwrap()
            .allow_burst(std::num::NonZeroU32::new(40).unwrap()),
    ));
    gate.limiter.await_keys_ready(Some(&[(); 40])).await;
    let client = http(&server, gate);
    let request = request();
    let budget = budget();
    let token = CancellationToken::new();
    let future = client.get(&request, &budget, &token);
    tokio::pin!(future);

    tokio::select! {
        biased;
        result = &mut future => panic!("Unexpected request completion: {result:?}"),
        () = tokio::time::sleep(Duration::from_millis(5)) => {}
    }

    token.cancel();
    assert_eq!(future.await.unwrap_err(), PapiHttpError::Canceled);
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn cancel_during_inflight_request_does_not_retry() {
    let server = MockServer::new(|_| {
        let mut reply = Reply::raw(200, "{}");
        reply.delay = Duration::from_secs(2);
        reply
    })
    .await;
    let client = http(&server, testing::gate());
    let token = CancellationToken::new();
    let cancel = token.clone();

    let operation = async move { client.get(&request(), &budget(), &token).await };

    // Cancellation and the delayed mock response share the test runtime
    let task = tokio::spawn(operation); // tokio-import-ok
    server.wait_for_requests(1).await;
    cancel.cancel();
    assert_eq!(task.await.unwrap().unwrap_err(), PapiHttpError::Canceled);
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn adapter_timeout_is_bounded_and_retryable() {
    let server = MockServer::new(|_| {
        let mut reply = Reply::raw(200, "{}");
        reply.delay = Duration::from_secs(2);
        reply
    })
    .await;
    let mut config = testing::config(&server.url);
    config.request_timeout = Duration::from_millis(50);
    let client =
        PapiHttpClient::new(&config, testing::gate(), Arc::new(AtomicTime::default())).unwrap();
    assert_eq!(
        client
            .get(&request(), &budget(), &CancellationToken::new())
            .await
            .unwrap_err(),
        PapiHttpError::Timeout
    );
    assert_eq!(server.requests().len(), 3);
}

#[rstest]
fn unknown_sdk_error_redacts_signed_locations() {
    let e = anyhow::Error::new(ConnectorError::ConnectorClientError {
        code: None,
        msg: "GET https://secret.invalid/?signature=SecretMarker".into(),
    });
    let e = PapiHttpError::from_sdk(&e);
    assert_eq!(e, PapiHttpError::Sdk);
    assert!(!format!("{e:?} {e}").contains("SecretMarker"));
}
