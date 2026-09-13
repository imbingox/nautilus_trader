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
use nautilus_network::ratelimiter::quota::Quota;
use rstest::rstest;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;
use crate::{
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
    let (canonical, signature) = request.query.rsplit_once("&signature=").unwrap();
    let key = hmac::Key::new(hmac::HMAC_SHA256, API_SECRET.as_bytes());
    let tag = hmac::sign(&key, canonical.as_bytes());
    let expected: String = tag
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert_eq!(signature, expected);
    assert_eq!(request.method, "GET");
    assert_eq!(request.api_key.as_deref(), Some(API_KEY));
    assert_eq!(request.params["recvWindow"], "5000");
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
    let server = MockServer::new(move |_| {
        if count.fetch_add(1, Ordering::SeqCst) == 0 {
            Reply::raw(503, r#"{"msg":"unavailable"}"#)
        } else {
            Reply::raw(200, "{}")
        }
    })
    .await;
    let client = http(&server, testing::gate());
    client
        .get(&request(), &budget(), &CancellationToken::new())
        .await
        .unwrap();
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
    let gate = testing::gate();
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
