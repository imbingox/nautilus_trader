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

//! Signed PAPI transport using the pinned SDK and Nautilus request policy.

pub(crate) mod command;
pub(crate) mod error;
pub(crate) mod query;

#[cfg(test)]
mod tests;

use std::{
    collections::BTreeMap,
    fmt::Debug,
    num::NonZeroU32,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU32, AtomicUsize, Ordering},
    },
    time::Duration,
};

use binance_sdk::{
    common::config::{ConfigurationRestApi, HttpAgent},
    derivatives_trading_portfolio_margin::{
        DerivativesTradingPortfolioMarginRestApi,
        rest_api::{RestApi, StartUserDataStreamResponse},
    },
};
use http::Method;
use nautilus_common::live::dst::time::{Instant, timeout};
use nautilus_core::{UnixNanos, string::secret::SecretString, time::AtomicTime};
use nautilus_network::{
    ratelimiter::{RateLimiter, clock::MonotonicClock, quota::Quota},
    retry::{RetryConfig, RetryManager},
};
use serde::Serialize;
use serde_json::value::RawValue;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use self::{
    command::{
        CancelUmOrderRequest, PapiCommandAcknowledgement, PapiCommandFailure, PapiCommandRequest,
        SubmitUmOrderRequest, UM_ORDER_ENDPOINT,
    },
    error::PapiHttpError,
    query::PapiRequest,
};
use crate::{observations::MAX_RESPONSE_BYTES, read_only::BinancePapiReadOnlyConfig};

const WEIGHT_PER_MINUTE: NonZeroU32 = NonZeroU32::new(3_000).expect("Positive request quota");
const WEIGHT_BURST: NonZeroU32 = NonZeroU32::new(40).expect("Positive request burst");
const ORDERS_PER_MINUTE: NonZeroU32 = NonZeroU32::new(1_000).expect("Positive order quota");
const ORDER_BURST: NonZeroU32 = NonZeroU32::new(20).expect("Positive order burst");
const LISTEN_KEY_ENDPOINT: &str = "/papi/v1/listenKey";

// A strong process-wide owner preserves quota and a throttle latch across client reconstruction
static SHARED_GATE: LazyLock<Arc<RequestGate>> = LazyLock::new(|| {
    Arc::new(RequestGate::new(
        Quota::per_minute(WEIGHT_PER_MINUTE).allow_burst(WEIGHT_BURST),
    ))
});

/// Successful response metadata, captured before the SDK consumes its decode closure.
///
/// Only numeric quota headers are exposed. Error headers are unavailable in SDK 69.2.1.
#[derive(Clone, Debug, Serialize)]
pub struct BinancePapiResponseMetadata {
    /// The exact endpoint version.
    pub endpoint: &'static str,
    /// The requested symbol, when the endpoint is symbol-scoped.
    pub symbol: Option<String>,
    /// HTTP response status.
    pub status: u16,
    /// Wall-clock time immediately before the signed request.
    pub ts_requested: UnixNanos,
    /// Wall-clock receipt time, after the SDK reads the body.
    pub ts_received: UnixNanos,
    /// Observed IP weight in the venue's minute window.
    pub used_weight_1m: Option<u32>,
    /// Observed account order count in the venue's minute window.
    pub order_count_1m: Option<u32>,
    /// Numeric Retry-After header in seconds, when present on a successful response.
    pub retry_after_seconds: Option<u32>,
}

pub(crate) struct PapiHttpClient {
    sdk: RestApi,
    gate: Arc<RequestGate>,
    request_timeout: Duration,
    clock: Arc<AtomicTime>,
}

impl PapiHttpClient {
    pub(crate) fn new(
        config: &BinancePapiReadOnlyConfig,
        gate: Arc<RequestGate>,
        clock: Arc<AtomicTime>,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let timeout_ms = u64::try_from(config.request_timeout.as_millis())?;
        let proxy = config
            .proxy_url
            .as_ref()
            .map(|url| reqwest::Proxy::all(url.expose_secret()))
            .transpose()
            .map_err(|_| anyhow::anyhow!("Could not configure PAPI proxy"))?;

        // The adapter's timeout expires first, keeping timeout classification out of SDK strings
        let sdk_config = ConfigurationRestApi::builder()
            .api_key(config.api_key.expose_secret().to_owned())
            .api_secret(config.api_secret.expose_secret().to_owned())
            .base_path(config.base_url.expose_secret().to_owned())
            .timeout(timeout_ms + 1_000)
            .retries(0)
            .compression(false)
            .agent(HttpAgent(Arc::new(move |builder| {
                let builder = builder
                    .use_rustls_tls()
                    .no_proxy()
                    .redirect(reqwest::redirect::Policy::none());

                match &proxy {
                    Some(proxy) => builder.proxy(proxy.clone()),
                    None => builder,
                }
            })))
            .build()
            .map_err(|_| anyhow::anyhow!("Could not configure PAPI SDK"))?;

        Ok(Self {
            sdk: DerivativesTradingPortfolioMarginRestApi::from_config(sdk_config),
            gate,
            request_timeout: config.request_timeout,
            clock,
        })
    }

    pub(crate) fn shared_gate() -> Arc<RequestGate> {
        Arc::clone(&SHARED_GATE)
    }

    pub(crate) fn is_throttled(&self) -> bool {
        self.gate.closed.is_cancelled()
    }

    pub(crate) fn now(&self) -> UnixNanos {
        self.clock.get_time_ns()
    }

    pub(crate) async fn get(
        &self,
        request: &PapiRequest,
        budget: &RequestBudget,
        cancel: &CancellationToken,
    ) -> Result<RawResponse, PapiHttpError> {
        // Collection timing includes quota waits and all attempts, not just the successful retry
        let requested_at = Instant::now();
        let retry = RetryManager::new(RetryConfig {
            max_retries: 2,
            initial_delay_ms: 200,
            max_delay_ms: 1_000,
            backoff_factor: 2.0,
            jitter_ms: 0,
            operation_timeout_ms: None,
            immediate_first: false,
            max_elapsed_ms: Some(budget.remaining_ms()?),
        });

        let response = retry
            .invocation(
                request.endpoint(),
                || self.attempt(request, budget, requested_at),
                PapiHttpError::retryable,
                |e| PapiHttpError::from_retry(&e),
            )
            .cancellation_token(cancel)
            .execute()
            .await?;
        budget.check()?;
        Ok(response)
    }

    pub(crate) async fn submit_um_order_with_barrier<F>(
        &self,
        request: &SubmitUmOrderRequest,
        budget: &RequestBudget,
        cancel: &CancellationToken,
        barrier: F,
    ) -> Result<PapiCommandResponse, PapiCommandDispatchError>
    where
        F: FnOnce() -> anyhow::Result<()>,
    {
        self.command_with_barrier(PapiCommandRequest::Submit(request), budget, cancel, barrier)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn submit_um_order(
        &self,
        request: &SubmitUmOrderRequest,
        budget: &RequestBudget,
        cancel: &CancellationToken,
    ) -> Result<PapiCommandResponse, PapiCommandFailure> {
        self.submit_um_order_with_barrier(request, budget, cancel, || Ok(()))
            .await
            .map_err(PapiCommandDispatchError::into_command_failure)
    }

    pub(crate) async fn cancel_um_order_with_barrier<F>(
        &self,
        request: &CancelUmOrderRequest,
        budget: &RequestBudget,
        cancel: &CancellationToken,
        barrier: F,
    ) -> Result<PapiCommandResponse, PapiCommandDispatchError>
    where
        F: FnOnce() -> anyhow::Result<()>,
    {
        self.command_with_barrier(PapiCommandRequest::Cancel(request), budget, cancel, barrier)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn cancel_um_order(
        &self,
        request: &CancelUmOrderRequest,
        budget: &RequestBudget,
        cancel: &CancellationToken,
    ) -> Result<PapiCommandResponse, PapiCommandFailure> {
        self.cancel_um_order_with_barrier(request, budget, cancel, || Ok(()))
            .await
            .map_err(PapiCommandDispatchError::into_command_failure)
    }

    async fn command_with_barrier<F>(
        &self,
        request: PapiCommandRequest<'_>,
        budget: &RequestBudget,
        cancel: &CancellationToken,
        barrier: F,
    ) -> Result<PapiCommandResponse, PapiCommandDispatchError>
    where
        F: FnOnce() -> anyhow::Result<()>,
    {
        let requested_at = Instant::now();
        budget
            .charge_request()
            .map_err(PapiCommandFailure::before_dispatch)
            .map_err(PapiCommandDispatchError::Command)?;

        let permit = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                return Err(PapiCommandFailure::before_dispatch(PapiHttpError::Canceled).into());
            }
            () = self.gate.closed.cancelled() => {
                return Err(PapiCommandFailure::before_dispatch(PapiHttpError::GateClosed).into());
            }
            permit = self.gate.concurrent.acquire() => {
                permit
                    .map_err(|_| PapiCommandFailure::before_dispatch(PapiHttpError::GateClosed))
                    .map_err(PapiCommandDispatchError::Command)?
            }
        };
        let weight_keys = [()];

        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                return Err(PapiCommandFailure::before_dispatch(PapiHttpError::Canceled).into());
            }
            () = self.gate.closed.cancelled() => {
                return Err(PapiCommandFailure::before_dispatch(PapiHttpError::GateClosed).into());
            }
            () = self.gate.limiter.await_keys_ready(Some(&weight_keys)) => {}
        }

        if request.uses_order_quota() {
            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    return Err(PapiCommandFailure::before_dispatch(PapiHttpError::Canceled).into());
                }
                () = self.gate.closed.cancelled() => {
                    return Err(PapiCommandFailure::before_dispatch(PapiHttpError::GateClosed).into());
                }
                () = self.gate.order_limiter.await_keys_ready(Some(&weight_keys)) => {}
            }
        }

        budget
            .check()
            .map_err(PapiCommandFailure::before_dispatch)
            .map_err(PapiCommandDispatchError::Command)?;

        if cancel.is_cancelled() {
            return Err(PapiCommandFailure::before_dispatch(PapiHttpError::Canceled).into());
        }

        if self.gate.closed.is_cancelled() {
            return Err(PapiCommandFailure::before_dispatch(PapiHttpError::GateClosed).into());
        }

        // This synchronous durability barrier is the final fallible step before socket dispatch.
        barrier().map_err(PapiCommandDispatchError::Barrier)?;

        let result = tokio::select! {
            biased;
            result = timeout(
                self.request_timeout,
                self.send_command(&request, requested_at),
            ) => result.unwrap_or(Err(PapiHttpError::Timeout)),
            () = cancel.cancelled() => Err(PapiHttpError::Canceled),
        };

        if matches!(result, Err(PapiHttpError::Throttled { .. })) {
            self.gate.closed.cancel();
        }

        drop(permit);
        let raw = result
            .map_err(|e| PapiCommandFailure::after_dispatch(&request, e))
            .map_err(PapiCommandDispatchError::Command)?;
        let acknowledgement = PapiCommandAcknowledgement::decode(&request, raw.body.get())
            .map_err(|e| PapiCommandFailure::after_dispatch(&request, e))
            .map_err(PapiCommandDispatchError::Command)?;
        Ok(PapiCommandResponse {
            raw,
            acknowledgement,
        })
    }

    async fn send_command(
        &self,
        request: &PapiCommandRequest<'_>,
        requested_at: Instant,
    ) -> Result<RawResponse, PapiHttpError> {
        let ts_requested = self.clock.get_time_ns();
        let response = self
            .sdk
            .send_signed_request::<Box<RawValue>>(
                UM_ORDER_ENDPOINT,
                request.method(),
                request.params(),
                BTreeMap::new(),
            )
            .await
            .map_err(|e| PapiHttpError::from_sdk(&e))?;

        if !(200..300).contains(&response.status) {
            return Err(PapiHttpError::Rejected {
                status: Some(response.status),
                code: None,
            });
        }

        let received_at = Instant::now();
        let metadata = BinancePapiResponseMetadata {
            endpoint: UM_ORDER_ENDPOINT,
            symbol: Some(request.symbol().to_owned()),
            status: response.status,
            ts_requested,
            ts_received: self.clock.get_time_ns(),
            used_weight_1m: response
                .headers
                .get("x-mbx-used-weight-1m")
                .and_then(|v| v.parse().ok()),
            order_count_1m: response
                .headers
                .get("x-mbx-order-count-1m")
                .and_then(|v| v.parse().ok()),
            retry_after_seconds: response
                .headers
                .get("retry-after")
                .and_then(|v| v.parse().ok()),
        };
        let body = response.data().await.map_err(|_| PapiHttpError::Decode)?;

        if body.get().len() > MAX_RESPONSE_BYTES {
            return Err(PapiHttpError::ResponseTooLarge);
        }

        Ok(RawResponse {
            body,
            metadata,
            requested_at,
            received_at,
        })
    }

    pub(crate) async fn create_listen_key(
        &self,
        budget: &RequestBudget,
        cancel: &CancellationToken,
    ) -> Result<ListenKey, PapiHttpError> {
        let response = self
            .session_request(ListenKeyOperation::Create, budget, cancel)
            .await?;
        let key = response.listen_key.ok_or(PapiHttpError::Decode)?;
        ListenKey::new(key)
    }

    pub(crate) async fn keepalive_listen_key(
        &self,
        budget: &RequestBudget,
        cancel: &CancellationToken,
    ) -> Result<(), PapiHttpError> {
        self.session_request(ListenKeyOperation::Keepalive, budget, cancel)
            .await
            .map(|_| ())
    }

    pub(crate) async fn close_listen_key(
        &self,
        budget: &RequestBudget,
        cancel: &CancellationToken,
    ) -> Result<(), PapiHttpError> {
        self.session_request(ListenKeyOperation::Close, budget, cancel)
            .await
            .map(|_| ())
    }

    async fn session_request(
        &self,
        operation: ListenKeyOperation,
        budget: &RequestBudget,
        cancel: &CancellationToken,
    ) -> Result<SessionResponse, PapiHttpError> {
        let requested_at = Instant::now();
        let retry = RetryManager::new(RetryConfig {
            max_retries: operation.max_retries(),
            initial_delay_ms: 200,
            max_delay_ms: 1_000,
            backoff_factor: 2.0,
            jitter_ms: 0,
            operation_timeout_ms: None,
            immediate_first: false,
            max_elapsed_ms: Some(budget.remaining_ms()?),
        });

        let response = retry
            .invocation(
                operation.name(),
                || self.attempt_session(operation, budget, requested_at),
                PapiHttpError::retryable,
                |e| PapiHttpError::from_retry(&e),
            )
            .cancellation_token(cancel)
            .execute()
            .await?;
        budget.check()?;
        Ok(response)
    }

    async fn attempt_session(
        &self,
        operation: ListenKeyOperation,
        budget: &RequestBudget,
        requested_at: Instant,
    ) -> Result<SessionResponse, PapiHttpError> {
        budget.charge_request()?;

        let permit = tokio::select! {
            biased;
            () = self.gate.closed.cancelled() => return Err(PapiHttpError::GateClosed),
            permit = self.gate.concurrent.acquire() => {
                permit.map_err(|_| PapiHttpError::GateClosed)?
            }
        };
        let keys = [()];

        tokio::select! {
            biased;
            () = self.gate.closed.cancelled() => return Err(PapiHttpError::GateClosed),
            () = self.gate.limiter.await_keys_ready(Some(&keys)) => {}
        }

        budget.check()?;
        let result = tokio::select! {
            biased;
            () = self.gate.closed.cancelled() => Err(PapiHttpError::GateClosed),
            result = timeout(
                self.request_timeout,
                self.send_session(operation, requested_at),
            ) => result.unwrap_or(Err(PapiHttpError::Timeout)),
        };

        if matches!(result, Err(PapiHttpError::Throttled { .. })) {
            self.gate.closed.cancel();
        }

        drop(permit);
        result
    }

    async fn send_session(
        &self,
        operation: ListenKeyOperation,
        requested_at: Instant,
    ) -> Result<SessionResponse, PapiHttpError> {
        let ts_requested = self.clock.get_time_ns();
        let response = self
            .sdk
            .send_request::<StartUserDataStreamResponse>(
                LISTEN_KEY_ENDPOINT,
                operation.method(),
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .await
            .map_err(|e| PapiHttpError::from_sdk(&e))?;

        if !(200..300).contains(&response.status) {
            return Err(PapiHttpError::Rejected {
                status: Some(response.status),
                code: None,
            });
        }

        let metadata = BinancePapiResponseMetadata {
            endpoint: LISTEN_KEY_ENDPOINT,
            symbol: None,
            status: response.status,
            ts_requested,
            ts_received: self.clock.get_time_ns(),
            used_weight_1m: response
                .headers
                .get("x-mbx-used-weight-1m")
                .and_then(|v| v.parse().ok()),
            order_count_1m: response
                .headers
                .get("x-mbx-order-count-1m")
                .and_then(|v| v.parse().ok()),
            retry_after_seconds: response
                .headers
                .get("retry-after")
                .and_then(|v| v.parse().ok()),
        };
        let received_at = Instant::now();
        let listen_key = if operation == ListenKeyOperation::Create {
            response
                .data()
                .await
                .map_err(|_| PapiHttpError::Decode)?
                .listen_key
        } else {
            // Binance permits empty keepalive and close bodies. Do not force JSON decoding.
            None
        };

        Ok(SessionResponse {
            listen_key,
            metadata,
            requested_at,
            received_at,
        })
    }

    async fn attempt(
        &self,
        request: &PapiRequest,
        budget: &RequestBudget,
        requested_at: Instant,
    ) -> Result<RawResponse, PapiHttpError> {
        budget.charge_request()?;

        let permit = tokio::select! {
            biased;
            () = self.gate.closed.cancelled() => return Err(PapiHttpError::GateClosed),
            permit = self.gate.concurrent.acquire() => {
                permit.map_err(|_| PapiHttpError::GateClosed)?
            }
        };
        let keys = vec![(); request.weight()];

        tokio::select! {
            biased;
            () = self.gate.closed.cancelled() => return Err(PapiHttpError::GateClosed),
            () = self.gate.limiter.await_keys_ready(Some(&keys)) => {}
        }

        budget.check()?;

        // Each retry invokes the SDK after acquiring weight, generating a fresh timestamp/signature
        let result = tokio::select! {
            biased;
            () = self.gate.closed.cancelled() => Err(PapiHttpError::GateClosed),
            result = timeout(self.request_timeout, self.send(request, requested_at)) => {
                result.unwrap_or(Err(PapiHttpError::Timeout))
            }
        };

        if matches!(result, Err(PapiHttpError::Throttled { .. })) {
            // SDK error headers are lost, so even a header-only Retry-After cannot reopen this gate
            self.gate.closed.cancel();
        }

        drop(permit);
        result
    }

    async fn send(
        &self,
        request: &PapiRequest,
        requested_at: Instant,
    ) -> Result<RawResponse, PapiHttpError> {
        let ts_requested = self.clock.get_time_ns();
        let response = self
            .sdk
            .send_signed_request::<Box<RawValue>>(
                request.endpoint(),
                Method::GET,
                request.params(),
                BTreeMap::new(),
            )
            .await
            .map_err(|e| PapiHttpError::from_sdk(&e))?;

        // Redirects are disabled; no alternate host receives the API key or signed query
        if !(200..300).contains(&response.status) {
            return Err(PapiHttpError::Rejected {
                status: Some(response.status),
                code: None,
            });
        }

        let received_at = Instant::now();
        let metadata = BinancePapiResponseMetadata {
            endpoint: request.endpoint(),
            symbol: request.symbol().map(str::to_owned),
            status: response.status,
            ts_requested,
            ts_received: self.clock.get_time_ns(),
            used_weight_1m: response
                .headers
                .get("x-mbx-used-weight-1m")
                .and_then(|v| v.parse().ok()),
            order_count_1m: response
                .headers
                .get("x-mbx-order-count-1m")
                .and_then(|v| v.parse().ok()),
            retry_after_seconds: response
                .headers
                .get("retry-after")
                .and_then(|v| v.parse().ok()),
        };
        let body = response.data().await.map_err(|_| PapiHttpError::Decode)?;

        // The SDK buffers before exposing the response; this bounds parsing/retention, not its buffer
        if body.get().len() > MAX_RESPONSE_BYTES {
            return Err(PapiHttpError::ResponseTooLarge);
        }

        Ok(RawResponse {
            body,
            metadata,
            requested_at,
            received_at,
        })
    }
}

/// Failure from the quota-to-dispatch boundary.
#[derive(Debug, Error)]
pub(crate) enum PapiCommandDispatchError {
    #[error("{0:?}")]
    Command(PapiCommandFailure),
    #[error("PAPI durable dispatch barrier failed")]
    Barrier(#[source] anyhow::Error),
}

/// A validated write acknowledgement and its transport metadata.
#[derive(Debug)]
pub(crate) struct PapiCommandResponse {
    pub(crate) raw: RawResponse,
    pub(crate) acknowledgement: PapiCommandAcknowledgement,
}

impl From<PapiCommandFailure> for PapiCommandDispatchError {
    fn from(value: PapiCommandFailure) -> Self {
        Self::Command(value)
    }
}

#[cfg(test)]
impl PapiCommandDispatchError {
    fn into_command_failure(self) -> PapiCommandFailure {
        match self {
            Self::Command(failure) => failure,
            Self::Barrier(error) => panic!("test command barrier unexpectedly failed: {error}"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ListenKeyOperation {
    Create,
    Keepalive,
    Close,
}

impl ListenKeyOperation {
    const fn method(self) -> Method {
        match self {
            Self::Create => Method::POST,
            Self::Keepalive => Method::PUT,
            Self::Close => Method::DELETE,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Create => "PAPI listen key create",
            Self::Keepalive => "PAPI listen key keepalive",
            Self::Close => "PAPI listen key close",
        }
    }

    const fn max_retries(self) -> u32 {
        match self {
            // POST returns the existing active key and PUT is idempotent by venue contract.
            Self::Create | Self::Keepalive => 2,
            // DELETE has account-global effect and must never race a replacement session.
            Self::Close => 0,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ListenKey(SecretString);

impl ListenKey {
    fn new(value: String) -> Result<Self, PapiHttpError> {
        if value.is_empty()
            || value.len() > 1_024
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(PapiHttpError::Decode);
        }

        Ok(Self(value.into()))
    }

    pub(crate) fn expose_secret(&self) -> &str {
        self.0.expose_secret()
    }
}

struct SessionResponse {
    listen_key: Option<String>,
    #[allow(dead_code, reason = "retained for typed session evidence")]
    metadata: BinancePapiResponseMetadata,
    #[allow(dead_code, reason = "retained for typed session evidence")]
    requested_at: Instant,
    #[allow(dead_code, reason = "retained for typed session evidence")]
    received_at: Instant,
}

impl Debug for PapiHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(PapiHttpClient))
            .field("request_timeout", &self.request_timeout)
            .field("throttled", &self.is_throttled())
            .finish_non_exhaustive()
    }
}

pub(crate) struct RawResponse {
    pub(crate) body: Box<RawValue>,
    pub(crate) metadata: BinancePapiResponseMetadata,
    /// Logical collection start, including quota waits and failed attempts.
    pub(crate) requested_at: Instant,
    pub(crate) received_at: Instant,
}

impl Debug for RawResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RawResponse))
            .field("metadata", &self.metadata)
            .field("body_bytes", &self.body.get().len())
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct RequestGate {
    limiter: RateLimiter<(), MonotonicClock>,
    order_limiter: RateLimiter<(), MonotonicClock>,
    concurrent: tokio::sync::Semaphore,
    closed: CancellationToken,
}

impl RequestGate {
    pub(crate) fn new(quota: Quota) -> Self {
        Self::with_order_quota(
            quota,
            Quota::per_minute(ORDERS_PER_MINUTE).allow_burst(ORDER_BURST),
        )
    }

    fn with_order_quota(quota: Quota, order_quota: Quota) -> Self {
        Self {
            limiter: RateLimiter::new_with_quota(Some(quota), vec![]),
            order_limiter: RateLimiter::new_with_quota(Some(order_quota), vec![]),
            concurrent: tokio::sync::Semaphore::new(4),
            closed: CancellationToken::new(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct RequestBudget {
    deadline: Instant,
    requests_left: AtomicU32,
    rows: AtomicUsize,
    max_rows: usize,
}

impl RequestBudget {
    pub(crate) fn new(
        timeout: Duration,
        max_requests: u32,
        max_rows: usize,
    ) -> Result<Self, PapiHttpError> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(PapiHttpError::Configuration)?;
        Ok(Self {
            deadline,
            requests_left: AtomicU32::new(max_requests),
            rows: AtomicUsize::new(0),
            max_rows,
        })
    }

    pub(crate) fn check(&self) -> Result<(), PapiHttpError> {
        if Instant::now() >= self.deadline {
            return Err(PapiHttpError::Budget);
        }

        Ok(())
    }

    pub(crate) fn remaining_ms(&self) -> Result<u64, PapiHttpError> {
        let remaining = self
            .deadline
            .saturating_duration_since(Instant::now())
            .as_millis();
        let remaining = u64::try_from(remaining).map_err(|_| PapiHttpError::Configuration)?;

        if remaining == 0 {
            return Err(PapiHttpError::Budget);
        }

        Ok(remaining)
    }

    fn charge_request(&self) -> Result<(), PapiHttpError> {
        self.check()?;
        self.requests_left
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
            .map_err(|_| PapiHttpError::Budget)?;
        Ok(())
    }

    pub(crate) fn charge_rows(&self, count: usize) -> Result<(), PapiHttpError> {
        self.check()?;
        self.rows
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(count).filter(|n| *n <= self.max_rows)
            })
            .map_err(|_| PapiHttpError::Budget)?;
        Ok(())
    }
}
