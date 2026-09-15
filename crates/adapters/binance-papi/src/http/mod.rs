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

//! Signed GET transport using the pinned SDK and Nautilus request policy.

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
        DerivativesTradingPortfolioMarginRestApi, rest_api::RestApi,
    },
};
use http::Method;
use nautilus_common::live::dst::time::{Instant, timeout};
use nautilus_core::{UnixNanos, time::AtomicTime};
use nautilus_network::{
    ratelimiter::{RateLimiter, clock::MonotonicClock, quota::Quota},
    retry::{RetryConfig, RetryManager},
};
use serde::Serialize;
use serde_json::value::RawValue;
use tokio_util::sync::CancellationToken;

use self::{error::PapiHttpError, query::PapiRequest};
use crate::{observations::MAX_RESPONSE_BYTES, read_only::BinancePapiReadOnlyConfig};

const WEIGHT_PER_MINUTE: NonZeroU32 = NonZeroU32::new(3_000).expect("Positive request quota");
const WEIGHT_BURST: NonZeroU32 = NonZeroU32::new(40).expect("Positive request burst");

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
    /// The exact GET endpoint version.
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

        // The adapter's timeout expires first, keeping timeout classification out of SDK strings
        let sdk_config = ConfigurationRestApi::builder()
            .api_key(config.api_key.expose_secret().to_owned())
            .api_secret(config.api_secret.expose_secret().to_owned())
            .base_path(config.base_url.expose_secret().to_owned())
            .timeout(timeout_ms + 1_000)
            .retries(0)
            .compression(false)
            .agent(HttpAgent(Arc::new(|builder| {
                builder
                    .use_rustls_tls()
                    .no_proxy()
                    .redirect(reqwest::redirect::Policy::none())
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
    concurrent: tokio::sync::Semaphore,
    closed: CancellationToken,
}

impl RequestGate {
    pub(crate) fn new(quota: Quota) -> Self {
        Self {
            limiter: RateLimiter::new_with_quota(Some(quota), vec![]),
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
