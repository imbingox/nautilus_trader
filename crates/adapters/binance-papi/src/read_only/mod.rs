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

//! Signed PAPI account-wide current reads and explicitly scoped historical coverage.
//!
//! This query surface does not publish an account, connect a LiveNode, or enable trading.
//! Historical reports remain incomplete until authenticated venue semantics are verified.

mod config;
mod projection;
mod risk;

#[cfg(test)]
mod current_tests;
#[cfg(test)]
mod engine_tests;
#[cfg(test)]
mod tests;

use std::{borrow::Cow, fmt::Debug, sync::Arc, time::Duration};

use nautilus_common::live::dst::time::Instant;
use nautilus_core::{UnixNanos, time::AtomicTime};
use nautilus_model::{
    events::AccountState,
    identifiers::{AccountId, ClientOrderId, InstrumentId, VenueOrderId},
    instruments::InstrumentAny,
    reports::{ExecutionMassStatus, OrderStatusReport, PositionStatusReport},
};
use parking_lot::Mutex;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

pub use self::config::BinancePapiReadOnlyConfig;
pub use crate::http::BinancePapiResponseMetadata;
use crate::{
    http::{
        PapiCommandResponse, PapiHttpClient, RequestBudget, RequestGate, error::PapiHttpError,
        query::PapiRequest,
    },
    observations::{
        AccountObservation, ObservationFailure, ObservationSlot, ObservationSource,
        ObservationTiming, ReceiptStatus,
    },
    reports::{InstrumentScope, ReportCollector, history::HistoryWindow},
    trading::coordinator::{
        CoordinatorDispatchError, PapiCommandCoordinator, dispatch_cancel_shared,
        dispatch_submit_shared,
    },
};

/// A bounded set of current and historical observations, with explicit incompleteness.
///
/// `mass_status.reports_complete()` remains false in this development stage. Successful
/// paging does not establish retention, order selection time, or complete algo discovery.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.adapters.binance_papi",
        frozen,
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.binance_papi")
)]
pub struct BinancePapiReadOnlySnapshot {
    /// Engine report representation, carrying the exact lower history bound.
    pub mass_status: ExecutionMassStatus,
    /// Fixed inclusive upper history bound used throughout the operation.
    pub window_end: UnixNanos,
    /// Explicit metadata scope scanned, including symbols with no current exposure.
    pub instrument_ids: Vec<InstrumentId>,
    /// Coverage limitations and failed historical sources.
    pub issues: Vec<String>,
    /// Receipt and quota metadata for each successful response.
    pub responses: Vec<BinancePapiResponseMetadata>,
}

/// Availability of one typed account projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BinancePapiProjectionStatus {
    /// All required sources are recent and semantically supported.
    Available,
    /// A source contains a product or economic state outside the supported scope.
    Unsupported,
    /// Sources disagree in generation or collection timing.
    Inconsistent,
    /// A required source has not been observed.
    Missing,
    /// A required source is older than the caller's freshness bound.
    Stale,
    /// A required source failed its most recent refresh.
    Failed,
    /// A replacement observation is still being collected.
    Refreshing,
    /// The owning client has been canceled.
    Canceled,
}

/// Typed account projection retained for the private-session recovery path.
#[derive(Clone, Debug, Serialize)]
pub struct BinancePapiAccountProjection {
    /// Portfolio Margin account identity.
    pub account_id: AccountId,
    /// Reported totals-only wallet state, when every required source is valid.
    pub account_state: Option<AccountState>,
    /// Wallet projection status.
    pub wallet_status: BinancePapiProjectionStatus,
    /// Wallet limitations or validation failures.
    pub wallet_issues: Vec<String>,
    /// Independent Portfolio Margin risk-source status.
    pub risk_status: BinancePapiProjectionStatus,
    /// Risk-source limitations or validation failures.
    pub risk_issues: Vec<String>,
    /// Endpoint and successful observation generation for every contributing source.
    pub source_generations: Vec<(&'static str, u64)>,
    /// Wallet collection span in monotonic nanoseconds.
    pub wallet_collection_span_ns: Option<u128>,
    /// Risk collection span in monotonic nanoseconds.
    pub risk_collection_span_ns: Option<u128>,
    /// Always false in issue #4; trade admission belongs to the subsequent phase.
    pub trading_authorized: bool,
}

impl BinancePapiReadOnlySnapshot {
    /// Serializes reports, exact history bounds, coverage issues, and response metadata.
    ///
    /// The output contains private account and execution data. It is evidence for review,
    /// not a statement of historical completeness or economic account validity.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be serialized.
    pub fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }
}

/// A cloneable PAPI GET client; clones share cancellation and retained observations.
///
/// All instances share a process-wide IP gate: 3000 weight/minute, burst 40, four concurrent
/// attempts. This reserves headroom below the venue's documented 6000 weight/minute allowance;
/// other processes require separate coordination. A throttle or ban latches the gate closed.
/// SDK 69.2.1 discards error headers, so automatic throttle recovery is unavailable.
#[derive(Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.adapters.binance_papi",
        frozen,
        from_py_object
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.binance_papi")
)]
pub struct BinancePapiReadOnlyClient {
    inner: Arc<ReadOnlyInner>,
}

impl BinancePapiReadOnlyClient {
    /// Constructs a read-only client from explicit credentials and optional preloaded UM instruments.
    ///
    /// Current reports load missing metadata from public USD-M exchange information.
    /// Supplied instruments define the separate historical and trading recovery scope.
    /// Construction performs no network requests and reads no environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid credentials, resource bounds, origin, account or instrument scope.
    pub fn new(
        config: &BinancePapiReadOnlyConfig,
        instruments: Vec<InstrumentAny>,
    ) -> anyhow::Result<Self> {
        Self::from_parts(
            config,
            instruments,
            PapiHttpClient::shared_gate(),
            Arc::new(AtomicTime::default()),
        )
    }

    pub(crate) fn from_parts(
        config: &BinancePapiReadOnlyConfig,
        instruments: Vec<InstrumentAny>,
        gate: Arc<RequestGate>,
        clock: Arc<AtomicTime>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            instruments.len() <= config.max_rows,
            "PAPI instrument metadata exceeds row budget"
        );
        let scope = InstrumentScope::new(instruments)?;
        let http = PapiHttpClient::new(config, gate, Arc::clone(&clock))?;
        let sources = observation_sources();
        let observations = sources
            .into_iter()
            .map(|source| StoredObservation {
                slot: ObservationSlot::new(config.account_id, source),
                metadata: None,
            })
            .collect();

        Ok(Self {
            inner: Arc::new(ReadOnlyInner {
                http,
                current_instruments: Mutex::new(scope.clone()),
                scope,
                clock,
                account_id: config.account_id,
                operation_timeout: config.operation_timeout,
                max_requests: config.max_requests,
                max_rows: config.max_rows,
                cancel: CancellationToken::new(),
                refresh: tokio::sync::Mutex::new(()),
                observations: Mutex::new(AccountObservations {
                    generation: 0,
                    sources: observations,
                }),
            }),
        })
    }

    /// Cancels outstanding and future operations on this client and all its clones.
    pub fn cancel(&self) {
        self.inner.cancel.cancel();

        for stored in &mut self.inner.observations.lock().sources {
            stored.slot.record_canceled();
        }
    }

    /// Returns whether the shared IP gate has been latched closed by a throttle or ban.
    #[must_use]
    pub fn is_throttled(&self) -> bool {
        self.inner.http.is_throttled()
    }

    /// Refreshes account, product-scope, and UM V1/V2 observations for later projection.
    ///
    /// Successful sources are retained independently; a failure preserves that source's prior
    /// response and marks it failed. All nine sources share one generation and operation budget.
    ///
    /// # Errors
    ///
    /// Returns an error for cancellation, quota/deadline exhaustion, any failed request, or invalid JSON.
    pub async fn refresh_account_observations(&self) -> anyhow::Result<()> {
        let budget = self.budget()?;

        let _guard = tokio::select! {
            biased;
            () = self.inner.cancel.cancelled() => return Err(PapiHttpError::Canceled.into()),
            result = tokio::time::timeout(
                Duration::from_millis(budget.remaining_ms()?),
                self.inner.refresh.lock(),
            ) => result.map_err(|_| PapiHttpError::Budget)?,
        };
        let generation = {
            let mut observations = self.inner.observations.lock();

            if self.inner.cancel.is_cancelled() {
                return Err(PapiHttpError::Canceled.into());
            }

            observations.generation = observations
                .generation
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("PAPI observation generation overflow"))?;

            // Retained diagnostic values are unavailable while their replacement is pending
            for stored in &mut observations.sources {
                stored.slot.record_refresh_started();
            }

            observations.generation
        };
        let mut refresh = ObservationRefresh {
            observations: &self.inner.observations,
            completed: false,
        };
        let mut failures = Vec::new();

        for (index, source) in observation_sources().into_iter().enumerate() {
            let response = self
                .inner
                .http
                .get(
                    &PapiRequest::Observation(source.clone()),
                    &budget,
                    &self.inner.cancel,
                )
                .await;
            let mut observations = self.inner.observations.lock();
            let stored = &mut observations.sources[index];

            let result = match response {
                Ok(response) => {
                    if self.inner.cancel.is_cancelled() {
                        stored.slot.record_canceled();
                        return Err(PapiHttpError::Canceled.into());
                    }

                    let result = stored.slot.record_response(
                        response.body.get(),
                        generation,
                        response.metadata.ts_received,
                        response.requested_at,
                        response.received_at,
                    );

                    if result.is_ok() {
                        stored.metadata = Some(response.metadata);
                    }

                    result
                }
                Err(e) => {
                    if e == PapiHttpError::Canceled {
                        stored.slot.record_canceled();
                        return Err(e.into());
                    }

                    stored.slot.record_failure(e.to_string());
                    Err(e.into())
                }
            };

            if let Err(e) = result {
                failures.push(format!("{}: {e}", source.endpoint()));
            }
        }

        if let Err(e) = budget.check() {
            let mut observations = self.inner.observations.lock();

            if self.inner.cancel.is_cancelled() {
                return Err(PapiHttpError::Canceled.into());
            }

            for stored in &mut observations.sources {
                stored.slot.record_failure(e.to_string());
            }

            refresh.completed = true;
            return Err(e.into());
        }

        refresh.completed = true;
        anyhow::ensure!(
            failures.is_empty(),
            "PAPI observation refresh failed: {}",
            failures.join("; ")
        );
        Ok(())
    }

    /// Serializes retained observations and their current receipt status as exact JSON evidence.
    ///
    /// The output contains private account data. Recent receipt does not establish economic
    /// validity or authorize order admission. Original JSON and numeric strings are preserved.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero receipt-age bound or if serialization fails.
    pub fn account_observations_json(&self, max_receipt_age: Duration) -> anyhow::Result<String> {
        anyhow::ensure!(
            !max_receipt_age.is_zero(),
            "PAPI maximum receipt age must be positive"
        );
        let observations = self.inner.observations.lock();
        let now = Instant::now();
        let canceled = ObservationFailure::Canceled;
        let is_canceled = self.inner.cancel.is_cancelled();
        let views: Vec<_> = observation_sources()
            .iter()
            .zip(&observations.sources)
            .map(|(source, stored)| ObservationView {
                endpoint: source.endpoint(),
                receipt_status: if is_canceled {
                    ReceiptStatus::Canceled
                } else {
                    stored.slot.receipt_status(now, max_receipt_age)
                },
                failure: if is_canceled {
                    Some(&canceled)
                } else {
                    stored.slot.failure()
                },
                timing: stored.slot.timing(now),
                observation: stored.slot.last_response(),
                metadata: stored.metadata.as_ref(),
            })
            .collect();
        Ok(serde_json::to_string(&views)?)
    }

    /// Projects retained observations into independent wallet and PM risk results.
    ///
    /// This is a read-only diagnostic snapshot. It neither updates the cache nor authorizes
    /// trading. A failed projection contains reasons and never publishes a partial account state.
    ///
    /// # Errors
    ///
    /// Returns an error for zero freshness bounds or if serialization fails.
    pub fn account_snapshot_json(
        &self,
        max_receipt_age: Duration,
        max_collection_span: Duration,
    ) -> anyhow::Result<String> {
        anyhow::ensure!(
            !max_receipt_age.is_zero(),
            "PAPI maximum receipt age must be positive"
        );
        anyhow::ensure!(
            !max_collection_span.is_zero(),
            "PAPI maximum collection span must be positive"
        );
        let observations = self.inner.observations.lock();
        let context = projection::AccountProjectionContext {
            account_id: self.inner.account_id,
            scope: &self.inner.scope,
            ts_init: self.inner.clock.get_time_ns(),
            now: Instant::now(),
            max_receipt_age,
            max_collection_span,
            canceled: self.inner.cancel.is_cancelled(),
        };
        let snapshot = projection::project_account_snapshot(&observations.sources, &context);
        Ok(serde_json::to_string(&snapshot)?)
    }

    /// Projects retained observations without serializing and reparsing adapter-owned JSON.
    ///
    /// # Errors
    ///
    /// Returns an error for zero freshness or collection-span bounds.
    pub fn account_projection(
        &self,
        max_receipt_age: Duration,
        max_collection_span: Duration,
    ) -> anyhow::Result<BinancePapiAccountProjection> {
        anyhow::ensure!(
            !max_receipt_age.is_zero(),
            "PAPI maximum receipt age must be positive"
        );
        anyhow::ensure!(
            !max_collection_span.is_zero(),
            "PAPI maximum collection span must be positive"
        );
        let observations = self.inner.observations.lock();
        let context = projection::AccountProjectionContext {
            account_id: self.inner.account_id,
            scope: &self.inner.scope,
            ts_init: self.inner.clock.get_time_ns(),
            now: Instant::now(),
            max_receipt_age,
            max_collection_span,
            canceled: self.inner.cancel.is_cancelled(),
        };
        let snapshot = projection::project_account_snapshot(&observations.sources, &context);
        let mut source_generations = Vec::new();

        for source in snapshot
            .wallet
            .sources
            .iter()
            .chain(&snapshot.portfolio_margin_risk.sources)
        {
            if let Some(generation) = source.generation()
                && !source_generations
                    .iter()
                    .any(|(endpoint, _)| *endpoint == source.endpoint())
            {
                source_generations.push((source.endpoint(), generation));
            }
        }

        Ok(BinancePapiAccountProjection {
            account_id: snapshot.account_id,
            account_state: snapshot.wallet.value,
            wallet_status: snapshot.wallet.status.into(),
            wallet_issues: snapshot.wallet.issues,
            risk_status: snapshot.portfolio_margin_risk.status.into(),
            risk_issues: snapshot.portfolio_margin_risk.issues,
            source_generations,
            wallet_collection_span_ns: snapshot.wallet.collection_span_ns,
            risk_collection_span_ns: snapshot.portfolio_margin_risk.collection_span_ns,
            trading_authorized: snapshot.trading_authorized,
        })
    }

    pub(crate) async fn create_listen_key(&self) -> anyhow::Result<crate::http::ListenKey> {
        let budget = self.budget()?;
        Ok(self
            .inner
            .http
            .create_listen_key(&budget, &self.inner.cancel)
            .await?)
    }

    pub(crate) async fn keepalive_listen_key(&self) -> anyhow::Result<()> {
        let budget = self.budget()?;
        Ok(self
            .inner
            .http
            .keepalive_listen_key(&budget, &self.inner.cancel)
            .await?)
    }

    pub(crate) async fn close_listen_key(&self) -> anyhow::Result<()> {
        let budget = self.budget()?;
        Ok(self
            .inner
            .http
            .close_listen_key(&budget, &self.inner.cancel)
            .await?)
    }

    pub(crate) fn now(&self) -> UnixNanos {
        self.inner.clock.get_time_ns()
    }

    pub(crate) fn instrument_ids(&self) -> Vec<InstrumentId> {
        self.inner.scope.instrument_ids()
    }

    pub(crate) async fn dispatch_submit<F>(
        &self,
        coordinator: &Arc<Mutex<Option<PapiCommandCoordinator>>>,
        operation_id: nautilus_core::UUID4,
        after_barrier: F,
    ) -> Result<PapiCommandResponse, CoordinatorDispatchError>
    where
        F: FnOnce() -> anyhow::Result<()> + Send,
    {
        let budget = self.budget().map_err(|e| {
            CoordinatorDispatchError::Command(
                crate::http::command::PapiCommandFailure::before_dispatch(
                    e.downcast::<PapiHttpError>()
                        .unwrap_or(PapiHttpError::Configuration),
                ),
            )
        })?;
        dispatch_submit_shared(
            coordinator,
            operation_id,
            &self.inner.http,
            &budget,
            &self.inner.cancel,
            after_barrier,
        )
        .await
    }

    pub(crate) async fn dispatch_cancel<F>(
        &self,
        coordinator: &Arc<Mutex<Option<PapiCommandCoordinator>>>,
        operation_id: nautilus_core::UUID4,
        after_barrier: F,
    ) -> Result<PapiCommandResponse, CoordinatorDispatchError>
    where
        F: FnOnce() -> anyhow::Result<()> + Send,
    {
        let budget = self.budget().map_err(|e| {
            CoordinatorDispatchError::Command(
                crate::http::command::PapiCommandFailure::before_dispatch(
                    e.downcast::<PapiHttpError>()
                        .unwrap_or(PapiHttpError::Configuration),
                ),
            )
        })?;
        dispatch_cancel_shared(
            coordinator,
            operation_id,
            &self.inner.http,
            &budget,
            &self.inner.cancel,
            after_barrier,
        )
        .await
    }

    pub(crate) async fn dispatch_cancel_batch(
        &self,
        coordinator: &Arc<Mutex<Option<PapiCommandCoordinator>>>,
        operation_ids: &[nautilus_core::UUID4],
    ) -> Vec<(
        nautilus_core::UUID4,
        Result<PapiCommandResponse, CoordinatorDispatchError>,
    )> {
        let budget = match self.budget() {
            Ok(budget) => budget,
            Err(e) => {
                let message = e.to_string();
                log::warn!("Could not initialize PAPI cancel budget: {message}");
                return operation_ids
                    .iter()
                    .map(|operation_id| {
                        (
                            *operation_id,
                            Err(CoordinatorDispatchError::Command(
                                crate::http::command::PapiCommandFailure::before_dispatch(
                                    PapiHttpError::Configuration,
                                ),
                            )),
                        )
                    })
                    .collect();
            }
        };
        let mut outcomes = Vec::with_capacity(operation_ids.len());
        for operation_id in operation_ids {
            let result = dispatch_cancel_shared(
                coordinator,
                *operation_id,
                &self.inner.http,
                &budget,
                &self.inner.cancel,
                || Ok(()),
            )
            .await;
            outcomes.push((*operation_id, result));
        }
        outcomes
    }

    /// Queries the account's order quota as unprojected JSON evidence.
    ///
    /// This GET consumes one IP-weight unit; it does not reserve or consume an order slot.
    ///
    /// # Errors
    ///
    /// Returns an error if the signed read fails or exhausts its budget.
    pub async fn query_order_rate_limit(&self) -> anyhow::Result<String> {
        let budget = self.budget()?;
        let response = self
            .inner
            .http
            .get(&PapiRequest::OrderRateLimit, &budget, &self.inner.cancel)
            .await?;
        let body = response.body.get().to_owned();
        budget.check()?;
        Ok(body)
    }

    /// Collects a fixed inclusive history window plus current orders and explicit positions.
    ///
    /// Fills are linked to ordinary orders or their algo parent using targeted child reads.
    /// Commission, active-source, position, schema and identity failures fail the request.
    /// Other failed historical legs are listed in the returned incomplete snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds, unsupported mode, unrepresentable reports, contradictory
    /// identities, unresolved fill linkage, or failure of a required read.
    pub async fn generate_mass_status(
        &self,
        start: UnixNanos,
        end: UnixNanos,
    ) -> anyhow::Result<BinancePapiReadOnlySnapshot> {
        let window = HistoryWindow::new(start, end)?;
        anyhow::ensure!(
            end <= self.inner.clock.get_time_ns(),
            "PAPI history end is in the future"
        );
        self.collector()?.mass_status(window).await
    }

    /// Returns current order reports, optionally filtered by instrument.
    ///
    /// With no instrument, ordinary and algo orders are queried across the UM account.
    /// Missing metadata is loaded from public USD-M exchange information. No partial list
    /// is returned when a source or report conversion fails.
    ///
    /// # Errors
    ///
    /// Returns an error for historical requests, unsupported mode, unresolved metadata,
    /// failed current reads, unresolved algo children or invalid reports.
    pub async fn generate_order_status_reports(
        &self,
        instrument_id: Option<InstrumentId>,
        open_only: bool,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        anyhow::ensure!(
            open_only,
            "PAPI history is incomplete; use the bounded mass status report"
        );
        self.collector()?
            .open_orders(instrument_id, &self.inner.current_instruments)
            .await
    }

    /// Returns current open or in-flight orders, querying the whole UM account when no instrument is given.
    ///
    /// Active orders are included regardless of their age. A partial result is never returned.
    ///
    /// # Errors
    ///
    /// Returns an error for failed current reads, unsupported mode, unresolved algo children or invalid reports.
    pub async fn generate_open_order_status_reports(
        &self,
        instrument_id: Option<InstrumentId>,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        self.generate_order_status_reports(instrument_id, true)
            .await
    }

    /// Returns current one-way positions, querying the whole UM account when no instrument is given.
    ///
    /// Account-wide queries omit zero positions, matching the Binance Futures adapter.
    /// A successful symbol-scoped empty response is a flat position. Sparse account V2 data
    /// is never used by itself to infer a flat position.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported mode, missing coverage, failed reads or inexact quantities.
    pub async fn generate_position_status_reports(
        &self,
        instrument_id: Option<InstrumentId>,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        self.collector()?
            .positions(instrument_id, &self.inner.current_instruments)
            .await
    }

    /// Resolves one order by its encoded venue identity or ordinary client order ID.
    ///
    /// Venue IDs use `PAPI:O:SYMBOL:ID` for ordinary orders and `PAPI:A:SYMBOL:ID` for algos.
    /// A venue not-found response remains an error while endpoint retention/absence is unverified.
    /// This method therefore cannot return absence evidence to the execution engine.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid scope/identity, an unresolved order, or any failed read or conversion.
    pub async fn generate_order_status_report(
        &self,
        instrument_id: InstrumentId,
        venue_order_id: Option<VenueOrderId>,
        client_order_id: Option<ClientOrderId>,
    ) -> anyhow::Result<OrderStatusReport> {
        let mut collector = self.collector()?;
        collector
            .single_order(instrument_id, venue_order_id, client_order_id)
            .await
    }

    pub(crate) fn recovery_collector(
        &self,
        max_requests: u32,
    ) -> anyhow::Result<ReportCollector<'_>> {
        Ok(ReportCollector {
            http: &self.inner.http,
            scope: Cow::Borrowed(&self.inner.scope),
            account_id: self.inner.account_id,
            clock: &self.inner.clock,
            cancel: &self.inner.cancel,
            budget: RequestBudget::new(
                self.inner.operation_timeout,
                max_requests,
                self.inner.max_rows,
            )?,
            responses: Vec::new(),
        })
    }

    fn budget(&self) -> anyhow::Result<RequestBudget> {
        Ok(RequestBudget::new(
            self.inner.operation_timeout,
            self.inner.max_requests,
            self.inner.max_rows,
        )?)
    }

    fn collector(&self) -> anyhow::Result<ReportCollector<'_>> {
        Ok(ReportCollector {
            http: &self.inner.http,
            scope: Cow::Borrowed(&self.inner.scope),
            account_id: self.inner.account_id,
            clock: &self.inner.clock,
            cancel: &self.inner.cancel,
            budget: self.budget()?,
            responses: Vec::new(),
        })
    }
}

impl From<projection::ProjectionStatus> for BinancePapiProjectionStatus {
    fn from(value: projection::ProjectionStatus) -> Self {
        match value {
            projection::ProjectionStatus::Available => Self::Available,
            projection::ProjectionStatus::Unsupported => Self::Unsupported,
            projection::ProjectionStatus::Inconsistent => Self::Inconsistent,
            projection::ProjectionStatus::Missing => Self::Missing,
            projection::ProjectionStatus::Stale => Self::Stale,
            projection::ProjectionStatus::Failed => Self::Failed,
            projection::ProjectionStatus::Refreshing => Self::Refreshing,
            projection::ProjectionStatus::Canceled => Self::Canceled,
        }
    }
}

impl Debug for BinancePapiReadOnlyClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(BinancePapiReadOnlyClient))
            .field("account_id", &self.inner.account_id)
            .field("instrument_count", &self.inner.scope.instrument_ids().len())
            .field("throttled", &self.is_throttled())
            .finish_non_exhaustive()
    }
}

struct ReadOnlyInner {
    http: PapiHttpClient,
    scope: InstrumentScope,
    current_instruments: Mutex<InstrumentScope>,
    clock: Arc<AtomicTime>,
    account_id: AccountId,
    operation_timeout: Duration,
    max_requests: u32,
    max_rows: usize,
    cancel: CancellationToken,
    refresh: tokio::sync::Mutex<()>,
    observations: Mutex<AccountObservations>,
}

struct AccountObservations {
    generation: u64,
    sources: Vec<StoredObservation>,
}

pub(super) struct StoredObservation {
    slot: ObservationSlot,
    metadata: Option<BinancePapiResponseMetadata>,
}

// Dropping an in-progress future must invalidate the refresh, not restore prior freshness
struct ObservationRefresh<'a> {
    observations: &'a Mutex<AccountObservations>,
    completed: bool,
}

impl Drop for ObservationRefresh<'_> {
    fn drop(&mut self) {
        if !self.completed {
            for stored in &mut self.observations.lock().sources {
                stored.slot.record_canceled();
            }
        }
    }
}

#[derive(Serialize)]
struct ObservationView<'a> {
    endpoint: &'static str,
    receipt_status: ReceiptStatus,
    failure: Option<&'a ObservationFailure>,
    timing: ObservationTiming,
    observation: Option<&'a AccountObservation>,
    metadata: Option<&'a BinancePapiResponseMetadata>,
}

fn observation_sources() -> [ObservationSource; 9] {
    [
        ObservationSource::Balance { asset: None },
        ObservationSource::Account,
        ObservationSource::UmAccountV1,
        ObservationSource::UmAccountV2,
        ObservationSource::UmOpenOrders,
        ObservationSource::UmOpenAlgos,
        ObservationSource::CmPositions,
        ObservationSource::CmOpenOrders,
        ObservationSource::MarginOpenOrders,
    ]
}
