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

//! Scoped private observation, execution reports, and fail-closed ordinary UM commands.

use std::{
    cell::RefCell,
    future::Future,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use nautilus_common::{
    cache::Cache,
    clients::{
        ExecutionClient,
        capital::{NativeCapitalCheck, NativeCapitalCheckDecision},
    },
    enums::LogLevel,
    live::runner::get_exec_event_sender,
    messages::{
        ExecutionReport,
        execution::{
            BatchCancelOrders, BatchModifyOrders, CancelAllOrders, CancelOrder,
            GenerateFillReports, GenerateOrderStatusReport, GenerateOrderStatusReports,
            GeneratePositionStatusReports, ModifyOrder, QueryAccount, QueryOrder, SubmitOrder,
            SubmitOrderList,
        },
    },
};
use nautilus_core::{Params, UUID4, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_execution::client::core::ExecutionClientCore;
use nautilus_live::{ExecutionEventEmitter, execution::failure::CommandFailure, task::TaskGroup};
use nautilus_model::{
    accounts::AccountAny,
    enums::{AccountType, LiquiditySide, OmsType},
    events::AccountState,
    identifiers::{AccountId, ClientId, InstrumentId, StrategyId, Venue, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, MarginBalance, Money, Price, Quantity},
};
use parking_lot::Mutex;

use crate::{
    config::{BinancePapiExecutionClientConfig, BinancePapiTradingConfig},
    read_only::BinancePapiReadOnlyClient,
    reports::parse::{OrderFamily, venue_order_id},
    trading::{
        commands::{batch_cancel_operations, cancel_operation, submit_operation},
        coordinator::{
            CoordinatorDispatchError, PapiCancelPreparation, PapiCommandCoordinator,
            PapiRebaselineToken, PapiVerifiedRiskSnapshot,
        },
        journal::{PapiOperationResolution, PapiOperationStage, PapiPersistedOperation},
    },
    websocket::{
        BinancePapiAccountSession, PapiIncrementalBundle, PapiRecoveryBundle,
        PapiRefreshAcknowledgement, PapiRiskRefreshHandler, PapiRiskRefreshSignal,
    },
};

const TASK_SHUTDOWN_GRACE: Duration = Duration::from_secs(1);
const TASK_SHUTDOWN_ABORT: Duration = Duration::from_secs(2);
const RISK_REFRESH_QUIET_DELAY: Duration = Duration::from_secs(3);
const RISK_REFRESH_MAX_DELAY: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
struct PapiPendingApplication {
    checkpoint: crate::websocket::PapiApplicationCheckpoint,
    account_state: Option<AccountState>,
    report: ExecutionReport,
}

#[derive(Clone, Debug)]
struct PapiPendingRiskApplication {
    application: PapiPendingApplication,
    dirty_generation: u64,
    token: Option<PapiRebaselineToken>,
    evidence: PapiVerifiedRiskSnapshot,
}

#[derive(Debug, Default)]
struct PapiRiskRefreshState {
    token: Option<PapiRebaselineToken>,
    pending: Option<PapiPendingRiskApplication>,
}

#[derive(Debug, Default)]
struct PapiRiskRefreshControl {
    ready: AtomicBool,
    dirty_generation: AtomicU64,
    hard_generation: AtomicU64,
    clean_generation: AtomicU64,
    notify: tokio::sync::Notify,
    completion: tokio::sync::Notify,
    state: Mutex<PapiRiskRefreshState>,
}

impl PapiRiskRefreshControl {
    fn mark_dirty(&self, fact_version: u64, hard: bool) {
        let _state = self.state.lock();

        // A transport gap can invalidate authority without receiving another account fact
        let generation = self
            .dirty_generation
            .load(Ordering::Acquire)
            .checked_add(1)
            .expect("PAPI risk refresh generation overflow")
            .max(fact_version);
        self.dirty_generation.store(generation, Ordering::Release);

        if hard {
            self.hard_generation.store(generation, Ordering::Release);
        }
        self.notify.notify_one();
    }

    fn hard_refresh_pending(&self) -> bool {
        self.hard_generation.load(Ordering::Acquire) > self.clean_generation.load(Ordering::Acquire)
    }

    fn mark_clean(&self) {
        let generation = self.dirty_generation.load(Ordering::Acquire);
        self.clean_generation.store(generation, Ordering::Release);
    }

    fn reset(&self) {
        self.ready.store(false, Ordering::Release);
        self.dirty_generation.store(0, Ordering::Release);
        self.hard_generation.store(0, Ordering::Release);
        self.clean_generation.store(0, Ordering::Release);
        let mut state = self.state.lock();
        state.pending = None;
        state.token = None;
    }
}

fn ensure_risk_rebaseline_started(
    control: &PapiRiskRefreshControl,
    coordinator: &Mutex<Option<PapiCommandCoordinator>>,
    acknowledger: &Mutex<Option<crate::websocket::PapiApplicationAcknowledger>>,
) -> anyhow::Result<PapiRebaselineToken> {
    if let Some(token) = control.state.lock().token {
        return Ok(token);
    }
    let applied_fact_version = acknowledger
        .lock()
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("PAPI application acknowledger is unavailable"))?
        .applied_fact_version();
    let token = coordinator
        .lock()
        .as_mut()
        .ok_or_else(|| anyhow::anyhow!("PAPI command coordinator is unavailable"))?
        .begin_rebaseline(applied_fact_version, Instant::now())?;
    control.state.lock().token = Some(token);
    Ok(token)
}

async fn coalesced_risk_generation(
    control: &PapiRiskRefreshControl,
    refresh_debounce: Duration,
    maximum_delay: Duration,
    cancel: &tokio_util::sync::CancellationToken,
) -> Option<u64> {
    let started = tokio::time::Instant::now();
    let maximum = started + maximum_delay;
    let mut quiet = started + refresh_debounce;
    let mut observed = control.dirty_generation.load(Ordering::Acquire);

    loop {
        let now = tokio::time::Instant::now();
        if now >= maximum {
            return Some(control.dirty_generation.load(Ordering::Acquire));
        }
        let deadline = quiet.min(maximum);
        tokio::select! {
            biased;
            () = cancel.cancelled() => return None,
            () = nautilus_common::live::dst::time::sleep(deadline.duration_since(now)) => {
                return Some(control.dirty_generation.load(Ordering::Acquire));
            }
            () = control.notify.notified() => {}
        }
        let current = control.dirty_generation.load(Ordering::Acquire);
        if current != observed {
            observed = current;
            quiet = tokio::time::Instant::now() + refresh_debounce;
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "Long-lived risk refresh worker owns an explicit immutable runtime context"
)]
async fn run_risk_refresh_worker(
    reader: BinancePapiReadOnlyClient,
    trading: BinancePapiTradingConfig,
    client_id: ClientId,
    account_id: AccountId,
    venue: Venue,
    emitter: ExecutionEventEmitter,
    coordinator: Arc<Mutex<Option<PapiCommandCoordinator>>>,
    acknowledger: Arc<Mutex<Option<crate::websocket::PapiApplicationAcknowledger>>>,
    control: Arc<PapiRiskRefreshControl>,
    cancel: tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
            () = control.notify.notified() => {}
        }

        if control.dirty_generation.load(Ordering::Acquire)
            <= control.clean_generation.load(Ordering::Acquire)
        {
            continue;
        }

        let Some(dirty_generation) = coalesced_risk_generation(
            &control,
            RISK_REFRESH_QUIET_DELAY,
            RISK_REFRESH_MAX_DELAY,
            &cancel,
        )
        .await
        else {
            return Ok(());
        };
        let token = if control.hard_refresh_pending() {
            match ensure_risk_rebaseline_started(&control, &coordinator, &acknowledger) {
                Ok(token) => Some(token),
                Err(e) => {
                    log::warn!("PAPI risk rebaseline could not start: {e}");
                    control.notify.notify_one();
                    continue;
                }
            }
        } else {
            None
        };
        let checkpoint = match acknowledger
            .lock()
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PAPI application acknowledger is unavailable"))
            .and_then(|acknowledger| acknowledger.applied_refresh_checkpoint())
        {
            Ok(checkpoint) => checkpoint,
            Err(e) => {
                log::warn!("PAPI risk refresh has no application checkpoint: {e}");
                control.notify.notify_one();
                continue;
            }
        };
        let rebaseline = match reader.collect_verified_risk_rebaseline(&trading).await {
            Ok(rebaseline) => rebaseline,
            Err(e) => {
                log::warn!("PAPI risk refresh failed: {e}");
                control.notify.notify_one();
                continue;
            }
        };
        let mut mass_status =
            ExecutionMassStatus::new(client_id, account_id, venue, reader.now(), None);
        mass_status.set_report_window(None, false);
        mass_status.add_order_reports(rebaseline.order_reports);
        mass_status.add_position_reports(rebaseline.position_reports);
        let report = ExecutionReport::MassStatus(Box::new(mass_status));
        let pending = PapiPendingRiskApplication {
            application: PapiPendingApplication {
                checkpoint,
                account_state: Some(rebaseline.account_state.clone()),
                report: report.clone(),
            },
            dirty_generation,
            token,
            evidence: rebaseline.snapshot,
        };
        let admitted = {
            let mut state = control.state.lock();
            if control.dirty_generation.load(Ordering::Acquire) == dirty_generation
                && state.token == token
                && state.pending.is_none()
            {
                state.pending = Some(pending);
                true
            } else {
                false
            }
        };

        if !admitted {
            log::warn!("PAPI risk refresh state changed during collection");
            control.notify.notify_one();
            continue;
        }

        if let Err(e) = emitter.try_send_account_state(rebaseline.account_state) {
            control.state.lock().pending = None;
            log::warn!("PAPI risk account-state delivery failed: {e}");
            control.notify.notify_one();
            continue;
        }

        if let Err(e) = emitter.try_send_execution_report(report) {
            control.state.lock().pending = None;
            log::warn!("PAPI risk report delivery failed: {e}");
            control.notify.notify_one();
            continue;
        }

        let completion = tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
            result = tokio::time::timeout(trading_risk_application_timeout(&trading), control.completion.notified()) => result,
        };

        if completion.is_err() {
            let mut state = control.state.lock();
            if state
                .pending
                .as_ref()
                .is_some_and(|pending| pending.application.checkpoint == checkpoint)
            {
                state.pending = None;
            }
            drop(state);
            log::warn!("PAPI risk refresh application acknowledgement timed out");
            control.notify.notify_one();
        } else if control.dirty_generation.load(Ordering::Acquire)
            > control.clean_generation.load(Ordering::Acquire)
        {
            control.notify.notify_one();
        }
    }
}

fn trading_risk_application_timeout(config: &BinancePapiTradingConfig) -> Duration {
    Duration::from_millis(config.max_risk_age_ms.max(1_000))
}

#[derive(Debug)]
pub(crate) struct BinancePapiExecutionClient {
    core: ExecutionClientCore,
    config: BinancePapiExecutionClientConfig,
    emitter: ExecutionEventEmitter,
    coordinator: Arc<Mutex<Option<PapiCommandCoordinator>>>,
    trading_connected: Arc<AtomicBool>,
    application_acknowledger: Arc<Mutex<Option<crate::websocket::PapiApplicationAcknowledger>>>,
    pending_application: Arc<Mutex<Option<PapiPendingApplication>>>,
    risk_refresh: Arc<PapiRiskRefreshControl>,
    reader: RefCell<Option<BinancePapiReadOnlyClient>>,
    session: Option<BinancePapiAccountSession>,
    pending_tasks: TaskGroup,
}

impl BinancePapiExecutionClient {
    pub(crate) fn new(core: ExecutionClientCore, config: BinancePapiExecutionClientConfig) -> Self {
        let emitter = ExecutionEventEmitter::new(
            get_atomic_clock_realtime(),
            core.trader_id,
            core.account_id,
            AccountType::Margin,
            None,
        );

        Self {
            core,
            config,
            emitter,
            coordinator: Arc::new(Mutex::new(None)),
            trading_connected: Arc::new(AtomicBool::new(false)),
            application_acknowledger: Arc::new(Mutex::new(None)),
            pending_application: Arc::new(Mutex::new(None)),
            risk_refresh: Arc::new(PapiRiskRefreshControl::default()),
            reader: RefCell::new(None),
            session: None,
            pending_tasks: TaskGroup::new(),
        }
    }

    fn spawn_task<F>(&self, description: &'static str, future: F) -> anyhow::Result<()>
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        self.pending_tasks
            .spawn(async move {
                if let Err(e) = future.await {
                    log::warn!("PAPI {description} failed: {e}");
                }
            })
            .map_err(|e| anyhow::anyhow!("PAPI {description} task admission is closed: {e}"))
    }

    async fn restart_pending_tasks(&self) -> anyhow::Result<()> {
        if self.pending_tasks.is_open() {
            return Ok(());
        }

        self.pending_tasks
            .finish_shutdown(TASK_SHUTDOWN_GRACE, TASK_SHUTDOWN_ABORT)
            .await
            .map_err(|e| anyhow::anyhow!("PAPI pending task shutdown failed: {e}"))?;
        self.pending_tasks
            .start_generation()
            .map_err(|e| anyhow::anyhow!("PAPI pending task restart failed: {e}"))
    }

    async fn finish_pending_tasks(&self) -> anyhow::Result<()> {
        self.pending_tasks.begin_shutdown();
        self.pending_tasks
            .finish_shutdown(TASK_SHUTDOWN_GRACE, TASK_SHUTDOWN_ABORT)
            .await
            .map_err(|e| anyhow::anyhow!("PAPI pending task shutdown failed: {e}"))
    }

    fn instruments(&self) -> anyhow::Result<Vec<InstrumentAny>> {
        let cache = self.core.cache();
        self.config
            .instrument_ids
            .iter()
            .map(|id| {
                cache
                    .instrument(id)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("PAPI instrument is not preloaded: {id}"))
            })
            .collect()
    }

    fn reader(&self) -> anyhow::Result<BinancePapiReadOnlyClient> {
        anyhow::ensure!(
            self.core.is_started(),
            "PAPI read-only client is not started"
        );

        if let Some(client) = self.reader.borrow().as_ref() {
            return Ok(client.clone());
        }

        let config = self
            .config
            .read_only
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PAPI read-only configuration is required"))?;
        let instruments = self.instruments()?;
        let client = BinancePapiReadOnlyClient::new(config, instruments)?;
        *self.reader.borrow_mut() = Some(client.clone());
        Ok(client)
    }

    fn begin_shutdown(&self) {
        self.trading_connected.store(false, Ordering::Release);
        self.application_acknowledger.lock().take();
        self.pending_application.lock().take();
        self.risk_refresh.reset();
        self.risk_refresh.completion.notify_waiters();
        let has_session = self.session.is_some();
        if let Some(session) = self.session.as_ref() {
            session.begin_shutdown();
        }

        if let Some(reader) = self.reader.borrow_mut().take() {
            reader.cancel();
        }

        if has_session || !self.pending_tasks.is_empty() {
            self.pending_tasks.begin_shutdown();
        }
        self.core.set_disconnected();
    }

    fn log_report_receipt(count: usize, report_type: &str, level: LogLevel) {
        let message = format!("Received {count} PAPI {report_type} reports");

        match level {
            LogLevel::Off => {}
            LogLevel::Trace => log::trace!("{message}"),
            LogLevel::Debug => log::debug!("{message}"),
            LogLevel::Info => log::info!("{message}"),
            LogLevel::Warning => log::warn!("{message}"),
            LogLevel::Error => log::error!("{message}"),
        }
    }

    fn trading_reader(&self) -> anyhow::Result<BinancePapiReadOnlyClient> {
        anyhow::ensure!(
            self.config.trading.is_some(),
            "PAPI trading configuration is not enabled"
        );
        anyhow::ensure!(
            self.core.is_connected() && self.trading_connected.load(Ordering::Acquire),
            "PAPI execution client is not connected for trading"
        );
        anyhow::ensure!(
            self.coordinator.lock().is_some(),
            "PAPI command coordinator is unavailable"
        );
        self.reader()
    }

    fn ordinary_venue_order_id(
        instrument_id: InstrumentId,
        order_id: i64,
    ) -> anyhow::Result<VenueOrderId> {
        let symbol = nautilus_binance::common::symbol::format_binance_symbol(&instrument_id);
        venue_order_id(&symbol, OrderFamily::Ordinary, order_id)
    }

    fn dispatch_cancel_operations(
        &self,
        operations: Vec<PapiPersistedOperation>,
    ) -> anyhow::Result<()> {
        let mut scoped = Vec::with_capacity(operations.len());
        for operation in operations {
            let order = self.core.get_order(&operation.client_order_id)?;
            scoped.push((operation, order));
        }
        let reader = match self.trading_reader() {
            Ok(reader) => reader,
            Err(e) => {
                for (_, order) in &scoped {
                    self.emitter.emit_order_cancel_rejected(
                        order,
                        order.venue_order_id(),
                        &e.to_string(),
                        get_atomic_clock_realtime().get_time_ns(),
                    );
                }
                return Ok(());
            }
        };

        let mut prepared = Vec::with_capacity(scoped.len());
        let mut rejected = Vec::new();
        let mut prepare_error = None;
        let mut cleanup_error = None;
        {
            let mut guard = self.coordinator.lock();
            let coordinator = guard
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("PAPI command coordinator is unavailable"))?;
            let mut scoped = scoped.into_iter();
            while let Some((operation, order)) = scoped.next() {
                match coordinator
                    .prepare_cancel(operation, get_atomic_clock_realtime().get_time_ns())
                {
                    Ok(PapiCancelPreparation::LocalSubmitCanceled { .. }) => {
                        self.emitter.emit_order_canceled(
                            &order,
                            order.venue_order_id(),
                            get_atomic_clock_realtime().get_time_ns(),
                        );
                    }
                    Ok(PapiCancelPreparation::Prepared {
                        cancel_operation_id,
                    }) => prepared.push((cancel_operation_id, order)),
                    Err(e) => {
                        prepare_error = Some(e.to_string());
                        rejected.push(order);
                        rejected.extend(scoped.map(|(_, order)| order));
                        break;
                    }
                }
            }

            if prepare_error.is_some() {
                for (operation_id, order) in &prepared {
                    if let Err(e) = coordinator.transition(
                        *operation_id,
                        PapiOperationStage::Resolved {
                            resolution: PapiOperationResolution::NotSent,
                        },
                        get_atomic_clock_realtime().get_time_ns(),
                    ) {
                        cleanup_error.get_or_insert(e);
                    }
                    rejected.push(order.clone());
                }
            }
        }

        if let Some(reason) = prepare_error {
            for order in &rejected {
                self.emitter.emit_order_cancel_rejected(
                    order,
                    order.venue_order_id(),
                    &reason,
                    get_atomic_clock_realtime().get_time_ns(),
                );
            }

            if let Some(e) = cleanup_error {
                anyhow::bail!("PAPI batch cancel cleanup could not be persisted: {e}");
            }
            return Ok(());
        }

        if prepared.is_empty() {
            return Ok(());
        }

        let operation_ids: Vec<_> = prepared
            .iter()
            .map(|(operation_id, _)| *operation_id)
            .collect();
        let coordinator = Arc::clone(&self.coordinator);
        let emitter = self.emitter.clone();
        let task_prepared = prepared.clone();

        if let Err(e) = self.spawn_task("cancel_order_batch", async move {
            let outcomes = reader
                .dispatch_cancel_batch(&coordinator, &operation_ids)
                .await;

            for ((expected_id, order), (operation_id, result)) in task_prepared.iter().zip(outcomes)
            {
                if *expected_id != operation_id {
                    log::error!("PAPI cancel batch outcome identity mismatch");
                    continue;
                }
                Self::handle_cancel_dispatch(&emitter, order, result);
            }
            Ok(())
        }) {
            let mut guard = self.coordinator.lock();
            if let Some(coordinator) = guard.as_mut() {
                for (operation_id, order) in &prepared {
                    if let Err(transition_error) = coordinator.transition(
                        *operation_id,
                        PapiOperationStage::Resolved {
                            resolution: PapiOperationResolution::NotSent,
                        },
                        get_atomic_clock_realtime().get_time_ns(),
                    ) {
                        log::error!(
                            "Failed to resolve unspawned PAPI batch cancel: {transition_error}"
                        );
                    }
                    self.emitter.emit_order_cancel_rejected(
                        order,
                        order.venue_order_id(),
                        &e.to_string(),
                        get_atomic_clock_realtime().get_time_ns(),
                    );
                }
            }
        }
        Ok(())
    }

    fn handle_submit_dispatch(
        emitter: &ExecutionEventEmitter,
        order: &OrderAny,
        result: Result<crate::http::PapiCommandResponse, CoordinatorDispatchError>,
    ) {
        match result {
            Ok(response) => match Self::ordinary_venue_order_id(
                order.instrument_id(),
                response.acknowledgement.venue_order_id,
            ) {
                Ok(venue_order_id) => emitter.emit_order_accepted(
                    order,
                    venue_order_id,
                    response.raw.metadata.ts_received,
                ),
                Err(e) => log::error!(
                    "PAPI accepted submit returned an unusable venue identity for {}: {e}",
                    order.client_order_id()
                ),
            },
            Err(CoordinatorDispatchError::Command(failure)) => match failure.classification {
                CommandFailure::NotSent(reason) => emitter.emit_order_denied(order, &reason),
                CommandFailure::VenueRejected(reason) => emitter.emit_order_rejected(
                    order,
                    &reason,
                    get_atomic_clock_realtime().get_time_ns(),
                    false,
                ),
                CommandFailure::Ambiguous(reason) => log::warn!(
                    "PAPI submit outcome is unknown for {}: {reason}",
                    order.client_order_id()
                ),
            },
            Err(CoordinatorDispatchError::Superseded) => log::debug!(
                "PAPI submit response for {} was superseded by an authoritative order report",
                order.client_order_id()
            ),
            Err(e) => log::warn!(
                "PAPI submit stopped before a venue outcome for {}: {e}",
                order.client_order_id()
            ),
        }
    }

    fn handle_cancel_dispatch(
        emitter: &ExecutionEventEmitter,
        order: &OrderAny,
        result: Result<crate::http::PapiCommandResponse, CoordinatorDispatchError>,
    ) {
        match result {
            Ok(response) => match Self::ordinary_venue_order_id(
                order.instrument_id(),
                response.acknowledgement.venue_order_id,
            ) {
                Ok(venue_order_id) => emitter.emit_order_canceled(
                    order,
                    Some(venue_order_id),
                    response.raw.metadata.ts_received,
                ),
                Err(e) => log::error!(
                    "PAPI accepted cancel returned an unusable venue identity for {}: {e}",
                    order.client_order_id()
                ),
            },
            Err(CoordinatorDispatchError::Command(failure)) => match failure.classification {
                CommandFailure::NotSent(reason) | CommandFailure::VenueRejected(reason) => {
                    emitter.emit_order_cancel_rejected(
                        order,
                        order.venue_order_id(),
                        &reason,
                        get_atomic_clock_realtime().get_time_ns(),
                    );
                }
                CommandFailure::Ambiguous(reason) => log::warn!(
                    "PAPI cancel outcome is unknown for {}: {reason}",
                    order.client_order_id()
                ),
            },
            Err(CoordinatorDispatchError::Superseded) => log::debug!(
                "PAPI cancel response for {} was superseded by an authoritative order report",
                order.client_order_id()
            ),
            Err(e) => log::warn!(
                "PAPI cancel stopped before a venue outcome for {}: {e}",
                order.client_order_id()
            ),
        }
    }

    fn order_report_applied(
        cache: &Cache,
        report: &OrderStatusReport,
        fills: &[FillReport],
    ) -> bool {
        let client_order_id = report
            .client_order_id
            .or_else(|| cache.client_order_id(&report.venue_order_id).copied());
        let Some(order) = client_order_id.and_then(|id| cache.order(&id)) else {
            return false;
        };

        order.account_id() == Some(report.account_id)
            && order.instrument_id() == report.instrument_id
            && order.venue_order_id() == Some(report.venue_order_id)
            && order.quantity() == report.quantity
            && order.filled_qty() >= report.filled_qty
            && (order.status() == report.order_status || order.ts_last() >= report.ts_last)
            && fills
                .iter()
                .all(|fill| order.trade_ids().contains(&&fill.trade_id))
    }

    fn fill_report_applied(cache: &Cache, report: &FillReport) -> bool {
        let client_order_id = report
            .client_order_id
            .or_else(|| cache.client_order_id(&report.venue_order_id).copied());
        client_order_id
            .and_then(|id| cache.order(&id))
            .is_some_and(|order| {
                order.account_id() == Some(report.account_id)
                    && order.instrument_id() == report.instrument_id
                    && order.venue_order_id() == Some(report.venue_order_id)
                    && order.trade_ids().contains(&&report.trade_id)
            })
    }

    fn position_report_applied(cache: &Cache, report: &PositionStatusReport) -> bool {
        let signed_quantity: rust_decimal::Decimal = cache
            .positions_open(
                None,
                Some(&report.instrument_id),
                None,
                Some(&report.account_id),
                None,
            )
            .iter()
            .map(|position| position.signed_decimal_qty())
            .sum();
        signed_quantity == report.signed_decimal_qty
    }

    fn expected_application_is_applied(&self, pending: &PapiPendingApplication) -> bool {
        let cache = self.core.cache();
        if pending.account_state.as_ref().is_some_and(|expected| {
            !cache
                .account(&expected.account_id)
                .and_then(|account| account.last_event())
                .is_some_and(|event| event.event_id == expected.event_id)
        }) {
            return false;
        }

        match &pending.report {
            ExecutionReport::Order(report) => Self::order_report_applied(&cache, report, &[]),
            ExecutionReport::Fill(report) => Self::fill_report_applied(&cache, report),
            ExecutionReport::OrderWithFills(report, fills) => {
                Self::order_report_applied(&cache, report, fills)
            }
            ExecutionReport::Position(report) => Self::position_report_applied(&cache, report),
            ExecutionReport::MassStatus(report) => {
                let order_reports = report.order_reports();
                let fill_reports = report.fill_reports();
                let orders_applied = order_reports.values().all(|order_report| {
                    Self::order_report_applied(
                        &cache,
                        order_report,
                        fill_reports
                            .get(&order_report.venue_order_id)
                            .map_or(&[], Vec::as_slice),
                    )
                });
                let fills_applied = fill_reports
                    .values()
                    .flatten()
                    .all(|fill| Self::fill_report_applied(&cache, fill));
                let positions_applied = report
                    .position_reports()
                    .values()
                    .flatten()
                    .all(|position| Self::position_report_applied(&cache, position));
                orders_applied && fills_applied && positions_applied
            }
        }
    }

    fn acknowledge_pending_application(&self) {
        let pending = self.pending_application.lock().clone();
        let Some(pending) = pending else {
            return;
        };

        if !self.expected_application_is_applied(&pending) {
            return;
        }
        let result = self
            .application_acknowledger
            .lock()
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PAPI application acknowledger is unavailable"))
            .and_then(|acknowledger| acknowledger.acknowledge(pending.checkpoint));

        match result {
            Ok(()) => {
                let mut current = self.pending_application.lock();
                if current
                    .as_ref()
                    .is_some_and(|value| value.checkpoint == pending.checkpoint)
                {
                    current.take();
                }
            }
            Err(e) => log::warn!("PAPI application acknowledgement was rejected: {e}"),
        }
    }

    fn acknowledge_pending_risk_application(&self, report: &ExecutionReport) {
        let ExecutionReport::MassStatus(applied) = report else {
            return;
        };
        let pending = {
            let refresh = self.risk_refresh.state.lock();
            refresh
                .pending
                .as_ref()
                .and_then(|pending| match &pending.application.report {
                    ExecutionReport::MassStatus(expected)
                        if expected.report_id == applied.report_id =>
                    {
                        Some(pending.clone())
                    }
                    _ => None,
                })
        };
        let Some(pending) = pending else {
            return;
        };

        if !self.expected_application_is_applied(&pending.application) {
            return;
        }
        let acknowledgement = self
            .application_acknowledger
            .lock()
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PAPI application acknowledger is unavailable"))
            .and_then(|acknowledger| {
                acknowledger.acknowledge_refresh(pending.application.checkpoint)
            });

        let mut refresh = self.risk_refresh.state.lock();
        let is_current = refresh.pending.as_ref().is_some_and(|current| {
            current.application.checkpoint == pending.application.checkpoint
        });
        let mut superseded_fact_version = None;
        let result = acknowledgement.and_then(|acknowledgement| match acknowledgement {
            PapiRefreshAcknowledgement::Superseded { fact_version } => {
                superseded_fact_version = Some(fact_version);
                Ok(false)
            }
            PapiRefreshAcknowledgement::Applied {
                fact_version: applied_fact_version,
            } => {
                if !is_current
                    || self.risk_refresh.dirty_generation.load(Ordering::Acquire)
                        > pending.dirty_generation
                {
                    return Ok(false);
                }

                let mut coordinator = self.coordinator.lock();
                let coordinator = coordinator
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("PAPI command coordinator is unavailable"))?;
                let result = match pending.token {
                    Some(token) => coordinator.install_rebaseline(
                        token,
                        pending.evidence.clone(),
                        applied_fact_version,
                        Instant::now(),
                        get_atomic_clock_realtime().get_time_ns(),
                    ),
                    None => coordinator.install_refresh(
                        pending.evidence.clone(),
                        applied_fact_version,
                        Instant::now(),
                        get_atomic_clock_realtime().get_time_ns(),
                    ),
                };
                result.map_err(anyhow::Error::from)?;
                Ok(true)
            }
        });

        if is_current {
            refresh.pending = None;
        }

        if result.as_ref().is_ok_and(|installed| *installed) {
            refresh.token = None;
            self.risk_refresh
                .clean_generation
                .store(pending.dirty_generation, Ordering::Release);
        }
        drop(refresh);

        if let Some(fact_version) = superseded_fact_version {
            self.risk_refresh.mark_dirty(fact_version, false);
        }

        if let Err(e) = result {
            self.risk_refresh.mark_dirty(pending.dirty_generation, true);

            if let Some(session) = self.session.as_ref() {
                session.revoke_trading_authorization();
            }
            log::warn!("PAPI risk refresh application was rejected: {e}");
        } else if result.is_ok_and(|installed| installed) {
            if !self.risk_refresh.hard_refresh_pending()
                && let Some(acknowledger) = self.application_acknowledger.lock().as_ref()
                && let Err(e) = acknowledger.authorize_trading(pending.application.checkpoint)
            {
                self.risk_refresh.mark_dirty(0, true);
                log::warn!("PAPI risk authorization was rejected: {e}");
            }
        } else if self.risk_refresh.dirty_generation.load(Ordering::Acquire)
            > self.risk_refresh.clean_generation.load(Ordering::Acquire)
        {
            self.risk_refresh.notify.notify_one();
        }
        self.risk_refresh.completion.notify_one();
    }

    fn apply_report_to_coordinator(&self, report: &ExecutionReport) -> Option<bool> {
        let incremental = matches!(
            report,
            ExecutionReport::Order(_) | ExecutionReport::OrderWithFills(_, _)
        );
        let reports: Vec<OrderStatusReport> = match report {
            ExecutionReport::Order(report) | ExecutionReport::OrderWithFills(report, _) => {
                vec![(**report).clone()]
            }
            ExecutionReport::MassStatus(report) => report.order_reports().into_values().collect(),
            ExecutionReport::Fill(_) | ExecutionReport::Position(_) => Vec::new(),
        };
        let mut guard = self.coordinator.lock();
        let Some(coordinator) = guard.as_mut() else {
            return incremental.then_some(false);
        };
        let mut owned = true;

        for report in reports {
            match coordinator
                .apply_matching_order_report(&report, get_atomic_clock_realtime().get_time_ns())
            {
                Ok(matched) => owned &= matched > 0,
                Err(e) => {
                    owned = false;
                    log::warn!("PAPI command journal rejected an applied order report: {e}");
                }
            }
        }
        incremental.then_some(owned)
    }

    fn pending_incremental_fact_version(&self, report: &ExecutionReport) -> Option<u64> {
        let applied_report_id = match report {
            ExecutionReport::Order(report) | ExecutionReport::OrderWithFills(report, _) => {
                report.report_id
            }
            ExecutionReport::Fill(_)
            | ExecutionReport::Position(_)
            | ExecutionReport::MassStatus(_) => return None,
        };
        let pending = self.pending_application.lock();
        let pending = pending.as_ref()?;
        let pending_report_id = match &pending.report {
            ExecutionReport::Order(report) | ExecutionReport::OrderWithFills(report, _) => {
                report.report_id
            }
            ExecutionReport::Fill(_)
            | ExecutionReport::Position(_)
            | ExecutionReport::MassStatus(_) => return None,
        };
        (pending_report_id == applied_report_id).then(|| pending.checkpoint.fact_version())
    }

    fn mark_unowned_incremental_report(&self, report: &ExecutionReport) {
        let Some(fact_version) = self.pending_incremental_fact_version(report) else {
            return;
        };
        self.risk_refresh.mark_dirty(fact_version, true);

        if let Some(session) = self.session.as_ref() {
            session.revoke_trading_authorization();
        }

        if self.risk_refresh.ready.load(Ordering::Acquire)
            && let Err(e) = ensure_risk_rebaseline_started(
                &self.risk_refresh,
                &self.coordinator,
                &self.application_acknowledger,
            )
        {
            log::warn!("PAPI unowned-order risk rebaseline could not start: {e}");
        }
    }
}

#[async_trait(?Send)]
impl ExecutionClient for BinancePapiExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
            && self
                .session
                .as_ref()
                .is_some_and(BinancePapiAccountSession::is_connected)
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        self.core.venue
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core.cache().account_owned(&self.core.account_id)
    }

    fn recovered_order_strategy(
        &self,
        report: &OrderStatusReport,
    ) -> anyhow::Result<Option<StrategyId>> {
        let coordinator = self.coordinator.lock();

        match coordinator.as_ref() {
            Some(coordinator) => Ok(coordinator.recovered_order_strategy(report)?),
            None => Ok(None),
        }
    }

    fn native_capital_check(&self) -> Option<NativeCapitalCheck> {
        self.config.trading.as_ref()?;
        let coordinator = Arc::clone(&self.coordinator);
        let trading_connected = Arc::clone(&self.trading_connected);
        let risk_refresh = Arc::clone(&self.risk_refresh);
        let account_id = self.core.account_id;
        let client_id = self.core.client_id;

        Some(Rc::new(move |request| {
            let deny = |reason: String| NativeCapitalCheckDecision::Denied(reason);

            if request.account_id != account_id || request.client_id != client_id {
                return deny(
                    "PAPI native capital route does not match the execution client".to_string(),
                );
            }

            if !trading_connected.load(Ordering::Acquire) {
                return deny("PAPI execution client is not connected for trading".to_string());
            }

            if risk_refresh.hard_refresh_pending() {
                return deny("PAPI account risk refresh is pending".to_string());
            }

            if request.full_position_exit || request.orders.len() != 1 {
                return deny(
                    "PAPI native capital check requires one increase-risk order".to_string(),
                );
            }
            let order = request.orders[0];
            if order.instrument_id() != request.instrument.id() {
                return deny("PAPI native capital instrument identity does not match".to_string());
            }
            let command = SubmitOrder::from_order(
                order,
                order.trader_id(),
                Some(client_id),
                None,
                UUID4::new(),
                UnixNanos::default(),
            );
            let operation = match submit_operation(&command, account_id) {
                Ok(operation) => operation,
                Err(e) => return deny(e.to_string()),
            };
            let Some(guard) = coordinator.try_lock() else {
                return deny("PAPI command coordinator is busy".to_string());
            };
            let Some(coordinator) = guard.as_ref() else {
                return deny("PAPI command coordinator is unavailable".to_string());
            };

            match coordinator.check_submit(&operation, Instant::now()) {
                Ok(_) => NativeCapitalCheckDecision::Approved,
                Err(e) => deny(e.to_string()),
            }
        }))
    }

    fn on_execution_report_applied(&self, report: &ExecutionReport) {
        if self.apply_report_to_coordinator(report) == Some(false) {
            self.mark_unowned_incremental_report(report);
        }
        self.acknowledge_pending_application();
        self.acknowledge_pending_risk_application(report);
    }

    fn provides_bulk_position_coverage(&self, instrument_id: InstrumentId) -> bool {
        self.config.instrument_ids.contains(&instrument_id)
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
        info: Option<Params>,
    ) -> anyhow::Result<()> {
        self.emitter
            .try_emit_account_state(balances, margins, reported, ts_event, info)
    }

    fn calculate_commission(
        &self,
        _instrument: &InstrumentAny,
        _last_qty: Quantity,
        _last_px: Price,
        _liquidity_side: LiquiditySide,
    ) -> anyhow::Result<Option<Money>> {
        anyhow::bail!("Binance PAPI commission calculation is not implemented")
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.core.is_started() {
            return Ok(());
        }

        self.config.validate()?;
        anyhow::ensure!(
            self.config.read_only.is_some(),
            "PAPI read-only configuration is required"
        );

        if !self.pending_tasks.is_open() {
            self.pending_tasks
                .start_generation()
                .map_err(|e| anyhow::anyhow!("PAPI pending task restart failed: {e}"))?;
        }

        if let Some(trading) = &self.config.trading {
            let coordinator =
                PapiCommandCoordinator::open(trading.clone(), self.config.account_id)?;
            *self.coordinator.lock() = Some(coordinator);
        }
        self.trading_connected.store(false, Ordering::Release);
        self.emitter.set_sender(get_exec_event_sender());
        self.core.set_started();
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.core.is_started(),
            "PAPI execution client is not started"
        );

        if self.core.is_connected() && self.pending_tasks.is_open() {
            return Ok(());
        }

        self.restart_pending_tasks().await?;

        if let Some(reader) = self.reader.borrow_mut().take() {
            reader.cancel();
        }

        if let Some(mut session) = self.session.take() {
            session.stop().await?;
        }

        let config = self
            .config
            .read_only
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PAPI read-only configuration is required"))?;
        let instruments = self.instruments()?;
        let emitter = self.emitter.clone();
        let client_id = self.core.client_id;
        let pending_application = Arc::clone(&self.pending_application);
        let handler = std::sync::Arc::new(move |mut bundle: PapiRecoveryBundle| {
            log::trace!(
                "Dispatching PAPI recovery bundle, initial={}",
                bundle.initial
            );
            bundle.snapshot.mass_status.client_id = client_id;
            let report = ExecutionReport::MassStatus(Box::new(bundle.snapshot.mass_status));
            *pending_application.lock() = Some(PapiPendingApplication {
                checkpoint: bundle.checkpoint,
                account_state: Some(bundle.account_state.clone()),
                report: report.clone(),
            });

            if let Err(e) = emitter.try_send_account_state(bundle.account_state) {
                pending_application.lock().take();
                return Err(e);
            }

            if let Err(e) = emitter.try_send_execution_report(report) {
                pending_application.lock().take();
                return Err(e);
            }
            Ok(())
        });
        let emitter = self.emitter.clone();
        let pending_application = Arc::clone(&self.pending_application);
        let incremental_handler = std::sync::Arc::new(move |bundle: PapiIncrementalBundle| {
            let report = if bundle.fills.is_empty() {
                ExecutionReport::Order(Box::new(bundle.order))
            } else {
                ExecutionReport::OrderWithFills(Box::new(bundle.order), bundle.fills)
            };
            *pending_application.lock() = Some(PapiPendingApplication {
                checkpoint: bundle.checkpoint,
                account_state: None,
                report: report.clone(),
            });

            if let Err(e) = emitter.try_send_execution_report(report) {
                pending_application.lock().take();
                return Err(e);
            }
            Ok(())
        });
        let risk_refresh_handler: Option<PapiRiskRefreshHandler> =
            self.config.trading.as_ref().map(|_| {
                let control = Arc::clone(&self.risk_refresh);
                let coordinator = Arc::clone(&self.coordinator);
                let acknowledger = Arc::clone(&self.application_acknowledger);

                Arc::new(move |signal: PapiRiskRefreshSignal| {
                    let hard = !coordinator.lock().as_ref().is_some_and(|coordinator| {
                        coordinator.covers_order_account_update(&signal.reason, &signal.symbols)
                    });
                    control.mark_dirty(signal.fact_version, hard);

                    if hard && control.ready.load(Ordering::Acquire) {
                        if let Some(acknowledger) = acknowledger.lock().as_ref() {
                            acknowledger.revoke_trading_authorization();
                        }

                        match ensure_risk_rebaseline_started(&control, &coordinator, &acknowledger)
                        {
                            Ok(_) => {}
                            Err(e) => {
                                log::warn!("PAPI risk rebaseline could not start: {e}");
                            }
                        }
                    }
                    Ok(())
                }) as PapiRiskRefreshHandler
            });
        let mut session = BinancePapiAccountSession::with_handlers(
            config,
            instruments,
            handler,
            incremental_handler,
            risk_refresh_handler,
        )?;
        let acknowledger = session.application_acknowledger();

        if let Some(coordinator) = self.coordinator.lock().as_mut() {
            coordinator.bind_session(acknowledger.clone());
        }
        *self.application_acknowledger.lock() = Some(acknowledger.clone());
        let control = Arc::clone(&self.risk_refresh);
        session.set_invalidation_handler(Arc::new(move || control.mark_dirty(0, true)));
        session.start().await?;
        let reader = session.read_only_client();
        self.risk_refresh.mark_clean();

        let mut coordinator = self.coordinator.lock().take();
        if let Some(mut value) = coordinator.take() {
            let recovery = value.recover_unknowns(&reader).await;
            let summary = match recovery {
                Ok(summary) => summary,
                Err(e) => {
                    *self.coordinator.lock() = Some(value);
                    session.stop().await?;
                    return Err(e.into());
                }
            };

            if summary.rounds > 0 {
                log::info!(
                    "Completed {} bounded PAPI command recovery rounds, applied {} reports, {} operations remain unresolved, budget_exhausted={}",
                    summary.rounds,
                    summary.reports_applied,
                    summary.unresolved,
                    summary.budget_exhausted,
                );
            }

            let trading = self
                .config
                .trading
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("PAPI trading configuration is unavailable"))?;
            let risk_checkpoint = match acknowledger.refresh_checkpoint() {
                Ok(checkpoint) => checkpoint,
                Err(e) => {
                    *self.coordinator.lock() = Some(value);
                    session.stop().await?;
                    return Err(e);
                }
            };
            let risk = match reader.collect_verified_risk_snapshot(trading).await {
                Ok(risk) => risk,
                Err(e) => {
                    *self.coordinator.lock() = Some(value);
                    session.stop().await?;
                    return Err(e);
                }
            };

            if let Err(e) = value.install_evidence(risk, Instant::now()) {
                *self.coordinator.lock() = Some(value);
                session.stop().await?;
                return Err(e.into());
            }
            *self.coordinator.lock() = Some(value);

            if let Err(e) = acknowledger.authorize_trading(risk_checkpoint) {
                session.stop().await?;
                return Err(e);
            }
            self.risk_refresh.ready.store(true, Ordering::Release);

            if self.risk_refresh.hard_refresh_pending() {
                session.revoke_trading_authorization();

                if let Err(e) = ensure_risk_rebaseline_started(
                    &self.risk_refresh,
                    &self.coordinator,
                    &self.application_acknowledger,
                ) {
                    log::warn!("PAPI startup risk rebaseline could not start: {e}");
                }
            }
        }

        if let Some(trading) = self.config.trading.clone() {
            let cancel = self.pending_tasks.cancellation_token();
            self.spawn_task(
                "risk refresh",
                run_risk_refresh_worker(
                    reader,
                    trading,
                    self.core.client_id,
                    self.core.account_id,
                    self.core.venue,
                    self.emitter.clone(),
                    Arc::clone(&self.coordinator),
                    Arc::clone(&self.application_acknowledger),
                    Arc::clone(&self.risk_refresh),
                    cancel,
                ),
            )?;
        }
        self.session = Some(session);
        self.core.set_connected();
        self.trading_connected.store(true, Ordering::Release);
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.core.set_stopped();
        self.begin_shutdown();
        self.trading_connected.store(false, Ordering::Release);
        self.coordinator.lock().take();
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.begin_shutdown();
        self.trading_connected.store(false, Ordering::Release);

        let session_result = if let Some(mut session) = self.session.take() {
            session.stop().await
        } else {
            Ok(())
        };
        let tasks_result = self.finish_pending_tasks().await;
        self.reader.borrow_mut().take();
        self.application_acknowledger.lock().take();
        self.pending_application.lock().take();
        session_result?;
        tasks_result?;
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.begin_shutdown();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.core.set_stopped();
        self.begin_shutdown();
        self.trading_connected.store(false, Ordering::Release);
        self.coordinator.lock().take();
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = self.core.get_order(&cmd.client_order_id)?;
        if order.is_closed() {
            self.emitter
                .emit_order_denied(&order, "Cannot submit a closed PAPI order");
            return Ok(());
        }
        let reader = match self.trading_reader() {
            Ok(reader) => reader,
            Err(e) => {
                self.emitter.emit_order_denied(&order, &e.to_string());
                return Ok(());
            }
        };
        let operation = match submit_operation(&cmd, self.core.account_id) {
            Ok(operation) => operation,
            Err(e) => {
                self.emitter.emit_order_denied(&order, &e.to_string());
                return Ok(());
            }
        };
        let operation_id = operation.operation_id;

        if let Err(e) = self
            .coordinator
            .lock()
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("PAPI command coordinator is unavailable"))?
            .prepare_submit(operation, Instant::now())
        {
            self.emitter.emit_order_denied(&order, &e.to_string());
            return Ok(());
        }

        let coordinator = Arc::clone(&self.coordinator);
        let emitter = self.emitter.clone();
        let barrier_emitter = emitter.clone();
        let barrier_order = order.clone();
        let task_order = order.clone();

        if let Err(e) = self.spawn_task("submit_order", async move {
            let result = reader
                .dispatch_submit(&coordinator, operation_id, move || {
                    barrier_emitter.try_emit_order_submitted(&barrier_order)
                })
                .await;
            Self::handle_submit_dispatch(&emitter, &task_order, result);
            Ok(())
        }) {
            if let Some(coordinator) = self.coordinator.lock().as_mut()
                && let Err(transition_error) = coordinator.transition(
                    operation_id,
                    PapiOperationStage::Resolved {
                        resolution: PapiOperationResolution::NotSent,
                    },
                    get_atomic_clock_realtime().get_time_ns(),
                )
            {
                log::error!("Failed to resolve unspawned PAPI submit: {transition_error}");
            }
            self.emitter.emit_order_denied(&order, &e.to_string());
        }
        Ok(())
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> anyhow::Result<()> {
        let orders = self.core.get_orders_for_list(&cmd.order_list)?;
        for order in &orders {
            self.emitter.emit_order_denied(
                order,
                "PAPI linked and batch order submission is not supported",
            );
        }
        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        let order = self.core.get_order(&cmd.client_order_id)?;
        self.emitter.emit_order_modify_rejected(
            &order,
            cmd.venue_order_id,
            "PAPI order modification is not supported",
            get_atomic_clock_realtime().get_time_ns(),
        );
        Ok(())
    }

    fn batch_modify_orders(&self, cmd: BatchModifyOrders) -> anyhow::Result<()> {
        anyhow::ensure!(
            !cmd.modifies.is_empty(),
            "PAPI batch modification cannot be empty"
        );

        for modify in cmd.modifies {
            self.modify_order(modify)?;
        }
        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let order = self.core.get_order(&cmd.client_order_id)?;
        let reader = match self.trading_reader() {
            Ok(reader) => reader,
            Err(e) => {
                self.emitter.emit_order_cancel_rejected(
                    &order,
                    cmd.venue_order_id,
                    &e.to_string(),
                    get_atomic_clock_realtime().get_time_ns(),
                );
                return Ok(());
            }
        };
        let operation = match cancel_operation(&cmd, self.core.account_id) {
            Ok(operation) => operation,
            Err(e) => {
                self.emitter.emit_order_cancel_rejected(
                    &order,
                    cmd.venue_order_id,
                    &e.to_string(),
                    get_atomic_clock_realtime().get_time_ns(),
                );
                return Ok(());
            }
        };
        let preparation = self
            .coordinator
            .lock()
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("PAPI command coordinator is unavailable"))?
            .prepare_cancel(operation, get_atomic_clock_realtime().get_time_ns());
        let operation_id = match preparation {
            Ok(PapiCancelPreparation::LocalSubmitCanceled { .. }) => {
                self.emitter.emit_order_canceled(
                    &order,
                    order.venue_order_id(),
                    get_atomic_clock_realtime().get_time_ns(),
                );
                return Ok(());
            }
            Ok(PapiCancelPreparation::Prepared {
                cancel_operation_id,
            }) => cancel_operation_id,
            Err(e) => {
                self.emitter.emit_order_cancel_rejected(
                    &order,
                    cmd.venue_order_id,
                    &e.to_string(),
                    get_atomic_clock_realtime().get_time_ns(),
                );
                return Ok(());
            }
        };

        let coordinator = Arc::clone(&self.coordinator);
        let emitter = self.emitter.clone();
        let task_order = order.clone();

        if let Err(e) = self.spawn_task("cancel_order", async move {
            let result = reader
                .dispatch_cancel(&coordinator, operation_id, || Ok(()))
                .await;
            Self::handle_cancel_dispatch(&emitter, &task_order, result);
            Ok(())
        }) {
            if let Some(coordinator) = self.coordinator.lock().as_mut()
                && let Err(transition_error) = coordinator.transition(
                    operation_id,
                    PapiOperationStage::Resolved {
                        resolution: PapiOperationResolution::NotSent,
                    },
                    get_atomic_clock_realtime().get_time_ns(),
                )
            {
                log::error!("Failed to resolve unspawned PAPI cancel: {transition_error}");
            }
            self.emitter.emit_order_cancel_rejected(
                &order,
                cmd.venue_order_id,
                &e.to_string(),
                get_atomic_clock_realtime().get_time_ns(),
            );
        }
        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let operations = match self
            .coordinator
            .lock()
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PAPI command coordinator is unavailable"))?
            .plan_cancel_all(&cmd)
        {
            Ok(operations) => operations,
            Err(e) => {
                log::warn!("PAPI cancel-all rejected before dispatch: {e}");
                return Ok(());
            }
        };
        self.dispatch_cancel_operations(operations)
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        let operations = match batch_cancel_operations(&cmd, self.core.account_id) {
            Ok(operations) => operations,
            Err(e) => {
                for cancel in &cmd.cancels {
                    if let Ok(order) = self.core.get_order(&cancel.client_order_id) {
                        self.emitter.emit_order_cancel_rejected(
                            &order,
                            cancel.venue_order_id,
                            &e.to_string(),
                            get_atomic_clock_realtime().get_time_ns(),
                        );
                    }
                }

                if cmd.cancels.is_empty() {
                    return Err(e.into());
                }
                return Ok(());
            }
        };
        self.dispatch_cancel_operations(operations)
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        anyhow::bail!("Binance PAPI account state is published by the private recovery session")
    }

    fn query_order(&self, cmd: QueryOrder) -> anyhow::Result<()> {
        let reader = self.reader()?;
        let emitter = self.emitter.clone();

        self.spawn_task("query_order", async move {
            match reader
                .generate_order_status_report(
                    cmd.instrument_id,
                    cmd.venue_order_id,
                    Some(cmd.client_order_id),
                )
                .await
            {
                Ok(report) => emitter.send_order_status_report(report),
                Err(e) => log::warn!(
                    "PAPI order query returned no authoritative report for {}: {e}",
                    cmd.client_order_id
                ),
            }
            Ok(())
        })
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let instrument_id = cmd
            .instrument_id
            .ok_or_else(|| anyhow::anyhow!("PAPI single-order queries require an instrument ID"))?;
        self.reader()?
            .generate_order_status_report(instrument_id, cmd.venue_order_id, cmd.client_order_id)
            .await
            .map(Some)
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        anyhow::ensure!(
            cmd.open_only,
            "PAPI history is incomplete; use the bounded mass status report"
        );
        let reports = self
            .reader()?
            .generate_order_status_reports(cmd.instrument_id, cmd.open_only)
            .await?;
        Self::log_report_receipt(reports.len(), "order status", cmd.log_receipt_level);
        Ok(reports)
    }

    async fn generate_fill_reports(
        &self,
        _cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        anyhow::bail!("PAPI fill history is incomplete; use the bounded mass status report")
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        anyhow::ensure!(
            cmd.start.is_none() && cmd.end.is_none(),
            "PAPI position reports support current observations only"
        );
        let reports = self
            .reader()?
            .generate_position_status_reports(cmd.instrument_id)
            .await?;
        Self::log_report_receipt(reports.len(), "position status", cmd.log_receipt_level);
        Ok(reports)
    }

    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        let end = get_atomic_clock_realtime().get_time_ns();
        let start = lookback_mins
            .unwrap_or(60)
            .checked_mul(60_000_000_000)
            .and_then(|lookback| end.as_u64().checked_sub(lookback))
            .ok_or_else(|| anyhow::anyhow!("PAPI history lookback exceeds timestamp bounds"))?;
        let mut snapshot = self
            .reader()?
            .generate_mass_status(start.into(), end)
            .await?;
        snapshot.mass_status.client_id = self.core.client_id;
        Ok(Some(snapshot.mass_status))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        collections::HashMap,
        rc::Rc,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use futures_util::StreamExt;
    use nautilus_common::{
        cache::Cache,
        clock::VirtualClock,
        factories::ExecutionClientFactory,
        live::runner::replace_exec_event_sender,
        messages::{
            ExecutionEvent,
            execution::{
                GenerateFillReportsBuilder, GenerateOrderStatusReportBuilder,
                GenerateOrderStatusReportsBuilder, GeneratePositionStatusReportsBuilder,
            },
        },
    };
    use nautilus_core::{UUID4, string::secret::SecretString, time::AtomicTime};
    use nautilus_execution::engine::ExecutionEngine;
    use nautilus_live::{
        execution::manager::ExecutionManager, runner::AsyncRunner, testing::ExecutionHarness,
    };
    use nautilus_model::{
        accounts::{AccountAny, MarginAccount},
        enums::{OrderSide, OrderStatus, OrderType, TimeInForce},
        events::OrderInitialized,
        identifiers::{AccountId, ClientOrderId, OrderListId, StrategyId, TraderId},
        orders::{MarketOrder, Order, OrderAny, OrderList, OrderTestBuilder},
        types::{Currency, Money, Price, Quantity},
    };
    use nautilus_portfolio::portfolio::Portfolio;
    use rstest::rstest;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        config::{
            BinancePapiExecutionClientConfig, BinancePapiInstrumentTradingConfig,
            BinancePapiTradingConfig,
        },
        factories::BinancePapiExecutionClientFactory,
        testing::{self, MockServer, Reply, TRADE_TIME, ms},
        trading::coordinator::{
            PapiAccountStatusEvidence, PapiMarginRuleEvidence, PapiPositionModeEvidence,
            PapiRiskUnitEvidence, PapiVerifiedInstrumentRisk, PapiVerifiedInstrumentRules,
            PapiVerifiedRiskSnapshot,
        },
    };

    fn client() -> Box<dyn ExecutionClient> {
        install_exec_event_sender();
        BinancePapiExecutionClientFactory::new()
            .create(
                TraderId::from("TRADER-001"),
                "BINANCE_PAPI",
                &BinancePapiExecutionClientConfig::default(),
                Rc::new(RefCell::new(Cache::default())).into(),
                Rc::new(RefCell::new(VirtualClock::new())),
            )
            .unwrap()
    }

    fn configured_client(
        server: &MockServer,
        symbols: &[&str],
        preload: bool,
    ) -> Box<dyn ExecutionClient> {
        install_exec_event_sender();
        let cache = Rc::new(RefCell::new(Cache::default()));

        if preload {
            for symbol in symbols {
                cache
                    .borrow_mut()
                    .add_instrument(testing::instrument(symbol))
                    .unwrap();
            }
        }

        let config = BinancePapiExecutionClientConfig {
            read_only: Some(testing::config(&server.url)),
            instrument_ids: symbols
                .iter()
                .map(|symbol| InstrumentId::from(format!("{symbol}-PERP.BINANCE")))
                .collect(),
            ..Default::default()
        };
        BinancePapiExecutionClientFactory::new()
            .create(
                TraderId::from("TRADER-001"),
                "PAPI-READ-007",
                &config,
                cache.into(),
                Rc::new(RefCell::new(VirtualClock::new())),
            )
            .unwrap()
    }

    fn install_exec_event_sender() {
        let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();
        replace_exec_event_sender(sender);
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_account_updates_extend_one_bounded_risk_refresh_window() {
        let control = Arc::new(PapiRiskRefreshControl::default());
        let updates = Arc::clone(&control);
        let cancel = tokio_util::sync::CancellationToken::new();
        control.mark_dirty(1, false);
        let started = tokio::time::Instant::now();
        let producer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            updates.mark_dirty(2, false);
            tokio::time::sleep(Duration::from_millis(5)).await;
            updates.mark_dirty(3, false);
        });

        let generation = coalesced_risk_generation(
            &control,
            Duration::from_millis(15),
            Duration::from_millis(100),
            &cancel,
        )
        .await;

        producer.await.unwrap();
        assert_eq!(generation, Some(3));
        assert_eq!(started.elapsed(), Duration::from_millis(25));
    }

    #[tokio::test(start_paused = true)]
    async fn continuous_account_updates_cannot_extend_risk_refresh_past_maximum_delay() {
        let control = Arc::new(PapiRiskRefreshControl::default());
        let updates = Arc::clone(&control);
        let cancel = tokio_util::sync::CancellationToken::new();
        control.mark_dirty(1, false);
        let started = tokio::time::Instant::now();
        let producer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            updates.mark_dirty(2, false);
            tokio::time::sleep(Duration::from_secs(1)).await;
            updates.mark_dirty(3, false);
            tokio::time::sleep(Duration::from_secs(2)).await;
            updates.mark_dirty(4, false);
        });

        let generation = coalesced_risk_generation(
            &control,
            Duration::from_secs(3),
            Duration::from_secs(5),
            &cancel,
        )
        .await;

        producer.await.unwrap();
        assert_eq!(generation, Some(4));
        assert_eq!(started.elapsed(), Duration::from_secs(5));
    }

    #[rstest]
    fn soft_dirty_does_not_activate_hard_admission_gate() {
        let control = PapiRiskRefreshControl::default();

        control.mark_dirty(1, false);
        assert!(!control.hard_refresh_pending());

        control.mark_dirty(2, true);
        assert!(control.hard_refresh_pending());

        control.mark_clean();
        assert!(!control.hard_refresh_pending());
    }

    fn trading_config(path: std::path::PathBuf) -> BinancePapiTradingConfig {
        BinancePapiTradingConfig {
            command_journal_path: path,
            risk_currency: Currency::USDT(),
            instrument_limits: vec![BinancePapiInstrumentTradingConfig {
                instrument_id: InstrumentId::from("BTCUSDT-PERP.BINANCE"),
                max_order_quantity: dec!(1.00),
                max_order_notional: dec!(50000.00),
                max_position_quantity: dec!(2.00),
                max_instrument_exposure: dec!(100000.00),
            }],
            max_account_exposure: dec!(100000.00),
            max_in_flight_operations: 4,
            max_risk_age_ms: 10_000,
            max_risk_collection_span_ms: 1_000,
            max_recovery_requests: 32,
            max_recovery_rounds: 3,
            recovery_recheck_interval_ms: 250,
            market_order_price_buffer_bps: 100,
            fee_buffer_bps: 10,
        }
    }

    fn trading_client(server: &MockServer, path: std::path::PathBuf) -> Box<dyn ExecutionClient> {
        install_exec_event_sender();
        let cache = Rc::new(RefCell::new(Cache::default()));
        cache
            .borrow_mut()
            .add_instrument(testing::instrument("BTCUSDT"))
            .unwrap();
        let config = BinancePapiExecutionClientConfig {
            read_only: Some(testing::config(&server.url)),
            instrument_ids: vec![InstrumentId::from("BTCUSDT-PERP.BINANCE")],
            trading: Some(trading_config(path)),
            ..Default::default()
        };
        BinancePapiExecutionClientFactory::new()
            .create(
                TraderId::from("TRADER-001"),
                "PAPI-TRADING-001",
                &config,
                cache.into(),
                Rc::new(RefCell::new(VirtualClock::new())),
            )
            .unwrap()
    }

    fn verified_risk_snapshot(now: Instant) -> PapiVerifiedRiskSnapshot {
        let generation = 1;
        let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
        PapiVerifiedRiskSnapshot {
            account_id: AccountId::from("BINANCE-PAPI-001"),
            generation,
            observed_at: now,
            collection_span: Duration::from_millis(10),
            status: PapiAccountStatusEvidence::Normal {
                endpoint: "/papi/v1/account",
                generation,
            },
            position_mode: PapiPositionModeEvidence::OneWay {
                endpoint: "/papi/v1/um/positionSide/dual",
                generation,
            },
            units: PapiRiskUnitEvidence {
                currency: Currency::USDT(),
                source: "offline authenticated-unit fixture",
            },
            margin_rule: PapiMarginRuleEvidence {
                source: "offline authenticated-bracket fixture",
                generation,
                max_initial_margin_rate: dec!(0.10),
            },
            available_initial_margin: dec!(50000),
            account_exposure: Decimal::ZERO,
            instruments: HashMap::from([(
                instrument_id,
                PapiVerifiedInstrumentRisk {
                    signed_position_quantity: Decimal::ZERO,
                    exposure: Decimal::ZERO,
                    reference_price: dec!(30000),
                    price_source: "offline authenticated-mark-price fixture",
                    price_generation: generation,
                    price_observed_at: now,
                    rules: PapiVerifiedInstrumentRules {
                        source: "offline authenticated-exchange-info fixture",
                        generation,
                        settlement_currency: Currency::USDT(),
                        trading: true,
                        price_increment: dec!(0.01),
                        quantity_increment: dec!(0.001),
                        min_price: Some(dec!(1)),
                        max_price: Some(dec!(1000000)),
                        min_quantity: Some(dec!(0.001)),
                        max_quantity: Some(dec!(1000)),
                        min_notional: Some(dec!(10)),
                        max_notional: Some(dec!(1000000)),
                    },
                },
            )]),
            open_orders: Vec::new(),
        }
    }

    fn engine_trading_harness(
        server: &MockServer,
        install_evidence: bool,
    ) -> (ExecutionHarness, TempDir) {
        let trader_id = TraderId::from("TRADER-001");
        let client_id = ClientId::from("PAPI-TRADING-ENGINE-001");
        let account_id = AccountId::from("BINANCE-PAPI-001");
        let instrument = testing::instrument("BTCUSDT");
        let harness = ExecutionHarness::new(trader_id, client_id, account_id, instrument);
        let account_state = AccountState::new(
            account_id,
            AccountType::Margin,
            Vec::new(),
            Vec::new(),
            true,
            UUID4::new(),
            UnixNanos::default(),
            UnixNanos::default(),
            None,
        )
        .with_total_only_balances(vec![Money::from("100000 USDT")])
        .unwrap();
        harness
            .cache()
            .borrow_mut()
            .add_account(AccountAny::Margin(MarginAccount::new(account_state, false)))
            .unwrap();

        let directory = TempDir::new().unwrap();
        let config = BinancePapiExecutionClientConfig {
            read_only: Some(testing::config(&server.url)),
            instrument_ids: vec![InstrumentId::from("BTCUSDT-PERP.BINANCE")],
            trading: Some(trading_config(
                directory.path().join("papi-commands.journal"),
            )),
            ..Default::default()
        };
        let core = ExecutionClientCore::new(
            trader_id,
            client_id,
            Venue::from("BINANCE"),
            OmsType::Netting,
            account_id,
            AccountType::Margin,
            None,
            harness.cache().clone(),
        );
        let mut client = BinancePapiExecutionClient::new(core, config);
        client.start().unwrap();
        let reader = BinancePapiReadOnlyClient::from_parts(
            client.config.read_only.as_ref().unwrap(),
            vec![testing::instrument("BTCUSDT")],
            testing::gate(),
            Arc::new(AtomicTime::default()),
        )
        .unwrap();
        *client.reader.borrow_mut() = Some(reader);
        client.core.set_connected();
        client.trading_connected.store(true, Ordering::Release);
        if install_evidence {
            client
                .coordinator
                .lock()
                .as_mut()
                .unwrap()
                .install_evidence(verified_risk_snapshot(Instant::now()), Instant::now())
                .unwrap();
        }
        harness.register_client(Box::new(client)).unwrap();
        (harness, directory)
    }

    fn engine_limit_order(
        client_order_id: &str,
        time_in_force: TimeInForce,
        post_only: bool,
    ) -> OrderAny {
        OrderTestBuilder::new(OrderType::Limit)
            .trader_id(TraderId::from("TRADER-001"))
            .strategy_id(StrategyId::from("TEST-001"))
            .instrument_id(InstrumentId::from("BTCUSDT-PERP.BINANCE"))
            .client_order_id(ClientOrderId::from(client_order_id))
            .side(OrderSide::Buy)
            .quantity(Quantity::from("0.001"))
            .price(Price::from("30000.00"))
            .time_in_force(time_in_force)
            .post_only(post_only)
            .build()
    }

    fn command_ack(request: &testing::RecordedRequest) -> Reply {
        Reply::json(&json!({
            "symbol": request.params["symbol"],
            "orderId": 42,
            "clientOrderId": request.params["newClientOrderId"],
        }))
    }

    async fn recovery_websocket() -> (
        String,
        tokio::sync::mpsc::UnboundedSender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/ws", listener.local_addr().unwrap());
        let (close_tx, mut close_rx) = tokio::sync::mpsc::unbounded_channel();

        let task = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();

                loop {
                    tokio::select! {
                        command = close_rx.recv() => {
                            if command.is_none() { return; }
                            websocket.close(None).await.unwrap();
                            break;
                        }
                        message = websocket.next() => {
                            if message.is_none() { break; }
                        }
                    }
                }
            }
        });
        (url, close_tx, task)
    }

    fn current_trading_reply(request: &testing::RecordedRequest) -> Reply {
        let mut reply = testing::supported_trading(request);

        if matches!(
            request.path.as_str(),
            "/fapi/v1/premiumIndex" | "/sapi/v1/portfolio/asset-index-price"
        ) {
            let mut value: serde_json::Value = serde_json::from_str(&reply.body).unwrap();
            let now = get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000;

            if value.is_array() {
                value[0]["time"] = json!(now);
            } else {
                value["time"] = json!(now);
            }
            reply = Reply::json(&value);
        }
        reply
    }

    async fn connected_recovery_client(
        server: &MockServer,
        websocket_url: String,
        journal_path: std::path::PathBuf,
    ) -> (ExecutionHarness, BinancePapiExecutionClient) {
        let harness = ExecutionHarness::new(
            TraderId::from("TRADER-001"),
            ClientId::from("PAPI-RECOVERY"),
            AccountId::from("BINANCE-PAPI-001"),
            testing::instrument("BTCUSDT"),
        );
        let mut read_only = testing::config(&server.url);
        read_only.websocket_url = SecretString::from(websocket_url);
        read_only.request_timeout = Duration::from_secs(3);
        let mut trading = trading_config(journal_path);
        trading.max_risk_age_ms = 60_000;
        trading.max_risk_collection_span_ms = 10_000;
        let config = BinancePapiExecutionClientConfig {
            read_only: Some(read_only),
            trading: Some(trading),
            instrument_ids: vec![harness.instrument_id()],
            ..Default::default()
        };
        let core = ExecutionClientCore::new(
            harness.trader_id(),
            harness.client_id(),
            Venue::from("BINANCE"),
            OmsType::Netting,
            harness.account_id(),
            AccountType::Margin,
            None,
            harness.cache().clone(),
        );
        let mut client = BinancePapiExecutionClient::new(core, config);
        client.start().unwrap();
        client.connect().await.unwrap();
        (harness, client)
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn reconnect_blocks_increase_risk_until_recovery_and_risk_application(
        #[case] second_gap: bool,
    ) {
        let gap = Arc::new(AtomicBool::new(false));
        let pending = Arc::new(tokio::sync::Notify::new());
        let server_gap = Arc::clone(&gap);
        let server_pending = Arc::clone(&pending);
        let server = MockServer::new(move |request| {
            if request.method == "POST" && request.path == "/papi/v1/um/order" {
                return command_ack(request);
            }
            let mut reply = current_trading_reply(request);

            if request.path == "/papi/v1/balance" && server_gap.swap(false, Ordering::AcqRel) {
                server_pending.notify_one();
                reply.delay = Duration::from_secs(1);
            }
            reply
        })
        .await;
        let directory = TempDir::new().unwrap();
        let (url, close, websocket) = recovery_websocket().await;
        let (mut harness, client) =
            connected_recovery_client(&server, url, directory.path().join("commands.journal"))
                .await;
        let acknowledger = client.application_acknowledger.lock().clone().unwrap();
        let coordinator = Arc::clone(&client.coordinator);
        let reader = client.reader().unwrap();
        harness.register_client(Box::new(client)).unwrap();
        assert!(
            harness
                .pump_until(Duration::from_secs(3), |_| acknowledger
                    .increase_risk_allowed())
                .await
        );
        let queued = engine_limit_order("GAP-QUEUED", TimeInForce::Gtc, false);
        let command = SubmitOrder::from_order(
            &queued,
            harness.trader_id(),
            Some(harness.client_id()),
            None,
            UUID4::new(),
            UnixNanos::default(),
        );
        let operation = submit_operation(&command, harness.account_id()).unwrap();
        let operation_id = operation.operation_id;
        coordinator
            .lock()
            .as_mut()
            .unwrap()
            .prepare_submit(operation, Instant::now())
            .unwrap();
        let old_checkpoint = acknowledger.refresh_checkpoint().unwrap();
        gap.store(true, Ordering::Release);
        close.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), pending.notified())
            .await
            .unwrap();

        if second_gap {
            gap.store(true, Ordering::Release);
            close.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(6), pending.notified())
                .await
                .unwrap();
        }

        // This command was admitted before the gap but has not crossed the HTTP barrier
        let queued_result = reader
            .dispatch_submit(&coordinator, operation_id, || Ok(()))
            .await;
        assert!(queued_result.is_err());
        assert!(matches!(
            coordinator.lock().as_ref().unwrap().journal().operations()[&operation_id].stage,
            PapiOperationStage::Resolved {
                resolution: PapiOperationResolution::NotSent
            }
        ));
        let denied = engine_limit_order("GAP-DENIED", TimeInForce::Gtc, false);
        harness.submit_via_risk(&denied);
        assert!(
            harness
                .pump_until(Duration::from_millis(500), |cache| {
                    cache
                        .order(&denied.client_order_id())
                        .is_some_and(|order| order.status() == OrderStatus::Denied)
                })
                .await
        );
        assert!(!acknowledger.increase_risk_allowed());
        assert!(acknowledger.authorize_trading(old_checkpoint).is_err());
        assert!(
            !server
                .requests()
                .iter()
                .any(|request| request.path == "/papi/v1/um/order" && request.method == "POST")
        );

        // Let REST converge without allowing the runner to apply its queued reports
        nautilus_common::testing::wait_until_async(
            || async { acknowledger.refresh_checkpoint().is_ok() },
            Duration::from_secs(6),
        )
        .await;
        assert!(acknowledger.applied_refresh_checkpoint().is_err());
        assert!(!acknowledger.increase_risk_allowed());

        assert!(
            harness
                .pump_until(Duration::from_secs(12), |_| acknowledger
                    .increase_risk_allowed())
                .await
        );
        let accepted = engine_limit_order("GAP-RECOVERED", TimeInForce::Gtc, false);
        harness.submit_via_risk(&accepted);
        assert!(
            harness
                .pump_until(Duration::from_secs(3), |cache| {
                    cache
                        .order(&accepted.client_order_id())
                        .is_some_and(|order| order.status() == OrderStatus::Accepted)
                })
                .await
        );
        assert_eq!(
            server
                .requests()
                .iter()
                .filter(|request| request.path == "/papi/v1/um/order" && request.method == "POST")
                .count(),
            1
        );
        harness.exec_engine().borrow_mut().stop_clients();
        websocket.abort();
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn journal_restart_restores_exact_strategy_without_claiming_external_order(
        #[case] startup: bool,
    ) {
        let resumed = Arc::new(AtomicBool::new(false));
        let server_resumed = Arc::clone(&resumed);
        let order_time = get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000 - 1000;
        let server = MockServer::new(move |request| {
            if request.method == "POST" && request.path == "/papi/v1/um/order" {
                return Reply {
                    delay: Duration::from_secs(1),
                    ..command_ack(request)
                };
            }

            if server_resumed.load(Ordering::Acquire)
                && matches!(
                    request.path.as_str(),
                    "/papi/v1/um/openOrders" | "/papi/v1/um/allOrders" | "/papi/v1/um/order"
                )
            {
                let mut owned = testing::order();
                owned["clientOrderId"] = json!("RESTART-OWNED");
                owned["side"] = json!("BUY");
                owned["origQty"] = json!("0.001");
                owned["price"] = json!("30000.00");
                owned["orderId"] = json!(42);
                owned["time"] = json!(order_time);
                owned["updateTime"] = owned["time"].clone();

                if request.path == "/papi/v1/um/order" {
                    return Reply::json(&owned);
                }
                let mut external = owned.clone();
                external["clientOrderId"] = json!("RESTART-EXTERNAL");
                external["orderId"] = json!(43);
                return Reply::json(&json!([owned, external]));
            }
            current_trading_reply(request)
        })
        .await;
        let (mut first, directory) = engine_trading_harness(&server, true);
        let owned = engine_limit_order("RESTART-OWNED", TimeInForce::Gtc, false);
        first.submit_via_risk(&owned);
        server.wait_for_requests(1).await;
        first.pump_for(Duration::from_millis(700)).await;
        first.exec_engine().borrow_mut().stop_clients();
        drop(first);
        resumed.store(true, Ordering::Release);

        let (url, _close, websocket) = recovery_websocket().await;
        let (mut second, client) =
            connected_recovery_client(&server, url, directory.path().join("papi-commands.journal"))
                .await;
        let report = client
            .pending_application
            .lock()
            .as_ref()
            .unwrap()
            .report
            .clone();
        second.register_client(Box::new(client)).unwrap();

        if startup {
            let ExecutionReport::MassStatus(snapshot) = report else {
                panic!("expected recovery snapshot")
            };
            let mut manager = ExecutionManager::new(
                second.clock().clone(),
                second.cache().clone(),
                Default::default(),
            )
            .unwrap();
            manager.reconcile_execution_mass_status(&snapshot, second.exec_engine());
            assert_eq!(
                second
                    .cache()
                    .borrow()
                    .order(&owned.client_order_id())
                    .unwrap()
                    .strategy_id(),
                owned.strategy_id()
            );
        }
        assert!(
            second
                .pump_until(Duration::from_secs(3), |cache| {
                    cache
                        .order(&owned.client_order_id())
                        .is_some_and(|order| order.status() == OrderStatus::Accepted)
                        && cache
                            .order(&ClientOrderId::from("RESTART-EXTERNAL"))
                            .is_some()
                })
                .await
        );
        let cache = second.cache().borrow();
        assert_eq!(
            cache.order(&owned.client_order_id()).unwrap().strategy_id(),
            owned.strategy_id()
        );
        assert_eq!(
            cache
                .order(&ClientOrderId::from("RESTART-EXTERNAL"))
                .unwrap()
                .strategy_id(),
            StrategyId::external()
        );
        drop(cache);
        assert_eq!(
            server
                .requests()
                .iter()
                .filter(|request| request.path == "/papi/v1/um/order" && request.method == "POST")
                .count(),
            1
        );
        assert!(
            server
                .requests()
                .iter()
                .any(|request| request.path == "/papi/v1/um/order" && request.method == "GET")
        );
        second.exec_engine().borrow_mut().stop_clients();
        websocket.abort();
    }

    #[tokio::test]
    async fn engine_risk_route_dispatches_one_submit_and_applies_acceptance() {
        let server = MockServer::new(|request| {
            if request.method == "POST" && request.path == "/papi/v1/um/order" {
                command_ack(request)
            } else {
                testing::quiet(request)
            }
        })
        .await;
        let (mut harness, _directory) = engine_trading_harness(&server, true);
        let order = engine_limit_order("ENGINE-ACCEPT-001", TimeInForce::Gtc, false);

        harness.submit_via_risk(&order);
        let accepted = harness
            .pump_until(Duration::from_secs(3), |cache| {
                cache
                    .order(&order.client_order_id())
                    .is_some_and(|cached| cached.status() == OrderStatus::Accepted)
            })
            .await;

        assert!(accepted, "PAPI order did not reach Accepted");
        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "POST");
        assert_eq!(requests[0].path, "/papi/v1/um/order");
        assert_eq!(requests[0].params["quantity"], "0.001");
        assert_eq!(requests[0].params["price"], "30000.00");
        assert_eq!(harness.risk_command_count(), 1);
    }

    #[tokio::test]
    async fn engine_risk_route_denies_missing_evidence_without_write_request() {
        let server = MockServer::new(testing::quiet).await;
        let (mut harness, _directory) = engine_trading_harness(&server, false);
        let order = engine_limit_order("ENGINE-DENY-001", TimeInForce::Gtc, false);

        harness.submit_via_risk(&order);
        let denied = harness
            .pump_until(Duration::from_secs(3), |cache| {
                cache
                    .order(&order.client_order_id())
                    .is_some_and(|cached| cached.status() == OrderStatus::Denied)
            })
            .await;

        assert!(
            denied,
            "PAPI order was not denied without admission evidence"
        );
        assert!(server.requests().is_empty());
    }

    #[tokio::test]
    async fn engine_risk_route_denies_unsupported_terms_without_write_request() {
        let server = MockServer::new(testing::quiet).await;
        let (mut harness, _directory) = engine_trading_harness(&server, true);
        let order = engine_limit_order("ENGINE-DENY-002", TimeInForce::Ioc, true);

        harness.submit_via_risk(&order);
        let denied = harness
            .pump_until(Duration::from_secs(3), |cache| {
                cache
                    .order(&order.client_order_id())
                    .is_some_and(|cached| cached.status() == OrderStatus::Denied)
            })
            .await;

        assert!(denied, "unsupported PAPI order terms were not denied");
        assert!(server.requests().is_empty());
    }

    #[tokio::test]
    async fn engine_risk_route_applies_explicit_venue_rejection() {
        let server = MockServer::new(|request| {
            if request.method == "POST" && request.path == "/papi/v1/um/order" {
                Reply::raw(400, r#"{"code":-2010,"msg":"NEW_ORDER_REJECTED"}"#)
            } else {
                testing::quiet(request)
            }
        })
        .await;
        let (mut harness, _directory) = engine_trading_harness(&server, true);
        let order = engine_limit_order("ENGINE-REJECT-001", TimeInForce::Gtc, false);

        harness.submit_via_risk(&order);
        let rejected = harness
            .pump_until(Duration::from_secs(3), |cache| {
                cache
                    .order(&order.client_order_id())
                    .is_some_and(|cached| cached.status() == OrderStatus::Rejected)
            })
            .await;

        assert!(rejected, "explicit PAPI rejection was not applied");
        assert_eq!(server.requests().len(), 1);
    }

    #[tokio::test]
    async fn engine_risk_route_keeps_timeout_ambiguous_after_one_dispatch() {
        let server = MockServer::new(|request| {
            if request.method == "POST" && request.path == "/papi/v1/um/order" {
                Reply {
                    delay: Duration::from_secs(1),
                    ..command_ack(request)
                }
            } else {
                testing::quiet(request)
            }
        })
        .await;
        let (mut harness, _directory) = engine_trading_harness(&server, true);
        let order = engine_limit_order("ENGINE-UNKNOWN-001", TimeInForce::Gtc, false);

        harness.submit_via_risk(&order);
        let submitted = harness
            .pump_until(Duration::from_secs(3), |cache| {
                cache
                    .order(&order.client_order_id())
                    .is_some_and(|cached| cached.status() == OrderStatus::Submitted)
            })
            .await;
        assert!(submitted, "PAPI dispatch barrier did not emit Submitted");
        harness.pump_for(Duration::from_millis(700)).await;

        assert_eq!(server.requests().len(), 1);
        assert_eq!(
            harness
                .cache()
                .borrow()
                .order(&order.client_order_id())
                .map(|cached| cached.status()),
            Some(OrderStatus::Submitted),
        );
    }

    #[tokio::test]
    async fn trading_start_requires_an_exclusive_durable_journal() {
        let server = MockServer::new(testing::quiet).await;
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("papi-commands.journal");
        let mut first = trading_client(&server, path.clone());
        let mut second = trading_client(&server, path.clone());

        first.start().unwrap();
        assert!(path.is_file());
        let e = second.start().unwrap_err();
        assert!(e.to_string().contains("already locked"));
        assert!(!second.is_connected());

        first.stop().unwrap();
        second.start().unwrap();
        second.stop().unwrap();
        assert!(server.requests().is_empty());
    }

    #[tokio::test]
    async fn corrupt_command_journal_prevents_client_start() {
        let server = MockServer::new(testing::quiet).await;
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("papi-commands.journal");
        std::fs::write(&path, [b'x'; 64]).unwrap();
        let mut client = trading_client(&server, path);

        let e = client.start().unwrap_err();
        assert!(e.to_string().contains("Corrupt PAPI command journal"));
        assert!(!client.is_connected());
        assert!(server.requests().is_empty());
    }

    #[tokio::test]
    async fn private_recovery_publishes_account_through_runner_and_portfolio() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let websocket_url = format!("ws://{}/ws", listener.local_addr().unwrap());
        let websocket_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();

            while let Some(message) = websocket.next().await {
                if matches!(
                    message,
                    Ok(tokio_tungstenite::tungstenite::Message::Close(_))
                ) {
                    break;
                }
            }
        });
        let server = MockServer::new(|request| {
            if request.path == "/papi/v1/balance" {
                Reply::json(&testing::supported_balances())
            } else {
                testing::quiet(request)
            }
        })
        .await;
        let cache = Rc::new(RefCell::new(Cache::default()));
        cache
            .borrow_mut()
            .add_instrument(testing::instrument("BTCUSDT"))
            .unwrap();
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let _portfolio = Portfolio::new(clock.clone(), cache.clone(), None);
        let mut read_only = testing::config(&server.url);
        read_only.websocket_url = SecretString::from(websocket_url);
        let config = BinancePapiExecutionClientConfig {
            read_only: Some(read_only),
            instrument_ids: vec![InstrumentId::from("BTCUSDT-PERP.BINANCE")],
            ..Default::default()
        };
        let mut client = BinancePapiExecutionClientFactory::new()
            .create(
                TraderId::from("TRADER-001"),
                "PAPI-OBSERVE-001",
                &config,
                cache.clone().into(),
                clock,
            )
            .unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();
        replace_exec_event_sender(sender);

        client.start().unwrap();
        client.connect().await.unwrap();
        assert!(client.is_connected());
        let event = tokio::time::timeout(Duration::from_secs(3), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        AsyncRunner::handle_exec_event(event);
        let account = client.get_account().unwrap();
        assert!(!account.total_only_balances().is_empty());

        client.stop().unwrap();
        client.disconnect().await.unwrap();
        assert!(!client.is_connected());
        websocket_task.abort();
    }

    #[tokio::test]
    async fn private_order_delta_applies_through_execution_engine_and_portfolio_once() {
        use futures_util::SinkExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let websocket_url = format!("ws://{}/ws", listener.local_addr().unwrap());
        let (websocket_tx, mut websocket_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

        let websocket_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(stream).await.unwrap();

            loop {
                tokio::select! {
                    command = websocket_rx.recv() => {
                        let Some(command) = command else { break };
                        websocket
                            .send(tokio_tungstenite::tungstenite::Message::Text(command.into()))
                            .await
                            .unwrap();
                    }
                    message = websocket.next() => {
                        match message {
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_)) | Err(_))
                            | None => break,
                            _ => {}
                        }
                    }
                }
            }
        });
        let server = MockServer::new(|request| {
            if request.path == "/papi/v1/balance" {
                Reply::json(&testing::supported_balances())
            } else {
                testing::quiet(request)
            }
        })
        .await;
        let cache = Rc::new(RefCell::new(Cache::default()));
        cache
            .borrow_mut()
            .add_instrument(testing::instrument("BTCUSDT"))
            .unwrap();
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let portfolio = Portfolio::new(clock.clone(), cache.clone(), None);
        let engine = Rc::new(RefCell::new(ExecutionEngine::new(
            clock.clone(),
            cache.clone(),
            None,
        )));
        ExecutionEngine::register_msgbus_handlers(&engine);
        let mut read_only = testing::config(&server.url);
        read_only.websocket_url = SecretString::from(websocket_url);
        read_only.listen_key_keepalive_interval = Duration::from_secs(60);
        read_only.transport_rotation_interval = Duration::from_secs(60);
        let config = BinancePapiExecutionClientConfig {
            read_only: Some(read_only),
            instrument_ids: vec![InstrumentId::from("BTCUSDT-PERP.BINANCE")],
            ..Default::default()
        };
        let mut client = BinancePapiExecutionClientFactory::new()
            .create(
                TraderId::from("TRADER-001"),
                "PAPI-OBSERVE-001",
                &config,
                cache.clone().into(),
                clock,
            )
            .unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();
        replace_exec_event_sender(sender);

        client.start().unwrap();
        client.connect().await.unwrap();
        let account = tokio::time::timeout(Duration::from_secs(3), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(account, ExecutionEvent::Account(_)));
        AsyncRunner::handle_exec_event(account);
        let baseline = tokio::time::timeout(Duration::from_secs(3), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            baseline,
            ExecutionEvent::Report(ExecutionReport::MassStatus(_))
        ));
        AsyncRunner::handle_exec_event(baseline);
        let baseline_requests = server.requests().len();
        let order_event = json!({
            "e": "ORDER_TRADE_UPDATE",
            "E": 1_700_000_000_003_i64,
            "T": 1_700_000_000_002_i64,
            "fs": "UM",
            "o": {
                "s": "BTCUSDT", "c": "client-1", "i": 42,
                "S": "BUY", "o": "LIMIT", "ot": "LIMIT", "f": "GTC",
                "q": "0.010", "p": "42000.10", "ap": "42000.10",
                "R": false, "ps": "BOTH", "x": "TRADE",
                "X": "PARTIALLY_FILLED", "z": "0.001", "l": "0.001",
                "L": "42000.10", "N": "BNB", "n": "-0.00000123",
                "m": false, "T": 1_700_000_000_002_i64, "t": 7
            }
        })
        .to_string();
        websocket_tx.send(order_event.clone()).unwrap();
        let event = tokio::time::timeout(Duration::from_secs(3), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let ExecutionEvent::Report(ExecutionReport::OrderWithFills(report, fills)) = &event else {
            panic!("Expected a PAPI order-with-fills incremental report")
        };
        assert_eq!(report.client_order_id.unwrap().as_str(), "client-1");
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].commission.as_decimal(), dec!(-0.00000123));
        AsyncRunner::handle_exec_event(event);

        {
            let cache = cache.borrow();
            let cached = cache.order(&ClientOrderId::from("client-1")).unwrap();
            assert_eq!(cached.status(), OrderStatus::PartiallyFilled);
            assert_eq!(cached.filled_qty().as_decimal(), dec!(0.001));
        }
        assert_eq!(
            portfolio.net_position(&InstrumentId::from("BTCUSDT-PERP.BINANCE")),
            dec!(0.001)
        );
        assert_eq!(server.requests().len(), baseline_requests);

        websocket_tx.send(order_event).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(server.requests().len(), baseline_requests);
        assert_eq!(
            portfolio.net_position(&InstrumentId::from("BTCUSDT-PERP.BINANCE")),
            dec!(0.001)
        );

        client.stop().unwrap();
        client.disconnect().await.unwrap();
        websocket_task.abort();
    }

    #[tokio::test]
    async fn test_factory_reports_preserve_scope_identity_and_incompleteness() {
        let server = MockServer::new(|request| match request.path.as_str() {
            "/papi/v1/um/openOrders" if request.params["symbol"] == "BTCUSDT" => {
                Reply::json(&json!([testing::order()]))
            }
            "/papi/v1/um/positionRisk" if !request.params.contains_key("symbol") => {
                let mut btc = testing::position("BTCUSDT");
                btc["positionAmt"] = json!("0.010");
                btc["entryPrice"] = json!("28511.00");
                let mut eth = testing::position("ETHUSDT");
                eth["positionAmt"] = json!("-0.100");
                eth["entryPrice"] = json!("2000.00");
                Reply::json(&json!([btc, eth]))
            }
            "/papi/v1/um/order" => Reply::json(&testing::order()),
            _ => testing::quiet(request),
        })
        .await;
        let mut client = configured_client(&server, &["BTCUSDT", "ETHUSDT"], true);
        assert!(server.requests().is_empty());
        client.start().unwrap();
        let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
        let orders = GenerateOrderStatusReportsBuilder::default()
            .ts_init(UnixNanos::default())
            .instrument_id(Some(instrument_id))
            .open_only(true)
            .start(Some(ms(TRADE_TIME + 1)))
            .build()
            .unwrap();
        let positions = GeneratePositionStatusReportsBuilder::default()
            .ts_init(UnixNanos::default())
            .build()
            .unwrap();
        let single = GenerateOrderStatusReportBuilder::default()
            .ts_init(UnixNanos::default())
            .instrument_id(Some(instrument_id))
            .client_order_id(Some(ClientOrderId::from("abc")))
            .build()
            .unwrap();
        let open = client.generate_order_status_reports(&orders).await.unwrap();
        let position_reports = client
            .generate_position_status_reports(&positions)
            .await
            .unwrap();
        let order = client
            .generate_order_status_report(&single)
            .await
            .unwrap()
            .unwrap();
        let mass = client
            .generate_mass_status(Some(60))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(open.len(), 1);
        assert_eq!(open[0].instrument_id, instrument_id);
        assert_eq!(open[0].venue_order_id, order.venue_order_id);
        assert_eq!(position_reports.len(), 2);
        assert_eq!(mass.client_id, ClientId::from("PAPI-READ-007"));
        assert_eq!(mass.account_id, client.account_id());
        assert!(mass.lookback_start().is_some());
        assert!(!mass.reports_complete());
        assert!(client.provides_bulk_position_coverage(instrument_id));
        assert!(
            !client.provides_bulk_position_coverage(InstrumentId::from("BNBUSDT-PERP.BINANCE"))
        );
        assert!(!client.is_connected());
        assert!(client.get_account().is_none());
        assert!(server.requests().iter().all(|request| {
            request.method == "GET" && request.params.contains_key("signature")
        }));
        client.stop().unwrap();
        assert!(client.generate_order_status_reports(&orders).await.is_err());
        client.start().unwrap();
        assert_eq!(
            client
                .generate_order_status_reports(&orders)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    fn direct_query_client(server: &MockServer) -> BinancePapiExecutionClient {
        let cache = Rc::new(RefCell::new(Cache::default()));
        cache
            .borrow_mut()
            .add_instrument(testing::instrument("BTCUSDT"))
            .unwrap();
        let config = BinancePapiExecutionClientConfig {
            read_only: Some(testing::config(&server.url)),
            instrument_ids: vec![InstrumentId::from("BTCUSDT-PERP.BINANCE")],
            ..Default::default()
        };
        let core = ExecutionClientCore::new(
            TraderId::from("TRADER-001"),
            ClientId::from("PAPI-QUERY-001"),
            Venue::from("BINANCE"),
            OmsType::Netting,
            config.account_id,
            AccountType::Margin,
            None,
            cache,
        );
        let mut client = BinancePapiExecutionClient::new(core, config);
        install_exec_event_sender();
        client.start().unwrap();
        let reader = BinancePapiReadOnlyClient::from_parts(
            client.config.read_only.as_ref().unwrap(),
            vec![testing::instrument("BTCUSDT")],
            testing::gate(),
            Arc::new(AtomicTime::default()),
        )
        .unwrap();
        *client.reader.borrow_mut() = Some(reader);
        client
    }

    fn query_command(client: &BinancePapiExecutionClient) -> QueryOrder {
        QueryOrder::new(
            TraderId::from("TRADER-001"),
            Some(client.client_id()),
            StrategyId::from("TEST-001"),
            InstrumentId::from("BTCUSDT-PERP.BINANCE"),
            ClientOrderId::from("abc"),
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        )
    }

    #[tokio::test]
    async fn query_order_emits_authoritative_read_only_report() {
        let server = MockServer::new(|request| match request.path.as_str() {
            "/papi/v1/um/order" => Reply::json(&testing::order()),
            _ => testing::quiet(request),
        })
        .await;
        let mut client = direct_query_client(&server);
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();
        client.emitter.set_sender(sender);
        let client_order_id = ClientOrderId::from("abc");

        client.query_order(query_command(&client)).unwrap();

        let event = tokio::time::timeout(Duration::from_secs(3), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        let ExecutionEvent::Report(ExecutionReport::Order(report)) = event else {
            panic!("Expected a PAPI order status report")
        };
        assert_eq!(report.client_order_id, Some(client_order_id));
        assert_eq!(
            report.instrument_id,
            InstrumentId::from("BTCUSDT-PERP.BINANCE")
        );
        assert!(
            server
                .requests()
                .iter()
                .all(|request| request.method == "GET")
        );

        client.stop().unwrap();
        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn query_order_failure_emits_no_absence_or_terminal_event() {
        let server = MockServer::new(|request| match request.path.as_str() {
            "/papi/v1/um/order" => {
                Reply::raw(400, r#"{"code":-2013,"msg":"Order does not exist"}"#)
            }
            _ => testing::quiet(request),
        })
        .await;
        let mut client = direct_query_client(&server);
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();
        client.emitter.set_sender(sender);

        client.query_order(query_command(&client)).unwrap();
        server.wait_for_requests(1).await;
        tokio::task::yield_now().await;
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));

        client.stop().unwrap();
        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn query_order_shutdown_cancels_old_generation_and_restart_uses_a_new_reader() {
        let request_count = Arc::new(AtomicUsize::new(0));
        let handler_count = Arc::clone(&request_count);
        let server = MockServer::new(move |request| match request.path.as_str() {
            "/papi/v1/um/order" if handler_count.fetch_add(1, Ordering::Relaxed) == 0 => Reply {
                delay: Duration::from_secs(2),
                ..Reply::json(&testing::order())
            },
            "/papi/v1/um/order" => Reply::json(&testing::order()),
            _ => testing::quiet(request),
        })
        .await;
        let mut client = direct_query_client(&server);
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();
        client.emitter.set_sender(sender.clone());

        client.query_order(query_command(&client)).unwrap();
        server.wait_for_requests(1).await;
        let shutdown_started = std::time::Instant::now();
        client.stop().unwrap();
        assert!(client.query_order(query_command(&client)).is_err());
        client.disconnect().await.unwrap();
        assert!(shutdown_started.elapsed() < Duration::from_secs(1));
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));

        client.start().unwrap();
        let reader = BinancePapiReadOnlyClient::from_parts(
            client.config.read_only.as_ref().unwrap(),
            vec![testing::instrument("BTCUSDT")],
            testing::gate(),
            Arc::new(AtomicTime::default()),
        )
        .unwrap();
        *client.reader.borrow_mut() = Some(reader);
        client.emitter.set_sender(sender);
        client.query_order(query_command(&client)).unwrap();

        let event = tokio::time::timeout(Duration::from_secs(3), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            event,
            ExecutionEvent::Report(ExecutionReport::Order(_))
        ));
        client.stop().unwrap();
        client.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn test_factory_failed_queries_remain_errors_and_never_publish_an_account() {
        let server = MockServer::new(|request| match request.path.as_str() {
            "/papi/v1/um/openOrders" | "/papi/v1/um/positionRisk" => Reply::raw(401, "{}"),
            _ => testing::quiet(request),
        })
        .await;
        let mut client = configured_client(&server, &["BTCUSDT"], true);
        client.start().unwrap();
        let orders = GenerateOrderStatusReportsBuilder::default()
            .ts_init(UnixNanos::default())
            .open_only(true)
            .build()
            .unwrap();
        let positions = GeneratePositionStatusReportsBuilder::default()
            .ts_init(UnixNanos::default())
            .build()
            .unwrap();
        let single = GenerateOrderStatusReportBuilder::default()
            .ts_init(UnixNanos::default())
            .instrument_id(Some(InstrumentId::from("BTCUSDT-PERP.BINANCE")))
            .client_order_id(Some(ClientOrderId::from("abc")))
            .build()
            .unwrap();

        assert!(client.generate_order_status_reports(&orders).await.is_err());
        assert!(
            client
                .generate_position_status_reports(&positions)
                .await
                .is_err()
        );
        assert!(client.generate_order_status_report(&single).await.is_err());
        assert!(client.generate_mass_status(None).await.is_err());
        assert!(client.get_account().is_none());
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn test_factory_rejects_unverifiable_filters_and_history_vectors_before_requests() {
        let server = MockServer::new(testing::quiet).await;
        let mut client = configured_client(&server, &["BTCUSDT"], true);
        client.start().unwrap();
        let history = GenerateOrderStatusReportsBuilder::default()
            .ts_init(UnixNanos::default())
            .open_only(false)
            .build()
            .unwrap();
        let fills = GenerateFillReportsBuilder::default()
            .ts_init(UnixNanos::default())
            .build()
            .unwrap();
        let positions = GeneratePositionStatusReportsBuilder::default()
            .ts_init(UnixNanos::default())
            .start(Some(ms(TRADE_TIME)))
            .build()
            .unwrap();

        assert!(
            client
                .generate_order_status_reports(&history)
                .await
                .is_err()
        );
        assert!(client.generate_fill_reports(fills).await.is_err());
        assert!(
            client
                .generate_position_status_reports(&positions)
                .await
                .is_err()
        );
        assert!(client.generate_mass_status(Some(u64::MAX)).await.is_err());
        assert!(server.requests().is_empty());
    }

    #[tokio::test]
    async fn test_missing_metadata_and_unaccepted_account_mapping_prevent_bootstrap() {
        let server = MockServer::new(testing::quiet).await;
        let mut client = configured_client(&server, &["BTCUSDT"], false);
        client.start().unwrap();
        let positions = GeneratePositionStatusReportsBuilder::default()
            .ts_init(UnixNanos::default())
            .build()
            .unwrap();
        let error = client
            .generate_position_status_reports(&positions)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not preloaded"));
        assert!(
            client
                .connect()
                .await
                .unwrap_err()
                .to_string()
                .contains("not preloaded")
        );
        assert!(!client.is_connected());
        assert!(client.get_account().is_none());
        assert!(server.requests().is_empty());

        for _ in 0..2 {
            client.stop().unwrap();
            client.disconnect().await.unwrap();
            client.reset().unwrap();
            client.dispose().unwrap();
        }
    }

    #[tokio::test]
    async fn test_start_and_connect_fail_without_account_or_connection() {
        let mut client = client();
        assert!(
            client
                .start()
                .unwrap_err()
                .to_string()
                .contains("read-only configuration is required")
        );
        assert!(
            client
                .connect()
                .await
                .unwrap_err()
                .to_string()
                .contains("not started")
        );
        assert!(!client.is_connected());
        assert!(client.get_account().is_none());
        assert!(
            !client.provides_bulk_position_coverage(InstrumentId::from("BTCUSDT-PERP.BINANCE"))
        );

        // Repeated cleanup after a failed start must remain safe
        for _ in 0..2 {
            client.stop().unwrap();
            client.disconnect().await.unwrap();
            client.reset().unwrap();
            client.dispose().unwrap();
        }
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn test_reconciliation_never_reports_empty_success() {
        let client = client();
        let ts_init = UnixNanos::default();
        let order = GenerateOrderStatusReportBuilder::default()
            .ts_init(ts_init)
            .build()
            .unwrap();
        let orders = GenerateOrderStatusReportsBuilder::default()
            .ts_init(ts_init)
            .open_only(false)
            .build()
            .unwrap();
        let fills = GenerateFillReportsBuilder::default()
            .ts_init(ts_init)
            .build()
            .unwrap();
        let positions = GeneratePositionStatusReportsBuilder::default()
            .ts_init(ts_init)
            .build()
            .unwrap();

        assert!(client.generate_order_status_report(&order).await.is_err());
        assert!(client.generate_order_status_reports(&orders).await.is_err());
        assert!(client.generate_fill_reports(fills).await.is_err());
        assert!(
            client
                .generate_position_status_reports(&positions)
                .await
                .is_err()
        );
        assert!(client.generate_mass_status(None).await.is_err());
    }

    #[rstest]
    fn test_account_operations_fail_without_publishing_state() {
        let client = client();
        let query = QueryAccount::new(
            TraderId::from("TRADER-001"),
            Some(client.client_id()),
            client.account_id(),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );
        assert!(client.query_account(query).is_err());
        assert!(
            client
                .generate_account_state(Vec::new(), Vec::new(), true, UnixNanos::default(), None)
                .is_err()
        );
        assert!(client.get_account().is_none());
    }

    #[rstest]
    fn test_empty_batch_commands_do_not_succeed() {
        let client = client();
        let trader_id = TraderId::from("TRADER-001");
        let strategy_id = StrategyId::from("TEST-001");
        let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
        let modifies = BatchModifyOrders::new(
            trader_id,
            None,
            strategy_id,
            instrument_id,
            Vec::new(),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );
        let cancels = BatchCancelOrders::new(
            trader_id,
            None,
            strategy_id,
            instrument_id,
            Vec::new(),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );
        assert!(client.batch_modify_orders(modifies).is_err());
        assert!(client.batch_cancel_orders(cancels).is_err());
    }
    #[rstest]
    fn test_order_commands_never_report_success() {
        let client = client();
        let trader_id = TraderId::from("TRADER-001");
        let strategy_id = StrategyId::from("TEST-001");
        let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
        let client_order_id = ClientOrderId::from("ORDER-001");
        let ts_init = UnixNanos::default();

        let order = OrderAny::Market(MarketOrder::new(
            trader_id,
            strategy_id,
            instrument_id,
            client_order_id,
            OrderSide::Buy,
            Quantity::from("0.001"),
            TimeInForce::Gtc,
            UUID4::new(),
            ts_init,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ));
        let submit = SubmitOrder::from_order(&order, trader_id, None, None, UUID4::new(), ts_init);
        let list = OrderList::new(
            OrderListId::from("LIST-001"),
            instrument_id,
            strategy_id,
            vec![client_order_id],
            ts_init,
        );
        let submit_list = SubmitOrderList::new(
            trader_id,
            None,
            strategy_id,
            list,
            vec![OrderInitialized::from(&order)],
            None,
            None,
            None,
            UUID4::new(),
            ts_init,
            None,
        );
        let modify = ModifyOrder::new(
            trader_id,
            None,
            strategy_id,
            instrument_id,
            client_order_id,
            None,
            Some(Quantity::from("0.002")),
            None,
            None,
            UUID4::new(),
            ts_init,
            None,
            None,
        );
        let cancel = CancelOrder::new(
            trader_id,
            None,
            strategy_id,
            instrument_id,
            client_order_id,
            None,
            UUID4::new(),
            ts_init,
            None,
            None,
        );
        let cancel_all = CancelAllOrders::new(
            trader_id,
            None,
            strategy_id,
            instrument_id,
            None,
            UUID4::new(),
            ts_init,
            None,
            None,
        );
        let query = QueryOrder::new(
            trader_id,
            None,
            strategy_id,
            instrument_id,
            client_order_id,
            None,
            UUID4::new(),
            ts_init,
            None,
            None,
        );
        assert!(client.submit_order(submit).is_err());
        assert!(client.submit_order_list(submit_list).is_err());
        assert!(client.modify_order(modify).is_err());
        assert!(client.cancel_order(cancel).is_err());
        assert!(client.cancel_all_orders(cancel_all).is_err());
        assert!(client.query_order(query).is_err());
    }
}
