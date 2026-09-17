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

//! One-owner listen-key, transport, event-buffer, and bounded recovery lifecycle.

use std::{
    collections::BTreeSet,
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use nautilus_common::live::dst::time::Instant;
use nautilus_live::task::TaskGroup;
use nautilus_model::{
    events::AccountState,
    identifiers::{AccountId, InstrumentId},
    instruments::{Instrument, InstrumentAny},
};
use nautilus_network::{
    RECONNECTED, SocketState, SocketStateSink,
    transport::Message,
    websocket::{TransportBackend, WebSocketClient, WebSocketConfig, types::EpochMessageHandler},
};
use parking_lot::Mutex;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use super::{
    messages::{PapiWsEvent, parse_event},
    state::FactState,
};
use crate::{
    http::{BinancePapiResponseMetadata, ListenKey, error::PapiHttpError},
    read_only::{
        BinancePapiAccountProjection, BinancePapiProjectionStatus, BinancePapiReadOnlyClient,
        BinancePapiReadOnlyConfig, BinancePapiReadOnlySnapshot,
    },
};

const MAX_RECOVERY_ROUNDS: u32 = 3;
const SHUTDOWN_GRACE_SECS: u64 = 3;
const SHUTDOWN_ABORT_SECS: u64 = 2;
const KNOWN_HISTORY_LIMITATION: &str =
    "Historical retention, time selection and algo linkage await authenticated validation";

/// Private account-session lifecycle. Transport availability and synchronization stay distinct.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BinancePapiSessionState {
    /// No listen key, transport, or recovery task is active.
    #[default]
    Stopped,
    /// A listen key or initial WebSocket transport is being established.
    Connecting,
    /// The transport may be live, but declared state has not converged.
    Recovering,
    /// Declared interval and source scope converged and were applied successfully.
    Synchronized,
    /// A gap, conflict, unsupported fact, or resource bound prevents synchronization.
    Restricted,
    /// Cancellation has begun and async ownership has not fully drained.
    Stopping,
}

/// Typed recovery evidence passed to issue #5 without collapsing it into mass status.
#[derive(Clone, Debug, Serialize)]
pub struct BinancePapiRecoveryEvidence {
    /// Account covered by this result.
    pub account_id: AccountId,
    /// Product scope, currently always `UM`.
    pub product: &'static str,
    /// Complete declared instrument metadata scope.
    pub instrument_ids: Vec<InstrumentId>,
    /// Current session lifecycle state.
    pub state: BinancePapiSessionState,
    /// Whether the current account-stream transport is active.
    pub transport_connected: bool,
    /// Whether declared state has converged and the recovery callback acknowledged application.
    pub synchronized: bool,
    /// Always false in issue #4; issue #5 owns trade admission.
    pub trading_authorized: bool,
    /// Lifecycle generation, incremented on every explicit start.
    pub session_generation: u64,
    /// Listen-key owner generation, incremented on replacement.
    pub listen_key_generation: u64,
    /// Recovery generation, incremented on every bounded convergence attempt.
    pub recovery_generation: u64,
    /// Epoch within the current `nautilus-network` WebSocket client.
    pub transport_epoch: u64,
    /// Highest received fact version acknowledged by the application callback.
    pub applied_fact_version: u64,
    /// Inclusive lower bound for the most recent recovery query.
    pub window_start: Option<nautilus_core::UnixNanos>,
    /// Inclusive fixed upper bound for the most recent recovery query.
    pub window_end: Option<nautilus_core::UnixNanos>,
    /// REST source receipts contributing to the most recent recovery.
    pub responses: Vec<BinancePapiResponseMetadata>,
    /// Availability of the totals-only wallet projection.
    pub wallet_status: Option<BinancePapiProjectionStatus>,
    /// Wallet-source validation and semantic limitations.
    pub wallet_issues: Vec<String>,
    /// Availability of the independent Portfolio Margin risk projection.
    pub risk_status: Option<BinancePapiProjectionStatus>,
    /// Risk-source validation and semantic limitations.
    pub risk_issues: Vec<String>,
    /// Endpoint and generation for every contributing account source.
    pub source_generations: Vec<(&'static str, u64)>,
    /// Wallet-source collection span in monotonic nanoseconds.
    pub wallet_collection_span_ns: Option<u128>,
    /// Risk-source collection span in monotonic nanoseconds.
    pub risk_collection_span_ns: Option<u128>,
    /// Elapsed time for the latest completed recovery in milliseconds.
    pub recovery_elapsed_ms: Option<u128>,
    /// Unresolved coverage, semantic, parsing, or application limitations.
    pub issues: Vec<String>,
    /// Events currently waiting in the bounded serial queue.
    pub buffered_messages: usize,
    /// Bytes currently waiting in the bounded serial queue.
    pub buffered_bytes: usize,
    /// Retained order, algo, and trade identities.
    pub retained_facts: usize,
    /// Idempotent duplicate facts observed.
    pub duplicate_count: u64,
    /// Same-identity facts with conflicting economics or linkage.
    pub conflict_count: u64,
}

impl BinancePapiRecoveryEvidence {
    fn stopped(account_id: AccountId, instrument_ids: Vec<InstrumentId>) -> Self {
        Self {
            account_id,
            product: "UM",
            instrument_ids,
            state: BinancePapiSessionState::Stopped,
            transport_connected: false,
            synchronized: false,
            trading_authorized: false,
            session_generation: 0,
            listen_key_generation: 0,
            recovery_generation: 0,
            transport_epoch: 0,
            applied_fact_version: 0,
            window_start: None,
            window_end: None,
            responses: Vec::new(),
            wallet_status: None,
            wallet_issues: Vec::new(),
            risk_status: None,
            risk_issues: Vec::new(),
            source_generations: Vec::new(),
            wallet_collection_span_ns: None,
            risk_collection_span_ns: None,
            recovery_elapsed_ms: None,
            issues: Vec::new(),
            buffered_messages: 0,
            buffered_bytes: 0,
            retained_facts: 0,
            duplicate_count: 0,
            conflict_count: 0,
        }
    }
}

pub(crate) struct PapiRecoveryBundle {
    pub(crate) account_state: AccountState,
    pub(crate) snapshot: BinancePapiReadOnlySnapshot,
    pub(crate) initial: bool,
}

pub(crate) type PapiRecoveryHandler =
    Arc<dyn Fn(PapiRecoveryBundle) -> anyhow::Result<()> + Send + Sync>;

/// A no-trading Portfolio Margin private account observation session.
pub struct BinancePapiAccountSession {
    config: BinancePapiReadOnlyConfig,
    reader: BinancePapiReadOnlyClient,
    symbols: Arc<BTreeSet<String>>,
    shared: Arc<SessionShared>,
    websocket: Arc<tokio::sync::Mutex<Option<WebSocketClient>>>,
    listen_key: Arc<Mutex<Option<OwnedListenKey>>>,
    tasks: TaskGroup,
    handler: PapiRecoveryHandler,
}

impl BinancePapiAccountSession {
    /// Constructs a private observation session without accessing the network.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration or instrument scope.
    pub fn new(
        config: &BinancePapiReadOnlyConfig,
        instruments: Vec<InstrumentAny>,
    ) -> anyhow::Result<Self> {
        Self::with_handler(config, instruments, Arc::new(|_| Ok(())))
    }

    pub(crate) fn with_handler(
        config: &BinancePapiReadOnlyConfig,
        instruments: Vec<InstrumentAny>,
        handler: PapiRecoveryHandler,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let symbols = instruments
            .iter()
            .map(|instrument| instrument.raw_symbol().to_string())
            .collect();
        let reader = BinancePapiReadOnlyClient::new(config, instruments)?;
        let evidence =
            BinancePapiRecoveryEvidence::stopped(config.account_id, reader.instrument_ids());

        Ok(Self {
            config: config.clone(),
            reader,
            symbols: Arc::new(symbols),
            shared: Arc::new(SessionShared {
                evidence: Mutex::new(evidence),
                facts: Mutex::new(FactState::new(config.max_rows)),
                queued_bytes: AtomicUsize::new(0),
                overflowed: AtomicBool::new(false),
                restricted: AtomicBool::new(false),
                next_owner: AtomicU64::new(0),
                current_owner: AtomicU64::new(0),
                pending_owner: AtomicU64::new(0),
            }),
            websocket: Arc::new(tokio::sync::Mutex::new(None)),
            listen_key: Arc::new(Mutex::new(None)),
            tasks: TaskGroup::new(),
            handler,
        })
    }

    /// Returns the shared read-only REST/report client used by this session.
    #[must_use]
    pub fn read_only_client(&self) -> BinancePapiReadOnlyClient {
        self.reader.clone()
    }

    /// Returns current typed recovery evidence with live queue counters.
    #[must_use]
    pub fn evidence(&self) -> BinancePapiRecoveryEvidence {
        self.shared.snapshot_evidence()
    }

    /// Returns whether the account-stream transport is currently active.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.shared.evidence.lock().transport_connected
    }

    /// Returns whether the declared recovery scope has converged and been applied.
    #[must_use]
    pub fn is_synchronized(&self) -> bool {
        self.shared.evidence.lock().synchronized
    }

    /// Starts a new listen-key owner, begins receiving, and performs bounded baseline recovery.
    ///
    /// # Errors
    ///
    /// Returns an error if transport setup, required REST sources, projection, or application fails.
    pub async fn start(&mut self) -> anyhow::Result<()> {
        if self.is_connected() && self.is_synchronized() && self.tasks.is_open() {
            return Ok(());
        }

        if self.tasks.is_open() && self.shared.current_owner.load(Ordering::Acquire) != 0 {
            self.begin_shutdown();
            self.finish_shutdown().await?;
        }

        if !self.tasks.is_open() {
            self.finish_shutdown().await?;
            self.tasks
                .start_generation()
                .map_err(|e| anyhow::anyhow!("Failed to start PAPI task generation: {e}"))?;
        }

        let cancel = self.tasks.cancellation_token();
        self.shared.restricted.store(false, Ordering::Release);
        self.shared.overflowed.store(false, Ordering::Release);
        self.shared.queued_bytes.store(0, Ordering::Release);
        {
            let mut evidence = self.shared.evidence.lock();
            evidence.session_generation = evidence
                .session_generation
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("PAPI session generation overflow"))?;
            evidence.state = BinancePapiSessionState::Connecting;
            evidence.transport_connected = false;
            evidence.synchronized = false;
            evidence.window_start = None;
            evidence.window_end = None;
            evidence.responses.clear();
            evidence.wallet_status = None;
            evidence.wallet_issues.clear();
            evidence.risk_status = None;
            evidence.risk_issues.clear();
            evidence.source_generations.clear();
            evidence.wallet_collection_span_ns = None;
            evidence.risk_collection_span_ns = None;
            evidence.recovery_elapsed_ms = None;
            evidence.issues.clear();
        }
        let owner = self.next_owner()?;
        let listen_key = match self.reader.create_listen_key().await {
            Ok(listen_key) => listen_key,
            Err(e) => return Err(self.cleanup_failed_start(e).await),
        };
        *self.listen_key.lock() = Some(OwnedListenKey {
            generation: owner,
            _key: listen_key.clone(),
        });
        let (tx, mut rx) = tokio::sync::mpsc::channel(self.config.max_websocket_buffer_messages);
        let websocket = match build_websocket(
            &self.config,
            &listen_key,
            owner,
            tx.clone(),
            Arc::clone(&self.shared),
            cancel.clone(),
        )
        .await
        {
            Ok(websocket) => websocket,
            Err(e) => return Err(self.cleanup_failed_start(e).await),
        };

        *self.websocket.lock().await = Some(websocket);
        self.shared.set_recovering();
        let driver = DriverContext {
            config: self.config.clone(),
            reader: self.reader.clone(),
            symbols: Arc::clone(&self.symbols),
            shared: Arc::clone(&self.shared),
            websocket: Arc::clone(&self.websocket),
            listen_key: Arc::clone(&self.listen_key),
            handler: Arc::clone(&self.handler),
            tx,
        };
        let initial = async {
            let mut owner = owner;

            for _ in 0..MAX_RECOVERY_ROUNDS {
                match recover(&driver, &mut rx, &cancel, true, owner).await? {
                    RecoveryOutcome::Synchronized => return Ok(()),
                    RecoveryOutcome::ReplaceListenKey => {
                        replace_listen_key(&driver, &cancel).await?;
                        owner = self.shared.current_owner.load(Ordering::Acquire);
                    }
                }
            }
            anyhow::bail!("PAPI listen key repeatedly expired during initial recovery")
        }
        .await;

        if let Err(e) = initial {
            return Err(self.cleanup_failed_start(e).await);
        }

        self.tasks
            .spawn_named(
                "papi-account-stream",
                run_driver(driver.clone(), rx, cancel.clone()),
            )
            .map_err(|e| anyhow::anyhow!("Failed to start PAPI account-stream driver: {e}"))?;
        self.tasks
            .spawn_named(
                "papi-listen-key-keepalive",
                run_keepalive(driver.clone(), cancel.clone()),
            )
            .map_err(|e| anyhow::anyhow!("Failed to start PAPI listen-key keepalive: {e}"))?;
        self.tasks
            .spawn_named("papi-transport-rotation", run_rotation(driver, cancel))
            .map_err(|e| anyhow::anyhow!("Failed to start PAPI transport rotation: {e}"))?;
        Ok(())
    }

    async fn cleanup_failed_start(&self, error: anyhow::Error) -> anyhow::Error {
        self.begin_shutdown();
        let cleanup = self.finish_shutdown().await;
        self.shared.restrict(error.to_string());

        match cleanup {
            Ok(()) => error,
            Err(cleanup) => anyhow::anyhow!("{error}; PAPI failed-start cleanup failed: {cleanup}"),
        }
    }

    /// Synchronously revokes synchronization and requests cancellation.
    pub fn begin_shutdown(&self) {
        {
            let mut evidence = self.shared.evidence.lock();
            evidence.state = BinancePapiSessionState::Stopping;
            evidence.transport_connected = false;
            evidence.synchronized = false;
        }
        self.tasks.begin_shutdown();
    }

    /// Completes bounded task, socket, and listen-key shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error when owned work cannot be drained inside the configured bounds.
    pub async fn stop(&mut self) -> anyhow::Result<()> {
        self.begin_shutdown();
        self.finish_shutdown().await
    }

    async fn finish_shutdown(&self) -> anyhow::Result<()> {
        if let Some(websocket) = self.websocket.lock().await.take() {
            websocket.disconnect().await;
        }

        if !self.tasks.is_open() {
            self.tasks
                .finish_shutdown(
                    Duration::from_secs(SHUTDOWN_GRACE_SECS),
                    Duration::from_secs(SHUTDOWN_ABORT_SECS),
                )
                .await
                .map_err(|e| anyhow::anyhow!("PAPI session shutdown failed: {e}"))?;
        }

        let owned = self.listen_key.lock().take();
        if let Some(owned) = owned
            && self.shared.current_owner.load(Ordering::Acquire) == owned.generation
        {
            // DELETE is account-global. It is only legal after the current owner has fully drained.
            if let Err(e) = self.reader.close_listen_key().await
                && !matches!(
                    e.downcast_ref::<PapiHttpError>(),
                    Some(PapiHttpError::ListenKeyExpired)
                )
            {
                self.shared
                    .restrict("PAPI listen-key close failed".to_string());
                return Err(e);
            }
        }

        self.shared.current_owner.store(0, Ordering::Release);
        self.shared.pending_owner.store(0, Ordering::Release);
        let mut evidence = self.shared.evidence.lock();
        evidence.state = BinancePapiSessionState::Stopped;
        evidence.transport_connected = false;
        evidence.synchronized = false;
        Ok(())
    }

    fn next_owner(&self) -> anyhow::Result<u64> {
        let owner = allocate_owner(&self.shared)?;
        self.shared.current_owner.store(owner, Ordering::Release);
        self.shared.evidence.lock().listen_key_generation = owner;
        Ok(owner)
    }
}

impl Debug for BinancePapiAccountSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(BinancePapiAccountSession))
            .field("evidence", &self.evidence())
            .finish_non_exhaustive()
    }
}

impl Drop for BinancePapiAccountSession {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

#[derive(Clone)]
struct DriverContext {
    config: BinancePapiReadOnlyConfig,
    reader: BinancePapiReadOnlyClient,
    symbols: Arc<BTreeSet<String>>,
    shared: Arc<SessionShared>,
    websocket: Arc<tokio::sync::Mutex<Option<WebSocketClient>>>,
    listen_key: Arc<Mutex<Option<OwnedListenKey>>>,
    handler: PapiRecoveryHandler,
    tx: tokio::sync::mpsc::Sender<Inbound>,
}

struct SessionShared {
    evidence: Mutex<BinancePapiRecoveryEvidence>,
    facts: Mutex<FactState>,
    queued_bytes: AtomicUsize,
    overflowed: AtomicBool,
    restricted: AtomicBool,
    next_owner: AtomicU64,
    current_owner: AtomicU64,
    pending_owner: AtomicU64,
}

impl SessionShared {
    fn accepts_owner(&self, owner: u64) -> bool {
        self.current_owner.load(Ordering::Acquire) == owner
            || self.pending_owner.load(Ordering::Acquire) == owner
    }

    fn snapshot_evidence(&self) -> BinancePapiRecoveryEvidence {
        let facts = self.facts.lock();
        let mut evidence = self.evidence.lock().clone();
        evidence.buffered_bytes = self.queued_bytes.load(Ordering::Acquire);
        evidence.retained_facts = facts.fact_count();
        evidence.duplicate_count = facts.duplicate_count;
        evidence.conflict_count = facts.conflict_count;
        evidence
    }

    fn set_recovering(&self) {
        if self.restricted.load(Ordering::Acquire) {
            return;
        }

        let mut evidence = self.evidence.lock();
        evidence.state = BinancePapiSessionState::Recovering;
        evidence.synchronized = false;
    }

    fn restrict(&self, issue: String) {
        self.restricted.store(true, Ordering::Release);
        let mut evidence = self.evidence.lock();
        evidence.state = BinancePapiSessionState::Restricted;
        evidence.synchronized = false;

        if !evidence.issues.contains(&issue) {
            evidence.issues.push(issue);
        }
    }
}

struct OwnedListenKey {
    generation: u64,
    _key: ListenKey,
}

enum Inbound {
    Frame {
        owner: u64,
        epoch: u64,
        bytes: Vec<u8>,
    },
    Transport {
        owner: u64,
        state: SocketState,
    },
    ReplaceListenKey {
        owner: u64,
    },
}

async fn build_websocket(
    config: &BinancePapiReadOnlyConfig,
    listen_key: &ListenKey,
    owner: u64,
    tx: tokio::sync::mpsc::Sender<Inbound>,
    shared: Arc<SessionShared>,
    cancel: CancellationToken,
) -> anyhow::Result<WebSocketClient> {
    let url = format!(
        "{}/{}",
        config.websocket_url.expose_secret().trim_end_matches('/'),
        listen_key.expose_secret(),
    );
    let websocket_config = WebSocketConfig {
        url,
        headers: Vec::new(),
        heartbeat_interval_secs: Some(180),
        heartbeat_payload: None,
        connect_timeout_ms: Some(u64::try_from(config.request_timeout.as_millis())?),
        reconnect_delay_initial_ms: Some(500),
        reconnect_delay_max_ms: Some(10_000),
        reconnect_backoff_factor: Some(2.0),
        reconnect_jitter_ms: Some(100),
        reconnect_max_attempts: Some(8),
        heartbeat_timeout_secs: Some(600),
        idle_timeout_ms: None,
        backend: if config.websocket_url.expose_secret().starts_with("ws://") {
            TransportBackend::Tungstenite
        } else {
            TransportBackend::default()
        },
        proxy_url: config
            .proxy_url
            .as_ref()
            .map(|url| url.expose_secret().to_owned()),
    };
    let frame_tx = tx.clone();
    let frame_shared = Arc::clone(&shared);
    let max_message_bytes = config.max_websocket_message_bytes;
    let max_buffer_bytes = config.max_websocket_buffer_bytes;
    let epoch_handler: EpochMessageHandler = Arc::new(move |epoch, message| {
        if !frame_shared.accepts_owner(owner) {
            return;
        }
        let bytes = match message {
            Message::Text(bytes) | Message::Binary(bytes) => bytes.to_vec(),
            Message::Ping(_) | Message::Pong(_) | Message::Close(_) => return,
        };

        if bytes.as_slice() == RECONNECTED.as_bytes() {
            let _ = frame_tx.try_send(Inbound::Transport {
                owner,
                state: SocketState::Connected,
            });
            return;
        }

        if bytes.len() > max_message_bytes
            || frame_shared
                .queued_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |queued| {
                    queued
                        .checked_add(bytes.len())
                        .filter(|total| *total <= max_buffer_bytes)
                })
                .is_err()
        {
            frame_shared.overflowed.store(true, Ordering::Release);
            frame_shared.restrict("PAPI account-stream buffer overflow".to_string());
            return;
        }

        if frame_tx
            .try_send(Inbound::Frame {
                owner,
                epoch,
                bytes: bytes.clone(),
            })
            .is_err()
        {
            frame_shared
                .queued_bytes
                .fetch_sub(bytes.len(), Ordering::AcqRel);
            frame_shared.overflowed.store(true, Ordering::Release);
            frame_shared.restrict("PAPI account-stream buffer overflow".to_string());
        }
    });
    let state_shared = Arc::clone(&shared);
    let state_tx = tx;
    let state_sink = SocketStateSink::new(move |state| {
        if !state_shared.accepts_owner(owner) {
            return;
        }
        {
            let mut evidence = state_shared.evidence.lock();
            evidence.transport_connected = state == SocketState::Connected;
            evidence.synchronized = false;

            if !state_shared.restricted.load(Ordering::Acquire) {
                evidence.state = BinancePapiSessionState::Recovering;
            }
        }

        if state_tx
            .try_send(Inbound::Transport { owner, state })
            .is_err()
        {
            state_shared.overflowed.store(true, Ordering::Release);
            state_shared.restrict("PAPI account-stream buffer overflow".to_string());
        }
    });

    WebSocketClient::epoch_builder()
        .config(websocket_config)
        .epoch_handler(epoch_handler)
        .state_sink(state_sink)
        .cancellation_token(cancel)
        .connect()
        .await
        .map_err(|_| anyhow::anyhow!("Failed to connect PAPI private WebSocket"))
}

async fn run_driver(
    context: DriverContext,
    mut rx: tokio::sync::mpsc::Receiver<Inbound>,
    cancel: CancellationToken,
) {
    loop {
        let inbound = tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            inbound = rx.recv() => inbound,
        };
        let Some(inbound) = inbound else {
            break;
        };

        match process_inbound(&context.shared, &context.symbols, inbound) {
            Ok(InboundAction::Ignore) => continue,
            Ok(InboundAction::ReplaceListenKey) => {
                if let Err(e) = replace_listen_key(&context, &cancel).await {
                    context.shared.restrict(e.to_string());
                    continue;
                }
            }
            Ok(InboundAction::Recover) => {}
            Err(e) => {
                context.shared.restrict(e);
                continue;
            }
        }

        tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            () = nautilus_common::live::dst::time::sleep(context.config.refresh_debounce) => {}
        }

        let owner = context.shared.current_owner.load(Ordering::Acquire);
        let result = recover(&context, &mut rx, &cancel, false, owner).await;

        match result {
            Ok(RecoveryOutcome::Synchronized) => {}
            Ok(RecoveryOutcome::ReplaceListenKey) => {
                if let Err(e) = replace_listen_key(&context, &cancel).await {
                    context.shared.restrict(e.to_string());
                }
            }
            Err(e) => context.shared.restrict(e.to_string()),
        }
    }
}

async fn run_keepalive(context: DriverContext, cancel: CancellationToken) {
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            () = nautilus_common::live::dst::time::sleep(
                context.config.listen_key_keepalive_interval,
            ) => {}
        }

        if context.shared.current_owner.load(Ordering::Acquire) == 0 {
            continue;
        }

        if let Err(e) = context.reader.keepalive_listen_key().await {
            let owner = context.shared.current_owner.load(Ordering::Acquire);

            if matches!(
                e.downcast_ref::<PapiHttpError>(),
                Some(PapiHttpError::ListenKeyExpired)
            ) {
                let _ = context.tx.send(Inbound::ReplaceListenKey { owner }).await;
            } else {
                context
                    .shared
                    .restrict("PAPI listen-key keepalive failed".to_string());
            }
        }
    }
}

async fn run_rotation(context: DriverContext, cancel: CancellationToken) {
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            () = nautilus_common::live::dst::time::sleep(
                context.config.transport_rotation_interval,
            ) => {}
        }

        let accepted = context
            .websocket
            .lock()
            .await
            .as_ref()
            .is_some_and(WebSocketClient::request_reconnect);

        if !accepted {
            context
                .shared
                .restrict("PAPI planned transport rotation was not accepted".to_string());
        }
    }
}

async fn replace_listen_key(
    context: &DriverContext,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let old_owner = context.shared.current_owner.load(Ordering::Acquire);
    let new_owner = allocate_owner(&context.shared)?;
    context
        .shared
        .pending_owner
        .store(new_owner, Ordering::Release);
    let replacement = async {
        anyhow::ensure!(!cancel.is_cancelled(), "PAPI session is stopping");
        let listen_key = context.reader.create_listen_key().await?;
        anyhow::ensure!(
            !cancel.is_cancelled()
                && context.shared.current_owner.load(Ordering::Acquire) == old_owner,
            "PAPI listen-key replacement became stale"
        );
        let websocket = build_websocket(
            &context.config,
            &listen_key,
            new_owner,
            context.tx.clone(),
            Arc::clone(&context.shared),
            cancel.child_token(),
        )
        .await?;
        Ok::<_, anyhow::Error>((listen_key, websocket))
    }
    .await;
    let (listen_key, replacement) = match replacement {
        Ok(replacement) => replacement,
        Err(e) => {
            context.shared.pending_owner.store(0, Ordering::Release);
            return Err(e);
        }
    };

    if cancel.is_cancelled() || context.shared.current_owner.load(Ordering::Acquire) != old_owner {
        context.shared.pending_owner.store(0, Ordering::Release);
        replacement.disconnect().await;
        anyhow::bail!("PAPI listen-key replacement became stale");
    }

    context
        .shared
        .current_owner
        .store(new_owner, Ordering::Release);
    context.shared.pending_owner.store(0, Ordering::Release);
    let old_websocket = context.websocket.lock().await.replace(replacement);
    *context.listen_key.lock() = Some(OwnedListenKey {
        generation: new_owner,
        _key: listen_key,
    });
    context.shared.evidence.lock().listen_key_generation = new_owner;

    if let Some(old_websocket) = old_websocket {
        old_websocket.disconnect().await;
    }
    Ok(())
}

fn allocate_owner(shared: &SessionShared) -> anyhow::Result<u64> {
    let previous = shared
        .next_owner
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |owner| {
            owner.checked_add(1)
        })
        .map_err(|_| anyhow::anyhow!("PAPI listen-key generation overflow"))?;
    let owner = previous + 1;
    Ok(owner)
}

async fn recover(
    context: &DriverContext,
    rx: &mut tokio::sync::mpsc::Receiver<Inbound>,
    cancel: &CancellationToken,
    initial: bool,
    owner: u64,
) -> anyhow::Result<RecoveryOutcome> {
    let config = &context.config;
    let reader = &context.reader;
    let symbols = &context.symbols;
    let shared = &context.shared;
    anyhow::ensure!(!cancel.is_cancelled(), "PAPI session is stopping");
    anyhow::ensure!(
        !shared.restricted.load(Ordering::Acquire),
        "PAPI session has an unresolved restriction"
    );
    shared.set_recovering();
    let recovery_started = Instant::now();
    let window_end = reader.now();
    let lookback_ns = u64::try_from(config.recovery_lookback.as_nanos())?;
    let window_start = window_end
        .as_u64()
        .checked_sub(lookback_ns)
        .ok_or_else(|| anyhow::anyhow!("PAPI recovery lookback exceeds timestamp bounds"))?
        .into();
    let recovery_generation = {
        let mut evidence = shared.evidence.lock();
        evidence.recovery_generation = evidence
            .recovery_generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("PAPI recovery generation overflow"))?;
        evidence.window_start = Some(window_start);
        evidence.window_end = Some(window_end);
        evidence.recovery_generation
    };
    let mut stable_result = None;

    for round in 0..MAX_RECOVERY_ROUNDS {
        shared.facts.lock().clear_dirty_sources();
        reader.refresh_account_observations().await?;
        let projection =
            reader.account_projection(config.operation_timeout, config.operation_timeout)?;
        let snapshot = reader
            .generate_mass_status(window_start, window_end)
            .await?;

        while let Ok(inbound) = rx.try_recv() {
            match process_inbound(shared, symbols, inbound).map_err(anyhow::Error::msg)? {
                InboundAction::ReplaceListenKey => {
                    return Ok(RecoveryOutcome::ReplaceListenKey);
                }
                InboundAction::Recover | InboundAction::Ignore => {}
            }
        }

        if shared.overflowed.load(Ordering::Acquire) {
            anyhow::bail!("PAPI account-stream buffer overflow")
        }

        let dirty = shared.facts.lock().has_dirty_sources();

        if !dirty {
            stable_result = Some((projection, snapshot));
            break;
        }

        if round + 1 == MAX_RECOVERY_ROUNDS {
            anyhow::bail!("PAPI recovery did not converge within the recheck bound")
        }
    }

    let (projection, mut snapshot) =
        stable_result.ok_or_else(|| anyhow::anyhow!("PAPI recovery produced no stable result"))?;
    validate_projection(&projection)?;
    anyhow::ensure!(
        snapshot.issues.as_slice() == [KNOWN_HISTORY_LIMITATION],
        "PAPI historical recovery has unresolved source failures"
    );
    let account_state = projection
        .account_state
        .ok_or_else(|| anyhow::anyhow!("PAPI wallet projection produced no account state"))?;
    snapshot.mass_status.client_id = *crate::consts::BINANCE_PAPI_CLIENT_ID;
    let fact_version = shared.facts.lock().fact_version;

    {
        let evidence = shared.evidence.lock();
        anyhow::ensure!(!cancel.is_cancelled(), "PAPI session is stopping");
        anyhow::ensure!(
            !shared.restricted.load(Ordering::Acquire)
                && !shared.overflowed.load(Ordering::Acquire),
            "PAPI session has an unresolved restriction"
        );
        anyhow::ensure!(
            evidence.recovery_generation == recovery_generation
                && evidence.session_generation != 0
                && shared.current_owner.load(Ordering::Acquire) == owner,
            "Stale PAPI recovery result"
        );
    }

    (context.handler)(PapiRecoveryBundle {
        account_state,
        snapshot: snapshot.clone(),
        initial,
    })?;

    let transport_epoch = if shared.current_owner.load(Ordering::Acquire) == owner {
        shared.evidence.lock().transport_epoch
    } else {
        0
    };
    let facts = shared.facts.lock();
    let mut evidence = shared.evidence.lock();

    if cancel.is_cancelled()
        || evidence.recovery_generation != recovery_generation
        || evidence.session_generation == 0
        || shared.current_owner.load(Ordering::Acquire) != owner
    {
        anyhow::bail!("Stale PAPI recovery result")
    }

    evidence.state = BinancePapiSessionState::Synchronized;
    evidence.synchronized = true;
    evidence.trading_authorized = false;
    evidence.applied_fact_version = fact_version;
    evidence.transport_epoch = transport_epoch;
    evidence.responses = snapshot.responses;
    evidence.wallet_status = Some(projection.wallet_status);
    evidence.wallet_issues = projection.wallet_issues;
    evidence.risk_status = Some(projection.risk_status);
    evidence.risk_issues = projection.risk_issues;
    evidence.source_generations = projection.source_generations;
    evidence.wallet_collection_span_ns = projection.wallet_collection_span_ns;
    evidence.risk_collection_span_ns = projection.risk_collection_span_ns;
    evidence.recovery_elapsed_ms = Some(recovery_started.elapsed().as_millis());
    evidence.issues = snapshot.issues;
    evidence.buffered_messages = rx.len();
    evidence.buffered_bytes = shared.queued_bytes.load(Ordering::Acquire);
    evidence.retained_facts = facts.fact_count();
    evidence.duplicate_count = facts.duplicate_count;
    evidence.conflict_count = facts.conflict_count;
    Ok(RecoveryOutcome::Synchronized)
}

enum RecoveryOutcome {
    Synchronized,
    ReplaceListenKey,
}

fn validate_projection(projection: &BinancePapiAccountProjection) -> anyhow::Result<()> {
    anyhow::ensure!(
        projection.wallet_status == BinancePapiProjectionStatus::Available,
        "PAPI wallet projection is unavailable"
    );
    anyhow::ensure!(
        projection.risk_status == BinancePapiProjectionStatus::Available,
        "PAPI risk projection is unavailable"
    );
    anyhow::ensure!(
        !projection.trading_authorized,
        "PAPI issue #4 projection cannot authorize trading"
    );
    Ok(())
}

enum InboundAction {
    Ignore,
    Recover,
    ReplaceListenKey,
}

fn process_inbound(
    shared: &SessionShared,
    symbols: &BTreeSet<String>,
    inbound: Inbound,
) -> Result<InboundAction, String> {
    match inbound {
        Inbound::Frame {
            owner,
            epoch,
            bytes,
        } => {
            shared.queued_bytes.fetch_sub(bytes.len(), Ordering::AcqRel);

            if owner != shared.current_owner.load(Ordering::Acquire) {
                return Ok(InboundAction::Ignore);
            }
            shared.evidence.lock().transport_epoch = epoch;
            let event = parse_event(&bytes).map_err(|e| e.to_string())?;

            if !event_in_scope(&event, symbols) {
                return Err("PAPI account-stream event is outside declared instruments".to_string());
            }

            if matches!(event, PapiWsEvent::ListenKeyExpired { .. }) {
                return Ok(InboundAction::ReplaceListenKey);
            }
            shared.facts.lock().apply(event)?;
            Ok(InboundAction::Recover)
        }
        Inbound::Transport { owner, state } => {
            if owner != shared.current_owner.load(Ordering::Acquire) {
                return Ok(InboundAction::Ignore);
            }

            if state == SocketState::Disconnected {
                shared.set_recovering();
                return Ok(InboundAction::Ignore);
            }
            Ok(InboundAction::Recover)
        }
        Inbound::ReplaceListenKey { owner } => {
            if owner == shared.current_owner.load(Ordering::Acquire) {
                Ok(InboundAction::ReplaceListenKey)
            } else {
                Ok(InboundAction::Ignore)
            }
        }
    }
}

fn event_in_scope(event: &PapiWsEvent, symbols: &BTreeSet<String>) -> bool {
    match event {
        PapiWsEvent::Order(order) => symbols.contains(&order.symbol),
        PapiWsEvent::Algo(algo) => symbols.contains(&algo.symbol),
        PapiWsEvent::Account(account) => account
            .positions
            .iter()
            .all(|position| symbols.contains(&position.symbol)),
        PapiWsEvent::Dirty(_) | PapiWsEvent::ListenKeyExpired { .. } => true,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use futures_util::{SinkExt, StreamExt};
    use nautilus_core::string::secret::SecretString;
    use rstest::rstest;
    use serde_json::json;
    use tokio_tungstenite::{accept_async, tungstenite::Message as WsMessage};

    use super::*;
    use crate::testing::{self, MockServer, Reply};

    async fn websocket_server(
        first_event: String,
    ) -> (
        String,
        tokio::sync::mpsc::UnboundedSender<String>,
        tokio::task::JoinHandle<()>,
        Arc<AtomicBool>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/ws", listener.local_addr().unwrap());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let connected = Arc::new(AtomicBool::new(false));
        let server_connected = Arc::clone(&connected);
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = accept_async(stream).await.unwrap();
            server_connected.store(true, Ordering::Release);
            websocket
                .send(WsMessage::Text(first_event.into()))
                .await
                .unwrap();

            loop {
                tokio::select! {
                    command = rx.recv() => {
                        let Some(command) = command else { break };
                        if websocket.send(WsMessage::Text(command.into())).await.is_err() {
                            break;
                        }
                    }
                    message = websocket.next() => {
                        match message {
                            Some(Ok(WsMessage::Close(_)) | Err(_)) | None => break,
                            _ => {}
                        }
                    }
                }
            }
        });
        (url, tx, task, connected)
    }

    fn partial_account_event() -> String {
        json!({
            "e": "ACCOUNT_UPDATE",
            "E": 1_700_000_000_000_i64,
            "T": 1_700_000_000_001_i64,
            "fs": "UM",
            "a": {
                "m": "ORDER",
                "P": [{"s": "BTCUSDT", "pa": "-2.00000000", "ps": "BOTH"}]
            }
        })
        .to_string()
    }

    #[rstest]
    fn owner_generations_are_not_reused_after_becoming_inactive() {
        let config = testing::config("http://127.0.0.1:1");
        let session =
            BinancePapiAccountSession::new(&config, vec![testing::instrument("BTCUSDT")]).unwrap();
        assert_eq!(session.next_owner().unwrap(), 1);
        session.shared.current_owner.store(0, Ordering::Release);
        assert_eq!(session.next_owner().unwrap(), 2);
    }

    #[rstest]
    fn pending_owner_keeps_both_transports_receivable_until_cutover() {
        let config = testing::config("http://127.0.0.1:1");
        let session =
            BinancePapiAccountSession::new(&config, vec![testing::instrument("BTCUSDT")]).unwrap();
        let current = session.next_owner().unwrap();
        let pending = allocate_owner(&session.shared).unwrap();
        session
            .shared
            .pending_owner
            .store(pending, Ordering::Release);

        assert!(session.shared.accepts_owner(current));
        assert!(session.shared.accepts_owner(pending));

        session
            .shared
            .current_owner
            .store(pending, Ordering::Release);
        session.shared.pending_owner.store(0, Ordering::Release);
        assert!(!session.shared.accepts_owner(current));
        assert!(session.shared.accepts_owner(pending));
    }

    #[rstest]
    #[tokio::test]
    async fn session_receives_before_baseline_and_restricts_unknown_events() {
        let (websocket_url, websocket_tx, websocket_task, websocket_ready) =
            websocket_server(partial_account_event()).await;
        let baseline_before_websocket = Arc::new(AtomicBool::new(false));
        let ready = Arc::clone(&websocket_ready);
        let violated = Arc::clone(&baseline_before_websocket);
        let server = MockServer::new(move |request| {
            if request.path != "/papi/v1/listenKey" && !ready.load(Ordering::Acquire) {
                violated.store(true, Ordering::Release);
            }

            if request.path == "/papi/v1/balance" {
                Reply::json(&testing::supported_balances())
            } else {
                testing::quiet(request)
            }
        })
        .await;
        let mut config = testing::config(&server.url);
        config.websocket_url = SecretString::from(websocket_url);
        config.listen_key_keepalive_interval = Duration::from_secs(10);
        config.transport_rotation_interval = Duration::from_secs(10);
        config.refresh_debounce = Duration::from_millis(10);
        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_observed = Arc::clone(&callback_count);
        let handler: PapiRecoveryHandler = Arc::new(move |bundle| {
            assert!(bundle.initial);
            assert!(!bundle.snapshot.mass_status.reports_complete());
            assert!(!bundle.account_state.total_only_balances.is_empty());
            callback_observed.fetch_add(1, Ordering::AcqRel);
            Ok(())
        });
        let mut session = BinancePapiAccountSession::with_handler(
            &config,
            vec![testing::instrument("BTCUSDT")],
            handler,
        )
        .unwrap();

        session.start().await.unwrap();
        let evidence = session.evidence();
        assert_eq!(evidence.state, BinancePapiSessionState::Synchronized);
        assert!(evidence.transport_connected);
        assert!(evidence.synchronized);
        assert!(!evidence.trading_authorized);
        assert_eq!(evidence.applied_fact_version, 1);
        assert_eq!(callback_count.load(Ordering::Acquire), 1);
        assert!(!baseline_before_websocket.load(Ordering::Acquire));

        websocket_tx
            .send(json!({"e": "UNKNOWN_CRITICAL", "E": 1_700_000_000_002_i64}).to_string())
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if session.evidence().state == BinancePapiSessionState::Restricted {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(!session.evidence().synchronized);
        websocket_tx.send(partial_account_event()).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            session.evidence().state,
            BinancePapiSessionState::Restricted
        );

        session.stop().await.unwrap();
        assert_eq!(session.evidence().state, BinancePapiSessionState::Stopped);
        let requests = server.requests();
        assert!(
            requests.iter().any(|request| {
                request.path == "/papi/v1/listenKey" && request.method == "DELETE"
            })
        );
        websocket_task.abort();
    }

    #[rstest]
    #[tokio::test]
    async fn listen_key_expiry_during_recovery_replaces_owner_without_early_delete() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let websocket_url = format!("ws://{}/ws", listener.local_addr().unwrap());
        let websocket_task = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            let mut first = accept_async(first).await.unwrap();
            first
                .send(WsMessage::Text(
                    json!({"e": "listenKeyExpired", "E": 1_700_000_000_000_i64})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            let (second, _) = listener.accept().await.unwrap();
            let second = accept_async(second).await.unwrap();
            std::future::pending::<()>().await;
            drop((first, second));
        });
        let server = MockServer::new(|request| {
            if request.path == "/papi/v1/balance" {
                Reply::json(&testing::supported_balances())
            } else {
                testing::quiet(request)
            }
        })
        .await;
        let mut config = testing::config(&server.url);
        config.websocket_url = SecretString::from(websocket_url);
        let mut session =
            BinancePapiAccountSession::new(&config, vec![testing::instrument("BTCUSDT")]).unwrap();

        session.start().await.unwrap();
        let evidence = session.evidence();
        assert_eq!(evidence.state, BinancePapiSessionState::Synchronized);
        assert_eq!(evidence.listen_key_generation, 2);
        let requests = server.requests();
        assert_eq!(
            requests
                .iter()
                .filter(|request| {
                    request.path == "/papi/v1/listenKey" && request.method == "POST"
                })
                .count(),
            2
        );
        assert!(!requests.iter().any(|request| request.method == "DELETE"));

        session.stop().await.unwrap();
        assert_eq!(
            server
                .requests()
                .iter()
                .filter(|request| request.method == "DELETE")
                .count(),
            1
        );
        websocket_task.abort();
    }

    #[rstest]
    #[tokio::test]
    async fn handshake_failure_closes_the_created_listen_key() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let websocket_url = format!("ws://{}/ws", listener.local_addr().unwrap());
        let websocket_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            drop(stream);
        });
        let server = MockServer::new(testing::quiet).await;
        let mut config = testing::config(&server.url);
        config.websocket_url = SecretString::from(websocket_url);
        let mut session =
            BinancePapiAccountSession::new(&config, vec![testing::instrument("BTCUSDT")]).unwrap();

        assert!(session.start().await.is_err());
        assert_eq!(
            session.evidence().state,
            BinancePapiSessionState::Restricted
        );
        let requests = server.requests();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method == "POST")
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method == "DELETE")
                .count(),
            1
        );
        websocket_task.abort();
    }
}
