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

//! Account-level admission, reservation, and restart-recovery coordination.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use nautilus_binance::common::symbol::format_binance_symbol;
use nautilus_common::messages::execution::CancelAllOrders;
use nautilus_core::{UUID4, UnixNanos};
use nautilus_live::execution::failure::CommandFailure;
use nautilus_model::{
    enums::{OrderSide, OrderStatus, OrderType, TimeInForce},
    identifiers::{AccountId, ClientOrderId, InstrumentId, StrategyId},
    reports::OrderStatusReport,
    types::Currency,
};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::journal::{
    JournalError, PapiCommandJournal, PapiIntentSide, PapiIntentTimeInForce,
    PapiOperationResolution, PapiOperationStage, PapiPersistedCommand, PapiPersistedOperation,
    PapiRecoveredOperation, PapiReservation, PapiSubmitIntent, PapiUnknownReason,
};
use crate::{
    config::{BinancePapiInstrumentTradingConfig, BinancePapiTradingConfig},
    http::{
        PapiCommandDispatchError, PapiCommandResponse, PapiHttpClient, RequestBudget,
        command::{
            CancelUmOrderRequest, PapiCommandAcknowledgement, PapiCommandBuildError,
            PapiCommandFailure, PapiUmOrderSide, PapiUmTimeInForce, SubmitUmOrderRequest,
        },
        error::PapiHttpError,
    },
    read_only::BinancePapiReadOnlyClient,
};

const BASIS_POINTS: u32 = 10_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PapiAccountStatusEvidence {
    Normal {
        endpoint: &'static str,
        generation: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PapiPositionModeEvidence {
    OneWay {
        endpoint: &'static str,
        generation: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PapiRiskUnitEvidence {
    pub(crate) currency: Currency,
    pub(crate) source: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PapiMarginRuleEvidence {
    pub(crate) source: &'static str,
    pub(crate) generation: u64,
    pub(crate) max_initial_margin_rate: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PapiVerifiedOpenOrder {
    pub(crate) instrument_id: InstrumentId,
    pub(crate) client_order_id: ClientOrderId,
    pub(crate) venue_order_id: i64,
    pub(crate) side: PapiIntentSide,
    pub(crate) quantity: Decimal,
    pub(crate) reduce_only: bool,
    pub(crate) worst_case_exposure: Decimal,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PapiVerifiedInstrumentRisk {
    pub(crate) signed_position_quantity: Decimal,
    pub(crate) exposure: Decimal,
    pub(crate) reference_price: Decimal,
    pub(crate) price_source: &'static str,
    pub(crate) price_generation: u64,
    pub(crate) price_observed_at: Instant,
    pub(crate) rules: PapiVerifiedInstrumentRules,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PapiVerifiedInstrumentRules {
    pub(crate) source: &'static str,
    pub(crate) generation: u64,
    pub(crate) settlement_currency: Currency,
    pub(crate) trading: bool,
    pub(crate) price_increment: Decimal,
    pub(crate) quantity_increment: Decimal,
    pub(crate) min_price: Option<Decimal>,
    pub(crate) max_price: Option<Decimal>,
    pub(crate) min_quantity: Option<Decimal>,
    pub(crate) max_quantity: Option<Decimal>,
    pub(crate) min_notional: Option<Decimal>,
    pub(crate) max_notional: Option<Decimal>,
}

#[derive(Clone, Debug)]
pub(crate) struct PapiVerifiedRiskSnapshot {
    pub(crate) account_id: AccountId,
    pub(crate) generation: u64,
    pub(crate) observed_at: Instant,
    pub(crate) collection_span: Duration,
    pub(crate) status: PapiAccountStatusEvidence,
    pub(crate) position_mode: PapiPositionModeEvidence,
    pub(crate) units: PapiRiskUnitEvidence,
    pub(crate) margin_rule: PapiMarginRuleEvidence,
    pub(crate) available_initial_margin: Decimal,
    pub(crate) account_exposure: Decimal,
    pub(crate) instruments: HashMap<InstrumentId, PapiVerifiedInstrumentRisk>,
    pub(crate) open_orders: Vec<PapiVerifiedOpenOrder>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PapiCommandPermissions {
    pub(crate) increase_risk: bool,
    pub(crate) verified_reduce_only: bool,
    pub(crate) targeted_cancel: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PapiRecoverySummary {
    pub(crate) rounds: u32,
    pub(crate) reports_applied: usize,
    pub(crate) unresolved: usize,
    pub(crate) budget_exhausted: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PapiCancelPreparation {
    LocalSubmitCanceled { submit_operation_id: UUID4 },
    Prepared { cancel_operation_id: UUID4 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PapiRebaselineToken {
    id: UUID4,
    starting_evidence_generation: u64,
    starting_applied_fact_version: u64,
}

#[derive(Debug)]
pub(crate) struct PapiCommandCoordinator {
    config: BinancePapiTradingConfig,
    journal: PapiCommandJournal,
    evidence: Option<PapiVerifiedRiskSnapshot>,
    recovered_unresolved: HashSet<UUID4>,
    submit_client_order_ids: HashSet<ClientOrderId>,
    uncertain: bool,
    rebaseline: Option<PapiRebaselineToken>,
    session: Option<crate::websocket::PapiApplicationAcknowledger>,
}

impl PapiCommandCoordinator {
    pub(crate) fn open(
        config: BinancePapiTradingConfig,
        account_id: AccountId,
    ) -> Result<Self, CoordinatorError> {
        config
            .validate()
            .map_err(|e| CoordinatorError::InvalidConfig(e.to_string()))?;
        let journal = PapiCommandJournal::open(&config.command_journal_path, account_id)?;
        let recovered_unresolved = journal
            .unresolved()
            .map(|operation| operation.operation.operation_id)
            .collect();
        let submit_client_order_ids = journal
            .operations()
            .values()
            .filter_map(|operation| {
                matches!(operation.operation.command, PapiPersistedCommand::Submit(_))
                    .then_some(operation.operation.client_order_id)
            })
            .collect();
        let uncertain = journal
            .operations()
            .values()
            .any(|operation| matches!(operation.stage, PapiOperationStage::Unknown { .. }));

        Ok(Self {
            config,
            journal,
            evidence: None,
            recovered_unresolved,
            submit_client_order_ids,
            uncertain,
            rebaseline: None,
            session: None,
        })
    }

    pub(crate) fn bind_session(&mut self, session: crate::websocket::PapiApplicationAcknowledger) {
        self.session = Some(session);
    }

    fn session_allows_increase_risk(&self) -> bool {
        self.session
            .as_ref()
            .is_none_or(|session| session.increase_risk_allowed())
    }

    pub(crate) fn install_evidence(
        &mut self,
        evidence: PapiVerifiedRiskSnapshot,
        now: Instant,
    ) -> Result<(), CoordinatorError> {
        self.validate_evidence(&evidence, now)?;
        self.validate_evidence_replacement(&evidence)?;
        self.evidence = Some(evidence);
        Ok(())
    }

    pub(crate) fn permissions(&self, now: Instant) -> PapiCommandPermissions {
        let evidence_current = self
            .evidence
            .as_ref()
            .is_some_and(|evidence| self.validate_evidence(evidence, now).is_ok());
        let recovery_restricted =
            !self.recovered_unresolved.is_empty() || self.uncertain || self.rebaseline.is_some();

        PapiCommandPermissions {
            increase_risk: evidence_current
                && !recovery_restricted
                && self.session_allows_increase_risk(),
            verified_reduce_only: evidence_current
                && self.evidence.as_ref().is_some_and(|evidence| {
                    evidence
                        .instruments
                        .values()
                        .any(|risk| !risk.signed_position_quantity.is_zero())
                }),
            targeted_cancel: self.has_cancelable_target(),
        }
    }

    pub(crate) fn covers_order_account_update(
        &self,
        reason: &str,
        symbols: &BTreeSet<String>,
    ) -> bool {
        if reason != "ORDER" {
            return false;
        }
        let Some(evidence) = self.evidence.as_ref() else {
            return false;
        };
        let covered_symbols: HashSet<_> = self
            .journal
            .operations()
            .values()
            .filter(|operation| risk_reservation_active(operation, evidence.generation))
            .filter(|operation| {
                matches!(operation.operation.command, PapiPersistedCommand::Submit(_))
            })
            .map(|operation| format_binance_symbol(&operation.operation.instrument_id))
            .collect();

        !covered_symbols.is_empty()
            && (symbols.is_empty()
                || symbols
                    .iter()
                    .all(|symbol| covered_symbols.contains(symbol)))
    }

    /// Freezes increase-risk admission and starts a generation-bound rebaseline attempt.
    pub(crate) fn begin_rebaseline(
        &mut self,
        applied_fact_version: u64,
        _now: Instant,
    ) -> Result<PapiRebaselineToken, CoordinatorError> {
        if self.rebaseline.is_some() {
            return Err(CoordinatorError::RebaselineInProgress);
        }

        let evidence_generation = self
            .evidence
            .as_ref()
            .ok_or(CoordinatorError::MissingEvidence)?
            .generation;

        let token = PapiRebaselineToken {
            id: UUID4::new(),
            starting_evidence_generation: evidence_generation,
            starting_applied_fact_version: applied_fact_version,
        };
        self.rebaseline = Some(token);
        Ok(token)
    }

    /// Installs a reconciled risk generation and durably ends only its covered reservations.
    pub(crate) fn install_rebaseline(
        &mut self,
        token: PapiRebaselineToken,
        evidence: PapiVerifiedRiskSnapshot,
        applied_fact_version: u64,
        now: Instant,
        ts_event: UnixNanos,
    ) -> Result<(), CoordinatorError> {
        if self.rebaseline != Some(token) {
            return Err(CoordinatorError::RebaselineToken);
        }

        if evidence.generation <= token.starting_evidence_generation
            || applied_fact_version == 0
            || applied_fact_version < token.starting_applied_fact_version
        {
            return Err(CoordinatorError::RebaselineGeneration);
        }

        self.validate_evidence(&evidence, now)?;
        self.validate_evidence_replacement(&evidence)?;
        self.validate_rebaseline_hard_limits(&evidence)?;
        let operation_ids = self.validate_rebaseline_coverage(&evidence)?;

        if !operation_ids.is_empty() {
            self.journal.rebaseline(
                &operation_ids,
                evidence.generation,
                applied_fact_version,
                ts_event,
            )?;
        }

        for operation_id in &operation_ids {
            self.recovered_unresolved.remove(operation_id);
        }
        self.evidence = Some(evidence);
        self.uncertain = self
            .journal
            .operations()
            .values()
            .any(|operation| matches!(operation.stage, PapiOperationStage::Unknown { .. }));
        self.rebaseline = None;
        Ok(())
    }

    pub(crate) fn install_refresh(
        &mut self,
        evidence: PapiVerifiedRiskSnapshot,
        applied_fact_version: u64,
        now: Instant,
        ts_event: UnixNanos,
    ) -> Result<(), CoordinatorError> {
        if self.rebaseline.is_some() || applied_fact_version == 0 {
            return Err(CoordinatorError::RebaselineGeneration);
        }

        self.validate_evidence(&evidence, now)?;
        self.validate_evidence_replacement(&evidence)?;
        self.validate_rebaseline_hard_limits(&evidence)?;
        let operation_ids = self.validate_rebaseline_coverage(&evidence)?;

        if !operation_ids.is_empty() {
            self.journal.rebaseline(
                &operation_ids,
                evidence.generation,
                applied_fact_version,
                ts_event,
            )?;
        }

        for operation_id in &operation_ids {
            self.recovered_unresolved.remove(operation_id);
        }
        self.evidence = Some(evidence);
        Ok(())
    }

    pub(crate) fn prepare_submit(
        &mut self,
        mut operation: PapiPersistedOperation,
        now: Instant,
    ) -> Result<PapiReservation, CoordinatorError> {
        let reservation = self.check_submit(&operation, now)?;
        let evidence_generation = self.current_evidence(now)?.generation;

        operation.generation = evidence_generation;
        operation.reservation = Some(reservation.clone());
        self.journal.append_prepared(operation.clone())?;
        self.submit_client_order_ids
            .insert(operation.client_order_id);
        Ok(reservation)
    }

    /// Checks the exact submit admission contract without reserving or writing the journal.
    pub(crate) fn check_submit(
        &self,
        operation: &PapiPersistedOperation,
        now: Instant,
    ) -> Result<PapiReservation, CoordinatorError> {
        let intent = match &operation.command {
            PapiPersistedCommand::Submit(intent) => intent,
            PapiPersistedCommand::Cancel { .. } => {
                return Err(CoordinatorError::WrongCommandKind);
            }
        };

        if operation.account_id != self.journal.account_id() {
            return Err(CoordinatorError::AccountMismatch);
        }

        if self
            .submit_client_order_ids
            .contains(&operation.client_order_id)
        {
            return Err(CoordinatorError::DuplicateClientOrderId(
                operation.client_order_id,
            ));
        }

        let evidence = self.current_evidence(now)?;

        if !intent_reduce_only(intent)
            && (!self.recovered_unresolved.is_empty()
                || self.uncertain
                || self.rebaseline.is_some()
                || !self.session_allows_increase_risk())
        {
            return Err(CoordinatorError::RecoveryRestricted);
        }

        if self.unresolved_count() >= self.config.max_in_flight_operations {
            return Err(CoordinatorError::InFlightLimit);
        }

        let limits = self.instrument_limits(operation.instrument_id)?;
        let reservation = self.build_reservation(intent, operation.instrument_id, evidence)?;
        self.validate_order_limits(intent, limits, &reservation, evidence)?;
        self.validate_position_limit(intent, operation.instrument_id, limits, evidence)?;
        self.validate_account_limits(operation.instrument_id, limits, &reservation, evidence)?;
        Ok(reservation)
    }

    pub(crate) fn prepare_cancel(
        &mut self,
        mut operation: PapiPersistedOperation,
        ts_event: UnixNanos,
    ) -> Result<PapiCancelPreparation, CoordinatorError> {
        if operation.account_id != self.journal.account_id() {
            return Err(CoordinatorError::AccountMismatch);
        }

        if !matches!(operation.command, PapiPersistedCommand::Cancel { .. }) {
            return Err(CoordinatorError::WrongCommandKind);
        }

        if let Some(submit_operation_id) =
            self.prepared_submit_target(operation.instrument_id, operation.client_order_id)
        {
            self.transition(
                submit_operation_id,
                PapiOperationStage::Resolved {
                    resolution: PapiOperationResolution::NotSent,
                },
                ts_event,
            )?;
            return Ok(PapiCancelPreparation::LocalSubmitCanceled {
                submit_operation_id,
            });
        }

        if self.unresolved_count() >= self.config.max_in_flight_operations {
            return Err(CoordinatorError::InFlightLimit);
        }

        if self.has_unresolved_cancel(operation.instrument_id, operation.client_order_id) {
            return Err(CoordinatorError::DuplicateCancelTarget(
                operation.client_order_id,
            ));
        }

        let target_generation = self
            .target_generation(operation.instrument_id, operation.client_order_id)
            .ok_or(CoordinatorError::UnknownCancelTarget(
                operation.client_order_id,
            ))?;
        operation.generation = target_generation;
        operation.reservation = None;
        let cancel_operation_id = operation.operation_id;
        self.journal.append_prepared(operation)?;
        Ok(PapiCancelPreparation::Prepared {
            cancel_operation_id,
        })
    }

    /// Freezes the currently owned ordinary UM targets within one cancel-all command scope.
    pub(crate) fn plan_cancel_all(
        &self,
        command: &CancelAllOrders,
    ) -> Result<Vec<PapiPersistedOperation>, CoordinatorError> {
        if command.params.is_some() {
            return Err(CoordinatorError::InvalidCancelAll);
        }

        let side = command.order_side.map(|side| match side {
            OrderSide::Buy => PapiIntentSide::Buy,
            OrderSide::Sell => PapiIntentSide::Sell,
        });
        let mut targets = Vec::new();

        for recovered in self.journal.operations().values() {
            let PapiPersistedCommand::Submit(intent) = &recovered.operation.command else {
                continue;
            };

            if recovered.operation.strategy_id != command.strategy_id
                || recovered.operation.instrument_id != command.instrument_id
                || side.is_some_and(|side| intent_side(intent) != side)
                || self.has_unresolved_cancel(
                    recovered.operation.instrument_id,
                    recovered.operation.client_order_id,
                )
            {
                continue;
            }

            let evidence_order = self.matching_verified_open_order(&recovered.operation, intent);
            let venue_order_id = match recovered.stage {
                PapiOperationStage::Prepared
                | PapiOperationStage::MayHaveDispatched
                | PapiOperationStage::Unknown { .. } => None,
                PapiOperationStage::Observed { venue_order_id } => Some(venue_order_id),
                PapiOperationStage::Resolved { .. } => {
                    let Some(order) = evidence_order else {
                        continue;
                    };
                    Some(order.venue_order_id)
                }
            };
            targets.push((recovered.operation.client_order_id, venue_order_id));
        }

        if targets.is_empty() {
            return Err(CoordinatorError::EmptyCancelAll);
        }

        targets.sort_unstable_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
        Ok(targets
            .into_iter()
            .map(|(client_order_id, venue_order_id)| PapiPersistedOperation {
                operation_id: UUID4::new(),
                account_id: self.journal.account_id(),
                strategy_id: command.strategy_id,
                instrument_id: command.instrument_id,
                client_order_id,
                generation: 0,
                ts_init: command.ts_init,
                command: PapiPersistedCommand::Cancel { venue_order_id },
                reservation: None,
            })
            .collect())
    }

    pub(crate) fn transition(
        &mut self,
        operation_id: UUID4,
        stage: PapiOperationStage,
        ts_event: UnixNanos,
    ) -> Result<(), CoordinatorError> {
        let is_unknown = matches!(stage, PapiOperationStage::Unknown { .. });
        let is_resolved = matches!(stage, PapiOperationStage::Resolved { .. });
        self.journal.transition(operation_id, stage, ts_event)?;

        if is_unknown {
            self.uncertain = true;
        } else {
            self.uncertain = self
                .journal
                .operations()
                .values()
                .any(|operation| matches!(operation.stage, PapiOperationStage::Unknown { .. }));
        }

        if is_resolved {
            self.recovered_unresolved.remove(&operation_id);
        }
        Ok(())
    }

    /// Rechecks send authority and durably crosses the dispatch boundary.
    pub(crate) fn dispatch_barrier(
        &mut self,
        operation_id: UUID4,
        now: Instant,
        ts_event: UnixNanos,
    ) -> Result<(), CoordinatorError> {
        if let Err(e) = self.validate_dispatch(operation_id, now) {
            if self
                .journal
                .operations()
                .get(&operation_id)
                .is_some_and(|operation| matches!(operation.stage, PapiOperationStage::Prepared))
            {
                self.transition(
                    operation_id,
                    PapiOperationStage::Resolved {
                        resolution: PapiOperationResolution::NotSent,
                    },
                    ts_event,
                )?;
            }
            return Err(e);
        }

        if let Err(e) = self.transition(
            operation_id,
            PapiOperationStage::MayHaveDispatched,
            ts_event,
        ) {
            // A failed sync has an uncertain durability result. No request is sent, and all
            // subsequent increase-risk admission remains closed for this process.
            self.uncertain = true;
            return Err(e);
        }
        Ok(())
    }

    /// Maps transport evidence into the durable operation state machine.
    pub(crate) fn apply_command_failure(
        &mut self,
        operation_id: UUID4,
        failure: &PapiCommandFailure,
        ts_event: UnixNanos,
    ) -> Result<(), CoordinatorError> {
        let stage = match &failure.classification {
            CommandFailure::NotSent(_) => PapiOperationStage::Resolved {
                resolution: PapiOperationResolution::NotSent,
            },
            CommandFailure::VenueRejected(_) => PapiOperationStage::Resolved {
                resolution: PapiOperationResolution::VenueRejected,
            },
            CommandFailure::Ambiguous(_) => PapiOperationStage::Unknown {
                reason: unknown_reason(&failure.error),
            },
        };
        self.transition(operation_id, stage, ts_event)
    }

    /// Records a positive write response without releasing its reservation.
    pub(crate) fn apply_command_success(
        &mut self,
        operation_id: UUID4,
        acknowledgement: &PapiCommandAcknowledgement,
        ts_event: UnixNanos,
    ) -> Result<CommandSuccessApplication, CoordinatorError> {
        let recovered = self
            .journal
            .operations()
            .get(&operation_id)
            .ok_or(CoordinatorError::UnknownOperation(operation_id))?;
        let operation = recovered.operation.clone();
        let current_stage = recovered.stage.clone();
        let venue_identity_matches = match operation.command {
            PapiPersistedCommand::Cancel {
                venue_order_id: Some(expected),
            } => expected == acknowledgement.venue_order_id,
            PapiPersistedCommand::Submit(_) | PapiPersistedCommand::Cancel { .. } => true,
        };

        if acknowledgement.venue_order_id <= 0
            || acknowledgement.client_order_id != operation.client_order_id.as_str()
            || !venue_identity_matches
        {
            self.transition(
                operation_id,
                PapiOperationStage::Unknown {
                    reason: PapiUnknownReason::Decode,
                },
                ts_event,
            )?;
            return Err(CoordinatorError::CommandResponseIdentity);
        }

        match current_stage {
            PapiOperationStage::Observed { venue_order_id }
                if venue_order_id == acknowledgement.venue_order_id =>
            {
                return Ok(CommandSuccessApplication::Superseded);
            }
            PapiOperationStage::Resolved {
                resolution:
                    PapiOperationResolution::VenueRejected
                    | PapiOperationResolution::Canceled
                    | PapiOperationResolution::Expired
                    | PapiOperationResolution::Filled,
            } => return Ok(CommandSuccessApplication::Superseded),
            _ => {}
        }

        self.transition(
            operation_id,
            PapiOperationStage::Observed {
                venue_order_id: acknowledgement.venue_order_id,
            },
            ts_event,
        )?;
        Ok(CommandSuccessApplication::Applied)
    }

    /// Dispatches one prepared submit from its durable intent and records every outcome.
    pub(crate) async fn dispatch_submit(
        &mut self,
        operation_id: UUID4,
        http: &PapiHttpClient,
        budget: &RequestBudget,
        cancel: &CancellationToken,
    ) -> Result<PapiCommandResponse, CoordinatorDispatchError> {
        let request = match self.submit_request(operation_id) {
            Ok(request) => request,
            Err(e) => {
                self.resolve_local_build_failure(operation_id, http.now())?;
                return Err(e.into());
            }
        };
        let result = http
            .submit_um_order_with_barrier(&request, budget, cancel, || {
                self.dispatch_barrier(operation_id, Instant::now(), http.now())
                    .map_err(anyhow::Error::from)
            })
            .await;
        self.apply_dispatch_result(operation_id, result, http.now())
    }

    /// Dispatches one prepared targeted cancel from its durable intent and records every outcome.
    pub(crate) async fn dispatch_cancel(
        &mut self,
        operation_id: UUID4,
        http: &PapiHttpClient,
        budget: &RequestBudget,
        cancel: &CancellationToken,
    ) -> Result<PapiCommandResponse, CoordinatorDispatchError> {
        let request = match self.cancel_request(operation_id) {
            Ok(request) => request,
            Err(e) => {
                self.resolve_local_build_failure(operation_id, http.now())?;
                return Err(e.into());
            }
        };
        let result = http
            .cancel_um_order_with_barrier(&request, budget, cancel, || {
                self.dispatch_barrier(operation_id, Instant::now(), http.now())
                    .map_err(anyhow::Error::from)
            })
            .await;
        self.apply_dispatch_result(operation_id, result, http.now())
    }

    /// Dispatches a fixed cancel target set one-by-one and retains every independent outcome.
    pub(crate) async fn dispatch_cancel_batch(
        &mut self,
        operation_ids: &[UUID4],
        http: &PapiHttpClient,
        budget: &RequestBudget,
        cancel: &CancellationToken,
    ) -> Result<Vec<(UUID4, Result<PapiCommandResponse, CoordinatorDispatchError>)>, CoordinatorError>
    {
        if operation_ids.is_empty()
            || operation_ids.len() > self.config.max_in_flight_operations
            || operation_ids.iter().copied().collect::<HashSet<_>>().len() != operation_ids.len()
            || operation_ids.iter().any(|operation_id| {
                !self
                    .journal
                    .operations()
                    .get(operation_id)
                    .is_some_and(|operation| {
                        matches!(
                            operation.operation.command,
                            PapiPersistedCommand::Cancel { .. }
                        ) && matches!(operation.stage, PapiOperationStage::Prepared)
                    })
            })
        {
            return Err(CoordinatorError::InvalidCancelBatch);
        }

        let mut outcomes = Vec::with_capacity(operation_ids.len());
        for operation_id in operation_ids {
            outcomes.push((
                *operation_id,
                self.dispatch_cancel(*operation_id, http, budget, cancel)
                    .await,
            ));
        }
        Ok(outcomes)
    }

    pub(crate) fn apply_order_report(
        &mut self,
        operation_id: UUID4,
        report: &OrderStatusReport,
        ts_event: UnixNanos,
    ) -> Result<(), CoordinatorError> {
        let recovered = self
            .journal
            .operations()
            .get(&operation_id)
            .ok_or(CoordinatorError::UnknownOperation(operation_id))?;
        let operation = recovered.operation.clone();
        let current_stage = recovered.stage.clone();

        if operation.account_id != report.account_id
            || operation.instrument_id != report.instrument_id
            || report.client_order_id != Some(operation.client_order_id)
        {
            return Err(CoordinatorError::ReportIdentityMismatch);
        }

        if let PapiPersistedCommand::Submit(intent) = &operation.command
            && !report_matches_intent(report, intent)
        {
            return Err(CoordinatorError::ReportTermsMismatch);
        }
        let venue_order_id = decode_ordinary_venue_order_id(report.venue_order_id.as_str())?;

        if matches!(current_stage, PapiOperationStage::Prepared) {
            return Err(CoordinatorError::ReportBeforeDispatch);
        }

        let active = matches!(
            report.order_status,
            OrderStatus::Submitted
                | OrderStatus::Accepted
                | OrderStatus::Triggered
                | OrderStatus::PendingUpdate
                | OrderStatus::PendingCancel
                | OrderStatus::PartiallyFilled
        );

        if active {
            if matches!(operation.command, PapiPersistedCommand::Cancel { .. }) {
                return Ok(());
            }

            if let PapiOperationStage::Observed {
                venue_order_id: current,
            } = current_stage
            {
                return if current == venue_order_id {
                    Ok(())
                } else {
                    Err(CoordinatorError::ReportIdentityMismatch)
                };
            }

            return self.transition(
                operation_id,
                PapiOperationStage::Observed { venue_order_id },
                ts_event,
            );
        }

        let resolution = match report.order_status {
            OrderStatus::Rejected => PapiOperationResolution::VenueRejected,
            OrderStatus::Canceled => PapiOperationResolution::Canceled,
            OrderStatus::Expired => PapiOperationResolution::Expired,
            OrderStatus::Filled => PapiOperationResolution::Filled,
            OrderStatus::Initialized
            | OrderStatus::Denied
            | OrderStatus::Emulated
            | OrderStatus::Released
            | OrderStatus::Voided
            | OrderStatus::Submitted
            | OrderStatus::Accepted
            | OrderStatus::Triggered
            | OrderStatus::PendingUpdate
            | OrderStatus::PendingCancel
            | OrderStatus::PartiallyFilled => {
                return Err(CoordinatorError::UnsupportedReportStatus);
            }
        };
        self.transition(
            operation_id,
            PapiOperationStage::Resolved { resolution },
            ts_event,
        )
    }

    /// Applies one authoritative report to every unresolved operation with the same identity.
    ///
    /// Returns the number of durable operations which own that order identity, including operations
    /// already resolved by an earlier report.
    pub(crate) fn apply_matching_order_report(
        &mut self,
        report: &OrderStatusReport,
        ts_event: UnixNanos,
    ) -> Result<usize, CoordinatorError> {
        let matching: Vec<_> = self
            .journal
            .operations()
            .values()
            .filter(|operation| {
                operation.operation.account_id == report.account_id
                    && operation.operation.instrument_id == report.instrument_id
                    && report.client_order_id == Some(operation.operation.client_order_id)
                    && !matches!(operation.stage, PapiOperationStage::Prepared)
            })
            .map(|operation| {
                (
                    operation.operation.operation_id,
                    matches!(operation.stage, PapiOperationStage::Resolved { .. }),
                )
            })
            .collect();

        for (operation_id, resolved) in &matching {
            if *resolved {
                continue;
            }
            self.apply_order_report(*operation_id, report, ts_event)?;
        }
        Ok(matching.len())
    }

    pub(crate) async fn recover_unknowns(
        &mut self,
        reader: &BinancePapiReadOnlyClient,
    ) -> Result<PapiRecoverySummary, CoordinatorError> {
        let mut collector = reader
            .recovery_collector(self.config.max_recovery_requests)
            .map_err(|_| CoordinatorError::RecoveryUnavailable)?;
        let mut summary = PapiRecoverySummary::default();
        let mut stopped = false;

        for round in 0..self.config.max_recovery_rounds {
            let queries = self.recovery_queries();

            if queries.is_empty() {
                break;
            }
            summary.rounds = round + 1;

            for (operation_id, instrument_id, client_order_id) in queries {
                match collector
                    .single_order(instrument_id, None, Some(client_order_id))
                    .await
                {
                    Ok(report) => {
                        self.apply_order_report(operation_id, &report, reader.now())?;
                        summary.reports_applied += 1;
                    }
                    Err(e) => {
                        let http_error = e.downcast_ref::<PapiHttpError>();

                        if http_error == Some(&PapiHttpError::Budget) {
                            summary.budget_exhausted = true;
                            stopped = true;
                            break;
                        }

                        if http_error.is_some_and(terminal_recovery_error) {
                            stopped = true;
                            break;
                        }
                    }
                }
            }

            if stopped || self.recovery_queries().is_empty() {
                break;
            }

            if round + 1 < self.config.max_recovery_rounds {
                tokio::time::sleep(Duration::from_millis(
                    self.config.recovery_recheck_interval_ms,
                ))
                .await;
            }
        }
        summary.unresolved = self.journal.unresolved().count();
        Ok(summary)
    }

    pub(crate) fn recovered_order_strategy(
        &self,
        report: &OrderStatusReport,
    ) -> Result<Option<StrategyId>, CoordinatorError> {
        let Some(client_order_id) = report.client_order_id else {
            return Ok(None);
        };
        let mut owner = None;

        for recovered in self.journal.operations().values() {
            let operation = &recovered.operation;
            let PapiPersistedCommand::Submit(intent) = &operation.command else {
                continue;
            };

            if operation.client_order_id != client_order_id {
                continue;
            }

            if operation.account_id != report.account_id
                || operation.instrument_id != report.instrument_id
                || owner.is_some_and(|strategy| strategy != operation.strategy_id)
            {
                return Err(CoordinatorError::ReportIdentityMismatch);
            }

            if !report_matches_intent(report, intent) {
                return Err(CoordinatorError::ReportTermsMismatch);
            }
            let venue_order_id = decode_ordinary_venue_order_id(report.venue_order_id.as_str())?;

            match recovered.stage {
                PapiOperationStage::Prepared
                | PapiOperationStage::Resolved {
                    resolution:
                        PapiOperationResolution::NotSent
                        | PapiOperationResolution::VenueRejected
                        | PapiOperationResolution::ProvedAbsent,
                } => return Err(CoordinatorError::ReportBeforeDispatch),
                PapiOperationStage::Observed {
                    venue_order_id: expected,
                } if venue_order_id != expected => {
                    return Err(CoordinatorError::ReportIdentityMismatch);
                }
                _ => {}
            }
            owner = Some(operation.strategy_id);
        }
        Ok(owner)
    }

    pub(crate) fn journal(&self) -> &PapiCommandJournal {
        &self.journal
    }

    fn current_evidence(
        &self,
        now: Instant,
    ) -> Result<&PapiVerifiedRiskSnapshot, CoordinatorError> {
        let evidence = self
            .evidence
            .as_ref()
            .ok_or(CoordinatorError::MissingEvidence)?;
        self.validate_evidence(evidence, now)?;
        Ok(evidence)
    }

    fn submit_request(
        &self,
        operation_id: UUID4,
    ) -> Result<SubmitUmOrderRequest, CoordinatorError> {
        let operation = &self
            .journal
            .operations()
            .get(&operation_id)
            .ok_or(CoordinatorError::UnknownOperation(operation_id))?
            .operation;
        let PapiPersistedCommand::Submit(intent) = &operation.command else {
            return Err(CoordinatorError::WrongCommandKind);
        };
        let symbol = format_binance_symbol(&operation.instrument_id);
        let side = match intent_side(intent) {
            PapiIntentSide::Buy => PapiUmOrderSide::Buy,
            PapiIntentSide::Sell => PapiUmOrderSide::Sell,
        };

        match intent {
            PapiSubmitIntent::Market {
                quantity,
                reduce_only,
                ..
            } => SubmitUmOrderRequest::market(
                symbol,
                side,
                *quantity,
                operation.client_order_id,
                *reduce_only,
            ),
            PapiSubmitIntent::Limit {
                quantity,
                price,
                time_in_force,
                reduce_only,
                ..
            } => {
                let time_in_force = match time_in_force {
                    PapiIntentTimeInForce::Gtc => PapiUmTimeInForce::Gtc,
                    PapiIntentTimeInForce::Ioc => PapiUmTimeInForce::Ioc,
                    PapiIntentTimeInForce::Fok => PapiUmTimeInForce::Fok,
                    PapiIntentTimeInForce::Gtx => PapiUmTimeInForce::Gtx,
                };
                SubmitUmOrderRequest::limit(
                    symbol,
                    side,
                    *quantity,
                    *price,
                    time_in_force,
                    operation.client_order_id,
                    *reduce_only,
                )
            }
        }
        .map_err(CoordinatorError::Build)
    }

    fn cancel_request(
        &self,
        operation_id: UUID4,
    ) -> Result<CancelUmOrderRequest, CoordinatorError> {
        let operation = &self
            .journal
            .operations()
            .get(&operation_id)
            .ok_or(CoordinatorError::UnknownOperation(operation_id))?
            .operation;
        let PapiPersistedCommand::Cancel { venue_order_id } = operation.command else {
            return Err(CoordinatorError::WrongCommandKind);
        };
        let symbol = format_binance_symbol(&operation.instrument_id);

        match venue_order_id {
            Some(venue_order_id) => CancelUmOrderRequest::by_order_id(symbol, venue_order_id),
            None => CancelUmOrderRequest::by_client_order_id(symbol, operation.client_order_id),
        }
        .map_err(CoordinatorError::Build)
    }

    fn resolve_local_build_failure(
        &mut self,
        operation_id: UUID4,
        ts_event: UnixNanos,
    ) -> Result<(), CoordinatorError> {
        if self
            .journal
            .operations()
            .get(&operation_id)
            .is_some_and(|operation| matches!(operation.stage, PapiOperationStage::Prepared))
        {
            self.transition(
                operation_id,
                PapiOperationStage::Resolved {
                    resolution: PapiOperationResolution::NotSent,
                },
                ts_event,
            )?;
        }
        Ok(())
    }

    fn apply_dispatch_result(
        &mut self,
        operation_id: UUID4,
        result: Result<PapiCommandResponse, PapiCommandDispatchError>,
        ts_event: UnixNanos,
    ) -> Result<PapiCommandResponse, CoordinatorDispatchError> {
        match result {
            Ok(response) => {
                match self.apply_command_success(
                    operation_id,
                    &response.acknowledgement,
                    ts_event,
                )? {
                    CommandSuccessApplication::Applied => Ok(response),
                    CommandSuccessApplication::Superseded => {
                        Err(CoordinatorDispatchError::Superseded)
                    }
                }
            }
            Err(PapiCommandDispatchError::Command(failure)) => {
                if self.report_already_applied(operation_id)? {
                    return Err(CoordinatorDispatchError::Superseded);
                }
                self.apply_command_failure(operation_id, &failure, ts_event)?;
                Err(CoordinatorDispatchError::Command(failure))
            }
            Err(PapiCommandDispatchError::Barrier(error)) => {
                Err(CoordinatorDispatchError::Barrier(error))
            }
        }
    }

    fn report_already_applied(&self, operation_id: UUID4) -> Result<bool, CoordinatorError> {
        let recovered = self
            .journal
            .operations()
            .get(&operation_id)
            .ok_or(CoordinatorError::UnknownOperation(operation_id))?;
        Ok(matches!(
            recovered.stage,
            PapiOperationStage::Observed { .. }
                | PapiOperationStage::Resolved {
                    resolution: PapiOperationResolution::VenueRejected
                        | PapiOperationResolution::Canceled
                        | PapiOperationResolution::Expired
                        | PapiOperationResolution::Filled,
                }
        ))
    }

    fn validate_dispatch(&self, operation_id: UUID4, now: Instant) -> Result<(), CoordinatorError> {
        let recovered = self
            .journal
            .operations()
            .get(&operation_id)
            .ok_or(CoordinatorError::UnknownOperation(operation_id))?;

        if !matches!(recovered.stage, PapiOperationStage::Prepared) {
            return Err(CoordinatorError::DispatchNotPrepared);
        }

        match &recovered.operation.command {
            PapiPersistedCommand::Submit(intent) => {
                let evidence = self.current_evidence(now)?;

                if evidence.generation != recovered.operation.generation {
                    return Err(CoordinatorError::GenerationMismatch);
                }

                if !intent_reduce_only(intent)
                    && (!self.recovered_unresolved.is_empty()
                        || self.uncertain
                        || self.rebaseline.is_some()
                        || !self.session_allows_increase_risk())
                {
                    return Err(CoordinatorError::RecoveryRestricted);
                }
            }
            PapiPersistedCommand::Cancel { .. } => {
                let generation = self
                    .target_generation(
                        recovered.operation.instrument_id,
                        recovered.operation.client_order_id,
                    )
                    .ok_or(CoordinatorError::UnknownCancelTarget(
                        recovered.operation.client_order_id,
                    ))?;

                if generation != recovered.operation.generation {
                    return Err(CoordinatorError::GenerationMismatch);
                }
            }
        }
        Ok(())
    }

    fn validate_evidence(
        &self,
        evidence: &PapiVerifiedRiskSnapshot,
        now: Instant,
    ) -> Result<(), CoordinatorError> {
        if evidence.account_id != self.journal.account_id() {
            return Err(CoordinatorError::AccountMismatch);
        }

        if evidence.generation == 0 {
            return Err(CoordinatorError::InvalidEvidence(
                "risk generation must be positive",
            ));
        }

        let status_generation = match evidence.status {
            PapiAccountStatusEvidence::Normal {
                endpoint,
                generation,
            } => {
                validate_source(endpoint)?;
                generation
            }
        };
        let mode_generation = match evidence.position_mode {
            PapiPositionModeEvidence::OneWay {
                endpoint,
                generation,
            } => {
                validate_source(endpoint)?;
                generation
            }
        };

        if status_generation != evidence.generation || mode_generation != evidence.generation {
            return Err(CoordinatorError::GenerationMismatch);
        }

        validate_source(evidence.units.source)?;
        validate_source(evidence.margin_rule.source)?;

        if evidence.units.currency != self.config.risk_currency {
            return Err(CoordinatorError::RiskCurrencyMismatch);
        }

        if evidence.margin_rule.generation != evidence.generation {
            return Err(CoordinatorError::GenerationMismatch);
        }

        if evidence.margin_rule.max_initial_margin_rate <= Decimal::ZERO
            || evidence.margin_rule.max_initial_margin_rate > Decimal::ONE
            || evidence.available_initial_margin < Decimal::ZERO
            || evidence.account_exposure < Decimal::ZERO
        {
            return Err(CoordinatorError::InvalidEvidence(
                "risk amounts and margin rate are outside supported bounds",
            ));
        }

        let age = now
            .checked_duration_since(evidence.observed_at)
            .ok_or(CoordinatorError::FutureEvidence)?;

        if age > Duration::from_millis(self.config.max_risk_age_ms) {
            return Err(CoordinatorError::StaleEvidence);
        }

        if evidence.collection_span > Duration::from_millis(self.config.max_risk_collection_span_ms)
        {
            return Err(CoordinatorError::CollectionSpan);
        }

        for limits in &self.config.instrument_limits {
            let risk = evidence.instruments.get(&limits.instrument_id).ok_or(
                CoordinatorError::MissingInstrumentEvidence(limits.instrument_id),
            )?;
            validate_source(risk.price_source)?;
            validate_source(risk.rules.source)?;

            if risk.reference_price <= Decimal::ZERO
                || risk.exposure < Decimal::ZERO
                || risk.price_generation == 0
                || risk.rules.generation == 0
            {
                return Err(CoordinatorError::InvalidEvidence(
                    "instrument risk values are outside supported bounds",
                ));
            }

            self.validate_instrument_rules(limits.instrument_id, &risk.rules)?;

            let price_age = now
                .checked_duration_since(risk.price_observed_at)
                .ok_or(CoordinatorError::FutureEvidence)?;

            if price_age > Duration::from_millis(self.config.max_risk_age_ms) {
                return Err(CoordinatorError::StalePrice(limits.instrument_id));
            }
        }

        let allowed: HashSet<_> = self
            .config
            .instrument_limits
            .iter()
            .map(|limits| limits.instrument_id)
            .collect();
        let mut open_order_ids = HashSet::new();

        for order in &evidence.open_orders {
            if !allowed.contains(&order.instrument_id) {
                return Err(CoordinatorError::UnsupportedInstrument(order.instrument_id));
            }

            if order.venue_order_id <= 0
                || order.quantity <= Decimal::ZERO
                || order.worst_case_exposure < Decimal::ZERO
                || (!order.reduce_only && order.worst_case_exposure.is_zero())
            {
                return Err(CoordinatorError::InvalidEvidence(
                    "open-order identity, quantity, or exposure is invalid",
                ));
            }

            if !open_order_ids.insert(order.client_order_id) {
                return Err(CoordinatorError::InvalidEvidence(
                    "open-order client identities must be unique",
                ));
            }
        }
        Ok(())
    }

    fn validate_evidence_replacement(
        &self,
        evidence: &PapiVerifiedRiskSnapshot,
    ) -> Result<(), CoordinatorError> {
        let Some(current) = &self.evidence else {
            return Ok(());
        };

        if evidence.generation <= current.generation {
            return Err(CoordinatorError::EvidenceGenerationNotIncreasing);
        }

        for (instrument_id, risk) in &evidence.instruments {
            let Some(current_risk) = current.instruments.get(instrument_id) else {
                continue;
            };

            if risk.price_generation < current_risk.price_generation
                || risk.rules.generation < current_risk.rules.generation
            {
                return Err(CoordinatorError::InstrumentGenerationRollback(
                    *instrument_id,
                ));
            }
        }
        Ok(())
    }

    fn build_reservation(
        &self,
        intent: &PapiSubmitIntent,
        instrument_id: InstrumentId,
        evidence: &PapiVerifiedRiskSnapshot,
    ) -> Result<PapiReservation, CoordinatorError> {
        let risk = evidence
            .instruments
            .get(&instrument_id)
            .ok_or(CoordinatorError::MissingInstrumentEvidence(instrument_id))?;
        let quantity = intent_quantity(intent);
        let buffered_reference = checked_add(
            risk.reference_price,
            checked_rate_amount(
                risk.reference_price,
                self.config.market_order_price_buffer_bps,
            )?,
        )?;
        let price = match intent {
            PapiSubmitIntent::Market { .. } => buffered_reference,
            PapiSubmitIntent::Limit { price, .. } => (*price).max(buffered_reference),
        };
        let notional = checked_mul(quantity, price)?;
        let fee = checked_rate_amount(notional, self.config.fee_buffer_bps)?;
        let (exposure, initial_margin) = if intent_reduce_only(intent) {
            (Decimal::ZERO, Decimal::ZERO)
        } else {
            (
                checked_add(notional, fee)?,
                checked_add(
                    checked_mul(notional, evidence.margin_rule.max_initial_margin_rate)?,
                    fee,
                )?,
            )
        };

        Ok(PapiReservation {
            risk_currency: self.config.risk_currency,
            quantity,
            notional,
            exposure,
            initial_margin,
        })
    }

    fn validate_order_limits(
        &self,
        intent: &PapiSubmitIntent,
        limits: &BinancePapiInstrumentTradingConfig,
        reservation: &PapiReservation,
        evidence: &PapiVerifiedRiskSnapshot,
    ) -> Result<(), CoordinatorError> {
        let quantity = intent_quantity(intent);
        if quantity <= Decimal::ZERO {
            return Err(CoordinatorError::InvalidOrder("quantity must be positive"));
        }

        if quantity > limits.max_order_quantity {
            return Err(CoordinatorError::OrderQuantityLimit);
        }

        if reservation.notional > limits.max_order_notional {
            return Err(CoordinatorError::OrderNotionalLimit);
        }

        let risk = evidence.instruments.get(&limits.instrument_id).ok_or(
            CoordinatorError::MissingInstrumentEvidence(limits.instrument_id),
        )?;
        let rules = &risk.rules;

        if quantity % rules.quantity_increment != Decimal::ZERO {
            return Err(CoordinatorError::QuantityIncrement);
        }

        if rules.min_quantity.is_some_and(|minimum| quantity < minimum) {
            return Err(CoordinatorError::InstrumentMinimumQuantity);
        }

        if rules.max_quantity.is_some_and(|maximum| quantity > maximum) {
            return Err(CoordinatorError::InstrumentMaximumQuantity);
        }

        let (minimum_notional, maximum_notional) = match intent {
            PapiSubmitIntent::Market { .. } => {
                let buffer = checked_rate_amount(
                    risk.reference_price,
                    self.config.market_order_price_buffer_bps,
                )?;
                let minimum_price = checked_sub(risk.reference_price, buffer)?;
                (checked_mul(quantity, minimum_price)?, reservation.notional)
            }
            PapiSubmitIntent::Limit { price, .. } => {
                if *price <= Decimal::ZERO {
                    return Err(CoordinatorError::InvalidOrder("price must be positive"));
                }

                if *price % rules.price_increment != Decimal::ZERO {
                    return Err(CoordinatorError::PriceIncrement);
                }

                if rules.min_price.is_some_and(|minimum| *price < minimum) {
                    return Err(CoordinatorError::InstrumentMinimumPrice);
                }

                if rules.max_price.is_some_and(|maximum| *price > maximum) {
                    return Err(CoordinatorError::InstrumentMaximumPrice);
                }

                let notional = checked_mul(quantity, *price)?;
                (notional, notional)
            }
        };

        if rules
            .min_notional
            .is_some_and(|minimum| minimum_notional < minimum)
        {
            return Err(CoordinatorError::InstrumentMinimumNotional);
        }

        if rules
            .max_notional
            .is_some_and(|maximum| maximum_notional > maximum)
        {
            return Err(CoordinatorError::InstrumentMaximumNotional);
        }
        Ok(())
    }

    fn validate_instrument_rules(
        &self,
        instrument_id: InstrumentId,
        rules: &PapiVerifiedInstrumentRules,
    ) -> Result<(), CoordinatorError> {
        if !rules.trading {
            return Err(CoordinatorError::InstrumentNotTrading(instrument_id));
        }

        if rules.settlement_currency != self.config.risk_currency {
            return Err(CoordinatorError::InstrumentSettlementCurrency(
                instrument_id,
            ));
        }

        if rules.price_increment <= Decimal::ZERO
            || rules.quantity_increment <= Decimal::ZERO
            || !valid_optional_positive_bounds(rules.min_price, rules.max_price)
            || !valid_optional_positive_bounds(rules.min_quantity, rules.max_quantity)
            || !valid_optional_positive_bounds(rules.min_notional, rules.max_notional)
        {
            return Err(CoordinatorError::InvalidInstrumentRules(instrument_id));
        }
        Ok(())
    }

    fn validate_position_limit(
        &self,
        intent: &PapiSubmitIntent,
        instrument_id: InstrumentId,
        limits: &BinancePapiInstrumentTradingConfig,
        evidence: &PapiVerifiedRiskSnapshot,
    ) -> Result<(), CoordinatorError> {
        let position = evidence
            .instruments
            .get(&instrument_id)
            .ok_or(CoordinatorError::MissingInstrumentEvidence(instrument_id))?
            .signed_position_quantity;
        let mut buy_quantity = Decimal::ZERO;
        let mut sell_quantity = Decimal::ZERO;
        let mut reduce_buy_quantity = Decimal::ZERO;
        let mut reduce_sell_quantity = Decimal::ZERO;

        for order in evidence
            .open_orders
            .iter()
            .filter(|order| order.instrument_id == instrument_id)
        {
            add_side_quantity(
                order.side,
                order.quantity,
                order.reduce_only,
                &mut buy_quantity,
                &mut sell_quantity,
                &mut reduce_buy_quantity,
                &mut reduce_sell_quantity,
            )?;
        }

        for operation in self.journal.operations().values().filter(|operation| {
            operation.operation.instrument_id == instrument_id
                && matches!(operation.operation.command, PapiPersistedCommand::Submit(_))
                && risk_reservation_active(operation, evidence.generation)
        }) {
            let PapiPersistedCommand::Submit(submit) = &operation.operation.command else {
                continue;
            };
            add_side_quantity(
                intent_side(submit),
                intent_quantity(submit),
                intent_reduce_only(submit),
                &mut buy_quantity,
                &mut sell_quantity,
                &mut reduce_buy_quantity,
                &mut reduce_sell_quantity,
            )?;
        }

        if intent_reduce_only(intent) {
            let remaining = match (position.is_sign_positive(), intent_side(intent)) {
                (true, PapiIntentSide::Sell) => checked_sub(position, reduce_sell_quantity)?,
                (false, PapiIntentSide::Buy) if position < Decimal::ZERO => {
                    checked_sub(-position, reduce_buy_quantity)?
                }
                _ => return Err(CoordinatorError::NotReducing),
            };

            if intent_quantity(intent) > remaining.max(Decimal::ZERO) {
                return Err(CoordinatorError::ReduceOnlyQuantity);
            }
            return Ok(());
        }

        match intent_side(intent) {
            PapiIntentSide::Buy => {
                buy_quantity = checked_add(buy_quantity, intent_quantity(intent))?;
            }
            PapiIntentSide::Sell => {
                sell_quantity = checked_add(sell_quantity, intent_quantity(intent))?;
            }
        }
        let long_position = position.max(Decimal::ZERO);
        let short_position = (-position).max(Decimal::ZERO);
        let potential_long = checked_add(long_position, buy_quantity)?;
        let potential_short = checked_add(short_position, sell_quantity)?;

        if potential_long > limits.max_position_quantity
            || potential_short > limits.max_position_quantity
        {
            return Err(CoordinatorError::PositionLimit);
        }
        Ok(())
    }

    fn validate_account_limits(
        &self,
        instrument_id: InstrumentId,
        limits: &BinancePapiInstrumentTradingConfig,
        reservation: &PapiReservation,
        evidence: &PapiVerifiedRiskSnapshot,
    ) -> Result<(), CoordinatorError> {
        let mut reserved_exposure = Decimal::ZERO;
        let mut reserved_instrument_exposure = Decimal::ZERO;
        let mut reserved_margin = Decimal::ZERO;
        let mut open_order_exposure = Decimal::ZERO;
        let mut open_instrument_exposure = Decimal::ZERO;

        for operation in self
            .journal
            .operations()
            .values()
            .filter(|operation| risk_reservation_active(operation, evidence.generation))
        {
            let Some(existing) = &operation.operation.reservation else {
                continue;
            };
            reserved_exposure = checked_add(reserved_exposure, existing.exposure)?;
            reserved_margin = checked_add(reserved_margin, existing.initial_margin)?;

            if operation.operation.instrument_id == instrument_id {
                reserved_instrument_exposure =
                    checked_add(reserved_instrument_exposure, existing.exposure)?;
            }
        }

        for order in &evidence.open_orders {
            open_order_exposure = checked_add(open_order_exposure, order.worst_case_exposure)?;

            if order.instrument_id == instrument_id {
                open_instrument_exposure =
                    checked_add(open_instrument_exposure, order.worst_case_exposure)?;
            }
        }
        let instrument_exposure = evidence
            .instruments
            .get(&instrument_id)
            .ok_or(CoordinatorError::MissingInstrumentEvidence(instrument_id))?
            .exposure;

        if checked_add(
            checked_add(
                checked_add(instrument_exposure, open_instrument_exposure)?,
                reserved_instrument_exposure,
            )?,
            reservation.exposure,
        )? > limits.max_instrument_exposure
        {
            return Err(CoordinatorError::InstrumentExposureLimit);
        }

        if checked_add(
            checked_add(
                checked_add(evidence.account_exposure, open_order_exposure)?,
                reserved_exposure,
            )?,
            reservation.exposure,
        )? > self.config.max_account_exposure
        {
            return Err(CoordinatorError::AccountExposureLimit);
        }

        if checked_add(reserved_margin, reservation.initial_margin)?
            > evidence.available_initial_margin
        {
            return Err(CoordinatorError::InitialMarginCapacity);
        }
        Ok(())
    }

    fn validate_rebaseline_coverage(
        &self,
        evidence: &PapiVerifiedRiskSnapshot,
    ) -> Result<Vec<UUID4>, CoordinatorError> {
        let mut operation_ids = Vec::new();

        for recovered in self.journal.unresolved() {
            let PapiPersistedCommand::Submit(intent) = &recovered.operation.command else {
                return Err(CoordinatorError::RebaselinePendingOperation);
            };
            let PapiOperationStage::Observed { venue_order_id } = recovered.stage else {
                return Err(CoordinatorError::RebaselinePendingOperation);
            };
            let covered = evidence.open_orders.iter().any(|order| {
                order.instrument_id == recovered.operation.instrument_id
                    && order.client_order_id == recovered.operation.client_order_id
                    && order.venue_order_id == venue_order_id
                    && order.side == intent_side(intent)
                    && order.quantity == intent_quantity(intent)
                    && order.reduce_only == intent_reduce_only(intent)
            });

            if !covered {
                return Err(CoordinatorError::RebaselineCoverage(
                    recovered.operation.client_order_id,
                ));
            }
            operation_ids.push(recovered.operation.operation_id);
        }
        Ok(operation_ids)
    }

    fn validate_rebaseline_hard_limits(
        &self,
        evidence: &PapiVerifiedRiskSnapshot,
    ) -> Result<(), CoordinatorError> {
        let mut account_exposure = evidence.account_exposure;

        for limits in &self.config.instrument_limits {
            let risk = evidence.instruments.get(&limits.instrument_id).ok_or(
                CoordinatorError::MissingInstrumentEvidence(limits.instrument_id),
            )?;
            let mut buy_quantity = Decimal::ZERO;
            let mut sell_quantity = Decimal::ZERO;
            let mut reduce_buy_quantity = Decimal::ZERO;
            let mut reduce_sell_quantity = Decimal::ZERO;
            let mut instrument_exposure = risk.exposure;

            for order in evidence
                .open_orders
                .iter()
                .filter(|order| order.instrument_id == limits.instrument_id)
            {
                add_side_quantity(
                    order.side,
                    order.quantity,
                    order.reduce_only,
                    &mut buy_quantity,
                    &mut sell_quantity,
                    &mut reduce_buy_quantity,
                    &mut reduce_sell_quantity,
                )?;
                instrument_exposure = checked_add(instrument_exposure, order.worst_case_exposure)?;
                account_exposure = checked_add(account_exposure, order.worst_case_exposure)?;
            }

            let long_position = risk.signed_position_quantity.max(Decimal::ZERO);
            let short_position = (-risk.signed_position_quantity).max(Decimal::ZERO);
            if checked_add(long_position, buy_quantity)? > limits.max_position_quantity
                || checked_add(short_position, sell_quantity)? > limits.max_position_quantity
            {
                return Err(CoordinatorError::PositionLimit);
            }

            if instrument_exposure > limits.max_instrument_exposure {
                return Err(CoordinatorError::InstrumentExposureLimit);
            }
        }

        if account_exposure > self.config.max_account_exposure {
            return Err(CoordinatorError::AccountExposureLimit);
        }
        Ok(())
    }

    fn instrument_limits(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<&BinancePapiInstrumentTradingConfig, CoordinatorError> {
        self.config
            .instrument_limits
            .iter()
            .find(|limits| limits.instrument_id == instrument_id)
            .ok_or(CoordinatorError::UnsupportedInstrument(instrument_id))
    }

    fn unresolved_count(&self) -> usize {
        self.journal.unresolved().count()
    }

    fn recovery_queries(&self) -> Vec<(UUID4, InstrumentId, ClientOrderId)> {
        self.journal
            .unresolved()
            .filter(|operation| {
                matches!(
                    operation.stage,
                    PapiOperationStage::MayHaveDispatched | PapiOperationStage::Unknown { .. }
                )
            })
            .map(|operation| {
                (
                    operation.operation.operation_id,
                    operation.operation.instrument_id,
                    operation.operation.client_order_id,
                )
            })
            .collect()
    }

    fn has_cancelable_target(&self) -> bool {
        self.journal.unresolved().any(|operation| {
            matches!(operation.operation.command, PapiPersistedCommand::Submit(_))
                && !matches!(operation.stage, PapiOperationStage::Prepared)
        }) || self
            .evidence
            .as_ref()
            .is_some_and(|evidence| !evidence.open_orders.is_empty())
    }

    fn has_unresolved_cancel(
        &self,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
    ) -> bool {
        self.journal.unresolved().any(|operation| {
            operation.operation.instrument_id == instrument_id
                && operation.operation.client_order_id == client_order_id
                && matches!(
                    operation.operation.command,
                    PapiPersistedCommand::Cancel { .. }
                )
        })
    }

    fn prepared_submit_target(
        &self,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
    ) -> Option<UUID4> {
        self.journal.unresolved().find_map(|operation| {
            (operation.operation.instrument_id == instrument_id
                && operation.operation.client_order_id == client_order_id
                && matches!(operation.operation.command, PapiPersistedCommand::Submit(_))
                && matches!(operation.stage, PapiOperationStage::Prepared))
            .then_some(operation.operation.operation_id)
        })
    }

    fn target_generation(
        &self,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
    ) -> Option<u64> {
        self.journal
            .unresolved()
            .find(|operation| {
                operation.operation.instrument_id == instrument_id
                    && operation.operation.client_order_id == client_order_id
                    && matches!(operation.operation.command, PapiPersistedCommand::Submit(_))
                    && !matches!(operation.stage, PapiOperationStage::Prepared)
            })
            .map(|operation| operation.operation.generation)
            .or_else(|| {
                let owned = self.journal.operations().values().find(|operation| {
                    operation.operation.instrument_id == instrument_id
                        && operation.operation.client_order_id == client_order_id
                        && matches!(operation.operation.command, PapiPersistedCommand::Submit(_))
                })?;
                let PapiPersistedCommand::Submit(intent) = &owned.operation.command else {
                    return None;
                };
                self.matching_verified_open_order(&owned.operation, intent)?;
                self.evidence.as_ref().map(|evidence| evidence.generation)
            })
    }

    fn matching_verified_open_order<'a>(
        &'a self,
        operation: &PapiPersistedOperation,
        intent: &PapiSubmitIntent,
    ) -> Option<&'a PapiVerifiedOpenOrder> {
        let observed_venue_order_id = self
            .journal
            .operations()
            .get(&operation.operation_id)
            .and_then(|recovered| match recovered.stage {
                PapiOperationStage::Observed { venue_order_id } => Some(venue_order_id),
                _ => None,
            });

        self.evidence.as_ref()?.open_orders.iter().find(|order| {
            order.instrument_id == operation.instrument_id
                && order.client_order_id == operation.client_order_id
                && observed_venue_order_id.is_none_or(|value| order.venue_order_id == value)
                && order.side == intent_side(intent)
                && order.quantity == intent_quantity(intent)
                && order.reduce_only == intent_reduce_only(intent)
        })
    }
}

/// Dispatches a prepared submit without holding the account coordinator lock across I/O.
pub(crate) async fn dispatch_submit_shared<F>(
    coordinator: &Arc<Mutex<Option<PapiCommandCoordinator>>>,
    operation_id: UUID4,
    http: &PapiHttpClient,
    budget: &RequestBudget,
    cancel: &CancellationToken,
    after_barrier: F,
) -> Result<PapiCommandResponse, CoordinatorDispatchError>
where
    F: FnOnce() -> anyhow::Result<()> + Send,
{
    let request = {
        let mut guard = coordinator.lock();
        let coordinator = guard.as_mut().ok_or(CoordinatorError::Unavailable)?;
        match coordinator.submit_request(operation_id) {
            Ok(request) => request,
            Err(e) => {
                coordinator.resolve_local_build_failure(operation_id, http.now())?;
                return Err(e.into());
            }
        }
    };
    let shared = Arc::clone(coordinator);
    let result = http
        .submit_um_order_with_barrier(&request, budget, cancel, move || {
            let mut guard = shared.lock();
            let coordinator = guard.as_mut().ok_or(CoordinatorError::Unavailable)?;
            coordinator.dispatch_barrier(operation_id, Instant::now(), http.now())?;
            if let Err(e) = after_barrier() {
                coordinator.transition(
                    operation_id,
                    PapiOperationStage::Resolved {
                        resolution: PapiOperationResolution::NotSent,
                    },
                    http.now(),
                )?;
                return Err(e);
            }
            Ok(())
        })
        .await;
    let mut guard = coordinator.lock();
    guard
        .as_mut()
        .ok_or(CoordinatorError::Unavailable)?
        .apply_dispatch_result(operation_id, result, http.now())
}

/// Dispatches a prepared cancel without holding the account coordinator lock across I/O.
pub(crate) async fn dispatch_cancel_shared<F>(
    coordinator: &Arc<Mutex<Option<PapiCommandCoordinator>>>,
    operation_id: UUID4,
    http: &PapiHttpClient,
    budget: &RequestBudget,
    cancel: &CancellationToken,
    after_barrier: F,
) -> Result<PapiCommandResponse, CoordinatorDispatchError>
where
    F: FnOnce() -> anyhow::Result<()> + Send,
{
    let request = {
        let mut guard = coordinator.lock();
        let coordinator = guard.as_mut().ok_or(CoordinatorError::Unavailable)?;
        match coordinator.cancel_request(operation_id) {
            Ok(request) => request,
            Err(e) => {
                coordinator.resolve_local_build_failure(operation_id, http.now())?;
                return Err(e.into());
            }
        }
    };
    let shared = Arc::clone(coordinator);
    let result = http
        .cancel_um_order_with_barrier(&request, budget, cancel, move || {
            let mut guard = shared.lock();
            let coordinator = guard.as_mut().ok_or(CoordinatorError::Unavailable)?;
            coordinator.dispatch_barrier(operation_id, Instant::now(), http.now())?;
            if let Err(e) = after_barrier() {
                coordinator.transition(
                    operation_id,
                    PapiOperationStage::Resolved {
                        resolution: PapiOperationResolution::NotSent,
                    },
                    http.now(),
                )?;
                return Err(e);
            }
            Ok(())
        })
        .await;
    let mut guard = coordinator.lock();
    guard
        .as_mut()
        .ok_or(CoordinatorError::Unavailable)?
        .apply_dispatch_result(operation_id, result, http.now())
}

#[derive(Debug, Error)]
pub(crate) enum CoordinatorError {
    #[error("PAPI command coordinator is unavailable")]
    Unavailable,
    #[error("Invalid PAPI trading configuration: {0}")]
    InvalidConfig(String),
    #[error("PAPI command does not match the requested coordinator operation")]
    WrongCommandKind,
    #[error("PAPI command journal does not contain operation {0}")]
    UnknownOperation(UUID4),
    #[error("PAPI command account does not match the coordinator account")]
    AccountMismatch,
    #[error("PAPI admission evidence is unavailable")]
    MissingEvidence,
    #[error("PAPI admission evidence is from the future")]
    FutureEvidence,
    #[error("PAPI admission evidence is stale")]
    StaleEvidence,
    #[error("PAPI reference price is stale for {0}")]
    StalePrice(InstrumentId),
    #[error("PAPI admission evidence collection span exceeds the configured limit")]
    CollectionSpan,
    #[error("PAPI admission evidence generations do not agree")]
    GenerationMismatch,
    #[error("PAPI admission evidence generation did not advance")]
    EvidenceGenerationNotIncreasing,
    #[error("PAPI instrument evidence generation moved backward for {0}")]
    InstrumentGenerationRollback(InstrumentId),
    #[error("PAPI admission evidence risk currency does not match configuration")]
    RiskCurrencyMismatch,
    #[error("Invalid PAPI admission evidence: {0}")]
    InvalidEvidence(&'static str),
    #[error("PAPI admission evidence is missing instrument {0}")]
    MissingInstrumentEvidence(InstrumentId),
    #[error("PAPI instrument is not allowlisted: {0}")]
    UnsupportedInstrument(InstrumentId),
    #[error("PAPI instrument is not in a verified trading state: {0}")]
    InstrumentNotTrading(InstrumentId),
    #[error("PAPI instrument settlement currency is not the configured risk currency: {0}")]
    InstrumentSettlementCurrency(InstrumentId),
    #[error("PAPI instrument rules are incomplete or invalid: {0}")]
    InvalidInstrumentRules(InstrumentId),
    #[error("PAPI increase-risk admission is restricted pending recovery")]
    RecoveryRestricted,
    #[error("PAPI risk rebaseline is already in progress")]
    RebaselineInProgress,
    #[error("PAPI risk rebaseline has an unmatched attempt token")]
    RebaselineToken,
    #[error("PAPI risk rebaseline evidence or application generation did not advance")]
    RebaselineGeneration,
    #[error("PAPI risk rebaseline still has a possibly dispatched or cancel operation")]
    RebaselinePendingOperation,
    #[error("PAPI risk rebaseline does not cover observed order {0}")]
    RebaselineCoverage(ClientOrderId),
    #[error("PAPI in-flight operation limit exceeded")]
    InFlightLimit,
    #[error("PAPI client order ID already exists in the command journal: {0}")]
    DuplicateClientOrderId(ClientOrderId),
    #[error("PAPI cancel target already has an unresolved cancel: {0}")]
    DuplicateCancelTarget(ClientOrderId),
    #[error("PAPI cancel batch is empty, duplicated, oversized, or not fully prepared")]
    InvalidCancelBatch,
    #[error("PAPI cancel-all contains unsupported parameters")]
    InvalidCancelAll,
    #[error("PAPI cancel-all has no owned ordinary UM target in scope")]
    EmptyCancelAll,
    #[error("PAPI cancel target is not a known owned ordinary UM order: {0}")]
    UnknownCancelTarget(ClientOrderId),
    #[error("PAPI recovery report identity does not match its durable operation")]
    ReportIdentityMismatch,
    #[error("PAPI recovery report terms do not match its durable submit intent")]
    ReportTermsMismatch,
    #[error("PAPI recovery report was observed before the durable dispatch barrier")]
    ReportBeforeDispatch,
    #[error("PAPI operation is not prepared for dispatch")]
    DispatchNotPrepared,
    #[error("PAPI command response identity does not match its durable operation")]
    CommandResponseIdentity,
    #[error("PAPI recovery report contains a status unsupported at the venue boundary")]
    UnsupportedReportStatus,
    #[error("PAPI recovery query budget could not be initialized")]
    RecoveryUnavailable,
    #[error("Invalid PAPI order: {0}")]
    InvalidOrder(&'static str),
    #[error("PAPI order quantity exceeds the configured limit")]
    OrderQuantityLimit,
    #[error("PAPI order notional exceeds the configured limit")]
    OrderNotionalLimit,
    #[error("PAPI order quantity is not aligned to the verified step size")]
    QuantityIncrement,
    #[error("PAPI order price is not aligned to the verified tick size")]
    PriceIncrement,
    #[error("PAPI order quantity is below the verified instrument minimum")]
    InstrumentMinimumQuantity,
    #[error("PAPI order quantity exceeds the verified instrument maximum")]
    InstrumentMaximumQuantity,
    #[error("PAPI order price is below the verified instrument minimum")]
    InstrumentMinimumPrice,
    #[error("PAPI order price exceeds the verified instrument maximum")]
    InstrumentMaximumPrice,
    #[error("PAPI order notional is below the verified instrument minimum")]
    InstrumentMinimumNotional,
    #[error("PAPI order notional exceeds the verified instrument maximum")]
    InstrumentMaximumNotional,
    #[error("PAPI order exceeds the configured absolute position limit")]
    PositionLimit,
    #[error("PAPI reduce-only order does not reduce the verified one-way position")]
    NotReducing,
    #[error("PAPI reduce-only quantity exceeds the verified reducible position")]
    ReduceOnlyQuantity,
    #[error("PAPI operation exceeds the configured instrument exposure limit")]
    InstrumentExposureLimit,
    #[error("PAPI operation exceeds the configured account exposure limit")]
    AccountExposureLimit,
    #[error("PAPI operation exceeds verified initial-margin capacity")]
    InitialMarginCapacity,
    #[error("PAPI Decimal arithmetic overflowed")]
    Arithmetic,
    #[error(transparent)]
    Build(#[from] PapiCommandBuildError),
    #[error(transparent)]
    Journal(#[from] JournalError),
}

#[derive(Debug, Error)]
pub(crate) enum CoordinatorDispatchError {
    #[error("PAPI command failed: {0:?}")]
    Command(PapiCommandFailure),
    #[error("PAPI durable dispatch barrier failed")]
    Barrier(#[source] anyhow::Error),
    #[error("PAPI command result was superseded by an authoritative order report")]
    Superseded,
    #[error(transparent)]
    Coordinator(#[from] CoordinatorError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommandSuccessApplication {
    Applied,
    Superseded,
}

fn intent_side(intent: &PapiSubmitIntent) -> PapiIntentSide {
    match intent {
        PapiSubmitIntent::Market { side, .. } | PapiSubmitIntent::Limit { side, .. } => *side,
    }
}

fn unknown_reason(error: &PapiHttpError) -> PapiUnknownReason {
    match error {
        PapiHttpError::Timeout => PapiUnknownReason::Timeout,
        PapiHttpError::Canceled => PapiUnknownReason::Canceled,
        PapiHttpError::Decode | PapiHttpError::ResponseTooLarge => PapiUnknownReason::Decode,
        PapiHttpError::Throttled { .. } => PapiUnknownReason::Throttled,
        PapiHttpError::Server(_) => PapiUnknownReason::Server,
        PapiHttpError::Rejected { .. } => PapiUnknownReason::VenueUncertain,
        PapiHttpError::Authentication { .. }
        | PapiHttpError::Clock
        | PapiHttpError::ListenKeyExpired
        | PapiHttpError::GateClosed
        | PapiHttpError::Sdk
        | PapiHttpError::Budget
        | PapiHttpError::Configuration => PapiUnknownReason::Transport,
    }
}

fn intent_quantity(intent: &PapiSubmitIntent) -> Decimal {
    match intent {
        PapiSubmitIntent::Market { quantity, .. } | PapiSubmitIntent::Limit { quantity, .. } => {
            *quantity
        }
    }
}

fn intent_reduce_only(intent: &PapiSubmitIntent) -> bool {
    match intent {
        PapiSubmitIntent::Market { reduce_only, .. }
        | PapiSubmitIntent::Limit { reduce_only, .. } => *reduce_only,
    }
}

fn risk_reservation_active(operation: &PapiRecoveredOperation, evidence_generation: u64) -> bool {
    if operation.operation.reservation.is_none()
        || !matches!(operation.operation.command, PapiPersistedCommand::Submit(_))
    {
        return false;
    }

    match operation.stage {
        PapiOperationStage::Resolved {
            resolution:
                PapiOperationResolution::Filled
                | PapiOperationResolution::Canceled
                | PapiOperationResolution::Expired,
        } => operation.operation.generation >= evidence_generation,
        PapiOperationStage::Resolved { .. } => false,
        _ => true,
    }
}

fn add_side_quantity(
    side: PapiIntentSide,
    quantity: Decimal,
    reduce_only: bool,
    buy_quantity: &mut Decimal,
    sell_quantity: &mut Decimal,
    reduce_buy_quantity: &mut Decimal,
    reduce_sell_quantity: &mut Decimal,
) -> Result<(), CoordinatorError> {
    let target = match (side, reduce_only) {
        (PapiIntentSide::Buy, false) => buy_quantity,
        (PapiIntentSide::Sell, false) => sell_quantity,
        (PapiIntentSide::Buy, true) => reduce_buy_quantity,
        (PapiIntentSide::Sell, true) => reduce_sell_quantity,
    };
    *target = checked_add(*target, quantity)?;
    Ok(())
}

fn validate_source(source: &'static str) -> Result<(), CoordinatorError> {
    if source.is_empty() {
        Err(CoordinatorError::InvalidEvidence(
            "evidence provenance source must not be empty",
        ))
    } else {
        Ok(())
    }
}

fn decode_ordinary_venue_order_id(value: &str) -> Result<i64, CoordinatorError> {
    let parts: Vec<_> = value.split(':').collect();

    if parts.len() != 4 || parts[0] != "PAPI" || parts[1] != "O" || parts[2].is_empty() {
        return Err(CoordinatorError::ReportIdentityMismatch);
    }
    let id: i64 = parts[3]
        .parse()
        .map_err(|_| CoordinatorError::ReportIdentityMismatch)?;

    if id <= 0 || id.to_string() != parts[3] {
        return Err(CoordinatorError::ReportIdentityMismatch);
    }
    Ok(id)
}

fn report_matches_intent(report: &OrderStatusReport, intent: &PapiSubmitIntent) -> bool {
    let expected_side = match intent_side(intent) {
        PapiIntentSide::Buy => OrderSide::Buy,
        PapiIntentSide::Sell => OrderSide::Sell,
    };

    if report.order_side != Some(expected_side)
        || report.quantity.as_decimal() != intent_quantity(intent)
        || report.reduce_only != intent_reduce_only(intent)
    {
        return false;
    }

    match intent {
        PapiSubmitIntent::Market { .. } => report.order_type == OrderType::Market,
        PapiSubmitIntent::Limit {
            price,
            time_in_force,
            ..
        } => {
            let (expected_tif, post_only) = match time_in_force {
                PapiIntentTimeInForce::Gtc => (TimeInForce::Gtc, false),
                PapiIntentTimeInForce::Ioc => (TimeInForce::Ioc, false),
                PapiIntentTimeInForce::Fok => (TimeInForce::Fok, false),
                PapiIntentTimeInForce::Gtx => (TimeInForce::Gtc, true),
            };
            report.order_type == OrderType::Limit
                && report.time_in_force == expected_tif
                && report.post_only == post_only
                && report
                    .price
                    .is_some_and(|value| value.as_decimal() == *price)
        }
    }
}

fn terminal_recovery_error(error: &PapiHttpError) -> bool {
    matches!(
        error,
        PapiHttpError::Authentication { .. }
            | PapiHttpError::Clock
            | PapiHttpError::Throttled { .. }
            | PapiHttpError::GateClosed
            | PapiHttpError::Canceled
            | PapiHttpError::Configuration
    )
}

fn checked_rate_amount(value: Decimal, basis_points: u32) -> Result<Decimal, CoordinatorError> {
    checked_mul(value, Decimal::from(basis_points))?
        .checked_div(Decimal::from(BASIS_POINTS))
        .ok_or(CoordinatorError::Arithmetic)
}

fn checked_add(lhs: Decimal, rhs: Decimal) -> Result<Decimal, CoordinatorError> {
    lhs.checked_add(rhs).ok_or(CoordinatorError::Arithmetic)
}

fn checked_sub(lhs: Decimal, rhs: Decimal) -> Result<Decimal, CoordinatorError> {
    lhs.checked_sub(rhs).ok_or(CoordinatorError::Arithmetic)
}

fn checked_mul(lhs: Decimal, rhs: Decimal) -> Result<Decimal, CoordinatorError> {
    lhs.checked_mul(rhs).ok_or(CoordinatorError::Arithmetic)
}

fn valid_optional_positive_bounds(minimum: Option<Decimal>, maximum: Option<Decimal>) -> bool {
    minimum.is_none_or(|value| value > Decimal::ZERO)
        && maximum.is_none_or(|value| value > Decimal::ZERO)
        && match (minimum, maximum) {
            (Some(minimum), Some(maximum)) => minimum <= maximum,
            _ => true,
        }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nautilus_core::time::AtomicTime;
    use nautilus_model::{
        enums::{OrderSide, OrderType, TimeInForce},
        identifiers::{StrategyId, TraderId, VenueOrderId},
        types::Quantity,
    };
    use rstest::rstest;
    use rust_decimal_macros::dec;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        testing::{self, MockServer, Reply},
        trading::journal::{PapiOperationResolution, PapiUnknownReason},
    };

    fn instrument_id() -> InstrumentId {
        InstrumentId::from("BTCUSDT-PERP.BINANCE")
    }

    fn config(path: &std::path::Path) -> BinancePapiTradingConfig {
        BinancePapiTradingConfig {
            command_journal_path: path.to_path_buf(),
            risk_currency: Currency::USDT(),
            instrument_limits: vec![BinancePapiInstrumentTradingConfig {
                instrument_id: instrument_id(),
                max_order_quantity: dec!(2),
                max_order_notional: dec!(100000),
                max_position_quantity: dec!(3),
                max_instrument_exposure: dec!(150000),
            }],
            max_account_exposure: dec!(200000),
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

    fn evidence(now: Instant, position: Decimal) -> PapiVerifiedRiskSnapshot {
        PapiVerifiedRiskSnapshot {
            account_id: AccountId::from("BINANCE-PAPI-001"),
            generation: 7,
            observed_at: now,
            collection_span: Duration::from_millis(20),
            status: PapiAccountStatusEvidence::Normal {
                endpoint: "/papi/v1/account",
                generation: 7,
            },
            position_mode: PapiPositionModeEvidence::OneWay {
                endpoint: "/papi/v1/um/positionSide/dual",
                generation: 7,
            },
            units: PapiRiskUnitEvidence {
                currency: Currency::USDT(),
                source: "authenticated PM unit verification",
            },
            margin_rule: PapiMarginRuleEvidence {
                source: "authenticated UM bracket verification",
                generation: 7,
                max_initial_margin_rate: dec!(0.1),
            },
            available_initial_margin: dec!(50000),
            account_exposure: dec!(30000),
            instruments: HashMap::from([(
                instrument_id(),
                PapiVerifiedInstrumentRisk {
                    signed_position_quantity: position,
                    exposure: dec!(30000),
                    reference_price: dec!(30000),
                    price_source: "authenticated mark price",
                    price_generation: 11,
                    price_observed_at: now,
                    rules: PapiVerifiedInstrumentRules {
                        source: "authenticated exchange info and status",
                        generation: 13,
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

    fn advance_evidence_generation(
        mut snapshot: PapiVerifiedRiskSnapshot,
        generation: u64,
    ) -> PapiVerifiedRiskSnapshot {
        snapshot.generation = generation;
        snapshot.status = PapiAccountStatusEvidence::Normal {
            endpoint: "/papi/v1/account",
            generation,
        };
        snapshot.position_mode = PapiPositionModeEvidence::OneWay {
            endpoint: "/papi/v1/um/positionSide/dual",
            generation,
        };
        snapshot.margin_rule.generation = generation;
        snapshot
    }

    fn submit(
        client_order_id: &str,
        side: PapiIntentSide,
        reduce_only: bool,
    ) -> PapiPersistedOperation {
        PapiPersistedOperation {
            operation_id: UUID4::new(),
            account_id: AccountId::from("BINANCE-PAPI-001"),
            strategy_id: StrategyId::from("S-001"),
            instrument_id: instrument_id(),
            client_order_id: ClientOrderId::from(client_order_id),
            generation: 0,
            ts_init: UnixNanos::from(1),
            command: PapiPersistedCommand::Submit(PapiSubmitIntent::Market {
                side,
                quantity: dec!(0.5),
                reduce_only,
            }),
            reservation: None,
        }
    }

    fn limit_submit(
        client_order_id: &str,
        quantity: Decimal,
        price: Decimal,
    ) -> PapiPersistedOperation {
        let mut operation = submit(client_order_id, PapiIntentSide::Buy, false);
        operation.command = PapiPersistedCommand::Submit(PapiSubmitIntent::Limit {
            side: PapiIntentSide::Buy,
            quantity,
            price,
            time_in_force: PapiIntentTimeInForce::Gtc,
            reduce_only: false,
        });
        operation
    }

    fn cancel(client_order_id: &str) -> PapiPersistedOperation {
        PapiPersistedOperation {
            operation_id: UUID4::new(),
            account_id: AccountId::from("BINANCE-PAPI-001"),
            strategy_id: StrategyId::from("S-001"),
            instrument_id: instrument_id(),
            client_order_id: ClientOrderId::from(client_order_id),
            generation: 0,
            ts_init: UnixNanos::from(2),
            command: PapiPersistedCommand::Cancel {
                venue_order_id: None,
            },
            reservation: None,
        }
    }

    fn cancel_all(strategy_id: &str, order_side: Option<OrderSide>) -> CancelAllOrders {
        CancelAllOrders::new(
            TraderId::from("TRADER-001"),
            None,
            StrategyId::from(strategy_id),
            instrument_id(),
            order_side,
            UUID4::new(),
            UnixNanos::from(10),
            None,
            None,
        )
    }

    fn report(client_order_id: &str, status: OrderStatus) -> OrderStatusReport {
        let filled_qty = if status == OrderStatus::Filled {
            Quantity::from("0.5")
        } else {
            Quantity::zero(1)
        };

        OrderStatusReport::new(
            AccountId::from("BINANCE-PAPI-001"),
            instrument_id(),
            Some(ClientOrderId::from(client_order_id)),
            VenueOrderId::from("PAPI:O:BTCUSDT:42"),
            Some(OrderSide::Buy),
            OrderType::Market,
            TimeInForce::Gtc,
            status,
            Quantity::from("0.5"),
            filled_qty,
            UnixNanos::from(2),
            UnixNanos::from(3),
            UnixNanos::from(4),
            None,
        )
    }

    fn mark_unknown(coordinator: &mut PapiCommandCoordinator, operation_id: UUID4) {
        coordinator
            .transition(
                operation_id,
                PapiOperationStage::MayHaveDispatched,
                UnixNanos::from(2),
            )
            .unwrap();
        coordinator
            .transition(
                operation_id,
                PapiOperationStage::Unknown {
                    reason: PapiUnknownReason::Timeout,
                },
                UnixNanos::from(3),
            )
            .unwrap();
    }

    #[rstest]
    fn prepared_reservation_uses_exact_buffered_values() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();

        let reservation = coordinator
            .prepare_submit(submit("O-001", PapiIntentSide::Buy, false), now)
            .unwrap();

        assert_eq!(reservation.quantity, dec!(0.5));
        assert_eq!(reservation.notional, dec!(15150));
        assert_eq!(reservation.exposure, dec!(15165.150));
        assert_eq!(reservation.initial_margin, dec!(1530.150));
        assert_eq!(coordinator.journal().unresolved().count(), 1);
    }

    #[rstest]
    fn verified_instrument_rules_fail_closed_before_persistence() {
        fn prepare(
            snapshot: PapiVerifiedRiskSnapshot,
            operation: PapiPersistedOperation,
        ) -> CoordinatorError {
            let directory = TempDir::new().unwrap();
            let now = snapshot.observed_at;
            let mut coordinator = PapiCommandCoordinator::open(
                config(&directory.path().join("commands.journal")),
                AccountId::from("BINANCE-PAPI-001"),
            )
            .unwrap();
            coordinator.install_evidence(snapshot, now).unwrap();
            let error = coordinator.prepare_submit(operation, now).unwrap_err();
            assert_eq!(coordinator.journal().operations().len(), 0);
            error
        }

        let now = Instant::now();
        let error = prepare(
            evidence(now, Decimal::ZERO),
            limit_submit("BAD-QTY-STEP", dec!(0.5005), dec!(30000)),
        );
        assert!(matches!(error, CoordinatorError::QuantityIncrement));

        let error = prepare(
            evidence(now, Decimal::ZERO),
            limit_submit("BAD-PRICE-TICK", dec!(0.5), dec!(30000.003)),
        );
        assert!(matches!(error, CoordinatorError::PriceIncrement));

        let mut snapshot = evidence(now, Decimal::ZERO);
        snapshot
            .instruments
            .get_mut(&instrument_id())
            .unwrap()
            .rules
            .min_notional = Some(dec!(14900));
        let error = prepare(
            snapshot,
            submit("MARKET-MIN-NOTIONAL", PapiIntentSide::Buy, false),
        );
        assert!(matches!(error, CoordinatorError::InstrumentMinimumNotional));
    }

    #[rstest]
    fn invalid_or_inactive_instrument_rules_reject_the_evidence_generation() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        let mut inactive = evidence(now, Decimal::ZERO);
        inactive
            .instruments
            .get_mut(&instrument_id())
            .unwrap()
            .rules
            .trading = false;
        assert!(matches!(
            coordinator.install_evidence(inactive, now).unwrap_err(),
            CoordinatorError::InstrumentNotTrading(id) if id == instrument_id()
        ));

        let mut wrong_currency = evidence(now, Decimal::ZERO);
        wrong_currency
            .instruments
            .get_mut(&instrument_id())
            .unwrap()
            .rules
            .settlement_currency = Currency::USDC();
        assert!(matches!(
            coordinator
                .install_evidence(wrong_currency, now)
                .unwrap_err(),
            CoordinatorError::InstrumentSettlementCurrency(id) if id == instrument_id()
        ));

        let mut invalid_bounds = evidence(now, Decimal::ZERO);
        let rules = &mut invalid_bounds
            .instruments
            .get_mut(&instrument_id())
            .unwrap()
            .rules;
        rules.min_quantity = Some(dec!(2));
        rules.max_quantity = Some(dec!(1));
        assert!(matches!(
            coordinator
                .install_evidence(invalid_bounds, now)
                .unwrap_err(),
            CoordinatorError::InvalidInstrumentRules(id) if id == instrument_id()
        ));
    }

    #[rstest]
    #[case("matching")]
    #[case("external")]
    #[case("account")]
    #[case("instrument")]
    #[case("quantity")]
    #[case("side")]
    #[case("reduce_only")]
    #[case("venue_id")]
    #[case("prepared")]
    fn recovered_strategy_requires_durable_matching_submission(#[case] scenario: &str) {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        let operation = submit("OWNED", PapiIntentSide::Buy, false);
        let id = operation.operation_id;
        coordinator.prepare_submit(operation, now).unwrap();

        if scenario != "prepared" {
            mark_unknown(&mut coordinator, id);
            coordinator
                .apply_order_report(
                    id,
                    &report("OWNED", OrderStatus::Accepted),
                    UnixNanos::from(4),
                )
                .unwrap();
        }
        let mut observed = report("OWNED", OrderStatus::Accepted);

        match scenario {
            "external" => observed.client_order_id = Some(ClientOrderId::from("FOREIGN")),
            "account" => observed.account_id = AccountId::from("BINANCE-PAPI-OTHER"),
            "instrument" => observed.instrument_id = InstrumentId::from("ETHUSDT-PERP.BINANCE"),
            "quantity" => observed.quantity = Quantity::from("0.6"),
            "side" => observed.order_side = Some(OrderSide::Sell),
            "reduce_only" => observed.reduce_only = true,
            "venue_id" => observed.venue_order_id = VenueOrderId::from("PAPI:O:BTCUSDT:43"),
            _ => {}
        }
        let result = coordinator.recovered_order_strategy(&observed);

        match scenario {
            "matching" => assert_eq!(result.unwrap(), Some(StrategyId::from("S-001"))),
            "external" => assert_eq!(result.unwrap(), None),
            _ => assert!(result.is_err()),
        }
    }

    #[rstest]
    fn unavailable_session_preserves_separate_reduce_only_and_cancel_permissions() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, dec!(1)), now)
            .unwrap();
        let owned = submit("OWNED", PapiIntentSide::Sell, true);
        let id = owned.operation_id;
        coordinator.prepare_submit(owned, now).unwrap();
        mark_unknown(&mut coordinator, id);
        let mut observed = report("OWNED", OrderStatus::Accepted);
        observed.order_side = Some(OrderSide::Sell);
        observed.reduce_only = true;
        coordinator
            .apply_order_report(id, &observed, UnixNanos::from(4))
            .unwrap();
        let session = crate::websocket::BinancePapiAccountSession::new(
            &testing::config("http://127.0.0.1:1"),
            vec![testing::instrument("BTCUSDT")],
        )
        .unwrap();
        coordinator.bind_session(session.application_acknowledger());
        let increase =
            coordinator.check_submit(&submit("INCREASE", PapiIntentSide::Buy, false), now);
        let reduce = coordinator.check_submit(&submit("REDUCE", PapiIntentSide::Sell, true), now);
        let cancel = coordinator.prepare_cancel(cancel("OWNED"), UnixNanos::from(5));
        assert!(matches!(
            increase,
            Err(CoordinatorError::RecoveryRestricted)
        ));
        assert!(reduce.is_ok());
        assert!(cancel.is_ok());
    }

    #[rstest]
    fn duplicate_client_order_id_never_reserves_twice() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        coordinator
            .prepare_submit(submit("O-001", PapiIntentSide::Buy, false), now)
            .unwrap();

        let e = coordinator
            .prepare_submit(submit("O-001", PapiIntentSide::Buy, false), now)
            .unwrap_err();

        assert!(matches!(
            e,
            CoordinatorError::DuplicateClientOrderId(id)
                if id == ClientOrderId::from("O-001")
        ));
        assert_eq!(coordinator.journal().unresolved().count(), 1);
    }

    #[rstest]
    fn restart_with_unresolved_operation_freezes_increase_risk() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("commands.journal");
        let now = Instant::now();
        let operation_id = {
            let mut coordinator =
                PapiCommandCoordinator::open(config(&path), AccountId::from("BINANCE-PAPI-001"))
                    .unwrap();
            coordinator
                .install_evidence(evidence(now, Decimal::ZERO), now)
                .unwrap();
            let operation = submit("O-001", PapiIntentSide::Buy, false);
            let operation_id = operation.operation_id;
            coordinator.prepare_submit(operation, now).unwrap();
            coordinator
                .transition(
                    operation_id,
                    PapiOperationStage::MayHaveDispatched,
                    UnixNanos::from(2),
                )
                .unwrap();
            operation_id
        };
        let mut recovered =
            PapiCommandCoordinator::open(config(&path), AccountId::from("BINANCE-PAPI-001"))
                .unwrap();
        recovered
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();

        assert!(!recovered.permissions(now).increase_risk);
        assert!(matches!(
            recovered
                .prepare_submit(submit("O-002", PapiIntentSide::Buy, false), now)
                .unwrap_err(),
            CoordinatorError::RecoveryRestricted
        ));

        recovered
            .transition(
                operation_id,
                PapiOperationStage::Resolved {
                    resolution: PapiOperationResolution::Canceled,
                },
                UnixNanos::from(3),
            )
            .unwrap();
        assert!(recovered.permissions(now).increase_risk);
    }

    #[rstest]
    #[case("prepared", true)]
    #[case("may_have_dispatched", true)]
    #[case("unknown", true)]
    #[case("observed", true)]
    #[case("resolved", false)]
    fn restart_preserves_restriction_for_every_durable_operation_stage(
        #[case] stage: &str,
        #[case] restricted: bool,
    ) {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("commands.journal");
        let now = Instant::now();

        {
            let mut coordinator =
                PapiCommandCoordinator::open(config(&path), AccountId::from("BINANCE-PAPI-001"))
                    .unwrap();
            coordinator
                .install_evidence(evidence(now, Decimal::ZERO), now)
                .unwrap();
            let operation = submit("O-001", PapiIntentSide::Buy, false);
            let operation_id = operation.operation_id;
            coordinator.prepare_submit(operation, now).unwrap();

            if stage != "prepared" {
                coordinator
                    .transition(
                        operation_id,
                        PapiOperationStage::MayHaveDispatched,
                        UnixNanos::from(2),
                    )
                    .unwrap();
            }

            match stage {
                "prepared" | "may_have_dispatched" => {}
                "unknown" => coordinator
                    .transition(
                        operation_id,
                        PapiOperationStage::Unknown {
                            reason: PapiUnknownReason::Timeout,
                        },
                        UnixNanos::from(3),
                    )
                    .unwrap(),
                "observed" | "resolved" => {
                    coordinator
                        .transition(
                            operation_id,
                            PapiOperationStage::Observed { venue_order_id: 42 },
                            UnixNanos::from(3),
                        )
                        .unwrap();

                    if stage == "resolved" {
                        coordinator
                            .transition(
                                operation_id,
                                PapiOperationStage::Resolved {
                                    resolution: PapiOperationResolution::Canceled,
                                },
                                UnixNanos::from(4),
                            )
                            .unwrap();
                    }
                }
                _ => unreachable!(),
            }
        }

        let mut recovered =
            PapiCommandCoordinator::open(config(&path), AccountId::from("BINANCE-PAPI-001"))
                .unwrap();
        recovered
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();

        assert_eq!(!recovered.permissions(now).increase_risk, restricted);
        assert_eq!(recovered.journal().unresolved().count() > 0, restricted);

        let result = recovered.prepare_submit(submit("O-002", PapiIntentSide::Buy, false), now);
        if restricted {
            assert!(matches!(result, Err(CoordinatorError::RecoveryRestricted)));
        } else {
            result.unwrap();
        }
    }

    #[rstest]
    fn ambiguous_result_freezes_new_increase_risk_and_keeps_reservation() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        let operation = submit("O-001", PapiIntentSide::Buy, false);
        let operation_id = operation.operation_id;
        coordinator.prepare_submit(operation, now).unwrap();
        coordinator
            .transition(
                operation_id,
                PapiOperationStage::MayHaveDispatched,
                UnixNanos::from(2),
            )
            .unwrap();
        coordinator
            .transition(
                operation_id,
                PapiOperationStage::Unknown {
                    reason: PapiUnknownReason::Timeout,
                },
                UnixNanos::from(3),
            )
            .unwrap();

        assert!(!coordinator.permissions(now).increase_risk);
        assert!(
            coordinator.journal().operations()[&operation_id]
                .operation
                .reservation
                .is_some()
        );
    }

    #[rstest]
    fn dispatch_barrier_rechecks_generation_and_resolves_without_dispatch() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        let operation = submit("O-001", PapiIntentSide::Buy, false);
        let operation_id = operation.operation_id;
        coordinator.prepare_submit(operation, now).unwrap();

        let mut replacement = evidence(now, Decimal::ZERO);
        replacement.generation = 8;
        replacement.status = PapiAccountStatusEvidence::Normal {
            endpoint: "/papi/v1/account",
            generation: 8,
        };
        replacement.position_mode = PapiPositionModeEvidence::OneWay {
            endpoint: "/papi/v1/um/positionSide/dual",
            generation: 8,
        };
        replacement.margin_rule.generation = 8;
        coordinator.install_evidence(replacement, now).unwrap();

        assert!(matches!(
            coordinator
                .dispatch_barrier(operation_id, now, UnixNanos::from(2))
                .unwrap_err(),
            CoordinatorError::GenerationMismatch
        ));
        assert_eq!(
            coordinator.journal().operations()[&operation_id].stage,
            PapiOperationStage::Resolved {
                resolution: PapiOperationResolution::NotSent,
            }
        );
        assert_eq!(coordinator.journal().unresolved().count(), 0);
    }

    #[rstest]
    fn command_outcomes_advance_durable_stages_conservatively() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();

        let not_sent = submit("N-001", PapiIntentSide::Buy, false);
        let not_sent_id = not_sent.operation_id;
        coordinator.prepare_submit(not_sent, now).unwrap();
        coordinator
            .apply_command_failure(
                not_sent_id,
                &PapiCommandFailure {
                    classification: CommandFailure::not_sent("budget"),
                    error: PapiHttpError::Budget,
                },
                UnixNanos::from(2),
            )
            .unwrap();

        let rejected = submit("R-001", PapiIntentSide::Buy, false);
        let rejected_id = rejected.operation_id;
        coordinator.prepare_submit(rejected, now).unwrap();
        coordinator
            .dispatch_barrier(rejected_id, now, UnixNanos::from(3))
            .unwrap();
        coordinator
            .apply_command_failure(
                rejected_id,
                &PapiCommandFailure {
                    classification: CommandFailure::venue_rejected("rejected"),
                    error: PapiHttpError::Rejected {
                        status: Some(400),
                        code: Some(-2010),
                    },
                },
                UnixNanos::from(4),
            )
            .unwrap();

        let ambiguous = submit("A-001", PapiIntentSide::Buy, false);
        let ambiguous_id = ambiguous.operation_id;
        coordinator.prepare_submit(ambiguous, now).unwrap();
        coordinator
            .dispatch_barrier(ambiguous_id, now, UnixNanos::from(5))
            .unwrap();
        coordinator
            .apply_command_failure(
                ambiguous_id,
                &PapiCommandFailure {
                    classification: CommandFailure::ambiguous("timeout"),
                    error: PapiHttpError::Timeout,
                },
                UnixNanos::from(6),
            )
            .unwrap();

        assert_eq!(
            coordinator.journal().operations()[&not_sent_id].stage,
            PapiOperationStage::Resolved {
                resolution: PapiOperationResolution::NotSent,
            }
        );
        assert_eq!(
            coordinator.journal().operations()[&rejected_id].stage,
            PapiOperationStage::Resolved {
                resolution: PapiOperationResolution::VenueRejected,
            }
        );
        assert_eq!(
            coordinator.journal().operations()[&ambiguous_id].stage,
            PapiOperationStage::Unknown {
                reason: PapiUnknownReason::Timeout,
            }
        );
        assert!(
            coordinator.journal().operations()[&ambiguous_id]
                .operation
                .reservation
                .is_some()
        );
        assert!(!coordinator.permissions(now).increase_risk);
    }

    #[rstest]
    fn successful_response_is_observed_until_terminal_report() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        let operation = submit("O-001", PapiIntentSide::Buy, false);
        let operation_id = operation.operation_id;
        coordinator.prepare_submit(operation, now).unwrap();
        coordinator
            .dispatch_barrier(operation_id, now, UnixNanos::from(2))
            .unwrap();
        coordinator
            .apply_command_success(
                operation_id,
                &PapiCommandAcknowledgement {
                    venue_order_id: 42,
                    client_order_id: "O-001".to_owned(),
                },
                UnixNanos::from(3),
            )
            .unwrap();

        assert_eq!(
            coordinator.journal().operations()[&operation_id].stage,
            PapiOperationStage::Observed { venue_order_id: 42 }
        );
        assert!(
            coordinator.journal().operations()[&operation_id]
                .operation
                .reservation
                .is_some()
        );
    }

    #[rstest]
    #[case(
        OrderStatus::Accepted,
        PapiOperationStage::Observed { venue_order_id: 42 }
    )]
    #[case(
        OrderStatus::Filled,
        PapiOperationStage::Resolved {
            resolution: PapiOperationResolution::Filled,
        }
    )]
    fn authoritative_report_supersedes_late_successful_response(
        #[case] status: OrderStatus,
        #[case] expected_stage: PapiOperationStage,
    ) {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        let operation = submit("O-001", PapiIntentSide::Buy, false);
        let operation_id = operation.operation_id;
        coordinator.prepare_submit(operation, now).unwrap();
        coordinator
            .dispatch_barrier(operation_id, now, UnixNanos::from(2))
            .unwrap();
        coordinator
            .apply_order_report(operation_id, &report("O-001", status), UnixNanos::from(3))
            .unwrap();

        let result = coordinator
            .apply_command_success(
                operation_id,
                &PapiCommandAcknowledgement {
                    venue_order_id: 42,
                    client_order_id: "O-001".to_owned(),
                },
                UnixNanos::from(4),
            )
            .unwrap();

        assert_eq!(result, CommandSuccessApplication::Superseded);
        assert_eq!(
            coordinator.journal().operations()[&operation_id].stage,
            expected_stage
        );
    }

    #[rstest]
    fn filled_reservation_remains_charged_while_immediate_capacity_is_available() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        let first = submit("O-001", PapiIntentSide::Buy, false);
        let first_id = first.operation_id;
        coordinator.prepare_submit(first, now).unwrap();
        coordinator
            .dispatch_barrier(first_id, now, UnixNanos::from(2))
            .unwrap();
        coordinator
            .apply_order_report(
                first_id,
                &report("O-001", OrderStatus::Filled),
                UnixNanos::from(3),
            )
            .unwrap();

        let recovered = &coordinator.journal().operations()[&first_id];
        assert!(risk_reservation_active(recovered, 7));
        coordinator
            .prepare_submit(submit("O-002", PapiIntentSide::Buy, false), now)
            .unwrap();
    }

    #[rstest]
    fn newer_soft_refresh_releases_terminal_carry_only_after_installation() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut baseline = evidence(now, Decimal::ZERO);
        baseline.available_initial_margin = dec!(2000);
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator.install_evidence(baseline, now).unwrap();
        let first = submit("O-001", PapiIntentSide::Buy, false);
        let first_id = first.operation_id;
        coordinator.prepare_submit(first, now).unwrap();
        coordinator
            .dispatch_barrier(first_id, now, UnixNanos::from(2))
            .unwrap();
        coordinator
            .apply_order_report(
                first_id,
                &report("O-001", OrderStatus::Filled),
                UnixNanos::from(3),
            )
            .unwrap();

        assert!(matches!(
            coordinator
                .prepare_submit(submit("O-002", PapiIntentSide::Buy, false), now)
                .unwrap_err(),
            CoordinatorError::InitialMarginCapacity
        ));

        let mut refreshed = advance_evidence_generation(evidence(now, dec!(0.5)), 8);
        refreshed.available_initial_margin = dec!(2000);
        coordinator
            .install_refresh(refreshed, 11, now, UnixNanos::from(4))
            .unwrap();
        assert!(!risk_reservation_active(
            &coordinator.journal().operations()[&first_id],
            8
        ));
        coordinator
            .prepare_submit(submit("O-002", PapiIntentSide::Buy, false), now)
            .unwrap();
    }

    #[rstest]
    fn order_account_update_is_soft_only_when_active_owned_risk_covers_its_symbols() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        coordinator
            .prepare_submit(submit("O-001", PapiIntentSide::Buy, false), now)
            .unwrap();

        assert!(
            coordinator
                .covers_order_account_update("ORDER", &BTreeSet::from(["BTCUSDT".to_string()]))
        );
        assert!(
            !coordinator.covers_order_account_update(
                "FUNDING_FEE",
                &BTreeSet::from(["BTCUSDT".to_string()])
            )
        );
        assert!(
            !coordinator
                .covers_order_account_update("ORDER", &BTreeSet::from(["ETHUSDT".to_string()]))
        );
    }

    #[rstest]
    fn matching_report_count_retains_owned_identity_after_resolution() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        let operation = submit("O-001", PapiIntentSide::Buy, false);
        let operation_id = operation.operation_id;
        coordinator.prepare_submit(operation, now).unwrap();
        coordinator
            .dispatch_barrier(operation_id, now, UnixNanos::from(2))
            .unwrap();

        let filled = report("O-001", OrderStatus::Filled);
        assert_eq!(
            coordinator
                .apply_matching_order_report(&filled, UnixNanos::from(3))
                .unwrap(),
            1
        );
        assert_eq!(
            coordinator
                .apply_matching_order_report(&filled, UnixNanos::from(4))
                .unwrap(),
            1
        );
        assert_eq!(
            coordinator
                .apply_matching_order_report(
                    &report("EXTERNAL", OrderStatus::Accepted),
                    UnixNanos::from(5),
                )
                .unwrap(),
            0
        );
    }

    #[rstest]
    fn rebaseline_requires_covered_orders_and_atomically_ends_old_reservations() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("commands.journal");
        let now = Instant::now();
        let operation_id = {
            let mut coordinator =
                PapiCommandCoordinator::open(config(&path), AccountId::from("BINANCE-PAPI-001"))
                    .unwrap();
            coordinator
                .install_evidence(evidence(now, Decimal::ZERO), now)
                .unwrap();
            let operation = submit("O-001", PapiIntentSide::Buy, false);
            let operation_id = operation.operation_id;
            coordinator.prepare_submit(operation, now).unwrap();
            coordinator
                .dispatch_barrier(operation_id, now, UnixNanos::from(2))
                .unwrap();
            coordinator
                .apply_command_success(
                    operation_id,
                    &PapiCommandAcknowledgement {
                        venue_order_id: 42,
                        client_order_id: "O-001".to_owned(),
                    },
                    UnixNanos::from(3),
                )
                .unwrap();

            let token = coordinator.begin_rebaseline(10, now).unwrap();
            assert!(!coordinator.permissions(now).increase_risk);
            let missing = advance_evidence_generation(evidence(now, Decimal::ZERO), 8);
            assert!(matches!(
                coordinator
                    .install_rebaseline(token, missing, 11, now, UnixNanos::from(4))
                    .unwrap_err(),
                CoordinatorError::RebaselineCoverage(id)
                    if id == ClientOrderId::from("O-001")
            ));
            assert!(!coordinator.permissions(now).increase_risk);

            let mut covered = advance_evidence_generation(evidence(now, Decimal::ZERO), 8);
            covered.open_orders.push(PapiVerifiedOpenOrder {
                instrument_id: instrument_id(),
                client_order_id: ClientOrderId::from("O-001"),
                venue_order_id: 42,
                side: PapiIntentSide::Buy,
                quantity: dec!(0.5),
                reduce_only: false,
                worst_case_exposure: dec!(15165.150),
            });
            coordinator
                .install_rebaseline(token, covered, 11, now, UnixNanos::from(5))
                .unwrap();
            assert!(coordinator.permissions(now).increase_risk);
            assert_eq!(coordinator.journal().unresolved().count(), 0);
            assert_eq!(
                coordinator.journal().operations()[&operation_id].stage,
                PapiOperationStage::Resolved {
                    resolution: PapiOperationResolution::RebasedAfterReconciliation,
                }
            );
            operation_id
        };

        let journal = PapiCommandJournal::open(&path, AccountId::from("BINANCE-PAPI-001")).unwrap();
        assert_eq!(journal.unresolved().count(), 0);
        assert_eq!(
            journal.operations()[&operation_id].stage,
            PapiOperationStage::Resolved {
                resolution: PapiOperationResolution::RebasedAfterReconciliation,
            }
        );
    }

    #[rstest]
    fn hard_rebaseline_freezes_increase_risk_with_a_pending_operation() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        coordinator
            .prepare_submit(submit("O-001", PapiIntentSide::Buy, false), now)
            .unwrap();

        coordinator.begin_rebaseline(10, now).unwrap();

        assert!(!coordinator.permissions(now).increase_risk);
        assert!(matches!(
            coordinator
                .prepare_submit(submit("O-002", PapiIntentSide::Buy, false), now)
                .unwrap_err(),
            CoordinatorError::RecoveryRestricted
        ));
    }

    #[rstest]
    fn repeated_rebaseline_cannot_reset_position_or_open_order_exposure_limits() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut trading = config(&directory.path().join("commands.journal"));
        trading.instrument_limits[0].max_order_quantity = dec!(0.5);
        trading.instrument_limits[0].max_position_quantity = dec!(1);
        let mut coordinator =
            PapiCommandCoordinator::open(trading, AccountId::from("BINANCE-PAPI-001")).unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();

        let mut active_orders = Vec::new();
        for (index, generation) in [(1_u64, 8_u64), (2_u64, 9_u64)] {
            let client_order_id = format!("O-{index:03}");
            let operation = submit(&client_order_id, PapiIntentSide::Buy, false);
            let operation_id = operation.operation_id;
            coordinator.prepare_submit(operation, now).unwrap();
            coordinator
                .dispatch_barrier(operation_id, now, UnixNanos::from(index + 1))
                .unwrap();
            coordinator
                .apply_command_success(
                    operation_id,
                    &PapiCommandAcknowledgement {
                        venue_order_id: 40 + index as i64,
                        client_order_id: client_order_id.clone(),
                    },
                    UnixNanos::from(index + 2),
                )
                .unwrap();
            active_orders.push(PapiVerifiedOpenOrder {
                instrument_id: instrument_id(),
                client_order_id: ClientOrderId::from(client_order_id),
                venue_order_id: 40 + index as i64,
                side: PapiIntentSide::Buy,
                quantity: dec!(0.5),
                reduce_only: false,
                worst_case_exposure: dec!(15165.150),
            });
            let token = coordinator.begin_rebaseline(index * 10, now).unwrap();
            let mut baseline =
                advance_evidence_generation(evidence(now, Decimal::ZERO), generation);
            baseline.open_orders.clone_from(&active_orders);
            coordinator
                .install_rebaseline(
                    token,
                    baseline,
                    index * 10 + 1,
                    now,
                    UnixNanos::from(index + 3),
                )
                .unwrap();
        }

        assert!(matches!(
            coordinator
                .prepare_submit(submit("O-003", PapiIntentSide::Buy, false), now)
                .unwrap_err(),
            CoordinatorError::PositionLimit
        ));

        let token = coordinator.begin_rebaseline(30, now).unwrap();
        let mut unchanged = advance_evidence_generation(evidence(now, Decimal::ZERO), 10);
        unchanged.open_orders = active_orders;
        coordinator
            .install_rebaseline(token, unchanged, 31, now, UnixNanos::from(10))
            .unwrap();
        assert!(matches!(
            coordinator
                .prepare_submit(submit("O-003", PapiIntentSide::Buy, false), now)
                .unwrap_err(),
            CoordinatorError::PositionLimit
        ));
    }

    #[rstest]
    #[case("price")]
    #[case("rules")]
    fn rebaseline_rejects_instrument_source_generation_rollback(#[case] source: &str) {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        let token = coordinator.begin_rebaseline(10, now).unwrap();
        let mut replacement = advance_evidence_generation(evidence(now, Decimal::ZERO), 8);
        let risk = replacement.instruments.get_mut(&instrument_id()).unwrap();

        match source {
            "price" => risk.price_generation -= 1,
            "rules" => risk.rules.generation -= 1,
            _ => unreachable!(),
        }

        assert!(matches!(
            coordinator
                .install_rebaseline(token, replacement, 11, now, UnixNanos::from(2))
                .unwrap_err(),
            CoordinatorError::InstrumentGenerationRollback(id) if id == instrument_id()
        ));
        assert_eq!(coordinator.evidence.as_ref().unwrap().generation, 7);
        assert!(!coordinator.permissions(now).increase_risk);
    }

    #[rstest]
    fn rebaseline_cannot_replace_evidence_installed_after_attempt_started() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        let token = coordinator.begin_rebaseline(10, now).unwrap();
        coordinator
            .install_evidence(
                advance_evidence_generation(evidence(now, Decimal::ZERO), 9),
                now,
            )
            .unwrap();

        let older = advance_evidence_generation(evidence(now, Decimal::ZERO), 8);
        assert!(matches!(
            coordinator
                .install_rebaseline(token, older, 11, now, UnixNanos::from(2))
                .unwrap_err(),
            CoordinatorError::EvidenceGenerationNotIncreasing
        ));
        assert_eq!(coordinator.evidence.as_ref().unwrap().generation, 9);
        assert!(!coordinator.permissions(now).increase_risk);
    }

    #[tokio::test]
    async fn durable_submit_dispatch_maps_valid_acknowledgement_to_observed() {
        let server = MockServer::new(|request| {
            Reply::json(&serde_json::json!({
                "symbol": request.params["symbol"],
                "orderId": 42,
                "clientOrderId": request.params["newClientOrderId"],
            }))
        })
        .await;
        let http = PapiHttpClient::new(
            &testing::config(&server.url),
            testing::gate(),
            Arc::new(AtomicTime::default()),
        )
        .unwrap();
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        let operation = submit("O-001", PapiIntentSide::Buy, false);
        let operation_id = operation.operation_id;
        coordinator.prepare_submit(operation, now).unwrap();

        let response = coordinator
            .dispatch_submit(
                operation_id,
                &http,
                &RequestBudget::new(Duration::from_secs(5), 10, 10).unwrap(),
                &CancellationToken::new(),
            )
            .await
            .unwrap();

        assert_eq!(response.acknowledgement.venue_order_id, 42);
        assert_eq!(server.requests().len(), 1);
        assert_eq!(
            coordinator.journal().operations()[&operation_id].stage,
            PapiOperationStage::Observed { venue_order_id: 42 }
        );
    }

    #[tokio::test]
    async fn durable_cancel_dispatch_maps_valid_acknowledgement_to_observed() {
        let server = MockServer::new(|request| {
            Reply::json(&serde_json::json!({
                "symbol": request.params["symbol"],
                "orderId": 42,
                "clientOrderId": request.params["origClientOrderId"],
            }))
        })
        .await;
        let http = PapiHttpClient::new(
            &testing::config(&server.url),
            testing::gate(),
            Arc::new(AtomicTime::default()),
        )
        .unwrap();
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        let prepared_submit = submit("O-001", PapiIntentSide::Buy, false);
        let submit_id = prepared_submit.operation_id;
        coordinator.prepare_submit(prepared_submit, now).unwrap();
        coordinator
            .transition(
                submit_id,
                PapiOperationStage::MayHaveDispatched,
                UnixNanos::from(2),
            )
            .unwrap();
        let cancel = cancel("O-001");
        let cancel_id = cancel.operation_id;
        coordinator
            .prepare_cancel(cancel, UnixNanos::from(3))
            .unwrap();

        let response = coordinator
            .dispatch_cancel(
                cancel_id,
                &http,
                &RequestBudget::new(Duration::from_secs(5), 10, 10).unwrap(),
                &CancellationToken::new(),
            )
            .await
            .unwrap();

        assert_eq!(response.acknowledgement.venue_order_id, 42);
        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "DELETE");
        assert_eq!(requests[0].params["origClientOrderId"], "O-001");
        assert_eq!(
            coordinator.journal().operations()[&cancel_id].stage,
            PapiOperationStage::Observed { venue_order_id: 42 }
        );
    }

    #[tokio::test]
    async fn cancel_batch_dispatches_fixed_targets_with_independent_outcomes() {
        let server = MockServer::new(|request| {
            let client_order_id = &request.params["origClientOrderId"];
            let venue_order_id = if client_order_id == "O-001" { 41 } else { 42 };
            Reply::json(&serde_json::json!({
                "symbol": request.params["symbol"],
                "orderId": venue_order_id,
                "clientOrderId": client_order_id,
            }))
        })
        .await;
        let http = PapiHttpClient::new(
            &testing::config(&server.url),
            testing::gate(),
            Arc::new(AtomicTime::default()),
        )
        .unwrap();
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();

        let mut cancel_ids = Vec::new();
        for (index, client_order_id) in ["O-001", "O-002"].into_iter().enumerate() {
            let submit = submit(client_order_id, PapiIntentSide::Buy, false);
            let submit_id = submit.operation_id;
            coordinator.prepare_submit(submit, now).unwrap();
            coordinator
                .transition(
                    submit_id,
                    PapiOperationStage::MayHaveDispatched,
                    UnixNanos::from(index as u64 + 2),
                )
                .unwrap();
            let cancel = cancel(client_order_id);
            let cancel_id = cancel.operation_id;
            coordinator
                .prepare_cancel(cancel, UnixNanos::from(index as u64 + 4))
                .unwrap();
            cancel_ids.push(cancel_id);
        }

        let outcomes = coordinator
            .dispatch_cancel_batch(
                &cancel_ids,
                &http,
                &RequestBudget::new(Duration::from_secs(5), 10, 10).unwrap(),
                &CancellationToken::new(),
            )
            .await
            .unwrap();

        assert_eq!(outcomes.len(), 2);
        assert_eq!(outcomes[0].0, cancel_ids[0]);
        assert_eq!(outcomes[1].0, cancel_ids[1]);
        assert!(outcomes.iter().all(|(_, outcome)| outcome.is_ok()));
        let requests = server.requests();
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(|request| request.method == "DELETE"));
        assert_eq!(requests[0].params["origClientOrderId"], "O-001");
        assert_eq!(requests[1].params["origClientOrderId"], "O-002");
        assert_eq!(
            coordinator.journal().operations()[&cancel_ids[0]].stage,
            PapiOperationStage::Observed { venue_order_id: 41 }
        );
        assert_eq!(
            coordinator.journal().operations()[&cancel_ids[1]].stage,
            PapiOperationStage::Observed { venue_order_id: 42 }
        );
    }

    #[tokio::test]
    async fn invalid_cancel_batch_is_rejected_before_dispatch() {
        let server = MockServer::new(|_| Reply::raw(500, "unexpected request")).await;
        let http = PapiHttpClient::new(
            &testing::config(&server.url),
            testing::gate(),
            Arc::new(AtomicTime::default()),
        )
        .unwrap();
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        let submit = submit("O-001", PapiIntentSide::Buy, false);
        let submit_id = submit.operation_id;
        coordinator.prepare_submit(submit, now).unwrap();
        coordinator
            .transition(
                submit_id,
                PapiOperationStage::MayHaveDispatched,
                UnixNanos::from(2),
            )
            .unwrap();
        let cancel = cancel("O-001");
        let cancel_id = cancel.operation_id;
        coordinator
            .prepare_cancel(cancel, UnixNanos::from(3))
            .unwrap();
        let budget = RequestBudget::new(Duration::from_secs(5), 10, 10).unwrap();
        let cancellation = CancellationToken::new();

        for operation_ids in [Vec::new(), vec![cancel_id, cancel_id], vec![submit_id]] {
            assert!(matches!(
                coordinator
                    .dispatch_cancel_batch(&operation_ids, &http, &budget, &cancellation)
                    .await
                    .unwrap_err(),
                CoordinatorError::InvalidCancelBatch
            ));
        }

        coordinator
            .transition(
                cancel_id,
                PapiOperationStage::MayHaveDispatched,
                UnixNanos::from(4),
            )
            .unwrap();
        assert!(matches!(
            coordinator
                .dispatch_cancel_batch(&[cancel_id], &http, &budget, &cancellation)
                .await
                .unwrap_err(),
            CoordinatorError::InvalidCancelBatch
        ));
        assert!(server.requests().is_empty());
    }

    #[tokio::test]
    async fn mismatched_success_identity_becomes_unknown_and_retains_reservation() {
        let server = MockServer::new(|request| {
            Reply::json(&serde_json::json!({
                "symbol": request.params["symbol"],
                "orderId": 42,
                "clientOrderId": "another-order",
            }))
        })
        .await;
        let http = PapiHttpClient::new(
            &testing::config(&server.url),
            testing::gate(),
            Arc::new(AtomicTime::default()),
        )
        .unwrap();
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        let operation = submit("O-001", PapiIntentSide::Buy, false);
        let operation_id = operation.operation_id;
        coordinator.prepare_submit(operation, now).unwrap();

        let error = coordinator
            .dispatch_submit(
                operation_id,
                &http,
                &RequestBudget::new(Duration::from_secs(5), 10, 10).unwrap(),
                &CancellationToken::new(),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            CoordinatorDispatchError::Command(PapiCommandFailure {
                classification: CommandFailure::Ambiguous(_),
                error: PapiHttpError::Decode,
            })
        ));
        assert_eq!(server.requests().len(), 1);
        assert_eq!(
            coordinator.journal().operations()[&operation_id].stage,
            PapiOperationStage::Unknown {
                reason: PapiUnknownReason::Decode,
            }
        );
        assert!(
            coordinator.journal().operations()[&operation_id]
                .operation
                .reservation
                .is_some()
        );
    }

    #[rstest]
    fn reduce_only_requires_fresh_directional_position_and_remaining_quantity() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, dec!(0.75)), now)
            .unwrap();

        let reservation = coordinator
            .prepare_submit(submit("R-001", PapiIntentSide::Sell, true), now)
            .unwrap();
        let e = coordinator
            .prepare_submit(submit("R-002", PapiIntentSide::Sell, true), now)
            .unwrap_err();
        let wrong_side = coordinator
            .prepare_submit(submit("R-003", PapiIntentSide::Buy, true), now)
            .unwrap_err();

        assert!(matches!(e, CoordinatorError::ReduceOnlyQuantity));
        assert!(matches!(wrong_side, CoordinatorError::NotReducing));
        assert_eq!(reservation.exposure, Decimal::ZERO);
        assert_eq!(reservation.initial_margin, Decimal::ZERO);
        assert!(coordinator.permissions(now).verified_reduce_only);
    }

    #[rstest]
    fn in_flight_position_exposure_and_margin_limits_fail_before_persistence() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let path = directory.path().join("in-flight.journal");
        let mut in_flight =
            PapiCommandCoordinator::open(config(&path), AccountId::from("BINANCE-PAPI-001"))
                .unwrap();
        in_flight
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();

        for index in 0..4 {
            in_flight
                .prepare_submit(
                    submit(&format!("O-{index}"), PapiIntentSide::Buy, false),
                    now,
                )
                .unwrap();
        }
        assert!(matches!(
            in_flight
                .prepare_submit(submit("O-4", PapiIntentSide::Buy, false), now)
                .unwrap_err(),
            CoordinatorError::InFlightLimit
        ));
        assert_eq!(in_flight.journal().unresolved().count(), 4);

        let mut position_config = config(&directory.path().join("position.journal"));
        position_config.instrument_limits[0].max_order_quantity = dec!(0.75);
        position_config.instrument_limits[0].max_position_quantity = dec!(0.75);
        let mut position =
            PapiCommandCoordinator::open(position_config, AccountId::from("BINANCE-PAPI-001"))
                .unwrap();
        position
            .install_evidence(evidence(now, dec!(0.5)), now)
            .unwrap();
        assert!(matches!(
            position
                .prepare_submit(submit("P-1", PapiIntentSide::Buy, false), now)
                .unwrap_err(),
            CoordinatorError::PositionLimit
        ));
        assert_eq!(position.journal().unresolved().count(), 0);

        let mut exposure_config = config(&directory.path().join("exposure.journal"));
        exposure_config.instrument_limits[0].max_order_notional = dec!(40000);
        exposure_config.instrument_limits[0].max_instrument_exposure = dec!(40000);
        let mut exposure =
            PapiCommandCoordinator::open(exposure_config, AccountId::from("BINANCE-PAPI-001"))
                .unwrap();
        exposure
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        assert!(matches!(
            exposure
                .prepare_submit(submit("E-1", PapiIntentSide::Buy, false), now)
                .unwrap_err(),
            CoordinatorError::InstrumentExposureLimit
        ));
        assert_eq!(exposure.journal().unresolved().count(), 0);

        let mut account_config = config(&directory.path().join("account.journal"));
        account_config.max_account_exposure = dec!(150000);
        let mut account_evidence = evidence(now, Decimal::ZERO);
        account_evidence.account_exposure = dec!(140000);
        let mut account =
            PapiCommandCoordinator::open(account_config, AccountId::from("BINANCE-PAPI-001"))
                .unwrap();
        account.install_evidence(account_evidence, now).unwrap();
        assert!(matches!(
            account
                .prepare_submit(submit("A-1", PapiIntentSide::Buy, false), now)
                .unwrap_err(),
            CoordinatorError::AccountExposureLimit
        ));
        assert_eq!(account.journal().unresolved().count(), 0);

        let mut margin_evidence = evidence(now, Decimal::ZERO);
        margin_evidence.available_initial_margin = dec!(1000);
        let mut margin = PapiCommandCoordinator::open(
            config(&directory.path().join("margin.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        margin.install_evidence(margin_evidence, now).unwrap();
        assert!(matches!(
            margin
                .prepare_submit(submit("M-1", PapiIntentSide::Buy, false), now)
                .unwrap_err(),
            CoordinatorError::InitialMarginCapacity
        ));
        assert_eq!(margin.journal().unresolved().count(), 0);
    }

    #[rstest]
    fn targeted_cancel_revokes_prepared_submit_or_deduplicates_dispatched_target() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        let prepared_submit = submit("O-001", PapiIntentSide::Buy, false);
        let submit_id = prepared_submit.operation_id;
        coordinator.prepare_submit(prepared_submit, now).unwrap();

        let local = coordinator
            .prepare_cancel(cancel("O-001"), UnixNanos::from(2))
            .unwrap();
        assert_eq!(
            local,
            PapiCancelPreparation::LocalSubmitCanceled {
                submit_operation_id: submit_id,
            }
        );
        assert!(matches!(
            coordinator.journal().operations()[&submit_id].stage,
            PapiOperationStage::Resolved {
                resolution: PapiOperationResolution::NotSent
            }
        ));

        let dispatched = submit("O-002", PapiIntentSide::Buy, false);
        let dispatched_id = dispatched.operation_id;
        coordinator.prepare_submit(dispatched, now).unwrap();
        coordinator
            .transition(
                dispatched_id,
                PapiOperationStage::MayHaveDispatched,
                UnixNanos::from(3),
            )
            .unwrap();
        let prepared = coordinator
            .prepare_cancel(cancel("O-002"), UnixNanos::from(4))
            .unwrap();
        assert!(matches!(prepared, PapiCancelPreparation::Prepared { .. }));

        assert!(coordinator.permissions(now).targeted_cancel);
        assert!(matches!(
            coordinator
                .prepare_cancel(cancel("O-002"), UnixNanos::from(5))
                .unwrap_err(),
            CoordinatorError::DuplicateCancelTarget(id)
                if id == ClientOrderId::from("O-002")
        ));
    }

    #[rstest]
    fn cancel_all_planner_freezes_only_owned_scoped_ordinary_targets() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut snapshot = evidence(now, Decimal::ZERO);
        snapshot.open_orders.push(PapiVerifiedOpenOrder {
            instrument_id: instrument_id(),
            client_order_id: ClientOrderId::from("EXTERNAL-001"),
            venue_order_id: 99,
            side: PapiIntentSide::Buy,
            quantity: dec!(0.5),
            reduce_only: false,
            worst_case_exposure: dec!(15165.150),
        });
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator.install_evidence(snapshot, now).unwrap();

        assert!(matches!(
            coordinator
                .plan_cancel_all(&cancel_all("S-001", None))
                .unwrap_err(),
            CoordinatorError::EmptyCancelAll
        ));
        assert!(matches!(
            coordinator
                .prepare_cancel(cancel("EXTERNAL-001"), UnixNanos::from(2))
                .unwrap_err(),
            CoordinatorError::UnknownCancelTarget(id)
                if id == ClientOrderId::from("EXTERNAL-001")
        ));

        let buy = submit("O-001", PapiIntentSide::Buy, false);
        let buy_id = buy.operation_id;
        coordinator.prepare_submit(buy, now).unwrap();
        coordinator
            .transition(
                buy_id,
                PapiOperationStage::MayHaveDispatched,
                UnixNanos::from(3),
            )
            .unwrap();

        let sell = submit("O-002", PapiIntentSide::Sell, false);
        let sell_id = sell.operation_id;
        coordinator.prepare_submit(sell, now).unwrap();
        coordinator
            .transition(
                sell_id,
                PapiOperationStage::MayHaveDispatched,
                UnixNanos::from(4),
            )
            .unwrap();
        coordinator
            .apply_command_success(
                sell_id,
                &PapiCommandAcknowledgement {
                    venue_order_id: 52,
                    client_order_id: "O-002".to_owned(),
                },
                UnixNanos::from(5),
            )
            .unwrap();

        coordinator
            .prepare_submit(submit("O-LOCAL", PapiIntentSide::Buy, false), now)
            .unwrap();
        let mut other_strategy = submit("O-OTHER", PapiIntentSide::Buy, false);
        other_strategy.strategy_id = StrategyId::from("S-002");
        let other_strategy_id = other_strategy.operation_id;
        coordinator.prepare_submit(other_strategy, now).unwrap();
        coordinator
            .transition(
                other_strategy_id,
                PapiOperationStage::MayHaveDispatched,
                UnixNanos::from(6),
            )
            .unwrap();

        let buy_targets = coordinator
            .plan_cancel_all(&cancel_all("S-001", Some(OrderSide::Buy)))
            .unwrap();
        assert_eq!(
            buy_targets
                .iter()
                .map(|operation| operation.client_order_id.as_str())
                .collect::<Vec<_>>(),
            ["O-001", "O-LOCAL"]
        );
        assert!(buy_targets.iter().all(|operation| {
            operation.account_id == AccountId::from("BINANCE-PAPI-001")
                && operation.strategy_id == StrategyId::from("S-001")
                && operation.instrument_id == instrument_id()
                && matches!(
                    operation.command,
                    PapiPersistedCommand::Cancel {
                        venue_order_id: None
                    }
                )
        }));

        let sell_targets = coordinator
            .plan_cancel_all(&cancel_all("S-001", Some(OrderSide::Sell)))
            .unwrap();
        assert_eq!(sell_targets.len(), 1);
        assert_eq!(
            sell_targets[0].client_order_id,
            ClientOrderId::from("O-002")
        );
        assert!(matches!(
            sell_targets[0].command,
            PapiPersistedCommand::Cancel {
                venue_order_id: Some(52)
            }
        ));

        let mut unsupported = cancel_all("S-001", None);
        unsupported.params = Some(serde_json::from_str(r#"{"unsupported":true}"#).unwrap());
        assert!(matches!(
            coordinator.plan_cancel_all(&unsupported).unwrap_err(),
            CoordinatorError::InvalidCancelAll
        ));
    }

    #[rstest]
    fn recovery_reports_advance_only_authoritative_matching_facts() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        let operation = submit("O-001", PapiIntentSide::Buy, false);
        let operation_id = operation.operation_id;
        coordinator.prepare_submit(operation, now).unwrap();

        assert!(matches!(
            coordinator
                .apply_order_report(
                    operation_id,
                    &report("O-001", OrderStatus::Accepted),
                    UnixNanos::from(3),
                )
                .unwrap_err(),
            CoordinatorError::ReportBeforeDispatch
        ));
        coordinator
            .transition(
                operation_id,
                PapiOperationStage::MayHaveDispatched,
                UnixNanos::from(4),
            )
            .unwrap();
        coordinator
            .apply_order_report(
                operation_id,
                &report("O-001", OrderStatus::Accepted),
                UnixNanos::from(5),
            )
            .unwrap();
        assert!(matches!(
            coordinator.journal().operations()[&operation_id].stage,
            PapiOperationStage::Observed { venue_order_id: 42 }
        ));

        let mut mismatched = report("OTHER", OrderStatus::Filled);
        mismatched.client_order_id = Some(ClientOrderId::from("OTHER"));
        assert!(matches!(
            coordinator
                .apply_order_report(operation_id, &mismatched, UnixNanos::from(6))
                .unwrap_err(),
            CoordinatorError::ReportIdentityMismatch
        ));
        coordinator
            .apply_order_report(
                operation_id,
                &report("O-001", OrderStatus::Filled),
                UnixNanos::from(7),
            )
            .unwrap();
        assert!(matches!(
            coordinator.journal().operations()[&operation_id].stage,
            PapiOperationStage::Resolved {
                resolution: PapiOperationResolution::Filled
            }
        ));
    }

    #[tokio::test]
    async fn bounded_recovery_applies_found_order_without_replaying_submit() {
        let server = MockServer::new(|request| {
            if request.path == "/papi/v1/um/order" {
                let mut order = testing::order();
                order["clientOrderId"] = serde_json::json!("O-001");
                order["origQty"] = serde_json::json!("0.5");
                order["side"] = serde_json::json!("BUY");
                order["type"] = serde_json::json!("MARKET");
                order["origType"] = serde_json::json!("MARKET");
                order["price"] = serde_json::json!("0");
                Reply::json(&order)
            } else {
                testing::quiet(request)
            }
        })
        .await;
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut trading = config(&directory.path().join("commands.journal"));
        trading.max_recovery_rounds = 1;
        trading.recovery_recheck_interval_ms = 1;
        let mut coordinator =
            PapiCommandCoordinator::open(trading, AccountId::from("BINANCE-PAPI-001")).unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        let operation = submit("O-001", PapiIntentSide::Buy, false);
        let operation_id = operation.operation_id;
        coordinator.prepare_submit(operation, now).unwrap();
        mark_unknown(&mut coordinator, operation_id);

        let summary = coordinator
            .recover_unknowns(&testing::client(&server, &["BTCUSDT"]))
            .await
            .unwrap();

        assert_eq!(summary.rounds, 1);
        assert_eq!(summary.reports_applied, 1);
        assert_eq!(summary.unresolved, 1);
        assert!(!summary.budget_exhausted);
        assert!(matches!(
            coordinator.journal().operations()[&operation_id].stage,
            PapiOperationStage::Observed {
                venue_order_id: 270_093_109
            }
        ));
        assert_eq!(server.requests().len(), 2);
        assert!(
            server
                .requests()
                .iter()
                .all(|request| request.method == "GET")
        );
    }

    #[tokio::test]
    async fn bounded_recovery_keeps_repeated_not_found_ambiguous() {
        let server = MockServer::new(testing::quiet).await;
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut trading = config(&directory.path().join("commands.journal"));
        trading.max_recovery_rounds = 2;
        trading.recovery_recheck_interval_ms = 1;
        let mut coordinator =
            PapiCommandCoordinator::open(trading, AccountId::from("BINANCE-PAPI-001")).unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        let operation = submit("O-001", PapiIntentSide::Buy, false);
        let operation_id = operation.operation_id;
        coordinator.prepare_submit(operation, now).unwrap();
        mark_unknown(&mut coordinator, operation_id);

        let summary = coordinator
            .recover_unknowns(&testing::client(&server, &["BTCUSDT"]))
            .await
            .unwrap();

        assert_eq!(summary.rounds, 2);
        assert_eq!(summary.reports_applied, 0);
        assert_eq!(summary.unresolved, 1);
        assert!(!summary.budget_exhausted);
        assert!(matches!(
            coordinator.journal().operations()[&operation_id].stage,
            PapiOperationStage::Unknown {
                reason: PapiUnknownReason::Timeout
            }
        ));
        assert_eq!(server.requests().len(), 4);
        assert!(
            server
                .requests()
                .iter()
                .all(|request| request.method == "GET")
        );
    }

    #[rstest]
    fn evidence_replacement_requires_a_newer_complete_generation() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();

        let same = advance_evidence_generation(evidence(now, Decimal::ZERO), 7);
        assert!(matches!(
            coordinator.install_evidence(same, now).unwrap_err(),
            CoordinatorError::EvidenceGenerationNotIncreasing
        ));

        let older = advance_evidence_generation(evidence(now, Decimal::ZERO), 6);
        assert!(matches!(
            coordinator.install_evidence(older, now).unwrap_err(),
            CoordinatorError::EvidenceGenerationNotIncreasing
        ));
        assert_eq!(coordinator.evidence.as_ref().unwrap().generation, 7);

        let newer = advance_evidence_generation(evidence(now, Decimal::ZERO), 8);
        coordinator.install_evidence(newer, now).unwrap();
        assert_eq!(coordinator.evidence.as_ref().unwrap().generation, 8);
    }

    #[rstest]
    #[case("price")]
    #[case("rules")]
    fn evidence_replacement_rejects_instrument_source_generation_rollback(#[case] source: &str) {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        coordinator
            .install_evidence(evidence(now, Decimal::ZERO), now)
            .unwrap();
        let mut replacement = advance_evidence_generation(evidence(now, Decimal::ZERO), 8);
        let risk = replacement.instruments.get_mut(&instrument_id()).unwrap();

        match source {
            "price" => risk.price_generation -= 1,
            "rules" => risk.rules.generation -= 1,
            _ => unreachable!(),
        }

        assert!(matches!(
            coordinator
                .install_evidence(replacement, now)
                .unwrap_err(),
            CoordinatorError::InstrumentGenerationRollback(id) if id == instrument_id()
        ));
        assert_eq!(coordinator.evidence.as_ref().unwrap().generation, 7);
        assert!(coordinator.permissions(now).increase_risk);
    }

    #[rstest]
    fn stale_mismatched_or_incomplete_evidence_fails_closed() {
        let directory = TempDir::new().unwrap();
        let now = Instant::now();
        let mut coordinator = PapiCommandCoordinator::open(
            config(&directory.path().join("commands.journal")),
            AccountId::from("BINANCE-PAPI-001"),
        )
        .unwrap();
        let mut stale = evidence(
            now.checked_sub(Duration::from_secs(11)).unwrap(),
            Decimal::ZERO,
        );
        stale
            .instruments
            .get_mut(&instrument_id())
            .unwrap()
            .price_observed_at = now;
        let mut mismatched = evidence(now, Decimal::ZERO);
        mismatched.margin_rule.generation = 8;
        let mut incomplete = evidence(now, Decimal::ZERO);
        incomplete.instruments.clear();

        assert!(matches!(
            coordinator.install_evidence(stale, now).unwrap_err(),
            CoordinatorError::StaleEvidence
        ));
        assert!(matches!(
            coordinator.install_evidence(mismatched, now).unwrap_err(),
            CoordinatorError::GenerationMismatch
        ));
        assert!(matches!(
            coordinator.install_evidence(incomplete, now).unwrap_err(),
            CoordinatorError::MissingInstrumentEvidence(id) if id == instrument_id()
        ));
        assert!(matches!(
            coordinator
                .prepare_submit(submit("O-001", PapiIntentSide::Buy, false), now)
                .unwrap_err(),
            CoordinatorError::MissingEvidence
        ));
    }
}
