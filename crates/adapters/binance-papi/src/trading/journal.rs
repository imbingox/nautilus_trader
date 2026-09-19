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

//! Checksummed append-only command journal with synchronous durability barriers.

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    identifiers::{AccountId, ClientOrderId, InstrumentId, StrategyId},
    types::Currency,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const FORMAT_VERSION: u16 = 2;
const FRAME_MAGIC: [u8; 8] = *b"PAPIJNL1";
const FRAME_HEADER_LEN: usize = FRAME_MAGIC.len() + size_of::<u32>() + blake3::OUT_LEN;
const MAX_RECORD_BYTES: usize = 1_048_576;

/// Exact risk amount reserved before an operation can be dispatched.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct PapiReservation {
    pub(crate) risk_currency: Currency,
    pub(crate) quantity: Decimal,
    pub(crate) notional: Decimal,
    pub(crate) exposure: Decimal,
    pub(crate) initial_margin: Decimal,
}

/// Supported ordinary UM submission persisted independently of HTTP encoding.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PapiSubmitIntent {
    Market {
        side: PapiIntentSide,
        quantity: Decimal,
        reduce_only: bool,
    },
    Limit {
        side: PapiIntentSide,
        quantity: Decimal,
        price: Decimal,
        time_in_force: PapiIntentTimeInForce,
        reduce_only: bool,
    },
}

/// Side retained in a durable PAPI command intent.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PapiIntentSide {
    Buy,
    Sell,
}

/// Time-in-force retained in a durable PAPI command intent.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PapiIntentTimeInForce {
    Gtc,
    Ioc,
    Fok,
    Gtx,
}

/// Full operation identity and command evidence written before dispatch.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct PapiPersistedOperation {
    pub(crate) operation_id: UUID4,
    pub(crate) account_id: AccountId,
    pub(crate) strategy_id: StrategyId,
    pub(crate) instrument_id: InstrumentId,
    pub(crate) client_order_id: ClientOrderId,
    pub(crate) generation: u64,
    pub(crate) ts_init: UnixNanos,
    pub(crate) command: PapiPersistedCommand,
    pub(crate) reservation: Option<PapiReservation>,
}

/// Supported command kinds retained for deterministic recovery.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PapiPersistedCommand {
    Submit(PapiSubmitIntent),
    Cancel { venue_order_id: Option<i64> },
}

/// Conservative reason why a dispatched operation needs recovery.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PapiUnknownReason {
    Transport,
    Timeout,
    Canceled,
    VenueUncertain,
    Decode,
    Throttled,
    Server,
}

/// Reliable terminal evidence for an operation.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PapiOperationResolution {
    NotSent,
    VenueRejected,
    ProvedAbsent,
    Canceled,
    Expired,
    Filled,
    RebasedAfterReconciliation,
}

/// Durable operation stage. This is not a Nautilus order state.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PapiOperationStage {
    Prepared,
    MayHaveDispatched,
    Unknown { reason: PapiUnknownReason },
    Observed { venue_order_id: i64 },
    Resolved { resolution: PapiOperationResolution },
}

/// Latest recovered state for one durable operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PapiRecoveredOperation {
    pub(crate) operation: PapiPersistedOperation,
    pub(crate) stage: PapiOperationStage,
}

/// Append-only journal whose successful mutations are durable before returning.
#[derive(Debug)]
pub(crate) struct PapiCommandJournal {
    path: PathBuf,
    file: File,
    account_id: AccountId,
    next_sequence: u64,
    operations: HashMap<UUID4, PapiRecoveredOperation>,
    poisoned: bool,
}

impl PapiCommandJournal {
    /// Opens an account-bound journal and repairs only a provably incomplete trailing frame.
    pub(crate) fn open(
        path: impl AsRef<Path>,
        account_id: AccountId,
    ) -> Result<Self, JournalError> {
        let path = path.as_ref();
        validate_path(path)?;
        let existed = path.exists();

        if existed && std::fs::symlink_metadata(path)?.file_type().is_symlink() {
            return Err(JournalError::InvalidPath(
                "PAPI command journal must not be a symbolic link",
            ));
        }

        let mut options = OpenOptions::new();
        options.read(true).append(true);

        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }

        let mut file = if existed {
            options.open(path)?
        } else {
            options.create_new(true).open(path)?
        };
        file.try_lock().map_err(|_| JournalError::Locked)?;

        if !existed {
            sync_parent(path)?;
        }

        let mut bytes = Vec::new();
        file.seek(SeekFrom::Start(0))?;
        file.read_to_end(&mut bytes)?;
        let (records, valid_len) = decode_frames(&bytes)?;

        if valid_len != bytes.len() {
            file.set_len(u64::try_from(valid_len).map_err(|_| JournalError::RecordTooLarge)?)?;
            file.sync_all()?;
        }

        let mut journal = Self {
            path: path.to_path_buf(),
            file,
            account_id,
            next_sequence: 0,
            operations: HashMap::new(),
            poisoned: false,
        };

        if records.is_empty() {
            journal.append_event(JournalEvent::Header {
                format_version: FORMAT_VERSION,
                account_id,
            })?;
        } else {
            journal.replay(records)?;
        }
        Ok(journal)
    }

    /// Durably records a validated operation before any dispatch can occur.
    pub(crate) fn append_prepared(
        &mut self,
        operation: PapiPersistedOperation,
    ) -> Result<(), JournalError> {
        self.ensure_writable()?;

        if operation.account_id != self.account_id {
            return Err(JournalError::AccountMismatch);
        }

        if self.operations.contains_key(&operation.operation_id) {
            return Err(JournalError::DuplicateOperation(operation.operation_id));
        }

        self.append_event(JournalEvent::Prepared {
            operation: operation.clone(),
        })?;
        self.operations.insert(
            operation.operation_id,
            PapiRecoveredOperation {
                operation,
                stage: PapiOperationStage::Prepared,
            },
        );
        Ok(())
    }

    /// Durably advances one operation through an allowed conservative transition.
    pub(crate) fn transition(
        &mut self,
        operation_id: UUID4,
        stage: PapiOperationStage,
        ts_event: UnixNanos,
    ) -> Result<(), JournalError> {
        self.ensure_writable()?;
        let current = self
            .operations
            .get(&operation_id)
            .ok_or(JournalError::MissingOperation(operation_id))?;

        if !valid_transition(&current.stage, &stage) {
            return Err(JournalError::InvalidTransition {
                from: current.stage.clone(),
                to: stage,
            });
        }

        self.append_event(JournalEvent::Transition {
            operation_id,
            stage: stage.clone(),
            ts_event,
        })?;
        self.operations
            .get_mut(&operation_id)
            .ok_or(JournalError::MissingOperation(operation_id))?
            .stage = stage;
        Ok(())
    }

    /// Atomically ends reservations covered by one reconciled risk baseline.
    pub(crate) fn rebaseline(
        &mut self,
        operation_ids: &[UUID4],
        evidence_generation: u64,
        applied_fact_version: u64,
        ts_event: UnixNanos,
    ) -> Result<(), JournalError> {
        self.ensure_writable()?;

        if operation_ids.is_empty() || evidence_generation == 0 || applied_fact_version == 0 {
            return Err(JournalError::InvalidRebaseline);
        }

        for operation_id in operation_ids {
            let current = self
                .operations
                .get(operation_id)
                .ok_or(JournalError::MissingOperation(*operation_id))?;

            if !matches!(current.stage, PapiOperationStage::Observed { .. })
                || !matches!(current.operation.command, PapiPersistedCommand::Submit(_))
            {
                return Err(JournalError::InvalidRebaseline);
            }
        }

        self.append_event(JournalEvent::Rebaseline {
            operation_ids: operation_ids.to_vec(),
            evidence_generation,
            applied_fact_version,
            ts_event,
        })?;

        for operation_id in operation_ids {
            self.operations
                .get_mut(operation_id)
                .ok_or(JournalError::MissingOperation(*operation_id))?
                .stage = PapiOperationStage::Resolved {
                resolution: PapiOperationResolution::RebasedAfterReconciliation,
            };
        }
        Ok(())
    }

    pub(crate) fn operations(&self) -> &HashMap<UUID4, PapiRecoveredOperation> {
        &self.operations
    }

    pub(crate) fn unresolved(&self) -> impl Iterator<Item = &PapiRecoveredOperation> {
        self.operations
            .values()
            .filter(|operation| !matches!(operation.stage, PapiOperationStage::Resolved { .. }))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn account_id(&self) -> AccountId {
        self.account_id
    }

    fn replay(&mut self, records: Vec<JournalRecord>) -> Result<(), JournalError> {
        for record in records {
            if record.sequence != self.next_sequence {
                return Err(JournalError::Corrupt {
                    offset: 0,
                    reason: "non-contiguous record sequence",
                });
            }

            match record.event {
                JournalEvent::Header {
                    format_version,
                    account_id,
                } => {
                    if record.sequence != 0 || format_version != FORMAT_VERSION {
                        return Err(JournalError::UnsupportedFormat(format_version));
                    }

                    if account_id != self.account_id {
                        return Err(JournalError::AccountMismatch);
                    }
                }
                JournalEvent::Prepared { operation } => {
                    if record.sequence == 0 || operation.account_id != self.account_id {
                        return Err(JournalError::AccountMismatch);
                    }

                    if self.operations.contains_key(&operation.operation_id) {
                        return Err(JournalError::DuplicateOperation(operation.operation_id));
                    }
                    self.operations.insert(
                        operation.operation_id,
                        PapiRecoveredOperation {
                            operation,
                            stage: PapiOperationStage::Prepared,
                        },
                    );
                }
                JournalEvent::Transition {
                    operation_id,
                    stage,
                    ts_event: _,
                } => {
                    let current = self
                        .operations
                        .get_mut(&operation_id)
                        .ok_or(JournalError::MissingOperation(operation_id))?;

                    if !valid_transition(&current.stage, &stage) {
                        return Err(JournalError::InvalidTransition {
                            from: current.stage.clone(),
                            to: stage,
                        });
                    }
                    current.stage = stage;
                }
                JournalEvent::Rebaseline {
                    operation_ids,
                    evidence_generation,
                    applied_fact_version,
                    ts_event: _,
                } => {
                    if operation_ids.is_empty()
                        || evidence_generation == 0
                        || applied_fact_version == 0
                    {
                        return Err(JournalError::InvalidRebaseline);
                    }

                    for operation_id in &operation_ids {
                        let current = self
                            .operations
                            .get(operation_id)
                            .ok_or(JournalError::MissingOperation(*operation_id))?;

                        if !matches!(current.stage, PapiOperationStage::Observed { .. })
                            || !matches!(current.operation.command, PapiPersistedCommand::Submit(_))
                        {
                            return Err(JournalError::InvalidRebaseline);
                        }
                    }

                    for operation_id in operation_ids {
                        self.operations
                            .get_mut(&operation_id)
                            .ok_or(JournalError::MissingOperation(operation_id))?
                            .stage = PapiOperationStage::Resolved {
                            resolution: PapiOperationResolution::RebasedAfterReconciliation,
                        };
                    }
                }
            }
            self.next_sequence = self
                .next_sequence
                .checked_add(1)
                .ok_or(JournalError::SequenceExhausted)?;
        }
        Ok(())
    }

    fn append_event(&mut self, event: JournalEvent) -> Result<(), JournalError> {
        self.ensure_writable()?;
        let next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(JournalError::SequenceExhausted)?;
        let record = JournalRecord {
            sequence: self.next_sequence,
            event,
        };
        let payload = serde_json::to_vec(&record)?;

        if payload.len() > MAX_RECORD_BYTES {
            return Err(JournalError::RecordTooLarge);
        }

        let frame = encode_frame(&payload)?;
        let result = self
            .file
            .write_all(&frame)
            .and_then(|()| self.file.sync_all());

        if let Err(e) = result {
            self.poisoned = true;
            return Err(JournalError::Io(e));
        }

        self.next_sequence = next_sequence;
        Ok(())
    }

    fn ensure_writable(&self) -> Result<(), JournalError> {
        if self.poisoned {
            Err(JournalError::Poisoned)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct JournalRecord {
    sequence: u64,
    event: JournalEvent,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum JournalEvent {
    Header {
        format_version: u16,
        account_id: AccountId,
    },
    Prepared {
        operation: PapiPersistedOperation,
    },
    Transition {
        operation_id: UUID4,
        stage: PapiOperationStage,
        ts_event: UnixNanos,
    },
    Rebaseline {
        operation_ids: Vec<UUID4>,
        evidence_generation: u64,
        applied_fact_version: u64,
        ts_event: UnixNanos,
    },
}

/// Durable journal failures. Errors never include command payloads or credentials.
#[derive(Debug, Error)]
pub(crate) enum JournalError {
    #[error("Invalid PAPI command journal path: {0}")]
    InvalidPath(&'static str),
    #[error("PAPI command journal is already locked by another owner")]
    Locked,
    #[error("PAPI command journal belongs to another account")]
    AccountMismatch,
    #[error("Unsupported PAPI command journal format version {0}")]
    UnsupportedFormat(u16),
    #[error("Corrupt PAPI command journal frame at byte {offset}: {reason}")]
    Corrupt { offset: usize, reason: &'static str },
    #[error("PAPI command journal record exceeds the 1 MiB limit")]
    RecordTooLarge,
    #[error("PAPI command journal sequence is exhausted")]
    SequenceExhausted,
    #[error("PAPI command journal already contains operation {0}")]
    DuplicateOperation(UUID4),
    #[error("PAPI command journal does not contain operation {0}")]
    MissingOperation(UUID4),
    #[error("Invalid PAPI command journal transition from {from:?} to {to:?}")]
    InvalidTransition {
        from: PapiOperationStage,
        to: PapiOperationStage,
    },
    #[error("Invalid PAPI command journal rebaseline")]
    InvalidRebaseline,
    #[error("PAPI command journal is poisoned after an uncertain durability result")]
    Poisoned,
    #[error("PAPI command journal I/O failed")]
    Io(#[from] std::io::Error),
    #[error("PAPI command journal encoding failed")]
    Encoding(#[from] serde_json::Error),
}

fn validate_path(path: &Path) -> Result<(), JournalError> {
    if !path.is_absolute() {
        return Err(JournalError::InvalidPath("path must be absolute"));
    }

    let parent = path.parent().ok_or(JournalError::InvalidPath(
        "path must have a parent directory",
    ))?;
    let metadata = std::fs::metadata(parent).map_err(JournalError::Io)?;

    if !metadata.is_dir() {
        return Err(JournalError::InvalidPath(
            "parent must be an existing directory",
        ));
    }

    if path.file_name().is_none() {
        return Err(JournalError::InvalidPath("path must name a file"));
    }
    Ok(())
}

fn sync_parent(path: &Path) -> Result<(), JournalError> {
    #[cfg(unix)]
    {
        let parent = path.parent().ok_or(JournalError::InvalidPath(
            "path must have a parent directory",
        ))?;
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn encode_frame(payload: &[u8]) -> Result<Vec<u8>, JournalError> {
    let length = u32::try_from(payload.len()).map_err(|_| JournalError::RecordTooLarge)?;
    let checksum = blake3::hash(payload);
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    frame.extend_from_slice(&FRAME_MAGIC);
    frame.extend_from_slice(&length.to_le_bytes());
    frame.extend_from_slice(checksum.as_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn decode_frames(bytes: &[u8]) -> Result<(Vec<JournalRecord>, usize), JournalError> {
    let mut records = Vec::new();
    let mut offset = 0;

    while offset < bytes.len() {
        let remaining = &bytes[offset..];

        if remaining.len() < FRAME_HEADER_LEN {
            break;
        }

        if remaining[..FRAME_MAGIC.len()] != FRAME_MAGIC {
            return Err(JournalError::Corrupt {
                offset,
                reason: "invalid frame magic",
            });
        }

        let length_offset = FRAME_MAGIC.len();
        let length_bytes: [u8; size_of::<u32>()] = remaining
            [length_offset..length_offset + size_of::<u32>()]
            .try_into()
            .map_err(|_| JournalError::Corrupt {
                offset,
                reason: "invalid frame length",
            })?;
        let length = usize::try_from(u32::from_le_bytes(length_bytes))
            .map_err(|_| JournalError::RecordTooLarge)?;

        if length > MAX_RECORD_BYTES {
            return Err(JournalError::RecordTooLarge);
        }

        let frame_len = FRAME_HEADER_LEN
            .checked_add(length)
            .ok_or(JournalError::RecordTooLarge)?;

        if remaining.len() < frame_len {
            break;
        }

        let checksum_offset = length_offset + size_of::<u32>();
        let payload_offset = checksum_offset + blake3::OUT_LEN;
        let expected = &remaining[checksum_offset..payload_offset];
        let payload = &remaining[payload_offset..frame_len];

        if blake3::hash(payload).as_bytes() != expected {
            return Err(JournalError::Corrupt {
                offset,
                reason: "frame checksum mismatch",
            });
        }

        let record = serde_json::from_slice(payload).map_err(|_| JournalError::Corrupt {
            offset,
            reason: "invalid record payload",
        })?;
        records.push(record);
        offset = offset
            .checked_add(frame_len)
            .ok_or(JournalError::RecordTooLarge)?;
    }
    Ok((records, offset))
}

fn valid_transition(from: &PapiOperationStage, to: &PapiOperationStage) -> bool {
    matches!(
        (from, to),
        (
            PapiOperationStage::Prepared,
            PapiOperationStage::MayHaveDispatched
                | PapiOperationStage::Resolved {
                    resolution: PapiOperationResolution::NotSent,
                },
        ) | (
            PapiOperationStage::MayHaveDispatched,
            PapiOperationStage::Unknown { .. }
                | PapiOperationStage::Observed { .. }
                | PapiOperationStage::Resolved {
                    resolution: PapiOperationResolution::VenueRejected
                        | PapiOperationResolution::ProvedAbsent
                        | PapiOperationResolution::Canceled
                        | PapiOperationResolution::Expired
                        | PapiOperationResolution::Filled,
                },
        ) | (
            PapiOperationStage::Unknown { .. },
            PapiOperationStage::Observed { .. }
                | PapiOperationStage::Resolved {
                    resolution: PapiOperationResolution::VenueRejected
                        | PapiOperationResolution::ProvedAbsent
                        | PapiOperationResolution::Canceled
                        | PapiOperationResolution::Expired
                        | PapiOperationResolution::Filled,
                },
        ) | (
            PapiOperationStage::Observed { .. },
            PapiOperationStage::Resolved {
                resolution: PapiOperationResolution::Canceled
                    | PapiOperationResolution::Expired
                    | PapiOperationResolution::Filled,
            },
        )
    )
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use nautilus_model::types::Currency;
    use rstest::rstest;
    use rust_decimal_macros::dec;
    use tempfile::TempDir;

    use super::*;

    fn operation(account_id: AccountId) -> PapiPersistedOperation {
        PapiPersistedOperation {
            operation_id: UUID4::new(),
            account_id,
            strategy_id: StrategyId::from("S-001"),
            instrument_id: InstrumentId::from("BTCUSDT-PERP.BINANCE"),
            client_order_id: ClientOrderId::from("strategy/A:1"),
            generation: 7,
            ts_init: UnixNanos::from(1_000_000_001),
            command: PapiPersistedCommand::Submit(PapiSubmitIntent::Limit {
                side: PapiIntentSide::Buy,
                quantity: dec!(0.0100),
                price: dec!(28511.2300),
                time_in_force: PapiIntentTimeInForce::Gtx,
                reduce_only: false,
            }),
            reservation: Some(PapiReservation {
                risk_currency: Currency::USDT(),
                quantity: dec!(0.0100),
                notional: dec!(285.11230000),
                exposure: dec!(285.39741230),
                initial_margin: dec!(28.79662700),
            }),
        }
    }

    fn journal_path(directory: &TempDir) -> PathBuf {
        directory.path().join("papi-commands.journal")
    }

    #[rstest]
    fn journal_round_trip_preserves_exact_intent_and_unresolved_state() {
        let directory = TempDir::new().unwrap();
        let path = journal_path(&directory);
        let account_id = AccountId::from("BINANCE-PAPI-001");
        let operation = operation(account_id);
        let operation_id = operation.operation_id;

        {
            let mut journal = PapiCommandJournal::open(&path, account_id).unwrap();
            journal.append_prepared(operation.clone()).unwrap();
            journal
                .transition(
                    operation_id,
                    PapiOperationStage::MayHaveDispatched,
                    UnixNanos::from(1_000_000_002),
                )
                .unwrap();
            journal
                .transition(
                    operation_id,
                    PapiOperationStage::Unknown {
                        reason: PapiUnknownReason::Timeout,
                    },
                    UnixNanos::from(1_000_000_003),
                )
                .unwrap();
        }

        let journal = PapiCommandJournal::open(&path, account_id).unwrap();
        assert_eq!(journal.path(), path);
        assert_eq!(journal.operations().len(), 1);
        assert_eq!(journal.unresolved().count(), 1);
        assert_eq!(journal.operations()[&operation_id].operation, operation);
        assert_eq!(
            journal.operations()[&operation_id].stage,
            PapiOperationStage::Unknown {
                reason: PapiUnknownReason::Timeout,
            }
        );
    }

    #[rstest]
    fn incomplete_trailing_frame_is_removed_before_new_appends() {
        let directory = TempDir::new().unwrap();
        let path = journal_path(&directory);
        let account_id = AccountId::from("BINANCE-PAPI-001");
        let operation = operation(account_id);

        {
            let mut journal = PapiCommandJournal::open(&path, account_id).unwrap();
            journal.append_prepared(operation).unwrap();
        }

        let valid_len = std::fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&FRAME_MAGIC[..3])
            .unwrap();

        let journal = PapiCommandJournal::open(&path, account_id).unwrap();
        assert_eq!(journal.operations().len(), 1);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_len);
    }

    #[rstest]
    fn checksum_corruption_fails_closed() {
        let directory = TempDir::new().unwrap();
        let path = journal_path(&directory);
        let account_id = AccountId::from("BINANCE-PAPI-001");

        {
            let mut journal = PapiCommandJournal::open(&path, account_id).unwrap();
            journal.append_prepared(operation(account_id)).unwrap();
        }

        let mut bytes = std::fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        std::fs::write(&path, bytes).unwrap();
        let e = PapiCommandJournal::open(&path, account_id).unwrap_err();
        assert!(matches!(e, JournalError::Corrupt { .. }));
    }

    #[rstest]
    fn prior_development_format_fails_closed() {
        let directory = TempDir::new().unwrap();
        let path = journal_path(&directory);
        let account_id = AccountId::from("BINANCE-PAPI-001");
        let payload = serde_json::to_vec(&JournalRecord {
            sequence: 0,
            event: JournalEvent::Header {
                format_version: 1,
                account_id,
            },
        })
        .unwrap();
        std::fs::write(&path, encode_frame(&payload).unwrap()).unwrap();

        let error = PapiCommandJournal::open(&path, account_id).unwrap_err();
        assert!(matches!(error, JournalError::UnsupportedFormat(1)));
    }

    #[rstest]
    fn journal_is_bound_to_one_account() {
        let directory = TempDir::new().unwrap();
        let path = journal_path(&directory);
        let first = AccountId::from("BINANCE-PAPI-001");
        drop(PapiCommandJournal::open(&path, first).unwrap());

        let e = PapiCommandJournal::open(&path, AccountId::from("BINANCE-PAPI-002")).unwrap_err();
        assert!(matches!(e, JournalError::AccountMismatch));
    }

    #[rstest]
    fn invalid_transition_is_rejected_without_an_append() {
        let directory = TempDir::new().unwrap();
        let path = journal_path(&directory);
        let account_id = AccountId::from("BINANCE-PAPI-001");
        let operation = operation(account_id);
        let operation_id = operation.operation_id;
        let mut journal = PapiCommandJournal::open(&path, account_id).unwrap();
        journal.append_prepared(operation).unwrap();
        let len = std::fs::metadata(&path).unwrap().len();

        let e = journal
            .transition(
                operation_id,
                PapiOperationStage::Resolved {
                    resolution: PapiOperationResolution::Filled,
                },
                UnixNanos::from(1_000_000_002),
            )
            .unwrap_err();
        assert!(matches!(e, JournalError::InvalidTransition { .. }));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), len);
    }

    #[rstest]
    fn duplicate_operation_is_rejected() {
        let directory = TempDir::new().unwrap();
        let path = journal_path(&directory);
        let account_id = AccountId::from("BINANCE-PAPI-001");
        let operation = operation(account_id);
        let mut journal = PapiCommandJournal::open(&path, account_id).unwrap();
        journal.append_prepared(operation.clone()).unwrap();
        let e = journal.append_prepared(operation).unwrap_err();
        assert!(matches!(e, JournalError::DuplicateOperation(_)));
    }

    #[rstest]
    fn second_owner_cannot_open_the_same_journal() {
        let directory = TempDir::new().unwrap();
        let path = journal_path(&directory);
        let account_id = AccountId::from("BINANCE-PAPI-001");
        let _first = PapiCommandJournal::open(&path, account_id).unwrap();
        let e = PapiCommandJournal::open(&path, account_id).unwrap_err();
        assert!(matches!(e, JournalError::Locked));
    }

    #[rstest]
    fn relative_and_symlink_paths_are_rejected() {
        let account_id = AccountId::from("BINANCE-PAPI-001");
        let e = PapiCommandJournal::open("relative.journal", account_id).unwrap_err();
        assert!(matches!(e, JournalError::InvalidPath(_)));

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let directory = TempDir::new().unwrap();
            let target = directory.path().join("target");
            std::fs::write(&target, []).unwrap();
            let link = directory.path().join("link");
            symlink(target, &link).unwrap();
            let e = PapiCommandJournal::open(link, account_id).unwrap_err();
            assert!(matches!(e, JournalError::InvalidPath(_)));
        }
    }
}
