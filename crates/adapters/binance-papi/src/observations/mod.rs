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

//! Offline, account-scoped observations for later REST integration.
//!
//! Parsing and receipt freshness do not establish economic validity, historical
//! completeness, an atomic exchange snapshot, or authority for order admission.

pub(crate) mod fields;
pub(crate) mod models;

#[cfg(test)]
mod tests;

use std::{collections::BTreeSet, time::Duration};

use nautilus_common::live::dst::time::Instant;
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::AccountId;
use serde::Serialize;
use serde_json::value::RawValue;

use self::models::{AccountSummary, AssetBalance, JsonObject, UmAccount};

// Bounds retained evidence and parsing allocations for each account response
pub(crate) const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// The source includes the exact endpoint version and requested asset coverage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) enum ObservationSource {
    Balance { asset: Option<String> },
    Account,
    UmAccountV1,
    UmAccountV2,
}

impl ObservationSource {
    #[must_use]
    pub(crate) const fn endpoint(&self) -> &'static str {
        match self {
            Self::Balance { .. } => "/papi/v1/balance",
            Self::Account => "/papi/v1/account",
            Self::UmAccountV1 => "/papi/v1/um/account",
            Self::UmAccountV2 => "/papi/v2/um/account",
        }
    }
}

/// A parsed response retains the complete JSON as evidence, including unknown fields.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct AccountObservation {
    pub(crate) account_id: AccountId,
    pub(crate) source: ObservationSource,
    pub(crate) generation: u64,
    pub(crate) ts_received: UnixNanos,
    raw: Box<RawValue>,
    pub(crate) data: ObservationData,
}

/// One slot belongs to one account and source; partial refreshes cannot replace other sources.
#[derive(Debug)]
pub(crate) struct ObservationSlot {
    account_id: AccountId,
    source: ObservationSource,
    last_response: Option<AccountObservation>,
    received_at: Option<Instant>,
    refresh_failed: bool,
}

impl ObservationSlot {
    #[must_use]
    pub(crate) const fn new(account_id: AccountId, source: ObservationSource) -> Self {
        Self {
            account_id,
            source,
            last_response: None,
            received_at: None,
            refresh_failed: false,
        }
    }

    /// Records a successful HTTP response before SDK optional fields lose wire distinctions.
    ///
    /// Transport errors must use `record_failure`. Invalid JSON, envelope shape, or identity
    /// fails the refresh and preserves the previous response. Malformed scalar fields remain
    /// classified as invalid observations; consumers must require and validate their inputs.
    pub(crate) fn record_response(
        &mut self,
        body: &str,
        generation: u64,
        ts_received: UnixNanos,
        received_at: Instant,
    ) -> anyhow::Result<()> {
        self.refresh_failed = true;

        anyhow::ensure!(
            body.len() <= MAX_RESPONSE_BYTES,
            "PAPI observation response exceeds size limit"
        );

        if self
            .last_response
            .as_ref()
            .is_some_and(|last| generation <= last.generation)
            || self.received_at.is_some_and(|last| received_at < last)
        {
            anyhow::bail!("Out-of-order PAPI observation");
        }

        let raw = serde_json::from_str(body)
            .map_err(|_| anyhow::anyhow!("Invalid PAPI response JSON"))?;
        let data = parse_response(&self.source, body)?;

        self.last_response = Some(AccountObservation {
            account_id: self.account_id,
            source: self.source.clone(),
            generation,
            ts_received,
            raw,
            data,
        });
        self.received_at = Some(received_at);
        self.refresh_failed = false;
        Ok(())
    }

    /// Retains the last parsed response for inspection after any failed refresh.
    pub(crate) fn record_failure(&mut self) {
        self.refresh_failed = true;
    }

    #[must_use]
    pub(crate) fn last_response(&self) -> Option<&AccountObservation> {
        self.last_response.as_ref()
    }

    /// Measures receipt age only. Even `Recent` is not proof of fresh venue risk data.
    #[must_use]
    pub(crate) fn receipt_status(&self, now: Instant, max_age: Duration) -> ReceiptStatus {
        if self.refresh_failed {
            return ReceiptStatus::Failed;
        }

        match self.received_at {
            None => ReceiptStatus::Missing,
            Some(received_at) => match now.checked_duration_since(received_at) {
                Some(age) if age <= max_age => ReceiptStatus::Recent,
                _ => ReceiptStatus::Stale,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReceiptStatus {
    Missing,
    Recent,
    Failed,
    Stale,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) enum ObservationData {
    Balances(Vec<AssetBalance>),
    Account(Box<AccountSummary>),
    UmAccount(UmAccount),
}

fn parse_response(source: &ObservationSource, body: &str) -> anyhow::Result<ObservationData> {
    match source {
        ObservationSource::Balance { asset } => {
            // Dispatch before Serde buffering so RawValue preserves exact field text
            let rows: Vec<AssetBalance> = match body.trim_start().as_bytes().first() {
                Some(b'[') => serde_json::from_str::<Vec<JsonObject<AssetBalance>>>(body)
                    .map(|rows| rows.into_iter().map(|row| row.0).collect())
                    .map_err(|_| anyhow::anyhow!("Invalid PAPI balance array"))?,
                Some(b'{') => {
                    anyhow::ensure!(
                        asset.is_some(),
                        "Unscoped PAPI balance response is not an array"
                    );
                    let JsonObject(row) = serde_json::from_str(body)
                        .map_err(|_| anyhow::anyhow!("Invalid PAPI balance object"))?;
                    vec![row]
                }
                _ => anyhow::bail!("Invalid PAPI balance response shape"),
            };

            validate_identities(rows.iter().map(|row| row.asset.as_str()))?;

            if let Some(asset) = asset {
                validate_identity(asset)?;
                anyhow::ensure!(
                    rows.len() == 1 && rows[0].asset == *asset,
                    "PAPI balance response does not match requested asset"
                );
            }

            Ok(ObservationData::Balances(rows))
        }
        ObservationSource::Account => {
            let JsonObject(summary): JsonObject<AccountSummary> = serde_json::from_str(body)
                .map_err(|_| anyhow::anyhow!("Invalid PAPI account response"))?;

            // An error object or empty object is not a successful account observation
            anyhow::ensure!(
                !matches!(summary.account_status, fields::Field::Missing),
                "PAPI account response has no accountStatus field"
            );
            Ok(ObservationData::Account(Box::new(summary)))
        }
        ObservationSource::UmAccountV1 | ObservationSource::UmAccountV2 => {
            let JsonObject(account): JsonObject<UmAccount> = serde_json::from_str(body)
                .map_err(|_| anyhow::anyhow!("Invalid PAPI UM account response"))?;
            validate_identities(account.assets.iter().map(|row| row.asset.as_str()))?;
            let mut positions = BTreeSet::new();

            for position in &account.positions {
                validate_identity(&position.symbol)?;
                validate_identity(&position.position_side)?;
                anyhow::ensure!(
                    positions.insert((&position.symbol, &position.position_side)),
                    "Duplicate PAPI position identity"
                );
            }

            Ok(ObservationData::UmAccount(account))
        }
    }
}

fn validate_identities<'a>(identities: impl Iterator<Item = &'a str>) -> anyhow::Result<()> {
    let mut seen = BTreeSet::new();

    for identity in identities {
        validate_identity(identity)?;
        anyhow::ensure!(seen.insert(identity), "Duplicate PAPI asset identity");
    }

    Ok(())
}

fn validate_identity(identity: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !identity.is_empty()
            && !identity
                .chars()
                .any(|c| c.is_whitespace() || c.is_control()),
        "Invalid PAPI observation identity"
    );
    Ok(())
}
