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

//! Exact position handoff bound to an immutable portfolio ownership context.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::PathBuf,
};

use nautilus_common::cache::Cache;
use nautilus_model::{
    identifiers::{AccountId, InstrumentId, StrategyId},
    orders::Order,
    reports::{ExecutionMassStatus, PositionStatusReport},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PapiPositionHandoffConfig {
    /// Durable receipt initialized only by the verified portfolio context owner.
    pub path: PathBuf,
    /// Immutable explicit handoff, including source shutdown and resolved prior orders.
    pub manifest: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: u8,
    handoff_id: String,
    identity: String,
    context_id: String,
    strategy_id: StrategyId,
    launcher_id: String,
    execution_account_id: AccountId,
    source_owner: String,
    source_stopped: bool,
    source_orders_resolved: bool,
    confirmed_at_ns: u64,
    positions: Vec<Position>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Position {
    instrument_id: InstrumentId,
    signed_quantity: Decimal,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    format: u8,
    manifest: Value,
    applied: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    payload: String,
    checksum: String,
}

impl PapiPositionHandoffConfig {
    pub(crate) fn require_applied(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.read()?.applied,
            "Position handoff has not been applied by the engine"
        );
        Ok(())
    }

    pub(crate) fn evidence(&self) -> Value {
        match self.read() {
            Ok(state) => serde_json::json!({"configured": true, "applied": state.applied,
                "handoff_id": self.manifest.get("handoff_id"), "error": null}),
            Err(e) => {
                serde_json::json!({"configured": true, "applied": false, "error": e.to_string()})
            }
        }
    }
    pub(crate) fn validate(
        &self,
        account: AccountId,
        scope: &[InstrumentId],
    ) -> anyhow::Result<()> {
        let manifest = self.manifest()?;
        anyhow::ensure!(
            self.path.is_absolute(),
            "Position handoff receipt path must be absolute"
        );
        anyhow::ensure!(
            manifest.format == 1 && manifest.execution_account_id == account,
            "Position handoff account or format mismatch"
        );
        anyhow::ensure!(
            manifest.source_stopped
                && manifest.source_orders_resolved
                && manifest.confirmed_at_ns > 0
                && !manifest.strategy_id.is_external(),
            "Position handoff requires a stopped source, resolved orders and stable strategy"
        );
        for value in [
            &manifest.handoff_id,
            &manifest.identity,
            &manifest.context_id,
            &manifest.launcher_id,
            &manifest.source_owner,
        ] {
            anyhow::ensure!(
                !value.trim().is_empty() && value.len() <= 256,
                "Position handoff identity is missing or invalid"
            );
        }
        let declared: std::collections::BTreeSet<_> = manifest
            .positions
            .iter()
            .map(|row| row.instrument_id)
            .collect();
        anyhow::ensure!(
            declared.len() == manifest.positions.len()
                && declared.len() == scope.len()
                && scope.iter().all(|id| declared.contains(id)),
            "Position handoff scope mismatch"
        );
        self.read()?;
        Ok(())
    }

    fn manifest(&self) -> anyhow::Result<Manifest> {
        Ok(serde_json::from_value(self.manifest.clone())?)
    }

    fn read(&self) -> anyhow::Result<State> {
        anyhow::ensure!(
            !fs::symlink_metadata(&self.path)?.file_type().is_symlink(),
            "Position handoff receipt must not be a symlink"
        );
        let envelope: Envelope = serde_json::from_slice(&fs::read(&self.path)?)?;
        anyhow::ensure!(
            blake3::hash(envelope.payload.as_bytes()).to_hex().as_str() == envelope.checksum,
            "Position handoff receipt checksum mismatch"
        );
        let state: State = serde_json::from_str(&envelope.payload)?;
        anyhow::ensure!(
            state.format == 1 && state.manifest == self.manifest,
            "Position handoff immutable context mismatch"
        );
        Ok(state)
    }

    pub(crate) fn strategy_for(
        &self,
        report: &PositionStatusReport,
    ) -> anyhow::Result<Option<StrategyId>> {
        let manifest = self.manifest()?;
        anyhow::ensure!(
            report.account_id == manifest.execution_account_id
                && report.venue_position_id.is_none(),
            "Position handoff report account or position mode mismatch"
        );
        let position = manifest
            .positions
            .iter()
            .find(|row| row.instrument_id == report.instrument_id)
            .ok_or_else(|| anyhow::anyhow!("Position handoff report outside declared scope"))?;
        let state = self.read()?;
        anyhow::ensure!(
            state.applied || position.signed_quantity == report.signed_decimal_qty,
            "Position handoff initial quantity mismatch for {}",
            report.instrument_id
        );
        Ok(Some(manifest.strategy_id))
    }

    pub(crate) fn verify_applied(
        &self,
        cache: &Cache,
        mass: &ExecutionMassStatus,
    ) -> anyhow::Result<()> {
        let manifest = self.manifest()?;
        anyhow::ensure!(
            mass.account_id == manifest.execution_account_id,
            "Position handoff application account mismatch"
        );
        anyhow::ensure!(
            cache
                .orders_open_refs(None, None, None, Some(&manifest.execution_account_id), None)
                .iter()
                .all(|order| order.strategy_id() == manifest.strategy_id),
            "Position handoff contains unowned open orders"
        );
        let reports = mass.position_reports();
        for expected in &manifest.positions {
            let rows = reports.get(&expected.instrument_id).ok_or_else(|| {
                anyhow::anyhow!(
                    "Position handoff application missing scope {}",
                    expected.instrument_id
                )
            })?;
            anyhow::ensure!(
                rows.len() == 1,
                "Position handoff requires one net position report"
            );
            let report = &rows[0];
            self.strategy_for(report)?;
            let positions = cache.positions_open(
                None,
                Some(&expected.instrument_id),
                None,
                Some(&manifest.execution_account_id),
                None,
            );
            anyhow::ensure!(
                positions
                    .iter()
                    .all(|position| position.strategy_id == manifest.strategy_id),
                "Position handoff contains unowned engine positions"
            );
            let quantity: Decimal = positions
                .iter()
                .map(|position| position.signed_decimal_qty())
                .sum();
            anyhow::ensure!(
                quantity == report.signed_decimal_qty,
                "Position handoff engine application is incomplete"
            );
        }
        let mut state = self.read()?;
        if !state.applied {
            state.applied = true;
            let payload = serde_json::to_string(&state)?;
            let envelope = Envelope {
                checksum: blake3::hash(payload.as_bytes()).to_hex().to_string(),
                payload,
            };
            let temporary = self.path.with_extension("pending");
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            file.write_all(&serde_json::to_vec(&envelope)?)?;
            file.sync_all()?;
            fs::rename(&temporary, &self.path)?;
            File::open(
                self.path
                    .parent()
                    .ok_or_else(|| anyhow::anyhow!("Missing handoff directory"))?,
            )?
            .sync_all()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::UnixNanos;
    use nautilus_model::{enums::PositionSide, types::Quantity};
    use rstest::rstest;
    use tempfile::TempDir;

    use super::*;

    fn configured(directory: &TempDir) -> PapiPositionHandoffConfig {
        let manifest = serde_json::json!({"format":1,"handoff_id":"handoff-1",
            "identity":"binance.com:papi:48","context_id":"context-1",
            "strategy_id":"Hedge-001","launcher_id":"local","execution_account_id":"BINANCE-PAPI-48",
            "source_owner":"retired-worker","source_stopped":true,"source_orders_resolved":true,
            "confirmed_at_ns":1,"positions":[{"instrument_id":"BTCUSDT-PERP.BINANCE","signed_quantity":"0.200"}]});
        let config = PapiPositionHandoffConfig {
            path: directory.path().join("position-handoff.json"),
            manifest,
        };
        let state = State {
            format: 1,
            manifest: config.manifest.clone(),
            applied: false,
        };
        let payload = serde_json::to_string(&state).unwrap();
        let envelope = Envelope {
            checksum: blake3::hash(payload.as_bytes()).to_hex().to_string(),
            payload,
        };
        fs::write(&config.path, serde_json::to_vec(&envelope).unwrap()).unwrap();
        config
    }

    fn report(account: &str, instrument: &str, quantity: &str) -> PositionStatusReport {
        PositionStatusReport::new(
            AccountId::from(account),
            InstrumentId::from(instrument),
            PositionSide::Long,
            Quantity::from(quantity),
            UnixNanos::from(1),
            UnixNanos::from(2),
            None,
            None,
            None,
        )
    }

    #[rstest]
    fn initial_position_handoff_requires_exact_report_identity_and_quantity() {
        let directory = TempDir::new().unwrap();
        let handoff = configured(&directory);
        let scope = [InstrumentId::from("BTCUSDT-PERP.BINANCE")];
        handoff
            .validate(AccountId::from("BINANCE-PAPI-48"), &scope)
            .unwrap();
        assert_eq!(
            handoff
                .strategy_for(&report("BINANCE-PAPI-48", "BTCUSDT-PERP.BINANCE", "0.200"))
                .unwrap(),
            Some(StrategyId::from("Hedge-001"))
        );
        for candidate in [
            report("BINANCE-PAPI-49", "BTCUSDT-PERP.BINANCE", "0.200"),
            report("BINANCE-PAPI-48", "ETHUSDT-PERP.BINANCE", "0.200"),
            report("BINANCE-PAPI-48", "BTCUSDT-PERP.BINANCE", "0.300"),
        ] {
            assert!(handoff.strategy_for(&candidate).is_err());
        }
        assert!(!handoff.read().unwrap().applied);
    }

    #[rstest]
    fn missing_or_changed_position_handoff_receipt_is_not_recreated() {
        let directory = TempDir::new().unwrap();
        let mut handoff = configured(&directory);
        handoff.manifest["context_id"] = Value::String("another-context".to_string());
        assert!(handoff.read().is_err());
        fs::remove_file(&handoff.path).unwrap();
        assert!(
            handoff
                .strategy_for(&report("BINANCE-PAPI-48", "BTCUSDT-PERP.BINANCE", "0.200"))
                .is_err()
        );
        assert!(!handoff.path.exists());
    }
}
