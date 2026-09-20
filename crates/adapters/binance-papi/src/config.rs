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

//! Configuration for Portfolio Margin construction and scoped read-only reports.

use std::{
    any::Any,
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use nautilus_common::factories::ClientConfig;
use nautilus_model::{
    identifiers::{AccountId, InstrumentId},
    types::Currency,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::read_only::BinancePapiReadOnlyConfig;

const MIN_TRADING_RISK_AGE_MS: u64 = 10_000;

/// Hard trading limits for one explicitly allowed linear UM instrument.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
pub struct BinancePapiInstrumentTradingConfig {
    /// The only instrument to which these limits apply.
    pub instrument_id: InstrumentId,
    /// Maximum base quantity accepted for one order.
    pub max_order_quantity: Decimal,
    /// Maximum risk-currency notional accepted for one order.
    pub max_order_notional: Decimal,
    /// Maximum absolute base position, including potentially executable operations.
    pub max_position_quantity: Decimal,
    /// Maximum risk-currency instrument exposure, including open and unknown operations.
    pub max_instrument_exposure: Decimal,
}

impl BinancePapiInstrumentTradingConfig {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.instrument_id.venue.as_str() == "BINANCE",
            "PAPI trading instruments must use the BINANCE venue"
        );
        anyhow::ensure!(
            self.max_order_quantity > Decimal::ZERO
                && self.max_order_notional > Decimal::ZERO
                && self.max_position_quantity > Decimal::ZERO
                && self.max_instrument_exposure > Decimal::ZERO,
            "PAPI instrument trading limits must be greater than zero"
        );
        anyhow::ensure!(
            self.max_order_quantity <= self.max_position_quantity,
            "PAPI maximum order quantity must not exceed maximum position quantity"
        );
        anyhow::ensure!(
            self.max_order_notional <= self.max_instrument_exposure,
            "PAPI maximum order notional must not exceed maximum instrument exposure"
        );
        Ok(())
    }
}

/// Explicit opt-in and finite resource limits for PAPI UM trading.
///
/// Presence of this configuration permits later trading admission checks to run. It never proves
/// that current account evidence is valid or authorizes a command by itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
pub struct BinancePapiTradingConfig {
    /// Absolute path to the account-bound durable command journal.
    pub command_journal_path: PathBuf,
    /// Unit for every configured notional and exposure limit.
    pub risk_currency: Currency,
    /// Per-instrument allowlist and hard limits.
    pub instrument_limits: Vec<BinancePapiInstrumentTradingConfig>,
    /// Maximum aggregate exposure in `risk_currency` across the allowed instruments.
    pub max_account_exposure: Decimal,
    /// Maximum operations that can be reserved, in flight, or awaiting recovery.
    pub max_in_flight_operations: usize,
    /// Maximum age of required risk evidence at admission time.
    pub max_risk_age_ms: u64,
    /// Maximum collection span of one risk evidence generation.
    pub max_risk_collection_span_ms: u64,
    /// Maximum requests consumed by one unknown-result recovery attempt.
    pub max_recovery_requests: u32,
    /// Maximum bounded reconciliation rounds before remaining restricted.
    pub max_recovery_rounds: u32,
    /// Delay between reconciliation evidence collections.
    pub recovery_recheck_interval_ms: u64,
    /// Adverse price buffer applied to market-order admission estimates, in basis points.
    pub market_order_price_buffer_bps: u32,
    /// Additional fee reserve applied to order admission estimates, in basis points.
    pub fee_buffer_bps: u32,
}

impl BinancePapiTradingConfig {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        validate_command_journal_path(&self.command_journal_path)?;
        anyhow::ensure!(
            (1..=256).contains(&self.instrument_limits.len()),
            "PAPI trading requires 1 to 256 instrument limits"
        );

        for limits in &self.instrument_limits {
            limits.validate()?;
            anyhow::ensure!(
                limits.max_instrument_exposure <= self.max_account_exposure,
                "PAPI maximum instrument exposure must not exceed maximum account exposure"
            );
        }

        let unique: BTreeSet<_> = self
            .instrument_limits
            .iter()
            .map(|limits| limits.instrument_id)
            .collect();
        anyhow::ensure!(
            unique.len() == self.instrument_limits.len(),
            "PAPI trading instrument limits must be unique"
        );
        anyhow::ensure!(
            self.max_account_exposure > Decimal::ZERO,
            "PAPI maximum account exposure must be greater than zero"
        );
        anyhow::ensure!(
            (1..=10_000).contains(&self.max_in_flight_operations),
            "Invalid PAPI in-flight operation limit"
        );
        anyhow::ensure!(
            (MIN_TRADING_RISK_AGE_MS..=60_000).contains(&self.max_risk_age_ms)
                && (1..=self.max_risk_age_ms).contains(&self.max_risk_collection_span_ms),
            "PAPI risk evidence age must be 10000 to 60000 milliseconds and cover the collection span"
        );
        anyhow::ensure!(
            (1..=10_000).contains(&self.max_recovery_requests)
                && (1..=100).contains(&self.max_recovery_rounds)
                && (1..=60_000).contains(&self.recovery_recheck_interval_ms),
            "Invalid PAPI recovery policy"
        );
        anyhow::ensure!(
            (1..=10_000).contains(&self.market_order_price_buffer_bps)
                && (1..=10_000).contains(&self.fee_buffer_bps),
            "PAPI admission buffers must be between 1 and 10000 basis points"
        );
        Ok(())
    }
}

fn validate_command_journal_path(path: &Path) -> anyhow::Result<()> {
    anyhow::ensure!(
        path.is_absolute() && path.file_name().is_some(),
        "PAPI command journal path must be an absolute file path"
    );
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("PAPI command journal path must have a parent directory"))?;
    anyhow::ensure!(
        parent.is_dir(),
        "PAPI command journal parent directory must exist"
    );

    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        anyhow::ensure!(
            !metadata.file_type().is_symlink(),
            "PAPI command journal path must not be a symbolic link"
        );
        anyhow::ensure!(
            metadata.is_file(),
            "PAPI command journal path must identify a file"
        );
    }
    Ok(())
}

/// Configuration for scoped Binance Portfolio Margin execution reports.
///
/// The default supports node construction without credentials. Supplying `read_only`
/// and explicit instrument IDs enables the Rust execution client's report methods.
/// Instruments must already exist in the node cache. Supplying `trading` explicitly enables the
/// durable command lifecycle, while increase-risk admission remains closed until the coordinator
/// has current, authenticated Portfolio Margin risk evidence.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.binance_papi", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.binance_papi")
)]
pub struct BinancePapiExecutionClientConfig {
    /// The account ID for this client.
    pub account_id: AccountId,
    /// Explicit credentials and request bounds; no environment fallback is used.
    pub read_only: Option<BinancePapiReadOnlyConfig>,
    /// Complete report scope, including instruments with only historical activity.
    pub instrument_ids: Vec<InstrumentId>,
    /// Explicit trading opt-in and finite admission limits. `None` keeps trading disabled.
    pub trading: Option<BinancePapiTradingConfig>,
}

impl Default for BinancePapiExecutionClientConfig {
    fn default() -> Self {
        Self {
            account_id: AccountId::from("BINANCE-PAPI-001"),
            read_only: None,
            instrument_ids: Vec::new(),
            trading: None,
        }
    }
}

impl BinancePapiExecutionClientConfig {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.account_id.get_issuer().as_str() == "BINANCE",
            "PAPI account issuer must be BINANCE"
        );

        if let Some(read_only) = &self.read_only {
            read_only.validate()?;
            anyhow::ensure!(
                read_only.account_id == self.account_id,
                "PAPI execution and read-only account IDs must match"
            );
            anyhow::ensure!(
                (1..=256).contains(&self.instrument_ids.len()),
                "PAPI read-only reports require 1 to 256 explicit instrument IDs"
            );
        } else {
            anyhow::ensure!(
                self.instrument_ids.is_empty(),
                "PAPI instrument scope requires read-only configuration"
            );
        }

        let unique: BTreeSet<_> = self.instrument_ids.iter().collect();
        anyhow::ensure!(
            unique.len() == self.instrument_ids.len()
                && self
                    .instrument_ids
                    .iter()
                    .all(|id| id.venue.as_str() == "BINANCE"),
            "PAPI instrument IDs must be unique and use the BINANCE venue"
        );

        if let Some(trading) = &self.trading {
            trading.validate()?;
            let read_only = self.read_only.as_ref().ok_or_else(|| {
                anyhow::anyhow!("PAPI trading configuration requires read-only configuration")
            })?;
            anyhow::ensure!(
                trading.max_recovery_requests <= read_only.max_requests,
                "PAPI trading recovery requests must not exceed the shared request budget"
            );
            anyhow::ensure!(
                trading
                    .instrument_limits
                    .iter()
                    .all(|limits| self.instrument_ids.contains(&limits.instrument_id)),
                "PAPI trading instruments must be included in the complete report scope"
            );
        }
        Ok(())
    }
}

impl ClientConfig for BinancePapiExecutionClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use rust_decimal_macros::dec;

    use super::*;

    fn instrument_limits(instrument_id: InstrumentId) -> BinancePapiInstrumentTradingConfig {
        BinancePapiInstrumentTradingConfig {
            instrument_id,
            max_order_quantity: dec!(1.25),
            max_order_notional: dec!(5000.00),
            max_position_quantity: dec!(2.50),
            max_instrument_exposure: dec!(10000.00),
        }
    }

    fn trading_config(instrument_id: InstrumentId) -> BinancePapiTradingConfig {
        BinancePapiTradingConfig {
            command_journal_path: std::env::temp_dir().join("papi-command-config-test.journal"),
            risk_currency: Currency::USDT(),
            instrument_limits: vec![instrument_limits(instrument_id)],
            max_account_exposure: dec!(15000.00),
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

    #[rstest]
    fn test_config_round_trip() {
        let config: BinancePapiExecutionClientConfig =
            toml::from_str("account_id = 'BINANCE-PAPI-002'").unwrap();
        let restored: BinancePapiExecutionClientConfig =
            toml::Value::try_from(&config).unwrap().try_into().unwrap();
        assert_eq!(restored.account_id, AccountId::from("BINANCE-PAPI-002"));
    }

    #[rstest]
    fn test_config_defaults() {
        let config: BinancePapiExecutionClientConfig = toml::from_str("").unwrap();
        assert_eq!(
            config.account_id,
            BinancePapiExecutionClientConfig::default().account_id
        );
        assert_eq!(
            config.account_id.get_issuer(),
            *crate::consts::BINANCE_PAPI_VENUE
        );
        assert!(config.trading.is_none());
        config.validate().unwrap();
    }

    #[rstest]
    #[case("product_type = 'COIN_M'")]
    #[case("api_key = 'unused'")]
    #[case("account_id = 'invalid'")]
    fn test_config_rejects_unsupported_or_invalid_fields(#[case] value: &str) {
        assert!(toml::from_str::<BinancePapiExecutionClientConfig>(value).is_err());
    }

    #[rstest]
    fn test_read_only_config_round_trip_and_redaction() {
        let config = BinancePapiExecutionClientConfig {
            read_only: Some(crate::testing::config("http://127.0.0.1:12345")),
            instrument_ids: vec![InstrumentId::from("BTCUSDT-PERP.BINANCE")],
            ..Default::default()
        };
        let restored: BinancePapiExecutionClientConfig =
            toml::Value::try_from(&config).unwrap().try_into().unwrap();
        restored.validate().unwrap();
        assert_eq!(restored.instrument_ids, config.instrument_ids);
        let read_only = restored.read_only.as_ref().unwrap();
        assert_eq!(read_only.account_id, config.account_id);
        assert_eq!(read_only.api_key.expose_secret(), crate::testing::API_KEY);
        assert_eq!(read_only.max_requests, 256);
        let rendered = format!("{restored:?}");
        assert!(!rendered.contains(crate::testing::API_KEY));
        assert!(!rendered.contains(crate::testing::API_SECRET));
        assert!(!rendered.contains("127.0.0.1"));
    }

    #[rstest]
    #[case("missing_scope")]
    #[case("duplicate_scope")]
    #[case("wrong_venue")]
    #[case("wrong_account")]
    #[case("missing_credentials")]
    fn test_read_only_config_rejects_ambiguous_scope(#[case] invalid: &str) {
        let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
        let mut config = BinancePapiExecutionClientConfig {
            read_only: Some(crate::testing::config("http://127.0.0.1:12345")),
            instrument_ids: vec![instrument_id],
            ..Default::default()
        };

        match invalid {
            "missing_scope" => config.instrument_ids.clear(),
            "duplicate_scope" => config.instrument_ids.push(instrument_id),
            "wrong_venue" => config.instrument_ids = vec![InstrumentId::from("BTCUSDT-PERP.OTHER")],
            "wrong_account" => config.account_id = AccountId::from("BINANCE-PAPI-002"),
            "missing_credentials" => config.read_only = None,
            _ => unreachable!(),
        }

        assert!(config.validate().is_err());
    }

    #[rstest]
    fn test_trading_config_round_trip_preserves_exact_limits() {
        let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
        let config = BinancePapiExecutionClientConfig {
            read_only: Some(crate::testing::config("http://127.0.0.1:12345")),
            instrument_ids: vec![instrument_id],
            trading: Some(trading_config(instrument_id)),
            ..Default::default()
        };

        config.validate().unwrap();
        let encoded = toml::Value::try_from(&config).unwrap();
        let restored: BinancePapiExecutionClientConfig = encoded.clone().try_into().unwrap();
        restored.validate().unwrap();

        assert_eq!(restored.trading, config.trading);
        assert_eq!(
            encoded["trading"]["instrument_limits"][0]["max_order_quantity"].as_str(),
            Some("1.25")
        );
        assert_eq!(
            encoded["trading"]["max_account_exposure"].as_str(),
            Some("15000.00")
        );
    }

    #[rstest]
    #[case("missing_read_only")]
    #[case("outside_report_scope")]
    #[case("recovery_budget_too_large")]
    fn test_execution_config_rejects_unsafe_trading_wiring(#[case] invalid: &str) {
        let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
        let mut config = BinancePapiExecutionClientConfig {
            read_only: Some(crate::testing::config("http://127.0.0.1:12345")),
            instrument_ids: vec![instrument_id],
            trading: Some(trading_config(instrument_id)),
            ..Default::default()
        };

        match invalid {
            "missing_read_only" => config.read_only = None,
            "outside_report_scope" => {
                config.instrument_ids = vec![InstrumentId::from("ETHUSDT-PERP.BINANCE")];
            }
            "recovery_budget_too_large" => {
                config.trading.as_mut().unwrap().max_recovery_requests = 257;
            }
            _ => unreachable!(),
        }

        assert!(config.validate().is_err());
    }

    #[rstest]
    #[case("duplicate_instrument")]
    #[case("zero_order_quantity")]
    #[case("order_quantity_above_position")]
    #[case("order_notional_above_instrument")]
    #[case("instrument_above_account")]
    #[case("invalid_risk_timing")]
    #[case("zero_price_buffer")]
    #[case("relative_journal")]
    #[case("journal_is_directory")]
    fn test_trading_config_rejects_invalid_limits(#[case] invalid: &str) {
        let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
        let mut config = trading_config(instrument_id);

        match invalid {
            "duplicate_instrument" => config
                .instrument_limits
                .push(instrument_limits(instrument_id)),
            "zero_order_quantity" => {
                config.instrument_limits[0].max_order_quantity = Decimal::ZERO;
            }
            "order_quantity_above_position" => {
                config.instrument_limits[0].max_order_quantity = dec!(3.00);
            }
            "order_notional_above_instrument" => {
                config.instrument_limits[0].max_order_notional = dec!(11000.00);
            }
            "instrument_above_account" => {
                config.max_account_exposure = dec!(9000.00);
            }
            "invalid_risk_timing" => config.max_risk_age_ms = MIN_TRADING_RISK_AGE_MS - 1,
            "zero_price_buffer" => config.market_order_price_buffer_bps = 0,
            "relative_journal" => config.command_journal_path = "relative.journal".into(),
            "journal_is_directory" => config.command_journal_path = std::env::temp_dir(),
            _ => unreachable!(),
        }

        assert!(config.validate().is_err());
    }
}
