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

use std::{any::Any, collections::BTreeSet};

use nautilus_common::factories::ClientConfig;
use nautilus_model::identifiers::{AccountId, InstrumentId};
use serde::{Deserialize, Serialize};

use crate::read_only::BinancePapiReadOnlyConfig;

/// Configuration for scoped Binance Portfolio Margin execution reports.
///
/// The default supports node construction without credentials. Supplying `read_only`
/// and explicit instrument IDs enables the Rust execution client's report methods.
/// Instruments must already exist in the node cache. LiveNode startup remains unavailable
/// until the native account balance mapping is accepted; trading is unsupported.
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
}

impl Default for BinancePapiExecutionClientConfig {
    fn default() -> Self {
        Self {
            account_id: AccountId::from("BINANCE-PAPI-001"),
            read_only: None,
            instrument_ids: Vec::new(),
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

    use super::*;

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
}
