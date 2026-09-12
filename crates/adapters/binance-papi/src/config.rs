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

//! Configuration for Binance Portfolio Margin node construction.

use std::any::Any;

use nautilus_common::factories::ClientConfig;
use nautilus_model::identifiers::AccountId;
use serde::{Deserialize, Serialize};

/// Configuration for the Binance Portfolio Margin execution skeleton.
///
/// Only node construction is supported. No credentials are read and no account state
/// or trading capability is available at this stage.
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
}

impl Default for BinancePapiExecutionClientConfig {
    fn default() -> Self {
        Self {
            account_id: AccountId::from("BINANCE-PAPI-001"),
        }
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
    }

    #[rstest]
    #[case("product_type = 'COIN_M'")]
    #[case("api_key = 'unused'")]
    #[case("account_id = 'invalid'")]
    fn test_config_rejects_unsupported_or_invalid_fields(#[case] value: &str) {
        assert!(toml::from_str::<BinancePapiExecutionClientConfig>(value).is_err());
    }
}
