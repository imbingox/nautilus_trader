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

//! Factory for Binance Portfolio Margin execution clients.

use std::{cell::RefCell, rc::Rc};

#[cfg(test)]
use nautilus_common::clock::TestClock;
use nautilus_common::{
    cache::CacheView,
    clients::ExecutionClient,
    clock::Clock,
    factories::{ClientConfig, ExecutionClientFactory},
};
use nautilus_execution::client::core::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::{ClientId, TraderId},
};

use crate::{
    config::BinancePapiExecutionClientConfig,
    consts::{BINANCE_PAPI, BINANCE_PAPI_VENUE},
    execution::BinancePapiExecutionClient,
};

/// Factory for scoped Binance Portfolio Margin read-only execution reports.
///
/// LiveNode startup remains unavailable until economic account projection is accepted.
#[derive(Debug, Clone, Default)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.binance_papi", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.binance_papi")
)]
pub struct BinancePapiExecutionClientFactory;

impl BinancePapiExecutionClientFactory {
    /// Creates a new [`BinancePapiExecutionClientFactory`].
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl ExecutionClientFactory for BinancePapiExecutionClientFactory {
    fn create(
        &self,
        trader_id: TraderId,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let config = config
            .as_any()
            .downcast_ref::<BinancePapiExecutionClientConfig>()
            .ok_or_else(|| anyhow::anyhow!(
                "Invalid config type for BinancePapiExecutionClientFactory: expected BinancePapiExecutionClientConfig"
            ))?;

        config.validate()?;

        let core = ExecutionClientCore::new(
            trader_id,
            ClientId::from(name),
            *BINANCE_PAPI_VENUE,
            OmsType::Netting,
            config.account_id,
            AccountType::Margin,
            None,
            cache,
        );
        Ok(Box::new(BinancePapiExecutionClient::new(
            core,
            config.clone(),
        )))
    }

    fn name(&self) -> &'static str {
        BINANCE_PAPI
    }

    fn config_type(&self) -> &'static str {
        stringify!(BinancePapiExecutionClientConfig)
    }
}

#[cfg(test)]
mod tests {
    use nautilus_binance::config::BinanceDataClientConfig;
    use nautilus_common::cache::Cache;
    use nautilus_model::identifiers::{AccountId, Venue};
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_factory_preserves_identity_and_shares_binance_venue() {
        let config = BinancePapiExecutionClientConfig {
            account_id: AccountId::from("BINANCE-PAPI-002"),
            ..Default::default()
        };
        let client = BinancePapiExecutionClientFactory::new()
            .create(
                TraderId::from("TRADER-001"),
                "PAPI-CUSTOM",
                &config,
                Rc::new(RefCell::new(Cache::default())).into(),
                Rc::new(RefCell::new(TestClock::new())),
            )
            .unwrap();
        assert_eq!(client.client_id(), ClientId::from("PAPI-CUSTOM"));
        assert_eq!(client.account_id(), config.account_id);
        assert_eq!(client.venue(), Venue::from("BINANCE"));
        assert_eq!(client.account_id().get_issuer(), client.venue());
        assert!(!client.is_connected());
        assert!(client.get_account().is_none());
    }

    #[rstest]
    fn test_factory_rejects_binance_config() {
        let result = BinancePapiExecutionClientFactory::new().create(
            TraderId::from("TRADER-001"),
            BINANCE_PAPI,
            &BinanceDataClientConfig::default(),
            Rc::new(RefCell::new(Cache::default())).into(),
            Rc::new(RefCell::new(TestClock::new())),
        );
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("expected BinancePapiExecutionClientConfig")
        );
    }
}
