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

use std::time::Duration;

use nautilus_core::string::secret::SecretString;
use nautilus_model::identifiers::AccountId;
use serde::{Deserialize, Serialize};
use url::{Host, Url};

/// Explicit credentials and resource bounds for PAPI read-only queries.
///
/// No environment variables are read. The supplied instruments define the complete query scope.
/// Credentials and the base URL are redacted from Rust and Python representations.
#[derive(Clone, Debug, Deserialize, Serialize)]
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
pub struct BinancePapiReadOnlyConfig {
    /// One Portfolio Margin account identity, with issuer `BINANCE`.
    pub account_id: AccountId,
    /// API key, redacted in diagnostics.
    pub api_key: SecretString,
    /// HMAC API secret, redacted in diagnostics.
    pub api_secret: SecretString,
    /// HTTPS origin, or an HTTP loopback origin for a local REST server.
    pub base_url: SecretString,
    /// Timeout for each network attempt, excluding quota acquisition.
    pub request_timeout: Duration,
    /// Total budget for one operation, including all pages, quota waits and retries.
    pub operation_timeout: Duration,
    /// Maximum number of request attempts per operation, including retries.
    pub max_requests: u32,
    /// Maximum rows decoded per operation, including overlapping history pages.
    pub max_rows: usize,
}

impl BinancePapiReadOnlyConfig {
    /// Creates a configuration with five-second attempts and a sixty-second total budget.
    #[must_use]
    pub fn new(account_id: AccountId, api_key: SecretString, api_secret: SecretString) -> Self {
        Self {
            account_id,
            api_key,
            api_secret,
            base_url: SecretString::from("https://papi.binance.com"),
            request_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(60),
            max_requests: 256,
            max_rows: 100_000,
        }
    }

    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.account_id.get_issuer().as_str() == "BINANCE",
            "PAPI account issuer must be BINANCE"
        );

        for credential in [&self.api_key, &self.api_secret] {
            let value = credential.expose_secret();
            anyhow::ensure!(
                !value.is_empty()
                    && value.len() <= 512
                    && value.bytes().all(|b| b.is_ascii_alphanumeric()),
                "PAPI requires nonempty alphanumeric HMAC credentials"
            );
        }

        let url = Url::parse(self.base_url.expose_secret())
            .map_err(|_| anyhow::anyhow!("Invalid PAPI base URL"))?;
        let loopback = match url.host() {
            Some(Host::Ipv4(ip)) => ip.is_loopback(),
            Some(Host::Ipv6(ip)) => ip.is_loopback(),
            _ => false,
        };

        anyhow::ensure!(
            (url.scheme() == "https" || (url.scheme() == "http" && loopback))
                && url.host().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && url.path() == "/",
            "PAPI base URL must be an HTTPS origin or HTTP loopback origin"
        );
        anyhow::ensure!(
            (Duration::from_millis(1)..=Duration::from_secs(60)).contains(&self.request_timeout)
                && self.operation_timeout >= self.request_timeout
                && self.operation_timeout <= Duration::from_mins(10),
            "Invalid PAPI request or operation timeout"
        );
        anyhow::ensure!(
            (1..=10_000).contains(&self.max_requests) && (1..=1_000_000).contains(&self.max_rows),
            "Invalid PAPI request or row budget"
        );
        Ok(())
    }
}
