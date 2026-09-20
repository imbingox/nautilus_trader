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
    /// Secure PAPI WebSocket path, or a loopback path for local tests.
    pub websocket_url: SecretString,
    /// Optional HTTP or HTTPS forward proxy shared by REST and WebSocket transports.
    pub proxy_url: Option<SecretString>,
    /// Timeout for each network attempt, excluding quota acquisition.
    pub request_timeout: Duration,
    /// Total budget for one operation, including all pages, quota waits and retries.
    pub operation_timeout: Duration,
    /// Maximum number of request attempts per operation, including retries.
    pub max_requests: u32,
    /// Maximum rows decoded per operation, including overlapping history pages.
    pub max_rows: usize,
    /// REST listen-key keepalive cadence, strictly below the venue's 60-minute expiry.
    pub listen_key_keepalive_interval: Duration,
    /// Planned transport replacement cadence, strictly below the venue's 24-hour limit.
    pub transport_rotation_interval: Duration,
    /// Historical overlap collected on initial synchronization and after a transport gap.
    pub recovery_lookback: Duration,
    /// Debounce applied while coalescing account-stream dirty sources.
    pub refresh_debounce: Duration,
    /// Maximum accepted WebSocket text or binary frame size.
    pub max_websocket_message_bytes: usize,
    /// Maximum number of account-stream events waiting for the serial recovery driver.
    pub max_websocket_buffer_messages: usize,
    /// Maximum aggregate bytes waiting for the serial recovery driver.
    pub max_websocket_buffer_bytes: usize,
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
            websocket_url: SecretString::from("wss://fstream.binance.com/pm/ws"),
            proxy_url: None,
            request_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(60),
            max_requests: 256,
            max_rows: 100_000,
            listen_key_keepalive_interval: Duration::from_mins(30),
            transport_rotation_interval: Duration::from_hours(23),
            recovery_lookback: Duration::from_hours(24),
            refresh_debounce: Duration::from_millis(250),
            max_websocket_message_bytes: 1_048_576,
            max_websocket_buffer_messages: 4_096,
            max_websocket_buffer_bytes: 8_388_608,
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

        let websocket_url = Url::parse(self.websocket_url.expose_secret())
            .map_err(|_| anyhow::anyhow!("Invalid PAPI WebSocket URL"))?;
        let websocket_loopback = match websocket_url.host() {
            Some(Host::Ipv4(ip)) => ip.is_loopback(),
            Some(Host::Ipv6(ip)) => ip.is_loopback(),
            _ => false,
        };
        anyhow::ensure!(
            (websocket_url.scheme() == "wss"
                || (websocket_url.scheme() == "ws" && websocket_loopback))
                && websocket_url.host().is_some()
                && websocket_url.username().is_empty()
                && websocket_url.password().is_none()
                && websocket_url.query().is_none()
                && websocket_url.fragment().is_none(),
            "PAPI WebSocket URL must be a WSS path or WS loopback path"
        );

        if let Some(proxy_url) = &self.proxy_url {
            let proxy_url = Url::parse(proxy_url.expose_secret())
                .map_err(|_| anyhow::anyhow!("Invalid PAPI proxy URL"))?;
            anyhow::ensure!(
                matches!(proxy_url.scheme(), "http" | "https")
                    && proxy_url.host().is_some()
                    && proxy_url.query().is_none()
                    && proxy_url.fragment().is_none()
                    && proxy_url.path() == "/",
                "PAPI proxy URL must be an HTTP or HTTPS origin"
            );
        }

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
        anyhow::ensure!(
            (Duration::from_secs(1)..Duration::from_hours(1))
                .contains(&self.listen_key_keepalive_interval)
                && (Duration::from_secs(1)..Duration::from_hours(24))
                    .contains(&self.transport_rotation_interval)
                && (Duration::from_secs(1)..=Duration::from_hours(24 * 7))
                    .contains(&self.recovery_lookback)
                && (Duration::from_millis(1)..=Duration::from_secs(30))
                    .contains(&self.refresh_debounce),
            "Invalid PAPI session timing"
        );
        anyhow::ensure!(
            (1_024..=8_388_608).contains(&self.max_websocket_message_bytes)
                && (1..=100_000).contains(&self.max_websocket_buffer_messages)
                && self.max_websocket_buffer_bytes >= self.max_websocket_message_bytes
                && self.max_websocket_buffer_bytes <= 134_217_728,
            "Invalid PAPI WebSocket resource bounds"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::string::secret::SecretString;
    use nautilus_model::identifiers::AccountId;
    use rstest::rstest;

    use super::BinancePapiReadOnlyConfig;

    fn config() -> BinancePapiReadOnlyConfig {
        BinancePapiReadOnlyConfig::new(
            AccountId::from("BINANCE-PAPI-001"),
            SecretString::from("OfflinePapiKey"),
            SecretString::from("OfflinePapiSecret"),
        )
    }

    #[rstest]
    fn proxy_credentials_are_redacted() {
        let mut config = config();
        config.proxy_url = Some(SecretString::from(
            "http://proxy-user:proxy-secret@localhost:7897",
        ));

        config.validate().unwrap();
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("proxy-user"));
        assert!(!rendered.contains("proxy-secret"));
    }

    #[rstest]
    #[case("socks5://127.0.0.1:1080")]
    #[case("http://127.0.0.1:7897/path")]
    #[case("http://127.0.0.1:7897?token=secret")]
    fn unsupported_proxy_url_is_rejected(#[case] proxy_url: &str) {
        let mut config = config();
        config.proxy_url = Some(SecretString::from(proxy_url));

        let e = config.validate().unwrap_err();
        assert_eq!(
            e.to_string(),
            "PAPI proxy URL must be an HTTP or HTTPS origin"
        );
        assert!(!e.to_string().contains(proxy_url));
    }
}
