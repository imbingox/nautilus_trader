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

use binance_sdk::common::errors::ConnectorError;
use nautilus_network::retry::RetryError;
use thiserror::Error;

/// SDK messages and source chains can contain signed URLs; retain only typed evidence.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum PapiHttpError {
    #[error("PAPI authentication or permission failure (status {status:?}, code {code:?})")]
    Authentication {
        status: Option<u16>,
        code: Option<i64>,
    },
    #[error("PAPI clock or receive-window failure (-1021)")]
    Clock,
    #[error("PAPI listen key is expired or invalid (-1125)")]
    ListenKeyExpired,
    #[error(
        "PAPI throttling closed the shared request gate (status {status:?}, code {code:?}); automatic recovery is unavailable"
    )]
    Throttled {
        status: Option<u16>,
        code: Option<i64>,
    },
    #[error("PAPI shared request gate is closed; verify venue backoff before restarting")]
    GateClosed,
    #[error("PAPI server failure (status {0})")]
    Server(u16),
    #[error("PAPI request failed (status {status:?}, code {code:?})")]
    Rejected {
        status: Option<u16>,
        code: Option<i64>,
    },
    #[error("PAPI SDK request failed; transport details are redacted")]
    Sdk,
    #[error("PAPI response JSON could not be decoded")]
    Decode,
    #[error("PAPI response exceeds the 8 MiB parsing limit")]
    ResponseTooLarge,
    #[error("PAPI read attempt timed out")]
    Timeout,
    #[error("PAPI read was canceled")]
    Canceled,
    #[error("PAPI read exhausted its operation budget")]
    Budget,
    #[error("Invalid PAPI retry configuration")]
    Configuration,
}

impl PapiHttpError {
    pub(crate) fn from_sdk(e: &anyhow::Error) -> Self {
        let Some(e) = e.downcast_ref::<ConnectorError>() else {
            return Self::Sdk;
        };

        let (status, code) = match e {
            ConnectorError::UnauthorizedError { code, .. } => (Some(401), *code),
            ConnectorError::ForbiddenError { code, .. } => (Some(403), *code),
            ConnectorError::TooManyRequestsError { code, .. } => (Some(429), *code),
            ConnectorError::RateLimitBanError { code, .. } => (Some(418), *code),
            ConnectorError::BadRequestError { code, .. } => (Some(400), *code),
            ConnectorError::NotFoundError { code, .. } => (Some(404), *code),
            ConnectorError::ConnectorClientError { code, .. } => (None, *code),
            ConnectorError::ServerError {
                status_code: Some(status),
                ..
            } => {
                return Self::Server(*status);
            }
            ConnectorError::ServerError {
                status_code: None, ..
            }
            | ConnectorError::NetworkError(_) => return Self::Sdk,
        };

        if matches!(status, Some(429 | 418)) || matches!(code, Some(-1003 | -1015)) {
            Self::Throttled { status, code }
        } else if matches!(status, Some(401 | 403)) || matches!(code, Some(-2014 | -2015 | -1022)) {
            Self::Authentication { status, code }
        } else if code == Some(-1021) {
            Self::Clock
        } else if code == Some(-1125) {
            Self::ListenKeyExpired
        } else if status.is_some() || code.is_some() {
            Self::Rejected { status, code }
        } else {
            Self::Sdk
        }
    }

    pub(crate) const fn retryable(&self) -> bool {
        matches!(self, Self::Timeout | Self::Server(500 | 502 | 503 | 504))
    }

    pub(crate) fn from_retry(e: &RetryError) -> Self {
        match e {
            RetryError::Canceled => Self::Canceled,
            RetryError::OperationTimeout { .. } => Self::Timeout,
            RetryError::ElapsedBudgetExceeded { .. } => Self::Budget,
            RetryError::InvalidConfiguration { .. } => Self::Configuration,
        }
    }
}
