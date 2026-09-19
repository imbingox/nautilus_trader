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

//! Typed ordinary UM write requests and command-outcome evidence.

use std::collections::BTreeMap;

use http::Method;
use nautilus_live::execution::failure::CommandFailure;
use nautilus_model::identifiers::ClientOrderId;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;

use super::error::PapiHttpError;

pub(super) const UM_ORDER_ENDPOINT: &str = "/papi/v1/um/order";

/// Side accepted by the first ordinary UM command scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PapiUmOrderSide {
    Buy,
    Sell,
}

impl PapiUmOrderSide {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

/// Time-in-force accepted for ordinary UM limit orders.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PapiUmTimeInForce {
    Gtc,
    Ioc,
    Fok,
    Gtx,
}

impl PapiUmTimeInForce {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Gtc => "GTC",
            Self::Ioc => "IOC",
            Self::Fok => "FOK",
            Self::Gtx => "GTX",
        }
    }
}

/// A validated ordinary UM submit request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SubmitUmOrderRequest {
    symbol: String,
    side: PapiUmOrderSide,
    quantity: Decimal,
    client_order_id: ClientOrderId,
    reduce_only: bool,
    kind: SubmitUmOrderKind,
}

impl SubmitUmOrderRequest {
    pub(crate) fn market(
        symbol: impl Into<String>,
        side: PapiUmOrderSide,
        quantity: Decimal,
        client_order_id: ClientOrderId,
        reduce_only: bool,
    ) -> Result<Self, PapiCommandBuildError> {
        Self::new(
            symbol.into(),
            side,
            quantity,
            client_order_id,
            reduce_only,
            SubmitUmOrderKind::Market,
        )
    }

    pub(crate) fn limit(
        symbol: impl Into<String>,
        side: PapiUmOrderSide,
        quantity: Decimal,
        price: Decimal,
        time_in_force: PapiUmTimeInForce,
        client_order_id: ClientOrderId,
        reduce_only: bool,
    ) -> Result<Self, PapiCommandBuildError> {
        if price <= Decimal::ZERO {
            return Err(PapiCommandBuildError::Price);
        }

        Self::new(
            symbol.into(),
            side,
            quantity,
            client_order_id,
            reduce_only,
            SubmitUmOrderKind::Limit {
                price,
                time_in_force,
            },
        )
    }

    fn new(
        symbol: String,
        side: PapiUmOrderSide,
        quantity: Decimal,
        client_order_id: ClientOrderId,
        reduce_only: bool,
        kind: SubmitUmOrderKind,
    ) -> Result<Self, PapiCommandBuildError> {
        validate_symbol(&symbol)?;
        validate_client_order_id(client_order_id.as_str())?;

        if quantity <= Decimal::ZERO {
            return Err(PapiCommandBuildError::Quantity);
        }

        Ok(Self {
            symbol,
            side,
            quantity,
            client_order_id,
            reduce_only,
            kind,
        })
    }

    fn params(&self) -> BTreeMap<String, Value> {
        let mut params = BTreeMap::from([
            (
                "newClientOrderId".to_string(),
                json!(self.client_order_id.as_str()),
            ),
            ("newOrderRespType".to_string(), json!("ACK")),
            ("positionSide".to_string(), json!("BOTH")),
            ("quantity".to_string(), json!(self.quantity.to_string())),
            ("reduceOnly".to_string(), json!(self.reduce_only)),
            ("side".to_string(), json!(self.side.as_str())),
            ("symbol".to_string(), json!(self.symbol)),
        ]);

        match self.kind {
            SubmitUmOrderKind::Market => {
                params.insert("type".to_string(), json!("MARKET"));
            }
            SubmitUmOrderKind::Limit {
                price,
                time_in_force,
            } => {
                params.insert("price".to_string(), json!(price.to_string()));
                params.insert("timeInForce".to_string(), json!(time_in_force.as_str()));
                params.insert("type".to_string(), json!("LIMIT"));
            }
        }
        params
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SubmitUmOrderKind {
    Market,
    Limit {
        price: Decimal,
        time_in_force: PapiUmTimeInForce,
    },
}

/// A validated ordinary UM cancel request with exactly one venue identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CancelUmOrderRequest {
    symbol: String,
    identity: CancelUmOrderIdentity,
}

impl CancelUmOrderRequest {
    pub(crate) fn by_order_id(
        symbol: impl Into<String>,
        order_id: i64,
    ) -> Result<Self, PapiCommandBuildError> {
        let symbol = symbol.into();
        validate_symbol(&symbol)?;

        if order_id <= 0 {
            return Err(PapiCommandBuildError::OrderId);
        }

        Ok(Self {
            symbol,
            identity: CancelUmOrderIdentity::OrderId(order_id),
        })
    }

    pub(crate) fn by_client_order_id(
        symbol: impl Into<String>,
        client_order_id: ClientOrderId,
    ) -> Result<Self, PapiCommandBuildError> {
        let symbol = symbol.into();
        validate_symbol(&symbol)?;
        validate_client_order_id(client_order_id.as_str())?;
        Ok(Self {
            symbol,
            identity: CancelUmOrderIdentity::ClientOrderId(client_order_id),
        })
    }

    fn params(&self) -> BTreeMap<String, Value> {
        let mut params = BTreeMap::from([("symbol".to_string(), json!(self.symbol))]);

        match &self.identity {
            CancelUmOrderIdentity::OrderId(order_id) => {
                params.insert("orderId".to_string(), json!(order_id));
            }
            CancelUmOrderIdentity::ClientOrderId(client_order_id) => {
                params.insert(
                    "origClientOrderId".to_string(),
                    json!(client_order_id.as_str()),
                );
            }
        }
        params
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CancelUmOrderIdentity {
    OrderId(i64),
    ClientOrderId(ClientOrderId),
}

/// Local request construction failures, which always occur before dispatch.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum PapiCommandBuildError {
    #[error("Invalid PAPI UM symbol")]
    Symbol,
    #[error("Invalid PAPI UM client order ID")]
    ClientOrderId,
    #[error("PAPI UM order quantity must be greater than zero")]
    Quantity,
    #[error("PAPI UM limit price must be greater than zero")]
    Price,
    #[error("PAPI UM venue order ID must be greater than zero")]
    OrderId,
}

/// Typed failure evidence paired with its execution-boundary classification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PapiCommandFailure {
    pub(crate) classification: CommandFailure,
    pub(crate) error: PapiHttpError,
}

/// Minimal authenticated identity returned by an accepted ordinary UM write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PapiCommandAcknowledgement {
    pub(crate) venue_order_id: i64,
    pub(crate) client_order_id: String,
}

impl PapiCommandAcknowledgement {
    pub(super) fn decode(
        request: &PapiCommandRequest<'_>,
        body: &str,
    ) -> Result<Self, PapiHttpError> {
        let response: PapiCommandResponseBody =
            serde_json::from_str(body).map_err(|_| PapiHttpError::Decode)?;

        if response.order_id <= 0
            || response.symbol != request.symbol()
            || !request.matches_response_identity(response.order_id, &response.client_order_id)
        {
            return Err(PapiHttpError::Decode);
        }

        Ok(Self {
            venue_order_id: response.order_id,
            client_order_id: response.client_order_id,
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PapiCommandResponseBody {
    symbol: String,
    order_id: i64,
    client_order_id: String,
}

impl PapiCommandFailure {
    pub(crate) fn before_dispatch(error: PapiHttpError) -> Self {
        Self {
            classification: CommandFailure::not_sent(error.to_string()),
            error,
        }
    }

    pub(super) fn after_dispatch(request: &PapiCommandRequest<'_>, error: PapiHttpError) -> Self {
        let classification = match &error {
            PapiHttpError::Authentication { .. }
            | PapiHttpError::Clock
            | PapiHttpError::ListenKeyExpired => CommandFailure::venue_rejected(error.to_string()),
            PapiHttpError::Rejected {
                status,
                code: Some(code),
            } if !is_ambiguous_code(*code)
                && !is_rate_limit_code(*code)
                && !request.cancel_not_found(*code)
                && status.is_some_and(|status| status < 500 && status != 418 && status != 429) =>
            {
                CommandFailure::venue_rejected(error.to_string())
            }
            _ => CommandFailure::ambiguous(error.to_string()),
        };
        Self {
            classification,
            error,
        }
    }
}

pub(super) enum PapiCommandRequest<'a> {
    Submit(&'a SubmitUmOrderRequest),
    Cancel(&'a CancelUmOrderRequest),
}

impl PapiCommandRequest<'_> {
    pub(super) const fn method(&self) -> Method {
        match self {
            Self::Submit(_) => Method::POST,
            Self::Cancel(_) => Method::DELETE,
        }
    }

    pub(super) fn params(&self) -> BTreeMap<String, Value> {
        let mut params = match self {
            Self::Submit(request) => request.params(),
            Self::Cancel(request) => request.params(),
        };
        params.insert("recvWindow".to_string(), Value::from(5_000));
        params
    }

    pub(super) fn symbol(&self) -> &str {
        match self {
            Self::Submit(request) => &request.symbol,
            Self::Cancel(request) => &request.symbol,
        }
    }

    fn matches_response_identity(&self, order_id: i64, client_order_id: &str) -> bool {
        match self {
            Self::Submit(request) => client_order_id == request.client_order_id.as_str(),
            Self::Cancel(request) => match &request.identity {
                CancelUmOrderIdentity::OrderId(expected) => order_id == *expected,
                CancelUmOrderIdentity::ClientOrderId(expected) => {
                    client_order_id == expected.as_str()
                }
            },
        }
    }

    pub(super) const fn uses_order_quota(&self) -> bool {
        matches!(self, Self::Submit(_))
    }

    fn cancel_not_found(&self, code: i64) -> bool {
        matches!(self, Self::Cancel(_)) && matches!(code, -2011 | -2013)
    }
}

fn validate_symbol(symbol: &str) -> Result<(), PapiCommandBuildError> {
    if symbol.is_empty()
        || symbol.len() > 64
        || !symbol
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        return Err(PapiCommandBuildError::Symbol);
    }
    Ok(())
}

pub(crate) fn validate_client_order_id(value: &str) -> Result<(), PapiCommandBuildError> {
    if value.is_empty()
        || value.len() > 32
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b':' | b'/' | b'_' | b'-')
        })
    {
        return Err(PapiCommandBuildError::ClientOrderId);
    }
    Ok(())
}

const fn is_ambiguous_code(code: i64) -> bool {
    matches!(code, -1000 | -1001 | -1006 | -1007)
}

const fn is_rate_limit_code(code: i64) -> bool {
    matches!(code, -1003 | -1015)
}
