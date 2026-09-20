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

//! Strict, credential-free decoding for Portfolio Margin account-stream facts.

use nautilus_model::enums::{OrderSide, OrderType, TimeInForce};
use rust_decimal::Decimal;
use serde::Deserialize;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
#[expect(
    clippy::large_enum_variant,
    reason = "Account-stream facts are ephemeral and immediately consumed"
)]
pub(super) enum PapiWsEvent {
    Order(OrderFact),
    Algo(AlgoFact),
    Account(AccountFact),
    Dirty(DirtyFact),
    ListenKeyExpired { event_time_ms: i64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct OrderFact {
    pub symbol: String,
    pub client_order_id: String,
    pub order_id: i64,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub post_only: bool,
    pub quantity: Decimal,
    pub price: Option<Decimal>,
    pub average_price: Option<Decimal>,
    pub reduce_only: bool,
    pub position_side: String,
    pub execution_type: String,
    pub status: String,
    pub accumulated_qty: Decimal,
    pub accepted_time_ms: i64,
    pub event_time_ms: i64,
    pub transaction_time_ms: i64,
    pub fill: Option<FillFact>,
}

impl OrderFact {
    pub(super) fn is_terminal(&self) -> bool {
        matches!(
            self.status.as_str(),
            "FILLED" | "CANCELED" | "REJECTED" | "EXPIRED" | "EXPIRED_IN_MATCH"
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct FillFact {
    pub trade_id: i64,
    pub quantity: Decimal,
    pub price: Decimal,
    pub commission: Decimal,
    pub commission_asset: String,
    pub maker: bool,
    pub trade_time_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct AlgoFact {
    pub symbol: String,
    pub client_algo_id: String,
    pub algo_id: i64,
    pub status: String,
    pub actual_order_id: Option<i64>,
    pub event_time_ms: i64,
    pub transaction_time_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct AccountFact {
    pub reason: String,
    pub positions: Vec<PositionFact>,
    pub event_time_ms: i64,
    pub transaction_time_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PositionFact {
    pub symbol: String,
    pub quantity: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DirtySource {
    Wallet,
    Risk,
    Orders,
    ProductScope,
    Configuration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct DirtyFact {
    pub source: DirtySource,
    pub event_type: String,
    pub event_time_ms: i64,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(super) enum PapiWsError {
    #[error("PAPI account-stream JSON is malformed")]
    Decode,
    #[error("PAPI account-stream event is missing a valid type or timestamp")]
    Envelope,
    #[error("PAPI account-stream event is outside the declared UM one-way scope")]
    Scope,
    #[error("PAPI account-stream event contains an invalid identity or amount")]
    Fact,
    #[error("PAPI account-stream event type or critical semantics are unsupported: {0}")]
    Unsupported(String),
}

pub(super) fn parse_event(payload: &[u8]) -> Result<PapiWsEvent, PapiWsError> {
    let envelope: Envelope = serde_json::from_slice(payload).map_err(|_| PapiWsError::Decode)?;
    validate_time(envelope.event_time)?;

    match envelope.event_type.as_str() {
        "ORDER_TRADE_UPDATE" => parse_order(payload),
        "ALGO_UPDATE" => parse_algo(payload),
        "ACCOUNT_UPDATE" => parse_account(payload),
        "listenKeyExpired" => Ok(PapiWsEvent::ListenKeyExpired {
            event_time_ms: envelope.event_time,
        }),
        "balanceUpdate" | "outboundAccountPosition" => Ok(PapiWsEvent::Dirty(DirtyFact {
            source: DirtySource::Wallet,
            event_type: envelope.event_type,
            event_time_ms: envelope.event_time,
        })),
        "riskLevelChange" => Ok(PapiWsEvent::Dirty(DirtyFact {
            source: DirtySource::Risk,
            event_type: envelope.event_type,
            event_time_ms: envelope.event_time,
        })),
        "liabilityChange" | "openOrderLoss" => Ok(PapiWsEvent::Dirty(DirtyFact {
            source: DirtySource::ProductScope,
            event_type: envelope.event_type,
            event_time_ms: envelope.event_time,
        })),
        "ACCOUNT_CONFIG_UPDATE" => Ok(PapiWsEvent::Dirty(DirtyFact {
            source: DirtySource::Configuration,
            event_type: envelope.event_type,
            event_time_ms: envelope.event_time,
        })),
        "executionReport" => Ok(PapiWsEvent::Dirty(DirtyFact {
            source: DirtySource::Orders,
            event_type: envelope.event_type,
            event_time_ms: envelope.event_time,
        })),
        "CONDITIONAL_ORDER_TRADE_UPDATE" => Err(PapiWsError::Unsupported(
            "legacy conditional-order event".to_string(),
        )),
        event_type => Err(PapiWsError::Unsupported(event_type.to_string())),
    }
}

fn parse_order(payload: &[u8]) -> Result<PapiWsEvent, PapiWsError> {
    let event: OrderEvent = serde_json::from_slice(payload).map_err(|_| PapiWsError::Decode)?;
    validate_product(&event.product)?;
    validate_times(event.event_time, event.transaction_time)?;
    validate_symbol(&event.order.symbol)?;
    validate_identity(&event.order.client_order_id)?;

    if event.order.position_side != "BOTH"
        || event
            .order
            .original_order_type
            .as_deref()
            .is_some_and(|original| original != event.order.order_type.as_str())
        || event.order.quantity <= Decimal::ZERO
        || event.order.accumulated_qty > event.order.quantity
    {
        return Err(PapiWsError::Scope);
    }

    let side = match event.order.side.as_str() {
        "BUY" => OrderSide::Buy,
        "SELL" => OrderSide::Sell,
        _ => return Err(PapiWsError::Fact),
    };
    let (order_type, price) = match event.order.order_type.as_str() {
        "MARKET" if event.order.price.is_zero() => (OrderType::Market, None),
        "LIMIT" if event.order.price > Decimal::ZERO => (OrderType::Limit, Some(event.order.price)),
        "MARKET" | "LIMIT" => return Err(PapiWsError::Fact),
        _ => {
            return Err(PapiWsError::Unsupported(format!(
                "order type {}",
                event.order.order_type
            )));
        }
    };
    let (time_in_force, post_only) = match event.order.time_in_force.as_str() {
        "GTC" => (TimeInForce::Gtc, false),
        "IOC" => (TimeInForce::Ioc, false),
        "FOK" => (TimeInForce::Fok, false),
        "GTX" => (TimeInForce::Gtc, true),
        value => {
            return Err(PapiWsError::Unsupported(format!("time in force {value}")));
        }
    };

    if order_type == OrderType::Market && (time_in_force != TimeInForce::Gtc || post_only) {
        return Err(PapiWsError::Fact);
    }
    let average_price = if event.order.accumulated_qty.is_zero() {
        if !event.order.average_price.is_zero() {
            return Err(PapiWsError::Fact);
        }
        None
    } else if event.order.average_price > Decimal::ZERO {
        Some(event.order.average_price)
    } else {
        return Err(PapiWsError::Fact);
    };

    if event.order.order_id <= 0
        || event.order.accumulated_qty < Decimal::ZERO
        || !matches!(
            event.order.status.as_str(),
            "NEW"
                | "PARTIALLY_FILLED"
                | "FILLED"
                | "CANCELED"
                | "REJECTED"
                | "EXPIRED"
                | "EXPIRED_IN_MATCH"
                | "PENDING_NEW"
                | "PENDING_CANCEL"
        )
    {
        return Err(PapiWsError::Fact);
    }

    if matches!(
        event.order.execution_type.as_str(),
        "CALCULATED" | "AMENDMENT" | "EXPIRED_IN_MATCH"
    ) {
        return Err(PapiWsError::Unsupported(format!(
            "execution type {}",
            event.order.execution_type
        )));
    }

    if !matches!(
        event.order.execution_type.as_str(),
        "NEW" | "TRADE" | "CANCELED" | "REJECTED" | "EXPIRED"
    ) {
        return Err(PapiWsError::Unsupported(format!(
            "execution type {}",
            event.order.execution_type
        )));
    }

    let fill = if event.order.execution_type == "TRADE" {
        let trade_id = event.order.trade_id.ok_or(PapiWsError::Fact)?;
        let quantity = event.order.last_qty.ok_or(PapiWsError::Fact)?;
        let price = event.order.last_price.ok_or(PapiWsError::Fact)?;
        let commission = event.order.commission.ok_or(PapiWsError::Fact)?;
        let commission_asset = event.order.commission_asset.ok_or(PapiWsError::Fact)?;
        let maker = event.order.maker.ok_or(PapiWsError::Fact)?;
        let trade_time_ms = event.order.trade_time.ok_or(PapiWsError::Fact)?;
        validate_identity(&commission_asset)?;
        validate_time(trade_time_ms)?;

        if trade_id <= 0 || quantity <= Decimal::ZERO || price <= Decimal::ZERO {
            return Err(PapiWsError::Fact);
        }

        Some(FillFact {
            trade_id,
            quantity,
            price,
            commission,
            commission_asset,
            maker,
            trade_time_ms,
        })
    } else {
        if event
            .order
            .last_qty
            .is_some_and(|quantity| !quantity.is_zero())
            || event.order.trade_id.is_some_and(|trade_id| trade_id > 0)
        {
            return Err(PapiWsError::Fact);
        }
        None
    };

    Ok(PapiWsEvent::Order(OrderFact {
        symbol: event.order.symbol,
        client_order_id: event.order.client_order_id,
        order_id: event.order.order_id,
        side,
        order_type,
        time_in_force,
        post_only,
        quantity: event.order.quantity,
        price,
        average_price,
        reduce_only: event.order.reduce_only,
        position_side: event.order.position_side,
        execution_type: event.order.execution_type,
        status: event.order.status,
        accumulated_qty: event.order.accumulated_qty,
        accepted_time_ms: event.transaction_time,
        event_time_ms: event.event_time,
        transaction_time_ms: event.transaction_time,
        fill,
    }))
}

fn parse_algo(payload: &[u8]) -> Result<PapiWsEvent, PapiWsError> {
    let event: AlgoEvent = serde_json::from_slice(payload).map_err(|_| PapiWsError::Decode)?;
    validate_product(&event.product)?;
    validate_times(event.event_time, event.transaction_time)?;
    let algo = event.algo.ok_or_else(|| {
        PapiWsError::Unsupported("legacy ALGO_UPDATE schema without ao".to_string())
    })?;
    validate_symbol(&algo.symbol)?;
    validate_identity(&algo.client_algo_id)?;

    if algo.algo_id <= 0
        || algo.position_side != "BOTH"
        || algo.algo_type != "CONDITIONAL"
        || !matches!(
            algo.status.as_str(),
            "NEW" | "CANCELED" | "TRIGGERING" | "TRIGGERED" | "FINISHED" | "REJECTED" | "EXPIRED"
        )
    {
        return Err(PapiWsError::Scope);
    }

    let actual_order_id = match algo.actual_order_id.as_deref() {
        None | Some("" | "0") => None,
        Some(value) if value.bytes().all(|byte| byte.is_ascii_digit()) => {
            let value = value.parse::<i64>().map_err(|_| PapiWsError::Fact)?;

            if value <= 0 {
                return Err(PapiWsError::Fact);
            }
            Some(value)
        }
        Some(_) => return Err(PapiWsError::Fact),
    };

    Ok(PapiWsEvent::Algo(AlgoFact {
        symbol: algo.symbol,
        client_algo_id: algo.client_algo_id,
        algo_id: algo.algo_id,
        status: algo.status,
        actual_order_id,
        event_time_ms: event.event_time,
        transaction_time_ms: event.transaction_time,
    }))
}

fn parse_account(payload: &[u8]) -> Result<PapiWsEvent, PapiWsError> {
    let event: AccountEvent = serde_json::from_slice(payload).map_err(|_| PapiWsError::Decode)?;
    validate_product(&event.product)?;
    validate_times(event.event_time, event.transaction_time)?;
    validate_identity(&event.account.reason)?;
    let mut positions = Vec::with_capacity(event.account.positions.len());

    for position in event.account.positions {
        validate_symbol(&position.symbol)?;

        if position.position_side != "BOTH" {
            return Err(PapiWsError::Scope);
        }
        positions.push(PositionFact {
            symbol: position.symbol,
            quantity: position.quantity,
        });
    }

    Ok(PapiWsEvent::Account(AccountFact {
        reason: event.account.reason,
        positions,
        event_time_ms: event.event_time,
        transaction_time_ms: event.transaction_time,
    }))
}

fn validate_product(product: &str) -> Result<(), PapiWsError> {
    if product == "UM" {
        Ok(())
    } else {
        Err(PapiWsError::Scope)
    }
}

fn validate_symbol(value: &str) -> Result<(), PapiWsError> {
    if !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        Ok(())
    } else {
        Err(PapiWsError::Fact)
    }
}

fn validate_identity(value: &str) -> Result<(), PapiWsError> {
    if !value.is_empty()
        && value.len() <= 128
        && !value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        Ok(())
    } else {
        Err(PapiWsError::Fact)
    }
}

fn validate_time(value: i64) -> Result<(), PapiWsError> {
    if value > 0 {
        Ok(())
    } else {
        Err(PapiWsError::Envelope)
    }
}

fn validate_times(event_time: i64, transaction_time: i64) -> Result<(), PapiWsError> {
    validate_time(event_time)?;
    validate_time(transaction_time)
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    value.parse().map_err(serde::de::Error::custom)
}

fn deserialize_optional_decimal<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?
        .map(|value| value.parse().map_err(serde::de::Error::custom))
        .transpose()
}

#[derive(Deserialize)]
struct Envelope {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time: i64,
}

#[derive(Deserialize)]
struct OrderEvent {
    #[serde(rename = "fs")]
    product: String,
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "T")]
    transaction_time: i64,
    #[serde(rename = "o")]
    order: OrderPayload,
}

#[derive(Deserialize)]
struct OrderPayload {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "c")]
    client_order_id: String,
    #[serde(rename = "i")]
    order_id: i64,
    #[serde(rename = "S")]
    side: String,
    #[serde(rename = "o")]
    order_type: String,
    #[serde(rename = "ot", default)]
    original_order_type: Option<String>,
    #[serde(rename = "f")]
    time_in_force: String,
    #[serde(rename = "q", deserialize_with = "deserialize_decimal")]
    quantity: Decimal,
    #[serde(rename = "p", deserialize_with = "deserialize_decimal")]
    price: Decimal,
    #[serde(rename = "ap", deserialize_with = "deserialize_decimal")]
    average_price: Decimal,
    #[serde(rename = "R")]
    reduce_only: bool,
    #[serde(rename = "ps")]
    position_side: String,
    #[serde(rename = "x")]
    execution_type: String,
    #[serde(rename = "X")]
    status: String,
    #[serde(rename = "z", deserialize_with = "deserialize_decimal")]
    accumulated_qty: Decimal,
    #[serde(
        rename = "l",
        default,
        deserialize_with = "deserialize_optional_decimal"
    )]
    last_qty: Option<Decimal>,
    #[serde(
        rename = "L",
        default,
        deserialize_with = "deserialize_optional_decimal"
    )]
    last_price: Option<Decimal>,
    #[serde(rename = "N", default)]
    commission_asset: Option<String>,
    #[serde(
        rename = "n",
        default,
        deserialize_with = "deserialize_optional_decimal"
    )]
    commission: Option<Decimal>,
    #[serde(rename = "m", default)]
    maker: Option<bool>,
    #[serde(rename = "T", default)]
    trade_time: Option<i64>,
    #[serde(rename = "t", default)]
    trade_id: Option<i64>,
}

#[derive(Deserialize)]
struct AlgoEvent {
    #[serde(rename = "fs")]
    product: String,
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "T")]
    transaction_time: i64,
    #[serde(rename = "ao")]
    algo: Option<AlgoPayload>,
}

#[derive(Deserialize)]
struct AlgoPayload {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "caid")]
    client_algo_id: String,
    #[serde(rename = "aid")]
    algo_id: i64,
    #[serde(rename = "at")]
    algo_type: String,
    #[serde(rename = "ps")]
    position_side: String,
    #[serde(rename = "X")]
    status: String,
    #[serde(rename = "ai", default)]
    actual_order_id: Option<String>,
}

#[derive(Deserialize)]
struct AccountEvent {
    #[serde(rename = "fs")]
    product: String,
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "T")]
    transaction_time: i64,
    #[serde(rename = "a")]
    account: AccountPayload,
}

#[derive(Deserialize)]
struct AccountPayload {
    #[serde(rename = "m")]
    reason: String,
    #[serde(rename = "P", default)]
    positions: Vec<PositionPayload>,
}

#[derive(Deserialize)]
struct PositionPayload {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "pa", deserialize_with = "deserialize_decimal")]
    quantity: Decimal,
    #[serde(rename = "ps")]
    position_side: String,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    #[rstest]
    fn parses_official_trade_without_original_order_type() {
        let payload = json!({
            "e": "ORDER_TRADE_UPDATE", "E": 1_700_000_000_001_i64,
            "T": 1_700_000_000_000_i64, "fs": "UM",
            "o": {
                "s": "BTCUSDT", "c": "client-1", "i": 42, "x": "TRADE",
                "S": "BUY", "o": "LIMIT", "f": "GTC",
                "q": "0.010", "p": "42000.10", "ap": "42000.10",
                "R": false, "ps": "BOTH",
                "X": "PARTIALLY_FILLED", "z": "0.002", "l": "0.001",
                "L": "42000.10", "N": "BNB", "n": "-0.00000123",
                "m": false, "T": 1_700_000_000_000_i64, "t": 7
            }
        });

        let PapiWsEvent::Order(event) = parse_event(payload.to_string().as_bytes()).unwrap() else {
            panic!("expected order event");
        };
        let fill = event.fill.unwrap();
        assert_eq!(fill.trade_id, 7);
        assert_eq!(
            fill.commission,
            Decimal::from_str_exact("-0.00000123").unwrap()
        );
    }

    #[rstest]
    fn rejects_conflicting_optional_original_order_type() {
        let payload = json!({
            "e": "ORDER_TRADE_UPDATE", "E": 1_700_000_000_001_i64,
            "T": 1_700_000_000_000_i64, "fs": "UM",
            "o": {
                "s": "BTCUSDT", "c": "client-1", "i": 42, "x": "NEW",
                "S": "BUY", "o": "LIMIT", "ot": "MARKET", "f": "GTC",
                "q": "0.010", "p": "42000.10", "ap": "0",
                "R": false, "ps": "BOTH", "X": "NEW", "z": "0"
            }
        });

        assert_eq!(
            parse_event(payload.to_string().as_bytes()),
            Err(PapiWsError::Scope)
        );
    }

    #[rstest]
    fn rejects_trade_without_commission_instead_of_estimating_zero() {
        let payload = json!({
            "e": "ORDER_TRADE_UPDATE", "E": 1_700_000_000_001_i64,
            "T": 1_700_000_000_000_i64, "fs": "UM",
            "o": {
                "s": "BTCUSDT", "c": "client-1", "i": 42, "x": "TRADE",
                "S": "BUY", "o": "MARKET", "ot": "MARKET", "f": "GTC",
                "q": "0.001", "p": "0", "ap": "42000",
                "R": false, "ps": "BOTH",
                "X": "FILLED", "z": "0.001", "l": "0.001", "L": "42000",
                "m": false, "T": 1_700_000_000_000_i64, "t": 7
            }
        });

        assert_eq!(
            parse_event(payload.to_string().as_bytes()),
            Err(PapiWsError::Fact)
        );
    }

    #[rstest]
    fn partial_account_update_preserves_only_explicit_position_rows() {
        let payload = json!({
            "e": "ACCOUNT_UPDATE", "E": 1_700_000_000_001_i64,
            "T": 1_700_000_000_000_i64, "fs": "UM",
            "a": {"m": "FUNDING_FEE", "B": [{"a": "USDT", "wb": "1"}], "P": []}
        });

        let PapiWsEvent::Account(event) = parse_event(payload.to_string().as_bytes()).unwrap()
        else {
            panic!("expected account event");
        };
        assert!(event.positions.is_empty());
    }

    #[rstest]
    fn rejects_unknown_and_legacy_conditional_events() {
        for event_type in ["NEW_CRITICAL_EVENT", "CONDITIONAL_ORDER_TRADE_UPDATE"] {
            let payload = json!({"e": event_type, "E": 1_700_000_000_000_i64});
            assert!(matches!(
                parse_event(payload.to_string().as_bytes()),
                Err(PapiWsError::Unsupported(_))
            ));
        }
    }
}
