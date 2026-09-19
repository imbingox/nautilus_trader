// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License. You may obtain a copy of the
//  License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Fail-closed conversion from Nautilus commands to the durable ordinary UM intent.

use std::collections::HashSet;

use nautilus_common::messages::execution::{BatchCancelOrders, CancelOrder, SubmitOrder};
use nautilus_model::{
    enums::{OrderSide, OrderType, TimeInForce},
    identifiers::AccountId,
};
use thiserror::Error;

use super::journal::{
    PapiIntentSide, PapiIntentTimeInForce, PapiPersistedCommand, PapiPersistedOperation,
    PapiSubmitIntent,
};
use crate::{
    http::command::{PapiCommandBuildError, validate_client_order_id},
    reports::parse::{OrderFamily, decode_order_id},
};

pub(crate) fn submit_operation(
    command: &SubmitOrder,
    account_id: AccountId,
) -> Result<PapiPersistedOperation, CommandValidationError> {
    let order = &command.order_init;
    validate_client_order_id(command.client_order_id.as_str())?;

    if command.trader_id != order.trader_id
        || command.strategy_id != order.strategy_id
        || command.instrument_id != order.instrument_id
        || command.client_order_id != order.client_order_id
    {
        return Err(CommandValidationError::Identity);
    }

    if command.params.is_some()
        || command.exec_algorithm_id.is_some()
        || order.exec_algorithm_id.is_some()
        || order.exec_algorithm_params.is_some()
        || order.exec_spawn_id.is_some()
        || order.quote_quantity
        || order.reconciliation
        || order.activation_price.is_some()
        || order.trigger_price.is_some()
        || order.trigger_type.is_some()
        || order.limit_offset.is_some()
        || order.trailing_offset.is_some()
        || order.trailing_offset_type.is_some()
        || order.expire_time.is_some()
        || order.display_qty.is_some()
        || order.emulation_trigger.is_some()
        || order.trigger_instrument_id.is_some()
        || order.contingency_type.is_some()
        || order.order_list_id.is_some()
        || order.linked_order_ids.is_some()
        || order.parent_order_id.is_some()
        || order.tags.is_some()
    {
        return Err(CommandValidationError::UnsupportedField);
    }

    let side = match order.order_side {
        OrderSide::Buy => PapiIntentSide::Buy,
        OrderSide::Sell => PapiIntentSide::Sell,
    };
    let quantity = order.quantity.as_decimal();
    let intent = match order.order_type {
        OrderType::Market => {
            if order.price.is_some() || order.post_only || order.time_in_force != TimeInForce::Gtc {
                return Err(CommandValidationError::MarketTerms);
            }
            PapiSubmitIntent::Market {
                side,
                quantity,
                reduce_only: order.reduce_only,
            }
        }
        OrderType::Limit => {
            let price = order.price.ok_or(CommandValidationError::LimitPrice)?;
            let time_in_force = match (order.time_in_force, order.post_only) {
                (TimeInForce::Gtc, false) => PapiIntentTimeInForce::Gtc,
                (TimeInForce::Ioc, false) => PapiIntentTimeInForce::Ioc,
                (TimeInForce::Fok, false) => PapiIntentTimeInForce::Fok,
                (TimeInForce::Gtc, true) => PapiIntentTimeInForce::Gtx,
                _ => return Err(CommandValidationError::LimitTimeInForce),
            };
            PapiSubmitIntent::Limit {
                side,
                quantity,
                price: price.as_decimal(),
                time_in_force,
                reduce_only: order.reduce_only,
            }
        }
        _ => return Err(CommandValidationError::OrderType),
    };

    Ok(PapiPersistedOperation {
        operation_id: command.command_id,
        account_id,
        strategy_id: command.strategy_id,
        instrument_id: command.instrument_id,
        client_order_id: command.client_order_id,
        generation: 0,
        ts_init: command.ts_init,
        command: PapiPersistedCommand::Submit(intent),
        reservation: None,
    })
}

pub(crate) fn cancel_operation(
    command: &CancelOrder,
    account_id: AccountId,
) -> Result<PapiPersistedOperation, CommandValidationError> {
    validate_client_order_id(command.client_order_id.as_str())?;

    if command.params.is_some() {
        return Err(CommandValidationError::UnsupportedField);
    }

    let venue_order_id = command
        .venue_order_id
        .map(|venue_order_id| {
            let symbol =
                nautilus_binance::common::symbol::format_binance_symbol(&command.instrument_id);
            let (family, order_id) = decode_order_id(venue_order_id, &symbol)
                .map_err(|_| CommandValidationError::VenueOrderId)?;
            if family != OrderFamily::Ordinary {
                return Err(CommandValidationError::VenueOrderId);
            }
            Ok(order_id)
        })
        .transpose()?;

    Ok(PapiPersistedOperation {
        operation_id: command.command_id,
        account_id,
        strategy_id: command.strategy_id,
        instrument_id: command.instrument_id,
        client_order_id: command.client_order_id,
        generation: 0,
        ts_init: command.ts_init,
        command: PapiPersistedCommand::Cancel { venue_order_id },
        reservation: None,
    })
}

pub(crate) fn batch_cancel_operations(
    command: &BatchCancelOrders,
    account_id: AccountId,
) -> Result<Vec<PapiPersistedOperation>, CommandValidationError> {
    if command.cancels.is_empty() {
        return Err(CommandValidationError::EmptyBatch);
    }

    let mut targets = HashSet::new();
    let mut operations = Vec::with_capacity(command.cancels.len());
    for cancel in &command.cancels {
        if cancel.trader_id != command.trader_id
            || cancel.strategy_id != command.strategy_id
            || cancel.instrument_id != command.instrument_id
            || cancel.client_id != command.client_id
            || !targets.insert(cancel.client_order_id)
        {
            return Err(CommandValidationError::BatchScope);
        }
        operations.push(cancel_operation(cancel, account_id)?);
    }
    Ok(operations)
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum CommandValidationError {
    #[error("PAPI command and initialized order identities do not match")]
    Identity,
    #[error("PAPI command contains a field outside the ordinary UM command capability")]
    UnsupportedField,
    #[error("PAPI supports only MARKET and LIMIT submissions")]
    OrderType,
    #[error("PAPI MARKET submission has unsupported price, post-only, or time-in-force terms")]
    MarketTerms,
    #[error("PAPI LIMIT submission requires an exact limit price")]
    LimitPrice,
    #[error("PAPI LIMIT submission has an unsupported time-in-force/post-only combination")]
    LimitTimeInForce,
    #[error("PAPI cancel venue order ID is not an ordinary UM identity in this symbol scope")]
    VenueOrderId,
    #[error("PAPI batch cancel must contain at least one target")]
    EmptyBatch,
    #[error("PAPI batch cancel targets must be unique and match the outer command scope")]
    BatchScope,
    #[error(transparent)]
    Build(#[from] PapiCommandBuildError),
}

#[cfg(test)]
mod tests {
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        enums::{OrderSide, TimeInForce, TriggerType},
        identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId, VenueOrderId},
        orders::{LimitOrder, MarketOrder, OrderAny},
        types::{Price, Quantity},
    };
    use rstest::rstest;
    use rust_decimal_macros::dec;

    use super::*;

    fn market_order() -> OrderAny {
        OrderAny::Market(MarketOrder::new(
            TraderId::from("TRADER-001"),
            StrategyId::from("S-001"),
            InstrumentId::from("BTCUSDT-PERP.BINANCE"),
            ClientOrderId::from("O-001"),
            OrderSide::Buy,
            Quantity::from("0.500"),
            TimeInForce::Gtc,
            UUID4::new(),
            UnixNanos::from(1),
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ))
    }

    fn limit_order(time_in_force: TimeInForce, post_only: bool) -> OrderAny {
        OrderAny::Limit(LimitOrder::new(
            TraderId::from("TRADER-001"),
            StrategyId::from("S-001"),
            InstrumentId::from("BTCUSDT-PERP.BINANCE"),
            ClientOrderId::from("O-001"),
            OrderSide::Sell,
            Quantity::from("0.500"),
            Price::from("30000.01"),
            time_in_force,
            None,
            post_only,
            true,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::from(1),
        ))
    }

    fn submit(order: &OrderAny) -> SubmitOrder {
        SubmitOrder::from_order(
            order,
            TraderId::from("TRADER-001"),
            None,
            None,
            UUID4::new(),
            UnixNanos::from(2),
        )
    }

    #[rstest]
    #[case(TimeInForce::Gtc, false, PapiIntentTimeInForce::Gtc)]
    #[case(TimeInForce::Ioc, false, PapiIntentTimeInForce::Ioc)]
    #[case(TimeInForce::Fok, false, PapiIntentTimeInForce::Fok)]
    #[case(TimeInForce::Gtc, true, PapiIntentTimeInForce::Gtx)]
    fn supported_limit_matrix_is_exact(
        #[case] time_in_force: TimeInForce,
        #[case] post_only: bool,
        #[case] expected: PapiIntentTimeInForce,
    ) {
        let order = limit_order(time_in_force, post_only);
        let operation =
            submit_operation(&submit(&order), AccountId::from("BINANCE-PAPI-001")).unwrap();
        let PapiPersistedCommand::Submit(PapiSubmitIntent::Limit {
            quantity,
            price,
            time_in_force,
            reduce_only,
            ..
        }) = operation.command
        else {
            panic!("expected a limit intent")
        };
        assert_eq!(quantity, dec!(0.500));
        assert_eq!(price, dec!(30000.01));
        assert_eq!(time_in_force, expected);
        assert!(reduce_only);
    }

    #[rstest]
    fn market_terms_and_unsupported_fields_fail_before_persistence() {
        let order = market_order();
        let operation =
            submit_operation(&submit(&order), AccountId::from("BINANCE-PAPI-001")).unwrap();
        assert!(matches!(
            operation.command,
            PapiPersistedCommand::Submit(PapiSubmitIntent::Market {
                quantity,
                reduce_only: false,
                ..
            }) if quantity == dec!(0.500)
        ));

        let mut unsupported = submit(&order);
        unsupported.order_init.trigger_type = Some(TriggerType::Default);
        assert_eq!(
            submit_operation(&unsupported, AccountId::from("BINANCE-PAPI-001")).unwrap_err(),
            CommandValidationError::UnsupportedField
        );

        let invalid_tif = limit_order(TimeInForce::Ioc, true);
        assert_eq!(
            submit_operation(&submit(&invalid_tif), AccountId::from("BINANCE-PAPI-001"))
                .unwrap_err(),
            CommandValidationError::LimitTimeInForce
        );
    }

    #[rstest]
    fn cancel_accepts_only_scoped_ordinary_venue_ids() {
        let command = CancelOrder::new(
            TraderId::from("TRADER-001"),
            None,
            StrategyId::from("S-001"),
            InstrumentId::from("BTCUSDT-PERP.BINANCE"),
            ClientOrderId::from("O-001"),
            Some(VenueOrderId::from("PAPI:O:BTCUSDT:42")),
            UUID4::new(),
            UnixNanos::from(1),
            None,
            None,
        );
        let operation = cancel_operation(&command, AccountId::from("BINANCE-PAPI-001")).unwrap();
        assert!(matches!(
            operation.command,
            PapiPersistedCommand::Cancel {
                venue_order_id: Some(42)
            }
        ));

        let mut algo = command;
        algo.venue_order_id = Some(VenueOrderId::from("PAPI:A:BTCUSDT:42"));
        assert_eq!(
            cancel_operation(&algo, AccountId::from("BINANCE-PAPI-001")).unwrap_err(),
            CommandValidationError::VenueOrderId
        );
    }

    #[rstest]
    fn batch_cancel_freezes_a_unique_uniform_target_set() {
        let first = CancelOrder::new(
            TraderId::from("TRADER-001"),
            None,
            StrategyId::from("S-001"),
            InstrumentId::from("BTCUSDT-PERP.BINANCE"),
            ClientOrderId::from("O-001"),
            None,
            UUID4::new(),
            UnixNanos::from(1),
            None,
            None,
        );
        let mut second = first.clone();
        second.client_order_id = ClientOrderId::from("O-002");
        second.command_id = UUID4::new();
        let batch = BatchCancelOrders::new(
            first.trader_id,
            first.client_id,
            first.strategy_id,
            first.instrument_id,
            vec![first.clone(), second],
            UUID4::new(),
            UnixNanos::from(2),
            None,
            None,
        );
        let operations =
            batch_cancel_operations(&batch, AccountId::from("BINANCE-PAPI-001")).unwrap();
        assert_eq!(operations.len(), 2);
        assert_ne!(operations[0].operation_id, operations[1].operation_id);

        let duplicate = BatchCancelOrders::new(
            first.trader_id,
            first.client_id,
            first.strategy_id,
            first.instrument_id,
            vec![first.clone(), first],
            UUID4::new(),
            UnixNanos::from(2),
            None,
            None,
        );
        assert_eq!(
            batch_cancel_operations(&duplicate, AccountId::from("BINANCE-PAPI-001")).unwrap_err(),
            CommandValidationError::BatchScope
        );
    }
}
