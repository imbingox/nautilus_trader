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

//! Adapter REST results exercised through the real execution manager and engine.

use std::{cell::RefCell, rc::Rc};

use async_trait::async_trait;
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    clock::TestClock,
    messages::execution::{
        GenerateFillReports, GenerateOrderStatusReport, GenerateOrderStatusReports,
        GeneratePositionStatusReports,
    },
    msgbus::{self, MessagingSwitchboard, stubs::get_typed_into_message_saving_handler},
};
use nautilus_core::{DurationNanos, Params, UUID4, UnixNanos};
use nautilus_execution::engine::ExecutionEngine;
use nautilus_live::manager::{ExecutionManager, ExecutionManagerConfig};
use nautilus_model::{
    accounts::{AccountAny, MarginAccount},
    enums::{AccountType, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType},
    events::{AccountState, OrderEventAny, OrderFilled},
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, PositionId, StrategyId, TradeId, Venue,
        VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderTestBuilder, stubs::TestOrderEventStubs},
    position::Position,
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
};
use rstest::rstest;
use rust_decimal_macros::dec;
use serde_json::json;

use super::BinancePapiReadOnlyClient;
use crate::{
    consts::{BINANCE_PAPI_CLIENT_ID, BINANCE_PAPI_VENUE},
    testing::{self, MockServer, Reply, TRADE_TIME, ms},
};

#[rstest]
#[case("0.11404400", "USDT")]
#[case("-0.00001234", "BNB")]
#[tokio::test]
async fn incomplete_history_keeps_exact_explicit_fees_without_position_or_portfolio_effects(
    #[case] commission: &'static str,
    #[case] currency: &'static str,
) {
    let server = MockServer::new(move |request| match request.path.as_str() {
        "/papi/v1/um/allOrders" => Reply::json(&json!([testing::filled_order()])),
        "/papi/v1/um/userTrades" => {
            let mut row = testing::trade();
            row["commission"] = json!(commission);
            row["commissionAsset"] = json!(currency);
            Reply::json(&json!([row]))
        }
        _ => testing::quiet(request),
    })
    .await;
    let reader = testing::client(&server, &["BTCUSDT"]);
    let snapshot = reader
        .generate_mass_status(ms(TRADE_TIME - 2_000), ms(TRADE_TIME))
        .await
        .unwrap();
    assert!(!snapshot.mass_status.reports_complete());
    let mut ctx = EngineContext::new(ReadClient::new(reader));
    let (handler, portfolio_events) = get_typed_into_message_saving_handler::<OrderEventAny>(None);
    let endpoint = MessagingSwitchboard::portfolio_update_order();
    msgbus::register_order_event_endpoint(endpoint, handler);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(snapshot.mass_status, Rc::clone(&ctx.engine))
        .await;
    msgbus::deregister_any(endpoint);

    let expected_fee = Money::from_decimal(
        commission.parse().unwrap(),
        Currency::try_from_str(currency).unwrap(),
    )
    .unwrap();
    let fills: Vec<_> = result
        .events
        .iter()
        .filter_map(|event| {
            if let OrderEventAny::Filled(fill) = event {
                Some(fill)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].commission, Some(expected_fee));
    assert_eq!(fills[0].trade_id, TradeId::from("67880589"));
    assert_eq!(fills[0].last_qty.as_decimal(), dec!(0.010));
    assert_eq!(fills[0].last_px.as_decimal(), dec!(28511));
    assert!(!fills[0].reconciliation);
    let cache = ctx.cache.borrow();
    let order = cache.order(&ClientOrderId::from("abc")).unwrap();
    assert_eq!(order.status(), OrderStatus::Filled);
    assert_eq!(
        order.commissions().get(&expected_fee.currency),
        Some(&expected_fee)
    );
    assert!(cache.positions(None, None, None, None, None).is_empty());
    assert!(
        portfolio_events
            .get_messages()
            .iter()
            .all(|event| !matches!(event, OrderEventAny::Filled(_)))
    );
}

#[tokio::test]
async fn incomplete_history_cannot_estimate_missing_venue_commission() {
    let server = MockServer::new(|request| {
        if request.path == "/papi/v1/um/allOrders" {
            Reply::json(&json!([testing::filled_order()]))
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let reader = testing::client(&server, &["BTCUSDT"]);
    let snapshot = reader
        .generate_mass_status(ms(TRADE_TIME - 2_000), ms(TRADE_TIME))
        .await
        .unwrap();
    let mut ctx = EngineContext::new(ReadClient::new(reader));
    let result = ctx
        .manager
        .reconcile_execution_mass_status(snapshot.mass_status, Rc::clone(&ctx.engine))
        .await;
    assert!(
        !result
            .events
            .iter()
            .any(|event| matches!(event, OrderEventAny::Filled(_)))
    );
    assert!(
        ctx.cache
            .borrow()
            .positions(None, None, None, None, None)
            .is_empty()
    );
}

#[tokio::test]
async fn explicit_flat_report_closes_cached_position_through_execution_path() {
    let server = MockServer::new(|request| {
        if request.path == "/papi/v1/um/positionRisk" {
            let mut row = testing::position("BTCUSDT");
            row["positionAmt"] = json!("0.000");
            row["entryPrice"] = json!("0");
            Reply::json(&json!([row]))
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let mut client = ReadClient::new(testing::client(&server, &["BTCUSDT"]));
    client.bulk_position_coverage = true;
    let mut ctx = EngineContext::new(client.clone());
    ctx.add_position();
    let (handler, portfolio_events) = get_typed_into_message_saving_handler::<OrderEventAny>(None);
    let endpoint = MessagingSwitchboard::portfolio_update_order();
    msgbus::register_order_event_endpoint(endpoint, handler);

    let events = ctx.manager.check_positions_consistency(&[&client]).await;

    for event in &events {
        ctx.engine.borrow_mut().process(event);
    }
    msgbus::deregister_any(endpoint);

    let fills: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            OrderEventAny::Filled(fill) => Some(fill),
            _ => None,
        })
        .collect();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].order_side, OrderSide::Sell);
    assert_eq!(fills[0].last_qty, Quantity::from("0.010"));
    assert!(fills[0].reconciliation);
    assert!(
        ctx.cache
            .borrow()
            .positions_open(None, None, None, None, None)
            .is_empty()
    );
    assert_eq!(
        portfolio_events
            .get_messages()
            .iter()
            .filter(|event| matches!(event, OrderEventAny::Filled(_)))
            .count(),
        1
    );
}

#[tokio::test]
async fn failed_periodic_reads_preserve_cached_orders_and_positions() {
    let server = MockServer::new(|request| {
        if matches!(
            request.path.as_str(),
            "/papi/v1/um/openOrders" | "/papi/v1/um/positionRisk"
        ) {
            Reply::raw(503, "{}")
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let client = ReadClient::new(testing::client(&server, &["BTCUSDT"]));
    let mut ctx = EngineContext::new(client.clone());
    ctx.add_accepted_order();
    ctx.add_position();

    for _ in 0..2 {
        let orders = ctx.manager.check_open_orders(&[&client]).await;
        let positions = ctx.manager.check_positions_consistency(&[&client]).await;
        assert!(orders.is_empty());
        assert!(positions.is_empty());
    }

    let cache = ctx.cache.borrow();
    let order = cache.order(&ClientOrderId::from("abc")).unwrap();
    assert_eq!(order.status(), OrderStatus::Accepted);
    assert!(order.filled_qty().is_zero());
    let positions = cache.positions_open(None, None, None, None, None);
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].signed_decimal_qty(), dec!(0.010));
    assert_eq!(
        ctx.manager
            .recon_check_retry_count(&ClientOrderId::from("abc")),
        0
    );
    assert_eq!(
        ctx.manager.position_recon_retry_count(&(
            InstrumentId::from("BTCUSDT-PERP.BINANCE"),
            account_id()
        )),
        0
    );
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|r| r.path == "/papi/v1/um/openOrders")
            .count(),
        6
    );
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|r| r.path == "/papi/v1/um/positionRisk")
            .count(),
        6
    );
}

#[rstest]
#[case(400, -2013)]
#[case(401, -2015)]
#[case(503, -1000)]
#[tokio::test]
async fn failed_targeted_read_cannot_turn_an_accepted_order_into_a_missing_order(
    #[case] status: u16,
    #[case] code: i64,
) {
    let server = MockServer::new(move |request| {
        if request.path == "/papi/v1/um/order" {
            Reply::raw(
                status,
                format!(r#"{{"code":{code},"msg":"offline lookup failure"}}"#),
            )
        } else {
            testing::quiet(request)
        }
    })
    .await;
    let mut client = ReadClient::new(testing::client(&server, &["BTCUSDT"]));

    // Synthetic complete bulk coverage drives the manager's targeted lookup branch
    client.covered_bulk = Some(vec![]);
    let mut ctx = EngineContext::with_config(
        client.clone(),
        ExecutionManagerConfig {
            open_check_open_only: false,
            open_check_lookback_mins: None,
            open_check_missing_retries: 1,
            open_check_threshold_ns: DurationNanos::ZERO,
            single_order_query_delay_ms: 0,
            ..Default::default()
        },
    );
    ctx.add_accepted_order();

    for _ in 0..2 {
        let events = ctx.manager.check_open_orders(&[&client]).await;
        assert!(events.is_empty());
    }

    let cache = ctx.cache.borrow();
    let order = cache.order(&ClientOrderId::from("abc")).unwrap();
    assert_eq!(order.status(), OrderStatus::Accepted);
    assert_eq!(
        order.venue_order_id(),
        Some(VenueOrderId::from("PAPI:O:BTCUSDT:270093109"))
    );
    assert!(order.filled_qty().is_zero());
    let requests = server.requests();
    let queries: Vec<_> = requests
        .iter()
        .filter(|r| r.path == "/papi/v1/um/order")
        .collect();
    assert_eq!(queries.len(), if status == 503 { 6 } else { 2 });
    assert!(queries.iter().all(|r| r.params["orderId"] == "270093109"));
}

fn account_id() -> AccountId {
    AccountId::from("BINANCE-PAPI-001")
}

struct EngineContext {
    cache: Rc<RefCell<Cache>>,
    manager: ExecutionManager,
    engine: Rc<RefCell<ExecutionEngine>>,
}

impl EngineContext {
    fn new(client: ReadClient) -> Self {
        Self::with_config(
            client,
            ExecutionManagerConfig {
                open_check_threshold_ns: DurationNanos::ZERO,
                position_check_threshold_ns: DurationNanos::ZERO,
                ..Default::default()
            },
        )
    }

    fn with_config(client: ReadClient, config: ExecutionManagerConfig) -> Self {
        let clock = Rc::new(RefCell::new(TestClock::new()));
        clock
            .borrow_mut()
            .advance_time(ms(TRADE_TIME + 1_000), true);
        let cache = Rc::new(RefCell::new(Cache::default()));
        // Synthetic core setup; read-only PAPI snapshots never mutate this cache
        let account_state = AccountState::new(
            account_id(),
            AccountType::Margin,
            vec![AccountBalance::new(
                Money::from("1000000 USDT"),
                Money::from("0 USDT"),
                Money::from("1000000 USDT"),
            )],
            vec![],
            true,
            UUID4::new(),
            ms(TRADE_TIME),
            ms(TRADE_TIME),
            Some(Currency::USDT()),
        );
        cache
            .borrow_mut()
            .add_account(AccountAny::Margin(MarginAccount::new(account_state, true)))
            .unwrap();
        cache
            .borrow_mut()
            .add_instrument(testing::instrument("BTCUSDT"))
            .unwrap();
        let manager = ExecutionManager::new(clock.clone(), cache.clone(), config).unwrap();
        let mut engine = ExecutionEngine::new(clock, cache.clone(), None);
        engine.register_client(Box::new(client)).unwrap();
        engine.register_oms_type(StrategyId::from("EXTERNAL"), OmsType::Netting);
        Self {
            cache,
            manager,
            engine: Rc::new(RefCell::new(engine)),
        }
    }

    fn add_accepted_order(&self) {
        let order = OrderTestBuilder::new(OrderType::Limit)
            .client_order_id(ClientOrderId::from("abc"))
            .instrument_id(InstrumentId::from("BTCUSDT-PERP.BINANCE"))
            .side(OrderSide::Sell)
            .quantity(Quantity::from("0.010"))
            .price(Price::from("28511.00"))
            .build();
        let submitted = TestOrderEventStubs::submitted(&order, account_id());
        self.cache
            .borrow_mut()
            .add_order(order, None, Some(*BINANCE_PAPI_CLIENT_ID), false)
            .unwrap();
        let order = self.cache.borrow_mut().update_order(&submitted).unwrap();
        let accepted = TestOrderEventStubs::accepted(
            &order,
            account_id(),
            VenueOrderId::from("PAPI:O:BTCUSDT:270093109"),
        );
        self.cache.borrow_mut().update_order(&accepted).unwrap();
    }

    fn add_position(&self) {
        let instrument = testing::instrument("BTCUSDT");
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(instrument.id())
            .strategy_id(StrategyId::from("EXTERNAL"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from("0.010"))
            .build();
        let fill: OrderFilled = TestOrderEventStubs::filled(
            &order,
            &instrument,
            Some(TradeId::from("cached-trade")),
            Some(PositionId::from("BTCUSDT-PERP.BINANCE-EXTERNAL")),
            Some(Price::from("28511.00")),
            Some(Quantity::from("0.010")),
            None,
            None,
            None,
            Some(account_id()),
        )
        .into();
        let position = Position::new(&instrument, fill);
        self.cache
            .borrow_mut()
            .add_position(&position, OmsType::Netting)
            .unwrap();
    }
}

/// Test-only bridge for exercising the read layer through existing core interfaces.
#[derive(Clone)]
struct ReadClient {
    reader: BinancePapiReadOnlyClient,
    covered_bulk: Option<Vec<OrderStatusReport>>,
    bulk_position_coverage: bool,
}

impl ReadClient {
    fn new(reader: BinancePapiReadOnlyClient) -> Self {
        Self {
            reader,
            covered_bulk: None,
            bulk_position_coverage: false,
        }
    }
}

#[async_trait(?Send)]
impl ExecutionClient for ReadClient {
    fn is_connected(&self) -> bool {
        true
    }

    fn client_id(&self) -> ClientId {
        *BINANCE_PAPI_CLIENT_ID
    }

    fn account_id(&self) -> AccountId {
        account_id()
    }

    fn venue(&self) -> Venue {
        *BINANCE_PAPI_VENUE
    }

    fn oms_type(&self) -> OmsType {
        OmsType::Netting
    }

    fn get_account(&self) -> Option<AccountAny> {
        None
    }

    fn provides_bulk_position_coverage(&self, _instrument_id: InstrumentId) -> bool {
        self.bulk_position_coverage
    }

    fn generate_account_state(
        &self,
        _balances: Vec<AccountBalance>,
        _margins: Vec<MarginBalance>,
        _reported: bool,
        _ts_event: UnixNanos,
        _info: Option<Params>,
    ) -> anyhow::Result<()> {
        anyhow::bail!("PAPI account projection is unavailable")
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn calculate_commission(
        &self,
        _instrument: &InstrumentAny,
        _last_qty: Quantity,
        _last_px: Price,
        _liquidity_side: LiquiditySide,
    ) -> anyhow::Result<Option<Money>> {
        anyhow::bail!("An exact venue commission is required")
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let instrument = cmd
            .instrument_id
            .ok_or_else(|| anyhow::anyhow!("Instrument scope is required"))?;
        self.reader
            .generate_order_status_report(instrument, cmd.venue_order_id, cmd.client_order_id)
            .await
            .map(Some)
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        if let Some(reports) = &self.covered_bulk {
            return Ok(reports.clone());
        }
        anyhow::ensure!(
            cmd.open_only,
            "PAPI standalone historical coverage is unverified"
        );
        self.reader
            .generate_open_order_status_reports(cmd.instrument_id)
            .await
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        self.reader
            .generate_position_status_reports(cmd.instrument_id)
            .await
    }

    async fn generate_fill_reports(
        &self,
        _cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        anyhow::bail!("PAPI fills require an explicitly incomplete bounded snapshot")
    }
}
