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

//! Construction-only PAPI client, rejecting all unimplemented execution operations.

use async_trait::async_trait;
use nautilus_common::{
    clients::ExecutionClient,
    messages::execution::{
        BatchCancelOrders, BatchModifyOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
        GenerateOrderStatusReport, GenerateOrderStatusReports, GeneratePositionStatusReports,
        ModifyOrder, QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList,
    },
};
use nautilus_core::{Params, UnixNanos};
use nautilus_execution::client::core::ExecutionClientCore;
use nautilus_model::{
    accounts::AccountAny,
    enums::{LiquiditySide, OmsType},
    identifiers::{AccountId, ClientId, InstrumentId, Venue},
    instruments::InstrumentAny,
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, MarginBalance, Money, Price, Quantity},
};

#[derive(Debug)]
pub(crate) struct BinancePapiExecutionClient {
    core: ExecutionClientCore,
}

impl BinancePapiExecutionClient {
    pub(crate) const fn new(core: ExecutionClientCore) -> Self {
        Self { core }
    }
}

#[async_trait(?Send)]
impl ExecutionClient for BinancePapiExecutionClient {
    fn is_connected(&self) -> bool {
        false
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        self.core.venue
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        None
    }

    fn provides_bulk_position_coverage(&self, _instrument_id: InstrumentId) -> bool {
        false
    }

    fn generate_account_state(
        &self,
        _balances: Vec<AccountBalance>,
        _margins: Vec<MarginBalance>,
        _reported: bool,
        _ts_event: UnixNanos,
        _info: Option<Params>,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Binance PAPI account state is not implemented")
    }

    fn calculate_commission(
        &self,
        _instrument: &InstrumentAny,
        _last_qty: Quantity,
        _last_px: Price,
        _liquidity_side: LiquiditySide,
    ) -> anyhow::Result<Option<Money>> {
        anyhow::bail!("Binance PAPI commission calculation is not implemented")
    }

    fn start(&mut self) -> anyhow::Result<()> {
        anyhow::bail!("Binance PAPI execution is not implemented; node construction only")
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        anyhow::bail!("Binance PAPI connection is not implemented; node construction only")
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        // No resources can be acquired because start and connect always fail
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        // Already disconnected; teardown remains idempotent after a failed start
        Ok(())
    }

    fn submit_order(&self, _cmd: SubmitOrder) -> anyhow::Result<()> {
        anyhow::bail!("Binance PAPI submit_order is not implemented")
    }

    fn submit_order_list(&self, _cmd: SubmitOrderList) -> anyhow::Result<()> {
        anyhow::bail!("Binance PAPI submit_order_list is not implemented")
    }

    fn modify_order(&self, _cmd: ModifyOrder) -> anyhow::Result<()> {
        anyhow::bail!("Binance PAPI modify_order is not implemented")
    }

    fn batch_modify_orders(&self, _cmd: BatchModifyOrders) -> anyhow::Result<()> {
        anyhow::bail!("Binance PAPI batch_modify_orders is not implemented")
    }

    fn cancel_order(&self, _cmd: CancelOrder) -> anyhow::Result<()> {
        anyhow::bail!("Binance PAPI cancel_order is not implemented")
    }

    fn cancel_all_orders(&self, _cmd: CancelAllOrders) -> anyhow::Result<()> {
        anyhow::bail!("Binance PAPI cancel_all_orders is not implemented")
    }

    fn batch_cancel_orders(&self, _cmd: BatchCancelOrders) -> anyhow::Result<()> {
        anyhow::bail!("Binance PAPI batch_cancel_orders is not implemented")
    }

    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        anyhow::bail!("Binance PAPI query_account is not implemented")
    }

    fn query_order(&self, _cmd: QueryOrder) -> anyhow::Result<()> {
        anyhow::bail!("Binance PAPI query_order is not implemented")
    }

    async fn generate_order_status_report(
        &self,
        _cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        anyhow::bail!("Binance PAPI generate_order_status_report is not implemented")
    }

    async fn generate_order_status_reports(
        &self,
        _cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        anyhow::bail!("Binance PAPI generate_order_status_reports is not implemented")
    }

    async fn generate_fill_reports(
        &self,
        _cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        anyhow::bail!("Binance PAPI generate_fill_reports is not implemented")
    }

    async fn generate_position_status_reports(
        &self,
        _cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        anyhow::bail!("Binance PAPI generate_position_status_reports is not implemented")
    }

    async fn generate_mass_status(
        &self,
        _lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        anyhow::bail!("Binance PAPI generate_mass_status is not implemented")
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::{
        cache::Cache,
        factories::ExecutionClientFactory,
        messages::execution::{
            GenerateFillReportsBuilder, GenerateOrderStatusReportBuilder,
            GenerateOrderStatusReportsBuilder, GeneratePositionStatusReportsBuilder,
        },
    };
    use nautilus_core::UUID4;
    use nautilus_model::{
        enums::{OrderSide, TimeInForce},
        events::OrderInitialized,
        identifiers::{ClientOrderId, OrderListId, StrategyId, TraderId},
        orders::{MarketOrder, OrderAny, OrderList},
    };
    use rstest::rstest;

    use super::*;
    use crate::{
        config::BinancePapiExecutionClientConfig, factories::BinancePapiExecutionClientFactory,
    };

    fn client() -> Box<dyn ExecutionClient> {
        BinancePapiExecutionClientFactory::new()
            .create(
                TraderId::from("TRADER-001"),
                "BINANCE_PAPI",
                &BinancePapiExecutionClientConfig::default(),
                Rc::new(RefCell::new(Cache::default())).into(),
            )
            .unwrap()
    }

    #[tokio::test]
    async fn test_start_and_connect_fail_without_account_or_connection() {
        let mut client = client();
        assert!(
            client
                .start()
                .unwrap_err()
                .to_string()
                .contains("not implemented")
        );
        assert!(
            client
                .connect()
                .await
                .unwrap_err()
                .to_string()
                .contains("not implemented")
        );
        assert!(!client.is_connected());
        assert!(client.get_account().is_none());
        assert!(
            !client.provides_bulk_position_coverage(InstrumentId::from("BTCUSDT-PERP.BINANCE"))
        );

        // Repeated cleanup after a failed start must remain safe
        for _ in 0..2 {
            client.stop().unwrap();
            client.disconnect().await.unwrap();
            client.reset().unwrap();
            client.dispose().unwrap();
        }
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn test_reconciliation_never_reports_empty_success() {
        let client = client();
        let ts_init = UnixNanos::default();
        let order = GenerateOrderStatusReportBuilder::default()
            .ts_init(ts_init)
            .build()
            .unwrap();
        let orders = GenerateOrderStatusReportsBuilder::default()
            .ts_init(ts_init)
            .open_only(false)
            .build()
            .unwrap();
        let fills = GenerateFillReportsBuilder::default()
            .ts_init(ts_init)
            .build()
            .unwrap();
        let positions = GeneratePositionStatusReportsBuilder::default()
            .ts_init(ts_init)
            .build()
            .unwrap();

        assert!(client.generate_order_status_report(&order).await.is_err());
        assert!(client.generate_order_status_reports(&orders).await.is_err());
        assert!(client.generate_fill_reports(fills).await.is_err());
        assert!(
            client
                .generate_position_status_reports(&positions)
                .await
                .is_err()
        );
        assert!(client.generate_mass_status(None).await.is_err());
    }

    #[rstest]
    fn test_account_operations_fail_without_publishing_state() {
        let client = client();
        let query = QueryAccount::new(
            TraderId::from("TRADER-001"),
            Some(client.client_id()),
            client.account_id(),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );
        assert!(client.query_account(query).is_err());
        assert!(
            client
                .generate_account_state(Vec::new(), Vec::new(), true, UnixNanos::default(), None)
                .is_err()
        );
        assert!(client.get_account().is_none());
    }

    #[rstest]
    fn test_empty_batch_commands_do_not_succeed() {
        let client = client();
        let trader_id = TraderId::from("TRADER-001");
        let strategy_id = StrategyId::from("TEST-001");
        let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
        let modifies = BatchModifyOrders::new(
            trader_id,
            None,
            strategy_id,
            instrument_id,
            Vec::new(),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );
        let cancels = BatchCancelOrders::new(
            trader_id,
            None,
            strategy_id,
            instrument_id,
            Vec::new(),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        );
        assert!(client.batch_modify_orders(modifies).is_err());
        assert!(client.batch_cancel_orders(cancels).is_err());
    }
    #[rstest]
    fn test_order_commands_never_report_success() {
        let client = client();
        let trader_id = TraderId::from("TRADER-001");
        let strategy_id = StrategyId::from("TEST-001");
        let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
        let client_order_id = ClientOrderId::from("ORDER-001");
        let ts_init = UnixNanos::default();

        let order = OrderAny::Market(MarketOrder::new(
            trader_id,
            strategy_id,
            instrument_id,
            client_order_id,
            OrderSide::Buy,
            Quantity::from("0.001"),
            TimeInForce::Gtc,
            UUID4::new(),
            ts_init,
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
        ));
        let submit = SubmitOrder::from_order(&order, trader_id, None, None, UUID4::new(), ts_init);
        let list = OrderList::new(
            OrderListId::from("LIST-001"),
            instrument_id,
            strategy_id,
            vec![client_order_id],
            ts_init,
        );
        let submit_list = SubmitOrderList::new(
            trader_id,
            None,
            strategy_id,
            list,
            vec![OrderInitialized::from(&order)],
            None,
            None,
            None,
            UUID4::new(),
            ts_init,
            None,
        );
        let modify = ModifyOrder::new(
            trader_id,
            None,
            strategy_id,
            instrument_id,
            client_order_id,
            None,
            Some(Quantity::from("0.002")),
            None,
            None,
            UUID4::new(),
            ts_init,
            None,
            None,
        );
        let cancel = CancelOrder::new(
            trader_id,
            None,
            strategy_id,
            instrument_id,
            client_order_id,
            None,
            UUID4::new(),
            ts_init,
            None,
            None,
        );
        let cancel_all = CancelAllOrders::new(
            trader_id,
            None,
            strategy_id,
            instrument_id,
            None,
            UUID4::new(),
            ts_init,
            None,
            None,
        );
        let query = QueryOrder::new(
            trader_id,
            None,
            strategy_id,
            instrument_id,
            client_order_id,
            None,
            UUID4::new(),
            ts_init,
            None,
            None,
        );
        assert!(client.submit_order(submit).is_err());
        assert!(client.submit_order_list(submit_list).is_err());
        assert!(client.modify_order(modify).is_err());
        assert!(client.cancel_order(cancel).is_err());
        assert!(client.cancel_all_orders(cancel_all).is_err());
        assert!(client.query_order(query).is_err());
    }
}
