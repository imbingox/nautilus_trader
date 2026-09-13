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

//! Scoped read-only execution reports, with live account startup gated on economic mapping.

use std::cell::RefCell;

use async_trait::async_trait;
use nautilus_common::{
    clients::ExecutionClient,
    enums::LogLevel,
    messages::execution::{
        BatchCancelOrders, BatchModifyOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
        GenerateOrderStatusReport, GenerateOrderStatusReports, GeneratePositionStatusReports,
        ModifyOrder, QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList,
    },
};
use nautilus_core::{Params, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_execution::client::core::ExecutionClientCore;
use nautilus_model::{
    accounts::AccountAny,
    enums::{LiquiditySide, OmsType},
    identifiers::{AccountId, ClientId, InstrumentId, Venue},
    instruments::InstrumentAny,
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, MarginBalance, Money, Price, Quantity},
};

use crate::{config::BinancePapiExecutionClientConfig, read_only::BinancePapiReadOnlyClient};

#[derive(Debug)]
pub(crate) struct BinancePapiExecutionClient {
    core: ExecutionClientCore,
    config: BinancePapiExecutionClientConfig,
    reader: RefCell<Option<BinancePapiReadOnlyClient>>,
}

impl BinancePapiExecutionClient {
    pub(crate) const fn new(
        core: ExecutionClientCore,
        config: BinancePapiExecutionClientConfig,
    ) -> Self {
        Self {
            core,
            config,
            reader: RefCell::new(None),
        }
    }

    fn reader(&self) -> anyhow::Result<BinancePapiReadOnlyClient> {
        anyhow::ensure!(
            self.core.is_started(),
            "PAPI read-only client is not started"
        );

        if let Some(client) = self.reader.borrow().as_ref() {
            return Ok(client.clone());
        }

        let config = self
            .config
            .read_only
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("PAPI read-only configuration is required"))?;
        let instruments = {
            let cache = self.core.cache();
            self.config
                .instrument_ids
                .iter()
                .map(|id| {
                    cache
                        .instrument(id)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("PAPI instrument is not preloaded: {id}"))
                })
                .collect::<anyhow::Result<Vec<_>>>()?
        };
        let client = BinancePapiReadOnlyClient::new(config, instruments)?;
        *self.reader.borrow_mut() = Some(client.clone());
        Ok(client)
    }

    fn cancel_reads(&mut self) {
        if let Some(client) = self.reader.get_mut().take() {
            client.cancel();
        }

        self.core.set_disconnected();
        self.core.set_stopped();
    }

    fn log_report_receipt(count: usize, report_type: &str, level: LogLevel) {
        let message = format!("Received {count} PAPI {report_type} reports");

        match level {
            LogLevel::Off => {}
            LogLevel::Trace => log::trace!("{message}"),
            LogLevel::Debug => log::debug!("{message}"),
            LogLevel::Info => log::info!("{message}"),
            LogLevel::Warning => log::warn!("{message}"),
            LogLevel::Error => log::error!("{message}"),
        }
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

    fn provides_bulk_position_coverage(&self, instrument_id: InstrumentId) -> bool {
        self.config.instrument_ids.contains(&instrument_id)
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
        self.config.validate()?;
        anyhow::ensure!(
            self.config.read_only.is_some(),
            "PAPI read-only configuration is required"
        );
        self.core.set_started();
        Ok(())
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        self.cancel_reads();
        anyhow::bail!(
            "PAPI LiveNode startup requires an accepted economic account balance mapping; \
             use BinancePapiReadOnlyClient for account evidence and reports"
        )
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.cancel_reads();
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.cancel_reads();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.cancel_reads();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.cancel_reads();
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
        cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        let instrument_id = cmd
            .instrument_id
            .ok_or_else(|| anyhow::anyhow!("PAPI single-order queries require an instrument ID"))?;
        self.reader()?
            .generate_order_status_report(instrument_id, cmd.venue_order_id, cmd.client_order_id)
            .await
            .map(Some)
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        anyhow::ensure!(
            cmd.open_only,
            "PAPI history is incomplete; use the bounded mass status report"
        );
        let reports = self
            .reader()?
            .generate_open_order_status_reports(cmd.instrument_id)
            .await?;
        Self::log_report_receipt(reports.len(), "order status", cmd.log_receipt_level);
        Ok(reports)
    }

    async fn generate_fill_reports(
        &self,
        _cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        anyhow::bail!("PAPI fill history is incomplete; use the bounded mass status report")
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        anyhow::ensure!(
            cmd.start.is_none() && cmd.end.is_none(),
            "PAPI position reports support current observations only"
        );
        let reports = self
            .reader()?
            .generate_position_status_reports(cmd.instrument_id)
            .await?;
        Self::log_report_receipt(reports.len(), "position status", cmd.log_receipt_level);
        Ok(reports)
    }

    async fn generate_mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        let end = get_atomic_clock_realtime().get_time_ns();
        let start = lookback_mins
            .unwrap_or(60)
            .checked_mul(60_000_000_000)
            .and_then(|lookback| end.as_u64().checked_sub(lookback))
            .ok_or_else(|| anyhow::anyhow!("PAPI history lookback exceeds timestamp bounds"))?;
        let mut snapshot = self
            .reader()?
            .generate_mass_status(start.into(), end)
            .await?;
        snapshot.mass_status.client_id = self.core.client_id;
        Ok(Some(snapshot.mass_status))
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
    use serde_json::json;

    use super::*;
    use crate::{
        config::BinancePapiExecutionClientConfig,
        factories::BinancePapiExecutionClientFactory,
        testing::{self, MockServer, Reply, TRADE_TIME, ms},
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

    fn configured_client(
        server: &MockServer,
        symbols: &[&str],
        preload: bool,
    ) -> Box<dyn ExecutionClient> {
        let cache = Rc::new(RefCell::new(Cache::default()));

        if preload {
            for symbol in symbols {
                cache
                    .borrow_mut()
                    .add_instrument(testing::instrument(symbol))
                    .unwrap();
            }
        }

        let config = BinancePapiExecutionClientConfig {
            read_only: Some(testing::config(&server.url)),
            instrument_ids: symbols
                .iter()
                .map(|symbol| InstrumentId::from(format!("{symbol}-PERP.BINANCE")))
                .collect(),
            ..Default::default()
        };
        BinancePapiExecutionClientFactory::new()
            .create(
                TraderId::from("TRADER-001"),
                "PAPI-READ-007",
                &config,
                cache.into(),
            )
            .unwrap()
    }

    #[tokio::test]
    async fn test_factory_reports_preserve_scope_identity_and_incompleteness() {
        let server = MockServer::new(|request| match request.path.as_str() {
            "/papi/v1/um/openOrders" if request.params["symbol"] == "BTCUSDT" => {
                Reply::json(&json!([testing::order()]))
            }
            "/papi/v1/um/order" => Reply::json(&testing::order()),
            _ => testing::quiet(request),
        })
        .await;
        let mut client = configured_client(&server, &["BTCUSDT", "ETHUSDT"], true);
        assert!(server.requests().is_empty());
        client.start().unwrap();
        let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
        let orders = GenerateOrderStatusReportsBuilder::default()
            .ts_init(UnixNanos::default())
            .instrument_id(Some(instrument_id))
            .open_only(true)
            .start(Some(ms(TRADE_TIME + 1)))
            .build()
            .unwrap();
        let positions = GeneratePositionStatusReportsBuilder::default()
            .ts_init(UnixNanos::default())
            .build()
            .unwrap();
        let single = GenerateOrderStatusReportBuilder::default()
            .ts_init(UnixNanos::default())
            .instrument_id(Some(instrument_id))
            .client_order_id(Some(ClientOrderId::from("abc")))
            .build()
            .unwrap();
        let open = client.generate_order_status_reports(&orders).await.unwrap();
        let position_reports = client
            .generate_position_status_reports(&positions)
            .await
            .unwrap();
        let order = client
            .generate_order_status_report(&single)
            .await
            .unwrap()
            .unwrap();
        let mass = client
            .generate_mass_status(Some(60))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(open.len(), 1);
        assert_eq!(open[0].instrument_id, instrument_id);
        assert_eq!(open[0].venue_order_id, order.venue_order_id);
        assert_eq!(position_reports.len(), 2);
        assert_eq!(mass.client_id, ClientId::from("PAPI-READ-007"));
        assert_eq!(mass.account_id, client.account_id());
        assert!(mass.lookback_start().is_some());
        assert!(!mass.reports_complete());
        assert!(client.provides_bulk_position_coverage(instrument_id));
        assert!(
            !client.provides_bulk_position_coverage(InstrumentId::from("BNBUSDT-PERP.BINANCE"))
        );
        assert!(!client.is_connected());
        assert!(client.get_account().is_none());
        assert!(server.requests().iter().all(|request| {
            request.method == "GET" && request.params.contains_key("signature")
        }));
        client.stop().unwrap();
        assert!(client.generate_order_status_reports(&orders).await.is_err());
        client.start().unwrap();
        assert_eq!(
            client
                .generate_order_status_reports(&orders)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn test_factory_failed_queries_remain_errors_and_never_publish_an_account() {
        let server = MockServer::new(|request| match request.path.as_str() {
            "/papi/v1/um/openOrders" | "/papi/v1/um/positionRisk" => Reply::raw(401, "{}"),
            _ => testing::quiet(request),
        })
        .await;
        let mut client = configured_client(&server, &["BTCUSDT"], true);
        client.start().unwrap();
        let orders = GenerateOrderStatusReportsBuilder::default()
            .ts_init(UnixNanos::default())
            .open_only(true)
            .build()
            .unwrap();
        let positions = GeneratePositionStatusReportsBuilder::default()
            .ts_init(UnixNanos::default())
            .build()
            .unwrap();
        let single = GenerateOrderStatusReportBuilder::default()
            .ts_init(UnixNanos::default())
            .instrument_id(Some(InstrumentId::from("BTCUSDT-PERP.BINANCE")))
            .client_order_id(Some(ClientOrderId::from("abc")))
            .build()
            .unwrap();

        assert!(client.generate_order_status_reports(&orders).await.is_err());
        assert!(
            client
                .generate_position_status_reports(&positions)
                .await
                .is_err()
        );
        assert!(client.generate_order_status_report(&single).await.is_err());
        assert!(client.generate_mass_status(None).await.is_err());
        assert!(client.get_account().is_none());
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn test_factory_rejects_unverifiable_filters_and_history_vectors_before_requests() {
        let server = MockServer::new(testing::quiet).await;
        let mut client = configured_client(&server, &["BTCUSDT"], true);
        client.start().unwrap();
        let history = GenerateOrderStatusReportsBuilder::default()
            .ts_init(UnixNanos::default())
            .open_only(false)
            .build()
            .unwrap();
        let fills = GenerateFillReportsBuilder::default()
            .ts_init(UnixNanos::default())
            .build()
            .unwrap();
        let positions = GeneratePositionStatusReportsBuilder::default()
            .ts_init(UnixNanos::default())
            .start(Some(ms(TRADE_TIME)))
            .build()
            .unwrap();

        assert!(
            client
                .generate_order_status_reports(&history)
                .await
                .is_err()
        );
        assert!(client.generate_fill_reports(fills).await.is_err());
        assert!(
            client
                .generate_position_status_reports(&positions)
                .await
                .is_err()
        );
        assert!(client.generate_mass_status(Some(u64::MAX)).await.is_err());
        assert!(server.requests().is_empty());
    }

    #[tokio::test]
    async fn test_missing_metadata_and_unaccepted_account_mapping_prevent_bootstrap() {
        let server = MockServer::new(testing::quiet).await;
        let mut client = configured_client(&server, &["BTCUSDT"], false);
        client.start().unwrap();
        let positions = GeneratePositionStatusReportsBuilder::default()
            .ts_init(UnixNanos::default())
            .build()
            .unwrap();
        let error = client
            .generate_position_status_reports(&positions)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not preloaded"));
        assert!(
            client
                .connect()
                .await
                .unwrap_err()
                .to_string()
                .contains("economic account")
        );
        assert!(!client.is_connected());
        assert!(client.get_account().is_none());
        assert!(server.requests().is_empty());

        for _ in 0..2 {
            client.stop().unwrap();
            client.disconnect().await.unwrap();
            client.reset().unwrap();
            client.dispose().unwrap();
        }
    }

    #[tokio::test]
    async fn test_start_and_connect_fail_without_account_or_connection() {
        let mut client = client();
        assert!(
            client
                .start()
                .unwrap_err()
                .to_string()
                .contains("read-only configuration is required")
        );
        assert!(
            client
                .connect()
                .await
                .unwrap_err()
                .to_string()
                .contains("economic account balance mapping")
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
