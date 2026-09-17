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

use std::collections::{BTreeMap, BTreeSet};

use super::messages::{AlgoFact, DirtySource, OrderFact, PapiWsEvent};

#[derive(Debug)]
pub(super) struct FactState {
    max_facts: usize,
    orders: BTreeMap<(String, i64), OrderFact>,
    algos: BTreeMap<(String, i64), AlgoFact>,
    trades: BTreeMap<(String, i64, i64), super::messages::FillFact>,
    dirty: BTreeSet<DirtySourceKey>,
    pub fact_version: u64,
    pub duplicate_count: u64,
    pub conflict_count: u64,
}

impl FactState {
    pub(super) fn new(max_facts: usize) -> Self {
        Self {
            max_facts,
            orders: BTreeMap::new(),
            algos: BTreeMap::new(),
            trades: BTreeMap::new(),
            dirty: BTreeSet::new(),
            fact_version: 0,
            duplicate_count: 0,
            conflict_count: 0,
        }
    }

    pub(super) fn apply(&mut self, event: PapiWsEvent) -> Result<(), String> {
        match event {
            PapiWsEvent::Order(order) => self.apply_order(order),
            PapiWsEvent::Algo(algo) => self.apply_algo(algo),
            PapiWsEvent::Account(account) => {
                self.dirty.insert(DirtySourceKey::Wallet);
                self.dirty.insert(DirtySourceKey::Risk);
                self.dirty.insert(DirtySourceKey::Positions);

                for position in account.positions {
                    self.dirty
                        .insert(DirtySourceKey::Instrument(position.symbol));
                }
                self.advance_version()
            }
            PapiWsEvent::Dirty(dirty) => {
                self.dirty.insert(dirty.source.into());
                self.advance_version()
            }
            PapiWsEvent::ListenKeyExpired { .. } => Err("PAPI listen key expired".to_string()),
        }
    }

    pub(super) fn has_dirty_sources(&self) -> bool {
        !self.dirty.is_empty()
    }

    pub(super) fn clear_dirty_sources(&mut self) {
        self.dirty.clear();
    }

    pub(super) fn fact_count(&self) -> usize {
        self.orders.len() + self.algos.len() + self.trades.len()
    }

    fn apply_order(&mut self, order: OrderFact) -> Result<(), String> {
        let key = (order.symbol.clone(), order.order_id);
        let trade = order
            .fill
            .as_ref()
            .map(|fill| ((order.symbol.clone(), order.order_id, fill.trade_id), fill));
        let insert_trade = match trade.as_ref() {
            Some((trade_key, fill)) => match self.trades.get(trade_key) {
                Some(existing) if existing == *fill => false,
                Some(_) => {
                    self.conflict_count = self.conflict_count.saturating_add(1);
                    return Err("Conflicting PAPI fill identity".to_string());
                }
                None => true,
            },
            None => false,
        };
        let insert_order = match self.orders.get(&key) {
            Some(existing) if existing.client_order_id != order.client_order_id => {
                self.conflict_count = self.conflict_count.saturating_add(1);
                return Err("Conflicting PAPI order identity".to_string());
            }
            Some(existing) => {
                existing != &order
                    && order.accumulated_qty >= existing.accumulated_qty
                    && (!existing.is_terminal() || order.is_terminal())
            }
            None => true,
        };
        let additions = usize::from(insert_trade)
            + usize::from(insert_order && !self.orders.contains_key(&key));

        if self.fact_count().saturating_add(additions) > self.max_facts {
            return Err("PAPI fact retention bound exhausted".to_string());
        }

        if insert_trade && let Some((trade_key, fill)) = trade {
            self.trades.insert(trade_key, fill.clone());
        }

        if insert_order {
            self.orders.insert(key, order.clone());
        }

        if insert_trade || insert_order {
            self.dirty.insert(DirtySourceKey::Orders);
            self.dirty.insert(DirtySourceKey::Instrument(order.symbol));
            self.advance_version()
        } else {
            self.duplicate_count = self.duplicate_count.saturating_add(1);
            Ok(())
        }
    }

    fn apply_algo(&mut self, algo: AlgoFact) -> Result<(), String> {
        let key = (algo.symbol.clone(), algo.algo_id);

        match self.algos.get(&key) {
            Some(existing) if existing == &algo => {
                self.duplicate_count = self.duplicate_count.saturating_add(1);
                return Ok(());
            }
            Some(existing)
                if existing.client_algo_id != algo.client_algo_id
                    || (existing.actual_order_id.is_some()
                        && algo.actual_order_id.is_some()
                        && existing.actual_order_id != algo.actual_order_id) =>
            {
                self.conflict_count = self.conflict_count.saturating_add(1);
                return Err("Conflicting PAPI algo identity".to_string());
            }
            Some(_) => {}
            None => self.ensure_capacity()?,
        }

        self.dirty.insert(DirtySourceKey::Orders);
        self.dirty
            .insert(DirtySourceKey::Instrument(algo.symbol.clone()));
        self.algos.insert(key, algo);
        self.advance_version()
    }

    fn ensure_capacity(&self) -> Result<(), String> {
        if self.fact_count() >= self.max_facts {
            Err("PAPI fact retention bound exhausted".to_string())
        } else {
            Ok(())
        }
    }

    fn advance_version(&mut self) -> Result<(), String> {
        self.fact_version = self
            .fact_version
            .checked_add(1)
            .ok_or_else(|| "PAPI fact version overflow".to_string())?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum DirtySourceKey {
    Wallet,
    Risk,
    Orders,
    Positions,
    ProductScope,
    Configuration,
    Instrument(String),
}

impl From<DirtySource> for DirtySourceKey {
    fn from(value: DirtySource) -> Self {
        match value {
            DirtySource::Wallet => Self::Wallet,
            DirtySource::Risk => Self::Risk,
            DirtySource::Orders => Self::Orders,
            DirtySource::ProductScope => Self::ProductScope,
            DirtySource::Configuration => Self::Configuration,
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::websocket::messages::FillFact;

    fn order(trade_id: i64, accumulated_qty: rust_decimal::Decimal) -> OrderFact {
        OrderFact {
            symbol: "BTCUSDT".to_string(),
            client_order_id: "client-1".to_string(),
            order_id: 42,
            execution_type: "TRADE".to_string(),
            status: "PARTIALLY_FILLED".to_string(),
            accumulated_qty,
            event_time_ms: 1_700_000_000_000,
            transaction_time_ms: 1_700_000_000_000,
            fill: Some(FillFact {
                trade_id,
                quantity: dec!(0.001),
                price: dec!(42000),
                commission: dec!(0.1),
                commission_asset: "USDT".to_string(),
                maker: false,
                trade_time_ms: 1_700_000_000_000,
            }),
        }
    }

    #[rstest]
    fn same_millisecond_distinct_trade_ids_are_retained() {
        let mut state = FactState::new(8);
        state
            .apply(PapiWsEvent::Order(order(7, dec!(0.001))))
            .unwrap();
        state
            .apply(PapiWsEvent::Order(order(8, dec!(0.002))))
            .unwrap();
        assert_eq!(state.trades.len(), 2);
        assert_eq!(state.fact_version, 2);
    }

    #[rstest]
    fn duplicate_trade_is_idempotent_and_conflict_restricts() {
        let mut state = FactState::new(8);
        let fact = order(7, dec!(0.001));
        state.apply(PapiWsEvent::Order(fact.clone())).unwrap();
        state.apply(PapiWsEvent::Order(fact.clone())).unwrap();
        assert!(state.duplicate_count > 0);

        let mut conflict = fact;
        conflict.fill.as_mut().unwrap().commission = dec!(0.2);
        assert!(state.apply(PapiWsEvent::Order(conflict)).is_err());
        assert_eq!(state.conflict_count, 1);
    }

    #[rstest]
    fn old_order_update_cannot_reduce_cumulative_quantity() {
        let mut state = FactState::new(8);
        state
            .apply(PapiWsEvent::Order(order(8, dec!(0.002))))
            .unwrap();
        state
            .apply(PapiWsEvent::Order(order(7, dec!(0.001))))
            .unwrap();
        assert_eq!(
            state.orders.values().next().unwrap().accumulated_qty,
            dec!(0.002)
        );
    }

    #[rstest]
    fn retention_exhaustion_never_evicts_old_trade_identity() {
        let mut state = FactState::new(2);
        state
            .apply(PapiWsEvent::Order(order(7, dec!(0.001))))
            .unwrap();
        assert!(
            state
                .apply(PapiWsEvent::Order(order(8, dec!(0.002))))
                .is_err()
        );
        assert!(state.trades.keys().any(|(_, _, trade_id)| *trade_id == 7));
    }

    #[rstest]
    fn conflicting_order_does_not_partially_insert_its_new_trade() {
        let mut state = FactState::new(8);
        state
            .apply(PapiWsEvent::Order(order(7, dec!(0.001))))
            .unwrap();
        let version = state.fact_version;
        let mut conflict = order(8, dec!(0.002));
        conflict.client_order_id = "different-client".to_string();

        assert!(state.apply(PapiWsEvent::Order(conflict)).is_err());
        assert_eq!(state.fact_version, version);
        assert!(!state.trades.keys().any(|(_, _, trade_id)| *trade_id == 8));
    }

    #[rstest]
    fn stale_order_with_new_trade_retains_fill_and_advances_checkpoint() {
        let mut state = FactState::new(8);
        state
            .apply(PapiWsEvent::Order(order(8, dec!(0.002))))
            .unwrap();
        let version = state.fact_version;
        state
            .apply(PapiWsEvent::Order(order(7, dec!(0.001))))
            .unwrap();

        assert_eq!(state.fact_version, version + 1);
        assert!(state.trades.keys().any(|(_, _, trade_id)| *trade_id == 7));
        assert_eq!(
            state.orders.values().next().unwrap().accumulated_qty,
            dec!(0.002)
        );
    }
}
