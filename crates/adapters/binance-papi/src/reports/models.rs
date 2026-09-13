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

use nautilus_binance::common::enums::{
    BinanceAlgoStatus, BinanceAlgoType, BinanceFuturesOrderType, BinanceOrderStatus,
    BinancePositionSide, BinanceSide, BinanceTimeInForce, BinanceWorkingType,
};
use serde::{Deserialize, Serialize};

use crate::observations::fields::{Amount, Field, VenueTime};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OrderRow {
    pub(crate) symbol: String,
    pub(crate) order_id: i64,
    pub(crate) client_order_id: String,
    pub(crate) side: BinanceSide,
    pub(crate) position_side: BinancePositionSide,
    pub(crate) status: BinanceOrderStatus,
    #[serde(rename = "type")]
    pub(crate) order_type: BinanceFuturesOrderType,
    pub(crate) orig_type: BinanceFuturesOrderType,
    pub(crate) time_in_force: BinanceTimeInForce,
    pub(crate) orig_qty: Amount,
    pub(crate) executed_qty: Amount,
    pub(crate) price: Amount,
    pub(crate) avg_price: Amount,
    pub(crate) reduce_only: bool,
    pub(crate) time: VenueTime,
    pub(crate) update_time: VenueTime,
    #[serde(default)]
    pub(crate) good_till_date: Field<VenueTime>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AlgoRow {
    pub(crate) symbol: String,
    pub(crate) algo_id: i64,
    pub(crate) client_algo_id: String,
    pub(crate) algo_type: BinanceAlgoType,
    pub(crate) order_type: BinanceFuturesOrderType,
    pub(crate) side: BinanceSide,
    pub(crate) position_side: BinancePositionSide,
    pub(crate) time_in_force: BinanceTimeInForce,
    pub(crate) quantity: Amount,
    pub(crate) algo_status: BinanceAlgoStatus,
    pub(crate) trigger_price: Amount,
    pub(crate) price: Amount,
    pub(crate) working_type: BinanceWorkingType,
    pub(crate) close_position: bool,
    pub(crate) reduce_only: bool,
    pub(crate) create_time: VenueTime,
    pub(crate) update_time: VenueTime,
    #[serde(default)]
    pub(crate) actual_order_id: Field<String>,
    #[serde(default)]
    pub(crate) trigger_time: Field<VenueTime>,
    #[serde(default)]
    pub(crate) good_till_date: Field<VenueTime>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TradeRow {
    pub(crate) symbol: String,
    pub(crate) id: i64,
    pub(crate) order_id: i64,
    pub(crate) side: BinanceSide,
    pub(crate) position_side: BinancePositionSide,
    pub(crate) price: Amount,
    pub(crate) qty: Amount,
    pub(crate) time: VenueTime,
    pub(crate) maker: bool,
    pub(crate) buyer: bool,
    #[serde(default)]
    pub(crate) commission: Field<Amount>,
    #[serde(default)]
    pub(crate) commission_asset: Field<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PositionRow {
    pub(crate) symbol: String,
    pub(crate) position_side: BinancePositionSide,
    pub(crate) position_amt: Amount,
    pub(crate) entry_price: Amount,
    pub(crate) update_time: VenueTime,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PositionMode {
    pub(crate) dual_side_position: bool,
}
