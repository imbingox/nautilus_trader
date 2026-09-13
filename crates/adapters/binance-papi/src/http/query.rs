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

use std::collections::BTreeMap;

use serde_json::Value;

use crate::observations::ObservationSource;

pub(crate) const PAGE_LIMIT: usize = 1_000;

/// Only signed GET endpoints can be constructed at this boundary.
#[derive(Clone, Debug)]
pub(crate) enum PapiRequest {
    Observation(ObservationSource),
    PositionMode,
    Positions {
        symbol: String,
    },
    OpenOrders {
        symbol: String,
    },
    Order {
        symbol: String,
        order_id: Option<i64>,
        client_order_id: Option<String>,
    },
    OpenAlgos {
        symbol: String,
    },
    History {
        endpoint: HistoryEndpoint,
        symbol: String,
        start: i64,
        end: i64,
    },
    Algo {
        symbol: String,
        algo_id: i64,
    },
    OrderRateLimit,
}

impl PapiRequest {
    pub(crate) fn endpoint(&self) -> &'static str {
        match self {
            Self::Observation(source) => source.endpoint(),
            Self::PositionMode => "/papi/v1/um/positionSide/dual",
            Self::Positions { .. } => "/papi/v1/um/positionRisk",
            Self::OpenOrders { .. } => "/papi/v1/um/openOrders",
            Self::Order { .. } => "/papi/v1/um/order",
            Self::OpenAlgos { .. } => "/papi/v1/um/algo/openAlgoOrders",
            Self::History { endpoint, .. } => endpoint.path(),
            Self::Algo { .. } => HistoryEndpoint::Algos.path(),
            Self::OrderRateLimit => "/papi/v1/rateLimit/order",
        }
    }

    pub(crate) const fn weight(&self) -> usize {
        match self {
            Self::Observation(ObservationSource::Balance { .. } | ObservationSource::Account) => 20,
            Self::Observation(ObservationSource::UmAccountV1 | ObservationSource::UmAccountV2)
            | Self::Positions { .. }
            | Self::History { .. }
            | Self::Algo { .. } => 5,
            Self::PositionMode => 30,
            Self::OpenOrders { .. }
            | Self::Order { .. }
            | Self::OpenAlgos { .. }
            | Self::OrderRateLimit => 1,
        }
    }

    pub(crate) fn symbol(&self) -> Option<&str> {
        match self {
            Self::Positions { symbol }
            | Self::OpenOrders { symbol }
            | Self::Order { symbol, .. }
            | Self::OpenAlgos { symbol }
            | Self::History { symbol, .. }
            | Self::Algo { symbol, .. } => Some(symbol),
            _ => None,
        }
    }

    pub(crate) fn params(&self) -> BTreeMap<String, Value> {
        let mut params = BTreeMap::from([("recvWindow".to_owned(), Value::from(5_000))]);

        if let Some(symbol) = self.symbol() {
            params.insert("symbol".to_owned(), Value::from(symbol));
        }

        match self {
            Self::Observation(ObservationSource::Balance { asset: Some(asset) }) => {
                params.insert("asset".to_owned(), Value::from(asset.as_str()));
            }
            Self::Order {
                order_id,
                client_order_id,
                ..
            } => {
                if let Some(id) = order_id {
                    params.insert("orderId".to_owned(), Value::from(*id));
                }

                if let Some(id) = client_order_id {
                    params.insert("origClientOrderId".to_owned(), Value::from(id.as_str()));
                }
            }
            Self::History { start, end, .. } => {
                params.insert("startTime".to_owned(), Value::from(*start));
                params.insert("endTime".to_owned(), Value::from(*end));
                params.insert("limit".to_owned(), Value::from(PAGE_LIMIT));
            }
            Self::Algo { algo_id, .. } => {
                params.insert("algoId".to_owned(), Value::from(*algo_id));
                params.insert("limit".to_owned(), Value::from(1));
            }
            _ => {}
        }

        params
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum HistoryEndpoint {
    Orders,
    Algos,
    Trades,
}

impl HistoryEndpoint {
    pub(crate) const fn path(self) -> &'static str {
        match self {
            Self::Orders => "/papi/v1/um/allOrders",
            Self::Algos => "/papi/v1/um/algo/allAlgoOrders",
            Self::Trades => "/papi/v1/um/userTrades",
        }
    }
}
