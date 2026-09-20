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

//! Account observations retain source fields without constructing account balances.

use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use serde_json::value::RawValue;

use super::fields::{Amount, Field, VenueTime};

/// All amounts are observations in `asset`; component inclusion remains unverified.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AssetBalance {
    pub(crate) asset: String,
    #[serde(default)]
    pub(crate) total_wallet_balance: Field<Amount>,
    #[serde(default)]
    pub(crate) cross_margin_asset: Field<Amount>,
    #[serde(default)]
    pub(crate) cross_margin_borrowed: Field<Amount>,
    #[serde(default)]
    pub(crate) cross_margin_interest: Field<Amount>,
    #[serde(default)]
    pub(crate) cross_margin_free: Field<Amount>,
    #[serde(default)]
    pub(crate) cross_margin_locked: Field<Amount>,
    #[serde(default)]
    pub(crate) um_wallet_balance: Field<Amount>,
    #[serde(default, rename = "umUnrealizedPNL")]
    pub(crate) um_unrealized_pnl: Field<Amount>,
    #[serde(default)]
    pub(crate) cm_wallet_balance: Field<Amount>,
    #[serde(default, rename = "cmUnrealizedPNL")]
    pub(crate) cm_unrealized_pnl: Field<Amount>,
    #[serde(default)]
    pub(crate) negative_balance: Field<Amount>,
    #[serde(default)]
    pub(crate) update_time: Field<VenueTime>,
}

/// Account-level observations are separate from native asset balances.
///
/// Equity, actual equity, maintenance margin, withdrawal capacity and open loss
/// are documented USD values. Initial margin and available balance retain their
/// source semantics until units are verified. `uniMMR` is a ratio. No USD value
/// is relabeled as USDT, rounded to Money, or allocated across collateral assets.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AccountSummary {
    #[serde(default, rename = "uniMMR")]
    pub(crate) uni_mmr: Field<Amount>,
    #[serde(default)]
    pub(crate) account_equity: Field<Amount>,
    #[serde(default)]
    pub(crate) actual_equity: Field<Amount>,
    #[serde(default)]
    pub(crate) account_initial_margin: Field<Amount>,
    #[serde(default)]
    pub(crate) account_maint_margin: Field<Amount>,
    #[serde(default)]
    pub(crate) account_status: Field<String>,
    #[serde(default)]
    pub(crate) virtual_max_withdraw_amount: Field<Amount>,
    #[serde(default)]
    pub(crate) total_available_balance: Field<Amount>,
    #[serde(default)]
    pub(crate) total_margin_open_loss: Field<Amount>,
    #[serde(default)]
    pub(crate) update_time: Field<VenueTime>,
}

/// UM V1/V2 detail is a component observation, not a second account owner.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct UmAccount {
    #[serde(deserialize_with = "deserialize_object_rows")]
    pub(crate) assets: Vec<UmAsset>,
    #[serde(deserialize_with = "deserialize_object_rows")]
    pub(crate) positions: Vec<UmPosition>,
}

/// Wallet, PnL and margin fields retain their asset identity and are never summed here.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UmAsset {
    pub(crate) asset: String,
    #[serde(default)]
    pub(crate) cross_wallet_balance: Field<Amount>,
    #[serde(default)]
    pub(crate) cross_un_pnl: Field<Amount>,
    #[serde(default)]
    pub(crate) maint_margin: Field<Amount>,
    #[serde(default)]
    pub(crate) initial_margin: Field<Amount>,
    #[serde(default)]
    pub(crate) position_initial_margin: Field<Amount>,
    #[serde(default)]
    pub(crate) open_order_initial_margin: Field<Amount>,
    #[serde(default)]
    pub(crate) update_time: Field<VenueTime>,
}

/// The union of V1 and V2 position fields, with absent fields kept unavailable.
///
/// Units require instrument metadata; no currency is guessed from the symbol.
/// Side strings are retained verbatim, including unknown modes, without claiming
/// hedge-mode support or interpreting an omitted row as a flat position.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UmPosition {
    pub(crate) symbol: String,
    pub(crate) position_side: String,
    #[serde(default)]
    pub(crate) initial_margin: Field<Amount>,
    #[serde(default)]
    pub(crate) maint_margin: Field<Amount>,
    #[serde(default)]
    pub(crate) unrealized_profit: Field<Amount>,
    #[serde(default)]
    pub(crate) position_initial_margin: Field<Amount>,
    #[serde(default)]
    pub(crate) open_order_initial_margin: Field<Amount>,
    #[serde(default)]
    pub(crate) leverage: Field<Amount>,
    #[serde(default)]
    pub(crate) entry_price: Field<Amount>,
    #[serde(default)]
    pub(crate) max_notional: Field<Amount>,
    #[serde(default)]
    pub(crate) bid_notional: Field<Amount>,
    #[serde(default)]
    pub(crate) ask_notional: Field<Amount>,
    #[serde(default)]
    pub(crate) position_amt: Field<Amount>,
    #[serde(default)]
    pub(crate) notional: Field<Amount>,
    #[serde(default)]
    pub(crate) update_time: Field<VenueTime>,
}

/// Minimal identity and exposure fields for current product-scope validation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ScopeRow {
    #[serde(default)]
    pub(crate) symbol: Field<String>,
    #[serde(default)]
    pub(crate) pair: Field<String>,
    #[serde(default)]
    pub(crate) position_amt: Field<Amount>,
}

/// Requires an object without Serde's positional-array representation for structs.
#[derive(Debug)]
pub(crate) struct JsonObject<T>(pub(crate) T);

impl<'de, T: DeserializeOwned> Deserialize<'de> for JsonObject<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(deserializer)?;

        if !raw.get().starts_with('{') {
            return Err(serde::de::Error::custom("Expected PAPI JSON object"));
        }

        serde_json::from_str(raw.get())
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}

fn deserialize_object_rows<'de, T: DeserializeOwned, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<T>, D::Error> {
    Vec::<JsonObject<T>>::deserialize(deserializer)
        .map(|rows| rows.into_iter().map(|row| row.0).collect())
}
