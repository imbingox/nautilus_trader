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

use nautilus_model::events::AccountState;
use serde_json::{Value, json};
use sqlx::{FromRow, Row, postgres::PgRow};

#[derive(Debug)]
pub struct AccountEventRow(pub AccountState);

impl<'r> FromRow<'r, PgRow> for AccountEventRow {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let nanos = |column: &str| -> Result<u64, sqlx::Error> {
            row.try_get::<&str, _>(column)?
                .parse()
                .map_err(|e| sqlx::Error::Decode(Box::new(e)))
        };
        let event = json!({
            "event_id": row.try_get::<&str, _>("id")?,
            "account_id": row.try_get::<&str, _>("account_id")?,
            "account_type": row.try_get::<&str, _>("kind")?,
            "base_currency": row.try_get::<Option<&str>, _>("base_currency")?,
            "balances": row.try_get::<Value, _>("balances")?,
            "margins": row.try_get::<Value, _>("margins")?,
            "is_reported": row.try_get::<bool, _>("is_reported")?,
            "ts_event": nanos("ts_event")?,
            "ts_init": nanos("ts_init")?,
            "info": row.try_get::<Option<Value>, _>("info")?,
            "total_only_balances": row.try_get::<Value, _>("total_only_balances")?,
        });
        serde_json::from_value(event)
            .map(Self)
            .map_err(|e| sqlx::Error::Decode(Box::new(e)))
    }
}
