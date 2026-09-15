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

use std::collections::HashMap;

use arrow::{datatypes::Schema, error::ArrowError, record_batch::RecordBatch};
use nautilus_model::events::AccountState;

use super::{
    ArrowSchemaProvider, DecodeTypedFromRecordBatch, EncodeToRecordBatch, EncodingError,
    json::{self, JsonFieldSpec},
};

const ACCOUNT_STATE_FIELDS: &[JsonFieldSpec] = &[
    JsonFieldSpec::utf8("account_id", false),
    JsonFieldSpec::utf8("account_type", false),
    JsonFieldSpec::utf8("base_currency", true),
    JsonFieldSpec::utf8_json("balances", false),
    JsonFieldSpec::utf8_json("margins", false),
    JsonFieldSpec::boolean("is_reported", false),
    JsonFieldSpec::utf8("event_id", false),
    JsonFieldSpec::u64("ts_event", false),
    JsonFieldSpec::u64("ts_init", false),
    JsonFieldSpec::utf8_json("info", true),
    JsonFieldSpec::utf8_json("total_only_balances", false),
];

impl ArrowSchemaProvider for AccountState {
    fn get_schema(metadata: Option<HashMap<String, String>>) -> Schema {
        json::schema_for_type("AccountState", metadata, ACCOUNT_STATE_FIELDS)
    }
}

impl EncodeToRecordBatch for AccountState {
    fn encode_batch(
        metadata: &HashMap<String, String>,
        data: &[Self],
    ) -> Result<RecordBatch, ArrowError> {
        json::encode_batch("AccountState", metadata, data, ACCOUNT_STATE_FIELDS)
    }

    fn metadata(&self) -> HashMap<String, String> {
        json::metadata_for_type("AccountState")
    }
}

impl DecodeTypedFromRecordBatch for AccountState {
    fn decode_typed_batch(
        metadata: &HashMap<String, String>,
        record_batch: RecordBatch,
    ) -> Result<Vec<Self>, EncodingError> {
        // These columns were introduced separately, so each has its own legacy default
        let fields = json::fields_for_schema(
            &record_batch,
            ACCOUNT_STATE_FIELDS,
            &["total_only_balances"],
        )?;
        let fields = json::fields_for_schema(&record_batch, &fields, &["info"])?;
        json::decode_batch(metadata, &record_batch, &fields, Some("AccountState"))
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::Params;
    use nautilus_model::{
        events::account::stubs::{cash_account_state, margin_account_state},
        types::Money,
    };
    use rstest::rstest;
    use serde_json::json;

    use super::*;
    use crate::arrow::{DecodeTypedFromRecordBatch, EncodeToRecordBatch, json::encode_batch};

    #[rstest]
    fn test_account_state_round_trip(cash_account_state: AccountState) {
        let mut info = Params::new();
        info.insert(
            "total_wallet_balance".to_string(),
            json!("1525000.00000001"),
        );
        info.insert("can_trade".to_string(), json!(true));
        let state = cash_account_state.with_info(Some(info));
        let metadata = state.metadata();
        let batch = AccountState::encode_batch(&metadata, std::slice::from_ref(&state)).unwrap();
        let decoded = AccountState::decode_typed_batch(batch.schema().metadata(), batch).unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].account_id, state.account_id);
        assert_eq!(decoded[0].balances, state.balances);
        assert_eq!(decoded[0].margins, state.margins);
        assert_eq!(decoded[0].base_currency, state.base_currency);
        assert_eq!(decoded[0].info, state.info);
    }

    #[rstest]
    fn test_account_state_decodes_legacy_batch_without_info(cash_account_state: AccountState) {
        let metadata = cash_account_state.metadata();
        let legacy_fields = &ACCOUNT_STATE_FIELDS[..ACCOUNT_STATE_FIELDS.len() - 2];
        let batch = encode_batch(
            "AccountState",
            &metadata,
            std::slice::from_ref(&cash_account_state),
            legacy_fields,
        )
        .unwrap();
        let decoded = AccountState::decode_typed_batch(batch.schema().metadata(), batch).unwrap();

        assert_eq!(decoded.len(), 1);
        assert!(decoded[0].info.is_none());
        assert!(decoded[0].total_only_balances.is_empty());
    }

    #[rstest]
    fn test_account_state_decodes_previous_schema_with_info(cash_account_state: AccountState) {
        let state = cash_account_state.with_info(Some(Params::new()));
        let metadata = state.metadata();
        let legacy_fields = &ACCOUNT_STATE_FIELDS[..ACCOUNT_STATE_FIELDS.len() - 1];
        let batch = encode_batch(
            "AccountState",
            &metadata,
            std::slice::from_ref(&state),
            legacy_fields,
        )
        .unwrap();
        let decoded = AccountState::decode_typed_batch(batch.schema().metadata(), batch).unwrap();

        assert_eq!(decoded[0].info, state.info);
        assert!(decoded[0].total_only_balances.is_empty());
        assert_eq!(decoded[0].balances, state.balances);
    }

    #[rstest]
    fn test_totals_only_account_state_arrow_round_trip() {
        let mut state = margin_account_state();
        state.balances.clear();
        let state = state
            .with_total_only_balances(vec![
                Money::from("-19.23 USD"),
                Money::from("0 USDT"),
                Money::from("0.12345678 BTC"),
            ])
            .unwrap();
        let batch =
            AccountState::encode_batch(&state.metadata(), std::slice::from_ref(&state)).unwrap();
        let decoded = AccountState::decode_typed_batch(batch.schema().metadata(), batch).unwrap();

        assert_eq!(
            serde_json::to_value(&decoded[0]).unwrap(),
            serde_json::to_value(&state).unwrap()
        );
    }
}
