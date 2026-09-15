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

use std::{cell::RefCell, rc::Rc};

use nautilus_common::{cache::Cache, clock::TestClock};
use nautilus_model::{
    identifiers::{AccountId, ClientId, InstrumentId, TraderId},
    python::instruments::instrument_any_to_pyobject,
};
use nautilus_system::get_global_pyo3_registry;
use pyo3::{
    prelude::*,
    types::{PyDict, PyList, PyModule},
};
use rstest::rstest;
use serde_json::Value;

use crate::{config::BinancePapiExecutionClientConfig, testing};

#[rstest]
fn test_python_read_only_constructors_registry_and_secret_boundaries() {
    Python::initialize();

    Python::attach(|py| {
        let module = PyModule::new(py, "binance_papi").unwrap();
        super::binance_papi(py, &module).unwrap();
        let account_id = AccountId::from("BINANCE-PAPI-009");
        let read_only = module
            .getattr("BinancePapiReadOnlyConfig")
            .unwrap()
            .call1((account_id, testing::API_KEY, testing::API_SECRET))
            .unwrap();
        let instruments = PyList::empty(py);
        instruments
            .append(instrument_any_to_pyobject(py, testing::instrument("BTCUSDT")).unwrap())
            .unwrap();
        let client_type = module.getattr("BinancePapiReadOnlyClient").unwrap();
        let client = client_type.call1((&read_only, &instruments)).unwrap();
        let raw: String = client
            .call_method1("account_observations_json", (1_000,))
            .unwrap()
            .extract()
            .unwrap();
        let observations: Value = serde_json::from_str(&raw).unwrap();

        assert!(
            observations
                .as_array()
                .unwrap()
                .iter()
                .all(|row| row["receipt_status"] == "missing")
        );
        let e = client
            .call_method1("account_observations_json", (0,))
            .unwrap_err();
        assert!(e.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        client.call_method0("cancel").unwrap();
        let canceled: String = client
            .call_method1("account_observations_json", (1_000,))
            .unwrap()
            .extract()
            .unwrap();
        let canceled: Value = serde_json::from_str(&canceled).unwrap();
        assert!(
            canceled
                .as_array()
                .unwrap()
                .iter()
                .all(|row| { row["receipt_status"] == "canceled" && row["observation"].is_null() })
        );
        assert!(client_type.call1((&read_only, PyList::empty(py))).is_err());
        assert!(read_only.getattr("api_key").is_err());
        assert!(read_only.getattr("api_secret").is_err());
        assert!(read_only.getattr("base_url").is_err());
        let rendered = read_only.repr().unwrap().to_string();
        assert!(!rendered.contains(testing::API_KEY));
        assert!(!rendered.contains(testing::API_SECRET));

        let kwargs = PyDict::new(py);
        kwargs.set_item("read_only", read_only).unwrap();
        kwargs
            .set_item(
                "instrument_ids",
                vec![InstrumentId::from("BTCUSDT-PERP.BINANCE")],
            )
            .unwrap();
        let config = module
            .getattr("BinancePapiExecutionClientConfig")
            .unwrap()
            .call((), Some(&kwargs))
            .unwrap();
        let factory = module
            .getattr("BinancePapiExecutionClientFactory")
            .unwrap()
            .call0()
            .unwrap();
        let registry = get_global_pyo3_registry();
        let extracted_config = registry.extract_config(py, config.unbind()).unwrap();
        let extracted_factory = registry.extract_exec_factory(py, factory.unbind()).unwrap();
        let config = extracted_config
            .as_any()
            .downcast_ref::<BinancePapiExecutionClientConfig>()
            .unwrap();
        let client = extracted_factory
            .create(
                TraderId::from("TRADER-001"),
                "PAPI-PYTHON-009",
                config,
                Rc::new(RefCell::new(Cache::default())).into(),
                Rc::new(RefCell::new(TestClock::new())),
            )
            .unwrap();

        assert_eq!(client.account_id(), account_id);
        assert_eq!(client.client_id(), ClientId::from("PAPI-PYTHON-009"));
        assert!(client.get_account().is_none());
        assert!(!client.is_connected());
    });
}
