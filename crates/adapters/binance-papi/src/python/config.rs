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

//! Python configuration bindings for scoped read-only PAPI execution reports.

use nautilus_core::python::to_pyvalue_err;
use nautilus_model::identifiers::{AccountId, InstrumentId};
use pyo3::prelude::*;

use crate::{config::BinancePapiExecutionClientConfig, read_only::BinancePapiReadOnlyConfig};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl BinancePapiExecutionClientConfig {
    /// Configuration for scoped Binance Portfolio Margin execution reports.
    ///
    /// The default supports node construction without credentials. Supplying `read_only`
    /// and explicit instrument IDs enables the Rust execution client's report methods.
    /// Instruments must already exist in the node cache. LiveNode startup remains unavailable
    /// until the native account balance mapping is accepted; trading is unsupported.
    #[new]
    #[pyo3(signature = (account_id=None, read_only=None, instrument_ids=None))]
    fn py_new(
        account_id: Option<AccountId>,
        read_only: Option<BinancePapiReadOnlyConfig>,
        instrument_ids: Option<Vec<InstrumentId>>,
    ) -> PyResult<Self> {
        let defaults = Self::default();
        let config = Self {
            account_id: account_id
                .or_else(|| read_only.as_ref().map(|config| config.account_id))
                .unwrap_or(defaults.account_id),
            read_only,
            instrument_ids: instrument_ids.unwrap_or(defaults.instrument_ids),
        };
        config.validate().map_err(to_pyvalue_err)?;
        Ok(config)
    }

    #[getter]
    fn account_id(&self) -> AccountId {
        self.account_id
    }

    #[getter]
    fn read_only(&self) -> Option<BinancePapiReadOnlyConfig> {
        self.read_only.clone()
    }

    #[getter]
    fn instrument_ids(&self) -> Vec<InstrumentId> {
        self.instrument_ids.clone()
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}
