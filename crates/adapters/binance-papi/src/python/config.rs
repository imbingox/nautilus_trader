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

//! Python configuration bindings for the PAPI skeleton.

use nautilus_model::identifiers::AccountId;
use pyo3::prelude::*;

use crate::config::BinancePapiExecutionClientConfig;

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl BinancePapiExecutionClientConfig {
    /// Configuration for node construction only; PAPI execution is not implemented.
    #[new]
    #[pyo3(signature = (account_id = None))]
    fn py_new(account_id: Option<AccountId>) -> Self {
        Self {
            account_id: account_id.unwrap_or_else(|| Self::default().account_id),
        }
    }

    #[getter]
    fn account_id(&self) -> AccountId {
        self.account_id
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}
