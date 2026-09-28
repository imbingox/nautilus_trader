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

//! Python factory bindings for scoped read-only PAPI execution reports.

use pyo3::prelude::*;

use crate::{consts::BINANCE_PAPI, factories::BinancePapiExecutionClientFactory};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl BinancePapiExecutionClientFactory {
    /// Factory for scoped Binance Portfolio Margin observation and execution reports.
    #[new]
    fn py_new() -> Self {
        Self::new()
    }

    /// Returns a read-only snapshot from the named client created by this factory.
    ///
    /// A snapshot is sampled evidence, not a durable trading permit. Consumers must expire it
    /// using `sampled_at_ns` and `valid_for_ms`; native admission remains authoritative.
    #[pyo3(name = "recovery_state_json")]
    fn py_recovery_state_json(&self, client_id: &str) -> Option<String> {
        self.recovery_state_json(client_id)
    }

    #[pyo3(name = "name")]
    fn py_name(&self) -> &'static str {
        BINANCE_PAPI
    }
}
