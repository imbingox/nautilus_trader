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

//! PAPI bindings registered in the same extension and registry as the live node.

mod config;
mod factories;

use nautilus_common::factories::{ClientConfig, ExecutionClientFactory};
use nautilus_core::python::to_pyruntime_err;
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    config::BinancePapiExecutionClientConfig,
    consts::{BINANCE_PAPI, BINANCE_PAPI_CLIENT_ID, BINANCE_PAPI_VENUE},
    factories::BinancePapiExecutionClientFactory,
};

#[expect(clippy::needless_pass_by_value)]
fn extract_exec_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn ExecutionClientFactory>> {
    Ok(Box::new(
        factory.extract::<BinancePapiExecutionClientFactory>(py)?,
    ))
}

#[expect(clippy::needless_pass_by_value)]
fn extract_exec_config(py: Python<'_>, config: Py<PyAny>) -> PyResult<Box<dyn ClientConfig>> {
    Ok(Box::new(
        config.extract::<BinancePapiExecutionClientConfig>(py)?,
    ))
}

/// Exposes `nautilus_trader.adapters.binance_papi`.
///
/// # Errors
///
/// Returns an error if class registration or registry initialization fails.
#[pymodule]
pub fn binance_papi(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add(stringify!(BINANCE_PAPI), BINANCE_PAPI)?;
    m.add(stringify!(BINANCE_PAPI_CLIENT_ID), *BINANCE_PAPI_CLIENT_ID)?;
    m.add(stringify!(BINANCE_PAPI_VENUE), *BINANCE_PAPI_VENUE)?;
    m.add_class::<BinancePapiExecutionClientConfig>()?;
    m.add_class::<BinancePapiExecutionClientFactory>()?;

    let registry = get_global_pyo3_registry();
    registry
        .register_exec_factory_extractor(BINANCE_PAPI.to_string(), extract_exec_factory)
        .map_err(to_pyruntime_err)?;
    registry
        .register_config_extractor(
            stringify!(BinancePapiExecutionClientConfig).to_string(),
            extract_exec_config,
        )
        .map_err(to_pyruntime_err)?;
    Ok(())
}
