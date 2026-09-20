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

//! Python lifecycle boundary for the no-trading PAPI private observation session.

use std::sync::Arc;

use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_model::python::instruments::pyobject_to_instrument_any;
use pyo3::prelude::*;

use crate::{
    read_only::BinancePapiReadOnlyConfig,
    websocket::BinancePapiAccountSession as NativeBinancePapiAccountSession,
};

#[derive(Clone)]
#[pyclass(
    name = "BinancePapiAccountSession",
    module = "nautilus_trader.adapters.binance_papi",
    skip_from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.binance_papi")]
pub(crate) struct BinancePapiAccountSession {
    inner: Arc<tokio::sync::Mutex<NativeBinancePapiAccountSession>>,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl BinancePapiAccountSession {
    /// A no-trading Portfolio Margin private account observation session.
    #[new]
    fn py_new(
        py: Python<'_>,
        config: &BinancePapiReadOnlyConfig,
        instruments: Vec<Py<PyAny>>,
    ) -> PyResult<Self> {
        let instruments = instruments
            .into_iter()
            .map(|instrument| pyobject_to_instrument_any(py, instrument))
            .collect::<PyResult<Vec<_>>>()?;
        let session =
            NativeBinancePapiAccountSession::new(config, instruments).map_err(to_pyvalue_err)?;
        Ok(Self {
            inner: Arc::new(tokio::sync::Mutex::new(session)),
        })
    }

    /// Starts a new listen-key owner, begins receiving, and performs bounded baseline recovery.
    ///
    /// # Errors
    ///
    /// Returns an error if transport setup, required REST sources, projection, or application fails.
    #[pyo3(name = "start")]
    #[gen_stub(override_return_type(type_repr = "typing.Awaitable[None]", imports = ("typing",)))]
    fn py_start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.lock().await.start().await.map_err(to_pyruntime_err)
        })
    }

    /// Completes bounded task, socket, and listen-key shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error when owned work cannot be drained inside the configured bounds.
    #[pyo3(name = "stop")]
    #[gen_stub(override_return_type(type_repr = "typing.Awaitable[None]", imports = ("typing",)))]
    fn py_stop<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            inner.lock().await.stop().await.map_err(to_pyruntime_err)
        })
    }

    /// Serializes current scoped recovery evidence without credentials or payload bodies.
    #[pyo3(name = "evidence_json")]
    fn py_evidence_json(&self) -> PyResult<String> {
        let session = self
            .inner
            .try_lock()
            .map_err(|_| to_pyruntime_err("PAPI session operation is in progress"))?;
        serde_json::to_string(&session.evidence()).map_err(to_pyruntime_err)
    }

    #[getter]
    fn is_connected(&self) -> PyResult<bool> {
        let session = self
            .inner
            .try_lock()
            .map_err(|_| to_pyruntime_err("PAPI session operation is in progress"))?;
        Ok(session.is_connected())
    }

    #[getter]
    fn is_synchronized(&self) -> PyResult<bool> {
        let session = self
            .inner
            .try_lock()
            .map_err(|_| to_pyruntime_err("PAPI session operation is in progress"))?;
        Ok(session.is_synchronized())
    }

    fn __repr__(&self) -> String {
        stringify!(BinancePapiAccountSession).to_string()
    }
}
