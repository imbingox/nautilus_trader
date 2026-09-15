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

//! Python boundaries for exact, read-only PAPI evidence and domain reports.

use std::time::Duration;

use nautilus_core::{
    python::{to_pyruntime_err, to_pyvalue_err},
    string::secret::SecretString,
};
use nautilus_model::{
    identifiers::{AccountId, ClientOrderId, InstrumentId, VenueOrderId},
    python::instruments::pyobject_to_instrument_any,
    reports::ExecutionMassStatus,
};
use pyo3::prelude::*;

use crate::read_only::{
    BinancePapiReadOnlyClient, BinancePapiReadOnlyConfig, BinancePapiReadOnlySnapshot,
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl BinancePapiReadOnlyConfig {
    /// Explicit credentials and resource bounds for PAPI read-only queries.
    ///
    /// No environment variables are read. The supplied instruments define the complete query scope.
    /// Credentials and the base URL are redacted from Rust and Python representations.
    #[new]
    #[pyo3(signature = (
        account_id, api_key, api_secret, base_url=None, request_timeout_ms=None,
        operation_timeout_ms=None, max_requests=None, max_rows=None,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        account_id: AccountId,
        api_key: String,
        api_secret: String,
        base_url: Option<String>,
        request_timeout_ms: Option<u64>,
        operation_timeout_ms: Option<u64>,
        max_requests: Option<u32>,
        max_rows: Option<usize>,
    ) -> PyResult<Self> {
        let mut config = Self::new(account_id, api_key.into(), api_secret.into());

        if let Some(base_url) = base_url {
            config.base_url = SecretString::from(base_url);
        }

        if let Some(timeout) = request_timeout_ms {
            config.request_timeout = Duration::from_millis(timeout);
        }

        if let Some(timeout) = operation_timeout_ms {
            config.operation_timeout = Duration::from_millis(timeout);
        }

        if let Some(max_requests) = max_requests {
            config.max_requests = max_requests;
        }

        if let Some(max_rows) = max_rows {
            config.max_rows = max_rows;
        }

        config.validate().map_err(to_pyvalue_err)?;
        Ok(config)
    }

    #[getter]
    fn account_id(&self) -> AccountId {
        self.account_id
    }

    #[getter]
    fn request_timeout_ms(&self) -> u128 {
        self.request_timeout.as_millis()
    }

    #[getter]
    fn operation_timeout_ms(&self) -> u128 {
        self.operation_timeout.as_millis()
    }

    #[getter]
    fn max_requests(&self) -> u32 {
        self.max_requests
    }

    #[getter]
    fn max_rows(&self) -> usize {
        self.max_rows
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl BinancePapiReadOnlyClient {
    /// A cloneable PAPI GET client; clones share cancellation and retained observations.
    ///
    /// All instances share a process-wide IP gate: 3000 weight/minute, burst 40, four concurrent
    /// attempts. This reserves headroom below the venue's documented 6000 weight/minute allowance;
    /// other processes require separate coordination. A throttle or ban latches the gate closed.
    /// SDK 69.2.1 discards error headers, so automatic throttle recovery is unavailable.
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
        Self::new(config, instruments).map_err(to_pyvalue_err)
    }

    /// Cancels outstanding and future operations on this client and all its clones.
    #[pyo3(name = "cancel")]
    fn py_cancel(&self) {
        self.cancel();
    }

    /// Returns whether the shared IP gate has been latched closed by a throttle or ban.
    #[getter]
    #[pyo3(name = "is_throttled")]
    fn py_is_throttled(&self) -> bool {
        self.is_throttled()
    }

    /// Refreshes account, product-scope, and UM V1/V2 observations for later projection.
    ///
    /// Successful sources are retained independently; a failure preserves that source's prior
    /// response and marks it failed. All nine sources share one generation and operation budget.
    ///
    /// # Errors
    ///
    /// Returns an error for cancellation, quota/deadline exhaustion, any failed request, or invalid JSON.
    #[pyo3(name = "refresh_account_observations")]
    #[gen_stub(override_return_type(type_repr = "typing.Awaitable[None]", imports = ("typing",)))]
    fn py_refresh_account_observations<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .refresh_account_observations()
                .await
                .map_err(to_pyruntime_err)
        })
    }

    /// Serializes retained observations and their current receipt status as exact JSON evidence.
    ///
    /// The output contains private account data. Recent receipt does not establish economic
    /// validity or authorize order admission. Original JSON and numeric strings are preserved.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero receipt-age bound or if serialization fails.
    #[pyo3(name = "account_observations_json")]
    fn py_account_observations_json(&self, max_receipt_age_ms: u64) -> PyResult<String> {
        if max_receipt_age_ms == 0 {
            return Err(to_pyvalue_err("PAPI maximum receipt age must be positive"));
        }

        self.account_observations_json(Duration::from_millis(max_receipt_age_ms))
            .map_err(to_pyruntime_err)
    }

    /// Projects retained observations into independent wallet and PM risk results.
    ///
    /// This is a read-only diagnostic snapshot. It neither updates the cache nor authorizes
    /// trading. A failed projection contains reasons and never publishes a partial account state.
    ///
    /// # Errors
    ///
    /// Returns an error for zero freshness bounds or if serialization fails.
    #[pyo3(name = "account_snapshot_json")]
    fn py_account_snapshot_json(
        &self,
        max_receipt_age_ms: u64,
        max_collection_span_ms: u64,
    ) -> PyResult<String> {
        if max_receipt_age_ms == 0 {
            return Err(to_pyvalue_err("PAPI maximum receipt age must be positive"));
        }

        if max_collection_span_ms == 0 {
            return Err(to_pyvalue_err(
                "PAPI maximum collection span must be positive",
            ));
        }

        self.account_snapshot_json(
            Duration::from_millis(max_receipt_age_ms),
            Duration::from_millis(max_collection_span_ms),
        )
        .map_err(to_pyruntime_err)
    }

    /// Queries the account's order quota as unprojected JSON evidence.
    ///
    /// This GET consumes one IP-weight unit; it does not reserve or consume an order slot.
    ///
    /// # Errors
    ///
    /// Returns an error if the signed read fails or exhausts its budget.
    #[pyo3(name = "query_order_rate_limit")]
    #[gen_stub(override_return_type(type_repr = "typing.Awaitable[str]", imports = ("typing",)))]
    fn py_query_order_rate_limit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .query_order_rate_limit()
                .await
                .map_err(to_pyruntime_err)
        })
    }

    /// Collects a fixed inclusive history window plus current orders and explicit positions.
    ///
    /// Fills are linked to ordinary orders or their algo parent using targeted child reads.
    /// Commission, active-source, position, schema and identity failures fail the request.
    /// Other failed historical legs are listed in the returned incomplete snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds, unsupported mode, unrepresentable reports, contradictory
    /// identities, unresolved fill linkage, or failure of a required read.
    #[pyo3(name = "generate_mass_status")]
    #[gen_stub(override_return_type(
        type_repr = "typing.Awaitable[BinancePapiReadOnlySnapshot]", imports = ("typing",),
    ))]
    fn py_generate_mass_status<'py>(
        &self,
        py: Python<'py>,
        start: u64,
        end: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .generate_mass_status(start.into(), end.into())
                .await
                .map_err(to_pyruntime_err)
        })
    }

    /// Returns all current open or in-flight orders within the requested metadata scope.
    ///
    /// Active orders are included regardless of their age. A partial result is never returned.
    ///
    /// # Errors
    ///
    /// Returns an error for failed current reads, unsupported mode, unresolved algo children or invalid reports.
    #[pyo3(name = "generate_open_order_status_reports", signature = (instrument_id=None))]
    #[gen_stub(override_return_type(
        type_repr = "typing.Awaitable[list[nautilus_trader.model.OrderStatusReport]]",
        imports = ("typing", "nautilus_trader.model"),
    ))]
    fn py_generate_open_order_status_reports<'py>(
        &self,
        py: Python<'py>,
        instrument_id: Option<InstrumentId>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .generate_open_order_status_reports(instrument_id)
                .await
                .map_err(to_pyruntime_err)
        })
    }

    /// Returns explicit one-way position rows for every requested instrument.
    ///
    /// An omitted row is an error. Sparse account V2 data is never used to infer a flat position.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported mode, missing coverage, failed reads or inexact quantities.
    #[pyo3(name = "generate_position_status_reports", signature = (instrument_id=None))]
    #[gen_stub(override_return_type(
        type_repr = "typing.Awaitable[list[nautilus_trader.model.PositionStatusReport]]",
        imports = ("typing", "nautilus_trader.model"),
    ))]
    fn py_generate_position_status_reports<'py>(
        &self,
        py: Python<'py>,
        instrument_id: Option<InstrumentId>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .generate_position_status_reports(instrument_id)
                .await
                .map_err(to_pyruntime_err)
        })
    }

    /// Resolves one order by its encoded venue identity or ordinary client order ID.
    ///
    /// Venue IDs use `PAPI:O:SYMBOL:ID` for ordinary orders and `PAPI:A:SYMBOL:ID` for algos.
    /// A venue not-found response remains an error while endpoint retention/absence is unverified.
    /// This method therefore cannot return absence evidence to the execution engine.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid scope/identity, an unresolved order, or any failed read or conversion.
    #[pyo3(name = "generate_order_status_report")]
    #[pyo3(signature = (instrument_id, venue_order_id=None, client_order_id=None))]
    #[gen_stub(override_return_type(
        type_repr = "typing.Awaitable[nautilus_trader.model.OrderStatusReport]",
        imports = ("typing", "nautilus_trader.model"),
    ))]
    fn py_generate_order_status_report<'py>(
        &self,
        py: Python<'py>,
        instrument_id: InstrumentId,
        venue_order_id: Option<VenueOrderId>,
        client_order_id: Option<ClientOrderId>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            client
                .generate_order_status_report(instrument_id, venue_order_id, client_order_id)
                .await
                .map_err(to_pyruntime_err)
        })
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl BinancePapiReadOnlySnapshot {
    #[pyo3(name = "mass_status")]
    fn py_mass_status(&self) -> ExecutionMassStatus {
        self.mass_status.clone()
    }

    #[getter]
    fn window_end(&self) -> u64 {
        self.window_end.as_u64()
    }

    #[getter]
    fn reports_complete(&self) -> bool {
        self.mass_status.reports_complete()
    }

    #[pyo3(name = "instrument_ids")]
    fn py_instrument_ids(&self) -> Vec<InstrumentId> {
        self.instrument_ids.clone()
    }

    #[pyo3(name = "issues")]
    fn py_issues(&self) -> Vec<String> {
        self.issues.clone()
    }

    /// Serializes reports, exact history bounds, coverage issues, and response metadata.
    ///
    /// The output contains private account and execution data. It is evidence for review,
    /// not a statement of historical completeness or economic account validity.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be serialized.
    #[pyo3(name = "to_json")]
    fn py_to_json(&self) -> PyResult<String> {
        self.to_json().map_err(to_pyruntime_err)
    }

    fn __repr__(&self) -> String {
        format!(
            "BinancePapiReadOnlySnapshot(window_end={}, reports_complete={}, instruments={}, issues={})",
            self.window_end,
            self.mass_status.reports_complete(),
            self.instrument_ids.len(),
            self.issues.len(),
        )
    }
}
