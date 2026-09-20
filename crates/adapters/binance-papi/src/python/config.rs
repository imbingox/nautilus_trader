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

use std::path::PathBuf;

use nautilus_core::python::to_pyvalue_err;
use nautilus_model::{
    identifiers::{AccountId, InstrumentId},
    types::Currency,
};
use pyo3::prelude::*;
use rust_decimal::Decimal;

use crate::{
    config::{
        BinancePapiExecutionClientConfig, BinancePapiInstrumentTradingConfig,
        BinancePapiTradingConfig,
    },
    read_only::BinancePapiReadOnlyConfig,
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl BinancePapiInstrumentTradingConfig {
    /// Hard trading limits for one explicitly allowed linear UM instrument.
    #[new]
    #[pyo3(signature = (
        instrument_id,
        max_order_quantity,
        max_order_notional,
        max_position_quantity,
        max_instrument_exposure,
    ))]
    fn py_new(
        instrument_id: InstrumentId,
        max_order_quantity: Decimal,
        max_order_notional: Decimal,
        max_position_quantity: Decimal,
        max_instrument_exposure: Decimal,
    ) -> PyResult<Self> {
        let config = Self {
            instrument_id,
            max_order_quantity,
            max_order_notional,
            max_position_quantity,
            max_instrument_exposure,
        };
        config.validate().map_err(to_pyvalue_err)?;
        Ok(config)
    }

    #[getter]
    fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    #[getter]
    fn max_order_quantity(&self) -> Decimal {
        self.max_order_quantity
    }

    #[getter]
    fn max_order_notional(&self) -> Decimal {
        self.max_order_notional
    }

    #[getter]
    fn max_position_quantity(&self) -> Decimal {
        self.max_position_quantity
    }

    #[getter]
    fn max_instrument_exposure(&self) -> Decimal {
        self.max_instrument_exposure
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl BinancePapiTradingConfig {
    /// Explicit opt-in and finite resource limits for PAPI UM trading.
    ///
    /// Presence of this configuration permits later trading admission checks to run. It never proves
    /// that current account evidence is valid or authorizes a command by itself.
    #[new]
    #[pyo3(signature = (
        command_journal_path,
        risk_currency,
        instrument_limits,
        max_account_exposure,
        max_in_flight_operations,
        max_risk_age_ms,
        max_risk_collection_span_ms,
        max_recovery_requests,
        max_recovery_rounds,
        recovery_recheck_interval_ms,
        market_order_price_buffer_bps,
        fee_buffer_bps,
    ))]
    #[expect(clippy::too_many_arguments)]
    fn py_new(
        command_journal_path: PathBuf,
        risk_currency: Currency,
        instrument_limits: Vec<BinancePapiInstrumentTradingConfig>,
        max_account_exposure: Decimal,
        max_in_flight_operations: usize,
        max_risk_age_ms: u64,
        max_risk_collection_span_ms: u64,
        max_recovery_requests: u32,
        max_recovery_rounds: u32,
        recovery_recheck_interval_ms: u64,
        market_order_price_buffer_bps: u32,
        fee_buffer_bps: u32,
    ) -> PyResult<Self> {
        let config = Self {
            command_journal_path,
            risk_currency,
            instrument_limits,
            max_account_exposure,
            max_in_flight_operations,
            max_risk_age_ms,
            max_risk_collection_span_ms,
            max_recovery_requests,
            max_recovery_rounds,
            recovery_recheck_interval_ms,
            market_order_price_buffer_bps,
            fee_buffer_bps,
        };
        config.validate().map_err(to_pyvalue_err)?;
        Ok(config)
    }

    #[getter]
    fn command_journal_path(&self) -> PathBuf {
        self.command_journal_path.clone()
    }

    #[getter]
    fn risk_currency(&self) -> Currency {
        self.risk_currency
    }

    #[getter]
    fn instrument_limits(&self) -> Vec<BinancePapiInstrumentTradingConfig> {
        self.instrument_limits.clone()
    }

    #[getter]
    fn max_account_exposure(&self) -> Decimal {
        self.max_account_exposure
    }

    #[getter]
    const fn max_in_flight_operations(&self) -> usize {
        self.max_in_flight_operations
    }

    #[getter]
    const fn max_risk_age_ms(&self) -> u64 {
        self.max_risk_age_ms
    }

    #[getter]
    const fn max_risk_collection_span_ms(&self) -> u64 {
        self.max_risk_collection_span_ms
    }

    #[getter]
    const fn max_recovery_requests(&self) -> u32 {
        self.max_recovery_requests
    }

    #[getter]
    const fn max_recovery_rounds(&self) -> u32 {
        self.max_recovery_rounds
    }

    #[getter]
    const fn recovery_recheck_interval_ms(&self) -> u64 {
        self.recovery_recheck_interval_ms
    }

    #[getter]
    const fn market_order_price_buffer_bps(&self) -> u32 {
        self.market_order_price_buffer_bps
    }

    #[getter]
    const fn fee_buffer_bps(&self) -> u32 {
        self.fee_buffer_bps
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl BinancePapiExecutionClientConfig {
    /// Configuration for scoped Binance Portfolio Margin execution reports.
    ///
    /// The default supports node construction without credentials. Supplying `read_only`
    /// and explicit instrument IDs enables the Rust execution client's report methods.
    /// Instruments must already exist in the node cache. Supplying `trading` explicitly enables the
    /// durable command lifecycle, while increase-risk admission remains closed until the coordinator
    /// has current, authenticated Portfolio Margin risk evidence.
    #[new]
    #[pyo3(signature = (account_id=None, read_only=None, instrument_ids=None, trading=None))]
    fn py_new(
        account_id: Option<AccountId>,
        read_only: Option<BinancePapiReadOnlyConfig>,
        instrument_ids: Option<Vec<InstrumentId>>,
        trading: Option<BinancePapiTradingConfig>,
    ) -> PyResult<Self> {
        let defaults = Self::default();
        let config = Self {
            account_id: account_id
                .or_else(|| read_only.as_ref().map(|config| config.account_id))
                .unwrap_or(defaults.account_id),
            read_only,
            instrument_ids: instrument_ids.unwrap_or(defaults.instrument_ids),
            trading,
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

    #[getter]
    fn trading(&self) -> Option<BinancePapiTradingConfig> {
        self.trading.clone()
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}
