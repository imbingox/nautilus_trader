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

//! Binance Portfolio Margin account evidence and bounded Rust/Python read-only queries.
//!
//! The [`read_only`] client uses the pinned Binance SDK for signed account observations,
//! order/fill/position reports, and explicitly incomplete historical snapshots. It accepts
//! explicit credentials and preloaded one-way UM instrument scope. Supported observations can
//! produce a diagnostic totals-only account projection and independent PM risk view. Trading is
//! unavailable.
//!
//! Factory-created Rust execution clients support scoped reports after explicit read-only
//! configuration and instrument preloading. LiveNode startup remains unavailable because the
//! diagnostic snapshot does not publish a live account or provide PM order admission. Public
//! market data and instruments use the existing `nautilus-binance` adapter.
//!
//! # Feature Flags
//!
//! - `extension-module`: Builds Python bindings into an extension module.
//! - `high-precision` (default): Uses 128-bit fixed-point domain values.
//! - `python`: Enables Python read-only query, configuration, and factory bindings.

#![deny(unsafe_code)]
#![deny(missing_debug_implementations)]
#![deny(clippy::missing_errors_doc)]
#![deny(clippy::missing_panics_doc)]

pub mod config;
pub mod consts;
pub mod factories;
pub mod read_only;

#[cfg(feature = "python")]
pub mod python;

mod execution;
mod http;
mod observations;
mod reports;

#[cfg(test)]
mod testing;
