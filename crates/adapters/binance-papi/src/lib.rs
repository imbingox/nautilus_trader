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

//! Binance Portfolio Margin account evidence, private observation, and bounded recovery.
//!
//! The [`read_only`] client uses the pinned Binance SDK for signed account observations,
//! order/fill/position reports, and explicitly incomplete historical snapshots. It accepts
//! explicit credentials and preloaded one-way UM instrument scope. Supported observations can
//! produce a totals-only account projection and independent PM risk view.
//!
//! The [`websocket`] session receives before collecting its REST baseline, owns listen-key renewal,
//! and publishes typed recovery evidence. Factory-created execution clients can connect in this
//! observation mode and publish account and reconciliation state after synchronization. Ordinary
//! UM submission, targeted cancellation, bounded batch cancellation, and cancel-all orchestration
//! are connected through a durable account-bound coordinator and typed write transport.
//! Modification and linked-order submission remain unsupported. Trading is explicit and
//! default-off; increase-risk admission remains fail-closed until current authenticated PM risk
//! evidence is available. An optional trading configuration defines finite admission limits.
//! Public market data and instruments use the existing `nautilus-binance` adapter.
//!
//! # Feature Flags
//!
//! - `extension-module`: Builds Python bindings into an extension module.
//! - `high-precision` (default): Uses 128-bit fixed-point domain values.
//! - `python`: Enables Python query, observation-session, configuration, and factory bindings.

#![deny(unsafe_code)]
#![deny(missing_debug_implementations)]
#![deny(clippy::missing_errors_doc)]
#![deny(clippy::missing_panics_doc)]

pub mod config;
pub mod consts;
pub mod factories;
pub mod read_only;
pub mod websocket;

#[cfg(feature = "python")]
pub mod python;

mod execution;
mod http;
mod observations;
mod reports;
#[allow(
    dead_code,
    reason = "durable trading paths remain disconnected until verified PAPI risk evidence is available"
)]
mod trading;

#[cfg(test)]
mod testing;
