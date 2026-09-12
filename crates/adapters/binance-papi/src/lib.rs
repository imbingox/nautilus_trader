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

//! Binance Portfolio Margin adapter construction and Python registration.
//!
//! This skeleton supports node construction only. Starting, connecting, trading and
//! reconciliation return errors until PAPI execution is implemented. Public market
//! data and instruments are provided by the existing `nautilus-binance` adapter.
//!
//! # Feature Flags
//!
//! - `extension-module`: Builds Python bindings into an extension module.
//! - `high-precision` (default): Uses 128-bit fixed-point domain values.
//! - `python`: Enables Python configuration and factory bindings.

#![deny(unsafe_code)]
#![deny(missing_debug_implementations)]
#![deny(clippy::missing_errors_doc)]
#![deny(clippy::missing_panics_doc)]

pub mod config;
pub mod consts;
pub mod factories;

#[cfg(feature = "python")]
pub mod python;

mod execution;
