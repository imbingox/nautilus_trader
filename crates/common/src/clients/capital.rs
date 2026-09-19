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

//! Route-bound delegation for account-specific native capital checks.

use std::rc::Rc;

use nautilus_model::{
    identifiers::{AccountId, ClientId},
    instruments::InstrumentAny,
    orders::OrderAny,
};

/// Inputs supplied when native account balances cannot express a capital check.
#[derive(Debug)]
pub struct NativeCapitalCheckRequest<'a> {
    /// Account whose capital would be consumed.
    pub account_id: AccountId,
    /// Execution client selected by both the command and cached order route.
    pub client_id: ClientId,
    /// Instrument already validated by the normal risk path.
    pub instrument: &'a InstrumentAny,
    /// Orders in the account-scoped risk group.
    pub orders: &'a [&'a OrderAny],
    /// Whether this command is a verified full-position exit.
    pub full_position_exit: bool,
}

/// Result of an account-specific capital check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeCapitalCheckDecision {
    /// The provider has sufficient current evidence for the native capital branch.
    Approved,
    /// The provider denied or could not safely evaluate the capital requirement.
    Denied(String),
}

/// Synchronous account-specific capital check installed by an execution integration.
pub type NativeCapitalCheck =
    Rc<dyn for<'a> Fn(&NativeCapitalCheckRequest<'a>) -> NativeCapitalCheckDecision + 'static>;
