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

//! Read-only views bound to an existing execution client's recovery authority.

use std::{fmt::Debug, sync::Arc};

/// Clones share the same native client source, never a separate recovery session.
#[derive(Clone)]
pub(crate) struct PapiRecoveryReader {
    read: Arc<dyn Fn() -> serde_json::Value + Send + Sync>,
}

impl Debug for PapiRecoveryReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(PapiRecoveryReader))
            .finish_non_exhaustive()
    }
}

impl PapiRecoveryReader {
    pub(crate) fn new(read: impl Fn() -> serde_json::Value + Send + Sync + 'static) -> Self {
        Self {
            read: Arc::new(read),
        }
    }

    pub(crate) fn snapshot(&self) -> serde_json::Value {
        (self.read)()
    }
}
