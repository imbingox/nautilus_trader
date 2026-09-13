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

use std::collections::{BTreeMap, BTreeSet};

use nautilus_core::UnixNanos;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use super::{
    ReportCollector,
    models::{AlgoRow, OrderRow, TradeRow},
    parse::commission,
};
use crate::http::query::{HistoryEndpoint, PAGE_LIMIT, PapiRequest};

const MILLISECOND_NS: u64 = 1_000_000;

// Six days is strictly shorter than every endpoint's seven-day maximum
const WINDOW_MS: i64 = 6 * 24 * 60 * 60 * 1_000;

#[derive(Clone, Copy, Debug)]
pub(crate) struct HistoryWindow {
    pub(crate) start: UnixNanos,
    pub(crate) end: UnixNanos,
    first_ms: i64,
    last_ms: i64,
}

impl HistoryWindow {
    pub(crate) fn new(start: UnixNanos, end: UnixNanos) -> anyhow::Result<Self> {
        anyhow::ensure!(start <= end, "PAPI history start exceeds end");
        let first_ms = i64::try_from(start.as_u64() / MILLISECOND_NS)?;
        let last_ms = i64::try_from(
            end.as_u64() / MILLISECOND_NS + u64::from(!end.as_u64().is_multiple_of(MILLISECOND_NS)),
        )?;

        Ok(Self {
            start,
            end,
            first_ms,
            last_ms,
        })
    }

    pub(crate) fn contains(self, time: UnixNanos) -> bool {
        time >= self.start && time <= self.end
    }
}

impl ReportCollector<'_> {
    pub(super) async fn history<T: HistoryRow>(
        &mut self,
        endpoint: HistoryEndpoint,
        symbol: &str,
        window: HistoryWindow,
    ) -> anyhow::Result<Vec<T>> {
        let mut result = BTreeMap::<i64, T>::new();
        let mut first = window.first_ms;

        while first <= window.last_ms {
            let last = window.last_ms.min(first + WINDOW_MS - 1);
            let mut pending = vec![(first, last)];

            while let Some((lo, hi)) = pending.pop() {
                let request = PapiRequest::History {
                    endpoint,
                    symbol: symbol.to_owned(),
                    start: lo,
                    end: hi,
                };
                let page: Vec<T> = self.rows(&request).await?;
                anyhow::ensure!(page.len() <= PAGE_LIMIT, PapiCoverageError);
                let mut page_ids = BTreeSet::new();

                for row in &page {
                    // Validate fees even on saturated pages which will be split
                    row.validate()?;
                    anyhow::ensure!(row.symbol() == symbol, PapiConsistencyError);
                    anyhow::ensure!(
                        row.id() > 0 && page_ids.insert(row.id()),
                        PapiConsistencyError
                    );
                    anyhow::ensure!(
                        (lo..=hi).contains(&row.selection_time_ms()),
                        PapiCoverageError
                    );
                }

                let saturated = page.len() == PAGE_LIMIT;

                for row in page {
                    if let Some(previous) = result.get(&row.id()) {
                        anyhow::ensure!(
                            serde_json::to_string(previous)? == serde_json::to_string(&row)?,
                            PapiConsistencyError
                        );
                    } else {
                        result.insert(row.id(), row);
                    }
                }

                if saturated {
                    anyhow::ensure!(lo < hi, PapiCoverageError);
                    let mid = lo + (hi - lo) / 2;
                    pending.push((mid + 1, hi));
                    pending.push((lo, mid));
                }
            }

            first = last + 1;
        }

        Ok(result.into_values().collect())
    }
}

pub(super) trait HistoryRow: DeserializeOwned + Serialize {
    fn id(&self) -> i64;
    fn symbol(&self) -> &str;
    fn selection_time_ms(&self) -> i64;
    fn validate(&self) -> anyhow::Result<()>;
}

impl HistoryRow for OrderRow {
    fn id(&self) -> i64 {
        self.order_id
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn selection_time_ms(&self) -> i64 {
        self.time.milliseconds
    }

    fn validate(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl HistoryRow for AlgoRow {
    fn id(&self) -> i64 {
        self.algo_id
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn selection_time_ms(&self) -> i64 {
        self.create_time.milliseconds
    }

    fn validate(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl HistoryRow for TradeRow {
    fn id(&self) -> i64 {
        self.id
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn selection_time_ms(&self) -> i64 {
        self.time.milliseconds
    }

    fn validate(&self) -> anyhow::Result<()> {
        commission(self).map(|_| ())
    }
}

#[derive(Debug, Error)]
#[error(
    "PAPI history coverage is ambiguous: saturated millisecond, out-of-window or nonprogressing page"
)]
pub(crate) struct PapiCoverageError;

#[derive(Debug, Error)]
#[error("Contradictory or duplicate PAPI response identities")]
pub(crate) struct PapiConsistencyError;

#[derive(Debug, Error)]
#[error("Invalid PAPI report response schema")]
pub(crate) struct PapiSchemaError;
