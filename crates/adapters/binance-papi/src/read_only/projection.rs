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

use std::time::Duration;

use nautilus_common::live::dst::time::Instant;
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    enums::AccountType,
    events::AccountState,
    identifiers::AccountId,
    types::{Currency, Money},
};
use serde::Serialize;

use super::StoredObservation;
use crate::{
    http::BinancePapiResponseMetadata,
    observations::{
        ObservationData, ObservationFailure, ObservationSource, ObservationTiming, ReceiptStatus,
        models::AccountSummary,
    },
    reports::InstrumentScope,
};

#[derive(Serialize)]
pub(super) struct AccountSnapshot<'a> {
    account_id: AccountId,
    trading_authorized: bool,
    wallet: ProjectionResult<'a, AccountState>,
    portfolio_margin_risk: ProjectionResult<'a, PortfolioMarginRiskView<'a>>,
}

#[derive(Serialize)]
struct ProjectionResult<'a, T> {
    status: ProjectionStatus,
    value: Option<T>,
    issues: Vec<String>,
    sources: Vec<ProjectionSource<'a>>,
    collection_span_ns: Option<u128>,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ProjectionStatus {
    Available,
    Unsupported,
    Inconsistent,
    Missing,
    Stale,
    Failed,
    Refreshing,
    Canceled,
}

#[derive(Serialize)]
struct ProjectionSource<'a> {
    endpoint: &'static str,
    source_scope: &'a ObservationSource,
    receipt_status: ReceiptStatus,
    failure: Option<&'a ObservationFailure>,
    generation: Option<u64>,
    timing: ObservationTiming,
    response_metadata: Option<&'a BinancePapiResponseMetadata>,
}

#[derive(Serialize)]
struct PortfolioMarginRiskView<'a> {
    endpoint: &'static str,
    generation: u64,
    account_summary: &'a AccountSummary,
    units: RiskUnits,
    status_recognized: bool,
}

#[derive(Serialize)]
struct RiskUnits {
    uni_mmr: &'static str,
    account_equity: &'static str,
    actual_equity: &'static str,
    account_initial_margin: &'static str,
    account_maint_margin: &'static str,
    virtual_max_withdraw_amount: &'static str,
    total_available_balance: &'static str,
    total_margin_open_loss: &'static str,
}

pub(super) struct AccountProjectionContext<'a> {
    pub account_id: AccountId,
    pub scope: &'a InstrumentScope,
    pub ts_init: UnixNanos,
    pub now: Instant,
    pub max_receipt_age: Duration,
    pub max_collection_span: Duration,
    pub canceled: bool,
}

pub(super) fn project_account_snapshot<'a>(
    sources: &'a [StoredObservation],
    context: &AccountProjectionContext<'_>,
) -> AccountSnapshot<'a> {
    let balance = find_source(sources, |source| {
        matches!(source, ObservationSource::Balance { asset: None })
    });
    let account = find_source(sources, |source| {
        matches!(source, ObservationSource::Account)
    });
    let um_v2 = find_source(sources, |source| {
        matches!(source, ObservationSource::UmAccountV2)
    });
    let um_orders = find_source(sources, |source| {
        matches!(source, ObservationSource::UmOpenOrders)
    });
    let um_algos = find_source(sources, |source| {
        matches!(source, ObservationSource::UmOpenAlgos)
    });
    let cm_positions = find_source(sources, |source| {
        matches!(source, ObservationSource::CmPositions)
    });
    let cm_orders = find_source(sources, |source| {
        matches!(source, ObservationSource::CmOpenOrders)
    });
    let margin_orders = find_source(sources, |source| {
        matches!(source, ObservationSource::MarginOpenOrders)
    });
    let wallet = project_wallet(
        [
            balance,
            um_v2,
            um_orders,
            um_algos,
            cm_positions,
            cm_orders,
            margin_orders,
        ],
        context,
    );
    let portfolio_margin_risk = project_risk(
        account,
        context.now,
        context.max_receipt_age,
        context.max_collection_span,
        context.canceled,
    );

    AccountSnapshot {
        account_id: context.account_id,
        trading_authorized: false,
        wallet,
        portfolio_margin_risk,
    }
}

fn project_wallet<'a>(
    sources: [Option<&'a StoredObservation>; 7],
    context: &AccountProjectionContext<'_>,
) -> ProjectionResult<'a, AccountState> {
    let validation = validate_sources(
        &sources,
        context.now,
        context.max_receipt_age,
        context.max_collection_span,
        context.canceled,
    );
    let mut issues = validation.issues;
    let mut status = validation.status;
    let mut totals = Vec::new();

    if issues.is_empty() {
        let Some(balance) = sources[0] else {
            issues.push("Validated PAPI balance source is unavailable".to_string());
            status = ProjectionStatus::Missing;
            return ProjectionResult {
                status,
                value: None,
                issues,
                sources: validation.sources,
                collection_span_ns: validation.collection_span_ns,
            };
        };
        let Some(observation) = balance.slot.last_response() else {
            issues.push("Validated PAPI balance observation is unavailable".to_string());
            status = ProjectionStatus::Missing;
            return ProjectionResult {
                status,
                value: None,
                issues,
                sources: validation.sources,
                collection_span_ns: validation.collection_span_ns,
            };
        };
        let ObservationData::Balances(rows) = &observation.data else {
            issues.push("PAPI balance source contained unexpected data".to_string());
            status = ProjectionStatus::Unsupported;
            return ProjectionResult {
                status,
                value: None,
                issues,
                sources: validation.sources,
                collection_span_ns: validation.collection_span_ns,
            };
        };

        for row in rows {
            match validate_wallet_row(row) {
                Ok(total) => totals.push(total),
                Err(e) => issues.push(e.to_string()),
            }
        }

        if !issues.is_empty() {
            status = ProjectionStatus::Unsupported;
        }
    }

    if issues.is_empty() {
        validate_product_scope(&sources[2..], context.scope, &mut issues);
        validate_um_positions(sources[1], context.scope, &mut issues);

        if !issues.is_empty() {
            status = ProjectionStatus::Unsupported;
        }
    }

    let value = if issues.is_empty() {
        let ts_event = sources
            .iter()
            .filter_map(|source| source.and_then(|stored| stored.slot.last_response()))
            .map(|observation| observation.ts_received)
            .max()
            .unwrap_or(context.ts_init);
        match AccountState::new(
            context.account_id,
            AccountType::Margin,
            Vec::new(),
            Vec::new(),
            true,
            UUID4::new(),
            ts_event,
            context.ts_init,
            None,
        )
        .with_total_only_balances(totals)
        {
            Ok(state) => Some(state),
            Err(e) => {
                issues.push(e.to_string());
                status = ProjectionStatus::Unsupported;
                None
            }
        }
    } else {
        None
    };
    ProjectionResult {
        status,
        value,
        issues,
        sources: validation.sources,
        collection_span_ns: validation.collection_span_ns,
    }
}

fn project_risk<'a>(
    source: Option<&'a StoredObservation>,
    now: Instant,
    max_receipt_age: Duration,
    max_collection_span: Duration,
    canceled: bool,
) -> ProjectionResult<'a, PortfolioMarginRiskView<'a>> {
    let validation = validate_sources(
        &[source],
        now,
        max_receipt_age,
        max_collection_span,
        canceled,
    );

    if !validation.issues.is_empty() {
        return ProjectionResult {
            status: validation.status,
            value: None,
            issues: validation.issues,
            sources: validation.sources,
            collection_span_ns: validation.collection_span_ns,
        };
    }

    let Some(observation) = source.and_then(|stored| stored.slot.last_response()) else {
        return ProjectionResult {
            status: ProjectionStatus::Missing,
            value: None,
            issues: vec!["Validated PAPI account observation is unavailable".to_string()],
            sources: validation.sources,
            collection_span_ns: validation.collection_span_ns,
        };
    };
    let ObservationData::Account(summary) = &observation.data else {
        return ProjectionResult {
            status: ProjectionStatus::Unsupported,
            value: None,
            issues: vec!["PAPI account source contained unexpected data".to_string()],
            sources: validation.sources,
            collection_span_ns: validation.collection_span_ns,
        };
    };
    let status_recognized = summary
        .account_status
        .require("accountStatus")
        .is_ok_and(|status| matches!(status.as_str(), "NORMAL" | "REDUCE_ONLY"));

    ProjectionResult {
        status: ProjectionStatus::Available,
        value: Some(PortfolioMarginRiskView {
            endpoint: observation.source.endpoint(),
            generation: observation.generation,
            account_summary: summary,
            units: RiskUnits {
                uni_mmr: "ratio",
                account_equity: "USD",
                actual_equity: "USD",
                account_initial_margin: "unverified",
                account_maint_margin: "USD",
                virtual_max_withdraw_amount: "USD withdrawal capacity",
                total_available_balance: "unverified",
                total_margin_open_loss: "USD",
            },
            status_recognized,
        }),
        issues: Vec::new(),
        sources: validation.sources,
        collection_span_ns: validation.collection_span_ns,
    }
}

fn validate_product_scope(
    sources: &[Option<&StoredObservation>],
    scope: &InstrumentScope,
    issues: &mut Vec<String>,
) {
    for (index, source) in sources.iter().enumerate() {
        let Some(ObservationData::ScopeRows(rows)) = source
            .and_then(|stored| stored.slot.last_response())
            .map(|observation| &observation.data)
        else {
            issues.push("Validated PAPI scope source contained unexpected data".to_string());
            continue;
        };

        match index {
            0 | 1 => {
                for row in rows {
                    let result = row
                        .symbol
                        .require("symbol")
                        .and_then(|symbol| scope.instrument(symbol).map(|_| symbol));

                    if let Err(e) = result {
                        issues.push(e.to_string());
                    }
                }
            }
            2 => {
                for row in rows {
                    let identity = row
                        .pair
                        .require("pair")
                        .or_else(|_| row.symbol.require("symbol"));

                    let Ok(identity) = identity else {
                        issues.push("PAPI CM position has no valid pair or symbol".to_string());
                        continue;
                    };

                    if identity
                        .chars()
                        .any(|c| c.is_whitespace() || c.is_control())
                    {
                        issues.push("PAPI CM position has invalid pair or symbol".to_string());
                        continue;
                    }

                    match row.position_amt.require("positionAmt") {
                        Ok(amount) if amount.value().is_zero() => {}
                        Ok(_) => {
                            issues.push("PAPI account has unsupported CM exposure".to_string());
                        }
                        Err(e) => issues.push(e.to_string()),
                    }
                }
            }
            3 if !rows.is_empty() => {
                issues.push("PAPI account has unsupported CM open orders".to_string());
            }
            4 if !rows.is_empty() => {
                issues.push("PAPI account has unsupported cross-margin open orders".to_string());
            }
            _ => {}
        }
    }
}

fn validate_um_positions(
    source: Option<&StoredObservation>,
    scope: &InstrumentScope,
    issues: &mut Vec<String>,
) {
    let Some(ObservationData::UmAccount(account)) = source
        .and_then(|stored| stored.slot.last_response())
        .map(|observation| &observation.data)
    else {
        issues.push("Validated PAPI UM account source contained unexpected data".to_string());
        return;
    };

    for position in &account.positions {
        match position.position_amt.require("positionAmt") {
            Ok(amount) if amount.value().is_zero() => {}
            Ok(_) => {
                if let Err(e) = scope.instrument(&position.symbol) {
                    issues.push(e.to_string());
                }
            }
            Err(e) => issues.push(e.to_string()),
        }
    }
}

struct SourceValidation<'a> {
    status: ProjectionStatus,
    issues: Vec<String>,
    sources: Vec<ProjectionSource<'a>>,
    collection_span_ns: Option<u128>,
}

fn validate_sources<'a>(
    sources: &[Option<&'a StoredObservation>],
    now: Instant,
    max_receipt_age: Duration,
    max_collection_span: Duration,
    canceled: bool,
) -> SourceValidation<'a> {
    let mut issues = Vec::new();
    let mut status = ProjectionStatus::Available;
    let mut generations = Vec::new();
    let mut earliest_request_age: Option<u128> = None;
    let mut latest_receipt_age: Option<u128> = None;
    let source_views = sources
        .iter()
        .flatten()
        .map(|source| ProjectionSource {
            endpoint: source.slot.source().endpoint(),
            source_scope: source.slot.source(),
            receipt_status: if canceled {
                ReceiptStatus::Canceled
            } else {
                source.slot.receipt_status(now, max_receipt_age)
            },
            failure: source.slot.failure(),
            generation: source
                .slot
                .last_response()
                .map(|observation| observation.generation),
            timing: source.slot.timing(now),
            response_metadata: source.metadata.as_ref(),
        })
        .collect();

    if canceled {
        return SourceValidation {
            status: ProjectionStatus::Canceled,
            issues: vec!["PAPI client was canceled".to_string()],
            sources: source_views,
            collection_span_ns: collection_span(sources, now),
        };
    }

    for source in sources {
        let Some(source) = source else {
            issues.push("Required PAPI observation source is missing".to_string());
            update_status(&mut status, ProjectionStatus::Missing);
            continue;
        };
        let receipt_status = source.slot.receipt_status(now, max_receipt_age);

        if let Some(observation) = source.slot.last_response() {
            generations.push(observation.generation);
        }

        if receipt_status != ReceiptStatus::Recent {
            issues.push(format!(
                "PAPI source {} is {receipt_status:?}",
                source.slot.source().endpoint()
            ));
            update_status(&mut status, projection_status(receipt_status));
            continue;
        }

        if source.slot.last_response().is_none() {
            issues.push("Required PAPI observation is missing".to_string());
            update_status(&mut status, ProjectionStatus::Missing);
            continue;
        }
        let timing = source.slot.timing(now);
        let (Some(age), Some(span)) = (timing.receipt_age_ns, timing.collection_span_ns) else {
            issues.push("PAPI source has no monotonic timing".to_string());
            update_status(&mut status, ProjectionStatus::Inconsistent);
            continue;
        };
        earliest_request_age = Some(
            earliest_request_age
                .unwrap_or_default()
                .max(age.saturating_add(span)),
        );
        latest_receipt_age = Some(latest_receipt_age.unwrap_or(u128::MAX).min(age));
    }

    if generations.windows(2).any(|pair| pair[0] != pair[1]) {
        issues.push("Required PAPI sources have different generations".to_string());
        update_status(&mut status, ProjectionStatus::Inconsistent);
    }

    let collection_span_ns = earliest_request_age
        .zip(latest_receipt_age)
        .map(|(earliest, latest)| earliest.saturating_sub(latest));

    if collection_span_ns.is_some_and(|span| span > max_collection_span.as_nanos()) {
        issues.push("PAPI source collection span exceeds its bound".to_string());
        update_status(&mut status, ProjectionStatus::Inconsistent);
    }

    SourceValidation {
        status,
        issues,
        sources: source_views,
        collection_span_ns,
    }
}

fn collection_span(sources: &[Option<&StoredObservation>], now: Instant) -> Option<u128> {
    let mut earliest_request_age: Option<u128> = None;
    let mut latest_receipt_age: Option<u128> = None;

    for timing in sources
        .iter()
        .flatten()
        .map(|source| source.slot.timing(now))
    {
        let (Some(age), Some(span)) = (timing.receipt_age_ns, timing.collection_span_ns) else {
            continue;
        };
        earliest_request_age = Some(
            earliest_request_age
                .unwrap_or_default()
                .max(age.saturating_add(span)),
        );
        latest_receipt_age = Some(latest_receipt_age.unwrap_or(u128::MAX).min(age));
    }

    earliest_request_age
        .zip(latest_receipt_age)
        .map(|(earliest, latest)| earliest.saturating_sub(latest))
}

fn projection_status(status: ReceiptStatus) -> ProjectionStatus {
    match status {
        ReceiptStatus::Missing => ProjectionStatus::Missing,
        ReceiptStatus::Recent => ProjectionStatus::Available,
        ReceiptStatus::Failed => ProjectionStatus::Failed,
        ReceiptStatus::Refreshing => ProjectionStatus::Refreshing,
        ReceiptStatus::Canceled => ProjectionStatus::Canceled,
        ReceiptStatus::Stale => ProjectionStatus::Stale,
    }
}

fn update_status(current: &mut ProjectionStatus, candidate: ProjectionStatus) {
    let priority = |status| match status {
        ProjectionStatus::Available => 0,
        ProjectionStatus::Unsupported => 1,
        ProjectionStatus::Inconsistent => 2,
        ProjectionStatus::Missing => 3,
        ProjectionStatus::Stale => 4,
        ProjectionStatus::Failed => 5,
        ProjectionStatus::Refreshing => 6,
        ProjectionStatus::Canceled => 7,
    };

    if priority(candidate) > priority(*current) {
        *current = candidate;
    }
}

fn validate_wallet_row(row: &crate::observations::models::AssetBalance) -> anyhow::Result<Money> {
    let borrowed = row
        .cross_margin_borrowed
        .require("crossMarginBorrowed")?
        .value();
    let interest = row
        .cross_margin_interest
        .require("crossMarginInterest")?
        .value();
    anyhow::ensure!(
        borrowed.is_zero() && interest.is_zero(),
        "PAPI asset {} has unsupported nonzero borrowing or interest",
        row.asset
    );
    let currency = Currency::try_from_str(&row.asset)
        .ok_or_else(|| anyhow::anyhow!("Unknown PAPI asset currency {}", row.asset))?;
    let amount = row
        .total_wallet_balance
        .require("totalWalletBalance")?
        .value();
    let money = Money::from_decimal(amount, currency)
        .map_err(|_| anyhow::anyhow!("PAPI asset {} cannot be represented as Money", row.asset))?;
    anyhow::ensure!(
        money.as_decimal() == amount,
        "PAPI asset {} loses precision when represented as Money",
        row.asset
    );
    Ok(money)
}

fn find_source(
    sources: &[StoredObservation],
    predicate: impl Fn(&ObservationSource) -> bool,
) -> Option<&StoredObservation> {
    sources
        .iter()
        .find(|source| predicate(source.slot.source()))
}
