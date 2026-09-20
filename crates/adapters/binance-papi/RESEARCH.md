# PAPI Read-Only Integration Research

Research date: 2026-09-12. Scope: [issue 3][issue-3], under [issue 1][issue-1], while
[issue 2][issue-2] implements the construction and Python installation skeleton.

This is the original design investigation, not a statement of live compatibility. Its evidence
consists of local source inspection and Binance's official documentation. No credentials,
authenticated requests, trading operations, or new adapter runtime tests were used for this report.
Later implementation outcomes are identified explicitly so historical proposals are not mistaken
for the adapter's current capability.

The later [UM account V2 verification](V2_VERIFICATION.md) records a separate authenticated
GET comparison on 2026-09-13, its observed coverage and field differences, and its remaining limits.
The [account mapping verification](ACCOUNT_VERIFICATION.md) adds balance, liability, fee, funding,
and failure-handling evidence, along with the decision to calculate unrealized PnL from live prices.

## Historical findings and implementation gates

Offline REST wrapping, exact parsing, and reconciliation work could proceed independently of the
installation skeleton. The selected direction was native asset accounting with separate PM risk
observations (option A). The later implementation publishes only a diagnostic totals-only wallet
after validating its supported zero-liability scope; PM admission and trading remain unavailable.

| Finding                                                                                                                  | Consequence                                                                                                |
| ------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------- |
| Nautilus has account-wide margins and an account `info` bag.                                                             | PM observations can be retained without inventing an instrument or changing core types.                    |
| PM purchasing power does not have an established mapping to native-currency `AccountBalance.free`.                       | Do not treat the PM account as a regular UM wallet.                                                        |
| The risk engine checks free balance in the calculated margin currency and can skip that check when the bucket is absent. | A USD-only account projection does not establish safe USDT/USDC order admission.                           |
| SDK 69.2.1 drops headers from HTTP error results and combines some transport and decode failures.                        | Reliable backoff and typed failure classification need an explicit SDK-boundary solution.                  |
| The SDK contains both deprecated conditional endpoints and their algo replacements.                                      | Use the current algo family; SDK method availability does not prove endpoint support.                      |
| Historical reads require a symbol, and time-window limits do not prove retention or completeness.                        | Declare instrument and history coverage explicitly; never infer completeness from current positions alone. |

## Evidence baseline

The core source inspected is repository commit
`1efe65a394392626e8eee9a6aa91aff9737732da`. The working tree also contains unfinished issue 2
changes; its PAPI skeleton is not a stable implementation baseline.

The locally cached `binance-sdk-69.2.1.crate` has SHA-256
`ac64a36e1c8f27a8fc2d13241fbf3b2a4ed71aad706b13650bfd10ea41a2c515`.
The 148 REST-module and common/configuration source files compared match that cached archive.
Its VCS metadata identifies Binance connector commit
`f1fde6a692fb69cd5c5c9090e49af3281f6d98a2`. This comparison checks local source consistency,
not a fresh build or authenticated behavior. The upstream source is
[Binance's Rust connector at the recorded commit][sdk-source].

SDK source references below use paths within that crate. Repository links refer to the current
checkout. Recheck relevant conclusions after upstream synchronization or an SDK change.

## Account representation

### What the current core can represent

Read [AccountBalance and MarginBalance](../../model/src/types/balance.rs),
[AccountState](../../model/src/events/account/state.rs),
[MarginAccount](../../model/src/accounts/margin.rs), and the
[accounting contracts](../../../docs/concepts/accounting.md).

- `AccountBalance` stores one currency's total, locked, and free values with
  `total == locked + free`. Negative totals are representable; they are not themselves a core gap.
- `MarginBalance` with `instrument_id=None` stores an account-wide margin entry by currency.
  Per-instrument and account-wide margins coexist and their totals can be added, so publishing the
  same obligation at both scopes double-counts it.
- `AccountState::with_info` can preserve venue-specific values. Use exact decimal strings, currency
  labels, source endpoint/version, and observation times. These fields are not automatically inputs
  to Nautilus risk calculations.
- `MarginAccount::apply` replaces margin stores on a nonempty account update. Balance updates
  replace supplied currency entries and retain omitted currencies. A partial REST failure must not
  clear margins; an omitted balance row does not automatically remove a stale balance.
- A venue-reported account should use `calculate_account_state=false` so local order/position
  accounting does not overwrite its reported balances. This does not implement PM risk rules.

### Historical field interpretation

The [official account reference][account-api] and SDK models expose the fields below. The treatment
was the design recommendation at the research date; formulas marked unresolved were not established
by the response examples.

| Source field or group                                                               | Treatment selected for investigation                                                                       |
| ----------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `/balance`: `totalWalletBalance`                                                    | Candidate native-asset total, once liability inclusion is verified. Never add its wallet components again. |
| `crossMarginAsset`, `crossMarginBorrowed`, `crossMarginInterest`, `negativeBalance` | Preserve separately. Verify which deductions are already included before computing a net balance.          |
| `crossMarginFree`, `crossMarginLocked`                                              | Preserve as cross-Margin observations. Do not equate them with PM-wide available/reserved amounts.         |
| `umWalletBalance`, `cmWalletBalance`, UM `crossWalletBalance`                       | Preserve as component observations; do not create a second authoritative wallet.                           |
| `umUnrealizedPNL`, `cmUnrealizedPNL`, UM `crossUnPnl`                               | Optional diagnostics, separate from wallet totals and locally calculated PnL.                              |
| `/account`: `accountEquity`, `actualEquity`                                         | USD valuations with different collateral-rate treatment. Keep distinct from native wallet assets.          |
| `accountInitialMargin`, `accountMaintMargin`                                        | Candidate account-wide USD margin observation, subject to unit and precision validation.                   |
| `totalAvailableBalance`, `virtualMaxWithdrawAmount`                                 | Preserve separately. Withdrawal capacity is not a substitute for order purchasing power.                   |
| `uniMMR`, `accountStatus`, `totalMarginOpenLoss`                                    | Preserve as venue risk context; unknown statuses remain explicit.                                          |
| UM `initialMargin`, `maintMargin`, position/open-order components                   | Useful diagnostics; do not add them to a PM aggregate that already includes them.                          |

The account reference leaves some units/descriptions sparse and includes empty numeric strings in
examples. An empty value is not zero. Validate fields required by a projection; represent genuinely
optional, documented unavailable observations as unavailable. Example values do not prove a
cross-field accounting identity.

### Mapping decision

Selected direction as of 2026-09-12: option A, native asset accounting with separate PM risk
observations. Keep one PM account owner and preserve assets, liabilities, and fees in their source
currencies. Keep PM valuation, margin requirements, and available capacity distinct from those
asset balances. The single USD valuation-account alternative is not selected.

The selected account projection uses `base_currency=None` and totals-only native balances. It does
not publish an account-wide USD `MarginBalance`; PM observations remain separate and preserve exact
decimals. The projection deliberately leaves native `free/locked` unavailable and does not provide
a PM order check.

REST synchronizes wallet balances, position quantities, and `entryPrice`; live prices and
synchronized positions drive local unrealized PnL. REST unrealized PnL is optional diagnostic data,
not a required account-projection or startup field. Sequential responses need not agree on PnL.
Venue PM equity, margins, and capacity remain separate risk inputs with their own validation and
freshness requirements. See the follow-up verification for the tested scope and remaining gaps.

Stage exact observations privately and build order/fill/position reports independently of the
unresolved balance projection. Reuse `AccountState.info` when an account event has a valid typed
projection; do not fabricate zero/free balances merely to get an event accepted. A read-only
research result is not a trading-ready account. The following constraints still apply:

- [Portfolio::equity](../../portfolio/src/portfolio.rs) adds position unrealized PnL to margin
  account totals. Publishing an equity figure that already includes PnL as the wallet total can
  count PnL twice. `calculate_account_state=false` does not change this equity formula.
- The margin-order path in the [risk engine](../../risk/src/engine/mod.rs) calculates an initial
  margin requirement and asks for `balance_free` in that requirement's currency. A missing balance
  takes a `continue` path. A USD margin observation or `info` value does not replace that check.
- `AccountBalance::from_total_and_free` and `from_total_and_locked` clamp their supplied component
  for nonnegative totals. This can hide a semantic mismatch such as genuine PM debt or purchasing
  power exceeding a native wallet. Validate meaning and exact representation before construction;
  an algebraically valid result is not evidence of economic correctness.
- USD `Money` normally has currency precision different from crypto valuation fields. Do not
  relabel USD as USDT or redefine a global currency to retain extra decimals. Preserve exact
  observations and explicitly resolve any lossy typed projection.

The implementation selected the scoped core extension: `AccountState.total_only_balances`
represents known native totals while keeping free/locked unavailable. It is restricted to reported
margin accounts without local account-state calculation. Native risk checks fail closed for
risk-increasing orders when only totals are available; no PM admission authority is inferred.

### Option A observation contract

The following groups describe adapter-internal data requirements, not new public types or finalized
configuration fields. Reuse the field inventory above and preserve source provenance until each
economic formula is verified.

| Group                | Identity and contents                                                                             | Validation and use                                                                                       |
| -------------------- | ------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| Native assets        | PM account plus asset; total wallet observation and wallet components.                            | One authoritative asset entry; preserve components without adding them again.                            |
| Native liabilities   | Same account and asset; borrowed principal, interest, and negative-balance observations.          | Keep each source amount separate; verify overlap and inclusion in wallet totals before deduction.        |
| Unrealized PnL       | Account, asset, and product; optional UM/CM reported unrealized amounts.                          | Diagnostics only; local PnL uses live prices and synchronized positions.                                 |
| PM risk observations | Account-level equity, margin, available capacity, withdrawal capacity, ratio, and status.         | Retain each field's verified unit; an unknown unit or unavailable amount cannot drive order admission.   |
| Observation metadata | Endpoint/version, requested scope, source update times, receipt times, and collection generation. | Track success and coverage per source; collection generation does not imply an atomic exchange snapshot. |

Use `Decimal` for validated internal amounts and decimal strings for serialized observations.
Keep absent, null, empty, malformed, and valid numeric fields distinguishable at the wire boundary.
Required malformed values fail validation; documented optional unavailability remains explicit.
Resolve currency identity and precision before projecting to `Money`.

A collection is eligible for account projection only when every source required by that projection
has succeeded and its fields are consistent under verified semantics. A failed refresh retains the
last successful observation for inspection, but marks its risk usability unavailable or stale.
Elapsed age must use a monotonic clock. Tolerated age and cross-source skew need explicit bounds and
tests; a repeated old venue timestamp is not proof of a fresh economic state.

Account-risk changes can occur without a native balance change. Do not suppress those updates using
`AccountState::has_same_balances_and_margins` alone: it intentionally ignores `info`. Preserve the
latest status, capacity, and freshness independently of balance-event deduplication.

### Risk integration decision criteria

The initial objective is conservative local admission based on verified venue observations and
bounded local reservations. Exact reproduction of Binance's full risk calculation is not assumed.
An admission estimate still needs validated incremental-margin and currency semantics; an account
equity figure alone does not provide such an estimate.

| Location               | Acceptance condition                                                                                                                                    | Main integration concern                                                                                   |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| PAPI execution adapter | Every risk-increasing order path passes one account-level gate, including orders created by execution algorithms and exposure-increasing modifications. | Existing core margin checks must not incorrectly reject an otherwise supported PM order.                   |
| Minimal core extension | A supported account capability supplies the PM check through the common admission path while existing account behavior remains intact.                  | The extension must stay focused, carry sufficient account/order context, and preserve lifecycle semantics. |

Evaluate both locations against the same requirements before choosing:

- A shared account budget covers all scoped instruments and strategies; it is never copied into
  both USDT and USDC free balances or into multiple product clients.
- Concurrent requests reserve capacity before submission. Release or reconcile reservations only
  on authoritative outcomes, including fills, rejections, cancellations, and unknown-result recovery.
  A REST refresh must not erase a reservation for an order not yet reflected by the venue snapshot.
- Missing, stale, inconsistent, or unknown-status risk data blocks risk-increasing admission. Define
  position-reducing behavior separately; do not assume every buy/sell or modification reduces risk.
- Validate that native balances, account margins, reported PnL, local PnL, and fees are counted once
  through both account queries and `Portfolio::equity`.
- A PM check must coexist with the common order-validation and risk limits. Do not obtain
  compatibility by bypassing the risk engine or returning zero calculated margin.

These are design and test obligations for later trading work. The read-only implementation collects
the required evidence; it does not enable trading or claim that either admission path is implemented.

### Account mapping acceptance cases

The option A acceptance cases below remain separate from REST connectivity and pagination
validation. The implementation has offline regressions for its supported and rejected states;
authenticated evidence remains subject to the explicit limits in
[ACCOUNT_VERIFICATION.md](ACCOUNT_VERIFICATION.md).

| Case                                                   | Required evidence                                                                                                         |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------- |
| BTC collateral with no USDT wallet balance             | Asset queries retain BTC and actual USDT amounts; PM capacity is separate and no USDT balance is invented.                |
| Borrowed funds, accrued interest, and negative balance | Identify which source totals already include each liability; prove that the projection subtracts each obligation once.    |
| Existing UM gains or losses                            | Wallet totals exclude unrealized PnL; live prices drive local PnL. REST PnL is diagnostic, not a required equality check. |
| Changes only in account risk status or capacity        | Preserve the update even when native balances and margin amounts are unchanged.                                           |
| USD valuation with more digits than `Money` supports   | Preserve the exact observation; reject a lossy typed projection until an explicit representation policy is accepted.      |
| Timeout, partial response, or stale snapshot           | Retain previous observations for inspection without treating them as current admission authority.                         |
| Multiple strategies and settlement currencies          | Later admission tests prove a single shared capacity budget and correct reservation lifecycle.                            |

The implementation does not claim a valid native `free/locked` mapping. It uses the scoped
totals-only representation and keeps those components unavailable. Choosing option A is not
permission to substitute cross-Margin free balance, withdrawal capacity, or a synthetic zero for
an unknown PM value.

## REST and SDK boundary

### Initial endpoint inventory

All paths below are GET requests. Weights are the documented IP costs, cross-checked against
`src/derivatives_trading_portfolio_margin/rest_api/mod.rs`. They are configuration evidence as of
the research date, not permanent constants guaranteed by Binance.

| Purpose                  | Path                              | SDK method                              | Weight                              |
| ------------------------ | --------------------------------- | --------------------------------------- | ----------------------------------- |
| Shared asset balances    | `/papi/v1/balance`                | `account_balance`                       | 20                                  |
| PM risk summary          | `/papi/v1/account`                | `account_information`                   | 20                                  |
| UM details               | `/papi/v1/um/account`             | `get_um_account_detail`                 | 5                                   |
| UM details V2            | `/papi/v2/um/account`             | `get_um_account_detail_v2`              | 5                                   |
| Position mode            | `/papi/v1/um/positionSide/dual`   | `get_um_current_position_mode`          | 30                                  |
| UM positions             | `/papi/v1/um/positionRisk`        | `query_um_position_information`         | 5                                   |
| Ordinary open orders     | `/papi/v1/um/openOrders`          | `query_all_current_um_open_orders`      | 1 per symbol; 40 without symbol     |
| CM positions             | `/papi/v1/cm/positionRisk`        | `query_cm_position_information`         | 1                                   |
| CM open orders           | `/papi/v1/cm/openOrders`          | `query_all_current_cm_open_orders`      | 1 per symbol; 40 without symbol     |
| Margin open orders       | `/papi/v1/margin/openOrders`      | raw GET; see below                      | 5 IP weight; dynamic unscoped count |
| Ordinary order lookup    | `/papi/v1/um/order`               | `query_um_order`                        | 1                                   |
| Ordinary history         | `/papi/v1/um/allOrders`           | `query_all_um_orders`                   | 5                                   |
| Fills                    | `/papi/v1/um/userTrades`          | `um_account_trade_list`                 | 5                                   |
| Open algo orders         | `/papi/v1/um/algo/openAlgoOrders` | `query_all_current_um_open_algo_orders` | 1 per symbol; 40 without symbol     |
| Algo history             | `/papi/v1/um/algo/allAlgoOrders`  | `query_um_algo_order_history`           | 5                                   |
| Order quota observations | `/papi/v1/rateLimit/order`        | `query_user_rate_limit`                 | 1                                   |

See the [account][account-api] and [trade][trade-api] references. Fetching order quota observations
does not consume a new-order slot or replace IP-weight accounting. Public instrument discovery
continues through the existing Binance adapter. SDK 69.2.1 incorrectly requires `symbol` for the
margin open-orders request even though its generated endpoint documentation says omission returns
all symbols. That documentation assigns IP weight 5 but also says the unscoped request count equals
the number of currently trading symbols. The wrapper uses the documented IP weight; authenticated
acceptance must verify the unscoped behavior and observed quota headers before repeated collection.

### Current documentation corrections

The [2026-04-14 change-log entry][changelog-algo] enables the UM algo family and deprecates the UM
conditional family effective 2026-04-28. SDK 69.2.1 contains both. Use the algo methods above;
do not build a fresh integration on `/um/conditional/*`. Reading external algo orders matters even
if the future first trading release only submits ordinary limit/market orders.

The [2024-08-23 entry][changelog-v2] describes UM account V2 as returning symbols with positions or
open orders. Its current account-reference field description instead says all market symbols are
returned. Treat V2 as sparse until successful authenticated observations establish coverage; use
`positionRisk` as the candidate position source and verify its coverage independently.

### Fixed dependency and precision

Use an exact Cargo version requirement, `=69.2.1`, with
`derivatives_trading_portfolio_margin` as the only product feature. Pinning the version does not
settle the TLS configuration: `Cargo.toml` leaves reqwest defaults enabled and enables native TLS
on tokio-tungstenite. Product-feature isolation does not remove these transport dependencies.
Retain the parent's TLS packaging decision as separate work; repeat dependency checks on the
formal adapter after integration.

Relevant monetary fields in the inspected account, order, and trade models are `Option<String>`;
identifiers and timestamps are generally `Option<i64>`. The balance response is an untagged
array/object union. A successful Serde decode is not a valid domain observation: missing fields,
empty strings, unexpected enums, duplicate rows, and identity mismatches need explicit validation.

At the conversion boundary:

- Parse decimal text exactly and reject overflow or excess nonzero precision. Use exact decimal
  parsing that does not silently round input beyond Decimal's capacity.
- Use instrument/currency precision, then verify round-trip equality after domain construction.
  Price/quantity constructors may round; decimal arithmetic alone does not prevent data loss.
- Preserve signed money, fees/rebates, and signed positions. Convert an absolute position quantity
  only after determining and validating its side. Do not apply `abs` to commission.
- Keep IDs as validated integer/string identities. Convert milliseconds to nanoseconds with checked
  arithmetic. Do not turn missing or invalid event times into the current time without a field-specific
  documented rule; zero position-update timestamps require their own coverage semantics.
- Validate `commissionAsset` independently of instrument settlement currency. Failure to represent
  a required commission fails the report request.

### Transport limitations that need implementation decisions

Inspection of SDK `src/common/config.rs`, `models.rs`, `errors.rs`, and `utils.rs` establishes:

- `ConfigurationRestApi` defaults to three retries and a 1000 ms timeout. Set retries to zero and
  choose explicit request and total-operation deadlines.
- Its private reqwest client cannot be replaced with Nautilus `HttpClient`. `HttpAgent` modifies a
  reqwest builder; it is not a response interceptor or a shared-transport replacement.
- Successful `RestApiResponse` values expose status, headers, and rate-limit observations.
  Capture metadata before consuming `data().await`.
- `http_request` discards response headers when it creates an error, including 429 and 418.
  Consequently the adapter cannot recover a header-only `Retry-After` through the current API.
  It also replaces 5xx message details with a generic status message.
- Request transport failures and response decoding failures can both become `ConnectorClientError`.
  Keep the request/decode phases distinct, but do not infer a typed network failure from arbitrary
  error display text. Decoding has its own `data().await` failure boundary; body-read failures occur
  earlier and remain less precise.
- `send_request` obtains a timestamp internally. A retry must invoke the SDK method again after
  quota acquisition to get a fresh signature. No timestamp-offset injection was found in this
  configuration; `-1021` needs clock diagnosis rather than repeated unchanged calls.
- Transport error strings can contain request URLs. Redact before logging or publishing diagnostics;
  do not emit SDK debug/error chains with signed query strings.

Recommended first boundary policy: typed known 5xx failures and adapter-owned timeouts may receive
bounded read retries; parse/validation errors do not. Unknown SDK client errors fail the read.
On throttling/ban without a reliable delay, fail the request and latch the shared gate closed so
subsequent reconciliation cycles cannot immediately retry. A timed automatic recovery policy needs
a verified source of the delay, likely a narrowly scoped SDK fix preserving error metadata. Do not
silently replace a missing delay with zero or claim the stock SDK can honor every server backoff.

### Rate limiting and errors

The [general information][general-info] specifies a PAPI IP allowance of 6000 weight/minute and
an account order allowance of 1200/minute. These are different quotas. Do not import the regular
FAPI/DAPI 2400/minute limit into PAPI solely because instruments share `BINANCE` identity.

Use shared [RateLimiter](../../network/src/ratelimiter/mod.rs) state across PAPI clients that share
an allowance, with endpoint weight charged before every attempt and page. Its key planner accounts
for repeated keys, which can represent weight; validate the quota/burst configuration against the
venue window and reserve room for other processes. HTTP client ownership does not define quota
ownership. Use [RetryManager](../../network/src/retry.rs) for cancellation, elapsed budgets, and
bounded backoff where its contract fits.

| Evidence                                                          | Read policy                                                                                               |
| ----------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| 401/403, `-2014`, `-2015`, `-1022`                                | Authentication, permission, or signature failure; no blind retry.                                         |
| `-1021`                                                           | Clock/receive-window failure; diagnose before trying again.                                               |
| 429/418 or venue throttling code                                  | Stop the shared request gate; apply only a verified recovery policy.                                      |
| Known transient 5xx or adapter timeout                            | Bounded GET retry within the original operation budget.                                                   |
| Decode, precision, schema, unsupported status, missing instrument | Fail or mark bounded historical coverage incomplete according to the report contract.                     |
| `-2013`                                                           | Only a correctly scoped single-order query can provide absence evidence; not a generic empty-list result. |
| Unknown SDK error                                                 | Preserve uncertainty and fail the query; never report a vanished account/order/position.                  |

Codes are from the [PAPI error reference][errors]. Retention, the endpoint family, and identity must
be checked before treating an order-not-found result as authoritative. An HTTP 404 is not sufficient.

## Reconciliation design

### Instrument and identity coverage

Reuse [BinanceFuturesHttpClient::request_instruments_with_config](../binance/src/futures/http/client.rs)
and [format_binance_symbol](../binance/src/common/symbol.rs). The existing Python
`load_binance_instruments` facade provides the public loader for node construction. Several Binance
precision and futures conversion helpers are `pub(crate)` and cannot be imported by this crate;
write focused PAPI parsers against public domain types rather than broadening upstream visibility.
The public `BinanceOrderStatus::to_nautilus_order_status` and time-in-force conversions in
[futures HTTP models](../binance/src/futures/http/models.rs) are candidates for direct reuse after
validating PAPI enum semantics. Create the public metadata client without PAPI credentials: its
instrument-loading path may request regular futures fee information when credentials are present.

Load required metadata before report generation. A missing in-scope active-order/position instrument
is an error. An unresolved historical instrument may make a bounded mass status incomplete. Only an
explicit provider scope permits dropping out-of-scope records; engine-side reconciliation filters
run too late to solve adapter parsing coverage.

History endpoints require a symbol. A scan over current positions, open orders, and cache entries
alone misses a symbol traded and fully closed outside this process. Prefer an explicit initial
instrument scope, scan every symbol in that scope, and state that coverage. If whole-UM discovery is
promised, include its full catalog and a strategy for retired instruments; inability to cover them
must not produce `reports_complete=true`.

Use one PM `AccountId`, a distinct `BINANCE_PAPI` client, and the existing `BINANCE` instrument venue.
Scope cache queries by account. Keep regular order IDs, algo IDs, and executed child-order IDs
distinct; verify stable client-order linkage through an algo trigger. Do not invent a new strategy
order from the child fill or replay the same fill against both parent and child.

`ExecutionMassStatus` keys orders by `VenueOrderId` alone. Verify symbol/order-family uniqueness or
define a reversible adapter identity encoding before inserting reports. Dedupe fills using account,
instrument, and trade identity and maintain identical encoding for later private-stream events.

### Report field mapping

These mappings use SDK wire models and current Nautilus report fields; they remain subject to the
identity and exact-conversion requirements above.

| Report                 | Inputs and mapping                                                                                                                                                                       |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `OrderStatusReport`    | Resolve `symbol`; link `orderId` and `clientOrderId`; map `origQty`/`executedQty` to quantity/filled quantity and `time`/`updateTime` to accepted/last timestamps.                       |
| Order execution terms  | Validate `side`, `type`, and `timeInForce`; preserve `reduceOnly`, post-only semantics, expiry, and meaningful price fields. A market-order zero price is not a fill price.              |
| Ordinary order status  | Reuse validated Binance status conversion: `NEW` to accepted, partial/filled/canceled/rejected to their corresponding states. Decide expiry policy explicitly and reject unknown values. |
| `FillReport`           | Map trade `id`, `orderId`, `qty`, `price`, `time`, and `side`; use `commission` plus `commissionAsset`; map the required `maker` flag to maker/taker liquidity.                          |
| `PositionStatusReport` | For one-way `BOTH`, derive long/short/flat from exact signed `positionAmt`, then absolute quantity; retain `entryPrice` as the average open price where meaningful.                      |
| Algo lifecycle         | Retain `algoId`/`clientAlgoId`, `algoStatus`, trigger metadata, and historical `actualOrderId`. Triggering is not evidence of a fill; fetch the child order and trades.                  |

An unfilled order's zero `avgPrice` is not an observed execution average. A missing commission,
maker flag, or required side is not a default value. For algo `closePosition` orders, an omitted or
zero quantity must not become an ordinary zero-quantity order: prove the close-position quantity
mapping from a covered position snapshot, or fail the in-scope report. The ordinary order and algo
status sets are separate; do not feed algo statuses into the ordinary conversion unconditionally.

### Bounded historical queries

The [trade reference][trade-api] sets a maximum result limit of 1000 for the ordinary history/fill
queries. Order-history windows must be shorter than seven days; fill windows may be at most seven
days. `fromId` cannot accompany fill start/end timestamps. These constraints do not establish a
complete retention horizon, the time field used to select orders, or cursor ordering guarantees.

Proposed initial algorithm:

1. Capture one end time, lower history bound, instrument scope, and total budget for the operation.
   Use fixed subwindows shorter than seven days. Convert bounds to venue milliseconds conservatively
   and apply exact requested time filters locally.
1. Query current ordinary and algo orders without the history lower bound. Keep active/in-flight
   orders even when they were created before it. Obtain and validate a complete position observation.
1. Query ordinary history, algo history, and fills for each scoped symbol. Start with time windows
   and an explicit limit. For a saturated page, bisect the inclusive millisecond interval into
   `[lo, mid]` and `[mid + 1, hi]`; never advance only to the last timestamp plus one.
1. Accept a non-saturated leaf only within the endpoint's verified coverage. If a single millisecond
   is saturated, fail coverage unless an independently verified ID-pagination path can exhaust it.
   Do not combine `fromId` with time parameters. An ID mode needs proofs of ordering, progress,
   boundary coverage, and termination; a first most-recent page is not proof of the earliest ID.
1. Bound pages, rows, concurrent requests, retries, and elapsed time. Repeated/nonprogressing pages,
   truncation, expired budgets, and ambiguous retention produce an error or incomplete history.
1. Dedupe overlapping observations, backfill required order reports for returned fills and tracked
   orders using targeted queries, and validate identity/status consistency. Creation-time filters
   may miss an old order filled or closed in the window; verify this venue contract explicitly.

This algorithm is a proposal for offline testing, not an observed Binance pagination guarantee.
Successful live reads must establish field selection and coverage before marking history complete.
The ordinary single-order endpoint documents expiry of some unfilled canceled/expired orders after
three days. Do not promise unlimited reconstruction, and do not infer another endpoint's retention
from its per-request window size.

### Completeness and failure behavior

Follow [the adapter report contract](../../../docs/developer_guide/adapters.md#reconciliation-reports)
and [execution reconciliation](../../../docs/concepts/execution/reconciliation.md).

- Override `generate_mass_status` to compose one fixed window and call
  `ExecutionMassStatus::set_report_window(Some(lower_bound), reports_complete)` explicitly. Its
  default completeness is true, which is unsafe for an unfinished collector.
- Set completeness true only after all required sources, scoped symbols, pages, mappings, and order
  linkage succeed. Independent REST responses are not an atomic exchange snapshot. Record observation
  times and reject contradictions; continuous REST/stream synchronization belongs to the next stage.
- An active-order or position query failure fails the report request. Commission conversion failure
  also fails the request. Other failed historical legs can preserve successful observations in an
  explicitly incomplete bounded mass status, following the core contract.
- Standalone bulk methods have no completeness flag. Return errors for partial required results;
  do not return a successful truncated vector.
- A single-order method returns `None` only for verified absence in the correct identity and
  endpoint scope. Failed queries return errors so the engine defers missing-order inference.
- Query position mode without changing it. Recommend proving one-way `BOTH` mode first; the issue 2
  netting metadata is not proof of account compatibility. Hedge-mode support needs stable side/position
  identities across reports and later stream events; otherwise reject that mode explicitly.
- Emit a flat report for an omitted touched position only after successful complete coverage proves
  absence. A sparse V2 response, stale cache, parse failure, or timeout does not establish that fact.

## Verification plan

Use `#[rstest]`, deterministic clocks, offline REST mocks, and synthetic credentials. Keep fixture
origins explicit: official example, synthetic edge case, or sanitized authenticated capture.
No test should depend on adapter environment variables.

| Area                    | Required cases                                                                                                                                                          |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Account models          | Array/object balance response; missing/null/empty values; duplicate asset rows; negative debt; mixed collateral; one shared wallet; partial source failure.             |
| Economic projection     | Borrow/interest inclusion; USD versus USDT; collateral discount; positive and negative PnL; no duplicated margin or PnL; no fabricated free balance.                    |
| Exact conversion        | Trailing zero equivalence; excess nonzero digits; Decimal overflow; domain overflow; signed rebates; fee in a third currency; invalid timestamps.                       |
| SDK boundary            | Zero automatic retries; fresh signed retry; 401/403; 429/418 with header-only delay; 5xx; truncated body; malformed JSON; signed-URL redaction.                         |
| Quotas and cancellation | Shared weighted budget across clients/pages/retries; exhausted deadline; cancellation while waiting; throttling latch blocks later cycles.                              |
| History                 | More than one page; exactly full page; same-millisecond saturation; duplicate/nonprogressing responses; lower/upper boundaries; closed-only symbol; retired instrument. |
| Orders and fills        | Old active order; fill whose order predates the window; missing linked order; algo parent/child transition; ID collision; unknown status; missing commission.           |
| Positions               | One-way long/short/flat; hedge-mode rejection or side identity; sparse V2; missing in-scope metadata; failed query never becomes flat.                                  |
| Engine behavior         | Incomplete history suppresses unsafe inference; failed periodic reads preserve cached state; missing lookup differs from failure; exact fees survive reconciliation.    |

Authenticated acceptance of the supported active, zero-liability projection completed on
2026-09-15 using a restricted key and GET requests only. The evidence records endpoint versions,
permissions, response shapes, times, quota headers, and sanitized results. Obtain evidence for both
quiet and active existing account states when available. Do not create positions, debt, transfers,
or orders to manufacture fixtures during read-only work.
Synthetic debt/limit tests do not replace successful authentication or prove live account semantics.

The parent records no usable PAPI testnet and a prior invalid-key 401 probe. This research does not
independently establish testnet availability, repeat the probe, or count it as successful acceptance.

## Implementation sequence and remaining decisions

1. Add the fixed SDK wrapper, strict internal observations, exact parsers, and transport mocks in the
   independent adapter. Resolve error-metadata/backoff handling before enabling automatic recovery.
1. Implement scoped ordinary/algo order, fill, and position reports with bounded queries and explicit
   completeness. Test actual engine reactions to errors and incomplete history.
1. Obtain authenticated observations and validate the selected option A field mapping, retention,
   V2 coverage, identifier uniqueness, and supported position mode. Decide the PM admission location
   using the criteria above; scope any required core change separately to preserve the thin fork.
1. Integrate the accepted config/lifecycle surface with issue 2 after its wiring settles. Preserve
   factory/venue identity and the CPython 3.14 baseline. Register any new adapter environment variable
   in `scripts/strip-adapter-env.bash` and keep tests independent of it.
1. On the final implementation, run focused Rust/REST/engine tests, Python boundary and wheel checks,
   feature-disabled dependency regression, `make format`, and `make pre-commit`. Repeat affected
   checks after integration edits. This research document does not satisfy those implementation gates.

The option A read-only implementation now includes exact observations, bounded reports, a
totals-only wallet projection, an independent PM risk view, scope checks, and fail-closed core risk
behavior. Automatic throttling recovery remains unavailable because SDK 69.2.1 discards error
headers. The current authenticated validation covers the supported projection and all newly added
account-wide scope queries; the explicit unsupported and unavailable live cases remain documented
rather than inferred.

[issue-1]: https://github.com/imbingox/nautilus_trader/issues/1
[issue-2]: https://github.com/imbingox/nautilus_trader/issues/2
[issue-3]: https://github.com/imbingox/nautilus_trader/issues/3
[account-api]: https://developers.binance.com/en/docs/catalog/advanced-trading-derivatives-trading-portfolio-margin/api/rest-api/account
[trade-api]: https://developers.binance.com/en/docs/catalog/advanced-trading-derivatives-trading-portfolio-margin/api/rest-api/trade
[general-info]: https://developers.binance.com/en/docs/products/derivatives-trading-portfolio-margin/general-info
[errors]: https://developers.binance.com/en/docs/products/derivatives-trading-portfolio-margin/error-code
[changelog-algo]: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/change-log#2026-04-14
[changelog-v2]: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/change-log#2024-08-23
[sdk-source]: https://github.com/binance/binance-connector-rust/tree/f1fde6a692fb69cd5c5c9090e49af3281f6d98a2
