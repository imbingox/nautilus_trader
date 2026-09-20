# PAPI Account Mapping Verification

Verification date: 2026-09-13. This follows [RESEARCH.md](RESEARCH.md) and the
[UM account V2 verification](V2_VERIFICATION.md). Source and offline tests use checkout
`c3716e2d0431246b90636adcf5bcd746fb283959`. This report records evidence and design decisions;
the subsequent implementation adds a read-only totals-only projection but does not enable LiveNode.

## Mapping decisions

Use native wallet balances and synchronize positions by quantity and `entryPrice`. Calculate
unrealized PnL locally from live prices and synchronized positions. REST unrealized PnL is optional
diagnostic data: it is neither a required account-projection/startup field nor a value that must
match exactly across sequential REST responses.

Keep venue PM equity, margin, capacity, and status separate from native wallet balances and local
PnL. These observations still need validated units and freshness for PM order admission. A local
position PnL calculation does not reproduce the exchange's collateral valuation or shared capacity.

The new evidence supports excluding unrealized PnL from wallet totals and avoiding another
deduction of the observed negative balance. It also verifies real order/fill linkage, fee signs,
and funding already reflected in wallet balances. Nonzero cross-Margin borrowing/interest remains
unsupported. The subsequent totals-only account representation intentionally keeps native
`free/locked` unavailable.

## Evidence collected

Two earlier account captures, starting at 02:39:40 and 02:46:00 UTC, contain `/balance`, `/account`,
and UM V1/V2 details. This verification analyzes those captures and adds two bounded collections:

| Collection                       | UTC interval         | Successful GETs | Scope                                                                                    |
| -------------------------------- | -------------------- | --------------- | ---------------------------------------------------------------------------------------- |
| Current account and recent flows | 08:46:39 to 08:46:51 | 13              | Account state before/after, positions, open orders, and seven days of income/interest.   |
| Existing position history        | 08:59:04 to 08:59:07 | 4               | One symbol, two days around its existing position update, approximately 65 days earlier. |

The first collection uses `/papi/v1/um/positionSide/dual`, `/papi/v1/balance`, `/papi/v1/account`,
`/papi/v2/um/account`, `/papi/v1/um/positionRisk`, `/papi/v1/um/openOrders`,
`/papi/v1/um/algo/openAlgoOrders`, `/papi/v1/um/income`,
`/papi/v1/margin/marginInterestHistory`, and `/papi/v1/portfolio/interest-history`.
Balance, PM summary, and UM V2 are each read twice. History queries have fixed start/end times.

The second collection uses `/papi/v1/um/userTrades`, `/papi/v1/um/allOrders`,
`/papi/v1/um/algo/allAlgoOrders`, and `/papi/v1/um/income`. Each request is limited to 1,000 rows;
none reaches that limit. Returned history rows match the requested symbol and time bounds.

An initial collection at 08:42:56 UTC stopped on its first request after a transport error, before
receiving an HTTP response. An unsigned ping subsequently succeeded, followed by the separate
successful collection above. The collectors have no automatic retries and stop on HTTP failures,
including throttling. Requests use verified TLS, no redirects or environment proxies, a ten-second
request deadline, a ninety-second collection deadline, and an eight-MiB response limit.

All authenticated operations are GETs. No trades, borrowing, transfers, cancellations, or account
mode changes were performed. Exact raw responses, metadata, analysis scripts, and test logs remain
outside the repository in an owner-only directory. Account amounts and identifiers are omitted
from this report. Monetary comparisons use `Decimal`; identifiers never pass through floats.

The current [official account reference][account-api] was read successfully. The trade reference
returned an empty page during this follow-up, so trade request contracts use the previously
recorded research and cached SDK 69.2.1, together with the successful authenticated responses.

## Wallets, liabilities, and negative balances

Both earlier captures and both new balance responses contain four asset rows. For every row:

```text
totalWalletBalance = crossMarginAsset + umWalletBalance + cmWalletBalance
```

In these samples, `crossMarginBorrowed` and `crossMarginInterest` are zero in all rows. Therefore,
this equality cannot distinguish a gross cross-Margin balance from one already net of principal
and interest. The official example is consistent with subtracting these liabilities inside
`crossMarginAsset`, but an example is insufficient to accept that formula generally. Require a
nonzero borrowing/interest sample before choosing further deductions.

One asset has a negative total, and its observations satisfy:

```text
negativeBalance = totalWalletBalance = umWalletBalance < 0
```

The other wallet components and unrealized PnL for that asset are zero. Its deficit is already in
the wallet total. Subtracting `abs(negativeBalance)` again duplicates it; subtracting the signed
negative field would instead cancel it. Preserve the field as an observation without a second
adjustment. This sample does not establish its general formula when borrowing or nonzero PnL
coexists with the negative wallet.

For a different asset, unrealized PnL is nonzero while the wallet-component equality still holds.
During the new collection, the PnL changes and wallet totals remain unchanged. This supports
keeping unrealized PnL outside the observed wallet total. All CM wallet/PnL components are zero;
nonzero CM behavior is not covered.

The seven-day cross-Margin interest response has `rows=[]` and `total=0`; the negative-balance
interest response is also empty. These results establish only the queried window's returned
records, not absence of debt or interest in every account state.

## Authenticated evidence and remaining limits

The 2026-09-13 captures cannot establish every acceptance case for the later implementation. The
current-build acceptance below resolves the projection and account-wide query gaps from those
captures. The following limits remain explicit and must not be replaced with synthetic live
evidence:

- No nonzero borrowing or accrued-interest state was available. The implementation detects and
  rejects that state; its economic mapping is deliberately unsupported rather than inferred from
  zero-liability samples.
- The account was not entirely flat because an existing GWEIUSDT position remained untouched.
  BTCUSDT nonzero-to-zero transitions and explicit flat rows now have live evidence, but an
  entirely flat account does not.
- No real algo parent-trigger-child lifecycle was present. Parent/child/fill correlation and
  contradictions have offline fixture coverage only.
- No venue retention boundary was reached. Returned bounded history does not prove full retention
  or completeness.
- No 429 or 418 response was manufactured. Gate latching is covered offline, while live error
  headers remain unavailable through SDK 69.2.1 and automatic recovery stays disabled.
These are evidence limits, not permission to broaden the supported account state. The unavailable
live cases remain documented limitations with conservative rejection or incompleteness semantics,
as applicable.

### Current implementation acceptance attempt

On 2026-09-15, the current acceptance script was run with an owner-only local credential file and
one preloaded UM instrument. The account refresh exhausted its sixty-second operation budget. All
nine source slots remained without a response and were marked failed; the order-quota and mass
status reads were therefore not attempted. Independent unsigned connectivity probes to the PAPI,
UM futures, and public API hosts also timed out while opening the TCP connection, before TLS or an
HTTP response.

This attempt provides no authentication result, endpoint-scope result, quota headers, account-state
evidence, or projection acceptance. It is a network-path limitation of the collection environment,
not evidence that the credentials, account, or implementation were rejected.

The collection was repeated through the user-provided local mixed proxy after an unsigned PAPI ping
returned HTTP 200 over the same route. All nine signed account sources then returned HTTP 401 with
Binance code `-2015`; the refresh failed closed, and the script did not attempt the order-quota or
mass-status reads. This establishes proxy connectivity and negative authentication/permission
handling, but `-2015` does not distinguish an invalid key from a disallowed proxy egress IP or
missing PAPI permission. No account response or quota header was returned.

A second owner-provided key was checked through the same proxy using signed GET requests only. The
wallet API permission query returned HTTP 200 and reported unrestricted IP access and reading
enabled, while futures and Portfolio Margin trading permissions were disabled. A Spot account
`USER_DATA` read succeeded with HTTP 200, but the UM and PAPI account reads both returned HTTP 401
with code `-2015`. This establishes that the second key is valid and not IP-restricted, and that the
general reading permission alone does not grant access to those derivatives account routes. It does
not establish whether Binance can grant PAPI `USER_DATA` access independently of trading authority.
No product trading permission was enabled and no write request was attempted.

The collection was then repeated with a PAPI-readable key and the account's active
`GWEIUSDT-PERP.BINANCE` scope. All nine account sources, the order quota, and bounded mass status
succeeded. All account sources were recent and had no failure, and both the wallet and PM risk
views were available without issues. The wallet contained four exact native totals-only balances;
no free or locked amount was synthesized. The mass status contained the one active position, no
orders or fills in its one-hour window, and retained `reports_complete=false` with its historical
coverage issue.

On 2026-09-19, the current build reproduced this acceptance through the explicitly configured
local proxy. Public UM metadata and private PAPI requests used the same proxy, but the public
metadata request received no PAPI credentials. All nine account sources, the order quota, and
bounded mass status succeeded again. The wallet and PM risk views were available without issues;
mass status reported one position and no orders or fills in the one-hour window, while retaining
`reports_complete=false`. The evidence reported `trading_authorized=false`, every authenticated
operation was a GET, and no account setting or trading command was invoked.

The first current-build run exposed an additional asset code, `U`, which correctly made the wallet
unsupported while the currency was unknown. Binance's read-only asset configuration identifies it
as United Stables, and public `UUSDT` and `BTCU` metadata assign eight-digit asset and commission
precision. After registering that currency with eight-digit precision, the exact wallet projection
succeeded. Observed account-wide request-weight increments were 40 for UM ordinary orders, 40 for
UM algo orders, 40 for CM orders, and 5 for margin orders, matching the configured reservations.

No credential, amount, or raw private response was added to the repository. The raw evidence
remains in an owner-only temporary directory. Every authenticated operation was a GET; no product
trading permission was enabled and no write request was attempted. This accepts the supported
read-only projection for the sampled active, zero-liability account. Nonzero liability, flat-account
transition, real algo lifecycle, retention-boundary, and live throttling evidence remain unavailable
and retain their fail-closed or incomplete behavior.

## PnL, fees, and funding

### Unrealized PnL comes from live prices

For a linear instrument, the calculation is:

```text
unrealized_pnl = signed_quantity * contract_multiplier * (mark_price - entry_price)
```

Use validated instrument metadata and the configured live price source. Comparing with Binance
valuation requires mark prices; a bid/ask or last-trade valuation can differ. The current
`positionRisk` response agrees exactly with this formula using its own fields. This is a
diagnostic result, not a requirement to fetch or match REST PnL during normal operation.

The sampled `breakEvenPrice` differs from `entryPrice`; the position report correctly uses
`entryPrice`. Keep the synchronized cost basis separate from fees and funding already in wallet
balances.

Sequential REST responses naturally have different prices, PnL, and margin valuations. Even the
two V2 responses have identical quantities and position `updateTime` while their PnL changes.
A position's last-change timestamp is neither the mark-price timestamp nor proof that an old,
unchanged position is stale. Validate position synchronization and market-data freshness
separately. Missing or differing optional REST PnL must not fail otherwise valid account startup.

[Portfolio::equity](../../portfolio/src/portfolio.rs) adds local unrealized PnL to margin-account
wallet totals. Publish the wallet basis once and keep venue equity out of that total.
`calculate_account_state=false` prevents local wallet recomputation but does not disable the
portfolio equity formula.

### Real fills and their cash flows

The targeted historical collection returns:

| Evidence                    | Count   | Check                                                                                                  |
| --------------------------- | ------- | ------------------------------------------------------------------------------------------------------ |
| Ordinary orders             | 41      | 39 filled and 2 canceled; all are limit orders.                                                        |
| Fills                       | 46      | Every fill links to one returned order with matching symbol, side, position side, and lifecycle times. |
| Maker/taker fills           | 22 / 24 | Both classifications are explicit in the response.                                                     |
| Commission income           | 46      | Each fill has exactly one matching income row, in the same fee currency, with `income = -commission`.  |
| Nonzero realized PnL income | 19      | Each matches one fill's `realizedPnl` and settlement currency exactly.                                 |
| Funding income              | 44      | Signed funding flows in the same historical window, with no trade identity.                            |
| Algo history                | 0       | No real parent/child lifecycle is covered by this sample.                                              |

For all 41 orders, the sum of the returned fill quantities equals `executedQty`. Order/fill
identities and `(incomeType, tranId)` identities are unique within their respective responses.
All 46 commissions are positive charges in the settlement currency. Rebates and fees paid in
another currency remain offline-test coverage, not authenticated evidence from this sample.

The separate recent seven-day income query returns 42 nonzero `FUNDING_FEE` entries, all negative.
Two entries fall strictly between each earlier balance receipt and the new balance request.
Across all four assets, the wallet changes match their signed sum exactly:

```text
new_totalWalletBalance - old_totalWalletBalance = sum(intervening_income)
new_umWalletBalance - old_umWalletBalance       = sum(intervening_income)
```

Exactly one asset has a nonzero change. This provides direct sample evidence that those funding
charges have already entered the reported wallet. Historical funding, realized PnL, and commission
records support reconciliation and audit; replaying them as fresh wallet adjustments after an
authoritative balance snapshot would count them again. The historical fill window has no matching
before/after wallet captures, so it proves fee/PnL linkage, not a full wallet-ledger reconstruction.

The [official income contract][income-api] states three-month retention. Successfully returning
the targeted older records does not establish full order/fill/algo retention, deleted-order
coverage, all-symbol coverage, or completeness of the account's history.

## Native balances and shared PM capacity

`crossMarginFree + crossMarginLocked` equals `crossMarginAsset` in the sampled no-borrowing state,
but differs from `totalWalletBalance` for two of the four assets. Copying those components into
`AccountBalance` alongside the total wallet would violate `total == free + locked`. Deriving or
clamping a component to force the identity would not establish its economic meaning.

The PM summaries also distinguish collateral-adjusted `accountEquity` from `actualEquity`. In all
four observed summaries:

```text
totalAvailableBalance = accountEquity - accountInitialMargin
```

`totalMarginOpenLoss` is zero and available balance equals withdrawal capacity in these samples.
These equalities do not prove behavior with working orders or make withdrawal capacity
interchangeable with trading capacity.

The current account reference explicitly identifies equity, actual equity, maintenance margin,
withdrawal capacity, and margin open loss as USD values. Its descriptions of
`accountInitialMargin` and `totalAvailableBalance` still omit explicit units. Their numerical
relationship supports a USD interpretation but does not complete the capacity contract.

Every sampled nonzero PM monetary summary field has meaningful digits beyond two decimal places.
[USD currency precision](../../model/src/currencies.rs) is two even with `high-precision` enabled.
[Money::from_decimal](../../model/src/types/money.rs) can round excess digits. A successful
constructor is insufficient: require an exact round trip or an explicitly accepted projection
policy. Preserve the original decimal observations; do not relabel USD as USDT.

The subsequent implementation resolves the model decision as follows:

- Existing observation types can retain native assets, liabilities, and PM measurements without a
  core extension. Negative wallet totals are already supported by `AccountBalance`.
- `AccountState.total_only_balances` and `MarginAccount` represent known native totals while keeping
  free/locked unavailable. The representation is rejected for locally calculated account state.
- PM capacity belongs to one account budget across currencies, instruments, and strategies.
  Duplicating it into USDT and USDC free balances would duplicate collateral.
- The [risk engine](../../risk/src/engine/mod.rs) rejects risk-increasing orders for totals-only
  margin accounts unless an order is explicitly reduce-only or a validated full-position exit.
  This is a guard, not PM admission or delegated buying power.
- [MarginModel](../../model/src/accounts/margin_model.rs) receives instrument, quantity, price,
  and leverage; it has no account/order/reservation context. Before trading, design the minimum
  capability needed for a shared PM check and reservation lifecycle instead of forcing capacity
  into that existing per-instrument calculation.

## Failure handling, coverage, and startup

The current account remains in one-way mode with one nonzero position and no ordinary or algo open
orders. V2 and `positionRisk` agree on nonzero identity and signed quantity. The earlier V2 report
remains the evidence for omission of zero rows; this collection does not exercise an entirely flat
account, orders-only state, multiple current positions, or a closing transition.

Existing adapter and engine tests passed for these failure behaviors:

- Failed or dropped refreshes retain the last successful observations and mark affected sources
  failed. Other sources can update independently. Receipt age uses a monotonic clock, including
  boundary, out-of-order, and clock-rollback cases. A recent receipt alone is not economic validity.
- Unknown statuses and absent/null/empty/invalid fields remain distinguishable. Risk observations
  can change independently of balance observations.
- The totals-only wallet projection requires exact registered currencies, preserves positive,
  negative, and explicit zero totals, and rejects missing/null/invalid fields, excess precision,
  nonzero debt, unsupported product scope, mixed generations, stale receipts, and excessive
  collection span without returning a partial account.
- Missing, malformed, or duplicate position coverage cannot synthesize a flat position. An
  explicitly reported zero position is accepted and closes a cached nonzero position through the
  execution/portfolio event path exactly once. Failed current reads do not become empty success.
- Partial or exhausted history stays explicitly incomplete. Pagination, saturated milliseconds,
  time-window boundaries, and request/row budgets have regression coverage.
- Missing or invalid commission is fatal. Through the execution manager, incomplete history keeps
  explicit fees without applying historical fills to positions or portfolio balances. Periodic
  and targeted read failures preserve cached orders and positions.
- Algo parent, child, and fill fixtures form one lifecycle; contradictory links fail. Timeout,
  cancellation, redirect, authentication, malformed-response, and 429/418 behavior is covered
  offline. No live faults or real algo triggers were manufactured.

[BinancePapiExecutionClient::connect](src/execution.rs) now supports default-off LiveNode startup,
account publication, ordinary UM commands, PM admission and bounded stream recovery. The live
command acceptance and remaining stream limitation are recorded below. Current mass statuses have
`reports_complete=false`; the core suppresses historical position/portfolio effects, but that flag
is not itself a universal LiveNode startup failure.

Later trading startup acceptance must establish:

1. Authenticated acceptance of the totals-only native wallet projection across its required
   account-wide sources and supported zero-liability scope.
1. A single PM admission budget with verified capacity and incremental-margin semantics, covering
   concurrent orders and modifications, reservations, fills, rejections, cancellations, and
   recovery of unknown submission outcomes.
1. Valid required sources, account mode, instrument coverage, bounded receipt age and collection
   skew, plus synchronized positions and the live prices needed by enabled valuation/risk checks.
   Optional REST unrealized PnL and cross-response PnL equality are excluded from this gate.
1. Accepted current-order/position coverage, including flat and closing states, and an explicit
   bootstrap policy for incomplete history. No missing position, fee, or cost basis may be invented.
1. Tested execution and stream synchronization, reconnect recovery, and rejection of risk-increasing
   orders when required risk data is missing, stale, inconsistent, or in an unsupported status.

## Offline validation

All 400 selected existing tests passed against the current checkout with Rust 1.98.1,
`ci-pr-wheel`, and high precision:

| Test selection                               | Passed | Purpose                                                                                                      |
| -------------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------ |
| `nautilus-binance-papi --lib`                | 244    | HTTP, observations, report parsing, read-only collection, execution-manager integration, and startup guards. |
| `nautilus-model --lib types::balance::tests` | 71     | Balance invariants, negative totals, currencies, bounds, and account-wide margins.                           |
| `nautilus-model --lib types::money::tests`   | 85     | Exact amount handling, currency precision, rounding, and bounds.                                             |

Tests ran through `scripts/strip-adapter-env.bash`, with the manual acceptance credential variable
also removed from the test environment. Cargo used `--offline --locked` and compiled this
checkout; the evidence is not execution of an old test binary. Private financial analysis used
the project's `python/.venv` interpreter.

These counts record the 2026-09-13 evidence checkout, not the subsequent implementation's final
validation. They do not validate PM admission capability or live recovery design.

## Issue #5 offline command validation

On 2026-09-19, the ordinary UM command lifecycle was validated entirely against loopback mock
venues. No production trading request was sent. The engine-wired tests exercise the real
RiskEngine and ExecutionEngine path through the PAPI client: current provenance-tagged synthetic
risk evidence permits exactly one signed POST and applies the resulting native acceptance, while
missing evidence and unsupported terms produce a denial with zero write requests. Explicit venue
rejection becomes a native rejection; a response timeout after the durable dispatch barrier leaves
the order submitted and the journal operation unknown without retrying or inventing a rejection.

The completed local selections were:

- `nautilus-binance-papi --lib`: 419 passed, covering command transport, coordinator, journal,
  recovery, private-stream application, and the engine-wired order lifecycle.
- `nautilus-binance-papi --features python --lib`: 420 passed, covering the same behavior with
  Python bindings enabled.
- `nautilus-execution --lib`: 923 passed, covering core reconciliation, post-application callback
  ordering, and exact client routing.
- `nautilus-risk --tests`: 1,132 passed and 2 ignored, covering native-capital delegation,
  totals-only fail-closed behavior, ordinary risk checks, and existing risk regressions.
- Python PAPI unit tests: 21 passed, covering Python configuration, factory wiring, Decimal limits,
  and default-off behavior.

Relevant Rust clippy selections passed with warnings denied, generated Python stubs were refreshed,
and repository formatting completed. On macOS, the Python-feature Rust test required the installed
Python 3.14 library path and allowing the linker's compact-unwind size warning; all 420 tests then
ran and passed.

Production increase-risk admission remains default-off and fail-closed unless authenticated
observations construct and install a current `PapiVerifiedRiskSnapshot`. The 2026-09-20 acceptance
below exercised that path for one explicitly bounded BTCUSDT scope. Other account states,
instruments and risk-rebaseline behavior remain outside that live evidence.

### Issue #5 acceptance continuation

The 2026-09-20 follow-up first audited the failure matrix against the tests and the owner-only
2026-09-19 evidence bundle. A later explicitly authorized run then loaded current public BTCUSDT
metadata and authenticated account-wide observations through the configured proxy. It limited
trading to `BTCUSDT-PERP.BINANCE`, `0.001 BTC` per order, `0.001 BTC` maximum position, and
`200 USDT` maximum order, instrument and account exposure. The pre-existing
`GWEIUSDT-PERP.BINANCE` long position remained in the read-only report scope and was never included
in the trading allowlist or modified.

The private values also reconfirm the previously recorded wallet boundary: borrowing and accrued
interest are zero, while one negative wallet row has `negativeBalance = totalWalletBalance =
umWalletBalance < 0`. The deficit is already included in the total and must not be deducted again.
This remains evidence only for that sampled state, not for nonzero borrowing or interest.

The installed risk snapshot required `NORMAL` account status, one-way position mode, a current
BTCUSDT mark price, exchange filters, authenticated leverage brackets and an authenticated USDT
asset index. It converted the reported USD available-margin value with the observed asset index,
using `max(index, 1)` as the conservative conversion denominator, and derived the maximum initial
margin rate from every valid bracket rather than a configured leverage constant. The live startup
collected and validated these inputs before the first increase-risk command was admitted.

The bounded live command results were:

| Action                         | Venue result                                                                         | Final account check                 |
| ------------------------------ | ------------------------------------------------------------------------------------ | ----------------------------------- |
| MARKET buy                     | Filled `0.001` at `80408.20`; taker fee `0.04020410 USDT`                            | Long `0.001`, no open BTCUSDT order |
| Reduce-only MARKET sell        | Filled `0.001` at `80241.40`; taker fee `0.04012070 USDT`                            | BTCUSDT flat, no open order         |
| LIMIT GTC plus targeted cancel | Accepted and canceled by exact owned order identity, with zero fill                  | BTCUSDT flat, no open order         |
| LIMIT IOC                      | REST reported `EXPIRED` with `filled_qty=0.000`                                      | BTCUSDT flat, no open order         |
| Unmarketable LIMIT FOK         | Explicit Binance `-5021` rejection because the order could not be filled immediately | BTCUSDT flat, no open order         |
| Marketable LIMIT FOK           | Filled `0.001` at `80411.60`; taker fee `0.04020580 USDT`                            | Long `0.001`, no open order         |
| FOK cleanup, reduce-only sell  | Filled `0.001` at `80432.80`; taker fee `0.04021640 USDT`                            | BTCUSDT flat, no open order         |
| GTX post-only plus cancel      | REST retained `post_only=true`; targeted cancel completed with zero fill             | BTCUSDT flat, no open order         |
| Private-stream MARKET retest   | Filled `0.001` at `80465.20`; taker fee `0.04023260 USDT`                            | Long `0.001`, no open BTCUSDT order |
| Retest reduce-only close       | Filled `0.001` at `80420.50`; taker fee `0.04021025 USDT`                            | BTCUSDT flat, no open order         |

The final independent authenticated read reported no BTCUSDT open order and an explicit flat
`0.000` BTCUSDT position. It also reported the original GWEIUSDT long quantity unchanged at `24`.
The owner-only evidence and command journals were created with mode `0600`; no credentials,
balances or raw private responses are stored in the repository.

The initial MARKET open and close, IOC, and the second reduce-only close emitted submitted and
accepted events but no prompt filled or expired event. REST mass status later proved every
terminal result. The marketable FOK fill was delivered only after a WebSocket Pong timeout,
reconnect and recovery, while GTC and GTX cancellations did deliver their canceled events.

Follow-up analysis found two private-stream causes. The official PAPI `ORDER_TRADE_UPDATE` model
does not include the USD-M `ot` field that the decoder had required. Also, an `ACCOUNT_UPDATE`
could start REST recovery before the immediately following order update was consumed. The decoder
now accepts an absent `ot` while still rejecting a conflicting value, and ordinary account updates
schedule a separate coalesced current-state risk refresh while the stream driver continues
delivering order deltas. A rebuilt-client retest delivered both MARKET fills about 160 ms after
submission. The later optimization was validated offline and has not been exercised by a new live
order.
The retest also exposed and fixed the race where that authoritative stream report could supersede
the still-returning HTTP response; a matching late response no longer repeats the durable journal
transition or emits a duplicate acceptance. Direct IOC expiration, partial fills and multi-fill
ordering remain unaccepted. One public WebSocket startup timeout and one malformed account response
failed before dispatch and left the account state unchanged.

The matrix audit has these results:

| Area                                          | Result                                | Current evidence                                                                                                                                                                                                                                                                                                                                    |
| --------------------------------------------- | ------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Parameter, precision and capability rejection | Covered offline                       | Typed command and engine-wired tests preserve Decimal text, reject unsupported terms and assert zero writes before dispatch.                                                                                                                                                                                                                        |
| Write outcome classification                  | Covered offline and bounded live      | Live accepted, filled, expired, canceled and explicit `-5021` rejection outcomes complement offline timeout, response-loss, decode, 503 and throttle cases.                                                                                                                                                                                         |
| Durable intent and restart stages             | Covered offline                       | Journal corruption and truncation fail closed; reopening `Prepared`, `MayHaveDispatched`, `Unknown` and `Observed` retains restriction, while `Resolved` does not.                                                                                                                                                                                  |
| Concurrent admission and dispatch freeze      | Partially covered offline             | Atomic reservations, in-flight limits and the final generation recheck are covered; a deterministic multi-task schedule matrix is still absent.                                                                                                                                                                                                     |
| Response/fact ordering and cancellation races | Partial live and offline coverage     | Targeted GTC/GTX cancellation completed live. MARKET fills arrive directly from the stream while account updates schedule an independent refresh; matching late HTTP responses are suppressed offline. Direct IOC expiration and the partial-fill cancellation matrix remain incomplete.                                                            |
| Execution application and accounting          | Partial live and offline coverage     | Live full fills, fees, open and reduce-only close were applied exactly, including prompt private-stream MARKET fills. Partial and multi-fill orders and other fee currencies remain incomplete.                                                                                                                                                     |
| Recovery scope and identity                   | Covered offline for implemented cases | Not-found ambiguity, bounded targeted recovery, old active orders, algo parent/child linkage, same-millisecond trade IDs and incomplete history are explicit. Venue retention remains live-unverified.                                                                                                                                              |
| Risk refresh and rebaseline                   | Startup live; dynamic path offline    | Authenticated startup installed current BTCUSDT risk evidence. Owned-order updates retain the prior reservation and coalesce a 3-5 second refresh without freezing admission; unknown changes freeze immediately. Refreshes avoid history scans and install only after current-state application. Sustained venue-risk lag remains live-unaccepted. |
| Restricted states and quota failure           | Covered offline                       | Missing/stale/inconsistent evidence, unsupported scope, failed sources, budget exhaustion and process-wide 429/418 latching fail closed. Live throttling is intentionally untested.                                                                                                                                                                 |
| Native core and Python boundaries             | Covered offline                       | Exact account/client routing, totals-only fallback, ordinary account regressions, explicit Python config, default-off behavior and credential redaction are tested.                                                                                                                                                                                 |

The follow-up adds regression coverage for non-advancing or source-regressing risk replacements,
every durable operation stage across coordinator reopen, official PAPI order payloads without
`ot`, account-before-order stream sequencing, stream/HTTP response races, and coalesced dynamic
risk refresh without historical reads. The remaining acceptance work centers on a live dynamic
rebaseline, partial and multi-fill orders, direct IOC expiration, unsupported account states,
venue retention and live throttling. Real commands
continue to require explicit trading configuration and bounded risk evidence; configuration
remains default-off.

[account-api]: https://developers.binance.com/en/docs/catalog/advanced-trading-derivatives-trading-portfolio-margin/api/rest-api/account
[income-api]: https://developers.binance.com/en/docs/catalog/advanced-trading-derivatives-trading-portfolio-margin/api/rest-api/account#get-um-income-history
