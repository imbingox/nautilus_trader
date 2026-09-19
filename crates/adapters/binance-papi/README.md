# nautilus-binance-papi

Binance Portfolio Margin (PAPI) adapter for NautilusTrader, with Rust and Python private account
observation, read-only queries, and an explicit ordinary UM command lifecycle.
The client collects exact account observations and
ordinary/algo order, fill, and one-way UM position reports through signed GET requests.
The read-only account projection is implemented and has authenticated supported-account acceptance.
The documented scope limits and trading gate remain in place.

The execution factory supports configuration, factory extraction and `LiveNode` construction.
Factory-created Rust clients can generate scoped reports after `start()` when configured with
`read_only` and explicit, preloaded instrument IDs. Standalone historical order/fill vectors
remain unavailable because their interface cannot express incomplete coverage. The factory's
mass status defaults to a sixty-minute lookback and preserves the configured client identity.
Position reports support current observations only; historical position filters fail explicitly.
`QueryOrder` runs as a managed GET-only task and emits one authoritative `OrderStatusReport` when
the venue response can be verified; failures and not-found responses never become absence evidence.

With explicit credentials and preloaded instrument scope, the execution client starts a private
account stream before collecting its REST baseline. It publishes the reported totals-only account
state, delivers deduplicated ordinary order/fill updates as typed execution reports, and uses
bounded mass status recovery for account changes and later transport gaps. Connection,
synchronization, delivery, application, and trading authorization remain separate. Trading is
default-off. With explicit trading configuration, supported commands use durable admission and
single-dispatch transport, but increase-risk submission remains fail-closed until authenticated PM
risk evidence is installed. Construction performs no network requests. Cleanup is idempotent,
bounded, and a later start uses a new cancellation domain.

The independent factory name and default client ID are `BINANCE_PAPI`. Instrument venue
remains `BINANCE`. The default account ID is `BINANCE-PAPI-001`, keeping its issuer
aligned with the venue used by the core account cache. Use `BinanceDataClientFactory`, `BinanceDataClientConfig` and
`load_binance_instruments` from `nautilus_trader.adapters.binance` for existing public
market data and instrument loading. No Binance code is copied or reconfigured by this adapter.

## Trading command contract

The durable operation ledger, account-level admission coordinator, quota-to-dispatch durability
barrier, typed ordinary UM write transport, and execution-engine application acknowledgment are
connected. Constructing a `BinancePapiTradingConfig` enables only this command machinery; it does
not make the client trading-ready or synthesize admission evidence. Production increase-risk
submission remains fail-closed until authenticated PM semantics produce a current
`PapiVerifiedRiskSnapshot`. Offline tests use provenance-tagged synthetic evidence.

The first command scope is fixed as follows. `QueryOrder` is connected through the read-only report
path. State-changing commands use the same coordinator for RiskEngine admission and the final
adapter check, then emit native lifecycle events from confirmed outcomes.

| Nautilus command    | PAPI operation                         | Required wire identity and parameters                                                                                                                                                                      | Required native result                                                                                                                                  |
| ------------------- | -------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SubmitOrder`       | `POST /papi/v1/um/order`               | Explicit allowlisted symbol, side, `MARKET` or `LIMIT`, exact base quantity, client order ID, and `reduceOnly`; LIMIT additionally requires exact price and GTC/IOC/FOK, with post-only mapped only to GTX | Local denial before dispatch, or native submitted/accepted/rejected/order facts according to confirmed evidence; an ambiguous result remains unresolved |
| `QueryOrder`        | `GET /papi/v1/um/order`                | Exactly one scoped symbol plus venue order ID or original client order ID                                                                                                                                  | One `OrderStatusReport`, or a distinct query failure/not-found result                                                                                   |
| `CancelOrder`       | `DELETE /papi/v1/um/order`             | Scoped symbol plus venue order ID or original client order ID                                                                                                                                              | Native cancel/final-order fact only when confirmed; an ambiguous result remains unresolved                                                              |
| `BatchCancelOrders` | Bounded per-order DELETE orchestration | A fixed, validated set of owned ordinary UM orders                                                                                                                                                         | One result per target; no atomic-success claim                                                                                                          |
| `CancelAllOrders`   | Bounded per-order DELETE orchestration | A fixed snapshot honoring the command's account, instrument, and side filters                                                                                                                              | One result per target; no wider venue cancel-all request                                                                                                |

`SubmitOrderList`, modify and cancel-replace commands, conditional/algo creation, OCO/OTO,
trailing, GTD, `closePosition`, quote-quantity, hedge-mode, CM/margin trading, transfers, borrowing,
leverage changes, and account-mode changes are outside this scope. Unsupported commands or field
combinations must fail before a trading HTTP request is made; fields must never be ignored or
silently downgraded. Observed algo orders remain part of reconciliation but are not tradable.

### Trading configuration boundary

`BinancePapiExecutionClientConfig.trading` defaults to `None`. An explicit trading configuration is
accepted only when all of these static conditions hold:

- Every trading instrument has unique positive Decimal limits for one order, absolute position,
  order notional, and total instrument exposure. The trading allowlist must be a subset of the
  complete report scope.
- All configured notionals share one explicit `risk_currency`. An instrument's actual settlement
  asset and every conversion input must later be verified against this unit. Instruments settled in
  another asset cannot inherit this authorization.
- Account exposure, in-flight operations, risk age and collection span, recovery requests and
  rounds, recheck interval, adverse market-price buffer, and fee buffer are finite and positive.
- The command journal uses an explicit absolute file path with an existing parent directory. Client
  startup opens an account-bound, checksummed append-only journal under an exclusive process lock;
  corrupt state, an account mismatch, or an uncertain durability result fails closed. The
  coordinator reconstructs exact quantity, notional, exposure, and initial-margin reservations;
  unresolved restart state blocks new increase-risk admission.
- Read-only credentials and the shared request budget are present. Recovery cannot configure more
  requests than that shared budget permits.

Decimal limits serialize as strings and Python accepts `decimal.Decimal`; no amount passes through
`f64`. These are independent hard ceilings. Reconciliation or installation of a new risk baseline
must not reset position, exposure, or in-flight limits.

### PM risk semantics required before admission

The existing PM risk observation being `available` means only that its source was readable. It does
not establish a trading unit, incremental-margin formula, or buying power. Admission remains closed
until authenticated evidence verifies the account status and one-way mode, the selected risk
currency, each required source field and unit, bracket/rule inputs, market-price source, source
generation, receipt age, and collection span.

The admission coordinator calculates its conservative order estimate from exact base quantity and
a fresh adverse buffered reference price, then adds the configured fee reserve. It accounts for
current position, same-direction open and unresolved operations, and a provenance-tagged PM
incremental-margin rule. Opposite open orders do not offset this exposure. Missing or stale prices,
rules, conversions, units, source fields, or unsupported nonzero product exposure close
increase-risk admission. A market estimate is a risk bound, not an execution-price guarantee.

The meaning and units of `accountInitialMargin`, `totalAvailableBalance`, and related PM fields have
not yet been accepted as sufficient admission evidence. No fallback uses withdrawal capacity,
totals-only wallet `free`, `accountEquity - accountMaintMargin`, or an arbitrary leverage divisor.
Until the required semantics are verified and mapped into the coordinator, configured trading
permission, current increase-risk permission, targeted cancellation permission, and safe
reduce-only permission all remain separate states; none can be inferred from `is_connected` or
`is_synchronized`.

The core risk engine has a generic totals-only native-capital delegation point bound to the exact
account and cached execution-client route. It does not bypass instrument, price, quantity,
notional, trading-state, or rate checks, and an absent or mismatched provider still fails closed.
PAPI deliberately does not register a provider until the authenticated PM units and formulas above
can populate the verified risk snapshot; configuration alone cannot activate the delegation.

On restart, a trading-configured client holds the journal lock before becoming started. Its
coordinator keeps every unresolved reservation and runs only bounded targeted GET recovery during
connection. A matching authoritative report can advance an operation to observed or terminal.
Repeated `-2011`/`-2013`, query failures, or an exhausted recovery budget remain ambiguous, retain
the reservation, and keep increase-risk admission restricted. Recovery never replays an unknown
POST.

Offline command verification enforces the authenticated instrument trading state, settlement
currency, tick/step alignment, quantity, price and notional bounds before durable preparation.
Targeted cancellation and cancel-all planning accept only ordinary UM orders whose ownership is
established by the journal; a verified open-order row cannot adopt an external order. Cancel-all
freezes a sorted account/strategy/instrument/side target set, and batch cancellation dispatches the
prepared set one request at a time with an independent result per target. An empty target set is an
error and no venue-wide cancel-all endpoint is used.

Risk rebaseline is a generation-bound durable transition. It requires an explicitly applied fact
checkpoint, a newer complete risk snapshot, no possibly dispatched or pending cancel operation,
and exact open-order coverage for every observed submit before ending its reservations. Repeated
rebaseline cannot reset position, open-order, instrument-exposure, or account-exposure limits.

## Private-stream capability matrix

| Source or event                                                         | Observation behavior                                                                                                                                                |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| listen key POST/PUT/DELETE                                              | API-key authenticated lifecycle with empty-response support; no signed query or arbitrary write surface.                                                            |
| `ORDER_TRADE_UPDATE`                                                    | Validates ordinary type, side, TIF, quantity, price, reduce-only and one-way terms; emits one deduplicated typed order/fill delivery with signed native commission. |
| `ALGO_UPDATE`                                                           | Accepts the current `ao` UM one-way schema and retains parent/child identity; legacy conditional events are restricted.                                             |
| `ACCOUNT_UPDATE`                                                        | Treats positions as partial rows, never clears an omitted position, and marks wallet/risk/position sources dirty for REST confirmation.                             |
| Balance, liability, risk, and config notices                            | Invalidates the affected source and coalesces a bounded REST refresh; it does not synthesize PM wallet totals from UM deltas.                                       |
| Unknown critical event, unsupported product/mode, conflict, or overflow | Revokes synchronization and latches the session in `restricted` until an explicit new session generation.                                                           |

The first supported private scope is declared linear UM instruments in one-way mode. WebSocket
facts and REST history share bounded identity retention. REST remains authoritative for the PM
wallet baseline and independent risk evidence. A fixed overlap window recovers activity that
opened and closed during a gap; missing or contradictory coverage fails synchronization rather
than being inferred from empty current-order results.

Session evidence records separate `received_fact_version`, `delivered_fact_version`, and
`applied_fact_version` checkpoints. Queueing a recovery or incremental execution report advances
only delivery and leaves a generation-bound application checkpoint pending. Only an explicit
application acknowledgement can advance `applied`; stale, skipped, or repeated acknowledgements
fail. The execution client receives an engine callback after reconciliation, verifies the expected
account, order, fill, and position effects in the shared cache, and only then acknowledges the
pending session checkpoint. Queueing alone never advances `applied`.

## Feature flags

- `extension-module`: Builds Python bindings into an extension module.
- `high-precision` (default): Uses 128-bit fixed-point domain values.
- `python`: Enables Python query, observation-session, configuration, and factory bindings.

The encompassing `nautilus-pyo3` crate enables `papi` by default, so regular Cargo builds,
maturin wheel builds and development installs include PAPI bindings. Pass
`--no-default-features` to Cargo or maturin to disable PAPI. Both adapters use the same
`_libnautilus` extension and factory registry. REST reads pin `binance-sdk =69.2.1`, enabling
only its Portfolio Margin product feature and Rustls. HTTP explicitly selects Rustls, but the
stock SDK still brings native-TLS/OpenSSL dependencies through its transport dependency graph.

## Python installation verification

The first verification target is CPython 3.14 on Linux x86_64. This is not a promise of
support for other platforms. The provisional distribution name is `nautilus-trader-papi`;
the import name remains `nautilus_trader`. Install this full wheel in its own environment,
without the upstream `nautilus-trader` distribution, which owns the same import files.

From a development environment prepared according to `CONTRIBUTING.md`, install the Linux
`patchelf` build requirement declared in `python/pyproject.toml` before invoking maturin
directly. It bundles the native libraries required by the SDK transport:

```bash
make sync UV_SYNC_FLAGS="--python 3.14"
uv pip install --python python/.venv/bin/python --only-binary :all: patchelf
make py-stubs
cd python
uv run --no-sync maturin build --locked --profile ci-pr-wheel \
  --config profile.ci-pr-wheel.package.nautilus-model.codegen-units=16 \
  --interpreter .venv/bin/python --out ../dist/papi-py314 --strip -j 1
```

The command uses maturin's configured feature list and the default `papi` feature.
`ci-pr-wheel` reduces local compilation cost, and the model code-generation setting and single
build job reduce peak memory use. This is an installation smoke test, not a release build or
publication. `make py-stubs` generates PAPI stubs even when the runtime feature is disabled,
just as the package retains the optional facade.

Install and test from a neutral directory, without a source installation or adapter variables:

```bash
repo="$PWD/.."  # Run from python/ after building
wheel="$(realpath ../dist/papi-py314/*-cp314-cp314-*.whl)"
check_dir="$(mktemp -d)"
uv venv --python 3.14 "$check_dir/python/.venv"
uv pip install --python "$check_dir/python/.venv/bin/python" "$wheel"
cp "$repo/python/tests/integration/binance_papi_wheel_smoke.py" "$check_dir/wheel_smoke.py"
cd "$check_dir"
bash "$repo/scripts/strip-adapter-env.bash" \
  "$check_dir/python/.venv/bin/python" -I wheel_smoke.py enabled
```

The smoke test checks the installed package location, distribution metadata, shared core
types, the existing Binance factory, combined Binance data/PAPI node construction and
offline startup failure without credential leakage. It does not connect to Binance. See
`examples/live/binance_papi/build_node.py` for the minimal construction example.

Repeat the build with `--no-default-features`, using a separate output and clean environment,
and run the smoke test with `disabled` to verify the original Binance path and absence of
PAPI bindings. Inspect `cargo tree -p nautilus-pyo3 --no-default-features` for dependency
isolation; it must contain neither `nautilus-binance-papi` nor `binance-sdk`.

## Read-only queries

Use `read_only::BinancePapiReadOnlyConfig` with an explicit account ID, HMAC key and secret,
then construct `BinancePapiReadOnlyClient` with 1 to 256 preloaded Binance linear UM instruments.
Public metadata can be loaded through the existing Binance adapter without PAPI credentials.
No adapter environment variables are read. HTTPS origins and local HTTP loopback origins are
supported; redirects and implicit environment proxy configuration are disabled. An explicit
HTTP or HTTPS `proxy_url` applies to both REST and WebSocket transports.

Python exposes `BinancePapiReadOnlyConfig`, `BinancePapiReadOnlyClient`,
`BinancePapiReadOnlySnapshot`, and `BinancePapiAccountSession` through
`nautilus_trader.adapters.binance_papi`. Query and session lifecycle methods are awaitable.
Python timestamps and receipt-age bounds use integer nanoseconds and milliseconds, respectively.
Configuration representations redact credentials and URLs, and no plaintext credential getters
are exposed.

The standalone no-trading observation session uses the same native state machine as the execution
client:

```python
session = BinancePapiAccountSession(config, instruments)
await session.start()
evidence = json.loads(session.evidence_json())
assert evidence["trading_authorized"] is False
await session.stop()
```

The session owns one listen key and WebSocket URL at a time. It renews the key separately from
WebSocket Ping/Pong, rotates the transport before 24 hours, and replaces an expired key without an
old owner deleting the replacement. Unknown critical events, scope violations, conflicts,
overflow, and bounded recovery failure move the session to `restricted`.

The client exposes:

- `refresh_account_observations` and `account_observations_json` for retained exact account evidence.
- `account_snapshot_json(max_receipt_age, max_collection_span)` for an independent native wallet
  projection and PM risk view with explicit validity and source metadata.
- `query_order_rate_limit` for unprojected quota JSON.
- `generate_open_order_status_reports` for current ordinary and algo orders.
- `generate_position_status_reports` for explicit one-way position rows.
- `generate_order_status_report` for an exact venue identity or ordinary client order ID.
- `generate_mass_status(start, end)` for a fixed inclusive history window, current orders,
  positions, response metadata, and coverage issues.

All scoped symbols are scanned, including those with no current exposure. Ordinary venue IDs
use `PAPI:O:SYMBOL:ID`; algo IDs use `PAPI:A:SYMBOL:ID`. Known child orders and fills retain the algo
parent's lifecycle. Contradictory identities, duplicate client IDs, missing child evidence,
unsupported states, and inexact domain conversions fail the read. Fees preserve their sign and
native currency; missing commission never falls back to a fee estimate. Hedge mode,
`closePosition` algos, and trailing algos are unsupported.

Historical pages use subwindows shorter than seven days and bisect saturated intervals without
skipping a shared timestamp. Attempts, rows, and elapsed time have fixed operation budgets.
An exhausted budget fails the operation. Saturated single milliseconds, nonprogressing pages,
and failed optional history sources appear as explicit coverage issues. Required current reads,
identity/schema failures, and commission failures return errors. Standalone report vectors
never return a partial success, and a single-order not-found result remains an error.

Every mass status has `reports_complete=false`. Exhausting pages does not prove the venue's
retention, selection timestamps or algo discovery contract. The real execution-engine tests
verify that these incomplete snapshots preserve explicit fees while suppressing historical
position/portfolio effects. Separate local REST/WebSocket tests verify that a typed order/fill
delta is applied once through the real execution engine and portfolio without forcing a full REST
recovery, while a duplicate delta has no second effect. These tests do not establish readiness for
live reconciliation.

Snapshot `to_json()` preserves the fixed window, instrument scope, reports, response metadata,
and coverage issues. `cancel()` stops outstanding and future calls on that read-only client.
Account observations distinguish `missing`, `refreshing`, `recent`, `failed`, `canceled`, and
`stale` receipt states. A completed partial refresh retains each failed source's prior response
and redacted failure reason independently of successful sources. Dropping a refresh future or
calling `cancel()` invalidates the current refresh while retaining diagnostic values; it cannot
restore the receipt validity of a prior response.

`account_observations_json(max_receipt_age)` requires a positive receipt-age bound and computes
age at each read using a monotonic clock. Each source includes `timing.receipt_age_ns` and
`timing.collection_span_ns`. Collection spans include quota waits and retries from the logical
GET start to the SDK's body receipt. Wall-clock request metadata describes the successful signed
attempt, not the logical start. Monotonic instants are not persisted or restored through replay.
These timings do not verify economic freshness or cross-endpoint atomicity. The account snapshot
requires one generation and a bounded combined span for each view. Its wallet uses
`totalWalletBalance` as native-currency totals only after validating zero borrowing and interest,
currency registration, exact `Money` conversion, UM instrument scope, and the absence of CM and
cross-margin exposure or orders. Missing, failed, canceled, stale, inconsistent, and semantically
unsupported results remain distinct and never return a partial `AccountState`.

The PM account summary is a separate view. It preserves exact decimal strings, field availability,
known/unknown status, verified USD or ratio units, and explicitly unverified units. Wallet and PM
risk failures do not invalidate each other. Neither view grants trading authority.

## Acceptance collection

The third stage has a local collection entry point. A bounded authenticated GET collection on
2026-09-13 succeeded for the account sources, order quota, and mass status of an active one-way
position. Its one-hour history window returned no orders or fills, so that capture did not add
order/fill linkage or commission evidence and did not test retention. A separate collection scoped
to flat instruments failed the explicit `positionRisk` coverage requirement: UM account V1
supplied explicit zero rows while V2 omitted them. Those captures predate the totals-only projection
and do not constitute command acceptance. Raw captures and credentials stay outside the repository.

On 2026-09-15, the current projection and collection script completed against an active GWEI UM
position. All nine account observation sources, the order-rate-limit query, and the bounded mass
status succeeded. The wallet and PM risk views were available; the wallet contained four exact
native totals-only currencies and no synthesized free/locked values. Binance returned the newly
listed `U` asset, identified by its read-only asset configuration as United Stables and by public
spot metadata with eight-digit asset precision. The built-in currency registry now carries that
identity and precision. The active position produced one report; the one-hour window contained no
orders or fills and remains explicitly incomplete. Observed account-wide weight increments were 40
for UM ordinary orders, 40 for UM algo orders, 40 for CM orders, and 5 for margin orders. The
collection used authenticated GET requests only and did not authorize trading.

On 2026-09-19, the current build reproduced that result through an explicitly configured local
proxy. Public UM metadata used the same proxy without receiving PAPI credentials. All read-only
collections succeeded, the wallet and PM risk views remained available without issues, and mass
status returned one position with no orders or fills in its one-hour incomplete window. The
evidence retained `reports_complete=false` and `trading_authorized=false`; no write request or
trading command was issued.

Prepare a local JSON credential file with `account_id`, `api_key`, and `api_secret`. Optional
fields match the Python read-only config constructor, including `base_url` for a configured
gateway or loopback server, `proxy_url`, and integer request/operation timeout bounds in
milliseconds. When the script loads public Binance UM metadata, it also uses the configured proxy
without forwarding PAPI credentials. The file contains plaintext secrets and must stay outside
version control.

From an environment containing the newly built PAPI-enabled wheel:

```bash
python examples/live/binance_papi/read_only_acceptance.py \
  --credentials /path/to/papi-credentials.json \
  --instrument-id BTCUSDT-PERP.BINANCE \
  --instrument-id ETHUSDT-PERP.BINANCE \
  --lookback-minutes 60 \
  --output /path/to/papi-evidence.json
```

The script loads public Binance UM metadata without passing PAPI credentials to the public
adapter. Alternatively, pass `--instruments-file` with a JSON array of previously serialized
`CryptoPerpetual.to_dict()` or `CryptoFuture.to_dict()` values. The loaded metadata must match
every requested instrument exactly; a partial catalog fails before PAPI requests.

The output is a new, owner-only file containing **unsanitized private account data**. It preserves
original JSON as strings within the bundle so Python never rounds raw numeric tokens. It records
source failures and leaves later operations unattempted after a required collection fails. An
existing output file is never overwritten. A zero exit status means the selected GET collections
succeeded; every acceptance decision remains subject to review. Sanitize captures before sharing
them or adding fixtures.

Authenticated acceptance of the supported projection must establish its zero-liability scope,
account-wide CM and margin query behavior, V2 coverage, exact wallet output, and validity metadata.
Evidence that the existing account cannot provide is listed explicitly in
[ACCOUNT_VERIFICATION.md](ACCOUNT_VERIFICATION.md); unsupported debt and uncollected live algo,
retention, flat-account, and throttling cases must not be represented as live acceptance. Their
offline regressions remain distinct from authenticated evidence. PM capacity is not assigned to a
USDT/USDC wallet, and a zero balance is not substituted for an unavailable projection. Totals-only
balances deliberately leave native free/locked components unavailable.

## Request policy

SDK retries are disabled. Signed GET operations use Nautilus bounded retries for known transient
5xx statuses and adapter-owned timeouts; each retry reacquires endpoint weight before a fresh SDK
signature. Authentication, clock, decode and unknown SDK failures are not blindly retried.
Diagnostics retain typed status/code evidence without SDK error strings or signed URLs.

The ordinary UM write transport allowlists only signed POST and DELETE requests to
`/papi/v1/um/order`. It accepts validated market or limit submissions and exactly identified
cancellations, preserves quantity and price as Decimal text, and generates its signature only after
all quota waits. Every write operation has at most one dispatch and never enters the GET retry loop.
Cancellation or budget failure before dispatch is `CommandFailure::NotSent`; an explicit supported
venue rejection is `VenueRejected`; transport loss, timeout, in-flight cancellation, `-1000`,
`-1001`, `-1006`, `-1007`, rate limiting, 5xx, or response decoding after dispatch is `Ambiguous`.
Cancel not-found codes also remain ambiguous pending order recovery. SDK 69.2.1 erases 5xx response
bodies, so its different documented 503 messages cannot be distinguished safely.

All quota waits complete before the coordinator rechecks the evidence generation and send
permission. The journal then synchronously records `MayHaveDispatched` as the final fallible step
before the socket request. A failed barrier sends nothing; a valid response must return matching
symbol, venue order ID, and client order ID. Success is retained as observed until authoritative
terminal evidence, while not-sent and explicit rejection release the operation and ambiguous
outcomes keep its reservation.

All Rust client instances share a process-wide 3000-weight/minute gate, a burst of 40, and four
concurrent attempts. The write transport additionally applies a process-wide 1000 new-order/minute
gate with burst 20, below the documented account allowance of 1200/minute. Failed dispatched
submissions conservatively consume this local quota; cancellation does not consume a new-order slot
but still consumes the shared request gate. Defaults are five seconds per attempt, sixty seconds per
operation, 256 attempts and 100,000 decoded rows. Clones of one read-only client share cancellation
and retained observations. The execution session and its ad hoc query/report reader use separate
cancellation domains while sharing the process-wide gate, so stopping a query cannot prevent the
session from closing its listen key. Other processes sharing the IP or account require separate
quota coordination.

Account-wide UM ordinary/algo and CM open-order reads reserve their documented unscoped weight of
40. The margin open-orders documentation assigns IP weight 5 but separately states that an
unscoped request count equals the number of currently trading symbols. Authenticated acceptance
must compare its quota headers before this diagnostic collection is run repeatedly.

A 429, 418, or recognized throttling code permanently closes the shared gate for the process.
SDK 69.2.1 loses error response headers, so an unknown `Retry-After` cannot trigger automatic
recovery. A restart requires externally verified venue backoff. Successful response metadata
retains endpoint/symbol, request/receipt times, status and numeric quota headers. The SDK buffers
the complete body before exposing it: the adapter's 8 MiB bound limits parsing and retention,
not that SDK allocation.

## Exact account observations

The internal `observations` module parses balance, PM account summary, UM account V1/V2, UM current
ordinary/algo orders, CM positions/orders, and cross-margin current orders. It retains native asset identity,
separate liabilities/PnL/margin fields, exact decimal text, source/version/scope, account ID,
collection generation, receipt time, and the original JSON. Missing, null, empty, invalid and
valid fields remain distinct. Unknown status strings remain observations, not trading permission.

Each account/source slot retains its last parsed response after a failed refresh. Receipt age
uses monotonic time; a recently received response does not establish freshness of venue risk
data. Risk-only updates are retained independently of balance changes. A response is bounded
to 8 MiB, and invalid envelope shapes, duplicate identities, or scope mismatches fail the refresh.
Malformed scalar values remain explicitly invalid; a consumer must require and economically
validate every field it uses. PM purchasing power is not projected into a native balance.

The read-only client refreshes all nine sources within one generation and operation budget,
retaining successful sources independently after a partial failure. The SDK decodes into raw
JSON before the exact observation parser runs, preserving missing/null distinctions. There is no
native free/locked balance mapping. A successful wallet projection contains a formal reported
margin `AccountState` with `base_currency=None`, empty complete balances and margins, and exact
native `total_only_balances`; the private session publishes it only after bounded recovery
converges. The
[synthetic account fixtures](test_data/observations/README.md) and
[official report examples](test_data/reports/README.md) do not establish live compatibility.

Authenticated endpoint behavior for the sampled scope and supported wallet projection is accepted.
Remaining acceptance work covers unsupported account states and unobserved historical, algo,
retention, flat-account, and throttling behavior. Authenticated PM risk semantics and separately
authorized live MARKET/LIMIT/post-only/reduce-only/cancel/fill/rebaseline acceptance remain open;
the connected production increase-risk path stays closed until that evidence exists.
The rationale and acceptance obligations are recorded in [RESEARCH.md](RESEARCH.md).

## Local tests

```bash
bash scripts/strip-adapter-env.bash cargo test --locked -p nautilus-binance-papi --lib
bash scripts/strip-adapter-env.bash cargo test --locked -p nautilus-binance-papi --features python --lib
uv run --project python --no-sync pytest \
  python/tests/unit/adapters/binance_papi
make py-stubs
make format
make pre-commit
```

The Python boundary tests require the PAPI-enabled extension to be installed. A feature-disabled
wheel skips that module; the standalone wheel smoke test uses mandatory assertions instead.
