# nautilus-binance-papi

Binance Portfolio Margin (PAPI) adapter for NautilusTrader, with Rust and Python private account
observation and read-only queries.
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

With explicit credentials and preloaded instrument scope, the execution client starts a private
account stream before collecting its REST baseline. It publishes the reported totals-only account
state and uses bounded mass status recovery for later transport gaps. Connection, synchronization,
and trading authorization remain separate; this stage never authorizes trading, and every
submit/modify/cancel path rejects the command. Construction performs no network requests. Cleanup
is idempotent, bounded, and a later start uses a new cancellation domain.

The independent factory name and default client ID are `BINANCE_PAPI`. Instrument venue
remains `BINANCE`. The default account ID is `BINANCE-PAPI-001`, keeping its issuer
aligned with the venue used by the core account cache. Use `BinanceDataClientFactory`, `BinanceDataClientConfig` and
`load_binance_instruments` from `nautilus_trader.adapters.binance` for existing public
market data and instrument loading. No Binance code is copied or reconfigured by this adapter.

## Private-stream capability matrix

| Source or event                                                         | Observation behavior                                                                                                                             |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| listen key POST/PUT/DELETE                                              | API-key authenticated lifecycle with empty-response support; no signed query or arbitrary write surface.                                         |
| `ORDER_TRADE_UPDATE`                                                    | Retains order identity and cumulative state; a fill requires trade ID, quantity, price, signed native commission, currency, liquidity, and time. |
| `ALGO_UPDATE`                                                           | Accepts the current `ao` UM one-way schema and retains parent/child identity; legacy conditional events are restricted.                          |
| `ACCOUNT_UPDATE`                                                        | Treats positions as partial rows, never clears an omitted position, and marks wallet/risk/position sources dirty for REST confirmation.          |
| Balance, liability, risk, and config notices                            | Invalidates the affected source and coalesces a bounded REST refresh; it does not synthesize PM wallet totals from UM deltas.                    |
| Unknown critical event, unsupported product/mode, conflict, or overflow | Revokes synchronization and latches the session in `restricted` until an explicit new session generation.                                        |

The first supported private scope is declared linear UM instruments in one-way mode. WebSocket
facts and REST history share bounded identity retention. REST remains authoritative for the PM
wallet baseline and independent risk evidence. A fixed overlap window recovers activity that
opened and closed during a gap; missing or contradictory coverage fails synchronization rather
than being inferred from empty current-order results.

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
position/portfolio effects. They do not establish readiness for live reconciliation.

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
and do not constitute its live acceptance. LiveNode startup remains unavailable. Raw captures and
credentials stay outside the repository.

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

Prepare a local JSON credential file with `account_id`, `api_key`, and `api_secret`. Optional
fields match the Python read-only config constructor, including `base_url` for a configured
gateway or loopback server and integer request/operation timeout bounds in milliseconds.
The file contains plaintext secrets and must stay outside version control.

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

SDK retries are disabled. Nautilus bounded retries cover known transient 5xx statuses and
adapter-owned timeouts; each retry reacquires endpoint weight before a fresh SDK signature.
Authentication, clock, decode and unknown SDK failures are not blindly retried. Diagnostics
retain typed status/code evidence without SDK error strings or signed URLs.

All Rust client instances share a process-wide 3000-weight/minute gate, a burst of 40, and
four concurrent attempts. Defaults are five seconds per attempt, sixty seconds per operation,
256 attempts and 100,000 decoded rows. Clones share cancellation and retained observations;
other processes sharing the IP require separate quota coordination.

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

The remaining acceptance work must establish authenticated endpoint behavior for the complete
scope checks and supported wallet projection, plus unobserved historical/algo lifecycle evidence.
PM admission and trading authorization remain later work.
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
