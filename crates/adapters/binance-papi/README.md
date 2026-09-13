# nautilus-binance-papi

Binance Portfolio Margin (PAPI) adapter for NautilusTrader, with Rust and Python read-only queries.
The client collects exact account observations and
ordinary/algo order, fill, and one-way UM position reports through signed GET requests.
Authenticated acceptance and economic account projection remain pending.

The execution factory supports configuration, factory extraction and `LiveNode` construction.
Factory-created Rust clients can generate scoped reports after `start()` when configured with
`read_only` and explicit, preloaded instrument IDs. Standalone historical order/fill vectors
remain unavailable because their interface cannot express incomplete coverage. The factory's
mass status defaults to a sixty-minute lookback and preserves the configured client identity.
Position reports support current observations only; historical position filters fail explicitly.

`LiveNode.run()` still fails its connection readiness check: native `total/free/locked` balances
have no accepted economic mapping. Account publication and trading remain unavailable.
Construction and rejected connection attempts perform no network requests. Cleanup cancels
outstanding reads and remains idempotent; a later `start()` creates a new read session.

The independent factory name and default client ID are `BINANCE_PAPI`. Instrument venue
remains `BINANCE`. The default account ID is `BINANCE-PAPI-001`, keeping its issuer
aligned with the venue used by the core account cache. Use `BinanceDataClientFactory`, `BinanceDataClientConfig` and
`load_binance_instruments` from `nautilus_trader.adapters.binance` for existing public
market data and instrument loading. No Binance code is copied or reconfigured by this adapter.

## Feature flags

- `extension-module`: Builds Python bindings into an extension module.
- `high-precision` (default): Uses 128-bit fixed-point domain values.
- `python`: Enables Python read-only query, configuration, and factory bindings.

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
explicit startup failure. It does not connect to Binance. See
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
supported; redirects and implicit environment proxy configuration are disabled.

Python exposes the same `BinancePapiReadOnlyConfig`, `BinancePapiReadOnlyClient`, and
`BinancePapiReadOnlySnapshot` through `nautilus_trader.adapters.binance_papi`. Query methods are
awaitable and return the shared Nautilus domain types. Python timestamps and receipt-age bounds
use integer nanoseconds and milliseconds, respectively. Configuration representations redact
credentials and URLs, and no plaintext credential getters are exposed.

The client exposes:

- `refresh_account_observations` and `account_observations_json` for retained exact account evidence.
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
An interrupted account refresh marks unread sources failed while retaining prior observations.

## Acceptance collection

The third stage has a local collection entry point. A bounded authenticated GET collection on
2026-09-13 succeeded for the account sources, order quota, and mass status of an active one-way
position. Its one-hour history window returned no orders or fills, so ordinary/algo linkage,
commission behavior, and retention remain unverified. A separate collection scoped to flat
instruments failed the explicit `positionRisk` coverage requirement: UM account V1 supplied
explicit zero rows while V2 omitted them. Native balance projection and LiveNode startup remain
unavailable. Raw captures and credentials stay outside the repository.

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

Authenticated captures must establish liability inclusion, native free/locked semantics, V2
coverage, and ordinary/algo linkage. Quiet-account samples cannot establish debt, mixed collateral,
active algo transitions, or retention behavior. The acceptance cases in [RESEARCH.md](RESEARCH.md)
remain open until the corresponding evidence exists. PM capacity is not assigned to a USDT/USDC
wallet, and a zero balance is not substituted for an unavailable projection. If the native balance
split cannot be established, scope the required account-model capability separately before enabling
LiveNode startup.

## Request policy

SDK retries are disabled. Nautilus bounded retries cover known transient 5xx statuses and
adapter-owned timeouts; each retry reacquires endpoint weight before a fresh SDK signature.
Authentication, clock, decode and unknown SDK failures are not blindly retried. Diagnostics
retain typed status/code evidence without SDK error strings or signed URLs.

All Rust client instances share a process-wide 3000-weight/minute gate, a burst of 40, and
four concurrent attempts. Defaults are five seconds per attempt, sixty seconds per operation,
256 attempts and 100,000 decoded rows. Clones share cancellation and retained observations;
other processes sharing the IP require separate quota coordination.

A 429, 418, or recognized throttling code permanently closes the shared gate for the process.
SDK 69.2.1 loses error response headers, so an unknown `Retry-After` cannot trigger automatic
recovery. A restart requires externally verified venue backoff. Successful response metadata
retains endpoint/symbol, request/receipt times, status and numeric quota headers. The SDK buffers
the complete body before exposing it: the adapter's 8 MiB bound limits parsing and retention,
not that SDK allocation.

## Exact account observations

The internal `observations` module parses balance,
PM account summary, and UM account V1/V2 response parsing. It retains native asset identity,
separate liabilities/PnL/margin fields, exact decimal text, source/version/scope, account ID,
collection generation, receipt time, and the original JSON. Missing, null, empty, invalid and
valid fields remain distinct. Unknown status strings remain observations, not trading permission.

Each account/source slot retains its last parsed response after a failed refresh. Receipt age
uses monotonic time; a recently received response does not establish freshness of venue risk
data. Risk-only updates are retained independently of balance changes. A response is bounded
to 8 MiB, and invalid envelope shapes, duplicate identities, or scope mismatches fail the refresh.
Malformed scalar values remain explicitly invalid; a consumer must require and economically
validate every field it uses. Exact account balances and PM purchasing power are not projected.

The read-only client refreshes all four sources within one generation and operation budget,
retaining successful sources independently after a partial failure. The SDK decodes into raw
JSON before the exact observation parser runs, preserving missing/null distinctions. There is
no account-state publication or native free/locked balance mapping. The
[synthetic account fixtures](test_data/observations/README.md) and
[official report examples](test_data/reports/README.md) do not establish live compatibility.

The remaining acceptance work must establish authenticated endpoint behavior, historical
coverage and linkage, a correct PM balance/capacity mapping, and successful LiveNode bootstrap.
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
