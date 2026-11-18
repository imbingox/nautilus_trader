# Cairn PAPI

**Cairn PAPI is an independent third-party fork based on
[NautilusTrader](https://github.com/nautechsystems/nautilus_trader). It is not affiliated with,
endorsed by, or supported by Nautech Systems Pty Ltd or the NautilusTrader project.**

This is a temporary fork primarily for personal use, adding Binance Portfolio Margin (PAPI)
support. It is not intended to become a long-term independent distribution. The intended direction
is to migrate to upstream when its PAPI support meets these needs; this is not a claim that suitable
upstream support is available today, and there is no migration date commitment.

Source and fork-specific support:
[imbingox/nautilus_trader](https://github.com/imbingox/nautilus_trader).
Report issues to the [fork issue tracker](https://github.com/imbingox/nautilus_trader/issues).
Nautech Systems Pty Ltd remains the upstream author and copyright holder. Upstream copyright and
attribution notices are retained, and the code remains licensed under
[LGPL-3.0-only](https://github.com/imbingox/nautilus_trader/blob/main/LICENSE).

## Distribution and installation status

The distribution name is `cairn-papi`; the Python import remains `nautilus_trader`, including
its `_libnautilus` extension. Version `2.0.0rc16` is the first independently named release
target. Check [PyPI](https://pypi.org/project/cairn-papi/) for published artifacts before installing.
Earlier PAPI release history below belongs to the old `nautilus-trader-papi` distribution.

The intended release target remains CPython 3.14 with the GIL enabled on Linux x86_64,
with a `manylinux_2_34` baseline. No source distribution is planned. Other platforms and Python
versions are not supported release targets; local developer builds do not change that scope.

Once the exact release is available on PyPI, install it in a fresh environment with
`uv pip install --only-binary :all: cairn-papi==2.0.0rc17`. Do not interpret a version in this
source document as proof of publication.

Alternatively, install a locally built and verified wheel into a fresh
environment. From the directory containing that wheel:

```bash
uv venv --python 3.14 cairn-env
uv pip install --python cairn-env/bin/python --only-binary :all: ./cairn_papi-2.0.0rc17-cp314-cp314-manylinux_2_34_x86_64.whl
uv pip check --python cairn-env/bin/python
cairn-env/bin/python -c "import nautilus_trader; print(nautilus_trader.__version__)"
```

**Do not install `cairn-papi`, `nautilus-trader-papi`, and `nautilus-trader` together.** All three
provide the same import namespace and can overwrite each other's files. Use exactly one per
environment. A different distribution name does not satisfy dependencies on the old name.

## Migration from the old distribution

After a Cairn PAPI release has been published and verified, update downstream dependency names,
version pins, artifact hashes, and lockfiles. This includes the `nacre_trader` worker extra and any
transitive dependency that still requires `nautilus-trader-papi` or `nautilus-trader`.
Do not use a compatibility shim or permanent `--no-deps` installation to conceal conflicts.

Keep `import nautilus_trader` and existing API calls unchanged. Change distribution metadata
queries to `importlib.metadata.version("cairn-papi")`. Use a fresh environment and run `pip check`
or `uv pip check` after installing the downstream application. Follow the upgrade and rollback
procedure below before moving a live process. Renaming does not change PAPI interfaces, trading
logic, risk controls, or broker attribution.

A future return to upstream also requires a fresh environment, dependency updates, and explicit
API, behavior, and persisted-state compatibility checks. It is not an automatic package swap.

## Earlier PAPI release history

## Wallet assets in 2.0.0rc17

Wallet initialization and recovery support borrowing and interest when the exchange's wallet
components reconcile. Account events expose principal and interest by asset. Wallet-only assets
such as WBETH can be discovered from the balance response and restored from account events.
Apply the updated table definitions before using PostgreSQL persistence.

The following versioned notes describe the old distribution. Matching version numbers do not imply
that independently built artifacts are interchangeable.

## Standard account queries in 2.0.0rc16

Running strategies can call `self.query_account(account_id, client_id=BINANCE_PAPI_CLIENT_ID)`.
Successful current-round collection publishes a standard `AccountState` with exact PM equity and
margin ratio under `info["papi"]["portfolio_margin"]`. The summary also includes optional
`account_equity` and `account_maint_margin` as exact USD decimal strings, plus raw `account_status`.
Missing or invalid optional fields are `None`, without reusing prior values. Each client admits one query task, coalescing
concurrent requests. Failure or a pending recovery/risk application checkpoint publishes no query
event; applications own refresh scheduling, timeouts and per-source last-good values.

Execution startup and recovery still require an explicit instrument scope. Wallet representation
restrictions remain in force. Account queries do not unblock node startup or grant trading authority. See
[the field and availability contract](https://github.com/imbingox/nautilus_trader/blob/papi-v2.0.0rc16/crates/adapters/binance-papi/README.md#standard-account-queries).

Queries also expose `info["papi"]["position_metrics"]`: separate UM V1/V2 observations with exact
position quantities, configured leverage, exchange unrealized PnL, metadata-derived currencies,
and original source/receipt times. Missing fields and metadata remain explicit. Positions and
orders outside the configured instruments no longer block account query publication; recovery and
trading scope checks remain in force.

## Account-wide current reports

Version `2.0.0rc8` supports current UM positions and ordinary/algo open orders without a manually
supplied instrument list:

```python
client = BinancePapiReadOnlyClient(config)
positions = await client.generate_position_status_reports()
orders = await client.generate_order_status_reports(open_only=True)
```

These methods return native Nautilus reports and discover required USD-M instrument metadata.
Historical reports and trading recovery still require an explicit instrument scope.

## Recovery fixes in 2.0.0rc9

Risk-increasing orders remain blocked during private-stream recovery until the execution engine
has applied the current baseline and fresh risk evidence is acknowledged. Restart reconciliation
restores the original strategy owner from matching durable submission records even with an empty
Cache, without replaying unresolved orders. The command journal format is unchanged.

## Risk refresh recovery in 2.0.0rc13

Account events can trigger a new risk collection without first waiting for those same account
facts to be applied. Previously delivered execution reports must still be applied, and new risk
evidence is installed only after the engine applies its matching report. Superseded snapshots
cannot restore authorization, and freshness limits are unchanged.

Recovery diagnostics include the latest received fact version and session `issues`. Application
checkpoint and risk-report acknowledgement timeouts restrict the session and stop the refresh
worker. The host must handle the failure and recreate the execution client. This change has offline
regression coverage; it does not add a live strategy acceptance claim.

## Cancellation of claimed orders in 2.0.0rc12

A strategy can cancel its recovered ordinary UM orders through the standard single, batch, and
cancel-all APIs without the original Submit journal entry. Cancellation uses the engine's current
order ownership and venue identity. The journal records the new cancellation and its recovery;
no position handoff or fabricated submission history is required.

## Recovery evidence and position handoff in 2.0.0rc11

The production `BinancePapiExecutionClientFactory` exposes `recovery_state_json(client_id)`
for the same execution client used by the Node. Evidence includes scope, generation, freshness,
report application progress, and operation permissions. Readers do not retain journal ownership.

Explicit position handoff verifies the account, instruments, quantities, and stable strategy
ownership before persisting a receipt after Cache application. Historical external orders remain
EXTERNAL, and legacy fills already covered by the position snapshot are not counted twice.
Failed handoff, foreign open orders, or lost receipts block new risk-increasing orders. Startup
reconciliation also notifies the source execution client after applying recovered positions.

## Safety boundary

Importing PAPI support does not authorize trading. `BinancePapiExecutionClientConfig` defaults to
`trading=None`. Trading requires explicit credentials, a durable absolute command-journal path,
finite per-instrument and account limits, an explicit report scope, and current authenticated risk
evidence.

The first release is deliberately limited to one-way USD-M perpetual execution for explicitly
configured instruments. Unsupported products, hedge mode, ambiguous account state, stale risk
evidence, unknown command outcomes, and unsupported borrowing or interest state fail closed.

Live trading can lose real capital. Review the
[adapter documentation](https://github.com/imbingox/nautilus_trader/blob/main/crates/adapters/binance-papi/README.md)
and the
[account verification record](https://github.com/imbingox/nautilus_trader/blob/main/crates/adapters/binance-papi/ACCOUNT_VERIFICATION.md)
before configuring a live process. The verification record distinguishes offline coverage,
limited live evidence, known gaps, and unsupported behavior.

## Upgrades and rollback

Stop the existing process before changing the installed extension. Record the installed package
version, reconcile the account, and back up the command journal and related persistent state.
Install the new version into a fresh environment, then start through the normal recovery and
reconciliation path.

Never delete a command journal or replay an unresolved POST to force recovery. Before rolling back,
confirm that the older version can read the persisted format and reconcile against current venue
state. Published files are immutable; a corrected build is released under a new version.

Source, build constraints, release manifests, and the operator procedure are maintained in the
[fork repository](https://github.com/imbingox/nautilus_trader).
