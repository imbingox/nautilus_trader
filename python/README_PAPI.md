# NautilusTrader PAPI

`nautilus-trader-papi` is an unofficial, full NautilusTrader distribution maintained in the
[`imbingox/nautilus_trader`](https://github.com/imbingox/nautilus_trader) fork. It adds guarded
Binance Portfolio Margin (PAPI) account, reconciliation, and execution support while retaining the
standard `nautilus_trader` import path and a single Rust extension.

This package is not published or supported by Nautech Systems. Nautech Systems remains the author
and copyright holder of the upstream project. Report fork-specific issues to the
[fork issue tracker](https://github.com/imbingox/nautilus_trader/issues).

## Supported wheel

The initial `2.0.0rc7` release supports only:

- CPython 3.14 with the GIL enabled
- Linux x86_64 with a `manylinux_2_34` baseline

No source distribution is published. Installations on other Python versions, operating systems,
architectures, or free-threaded Python builds are unsupported and must not fall back to a local
Rust build.

Install the exact binary release in a new virtual environment:

```bash
python3.14 -m venv .venv
.venv/bin/python -m pip install --only-binary=:all: nautilus-trader-papi==2.0.0rc7
```

Do not install `nautilus-trader` and `nautilus-trader-papi` together. Both distributions provide
the same `nautilus_trader` package and `_libnautilus` extension. Remove the official distribution,
or preferably create a fresh environment, before installing this fork.

The fork uses its own PEP 440 version sequence. `2.0.0rc7` is the first published PAPI candidate;
future PAPI changes receive a new public version even when the tracked upstream version is
unchanged. A matching number does not imply that the two distributions are interchangeable.

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
