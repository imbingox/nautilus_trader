# PAPI wheel release

This fork publishes `nautilus-trader-papi` independently from the upstream NautilusTrader release
pipeline. The package intentionally preserves the `nautilus_trader` import path and therefore
conflicts with the official `nautilus-trader` distribution.

## Frozen release contract

| Field               | Value                                                       |
| ------------------- | ----------------------------------------------------------- |
| Distribution        | `nautilus-trader-papi`                                      |
| Version             | `2.0.0rc8`                                                  |
| Python              | CPython 3.14, GIL build                                     |
| ABI                 | `cp314`                                                     |
| Platform            | Linux x86_64                                                |
| Platform baseline   | `manylinux_2_34`                                            |
| Build profile       | Cargo `release`                                             |
| Python extension    | Full `nautilus-pyo3` extension with high precision and PAPI |
| Source distribution | Not published                                               |
| Release tag         | `papi-v2.0.0rc8`                                            |

The upstream baseline is
`46a5658a2f66cf0a798d414dc1b63d98cf10fcc1`. The PAPI feature head before upstream integration is
`67a5fb9a9c1d938bfdb5d3c2531b3e975097423b`, the verified feature merge is
`dda2c3d8f6e6d6bfc3e5a7e72917e69c69129e9a`, and the first `main` integration is
`06c658b3606e052fd6f1cac43b53205d746fb35a`. The final release manifest records the later release
commit and tag.

The version belongs to this distribution. Increment it for every changed PAPI release candidate,
even when the upstream Python version remains unchanged. PyPI files are immutable, so never rebuild
different bytes under an uploaded version.

## Changes in 2.0.0rc8

Current position and open-order reports can query the whole UM account without supplied
instruments. The client discovers active instruments from official USD-M exchange metadata and
returns native Nautilus status reports. Historical reports and trading recovery retain their
explicit instrument scope.

Read-only live acceptance on 2026-09-25 returned one nonzero position and no ordinary or algo open
orders in two consecutive rounds. Position direction, quantity, and entry price matched signed
`positionRisk` and UM V1 account responses exactly. The V1 account contained 907 position rows;
906 zero positions were omitted from the native account-wide result. Nonempty open orders and an
algo trigger/child lifecycle were not available for this live acceptance. No orders were placed
or canceled, and account settings were not changed.

## Downstream dependency audit

The wheel declares no runtime dependency on the official distribution. A repository scan found no
dependency on `nautilus-trader` inside this fork. The local `nacre_trader` project has a `worker`
extra pinned to `nautilus_trader==1.230.0`; that extra cannot coexist with this package and must be
changed to an explicit `nautilus-trader-papi` version before that worker environment adopts PAPI.
Do not use an empty compatibility package or a permanent `--no-deps` installation to bypass the
declared dependency.

For every downstream environment:

1. Resolve its direct and transitive `nautilus-trader` requirements.
2. Create a new environment and install exactly one of the two distributions.
3. Run `pip check` or `uv pip check` after all downstream dependencies are installed.
4. Confirm `importlib.metadata.distribution("nautilus-trader")` is absent in a PAPI environment.

## Build and verification

The scripts in `scripts/papi-release` are the release entry points. They default to validation and
never upload files.

The formal builder requires a clean checkout at an explicit commit or release tag, CPython 3.14,
Linux x86_64, the locked Rust/Python dependencies, and the exact maturin pin. It builds the full
release wheel and rejects a non-`manylinux_2_34_x86_64` artifact.

```bash
bash scripts/papi-release/build-wheel.bash <release-commit-or-tag> dist/papi
bash scripts/papi-release/verify-wheel.bash dist/papi
python3 scripts/papi-release/generate-manifest.py \
  --wheel-dir dist/papi \
  --source-ref <release-commit-or-tag> \
  --tag papi-v2.0.0rc8 \
  --output dist/papi/release-manifest.json
```

`verify-wheel.bash` checks the filename and tags, package metadata, archive contents, license,
forbidden files, size, wheel integrity, dynamic-library policy through `auditwheel`, rendered
metadata through pinned `twine`, and an isolated installed-wheel smoke test. The smoke test runs
outside the source tree with `python -I`; it verifies the shared extension, the PAPI facade,
constructor-only secrets and endpoints, exact `Decimal` trading limits, explicit journal wiring,
default trading disablement, and bounded offline failure.

The formal wheel uses the features declared by `python/pyproject.toml`: `extension-module`, `arrow`,
`betfair`, `high-precision`, `mimalloc`, `papi`, `redis`, `postgres`, `defi`, `hypersync`, and
`tracing-bridge`. The Binance SDK remains pinned at `69.2.1`. PAPI HTTP requests select Rustls, but
the full dependency graph also contains native-TLS/OpenSSL. `auditwheel show` is therefore required;
the repaired wheel must carry allowed native libraries rather than depend on undeclared build-host
paths.

A PAPI-disabled wheel is a regression artifact only. Build it from the same source with the other
release features unchanged, verify the SDK and PAPI crate are absent from its dependency graph, and
run `binance_papi_wheel_smoke.py disabled`. Never upload it under the release name and version.

## Capability statement

| Status                           | Scope                                                                                                                                                                                                         |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Implemented and offline verified | Authenticated account observations, account-wide current reports, scoped history, durable command journal, bounded recovery, exact fee/quantity handling, explicit admission limits, default-disabled trading |
| Limited live evidence            | BTCUSDT one-way market open/reduce-only close, GTC/GTX targeted cancellation, FOK behavior, live market fills                                                                                                 |
| Not fully live verified          | Partial or multiple fills, direct IOC terminal delivery, every concurrent scheduling case, complete account-flat coverage, retention boundaries, venue throttling, latest dynamic rebaseline optimization     |
| Unsupported and rejected         | Hedge mode, coin-margined or margin products, unspecified trading instruments, ambiguous credentials or account identity, nonzero unsupported borrowing/interest state, unbounded or stale risk evidence      |

The release does not broaden the adapter's trading scope. Additional live acceptance requires a
separate operator decision with explicit account, action, and size; packaging work never authorizes
orders or account changes.

## Trusted Publishing and promotion

The workflow template is stored outside `.github` at
`scripts/papi-release/papi-release.yml`. A fork maintainer must review and place an approved version
at `.github/workflows/papi-release.yml`, because repository policy reserves that path for
maintainers.

Configure separate pending publishers or projects on TestPyPI and PyPI with owner `imbingox`,
repository `nautilus_trader`, the exact adopted workflow filename, and environments `testpypi` and
`pypi`. Only publish jobs receive `id-token: write`; the production environment must restrict the
release tag and require approval where the GitHub plan supports it.

The promotion sequence is:

1. Merge the validated release branch into `main` without changing the candidate tree.
2. Create signed tag `papi-v2.0.0rc8` at that exact commit.
3. Build and verify the wheel once, then freeze its SHA-256 manifest.
4. Upload the frozen wheel to TestPyPI through the protected OIDC job.
5. Run `verify-index.bash testpypi ...`; it downloads the file, compares SHA-256, and repeats the
   isolated installation check.
6. Approve the PyPI environment and upload the same artifact bytes without rebuilding.
7. Run `verify-index.bash pypi ...` and archive the manifest and job URLs.

Any source, version, build flag, repair step, metadata, or byte change creates a new candidate and
restarts TestPyPI validation. A missing publisher, registry size limit, failed native audit, or
hash mismatch blocks promotion.

## Upgrade and rollback

Before installation, stop the existing process, capture its package version and account state, and
back up the command journal and related persistence. Install into a fresh environment, verify only
`nautilus-trader-papi` owns the `nautilus_trader` package, then start through recovery and venue
reconciliation.

Do not delete or rewrite a journal to clear an unknown command. Before rollback, confirm the older
binary understands the persisted format and reconcile current venue state. If compatibility cannot
be proven, keep trading restricted and recover with an operator-reviewed procedure. Registry files
and release tags are immutable; corrections use a new package version.
