# nautilus-binance-papi

Binance Portfolio Margin (PAPI) execution adapter skeleton for NautilusTrader.
It supports configuration, factory extraction and `LiveNode` construction only.
Starting or connecting the client, submitting or querying orders, publishing account
state and generating reconciliation reports return explicit errors. Cleanup is idempotent.
The engine logs the client error; `LiveNode.run()` then fails its connection readiness
check. No credentials, account balances, positions or network resources are loaded by this client.
The core margin/netting metadata is reserved for construction; it does not establish PM
account compatibility or support for a position mode.

The independent factory name and default client ID are `BINANCE_PAPI`. Instrument venue
remains `BINANCE`. The default account ID is `BINANCE-PAPI-001`, keeping its issuer
aligned with the venue used by the core account cache. Use `BinanceDataClientFactory`, `BinanceDataClientConfig` and
`load_binance_instruments` from `nautilus_trader.adapters.binance` for existing public
market data and instrument loading. No Binance code is copied or reconfigured by this adapter.

## Feature flags

- `extension-module`: Builds Python bindings into an extension module.
- `high-precision` (default): Uses 128-bit fixed-point domain values.
- `python`: Enables Python configuration and factory bindings.

The encompassing `nautilus-pyo3` crate enables `papi` by default, so regular Cargo builds,
maturin wheel builds and development installs include PAPI bindings. Pass
`--no-default-features` to Cargo or maturin to disable PAPI. Both adapters use the same
`_libnautilus` extension and factory registry. The skeleton does not depend on `binance-sdk`
or OpenSSL; REST integration belongs to the next implementation stage.

## Python installation verification

The first verification target is CPython 3.14 on Linux x86_64. This is not a promise of
support for other platforms. The provisional distribution name is `nautilus-trader-papi`;
the import name remains `nautilus_trader`. Install this full wheel in its own environment,
without the upstream `nautilus-trader` distribution, which owns the same import files.

From a development environment prepared according to `CONTRIBUTING.md`:

```bash
make sync UV_SYNC_FLAGS="--python 3.14"
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

## Local tests

```bash
bash scripts/strip-adapter-env.bash cargo test --locked -p nautilus-binance-papi --lib
uv run --project python --no-sync pytest \
  python/tests/unit/adapters/binance_papi/test_binance_papi_factories.py
make format
make pre-commit
```

The Python boundary tests require the PAPI-enabled extension to be installed. A feature-disabled
wheel skips that module; the standalone wheel smoke test uses mandatory assertions instead.
