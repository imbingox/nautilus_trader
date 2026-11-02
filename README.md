# Cairn PAPI

[![Candidate build](https://github.com/imbingox/nautilus_trader/actions/workflows/papi-build.yml/badge.svg?branch=main)](https://github.com/imbingox/nautilus_trader/actions/workflows/papi-build.yml)

**Cairn PAPI is an independent third-party fork based on
[NautilusTrader](https://github.com/nautechsystems/nautilus_trader). It is not affiliated with,
endorsed by, or supported by Nautech Systems Pty Ltd.**

This temporary fork is primarily for personal use and adds Binance Portfolio Margin (PAPI)
support. It is not intended to become a long-term independent distribution. The plan is to migrate
back to upstream when its PAPI support meets these needs. This does not imply suitable upstream
PAPI support is available today, and no migration date is promised.

## Installation and compatibility

The distribution is being renamed from `nautilus-trader-papi` to **`cairn-papi`**.
The Python import remains **`nautilus_trader`**. Check
[PyPI](https://pypi.org/project/cairn-papi/) for published artifacts and follow the
[installation and migration instructions](python/README_PAPI.md).

The intended binary release target remains CPython 3.14 (GIL enabled), Linux x86_64,
`manylinux_2_34`. Local builds on other platforms are development artifacts only.

**Never install `cairn-papi`, `nautilus-trader-papi`, or official `nautilus-trader` together:**
they share the same Python package and native extension paths. Prefer a fresh environment.
Downstream dependency names, lockfiles, and artifact hashes must be updated separately.

This rename does not change trading logic, PAPI interfaces, risk controls, or broker attribution.
For capabilities, limitations, and recovery requirements, read the
[PAPI adapter documentation](crates/adapters/binance-papi/README.md) and
[verification record](crates/adapters/binance-papi/ACCOUNT_VERIFICATION.md).

## Documentation and support

- [Fork issues and support](https://github.com/imbingox/nautilus_trader/issues).
- [Package description and migration](python/README_PAPI.md).
- [Build and release procedure](docs/developer_guide/papi_release.md).
- [Development setup](docs/developer_guide/environment_setup.md) and [contribution standards](CONTRIBUTING.md).
- [Security reporting](SECURITY.md).
- [Upstream documentation](https://nautilustrader.io/docs/), for inherited APIs only; it does not document or support this fork's PAPI implementation.

## Upstream attribution and license

Cairn PAPI is based on [NautilusTrader](https://github.com/nautechsystems/nautilus_trader),
originally developed by Nautech Systems Pty Ltd. The Rust engine, Python namespace, API names,
and upstream source references retain their original names for compatibility and attribution.

Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
Upstream copyright and attribution notices are retained. See [LICENSE](LICENSE) for the
GNU Lesser General Public License v3.0 and [upstream authors](https://github.com/nautechsystems/nautilus_trader/graphs/contributors).
