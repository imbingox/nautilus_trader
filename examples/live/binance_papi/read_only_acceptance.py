# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""
Collect private PAPI GET evidence for manual account and endpoint acceptance.

Read explicit credentials from a local JSON file. Write a new file with owner-only
permissions, containing unsanitized account data. The collection never places orders or
changes account settings. Successful collection does not by itself accept a projected
account, prove complete history, or enable LiveNode startup.

"""

import argparse
import asyncio
import json
import os
from decimal import Decimal
from pathlib import Path
from time import time_ns
from typing import Any

from nautilus_trader.adapters.binance import BinanceDataClientConfig
from nautilus_trader.adapters.binance import BinanceEnvironment
from nautilus_trader.adapters.binance import BinanceInstrumentProviderConfig
from nautilus_trader.adapters.binance import BinanceProductType
from nautilus_trader.adapters.binance import load_binance_instruments
from nautilus_trader.adapters.binance_papi import BinancePapiReadOnlyClient
from nautilus_trader.adapters.binance_papi import BinancePapiReadOnlyConfig
from nautilus_trader.model import AccountId
from nautilus_trader.model import CryptoFuture
from nautilus_trader.model import CryptoPerpetual
from nautilus_trader.model import InstrumentId


def _read_json(path: Path, max_bytes: int) -> Any:
    with path.open("rb") as stream:
        payload = stream.read(max_bytes + 1)
    if len(payload) > max_bytes:
        raise ValueError("Input file exceeds its size limit")
    return json.loads(payload)


def _load_credentials(path: Path) -> tuple[BinancePapiReadOnlyConfig, str | None]:
    try:
        values = _read_json(path, 16_384)
        account_id = AccountId(values.pop("account_id"))
        proxy_url = values.get("proxy_url")
        config = BinancePapiReadOnlyConfig(account_id=account_id, **values)
    except (KeyError, TypeError, ValueError, AttributeError):
        raise ValueError("Invalid PAPI credential configuration") from None
    else:
        return config, proxy_url


async def _load_scope(
    instrument_ids: list[InstrumentId],
    instruments_file: Path | None,
    proxy_url: str | None,
) -> list[CryptoFuture | CryptoPerpetual]:
    if instruments_file is None:
        loaded = await load_binance_instruments(
            BinanceDataClientConfig(
                product_type=BinanceProductType.USD_M,
                environment=BinanceEnvironment.LIVE,
                instrument_provider=BinanceInstrumentProviderConfig(
                    load_all=False,
                    load_ids=[str(instrument_id) for instrument_id in instrument_ids],
                    query_commission_rates=False,
                ),
                proxy_url=proxy_url,
            ),
        )
        instruments = [
            instrument
            for instrument in loaded
            if isinstance(instrument, (CryptoFuture, CryptoPerpetual))
        ]
    else:
        rows = _read_json(instruments_file, 8 * 1024 * 1024)
        if not isinstance(rows, list) or len(rows) != len(instrument_ids):
            raise ValueError(
                "Instrument metadata must have exactly one row per requested instrument",
            )
        instruments = []

        for row in rows:
            if not isinstance(row, dict):
                raise TypeError("Instrument metadata rows must be JSON objects")
            if row.get("type") == "CryptoPerpetual":
                instruments.append(CryptoPerpetual.from_dict(row))
            elif row.get("type") == "CryptoFuture":
                instruments.append(CryptoFuture.from_dict(row))
            else:
                raise ValueError("Instrument metadata must contain linear UM futures")

    if len(instruments) != len(instrument_ids) or {i.id for i in instruments} != set(
        instrument_ids,
    ):
        raise ValueError(
            "Loaded metadata does not exactly match the requested instrument scope",
        )
    return instruments


def _decimal_json(value: object) -> str:
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError("Instrument metadata contains a value that cannot be serialized")


async def collect_evidence(
    config: BinancePapiReadOnlyConfig,
    instruments: list[CryptoFuture | CryptoPerpetual],
    start: int,
    end: int,
    max_receipt_age_ms: int = 30_000,
) -> dict[str, Any]:
    """
    Collect scoped observations and preserve partial failures for manual review.

    JSON payloads remain strings inside the bundle so Python never reparses or rounds
    raw numeric tokens. A recent receipt or successful mock response does not establish
    authenticated venue compatibility. Inspect each required acceptance case separately.

    """
    if not 0 <= start <= end <= time_ns():
        raise ValueError("Invalid inclusive history window")
    if not 1 <= max_receipt_age_ms <= 2**64 - 1:
        raise ValueError(
            "Receipt age must be a positive unsigned 64-bit millisecond value",
        )

    client = BinancePapiReadOnlyClient(config, instruments)
    operations = dict.fromkeys(
        ("account_observations", "account_snapshot", "order_rate_limit", "mass_status"),
        "not_attempted",
    )
    evidence: dict[str, Any] = {
        "schema_version": 2,
        "account_id": str(config.account_id),
        "instrument_ids": [str(instrument.id) for instrument in instruments],
        "instruments_json": json.dumps(
            [i.to_dict() for i in instruments],
            default=_decimal_json,
        ),
        "window_start_ns": start,
        "window_end_ns": end,
        "collection_started_ns": time_ns(),
        "max_receipt_age_ms": max_receipt_age_ms,
        "operations": operations,
        "acceptance": {
            "venue_semantics": "requires_manual_review",
            "native_balance_mapping": "requires_snapshot_review",
            "history_completeness": "unverified",
            "live_node_startup": "unavailable",
        },
    }
    operation = "account_observations"
    try:
        await client.refresh_account_observations()
        operations[operation] = "succeeded"
        operation = "order_rate_limit"
        evidence["order_rate_limit_json"] = await client.query_order_rate_limit()
        operations[operation] = "succeeded"
        operation = "mass_status"
        snapshot = await client.generate_mass_status(start, end)
        evidence["snapshot_json"] = snapshot.to_json()
        operations[operation] = "succeeded"
    except RuntimeError as e:
        operations[operation] = "failed"
        evidence["error"] = str(e)
    finally:
        try:
            try:
                evidence["account_snapshot_json"] = client.account_snapshot_json(
                    max_receipt_age_ms,
                    max_receipt_age_ms,
                )
                operations["account_snapshot"] = "succeeded"
            except RuntimeError as e:
                operations["account_snapshot"] = "failed"
                evidence["account_snapshot_error"] = str(e)
            evidence["account_observations_json"] = client.account_observations_json(
                max_receipt_age_ms,
            )
        finally:
            client.cancel()
            evidence["collection_finished_ns"] = time_ns()
    return evidence


async def _collect(
    args: argparse.Namespace,
    config: BinancePapiReadOnlyConfig,
    proxy_url: str | None,
) -> dict[str, Any]:
    instrument_ids = [InstrumentId.from_str(value) for value in args.instrument_id]
    if not 1 <= len(instrument_ids) <= 256 or len(set(instrument_ids)) != len(
        instrument_ids,
    ):
        raise ValueError("Specify 1 to 256 unique instrument IDs")
    instruments = await _load_scope(instrument_ids, args.instruments_file, proxy_url)
    end = time_ns()
    start = end - args.lookback_minutes * 60_000_000_000
    return await collect_evidence(config, instruments, start, end)


def main(argv: list[str] | None = None) -> int:
    """
    Write a private evidence bundle without overwriting an existing file.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credentials", type=Path, required=True)
    parser.add_argument("--instrument-id", action="append", required=True)
    parser.add_argument("--instruments-file", type=Path)
    parser.add_argument("--lookback-minutes", type=int, default=60)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if not 1 <= args.lookback_minutes <= 10_080:
        parser.error("lookback-minutes must be between 1 and 10080")

    try:
        config, proxy_url = _load_credentials(args.credentials)
        descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                evidence = asyncio.run(_collect(args, config, proxy_url))
                json.dump(evidence, stream, indent=2)
                stream.write("\n")
        except BaseException:
            args.output.unlink(missing_ok=True)
            raise
    except FileExistsError:
        print("Output already exists; choose a new evidence file")
        return 1
    except (OSError, ValueError, TypeError, RuntimeError):
        print(
            "Collection setup failed; check credentials, instrument metadata, and output path",
        )
        return 1
    except KeyboardInterrupt:
        print("Collection canceled")
        return 130

    success = all(status == "succeeded" for status in evidence["operations"].values())
    result = "complete" if success else "incomplete"
    print(f"GET collection {result}; private evidence written to {args.output}")
    print("Venue and account acceptance still require manual review")
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
