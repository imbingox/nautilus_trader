#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.gnu.org/licenses/lgpl-3.0.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""
Run one bounded live-order acceptance action through the Binance PAPI execution client.

WARNING: This script places REAL orders against a live Portfolio Margin account. It is intentionally
limited to 0.001 BTC on BTCUSDT perpetual with at most 200 USDT of configured exposure. Run each
position-changing action in a separate process so startup installs a new authenticated risk snapshot.

"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import threading
from decimal import Decimal
from pathlib import Path
from time import time_ns
from typing import Any, cast

from nautilus_trader.adapters.binance import (
    BinanceDataClientConfig,
    BinanceDataClientFactory,
    BinanceEnvironment,
    BinanceInstrumentProviderConfig,
    BinanceProductType,
    load_binance_instruments,
)
from nautilus_trader.adapters.binance_papi import (
    BINANCE_PAPI_CLIENT_ID,
    BinancePapiExecutionClientConfig,
    BinancePapiExecutionClientFactory,
    BinancePapiInstrumentTradingConfig,
    BinancePapiReadOnlyClient,
    BinancePapiReadOnlyConfig,
    BinancePapiTradingConfig,
)
from nautilus_trader.common import Environment
from nautilus_trader.config import LiveRiskEngineConfig
from nautilus_trader.live import LiveNode
from nautilus_trader.model import (
    AccountId,
    CryptoPerpetual,
    Currency,
    InstrumentId,
    OrderSide,
    Quantity,
    TimeInForce,
    TraderId,
)
from nautilus_trader.trading import Strategy

INSTRUMENT_ID = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")
OBSERVATION_INSTRUMENT_IDS = (
    INSTRUMENT_ID,
    InstrumentId.from_str("GWEIUSDT-PERP.BINANCE"),
)
QUANTITY = Quantity.from_str("0.001")
MAX_EXPOSURE = Decimal(200)
ACTIONS = (
    "market-open",
    "market-close",
    "limit-gtc",
    "limit-ioc",
    "limit-fok",
    "limit-gtx",
)


def _read_json(path: Path, max_bytes: int = 16_384) -> Any:
    with path.open("rb") as stream:
        payload = stream.read(max_bytes + 1)
    if len(payload) > max_bytes:
        raise ValueError("Input file exceeds its size limit")
    return json.loads(payload)


def _load_credentials(path: Path) -> tuple[BinancePapiReadOnlyConfig, str | None]:
    values = _read_json(path)
    if not isinstance(values, dict):
        raise TypeError("Credential configuration must be a JSON object")
    try:
        account_id = AccountId(values.pop("account_id"))
        proxy_url = values.get("proxy_url")
        config = BinancePapiReadOnlyConfig(account_id=account_id, **values)
    except (KeyError, TypeError, ValueError, AttributeError):
        raise ValueError("Invalid PAPI credential configuration") from None
    return config, proxy_url


async def _load_instruments(proxy_url: str | None) -> list[CryptoPerpetual]:
    instruments = cast(
        list[CryptoPerpetual],
        await load_binance_instruments(
            BinanceDataClientConfig(
                product_type=BinanceProductType.USD_M,
                environment=BinanceEnvironment.LIVE,
                instrument_provider=BinanceInstrumentProviderConfig(
                    load_all=False,
                    load_ids=[str(instrument_id) for instrument_id in OBSERVATION_INSTRUMENT_IDS],
                    query_commission_rates=False,
                ),
                proxy_url=proxy_url,
            ),
        ),
    )
    matched = [
        instrument for instrument in instruments if instrument.id in OBSERVATION_INSTRUMENT_IDS
    ]

    if len(matched) != len(OBSERVATION_INSTRUMENT_IDS):
        raise RuntimeError("Public metadata did not return the complete observation scope")
    return matched


def _json_value(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def _reports(reports: list[Any]) -> list[Any]:
    return [report.to_dict() for report in reports]


async def _observe(
    config: BinancePapiReadOnlyConfig,
    instruments: list[Any],
    *,
    include_history: bool,
) -> dict[str, Any]:
    client = BinancePapiReadOnlyClient(config, instruments)
    try:
        await client.refresh_account_observations()
        result = {
            "observed_ns": time_ns(),
            "account_observations_json": client.account_observations_json(60_000),
            "open_orders": _reports(
                await client.generate_open_order_status_reports(INSTRUMENT_ID),
            ),
            "positions": _reports(
                await client.generate_position_status_reports(INSTRUMENT_ID),
            ),
        }

        if include_history:
            end = time_ns()
            snapshot = await client.generate_mass_status(end - 3_600_000_000_000, end)
            result["mass_status_json"] = snapshot.to_json()
        return result
    finally:
        client.cancel()


def _position_quantity(observation: dict[str, Any]) -> Decimal:
    nonflat = [row for row in observation["positions"] if str(row["position_side"]) != "FLAT"]
    if not nonflat:
        return Decimal(0)
    if len(nonflat) != 1:
        raise RuntimeError("Expected at most one non-flat BTCUSDT position")
    row = nonflat[0]
    quantity = Decimal(str(row["quantity"]))
    side = str(row["position_side"])
    if side.endswith("SHORT"):
        return -quantity
    if not side.endswith("LONG"):
        raise RuntimeError("Unsupported BTCUSDT position side")
    return quantity


def _validate_preflight(action: str, observation: dict[str, Any]) -> None:
    if observation["open_orders"]:
        raise RuntimeError("BTCUSDT has existing open orders; refusing to start acceptance")
    position = _position_quantity(observation)
    if action == "market-close":
        if position != Decimal("0.001"):
            raise RuntimeError("market-close requires an existing long BTCUSDT position of 0.001")
    elif position != Decimal(0):
        raise RuntimeError(f"{action} requires a flat BTCUSDT position")


def _matching_postflight_order(evidence: dict[str, Any]) -> dict[str, Any] | None:
    mass_status_json = evidence.get("postflight", {}).get("mass_status_json")
    if not mass_status_json:
        return None
    mass_status = json.loads(mass_status_json)["mass_status"]
    matches = [
        report
        for report in mass_status["order_reports"].values()
        if report.get("client_order_id") == evidence.get("client_order_id")
    ]

    if len(matches) > 1:
        raise RuntimeError("Postflight returned duplicate reports for the acceptance order")
    return matches[0] if matches else None


def _evaluate_postflight(action: str, evidence: dict[str, Any]) -> None:
    postflight = evidence["postflight"]
    order = _matching_postflight_order(evidence)
    event_types = {event["type"] for event in evidence["events"]}
    flat = _position_quantity(postflight) == Decimal(0)
    no_open_orders = not postflight["open_orders"]

    accepted = False
    reason = "Postflight did not prove the expected terminal outcome"

    if action == "limit-ioc" and order is not None:
        accepted = (
            order["order_status"] == "EXPIRED"
            and Decimal(str(order["filled_qty"])) == Decimal(0)
            and flat
            and no_open_orders
        )
        reason = "REST confirmed zero-fill expiration" if accepted else reason
    elif action == "limit-fok" and order is not None:
        accepted = (
            order["order_status"] == "FILLED"
            and Decimal(str(order["filled_qty"])) == QUANTITY.as_decimal()
            and _position_quantity(postflight) == QUANTITY.as_decimal()
            and no_open_orders
        )
        reason = "REST confirmed the full FOK fill" if accepted else reason
    elif action in {"limit-gtc", "limit-gtx"}:
        accepted = "OrderCanceled" in event_types and flat and no_open_orders
        reason = "Event stream and REST confirmed targeted cancellation" if accepted else reason
    elif action == "market-open" and order is not None:
        accepted = (
            order["order_status"] == "FILLED"
            and Decimal(str(order["filled_qty"])) == QUANTITY.as_decimal()
            and _position_quantity(postflight) == QUANTITY.as_decimal()
            and no_open_orders
        )
        reason = "REST confirmed the opening fill" if accepted else reason
    elif action == "market-close" and order is not None:
        accepted = (
            order["order_status"] == "FILLED"
            and Decimal(str(order["filled_qty"])) == QUANTITY.as_decimal()
            and flat
            and no_open_orders
        )
        reason = "REST confirmed the reduce-only closing fill" if accepted else reason

    evidence["postflight_order"] = order
    evidence["accepted_outcome"] = accepted
    evidence["outcome_reason"] = reason


class AcceptanceStrategy(Strategy):
    """Submit one acceptance order and stop after its expected terminal event."""

    def __init__(self) -> None:
        super().__init__()
        self.action = ""
        self.evidence: dict[str, Any] = {}
        self.instrument: CryptoPerpetual | None = None
        self.order: Any | None = None
        self.submitted = False
        self.cancel_requested = False
        self.filled_quantity = Decimal(0)

    def configure(self, action: str, evidence: dict[str, Any]) -> None:
        self.action = action
        self.evidence = evidence

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(INSTRUMENT_ID)
        if self.instrument is None:
            self.evidence["strategy_error"] = "BTCUSDT instrument is unavailable"
            self.shutdown_system("PAPI acceptance instrument unavailable")
            return
        self.subscribe_quotes(INSTRUMENT_ID)

    def on_quote(self, quote: Any) -> None:
        if self.submitted:
            return
        self.submitted = True
        if self.action == "market-open":
            self.order = self.order_factory.market(
                INSTRUMENT_ID,
                OrderSide.BUY,
                QUANTITY,
                reduce_only=False,
            )
        elif self.action == "market-close":
            self.order = self.order_factory.market(
                INSTRUMENT_ID,
                OrderSide.SELL,
                QUANTITY,
                reduce_only=True,
            )
        else:
            instrument = self.instrument
            if instrument is None:
                self.evidence["strategy_error"] = "BTCUSDT instrument disappeared before submit"
                self._finish("PAPI acceptance instrument unavailable", accepted_outcome=False)
                return

            reference_price = (
                quote.ask_price.as_decimal()
                if self.action == "limit-fok"
                else quote.bid_price.as_decimal()
            )
            multiplier = Decimal("1.01") if self.action == "limit-fok" else Decimal("0.99")
            price = instrument.make_price(float(reference_price * multiplier))
            time_in_force = {
                "limit-gtc": TimeInForce.GTC,
                "limit-ioc": TimeInForce.IOC,
                "limit-fok": TimeInForce.FOK,
                "limit-gtx": TimeInForce.GTC,
            }[self.action]
            self.order = self.order_factory.limit(
                INSTRUMENT_ID,
                OrderSide.BUY,
                QUANTITY,
                price,
                time_in_force=time_in_force,
                post_only=self.action == "limit-gtx",
                reduce_only=False,
            )
        self.evidence["client_order_id"] = str(self.order.client_order_id)
        self.evidence["submitted_order"] = self.order.to_dict()
        self.submit_order(self.order, client_id=BINANCE_PAPI_CLIENT_ID)

    def _record(self, event: Any) -> None:
        self.evidence.setdefault("events", []).append(event.to_dict())

    def _finish(self, reason: str, *, accepted_outcome: bool) -> None:
        self.evidence["terminal_reason"] = reason
        self.evidence["accepted_outcome"] = accepted_outcome
        self.shutdown_system(reason)

    def on_order_submitted(self, event: Any) -> None:
        self._record(event)

    def on_order_accepted(self, event: Any) -> None:
        self._record(event)
        if self.action in {"limit-gtc", "limit-gtx"} and not self.cancel_requested:
            self.cancel_requested = True
            order = self.order
            if order is None:
                self.evidence["strategy_error"] = "Accepted order identity is unavailable"
                self._finish("PAPI acceptance order identity unavailable", accepted_outcome=False)
                return

            self.cancel_order(order.client_order_id, client_id=BINANCE_PAPI_CLIENT_ID)

    def on_order_filled(self, event: Any) -> None:
        self._record(event)
        self.filled_quantity += event.last_qty.as_decimal()
        if self.filled_quantity > QUANTITY.as_decimal():
            self.evidence["strategy_error"] = "Acceptance fills exceeded the submitted quantity"
            self._finish("PAPI acceptance overfill", accepted_outcome=False)
        elif self.filled_quantity == QUANTITY.as_decimal():
            self._finish(
                "PAPI acceptance order filled",
                accepted_outcome=self.action in {"market-open", "market-close"},
            )

    def on_order_canceled(self, event: Any) -> None:
        self._record(event)
        self._finish(
            "PAPI acceptance order canceled",
            accepted_outcome=self.action in {"limit-gtc", "limit-gtx"},
        )

    def on_order_expired(self, event: Any) -> None:
        self._record(event)
        self._finish(
            "PAPI acceptance order expired",
            accepted_outcome=self.action in {"limit-ioc", "limit-fok"},
        )

    def on_order_denied(self, event: Any) -> None:
        self._record(event)
        self._finish("PAPI acceptance order denied", accepted_outcome=False)

    def on_order_rejected(self, event: Any) -> None:
        self._record(event)
        self._finish("PAPI acceptance order rejected", accepted_outcome=False)

    def on_order_cancel_rejected(self, event: Any) -> None:
        self._record(event)
        self._finish("PAPI acceptance cancel rejected", accepted_outcome=False)


def _build_node(
    config: BinancePapiReadOnlyConfig,
    proxy_url: str | None,
    journal: Path,
    strategy: AcceptanceStrategy,
) -> LiveNode:
    limits = BinancePapiInstrumentTradingConfig(
        instrument_id=INSTRUMENT_ID,
        max_order_quantity=Decimal("0.001"),
        max_order_notional=MAX_EXPOSURE,
        max_position_quantity=Decimal("0.001"),
        max_instrument_exposure=MAX_EXPOSURE,
    )
    trading = BinancePapiTradingConfig(
        command_journal_path=journal,
        risk_currency=Currency.from_str("USDT"),
        instrument_limits=[limits],
        max_account_exposure=MAX_EXPOSURE,
        max_in_flight_operations=2,
        max_risk_age_ms=60_000,
        max_risk_collection_span_ms=60_000,
        max_recovery_requests=64,
        max_recovery_rounds=3,
        recovery_recheck_interval_ms=1_000,
        market_order_price_buffer_bps=100,
        fee_buffer_bps=50,
    )
    node = (
        LiveNode.builder(
            "BINANCE-PAPI-ACCEPTANCE", TraderId("PAPI-ACCEPTANCE-001"), Environment.LIVE
        )
        .with_risk_engine_config(LiveRiskEngineConfig(bypass=True))
        .with_timeout_connection(90)
        .with_timeout_disconnection_secs(10)
        .with_delay_post_stop_secs(2)
        .add_data_client(
            None,
            BinanceDataClientFactory(),
            BinanceDataClientConfig(
                product_type=BinanceProductType.USD_M,
                environment=BinanceEnvironment.LIVE,
                instrument_provider=BinanceInstrumentProviderConfig(
                    load_all=False,
                    load_ids=[str(instrument_id) for instrument_id in OBSERVATION_INSTRUMENT_IDS],
                    query_commission_rates=False,
                ),
                proxy_url=proxy_url,
            ),
        )
        .add_exec_client(
            None,
            BinancePapiExecutionClientFactory(),
            BinancePapiExecutionClientConfig(
                account_id=config.account_id,
                read_only=config,
                instrument_ids=list(OBSERVATION_INSTRUMENT_IDS),
                trading=trading,
            ),
        )
        .build()
    )
    node.add_strategy(strategy)
    return node


def _write_new_private(path: Path, value: dict[str, Any]) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, default=_json_value)
        stream.write("\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=ACTIONS)
    parser.add_argument("--credentials", type=Path, required=True)
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout-secs", type=int, default=120)
    args = parser.parse_args(argv)
    if not args.journal.is_absolute() or not args.output.is_absolute():
        parser.error("journal and output paths must be absolute")
    if args.journal.exists() or args.output.exists():
        parser.error("journal and output paths must not already exist")
    if not 30 <= args.timeout_secs <= 300:
        parser.error("timeout-secs must be between 30 and 300")

    evidence: dict[str, Any] = {
        "schema_version": 1,
        "action": args.action,
        "instrument_id": str(INSTRUMENT_ID),
        "quantity": str(QUANTITY),
        "max_exposure_usdt": str(MAX_EXPOSURE),
        "started_ns": time_ns(),
        "events": [],
    }
    node = None
    timer = None
    exit_code = 1
    try:
        config, proxy_url = _load_credentials(args.credentials)
        instruments = asyncio.run(_load_instruments(proxy_url))
        evidence["preflight"] = asyncio.run(
            _observe(config, instruments, include_history=False),
        )
        _validate_preflight(args.action, evidence["preflight"])

        strategy = AcceptanceStrategy()
        strategy.configure(args.action, evidence)
        node = _build_node(config, proxy_url, args.journal, strategy)

        def stop_on_timeout() -> None:
            if not evidence.get("accepted_outcome"):
                evidence["timed_out"] = True
            os.kill(os.getpid(), signal.SIGINT)

        timer = threading.Timer(args.timeout_secs, stop_on_timeout)
        timer.daemon = True
        timer.start()
        node.run()
        timer.cancel()
        evidence["postflight"] = asyncio.run(
            _observe(config, instruments, include_history=True),
        )
        _evaluate_postflight(args.action, evidence)
        evidence["completed_ns"] = time_ns()
        exit_code = 0 if evidence.get("accepted_outcome") else 1
    except Exception as e:  # noqa: BLE001 - Preserve the live failure in the private evidence.
        evidence["error_type"] = type(e).__name__
        evidence["error"] = str(e)
        evidence["failed_ns"] = time_ns()
    finally:
        if timer is not None:
            timer.cancel()
        if node is not None:
            node.dispose()
        try:
            _write_new_private(args.output, evidence)
        except FileExistsError:
            print("Evidence output already exists; refusing to overwrite")
            return 1

    print(f"PAPI {args.action} acceptance evidence written to {args.output}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
