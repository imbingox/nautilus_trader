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
Exercise PAPI Python boundaries against synthetic HTTP responses.

Use explicit test credentials without reading the host environment.

"""

import asyncio
import hashlib
import hmac
import json
import stat
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import contextmanager
from contextlib import suppress
from decimal import Decimal
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from pathlib import Path
from threading import Event
from threading import Thread
from typing import Any
from urllib.parse import parse_qs
from urllib.parse import urlsplit

import pytest
from unit.adapters.example_modules import load_example_module

from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import AccountId
from nautilus_trader.model import ClientOrderId
from nautilus_trader.model import ExecutionMassStatus
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import OrderStatusReport
from nautilus_trader.model import PositionStatusReport
from nautilus_trader.model import TraderId
from nautilus_trader.testkit.providers import TestInstrumentProvider


papi = pytest.importorskip("nautilus_trader.adapters.binance_papi")
TEST_DATA = Path(__file__).resolve().parents[5] / "crates/adapters/binance-papi/test_data"
API_KEY = "OfflinePapiKey"
API_SECRET = "OfflinePapiSecret"
TRADE_TIME = 1_680_688_557_875
INSTRUMENT_ID = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")
type Reply = tuple[int, str]


def _quiet(path: str, params: dict[str, list[str]]) -> Reply:
    if path == "/papi/v1/um/positionSide/dual":
        return 200, '{"dualSidePosition":false}'
    if path == "/papi/v1/um/positionRisk":
        rows = json.loads((TEST_DATA / "reports/positions.json").read_text(encoding="utf-8"))
        rows[0]["symbol"] = params["symbol"][0]
        return 200, json.dumps(rows[:1])
    sources = {
        "/papi/v1/balance": "observations/balances.json",
        "/papi/v1/account": "observations/account.json",
        "/papi/v1/um/account": "observations/um_account.json",
        "/papi/v2/um/account": "observations/um_account.json",
        "/papi/v1/rateLimit/order": "reports/order_rate_limit.json",
    }

    if path in sources:
        return 200, (TEST_DATA / sources[path]).read_text(encoding="utf-8")
    if path in {
        "/papi/v1/um/openOrders",
        "/papi/v1/um/algo/openAlgoOrders",
        "/papi/v1/um/allOrders",
        "/papi/v1/um/algo/allAlgoOrders",
        "/papi/v1/um/userTrades",
    }:
        return 200, "[]"
    return 400, '{"code":-2013,"msg":"Order does not exist"}'


@contextmanager
def _serve(
    reply: Callable[[str, dict[str, list[str]]], Reply] = _quiet,
) -> Iterator[tuple[str, list[dict[str, Any]]]]:
    requests: list[dict[str, Any]] = []

    class Handler(BaseHTTPRequestHandler):
        """
        Serve synthetic signed GET responses without logging request URLs.
        """

        def do_GET(self) -> None:
            """
            Record the request and return its synthetic response.
            """
            target = urlsplit(self.path)
            params = parse_qs(target.query)
            unsigned = "&".join(
                part for part in target.query.split("&") if not part.startswith("signature=")
            )
            expected = hmac.new(API_SECRET.encode(), unsigned.encode(), hashlib.sha256).hexdigest()
            requests.append(
                {
                    "path": target.path,
                    "api_key": self.headers.get("X-MBX-APIKEY"),
                    "signature_valid": hmac.compare_digest(
                        params.get("signature", [""])[0],
                        expected,
                    ),
                },
            )
            status, body = reply(target.path, params)
            payload = body.encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            with suppress(BrokenPipeError, ConnectionResetError):
                self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            """
            Suppress request logs that would contain synthetic signatures.
            """

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def _config(url: str, **kwargs: Any) -> Any:
    return papi.BinancePapiReadOnlyConfig(
        account_id=AccountId("BINANCE-PAPI-009"),
        api_key=API_KEY,
        api_secret=API_SECRET,
        base_url=url,
        **kwargs,
    )


def _filled_order() -> dict[str, Any]:
    row = json.loads((TEST_DATA / "reports/order.json").read_text(encoding="utf-8"))
    row.update(
        positionSide="BOTH",
        price="28511.00",
        side="SELL",
        origQty="0.010",
        executedQty="0.010",
        avgPrice="28511.00",
        status="FILLED",
        orderId=270_093_109,
        clientOrderId="abc",
        time=TRADE_TIME - 1_000,
        updateTime=TRADE_TIME,
    )
    return row


def test_python_queries_return_shared_domain_types_and_exact_fees() -> None:
    """
    Exercise the asynchronous bindings, snapshot serialization, and SDK authentication.
    """

    def reply(path: str, params: dict[str, list[str]]) -> Reply:
        if path == "/papi/v1/um/order":
            return 200, json.dumps(_filled_order())
        if path == "/papi/v1/um/allOrders":
            return 200, json.dumps([_filled_order()])
        if path == "/papi/v1/um/userTrades":
            rows = json.loads((TEST_DATA / "reports/trades.json").read_text(encoding="utf-8"))
            rows[0]["commission"] = "-0.00001234"
            rows[0]["commissionAsset"] = "BNB"
            return 200, json.dumps(rows[:1])
        return _quiet(path, params)

    with _serve(reply) as (url, requests):
        config = _config(url)
        client = papi.BinancePapiReadOnlyClient(
            config,
            [TestInstrumentProvider.btcusdt_perp_binance()],
        )
        assert requests == []

        async def query() -> tuple[Any, Any, Any]:
            await client.refresh_account_observations()
            await client.query_order_rate_limit()
            assert await client.generate_open_order_status_reports() == []
            positions = await client.generate_position_status_reports(INSTRUMENT_ID)
            order = await client.generate_order_status_report(
                INSTRUMENT_ID,
                client_order_id=ClientOrderId("abc"),
            )
            snapshot = await client.generate_mass_status(
                (TRADE_TIME - 2_000) * 1_000_000,
                TRADE_TIME * 1_000_000,
            )
            return positions, order, snapshot

        positions, order, snapshot = asyncio.run(query())
        mass = snapshot.mass_status()
        fill = next(iter(mass.fill_reports.values()))[0]
        assert isinstance(positions[0], PositionStatusReport)
        assert isinstance(order, OrderStatusReport)
        assert isinstance(mass, ExecutionMassStatus)
        assert order.venue_order_id == fill.venue_order_id
        assert fill.commission.as_decimal() == Decimal("-0.00001234")
        assert str(fill.commission.currency) == "BNB"
        assert snapshot.instrument_ids() == [INSTRUMENT_ID]
        assert snapshot.window_end == TRADE_TIME * 1_000_000
        assert snapshot.reports_complete is False
        assert mass.reports_complete is False
        assert snapshot.issues()
        assert json.loads(snapshot.to_json())["mass_status"]["reports_complete"] is False
        assert all(request["signature_valid"] for request in requests)
        assert all(request["api_key"] == API_KEY for request in requests)
        assert API_KEY not in repr(config) + repr(client) + snapshot.to_json()
        assert API_SECRET not in repr(config) + repr(client) + snapshot.to_json()
        for name in ("api_key", "api_secret", "base_url"):
            assert not hasattr(config, name)


@pytest.mark.parametrize("overrides", [{"max_requests": 0}, {"request_timeout_ms": 0}])
def test_invalid_bounds_fail_without_network(overrides: dict[str, int]) -> None:
    """
    Reject invalid request budgets at the Python constructor boundary.
    """
    with _serve() as (url, requests):
        with pytest.raises(ValueError, match="PAPI"):
            _config(url, **overrides)
        assert requests == []


def test_python_scope_and_factory_validation_preserve_account_identity() -> None:
    """
    Require explicit supported metadata and matching account identities.
    """
    with _serve() as (url, requests):
        config = _config(url)

        for instruments in (
            [],
            [TestInstrumentProvider.btcusdt_perp_binance()] * 2,
            [TestInstrumentProvider.btcusdt_binance()],
        ):
            with pytest.raises(ValueError, match="PAPI"):
                papi.BinancePapiReadOnlyClient(config, instruments)
        execution = papi.BinancePapiExecutionClientConfig(
            read_only=config,
            instrument_ids=[INSTRUMENT_ID],
        )
        assert execution.account_id == config.account_id
        assert execution.read_only.operation_timeout_ms == 60_000
        assert execution.instrument_ids == [INSTRUMENT_ID]
        with pytest.raises(ValueError, match="must match"):
            papi.BinancePapiExecutionClientConfig(
                account_id=AccountId("BINANCE-PAPI-010"),
                read_only=config,
                instrument_ids=[INSTRUMENT_ID],
            )
        assert requests == []


def test_python_cancellation_preserves_failed_refresh_evidence() -> None:
    """
    Mark unread account sources failed when Python drops an in-flight refresh.
    """
    entered = Event()
    release = Event()

    def reply(path: str, params: dict[str, list[str]]) -> Reply:
        if path == "/papi/v1/balance":
            entered.set()
            release.wait(10)
        return _quiet(path, params)

    with _serve(reply) as (url, requests):
        client = papi.BinancePapiReadOnlyClient(
            _config(url),
            [TestInstrumentProvider.btcusdt_perp_binance()],
        )

        async def cancel_refresh() -> None:
            pending = client.refresh_account_observations()
            assert await asyncio.to_thread(entered.wait, 3)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            client.cancel()
            with pytest.raises(RuntimeError, match="canceled"):
                await client.query_order_rate_limit()

        try:
            asyncio.run(cancel_refresh())
            sources = json.loads(client.account_observations_json(30_000))
            assert all(source["receipt_status"] == "failed" for source in sources)
            assert len(requests) == 1
        finally:
            release.set()


def test_python_failed_single_order_query_never_becomes_none() -> None:
    """
    Preserve the error boundary for unverified order-not-found responses.
    """
    with _serve() as (url, _requests):
        client = papi.BinancePapiReadOnlyClient(
            _config(url),
            [TestInstrumentProvider.btcusdt_perp_binance()],
        )

        async def query() -> None:
            await client.generate_order_status_report(
                INSTRUMENT_ID,
                client_order_id=ClientOrderId("abc"),
            )

        with pytest.raises(RuntimeError):
            asyncio.run(query())


def test_configured_live_node_rejects_unaccepted_account_mapping_without_requests() -> None:
    """
    Keep valid credentials from implicitly enabling account publication or startup.
    """
    with _serve() as (url, requests):
        node = (
            LiveNode.builder("PAPI-READ-ONLY", TraderId("PAPI-001"), Environment.LIVE)
            .with_timeout_connection(1)
            .add_exec_client(
                None,
                papi.BinancePapiExecutionClientFactory(),
                papi.BinancePapiExecutionClientConfig(
                    read_only=_config(url),
                    instrument_ids=[INSTRUMENT_ID],
                ),
            )
            .build()
        )
        with pytest.raises(RuntimeError, match="readiness timeout"):
            node.run()
        assert requests == []


@pytest.mark.parametrize("fail_account", [False, True])
def test_acceptance_script_keeps_exact_private_evidence_and_source_failures(
    tmp_path: Path,
    fail_account: bool,
) -> None:
    """
    Run the real example with serialized instruments and only loopback HTTP requests.
    """

    def reply(path: str, params: dict[str, list[str]]) -> Reply:
        if path == "/papi/v1/account" and fail_account:
            return 401, '{"code":-2015,"msg":"Invalid key"}'
        status, body = _quiet(path, params)
        if path == "/papi/v1/balance":
            body = body.replace("{", '{"futureNumeric":0.12345678901234567890123456789,', 1)
        return status, body

    with _serve(reply) as (url, requests):
        credentials = tmp_path / "credentials.json"
        credentials.write_text(
            json.dumps(
                {
                    "account_id": "BINANCE-PAPI-009",
                    "api_key": API_KEY,
                    "api_secret": API_SECRET,
                    "base_url": url,
                },
            ),
            encoding="utf-8",
        )
        instrument_file = tmp_path / "instruments.json"
        instrument_file.write_text(
            json.dumps([TestInstrumentProvider.btcusdt_perp_binance().to_dict()], default=str),
            encoding="utf-8",
        )
        output = tmp_path / "evidence.json"
        example = load_example_module("binance_papi", "read_only_acceptance")
        argv = [
            "--credentials",
            str(credentials),
            "--instrument-id",
            str(INSTRUMENT_ID),
            "--instruments-file",
            str(instrument_file),
            "--output",
            str(output),
        ]
        result = example.main(argv)
        evidence = json.loads(output.read_text(encoding="utf-8"))
        assert result == int(fail_account)
        assert stat.S_IMODE(output.stat().st_mode) == 0o600
        assert "0.12345678901234567890123456789" in evidence["account_observations_json"]
        assert evidence["acceptance"]["native_balance_mapping"] == "unavailable"
        assert evidence["acceptance"]["history_completeness"] == "unverified"
        assert evidence["operations"]["account_observations"] == (
            "failed" if fail_account else "succeeded"
        )

        if fail_account:
            assert evidence["operations"]["mass_status"] == "not_attempted"
            assert (
                json.loads(evidence["account_observations_json"])[1]["receipt_status"] == "failed"
            )
        else:
            assert json.loads(evidence["snapshot_json"])["mass_status"]["reports_complete"] is False
        previous = output.read_bytes()
        request_count = len(requests)
        assert example.main(argv) == 1
        assert output.read_bytes() == previous
        assert len(requests) == request_count
