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
Verify standard account queries using an installed wheel and loopback venue.
"""

import base64
import hashlib
import importlib.metadata
import json
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

from nautilus_trader.adapters.binance import BinanceDataClientConfig
from nautilus_trader.adapters.binance import BinanceDataClientFactory
from nautilus_trader.adapters.binance import BinanceInstrumentProviderConfig
from nautilus_trader.adapters.binance import BinanceProductType
from nautilus_trader.adapters.binance_papi import BINANCE_PAPI_CLIENT_ID
from nautilus_trader.adapters.binance_papi import BinancePapiExecutionClientConfig
from nautilus_trader.adapters.binance_papi import BinancePapiExecutionClientFactory
from nautilus_trader.adapters.binance_papi import BinancePapiReadOnlyConfig
from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import AccountId
from nautilus_trader.model import Currency
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import TraderId
from nautilus_trader.trading import Strategy


ACCOUNT = AccountId("BINANCE-PAPI-001")
SOURCE_MS = 1617939110373
INSTRUMENT = InstrumentId.from_str("BTCUSDT-PERP.BINANCE")


class Venue:
    """
    Provide a quiet account without any trading endpoints or private stream updates.
    """

    def __init__(self) -> None:
        """
        Initialize mutable REST fixture facts.
        """
        self.equity = "2234.1234567800"
        self.zero = False
        self.fail = False
        self.block_recovery = False
        self.requests = []
        fixture = (
            Path(__file__).resolve().parents[3]
            / "crates/adapters/binance/test_data"
            / "futures/http_json/exchange_info_usdm.json"
        )
        self.exchange_info = json.loads(fixture.read_text())

    def response(self, path: str) -> dict | list:  # noqa: PLR0911 - fixture endpoint dispatch
        """
        Return a bounded synthetic venue response.
        """
        self.requests.append(path)
        if path == "/fapi/v1/exchangeInfo":
            return self.exchange_info
        if path == "/papi/v1/listenKey":
            return {"listenKey": "offline-query-account"}
        if path == "/papi/v1/account":
            return {
                "actualEquity": None if self.fail else self.equity,
                "uniMMR": "51.12345678",
                "accountStatus": "NORMAL",
                "updateTime": SOURCE_MS,
            }
        if path == "/papi/v1/balance":
            assets = [("USDT", "0")] if self.zero else [("USDT", "1000"), ("BTC", "1")]
            return [
                {
                    "asset": asset,
                    "totalWalletBalance": amount,
                    "crossMarginBorrowed": "1" if self.block_recovery else "0",
                    "crossMarginInterest": "0",
                    "updateTime": SOURCE_MS,
                }
                for asset, amount in assets
            ]
        if path in {"/papi/v1/um/account", "/papi/v2/um/account"}:
            return {"assets": [], "positions": []}
        if path == "/papi/v1/um/positionSide/dual":
            return {"dualSidePosition": False}
        allowed = {
            "/papi/v1/um/openOrders",
            "/papi/v1/um/algo/openAlgoOrders",
            "/papi/v1/cm/positionRisk",
            "/papi/v1/cm/openOrders",
            "/papi/v1/margin/openOrders",
            "/papi/v1/um/positionRisk",
            "/papi/v1/um/allOrders",
            "/papi/v1/um/algo/allAlgoOrders",
            "/papi/v1/um/userTrades",
        }

        if path in allowed:
            return []
        raise AssertionError(f"Unexpected endpoint {path}")


def serve_websocket(handler: BaseHTTPRequestHandler) -> None:
    """
    Keep fixture sockets alive and answer WebSocket control frames.
    """
    handler.connection.settimeout(60)
    try:
        while header := handler.rfile.read(2):
            opcode = header[0] & 15
            size = header[1] & 127

            if size == 126:
                size = int.from_bytes(handler.rfile.read(2), "big")
            elif size == 127:
                size = int.from_bytes(handler.rfile.read(8), "big")
            mask = handler.rfile.read(4) if header[1] & 128 else None
            payload = handler.rfile.read(size)

            if mask:
                payload = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
            if opcode == 8:
                handler.connection.sendall(b"\x88\x00")
                break
            if opcode == 9:
                handler.connection.sendall(bytes([0x8A, len(payload)]) + payload)
    except (OSError, TimeoutError):
        pass


def handler(venue: Venue) -> type[BaseHTTPRequestHandler]:
    """
    Build a standard-library HTTP/WebSocket fixture handler.
    """

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args: object) -> None:
            pass

        def do_GET(self):
            if self.headers.get("Upgrade", "").lower() == "websocket":
                key = self.headers["Sec-WebSocket-Key"] + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
                accept = base64.b64encode(
                    hashlib.sha1(key.encode(), usedforsecurity=False).digest(),
                ).decode()
                self.send_response(101)
                self.send_header("Upgrade", "websocket")
                self.send_header("Connection", "Upgrade")
                self.send_header("Sec-WebSocket-Accept", accept)
                self.end_headers()
                serve_websocket(self)
                return
            self.respond()

        def do_POST(self):
            assert urlsplit(self.path).path == "/papi/v1/listenKey"
            self.respond()

        def do_DELETE(self):
            assert urlsplit(self.path).path == "/papi/v1/listenKey"
            self.respond()

        def respond(self):
            body = json.dumps(venue.response(urlsplit(self.path).path)).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


class QueryStrategy(Strategy):
    """
    Verify account updates, partial failure, and subsequent recovery.
    """

    def __init__(self) -> None:
        """
        Initialize verification progress.
        """
        super().__init__()
        self.stage = 0
        self.started = False
        self.error = None
        self.observations = []

    def on_start(self) -> None:
        """
        Request the first refresh and watch cache events.
        """
        self.started = True
        self.deadline = time.monotonic() + 45
        self.query_count = self.venue.requests.count("/papi/v1/account")
        self.submit_queries()
        self.clock.set_timer_ns("account-query-check", 100_000_000, callback=self.check)

    def submit_queries(self) -> None:
        """
        Submit a burst of standard commands for coalescing verification.
        """
        for _ in range(50):
            self.query_account(ACCOUNT, client_id=BINANCE_PAPI_CLIENT_ID)

    def check(self, _event) -> None:
        """
        Record callback failures so the process fails after node shutdown.
        """
        try:
            self.check_result()
        except Exception as e:
            self.error = repr(e)
            self.shutdown_system("Account query verification failed")

    def check_result(self) -> None:
        """
        Verify each phase before advancing the REST fixture.
        """
        assert time.monotonic() < self.deadline, "Account query timed out"
        account = self.cache.account(ACCOUNT)
        event = account.last_event
        info = event.info.get("papi")
        if not info:
            return
        summary = info["portfolio_margin"]
        assert summary["units"] == {"actual_equity": "USD", "uni_mmr": "ratio"}
        assert summary["uni_mmr"] == "51.12345678"
        assert summary["ts_source"] == SOURCE_MS * 1_000_000
        assert summary["ts_received"] > summary["ts_source"]
        assert len(info["sources"]) == 9

        if self.stage == 0:
            assert summary["actual_equity"] == "2234.1234567800"
            self.observations.append(info)
            self.last_id = event.event_id
            self.stage = 1
            self.venue.equity = "2235.1234567800"
            self.venue.zero = True
            self.submit_queries()
        elif self.stage == 1 and event.event_id != self.last_id:
            assert summary["actual_equity"] == "2235.1234567800"
            assert account.balance_total(Currency.from_str("USDT")).as_decimal() == 0
            assert account.balance_total(Currency.from_str("BTC")).as_decimal() == 1
            assert [row["asset"] for row in info["wallet"]["asset_times"]] == ["USDT"]
            self.observations.append(info)
            self.last_id = event.event_id
            self.stage = 2
            self.venue.fail = True
            self.failed_at = time.monotonic()
            self.submit_queries()
        elif self.stage == 2:
            assert event.event_id == self.last_id, "Failed query published a new event"
            if time.monotonic() - self.failed_at < 6:
                return
            assert self.venue.requests.count("/papi/v1/account") == self.query_count + 3
            self.venue.fail = False
            self.venue.equity = "2236.1234567800"
            self.stage = 3
            self.submit_queries()
        elif self.stage == 3 and event.event_id != self.last_id:
            assert summary["actual_equity"] == "2236.1234567800"
            assert self.venue.requests.count("/papi/v1/account") == self.query_count + 4
            self.observations.append(info)
            self.stage = 4
            self.shutdown_system("Account query verification passed")


def main() -> None:
    """
    Run the loopback venue and real node without enabling trading or submitting orders.
    """
    venue = Venue()
    venue.block_recovery = "--blocked" in sys.argv
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler(venue))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f"127.0.0.1:{server.server_port}"
    config = BinancePapiReadOnlyConfig(
        account_id=ACCOUNT,
        api_key="OfflinePapiKey",
        api_secret="OfflinePapiSecret",
        base_url=f"http://{origin}",
        websocket_url=f"ws://{origin}/ws",
    )
    check = unittest.TestCase()
    with check.assertRaisesRegex(ValueError, "1 to 256"):  # noqa: PT027 - stdlib-only wheel check
        BinancePapiExecutionClientConfig(account_id=ACCOUNT, read_only=config, instrument_ids=[])

    node = (
        LiveNode.builder("PAPI-QUERY-WHEEL", TraderId("QUERY-001"), Environment.LIVE)
        .with_timeout_connection(15)
        .with_timeout_disconnection_secs(3)
        .with_delay_post_stop_secs(0)
        .add_data_client(
            None,
            BinanceDataClientFactory(),
            BinanceDataClientConfig(
                product_type=BinanceProductType.USD_M,
                base_url_http=f"http://{origin}",
                base_url_ws=f"ws://{origin}/ws",
                instrument_provider=BinanceInstrumentProviderConfig(
                    load_all=False,
                    load_ids=[str(INSTRUMENT)],
                    query_commission_rates=False,
                ),
            ),
        )
        .add_exec_client(
            None,
            BinancePapiExecutionClientFactory(),
            BinancePapiExecutionClientConfig(
                account_id=ACCOUNT,
                read_only=config,
                instrument_ids=[INSTRUMENT],
            ),
        )
        .build()
    )
    strategy = QueryStrategy()
    strategy.venue = venue
    node.add_strategy(strategy)
    try:
        if venue.block_recovery:
            with check.assertRaisesRegex(  # noqa: PT027 - stdlib-only wheel check
                RuntimeError,
                "exec-connect timeout|readiness timeout",
            ):
                node.run()
            assert not strategy.started
            print("Blocked recovery correctly prevented strategy startup")  # noqa: T201
            return

        node.run()
        assert strategy.error is None, strategy.error
        assert strategy.stage == 4, f"Incomplete stage {strategy.stage}"
        print(  # noqa: T201 - standalone verification
            json.dumps(
                {
                    "version": importlib.metadata.version("nautilus-trader-papi"),
                    "result": "passed",
                    "observations": strategy.observations,
                },
            ),
        )
    finally:
        node.dispose()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


if __name__ == "__main__":
    main()
