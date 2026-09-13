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
Validate an installed full wheel from a neutral directory using ``python -I``.

Pass ``enabled`` or ``disabled`` to verify the corresponding PAPI build feature.
This script needs only the installed wheel and the Python standard library.

"""

import asyncio
import importlib
import importlib.metadata
import sys
import unittest
from pathlib import Path

import nautilus_trader
from nautilus_trader import _libnautilus
from nautilus_trader.adapters.binance import BINANCE_VENUE
from nautilus_trader.adapters.binance import BinanceDataClientConfig
from nautilus_trader.adapters.binance import BinanceDataClientFactory
from nautilus_trader.adapters.binance import BinanceEnvironment
from nautilus_trader.adapters.binance import BinanceExecutionClientConfig
from nautilus_trader.adapters.binance import BinanceExecutionClientFactory
from nautilus_trader.adapters.binance import BinanceProductType
from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import AccountId
from nautilus_trader.model import CryptoPerpetual
from nautilus_trader.model import Currency
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import Price
from nautilus_trader.model import Quantity
from nautilus_trader.model import Symbol
from nautilus_trader.model import TraderId


def main() -> None:
    """
    Check package isolation, shared types, factory extraction and node construction.
    """
    assert len(sys.argv) == 2
    assert sys.argv[1] in {"enabled", "disabled"}
    for module in (nautilus_trader, _libnautilus):
        assert Path(module.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
    for name in ("analysis", "backtest", "betfair", "binance", "blockchain", "live", "model"):
        assert hasattr(_libnautilus, name), f"Full wheel is missing {name}"
    assert _libnautilus.model.HIGH_PRECISION
    distribution = importlib.metadata.distribution("nautilus-trader-papi")
    assert distribution.version == nautilus_trader.__version__
    direct_url = distribution.read_text("direct_url.json")
    assert direct_url is None or "editable" not in direct_url

    trader_id = TraderId("PAPI-001")
    data_config = BinanceDataClientConfig(
        product_type=BinanceProductType.USD_M,
        environment=BinanceEnvironment.LIVE,
    )
    assert BinanceExecutionClientFactory().name() == "BINANCE"
    original = (
        LiveNode.builder("BINANCE-WHEEL", trader_id, Environment.LIVE)
        .add_data_client(None, BinanceDataClientFactory(), data_config)
        .add_exec_client(
            None,
            BinanceExecutionClientFactory(),
            BinanceExecutionClientConfig(
                account_id=AccountId("BINANCE-001"),
                product_type=BinanceProductType.USD_M,
                environment=BinanceEnvironment.LIVE,
                api_key="offline-test-key",
                api_secret="offline-test-secret",
                use_ws_trading=False,
            ),
        )
        .build()
    )
    assert original.trader_id == trader_id

    if sys.argv[1] == "disabled":
        assert not hasattr(_libnautilus, "binance_papi")
        check = unittest.TestCase()
        with check.assertRaises(ModuleNotFoundError) as raised:  # noqa: PT027 - stdlib-only wheel check
            importlib.import_module("nautilus_trader.adapters.binance_papi")
        assert raised.exception.name == "nautilus_trader._libnautilus.binance_papi"
        print("PAPI-disabled wheel checks passed")  # noqa: T201 - standalone verification
        return

    papi = importlib.import_module("nautilus_trader.adapters.binance_papi")
    assert (
        papi.BinancePapiExecutionClientConfig
        is _libnautilus.binance_papi.BinancePapiExecutionClientConfig
    )
    assert (
        papi.BinancePapiExecutionClientFactory
        is _libnautilus.binance_papi.BinancePapiExecutionClientFactory
    )
    assert papi.BINANCE_PAPI_VENUE == BINANCE_VENUE
    account_id = AccountId("BINANCE-PAPI-002")
    config = papi.BinancePapiExecutionClientConfig(account_id=account_id)
    assert config.account_id == account_id
    assert isinstance(config.account_id, AccountId)
    assert papi.BinancePapiExecutionClientFactory().name() == "BINANCE_PAPI"
    for name in (
        "BinancePapiReadOnlyClient",
        "BinancePapiReadOnlyConfig",
        "BinancePapiReadOnlySnapshot",
    ):
        assert getattr(papi, name) is getattr(_libnautilus.binance_papi, name)
    instrument = CryptoPerpetual(
        instrument_id=InstrumentId.from_str("BTCUSDT-PERP.BINANCE"),
        raw_symbol=Symbol("BTCUSDT"),
        base_currency=Currency.from_str("BTC"),
        quote_currency=Currency.from_str("USDT"),
        settlement_currency=Currency.from_str("USDT"),
        is_inverse=False,
        price_precision=1,
        size_precision=3,
        price_increment=Price.from_str("0.1"),
        size_increment=Quantity.from_str("0.001"),
        ts_event=0,
        ts_init=0,
    )
    read_only = papi.BinancePapiReadOnlyConfig(
        account_id=account_id,
        api_key="OfflinePapiKey",
        api_secret="OfflinePapiSecret",
        base_url="http://127.0.0.1:9",
    )
    reader = papi.BinancePapiReadOnlyClient(read_only, [instrument])
    reader.cancel()

    async def canceled_query() -> None:
        await reader.query_order_rate_limit()

    check = unittest.TestCase()
    with check.assertRaisesRegex(RuntimeError, "canceled"):  # noqa: PT027 - stdlib-only check
        asyncio.run(canceled_query())
    assert "OfflinePapiSecret" not in repr(read_only)
    assert not hasattr(read_only, "api_secret")
    node = (
        LiveNode.builder("PAPI-WHEEL", trader_id, Environment.LIVE)
        .add_data_client(None, BinanceDataClientFactory(), data_config)
        .add_exec_client(None, papi.BinancePapiExecutionClientFactory(), config)
        .build()
    )
    assert node.trader_id == trader_id

    # Exclude data clients here so the failure check cannot connect to public data services
    unsupported = (
        LiveNode.builder("PAPI-UNSUPPORTED", trader_id, Environment.LIVE)
        .with_timeout_connection(1)
        .add_exec_client(
            None,
            papi.BinancePapiExecutionClientFactory(),
            papi.BinancePapiExecutionClientConfig(
                read_only=read_only,
                instrument_ids=[instrument.id],
            ),
        )
        .build()
    )
    # The engine logs the client's PAPI error and the node fails its readiness check
    check = unittest.TestCase()
    with check.assertRaisesRegex(RuntimeError, "readiness timeout"):  # noqa: PT027 - stdlib-only check
        unsupported.run()
    print("PAPI-enabled wheel checks passed")  # noqa: T201 - standalone verification


if __name__ == "__main__":
    main()
