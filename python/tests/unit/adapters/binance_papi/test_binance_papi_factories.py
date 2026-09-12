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
Test the PAPI boundary in the shared Python extension.
"""

import pytest

from nautilus_trader import _libnautilus
from nautilus_trader.adapters.binance import BINANCE_VENUE
from nautilus_trader.adapters.binance import BinanceDataClientConfig
from nautilus_trader.adapters.binance import BinanceDataClientFactory
from nautilus_trader.adapters.binance import BinanceEnvironment
from nautilus_trader.adapters.binance import BinanceExecutionClientFactory
from nautilus_trader.adapters.binance import BinanceProductType
from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import AccountId
from nautilus_trader.model import TraderId


papi = pytest.importorskip("nautilus_trader.adapters.binance_papi")


def test_config_and_factory_share_core_types() -> None:
    """
    Verify the facade exposes the extension's registered classes and domain types.
    """
    account_id = AccountId("BINANCE-PAPI-002")
    config = papi.BinancePapiExecutionClientConfig(account_id=account_id)
    assert config.account_id == account_id
    assert isinstance(config.account_id, AccountId)
    assert (
        papi.BinancePapiExecutionClientConfig
        is _libnautilus.binance_papi.BinancePapiExecutionClientConfig
    )
    assert papi.BinancePapiExecutionClientFactory().name() == "BINANCE_PAPI"
    assert BinanceExecutionClientFactory().name() == "BINANCE"
    assert papi.BINANCE_PAPI_VENUE == BINANCE_VENUE


def test_default_account_issuer_matches_instrument_venue() -> None:
    """
    Keep the default account issuer consistent with the core's venue account index.
    """
    config = papi.BinancePapiExecutionClientConfig()
    assert config.account_id == AccountId("BINANCE-PAPI-001")
    assert str(config.account_id).split("-", 1)[0] == str(papi.BINANCE_PAPI_VENUE)


def test_builder_accepts_public_data_and_papi_execution() -> None:
    """
    Exercise both config extractors and factories through the real node builder.
    """
    trader_id = TraderId("PAPI-001")
    node = (
        LiveNode.builder("PAPI-PYTEST", trader_id, Environment.LIVE)
        .add_data_client(
            None,
            BinanceDataClientFactory(),
            BinanceDataClientConfig(
                product_type=BinanceProductType.USD_M,
                environment=BinanceEnvironment.LIVE,
            ),
        )
        .add_exec_client(
            None,
            papi.BinancePapiExecutionClientFactory(),
            papi.BinancePapiExecutionClientConfig(account_id=AccountId("BINANCE-PAPI-002")),
        )
        .build()
    )
    assert node.trader_id == trader_id
    assert node.environment == Environment.LIVE


def test_builder_rejects_wrong_config() -> None:
    """
    A Binance data config must not be accepted by the PAPI factory.
    """
    with pytest.raises((ValueError, TypeError, RuntimeError), match=r"(?i)config"):
        (
            LiveNode.builder("PAPI-WRONG-CONFIG", TraderId("PAPI-001"), Environment.LIVE)
            .add_exec_client(
                None,
                papi.BinancePapiExecutionClientFactory(),
                BinanceDataClientConfig(),
            )
            .build()
        )


def test_run_fails_explicitly_without_network() -> None:
    """
    A construction-only execution client cannot silently start successfully.
    """
    node = (
        LiveNode.builder("PAPI-UNSUPPORTED", TraderId("PAPI-001"), Environment.LIVE)
        .with_timeout_connection(1)
        .add_exec_client(
            None,
            papi.BinancePapiExecutionClientFactory(),
            papi.BinancePapiExecutionClientConfig(),
        )
        .build()
    )
    with pytest.raises(RuntimeError, match="readiness timeout"):
        node.run()
