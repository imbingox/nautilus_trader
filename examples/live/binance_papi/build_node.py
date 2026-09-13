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
Construct a node with Binance public USD-M data and the PAPI execution factory.

Requires a wheel built with the ``papi`` feature. This example does not run the
node or contact Binance. PAPI LiveNode startup awaits an accepted account balance mapping.
Use read_only_acceptance.py to collect scoped account evidence and execution reports.

"""

from nautilus_trader.adapters.binance import BinanceDataClientConfig
from nautilus_trader.adapters.binance import BinanceDataClientFactory
from nautilus_trader.adapters.binance import BinanceEnvironment
from nautilus_trader.adapters.binance import BinanceProductType
from nautilus_trader.adapters.binance_papi import BinancePapiExecutionClientConfig
from nautilus_trader.adapters.binance_papi import BinancePapiExecutionClientFactory
from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import TraderId


def build_node() -> LiveNode:
    """
    Build a node without starting clients or acquiring network resources.
    """
    return (
        LiveNode.builder("BINANCE-PAPI-EXAMPLE", TraderId("PAPI-001"), Environment.LIVE)
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
            BinancePapiExecutionClientFactory(),
            BinancePapiExecutionClientConfig(),
        )
        .build()
    )


if __name__ == "__main__":
    node = build_node()
    print(f"Constructed node for {node.trader_id}; PAPI account bootstrap remains unavailable")
