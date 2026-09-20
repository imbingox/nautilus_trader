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
Binance Portfolio Margin read-only queries (requires the ``papi`` build feature).

Use ``nautilus_trader.adapters.binance`` for public data and instrument loading.
``BinancePapiReadOnlyClient`` returns exact account evidence and execution reports.
``BinancePapiAccountSession`` adds private-stream observation and bounded recovery.
Trading commands remain disabled.

"""

from nautilus_trader._libnautilus.binance_papi import BINANCE_PAPI
from nautilus_trader._libnautilus.binance_papi import BINANCE_PAPI_CLIENT_ID
from nautilus_trader._libnautilus.binance_papi import BINANCE_PAPI_VENUE
from nautilus_trader._libnautilus.binance_papi import BinancePapiAccountSession
from nautilus_trader._libnautilus.binance_papi import BinancePapiExecutionClientConfig
from nautilus_trader._libnautilus.binance_papi import BinancePapiExecutionClientFactory
from nautilus_trader._libnautilus.binance_papi import BinancePapiInstrumentTradingConfig
from nautilus_trader._libnautilus.binance_papi import BinancePapiReadOnlyClient
from nautilus_trader._libnautilus.binance_papi import BinancePapiReadOnlyConfig
from nautilus_trader._libnautilus.binance_papi import BinancePapiReadOnlySnapshot
from nautilus_trader._libnautilus.binance_papi import BinancePapiTradingConfig


__all__ = [
    "BINANCE_PAPI",
    "BINANCE_PAPI_CLIENT_ID",
    "BINANCE_PAPI_VENUE",
    "BinancePapiAccountSession",
    "BinancePapiExecutionClientConfig",
    "BinancePapiExecutionClientFactory",
    "BinancePapiInstrumentTradingConfig",
    "BinancePapiReadOnlyClient",
    "BinancePapiReadOnlyConfig",
    "BinancePapiReadOnlySnapshot",
    "BinancePapiTradingConfig",
]
