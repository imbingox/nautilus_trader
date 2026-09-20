# Report fixtures

These are official Binance Portfolio Margin REST response examples retrieved on 2026-09-12.
JSON formatting is normalized; example fields and values are retained. They are not
authenticated account captures and do not establish live endpoint semantics or retention.

| Fixture                 | Official reference section                                       |
| ----------------------- | ---------------------------------------------------------------- |
| `order.json`            | [Trade reference][trade]: Query UM Order.                        |
| `orders.json`           | [Trade reference][trade]: Query All UM Orders.                   |
| `trades.json`           | [Trade reference][trade]: UM Account Trade List.                 |
| `open_algos.json`       | [Trade reference][trade]: Query All Current UM Open Algo Orders. |
| `algo_history.json`     | [Trade reference][trade]: Query UM Algo Order History.           |
| `positions.json`        | [Account reference][account]: Query UM Position Information.     |
| `position_mode.json`    | [Account reference][account]: Get UM Current Position Mode.      |
| `order_rate_limit.json` | [Account reference][account]: Query User Rate Limit.             |

The ordinary-order example contains a hedge-mode `SHORT` position side and a zero limit price.
The position-mode example enables hedge mode. Tests deliberately reject these as unsupported
inputs. `src/testing.rs` derives explicitly synthetic one-way scenarios by changing identity,
price, quantity, status and timestamps; the fixture files keep the original examples. Triggered
parent/child lifecycles, pagination, invalid fields and error responses are synthetic tests.

Account response fixtures have their own [provenance](../observations/README.md).

[trade]: https://developers.binance.com/en/docs/catalog/advanced-trading-derivatives-trading-portfolio-margin/api/rest-api/trade
[account]: https://developers.binance.com/en/docs/catalog/advanced-trading-derivatives-trading-portfolio-margin/api/rest-api/account
