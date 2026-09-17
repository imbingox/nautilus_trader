# AccountState

`AccountState` carries a snapshot of an account's balances and margins. The system publishes it when
the venue reports an account update through the execution client, or when the `Portfolio`
recalculates account state after a position update (for margin accounts with
`calculate_account_state` enabled). The `Portfolio` subscribes to these events internally
to maintain exposure and balance tracking.

The `is_reported` flag distinguishes venue-reported snapshots from system-calculated ones.

## Fields

| Field                 | Python type            | Required/default | Description                                                               |
| --------------------- | ---------------------- | ---------------- | ------------------------------------------------------------------------- |
| `account_id`          | `AccountId`            | Required         | The account ID (with the venue).                                          |
| `account_type`        | `AccountType`          | Required         | The account type (`CASH`, `MARGIN`, `BETTING`, or `WALLET`).              |
| `base_currency`       | `Currency` or `None`   | `None`           | The account base currency (`None` for multi-currency accounts).           |
| `is_reported`         | `bool`                 | Required         | If the state is reported from the exchange (otherwise system-calculated). |
| `balances`            | `list[AccountBalance]` | Required         | The account balances (may be empty).                                      |
| `margins`             | `list[MarginBalance]`  | Required         | The margin balances (may be empty).                                       |
| `event_id`            | `UUID4`                | Required         | The event ID.                                                             |
| `ts_event`            | `int`                  | Required         | UNIX timestamp (nanoseconds) when the event occurred.                     |
| `ts_init`             | `int`                  | Required         | UNIX timestamp (nanoseconds) when the object was initialized.             |
| `total_only_balances` | `list[Money]`          | `[]`             | Reported totals with unavailable free and locked components.              |
| `info`                | `dict`                 | `None`           | Venue-specific account data with no typed field (empty dict when unset).  |

## Totals-only balances

An adapter can report `total_only_balances` when it knows a currency's total but cannot establish
its free and locked components. This representation is supported only by `MarginAccount` with
`calculate_account_state=False`; it does not change the `AccountBalance` invariant.

`balance_total()` and `balances_total()` combine complete and totals-only balances. For a
totals-only currency, `balance()`, `balance_free()`, and `balance_locked()` return `None`, and
the corresponding batch queries omit that currency. Unknown components are not zero or buying power.

Applying either representation removes the other representation for the same currency. Omitted
currencies retain their previous state, while explicit zeros update it. Duplicate currencies within
either collection or overlap between the collections reject the event before any balances, margins,
or event history change. A representation switch remains a state change even when the total is equal.

Old serialized records without `total_only_balances` read it as an empty collection. Native capital
checks reject orders without explicit reduce-only or validated full-position-exit intent for accounts
carrying totals-only balances. An opposite-side order is not proof of reduction in every OMS mode.
Quantity increases and price or trigger-price changes are also rejected. This representation does
not authorize trading or delegate capital checks to an adapter.

## Example

Account state is normally consumed through the `Portfolio` rather than a dedicated handler:

```python
from nautilus_trader.model import Venue

# Account state is tracked by the portfolio; query it by venue
account = self.portfolio.account(venue=Venue("BINANCE"))
self.log.info(f"Account state: {account}")
```

The result is detached from the Portfolio. Mutating the returned account does not change the
authoritative account held by the engine.

## Related guides

- [Events](index.md) - Event categories and dispatch.
- [Accounting](../accounting.md) - Account types, balances, and margin models.
- [Portfolio](../portfolio.md) - How account state feeds exposure and balance tracking.
