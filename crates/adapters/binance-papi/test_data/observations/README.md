# Account observation fixtures

These fixtures are synthetic offline parser inputs, not authenticated captures. Field names follow
the Portfolio Margin account endpoints and the locally inspected `binance-sdk` 69.2.1 models.
The field inventory and official references are recorded in [RESEARCH.md](../../RESEARCH.md).

- `balances.json` exercises mixed collateral, signed wallet/PnL values, separately retained debt,
  and unavailable fields. Its numbers do not establish any accounting identity or debt formula.
- `account.json` exercises USD precision beyond cents, an unknown status, unavailable capacity,
  and preservation of an unknown field in the raw response.
- `um_account.json` exercises asset components and a sparse V2-shaped short position with a zero
  venue timestamp. It does not establish endpoint coverage or account position-mode support.

Inline test cases cover malformed values, precision limits, response shapes and lifecycle failures.
No fixture establishes economic mapping, successful authentication, or live compatibility.
