# PAPI UM Account V2 Verification

Verification date: 2026-09-13. Scope: choosing the UM account-detail source and checking current
position coverage. This follows the account-version questions in [RESEARCH.md](RESEARCH.md).

## Result

Prefer `/papi/v2/um/account` for UM account observations. The observed V2 responses retain every
nonzero position found in the comparison sources and omit zero positions. Use the currently
supported `/papi/v1/um/positionRisk` endpoint for position-report fields that V2 does not supply,
including `entryPrice`. V2 is not a field-compatible replacement for V1 account detail.

This result supports the source selection for the observed one-way account. It does not establish
complete coverage across all account states, accept missing-as-flat behavior in the adapter, or
enable LiveNode startup. No production code or existing tests were changed.

## Evidence collected

A fresh authenticated collection ran from 05:43:12 to 05:43:18 UTC. All seven requests were GETs,
returned HTTP 200, and completed without retries. The request sequence was:

1. `/papi/v1/um/positionSide/dual` to confirm one-way mode.
1. `/papi/v2/um/account` before the comparison reads.
1. `/papi/v1/um/account` as an acceptance reference.
1. `/papi/v1/um/positionRisk` without a symbol filter.
1. `/papi/v1/um/openOrders` without a symbol filter.
1. `/papi/v1/um/algo/openAlgoOrders` without a symbol filter.
1. `/papi/v2/um/account` after the comparison reads.

Requests used the existing acceptance credentials, verified TLS, a ten-second request timeout,
a ninety-second collection deadline, and an eight-MiB response limit. Redirects and implicit
environment proxies were disabled. No orders, transfers, borrowing, or account changes were
requested. Raw responses and request metadata remain in owner-only local files outside the
repository. Captures contain neither API keys nor signed request URLs.

| Observation               | V1 account | V2 account | Position risk |
| ------------------------- | ---------- | ---------- | ------------- |
| Position rows             | 897        | 1          | 1             |
| Nonzero positions         | 1          | 1          | 1             |
| Asset rows                | 11         | 11         | Not supplied  |
| `entryPrice` supplied     | Yes        | No         | Yes           |
| Response body size, bytes | 304,977    | 2,707      | 295           |

Exact `Decimal` comparisons established:

- Nonzero position identities and signed quantities agree across V1, both V2 responses, and
  `positionRisk`.
- All 896 V1 rows omitted by V2 have zero `positionAmt`.
- The common position's `updateTime` agrees across the four responses.
- V1 and V2 asset identities and `crossWalletBalance` amounts agree.
- `positionRisk.entryPrice` agrees with the V1 account position's entry price.
- Both current ordinary-order and algo-order queries return empty arrays.

Two earlier local acceptance captures, starting at 02:39:40 and 02:46:00 UTC, were also compared.
Each has 897 V1 position rows, one V2 position row, matching nonzero identities and quantities, and
896 omitted zero rows. Repeated agreement is evidence for these samples, not an atomic snapshot
guarantee across independent endpoints.

## Documentation and field checks

The current [official account reference][account-api] was read in the browser. It lists V2 at
`/papi/v2/um/account`, with `timestamp` and optional `recvWindow` query parameters; no symbol or
pagination parameter is listed. Its V2 `positions` description still says that all market symbols
are returned. That description does not match the observed omission of zero rows.

The earlier research records the [2024-08-23 change-log entry][change-log] as describing V2 results
as symbols with positions or open orders. This follow-up did not independently reload that entry
successfully. The current reference, SDK 69.2.1 models, and authenticated responses were sufficient
to verify the field differences below.

The V2 position fields observed are `symbol`, `positionSide`, `positionAmt`, `updateTime`,
`initialMargin`, `maintMargin`, `unrealizedProfit`, and `notional`. V1 additionally supplies
`entryPrice`, `leverage`, position/open-order initial-margin components, and other details.
The current [position-report parser](src/reports/parse.rs) requires a valid entry price for an open
position, so replacing its input with V2 alone would lose required information.

## Validation limits and implementation consequences

The comparison harness passed eight offline checks covering missing envelopes, omitted zero and
nonzero positions, exact quantity differences, duplicate identities and JSON members, malformed
amounts, and unsupported position sides. These checks validate the harness; they are not adapter
regression tests or proof of exchange coverage.

The authenticated account had one nonzero one-way position and no open orders. The collection did
not exercise an entirely flat account, multiple simultaneous nonzero positions, a zero position
with working ordinary/algo orders, a position closing during collection, hedge mode, or recovery
after a stream gap. No trading operations were used to manufacture these cases.

The next implementation should:

- Use V2 as the primary UM account-observation source and `positionRisk` for required position
  details, matching by account, symbol, and position side and checking signed quantities.
- Keep V1 as an acceptance reference; this result does not require unconditional dual-version
  reads in production.
- Define and test the complete V2 snapshot contract before inferring flat positions from omitted
  rows. Query success, validated envelope and mode, instrument coverage, freshness, and execution
  synchronization are required independently. Missing rows after a failed or partial read remain
  unavailable.
- Preserve errors for contradictory sources, invalid required fields, and unsupported states.
  A generation number and matching quantities do not make REST responses atomic.

The existing explicit-position-coverage checks, account-projection gate, and LiveNode startup gate
remain in place. Native balance and PM purchasing-power semantics are outside this verification.

[account-api]: https://developers.binance.com/en/docs/catalog/advanced-trading-derivatives-trading-portfolio-margin/api/rest-api/account#get-um-account-detail-v2
[change-log]: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/change-log#2024-08-23
