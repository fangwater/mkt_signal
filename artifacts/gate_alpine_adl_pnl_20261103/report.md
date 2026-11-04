# Gate ALPINE ADL loss attribution

Environment: gate_fr_arb01. Read-only exchange queries. Valuation: 2026-11-03T14:13:55Z.

## Correction: observed account equity is authoritative

The earlier 6,294.25 USDT spot-only small-trade-price calculation is **not an
observed account NAV loss**. It must not be used to claim the account fell by
that amount or to answer the incremental ADL loss question.

The deployed account monitor records Gate's `actual_eq_usd` directly from
`unified.assets.e`; NAV's FR overview reads the same `actual_equity_usd` fact.
Source code: mkt_parsers/src/account_event/gate_account_event_parser.rs
(`parse_unified_assets`) and crypto_nav_manager/src/server/fr_overview.rs
(`snapshot_risk`).

| UTC time | Whole-account exchange equity, USD |
| --- | ---: |
| 2026-10-30 10:15:37 | 753878.65 |
| 2026-10-30 10:16:39, ADL time at second resolution | 754789.72 |
| 2026-11-03 14:21:47 | 752425.94 |

Observed change from the ADL-time reading is
-2363.78 USD.
From the reading one minute earlier, the change is
-1452.71 USD.
Equity moved rapidly around the event. Neither comparison isolates ALPINE;
both include every other asset, funding, fees and account changes. An exact
whole-account loss bridge requires reconciling those additional flows.

The PostgreSQL strategy_snapshots lookup did not supply contemporaneous data:
its latest stored snapshot for this strategy was 2026-10-11. It was excluded
from the current/ADL equity comparison. account_equity_history.json contains
the actual account-monitor history used above.



ADL occurred 2026-10-30 10:16:39.7052–10:16:40.7977 UTC (18:16:39–40 Beijing).
Four fills bought back 8,506 shorts; 2,576 shorts remained. Spot inventory did
not trade after ADL. On 2026-11-03 13:45:12–13:45:43 UTC, 77 fills restored
8,500 shorts. The extra six ADL contracts mainly removed an existing small
overhedge; current net inventory is +0.718616 ALPINE.

## Incremental loss caused by ADL, relative to retaining the original shorts

For the main 8,500 units:

| Component | USDT loss |
| --- | ---: |
| ADL buyback VWAP 0.4348 minus replacement short VWAP 0.3026980117647058823529411765 | 1122.8669 |
| Replacement short fees | 0.51458662 |
| Funding difference (ADL avoided a net funding payment) | -40.0189962000 |
| Total incremental loss | 1083.3624904200 |

Funding uses the 12 factual account-book settlements on the unchanged remaining
2,576 shorts, scaled by 8,500 / 2,576. Actual retained-short funding summed to
-12.1281099072 USDT, so the missing shorts avoided a net payment of 40.0189962000 USDT.
Including all 8,506 ADL units and marking the six-unit position difference at
0.3025 gives 1084.1658417168 USDT incremental loss.

ADL itself booked -970.010401579562 USDT realized futures
PnL versus the historical entry price. This is already represented in the
incremental comparison; adding it again double counts the same ADL settlement.

## Illustrative spot-trade-price calculation, not an account equity loss

The final public spot trade preceding ADL was at 1.1215, trade
2955582, timestamp 1793355399493.638000 milliseconds.
At valuation the spot ticker last price was 0.381. Therefore the
two-price arithmetic for 8,500 units is:

8,500 × (1.1215 − 0.381) = 6294.2500 USDT.

This is not evidence of a 6,294.25 USDT account equity loss. It includes the
change of a large spot premium to perpetuals, and applies small-trade prices
to the whole position. It is not all incremental ADL damage and is not additive to the
futures counterfactual loss. Small public trade prices do not prove that the
entire 8,500-unit spot inventory could have been sold at those prices.

Evidence: exchange_initial.jsonl, exchange_followup.jsonl,
exchange_valuation.jsonl, exchange_event_prices.jsonl. analyze.py verifies
fill identities across both trade endpoints, fees against account-book entries,
all four ADL realized-PnL identities, and absence of subsequent spot trades.
