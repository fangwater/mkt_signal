# gate_fr_arb01 ALPINE alignment and ESPORTS audit

Target: `jp-meta-elvpn`, `/home/ubuntu/gate_fr_arb01`, Gate unified account.
Local branch: `arbmm`, synchronized with `origin/arbmm` at
`6ee63160771cebecf807efd263d92ff465f31b22` after fetching on 2026-11-03.

## Executed ALPINE alignment

The user authorized rebuilding the ALPINE futures hedge. Only ALPINE_USDT was
traded; no settings, leverage, Redis values, deployed code, or processes changed.

- Execution: 2026-11-03 13:45:12–13:45:43 UTC.
- Spot inventory: 11,076.718616 ALPINE.
- Futures: -2,576 contracts before, -11,076 afterward; multiplier 1.
- Sold 8,500 contracts through 26 IOC limit orders, approximately 100 USDT each.
- Minimum sell price: 0.3007 USDT, anchored within 1% of the initial best bid.
- Exchange fill verification: 77 fills, 8,500 ALPINE, 2,572.9331 USDT notional.
- VWAP: 0.30269801176470588235 USDT; reported fees: 0.51458662 USDT.
- Remaining net long: 0.718616 ALPINE, approximately 0.218 USDT.
- The dashboard independently showed -11,076 futures and the same residual
  through the final verification at 2026-11-03 13:49:22 UTC.

Receipts: `dry_run.jsonl`, `execution.jsonl`, `verification.jsonl`.
`align_alpine.py` defaults to dry-run. Its preflight deliberately requires the
original 8,400–8,600 ALPINE imbalance and will reject another execution once
aligned. Credentials are read in memory from the target process and are not
included in these files.

## ESPORTS diagnosis (read-only)

The fetched 362 futures fills contained no nonnumeric client identities or ADL
markers. On 2026-11-01 17:01 UTC, the following three normal hedge sell orders
executed at Gate but remained in local Commit state:

| Client order ID | Exchange order ID | Contracts sold | Base quantity |
| --- | --- | ---: | ---: |
| 4016692701285056523 | 256986654921632358 | 15.1 | 1,510 |
| 4016692701285056524 | 256986654921632768 | 54.8 | 5,480 |
| 4016692701285056525 | 256986654921633475 | 54.3 | 5,430 |

After three orphan queries, at 17:02:37–17:02:38 UTC the local orphan path
exhausted its query budget and finalized each order as Expired/Rejected with
zero cumulative fill. ArbHedge released the corresponding pending quantities
and sent additional hedge sells. This omitted 12,420 base units of factual fills
from the strategy ledger. The initial residual, ordinary subsequent trades,
and base-asset fee rebates explain the small difference from the current extra
short inventory.

Current exchange inventory at the audit: spot 1,457,071.745125 ESPORTS;
futures -14,694.7 contracts with multiplier 100 = -1,469,470 ESPORTS;
net -12,398.254875 ESPORTS (approximately -168 USDT).
The dashboard's float quantity differs by about 0.0195 ESPORTS.
The exchange reports `partial_hedged`, unhedged 124 contracts, ADL rank 1.

The logs prove a false local zero-fill terminal decision after query timeouts.
Why the original execution acknowledgments/user-stream fills and later queries
failed to reconcile has not yet been traced further. ESPORTS was not traded.

Evidence: `esports_20261101_hedge_records.log`, `esports_suspect_orders.log`,
and the grouped exchange fills in `verification.jsonl`.

## Later authorized ESPORTS alignment

At 2026-11-03 13:55:59 UTC, after the user separately authorized correction,
bought 124.0 ESPORTS_USDT contracts with multiplier 100 using a reduce-only IOC
limit order. Exchange order 256986654986929807 filled all 12,400 base units.
The short position changed from -14,694.7 to -14,570.7 contracts. Exact net
inventory is now +1.745125 ESPORTS, approximately 0.023 USDT. Gate reported
`full_hedged`, `unhedged_size=0`, and ADL rank 5 afterward. The dashboard still
showed the corrected position at 14:23:46 UTC; its f32 position representation
differs by about 0.0195 ESPORTS from the exchange's exact decimal quantity.

Receipts: `esports_dry_run.jsonl`, `esports_execution.jsonl`.
The later full FR audit and Delta enable receipts are in
`../fr_full_audit_20261103/README.md`.
