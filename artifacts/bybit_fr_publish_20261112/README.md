# Bybit FR arb01 publication, 2026-11-12

At 15:34 UTC, JP `bybit_fr_arb01` has all eight services running, including
trade_signal. The seven Rust binaries were built locally from synchronized
`arbmm 87bf37fb`, uploaded to explicit temporary paths, SHA-256 checked and
atomically installed. Collateral preflight subsequently required the small
script fix `8ebec272`, described below. No production-host Cargo build ran.

Before publication, a read-only /proc audit of SG's `bybit_fr_arb01` and
`bybit-intra-arb01` found no processes. JP's original eight processes and config
fingerprints were recorded; logs and rollback binaries are in
`~/bybit_fr_arb01/backups/bybit_fr_publish_87bf37fb_20261112/`.
All starts/stops used environment-local wrappers. Source IPs remain
172.31.35.232/172.31.35.233; both passed independent signed account-info reads.
The actual engine and account-monitor sockets use those addresses, and both WS
connections authenticated. Persist remains on CPU15, all other services on
0-5,48-95. Config fingerprints and 86 recorded other FR/Bybit market-data
process identities, binary hashes where applicable, and CPU masks stayed unchanged.

No orders were present before stopping or afterward. The env-local cancellation
helper ran in dry-run and execute modes, with an empty plan. Separate reads
checked active linear orders plus all five supported spot order filters.

Only three futures taker orders were submitted while all env processes were
stopped, after installation and before new pre_trade initialized:

| Symbol | Futures action | Quantity | Reduce-only | Filled USDT | Final net USDT |
| --- | --- | ---: | --- | ---: | ---: |
| VELO | BUY | 2150 | yes | 6.895050 | -0.005406 |
| SLX | SELL | 186 | no | 6.173540 | 0.023313 |
| DYDX | SELL | 70.2 | no | 6.164262 | 0.008700 |

Three orders/four taker fills total 19.232852 USDT; fees 0.00346192 USDT.
Smallest submission gap was 5.215 seconds; all orders were below 100U.
Each request was persisted before submission and confirmed through order status,
executions and refreshed actual quantities before another order was sent.
All non-stablecoin spot inventory/borrow principal, including BNB, was preserved.
No wallet transfer, leverage or trading-parameter change was submitted.

All priced non-BNB per-asset residuals were below 5U before signal restoration;
their absolute sum was 47.98658U, with ZRO largest at 3.95694U. These figures
cover many small historical holdings and are not a claim of a zero account net.
EUR 0.00485557 (wallet USD reference about 0.0054) and HFT 20.15738603 had no
matching priced USDT futures route and were preserved. HFT had no spot ticker
in the public snapshot either. Delist retains these as ignored spot tails.

The first pre_trade start was blocked by missing BSB in Bybit's bulk collateral
list. The official currency-specific request returned BSB with marginCollateral
and collateralSwitch both true; borrowable was false. Its active spot/futures
markets and the existing forward-only configuration were independently checked.
The guard now individually queries only absent currencies, retaining strict
identity, response and boolean validation. It never infers enabled collateral
from absence. The live env helper exactly matched the old repository helper,
was backed up, then replaced with the verified `8ebec272` artifact. Forty
collateral/preflight tests passed, including missing bulk BSB, enabled/disabled
switches and malformed/missing targeted responses. Dry-run and actual startup
checks passed with disabled=[], enabled=[], errors=[]; no collateral switch or
symbol-list change was needed.

Seven base services were checked first, with fresh snapshots, no orders and
holdings identical to the aligned state; signal was then restored. Initially
HMSTR/HNT/HOLO/QTUM marks were missing after the subscriber restart. Incoming
market data populated them without manual injection or publisher restart.
At 15:34 UTC Delist was complete/ok; sampled recent logs had no panic,
missing-factor, hedge_stalled, ERROR or exchange-dispatch rejection.

A separate version audit found Gate FR01-04 still running pre_trade SHA
3351318bf02d219073056f8ea4bbbc503dc73e2f8e5b4b9449b4b9af6f85b1e8,
which matches their committed `293f6352` deployment receipt. They do not include
`90b24c42`'s exception-only hedge guard. This Bybit operation did not update
those environments; they must not be described as already on the same guard.

See `verification.json` for hashes, process/socket evidence and per-order
results. Full snapshots and confirmations remain in ignored local
`data/ops/bybit_fr_publish_20261112/` and remote
`/tmp/bybit_fr_publish_87bf37fb_20261112/`.
