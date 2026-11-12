# Binance FR 02/03/04 publish and futures alignment

2026-11-12 UTC. User authorized a full republish and small taker alignment,
excluding BNB. Built the seven binaries locally from synchronized, clean
`arbmm b91dffca`, then uploaded exact artifacts, verified SHA-256 and atomically
installed them. Later commits in the same workspace concerned Delist only and
were not part of this trading-binary publication.

All 24 services, including three trade signals, were restored with env-local
scripts. Logs and previous binaries are preserved in each environment under
`backups/binance_fr_publish_align_b91dffca_20261112/`. Config fingerprints and
source IP `172.31.35.231` stayed unchanged. Persist remains on CPU15;
config servers 02/04 were restored to their original CPU0-5 mask; remaining
services retain their original housekeeping masks. Gate/Bitget's 56 recorded
processes stayed unchanged. Delist independently changed PID during this
operation; it is explicitly not included in the unchanged-process assertion.

Stopped engines/signals and remaining services before manual operations.
FR04 retained 20 margin orders on CTK/GPS; cancellation was followed by strict
independent queries of margin, UM and conditional orders. All were empty.
FR02/03 had no orders after stopping. Snapshots cover all nonzero holdings;
no held multiplier contract or unmapped asset was present.

| Account | Futures taker orders | Filled USDT | Symbols | Residual absolute sum, excluding BNB | Largest residual asset |
| --- | ---: | ---: | --- | ---: | ---: |
| 02 | 6 | 494.85409 | FF, MUBARAK | 18.67568 | 4.17811 |
| 03 | 1 | 7.38100 | AWE | 8.31917 | 3.94686 |
| 04 | 3 | 113.073286 | PARTI, MANTA, CVX | 39.43458 | 4.93540 |

All 10 orders were SELLs and fully confirmed through order queries, taker-fill
records and refreshed account quantities. Total 615.308376 USDT; fees
0.09608783 USDT. Largest order 98.93218 USDT, below the requested 100U.
The submission scheduler waited at least five seconds between attempts;
exchange completion timestamps differ by as little as 4.995 seconds due to
request/completion timing. No spot order, BNB order, wallet transfer or leverage
change was submitted. Non-stablecoin spot principal/borrowed quantities and BNB
futures were checked after each fill; interest accrual was allowed. Sub-5U
per-asset tails were intentionally retained.

The figures above describe the stopped-account alignment, not a guarantee of
zero exposure after trading resumes. Each environment was independently
checked again for unchanged inventory, no orders and sub-5U non-BNB residuals
before enabling its signal. At 15:01 UTC all 24 process hashes/CPU masks and
fresh snapshots passed; all three Delist account reports were complete/ok.
No sampled panic, missing-factor or hedge_stalled event was found.

After resuming, AWE on 03 received new spot fills. The first 1635-unit futures
SELL filled at 15:00:33; another 841-unit spot fill produced an active futures
SELL. This was new strategy activity after the manual 121-unit alignment.
At 15:02:52 UTC, further fills left 1826.39404 AWE (111.37351U) net long,
with two live futures SELLs totaling 1827 units (841 + 986). This was not
zero live exposure; the remaining quantity had corresponding maker orders.

Rejection cooldowns were already implemented in the published binary and are
active. A 51015 rejection starts a 60-second account-wide USDT-borrow cooldown:
BUYs needing borrowed USDT are blocked, while buys covered by free cash remain
eligible. Retrying borrowing also requires a newer complete capacity snapshot.
A 51042 rejection starts a cross-symbol margin-opening cooldown for both sides,
backing off 1, 2, 4, 8, then at most 15 minutes. In-flight rejects from the same
burst do not repeatedly multiply that frequency delay. These checks apply to
margin opening and do not impose this cooldown on futures hedges.

Live logs confirm 51042 at 15:01:23 with retry after 15:02:23, at 15:02:24 with
retry after 15:04:24, and at 15:09:03 with retry after 15:13:03. The sampled
interiors of the first two cooldown windows contain no new margin-opening
order creation. Multiple rejects from orders already submitted before the
first response, and a new rejection after cooldown expiry, do not by themselves
show failed cooldown behavior. The earlier description of these observed
rejects as an unfixed cooldown problem was inaccurate.

The 15:01 verification counts 14/40 occurrences of 51015/51042 in each of the
engine and pre-trade logs; these represent propagated events, not twice as
many failed orders. FR04's sampled ERROR records were local position-limit
rejections of new opening orders; processes stayed alive.

Machine-readable build hashes, runtime identities, residuals and checks are in
`verification.json`. Full read-only exchange snapshots, per-order requests,
confirmations and fills are retained in ignored local
`data/ops/binance_fr_publish_align_20261112/` and remote
`/tmp/binance_fr_publish_align_b91dffca_20261112/`.
