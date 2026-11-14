# SG Bybit intra arb01 / arb02 redeploy and neutralization

Verified 2026-10-16 07:43 UTC. Both environments were republished and all seven
environment processes remain stopped, as requested. No startup smoke test was run.

| Environment | Running processes | Open spot/linear orders | Filled hedge orders | Signed net exposure (USDT) | Sum of absolute per-asset exposure (USDT) | Largest per-asset residual (USDT) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| bybit-intra-arb01 | 0 | 0 | 15 | +3.30 | 31.61 | 4.98 |
| bybit-intra-arb02 | 0 | 0 | 3 | +0.14 | 8.67 | 4.13 |

Residuals are constrained by minimum order values and quantity precision. Both
final dry-runs produced no further valid direct adjustment orders. Borrowing
remains outstanding, so interest and market prices can change these figures.
USDC linear, inverse and option position queries returned no positions for either
account. Spot and USDT linear exposure covered all held assets, including assets
outside the strategy's Redis symbol lists.

Exposure uses walletBalance minus spotBorrow plus signed linear base quantity,
consistent with the current [Bybit wallet balance contract](https://bybit-exchange.github.io/docs/v5/account/wallet-balance).
The live wallet identity was checked against equity and unrealised PnL. Accrued
interest was not deducted a second time. Orders reused the deployed source-bound
REST signing and external numeric order-ID helpers, used 0.5% maximum slippage,
and were individually confirmed Filled via order queries. Detailed order intents,
exchange order IDs and fills are retained in the JSONL files.

All six deployed binaries in each environment match the local release manifest:
`b68065b7cb50bef48e111f341ae1551cfea4982336974537716e3dd87fc8c314`.
The local production branch was `arbmm`, synchronized with `origin/arbmm` at
`133bf23f`. Existing uncommitted worktree changes were preserved and included in
the build; provenance.json records the snapshot location and digest.

Both env.sh files, trade_engine.toml, iceoryx2.toml and config files match their
pre-deployment hashes. Existing source binding remains 172.31.7.124 on the order
NIC. Arb01 CPU 12/13/14/15 settings and arb02 housekeeping settings are preserved.
No other Bybit runtime was stopped or republished.

Backups on SG (contain configuration; retained on SG only):

- `/home/ubuntu/bybit_intra_redeploy_backup_20261016/bybit-intra-arb01/runtime-before.tar`
- `/home/ubuntu/bybit_intra_redeploy_backup_20261016/bybit-intra-arb02/runtime-before.tar`

Validation: release build succeeded; SHA-256 staging/install checks passed; both
start-intra --check-only validations passed with every process stopped; final
exchange order queries were empty; final /proc inventory and configuration
hash comparison passed. Config/Viz and both local persistence source listeners
are absent. The shared market-data, MM and bridge services remain running.
