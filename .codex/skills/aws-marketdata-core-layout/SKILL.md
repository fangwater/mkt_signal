---
name: aws-marketdata-core-layout
description: AWS market-data deployment layout for mkt_signal. Use when deploying or documenting the current AWS market-data host core binding for spread_pbs and depth_pub, especially Binance/Gate/OKEx/Bitget market-data publishers and the live Binance depth override.
---

# AWS Marketdata Core Layout

## Scope

Use this skill in `/home/ubuntu/crypto_mkt/mkt_signal` when redeploying the current AWS market-data host.

This layout pins market-data processes to dedicated cores where possible. The
current live host has a Binance depth override on CPU28 because CPU15 is
reserved for persist managers and CPU11 is occupied by Binance futures
bookticker `spread_pbs`.

- 5 `spread_pbs` processes:
  - Binance split into 2 processes: `binance-margin` and `binance-futures`.
  - Gate, OKEx, and Bitget each run as one `*-both` process.
- 3 `depth_pub` processes:
  - Bitget and Gate each run as one `*-both` process.
  - Binance runs one `binance-both` depth publisher.
  - OKEx depth is intentionally not started in this layout.

## Core Map

| CPU | Process | Venue dir | Env override | pmdaemon name |
| --- | --- | --- | --- | --- |
| 8 | `spread_pbs` | `~/spread_pbs/binance-margin` | `SPREAD_PBS_CORE=8` | `spp_bn_mg` |
| 9 | `spread_pbs` | `~/spread_pbs/binance-futures` | `SPREAD_PBS_CORE=9` | `spp_bn_fu` |
| 10 | `spread_pbs` | `~/spread_pbs/gate-both` | `SPREAD_PBS_CORE=10` | `spp_gt_bo` |
| 11 | reserved / Binance futures bookticker live override | `~/spread_pbs/binance-futures` | `SPREAD_PBS_CORE=11` | `spp_bn_fu_bookticker` |
| 12 | `spread_pbs` | `~/spread_pbs/bitget-both` | `SPREAD_PBS_CORE=12` | `spp_bg_bo` |
| 13 | `depth_pub` | `~/depth_pub/bitget-both` | `DEPTH_PUB_CORE=13` | `dp_bg_both` |
| 14 | `depth_pub` | `~/depth_pub/gate-both` | `DEPTH_PUB_CORE=14` | `dp_gt_both` |
| 15 | `persist_manager` only | FR/intra envs | `PERSIST_MANAGER_CORE=15` | multiple |
| 28 | `depth_pub` live override | `~/depth_pub/binance-both` | start with `--core 28` | `dp_bn_both` |

Treat the table as authoritative for this AWS host unless the user explicitly updates the topology.
Do not start Binance `depth_pub` on CPU15; it is reserved for persist managers.
If CPU11 is occupied, use a quiet helper/overflow core such as CPU28 and run
Binance margin+futures together as `dp_bn_both`.

## Deployment Notes

Check worktree state before changing repo scripts:

```bash
git status --short
git diff --stat
```

Deploy the selected venues from the repo checkout:

```bash
bash scripts/spread_pbs/deploy_spread_pbs.sh \
  --venue binance-margin \
  --venue binance-futures \
  --venue gate-both \
  --venue okex-both \
  --venue bitget-both

bash scripts/deploy_depth_pub.sh \
  --venue bitget-both \
  --venue gate-both \
  --venue binance-both
```

The start scripts read per-venue `env.sh` files:

- `~/spread_pbs/<venue>/env.sh` with `export SPREAD_PBS_CORE='<cpu>'`
- `~/depth_pub/<venue>/env.sh` with `export DEPTH_PUB_CORE='<cpu>'`

Write or preserve only the relevant core override in each deployed venue. Do not hard-code credentials in repo files. OKEx `spread_pbs` still needs `OKX_API_KEY`, `OKX_API_SECRET`, and `OKX_PASSPHRASE` available in its venue `env.sh` for SBE handshake.

## Startup Order

Start each process from its deployed venue directory:

```bash
cd ~/spread_pbs/binance-margin && ./scripts/start_spread_pbs.sh
cd ~/spread_pbs/binance-futures && ./scripts/start_spread_pbs.sh
cd ~/spread_pbs/gate-both && ./scripts/start_spread_pbs.sh
cd ~/spread_pbs/bitget-both && ./scripts/start_spread_pbs.sh

cd ~/depth_pub/bitget-both && ./scripts/start_depth_pub.sh
cd ~/depth_pub/gate-both && ./scripts/start_depth_pub.sh
cd ~/depth_pub/binance-both && pmdaemon delete dp_bn_fu || true
cd ~/depth_pub/binance-both && pmdaemon delete dp_bn_both || true
cd ~/depth_pub/binance-both && pmdaemon start ./depth_pub \
  --name dp_bn_both \
  --cwd "$HOME/depth_pub/binance-both" \
  --env RUST_LOG=info \
  -- --venue binance-margin --venue binance-futures --core 28
```

Before starting a `*-both` `spread_pbs`, stop conflicting single-side processes for the same exchange. The start script checks for conflicts, but do not rely on it as the only guard when operating live deployments.

## Verification

After startup, verify the market-data process names exist and are pinned to the
expected live cores:

```bash
pmdaemon list
ps -eo pid,psr,comm,args | rg 'spread_pbs|depth_pub'
```

Expected process names:

- `spp_bn_mg`
- `spp_bn_fu`
- `spp_gt_bo`
- `spp_bg_bo`
- `dp_bg_both`
- `dp_gt_both`
- `dp_bn_both`

If a process is missing or on a different CPU, inspect that venue's `env.sh` first, then restart only that venue.
