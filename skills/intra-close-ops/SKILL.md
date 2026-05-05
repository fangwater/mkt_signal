---
name: intra-close-ops
description: Operate the repo-local intra spot/futures arbitrage close flows. Use when asked to cancel all orders, flatten futures exposure, or fully exit same-exchange intra arb environments such as okex-intra-arb01, gate-intra-arb02, bybit-intra-arb03, bitget-intra-arb01, or binance-intra-*; also use when choosing between scripts/close_*_intra_full_exit.sh and scripts/close_intra_* entrypoints.
---

# Intra Close Ops

## Overview

Use the repository's existing close wrappers for same-exchange spot/futures intra arbitrage. Prefer the highest-level script matching the requested outcome; preserve dry-run behavior unless the user explicitly asks to execute live orders.

If the request is about pre-close risk cleanup or leverage alignment for the whole intra universe, include the dedicated leverage helper:

- `scripts/set_intra_futures_leverage.py --env-name <exchange>-intra-<suffix> --all-trade-symbols --leverage <n> [--execute]`
- That helper reads the intra `fwd` + `bwd` Redis lists and can also target a single symbol with `--symbol`.

## Choose The Command

- Cancel orders only: use `scripts/close_intra_all_orders.sh --env-name <exchange>-intra-<suffix>`.
- Flatten futures exposure only: use `scripts/close_intra_futures_exposure.sh --env-name <exchange>-intra-<suffix>`.
- Close/exit an intra environment: use `scripts/close_intra_full_exit.sh --env-name <exchange>-intra-<suffix>`.
- Batch set futures leverage across all intra trade symbols: use `scripts/set_intra_futures_leverage.py --env-name <exchange>-intra-<suffix> --all-trade-symbols --leverage <n> [--execute]`.
- Use exchange convenience wrappers when the target is the default `arb01` env, or pass `--env-name` to override:
  - `scripts/close_okex_intra_full_exit.sh` defaults to `okex-intra-arb01`.
  - `scripts/close_gate_intra_full_exit.sh` defaults to `gate-intra-arb01`.
  - `scripts/close_bybit_intra_full_exit.sh` defaults to `bybit-intra-arb01`.
  - `scripts/close_bitget_intra_full_exit.sh` defaults to `bitget-intra-arb01`.

## Modes

- Default behavior is dry-run. Add `--execute` only after the user asks for real cancellation or orders.
- `--full-exit` maps to `--mode full-exit`: cancel orders, then close both spot and futures legs to zero.
- Without `--full-exit`, exchange wrappers use their default alignment mode:
  - OKX wrapper uses `--mode align-exposure`.
  - Gate, Bybit, and Bitget wrappers use `--mode align-futures-to-spot`.
- Pass common filters through to the underlying script when requested: `--symbol`, `--skip-assets`, `--min-net-usdt`, `--skip-cancel`, `--td-mode`, `--timeout`.
- For leverage cleanup, prefer `--all-trade-symbols` over dashboard-only selection when the user says "all币对", "fwd+bwd", or "全量". Use `--source dashboard` only when the request is explicitly based on current exposure.

## Env Rules

- Env names must match `<exchange>-intra-<suffix>` and resolve `env.sh` from `$HOME/<env-name>/env.sh` unless `--env-dir` is provided.
- Treat `arb01`, `arb02`, and `arb03` as suffixes; do not silently substitute `arb01` when the user names another suffix.
- If the current working directory already matches `<exchange>-intra-<suffix>`, the generic scripts can infer the env name, but an explicit `--env-name` is clearer for live operations.

## Binance Caveat

Binance intra full-exit is not implemented in `scripts/close_intra_full_exit.sh`. For Binance STANDARD intra environments, use the existing pieces instead:

```bash
scripts/close_intra_all_orders.sh --env-name binance-intra-<suffix> [--execute]
scripts/close_intra_futures_exposure.sh --env-name binance-intra-<suffix> [--execute]
```

Do not present Binance as supporting one-step spot+futures full exit unless the repo scripts have changed and you have verified that support locally.

## Examples

```bash
# Dry-run Gate arb02 alignment close
scripts/close_gate_intra_full_exit.sh --env-name gate-intra-arb02

# Execute Gate arb02 full exit
scripts/close_gate_intra_full_exit.sh --env-name gate-intra-arb02 --full-exit --execute

# Execute OKX arb03 for one asset
scripts/close_okex_intra_full_exit.sh --env-name okex-intra-arb03 --symbol BTC --execute

# Dry-run batch leverage alignment for all intra trade symbols
scripts/set_intra_futures_leverage.py --env-name bitget-intra-arb01 --all-trade-symbols --leverage 5
```
