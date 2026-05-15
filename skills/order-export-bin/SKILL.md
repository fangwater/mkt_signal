---
name: order-export-bin
description: Use when the user asks about the `order_export` binary in this repo: how to install it, how its CLI arguments and default paths work, and how to run it for MM/FR envs such as `binance_mm_alpha` and `okex_mm_alpha`. Focus on the real repo entrypoints `src/bin/order_export.rs` and `scripts/deploy_order_export.sh`.
metadata:
  short-description: order_export bin usage for MM/FR envs
---

# Order Export Bin

Use this skill when the user wants the current repo's `order_export` binary usage.

Primary sources in this repo:
- `src/bin/order_export.rs`
- `scripts/deploy_order_export.sh`

## What `order_export` does

- Reads RocksDB from `persist_manager`.
- Exports one UTC day or one explicit UTC time window into a local output directory.
- Produces exactly three parquet files:
  - `uniform_orders.parquet`
  - `order_updates_unmatched.parquet`
  - `trade_updates_unmatched.parquet`

## Rules

- Prefer commands and paths that are verifiably present in this repo.
- Treat timestamps as UTC unless the user explicitly asks for local-time conversion.
- Prefer `--date YYYY-MM-DD` for day-level export.
- Use `--start` and `--end` only for incident windows that need narrower slicing.
- If the user is in an env directory like `~/binance_mm_alpha` or `~/okex_mm_alpha`, let `order_export` infer `env_name` from `cwd`.
- `base_dir` must be absolute.
- `env_name` must match one of: `<exchange>_<mm|fr|intra>_<suffix>` (underscore form, e.g. `binance_mm_alpha`, `okex_intra_hf01`), `<exchange>-<mm|fr|intra>-<suffix>` (dash form, e.g. `binance-intra-arb01`, `okex-intra-arb01`), or `<open_ex>-<hedge_ex>_cross_<suffix>` (cross). Separator must be consistent within an `env_name` — mixed forms like `binance_intra-arb01` are rejected. Normalized to lowercase.

## Install

Preferred install path from this repo:

```bash
bash scripts/deploy_order_export.sh
```

That script installs:

```text
/home/$USER/order_export/bin/order_export
```

It also expects:

```bash
export ORDER_EXPORT_BASE_DIR=/home/$USER
```

## Default path resolution

`order_export` resolves paths like this:

- `--base-dir` or `ORDER_EXPORT_BASE_DIR` or `PERSIST_EXPORT_BASE_DIR`
- `--env-name` or `ORDER_EXPORT_ENV_NAME` or `PERSIST_EXPORT_ENV_NAME`
- input dir default:
  - `<base_dir>/<env_name>/data/persist_manager`
- output root default:
  - current working directory

If the user runs from `~/binance_mm_alpha`:

```bash
cd ~/binance_mm_alpha
/home/$USER/order_export/bin/order_export --date 2026-03-25
```

Then output defaults to:

```text
~/binance_mm_alpha/20260325
```

For explicit ranges:

```text
<cwd>/20260325T010203.000000Z__20260325T020304.000000Z
```

## Standard workflow

1. Confirm the env directory exists and contains `data/persist_manager`.
2. `cd` into the env directory if you want `env_name` inference and output beside the env.
3. Run `order_export` with either `--date` or `--start` plus `--end`.
4. The output directory will contain the exported parquet files for that window.

## Common commands

`binance_mm_alpha` day export:

```bash
cd ~/binance_mm_alpha
/home/$USER/order_export/bin/order_export --date 2026-04-03
```

`okex_mm_alpha` explicit UTC window:

```bash
cd ~/okex_mm_alpha
/home/$USER/order_export/bin/order_export \
  --start 2026-04-16T13:18:24Z \
  --end 2026-04-16T13:18:34Z
```

intra arb env (dash form) day export:

```bash
cd ~/okex-intra-arb01
/home/$USER/order_export/bin/order_export --date 2026-05-12
```

Run from outside the env directory:

```bash
/home/$USER/order_export/bin/order_export \
  --base-dir /home/$USER \
  --env-name binance_mm_alpha \
  --date 2026-04-03
```

Override input/output explicitly:

```bash
/home/$USER/order_export/bin/order_export \
  --base-dir /home/$USER \
  --env-name okex_mm_alpha \
  --input-dir /home/$USER/okex_mm_alpha/data/persist_manager \
  --output-root /tmp/order_export_debug \
  --date 2026-04-16
```

## Output naming

- `--date 2026-04-03` writes to `20260403`
- `--start ... --end ...` writes to `<start>__<end>` in UTC compact form

## Known repo facts

- `scripts/deploy_order_export.sh` exists in this repo and is the canonical install helper.
- `order_export` accepts either `--date` or `--start` with `--end`, but not both modes together.
- default input dir is `<base_dir>/<env_name>/data/persist_manager`
- default output root is the current working directory
