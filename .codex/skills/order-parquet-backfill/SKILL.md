---
name: order-parquet-backfill
description: Import order_export parquet backfill files into mkt_signal persist sync center RocksDB. Use when the user needs a one-time historical migration or repair for order parquet data older than the normal 24h sync repair window, especially with data dirs containing uniform_orders.parquet, order_updates_unmatched.parquet, and trade_updates_unmatched.parquet.
---

# Order Parquet Backfill

## Workflow

Use this skill in `/home/ubuntu/crypto_mkt/mkt_signal` unless the user gives another checkout.

1. Check the worktree before touching files:

```bash
git status --short
```

2. Build or check the helper:

```bash
cargo check -p persist_manager --features runtime --bin order_parquet_backfill
cargo build --release -p persist_manager --features runtime --bin order_parquet_backfill
```

3. Confirm inputs before running an import:

- `--data-dir`: directory containing standard order_export parquet files.
- `--source-id`: explicit persist sync source id such as `okex-intra-arb01`, `binance-intra-arb01`, or `bybit-intra-arb01`.
- `--db-dir`: center RocksDB path, normally `data/persist_sync_center`.
- `--cf`: optional repeatable filter. Omit to import all standard CF files.

Standard filenames:

- `uniform_orders.parquet`
- `order_updates_unmatched.parquet`
- `trade_updates_unmatched.parquet`

4. Run the helper:

```bash
target/release/order_parquet_backfill \
  --data-dir /path/to/order_export_dir \
  --source-id okex-intra-arb01 \
  --db-dir data/persist_sync_center
```

For a single CF:

```bash
target/release/order_parquet_backfill \
  --data-dir /path/to/order_export_dir \
  --source-id okex-intra-arb01 \
  --db-dir data/persist_sync_center \
  --cf uniform_orders
```

## Behavior

- Writes to center source CFs named by `persist_manager::sync::center_source_cf_name`, e.g. `okex-intra-arb01__uniform_orders`.
- Re-encodes parquet rows into the same RocksDB value layouts used by persist_manager readers.
- Defaults to idempotent behavior: existing keys are counted as `duplicates` and skipped.
- Use `--overwrite` only when the user explicitly wants existing keys replaced.
- Logs per-file and final counts: `rows`, `inserted`, `overwritten`, `duplicates`, `skipped`.
- Logs progress every `--progress-rows` rows, default `50000`.

## Safety

Do not run this while another process is writing the same RocksDB directory unless the user explicitly accepts the risk. If a read server has the DB open as secondary, that is usually okay; another primary writer is not.

State the target `data-dir`, `source-id`, `db-dir`, selected CFs, and overwrite mode before executing the import. This operation mutates the local center DB but does not place or cancel orders.

If the import is repeated without `--overwrite`, unchanged keys should appear as duplicates rather than new inserts.
