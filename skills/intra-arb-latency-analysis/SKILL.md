---
name: intra-arb-latency-analysis
description: Analyze intra spot/futures arbitrage order latency from local uniform_orders parquet snapshots. Use when the user asks for mkt_ts to signal_ts, create_ts to signal_ts, update_ts to submit_ts, local_ts to submit_ts, or NEW rows that later filled latency for environments such as bybit-intra-arb01, okex-intra-arb01, gate-intra-arb01, bitget-intra-arb01, or binance-intra-arb01, especially from order_export or data/intra_order_export_backfill snapshots.
---

# Intra Arb Latency Analysis

Use repo-local parquet snapshots instead of re-reading remote persist services when the data is already available on disk.

## Workflow

1. Work in `/home/ubuntu/crypto_mkt/mkt_signal` unless the user points to another checkout.
2. If the user does not already have a local snapshot, fetch one first with the existing backfill script.
3. Run the latency helper on `uniform_orders.parquet`.
4. For same-exchange spot/futures intra analysis, report spot and futures results in one combined table, not as separate sections.
5. Include a Chinese `含义` column in the table.
6. For spot/open-leg rows, report:
   - `signal_ts - mkt_ts`
   - `create_ts - signal_ts`
   - `update_ts - submit_ts` on the `NEW` row
   - `local_ts - submit_ts`
7. For futures hedge rows, report:
   - `futures.create_ts - margin.local_ts`
   - `futures.create_ts - margin.update_ts`
   - `futures.update_ts - futures.create_ts`
   - `futures.local_ts - futures.create_ts`
8. For Bybit intra hedge analysis, also report the taker hedge reaction latency from margin local receive time to futures submit time.

## Fetch Snapshot

For remote intra envs, use the existing read-only export flow:

```bash
bash scripts/fetch_intra_uniform_orders_backfill.sh \
  --skip-build \
  --start 2026-06-12T10:11:44Z \
  --end 2026-06-14T10:11:44Z \
  --run-id bybit_intra_arb01_48h_20260614T101144Z \
  bybit-intra-arb01
```

Typical output layout:

```text
data/intra_order_export_backfill/<run-id>/<source-id>/uniform_orders.parquet
data/intra_order_export_backfill/<run-id>/<source-id>/order_updates_unmatched.parquet
data/intra_order_export_backfill/<run-id>/<source-id>/trade_updates_unmatched.parquet
```

## Run The Helper

Analyze one snapshot:

```bash
python3 skills/intra-arb-latency-analysis/scripts/analyze_intra_arb_latency.py \
  --parquet data/intra_order_export_backfill/bybit_intra_arb01_48h_20260614T101144Z/bybit-intra-arb01/uniform_orders.parquet
```

Only the subset whose `NEW` orders later had fills:

```bash
python3 skills/intra-arb-latency-analysis/scripts/analyze_intra_arb_latency.py \
  --parquet data/intra_order_export_backfill/bybit_intra_arb01_48h_20260614T101144Z/bybit-intra-arb01/uniform_orders.parquet \
  --subset new-with-fill
```

Disable venue filtering or change the normal-path cap:

```bash
python3 skills/intra-arb-latency-analysis/scripts/analyze_intra_arb_latency.py \
  --parquet /path/to/uniform_orders.parquet \
  --venue any \
  --normal-max-ms 200 \
  --format json
```

Include Bybit taker hedge latency:

```bash
python3 skills/intra-arb-latency-analysis/scripts/analyze_intra_arb_latency.py \
  --parquet data/intra_order_export_backfill/bybit_intra_arb01_48h_20260614T101144Z/bybit-intra-arb01/uniform_orders.parquet \
  --include-hedge
```

## Analysis Rules

The helper matches the notebook-style latency workflow used in this repo:

- Start from `status == NEW`.
- Require `signal_ts > 0`, `mkt_ts > 0`, and `submit_ts > 0`.
- Default venue filter is `BybitMargin`. Pass `--venue any` to disable it.
- Compute stats on the normal-path subset: non-negative values and `<= --normal-max-ms` for each metric.
- Report counts and `p50/p90/p95/p99/max`, plus `>5ms` and `>10ms` ratios.

Subset semantics:

- `all-new`: all valid `NEW` rows after the timestamp and venue filters.
- `new-with-fill`: those same `NEW` rows whose `client_order_id` later has any row with `amount_update > 0`.

Important: `NEW.amount_update` is normally zero. "Has fill" must be derived from later rows on the same `client_order_id`, not from the `NEW` row itself.

## Bybit Hedge Rules

Use the Bybit futures hedge mapping only when the futures `from_key` has the form:

```text
<margin_open_client_order_id>|arb_hedge_force_taker_direct|<margin_trigger_update_ts>
```

In that case:

- Deduplicate margin trigger rows by `(client_order_id, update_ts)`.
- Keep the earliest `local_ts` for each deduplicated trigger event.
- Map the futures hedge row back to the margin trigger row by:
  - `open_client_order_id`
  - `trigger_update_ts_from_key == margin.update_ts`

Primary hedge metric:

- `futures_submit_minus_margin_local_ms = futures.submit_ts - margin.local_ts`

This is the standard local-process hedge reaction metric for Bybit intra.

Secondary reference metrics:

- `futures_submit_minus_trigger_update_ms`
- `futures_local_minus_submit_ms`
- `futures_update_minus_submit_ms`

## Combined Table Meanings

- `signal_ts - mkt_ts`: 行情延迟.
- `create_ts - signal_ts`: 内部延迟.
- `update_ts - submit_ts`: 挂单延迟; for spot/open-leg analysis this is measured on the `NEW` row.
- `local_ts - submit_ts`: 完整回报延迟.
- `futures.create_ts - margin.local_ts`: taker触发内部延迟.
- `futures.create_ts - margin.update_ts`: 合约挂到距离现货撤单/成交延迟.
- `futures.update_ts - futures.create_ts`: 合约挂单延迟.
- `futures.local_ts - futures.create_ts`: 合约完整回报延迟.

For futures hedge rows, `futures.create_ts - margin.local_ts` is the preferred local-process taker hedge reaction metric.

When comparing with older notebooks, keep the same filters. Do not mix in terminal rows unless the user explicitly asks for a different definition.

Relevant repo artifacts for deeper follow-up:

- `persist_read_bybit_intra_arb01_20260612_new_latency.executed.ipynb`
- `market_data_latency_analysis.ipynb`
- `eth_open_hedge_analysis.ipynb`
