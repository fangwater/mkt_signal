---
name: intra-arb-latency-analysis
description: Analyze intra spot/futures arbitrage order latency from local uniform_orders parquet snapshots. Use when the user asks for mkt_ts to signal_ts, signal_ts to submit_ts, update_ts to submit_ts, local_ts to submit_ts, or NEW rows that later filled latency for environments such as bybit-intra-arb01, okex-intra-arb01, gate-intra-arb01, bitget-intra-arb01, or binance-intra-arb01, especially from order_export or data/intra_order_export_backfill snapshots.
---

# Intra Arb Latency Analysis

Use repo-local parquet snapshots instead of re-reading remote persist services when the data is already available on disk.

## Workflow

1. Work in `/home/ubuntu/crypto_mkt/mkt_signal` unless the user points to another checkout.
2. If the user does not already have a local snapshot, fetch one first with the existing backfill script.
3. Run the latency helper on `uniform_orders.parquet`.
4. Report the three main segments:
   - `signal_ts - mkt_ts`
   - `submit_ts - signal_ts`
   - `update_ts - submit_ts`
5. Keep `local_ts - submit_ts` as the local-clock fallback when `update_ts - submit_ts` needs cross-clock context.
6. For Bybit intra hedge analysis, also report the taker hedge reaction latency from margin local receive time to futures submit time.

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

## Interpretation Notes

- `signal_ts - mkt_ts` is the market-to-signal stage.
- `submit_ts - signal_ts` is the in-process signal-to-submit stage.
- `update_ts - submit_ts` is the submit-to-exchange-ack stage, but it can have missing or cross-clock quirks.
- `local_ts - submit_ts` is the better SG local-clock correlation metric when the user wants a pure local receive latency.
- `futures submit_ts - margin local_ts` is the preferred Bybit taker hedge reaction metric when the user asks for margin local ts to hedge submit ts.

When comparing with older notebooks, keep the same filters. Do not mix in terminal rows unless the user explicitly asks for a different definition.

Relevant repo artifacts for deeper follow-up:

- `persist_read_bybit_intra_arb01_20260612_new_latency.executed.ipynb`
- `market_data_latency_analysis.ipynb`
- `eth_open_hedge_analysis.ipynb`
