#!/usr/bin/env python3
"""Build audited CN HFQ 5min/15min baseline and 96 labels from minute prices."""
from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from rebuild_cn_hfq_ylabels import (
    BENCHMARKS, CONTRACT, HORIZONS, KEYS, atomic_json, build_labels,
    day_files, label_columns, signature,
)

BAR_MINUTES = 5
SUM_COLUMNS = [
    "volume", "amount", "count", "buy_count", "sell_count", "buy_amount",
    "sell_amount", "buy_volume", "sell_volume", "large_order", "medium_order",
    "small_order", "large_buy", "large_sell", "medium_buy", "medium_sell",
    "small_buy", "small_sell", "net_buy_large", "net_buy_medium", "net_buy_small",
]
DEPTH_COLUMNS = [f"{side}{level}{kind}" for side in ("bid", "ask")
                 for level in range(5) for kind in ("p", "v")]
PRICE_COLUMNS = ["open", "high", "low", "close", "vwap", "buy_vwap", "sell_vwap", "twap", "mid_price"]
EXPECTED_COLUMNS = set(KEYS + SUM_COLUMNS + DEPTH_COLUMNS + PRICE_COLUMNS +
                       ["avg_amount", "net_buy_amount", "net_buy_volume", "net_buy_pct"])
BAR_CONTRACT = {
    "bar_minutes": 5,
    "alignment": "first observed minute of each contiguous source segment",
    "segment_rule": "TradDay boundary or source timestamp difference other than 60 seconds",
    "partial_tail": "retain; do not fill missing minutes",
    "timestamp": "first contributing minute, UTC Unix seconds",
    "ohlc": "actual trading minutes only; no-trade bar retains source last close",
    "vwap": "source adjusted quotation weighted by corresponding volume; zero-volume retains last source quotation",
    "twap": "equal-duration mean of contributing minute TWAPs; propagate null",
    "book": "all 20 fields and mid_price from the same last contributing minute",
    "factor_windows": "same existing formula row counts, one input row per 5min bar",
    "factor_alignment": "shift(1) within segment; null first row of segment",
    "label_windows": "same existing 96-label row counts, one row per 5min bar",
}
def bar_contract(bar_minutes: int) -> dict:
    if bar_minutes not in (5, 15):
        raise ValueError("supported bar lengths are 5 and 15 minutes")
    return {
        **BAR_CONTRACT, "bar_minutes": bar_minutes,
        "factor_windows": f"same existing formula row counts, one input row per {bar_minutes}min bar",
        "label_windows": f"same existing 96-label row counts, one row per {bar_minutes}min bar",
    }


def label_contract(bar_minutes: int) -> dict:
    return {
        **CONTRACT, "horizon_unit": f"observed {bar_minutes}min bars across session and day gaps",
        "horizon_bars": list(HORIZONS),
        "horizon_minutes": [h * bar_minutes for h in HORIZONS],
        "price_source": "aggregated existing additive HFQ minute quotations",
        "bar_contract": bar_contract(bar_minutes),
    }


def label_schema(bar_minutes: int) -> pa.Schema:
    return pa.schema(
        [pa.field("contract_id", pa.large_string()), pa.field("ts", pa.int64())]
        + [pa.field(name, pa.float64()) for name in label_columns(bar_minutes)],
        metadata={b"cn_hfq_ylabels_contract": json.dumps(label_contract(bar_minutes), sort_keys=True).encode()},
    )


def dataset_names(bar_minutes: int) -> tuple[str, str, str]:
    bar_contract(bar_minutes)
    return tuple(f"{prefix}_{bar_minutes}min_hfq" for prefix in ("baseline_data", "baseline_factor", "ylabel"))


def aggregate_minutes(frame: pd.DataFrame, bar_minutes: int = BAR_MINUTES) -> tuple[pd.DataFrame, dict]:
    """Python/NumPy correctness baseline; no multiplier or HFQ gap is reapplied."""
    bar_contract(bar_minutes)
    if set(frame.columns) != EXPECTED_COLUMNS or frame.empty:
        raise ValueError("expected nonempty 56-column HFQ source")
    ts = frame.ts.to_numpy(dtype=np.int64)
    if np.any(np.diff(ts) <= 0) or np.any(ts % 60):
        raise ValueError("timestamps must be strictly increasing UTC minute endpoints")
    segment_start = np.r_[True, np.diff(ts) != 60]
    indices = np.arange(len(frame))
    segment_anchor = np.maximum.accumulate(np.where(segment_start, indices, 0))
    bucket_start = segment_start | ((indices - segment_anchor) % bar_minutes == 0)
    starts = np.flatnonzero(bucket_start)
    ends = np.r_[starts[1:], len(frame)]
    last = ends - 1
    sizes = ends - starts
    result = frame.iloc[last].reset_index(drop=True).copy()
    result["ts"] = ts[starts]
    result["contract_id"] = frame.contract_id.iloc[starts].to_numpy()
    if frame.contract_id.nunique() != 1 or frame.contract_id.isna().any():
        raise ValueError("one nonnull product per input day required")
    for name in SUM_COLUMNS:
        result[name] = np.add.reduceat(frame[name].to_numpy(dtype=float), starts)
    count = result["count"].to_numpy()
    amount = result["amount"].to_numpy()
    result["avg_amount"] = np.divide(amount, count, out=np.zeros(len(starts)), where=count > 0)
    result["net_buy_amount"] = result.buy_amount - result.sell_amount
    result["net_buy_volume"] = result.buy_volume - result.sell_volume
    directed = (result.buy_amount + result.sell_amount).to_numpy()
    result["net_buy_pct"] = np.divide(
        result.net_buy_amount.to_numpy(), directed, out=np.zeros(len(starts)), where=directed > 0,
    )
    # Carried empty-minute OHLC must not become the first print/high/low of a
    # bar that does contain actual trades.
    active = frame["count"].to_numpy() > 0
    active_count = np.add.reduceat(active.astype(np.int64), starts)
    first_trade = np.minimum.reduceat(np.where(active, indices, len(frame)), starts)
    last_trade = np.maximum.reduceat(np.where(active, indices, -1), starts)
    has_trade = active_count > 0
    safe_first = np.minimum(first_trade, len(frame) - 1)
    safe_last = np.maximum(last_trade, 0)
    carried = frame.close.to_numpy(dtype=float)[last]
    result["open"] = np.where(has_trade, frame.open.to_numpy(dtype=float)[safe_first], carried)
    result["close"] = np.where(has_trade, frame.close.to_numpy(dtype=float)[safe_last], carried)
    for name, reduce, sentinel in [("high", np.maximum, -np.inf), ("low", np.minimum, np.inf)]:
        value = reduce.reduceat(np.where(active, frame[name].to_numpy(dtype=float), sentinel), starts)
        result[name] = np.where(has_trade, value, carried)
    for name, weight_name in [("vwap", "volume"), ("buy_vwap", "buy_volume"), ("sell_vwap", "sell_volume")]:
        price = frame[name].to_numpy(dtype=float)
        weight = frame[weight_name].to_numpy(dtype=float)
        # A missing quote with zero weight contributes nothing. A missing
        # quote for actual volume propagates to the whole weighted VWAP.
        contribution = np.zeros(len(frame))
        np.multiply(price, weight, out=contribution, where=weight != 0)
        numerator = np.add.reduceat(contribution, starts)
        denominator = result[weight_name].to_numpy()
        value = price[last].copy()
        np.divide(numerator, denominator, out=value, where=denominator > 0)
        result[name] = value
    result["twap"] = np.add.reduceat(frame.twap.to_numpy(dtype=float), starts) / sizes
    if np.isinf(result.drop(columns=KEYS).to_numpy(dtype=float)).any():
        raise ValueError("nonfinite aggregate result")
    metadata = {
        "bar_minutes": bar_minutes,
        "ts": result.ts.astype(int).tolist(),
        "segment_start_ts": ts[segment_start].astype(int).tolist(),
        "source_minutes": sizes.astype(int).tolist(),
        "end_ts": (ts[last] + 60).astype(int).tolist(),
    }
    return result[frame.columns], metadata


def write_verified(table: pa.Table, destination: Path) -> dict:
    destination.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, destination, compression="zstd", compression_level=3)
    payload = destination.read_bytes()
    actual = pq.read_table(pa.BufferReader(payload))
    if not table.equals(actual, check_metadata=True):
        raise ValueError(f"all-column readback mismatch: {destination}")
    return {"file": destination.name, "rows": table.num_rows, "size": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest()}


def build_product(job: dict) -> dict:
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    started = time.monotonic()
    exchange, product = job["exchange"], job["product"]
    bar_minutes = job["bar_minutes"]
    base_name, _, ylabel_name = dataset_names(bar_minutes)
    stage = Path(job["stage_root"])
    paths = day_files(Path(job["in_root"]) / exchange / product, job["end"])
    inputs, baselines, segments, spans, frames = [], [], [], [], []
    position = 0
    previous_ts = None
    base_schema = None
    for path in paths:
        before = signature(path)
        payload = path.read_bytes()
        table = pq.read_table(pa.BufferReader(payload))
        if signature(path) != before:
            raise ValueError(f"source changed while reading: {path}")
        frame = table.to_pandas()
        if not frame.contract_id.eq(product).all():
            raise ValueError(f"unexpected product in {path}")
        if previous_ts is not None and frame.ts.iloc[0] <= previous_ts:
            raise ValueError(f"overlapping source days: {path}")
        previous_ts = int(frame.ts.iloc[-1])
        bars, metadata = aggregate_minutes(frame, bar_minutes)
        if base_schema is None:
            base_schema = table.schema.remove_metadata().with_metadata(
                {b"cn_hfq_bars_contract": json.dumps(bar_contract(bar_minutes), sort_keys=True).encode()},
            )
        frames.append(bars)
        spans.append((path.name, position, len(bars)))
        position += len(bars)
        inputs.append({"file": path.name, **before, "rows": len(frame),
                       "sha256": hashlib.sha256(payload).hexdigest()})
        if path.stem < job["start"]:
            continue
        if not (np.isfinite(bars.bid0p) & np.isfinite(bars.ask0p) & (bars.ask0p >= bars.bid0p)).all():
            raise ValueError(f"invalid bilateral book: {path}")
        bar_table = pa.Table.from_pandas(bars, schema=base_schema, preserve_index=False)
        baselines.append(write_verified(bar_table, stage / base_name / exchange / product / path.name))
        metadata["trading_day"] = int(path.stem)
        segment_path = stage / base_name / "_segments" / exchange / product / (path.stem + ".json")
        atomic_json(segment_path, metadata)
        segments.append({"file": path.name, "segments": len(metadata["segment_start_ts"]),
                         "partial_bars": sum(n < bar_minutes for n in metadata["source_minutes"]),
                         "source_minutes": len(frame)})
    if not frames or not baselines:
        raise ValueError(f"no selected input for {exchange}/{product}")
    all_bars = pd.concat(frames, ignore_index=True)
    labels = build_labels(all_bars, bar_minutes=bar_minutes)
    if not labels[KEYS].equals(all_bars[KEYS]):
        raise ValueError("computed label keys changed")
    if np.isinf(labels.drop(columns=KEYS).to_numpy(dtype=float)).any():
        raise ValueError("infinite labels")
    label_table = pa.Table.from_pandas(labels, schema=label_schema(bar_minutes), preserve_index=False)
    ylabels = []
    for name, offset, length in spans:
        if name[:8] < job["start"]:
            continue
        ylabels.append(write_verified(
            label_table.slice(offset, length), stage / ylabel_name / exchange / product / name,
        ))
    report = {
        "exchange": exchange, "product": product, "status": "verified", "bar_minutes": bar_minutes,
        "files": len(baselines), "rows": sum(r["rows"] for r in baselines),
        "source_rows": sum(r["source_minutes"] for r in segments),
        "warmup_bar_rows": sum(len(f) for f in frames) - sum(r["rows"] for r in baselines),
        "segments": sum(r["segments"] for r in segments),
        "partial_bars": sum(r["partial_bars"] for r in segments),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "input_files": inputs, "baseline_files": baselines, "ylabel_files": ylabels,
        "segments_by_day": segments,
    }
    atomic_json(stage / "_audit" / f"{exchange}_{product}.json", report)
    return {k: v for k, v in report.items() if not k.endswith("_files") and k != "segments_by_day"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--universe-json", type=Path, required=True)
    parser.add_argument("--in-root", type=Path, required=True)
    parser.add_argument("--stage-root", type=Path, required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--bar-minutes", type=int, choices=(5, 15), default=BAR_MINUTES)
    parser.add_argument("--product", help="Optional authorized subset for validation.")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    start, end = [pd.Timestamp(v).strftime("%Y%m%d") for v in (args.start, args.end)]
    if start > end or args.workers < 1 or args.in_root.resolve() == args.stage_root.resolve():
        parser.error("invalid range, worker count or root")
    if "rocksdb" in str(args.stage_root).lower():
        parser.error("refusing a RocksDB output path")
    universe_payload = args.universe_json.read_bytes()
    universe = json.loads(universe_payload)
    products = [(e, p) for e, group in universe["included_products_by_exchange"].items() for p in group]
    if len(products) != 75 or len({p for _, p in products}) != 75:
        parser.error("expected authorized 75-product universe")
    if args.product:
        requested = set(args.product.upper().split(","))
        if not requested <= {p for _, p in products}:
            parser.error("product outside authorized scope")
        products = [(e, p) for e, p in products if p in requested]
    report = {
        "status": "planned", "start": start, "end": end,
        "products": [f"{e}/{p}" for e, p in products],
        "in_root": str(args.in_root.resolve()), "stage_root": str(args.stage_root.resolve()),
        "workers": args.workers, "bar_contract": bar_contract(args.bar_minutes),
        "label_contract": label_contract(args.bar_minutes), "bar_minutes": args.bar_minutes,
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "ylabel_script_sha256": hashlib.sha256(Path(__file__).with_name("rebuild_cn_hfq_ylabels.py").read_bytes()).hexdigest(),
        "universe_sha256": hashlib.sha256(universe_payload).hexdigest(),
        "python": sys.version, "numpy": np.__version__, "pandas": pd.__version__, "pyarrow": pa.__version__,
    }
    print(json.dumps({k: v for k, v in report.items() if not k.endswith("contract")}), flush=True)
    if not args.execute:
        return
    args.stage_root.mkdir(parents=True, exist_ok=False)
    audit = args.stage_root / "_audit"
    audit.mkdir()
    for script in [Path(__file__), Path(__file__).with_name("rebuild_cn_hfq_ylabels.py")]:
        shutil.copyfile(script, audit / script.name)
    (audit / "universe.json").write_bytes(universe_payload)
    started = time.monotonic()
    report["status"] = "running"
    atomic_json(audit / "run.json", report)
    jobs = [{"exchange": e, "product": p, "in_root": str(args.in_root),
             "stage_root": str(args.stage_root), "start": start, "end": end,
             "bar_minutes": args.bar_minutes} for e, p in products]
    results = []
    try:
        with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = [pool.submit(build_product, job) for job in jobs]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                results.append(result)
                print(json.dumps({"completed": len(results), "total": len(jobs), **result}), flush=True)
        report.update(status="verified", results=results,
                      files=sum(r["files"] for r in results), rows=sum(r["rows"] for r in results),
                      source_rows=sum(r["source_rows"] for r in results),
                      partial_bars=sum(r["partial_bars"] for r in results),
                      elapsed_seconds=round(time.monotonic() - started, 3))
        atomic_json(audit / "run.json", report)
        print(json.dumps({k: report[k] for k in ("status", "files", "rows", "partial_bars")}), flush=True)
    except BaseException as error:
        report.update(status="failed", error=repr(error), results=results)
        atomic_json(audit / "run.json", report)
        raise


if __name__ == "__main__":
    main()
