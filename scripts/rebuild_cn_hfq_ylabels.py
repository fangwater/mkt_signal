#!/usr/bin/env python3
"""Rebuild the observed 96-label CN HFQ research contract, with audited publication."""
from __future__ import annotations

import argparse
import concurrent.futures
import ctypes
import hashlib
import json
import os
from pathlib import Path
import sys
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


BENCHMARKS = {"midp": ("mid_price", 0), "vwap": ("vwap", 1), "twap": ("twap", 1)}
HORIZONS = (1, 5, 15, 30, 60, 240, 720, 1440)
VARIANTS = ("chg", "dir", "vol30", "re")
KEYS = ["contract_id", "ts"]
INPUT_COLUMNS = KEYS + ["mid_price", "vwap", "twap"]
LABEL_COLUMNS = [
    f"{benchmark}_{variant}_{horizon}m"
    for benchmark in BENCHMARKS
    for horizon in HORIZONS
    for variant in VARIANTS
]
CONTRACT = {
    "horizon_unit": "observed trading-minute rows, across session and day gaps",
    "horizon_minutes": list(HORIZONS),
    "price_row_offset": {key: offset for key, (_, offset) in BENCHMARKS.items()},
    "price_source": "existing additive HFQ minute prices, without additional filling or volume masking",
    "chg": "P[i+h]/P[i]-1; zero denominator or nonfinite input/result is null",
    "dir": "1 if chg>0, otherwise 0; null when chg is null",
    "vol30": "sample std(ddof=1) of the last 30 h-step returns at the future endpoint",
    "re": "average rank of the last value among 1440 h-step returns at the future endpoint",
    "window_min_periods": {"vol30": 30, "re": 1440},
    "output_ts": "same UTC Unix seconds and row keys as the HFQ source",
}
SCHEMA = pa.schema(
    [pa.field("contract_id", pa.large_string()), pa.field("ts", pa.int64())]
    + [pa.field(name, pa.float64()) for name in LABEL_COLUMNS],
    metadata={b"cn_hfq_ylabels_contract": json.dumps(CONTRACT, sort_keys=True).encode()},
)


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    os.replace(temporary, path)


def label_columns(bar_minutes: int = 1) -> list[str]:
    if bar_minutes < 1:
        raise ValueError("bar_minutes must be positive")
    return [
        f"{benchmark}_{variant}_{horizon * bar_minutes}m"
        for benchmark in BENCHMARKS
        for horizon in HORIZONS
        for variant in VARIANTS
    ]


def build_labels(frame: pd.DataFrame, bar_minutes: int = 1) -> pd.DataFrame:
    """Python correctness baseline. Row positions, not elapsed wall time, define horizons."""
    labels = {}
    for benchmark, (column, offset) in BENCHMARKS.items():
        price = frame[column].astype("float64")
        price = price.where(np.isfinite(price)).shift(-offset)
        for horizon in HORIZONS:
            denominator = price.shift(horizon)
            backward = (price / denominator - 1).where(denominator.ne(0))
            backward = backward.where(np.isfinite(backward))
            forward = backward.shift(-horizon)
            suffix = f"{horizon * bar_minutes}m"
            labels[f"{benchmark}_chg_{suffix}"] = forward
            labels[f"{benchmark}_dir_{suffix}"] = (
                forward.gt(0).astype("float64").where(forward.notna())
            )
            labels[f"{benchmark}_vol30_{suffix}"] = (
                backward.rolling(30, min_periods=30).std(ddof=1).shift(-horizon)
            )
            labels[f"{benchmark}_re_{suffix}"] = (
                backward.rolling(1440, min_periods=1440)
                .rank(method="average").shift(-horizon)
            )
    return pd.concat(
        [frame[KEYS].reset_index(drop=True), pd.DataFrame(labels).reset_index(drop=True)],
        axis=1,
    )


def day_files(directory: Path, end: str) -> list[Path]:
    return sorted(
        p for p in directory.iterdir()
        if p.is_file() and p.suffix == ".parquet"
        and len(p.stem) == 8 and p.stem.isdigit() and p.stem <= end
    )


def signature(path: Path) -> dict:
    stat = path.stat()
    return {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def load_source(paths: list[Path], product: str):
    frames, records, spans = [], [], []
    position = 0
    previous_ts = None
    for path in paths:
        before = signature(path)
        payload = path.read_bytes()
        table = pq.read_table(pa.BufferReader(payload), columns=INPUT_COLUMNS)
        if before != signature(path):
            raise RuntimeError(f"source changed while reading: {path}")
        frame = table.to_pandas()
        if frame[KEYS].isna().any().any():
            raise ValueError(f"null source key: {path}")
        if not frame.contract_id.eq(product).all():
            raise ValueError(f"unexpected contract_id: {path}")
        timestamps = frame.ts.to_numpy()
        if len(timestamps):
            if np.any(np.diff(timestamps) <= 0):
                raise ValueError(f"unsorted or duplicate source timestamps: {path}")
            if previous_ts is not None and timestamps[0] <= previous_ts:
                raise ValueError(f"overlapping source day timestamps: {path}")
            previous_ts = int(timestamps[-1])
        spans.append((path.name, position, len(frame)))
        position += len(frame)
        frames.append(frame)
        records.append({
            "file": path.name, **before, "rows": len(frame),
            "sha256": hashlib.sha256(payload).hexdigest(),
        })
    if not frames:
        raise ValueError(f"no source files for {product}")
    return pd.concat(frames, ignore_index=True), spans, records


def rebuild_product(job: dict) -> dict:
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    started = time.monotonic()
    exchange, product = job["exchange"], job["product"]
    source = Path(job["in_root"]) / exchange / product
    stage = Path(job["stage_root"]) / exchange / product
    live = Path(job["out_root"]) / exchange / product
    paths = day_files(source, job["end"])
    selected = {p.name for p in paths if p.stem >= job["start"]}
    existing_selected = {
        p.name for p in live.iterdir()
        if p.suffix == ".parquet" and job["start"] <= p.stem <= job["end"]
    }
    if selected != existing_selected:
        raise ValueError(f"source/live selected file sets differ: {exchange}/{product}")
    stage.mkdir(parents=True, exist_ok=False)
    old_snapshot = {p.name: signature(p) for p in live.iterdir() if p.is_file()}
    frame, spans, inputs = load_source(paths, product)
    labels = build_labels(frame)
    if len(labels) != len(frame) or not labels[KEYS].equals(frame[KEYS]):
        raise ValueError(f"computed keys differ: {exchange}/{product}")
    values = labels[LABEL_COLUMNS].to_numpy()
    if np.isinf(values).any():
        raise ValueError(f"infinite label: {exchange}/{product}")
    table = pa.Table.from_pandas(labels, schema=SCHEMA, preserve_index=False)
    output_records, rows = [], 0
    for name, offset, length in spans:
        if name not in selected:
            continue
        expected = table.slice(offset, length)
        destination = stage / name
        pq.write_table(expected, destination, compression="zstd", compression_level=3)
        payload = destination.read_bytes()
        actual = pq.read_table(pa.BufferReader(payload))
        if not expected.equals(actual, check_metadata=True):
            raise ValueError(f"parquet roundtrip differs: {destination}")
        rows += length
        output_records.append({
            "file": name, "rows": length, "size": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        })
    # Preserve every existing file outside the requested dates. A directory exchange
    # publishes a whole product without exposing a partially rewritten daily set.
    preserved = 0
    for path in live.iterdir():
        if path.name in selected:
            continue
        if not path.is_file():
            raise ValueError(f"unexpected entry in live product directory: {path}")
        os.link(path, stage / path.name)
        preserved += 1
    report = {
        "exchange": exchange, "product": product, "status": "verified",
        "files": len(output_records), "rows": rows, "columns": len(SCHEMA),
        "preserved_out_of_range_files": preserved,
        "source_files": inputs, "output_files": output_records,
        "old_snapshot": old_snapshot,
        "input_manifest_sha256": hashlib.sha256(
            json.dumps(inputs, sort_keys=True).encode()
        ).hexdigest(),
        "output_manifest_sha256": hashlib.sha256(
            json.dumps(output_records, sort_keys=True).encode()
        ).hexdigest(),
        "validation": "every output file read back and all 98 columns equal computed values and source keys",
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
    atomic_json(Path(job["stage_root"]) / "_audit" / f"{exchange}_{product}.json", report)
    return {k: v for k, v in report.items() if k not in {"source_files", "output_files", "old_snapshot"}}


def exchange_directories(left: Path, right: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    rename = libc.renameat2
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    if rename(-100, os.fsencode(left), -100, os.fsencode(right), 2):
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), f"{left} <-> {right}")


def publish(stage_root: Path, in_root: Path, out_root: Path, products: list, report: dict):
    # Check every source and live file for concurrent rewrites before any exchange.
    for exchange, product in products:
        audit = json.loads((stage_root / "_audit" / f"{exchange}_{product}.json").read_text())
        source = in_root / exchange / product
        live = out_root / exchange / product
        for record in audit["source_files"]:
            if signature(source / record["file"]) != {k: record[k] for k in ["size", "mtime_ns"]}:
                raise RuntimeError(f"source changed before publish: {source / record['file']}")
        current = {p.name: signature(p) for p in live.iterdir() if p.is_file()}
        if current != audit["old_snapshot"]:
            raise RuntimeError(f"live labels changed before publish: {live}")
    previous = stage_root / "_previous"
    report["published_products"] = []
    report["backup_root"] = str(previous)
    atomic_json(stage_root / "_audit" / "run.json", report)
    for exchange, product in products:
        staged = stage_root / exchange / product
        live = out_root / exchange / product
        backup = previous / exchange / product
        backup.parent.mkdir(parents=True, exist_ok=True)
        exchange_directories(staged, live)
        os.replace(staged, backup)
        report["published_products"].append(f"{exchange}/{product}")
        atomic_json(stage_root / "_audit" / "run.json", report)
        print(f"published {exchange}/{product}", flush=True)
    provenance = out_root / "_provenance" / "research_75_ylabels_20261014.json"
    report["status"] = "published"
    atomic_json(provenance, report)
    atomic_json(stage_root / "_audit" / "run.json", report)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--universe-json", type=Path, required=True)
    parser.add_argument("--in-root", type=Path, required=True)
    parser.add_argument("--out-root", type=Path, required=True)
    parser.add_argument("--stage-root", type=Path, required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--product", help="Optional comma-separated subset for validation.")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args()
    start, end = (pd.Timestamp(value).strftime("%Y%m%d") for value in [args.start, args.end])
    if start > end or args.workers < 1:
        parser.error("invalid date range or worker count")
    universe_payload = args.universe_json.read_bytes()
    universe = json.loads(universe_payload)
    products = [
        (exchange, product)
        for exchange, group in universe["included_products_by_exchange"].items()
        for product in group
    ]
    if len(products) != 75 or len({p for _, p in products}) != 75:
        parser.error("expected the authorized 75-product universe")
    if args.product:
        requested = set(args.product.upper().split(","))
        if not requested <= {p for _, p in products}:
            parser.error("requested product is outside the authorized universe")
        products = [item for item in products if item[1] in requested]
    if args.publish and len(products) != 75:
        parser.error("publication requires the complete 75-product scope")
    in_root, out_root, stage_root = (p.resolve() for p in [args.in_root, args.out_root, args.stage_root])
    if len({in_root, out_root, stage_root}) != 3:
        parser.error("input, output and staging roots must differ")
    if "rocksdb" in str(out_root).lower() or "rocksdb" in str(stage_root).lower():
        parser.error("refusing to write labels to a RocksDB path")
    plan = {
        "status": "planned", "start": start, "end": end,
        "products": [f"{e}/{p}" for e, p in products],
        "in_root": str(in_root), "out_root": str(out_root), "stage_root": str(stage_root),
        "contract": CONTRACT, "workers": args.workers,
        "universe_sha256": hashlib.sha256(universe_payload).hexdigest(),
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "python": sys.version, "pandas": pd.__version__, "pyarrow": pa.__version__,
    }
    print(json.dumps({k: v for k, v in plan.items() if k != "contract"}), flush=True)
    if not args.execute:
        return
    stage_root.mkdir(parents=True, exist_ok=False)
    if stage_root.stat().st_dev != out_root.stat().st_dev:
        raise ValueError("staging and live roots must share a filesystem for atomic publication")
    started = time.monotonic()
    plan["status"] = "running"
    atomic_json(stage_root / "_audit" / "run.json", plan)
    jobs = [
        {"exchange": e, "product": p, "in_root": str(in_root), "out_root": str(out_root),
         "stage_root": str(stage_root), "start": start, "end": end}
        for e, p in products
    ]
    results = []
    try:
        with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(rebuild_product, job): job for job in jobs}
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                results.append(result)
                print(json.dumps({"completed": len(results), "total": len(jobs), **result}), flush=True)
        plan["results"] = results
        plan["files"] = sum(r["files"] for r in results)
        plan["rows"] = sum(r["rows"] for r in results)
        plan["elapsed_seconds"] = round(time.monotonic() - started, 3)
        plan["status"] = "verified"
        atomic_json(stage_root / "_audit" / "run.json", plan)
        if args.publish:
            publish(stage_root, in_root, out_root, products, plan)
        print(json.dumps({"status": plan["status"], "files": plan["files"], "rows": plan["rows"]}), flush=True)
    except BaseException as error:
        plan["status"] = "failed"
        plan["error"] = repr(error)
        atomic_json(stage_root / "_audit" / "run.json", plan)
        raise


if __name__ == "__main__":
    main()
