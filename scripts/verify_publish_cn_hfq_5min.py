#!/usr/bin/env python3
"""Verify every CN HFQ 5min/15min factor file and publish the audited datasets."""
from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import shutil
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from build_cn_hfq_5min import KEYS, atomic_json, bar_contract, dataset_names, label_columns, signature


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_product(job: dict) -> dict:
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    started = time.monotonic()
    exchange, product = job["exchange"], job["product"]
    bar_minutes = job["bar_minutes"]
    datasets = dataset_names(bar_minutes)
    stage = Path(job["stage_root"])
    report = json.loads((stage / "_audit" / f"{exchange}_{product}.json").read_text())
    expected = {r["file"] for r in report["baseline_files"]}
    for name in datasets:
        actual = {p.name for p in (stage / name / exchange / product).glob("*.parquet")}
        if expected != actual:
            raise ValueError(f"file set mismatch: {name}/{exchange}/{product}")
    source = Path(job["in_root"]) / exchange / product
    for record in report["input_files"]:
        if signature(source / record["file"]) != {k: record[k] for k in ("size", "mtime_ns")}:
            raise ValueError(f"source changed: {source / record['file']}")
    baselines = {r["file"]: r for r in report["baseline_files"]}
    ylabels = {r["file"]: r for r in report["ylabel_files"]}
    factor_records, samples = [], []
    factor_names = job["factor_names"]
    nonnull = np.zeros(len(factor_names), dtype=np.int64)
    rows, segment_rows, comparison_values = 0, 0, 0
    max_segment_bar_rows = 0
    comparisons = {f"cn_features_factor_{number}": 0 for number in ("005", "007", "034", "081")}
    sorted_names = sorted(expected)
    for name in sorted_names:
        base_path, factor_path, ylabel_path = [stage / dataset / exchange / product / name for dataset in datasets]
        for path, record in [(base_path, baselines[name]), (ylabel_path, ylabels[name])]:
            if path.stat().st_size != record["size"] or digest(path) != record["sha256"]:
                raise ValueError(f"verified data changed: {path}")
        base_table = pq.read_table(base_path)
        factor_payload = factor_path.read_bytes()
        factor_table = pq.read_table(pa.BufferReader(factor_payload))
        ylabel_table = pq.read_table(ylabel_path, columns=KEYS)
        if base_table.num_columns != 56 or factor_table.column_names != KEYS + factor_names:
            raise ValueError(f"unexpected schema: {factor_path}")
        if pq.read_schema(ylabel_path).names != KEYS + label_columns(bar_minutes):
            raise ValueError(f"unexpected ylabel schema: {ylabel_path}")
        base_keys = base_table.select(KEYS).cast(pa.schema([pa.field("contract_id", pa.large_string()), pa.field("ts", pa.int64())]))
        for table in [factor_table, ylabel_table]:
            keys = table.select(KEYS).cast(base_keys.schema)
            if not keys.equals(base_keys, check_metadata=False):
                raise ValueError(f"key mismatch: {factor_path}")
        base = base_table.to_pandas()
        factors = factor_table.to_pandas()
        values = factors[factor_names].to_numpy(dtype=float)
        if np.isinf(values).any():
            raise ValueError(f"infinite factor: {factor_path}")
        nonnull += np.isfinite(values).sum(axis=0)
        segments_path = stage / datasets[0] / "_segments" / exchange / product / (name[:8] + ".json")
        metadata = json.loads(segments_path.read_text())
        if (metadata["bar_minutes"] != bar_minutes or metadata["trading_day"] != int(name[:8])
                or metadata["ts"] != base.ts.tolist()):
            raise ValueError(f"segment identity mismatch: {segments_path}")
        segment_start = base.ts.isin(metadata["segment_start_ts"]).to_numpy()
        group = np.cumsum(segment_start)
        if not np.isnan(values[segment_start]).all():
            raise ValueError(f"non-null factor at a segment start: {factor_path}")
        segment_rows += int(segment_start.sum())
        segment_indices = np.flatnonzero(segment_start)
        if not len(segment_indices) or segment_indices[0] != 0:
            raise ValueError(f"missing first segment marker: {segments_path}")
        max_segment_bar_rows = max(max_segment_bar_rows, int(np.diff(np.r_[segment_indices, len(base)]).max()))
        # Independent Python checks cover a time difference, native five-level
        # quantity imbalance and strict rolling 30/3-bar windows, all shifted.
        mid = (base.bid0p + base.ask0p) / 2
        imbalance = base[[f"bid{i}v" for i in range(5)]].sum(axis=1, skipna=False) - base[[f"ask{i}v" for i in range(5)]].sum(axis=1, skipna=False)
        ask3 = base[[f"ask{i}v" for i in range(3)]].sum(axis=1, skipna=False)
        mean30 = ask3.groupby(group).transform(lambda s: s.rolling(30, min_periods=30).mean())
        ratio = (ask3 / mean30).where(mean30.ne(0))
        independent = {
            "cn_features_factor_005": mid.groupby(group).diff().groupby(group).shift(1),
            "cn_features_factor_007": imbalance.groupby(group).shift(1),
            "cn_features_factor_034": ratio.groupby(group).shift(1),
            "cn_features_factor_081": (base.bid4v + base.ask4v).groupby(group).transform(
                lambda s: s.rolling(3, min_periods=3).mean(),
            ).groupby(group).shift(1),
        }
        for column, expected_values in independent.items():
            np.testing.assert_allclose(factors[column], expected_values, rtol=1e-9, atol=1e-8, equal_nan=True,
                                       err_msg=f"independent Python mismatch: {factor_path} {column}")
            comparisons[column] += int(expected_values.notna().sum())
            comparison_values += len(expected_values)
        record = {"file": name, "rows": len(base), "size": len(factor_payload),
                  "sha256": hashlib.sha256(factor_payload).hexdigest()}
        factor_records.append(record)
        if name in {sorted_names[0], sorted_names[-1]}:
            samples.append({"file": f"{exchange}/{product}/{name}", "rows": len(base),
                            "baseline_sha256": baselines[name]["sha256"],
                            "factor_sha256": record["sha256"], "ylabel_sha256": ylabels[name]["sha256"]})
        rows += len(base)
    result = {
        "exchange": exchange, "product": product, "status": "verified",
        "files": len(factor_records), "rows": rows, "segment_start_rows": segment_rows,
        "max_segment_bar_rows": max_segment_bar_rows,
        "independent_comparison_rows": comparison_values, "independent_finite_values": comparisons,
        "factor_nonnull_by_column": dict(zip(factor_names, nonnull.astype(int).tolist())),
        "factor_files": factor_records, "samples": samples,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
    atomic_json(stage / "_audit" / f"verified_{exchange}_{product}.json", result)
    return {k: v for k, v in result.items() if k not in {"factor_files", "factor_nonnull_by_column"}}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage-root", type=Path, required=True)
    parser.add_argument("--out-root", type=Path, required=True)
    parser.add_argument("--factor-schema-source", type=Path, required=True)
    parser.add_argument("--factor-binary", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--wait-for-factors", action="store_true",
                        help="Verify products as their complete atomic daily factor files become available.")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args()
    stage, output = args.stage_root.resolve(), args.out_root.resolve()
    run = json.loads((stage / "_audit" / "run.json").read_text())
    if run["status"] != "verified" or len(run["products"]) != 75:
        parser.error("complete verified 75-product baseline/label build required")
    bar_minutes = run["bar_contract"]["bar_minutes"]
    datasets = dataset_names(bar_minutes)
    factor_names = pq.read_schema(args.factor_schema_source).names[2:]
    if len(factor_names) != 632 or args.workers < 1:
        parser.error("expected 632 factors and positive workers")
    report = {
        "status": "planned", "stage_root": str(stage), "out_root": str(output),
        "in_root": run["in_root"], "start": run["start"], "end": run["end"],
        "products": run["products"], "bar_contract": bar_contract(bar_minutes),
        "bar_minutes": bar_minutes,
        "baseline_label_build": str(stage / "_audit" / "run.json"),
        "script_sha256": digest(Path(__file__)), "factor_binary_sha256": digest(args.factor_binary),
        "validation": "full 634 factor columns read in every file; all three keys equal; all segment heads null; no infinite factors; independent Python factor_005/007/034/081 comparison",
    }
    print(json.dumps({k: v for k, v in report.items() if k != "bar_contract"}), flush=True)
    if not args.execute:
        return
    if stage.stat().st_dev != output.stat().st_dev:
        raise ValueError("publication requires staging and output on the same filesystem")
    for name in datasets:
        if args.publish and (output / name).exists():
            raise FileExistsError(f"refusing to overwrite an existing new-frequency dataset: {output / name}")
    audit = stage / "_audit"
    shutil.copyfile(Path(__file__), audit / Path(__file__).name)
    shutil.copyfile(Path(__file__).parents[1] / "src" / "bin" / "cn_features_1min_hfq.rs", audit / "cn_features_1min_hfq.rs")
    shutil.copyfile(args.factor_binary, audit / "cn_features_1min_hfq")
    started = time.monotonic()
    report["status"] = "running"
    atomic_json(audit / "verification.json", report)
    results = []
    try:
        jobs = [{"exchange": item.split("/")[0], "product": item.split("/")[1],
                 "stage_root": str(stage), "in_root": run["in_root"], "factor_names": factor_names,
                 "bar_minutes": bar_minutes} for item in run["products"]]
        expected_counts = {(r["exchange"], r["product"]): r["files"] for r in run["results"]}
        with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as pool:
            pending, active = jobs.copy(), {}
            while pending or active:
                for job in pending.copy():
                    if len(active) >= args.workers:
                        break
                    directory = stage / datasets[1] / job["exchange"] / job["product"]
                    ready = (not args.wait_for_factors or len(list(directory.glob("*.parquet")))
                             == expected_counts[(job["exchange"], job["product"])])
                    if ready:
                        active[pool.submit(verify_product, job)] = job
                        pending.remove(job)
                if not active:
                    if time.monotonic() - started > 3600:
                        raise TimeoutError("factor producers did not complete within one hour")
                    time.sleep(1)
                    continue
                done, _ = concurrent.futures.wait(active, timeout=1,
                                                 return_when=concurrent.futures.FIRST_COMPLETED)
                for future in done:
                    result = future.result()
                    del active[future]
                    results.append(result)
                    print(json.dumps({"completed": len(results), "total": 75, **{k: v for k, v in result.items() if k != "samples"}}), flush=True)
        report.update(status="verified", results=results, files=sum(r["files"] for r in results),
                      rows=sum(r["rows"] for r in results),
                      segment_start_rows=sum(r["segment_start_rows"] for r in results),
                      max_segment_bar_rows=max(r["max_segment_bar_rows"] for r in results),
                      elapsed_seconds=round(time.monotonic() - started, 3))
        if report["files"] != run["files"] or report["rows"] != run["rows"]:
            raise ValueError("verification totals differ from baseline/label build")
        totals = np.zeros(632, dtype=np.int64)
        for job in jobs:
            details = json.loads((audit / f"verified_{job['exchange']}_{job['product']}.json").read_text())
            totals += np.array([details["factor_nonnull_by_column"][name] for name in factor_names])
        report["factor_nonnull_by_column"] = dict(zip(factor_names, totals.astype(int).tolist()))
        report["all_null_factor_columns"] = [name for name, count in zip(factor_names, totals) if count == 0]
        atomic_json(audit / "verification.json", report)
        if args.publish:
            for job in jobs:
                details = json.loads((audit / f"{job['exchange']}_{job['product']}.json").read_text())
                source = Path(run["in_root"]) / job["exchange"] / job["product"]
                for record in details["input_files"]:
                    if signature(source / record["file"]) != {k: record[k] for k in ("size", "mtime_ns")}:
                        raise ValueError(f"source changed before publication: {source / record['file']}")
            # All data are new, so an atomic rename of each complete dataset
            # publishes no partially generated product directories.
            for name in datasets:
                os.rename(stage / name, output / name)
            report["status"] = "published"
            report["published_roots"] = {name: str(output / name) for name in datasets}
            for result in results:
                for sample in result["samples"]:
                    for name, hash_key in zip(datasets, ("baseline_sha256", "factor_sha256", "ylabel_sha256")):
                        if digest(output / name / sample["file"]) != sample[hash_key]:
                            raise ValueError("post-publication sample checksum mismatch")
            report["post_publication_samples"] = sum(len(r["samples"]) for r in results)
            for name in datasets:
                atomic_json(output / name / "_provenance" / "research_75_20261014.json", report)
            atomic_json(audit / "verification.json", report)
        print(json.dumps({k: report[k] for k in ("status", "files", "rows")}), flush=True)
    except BaseException as error:
        report.update(status="failed", error=repr(error), results=results)
        atomic_json(audit / "verification.json", report)
        raise


if __name__ == "__main__":
    main()
