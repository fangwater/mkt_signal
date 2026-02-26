#!/usr/bin/env python3
"""
Factor comparison script: compares Rust factor_test output against Python implementations.

Usage:
    python scripts/factor_test_compare.py /tmp/factor_test_output.json
"""

import sys
import json
import importlib.util
import numpy as np
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------------------------
# Load Python factor pool
# ---------------------------------------------------------------------------
FACTOR_POOL_PATH = Path(__file__).resolve().parent.parent / "final_factor_pool_update20260123.py"


def load_factor_pool():
    spec = importlib.util.spec_from_file_location("factor_pool", str(FACTOR_POOL_PATH))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Build DataFrame from JSON input
# ---------------------------------------------------------------------------
def build_dataframe(input_data: dict) -> pd.DataFrame:
    """Build a DataFrame with trade-flow fields + depth columns (bid{i}p/v, ask{i}p/v)."""
    df = pd.DataFrame()

    # Trade flow fields
    trade_fields = [
        "open", "high", "low", "close", "volume", "amount", "avg_amount", "count",
        "buy_count", "sell_count", "buy_amount", "sell_amount", "buy_volume", "sell_volume",
        "large_order", "medium_order", "small_order", "large_buy", "large_sell",
        "medium_buy", "medium_sell", "small_buy", "small_sell", "vwap",
        "buy_vwap", "sell_vwap", "net_buy_amount", "net_buy_volume", "net_buy_pct",
        "net_buy_large", "net_buy_medium", "net_buy_small",
    ]
    for field in trade_fields:
        if field in input_data:
            df[field] = input_data[field]

    # Depth columns: bid{i}p, bid{i}v, ask{i}p, ask{i}v
    depth_bids = input_data.get("depth_bids", [])
    depth_asks = input_data.get("depth_asks", [])
    num_bars = len(depth_bids)

    for i in range(20):
        bid_prices = [depth_bids[t][i][0] if i < len(depth_bids[t]) else 0.0 for t in range(num_bars)]
        bid_amounts = [depth_bids[t][i][1] if i < len(depth_bids[t]) else 0.0 for t in range(num_bars)]
        ask_prices = [depth_asks[t][i][0] if i < len(depth_asks[t]) else 0.0 for t in range(num_bars)]
        ask_amounts = [depth_asks[t][i][1] if i < len(depth_asks[t]) else 0.0 for t in range(num_bars)]
        df[f"bid{i}p"] = bid_prices
        df[f"bid{i}v"] = bid_amounts
        df[f"ask{i}p"] = ask_prices
        df[f"ask{i}v"] = ask_amounts

    return df

# ---------------------------------------------------------------------------
# Rust name → Python function mapping
# ---------------------------------------------------------------------------
def build_factor_mapping(pool):
    """Map Rust factor names to Python callable functions."""
    mapping = {}

    # factor_NNN → factor_NNN(df)
    for i in range(1, 200):
        name = f"factor_{i:03d}"
        func_name = f"factor_{i:03d}"
        if hasattr(pool, func_name):
            mapping[name] = getattr(pool, func_name)

    # factor_trades_NNN → factor_trades_NNN(df)
    for i in range(1, 60):
        name = f"factor_trades_{i:03d}"
        func_name = f"factor_trades_{i:03d}"
        if hasattr(pool, func_name):
            mapping[name] = getattr(pool, func_name)

    # baseline_NNN → baseline_NNN(df)
    for i in range(1, 210):
        name = f"baseline_{i:03d}"
        func_name = f"baseline_{i:03d}"
        if hasattr(pool, func_name):
            mapping[name] = getattr(pool, func_name)

    # TD_TI_NNN, TD_MT_NNN, TD_VI_NNN, TD_PT_NNN, TD_CI_NNN, TD_PR_NNN, TD_SI_NNN
    for prefix in ["TD_TI", "TD_MT", "TD_VI", "TD_PT", "TD_CI", "TD_PR", "TD_SI"]:
        for i in range(1, 60):
            name = f"{prefix}_{i:03d}"
            if hasattr(pool, name):
                mapping[name] = getattr(pool, name)

    # TP_VPI_NNN
    for i in range(1, 30):
        name = f"TP_VPI_{i:03d}"
        if hasattr(pool, name):
            mapping[name] = getattr(pool, name)

    return mapping


# ---------------------------------------------------------------------------
# Compare a single scenario
# ---------------------------------------------------------------------------
def compare_scenario(scenario_name, scenario_data, pool, factor_mapping):
    """Compare Rust vs Python factor values for one scenario."""
    df = build_dataframe(scenario_data["input"])
    rust_fusion = scenario_data.get("fusion_factors", {})
    rust_kline = scenario_data.get("kline_factors", {})

    results = []

    # Compare fusion factors
    for rust_name, rust_val in sorted(rust_fusion.items()):
        if rust_val is None:
            continue  # Rust returned NaN / warming_up, skip

        py_name = rust_name
        py_func = factor_mapping.get(py_name)
        if py_func is None:
            results.append({
                "factor": rust_name,
                "rust": rust_val,
                "python": None,
                "abs_err": None,
                "rel_err": None,
                "status": "NO_PY_FUNC",
            })
            continue

        try:
            py_arr = py_func(df)
            py_val = py_arr[-1] if len(py_arr) > 0 else np.nan
            py_val = float(py_val)
        except Exception as e:
            results.append({
                "factor": rust_name,
                "rust": rust_val,
                "python": None,
                "abs_err": None,
                "rel_err": None,
                "status": f"PY_ERROR: {e}",
            })
            continue

        if np.isnan(py_val):
            results.append({
                "factor": rust_name,
                "rust": rust_val,
                "python": None,
                "abs_err": None,
                "rel_err": None,
                "status": "PY_NAN",
            })
            continue

        abs_err = abs(rust_val - py_val)
        denom = max(abs(rust_val), abs(py_val), 1e-15)
        rel_err = abs_err / denom

        if rel_err < 1e-6:
            status = "MATCH"
        elif rel_err < 1e-3:
            status = "CLOSE"
        elif rel_err < 0.05:
            status = "DIFF"
        else:
            status = "MISMATCH"

        results.append({
            "factor": rust_name,
            "rust": rust_val,
            "python": py_val,
            "abs_err": abs_err,
            "rel_err": rel_err,
            "status": status,
        })

    return results


# ---------------------------------------------------------------------------
# Print report
# ---------------------------------------------------------------------------
def print_report(scenario_name, results):
    """Print comparison report for one scenario."""
    if not results:
        print(f"\n{'='*80}")
        print(f"Scenario: {scenario_name} — no comparable factors")
        return

    match = [r for r in results if r["status"] == "MATCH"]
    close = [r for r in results if r["status"] == "CLOSE"]
    diff = [r for r in results if r["status"] == "DIFF"]
    mismatch = [r for r in results if r["status"] == "MISMATCH"]
    no_py = [r for r in results if r["status"] == "NO_PY_FUNC"]
    py_nan = [r for r in results if r["status"] == "PY_NAN"]
    py_err = [r for r in results if r["status"].startswith("PY_ERROR")]

    total_compared = len(match) + len(close) + len(diff) + len(mismatch)

    print(f"\n{'='*80}")
    print(f"Scenario: {scenario_name}")
    print(f"{'='*80}")
    print(f"  Rust factors with value:  {len(results)}")
    print(f"  Python function found:    {len(results) - len(no_py)}")
    print(f"  Compared (both finite):   {total_compared}")
    print(f"  ---")
    print(f"  MATCH   (rel_err < 1e-6): {len(match)}")
    print(f"  CLOSE   (rel_err < 1e-3): {len(close)}")
    print(f"  DIFF    (rel_err < 0.05): {len(diff)}")
    print(f"  MISMATCH(rel_err >= 0.05):{len(mismatch)}")
    print(f"  NO_PY_FUNC:               {len(no_py)}")
    print(f"  PY_NAN:                   {len(py_nan)}")
    print(f"  PY_ERROR:                 {len(py_err)}")

    # Show details for CLOSE/DIFF/MISMATCH
    for label, group in [("CLOSE", close), ("DIFF", diff), ("MISMATCH", mismatch)]:
        if not group:
            continue
        print(f"\n  --- {label} factors ---")
        for r in sorted(group, key=lambda x: -(x["rel_err"] or 0)):
            print(f"    {r['factor']:30s}  rust={r['rust']:>15.8g}  py={r['python']:>15.8g}  "
                  f"abs={r['abs_err']:.6g}  rel={r['rel_err']:.6g}")

    # Show PY_ERROR details
    if py_err:
        print(f"\n  --- PY_ERROR factors ---")
        for r in py_err[:10]:
            print(f"    {r['factor']:30s}  rust={r['rust']:>15.8g}  err={r['status']}")

    # Show NO_PY_FUNC (just count + sample)
    if no_py:
        samples = [r["factor"] for r in no_py[:5]]
        print(f"\n  --- NO_PY_FUNC ({len(no_py)} total, samples: {', '.join(samples)})")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <factor_test_output.json>")
        sys.exit(1)

    json_path = sys.argv[1]
    with open(json_path) as f:
        data = json.load(f)

    print("Loading Python factor pool...")
    pool = load_factor_pool()
    factor_mapping = build_factor_mapping(pool)
    print(f"  Loaded {len(factor_mapping)} Python factor functions")

    scenarios = data.get("scenarios", {})
    all_results = {}

    for scenario_name in sorted(scenarios.keys()):
        scenario_data = scenarios[scenario_name]
        results = compare_scenario(scenario_name, scenario_data, pool, factor_mapping)
        all_results[scenario_name] = results
        print_report(scenario_name, results)

    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    for name, results in sorted(all_results.items()):
        total_compared = len([r for r in results if r["status"] in ("MATCH", "CLOSE", "DIFF", "MISMATCH")])
        match_count = len([r for r in results if r["status"] == "MATCH"])
        close_count = len([r for r in results if r["status"] == "CLOSE"])
        mismatch_count = len([r for r in results if r["status"] == "MISMATCH"])
        print(f"  {name:20s}  compared={total_compared:3d}  match={match_count:3d}  "
              f"close={close_count:3d}  mismatch={mismatch_count:3d}")


if __name__ == "__main__":
    main()
