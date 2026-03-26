#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze ret_score effectiveness using only exported order quote/fill data."
    )
    parser.add_argument(
        "export_dir",
        type=Path,
        help="Directory containing uniform_orders.parquet",
    )
    return parser.parse_args()


def load_latest_orders(export_dir: Path) -> pd.DataFrame:
    uniform_path = export_dir / "uniform_orders.parquet"
    df = pq.read_table(uniform_path).to_pandas()
    df["ret_score"] = pd.to_numeric(
        df["from_key"].str.extract(r"ret_score=([-+0-9.eE]+)")[0],
        errors="coerce",
    )
    df["ret_sign"] = df["ret_score"].apply(
        lambda x: "pos" if pd.notna(x) and x > 0 else ("neg" if pd.notna(x) and x < 0 else "zero")
    )

    latest = df.sort_values("ts_us").groupby("client_order_id", as_index=False).tail(1).copy()
    latest["is_filled"] = latest["status"].isin(["FILLED", "PARTIALLY_FILLED"])
    latest["filled_qty"] = latest["amount_update"].where(latest["is_filled"], 0.0)
    latest["filled_ratio"] = latest["filled_qty"] / latest["amount_init"].replace(0, np.nan)
    latest["signed_fill_qty"] = np.where(
        latest["side"].eq("BUY"),
        latest["filled_qty"],
        -latest["filled_qty"],
    )
    return latest


def print_order_level_stats(latest: pd.DataFrame) -> None:
    print("== Order Level ==")
    print(f"latest_orders={len(latest)}")
    print("ret_sign counts:")
    print(latest["ret_sign"].value_counts(dropna=False).to_string())
    print()

    side_stats = (
        latest.groupby(["ret_sign", "side"])
        .agg(
            orders=("client_order_id", "count"),
            fill_rate=("is_filled", "mean"),
            avg_filled_ratio=("filled_ratio", "mean"),
            fully_filled_rate=("status", lambda s: (s == "FILLED").mean()),
            canceled_rate=("status", lambda s: (s == "CANCELED").mean()),
        )
        .reset_index()
    )
    print(side_stats.to_string(index=False))
    print()


def build_signal_level(latest: pd.DataFrame) -> pd.DataFrame:
    side_aggs = (
        latest.groupby(["signal_ts", "ret_sign", "side"])
        .agg(
            orders=("client_order_id", "count"),
            filled_orders=("is_filled", "sum"),
            fill_qty=("filled_qty", "sum"),
            init_qty=("amount_init", "sum"),
        )
        .reset_index()
    )
    pt = side_aggs.pivot_table(
        index=["signal_ts", "ret_sign"],
        columns="side",
        values=["orders", "filled_orders", "fill_qty", "init_qty"],
        fill_value=0.0,
    )
    pt.columns = ["_".join(col) for col in pt.columns]
    pt = pt.reset_index()
    for col in [
        "orders_BUY",
        "orders_SELL",
        "filled_orders_BUY",
        "filled_orders_SELL",
        "fill_qty_BUY",
        "fill_qty_SELL",
        "init_qty_BUY",
        "init_qty_SELL",
    ]:
        if col not in pt:
            pt[col] = 0.0

    ret_score = latest.groupby("signal_ts")["ret_score"].first().rename("ret_score")
    pt = pt.merge(ret_score, on="signal_ts", how="left")
    pt["buy_minus_sell_filled_orders"] = pt["filled_orders_BUY"] - pt["filled_orders_SELL"]
    pt["buy_minus_sell_fill_qty"] = pt["fill_qty_BUY"] - pt["fill_qty_SELL"]
    pt["total_init_qty"] = pt["init_qty_BUY"] + pt["init_qty_SELL"]
    pt["net_fill_ratio"] = pt["buy_minus_sell_fill_qty"] / pt["total_init_qty"].replace(0, np.nan)
    return pt


def print_signal_level_stats(sig: pd.DataFrame) -> None:
    print("== Signal Level ==")
    sig = sig.dropna(subset=["ret_score", "net_fill_ratio"]).copy()
    print(f"signals={len(sig)}")
    print(
        f"pearson(ret_score, net_fill_ratio)={sig['ret_score'].corr(sig['net_fill_ratio'], method='pearson'):.6f}"
    )
    print(
        f"spearman(ret_score, net_fill_ratio)={sig['ret_score'].corr(sig['net_fill_ratio'], method='spearman'):.6f}"
    )
    print()

    sign_summary = (
        sig.groupby("ret_sign")
        .agg(
            signals=("signal_ts", "count"),
            buy_more_filled_orders=("buy_minus_sell_filled_orders", lambda s: (s > 0).mean()),
            sell_more_filled_orders=("buy_minus_sell_filled_orders", lambda s: (s < 0).mean()),
            avg_buy_minus_sell_fill_qty=("buy_minus_sell_fill_qty", "mean"),
            avg_net_fill_ratio=("net_fill_ratio", "mean"),
        )
        .reset_index()
    )
    print(sign_summary.to_string(index=False))
    print()

    sig["bucket"] = pd.qcut(sig["ret_score"], 5, duplicates="drop")
    bucket_summary = (
        sig.groupby("bucket", observed=False)
        .agg(
            n=("signal_ts", "count"),
            ret_score_min=("ret_score", "min"),
            ret_score_max=("ret_score", "max"),
            avg_net_fill_ratio=("net_fill_ratio", "mean"),
            median_net_fill_ratio=("net_fill_ratio", "median"),
        )
        .reset_index(drop=True)
    )
    print("quintiles:")
    print(bucket_summary.to_string(index=False))


def main() -> int:
    args = parse_args()
    export_dir = args.export_dir.expanduser().resolve()
    latest = load_latest_orders(export_dir)
    print_order_level_stats(latest)
    signal_level = build_signal_level(latest)
    print_signal_level_stats(signal_level)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
