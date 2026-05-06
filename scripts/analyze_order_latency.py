#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


QUANTILES = [0.5, 0.9, 0.95, 0.99]
TRADE_STATUSES = {"PARTIALLY_FILLED", "FILLED"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze order/fill latency from order_export parquet directories."
    )
    parser.add_argument(
        "export_dirs",
        nargs="+",
        type=Path,
        help="Directories containing uniform_orders.parquet and optional *_updates_unmatched.parquet",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=8,
        help="Print the worst N uniform trade update_ts->local outliers per export dir.",
    )
    return parser.parse_args()


def read_parquet_if_exists(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    return pq.read_table(path).to_pandas()


def infer_label(export_dir: Path) -> str:
    parts = export_dir.resolve().parts
    if len(parts) >= 2:
        return parts[-2]
    return export_dir.name


def to_ms(delta_us: pd.Series) -> pd.Series:
    return pd.to_numeric(delta_us, errors="coerce") / 1_000.0


def finite_values(values: pd.Series) -> pd.Series:
    values = pd.to_numeric(values, errors="coerce")
    return values[np.isfinite(values)]


def latency_stats(values: pd.Series) -> dict[str, object]:
    vals = finite_values(values)
    if vals.empty:
        return {
            "n": 0,
            "negative": 0,
            "min": np.nan,
            "p50": np.nan,
            "p90": np.nan,
            "p95": np.nan,
            "p99": np.nan,
            "max": np.nan,
            "mean": np.nan,
        }

    qs = vals.quantile(QUANTILES)
    return {
        "n": int(vals.size),
        "negative": int((vals < 0).sum()),
        "min": vals.min(),
        "p50": qs.loc[0.5],
        "p90": qs.loc[0.9],
        "p95": qs.loc[0.95],
        "p99": qs.loc[0.99],
        "max": vals.max(),
        "mean": vals.mean(),
    }


def format_float(value: object) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.3f}"


def print_stats_table(rows: list[dict[str, object]]) -> None:
    if not rows:
        print("(no latency rows)")
        return
    df = pd.DataFrame(rows)
    for col in ["min", "p50", "p90", "p95", "p99", "max", "mean"]:
        df[col] = df[col].map(format_float)
    print(df.to_string(index=False))


def status_counts(df: pd.DataFrame) -> str:
    if df.empty or "status" not in df:
        return "n/a"
    counts = df["status"].value_counts(dropna=False).head(8)
    return ", ".join(f"{k}={v}" for k, v in counts.items())


def symbol_counts(df: pd.DataFrame) -> str:
    if df.empty or "symbol" not in df:
        return "n/a"
    counts = df["symbol"].value_counts(dropna=False).head(8)
    return ", ".join(f"{k}={v}" for k, v in counts.items())


def valid_positive_ts(df: pd.DataFrame, columns: Iterable[str]) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    for col in columns:
        mask &= pd.to_numeric(df[col], errors="coerce").fillna(0) > 0
    return mask


def build_uniform_latency_rows(uniform: pd.DataFrame) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    if uniform.empty:
        return rows

    all_update_mask = valid_positive_ts(uniform, ["recv_ts_us", "update_ts"])
    trade_mask = (
        all_update_mask
        & uniform["status"].isin(TRADE_STATUSES)
        & (pd.to_numeric(uniform["amount_update"], errors="coerce").fillna(0.0) > 0.0)
    )
    signal_mask = all_update_mask & valid_positive_ts(uniform, ["signal_ts"])

    metrics = {
        "uniform.update_to_local_ms": to_ms(
            uniform.loc[all_update_mask, "recv_ts_us"] - uniform.loc[all_update_mask, "update_ts"]
        ),
        "uniform.trade_update_to_local_ms": to_ms(
            uniform.loc[trade_mask, "recv_ts_us"] - uniform.loc[trade_mask, "update_ts"]
        ),
        "uniform.signal_to_update_ms": to_ms(
            uniform.loc[signal_mask, "update_ts"] - uniform.loc[signal_mask, "signal_ts"]
        ),
        "uniform.signal_to_local_ms": to_ms(
            uniform.loc[signal_mask, "recv_ts_us"] - uniform.loc[signal_mask, "signal_ts"]
        ),
    }

    ordered = uniform.loc[signal_mask].sort_values(["client_order_id", "recv_ts_us"])
    if not ordered.empty:
        first = ordered.groupby("client_order_id", as_index=False).first()
        metrics["order_first.signal_to_first_update_ms"] = to_ms(
            first["update_ts"] - first["signal_ts"]
        )
        metrics["order_first.first_update_to_local_ms"] = to_ms(
            first["recv_ts_us"] - first["update_ts"]
        )
        metrics["order_first.signal_to_first_local_ms"] = to_ms(
            first["recv_ts_us"] - first["signal_ts"]
        )

    for metric, values in metrics.items():
        row = {"metric": metric}
        row.update(latency_stats(values))
        rows.append(row)
    return rows


def build_update_latency_rows(
    order_updates: pd.DataFrame, trade_updates: pd.DataFrame
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    if not order_updates.empty:
        mask = valid_positive_ts(order_updates, ["ts_us", "event_time"])
        row = {"metric": "unmatched_order.event_to_local_ms"}
        row.update(latency_stats(to_ms(order_updates.loc[mask, "ts_us"] - order_updates.loc[mask, "event_time"])))
        rows.append(row)

    if not trade_updates.empty:
        event_mask = valid_positive_ts(trade_updates, ["ts_us", "event_time"])
        trade_mask = valid_positive_ts(trade_updates, ["ts_us", "trade_time"])
        event_trade_mask = valid_positive_ts(trade_updates, ["event_time", "trade_time"])
        metrics = {
            "unmatched_trade.event_to_local_ms": to_ms(
                trade_updates.loc[event_mask, "ts_us"] - trade_updates.loc[event_mask, "event_time"]
            ),
            "unmatched_trade.trade_time_to_local_ms": to_ms(
                trade_updates.loc[trade_mask, "ts_us"] - trade_updates.loc[trade_mask, "trade_time"]
            ),
            "unmatched_trade.trade_time_to_event_ms": to_ms(
                trade_updates.loc[event_trade_mask, "event_time"]
                - trade_updates.loc[event_trade_mask, "trade_time"]
            ),
        }
        for metric, values in metrics.items():
            row = {"metric": metric}
            row.update(latency_stats(values))
            rows.append(row)
    return rows


def print_worst_uniform_trades(uniform: pd.DataFrame, top: int) -> None:
    if top <= 0 or uniform.empty:
        return
    mask = (
        valid_positive_ts(uniform, ["recv_ts_us", "update_ts"])
        & uniform["status"].isin(TRADE_STATUSES)
        & (pd.to_numeric(uniform["amount_update"], errors="coerce").fillna(0.0) > 0.0)
    )
    trades = uniform.loc[mask].copy()
    if trades.empty:
        return
    trades["update_to_local_ms"] = to_ms(trades["recv_ts_us"] - trades["update_ts"])
    cols = [
        "update_to_local_ms",
        "symbol",
        "client_order_id",
        "status",
        "amount_update",
        "update_ts",
        "recv_ts_us",
        "from_key",
    ]
    worst = trades.sort_values("update_to_local_ms", ascending=False).head(top)[cols]
    worst["update_to_local_ms"] = worst["update_to_local_ms"].map(format_float)
    print(f"\nWorst uniform trade update_to_local_ms top {top}:")
    print(worst.to_string(index=False))


def analyze_export_dir(export_dir: Path, top: int) -> None:
    export_dir = export_dir.expanduser().resolve()
    label = infer_label(export_dir)
    uniform = read_parquet_if_exists(export_dir / "uniform_orders.parquet")
    order_updates = read_parquet_if_exists(export_dir / "order_updates_unmatched.parquet")
    trade_updates = read_parquet_if_exists(export_dir / "trade_updates_unmatched.parquet")

    print(f"\n== {label} ==")
    print(f"dir={export_dir}")
    if uniform.empty:
        print("uniform_orders rows=0")
    else:
        orders = uniform["client_order_id"].nunique(dropna=True)
        trade_rows = int(
            (
                uniform["status"].isin(TRADE_STATUSES)
                & (pd.to_numeric(uniform["amount_update"], errors="coerce").fillna(0.0) > 0.0)
            ).sum()
        )
        print(
            "uniform_orders "
            f"rows={len(uniform)} orders={orders} trade_rows={trade_rows} "
            f"symbols_top=[{symbol_counts(uniform)}] status_top=[{status_counts(uniform)}]"
        )
    print(
        f"order_updates_unmatched rows={len(order_updates)} "
        f"trade_updates_unmatched rows={len(trade_updates)}"
    )

    rows = build_uniform_latency_rows(uniform)
    rows.extend(build_update_latency_rows(order_updates, trade_updates))
    print("\nLatency ms:")
    print_stats_table(rows)
    print_worst_uniform_trades(uniform, top)


def main() -> int:
    args = parse_args()
    for export_dir in args.export_dirs:
        analyze_export_dir(export_dir, args.top)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
