#!/usr/bin/env python3
"""Build a reproducible six-hour Binance intra-arb latency notebook."""

from __future__ import annotations

import argparse
from pathlib import Path

import nbformat as nbf


def markdown(source: str) -> dict:
    return nbf.v4.new_markdown_cell(source)


def code(source: str) -> dict:
    return nbf.v4.new_code_cell(source)


NOTEBOOK_TITLE = """# Binance intra-arb latency: recent 7 days in 6-hour windows

This notebook analyzes the local uniform_orders.parquet snapshot fetched from jp-meta-elvpn for binance-intra-arb01.

All windows are fixed UTC six-hour buckets. The first and last buckets can be partial because the export window is anchored to the remote clock. Latency distributions keep the repository helper normal-path cap of 100 ms for p50/p90/p95/p99, while reporting excluded and full-sample tails separately.

The opening-leg metrics are grouped by the dual-BBO trigger source: spot, futures, tie, or legacy_unknown. Binance futures hedge metrics use the existing from_key mapping and report unmatched hedge rows explicitly.
"""


SETUP_CELL = r'''from pathlib import Path
import importlib.util
import json
import warnings

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from IPython.display import display, Markdown

warnings.filterwarnings("ignore", category=FutureWarning)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 240)
pd.set_option("display.max_rows", 200)

REPO_ROOT = Path.cwd()
DATA_PATH = Path("data/intra_order_export_backfill/binance_intra_arb01_7d_20260815T005241Z/binance-intra-arb01/uniform_orders.parquet")
OUT_DIR = DATA_PATH.parent
NORMAL_MAX_MS = 100.0
WINDOW_FREQ = "6h"
EARLY_LATE_WINDOWS = 4

helper_path = REPO_ROOT / "skills/intra-arb-latency-analysis/scripts/analyze_intra_arb_latency.py"
spec = importlib.util.spec_from_file_location("intra_latency_helper", helper_path)
if spec is None or spec.loader is None:
    raise RuntimeError(f"cannot load helper: {helper_path}")
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)

if not DATA_PATH.exists():
    raise FileNotFoundError(DATA_PATH)

orders = helper.load_orders(DATA_PATH)
orders["ts"] = pd.to_datetime(orders["ts_us"], unit="us", utc=True)
print(f"snapshot: {DATA_PATH}")
print(f"rows={len(orders):,} columns={len(orders.columns)}")
print(f"persisted UTC range: {orders['ts'].min().isoformat()} -> {orders['ts'].max().isoformat()}")
'''


OPEN_CELL = r'''margin_new = helper.select_new_orders(orders, "BinanceMargin")
margin_new_with_fill, _positive_fill_ids = helper.select_new_with_fill(orders, margin_new)

def count_summary(frame: pd.DataFrame, label: str) -> dict:
    return {
        "subset": label,
        "rows": int(len(frame)),
        "unique_client_order_id": int(frame["client_order_id"].nunique()),
        "trigger_spot": int((frame["trigger_leg"] == "spot").sum()),
        "trigger_futures": int((frame["trigger_leg"] == "futures").sum()),
        "trigger_tie": int((frame["trigger_leg"] == "tie").sum()),
        "trigger_legacy_unknown": int((frame["trigger_leg"] == "legacy_unknown").sum()),
    }

overview = pd.DataFrame([
    count_summary(margin_new, "all-new"),
    count_summary(margin_new_with_fill, "new-with-fill"),
])
display(overview)

display(Markdown(
    f"Normal-path rule: each metric keeps non-negative observations with latency <= {NORMAL_MAX_MS:.0f} ms for quantiles. "
    "Negative observations, over-cap observations, actual maxima, and unmatched hedge rows remain visible in the tables below."
))
'''


COMMON_STATS_CELL = r'''OPEN_METRICS = {
    "signal_to_trigger_mkt": ("signal_minus_trigger_mkt_ms", "行情事件到信号"),
    "signal_to_create": ("create_minus_signal_ms", "signal到margin create"),
    "margin_new_ack": ("update_minus_create_ms", "margin NEW update到create"),
    "margin_full_ack": ("local_minus_create_ms", "margin local到create"),
}

HEDGE_METRICS = {
    "futures_create_minus_margin_local": ("futures_create_minus_margin_local_ms", "合约create到margin local"),
    "futures_create_minus_margin_update": ("futures_create_minus_margin_update_ms", "合约create到margin update"),
    "futures_new_ack": ("futures_update_minus_create_ms", "合约NEW update到create"),
    "futures_full_ack": ("futures_local_minus_create_ms", "合约local到create"),
}

def with_window(frame: pd.DataFrame, time_col: str) -> pd.DataFrame:
    out = frame.copy()
    out["window_start"] = pd.to_datetime(
        pd.to_numeric(out[time_col], errors="coerce"), unit="us", utc=True
    ).dt.floor(WINDOW_FREQ)
    return out[out["window_start"].notna()].copy()

def _summary_value(summary: dict, key: str):
    value = summary.get(key)
    return float(value) if value is not None else np.nan

def six_hour_stats(
    frame: pd.DataFrame,
    metric_specs: dict,
    time_col: str,
    group_cols: list[str] | None = None,
) -> pd.DataFrame:
    group_cols = list(group_cols or [])
    work = with_window(frame, time_col)
    rows = []
    grouped_cols = ["window_start", *group_cols]
    grouped = work.groupby(grouped_cols, dropna=False, observed=True) if grouped_cols else [((), work)]
    for keys, sub in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = {"window_start": keys[0] if grouped_cols else pd.NaT}
        for col, value in zip(group_cols, keys[1:]):
            row[col] = value
        row["candidate_rows"] = int(len(sub))
        row["unique_client_order_id"] = int(sub["client_order_id"].nunique()) if "client_order_id" in sub else np.nan
        for metric_name, (value_col, _meaning) in metric_specs.items():
            summary = helper.metric_summary(sub, value_col, NORMAL_MAX_MS)
            row[f"{metric_name}_candidate"] = summary.get("candidate_rows", 0)
            row[f"{metric_name}_normal"] = summary.get("normal_rows", 0)
            row[f"{metric_name}_negative"] = summary.get("negative_rows", 0)
            row[f"{metric_name}_over_cap"] = summary.get("gt_normal_max_rows", 0)
            row[f"{metric_name}_p50_ms"] = _summary_value(summary, "p50_ms")
            row[f"{metric_name}_p90_ms"] = _summary_value(summary, "p90_ms")
            row[f"{metric_name}_p95_ms"] = _summary_value(summary, "p95_ms")
            row[f"{metric_name}_p99_ms"] = _summary_value(summary, "p99_ms")
            row[f"{metric_name}_actual_max_ms"] = _summary_value(summary, "actual_max_ms")
            row[f"{metric_name}_gt10_all_pct"] = _summary_value(summary, "gt10_all_pct")
        rows.append(row)
    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values(["window_start", *group_cols]).reset_index(drop=True)
    return result

open_window_stats = six_hour_stats(
    margin_new,
    OPEN_METRICS,
    time_col="ts_us",
    group_cols=["trigger_leg"],
)
display(open_window_stats)
'''


HEDGE_CELL = r'''def build_binance_hedge_frame(all_orders: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    work = all_orders.copy()
    for col in ["client_order_id", "from_key", "status", "trading_venue"]:
        work[col] = work[col].astype("string")

    margin = work[work["trading_venue"].eq("BinanceMargin")].copy()
    futures = work[
        work["trading_venue"].eq("BinanceFutures")
        & work["status"].eq("NEW")
    ].copy()
    parsed = futures["from_key"].str.extract(
        r"^(?P<open_client_order_id>[^|]+)\|(?P<hedge_reason>arb_hedge_[^|]+)\|(?P<trigger_update_ts_from_key>\d+)"
    )
    futures = futures.join(parsed)
    futures["open_client_order_id"] = futures["open_client_order_id"].astype("string")
    futures["trigger_update_ts_from_key"] = pd.to_numeric(
        futures["trigger_update_ts_from_key"], errors="coerce"
    ).astype("Int64")

    margin_event_raw = margin[
        ["client_order_id", "status", "update_ts", "local_ts", "ts_us"]
    ].rename(
        columns={
            "client_order_id": "open_client_order_id",
            "status": "margin_trigger_status",
            "update_ts": "margin_trigger_update_ts",
            "local_ts": "margin_trigger_local_ts",
            "ts_us": "margin_ts_us",
        }
    )
    margin_event_raw["open_client_order_id"] = margin_event_raw["open_client_order_id"].astype("string")
    margin_event_raw["margin_trigger_update_ts"] = pd.to_numeric(
        margin_event_raw["margin_trigger_update_ts"], errors="coerce"
    ).astype("Int64")
    duplicate_keys = int(
        margin_event_raw.groupby(
            ["open_client_order_id", "margin_trigger_update_ts"], dropna=False
        ).size().gt(1).sum()
    )
    margin_event = (
        margin_event_raw.sort_values(
            ["open_client_order_id", "margin_trigger_update_ts", "margin_trigger_local_ts", "margin_ts_us"]
        )
        .drop_duplicates(["open_client_order_id", "margin_trigger_update_ts"], keep="first")
    )
    hedge = futures.merge(
        margin_event,
        how="left",
        left_on=["open_client_order_id", "trigger_update_ts_from_key"],
        right_on=["open_client_order_id", "margin_trigger_update_ts"],
        validate="many_to_one",
    )

    def diff_ms(lhs: pd.Series, rhs: pd.Series) -> pd.Series:
        return (
            pd.to_numeric(lhs, errors="coerce") - pd.to_numeric(rhs, errors="coerce")
        ) / 1000.0

    hedge["futures_create_minus_margin_local_ms"] = diff_ms(
        hedge["create_ts"], hedge["margin_trigger_local_ts"]
    )
    hedge["futures_create_minus_margin_update_ms"] = diff_ms(
        hedge["create_ts"], hedge["margin_trigger_update_ts"]
    )
    hedge["futures_update_minus_create_ms"] = diff_ms(
        hedge["update_ts"], hedge["create_ts"]
    )
    hedge["futures_local_minus_create_ms"] = diff_ms(
        hedge["local_ts"], hedge["create_ts"]
    )
    matched = hedge["margin_trigger_local_ts"].notna()
    meta = {
        "futures_new_rows": int(len(futures)),
        "futures_unique_client_order_id": int(futures["client_order_id"].nunique()),
        "parsed_rows": int(futures["trigger_update_ts_from_key"].notna().sum()),
        "matched_rows": int(matched.sum()),
        "unmatched_rows": int((~matched).sum()),
        "deduplicated_margin_trigger_rows": int(len(margin_event)),
        "duplicate_margin_trigger_keys": duplicate_keys,
        "hedge_reason_counts": futures["hedge_reason"].value_counts(dropna=False).to_dict(),
        "matched_margin_status_counts": hedge.loc[matched, "margin_trigger_status"].value_counts(dropna=False).to_dict(),
    }
    return hedge, meta

hedge, hedge_meta = build_binance_hedge_frame(orders)
display(pd.DataFrame([hedge_meta]))
hedge_window_stats = six_hour_stats(
    hedge,
    HEDGE_METRICS,
    time_col="create_ts",
    group_cols=[],
)
display(hedge_window_stats)
'''


TREND_CELL = r'''def trend_summary(
    stats: pd.DataFrame,
    metric_specs: dict,
    label: str,
    group_col: str | None = None,
) -> pd.DataFrame:
    work = stats.copy()
    if group_col is None:
        work["segment"] = "all"
    else:
        work["segment"] = work[group_col].astype("string").fillna("unknown")
    rows = []
    for metric_name in metric_specs:
        for segment, sub in work.groupby("segment", dropna=False, observed=True):
            sub = sub.sort_values("window_start")
            sub = sub[sub[f"{metric_name}_p50_ms"].notna()].copy()
            if sub.empty:
                continue
            n_edge = min(EARLY_LATE_WINDOWS, len(sub))
            early = sub.head(n_edge)
            late = sub.tail(n_edge)
            early_p50 = float(early[f"{metric_name}_p50_ms"].median())
            late_p50 = float(late[f"{metric_name}_p50_ms"].median())
            early_p99 = float(early[f"{metric_name}_p99_ms"].median())
            late_p99 = float(late[f"{metric_name}_p99_ms"].median())
            delta_p50 = late_p50 - early_p50
            delta_p99 = late_p99 - early_p99
            p50_pct = delta_p50 / max(abs(early_p50), 0.001) * 100.0
            p99_pct = delta_p99 / max(abs(early_p99), 0.001) * 100.0
            x = (sub["window_start"] - sub["window_start"].min()).dt.total_seconds() / 86400.0
            y = sub[f"{metric_name}_p50_ms"].to_numpy(dtype=float)
            slope = float(np.polyfit(x.to_numpy(dtype=float), y, 1)[0]) if len(sub) >= 2 else np.nan
            changed = (abs(delta_p99) >= 0.2 and abs(p99_pct) >= 20.0) or (
                abs(delta_p50) >= 0.1 and abs(p50_pct) >= 20.0
            )
            direction = "上升" if delta_p99 > 0.0 else "下降" if delta_p99 < 0.0 else "稳定"
            rows.append({
                "scope": label,
                "segment": str(segment),
                "metric": metric_name,
                "含义": metric_specs[metric_name][1],
                "windows": int(len(sub)),
                "early_24h_p50_ms": early_p50,
                "late_24h_p50_ms": late_p50,
                "delta_p50_ms": delta_p50,
                "delta_p50_pct": p50_pct,
                "early_24h_p99_ms": early_p99,
                "late_24h_p99_ms": late_p99,
                "delta_p99_ms": delta_p99,
                "delta_p99_pct": p99_pct,
                "p50_slope_ms_per_day": slope,
                "change_flag": "有变化" if changed else "未见明显变化",
                "direction_by_p99": direction,
            })
    return pd.DataFrame(rows).sort_values(["scope", "metric", "segment"]).reset_index(drop=True)

open_trend = trend_summary(open_window_stats, OPEN_METRICS, "margin opening", "trigger_leg")
hedge_trend = trend_summary(hedge_window_stats, HEDGE_METRICS, "futures hedge")
trend = pd.concat([open_trend, hedge_trend], ignore_index=True)
numeric_cols = trend.select_dtypes(include="number").columns
trend[numeric_cols] = trend[numeric_cols].round(4)
display(trend)

display(Markdown(
    "Change flag is a conservative screen: the median p99 change across the first versus last four six-hour windows is at least 0.2 ms and 20%, or the corresponding p50 change is at least 0.1 ms and 20%. This is a screening rule for operational review, not a causal test."
))
'''


PLOT_CELL = r'''def plot_trends(stats: pd.DataFrame, metric_specs: dict, title: str, path: Path, group_col: str | None = None):
    plot_stats = stats.copy()
    plot_stats["segment"] = "all" if group_col is None else plot_stats[group_col].astype("string").fillna("unknown")
    fig, axes = plt.subplots(2, 2, figsize=(15, 10), sharex=True)
    axes = axes.ravel()
    for ax, (metric_name, (_value_col, meaning)) in zip(axes, metric_specs.items()):
        for segment, sub in plot_stats.groupby("segment", observed=True):
            sub = sub.sort_values("window_start")
            ax.plot(sub["window_start"], sub[f"{metric_name}_p50_ms"], marker="o", linewidth=1.5, label=f"{segment} p50")
            ax.plot(sub["window_start"], sub[f"{metric_name}_p99_ms"], marker=".", linestyle="--", linewidth=1.0, label=f"{segment} p99")
        ax.set_title(metric_name)
        ax.set_ylabel("latency (ms)")
        ax.grid(True, alpha=0.25)
        ax.legend(fontsize=8, ncol=2)
    for ax in axes[-2:]:
        ax.tick_params(axis="x", rotation=30)
    fig.suptitle(title)
    fig.tight_layout()
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.show()

open_plot_path = OUT_DIR / "binance_intra_arb01_open_latency_6h.png"
hedge_plot_path = OUT_DIR / "binance_intra_arb01_hedge_latency_6h.png"
plot_trends(open_window_stats, OPEN_METRICS, "Binance intra-arb opening latency by UTC 6-hour window", open_plot_path, "trigger_leg")
plot_trends(hedge_window_stats, HEDGE_METRICS, "Binance intra-arb futures hedge latency by UTC 6-hour window", hedge_plot_path)
print(open_plot_path)
print(hedge_plot_path)
'''


EXPORT_CELL = r'''open_window_stats.to_csv(OUT_DIR / "binance_intra_arb01_open_latency_6h.csv", index=False)
hedge_window_stats.to_csv(OUT_DIR / "binance_intra_arb01_hedge_latency_6h.csv", index=False)
trend.to_csv(OUT_DIR / "binance_intra_arb01_latency_trend_summary.csv", index=False)

notebook_summary = {
    "source": "binance-intra-arb01",
    "remote_host": "jp-meta-elvpn",
    "snapshot": str(DATA_PATH),
    "window_frequency": WINDOW_FREQ,
    "normal_max_ms": NORMAL_MAX_MS,
    "rows_total": int(len(orders)),
    "margin_new_rows": int(len(margin_new)),
    "margin_new_with_fill_rows": int(len(margin_new_with_fill)),
    "hedge": hedge_meta,
    "trend_rows": int(len(trend)),
}
(OUT_DIR / "binance_intra_arb01_latency_notebook_summary.json").write_text(
    json.dumps(notebook_summary, indent=2, default=str) + "\n"
)
print(json.dumps(notebook_summary, indent=2, default=str))
'''


def build_notebook(data_path: str, output_path: Path) -> None:
    setup = SETUP_CELL.replace(
        'DATA_PATH = Path("data/intra_order_export_backfill/binance_intra_arb01_7d_20260815T005241Z/binance-intra-arb01/uniform_orders.parquet")',
        f"DATA_PATH = Path({data_path!r})",
    )
    notebook = nbf.v4.new_notebook(
        metadata={
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.12"},
        },
        cells=[
            markdown(NOTEBOOK_TITLE),
            code(setup),
            code(OPEN_CELL),
            code(COMMON_STATS_CELL),
            code(HEDGE_CELL),
            code(TREND_CELL),
            code(PLOT_CELL),
            code(EXPORT_CELL),
        ],
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(nbf.writes(notebook), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", required=True, help="uniform_orders.parquet path, relative to repo root")
    parser.add_argument("--output", required=True, help="output notebook path")
    args = parser.parse_args()
    build_notebook(args.data, Path(args.output))


if __name__ == "__main__":
    main()
