#!/usr/bin/env python3
"""Analyze intra-arb latency from a local uniform_orders parquet snapshot."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

TIMESTAMP_COLS = [
    "ts_us",
    "recv_ts_us",
    "create_ts",
    "update_ts",
    "signal_ts",
    "submit_ts",
    "local_ts",
    "mkt_ts",
    "signal_open_ts",
    "signal_hedge_ts",
]

SIGNAL_BBO_COLUMNS = [
    "signal_open_venue",
    "signal_open_ts",
    "signal_open_bid_price",
    "signal_open_bid_qty",
    "signal_open_ask_price",
    "signal_open_ask_qty",
    "signal_hedge_venue",
    "signal_hedge_ts",
    "signal_hedge_bid_price",
    "signal_hedge_bid_qty",
    "signal_hedge_ask_price",
    "signal_hedge_ask_qty",
]

METRICS = [
    ("signal_minus_mkt_ms", "signal_ts", "mkt_ts"),
    ("create_minus_signal_ms", "create_ts", "signal_ts"),
    ("update_minus_create_ms", "update_ts", "create_ts"),
    ("local_minus_create_ms", "local_ts", "create_ts"),
]

HEDGE_REASON = "arb_hedge_force_taker_direct"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet", required=True, help="Path to uniform_orders.parquet")
    parser.add_argument(
        "--venue",
        default="BybitMargin",
        help="Filter NEW rows by trading_venue. Use 'any' to disable filtering.",
    )
    parser.add_argument(
        "--subset",
        choices=["all-new", "new-with-fill", "both"],
        default="both",
        help="Which NEW-order subset to report.",
    )
    parser.add_argument(
        "--normal-max-ms",
        type=float,
        default=100.0,
        help="Upper bound for normal-path latency samples.",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format.",
    )
    parser.add_argument(
        "--json-out",
        help="Optional path to also write the full result as JSON.",
    )
    parser.add_argument(
        "--include-hedge",
        action="store_true",
        help="Also analyze margin trigger local_ts to taker hedge submit_ts latency when futures hedge rows can be mapped from from_key.",
    )
    return parser.parse_args()


def load_orders(path: Path) -> pd.DataFrame:
    orders = pd.read_parquet(path)
    signal_bbo_schema_kind(orders)
    for col in TIMESTAMP_COLS + ["amount_update"]:
        if col in orders.columns:
            orders[col] = pd.to_numeric(orders[col], errors="coerce")
    if "ts_us" in orders.columns:
        orders["ts"] = pd.to_datetime(orders["ts_us"], unit="us", utc=True)
    return orders.sort_values("ts_us").reset_index(drop=True)


def signal_bbo_schema_kind(frame: pd.DataFrame) -> str:
    present = set(SIGNAL_BBO_COLUMNS).intersection(frame.columns)
    if not present:
        return "legacy"
    if len(present) != len(SIGNAL_BBO_COLUMNS):
        missing = sorted(set(SIGNAL_BBO_COLUMNS) - present)
        raise ValueError(f"incomplete signal_bbo parquet schema; missing columns: {missing}")
    return "full"


def classify_trigger_market_time(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    legacy_mkt_ts = pd.to_numeric(out["mkt_ts"], errors="coerce")
    trigger_mkt_ts = legacy_mkt_ts.copy()
    trigger_leg = pd.Series("legacy_unknown", index=out.index, dtype="object")

    schema_kind = signal_bbo_schema_kind(out)
    if schema_kind == "legacy":
        record_kind = pd.Series("legacy_schema", index=out.index, dtype="object")
    else:
        open_ts = pd.to_numeric(out["signal_open_ts"], errors="coerce")
        hedge_ts = pd.to_numeric(out["signal_hedge_ts"], errors="coerce")
        open_valid = open_ts.notna() & open_ts.gt(0)
        hedge_valid = hedge_ts.notna() & hedge_ts.gt(0)
        dual_bbo = open_valid & hedge_valid
        incomplete_bbo = open_valid ^ hedge_valid

        record_kind = pd.Series(
            "legacy_or_empty_signal_bbo", index=out.index, dtype="object"
        )
        record_kind.loc[incomplete_bbo] = "incomplete_signal_bbo"
        record_kind.loc[dual_bbo] = "new_signal_bbo"

        spot_trigger = dual_bbo & open_ts.gt(hedge_ts)
        futures_trigger = dual_bbo & hedge_ts.gt(open_ts)
        tied_trigger = dual_bbo & open_ts.eq(hedge_ts)
        trigger_leg.loc[spot_trigger] = "spot"
        trigger_leg.loc[futures_trigger] = "futures"
        trigger_leg.loc[tied_trigger] = "tie"
        trigger_mkt_ts.loc[dual_bbo] = pd.concat(
            [open_ts.loc[dual_bbo], hedge_ts.loc[dual_bbo]], axis=1
        ).max(axis=1)

    out["trigger_record_kind"] = record_kind
    out["trigger_leg"] = trigger_leg
    out["trigger_mkt_ts"] = trigger_mkt_ts
    out["signal_minus_trigger_mkt_ms"] = (
        pd.to_numeric(out["signal_ts"], errors="coerce") - trigger_mkt_ts
    ) / 1000.0
    return out


def select_new_orders(orders: pd.DataFrame, venue: str) -> pd.DataFrame:
    required = ["status", "signal_ts", "mkt_ts", "create_ts", "client_order_id"]
    missing = [col for col in required if col not in orders.columns]
    if missing:
        raise ValueError(f"missing required columns: {missing}")

    legacy_mkt_ts = pd.to_numeric(orders["mkt_ts"], errors="coerce")
    valid_market_time = legacy_mkt_ts.gt(0)
    if signal_bbo_schema_kind(orders) == "full":
        open_ts = pd.to_numeric(orders["signal_open_ts"], errors="coerce")
        hedge_ts = pd.to_numeric(orders["signal_hedge_ts"], errors="coerce")
        valid_market_time |= open_ts.gt(0) & hedge_ts.gt(0)

    mask = (
        orders["status"].astype(str).eq("NEW")
        & (orders["signal_ts"] > 0)
        & valid_market_time
        & (orders["create_ts"] > 0)
    )
    if venue.lower() != "any":
        if "trading_venue" not in orders.columns:
            raise ValueError("trading_venue column is required when venue filtering is enabled")
        mask &= orders["trading_venue"].astype(str).str.lower().eq(venue.lower())

    new_orders = orders.loc[mask].copy()
    new_orders["client_order_id"] = new_orders["client_order_id"].astype(str)
    for metric_name, lhs, rhs in METRICS:
        lhs_vals = pd.to_numeric(new_orders.get(lhs), errors="coerce")
        rhs_vals = pd.to_numeric(new_orders.get(rhs), errors="coerce")
        if rhs == "mkt_ts":
            rhs_vals = rhs_vals.where(rhs_vals.gt(0))
        new_orders[metric_name] = (lhs_vals - rhs_vals) / 1000.0
    return classify_trigger_market_time(new_orders)


def select_new_with_fill(orders: pd.DataFrame, new_orders: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    filled_ids = set(
        orders.loc[orders["amount_update"].fillna(0) > 0, "client_order_id"].astype(str)
    )
    out = new_orders[new_orders["client_order_id"].isin(filled_ids)].copy()
    return out, len(filled_ids)


def metric_summary(frame: pd.DataFrame, value_col: str, normal_max_ms: float) -> dict[str, Any]:
    vals = pd.to_numeric(frame.get(value_col), errors="coerce").dropna()
    nonneg = vals[vals >= 0]
    normal = nonneg[nonneg <= normal_max_ms]

    summary: dict[str, Any] = {
        "candidate_rows": int(len(vals)),
        "nonneg_rows": int(len(nonneg)),
        "normal_rows": int(len(normal)),
        "negative_rows": int((vals < 0).sum()),
        "gt_normal_max_rows": int((nonneg > normal_max_ms).sum()),
        "normal_max_ms": normal_max_ms,
    }
    if not nonneg.empty:
        summary.update(
            actual_max_ms=float(nonneg.max()),
            gt5_rows=int((nonneg > 5).sum()),
            gt10_rows=int((nonneg > 10).sum()),
            gt5_all_pct=float((nonneg > 5).mean() * 100.0),
            gt10_all_pct=float((nonneg > 10).mean() * 100.0),
        )
    if normal.empty:
        return summary

    q = normal.quantile([0.5, 0.9, 0.95, 0.99])
    summary.update(
        mean_ms=float(normal.mean()),
        p50_ms=float(q.loc[0.5]),
        p90_ms=float(q.loc[0.9]),
        p95_ms=float(q.loc[0.95]),
        p99_ms=float(q.loc[0.99]),
        max_ms=float(normal.max()),
        gt5_pct=float((normal > 5).mean() * 100.0),
        gt10_pct=float((normal > 10).mean() * 100.0),
    )
    return summary


def subset_summary(name: str, frame: pd.DataFrame, normal_max_ms: float) -> dict[str, Any]:
    out: dict[str, Any] = {
        "subset": name,
        "rows": int(len(frame)),
        "unique_client_order_id": int(frame["client_order_id"].nunique()) if "client_order_id" in frame.columns else 0,
        "metrics": {},
    }
    for metric_name, _lhs, _rhs in METRICS:
        out["metrics"][metric_name] = metric_summary(frame, metric_name, normal_max_ms)
    out["trigger_mkt"] = trigger_mkt_summary(frame, normal_max_ms)
    return out


def trigger_mkt_summary(frame: pd.DataFrame, normal_max_ms: float) -> dict[str, Any]:
    record_kind_counts = {
        str(key): int(value)
        for key, value in frame["trigger_record_kind"].value_counts(dropna=False).items()
    }
    trigger_counts = {
        str(key): int(value)
        for key, value in frame["trigger_leg"].value_counts(dropna=False).items()
    }
    groups: dict[str, Any] = {}
    for trigger_leg in ("spot", "futures", "tie", "legacy_unknown"):
        group = frame[frame["trigger_leg"].eq(trigger_leg)]
        if group.empty:
            continue
        groups[trigger_leg] = {
            "rows": int(len(group)),
            "share_pct": float(len(group) / len(frame) * 100.0) if len(frame) else 0.0,
            "signal_minus_trigger_mkt_ms": metric_summary(
                group, "signal_minus_trigger_mkt_ms", normal_max_ms
            ),
            "create_minus_signal_ms": metric_summary(
                group, "create_minus_signal_ms", normal_max_ms
            ),
        }
    return {
        "rule": {
            "new_record": "both signal_open_ts and signal_hedge_ts are positive",
            "spot_trigger": "signal_open_ts > signal_hedge_ts",
            "futures_trigger": "signal_hedge_ts > signal_open_ts",
            "tie": "signal_open_ts == signal_hedge_ts",
            "legacy_fallback": "use mkt_ts and leave trigger_leg unknown",
        },
        "record_kind_counts": record_kind_counts,
        "trigger_counts": trigger_counts,
        "groups": groups,
    }


def build_result(
    parquet_path: Path,
    venue: str,
    normal_max_ms: float,
    new_orders: pd.DataFrame,
    new_with_fill: pd.DataFrame,
    positive_amount_rows: int,
    positive_amount_ids: int,
    subset_mode: str,
) -> dict[str, Any]:
    subsets: dict[str, Any] = {}
    if subset_mode in {"all-new", "both"}:
        subsets["all-new"] = subset_summary("all-new", new_orders, normal_max_ms)
    if subset_mode in {"new-with-fill", "both"}:
        subsets["new-with-fill"] = subset_summary("new-with-fill", new_with_fill, normal_max_ms)

    return {
        "parquet": str(parquet_path),
        "venue": venue,
        "normal_max_ms": normal_max_ms,
        "counts": {
            "all_new_rows": int(len(new_orders)),
            "all_new_unique_client_order_id": int(new_orders["client_order_id"].nunique()),
            "new_with_fill_rows": int(len(new_with_fill)),
            "new_with_fill_unique_client_order_id": int(new_with_fill["client_order_id"].nunique()),
            "new_with_fill_share_pct": (
                float(len(new_with_fill)) / float(len(new_orders)) * 100.0 if len(new_orders) else 0.0
            ),
            "positive_amount_update_rows": positive_amount_rows,
            "positive_amount_update_unique_client_order_id": positive_amount_ids,
        },
        "subsets": subsets,
    }


def hedge_metric_summary(vals: pd.Series, normal_max_ms: float) -> dict[str, Any]:
    vals = pd.to_numeric(vals, errors="coerce").dropna()
    nonneg = vals[vals >= 0]
    normal = nonneg[nonneg <= normal_max_ms]
    out: dict[str, Any] = {
        "candidate_rows": int(len(vals)),
        "nonneg_rows": int(len(nonneg)),
        "normal_rows": int(len(normal)),
        "negative_rows": int((vals < 0).sum()),
        "gt_normal_max_rows": int((nonneg > normal_max_ms).sum()),
        "normal_max_ms": normal_max_ms,
    }
    if normal.empty:
        return out
    q = normal.quantile([0.5, 0.9, 0.95, 0.99])
    out.update(
        mean_ms=float(normal.mean()),
        p50_ms=float(q.loc[0.5]),
        p90_ms=float(q.loc[0.9]),
        p95_ms=float(q.loc[0.95]),
        p99_ms=float(q.loc[0.99]),
        max_ms=float(normal.max()),
        gt1_pct=float((normal > 1).mean() * 100.0),
        gt10_pct=float((normal > 10).mean() * 100.0),
        gt100_pct=float((normal > 100).mean() * 100.0),
    )
    return out


def analyze_bybit_taker_hedge(orders: pd.DataFrame, normal_max_ms: float) -> dict[str, Any]:
    required = {
        "client_order_id",
        "from_key",
        "symbol",
        "side",
        "status",
        "trading_venue",
        "order_type",
        "price",
        "amount_init",
        "amount_update",
        "submit_ts",
        "update_ts",
        "local_ts",
        "mkt_ts",
        "signal_ts",
        "ts_us",
        "recv_ts_us",
    }
    missing = sorted(required - set(orders.columns))
    if missing:
        raise ValueError(f"hedge analysis missing required columns: {missing}")

    work = orders.copy()
    for col in ["client_order_id", "from_key", "symbol", "side", "status", "trading_venue", "order_type"]:
        work[col] = work[col].astype("string")

    margin = work[work["trading_venue"].eq("BybitMargin")].copy()
    futures = work[work["trading_venue"].eq("BybitFutures")].copy()

    parts = futures["from_key"].str.split("|", n=2, expand=True)
    futures["open_client_order_id"] = parts[0].astype("string")
    futures["hedge_reason"] = parts[1].astype("string")
    futures["trigger_update_ts_from_key"] = pd.to_numeric(parts[2], errors="coerce").astype("Int64")
    futures = futures[futures["hedge_reason"].eq(HEDGE_REASON)].copy()

    margin_event_raw = margin[
        [
            "client_order_id",
            "symbol",
            "side",
            "status",
            "order_type",
            "price",
            "amount_init",
            "amount_update",
            "submit_ts",
            "update_ts",
            "local_ts",
            "mkt_ts",
            "signal_ts",
            "from_key",
            "ts_us",
            "recv_ts_us",
        ]
    ].copy().rename(
        columns={
            "client_order_id": "open_client_order_id",
            "symbol": "margin_symbol",
            "side": "margin_side",
            "status": "margin_trigger_status",
            "order_type": "margin_order_type",
            "price": "margin_price",
            "amount_init": "margin_amount_init",
            "amount_update": "margin_amount_update",
            "submit_ts": "margin_submit_ts",
            "update_ts": "margin_trigger_update_ts",
            "local_ts": "margin_trigger_local_ts",
            "mkt_ts": "margin_mkt_ts",
            "signal_ts": "margin_signal_ts",
            "from_key": "margin_from_key",
            "ts_us": "margin_ts_us",
            "recv_ts_us": "margin_recv_ts_us",
        }
    )
    margin_event_raw["open_client_order_id"] = margin_event_raw["open_client_order_id"].astype("string")
    margin_event_raw["margin_trigger_update_ts"] = pd.to_numeric(
        margin_event_raw["margin_trigger_update_ts"], errors="coerce"
    ).astype("Int64")
    margin_event_raw["trigger_event_dup_count"] = margin_event_raw.groupby(
        ["open_client_order_id", "margin_trigger_update_ts"]
    )["margin_trigger_update_ts"].transform("size")

    margin_event = (
        margin_event_raw.sort_values(
            [
                "open_client_order_id",
                "margin_trigger_update_ts",
                "margin_trigger_local_ts",
                "margin_ts_us",
                "margin_recv_ts_us",
            ]
        ).drop_duplicates(["open_client_order_id", "margin_trigger_update_ts"], keep="first")
    )

    hedge = futures.merge(
        margin_event,
        how="left",
        left_on=["open_client_order_id", "trigger_update_ts_from_key"],
        right_on=["open_client_order_id", "margin_trigger_update_ts"],
        suffixes=("_fut", "_margin"),
    )

    hedge = hedge.rename(
        columns={
            "client_order_id": "futures_client_order_id",
            "symbol": "futures_symbol",
            "side": "futures_side",
            "status": "futures_status",
            "order_type": "futures_order_type",
            "price": "futures_price",
            "amount_init": "futures_amount_init",
            "amount_update": "futures_amount_update",
            "submit_ts": "futures_submit_ts",
            "update_ts": "futures_update_ts",
            "local_ts": "futures_local_ts",
            "mkt_ts": "futures_mkt_ts",
            "signal_ts": "futures_signal_ts",
            "from_key": "futures_from_key",
        }
    )

    def diff_ms(a: pd.Series, b: pd.Series) -> pd.Series:
        return (pd.to_numeric(a, errors="coerce") - pd.to_numeric(b, errors="coerce")) / 1000.0

    hedge["futures_submit_minus_margin_local_ms"] = diff_ms(
        hedge["futures_submit_ts"], hedge["margin_trigger_local_ts"]
    )
    hedge["futures_submit_minus_trigger_update_ms"] = diff_ms(
        hedge["futures_submit_ts"], hedge["trigger_update_ts_from_key"]
    )
    hedge["futures_local_minus_submit_ms"] = diff_ms(
        hedge["futures_local_ts"], hedge["futures_submit_ts"]
    )
    hedge["futures_update_minus_submit_ms"] = diff_ms(
        hedge["futures_update_ts"], hedge["futures_submit_ts"]
    )

    result = {
        "hedge_reason": HEDGE_REASON,
        "rows_futures": int(len(futures)),
        "rows_margin_events_raw": int(len(margin_event_raw)),
        "rows_margin_events_dedup": int(len(margin_event)),
        "rows_matched": int(len(hedge)),
        "matched_open_client_order_id": int(hedge["open_client_order_id"].nunique()),
        "duplicate_margin_trigger_keys": int(
            margin_event_raw.groupby(["open_client_order_id", "margin_trigger_update_ts"]).size().gt(1).sum()
        ),
        "metrics": {
            "futures_submit_minus_margin_local_ms": hedge_metric_summary(
                hedge["futures_submit_minus_margin_local_ms"], normal_max_ms
            ),
            "futures_submit_minus_trigger_update_ms": hedge_metric_summary(
                hedge["futures_submit_minus_trigger_update_ms"], normal_max_ms
            ),
            "futures_local_minus_submit_ms": hedge_metric_summary(
                hedge["futures_local_minus_submit_ms"], normal_max_ms
            ),
            "futures_update_minus_submit_ms": hedge_metric_summary(
                hedge["futures_update_minus_submit_ms"], normal_max_ms
            ),
        },
    }

    status_summary = {}
    normal = hedge[
        pd.to_numeric(hedge["futures_submit_minus_margin_local_ms"], errors="coerce").between(0, 1, inclusive="both")
    ].copy()
    if not normal.empty:
        grouped = normal.groupby("margin_trigger_status")["futures_submit_minus_margin_local_ms"]
        for status, values in grouped:
            q = values.quantile([0.5, 0.9, 0.95, 1.0])
            status_summary[str(status)] = {
                "n": int(len(values)),
                "p50_ms": float(q.loc[0.5]),
                "p90_ms": float(q.loc[0.9]),
                "p95_ms": float(q.loc[0.95]),
                "max_ms": float(q.loc[1.0]),
            }
    result["normal_le1ms_by_margin_status"] = status_summary
    return result


def print_text(result: dict[str, Any]) -> None:
    print("Snapshot")
    print(f"  parquet: {result['parquet']}")
    print(f"  venue: {result['venue']}")
    print(f"  normal_max_ms: {result['normal_max_ms']}")
    counts = result["counts"]
    print("Counts")
    print(f"  all_new_rows: {counts['all_new_rows']}")
    print(f"  new_with_fill_rows: {counts['new_with_fill_rows']}")
    print(f"  new_with_fill_share_pct: {counts['new_with_fill_share_pct']:.3f}")
    print(f"  positive_amount_update_rows: {counts['positive_amount_update_rows']}")

    metric_labels = {
        "signal_minus_mkt_ms": "signal_ts - mkt_ts",
        "create_minus_signal_ms": "create_ts - signal_ts",
        "update_minus_create_ms": "update_ts - create_ts",
        "local_minus_create_ms": "local_ts - create_ts",
    }
    for subset_name, subset in result["subsets"].items():
        print(f"Subset: {subset_name}")
        print(f"  rows: {subset['rows']}")
        print(f"  unique_client_order_id: {subset['unique_client_order_id']}")
        for metric_name in (
            "signal_minus_mkt_ms",
            "create_minus_signal_ms",
            "update_minus_create_ms",
            "local_minus_create_ms",
        ):
            stats = subset["metrics"][metric_name]
            print(f"  {metric_labels[metric_name]}")
            print(
                "    "
                f"candidate={stats['candidate_rows']} nonneg={stats['nonneg_rows']} "
                f"normal={stats['normal_rows']} neg={stats['negative_rows']} "
                f"gt_max={stats['gt_normal_max_rows']}"
            )
            if "p50_ms" in stats:
                print(
                    "    "
                    f"p50={stats['p50_ms']:.3f} p90={stats['p90_ms']:.3f} "
                    f"p95={stats['p95_ms']:.3f} p99={stats['p99_ms']:.3f} "
                    f"normal_max={stats['max_ms']:.3f} "
                    f"actual_max={stats['actual_max_ms']:.3f} "
                    f">5ms={stats['gt5_all_pct']:.3f}% "
                    f">10ms={stats['gt10_all_pct']:.3f}%"
                )
        trigger_mkt = subset["trigger_mkt"]
        print("  trigger mkt classification")
        print(f"    record_kind_counts={trigger_mkt['record_kind_counts']}")
        print(f"    trigger_counts={trigger_mkt['trigger_counts']}")
        for trigger_leg, group in trigger_mkt["groups"].items():
            trigger_stats = group["signal_minus_trigger_mkt_ms"]
            internal_stats = group["create_minus_signal_ms"]
            print(
                "    "
                f"{trigger_leg}: rows={group['rows']} share={group['share_pct']:.3f}%"
            )
            if "p50_ms" in trigger_stats:
                print(
                    "      "
                    f"signal-trigger_mkt p50={trigger_stats['p50_ms']:.3f} "
                    f"p90={trigger_stats['p90_ms']:.3f} "
                    f"p95={trigger_stats['p95_ms']:.3f} "
                    f"p99={trigger_stats['p99_ms']:.3f} "
                    f"normal_max={trigger_stats['max_ms']:.3f} "
                    f"actual_max={trigger_stats['actual_max_ms']:.3f}"
                )
            if "p50_ms" in internal_stats:
                print(
                    "      "
                    f"create-signal p50={internal_stats['p50_ms']:.3f} "
                    f"p90={internal_stats['p90_ms']:.3f} "
                    f"p95={internal_stats['p95_ms']:.3f} "
                    f"p99={internal_stats['p99_ms']:.3f} "
                    f"normal_max={internal_stats['max_ms']:.3f} "
                    f"actual_max={internal_stats['actual_max_ms']:.3f}"
                )
    hedge = result.get("hedge")
    if hedge:
        print("Hedge")
        print(f"  rows_futures: {hedge['rows_futures']}")
        print(f"  rows_matched: {hedge['rows_matched']}")
        print(f"  matched_open_client_order_id: {hedge['matched_open_client_order_id']}")
        print(f"  duplicate_margin_trigger_keys: {hedge['duplicate_margin_trigger_keys']}")
        hedge_labels = {
            "futures_submit_minus_margin_local_ms": "futures submit_ts - margin local_ts",
            "futures_submit_minus_trigger_update_ms": "futures submit_ts - margin update_ts(from_key)",
            "futures_local_minus_submit_ms": "futures local_ts - futures submit_ts",
            "futures_update_minus_submit_ms": "futures update_ts - futures submit_ts",
        }
        for key in (
            "futures_submit_minus_margin_local_ms",
            "futures_submit_minus_trigger_update_ms",
            "futures_local_minus_submit_ms",
            "futures_update_minus_submit_ms",
        ):
            stats = hedge["metrics"][key]
            print(f"  {hedge_labels[key]}")
            print(
                "    "
                f"candidate={stats['candidate_rows']} nonneg={stats['nonneg_rows']} "
                f"normal={stats['normal_rows']} neg={stats['negative_rows']} "
                f"gt_max={stats['gt_normal_max_rows']}"
            )
            if "p50_ms" in stats:
                print(
                    "    "
                    f"p50={stats['p50_ms']:.3f} p90={stats['p90_ms']:.3f} "
                    f"p95={stats['p95_ms']:.3f} p99={stats['p99_ms']:.3f} "
                    f"max={stats['max_ms']:.3f} >1ms={stats['gt1_pct']:.3f}% "
                    f">10ms={stats['gt10_pct']:.3f}% >100ms={stats['gt100_pct']:.3f}%"
                )
        if hedge.get("normal_le1ms_by_margin_status"):
            print("  normal <=1ms by margin status")
            for status, stats in sorted(hedge["normal_le1ms_by_margin_status"].items()):
                print(
                    "    "
                    f"{status}: n={stats['n']} p50={stats['p50_ms']:.3f} "
                    f"p90={stats['p90_ms']:.3f} p95={stats['p95_ms']:.3f} max={stats['max_ms']:.3f}"
                )


def main() -> None:
    args = parse_args()
    parquet_path = Path(args.parquet).expanduser().resolve()
    orders = load_orders(parquet_path)
    new_orders = select_new_orders(orders, args.venue)
    new_with_fill, positive_amount_ids = select_new_with_fill(orders, new_orders)
    positive_amount_rows = int((orders["amount_update"].fillna(0) > 0).sum())

    result = build_result(
        parquet_path=parquet_path,
        venue=args.venue,
        normal_max_ms=args.normal_max_ms,
        new_orders=new_orders,
        new_with_fill=new_with_fill,
        positive_amount_rows=positive_amount_rows,
        positive_amount_ids=positive_amount_ids,
        subset_mode=args.subset,
    )
    if args.include_hedge:
        result["hedge"] = analyze_bybit_taker_hedge(orders, args.normal_max_ms)

    if args.json_out:
        Path(args.json_out).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")

    if args.format == "json":
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print_text(result)


if __name__ == "__main__":
    main()
