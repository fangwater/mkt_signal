#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Print rolling_metrics thresholds from Redis HASH as a three-line table.

Reads
  - Redis HASH `rolling_metrics_thresholds_{open_venue}_{hedge_venue}` where
    field = symbol_pair (e.g. "binance-margin_binance-futures::BTCUSDT")
    value = JSON payload produced by rolling_metrics service.

Outputs
  - 分别为 spread_rate、bidask_sr、askbid_sr 打印三线表；
    每张表包含：symbol、update_tp、sample_size、对应因子实时值，以及分位阈值列。
  - Null / missing values render as '-' by default.

示例：
  python scripts/print_fr_rolling_metrics_thresholds.py --open-venue binance-margin --hedge-venue binance-futures
  python scripts/print_fr_rolling_metrics_thresholds.py --open-venue okex-margin --hedge-venue okex-futures --symbol BTCUSDT
  # 也可不带参数，脚本会基于当前目录名推断（形如 binance-margin-binance-futures）
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

VENUE_RE = r"[a-z0-9]+-(?:margin|futures|spot|swap|perp|perpetual)"


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def infer_venues_from_cwd() -> Optional[Tuple[str, str]]:
    name = Path.cwd().name.lower()
    matched = re.fullmatch(rf"({VENUE_RE})[-_]({VENUE_RE})", name)
    if not matched:
        return None
    return matched.group(1), matched.group(2)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Print Redis hash rolling_metrics_thresholds_{open_venue}_{hedge_venue} as a three-line table（可省略 open/hedge，默认按目录推断 margin/futures）"
        )
    )
    p.add_argument("--open-venue", help="open 侧 venue（如 binance-margin）")
    p.add_argument("--hedge-venue", help="hedge 侧 venue（如 binance-futures）")
    p.add_argument(
        "--symbol",
        help="仅打印单个 symbol（匹配 base_symbol 或字段名 / 键尾部）",
    )
    p.add_argument(
        "--symbols",
        nargs="*",
        help="If set, only include rows where base_symbol matches any provided symbol",
    )
    p.add_argument(
        "--tsfmt",
        choices=["raw", "iso"],
        default="iso",
        help="Format for update_tp column (default: iso)",
    )
    p.add_argument(
        "--na",
        default="-",
        help="Placeholder for missing/NaN values (default: '-')",
    )
    p.add_argument(
        "--sort",
        choices=["symbol_pair", "base_symbol"],
        default="base_symbol",
        help="Sort rows by this field (default: base_symbol)",
    )
    args = p.parse_args()

    open_venue = args.open_venue
    hedge_venue = args.hedge_venue
    if not open_venue and not hedge_venue:
        inferred = infer_venues_from_cwd()
        if inferred:
            open_venue, hedge_venue = inferred
            print(
                f"[INFO] 未提供 open/hedge，基于目录推断: open={open_venue}, hedge={hedge_venue}",
                file=sys.stderr,
            )
    if not open_venue or not hedge_venue:
        p.error("需要 --open-venue 与 --hedge-venue，或在目录名使用 <open-venue>-<hedge-venue> 以自动推断")

    args.open_venue = open_venue
    args.hedge_venue = hedge_venue
    return args


def connect_redis(args: argparse.Namespace):
    redis = try_import_redis()
    if redis is None:
        return None
    return redis.Redis(
        host="127.0.0.1",
        port=6379,
        db=0,
        password=None,
    )


def read_hash(rds, key: str) -> Dict[str, Dict]:
    result: Dict[str, Dict] = {}
    data = rds.hgetall(key)
    for k, v in data.items():
        field = k.decode("utf-8", "ignore") if isinstance(k, bytes) else str(k)
        val = v.decode("utf-8", "ignore") if isinstance(v, bytes) else str(v)
        try:
            obj = json.loads(val)
            if isinstance(obj, dict):
                result[field] = obj
        except Exception:
            # Skip non-JSON entries silently
            continue
    return result


def is_nan(value: object) -> bool:
    return isinstance(value, float) and math.isnan(value)


def format_number(value: Optional[float], na: str) -> str:
    if value is None or is_nan(value):
        return na
    try:
        num = float(value)
        text = f"{num:.8f}".rstrip("0").rstrip(".")
        return text or "0"
    except Exception:
        return na


def format_ts(value: Optional[int], mode: str, na: str) -> str:
    if value is None:
        return na
    try:
        ts_ms = int(value)
    except Exception:
        return na
    if mode == "raw":
        return str(ts_ms)
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts_ms)


def filter_symbols(rows: Dict[str, Dict], symbols: Optional[List[str]]) -> Dict[str, Dict]:
    if not symbols:
        return rows
    symbol_set = {s.upper() for s in symbols if s}
    filtered: Dict[str, Dict] = {}
    for key, obj in rows.items():
        base_value = obj.get("base_symbol") or obj.get("symbol") or ""
        base = str(base_value).upper()
        key_upper = str(key).upper()
        key_tail = key_upper.split("::")[-1]
        if base in symbol_set or key_upper in symbol_set or key_tail in symbol_set:
            filtered[key] = obj
    return filtered


def sort_items(data: Dict[str, Dict], sort_field: str):
    if sort_field == "base_symbol":
        return sorted(
            data.items(),
            key=lambda item: (str(item[1].get("base_symbol", "")), item[0]),
        )
    return sorted(data.items(), key=lambda item: item[0])


def collect_quantile_columns(
    data: Dict[str, Dict], quantile_key: str, display_prefix: str
) -> List[str]:
    columns: Dict[str, Optional[float]] = {}
    for obj in data.values():
        entries = obj.get(quantile_key)
        if not isinstance(entries, list):
            continue
        for item in entries:
            if not isinstance(item, dict):
                continue
            col_name, order_key = quantile_column_key(item)
            if col_name is None:
                continue
            full_name = f"{display_prefix}_{col_name}"
            if full_name not in columns:
                columns[full_name] = order_key

    def sort_key(item: Tuple[str, Optional[float]]):
        name, order = item
        if order is not None:
            return (0, int(round(order * 100)))
        suffix = name.rpartition("_")[2]
        try:
            return (1, int(suffix))
        except ValueError:
            return (2, name)

    return [col for col, _ in sorted(columns.items(), key=sort_key)]


def quantile_column_key(item: Dict[str, Any]) -> Tuple[Optional[str], Optional[float]]:
    quantile = item.get("quantile")
    label = item.get("label")
    if isinstance(quantile, (int, float)):
        try:
            q = float(quantile)
        except Exception:
            q = None
        if q is not None:
            if q > 1.0:
                q /= 100.0
            if 0.0 <= q <= 1.0:
                return str(int(round(q * 100))), q
    if isinstance(label, str) and label:
        return label, None
    return None, None


def build_quantile_map(entries: Any, display_prefix: str) -> Dict[str, Optional[float]]:
    result: Dict[str, Optional[float]] = {}
    if not isinstance(entries, list):
        return result
    for item in entries:
        if not isinstance(item, dict):
            continue
        col_name, _ = quantile_column_key(item)
        if col_name is None:
            continue
        full_name = f"{display_prefix}_{col_name}"
        threshold = item.get("threshold")
        if threshold is None:
            result[full_name] = None
        else:
            try:
                result[full_name] = float(threshold)
            except Exception:
                result[full_name] = None
    return result


def build_factor_table(
    data: Dict[str, Dict],
    sort_field: str,
    value_field: str,
    quantile_key: str,
    value_header: str,
    quantile_prefix: str,
    na: str,
    tsfmt: str,
) -> Tuple[List[str], List[List[str]]]:
    quantile_columns = collect_quantile_columns(data, quantile_key, quantile_prefix)
    headers = [
        "symbol",
        "update_tp",
        "sample_size",
        value_header,
    ]
    headers.extend(quantile_columns)
    rows: List[List[str]] = []
    for _field, obj in sort_items(data, sort_field):
        base_symbol = obj.get("base_symbol") or obj.get("symbol") or "-"
        update_str = format_ts(obj.get("update_tp"), tsfmt, na)
        sample_str = str(obj.get("sample_size") or 0)
        value = format_number(obj.get(value_field), na)
        row = [
            base_symbol,
            update_str,
            sample_str,
            value,
        ]
        quant_map = build_quantile_map(obj.get(quantile_key), quantile_prefix)
        for col in quantile_columns:
            row.append(format_number(quant_map.get(col), na))
        rows.append(row)
    return headers, rows


def compute_col_widths(headers: List[str], rows: List[List[str]]) -> List[int]:
    widths = [len(h) for h in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))
    return widths


def print_three_line_table(headers: List[str], rows: List[List[str]]) -> None:
    widths = compute_col_widths(headers, rows)

    def fmt(values: List[str]) -> str:
        cells: List[str] = []
        for idx, val in enumerate(values):
            cells.append(val.ljust(widths[idx]))
        return "  ".join(cells)

    header_line = fmt(headers)
    top_rule = "=" * len(header_line)
    mid_rule = "-" * len(header_line)
    bot_rule = "=" * len(header_line)

    print(top_rule)
    print(header_line)
    print(mid_rule)
    for row in rows:
        print(fmt(row))
    print(bot_rule)


def main() -> int:
    args = parse_args()
    rds = connect_redis(args)
    if rds is None:
        print("redis 包未安装，请 `pip install redis` 后重试。", file=sys.stderr)
        return 2

    open_venue = args.open_venue.strip()
    hedge_venue = args.hedge_venue.strip()
    if not open_venue or not hedge_venue:
        print("open-venue 和 hedge-venue 均不能为空。", file=sys.stderr)
        return 1
    hash_key = f"rolling_metrics_thresholds_{open_venue}_{hedge_venue}"
    print(f"📍 Reading from Redis hash: {hash_key}")
    print("📍 Redis: 127.0.0.1:6379/0\n")

    data = read_hash(rds, hash_key)
    if not data:
        print(f"⚠️  Redis HASH '{hash_key}' 为空或不存在。")
        return 0

    target_symbols: List[str] = []
    if args.symbol:
        target_symbols.append(args.symbol)
    if args.symbols:
        target_symbols.extend(args.symbols)

    filtered = filter_symbols(data, target_symbols or None)
    if not filtered:
        if target_symbols:
            joined = ", ".join(target_symbols)
            print(f"未匹配到指定的 symbol：{joined}")
        else:
            print("未匹配到任何 symbol。")
        return 0

    tables = [
        {
            "label": "spread_rate",
            "value_field": "spread_rate",
            "value_header": "spread_rate",
            "quantile_key": "spread_quantiles",
            "quantile_prefix": "spread",
        },
        {
            "label": "bidask_sr",
            "value_field": "bidask_sr",
            "value_header": "bidask_sr",
            "quantile_key": "bidask_quantiles",
            "quantile_prefix": "bidask",
        },
        {
            "label": "askbid_sr",
            "value_field": "askbid_sr",
            "value_header": "askbid_sr",
            "quantile_key": "askbid_quantiles",
            "quantile_prefix": "askbid",
        },
    ]

    first = True
    for spec in tables:
        headers, rows = build_factor_table(
            filtered,
            args.sort,
            spec["value_field"],
            spec["quantile_key"],
            spec["value_header"],
            spec["quantile_prefix"],
            args.na,
            args.tsfmt,
        )
        if not first:
            print()
        print(f"## {spec['label']}")
        if rows:
            print_three_line_table(headers, rows)
        else:
            print("(no data)")
        first = False
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
