#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Print rolling_metrics thresholds from Redis HASH as a three-line table.

Reads
  - Redis HASH (default `rolling_metrics_thresholds`) where
    field = symbol_pair (e.g. "binance_binance-futures::BTCUSDT")
    value = JSON payload produced by rolling_metrics service.

Outputs
  - Three-line table (top/header/bottom rule) with columns:
      symbol_pair, base_symbol, update_tp, sample_size,
      bidask_sr, askbid_sr, bidask_lower, bidask_upper,
      askbid_lower, askbid_upper.
  - Null / missing values render as '-' by default.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Print Redis hash rolling_metrics_thresholds as a three-line table"
    )
    p.add_argument(
        "--redis-url",
        default=os.environ.get("REDIS_URL"),
        help="Redis URL, e.g. redis://:pwd@host:6379/0 (overrides host/port/db)",
    )
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
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
        "--key",
        default="rolling_metrics_thresholds",
        help="Redis HASH key to read (default: rolling_metrics_thresholds)",
    )
    p.add_argument(
        "--sort",
        choices=["symbol_pair", "base_symbol"],
        default="base_symbol",
        help="Sort rows by this field (default: base_symbol)",
    )
    return p.parse_args()


def connect_redis(args: argparse.Namespace):
    redis = try_import_redis()
    if redis is None:
        return None
    if args.redis_url:
        return redis.from_url(args.redis_url)
    return redis.Redis(
        host=args.host,
        port=args.port,
        db=args.db,
        password=args.password,
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
    symbol_set = set(symbols)
    filtered: Dict[str, Dict] = {}
    for key, obj in rows.items():
        base = obj.get("base_symbol") or obj.get("symbol") or ""
        if base in symbol_set or key in symbol_set:
            filtered[key] = obj
    return filtered


def build_matrix(
    data: Dict[str, Dict], sort_field: str, na: str, tsfmt: str
) -> Tuple[List[str], List[List[str]]]:
    headers = [
        "symbol_pair",
        "base_symbol",
        "update_tp",
        "sample_size",
        "bidask_sr",
        "askbid_sr",
        "bidask_lower",
        "bidask_upper",
        "askbid_lower",
        "askbid_upper",
    ]
    if sort_field == "base_symbol":
        sorted_items = sorted(
            data.items(), key=lambda item: (str(item[1].get("base_symbol", "")), item[0])
        )
    else:
        sorted_items = sorted(data.items(), key=lambda item: item[0])

    rows: List[List[str]] = []
    for key, obj in sorted_items:
        base_symbol = obj.get("base_symbol") or obj.get("symbol") or "-"
        row = [
            key,
            base_symbol,
            format_ts(obj.get("update_tp"), tsfmt, na),
            str(obj.get("sample_size") or 0),
            format_number(obj.get("bidask_sr"), na),
            format_number(obj.get("askbid_sr"), na),
            format_number(obj.get("bidask_lower"), na),
            format_number(obj.get("bidask_upper"), na),
            format_number(obj.get("askbid_lower"), na),
            format_number(obj.get("askbid_upper"), na),
        ]
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

    data = read_hash(rds, args.key)
    if not data:
        print("Redis HASH 为空或 key 不存在。")
        return 0

    filtered = filter_symbols(data, args.symbols)
    if not filtered:
        print("未匹配到指定的 symbol。")
        return 0

    headers, rows = build_matrix(filtered, args.sort, args.na, args.tsfmt)
    print_three_line_table(headers, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
