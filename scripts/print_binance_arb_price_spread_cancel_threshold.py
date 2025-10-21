#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Print binance_arb_price_spread_cancel_threshold from Redis (HASH) as a three-line table.

目的：
  - 读取 Redis HASH `binance_arb_price_spread_cancel_threshold`：
        field = symbol
        value = JSON { "symbol", "update_tp", "bidask_sr_cancel" }
  - 以三线表（top rule / header rule / bottom rule）输出，便于终端查看。

示例：
    python scripts/print_binance_arb_price_spread_cancel_threshold.py
    python scripts/print_binance_arb_price_spread_cancel_threshold.py --symbols BTCUSDT ETHUSDT
    python scripts/print_binance_arb_price_spread_cancel_threshold.py --tsfmt raw
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
        description="Print Redis hash binance_arb_price_spread_cancel_threshold as a three-line table"
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
        help="Filter to specific symbols (e.g. ADAUSDT ETHUSDT)",
    )
    p.add_argument(
        "--tsfmt",
        choices=["raw", "iso"],
        default="iso",
        help="Format for update_tp column: raw epoch ms or ISO-8601 UTC",
    )
    p.add_argument(
        "--na",
        default="-",
        help="Placeholder for missing/NaN values (default: '-')",
    )
    p.add_argument(
        "--key",
        default="binance_arb_price_spread_cancel_threshold",
        help="Redis HASH key (default: binance_arb_price_spread_cancel_threshold)",
    )
    return p.parse_args()


def connect_redis(args) -> Optional["redis.Redis"]:
    redis = try_import_redis()
    if redis is None:
        return None
    if args.redis_url:
        return redis.from_url(args.redis_url)
    return redis.Redis(host=args.host, port=args.port, db=args.db, password=args.password)


def read_hash(rds, key: str, symbols: Optional[List[str]]) -> Dict[str, Dict]:
    result: Dict[str, Dict] = {}
    data = rds.hgetall(key)
    for k, v in data.items():
        sym = k.decode("utf-8", "ignore") if isinstance(k, bytes) else str(k)
        if symbols and sym not in symbols:
            continue
        raw = v.decode("utf-8", "ignore") if isinstance(v, bytes) else str(v)
        try:
            obj = json.loads(raw)
        except Exception:
            obj = {"symbol": sym, "update_tp": None, "bidask_sr_cancel": None}
        result[sym] = obj
    return result


def is_nan(x: object) -> bool:
    return isinstance(x, float) and math.isnan(x)


def format_number(x: Optional[float], na: str) -> str:
    if x is None or is_nan(x):
        return na
    try:
        s = f"{float(x):.8f}"
        s = s.rstrip("0").rstrip(".")
        if s == "-0":
            s = "0"
        return s
    except Exception:
        return na


def format_ts(ts_val: Optional[int], mode: str, na: str) -> str:
    if ts_val is None:
        return na
    try:
        ts_ms = int(ts_val)
    except Exception:
        return na
    if mode == "raw":
        return str(ts_ms)
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts_ms)


def build_matrix(sym_to_obj: Dict[str, Dict], na: str, tsfmt: str) -> Tuple[List[str], List[List[str]]]:
    headers = ["symbol", "update_tp", "bidask_sr_cancel"]
    rows: List[List[str]] = []
    for sym in sorted(sym_to_obj.keys()):
        obj = sym_to_obj.get(sym, {})
        row = [
            sym,
            format_ts(obj.get("update_tp"), tsfmt, na),
            format_number(obj.get("bidask_sr_cancel"), na),
        ]
        rows.append(row)
    return headers, rows


def compute_col_widths(headers: List[str], rows: List[List[str]]) -> List[int]:
    widths = [len(h) for h in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))
    return widths


def format_row(values: List[str], widths: List[int]) -> str:
    return "  ".join(value.ljust(width) for value, width in zip(values, widths))


def print_table(headers: List[str], rows: List[List[str]]) -> None:
    widths = compute_col_widths(headers, rows)
    header_line = format_row(headers, widths)
    top_rule = "=" * len(header_line)
    mid_rule = "-" * len(header_line)
    bot_rule = "=" * len(header_line)
    print(top_rule)
    print(header_line)
    print(mid_rule)
    for row in rows:
        print(format_row(row, widths))
    print(bot_rule)


def main() -> int:
    args = parse_args()
    rds = connect_redis(args)
    if rds is None:
        print("redis 包未安装，请使用 venv 安装或 --user 安装 redis。", file=sys.stderr)
        return 2
    payload = read_hash(rds, args.key, args.symbols)
    if not payload:
        print("HASH 为空或未找到匹配的 symbol。")
        return 0
    headers, rows = build_matrix(payload, args.na, args.tsfmt)
    print_table(headers, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
