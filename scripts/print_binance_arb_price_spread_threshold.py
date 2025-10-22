#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Print the first table (binance_arb_price_spread_threshold) from Redis HASH.

Purpose 
  - Read Redis HASH `binance_arb_price_spread_threshold`:
      field = symbol,
      value = JSON { symbol, update_tp, bidask_sr_open_threshold, bidask_sr_close_threshold, askbid_sr_open_threshold, askbid_sr_close_threshold }

  - Print the table in a "three-line table" (三线表) style:
      top rule, header rule, bottom rule; no vertical grid lines.

CLI Examples 
  - From Redis (default localhost):
      python scripts/print_binance_arb_price_spread_threshold.py 

  - Specify Redis URL: 
      python scripts/print_binance_arb_price_spread_threshold.py --redis-url redis://:pwd@127.0.0.1:6379/0 

  - Limit to specific symbols: 
      python scripts/print_binance_arb_price_spread_threshold.py --symbols ADAUSDT ETHUSDT XRPUSDT 

Notes 
  - Requires `redis` Python package. 
  - `update_tp` prints as UTC time by default. 
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
    p = argparse.ArgumentParser(description="Print Redis hash binance_arb_price_spread_threshold as a three-line table")
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
        help="If set, only include these symbols (e.g., ADAUSDT ETHUSDT)",
    )
    p.add_argument(
        "--tsfmt",
        choices=["raw", "iso"],
        default="iso",
        help="Format for update_tp: raw epoch ms or ISO-8601 UTC (default: iso)",
    )
    p.add_argument(
        "--na",
        default="-",
        help="Placeholder for missing/NaN values (default: '-')",
    )
    p.add_argument("--key", default="binance_arb_price_spread_threshold", help="Redis HASH key to read (default: binance_arb_price_spread_threshold)")
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
        sym = k.decode('utf-8', 'ignore') if isinstance(k, bytes) else str(k)
        if symbols and sym not in symbols:
            continue
        raw = v.decode('utf-8', 'ignore') if isinstance(v, bytes) else str(v)
        try:
            obj = json.loads(raw)
        except Exception:
            # tolerate non-JSON values
            obj = {"symbol": sym, "update_tp": None}
        # Normalize ts/update_tp naming for printing
        if 'ts' not in obj and 'update_tp' in obj:
            obj['ts'] = obj.get('update_tp')
        result[sym] = obj
    return result


def is_nan(x: object) -> bool:
    return isinstance(x, float) and math.isnan(x)


def build_symbol_data(sym_to_obj: Dict[str, Dict]) -> Dict[str, Dict[str, Optional[float]]]:
    # For printing, we trust the imported hash; just normalize field names
    out: Dict[str, Dict[str, Optional[float]]] = {}
    for sym in sorted(sym_to_obj.keys()):
        obj = sym_to_obj.get(sym, {})
        out[sym] = {
            "ts": obj.get("ts"),
            "bidask_sr_open_threshold": obj.get("bidask_sr_open_threshold"),
            "bidask_sr_close_threshold": obj.get("bidask_sr_close_threshold"),
            "askbid_sr_open_threshold": obj.get("askbid_sr_open_threshold"),
            "askbid_sr_close_threshold": obj.get("askbid_sr_close_threshold"),
        }
    return out


def format_number(x: Optional[float], na: str) -> str:
    if x is None or is_nan(x):
        return na
    try:
        s = f"{float(x):.8f}"
        # trim trailing zeros
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
    # iso
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts_ms)


def build_matrix(sym_to_obj: Dict[str, Dict], na: str, tsfmt: str) -> Tuple[List[str], List[List[str]]]:
    # Columns fixed order; rows are symbols
    headers = [
        "symbol",
        "update_tp",
        "bidask_sr_open_threshold",
        "bidask_sr_close_threshold",
        "askbid_sr_open_threshold",
        "askbid_sr_close_threshold",
    ]

    rows: List[List[str]] = []
    for sym in sorted(sym_to_obj.keys()):
        obj = sym_to_obj.get(sym, {})
        row = [
            sym,
            format_ts(obj.get("ts"), tsfmt, na),
            format_number(obj.get("bidask_sr_open_threshold"), na),
            format_number(obj.get("bidask_sr_close_threshold"), na),
            format_number(obj.get("askbid_sr_open_threshold"), na),
            format_number(obj.get("askbid_sr_close_threshold"), na),
        ]
        rows.append(row)
    return headers, rows


def write_to_redis_hash(rds, key: str, sym_rows: Dict[str, Dict[str, Optional[float]]], clean: bool = True) -> None:
    # Prepare JSON per symbol and write via pipeline
    pipe = rds.pipeline(transaction=False)
    for sym, row in sym_rows.items():
        # Store update_tp as raw ts milliseconds for simplicity
        payload = {
            "symbol": sym,
            "update_tp": row.get("ts"),
            "bidask_sr_open_threshold": row.get("bidask_sr_open_threshold"),
            "bidask_sr_close_threshold": row.get("bidask_sr_close_threshold"),
            "askbid_sr_open_threshold": row.get("askbid_sr_open_threshold"),
            "askbid_sr_close_threshold": row.get("askbid_sr_close_threshold"),
        }
        # Ensure NaN never appears in JSON
        payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        pipe.hset(key, sym, payload_json)

    if clean:
        try:
            existing = rds.hkeys(key)
            existing_syms = set(k.decode("utf-8", "ignore") if isinstance(k, bytes) else k for k in existing)
            desired_syms = set(sym_rows.keys())
            stale = list(existing_syms - desired_syms)
            if stale:
                pipe.hdel(key, *stale)
        except Exception:
            # If hkeys fails for any reason, skip cleaning
            pass

    pipe.execute()


def compute_col_widths(headers: List[str], rows: List[List[str]]) -> List[int]:
    ncols = len(headers)
    widths = [0] * ncols
    for i, h in enumerate(headers):
        widths[i] = max(widths[i], len(h))
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(cell))
    return widths


def print_three_line_table(headers: List[str], rows: List[List[str]]) -> None:
    widths = compute_col_widths(headers, rows)

    def fmt_row(values: List[str]) -> str:
        parts: List[str] = []
        for i, v in enumerate(values):
            if i == 0:
                # left align field name
                parts.append(v.ljust(widths[i]))
            else:
                # right align numbers/values
                parts.append(v.rjust(widths[i]))
        return "  ".join(parts)

    header_line = fmt_row(headers)
    top_rule = "=" * len(header_line)
    mid_rule = "-" * len(header_line)
    bot_rule = "=" * len(header_line)

    print(top_rule)
    print(header_line)
    print(mid_rule)
    for r in rows:
        print(fmt_row(r))
    print(bot_rule)


def main() -> int:
    args = parse_args()

    rds = connect_redis(args)
    if rds is None:
        print("redis 包未安装，请使用 venv 安装或 --user 安装 redis。", file=sys.stderr)
        return 2

    sym_to_obj: Dict[str, Dict] = read_hash(rds, args.key, args.symbols)

    if not sym_to_obj:
        print("No matching symbols found.")
        return 0 

    sym_rows = build_symbol_data(sym_to_obj) 
    if not sym_rows: 
        print("No symbols found in hash key.") 
        return 0 

    headers, rows = build_matrix(sym_rows, na=args.na, tsfmt=args.tsfmt)
    print_three_line_table(headers, rows) 
    return 0 


if __name__ == "__main__": 
    raise SystemExit(main()) 
