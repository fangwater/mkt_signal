#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Print binance_forward_arb_params from Redis (HASH) as a three-line table.

Reads
  - Redis HASH `binance_forward_arb_params` (or --key) where
    field = param_name, value = string (or JSON string for complex types).

Prints
  - Two columns: param, value
  - Three-line table style: top rule, header rule, bottom rule; no vertical lines.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, List, Tuple


def try_import_redis():
    try:
        import redis  # type: ignore
        return redis
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print Redis hash binance_forward_arb_params as a three-line table")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    p.add_argument("--key", default="binance_forward_arb_params")
    p.add_argument("--prefix", help="Only print params with this prefix, e.g., funding_rate_strategy.")
    return p.parse_args()


def read_hash(rds, key: str) -> Dict[str, str]:
    data = rds.hgetall(key)
    out: Dict[str, str] = {}
    for k, v in data.items():
        kk = k.decode('utf-8', 'ignore') if isinstance(k, bytes) else str(k)
        vv = v.decode('utf-8', 'ignore') if isinstance(v, bytes) else str(v)
        out[kk] = vv
    return out


def build_rows(kv: Dict[str, str], prefix: str | None) -> Tuple[List[str], List[List[str]]]:
    headers = ["param", "value"]
    rows: List[List[str]] = []
    for k in sorted(kv.keys()):
        if prefix and not k.startswith(prefix):
            continue
        v = kv[k]
        # Pretty display JSON-ish values without extra quotes
        try:
            parsed = json.loads(v)
            # For JSON scalars, cast back to string without extra quotes
            if isinstance(parsed, (int, float)):
                v = str(parsed)
            elif isinstance(parsed, bool):
                v = "true" if parsed else "false"
            elif parsed is None:
                v = "null"
            elif isinstance(parsed, (list, dict)):
                v = json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))
            elif isinstance(parsed, str):
                v = parsed
        except Exception:
            pass
        rows.append([k, v])
    return headers, rows


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
            align = str.ljust if i == 0 else str.ljust  # left align both for readability
            parts.append(align(v, widths[i]))
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
    redis = try_import_redis()
    if redis is None:
        print("redis 包未安装，请使用 venv 安装或 --user 安装 redis。", file=sys.stderr)
        return 2
    rds = redis.from_url(args.redis_url) if args.redis_url else redis.Redis(
        host=args.host, port=args.port, db=args.db, password=args.password
    )
    kv = read_hash(rds, args.key)
    if not kv:
        print("未找到参数或 HASH 为空。")
        return 0
    headers, rows = build_rows(kv, args.prefix)
    print_three_line_table(headers, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

