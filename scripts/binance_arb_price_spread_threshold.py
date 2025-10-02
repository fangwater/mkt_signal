#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Import binance_arb_price_spread_threshold into Redis (HASH) from aggregated keys.

Behavior
  - Scan Redis keys like `binance:*`, pick only aggregated entries `binance:SYMBOL` (two segments),
    excluding suffixes like `:spot_bookticker` / `:swap_bookticker`.
  - Parse their JSON payloads (tolerate `NaN` by converting to null when possible), and for each symbol keep:
      ts (milliseconds), bidask_lower, bidask_upper, askbid_lower, askbid_upper
  - Filter rule: if any bound is NaN/None/non-finite OR equals 0 (0/0.0/-0.0), skip that symbol.
  - Write results as a Redis HASH at key `binance_arb_price_spread_threshold` (configurable via --write-key),
    field = symbol, value = compact JSON with fields: symbol, update_tp, bidask_lower/upper, askbid_lower/upper.
  - Optionally clean stale fields from the HASH.

CLI Examples
  - Default:
      python scripts/binance_arb_price_spread_threshold.py
  - Custom Redis URL and no-clean:
      python scripts/binance_arb_price_spread_threshold.py --redis-url redis://:pwd@127.0.0.1:6379/0 --no-clean
  - Only specific symbols:
      python scripts/binance_arb_price_spread_threshold.py --symbols ADAUSDT ETHUSDT
  - Dry run (compute but do not write):
      python scripts/binance_arb_price_spread_threshold.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, Iterable, List, Optional, Tuple
import math


def try_import_redis():
    try:
        import redis  # type: ignore
        return redis
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Import binance_arb_price_spread_threshold from binance:* aggregated keys")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    p.add_argument("--pattern", default="binance:*", help="Key pattern to scan (default: binance:*)")
    p.add_argument(
        "--exclude-suffixes",
        nargs="*",
        default=[":spot_bookticker", ":swap_bookticker"],
        help="Key suffixes to exclude",
    )
    p.add_argument("--write-key", default="binance_arb_price_spread_threshold")
    p.add_argument("--symbols", nargs="*", help="Only include these symbols")
    p.add_argument("--dry-run", action="store_true", help="Do not write to Redis")
    p.add_argument("--no-clean", action="store_true", help="Do not remove stale fields from HASH")
    return p.parse_args()


def is_invalid_bound(x: object) -> bool:
    """Return True if value is invalid for import.

    Invalid when:
      - None / missing
      - cannot be parsed as float
      - is NaN / Infinity / -Infinity (non-finite)
      - equals zero (0.0)
    """
    if x is None:
        return True
    try:
        fx = float(x)
    except Exception:
        return True
    # NaN or non-finite
    if math.isnan(fx) or not math.isfinite(fx):
        return True
    # zero-like (includes -0.0)
    return fx == 0.0


def iter_from_redis(rds, pattern: str, exclude_suffixes: List[str], symbols: Optional[List[str]]) -> Iterable[Tuple[str, Dict]]:
    for key in rds.scan_iter(match=pattern):
        if isinstance(key, bytes):
            key = key.decode("utf-8", "ignore")
        if any(key.endswith(suf) for suf in exclude_suffixes):
            continue
        parts = key.split(":")
        if len(parts) != 2:
            continue
        _prefix, sym = parts
        if symbols and sym not in symbols:
            continue
        raw = rds.get(key)
        if raw is None:
            continue
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "ignore")
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            safe = raw.replace("NaN", "null")
            obj = json.loads(safe)
        yield sym, obj


def build_rows(sym_to_obj: Dict[str, Dict]) -> Dict[str, Dict]:
    out: Dict[str, Dict] = {}
    for sym, obj in sym_to_obj.items():
        b_lower = obj.get("bidask_lower")
        b_upper = obj.get("bidask_upper")
        a_lower = obj.get("askbid_lower")
        a_upper = obj.get("askbid_upper")
        # Skip if any bound is invalid (NaN/None/non-finite) or equals zero
        if (
            is_invalid_bound(b_lower)
            or is_invalid_bound(b_upper)
            or is_invalid_bound(a_lower)
            or is_invalid_bound(a_upper)
        ):
            continue
        ts = None
        try:
            ts = int(obj.get("ts")) if obj.get("ts") is not None else None
        except Exception:
            ts = None
        out[sym] = {
            "symbol": sym,
            "update_tp": ts,
            "bidask_lower": b_lower,
            "bidask_upper": b_upper,
            "askbid_lower": a_lower,
            "askbid_upper": a_upper,
        }
    return out


def write_hash(rds, key: str, rows: Dict[str, Dict], clean: bool) -> None:
    pipe = rds.pipeline(transaction=False)
    for sym, payload in rows.items():
        pipe.hset(key, sym, json.dumps(payload, ensure_ascii=False, separators=(',', ':')))
    if clean:
        try:
            existing = rds.hkeys(key)
            existing_syms = set(k.decode('utf-8', 'ignore') if isinstance(k, bytes) else k for k in existing)
            stale = list(existing_syms - set(rows.keys()))
            if stale:
                pipe.hdel(key, *stale)
        except Exception:
            pass
    pipe.execute()


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("redis 包未安装，请使用 venv 安装或 --user 安装 redis。", file=sys.stderr)
        return 2

    rds = redis.from_url(args.redis_url) if args.redis_url else redis.Redis(
        host=args.host, port=args.port, db=args.db, password=args.password
    )

    sym_to_obj: Dict[str, Dict] = {}
    for sym, obj in iter_from_redis(rds, args.pattern, args.exclude_suffixes, args.symbols):
        sym_to_obj[sym] = obj

    rows = build_rows(sym_to_obj)
    if not rows:
        print("没有可写入的 symbol（全部被零边界过滤）。")
        return 0
    if args.dry_run:
        print(f"将写入 {len(rows)} 条到 HASH {args.write_key}（dry-run）")
        return 0
    write_hash(rds, args.write_key, rows, clean=(not args.no_clean))
    print(f"已写入 {len(rows)} 条到 HASH {args.write_key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
