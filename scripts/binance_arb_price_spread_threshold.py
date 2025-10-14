#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Import binance_arb_price_spread_threshold into Redis (HASH) using rolling_metrics thresholds.

Behavior
  - Read Redis HASH `rolling_metrics_thresholds` (configurable via --rolling-key).
  - Filter rows by the hard-coded symbol allowlist `SYMBOL_ALLOWLIST`.
  - Parse JSON payloads to extract:
      update_tp (timestamp in milliseconds), bidask_lower, bidask_upper, askbid_lower, askbid_upper.
  - Filter rule: if any bound is NaN/None/non-finite OR equals 0 (0/0.0/-0.0), skip that symbol.
  - Write results as a Redis HASH at key `binance_arb_price_spread_threshold` (configurable via --write-key),
    field = symbol, value = compact JSON with fields:
      symbol, update_tp,
      bidask_sr_open_threshold, bidask_sr_close_threshold,
      askbid_sr_open_threshold, askbid_sr_close_threshold.
  - Optionally clean stale fields from the HASH.

CLI Examples
  - Typical usage:
      python scripts/binance_arb_price_spread_threshold.py
  - Custom rolling HASH with dry-run:
      python scripts/binance_arb_price_spread_threshold.py --rolling-key rolling_metrics_thresholds_backup --dry-run
  - Dry run (compute but do not write):
      python scripts/binance_arb_price_spread_threshold.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, List, Tuple, Optional, Set
import math

SYMBOL_ALLOWLIST: List[str] = [
    # 8h symbols
    "HIGHUSDT",
    "EGLDUSDT",
    "SFPUSDT",
    "IOTXUSDT",
    "ZENUSDT",
    "COTIUSDT",
    "ZILUSDT",
    "SUSHIUSDT",
    "MINAUSDT",
    "ENJUSDT",
    "KSMUSDT",
    "VETUSDT",
    "SXPUSDT",
    "BICOUSDT",
    "C98USDT",
    "CHRUSDT",
    "UNIUSDT",
    "NEOUSDT",
    "CELOUSDT",
    "KAVAUSDT",
    "ASTRUSDT",
    # 4h symbols
    "HEIUSDT",
    "NFPUSDT",
    "TNSRUSDT",
    "SANTOSUSDT",
    "FLUXUSDT",
    "KDAUSDT",
    "BEAMXUSDT",
    "AUCTIONUSDT",
    "AIUSDT",
    "INITUSDT",
    "A2ZUSDT",
    "USTCUSDT",
    "SAGAUSDT",
    "SLPUSDT",
    "VANRYUSDT",
    "WCTUSDT",
    "AXLUSDT",
    "JTOUSDT",
    "TWTUSDT",
    "PUMPUSDT",
    "MANTAUSDT",
    "MEMEUSDT",
    "ILVUSDT",
    "ORCAUSDT",
    "SUNUSDT",
]


def try_import_redis():
    try:
        import redis  # type: ignore
        return redis
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Import binance_arb_price_spread_threshold from rolling_metrics thresholds")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    p.add_argument("--write-key", default="binance_arb_price_spread_threshold")
    p.add_argument("--rolling-key", default="rolling_metrics_thresholds", help="Redis HASH key providing rolling metrics thresholds")
    p.add_argument("--dry-run", action="store_true", help="Do not write to Redis")
    p.add_argument("--no-clean", action="store_true", help="Do not remove stale fields from HASH")
    return p.parse_args()


def normalize_bound(name: str, value: object) -> Tuple[Optional[float], Optional[str]]:
    if value is None:
        return None, f"{name} 缺失"
    try:
        fx = float(value)
    except Exception:
        return None, f"{name} 非数值({value})"
    if math.isnan(fx) or not math.isfinite(fx):
        return None, f"{name} 非有限数({fx})"
    if fx == 0.0:
        return None, f"{name} 为0"
    return fx, None


def collect_rows_from_rolling(
    rds,
    rolling_key: str,
    symbols: List[str],
) -> Tuple[Dict[str, Dict], List[str], List[str], Dict[str, str]]:
    out: Dict[str, Dict] = {}
    symbol_set = {s.upper() for s in symbols}
    success: List[str] = []
    missing: List[str] = []
    invalid: Dict[str, str] = {}

    try:
        data = rds.hgetall(rolling_key)
    except Exception as exc:
        print(f"Failed to read rolling HASH {rolling_key}: {exc}", file=sys.stderr)
        return out, success, symbols.copy(), invalid

    entry_map: Dict[str, Dict] = {}
    for field, raw in data.items():
        key = field.decode("utf-8", "ignore") if isinstance(field, bytes) else str(field)
        payload = raw.decode("utf-8", "ignore") if isinstance(raw, bytes) else str(raw)
        try:
            obj = json.loads(payload)
        except json.JSONDecodeError:
            safe = payload.replace("NaN", "null")
            obj = json.loads(safe)
        if not isinstance(obj, dict):
            continue

        base_symbol = (
            obj.get("base_symbol")
            or obj.get("symbol")
            or key.split("::")[-1]
        )
        if not base_symbol:
            continue
        normalized_symbol = base_symbol.upper()
        if normalized_symbol in symbol_set:
            entry_map[normalized_symbol] = obj

    for symbol in symbols:
        sym_upper = symbol.upper()
        obj = entry_map.get(sym_upper)
        if obj is None:
            missing.append(symbol)
            continue

        b_lower, err = normalize_bound("bidask_lower", obj.get("bidask_lower"))
        if err:
            invalid[symbol] = err
            continue
        b_upper, err = normalize_bound("bidask_upper", obj.get("bidask_upper"))
        if err:
            invalid[symbol] = err
            continue
        a_lower, err = normalize_bound("askbid_lower", obj.get("askbid_lower"))
        if err:
            invalid[symbol] = err
            continue
        a_upper, err = normalize_bound("askbid_upper", obj.get("askbid_upper"))
        if err:
            invalid[symbol] = err
            continue

        ts_val = obj.get("update_tp")
        if ts_val is None:
            ts_val = obj.get("ts")
        try:
            ts = int(ts_val) if ts_val is not None else None
        except Exception:
            ts = None

        out[sym_upper] = {
            "symbol": sym_upper,
            "update_tp": ts,
            "bidask_sr_open_threshold": b_lower,
            "bidask_sr_close_threshold": b_upper,
            "askbid_sr_open_threshold": a_lower,
            "askbid_sr_close_threshold": a_upper,
        }
        success.append(symbol)

    return out, success, missing, invalid


def print_summary(success: List[str], missing: List[str], invalid: Dict[str, str]) -> None:
    if success:
        print("成功导入符号: " + ", ".join(success))
    if missing:
        print("缺少rolling记录: " + ", ".join(missing))
    if invalid:
        print("阈值不合法的符号:")
        for sym in invalid:
            print(f"  - {sym}: {invalid[sym]}")


def determine_stale_symbols(rds, key: str, new_symbols: Set[str]) -> List[str]:
    try:
        existing = rds.hkeys(key)
    except Exception:
        return []
    existing_syms = set(
        k.decode("utf-8", "ignore") if isinstance(k, bytes) else str(k) for k in existing
    )
    return sorted(existing_syms - new_symbols)


def write_hash(
    rds,
    key: str,
    rows: Dict[str, Dict],
    clean: bool,
    stale_symbols: List[str],
) -> None:
    pipe = rds.pipeline(transaction=False)
    for sym, payload in rows.items():
        pipe.hset(key, sym, json.dumps(payload, ensure_ascii=False, separators=(',', ':')))
    if clean:
        if stale_symbols:
            pipe.hdel(key, *stale_symbols)
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

    rows, success, missing, invalid = collect_rows_from_rolling(
        rds, args.rolling_key, SYMBOL_ALLOWLIST
    )
    print_summary(success, missing, invalid)
    new_symbol_set = set(rows.keys())
    stale_symbols = (
        determine_stale_symbols(rds, args.write_key, new_symbol_set)
        if not args.no_clean
        else []
    )
    if stale_symbols:
        print("将清理 Redis 中以下符号: " + ", ".join(stale_symbols))
    if args.dry_run:
        if rows:
            print(f"将写入 {len(rows)} 条到 HASH {args.write_key}（dry-run）")
        if stale_symbols:
            print(f"dry-run: 将清理 {len(stale_symbols)} 个字段")
        if not rows:
            print("没有可写入的 symbol（均未满足允许名单或被阈值过滤）。")
        return 0
    if not rows:
        if stale_symbols:
            write_hash(
                rds,
                args.write_key,
                rows,
                clean=True,
                stale_symbols=stale_symbols,
            )
            print(f"已清理 {len(stale_symbols)} 个字段，当前无有效阈值可写入。")
        else:
            print("没有可写入的 symbol（均未满足允许名单或被阈值过滤）。")
        return 0
    write_hash(
        rds,
        args.write_key,
        rows,
        clean=(not args.no_clean),
        stale_symbols=stale_symbols,
    )
    print(f"已写入 {len(rows)} 条到 HASH {args.write_key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
