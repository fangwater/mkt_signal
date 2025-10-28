#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Import `binance_arb_price_spread_cancel_threshold` into Redis (HASH) using rolling metrics quantiles.

Behavior
  - 读取 Redis HASH `rolling_metrics_thresholds`（可通过 --rolling-key 覆盖）。
  - 使用 `bidask` 因子的指定分位点（默认 5%）作为撤单阈值。
  - 写入 Redis HASH（默认 key: `binance_arb_price_spread_cancel_threshold`）：
        field = symbol，大写
        value = 紧凑 JSON: { "symbol", "update_tp", "bidask_sr_cancel" }
  - 可选 dry-run / 清理多余字段。

用法示例：
    python scripts/binance_arb_price_spread_cancel_threshold.py
    python scripts/binance_arb_price_spread_cancel_threshold.py --dry-run
    python scripts/binance_arb_price_spread_cancel_threshold.py --bidask-quantile 10
    python scripts/binance_arb_price_spread_cancel_threshold.py --rolling-key rolling_metrics_thresholds_backup
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, List, Set

from binance_threshold_utils import (
    SYMBOL_ALLOWLIST,
    collect_rows_from_rolling,
    determine_stale_symbols,
    print_summary,
    quantile_value,
    try_import_redis,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Import binance_arb_price_spread_cancel_threshold from rolling metrics thresholds"
    )
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    p.add_argument(
        "--rolling-key",
        default="rolling_metrics_thresholds",
        help="Redis HASH key providing rolling metrics thresholds",
    )
    p.add_argument(
        "--write-key",
        default="binance_arb_price_spread_cancel_threshold",
        help="Redis HASH key to write cancel thresholds into",
    )
    p.add_argument(
        "--bidask-quantile",
        default="5",
        help="bidask 因子使用的分位数，支持百分比（5 表示 5%%）或小数（0.05），也可传 label",
    )
    p.add_argument("--dry-run", action="store_true", help="Only display summary, do not write to Redis")
    p.add_argument(
        "--no-clean",
        action="store_true",
        help="Do not remove stale fields from the destination HASH",
    )
    return p.parse_args()


def parse_quantile(value: str) -> object:
    value = value.strip()
    try:
        return float(value)
    except ValueError:
        return value.lower()


def build_cancel_rows(rows: Dict[str, Dict], quantile_target: object) -> Dict[str, Dict]:
    cancel_rows: Dict[str, Dict] = {}
    for symbol, payload in rows.items():
        cancel_val = quantile_value(payload["quantiles"], "bidask", quantile_target)
        if cancel_val is None:
            continue
        cancel_rows[symbol] = {
            "symbol": symbol,
            "update_tp": payload.get("update_tp"),
            "bidask_sr_cancel": cancel_val,
        }
    return cancel_rows


def write_cancel_hash(
    rds,
    key: str,
    rows: Dict[str, Dict],
    clean: bool,
    stale_symbols: List[str],
) -> None:
    pipe = rds.pipeline(transaction=False)
    for sym, payload in rows.items():
        pipe.hset(key, sym, json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    if clean and stale_symbols:
        pipe.hdel(key, *stale_symbols)
    pipe.execute()


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("redis 包未安装，请使用 venv 安装或 --user 安装 redis。", file=sys.stderr)
        return 2

    rds = (
        redis.from_url(args.redis_url)
        if args.redis_url
        else redis.Redis(host=args.host, port=args.port, db=args.db, password=args.password)
    )

    full_rows, success, missing, invalid = collect_rows_from_rolling(
        rds, args.rolling_key, SYMBOL_ALLOWLIST
    )
    print_summary(success, missing, invalid)

    quantile_target = parse_quantile(args.bidask_quantile)
    cancel_rows = build_cancel_rows(full_rows, quantile_target)
    missing_symbols = sorted(set(full_rows.keys()) - set(cancel_rows.keys()))
    if missing_symbols:
        print("以下 symbol 缺少对应分位值，已跳过: " + ", ".join(missing_symbols))

    new_symbols: Set[str] = set(cancel_rows.keys())
    stale_symbols = (
        determine_stale_symbols(rds, args.write_key, new_symbols) if not args.no_clean else []
    )
    if stale_symbols:
        print("将清理 Redis 中以下符号: " + ", ".join(stale_symbols))

    if args.dry_run:
        if cancel_rows:
            print(f"将写入 {len(cancel_rows)} 条到 HASH {args.write_key}（dry-run）")
        if stale_symbols:
            print(f"dry-run: 将清理 {len(stale_symbols)} 个字段")
        if not cancel_rows:
            print("没有可写入的 symbol（均未满足允许名单或被阈值过滤）。")
        return 0

    if not cancel_rows:
        if stale_symbols:
            write_cancel_hash(rds, args.write_key, cancel_rows, clean=True, stale_symbols=stale_symbols)
            print(f"已清理 {len(stale_symbols)} 个字段，当前无有效阈值可写入。")
        else:
            print("没有可写入的 symbol（均未满足允许名单或被阈值过滤）。")
        return 0

    write_cancel_hash(
        rds,
        args.write_key,
        cancel_rows,
        clean=(not args.no_clean),
        stale_symbols=stale_symbols,
    )
    print(f"已写入 {len(cancel_rows)} 条到 HASH {args.write_key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
