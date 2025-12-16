#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将 Funding Rate 的 symbol lists 从某个来源 suffix 复制到 “venue pair” suffix，方便跨所/跨 venue 部署。

写入 Redis key（String 类型，JSON 数组）：
  - fr_dump_symbols:{pair_suffix}
  - fr_fwd_trade_symbols:{pair_suffix}
  - fr_bwd_trade_symbols:{pair_suffix}

pair_suffix 规则：
  pair_suffix = "{open_venue}_{hedge_venue}"，其中 venue 将 '-' 替换为 '_'。
  例：open=binance-margin, hedge=okex-futures -> binance_margin_okex_futures

示例：
  python scripts/xarb/sync_symbol_lists_pair.py --open-venue binance-margin --hedge-venue okex-futures --source okex
  python scripts/xarb/sync_symbol_lists_pair.py --open-venue okex-margin --hedge-venue okex-futures --source okex
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Optional


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def venue_to_suffix(venue: str) -> str:
    return venue.strip().lower().replace("-", "_")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Copy FR symbol lists to a venue-pair suffix")
    p.add_argument("--open-venue", required=True, help="open venue（如 binance-margin）")
    p.add_argument("--hedge-venue", required=True, help="hedge venue（如 okex-futures）")
    p.add_argument(
        "--source",
        help="源 suffix（优先使用）。例如 okex 或 okex_futures；默认使用 hedge exchange 前缀（如 okex）",
    )
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    p.add_argument("--dry-run", action="store_true", help="只打印，不写入")
    return p.parse_args()


def read_json_list(rds, key: str) -> Optional[list]:
    data = rds.get(key)
    if not data:
        return None
    text = data.decode("utf-8", "ignore") if isinstance(data, bytes) else str(data)
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, list) else None
    except Exception:
        return None


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    rds = redis.from_url(args.redis_url) if args.redis_url else redis.Redis(
        host=args.host, port=args.port, db=args.db, password=args.password
    )

    open_suffix = venue_to_suffix(args.open_venue)
    hedge_suffix = venue_to_suffix(args.hedge_venue)
    pair_suffix = f"{open_suffix}_{hedge_suffix}"

    hedge_exchange = args.hedge_venue.split("-", 1)[0].strip().lower()
    source_suffix = (args.source or hedge_exchange).strip().lower()

    prefixes = [
        "fr_dump_symbols",
        "fr_fwd_trade_symbols",
        "fr_bwd_trade_symbols",
    ]

    print(f"📍 Redis: {args.host}:{args.port}/{args.db}")
    print(f"🎯 pair_suffix: {pair_suffix}")
    print(f"📖 source_suffix: {source_suffix}")
    print("")

    for prefix in prefixes:
        src_key = f"{prefix}:{source_suffix}"
        dst_key = f"{prefix}:{pair_suffix}"

        symbols = read_json_list(rds, src_key)
        if symbols is None:
            print(f"⚠️  missing/invalid source key: {src_key}")
            continue

        print(f"✅ {src_key} -> {dst_key} ({len(symbols)} symbols)")
        if not args.dry_run:
            rds.set(dst_key, json.dumps(symbols))

    if args.dry_run:
        print("\n[DRY RUN] 未写入 Redis")
    else:
        print("\n✅ 写入完成")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

