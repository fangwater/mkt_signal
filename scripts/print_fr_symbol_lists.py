#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 Funding Rate 交易对列表（从 Redis 读取）。

读取 4 个 Redis key：
  1. fr_dump_symbols:binance_um      - U本位合约平仓列表
  2. fr_trade_symbols:binance_um     - U本位合约建仓列表
  3. fr_dump_symbols:binance_margin  - 现货杠杆平仓列表
  4. fr_trade_symbols:binance_margin - 现货杠杆建仓列表

示例：
  python scripts/print_fr_symbol_lists.py
  python scripts/print_fr_symbol_lists.py --redis-url redis://:pwd@127.0.0.1:6379/0
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import List


def try_import_redis():
    try:
        import redis  # type: ignore
        return redis
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print Funding Rate symbol lists from Redis")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    return p.parse_args()


def print_symbol_list(rds, key: str, title: str) -> None:
    """打印单个交易对列表"""
    print(f"\n{title} ({key}):")
    symbols_json = rds.get(key)

    if not symbols_json:
        print("  ⚠️  未找到数据")
        return

    symbols_str = symbols_json.decode('utf-8', 'ignore') if isinstance(symbols_json, bytes) else str(symbols_json)

    try:
        symbols = json.loads(symbols_str)
        if isinstance(symbols, list):
            print(f"  总数: {len(symbols)}")
            # 分列打印，每行5个
            for i in range(0, len(symbols), 5):
                chunk = symbols[i:i+5]
                print("  " + "  ".join(f"{s:15}" for s in chunk))
        else:
            print(f"  格式异常: {symbols_str}")
    except Exception as e:
        print(f"  解析失败: {e}")
        print(f"  原始值: {symbols_str}")


def print_all_symbol_lists(rds) -> None:
    """打印所有交易对列表"""
    print("\n📊 Funding Rate 交易对列表配置:")
    print("=" * 80)

    print_symbol_list(rds, "fr_dump_symbols:binance_um", "🔴 Binance UM - 平仓列表")
    print_symbol_list(rds, "fr_trade_symbols:binance_um", "🟢 Binance UM - 建仓列表")
    print_symbol_list(rds, "fr_dump_symbols:binance_margin", "🔴 Binance Margin - 平仓列表")
    print_symbol_list(rds, "fr_trade_symbols:binance_margin", "🟢 Binance Margin - 建仓列表")


def print_summary(rds) -> None:
    """打印统计摘要"""
    print("\n📈 统计摘要:")
    print("=" * 80)

    keys = [
        "fr_dump_symbols:binance_um",
        "fr_trade_symbols:binance_um",
        "fr_dump_symbols:binance_margin",
        "fr_trade_symbols:binance_margin",
    ]

    total_symbols = 0
    for key in keys:
        symbols_json = rds.get(key)
        if symbols_json:
            symbols_str = symbols_json.decode('utf-8', 'ignore') if isinstance(symbols_json, bytes) else str(symbols_json)
            try:
                symbols = json.loads(symbols_str)
                if isinstance(symbols, list):
                    total_symbols += len(symbols)
                    print(f"  {key:40} {len(symbols):3} 个")
            except Exception:
                pass

    print(f"\n  总计: {total_symbols} 个交易对（跨所有列表）")


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    rds = redis.from_url(args.redis_url) if args.redis_url else redis.Redis(
        host=args.host, port=args.port, db=args.db, password=args.password
    )

    print(f"📍 Redis: {args.host}:{args.port}/{args.db}\n")

    # 打印所有列表
    print_all_symbol_lists(rds)

    # 打印统计摘要
    print_summary(rds)

    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
