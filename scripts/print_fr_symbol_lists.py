#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 Funding Rate 交易对列表（按交易所维度，从 Redis 读取）。

读取 4 个 Redis key（均为 String，JSON 数组）：
  1. fr_dump_symbols:{exchange}          - 平仓列表
  2. fr_trade_symbols:{exchange}         - 建仓列表
  3. fr_fwd_trade_symbols:{exchange}     - 正套建仓列表
  4. fr_bwd_trade_symbols:{exchange}     - 反套建仓列表

示例：
  python scripts/print_fr_symbol_lists.py --exchange binance
  python scripts/print_fr_symbol_lists.py       # 在目录名包含 binance/okex/bybit/... 前缀时自动推断
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import List, Optional

SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]


def try_import_redis():
    try:
        import redis  # type: ignore
        return redis
    except Exception:
        return None


def infer_exchange_from_cwd() -> Optional[str]:
    """从当前目录名推断 exchange（如 binance_fr_trade -> binance）"""
    from pathlib import Path

    name = Path.cwd().name.lower()
    candidates = [name]
    if "_" in name:
        candidates.append(name.split("_", 1)[0])
    for cand in candidates:
        for ex in SUPPORTED_EXCHANGES:
            if cand.startswith(ex):
                return ex
    return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print Funding Rate symbol lists from Redis")
    p.add_argument(
        "--exchange",
        choices=SUPPORTED_EXCHANGES,
        help="交易所名称（可选，若未提供则尝试从目录名推断）",
    )
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


def print_all_symbol_lists(rds, exchange: str) -> None:
    """打印所有交易对列表"""
    print("\n📊 Funding Rate 交易对列表配置:")
    print("=" * 80)

    title_prefix = exchange.upper()
    print_symbol_list(rds, f"fr_dump_symbols:{exchange}", f"🔴 {title_prefix} - 平仓列表")
    print_symbol_list(rds, f"fr_trade_symbols:{exchange}", f"🟢 {title_prefix} - 建仓列表")
    print_symbol_list(rds, f"fr_fwd_trade_symbols:{exchange}", f"🟢 {title_prefix} - 正套建仓列表")
    print_symbol_list(rds, f"fr_bwd_trade_symbols:{exchange}", f"🔴 {title_prefix} - 反套建仓列表")


def print_summary(rds, exchange: str) -> None:
    """打印统计摘要"""
    print("\n📈 统计摘要:")
    print("=" * 80)

    keys = [
        f"fr_dump_symbols:{exchange}",
        f"fr_trade_symbols:{exchange}",
        f"fr_fwd_trade_symbols:{exchange}",
        f"fr_bwd_trade_symbols:{exchange}",
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

    exchange = args.exchange or infer_exchange_from_cwd()
    if not exchange:
        print("❌ 需要 --exchange，或在目录名包含 binance/okex/bybit/bitget/gate 前缀以自动推断", file=sys.stderr)
        return 2

    rds = redis.from_url(args.redis_url) if args.redis_url else redis.Redis(
        host=args.host, port=args.port, db=args.db, password=args.password
    )

    print(f"📍 Redis: {args.host}:{args.port}/{args.db}\n")

    # 打印所有列表
    print_all_symbol_lists(rds, exchange)

    # 打印统计摘要
    print_summary(rds, exchange)

    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
