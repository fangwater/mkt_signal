#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将 Funding Rate 交易对列表同步到 Redis 并打印。

写入 4 个 Redis key（String 类型，JSON 数组）：
  1. fr_dump_symbols:binance_um      - U本位合约平仓列表
  2. fr_trade_symbols:binance_um     - U本位合约建仓列表
  3. fr_dump_symbols:binance_margin  - 现货杠杆平仓列表
  4. fr_trade_symbols:binance_margin - 现货杠杆建仓列表

同步完成后自动打印所有列表。

示例：
  python scripts/sync_fr_symbol_lists.py
  python scripts/sync_fr_symbol_lists.py --redis-url redis://:pwd@127.0.0.1:6379/0
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
    p = argparse.ArgumentParser(description="Sync Funding Rate symbol lists to Redis")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    return p.parse_args()


# ========== 交易对白名单配置 ==========

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
    "CUSDT",
    "XPLUSDT",
]

# ========== Symbol Lists 配置 ==========

# Binance UM (合约) - 平仓列表
# 包含所有白名单交易对
DUMP_SYMBOLS_UM = SYMBOL_ALLOWLIST.copy()

# Binance UM (合约) - 建仓列表
# 包含所有白名单交易对
TRADE_SYMBOLS_UM = SYMBOL_ALLOWLIST.copy()

# Binance Margin (现货杠杆) - 平仓列表
# 包含所有白名单交易对
DUMP_SYMBOLS_MARGIN = SYMBOL_ALLOWLIST.copy()

# Binance Margin (现货杠杆) - 建仓列表
# 包含所有白名单交易对
TRADE_SYMBOLS_MARGIN = SYMBOL_ALLOWLIST.copy()


def sync_symbol_lists(rds) -> int:
    """同步交易对列表到 Redis"""
    total = 0

    # 1. Binance UM - 平仓列表
    key = "fr_dump_symbols:binance_um"
    symbols_json = json.dumps(DUMP_SYMBOLS_UM, ensure_ascii=False)
    rds.set(key, symbols_json)
    print(f"✅ 已写入 {len(DUMP_SYMBOLS_UM)} 个交易对到 '{key}'")
    total += len(DUMP_SYMBOLS_UM)

    # 2. Binance UM - 建仓列表
    key = "fr_trade_symbols:binance_um"
    symbols_json = json.dumps(TRADE_SYMBOLS_UM, ensure_ascii=False)
    rds.set(key, symbols_json)
    print(f"✅ 已写入 {len(TRADE_SYMBOLS_UM)} 个交易对到 '{key}'")
    total += len(TRADE_SYMBOLS_UM)

    # 3. Binance Margin - 平仓列表
    key = "fr_dump_symbols:binance_margin"
    symbols_json = json.dumps(DUMP_SYMBOLS_MARGIN, ensure_ascii=False)
    rds.set(key, symbols_json)
    print(f"✅ 已写入 {len(DUMP_SYMBOLS_MARGIN)} 个交易对到 '{key}'")
    total += len(DUMP_SYMBOLS_MARGIN)

    # 4. Binance Margin - 建仓列表
    key = "fr_trade_symbols:binance_margin"
    symbols_json = json.dumps(TRADE_SYMBOLS_MARGIN, ensure_ascii=False)
    rds.set(key, symbols_json)
    print(f"✅ 已写入 {len(TRADE_SYMBOLS_MARGIN)} 个交易对到 '{key}'")
    total += len(TRADE_SYMBOLS_MARGIN)

    return total


def print_three_line_table(headers: List[str], rows: List[List[str]]) -> None:
    """打印三线表格"""
    # 计算列宽
    ncols = len(headers)
    widths = [0] * ncols
    for i, h in enumerate(headers):
        widths[i] = max(widths[i], len(h))
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(cell))

    # 格式化行
    def fmt_row(values: List[str]) -> str:
        parts: List[str] = []
        for i, v in enumerate(values):
            parts.append(v.ljust(widths[i]))
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
    print("\n📊 交易对列表配置:")
    print("=" * 80)

    print_symbol_list(rds, "fr_dump_symbols:binance_um", "🔴 Binance UM - 平仓列表")
    print_symbol_list(rds, "fr_trade_symbols:binance_um", "🟢 Binance UM - 建仓列表")
    print_symbol_list(rds, "fr_dump_symbols:binance_margin", "🔴 Binance Margin - 平仓列表")
    print_symbol_list(rds, "fr_trade_symbols:binance_margin", "🟢 Binance Margin - 建仓列表")


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    rds = redis.from_url(args.redis_url) if args.redis_url else redis.Redis(
        host=args.host, port=args.port, db=args.db, password=args.password
    )

    print("🔄 开始同步 Funding Rate 交易对列表...")
    print(f"📍 Redis: {args.host}:{args.port}/{args.db}")
    print()

    # 同步列表
    total = sync_symbol_lists(rds)
    print(f"\n✅ 共写入 {total} 个交易对（跨4个列表）")

    # 打印结果
    print_all_symbol_lists(rds)

    print("\n✅ 同步完成！")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
