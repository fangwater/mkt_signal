#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将 Funding Rate 交易对列表同步到 Redis 并打印。

Symbol 列表是 exchange 维度的。

根据交易所写入 Redis key（String 类型，JSON 数组）：
  - fr_dump_symbols:{exchange}          - 平仓列表
  - fr_trade_symbols:{exchange}         - 建仓列表
  - fr_fwd_trade_symbols:{exchange}     - 正套建仓列表
  - fr_bwd_trade_symbols:{exchange}     - 反套建仓列表

示例：
  python scripts/sync_fr_symbol_lists.py --exchange binance
  python scripts/sync_fr_symbol_lists.py --exchange okex
  python scripts/sync_fr_symbol_lists.py --exchange binance --redis-url redis://:pwd@127.0.0.1:6379/0
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, List

# 支持的交易所
SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]


def try_import_redis():
    try:
        import redis  # type: ignore
        return redis
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync Funding Rate symbol lists to Redis")
    p.add_argument("--exchange", required=True, choices=SUPPORTED_EXCHANGES,
                   help="交易所名称（必填）")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    return p.parse_args()


# ========== 交易对白名单配置 ==========

# 正套币对列表
FWD_SYMBOLS_8H: List[str] = [
    "ONEUSDT", "ICXUSDT", "SFPUSDT", "ZILUSDT", "DENTUSDT",
    "HOTUSDT", "ANKRUSDT", "HIGHUSDT", "RDNTUSDT", "CHRUSDT",
]

FWD_SYMBOLS_4H: List[str] = [
    # "A2ZUSDT", "SAHARAUSDT", "NFPUSDT", "CUSDT", "SANTOSUSDT",
    # "PUMPUSDT", "JTOUSDT", "NEWTUSDT", "AIXBTUSDT", "AUCTIONUSDT",

    'A2ZUSDT', 'SAHARAUSDT', 'NFPUSDT', 'CUSDT', 'ACEUSDT', 'BEAMXUSDT', 
    'SANTOSUSDT', 'AIUSDT', 'USTCUSDT', 'SLPUSDT', 'AXLUSDT', 'VANRYUSDT', 
    'XPLUSDT', 'SAGAUSDT', 'PUMPUSDT', 'INITUSDT', 'LINEAUSDT', 'CATIUSDT',
    "JTOUSDT", "NEWTUSDT", "AIXBTUSDT", "AUCTIONUSDT",
]

FWD_SYMBOLS: List[str] = FWD_SYMBOLS_8H + FWD_SYMBOLS_4H

# 反套币对列表
BWD_SYMBOLS_8H: List[str] = [
    "HFTUSDT", "STGUSDT", "ALICEUSDT", "ZENUSDT", "APTUSDT", 
    "DASHUSDT", "ASTRUSDT", "KSMUSDT", "ZECUSDT", "MINAUSDT", 
    
]

BWD_SYMBOLS_4H: List[str] = [
    # "USUALUSDT", "DYMUSDT", "XAIUSDT", "TREEUSDT", "RAREUSDT",
    # "RVNUSDT", "NEWTUSDT", "WLFIUSDT", "STRKUSDT", "WCTUSDT",
    # "AUCTIONUSDT", "ZKUSDT", "ILVUSDT", "BABYUSDT", "ORCAUSDT",
    # "BANANAUSDT", "FLUXUSDT",

    'USUALUSDT', 'XAIUSDT', 'RVNUSDT', 'TREEUSDT', 'RAREUSDT',
    'NEWTUSDT', 'WLFIUSDT', 'STRKUSDT', 'AUCTIONUSDT', 'ILVUSDT', 
    'RONINUSDT', 'ZKUSDT', 'WCTUSDT', 'BANANAUSDT', 'ORCAUSDT', 
    'BABYUSDT', 'FLUXUSDT',
    "DYMUSDT",
]

# # 正套币对列表
# FWD_SYMBOLS_8H: List[str] = [
# ]

# FWD_SYMBOLS_4H: List[str] = [
# ]

# FWD_SYMBOLS: List[str] = FWD_SYMBOLS_8H + FWD_SYMBOLS_4H

# # 反套币对列表
# BWD_SYMBOLS_8H: List[str] = [
# ]

# BWD_SYMBOLS_4H: List[str] = [
# ]

BWD_SYMBOLS: List[str] = BWD_SYMBOLS_8H + BWD_SYMBOLS_4H

# 合并所有交易对（用于平仓列表）
SYMBOL_ALLOWLIST: List[str] = list(set(FWD_SYMBOLS + BWD_SYMBOLS))


def sync_symbol_lists(rds, exchange: str) -> int:
    """同步交易对列表到 Redis"""
    total = 0

    # 1. 平仓列表
    key = f"fr_dump_symbols:{exchange}"
    rds.set(key, json.dumps(SYMBOL_ALLOWLIST, ensure_ascii=False))
    print(f"✅ 已写入 {len(SYMBOL_ALLOWLIST)} 个交易对到 '{key}'")
    total += len(SYMBOL_ALLOWLIST)

    # 2. 建仓列表
    key = f"fr_trade_symbols:{exchange}"
    rds.set(key, json.dumps(SYMBOL_ALLOWLIST, ensure_ascii=False))
    print(f"✅ 已写入 {len(SYMBOL_ALLOWLIST)} 个交易对到 '{key}'")
    total += len(SYMBOL_ALLOWLIST)

    # 3. 正套建仓列表
    key = f"fr_fwd_trade_symbols:{exchange}"
    rds.set(key, json.dumps(FWD_SYMBOLS, ensure_ascii=False))
    print(f"✅ 已写入 {len(FWD_SYMBOLS)} 个交易对到 '{key}'")
    total += len(FWD_SYMBOLS)

    # 4. 反套建仓列表
    key = f"fr_bwd_trade_symbols:{exchange}"
    rds.set(key, json.dumps(BWD_SYMBOLS, ensure_ascii=False))
    print(f"✅ 已写入 {len(BWD_SYMBOLS)} 个交易对到 '{key}'")
    total += len(BWD_SYMBOLS)

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


def print_all_symbol_lists(rds, exchange: str) -> None:
    """打印所有交易对列表"""
    print("\n📊 交易对列表配置:")
    print("=" * 80)

    print_symbol_list(rds, f"fr_dump_symbols:{exchange}", f"🔴 {exchange.upper()} - 平仓列表")
    print_symbol_list(rds, f"fr_trade_symbols:{exchange}", f"🟢 {exchange.upper()} - 建仓列表")
    print_symbol_list(rds, f"fr_fwd_trade_symbols:{exchange}", f"🟢 {exchange.upper()} - 正套建仓列表")
    print_symbol_list(rds, f"fr_bwd_trade_symbols:{exchange}", f"🔴 {exchange.upper()} - 反套建仓列表")


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    rds = redis.from_url(args.redis_url) if args.redis_url else redis.Redis(
        host=args.host, port=args.port, db=args.db, password=args.password
    )

    print(f"🔄 开始同步 Funding Rate 交易对列表 (exchange={args.exchange})...")
    print(f"📍 Redis: {args.host}:{args.port}/{args.db}")
    print()

    # 同步列表
    total = sync_symbol_lists(rds, args.exchange)
    print(f"\n✅ 共写入 {total} 个交易对条目")

    # 打印结果
    print_all_symbol_lists(rds, args.exchange)

    print("\n✅ 同步完成！")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
