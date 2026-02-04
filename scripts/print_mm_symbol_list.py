#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印做市(MM)交易对列表（按 venue 维度，从 Redis 读取）。

读取 Redis key（String，JSON 数组）：
  - mm_trade_symbols:{key_suffix}

其中 key_suffix 默认等于 venue（如 binance-futures）。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Optional

SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]

EXCHANGE_DEFAULTS = {
    "binance": "binance-futures",
    "okex": "okex-futures",
    "bybit": "bybit-futures",
    "bitget": "bitget-futures",
    "gate": "gate-futures",
}


def try_import_redis():
    try:
        import redis  # type: ignore
        return redis
    except Exception:
        return None


def infer_exchange_from_cwd() -> Optional[str]:
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
    p = argparse.ArgumentParser(description="Print MM symbol list from Redis")
    p.add_argument("--venue", help="venue（如 binance-futures）")
    p.add_argument(
        "--exchange",
        choices=SUPPORTED_EXCHANGES,
        help="交易所名称（可选，若未提供则尝试从目录名推断）",
    )
    return p.parse_args()


def resolve_venue(args: argparse.Namespace) -> Optional[str]:
    if args.venue:
        return args.venue.strip().lower()
    if args.exchange:
        return EXCHANGE_DEFAULTS.get(args.exchange)
    inferred = infer_exchange_from_cwd()
    if inferred:
        return EXCHANGE_DEFAULTS.get(inferred)
    return None


def make_key_suffix(venue: str) -> str:
    return venue.strip().lower()


def print_symbol_list(rds, key: str, title: str) -> None:
    print(f"\n{title} ({key}):")
    symbols_json = rds.get(key)

    if not symbols_json:
        print("  ⚠️  未找到数据")
        return

    symbols_str = (
        symbols_json.decode("utf-8", "ignore")
        if isinstance(symbols_json, bytes)
        else str(symbols_json)
    )

    try:
        symbols = json.loads(symbols_str)
        if isinstance(symbols, list):
            print(f"  总数: {len(symbols)}")
            for i in range(0, len(symbols), 5):
                chunk = symbols[i : i + 5]
                print("  " + "  ".join(f"{s:15}" for s in chunk))
        else:
            print(f"  格式异常: {symbols_str}")
    except Exception as e:
        print(f"  解析失败: {e}")
        print(f"  原始值: {symbols_str}")


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    venue = resolve_venue(args)
    if not venue:
        print(
            "❌ 需要 --venue，或 --exchange，或在目录名包含 binance/okex/bybit/bitget/gate 前缀以自动推断",
            file=sys.stderr,
        )
        return 2

    key_suffix = make_key_suffix(venue)

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    print("📍 Redis: 127.0.0.1:6379/0\n")
    print("📊 MM 交易对列表配置:")
    print("=" * 80)

    print_symbol_list(rds, f"mm_trade_symbols:{key_suffix}", f"🟢 {key_suffix} - 交易列表")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
