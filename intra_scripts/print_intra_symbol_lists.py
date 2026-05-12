#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 intra（同所期现）交易对列表（按 exchange 维度，从 Redis 读取）。

读取 4 个 Redis key（均为 String，JSON 数组）：
  1. intra_dump_symbols:{exchange}        - 平仓列表
  2. intra_fwd_trade_symbols:{exchange}   - 正套建仓列表
  3. intra_bwd_trade_symbols:{exchange}   - 反套建仓列表
  4. {env_name}:intra_unimmr_close_symbols:{open_venue}_{hedge_venue}
                                            - UniMMR 算法平仓候选列表

key_suffix 为单一 exchange（同所期现），可通过 --exchange / --env-name / CWD 推断。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Optional

SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]
NAMESPACE = "intra"


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def normalize_exchange(ex: str) -> str:
    ex = (ex or "").strip().lower()
    if ex == "okx":
        ex = "okex"
    return ex


def exchange_from_venue(venue: str) -> Optional[str]:
    v = (venue or "").strip().lower()
    if not v:
        return None
    ex = normalize_exchange(v.split("-", 1)[0])
    if ex not in SUPPORTED_EXCHANGES:
        return None
    return ex


def infer_exchange_from_name(name: str) -> Optional[str]:
    n = (name or "").strip().lower()
    m = re.match(r"^([a-z0-9]+)[-_]intra([_-].*)?$", n)
    if not m:
        return None
    ex = normalize_exchange(m.group(1))
    if ex not in SUPPORTED_EXCHANGES:
        return None
    return ex


def infer_exchange_from_cwd() -> Optional[str]:
    return infer_exchange_from_name(Path.cwd().name)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print intra symbol lists from Redis")
    p.add_argument("--exchange", default=os.environ.get("EXCHANGE"))
    p.add_argument("--open-venue", default=os.environ.get("OPEN_VENUE"))
    p.add_argument("--hedge-venue", default=os.environ.get("HEDGE_VENUE"))
    p.add_argument("--env-name", help="环境目录名（例如 binance-intra-trade）")
    return p.parse_args()


def resolve_exchange(args: argparse.Namespace) -> Optional[str]:
    if args.exchange:
        return normalize_exchange(args.exchange)
    if args.open_venue:
        ex = exchange_from_venue(args.open_venue)
        if ex:
            return ex
    if args.env_name:
        ex = infer_exchange_from_name(args.env_name)
        if ex:
            return ex
    return infer_exchange_from_cwd()


def resolve_env_name(args: argparse.Namespace) -> str:
    if args.env_name:
        return args.env_name.strip().lower()
    return Path.cwd().name.strip().lower()


def resolve_venues(args: argparse.Namespace, exchange: str) -> tuple[str, str]:
    open_venue = (args.open_venue or f"{exchange}-margin").strip().lower()
    hedge_venue = (args.hedge_venue or f"{exchange}-futures").strip().lower()
    return open_venue, hedge_venue


def unimmr_close_key(env_name: str, open_venue: str, hedge_venue: str) -> str:
    suffix = f"{open_venue.strip().lower()}_{hedge_venue.strip().lower()}"
    return f"{env_name}:intra_unimmr_close_symbols:{suffix}" if env_name else f"intra_unimmr_close_symbols:{suffix}"


def print_symbol_list(rds, key: str, title: str) -> int:
    print(f"\n{title} ({key}):")
    data = rds.get(key)
    if not data:
        print("  ⚠️  未找到数据")
        return 0
    text = data.decode("utf-8", "ignore") if isinstance(data, bytes) else str(data)
    try:
        symbols = json.loads(text)
        if isinstance(symbols, list):
            print(f"  总数: {len(symbols)}")
            for i in range(0, len(symbols), 5):
                chunk = symbols[i : i + 5]
                print("  " + "  ".join(f"{s:15}" for s in chunk))
            return len(symbols)
        print(f"  格式异常: {text}")
        return 0
    except Exception as exc:
        print(f"  解析失败: {exc}")
        print(f"  原始值: {text}")
        return 0


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    exchange = resolve_exchange(args)
    if not exchange or exchange not in SUPPORTED_EXCHANGES:
        print(
            "❌ 需要 --exchange / --open-venue / --env-name，或在目录名包含 '<exchange>-intra-...' 以自动推断",
            file=sys.stderr,
        )
        return 2
    open_venue, hedge_venue = resolve_venues(args, exchange)
    env_name = resolve_env_name(args)

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    print("📍 Redis: 127.0.0.1:6379/0")
    print(f"📦 env_name: {env_name or '-'}")
    print(f"📍 exchange: {exchange}\n")

    print("\n📊 intra 交易对列表配置:")
    print("=" * 80)
    total = 0
    total += print_symbol_list(rds, f"{NAMESPACE}_dump_symbols:{exchange}", "🔴 dump_symbols")
    total += print_symbol_list(rds, f"{NAMESPACE}_fwd_trade_symbols:{exchange}", "🟢 fwd_trade_symbols")
    total += print_symbol_list(rds, f"{NAMESPACE}_bwd_trade_symbols:{exchange}", "🔴 bwd_trade_symbols")
    total += print_symbol_list(
        rds,
        unimmr_close_key(env_name, open_venue, hedge_venue),
        "🟠 unimmr_close_symbols",
    )

    print("\n📈 统计摘要:")
    print("=" * 80)
    for k in [
        f"{NAMESPACE}_dump_symbols:{exchange}",
        f"{NAMESPACE}_fwd_trade_symbols:{exchange}",
        f"{NAMESPACE}_bwd_trade_symbols:{exchange}",
        unimmr_close_key(env_name, open_venue, hedge_venue),
    ]:
        data = rds.get(k)
        if not data:
            continue
        text = data.decode("utf-8", "ignore") if isinstance(data, bytes) else str(data)
        try:
            symbols = json.loads(text)
            if isinstance(symbols, list):
                print(f"  {k:45} {len(symbols):3} 个")
        except Exception:
            pass
    print(f"\n  总计: {total} 个交易对（intra all lists）\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
