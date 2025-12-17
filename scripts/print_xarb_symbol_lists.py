#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 xarb（跨所）交易对列表（按跨所 pair 维度，从 Redis 读取）。

读取 3 个 Redis key（均为 String，JSON 数组）：
  1. xarb_dump_symbols:{key_suffix}          - 平仓列表
  2. xarb_fwd_trade_symbols:{key_suffix}     - 正套建仓列表
  3. xarb_bwd_trade_symbols:{key_suffix}     - 反套建仓列表

其中 key_suffix 为 "<open_exchange>-<hedge_exchange>"（例如 okex-binance）。
可通过 --open-venue/--hedge-venue、--env-name 或 CWD 目录名推断。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from typing import List, Optional, Tuple

SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]
NAMESPACE = "xarb"


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
    ex = v.split("-", 1)[0]
    ex = normalize_exchange(ex)
    if ex not in SUPPORTED_EXCHANGES:
        return None
    return ex


def infer_pair_from_name(name: str) -> Optional[Tuple[str, str]]:
    n = (name or "").strip().lower()
    m = re.match(r"^([a-z0-9]+)[-_]([a-z0-9]+)[-_]xarb([_-].*)?$", n)
    if not m:
        return None
    open_ex = normalize_exchange(m.group(1))
    hedge_ex = normalize_exchange(m.group(2))
    if open_ex not in SUPPORTED_EXCHANGES or hedge_ex not in SUPPORTED_EXCHANGES:
        return None
    if open_ex == hedge_ex:
        return None
    return open_ex, hedge_ex


def infer_pair_from_cwd() -> Optional[Tuple[str, str]]:
    from pathlib import Path

    return infer_pair_from_name(Path.cwd().name)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print xarb symbol lists from Redis")
    p.add_argument("--open-venue", help="开仓 venue（例如 okex-futures）")
    p.add_argument("--hedge-venue", help="对冲 venue（例如 binance-futures）")
    p.add_argument("--env-name", help="环境目录名（例如 okex-binance-xarb-trade）")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    return p.parse_args()


def resolve_key_suffix(args: argparse.Namespace) -> Optional[str]:
    if args.open_venue and args.hedge_venue:
        open_ex = exchange_from_venue(args.open_venue)
        hedge_ex = exchange_from_venue(args.hedge_venue)
        if not open_ex or not hedge_ex:
            return None
        return f"{open_ex}-{hedge_ex}"

    if args.env_name:
        pair = infer_pair_from_name(args.env_name)
        if pair:
            return f"{pair[0]}-{pair[1]}"

    pair = infer_pair_from_cwd()
    if pair:
        return f"{pair[0]}-{pair[1]}"

    return None


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

    key_suffix = resolve_key_suffix(args)
    if not key_suffix:
        print(
            "❌ 需要 --open-venue/--hedge-venue 或 --env-name，或在目录名包含 '<open>-<hedge>-xarb-...' 以自动推断",
            file=sys.stderr,
        )
        return 2

    rds = (
        redis.from_url(args.redis_url)
        if args.redis_url
        else redis.Redis(host=args.host, port=args.port, db=args.db, password=args.password)
    )

    print(f"📍 Redis: {args.host}:{args.port}/{args.db}")
    print(f"📍 key_suffix: {key_suffix}\n")

    print("\n📊 xarb 交易对列表配置:")
    print("=" * 80)
    total = 0
    total += print_symbol_list(rds, f"{NAMESPACE}_dump_symbols:{key_suffix}", "🔴 dump_symbols")
    total += print_symbol_list(rds, f"{NAMESPACE}_fwd_trade_symbols:{key_suffix}", "🟢 fwd_trade_symbols")
    total += print_symbol_list(rds, f"{NAMESPACE}_bwd_trade_symbols:{key_suffix}", "🔴 bwd_trade_symbols")

    print("\n📈 统计摘要:")
    print("=" * 80)
    for k in [
        f"{NAMESPACE}_dump_symbols:{key_suffix}",
        f"{NAMESPACE}_fwd_trade_symbols:{key_suffix}",
        f"{NAMESPACE}_bwd_trade_symbols:{key_suffix}",
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
    print(f"\n  总计: {total} 个交易对（跨所有列表）\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

