#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 Funding Rate 交易对列表（按 env_name + key_suffix 维度，从 Redis 读取）。

读取 4 个 Redis key（均为 String，JSON 数组）：
  1. {env_name}:fr_dump_symbols:{key_suffix}          - 平仓列表
  2. {env_name}:fr_trade_symbols:{key_suffix}         - 建仓列表（旧字段）
  3. {env_name}:fr_fwd_trade_symbols:{key_suffix}     - 正套建仓列表
  4. {env_name}:fr_bwd_trade_symbols:{key_suffix}     - 反套建仓列表
  5. {env_name}:fr_unimmr_close_symbols:{key_suffix}  - UniMMR 算法平仓候选列表

其中 key_suffix 为 "<open_venue>_<hedge_venue>"（例如 gate-margin_gate-futures）。
env_name 为部署目录名，例如 `binance_fr_trade01`。

示例：
  python scripts/print_fr_symbol_lists.py --env-name binance_fr_trade01 --exchange binance
  python scripts/print_fr_symbol_lists.py       # 在部署目录下自动推断 env_name/exchange
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import List, Optional

SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]

EXCHANGE_DEFAULTS = {
    "binance": ("binance-margin", "binance-futures"),
    "okex": ("okex-margin", "okex-futures"),
    "bybit": ("bybit-margin", "bybit-futures"),
    "bitget": ("bitget-margin", "bitget-futures"),
    "gate": ("gate-margin", "gate-futures"),
}


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
    p.add_argument("--open-venue", help="open 侧 venue（如 binance-margin）")
    p.add_argument("--hedge-venue", help="hedge 侧 venue（如 binance-futures）")
    p.add_argument(
        "--exchange",
        choices=SUPPORTED_EXCHANGES,
        help="交易所名称（可选，若未提供则尝试从目录名推断）",
    )
    p.add_argument(
        "--env-name",
        help="部署 env 名（例如 binance_fr_trade01）；未提供时使用当前目录名",
    )
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


def make_key_suffix(open_venue: str, hedge_venue: str) -> str:
    return f"{open_venue.strip().lower()}_{hedge_venue.strip().lower()}"


def symbol_list_key(env_name: str, list_name: str, key_suffix: str) -> str:
    return f"{env_name}:fr_{list_name}:{key_suffix}"


def infer_env_name_from_cwd() -> Optional[str]:
    name = Path.cwd().name.strip().lower()
    return name or None


def warn_if_env_name_mismatched(env_name: str, exchange: str) -> None:
    pattern = rf"^{re.escape(exchange)}_fr_[a-z0-9][a-z0-9_-]*$"
    if not re.match(pattern, env_name):
        print(
            f"[WARN] env-name '{env_name}' 不符合 {exchange}_fr_<suffix> 规范，仍然继续",
            file=sys.stderr,
        )


def resolve_venues(args: argparse.Namespace) -> Optional[tuple[str, str]]:
    if args.open_venue and args.hedge_venue:
        return args.open_venue.strip().lower(), args.hedge_venue.strip().lower()
    if args.exchange:
        return EXCHANGE_DEFAULTS.get(args.exchange)
    inferred = infer_exchange_from_cwd()
    if inferred:
        return EXCHANGE_DEFAULTS.get(inferred)
    return None


def print_all_symbol_lists(rds, env_name: str, key_suffix: str) -> None:
    """打印所有交易对列表"""
    print("\n📊 Funding Rate 交易对列表配置:")
    print("=" * 80)

    print_symbol_list(
        rds,
        symbol_list_key(env_name, "dump_symbols", key_suffix),
        f"🔴 {key_suffix} - 平仓列表",
    )
    print_symbol_list(
        rds,
        symbol_list_key(env_name, "trade_symbols", key_suffix),
        f"🟢 {key_suffix} - 建仓列表",
    )
    print_symbol_list(
        rds,
        symbol_list_key(env_name, "unimmr_close_symbols", key_suffix),
        f"🟠 {key_suffix} - UniMMR 平仓候选",
    )
    print_symbol_list(
        rds,
        symbol_list_key(env_name, "fwd_trade_symbols", key_suffix),
        f"🟢 {key_suffix} - 正套建仓列表",
    )
    print_symbol_list(
        rds,
        symbol_list_key(env_name, "bwd_trade_symbols", key_suffix),
        f"🔴 {key_suffix} - 反套建仓列表",
    )


def print_summary(rds, env_name: str, key_suffix: str) -> None:
    """打印统计摘要"""
    print("\n📈 统计摘要:")
    print("=" * 80)

    keys = [
        symbol_list_key(env_name, "dump_symbols", key_suffix),
        symbol_list_key(env_name, "trade_symbols", key_suffix),
        symbol_list_key(env_name, "unimmr_close_symbols", key_suffix),
        symbol_list_key(env_name, "fwd_trade_symbols", key_suffix),
        symbol_list_key(env_name, "bwd_trade_symbols", key_suffix),
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

    venues = resolve_venues(args)
    if not venues:
        print(
            "❌ 需要 --open-venue/--hedge-venue，或 --exchange，或在目录名包含 binance/okex/bybit/bitget/gate 前缀以自动推断",
            file=sys.stderr,
        )
        return 2
    open_venue, hedge_venue = venues
    key_suffix = make_key_suffix(open_venue, hedge_venue)
    exchange = open_venue.split("-", 1)[0] if "-" in open_venue else open_venue
    env_name = (args.env_name or infer_env_name_from_cwd() or "").strip().lower()
    if not env_name:
        print(
            "❌ 需要 --env-name，或在 <exchange>_fr_<suffix> 命名的目录下运行以自动推断",
            file=sys.stderr,
        )
        return 2
    if not args.env_name:
        print(f"[INFO] 未提供 env-name，基于目录推断: {env_name}", file=sys.stderr)
    warn_if_env_name_mismatched(env_name, exchange)

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    print(f"📦 Env: {env_name}")
    print("📍 Redis: 127.0.0.1:6379/0\n")

    # 打印所有列表
    print_all_symbol_lists(rds, env_name, key_suffix)

    # 打印统计摘要
    print_summary(rds, env_name, key_suffix)

    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
