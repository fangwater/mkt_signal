#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将 xarb（跨所）交易对列表同步到 Redis 并打印（按跨所 pair 维度）。

写入 3 个 Redis key（String 类型，JSON 数组）：
  - xarb_dump_symbols:{key_suffix}          - 平仓列表
  - xarb_fwd_trade_symbols:{key_suffix}     - 正套建仓列表
  - xarb_bwd_trade_symbols:{key_suffix}     - 反套建仓列表

其中 key_suffix 为 "<open_exchange>-<hedge_exchange>"（例如 okex-binance）。

推断规则（优先级从高到低）：
  1) --open-venue/--hedge-venue（例如 okex-futures / binance-futures）
  2) --env-name（例如 okex-binance-xarb-trade）
  3) CWD 目录名（例如 okex-binance-xarb-trade）
"""

from __future__ import annotations

import argparse
import json
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
    p = argparse.ArgumentParser(description="Sync xarb symbol lists to Redis")
    p.add_argument("--open-venue", help="开仓 venue（例如 okex-futures）")
    p.add_argument("--hedge-venue", help="对冲 venue（例如 binance-futures）")
    p.add_argument("--env-name", help="环境目录名（例如 okex-binance-xarb-trade）")
    return p.parse_args()


# ========== 交易对白名单配置 ==========

FWD_SYMBOLS: List[str] = ["SOLUSDT","dogeusdt","wldusdt","filusdt","dotusdt","btcusdt","ethusdt"]

BWD_SYMBOLS: List[str] = ["SOLUSDT","dogeusdt","wldusdt","filusdt","dotusdt","btcusdt","ethusdt"]

FWD_SYMBOLS: List[str] = []
BWD_SYMBOLS: List[str] = []


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


def print_symbol_list(rds, key: str, title: str) -> None:
    print(f"\n{title} ({key}):")
    data = rds.get(key)
    if not data:
        print("  ⚠️  未找到数据")
        return
    text = data.decode("utf-8", "ignore") if isinstance(data, bytes) else str(data)
    try:
        symbols = json.loads(text)
        if isinstance(symbols, list):
            print(f"  总数: {len(symbols)}")
            for i in range(0, len(symbols), 5):
                chunk = symbols[i : i + 5]
                print("  " + "  ".join(f"{s:15}" for s in chunk))
        else:
            print(f"  格式异常: {text}")
    except Exception as exc:
        print(f"  解析失败: {exc}")
        print(f"  原始值: {text}")


def sync_symbol_lists(rds, key_suffix: str) -> int:
    dump_key = f"{NAMESPACE}_dump_symbols:{key_suffix}"
    fwd_key = f"{NAMESPACE}_fwd_trade_symbols:{key_suffix}"
    bwd_key = f"{NAMESPACE}_bwd_trade_symbols:{key_suffix}"

    empty_list: List[str] = []
    rds.set(dump_key, json.dumps(empty_list, ensure_ascii=False))
    print(f"✅ 已写入 {len(empty_list)} 个交易对到 '{dump_key}'（平仓列表）")

    rds.set(fwd_key, json.dumps(FWD_SYMBOLS, ensure_ascii=False))
    print(f"✅ 已写入 {len(FWD_SYMBOLS)} 个交易对到 '{fwd_key}'（正套）")

    rds.set(bwd_key, json.dumps(BWD_SYMBOLS, ensure_ascii=False))
    print(f"✅ 已写入 {len(BWD_SYMBOLS)} 个交易对到 '{bwd_key}'（反套）")

    return len(FWD_SYMBOLS) + len(BWD_SYMBOLS)


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

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    print(f"🔄 开始同步 xarb 交易对列表 (key_suffix={key_suffix})...")
    print("📍 Redis: 127.0.0.1:6379/0")
    print()

    total = sync_symbol_lists(rds, key_suffix)
    print(f"\n✅ 共写入 {total} 个交易对条目")

    print("\n📊 xarb 交易对列表配置:")
    print("=" * 80)
    print_symbol_list(rds, f"{NAMESPACE}_dump_symbols:{key_suffix}", "🔴 dump_symbols")
    print_symbol_list(rds, f"{NAMESPACE}_fwd_trade_symbols:{key_suffix}", "🟢 fwd_trade_symbols")
    print_symbol_list(rds, f"{NAMESPACE}_bwd_trade_symbols:{key_suffix}", "🔴 bwd_trade_symbols")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
