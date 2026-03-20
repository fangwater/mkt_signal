#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将做市(MM)交易对列表同步到 Redis 并打印（按 venue 维度）。

写入 Redis key（String 类型，JSON 数组）：
  - mm_trade_symbols:{key_suffix}

其中 key_suffix 必须由当前目录推断得到的 venue 决定（如 binance_mm_beta -> binance-futures）。

示例：
  cd ~/binance_mm_beta
  python scripts/sync_mm_symbol_list.py
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import List, Optional

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
    p = argparse.ArgumentParser(
        description="Sync MM symbol list to Redis (venue is inferred from current directory)"
    )
    return p.parse_args()


# ======== 配置列表（请按需替换） ========
SYMBOLS: List[str] = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "AAVEUSDT",
    "ADAUSDT",
    "ARBUSDT",
    "ATOMUSDT",
    "AVAXUSDT",
    "BCHUSDT",
    "BNBUSDT",
    "DOGEUSDT",
    "DOTUSDT",
    "ETCUSDT",
    "FILUSDT",
    "HBARUSDT",
    "LINKUSDT",
    "LTCUSDT",
    "ONDOUSDT",
    "TONUSDT",
    "TRUMPUSDT",
    "TRXUSDT",
    "WLDUSDT",
    "XLMUSDT",
    "XRPUSDT",
]


def resolve_venue() -> Optional[str]:
    inferred = infer_exchange_from_cwd()
    if inferred:
        return EXCHANGE_DEFAULTS.get(inferred)
    return None


def make_key_suffix(venue: str) -> str:
    return venue.strip().lower()


def main() -> int:
    _args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    venue = resolve_venue()
    if not venue:
        print(
            "❌ 无法从当前目录推断 venue。请在目录名包含 binance/okex/bybit/bitget/gate 前缀的 MM 目录运行（如 ~/binance_mm_beta）",
            file=sys.stderr,
        )
        return 2

    key_suffix = make_key_suffix(venue)

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    key_trade = f"mm_trade_symbols:{key_suffix}"
    payload = json.dumps(SYMBOLS)

    rds.set(key_trade, payload)

    print(f"✅ 已写入 {len(SYMBOLS)} 个交易对到 {key_trade}")
    print("📍 Redis: 127.0.0.1:6379/0\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
