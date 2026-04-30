#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 FR per-symbol max_pos_u 覆盖配置（从 Redis 读取，不修改）。

读取 Redis String(JSON):
  <env_name>:<open_venue>:<hedge_venue>:max_pos_u_overrides
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple

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


def infer_venues_from_cwd() -> Optional[Tuple[str, str]]:
    name = Path.cwd().name.lower()
    candidates = [name]
    if "_" in name:
        candidates.append(name.split("_", 1)[0])
    for cand in candidates:
        for ex, pair in EXCHANGE_DEFAULTS.items():
            if cand.startswith(ex):
                return pair
    return None


def infer_env_name_from_cwd() -> Optional[str]:
    name = Path.cwd().name.strip().lower()
    return name or None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print FR max_pos_u overrides from Redis")
    p.add_argument("--open-venue")
    p.add_argument("--hedge-venue")
    p.add_argument("--env-name")
    args = p.parse_args()

    if not args.open_venue and not args.hedge_venue:
        inferred = infer_venues_from_cwd()
        if inferred:
            args.open_venue, args.hedge_venue = inferred

    if not args.open_venue or not args.hedge_venue:
        p.error("需要 --open-venue 与 --hedge-venue，或在 FR 目录运行")

    args.open_venue = args.open_venue.lower()
    args.hedge_venue = args.hedge_venue.lower()

    if not args.env_name:
        args.env_name = infer_env_name_from_cwd()
    if not args.env_name:
        p.error("无法推断 env_name")
    return args


def make_max_pos_u_key(env_name: str, open_venue: str, hedge_venue: str) -> str:
    return f"{env_name}:{open_venue}:{hedge_venue}:max_pos_u_overrides"


def print_value(rds, key: str) -> None:
    raw = rds.get(key)
    print("\n📊 FR max_pos_u 覆盖配置:")
    print("=" * 80)
    if raw is None:
        print(f"⚠️  STRING '{key}' 为空或不存在")
        return
    text = raw.decode("utf-8", "ignore") if isinstance(raw, bytes) else str(raw)
    try:
        mapping: Dict[str, float] = json.loads(text)
    except Exception:
        print(text)
        return
    if not isinstance(mapping, dict) or not mapping:
        print("{}")
        return
    for symbol in sorted(mapping.keys()):
        print(f"  {symbol:24} {float(mapping[symbol]):>12g}")


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装", file=sys.stderr)
        return 2

    key = make_max_pos_u_key(args.env_name, args.open_venue, args.hedge_venue)
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)
    print(f"🔎 读取 FR max_pos_u 覆盖配置: {key}")
    print(f"📁 env_name: {args.env_name}")
    print(f"🏷️ open: {args.open_venue}  hedge: {args.hedge_venue}")
    print("📍 Redis: 127.0.0.1:6379/0")
    print_value(rds, key)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
