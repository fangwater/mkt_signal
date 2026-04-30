#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 intra per-symbol 单手量(amount_u USDT) 覆盖配置（从 Redis 读取，不修改）。

读取 Redis String(JSON):
  <env_name>:<open_venue>:<hedge_venue>:amount_u_overrides
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, Optional

SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]


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


def infer_env_name_from_cwd() -> Optional[str]:
    name = Path.cwd().name.strip().lower()
    return name or None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print intra amount_u overrides from Redis")
    p.add_argument("--exchange", default=os.environ.get("EXCHANGE"))
    p.add_argument("--open-venue", default=os.environ.get("OPEN_VENUE"))
    p.add_argument("--hedge-venue", default=os.environ.get("HEDGE_VENUE"))
    p.add_argument("--env-name")
    args = p.parse_args()

    exchange = args.exchange
    if not exchange:
        exchange = infer_exchange_from_name(args.env_name) if args.env_name else infer_exchange_from_cwd()

    if exchange:
        exchange = normalize_exchange(exchange)
        if not args.open_venue:
            args.open_venue = f"{exchange}-margin"
        if not args.hedge_venue:
            args.hedge_venue = f"{exchange}-futures"

    if not args.open_venue or not args.hedge_venue:
        p.error("需要 --exchange 或 --open-venue/--hedge-venue")

    args.open_venue = args.open_venue.lower()
    args.hedge_venue = args.hedge_venue.lower()

    if not args.env_name:
        args.env_name = infer_env_name_from_cwd()
    if not args.env_name:
        p.error("无法推断 env_name")
    return args


def make_amount_u_key(env_name: str, open_venue: str, hedge_venue: str) -> str:
    return f"{env_name}:{open_venue}:{hedge_venue}:amount_u_overrides"


def print_value(rds, key: str) -> None:
    raw = rds.get(key)
    print("\n📊 intra amount_u 覆盖配置:")
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

    key = make_amount_u_key(args.env_name, args.open_venue, args.hedge_venue)
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)
    print(f"🔎 读取 intra amount_u 覆盖配置: {key}")
    print(f"📁 env_name: {args.env_name}")
    print(f"🏷️ open: {args.open_venue}  hedge: {args.hedge_venue}")
    print("📍 Redis: 127.0.0.1:6379/0")
    print_value(rds, key)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
