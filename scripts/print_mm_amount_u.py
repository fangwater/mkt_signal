#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 MM amount_u 覆盖配置（从 Redis 读取）。

读取 Redis String(JSON):
  <env_name>:<venue>:mm:amount_u

JSON 格式:
  {
    "BTCUSDT": 150.0,
    "ETHUSDT": 80.0
  }

说明:
  - 仅 MM 使用该配置。
  - 未配置的 symbol 会回退到 strategy params 里的 default_order_amount。
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, Optional


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
    for ex in ["binance", "okex", "okx", "bybit", "bitget", "gate"]:
        if re.search(rf"(^|[^a-z0-9]){ex}([^a-z0-9]|$)", n):
            return normalize_exchange(ex)
    return None


def infer_exchange_from_cwd() -> Optional[str]:
    return infer_exchange_from_name(Path.cwd().name)


def infer_env_name_from_cwd() -> Optional[str]:
    name = Path.cwd().name.strip().lower()
    return name or None


def resolve_venue() -> Optional[str]:
    ex = infer_exchange_from_cwd()
    if ex:
        return f"{ex}-futures"
    return None


def make_amount_u_key(env_name: str, venue: str) -> str:
    return f"{env_name}:{venue}:mm:amount_u"


def normalize_symbol(raw: str) -> str:
    text = re.sub(r"[^A-Za-z0-9]", "", (raw or "").strip()).upper()
    if not text:
        raise ValueError(f"invalid symbol: {raw!r}")
    return text


def normalize_amount_u_mapping(raw_json: str) -> Dict[str, float]:
    try:
        payload = json.loads(raw_json)
    except Exception as exc:
        raise ValueError(f"invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("JSON must be an object: {symbol: amount_u}")

    normalized: Dict[str, float] = {}
    for raw_symbol, raw_value in payload.items():
        symbol = normalize_symbol(str(raw_symbol))
        try:
            amount_u = float(raw_value)
        except Exception as exc:
            raise ValueError(f"invalid amount_u for {symbol}: {raw_value}") from exc
        if not (amount_u > 0.0):
            raise ValueError(f"amount_u must be > 0 for {symbol}: {raw_value}")
        normalized[symbol] = amount_u
    return normalized


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Print MM amount_u overrides(JSON symbol->amount_u) from Redis"
    )
    return p.parse_args()


def print_value(rds, key: str) -> None:
    raw = rds.get(key)
    print("\n📊 MM amount_u 覆盖配置:")
    print("=" * 80)
    if raw is None:
        print(f"⚠️  STRING '{key}' 为空或不存在")
        return

    text = raw.decode("utf-8", "ignore") if isinstance(raw, bytes) else str(raw)
    try:
        mapping = normalize_amount_u_mapping(text)
    except ValueError:
        print(text)
        return

    if not mapping:
        print("{}")
        return

    for symbol in sorted(mapping.keys()):
        print(f"  {symbol:24} {mapping[symbol]:>12g}")


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
        return 1

    env_name = infer_env_name_from_cwd()
    if not env_name:
        print("❌ 无法从当前目录推断 env_name", file=sys.stderr)
        return 1

    key = make_amount_u_key(env_name, venue)
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    print(f"🔎 读取 MM amount_u 覆盖配置: {key}")
    print(f"📁 env_name: {env_name}")
    print(f"🏷️ venue: {venue}")
    print("📍 Redis: 127.0.0.1:6379/0")

    print_value(rds, key)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
