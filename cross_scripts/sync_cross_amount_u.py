#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
同步 cross（跨所）per-symbol 单手量(amount_u USDT)覆盖到 Redis 并打印。

写入 Redis String(JSON):
  <env_name>:<open_venue>:<hedge_venue>:amount_u_overrides

JSON 格式:
  {"BTCUSDT": 150.0, "ETH-USDT": 80}

说明:
  - 命中 symbol 即覆盖 strategy_params 里的 default order_amount；
    没命中按 strategy_params 的标量走。
  - symbol 自动规范化为 alphanumeric uppercase。
  - value 必须是大于 0 的数字。

示例:
  cd ~/okex-binance-cross-trade
  python cross_scripts/sync_cross_amount_u.py --json '{"BTCUSDT":150,"eth-usdt":80}'
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple

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


def infer_pair_from_name(name: str) -> Optional[Tuple[str, str]]:
    n = (name or "").strip().lower()
    m = re.match(r"^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross([_-].*)?$", n)
    if not m:
        return None
    open_ex = normalize_exchange(m.group(1))
    hedge_ex = normalize_exchange(m.group(2))
    if open_ex not in SUPPORTED_EXCHANGES or hedge_ex not in SUPPORTED_EXCHANGES:
        return None
    return open_ex, hedge_ex


def infer_cross_venues_from_env_name(env_name: str) -> Optional[Tuple[str, str]]:
    pair = infer_pair_from_name(env_name)
    if not pair:
        return None
    if pair[0] == pair[1]:
        return None
    return f"{pair[0]}-futures", f"{pair[1]}-futures"


def infer_cross_venues_from_cwd() -> Optional[Tuple[str, str]]:
    return infer_cross_venues_from_env_name(Path.cwd().name)


def infer_env_name_from_cwd() -> Optional[str]:
    name = Path.cwd().name.strip().lower()
    return name or None


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


def dumps_amount_u_mapping(mapping: Dict[str, float]) -> str:
    ordered = {symbol: float(f"{mapping[symbol]:.12g}") for symbol in sorted(mapping.keys())}
    return json.dumps(ordered, ensure_ascii=False, separators=(",", ":"))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync cross amount_u overrides to Redis")
    p.add_argument("--open-venue", help="open 侧 venue（例如 okex-futures）")
    p.add_argument("--hedge-venue", help="hedge 侧 venue（例如 binance-futures）")
    p.add_argument("--env-name", help="环境目录名（默认取 CWD basename）")
    p.add_argument("--json", default="{}", help='JSON mapping')
    args = p.parse_args()

    if not args.open_venue and not args.hedge_venue:
        inferred = (
            infer_cross_venues_from_env_name(args.env_name) if args.env_name else infer_cross_venues_from_cwd()
        )
        if inferred:
            args.open_venue, args.hedge_venue = inferred
            print(f"[INFO] 基于目录推断: open={args.open_venue}, hedge={args.hedge_venue}", file=sys.stderr)

    if not args.open_venue or not args.hedge_venue:
        p.error("需要 --open-venue 与 --hedge-venue，或使用 --env-name / 在 cross 目录运行")

    args.open_venue = args.open_venue.lower()
    args.hedge_venue = args.hedge_venue.lower()

    if not args.env_name:
        args.env_name = infer_env_name_from_cwd()
    if not args.env_name:
        p.error("无法推断 env_name，请通过 --env-name 显式提供")
    return args


def make_amount_u_key(env_name: str, open_venue: str, hedge_venue: str) -> str:
    return f"{env_name}:{open_venue}:{hedge_venue}:amount_u_overrides"


def print_value(rds, key: str) -> None:
    raw = rds.get(key)
    print("\n📊 cross amount_u 覆盖配置:")
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
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装", file=sys.stderr)
        return 2

    try:
        mapping = normalize_amount_u_mapping(args.json)
    except ValueError as exc:
        print(f"❌ {exc}", file=sys.stderr)
        return 1

    key = make_amount_u_key(args.env_name, args.open_venue, args.hedge_venue)
    payload = dumps_amount_u_mapping(mapping)
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)
    rds.set(key, payload)

    print(f"🔄 同步 cross amount_u 覆盖配置: {key}")
    print(f"📁 env_name: {args.env_name}")
    print(f"🏷️ open: {args.open_venue}  hedge: {args.hedge_venue}")
    print(f"🧩 symbols: {len(mapping)}")
    print("📍 Redis: 127.0.0.1:6379/0")
    print_value(rds, key)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
