#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
同步 intra（同所期现）per-symbol max_pos_u(USDT) 覆盖到 Redis 并打印。

写入 Redis String(JSON):
  <env_name>:<open_venue>:<hedge_venue>:max_pos_u_overrides

JSON 格式:
  {
    "BTCUSDT": 5000.0,
    "ETH-USDT": 2000
  }

说明:
  - pre_trade 拉取该映射；命中 symbol 即覆盖 risk_params 里的标量 max_pos_u；
    没命中按标量走。
  - symbol 自动规范化为 alphanumeric uppercase（去掉常见分隔符）。
  - value 必须是大于 0 的数字。
  - env_name 来自 ENV_NAME 环境变量或当前工作目录名。

示例:
  cd ~/okex-intra-arb01
  python intra_scripts/sync_intra_max_pos_u.py --json '{"BTCUSDT":5000,"eth-usdt":2000}'
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


def normalize_symbol(raw: str) -> str:
    text = re.sub(r"[^A-Za-z0-9]", "", (raw or "").strip()).upper()
    if not text:
        raise ValueError(f"invalid symbol: {raw!r}")
    return text


def normalize_max_pos_u_mapping(raw_json: str) -> Dict[str, float]:
    try:
        payload = json.loads(raw_json)
    except Exception as exc:
        raise ValueError(f"invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("JSON must be an object: {symbol: max_pos_u}")

    normalized: Dict[str, float] = {}
    for raw_symbol, raw_value in payload.items():
        symbol = normalize_symbol(str(raw_symbol))
        try:
            max_pos_u = float(raw_value)
        except Exception as exc:
            raise ValueError(f"invalid max_pos_u for {symbol}: {raw_value}") from exc
        if not (max_pos_u > 0.0):
            raise ValueError(f"max_pos_u must be > 0 for {symbol}: {raw_value}")
        normalized[symbol] = max_pos_u
    return normalized


def dumps_max_pos_u_mapping(mapping: Dict[str, float]) -> str:
    ordered = {symbol: float(f"{mapping[symbol]:.12g}") for symbol in sorted(mapping.keys())}
    return json.dumps(ordered, ensure_ascii=False, separators=(",", ":"))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync intra max_pos_u overrides(JSON symbol->max_pos_u) to Redis")
    p.add_argument("--exchange", default=os.environ.get("EXCHANGE"))
    p.add_argument("--open-venue", default=os.environ.get("OPEN_VENUE"))
    p.add_argument("--hedge-venue", default=os.environ.get("HEDGE_VENUE"))
    p.add_argument("--env-name", help="环境目录名（默认取 CWD basename）")
    p.add_argument("--json", default="{}", help='JSON mapping, e.g. \'{"BTCUSDT":5000,"ETH-USDT":2000}\'')
    args = p.parse_args()

    exchange = args.exchange
    if not exchange:
        exchange = infer_exchange_from_name(args.env_name) if args.env_name else infer_exchange_from_cwd()
        if exchange:
            print(f"[INFO] 未提供 --exchange，基于目录推断: exchange={exchange}", file=sys.stderr)

    if exchange:
        exchange = normalize_exchange(exchange)
        if exchange not in SUPPORTED_EXCHANGES:
            p.error(f"不支持的 exchange: {exchange}")
        if not args.open_venue:
            args.open_venue = f"{exchange}-margin"
        if not args.hedge_venue:
            args.hedge_venue = f"{exchange}-futures"

    if not args.open_venue or not args.hedge_venue:
        p.error("需要 --exchange 或同时提供 --open-venue/--hedge-venue")

    args.open_venue = args.open_venue.lower()
    args.hedge_venue = args.hedge_venue.lower()

    if not args.env_name:
        args.env_name = infer_env_name_from_cwd()
    if not args.env_name:
        p.error("无法推断 env_name，请通过 --env-name 显式提供或在合适的目录运行")
    return args


def make_max_pos_u_key(env_name: str, open_venue: str, hedge_venue: str) -> str:
    return f"{env_name}:{open_venue}:{hedge_venue}:max_pos_u_overrides"


def print_value(rds, key: str) -> None:
    raw = rds.get(key)
    print("\n📊 intra max_pos_u 覆盖配置:")
    print("=" * 80)
    if raw is None:
        print(f"⚠️  STRING '{key}' 为空或不存在")
        return
    text = raw.decode("utf-8", "ignore") if isinstance(raw, bytes) else str(raw)
    try:
        mapping = normalize_max_pos_u_mapping(text)
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
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    try:
        mapping = normalize_max_pos_u_mapping(args.json)
    except ValueError as exc:
        print(f"❌ {exc}", file=sys.stderr)
        return 1

    key = make_max_pos_u_key(args.env_name, args.open_venue, args.hedge_venue)
    payload = dumps_max_pos_u_mapping(mapping)
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)
    rds.set(key, payload)

    print(f"🔄 同步 intra max_pos_u 覆盖配置: {key}")
    print(f"📁 env_name: {args.env_name}")
    print(f"🏷️ open: {args.open_venue}  hedge: {args.hedge_venue}")
    print(f"🧩 symbols: {len(mapping)}")
    print("📍 Redis: 127.0.0.1:6379/0")

    print_value(rds, key)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
