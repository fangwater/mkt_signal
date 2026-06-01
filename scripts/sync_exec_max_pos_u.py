#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
同步 Exec per-symbol max_pos_u(USDT) 覆盖到 Redis 并打印。

写入 Redis String(JSON):
  <env_name>:<exec_venue>:exec:max_pos_u

JSON 格式:
  {"BTCUSDT": 5000.0, "ETH-USDT": 2000}

说明:
  - pre_trade 拉取该映射；命中 symbol 即覆盖 risk_params 里的 max_pos_u 标量。
  - symbol 自动规范化为 alphanumeric uppercase。
  - value 必须是大于 0 的数字。

示例:
  cd ~/binance_exec_trade
  python scripts/sync_exec_max_pos_u.py --json '{"BTCUSDT":5000,"eth-usdt":2000}'
  python scripts/sync_exec_max_pos_u.py --exec-venue binance-futures --json '{"BTCUSDT":5000}'
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, Optional


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


def infer_exec_venue_from_cwd() -> Optional[str]:
    name = Path.cwd().name.lower()
    candidates = [name]
    if "_" in name:
        candidates.append(name.split("_", 1)[0])
    for cand in candidates:
        for exchange, venue in EXCHANGE_DEFAULTS.items():
            if cand.startswith(exchange):
                return venue
    return None


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
    p = argparse.ArgumentParser(description="Sync Exec max_pos_u overrides to Redis")
    p.add_argument("--exec-venue", default=os.environ.get("EXEC_VENUE"), help="exec venue（如 binance-futures）")
    p.add_argument("--env-name", default=os.environ.get("ENV_NAME"), help="环境目录名（默认取 CWD basename）")
    p.add_argument("--json", default="{}", help="JSON mapping: {symbol: max_pos_u}")
    args = p.parse_args()

    if not args.exec_venue:
        args.exec_venue = infer_exec_venue_from_cwd()
        if args.exec_venue:
            print(f"[INFO] 基于目录推断: exec={args.exec_venue}", file=sys.stderr)

    if not args.exec_venue:
        p.error("需要 --exec-venue，或在 exec 目录运行")
    args.exec_venue = args.exec_venue.lower()

    if not args.env_name:
        args.env_name = infer_env_name_from_cwd()
    if not args.env_name:
        p.error("无法推断 env_name，请通过 --env-name 显式提供")
    args.env_name = args.env_name.strip().lower()
    return args


def make_max_pos_u_key(env_name: str, exec_venue: str) -> str:
    return f"{env_name}:{exec_venue}:exec:max_pos_u"


def print_three_line_table(headers, rows) -> None:
    widths = [len(h) for h in headers]
    for row in rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(value))

    def fmt(values):
        return "  ".join(values[idx].ljust(widths[idx]) for idx in range(len(headers)))

    header_line = fmt(headers)
    print("=" * len(header_line))
    print(header_line)
    print("-" * len(header_line))
    for row in rows:
        print(fmt(row))
    print("=" * len(header_line))


def print_value(rds, key: str) -> None:
    raw = rds.get(key)
    print("\n📊 Exec max_pos_u 覆盖配置:")
    print("=" * 80)
    print(f"🔑 Redis String Key: {key}")
    if raw is None:
        print("⚠️  STRING 为空或不存在")
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
    rows = [[symbol, f"{mapping[symbol]:g}"] for symbol in sorted(mapping.keys())]
    print_three_line_table(["Symbol", "max_pos_u"], rows)


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装", file=sys.stderr)
        return 2

    try:
        mapping = normalize_max_pos_u_mapping(args.json)
    except ValueError as exc:
        print(f"❌ {exc}", file=sys.stderr)
        return 1

    key = make_max_pos_u_key(args.env_name, args.exec_venue)
    payload = dumps_max_pos_u_mapping(mapping)
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)
    rds.set(key, payload)

    print(f"🔄 同步 Exec max_pos_u 覆盖配置: {key}")
    print(f"📁 env_name: {args.env_name}")
    print(f"🏷️ exec: {args.exec_venue}")
    print(f"🧩 symbols: {len(mapping)}")
    print("📍 Redis: 127.0.0.1:6379/0")
    print_value(rds, key)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
