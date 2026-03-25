#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 MM 的 tlen 阈值配置（从 Redis 读取）。

Redis key 约定：
  - Hash: <venue_prefix>:<strategy>:tlen_threshold
    例如：binance_futures:mm:tlen_threshold

示例：
  python mm_scripts/print_mm_tlen_threshold.py
  python mm_scripts/print_mm_tlen_threshold.py BTCUSDT
  python mm_scripts/print_mm_tlen_threshold.py --venue gate-futures
  python mm_scripts/print_mm_tlen_threshold.py --venue all
"""

from __future__ import annotations

import argparse
import sys
from typing import Dict, Iterable, List, Optional

SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]
SUPPORTED_VENUES = [f"{exchange}-futures" for exchange in SUPPORTED_EXCHANGES]
STRATEGY = "mm"


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def normalize_exchange(exchange: str) -> str:
    ex = (exchange or "").strip().lower()
    if ex == "okx":
        ex = "okex"
    return ex


def normalize_venue(venue: str) -> Optional[str]:
    raw = (venue or "").strip().lower().replace("_", "-")
    if not raw:
        return None
    parts = raw.split("-", 1)
    if len(parts) != 2:
        return None
    exchange, market = parts
    exchange = normalize_exchange(exchange)
    normalized = f"{exchange}-{market}"
    if normalized not in SUPPORTED_VENUES:
        return None
    return normalized


def normalize_symbol(symbol: str) -> str:
    return "".join(ch for ch in (symbol or "").upper() if ch.isalnum())


def venue_prefix(venue: str) -> str:
    return venue.replace("-", "_")


def redis_key(venue: str) -> str:
    return f"{venue_prefix(venue)}:{STRATEGY}:tlen_threshold"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print mm tlen thresholds from Redis")
    parser.add_argument(
        "symbols",
        nargs="*",
        help="可选，按 symbol 过滤，例如 BTCUSDT ETHUSDT",
    )
    parser.add_argument(
        "--venue",
        default="binance-futures",
        help="目标 venue，支持 binance-futures/okex-futures/bybit-futures/bitget-futures/gate-futures/all，默认 binance-futures",
    )
    parser.add_argument("--redis-host", default="127.0.0.1", help="Redis host，默认 127.0.0.1")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port，默认 6379")
    parser.add_argument("--redis-db", type=int, default=0, help="Redis db，默认 0")
    parser.add_argument("--redis-password", default=None, help="Redis password，默认空")
    return parser.parse_args()


def expand_venues(value: str) -> List[str]:
    raw = (value or "").strip().lower()
    if raw == "all":
        return SUPPORTED_VENUES[:]
    venue = normalize_venue(raw)
    if venue is None:
        raise ValueError(f"unsupported venue: {value}")
    return [venue]


def print_three_line_table(headers: List[str], rows: List[List[str]]) -> None:
    widths = [len(header) for header in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))

    def fmt(values: List[str]) -> str:
        return "  ".join(values[index].ljust(widths[index]) for index in range(len(values)))

    header_line = fmt(headers)
    rule_top = "=" * len(header_line)
    rule_mid = "-" * len(header_line)

    print(rule_top)
    print(header_line)
    print(rule_mid)
    for row in rows:
        print(fmt(row))
    print(rule_top)


def decode_hash(data: Dict[object, object]) -> Dict[str, str]:
    decoded: Dict[str, str] = {}
    for raw_key, raw_value in data.items():
        key = raw_key.decode("utf-8", "ignore") if isinstance(raw_key, bytes) else str(raw_key)
        value = (
            raw_value.decode("utf-8", "ignore") if isinstance(raw_value, bytes) else str(raw_value)
        )
        decoded[key] = value
    return decoded


def filter_symbols(data: Dict[str, str], symbols: Iterable[str]) -> Dict[str, str]:
    wanted = {normalize_symbol(symbol) for symbol in symbols if normalize_symbol(symbol)}
    if not wanted:
        return data
    return {symbol: value for symbol, value in data.items() if normalize_symbol(symbol) in wanted}


def print_one(rds, venue: str, symbols: List[str]) -> None:
    key = redis_key(venue)
    data = filter_symbols(decode_hash(rds.hgetall(key)), symbols)

    print(f"\n📍 Venue={venue} Strategy={STRATEGY}")
    print(f"🔎 Redis Hash: {key}")

    if not data:
        print("⚠️  当前组合下没有读取到数据")
        return

    rows = [[symbol, data[symbol]] for symbol in sorted(data.keys())]
    print(f"📊 symbols={len(rows)}")
    print_three_line_table(["symbol", "tlen_threshold"], rows)


def main() -> int:
    args = parse_args()

    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    try:
        venues = expand_venues(args.venue)
    except ValueError as exc:
        print(f"❌ {exc}", file=sys.stderr)
        return 1

    rds = redis.Redis(
        host=args.redis_host,
        port=args.redis_port,
        db=args.redis_db,
        password=args.redis_password,
    )

    print(f"📍 Redis: {args.redis_host}:{args.redis_port}/{args.redis_db}")
    if args.symbols:
        print(f"🔬 Symbol Filter: {', '.join(normalize_symbol(symbol) for symbol in args.symbols)}")

    for venue in venues:
        print_one(rds, venue, args.symbols)

    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
