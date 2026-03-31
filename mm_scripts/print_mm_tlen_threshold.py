#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印共享 tlen 阈值配置（从 Redis 读取）。

Redis key 约定：
  - Hash: <venue_prefix>:tlen_threshold
    例如：binance_futures:tlen_threshold

示例：
  python mm_scripts/print_mm_tlen_threshold.py
  python mm_scripts/print_mm_tlen_threshold.py BTCUSDT
  python mm_scripts/print_mm_tlen_threshold.py --venue all
"""

from __future__ import annotations

import argparse
import sys
from typing import Dict, Iterable, List, Optional

SUPPORTED_VENUES = [
    "binance-margin",
    "binance-futures",
    "okex-margin",
    "okex-futures",
]
QUOTE_ASSETS = ("USDT", "USDC", "BUSD", "FDUSD", "USD")


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


def split_assets(symbol: str):
    text = "".join(ch for ch in (symbol or "").upper() if ch.isalnum())
    for quote in QUOTE_ASSETS:
        if text.endswith(quote) and len(text) > len(quote):
            return text[: -len(quote)], quote
    return text, "USDT"


def normalize_symbol_for_venue(symbol: str, venue: str) -> str:
    text = (symbol or "").strip().upper().replace("_", "-").replace("/", "-")
    if not text:
        return ""
    if venue == "okex-margin":
        if text.endswith("-SWAP"):
            text = text[: -len("-SWAP")]
        if "-" in text:
            return text
        base, quote = split_assets(text)
        return f"{base}-{quote}"
    if venue == "okex-futures":
        if text.endswith("-SWAP"):
            return text
        if "-" in text:
            return f"{text}-SWAP"
        base, quote = split_assets(text)
        return f"{base}-{quote}-SWAP"
    return text.replace("-", "").replace("SWAP", "")


def venue_prefix(venue: str) -> str:
    return venue.replace("-", "_")


def redis_key(venue: str) -> str:
    return f"{venue_prefix(venue)}:tlen_threshold"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print shared tlen thresholds from Redis")
    parser.add_argument(
        "symbols",
        nargs="*",
        help="可选，按 symbol 过滤，例如 BTCUSDT ETHUSDT",
    )
    parser.add_argument(
        "--venue",
        default="binance-futures",
        help="目标 venue，支持 binance-margin/binance-futures/okex-margin/okex-futures/all，默认 binance-futures",
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


def filter_symbols(data: Dict[str, str], symbols: Iterable[str], venue: str) -> Dict[str, str]:
    wanted = {
        normalize_symbol_for_venue(symbol, venue)
        for symbol in symbols
        if normalize_symbol_for_venue(symbol, venue)
    }
    if not wanted:
        return data
    return {symbol: value for symbol, value in data.items() if symbol in wanted}


def print_one(rds, venue: str, symbols: List[str]) -> None:
    key = redis_key(venue)
    data = filter_symbols(decode_hash(rds.hgetall(key)), symbols, venue)

    print(f"\n📍 Venue={venue}")
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
        print(f"🔬 Raw Symbol Filter: {', '.join(args.symbols)}")

    for venue in venues:
        print_one(rds, venue, args.symbols)

    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
