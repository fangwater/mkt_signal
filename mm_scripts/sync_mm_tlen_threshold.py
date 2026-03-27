#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
同步 MM 的 tlen 阈值到 Redis。

Redis key 约定：
  - Hash: <hedge_venue_prefix>_<open_venue_prefix>:<strategy>:tlen_threshold
    单 venue MM 例如：binance_futures_binance_futures:mm:tlen_threshold

其中：
  - venue_prefix = venue 中的 '-' 替换为 '_'，例如 binance-futures -> binance_futures
  - strategy 在本脚本中固定为 mm

脚本内明文配置：
  - 直接编辑下方 TLEN_THRESHOLD_JSON，格式必须为 JSON object，形如：
      {
        "BTCUSDT": 120000.0,
        "ETHUSDT": 80000.0
      }

示例：
  python mm_scripts/sync_mm_tlen_threshold.py
  python mm_scripts/sync_mm_tlen_threshold.py --venue gate-futures
  python mm_scripts/sync_mm_tlen_threshold.py --venue all
  python mm_scripts/sync_mm_tlen_threshold.py --symbol BTCUSDT --symbol ETHUSDT
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from typing import Dict, Iterable, List, Optional

SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]
SUPPORTED_VENUES = [f"{exchange}-futures" for exchange in SUPPORTED_EXCHANGES]
STRATEGY = "mm"

# 明文阈值配置。请按需直接编辑这里，保持 JSON object 格式：symbol -> value。
TLEN_THRESHOLD_JSON = r"""
{
  "BTCUSDT": 319.0965,
  "TRXUSDT": 3083.44555,
  "FILUSDT": 28531.845,
  "BNBUSDT": 98.5448,
  "AVAXUSDT": 2572.922,
  "DOTUSDT": 9402.1025,
  "BCHUSDT": 242.8828,
  "ARBUSDT": 9807.98592
}
"""


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
    prefix = venue_prefix(venue)
    return f"{prefix}_{prefix}:{STRATEGY}:tlen_threshold"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync mm tlen thresholds to Redis")
    parser.add_argument(
        "--venue",
        default="binance-futures",
        help="目标 venue，支持 binance-futures/okex-futures/bybit-futures/bitget-futures/gate-futures/all，默认 binance-futures",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        default=[],
        help="仅同步指定 symbol，可重复传入，例如 --symbol BTCUSDT --symbol ETHUSDT",
    )
    parser.add_argument("--redis-host", default="127.0.0.1", help="Redis host，默认 127.0.0.1")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port，默认 6379")
    parser.add_argument("--redis-db", type=int, default=0, help="Redis db，默认 0")
    parser.add_argument("--redis-password", default=None, help="Redis password，默认空")
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="允许同步空配置；默认空配置会报错，避免误删线上数据",
    )
    return parser.parse_args()


def expand_venues(value: str) -> List[str]:
    raw = (value or "").strip().lower()
    if raw == "all":
        return SUPPORTED_VENUES[:]
    venue = normalize_venue(raw)
    if venue is None:
        raise ValueError(f"unsupported venue: {value}")
    return [venue]


def load_thresholds_from_script() -> Dict[str, float]:
    try:
        parsed = json.loads(TLEN_THRESHOLD_JSON)
    except Exception as exc:
        raise ValueError(f"TLEN_THRESHOLD_JSON 不是合法 JSON: {exc}") from exc

    if not isinstance(parsed, dict):
        raise ValueError("TLEN_THRESHOLD_JSON 必须是 JSON object，格式为 symbol -> value")

    result: Dict[str, float] = {}
    for raw_symbol, raw_value in parsed.items():
        symbol = normalize_symbol(str(raw_symbol))
        if not symbol:
            continue
        try:
            value = float(raw_value)
        except Exception as exc:
            raise ValueError(f"symbol={raw_symbol!r} 的 value 无法解析为数字: {raw_value!r}") from exc
        if math.isnan(value) or math.isinf(value):
            raise ValueError(f"symbol={raw_symbol!r} 的 value 非法: {raw_value!r}")
        result[symbol] = value
    return dict(sorted(result.items()))


def filter_symbols(data: Dict[str, float], symbols: Iterable[str]) -> Dict[str, float]:
    wanted = {normalize_symbol(symbol) for symbol in symbols if normalize_symbol(symbol)}
    if not wanted:
        return data
    return {symbol: value for symbol, value in data.items() if symbol in wanted}


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


def sync_one(rds, venue: str, thresholds: Dict[str, float]) -> None:
    key = redis_key(venue)
    meta_key = f"{key}:meta"

    rds.delete(key)
    rds.delete(meta_key)
    if thresholds:
        mapping = {symbol: f"{value:.8f}" for symbol, value in thresholds.items()}
        rds.hset(key, mapping=mapping)

    print(f"\n📍 Venue={venue} Strategy={STRATEGY}")
    print(f"🔄 写入 Redis Hash: {key}")
    print(f"🧹 删除旧 Meta Key: {meta_key}")
    print(f"✅ 已同步 symbols: {len(thresholds)}")

    if thresholds:
        rows = [[symbol, f"{value:.8f}"] for symbol, value in thresholds.items()]
        print_three_line_table(["symbol", "tlen_threshold"], rows)
    else:
        print("⚠️  当前配置为空，Hash 已清空")


def main() -> int:
    args = parse_args()

    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    try:
        venues = expand_venues(args.venue)
        thresholds = filter_symbols(load_thresholds_from_script(), args.symbol)
    except ValueError as exc:
        print(f"❌ {exc}", file=sys.stderr)
        return 1

    if not thresholds and not args.allow_empty:
        print(
            "❌ 当前 TLEN_THRESHOLD_JSON 为空，或经 --symbol 过滤后为空。请先在脚本里填写 JSON 配置；如需强制清空 Redis，请显式加 --allow-empty",
            file=sys.stderr,
        )
        return 1

    rds = redis.Redis(
        host=args.redis_host,
        port=args.redis_port,
        db=args.redis_db,
        password=args.redis_password,
    )

    print(f"📍 Redis: {args.redis_host}:{args.redis_port}/{args.redis_db}")
    print(f"📦 Venue Targets: {', '.join(venues)}")
    print(f"🧭 Strategy Target: {STRATEGY}")

    for venue in venues:
        sync_one(rds, venue, thresholds)

    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
