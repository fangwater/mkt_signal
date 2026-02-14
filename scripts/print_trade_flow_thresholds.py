#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 trade_flow_feature 的金额阈值配置（从 Redis STRING key 读取）。

读取:
  {venue}:{symbol}:amount-threshold

示例:
  python scripts/print_trade_flow_thresholds.py --venue binance-futures
  python scripts/print_trade_flow_thresholds.py --venue okex-futures --symbol BTC-USDT
  python scripts/print_trade_flow_thresholds.py --venue bybit-futures
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

AMOUNT_THRESHOLD_SUFFIX = "amount-threshold"
VENUE_DIR_REGEX = re.compile(r"^[a-z0-9]+-(?:futures|margin|spot|swap|perp|perpetual)$")


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


@dataclass
class PrintedRow:
    symbol: str
    medium_notional_threshold: float
    large_notional_threshold: float
    suffix: str
    key: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Print trade_flow_feature amount threshold entries from Redis"
    )
    p.add_argument("--venue", help="venue，例如 binance-futures；省略时尝试按当前目录名推断")
    p.add_argument(
        "--symbol",
        help="仅打印单个 symbol（大小写不敏感）",
    )
    p.add_argument(
        "--symbols",
        nargs="*",
        help="仅打印给定 symbols（大小写不敏感）",
    )
    p.add_argument(
        "--show-key",
        action="store_true",
        help="输出 source_key 列",
    )
    p.add_argument("--redis-host", default="127.0.0.1")
    p.add_argument("--redis-port", type=int, default=6379)
    p.add_argument("--redis-db", type=int, default=0)
    p.add_argument("--redis-username", default=None)
    p.add_argument("--redis-password", default=None)
    p.add_argument(
        "--redis-prefix",
        default="",
        help="可选 key 前缀（与 Rust RedisSettings.prefix 含义一致）",
    )
    return p.parse_args()


def _decode_maybe_bytes(v: Any) -> str:
    if isinstance(v, bytes):
        return v.decode("utf-8", "ignore")
    return str(v)


def infer_venue_from_cwd() -> Optional[str]:
    name = Path.cwd().name.lower()
    if VENUE_DIR_REGEX.fullmatch(name):
        return name
    return None


def _is_finite(v: float) -> bool:
    return not (v != v or v == float("inf") or v == float("-inf"))


def parse_optional_f64(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        f = float(value)
    except Exception:
        return None
    if not _is_finite(f):
        return None
    return f


def with_prefix(prefix: str, key: str) -> str:
    return f"{prefix}{key}" if prefix else key


def split_key(full_key: str, prefix: str) -> str:
    if prefix and full_key.startswith(prefix):
        return full_key[len(prefix) :]
    return full_key


def parse_symbol_from_key(logical_key: str, venue: str, suffix: str) -> Optional[str]:
    parts = logical_key.split(":")
    if len(parts) != 3:
        return None
    key_venue, symbol, key_suffix = parts
    if key_venue != venue or key_suffix != suffix:
        return None
    return symbol if symbol else None


def collect_entries(
    value: Any,
    out: List[Dict[str, Any]],
    symbol_from_key: Optional[str],
) -> None:
    if isinstance(value, list):
        for item in value:
            collect_entries(item, out, symbol_from_key)
        return

    if not isinstance(value, dict):
        return

    medium_notional_threshold = parse_optional_f64(value.get("medium_notional_threshold"))
    large_notional_threshold = parse_optional_f64(value.get("large_notional_threshold"))
    if medium_notional_threshold is not None and large_notional_threshold is not None:
        symbol_raw = value.get("symbol")
        symbol = None
        if isinstance(symbol_raw, str) and symbol_raw.strip():
            symbol = symbol_raw.strip()
        elif symbol_from_key:
            symbol = symbol_from_key

        if symbol and medium_notional_threshold > 0.0 and large_notional_threshold > 0.0 and medium_notional_threshold <= large_notional_threshold:
            out.append(
                {
                    "symbol": symbol,
                    "medium_notional_threshold": medium_notional_threshold,
                    "large_notional_threshold": large_notional_threshold,
                }
            )

    for child in value.values():
        if isinstance(child, (dict, list)):
            collect_entries(child, out, symbol_from_key)


def format_num(v: Optional[float]) -> str:
    if v is None:
        return "-"
    text = f"{v:.10f}".rstrip("0").rstrip(".")
    return text or "0"


def print_table(headers: List[str], rows: List[List[str]]) -> None:
    widths = [len(h) for h in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def fmt(row: Iterable[str]) -> str:
        return "  ".join(text.ljust(widths[i]) for i, text in enumerate(row))

    header_line = fmt(headers)
    rule_top = "=" * len(header_line)
    rule_mid = "-" * len(header_line)
    print(rule_top)
    print(header_line)
    print(rule_mid)
    for row in rows:
        print(fmt(row))
    print(rule_top)


def build_symbol_filter(args: argparse.Namespace) -> Optional[set[str]]:
    symbols: List[str] = []
    if args.symbol:
        symbols.append(args.symbol)
    if args.symbols:
        symbols.extend(args.symbols)
    if not symbols:
        return None
    return {s.strip().upper() for s in symbols if s and s.strip()}


def connect_redis(args: argparse.Namespace):
    redis = try_import_redis()
    if redis is None:
        raise SystemExit("缺少 redis 包，请先安装: pip install redis")
    kwargs: Dict[str, Any] = {
        "host": args.redis_host,
        "port": args.redis_port,
        "db": args.redis_db,
        "password": args.redis_password,
        "decode_responses": False,
    }
    if args.redis_username:
        kwargs["username"] = args.redis_username
    return redis.Redis(**kwargs)


def main() -> int:
    args = parse_args()
    venue = (args.venue or "").strip()
    if not venue:
        inferred = infer_venue_from_cwd()
        if inferred:
            venue = inferred
            print(f"[INFO] 未提供 --venue，基于目录推断: {venue}", file=sys.stderr)
    if not venue:
        raise SystemExit("需要 --venue，或在目录名使用 <exchange>-<market> 以自动推断（如 binance-futures）")

    suffixes = [AMOUNT_THRESHOLD_SUFFIX]

    prefix = args.redis_prefix or ""
    symbol_filter = build_symbol_filter(args)
    rds = connect_redis(args)

    merged: Dict[str, PrintedRow] = {}
    parse_errors = 0

    for suffix in suffixes:
        pattern = with_prefix(prefix, f"{venue}:*:{suffix}")
        keys = rds.keys(pattern)
        for raw_key in keys:
            full_key = _decode_maybe_bytes(raw_key)
            logical = split_key(full_key, prefix)
            key_symbol = parse_symbol_from_key(logical, venue, suffix)
            if not key_symbol:
                continue

            raw = rds.get(full_key)
            if raw is None:
                continue
            text = _decode_maybe_bytes(raw)
            try:
                payload = json.loads(text)
            except Exception:
                parse_errors += 1
                continue

            entries: List[Dict[str, Any]] = []
            collect_entries(payload, entries, key_symbol)
            for entry in entries:
                symbol = str(entry["symbol"]).strip()
                if not symbol:
                    continue
                if symbol_filter and symbol.upper() not in symbol_filter:
                    continue
                merged[symbol] = PrintedRow(
                    symbol=symbol,
                    medium_notional_threshold=float(entry["medium_notional_threshold"]),
                    large_notional_threshold=float(entry["large_notional_threshold"]),
                    suffix=suffix,
                    key=logical,
                )

    print(
        f"query: venue={venue} suffixes={','.join(suffixes)} "
        f"redis={args.redis_host}:{args.redis_port}/{args.redis_db} prefix={prefix!r}"
    )
    if symbol_filter:
        print(f"symbol_filter={sorted(symbol_filter)}")

    rows_sorted = sorted(merged.values(), key=lambda r: r.symbol)
    if not rows_sorted:
        print("no threshold entries found")
        if parse_errors:
            print(f"parse_errors={parse_errors}", file=sys.stderr)
        return 0

    headers = ["symbol", "medium_notional_threshold", "large_notional_threshold", "source_suffix"]
    if args.show_key:
        headers.append("source_key")

    table_rows: List[List[str]] = []
    for row in rows_sorted:
        line = [
            row.symbol,
            format_num(row.medium_notional_threshold),
            format_num(row.large_notional_threshold),
            row.suffix,
        ]
        if args.show_key:
            line.append(row.key)
        table_rows.append(line)

    print_table(headers, table_rows)
    print(f"rows={len(rows_sorted)} parse_errors={parse_errors}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
