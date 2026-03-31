#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Print trade_flow_feature amount thresholds from Redis.

Reads hash key:
  {venue}:amount-thresholds

Hash field:
  - symbol

Hash value:
  - JSON object containing medium_notional_threshold / large_notional_threshold

Examples:
  python scripts/print_trade_flow_thresholds.py --venue binance-futures
  python scripts/print_trade_flow_thresholds.py --venue okex-margin --symbol BTCUSDT
  python scripts/print_trade_flow_thresholds.py
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

AMOUNT_THRESHOLD_SUFFIX = "amount-thresholds"
VENUE_RE = r"[a-z0-9]+-(?:margin|futures|spot|swap|perp|perpetual)"


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def infer_venue_from_cwd() -> Optional[str]:
    name = Path.cwd().name.lower()
    if re.fullmatch(VENUE_RE, name):
        return name
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Print trade_flow_feature amount thresholds from Redis hash key "
            "'{venue}:amount-thresholds'."
        )
    )
    parser.add_argument("--venue", help="Venue, e.g. binance-futures")
    parser.add_argument("--symbol", help="Filter by symbol, e.g. BTCUSDT")
    parser.add_argument("--host", default="127.0.0.1", help="Redis host")
    parser.add_argument("--port", type=int, default=6379, help="Redis port")
    parser.add_argument("--db", type=int, default=0, help="Redis DB index")
    parser.add_argument("--password", default=None, help="Redis password")
    parser.add_argument(
        "--prefix",
        default="",
        help="Optional Redis key prefix (if your deployment sets one)",
    )
    parser.add_argument(
        "--show-invalid",
        action="store_true",
        help="Include invalid threshold rows (non-finite/<=0/medium>large)",
    )
    args = parser.parse_args()

    venue = args.venue
    if not venue:
        venue = infer_venue_from_cwd()
        if venue:
            print(f"[INFO] venue inferred from cwd: {venue}", file=sys.stderr)
    if not venue:
        parser.error(
            "missing --venue; or run under a directory named like <exchange>-<market>"
        )
    args.venue = venue.lower()
    return args


def with_prefix(prefix: str, key: str) -> str:
    return f"{prefix}{key}" if prefix else key


def to_f64(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except Exception:
            return None
    return None


def collect_threshold_entries(value: Any, out: List[Dict[str, Any]]) -> None:
    if isinstance(value, list):
        for item in value:
            collect_threshold_entries(item, out)
        return

    if not isinstance(value, dict):
        return

    medium = to_f64(value.get("medium_notional_threshold"))
    large = to_f64(value.get("large_notional_threshold"))
    if medium is not None and large is not None:
        out.append(
            {
                "symbol": value.get("symbol"),
                "medium_notional_threshold": medium,
                "large_notional_threshold": large,
            }
        )

    for child in value.values():
        if isinstance(child, (list, dict)):
            collect_threshold_entries(child, out)


def format_num(value: Optional[float]) -> str:
    if value is None:
        return "-"
    text = f"{value:.8f}".rstrip("0").rstrip(".")
    return text or "0"


def print_table(rows: List[Dict[str, str]]) -> None:
    headers = ["symbol", "medium_notional", "large_notional", "valid", "source_key"]
    widths = {h: len(h) for h in headers}
    for row in rows:
        for h in headers:
            widths[h] = max(widths[h], len(row.get(h, "")))

    def make_line(ch: str = "-") -> str:
        return "+" + "+".join(ch * (widths[h] + 2) for h in headers) + "+"

    print(make_line("-"))
    print(
        "| "
        + " | ".join(h.ljust(widths[h]) for h in headers)
        + " |"
    )
    print(make_line("="))
    for row in rows:
        print(
            "| "
            + " | ".join(row.get(h, "").ljust(widths[h]) for h in headers)
            + " |"
        )
    print(make_line("-"))


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print(
            "[ERROR] redis package is not installed. Install with: pip install redis",
            file=sys.stderr,
        )
        return 2

    client = redis.Redis(
        host=args.host,
        port=args.port,
        db=args.db,
        password=args.password,
    )
    try:
        client.ping()
    except Exception as err:
        print(
            f"[ERROR] failed to connect redis {args.host}:{args.port} db={args.db}: {err}",
            file=sys.stderr,
        )
        return 1

    logical_key = f"{args.venue}:{AMOUNT_THRESHOLD_SUFFIX}"
    full_key = with_prefix(args.prefix, logical_key)
    symbol_filter = args.symbol.upper() if args.symbol else None

    rows: List[Dict[str, str]] = []
    total_fields = 0
    parse_failed = 0
    invalid_count = 0

    raw_map = client.hgetall(full_key)
    for raw_symbol in sorted(raw_map.keys(), key=lambda item: item.decode("utf-8", "ignore") if isinstance(item, bytes) else str(item)):
        total_fields += 1
        raw_value = raw_map[raw_symbol]
        symbol = (
            raw_symbol.decode("utf-8", "ignore")
            if isinstance(raw_symbol, bytes)
            else str(raw_symbol)
        ).strip()
        if not symbol:
            continue
        if symbol_filter and symbol.upper() != symbol_filter:
            continue
        raw_json = raw_value.decode("utf-8", "ignore") if isinstance(raw_value, bytes) else str(raw_value)

        try:
            payload = json.loads(raw_json)
        except Exception:
            parse_failed += 1
            continue

        entries: List[Dict[str, Any]] = []
        collect_threshold_entries(payload, entries)

        for entry in entries:
            entry_symbol = entry.get("symbol")
            row_symbol = str(entry_symbol).strip() if entry_symbol else symbol
            if not row_symbol:
                continue

            if symbol_filter and row_symbol.upper() != symbol_filter:
                continue

            medium = to_f64(entry.get("medium_notional_threshold"))
            large = to_f64(entry.get("large_notional_threshold"))
            valid = (
                medium is not None
                and large is not None
                and math.isfinite(medium)
                and math.isfinite(large)
                and medium > 0.0
                and large > 0.0
                and medium <= large
            )
            if not valid:
                invalid_count += 1
                if not args.show_invalid:
                    continue

            rows.append(
                {
                    "symbol": row_symbol,
                    "medium_notional": format_num(medium),
                    "large_notional": format_num(large),
                    "valid": "yes" if valid else "no",
                    "source_key": logical_key,
                }
            )

    rows.sort(key=lambda x: (x["symbol"].upper(), x["source_key"]))

    print(
        f"[INFO] venue={args.venue} redis_key='{logical_key}' fields={total_fields} "
        f"rows={len(rows)} parse_failed_fields={parse_failed} invalid_rows={invalid_count}"
    )

    if not rows:
        print("[INFO] no threshold rows found")
        return 0

    print_table(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
