#!/usr/bin/env python3
"""Print materialized Bitget position-tier cache from Redis."""

from __future__ import annotations

import argparse
import json
import os
from typing import Any, List

from lib.bitget_tier_pool import DEFAULT_CACHE_KEY, bitget_symbol


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def redis_client(args: argparse.Namespace) -> Any:
    redis = try_import_redis()
    if redis is None:
        raise SystemExit("redis package is not installed; run pip install redis")
    host = args.redis_host or os.environ.get("REDIS_HOST", "127.0.0.1")
    port = args.redis_port if args.redis_port is not None else int(os.environ.get("REDIS_PORT", "6379"))
    db = args.redis_db if args.redis_db is not None else int(os.environ.get("REDIS_DB", "0"))
    password = args.redis_password if args.redis_password is not None else os.environ.get("REDIS_PASSWORD", "")
    return redis.Redis(host=host, port=port, db=db, password=password or None)


def parse_symbols(values: List[str]) -> List[str]:
    out = set()
    for value in values:
        for part in str(value or "").replace(",", " ").split():
            symbol = bitget_symbol(part)
            if symbol:
                out.add(symbol)
    return sorted(out)


def load_payload(rds: Any, key: str) -> dict:
    raw = rds.get(key)
    if not raw:
        raise SystemExit(f"Redis key not found: {key}")
    text = raw.decode("utf-8", "ignore") if isinstance(raw, (bytes, bytearray)) else str(raw)
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise SystemExit(f"Redis key is not a JSON object: {key}")
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print Redis Bitget position-tier cache.")
    parser.add_argument("--cache-key", default=DEFAULT_CACHE_KEY, help=f"Redis cache key (default: {DEFAULT_CACHE_KEY})")
    parser.add_argument("--symbol", action="append", default=[], help="Symbol/asset filter. Can be repeated.")
    parser.add_argument("--leverage", default="5", help="Leverage key to display, e.g. 5.")
    parser.add_argument("--json", action="store_true", help="Print raw cache JSON.")
    parser.add_argument("--show-tiers", action="store_true", help="Print tiers for selected symbols.")
    parser.add_argument("--redis-host", default=None)
    parser.add_argument("--redis-port", type=int, default=None)
    parser.add_argument("--redis-db", type=int, default=None)
    parser.add_argument("--redis-password", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rds = redis_client(args)
    payload = load_payload(rds, args.cache_key)
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    symbols = payload.get("symbols") or {}
    if not isinstance(symbols, dict):
        raise SystemExit("cache symbols field is not an object")
    filters = parse_symbols(args.symbol)
    selected = filters or sorted(symbols)
    print(
        f"[cache] key={args.cache_key} active={payload.get('active_symbol_count')} "
        f"cached={payload.get('cached_symbol_count')} missing={payload.get('missing_symbol_count')} "
        f"updated_at_ms={payload.get('updated_at_ms')}"
    )
    for symbol in selected:
        record = symbols.get(symbol)
        if not isinstance(record, dict):
            print(f"  {symbol:<16} missing")
            continue
        risk = record.get("risk_limit_by_leverage") or {}
        cap = risk.get(str(args.leverage)) if isinstance(risk, dict) else None
        print(
            f"  {symbol:<16} max_leverage={record.get('max_leverage')} "
            f"risk_limit@{args.leverage}x={cap or 'N/A'} tiers={len(record.get('tiers') or [])} "
            f"updated_at_ms={record.get('updated_at_ms')}"
        )
        if args.show_tiers:
            for row in record.get("tiers") or []:
                print(
                    f"    level={row.get('level')} start={row.get('startUnit')} "
                    f"end={row.get('endUnit')} maxLev={row.get('leverage')} mmr={row.get('keepMarginRate')}"
                )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
