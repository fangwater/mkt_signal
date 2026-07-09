#!/usr/bin/env python3
"""Print Bitget position-tier pool and expanded query symbols."""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

from lib.bitget_tier_pool import DEFAULT_POOL_KEY, expand_pool_symbols, load_pool_from_redis


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read Redis Bitget position-tier env pool and print expanded symbols.",
    )
    parser.add_argument("--pool-key", default=DEFAULT_POOL_KEY, help=f"Redis key (default: {DEFAULT_POOL_KEY})")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument("--symbols-only", action="store_true", help="Print one Bitget symbol per line.")
    parser.add_argument("--show-keys", action="store_true", help="Show per-env Redis symbol-list key counts.")
    parser.add_argument("--redis-host", default=None)
    parser.add_argument("--redis-port", type=int, default=None)
    parser.add_argument("--redis-db", type=int, default=None)
    parser.add_argument("--redis-password", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rds = redis_client(args)
    specs = load_pool_from_redis(rds, args.pool_key)
    expanded, query_symbols = expand_pool_symbols(rds, specs)

    if args.symbols_only:
        for symbol in query_symbols:
            print(symbol)
        return 0

    if args.json:
        payload = {
            "pool_key": args.pool_key,
            "env_count": len(specs),
            "expanded_env_count": len(expanded),
            "query_symbol_count": len(query_symbols),
            "query_symbols": query_symbols,
            "envs": [
                {
                    "env_name": item.spec.env_name,
                    "mode": item.spec.mode,
                    "bitget_side": item.spec.bitget_side,
                    "open_venue": item.spec.open_venue,
                    "hedge_venue": item.spec.hedge_venue,
                    "asset_count": len(item.assets),
                    "bitget_symbol_count": len(item.bitget_symbols),
                    "bitget_symbols": list(item.bitget_symbols),
                    "key_counts": item.key_counts,
                }
                for item in expanded
            ],
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    print(
        f"[pool] key={args.pool_key} envs={len(specs)} "
        f"bitget_envs={len(expanded)} query_symbols={len(query_symbols)}"
    )
    for item in expanded:
        print(
            f"  {item.spec.env_name:<32} mode={item.spec.mode:<5} "
            f"bitget_side={item.spec.bitget_side:<5} assets={len(item.assets):<4} "
            f"symbols={len(item.bitget_symbols):<4} open={item.spec.open_venue} hedge={item.spec.hedge_venue}"
        )
        if args.show_keys:
            for key in item.keys:
                print(f"    {key}: {item.key_counts.get(key, 0)}")
    if query_symbols:
        print("[query_symbols]")
        print(",".join(query_symbols))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
