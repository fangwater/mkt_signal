#!/usr/bin/env python3
"""Sync the Bitget position-tier sidecar env pool into Redis.

The pool stores env metadata only. Workers/readers expand the current online
symbols from Redis every round and deduplicate Bitget symbols before querying
Bitget's public tier endpoint.

Examples:
  scripts/sync_bitget_position_tier_pool.py \
    --env-name bitget-intra-arb01 \
    --env-name bitget_fr_arb02 \
    --env-name bitget-gate-cross-arb01

  scripts/sync_bitget_position_tier_pool.py --env-set-file config/bitget_tier_pool.envs
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any, List

from lib.bitget_tier_pool import (
    DEFAULT_POOL_KEY,
    dedup_env_specs,
    dump_pool_json,
    read_env_names_from_file,
    resolve_env_spec,
)


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
        description="Overwrite Redis Bitget position-tier env pool.",
    )
    parser.add_argument("--env-name", action="append", default=[], help="Env name. Can be repeated.")
    parser.add_argument("--env-set-file", action="append", default=[], help="Text/JSON file containing env names.")
    parser.add_argument("--pool-key", default=DEFAULT_POOL_KEY, help=f"Redis key (default: {DEFAULT_POOL_KEY})")
    parser.add_argument("--home-dir", default=os.path.expanduser("~"), help="Base dir for env folders.")
    parser.add_argument("--no-env-sh", action="store_true", help="Do not source <home-dir>/<env>/env.sh.")
    parser.add_argument("--include-non-bitget", action="store_true", help="Keep envs that do not involve Bitget.")
    parser.add_argument("--dry-run", action="store_true", help="Print payload without writing Redis.")
    parser.add_argument("--redis-host", default=None)
    parser.add_argument("--redis-port", type=int, default=None)
    parser.add_argument("--redis-db", type=int, default=None)
    parser.add_argument("--redis-password", default=None)
    return parser.parse_args()


def collect_env_names(args: argparse.Namespace) -> List[str]:
    names: List[str] = []
    names.extend(args.env_name)
    for path in args.env_set_file:
        names.extend(read_env_names_from_file(path))
    return [name.strip() for name in names if name.strip()]


def main() -> int:
    args = parse_args()
    env_names = collect_env_names(args)
    if not env_names:
        raise SystemExit("no envs provided; pass --env-name or --env-set-file")

    specs = []
    for env_name in env_names:
        spec = resolve_env_spec(env_name, home_dir=args.home_dir, no_env_sh=args.no_env_sh)
        if not spec.has_bitget and not args.include_non_bitget:
            print(f"[skip] {spec.env_name}: no Bitget side", file=sys.stderr)
            continue
        specs.append(spec)
    specs = dedup_env_specs(specs)
    if not specs:
        raise SystemExit("no Bitget envs resolved")

    payload = dump_pool_json(specs)
    print(f"[pool] key={args.pool_key} envs={len(specs)}")
    for spec in specs:
        print(
            f"  {spec.env_name:<32} mode={spec.mode:<5} bitget_side={spec.bitget_side:<5} "
            f"open={spec.open_venue} hedge={spec.hedge_venue}"
        )

    if args.dry_run:
        print(payload)
        return 0

    rds = redis_client(args)
    rds.set(args.pool_key, payload)
    print(f"[redis] SET {args.pool_key} bytes={len(payload)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
