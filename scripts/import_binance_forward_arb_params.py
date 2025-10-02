#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Import binance_forward_arb_params into Redis (HASH) from TOML files.

Sources
  - config/funding_rate_strategy.toml: import ALL values (flatten with dot notation),
    prefix keys with "funding_rate_strategy.".
  - config/pre_trade.toml: import ONLY [params] subset keys
      - max_pos_u
      - max_symbol_exposure_ratio
      - max_total_exposure_ratio
    prefixed as "pre_trade.params.".

Writes
  - Redis HASH key: binance_forward_arb_params (configurable with --write-key)
  - field = flattened param name, value = string representation (non-scalars as compact JSON)
  - By default, delete stale fields not present this run; use --no-clean to keep them.

CLI Examples
  - Default:
      python scripts/import_binance_forward_arb_params.py
  - Custom files and key:
      python scripts/import_binance_forward_arb_params.py \
        --funding-file config/funding_rate_strategy.toml \
        --pretrade-file config/pre_trade.toml \
        --write-key binance_forward_arb_params
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional


def try_import_redis():
    try:
        import redis  # type: ignore
        return redis
    except Exception:
        return None


def load_toml(path: str) -> Dict[str, Any]:
    # Prefer stdlib tomllib (3.11+), fallback to tomli
    try:
        import tomllib  # type: ignore
        with open(path, "rb") as f:
            return tomllib.load(f)
    except Exception:
        try:
            import tomli  # type: ignore
            with open(path, "rb") as f:
                return tomli.load(f)
        except Exception as e:
            raise RuntimeError(f"无法解析 TOML 文件: {path}，请安装 tomli 或使用 Python 3.11+ (tomllib)。{e}")


def flatten(prefix: str, data: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    def _walk(cur_prefix: str, obj: Any):
        if isinstance(obj, dict):
            for k, v in obj.items():
                _walk(f"{cur_prefix}.{k}" if cur_prefix else str(k), v)
        else:
            out[cur_prefix] = obj
    _walk(prefix, data)
    return out


def to_string(v: Any) -> str:
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, bool):
        return "true" if v else "false"
    if v is None:
        return "null"
    if isinstance(v, str):
        return v
    # list/dict and anything else -> compact JSON
    try:
        return json.dumps(v, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return str(v)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Import binance_forward_arb_params from TOML files into Redis HASH")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    p.add_argument("--write-key", default="binance_forward_arb_params")
    p.add_argument("--funding-file", default="config/funding_rate_strategy.toml")
    p.add_argument("--pretrade-file", default="config/pre_trade.toml")
    p.add_argument("--no-clean", action="store_true", help="Do not remove stale fields from HASH")
    p.add_argument(
        "--include-all-pretrade-params",
        action="store_true",
        help="Import all keys under [params] (default: only the 3 specified keys)",
    )
    return p.parse_args()


def build_kv(args) -> Dict[str, str]:
    # funding_rate_strategy: all values
    frs = load_toml(args.funding_file)
    frs_flat = flatten("funding_rate_strategy", frs)

    # pre_trade [params]: subset or all
    pre = load_toml(args.pretrade_file)
    params = pre.get("params", {}) if isinstance(pre, dict) else {}
    kv: Dict[str, str] = {}
    for k, v in frs_flat.items():
        kv[k] = to_string(v)

    if args.include_all_pretrade_params:
        for k, v in params.items():
            kv[f"pre_trade.params.{k}"] = to_string(v)
    else:
        pick = ["max_pos_u", "max_symbol_exposure_ratio", "max_total_exposure_ratio"]
        for k in pick:
            if k in params:
                kv[f"pre_trade.params.{k}"] = to_string(params[k])
    return kv


def write_hash(rds, key: str, kv: Dict[str, str], clean: bool) -> None:
    pipe = rds.pipeline(transaction=False)
    if kv:
        pipe.hset(key, mapping=kv)
    if clean:
        try:
            existing = rds.hkeys(key)
            existing_keys = set(k.decode('utf-8', 'ignore') if isinstance(k, bytes) else k for k in existing)
            stale = list(existing_keys - set(kv.keys()))
            if stale:
                pipe.hdel(key, *stale)
        except Exception:
            pass
    pipe.execute()


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("redis 包未安装，请使用 venv 安装或 --user 安装 redis。", file=sys.stderr)
        return 2
    try:
        kv = build_kv(args)
    except Exception as e:
        print(str(e), file=sys.stderr)
        return 2

    rds = redis.from_url(args.redis_url) if args.redis_url else redis.Redis(
        host=args.host, port=args.port, db=args.db, password=args.password
    )
    write_hash(rds, args.write_key, kv, clean=(not args.no_clean))
    print(f"已写入 {len(kv)} 个参数到 HASH {args.write_key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

