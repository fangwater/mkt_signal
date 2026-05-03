#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 cross per-symbol hedge_price_offset_limit 覆盖配置（从 Redis 读取，不修改）。

读取 Redis STRING(JSON)，按 mm 同款两套 key：
  - 合并键: <env>:<open>:<hedge>:hedge_price_offset_limits
            JSON {symbol: {hedge_price_offset_limit_lower, hedge_price_offset_limit_upper}}
  - 拆分键: <env>:<open>:<hedge>:hedge_price_offset_limit_upper / ..._lower
            JSON {symbol: f64}

合并键存在则直接展示；缺失则尝试用拆分键拼回（缺一个 symbol 整体丢弃）。
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def normalize_exchange(ex: str) -> str:
    ex = (ex or "").strip().lower()
    if ex == "okx":
        ex = "okex"
    return ex


def infer_pair_from_name(name: str) -> Optional[Tuple[str, str]]:
    n = (name or "").strip().lower()
    m = re.match(r"^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross([_-].*)?$", n)
    if not m:
        return None
    o = normalize_exchange(m.group(1))
    h = normalize_exchange(m.group(2))
    if o not in SUPPORTED_EXCHANGES or h not in SUPPORTED_EXCHANGES or o == h:
        return None
    return f"{o}-futures", f"{h}-futures"


def infer_env_name_from_cwd() -> Optional[str]:
    name = Path.cwd().name.strip().lower()
    return name or None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print cross hedge_price_offset_limit overrides from Redis")
    p.add_argument("--open-venue")
    p.add_argument("--hedge-venue")
    p.add_argument("--env-name")
    args = p.parse_args()

    if not args.open_venue and not args.hedge_venue:
        inferred = infer_pair_from_name(args.env_name or Path.cwd().name)
        if inferred:
            args.open_venue, args.hedge_venue = inferred

    if not args.open_venue or not args.hedge_venue:
        p.error("需要 --open-venue 与 --hedge-venue")

    args.open_venue = args.open_venue.lower()
    args.hedge_venue = args.hedge_venue.lower()

    if not args.env_name:
        args.env_name = infer_env_name_from_cwd()
    if not args.env_name:
        p.error("无法推断 env_name")
    return args


def make_combined_key(env: str, open_v: str, hedge_v: str) -> str:
    return f"{env}:{open_v}:{hedge_v}:hedge_price_offset_limits"


def make_upper_key(env: str, open_v: str, hedge_v: str) -> str:
    return f"{env}:{open_v}:{hedge_v}:hedge_price_offset_limit_upper"


def make_lower_key(env: str, open_v: str, hedge_v: str) -> str:
    return f"{env}:{open_v}:{hedge_v}:hedge_price_offset_limit_lower"


def _decode(raw: Any) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "ignore")
    return str(raw)


def _parse_split(raw: Any) -> Dict[str, float]:
    if raw is None:
        return {}
    try:
        decoded = json.loads(_decode(raw))
    except Exception:
        return {}
    if not isinstance(decoded, dict):
        return {}
    out: Dict[str, float] = {}
    for sym, v in decoded.items():
        try:
            out[str(sym).strip().upper()] = float(v)
        except Exception:
            continue
    return out


def print_value(rds, combined_key: str, upper_key: str, lower_key: str) -> None:
    print("\n📊 cross hedge_price_offset_limit 覆盖配置:")
    print("=" * 80)
    raw = rds.get(combined_key)
    rows: Dict[str, Dict[str, float]] = {}
    source = ""
    if raw is not None:
        try:
            decoded = json.loads(_decode(raw))
        except Exception:
            print(_decode(raw))
            return
        if isinstance(decoded, dict):
            for sym, v in decoded.items():
                if isinstance(v, dict):
                    try:
                        rows[str(sym).strip().upper()] = {
                            "lower": float(v.get("hedge_price_offset_limit_lower")),
                            "upper": float(v.get("hedge_price_offset_limit_upper")),
                        }
                    except Exception:
                        continue
        source = "combined"
    else:
        upper = _parse_split(rds.get(upper_key))
        lower = _parse_split(rds.get(lower_key))
        for sym in sorted(set(upper) | set(lower)):
            if sym in upper and sym in lower:
                rows[sym] = {"lower": lower[sym], "upper": upper[sym]}
        source = "split"
    if not rows:
        print(f"⚠️  无覆盖（合并键 '{combined_key}' 与拆分键均空/缺失）")
        return
    print(f"📦 source={source} symbols={len(rows)}")
    print(f"  {'SYMBOL':<24} {'LOWER':>14} {'UPPER':>14}")
    print(f"  {'-'*24} {'-'*14:>14} {'-'*14:>14}")
    for sym in sorted(rows):
        print(f"  {sym:<24} {rows[sym]['lower']:>14g} {rows[sym]['upper']:>14g}")


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装", file=sys.stderr)
        return 2

    combined_key = make_combined_key(args.env_name, args.open_venue, args.hedge_venue)
    upper_key = make_upper_key(args.env_name, args.open_venue, args.hedge_venue)
    lower_key = make_lower_key(args.env_name, args.open_venue, args.hedge_venue)
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)
    print(f"🔎 读取 cross hedge_price_offset_limit 覆盖配置")
    print(f"📁 env_name: {args.env_name}")
    print(f"🏷️ open: {args.open_venue}  hedge: {args.hedge_venue}")
    print(f"📍 Redis: 127.0.0.1:6379/0")
    print(f"🔑 combined: {combined_key}")
    print(f"🔑 upper   : {upper_key}")
    print(f"🔑 lower   : {lower_key}")
    print_value(rds, combined_key, upper_key, lower_key)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
