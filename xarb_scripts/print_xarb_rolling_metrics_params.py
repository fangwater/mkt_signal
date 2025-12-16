#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 Redis HASH `rolling_metrics_params_{open_venue}_{hedge_venue}`（xarb futures-only）。

示例：
  python xarb_scripts/print_xarb_rolling_metrics_params.py --open-venue okex-futures --hedge-venue binance-futures
  # 也可不带参数，脚本会基于当前目录名推断（形如 okex-binance-xarb-trade -> okex-futures/binance-futures）
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

ALLOWED_EXCHANGES = {"binance", "okex", "bybit", "bitget", "gate"}
EXCHANGE_ALIASES = {"okx": "okex"}


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def normalize_exchange(exchange: str) -> str:
    ex = exchange.strip().lower()
    return EXCHANGE_ALIASES.get(ex, ex)


def infer_exchanges_from_cwd() -> Optional[Tuple[str, str]]:
    name = Path.cwd().name.lower()
    if "xarb" not in name:
        return None
    tokens = [t for t in re.split(r"[^a-z0-9]+", name) if t]
    found = []
    for tok in tokens:
        ex = normalize_exchange(tok)
        if ex in ALLOWED_EXCHANGES and ex not in found:
            found.append(ex)
    if len(found) >= 2 and found[0] != found[1]:
        return found[0], found[1]
    return None


def ensure_futures_venue(venue: str) -> str:
    v = venue.strip().lower()
    if not v.endswith("-futures"):
        raise SystemExit(f"xarb 只支持 futures 资产类型，venue 必须以 -futures 结尾: {venue}")
    return v


def resolve_venues(
    open_venue: str | None,
    hedge_venue: str | None,
) -> Tuple[str, str]:
    if open_venue or hedge_venue:
        if not open_venue or not hedge_venue:
            raise SystemExit("同时提供 --open-venue 与 --hedge-venue，或都不提供")
        open_v = ensure_futures_venue(open_venue)
        hedge_v = ensure_futures_venue(hedge_venue)
        if open_v == hedge_v:
            raise SystemExit(f"xarb 需要跨所：open={open_v} hedge={hedge_v}")
        return open_v, hedge_v

    inferred = infer_exchanges_from_cwd()
    if not inferred:
        raise SystemExit(
            "需要提供 --open-venue/--hedge-venue，或在目录名包含 <open>-<hedge>-xarb-... 以自动推断"
        )
    return f"{inferred[0]}-futures", f"{inferred[1]}-futures"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Print Redis hash rolling_metrics_params_{open_venue}_{hedge_venue} as JSON（xarb futures-only；可省略参数，默认按目录名推断）"
    )
    p.add_argument("--open-venue", help="open 侧 venue（如 okex-futures）")
    p.add_argument("--hedge-venue", help="hedge 侧 venue（如 binance-futures）")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    p.add_argument("--prefix", help="只打印指定前缀的参数，例如 --prefix bidask_")
    args = p.parse_args()

    open_venue, hedge_venue = resolve_venues(
        open_venue=args.open_venue,
        hedge_venue=args.hedge_venue,
    )
    args.open_venue = open_venue
    args.hedge_venue = hedge_venue
    return args


def read_hash(rds, key: str) -> Dict[str, str]:
    data = rds.hgetall(key)

    def decode(obj: object) -> str:
        return obj.decode("utf-8", "ignore") if isinstance(obj, bytes) else str(obj)

    return {decode(k): decode(v) for k, v in data.items()}


def decode_value(raw: str) -> Any:
    try:
        return json.loads(raw)
    except Exception:
        return raw


def build_json_output(kv: Dict[str, str], prefix: str | None) -> Dict[str, Any]:
    result: Dict[str, Any] = {}

    general: Dict[str, Any] = {
        key: decode_value(value)
        for key, value in sorted(kv.items())
        if key != "factors"
        and not key.endswith("_lower_quantile")
        and not key.endswith("_upper_quantile")
    }
    factors_raw = kv.get("factors")
    if not factors_raw:
        result["general"] = general
        return result

    try:
        factors = json.loads(factors_raw)
    except json.JSONDecodeError as exc:
        general["factors_error"] = f"factors 解析失败: {exc}"
    else:
        if not isinstance(factors, dict):
            general["factors_error"] = "factors 需为对象"
        else:
            if prefix:
                factors = {name: cfg for name, cfg in factors.items() if name.startswith(prefix)}
            general["factors"] = factors

    result["general"] = general
    return result


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("redis 包未安装，请先 `pip install redis`。", file=sys.stderr)
        return 2
    rds = redis.from_url(args.redis_url) if args.redis_url else redis.Redis(
        host=args.host, port=args.port, db=args.db, password=args.password
    )

    open_venue = args.open_venue.strip()
    hedge_venue = args.hedge_venue.strip()
    if not open_venue or not hedge_venue:
        print("open-venue 和 hedge-venue 均不能为空。", file=sys.stderr)
        return 1
    key = f"rolling_metrics_params_{open_venue}_{hedge_venue}"
    print(f"📍 Reading from Redis hash: {key}", file=sys.stderr)
    print(f"📍 Redis: {args.host}:{args.port}/{args.db}", file=sys.stderr)
    if args.prefix:
        print(f"📍 Filter prefix: {args.prefix}", file=sys.stderr)
    print("", file=sys.stderr)

    kv = read_hash(rds, key)
    if not kv:
        print(f"⚠️  未找到参数或 HASH '{key}' 为空。", file=sys.stderr)
        return 0

    data = build_json_output(kv, args.prefix)
    print(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
