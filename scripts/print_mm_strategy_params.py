#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 MM 策略参数（从 Redis 读取）。

读取 Redis Hash:
  mm_strategy_params_{venue}
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Dict, Optional


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


def infer_exchange_from_name(name: str) -> Optional[str]:
    n = (name or "").strip().lower()
    for ex in ["binance", "okex", "okx", "bybit", "bitget", "gate"]:
        if re.search(rf"(^|[^a-z0-9]){ex}([^a-z0-9]|$)", n):
            return normalize_exchange(ex)
    return None


def infer_exchange_from_cwd() -> Optional[str]:
    return infer_exchange_from_name(Path.cwd().name)


def normalize_venue(raw: str) -> Optional[str]:
    if not raw:
        return None
    v = raw.strip().lower().replace("_", "-")
    if "-" not in v:
        v = f"{v}-futures"
    return v


def resolve_venue(args: argparse.Namespace) -> Optional[str]:
    if args.venue:
        return normalize_venue(args.venue)
    if args.env_name:
        ex = infer_exchange_from_name(args.env_name)
        if ex:
            return f"{ex}-futures"
    ex = infer_exchange_from_cwd()
    if ex:
        return f"{ex}-futures"
    return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print mm strategy params from Redis")
    p.add_argument("--venue", help="MM venue（例如 binance-futures）")
    p.add_argument("--env-name", help="环境目录名（例如 binance_mm_beta）")
    return p.parse_args()


PARAM_COMMENTS: Dict[str, str] = {
    "order_amount": "单笔下单量(USDT)",
    "open_price_offsets": "开仓挂单档位(JSON数组)",
    "open_order_timeout": "开仓订单超时(秒)",
    "next_query_delay_ms": "对冲 query 触发间隔(ms)",
    "hedge_aggressive_seq_threshold": "对冲激进阈值(request_seq>=该值时不偏移，但仍为maker限价单)",
    "max_hedge_price_pct_change": "对冲价格最大变动阈值(%)，范围1-99，可为小数，超过则强制 taker",
    "signal_cooldown": "信号冷却时间(秒)",
}


def print_params(rds, key: str) -> None:
    print("\n📊 mm 策略参数配置:")
    print("=" * 80)
    data = rds.hgetall(key)
    if not data:
        print(f"⚠️  HASH '{key}' 为空或不存在")
        return

    for raw_k, raw_v in sorted(data.items(), key=lambda kv: str(kv[0])):
        k = raw_k.decode("utf-8", "ignore") if isinstance(raw_k, bytes) else str(raw_k)
        v = raw_v.decode("utf-8", "ignore") if isinstance(raw_v, bytes) else str(raw_v)
        comment = PARAM_COMMENTS.get(k, "")
        if comment:
            print(f"  {k:28} {v:>12}  # {comment}")
        else:
            print(f"  {k:28} {v:>12}")


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    venue = resolve_venue(args)
    if not venue:
        print("❌ 需要 --venue，或从 --env-name / CWD 推断 exchange", file=sys.stderr)
        return 1

    key = f"mm_strategy_params_{venue}"
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    print(f"🔎 读取 mm 策略参数: {key}")
    print("📍 Redis: 127.0.0.1:6379/0")

    print_params(rds, key)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
