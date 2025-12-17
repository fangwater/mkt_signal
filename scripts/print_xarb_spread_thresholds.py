#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 xarb（跨所）价差阈值配置（从 Redis 读取）。

读取 Redis Hash:
  `xarb_spread_thresholds_{open_venue}_{hedge_venue}`

推断规则：
  - open/hedge venue：优先 --open-venue/--hedge-venue，其次 --env-name，再其次 CWD（如 okex-binance-xarb-trade）
  - xarb 固定 futures-only：默认推断为 <exchange>-futures
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
    m = re.match(r"^([a-z0-9]+)[-_]([a-z0-9]+)[-_]xarb([_-].*)?$", n)
    if not m:
        return None
    open_ex = normalize_exchange(m.group(1))
    hedge_ex = normalize_exchange(m.group(2))
    if open_ex not in SUPPORTED_EXCHANGES or hedge_ex not in SUPPORTED_EXCHANGES:
        return None
    if open_ex == hedge_ex:
        return None
    return open_ex, hedge_ex


def infer_xarb_venues_from_env_name(env_name: str) -> Optional[Tuple[str, str]]:
    pair = infer_pair_from_name(env_name)
    if not pair:
        return None
    return f"{pair[0]}-futures", f"{pair[1]}-futures"


def infer_xarb_venues_from_cwd() -> Optional[Tuple[str, str]]:
    return infer_xarb_venues_from_env_name(Path.cwd().name)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print xarb spread thresholds from Redis")
    p.add_argument("--open-venue", help="open 侧 venue（例如 okex-futures）")
    p.add_argument("--hedge-venue", help="hedge 侧 venue（例如 binance-futures）")
    p.add_argument("--env-name", help="环境目录名（例如 okex-binance-xarb-trade）")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    args = p.parse_args()

    open_venue = args.open_venue
    hedge_venue = args.hedge_venue
    if not open_venue and not hedge_venue:
        inferred = (
            infer_xarb_venues_from_env_name(args.env_name)
            if args.env_name
            else infer_xarb_venues_from_cwd()
        )
        if inferred:
            open_venue, hedge_venue = inferred
            print(
                f"[INFO] 未提供 open/hedge，基于目录推断: open={open_venue}, hedge={hedge_venue}",
                file=sys.stderr,
            )
    if not open_venue or not hedge_venue:
        p.error(
            "需要 --open-venue 与 --hedge-venue，或使用 --env-name / 在目录名包含 '<open>-<hedge>-xarb-...' 以自动推断"
        )

    args.open_venue = open_venue.lower()
    args.hedge_venue = hedge_venue.lower()
    return args


SPREAD_THRESHOLD_MAPPING: Dict[str, str] = {
    "forward_open_mm": "spread_15",
    "forward_open_mt": "bidask_10",
    "forward_cancel_mm": "spread_20",
    "forward_cancel_mt": "bidask_15",
    "backward_open_mm": "spread_30",
    "backward_open_mt": "askbid_90",
    "backward_cancel_mm": "spread_25",
    "backward_cancel_mt": "askbid_85",
}

THRESHOLD_ORDER = list(SPREAD_THRESHOLD_MAPPING.keys())


def print_three_line_table(headers: List[str], rows: List[List[str]]) -> None:
    widths = [len(h) for h in headers]
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(cell))

    def fmt_row(values: List[str]) -> str:
        return "  ".join(values[i].ljust(widths[i]) for i in range(len(values)))

    header_line = fmt_row(headers)
    top_rule = "=" * len(header_line)
    mid_rule = "-" * len(header_line)
    bot_rule = "=" * len(header_line)

    print(top_rule)
    print(header_line)
    print(mid_rule)
    for r in rows:
        print(fmt_row(r))
    print(bot_rule)


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    key = f"xarb_spread_thresholds_{args.open_venue}_{args.hedge_venue}"
    rds = (
        redis.from_url(args.redis_url)
        if args.redis_url
        else redis.Redis(host=args.host, port=args.port, db=args.db, password=args.password)
    )

    print(f"📍 Redis: {args.host}:{args.port}/{args.db}")
    print(f"📊 xarb 价差阈值: {key}")
    print("-" * 80)

    print("🔧 映射配置（对所有 symbol 通用）:")
    rows = [[k, SPREAD_THRESHOLD_MAPPING.get(k, "-")] for k in THRESHOLD_ORDER]
    print_three_line_table(["operation", "percentile_reference"], rows)

    data = rds.hgetall(key)
    if not data:
        print("\n⚠️  Redis 中未找到阈值数据（HASH 为空或不存在）\n")
        return 0

    kv: Dict[str, str] = {}
    for k, v in data.items():
        kk = k.decode("utf-8", "ignore") if isinstance(k, bytes) else str(k)
        vv = v.decode("utf-8", "ignore") if isinstance(v, bytes) else str(v)
        kv[kk] = vv

    all_symbols = set()
    for field_key in kv:
        parts = field_key.split("_")
        if len(parts) >= 4:
            symbol = "_".join(parts[:-3])
            all_symbols.add(symbol)

    print("\n📈 统计:")
    print(f"   - 已同步 symbols: {len(all_symbols)} 个")
    print(f"   - 阈值字段总数: {len(kv)} 个")
    if all_symbols:
        print(f"   - Symbols: {', '.join(sorted(all_symbols))}")

    if all_symbols:
        print("\n📊 各 Symbol 具体阈值:")
        for symbol in sorted(all_symbols):
            rows = []
            for op in THRESHOLD_ORDER:
                rows.append([op, kv.get(f"{symbol}_{op}", "-")])
            print(f"\n🔹 {symbol}:")
            print_three_line_table(["operation", "threshold_value"], rows)

    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

