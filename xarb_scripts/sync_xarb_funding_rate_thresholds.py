#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将 xarb（跨所）资金费率阈值同步到 Redis 并打印。

写入 Redis Hash:
  `xarb_funding_rate_thresholds_{open_venue}_{hedge_venue}`

说明:
  - xarb 固定 futures-only：open/hedge venue 默认推断为 <exchange>-futures。
  - 可通过 --open-venue/--hedge-venue 指定；也可通过 --env-name 或 CWD 目录名推断：
      okex-binance-xarb-trade -> open=okex-futures hedge=binance-futures
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]

# 阈值配置（与 fr 版本一致，xarb 可按需调整）
FUNDING_RATE_THRESHOLDS: Dict[str, str] = {
    "8h_forward_open": "0.0010",
    "8h_forward_close": "0.0005",
    "8h_backward_open": "-0.0010",
    "8h_backward_close": "-0.0005",
    "4h_forward_open": "0.0010",
    "4h_forward_close": "0.0005",
    "4h_backward_open": "-0.0010",
    "4h_backward_close": "-0.0005",
}

THRESHOLD_COMMENTS: Dict[str, str] = {
    "8h_forward_open": "8h 正套开仓(预测费率>阈值)",
    "8h_forward_close": "8h 正套平仓(预测费率<阈值)",
    "8h_backward_open": "8h 反套开仓(预测费率<阈值)",
    "8h_backward_close": "8h 反套平仓(预测费率>阈值)",
    "4h_forward_open": "4h 正套开仓(预测费率>阈值)",
    "4h_forward_close": "4h 正套平仓(预测费率<阈值)",
    "4h_backward_open": "4h 反套开仓(预测费率<阈值)",
    "4h_backward_close": "4h 反套平仓(预测费率>阈值)",
}

THRESHOLD_ORDER = [
    "8h_forward_open",
    "8h_forward_close",
    "8h_backward_open",
    "8h_backward_close",
    "4h_forward_open",
    "4h_forward_close",
    "4h_backward_open",
    "4h_backward_close",
]


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
    p = argparse.ArgumentParser(description="Sync xarb funding rate thresholds to Redis")
    p.add_argument("--open-venue", help="open 侧 venue（例如 okex-futures）")
    p.add_argument("--hedge-venue", help="hedge 侧 venue（例如 binance-futures）")
    p.add_argument("--env-name", help="环境目录名（例如 okex-binance-xarb-trade）")
    return p.parse_args()


def resolve_venues(args: argparse.Namespace) -> Optional[Tuple[str, str]]:
    if args.open_venue and args.hedge_venue:
        return args.open_venue.lower(), args.hedge_venue.lower()
    if args.env_name:
        inferred = infer_xarb_venues_from_env_name(args.env_name)
        if inferred:
            return inferred[0], inferred[1]
    inferred = infer_xarb_venues_from_cwd()
    if inferred:
        return inferred[0], inferred[1]
    return None


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

    venues = resolve_venues(args)
    if not venues:
        print(
            "❌ 需要 --open-venue/--hedge-venue 或 --env-name，或在目录名包含 '<open>-<hedge>-xarb-...' 以自动推断",
            file=sys.stderr,
        )
        return 2

    open_venue, hedge_venue = venues
    key = f"xarb_funding_rate_thresholds_{open_venue}_{hedge_venue}"

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    rds.hset(key, mapping=FUNDING_RATE_THRESHOLDS)
    print(f"✅ 已写入 {len(FUNDING_RATE_THRESHOLDS)} 个资金费率阈值到 HASH '{key}'")
    print("📍 Redis: 127.0.0.1:6379/0")

    rows: List[List[str]] = []
    for k in THRESHOLD_ORDER:
        v = FUNDING_RATE_THRESHOLDS.get(k, "")
        rows.append([k, v, THRESHOLD_COMMENTS.get(k, "")])
    print_three_line_table(["key", "value", "comment"], rows)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
