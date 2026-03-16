#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 xarb（跨所）资金费率阈值（从 Redis 读取）。

读取 Redis Hash:
  `xarb_funding_rate_thresholds_{open_venue}_{hedge_venue}`

推断规则：
  - open/hedge venue：优先 --open-venue/--hedge-venue，其次 --env-name，再其次 CWD（如 okex-binance-xarb-trade）
  - xarb 固定 futures-only：默认推断为 <exchange>-futures
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]

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
    return open_ex, hedge_ex


def infer_xarb_venues_from_env_name(env_name: str) -> Optional[Tuple[str, str]]:
    pair = infer_pair_from_name(env_name)
    if not pair:
        return None
    if pair[0] == pair[1]:
        return f"{pair[0]}-margin", f"{pair[1]}-futures"
    return f"{pair[0]}-futures", f"{pair[1]}-futures"


def infer_xarb_venues_from_cwd() -> Optional[Tuple[str, str]]:
    return infer_xarb_venues_from_env_name(Path.cwd().name)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print xarb funding rate thresholds from Redis")
    p.add_argument("--open-venue", help="open 侧 venue（例如 okex-futures）")
    p.add_argument("--hedge-venue", help="hedge 侧 venue（例如 binance-futures）")
    p.add_argument("--env-name", help="环境目录名（例如 okex-binance-xarb-trade）")
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

    key = f"xarb_funding_rate_thresholds_{args.open_venue}_{args.hedge_venue}"
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    data = rds.hgetall(key)
    print("📍 Redis: 127.0.0.1:6379/0")
    print(f"📊 xarb 资金费率阈值: {key}")
    print("-" * 80)
    if not data:
        print(f"⚠️  HASH '{key}' 为空或不存在\n")
        return 0

    kv: Dict[str, str] = {}
    for k, v in data.items():
        kk = k.decode("utf-8", "ignore") if isinstance(k, bytes) else str(k)
        vv = v.decode("utf-8", "ignore") if isinstance(v, bytes) else str(v)
        kv[kk] = vv

    rows: List[List[str]] = []
    for k in THRESHOLD_ORDER:
        if k in kv:
            rows.append([k, kv[k], THRESHOLD_COMMENTS.get(k, "")])
    rows.extend([[k, kv[k], "-"] for k in sorted(kv.keys()) if k not in THRESHOLD_ORDER])
    print_three_line_table(["key", "value", "comment"], rows)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
