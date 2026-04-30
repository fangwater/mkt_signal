#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 cross（跨所）Pre-Trade 风控参数（从 Redis 读取，futures-only）。

读取 Redis Hash:
  "<dir>:<open_venue>:<hedge_venue>:pre_trade_risk_params"

open/hedge 推断规则同 sync_cross_risk_params.py。
dir 推断优先级：--dir-prefix > --env-name > CWD 目录名
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from typing import Dict, List, Optional, Tuple


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]


def normalize_exchange(ex: str) -> str:
    ex = (ex or "").strip().lower()
    if ex == "okx":
        ex = "okex"
    return ex


def ensure_cross_venue(venue: str) -> str:
    v = (venue or "").strip().lower()
    if not re.match(r"^[a-z0-9]+-(futures|swap|perp|perpetual)$", v):
        raise SystemExit(f"cross venue 非法: {venue}")
    return v


def infer_pair_from_name(name: str) -> Optional[Tuple[str, str]]:
    n = (name or "").strip().lower()
    m = re.match(r"^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross([_-].*)?$", n)
    if not m:
        return None
    open_ex = normalize_exchange(m.group(1))
    hedge_ex = normalize_exchange(m.group(2))
    if open_ex not in SUPPORTED_EXCHANGES or hedge_ex not in SUPPORTED_EXCHANGES:
        return None
    return open_ex, hedge_ex


def infer_cross_venues_from_env_name(env_name: str) -> Optional[Tuple[str, str]]:
    pair = infer_pair_from_name(env_name)
    if not pair:
        return None
    if pair[0] == pair[1]:
        return None  # cross 要求 open/hedge 必须不同 exchange
    return f"{pair[0]}-futures", f"{pair[1]}-futures"


def infer_cross_venues_from_cwd() -> Optional[Tuple[str, str]]:
    from pathlib import Path

    return infer_cross_venues_from_env_name(Path.cwd().name)


def infer_dir_prefix_from_cwd() -> Optional[str]:
    from pathlib import Path

    name = Path.cwd().name.strip().lower()
    return name or None


def normalize_dir_prefix(prefix: Optional[str]) -> Optional[str]:
    if prefix is None:
        return None
    value = prefix.strip()
    if not value:
        return None
    return value.lower()


def resolve_dir_prefix(dir_prefix: Optional[str], env_name: Optional[str]) -> Optional[str]:
    if dir_prefix:
        return normalize_dir_prefix(dir_prefix)
    if env_name:
        return normalize_dir_prefix(env_name)
    return infer_dir_prefix_from_cwd()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print cross pre-trade risk params from Redis (futures-only)")
    p.add_argument("--open-venue", default=os.environ.get("OPEN_VENUE"))
    p.add_argument("--hedge-venue", default=os.environ.get("HEDGE_VENUE"))
    p.add_argument("--env-name", help="环境目录名（例如 okex-binance-cross-trade）")
    p.add_argument("--dir-prefix", help="Redis key 前缀中的 dir（默认使用 --env-name 或当前目录名）")
    args = p.parse_args()

    open_venue = args.open_venue
    hedge_venue = args.hedge_venue
    if not open_venue and not hedge_venue:
        inferred = infer_cross_venues_from_env_name(args.env_name) if args.env_name else infer_cross_venues_from_cwd()
        if inferred:
            open_venue, hedge_venue = inferred
            print(f"[INFO] 未提供 open/hedge，基于目录推断: open={open_venue}, hedge={hedge_venue}", file=sys.stderr)

    if not open_venue or not hedge_venue:
        p.error(
            "需要 --open-venue 与 --hedge-venue（futures-only），或使用 --env-name / 在目录名包含 '<open>-<hedge>-cross-...' 以自动推断"
        )

    args.open_venue = ensure_cross_venue(open_venue)
    args.hedge_venue = ensure_cross_venue(hedge_venue)
    if args.open_venue == args.hedge_venue:
        p.error(f"cross open/hedge venue 不能完全相同：open={args.open_venue} hedge={args.hedge_venue}")
    return args


PARAM_COMMENTS: Dict[str, str] = {
    "max_pos_u": "最大单币种持仓(USDT)",
    "max_symbol_exposure_ratio": "单币种最大敞口比例",
    "max_total_exposure_ratio": "总敞口比例",
    "max_leverage": "最大杠杆倍数",
    "max_pending_limit_orders": "最大挂单数",
    "arb_max_pending_limit_buy_orders": "套利买侧最大挂单数",
    "arb_max_pending_limit_sell_orders": "套利卖侧最大挂单数",
    "arb_open_order_rate_limit_per_min": "套利开仓60s频率上限",
    "arb_open_order_rate_limit_10s": "套利开仓10s频率上限",
    "arb_hedge_order_rate_limit_per_min": "套利对冲60s频率上限",
    "arb_hedge_order_rate_limit_10s": "套利对冲10s频率上限",
}

PARAM_ORDER = [
    "max_pos_u",
    "max_symbol_exposure_ratio",
    "max_total_exposure_ratio",
    "max_leverage",
    "max_pending_limit_orders",
    "arb_max_pending_limit_buy_orders",
    "arb_max_pending_limit_sell_orders",
    "arb_open_order_rate_limit_per_min",
    "arb_open_order_rate_limit_10s",
    "arb_hedge_order_rate_limit_per_min",
    "arb_hedge_order_rate_limit_10s",
]


def build_risk_params_key(open_venue: str, hedge_venue: str, dir_prefix: Optional[str]) -> str:
    resolved = normalize_dir_prefix(dir_prefix) or infer_dir_prefix_from_cwd()
    if resolved:
        return f"{resolved}:{open_venue}:{hedge_venue}:pre_trade_risk_params"
    return f"{open_venue}:{hedge_venue}:pre_trade_risk_params"


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


def decode_map(data: Dict) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in data.items():
        kk = k.decode("utf-8", "ignore") if isinstance(k, bytes) else str(k)
        vv = v.decode("utf-8", "ignore") if isinstance(v, bytes) else str(v)
        out[kk] = vv
    return out


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    dir_prefix = resolve_dir_prefix(args.dir_prefix, args.env_name)
    key = build_risk_params_key(args.open_venue, args.hedge_venue, dir_prefix)
    data = rds.hgetall(key)
    print("📍 Redis: 127.0.0.1:6379/0")
    print(f"📍 pretrade open={args.open_venue} hedge={args.hedge_venue}")
    print(f"🔑 Redis Hash Key: {key}")

    if not data:
        print("⚠️  未找到参数或 HASH 为空")
        print("💡 提示：先运行 sync_cross_risk_params.py 同步参数")
        print()
        return 0

    kv = decode_map(data)
    rows: List[List[str]] = []
    for k in PARAM_ORDER:
        if k in kv:
            rows.append([k, kv[k], PARAM_COMMENTS.get(k, "-")])
    for k in sorted(kv.keys()):
        if k not in PARAM_ORDER:
            rows.append([k, kv[k], "-"])
    print_three_line_table(["Parameter", "Value", "Comment"], rows)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
