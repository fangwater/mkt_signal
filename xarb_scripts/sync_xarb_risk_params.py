#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将 xarb（跨所）Pre-Trade 风控参数同步到 Redis 并打印（futures-only）。

注意：
  - Rust pre_trade 固定读取 hash key: `pre_trade_risk_params`，并通过 Redis prefix 隔离实例。
  - pre_trade 的 prefix 规则："<open_venue>:<hedge_venue>:"
  - 因此这里写入的 Redis Hash key 为：
      "<open_venue>:<hedge_venue>:pre_trade_risk_params"

推断规则（优先级从高到低）：
  1) --open-venue/--hedge-venue（必须为 *-futures）
  2) 环境变量 OPEN_VENUE/HEDGE_VENUE（deploy_setup_env_xarb.sh 生成的 env.sh 会设置）
  3) --env-name（例如 okex-binance-xarb-trade）
  4) CWD 目录名（例如 okex-binance-xarb-trade）
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


def ensure_futures_venue(venue: str) -> str:
    v = (venue or "").strip().lower()
    if not v.endswith("-futures"):
        raise SystemExit(f"xarb 只支持 futures：venue 必须以 -futures 结尾: {venue}")
    return v


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
    from pathlib import Path

    return infer_xarb_venues_from_env_name(Path.cwd().name)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync xarb pre-trade risk params to Redis (futures-only)")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    p.add_argument("--open-venue", default=os.environ.get("OPEN_VENUE"))
    p.add_argument("--hedge-venue", default=os.environ.get("HEDGE_VENUE"))
    p.add_argument("--env-name", help="环境目录名（例如 okex-binance-xarb-trade）")
    args = p.parse_args()

    open_venue = args.open_venue
    hedge_venue = args.hedge_venue
    if not open_venue and not hedge_venue:
        inferred = infer_xarb_venues_from_env_name(args.env_name) if args.env_name else infer_xarb_venues_from_cwd()
        if inferred:
            open_venue, hedge_venue = inferred
            print(f"[INFO] 未提供 open/hedge，基于目录推断: open={open_venue}, hedge={hedge_venue}")

    if not open_venue or not hedge_venue:
        p.error(
            "需要 --open-venue 与 --hedge-venue（futures-only），或使用 --env-name / 在目录名包含 '<open>-<hedge>-xarb-...' 以自动推断"
        )

    args.open_venue = ensure_futures_venue(open_venue)
    args.hedge_venue = ensure_futures_venue(hedge_venue)
    if args.open_venue == args.hedge_venue:
        p.error(f"xarb 需要跨所：open={args.open_venue} hedge={args.hedge_venue}")
    return args


RISK_PARAMS = {
    "max_pos_u": "10000.0",
    "max_symbol_exposure_ratio": "0.015",
    "max_total_exposure_ratio": "0.01",
    "max_leverage": "1.75",
    "max_pending_limit_orders": "10",
}

PARAM_COMMENTS: Dict[str, str] = {
    "max_pos_u": "最大单币种持仓(USDT)",
    "max_symbol_exposure_ratio": "单币种最大敞口比例",
    "max_total_exposure_ratio": "总敞口比例",
    "max_leverage": "最大杠杆倍数",
    "max_pending_limit_orders": "最大挂单数",
}


def build_risk_params_key(open_venue: str, hedge_venue: str) -> str:
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

    rds = redis.from_url(args.redis_url) if args.redis_url else redis.Redis(
        host=args.host, port=args.port, db=args.db, password=args.password
    )

    key = build_risk_params_key(args.open_venue, args.hedge_venue)
    rds.hset(key, mapping=RISK_PARAMS)
    print(f"✅ 已写入 {len(RISK_PARAMS)} 个参数到 HASH '{key}' (primary)")
    print(f"📍 Redis: {args.host}:{args.port}/{args.db}")
    print(f"📍 pretrade open={args.open_venue} hedge={args.hedge_venue}")

    data = rds.hgetall(key)
    kv = decode_map(data)
    rows: List[List[str]] = []
    for k in RISK_PARAMS.keys():
        rows.append([k, kv.get(k, "-"), PARAM_COMMENTS.get(k, "-")])
    rows.extend([[k, kv[k], "-"] for k in sorted(kv.keys()) if k not in RISK_PARAMS])
    print_three_line_table(["Parameter", "Value", "Comment"], rows)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
