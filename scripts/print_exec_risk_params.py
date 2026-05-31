#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 Exec Pre-Trade 风控参数（从 Redis 读取，不修改）。

读取 Redis Hash:
  `<dir>:<open>:<hedge>:pre_trade_risk_params`

同时读取 per-symbol max_pos_u 覆盖 key:
  `<env>:<venue>:exec:max_pos_u`

示例：
  cd ~/binance_exec_trade
  python scripts/print_exec_risk_params.py
  python scripts/print_exec_risk_params.py --exec-venue binance-futures
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


EXCHANGE_DEFAULTS = {
    "binance": "binance-futures",
    "okex": "okex-futures",
    "bybit": "bybit-futures",
    "bitget": "bitget-futures",
    "gate": "gate-futures",
}


def infer_exec_venue_from_cwd() -> Optional[str]:
    name = Path.cwd().name.lower()
    candidates = [name]
    if "_" in name:
        candidates.append(name.split("_", 1)[0])
    for cand in candidates:
        for exchange, venue in EXCHANGE_DEFAULTS.items():
            if cand.startswith(exchange):
                return venue
    return None


def infer_env_name_from_cwd() -> Optional[str]:
    name = Path.cwd().name.strip().lower()
    return name or None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print Exec pre-trade risk params from Redis")
    p.add_argument("--exec-venue", default=os.environ.get("EXEC_VENUE"))
    p.add_argument("--open-venue", default=os.environ.get("OPEN_VENUE"))
    p.add_argument("--hedge-venue", default=os.environ.get("HEDGE_VENUE"))
    p.add_argument("--env-name", default=os.environ.get("ENV_NAME"))
    args = p.parse_args()

    exec_venue = args.exec_venue
    if not exec_venue:
        exec_venue = infer_exec_venue_from_cwd()
        if exec_venue:
            print(f"[INFO] 未提供 exec venue，基于目录推断: exec={exec_venue}")

    if not exec_venue:
        p.error("需要 --exec-venue，或在目录名包含 <exchange> 前缀（如 binance_exec_trade）的 exec 目录运行")

    args.exec_venue = exec_venue.lower()
    args.open_venue = (args.open_venue or args.exec_venue).lower()
    args.hedge_venue = (args.hedge_venue or args.exec_venue).lower()

    if not args.env_name:
        args.env_name = infer_env_name_from_cwd()
    if not args.env_name:
        p.error("无法推断 env_name，请通过 --env-name 显式提供")
    args.env_name = args.env_name.strip().lower()
    return args


PARAM_COMMENTS: Dict[str, str] = {
    "max_pos_u": "默认单币最大持仓(USDT)，可被 exec:max_pos_u 覆盖",
    "max_leverage": "最大杠杆倍数",
    "max_pending_limit_orders": "最大 live exec 限价挂单数",
    "exec_order_rate_limit_per_min": "Exec 60s 下单频率上限（0=关闭）",
    "exec_order_rate_limit_10s": "Exec 10s 下单频率上限（0=关闭）",
    "exec_max_position_imbalance_ratio": "abs(long_u-short_u)/(long_u+short_u)，0=关闭",
}


PARAM_ORDER = [
    "max_pos_u",
    "max_leverage",
    "max_pending_limit_orders",
    "exec_order_rate_limit_per_min",
    "exec_order_rate_limit_10s",
    "exec_max_position_imbalance_ratio",
]


def build_risk_params_key(env_name: str, open_venue: str, hedge_venue: str) -> str:
    return f"{env_name}:{open_venue}:{hedge_venue}:pre_trade_risk_params"


def build_exec_max_pos_u_key(env_name: str, exec_venue: str) -> str:
    return f"{env_name}:{exec_venue}:exec:max_pos_u"


def print_three_line_table(headers: List[str], rows: List[List[str]]) -> None:
    ncols = len(headers)
    widths = [0] * ncols
    for i, h in enumerate(headers):
        widths[i] = max(widths[i], len(h))
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(cell))

    def fmt_row(values: List[str]) -> str:
        return "  ".join(values[i].ljust(widths[i]) for i in range(ncols))

    header_line = fmt_row(headers)
    print("=" * len(header_line))
    print(header_line)
    print("-" * len(header_line))
    for r in rows:
        print(fmt_row(r))
    print("=" * len(header_line))


def decode_hash(data) -> Dict[str, str]:
    kv: Dict[str, str] = {}
    for k, v in data.items():
        kk = k.decode("utf-8", "ignore") if isinstance(k, bytes) else str(k)
        vv = v.decode("utf-8", "ignore") if isinstance(v, bytes) else str(v)
        kv[kk] = vv
    return kv


def print_risk_params(rds, env_name: str, open_venue: str, hedge_venue: str) -> None:
    print("\n📊 Exec pre_trade 风控参数:")
    print("-" * 80)

    key = build_risk_params_key(env_name, open_venue, hedge_venue)
    data = rds.hgetall(key)
    print(f"🔑 Redis Hash Key: {key}")

    if not data:
        print("⚠️  未找到参数或 HASH 为空")
        print("\n💡 提示：请先运行 sync_exec_risk_params.py 同步参数")
        return

    kv = decode_hash(data)
    headers = ["Parameter", "Value", "Comment"]
    rows: List[List[str]] = []
    for param_key in PARAM_ORDER:
        if param_key in kv:
            rows.append([param_key, kv[param_key], PARAM_COMMENTS.get(param_key, "-")])
    rows.extend([k, kv[k], "-"] for k in sorted(kv.keys()) if k not in PARAM_ORDER)
    print_three_line_table(headers, rows)


def print_exec_max_pos_u_overrides(rds, env_name: str, exec_venue: str) -> None:
    key = build_exec_max_pos_u_key(env_name, exec_venue)
    print("\n📊 Exec per-symbol max_pos_u 覆盖配置:")
    print("-" * 80)
    print(f"🔑 Redis String Key: {key}")
    raw = rds.get(key)
    if raw is None:
        print("⚠️  STRING 为空或不存在；当前使用 hash 里的 max_pos_u 作为默认单币限制")
        return

    text = raw.decode("utf-8", "ignore") if isinstance(raw, bytes) else str(raw)
    try:
        mapping = json.loads(text)
    except Exception:
        print(text)
        return
    if not isinstance(mapping, dict) or not mapping:
        print("{}")
        return

    rows: List[List[str]] = []
    for symbol in sorted(mapping.keys()):
        rows.append([str(symbol), f"{float(mapping[symbol]):g}"])
    print_three_line_table(["Symbol", "max_pos_u"], rows)


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    print("📍 Redis: 127.0.0.1:6379/0")
    print(f"📁 env_name: {args.env_name}")
    print(f"📍 exec={args.exec_venue} open={args.open_venue} hedge={args.hedge_venue}")

    print_risk_params(rds, args.env_name, args.open_venue, args.hedge_venue)
    print_exec_max_pos_u_overrides(rds, args.env_name, args.exec_venue)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
