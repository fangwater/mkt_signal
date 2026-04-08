#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将 MM Pre-Trade 风控参数同步到 Redis 并打印。

写入 Redis Hash:
  `<dir>:<open>:<hedge>:pre_trade_risk_params` - 风控参数（max_pos_u, max_leverage 等）

MM 默认 open/hedge 都是 futures（同一 venue）。

示例：
  cd ~/binance_mm_beta
  python scripts/sync_mm_risk_params.py
"""

from __future__ import annotations

import argparse
import sys
from typing import Dict, List


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


EXCHANGE_DEFAULTS = {
    "binance": ("binance-futures", "binance-futures"),
    "okex": ("okex-futures", "okex-futures"),
    "bybit": ("bybit-futures", "bybit-futures"),
    "bitget": ("bitget-futures", "bitget-futures"),
    "gate": ("gate-futures", "gate-futures"),
}


def infer_venues_from_cwd():
    """从当前目录名推断 open/hedge（如 binance_mm_beta -> binance-futures/binance-futures）"""
    from pathlib import Path

    name = Path.cwd().name.lower()
    candidates = [name]
    if "_" in name:
        candidates.append(name.split("_", 1)[0])
    for cand in candidates:
        for ex, pair in EXCHANGE_DEFAULTS.items():
            if cand.startswith(ex):
                return pair
    return None


def infer_dir_prefix_from_cwd() -> str | None:
    from pathlib import Path

    name = Path.cwd().name.strip().lower()
    return name or None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sync MM pre-trade risk params to Redis (venues are inferred from current directory)"
    )
    args = p.parse_args()

    inferred = infer_venues_from_cwd()
    if not inferred:
        p.error(
            "无法从当前目录推断 open/hedge venue。请在目录名包含 <exchange> 前缀（如 binance_mm_beta）的 MM 目录运行"
        )

    open_venue, hedge_venue = inferred
    print(f"[INFO] 基于目录推断: open={open_venue}, hedge={hedge_venue}")

    args.open_venue = open_venue
    args.hedge_venue = hedge_venue
    return args


# ========== 风控参数配置 ==========

# Hash key: pre_trade_risk_params
RISK_PARAMS = {
    "max_pos_u": "100000.0",  # 最大单币种持仓 (USDT)
    "max_symbol_exposure_ratio": "0.015",  # 单币种最大敞口比例（0.0-1.0）
    "max_total_exposure_ratio": "0.01",  # 总敞口比例（0.0-1.0）
    "max_leverage": "1.75",  # 最大杠杆倍数（>0）
    "max_pending_limit_orders": "10",  # 最大挂单数（>=0）
    "order_rate_limit_per_min": "0",  # 开仓侧每分钟下单次数限制（0=关闭）
    "order_rate_limit_10s": "0",  # 开仓侧10秒下单次数限制（0=关闭）
    "hedge_order_rate_limit_per_min": "0",  # 对冲侧每分钟下单次数限制（0=关闭）
    "hedge_order_rate_limit_10s": "0",  # 对冲侧10秒下单次数限制（0=关闭）
}


PARAM_COMMENTS: Dict[str, str] = {
    "max_pos_u": "最大单币种持仓(USDT)",
    "max_symbol_exposure_ratio": "单币种最大敞口比例",
    "max_total_exposure_ratio": "总敞口比例",
    "max_leverage": "最大杠杆倍数",
    "max_pending_limit_orders": "最大挂单数",
    "order_rate_limit_per_min": "Open order rate limit/Min（开仓侧独立额度，0=关闭）",
    "order_rate_limit_10s": "Open order rate limit 10s（开仓侧独立额度，0=关闭）",
    "hedge_order_rate_limit_per_min": "Hedge order rate limit/Min（对冲侧独立额度，0=关闭）",
    "hedge_order_rate_limit_10s": "Hedge order rate limit 10s（对冲侧独立额度，0=关闭）",
}


def build_risk_params_key(open_venue: str | None, hedge_venue: str | None) -> str:
    if not open_venue or not hedge_venue:
        raise ValueError("missing open/hedge venue")
    dir_prefix = infer_dir_prefix_from_cwd()
    if dir_prefix:
        return f"{dir_prefix}:{open_venue}:{hedge_venue}:pre_trade_risk_params"
    return f"{open_venue}:{hedge_venue}:pre_trade_risk_params"


def sync_risk_params(rds, open_venue: str | None, hedge_venue: str | None) -> int:
    key = build_risk_params_key(open_venue, hedge_venue)
    rds.hset(key, mapping=RISK_PARAMS)
    print(f"✅ 已写入 {len(RISK_PARAMS)} 个参数到 HASH '{key}'")
    return len(RISK_PARAMS)


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


def print_risk_params(rds, open_venue: str | None, hedge_venue: str | None) -> None:
    print("\n📊 MM pre_trade 风控参数:")
    print("-" * 80)

    key = build_risk_params_key(open_venue, hedge_venue)
    print(f"🔑 Redis Hash Key: {key}")
    data = rds.hgetall(key)

    if not data:
        print("⚠️  未找到参数或 HASH 为空")
        return

    kv: Dict[str, str] = {}
    for k, v in data.items():
        kk = k.decode("utf-8", "ignore") if isinstance(k, bytes) else str(k)
        vv = v.decode("utf-8", "ignore") if isinstance(v, bytes) else str(v)
        kv[kk] = vv

    headers = ["Parameter", "Value", "Comment"]
    rows: List[List[str]] = []
    for param_key in RISK_PARAMS.keys():
        rows.append([param_key, kv.get(param_key, "-"), PARAM_COMMENTS.get(param_key, "-")])
    rows.extend([k, kv[k], "-"] for k in sorted(kv.keys()) if k not in RISK_PARAMS)

    print_three_line_table(headers, rows)


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    print(f"🔄 同步 MM pre_trade 风控参数: open={args.open_venue}, hedge={args.hedge_venue}")
    print("📍 Redis: 127.0.0.1:6379/0")

    sync_risk_params(rds, args.open_venue, args.hedge_venue)
    print_risk_params(rds, args.open_venue, args.hedge_venue)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
