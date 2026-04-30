#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将 Funding Rate Pre-Trade 风控参数同步到 Redis 并打印。

写入 Redis Hash:
  `<dir>:<open>:<hedge>:pre_trade_risk_params` - 风控参数（max_pos_u, max_leverage等）

同步完成后自动打印所有参数。

示例：
  python scripts/sync_fr_risk_params.py --open-venue binance-margin --hedge-venue binance-futures
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List


def try_import_redis():
    try:
        import redis  # type: ignore
        return redis
    except Exception:
        return None


EXCHANGE_DEFAULTS = {
    "binance": ("binance-margin", "binance-futures"),
    "okex": ("okex-margin", "okex-futures"),
    "bybit": ("bybit-margin", "bybit-futures"),
    "bitget": ("bitget-margin", "bitget-futures"),
    "gate": ("gate-margin", "gate-futures"),
}


def infer_venues_from_cwd():
    """从当前目录名推断 open/hedge（如 okex_fr_trade -> okex-margin/okex-futures）"""
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
    p = argparse.ArgumentParser(description="Sync Funding Rate pre-trade risk params to Redis")
    p.add_argument("--open-venue", default=os.environ.get("OPEN_VENUE"))
    p.add_argument("--hedge-venue", default=os.environ.get("HEDGE_VENUE"))
    args = p.parse_args()

    open_venue = args.open_venue
    hedge_venue = args.hedge_venue
    if not open_venue and not hedge_venue:
        if inferred := infer_venues_from_cwd():
            open_venue, hedge_venue = inferred
            print(f"[INFO] 未提供 open/hedge，基于目录推断: open={open_venue}, hedge={hedge_venue}")

    if not open_venue or not hedge_venue:
        p.error(
            "需要 --open-venue 与 --hedge-venue，或在目录名包含 <exchange> 前缀（如 okex_fr_trade）以自动推断"
        )

    args.open_venue = open_venue
    args.hedge_venue = hedge_venue
    return args


# ========== 风控参数配置 ==========

# Hash key: pre_trade_risk_params
RISK_PARAMS = {
    # 最大单币种持仓 (USDT)
    "max_pos_u": "100000.0",

    # 单币种最大敞口比例（0.0-1.0）
    "max_symbol_exposure_ratio": "0.015",

    # 总敞口比例（0.0-1.0）
    "max_total_exposure_ratio": "0.01",

    # 最大杠杆倍数（>0）
    "max_leverage": "1.75",

    # 最大挂单数（>=0）
    "max_pending_limit_orders": "10",

    # 套利买/卖侧最大限价挂单数（>=0，0 表示关闭方向风控）
    "arb_max_pending_limit_buy_orders": "0",
    "arb_max_pending_limit_sell_orders": "0",

    # 套利开仓 60s/10s 下单数频率上限（>=0，0 表示不限）
    "arb_open_order_rate_limit_per_min": "0",
    "arb_open_order_rate_limit_10s": "0",

    # 套利对冲 60s/10s 下单数频率上限（>=0，0 表示不限）
    "arb_hedge_order_rate_limit_per_min": "0",
    "arb_hedge_order_rate_limit_10s": "0",
}

# ========== 参数注释（用于打印） ==========

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

def build_risk_params_key(open_venue: str | None, hedge_venue: str | None) -> str:
    if not open_venue or not hedge_venue:
        raise ValueError("missing open/hedge venue")
    dir_prefix = infer_dir_prefix_from_cwd()
    if dir_prefix:
        return f"{dir_prefix}:{open_venue}:{hedge_venue}:pre_trade_risk_params"
    return f"{open_venue}:{hedge_venue}:pre_trade_risk_params"


def sync_risk_params(rds, open_venue: str | None, hedge_venue: str | None) -> int:
    """同步风控参数到 Redis Hash"""
    key = build_risk_params_key(open_venue, hedge_venue)
    rds.hset(key, mapping=RISK_PARAMS)
    print(f"✅ 已写入 {len(RISK_PARAMS)} 个参数到 HASH '{key}' (primary)")
    return len(RISK_PARAMS) 


def print_three_line_table(headers: List[str], rows: List[List[str]]) -> None:
    """打印三线表格"""
    # 计算列宽
    ncols = len(headers)
    widths = [0] * ncols
    for i, h in enumerate(headers):
        widths[i] = max(widths[i], len(h))
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(cell))

    # 格式化行
    def fmt_row(values: List[str]) -> str:
        parts: List[str] = []
        for i, v in enumerate(values):
            parts.append(v.ljust(widths[i]))
        return "  ".join(parts)

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


def print_risk_params(rds, open_venue: str | None, hedge_venue: str | None) -> None:
    """打印风控参数"""
    print("\n📊 风控参数:")
    print("-" * 80)

    key = build_risk_params_key(open_venue, hedge_venue)

    print(f"🔑 Redis Hash Key: {key}")
    data = rds.hgetall(key)

    if not data:
        print("⚠️  未找到参数或 HASH 为空")
        return

    # 解码数据
    kv: Dict[str, str] = {}
    for k, v in data.items():
        kk = k.decode('utf-8', 'ignore') if isinstance(k, bytes) else str(k)
        vv = v.decode('utf-8', 'ignore') if isinstance(v, bytes) else str(v)
        kv[kk] = vv

    # 构建表格行
    headers = ["Parameter", "Value", "Comment"]
    rows: List[List[str]] = []

    # 按照定义顺序输出
    for param_key in RISK_PARAMS.keys():
        value = kv.get(param_key, "-")
        comment = PARAM_COMMENTS.get(param_key, "-")
        rows.append([param_key, value, comment])

    # 输出额外的参数（如果有）
    rows.extend([k, kv[k], "-"] for k in sorted(kv.keys()) if k not in RISK_PARAMS)
    print_three_line_table(headers, rows)


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    print("🔄 开始同步 Funding Rate 风控参数...")
    print("📍 Redis: 127.0.0.1:6379/0")
    print(f"📍 pretrade open={args.open_venue} hedge={args.hedge_venue}")
    print()

    # 同步参数
    sync_risk_params(rds, args.open_venue, args.hedge_venue)

    # 打印结果
    print_risk_params(rds, args.open_venue, args.hedge_venue)

    print("\n✅ 同步完成！")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
