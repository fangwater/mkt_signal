#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将 Funding Rate 策略参数同步到 Redis 并打印。

写入 Redis Hash:
  `fr_strategy_params_{open_venue}_{hedge_venue}` - 策略参数（mode, order_amount等，按 open/hedge 组合区分）

注意：交易对列表请使用 sync_fr_symbol_lists.py 脚本单独同步。

同步完成后自动打印所有参数。

示例：
  python scripts/sync_fr_strategy_params.py --open-venue binance-margin --hedge-venue binance-futures
  python scripts/sync_fr_strategy_params.py
  # 也可不带 open/hedge，脚本会基于当前目录名推断 exchange（形如 okex_fr_trade -> okex-margin/okex-futures）
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple


EXCHANGE_DEFAULTS = {
    "binance": ("binance-margin", "binance-futures"),
    "okex": ("okex-margin", "okex-futures"),
    "bybit": ("bybit-margin", "bybit-futures"),
    "bitget": ("bitget-margin", "bitget-futures"),
    "gate": ("gate-margin", "gate-futures"),
}


def try_import_redis():
    try:
        import redis  # type: ignore
        return redis
    except Exception:
        return None


def infer_venues_from_cwd() -> Optional[Tuple[str, str]]:
    """从当前目录名推断 open/hedge（如 okex_fr_trade -> okex-margin/okex-futures）"""
    name = Path.cwd().name.lower()
    candidates = [name]
    if "_" in name:
        candidates.append(name.split("_", 1)[0])
    for cand in candidates:
        for ex, pair in EXCHANGE_DEFAULTS.items():
            if cand.startswith(ex):
                return pair
    return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sync Funding Rate strategy params to Redis（可省略 open/hedge，默认按目录推断 margin/futures）"
    )
    p.add_argument("--open-venue", help="open 侧 venue（如 binance-margin）")
    p.add_argument("--hedge-venue", help="hedge 侧 venue（如 binance-futures）")
    args = p.parse_args()

    open_venue = args.open_venue
    hedge_venue = args.hedge_venue
    if not open_venue and not hedge_venue:
        inferred = infer_venues_from_cwd()
        if inferred:
            open_venue, hedge_venue = inferred
            print(
                f"[INFO] 未提供 open/hedge，基于目录推断: open={open_venue}, hedge={hedge_venue}"
            )
    if not open_venue or not hedge_venue:
        p.error(
            "需要 --open-venue 与 --hedge-venue，或在目录名包含 <exchange> 前缀（如 okex_fr_trade）以自动推断"
        )

    args.open_venue = open_venue
    args.hedge_venue = hedge_venue
    return args


# ========== 策略参数配置 ==========

# Hash key: fr_strategy_params_{open_venue}_{hedge_venue}
STRATEGY_PARAMS = {
    # 做市模式：MM（Maker-Maker）或 MT（Maker-Taker）
    "mode": "MM",

    # 单笔下单量（USDT）
    "order_amount": "100.0",

    # 开仓波动缩放系数（实际边界=vol*open_scale）
    "open_scale": "1.0",

    # 每轮开仓档位数
    "open_orders_per_round": "10",
    
    # 开仓订单超时（秒）
    "open_order_timeout": "150",

    # 对冲订单超时（秒）
    "hedge_timeout": "60",

    # 对冲价格偏移（万分之几）
    "hedge_price_offset": "0.001",

    # 对冲激进阈值（request_seq>=该值时不偏移，但仍为maker限价单）
    "hedge_aggressive_seq_threshold": "6",

    # 是否启用基于 tlen 的 open 撤单链路
    "enable_tlen_cancel": "false",

    # tlen 撤单触发频率（毫秒）
    "tlen_cancel_freq_ms": "3000",

    # 是否启用 environment 开仓限制
    "enable_environment_model": "true",

    # 是否启用波动率限制
    "enable_volatility_limit": "true",

    # 开仓波动率限制分位数
    "open_volatility_limit": "70",

    # return score 模型输出通道
    "return_model_service": "-",

    # environment 模型输出通道
    "environment_model_service": "-",

    # 对冲价格最大变动阈值（%）
    "max_hedge_price_pct_change": "5",

    # 信号冷却时间（秒）
    "signal_cooldown": "5",
}

# ========== 参数注释（用于打印） ==========

PARAM_COMMENTS: Dict[str, str] = {
    "mode": "做市模式(MM=双边挂单/MT=吃单对冲)",
    "order_amount": "单笔下单量(USDT)",
    "open_scale": "开仓 plan 的波动边界缩放系数（实际边界=vol*open_scale）",
    "open_orders_per_round": "每轮开仓档位数",
    "open_order_timeout": "开仓订单超时(秒)",
    "hedge_timeout": "对冲订单超时(秒)",
    "hedge_price_offset": "对冲价格偏移(万分之几)",
    "hedge_aggressive_seq_threshold": "对冲激进阈值(request_seq>=该值时不偏移，但仍为maker限价单)",
    "enable_tlen_cancel": "是否启用基于 tlen 的 open 撤单链路（true=允许发 trigger/query/cancel）",
    "tlen_cancel_freq_ms": "tlen 撤单触发频率(ms)，需为正整数，默认 3000",
    "enable_environment_model": "是否启用 env 开仓限制（false=继续读取 env / pnlu 并写入 from_key，但不阻拦开仓）",
    "enable_volatility_limit": "是否启用波动率限制下单",
    "open_volatility_limit": "波动率限制分位数（读取 rolling_metrics 的 open_vol_xx，默认 70）",
    "return_model_service": "收益率模型输出通道名（'-' 表示不读取；配置通道名时仅读取并记录到 from_key，不拦截开仓）",
    "environment_model_service": "环境模型输出通道名（'-' 表示禁用）",
    "max_hedge_price_pct_change": "对冲价格最大变动阈值(%)，范围>0且<=99，可为小数，超过则强制 taker",
    "signal_cooldown": "信号冷却时间(秒)",
}

PARAM_PRINT_ORDER = [
    "mode",
    "order_amount",
    "open_scale",
    "open_orders_per_round",
    "open_order_timeout",
    "hedge_timeout",
    "hedge_price_offset",
    "hedge_aggressive_seq_threshold",
    "enable_tlen_cancel",
    "tlen_cancel_freq_ms",
    "enable_environment_model",
    "enable_volatility_limit",
    "open_volatility_limit",
    "return_model_service",
    "environment_model_service",
    "max_hedge_price_pct_change",
    "signal_cooldown",
]


def sync_strategy_params(rds, key: str) -> int:
    """同步策略参数到 Redis Hash"""
    rds.hset(key, mapping=STRATEGY_PARAMS)
    print(f"✅ 已写入 {len(STRATEGY_PARAMS)} 个参数到 HASH '{key}'")
    return len(STRATEGY_PARAMS)


def format_number(val: float) -> str:
    """格式化数字，去除尾部0"""
    s = f"{val:.8f}"
    s = s.rstrip("0").rstrip(".")
    if s == "-0":
        s = "0"
    return s


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


def print_strategy_params(rds, key: str) -> None:
    """打印策略参数"""
    print(f"\n📊 策略参数 ({key}):")
    print("-" * 80)

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
    headers = ["param", "value", "comment"]
    rows: List[List[str]] = []

    # 按照定义顺序输出
    printed = set()
    for param_key in PARAM_PRINT_ORDER:
        value = kv.get(param_key, "-")
        comment = PARAM_COMMENTS.get(param_key, "-")
        rows.append([param_key, value, comment])
        printed.add(param_key)

    # 输出额外的参数（如果有）
    for k in sorted(kv.keys()):
        if k not in printed:
            rows.append([k, kv[k], "-"])

    print_three_line_table(headers, rows)


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    print("🔄 开始同步 Funding Rate 策略参数...")
    print("📍 Redis: 127.0.0.1:6379/0")
    print()

    # 同步参数
    open_venue = args.open_venue.strip()
    hedge_venue = args.hedge_venue.strip()
    key = f"fr_strategy_params_{open_venue}_{hedge_venue}"

    sync_strategy_params(rds, key)

    # 打印结果
    print_strategy_params(rds, key)

    print("\n✅ 同步完成！")
    print("\n💡 提示：如需同步交易对列表，请运行: python scripts/sync_fr_symbol_lists.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
