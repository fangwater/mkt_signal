#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将 intra（同所期现）策略参数同步到 Redis 并打印。

写入 Redis Hash:
  `intra_strategy_params_{open_venue}_{hedge_venue}` - 策略参数

说明:
  - 同所期现：open=<exchange>-margin, hedge=<exchange>-futures
  - 推断优先级：--exchange / --open-venue / --hedge-venue / --env-name / CWD
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Dict, Optional

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


def infer_exchange_from_name(name: str) -> Optional[str]:
    n = (name or "").strip().lower()
    m = re.match(r"^([a-z0-9]+)[-_]intra([_-].*)?$", n)
    if not m:
        return None
    ex = normalize_exchange(m.group(1))
    if ex not in SUPPORTED_EXCHANGES:
        return None
    return ex


def infer_exchange_from_cwd() -> Optional[str]:
    return infer_exchange_from_name(Path.cwd().name)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync intra strategy params to Redis")
    p.add_argument("--exchange", default=os.environ.get("EXCHANGE"))
    p.add_argument("--open-venue", default=os.environ.get("OPEN_VENUE"))
    p.add_argument("--hedge-venue", default=os.environ.get("HEDGE_VENUE"))
    p.add_argument("--env-name", help="环境目录名（例如 binance-intra-trade）")
    args = p.parse_args()

    exchange = args.exchange
    if not exchange:
        exchange = infer_exchange_from_name(args.env_name) if args.env_name else infer_exchange_from_cwd()
        if exchange:
            print(f"[INFO] 未提供 --exchange，基于目录推断: exchange={exchange}", file=sys.stderr)

    if exchange:
        exchange = normalize_exchange(exchange)
        if exchange not in SUPPORTED_EXCHANGES:
            p.error(f"不支持的 exchange: {exchange}")
        if not args.open_venue:
            args.open_venue = f"{exchange}-margin"
        if not args.hedge_venue:
            args.hedge_venue = f"{exchange}-futures"

    if not args.open_venue or not args.hedge_venue:
        p.error("需要 --exchange 或同时提供 --open-venue/--hedge-venue，或使用 --env-name <exchange>-intra-<tag>")

    args.open_venue = args.open_venue.lower()
    args.hedge_venue = args.hedge_venue.lower()
    return args


# Hash key: intra_strategy_params_{open_venue}_{hedge_venue}
# 注：intra 默认 enable_volatility_limit=false。Rust ArbDecision 已支持 mm 同款
# inline vol gate；老 intra 实例升级时如果 redis 没写过该字段，进程会用默认值
# 启动，所以这里默认关掉避免 vol gate 突然激活拦截开仓。需要时面板或 sync 改 true。
STRATEGY_PARAMS = {
    "order_amount": "100.0",
    "vol_band_scale": "[0.0,1.0]",
    "open_orders_per_round": "10",
    "open_order_timeout": "120",
    "hedge_timeout": "60",
    "hedge_vol_multiplier": "2.0",
    "hedge_offset_ratio": "1.3",
    "hedge_price_offset_limit_lower": "0.0003",
    "hedge_price_offset_limit_upper": "0.005",
    "enable_return_score_adjust_hedge": "true",
    "hedge_aggressive_seq_threshold": "50",
    "enable_tlen_cancel": "false",
    "tlen_cancel_freq_ms": "3000",
    "spread_cancel_cooldown_ms": "100",
    "enable_environment_model": "true",
    "enable_volatility_limit": "false",
    "open_volatility_limit": "70",
    "return_model_service": "-",
    "environment_model_service": "-",
    "signal_cooldown": "5",
    "max_hedge_price_pct_change": "5",
}

PARAM_COMMENTS: Dict[str, str] = {
    "order_amount": "单笔下单量(USDT)",
    "vol_band_scale": "开仓 plan 波动带缩放倍率 [low,high]（JSON 数组，实际偏移区间=vol*[low,high]）",
    "open_orders_per_round": "单边 plan 的档位数（从同侧盘口线性铺到 vol 边界）",
    "open_order_timeout": "开仓订单超时(秒)",
    "hedge_timeout": "对冲订单超时(秒)",
    "hedge_vol_multiplier": "对冲波动率倍数（bound = volatility * hedge_vol_multiplier）",
    "hedge_offset_ratio": "对冲偏移手动系数（最终 offset 直接乘该系数）",
    "hedge_price_offset_limit_lower": "对冲侧偏移下界（price_offset_limit）",
    "hedge_price_offset_limit_upper": "对冲侧偏移上界（price_offset_limit）",
    "enable_return_score_adjust_hedge": "是否启用 return score 调整 hedge offset（false=使用中性 score 计算 hedge offset）",
    "hedge_aggressive_seq_threshold": "对冲激进阈值(request_seq>=该值时不偏移，但仍为maker限价单)",
    "enable_tlen_cancel": "是否启用基于 tlen 的 open 撤单链路（true=允许发 trigger/query/cancel）",
    "tlen_cancel_freq_ms": "tlen 撤单触发频率(ms)，需为正整数，默认 3000",
    "spread_cancel_cooldown_ms": "spread cancel 冷却时间(ms)，需为非负整数，默认 100；0 表示不冷却",
    "enable_environment_model": "是否启用 env 开仓限制（false=继续读取 env / pnlu 并写入 from_key，但不阻拦开仓）",
    "enable_volatility_limit": "是否启用基于 inline vol 阈值的开仓 gate（true=vol > rolling-percentile threshold 时拦截开仓；intra 默认 false）",
    "open_volatility_limit": "开仓 vol gate 使用的 rolling 分位数（0-100，默认 70；与 mm 同语义）",
    "return_model_service": "收益率模型输出通道名（'-' 表示不读取；配置通道名时仅读取并记录到 from_key，不拦截开仓）",
    "environment_model_service": "环境模型输出通道名（'-' 表示禁用）",
    "max_hedge_price_pct_change": "对冲价格最大变动阈值(%)，范围>0且<=99，可为小数，超过则强制 taker",
    "signal_cooldown": "信号冷却时间(秒)",
}

PARAM_PRINT_ORDER = [
    "order_amount",
    "vol_band_scale",
    "open_orders_per_round",
    "open_order_timeout",
    "hedge_timeout",
    "hedge_vol_multiplier",
    "hedge_offset_ratio",
    "hedge_price_offset_limit_lower",
    "hedge_price_offset_limit_upper",
    "enable_return_score_adjust_hedge",
    "hedge_aggressive_seq_threshold",
    "enable_tlen_cancel",
    "tlen_cancel_freq_ms",
    "spread_cancel_cooldown_ms",
    "enable_environment_model",
    "enable_volatility_limit",
    "open_volatility_limit",
    "return_model_service",
    "environment_model_service",
    "max_hedge_price_pct_change",
    "signal_cooldown",
]


def sync_strategy_params(rds, key: str) -> int:
    # 兼容性清理：删掉 intra 已废弃的字段（此前 enable_volatility_limit /
    # open_volatility_limit 也在 stale 列表里，但 Rust 端已支持 inline vol gate
    # 后这两个字段重新启用，故移出 stale 名单。）
    for stale_field in (
        "price_offsets",
        "hedge_price_offset_fallback",
        "open_scale",
        "hedge_price_offset",
        "hedge_window_scale_low",
        "hedge_window_scale_high",
    ):
        rds.hdel(key, stale_field)
    rds.hset(key, mapping=STRATEGY_PARAMS)
    print(f"✅ 已写入 {len(STRATEGY_PARAMS)} 个参数到 HASH '{key}'")
    return len(STRATEGY_PARAMS)


def print_params(rds, key: str) -> None:
    print("\n📊 intra 策略参数配置:")
    print("=" * 80)
    data = rds.hgetall(key)
    if not data:
        print(f"⚠️  HASH '{key}' 为空或不存在")
        return

    decoded: Dict[str, str] = {}
    for raw_k, raw_v in data.items():
        k = raw_k.decode("utf-8", "ignore") if isinstance(raw_k, bytes) else str(raw_k)
        v = raw_v.decode("utf-8", "ignore") if isinstance(raw_v, bytes) else str(raw_v)
        decoded[k] = v

    def print_one(k: str, v: str) -> None:
        comment = PARAM_COMMENTS.get(k, "")
        if comment:
            print(f"  {k:28} {v:>12}  # {comment}")
        else:
            print(f"  {k:28} {v:>12}")

    printed = set()
    for k in PARAM_PRINT_ORDER:
        if k in decoded:
            print_one(k, decoded[k])
            printed.add(k)

    for k in sorted(decoded.keys()):
        if k in printed:
            continue
        print_one(k, decoded[k])


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    key = f"intra_strategy_params_{args.open_venue}_{args.hedge_venue}"
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    print(f"🔄 同步 intra 策略参数: {key}")
    print("📍 Redis: 127.0.0.1:6379/0")

    sync_strategy_params(rds, key)
    print_params(rds, key)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
