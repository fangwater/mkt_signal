#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将 xarb（跨所）策略参数同步到 Redis 并打印。

写入 Redis Hash:
  `xarb_strategy_params_{open_venue}_{hedge_venue}` - 策略参数（mode, order_amount 等，按 open/hedge 组合区分）

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
from typing import Dict, Optional, Tuple

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
    p = argparse.ArgumentParser(description="Sync xarb strategy params to Redis")
    p.add_argument("--open-venue", help="open 侧 venue（例如 okex-futures）")
    p.add_argument("--hedge-venue", help="hedge 侧 venue（例如 binance-futures）")
    p.add_argument("--env-name", help="环境目录名（例如 okex-binance-xarb-trade）")
    args = p.parse_args()

    open_venue = args.open_venue
    hedge_venue = args.hedge_venue
    if not open_venue and not hedge_venue:
        inferred = (
            infer_xarb_venues_from_env_name(args.env_name) if args.env_name else infer_xarb_venues_from_cwd()
        )
        if inferred:
            open_venue, hedge_venue = inferred
            print(f"[INFO] 未提供 open/hedge，基于目录推断: open={open_venue}, hedge={hedge_venue}")

    if not open_venue or not hedge_venue:
        p.error(
            "需要 --open-venue 与 --hedge-venue，或使用 --env-name / 在目录名包含 '<open>-<hedge>-xarb-...' 以自动推断"
        )

    args.open_venue = open_venue.lower()
    args.hedge_venue = hedge_venue.lower()
    return args


# Hash key: xarb_strategy_params_{open_venue}_{hedge_venue}
STRATEGY_PARAMS = {
    "mode": "MM",
    "order_amount": "100.0",
    "open_scale": "1.0",
    "open_orders_per_round": "10",
    "open_order_timeout": "120",
    "hedge_timeout": "60",
    "hedge_price_offset_fallback": "0.001",
    "hedge_aggressive_seq_threshold": "50",
    "enable_tlen_cancel": "false",
    "tlen_cancel_freq_ms": "3000",
    "enable_return_score_model": "false",
    "enable_environment_model": "true",
    "enable_volatility_limit": "true",
    "open_volatility_limit": "70",
    "return_model_service": "-",
    "environment_model_service": "-",
    "signal_cooldown": "5",
    "max_hedge_price_pct_change": "5"
}

PARAM_COMMENTS: Dict[str, str] = {
    "mode": "做市模式(MM=双边挂单/MT=吃单对冲)",
    "order_amount": "单笔下单量(USDT)",
    "open_scale": "开仓 plan 的波动边界缩放系数（实际边界=vol*open_scale）",
    "open_orders_per_round": "单边 plan 的档位数（从同侧盘口线性铺到 vol 边界）",
    "open_order_timeout": "开仓订单超时(秒)",
    "hedge_timeout": "对冲订单超时(秒)",
    "hedge_price_offset_fallback": "波动率因子不可用时的对冲默认价格偏移(万分之几)",
    "hedge_aggressive_seq_threshold": "对冲激进阈值(request_seq>=该值时不偏移，但仍为maker限价单)",
    "enable_tlen_cancel": "是否启用基于 tlen 的 open 撤单链路（true=允许发 trigger/query/cancel）",
    "tlen_cancel_freq_ms": "tlen 撤单触发频率(ms)，需为正整数，默认 3000",
    "enable_return_score_model": "是否启用 return score 拦截(true/false，false=只读取/透传，不拦截开仓)",
    "enable_environment_model": "是否启用 env 开仓限制（false=继续读取 env / pnlu 并写入 from_key，但不阻拦开仓）",
    "enable_volatility_limit": "是否启用波动率限制下单",
    "open_volatility_limit": "波动率限制分位数（读取 rolling_metrics 的 open_vol_xx，默认 70）",
    "return_model_service": "收益率模型输出通道名（'-' 表示不读取；配置通道名时 false 也会读取但不拦截）",
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
    "hedge_price_offset_fallback",
    "hedge_aggressive_seq_threshold",
    "enable_tlen_cancel",
    "tlen_cancel_freq_ms",
    "enable_return_score_model",
    "enable_environment_model",
    "enable_volatility_limit",
    "open_volatility_limit",
    "return_model_service",
    "environment_model_service",
    "max_hedge_price_pct_change",
    "signal_cooldown",
]


def sync_strategy_params(rds, key: str) -> int:
    rds.hdel(key, "price_offsets")
    rds.hdel(key, "hedge_price_offset")
    rds.hset(key, mapping=STRATEGY_PARAMS)
    print(f"✅ 已写入 {len(STRATEGY_PARAMS)} 个参数到 HASH '{key}'")
    return len(STRATEGY_PARAMS)


def print_params(rds, key: str) -> None:
    print("\n📊 xarb 策略参数配置:")
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

    open_venue = args.open_venue
    hedge_venue = args.hedge_venue
    key = f"xarb_strategy_params_{open_venue}_{hedge_venue}"

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    print(f"🔄 同步 xarb 策略参数: {key}")
    print("📍 Redis: 127.0.0.1:6379/0")

    sync_strategy_params(rds, key)
    print_params(rds, key)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
