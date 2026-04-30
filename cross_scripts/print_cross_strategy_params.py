#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 cross（跨所）策略参数（从 Redis 读取）。

读取 Redis Hash:
  `cross_strategy_params_{open_venue}_{hedge_venue}`

推断规则：
  - open/hedge venue：优先 --open-venue/--hedge-venue，其次 --env-name，再其次 CWD（如 okex-binance-cross-trade）
  - cross 固定 futures-only：默认推断为 <exchange>-futures
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
    return infer_cross_venues_from_env_name(Path.cwd().name)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print cross strategy params from Redis")
    p.add_argument("--open-venue", help="open 侧 venue（例如 okex-futures）")
    p.add_argument("--hedge-venue", help="hedge 侧 venue（例如 binance-futures）")
    p.add_argument("--env-name", help="环境目录名（例如 okex-binance-cross-trade）")
    args = p.parse_args()

    open_venue = args.open_venue
    hedge_venue = args.hedge_venue
    if not open_venue and not hedge_venue:
        inferred = (
            infer_cross_venues_from_env_name(args.env_name)
            if args.env_name
            else infer_cross_venues_from_cwd()
        )
        if inferred:
            open_venue, hedge_venue = inferred
            print(
                f"[INFO] 未提供 open/hedge，基于目录推断: open={open_venue}, hedge={hedge_venue}",
                file=sys.stderr,
            )

    if not open_venue or not hedge_venue:
        p.error(
            "需要 --open-venue 与 --hedge-venue，或使用 --env-name / 在目录名包含 '<open>-<hedge>-cross-...' 以自动推断"
        )

    args.open_venue = open_venue.lower()
    args.hedge_venue = hedge_venue.lower()
    return args


PARAM_COMMENTS: Dict[str, str] = {
    "mode": "做市模式(MM=双边挂单/MT=吃单对冲)",
    "order_amount": "单笔下单量(USDT)",
    "open_scale": "开仓 plan 的波动边界缩放系数（实际边界=vol*open_scale）",
    "open_orders_per_round": "单边 plan 的档位数（从同侧盘口线性铺到 vol 边界）",
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


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    key = f"cross_strategy_params_{args.open_venue}_{args.hedge_venue}"
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    data = rds.hgetall(key)
    print("📍 Redis: 127.0.0.1:6379/0")
    print(f"📊 cross 策略参数: {key}")
    print("=" * 80)
    if not data:
        print(f"⚠️  HASH '{key}' 为空或不存在")
        return 0

    items = []
    for raw_k, raw_v in data.items():
        k = raw_k.decode("utf-8", "ignore") if isinstance(raw_k, bytes) else str(raw_k)
        v = raw_v.decode("utf-8", "ignore") if isinstance(raw_v, bytes) else str(raw_v)
        items.append((k, v))

    decoded = dict(items)

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

    for k, v in sorted(items, key=lambda kv: kv[0]):
        if k in printed:
            continue
        print_one(k, v)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
