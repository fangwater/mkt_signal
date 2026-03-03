#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 MM 策略参数（从 Redis 读取）。

读取 Redis Hash:
  mm_strategy_params_{venue}

venue 必须由当前目录强制推断（例如 binance_mm_beta -> binance-futures）。

示例:
  cd ~/binance_mm_beta
  python scripts/print_mm_strategy_params.py
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Dict, Optional


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
    for ex in ["binance", "okex", "okx", "bybit", "bitget", "gate"]:
        if re.search(rf"(^|[^a-z0-9]){ex}([^a-z0-9]|$)", n):
            return normalize_exchange(ex)
    return None


def infer_exchange_from_cwd() -> Optional[str]:
    return infer_exchange_from_name(Path.cwd().name)


def resolve_venue() -> Optional[str]:
    ex = infer_exchange_from_cwd()
    if ex:
        return f"{ex}-futures"
    return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Print mm strategy params from Redis (venue is inferred from current directory)"
    )
    return p.parse_args()


PARAM_COMMENTS: Dict[str, str] = {
    "order_amount": "单笔下单量(USDT)",
    "open_price_offsets": "开仓挂单档位(JSON数组)",
    "open_vol_upper_scale": "开仓侧上界修正系数（基于波动率因子）",
    "open_vol_lower_scale": "开仓侧下界修正系数（基于波动率因子）",
    "open_price_offset_limit_upper": "开仓侧偏移上界（price_offset_limit）",
    "open_price_offset_limit_lower": "开仓侧偏移下界（price_offset_limit）",
    "open_order_timeout": "开仓订单超时(秒)",
    "next_query_delay_ms": "对冲 query 触发间隔(ms)",
    "hedge_vol_upper_scale": "对冲侧上界修正系数（基于波动率因子，如 rl_return_volatility）",
    "hedge_vol_lower_scale": "对冲侧下界修正系数（基于波动率因子，如 rl_return_volatility）",
    "hedge_price_offset_limit_upper": "对冲侧偏移上界（price_offset_limit）",
    "hedge_price_offset_limit_lower": "对冲侧偏移下界（price_offset_limit）",
    "hedge_offset_expand_k": "同向成交占优时区间放大系数",
    "hedge_offset_compress_k": "反向压力/迫切度下区间压缩系数",
    "hedge_offset_scale_min": "对冲区间缩放下限",
    "hedge_offset_scale_max": "对冲区间缩放上限",
    "hedge_offset_shift_out_k": "同向成交占优时区间外移系数",
    "hedge_offset_shift_in_k": "反向压力/迫切度下区间内移系数",
    "hedge_aggressive_seq_threshold": "对冲激进阈值(request_seq>=该值时不偏移，但仍为maker限价单)",
    "return_model_service": "收益率模型输出通道名（'-' 表示禁用）",
    "environment_model_service": "环境模型输出通道名（'-' 表示禁用）",
    "max_hedge_price_pct_change": "对冲价格最大变动阈值(%)，范围1-99，可为小数，超过则强制 taker",
    "signal_cooldown": "信号冷却时间(秒)",
}

PARAM_PRINT_ORDER = [
    "order_amount",
    "open_price_offsets",
    "open_vol_upper_scale",
    "open_vol_lower_scale",
    "open_price_offset_limit_upper",
    "open_price_offset_limit_lower",
    "open_order_timeout",
    "next_query_delay_ms",
    "hedge_vol_upper_scale",
    "hedge_vol_lower_scale",
    "hedge_price_offset_limit_upper",
    "hedge_price_offset_limit_lower",
    "hedge_offset_expand_k",
    "hedge_offset_compress_k",
    "hedge_offset_scale_min",
    "hedge_offset_scale_max",
    "hedge_offset_shift_out_k",
    "hedge_offset_shift_in_k",
    "hedge_aggressive_seq_threshold",
    "return_model_service",
    "environment_model_service",
    "max_hedge_price_pct_change",
    "signal_cooldown",
]


def print_params(rds, key: str) -> None:
    print("\n📊 mm 策略参数配置:")
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

    # 兼容未来新增字段：未在固定顺序中的字段按字典序追加。
    for k in sorted(decoded.keys()):
        if k in printed:
            continue
        print_one(k, decoded[k])


def main() -> int:
    _args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    venue = resolve_venue()
    if not venue:
        print(
            "❌ 无法从当前目录推断 venue。请在目录名包含 binance/okex/bybit/bitget/gate 前缀的 MM 目录运行（如 ~/binance_mm_beta）",
            file=sys.stderr,
        )
        return 1

    key = f"mm_strategy_params_{venue}"
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    print(f"🔎 读取 mm 策略参数: {key}")
    print("📍 Redis: 127.0.0.1:6379/0")

    print_params(rds, key)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
