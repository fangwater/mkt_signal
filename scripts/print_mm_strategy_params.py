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


def infer_env_name_from_cwd() -> Optional[str]:
    name = Path.cwd().name.strip().lower()
    return name or None


def resolve_venue() -> Optional[str]:
    ex = infer_exchange_from_cwd()
    if ex:
        return f"{ex}-futures"
    return None


def make_strategy_key(venue: str) -> str:
    return f"mm_strategy_params_{venue}"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Print mm strategy params from Redis (venue is inferred from current directory)"
    )
    return p.parse_args()


PARAM_COMMENTS: Dict[str, str] = {
    "default_order_amount": "默认单量(USDT，可被配置覆盖)",
    "order_interval_ms": "报单触发间隔(ms)",
    "open_orders_per_round": "MM open 每轮报单数量",
    "open_buy_vol_scale": "MM open 买侧波动率放缩区间，格式为长度为2的 JSON 数组，例如 [0.1,2.0]",
    "open_sell_vol_scale": "MM open 卖侧波动率放缩区间，格式为长度为2的 JSON 数组，例如 [0.1,2.0]",
    "hedge_orders_per_round": "MM hedge 每轮拆单档数",
    "open_order_timeout": "开仓订单超时(秒)",
    "next_query_delay_ms": "对冲 query 触发间隔(ms)",
    "hedge_vol_multiplier": "对冲波动率倍数（bound = volatility * hedge_vol_multiplier）",
    "hedge_price_offset_limit_upper": "对冲侧偏移上界（price_offset_limit）",
    "hedge_price_offset_limit_lower": "对冲侧偏移下界（price_offset_limit）",
    "hedge_offset_ratio": "对冲偏移手动系数（最终 offset 直接乘该系数）",
    "hedge_window_scale_low": "对冲拆单窗口下界系数（中心 offset 乘该系数作为最内层档位）",
    "hedge_window_scale_high": "对冲拆单窗口上界系数（中心 offset 乘该系数作为最外层档位）",
    "enable_return_score_adjust_hegde": "是否启用 return score 调整 MM hedge offset（false=使用中性 score 计算 hedge offset）",
    "enable_environment_model": "是否启用 env 开仓限制（false=继续读取 env / pnlu 并写入 from_key，但不阻拦开仓）",
    "enable_volatility_limit": "是否启用波动率限制下单",
    "open_volatility_limit": "波动率限制分位数（trade signal / MM 决策侧内联波动率阈值采样使用，默认 70）",
    "enable_tradecount_limit": "是否启用 tradecount 限制下单（仅 MM；tradecount rolling mean 大于阈值才允许开仓）",
    "open_tradecount_limit": "tradecount 限制分位数（trade signal / MM 决策侧对 count.rolling(30,min_periods=25).mean() 做内联阈值采样，默认 70）",
    "enable_open_time_block": "是否启用 UTC 时间段开仓阻断（true=在 open_block_utc_time_range 内 trade signal 不发开仓单）",
    "open_block_utc_time_range": "UTC 开仓阻断时间段，格式 HH:MM-HH:MM，允许跨天，开始/结束不能相同",
    "hedge_aggressive_seq_threshold": "对冲激进阈值(request_seq>=该值时不偏移，但仍为maker限价单)",
    "enable_return_score_cancel": "是否启用基于模型 score_quantile 的 MM open 方向撤单",
    "return_score_buy_cancel_quantile": "score_quantile*100 大于该分位数时，撤掉 symbol 所有 sell 方向 open 单；范围 (0,99)，默认 90",
    "return_score_sell_cancel_quantile": "score_quantile*100 小于该分位数时，撤掉 symbol 所有 buy/long 方向 open 单；范围 (0,99)，默认 10",
    "enable_tlen_cancel": "是否启用基于 tlen 的 MM open 撤单链路（true=允许发 MMCancelTrigger 并走 query/cancel）",
    "tlen_cancel_freq_ms": "MMCancelTrigger 触发频率(ms)，需为正整数，默认 3000",
    "return_model_service": "收益率模型输出通道名（'-' 表示禁用）",
    "environment_model_service": "环境模型输出通道名（'-' 表示禁用）",
    "max_hedge_price_pct_change": "对冲价格最大变动阈值(%)，范围>0且<=99，可为小数，超过则强制 taker",
    "signal_cooldown": "信号冷却时间(秒)",
}

PARAM_PRINT_ORDER = [
    "default_order_amount",
    "order_interval_ms",
    "open_orders_per_round",
    "open_buy_vol_scale",
    "open_sell_vol_scale",
    "hedge_orders_per_round",
    "open_order_timeout",
    "next_query_delay_ms",
    "hedge_vol_multiplier",
    "hedge_price_offset_limit_upper",
    "hedge_price_offset_limit_lower",
    "hedge_offset_ratio",
    "hedge_window_scale_low",
    "hedge_window_scale_high",
    "enable_return_score_adjust_hegde",
    "enable_environment_model",
    "enable_volatility_limit",
    "open_volatility_limit",
    "enable_tradecount_limit",
    "open_tradecount_limit",
    "enable_open_time_block",
    "open_block_utc_time_range",
    "hedge_aggressive_seq_threshold",
    "enable_return_score_cancel",
    "return_score_buy_cancel_quantile",
    "return_score_sell_cancel_quantile",
    "enable_tlen_cancel",
    "tlen_cancel_freq_ms",
    "return_model_service",
    "environment_model_service",
    "max_hedge_price_pct_change",
    "signal_cooldown",
]


def validate_open_block_utc_time_range(raw: str) -> str:
    text = (raw or "").strip()
    matched = re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)-([01]\d|2[0-3]):([0-5]\d)", text)
    if not matched:
        raise ValueError(
            "open_block_utc_time_range 必须使用 UTC HH:MM-HH:MM 格式，例如 15:55-23:59"
        )

    start_hour, start_min, end_hour, end_min = [int(part) for part in matched.groups()]
    start_total = start_hour * 60 + start_min
    end_total = end_hour * 60 + end_min
    if end_total == start_total:
        raise ValueError(
            "open_block_utc_time_range 开始/结束时间不能相同，避免全天/空窗口歧义"
        )
    return text


def validate_strategy_params(params: Dict[str, str]) -> None:
    if "open_block_utc_time_range" in params:
        validate_open_block_utc_time_range(params["open_block_utc_time_range"])


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

    try:
        validate_strategy_params(decoded)
    except ValueError as exc:
        print(f"❌ 当前 Redis 参数校验失败: {exc}", file=sys.stderr)

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
    key = make_strategy_key(venue)
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    print(f"🔎 读取 mm 策略参数: {key}")
    print(f"🏷️ venue: {venue}")
    print("📍 Redis: 127.0.0.1:6379/0")

    print_params(rds, key)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
