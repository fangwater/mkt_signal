#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将 binance_forward_arb_params 常量参数写入 Redis HASH（覆盖/新增），并可选打印生效值。

参数写入到 HASH `binance_forward_arb_params`（可通过 --key 指定）：
  - 资金费率预测参数: interval, predict_num, refresh_secs, fetch_secs, fetch_offset_secs, history_limit
  - 4h/8h 阈值参数: fr_4h_* 与 fr_8h_*（共 8 个）
  - Pre-Trade 限制: pre_trade_max_pos_u, pre_trade_max_symbol_exposure_ratio, pre_trade_max_total_exposure_ratio
  - 下单参数: order_mode, order_open_range(数组), order_close_range(数组), order_amount_u, order_max_open_order_keep_s, order_max_close_order_keep_s

示例：
  python scripts/sync_binance_forward_arb_params.py
  python scripts/sync_binance_forward_arb_params.py --redis-url redis://:pwd@127.0.0.1:6379/0
"""

from __future__ import annotations

import argparse
import os
import sys


def try_import_redis():
    try:
        import redis  # type: ignore
        return redis
    except Exception:
        return None


STORE_FIELDS_TO_PURGE = [
    "pre_trade_store_enable",
    "pre_trade_store_prefix",
    "pre_trade_store_redis_url",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync constants to Redis HASH binance_forward_arb_params")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    p.add_argument("--key", default="binance_forward_arb_params")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("redis 包未安装，请使用 venv 或 --user 安装 redis。", file=sys.stderr)
        return 2

    rds = redis.from_url(args.redis_url) if args.redis_url else redis.Redis(
        host=args.host, port=args.port, db=args.db, password=args.password
    )

    try:
        rds.hdel(args.key, *STORE_FIELDS_TO_PURGE)
    except Exception:
        pass

    params = {
        # 资金费率预测
        "interval": "6",
        "predict_num": "1",
        "refresh_secs": "30",
        "fetch_secs": "7200",
        "fetch_offset_secs": "120",
        "history_limit": "100",
        # 4h thresholds
        "fr_4h_open_upper_threshold": "0.00004",
        "fr_4h_open_lower_threshold": "-0.00004",
        "fr_4h_close_lower_threshold": "-0.0008",
        "fr_4h_close_upper_threshold": "0.0008",
        # 8h thresholds
        "fr_8h_open_upper_threshold": "0.00008",
        "fr_8h_open_lower_threshold": "-0.00008",
        "fr_8h_close_lower_threshold": "-0.001",
        "fr_8h_close_upper_threshold": "0.001",
        # Funding strategy reload/signal params
        "signal_min_interval_ms": "1000",
        "reload_interval_secs": "60",
        # Pre-trade 限制
        "pre_trade_max_pos_u": "100",
        "pre_trade_max_symbol_exposure_ratio": "0.5",
        "pre_trade_max_total_exposure_ratio": "0.5",
        "pre_trade_refresh_secs": "30",
        # 下单参数
        "order_mode": "ladder",
        "order_open_range": "[0.0,0.01,0.02,0.03]",
        "order_close_range": "[0.0,0.01,0.02,0.03]",
        "order_ladder_cancel_bidask_threshold": "0.0005",
        "order_ladder_open_bidask_threshold": "-0.0004",
        "order_amount_u": "15",
        "order_max_open_order_keep_s": "30",
        "order_max_close_order_keep_s": "1",
        "max_pending_limit_orders": "5",
    }

    # HMSET (HSET 多字段)
    rds.hset(args.key, mapping=params)
    print(f"已写入/覆盖 {len(params)} 个参数到 HASH {args.key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
