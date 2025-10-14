#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
同步 rolling_metrics 服务所需的参数到 Redis HASH。

默认写入 HASH `rolling_metrics_params`（可用 --key 修改），字段包括：
  - MAX_LENGTH：环形缓冲容量（条数）
  - ROLLING_WINDOW：每次计算使用的滑窗长度
  - MIN_PERIODS：分位计算的最小样本数量
  - RESAMPLE_INTERVAL_MS：价差率重采样周期（毫秒）
  - bidask_lower_quantile、bidask_upper_quantile
  - askbid_lower_quantile、askbid_upper_quantile
  - refresh_sec：分位重算周期（秒）
  - reload_param_sec：配置热更新周期（秒）
  - output_hash_key：写入结果的 Redis HASH 名称

示例：
  python scripts/sync_rolling_metrics_params.py
  python scripts/sync_rolling_metrics_params.py --redis-url redis://:pwd@127.0.0.1:6379/0
  python scripts/sync_rolling_metrics_params.py --max-length 200000 --rolling-window 120000
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict

DEFAULTS = {
    "MAX_LENGTH": 150_000,
    "ROLLING_WINDOW": 100_000,
    "MIN_PERIODS": 90_0,
    "RESAMPLE_INTERVAL_MS": 1_000,
    "bidask_lower_quantile": 0.05,
    "bidask_upper_quantile": 0.70,
    "askbid_lower_quantile": 0.30,
    "askbid_upper_quantile": 0.95,
    "refresh_sec": 30,
    "reload_param_sec": 3,
    "output_hash_key": "rolling_metrics_thresholds",
}


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync rolling_metrics parameters to Redis HASH")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    p.add_argument("--key", default="rolling_metrics_params", help="Redis HASH key to write")
    p.add_argument("--max-length", type=int)
    p.add_argument("--rolling-window", type=int)
    p.add_argument("--min-periods", type=int)
    p.add_argument("--resample-interval-ms", type=int)
    p.add_argument("--bidask-lower-quantile", type=float)
    p.add_argument("--bidask-upper-quantile", type=float)
    p.add_argument("--askbid-lower-quantile", type=float)
    p.add_argument("--askbid-upper-quantile", type=float)
    p.add_argument("--refresh-sec", type=int)
    p.add_argument("--reload-param-sec", type=int)
    p.add_argument("--output-hash-key")
    p.add_argument("--dry-run", action="store_true", help="Only print payload without writing")
    return p.parse_args()


def build_payload(args: argparse.Namespace) -> Dict[str, str]:
    payload = dict(DEFAULTS)
    if args.max_length is not None:
        payload["MAX_LENGTH"] = args.max_length
    if args.rolling_window is not None:
        payload["ROLLING_WINDOW"] = args.rolling_window
    if args.min_periods is not None:
        payload["MIN_PERIODS"] = args.min_periods
    if args.resample_interval_ms is not None:
        payload["RESAMPLE_INTERVAL_MS"] = max(1, args.resample_interval_ms)
    if args.bidask_lower_quantile is not None:
        payload["bidask_lower_quantile"] = args.bidask_lower_quantile
    if args.bidask_upper_quantile is not None:
        payload["bidask_upper_quantile"] = args.bidask_upper_quantile
    if args.askbid_lower_quantile is not None:
        payload["askbid_lower_quantile"] = args.askbid_lower_quantile
    if args.askbid_upper_quantile is not None:
        payload["askbid_upper_quantile"] = args.askbid_upper_quantile
    if args.refresh_sec is not None:
        payload["refresh_sec"] = args.refresh_sec
    if args.reload_param_sec is not None:
        payload["reload_param_sec"] = args.reload_param_sec
    if args.output_hash_key:
        payload["output_hash_key"] = args.output_hash_key
    return {k: str(v) for k, v in payload.items()}


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("redis 包未安装，请使用 pip install redis。", file=sys.stderr)
        return 2

    if args.redis_url:
        rds = redis.from_url(args.redis_url)
    else:
        rds = redis.Redis(host=args.host, port=args.port, db=args.db, password=args.password)

    payload = build_payload(args)
    if args.dry_run:
        print("dry-run: 即将写入的字段：")
        for k, v in payload.items():
            print(f"  {k} = {v}")
        return 0

    rds.hset(args.key, mapping=payload)
    print(f"已写入 {len(payload)} 个字段到 HASH {args.key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
