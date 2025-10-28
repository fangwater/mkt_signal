#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
同步 rolling_metrics 服务所需的参数到 Redis HASH。

默认写入 HASH `rolling_metrics_params`（可用 --key 修改），字段包括：
  - MAX_LENGTH：环形缓冲容量（条数）
  - refresh_sec：分位重算周期（秒）
  - reload_param_sec：配置热更新周期（秒）
  - output_hash_key：写入结果的 Redis HASH 名称
  - factors：因子配置对象，键为因子名，值包含采样周期 / 滑窗 / 最小样本 / quantiles。
    示例：
    {
      "bidask": {"resample_interval_ms": 1000, "rolling_window": 100000,
                 "min_periods": 90000, "quantiles": [5, 70]},
      "spread": {"resample_interval_ms": 10000, "rolling_window": 60000,
                 "min_periods": 40000, "quantiles": [30, 95]}
    }

示例：
  python scripts/sync_rolling_metrics_params.py
  python scripts/sync_rolling_metrics_params.py --redis-url redis://:pwd@127.0.0.1:6379/0
  python scripts/sync_rolling_metrics_params.py --max-length 200000
  python scripts/sync_rolling_metrics_params.py --factors-json '
    {"bidask":{"resample_interval_ms":1000,"rolling_window":100000,
    "min_periods":90000,"quantiles":[5,70]}}'
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from typing import Any, Dict

DEFAULTS = {
    "MAX_LENGTH": 150_000,
    "refresh_sec": 30,
    "reload_param_sec": 3,
    "output_hash_key": "rolling_metrics_thresholds",
    "factors": {
        "bidask": {
            "resample_interval_ms": 1_000,
            "rolling_window": 100_000,
            "min_periods": 1,
            "quantiles": [5, 70],
        },
        "askbid": {
            "resample_interval_ms": 1_000,
            "rolling_window": 100_000,
            "min_periods": 1,
            "quantiles": [30, 95],
        },
        "mid_spot": {
            "resample_interval_ms": 1_000,
            "rolling_window": 100_000,
            "min_periods": 1,
            "quantiles": [30, 95],
        },
        "mid_swap": {
            "resample_interval_ms": 1_000,
            "rolling_window": 100_000,
            "min_periods": 1,
            "quantiles": [30, 95],
        },
        "spread": {
            "resample_interval_ms": 1_000,
            "rolling_window": 100_000,
            "min_periods": 1,
            "quantiles": [30, 95],
        },
    },
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
    p.add_argument("--refresh-sec", type=int)
    p.add_argument("--reload-param-sec", type=int)
    p.add_argument("--output-hash-key")
    p.add_argument(
        "--factors-json",
        help=(
            "自定义因子配置，JSON 对象。键为因子名，值需包含 "
            "resample_interval_ms、rolling_window、min_periods、quantiles 等字段。"
        ),
    )
    p.add_argument("--dry-run", action="store_true", help="Only print payload without writing")
    return p.parse_args()


DEPRECATED_FIELDS = [
    "bidask_lower_quantile",
    "bidask_upper_quantile",
    "askbid_lower_quantile",
    "askbid_upper_quantile",
]


def clone_defaults() -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    for k, v in DEFAULTS.items():
        if isinstance(v, (list, dict)):
            payload[k] = clone_value(v)
        else:
            payload[k] = v
    return payload


def clone_value(value: Any) -> Any:
    if isinstance(value, list):
        return [clone_value(v) for v in value]
    if isinstance(value, dict):
        return {k: clone_value(v) for k, v in value.items()}
    return value


def value_to_str(value: Any) -> str:
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return str(value)


def build_payload(args: argparse.Namespace) -> Dict[str, str]:
    payload = clone_defaults()
    if args.max_length is not None:
        payload["MAX_LENGTH"] = args.max_length
    if args.refresh_sec is not None:
        payload["refresh_sec"] = args.refresh_sec
    if args.reload_param_sec is not None:
        payload["reload_param_sec"] = args.reload_param_sec
    if args.output_hash_key:
        payload["output_hash_key"] = args.output_hash_key
    if args.factors_json:
        try:
            raw_factors = json.loads(args.factors_json)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"解析 --factors-json 失败: {exc}") from exc
        if not isinstance(raw_factors, dict) or not raw_factors:
            raise SystemExit("--factors-json 需为非空对象")
        payload["factors"] = validate_factors(raw_factors)
    return {k: value_to_str(v) for k, v in payload.items()}


def validate_factors(factors: Dict[str, Any]) -> Dict[str, Any]:
    cleaned: Dict[str, Any] = {}
    for factor_name, cfg in factors.items():
        if not isinstance(cfg, dict):
            raise SystemExit(f"factors.{factor_name} 必须是对象")
        cleaned_cfg: Dict[str, Any] = {}
        for key in ("resample_interval_ms", "rolling_window", "min_periods"):
            if key not in cfg:
                raise SystemExit(f"factors.{factor_name} 缺少 {key}")
            try:
                value = int(cfg[key])
            except Exception as exc:  # noqa: PIE786
                raise SystemExit(
                    f"factors.{factor_name}.{key} 需为整数: {exc}"
                ) from exc
            if value <= 0:
                raise SystemExit(f"factors.{factor_name}.{key} 需为正数")
            cleaned_cfg[key] = value
        quantiles_raw = cfg.get("quantiles", [])
        if quantiles_raw is None:
            quantiles = []
        else:
            if not isinstance(quantiles_raw, list):
                raise SystemExit(f"factors.{factor_name}.quantiles 需为数组")
            quantiles = []
            for q in quantiles_raw:
                try:
                    num = float(q)
                except Exception as exc:  # noqa: PIE786
                    raise SystemExit(
                        f"factors.{factor_name}.quantiles 包含非数值: {exc}"
                    ) from exc
                if not math.isfinite(num):
                    raise SystemExit(f"factors.{factor_name}.quantiles 存在无效值")
                if abs(num - round(num)) < 1e-6:
                    quantiles.append(int(round(num)))
                else:
                    quantiles.append(num)
        cleaned_cfg["quantiles"] = quantiles
        cleaned[factor_name] = cleaned_cfg
    return cleaned


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
    deprecated = list(DEPRECATED_FIELDS)
    if args.dry_run:
        print("dry-run: 即将写入的字段：")
        for k, v in payload.items():
            print(f"  {k} = {v}")
        if deprecated:
            print("dry-run: 将移除旧字段：", ", ".join(deprecated))
        return 0

    pipe = rds.pipeline()
    if deprecated:
        pipe.hdel(args.key, *deprecated)
    pipe.hset(args.key, mapping=payload)
    pipe.execute()
    print(f"已写入 {len(payload)} 个字段到 HASH {args.key}")
    if deprecated:
        print(f"已删除旧字段：{', '.join(deprecated)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
