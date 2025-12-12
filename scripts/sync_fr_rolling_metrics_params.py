#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
同步 rolling_metrics 服务所需的参数到 Redis HASH。

写入 HASH `rolling_metrics_params_{open_venue}_{hedge_venue}`，字段包括：
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
  python scripts/sync_fr_rolling_metrics_params.py --open-venue binance-margin --hedge-venue binance-futures
  python scripts/sync_fr_rolling_metrics_params.py --open-venue okex-margin --hedge-venue okex-futures --redis-url redis://:pwd@127.0.0.1:6379/0
  python scripts/sync_fr_rolling_metrics_params.py --open-venue binance-margin --hedge-venue binance-futures --max-length 200000
  python scripts/sync_fr_rolling_metrics_params.py --open-venue okex-margin --hedge-venue okex-futures --factors-json '
    {"bidask":{"resample_interval_ms":1000,"rolling_window":100000,
    "min_periods":90000,"quantiles":[5,70]}}'
  # 也可不带参数，脚本会基于当前目录名推断 exchange（形如 okex_fr_trade -> okex-margin/okex-futures）
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from typing import Any, Dict, Optional, Tuple

DEFAULTS = {
    "MAX_LENGTH": 150_000,
    "refresh_sec": 30,
    "reload_param_sec": 3,
    # output_hash_key 将在运行时根据 open/hedge 设置
    "factors": {
        "bidask": {
            "resample_interval_ms": 1_000,
            "rolling_window": 100_000,
            "min_periods": 1,
            "quantiles": [10, 15],
        },
        "askbid": {
            "resample_interval_ms": 1_000,
            "rolling_window": 100_000,
            "min_periods": 1,
            "quantiles": [85, 90],
        },
        "spread": {
            "resample_interval_ms": 1_000,
            "rolling_window": 100_000,
            "min_periods": 1,
            "quantiles": [15, 20, 25, 30],
        },
    },
}


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


def infer_venues_from_cwd() -> Optional[Tuple[str, str]]:
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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sync rolling_metrics parameters to Redis HASH（可省略 open/hedge，默认按目录名推断 margin/futures）"
    )
    p.add_argument("--open-venue", help="open 侧 venue（如 binance-margin）")
    p.add_argument("--hedge-venue", help="hedge 侧 venue（如 binance-futures）")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    p.add_argument("--max-length", type=int)
    p.add_argument("--refresh-sec", type=int)
    p.add_argument("--reload-param-sec", type=int)
    p.add_argument(
        "--output-hash-key",
        help="自定义输出 hash key（可选，默认为 rolling_metrics_thresholds_{open}_{hedge}）",
    )
    p.add_argument(
        "--factors-json",
        help=(
            "自定义因子配置，JSON 对象。键为因子名，值需包含 "
            "resample_interval_ms、rolling_window、min_periods、quantiles 等字段。"
        ),
    )
    p.add_argument("--dry-run", action="store_true", help="Only print payload without writing")
    args = p.parse_args()

    open_venue = args.open_venue
    hedge_venue = args.hedge_venue

    if not open_venue and not hedge_venue:
        if inferred := infer_venues_from_cwd():
            open_venue, hedge_venue = inferred
            print(
                f"[INFO] 未提供 open/hedge，基于目录推断: open={open_venue}, hedge={hedge_venue}"
            )
    if not open_venue or not hedge_venue:
        p.error("需要 --open-venue 与 --hedge-venue，或在目录名包含 <exchange> 前缀（如 okex_fr_trade）以自动推断")

    args.open_venue = open_venue
    args.hedge_venue = hedge_venue
    return args


DEPRECATED_FIELDS = [
    "bidask_lower_quantile",
    "bidask_upper_quantile",
    "askbid_lower_quantile",
    "askbid_upper_quantile",
]


def clone_defaults() -> Dict[str, Any]:
    return {
        k: clone_value(v) if isinstance(v, (list, dict)) else v
        for k, v in DEFAULTS.items()
    }


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

    # 设置 output_hash_key，默认值带上 open/hedge 后缀
    if args.output_hash_key:
        payload["output_hash_key"] = args.output_hash_key
    else:
        payload["output_hash_key"] = (
            f"rolling_metrics_thresholds_{args.open_venue}_{args.hedge_venue}"
        )

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
        quantiles = []
        if quantiles_raw is not None:
            if not isinstance(quantiles_raw, list):
                raise SystemExit(f"factors.{factor_name}.quantiles 需为数组")
            for q in quantiles_raw:
                try:
                    num = float(q)
                except Exception as exc:  # noqa: PIE786
                    raise SystemExit(
                        f"factors.{factor_name}.quantiles 包含非数值: {exc}"
                    ) from exc
                if not math.isfinite(num):
                    raise SystemExit(f"factors.{factor_name}.quantiles 存在无效值")
                quantiles.append(int(round(num)) if abs(num - round(num)) < 1e-6 else num)
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

    open_venue = args.open_venue.strip()
    hedge_venue = args.hedge_venue.strip()
    if not open_venue or not hedge_venue:
        print("open-venue 和 hedge-venue 均不能为空", file=sys.stderr)
        return 1
    args.open_venue = open_venue  # normalize for downstream use
    args.hedge_venue = hedge_venue

    # 根据 open/hedge 生成 hash key
    hash_key = f"rolling_metrics_params_{open_venue}_{hedge_venue}"

    payload = build_payload(args)
    deprecated = list(DEPRECATED_FIELDS)

    if args.dry_run:
        print(f"dry-run: 目标 HASH key: {hash_key}")
        print("dry-run: 即将写入的字段：")
        for k, v in payload.items():
            print(f"  {k} = {v}")
        if deprecated:
            print("dry-run: 将移除旧字段：", ", ".join(deprecated))
        return 0

    pipe = rds.pipeline()
    if deprecated:
        pipe.hdel(hash_key, *deprecated)
    pipe.hset(hash_key, mapping=payload)
    pipe.execute()

    print(f"✅ 已写入 {len(payload)} 个字段到 HASH '{hash_key}'")
    if deprecated:
        print(f"已删除旧字段：{', '.join(deprecated)}")
    print(f"\n💡 下一步：")
    print(
        "  - 查看配置: python scripts/print_fr_rolling_metrics_params.py "
        f"--open-venue {open_venue} --hedge-venue {hedge_venue}"
    )
    print(
        "  - 启动服务: cargo run --bin rolling_metrics "
        f"-- --open-venue {open_venue} --hedge-venue {hedge_venue}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
