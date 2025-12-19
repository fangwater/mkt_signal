#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
同步 rolling_metrics 服务所需的参数到 Redis HASH（xarb futures-only）。

写入 HASH `rolling_metrics_params_{open_venue}_{hedge_venue}`，字段包括：
  - MAX_LENGTH：环形缓冲容量（条数）
  - refresh_sec：分位重算周期（秒）
  - reload_param_sec：配置热更新周期（秒）
  - output_hash_key：写入结果的 Redis HASH 名称
  - factors：因子配置对象，键为因子名，值包含采样周期 / 滑窗 / 最小样本 / quantiles。

xarb 约定：
  - 目录名: <open>-<hedge>-xarb-trade（例如 okex-binance-xarb-trade）
  - 资产类型固定为 futures：open/hedge 两侧都会被设置为 <exchange>-futures

示例：
  python xarb_scripts/sync_xarb_rolling_metrics_params.py --open-venue okex-futures --hedge-venue binance-futures --redis-url redis://:pwd@127.0.0.1:6379/0
  # 也可不带参数，脚本会基于当前目录名推断（形如 okex-binance-xarb-trade -> okex-futures/binance-futures）
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from pathlib import Path
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
            "quantiles": [5, 10, 90, 95],
        },
    },
}

ALLOWED_EXCHANGES = {"binance", "okex", "bybit", "bitget", "gate"}
EXCHANGE_ALIASES = {"okx": "okex"}


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def normalize_exchange(exchange: str) -> str:
    ex = exchange.strip().lower()
    return EXCHANGE_ALIASES.get(ex, ex)


def infer_exchanges_from_cwd() -> Optional[Tuple[str, str]]:
    name = Path.cwd().name.lower()
    if "xarb" not in name:
        return None
    tokens = [t for t in re.split(r"[^a-z0-9]+", name) if t]
    found = []
    for tok in tokens:
        ex = normalize_exchange(tok)
        if ex in ALLOWED_EXCHANGES and ex not in found:
            found.append(ex)
    if len(found) >= 2 and found[0] != found[1]:
        return found[0], found[1]
    return None


def ensure_futures_venue(venue: str) -> str:
    v = venue.strip().lower()
    if not v.endswith("-futures"):
        raise SystemExit(f"xarb 只支持 futures 资产类型，venue 必须以 -futures 结尾: {venue}")
    return v


def resolve_venues(
    open_venue: str | None,
    hedge_venue: str | None,
) -> Tuple[str, str]:
    if open_venue or hedge_venue:
        if not open_venue or not hedge_venue:
            raise SystemExit("同时提供 --open-venue 与 --hedge-venue，或都不提供")
        open_v = ensure_futures_venue(open_venue)
        hedge_v = ensure_futures_venue(hedge_venue)
        if open_v == hedge_v:
            raise SystemExit(f"xarb 需要跨所：open={open_v} hedge={hedge_v}")
        return open_v, hedge_v

    inferred = infer_exchanges_from_cwd()
    if not inferred:
        raise SystemExit(
            "需要提供 --open-venue/--hedge-venue，或在目录名包含 <open>-<hedge>-xarb-... 以自动推断"
        )
    return f"{inferred[0]}-futures", f"{inferred[1]}-futures"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sync rolling_metrics parameters to Redis HASH（xarb futures-only；可省略参数，默认按目录名推断）"
    )
    p.add_argument("--open-venue", help="open 侧 venue（如 okex-futures）")
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

    open_venue, hedge_venue = resolve_venues(
        open_venue=args.open_venue,
        hedge_venue=args.hedge_venue,
    )
    args.open_venue = open_venue
    args.hedge_venue = hedge_venue
    return args


DEPRECATED_FIELDS = [
    "bidask_lower_quantile",
    "bidask_upper_quantile",
    "askbid_lower_quantile",
    "askbid_upper_quantile",
]


def clone_value(value: Any) -> Any:
    if isinstance(value, list):
        return [clone_value(v) for v in value]
    if isinstance(value, dict):
        return {k: clone_value(v) for k, v in value.items()}
    return value


def clone_defaults() -> Dict[str, Any]:
    return {
        k: clone_value(v) if isinstance(v, (list, dict)) else v
        for k, v in DEFAULTS.items()
    }


def value_to_str(value: Any) -> str:
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return str(value)


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
                raise SystemExit(f"factors.{factor_name}.{key} 需为整数: {exc}") from exc
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
    args.open_venue = open_venue
    args.hedge_venue = hedge_venue

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
    print("\n💡 下一步：")
    print(
        "  - 查看配置: python xarb_scripts/print_xarb_rolling_metrics_params.py "
        f"--open-venue {open_venue} --hedge-venue {hedge_venue}"
    )
    print(
        "  - 启动服务: cargo run --bin rolling_metrics "
        f"-- --open-venue {open_venue} --hedge-venue {hedge_venue}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
