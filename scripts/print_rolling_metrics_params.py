#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 Redis HASH `rolling_metrics_params`。

支持两种输出：
  - JSON：结构化输出 general / factors，便于进一步处理。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict


def try_import_redis():
    try:
        import redis  # type: ignore
        return redis
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Print Redis hash rolling_metrics_params as JSON"
    )
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    p.add_argument("--key", default="rolling_metrics_params")
    p.add_argument("--prefix", help="只打印指定前缀的参数，例如 --prefix bidask_")
    return p.parse_args()


def read_hash(rds, key: str) -> Dict[str, str]:
    data = rds.hgetall(key)

    def decode(obj: object) -> str:
        return obj.decode("utf-8", "ignore") if isinstance(obj, bytes) else str(obj)

    return {decode(k): decode(v) for k, v in data.items()}


def decode_value(raw: str) -> Any:
    try:
        return json.loads(raw)
    except Exception:
        return raw


def build_json_output(kv: Dict[str, str], prefix: str | None) -> Dict[str, Any]:
    result: Dict[str, Any] = {}

    general: Dict[str, Any] = {
        key: decode_value(value)
        for key, value in sorted(kv.items())
        if key != "factors"
        and not key.endswith("_lower_quantile")
        and not key.endswith("_upper_quantile")
    }
    factors_raw = kv.get("factors")
    if not factors_raw:
        result["general"] = general
        return result

    try:
        factors = json.loads(factors_raw)
    except json.JSONDecodeError as exc:
        general["factors_error"] = f"factors 解析失败: {exc}"
    else:
        if not isinstance(factors, dict):
            general["factors_error"] = "factors 需为对象"
        else:
            if prefix:
                factors = {name: cfg for name, cfg in factors.items() if name.startswith(prefix)}
            general["factors"] = factors

    result["general"] = general
    return result


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("redis 包未安装，请先 `pip install redis`。", file=sys.stderr)
        return 2
    rds = redis.from_url(args.redis_url) if args.redis_url else redis.Redis(
        host=args.host, port=args.port, db=args.db, password=args.password
    )

    kv = read_hash(rds, args.key)
    if not kv:
        print("未找到参数或 HASH 为空。")
        return 0

    data = build_json_output(kv, args.prefix)
    print(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
