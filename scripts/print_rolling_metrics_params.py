#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 Redis HASH `rolling_metrics_params`，参考 `print_binance_forward_arb_params.py` 的三线表输出。

字段示例：
  MAX_LENGTH, ROLLING_WINDOW, MIN_PERIODS,
  bidask_lower_quantile, bidask_upper_quantile,
  askbid_lower_quantile, askbid_upper_quantile,
  refresh_sec, reload_param_sec, output_hash_key.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, List, Tuple


def try_import_redis():
    try:
        import redis  # type: ignore
        return redis
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Print Redis hash rolling_metrics_params as a three-line table"
    )
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    p.add_argument("--key", default="rolling_metrics_params")
    p.add_argument(
        "--prefix",
        help="只打印指定前缀的参数，例如 --prefix bidask_",
    )
    return p.parse_args()


def read_hash(rds, key: str) -> Dict[str, str]:
    data = rds.hgetall(key)
    out: Dict[str, str] = {}
    for k, v in data.items():
        kk = k.decode("utf-8", "ignore") if isinstance(k, bytes) else str(k)
        vv = v.decode("utf-8", "ignore") if isinstance(v, bytes) else str(v)
        out[kk] = vv
    return out


PARAM_COMMENTS: Dict[str, str] = {
    "MAX_LENGTH": "环形缓冲最大容量",
    "ROLLING_WINDOW": "分位点计算使用的样本数",
    "MIN_PERIODS": "触发计算的最小样本数",
    "bidask_lower_quantile": "低于阈值→现货开多 / 合约开空（开仓）",
    "bidask_upper_quantile": "高于阈值→现货开空 / 合约开多（平仓）",
    "askbid_upper_quantile": "高于阈值→现货开空 / 合约开多（开仓）",
    "askbid_lower_quantile": "低于阈值→现货开多 / 合约开空（平仓）",
    "refresh_sec": "计算线程周期（秒）",
    "reload_param_sec": "配置重载周期（秒）",
    "output_hash_key": "结果写入的 Redis HASH",
}


def build_rows(kv: Dict[str, str], prefix: str | None) -> Tuple[List[str], List[List[str]]]:
    headers = ["param", "value", "comment"]
    rows: List[List[str]] = []
    for k in sorted(kv.keys()):
        if prefix and not k.startswith(prefix):
            continue
        v = kv[k]
        try:
            parsed = json.loads(v)
            if isinstance(parsed, (int, float)):
                v = str(parsed)
            elif isinstance(parsed, bool):
                v = "true" if parsed else "false"
            elif parsed is None:
                v = "null"
            elif isinstance(parsed, (list, dict)):
                v = json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))
            elif isinstance(parsed, str):
                v = parsed
        except Exception:
            pass
        comment = PARAM_COMMENTS.get(k, "-")
        rows.append([k, v, comment])
    return headers, rows


def compute_col_widths(headers: List[str], rows: List[List[str]]) -> List[int]:
    ncols = len(headers)
    widths = [len(h) for h in headers]
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(cell))
    return widths


def print_three_line_table(headers: List[str], rows: List[List[str]]) -> None:
    widths = compute_col_widths(headers, rows)

    def fmt_row(values: List[str]) -> str:
        parts: List[str] = []
        for i, v in enumerate(values):
            parts.append(v.ljust(widths[i]))
        return "  ".join(parts)

    header_line = fmt_row(headers)
    top_rule = "=" * len(header_line)
    mid_rule = "-" * len(header_line)
    bot_rule = "=" * len(header_line)

    print(top_rule)
    print(header_line)
    print(mid_rule)
    for r in rows:
        print(fmt_row(r))
    print(bot_rule)


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

    headers, rows = build_rows(kv, args.prefix)
    print_three_line_table(headers, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
