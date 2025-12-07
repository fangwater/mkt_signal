#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印资金费率阈值（从 Redis 读取）。

读取 Redis Hash:
  `funding_rate_thresholds_{exchange}` - 资金费率阈值（8个字段，不区分 MM/MT）

格式: {period}_{direction}_{operation}
  - period: 8h, 4h
  - direction: forward, backward
  - operation: open, close

示例：
  python scripts/print_funding_rate_thresholds.py --exchange binance
  python scripts/print_funding_rate_thresholds.py --exchange okex --redis-url redis://:pwd@127.0.0.1:6379/0
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List

# 支持的交易所
SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]


def try_import_redis():
    try:
        import redis  # type: ignore
        return redis
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print Funding Rate thresholds from Redis")
    p.add_argument("--exchange", required=True, choices=SUPPORTED_EXCHANGES,
                   help="交易所名称（必填）")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    return p.parse_args()


# ========== 阈值注释 ==========

THRESHOLD_COMMENTS: Dict[str, str] = {
    "8h_forward_open": "8h 正套开仓(预测费率>阈值)",
    "8h_forward_close": "8h 正套平仓(预测费率<阈值)",
    "8h_backward_open": "8h 反套开仓(预测费率<阈值)",
    "8h_backward_close": "8h 反套平仓(预测费率>阈值)",
    "4h_forward_open": "4h 正套开仓(预测费率>阈值)",
    "4h_forward_close": "4h 正套平仓(预测费率<阈值)",
    "4h_backward_open": "4h 反套开仓(预测费率<阈值)",
    "4h_backward_close": "4h 反套平仓(预测费率>阈值)",
}

# 阈值顺序
THRESHOLD_ORDER = [
    "8h_forward_open",
    "8h_forward_close",
    "8h_backward_open",
    "8h_backward_close",
    "4h_forward_open",
    "4h_forward_close",
    "4h_backward_open",
    "4h_backward_close",
]


def print_three_line_table(headers: List[str], rows: List[List[str]]) -> None:
    """打印三线表格"""
    # 计算列宽
    ncols = len(headers)
    widths = [0] * ncols
    for i, h in enumerate(headers):
        widths[i] = max(widths[i], len(h))
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(cell))

    # 格式化行
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


def print_thresholds(rds, exchange: str) -> None:
    """打印资金费率阈值"""
    key = f"funding_rate_thresholds_{exchange}"
    print(f"📊 资金费率阈值 ({key}):")
    print("-" * 80)

    data = rds.hgetall(key)

    if not data:
        print("⚠️  未找到阈值或 HASH 为空")
        return

    # 解码数据
    kv: Dict[str, str] = {}
    for k, v in data.items():
        kk = k.decode('utf-8', 'ignore') if isinstance(k, bytes) else str(k)
        vv = v.decode('utf-8', 'ignore') if isinstance(v, bytes) else str(v)
        kv[kk] = vv

    # 构建表格行
    headers = ["threshold", "value", "comment"]
    rows: List[List[str]] = []

    # 按照定义顺序输出
    for threshold_key in THRESHOLD_ORDER:
        if threshold_key in kv:
            value = kv[threshold_key]
            comment = THRESHOLD_COMMENTS.get(threshold_key, "-")
            rows.append([threshold_key, value, comment])

    # 输出额外的阈值（如果有）
    for k in sorted(kv.keys()):
        if k not in THRESHOLD_ORDER:
            rows.append([k, kv[k], "-"])

    print_three_line_table(headers, rows)


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    rds = redis.from_url(args.redis_url) if args.redis_url else redis.Redis(
        host=args.host, port=args.port, db=args.db, password=args.password
    )

    print(f"📍 Redis: {args.host}:{args.port}/{args.db}\n")

    # 打印阈值
    print_thresholds(rds, args.exchange)

    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
