#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印价差阈值配置（从 Redis 读取）。

显示内容：
  1. 价差阈值映射配置（对所有 symbol 通用）
  2. 已同步的 symbols 统计信息

读取 Redis Hash:
  `binance_spread_thresholds` - 价差阈值（每个 symbol 12个字段）

配置格式: operation -> percentile_reference
  - operation: {direction}_{operation}_{mode}
  - percentile_reference: binance_{factor}_{percentile}

示例：
  python scripts/print_spread_thresholds.py
  python scripts/print_spread_thresholds.py --redis-url redis://:pwd@127.0.0.1:6379/0
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List


def try_import_redis():
    try:
        import redis  # type: ignore
        return redis
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print spread thresholds configuration from Redis")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    return p.parse_args()


# ========== 价差阈值映射配置 ==========
#
# 此配置对所有 symbol 通用，定义每个操作使用哪个百分位
# 格式: "binance_{factor}_{percentile}"
#   - factor: bidask, askbid, spread
#   - percentile: 5, 10, 15, 85, 90, 95

SPREAD_THRESHOLD_MAPPING = {
    "forward_open_mm": "binance_spread_10",
    "forward_open_mt": "binance_spread_15",
    "forward_cancel_mm": "binance_spread_85",
    "forward_cancel_mt": "binance_spread_90",
    "forward_close_mm": "binance_spread_85",
    "forward_close_mt": "binance_spread_90",
    "backward_open_mm": "binance_spread_10",
    "backward_open_mt": "binance_spread_15",
    "backward_cancel_mm": "binance_spread_85",
    "backward_cancel_mt": "binance_spread_90",
    "backward_close_mm": "binance_spread_85",
    "backward_close_mt": "binance_spread_90",
}

THRESHOLD_ORDER = list(SPREAD_THRESHOLD_MAPPING.keys())


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


def print_thresholds(rds) -> None:
    """打印价差阈值配置"""
    print("📊 价差阈值配置 (对所有 symbol 通用):")
    print("-" * 80)

    # 打印配置映射表
    headers = ["operation", "percentile_reference"]
    rows: List[List[str]] = []

    for operation in THRESHOLD_ORDER:
        percentile_ref = SPREAD_THRESHOLD_MAPPING.get(operation, "-")
        rows.append([operation, percentile_ref])

    print_three_line_table(headers, rows)

    # 读取 Redis 数据并统计
    key = "binance_spread_thresholds"
    data = rds.hgetall(key)

    if not data:
        print("\n⚠️  Redis 中未找到阈值数据")
        return

    # 解码数据
    kv: Dict[str, str] = {}
    for k, v in data.items():
        kk = k.decode('utf-8', 'ignore') if isinstance(k, bytes) else str(k)
        vv = v.decode('utf-8', 'ignore') if isinstance(v, bytes) else str(v)
        kv[kk] = vv

    # 统计 symbol
    all_symbols = set()
    for field_key in kv:
        parts = field_key.split("_")
        if len(parts) >= 4:
            symbol = "_".join(parts[:-3])
            all_symbols.add(symbol)

    print(f"\n📈 统计:")
    print(f"   - 已同步 symbols: {len(all_symbols)} 个")
    print(f"   - 阈值字段总数: {len(kv)} 个")
    if all_symbols:
        print(f"   - Symbols 列表: {', '.join(sorted(all_symbols))}")

    # 打印每个 symbol 的具体阈值值
    if all_symbols:
        print(f"\n📊 各 Symbol 具体阈值:")
        print("-" * 80)

        for symbol in sorted(all_symbols):
            print(f"\n🔹 {symbol}:")

            # 准备值表
            value_headers = ["operation", "threshold_value"]
            value_rows: List[List[str]] = []

            for operation in THRESHOLD_ORDER:
                field_key = f"{symbol}_{operation}"
                value = kv.get(field_key, "-")
                value_rows.append([operation, value])

            print_three_line_table(value_headers, value_rows)


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

    # 打印阈值配置
    print_thresholds(rds)

    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
