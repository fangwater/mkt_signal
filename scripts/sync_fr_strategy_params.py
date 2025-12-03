#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将 Funding Rate 策略参数同步到 Redis 并打印。

写入 Redis Hash:
  `fr_strategy_params` - 策略参数（mode, order_amount等）

注意：交易对列表请使用 sync_fr_symbol_lists.py 脚本单独同步。

同步完成后自动打印所有参数。

示例：
  python scripts/sync_fr_strategy_params.py
  python scripts/sync_fr_strategy_params.py --redis-url redis://:pwd@127.0.0.1:6379/0
"""

from __future__ import annotations

import argparse
import json
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
    p = argparse.ArgumentParser(description="Sync Funding Rate strategy params to Redis")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    return p.parse_args()


# ========== 策略参数配置 ==========

# Hash key: fr_strategy_params
STRATEGY_PARAMS = {
    # 做市模式：MM（Maker-Maker）或 MT（Maker-Taker）
    "mode": "MM",

    # 单笔下单量（USDT）
    "order_amount": "100.0",

    # 开仓挂单档位（JSON 数组）
    "price_offsets": "[0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.0012, 0.0014, 0.0016, 0.0018, 0.002]",
    
    # 开仓订单超时（秒）
    "open_order_timeout": "150",

    # 对冲订单超时（秒）
    "hedge_timeout": "60",

    # 对冲价格偏移（万分之几）
    "hedge_price_offset": "0.001",

    # 信号冷却时间（秒）
    "signal_cooldown": "5",
}

# ========== 参数注释（用于打印） ==========

PARAM_COMMENTS: Dict[str, str] = {
    "mode": "做市模式(MM=双边挂单/MT=吃单对冲)",
    "order_amount": "单笔下单量(USDT)",
    "price_offsets": "开仓挂单档位(JSON数组)",
    "open_order_timeout": "开仓订单超时(秒)",
    "hedge_timeout": "对冲订单超时(秒)",
    "hedge_price_offset": "对冲价格偏移(万分之几)",
    "signal_cooldown": "信号冷却时间(秒)",
}


def sync_strategy_params(rds) -> int:
    """同步策略参数到 Redis Hash"""
    key = "fr_strategy_params"
    rds.hset(key, mapping=STRATEGY_PARAMS)
    print(f"✅ 已写入 {len(STRATEGY_PARAMS)} 个参数到 HASH '{key}'")
    return len(STRATEGY_PARAMS)


def format_number(val: float) -> str:
    """格式化数字，去除尾部0"""
    s = f"{val:.8f}"
    s = s.rstrip("0").rstrip(".")
    if s == "-0":
        s = "0"
    return s


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


def print_strategy_params(rds) -> None:
    """打印策略参数"""
    print("\n📊 策略参数 (fr_strategy_params):")
    print("-" * 80)

    key = "fr_strategy_params"
    data = rds.hgetall(key)

    if not data:
        print("⚠️  未找到参数或 HASH 为空")
        return

    # 解码数据
    kv: Dict[str, str] = {}
    for k, v in data.items():
        kk = k.decode('utf-8', 'ignore') if isinstance(k, bytes) else str(k)
        vv = v.decode('utf-8', 'ignore') if isinstance(v, bytes) else str(v)
        kv[kk] = vv

    # 构建表格行
    headers = ["param", "value", "comment"]
    rows: List[List[str]] = []

    # 按照定义顺序输出
    for param_key in STRATEGY_PARAMS.keys():
        value = kv.get(param_key, "-")
        comment = PARAM_COMMENTS.get(param_key, "-")
        rows.append([param_key, value, comment])

    # 输出额外的参数（如果有）
    for k in sorted(kv.keys()):
        if k not in STRATEGY_PARAMS:
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

    print("🔄 开始同步 Funding Rate 策略参数...")
    print(f"📍 Redis: {args.host}:{args.port}/{args.db}")
    print()

    # 同步参数
    sync_strategy_params(rds)

    # 打印结果
    print_strategy_params(rds)

    print("\n✅ 同步完成！")
    print("\n💡 提示：如需同步交易对列表，请运行: python scripts/sync_fr_symbol_lists.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
