#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将资金费率阈值同步到 Redis 并打印。

写入 Redis Hash:
  `funding_rate_thresholds_{open_venue}_{hedge_venue}` - 资金费率阈值（8个字段，不区分 MM/MT）

格式: {period}_{direction}_{operation}
  - period: 8h, 4h
  - direction: forward, backward
  - operation: open, close

同步完成后自动打印所有阈值。

示例：
  python scripts/sync_funding_rate_thresholds.py --exchange binance
  python scripts/sync_funding_rate_thresholds.py --exchange okex --redis-url redis://:pwd@127.0.0.1:6379/0
  # 也可省略 --exchange，脚本会基于当前目录名推断 (binance/okex/bybit/bitget/gate 前缀)
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

# 支持的交易所
SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]


def try_import_redis():
    try:
        import redis  # type: ignore
        return redis
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync Funding Rate thresholds to Redis")
    p.add_argument(
        "--exchange",
        choices=SUPPORTED_EXCHANGES,
        help="交易所名称（可选，未提供则尝试根据当前目录名推断）",
    )
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    return p.parse_args()


# ========== 资金费率阈值配置 ==========
#
# 根据资金费率的特点设置阈值：
# - 正套开仓：预测费率较高时（正费率），卖出合约、买入现货
# - 正套平仓：预测费率转负或较低时平仓
# - 反套开仓：预测费率较低时（负费率），买入合约、卖出现货
# - 反套平仓：预测费率转正或较高时平仓
#
# 8h 周期阈值较大（费率绝对值更大）
# 4h 周期阈值较小（费率绝对值更小）
FUNDING_RATE_THRESHOLDS = {
    # 8小时周期（币安默认）
    "8h_forward_open": "0.00008",       # 正套开仓：预测费率 > 0.008% 时开仓
    "8h_forward_close": "-0.0005",     # 正套平仓：预测费率 < -0.008% 时平仓
    "8h_backward_open": "-0.00008",     # 反套开仓：预测费率 < -0.005% 时开仓
    "8h_backward_close": "0.0005",     # 反套平仓：预测费率 > 0.005% 时平仓

    # 4小时周期（部分交易所）
    "4h_forward_open": "0.00004",      # 正套开仓：预测费率 > 0.004% 时开仓
    "4h_forward_close": "-0.0005",    # 正套平仓：预测费率 < -0.004% 时平仓
    "4h_backward_open": "-0.00004",    # 反套开仓：预测费率 < -0.005% 时开仓
    "4h_backward_close": "0.0005",    # 反套平仓：预测费率 > 0.005% 时平仓
} 

# ========== 阈值注释（用于打印） ==========

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


def funding_thresholds_key(exchange: str) -> str:
    """生成与 Rust 端一致的资金费率阈值 Redis Key"""
    venue_pairs = {
        "binance": ("binance-margin", "binance-futures"),
        "okex": ("okex-margin", "okex-futures"),
        "bybit": ("bybit-margin", "bybit-futures"),
        "bitget": ("bitget-margin", "bitget-futures"),
        "gate": ("gate-margin", "gate-futures"),
    }
    if exchange not in venue_pairs:
        raise ValueError(f"Unsupported exchange: {exchange}")
    open_slug, hedge_slug = venue_pairs[exchange]
    return f"funding_rate_thresholds_{open_slug}_{hedge_slug}"


def infer_exchange_from_cwd() -> Optional[str]:
    """从当前目录名推断 exchange（如 binance_fr_trade -> binance）"""
    name = Path.cwd().name.lower()
    candidates = [name]
    if "_" in name:
        candidates.append(name.split("_", 1)[0])
    for cand in candidates:
        for ex in SUPPORTED_EXCHANGES:
            if cand.startswith(ex):
                return ex
    return None


def sync_thresholds(rds, exchange: str) -> int:
    """同步资金费率阈值到 Redis Hash"""
    key = funding_thresholds_key(exchange)
    rds.hset(key, mapping=FUNDING_RATE_THRESHOLDS)
    print(f"✅ 已写入 {len(FUNDING_RATE_THRESHOLDS)} 个资金费率阈值到 HASH '{key}'")
    return len(FUNDING_RATE_THRESHOLDS)


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
    key = funding_thresholds_key(exchange)
    print(f"\n📊 资金费率阈值 ({key}):")
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

    exchange = args.exchange or infer_exchange_from_cwd()
    if not exchange:
        print(
            "❌ 需要 --exchange，或在目录名包含 binance/okex/bybit/bitget/gate 前缀以自动推断",
            file=sys.stderr,
        )
        return 2
    if not args.exchange:
        print(f"[INFO] 未提供 exchange，基于目录推断: {exchange}", file=sys.stderr)

    rds = redis.from_url(args.redis_url) if args.redis_url else redis.Redis(
        host=args.host, port=args.port, db=args.db, password=args.password
    )

    print(f"🔄 开始同步资金费率阈值 (exchange={exchange})...")
    print(f"📍 Redis: {args.host}:{args.port}/{args.db}")
    print()

    # 同步阈值
    sync_thresholds(rds, exchange)

    # 打印结果
    print_thresholds(rds, exchange)

    print("\n✅ 同步完成！")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
