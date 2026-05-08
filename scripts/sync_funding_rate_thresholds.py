#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将资金费率阈值同步到 Redis 并打印。

写入 Redis Hash（env-aware）:
  `<env_name>:funding_rate_thresholds_{open_venue}_{hedge_venue}`
    - env_name: 部署目录名，例如 `binance_fr_arb01`
    - 8 个字段（不区分 MM/MT）

格式: {period}_{direction}_{operation}
  - period: 8h, 4h
  - direction: forward, backward
  - operation: open, close

同步完成后自动打印所有阈值。

示例：
  # 在部署目录下（例如 ~/binance_fr_arb01）直接运行：
  python scripts/sync_funding_rate_thresholds.py
  # 或显式传 env-name / exchange：
  python scripts/sync_funding_rate_thresholds.py --env-name binance_fr_arb01
  python scripts/sync_funding_rate_thresholds.py --env-name okex_fr_trade --exchange okex
"""

from __future__ import annotations

import argparse
import os
import re
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
    p.add_argument(
        "--env-name",
        help="部署 env 名（例如 binance_fr_arb01）；未提供时使用当前目录名",
    )
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
    "8h_forward_open": "0.00006",       # 正套开仓：预测费率 > 0.006% 时开仓
    "8h_forward_close": "-0.0004",      # 正套平仓：预测费率 < -0.040% 时平仓
    "8h_backward_open": "-0.00006",     # 反套开仓：预测费率 < -0.006% 时开仓
    "8h_backward_close": "0.0004",      # 反套平仓：预测费率 > 0.040% 时平仓

    # 4小时周期（部分交易所）
    "4h_forward_open": "0.00003",       # 正套开仓：预测费率 > 0.003% 时开仓
    "4h_forward_close": "-0.0004",      # 正套平仓：预测费率 < -0.040% 时平仓
    "4h_backward_open": "-0.00003",     # 反套开仓：预测费率 < -0.003% 时开仓
    "4h_backward_close": "0.0004",      # 反套平仓：预测费率 > 0.040% 时平仓
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


VENUE_PAIRS: Dict[str, tuple] = {
    "binance": ("binance-margin", "binance-futures"),
    "okex": ("okex-margin", "okex-futures"),
    "bybit": ("bybit-margin", "bybit-futures"),
    "bitget": ("bitget-margin", "bitget-futures"),
    "gate": ("gate-margin", "gate-futures"),
}


def funding_thresholds_key(env_name: str, exchange: str) -> str:
    """生成与 Rust 端一致的资金费率阈值 Redis Key（env-aware）"""
    if exchange not in VENUE_PAIRS:
        raise ValueError(f"Unsupported exchange: {exchange}")
    open_slug, hedge_slug = VENUE_PAIRS[exchange]
    return f"{env_name}:funding_rate_thresholds_{open_slug}_{hedge_slug}"


def infer_env_name_from_cwd() -> Optional[str]:
    """从当前目录名推断部署 env 名（如 ~/binance_fr_arb01 -> binance_fr_arb01）"""
    name = Path.cwd().name.strip().lower()
    return name or None


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


def warn_if_env_name_mismatched(env_name: str, exchange: str) -> None:
    """env_name 应形如 `<exchange>_fr_<suffix>`，不一致时打印 warning。"""
    pattern = rf"^{re.escape(exchange)}_fr_[a-z0-9][a-z0-9_-]*$"
    if not re.match(pattern, env_name):
        print(
            f"[WARN] env-name '{env_name}' 不符合 {exchange}_fr_<suffix> 规范，仍然继续",
            file=sys.stderr,
        )


def sync_thresholds(rds, env_name: str, exchange: str) -> int:
    """同步资金费率阈值到 Redis Hash"""
    key = funding_thresholds_key(env_name, exchange)
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


def print_thresholds(rds, env_name: str, exchange: str) -> None:
    """打印资金费率阈值"""
    key = funding_thresholds_key(env_name, exchange)
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

    env_name = (args.env_name or infer_env_name_from_cwd() or "").strip().lower()
    if not env_name:
        print(
            "❌ 需要 --env-name，或在 <exchange>_fr_<suffix> 命名的目录下运行以自动推断",
            file=sys.stderr,
        )
        return 2
    if not args.env_name:
        print(f"[INFO] 未提供 env-name，基于目录推断: {env_name}", file=sys.stderr)
    warn_if_env_name_mismatched(env_name, exchange)

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    print(f"🔄 开始同步资金费率阈值 (env={env_name}, exchange={exchange})...")
    print("📍 Redis: 127.0.0.1:6379/0")
    print()

    # 同步阈值
    sync_thresholds(rds, env_name, exchange)

    # 打印结果
    print_thresholds(rds, env_name, exchange)

    print("\n✅ 同步完成！")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
