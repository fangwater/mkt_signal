#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将 Funding Rate 交易对列表同步到 Redis 并打印（按 key_suffix 维度）。

根据 open/hedge 生成 key_suffix 并写入 Redis key（String 类型，JSON 数组）：
  - fr_dump_symbols:{key_suffix}          - 平仓列表
  - fr_fwd_trade_symbols:{key_suffix}     - 正套建仓列表
  - fr_bwd_trade_symbols:{key_suffix}     - 反套建仓列表

其中 key_suffix 为 "<open_venue>_<hedge_venue>"（例如 gate-margin_gate-futures）。

示例：
  python scripts/sync_fr_symbol_lists.py --open-venue binance-margin --hedge-venue binance-futures
  python scripts/sync_fr_symbol_lists.py --exchange binance
  python scripts/sync_fr_symbol_lists.py       # 在目录名包含 binance/okex/bybit/... 前缀时自动推断
  python scripts/sync_fr_symbol_lists.py --exchange okex
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, List, Optional

# 支持的交易所
SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]

# 目录推断用的默认 open/hedge 组合
EXCHANGE_DEFAULTS = {
    "binance": ("binance-margin", "binance-futures"),
    "okex": ("okex-margin", "okex-futures"),
    "bybit": ("bybit-margin", "bybit-futures"),
    "bitget": ("bitget-margin", "bitget-futures"),
    "gate": ("gate-margin", "gate-futures"),
}


def try_import_redis():
    try:
        import redis  # type: ignore
        return redis
    except Exception:
        return None


def infer_exchange_from_cwd() -> Optional[str]:
    """从当前目录名推断 exchange（如 binance_fr_trade -> binance）"""
    from pathlib import Path

    name = Path.cwd().name.lower()
    candidates = [name]
    if "_" in name:
        candidates.append(name.split("_", 1)[0])
    for cand in candidates:
        for ex in SUPPORTED_EXCHANGES:
            if cand.startswith(ex):
                return ex
    return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync Funding Rate symbol lists to Redis")
    p.add_argument("--open-venue", help="open 侧 venue（如 binance-margin）")
    p.add_argument("--hedge-venue", help="hedge 侧 venue（如 binance-futures）")
    p.add_argument(
        "--exchange",
        choices=SUPPORTED_EXCHANGES,
        help="交易所名称（可选，若未提供则尝试从目录名推断）",
    )
    return p.parse_args()


# ========== 交易对白名单配置 ==========

# 正套币对列表（替换为提供的 OKX 列表）
# 默认正套币对列表（OKX）
FWD_SYMBOLS_8H: List[str] = [
    "ONE-USDT-SWAP",
    "BICO-USDT-SWAP",
]

FWD_SYMBOLS_4H: List[str] = [
    "DEGEN-USDT-SWAP",
    "ACT-USDT-SWAP",
    "S-USDT-SWAP",
    "TRB-USDT-SWAP",
    "HYPE-USDT-SWAP",
]

FWD_SYMBOLS: List[str] = FWD_SYMBOLS_8H + FWD_SYMBOLS_4H

# 反套币对列表（替换为提供的 OKX 列表）
# 默认反套币对列表（OKX）
BWD_SYMBOLS_8H: List[str] = [
    "APT-USDT-SWAP",
    "ATOM-USDT-SWAP",
    "XTZ-USDT-SWAP",
    "ZRX-USDT-SWAP",
    "ATH-USDT-SWAP",
    "CELO-USDT-SWAP",
    "CORE-USDT-SWAP",
    "COMP-USDT-SWAP",
    "ICP-USDT-SWAP",
    "IOST-USDT-SWAP",
    "KSM-USDT-SWAP",
]

BWD_SYMBOLS_4H: List[str] = [
    "BERA-USDT-SWAP",
    "MORPHO-USDT-SWAP",
    "ENA-USDT-SWAP",
    "ZETA-USDT-SWAP",
    "KMNO-USDT-SWAP",
    "IMX-USDT-SWAP",
    "WLFI-USDT-SWAP",
    "ZK-USDT-SWAP",
    "UMA-USDT-SWAP",
    "JUP-USDT-SWAP",
    "2Z-USDT-SWAP",
    "HUMA-USDT-SWAP",
]

BWD_SYMBOLS: List[str] = BWD_SYMBOLS_8H + BWD_SYMBOLS_4H

# 合并所有交易对（用于平仓列表）
SYMBOL_ALLOWLIST: List[str] = list(set(FWD_SYMBOLS + BWD_SYMBOLS))

# Gate 专用交易对列表（USDT，下划线格式）
GATE_FWD_SYMBOLS_8H: List[str] = [
    "STX_USDT",
    "RSR_USDT",
    "VET_USDT",
    "IOTA_USDT",
    "ACH_USDT",
    "RUNE_USDT",
]

GATE_FWD_SYMBOLS_4H: List[str] = [
    "COOKIE_USDT",
    "ALCH_USDT",
    "NOT_USDT",
    "PEAQ_USDT",
    "CETUS_USDT",
    "AEVO_USDT",
    "JTO_USDT",
    "PNUT_USDT",
    "MANTA_USDT",
    "KAIA_USDT",
    "XDC_USDT",
    "CATI_USDT",
    "DOGS_USDT",
    "USTC_USDT",
    "SATS_USDT",
    "XPL_USDT",
    "VINE_USDT",
    "POPCAT_USDT",
    "METIS_USDT",
    "HUMA_USDT",
    "MOVE_USDT",
    "AKT_USDT",
]

GATE_FWD_SYMBOLS: List[str] = GATE_FWD_SYMBOLS_8H + GATE_FWD_SYMBOLS_4H

GATE_BWD_SYMBOLS_8H: List[str] = [
    "APT_USDT",
    "ATOM_USDT",
    "CORE_USDT",
    "MINA_USDT",
    "ICP_USDT",
]

GATE_BWD_SYMBOLS_4H: List[str] = [
    "BB_USDT",
    "ZORA_USDT",
    "OM_USDT",
    "SQD_USDT",
    "KERNEL_USDT",
]

GATE_BWD_SYMBOLS: List[str] = GATE_BWD_SYMBOLS_8H + GATE_BWD_SYMBOLS_4H


def make_key_suffix(open_venue: str, hedge_venue: str) -> str:
    return f"{open_venue.strip().lower()}_{hedge_venue.strip().lower()}"


def resolve_venues(args: argparse.Namespace) -> Optional[tuple[str, str]]:
    if args.open_venue and args.hedge_venue:
        return args.open_venue.strip().lower(), args.hedge_venue.strip().lower()
    if args.exchange:
        return EXCHANGE_DEFAULTS.get(args.exchange)
    inferred = infer_exchange_from_cwd()
    if inferred:
        return EXCHANGE_DEFAULTS.get(inferred)
    return None


def resolve_symbol_lists(exchange: str) -> tuple[List[str], List[str], str]:
    ex = exchange.strip().lower()
    if ex == "gate":
        return GATE_FWD_SYMBOLS, GATE_BWD_SYMBOLS, "gate"
    return FWD_SYMBOLS, BWD_SYMBOLS, ex or "default"


def sync_symbol_lists(
    rds,
    key_suffix: str,
    fwd_symbols: List[str],
    bwd_symbols: List[str],
) -> int:
    """同步交易对列表到 Redis"""
    # 1. 平仓列表（默认空）
    dump_key = f"fr_dump_symbols:{key_suffix}"
    empty_list: List[str] = []
    rds.set(dump_key, json.dumps(empty_list, ensure_ascii=False))
    print(f"✅ 已写入 {len(empty_list)} 个交易对到 '{dump_key}'（平仓列表）")

    # 2. 正套建仓列表
    fwd_key = f"fr_fwd_trade_symbols:{key_suffix}"
    rds.set(fwd_key, json.dumps(fwd_symbols, ensure_ascii=False))
    print(f"✅ 已写入 {len(fwd_symbols)} 个交易对到 '{fwd_key}'（正套）")
    total = len(fwd_symbols)
    # 3. 反套建仓列表
    bwd_key = f"fr_bwd_trade_symbols:{key_suffix}"
    rds.set(bwd_key, json.dumps(bwd_symbols, ensure_ascii=False))
    print(f"✅ 已写入 {len(bwd_symbols)} 个交易对到 '{bwd_key}'（反套）")
    total += len(bwd_symbols)

    return total


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


def print_symbol_list(rds, key: str, title: str) -> None:
    """打印单个交易对列表"""
    print(f"\n{title} ({key}):")
    symbols_json = rds.get(key)

    if not symbols_json:
        print("  ⚠️  未找到数据")
        return

    symbols_str = symbols_json.decode('utf-8', 'ignore') if isinstance(symbols_json, bytes) else str(symbols_json)

    try:
        symbols = json.loads(symbols_str)
        if isinstance(symbols, list):
            print(f"  总数: {len(symbols)}")
            # 分列打印，每行5个
            for i in range(0, len(symbols), 5):
                chunk = symbols[i:i+5]
                print("  " + "  ".join(f"{s:15}" for s in chunk))
        else:
            print(f"  格式异常: {symbols_str}")
    except Exception as e:
        print(f"  解析失败: {e}")
        print(f"  原始值: {symbols_str}")


def print_all_symbol_lists(rds, key_suffix: str) -> None:
    """打印所有交易对列表"""
    print("\n📊 交易对列表配置:")
    print("=" * 80)

    print_symbol_list(rds, f"fr_dump_symbols:{key_suffix}", f"🔴 {key_suffix} - 平仓列表")
    print_symbol_list(rds, f"fr_fwd_trade_symbols:{key_suffix}", f"🟢 {key_suffix} - 正套建仓列表")
    print_symbol_list(rds, f"fr_bwd_trade_symbols:{key_suffix}", f"🔴 {key_suffix} - 反套建仓列表")


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    venues = resolve_venues(args)
    if not venues:
        print(
            "❌ 需要 --open-venue/--hedge-venue，或 --exchange，或在目录名包含 binance/okex/bybit/bitget/gate 前缀以自动推断",
            file=sys.stderr,
        )
        return 2
    open_venue, hedge_venue = venues
    key_suffix = make_key_suffix(open_venue, hedge_venue)
    exchange = open_venue.split("-", 1)[0] if "-" in open_venue else open_venue
    fwd_symbols, bwd_symbols, source = resolve_symbol_lists(exchange)

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    print(f"🔄 开始同步 Funding Rate 交易对列表 (key_suffix={key_suffix})...")
    print("📍 Redis: 127.0.0.1:6379/0")
    print(f"📋 Symbol source: {source}")
    print()

    # 同步列表
    total = sync_symbol_lists(rds, key_suffix, fwd_symbols, bwd_symbols)
    print(f"\n✅ 共写入 {total} 个交易对条目")

    # 打印结果
    print_all_symbol_lists(rds, key_suffix)

    print("\n✅ 同步完成！")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
