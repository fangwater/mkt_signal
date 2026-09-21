#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将 Funding Rate 交易对列表同步到 Redis 并打印（按 env_name + key_suffix 维度）。

根据 open/hedge 生成 key_suffix 并写入 Redis key（String 类型，JSON 数组）：
  - {env_name}:fr_dump_symbols:{key_suffix}          - 平仓列表
  - {env_name}:fr_unimmr_close_symbols:{key_suffix}  - UniMMR 算法平仓候选列表
  - {env_name}:fr_fwd_trade_symbols:{key_suffix}     - 正套建仓列表
  - {env_name}:fr_bwd_trade_symbols:{key_suffix}     - 反套建仓列表

注意：脚本内置列表为空的 key 不会写入（保留 Redis 线上现有值；key 缺失
对消费端等价空列表）。dump/unimmr_close 属运行时管理列表，请通过
fr_config_server 的 /api/symbol-lists 维护（含清空）。

其中 key_suffix 为 "<open_venue>_<hedge_venue>"（例如 gate-margin_gate-futures）。
env_name 为部署目录名，例如 `binance_fr_trade01`。

示例：
  python scripts/sync_fr_symbol_lists.py --env-name binance_fr_trade01 --exchange binance
  python scripts/sync_fr_symbol_lists.py       # 在部署目录下自动推断 env_name/exchange
  python scripts/sync_fr_symbol_lists.py --env-name okex_fr_trade --exchange okex
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
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
    p.add_argument(
        "--env-name",
        help="部署 env 名（例如 binance_fr_trade01）；未提供时使用当前目录名",
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

# 运行时管理列表：默认为空表示"不触碰线上值"，而非"清空"。
# 如需清空请通过 fr_config_server /api/symbol-lists 显式保存 []。
DUMP_SYMBOLS: List[str] = []
UNIMMR_CLOSE_SYMBOLS: List[str] = []

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


def symbol_list_key(env_name: str, list_name: str, key_suffix: str) -> str:
    return f"{env_name}:fr_{list_name}:{key_suffix}"


def infer_env_name_from_cwd() -> Optional[str]:
    name = Path.cwd().name.strip().lower()
    return name or None


def warn_if_env_name_mismatched(env_name: str, exchange: str) -> None:
    pattern = rf"^{re.escape(exchange)}_fr_[a-z0-9][a-z0-9_-]*$"
    if not re.match(pattern, env_name):
        print(
            f"[WARN] env-name '{env_name}' 不符合 {exchange}_fr_<suffix> 规范，仍然继续",
            file=sys.stderr,
        )


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


def write_or_keep_symbol_list(
    rds, key: str, symbols: List[str], label: str
) -> int:
    """内置列表非空才写入；为空时跳过，保留 Redis 线上现有值。

    dump/unimmr_close 等运行时管理列表由 config server 维护，脚本内置
    为空时无条件写 [] 会把线上配置清掉（key 缺失对消费端等价空列表）。
    返回写入的条目数（跳过时为 0）。
    """
    if not symbols:
        existing = rds.get(key)
        print(
            f"⏭️  内置{label}为空，跳过写入 '{key}'"
            + (
                "（保留线上现有值；如需清空请走 config server 显式保存 []）"
                if existing
                else "（key 不存在，等价空列表）"
            )
        )
        return 0
    rds.set(key, json.dumps(symbols, ensure_ascii=False))
    print(f"✅ 已写入 {len(symbols)} 个交易对到 '{key}'（{label}）")
    return len(symbols)


def sync_symbol_lists(
    rds,
    env_name: str,
    key_suffix: str,
    fwd_symbols: List[str],
    bwd_symbols: List[str],
) -> int:
    """同步交易对列表到 Redis（内置列表为空的 key 跳过，保留线上值）"""
    total = 0

    # 1. 平仓列表（运行时管理，内置为空则跳过）
    total += write_or_keep_symbol_list(
        rds,
        symbol_list_key(env_name, "dump_symbols", key_suffix),
        DUMP_SYMBOLS,
        "平仓列表",
    )

    # 2. UniMMR 算法平仓候选列表（运行时管理，内置为空则跳过）
    total += write_or_keep_symbol_list(
        rds,
        symbol_list_key(env_name, "unimmr_close_symbols", key_suffix),
        UNIMMR_CLOSE_SYMBOLS,
        "UniMMR 平仓候选",
    )

    # 3. 正套建仓列表
    total += write_or_keep_symbol_list(
        rds,
        symbol_list_key(env_name, "fwd_trade_symbols", key_suffix),
        fwd_symbols,
        "正套",
    )

    # 4. 反套建仓列表
    total += write_or_keep_symbol_list(
        rds,
        symbol_list_key(env_name, "bwd_trade_symbols", key_suffix),
        bwd_symbols,
        "反套",
    )

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


def print_all_symbol_lists(rds, env_name: str, key_suffix: str) -> None:
    """打印所有交易对列表"""
    print("\n📊 交易对列表配置:")
    print("=" * 80)

    print_symbol_list(
        rds,
        symbol_list_key(env_name, "dump_symbols", key_suffix),
        f"🔴 {key_suffix} - 平仓列表",
    )
    print_symbol_list(
        rds,
        symbol_list_key(env_name, "unimmr_close_symbols", key_suffix),
        f"🟠 {key_suffix} - UniMMR 平仓候选",
    )
    print_symbol_list(
        rds,
        symbol_list_key(env_name, "fwd_trade_symbols", key_suffix),
        f"🟢 {key_suffix} - 正套建仓列表",
    )
    print_symbol_list(
        rds,
        symbol_list_key(env_name, "bwd_trade_symbols", key_suffix),
        f"🔴 {key_suffix} - 反套建仓列表",
    )


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

    print(f"🔄 开始同步 Funding Rate 交易对列表 (env={env_name}, key_suffix={key_suffix})...")
    print("📍 Redis: 127.0.0.1:6379/0")
    print(f"📋 Symbol source: {source}")
    print()

    # 同步列表
    total = sync_symbol_lists(rds, env_name, key_suffix, fwd_symbols, bwd_symbols)
    print(f"\n✅ 共写入 {total} 个交易对条目")

    # 打印结果
    print_all_symbol_lists(rds, env_name, key_suffix)

    print("\n✅ 同步完成！")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
