#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将 cross（跨所）交易对列表同步到 Redis 并打印（按跨所 pair 维度）。

写入 4 个 Redis key（String 类型，JSON 数组）：
  - cross_dump_symbols:{key_suffix}          - 平仓列表
  - cross_fwd_trade_symbols:{key_suffix}     - 正套建仓列表
  - cross_bwd_trade_symbols:{key_suffix}     - 反套建仓列表
  - {env_name}:cross_unimmr_close_symbols:{open_venue}_{hedge_venue}
                                                - UniMMR 算法平仓候选列表

其中 key_suffix 为 "<open_exchange>-<hedge_exchange>"（例如 okex-binance）。

注意：脚本内置列表为空的 key 不会写入（保留 Redis 线上现有值；key 缺失
对消费端等价空列表）。dump/unimmr_close 属运行时管理列表，请通过
cross_config_server 的 /api/symbol-lists 维护（含清空）。

推断规则（优先级从高到低）：
  1) --open-venue/--hedge-venue（例如 okex-futures / binance-futures）
  2) --env-name（例如 okex-binance-cross-trade）
  3) CWD 目录名（例如 okex-binance-cross-trade）
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from typing import List, Optional, Tuple

SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]
NAMESPACE = "cross"


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def normalize_exchange(ex: str) -> str:
    ex = (ex or "").strip().lower()
    if ex == "okx":
        ex = "okex"
    return ex


def exchange_from_venue(venue: str) -> Optional[str]:
    v = (venue or "").strip().lower()
    if not v:
        return None
    ex = v.split("-", 1)[0]
    ex = normalize_exchange(ex)
    if ex not in SUPPORTED_EXCHANGES:
        return None
    return ex


def infer_pair_from_name(name: str) -> Optional[Tuple[str, str]]:
    n = (name or "").strip().lower()
    m = re.match(r"^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross([_-].*)?$", n)
    if not m:
        return None
    open_ex = normalize_exchange(m.group(1))
    hedge_ex = normalize_exchange(m.group(2))
    if open_ex not in SUPPORTED_EXCHANGES or hedge_ex not in SUPPORTED_EXCHANGES:
        return None
    return open_ex, hedge_ex


def infer_pair_from_cwd() -> Optional[Tuple[str, str]]:
    from pathlib import Path

    return infer_pair_from_name(Path.cwd().name)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync cross symbol lists to Redis")
    p.add_argument("--open-venue", help="开仓 venue（例如 okex-futures）")
    p.add_argument("--hedge-venue", help="对冲 venue（例如 binance-futures）")
    p.add_argument("--env-name", help="环境目录名（例如 okex-binance-cross-trade）")
    return p.parse_args()


# ========== 交易对白名单配置 ==========

# 运行时管理列表：默认为空表示"不触碰线上值"，而非"清空"。
# 如需清空请通过 cross_config_server /api/symbol-lists 显式保存 []。
DUMP_SYMBOLS: List[str] = []
UNIMMR_CLOSE_SYMBOLS: List[str] = []
FWD_SYMBOLS: List[str] = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "AAVEUSDT",
    "ADAUSDT",
    "ARBUSDT",
    "ATOMUSDT",
    "AVAXUSDT",
    "BCHUSDT",
    "BNBUSDT",
    "DOGEUSDT",
    "DOTUSDT",
    "ETCUSDT",
    "FILUSDT",
    "HBARUSDT",
    "LINKUSDT",
    "LTCUSDT",
    "ONDOUSDT",
    "TONUSDT",
    "TRUMPUSDT",
    "TRXUSDT",
    "WLDUSDT",
    "XLMUSDT",
    "XRPUSDT",
]
BWD_SYMBOLS: List[str] = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "AAVEUSDT",
    "ADAUSDT",
    "ARBUSDT",
    "ATOMUSDT",
    "AVAXUSDT",
    "BCHUSDT",
    "BNBUSDT",
    "DOGEUSDT",
    "DOTUSDT",
    "ETCUSDT",
    "FILUSDT",
    "HBARUSDT",
    "LINKUSDT",
    "LTCUSDT",
    "ONDOUSDT",
    "TONUSDT",
    "TRUMPUSDT",
    "TRXUSDT",
    "WLDUSDT",
    "XLMUSDT",
    "XRPUSDT",
]


def resolve_key_suffix(args: argparse.Namespace) -> Optional[str]:
    if args.open_venue and args.hedge_venue:
        open_ex = exchange_from_venue(args.open_venue)
        hedge_ex = exchange_from_venue(args.hedge_venue)
        if not open_ex or not hedge_ex:
            return None
        return f"{open_ex}-{hedge_ex}"

    if args.env_name:
        pair = infer_pair_from_name(args.env_name)
        if pair:
            return f"{pair[0]}-{pair[1]}"

    pair = infer_pair_from_cwd()
    if pair:
        return f"{pair[0]}-{pair[1]}"

    return None


def resolve_env_name(args: argparse.Namespace) -> str:
    if args.env_name:
        return args.env_name.strip().lower()
    from pathlib import Path

    return Path.cwd().name.strip().lower()


def resolve_venues(args: argparse.Namespace, key_suffix: str) -> Optional[Tuple[str, str]]:
    if args.open_venue and args.hedge_venue:
        return args.open_venue.strip().lower(), args.hedge_venue.strip().lower()

    pair = infer_pair_from_name(args.env_name or "")
    if not pair:
        pair = infer_pair_from_cwd()
    if pair:
        return f"{pair[0]}-futures", f"{pair[1]}-futures"

    parts = key_suffix.split("-", 1)
    if len(parts) == 2:
        return f"{parts[0]}-futures", f"{parts[1]}-futures"
    return None


def unimmr_close_key(env_name: str, open_venue: str, hedge_venue: str) -> str:
    suffix = f"{open_venue.strip().lower()}_{hedge_venue.strip().lower()}"
    return f"{env_name}:cross_unimmr_close_symbols:{suffix}" if env_name else f"cross_unimmr_close_symbols:{suffix}"


def print_symbol_list(rds, key: str, title: str) -> None:
    print(f"\n{title} ({key}):")
    data = rds.get(key)
    if not data:
        print("  ⚠️  未找到数据")
        return
    text = data.decode("utf-8", "ignore") if isinstance(data, bytes) else str(data)
    try:
        symbols = json.loads(text)
        if isinstance(symbols, list):
            print(f"  总数: {len(symbols)}")
            for i in range(0, len(symbols), 5):
                chunk = symbols[i : i + 5]
                print("  " + "  ".join(f"{s:15}" for s in chunk))
        else:
            print(f"  格式异常: {text}")
    except Exception as exc:
        print(f"  解析失败: {exc}")
        print(f"  原始值: {text}")


def normalize_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper()


def validate_symbol_partition() -> bool:
    dump_set = {normalize_symbol(symbol) for symbol in DUMP_SYMBOLS if normalize_symbol(symbol)}
    fwd_set = {normalize_symbol(symbol) for symbol in FWD_SYMBOLS if normalize_symbol(symbol)}
    bwd_set = {normalize_symbol(symbol) for symbol in BWD_SYMBOLS if normalize_symbol(symbol)}

    conflicts = sorted(dump_set & (fwd_set | bwd_set))
    if conflicts:
        print(
            "❌ 配置错误：以下交易对同时出现在 dump 与 fwd/bwd 列表中（只允许二选一）:",
            file=sys.stderr,
        )
        print("   " + ", ".join(conflicts), file=sys.stderr)
        return False

    return True


def write_or_keep_symbol_list(rds, key: str, symbols: List[str], label: str) -> int:
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


def sync_symbol_lists(rds, key_suffix: str, env_name: str, open_venue: str, hedge_venue: str) -> int:
    """同步交易对列表到 Redis（内置列表为空的 key 跳过，保留线上值）"""
    dump_key = f"{NAMESPACE}_dump_symbols:{key_suffix}"
    fwd_key = f"{NAMESPACE}_fwd_trade_symbols:{key_suffix}"
    bwd_key = f"{NAMESPACE}_bwd_trade_symbols:{key_suffix}"
    unimmr_key = unimmr_close_key(env_name, open_venue, hedge_venue)

    total = 0
    total += write_or_keep_symbol_list(rds, dump_key, DUMP_SYMBOLS, "平仓列表")
    total += write_or_keep_symbol_list(rds, fwd_key, FWD_SYMBOLS, "正套")
    total += write_or_keep_symbol_list(rds, bwd_key, BWD_SYMBOLS, "反套")
    total += write_or_keep_symbol_list(rds, unimmr_key, UNIMMR_CLOSE_SYMBOLS, "UniMMR 平仓候选")
    return total


def main() -> int:
    args = parse_args()
    if not validate_symbol_partition():
        return 2

    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    key_suffix = resolve_key_suffix(args)
    if not key_suffix:
        print(
            "❌ 需要 --open-venue/--hedge-venue 或 --env-name，或在目录名包含 '<open>-<hedge>-cross-...' 以自动推断",
            file=sys.stderr,
        )
        return 2
    venues = resolve_venues(args, key_suffix)
    if not venues:
        print("❌ 无法推断 open/hedge venue", file=sys.stderr)
        return 2
    open_venue, hedge_venue = venues
    env_name = resolve_env_name(args)

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    print(f"🔄 开始同步 cross 交易对列表 (key_suffix={key_suffix})...")
    print(f"📦 Env: {env_name or '-'}")
    print("📍 Redis: 127.0.0.1:6379/0")
    print()

    total = sync_symbol_lists(rds, key_suffix, env_name, open_venue, hedge_venue)
    print(f"\n✅ 共写入 {total} 个交易对条目")

    print("\n📊 cross 交易对列表配置:")
    print("=" * 80)
    print_symbol_list(rds, f"{NAMESPACE}_dump_symbols:{key_suffix}", "🔴 dump_symbols")
    print_symbol_list(rds, f"{NAMESPACE}_fwd_trade_symbols:{key_suffix}", "🟢 fwd_trade_symbols")
    print_symbol_list(rds, f"{NAMESPACE}_bwd_trade_symbols:{key_suffix}", "🔴 bwd_trade_symbols")
    print_symbol_list(rds, unimmr_close_key(env_name, open_venue, hedge_venue), "🟠 unimmr_close_symbols")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
