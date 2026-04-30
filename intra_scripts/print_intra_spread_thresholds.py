#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 intra（同所期现）价差阈值配置（从 Redis 读取）。

读取 Redis Hash:
  `intra_spread_thresholds_{open_venue}_{hedge_venue}`

推断规则：
  - --exchange / --open-venue / --hedge-venue / --env-name / CWD
  - 同所期现：open=<exchange>-margin, hedge=<exchange>-futures
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional

SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]


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


def infer_exchange_from_name(name: str) -> Optional[str]:
    n = (name or "").strip().lower()
    m = re.match(r"^([a-z0-9]+)[-_]intra([_-].*)?$", n)
    if not m:
        return None
    ex = normalize_exchange(m.group(1))
    if ex not in SUPPORTED_EXCHANGES:
        return None
    return ex


def infer_exchange_from_cwd() -> Optional[str]:
    return infer_exchange_from_name(Path.cwd().name)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print intra spread thresholds from Redis")
    p.add_argument("--exchange", default=os.environ.get("EXCHANGE"))
    p.add_argument("--open-venue", default=os.environ.get("OPEN_VENUE"))
    p.add_argument("--hedge-venue", default=os.environ.get("HEDGE_VENUE"))
    p.add_argument("--env-name", help="环境目录名（例如 binance-intra-trade）")
    args = p.parse_args()

    exchange = args.exchange
    if not exchange:
        exchange = infer_exchange_from_name(args.env_name) if args.env_name else infer_exchange_from_cwd()
        if exchange:
            print(f"[INFO] 未提供 --exchange，基于目录推断: exchange={exchange}", file=sys.stderr)

    if exchange:
        exchange = normalize_exchange(exchange)
        if exchange not in SUPPORTED_EXCHANGES:
            p.error(f"不支持的 exchange: {exchange}")
        if not args.open_venue:
            args.open_venue = f"{exchange}-margin"
        if not args.hedge_venue:
            args.hedge_venue = f"{exchange}-futures"

    if not args.open_venue or not args.hedge_venue:
        p.error("需要 --exchange 或同时提供 --open-venue/--hedge-venue，或使用 --env-name <exchange>-intra-<tag>")

    args.open_venue = args.open_venue.lower()
    args.hedge_venue = args.hedge_venue.lower()
    return args


def print_three_line_table(headers: List[str], rows: List[List[str]]) -> None:
    widths = [len(h) for h in headers]
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(cell))

    def fmt_row(values: List[str]) -> str:
        return "  ".join(values[i].ljust(widths[i]) for i in range(len(values)))

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


def load_sync_config(rds, key: str) -> Optional[Dict]:
    raw = rds.get(key)
    if not raw:
        return None
    text = raw.decode("utf-8", "ignore") if isinstance(raw, bytes) else str(raw)
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def infer_threshold_order_from_data(field_keys: List[str]) -> List[str]:
    suffixes = set()
    for field_key in field_keys:
        parts = field_key.split("_")
        if len(parts) < 4:
            continue
        suffixes.add("_".join(parts[-3:]))
    return sorted(suffixes)


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    key = f"intra_spread_thresholds_{args.open_venue}_{args.hedge_venue}"
    config_key = f"intra_spread_thresholds_config_{args.open_venue}_{args.hedge_venue}"
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    print("📍 Redis: 127.0.0.1:6379/0")
    print(f"📊 intra 价差阈值: {key}")
    print("-" * 80)

    cfg = load_sync_config(rds, config_key)
    mapping = None
    threshold_order: List[str] = []
    if cfg:
        mapping = cfg.get("mapping") if isinstance(cfg.get("mapping"), dict) else None
        threshold_order = (
            cfg.get("threshold_order")
            if isinstance(cfg.get("threshold_order"), list)
            else list(mapping.keys()) if mapping else []
        )
        generated_at = cfg.get("generated_at")
        print(f"🧾 sync 配置: {config_key}" + (f" (generated_at={generated_at})" if generated_at else ""))

    if mapping and threshold_order:
        print("🔧 映射配置（来自 Redis sync 配置，对所有 symbol 通用）:")
        rows = [[k, str(mapping.get(k, "-"))] for k in threshold_order]
        print_three_line_table(["operation", "percentile_reference"], rows)

    data = rds.hgetall(key)
    if not data:
        print("\n⚠️  Redis 中未找到阈值数据（HASH 为空或不存在）\n")
        return 0

    kv: Dict[str, str] = {}
    for k, v in data.items():
        kk = k.decode("utf-8", "ignore") if isinstance(k, bytes) else str(k)
        vv = v.decode("utf-8", "ignore") if isinstance(v, bytes) else str(v)
        kv[kk] = vv

    scalar_kv: Dict[str, str] = {}
    json_rows: Dict[str, Dict[str, str]] = {}
    for field_key, val in kv.items():
        try:
            obj = json.loads(val)
        except Exception:
            obj = None

        if isinstance(obj, dict):
            row = {k: str(v) for k, v in obj.items()}
            if row:
                json_rows[field_key] = row
                continue

        scalar_kv[field_key] = val

    if not threshold_order:
        threshold_order = infer_threshold_order_from_data(list(scalar_kv.keys()))

    all_symbols = set()
    for field_key in scalar_kv:
        parts = field_key.split("_")
        if len(parts) >= 4:
            symbol = "_".join(parts[:-3])
            all_symbols.add(symbol)

    print("\n📈 统计:")
    if all_symbols:
        print(f"   - 已同步 symbols: {len(all_symbols)} 个")
        print(f"   - 阈值字段总数: {len(scalar_kv)} 个")
    elif json_rows:
        print(f"   - 已同步 symbols: {len(json_rows)} 个 (legacy JSON 格式)")
        print(f"   - 阈值字段总数: {len(json_rows)} 个 (每个 field=一个 symbol)")
    else:
        print(f"   - 已同步 symbols: 0 个")
        print(f"   - 阈值字段总数: {len(kv)} 个")
    if all_symbols:
        print(f"   - Symbols: {', '.join(sorted(all_symbols))}")

    if all_symbols:
        print("\n📊 各 Symbol 具体阈值:")
        for symbol in sorted(all_symbols):
            rows = []
            for op in threshold_order:
                rows.append([op, scalar_kv.get(f"{symbol}_{op}", "-")])
            print(f"\n🔹 {symbol}:")
            print_three_line_table(["operation", "threshold_value"], rows)
    elif json_rows:
        print("\n⚠️  检测到 legacy JSON 格式阈值（建议重新运行 sync_intra_spread_thresholds.py 以写入标准字段格式）")
        print("\n📊 各 Symbol 具体阈值:")
        for symbol in sorted(json_rows.keys()):
            rows = []
            row = json_rows[symbol]
            for op in threshold_order or sorted(row.keys()):
                rows.append([op, row.get(op, "-")])
            print(f"\n🔹 {symbol}:")
            print_three_line_table(["operation", "threshold_value"], rows)

    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
