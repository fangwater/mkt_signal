#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 MM return-model-score 阈值（从 Redis 读取）。

读取 Redis Hash:
  return_model_score_thresholds_{venue}

venue 由当前目录推断（如 binance_mm_beta -> binance-futures）。

示例:
  cd ~/binance_mm_beta
  python scripts/print_mm_return_score_thresholds.py
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

OPERATIONS: List[str] = [
    "forward_open",
    "forward_cancel",
    "backward_open",
    "backward_cancel",
]

DEFAULT_RETURN_MODEL_SCORE_MAPPING: Dict[str, str] = {
    "forward_open": "score_90",
    "forward_cancel": "score_80",
    "backward_open": "score_10",
    "backward_cancel": "score_20",
}


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
    name_l = (name or "").strip().lower()
    for ex in ["binance", "okex", "okx", "bybit", "bitget", "gate"]:
        if re.search(rf"(^|[^a-z0-9]){ex}([^a-z0-9]|$)", name_l):
            return normalize_exchange(ex)
    return None


def infer_exchange_from_cwd() -> Optional[str]:
    return infer_exchange_from_name(Path.cwd().name)


def resolve_venue() -> Optional[str]:
    ex = infer_exchange_from_cwd()
    if ex:
        return f"{ex}-futures"
    return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print MM return-model-score thresholds from Redis (venue inferred from cwd)"
    )
    return parser.parse_args()


def decode_map(data: Dict[object, object]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in data.items():
        kk = k.decode("utf-8", "ignore") if isinstance(k, bytes) else str(k)
        vv = v.decode("utf-8", "ignore") if isinstance(v, bytes) else str(v)
        out[kk] = vv
    return out


def print_table(headers: List[str], rows: List[List[str]]) -> None:
    widths = [len(h) for h in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def fmt(row: List[str]) -> str:
        return "  ".join(cell.ljust(widths[idx]) for idx, cell in enumerate(row))

    top = "=" * len(fmt(headers))
    mid = "-" * len(fmt(headers))
    print(top)
    print(fmt(headers))
    print(mid)
    for row in rows:
        print(fmt(row))
    print(top)


def parse_field(field_key: str) -> Optional[Tuple[str, str]]:
    for op in sorted(OPERATIONS, key=len, reverse=True):
        suffix = f"_{op}"
        if not field_key.endswith(suffix):
            continue
        symbol = field_key[: -len(suffix)].rstrip("_")
        if not symbol:
            return None
        return symbol, op
    return None


def main() -> int:
    _args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    venue = resolve_venue()
    if not venue:
        print(
            "❌ 无法从当前目录推断 venue。请在目录名包含 binance/okex/bybit/bitget/gate 前缀的 MM 目录运行（如 ~/binance_mm_beta）",
            file=sys.stderr,
        )
        return 1

    key = f"return_model_score_thresholds_{venue}"
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)
    threshold_raw = decode_map(rds.hgetall(key))

    print("📍 Redis: 127.0.0.1:6379/0")
    print(f"📍 Venue: {venue}")
    print(f"📍 Threshold Key: {key}")

    print("\nReturn Model Score Mapping:")
    mapping_rows = [[op, DEFAULT_RETURN_MODEL_SCORE_MAPPING.get(op, "-")] for op in OPERATIONS]
    print_table(["operation", "score_ref"], mapping_rows)

    if not threshold_raw:
        print("\n⚠️  threshold hash is empty")
        return 0

    by_symbol: Dict[str, Dict[str, str]] = {}
    for field, value in threshold_raw.items():
        parsed = parse_field(field)
        if parsed is None:
            continue
        symbol, op = parsed
        by_symbol.setdefault(symbol, {})[op] = value

    print("\nThresholds By Symbol:")
    print(f"symbols={len(by_symbol)} fields={len(threshold_raw)}")
    for symbol in sorted(by_symbol.keys()):
        print(f"\n[{symbol}]")
        rows = [[op, by_symbol[symbol].get(op, "-")] for op in OPERATIONS]
        print_table(["operation", "threshold"], rows)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
