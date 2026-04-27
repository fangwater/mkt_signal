#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Deprecated: MM no longer reads return_model_score_thresholds_*."""

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
    _ = parse_args()
    print("MM return_model_score_thresholds_* 已废弃；cancel 改用 strategy params 中的 return score quantile 配置。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
