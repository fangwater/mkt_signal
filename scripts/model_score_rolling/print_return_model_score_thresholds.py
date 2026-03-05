#!/usr/bin/env python3

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple


MODEL_RE = r"[a-zA-Z0-9][a-zA-Z0-9._-]*"
VENUE_PREFIX_PATTERN = re.compile(
    r"^([a-z0-9]+-(?:margin|futures|spot|swap|perpetual|perp))(?:$|[-_.])"
)

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


def infer_model_name_from_cwd() -> Optional[str]:
    name = Path.cwd().name.strip()
    if not name:
        return None
    if re.fullmatch(MODEL_RE, name) is None:
        return None
    return name


def infer_venue_from_model_name(model_name: str) -> str:
    lower = model_name.strip().lower()
    matched = VENUE_PREFIX_PATTERN.match(lower)
    if matched:
        return matched.group(1)

    raise SystemExit(
        "cannot infer venue from model_name prefix; expected prefix like "
        "'binance-futures-...' or 'okex-margin-...'"
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Print return-model-score thresholds by model_name"
    )
    p.add_argument("--model-name", help="model name")
    args = p.parse_args()

    model_name = (args.model_name or "").strip()
    if not model_name:
        inferred = infer_model_name_from_cwd()
        if inferred:
            model_name = inferred
            print(f"[INFO] inferred model_name from cwd: {model_name}", file=sys.stderr)

    if not model_name:
        p.error("--model-name is required (or run under a model_name directory)")

    if re.fullmatch(MODEL_RE, model_name) is None:
        p.error(f"invalid model_name: {model_name}")

    args.model_name = model_name
    return args


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
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt(row: List[str]) -> str:
        return "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row))

    top = "=" * len(fmt(headers))
    mid = "-" * len(fmt(headers))
    print(top)
    print(fmt(headers))
    print(mid)
    for row in rows:
        print(fmt(row))
    print(top)


def parse_field(field_key: str, operations: List[str]) -> Optional[Tuple[str, str]]:
    for operation in sorted(operations, key=len, reverse=True):
        suffix = f"_{operation}"
        if not field_key.endswith(suffix):
            continue
        symbol = field_key[: -len(suffix)].rstrip("_")
        if not symbol:
            return None
        return symbol, operation
    return None


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("redis package is not installed. Please run: pip install redis", file=sys.stderr)
        return 2

    model_name = args.model_name.strip()
    venue = infer_venue_from_model_name(model_name)
    threshold_key = f"return_model_score_thresholds_{venue}"

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    mapping = dict(DEFAULT_RETURN_MODEL_SCORE_MAPPING)
    operations = list(mapping.keys())

    threshold_raw = decode_map(rds.hgetall(threshold_key))

    print(f"[INFO] redis: 127.0.0.1:6379/0", file=sys.stderr)
    print(f"[INFO] venue: {venue}", file=sys.stderr)
    print(f"[INFO] threshold key: {threshold_key}", file=sys.stderr)
    print("[INFO] mapping source: built-in script constant", file=sys.stderr)
    print("", file=sys.stderr)

    print("Return Model Score Mapping:")
    map_rows = [[op, mapping.get(op, "-")] for op in operations]
    print_table(["operation", "score_ref"], map_rows)

    if not threshold_raw:
        print("\n[WARN] threshold hash is empty")
        return 0

    by_symbol: Dict[str, Dict[str, str]] = {}
    for field, value in threshold_raw.items():
        parsed = parse_field(field, operations)
        if parsed is None:
            continue
        symbol, operation = parsed
        by_symbol.setdefault(symbol, {})[operation] = value

    print("\nThresholds By Symbol:")
    print(f"symbols={len(by_symbol)} fields={len(threshold_raw)}")

    for symbol in sorted(by_symbol.keys()):
        print(f"\n[{symbol}]")
        rows = []
        for op in operations:
            rows.append([op, by_symbol[symbol].get(op, "-")])
        print_table(["operation", "threshold"], rows)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
