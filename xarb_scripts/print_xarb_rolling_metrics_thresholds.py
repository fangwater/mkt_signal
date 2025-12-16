#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Print rolling_metrics thresholds from Redis HASH as a three-line table（xarb futures-only）。

Reads
  - Redis HASH `rolling_metrics_thresholds_{open_venue}_{hedge_venue}` where
    field = symbol_pair (e.g. "okex-futures_binance-futures::BTCUSDT")
    value = JSON payload produced by rolling_metrics service.

xarb 约定：
  - 目录名: <open>-<hedge>-xarb-trade（例如 okex-binance-xarb-trade）
  - 资产类型固定为 futures：open/hedge 两侧都会被设置为 <exchange>-futures

示例：
  python xarb_scripts/print_xarb_rolling_metrics_thresholds.py --open-venue okex-futures --hedge-venue binance-futures
  # 也可不带参数，脚本会基于当前目录名推断（形如 okex-binance-xarb-trade -> okex-futures/binance-futures）
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ALLOWED_EXCHANGES = {"binance", "okex", "bybit", "bitget", "gate"}
EXCHANGE_ALIASES = {"okx": "okex"}


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def normalize_exchange(exchange: str) -> str:
    ex = exchange.strip().lower()
    return EXCHANGE_ALIASES.get(ex, ex)


def infer_exchanges_from_cwd() -> Optional[Tuple[str, str]]:
    name = Path.cwd().name.lower()
    if "xarb" not in name:
        return None
    tokens = [t for t in re.split(r"[^a-z0-9]+", name) if t]
    found = []
    for tok in tokens:
        ex = normalize_exchange(tok)
        if ex in ALLOWED_EXCHANGES and ex not in found:
            found.append(ex)
    if len(found) >= 2 and found[0] != found[1]:
        return found[0], found[1]
    return None


def ensure_futures_venue(venue: str) -> str:
    v = venue.strip().lower()
    if not v.endswith("-futures"):
        raise SystemExit(f"xarb 只支持 futures 资产类型，venue 必须以 -futures 结尾: {venue}")
    return v


def resolve_venues(
    open_venue: str | None,
    hedge_venue: str | None,
) -> Tuple[str, str]:
    if open_venue or hedge_venue:
        if not open_venue or not hedge_venue:
            raise SystemExit("同时提供 --open-venue 与 --hedge-venue，或都不提供")
        open_v = ensure_futures_venue(open_venue)
        hedge_v = ensure_futures_venue(hedge_venue)
        if open_v == hedge_v:
            raise SystemExit(f"xarb 需要跨所：open={open_v} hedge={hedge_v}")
        return open_v, hedge_v

    inferred = infer_exchanges_from_cwd()
    if not inferred:
        raise SystemExit(
            "需要提供 --open-venue/--hedge-venue，或在目录名包含 <open>-<hedge>-xarb-... 以自动推断"
        )
    return f"{inferred[0]}-futures", f"{inferred[1]}-futures"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Print Redis hash rolling_metrics_thresholds_{open_venue}_{hedge_venue} as a three-line table（xarb futures-only；可省略参数，默认按目录名推断）"
        )
    )
    p.add_argument("--open-venue", help="open 侧 venue（如 okex-futures）")
    p.add_argument("--hedge-venue", help="hedge 侧 venue（如 binance-futures）")
    p.add_argument(
        "--redis-url",
        default=os.environ.get("REDIS_URL"),
        help="Redis URL, e.g. redis://:pwd@host:6379/0 (overrides host/port/db)",
    )
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    p.add_argument(
        "--symbol",
        help="仅打印单个 symbol（匹配 base_symbol 或字段名 / 键尾部）",
    )
    p.add_argument(
        "--symbols",
        nargs="*",
        help="If set, only include rows where base_symbol matches any provided symbol",
    )
    p.add_argument(
        "--tsfmt",
        choices=["raw", "iso"],
        default="iso",
        help="Format for update_tp column (default: iso)",
    )
    p.add_argument(
        "--na",
        default="-",
        help="Placeholder for missing/NaN values (default: '-')",
    )
    p.add_argument(
        "--sort",
        choices=["symbol_pair", "base_symbol"],
        default="base_symbol",
        help="Sort rows by this field (default: base_symbol)",
    )
    args = p.parse_args()

    open_venue, hedge_venue = resolve_venues(
        open_venue=args.open_venue,
        hedge_venue=args.hedge_venue,
    )
    args.open_venue = open_venue
    args.hedge_venue = hedge_venue
    return args


def connect_redis(args: argparse.Namespace):
    redis = try_import_redis()
    if redis is None:
        return None
    if args.redis_url:
        return redis.from_url(args.redis_url)
    return redis.Redis(
        host=args.host,
        port=args.port,
        db=args.db,
        password=args.password,
    )


def read_hash(rds, key: str) -> Dict[str, Dict]:
    result: Dict[str, Dict] = {}
    data = rds.hgetall(key)
    for k, v in data.items():
        field = k.decode("utf-8", "ignore") if isinstance(k, bytes) else str(k)
        val = v.decode("utf-8", "ignore") if isinstance(v, bytes) else str(v)
        try:
            obj = json.loads(val)
            if isinstance(obj, dict):
                result[field] = obj
        except Exception:
            continue
    return result


def is_nan(value: object) -> bool:
    return isinstance(value, float) and math.isnan(value)


def format_number(value: Optional[float], na: str) -> str:
    if value is None or is_nan(value):
        return na
    try:
        num = float(value)
        text = f"{num:.8f}".rstrip("0").rstrip(".")
        return text or "0"
    except Exception:
        return na


def format_ts(value: Optional[int], mode: str, na: str) -> str:
    if value is None:
        return na
    try:
        ts_ms = int(value)
    except Exception:
        return na
    if mode == "raw":
        return str(ts_ms)
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts_ms)


def filter_symbols(rows: Dict[str, Dict], symbols: Optional[List[str]]) -> Dict[str, Dict]:
    if not symbols:
        return rows
    symbol_set = {s.upper() for s in symbols if s}
    filtered: Dict[str, Dict] = {}
    for key, obj in rows.items():
        base_value = obj.get("base_symbol") or obj.get("symbol") or ""
        base = str(base_value).upper()
        key_upper = str(key).upper()
        key_tail = key_upper.split("::")[-1]
        if base in symbol_set or key_upper in symbol_set or key_tail in symbol_set:
            filtered[key] = obj
    return filtered


def sort_items(data: Dict[str, Dict], sort_field: str):
    if sort_field == "base_symbol":
        return sorted(
            data.items(),
            key=lambda item: (str(item[1].get("base_symbol", "")), item[0]),
        )
    return sorted(data.items(), key=lambda item: item[0])


def collect_quantile_columns(
    data: Dict[str, Dict], quantile_key: str, display_prefix: str
) -> List[str]:
    columns: Dict[str, Optional[float]] = {}
    for obj in data.values():
        entries = obj.get(quantile_key)
        if not isinstance(entries, list):
            continue
        for item in entries:
            if not isinstance(item, dict):
                continue
            q = item.get("q")
            v = item.get("v")
            if q is None:
                continue
            col = f"{display_prefix}q{q}"
            if col not in columns:
                columns[col] = None
            if isinstance(v, (int, float)) and math.isfinite(float(v)):
                columns[col] = float(v)
    def col_key(c: str) -> float:
        try:
            return float(c.split("q", 1)[1])
        except Exception:
            return 0.0
    return sorted(columns.keys(), key=col_key)


def print_three_line_table(headers: List[str], rows: List[List[str]]) -> None:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt_row(cols: List[str]) -> str:
        return " | ".join(c.ljust(widths[i]) for i, c in enumerate(cols))

    top = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    mid = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    bot = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    print(top)
    print("| " + fmt_row(headers) + " |")
    print(mid)
    for r in rows:
        print("| " + fmt_row(r) + " |")
    print(bot)


def get_factor_value(obj: Dict[str, Any], key: str) -> Optional[float]:
    raw = obj.get(key)
    if raw is None:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def build_table(
    data: Dict[str, Dict],
    *,
    factor_label: str,
    value_key: str,
    quantile_key: str,
    na: str,
    tsfmt: str,
    sort_field: str,
) -> Tuple[List[str], List[List[str]]]:
    q_cols = collect_quantile_columns(data, quantile_key=quantile_key, display_prefix="")
    headers = [
        "base_symbol",
        "symbol_pair",
        "update_tp",
        "sample_size",
        factor_label,
        *q_cols,
    ]

    rows: List[List[str]] = []
    for symbol_pair, obj in sort_items(data, sort_field=sort_field):
        base_symbol = str(obj.get("base_symbol") or obj.get("symbol") or "")
        update_tp = format_ts(obj.get("update_tp"), tsfmt, na)
        sample_size = str(obj.get("sample_size") or obj.get("n") or na)
        value = format_number(get_factor_value(obj, value_key), na)
        q_map: Dict[str, str] = {c: na for c in q_cols}
        entries = obj.get(quantile_key)
        if isinstance(entries, list):
            for item in entries:
                if not isinstance(item, dict):
                    continue
                q = item.get("q")
                v = item.get("v")
                if q is None:
                    continue
                col = f"q{q}"
                if col in q_map:
                    q_map[col] = format_number(v if isinstance(v, (int, float)) else None, na)
        rows.append([base_symbol, symbol_pair, update_tp, sample_size, value, *[q_map[c] for c in q_cols]])

    return headers, rows


def main() -> int:
    args = parse_args()
    rds = connect_redis(args)
    if rds is None:
        print("redis 包未安装，请先 `pip install redis`。", file=sys.stderr)
        return 2

    open_venue = args.open_venue.strip()
    hedge_venue = args.hedge_venue.strip()
    key = f"rolling_metrics_thresholds_{open_venue}_{hedge_venue}"

    print(f"📍 Reading from Redis hash: {key}", file=sys.stderr)
    print(f"📍 Redis: {args.host}:{args.port}/{args.db}", file=sys.stderr)
    print("", file=sys.stderr)

    data = read_hash(rds, key)
    if not data:
        print(f"⚠️  未找到阈值或 HASH '{key}' 为空。", file=sys.stderr)
        return 0

    if args.symbol:
        sym = args.symbol.upper()
        data = {
            k: v
            for k, v in data.items()
            if sym in str(k).upper()
            or sym == str(v.get("base_symbol", "")).upper()
            or sym == str(v.get("symbol", "")).upper()
        }

    data = filter_symbols(data, args.symbols)
    if not data:
        print("⚠️  过滤后无任何行。", file=sys.stderr)
        return 0

    tables = [
        ("spread_rate", "spread_rate", "spread_quantiles"),
        ("bidask_sr", "bidask_sr", "bidask_quantiles"),
        ("askbid_sr", "askbid_sr", "askbid_quantiles"),
    ]

    for label, value_key, quantile_key in tables:
        print(f"\n📊 {label}")
        headers, rows = build_table(
            data,
            factor_label=label,
            value_key=value_key,
            quantile_key=quantile_key,
            na=args.na,
            tsfmt=args.tsfmt,
            sort_field=args.sort,
        )
        print_three_line_table(headers, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
