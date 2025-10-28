#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 rolling_metrics 阈值，输出单一配置下的三线表。

输出两张表：
  * 基础因子：update_tp、midprice_spot、midprice_swap、spread_rate、bidask_sr、askbid_sr。
  * 分位阈值：列名形如 binance_midprice_spot_5、binance_spread_97。

数据源：Redis HASH（默认 `rolling_metrics_thresholds`），字段格式 `spot_swap::symbol`。
示例：
  python scripts/binance_arb_price_spread_threshold.py
  python scripts/binance_arb_price_spread_threshold.py --symbols BTCUSDT ETHUSDT
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

SYMBOL_ALLOWLIST: List[str] = [
    # 8h symbols
    "HIGHUSDT",
    "EGLDUSDT",
    "SFPUSDT",
    "IOTXUSDT",
    "ZENUSDT",
    "COTIUSDT",
    "ZILUSDT",
    "SUSHIUSDT",
    "MINAUSDT",
    "ENJUSDT",
    "KSMUSDT",
    "VETUSDT",
    "SXPUSDT",
    "BICOUSDT",
    "C98USDT",
    "CHRUSDT",
    "UNIUSDT",
    "NEOUSDT",
    "CELOUSDT",
    "KAVAUSDT",
    "ASTRUSDT",
    # 4h symbols
    "HEIUSDT",
    "NFPUSDT",
    "TNSRUSDT",
    "SANTOSUSDT",
    "FLUXUSDT",
    "KDAUSDT",
    "BEAMXUSDT",
    "AUCTIONUSDT",
    "AIUSDT",
    "INITUSDT",
    "A2ZUSDT",
    "USTCUSDT",
    "SAGAUSDT",
    "SLPUSDT",
    "VANRYUSDT",
    "WCTUSDT",
    "AXLUSDT",
    "JTOUSDT",
    "TWTUSDT",
    "PUMPUSDT",
    "MANTAUSDT",
    "MEMEUSDT",
    "ILVUSDT",
    "ORCAUSDT",
    "SUNUSDT",
]

BASE_METRIC_ORDER = [
    "update_tp",
    "midprice_spot",
    "midprice_swap",
    "spread_rate",
    "bidask_sr",
    "askbid_sr",
]

BASE_METRICS = {
    "mid_price_spot": "midprice_spot",
    "mid_price_swap": "midprice_swap",
    "spread_rate": "spread_rate",
    "bidask_sr": "bidask_sr",
    "askbid_sr": "askbid_sr",
}

QUANTILE_KEYS = {
    "bidask_quantiles": "bidask",
    "askbid_quantiles": "askbid",
    "mid_spot_quantiles": "midprice_spot",
    "mid_swap_quantiles": "midprice_swap",
    "spread_quantiles": "spread",
}

PREFIX_ORDER = {
    "midprice_spot": 0,
    "midprice_swap": 1,
    "spread": 2,
    "bidask": 3,
    "askbid": 4,
}


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="打印 rolling_metrics 阈值，输出基础与分位三线表"
    )
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    p.add_argument(
        "--rolling-key",
        default="rolling_metrics_thresholds",
        help="Redis HASH key providing rolling metrics thresholds",
    )
    p.add_argument(
        "--symbols",
        nargs="*",
        help="仅打印指定符号（默认内置 allowlist）",
    )
    p.add_argument("--na", default="-", help="缺失值占位符，默认 '-'")
    return p.parse_args()


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
    for field, raw in data.items():
        k = field.decode("utf-8", "ignore") if isinstance(field, bytes) else str(field)
        payload = raw.decode("utf-8", "ignore") if isinstance(raw, bytes) else str(raw)
        try:
            obj = json.loads(payload)
        except json.JSONDecodeError:
            safe = payload.replace("NaN", "null")
            obj = json.loads(safe)
        if isinstance(obj, dict):
            obj.setdefault("symbol_pair", k)
            result[k] = obj
    return result


def extract_symbol(field: str, obj: Dict[str, object]) -> str:
    parts = field.split("::")
    if len(parts) >= 2:
        return parts[-1].upper()
    raw = obj.get("base_symbol") or obj.get("symbol") or field
    return str(raw).upper()


def format_float(value: Optional[float], na: str) -> str:
    if value is None:
        return na
    try:
        if not isinstance(value, (int, float)):
            value = float(value)
        if not (value == value and abs(value) != float("inf")):
            return na
        text = f"{value:.8f}".rstrip("0").rstrip(".")
        return text or "0"
    except Exception:
        return na


def format_ts(value: Optional[object], na: str) -> str:
    if value is None:
        return na
    try:
        ts = int(value)
    except Exception:
        return na
    try:
        dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)


def quantile_column_name(prefix: str, quantile: Optional[object], label: Optional[object]) -> Optional[str]:
    if quantile is not None:
        try:
            q = float(quantile)
        except Exception:
            q = None
        if q is not None:
            if q > 1.0:
                q /= 100.0
            if 0.0 <= q <= 1.0:
                return f"{prefix}_{int(round(q * 100))}"
    if isinstance(label, str) and label.strip():
        safe = label.strip().replace(" ", "_").lower()
        return f"{prefix}_{safe}"
    return None


def build_tables(
    entries: Dict[str, Dict], symbols_filter: Optional[List[str]]
) -> Tuple[
    Dict[str, Dict[str, Optional[object]]],
    Dict[str, Dict[str, Optional[object]]],
    List[str],
]:
    allowed = (
        {s.upper() for s in symbols_filter}
        if symbols_filter
        else {s.upper() for s in SYMBOL_ALLOWLIST}
    )
    base_tables: Dict[str, Dict[str, Optional[object]]] = defaultdict(dict)
    quant_tables: Dict[str, Dict[str, Optional[object]]] = defaultdict(dict)
    symbols_set: set[str] = set()

    for field, obj in entries.items():
        symbol = extract_symbol(field, obj)
        if allowed and symbol not in allowed:
            continue
        symbols_set.add(symbol)

        ts_val = obj.get("update_tp") or obj.get("ts")
        base_tables.setdefault("update_tp", {})[symbol] = ts_val
        for raw_key, alias in BASE_METRICS.items():
            base_tables.setdefault(alias, {})[symbol] = obj.get(raw_key)

        for raw_key, prefix in QUANTILE_KEYS.items():
            quantiles = obj.get(raw_key)
            if not isinstance(quantiles, list):
                continue
            for item in quantiles:
                if not isinstance(item, dict):
                    continue
                column = quantile_column_name(prefix, item.get("quantile"), item.get("label"))
                if column is None:
                    continue
                display = f"binance_{column}"
                quant_tables.setdefault(display, {})[symbol] = item.get("threshold")

    return base_tables, quant_tables, sorted(symbols_set)


def print_three_line_table(headers: List[str], rows: List[List[str]]) -> None:
    widths = [len(h) for h in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def fmt(values: List[str]) -> str:
        return "  ".join(val.ljust(widths[idx]) for idx, val in enumerate(values))

    header_line = fmt(headers)
    top_rule = "=" * len(header_line)
    mid_rule = "-" * len(header_line)
    bot_rule = "=" * len(header_line)

    print(top_rule)
    print(header_line)
    print(mid_rule)
    for row in rows:
        print(fmt(row))
    print(bot_rule)


def quant_column_sort_key(name: str) -> Tuple[int, int, object]:
    prefix, _, suffix = name.rpartition("_")
    core = prefix
    if core.startswith("binance_"):
        core = core[len("binance_") :]
    order = PREFIX_ORDER.get(core, 99)
    try:
        value = int(suffix)
        return order, 0, value
    except ValueError:
        return order, 1, suffix


def main() -> int:
    args = parse_args()
    rds = connect_redis(args)
    if rds is None:
        print("redis 包未安装，请先 `pip install redis`。", file=sys.stderr)
        return 2

    entries = read_hash(rds, args.rolling_key)
    if not entries:
        print("Redis HASH 为空或 key 不存在。")
        return 0

    base_tables, quant_tables, symbols = build_tables(entries, args.symbols)
    if not symbols:
        print("未匹配到指定的 symbol。")
        return 0

    base_headers = ["symbol"] + BASE_METRIC_ORDER
    base_rows: List[List[str]] = []
    for symbol in symbols:
        row: List[str] = [symbol]
        for column in BASE_METRIC_ORDER:
            if column == "update_tp":
                value = base_tables.get("update_tp", {}).get(symbol)
                row.append(format_ts(value, args.na))
            else:
                value = base_tables.get(column, {}).get(symbol)
                row.append(format_float(value, args.na))
        base_rows.append(row)
    print("## base metrics")
    print_three_line_table(base_headers, base_rows)

    quant_columns = sorted(quant_tables.keys(), key=quant_column_sort_key)
    if quant_columns:
        quant_headers = ["symbol"] + quant_columns
        quant_rows: List[List[str]] = []
        for symbol in symbols:
            row = [symbol]
            for column in quant_columns:
                value = quant_tables[column].get(symbol)
                row.append(format_float(value, args.na))
            quant_rows.append(row)
        print()
        print("## quantile thresholds")
        print_three_line_table(quant_headers, quant_rows)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
