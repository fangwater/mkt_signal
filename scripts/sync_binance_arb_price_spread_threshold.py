#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sync Redis hash `binance_arb_price_spread_threshold` using rolling metrics quantiles.

The script:
  * Reads per-symbol quantile data from `rolling_metrics_thresholds` (configurable).
  * Maps factor quantiles to user-defined threshold keys (default mapping provided).
  * Writes compact JSON payloads to the destination HASH with optional cleanup.
  * Prints a three-line summary table of the computed thresholds.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from binance_threshold_utils import (
    SYMBOL_ALLOWLIST,
    collect_rows_from_rolling,
    determine_stale_symbols,
    print_summary,
    quantile_value,
    try_import_redis,
)


DEFAULT_MAPPING = {
    # 正套
    "forward_arb_open_tr": ("quantile", "bidask", 5.0),
    "forward_arb_cancel_tr": ("quantile", "bidask", 10.0),
    "forward_arb_close_tr": ("quantile", "askbid", 95.0),
    "forward_arb_cancel_close_tr": ("quantile", "askbid", 90.0),
    # 反套
    "backward_arb_open_tr": ("quantile", "askbid", 5.0),
    "backward_arb_cancel_tr": ("quantile", "askbid", 10.0),
    "backward_arb_close_tr": ("quantile", "bidask", 95.0),
    "backward_arb_cancel_close_tr": ("quantile", "bidask", 90.0),
}


MappingEntry = Tuple[str, str, object]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sync binance_arb_price_spread_threshold hash from rolling metrics"
    )
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))

    p.add_argument(
        "--rolling-key",
        default="rolling_metrics_thresholds",
        help="Redis HASH key containing rolling metrics (default: rolling_metrics_thresholds)",
    )
    p.add_argument(
        "--write-key",
        default="binance_arb_price_spread_threshold",
        help="Redis HASH key to write thresholds into (default: binance_arb_price_spread_threshold)",
    )
    p.add_argument(
        "--symbols",
        nargs="*",
        help="Only include these symbols (upper/lower case ignored)",
    )
    p.add_argument(
        "--allow-all",
        action="store_true",
        help="Process all symbols from rolling metrics instead of the predefined allowlist",
    )
    p.add_argument(
        "--map",
        action="append",
        metavar="KEY=FACTOR:Q",
        help="Map Redis key to factor quantile (e.g. bidask_sr_open_threshold=bidask:5). "
        "Q accepts percentage (5) or decimal (0.05); labels are also supported.",
    )
    p.add_argument(
        "--field",
        action="append",
        metavar="KEY=FIELD",
        help="Map Redis key to a direct field from rolling payload (e.g. spot_mid=mid_price_spot).",
    )
    p.add_argument(
        "--const",
        action="append",
        metavar="KEY=VALUE",
        help="Set Redis key to a constant numeric value (e.g. order_max=0.0015).",
    )
    p.add_argument(
        "--no-clean",
        action="store_true",
        help="Keep stale symbols in destination HASH (skip HDEL).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print results without writing to Redis.",
    )
    p.add_argument(
        "--tsfmt",
        choices=["iso", "raw"],
        default="iso",
        help="update_tp column format (default: iso, as UTC time).",
    )
    return p.parse_args()


def parse_mapping(args: argparse.Namespace) -> Dict[str, MappingEntry]:
    mapping: Dict[str, MappingEntry] = dict(DEFAULT_MAPPING)

    if args.map:
        for item in args.map:
            if "=" not in item or ":" not in item:
                raise SystemExit(f"--map 参数格式错误: {item}")
            key, spec = item.split("=", 1)
            factor, target = spec.split(":", 1)
            key = key.strip()
            if not key:
                raise SystemExit(f"--map 缺少键名: {item}")
            factor = factor.strip().lower()
            target_value: object
            target = target.strip()
            try:
                target_value = float(target)
            except ValueError:
                target_value = target
            mapping[key] = ("quantile", factor, target_value)

    if args.field:
        for item in args.field:
            if "=" not in item:
                raise SystemExit(f"--field 参数格式错误: {item}")
            key, field = item.split("=", 1)
            key = key.strip()
            field = field.strip()
            if not key or not field:
                raise SystemExit(f"--field 参数缺少键或字段: {item}")
            mapping[key] = ("field", field, None)

    if args.const:
        for item in args.const:
            if "=" not in item:
                raise SystemExit(f"--const 参数格式错误: {item}")
            key, raw_value = item.split("=", 1)
            key = key.strip()
            raw_value = raw_value.strip()
            if not key:
                raise SystemExit(f"--const 参数缺少键名: {item}")
            try:
                value = float(raw_value)
            except ValueError:
                raise SystemExit(f"--const 需要数值: {item}")
            mapping[key] = ("const", "const", value)

    return mapping


def resolve_mapping_for_symbol(row: Dict, mapping: Dict[str, MappingEntry]) -> Tuple[Dict[str, object], List[str]]:
    quantiles = row["quantiles"]
    resolved: Dict[str, object] = {}
    missing_keys: List[str] = []

    for key, entry in mapping.items():
        mode, arg1, arg2 = entry
        if mode == "quantile":
            value = quantile_value(quantiles, arg1, arg2)
            if value is None:
                missing_keys.append(f"{key}({arg1}:{arg2})")
            else:
                resolved[key] = value
        elif mode == "field":
            value = row.get(arg1)
            if value is None:
                value = row["raw"].get(arg1)
            if value is None:
                missing_keys.append(f"{key}(field:{arg1})")
            else:
                resolved[key] = value
        elif mode == "const":
            resolved[key] = arg2
        else:
            missing_keys.append(f"{key}(unknown mode)")

    return resolved, missing_keys


def format_ts(value: object, mode: str) -> str:
    if value is None:
        return "-"
    try:
        ts_ms = int(value)
    except Exception:
        return str(value)
    if mode == "raw":
        return str(ts_ms)
    try:
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts_ms)


def format_number(value: object) -> str:
    if value is None:
        return "-"
    try:
        num = float(value)
    except Exception:
        return str(value)
    text = f"{num:.8f}".rstrip("0").rstrip(".")
    return text or "0"


def print_threshold_table(rows: Dict[str, Dict], mapping_keys: List[str], tsfmt: str) -> None:
    headers = ["symbol", "update_tp", "sample_size"] + mapping_keys
    column_widths = [len(h) for h in headers]
    formatted_rows: List[List[str]] = []
    symbols = sorted(rows.keys())
    for symbol in symbols:
        payload = rows[symbol]
        sample = payload.get("sample_size")
        row: List[str] = [
            symbol,
            format_ts(payload["update_tp"], tsfmt),
            str(sample) if sample is not None else "-",
        ]
        for key in mapping_keys:
            row.append(format_number(payload["payload"].get(key)))
        formatted_rows.append(row)
        for idx, cell in enumerate(row):
            column_widths[idx] = max(column_widths[idx], len(cell))

    def fmt(values: List[str]) -> str:
        return "  ".join(val.ljust(column_widths[idx]) for idx, val in enumerate(values))

    header_line = fmt(headers)
    top_rule = "=" * len(header_line)
    mid_rule = "-" * len(header_line)
    bot_rule = "=" * len(header_line)

    print(top_rule)
    print(header_line)
    print(mid_rule)
    for row in formatted_rows:
        print(fmt(row))
    print(bot_rule)


def main() -> int:
    args = parse_args()
    mapping = parse_mapping(args)
    redis = try_import_redis()
    if redis is None:
        print("redis 包未安装，请先 `pip install redis`。", file=sys.stderr)
        return 2

    rds = (
        redis.from_url(args.redis_url)
        if args.redis_url
        else redis.Redis(host=args.host, port=args.port, db=args.db, password=args.password)
    )

    allow = None if args.allow_all else SYMBOL_ALLOWLIST
    rows, success, missing, skipped = collect_rows_from_rolling(rds, args.rolling_key, allow)
    print_summary(success, missing, skipped)

    if args.symbols:
        wanted = {s.upper() for s in args.symbols}
        rows = {sym: payload for sym, payload in rows.items() if sym.upper() in wanted}
        if not rows:
            print("未找到匹配的 symbol，停止执行。")
            return 1

    resolved_rows: Dict[str, Dict] = {}
    errors: List[str] = []

    for symbol, row in rows.items():
        payload_values, missing_keys = resolve_mapping_for_symbol(row, mapping)
        if missing_keys:
            errors.append(f"{symbol}: 缺少 {', '.join(missing_keys)}")
            continue
        update_tp = row.get("update_tp")
        try:
            update_tp_val = int(update_tp) if update_tp is not None else None
        except Exception:
            update_tp_val = update_tp

        payload = {
            "symbol": symbol,
            "futures_symbol": symbol,
            "update_tp": update_tp_val,
            **payload_values,
        }
        resolved_rows[symbol] = {
            "update_tp": update_tp_val,
            "sample_size": row.get("sample_size"),
            "payload": payload,
        }

    if errors:
        print("以下 symbol 缺少映射所需的阈值，未写入 Redis：")
        for err in errors:
            print(f"  - {err}")
        return 1

    if not resolved_rows:
        print("没有可写入的数据。")
        return 0

    mapping_keys = list(mapping.keys())
    print_threshold_table(resolved_rows, mapping_keys, args.tsfmt)

    dest_symbols = set(resolved_rows.keys())
    stale_symbols = [] if args.no_clean else determine_stale_symbols(rds, args.write_key, dest_symbols)
    if stale_symbols and not args.dry_run:
        print(f"将删除过期符号: {', '.join(stale_symbols)}")

    if args.dry_run:
        print(f"dry-run: 将写入 {len(resolved_rows)} 条到 HASH {args.write_key}")
        if stale_symbols:
            print(f"dry-run: 将清理 {len(stale_symbols)} 个字段")
        return 0

    pipe = rds.pipeline(transaction=False)
    for symbol, info in resolved_rows.items():
        payload_json = json.dumps(info["payload"], ensure_ascii=False, separators=(",", ":"))
        pipe.hset(args.write_key, symbol, payload_json)
    if stale_symbols:
        pipe.hdel(args.write_key, *stale_symbols)
    pipe.execute()

    print(
        f"已写入 {len(resolved_rows)} 条到 HASH {args.write_key}"
        + (f"，清理 {len(stale_symbols)} 个字段" if stale_symbols else "")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
