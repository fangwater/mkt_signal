#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
从 rolling_metrics_thresholds_{open_venue}_{hedge_venue} 读取百分位数据，生成价差阈值并同步到 Redis（intra 同所期现）。

工作流程：
  1. 从 Redis 读取 intra_dump_symbols:{exchange}、intra_fwd_trade_symbols:{exchange}、
     intra_bwd_trade_symbols:{exchange}、{env_name}:intra_unimmr_close_symbols:{open_venue}_{hedge_venue}
  2. 合并列表得到要同步的 symbols（去重）
  3. 从 rolling_metrics_thresholds_{open_venue}_{hedge_venue} 读取这些 symbols 的百分位数据
  4. 根据 SPREAD_THRESHOLD_MAPPING 提取对应百分位值
  5. 生成价差阈值并写入 intra_spread_thresholds_{open_venue}_{hedge_venue}

推断规则：--exchange / --open-venue / --hedge-venue / --env-name / CWD（<exchange>-intra-<tag>）
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
from typing import Dict, List, Optional, Set

SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]
NAMESPACE = "intra"
QUANTILE_MATCH_EPSILON = 1e-6


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
    ex = normalize_exchange(v.split("-", 1)[0])
    if ex not in SUPPORTED_EXCHANGES:
        return None
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


def infer_env_name_from_cwd() -> str:
    return Path.cwd().name.strip().lower()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync intra spread thresholds from rolling metrics to Redis")
    p.add_argument("--exchange", default=os.environ.get("EXCHANGE"))
    p.add_argument("--open-venue", default=os.environ.get("OPEN_VENUE"))
    p.add_argument("--hedge-venue", default=os.environ.get("HEDGE_VENUE"))
    p.add_argument("--env-name", help="环境目录名（例如 binance-intra-trade）")
    p.add_argument("--symbol", help="只同步指定 symbol（如 BTCUSDT）")
    return p.parse_args()


SPREAD_THRESHOLD_MAPPING = {
    "forward_open_mm": "spread_5",
    "forward_open_mt": "bidask_10",
    "forward_cancel_mm": "spread_10",
    "forward_cancel_mt": "bidask_15",
    "backward_open_mm": "spread_95",
    "backward_open_mt": "askbid_90",
    "backward_cancel_mm": "spread_90",
    "backward_cancel_mt": "askbid_85",
}

THRESHOLD_ORDER = list(SPREAD_THRESHOLD_MAPPING.keys())


def _read_symbol_list(rds, key: str, label: str, symbols_set: Set[str]) -> None:
    data = rds.get(key)
    if not data:
        return
    text = data.decode("utf-8", "ignore") if isinstance(data, bytes) else str(data)
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            symbols_set.update(s.upper() for s in parsed if s)
            print(f"📖 从 '{key}' ({label}) 读取 {len(parsed)} 个 symbols")
    except Exception as exc:
        print(f"⚠️  解析 '{key}' 失败: {exc}")


def unimmr_close_key(env_name: str, open_venue: str, hedge_venue: str) -> str:
    suffix = f"{open_venue.strip().lower()}_{hedge_venue.strip().lower()}"
    return f"{env_name}:intra_unimmr_close_symbols:{suffix}" if env_name else f"intra_unimmr_close_symbols:{suffix}"


def load_symbol_lists(
    rds,
    exchange: str,
    env_name: Optional[str] = None,
    open_venue: Optional[str] = None,
    hedge_venue: Optional[str] = None,
) -> List[str]:
    symbols_set: Set[str] = set()
    _read_symbol_list(rds, f"{NAMESPACE}_dump_symbols:{exchange}", "dump", symbols_set)
    _read_symbol_list(rds, f"{NAMESPACE}_fwd_trade_symbols:{exchange}", "fwd_trade", symbols_set)
    _read_symbol_list(rds, f"{NAMESPACE}_bwd_trade_symbols:{exchange}", "bwd_trade", symbols_set)
    if open_venue and hedge_venue:
        _read_symbol_list(
            rds,
            unimmr_close_key(env_name or "", open_venue, hedge_venue),
            "unimmr_close",
            symbols_set,
        )
    result = sorted(symbols_set)
    print(f"✅ 合并后共 {len(result)} 个唯一 symbols")
    return result


def normalize_for_rolling(symbol: str) -> str:
    cleaned = symbol.upper().replace("-", "").replace("_", "")
    if cleaned.endswith("SWAP"):
        cleaned = cleaned[:-4]
    return cleaned


def read_rolling_metrics(rds, key: str) -> Dict[str, Dict]:
    """rolling_metrics 输出 hash field 形如 "<open>_<hedge>::<SYMBOL>"，统一索引为 base symbol。"""
    result: Dict[str, Dict] = {}
    data = rds.hgetall(key)
    for k, v in data.items():
        field = k.decode("utf-8", "ignore") if isinstance(k, bytes) else str(k)
        val = v.decode("utf-8", "ignore") if isinstance(v, bytes) else str(v)
        try:
            obj = json.loads(val)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        base_symbol = obj.get("base_symbol") or obj.get("symbol")
        if isinstance(base_symbol, str) and base_symbol.strip():
            sym = normalize_for_rolling(base_symbol)
            result[sym] = obj
            continue
        tail = field.split("::")[-1]
        sym = normalize_for_rolling(tail)
        result[sym] = obj
    return result


def extract_quantile_value(obj: Dict, field_ref: str) -> Optional[float]:
    parts = field_ref.split("_")
    if len(parts) < 2:
        return None

    factor = parts[0]
    percentile_str = parts[1]

    try:
        percentile = float(percentile_str) / 100.0
    except ValueError:
        return None

    if factor == "bidask":
        quantile_key = "bidask_quantiles"
    elif factor == "askbid":
        quantile_key = "askbid_quantiles"
    elif factor == "spread":
        quantile_key = "spread_quantiles"
    else:
        return None

    quantiles = obj.get(quantile_key)
    if not isinstance(quantiles, list):
        return None

    def _to_q(raw: object) -> Optional[float]:
        if not isinstance(raw, (int, float, str)):
            return None
        try:
            q = float(raw)
        except Exception:
            return None
        if q > 1.0:
            q = q / 100.0
        if 0.0 <= q <= 1.0:
            return q
        return None

    def _to_v(item: Dict) -> Optional[float]:
        for key in ("v", "threshold", "value"):
            if key in item:
                try:
                    return float(item[key])
                except Exception:
                    return None
        return None

    for item in quantiles:
        if not isinstance(item, dict):
            continue
        q = _to_q(item.get("q") if "q" in item else item.get("quantile"))
        if q is None:
            continue
        if abs(q - percentile) < QUANTILE_MATCH_EPSILON:
            return _to_v(item)

    return None


def generate_spread_thresholds(symbols: List[str], rolling: Dict[str, Dict]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for sym in symbols:
        key = normalize_for_rolling(sym)
        obj = rolling.get(key)
        if not obj:
            continue
        row: Dict[str, float] = {}
        for dst_field, src_ref in SPREAD_THRESHOLD_MAPPING.items():
            v = extract_quantile_value(obj, src_ref)
            if v is None or math.isnan(v):
                continue
            row[dst_field] = float(v)
        if row:
            out[key] = row
    return out


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    exchange = args.exchange
    if not exchange:
        exchange = infer_exchange_from_name(args.env_name) if args.env_name else infer_exchange_from_cwd()
        if exchange:
            print(f"[INFO] 未提供 --exchange，基于目录推断: exchange={exchange}", file=sys.stderr)

    if exchange:
        exchange = normalize_exchange(exchange)
        if exchange not in SUPPORTED_EXCHANGES:
            print(f"❌ 不支持的 exchange: {exchange}", file=sys.stderr)
            return 2
        if not args.open_venue:
            args.open_venue = f"{exchange}-margin"
        if not args.hedge_venue:
            args.hedge_venue = f"{exchange}-futures"

    if not args.open_venue or not args.hedge_venue:
        print("❌ 需要 --exchange 或同时提供 --open-venue/--hedge-venue，或使用 --env-name", file=sys.stderr)
        return 2

    open_venue = args.open_venue.lower()
    hedge_venue = args.hedge_venue.lower()
    if not exchange:
        exchange = exchange_from_venue(open_venue)

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    env_name = (args.env_name or infer_env_name_from_cwd()).strip().lower()
    symbols = load_symbol_lists(rds, exchange, env_name, open_venue, hedge_venue)
    if args.symbol:
        symbols = [args.symbol.upper()]

    rolling_key = f"rolling_metrics_thresholds_{open_venue}_{hedge_venue}"
    print(f"📍 rolling metrics key: {rolling_key}")

    rolling = read_rolling_metrics(rds, rolling_key)
    if not rolling:
        print(f"❌ rolling metrics hash '{rolling_key}' 为空或不存在", file=sys.stderr)
        return 2

    thresholds = generate_spread_thresholds(symbols, rolling)
    write_key = f"{NAMESPACE}_spread_thresholds_{open_venue}_{hedge_venue}"
    config_key = f"{NAMESPACE}_spread_thresholds_config_{open_venue}_{hedge_venue}"

    config = {
        "schema_version": 1,
        "namespace": NAMESPACE,
        "open_venue": open_venue,
        "hedge_venue": hedge_venue,
        "rolling_key": rolling_key,
        "mapping": SPREAD_THRESHOLD_MAPPING,
        "threshold_order": THRESHOLD_ORDER,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    rds.set(config_key, json.dumps(config, ensure_ascii=False, sort_keys=True))

    mm_order = [k for k in THRESHOLD_ORDER if k.endswith("_mm")]
    mt_order = [k for k in THRESHOLD_ORDER if k.endswith("_mt")]
    other_order = [k for k in THRESHOLD_ORDER if not k.endswith(("_mm", "_mt"))]

    all_fields: Dict[str, str] = {}
    skipped_symbols: List[str] = []
    mm_skipped: List[str] = []
    mt_skipped: List[str] = []

    for sym_key, row in thresholds.items():
        mm_missing = [k for k in mm_order if k not in row]
        mt_missing = [k for k in mt_order if k not in row]
        sym_written = False

        if not mm_missing:
            for suffix in mm_order:
                all_fields[f"{sym_key}_{suffix}"] = f"{float(row[suffix]):.8f}".rstrip("0").rstrip(".")
            sym_written = True
        else:
            mm_skipped.append(sym_key)

        if not mt_missing:
            for suffix in mt_order:
                all_fields[f"{sym_key}_{suffix}"] = f"{float(row[suffix]):.8f}".rstrip("0").rstrip(".")
            sym_written = True
        else:
            mt_skipped.append(sym_key)

        for suffix in other_order:
            if suffix in row:
                all_fields[f"{sym_key}_{suffix}"] = f"{float(row[suffix]):.8f}".rstrip("0").rstrip(".")
                sym_written = True

        if not sym_written:
            skipped_symbols.append(sym_key)

    if mm_skipped:
        print(f"⚠️  mm 阈值缺少 rolling 数据，跳过 {len(mm_skipped)} 个 symbols: {', '.join(sorted(set(mm_skipped)))}", file=sys.stderr)
    if mt_skipped:
        print(f"⚠️  mt 阈值缺少 rolling 数据，跳过 {len(mt_skipped)} 个 symbols: {', '.join(sorted(set(mt_skipped)))}", file=sys.stderr)

    if not all_fields:
        print("❌ 无可写入的阈值字段（rolling metrics 数据不足或 symbols 列表为空）", file=sys.stderr)
        print(f"🧾 sync 配置已写入 '{config_key}'")
        return 2

    rds.hset(write_key, mapping=all_fields)

    successful_symbols = len(set(k.rsplit("_", 3)[0] for k in all_fields.keys()))
    print(f"✅ 已写入 {len(all_fields)} 个价差阈值字段到 HASH '{write_key}'")
    print(f"🧾 已写入 sync 配置到 '{config_key}'")
    print(f"   成功: {successful_symbols} 个 symbols")
    if skipped_symbols:
        print(f"   跳过(全缺): {len(skipped_symbols)} 个 symbols ({', '.join(sorted(set(skipped_symbols)))})")
    print("📍 Redis: 127.0.0.1:6379/0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
