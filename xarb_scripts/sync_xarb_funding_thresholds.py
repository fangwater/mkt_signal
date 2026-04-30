#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
从 rolling_metrics_thresholds_{open_venue}_{hedge_venue} 读取资费分位数据，
生成 xarb 专用 funding 阈值并同步到 Redis。

工作流程：
  1. 从 Redis 读取 xarb_dump_symbols:{key_suffix}、xarb_fwd_trade_symbols:{key_suffix}、
     xarb_bwd_trade_symbols:{key_suffix}
  2. 合并列表得到要同步的 symbols（去重）
  3. 从 rolling_metrics_thresholds_{open_venue}_{hedge_venue} 读取这些 symbols 的分位数据
  4. 根据 FUNDING_RATE_THRESHOLD_MAPPING 配置，提取对应的分位值
  5. 生成 funding 阈值并写入 xarb_funding_thresholds_{open_venue}_{hedge_venue}

说明：
  - xarb 的 funding 阈值与 spread 阈值分开维护
  - 当前只输出 forward_open_mm / backward_open_mm 两个字段
  - 单边 futures 组合默认使用该 futures 腿的 premium_rate 50 分位
  - 双 futures 组合默认使用 spread_fr_80 / spread_fr_20
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]
NAMESPACE = "xarb"


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


def infer_pair_from_name(name: str) -> Optional[Tuple[str, str]]:
    n = (name or "").strip().lower()
    m = re.match(r"^([a-z0-9]+)[-_]([a-z0-9]+)[-_]xarb([_-].*)?$", n)
    if not m:
        return None
    open_ex = normalize_exchange(m.group(1))
    hedge_ex = normalize_exchange(m.group(2))
    if open_ex not in SUPPORTED_EXCHANGES or hedge_ex not in SUPPORTED_EXCHANGES:
        return None
    return open_ex, hedge_ex


def infer_xarb_venues_from_env_name(env_name: str) -> Optional[Tuple[str, str]]:
    pair = infer_pair_from_name(env_name)
    if not pair:
        return None
    if pair[0] == pair[1]:
        return f"{pair[0]}-margin", f"{pair[1]}-futures"
    return f"{pair[0]}-futures", f"{pair[1]}-futures"


def infer_xarb_venues_from_cwd() -> Optional[Tuple[str, str]]:
    return infer_xarb_venues_from_env_name(Path.cwd().name)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sync xarb funding thresholds from rolling metrics to Redis"
    )
    p.add_argument("--open-venue", help="open 侧 venue（例如 okex-futures）")
    p.add_argument("--hedge-venue", help="hedge 侧 venue（例如 binance-futures）")
    p.add_argument("--env-name", help="环境目录名（例如 okex-binance-xarb-trade）")
    p.add_argument("--symbol", help="只同步指定 symbol（如 BTCUSDT 或 BTC-USDT）")
    return p.parse_args()


# fr 因子链按 venue 对动态选择(和 fr_config_server.default_funding_factor_chain 一致)。
# Rust 侧 `arb_open_filter::lookup_factor_realtime_value` 必须支持这里出现的因子名。
def default_funding_factor_chain(open_venue: str, hedge_venue: str) -> List[Dict[str, object]]:
    o = (open_venue or "").strip().lower()
    h = (hedge_venue or "").strip().lower()
    if o.endswith("-futures") and h.endswith("-futures"):
        return [
            {"factor": "spread_fr", "enabled": True, "forward_open": 80, "backward_open": 20}
        ]
    return [
        {"factor": "hedge_premium_rate", "enabled": True, "forward_open": 50, "backward_open": 50}
    ]


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


def load_symbol_lists(rds, key_suffix: str) -> List[str]:
    symbols_set: Set[str] = set()
    _read_symbol_list(rds, f"{NAMESPACE}_dump_symbols:{key_suffix}", "dump", symbols_set)
    _read_symbol_list(rds, f"{NAMESPACE}_fwd_trade_symbols:{key_suffix}", "fwd_trade", symbols_set)
    _read_symbol_list(rds, f"{NAMESPACE}_bwd_trade_symbols:{key_suffix}", "bwd_trade", symbols_set)
    result = sorted(symbols_set)
    print(f"✅ 合并后共 {len(result)} 个唯一 symbols")
    return result


def normalize_for_rolling(symbol: str) -> str:
    cleaned = symbol.upper().replace("-", "").replace("_", "")
    if cleaned.endswith("SWAP"):
        cleaned = cleaned[:-4]
    return cleaned


def read_rolling_metrics(rds, key: str) -> Dict[str, Dict]:
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
            result[normalize_for_rolling(base_symbol)] = obj
            continue

        result[normalize_for_rolling(field.split("::")[-1])] = obj
    return result


def extract_quantile_value(obj: Dict, field_ref: str) -> Optional[float]:
    parts = field_ref.split("_")
    if len(parts) < 3:
        return None

    factor = "_".join(parts[:-1])
    percentile_str = parts[-1]

    try:
        percentile = float(percentile_str) / 100.0
    except ValueError:
        return None

    quantile_key = {
        "open_premium_rate": "open_premium_rate_quantiles",
        "hedge_premium_rate": "hedge_premium_rate_quantiles",
        "spread_fr": "spread_fr_quantiles",
    }.get(factor)
    if quantile_key is None:
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
                    value = float(item[key])
                except Exception:
                    return None
                if math.isnan(value):
                    return None
                return value
        return None

    for item in quantiles:
        if not isinstance(item, dict):
            continue
        q = _to_q(item.get("q") if "q" in item else item.get("quantile"))
        if q is None:
            continue
        if abs(q - percentile) < 1e-9:
            return _to_v(item)

    return None


def generate_funding_thresholds(
    symbols: List[str],
    rolling: Dict[str, Dict],
    mapping: Dict[str, str],
) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for sym in symbols:
        key = normalize_for_rolling(sym)
        obj = rolling.get(key)
        if not obj:
            continue
        row: Dict[str, float] = {}
        for dst_field, src_ref in mapping.items():
            value = extract_quantile_value(obj, src_ref)
            if value is None:
                continue
            row[dst_field] = float(value)
        if row:
            out[key] = row
    return out


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    if args.open_venue and args.hedge_venue:
        open_venue, hedge_venue = args.open_venue.lower(), args.hedge_venue.lower()
    elif args.env_name:
        inferred = infer_xarb_venues_from_env_name(args.env_name)
        if not inferred:
            print(f"❌ 无法从 --env-name 推断 venues: {args.env_name}", file=sys.stderr)
            return 2
        open_venue, hedge_venue = inferred
    else:
        inferred = infer_xarb_venues_from_cwd()
        if not inferred:
            print(
                "❌ 需要 --open-venue/--hedge-venue 或 --env-name，或在目录名包含 '<open>-<hedge>-xarb-...'",
                file=sys.stderr,
            )
            return 2
        open_venue, hedge_venue = inferred

    open_ex = exchange_from_venue(open_venue)
    hedge_ex = exchange_from_venue(hedge_venue)
    if not open_ex or not hedge_ex:
        print(f"❌ 无效 venue: open={open_venue} hedge={hedge_venue}", file=sys.stderr)
        return 2
    key_suffix = f"{open_ex}-{hedge_ex}"

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    symbols = load_symbol_lists(rds, key_suffix)
    if args.symbol:
        symbols = [args.symbol.upper()]

    rolling_key = f"rolling_metrics_thresholds_{open_venue}_{hedge_venue}"
    print(f"📍 rolling metrics key: {rolling_key}")

    # NAMESPACE 设置成 "fr" 让 key 是 fr_funding_thresholds_config_*,和 Rust 侧
    # `funding_chain_config_key("fr", ...)` 对齐。
    config_key = f"fr_funding_thresholds_config_{open_venue}_{hedge_venue}"

    chain_payload = [
        {
            "factor": entry["factor"],
            "enabled": bool(entry.get("enabled", True)),
            "forward_open": float(entry.get("forward_open")),
            "backward_open": float(entry.get("backward_open")),
        }
        for entry in default_funding_factor_chain(open_venue, hedge_venue)
    ]
    config = {
        "factor_chain": chain_payload,
        "rolling_key": rolling_key,
    }
    rds.set(config_key, json.dumps(config, ensure_ascii=False, sort_keys=True))

    print(f"🧾 已写入 funding factor_chain 配置到 '{config_key}'")
    print(f"   chain 长度={len(chain_payload)} enabled={sum(1 for e in chain_payload if e['enabled'])}")
    for entry in chain_payload:
        print(
            f"   - {entry['factor']}: enabled={entry['enabled']} "
            f"forward_open={entry['forward_open']} backward_open={entry['backward_open']}"
        )
    print(f"📍 rolling_key={rolling_key}")
    print(f"📦 涉及 symbols={len(symbols)}(由 pre_trade 从 rolling 实时算阈值,本脚本只写 chain config)")
    print("📍 Redis: 127.0.0.1:6379/0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
