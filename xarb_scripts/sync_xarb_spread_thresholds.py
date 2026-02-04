#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
从 rolling_metrics_thresholds_{open_venue}_{hedge_venue} 读取百分位数据，生成价差阈值并同步到 Redis（xarb 版本）。

工作流程：
  1. 从 Redis 读取 xarb_dump_symbols:{key_suffix}、xarb_fwd_trade_symbols:{key_suffix}、xarb_bwd_trade_symbols:{key_suffix}
  2. 合并列表得到要同步的 symbols（去重）
  3. 从 rolling_metrics_thresholds_{open_venue}_{hedge_venue} 读取这些 symbols 的百分位数据
  4. 根据 SPREAD_THRESHOLD_MAPPING 配置，提取对应的百分位值
  5. 生成价差阈值并写入 xarb_spread_thresholds_{open_venue}_{hedge_venue}

其中 key_suffix 为 "<open_exchange>-<hedge_exchange>"（例如 okex-binance）。

推断规则：
  - open/hedge venue：优先 --open-venue/--hedge-venue，其次 --env-name，再其次 CWD（如 okex-binance-xarb-trade）
  - key_suffix：从 open/hedge venue 的 exchange 前缀推断（okex-futures -> okex）
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
    if open_ex == hedge_ex:
        return None
    return open_ex, hedge_ex


def infer_xarb_venues_from_env_name(env_name: str) -> Optional[Tuple[str, str]]:
    pair = infer_pair_from_name(env_name)
    if not pair:
        return None
    return f"{pair[0]}-futures", f"{pair[1]}-futures"


def infer_xarb_venues_from_cwd() -> Optional[Tuple[str, str]]:
    return infer_xarb_venues_from_env_name(Path.cwd().name)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync xarb spread thresholds from rolling metrics to Redis")
    p.add_argument("--open-venue", help="open 侧 venue（例如 okex-futures）")
    p.add_argument("--hedge-venue", help="hedge 侧 venue（例如 binance-futures）")
    p.add_argument("--env-name", help="环境目录名（例如 okex-binance-xarb-trade）")
    p.add_argument("--symbol", help="只同步指定 symbol（如 BTCUSDT 或 BTC-USDT）")
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
    """
    rolling_metrics 输出的 hash field 在 xarb 环境常见为：
      "<open>_<hedge>::<SYMBOL>" 例如 "okex-futures_binance-futures::BTCUSDT"

    为了便于下游按 SYMBOL 查找，这里会把 field 统一索引为 symbol（去掉前缀部分）。
    """
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

        # 优先用 payload 内的 base_symbol，其次使用 field 尾部（:: 之后）
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
        # 兼容 10/15/90（百分位整数）与 0.10/0.15/0.90（比例）
        if q > 1.0:
            q = q / 100.0
        if 0.0 <= q <= 1.0:
            return q
        return None

    def _to_v(item: Dict) -> Optional[float]:
        # 兼容两种 schema:
        # - {"q": 0.15, "v": 0.0002}
        # - {"quantile": 0.15, "threshold": 0.0002}
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
        if abs(q - percentile) < 1e-9:
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

    if args.open_venue and args.hedge_venue:
        open_venue, hedge_venue = args.open_venue.lower(), args.hedge_venue.lower()
    elif args.env_name:
        inferred = infer_xarb_venues_from_env_name(args.env_name)
        if not inferred:
            print(f"❌ 无法从 --env-name 推断 venues: {args.env_name}", file=sys.stderr)
            return 2
        open_venue, hedge_venue = inferred[0], inferred[1]
    else:
        inferred = infer_xarb_venues_from_cwd()
        if not inferred:
            print("❌ 需要 --open-venue/--hedge-venue 或 --env-name，或在目录名包含 '<open>-<hedge>-xarb-...'", file=sys.stderr)
            return 2
        open_venue, hedge_venue = inferred[0], inferred[1]

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

    all_fields: Dict[str, str] = {}
    skipped_symbols: List[str] = []

    for sym_key, row in thresholds.items():
        missing = [k for k in THRESHOLD_ORDER if k not in row]
        if missing:
            skipped_symbols.append(sym_key)
            continue

        for suffix in THRESHOLD_ORDER:
            value = float(row[suffix])
            all_fields[f"{sym_key}_{suffix}"] = f"{value:.8f}".rstrip("0").rstrip(".")

    if not all_fields:
        print(f"❌ 无可写入的阈值字段（可能 rolling metrics 数据不足或 symbols 列表为空）", file=sys.stderr)
        print(f"🧾 sync 配置已写入 '{config_key}'（可用 print 脚本查看 mapping 是否匹配）")
        return 2

    rds.hset(write_key, mapping=all_fields)

    successful_symbols = len(set(k.rsplit("_", 3)[0] for k in all_fields.keys()))
    print(f"✅ 已写入 {len(all_fields)} 个价差阈值字段到 HASH '{write_key}'")
    print(f"🧾 已写入 sync 配置到 '{config_key}'")
    print(f"   成功: {successful_symbols} 个 symbols")
    if skipped_symbols:
        print(f"   跳过: {len(skipped_symbols)} 个 symbols ({', '.join(sorted(set(skipped_symbols)))})")
    print("📍 Redis: 127.0.0.1:6379/0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
