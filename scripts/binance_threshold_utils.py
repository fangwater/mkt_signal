#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Shared helpers for Binance spread threshold tooling.

Utilities include:
  * Redis connection helpers
  * Reading rolling_metrics threshold payloads
  * Building per-symbol quantile lookup maps
  * Determining stale symbols in destination hashes
  * Printing compact summaries for sync scripts
"""

from __future__ import annotations

import json
import math
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


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

# Mapping from rolling quantile field name to canonical factor name
ROLLING_FACTOR_FIELDS = {
    "bidask_quantiles": "bidask",
    "askbid_quantiles": "askbid",
    "mid_spot_quantiles": "mid_spot",
    "mid_swap_quantiles": "mid_swap",
    "spread_quantiles": "spread",
}


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def read_hash(rds, key: str) -> Dict[str, Dict]:
    """Return decoded JSON payloads from a Redis HASH."""
    result: Dict[str, Dict] = {}
    for field, raw in rds.hgetall(key).items():
        decoded_key = field.decode("utf-8", "ignore") if isinstance(field, bytes) else str(field)
        text = raw.decode("utf-8", "ignore") if isinstance(raw, bytes) else str(raw)
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            safe = text.replace("NaN", "null")
            try:
                payload = json.loads(safe)
            except Exception:
                continue
        if isinstance(payload, dict):
            payload.setdefault("symbol_pair", decoded_key)
            result[decoded_key] = payload
    return result


def extract_symbol(field: str, payload: Dict[str, object]) -> str:
    """Extract base symbol from rolling metrics field/payload."""
    if "::" in field:
        return field.split("::")[-1].upper()
    base = payload.get("base_symbol") or payload.get("symbol")
    if isinstance(base, str) and base:
        return base.upper()
    return field.upper()


def _normalize_quantile(value: object) -> Optional[float]:
    """Convert raw quantile to 0-1 float range, return None on failure."""
    try:
        q = float(value)
    except Exception:
        return None
    if not math.isfinite(q):
        return None
    if q > 1.0:
        q /= 100.0
    if q < 0.0 or q > 1.0:
        return None
    # round to mitigate floating errors while keeping precision
    return round(q, 6)


def build_quantile_lookup(payload: Dict[str, object]) -> Dict[str, Dict[object, Optional[float]]]:
    """Create lookup maps for each factor quantile."""
    lookup: Dict[str, Dict[object, Optional[float]]] = {}
    for field, factor in ROLLING_FACTOR_FIELDS.items():
        entries = payload.get(field)
        factor_map: Dict[object, Optional[float]] = {}
        if isinstance(entries, list):
            for item in entries:
                if not isinstance(item, dict):
                    continue
                quantile = _normalize_quantile(item.get("quantile"))
                threshold = item.get("threshold")
                threshold_val: Optional[float]
                try:
                    threshold_val = float(threshold) if threshold is not None else None
                except Exception:
                    threshold_val = None
                if quantile is not None:
                    factor_map[quantile] = threshold_val
                label = item.get("label")
                if isinstance(label, str) and label.strip():
                    factor_map[label.strip().lower()] = threshold_val
        lookup[factor] = factor_map
    return lookup


def quantile_value(
    quantile_map: Dict[str, Dict[object, Optional[float]]],
    factor: str,
    target: object,
) -> Optional[float]:
    """Fetch quantile threshold by numeric (0-1 or 0-100) or label key."""
    factors = quantile_map.get(factor)
    if not factors:
        return None
    # Numeric search
    normalized = _normalize_quantile(target)
    if normalized is not None and normalized in factors:
        return factors[normalized]
    # Label lookup, stored in lowercase
    if isinstance(target, str):
        return factors.get(target.strip().lower())
    return None


def collect_rows_from_rolling(
    rds,
    rolling_key: str,
    allow_symbols: Optional[Sequence[str]] = None,
) -> Tuple[Dict[str, Dict], List[str], List[str], List[str]]:
    """
    Collect per-symbol payloads from rolling metrics.

    Returns (rows, success_symbols, missing_symbols, skipped_symbols).
    Each row contains:
        symbol, symbol_pair, update_tp, sample_size,
        bidask_sr, askbid_sr,
        quantiles (factor -> map), raw (original JSON)
    """
    entries = read_hash(rds, rolling_key)
    allow_set = {s.upper() for s in allow_symbols} if allow_symbols else None

    rows: Dict[str, Dict] = {}
    success: List[str] = []
    missing: List[str] = []
    skipped: List[str] = []

    for field, payload in entries.items():
        symbol = extract_symbol(field, payload)
        if allow_set and symbol not in allow_set:
            skipped.append(symbol)
            continue
        update_tp = payload.get("update_tp") or payload.get("ts")
        sample_size = payload.get("sample_size")
        try:
            sample_size_val = int(sample_size) if sample_size is not None else 0
        except Exception:
            sample_size_val = 0

        quantiles = build_quantile_lookup(payload)
        row = {
            "symbol": symbol,
            "symbol_pair": payload.get("symbol_pair", field),
            "update_tp": update_tp,
            "sample_size": sample_size_val,
            "bidask_sr": payload.get("bidask_sr"),
            "askbid_sr": payload.get("askbid_sr"),
            "mid_price_spot": payload.get("mid_price_spot"),
            "mid_price_swap": payload.get("mid_price_swap"),
            "spread_rate": payload.get("spread_rate"),
            "quantiles": quantiles,
            "raw": payload,
        }
        rows[symbol] = row
        if sample_size_val > 0:
            success.append(symbol)
        else:
            missing.append(symbol)

    return rows, success, missing, skipped


def determine_stale_symbols(rds, dest_key: str, desired_symbols: Iterable[str]) -> List[str]:
    """Return sorted list of symbols present in dest_key but not in desired_symbols."""
    desired_set = {s.upper() for s in desired_symbols}
    if not desired_set:
        return []
    try:
        existing = rds.hkeys(dest_key)
    except Exception:
        return []
    existing_set = {
        key.decode("utf-8", "ignore") if isinstance(key, bytes) else str(key)
        for key in existing
    }
    stale = sorted(sym for sym in existing_set if sym.upper() not in desired_set)
    return stale


def print_summary(success: Sequence[str], missing: Sequence[str], skipped: Sequence[str]) -> None:
    """Print a human-readable summary of collection results."""
    print(f"成功加载 {len(success)} 个 symbol")
    if missing:
        missing_sorted = ", ".join(sorted(set(missing)))
        print(f"缺少样本或为空: {missing_sorted}")
    if skipped:
        skipped_sorted = ", ".join(sorted(set(skipped)))
        print(f"未在允许列表中，已跳过: {skipped_sorted}")

