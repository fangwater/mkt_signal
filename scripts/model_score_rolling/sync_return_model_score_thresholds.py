#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple


MODEL_RE = r"[a-zA-Z0-9][a-zA-Z0-9._-]*"
VENUE_PREFIX_PATTERN = re.compile(
    r"^([a-z0-9]+-(?:margin|futures|spot|swap|perpetual|perp))(?:$|[-_.])"
)

# Business operation -> score quantile reference.
# You can update these references without changing code logic.
RETURN_MODEL_SCORE_MAPPING: Dict[str, str] = {
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
    if lower == "binance_futures_direction_model":
        return "binance-futures"
    matched = VENUE_PREFIX_PATTERN.match(lower)
    if matched:
        return matched.group(1)

    raise SystemExit(
        "cannot infer venue from model_name prefix; expected prefix like "
        "'binance-futures-...' or special model name 'binance_futures_direction_model'"
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sync return-model-score thresholds from rolling output by model_name"
    )
    p.add_argument("--model-name", help="model name")
    p.add_argument("--dry-run", action="store_true", help="print only, no redis write")
    args = p.parse_args()

    model_name = (args.model_name or "").strip()
    if not model_name:
        inferred = infer_model_name_from_cwd()
        if inferred:
            model_name = inferred
            print(f"[INFO] inferred model_name from cwd: {model_name}")

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


def normalize_quantile(q_raw: object) -> Optional[int]:
    try:
        q = float(q_raw)
    except Exception:
        return None

    if not math.isfinite(q):
        return None
    if q > 1.0:
        q = q / 100.0
    if q < 0.0 or q > 1.0:
        return None

    p = int(round(q * 100.0))
    if p < 0 or p > 100:
        return None
    return p


def build_score_points(payload: Dict) -> Optional[Tuple[str, Dict[str, float]]]:
    quantiles = payload.get("quantiles")
    thresholds = payload.get("thresholds")
    ready = bool(payload.get("ready", False))

    if not ready:
        return None
    if not isinstance(quantiles, list) or not isinstance(thresholds, list):
        return None
    if len(quantiles) != len(thresholds) or not quantiles:
        return None

    symbol = str(payload.get("symbol", "")).strip().upper()
    if not symbol:
        return None

    points: Dict[str, float] = {}
    for q_raw, t_raw in zip(quantiles, thresholds):
        percentile = normalize_quantile(q_raw)
        if percentile is None:
            continue

        try:
            threshold = float(t_raw)
        except Exception:
            continue
        if not math.isfinite(threshold):
            continue

        points[f"score_{percentile}"] = threshold

    if not points:
        return None

    return symbol, points


def format_threshold(value: float) -> str:
    return f"{value:.12g}"


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("redis package is not installed. Please run: pip install redis", file=sys.stderr)
        return 2

    model_name = args.model_name.strip()
    venue = infer_venue_from_model_name(model_name)
    read_key = f"model_score_rolling_thresholds_{model_name}"
    write_key = f"return_model_score_thresholds_{venue}"

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    raw = decode_map(rds.hgetall(read_key))
    if not raw:
        print(f"[WARN] source hash is empty: {read_key}")

    output_fields: Dict[str, str] = {}
    total = 0
    ready_symbols = 0
    synced_symbols = 0

    for field_symbol, text in raw.items():
        total += 1
        try:
            payload = json.loads(text)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue

        parsed = build_score_points(payload)
        if parsed is None:
            continue

        ready_symbols += 1
        payload_symbol, points = parsed
        symbol = payload_symbol or field_symbol.upper()

        symbol_fields: Dict[str, str] = {}
        for operation, score_ref in RETURN_MODEL_SCORE_MAPPING.items():
            value = points.get(score_ref)
            if value is None:
                symbol_fields = {}
                break
            symbol_fields[f"{symbol}_{operation}"] = format_threshold(value)

        if not symbol_fields:
            continue

        output_fields.update(symbol_fields)
        synced_symbols += 1

    existing_fields = {
        x.decode("utf-8", "ignore") if isinstance(x, bytes) else str(x)
        for x in rds.hkeys(write_key)
    }
    new_fields = set(output_fields.keys())
    stale_fields = sorted(existing_fields - new_fields)

    if args.dry_run:
        print(f"dry-run venue: {venue}")
        print(f"dry-run source: {read_key}")
        print(f"dry-run target: {write_key}")
        print(f"dry-run stats: total={total} ready={ready_symbols} synced={synced_symbols}")
        print("dry-run business mapping:")
        for k, v in RETURN_MODEL_SCORE_MAPPING.items():
            print(f"  {k} -> {v}")
        print(f"dry-run output fields: {len(output_fields)}")
        if stale_fields:
            print(f"dry-run stale fields: {len(stale_fields)}")
        return 0

    pipe = rds.pipeline()
    if output_fields:
        pipe.hset(write_key, mapping=output_fields)
    if stale_fields:
        pipe.hdel(write_key, *stale_fields)
    pipe.execute()

    print("[INFO] return model score thresholds synced")
    print(f"[INFO] venue: {venue}")
    print(f"[INFO] source: {read_key}")
    print(f"[INFO] target: {write_key}")
    print(
        f"[INFO] stats: total={total} ready={ready_symbols} synced={synced_symbols} "
        f"written={len(output_fields)} stale_removed={len(stale_fields)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
