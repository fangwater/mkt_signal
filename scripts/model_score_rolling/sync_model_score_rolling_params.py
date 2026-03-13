#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Optional


MODEL_RE = r"[a-zA-Z0-9][a-zA-Z0-9._-]*"

DEFAULTS = {
    "max_length": 150_000,
    "reload_param_sec": 60,
    "rolling_window": 17_800,
    "min_periods": 100,
    "quantiles": [0.9, 0.8, 0.2, 0.1],
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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sync default Redis hash model_score_rolling_params_{model_name}"
    )
    p.add_argument("--model-name", help="model name")
    p.add_argument("--dry-run", action="store_true", help="print default payload only, no redis write")
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


def to_str(value: Any) -> str:
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return str(value)


def build_payload(model_name: str) -> Dict[str, str]:
    payload: Dict[str, Any] = {
        "output_hash_key": f"model_score_rolling_thresholds_{model_name}",
        "reload_param_sec": DEFAULTS["reload_param_sec"],
        "max_length": DEFAULTS["max_length"],
        "rolling_window": DEFAULTS["rolling_window"],
        "min_periods": DEFAULTS["min_periods"],
        "quantiles": DEFAULTS["quantiles"],
    }

    return {k: to_str(v) for k, v in payload.items()}


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("redis package is not installed. Please run: pip install redis", file=sys.stderr)
        return 2

    model_name = args.model_name.strip()
    hash_key = f"model_score_rolling_params_{model_name}"
    payload = build_payload(model_name)

    if args.dry_run:
        print(f"dry-run hash key: {hash_key}")
        print("dry-run payload:")
        for key, value in payload.items():
            print(f"  {key} = {value}")
        return 0

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    try:
        rds.hset(hash_key, mapping=payload)
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] HSET failed: key={hash_key} err={exc}", file=sys.stderr)
        return 1

    print("[INFO] model_score_rolling params synced")
    print(f"[INFO] redis hash: {hash_key}")
    print("[INFO] fields:")
    for key, value in payload.items():
        print(f"  {key} = {value}")

    print("\n[INFO] next steps:")
    print(
        "  - print params: python scripts/model_score_rolling/print_model_score_rolling_params.py "
        f"--model-name {model_name}"
    )
    print(
        "  - model_pub will pick up redis params on the next reload cycle"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
