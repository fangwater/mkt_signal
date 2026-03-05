#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Optional


MODEL_RE = r"[a-zA-Z0-9][a-zA-Z0-9._-]*"


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
        description="Print Redis hash model_score_rolling_params_{model_name} as JSON"
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


def read_hash(rds, key: str) -> Dict[str, str]:
    data = rds.hgetall(key)

    def decode(obj: object) -> str:
        return obj.decode("utf-8", "ignore") if isinstance(obj, bytes) else str(obj)

    return {decode(k): decode(v) for k, v in data.items()}


def maybe_decode_json(raw: str) -> Any:
    text = raw.strip()
    if not text:
        return raw
    if text[0] not in "[{\"-0123456789tfn":
        return raw
    try:
        return json.loads(text)
    except Exception:
        return raw


def build_output(kv: Dict[str, str]) -> Dict[str, Any]:
    known_fields = {
        "input_services",
        "input_service",
        "output_hash_key",
        "refresh_sec",
        "reload_param_sec",
        "max_length",
        "rolling_window",
        "min_periods",
        "quantiles",
    }

    result: Dict[str, Any] = {}
    selected: Dict[str, Any] = {}

    for key in sorted(kv.keys()):
        if key in known_fields:
            selected[key] = maybe_decode_json(kv[key])
            continue
        selected[key] = maybe_decode_json(kv[key])

    result["params"] = selected
    return result


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("redis package is not installed. Please run: pip install redis", file=sys.stderr)
        return 2

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    model_name = args.model_name.strip()
    key = f"model_score_rolling_params_{model_name}"

    print(f"[INFO] redis: 127.0.0.1:6379/0", file=sys.stderr)
    print(f"[INFO] hash key: {key}", file=sys.stderr)
    print("", file=sys.stderr)

    kv = read_hash(rds, key)
    if not kv:
        print(f"[WARN] hash '{key}' is empty or does not exist", file=sys.stderr)
        return 0

    out = build_output(kv)
    print(json.dumps(out, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
