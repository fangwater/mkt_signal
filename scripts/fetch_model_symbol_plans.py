#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


DEFAULT_BASE_URL = "http://127.0.0.1:6300"
DEFAULT_MODEL_NAME = "binance_futures_direction_model"


def build_url(base_url: str, path: str, query: Optional[Dict[str, str]] = None) -> str:
    base = base_url.rstrip("/")
    url = f"{base}{path}"
    if query:
        encoded = urllib.parse.urlencode(query)
        if encoded:
            url = f"{url}?{encoded}"
    return url


def http_get_json(url: str, timeout_sec: float) -> Dict[str, Any]:
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            payload = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} {exc.reason}: {url}; {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"URL error for {url}: {exc.reason}") from exc

    obj = json.loads(payload)
    if not isinstance(obj, dict):
        raise RuntimeError(f"unexpected json type for {url}: {type(obj).__name__}")
    return obj


def fetch_symbol_rows(base_url: str, model_name: str, timeout_sec: float) -> List[Dict[str, Any]]:
    model_q = urllib.parse.quote(model_name, safe="")
    url = build_url(base_url, f"/api/models/{model_q}/symbols")
    payload = http_get_json(url, timeout_sec)
    items = payload.get("items")
    if not isinstance(items, list):
        raise RuntimeError(f"unexpected symbols payload: {url}")
    out: List[Dict[str, Any]] = []
    for item in items:
        if isinstance(item, dict):
            out.append(item)
    return out


def fetch_symbol_detail(
    base_url: str,
    model_name: str,
    symbol: str,
    group_key: str,
    timeout_sec: float,
) -> Dict[str, Any]:
    model_q = urllib.parse.quote(model_name, safe="")
    symbol_q = urllib.parse.quote(symbol, safe="")
    query = {"group_key": group_key} if group_key else None
    url = build_url(base_url, f"/api/models/{model_q}/symbols/{symbol_q}", query=query)
    return http_get_json(url, timeout_sec)


def trim_detail(detail: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "symbol": detail.get("symbol"),
        "group_key": detail.get("group_key"),
        "return_name": detail.get("return_name"),
        "feature_dim": detail.get("feature_dim"),
        "factor_count": detail.get("factor_count"),
        "grpc_ready": detail.get("grpc_ready"),
        "train_window_start_ts": detail.get("train_window_start_ts"),
        "train_window_end_ts": detail.get("train_window_end_ts"),
        "train_start_date": detail.get("train_start_date"),
        "train_end_date": detail.get("train_end_date"),
        "train_samples": detail.get("train_samples"),
        "train_time_sec": detail.get("train_time_sec"),
        "factors": detail.get("factors", []),
        "dim_factors": detail.get("dim_factors", []),
        "info_summary": detail.get("info_summary", {}),
        "model_meta": detail.get("model_meta", {}),
        "artifacts": detail.get("artifacts", {}),
        "warnings": detail.get("warnings", []),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch per-symbol plans for a model from model_manager"
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="model_manager base url")
    parser.add_argument(
        "--model-name",
        default=DEFAULT_MODEL_NAME,
        help="model name to fetch",
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=15.0,
        help="HTTP timeout seconds",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Only fetch the first N symbol/group rows for quick inspection",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional output json path; prints to stdout if empty",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    model_name = args.model_name.strip()
    if not model_name:
        print("model_name is required", file=sys.stderr)
        return 2

    rows = fetch_symbol_rows(args.base_url, model_name, args.timeout_sec)
    if args.limit > 0:
        rows = rows[: args.limit]

    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        symbol = str(row.get("symbol", "")).strip()
        if not symbol:
            continue
        group_key = str(row.get("group_key", "")).strip()
        detail = fetch_symbol_detail(
            args.base_url,
            model_name,
            symbol,
            group_key,
            args.timeout_sec,
        )
        grouped[symbol].append(trim_detail(detail))

    result = {
        "model_name": model_name,
        "base_url": args.base_url.rstrip("/"),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "row_count": len(rows),
        "symbol_count": len(grouped),
        "symbols": dict(sorted(grouped.items())),
    }

    rendered = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output.strip():
        output_path = Path(args.output).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
        print(f"[OK] wrote {output_path}")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
