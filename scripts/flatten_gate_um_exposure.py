#!/usr/bin/env python3
"""List or close all Gate unified-account USDT futures positions."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

HOST = os.environ.get("GATE_API_BASE", "https://api.gateio.ws").rstrip("/")
PREFIX = "/api/v4"


def load_credentials() -> tuple[str, str]:
    api_key = os.environ.get("GATE_API_KEY", "").strip()
    api_secret = os.environ.get("GATE_API_SECRET", "").strip()
    missing = [
        name
        for name, value in (
            ("GATE_API_KEY", api_key),
            ("GATE_API_SECRET", api_secret),
        )
        if not value
    ]
    if missing:
        print(f"Missing env vars: {', '.join(missing)}", file=sys.stderr)
        raise SystemExit(1)
    return api_key, api_secret


def normalize_contract(value: str) -> str:
    upper = (value or "").upper()
    if upper.endswith("_USDT"):
        return upper
    cleaned = re.sub(r"[^A-Za-z0-9]", "", upper)
    if cleaned.endswith("USDT") and len(cleaned) > 4:
        return f"{cleaned[:-4]}_USDT"
    return upper


def parse_contract_filters(args: argparse.Namespace) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in args.symbol:
        contract = normalize_contract(raw)
        if contract and contract not in seen:
            out.append(contract)
            seen.add(contract)
    for chunk in args.symbols:
        for part in re.split(r"[\s,]+", chunk.strip()):
            contract = normalize_contract(part)
            if contract and contract not in seen:
                out.append(contract)
                seen.add(contract)
    return out


def build_query(params: Dict[str, Any]) -> str:
    items = [(k, v) for k, v in params.items() if v not in ("", None)]
    items.sort(key=lambda item: item[0])
    return urlencode(items, doseq=True)


def sign(method: str, url: str, query_string: str, body: str, api_secret: str) -> Dict[str, str]:
    timestamp = str(int(time.time()))
    body_hash = hashlib.sha512(body.encode("utf-8")).hexdigest()
    payload = f"{method}\n{url}\n{query_string}\n{body_hash}\n{timestamp}"
    signature = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha512).hexdigest()
    return {"Timestamp": timestamp, "SIGN": signature}


def request(
    api_key: str,
    api_secret: str,
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> Any:
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True) if payload else ""
    query_string = build_query(params or {})
    url = f"{HOST}{PREFIX}{path}"
    if query_string:
        url = f"{url}?{query_string}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "KEY": api_key,
    }
    headers.update(sign(method.upper(), f"{PREFIX}{path}", query_string, body, api_secret))
    resp = requests.request(method.upper(), url, headers=headers, data=body)
    try:
        data = resp.json()
    except ValueError:
        raise RuntimeError(f"Gate {method} {path} returned non-JSON: {resp.status_code} {resp.text}")
    if resp.status_code >= 300:
        raise RuntimeError(f"Gate {method} {path} failed: http={resp.status_code} body={data}")
    return data


def fetch_positions(api_key: str, api_secret: str) -> List[Dict[str, Any]]:
    data = request(api_key, api_secret, "GET", "/futures/usdt/positions")
    return data if isinstance(data, list) else []


def parse_qty(value: Any) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return 0.0


def format_decimal(value: float) -> str:
    text = f"{value:.12f}".rstrip("0").rstrip(".")
    return text if text else "0"


def build_close_payload(position: Dict[str, Any], idx: int) -> Dict[str, Any]:
    size = parse_qty(position.get("size"))
    if size == 0:
        raise ValueError("position size is zero")
    signed_size = abs(size) if size < 0 else -abs(size)
    return {
        "contract": str(position.get("contract") or ""),
        "size": format_decimal(signed_size),
        "price": "0",
        "tif": "ioc",
        "account": "unified",
        "reduce_only": True,
        "text": f"t-mmclose{int(time.time() * 1000)}{idx}",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List/close all Gate unified-account USDT futures positions.")
    parser.add_argument("--symbol", action="append", default=[], help="Filter by symbol, e.g. BTCUSDT.")
    parser.add_argument(
        "--symbols",
        action="append",
        default=[],
        help="Comma/space separated symbol list, e.g. BTCUSDT,ETHUSDT.",
    )
    parser.add_argument("--execute", action="store_true", help="Actually place close orders.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_key, api_secret = load_credentials()
    filters = set(parse_contract_filters(args))

    raw_positions = fetch_positions(api_key, api_secret)
    positions = []
    for row in raw_positions:
        contract = str(row.get("contract") or "")
        if filters and contract not in filters:
            continue
        size = parse_qty(row.get("size"))
        if size == 0:
            continue
        positions.append(row)

    print(f"[gate] open usdt futures positions: {len(positions)}")
    plans = []
    for idx, position in enumerate(positions, start=1):
        payload = build_close_payload(position, idx)
        plans.append(payload)
        print(json.dumps(payload, ensure_ascii=True, sort_keys=True))

    if not args.execute:
        print("Dry-run only. Re-run with --execute to place close orders.")
        return 0

    for payload in plans:
        result = request(
            api_key,
            api_secret,
            "POST",
            "/futures/usdt/orders",
            payload=payload,
        )
        print(json.dumps({"contract": payload["contract"], "result": result}, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
