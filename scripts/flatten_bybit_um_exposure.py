#!/usr/bin/env python3
"""List or close all Bybit unified-account USDT futures positions."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import sys
import time
from typing import Any, Dict, List

import requests

HOST = os.environ.get("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
RECV_WINDOW_MS = "5000"


def load_credentials() -> tuple[str, str]:
    api_key = os.environ.get("BYBIT_API_KEY", "").strip()
    api_secret = os.environ.get("BYBIT_API_SECRET", "").strip()
    missing = [
        name
        for name, value in (
            ("BYBIT_API_KEY", api_key),
            ("BYBIT_API_SECRET", api_secret),
        )
        if not value
    ]
    if missing:
        print(f"Missing env vars: {', '.join(missing)}", file=sys.stderr)
        raise SystemExit(1)
    return api_key, api_secret


def normalize_symbol(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", value or "").upper()


def parse_symbol_filters(args: argparse.Namespace) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in args.symbol:
        symbol = normalize_symbol(raw)
        if symbol and symbol not in seen:
            out.append(symbol)
            seen.add(symbol)
    for chunk in args.symbols:
        for part in re.split(r"[\s,]+", chunk.strip()):
            symbol = normalize_symbol(part)
            if symbol and symbol not in seen:
                out.append(symbol)
                seen.add(symbol)
    return out


def sign(api_key: str, api_secret: str, timestamp_ms: str, payload: str) -> str:
    raw = f"{timestamp_ms}{api_key}{RECV_WINDOW_MS}{payload}"
    return hmac.new(api_secret.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()


def request(
    api_key: str,
    api_secret: str,
    method: str,
    path: str,
    *,
    query: str = "",
    body: str = "",
) -> Dict[str, Any]:
    timestamp_ms = str(int(time.time() * 1000))
    payload = body if method.upper() != "GET" else query
    signature = sign(api_key, api_secret, timestamp_ms, payload)
    url = f"{HOST}{path}"
    if query:
        url = f"{url}?{query}"
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": timestamp_ms,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW_MS,
        "Content-Type": "application/json",
    }
    resp = requests.request(method.upper(), url, headers=headers, data=body)
    try:
        data = resp.json()
    except ValueError:
        raise RuntimeError(f"Bybit {method} {path} returned non-JSON: {resp.status_code} {resp.text}")
    if resp.status_code >= 300 or data.get("retCode") not in (0, "0", None):
        raise RuntimeError(f"Bybit {method} {path} failed: http={resp.status_code} body={data}")
    return data


def fetch_positions(api_key: str, api_secret: str) -> List[Dict[str, Any]]:
    cursor = ""
    positions: List[Dict[str, Any]] = []
    while True:
        params = [("category", "linear"), ("settleCoin", "USDT"), ("limit", "200")]
        if cursor:
            params.append(("cursor", cursor))
        query = "&".join(f"{k}={v}" for k, v in params)
        data = request(api_key, api_secret, "GET", "/v5/position/list", query=query)
        result = data.get("result") or {}
        batch = result.get("list") or []
        if not isinstance(batch, list):
            break
        positions.extend(batch)
        next_cursor = (result.get("nextPageCursor") or "").strip()
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    return positions


def parse_qty(value: Any) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return 0.0


def format_decimal(value: float) -> str:
    text = f"{value:.12f}".rstrip("0").rstrip(".")
    return text if text else "0"


def build_close_payload(position: Dict[str, Any], idx: int) -> Dict[str, Any]:
    side = str(position.get("side") or "")
    qty = parse_qty(position.get("size"))
    if qty <= 0:
        raise ValueError("position size must be positive")
    payload: Dict[str, Any] = {
        "category": "linear",
        "symbol": str(position.get("symbol") or ""),
        "side": "Sell" if side.lower() == "buy" else "Buy",
        "orderType": "Market",
        "qty": format_decimal(qty),
        "reduceOnly": True,
        "orderLinkId": f"mmclose{int(time.time() * 1000)}{idx}",
    }
    if "positionIdx" in position:
        payload["positionIdx"] = int(position.get("positionIdx") or 0)
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List/close all Bybit unified-account USDT futures positions.")
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
    filters = set(parse_symbol_filters(args))

    raw_positions = fetch_positions(api_key, api_secret)
    positions = []
    for row in raw_positions:
        symbol = str(row.get("symbol") or "")
        if filters and symbol not in filters:
            continue
        qty = parse_qty(row.get("size"))
        if qty <= 0:
            continue
        side = str(row.get("side") or "")
        if side.lower() not in {"buy", "sell"}:
            continue
        positions.append(row)

    print(f"[bybit] open usdt futures positions: {len(positions)}")
    plans = []
    for idx, position in enumerate(positions, start=1):
        payload = build_close_payload(position, idx)
        plans.append(payload)
        print(json.dumps(payload, ensure_ascii=True, sort_keys=True))

    if not args.execute:
        print("Dry-run only. Re-run with --execute to place close orders.")
        return 0

    for payload in plans:
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
        result = request(api_key, api_secret, "POST", "/v5/order/create", body=body)
        print(json.dumps({"symbol": payload["symbol"], "result": result.get("result")}, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
