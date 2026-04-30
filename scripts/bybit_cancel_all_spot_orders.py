#!/usr/bin/env python3
"""List or cancel all Bybit unified-account SPOT (incl. margin) open orders."""

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


def fetch_open_orders(api_key: str, api_secret: str, symbol: Optional[str]) -> List[Dict[str, Any]]:
    cursor = ""
    orders: List[Dict[str, Any]] = []
    while True:
        params = [("category", "spot"), ("limit", "50")]
        if symbol:
            params.append(("symbol", symbol))
        if cursor:
            params.append(("cursor", cursor))
        query = "&".join(f"{k}={v}" for k, v in params)
        data = request(api_key, api_secret, "GET", "/v5/order/realtime", query=query)
        result = data.get("result") or {}
        batch = result.get("list") or []
        if not isinstance(batch, list):
            break
        orders.extend(batch)
        next_cursor = (result.get("nextPageCursor") or "").strip()
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    return orders


def cancel_all(api_key: str, api_secret: str, symbol: Optional[str]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"category": "spot"}
    if symbol:
        payload["symbol"] = symbol
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
    return request(api_key, api_secret, "POST", "/v5/order/cancel-all", body=body)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List/cancel all Bybit unified-account spot open orders.")
    parser.add_argument("--symbol", action="append", default=[], help="Filter by symbol, e.g. BTCUSDT.")
    parser.add_argument(
        "--symbols",
        action="append",
        default=[],
        help="Comma/space separated symbol list, e.g. BTCUSDT,ETHUSDT.",
    )
    parser.add_argument("--execute", action="store_true", help="Actually cancel orders.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_key, api_secret = load_credentials()
    symbols = parse_symbol_filters(args)

    if symbols:
        all_orders: List[Dict[str, Any]] = []
        for symbol in symbols:
            all_orders.extend(fetch_open_orders(api_key, api_secret, symbol))
    else:
        all_orders = fetch_open_orders(api_key, api_secret, None)

    print(f"[bybit] open spot orders: {len(all_orders)}")
    for order in all_orders:
        print(
            json.dumps(
                {
                    "symbol": order.get("symbol"),
                    "orderId": order.get("orderId"),
                    "orderLinkId": order.get("orderLinkId"),
                    "side": order.get("side"),
                    "orderType": order.get("orderType"),
                    "price": order.get("price"),
                    "qty": order.get("qty"),
                    "isLeverage": order.get("isLeverage"),
                },
                ensure_ascii=True,
                sort_keys=True,
            )
        )

    if not args.execute:
        print("Dry-run only. Re-run with --execute to cancel.")
        return 0

    if not all_orders:
        print("No open orders to cancel.")
        return 0

    if symbols:
        for symbol in symbols:
            result = cancel_all(api_key, api_secret, symbol)
            print(
                json.dumps(
                    {"symbol": symbol, "result": result.get("result")},
                    ensure_ascii=True,
                    sort_keys=True,
                )
            )
    else:
        result = cancel_all(api_key, api_secret, None)
        print(json.dumps({"scope": "spot", "result": result.get("result")}, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
