#!/usr/bin/env python3
"""List or cancel all Bitget unified-account USDT futures open orders."""

from __future__ import annotations

import argparse
import base64
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

HOST = os.environ.get("BITGET_API_BASE", "https://api.bitget.com").rstrip("/")
CATEGORY = "USDT-FUTURES"


def load_credentials() -> tuple[str, str, str]:
    api_key = os.environ.get("BITGET_API_KEY", "").strip()
    api_secret = os.environ.get("BITGET_API_SECRET", "").strip()
    passphrase = os.environ.get("BITGET_API_PASSPHRASE", "").strip()
    missing = [
        name
        for name, value in (
            ("BITGET_API_KEY", api_key),
            ("BITGET_API_SECRET", api_secret),
            ("BITGET_API_PASSPHRASE", passphrase),
        )
        if not value
    ]
    if missing:
        print(f"Missing env vars: {', '.join(missing)}", file=sys.stderr)
        raise SystemExit(1)
    return api_key, api_secret, passphrase


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


def sign(api_secret: str, timestamp_ms: str, method: str, path_with_query: str, body: str) -> str:
    payload = f"{timestamp_ms}{method.upper()}{path_with_query}{body}"
    raw = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(raw).decode("utf-8")


def request(
    api_key: str,
    api_secret: str,
    passphrase: str,
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    query = urlencode([(k, v) for k, v in (params or {}).items() if v not in ("", None)])
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True) if payload else ""
    path_with_query = f"{path}?{query}" if query else path
    timestamp_ms = str(int(time.time() * 1000))
    signature = sign(api_secret, timestamp_ms, method, path_with_query, body)
    url = f"{HOST}{path_with_query}"
    headers = {
        "ACCESS-KEY": api_key,
        "ACCESS-SIGN": signature,
        "ACCESS-TIMESTAMP": timestamp_ms,
        "ACCESS-PASSPHRASE": passphrase,
        "locale": "zh-CN",
        "Content-Type": "application/json",
    }
    resp = requests.request(method.upper(), url, headers=headers, data=body)
    try:
        data = resp.json()
    except ValueError:
        raise RuntimeError(f"Bitget {method} {path} returned non-JSON: {resp.status_code} {resp.text}")
    if resp.status_code >= 300 or data.get("code") not in ("00000", "0", None):
        raise RuntimeError(f"Bitget {method} {path} failed: http={resp.status_code} body={data}")
    return data


def fetch_open_orders(api_key: str, api_secret: str, passphrase: str, symbol: Optional[str]) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {"category": CATEGORY}
    if symbol:
        params["symbol"] = symbol
    data = request(
        api_key,
        api_secret,
        passphrase,
        "GET",
        "/api/v3/trade/unfilled-orders",
        params=params,
    )
    result = data.get("data") or {}
    items = result.get("entrustedList") or result.get("list") or []
    return items if isinstance(items, list) else []


def cancel_all(api_key: str, api_secret: str, passphrase: str, symbol: Optional[str]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"category": CATEGORY}
    if symbol:
        payload["symbol"] = symbol
    return request(
        api_key,
        api_secret,
        passphrase,
        "POST",
        "/api/v3/trade/cancel-symbol-order",
        payload=payload,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List/cancel all Bitget unified-account USDT futures open orders.")
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
    api_key, api_secret, passphrase = load_credentials()
    symbols = parse_symbol_filters(args)

    if symbols:
        all_orders: List[Dict[str, Any]] = []
        for symbol in symbols:
            all_orders.extend(fetch_open_orders(api_key, api_secret, passphrase, symbol))
    else:
        all_orders = fetch_open_orders(api_key, api_secret, passphrase, None)

    print(f"[bitget] open usdt-futures orders: {len(all_orders)}")
    for order in all_orders:
        print(
            json.dumps(
                {
                    "symbol": order.get("symbol"),
                    "orderId": order.get("orderId"),
                    "clientOid": order.get("clientOid"),
                    "side": order.get("side"),
                    "orderType": order.get("orderType"),
                    "price": order.get("price"),
                    "size": order.get("size") or order.get("qty"),
                },
                ensure_ascii=True,
                sort_keys=True,
            )
        )

    if not args.execute:
        print("Dry-run only. Re-run with --execute to cancel.")
        return 0

    if symbols:
        for symbol in symbols:
            result = cancel_all(api_key, api_secret, passphrase, symbol)
            print(json.dumps({"symbol": symbol, "result": result.get("data")}, ensure_ascii=True, sort_keys=True))
    else:
        result = cancel_all(api_key, api_secret, passphrase, None)
        print(json.dumps({"scope": CATEGORY, "result": result.get("data")}, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
