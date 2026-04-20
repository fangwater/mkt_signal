#!/usr/bin/env python3
"""List or cancel all Gate unified-account USDT futures open orders."""

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
) -> Any:
    body = ""
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


def fetch_open_orders(api_key: str, api_secret: str, contract: Optional[str]) -> List[Dict[str, Any]]:
    orders: List[Dict[str, Any]] = []
    page = 1
    while True:
        params: Dict[str, Any] = {"status": "open", "page": page, "limit": 100}
        if contract:
            params["contract"] = contract
        batch = request(api_key, api_secret, "GET", "/futures/usdt/orders", params=params)
        if not isinstance(batch, list):
            break
        if not batch:
            break
        orders.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return orders


def cancel_order(api_key: str, api_secret: str, order_id: str, contract: str) -> Any:
    params = {"contract": contract} if contract else {}
    return request(api_key, api_secret, "DELETE", f"/futures/usdt/orders/{order_id}", params=params)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List/cancel all Gate unified-account USDT futures open orders.")
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
    contracts = parse_contract_filters(args)

    if contracts:
        all_orders: List[Dict[str, Any]] = []
        for contract in contracts:
            all_orders.extend(fetch_open_orders(api_key, api_secret, contract))
    else:
        all_orders = fetch_open_orders(api_key, api_secret, None)

    print(f"[gate] open usdt futures orders: {len(all_orders)}")
    for order in all_orders:
        print(
            json.dumps(
                {
                    "contract": order.get("contract"),
                    "id": order.get("id"),
                    "text": order.get("text"),
                    "status": order.get("status"),
                    "price": order.get("price"),
                    "size": order.get("size"),
                },
                ensure_ascii=True,
                sort_keys=True,
            )
        )

    if not args.execute:
        print("Dry-run only. Re-run with --execute to cancel.")
        return 0

    for order in all_orders:
        order_id = str(order.get("id") or "")
        contract = str(order.get("contract") or "")
        if not order_id or not contract:
            continue
        result = cancel_order(api_key, api_secret, order_id, contract)
        print(json.dumps({"contract": contract, "id": order_id, "result": result}, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
