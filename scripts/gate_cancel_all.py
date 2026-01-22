#!/usr/bin/env python3
"""Cancel all open orders on Gate.io (spot/unified + futures via REST).

Usage:
  export GATE_API_KEY="your_api_key"
  export GATE_API_SECRET="your_api_secret"
  python scripts/gate_cancel_all.py
  python scripts/gate_cancel_all.py --mode spot --spot-account unified
  python scripts/gate_cancel_all.py --mode futures --settle usdt
  python scripts/gate_cancel_all.py --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode

import requests

HOST = "https://api.gateio.ws"
PREFIX = "/api/v4"


def gen_sign(method: str, url: str, query_string: str, body: str, api_key: str, api_secret: str) -> Dict[str, str]:
    t = str(int(time.time()))
    hashed_body = hashlib.sha512(body.encode("utf-8")).hexdigest()
    sign_string = f"{method}\n{url}\n{query_string}\n{hashed_body}\n{t}"
    signature = hmac.new(api_secret.encode("utf-8"), sign_string.encode("utf-8"), hashlib.sha512).hexdigest()
    return {
        "KEY": api_key,
        "Timestamp": t,
        "SIGN": signature,
    }


def load_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("GATE_API_KEY", "").strip()
    api_secret = os.environ.get("GATE_API_SECRET", "").strip()
    missing = [name for name, value in [
        ("GATE_API_KEY", api_key),
        ("GATE_API_SECRET", api_secret)
    ] if not value]
    if missing:
        print(f"Missing env vars: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret


def build_query(params: Dict[str, Any]) -> str:
    items: List[Tuple[str, Any]] = []
    for key, value in params.items():
        if value is None or value == "":
            continue
        items.append((key, value))
    items.sort(key=lambda kv: kv[0])
    return urlencode(items, doseq=True)


def request(
    api_key: str,
    api_secret: str,
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    body: str = "",
) -> requests.Response:
    if params is None:
        params = {}
    query_string = build_query(params)
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    headers.update(gen_sign(method, PREFIX + path, query_string, body, api_key, api_secret))
    url = HOST + PREFIX + path
    if query_string:
        url = f"{url}?{query_string}"
    return requests.request(method, url, headers=headers, data=body)


def read_json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except ValueError:
        return {"raw": resp.text}


def fetch_spot_open_orders(
    api_key: str,
    api_secret: str,
    account: str,
    currency_pair: str,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    orders: List[Dict[str, Any]] = []
    page = 1
    while True:
        params = {"page": page, "limit": limit}
        if account:
            params["account"] = account
        if currency_pair:
            params["currency_pair"] = currency_pair
        resp = request(api_key, api_secret, "GET", "/spot/open_orders", params=params)
        if resp.status_code != 200:
            data = read_json(resp)
            raise RuntimeError(f"spot open_orders failed: {resp.status_code} {data}")
        data = read_json(resp)
        if isinstance(data, dict) and "orders" in data:
            batch = data.get("orders", [])
        elif isinstance(data, list):
            batch = data
        else:
            raise RuntimeError(f"unexpected spot open_orders response: {data}")
        if not batch:
            break
        orders.extend(batch)
        if len(batch) < limit:
            break
        page += 1
    return orders


def fetch_futures_open_orders(
    api_key: str,
    api_secret: str,
    settle: str,
    contract: str,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    orders: List[Dict[str, Any]] = []
    page = 1
    while True:
        params = {"status": "open", "page": page, "limit": limit}
        if contract:
            params["contract"] = contract
        resp = request(api_key, api_secret, "GET", f"/futures/{settle}/orders", params=params)
        if resp.status_code != 200:
            data = read_json(resp)
            raise RuntimeError(f"futures open_orders failed: {resp.status_code} {data}")
        data = read_json(resp)
        if not isinstance(data, list):
            raise RuntimeError(f"unexpected futures open_orders response: {data}")
        if not data:
            break
        orders.extend(data)
        if len(data) < limit:
            break
        page += 1
    return orders


def cancel_spot_order(
    api_key: str,
    api_secret: str,
    order_id: str,
    currency_pair: str,
    account: str,
) -> requests.Response:
    params = {"currency_pair": currency_pair}
    if account:
        params["account"] = account
    return request(api_key, api_secret, "DELETE", f"/spot/orders/{order_id}", params=params)


def cancel_futures_order(
    api_key: str,
    api_secret: str,
    settle: str,
    order_id: str,
    contract: str,
) -> requests.Response:
    params = {"contract": contract} if contract else {}
    return request(api_key, api_secret, "DELETE", f"/futures/{settle}/orders/{order_id}", params=params)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cancel all open orders on Gate.io (spot/unified + futures).")
    parser.add_argument(
        "--mode",
        choices=["spot", "futures", "both"],
        default="both",
        help="Which market to cancel.",
    )
    parser.add_argument(
        "--spot-account",
        default="unified",
        help="Spot account type (e.g. unified/spot).",
    )
    parser.add_argument(
        "--spot-pair",
        default="",
        help="Limit spot cancel to a currency_pair, e.g. BTC_USDT.",
    )
    parser.add_argument(
        "--settle",
        default="usdt",
        help="Futures settle currency (default: usdt).",
    )
    parser.add_argument(
        "--futures-contract",
        default="",
        help="Limit futures cancel to a contract, e.g. BTC_USDT.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only list open orders, do not cancel.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_key, api_secret = load_credentials()

    if args.mode in ("spot", "both"):
        spot_orders = fetch_spot_open_orders(
            api_key, api_secret, args.spot_account, args.spot_pair
        )
        print(f"[spot] open orders: {len(spot_orders)}")
        if not args.dry_run:
            for order in spot_orders:
                order_id = str(order.get("id") or order.get("order_id") or "")
                currency_pair = order.get("currency_pair", "") or args.spot_pair
                if not order_id or not currency_pair:
                    print(f"[spot] skip order with missing id/pair: {order}")
                    continue
                resp = cancel_spot_order(
                    api_key, api_secret, order_id, currency_pair, args.spot_account
                )
                data = read_json(resp)
                print(f"[spot] cancel order_id={order_id} {resp.status_code} {data}")

    if args.mode in ("futures", "both"):
        futures_orders = fetch_futures_open_orders(
            api_key, api_secret, args.settle, args.futures_contract
        )
        print(f"[futures] open orders: {len(futures_orders)}")
        if not args.dry_run:
            for order in futures_orders:
                order_id = str(order.get("id") or order.get("order_id") or "")
                contract = order.get("contract", "") or args.futures_contract
                if not order_id:
                    print(f"[futures] skip order with missing id: {order}")
                    continue
                resp = cancel_futures_order(
                    api_key, api_secret, args.settle, order_id, contract
                )
                data = read_json(resp)
                print(f"[futures] cancel order_id={order_id} {resp.status_code} {data}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
