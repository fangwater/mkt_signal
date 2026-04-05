#!/usr/bin/env python3
"""Cancel all Binance STANDARD spot open orders via REST.

Default behavior is dry-run:
  1) query current spot open orders
  2) print grouped cancel plan by symbol

Use --execute to actually send `DELETE /api/v3/openOrders` per symbol.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional


DEFAULT_BASE_URL = os.environ.get("BINANCE_SPOT_API_URL") or "https://api.binance.com"
OPEN_ORDERS_PATH = "/api/v3/openOrders"


@dataclass(frozen=True)
class OpenOrder:
    symbol: str
    order_id: Optional[int]
    orig_client_order_id: str
    side: str
    order_type: str
    price: str
    orig_qty: str
    executed_qty: str
    status: str

    @property
    def key(self) -> tuple[str, Optional[int], str]:
        return (self.symbol, self.order_id, self.orig_client_order_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cancel all Binance STANDARD spot open orders via REST",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="Binance spot REST base URL",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        default=[],
        help="Restrict to specific symbol(s); repeatable",
    )
    parser.add_argument(
        "--recv-window",
        type=int,
        default=5000,
        help="recvWindow in milliseconds",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="HTTP timeout in seconds",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually cancel orders; default is dry-run",
    )
    return parser.parse_args()


def now_ms() -> int:
    return int(time.time() * 1000)


def sign_query(params: Dict[str, str], secret: str) -> str:
    items = sorted((k, str(v)) for k, v in params.items() if v is not None)
    query = urllib.parse.urlencode(items, safe="-_.~")
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def load_credentials() -> tuple[str, str]:
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: missing BINANCE_API_KEY / BINANCE_API_SECRET", file=sys.stderr)
        raise SystemExit(1)
    return api_key, api_secret


def normalize_symbols(raw_symbols: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in raw_symbols:
        symbol = raw.strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def signed_rest_request(
    *,
    base_url: str,
    path: str,
    params: Dict[str, str],
    api_key: str,
    api_secret: str,
    method: str,
    timeout: int,
) -> tuple[int, str, dict[str, str]]:
    query_params = dict(params)
    query_params.setdefault("timestamp", str(now_ms()))
    query = urllib.parse.urlencode(sorted(query_params.items()), safe="-_.~")
    signature = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    url = f"{base_url.rstrip('/')}{path}?{query}&signature={signature}"
    req = urllib.request.Request(url, method=method, headers={"X-MBX-APIKEY": api_key})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
            headers = dict(resp.headers.items())
            return status, body, headers
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        headers = dict(getattr(exc, "headers", {}).items()) if getattr(exc, "headers", None) else {}
        return exc.code, body, headers
    except Exception as exc:
        return 0, str(exc), {}


def header_value(headers: dict[str, str], key: str) -> str:
    return headers.get(key) or headers.get(key.lower()) or "-"


def parse_order_id(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def parse_open_orders(body: str) -> list[OpenOrder]:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []

    orders: list[OpenOrder] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        orders.append(
            OpenOrder(
                symbol=symbol,
                order_id=parse_order_id(item.get("orderId")),
                orig_client_order_id=str(item.get("clientOrderId", "")).strip(),
                side=str(item.get("side", "")).strip().upper(),
                order_type=str(item.get("type", "")).strip().upper(),
                price=str(item.get("price", "")).strip(),
                orig_qty=str(item.get("origQty", "")).strip(),
                executed_qty=str(item.get("executedQty", "")).strip(),
                status=str(item.get("status", "")).strip().upper(),
            )
        )
    return orders


def query_open_orders(
    *,
    base_url: str,
    api_key: str,
    api_secret: str,
    recv_window: int,
    timeout: int,
    requested_symbols: list[str],
) -> list[OpenOrder]:
    all_orders: list[OpenOrder] = []
    query_symbols = requested_symbols or [""]

    for symbol in query_symbols:
        params: Dict[str, str] = {"recvWindow": str(recv_window)}
        if symbol:
            params["symbol"] = symbol
        status, body, headers = signed_rest_request(
            base_url=base_url,
            path=OPEN_ORDERS_PATH,
            params=params,
            api_key=api_key,
            api_secret=api_secret,
            method="GET",
            timeout=timeout,
        )
        orders = parse_open_orders(body)
        scope = symbol or "ALL"
        print(
            f"[query] symbol={scope} status={status} open_orders={len(orders)} "
            f"used_weight_1m={header_value(headers, 'x-mbx-used-weight-1m')} "
            f"order_count_1m={header_value(headers, 'x-mbx-order-count-1m')}"
        )
        if status != 200:
            print(f"[query] failed for symbol={scope}: {body}", file=sys.stderr)
            raise SystemExit(1)
        all_orders.extend(orders)

    dedup: dict[tuple[str, Optional[int], str], OpenOrder] = {}
    for order in all_orders:
        dedup[order.key] = order
    return sorted(dedup.values(), key=lambda order: (order.symbol, order.order_id or -1, order.orig_client_order_id))


def print_cancel_plan(orders: list[OpenOrder], execute: bool) -> None:
    grouped: dict[str, list[OpenOrder]] = defaultdict(list)
    for order in orders:
        grouped[order.symbol].append(order)
    print(f"[plan] symbols={len(grouped)} open_orders={len(orders)} execute={execute}")
    for symbol in sorted(grouped):
        print(f"[plan] symbol={symbol} open_orders={len(grouped[symbol])}")
        for order in grouped[symbol]:
            order_ref = (
                f"orderId={order.order_id}" if order.order_id is not None else f"clientOrderId={order.orig_client_order_id}"
            )
            print(
                f"  [order] {order_ref} side={order.side} type={order.order_type} "
                f"price={order.price} origQty={order.orig_qty} executedQty={order.executed_qty} status={order.status}"
            )


def cancel_symbol(
    *,
    base_url: str,
    symbol: str,
    api_key: str,
    api_secret: str,
    recv_window: int,
    timeout: int,
) -> tuple[int, str, dict[str, str]]:
    return signed_rest_request(
        base_url=base_url,
        path=OPEN_ORDERS_PATH,
        params={"symbol": symbol, "recvWindow": str(recv_window)},
        api_key=api_key,
        api_secret=api_secret,
        method="DELETE",
        timeout=timeout,
    )


def main() -> None:
    args = parse_args()
    api_key, api_secret = load_credentials()
    requested_symbols = normalize_symbols(args.symbol)
    orders = query_open_orders(
        base_url=args.base_url,
        api_key=api_key,
        api_secret=api_secret,
        recv_window=args.recv_window,
        timeout=args.timeout,
        requested_symbols=requested_symbols,
    )
    print_cancel_plan(orders, args.execute)
    if not args.execute or not orders:
        return

    symbols = sorted({order.symbol for order in orders})
    failed = False
    for symbol in symbols:
        status, body, headers = cancel_symbol(
            base_url=args.base_url,
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            recv_window=args.recv_window,
            timeout=args.timeout,
        )
        print(
            f"[cancel] symbol={symbol} status={status} "
            f"used_weight_1m={header_value(headers, 'x-mbx-used-weight-1m')} "
            f"order_count_1m={header_value(headers, 'x-mbx-order-count-1m')}"
        )
        if status != 200:
            failed = True
            print(f"[cancel] failed symbol={symbol}: {body}", file=sys.stderr)
            continue
        try:
            payload = json.loads(body)
            cancelled = len(payload) if isinstance(payload, list) else 1
        except json.JSONDecodeError:
            cancelled = -1
        print(f"[cancel] symbol={symbol} cancelled={cancelled}")

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
