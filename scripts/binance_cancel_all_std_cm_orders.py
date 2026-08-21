#!/usr/bin/env python3
"""Query and optionally cancel every Binance standard-account COIN-M open order."""

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
from typing import Any, Optional

from binance_local_ip import resolve_local_address, urlopen_with_local_address


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default=os.environ.get("BINANCE_DAPI_URL") or "https://dapi.binance.com",
    )
    parser.add_argument("--symbol", action="append", default=[])
    parser.add_argument("--recv-window", type=int, default=5000)
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--local-address", default=None)
    parser.add_argument("--trade-engine-config", default=None)
    parser.add_argument("--env-dir", default=None)
    return parser.parse_args()


def signed_request(
    base_url: str,
    path: str,
    method: str,
    params: dict[str, str],
    api_key: str,
    api_secret: str,
    timeout: int,
    local_address: Optional[str],
) -> tuple[int, str]:
    query_params = dict(params)
    query_params["timestamp"] = str(int(time.time() * 1000))
    query = urllib.parse.urlencode(sorted(query_params.items()), safe="-_.~")
    signature = hmac.new(
        api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    url = f"{base_url.rstrip('/')}{path}?{query}&signature={signature}"
    request = urllib.request.Request(url, method=method, headers={"X-MBX-APIKEY": api_key})
    try:
        with urlopen_with_local_address(
            request, timeout=timeout, local_address=local_address
        ) as response:
            return response.getcode(), response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8", errors="replace")
    except Exception as error:  # pragma: no cover - operational network failure
        return 0, str(error)


def parse_orders(body: str) -> list[dict[str, Any]]:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return []
    return [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []


def main() -> None:
    args = parse_args()
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        raise SystemExit("missing BINANCE_API_KEY / BINANCE_API_SECRET")
    local_address, source = resolve_local_address(
        explicit_local_address=args.local_address,
        trade_engine_config=args.trade_engine_config,
        env_dir=args.env_dir,
    )
    filters = {value.strip().upper() for value in args.symbol if value.strip()}
    status, body = signed_request(
        args.base_url,
        "/dapi/v1/openOrders",
        "GET",
        {"recvWindow": str(args.recv_window)},
        api_key,
        api_secret,
        args.timeout,
        local_address,
    )
    if status != 200:
        print(f"open-orders query failed status={status} body={body}", file=sys.stderr)
        raise SystemExit(1)
    orders = [
        order
        for order in parse_orders(body)
        if not filters or str(order.get("symbol", "")).upper() in filters
    ]
    print(
        f"COIN-M open_orders={len(orders)} execute={args.execute} "
        f"local_address={local_address or 'system-default'} source={source}"
    )
    if not args.execute:
        for order in orders:
            print(
                f"would_cancel symbol={order.get('symbol')} orderId={order.get('orderId')} "
                f"clientOrderId={order.get('clientOrderId')}"
            )
        return
    failures = 0
    for order in orders:
        symbol = str(order.get("symbol", "")).strip().upper()
        order_id = order.get("orderId")
        if not symbol or order_id is None:
            failures += 1
            continue
        cancel_status, cancel_body = signed_request(
            args.base_url,
            "/dapi/v1/order",
            "DELETE",
            {
                "symbol": symbol,
                "orderId": str(order_id),
                "recvWindow": str(args.recv_window),
            },
            api_key,
            api_secret,
            args.timeout,
            local_address,
        )
        print(f"cancel symbol={symbol} orderId={order_id} status={cancel_status}")
        if not 200 <= cancel_status < 300:
            print(cancel_body, file=sys.stderr)
            failures += 1
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
