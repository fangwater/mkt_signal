#!/usr/bin/env python3
"""Query a Binance unified account margin order and print its creation time."""

from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
from typing import Any, Dict

from sell_margin_spot import request_papi


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query Binance PAPI margin order time",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--base-url",
        default=(
            os.environ.get("BINANCE_PAPI_URL")
            or os.environ.get("BINANCE_FAPI_URL")
            or "https://papi.binance.com"
        ),
        help="REST base URL for PAPI",
    )
    parser.add_argument("--symbol", required=True, help="Trading pair, e.g. CHRUSDT")
    parser.add_argument("--order-id", dest="order_id", help="orderId")
    parser.add_argument("--client-order-id", dest="client_order_id", help="origClientOrderId")
    parser.add_argument(
        "--recv-window",
        dest="recv_window",
        type=int,
        help="recvWindow in milliseconds",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Print raw response body",
    )
    return parser.parse_args()


def load_credentials() -> tuple[str, str]:
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: missing BINANCE_API_KEY / BINANCE_API_SECRET", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret


def build_params(args: argparse.Namespace) -> Dict[str, Any]:
    if not args.order_id and not args.client_order_id:
        print("ERROR: require --order-id or --client-order-id.", file=sys.stderr)
        sys.exit(1)
    params: Dict[str, Any] = {"symbol": args.symbol.upper().strip()}
    if args.order_id:
        params["orderId"] = str(args.order_id).strip()
    if args.client_order_id:
        params["origClientOrderId"] = str(args.client_order_id).strip()
    if args.recv_window is not None:
        params["recvWindow"] = str(args.recv_window)
    return params


def format_ts_ms(ts_ms: int | None) -> str:
    if ts_ms is None:
        return ""
    try:
        ts_int = int(ts_ms)
    except Exception:
        return ""
    return datetime.datetime.fromtimestamp(
        ts_int / 1000,
        datetime.timezone.utc,
    ).isoformat().replace("+00:00", "Z")


def main() -> None:
    args = parse_args()
    api_key, api_secret = load_credentials()
    params = build_params(args)

    status, body, _headers = request_papi(
        args.base_url.rstrip("/"),
        "/papi/v1/margin/order",
        params,
        api_key,
        api_secret,
        method="GET",
    )

    if status != 200:
        print(f"ERR status={status} body={body}", file=sys.stderr)
        sys.exit(1)

    payload: Dict[str, Any] = {}
    try:
        parsed = json.loads(body)
        if isinstance(parsed, dict):
            payload = parsed
    except json.JSONDecodeError:
        pass

    time_ms = payload.get("time") or payload.get("updateTime")
    time_utc = format_ts_ms(time_ms)
    print(
        "status=200"
        f" symbol={params['symbol']}"
        f" orderId={payload.get('orderId', '')}"
        f" clientOrderId={payload.get('clientOrderId', payload.get('origClientOrderId', ''))}"
        f" time_ms={time_ms}"
        f" time_utc={time_utc}"
    )

    if args.raw:
        print(body)


if __name__ == "__main__":
    main()
