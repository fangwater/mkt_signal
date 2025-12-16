#!/usr/bin/env python3
"""Test whether Binance UM supports futures order placement under unified (Portfolio Margin) account.

This script can try both APIs with the same credentials:
  - Standard UM Futures API (FAPI): POST /fapi/v1/order
  - Portfolio Margin API (PAPI):   POST /papi/v1/um/order

Recommended safe usage: place a LIMIT+GTX order away from mark price and auto-cancel it.

Examples:
  BINANCE_API_KEY=... BINANCE_API_SECRET=... \\
    python scripts/test_binance_unified_um_order.py \\
      --symbol BTCUSDT --side BUY --quantity 0.001 \\
      --type LIMIT --auto-price --offset-bps 50 \\
      --place-order

  # Only test FAPI (useful to see whether unified account keys can place via /fapi)
  BINANCE_API_KEY=... BINANCE_API_SECRET=... \\
    python scripts/test_binance_unified_um_order.py \\
      --only fapi --symbol BTCUSDT --side BUY --quantity 0.001 \\
      --type LIMIT --auto-price --place-order
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional, Tuple

from sell_margin_spot import request_papi


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test Binance unified account UM order placement via FAPI and/or PAPI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--only",
        choices=["both", "fapi", "papi"],
        default="both",
        help="Which API to test",
    )
    parser.add_argument(
        "--fapi-base-url",
        default=(os.environ.get("BINANCE_FAPI_URL") or "https://fapi.binance.com"),
        help="Binance UM Futures REST base URL (FAPI)",
    )
    parser.add_argument(
        "--papi-base-url",
        default=(os.environ.get("BINANCE_PAPI_URL") or "https://papi.binance.com"),
        help="Binance Portfolio Margin REST base URL (PAPI)",
    )
    parser.add_argument(
        "--testnet",
        action="store_true",
        help="Use Binance UM futures testnet for FAPI (https://testnet.binancefuture.com)",
    )

    parser.add_argument("--symbol", required=True, help="Contract symbol, e.g. BTCUSDT")
    parser.add_argument(
        "--side",
        default="BUY",
        choices=["BUY", "SELL"],
        help="Order side",
    )
    parser.add_argument(
        "--type",
        default="LIMIT",
        choices=["LIMIT", "MARKET"],
        help="Order type (LIMIT recommended for safe test + cancel)",
    )
    parser.add_argument("--quantity", required=True, help="Order quantity (base asset amount)")
    parser.add_argument("--price", help="Price (required for LIMIT unless --auto-price)")
    parser.add_argument(
        "--time-in-force",
        dest="time_in_force",
        default=None,
        help="Time in force for LIMIT, e.g. GTX (post-only), GTC",
    )
    parser.add_argument(
        "--position-side",
        dest="position_side",
        choices=["BOTH", "LONG", "SHORT"],
        default="BOTH",
        help="Position side (hedge mode)",
    )
    parser.add_argument("--reduce-only", action="store_true", help="Send reduceOnly=true")
    parser.add_argument(
        "--recv-window",
        dest="recv_window",
        type=int,
        default=5000,
        help="recvWindow in milliseconds",
    )

    parser.add_argument(
        "--auto-price",
        action="store_true",
        help="For LIMIT orders: compute a price from mark price +/- offset-bps",
    )
    parser.add_argument(
        "--offset-bps",
        type=float,
        default=50.0,
        help="Price offset in bps (1 bps = 0.01%) used with --auto-price",
    )

    parser.add_argument(
        "--place-order",
        action="store_true",
        help="Actually submit the order (otherwise only queries account endpoints)",
    )
    parser.add_argument(
        "--skip-cancel",
        action="store_true",
        help="Do not auto-cancel LIMIT orders after placement",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="HTTP timeout seconds",
    )
    return parser.parse_args()


def load_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: please set BINANCE_API_KEY / BINANCE_API_SECRET in environment.", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret


def http_get_json(url: str, timeout: int) -> Tuple[int, Any, str]:
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        status = exc.code
        body = exc.read().decode("utf-8", errors="replace")
    except Exception as exc:  # pragma: no cover - network failure
        return 0, None, str(exc)

    try:
        return status, json.loads(body), body
    except json.JSONDecodeError:
        return status, None, body


def get_mark_price(fapi_base_url: str, symbol: str, timeout: int) -> Optional[float]:
    url = f"{fapi_base_url.rstrip('/')}/fapi/v1/premiumIndex?{urllib.parse.urlencode({'symbol': symbol})}"
    status, parsed, raw = http_get_json(url, timeout=timeout)
    if status != 200 or not isinstance(parsed, dict):
        print(f"WARN: failed to query mark price: status={status} body={raw}", file=sys.stderr)
        return None
    try:
        return float(parsed.get("markPrice"))
    except Exception:
        return None


def compute_limit_price(mark_price: float, side: str, offset_bps: float) -> str:
    offset = max(0.0, float(offset_bps)) / 10000.0
    if side.upper() == "BUY":
        price = mark_price * (1.0 - offset)
    else:
        price = mark_price * (1.0 + offset)
    return f"{price:.6f}".rstrip("0").rstrip(".")


def print_json(label: str, body: str) -> None:
    try:
        parsed = json.loads(body)
        formatted = json.dumps(parsed, ensure_ascii=False, indent=2, sort_keys=True)
        print(f"{label}:\n{formatted}")
    except json.JSONDecodeError:
        print(f"{label}: {body}")


def signed_get(
    base_url: str,
    path: str,
    params: Dict[str, str],
    api_key: str,
    api_secret: str,
    timeout: int,
) -> Tuple[int, str]:
    status, body, _headers = request_papi(
        base_url.rstrip("/"),
        path,
        params,
        api_key,
        api_secret,
        method="GET",
        timeout=timeout,
    )
    return status, body


def signed_call(
    base_url: str,
    path: str,
    params: Dict[str, str],
    api_key: str,
    api_secret: str,
    method: str,
    timeout: int,
) -> Tuple[int, str]:
    status, body, _headers = request_papi(
        base_url.rstrip("/"),
        path,
        params,
        api_key,
        api_secret,
        method=method,
        timeout=timeout,
    )
    return status, body


def extract_order_id(body: str) -> Optional[int]:
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, dict) and "orderId" in parsed:
        try:
            return int(parsed["orderId"])
        except Exception:
            return None
    return None


def main() -> None:
    args = parse_args()
    api_key, api_secret = load_credentials()

    symbol = args.symbol.upper().strip()
    side = args.side.upper()
    order_type = args.type.upper()

    fapi_base_url = args.fapi_base_url
    if args.testnet and not os.environ.get("BINANCE_FAPI_URL"):
        fapi_base_url = "https://testnet.binancefuture.com"
    papi_base_url = args.papi_base_url

    print(f"Target: symbol={symbol} side={side} type={order_type} quantity={args.quantity}")
    print(f"FAPI base: {fapi_base_url.rstrip('/')}")
    print(f"PAPI base: {papi_base_url.rstrip('/')}")

    mark_price: Optional[float] = None
    if order_type == "LIMIT" and args.auto_price:
        mark_price = get_mark_price(fapi_base_url, symbol, timeout=args.timeout)
        if mark_price is not None:
            print(f"Mark price: {mark_price}")

    if args.only in {"both", "fapi"}:
        status, body = signed_get(
            fapi_base_url,
            "/fapi/v2/account",
            {"recvWindow": str(args.recv_window)},
            api_key,
            api_secret,
            timeout=args.timeout,
        )
        print_json("FAPI /fapi/v2/account", body)
        if status != 200:
            print(f"FAPI account query failed: status={status}", file=sys.stderr)

    if args.only in {"both", "papi"}:
        status, body = signed_get(
            papi_base_url,
            "/papi/v1/um/account",
            {"recvWindow": str(args.recv_window)},
            api_key,
            api_secret,
            timeout=args.timeout,
        )
        print_json("PAPI /papi/v1/um/account", body)
        if status != 200:
            print(f"PAPI account query failed: status={status}", file=sys.stderr)

    if not args.place_order:
        print("No order submitted (use --place-order to actually test order placement).")
        return

    params: Dict[str, str] = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": str(args.quantity).strip(),
        "positionSide": args.position_side.upper(),
        "recvWindow": str(args.recv_window),
    }
    if args.reduce_only:
        params["reduceOnly"] = "true"

    if order_type == "LIMIT":
        if args.time_in_force:
            params["timeInForce"] = args.time_in_force.strip().upper()
        else:
            params["timeInForce"] = "GTX"

        if args.price:
            params["price"] = str(args.price).strip()
        elif args.auto_price and mark_price is not None:
            params["price"] = compute_limit_price(mark_price, side=side, offset_bps=args.offset_bps)
        else:
            print("ERROR: LIMIT order requires --price or (--auto-price and mark price available).", file=sys.stderr)
            sys.exit(1)

        print(
            f"LIMIT params: price={params.get('price')} timeInForce={params.get('timeInForce')} "
            f"offset_bps={args.offset_bps if args.auto_price else '-'}"
        )

    results: Dict[str, Tuple[int, str]] = {}

    if args.only in {"both", "fapi"}:
        status, body = signed_call(
            fapi_base_url,
            "/fapi/v1/order",
            params,
            api_key,
            api_secret,
            method="POST",
            timeout=args.timeout,
        )
        results["fapi_order"] = (status, body)
        print_json("FAPI POST /fapi/v1/order", body)

        if order_type == "LIMIT" and not args.skip_cancel:
            order_id = extract_order_id(body)
            if status == 200 and order_id is not None:
                c_status, c_body = signed_call(
                    fapi_base_url,
                    "/fapi/v1/order",
                    {"symbol": symbol, "orderId": str(order_id), "recvWindow": str(args.recv_window)},
                    api_key,
                    api_secret,
                    method="DELETE",
                    timeout=args.timeout,
                )
                results["fapi_cancel"] = (c_status, c_body)
                print_json("FAPI DELETE /fapi/v1/order", c_body)

    if args.only in {"both", "papi"}:
        status, body = signed_call(
            papi_base_url,
            "/papi/v1/um/order",
            params,
            api_key,
            api_secret,
            method="POST",
            timeout=args.timeout,
        )
        results["papi_order"] = (status, body)
        print_json("PAPI POST /papi/v1/um/order", body)

        if order_type == "LIMIT" and not args.skip_cancel:
            order_id = extract_order_id(body)
            if status == 200 and order_id is not None:
                c_status, c_body = signed_call(
                    papi_base_url,
                    "/papi/v1/um/order",
                    {"symbol": symbol, "orderId": str(order_id), "recvWindow": str(args.recv_window)},
                    api_key,
                    api_secret,
                    method="DELETE",
                    timeout=args.timeout,
                )
                results["papi_cancel"] = (c_status, c_body)
                print_json("PAPI DELETE /papi/v1/um/order", c_body)

    attempted = [k for k in results.keys() if k.endswith("_order")]
    ok = [k for k, (st, _b) in results.items() if k.endswith("_order") and 200 <= st < 300]
    if attempted and not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()

