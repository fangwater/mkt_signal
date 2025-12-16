#!/usr/bin/env python3
"""Test Binance UM futures order placement via WebSocket API under unified account (Portfolio Margin).

This script is meant to answer: "In unified account mode, can Binance UM place futures orders via WebSocket?"

It supports trying (configurable):
  - UM Futures WebSocket API (FAPI WS): wss://ws-fapi.binance.com/ws-fapi/v1
  - Portfolio Margin WebSocket API (PAPI WS): configurable via --papi-ws-url (if your account uses it)

By default it only probes the endpoint with a public method (time). To actually place an order,
pass --place-order (LIMIT + GTX is recommended) and optionally auto-cancel.

Dependencies:
  pip install websocket-client

Examples:
  # Probe FAPI WS endpoint
  python scripts/test_binance_unified_um_ws_order.py --only fapi

  # Place a safe LIMIT+GTX and auto-cancel (FAPI WS)
  BINANCE_API_KEY=... BINANCE_API_SECRET=... \\
    python scripts/test_binance_unified_um_ws_order.py \\
      --only fapi --symbol BTCUSDT --side BUY --quantity 0.001 \\
      --type LIMIT --auto-price --offset-bps 50 --place-order
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import ssl
import sys
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional, Tuple


DEFAULT_FAPI_WS_URL = "wss://ws-fapi.binance.com/ws-fapi/v1"
DEFAULT_FAPI_WS_TESTNET_URL = "wss://testnet.binancefuture.com/ws-fapi/v1"

# PAPI WS url differs by environment/account; keep it configurable instead of hard-coding.
DEFAULT_PAPI_WS_URL = os.environ.get("BINANCE_PAPI_WS_URL", "").strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test Binance UM futures WebSocket trading under unified account",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--only",
        choices=["both", "fapi", "papi"],
        default="fapi",
        help="Which WS endpoint to test",
    )
    parser.add_argument(
        "--fapi-ws-url",
        default=(os.environ.get("BINANCE_FAPI_WS_URL") or DEFAULT_FAPI_WS_URL),
        help="UM Futures WebSocket API URL",
    )
    parser.add_argument(
        "--papi-ws-url",
        default=DEFAULT_PAPI_WS_URL,
        help="Portfolio Margin WebSocket API URL (set BINANCE_PAPI_WS_URL or pass this flag)",
    )
    parser.add_argument(
        "--testnet",
        action="store_true",
        help="Use UM futures testnet WS URL for FAPI (only if --fapi-ws-url not set)",
    )

    parser.add_argument("--symbol", default="BTCUSDT", help="Contract symbol, e.g. BTCUSDT")
    parser.add_argument("--side", default="BUY", choices=["BUY", "SELL"], help="Order side")
    parser.add_argument("--type", default="LIMIT", choices=["LIMIT", "MARKET"], help="Order type")
    parser.add_argument("--quantity", default="0.001", help="Order quantity (base asset amount)")
    parser.add_argument("--price", help="LIMIT price (required unless --auto-price)")
    parser.add_argument(
        "--time-in-force",
        dest="time_in_force",
        default="GTX",
        help="LIMIT timeInForce (GTX is post-only)",
    )
    parser.add_argument(
        "--position-side",
        dest="position_side",
        choices=["BOTH", "LONG", "SHORT"],
        default="BOTH",
        help="Position side (hedge mode)",
    )
    parser.add_argument("--reduce-only", action="store_true", help="Send reduceOnly=true")
    parser.add_argument("--recv-window", dest="recv_window", type=int, default=5000, help="recvWindow ms")

    parser.add_argument("--auto-price", action="store_true", help="Compute LIMIT price from mark price")
    parser.add_argument("--offset-bps", type=float, default=50.0, help="Offset bps for --auto-price")

    parser.add_argument("--probe-only", action="store_true", help="Only call public method (time), no auth")
    parser.add_argument("--place-order", action="store_true", help="Submit order.place over WS")
    parser.add_argument("--skip-cancel", action="store_true", help="Do not auto-cancel LIMIT orders")
    parser.add_argument("--timeout", type=int, default=10, help="WS timeout seconds")
    return parser.parse_args()


def try_import_ws():
    try:
        import websocket  # type: ignore

        return websocket
    except Exception:
        return None


def now_ms() -> int:
    return int(time.time() * 1000)


def sign_params(params: Dict[str, str], secret: str) -> str:
    items = sorted((k, str(v)) for k, v in params.items() if v is not None)
    query = urllib.parse.urlencode(items, safe="-_.~")
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


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
    except Exception as exc:  # pragma: no cover - network failure
        return 0, None, str(exc)

    try:
        return status, json.loads(body), body
    except json.JSONDecodeError:
        return status, None, body


def get_mark_price(fapi_rest_base_url: str, symbol: str, timeout: int) -> Optional[float]:
    url = f"{fapi_rest_base_url.rstrip('/')}/fapi/v1/premiumIndex?{urllib.parse.urlencode({'symbol': symbol})}"
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


def pretty(label: str, obj: Any) -> None:
    try:
        print(f"{label}:\n{json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)}")
    except Exception:
        print(f"{label}: {obj}")


def ws_call(ws, payload: Dict[str, Any]) -> Dict[str, Any]:
    ws.send(json.dumps(payload))
    raw = ws.recv()
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
        return {"raw": raw}
    except json.JSONDecodeError:
        return {"raw": raw}


def extract_ws_order_id(resp: Dict[str, Any]) -> Optional[int]:
    if not isinstance(resp, dict):
        return None
    res = resp.get("result")
    if isinstance(res, dict) and "orderId" in res:
        try:
            return int(res["orderId"])
        except Exception:
            return None
    return None


def run_endpoint(
    ws_url: str,
    name: str,
    args: argparse.Namespace,
    api_key: Optional[str],
    api_secret: Optional[str],
) -> Tuple[bool, Optional[int]]:
    websocket = try_import_ws()
    if websocket is None:
        print("ERROR: missing dependency websocket-client. Install via: pip install websocket-client", file=sys.stderr)
        return False, None

    sslopt = {"cert_reqs": ssl.CERT_REQUIRED}
    print(f"\n=== {name} ===")
    print(f"WS URL: {ws_url}")

    try:
        ws = websocket.create_connection(ws_url, timeout=args.timeout, sslopt=sslopt)
    except Exception as exc:
        print(f"ERROR: connect failed: {exc}", file=sys.stderr)
        return False, None

    try:
        probe = {"id": 1, "method": "time", "params": {}}
        resp = ws_call(ws, probe)
        pretty("probe(time) response", resp)

        if args.probe_only and not args.place_order:
            return True, None

        if not args.place_order:
            print("No order submitted (use --place-order).")
            return True, None

        if api_key is None or api_secret is None:
            print("ERROR: missing BINANCE_API_KEY / BINANCE_API_SECRET for --place-order", file=sys.stderr)
            return False, None

        symbol = args.symbol.upper().strip()
        side = args.side.upper()
        order_type = args.type.upper()

        params: Dict[str, str] = {
            "apiKey": api_key,
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": str(args.quantity).strip(),
            "positionSide": args.position_side.upper(),
            "recvWindow": str(args.recv_window),
            "timestamp": str(now_ms()),
        }
        if args.reduce_only:
            params["reduceOnly"] = "true"

        if order_type == "LIMIT":
            params["timeInForce"] = str(args.time_in_force).strip().upper()
            if args.price:
                params["price"] = str(args.price).strip()
            elif args.auto_price:
                # REST mark price can be queried from either prod/testnet; use the matching REST base.
                if "testnet" in ws_url:
                    rest_base = "https://testnet.binancefuture.com"
                else:
                    rest_base = "https://fapi.binance.com"
                mark = get_mark_price(rest_base, symbol, timeout=args.timeout)
                if mark is None:
                    print("ERROR: --auto-price requested but mark price query failed.", file=sys.stderr)
                    return False, None
                params["price"] = compute_limit_price(mark, side=side, offset_bps=args.offset_bps)
                print(f"Auto price: mark={mark} -> price={params['price']} offset_bps={args.offset_bps}")
            else:
                print("ERROR: LIMIT requires --price or --auto-price", file=sys.stderr)
                return False, None

        signature = sign_params(params, api_secret)
        signed_params: Dict[str, Any] = dict(params)
        signed_params["signature"] = signature

        place_payload = {"id": 2, "method": "order.place", "params": signed_params}
        resp = ws_call(ws, place_payload)
        pretty("order.place response", resp)

        order_id = extract_ws_order_id(resp)
        ok = "error" not in resp

        if ok and order_type == "LIMIT" and order_id is not None and not args.skip_cancel:
            cancel_params: Dict[str, str] = {
                "apiKey": api_key,
                "symbol": symbol,
                "orderId": str(order_id),
                "recvWindow": str(args.recv_window),
                "timestamp": str(now_ms()),
            }
            cancel_sig = sign_params(cancel_params, api_secret)
            cancel_payload = {
                "id": 3,
                "method": "order.cancel",
                "params": {**cancel_params, "signature": cancel_sig},
            }
            c_resp = ws_call(ws, cancel_payload)
            pretty("order.cancel response", c_resp)

        return ok, order_id
    finally:
        try:
            ws.close()
        except Exception:
            pass


def main() -> None:
    args = parse_args()

    if args.testnet and not os.environ.get("BINANCE_FAPI_WS_URL"):
        args.fapi_ws_url = DEFAULT_FAPI_WS_TESTNET_URL

    if args.only in {"both", "papi"} and not args.papi_ws_url:
        print(
            "ERROR: --papi-ws-url is required for --only papi/both (set BINANCE_PAPI_WS_URL).",
            file=sys.stderr,
        )
        sys.exit(1)

    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    if args.place_order:
        api_key, api_secret = load_credentials()

    any_failed = False

    if args.only in {"both", "fapi"}:
        ok, _ = run_endpoint(args.fapi_ws_url, "FAPI WS (UM Futures)", args, api_key, api_secret)
        any_failed = any_failed or (not ok)

    if args.only in {"both", "papi"}:
        ok, _ = run_endpoint(args.papi_ws_url, "PAPI WS (Portfolio Margin)", args, api_key, api_secret)
        any_failed = any_failed or (not ok)

    if any_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()

