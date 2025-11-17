#!/usr/bin/env python3
"""Submit a Binance PM margin spot order (cross margin by default).

Example:
  BINANCE_API_KEY=... BINANCE_API_SECRET=... \
  python scripts/sell_margin_spot.py --symbol TWTUSDT --quantity 37.98024 --side SELL --type MARKET
"""

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Submit a Binance Portfolio Margin spot order (cross margin)"
    )
    parser.add_argument(
        "--base-url",
        default=(
            os.environ.get("BINANCE_PAPI_URL")
            or os.environ.get("BINANCE_FAPI_URL")
            or "https://papi.binance.com"
        ),
        help="Base REST endpoint, default https://papi.binance.com",
    )
    parser.add_argument("--symbol", required=True, help="Trading pair, e.g. TWTUSDT")
    parser.add_argument(
        "--side",
        default="SELL",
        choices=["BUY", "SELL"],
        help="Order side, default SELL",
    )
    parser.add_argument(
        "--type",
        default="MARKET",
        choices=["MARKET", "LIMIT"],
        help="Order type, default MARKET",
    )
    parser.add_argument(
        "--quantity",
        help="Order quantity (base asset amount, e.g. 37.98024)",
    )
    parser.add_argument(
        "--quote-order-qty",
        dest="quote_order_qty",
        help="Quote order quantity (market orders only)",
    )
    parser.add_argument("--price", help="Price; required for LIMIT orders")
    parser.add_argument(
        "--time-in-force",
        dest="time_in_force",
        help="Time in force for LIMIT orders, e.g. GTC, IOC",
    )
    parser.add_argument(
        "--client-order-id",
        dest="client_order_id",
        help="Optional newClientOrderId",
    )
    parser.add_argument(
        "--isolated",
        action="store_true",
        help="Use isolated margin (default is cross)",
    )
    parser.add_argument(
        "--side-effect",
        dest="side_effect",
        choices=["AUTO_REPAY", "MARGIN_BUY", "NO_SIDE_EFFECT", "AUTO_BORROW_REPAY"],
        default="AUTO_BORROW_REPAY",
        help="Optional sideEffectType，默认 AUTO_BORROW_REPAY（自动借币+归还）",
    )
    parser.add_argument(
        "--recv-window",
        type=int,
        dest="recv_window",
        help="recvWindow in milliseconds (default 5000)",
    )
    return parser.parse_args()


def now_ms() -> int:
    return int(time.time() * 1000)


def sign(query: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def request_papi(
    base_url: str,
    path: str,
    params: dict,
    api_key: str,
    api_secret: str,
    method: str = "POST",
    timeout: int = 10,
):
    q = dict(params)
    q.setdefault("recvWindow", "5000")
    q["timestamp"] = str(now_ms())
    items = sorted(q.items(), key=lambda kv: kv[0])
    query = urllib.parse.urlencode(items, safe="-_.~")
    signature = sign(query, api_secret)
    url = f"{base_url}{path}?{query}&signature={signature}"
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
    except Exception as exc:  # pragma: no cover - network failure
        return 0, str(exc), {}


def ensure_params(args: argparse.Namespace) -> dict:
    params = {
        "symbol": args.symbol.upper().strip(),
        "side": args.side.upper(),
        "type": args.type.upper(),
    }

    if args.type.upper() == "LIMIT" and not args.price:
        print("ERROR: LIMIT order requires --price.", file=sys.stderr)
        sys.exit(1)

    if not args.quantity and not args.quote_order_qty:
        print("ERROR: require --quantity or --quote-order-qty.", file=sys.stderr)
        sys.exit(1)

    if args.quantity:
        params["quantity"] = str(args.quantity).strip()
    if args.quote_order_qty:
        params["quoteOrderQty"] = str(args.quote_order_qty).strip()
    if args.price:
        params["price"] = str(args.price).strip()
    if args.time_in_force:
        params["timeInForce"] = args.time_in_force.strip().upper()
    if args.client_order_id:
        params["newClientOrderId"] = args.client_order_id.strip()
    if args.isolated:
        params["isIsolated"] = "TRUE"
    if args.side_effect:
        params["sideEffectType"] = args.side_effect
    if args.recv_window is not None:
        params["recvWindow"] = str(args.recv_window)

    return params


def main() -> None:
    args = parse_args()

    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: please set BINANCE_API_KEY / BINANCE_API_SECRET in environment.", file=sys.stderr)
        sys.exit(1)

    params = ensure_params(args)

    base_url = args.base_url.rstrip("/")
    print(
        f"Submitting margin spot order: symbol={params['symbol']} side={params['side']} type={params['type']}"
        f" quantity={params.get('quantity')} quoteOrderQty={params.get('quoteOrderQty')} via {base_url}"
    )

    status, body, headers = request_papi(
        base_url,
        "/papi/v1/margin/order",
        params,
        api_key,
        api_secret,
        method="POST",
    )

    weight = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
    order_count = headers.get("x-mbx-order-count-1m") or headers.get("x-mbx-order-count")
    tag = "OK" if 200 <= status < 300 else "ERR"
    print(f"Result: {tag} {status}; used_weight={weight}; order_count={order_count}")

    try:
        parsed = json.loads(body)
        formatted = json.dumps(parsed, ensure_ascii=False, indent=2, sort_keys=True)
        print(formatted)
    except json.JSONDecodeError:
        print(body)

    if not (200 <= status < 300):
        sys.exit(1)


if __name__ == "__main__":
    main()
