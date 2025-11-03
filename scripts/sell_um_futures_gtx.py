#!/usr/bin/env python3
"""Submit a Binance Portfolio Margin UM futures LIMIT_MAKER (GTX) order.

Example:
  BINANCE_API_KEY=... BINANCE_API_SECRET=... \\
  python scripts/sell_um_futures_gtx.py --symbol BTCUSDT --quantity 0.001 --price 75000 \\
         --position-side SHORT
"""

import argparse
import os
import sys

from sell_margin_spot import request_papi


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Submit a Binance Portfolio Margin UM futures LIMIT_MAKER (GTX) order",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--base-url",
        default=(
            os.environ.get("BINANCE_PAPI_URL")
            or os.environ.get("BINANCE_FAPI_URL")
            or "https://papi.binance.com"
        ),
        help="Base REST endpoint",
    )
    parser.add_argument("--symbol", required=True, help="Contract symbol, e.g. BTCUSDT")
    parser.add_argument(
        "--side",
        default="SELL",
        choices=["BUY", "SELL"],
        help="Order side (maker opposite of margin direction)",
    )
    parser.add_argument(
        "--quantity",
        required=True,
        help="Order quantity (base asset amount)",
    )
    parser.add_argument(
        "--price",
        required=True,
        help="Maker limit price",
    )
    parser.add_argument(
        "--position-side",
        dest="position_side",
        choices=["BOTH", "LONG", "SHORT"],
        default="BOTH",
        help="Position side (hedge mode)",
    )
    parser.add_argument(
        "--reduce-only",
        action="store_true",
        help="Send reduceOnly=true",
    )
    parser.add_argument(
        "--client-order-id",
        dest="client_order_id",
        help="Optional newClientOrderId",
    )
    parser.add_argument(
        "--recv-window",
        dest="recv_window",
        type=int,
        help="recvWindow in milliseconds",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: please set BINANCE_API_KEY / BINANCE_API_SECRET in environment.", file=sys.stderr)
        sys.exit(1)

    params = {
        "symbol": args.symbol.upper().strip(),
        "side": args.side.upper(),
        "type": "LIMIT",
        "timeInForce": "GTX",
        "quantity": str(args.quantity).strip(),
        "price": str(args.price).strip(),
    }

    if args.position_side:
        params["positionSide"] = args.position_side.upper()
    if args.reduce_only:
        params["reduceOnly"] = "true"
    if args.client_order_id:
        params["newClientOrderId"] = args.client_order_id.strip()
    if args.recv_window is not None:
        params["recvWindow"] = str(args.recv_window)

    base_url = args.base_url.rstrip("/")
    print(
        f"Submitting UM GTX order: symbol={params['symbol']} side={params['side']} "
        f"type={params['type']} tif={params['timeInForce']} price={params['price']} "
        f"quantity={params['quantity']} positionSide={params.get('positionSide')} via {base_url}"
    )

    status, body, headers = request_papi(
        base_url=base_url,
        method="POST",
        path="/papi/v1/um/order",
        params=params,
        api_key=api_key,
        api_secret=api_secret,
    )

    print(f"Status: {status}")
    print("Headers:")
    for key, value in headers.items():
        print(f"  {key}: {value}")
    print("Body:")
    print(body)

    if status >= 400:
        print(
            "GTX request rejected. This is expected if the price would take liquidity.",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
