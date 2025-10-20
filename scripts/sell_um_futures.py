#!/usr/bin/env python3
"""Submit a Binance Portfolio Margin UM futures order.

Example:
  BINANCE_API_KEY=... BINANCE_API_SECRET=... \\
  python scripts/sell_um_futures.py --symbol BTCUSDT --side SELL --type MARKET --quantity 0.001 \\
         --position-side SHORT --reduce-only
"""

import argparse
import json
import os
import sys

from sell_margin_spot import request_papi


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Submit a Binance Portfolio Margin UM futures order",
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
        help="Order side",
    )
    parser.add_argument(
        "--type",
        default="MARKET",
        choices=[
            "LIMIT",
            "MARKET",
            "STOP",
            "STOP_MARKET",
            "TAKE_PROFIT",
            "TAKE_PROFIT_MARKET",
            "TRAILING_STOP_MARKET",
        ],
        help="Order type",
    )
    parser.add_argument(
        "--quantity",
        required=True,
        help="Order quantity (base asset amount)",
    )
    parser.add_argument("--price", help="Price (required for LIMIT / STOP / TAKE_PROFIT)")
    parser.add_argument(
        "--time-in-force",
        dest="time_in_force",
        help="Time in force for price orders, e.g. GTC, IOC, FOK",
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
        "--close-position",
        dest="close_position",
        action="store_true",
        help="Send closePosition=true (for STOP/TAKE_PROFIT market orders)",
    )
    parser.add_argument(
        "--stop-price",
        dest="stop_price",
        help="Trigger price for STOP/TAKE_PROFIT orders",
    )
    parser.add_argument(
        "--activation-price",
        dest="activation_price",
        help="Activation price for trailing stop",
    )
    parser.add_argument(
        "--callback-rate",
        dest="callback_rate",
        help="Callback rate (0.1 - 5) for trailing stop",
    )
    parser.add_argument(
        "--working-type",
        dest="working_type",
        choices=["MARK_PRICE", "CONTRACT_PRICE", "LAST_PRICE"],
        help="Trigger price reference",
    )
    parser.add_argument(
        "--price-protect",
        dest="price_protect",
        action="store_true",
        help="Enable priceProtect=true",
    )
    parser.add_argument(
        "--client-order-id",
        dest="client_order_id",
        help="Optional newClientOrderId",
    )
    parser.add_argument(
        "--new-order-resp-type",
        dest="new_order_resp_type",
        choices=["ACK", "RESULT", "FULL"],
        help="newOrderRespType override",
    )
    parser.add_argument(
        "--recv-window",
        dest="recv_window",
        type=int,
        help="recvWindow in milliseconds",
    )
    return parser.parse_args()


def ensure_params(args: argparse.Namespace) -> dict:
    params = {
        "symbol": args.symbol.upper().strip(),
        "side": args.side.upper(),
        "type": args.type.upper(),
        "quantity": str(args.quantity).strip(),
    }

    if args.position_side:
        params["positionSide"] = args.position_side.upper()

    # Price-related validations
    price_required_types = {"LIMIT", "STOP", "TAKE_PROFIT"}
    if args.type.upper() in price_required_types and not args.price:
        print("ERROR: --price is required for LIMIT/STOP/TAKE_PROFIT orders.", file=sys.stderr)
        sys.exit(1)

    stop_required_types = {"STOP", "TAKE_PROFIT", "STOP_MARKET", "TAKE_PROFIT_MARKET"}
    if args.type.upper() in stop_required_types and not args.stop_price:
        print("ERROR: --stop-price is required for STOP/TAKE_PROFIT orders.", file=sys.stderr)
        sys.exit(1)

    if args.type.upper() == "TRAILING_STOP_MARKET" and not args.callback_rate:
        print("ERROR: --callback-rate is required for TRAILING_STOP_MARKET orders.", file=sys.stderr)
        sys.exit(1)

    if args.price:
        params["price"] = str(args.price).strip()
    if args.time_in_force:
        params["timeInForce"] = args.time_in_force.strip().upper()
    if args.stop_price:
        params["stopPrice"] = str(args.stop_price).strip()
    if args.activation_price:
        params["activationPrice"] = str(args.activation_price).strip()
    if args.callback_rate:
        params["callbackRate"] = str(args.callback_rate).strip()
    if args.working_type:
        params["workingType"] = args.working_type
    if args.client_order_id:
        params["newClientOrderId"] = args.client_order_id.strip()
    if args.new_order_resp_type:
        params["newOrderRespType"] = args.new_order_resp_type
    if args.recv_window is not None:
        params["recvWindow"] = str(args.recv_window)
    if args.reduce_only:
        params["reduceOnly"] = "true"
    if args.close_position:
        params["closePosition"] = "true"
    if args.price_protect:
        params["priceProtect"] = "true"

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
        f"Submitting UM order: symbol={params['symbol']} side={params['side']} type={params['type']}"
        f" quantity={params.get('quantity')} positionSide={params.get('positionSide')} via {base_url}"
    )

    status, body, headers = request_papi(
        base_url,
        "/papi/v1/um/order",
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
