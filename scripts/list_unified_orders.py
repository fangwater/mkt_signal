#!/usr/bin/env python3
"""Query Binance unified account margin/UM open orders for a hard-coded symbol list.

Set --cancel to cancel any open orders found.
Requires BINANCE_API_KEY / BINANCE_API_SECRET in the environment.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Tuple

from sell_margin_spot import request_papi

SYMBOLS = [
    "ONEUSDT",
    "ICXUSDT",
    "SFPUSDT",
    "ZILUSDT",
    "DENTUSDT",
    "HOTUSDT",
    "ANKRUSDT",
    "HIGHUSDT",
    "RDNTUSDT",
    "CHRUSDT",
    "A2ZUSDT",
    "SAHARAUSDT",
    "NFPUSDT",
    "CUSDT",
    "ACEUSDT",
    "BEAMXUSDT",
    "SANTOSUSDT",
    "AIUSDT",
    "USTCUSDT",
    "SLPUSDT",
    "AXLUSDT",
    "VANRYUSDT",
    "XPLUSDT",
    "SAGAUSDT",
    "PUMPUSDT",
    "INITUSDT",
    "LINEAUSDT",
    "CATIUSDT",
    "JTOUSDT",
    "NEWTUSDT",
    "AIXBTUSDT",
    "AUCTIONUSDT",
]

MARGIN_OPEN_ORDERS_PATH = "/papi/v1/margin/openOrders"
UM_OPEN_ORDERS_PATH = "/papi/v1/um/openOrders"
MARGIN_CANCEL_PATH = "/papi/v1/margin/order"
UM_CANCEL_PATH = "/papi/v1/um/order"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query Binance unified account margin/UM open orders",
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
    parser.add_argument(
        "--recv-window",
        type=int,
        dest="recv_window",
        help="recvWindow in milliseconds",
    )
    parser.add_argument(
        "--isolated",
        action="store_true",
        help="Use isolated margin for margin open orders/cancel",
    )
    parser.add_argument(
        "--cancel",
        action="store_true",
        help="Cancel open orders after query",
    )
    return parser.parse_args()


def load_credentials() -> tuple[str, str]:
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: missing BINANCE_API_KEY / BINANCE_API_SECRET", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret


def normalize_symbols(symbols: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for raw in symbols:
        sym = raw.strip().upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(sym)
    return out


def fetch_open_orders(
    base_url: str,
    path: str,
    symbol: str,
    api_key: str,
    api_secret: str,
    recv_window: int | None,
    extra_params: Dict[str, Any] | None = None,
) -> Tuple[int, str, Dict[str, str], List[Dict[str, Any]]]:
    params: Dict[str, Any] = {"symbol": symbol}
    if recv_window is not None:
        params["recvWindow"] = str(recv_window)
    if extra_params:
        params.update(extra_params)

    status, body, headers = request_papi(
        base_url.rstrip("/"),
        path,
        params,
        api_key,
        api_secret,
        method="GET",
    )

    parsed: List[Dict[str, Any]] = []
    if status == 200:
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, list):
            parsed = payload
        else:
            print(
                f"Unexpected response for {path} {symbol}: {body}",
                file=sys.stderr,
            )
    else:
        print(
            f"Request failed for {path} {symbol}: status={status} body={body}",
            file=sys.stderr,
        )

    return status, body, headers, parsed


def cancel_order(
    base_url: str,
    path: str,
    symbol: str,
    order_id: str,
    api_key: str,
    api_secret: str,
    recv_window: int | None,
    extra_params: Dict[str, Any] | None = None,
) -> Tuple[int, str, Dict[str, str]]:
    params: Dict[str, Any] = {"symbol": symbol, "orderId": order_id}
    if recv_window is not None:
        params["recvWindow"] = str(recv_window)
    if extra_params:
        params.update(extra_params)

    return request_papi(
        base_url.rstrip("/"),
        path,
        params,
        api_key,
        api_secret,
        method="DELETE",
    )


def print_orders(kind: str, orders: List[Dict[str, Any]]) -> None:
    if not orders:
        return
    for order in orders:
        order_id = order.get("orderId")
        side = order.get("side", "")
        order_type = order.get("type", "")
        price = order.get("price", "")
        orig_qty = order.get("origQty", order.get("origQuantity", ""))
        exec_qty = order.get("executedQty", "")
        pos_side = order.get("positionSide", "")
        details = [
            f"id={order_id}",
            f"side={side}",
            f"type={order_type}",
            f"price={price}",
            f"origQty={orig_qty}",
            f"executedQty={exec_qty}",
        ]
        if pos_side:
            details.append(f"positionSide={pos_side}")
        print(f"  - [{kind}] " + " ".join(details))


def cancel_orders(
    kind: str,
    base_url: str,
    path: str,
    symbol: str,
    orders: List[Dict[str, Any]],
    api_key: str,
    api_secret: str,
    recv_window: int | None,
    extra_params: Dict[str, Any] | None = None,
) -> None:
    if not orders:
        return
    print(f"[{kind}] canceling {len(orders)} open orders...")
    for order in orders:
        order_id = order.get("orderId")
        if order_id is None:
            print(f"[{kind}] skip cancel: missing orderId for {symbol}")
            continue
        status, body, headers = cancel_order(
            base_url,
            path,
            symbol,
            str(order_id),
            api_key,
            api_secret,
            recv_window,
            extra_params=extra_params,
        )
        weight = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
        tag = "OK" if 200 <= status < 300 else "ERR"
        print(
            f"[{kind}] cancel orderId={order_id} {tag} {status}; used_weight={weight}"
        )
        if status >= 300:
            print(body)


def main() -> None:
    args = parse_args()
    api_key, api_secret = load_credentials()
    symbols = normalize_symbols(SYMBOLS)
    if not symbols:
        print("ERROR: no symbols configured in SYMBOLS list", file=sys.stderr)
        sys.exit(1)

    base_url = args.base_url.rstrip("/")
    margin_params: Dict[str, Any] = {}
    if args.isolated:
        margin_params["isIsolated"] = "TRUE"

    for symbol in symbols:
        print(f"\n=== {symbol} ===")

        m_status, _m_body, m_headers, margin_orders = fetch_open_orders(
            base_url,
            MARGIN_OPEN_ORDERS_PATH,
            symbol,
            api_key,
            api_secret,
            args.recv_window,
            extra_params=margin_params,
        )
        m_weight = m_headers.get("x-mbx-used-weight-1m") or m_headers.get("x-mbx-used-weight")
        m_tag = "OK" if 200 <= m_status < 300 else "ERR"
        print(
            f"[margin] {m_tag} {m_status}; used_weight={m_weight}; count={len(margin_orders)}"
        )
        print_orders("margin", margin_orders)

        u_status, _u_body, u_headers, um_orders = fetch_open_orders(
            base_url,
            UM_OPEN_ORDERS_PATH,
            symbol,
            api_key,
            api_secret,
            args.recv_window,
        )
        u_weight = u_headers.get("x-mbx-used-weight-1m") or u_headers.get("x-mbx-used-weight")
        u_tag = "OK" if 200 <= u_status < 300 else "ERR"
        print(
            f"[um] {u_tag} {u_status}; used_weight={u_weight}; count={len(um_orders)}"
        )
        print_orders("um", um_orders)

        if args.cancel:
            cancel_orders(
                "margin",
                base_url,
                MARGIN_CANCEL_PATH,
                symbol,
                margin_orders,
                api_key,
                api_secret,
                args.recv_window,
                extra_params=margin_params,
            )
            cancel_orders(
                "um",
                base_url,
                UM_CANCEL_PATH,
                symbol,
                um_orders,
                api_key,
                api_secret,
                args.recv_window,
            )


if __name__ == "__main__":
    main()
