#!/usr/bin/env python3
"""Cancel all Binance unified-account open orders by symbol.

Default behavior is dry-run:
  1) query current open orders
  2) infer the set of symbols with open orders
  3) print the cancel plan

Use --execute to actually call the cancel-all endpoint for each symbol.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from sell_margin_spot import request_papi

UM_OPEN_ORDERS_PATH = "/papi/v1/um/openOrders"
UM_CANCEL_ALL_PATH = "/papi/v1/um/allOpenOrders"
MARGIN_OPEN_ORDERS_PATH = "/papi/v1/margin/openOrders"
MARGIN_CANCEL_ALL_PATH = "/papi/v1/margin/allOpenOrders"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cancel all Binance unified-account open orders by symbol",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--base-url",
        default=(
            os.environ.get("BINANCE_PAPI_URL")
            or os.environ.get("BINANCE_FAPI_URL")
            or "https://papi.binance.com"
        ),
        help="Binance Portfolio Margin REST base URL",
    )
    parser.add_argument(
        "--scope",
        choices=["um", "margin", "both"],
        default="both",
        help="Which unified-account order scopes to cancel",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        default=[],
        help="Restrict to specific symbol(s); repeatable, e.g. --symbol BTCUSDT --symbol ETHUSDT",
    )
    parser.add_argument(
        "--isolated",
        action="store_true",
        help="Use isolated margin when querying/canceling margin orders",
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


def load_credentials() -> tuple[str, str]:
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: missing BINANCE_API_KEY / BINANCE_API_SECRET", file=sys.stderr)
        raise SystemExit(1)
    return api_key, api_secret


def normalize_symbols(raw_symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in raw_symbols:
        symbol = raw.strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def parse_orders(body: str) -> list[dict[str, Any]]:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return []
    return payload if isinstance(payload, list) else []


def header_value(headers: dict[str, str], key: str) -> str:
    return headers.get(key) or headers.get(key.lower()) or "-"


def query_open_orders(
    *,
    base_url: str,
    path: str,
    api_key: str,
    api_secret: str,
    timeout: int,
    recv_window: int,
    extra_params: dict[str, str] | None = None,
) -> tuple[int, str, dict[str, str], list[dict[str, Any]]]:
    params: dict[str, str] = {"recvWindow": str(recv_window)}
    if extra_params:
        params.update(extra_params)
    status, body, headers = request_papi(
        base_url,
        path,
        params,
        api_key,
        api_secret,
        method="GET",
        timeout=timeout,
    )
    return status, body, headers, parse_orders(body)


def discover_symbols_from_orders(orders: list[dict[str, Any]]) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    for order in orders:
        raw = str(order.get("symbol", "")).strip().upper()
        if not raw or raw in seen:
            continue
        seen.add(raw)
        symbols.append(raw)
    return symbols


def cancel_all_for_symbol(
    *,
    base_url: str,
    path: str,
    symbol: str,
    api_key: str,
    api_secret: str,
    timeout: int,
    recv_window: int,
    extra_params: dict[str, str] | None = None,
) -> tuple[int, str, dict[str, str]]:
    params: dict[str, str] = {
        "symbol": symbol,
        "recvWindow": str(recv_window),
    }
    if extra_params:
        params.update(extra_params)
    return request_papi(
        base_url,
        path,
        params,
        api_key,
        api_secret,
        method="DELETE",
        timeout=timeout,
    )


def print_scope_query_result(scope: str, status: int, headers: dict[str, str], orders: list[dict[str, Any]]) -> None:
    print(
        f"[{scope}] query status={status} open_orders={len(orders)} "
        f"used_weight_1m={header_value(headers, 'x-mbx-used-weight-1m')} "
        f"order_count_1m={header_value(headers, 'x-mbx-order-count-1m')}"
    )


def handle_scope(
    *,
    scope: str,
    query_path: str,
    cancel_path: str,
    base_url: str,
    api_key: str,
    api_secret: str,
    timeout: int,
    recv_window: int,
    requested_symbols: list[str],
    extra_params: dict[str, str] | None,
    execute: bool,
) -> int:
    symbols = list(requested_symbols)
    orders: list[dict[str, Any]] = []

    if not symbols:
        status, body, headers, orders = query_open_orders(
            base_url=base_url,
            path=query_path,
            api_key=api_key,
            api_secret=api_secret,
            timeout=timeout,
            recv_window=recv_window,
            extra_params=extra_params,
        )
        print_scope_query_result(scope, status, headers, orders)
        if status != 200:
            print(f"[{scope}] query failed; body={body}", file=sys.stderr)
            if scope == "margin" and not requested_symbols:
                print(
                    "[margin] hint: if Binance requires symbol for margin openOrders on your account,"
                    " rerun with explicit --symbol values or use --scope um.",
                    file=sys.stderr,
                )
            return 1
        symbols = discover_symbols_from_orders(orders)
    else:
        print(f"[{scope}] using explicit symbols: {', '.join(symbols)}")

    if not symbols:
        print(f"[{scope}] no open-order symbols found")
        return 0

    print(f"[{scope}] symbols_to_cancel={','.join(symbols)} execute={execute}")
    if not execute:
        return 0

    exit_code = 0
    for symbol in symbols:
        status, body, headers = cancel_all_for_symbol(
            base_url=base_url,
            path=cancel_path,
            symbol=symbol,
            api_key=api_key,
            api_secret=api_secret,
            timeout=timeout,
            recv_window=recv_window,
            extra_params=extra_params,
        )
        ok = 200 <= status < 300
        print(
            f"[{scope}] cancel symbol={symbol} status={status} "
            f"used_weight_1m={header_value(headers, 'x-mbx-used-weight-1m')} "
            f"order_count_1m={header_value(headers, 'x-mbx-order-count-1m')}"
        )
        if ok:
            try:
                payload = json.loads(body)
                print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            except json.JSONDecodeError:
                print(body)
        else:
            print(f"[{scope}] cancel failed for {symbol}: {body}", file=sys.stderr)
            exit_code = 1
    return exit_code


def main() -> None:
    args = parse_args()
    api_key, api_secret = load_credentials()
    base_url = args.base_url.rstrip("/")
    symbols = normalize_symbols(args.symbol)

    margin_params: dict[str, str] = {}
    if args.isolated:
        margin_params["isIsolated"] = "TRUE"

    exit_code = 0
    if args.scope in {"um", "both"}:
        exit_code |= handle_scope(
            scope="um",
            query_path=UM_OPEN_ORDERS_PATH,
            cancel_path=UM_CANCEL_ALL_PATH,
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            timeout=args.timeout,
            recv_window=args.recv_window,
            requested_symbols=symbols,
            extra_params=None,
            execute=args.execute,
        )

    if args.scope in {"margin", "both"}:
        exit_code |= handle_scope(
            scope="margin",
            query_path=MARGIN_OPEN_ORDERS_PATH,
            cancel_path=MARGIN_CANCEL_ALL_PATH,
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            timeout=args.timeout,
            recv_window=args.recv_window,
            requested_symbols=symbols,
            extra_params=margin_params or None,
            execute=args.execute,
        )

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
