#!/usr/bin/env python3
"""Transfer an asset between Binance Spot MAIN and Portfolio Margin.

Dry-run is the default. The script submits at most one POST and never retries
an uncertain transfer result.
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
from decimal import Decimal, InvalidOperation
from typing import Any


TRANSFER_TYPES = ("MAIN_PORTFOLIO_MARGIN", "PORTFOLIO_MARGIN_MAIN")
PM_FIELDS = (
    "totalWalletBalance",
    "crossMarginAsset",
    "crossMarginFree",
    "crossMarginLocked",
    "crossMarginBorrowed",
    "crossMarginInterest",
    "umWalletBalance",
    "umUnrealizedPNL",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transfer between Binance Spot MAIN and Portfolio Margin"
    )
    parser.add_argument("--type", choices=TRANSFER_TYPES, required=True)
    parser.add_argument("--asset", default="USDT")
    amount_group = parser.add_mutually_exclusive_group(required=True)
    amount_group.add_argument("--amount", help="Exact positive transfer amount")
    amount_group.add_argument(
        "--all",
        action="store_true",
        help="Transfer the source wallet's current free balance",
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--recv-window", type=int, default=5000)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument(
        "--sapi-url",
        default=os.environ.get("BINANCE_SAPI_URL", "https://api.binance.com"),
    )
    parser.add_argument(
        "--papi-url",
        default=os.environ.get("BINANCE_PAPI_URL", "https://papi.binance.com"),
    )
    return parser.parse_args()


def signed_request(
    base_url: str,
    path: str,
    params: dict[str, str],
    api_key: str,
    api_secret: str,
    method: str,
    timeout: float,
) -> tuple[int, str]:
    query_params = dict(params)
    query_params["timestamp"] = str(int(time.time() * 1000))
    query = urllib.parse.urlencode(sorted(query_params.items()), safe="-_.~")
    signature = hmac.new(
        api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    url = f"{base_url.rstrip('/')}{path}?{query}&signature={signature}"
    request = urllib.request.Request(
        url, method=method, headers={"X-MBX-APIKEY": api_key}
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.getcode(), response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")
    except Exception as exc:
        raise RuntimeError(
            f"transport result unknown: {type(exc).__name__}: {exc}"
        ) from exc


def request_json(
    base_url: str,
    path: str,
    params: dict[str, str],
    api_key: str,
    api_secret: str,
    method: str,
    timeout: float,
) -> Any:
    status, body = signed_request(
        base_url, path, params, api_key, api_secret, method, timeout
    )
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{path} returned invalid JSON: {exc}") from exc
    if not 200 <= status < 300:
        raise RuntimeError(f"{path} failed: status={status} body={payload}")
    return payload


def find_asset(rows: list[dict[str, Any]], asset: str) -> dict[str, Any]:
    return next(
        (row for row in rows if str(row.get("asset", "")).upper() == asset), {}
    )


def query_balances(
    args: argparse.Namespace, api_key: str, api_secret: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    params = {"recvWindow": str(args.recv_window)}
    spot_payload = request_json(
        args.sapi_url,
        "/api/v3/account",
        params,
        api_key,
        api_secret,
        "GET",
        args.timeout,
    )
    pm_payload = request_json(
        args.papi_url,
        "/papi/v1/balance",
        params,
        api_key,
        api_secret,
        "GET",
        args.timeout,
    )
    if not isinstance(spot_payload, dict) or not isinstance(
        spot_payload.get("balances"), list
    ):
        raise RuntimeError("/api/v3/account response is missing balances")
    if not isinstance(pm_payload, list):
        raise RuntimeError("/papi/v1/balance response is not an array")
    return (
        find_asset(spot_payload["balances"], args.asset),
        find_asset(pm_payload, args.asset),
    )


def positive_decimal(raw: Any, label: str) -> Decimal:
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, ValueError) as exc:
        raise RuntimeError(f"invalid {label}: {raw}") from exc
    if not value.is_finite() or value <= 0:
        raise RuntimeError(f"{label} must be positive, got {raw}")
    return value


def source_free(
    transfer_type: str, spot_row: dict[str, Any], pm_row: dict[str, Any]
) -> Decimal:
    if transfer_type == "MAIN_PORTFOLIO_MARGIN":
        return positive_decimal(spot_row.get("free", "0"), "Spot free balance")
    return positive_decimal(pm_row.get("crossMarginFree", "0"), "PM free balance")


def print_balances(label: str, spot_row: dict[str, Any], pm_row: dict[str, Any]) -> None:
    print(
        f"{label} spot_free={spot_row.get('free', '0')} "
        f"spot_locked={spot_row.get('locked', '0')}"
    )
    selected_pm = {field: pm_row.get(field) for field in PM_FIELDS}
    print(f"{label} pm={json.dumps(selected_pm, sort_keys=True)}")


def main() -> int:
    args = parse_args()
    args.asset = args.asset.strip().upper()
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: BINANCE_API_KEY / BINANCE_API_SECRET not set", file=sys.stderr)
        return 2

    try:
        spot_before, pm_before = query_balances(args, api_key, api_secret)
        available = source_free(args.type, spot_before, pm_before)
        amount = available if args.all else positive_decimal(args.amount, "amount")
        if amount > available:
            raise RuntimeError(
                f"amount {amount} exceeds source free balance {available}"
            )

        print_balances("before", spot_before, pm_before)
        print(
            f"prepared type={args.type} asset={args.asset} "
            f"amount={format(amount, 'f')}"
        )
        if not args.execute:
            print("dry-run only; add --execute to submit exactly one transfer")
            return 0

        status, body = signed_request(
            args.sapi_url,
            "/sapi/v1/asset/transfer",
            {
                "type": args.type,
                "asset": args.asset,
                "amount": format(amount, "f"),
                "recvWindow": str(args.recv_window),
            },
            api_key,
            api_secret,
            "POST",
            args.timeout,
        )
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = body
        print(f"transfer status={status} response={json.dumps(payload, sort_keys=True)}")
        if not 200 <= status < 300:
            return 1

        spot_after, pm_after = query_balances(args, api_key, api_secret)
        print_balances("after", spot_after, pm_after)
        return 0
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        print(
            "Do not retry a transfer with an unknown transport result.",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
