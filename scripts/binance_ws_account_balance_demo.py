#!/usr/bin/env python3
"""Demo Binance USD-M Futures WS API account balance query.

This queries the USD-M futures WebSocket API method `v2/account.balance`.
It does not read Spot MAIN wallet balances and does not transfer funds.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
import uuid
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, Optional
from urllib.parse import urlencode

import websocket


DEFAULT_URL = "wss://ws-fapi.binance.com/ws-fapi/v1"
DEFAULT_METHOD = "v2/account.balance"


def decimal_or_none(value: Any) -> Optional[Decimal]:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def sign_params(params: Dict[str, Any], api_secret: str) -> str:
    payload = urlencode(sorted((k, str(v)) for k, v in params.items()))
    return hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


def build_request(api_key: str, api_secret: str, method: str, recv_window: int) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "apiKey": api_key,
        "timestamp": int(time.time() * 1000),
    }
    if recv_window > 0:
        params["recvWindow"] = recv_window
    params["signature"] = sign_params(params, api_secret)
    return {
        "id": str(uuid.uuid4()),
        "method": method,
        "params": params,
    }


def find_asset_row(result: Any, asset: str) -> Optional[Dict[str, Any]]:
    if not isinstance(result, list):
        return None
    want = asset.upper()
    for row in result:
        if isinstance(row, dict) and str(row.get("asset", "")).upper() == want:
            return row
    return None


def asset_names(result: Any) -> Iterable[str]:
    if not isinstance(result, list):
        return []
    return [
        str(row.get("asset", ""))
        for row in result
        if isinstance(row, dict) and row.get("asset")
    ]


def print_asset_summary(response: Dict[str, Any], asset: str, elapsed_ms: float) -> bool:
    status = response.get("status")
    result = response.get("result")
    rate_limits = response.get("rateLimits") or []
    row = find_asset_row(result, asset)
    if status != 200:
        print(
            f"[error] status={status} elapsed_ms={elapsed_ms:.1f} response={json.dumps(response, ensure_ascii=False)}",
            flush=True,
        )
        return False
    if row is None:
        print(
            f"[error] status=200 asset={asset} not found assets={list(asset_names(result))}",
            flush=True,
        )
        return False

    cross_wallet = decimal_or_none(row.get("crossWalletBalance"))
    cross_unpnl = decimal_or_none(row.get("crossUnPnl"))
    cross_equity = None
    if cross_wallet is not None and cross_unpnl is not None:
        cross_equity = cross_wallet + cross_unpnl

    weight = ""
    for limit in rate_limits:
        if isinstance(limit, dict) and limit.get("rateLimitType") == "REQUEST_WEIGHT":
            weight = (
                f" weight={limit.get('count')}/{limit.get('limit')}"
                f" {limit.get('intervalNum')}{limit.get('interval')}"
            )
            break

    print(
        " ".join(
            part
            for part in [
                f"[ok] status=200 elapsed_ms={elapsed_ms:.1f}{weight}",
                f"asset={row.get('asset')}",
                f"balance={row.get('balance')}",
                f"crossWalletBalance={row.get('crossWalletBalance')}",
                f"crossUnPnl={row.get('crossUnPnl')}",
                f"crossEquity={cross_equity}" if cross_equity is not None else "",
                f"availableBalance={row.get('availableBalance')}",
                f"maxWithdrawAmount={row.get('maxWithdrawAmount')}",
                f"marginAvailable={row.get('marginAvailable')}",
                f"updateTime={row.get('updateTime')}",
            ]
            if part
        ),
        flush=True,
    )
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read Binance USD-M Futures v2/account.balance over WebSocket API."
    )
    parser.add_argument("--url", default=os.environ.get("BINANCE_FAPI_WS_API_URL", DEFAULT_URL))
    parser.add_argument("--method", default=DEFAULT_METHOD)
    parser.add_argument("--asset", default="USDT")
    parser.add_argument("--interval-sec", type=float, default=20.0)
    parser.add_argument("--count", type=int, default=3)
    parser.add_argument("--recv-window", type=int, default=5000)
    parser.add_argument("--timeout-sec", type=float, default=10.0)
    parser.add_argument("--api-key-env", default="BINANCE_API_KEY")
    parser.add_argument("--api-secret-env", default="BINANCE_API_SECRET")
    parser.add_argument("--print-json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_key = os.environ.get(args.api_key_env, "").strip()
    api_secret = os.environ.get(args.api_secret_env, "").strip()
    if not api_key:
        print(f"[error] missing {args.api_key_env}", file=sys.stderr)
        return 2
    if not api_secret:
        print(f"[error] missing {args.api_secret_env}", file=sys.stderr)
        return 2

    ws = websocket.create_connection(args.url, timeout=args.timeout_sec)
    ok = True
    try:
        print(
            f"[info] connected url={args.url} method={args.method} asset={args.asset} interval_sec={args.interval_sec} count={args.count}",
            flush=True,
        )
        for idx in range(args.count):
            req = build_request(api_key, api_secret, args.method, args.recv_window)
            sent_at = time.monotonic()
            ws.send(json.dumps(req, separators=(",", ":")))
            raw = ws.recv()
            elapsed_ms = (time.monotonic() - sent_at) * 1000.0
            response = json.loads(raw)
            if args.print_json:
                print(json.dumps(response, ensure_ascii=False, sort_keys=True), flush=True)
            ok = print_asset_summary(response, args.asset, elapsed_ms) and ok
            if idx + 1 < args.count:
                time.sleep(max(0.0, args.interval_sec))
    finally:
        ws.close()
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
