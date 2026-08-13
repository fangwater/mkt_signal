#!/usr/bin/env python3
"""List or cancel all Bybit unified-account USDT futures open orders."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter

try:
    from binance_local_ip import resolve_local_address
except ModuleNotFoundError:  # Preserve old standalone deployments until the helper is synced.
    def resolve_local_address(
        *,
        explicit_local_address: Optional[str] = None,
        trade_engine_config: Optional[str] = None,
        env_dir: Optional[str] = None,
        cwd: Optional[str] = None,
    ) -> tuple[Optional[str], str]:
        del trade_engine_config, env_dir, cwd
        value = (explicit_local_address or "").strip()
        if value and value not in {"0.0.0.0", "::"}:
            return value, "cli --local-address"
        return None, "system-default (binance_local_ip.py unavailable)"

HOST = os.environ.get("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
RECV_WINDOW_MS = "5000"
REQUEST_TIMEOUT_SECONDS = 15


class SourceAddressAdapter(HTTPAdapter):
    def __init__(self, source_address: str, **kwargs):
        self._source_address = (source_address, 0)
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        pool_kwargs["source_address"] = self._source_address
        return super().init_poolmanager(connections, maxsize, block=block, **pool_kwargs)

    def proxy_manager_for(self, proxy, **proxy_kwargs):
        proxy_kwargs["source_address"] = self._source_address
        return super().proxy_manager_for(proxy, **proxy_kwargs)


def build_session(local_address: Optional[str]) -> requests.Session:
    session = requests.Session()
    if local_address:
        adapter = SourceAddressAdapter(local_address, max_retries=0)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
    return session


def load_credentials() -> tuple[str, str]:
    api_key = os.environ.get("BYBIT_API_KEY", "").strip()
    api_secret = os.environ.get("BYBIT_API_SECRET", "").strip()
    missing = [
        name
        for name, value in (
            ("BYBIT_API_KEY", api_key),
            ("BYBIT_API_SECRET", api_secret),
        )
        if not value
    ]
    if missing:
        print(f"Missing env vars: {', '.join(missing)}", file=sys.stderr)
        raise SystemExit(1)
    return api_key, api_secret


def normalize_symbol(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", value or "").upper()


def parse_symbol_filters(args: argparse.Namespace) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in args.symbol:
        symbol = normalize_symbol(raw)
        if symbol and symbol not in seen:
            out.append(symbol)
            seen.add(symbol)
    for chunk in args.symbols:
        for part in re.split(r"[\s,]+", chunk.strip()):
            symbol = normalize_symbol(part)
            if symbol and symbol not in seen:
                out.append(symbol)
                seen.add(symbol)
    return out


def sign(api_key: str, api_secret: str, timestamp_ms: str, payload: str) -> str:
    raw = f"{timestamp_ms}{api_key}{RECV_WINDOW_MS}{payload}"
    return hmac.new(api_secret.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()


def request(
    session: requests.Session,
    api_key: str,
    api_secret: str,
    method: str,
    path: str,
    *,
    query: str = "",
    body: str = "",
) -> Dict[str, Any]:
    timestamp_ms = str(int(time.time() * 1000))
    payload = body if method.upper() != "GET" else query
    signature = sign(api_key, api_secret, timestamp_ms, payload)
    url = f"{HOST}{path}"
    if query:
        url = f"{url}?{query}"
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": timestamp_ms,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW_MS,
        "Content-Type": "application/json",
    }
    resp = session.request(
        method.upper(),
        url,
        headers=headers,
        data=body,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    try:
        data = resp.json()
    except ValueError:
        raise RuntimeError(f"Bybit {method} {path} returned non-JSON: {resp.status_code} {resp.text}")
    if resp.status_code >= 300 or data.get("retCode") not in (0, "0", None):
        raise RuntimeError(f"Bybit {method} {path} failed: http={resp.status_code} body={data}")
    return data


def fetch_open_orders(
    session: requests.Session,
    api_key: str,
    api_secret: str,
    symbol: Optional[str],
) -> List[Dict[str, Any]]:
    cursor = ""
    orders: List[Dict[str, Any]] = []
    while True:
        params = [("category", "linear"), ("limit", "50")]
        if symbol:
            params.append(("symbol", symbol))
        else:
            params.append(("settleCoin", "USDT"))
        if cursor:
            params.append(("cursor", cursor))
        query = "&".join(f"{k}={v}" for k, v in params)
        data = request(session, api_key, api_secret, "GET", "/v5/order/realtime", query=query)
        result = data.get("result") or {}
        batch = result.get("list") or []
        if not isinstance(batch, list):
            break
        orders.extend(batch)
        next_cursor = (result.get("nextPageCursor") or "").strip()
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    return orders


def cancel_all(
    session: requests.Session,
    api_key: str,
    api_secret: str,
    symbol: Optional[str],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"category": "linear"}
    if symbol:
        payload["symbol"] = symbol
    else:
        payload["settleCoin"] = "USDT"
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
    return request(session, api_key, api_secret, "POST", "/v5/order/cancel-all", body=body)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List/cancel all Bybit unified-account USDT futures open orders.")
    parser.add_argument("--symbol", action="append", default=[], help="Filter by symbol, e.g. BTCUSDT.")
    parser.add_argument(
        "--symbols",
        action="append",
        default=[],
        help="Comma/space separated symbol list, e.g. BTCUSDT,ETHUSDT.",
    )
    parser.add_argument(
        "--env-dir",
        default="",
        help="Environment directory containing trade_engine.toml for source-IP selection.",
    )
    parser.add_argument(
        "--trade-engine-config",
        default="",
        help="Explicit trade_engine.toml path for source-IP selection.",
    )
    parser.add_argument(
        "--local-address",
        default="",
        help="Explicit local source IP; overrides config discovery.",
    )
    parser.add_argument(
        "--require-local-address",
        action="store_true",
        help="Fail before any API request unless a local source IP is resolved.",
    )
    parser.add_argument("--execute", action="store_true", help="Actually cancel orders.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_key, api_secret = load_credentials()
    symbols = parse_symbol_filters(args)
    local_address, local_address_source = resolve_local_address(
        explicit_local_address=args.local_address,
        trade_engine_config=args.trade_engine_config,
        env_dir=args.env_dir,
        cwd=os.getcwd(),
    )
    if args.require_local_address and not local_address:
        print(
            f"[ERROR] no local source IP resolved: {local_address_source}",
            file=sys.stderr,
        )
        return 2
    print(
        f"[bybit] local source address: {local_address or '<system-default>'} "
        f"({local_address_source})"
    )
    session = build_session(local_address)

    if symbols:
        all_orders: List[Dict[str, Any]] = []
        for symbol in symbols:
            all_orders.extend(fetch_open_orders(session, api_key, api_secret, symbol))
    else:
        all_orders = fetch_open_orders(session, api_key, api_secret, None)

    print(f"[bybit] open linear orders: {len(all_orders)}")
    for order in all_orders:
        print(
            json.dumps(
                {
                    "symbol": order.get("symbol"),
                    "orderId": order.get("orderId"),
                    "orderLinkId": order.get("orderLinkId"),
                    "side": order.get("side"),
                    "orderType": order.get("orderType"),
                    "price": order.get("price"),
                    "qty": order.get("qty"),
                },
                ensure_ascii=True,
                sort_keys=True,
            )
        )

    if not args.execute:
        print("Dry-run only. Re-run with --execute to cancel.")
        return 0

    if symbols:
        for symbol in symbols:
            result = cancel_all(session, api_key, api_secret, symbol)
            print(
                json.dumps(
                    {"symbol": symbol, "result": result.get("result")},
                    ensure_ascii=True,
                    sort_keys=True,
                )
            )
    else:
        result = cancel_all(session, api_key, api_secret, None)
        print(json.dumps({"scope": "USDT", "result": result.get("result")}, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
