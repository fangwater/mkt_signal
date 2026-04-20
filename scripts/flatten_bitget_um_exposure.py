#!/usr/bin/env python3
"""List or close all Bitget unified-account USDT futures positions."""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import re
import sys
import time
from typing import Any, Dict, List
from urllib.parse import urlencode

import requests

HOST = os.environ.get("BITGET_API_BASE", "https://api.bitget.com").rstrip("/")
CATEGORY = "USDT-FUTURES"


def load_credentials() -> tuple[str, str, str]:
    api_key = os.environ.get("BITGET_API_KEY", "").strip()
    api_secret = os.environ.get("BITGET_API_SECRET", "").strip()
    passphrase = os.environ.get("BITGET_API_PASSPHRASE", "").strip()
    missing = [
        name
        for name, value in (
            ("BITGET_API_KEY", api_key),
            ("BITGET_API_SECRET", api_secret),
            ("BITGET_API_PASSPHRASE", passphrase),
        )
        if not value
    ]
    if missing:
        print(f"Missing env vars: {', '.join(missing)}", file=sys.stderr)
        raise SystemExit(1)
    return api_key, api_secret, passphrase


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


def sign(api_secret: str, timestamp_ms: str, method: str, path_with_query: str, body: str) -> str:
    payload = f"{timestamp_ms}{method.upper()}{path_with_query}{body}"
    raw = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(raw).decode("utf-8")


def request(
    api_key: str,
    api_secret: str,
    passphrase: str,
    method: str,
    path: str,
    *,
    params: Dict[str, Any] | None = None,
    payload: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    query = urlencode([(k, v) for k, v in (params or {}).items() if v not in ("", None)])
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True) if payload else ""
    path_with_query = f"{path}?{query}" if query else path
    timestamp_ms = str(int(time.time() * 1000))
    signature = sign(api_secret, timestamp_ms, method, path_with_query, body)
    url = f"{HOST}{path_with_query}"
    headers = {
        "ACCESS-KEY": api_key,
        "ACCESS-SIGN": signature,
        "ACCESS-TIMESTAMP": timestamp_ms,
        "ACCESS-PASSPHRASE": passphrase,
        "locale": "zh-CN",
        "Content-Type": "application/json",
    }
    resp = requests.request(method.upper(), url, headers=headers, data=body)
    try:
        data = resp.json()
    except ValueError:
        raise RuntimeError(f"Bitget {method} {path} returned non-JSON: {resp.status_code} {resp.text}")
    if resp.status_code >= 300 or data.get("code") not in ("00000", "0", None):
        raise RuntimeError(f"Bitget {method} {path} failed: http={resp.status_code} body={data}")
    return data


def fetch_positions(api_key: str, api_secret: str, passphrase: str) -> List[Dict[str, Any]]:
    data = request(
        api_key,
        api_secret,
        passphrase,
        "GET",
        "/api/v3/position/current-position",
        params={"category": CATEGORY},
    )
    result = data.get("data") or {}
    items = result.get("list") or []
    return items if isinstance(items, list) else []


def parse_qty(value: Any) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return 0.0


def format_decimal(value: float) -> str:
    text = f"{value:.12f}".rstrip("0").rstrip(".")
    return text if text else "0"


def build_close_payload(position: Dict[str, Any], idx: int) -> Dict[str, Any]:
    qty = parse_qty(position.get("available"))
    if qty <= 0:
        qty = parse_qty(position.get("total"))
    if qty <= 0:
        raise ValueError("position qty must be positive")
    pos_side = str(position.get("posSide") or "").lower()
    hold_mode = str(position.get("holdMode") or "").lower()
    payload: Dict[str, Any] = {
        "category": CATEGORY,
        "symbol": str(position.get("symbol") or ""),
        "orderType": "market",
        "qty": format_decimal(qty),
        "clientOid": f"mmclose{int(time.time() * 1000)}{idx}",
        "side": "sell" if pos_side == "long" else "buy",
    }
    if hold_mode == "hedge_mode":
        payload["posSide"] = pos_side
    else:
        payload["reduceOnly"] = "YES"
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List/close all Bitget unified-account USDT futures positions.")
    parser.add_argument("--symbol", action="append", default=[], help="Filter by symbol, e.g. BTCUSDT.")
    parser.add_argument(
        "--symbols",
        action="append",
        default=[],
        help="Comma/space separated symbol list, e.g. BTCUSDT,ETHUSDT.",
    )
    parser.add_argument("--execute", action="store_true", help="Actually place close orders.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_key, api_secret, passphrase = load_credentials()
    filters = set(parse_symbol_filters(args))

    raw_positions = fetch_positions(api_key, api_secret, passphrase)
    positions = []
    for row in raw_positions:
        symbol = str(row.get("symbol") or "")
        if filters and symbol not in filters:
            continue
        pos_side = str(row.get("posSide") or "").lower()
        if pos_side not in {"long", "short"}:
            continue
        qty = parse_qty(row.get("available"))
        if qty <= 0:
            qty = parse_qty(row.get("total"))
        if qty <= 0:
            continue
        positions.append(row)

    print(f"[bitget] open usdt-futures positions: {len(positions)}")
    plans = []
    for idx, position in enumerate(positions, start=1):
        payload = build_close_payload(position, idx)
        plans.append(payload)
        print(json.dumps(payload, ensure_ascii=True, sort_keys=True))

    if not args.execute:
        print("Dry-run only. Re-run with --execute to place close orders.")
        return 0

    for payload in plans:
        result = request(
            api_key,
            api_secret,
            passphrase,
            "POST",
            "/api/v3/trade/place-order",
            payload=payload,
        )
        print(json.dumps({"symbol": payload["symbol"], "result": result.get("data")}, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
