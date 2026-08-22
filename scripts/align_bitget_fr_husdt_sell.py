#!/usr/bin/env python3
"""Sell the excess HUSDT margin balance needed to align Bitget FR exposure.

This is deliberately limited to HUSDT and the Bitget FR runtime directory.
It computes the current H balance available to trade plus the signed
USDT-FUTURES HUSDT position.  A positive result is excess H that can be sold
through cross margin to bring the combined base-coin exposure toward zero.

The default is read-only.  --execute submits one MARGIN market sell; it never
changes the futures position, cancels orders, or starts/stops services.

Run from the intended environment:
  cd ~/bitget_fr_arb02
  python3 ~/mkt_signal/scripts/align_bitget_fr_husdt_sell.py
  python3 ~/mkt_signal/scripts/align_bitget_fr_husdt_sell.py --execute
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal, InvalidOperation, ROUND_DOWN


BITGET_BASE = os.environ.get("BITGET_API_BASE", "https://api.bitget.com").rstrip("/")
ENV_DIR_PATTERN = re.compile(r"^bitget_fr_")
AUTHORITATIVE_KEYS = (
    "BITGET_API_KEY",
    "BITGET_API_SECRET",
    "BITGET_PASSPHRASE",
    "BITGET_API_PASSPHRASE",
)
SYMBOL = "HUSDT"
ASSET = "H"
ZERO = Decimal("0")


def decimal_or(value: object, default: str = "0") -> Decimal:
    if value in (None, ""):
        return Decimal(default)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal(default)


def format_decimal(value: Decimal) -> str:
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= ZERO:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def step_from_precision(value: object, default: str = "0.000001") -> Decimal:
    try:
        precision = int(str(value))
    except (TypeError, ValueError):
        return Decimal(default)
    return Decimal("1").scaleb(-precision) if precision >= 0 else Decimal(default)


def http_request(url: str, *, method: str = "GET", headers=None, data=None, timeout: int = 15):
    req = urllib.request.Request(url, data=data, method=method.upper())
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.getcode(), response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001
        return 0, str(exc)


def bitget_private(method: str, path: str, api_key: str, api_secret: str, passphrase: str, *, query=None, body=None):
    method = method.upper()
    query_text = urllib.parse.urlencode(sorted(query.items())) if query else ""
    request_path = f"{path}?{query_text}" if query_text else path
    body_text = "" if method == "GET" or body is None else json.dumps(body, separators=(",", ":"))
    timestamp = str(int(time.time() * 1000))
    payload = f"{timestamp}{method}{request_path}{body_text}"
    signature = base64.b64encode(
        hmac.new(api_secret.encode(), payload.encode(), hashlib.sha256).digest()
    ).decode()
    headers = {
        "ACCESS-KEY": api_key,
        "ACCESS-SIGN": signature,
        "ACCESS-TIMESTAMP": timestamp,
        "ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
        "locale": "en-US",
    }
    return http_request(
        f"{BITGET_BASE}{request_path}",
        method=method,
        headers=headers,
        data=None if method == "GET" else body_text.encode(),
    )


def require_bitget_fr_environment() -> str:
    environment = os.path.basename(os.path.normpath(os.getcwd()))
    if not ENV_DIR_PATTERN.match(environment):
        sys.exit(
            f"[ERROR] run only from a bitget_fr_* environment, got CWD={os.getcwd()!r}"
        )
    return environment


def source_local_env() -> None:
    env_path = os.path.join(os.getcwd(), "env.sh")
    if not os.path.isfile(env_path):
        sys.exit(f"[ERROR] missing {env_path}")
    proc = subprocess.run(
        ["bash", "-lc", f"set -a; source {env_path} >/dev/null 2>&1; env -0"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        sys.exit("[ERROR] unable to source local env.sh")
    for item in proc.stdout.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key_b, value_b = item.split(b"=", 1)
        key = key_b.decode("utf-8", errors="ignore")
        if key in AUTHORITATIVE_KEYS:
            os.environ[key] = value_b.decode("utf-8", errors="replace")


def credentials() -> tuple[str, str, str]:
    api_key = os.environ.get("BITGET_API_KEY", "").strip()
    api_secret = os.environ.get("BITGET_API_SECRET", "").strip()
    passphrase = (
        os.environ.get("BITGET_PASSPHRASE", "")
        or os.environ.get("BITGET_API_PASSPHRASE", "")
    ).strip()
    if not api_key or not api_secret or not passphrase:
        sys.exit("[ERROR] Bitget API credentials are missing from local env.sh")
    return api_key, api_secret, passphrase


def api_json(status: int, body: str, endpoint: str) -> dict:
    if not 200 <= status < 300:
        sys.exit(f"[ERROR] {endpoint} HTTP {status}: {body[:500]}")
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        sys.exit(f"[ERROR] {endpoint} returned non-JSON: {body[:500]}")
    if str(parsed.get("code", "")) not in ("0", "00000"):
        sys.exit(f"[ERROR] {endpoint}: code={parsed.get('code')} msg={parsed.get('msg', '')}")
    return parsed


def fetch_margin_spec() -> tuple[Decimal, Decimal, Decimal]:
    status, body = http_request(f"{BITGET_BASE}/api/v3/market/instruments?category=MARGIN")
    rows = api_json(status, body, "MARGIN instruments").get("data", []) or []
    for row in rows:
        if str(row.get("symbol", "")).upper() != SYMBOL:
            continue
        step = decimal_or(
            row.get("quantityStep")
            or row.get("quantityMultiplier")
            or row.get("sizeStep")
            or row.get("baseSizeStep"),
            str(step_from_precision(row.get("quantityPrecision"))),
        )
        min_qty = decimal_or(
            row.get("minOrderQuantity")
            or row.get("minOrderQty")
            or row.get("minTradeNum")
            or row.get("minQuantity")
        )
        min_amount = decimal_or(row.get("minOrderAmount"))
        if step <= ZERO:
            sys.exit("[ERROR] HUSDT MARGIN instrument has an invalid quantity step")
        return step, min_qty, min_amount
    sys.exit("[ERROR] HUSDT is not an enabled Bitget MARGIN instrument")


def fetch_mark() -> Decimal:
    status, body = http_request(f"{BITGET_BASE}/api/v2/spot/market/tickers")
    rows = api_json(status, body, "spot tickers").get("data", []) or []
    for row in rows:
        if str(row.get("symbol", "")).upper() == SYMBOL:
            return decimal_or(row.get("bidPr") or row.get("lastPr") or row.get("askPr"))
    sys.exit("[ERROR] HUSDT spot ticker was not returned")


def fetch_available_h(api_key: str, api_secret: str, passphrase: str) -> tuple[Decimal, Decimal, Decimal]:
    status, body = bitget_private("GET", "/api/v3/account/assets", api_key, api_secret, passphrase)
    data = api_json(status, body, "account assets").get("data", [])
    rows = data.get("assets", []) if isinstance(data, dict) else data
    for row in rows or []:
        if str(row.get("coin", "")).upper() != ASSET:
            continue
        available = decimal_or(row.get("available") or row.get("equity") or row.get("balance"))
        borrowed = decimal_or(row.get("borrow") or row.get("debts") or row.get("debt"))
        return available, borrowed, decimal_or(row.get("equity") or row.get("balance"))
    return ZERO, ZERO, ZERO


def fetch_futures_position(api_key: str, api_secret: str, passphrase: str) -> Decimal:
    status, body = bitget_private(
        "GET", "/api/v3/position/current-position", api_key, api_secret, passphrase,
        query={"category": "USDT-FUTURES"},
    )
    data = api_json(status, body, "current position").get("data", [])
    rows = data.get("positions") or data.get("list") or [] if isinstance(data, dict) else data
    for row in rows or []:
        if str(row.get("symbol", "")).upper() != SYMBOL:
            continue
        size = decimal_or(row.get("total") or row.get("size") or row.get("holdSize"))
        side = str(row.get("posSide") or row.get("holdSide") or row.get("side", "")).lower()
        return -abs(size) if side in ("short", "sell") else abs(size)
    return ZERO


def main() -> None:
    parser = argparse.ArgumentParser(description="Align Bitget FR HUSDT exposure by selling only excess H margin balance")
    parser.add_argument("--execute", action="store_true", help="submit the single calculated MARGIN market sell")
    args = parser.parse_args()

    environment = require_bitget_fr_environment()
    source_local_env()
    api_key, api_secret, passphrase = credentials()
    step, min_qty, min_amount = fetch_margin_spec()
    mark = fetch_mark()
    available, borrowed, equity = fetch_available_h(api_key, api_secret, passphrase)
    futures = fetch_futures_position(api_key, api_secret, passphrase)

    net_before = available + futures
    requested = max(net_before, ZERO)
    sell_qty = floor_to_step(min(requested, max(available, ZERO)), step)
    print(f"environment={environment} symbol={SYMBOL} action=MARGIN_market_sell_only")
    print(f"available_H={format_decimal(available)} equity_H={format_decimal(equity)} borrowed_H={format_decimal(borrowed)}")
    print(f"futures_H_signed={format_decimal(futures)} net_H_before={format_decimal(net_before)} mark={format_decimal(mark)}")
    print(f"step={format_decimal(step)} min_qty={format_decimal(min_qty)} min_notional={format_decimal(min_amount)}")

    if net_before <= ZERO:
        print("No sell: H exposure is already aligned or net short.")
        return
    if sell_qty <= ZERO or sell_qty < min_qty:
        sys.exit(f"[ERROR] calculated sell quantity {format_decimal(sell_qty)} is below minimum")
    notional = sell_qty * mark
    if min_amount > ZERO and notional < min_amount:
        sys.exit(f"[ERROR] calculated sell notional {format_decimal(notional)} is below minimum")

    print(f"planned_sell_H={format_decimal(sell_qty)} estimated_USDT={format_decimal(notional)}")
    if not args.execute:
        print("Dry-run only. Re-run with --execute to submit this one MARGIN sell order.")
        return

    payload = {
        "category": "MARGIN",
        "symbol": SYMBOL,
        "side": "sell",
        "orderType": "market",
        "qty": format_decimal(sell_qty),
        "clientOid": f"fr-align-h-sell-{int(time.time() * 1000)}",
    }
    status, body = bitget_private("POST", "/api/v3/trade/place-order", api_key, api_secret, passphrase, body=payload)
    response = api_json(status, body, "place HUSDT MARGIN sell")
    print(f"submitted HUSDT MARGIN sell qty={payload['qty']} order_id={response.get('data', {})}")


if __name__ == "__main__":
    main()
