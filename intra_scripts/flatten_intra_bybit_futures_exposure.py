#!/usr/bin/env python3
"""Flatten intra (Bybit intra-exchange spot/futures arb) net exposure via linear futures.

Reads pre_trade_exposure from the intra dashboard, then submits Bybit linear
market orders to zero net_qty on the futures leg.

Usage (from the intra deploy directory, env.sh already sourced by wrapper):
  python flatten_intra_bybit_futures_exposure.py --suffix trade
  python flatten_intra_bybit_futures_exposure.py --suffix trade --min-net-usdt 10 --execute
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import math
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "lib"))
from exchange_state import fetch_exchange_state  # noqa: E402

DASHBOARD_HOST = os.environ.get("DASHBOARD_HOST", "127.0.0.1")
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "4191"))

HOST = os.environ.get("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
RECV_WINDOW_MS = "5000"


# ---------------------------------------------------------------------------
# Bybit REST helpers
# ---------------------------------------------------------------------------

def load_credentials() -> Tuple[str, str]:
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


def sign(api_key: str, api_secret: str, timestamp_ms: str, payload: str) -> str:
    raw = f"{timestamp_ms}{api_key}{RECV_WINDOW_MS}{payload}"
    return hmac.new(api_secret.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()


def request(
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
    resp = requests.request(method.upper(), url, headers=headers, data=body)
    try:
        data = resp.json()
    except ValueError:
        raise RuntimeError(f"Bybit {method} {path} returned non-JSON: {resp.status_code} {resp.text}")
    if resp.status_code >= 300 or data.get("retCode") not in (0, "0", None):
        raise RuntimeError(f"Bybit {method} {path} failed: http={resp.status_code} body={data}")
    return data


# ---------------------------------------------------------------------------
# Bybit linear instrument specs (public, no auth)
# ---------------------------------------------------------------------------

@dataclass
class LinearSpec:
    symbol: str
    qty_step: float
    min_qty: float


def fetch_linear_specs() -> Dict[str, LinearSpec]:
    """Fetch all linear USDT instrument specs."""
    specs: Dict[str, LinearSpec] = {}
    cursor = ""
    while True:
        params = [("category", "linear"), ("settleCoin", "USDT"), ("limit", "1000")]
        if cursor:
            params.append(("cursor", cursor))
        query = "&".join(f"{k}={v}" for k, v in params)
        url = f"{HOST}/v5/market/instruments-info?{query}"
        resp = requests.get(url, timeout=15)
        data = resp.json()
        if data.get("retCode") not in (0, "0", None):
            raise RuntimeError(f"Bybit instruments-info failed: {data}")
        result = data.get("result") or {}
        batch = result.get("list") or []
        if not isinstance(batch, list):
            break
        for entry in batch:
            symbol = str(entry.get("symbol") or "").upper()
            if not symbol.endswith("USDT"):
                continue
            status = str(entry.get("status") or "").lower()
            if status != "trading":
                continue
            lot = entry.get("lotSizeFilter") or {}
            try:
                qty_step = float(str(lot.get("qtyStep") or "0"))
                min_qty = float(str(lot.get("minOrderQty") or "0"))
            except (TypeError, ValueError):
                continue
            if qty_step <= 0:
                continue
            specs[symbol] = LinearSpec(symbol=symbol, qty_step=qty_step, min_qty=min_qty)
        next_cursor = (result.get("nextPageCursor") or "").strip()
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    return specs


def align_qty(qty: float, spec: LinearSpec) -> float:
    if spec.qty_step > 0:
        qty = math.floor(qty / spec.qty_step) * spec.qty_step
    if qty < spec.min_qty:
        return 0.0
    return qty


def format_qty(qty: float, spec: LinearSpec) -> str:
    step_str = (f"{spec.qty_step:.12f}").rstrip("0").rstrip(".")
    if "." in step_str:
        decimals = len(step_str.split(".", 1)[1])
    else:
        decimals = 0
    text = f"{qty:.{decimals}f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


# ---------------------------------------------------------------------------
# Snapshot reader
# ---------------------------------------------------------------------------

@dataclass
class ExposureRow:
    asset: str
    symbol: str        # bybit linear, e.g. BTCUSDT
    net_qty: float
    net_usdt: float
    open_qty: float
    hedge_qty: float


def fetch_snapshot_legacy(exchange: str, suffix: str) -> List[ExposureRow]:
    """Fetch exposure rows from the dashboard pre_trade_exposure snapshot (legacy path)."""
    url = (
        f"http://{DASHBOARD_HOST}:{DASHBOARD_PORT}"
        f"/intra/{exchange}-intra-{suffix}/snapshot"
    )
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        print(f"ERROR: failed to fetch snapshot from {url}: {exc}", file=sys.stderr)
        sys.exit(1)

    rows: List[ExposureRow] = []
    for entry in data.get("entries", []):
        if entry.get("channel") != "pre_trade_exposure":
            continue
        for r in entry.get("entry", {}).get("rows", []):
            if r.get("is_total"):
                continue
            asset = r.get("asset", "")
            if not asset:
                continue
            rows.append(
                ExposureRow(
                    asset=asset.upper(),
                    symbol=f"{asset.upper()}USDT",
                    net_qty=float(r.get("net_qty", 0)),
                    net_usdt=float(r.get("net_usdt", 0)),
                    open_qty=float(r.get("open_qty", 0)),
                    hedge_qty=float(r.get("hedge_qty", 0)),
                )
            )
    return rows


def fetch_snapshot_exchange(exchange: str, suffix: str, timeout: int = 10) -> List[ExposureRow]:
    """Fetch exposure rows by querying Bybit REST APIs directly (lib path)."""
    canonical = fetch_exchange_state(exchange, suffix, timeout=timeout, verbose=True)
    rows: List[ExposureRow] = []
    for entry in canonical:
        net_qty = float(entry.exposure)
        mark = float(entry.mark_price)
        net_usdt = net_qty * mark if mark > 0 else 0.0
        rows.append(
            ExposureRow(
                asset=entry.asset,
                symbol=f"{entry.asset}USDT",
                net_qty=net_qty,
                net_usdt=net_usdt,
                open_qty=float(entry.balance),
                hedge_qty=float(entry.um_position),
            )
        )
    return rows


def fetch_exposure_rows(args: argparse.Namespace) -> List[ExposureRow]:
    exchange = (args.exchange or "bybit").strip().lower()
    if args.source == "dashboard":
        print(f"[info] using DASHBOARD snapshot (legacy) for {exchange}-intra-{args.suffix}")
        return fetch_snapshot_legacy(exchange, args.suffix)
    print(f"[info] using EXCHANGE state for {exchange}-intra-{args.suffix}")
    return fetch_snapshot_exchange(exchange, args.suffix, timeout=args.timeout)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Flatten intra (Bybit) net exposure via linear futures market orders",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--exchange",
        default="bybit",
        help="exchange tag in dashboard URL (default bybit)",
    )
    parser.add_argument(
        "--suffix",
        required=True,
        help="env suffix, e.g. 'trade' → reads from <exchange>-intra-trade",
    )
    parser.add_argument(
        "--min-net-usdt",
        type=float,
        default=5.0,
        dest="min_net_usdt",
        help="Only process rows with abs(net_usdt) above this threshold",
    )
    parser.add_argument(
        "--skip-assets",
        dest="skip_assets",
        default="",
        help="Comma-separated assets to skip",
    )
    parser.add_argument(
        "--reduce-only",
        dest="reduce_only",
        action="store_true",
        help="Set reduceOnly=true on close orders (default false; intra typically needs both directions)",
    )
    parser.add_argument("--execute", action="store_true", help="actually submit; default dry-run")
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument(
        "--source",
        choices=["exchange", "dashboard"],
        default="exchange",
        help="exposure data source (default: exchange = direct Bybit REST; dashboard = legacy panel)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    api_key, api_secret = load_credentials()

    skip = {s.strip().upper() for s in args.skip_assets.split(",") if s.strip()}
    exchange = (args.exchange or "bybit").strip().lower()

    print(f"[info] fetching exposure rows for exchange={exchange} suffix={args.suffix} ...")
    rows = fetch_exposure_rows(args)
    if not rows:
        print("No exposure rows found.")
        return

    print("[info] fetching Bybit linear instrument specs ...")
    specs = fetch_linear_specs()

    targets: List[Tuple[ExposureRow, LinearSpec, float, str]] = []
    print()
    print(f"{'Asset':<10} {'Net Qty':>14} {'Net USDT':>10} {'Side':>6} {'Order Qty':>12}")
    print("-" * 60)

    for row in rows:
        if row.asset in skip:
            print(f"  [skip] {row.asset} (in skip list)")
            continue
        if abs(row.net_usdt) < args.min_net_usdt:
            continue
        spec = specs.get(row.symbol)
        if spec is None:
            print(f"  [skip] {row.symbol} (not a trading linear instrument)")
            continue
        raw_qty = abs(row.net_qty)
        aligned = align_qty(raw_qty, spec)
        if aligned <= 0:
            print(
                f"{row.asset:<10} {row.net_qty:>14.6f} {row.net_usdt:>10.2f} "
                f"{'--':>6} {'< minQty':>12}"
            )
            continue
        side = "Sell" if row.net_qty > 0 else "Buy"
        qty_str = format_qty(aligned, spec)
        print(
            f"{row.asset:<10} {row.net_qty:>14.6f} {row.net_usdt:>10.2f} "
            f"{side:>6} {qty_str:>12}"
        )
        targets.append((row, spec, aligned, side))

    print("-" * 60)
    print(f"Total: {len(targets)} orders")
    print()

    if not targets:
        print("No valid orders to submit.")
        return

    if not args.execute:
        print("Dry-run mode. Add --execute to actually submit orders.")
        return

    print("=" * 60)
    print("EXECUTING ORDERS")
    print("=" * 60)
    failures = 0
    for idx, (row, spec, qty, side) in enumerate(targets, start=1):
        qty_str = format_qty(qty, spec)
        payload: Dict[str, Any] = {
            "category": "linear",
            "symbol": spec.symbol,
            "side": side,
            "orderType": "Market",
            "qty": qty_str,
            "reduceOnly": bool(args.reduce_only),
            "orderLinkId": f"intraflat{int(time.time() * 1000)}{idx:02d}",
        }
        print(f"\n[order] {spec.symbol} {side} qty={qty_str} reduceOnly={payload['reduceOnly']} ...")
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
        try:
            data = request(api_key, api_secret, "POST", "/v5/order/create", body=body)
            print(f"  [OK] {json.dumps({'symbol': spec.symbol, 'result': data.get('result')}, sort_keys=True)}")
        except Exception as exc:
            failures += 1
            print(f"  [ERR] {exc}", file=sys.stderr)

    print()
    if failures:
        print(f"WARN: {failures}/{len(targets)} orders failed", file=sys.stderr)
        sys.exit(1)
    print(f"All {len(targets)} orders submitted successfully.")


if __name__ == "__main__":
    main()
