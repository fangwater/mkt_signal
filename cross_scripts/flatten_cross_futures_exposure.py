#!/usr/bin/env python3
"""Flatten cross futures-spot arb net exposure by trading UM futures.

Reads the exposure snapshot from the dashboard, finds symbols with non-zero
net_qty, and places market orders on the futures leg to zero out the exposure.

Usage:
  # from the cross deploy directory (env.sh already sourced by wrapper script)
  python flatten_cross_futures_exposure.py --suffix trade01
  python flatten_cross_futures_exposure.py --suffix trade01 --min-net-usdt 10 --execute
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
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, List, Optional 

SKIP_ASSETS = {"BNB"} 

DASHBOARD_HOST = "127.0.0.1"
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "4191"))

FAPI_BASE = "https://fapi.binance.com"
PAPI_BASE = "https://papi.binance.com"

EXCHANGE_INFO_URL = f"{FAPI_BASE}/fapi/v1/exchangeInfo"


# ---------------------------------------------------------------------------
# HTTP helpers (reuse pattern from sell_margin_spot.py / flatten_binance_std_um.py)
# ---------------------------------------------------------------------------

def now_ms() -> int:
    return int(time.time() * 1000)


def sign(query: str, secret: str) -> str:
    return hmac.new(
        secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def signed_request(
    base_url: str,
    path: str,
    params: dict,
    api_key: str,
    api_secret: str,
    method: str = "POST",
    timeout: int = 10,
):
    q = dict(params)
    q.setdefault("recvWindow", "5000")
    q["timestamp"] = str(now_ms())
    items = sorted(q.items(), key=lambda kv: kv[0])
    query = urllib.parse.urlencode(items, safe="-_.~")
    signature = sign(query, api_secret)
    url = f"{base_url}{path}?{query}&signature={signature}"
    req = urllib.request.Request(
        url, method=method, headers={"X-MBX-APIKEY": api_key}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
            headers = dict(resp.headers.items())
            return status, body, headers
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        headers = (
            dict(exc.headers.items()) if getattr(exc, "headers", None) else {}
        )
        return exc.code, body, headers
    except Exception as exc:
        return 0, str(exc), {}


# ---------------------------------------------------------------------------
# Exchange info: stepSize / minQty
# ---------------------------------------------------------------------------

@dataclass
class SymbolSpec:
    symbol: str
    step_size: float
    min_qty: float
    quantity_precision: int


def fetch_exchange_info() -> Dict[str, SymbolSpec]:
    req = urllib.request.Request(EXCHANGE_INFO_URL)
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    specs: Dict[str, SymbolSpec] = {}
    for s in data.get("symbols", []):
        symbol = s.get("symbol", "")
        if not symbol.endswith("USDT"):
            continue
        step_size = 1.0
        min_qty = 0.0
        for f in s.get("filters", []):
            if f.get("filterType") == "LOT_SIZE":
                step_size = float(f.get("stepSize", "1"))
                min_qty = float(f.get("minQty", "0"))
                break
        precision = int(s.get("quantityPrecision", 0))
        specs[symbol] = SymbolSpec(
            symbol=symbol,
            step_size=step_size,
            min_qty=min_qty,
            quantity_precision=precision,
        )
    return specs


def align_qty(qty: float, spec: SymbolSpec) -> float:
    """Round qty down to stepSize and respect quantityPrecision."""
    if spec.step_size > 0:
        qty = math.floor(qty / spec.step_size) * spec.step_size
    qty = round(qty, spec.quantity_precision)
    return qty


def format_qty(qty: float, spec: SymbolSpec) -> str:
    return f"{qty:.{spec.quantity_precision}f}"


# ---------------------------------------------------------------------------
# Snapshot reader
# ---------------------------------------------------------------------------

@dataclass
class ExposureRow:
    asset: str
    symbol: str  # e.g. BTCUSDT
    net_qty: float
    net_usdt: float
    open_qty: float
    hedge_qty: float


def fetch_snapshot(suffix: str) -> List[ExposureRow]:
    """Read exposure from dashboard snapshot API."""
    url = (
        f"http://{DASHBOARD_HOST}:{DASHBOARD_PORT}"
        f"/cross/binance-binance-cross-{suffix}/snapshot"
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
                    asset=asset,
                    symbol=f"{asset}USDT",
                    net_qty=float(r.get("net_qty", 0)),
                    net_usdt=float(r.get("net_usdt", 0)),
                    open_qty=float(r.get("open_qty", 0)),
                    hedge_qty=float(r.get("hedge_qty", 0)),
                )
            )
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Flatten cross futures-spot arb net exposure (Binance UM futures)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--suffix",
        required=True,
        help="env suffix, e.g. 'trade01' → reads from binance-binance-cross-trade01",
    )
    parser.add_argument(
        "--min-net-usdt",
        type=float,
        default=5.0,
        dest="min_net_usdt",
        help="Only process symbols with abs(net_usdt) above this threshold",
    )
    parser.add_argument(
        "--skip-assets",
        dest="skip_assets",
        default="BNB",
        help="Comma-separated list of assets to skip (default: BNB)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually submit orders; default is dry-run",
    )
    return parser.parse_args()


def resolve_api(account_mode: str):
    """Return (base_url, order_path) based on BINANCE_ACCOUNT_MODE."""
    mode = (account_mode or "").strip().upper()
    if mode in ("STANDARD", "STD", ""):
        return FAPI_BASE, "/fapi/v1/order"
    else:
        # PM / UNIFIED
        return PAPI_BASE, "/papi/v1/um/order"


def main() -> None:
    args = parse_args()

    # --- credentials ---
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print(
            "ERROR: BINANCE_API_KEY / BINANCE_API_SECRET not set. "
            "Please source env.sh first.",
            file=sys.stderr,
        )
        sys.exit(1)

    account_mode = os.environ.get("BINANCE_ACCOUNT_MODE", "STANDARD")
    base_url, order_path = resolve_api(account_mode)

    skip = {s.strip().upper() for s in args.skip_assets.split(",") if s.strip()}

    # --- fetch data ---
    print(f"[info] account_mode={account_mode} base_url={base_url} order_path={order_path}")
    print(f"[info] fetching snapshot for suffix={args.suffix} ...")
    rows = fetch_snapshot(args.suffix)
    if not rows:
        print("No exposure rows found.")
        return

    print(f"[info] fetching exchangeInfo for step sizes ...")
    specs = fetch_exchange_info()

    # --- filter ---
    targets: List[ExposureRow] = []
    for row in rows:
        if row.asset in skip:
            print(f"  [skip] {row.asset} (in skip list)")
            continue
        if abs(row.net_usdt) < args.min_net_usdt:
            continue
        if row.symbol not in specs:
            print(f"  [skip] {row.symbol} (not found in exchangeInfo)")
            continue
        targets.append(row)

    targets.sort(key=lambda r: abs(r.net_usdt), reverse=True)

    if not targets:
        print("No symbols to flatten after filtering.")
        return

    # --- plan ---
    print()
    print(f"{'Asset':<10} {'Net Qty':>14} {'Net USDT':>10} {'Action':>6} {'Order Qty':>12}")
    print("-" * 56)

    orders = []
    for row in targets:
        spec = specs[row.symbol]
        # net_qty > 0 → we are long exposure → need to SELL futures to flatten
        # net_qty < 0 → we are short exposure → need to BUY futures to flatten
        raw_qty = abs(row.net_qty)
        aligned_qty = align_qty(raw_qty, spec)
        if aligned_qty < spec.min_qty:
            print(
                f"{row.asset:<10} {row.net_qty:>14.6f} {row.net_usdt:>10.2f} "
                f"{'--':>6} {'< minQty':>12}"
            )
            continue
        side = "SELL" if row.net_qty > 0 else "BUY"
        qty_str = format_qty(aligned_qty, spec)
        print(
            f"{row.asset:<10} {row.net_qty:>14.6f} {row.net_usdt:>10.2f} "
            f"{side:>6} {qty_str:>12}"
        )
        orders.append((row, side, qty_str, spec))

    print("-" * 56)
    print(f"Total: {len(orders)} orders")
    print()

    if not orders:
        print("No valid orders to submit.")
        return

    if not args.execute:
        print("Dry-run mode. Add --execute to actually submit orders.")
        return

    # --- execute ---
    print("=" * 56)
    print("EXECUTING ORDERS")
    print("=" * 56)
    failures = 0
    for row, side, qty_str, spec in orders:
        params: Dict[str, Any] = {
            "symbol": row.symbol,
            "side": side,
            "type": "MARKET",
            "quantity": qty_str,
            "reduceOnly": "false",
        }
        print(f"\n[order] {row.symbol} {side} {qty_str} ...")
        status, body, headers = signed_request(
            base_url, order_path, params, api_key, api_secret, method="POST"
        )
        weight = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
        tag = "OK" if 200 <= status < 300 else "ERR"
        print(f"  [{tag}] status={status} weight={weight}")
        try:
            parsed = json.loads(body)
            print(f"  {json.dumps(parsed, ensure_ascii=False)}")
        except json.JSONDecodeError:
            print(f"  {body}")
        if not (200 <= status < 300):
            failures += 1

    print()
    if failures:
        print(f"WARN: {failures}/{len(orders)} orders failed", file=sys.stderr)
        sys.exit(1)
    else:
        print(f"All {len(orders)} orders submitted successfully.")


if __name__ == "__main__":
    main()
