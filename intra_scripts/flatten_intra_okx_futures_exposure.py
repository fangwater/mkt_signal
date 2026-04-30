#!/usr/bin/env python3
"""Flatten intra (OKX intra-exchange spot/futures arb) net exposure via SWAP.

Reads pre_trade_exposure from the intra dashboard, converts net_qty to OKX
SWAP contracts using the live ctVal, and submits market orders to zero the
exposure on the futures (SWAP) leg.

Usage (from the intra deploy directory, env.sh already sourced by wrapper):
  python flatten_intra_okx_futures_exposure.py --suffix trade
  python flatten_intra_okx_futures_exposure.py --suffix trade --min-net-usdt 10 --execute
"""

from __future__ import annotations

import argparse
import base64
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
from typing import Any, Dict, List, Optional, Tuple

DASHBOARD_HOST = os.environ.get("DASHBOARD_HOST", "127.0.0.1")
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "4191"))

DEFAULT_BASE_URL = os.environ.get("OKX_BASE_URL", "https://www.okx.com")


# ---------------------------------------------------------------------------
# OKX REST helpers
# ---------------------------------------------------------------------------

def utc_timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def json_body(data: Any) -> str:
    if data is None:
        return ""
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def sign(timestamp: str, method: str, request_path: str, body: str, secret: str) -> str:
    payload = f"{timestamp}{method.upper()}{request_path}{body}"
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def request_okx(
    base_url: str,
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    body: Any = None,
    timeout: int = 10,
    simulated: bool = False,
    private: bool = True,
) -> Tuple[int, str, Dict[str, str]]:
    method = method.upper()
    params = params or {}
    query = urllib.parse.urlencode(params) if params else ""
    request_path = f"{path}?{query}" if query else path

    body_str = "" if method == "GET" else json_body(body)
    url = f"{base_url.rstrip('/')}{request_path}"
    data = None if method == "GET" else body_str.encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")

    if private:
        timestamp = utc_timestamp()
        signature = sign(timestamp, method, request_path, body_str, api_secret)
        req.add_header("OK-ACCESS-KEY", api_key)
        req.add_header("OK-ACCESS-SIGN", signature)
        req.add_header("OK-ACCESS-TIMESTAMP", timestamp)
        req.add_header("OK-ACCESS-PASSPHRASE", passphrase)
    if simulated:
        req.add_header("x-simulated-trading", "1")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read().decode("utf-8", "replace"), dict(resp.headers.items())
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", "replace")
        headers = dict(getattr(exc, "headers", {}).items()) if getattr(exc, "headers", None) else {}
        return exc.code, body_text, headers
    except Exception as exc:
        return 0, str(exc), {}


def okx_response_ok(body: str) -> Tuple[bool, str]:
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return False, "non-JSON response body"
    code = str(parsed.get("code", "")).strip()
    msg = str(parsed.get("msg", "")).strip()
    if code == "0":
        return True, ""
    brief = f"code={code} msg={msg}".strip()
    data = parsed.get("data")
    if isinstance(data, list) and data:
        first = data[0] if isinstance(data[0], dict) else None
        if first:
            s_code = str(first.get("sCode", "")).strip()
            s_msg = str(first.get("sMsg", "")).strip()
            if s_code or s_msg:
                brief = f"{brief} sCode={s_code} sMsg={s_msg}".strip()
    return False, brief


# ---------------------------------------------------------------------------
# OKX SWAP instrument specs
# ---------------------------------------------------------------------------

@dataclass
class SwapSpec:
    inst_id: str          # e.g. BTC-USDT-SWAP
    ct_val: float         # base coin per contract
    lot_sz: float         # min increment (contracts), almost always 1
    min_sz: float         # minimum order size (contracts)


def fetch_swap_specs(base_url: str, timeout: int) -> Dict[str, SwapSpec]:
    """Fetch OKX SWAP instrument specs (USDT-margined, live state)."""
    url = f"{base_url.rstrip('/')}/api/v5/public/instruments?instType=SWAP"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", "replace")
    parsed = json.loads(body)
    if str(parsed.get("code", "")) != "0":
        raise SystemExit(f"OKX instruments fetch failed: {parsed.get('msg')}")
    specs: Dict[str, SwapSpec] = {}
    for entry in parsed.get("data") or []:
        if not isinstance(entry, dict):
            continue
        inst_id = str(entry.get("instId") or "").upper()
        if not inst_id.endswith("-USDT-SWAP"):
            continue
        if str(entry.get("state") or "").lower() != "live":
            continue
        try:
            ct_val = float(entry.get("ctVal") or 0)
            lot_sz = float(entry.get("lotSz") or 1)
            min_sz = float(entry.get("minSz") or 1)
        except (TypeError, ValueError):
            continue
        if ct_val <= 0:
            continue
        specs[inst_id] = SwapSpec(inst_id=inst_id, ct_val=ct_val, lot_sz=lot_sz, min_sz=min_sz)
    return specs


def asset_to_inst_id(asset: str) -> str:
    return f"{asset.upper()}-USDT-SWAP"


def align_contracts(raw_contracts: float, spec: SwapSpec) -> float:
    """Floor raw_contracts to lotSz multiple. Returns 0 if below minSz."""
    if spec.lot_sz > 0:
        n = math.floor(raw_contracts / spec.lot_sz) * spec.lot_sz
    else:
        n = math.floor(raw_contracts)
    if n < spec.min_sz:
        return 0.0
    return n


def format_sz(contracts: float, spec: SwapSpec) -> str:
    if spec.lot_sz >= 1 and abs(contracts - round(contracts)) < 1e-9:
        return str(int(round(contracts)))
    # OKX accepts string with arbitrary decimals; trim trailing zeros
    s = f"{contracts:.8f}".rstrip("0").rstrip(".")
    return s or "0"


# ---------------------------------------------------------------------------
# Snapshot reader
# ---------------------------------------------------------------------------

@dataclass
class ExposureRow:
    asset: str
    inst_id: str       # OKX swap inst_id
    net_qty: float     # base coin units
    net_usdt: float
    open_qty: float
    hedge_qty: float


def fetch_snapshot(exchange: str, suffix: str) -> List[ExposureRow]:
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
                    inst_id=asset_to_inst_id(asset),
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
        description="Flatten intra (OKX) net exposure via SWAP market orders",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--exchange",
        default="okex",
        help="exchange tag in dashboard URL (default okex)",
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
        "--td-mode",
        dest="td_mode",
        choices=["cross", "isolated"],
        default="cross",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="OKX REST base URL")
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--simulate", action="store_true", help="x-simulated-trading: 1")
    parser.add_argument("--execute", action="store_true", help="actually submit; default dry-run")
    return parser.parse_args()


def load_credentials() -> Tuple[str, str, str]:
    api_key = os.environ.get("OKX_API_KEY", "").strip()
    api_secret = os.environ.get("OKX_API_SECRET", "").strip()
    passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()
    missing = [
        name
        for name, value in [
            ("OKX_API_KEY", api_key),
            ("OKX_API_SECRET", api_secret),
            ("OKX_PASSPHRASE", passphrase),
        ]
        if not value
    ]
    if missing:
        print(f"ERROR: missing env vars: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret, passphrase


def main() -> None:
    args = parse_args()
    api_key, api_secret, passphrase = load_credentials()
    base_url = args.base_url.rstrip("/")

    skip = {s.strip().upper() for s in args.skip_assets.split(",") if s.strip()}
    exchange = (args.exchange or "okex").strip().lower()

    print(f"[info] fetching snapshot for exchange={exchange} suffix={args.suffix} ...")
    rows = fetch_snapshot(exchange, args.suffix)
    if not rows:
        print("No exposure rows found.")
        return

    print("[info] fetching OKX SWAP instrument specs ...")
    specs = fetch_swap_specs(base_url, args.timeout)

    targets: List[Tuple[ExposureRow, SwapSpec, float, str]] = []
    print()
    print(f"{'Asset':<10} {'Net Qty':>14} {'Net USDT':>10} {'Side':>6} {'Contracts':>12}")
    print("-" * 60)

    for row in rows:
        if row.asset in skip:
            print(f"  [skip] {row.asset} (in skip list)")
            continue
        if abs(row.net_usdt) < args.min_net_usdt:
            continue
        spec = specs.get(row.inst_id)
        if spec is None:
            print(f"  [skip] {row.inst_id} (no live SWAP instrument)")
            continue
        raw_contracts = abs(row.net_qty) / spec.ct_val
        contracts = align_contracts(raw_contracts, spec)
        if contracts <= 0:
            print(
                f"{row.asset:<10} {row.net_qty:>14.6f} {row.net_usdt:>10.2f} "
                f"{'--':>6} {'< minSz':>12}"
            )
            continue
        side = "sell" if row.net_qty > 0 else "buy"
        sz_str = format_sz(contracts, spec)
        print(
            f"{row.asset:<10} {row.net_qty:>14.6f} {row.net_usdt:>10.2f} "
            f"{side:>6} {sz_str:>12}"
        )
        targets.append((row, spec, contracts, side))

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
    for row, spec, contracts, side in targets:
        sz_str = format_sz(contracts, spec)
        body = {
            "instId": spec.inst_id,
            "tdMode": args.td_mode,
            "side": side,
            "ordType": "market",
            "sz": sz_str,
            "reduceOnly": False,
        }
        print(f"\n[order] {spec.inst_id} {side} sz={sz_str} (≈{contracts * spec.ct_val:.6f} {row.asset}) ...")
        status, resp_body, headers = request_okx(
            base_url,
            "POST",
            "/api/v5/trade/order",
            api_key,
            api_secret,
            passphrase,
            body=body,
            timeout=args.timeout,
            simulated=args.simulate,
        )
        ok, brief = okx_response_ok(resp_body)
        final_ok = (200 <= status < 300) and ok
        tag = "OK" if final_ok else "ERR"
        print(f"  [{tag}] status={status} reqId={headers.get('x-request-id')}")
        try:
            print(f"  {json.dumps(json.loads(resp_body), ensure_ascii=False)}")
        except json.JSONDecodeError:
            print(f"  {resp_body}")
        if not final_ok:
            failures += 1
            if brief:
                print(f"  rejected: {brief}", file=sys.stderr)

    print()
    if failures:
        print(f"WARN: {failures}/{len(targets)} orders failed", file=sys.stderr)
        sys.exit(1)
    print(f"All {len(targets)} orders submitted successfully.")


if __name__ == "__main__":
    main()
