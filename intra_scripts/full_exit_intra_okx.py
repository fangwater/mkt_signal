#!/usr/bin/env python3
"""Full exit for intra (OKX intra-exchange spot/futures arb).

Cancels all open orders, then closes BOTH the margin (spot) leg and the SWAP
(futures) leg to zero, using the dashboard pre_trade_exposure snapshot as the
source of truth for current positions.

Reads:
  - GET http://<DASHBOARD_HOST>:<DASHBOARD_PORT>/intra/okex-intra-<suffix>/snapshot
  - GET <OKX_BASE_URL>/api/v5/public/instruments?instType=SPOT
  - GET <OKX_BASE_URL>/api/v5/public/instruments?instType=SWAP

Writes (only with --execute):
  - subprocess: scripts/okx_cancel_all_margin_orders.py --real --execute
  - subprocess: scripts/okx_swap_open_orders.py --real --cancel
  - POST /api/v5/trade/order  (margin close: side opposite sign(open_qty))
  - POST /api/v5/trade/order  (swap close:   side opposite sign(hedge_qty))

Usage (under env.sh sourced):
  python intra_scripts/full_exit_intra_okx.py --suffix arb01           # dry-run
  python intra_scripts/full_exit_intra_okx.py --suffix arb01 --execute # real
  python intra_scripts/full_exit_intra_okx.py --suffix arb01 --symbol BTC --execute
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import math
import os
import subprocess
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

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(REPO_ROOT, "scripts")


# ---------------------------------------------------------------------------
# OKX REST helpers (mirrors flatten_intra_okx_futures_exposure.py)
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
# Instrument specs
# ---------------------------------------------------------------------------

@dataclass
class SwapSpec:
    inst_id: str          # BTC-USDT-SWAP
    ct_val: float         # base coin per contract
    lot_sz: float
    min_sz: float


@dataclass
class SpotSpec:
    inst_id: str          # BTC-USDT
    lot_sz: float         # base coin increment
    min_sz: float         # base coin minimum
    tick_sz: float


def fetch_swap_specs(base_url: str, timeout: int) -> Dict[str, SwapSpec]:
    url = f"{base_url.rstrip('/')}/api/v5/public/instruments?instType=SWAP"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", "replace")
    parsed = json.loads(body)
    if str(parsed.get("code", "")) != "0":
        raise SystemExit(f"OKX SWAP instruments fetch failed: {parsed.get('msg')}")
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


def fetch_spot_specs(base_url: str, timeout: int) -> Dict[str, SpotSpec]:
    url = f"{base_url.rstrip('/')}/api/v5/public/instruments?instType=SPOT"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", "replace")
    parsed = json.loads(body)
    if str(parsed.get("code", "")) != "0":
        raise SystemExit(f"OKX SPOT instruments fetch failed: {parsed.get('msg')}")
    specs: Dict[str, SpotSpec] = {}
    for entry in parsed.get("data") or []:
        if not isinstance(entry, dict):
            continue
        inst_id = str(entry.get("instId") or "").upper()
        if not inst_id.endswith("-USDT"):
            continue
        if str(entry.get("state") or "").lower() != "live":
            continue
        try:
            lot_sz = float(entry.get("lotSz") or 0)
            min_sz = float(entry.get("minSz") or 0)
            tick_sz = float(entry.get("tickSz") or 0)
        except (TypeError, ValueError):
            continue
        specs[inst_id] = SpotSpec(inst_id=inst_id, lot_sz=lot_sz, min_sz=min_sz, tick_sz=tick_sz)
    return specs


def asset_to_swap_id(asset: str) -> str:
    return f"{asset.upper()}-USDT-SWAP"


def asset_to_spot_id(asset: str) -> str:
    return f"{asset.upper()}-USDT"


def align_floor(qty: float, lot: float, minimum: float) -> float:
    if lot > 0:
        n = math.floor(qty / lot) * lot
    else:
        n = qty
    if n < minimum:
        return 0.0
    return n


def format_size(qty: float, lot: float) -> str:
    if lot >= 1 and abs(qty - round(qty)) < 1e-9:
        return str(int(round(qty)))
    s = f"{qty:.10f}".rstrip("0").rstrip(".")
    return s or "0"


# ---------------------------------------------------------------------------
# Snapshot reader
# ---------------------------------------------------------------------------

@dataclass
class ExposureRow:
    asset: str
    open_qty: float
    hedge_qty: float
    net_qty: float
    net_usdt: float


def fetch_snapshot(suffix: str) -> List[ExposureRow]:
    url = (
        f"http://{DASHBOARD_HOST}:{DASHBOARD_PORT}"
        f"/intra/okex-intra-{suffix}/snapshot"
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
            asset = (r.get("asset") or "").upper()
            if not asset:
                continue
            rows.append(
                ExposureRow(
                    asset=asset,
                    open_qty=float(r.get("open_qty") or 0),
                    hedge_qty=float(r.get("hedge_qty") or 0),
                    net_qty=float(r.get("net_qty") or 0),
                    net_usdt=float(r.get("net_usdt") or 0),
                )
            )
    return rows


# ---------------------------------------------------------------------------
# Cancel step (subprocess into existing helpers)
# ---------------------------------------------------------------------------

def cancel_all_orders(python_bin: str, simulated: bool, timeout: int) -> None:
    """Cancel all margin and swap open orders. Print and continue on failure."""
    real_flag: List[str] = [] if simulated else ["--real"]
    margin_cmd = [
        python_bin,
        os.path.join(SCRIPTS_DIR, "okx_cancel_all_margin_orders.py"),
        *real_flag,
        "--execute",
        "--timeout",
        str(timeout),
    ]
    swap_cmd = [
        python_bin,
        os.path.join(SCRIPTS_DIR, "okx_swap_open_orders.py"),
        *real_flag,
        "--cancel",
        "--timeout",
        str(timeout),
    ]
    print("[cancel] === OKX MARGIN open orders ===")
    rc = subprocess.call(margin_cmd)
    if rc != 0:
        print(f"[cancel] WARN okx_cancel_all_margin_orders.py exited {rc}", file=sys.stderr)
    print("\n[cancel] === OKX SWAP open orders ===")
    rc = subprocess.call(swap_cmd)
    if rc != 0:
        print(f"[cancel] WARN okx_swap_open_orders.py exited {rc}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Plan + execute
# ---------------------------------------------------------------------------

@dataclass
class MarginAction:
    inst_id: str
    side: str         # buy | sell
    sz: float         # base coin
    sz_str: str
    tgt_ccy: Optional[str]
    raw_qty: float    # unaligned |open_qty|


@dataclass
class SwapAction:
    inst_id: str
    side: str         # buy | sell
    contracts: float
    sz_str: str
    base_qty: float   # contracts * ct_val (informational)
    raw_base_qty: float


def build_margin_action(open_qty: float, spec: SpotSpec) -> Tuple[Optional[MarginAction], str]:
    if abs(open_qty) <= 0:
        return None, "open_qty=0"
    side = "sell" if open_qty > 0 else "buy"
    raw = abs(open_qty)
    aligned = align_floor(raw, spec.lot_sz, spec.min_sz)
    if aligned <= 0:
        return None, f"|open_qty|={raw} below minSz={spec.min_sz}"
    sz_str = format_size(aligned, spec.lot_sz)
    tgt_ccy = "base_ccy" if side == "buy" else None
    return MarginAction(
        inst_id=spec.inst_id,
        side=side,
        sz=aligned,
        sz_str=sz_str,
        tgt_ccy=tgt_ccy,
        raw_qty=raw,
    ), ""


def build_swap_action(hedge_qty: float, spec: SwapSpec) -> Tuple[Optional[SwapAction], str]:
    if abs(hedge_qty) <= 0:
        return None, "hedge_qty=0"
    side = "sell" if hedge_qty > 0 else "buy"
    raw_base = abs(hedge_qty)
    raw_contracts = raw_base / spec.ct_val
    aligned = align_floor(raw_contracts, spec.lot_sz, spec.min_sz)
    if aligned <= 0:
        return None, f"|hedge_qty|={raw_base} → {raw_contracts:.6f} contracts below minSz={spec.min_sz}"
    sz_str = format_size(aligned, spec.lot_sz)
    return SwapAction(
        inst_id=spec.inst_id,
        side=side,
        contracts=aligned,
        sz_str=sz_str,
        base_qty=aligned * spec.ct_val,
        raw_base_qty=raw_base,
    ), ""


def build_margin_delta_action(delta_qty: float, spec: SpotSpec) -> Tuple[Optional[MarginAction], str]:
    """Adjust margin open_qty by delta_qty instead of closing it to zero."""
    if abs(delta_qty) <= 0:
        return None, "open_delta=0"
    side = "buy" if delta_qty > 0 else "sell"
    raw = abs(delta_qty)
    aligned = align_floor(raw, spec.lot_sz, spec.min_sz)
    if aligned <= 0:
        return None, f"|open_delta|={raw} below minSz={spec.min_sz}"
    sz_str = format_size(aligned, spec.lot_sz)
    tgt_ccy = "base_ccy" if side == "buy" else None
    return MarginAction(
        inst_id=spec.inst_id,
        side=side,
        sz=aligned,
        sz_str=sz_str,
        tgt_ccy=tgt_ccy,
        raw_qty=raw,
    ), ""


def build_swap_delta_action(delta_qty: float, spec: SwapSpec) -> Tuple[Optional[SwapAction], str]:
    """Adjust SWAP hedge_qty by delta base qty instead of closing it to zero."""
    if abs(delta_qty) <= 0:
        return None, "hedge_delta=0"
    side = "buy" if delta_qty > 0 else "sell"
    raw_base = abs(delta_qty)
    raw_contracts = raw_base / spec.ct_val
    aligned = align_floor(raw_contracts, spec.lot_sz, spec.min_sz)
    if aligned <= 0:
        return None, f"|hedge_delta|={raw_base} → {raw_contracts:.6f} contracts below minSz={spec.min_sz}"
    sz_str = format_size(aligned, spec.lot_sz)
    return SwapAction(
        inst_id=spec.inst_id,
        side=side,
        contracts=aligned,
        sz_str=sz_str,
        base_qty=aligned * spec.ct_val,
        raw_base_qty=raw_base,
    ), ""


def build_align_actions(
    row: ExposureRow,
    spot_spec: Optional[SpotSpec],
    swap_spec: Optional[SwapSpec],
) -> Tuple[Optional[MarginAction], str, Optional[SwapAction], str]:
    """Clear only net exposure by reducing the larger leg down to the smaller leg."""
    open_qty = row.open_qty
    hedge_qty = row.hedge_qty
    eps = 1e-12

    margin_action: Optional[MarginAction] = None
    margin_note = ""
    swap_action: Optional[SwapAction] = None
    swap_note = ""

    if abs(open_qty) <= eps and abs(hedge_qty) <= eps:
        return None, "open_qty=0", None, "hedge_qty=0"

    if abs(open_qty) <= eps:
        margin_note = "open_qty=0"
        if swap_spec is None:
            swap_note = f"no live SWAP instrument {asset_to_swap_id(row.asset)}"
        else:
            swap_action, swap_note = build_swap_action(hedge_qty, swap_spec)
        return margin_action, margin_note, swap_action, swap_note

    if abs(hedge_qty) <= eps:
        swap_note = "hedge_qty=0"
        if spot_spec is None:
            margin_note = f"no live SPOT instrument {asset_to_spot_id(row.asset)}"
        else:
            margin_action, margin_note = build_margin_action(open_qty, spot_spec)
        return margin_action, margin_note, swap_action, swap_note

    if open_qty * hedge_qty < 0:
        if abs(open_qty) > abs(hedge_qty):
            target_open = -hedge_qty
            delta_open = target_open - open_qty
            if spot_spec is None:
                margin_note = f"no live SPOT instrument {asset_to_spot_id(row.asset)}"
            else:
                margin_action, margin_note = build_margin_delta_action(delta_open, spot_spec)
            swap_note = "short-leg target"
        elif abs(hedge_qty) > abs(open_qty):
            target_hedge = -open_qty
            delta_hedge = target_hedge - hedge_qty
            margin_note = "short-leg target"
            if swap_spec is None:
                swap_note = f"no live SWAP instrument {asset_to_swap_id(row.asset)}"
            else:
                swap_action, swap_note = build_swap_delta_action(delta_hedge, swap_spec)
        else:
            margin_note = "already aligned"
            swap_note = "already aligned"
        return margin_action, margin_note, swap_action, swap_note

    # Same sign means there is no offsetting hedge. Close both legs toward zero.
    if spot_spec is None:
        margin_note = f"no live SPOT instrument {asset_to_spot_id(row.asset)}"
    else:
        margin_action, margin_note = build_margin_action(open_qty, spot_spec)
    if swap_spec is None:
        swap_note = f"no live SWAP instrument {asset_to_swap_id(row.asset)}"
    else:
        swap_action, swap_note = build_swap_action(hedge_qty, swap_spec)
    return margin_action, margin_note, swap_action, swap_note


def submit_margin_order(
    base_url: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    action: MarginAction,
    td_mode: str,
    timeout: int,
    simulated: bool,
) -> bool:
    body: Dict[str, Any] = {
        "instId": action.inst_id,
        "tdMode": td_mode,
        "side": action.side,
        "ordType": "market",
        "sz": action.sz_str,
    }
    if action.tgt_ccy:
        body["tgtCcy"] = action.tgt_ccy
    print(f"\n[margin] {action.inst_id} {action.side} sz={action.sz_str} (≈{action.sz} base)")
    status, resp_body, headers = request_okx(
        base_url,
        "POST",
        "/api/v5/trade/order",
        api_key,
        api_secret,
        passphrase,
        body=body,
        timeout=timeout,
        simulated=simulated,
    )
    ok, brief = okx_response_ok(resp_body)
    final_ok = (200 <= status < 300) and ok
    tag = "OK" if final_ok else "ERR"
    print(f"  [{tag}] status={status} reqId={headers.get('x-request-id')}")
    try:
        print(f"  {json.dumps(json.loads(resp_body), ensure_ascii=False)}")
    except json.JSONDecodeError:
        print(f"  {resp_body}")
    if not final_ok and brief:
        print(f"  rejected: {brief}", file=sys.stderr)
    return final_ok


def submit_swap_order(
    base_url: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    action: SwapAction,
    td_mode: str,
    timeout: int,
    simulated: bool,
) -> bool:
    body: Dict[str, Any] = {
        "instId": action.inst_id,
        "tdMode": td_mode,
        "side": action.side,
        "ordType": "market",
        "sz": action.sz_str,
        "reduceOnly": False,
    }
    print(
        f"\n[swap] {action.inst_id} {action.side} sz={action.sz_str} contracts "
        f"(≈{action.base_qty:.8f} base)"
    )
    status, resp_body, headers = request_okx(
        base_url,
        "POST",
        "/api/v5/trade/order",
        api_key,
        api_secret,
        passphrase,
        body=body,
        timeout=timeout,
        simulated=simulated,
    )
    ok, brief = okx_response_ok(resp_body)
    final_ok = (200 <= status < 300) and ok
    tag = "OK" if final_ok else "ERR"
    print(f"  [{tag}] status={status} reqId={headers.get('x-request-id')}")
    try:
        print(f"  {json.dumps(json.loads(resp_body), ensure_ascii=False)}")
    except json.JSONDecodeError:
        print(f"  {resp_body}")
    if not final_ok and brief:
        print(f"  rejected: {brief}", file=sys.stderr)
    return final_ok


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cancel orders and close/align exposure for OKX intra arb",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--suffix", required=True, help="env suffix, e.g. 'arb01' → okex-intra-arb01")
    parser.add_argument(
        "--mode",
        choices=["full-exit", "align-exposure"],
        default="full-exit",
        help=(
            "full-exit closes both margin and swap legs to zero; "
            "align-exposure only clears net exposure by reducing the larger leg"
        ),
    )
    parser.add_argument(
        "--symbol",
        default="",
        help="Optional single asset filter (base coin, e.g. BTC). Empty = all assets in snapshot.",
    )
    parser.add_argument(
        "--skip-assets",
        dest="skip_assets",
        default="",
        help="Comma-separated assets to skip (e.g. BNB,SOL)",
    )
    parser.add_argument(
        "--min-net-usdt",
        type=float,
        default=5.0,
        dest="min_net_usdt",
        help="Skip rows where |open_qty|*mark, |hedge_qty|*mark, AND |net_usdt| are all below this",
    )
    parser.add_argument(
        "--td-mode",
        dest="td_mode",
        choices=["cross", "isolated"],
        default="cross",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="OKX REST base URL")
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--simulate", action="store_true", help="x-simulated-trading: 1 (paper)")
    parser.add_argument("--execute", action="store_true", help="actually submit; default dry-run")
    parser.add_argument(
        "--skip-cancel",
        action="store_true",
        help="Skip the up-front cancel-all step (useful if already cancelled)",
    )
    parser.add_argument(
        "--cancel-settle-sec",
        type=float,
        default=1.5,
        help="Sleep after cancel-all before re-reading snapshot",
    )
    parser.add_argument(
        "--post-execute-sleep-sec",
        type=float,
        default=1.0,
        help="Sleep after orders before fetching residual snapshot",
    )
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


def estimate_mark(row: ExposureRow) -> float:
    """Best-effort price estimate from net_qty / net_usdt; falls back to 0."""
    qty = row.open_qty + row.hedge_qty
    if abs(qty) > 1e-12 and abs(row.net_usdt) > 0:
        return abs(row.net_usdt / qty)
    if abs(row.open_qty) > 1e-12:
        return abs(row.net_usdt) / abs(row.open_qty) if abs(row.net_usdt) > 0 else 0.0
    return 0.0


def main() -> None:
    args = parse_args()
    api_key, api_secret, passphrase = load_credentials()
    base_url = args.base_url.rstrip("/")
    simulated = args.simulate

    print(
        f"[info] suffix={args.suffix} mode={args.mode} "
        f"execute={args.execute} simulate={simulated} td_mode={args.td_mode}"
    )

    if not args.skip_cancel:
        if args.execute:
            cancel_all_orders(sys.executable, simulated, args.timeout)
            if args.cancel_settle_sec > 0:
                print(f"\n[info] sleeping {args.cancel_settle_sec}s for cancellations to settle ...")
                time.sleep(args.cancel_settle_sec)
        else:
            print("[cancel] dry-run: would call okx_cancel_all_margin_orders.py + okx_swap_open_orders.py")

    print("\n[info] fetching dashboard snapshot ...")
    rows = fetch_snapshot(args.suffix)
    if not rows:
        print("No exposure rows found. Nothing to close.")
        return

    print("[info] fetching OKX SPOT instrument specs ...")
    spot_specs = fetch_spot_specs(base_url, args.timeout)
    print("[info] fetching OKX SWAP instrument specs ...")
    swap_specs = fetch_swap_specs(base_url, args.timeout)

    skip = {s.strip().upper() for s in args.skip_assets.split(",") if s.strip()}
    only_asset = args.symbol.strip().upper()

    plans: List[Tuple[ExposureRow, Optional[MarginAction], str, Optional[SwapAction], str]] = []

    print()
    print(
        f"{'Asset':<8} {'OpenQty':>14} {'HedgeQty':>14} {'NetUSDT':>10} "
        f"{'MgnSide':>8} {'MgnSz':>14} {'SwapSide':>8} {'SwapSz':>10}"
    )
    print("-" * 100)

    for row in rows:
        if only_asset and row.asset != only_asset:
            continue
        if row.asset in skip:
            print(f"  [skip] {row.asset} (in skip list)")
            continue

        mark = estimate_mark(row)
        open_usdt = abs(row.open_qty) * mark
        hedge_usdt = abs(row.hedge_qty) * mark
        if (
            open_usdt < args.min_net_usdt
            and hedge_usdt < args.min_net_usdt
            and abs(row.net_usdt) < args.min_net_usdt
        ):
            continue

        spot_id = asset_to_spot_id(row.asset)
        swap_id = asset_to_swap_id(row.asset)
        spot_spec = spot_specs.get(spot_id)
        swap_spec = swap_specs.get(swap_id)

        if args.mode == "align-exposure":
            margin_action, margin_note, swap_action, swap_note = build_align_actions(
                row,
                spot_spec,
                swap_spec,
            )
        else:
            margin_action: Optional[MarginAction] = None
            margin_note = ""
            if spot_spec is None:
                margin_note = f"no live SPOT instrument {spot_id}"
            else:
                margin_action, margin_note = build_margin_action(row.open_qty, spot_spec)

            swap_action: Optional[SwapAction] = None
            swap_note = ""
            if swap_spec is None:
                swap_note = f"no live SWAP instrument {swap_id}"
            else:
                swap_action, swap_note = build_swap_action(row.hedge_qty, swap_spec)

        m_side = margin_action.side if margin_action else "--"
        m_sz = margin_action.sz_str if margin_action else (margin_note or "--")
        s_side = swap_action.side if swap_action else "--"
        s_sz = swap_action.sz_str if swap_action else (swap_note or "--")

        print(
            f"{row.asset:<8} {row.open_qty:>14.8f} {row.hedge_qty:>14.8f} {row.net_usdt:>10.2f} "
            f"{m_side:>8} {m_sz:>14} {s_side:>8} {s_sz:>10}"
        )

        if margin_action is None and swap_action is None:
            continue
        plans.append((row, margin_action, margin_note, swap_action, swap_note))

    print("-" * 100)
    margin_count = sum(1 for _, m, _, _, _ in plans if m is not None)
    swap_count = sum(1 for _, _, _, s, _ in plans if s is not None)
    print(f"Plan: {margin_count} margin orders, {swap_count} swap orders, {len(plans)} assets")

    if not plans:
        print("Nothing to close.")
        return

    if not args.execute:
        print("\nDry-run mode. Add --execute to actually submit orders.")
        return

    print("\n" + "=" * 100)
    print("EXECUTING ORDERS")
    print("=" * 100)

    failures = 0
    for row, margin_action, _, swap_action, _ in plans:
        if margin_action is not None:
            if not submit_margin_order(
                base_url, api_key, api_secret, passphrase,
                margin_action, args.td_mode, args.timeout, simulated,
            ):
                failures += 1
        if swap_action is not None:
            if not submit_swap_order(
                base_url, api_key, api_secret, passphrase,
                swap_action, args.td_mode, args.timeout, simulated,
            ):
                failures += 1

    if args.post_execute_sleep_sec > 0:
        print(f"\n[info] sleeping {args.post_execute_sleep_sec}s before residual check ...")
        time.sleep(args.post_execute_sleep_sec)

    print("\n[info] residual exposure snapshot:")
    residual = fetch_snapshot(args.suffix)
    print(f"{'Asset':<8} {'OpenQty':>14} {'HedgeQty':>14} {'NetUSDT':>10}")
    print("-" * 50)
    nonzero = 0
    for row in residual:
        if only_asset and row.asset != only_asset:
            continue
        if row.asset in skip:
            continue
        if (
            abs(row.open_qty) <= 1e-12
            and abs(row.hedge_qty) <= 1e-12
            and abs(row.net_usdt) < args.min_net_usdt
        ):
            continue
        nonzero += 1
        print(
            f"{row.asset:<8} {row.open_qty:>14.8f} {row.hedge_qty:>14.8f} {row.net_usdt:>10.2f}"
        )
    if nonzero == 0:
        print("(all assets cleared within --min-net-usdt threshold)")

    print()
    if failures:
        print(f"WARN: {failures} order submissions failed", file=sys.stderr)
        sys.exit(1)
    print("All orders submitted successfully.")


if __name__ == "__main__":
    main()
