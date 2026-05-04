#!/usr/bin/env python3
"""Cancel Gate intra orders, then either full-exit or align futures to spot.

Modes:
  - full-exit: close the Gate unified spot leg and USDT futures leg to zero,
    then repay Gate unified loans when residual exposure is within threshold.
  - align-futures-to-spot: leave spot unchanged and trade futures so hedge_qty
    becomes -open_qty.

The script reads pre_trade_exposure from the local dashboard:
  GET http://<DASHBOARD_HOST>:<DASHBOARD_PORT>/intra/gate-intra-<suffix>/snapshot

Default is dry-run. Add --execute to cancel orders and submit IOC orders.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import math
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR, getcontext
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode
import urllib.request

import requests

getcontext().prec = 36

DASHBOARD_HOST = os.environ.get("DASHBOARD_HOST", "127.0.0.1")
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "4191"))
HOST = os.environ.get("GATE_API_BASE", "https://api.gateio.ws").rstrip("/")
PREFIX = "/api/v4"

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(REPO_ROOT, "scripts")


def dec(value: Any, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return Decimal(default)


def fmt_dec(value: Decimal) -> str:
    if value == 0:
        return "0"
    text = format(value.normalize(), "f")
    if text == "-0":
        return "0"
    return text


def precision_step(precision: int) -> Decimal:
    if precision <= 0:
        return Decimal("1")
    return Decimal(1).scaleb(-precision)


def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def load_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("GATE_API_KEY", "").strip()
    api_secret = os.environ.get("GATE_API_SECRET", "").strip()
    missing = [
        name
        for name, value in (
            ("GATE_API_KEY", api_key),
            ("GATE_API_SECRET", api_secret),
        )
        if not value
    ]
    if missing:
        raise SystemExit(f"missing env vars: {', '.join(missing)}")
    return api_key, api_secret


def build_query(params: Dict[str, Any]) -> str:
    items = [(key, value) for key, value in params.items() if value not in ("", None)]
    items.sort(key=lambda item: item[0])
    return urlencode(items, doseq=True)


def sign(method: str, path: str, query_string: str, body: str, api_secret: str) -> Dict[str, str]:
    timestamp = str(int(time.time()))
    body_hash = hashlib.sha512(body.encode("utf-8")).hexdigest()
    payload = f"{method.upper()}\n{path}\n{query_string}\n{body_hash}\n{timestamp}"
    signature = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha512).hexdigest()
    return {"Timestamp": timestamp, "SIGN": signature}


def private_request(
    api_key: str,
    api_secret: str,
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
) -> Tuple[int, Any]:
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True) if payload else ""
    query_string = build_query(params or {})
    url = f"{HOST}{PREFIX}{path}"
    if query_string:
        url = f"{url}?{query_string}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "KEY": api_key,
    }
    headers.update(sign(method, f"{PREFIX}{path}", query_string, body, api_secret))
    resp = requests.request(method.upper(), url, headers=headers, data=body, timeout=timeout)
    try:
        data = resp.json()
    except ValueError:
        data = {"raw": resp.text}
    return resp.status_code, data


def public_get(path: str, *, params: Optional[Dict[str, Any]] = None, timeout: int = 10) -> Any:
    url = f"{HOST}{PREFIX}{path}"
    resp = requests.get(url, params=params or {}, timeout=timeout)
    try:
        data = resp.json()
    except ValueError:
        raise RuntimeError(f"Gate public GET {path} returned non-JSON: {resp.status_code} {resp.text}")
    if resp.status_code >= 300:
        raise RuntimeError(f"Gate public GET {path} failed: http={resp.status_code} body={data}")
    return data


@dataclass
class ExposureRow:
    asset: str
    open_qty: Decimal
    open_usdt: Decimal
    hedge_qty: Decimal
    hedge_usdt: Decimal
    net_qty: Decimal
    net_usdt: Decimal


def fetch_snapshot(suffix: str) -> List[ExposureRow]:
    url = f"http://{DASHBOARD_HOST}:{DASHBOARD_PORT}/intra/gate-intra-{suffix}/snapshot"
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        raise SystemExit(f"failed to fetch snapshot from {url}: {exc}")

    rows: List[ExposureRow] = []
    for entry in data.get("entries", []):
        if entry.get("channel") != "pre_trade_exposure":
            continue
        for row in entry.get("entry", {}).get("rows", []):
            if row.get("is_total"):
                continue
            asset = str(row.get("asset") or "").strip().upper()
            if not asset:
                continue
            rows.append(
                ExposureRow(
                    asset=asset,
                    open_qty=dec(row.get("open_qty")),
                    open_usdt=dec(row.get("open_usdt")),
                    hedge_qty=dec(row.get("hedge_qty")),
                    hedge_usdt=dec(row.get("hedge_usdt")),
                    net_qty=dec(row.get("net_qty")),
                    net_usdt=dec(row.get("net_usdt")),
                )
            )
    return rows


@dataclass
class LoanRow:
    currency: str
    amount: Decimal
    category: str


def parse_asset_csv(value: str) -> set[str]:
    return {normalize_asset(part) for part in value.split(",") if part.strip()}


def fetch_unified_loans(api_key: str, api_secret: str, timeout: int) -> List[LoanRow]:
    status, data = private_request(api_key, api_secret, "GET", "/unified/loans", timeout=timeout)
    if status >= 300:
        raise RuntimeError(f"Gate GET /unified/loans failed: http={status} body={data}")
    if not isinstance(data, list):
        raise RuntimeError(f"Gate GET /unified/loans returned non-list body: {data}")

    rows: List[LoanRow] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        currency = normalize_asset(str(entry.get("currency") or ""))
        amount = dec(entry.get("amount"))
        if not currency or amount <= 0:
            continue
        rows.append(
            LoanRow(
                currency=currency,
                amount=amount,
                category=str(entry.get("type") or "").strip().lower(),
            )
        )
    rows.sort(key=lambda row: row.currency)
    return rows


def select_repay_loans(
    loans: List[LoanRow],
    *,
    only_asset: str,
    skip_assets: set[str],
    repay_assets: set[str],
) -> List[LoanRow]:
    selected: List[LoanRow] = []
    for loan in loans:
        if only_asset and loan.currency != only_asset:
            continue
        if loan.currency in skip_assets:
            continue
        if repay_assets and loan.currency not in repay_assets:
            continue
        selected.append(loan)
    return selected


def print_repay_plan(loans: List[LoanRow]) -> None:
    print("\n[repay] Gate unified loans")
    if not loans:
        print("  No repayable Gate unified loans found.")
        return
    print(f"{'Currency':<10} {'Amount':>24} {'Category':>12}")
    print("-" * 50)
    for loan in loans:
        print(f"{loan.currency:<10} {fmt_dec(loan.amount):>24} {loan.category or '--':>12}")


def residual_repay_blockers(
    rows: List[ExposureRow],
    *,
    only_asset: str,
    skip_assets: set[str],
    min_usdt: Decimal,
) -> List[ExposureRow]:
    blockers: List[ExposureRow] = []
    for row in rows:
        if only_asset and row.asset != only_asset:
            continue
        if row.asset in skip_assets:
            continue
        gross_usdt = abs(row.open_usdt) + abs(row.hedge_usdt)
        if gross_usdt >= min_usdt or abs(row.net_usdt) >= min_usdt:
            blockers.append(row)
    return blockers


@dataclass
class SpotSpec:
    pair: str
    amount_step: Decimal
    min_base_amount: Decimal
    price_step: Decimal
    min_quote_amount: Decimal


@dataclass
class FuturesSpec:
    contract: str
    quanto: Decimal
    size_step: Decimal
    min_size: Decimal
    enable_decimal: bool


def asset_to_pair(asset: str) -> str:
    return f"{asset.upper()}_USDT"


def asset_to_contract(asset: str) -> str:
    return f"{asset.upper()}_USDT"


def normalize_asset(value: str) -> str:
    text = value.strip().upper()
    if text.endswith("_USDT"):
        return text[:-5]
    cleaned = "".join(ch for ch in text if ch.isalnum())
    if cleaned.endswith("USDT") and len(cleaned) > 4:
        return cleaned[:-4]
    return cleaned


def fetch_spot_specs(timeout: int) -> Dict[str, SpotSpec]:
    data = public_get("/spot/currency_pairs", timeout=timeout)
    specs: Dict[str, SpotSpec] = {}
    if not isinstance(data, list):
        return specs
    for entry in data:
        if not isinstance(entry, dict):
            continue
        pair = str(entry.get("id") or entry.get("currency_pair") or "").upper()
        if not pair.endswith("_USDT"):
            continue
        status = str(entry.get("trade_status") or entry.get("status") or "").lower()
        if status and status not in {"tradable", "trading", "1"}:
            continue
        amount_precision = int(dec(entry.get("amount_precision"), "8"))
        price_precision = int(dec(entry.get("precision"), "8"))
        amount_step = precision_step(amount_precision)
        price_step = precision_step(price_precision)
        min_base = dec(entry.get("min_base_amount"))
        min_quote = dec(entry.get("min_quote_amount"))
        specs[pair] = SpotSpec(
            pair=pair,
            amount_step=amount_step,
            min_base_amount=min_base,
            price_step=price_step,
            min_quote_amount=min_quote,
        )
    return specs


def fetch_futures_specs(settle: str, timeout: int) -> Dict[str, FuturesSpec]:
    data = public_get(f"/futures/{settle}/contracts", timeout=timeout)
    specs: Dict[str, FuturesSpec] = {}
    if not isinstance(data, list):
        return specs
    for entry in data:
        if not isinstance(entry, dict):
            continue
        contract = str(entry.get("name") or entry.get("contract") or "").upper()
        if not contract.endswith("_USDT"):
            continue
        in_delisting = parse_bool(entry.get("in_delisting"))
        trade_status = str(entry.get("trade_status") or entry.get("status") or "").lower()
        if in_delisting or trade_status in {"delisting", "finished", "disabled"}:
            continue
        quanto = dec(entry.get("quanto_multiplier"), "1")
        if quanto <= 0:
            continue
        step = dec(entry.get("order_size_step"), "1")
        min_size = dec(entry.get("order_size_min"), "1")
        specs[contract] = FuturesSpec(
            contract=contract,
            quanto=quanto,
            size_step=step if step > 0 else Decimal("1"),
            min_size=min_size if min_size > 0 else Decimal("1"),
            enable_decimal=parse_bool(entry.get("enable_decimal")),
        )
    return specs


def fetch_best_bid_ask(pair: str, timeout: int) -> Tuple[Decimal, Decimal]:
    data = public_get("/spot/order_book", params={"currency_pair": pair, "limit": 1}, timeout=timeout)
    bids = data.get("bids") if isinstance(data, dict) else None
    asks = data.get("asks") if isinstance(data, dict) else None
    if not bids or not asks:
        raise RuntimeError(f"empty order book for {pair}: {data}")

    def price(level: Any) -> Decimal:
        if isinstance(level, dict):
            return dec(level.get("p") or level.get("price"))
        if isinstance(level, list) and level:
            return dec(level[0])
        return Decimal("0")

    bid = price(bids[0])
    ask = price(asks[0])
    if bid <= 0 or ask <= 0:
        raise RuntimeError(f"invalid order book for {pair}: bid={bid} ask={ask}")
    return bid, ask


@dataclass
class SpotAction:
    pair: str
    side: str
    base_qty: Decimal
    amount: str
    price: str
    raw_base_qty: Decimal


@dataclass
class FuturesAction:
    contract: str
    signed_contracts: Decimal
    size: str
    base_qty: Decimal
    raw_base_qty: Decimal
    reduce_only: bool


def build_spot_action(
    delta_open_qty: Decimal,
    spec: SpotSpec,
    *,
    slippage_bps: Decimal,
    timeout: int,
) -> Tuple[Optional[SpotAction], str]:
    if delta_open_qty == 0:
        return None, "spot_delta=0"
    side = "buy" if delta_open_qty > 0 else "sell"
    raw = abs(delta_open_qty)
    qty = floor_to_step(raw, spec.amount_step)
    if qty <= 0 or (spec.min_base_amount > 0 and qty < spec.min_base_amount):
        return None, f"|spot_delta|={fmt_dec(raw)} below min_base_amount={fmt_dec(spec.min_base_amount)}"

    bid, ask = fetch_best_bid_ask(spec.pair, timeout)
    slip = slippage_bps / Decimal("10000")
    if side == "buy":
        price = ceil_to_step(ask * (Decimal("1") + slip), spec.price_step)
    else:
        price = floor_to_step(bid * (Decimal("1") - slip), spec.price_step)
    if price <= 0:
        return None, f"invalid aggressive IOC price for {spec.pair}"
    quote = qty * price
    if spec.min_quote_amount > 0 and quote < spec.min_quote_amount:
        return None, f"quote={fmt_dec(quote)} below min_quote_amount={fmt_dec(spec.min_quote_amount)}"
    return (
        SpotAction(
            pair=spec.pair,
            side=side,
            base_qty=qty,
            amount=fmt_dec(qty),
            price=fmt_dec(price),
            raw_base_qty=raw,
        ),
        "",
    )


def build_futures_action(
    delta_hedge_qty: Decimal,
    spec: FuturesSpec,
    *,
    reduce_only: bool,
) -> Tuple[Optional[FuturesAction], str]:
    if delta_hedge_qty == 0:
        return None, "hedge_delta=0"
    raw_base = abs(delta_hedge_qty)
    raw_contracts = raw_base / spec.quanto
    contracts = floor_to_step(raw_contracts, spec.size_step)
    if not spec.enable_decimal:
        contracts = contracts.to_integral_value(rounding=ROUND_DOWN)
    if contracts <= 0 or contracts < spec.min_size:
        return None, (
            f"|hedge_delta|={fmt_dec(raw_base)} -> {fmt_dec(raw_contracts)} contracts "
            f"below min_size={fmt_dec(spec.min_size)}"
        )
    signed = contracts if delta_hedge_qty > 0 else -contracts
    return (
        FuturesAction(
            contract=spec.contract,
            signed_contracts=signed,
            size=fmt_dec(signed),
            base_qty=contracts * spec.quanto,
            raw_base_qty=raw_base,
            reduce_only=reduce_only,
        ),
        "",
    )


def cancel_all_orders(python_bin: str, execute: bool) -> None:
    cmd = [python_bin, os.path.join(SCRIPTS_DIR, "gate_cancel_all.py")]
    if not execute:
        cmd.append("--dry-run")
    print("[cancel] === Gate spot/unified + futures open orders ===")
    rc = subprocess.call(cmd)
    if rc != 0:
        print(f"[cancel] WARN gate_cancel_all.py exited {rc}", file=sys.stderr)


def submit_spot_order(
    api_key: str,
    api_secret: str,
    action: SpotAction,
    *,
    timeout: int,
    auto_borrow: bool,
    auto_repay: bool,
    idx: int,
) -> bool:
    payload: Dict[str, Any] = {
        "currency_pair": action.pair,
        "type": "limit",
        "account": "unified",
        "side": action.side,
        "amount": action.amount,
        "price": action.price,
        "time_in_force": "ioc",
        "text": f"t-intragatespot{int(time.time() * 1000)}{idx:02d}",
    }
    if auto_borrow:
        payload["auto_borrow"] = True
    if auto_repay:
        payload["auto_repay"] = True

    print(f"\n[spot] {action.pair} {action.side} amount={action.amount} price={action.price} IOC")
    status, data = private_request(api_key, api_secret, "POST", "/spot/orders", payload=payload, timeout=timeout)
    ok = 200 <= status < 300
    print(f"  [{'OK' if ok else 'ERR'}] status={status} {json.dumps(data, ensure_ascii=True, sort_keys=True)}")
    return ok


def submit_futures_order(
    api_key: str,
    api_secret: str,
    action: FuturesAction,
    *,
    settle: str,
    timeout: int,
    idx: int,
) -> bool:
    payload: Dict[str, Any] = {
        "contract": action.contract,
        "size": action.size,
        "price": "0",
        "tif": "ioc",
        "account": "unified",
        "reduce_only": bool(action.reduce_only),
        "text": f"t-intragatefut{int(time.time() * 1000)}{idx:02d}",
    }
    side = "buy" if action.signed_contracts > 0 else "sell"
    print(
        f"\n[futures] {action.contract} {side} size={action.size} "
        f"(~{fmt_dec(action.base_qty)} base) reduce_only={action.reduce_only}"
    )
    status, data = private_request(
        api_key,
        api_secret,
        "POST",
        f"/futures/{settle}/orders",
        payload=payload,
        timeout=timeout,
    )
    ok = 200 <= status < 300
    print(f"  [{'OK' if ok else 'ERR'}] status={status} {json.dumps(data, ensure_ascii=True, sort_keys=True)}")
    return ok


def submit_unified_repay(
    api_key: str,
    api_secret: str,
    loan: LoanRow,
    *,
    timeout: int,
    idx: int,
) -> bool:
    payload: Dict[str, Any] = {
        "currency": loan.currency,
        "amount": fmt_dec(loan.amount),
        "type": "repay",
        "repaid_all": True,
        "text": f"t-repay{int(time.time() * 1000)}{idx:02d}",
    }
    print(f"\n[repay] {loan.currency} amount={fmt_dec(loan.amount)} repaid_all=true")
    status, data = private_request(api_key, api_secret, "POST", "/unified/loans", payload=payload, timeout=timeout)
    ok = 200 <= status < 300
    print(f"  [{'OK' if ok else 'ERR'}] status={status} {json.dumps(data, ensure_ascii=True, sort_keys=True)}")
    return ok


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cancel orders and close/align exposure for Gate intra arb",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--suffix", required=True, help="env suffix, e.g. arb01 -> gate-intra-arb01")
    parser.add_argument(
        "--mode",
        choices=["full-exit", "align-futures-to-spot", "align-exposure"],
        default="full-exit",
        help=(
            "full-exit closes spot and futures to zero; align-futures-to-spot leaves spot "
            "unchanged and trades futures to hedge it. align-exposure is accepted as an alias."
        ),
    )
    parser.add_argument("--symbol", default="", help="Optional asset/pair filter, e.g. BTC or BTCUSDT")
    parser.add_argument("--skip-assets", default="", help="Comma-separated assets to skip")
    parser.add_argument("--min-net-usdt", type=float, default=5.0, help="Dust threshold for reporting/filtering")
    parser.add_argument("--settle", default="usdt", help="Gate futures settle currency")
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--spot-slippage-bps", type=float, default=50.0, help="Aggressive spot IOC price offset")
    parser.add_argument("--execute", action="store_true", help="actually cancel and submit orders")
    parser.add_argument("--skip-cancel", action="store_true", help="skip the up-front cancel-all step")
    parser.add_argument("--cancel-settle-sec", type=float, default=1.5)
    parser.add_argument("--post-execute-sleep-sec", type=float, default=1.0)
    parser.add_argument("--disable-auto-borrow", action="store_true", help="do not set auto_borrow on spot orders")
    parser.add_argument("--disable-auto-repay", action="store_true", help="do not set auto_repay on spot orders")
    parser.add_argument(
        "--skip-repay",
        action="store_true",
        help="skip post-full-exit Gate unified loan repayment",
    )
    parser.add_argument(
        "--repay-assets",
        default="",
        help="Comma-separated repayment asset allowlist; defaults to all selected assets",
    )
    parser.add_argument(
        "--force-repay",
        action="store_true",
        help="repay even when residual gross/net exposure exceeds --min-net-usdt",
    )
    parser.add_argument("--post-repay-sleep-sec", type=float, default=1.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    api_key, api_secret = load_credentials()
    settle = args.settle.lower()
    mode = args.mode
    if mode == "align-exposure":
        mode = "align-futures-to-spot"
    only_asset = normalize_asset(args.symbol) if args.symbol else ""
    skip = {normalize_asset(part) for part in args.skip_assets.split(",") if part.strip()}

    print(f"[info] suffix={args.suffix} mode={mode} execute={args.execute} settle={settle}")

    if not args.skip_cancel:
        cancel_all_orders(sys.executable, args.execute)
        if args.execute and args.cancel_settle_sec > 0:
            print(f"\n[info] sleeping {args.cancel_settle_sec}s for cancellations to settle ...")
            time.sleep(args.cancel_settle_sec)

    print("\n[info] fetching dashboard snapshot ...")
    rows = fetch_snapshot(args.suffix)
    if not rows:
        print("No exposure rows found. Nothing to close.")
        return

    print("[info] fetching Gate futures contract specs ...")
    futures_specs = fetch_futures_specs(settle, args.timeout)
    spot_specs: Dict[str, SpotSpec] = {}
    if mode == "full-exit":
        print("[info] fetching Gate spot currency pair specs ...")
        spot_specs = fetch_spot_specs(args.timeout)

    plans: List[Tuple[ExposureRow, Optional[SpotAction], str, Optional[FuturesAction], str]] = []

    print()
    print(
        f"{'Asset':<8} {'OpenQty':>14} {'HedgeQty':>14} {'NetUSDT':>10} "
        f"{'SpotSide':>8} {'SpotSz':>14} {'FutSize':>12} {'FutRO':>5}"
    )
    print("-" * 98)

    for row in rows:
        if only_asset and row.asset != only_asset:
            continue
        if row.asset in skip:
            print(f"  [skip] {row.asset} (in skip list)")
            continue

        spot_action: Optional[SpotAction] = None
        spot_note = "--"
        futures_action: Optional[FuturesAction] = None
        futures_note = "--"

        contract = asset_to_contract(row.asset)
        fut_spec = futures_specs.get(contract)

        if mode == "full-exit":
            pair = asset_to_pair(row.asset)
            spot_spec = spot_specs.get(pair)
            if spot_spec is None:
                spot_note = f"no spot {pair}"
            else:
                spot_action, spot_note = build_spot_action(
                    -row.open_qty,
                    spot_spec,
                    slippage_bps=dec(args.spot_slippage_bps),
                    timeout=args.timeout,
                )
            if fut_spec is None:
                futures_note = f"no futures {contract}"
            else:
                futures_action, futures_note = build_futures_action(
                    -row.hedge_qty,
                    fut_spec,
                    reduce_only=True,
                )
        else:
            if abs(row.net_usdt) < dec(args.min_net_usdt) and row.net_qty == 0:
                continue
            if fut_spec is None:
                futures_note = f"no futures {contract}"
            else:
                target_hedge = -row.open_qty
                delta_hedge = target_hedge - row.hedge_qty
                futures_action, futures_note = build_futures_action(
                    delta_hedge,
                    fut_spec,
                    reduce_only=False,
                )

        spot_side = spot_action.side if spot_action else "--"
        spot_sz = spot_action.amount if spot_action else spot_note
        fut_sz = futures_action.size if futures_action else futures_note
        fut_ro = str(futures_action.reduce_only) if futures_action else "--"

        print(
            f"{row.asset:<8} {fmt_dec(row.open_qty):>14} {fmt_dec(row.hedge_qty):>14} "
            f"{fmt_dec(row.net_usdt):>10} {spot_side:>8} {spot_sz:>14} {fut_sz:>12} {fut_ro:>5}"
        )

        if spot_action is not None or futures_action is not None:
            plans.append((row, spot_action, spot_note, futures_action, futures_note))

    print("-" * 98)
    spot_count = sum(1 for _, spot, _, _, _ in plans if spot is not None)
    futures_count = sum(1 for _, _, _, fut, _ in plans if fut is not None)
    print(f"Plan: {spot_count} spot orders, {futures_count} futures orders, {len(plans)} assets")

    if not plans:
        print("Nothing to submit.")
        return
    if not args.execute:
        print("\nDry-run mode. Add --execute to actually cancel and submit orders.")
        return

    print("\n" + "=" * 98)
    print("EXECUTING ORDERS")
    print("=" * 98)
    failures = 0
    for idx, (_, spot_action, _, futures_action, _) in enumerate(plans, start=1):
        if spot_action is not None:
            if not submit_spot_order(
                api_key,
                api_secret,
                spot_action,
                timeout=args.timeout,
                auto_borrow=not args.disable_auto_borrow,
                auto_repay=not args.disable_auto_repay,
                idx=idx,
            ):
                failures += 1
        if futures_action is not None:
            if not submit_futures_order(
                api_key,
                api_secret,
                futures_action,
                settle=settle,
                timeout=args.timeout,
                idx=idx,
            ):
                failures += 1

    if args.post_execute_sleep_sec > 0:
        print(f"\n[info] sleeping {args.post_execute_sleep_sec}s before residual check ...")
        time.sleep(args.post_execute_sleep_sec)

    print("\n[info] residual exposure snapshot:")
    residual = fetch_snapshot(args.suffix)
    print(f"{'Asset':<8} {'OpenQty':>14} {'HedgeQty':>14} {'NetUSDT':>10}")
    print("-" * 52)
    nonzero = 0
    min_net = dec(args.min_net_usdt)
    for row in residual:
        if only_asset and row.asset != only_asset:
            continue
        if row.asset in skip:
            continue
        if row.open_qty == 0 and row.hedge_qty == 0 and abs(row.net_usdt) < min_net:
            continue
        nonzero += 1
        print(
            f"{row.asset:<8} {fmt_dec(row.open_qty):>14} {fmt_dec(row.hedge_qty):>14} "
            f"{fmt_dec(row.net_usdt):>10}"
        )
    if nonzero == 0:
        print("(all tracked assets cleared/aligned within threshold)")

    if failures:
        print(f"WARN: {failures} order submissions failed", file=sys.stderr)
        raise SystemExit(1)
    print("All orders submitted successfully.")


if __name__ == "__main__":
    main()
