#!/usr/bin/env python3
"""Flatten Bybit Unified Trading Account positions (FR arb cleanup).

Self-contained: no imports from other scripts in this repo.

Four-phase pipeline per symbol (no dust convert):
  Phase R — explicit /v5/account/no-convert-repay (per repo memory; buying back
            alone updates walletBalance but does NOT zero borrowAmount/LTV/IM)
  Phase U — USDT perpetual (category=linear) MARKET align/close
  Phase B — clear-mode only — spot BUY (category=spot), THEN explicit repay
  Phase S — clear-mode only — spot SELL for USDT

Modes:
  align (default) — net_qty = walletBalance - borrowAmount + linear_position = 0;
                    linear order is allowed to add/flip to match spot exposure
  clear           — close linear to 0 with reduceOnly; B or S to drain asset

Behavior:
  - CWD basename must match ^bybit_fr_ or ^bybit-intra-.
  - Auto-sources ./env.sh; BYBIT_API_KEY/SECRET — env.sh always wins.
  - Dry-run by default; --execute required.

Usage:
  python3 scripts/flatten_bybit_pm.py --symbols BTCUSDT,ETHUSDT
  python3 scripts/flatten_bybit_pm.py --symbols BTCUSDT --mode clear --execute
"""

from __future__ import annotations

import argparse
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
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
from typing import Any, Dict, List, Optional, Tuple


BYBIT_BASE = os.environ.get("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
RECV_WINDOW_MS = "5000"

ENV_DIR_PATTERN = re.compile(r"^(bybit_fr_|bybit[-_]intra[-_])")
AUTHORITATIVE_KEYS = ("BYBIT_API_KEY", "BYBIT_API_SECRET")
ZERO = Decimal("0")


@dataclass
class SymbolSpec:
    symbol: str           # BTCUSDT (used as-is by both spot & linear)
    asset: str            # BTC
    spot_qty_step: Decimal
    spot_min_qty: Decimal
    linear_qty_step: Decimal
    linear_min_qty: Decimal


@dataclass
class SymbolState:
    spec: SymbolSpec
    free: Decimal              # walletBalance (>=0 for normal coins; bybit reports negatives if shorted)
    borrowed: Decimal          # borrowAmount (positive)
    interest: Decimal          # accruedInterest
    linear_position: Decimal   # signed base-coin qty


@dataclass
class SymbolPlan:
    state: SymbolState
    repay_amt: Decimal
    free_after: Decimal
    borrow_after: Decimal
    net_qty: Decimal
    linear_side: Optional[str]
    linear_qty: Decimal
    linear_reduce_only: bool
    linear_skip_reason: Optional[str]
    buyback_amt: Decimal
    buyback_skip_reason: Optional[str]
    selldown_amt: Decimal
    selldown_skip_reason: Optional[str]


@dataclass
class PhaseOutcome:
    ok: Optional[bool] = None
    err: str = ""


@dataclass
class SymbolResult:
    plan: SymbolPlan
    repay: PhaseOutcome = field(default_factory=PhaseOutcome)
    linear: PhaseOutcome = field(default_factory=PhaseOutcome)
    buyback: PhaseOutcome = field(default_factory=PhaseOutcome)
    post_buyback_repay: PhaseOutcome = field(default_factory=PhaseOutcome)
    selldown: PhaseOutcome = field(default_factory=PhaseOutcome)


# -------------------- helpers --------------------


def decimal_or(value, default="0"):
    if value in (None, ""):
        return Decimal(default)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal(default)


def floor_to_step(value, step):
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def ceil_to_step(value, step):
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_UP) * step


def format_decimal(value):
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def http_request(url, *, method="GET", headers=None, data=None, timeout=15):
    req = urllib.request.Request(url, data=data, method=method.upper())
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001
        return 0, str(exc)


def bybit_sign(api_key, api_secret, timestamp_ms, recv_window, payload):
    raw = f"{timestamp_ms}{api_key}{recv_window}{payload}"
    return hmac.new(api_secret.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()


def bybit_private(method, path, api_key, api_secret, *, query=None, body=None, timeout=15):
    method = method.upper()
    query_str = ""
    body_str = ""
    if method == "GET" and query:
        # Use sorted, key=value joined by & (Bybit V5 signing)
        items = sorted(query.items(), key=lambda kv: kv[0])
        query_str = "&".join(f"{k}={v}" for k, v in items)
    elif method != "GET" and body is not None:
        body_str = json.dumps(body, ensure_ascii=False, separators=(",", ":"))

    payload = query_str if method == "GET" else body_str
    ts_ms = str(int(time.time() * 1000))
    sig = bybit_sign(api_key, api_secret, ts_ms, RECV_WINDOW_MS, payload)
    url = f"{BYBIT_BASE}{path}"
    if query_str:
        url = f"{url}?{query_str}"
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": sig,
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": ts_ms,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW_MS,
        "Content-Type": "application/json",
    }
    return http_request(
        url, method=method, headers=headers,
        data=None if method == "GET" else body_str.encode("utf-8"),
        timeout=timeout,
    )


def bybit_ok(body: str) -> Tuple[bool, str]:
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return False, "non-json"
    code = parsed.get("retCode")
    if code == 0:
        return True, ""
    return False, f"retCode={code} retMsg={parsed.get('retMsg', '')}"


def check_env_safety() -> str:
    cwd_name = os.path.basename(os.path.normpath(os.getcwd()))
    if not ENV_DIR_PATTERN.match(cwd_name):
        sys.stderr.write(
            f"[ERROR] CWD basename must match ^bybit_fr_ or ^bybit-intra-, got {cwd_name!r} "
            f"(CWD={os.getcwd()}). Aborting for safety.\n"
        )
        sys.exit(2)
    return cwd_name


def auto_source_env_sh() -> None:
    env_path = os.path.join(os.getcwd(), "env.sh")
    if not os.path.isfile(env_path):
        return
    proc = subprocess.run(
        ["bash", "-lc", f"set -a; source {env_path} >/dev/null 2>&1; env -0"],
        check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        return
    for item in proc.stdout.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key_b, value_b = item.split(b"=", 1)
        key = key_b.decode("utf-8", errors="ignore")
        new_value = value_b.decode("utf-8", errors="replace")
        if key in AUTHORITATIVE_KEYS:
            old = os.environ.get(key)
            if old and old != new_value:
                sys.stderr.write(
                    f"[WARN] env.sh overrides existing {key} from process env "
                    f"(env.sh wins to prevent cross-account ops)\n"
                )
            os.environ[key] = new_value
        else:
            if key in os.environ:
                continue
            os.environ[key] = new_value


def load_credentials() -> Tuple[str, str]:
    k = os.environ.get("BYBIT_API_KEY", "").strip()
    s = os.environ.get("BYBIT_API_SECRET", "").strip()
    if not k or not s:
        sys.stderr.write("[ERROR] missing BYBIT_API_KEY / BYBIT_API_SECRET.\n")
        sys.exit(2)
    return k, s


def parse_symbols(raw: str) -> List[str]:
    out = []
    for tok in raw.split(","):
        s = tok.strip().upper()
        if s:
            out.append(s)
    if not out:
        sys.exit("[ERROR] --symbols produced empty list")
    return out


def split_usdt(symbol: str) -> str:
    if symbol.endswith("USDT"):
        return symbol[:-4]
    sys.exit(f"[ERROR] only USDT-quoted symbols supported, got {symbol!r}")


# -------------------- state fetch --------------------


def fetch_instrument_specs(category: str, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for sym in symbols:
        status, body = http_request(
            f"{BYBIT_BASE}/v5/market/instruments-info?category={category}&symbol={sym}",
            timeout=15,
        )
        if not (200 <= status < 300):
            sys.exit(f"[ERROR] instruments-info {category} {sym} status={status} body={body}")
        parsed = json.loads(body)
        if parsed.get("retCode") != 0:
            sys.exit(f"[ERROR] instruments-info {category} {sym}: {body}")
        rows = parsed.get("result", {}).get("list", []) or []
        if not rows:
            sys.exit(f"[ERROR] no instrument info for {category} {sym}")
        out[sym] = rows[0]
    return out


def fetch_specs(symbols: List[str]) -> Dict[str, SymbolSpec]:
    spot = fetch_instrument_specs("spot", symbols)
    linear = fetch_instrument_specs("linear", symbols)
    out: Dict[str, SymbolSpec] = {}
    for sym in symbols:
        sp_lot = spot[sym].get("lotSizeFilter", {}) or {}
        ln_lot = linear[sym].get("lotSizeFilter", {}) or {}
        out[sym] = SymbolSpec(
            symbol=sym,
            asset=split_usdt(sym),
            spot_qty_step=decimal_or(sp_lot.get("basePrecision") or sp_lot.get("qtyStep"), "1"),
            spot_min_qty=decimal_or(sp_lot.get("minOrderQty"), "0"),
            linear_qty_step=decimal_or(ln_lot.get("qtyStep"), "1"),
            linear_min_qty=decimal_or(ln_lot.get("minOrderQty"), "0"),
        )
    return out


def fetch_wallet(api_key, api_secret) -> Dict[str, Tuple[Decimal, Decimal, Decimal]]:
    status, body = bybit_private(
        "GET", "/v5/account/wallet-balance", api_key, api_secret,
        query={"accountType": "UNIFIED"},
    )
    if not (200 <= status < 300):
        sys.exit(f"[ERROR] wallet-balance status={status} body={body}")
    parsed = json.loads(body)
    if parsed.get("retCode") != 0:
        sys.exit(f"[ERROR] wallet-balance: {body}")
    out: Dict[str, Tuple[Decimal, Decimal, Decimal]] = {}
    for account in parsed.get("result", {}).get("list", []) or []:
        for c in account.get("coin", []) or []:
            coin = str(c.get("coin", "")).upper()
            if not coin:
                continue
            wallet = decimal_or(c.get("walletBalance"))
            borrowed = decimal_or(c.get("borrowAmount"))
            interest = decimal_or(c.get("accruedInterest"))
            out[coin] = (wallet, borrowed, interest)
    return out


def fetch_linear_positions(symbols: List[str], api_key, api_secret) -> Dict[str, Decimal]:
    out: Dict[str, Decimal] = {}
    for sym in symbols:
        status, body = bybit_private(
            "GET", "/v5/position/list", api_key, api_secret,
            query={"category": "linear", "symbol": sym},
        )
        if not (200 <= status < 300):
            sys.stderr.write(f"[WARN] position list {sym} status={status} body={body}\n")
            out[sym] = ZERO
            continue
        parsed = json.loads(body)
        if parsed.get("retCode") != 0:
            sys.stderr.write(f"[WARN] position list {sym}: {body}\n")
            out[sym] = ZERO
            continue
        total = ZERO
        for row in parsed.get("result", {}).get("list", []) or []:
            size = decimal_or(row.get("size"))
            side = str(row.get("side", "")).lower()
            if size == 0:
                continue
            if side == "sell":
                total -= size
            else:
                total += size
        out[sym] = total
    return out


# -------------------- planning --------------------


def plan_symbol(state: SymbolState, mode: str) -> SymbolPlan:
    spec = state.spec
    free = max(state.free, ZERO)   # use positive part for "free" semantics
    borrowed = state.borrowed
    interest = state.interest
    pos = state.linear_position

    repay_amt = min(free, borrowed) if (free > 0 and borrowed > 0) else ZERO
    free_after = free - repay_amt
    borrow_after = borrowed - repay_amt
    net_qty = free_after - borrow_after + pos

    linear_side: Optional[str] = None
    linear_qty = ZERO
    linear_reduce_only = mode == "clear"
    linear_skip: Optional[str] = None
    buyback_amt = ZERO
    buyback_skip: Optional[str] = None
    selldown_amt = ZERO
    selldown_skip: Optional[str] = None

    if mode == "align":
        delta = -net_qty
    else:
        delta = -pos

    if delta == 0:
        linear_skip = "no linear action needed (target delta is zero)"
    else:
        linear_side = "Buy" if delta > 0 else "Sell"
        target = abs(delta)
        linear_qty = floor_to_step(target, spec.linear_qty_step)
        if linear_qty < spec.linear_min_qty:
            linear_skip = (
                f"qty {format_decimal(linear_qty)} < min "
                f"{format_decimal(spec.linear_min_qty)}: dust below threshold"
            )
            linear_qty = ZERO
            linear_side = None
        elif linear_reduce_only and linear_side == "Sell" and pos <= 0:
            linear_skip = f"reduceOnly Sell needs pos>0, have {format_decimal(pos)}"
            linear_qty = ZERO
            linear_side = None
        elif linear_reduce_only and linear_side == "Buy" and pos >= 0:
            linear_skip = f"reduceOnly Buy needs pos<0, have {format_decimal(pos)}"
            linear_qty = ZERO
            linear_side = None

    if mode == "clear":
        if borrow_after > 0:
            owed = borrow_after + interest
            buyback_amt = ceil_to_step(owed, spec.spot_qty_step)
            if buyback_amt < spec.spot_min_qty:
                buyback_skip = (
                    f"buyback qty {format_decimal(buyback_amt)} < min "
                    f"{format_decimal(spec.spot_min_qty)} (owed={format_decimal(owed)})"
                )
                buyback_amt = ZERO
        elif free_after > 0:
            selldown_amt = floor_to_step(free_after, spec.spot_qty_step)
            if selldown_amt < spec.spot_min_qty:
                selldown_skip = (
                    f"selldown qty {format_decimal(selldown_amt)} < min "
                    f"{format_decimal(spec.spot_min_qty)} (free_after={format_decimal(free_after)})"
                )
                selldown_amt = ZERO

    return SymbolPlan(
        state=state,
        repay_amt=repay_amt,
        free_after=free_after,
        borrow_after=borrow_after,
        net_qty=net_qty,
        linear_side=linear_side,
        linear_qty=linear_qty,
        linear_reduce_only=linear_reduce_only,
        linear_skip_reason=linear_skip,
        buyback_amt=buyback_amt,
        buyback_skip_reason=buyback_skip,
        selldown_amt=selldown_amt,
        selldown_skip_reason=selldown_skip,
    )


# -------------------- printing --------------------


def print_plan(env_name, mode, plans: List[SymbolPlan], execute: bool) -> None:
    print(f"[info] env={env_name} mode={mode} execute={execute}")
    print()
    header = (
        f"{'Symbol':<12} {'Asset':<6} {'Wallet':>14} {'Borrowed':>14} {'Interest':>10} "
        f"{'Pos':>14} {'Repay':>12} {'Net':>12} {'Lin Side':>9} {'Lin Qty':>14} "
        f"{'Lin RO':>6} {'Buyback':>12} {'Selldown':>12} Notes"
    )
    print(header)
    print("-" * len(header))
    for p in plans:
        s = p.state
        notes = []
        if p.linear_skip_reason:
            notes.append(f"linear_skip: {p.linear_skip_reason}")
        if p.buyback_skip_reason:
            notes.append(f"buyback_skip: {p.buyback_skip_reason}")
        if p.selldown_skip_reason:
            notes.append(f"selldown_skip: {p.selldown_skip_reason}")
        if mode != "clear" and p.borrow_after > 0:
            notes.append(f"leftover_borrow={format_decimal(p.borrow_after)}")
        print(
            f"{s.spec.symbol:<12} {s.spec.asset:<6} "
            f"{format_decimal(s.free):>14} "
            f"{format_decimal(s.borrowed):>14} "
            f"{format_decimal(s.interest):>10} "
            f"{format_decimal(s.linear_position):>14} "
            f"{format_decimal(p.repay_amt):>12} "
            f"{format_decimal(p.net_qty):>12} "
            f"{(p.linear_side or '-'):>9} "
            f"{format_decimal(p.linear_qty):>14} "
            f"{str(p.linear_reduce_only).lower():>6} "
            f"{format_decimal(p.buyback_amt):>12} "
            f"{format_decimal(p.selldown_amt):>12} "
            f"{'; '.join(notes)}"
        )
    print("-" * len(header))


def print_report(env_name, mode, results: List[SymbolResult]) -> int:
    print()
    print(f"[report] env={env_name} mode={mode}")
    print()
    header = (
        f"{'Symbol':<12} {'Repay':>8} {'Linear':>8} {'Buyback':>8} {'PostRepay':>10} {'Selldown':>10} Details"
    )
    print(header)
    print("-" * len(header))
    failures = 0
    for r in results:
        details = []
        for label, outcome in (
            ("repay", r.repay),
            ("linear", r.linear),
            ("buyback", r.buyback),
            ("post_buyback_repay", r.post_buyback_repay),
            ("selldown", r.selldown),
        ):
            if outcome.ok is False and outcome.err:
                details.append(f"{label}: {outcome.err}")
                failures += 1

        def tag(o: PhaseOutcome) -> str:
            if o.ok is None:
                return "-"
            return "OK" if o.ok else "ERR"

        print(
            f"{r.plan.state.spec.symbol:<12} "
            f"{tag(r.repay):>8} {tag(r.linear):>8} {tag(r.buyback):>8} "
            f"{tag(r.post_buyback_repay):>10} {tag(r.selldown):>10} "
            f"{'; '.join(details)}"
        )
    print("-" * len(header))
    return failures


# -------------------- execution --------------------


def submit_no_convert_repay(api_key, api_secret, coin: str) -> PhaseOutcome:
    body = {"coin": coin, "repaymentType": "FLEXIBLE"}
    print(f"\n[repay] {coin} repaymentType=FLEXIBLE (no-convert)")
    status, resp = bybit_private(
        "POST", "/v5/account/no-convert-repay", api_key, api_secret, body=body,
    )
    ok_http = 200 <= status < 300
    ok_ret, brief = bybit_ok(resp)
    ok = ok_http and ok_ret
    print(f"  [{'OK' if ok else 'ERR'}] status={status} {brief}")
    print(f"  {resp}")
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} {brief}"[:200])


def execute_repay(plan: SymbolPlan, api_key, api_secret) -> PhaseOutcome:
    if plan.repay_amt <= 0:
        return PhaseOutcome(ok=None)
    # no-convert-repay doesn't take amount — repays whatever is possible against the borrow
    return submit_no_convert_repay(api_key, api_secret, plan.state.spec.asset)


def execute_linear(plan: SymbolPlan, api_key, api_secret) -> PhaseOutcome:
    if plan.linear_skip_reason or plan.linear_qty <= 0 or not plan.linear_side:
        return PhaseOutcome(ok=None)
    sym = plan.state.spec.symbol
    qty = format_decimal(plan.linear_qty)
    body = {
        "category": "linear",
        "symbol": sym,
        "side": plan.linear_side,
        "orderType": "Market",
        "qty": qty,
        "reduceOnly": plan.linear_reduce_only,
        "timeInForce": "IOC",
    }
    print(f"\n[linear] {sym} {plan.linear_side} qty={qty} reduceOnly={str(plan.linear_reduce_only).lower()}")
    status, resp = bybit_private("POST", "/v5/order/create", api_key, api_secret, body=body)
    ok_http = 200 <= status < 300
    ok_ret, brief = bybit_ok(resp)
    ok = ok_http and ok_ret
    print(f"  [{'OK' if ok else 'ERR'}] status={status} {brief}")
    print(f"  {resp}")
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} {brief}"[:200])


def execute_buyback(plan: SymbolPlan, api_key, api_secret) -> PhaseOutcome:
    if plan.buyback_skip_reason or plan.buyback_amt <= 0:
        return PhaseOutcome(ok=None)
    sym = plan.state.spec.symbol
    qty = format_decimal(plan.buyback_amt)
    body = {
        "category": "spot",
        "symbol": sym,
        "side": "Buy",
        "orderType": "Market",
        "qty": qty,
        "marketUnit": "baseCoin",
    }
    print(f"\n[buyback] {sym} Buy qty={qty} (base) category=spot")
    status, resp = bybit_private("POST", "/v5/order/create", api_key, api_secret, body=body)
    ok_http = 200 <= status < 300
    ok_ret, brief = bybit_ok(resp)
    ok = ok_http and ok_ret
    print(f"  [{'OK' if ok else 'ERR'}] status={status} {brief}")
    print(f"  {resp}")
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} {brief}"[:200])


def execute_selldown(plan: SymbolPlan, api_key, api_secret) -> PhaseOutcome:
    if plan.selldown_skip_reason or plan.selldown_amt <= 0:
        return PhaseOutcome(ok=None)
    sym = plan.state.spec.symbol
    qty = format_decimal(plan.selldown_amt)
    body = {
        "category": "spot",
        "symbol": sym,
        "side": "Sell",
        "orderType": "Market",
        "qty": qty,
        "marketUnit": "baseCoin",
    }
    print(f"\n[selldown] {sym} Sell qty={qty} (base) category=spot")
    status, resp = bybit_private("POST", "/v5/order/create", api_key, api_secret, body=body)
    ok_http = 200 <= status < 300
    ok_ret, brief = bybit_ok(resp)
    ok = ok_http and ok_ret
    print(f"  [{'OK' if ok else 'ERR'}] status={status} {brief}")
    print(f"  {resp}")
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} {brief}"[:200])


# -------------------- main --------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Flatten Bybit UTA positions (repay + linear close + buyback/selldown)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--symbols", required=True,
                        help="Comma-separated symbol list (BTCUSDT,...); USDT-quoted only")
    parser.add_argument("--mode", choices=["align", "clear"], default="align")
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    env_name = check_env_safety()
    auto_source_env_sh()
    api_key, api_secret = load_credentials()
    symbols = parse_symbols(args.symbols)

    specs = fetch_specs(symbols)
    balances = fetch_wallet(api_key, api_secret)
    positions = fetch_linear_positions(symbols, api_key, api_secret)

    plans: List[SymbolPlan] = []
    for sym in symbols:
        spec = specs[sym]
        free, borrowed, interest = balances.get(spec.asset, (ZERO, ZERO, ZERO))
        pos = positions.get(sym, ZERO)
        state = SymbolState(
            spec=spec, free=free, borrowed=borrowed, interest=interest,
            linear_position=pos,
        )
        plans.append(plan_symbol(state, args.mode))

    print_plan(env_name, args.mode, plans, args.execute)
    if not args.execute:
        print("\nDry-run. Pass --execute to actually run.")
        return

    print("\n" + "=" * 60)
    print("EXECUTING")
    print("=" * 60)

    results_by_symbol: Dict[str, SymbolResult] = {
        p.state.spec.symbol: SymbolResult(plan=p) for p in plans
    }

    print("\n--- Phase R: no-convert-repay ---")
    for p in plans:
        r = results_by_symbol[p.state.spec.symbol]
        r.repay = execute_repay(p, api_key, api_secret)

    print("\n--- Phase U: linear align/close ---")
    for p in plans:
        r = results_by_symbol[p.state.spec.symbol]
        r.linear = execute_linear(p, api_key, api_secret)

    if args.mode == "clear":
        print("\n--- Phase B: spot buyback + explicit post-buyback no-convert-repay ---")
        for p in plans:
            r = results_by_symbol[p.state.spec.symbol]
            r.buyback = execute_buyback(p, api_key, api_secret)
            # Per memory: buyback updates walletBalance but borrowAmount/LTV/IM stay
            # unless we explicitly call no-convert-repay.
            if r.buyback.ok and p.buyback_amt > 0:
                r.post_buyback_repay = submit_no_convert_repay(
                    api_key, api_secret, p.state.spec.asset,
                )

        print("\n--- Phase S: spot selldown ---")
        for p in plans:
            r = results_by_symbol[p.state.spec.symbol]
            r.selldown = execute_selldown(p, api_key, api_secret)

    failures = print_report(env_name, args.mode, list(results_by_symbol.values()))
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
