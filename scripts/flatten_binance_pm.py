#!/usr/bin/env python3
"""Flatten Binance Portfolio Margin positions (FR arb cleanup).

Self-contained: no imports from other scripts in this repo.

Three-phase pipeline per symbol:
  Phase R — repay margin borrow (always; max repayable = min(free, borrowed))
  Phase U — UM MARKET align/close
  Phase D — (clear mode only) collect to margin pool + dust-convert to BNB

Modes:
  align (default) — make net_qty = margin_free + um_position - margin_borrowed = 0;
                    UM order is allowed to add/flip to match margin exposure
  clear           — close UM position to 0 with reduceOnly; convert remaining asset → BNB

Behavior:
  - Must run from an env dir whose basename matches ^binance_fr_ (CWD safety check).
  - Auto-sources ./env.sh from CWD if present; values already in process env take
    precedence. BINANCE_API_KEY / BINANCE_API_SECRET are then read from os.environ.
  - Dry-run by default. --execute is required to send state-changing requests.

Usage:
  python3 scripts/flatten_binance_pm.py --symbols BTCUSDT,ETHUSDT
  python3 scripts/flatten_binance_pm.py --symbols BTCUSDT --execute
  python3 scripts/flatten_binance_pm.py --symbols BTCUSDT --mode clear --execute
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


BINANCE_PAPI_BASE = os.environ.get("BINANCE_PAPI_URL", "https://papi.binance.com")
BINANCE_FAPI_BASE = os.environ.get("BINANCE_FAPI_URL", "https://fapi.binance.com")
BINANCE_SAPI_BASE = os.environ.get("BINANCE_SAPI_URL", "https://api.binance.com")

PAPI_BALANCE_PATH = "/papi/v1/balance"
PAPI_UM_POSITION_PATH = "/papi/v1/um/positionRisk"
PAPI_REPAY_PATH = "/papi/v1/repayLoan"
PAPI_UM_ORDER_PATH = "/papi/v1/um/order"
PAPI_MARGIN_ORDER_PATH = "/papi/v1/margin/order"
PAPI_ASSET_COLLECTION_PATH = "/papi/v1/asset-collection"
SAPI_DUST_PATH = "/sapi/v1/asset/dust"
FAPI_EXCHANGE_INFO_URL = f"{BINANCE_FAPI_BASE}/fapi/v1/exchangeInfo"

ENV_DIR_PATTERN = re.compile(r"^binance_fr_")
ZERO = Decimal("0")


# -------------------- data classes --------------------


@dataclass
class SymbolSpec:
    symbol: str
    asset: str
    step_size: Decimal
    min_qty: Decimal
    min_notional: Decimal


@dataclass
class SymbolState:
    spec: SymbolSpec
    margin_free: Decimal
    margin_borrowed: Decimal
    margin_interest: Decimal
    um_position: Decimal


@dataclass
class SymbolPlan:
    state: SymbolState
    repay_amt: Decimal
    free_after: Decimal
    borrow_after: Decimal
    net_qty: Decimal
    um_side: Optional[str]
    um_qty: Decimal
    um_reduce_only: bool
    um_skip_reason: Optional[str]
    buyback_amt: Decimal
    buyback_skip_reason: Optional[str]
    selldown_amt: Decimal
    selldown_skip_reason: Optional[str]
    dust_amt: Decimal
    dust_skip_reason: Optional[str]


@dataclass
class PhaseOutcome:
    ok: Optional[bool] = None  # None = not attempted
    err: str = ""


@dataclass
class SymbolResult:
    plan: SymbolPlan
    repay: PhaseOutcome = field(default_factory=PhaseOutcome)
    um: PhaseOutcome = field(default_factory=PhaseOutcome)
    buyback: PhaseOutcome = field(default_factory=PhaseOutcome)
    selldown: PhaseOutcome = field(default_factory=PhaseOutcome)
    asset_collect: PhaseOutcome = field(default_factory=PhaseOutcome)
    dust: PhaseOutcome = field(default_factory=PhaseOutcome)


# -------------------- helpers --------------------


def now_ms() -> int:
    return int(time.time() * 1000)


def decimal_or(value: Any, default: str = "0") -> Decimal:
    if value in (None, ""):
        return Decimal(default)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal(default)


def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_UP) * step


def format_decimal(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def http_request(
    url: str,
    *,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 15,
) -> Tuple[int, str]:
    req = urllib.request.Request(url, method=method.upper())
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001
        return 0, str(exc)


def binance_sign(query: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def signed_request(
    base: str,
    path: str,
    params: Dict[str, Any],
    api_key: str,
    api_secret: str,
    *,
    method: str = "GET",
    timeout: int = 15,
) -> Tuple[int, str]:
    payload = dict(params)
    payload.setdefault("recvWindow", "5000")
    payload["timestamp"] = str(now_ms())
    query = urllib.parse.urlencode(sorted(payload.items(), key=lambda kv: kv[0]), safe="-_.~")
    signature = binance_sign(query, api_secret)
    url = f"{base}{path}?{query}&signature={signature}"
    return http_request(url, method=method, headers={"X-MBX-APIKEY": api_key}, timeout=timeout)


def public_json(url: str, *, timeout: int = 15) -> Any:
    status, body = http_request(url, timeout=timeout)
    if not (200 <= status < 300):
        sys.exit(f"[ERROR] public request failed: url={url} status={status} body={body}")
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        sys.exit(f"[ERROR] invalid json from {url}: {exc}")


def check_env_safety() -> str:
    cwd_name = os.path.basename(os.path.normpath(os.getcwd()))
    if not ENV_DIR_PATTERN.match(cwd_name):
        sys.stderr.write(
            f"[ERROR] CWD basename must match ^binance_fr_, got {cwd_name!r} "
            f"(CWD={os.getcwd()}). Aborting for safety.\n"
        )
        sys.exit(2)
    return cwd_name


AUTHORITATIVE_KEYS = ("BINANCE_API_KEY", "BINANCE_API_SECRET")


def auto_source_env_sh() -> None:
    """Source ./env.sh into os.environ if it exists.

    For BINANCE_API_KEY / BINANCE_API_SECRET, env.sh always wins (overrides any
    pre-existing process env) — running in the wrong account silently is a much
    worse failure than ignoring an intentional manual override. A warning is
    printed when env.sh actually overrides a different pre-existing value.

    For every other key, only fills if not already present in os.environ.
    """
    env_path = os.path.join(os.getcwd(), "env.sh")
    if not os.path.isfile(env_path):
        return
    proc = subprocess.run(
        ["bash", "-lc", f"set -a; source {env_path} >/dev/null 2>&1; env -0"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
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
            old_value = os.environ.get(key)
            if old_value and old_value != new_value:
                sys.stderr.write(
                    f"[WARN] env.sh overrides existing {key} from process env "
                    f"(was set by calling shell; env.sh value wins to prevent cross-account ops)\n"
                )
            os.environ[key] = new_value
        else:
            if key in os.environ:
                continue
            os.environ[key] = new_value


def load_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        sys.stderr.write(
            "[ERROR] missing BINANCE_API_KEY / BINANCE_API_SECRET "
            "(checked process env and ./env.sh).\n"
        )
        sys.exit(2)
    return api_key, api_secret


def parse_symbols(raw: str) -> List[str]:
    out: List[str] = []
    for tok in raw.split(","):
        sym = tok.strip().upper()
        if sym:
            out.append(sym)
    if not out:
        sys.exit("[ERROR] --symbols produced empty list after parsing")
    return out


def asset_from_symbol(symbol: str) -> str:
    if symbol.endswith("USDT"):
        return symbol[:-4]
    sys.exit(f"[ERROR] only USDT-quoted symbols supported, got {symbol!r}")


# -------------------- state fetch --------------------


def fetch_balance(
    api_key: str, api_secret: str
) -> Dict[str, Tuple[Decimal, Decimal, Decimal]]:
    """Return {asset: (free, borrowed, interest)} from PAPI balance."""
    status, body = signed_request(
        BINANCE_PAPI_BASE, PAPI_BALANCE_PATH, {}, api_key, api_secret, method="GET"
    )
    if not (200 <= status < 300):
        sys.exit(f"[ERROR] {PAPI_BALANCE_PATH} status={status} body={body}")
    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        sys.exit(f"[ERROR] {PAPI_BALANCE_PATH} non-JSON: {exc}")
    if not isinstance(data, list):
        sys.exit(f"[ERROR] {PAPI_BALANCE_PATH} expected list, got {type(data).__name__}")
    out: Dict[str, Tuple[Decimal, Decimal, Decimal]] = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        asset = str(item.get("asset", "")).strip().upper()
        if not asset:
            continue
        free = decimal_or(item.get("crossMarginFree"))
        borrowed = decimal_or(item.get("crossMarginBorrowed"))
        interest = decimal_or(item.get("crossMarginInterest"))
        out[asset] = (free, borrowed, interest)
    return out


def fetch_um_positions(symbols: List[str], api_key: str, api_secret: str) -> Dict[str, Decimal]:
    out: Dict[str, Decimal] = {sym: ZERO for sym in symbols}
    for sym in symbols:
        status, body = signed_request(
            BINANCE_PAPI_BASE,
            PAPI_UM_POSITION_PATH,
            {"symbol": sym},
            api_key,
            api_secret,
            method="GET",
        )
        if not (200 <= status < 300):
            sys.stderr.write(f"[WARN] {PAPI_UM_POSITION_PATH} {sym} status={status} body={body}\n")
            continue
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            sys.stderr.write(f"[WARN] {PAPI_UM_POSITION_PATH} {sym} non-JSON: {body}\n")
            continue
        rows = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
        amt = ZERO
        for row in rows:
            if not isinstance(row, dict):
                continue
            if str(row.get("symbol", "")).strip().upper() != sym:
                continue
            amt += decimal_or(row.get("positionAmt"))
        out[sym] = amt
    return out


def fetch_specs(symbols: List[str]) -> Dict[str, SymbolSpec]:
    data = public_json(FAPI_EXCHANGE_INFO_URL)
    out: Dict[str, SymbolSpec] = {}
    wanted = set(symbols)
    for item in data.get("symbols", []):
        sym = str(item.get("symbol", "")).strip().upper()
        if sym not in wanted:
            continue
        step_size = Decimal("1")
        min_qty = ZERO
        min_notional = ZERO
        for flt in item.get("filters", []):
            ftype = flt.get("filterType")
            if ftype == "LOT_SIZE":
                step_size = decimal_or(flt.get("stepSize"), "1")
                min_qty = decimal_or(flt.get("minQty"))
            elif ftype == "MIN_NOTIONAL":
                min_notional = decimal_or(flt.get("notional"))
        out[sym] = SymbolSpec(
            symbol=sym,
            asset=asset_from_symbol(sym),
            step_size=step_size,
            min_qty=min_qty,
            min_notional=min_notional,
        )
    return out


# -------------------- planning --------------------


def plan_symbol(state: SymbolState, mode: str) -> SymbolPlan:
    spec = state.spec
    free = state.margin_free
    borrowed = state.margin_borrowed
    interest = state.margin_interest
    pos = state.um_position

    repay_amt = min(free, borrowed) if (free > 0 and borrowed > 0) else ZERO
    free_after = free - repay_amt
    borrow_after = borrowed - repay_amt
    net_qty = free_after - borrow_after + pos

    um_side: Optional[str] = None
    um_qty = ZERO
    um_reduce_only = mode == "clear"
    um_skip: Optional[str] = None
    buyback_amt = ZERO
    buyback_skip: Optional[str] = None
    selldown_amt = ZERO
    selldown_skip: Optional[str] = None
    dust_amt = ZERO
    dust_skip: Optional[str] = None

    if mode == "align":
        delta = -net_qty
    else:  # clear
        delta = -pos

    if delta == 0:
        um_skip = "no UM action needed (target delta is zero)"
    else:
        um_side = "BUY" if delta > 0 else "SELL"
        target_qty = abs(delta)
        um_qty = floor_to_step(target_qty, spec.step_size)
        if um_qty < spec.min_qty:
            um_skip = (
                f"qty {format_decimal(target_qty)} -> {format_decimal(um_qty)} < minQty "
                f"{format_decimal(spec.min_qty)}: dust below API closing threshold"
            )
            um_qty = ZERO
            um_side = None
        elif um_reduce_only and um_side == "SELL" and pos <= 0:
            um_skip = f"reduceOnly SELL needs pos>0, have pos={format_decimal(pos)}"
            um_qty = ZERO
            um_side = None
        elif um_reduce_only and um_side == "BUY" and pos >= 0:
            um_skip = f"reduceOnly BUY needs pos<0, have pos={format_decimal(pos)}"
            um_qty = ZERO
            um_side = None

    # In clear mode, after Phase R one of (free_after, borrow_after) is always 0.
    # - borrow_after > 0 → Phase B: BUY (AUTO_BORROW_REPAY) to extinguish debt
    # - free_after   > 0 → Phase S: SELL the free asset for USDT (NO_SIDE_EFFECT)
    # Residual after either step (rounding remainder) is left for Phase D dust convert.
    expected_residual = free_after
    if mode == "clear":
        if borrow_after > 0:
            owed = borrow_after + interest
            buyback_amt = ceil_to_step(owed, spec.step_size)
            if buyback_amt < spec.min_qty:
                buyback_skip = (
                    f"buyback qty {format_decimal(buyback_amt)} < minQty "
                    f"{format_decimal(spec.min_qty)} (owed={format_decimal(owed)}); "
                    f"borrow leftover requires manual cleanup"
                )
                buyback_amt = ZERO
            else:
                # post-buyback excess goes to free, which dust step picks up
                expected_residual = free_after + (buyback_amt - owed)
        elif free_after > 0:
            selldown_amt = floor_to_step(free_after, spec.step_size)
            if selldown_amt < spec.min_qty:
                selldown_skip = (
                    f"selldown qty {format_decimal(selldown_amt)} < minQty "
                    f"{format_decimal(spec.min_qty)} (free_after={format_decimal(free_after)}); "
                    f"residual goes straight to dust step"
                )
                selldown_amt = ZERO
                # residual to dust = entire free_after
            else:
                # post-selldown residual (rounding remainder) goes to dust
                expected_residual = free_after - selldown_amt

        if expected_residual > 0:
            dust_amt = expected_residual
        else:
            dust_skip = (
                f"no expected residual to dust-convert "
                f"(expected_residual={format_decimal(expected_residual)})"
            )

    return SymbolPlan(
        state=state,
        repay_amt=repay_amt,
        free_after=free_after,
        borrow_after=borrow_after,
        net_qty=net_qty,
        um_side=um_side,
        um_qty=um_qty,
        um_reduce_only=um_reduce_only,
        um_skip_reason=um_skip,
        buyback_amt=buyback_amt,
        buyback_skip_reason=buyback_skip,
        selldown_amt=selldown_amt,
        selldown_skip_reason=selldown_skip,
        dust_amt=dust_amt,
        dust_skip_reason=dust_skip,
    )


# -------------------- printing --------------------


def print_plan(env_name: str, mode: str, plans: List[SymbolPlan], execute: bool) -> None:
    print(f"[info] env={env_name} mode={mode} execute={execute}")
    print()
    header = (
        f"{'Symbol':<12} {'Asset':<6} {'Free':>14} {'Borrowed':>14} {'Interest':>10} "
        f"{'Pos':>14} {'Repay':>12} {'Net':>12} {'UM Side':>8} {'UM Qty':>14} "
        f"{'UM RO':>5} {'Buyback':>12} {'Selldown':>12} {'Dust':>12} Notes"
    )
    print(header)
    print("-" * len(header))
    for p in plans:
        s = p.state
        notes_parts: List[str] = []
        if p.um_skip_reason:
            notes_parts.append(f"um_skip: {p.um_skip_reason}")
        if p.buyback_skip_reason:
            notes_parts.append(f"buyback_skip: {p.buyback_skip_reason}")
        if p.selldown_skip_reason:
            notes_parts.append(f"selldown_skip: {p.selldown_skip_reason}")
        if p.dust_skip_reason:
            notes_parts.append(f"dust_skip: {p.dust_skip_reason}")
        if mode != "clear" and p.borrow_after > 0:
            notes_parts.append(f"leftover_borrow={format_decimal(p.borrow_after)}")
        notes = "; ".join(notes_parts)
        print(
            f"{s.spec.symbol:<12} {s.spec.asset:<6} "
            f"{format_decimal(s.margin_free):>14} "
            f"{format_decimal(s.margin_borrowed):>14} "
            f"{format_decimal(s.margin_interest):>10} "
            f"{format_decimal(s.um_position):>14} "
            f"{format_decimal(p.repay_amt):>12} "
            f"{format_decimal(p.net_qty):>12} "
            f"{(p.um_side or '-'):>8} "
            f"{format_decimal(p.um_qty):>14} "
            f"{str(p.um_reduce_only).lower():>5} "
            f"{format_decimal(p.buyback_amt):>12} "
            f"{format_decimal(p.selldown_amt):>12} "
            f"{format_decimal(p.dust_amt):>12} "
            f"{notes}"
        )
    print("-" * len(header))


def print_report(env_name: str, mode: str, results: List[SymbolResult]) -> int:
    print()
    print(f"[report] env={env_name} mode={mode}")
    print()
    header = (
        f"{'Symbol':<12} {'Repay':>8} {'UM':>8} {'Buyback':>8} {'Selldown':>8} "
        f"{'Collect':>8} {'Dust':>8} Details"
    )
    print(header)
    print("-" * len(header))
    failures = 0
    for r in results:
        details: List[str] = []
        for label, outcome in (
            ("repay", r.repay),
            ("um", r.um),
            ("buyback", r.buyback),
            ("selldown", r.selldown),
            ("collect", r.asset_collect),
            ("dust", r.dust),
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
            f"{tag(r.repay):>8} {tag(r.um):>8} {tag(r.buyback):>8} "
            f"{tag(r.selldown):>8} {tag(r.asset_collect):>8} {tag(r.dust):>8} "
            f"{'; '.join(details)}"
        )
    print("-" * len(header))
    return failures


# -------------------- execution --------------------


def execute_repay(
    plan: SymbolPlan, api_key: str, api_secret: str, *, dry_run: bool = False
) -> PhaseOutcome:
    if plan.repay_amt <= 0:
        return PhaseOutcome(ok=None)
    if dry_run:
        return PhaseOutcome(ok=None)
    asset = plan.state.spec.asset
    amount = format_decimal(plan.repay_amt)
    print(f"\n[repay] asset={asset} amount={amount}")
    status, body = signed_request(
        BINANCE_PAPI_BASE,
        PAPI_REPAY_PATH,
        {"asset": asset, "amount": amount},
        api_key,
        api_secret,
        method="POST",
    )
    ok = 200 <= status < 300
    tag = "OK" if ok else "ERR"
    print(f"  [{tag}] status={status}")
    print(f"  {body}")
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} body={body[:200]}")


def execute_um(
    plan: SymbolPlan, api_key: str, api_secret: str, *, dry_run: bool = False
) -> PhaseOutcome:
    if plan.um_skip_reason or plan.um_qty <= 0 or not plan.um_side:
        return PhaseOutcome(ok=None)
    if dry_run:
        return PhaseOutcome(ok=None)
    sym = plan.state.spec.symbol
    qty = format_decimal(plan.um_qty)
    print(f"\n[um] {sym} {plan.um_side} qty={qty} reduceOnly={str(plan.um_reduce_only).lower()}")
    params: Dict[str, Any] = {
        "symbol": sym,
        "side": plan.um_side,
        "type": "MARKET",
        "quantity": qty,
    }
    if plan.um_reduce_only:
        params["reduceOnly"] = "true"
    status, body = signed_request(
        BINANCE_PAPI_BASE,
        PAPI_UM_ORDER_PATH,
        params,
        api_key,
        api_secret,
        method="POST",
    )
    ok = 200 <= status < 300
    tag = "OK" if ok else "ERR"
    print(f"  [{tag}] status={status}")
    print(f"  {body}")
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} body={body[:200]}")


def execute_buyback(
    plan: SymbolPlan, api_key: str, api_secret: str, *, dry_run: bool = False
) -> PhaseOutcome:
    if plan.buyback_skip_reason or plan.buyback_amt <= 0:
        return PhaseOutcome(ok=None)
    if dry_run:
        return PhaseOutcome(ok=None)
    sym = plan.state.spec.symbol
    qty = format_decimal(plan.buyback_amt)
    print(f"\n[buyback] {sym} BUY qty={qty} sideEffect=AUTO_BORROW_REPAY")
    status, body = signed_request(
        BINANCE_PAPI_BASE,
        PAPI_MARGIN_ORDER_PATH,
        {
            "symbol": sym,
            "side": "BUY",
            "type": "MARKET",
            "quantity": qty,
            "sideEffect": "AUTO_BORROW_REPAY",
        },
        api_key,
        api_secret,
        method="POST",
    )
    ok = 200 <= status < 300
    tag = "OK" if ok else "ERR"
    print(f"  [{tag}] status={status}")
    print(f"  {body}")
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} body={body[:200]}")


def execute_selldown(
    plan: SymbolPlan, api_key: str, api_secret: str, *, dry_run: bool = False
) -> PhaseOutcome:
    if plan.selldown_skip_reason or plan.selldown_amt <= 0:
        return PhaseOutcome(ok=None)
    if dry_run:
        return PhaseOutcome(ok=None)
    sym = plan.state.spec.symbol
    qty = format_decimal(plan.selldown_amt)
    print(f"\n[selldown] {sym} SELL qty={qty} sideEffect=NO_SIDE_EFFECT")
    status, body = signed_request(
        BINANCE_PAPI_BASE,
        PAPI_MARGIN_ORDER_PATH,
        {
            "symbol": sym,
            "side": "SELL",
            "type": "MARKET",
            "quantity": qty,
            "sideEffect": "NO_SIDE_EFFECT",
        },
        api_key,
        api_secret,
        method="POST",
    )
    ok = 200 <= status < 300
    tag = "OK" if ok else "ERR"
    print(f"  [{tag}] status={status}")
    print(f"  {body}")
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} body={body[:200]}")


def execute_asset_collection(
    plan: SymbolPlan, api_key: str, api_secret: str, *, dry_run: bool = False
) -> PhaseOutcome:
    if plan.dust_skip_reason or plan.dust_amt <= 0:
        return PhaseOutcome(ok=None)
    if dry_run:
        return PhaseOutcome(ok=None)
    asset = plan.state.spec.asset
    print(f"\n[collect] asset={asset}")
    status, body = signed_request(
        BINANCE_PAPI_BASE,
        PAPI_ASSET_COLLECTION_PATH,
        {"asset": asset},
        api_key,
        api_secret,
        method="POST",
    )
    ok = 200 <= status < 300
    tag = "OK" if ok else "ERR"
    print(f"  [{tag}] status={status}")
    print(f"  {body}")
    # asset-collection commonly returns success even with zero balance, and may
    # return errors that don't block the dust step (e.g. "no UM wallet balance").
    # Don't treat as fatal — log and let dust step decide.
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} body={body[:200]}")


def execute_dust_batch(
    plans: List[SymbolPlan],
    results_by_symbol: Dict[str, SymbolResult],
    api_key: str,
    api_secret: str,
    *,
    dry_run: bool = False,
) -> None:
    assets = []
    for p in plans:
        r = results_by_symbol[p.state.spec.symbol]
        # only attempt dust for symbols where (a) clear-mode planned dust > 0,
        # (b) UM phase didn't outright fail (Phase D gated on Phase U per-symbol)
        if p.dust_skip_reason or p.dust_amt <= 0:
            continue
        if r.um.ok is False:
            r.dust = PhaseOutcome(ok=None, err="skipped: UM phase failed")
            continue
        assets.append(p.state.spec.asset)
    if not assets:
        return
    if dry_run:
        return

    # /sapi/v1/asset/dust accepts repeated `asset` params
    params: List[Tuple[str, str]] = [("asset", a) for a in assets]
    params.append(("recvWindow", "5000"))
    params.append(("timestamp", str(now_ms())))
    params_sorted = sorted(params, key=lambda kv: kv[0])
    query = urllib.parse.urlencode(params_sorted, safe="-_.~")
    signature = binance_sign(query, api_secret)
    url = f"{BINANCE_SAPI_BASE}{SAPI_DUST_PATH}?{query}&signature={signature}"
    print(f"\n[dust] assets={','.join(assets)}")
    status, body = http_request(
        url, method="POST", headers={"X-MBX-APIKEY": api_key}, timeout=20
    )
    ok = 200 <= status < 300
    # Special case: -31002 "illegal parameter" typically means no eligible balance to
    # convert (e.g. the asset just isn't in spot wallet). Classify as not-attempted
    # (warning) rather than failure so partial-completion exit code 1 isn't triggered.
    is_no_balance = False
    if not ok:
        try:
            parsed = json.loads(body)
            if str(parsed.get("code", "")) == "-31002":
                is_no_balance = True
        except json.JSONDecodeError:
            pass
    if is_no_balance:
        tag = "SKIP"
    else:
        tag = "OK" if ok else "ERR"
    print(f"  [{tag}] status={status}")
    print(f"  {body}")
    if is_no_balance:
        outcome = PhaseOutcome(ok=None, err="no eligible dust balance (-31002)")
    else:
        outcome = PhaseOutcome(ok=ok, err="" if ok else f"status={status} body={body[:200]}")
    # Batch result applies to every asset in the call
    for p in plans:
        r = results_by_symbol[p.state.spec.symbol]
        if p.state.spec.asset in assets:
            r.dust = outcome


# -------------------- main --------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Flatten Binance PM positions (repay + UM close + optional dust→BNB)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--symbols",
        required=True,
        help="Comma-separated symbol list (e.g. BTCUSDT,ETHUSDT); USDT-quoted only",
    )
    parser.add_argument(
        "--mode",
        choices=["align", "clear"],
        default="align",
        help="align: zero net_qty via UM; clear: close UM + dust→BNB",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually submit requests; default is dry-run",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    env_name = check_env_safety()
    auto_source_env_sh()
    api_key, api_secret = load_credentials()
    symbols = parse_symbols(args.symbols)

    specs = fetch_specs(symbols)
    missing = [s for s in symbols if s not in specs]
    if missing:
        sys.exit(f"[ERROR] symbols not in fapi exchangeInfo: {missing}")

    balances = fetch_balance(api_key, api_secret)
    positions = fetch_um_positions(symbols, api_key, api_secret)

    plans: List[SymbolPlan] = []
    for sym in symbols:
        spec = specs[sym]
        free, borrowed, interest = balances.get(spec.asset, (ZERO, ZERO, ZERO))
        pos = positions.get(sym, ZERO)
        state = SymbolState(
            spec=spec,
            margin_free=free,
            margin_borrowed=borrowed,
            margin_interest=interest,
            um_position=pos,
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

    # Phase R: repay (best-effort, continue-on-error)
    print("\n--- Phase R: repay ---")
    for p in plans:
        r = results_by_symbol[p.state.spec.symbol]
        r.repay = execute_repay(p, api_key, api_secret)

    # Phase U: UM align/close (best-effort, continue-on-error)
    print("\n--- Phase U: UM align/close ---")
    for p in plans:
        r = results_by_symbol[p.state.spec.symbol]
        r.um = execute_um(p, api_key, api_secret)

    # Phase B (clear-mode only): buyback margin to cover remaining borrow
    if args.mode == "clear":
        print("\n--- Phase B: buyback margin (AUTO_BORROW_REPAY) ---")
        for p in plans:
            r = results_by_symbol[p.state.spec.symbol]
            r.buyback = execute_buyback(p, api_key, api_secret)

    # Phase S (clear-mode only): sell free asset down to USDT
    if args.mode == "clear":
        print("\n--- Phase S: selldown margin (NO_SIDE_EFFECT) ---")
        for p in plans:
            r = results_by_symbol[p.state.spec.symbol]
            r.selldown = execute_selldown(p, api_key, api_secret)

    # Phase D: dust convert (clear mode only)
    if args.mode == "clear":
        print("\n--- Phase D: asset-collection + dust→BNB ---")
        for p in plans:
            r = results_by_symbol[p.state.spec.symbol]
            # Gate: only attempt collect if UM phase didn't outright fail for this symbol
            if r.um.ok is False:
                r.asset_collect = PhaseOutcome(ok=None, err="skipped: UM phase failed")
                continue
            r.asset_collect = execute_asset_collection(p, api_key, api_secret)
        execute_dust_batch(plans, results_by_symbol, api_key, api_secret)

    failures = print_report(env_name, args.mode, list(results_by_symbol.values()))
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
