#!/usr/bin/env python3
"""Flatten OKEx Unified Account positions (FR arb cleanup).

Self-contained: no imports from other scripts in this repo.

Four-phase pipeline per symbol (no dust convert):
  Phase R — explicit repay via /api/v5/account/borrow-repay
  Phase U — SWAP reduce-only MARKET close (tdMode=cross)
  Phase B — clear-mode only — BUY spot (tdMode=cross) to extinguish borrow
  Phase S — clear-mode only — SELL spot for USDT (free>borrow case)

Modes:
  align (default) — net_qty = free + swap_position - borrowed = 0; SWAP reduce-only
  clear           — close SWAP to 0; then B or S to drain asset

Behavior:
  - CWD basename must match ^okex_fr_ or ^okex-intra- (safety guard).
  - Auto-sources ./env.sh; OKX_API_KEY/SECRET/PASSPHRASE — env.sh always wins.
  - Dry-run by default. --execute required for state changes.

Usage:
  python3 scripts/flatten_okex_pm.py --symbols BTCUSDT,ETHUSDT
  python3 scripts/flatten_okex_pm.py --symbols BTCUSDT --mode clear --execute
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
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
from typing import Any, Dict, List, Optional, Tuple


OKX_BASE = os.environ.get("OKX_BASE_URL", "https://www.okx.com").rstrip("/")

OKX_BALANCE_PATH = "/api/v5/account/balance"
OKX_POSITIONS_PATH = "/api/v5/account/positions"
OKX_BORROW_REPAY_PATH = "/api/v5/account/borrow-repay"
OKX_ORDER_PATH = "/api/v5/trade/order"
OKX_INSTRUMENTS_PUBLIC = "/api/v5/public/instruments"

ENV_DIR_PATTERN = re.compile(r"^(okex_fr_|okex[-_]intra[-_])")
AUTHORITATIVE_KEYS = ("OKX_API_KEY", "OKX_API_SECRET", "OKX_PASSPHRASE")
ZERO = Decimal("0")


@dataclass
class SymbolSpec:
    symbol: str           # caller form, e.g. BTCUSDT
    asset: str            # BTC
    spot_inst: str        # BTC-USDT
    swap_inst: str        # BTC-USDT-SWAP
    spot_lot: Decimal     # spot lotSz (in base coin)
    spot_min_sz: Decimal
    swap_lot: Decimal     # SWAP lotSz (in contracts)
    swap_min_sz: Decimal
    swap_contract_size: Decimal  # ctVal * ctMult: base coin per 1 contract


@dataclass
class SymbolState:
    spec: SymbolSpec
    free: Decimal
    borrowed: Decimal
    interest: Decimal
    swap_position_coins: Decimal  # signed, base-coin amount


@dataclass
class SymbolPlan:
    state: SymbolState
    repay_amt: Decimal
    free_after: Decimal
    borrow_after: Decimal
    net_qty: Decimal
    swap_side: Optional[str]
    swap_contracts: Decimal
    swap_skip_reason: Optional[str]
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
    swap: PhaseOutcome = field(default_factory=PhaseOutcome)
    buyback: PhaseOutcome = field(default_factory=PhaseOutcome)
    selldown: PhaseOutcome = field(default_factory=PhaseOutcome)


# -------------------- helpers --------------------


def utc_timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


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
    data: Optional[bytes] = None,
    timeout: int = 15,
) -> Tuple[int, str]:
    req = urllib.request.Request(url, data=data, method=method.upper())
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001
        return 0, str(exc)


def okx_sign(timestamp: str, method: str, request_path: str, body: str, secret: str) -> str:
    payload = f"{timestamp}{method.upper()}{request_path}{body}"
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def okx_private(
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    timeout: int = 15,
) -> Tuple[int, str]:
    method = method.upper()
    params = params or {}
    query = urllib.parse.urlencode(params) if params else ""
    request_path = f"{path}?{query}" if query else path
    body_text = "" if method == "GET" else json.dumps(body or {}, ensure_ascii=False, separators=(",", ":"))
    timestamp = utc_timestamp()
    signature = okx_sign(timestamp, method, request_path, body_text, api_secret)
    headers = {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
    }
    return http_request(
        f"{OKX_BASE}{request_path}",
        method=method,
        headers=headers,
        data=None if method == "GET" else body_text.encode("utf-8"),
        timeout=timeout,
    )


def okx_response_ok(body: str) -> Tuple[bool, str]:
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return False, "non-json"
    code = str(parsed.get("code", ""))
    if code == "0":
        return True, ""
    msg = str(parsed.get("msg", ""))
    data = parsed.get("data")
    extras = ""
    if isinstance(data, list) and data and isinstance(data[0], dict):
        s_code = str(data[0].get("sCode", ""))
        s_msg = str(data[0].get("sMsg", ""))
        if s_code or s_msg:
            extras = f" sCode={s_code} sMsg={s_msg}"
    return False, f"code={code} msg={msg}{extras}"


def check_env_safety() -> str:
    cwd_name = os.path.basename(os.path.normpath(os.getcwd()))
    if not ENV_DIR_PATTERN.match(cwd_name):
        sys.stderr.write(
            f"[ERROR] CWD basename must match ^okex_fr_ or ^okex-intra-, got {cwd_name!r} "
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


def load_credentials() -> Tuple[str, str, str]:
    k = os.environ.get("OKX_API_KEY", "").strip()
    s = os.environ.get("OKX_API_SECRET", "").strip()
    p = os.environ.get("OKX_PASSPHRASE", "").strip()
    if not k or not s or not p:
        sys.stderr.write(
            "[ERROR] missing OKX_API_KEY / OKX_API_SECRET / OKX_PASSPHRASE "
            "(checked process env and ./env.sh).\n"
        )
        sys.exit(2)
    return k, s, p


def parse_symbols(raw: str) -> List[str]:
    out: List[str] = []
    for tok in raw.split(","):
        sym = tok.strip().upper()
        if sym:
            out.append(sym)
    if not out:
        sys.exit("[ERROR] --symbols produced empty list")
    return out


def split_usdt(symbol: str) -> str:
    if symbol.endswith("USDT"):
        return symbol[:-4]
    sys.exit(f"[ERROR] only USDT-quoted symbols supported, got {symbol!r}")


# -------------------- state fetch --------------------


def fetch_specs(symbols: List[str]) -> Dict[str, SymbolSpec]:
    """Fetch SWAP + SPOT instruments and join into SymbolSpec."""
    wanted_pairs = {split_usdt(s): s for s in symbols}

    # SWAP
    status, body = http_request(
        f"{OKX_BASE}{OKX_INSTRUMENTS_PUBLIC}?instType=SWAP", timeout=15
    )
    if not (200 <= status < 300):
        sys.exit(f"[ERROR] instruments SWAP status={status} body={body}")
    swap_data = json.loads(body)
    if str(swap_data.get("code", "")) != "0":
        sys.exit(f"[ERROR] instruments SWAP: {body}")

    swap_specs: Dict[str, Dict[str, Any]] = {}
    for item in swap_data.get("data", []):
        if item.get("ctType") != "linear" or item.get("settleCcy") != "USDT":
            continue
        inst_id = str(item.get("instId", ""))  # e.g. BTC-USDT-SWAP
        if not inst_id.endswith("-USDT-SWAP"):
            continue
        asset = inst_id.replace("-USDT-SWAP", "")
        if asset not in wanted_pairs:
            continue
        swap_specs[asset] = item

    # SPOT
    status, body = http_request(
        f"{OKX_BASE}{OKX_INSTRUMENTS_PUBLIC}?instType=SPOT", timeout=15
    )
    if not (200 <= status < 300):
        sys.exit(f"[ERROR] instruments SPOT status={status} body={body}")
    spot_data = json.loads(body)
    if str(spot_data.get("code", "")) != "0":
        sys.exit(f"[ERROR] instruments SPOT: {body}")

    spot_specs: Dict[str, Dict[str, Any]] = {}
    for item in spot_data.get("data", []):
        inst_id = str(item.get("instId", ""))  # e.g. BTC-USDT
        if not inst_id.endswith("-USDT"):
            continue
        asset = inst_id.replace("-USDT", "")
        if asset not in wanted_pairs:
            continue
        spot_specs[asset] = item

    out: Dict[str, SymbolSpec] = {}
    for asset, sym in wanted_pairs.items():
        if asset not in swap_specs:
            sys.exit(f"[ERROR] SWAP instrument not found for {asset}-USDT-SWAP")
        if asset not in spot_specs:
            sys.exit(f"[ERROR] SPOT instrument not found for {asset}-USDT")
        sw = swap_specs[asset]
        sp = spot_specs[asset]
        ct_val = decimal_or(sw.get("ctVal"), "1")
        ct_mult = decimal_or(sw.get("ctMult"), "1")
        out[sym] = SymbolSpec(
            symbol=sym,
            asset=asset,
            spot_inst=f"{asset}-USDT",
            swap_inst=f"{asset}-USDT-SWAP",
            spot_lot=decimal_or(sp.get("lotSz"), "1"),
            spot_min_sz=decimal_or(sp.get("minSz"), "0"),
            swap_lot=decimal_or(sw.get("lotSz"), "1"),
            swap_min_sz=decimal_or(sw.get("minSz"), "0"),
            swap_contract_size=ct_val * ct_mult,
        )
    return out


def fetch_balance(api_key: str, api_secret: str, passphrase: str) -> Dict[str, Tuple[Decimal, Decimal, Decimal]]:
    """Return {asset: (avail_eq, liability, interest)} from OKEx unified balance."""
    status, body = okx_private("GET", OKX_BALANCE_PATH, api_key, api_secret, passphrase)
    if not (200 <= status < 300):
        sys.exit(f"[ERROR] {OKX_BALANCE_PATH} status={status} body={body}")
    parsed = json.loads(body)
    if str(parsed.get("code", "")) != "0":
        sys.exit(f"[ERROR] {OKX_BALANCE_PATH}: {body}")
    out: Dict[str, Tuple[Decimal, Decimal, Decimal]] = {}
    for account in parsed.get("data", []):
        for d in account.get("details", []):
            ccy = str(d.get("ccy", "")).upper()
            if not ccy:
                continue
            avail = decimal_or(d.get("availEq"))
            liab = decimal_or(d.get("liab"))
            interest = decimal_or(d.get("interest"))
            # `liab` in OKEx is signed: negative means borrow (debt).
            # Convert to positive "borrowed" amount.
            borrowed = -liab if liab < 0 else ZERO
            interest_pos = -interest if interest < 0 else interest
            out[ccy] = (avail, borrowed, interest_pos)
    return out


def fetch_swap_positions(
    swap_insts: List[str], api_key: str, api_secret: str, passphrase: str
) -> Dict[str, Decimal]:
    """Return {swap_inst_id: signed_contract_position}."""
    out: Dict[str, Decimal] = {inst: ZERO for inst in swap_insts}
    if not swap_insts:
        return out
    params = {"instType": "SWAP", "instId": ",".join(swap_insts)}
    status, body = okx_private("GET", OKX_POSITIONS_PATH, api_key, api_secret, passphrase, params=params)
    if not (200 <= status < 300):
        sys.stderr.write(f"[WARN] positions status={status} body={body}\n")
        return out
    parsed = json.loads(body)
    if str(parsed.get("code", "")) != "0":
        sys.stderr.write(f"[WARN] positions: {body}\n")
        return out
    for row in parsed.get("data", []):
        inst = str(row.get("instId", ""))
        if inst not in out:
            continue
        pos = decimal_or(row.get("pos"))
        pos_side = str(row.get("posSide", "")).lower()
        if pos_side == "short":
            pos = -abs(pos)
        elif pos_side == "long":
            pos = abs(pos)
        # posSide=net keeps the sign as-is
        out[inst] += pos
    return out


# -------------------- planning --------------------


def plan_symbol(state: SymbolState, mode: str) -> SymbolPlan:
    spec = state.spec
    free = state.free
    borrowed = state.borrowed
    interest = state.interest
    pos_coins = state.swap_position_coins

    repay_amt = min(free, borrowed) if (free > 0 and borrowed > 0) else ZERO
    free_after = free - repay_amt
    borrow_after = borrowed - repay_amt
    net_qty = free_after - borrow_after + pos_coins

    swap_side: Optional[str] = None
    swap_contracts = ZERO
    swap_skip: Optional[str] = None
    buyback_amt = ZERO
    buyback_skip: Optional[str] = None
    selldown_amt = ZERO
    selldown_skip: Optional[str] = None

    if mode == "align":
        delta_coins = -net_qty
    else:
        delta_coins = -pos_coins

    if delta_coins == 0:
        swap_skip = "no SWAP action needed (target delta is zero)"
    else:
        swap_side = "buy" if delta_coins > 0 else "sell"
        target_contracts = abs(delta_coins) / spec.swap_contract_size if spec.swap_contract_size > 0 else ZERO
        swap_contracts = floor_to_step(target_contracts, spec.swap_lot)
        if swap_contracts < spec.swap_min_sz:
            swap_skip = (
                f"contracts {format_decimal(swap_contracts)} < minSz "
                f"{format_decimal(spec.swap_min_sz)}: dust below API closing threshold"
            )
            swap_contracts = ZERO
            swap_side = None
        elif swap_side == "sell" and pos_coins <= 0:
            swap_skip = f"reduceOnly sell needs pos>0, have pos={format_decimal(pos_coins)}"
            swap_contracts = ZERO
            swap_side = None
        elif swap_side == "buy" and pos_coins >= 0:
            swap_skip = f"reduceOnly buy needs pos<0, have pos={format_decimal(pos_coins)}"
            swap_contracts = ZERO
            swap_side = None

    if mode == "clear":
        if borrow_after > 0:
            owed = borrow_after + interest
            buyback_amt = ceil_to_step(owed, spec.spot_lot)
            if buyback_amt < spec.spot_min_sz:
                buyback_skip = (
                    f"buyback qty {format_decimal(buyback_amt)} < spot minSz "
                    f"{format_decimal(spec.spot_min_sz)} (owed={format_decimal(owed)})"
                )
                buyback_amt = ZERO
        elif free_after > 0:
            selldown_amt = floor_to_step(free_after, spec.spot_lot)
            if selldown_amt < spec.spot_min_sz:
                selldown_skip = (
                    f"selldown qty {format_decimal(selldown_amt)} < spot minSz "
                    f"{format_decimal(spec.spot_min_sz)} (free_after={format_decimal(free_after)})"
                )
                selldown_amt = ZERO

    return SymbolPlan(
        state=state,
        repay_amt=repay_amt,
        free_after=free_after,
        borrow_after=borrow_after,
        net_qty=net_qty,
        swap_side=swap_side,
        swap_contracts=swap_contracts,
        swap_skip_reason=swap_skip,
        buyback_amt=buyback_amt,
        buyback_skip_reason=buyback_skip,
        selldown_amt=selldown_amt,
        selldown_skip_reason=selldown_skip,
    )


# -------------------- printing --------------------


def print_plan(env_name: str, mode: str, plans: List[SymbolPlan], execute: bool) -> None:
    print(f"[info] env={env_name} mode={mode} execute={execute}")
    print()
    header = (
        f"{'Symbol':<12} {'Asset':<6} {'Free':>14} {'Borrowed':>14} {'Interest':>10} "
        f"{'PosCoins':>14} {'Repay':>12} {'Net':>12} {'SWAP':>6} {'Contracts':>12} "
        f"{'Buyback':>12} {'Selldown':>12} Notes"
    )
    print(header)
    print("-" * len(header))
    for p in plans:
        s = p.state
        notes_parts: List[str] = []
        if p.swap_skip_reason:
            notes_parts.append(f"swap_skip: {p.swap_skip_reason}")
        if p.buyback_skip_reason:
            notes_parts.append(f"buyback_skip: {p.buyback_skip_reason}")
        if p.selldown_skip_reason:
            notes_parts.append(f"selldown_skip: {p.selldown_skip_reason}")
        if mode != "clear" and p.borrow_after > 0:
            notes_parts.append(f"leftover_borrow={format_decimal(p.borrow_after)}")
        notes = "; ".join(notes_parts)
        print(
            f"{s.spec.symbol:<12} {s.spec.asset:<6} "
            f"{format_decimal(s.free):>14} "
            f"{format_decimal(s.borrowed):>14} "
            f"{format_decimal(s.interest):>10} "
            f"{format_decimal(s.swap_position_coins):>14} "
            f"{format_decimal(p.repay_amt):>12} "
            f"{format_decimal(p.net_qty):>12} "
            f"{(p.swap_side or '-'):>6} "
            f"{format_decimal(p.swap_contracts):>12} "
            f"{format_decimal(p.buyback_amt):>12} "
            f"{format_decimal(p.selldown_amt):>12} "
            f"{notes}"
        )
    print("-" * len(header))


def print_report(env_name: str, mode: str, results: List[SymbolResult]) -> int:
    print()
    print(f"[report] env={env_name} mode={mode}")
    print()
    header = (
        f"{'Symbol':<12} {'Repay':>8} {'SWAP':>8} {'Buyback':>8} {'Selldown':>8} Details"
    )
    print(header)
    print("-" * len(header))
    failures = 0
    for r in results:
        details: List[str] = []
        for label, outcome in (
            ("repay", r.repay),
            ("swap", r.swap),
            ("buyback", r.buyback),
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
            f"{tag(r.repay):>8} {tag(r.swap):>8} {tag(r.buyback):>8} {tag(r.selldown):>8} "
            f"{'; '.join(details)}"
        )
    print("-" * len(header))
    return failures


# -------------------- execution --------------------


def execute_repay(plan: SymbolPlan, api_key: str, api_secret: str, passphrase: str) -> PhaseOutcome:
    if plan.repay_amt <= 0:
        return PhaseOutcome(ok=None)
    asset = plan.state.spec.asset
    amt = format_decimal(plan.repay_amt)
    print(f"\n[repay] ccy={asset} amt={amt}")
    status, body = okx_private(
        "POST",
        OKX_BORROW_REPAY_PATH,
        api_key, api_secret, passphrase,
        body={"ccy": asset, "side": "repay", "amt": amt},
    )
    okx_ok, brief = okx_response_ok(body)
    ok = (200 <= status < 300) and okx_ok
    print(f"  [{'OK' if ok else 'ERR'}] status={status} {brief}")
    print(f"  {body}")
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} {brief}"[:200])


def execute_swap(plan: SymbolPlan, api_key: str, api_secret: str, passphrase: str) -> PhaseOutcome:
    if plan.swap_skip_reason or plan.swap_contracts <= 0 or not plan.swap_side:
        return PhaseOutcome(ok=None)
    inst = plan.state.spec.swap_inst
    sz = format_decimal(plan.swap_contracts)
    print(f"\n[swap] {inst} {plan.swap_side} sz={sz} (contracts) reduceOnly=true")
    status, body = okx_private(
        "POST",
        OKX_ORDER_PATH,
        api_key, api_secret, passphrase,
        body={
            "instId": inst,
            "tdMode": "cross",
            "side": plan.swap_side,
            "ordType": "market",
            "sz": sz,
            "reduceOnly": True,
        },
    )
    okx_ok, brief = okx_response_ok(body)
    ok = (200 <= status < 300) and okx_ok
    print(f"  [{'OK' if ok else 'ERR'}] status={status} {brief}")
    print(f"  {body}")
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} {brief}"[:200])


def execute_buyback(plan: SymbolPlan, api_key: str, api_secret: str, passphrase: str) -> PhaseOutcome:
    if plan.buyback_skip_reason or plan.buyback_amt <= 0:
        return PhaseOutcome(ok=None)
    inst = plan.state.spec.spot_inst
    sz = format_decimal(plan.buyback_amt)
    print(f"\n[buyback] {inst} buy sz={sz} (base) tdMode=cross")
    status, body = okx_private(
        "POST",
        OKX_ORDER_PATH,
        api_key, api_secret, passphrase,
        body={
            "instId": inst,
            "tdMode": "cross",
            "side": "buy",
            "ordType": "market",
            "sz": sz,
            "tgtCcy": "base_ccy",
        },
    )
    okx_ok, brief = okx_response_ok(body)
    ok = (200 <= status < 300) and okx_ok
    print(f"  [{'OK' if ok else 'ERR'}] status={status} {brief}")
    print(f"  {body}")
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} {brief}"[:200])


def execute_selldown(plan: SymbolPlan, api_key: str, api_secret: str, passphrase: str) -> PhaseOutcome:
    if plan.selldown_skip_reason or plan.selldown_amt <= 0:
        return PhaseOutcome(ok=None)
    inst = plan.state.spec.spot_inst
    sz = format_decimal(plan.selldown_amt)
    print(f"\n[selldown] {inst} sell sz={sz} (base) tdMode=cross")
    status, body = okx_private(
        "POST",
        OKX_ORDER_PATH,
        api_key, api_secret, passphrase,
        body={
            "instId": inst,
            "tdMode": "cross",
            "side": "sell",
            "ordType": "market",
            "sz": sz,
            "tgtCcy": "base_ccy",
        },
    )
    okx_ok, brief = okx_response_ok(body)
    ok = (200 <= status < 300) and okx_ok
    print(f"  [{'OK' if ok else 'ERR'}] status={status} {brief}")
    print(f"  {body}")
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} {brief}"[:200])


# -------------------- main --------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Flatten OKEx Unified positions (repay + SWAP close + buyback/selldown)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--symbols",
        required=True,
        help="Comma-separated symbol list (e.g. BTCUSDT,ETHUSDT); USDT-quoted only",
    )
    parser.add_argument("--mode", choices=["align", "clear"], default="align")
    parser.add_argument("--execute", action="store_true", help="Actually submit; default dry-run")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    env_name = check_env_safety()
    auto_source_env_sh()
    api_key, api_secret, passphrase = load_credentials()
    symbols = parse_symbols(args.symbols)

    specs = fetch_specs(symbols)
    balances = fetch_balance(api_key, api_secret, passphrase)
    swap_insts = [specs[s].swap_inst for s in symbols]
    swap_positions = fetch_swap_positions(swap_insts, api_key, api_secret, passphrase)

    plans: List[SymbolPlan] = []
    for sym in symbols:
        spec = specs[sym]
        free, borrowed, interest = balances.get(spec.asset, (ZERO, ZERO, ZERO))
        pos_contracts = swap_positions.get(spec.swap_inst, ZERO)
        pos_coins = pos_contracts * spec.swap_contract_size
        state = SymbolState(
            spec=spec, free=free, borrowed=borrowed, interest=interest,
            swap_position_coins=pos_coins,
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

    print("\n--- Phase R: repay (borrow-repay) ---")
    for p in plans:
        r = results_by_symbol[p.state.spec.symbol]
        r.repay = execute_repay(p, api_key, api_secret, passphrase)

    print("\n--- Phase U: SWAP reduce-only ---")
    for p in plans:
        r = results_by_symbol[p.state.spec.symbol]
        r.swap = execute_swap(p, api_key, api_secret, passphrase)

    if args.mode == "clear":
        print("\n--- Phase B: spot buyback (tdMode=cross) ---")
        for p in plans:
            r = results_by_symbol[p.state.spec.symbol]
            r.buyback = execute_buyback(p, api_key, api_secret, passphrase)

        print("\n--- Phase S: spot selldown (tdMode=cross) ---")
        for p in plans:
            r = results_by_symbol[p.state.spec.symbol]
            r.selldown = execute_selldown(p, api_key, api_secret, passphrase)

    failures = print_report(env_name, args.mode, list(results_by_symbol.values()))
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
