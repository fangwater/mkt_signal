#!/usr/bin/env python3
"""Flatten Bitget UTA positions (FR arb cleanup).

Self-contained: no imports from other scripts in this repo.

Three-phase pipeline per symbol (Phase R removed; no dust convert):
  Phase U — USDT-FUTURES MARKET align/close
  Phase B — clear-mode only — cross-margin BUY (category=margin), which
            auto-borrows on demand AND auto-repays existing borrow on settlement
  Phase S — clear-mode only — cross-margin SELL for USDT (free>borrow case)

Note: Bitget UTA has no documented manual repay REST endpoint. Debt clearing
relies on Phase B's auto-repay via the BUY back, per user direction.

Modes:
  align (default) — net_qty = available + futures_position_coins - borrow = 0;
                    futures order is allowed to add/flip to match spot exposure
  clear           — close futures to 0 with reduceOnly; B or S to drain asset

Behavior:
  - CWD basename must match ^bitget_fr_ or ^bitget-intra-.
  - Auto-sources ./env.sh; BITGET_API_KEY/SECRET/PASSPHRASE — env.sh always wins.
  - Dry-run by default; --execute required.

Usage:
  python3 scripts/flatten_bitget_pm.py --symbol BTCUSDT
  python3 scripts/flatten_bitget_pm.py --symbols BTCUSDT,ETHUSDT
  python3 scripts/flatten_bitget_pm.py --symbols BTCUSDT --mode clear --execute
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


BITGET_BASE = os.environ.get("BITGET_API_BASE", "https://api.bitget.com").rstrip("/")

ENV_DIR_PATTERN = re.compile(r"^(bitget_fr_|bitget[-_]intra[-_])")
# Bitget repo scripts accept either passphrase env var name.
AUTHORITATIVE_KEYS = ("BITGET_API_KEY", "BITGET_API_SECRET", "BITGET_PASSPHRASE", "BITGET_API_PASSPHRASE")
ZERO = Decimal("0")


@dataclass
class SymbolSpec:
    symbol: str           # BTCUSDT (used as-is for both margin & futures, distinguished by category)
    asset: str            # BTC
    spot_qty_step: Decimal
    spot_min_qty: Decimal
    futures_qty_step: Decimal
    futures_min_qty: Decimal


@dataclass
class SymbolState:
    spec: SymbolSpec
    free: Decimal
    borrowed: Decimal
    interest: Decimal
    futures_position: Decimal  # signed base-coin qty


@dataclass
class SymbolPlan:
    state: SymbolState
    free_after: Decimal       # = free (no Phase R)
    net_qty: Decimal
    futures_side: Optional[str]
    futures_qty: Decimal
    futures_reduce_only: bool
    futures_skip_reason: Optional[str]
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
    futures: PhaseOutcome = field(default_factory=PhaseOutcome)
    buyback: PhaseOutcome = field(default_factory=PhaseOutcome)
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


def bitget_sign(api_secret, ts_ms, method, request_path_with_query, body):
    payload = f"{ts_ms}{method.upper()}{request_path_with_query}{body}"
    digest = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def bitget_private(method, path, api_key, api_secret, passphrase, *, query=None, body=None, timeout=15):
    method = method.upper()
    query_str = ""
    if query:
        query_str = urllib.parse.urlencode(sorted(query.items(), key=lambda kv: kv[0]))
    request_path = path
    if query_str:
        request_path = f"{path}?{query_str}"
    body_text = "" if (method == "GET" or body is None) else json.dumps(body, ensure_ascii=False, separators=(",", ":"))
    ts_ms = str(int(time.time() * 1000))
    sig = bitget_sign(api_secret, ts_ms, method, request_path, body_text)
    headers = {
        "ACCESS-KEY": api_key,
        "ACCESS-SIGN": sig,
        "ACCESS-TIMESTAMP": ts_ms,
        "ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
        "locale": "en-US",
    }
    url = f"{BITGET_BASE}{request_path}"
    return http_request(
        url, method=method, headers=headers,
        data=None if method == "GET" else body_text.encode("utf-8"),
        timeout=timeout,
    )


def bitget_ok(body: str) -> Tuple[bool, str]:
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return False, "non-json"
    code = str(parsed.get("code", ""))
    if code in ("0", "00000"):
        return True, ""
    return False, f"code={code} msg={parsed.get('msg', '')}"


def check_env_safety() -> str:
    cwd_name = os.path.basename(os.path.normpath(os.getcwd()))
    if not ENV_DIR_PATTERN.match(cwd_name):
        sys.stderr.write(
            f"[ERROR] CWD basename must match ^bitget_fr_ or ^bitget-intra-, got {cwd_name!r} "
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


def load_credentials() -> Tuple[str, str, str]:
    k = os.environ.get("BITGET_API_KEY", "").strip()
    s = os.environ.get("BITGET_API_SECRET", "").strip()
    # accept either passphrase name
    p = (os.environ.get("BITGET_PASSPHRASE", "") or os.environ.get("BITGET_API_PASSPHRASE", "")).strip()
    if not k or not s or not p:
        sys.stderr.write(
            "[ERROR] missing BITGET_API_KEY / BITGET_API_SECRET / "
            "BITGET_PASSPHRASE (or BITGET_API_PASSPHRASE).\n"
        )
        sys.exit(2)
    return k, s, p


def normalize_symbol(raw: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", raw or "").upper()


def parse_symbol_args(single_symbols: List[str], multi_symbols: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    chunks = list(single_symbols or []) + list(multi_symbols or [])
    for chunk in chunks:
        for tok in re.split(r"[\s,]+", chunk.strip()):
            s = normalize_symbol(tok)
            if s and s not in seen:
                out.append(s)
                seen.add(s)
    if not out:
        sys.exit("[ERROR] provide at least one --symbol or --symbols value")
    return out


def split_usdt(symbol: str) -> str:
    if symbol.endswith("USDT"):
        return symbol[:-4]
    sys.exit(f"[ERROR] only USDT-quoted symbols supported, got {symbol!r}")


# -------------------- state fetch --------------------


def fetch_instruments(category: str, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    status, body = http_request(
        f"{BITGET_BASE}/api/v3/market/instruments?category={category}", timeout=15
    )
    if not (200 <= status < 300):
        sys.exit(f"[ERROR] instruments {category} status={status} body={body}")
    parsed = json.loads(body)
    if str(parsed.get("code", "")) not in ("0", "00000"):
        sys.exit(f"[ERROR] instruments {category}: {body}")
    rows = parsed.get("data", []) or []
    wanted = set(symbols)
    for item in rows:
        sym = str(item.get("symbol", "")).upper()
        if sym in wanted:
            out[sym] = item
    return out


def fetch_specs(symbols: List[str]) -> Dict[str, SymbolSpec]:
    margin = fetch_instruments("MARGIN", symbols)
    futures = fetch_instruments("USDT-FUTURES", symbols)
    out: Dict[str, SymbolSpec] = {}
    for sym in symbols:
        m = margin.get(sym, {})
        f = futures.get(sym, {})
        if not m:
            sys.exit(f"[ERROR] Bitget MARGIN instrument not found for {sym}")
        if not f:
            sys.exit(f"[ERROR] Bitget USDT-FUTURES instrument not found for {sym}")
        out[sym] = SymbolSpec(
            symbol=sym,
            asset=split_usdt(sym),
            # Bitget instruments expose qty step under different keys depending on category;
            # try several common names.
            spot_qty_step=decimal_or(
                m.get("quantityStep") or m.get("sizeStep") or m.get("baseSizeStep"), "1"
            ),
            spot_min_qty=decimal_or(
                m.get("minOrderQuantity") or m.get("minTradeNum") or m.get("minQuantity"), "0"
            ),
            futures_qty_step=decimal_or(
                f.get("quantityStep") or f.get("sizeStep") or f.get("baseSizeStep"), "1"
            ),
            futures_min_qty=decimal_or(
                f.get("minOrderQuantity") or f.get("minTradeNum") or f.get("minQuantity"), "0"
            ),
        )
    return out


def fetch_assets(api_key, api_secret, passphrase) -> Dict[str, Tuple[Decimal, Decimal, Decimal]]:
    status, body = bitget_private(
        "GET", "/api/v3/account/assets", api_key, api_secret, passphrase,
    )
    if not (200 <= status < 300):
        sys.exit(f"[ERROR] /api/v3/account/assets status={status} body={body}")
    parsed = json.loads(body)
    if str(parsed.get("code", "")) not in ("0", "00000"):
        sys.exit(f"[ERROR] /api/v3/account/assets: {body}")
    data = parsed.get("data")
    if isinstance(data, dict):
        assets = data.get("assets", [])
    elif isinstance(data, list):
        assets = data
    else:
        assets = []
    out: Dict[str, Tuple[Decimal, Decimal, Decimal]] = {}
    for item in assets or []:
        if not isinstance(item, dict):
            continue
        coin = str(item.get("coin", "")).upper()
        if not coin:
            continue
        avail = decimal_or(item.get("available"))
        # Bitget exposes both `borrow` (principal) and `debt`/`debts` (with interest);
        # use `debt` if present, else fall back to `borrow`.
        debt = decimal_or(item.get("debt") or item.get("debts"))
        borrow = decimal_or(item.get("borrow"))
        borrowed = debt if debt > 0 else borrow
        interest = max(debt - borrow, ZERO)
        out[coin] = (avail, borrowed, interest)
    return out


def fetch_positions(symbols: List[str], api_key, api_secret, passphrase) -> Dict[str, Decimal]:
    out: Dict[str, Decimal] = {s: ZERO for s in symbols}
    status, body = bitget_private(
        "GET", "/api/v3/position/current-position", api_key, api_secret, passphrase,
        query={"category": "USDT-FUTURES"},
    )
    if not (200 <= status < 300):
        sys.stderr.write(f"[WARN] current-position status={status} body={body}\n")
        return out
    parsed = json.loads(body)
    if str(parsed.get("code", "")) not in ("0", "00000"):
        sys.stderr.write(f"[WARN] current-position: {body}\n")
        return out
    data = parsed.get("data")
    if isinstance(data, dict):
        rows = data.get("positions") or data.get("list") or []
    else:
        rows = data if isinstance(data, list) else []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol", "")).upper()
        if sym not in out:
            continue
        size = decimal_or(row.get("total") or row.get("size") or row.get("holdSize"))
        side = str(row.get("posSide") or row.get("holdSide") or row.get("side", "")).lower()
        if side in ("short", "sell"):
            out[sym] = -abs(size)
        elif side in ("long", "buy"):
            out[sym] = abs(size)
        else:
            out[sym] = size
    return out


# -------------------- planning --------------------


def plan_symbol(state: SymbolState, mode: str) -> SymbolPlan:
    spec = state.spec
    free = state.free
    borrowed = state.borrowed
    interest = state.interest
    pos = state.futures_position
    # No Phase R; free_after = free, borrow_after = borrowed.
    free_after = free
    net_qty = free - borrowed + pos

    futures_side: Optional[str] = None
    futures_qty = ZERO
    futures_reduce_only = mode == "clear"
    futures_skip: Optional[str] = None
    buyback_amt = ZERO
    buyback_skip: Optional[str] = None
    selldown_amt = ZERO
    selldown_skip: Optional[str] = None

    if mode == "align":
        delta = -net_qty
    else:
        delta = -pos

    if delta == 0:
        futures_skip = "no futures action needed (target delta is zero)"
    else:
        futures_side = "buy" if delta > 0 else "sell"
        target = abs(delta)
        futures_qty = floor_to_step(target, spec.futures_qty_step)
        if futures_qty < spec.futures_min_qty:
            futures_skip = (
                f"qty {format_decimal(futures_qty)} < min "
                f"{format_decimal(spec.futures_min_qty)}: dust below threshold"
            )
            futures_qty = ZERO
            futures_side = None
        elif futures_reduce_only and futures_side == "sell" and pos <= 0:
            futures_skip = f"reduceOnly sell needs pos>0, have {format_decimal(pos)}"
            futures_qty = ZERO
            futures_side = None
        elif futures_reduce_only and futures_side == "buy" and pos >= 0:
            futures_skip = f"reduceOnly buy needs pos<0, have {format_decimal(pos)}"
            futures_qty = ZERO
            futures_side = None

    if mode == "clear":
        # If borrow > free → net BUY needed to extinguish borrow (cross-margin auto-repay)
        if borrowed > free:
            owed = (borrowed - free) + interest
            buyback_amt = ceil_to_step(owed, spec.spot_qty_step)
            if buyback_amt < spec.spot_min_qty:
                buyback_skip = (
                    f"buyback qty {format_decimal(buyback_amt)} < min "
                    f"{format_decimal(spec.spot_min_qty)} (owed={format_decimal(owed)})"
                )
                buyback_amt = ZERO
        elif free > borrowed:
            sell_target = free - borrowed
            selldown_amt = floor_to_step(sell_target, spec.spot_qty_step)
            if selldown_amt < spec.spot_min_qty:
                selldown_skip = (
                    f"selldown qty {format_decimal(selldown_amt)} < min "
                    f"{format_decimal(spec.spot_min_qty)} (free-borrow={format_decimal(sell_target)})"
                )
                selldown_amt = ZERO

    return SymbolPlan(
        state=state,
        free_after=free_after,
        net_qty=net_qty,
        futures_side=futures_side,
        futures_qty=futures_qty,
        futures_reduce_only=futures_reduce_only,
        futures_skip_reason=futures_skip,
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
        f"{'Symbol':<12} {'Asset':<6} {'Avail':>14} {'Borrowed':>14} {'Interest':>10} "
        f"{'Pos':>14} {'Net':>12} {'Fut Side':>9} {'Fut Qty':>14} "
        f"{'Fut RO':>6} {'Buyback':>12} {'Selldown':>12} Notes"
    )
    print(header)
    print("-" * len(header))
    for p in plans:
        s = p.state
        notes = []
        if p.futures_skip_reason:
            notes.append(f"futures_skip: {p.futures_skip_reason}")
        if p.buyback_skip_reason:
            notes.append(f"buyback_skip: {p.buyback_skip_reason}")
        if p.selldown_skip_reason:
            notes.append(f"selldown_skip: {p.selldown_skip_reason}")
        if mode != "clear" and s.borrowed > 0:
            notes.append(f"borrowed={format_decimal(s.borrowed)} (no Phase R; clear mode handles)")
        print(
            f"{s.spec.symbol:<12} {s.spec.asset:<6} "
            f"{format_decimal(s.free):>14} "
            f"{format_decimal(s.borrowed):>14} "
            f"{format_decimal(s.interest):>10} "
            f"{format_decimal(s.futures_position):>14} "
            f"{format_decimal(p.net_qty):>12} "
            f"{(p.futures_side or '-'):>9} "
            f"{format_decimal(p.futures_qty):>14} "
            f"{str(p.futures_reduce_only).lower():>6} "
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
        f"{'Symbol':<12} {'Futures':>8} {'Buyback':>8} {'Selldown':>8} Details"
    )
    print(header)
    print("-" * len(header))
    failures = 0
    for r in results:
        details = []
        for label, outcome in (
            ("futures", r.futures),
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
            f"{tag(r.futures):>8} {tag(r.buyback):>8} {tag(r.selldown):>8} "
            f"{'; '.join(details)}"
        )
    print("-" * len(header))
    return failures


# -------------------- execution --------------------


def execute_futures(plan: SymbolPlan, api_key, api_secret, passphrase) -> PhaseOutcome:
    if plan.futures_skip_reason or plan.futures_qty <= 0 or not plan.futures_side:
        return PhaseOutcome(ok=None)
    sym = plan.state.spec.symbol
    qty = format_decimal(plan.futures_qty)
    body = {
        "category": "USDT-FUTURES",
        "symbol": sym,
        "side": plan.futures_side,
        "orderType": "market",
        "qty": qty,
        "clientOid": f"frflat-{int(time.time() * 1000)}",
    }
    if plan.futures_reduce_only:
        body["reduceOnly"] = "YES"
    print(f"\n[futures] {sym} {plan.futures_side} qty={qty} reduceOnly={str(plan.futures_reduce_only).lower()}")
    status, resp = bitget_private(
        "POST", "/api/v3/trade/place-order", api_key, api_secret, passphrase, body=body,
    )
    ok_http = 200 <= status < 300
    ok_ret, brief = bitget_ok(resp)
    ok = ok_http and ok_ret
    print(f"  [{'OK' if ok else 'ERR'}] status={status} {brief}")
    print(f"  {resp}")
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} {brief}"[:200])


def execute_buyback(plan: SymbolPlan, api_key, api_secret, passphrase) -> PhaseOutcome:
    if plan.buyback_skip_reason or plan.buyback_amt <= 0:
        return PhaseOutcome(ok=None)
    sym = plan.state.spec.symbol
    qty = format_decimal(plan.buyback_amt)
    body = {
        "category": "MARGIN",  # cross-margin (auto-borrow + auto-repay)
        "symbol": sym,
        "side": "buy",
        "orderType": "market",
        "qty": qty,
        "clientOid": f"frbuy-{int(time.time() * 1000)}",
    }
    print(f"\n[buyback] {sym} buy qty={qty} category=MARGIN (auto-repay)")
    status, resp = bitget_private(
        "POST", "/api/v3/trade/place-order", api_key, api_secret, passphrase, body=body,
    )
    ok_http = 200 <= status < 300
    ok_ret, brief = bitget_ok(resp)
    ok = ok_http and ok_ret
    print(f"  [{'OK' if ok else 'ERR'}] status={status} {brief}")
    print(f"  {resp}")
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} {brief}"[:200])


def execute_selldown(plan: SymbolPlan, api_key, api_secret, passphrase) -> PhaseOutcome:
    if plan.selldown_skip_reason or plan.selldown_amt <= 0:
        return PhaseOutcome(ok=None)
    sym = plan.state.spec.symbol
    qty = format_decimal(plan.selldown_amt)
    body = {
        "category": "MARGIN",
        "symbol": sym,
        "side": "sell",
        "orderType": "market",
        "qty": qty,
        "clientOid": f"frsell-{int(time.time() * 1000)}",
    }
    print(f"\n[selldown] {sym} sell qty={qty} category=MARGIN")
    status, resp = bitget_private(
        "POST", "/api/v3/trade/place-order", api_key, api_secret, passphrase, body=body,
    )
    ok_http = 200 <= status < 300
    ok_ret, brief = bitget_ok(resp)
    ok = ok_http and ok_ret
    print(f"  [{'OK' if ok else 'ERR'}] status={status} {brief}")
    print(f"  {resp}")
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} {brief}"[:200])


# -------------------- main --------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Flatten Bitget UTA positions (no Phase R; futures close + buyback/selldown)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--symbol", action="append", default=[],
                        help="Single symbol filter; repeatable, e.g. --symbol BTCUSDT --symbol ETHUSDT")
    parser.add_argument("--symbols", action="append", default=[],
                        help="Comma/space-separated symbol list (BTCUSDT,...); USDT-quoted only")
    parser.add_argument("--mode", choices=["align", "clear"], default="align")
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    env_name = check_env_safety()
    auto_source_env_sh()
    api_key, api_secret, passphrase = load_credentials()
    symbols = parse_symbol_args(args.symbol, args.symbols)

    specs = fetch_specs(symbols)
    balances = fetch_assets(api_key, api_secret, passphrase)
    positions = fetch_positions(symbols, api_key, api_secret, passphrase)

    plans: List[SymbolPlan] = []
    for sym in symbols:
        spec = specs[sym]
        free, borrowed, interest = balances.get(spec.asset, (ZERO, ZERO, ZERO))
        pos = positions.get(sym, ZERO)
        state = SymbolState(
            spec=spec, free=free, borrowed=borrowed, interest=interest,
            futures_position=pos,
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

    print("\n--- Phase U: futures align/close ---")
    for p in plans:
        r = results_by_symbol[p.state.spec.symbol]
        r.futures = execute_futures(p, api_key, api_secret, passphrase)

    if args.mode == "clear":
        print("\n--- Phase B: cross-margin buyback (category=margin, auto-repay) ---")
        for p in plans:
            r = results_by_symbol[p.state.spec.symbol]
            r.buyback = execute_buyback(p, api_key, api_secret, passphrase)

        print("\n--- Phase S: cross-margin selldown ---")
        for p in plans:
            r = results_by_symbol[p.state.spec.symbol]
            r.selldown = execute_selldown(p, api_key, api_secret, passphrase)

    failures = print_report(env_name, args.mode, list(results_by_symbol.values()))
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
