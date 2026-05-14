#!/usr/bin/env python3
"""Flatten Gate Unified Account positions (FR arb cleanup).

Self-contained: no imports from other scripts in this repo.

Four-phase pipeline per symbol (no dust convert):
  Phase R — explicit /unified/loans repay (per repo memory; order-level auto_repay
            does not retire historical borrow)
  Phase U — USDT futures reduce-only MARKET close
  Phase B — clear-mode only — spot BUY (account=unified) with auto_repay=true,
            THEN explicit /unified/loans repay (still required per memory)
  Phase S — clear-mode only — spot SELL for USDT (free>borrow case)

Modes:
  align (default) — net_qty = available + futures_position_coins - borrowed = 0
  clear           — close futures fully; then B or S to drain asset

Behavior:
  - CWD basename must match ^gate_fr_ or ^gate-intra-.
  - Auto-sources ./env.sh; GATE_API_KEY/SECRET — env.sh always wins.
  - Dry-run by default; --execute required.

Usage:
  python3 scripts/flatten_gate_pm.py --symbols BTCUSDT,ETHUSDT
  python3 scripts/flatten_gate_pm.py --symbols BTCUSDT --mode clear --execute
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


GATE_HOST = "https://api.gateio.ws"
GATE_PREFIX = "/api/v4"

ENV_DIR_PATTERN = re.compile(r"^(gate_fr_|gate[-_]intra[-_])")
AUTHORITATIVE_KEYS = ("GATE_API_KEY", "GATE_API_SECRET")
ZERO = Decimal("0")


@dataclass
class SymbolSpec:
    symbol: str           # caller form, e.g. BTCUSDT
    asset: str            # BTC
    spot_pair: str        # BTC_USDT
    futures_contract: str # BTC_USDT
    spot_amount_step: Decimal
    spot_min_base_amount: Decimal
    futures_contract_size: Decimal  # quanto_multiplier
    futures_step_contracts: Decimal
    futures_min_contracts: Decimal


@dataclass
class SymbolState:
    spec: SymbolSpec
    free: Decimal              # unified available
    borrowed: Decimal          # unified borrowed
    interest: Decimal          # interest (treated >=0)
    futures_position_contracts: Decimal  # signed


@dataclass
class SymbolPlan:
    state: SymbolState
    repay_amt: Decimal
    free_after: Decimal
    borrow_after: Decimal
    net_qty: Decimal
    futures_side: Optional[str]   # "buy" / "sell"
    futures_contracts: Decimal
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
    repay: PhaseOutcome = field(default_factory=PhaseOutcome)
    futures: PhaseOutcome = field(default_factory=PhaseOutcome)
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


def gate_sign(method, path, query, body, secret, timestamp):
    hashed_body = hashlib.sha512(body.encode("utf-8")).hexdigest()
    sign_string = f"{method}\n{path}\n{query}\n{hashed_body}\n{timestamp}"
    return hmac.new(secret.encode("utf-8"), sign_string.encode("utf-8"), hashlib.sha512).hexdigest()


def gate_private(method, path, api_key, api_secret, *, params=None, body=None, timeout=15):
    method = method.upper()
    params = params or {}
    query = urllib.parse.urlencode(sorted(params.items(), key=lambda kv: kv[0]), doseq=True)
    body_text = "" if body is None else json.dumps(body, ensure_ascii=False, separators=(",", ":"))
    ts = str(int(time.time()))
    request_path = f"{GATE_PREFIX}{path}"
    signature = gate_sign(method, request_path, query, body_text, api_secret, ts)
    url = f"{GATE_HOST}{request_path}"
    if query:
        url = f"{url}?{query}"
    return http_request(
        url, method=method,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "KEY": api_key,
            "Timestamp": ts,
            "SIGN": signature,
        },
        data=None if method == "GET" else body_text.encode("utf-8"),
        timeout=timeout,
    )


def check_env_safety() -> str:
    cwd_name = os.path.basename(os.path.normpath(os.getcwd()))
    if not ENV_DIR_PATTERN.match(cwd_name):
        sys.stderr.write(
            f"[ERROR] CWD basename must match ^gate_fr_ or ^gate-intra-, got {cwd_name!r} "
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
    k = os.environ.get("GATE_API_KEY", "").strip()
    s = os.environ.get("GATE_API_SECRET", "").strip()
    if not k or not s:
        sys.stderr.write("[ERROR] missing GATE_API_KEY / GATE_API_SECRET.\n")
        sys.exit(2)
    return k, s


def parse_symbols(raw: str) -> List[str]:
    out = []
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
    wanted_pairs = {split_usdt(s): s for s in symbols}

    # Spot currency pairs
    status, body = http_request(f"{GATE_HOST}{GATE_PREFIX}/spot/currency_pairs", timeout=15)
    if not (200 <= status < 300):
        sys.exit(f"[ERROR] spot currency_pairs status={status} body={body}")
    spot_data = json.loads(body)

    spot_specs: Dict[str, Dict[str, Any]] = {}
    for item in spot_data:
        cp = str(item.get("id", ""))
        if not cp.endswith("_USDT"):
            continue
        asset = cp.replace("_USDT", "")
        if asset not in wanted_pairs:
            continue
        spot_specs[asset] = item

    # Futures contracts
    status, body = http_request(f"{GATE_HOST}{GATE_PREFIX}/futures/usdt/contracts", timeout=15)
    if not (200 <= status < 300):
        sys.exit(f"[ERROR] futures contracts status={status} body={body}")
    fut_data = json.loads(body)

    fut_specs: Dict[str, Dict[str, Any]] = {}
    for item in fut_data:
        c = str(item.get("name", ""))
        if not c.endswith("_USDT"):
            continue
        asset = c.replace("_USDT", "")
        if asset not in wanted_pairs:
            continue
        fut_specs[asset] = item

    out: Dict[str, SymbolSpec] = {}
    for asset, sym in wanted_pairs.items():
        if asset not in fut_specs:
            sys.exit(f"[ERROR] Gate futures contract not found for {asset}_USDT")
        if asset not in spot_specs:
            sys.exit(f"[ERROR] Gate spot currency pair not found for {asset}_USDT")
        sp = spot_specs[asset]
        fu = fut_specs[asset]
        # Gate spot: amount_precision is base-asset decimal places. step = 10^-precision.
        amount_prec = int(decimal_or(sp.get("amount_precision"), "0"))
        spot_step = Decimal(10) ** -amount_prec if amount_prec > 0 else Decimal("1")
        out[sym] = SymbolSpec(
            symbol=sym,
            asset=asset,
            spot_pair=f"{asset}_USDT",
            futures_contract=f"{asset}_USDT",
            spot_amount_step=spot_step,
            spot_min_base_amount=decimal_or(sp.get("min_base_amount"), "0"),
            futures_contract_size=decimal_or(fu.get("quanto_multiplier"), "1"),
            futures_step_contracts=decimal_or(fu.get("order_size_step"), "1"),
            futures_min_contracts=decimal_or(fu.get("order_size_min"), "0"),
        )
    return out


def fetch_unified_balance(api_key, api_secret) -> Dict[str, Tuple[Decimal, Decimal, Decimal]]:
    status, body = gate_private("GET", "/unified/accounts", api_key, api_secret)
    if not (200 <= status < 300):
        sys.exit(f"[ERROR] /unified/accounts status={status} body={body}")
    parsed = json.loads(body)
    balances = parsed.get("balances", {}) or {}
    out: Dict[str, Tuple[Decimal, Decimal, Decimal]] = {}
    if not isinstance(balances, dict):
        return out
    for asset, row in balances.items():
        if not isinstance(row, dict):
            continue
        avail = decimal_or(row.get("available"))
        borrowed = decimal_or(row.get("borrowed"))
        # Gate exposes "total_liab" or "interest"; prefer interest when present
        interest = decimal_or(row.get("interest"))
        out[asset.upper()] = (avail, borrowed, interest)
    return out


def fetch_futures_positions(symbols: List[str], api_key, api_secret) -> Dict[str, Decimal]:
    """Return {contract: signed_contracts}."""
    out: Dict[str, Decimal] = {}
    for sym in symbols:
        contract = f"{split_usdt(sym)}_USDT"
        status, body = gate_private(
            "GET", f"/futures/usdt/positions/{contract}", api_key, api_secret
        )
        if not (200 <= status < 300):
            sys.stderr.write(f"[WARN] positions {contract} status={status} body={body}\n")
            out[contract] = ZERO
            continue
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            out[contract] = ZERO
            continue
        if isinstance(data, dict):
            out[contract] = decimal_or(data.get("size"))
        else:
            out[contract] = ZERO
    return out


# -------------------- planning --------------------


def plan_symbol(state: SymbolState, mode: str) -> SymbolPlan:
    spec = state.spec
    free = state.free
    borrowed = state.borrowed
    interest = state.interest
    pos_contracts = state.futures_position_contracts
    pos_coins = pos_contracts * spec.futures_contract_size

    repay_amt = min(free, borrowed) if (free > 0 and borrowed > 0) else ZERO
    free_after = free - repay_amt
    borrow_after = borrowed - repay_amt
    net_qty = free_after - borrow_after + pos_coins

    futures_side: Optional[str] = None
    futures_contracts = ZERO
    futures_skip: Optional[str] = None
    buyback_amt = ZERO
    buyback_skip: Optional[str] = None
    selldown_amt = ZERO
    selldown_skip: Optional[str] = None

    if mode == "align":
        delta_coins = -net_qty
    else:
        delta_coins = -pos_coins

    if delta_coins == 0:
        futures_skip = "no futures action needed (target delta is zero)"
    else:
        futures_side = "buy" if delta_coins > 0 else "sell"
        target_contracts = abs(delta_coins) / spec.futures_contract_size if spec.futures_contract_size > 0 else ZERO
        futures_contracts = floor_to_step(target_contracts, spec.futures_step_contracts)
        if futures_contracts < spec.futures_min_contracts:
            futures_skip = (
                f"contracts {format_decimal(futures_contracts)} < min "
                f"{format_decimal(spec.futures_min_contracts)}: dust below threshold"
            )
            futures_contracts = ZERO
            futures_side = None
        elif futures_side == "sell" and pos_contracts <= 0:
            futures_skip = f"reduce_only sell needs pos>0 contracts, have {format_decimal(pos_contracts)}"
            futures_contracts = ZERO
            futures_side = None
        elif futures_side == "buy" and pos_contracts >= 0:
            futures_skip = f"reduce_only buy needs pos<0 contracts, have {format_decimal(pos_contracts)}"
            futures_contracts = ZERO
            futures_side = None

    if mode == "clear":
        if borrow_after > 0:
            owed = borrow_after + interest
            buyback_amt = ceil_to_step(owed, spec.spot_amount_step)
            if buyback_amt < spec.spot_min_base_amount:
                buyback_skip = (
                    f"buyback qty {format_decimal(buyback_amt)} < spot min_base_amount "
                    f"{format_decimal(spec.spot_min_base_amount)} (owed={format_decimal(owed)})"
                )
                buyback_amt = ZERO
        elif free_after > 0:
            selldown_amt = floor_to_step(free_after, spec.spot_amount_step)
            if selldown_amt < spec.spot_min_base_amount:
                selldown_skip = (
                    f"selldown qty {format_decimal(selldown_amt)} < spot min_base_amount "
                    f"{format_decimal(spec.spot_min_base_amount)} (free_after={format_decimal(free_after)})"
                )
                selldown_amt = ZERO

    return SymbolPlan(
        state=state,
        repay_amt=repay_amt,
        free_after=free_after,
        borrow_after=borrow_after,
        net_qty=net_qty,
        futures_side=futures_side,
        futures_contracts=futures_contracts,
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
        f"{'Symbol':<12} {'Asset':<6} {'Free':>14} {'Borrowed':>14} {'Interest':>10} "
        f"{'PosContracts':>14} {'Repay':>12} {'Net':>12} {'Fut Side':>8} {'Contracts':>12} "
        f"{'Buyback':>12} {'Selldown':>12} Notes"
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
        if mode != "clear" and p.borrow_after > 0:
            notes.append(f"leftover_borrow={format_decimal(p.borrow_after)}")
        print(
            f"{s.spec.symbol:<12} {s.spec.asset:<6} "
            f"{format_decimal(s.free):>14} "
            f"{format_decimal(s.borrowed):>14} "
            f"{format_decimal(s.interest):>10} "
            f"{format_decimal(s.futures_position_contracts):>14} "
            f"{format_decimal(p.repay_amt):>12} "
            f"{format_decimal(p.net_qty):>12} "
            f"{(p.futures_side or '-'):>8} "
            f"{format_decimal(p.futures_contracts):>12} "
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
        f"{'Symbol':<12} {'Repay':>8} {'Fut':>8} {'Buyback':>8} {'PostRepay':>10} {'Selldown':>10} Details"
    )
    print(header)
    print("-" * len(header))
    failures = 0
    for r in results:
        details = []
        for label, outcome in (
            ("repay", r.repay),
            ("futures", r.futures),
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
            f"{tag(r.repay):>8} {tag(r.futures):>8} {tag(r.buyback):>8} "
            f"{tag(r.post_buyback_repay):>10} {tag(r.selldown):>10} "
            f"{'; '.join(details)}"
        )
    print("-" * len(header))
    return failures


# -------------------- execution --------------------


def submit_unified_repay(
    api_key, api_secret, currency: str, amount: Decimal, *, idx: int
) -> PhaseOutcome:
    body = {
        "currency": currency,
        "amount": format_decimal(amount),
        "type": "repay",
        "repaid_all": True,
        "text": f"t-repay{int(time.time() * 1000)}{idx:02d}",
    }
    print(f"\n[repay] {currency} amount={format_decimal(amount)} repaid_all=true")
    status, body_resp = gate_private("POST", "/unified/loans", api_key, api_secret, body=body)
    ok = 200 <= status < 300
    print(f"  [{'OK' if ok else 'ERR'}] status={status}")
    print(f"  {body_resp}")
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} body={body_resp[:200]}")


def execute_repay(plan: SymbolPlan, api_key, api_secret, idx: int) -> PhaseOutcome:
    if plan.repay_amt <= 0:
        return PhaseOutcome(ok=None)
    return submit_unified_repay(api_key, api_secret, plan.state.spec.asset, plan.repay_amt, idx=idx)


def execute_futures(plan: SymbolPlan, api_key, api_secret) -> PhaseOutcome:
    if plan.futures_skip_reason or plan.futures_contracts <= 0 or not plan.futures_side:
        return PhaseOutcome(ok=None)
    sym = plan.state.spec.futures_contract
    side = plan.futures_side
    qty = plan.futures_contracts
    signed_size = -qty if side == "sell" else qty
    body = {
        "text": f"t-frflat-{int(time.time() * 1000)}",
        "contract": sym,
        "size": format_decimal(signed_size),
        "price": "0",
        "tif": "ioc",
        "reduce_only": True,
    }
    print(f"\n[futures] {sym} {side} size={body['size']} (contracts) reduce_only=true")
    status, body_resp = gate_private("POST", "/futures/usdt/orders", api_key, api_secret, body=body)
    ok = 200 <= status < 300
    print(f"  [{'OK' if ok else 'ERR'}] status={status}")
    print(f"  {body_resp}")
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} body={body_resp[:200]}")


def execute_buyback(plan: SymbolPlan, api_key, api_secret) -> PhaseOutcome:
    if plan.buyback_skip_reason or plan.buyback_amt <= 0:
        return PhaseOutcome(ok=None)
    pair = plan.state.spec.spot_pair
    amt = format_decimal(plan.buyback_amt)
    body = {
        "text": f"t-frbuy-{int(time.time() * 1000)}",
        "currency_pair": pair,
        "type": "market",
        "side": "buy",
        "amount": amt,
        "time_in_force": "ioc",
        "account": "unified",
        "auto_repay": True,
    }
    print(f"\n[buyback] {pair} buy amount={amt} (base) auto_repay=true")
    status, body_resp = gate_private("POST", "/spot/orders", api_key, api_secret, body=body)
    ok = 200 <= status < 300
    print(f"  [{'OK' if ok else 'ERR'}] status={status}")
    print(f"  {body_resp}")
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} body={body_resp[:200]}")


def execute_selldown(plan: SymbolPlan, api_key, api_secret) -> PhaseOutcome:
    if plan.selldown_skip_reason or plan.selldown_amt <= 0:
        return PhaseOutcome(ok=None)
    pair = plan.state.spec.spot_pair
    amt = format_decimal(plan.selldown_amt)
    body = {
        "text": f"t-frsell-{int(time.time() * 1000)}",
        "currency_pair": pair,
        "type": "market",
        "side": "sell",
        "amount": amt,
        "time_in_force": "ioc",
        "account": "unified",
    }
    print(f"\n[selldown] {pair} sell amount={amt} (base)")
    status, body_resp = gate_private("POST", "/spot/orders", api_key, api_secret, body=body)
    ok = 200 <= status < 300
    print(f"  [{'OK' if ok else 'ERR'}] status={status}")
    print(f"  {body_resp}")
    return PhaseOutcome(ok=ok, err="" if ok else f"status={status} body={body_resp[:200]}")


# -------------------- main --------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Flatten Gate Unified positions (repay + futures close + buyback/selldown)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--symbols", required=True,
                        help="Comma-separated symbol list (BTCUSDT,ETHUSDT); USDT-quoted only")
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
    balances = fetch_unified_balance(api_key, api_secret)
    futures_positions = fetch_futures_positions(symbols, api_key, api_secret)

    plans: List[SymbolPlan] = []
    for sym in symbols:
        spec = specs[sym]
        free, borrowed, interest = balances.get(spec.asset, (ZERO, ZERO, ZERO))
        pos = futures_positions.get(spec.futures_contract, ZERO)
        state = SymbolState(
            spec=spec, free=free, borrowed=borrowed, interest=interest,
            futures_position_contracts=pos,
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

    print("\n--- Phase R: explicit /unified/loans repay ---")
    for idx, p in enumerate(plans):
        r = results_by_symbol[p.state.spec.symbol]
        r.repay = execute_repay(p, api_key, api_secret, idx)

    print("\n--- Phase U: futures reduce-only ---")
    for p in plans:
        r = results_by_symbol[p.state.spec.symbol]
        r.futures = execute_futures(p, api_key, api_secret)

    if args.mode == "clear":
        print("\n--- Phase B: spot buyback + post-buyback explicit repay ---")
        for idx, p in enumerate(plans):
            r = results_by_symbol[p.state.spec.symbol]
            r.buyback = execute_buyback(p, api_key, api_secret)
            # Per repo memory: order-level auto_repay only covers own-order borrow;
            # historical borrow must be cleared with an explicit /unified/loans call.
            if r.buyback.ok and p.buyback_amt > 0:
                # Repay full remaining borrow_after + interest
                target = p.borrow_after + p.state.interest
                if target > 0:
                    r.post_buyback_repay = submit_unified_repay(
                        api_key, api_secret, p.state.spec.asset, target, idx=idx + 1000,
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
