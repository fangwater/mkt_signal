#!/usr/bin/env python3
"""Compare Gate futures-tier maintenance margin against unified.assets push.

Read-only helper. It fetches current Gate futures positions, fetches each
contract's risk-limit tier table, computes:

    sum(max(abs(position.value) * maintenance_rate - deduction, 0))

Then it subscribes to one Gate unified.assets websocket update and compares the
computed futures maintenance margin with the pushed account-level maintenance
margin inferred using the repo's current parser convention.

Usage:
  source ~/gate_fr_arb01/env.sh
  python scripts/gate_compare_maintenance_margin.py --settle usdt
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
from decimal import Decimal, InvalidOperation, getcontext
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode

import requests

try:
    import websocket  # type: ignore
except ImportError:
    websocket = None


getcontext().prec = 40

HOST = os.environ.get("GATE_API_BASE", "https://api.gateio.ws").rstrip("/")
PREFIX = "/api/v4"
GATE_UNIFIED_WS_URL = os.environ.get("GATE_UNIFIED_WS_URL", "wss://ws.gate.com/v4/ws/unified")


def dec(value: Any, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    try:
        text = str(value).strip()
        if not text:
            return Decimal(default)
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return Decimal(default)


def fmt(value: Decimal, places: int = 8) -> str:
    q = Decimal(10) ** -places
    return str(value.quantize(q).normalize())


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
    signature = hmac.new(
        api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha512
    ).hexdigest()
    return {"Timestamp": timestamp, "SIGN": signature}


def private_request(
    api_key: str,
    api_secret: str,
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
) -> Any:
    body = ""
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
    if resp.status_code >= 300:
        raise RuntimeError(f"{method} {path} status={resp.status_code} body={data}")
    return data


def public_request(path: str, *, params: Optional[Dict[str, Any]] = None, timeout: int = 10) -> Any:
    query_string = build_query(params or {})
    url = f"{HOST}{PREFIX}{path}"
    if query_string:
        url = f"{url}?{query_string}"
    resp = requests.get(url, headers={"Accept": "application/json"}, timeout=timeout)
    try:
        data = resp.json()
    except ValueError:
        data = {"raw": resp.text}
    if resp.status_code >= 300:
        raise RuntimeError(f"GET {path} status={resp.status_code} body={data}")
    return data


def normalize_contract(contract: str) -> str:
    text = contract.strip().upper()
    if text.endswith("_USDT"):
        return text
    text = "".join(ch for ch in text if ch.isalnum())
    if text.endswith("USDT") and len(text) > 4:
        return f"{text[:-4]}_USDT"
    return text


def parse_contract_filter(values: Iterable[str]) -> Optional[set[str]]:
    out: set[str] = set()
    for value in values:
        for part in value.split(","):
            contract = normalize_contract(part)
            if contract:
                out.add(contract)
    return out or None


def position_contract(row: Dict[str, Any]) -> str:
    return str(row.get("contract") or row.get("symbol") or "").strip().upper()


def position_value(row: Dict[str, Any]) -> Decimal:
    direct = dec(row.get("value"))
    if direct != 0:
        return abs(direct)
    # Fallback for payloads without value. Gate positions normally include value.
    size = abs(dec(row.get("size")))
    mark_price = dec(row.get("mark_price") or row.get("mark_price_round") or row.get("entry_price"))
    multiplier = dec(row.get("quanto_multiplier") or row.get("contract_size") or "1")
    return abs(size * mark_price * multiplier)


def nonzero_positions(rows: Any, contract_filter: Optional[set[str]]) -> List[Dict[str, Any]]:
    if not isinstance(rows, list):
        raise RuntimeError(f"positions response is not a list: {rows}")
    out: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        contract = position_contract(row)
        if not contract:
            continue
        if contract_filter is not None and contract not in contract_filter:
            continue
        if dec(row.get("size")) == 0 and position_value(row) == 0:
            continue
        out.append(row)
    return out


def fetch_risk_tiers(settle: str, contract: str, timeout: int) -> List[Dict[str, Any]]:
    data = public_request(
        f"/futures/{settle}/risk_limit_tiers",
        params={"contract": contract},
        timeout=timeout,
    )
    if not isinstance(data, list):
        raise RuntimeError(f"risk_limit_tiers response for {contract} is not a list: {data}")
    return [row for row in data if isinstance(row, dict)]


def tier_risk_limit(row: Dict[str, Any]) -> Decimal:
    return dec(row.get("risk_limit") or row.get("limit") or row.get("max_risk_limit"))


def choose_position_risk_limit_tier(
    tiers: List[Dict[str, Any]], position: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    position_risk_limit = dec(position.get("risk_limit"))
    if position_risk_limit > 0:
        for tier in tiers:
            if tier_risk_limit(tier) == position_risk_limit:
                return tier
    return None


def choose_value_tier(tiers: List[Dict[str, Any]], value: Decimal) -> Optional[Dict[str, Any]]:
    sorted_tiers = sorted(tiers, key=tier_risk_limit)
    for tier in sorted_tiers:
        limit = tier_risk_limit(tier)
        if limit > 0 and value <= limit:
            return tier
    return sorted_tiers[-1] if sorted_tiers else None


def calc_position_maintenance(position: Dict[str, Any], tier: Dict[str, Any]) -> Decimal:
    value = position_value(position)
    maintenance_rate = dec(tier.get("maintenance_rate"))
    deduction = dec(tier.get("deduction"))
    maintenance = value * maintenance_rate - deduction
    return maintenance if maintenance > 0 else Decimal("0")


def ws_sign(channel: str, event: str, timestamp: int, secret: str) -> str:
    message = f"channel={channel}&event={event}&time={timestamp}"
    return hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha512).hexdigest()


def fetch_unified_assets_push(api_key: str, api_secret: str, timeout: int) -> Dict[str, Any]:
    if websocket is None:
        raise RuntimeError("missing dependency websocket-client; install with: pip install websocket-client")

    ws = websocket.create_connection(GATE_UNIFIED_WS_URL, timeout=timeout)
    try:
        timestamp = int(time.time())
        channel = "unified.assets"
        req = {
            "time": timestamp,
            "channel": channel,
            "event": "subscribe",
            "payload": [],
            "auth": {
                "method": "api_key",
                "KEY": api_key,
                "SIGN": ws_sign(channel, "subscribe", timestamp, api_secret),
            },
        }
        ws.send(json.dumps(req, separators=(",", ":")))
        deadline = time.time() + timeout
        last_payload: Optional[Dict[str, Any]] = None
        while time.time() < deadline:
            ws.settimeout(max(0.1, deadline - time.time()))
            msg = ws.recv()
            if isinstance(msg, bytes):
                msg = msg.decode("utf-8", "replace")
            data = json.loads(msg)
            if data.get("channel") != channel:
                continue
            if data.get("event") == "update" and isinstance(data.get("result"), dict):
                return data
            last_payload = data if isinstance(data, dict) else last_payload
        raise RuntimeError(f"timed out waiting for unified.assets update; last={last_payload}")
    finally:
        ws.close()


def pushed_maintenance_from_result(result: Dict[str, Any]) -> Dict[str, Decimal]:
    parser_margin_balance = abs(dec(result.get("b")))
    total_margin = dec(result.get("T"))
    r_pct = dec(result.get("r"))
    r_cap_pct = dec(result.get("R"))
    out = {
        "parser_margin_balance": parser_margin_balance,
        "total_margin": total_margin,
        "r_pct": r_pct,
        "R_pct": r_cap_pct,
        # Current checked-in parser convention as of this script:
        # margin_balance = abs(b); margin_ratio = R / 100;
        # maintenance_margin = abs(b) / (R / 100).
        "current_parser_margin_ratio": r_cap_pct / Decimal("100") if r_cap_pct != 0 else Decimal("0"),
        "current_parser_maintenance": (
            parser_margin_balance / (r_cap_pct / Decimal("100"))
            if r_cap_pct != 0
            else Decimal("0")
        ),
        "current_parser_initial": (
            parser_margin_balance / (r_pct / Decimal("100")) if r_pct != 0 else Decimal("0")
        ),
        # Diagnostic alternatives. Gate field docs/history around r/R/T/b have
        # been easy to misread; print these so a live sample can expose which
        # convention matches the tier sum.
        "T_r_rate_formula_maintenance": total_margin * (r_pct / Decimal("100")),
        "T_R_rate_formula_maintenance": total_margin * (r_cap_pct / Decimal("100")),
        "b_r_rate_formula_maintenance": parser_margin_balance * (r_pct / Decimal("100")),
        "b_R_rate_formula_maintenance": parser_margin_balance * (r_cap_pct / Decimal("100")),
    }
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only comparison of Gate tier maintenance margin and unified.assets push.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--settle", default="usdt", help="Gate futures settle currency.")
    parser.add_argument("--timeout", type=int, default=15, help="HTTP and websocket timeout seconds.")
    parser.add_argument(
        "--contract",
        action="append",
        default=[],
        help="Optional contract filter, comma-separated accepted, e.g. HNT_USDT,ZEREBRO_USDT.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON summary only.")
    parser.add_argument(
        "--skip-ws",
        action="store_true",
        help="Only compute futures tier maintenance margin; skip unified.assets push comparison.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    api_key, api_secret = load_credentials()
    settle = args.settle.lower()
    contract_filter = parse_contract_filter(args.contract)

    positions_raw = private_request(
        api_key, api_secret, "GET", f"/futures/{settle}/positions", timeout=args.timeout
    )
    positions = nonzero_positions(positions_raw, contract_filter)

    rows: List[Dict[str, Any]] = []
    total_position_risk_limit_calc = Decimal("0")
    total_value_tier_calc = Decimal("0")
    for pos in positions:
        contract = position_contract(pos)
        value = position_value(pos)
        tiers = fetch_risk_tiers(settle, contract, args.timeout)
        position_tier = choose_position_risk_limit_tier(tiers, pos)
        value_tier = choose_value_tier(tiers, value)
        if position_tier is None and value_tier is None:
            rows.append({"contract": contract, "error": "no tier", "value": str(value)})
            continue
        selected_tier = position_tier or value_tier
        assert selected_tier is not None
        selected_maintenance = calc_position_maintenance(pos, selected_tier)
        value_tier_maintenance = (
            calc_position_maintenance(pos, value_tier) if value_tier is not None else Decimal("0")
        )
        total_position_risk_limit_calc += selected_maintenance
        total_value_tier_calc += value_tier_maintenance
        rows.append(
            {
                "contract": contract,
                "size": str(pos.get("size", "")),
                "value": str(value),
                "position_risk_limit": str(pos.get("risk_limit", "")),
                "selected_tier": selected_tier.get("tier"),
                "selected_tier_risk_limit": str(tier_risk_limit(selected_tier)),
                "selected_maintenance_rate": str(selected_tier.get("maintenance_rate", "")),
                "selected_deduction": str(selected_tier.get("deduction", "")),
                "selected_calculated_maintenance": str(selected_maintenance),
                "value_tier": value_tier.get("tier") if value_tier is not None else None,
                "value_tier_risk_limit": str(tier_risk_limit(value_tier)) if value_tier is not None else "",
                "value_tier_maintenance_rate": (
                    str(value_tier.get("maintenance_rate", "")) if value_tier is not None else ""
                ),
                "value_tier_deduction": str(value_tier.get("deduction", "")) if value_tier is not None else "",
                "value_tier_calculated_maintenance": str(value_tier_maintenance),
            }
        )

    push_payload = None
    push_metrics = None
    if not args.skip_ws:
        push_payload = fetch_unified_assets_push(api_key, api_secret, args.timeout)
        push_metrics = pushed_maintenance_from_result(push_payload["result"])

    summary: Dict[str, Any] = {
        "settle": settle,
        "position_count": len(positions),
        "position_risk_limit_tier_maintenance_margin": str(total_position_risk_limit_calc),
        "value_tier_maintenance_margin": str(total_value_tier_calc),
        "positions": rows,
    }
    if push_metrics is not None:
        current_parser_maintenance = push_metrics["current_parser_maintenance"]
        rate_formula_maintenance = push_metrics["b_R_rate_formula_maintenance"]
        summary["unified_assets_result"] = push_payload["result"] if push_payload else None
        summary["pushed_current_parser_maintenance_margin"] = str(current_parser_maintenance)
        summary["pushed_b_R_rate_formula_maintenance_margin"] = str(rate_formula_maintenance)
        summary["position_risk_limit_diff_vs_current_parser"] = str(
            total_position_risk_limit_calc - current_parser_maintenance
        )
        summary["value_tier_diff_vs_current_parser"] = str(
            total_value_tier_calc - current_parser_maintenance
        )
        summary["position_risk_limit_diff_vs_b_R_rate_formula"] = str(
            total_position_risk_limit_calc - rate_formula_maintenance
        )
        summary["value_tier_diff_vs_b_R_rate_formula"] = str(
            total_value_tier_calc - rate_formula_maintenance
        )

    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return

    print(f"settle={settle} positions={len(positions)}")
    print()
    print(
        f"{'contract':<16} {'size':>16} {'value':>16} {'pos_t':>6} "
        f"{'pos_rl':>12} {'pos_mm':>14} {'val_t':>6} {'val_rl':>12} {'val_mm':>14}"
    )
    for row in rows:
        if "error" in row:
            print(f"{row['contract']:<16} ERROR {row['error']} value={row.get('value')}")
            continue
        print(
            f"{row['contract']:<16} {row['size']:>16} {fmt(dec(row['value'])):>16} "
            f"{str(row['selected_tier']):>6} {fmt(dec(row['selected_tier_risk_limit']), 4):>12} "
            f"{fmt(dec(row['selected_calculated_maintenance'])):>14} "
            f"{str(row['value_tier']):>6} {fmt(dec(row['value_tier_risk_limit']), 4):>12} "
            f"{fmt(dec(row['value_tier_calculated_maintenance'])):>14}"
        )

    print()
    print(
        "position_risk_limit_tier_maintenance_margin="
        f"{fmt(total_position_risk_limit_calc)}"
    )
    print(f"value_tier_maintenance_margin={fmt(total_value_tier_calc)}")
    if push_metrics is not None:
        print()
        print("unified.assets push fields:")
        print(
            f"abs(b)={fmt(push_metrics['parser_margin_balance'])} "
            f"T={fmt(push_metrics['total_margin'])} "
            f"r={fmt(push_metrics['r_pct'])}% R={fmt(push_metrics['R_pct'])}%"
        )
        print(
            "pushed_current_parser_maintenance_margin="
            f"{fmt(push_metrics['current_parser_maintenance'])}"
        )
        print(
            "pushed_current_parser_initial_margin="
            f"{fmt(push_metrics['current_parser_initial'])}"
        )
        print(
            "diagnostic_b_R_rate_formula_maintenance="
            f"{fmt(push_metrics['b_R_rate_formula_maintenance'])}"
        )
        print(
            "diagnostic_T_R_rate_formula_maintenance="
            f"{fmt(push_metrics['T_R_rate_formula_maintenance'])}"
        )
        print()
        print(
            "position_risk_limit_diff_vs_current_parser="
            f"{fmt(total_position_risk_limit_calc - push_metrics['current_parser_maintenance'])}"
        )
        print(
            "value_tier_diff_vs_current_parser="
            f"{fmt(total_value_tier_calc - push_metrics['current_parser_maintenance'])}"
        )
        print(
            "position_risk_limit_diff_vs_b_R_rate_formula="
            f"{fmt(total_position_risk_limit_calc - push_metrics['b_R_rate_formula_maintenance'])}"
        )
        print(
            "value_tier_diff_vs_b_R_rate_formula="
            f"{fmt(total_value_tier_calc - push_metrics['b_R_rate_formula_maintenance'])}"
        )
        print()
        print(
            "note: current_parser uses R as margin_ratio percent: "
            "maintenance_margin = abs(b) / (R / 100)."
        )


if __name__ == "__main__":
    main()
