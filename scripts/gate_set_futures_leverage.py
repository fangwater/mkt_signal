#!/usr/bin/env python3
"""Set Gate USDT futures leverage for contracts from an intra dashboard snapshot.

Default is dry-run. Source the target env first so GATE_API_KEY/GATE_API_SECRET
are available, then add --execute to call Gate.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlencode
import urllib.request

import requests


DASHBOARD_HOST = os.environ.get("DASHBOARD_HOST", "127.0.0.1")
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "4191"))
HOST = os.environ.get("GATE_API_BASE", "https://api.gateio.ws").rstrip("/")
PREFIX = "/api/v4"


def dec(value: Any, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return Decimal(default)


def fmt(value: Any) -> str:
    if value is None:
        return "--"
    return str(value)


def normalize_asset(value: str) -> str:
    text = value.strip().upper()
    if text.endswith("_USDT"):
        return text[:-5]
    cleaned = "".join(ch for ch in text if ch.isalnum())
    if cleaned.endswith("USDT") and len(cleaned) > 4:
        return cleaned[:-4]
    return cleaned


def normalize_contract(value: str) -> str:
    text = value.strip().upper()
    if text.endswith("_USDT"):
        return text
    asset = normalize_asset(text)
    if not asset:
        return ""
    return f"{asset}_USDT"


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
    timeout: int = 10,
) -> Tuple[int, Any]:
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
    return resp.status_code, data


def fetch_dashboard_contracts(suffix: str, min_gross_usdt: Decimal) -> List[str]:
    url = f"http://{DASHBOARD_HOST}:{DASHBOARD_PORT}/intra/gate-intra-{suffix}/snapshot"
    try:
        with urllib.request.urlopen(urllib.request.Request(url), timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        raise SystemExit(f"failed to fetch snapshot from {url}: {exc}")

    contracts: Set[str] = set()
    for entry in data.get("entries", []):
        if entry.get("channel") != "pre_trade_exposure":
            continue
        for row in entry.get("entry", {}).get("rows", []):
            if row.get("is_total"):
                continue
            asset = normalize_asset(str(row.get("asset") or ""))
            if not asset:
                continue
            gross = abs(dec(row.get("open_usdt"))) + abs(dec(row.get("hedge_usdt"))) + abs(dec(row.get("net_usdt")))
            if gross < min_gross_usdt:
                continue
            contracts.add(f"{asset}_USDT")
    return sorted(contracts)


def parse_csv_contracts(values: Iterable[str]) -> List[str]:
    contracts: Set[str] = set()
    for value in values:
        for part in value.split(","):
            contract = normalize_contract(part)
            if contract:
                contracts.add(contract)
    return sorted(contracts)


def position_mode(position: Any) -> str:
    if not isinstance(position, dict):
        return "unknown"
    leverage = str(position.get("leverage", "")).strip()
    if leverage == "0":
        return "cross"
    if leverage:
        return "isolated"
    return "unknown"


def leverage_params_for(position: Any, target: str, force_isolated: bool) -> Dict[str, str]:
    if force_isolated:
        return {"leverage": target}
    if position_mode(position) == "cross":
        return {"leverage": "0", "cross_leverage_limit": target}
    return {"leverage": target}


def print_position(prefix: str, contract: str, position: Any) -> None:
    if not isinstance(position, dict):
        print(f"{prefix:<7} {contract:<14} non-dict position body={position}")
        return
    print(
        f"{prefix:<7} {contract:<14} mode={position_mode(position):<8} "
        f"lev={fmt(position.get('leverage')):<6} cross_limit={fmt(position.get('cross_leverage_limit')):<6} "
        f"risk_limit={fmt(position.get('risk_limit')):<12} lev_max={fmt(position.get('leverage_max')):<8} "
        f"size={fmt(position.get('size')):<12} value={fmt(position.get('value'))}"
    )


def is_position_not_found(status: int, body: Any) -> bool:
    if status < 300:
        return False
    if not isinstance(body, dict):
        return False
    return str(body.get("label", "")).strip().upper() == "POSITION_NOT_FOUND"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Set Gate USDT futures leverage/cross leverage limit for intra contracts.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--suffix", default="", help="Intra suffix, e.g. arb01 reads gate-intra-arb01 dashboard.")
    parser.add_argument("--contracts", action="append", default=[], help="Contracts/assets CSV, e.g. DOGE_USDT,BTC.")
    parser.add_argument("--symbol", action="append", default=[], help="Alias for --contracts; accepts asset or contract.")
    parser.add_argument("--leverage", required=True, help="Target leverage value, e.g. 5.")
    parser.add_argument("--settle", default="usdt", help="Gate futures settle currency.")
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--min-gross-usdt", type=float, default=0.0, help="Dashboard row gross threshold.")
    parser.add_argument("--execute", action="store_true", help="Call Gate API. Without this, only prints the plan.")
    parser.add_argument(
        "--force-isolated",
        action="store_true",
        help="Send leverage=<target> even for cross positions; default preserves cross with leverage=0&cross_leverage_limit=<target>.",
    )
    parser.add_argument(
        "--strict-missing-position",
        action="store_true",
        help="Treat POSITION_NOT_FOUND as an error instead of skipping the contract.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    target = str(args.leverage).strip()
    if not target or dec(target) <= 0:
        raise SystemExit("--leverage must be positive")

    contracts = parse_csv_contracts([*args.contracts, *args.symbol])
    if args.suffix:
        contracts.extend(fetch_dashboard_contracts(args.suffix, dec(args.min_gross_usdt)))
    contracts = sorted(set(contracts))
    if not contracts:
        raise SystemExit("no contracts selected; pass --suffix or --contracts")

    api_key, api_secret = load_credentials()
    settle = args.settle.lower()
    print(f"[info] settle={settle} target_leverage={target} contracts={len(contracts)} execute={args.execute}")

    failures = 0
    for idx, contract in enumerate(contracts, start=1):
        path = f"/futures/{settle}/positions/{contract}"
        status, before = private_request(api_key, api_secret, "GET", path, timeout=args.timeout)
        if status >= 300:
            if is_position_not_found(status, before) and not args.strict_missing_position:
                print(f"[{idx}/{len(contracts)}] {contract} skip: Gate futures position not found")
                continue
            failures += 1
            print(f"[{idx}/{len(contracts)}] {contract} GET failed status={status} body={before}")
            continue
        print(f"\n[{idx}/{len(contracts)}]")
        print_position("before", contract, before)

        params = leverage_params_for(before, target, args.force_isolated)
        print(f"plan    {contract:<14} POST {path}/leverage?{build_query(params)}")
        if not args.execute:
            continue

        status, after = private_request(
            api_key,
            api_secret,
            "POST",
            f"{path}/leverage",
            params=params,
            timeout=args.timeout,
        )
        if status >= 300:
            failures += 1
            print(f"after   {contract:<14} ERR status={status} body={after}")
            continue
        print_position("after", contract, after)

    if failures:
        print(f"\nWARN: {failures} contracts failed", file=sys.stderr)
        raise SystemExit(1)
    print("\nDone.")


if __name__ == "__main__":
    main()
