#!/usr/bin/env python3
"""Place a Gate.io USDT futures order to close (reduce) a position.

Defaults to market + reduce_only and supports base-qty -> contracts conversion
using Gate's quanto_multiplier (contract multiplier).

Usage:
  export GATE_API_KEY="your_api_key"
  export GATE_API_SECRET="your_api_secret"
  python scripts/gate_futures_close.py --contract ALCH_USDT --close-all --execute
  python scripts/gate_futures_close.py --contract ALCH_USDT --base-qty 123.45 --side buy --execute
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import math
import os
import sys
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlencode

import requests

HOST = "https://api.gateio.ws"
PREFIX = "/api/v4"


def gen_sign(method: str, url: str, query_string: str, body: str, api_key: str, api_secret: str) -> Dict[str, str]:
    t = str(int(time.time()))
    hashed_body = hashlib.sha512(body.encode("utf-8")).hexdigest()
    sign_string = f"{method}\n{url}\n{query_string}\n{hashed_body}\n{t}"
    signature = hmac.new(api_secret.encode("utf-8"), sign_string.encode("utf-8"), hashlib.sha512).hexdigest()
    return {
        "KEY": api_key,
        "Timestamp": t,
        "SIGN": signature,
    }


def load_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("GATE_API_KEY", "").strip()
    api_secret = os.environ.get("GATE_API_SECRET", "").strip()
    missing = [name for name, value in [
        ("GATE_API_KEY", api_key),
        ("GATE_API_SECRET", api_secret)
    ] if not value]
    if missing:
        print(f"Missing env vars: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret


def build_query(params: Dict[str, Any]) -> str:
    items = [(k, v) for k, v in params.items() if v not in ("", None)]
    items.sort(key=lambda kv: kv[0])
    return urlencode(items, doseq=True)


def request(
    api_key: str,
    api_secret: str,
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    body: str = "",
) -> requests.Response:
    if params is None:
        params = {}
    query_string = build_query(params)
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    headers.update(gen_sign(method, PREFIX + path, query_string, body, api_key, api_secret))
    url = HOST + PREFIX + path
    if query_string:
        url = f"{url}?{query_string}"
    return requests.request(method, url, headers=headers, data=body)


def read_json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except ValueError:
        return {"raw": resp.text}


def parse_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        val = value.strip()
        if not val:
            return None
        try:
            return float(val)
        except ValueError:
            return None
    return None


def parse_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        val = value.strip().lower()
        if val in {"true", "1", "yes", "y"}:
            return True
        if val in {"false", "0", "no", "n"}:
            return False
    return None


def format_qty(value: float) -> str:
    text = f"{value:.12f}".rstrip("0").rstrip(".")
    if text == "-0":
        return "0"
    return text


def quantize_down(value: float, step: float) -> float:
    if step <= 0:
        return value
    steps = math.floor((abs(value) + 1e-12) / step)
    return steps * step


def fetch_contract_info(settle: str, contract: str) -> Dict[str, Any]:
    url = f"{HOST}{PREFIX}/futures/{settle}/contracts/{contract}"
    resp = requests.get(url)
    if resp.status_code != 200:
        raise RuntimeError(f"contract info failed: {resp.status_code} {read_json(resp)}")
    data = read_json(resp)
    if not isinstance(data, dict):
        raise RuntimeError(f"unexpected contract info: {data}")
    return data


def fetch_position(api_key: str, api_secret: str, settle: str, contract: str) -> Dict[str, Any]:
    resp = request(api_key, api_secret, "GET", f"/futures/{settle}/positions/{contract}")
    if resp.status_code != 200:
        raise RuntimeError(f"position query failed: {resp.status_code} {read_json(resp)}")
    data = read_json(resp)
    if not isinstance(data, dict):
        raise RuntimeError(f"unexpected position response: {data}")
    return data


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gate.io futures close helper.")
    parser.add_argument("--contract", default="ALCH_USDT", help="Futures contract, e.g. ALCH_USDT")
    parser.add_argument("--settle", default="usdt", help="Settlement currency (default: usdt)")
    parser.add_argument("--close-all", action="store_true", help="Close the full current position.")
    parser.add_argument("--base-qty", type=float, default=None, help="Base qty to close (e.g. 123.45 ALCH).")
    parser.add_argument("--contracts", type=float, default=None, help="Contracts to close (in contracts).")
    parser.add_argument("--side", choices=["buy", "sell"], default="", help="Order side for --base-qty/--contracts.")
    parser.add_argument("--price", default="", help="Limit price; omit for market order.")
    parser.add_argument("--tif", default="", help="Override tif (default: ioc for market, poc for limit).")
    parser.add_argument("--no-reduce-only", action="store_true", help="Disable reduce_only flag.")
    parser.add_argument("--execute", action="store_true", help="Actually submit the order.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_key, api_secret = load_credentials()

    contract = args.contract.upper()
    settle = args.settle.lower()

    contract_info = fetch_contract_info(settle, contract)
    quanto = parse_number(contract_info.get("quanto_multiplier")) or 1.0
    min_contracts = parse_number(contract_info.get("order_size_min")) or 0.0
    step_contracts = parse_number(contract_info.get("order_size_step")) or 0.0
    enable_decimal = parse_bool(contract_info.get("enable_decimal"))

    position = None
    pos_size = None
    pos_side = None
    if args.close_all or (args.base_qty is None and args.contracts is None):
        position = fetch_position(api_key, api_secret, settle, contract)
        pos_size = parse_number(position.get("size"))
        if pos_size is None:
            print(f"Position size missing in response: {position}", file=sys.stderr)
            return 1
        if pos_size == 0:
            print("Position size is 0; nothing to close.")
            return 0
        pos_side = "sell" if pos_size > 0 else "buy"

    if args.base_qty is not None and args.contracts is not None:
        print("Specify only one of --base-qty or --contracts.", file=sys.stderr)
        return 1

    if args.base_qty is not None:
        if not args.side:
            print("--side is required with --base-qty.", file=sys.stderr)
            return 1
        desired_contracts = args.base_qty / quanto if quanto > 0 else args.base_qty
        side = args.side
    elif args.contracts is not None:
        if not args.side:
            print("--side is required with --contracts.", file=sys.stderr)
            return 1
        desired_contracts = args.contracts
        side = args.side
    else:
        desired_contracts = abs(pos_size) if pos_size is not None else 0.0
        side = pos_side or ""

    if not side:
        print("Unable to determine side; pass --side.", file=sys.stderr)
        return 1

    rounded_contracts = quantize_down(desired_contracts, step_contracts)
    if step_contracts <= 0 and enable_decimal is False:
        rounded_contracts = math.floor(abs(desired_contracts) + 1e-12)
    if rounded_contracts <= 0:
        print(
            f"Computed contracts <= 0 after step rounding (desired={desired_contracts}, step={step_contracts}).",
            file=sys.stderr,
        )
        return 1
    if min_contracts > 0 and rounded_contracts < min_contracts:
        print(
            f"Contracts below min order size (contracts={rounded_contracts}, min={min_contracts}).",
            file=sys.stderr,
        )
        return 1

    signed_size = rounded_contracts if side == "buy" else -rounded_contracts
    if pos_size is not None and args.no_reduce_only is False:
        if abs(signed_size) > abs(pos_size) + 1e-9:
            signed_size = (abs(pos_size) if pos_size is not None else rounded_contracts)
            signed_size = signed_size if side == "buy" else -signed_size

    tif = args.tif.strip().lower()
    if args.price:
        price = args.price
        tif = tif or "poc"
    else:
        price = "0"
        tif = tif or "ioc"

    payload = {
        "contract": contract,
        "size": format_qty(signed_size),
        "price": price,
        "tif": tif,
        "account": "unified",
        "reduce_only": not args.no_reduce_only,
        "text": f"t-{int(time.time() * 1000)}",
    }

    print(f"contract={contract} settle={settle}")
    print(
        f"quanto_multiplier={contract_info.get('quanto_multiplier')} "
        f"order_size_min={min_contracts} order_size_step={step_contracts} "
        f"enable_decimal={enable_decimal}"
    )
    if position is not None:
        print(f"position_size={pos_size} contracts")
    print(f"requested_contracts={desired_contracts} -> rounded_contracts={rounded_contracts} side={side}")
    if args.base_qty is not None:
        print(f"base_qty={args.base_qty} -> contracts={desired_contracts}")
    print("order_payload:")
    print(json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=True))

    if not args.execute:
        print("Dry-run only. Re-run with --execute to submit the order.")
        return 0

    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
    resp = request(api_key, api_secret, "POST", f"/futures/{settle}/orders", body=body)
    data = read_json(resp)
    tag = "OK" if 200 <= resp.status_code < 300 else "ERR"
    print(f"Order result: {tag} http={resp.status_code}")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    return 0 if tag == "OK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
