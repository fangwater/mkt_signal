#!/usr/bin/env python3
"""Submit a Gate unified-account spot order.

Examples:
  set -a; source ~/gate_fr_arb02/env.sh; set +a
  python3 scripts/sell_gate_unified_spot.py --symbol AINUSDT --quantity 1000
  python3 scripts/sell_gate_unified_spot.py --symbol AINUSDT --quantity 1000 --execute
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Tuple


HOST = os.environ.get("GATE_API_BASE", "https://api.gateio.ws").rstrip("/")
PREFIX = "/api/v4"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Submit a Gate unified-account spot order (dry-run unless --execute)"
    )
    parser.add_argument("--base-url", default=HOST, help="Gate REST endpoint")
    parser.add_argument("--symbol", required=True, help="Trading pair, e.g. AINUSDT or AIN_USDT")
    parser.add_argument(
        "--side",
        default="SELL",
        choices=["BUY", "SELL", "buy", "sell"],
        help="Order side, default SELL",
    )
    parser.add_argument(
        "--type",
        default="MARKET",
        choices=["MARKET", "LIMIT", "market", "limit"],
        help="Order type, default MARKET",
    )
    parser.add_argument("--quantity", required=True, help="Base asset amount, e.g. 14780.69")
    parser.add_argument("--price", help="Price; required for LIMIT orders")
    parser.add_argument(
        "--time-in-force",
        default="ioc",
        choices=["gtc", "ioc", "poc", "fok"],
        help="Gate spot time_in_force, default ioc",
    )
    parser.add_argument("--text", help="Optional Gate text/client tag")
    parser.add_argument(
        "--account",
        default="unified",
        choices=["unified", "spot", "margin", "cross_margin"],
        help="Gate spot account type, default unified",
    )
    parser.add_argument(
        "--auto-repay",
        action="store_true",
        help="Set auto_repay=true on the spot order. Usually leave false for SELL selldown.",
    )
    parser.add_argument("--execute", action="store_true", help="Actually submit the order")
    return parser.parse_args()


def normalize_currency_pair(symbol: str) -> str:
    text = symbol.strip().upper()
    if not text:
        raise ValueError("empty --symbol")
    if text.endswith("_USDT"):
        return text
    cleaned = re.sub(r"[^A-Z0-9]", "", text)
    if cleaned.endswith("USDT") and len(cleaned) > 4:
        return f"{cleaned[:-4]}_USDT"
    raise ValueError(f"unsupported USDT symbol format: {symbol!r}")


def load_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("GATE_API_KEY", "").strip()
    api_secret = os.environ.get("GATE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: please set GATE_API_KEY / GATE_API_SECRET in environment.", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret


def sign(method: str, path: str, query: str, body: str, api_secret: str, timestamp: str) -> str:
    hashed_body = hashlib.sha512(body.encode("utf-8")).hexdigest()
    payload = f"{method}\n{path}\n{query}\n{hashed_body}\n{timestamp}"
    return hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha512).hexdigest()


def request_gate(
    base_url: str,
    method: str,
    path: str,
    body: Dict[str, Any],
    api_key: str,
    api_secret: str,
    timeout: int = 15,
) -> Tuple[int, str]:
    method = method.upper()
    query = ""
    body_text = json.dumps(body, ensure_ascii=False, separators=(",", ":"))
    request_path = f"{PREFIX}{path}"
    timestamp = str(int(time.time()))
    signature = sign(method, request_path, query, body_text, api_secret, timestamp)
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}{request_path}",
        data=body_text.encode("utf-8"),
        method=method,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "KEY": api_key,
            "Timestamp": timestamp,
            "SIGN": signature,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001
        return 0, str(exc)


def build_body(args: argparse.Namespace) -> Dict[str, Any]:
    order_type = args.type.lower()
    if order_type == "limit" and not args.price:
        raise ValueError("LIMIT order requires --price")
    body: Dict[str, Any] = {
        "text": args.text or f"t-manual-{int(time.time() * 1000)}",
        "currency_pair": normalize_currency_pair(args.symbol),
        "type": order_type,
        "side": args.side.lower(),
        "amount": str(args.quantity).strip(),
        "time_in_force": args.time_in_force,
        "account": args.account,
    }
    if args.price:
        body["price"] = str(args.price).strip()
    if args.auto_repay:
        body["auto_repay"] = True
    return body


def main() -> int:
    args = parse_args()
    try:
        body = build_body(args)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print(
        "Gate spot order: "
        f"pair={body['currency_pair']} account={body['account']} side={body['side']} "
        f"type={body['type']} amount={body['amount']} tif={body['time_in_force']} execute={args.execute}"
    )
    print(json.dumps(body, ensure_ascii=False, indent=2, sort_keys=True))

    if not args.execute:
        print("Dry-run only. Re-run with --execute to submit.")
        return 0

    api_key, api_secret = load_credentials()
    status, resp_body = request_gate(
        args.base_url.rstrip("/"),
        "POST",
        "/spot/orders",
        body,
        api_key,
        api_secret,
    )
    tag = "OK" if 200 <= status < 300 else "ERR"
    print(f"Result: {tag} status={status}")
    try:
        parsed = json.loads(resp_body)
        print(json.dumps(parsed, ensure_ascii=False, indent=2, sort_keys=True))
    except json.JSONDecodeError:
        print(resp_body)
    return 0 if 200 <= status < 300 else 1


if __name__ == "__main__":
    raise SystemExit(main())
