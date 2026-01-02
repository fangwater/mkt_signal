#!/usr/bin/env python3
"""Gate.io transfer helper (REST).

Default endpoint is /wallet/transfers. You can override with --path if needed.

Usage:
  export GATE_API_KEY="your_api_key"
  export GATE_API_SECRET="your_api_secret"
  python scripts/gate_transfer.py --currency USDT --amount 100 --from spot --to futures
  python scripts/gate_transfer.py --path /unified/transfer --payload-json '{"currency":"USDT","from":"unified","to":"futures","amount":"100"}'
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import time

import requests

HOST = "https://api.gateio.ws"
PREFIX = "/api/v4"


def gen_sign(method: str, url: str, query_string: str, body: str, api_key: str, api_secret: str) -> dict:
    t = str(int(time.time()))
    hashed_body = hashlib.sha512(body.encode("utf-8")).hexdigest()
    sign_string = f"{method}\n{url}\n{query_string}\n{hashed_body}\n{t}"
    signature = hmac.new(api_secret.encode("utf-8"), sign_string.encode("utf-8"), hashlib.sha512).hexdigest()
    return {
        "KEY": api_key,
        "Timestamp": t,
        "SIGN": signature,
    }


def load_credentials() -> tuple[str, str]:
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gate.io transfer helper.")
    parser.add_argument("--path", default="/wallet/transfers", help="API path, e.g. /wallet/transfers")
    parser.add_argument("--currency", default="", help="Currency code, e.g. USDT")
    parser.add_argument("--amount", default="", help="Amount as string, e.g. 100")
    parser.add_argument("--from", dest="from_account", default="", help="From account, e.g. spot/unified/futures")
    parser.add_argument("--to", dest="to_account", default="", help="To account, e.g. futures/spot")
    parser.add_argument("--currency-pair", default="", help="Optional currency_pair for margin-related transfers")
    parser.add_argument("--payload-json", default="", help="Raw JSON string to override payload")
    return parser.parse_args()


def build_payload(args: argparse.Namespace) -> dict:
    if args.payload_json:
        try:
            payload = json.loads(args.payload_json)
        except json.JSONDecodeError as exc:
            print(f"Invalid --payload-json: {exc}", file=sys.stderr)
            sys.exit(1)
        if not isinstance(payload, dict):
            print("--payload-json must be a JSON object", file=sys.stderr)
            sys.exit(1)
        return payload

    missing = []
    if not args.currency:
        missing.append("--currency")
    if not args.amount:
        missing.append("--amount")
    if not args.from_account:
        missing.append("--from")
    if not args.to_account:
        missing.append("--to")
    if missing:
        print(f"Missing required args: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    payload = {
        "currency": args.currency,
        "amount": str(args.amount),
        "from": args.from_account,
        "to": args.to_account,
    }
    if args.currency_pair:
        payload["currency_pair"] = args.currency_pair
    return payload


def main() -> None:
    args = parse_args()
    api_key, api_secret = load_credentials()

    payload = build_payload(args)
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
    query_param = ""
    url_path = args.path
    if not url_path.startswith("/"):
        url_path = "/" + url_path

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    headers.update(gen_sign("POST", PREFIX + url_path, query_param, body, api_key, api_secret))

    full_url = HOST + PREFIX + url_path
    print(f"POST {full_url}")
    print(f"Payload: {body}")
    print("-" * 50)

    resp = requests.post(full_url, headers=headers, data=body)
    print(f"Status: {resp.status_code}")
    try:
        print(json.dumps(resp.json(), indent=2, ensure_ascii=False))
    except ValueError:
        print(resp.text)


if __name__ == "__main__":
    main()
