#!/usr/bin/env python3
"""Fetch Gate.io futures positions (USDT/coin-settled).

Usage:
  export GATE_API_KEY="your_api_key"
  export GATE_API_SECRET="your_api_secret"
  python scripts/gate_futures_positions.py --settle usdt
  python scripts/gate_futures_positions.py --settle usdt --contract APT_USDT
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
    parser = argparse.ArgumentParser(description="Gate.io futures positions.")
    parser.add_argument("--settle", default="usdt", help="Settlement currency (e.g. usdt, btc)")
    parser.add_argument("--contract", default="", help="Optional contract, e.g. APT_USDT")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    api_key, api_secret = load_credentials()

    if args.contract:
        url_path = f"/futures/{args.settle}/positions/{args.contract}"
    else:
        url_path = f"/futures/{args.settle}/positions"
    query_param = ""
    body = ""

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    headers.update(gen_sign("GET", PREFIX + url_path, query_param, body, api_key, api_secret))

    full_url = HOST + PREFIX + url_path
    print(f"GET {full_url}")
    print("-" * 50)

    resp = requests.get(full_url, headers=headers)
    print(f"Status: {resp.status_code}")
    try:
        print(json.dumps(resp.json(), indent=2, ensure_ascii=False))
    except ValueError:
        print(resp.text)


if __name__ == "__main__":
    main()
