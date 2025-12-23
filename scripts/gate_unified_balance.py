#!/usr/bin/env python3
"""获取 Gate.io 统一账户余额快照。

用法：
  export GATE_API_KEY="your_api_key"
  export GATE_API_SECRET="your_api_secret"
  python scripts/gate_unified_balance.py
"""

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
    """生成 Gate.io REST API 签名。"""
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
        print(f"请设置环境变量: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret


def main():
    api_key, api_secret = load_credentials()

    url = "/unified/accounts"
    query_param = ""
    body = ""

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    sign_headers = gen_sign("GET", PREFIX + url, query_param, body, api_key, api_secret)
    headers.update(sign_headers)

    full_url = HOST + PREFIX + url
    print(f"GET {full_url}")
    print("-" * 50)

    r = requests.get(full_url, headers=headers)
    print(f"Status: {r.status_code}")
    print(json.dumps(r.json(), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
