#!/usr/bin/env python3
"""Check margin loan/liability APIs."""

import hashlib
import hmac
import json
import os
import time
import urllib.request
import urllib.parse
import urllib.error


def now_ms():
    return int(time.time() * 1000)


def sign(query, secret):
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()


def call_api(base_url, path, api_key, api_secret):
    params = {"timestamp": str(now_ms()), "recvWindow": "5000"}
    query = urllib.parse.urlencode(sorted(params.items()))
    signature = sign(query, api_secret)
    url = f"{base_url}{path}?{query}&signature={signature}"

    req = urllib.request.Request(url, headers={"X-MBX-APIKEY": api_key})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.getcode(), resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def main():
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    base_url = os.environ.get("BINANCE_PAPI_URL", "https://papi.binance.com")

    if not api_key or not api_secret:
        print("ERROR: set BINANCE_API_KEY / BINANCE_API_SECRET")
        return

    # Try different endpoints to find liability info
    endpoints = [
        "/papi/v1/margin/marginLoan",
        "/papi/v1/margin/account",
        "/papi/v1/account",
        "/papi/v1/margin/maxBorrowable",
    ]

    for ep in endpoints:
        print(f"\n=== {ep} ===")
        status, body = call_api(base_url, ep, api_key, api_secret)
        print(f"Status: {status}")
        try:
            data = json.loads(body)
            print(json.dumps(data, indent=2)[:2000])
        except:
            print(body[:500])


if __name__ == "__main__":
    main()
