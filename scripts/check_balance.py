#!/usr/bin/env python3
"""Check /papi/v1/balance API response for crossMarginBorrowed field."""

import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.request
import urllib.parse


def now_ms():
    return int(time.time() * 1000)


def sign(query, secret):
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()


def fetch_balance(url, api_key):
    req = urllib.request.Request(url, headers={"X-MBX-APIKEY": api_key})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode(errors="replace")
            return True, resp.status, body, None
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        return False, exc.code, body, f"HTTPError: {exc.code} {exc.reason}"
    except urllib.error.URLError as exc:
        return False, None, "", f"URLError: {exc.reason}"


def main():
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    base_url = os.environ.get("BINANCE_PAPI_URL", "https://papi.binance.com")

    if not api_key or not api_secret:
        print("ERROR: set BINANCE_API_KEY / BINANCE_API_SECRET")
        return

    params = {"timestamp": str(now_ms()), "recvWindow": "5000"}
    query = urllib.parse.urlencode(sorted(params.items()))
    signature = sign(query, api_secret)
    url = f"{base_url}/papi/v1/balance?{query}&signature={signature}"

    success, status, body, error = fetch_balance(url, api_key)
    status_text = status if status is not None else "N/A"
    print(f"Request success: {success} (status={status_text})")
    if error:
        print(f"Request error: {error}")
    print("Raw response:")
    print(body)
    if not success:
        return

    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        print(f"ERROR: invalid JSON response: {exc}")
        return

    print("=== Assets with crossMarginBorrowed > 0 ===")
    for item in data:
        borrowed = float(item.get("crossMarginBorrowed", "0"))
        if borrowed > 0:
            print(json.dumps(item, indent=2))

    print("\n=== SOL balance (full response) ===")
    for item in data:
        if item.get("asset") == "SOL":
            print(json.dumps(item, indent=2))

    print("\n=== All assets with any non-zero margin field ===")
    for item in data:
        asset = item.get("asset")
        if asset in ["SOL", "SUI"]:
            print(f"{asset}: {json.dumps(item, indent=2)}")


if __name__ == "__main__":
    main()
