#!/usr/bin/env python3
"""Set Binance UM dual-side (hedge) position mode for the whole account.

Usage:
  python scripts/set_um_dual_side.py [--base-url ...] [--disable]

The ``/papi/v1/um/positionSide/dual`` endpoint acts on the entire UM account, so
this helper simply toggles the mode once per invocation.

Environment variables:
  - ``BINANCE_API_KEY``
  - ``BINANCE_API_SECRET``
"""

import argparse
import hashlib
import hmac
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Toggle Binance UM dual-side (hedge) position mode for the whole account"
    )
    parser.add_argument(
        "--base-url",
        default=(
            os.environ.get("BINANCE_PAPI_URL")
            or os.environ.get("BINANCE_FAPI_URL")
            or "https://papi.binance.com"
        ),
        help="Binance UM REST base url (default: https://papi.binance.com)",
    )
    parser.add_argument(
        "--disable",
        action="store_true",
        help="Disable dual-side (switch back to single-side). Default: enable dual-side.",
    )
    parser.add_argument(
        "--recv-window",
        type=int,
        default=None,
        help="Binance recvWindow in ms (optional; default relies on API default 5000)",
    )
    return parser.parse_args()


def now_ms() -> int:
    return int(time.time() * 1000)


def sign(query: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def post_papi(base_url: str, path: str, params: dict, api_key: str, api_secret: str, timeout: int = 10):
    q = dict(params)
    q.setdefault("recvWindow", "5000")
    q["timestamp"] = str(now_ms())
    items = sorted(q.items(), key=lambda kv: kv[0])
    query = urllib.parse.urlencode(items, safe="-_.~")
    sig = sign(query, api_secret)
    url = f"{base_url}{path}?{query}&signature={sig}"
    req = urllib.request.Request(url, method="POST", headers={"X-MBX-APIKEY": api_key})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
            headers = dict(resp.headers.items())
            return status, body, headers
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        headers = dict(getattr(e, "headers", {}).items()) if getattr(e, "headers", None) else {}
        return e.code, body, headers
    except Exception as e:  # pragma: no cover - network failures
        return 0, str(e), {}


def main():
    args = parse_args()

    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: Please set BINANCE_API_KEY and BINANCE_API_SECRET in environment.")
        sys.exit(1)

    base_url = args.base_url.rstrip("/")
    dual_value = not args.disable
    dual_str = "true" if dual_value else "false"

    params = {"dualSidePosition": dual_str}
    if args.recv_window is not None:
        params["recvWindow"] = str(args.recv_window)

    print(f"Setting dualSidePosition={dual_str} via {base_url} ...")

    status, body, headers = post_papi(
        base_url,
        "/papi/v1/um/positionSide/dual",
        params,
        api_key,
        api_secret,
    )

    used_w = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
    tag = "OK" if 200 <= status < 300 else "ERR"
    print(f"Result: {tag} {status}; used_weight={used_w}")
    if not (200 <= status < 300):
        print(f"body: {body}")


if __name__ == "__main__":
    main()
