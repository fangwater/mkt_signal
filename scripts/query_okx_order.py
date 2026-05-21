#!/usr/bin/env python3
"""Query specific OKX order(s) by instId + clOrdId.

Examples:
  source /home/fanghaizhou/okex_mm_alpha/env.sh
  python scripts/query_okx_order.py --inst-id BNB-USDT-SWAP 7126990107311080827

  python scripts/query_okx_order.py --inst-id BNB-USDT-SWAP \
    7126990107311080827 7126990107311080828
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_BASE_URL = os.environ.get("OKX_BASE_URL", "https://openapi.okx.com")


def utc_timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def sign(timestamp: str, method: str, request_path: str, body: str, secret: str) -> str:
    payload = f"{timestamp}{method.upper()}{request_path}{body}"
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def request_okx_private(
    base_url: str,
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
    simulated: bool = False,
) -> Tuple[int, str, Dict[str, str]]:
    method = method.upper()
    params = params or {}
    query = urllib.parse.urlencode(params) if params else ""
    request_path = f"{path}?{query}" if query else path

    timestamp = utc_timestamp()
    signature = sign(timestamp, method, request_path, "", api_secret)

    url = f"{base_url.rstrip('/')}{request_path}"
    req = urllib.request.Request(url, data=None, method=method)
    req.add_header("OK-ACCESS-KEY", api_key)
    req.add_header("OK-ACCESS-SIGN", signature)
    req.add_header("OK-ACCESS-TIMESTAMP", timestamp)
    req.add_header("OK-ACCESS-PASSPHRASE", passphrase)
    req.add_header("Content-Type", "application/json")
    if simulated:
        req.add_header("x-simulated-trading", "1")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read().decode("utf-8", "replace"), dict(resp.headers.items())
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", "replace")
        headers = dict(getattr(exc, "headers", {}).items()) if getattr(exc, "headers", None) else {}
        return exc.code, body_text, headers
    except Exception as exc:  # pragma: no cover
        return 0, str(exc), {}


def load_credentials() -> Tuple[str, str, str]:
    api_key = os.environ.get("OKX_API_KEY", "").strip()
    api_secret = os.environ.get("OKX_API_SECRET", "").strip()
    passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()
    missing = [
        name
        for name, value in [
            ("OKX_API_KEY", api_key),
            ("OKX_API_SECRET", api_secret),
            ("OKX_PASSPHRASE", passphrase),
        ]
        if not value
    ]
    if missing:
        raise SystemExit(f"missing env vars: {', '.join(missing)}")
    return api_key, api_secret, passphrase


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query OKX order(s) with instId + clOrdId",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("cl_ord_ids", nargs="+", help="One or more OKX clOrdId values")
    parser.add_argument("--inst-id", required=True, help="OKX instId, e.g. BNB-USDT-SWAP")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="OKX REST base URL")
    parser.add_argument("--timeout", type=int, default=10, help="HTTP timeout seconds")
    parser.add_argument("--simulated", action="store_true", help="Send x-simulated-trading=1")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON response")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    api_key, api_secret, passphrase = load_credentials()

    for cl_ord_id in args.cl_ord_ids:
        params = {
            "instId": args.inst_id.strip().upper(),
            "clOrdId": str(cl_ord_id).strip(),
        }
        status, body_text, _headers = request_okx_private(
            base_url=args.base_url,
            method="GET",
            path="/api/v5/trade/order",
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            params=params,
            timeout=args.timeout,
            simulated=args.simulated,
        )
        print(f"=== clOrdId={cl_ord_id} http_status={status} ===")
        if args.pretty:
            try:
                parsed = json.loads(body_text)
            except json.JSONDecodeError:
                print(body_text)
            else:
                print(json.dumps(parsed, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(body_text)


if __name__ == "__main__":
    main()
