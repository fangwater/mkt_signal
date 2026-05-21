#!/usr/bin/env python3
"""Query OKX SWAP open orders and optionally cancel them.

Defaults to REAL trading. Add --simulate for paper.

Examples:
  OKX_API_KEY=... OKX_API_SECRET=... OKX_PASSPHRASE=... \
    python scripts/okx_swap_open_orders.py

  OKX_API_KEY=... OKX_API_SECRET=... OKX_PASSPHRASE=... \
    python scripts/okx_swap_open_orders.py --cancel
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
MAX_BATCH = 20


def utc_timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def json_body(data: Any) -> str:
    if data is None:
        return ""
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


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
    body: Any = None,
    timeout: int = 10,
    simulated: bool = False,
) -> Tuple[int, str, Dict[str, str]]:
    method = method.upper()
    params = params or {}
    query = urllib.parse.urlencode(params) if params else ""
    request_path = f"{path}?{query}" if query else path

    body_str = "" if method == "GET" else json_body(body)
    timestamp = utc_timestamp()
    signature = sign(timestamp, method, request_path, body_str, api_secret)

    url = f"{base_url.rstrip('/')}{request_path}"
    data = None if method == "GET" else body_str.encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("OK-ACCESS-KEY", api_key)
    req.add_header("OK-ACCESS-SIGN", signature)
    req.add_header("OK-ACCESS-TIMESTAMP", timestamp)
    req.add_header("OK-ACCESS-PASSPHRASE", passphrase)
    req.add_header("Content-Type", "application/json")
    if simulated:
        req.add_header("x-simulated-trading", "1")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            body_text = resp.read().decode("utf-8", "replace")
            headers = dict(resp.headers.items())
            return status, body_text, headers
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", "replace")
        headers = dict(getattr(exc, "headers", {}).items()) if getattr(exc, "headers", None) else {}
        return exc.code, body_text, headers
    except Exception as exc:  # pragma: no cover - network failure
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
        print(f"ERROR: missing env vars: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret, passphrase


def parse_okx_body(body_text: str) -> Tuple[bool, str, str, List[Dict[str, Any]]]:
    try:
        parsed = json.loads(body_text)
    except json.JSONDecodeError:
        return False, "non-json", "", []
    code = str(parsed.get("code", "")).strip()
    msg = str(parsed.get("msg", "")).strip()
    data = parsed.get("data")
    if not isinstance(data, list):
        data = []
    return code == "0", code, msg, data


def fetch_open_orders(
    base_url: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    inst_id: Optional[str],
    limit: int,
    after: Optional[str],
    timeout: int,
    simulated: bool,
) -> Tuple[List[Dict[str, Any]], Optional[str], bool]:
    params: Dict[str, Any] = {"instType": "SWAP", "limit": str(limit)}
    if inst_id:
        params["instId"] = inst_id
    if after:
        params["after"] = after

    status, body_text, _headers = request_okx_private(
        base_url=base_url,
        method="GET",
        path="/api/v5/trade/orders-pending",
        api_key=api_key,
        api_secret=api_secret,
        passphrase=passphrase,
        params=params,
        timeout=timeout,
        simulated=simulated,
    )
    ok, code, msg, data = parse_okx_body(body_text)
    if not ok:
        print(
            f"Query failed: http={status} code={code} msg={msg} body={body_text}",
            file=sys.stderr,
        )
        return [], None, False
    next_after = data[-1].get("ordId") if data else None
    return data, str(next_after) if next_after else None, True


def format_order(order: Dict[str, Any]) -> str:
    inst_id = order.get("instId", "")
    ord_id = order.get("ordId", "")
    side = order.get("side", "")
    px = order.get("px", "")
    sz = order.get("sz", "")
    state = order.get("state", "")
    cl_ord_id = order.get("clOrdId", "")
    extra = f" clOrdId={cl_ord_id}" if cl_ord_id else ""
    return f"{inst_id} ordId={ord_id} side={side} px={px} sz={sz} state={state}{extra}"


def chunked(items: List[Dict[str, Any]], size: int) -> List[List[Dict[str, Any]]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def cancel_orders(
    base_url: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    orders: List[Dict[str, Any]],
    timeout: int,
    simulated: bool,
) -> None:
    for batch in chunked(orders, MAX_BATCH):
        payload: List[Dict[str, Any]] = []
        for order in batch:
            inst_id = order.get("instId")
            ord_id = order.get("ordId")
            if not inst_id or not ord_id:
                continue
            payload.append({"instId": inst_id, "ordId": ord_id})
        if not payload:
            continue

        status, body_text, _headers = request_okx_private(
            base_url=base_url,
            method="POST",
            path="/api/v5/trade/cancel-batch-orders",
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            body=payload,
            timeout=timeout,
            simulated=simulated,
        )
        ok, code, msg, data = parse_okx_body(body_text)
        tag = "OK" if ok and 200 <= status < 300 else "ERR"
        print(f"Cancel batch {tag}: http={status} code={code} msg={msg}")
        if not ok:
            print(body_text)
            continue
        for item in data:
            inst_id = item.get("instId", "")
            ord_id = item.get("ordId", "")
            s_code = item.get("sCode", "")
            s_msg = item.get("sMsg", "")
            s_tag = "OK" if str(s_code) in ("", "0") else "ERR"
            print(f"  - {s_tag} {inst_id} ordId={ord_id} sCode={s_code} sMsg={s_msg}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query OKX SWAP open orders and optionally cancel them",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--inst-id", dest="inst_id", help="Optional instId filter, e.g. BTC-USDT-SWAP")
    parser.add_argument("--limit", type=int, default=100, help="Orders per request (1-100)")
    parser.add_argument("--fetch-all", action="store_true", help="Paginate until no more orders")
    parser.add_argument("--max-pages", type=int, default=10, help="Max pages when using --fetch-all")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="OKX REST base URL")
    parser.add_argument("--timeout", type=int, default=10, help="HTTP timeout seconds")
    parser.add_argument("--simulate", action="store_true", help="Send x-simulated-trading: 1 (paper). Default: real")
    parser.add_argument("--cancel", action="store_true", help="Cancel all open orders returned")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.limit < 1 or args.limit > 100:
        print("ERROR: --limit must be between 1 and 100", file=sys.stderr)
        sys.exit(1)

    api_key, api_secret, passphrase = load_credentials()
    base_url = args.base_url.rstrip("/")
    simulated = args.simulate

    inst_id = args.inst_id.strip().upper() if args.inst_id else None

    orders: List[Dict[str, Any]] = []
    after = None
    pages = 0
    while True:
        batch, next_after, ok = fetch_open_orders(
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            inst_id=inst_id,
            limit=args.limit,
            after=after,
            timeout=args.timeout,
            simulated=simulated,
        )
        if not ok:
            sys.exit(2)
        orders.extend(batch)
        pages += 1
        if not args.fetch_all or not batch:
            break
        if next_after is None or next_after == after:
            break
        if args.max_pages and pages >= args.max_pages:
            break
        after = next_after

    print(f"Open orders count: {len(orders)}")
    for order in orders:
        print(f"  - {format_order(order)}")

    if not args.cancel:
        print("Skip cancel: pass --cancel to cancel all open orders.")
        return

    if not orders:
        print("No open orders to cancel.")
        return

    cancel_orders(
        base_url=base_url,
        api_key=api_key,
        api_secret=api_secret,
        passphrase=passphrase,
        orders=orders,
        timeout=args.timeout,
        simulated=simulated,
    )


if __name__ == "__main__":
    main()
