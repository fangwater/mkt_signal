#!/usr/bin/env python3
"""Submit an OKX cross-margin order (default sell, can set buy) and query positions.

Examples:
  OKX_API_KEY=... OKX_API_SECRET=... OKX_PASSPHRASE=... \\
    python scripts/okx_margin_sell.py --inst-id BTC-USDT --sz 0.01 --ord-type market --execute
  # buy shortcut
  OKX_API_KEY=... OKX_API_SECRET=... OKX_PASSPHRASE=... \\
    python scripts/okx_margin_sell.py --inst-id BTC-USDT --sz 0.01 --ord-type market --buy --execute
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
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

DEFAULT_BASE_URL = os.environ.get("OKX_BASE_URL", "https://www.okx.com")


def utc_timestamp() -> str:
    """OKX expects ISO8601 with milliseconds, suffixed by Z."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def json_body(data: Dict[str, Any] | None) -> str:
    if not data:
        return ""
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def sign(timestamp: str, method: str, request_path: str, body: str, secret: str) -> str:
    payload = f"{timestamp}{method.upper()}{request_path}{body}"
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def request_okx(
    base_url: str,
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    params: Dict[str, Any] | None = None,
    body: Dict[str, Any] | None = None,
    timeout: int = 10,
    simulated: bool = False,
) -> Tuple[int, str, Dict[str, str]]:
    method = method.upper()
    params = params or {}
    query = urllib.parse.urlencode(params) if params else ""
    request_path = path
    if query:
        request_path = f"{path}?{query}"

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Place an OKX margin order with tdMode=cross and then query /account/positions",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--inst-id", required=True, help="Instrument ID, e.g. BTC-USDT")
    parser.add_argument("--sz", required=True, help="Order size (base currency)")
    parser.add_argument(
        "--side",
        choices=["buy", "sell"],
        default="sell",
        help="Order side, default sell",
    )
    parser.add_argument(
        "--buy",
        action="store_true",
        help="Shortcut for --side buy",
    )
    parser.add_argument(
        "--ord-type",
        dest="ord_type",
        choices=["market", "limit", "post_only", "fok", "ioc"],
        default="market",
        help="OKX ordType, default market",
    )
    parser.add_argument("--px", help="Price, required for limit/post-only/fok/ioc")
    parser.add_argument(
        "--td-mode",
        dest="td_mode",
        choices=["cross", "isolated", "cash", "spot_isolated"],
        default="cross",
        help="Trade mode; cross = cross margin",
    )
    parser.add_argument(
        "--tgt-ccy",
        dest="tgt_ccy",
        choices=["base_ccy", "quote_ccy"],
        help="Target currency for market buys; not needed for sells",
    )
    parser.add_argument(
        "--ccy",
        help="Margin currency (multi-currency margin only, optional)",
    )
    parser.add_argument("--cl-ord-id", dest="cl_ord_id", help="Optional client order ID (<= 32 chars)")
    parser.add_argument("--tag", help="Optional tag, e.g. strategy name")
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="OKX REST base URL",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="HTTP timeout in seconds",
    )
    parser.add_argument(
        "--sleep-sec",
        type=float,
        default=0.5,
        help="Sleep before querying positions (gives API time to settle)",
    )
    parser.add_argument(
        "--simulate",
        action="store_true",
        help="Add x-simulated-trading: 1 header for paper trading",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually send the order; default is dry-run",
    )
    parser.add_argument(
        "--skip-positions",
        action="store_true",
        help="Skip GET /account/positions after the order",
    )
    parser.add_argument(
        "--skip-balance",
        action="store_true",
        help="Skip GET /account/balance after the order",
    )
    parser.add_argument(
        "--positions-inst-type",
        dest="positions_inst_type",
        help="Optional instType filter for /account/positions (e.g. MARGIN)",
    )
    return parser.parse_args()


def load_credentials() -> Tuple[str, str, str]:
    api_key = os.environ.get("OKX_API_KEY", "").strip()
    api_secret = os.environ.get("OKX_API_SECRET", "").strip()
    passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()
    missing = [name for name, value in [("OKX_API_KEY", api_key), ("OKX_API_SECRET", api_secret), ("OKX_PASSPHRASE", passphrase)] if not value]
    if missing:
        print(f"Please set environment variables: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret, passphrase


def preview_env(api_key: str, api_secret: str) -> None:
    ipc_namespace = os.environ.get("IPC_NAMESPACE", "")
    secret_preview = f"{api_secret[:4]}...{api_secret[-4:]}" if len(api_secret) >= 8 else "***"
    print("OKX env set:")
    print(f"  OKX_API_KEY={api_key}")
    print(f"  OKX_API_SECRET={secret_preview}")
    print("  OKX_PASSPHRASE=***")
    if ipc_namespace:
        print(f"  IPC_NAMESPACE={ipc_namespace}")


def build_order_payload(args: argparse.Namespace) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "instId": args.inst_id.strip(),
        "tdMode": args.td_mode,
        "side": args.side,
        "ordType": args.ord_type,
        "sz": str(args.sz).strip(),
    }
    if args.px:
        payload["px"] = str(args.px).strip()
    if args.tgt_ccy:
        payload["tgtCcy"] = args.tgt_ccy
    if args.ccy:
        payload["ccy"] = args.ccy.strip()
    if args.cl_ord_id:
        payload["clOrdId"] = args.cl_ord_id.strip()
    if args.tag:
        payload["tag"] = args.tag.strip()
    return payload


def pretty_print_json(label: str, body: str) -> None:
    print(label)
    try:
        parsed = json.loads(body)
        print(json.dumps(parsed, ensure_ascii=False, indent=2, sort_keys=True))
    except json.JSONDecodeError:
        print(body)


def okx_response_ok(body: str) -> tuple[bool, str]:
    """Return (ok, brief_error). OKX uses HTTP 200 even when `code != "0"`."""
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return False, "non-JSON response body"

    code = str(parsed.get("code", "")).strip()
    msg = str(parsed.get("msg", "")).strip()
    if code == "0":
        return True, ""

    brief = f"code={code} msg={msg}".strip()
    data = parsed.get("data")
    if isinstance(data, list) and data:
        first = data[0] if isinstance(data[0], dict) else None
        if first:
            s_code = str(first.get("sCode", "")).strip()
            s_msg = str(first.get("sMsg", "")).strip()
            if s_code or s_msg:
                brief = f"{brief} sCode={s_code} sMsg={s_msg}".strip()
    return False, brief


def split_inst(inst_id: str) -> Tuple[str | None, str | None]:
    if "-" not in inst_id:
        return None, None
    base, quote = inst_id.split("-", 1)
    return base.upper(), quote.upper()


def main() -> None:
    args = parse_args()
    if args.buy:
        args.side = "buy"
    api_key, api_secret, passphrase = load_credentials()
    base_url = args.base_url.rstrip("/")

    preview_env(api_key, api_secret)
    print(f"Base URL: {base_url}")

    order_payload = build_order_payload(args)
    print("Order payload:")
    print(json.dumps(order_payload, ensure_ascii=False, indent=2, sort_keys=True))

    if args.ord_type != "market" and not args.px:
        print("Limit/post-only/fok/ioc orders require --px.", file=sys.stderr)
        sys.exit(1)

    if not args.execute:
        print("Dry-run only. Re-run with --execute to submit the order.")
    else:
        order_ok = True
        status, body, headers = request_okx(
            base_url,
            "POST",
            "/api/v5/trade/order",
            api_key,
            api_secret,
            passphrase,
            body=order_payload,
            timeout=args.timeout,
            simulated=args.simulate,
        )
        okx_ok, okx_brief = okx_response_ok(body)
        order_ok = (200 <= status < 300) and okx_ok
        tag = "OK" if order_ok else "ERR"
        print(f"Order result: {tag} http={status}; reqId={headers.get('x-request-id')}")
        pretty_print_json("Order response:", body)
        if not order_ok and okx_brief:
            print(f"Order rejected by OKX: {okx_brief}", file=sys.stderr)

    if args.execute and order_ok and args.sleep_sec > 0:
        time.sleep(args.sleep_sec)

    if not args.skip_positions:
        pos_params: Dict[str, Any] = {"instId": args.inst_id}
        if args.positions_inst_type:
            pos_params["instType"] = args.positions_inst_type
        status, body, _ = request_okx(
            base_url,
            "GET",
            "/api/v5/account/positions",
            api_key,
            api_secret,
            passphrase,
            params=pos_params,
            timeout=args.timeout,
            simulated=args.simulate,
        )
        tag = "OK" if status == 200 else "ERR"
        print(f"Positions result: {tag} {status}")
        pretty_print_json("Positions response:", body)
        if status != 200:
            sys.exit(1)
        if args.execute and not order_ok:
            sys.exit(1)

    if not args.skip_balance:
        base_ccy, quote_ccy = split_inst(args.inst_id)
        ccys = [ccy for ccy in (base_ccy, quote_ccy) if ccy]
        if ccys:
            status, body, _ = request_okx(
                base_url,
                "GET",
                "/api/v5/account/balance",
                api_key,
                api_secret,
                passphrase,
                params={"ccy": ",".join(ccys)},
                timeout=args.timeout,
                simulated=args.simulate,
            )
            tag = "OK" if status == 200 else "ERR"
            print(f"Balance result: {tag} {status} (ccy={','.join(ccys)})")
            pretty_print_json("Balance response:", body)
            if status != 200:
                sys.exit(1)


if __name__ == "__main__":
    main()
