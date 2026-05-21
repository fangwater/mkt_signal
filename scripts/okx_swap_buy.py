#!/usr/bin/env python3
"""下 OKX U 本位合约（SWAP/FUTURES）买单的快捷脚本。

示例：
  OKX_API_KEY=... OKX_API_SECRET=... OKX_PASSPHRASE=... \\
    python scripts/okx_swap_buy.py --inst-id BTC-USDT-SWAP --sz 0.001 --ord-type market --execute
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
from typing import Any, Dict, Tuple

DEFAULT_BASE_URL = os.environ.get("OKX_BASE_URL", "https://openapi.okx.com")


def utc_timestamp() -> str:
    """OKX 期望的 ISO8601 毫秒时间戳。"""
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


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
        description="下 OKX U 本位合约买单（SWAP/FUTURES），tdMode 默认 cross",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--inst-id", required=True, help="合约 ID，例如 BTC-USDT-SWAP")
    parser.add_argument("--sz", required=True, help="下单张数/币数，按 OKX 规格填写")
    parser.add_argument(
        "--side",
        choices=["buy", "sell"],
        default="buy",
        help="下单方向，默认 buy",
    )
    parser.add_argument(
        "--pos-side",
        dest="pos_side",
        choices=["long", "short", "net"],
        help="双向持仓需指定 long/short；单向模式可不填或填 net",
    )
    parser.add_argument(
        "--ord-type",
        dest="ord_type",
        choices=["market", "limit", "post_only", "fok", "ioc"],
        default="market",
        help="OKX ordType",
    )
    parser.add_argument("--px", help="价格（限价单必填）")
    parser.add_argument(
        "--td-mode",
        dest="td_mode",
        choices=["cross", "isolated"],
        default="cross",
        help="cross/isolated",
    )
    parser.add_argument("--cl-ord-id", dest="cl_ord_id", help="自定义订单号（<=32 字符）")
    parser.add_argument("--tag", help="标签/策略名")
    parser.add_argument("--reduce-only", dest="reduce_only", action="store_true", help="只减仓")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="OKX REST 基础 URL")
    parser.add_argument("--timeout", type=int, default=10, help="HTTP 超时秒数")
    parser.add_argument("--simulate", action="store_true", help="x-simulated-trading: 1 纸上交易")
    parser.add_argument("--execute", action="store_true", help="实际下单；未加该参数为 dry-run")
    parser.add_argument("--sleep-sec", type=float, default=0.5, help="下单后查询持仓前的等待秒数")
    return parser.parse_args()


def load_credentials() -> tuple[str, str, str]:
    api_key = os.environ.get("OKX_API_KEY", "").strip()
    api_secret = os.environ.get("OKX_API_SECRET", "").strip()
    passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()
    missing = [name for name, value in [("OKX_API_KEY", api_key), ("OKX_API_SECRET", api_secret), ("OKX_PASSPHRASE", passphrase)] if not value]
    if missing:
        print(f"请设置环境变量: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret, passphrase


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
    if args.pos_side:
        payload["posSide"] = args.pos_side
    if args.cl_ord_id:
        payload["clOrdId"] = args.cl_ord_id.strip()
    if args.tag:
        payload["tag"] = args.tag.strip()
    if args.reduce_only:
        payload["reduceOnly"] = True
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


def main() -> None:
    args = parse_args()
    api_key, api_secret, passphrase = load_credentials()
    base_url = args.base_url.rstrip("/")

    order_payload = build_order_payload(args)
    print("Order payload:")
    print(json.dumps(order_payload, ensure_ascii=False, indent=2, sort_keys=True))

    if args.ord_type != "market" and not args.px:
        print("限价/冰山/IOC/FOK/只做Maker需提供 --px", file=sys.stderr)
        sys.exit(1)

    if not args.execute:
        print("Dry-run：未发送下单请求，添加 --execute 才会真正下单。")
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
        print(
            f"Order result: {tag} http={status}; reqId={headers.get('x-request-id')}"
        )
        pretty_print_json("Order response:", body)
        if not order_ok and okx_brief:
            print(f"Order rejected by OKX: {okx_brief}", file=sys.stderr)

    if args.execute and order_ok and args.sleep_sec > 0:
        time.sleep(args.sleep_sec)

    if args.execute:
        # 查询持仓
        pos_params: Dict[str, Any] = {"instId": args.inst_id}
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
        if not order_ok:
            sys.exit(1)


if __name__ == "__main__":
    main()
