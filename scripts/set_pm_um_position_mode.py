#!/usr/bin/env python3
"""Set Binance Portfolio Margin UM position mode to BOTH (single-side).

The Portfolio Margin API exposes ``/papi/v1/um/positionSide/dual`` to configure
UM position mode for the entire account. This helper script sets the mode to
single-side (BOTH) by default, or toggles to HEDGE when ``--mode HEDGE`` is
specified.

Usage example:
  BINANCE_API_KEY=... BINANCE_API_SECRET=... \\
    python scripts/set_pm_um_position_mode.py --mode BOTH
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, Tuple

DEFAULT_BASE_URL = (
    os.environ.get("BINANCE_PAPI_URL")
    or os.environ.get("BINANCE_FAPI_URL")
    or "https://papi.binance.com"
)

POSITION_MODE_PATH = "/papi/v1/um/positionSide/dual"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="设置 Binance Portfolio Margin UM 持仓模式（默认 BOTH 单向持仓）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="REST API 地址，默认 https://papi.binance.com",
    )
    parser.add_argument(
        "--mode",
        choices=["BOTH", "HEDGE"],
        default="BOTH",
        help="目标 UM 持仓模式，BOTH=单向，HEDGE=双向",
    )
    parser.add_argument(
        "--recv-window",
        type=int,
        dest="recv_window",
        default=5000,
        help="可选 recvWindow（毫秒）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅查询当前模式并显示是否需要修改，不发送修改请求",
    )
    return parser.parse_args()


def now_ms() -> int:
    return int(time.time() * 1000)


def sign(query: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def request_signed(
    base_url: str,
    path: str,
    params: Dict[str, str],
    api_key: str,
    api_secret: str,
    method: str = "GET",
    timeout: int = 10,
) -> Tuple[int, str, Dict[str, str]]:
    q = dict(params)
    q.setdefault("recvWindow", "5000")
    q["timestamp"] = str(now_ms())
    query = urllib.parse.urlencode(sorted(q.items(), key=lambda kv: kv[0]), safe="-_.~")
    sig = sign(query, api_secret)
    url = f"{base_url}{path}?{query}&signature={sig}"
    req = urllib.request.Request(url, method=method, headers={"X-MBX-APIKEY": api_key})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
            headers = dict(resp.headers.items())
            return status, body, headers
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        headers = dict(getattr(exc, "headers", {}).items()) if getattr(exc, "headers", None) else {}
        return exc.code, body, headers
    except Exception as exc:  # pragma: no cover - network errors
        return 0, str(exc), {}


def ensure_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("请先在环境变量中设置 BINANCE_API_KEY / BINANCE_API_SECRET。", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret


def show_response(label: str, body: str) -> None:
    try:
        parsed = json.loads(body)
        formatted = json.dumps(parsed, ensure_ascii=False, indent=2, sort_keys=True)
        print(f"{label}:\n{formatted}")
    except json.JSONDecodeError:
        print(f"{label}: {body}")


def main() -> None:
    args = parse_args()
    api_key, api_secret = ensure_credentials()
    base_url = args.base_url.rstrip("/")

    # 先查询当前模式
    status, body, _ = request_signed(
        base_url,
        POSITION_MODE_PATH,
        {"recvWindow": str(args.recv_window)},
        api_key,
        api_secret,
        method="GET",
    )
    if status != 200:
        print(f"查询持仓模式失败 status={status} body={body}", file=sys.stderr)
        sys.exit(1)

    data = json.loads(body)
    dual_side = bool(data.get("dualSidePosition"))
    current_mode = "HEDGE" if dual_side else "BOTH"
    print(f"当前持仓模式: {current_mode}")

    if args.dry_run:
        print("Dry-run 模式，未发送修改请求。")
        return

    if current_mode == args.mode:
        print("当前模式已符合期望，无需修改。")
        return

    target_dual = "true" if args.mode == "HEDGE" else "false"
    print(f"准备将 UM 持仓模式设置为 {args.mode}（dualSidePosition={target_dual}) ...")
    status, body, headers = request_signed(
        base_url,
        POSITION_MODE_PATH,
        {"dualSidePosition": target_dual, "recvWindow": str(args.recv_window)},
        api_key,
        api_secret,
        method="POST",
    )
    used_weight = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
    if not (200 <= status < 300):
        print(f"设置失败 status={status} body={body}", file=sys.stderr)
        sys.exit(1)
    print(f"设置成功，used_weight={used_weight}")
    show_response("API 响应", body)


if __name__ == "__main__":
    main()
