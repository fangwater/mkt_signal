#!/usr/bin/env python3
"""检查 Binance 账户模式（UNIFIED / STANDARD）。

规则：
  1) 先请求 PAPI /papi/v1/um/account，成功则视为 UNIFIED（或返回 accountType）。
  2) 如果 PAPI 失败，再请求 FAPI /fapi/v2/account，成功则视为 STANDARD。
  3) 两者都失败则输出错误并退出。
"""

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
from typing import Any, Dict, Optional, Tuple


def now_ms() -> int:
    return int(time.time() * 1000)


def sign(query: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def signed_get(
    base_url: str,
    path: str,
    params: Dict[str, Any],
    api_key: str,
    api_secret: str,
    timeout: int,
) -> Tuple[int, str, Dict[str, str]]:
    q = dict(params)
    q.setdefault("recvWindow", "5000")
    q["timestamp"] = str(now_ms())
    items = sorted((k, str(v)) for k, v in q.items())
    query = urllib.parse.urlencode(items, safe="-_.~")
    sig = sign(query, api_secret)
    url = f"{base_url}{path}?{query}&signature={sig}"
    req = urllib.request.Request(url, method="GET", headers={"X-MBX-APIKEY": api_key})
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
    except Exception as exc:
        return 0, str(exc), {}


def parse_account_type(account_data: Dict[str, Any]) -> Optional[str]:
    visited: set[int] = set()

    def _search(obj: Any, depth: int = 0) -> Optional[str]:
        if depth > 6:
            return None
        obj_id = id(obj)
        if obj_id in visited:
            return None
        visited.add(obj_id)

        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "accountType" and isinstance(value, str) and value.strip():
                    return value.strip().upper()
                if key in {"portfolioMargin", "isPortfolioMargin", "portfolioMarginAccount"}:
                    if isinstance(value, bool) and value:
                        return "PORTFOLIO"
                res = _search(value, depth + 1)
                if res:
                    return res
        elif isinstance(obj, list):
            for item in obj:
                res = _search(item, depth + 1)
                if res:
                    return res
        return None

    return _search(account_data)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="检查 Binance 账户模式（UNIFIED / STANDARD）")
    parser.add_argument(
        "--papi-url",
        default=os.environ.get("BINANCE_PAPI_URL", "https://papi.binance.com"),
        help="Binance PAPI 基础地址（默认：https://papi.binance.com）",
    )
    parser.add_argument(
        "--fapi-url",
        default=os.environ.get("BINANCE_FAPI_URL", "https://fapi.binance.com"),
        help="Binance FAPI 基础地址（默认：https://fapi.binance.com）",
    )
    parser.add_argument(
        "--recv-window",
        type=int,
        default=None,
        help="自定义 recvWindow（毫秒）。不指定时使用 Binance 默认 5000。",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="HTTP 超时秒数（默认：10）",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: 请在环境变量中设置 BINANCE_API_KEY 与 BINANCE_API_SECRET。", file=sys.stderr)
        sys.exit(1)

    params: Dict[str, Any] = {}
    if args.recv_window is not None:
        params["recvWindow"] = str(args.recv_window)

    papi_url = args.papi_url.rstrip("/")
    fapi_url = args.fapi_url.rstrip("/")

    status, body, headers = signed_get(
        papi_url, "/papi/v1/um/account", params, api_key, api_secret, args.timeout
    )
    if 200 <= status < 300:
        try:
            data = json.loads(body)
        except json.JSONDecodeError as exc:
            print(f"ERROR: PAPI 响应 JSON 解析失败: {exc}", file=sys.stderr)
            print(body)
            sys.exit(1)
        account_type = parse_account_type(data) or "UNIFIED"
        if account_type == "PORTFOLIO":
            mode = "UNIFIED"
            extra = " (PORTFOLIO)"
        else:
            mode = "UNIFIED" if account_type == "UNIFIED" else account_type
            extra = ""
        print(f"检测结果: {mode}{extra} (via PAPI)")
        print(f"建议: export BINANCE_ACCOUNT_MODE={mode}")
        return

    status2, body2, _ = signed_get(
        fapi_url, "/fapi/v2/account", params, api_key, api_secret, args.timeout
    )
    if 200 <= status2 < 300:
        print("检测结果: STANDARD (via FAPI)")
        print("建议: export BINANCE_ACCOUNT_MODE=STANDARD")
        return

    print("ERROR: 无法判定 Binance 账户模式。", file=sys.stderr)
    print(f"PAPI status={status} body={body}", file=sys.stderr)
    print(f"FAPI status={status2} body={body2}", file=sys.stderr)
    sys.exit(2)


if __name__ == "__main__":
    main()
