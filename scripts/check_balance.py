#!/usr/bin/env python3
"""Check Binance balance APIs for UNIFIED / STANDARD accounts.

默认行为：
  1) 若 --mode 未指定，则优先使用环境变量 BINANCE_ACCOUNT_MODE；
  2) 若环境变量也未设置，则自动探测（PAPI -> FAPI）；
  3) UNIFIED 走 /papi/v1/balance，STANDARD 走 /fapi/v2/balance。
"""

import argparse
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple


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
) -> Tuple[bool, int, str, Optional[str]]:
    q = dict(params)
    q.setdefault("recvWindow", "5000")
    q["timestamp"] = str(now_ms())
    query = urllib.parse.urlencode(sorted((k, str(v)) for k, v in q.items()), safe="-_.~")
    signature = sign(query, api_secret)
    url = f"{base_url.rstrip('/')}{path}?{query}&signature={signature}"

    req = urllib.request.Request(url, method="GET", headers={"X-MBX-APIKEY": api_key})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return True, resp.getcode(), body, None
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return False, exc.code, body, f"HTTPError: {exc.code} {exc.reason}"
    except urllib.error.URLError as exc:
        return False, 0, "", f"URLError: {exc.reason}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="检查 Binance 余额（支持 UNIFIED / STANDARD）")
    parser.add_argument(
        "--mode",
        choices=["UNIFIED", "STANDARD", "AUTO"],
        default="AUTO",
        help="账户模式；AUTO 时按 BINANCE_ACCOUNT_MODE 或自动探测",
    )
    parser.add_argument(
        "--papi-url",
        default=os.environ.get("BINANCE_PAPI_URL", "https://papi.binance.com"),
        help="Binance PAPI 基础地址",
    )
    parser.add_argument(
        "--fapi-url",
        default=os.environ.get("BINANCE_FAPI_URL", "https://fapi.binance.com"),
        help="Binance FAPI 基础地址",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="HTTP 超时秒数",
    )
    parser.add_argument(
        "--recv-window",
        type=int,
        default=5000,
        help="recvWindow（毫秒）",
    )
    parser.add_argument(
        "--asset",
        default="SOL",
        help="打印该资产的完整余额条目（默认 SOL）",
    )
    return parser.parse_args()


def normalize_mode(value: str) -> str:
    v = value.strip().upper()
    if v in {"UNIFIED", "STANDARD", "AUTO"}:
        return v
    return "AUTO"


def detect_mode(
    api_key: str,
    api_secret: str,
    papi_url: str,
    fapi_url: str,
    timeout: int,
    recv_window: int,
) -> Tuple[Optional[str], str]:
    params = {"recvWindow": str(recv_window)}
    ok_papi, status_papi, body_papi, _ = signed_get(
        papi_url, "/papi/v1/um/account", params, api_key, api_secret, timeout
    )
    if ok_papi and 200 <= status_papi < 300:
        return "UNIFIED", "via PAPI"

    ok_fapi, status_fapi, body_fapi, _ = signed_get(
        fapi_url, "/fapi/v2/account", params, api_key, api_secret, timeout
    )
    if ok_fapi and 200 <= status_fapi < 300:
        return "STANDARD", "via FAPI"

    return None, (
        f"PAPI status={status_papi} body={body_papi}; "
        f"FAPI status={status_fapi} body={body_fapi}"
    )


def parse_json_list(body: str) -> Optional[List[Dict[str, Any]]]:
    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        print(f"ERROR: invalid JSON response: {exc}")
        return None
    if not isinstance(data, list):
        print("ERROR: response is not a JSON array")
        print(body)
        return None
    return [item for item in data if isinstance(item, dict)]


def print_unified_view(items: List[Dict[str, Any]], asset: str) -> None:
    target = asset.upper()

    print("=== UNIFIED: Assets with crossMarginBorrowed > 0 ===")
    for item in items:
        try:
            borrowed = float(item.get("crossMarginBorrowed", "0"))
        except (TypeError, ValueError):
            borrowed = 0.0
        if borrowed > 0:
            print(json.dumps(item, indent=2, ensure_ascii=False))

    print(f"\n=== UNIFIED: {target} balance (full response) ===")
    found_target = False
    for item in items:
        if str(item.get("asset", "")).upper() == target:
            found_target = True
            print(json.dumps(item, indent=2, ensure_ascii=False))
    if not found_target:
        print(f"{target} not found")

    print("\n=== UNIFIED: Selected non-zero assets (SOL/SUI) ===")
    for item in items:
        symbol = str(item.get("asset", "")).upper()
        if symbol in {"SOL", "SUI"}:
            print(f"{symbol}: {json.dumps(item, indent=2, ensure_ascii=False)}")


def print_standard_view(items: List[Dict[str, Any]], asset: str) -> None:
    target = asset.upper()

    print("=== STANDARD: Assets with non-zero balance/crossWalletBalance ===")
    for item in items:
        try:
            balance = float(item.get("balance", "0"))
        except (TypeError, ValueError):
            balance = 0.0
        try:
            cross_wallet = float(item.get("crossWalletBalance", "0"))
        except (TypeError, ValueError):
            cross_wallet = 0.0
        if balance != 0.0 or cross_wallet != 0.0:
            print(json.dumps(item, indent=2, ensure_ascii=False))

    print(f"\n=== STANDARD: {target} balance (full response) ===")
    found_target = False
    for item in items:
        if str(item.get("asset", "")).upper() == target:
            found_target = True
            print(json.dumps(item, indent=2, ensure_ascii=False))
    if not found_target:
        print(f"{target} not found")


def main() -> None:
    args = parse_args()

    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: set BINANCE_API_KEY / BINANCE_API_SECRET")
        return

    mode_arg = normalize_mode(args.mode)
    mode_env = normalize_mode(os.environ.get("BINANCE_ACCOUNT_MODE", ""))
    mode = mode_arg
    mode_source = "--mode"
    if mode == "AUTO" and mode_env in {"UNIFIED", "STANDARD"}:
        mode = mode_env
        mode_source = "BINANCE_ACCOUNT_MODE"

    if mode == "AUTO":
        detected_mode, reason = detect_mode(
            api_key=api_key,
            api_secret=api_secret,
            papi_url=args.papi_url,
            fapi_url=args.fapi_url,
            timeout=args.timeout,
            recv_window=args.recv_window,
        )
        if detected_mode is None:
            print("ERROR: failed to detect account mode")
            print(reason)
            return
        mode = detected_mode
        mode_source = reason

    if mode == "UNIFIED":
        base_url = args.papi_url
        path = "/papi/v1/balance"
    else:
        base_url = args.fapi_url
        path = "/fapi/v2/balance"

    params = {"recvWindow": str(args.recv_window)}
    success, status, body, error = signed_get(base_url, path, params, api_key, api_secret, args.timeout)

    status_text = status if status else "N/A"
    print(f"Mode: {mode} ({mode_source})")
    print(f"Endpoint: {base_url.rstrip('/')}{path}")
    print(f"Request success: {success} (status={status_text})")
    if error:
        print(f"Request error: {error}")
    print("Raw response:")
    print(body)
    if not success:
        return

    items = parse_json_list(body)
    if items is None:
        return

    if mode == "UNIFIED":
        print_unified_view(items, args.asset)
    else:
        print_standard_view(items, args.asset)


if __name__ == "__main__":
    main()
