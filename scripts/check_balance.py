#!/usr/bin/env python3
"""Check balance APIs for Binance and Gate.

默认行为：
  - Binance:
    1) 若 --mode 未指定，则优先使用环境变量 BINANCE_ACCOUNT_MODE；
    2) 若环境变量也未设置，则自动探测（PAPI -> FAPI）；
    3) UNIFIED 走 /papi/v1/balance，STANDARD 走 /fapi/v2/balance。
  - Gate:
    1) 当前仅支持统一账户（UNIFIED）；
    2) 查询 /api/v4/unified/accounts。
"""

from __future__ import annotations

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


def sign_binance(query: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def signed_get_binance(
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
    signature = sign_binance(query, api_secret)
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


def sign_gate(method: str, signed_path: str, query: str, body: str, secret: str, timestamp: str) -> str:
    body_hash = hashlib.sha512(body.encode("utf-8")).hexdigest()
    signing_payload = f"{method}\n{signed_path}\n{query}\n{body_hash}\n{timestamp}"
    return hmac.new(secret.encode("utf-8"), signing_payload.encode("utf-8"), hashlib.sha512).hexdigest()


def signed_get_gate(
    base_url: str,
    api_prefix: str,
    path: str,
    params: Dict[str, Any],
    api_key: str,
    api_secret: str,
    timeout: int,
) -> Tuple[bool, int, str, Optional[str], str]:
    query = urllib.parse.urlencode(sorted((k, str(v)) for k, v in params.items()), safe="-_.~")
    normalized_prefix = "/" + api_prefix.strip("/")
    signed_path = f"{normalized_prefix}{path}"
    timestamp = str(int(time.time()))
    signature = sign_gate("GET", signed_path, query, "", api_secret, timestamp)

    url = f"{base_url.rstrip('/')}{signed_path}"
    if query:
        url = f"{url}?{query}"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "KEY": api_key,
            "Timestamp": timestamp,
            "SIGN": signature,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return True, resp.getcode(), body, None, signed_path
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return False, exc.code, body, f"HTTPError: {exc.code} {exc.reason}", signed_path
    except urllib.error.URLError as exc:
        return False, 0, "", f"URLError: {exc.reason}", signed_path


def parse_args() -> argparse.Namespace:
    default_exchange = os.environ.get("CHECK_BALANCE_EXCHANGE", "binance").strip().lower()
    if default_exchange not in {"binance", "gate"}:
        default_exchange = "binance"

    parser = argparse.ArgumentParser(description="检查 Binance / Gate 余额")
    parser.add_argument(
        "--exchange",
        choices=["binance", "gate"],
        default=default_exchange,
        help="交易所（默认 binance）",
    )
    parser.add_argument(
        "--mode",
        choices=["UNIFIED", "STANDARD", "AUTO"],
        default=None,
        help="账户模式；默认 binance=AUTO、gate=UNIFIED。Gate 仅接受 UNIFIED",
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
        "--gate-url",
        default=os.environ.get("GATE_API_URL", "https://api.gateio.ws"),
        help="Gate API 基础地址",
    )
    parser.add_argument(
        "--gate-prefix",
        default=os.environ.get("GATE_API_PREFIX", "/api/v4"),
        help="Gate API 前缀路径",
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
        help="Binance recvWindow（毫秒）",
    )
    parser.add_argument(
        "--asset",
        default="SOL",
        help="打印该资产的完整余额条目（默认 SOL）",
    )
    return parser.parse_args()


def normalize_mode(value: Optional[str]) -> str:
    if value is None:
        return "AUTO"
    v = value.strip().upper()
    if v in {"UNIFIED", "STANDARD", "AUTO"}:
        return v
    return "AUTO"


def detect_binance_mode(
    api_key: str,
    api_secret: str,
    papi_url: str,
    fapi_url: str,
    timeout: int,
    recv_window: int,
) -> Tuple[Optional[str], str]:
    params = {"recvWindow": str(recv_window)}
    ok_papi, status_papi, body_papi, _ = signed_get_binance(
        papi_url, "/papi/v1/um/account", params, api_key, api_secret, timeout
    )
    if ok_papi and 200 <= status_papi < 300:
        return "UNIFIED", "via PAPI"

    ok_fapi, status_fapi, body_fapi, _ = signed_get_binance(
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


def parse_json_any(body: str) -> Optional[Any]:
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        print(f"ERROR: invalid JSON response: {exc}")
        return None


def to_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def find_gate_row_symbol(row: Dict[str, Any]) -> Optional[str]:
    for key in ("currency", "ccy", "asset", "symbol", "coin"):
        raw = row.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip().upper()
    return None


def estimate_gate_row_balance(row: Dict[str, Any]) -> Optional[float]:
    for key in ("total", "equity", "eq", "balance", "available_balance", "available", "avail"):
        value = to_float(row.get(key))
        if value is not None:
            return value
    available = to_float(row.get("available"))
    if available is None:
        available = to_float(row.get("avail"))
    locked = to_float(row.get("locked"))
    if locked is None:
        locked = to_float(row.get("freeze"))
    if locked is None:
        locked = to_float(row.get("frozen"))
    if available is not None and locked is not None:
        return available + locked
    return None


def extract_gate_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    candidates = [
        payload.get("data"),
        payload.get("result"),
        payload.get("accounts"),
    ]
    nested_data = payload.get("data")
    if isinstance(nested_data, dict):
        candidates.append(nested_data.get("accounts"))

    for candidate in candidates:
        if isinstance(candidate, list):
            return [item for item in candidate if isinstance(item, dict)]
    return []


def extract_gate_balance_map(payload: Any) -> Optional[Dict[str, Any]]:
    if isinstance(payload, dict):
        balances = payload.get("balances")
        if isinstance(balances, dict):
            return balances
    return None


def print_unified_view(items: List[Dict[str, Any]], asset: str) -> None:
    target = asset.upper()

    print("=== UNIFIED: Assets with crossMarginBorrowed > 0 ===")
    for item in items:
        borrowed = to_float(item.get("crossMarginBorrowed")) or 0.0
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
        balance = to_float(item.get("balance")) or 0.0
        cross_wallet = to_float(item.get("crossWalletBalance")) or 0.0
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


def print_gate_unified_view(payload: Any, asset: str) -> None:
    target = asset.upper()
    rows = extract_gate_rows(payload)
    balance_map = extract_gate_balance_map(payload)
    printed_any = False

    if rows:
        print("=== GATE UNIFIED: assets with non-zero estimated balance ===")
        found_non_zero = False
        for row in rows:
            symbol = find_gate_row_symbol(row) or "UNKNOWN"
            balance = estimate_gate_row_balance(row)
            if balance is None or balance == 0.0:
                continue
            found_non_zero = True
            printed_any = True
            print(f"{symbol}: est_balance={balance}")
            print(json.dumps(row, indent=2, ensure_ascii=False))
        if not found_non_zero:
            print("none")

        print(f"\n=== GATE UNIFIED: {target} balance (full response) ===")
        found_target = False
        for row in rows:
            symbol = find_gate_row_symbol(row)
            if symbol == target:
                found_target = True
                printed_any = True
                print(json.dumps(row, indent=2, ensure_ascii=False))
        if not found_target:
            print(f"{target} not found")

    if balance_map is not None:
        print("\n=== GATE UNIFIED: balances map with non-zero balance/liability ===")
        found_non_zero = False
        for raw_symbol, detail in balance_map.items():
            if not isinstance(detail, dict):
                continue

            symbol = str(raw_symbol).upper()
            available = to_float(detail.get("available")) or 0.0
            freeze = to_float(detail.get("freeze"))
            if freeze is None:
                freeze = to_float(detail.get("frozen"))
            freeze = freeze or 0.0
            spot_in_use = to_float(detail.get("spot_in_use")) or 0.0

            total_liab = to_float(detail.get("total_liab")) or 0.0
            if total_liab > 0:
                liability = total_liab
            else:
                borrowed = to_float(detail.get("borrowed")) or 0.0
                negative_liab = to_float(detail.get("negative_liab")) or 0.0
                futures_pos_liab = to_float(detail.get("futures_pos_liab")) or 0.0
                liability = borrowed + negative_liab + futures_pos_liab

            approx_balance = available + freeze + spot_in_use
            if approx_balance != 0.0 or liability != 0.0:
                found_non_zero = True
                printed_any = True
                print(f"{symbol}: approx_balance={approx_balance}, liability={liability}")
                print(json.dumps(detail, indent=2, ensure_ascii=False))

        if not found_non_zero:
            print("none")

        print(f"\n=== GATE UNIFIED: {target} balance (balances map) ===")
        target_detail = None
        for raw_symbol, detail in balance_map.items():
            if str(raw_symbol).upper() == target and isinstance(detail, dict):
                target_detail = detail
                break
        if target_detail is None:
            print(f"{target} not found")
        else:
            printed_any = True
            print(json.dumps(target_detail, indent=2, ensure_ascii=False))

    if not printed_any:
        print("无法识别 Gate 返回结构，请直接查看 Raw response。")


def run_binance(args: argparse.Namespace) -> None:
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: set BINANCE_API_KEY / BINANCE_API_SECRET")
        return

    mode_arg = normalize_mode(args.mode or "AUTO")
    mode_env = normalize_mode(os.environ.get("BINANCE_ACCOUNT_MODE", ""))
    mode = mode_arg
    mode_source = "--mode" if args.mode else "default(AUTO)"
    if mode == "AUTO" and mode_env in {"UNIFIED", "STANDARD"}:
        mode = mode_env
        mode_source = "BINANCE_ACCOUNT_MODE"

    if mode == "AUTO":
        detected_mode, reason = detect_binance_mode(
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
    success, status, body, error = signed_get_binance(base_url, path, params, api_key, api_secret, args.timeout)

    status_text = status if status else "N/A"
    print("Exchange: binance")
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


def run_gate(args: argparse.Namespace) -> None:
    api_key = os.environ.get("GATE_API_KEY", "").strip()
    api_secret = os.environ.get("GATE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: set GATE_API_KEY / GATE_API_SECRET")
        return

    mode_arg = normalize_mode(args.mode or "UNIFIED")
    if mode_arg != "UNIFIED":
        print("ERROR: gate only accepts --mode UNIFIED")
        return
    mode_source = "--mode" if args.mode else "default(UNIFIED)"

    success, status, body, error, signed_path = signed_get_gate(
        base_url=args.gate_url,
        api_prefix=args.gate_prefix,
        path="/unified/accounts",
        params={},
        api_key=api_key,
        api_secret=api_secret,
        timeout=args.timeout,
    )

    status_text = status if status else "N/A"
    print("Exchange: gate")
    print(f"Mode: UNIFIED ({mode_source})")
    print(f"Endpoint: {args.gate_url.rstrip('/')}{signed_path}")
    print(f"Request success: {success} (status={status_text})")
    if error:
        print(f"Request error: {error}")
    print("Raw response:")
    print(body)
    if not success:
        return

    payload = parse_json_any(body)
    if payload is None:
        return
    print_gate_unified_view(payload, args.asset)


def main() -> None:
    args = parse_args()
    if args.exchange == "gate":
        run_gate(args)
    else:
        run_binance(args)


if __name__ == "__main__":
    main()
