#!/usr/bin/env python3
"""Bybit auto-repay endpoint 探针。

支持两个端点：
  - quick-repayment    POST /v5/account/quick-repayment
                       会跨币种用其他资产折价补差（消耗 USDT 等）
  - no-convert-repay   POST /v5/account/no-convert-repay  (推荐)
                       严格只用同币 spot available 抵同币 borrow，不动其他币

流程（针对单个 coin）：
  1. GET /v5/account/wallet-balance  → 记录 before {borrowAmount, walletBalance, equity}
  2. POST <endpoint> {coin, ...}     → 打印 status / retCode / retMsg / result
  3. GET /v5/account/wallet-balance  → 记录 after，对比 delta

默认 dry-run，加 --execute 才真发 step 2 + 3。

警告：quick-repayment 已确认会跨币种买卖（消耗 USDT 折价补差）。除非确认接受副作用，
默认应使用 --endpoint no-convert-repay。
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
from decimal import Decimal
from typing import Any, Dict, Tuple

import requests

HOST = os.environ.get("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
RECV_WINDOW_MS = "5000"


def load_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("BYBIT_API_KEY", "").strip()
    api_secret = os.environ.get("BYBIT_API_SECRET", "").strip()
    missing = [
        name
        for name, value in (
            ("BYBIT_API_KEY", api_key),
            ("BYBIT_API_SECRET", api_secret),
        )
        if not value
    ]
    if missing:
        raise SystemExit(f"missing env vars: {', '.join(missing)}")
    return api_key, api_secret


def sign(api_secret: str, payload: str) -> Tuple[str, str]:
    timestamp_ms = str(int(time.time() * 1000))
    raw = f"{timestamp_ms}{api_key_global}{RECV_WINDOW_MS}{payload}"
    sig = hmac.new(api_secret.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()
    return timestamp_ms, sig


def call(
    api_key: str,
    api_secret: str,
    method: str,
    path: str,
    *,
    query: str = "",
    body: str = "",
) -> Dict[str, Any]:
    payload = body if method.upper() != "GET" else query
    timestamp_ms, signature = sign(api_secret, payload)
    url = f"{HOST}{path}"
    if query:
        url = f"{url}?{query}"
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": timestamp_ms,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW_MS,
        "Content-Type": "application/json",
    }
    resp = requests.request(method.upper(), url, headers=headers, data=body)
    try:
        data = resp.json()
    except ValueError:
        raise RuntimeError(f"non-JSON response: {resp.status_code} {resp.text}")
    return {"http_status": resp.status_code, "body": data}


def get_coin_snapshot(api_key: str, api_secret: str, coin: str) -> Dict[str, str]:
    out = call(api_key, api_secret, "GET", "/v5/account/wallet-balance", query="accountType=UNIFIED")
    if out["http_status"] != 200 or out["body"].get("retCode") != 0:
        raise RuntimeError(f"wallet-balance failed: {out}")
    accounts = out["body"]["result"]["list"]
    for account in accounts:
        for c in account.get("coin", []):
            if c.get("coin") == coin:
                return {
                    "walletBalance": c.get("walletBalance", ""),
                    "borrowAmount": c.get("borrowAmount", ""),
                    "spotBorrow": c.get("spotBorrow", ""),
                    "accruedInterest": c.get("accruedInterest", ""),
                    "equity": c.get("equity", ""),
                    "usdValue": c.get("usdValue", ""),
                }
    raise RuntimeError(f"coin {coin} not found in wallet-balance response")


def print_snapshot(label: str, snap: Dict[str, str]) -> None:
    print(f"\n[{label}]")
    for k, v in snap.items():
        print(f"  {k:<18} {v}")


def diff_snapshot(before: Dict[str, str], after: Dict[str, str]) -> None:
    print("\n[delta after − before]")
    for k in ("walletBalance", "borrowAmount", "spotBorrow", "accruedInterest", "equity"):
        try:
            b = Decimal(before.get(k, "0") or "0")
            a = Decimal(after.get(k, "0") or "0")
            d = a - b
            mark = "  →  " if d == 0 else " ⚠ "
            print(f"  {k:<18} {b}  →  {a}    delta = {d}  {mark}")
        except Exception as e:
            print(f"  {k:<18} (compare failed: {e})")


ENDPOINTS = {
    "no-convert-repay": "/v5/account/no-convert-repay",
    "quick-repayment": "/v5/account/quick-repayment",
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--coin", default="BTC", help="coin to probe (default: BTC)")
    parser.add_argument(
        "--endpoint",
        choices=sorted(ENDPOINTS),
        default="no-convert-repay",
        help="which repay endpoint to hit (default: no-convert-repay = 纯同币 repay)",
    )
    parser.add_argument(
        "--repayment-type",
        choices=("FLEXIBLE", "FIXED", "ALL"),
        default="FLEXIBLE",
        help="no-convert-repay 专用：FLEXIBLE 同步返回；FIXED/ALL 异步 (default: FLEXIBLE)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="actually POST to the chosen endpoint (default: dry-run, only GET)",
    )
    args = parser.parse_args()

    coin = args.coin.strip().upper()
    path = ENDPOINTS[args.endpoint]
    api_key, api_secret = load_credentials()
    global api_key_global
    api_key_global = api_key

    print(
        f"=== Bybit repay probe (coin={coin}, endpoint={args.endpoint}, "
        f"execute={args.execute}) ==="
    )

    # step 1: snapshot before (target coin + USDT 作为跨币种泄漏检测基线)
    before = get_coin_snapshot(api_key, api_secret, coin)
    print_snapshot(f"BEFORE wallet-balance ({coin})", before)
    usdt_before = None
    if coin != "USDT":
        try:
            usdt_before = get_coin_snapshot(api_key, api_secret, "USDT")
            print_snapshot("BEFORE wallet-balance (USDT, 跨币种泄漏基线)", usdt_before)
        except Exception as e:
            print(f"\n[USDT before snapshot skipped: {e}]")

    if not args.execute:
        print(f"\n(dry-run; pass --execute to call POST {path})")
        return

    # step 2: call repay
    payload: Dict[str, Any] = {"coin": coin}
    if args.endpoint == "no-convert-repay":
        payload["repaymentType"] = args.repayment_type
    body = json.dumps(payload, separators=(",", ":"))
    out = call(api_key, api_secret, "POST", path, body=body)
    print(f"\n[POST {path}]")
    print(f"  request    : {body}")
    print(f"  http_status: {out['http_status']}")
    print(f"  retCode    : {out['body'].get('retCode')}")
    print(f"  retMsg     : {out['body'].get('retMsg')}")
    print(f"  result     : {json.dumps(out['body'].get('result'), indent=2)}")
    print(f"  retExtInfo : {json.dumps(out['body'].get('retExtInfo'), indent=2)}")

    # step 3: snapshot after (also pull USDT to verify nothing leaks across coins)
    time.sleep(2)  # 给后台结算一点时间，FLEXIBLE 是同步但 wallet 可能稍延迟刷新
    after = get_coin_snapshot(api_key, api_secret, coin)
    print_snapshot(f"AFTER wallet-balance ({coin})", after)
    diff_snapshot(before, after)

    if coin != "USDT" and usdt_before is not None:
        try:
            usdt_after = get_coin_snapshot(api_key, api_secret, "USDT")
            print_snapshot("AFTER wallet-balance (USDT)", usdt_after)
            print("\n[USDT delta — 应该全是 0，否则说明端点跨币种动了 USDT]")
            diff_snapshot(usdt_before, usdt_after)
        except Exception as e:
            print(f"\n[USDT after snapshot skipped: {e}]")


api_key_global = ""

if __name__ == "__main__":
    main()
