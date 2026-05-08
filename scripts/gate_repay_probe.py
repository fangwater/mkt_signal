#!/usr/bin/env python3
"""Gate UNIFIED auto-repay endpoint 探针。

验证 `POST /api/v4/unified/loans type=repay repaid_all=true` 是否真的只用
同币种 `available` 抵同币种 `borrowed`，USDT 等其他币种不动。

流程（针对单个 currency）：
  1. GET /api/v4/unified/accounts → 记录 before {target coin + USDT 基线}
  2. POST /api/v4/unified/loans {currency, amount, type:"repay", repaid_all:true}
  3. GET /api/v4/unified/accounts → 记录 after，对比 delta

默认 dry-run（仅 step 1）。加 --execute 才真发。
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
from urllib.parse import urlencode

import requests

HOST = os.environ.get("GATE_API_BASE", "https://api.gateio.ws").rstrip("/")
PREFIX = "/api/v4"


def load_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("GATE_API_KEY", "").strip()
    api_secret = os.environ.get("GATE_API_SECRET", "").strip()
    missing = [n for n, v in (("GATE_API_KEY", api_key), ("GATE_API_SECRET", api_secret)) if not v]
    if missing:
        raise SystemExit(f"missing env vars: {', '.join(missing)}")
    return api_key, api_secret


def sign(method: str, path: str, query_string: str, body: str, api_secret: str) -> Dict[str, str]:
    timestamp = str(int(time.time()))
    body_hash = hashlib.sha512(body.encode("utf-8")).hexdigest()
    payload = f"{method.upper()}\n{path}\n{query_string}\n{body_hash}\n{timestamp}"
    signature = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha512).hexdigest()
    return {"Timestamp": timestamp, "SIGN": signature}


def call(api_key: str, api_secret: str, method: str, path: str, *,
         params: Dict[str, Any] | None = None, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    body = json.dumps(payload, separators=(",", ":")) if payload else ""
    items = sorted([(k, v) for k, v in (params or {}).items() if v not in ("", None)])
    query_string = urlencode(items, doseq=True)
    full_path = f"{PREFIX}{path}"
    url = f"{HOST}{full_path}"
    if query_string:
        url = f"{url}?{query_string}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "KEY": api_key,
    }
    headers.update(sign(method, full_path, query_string, body, api_secret))
    resp = requests.request(method.upper(), url, headers=headers, data=body, timeout=15)
    try:
        data = resp.json()
    except ValueError:
        data = {"raw": resp.text}
    return {"http_status": resp.status_code, "body": data}


def fetch_balance(api_key: str, api_secret: str, currency: str) -> Dict[str, str]:
    out = call(api_key, api_secret, "GET", "/unified/accounts")
    if out["http_status"] != 200:
        raise RuntimeError(f"GET /unified/accounts failed: {out}")
    balances = out["body"].get("balances", {})
    bal = balances.get(currency)
    if bal is None:
        raise RuntimeError(f"currency {currency} not found in balances")
    keep = ("available", "freeze", "borrowed", "negative_liab", "total_liab", "equity")
    return {k: bal.get(k, "") for k in keep}


def print_snap(label: str, snap: Dict[str, str]) -> None:
    print(f"\n[{label}]")
    for k, v in snap.items():
        print(f"  {k:<14} {v}")


def diff_snap(before: Dict[str, str], after: Dict[str, str]) -> None:
    print("\n[delta after − before]")
    for k in ("available", "freeze", "borrowed", "negative_liab", "total_liab", "equity"):
        try:
            b = Decimal(before.get(k, "0") or "0")
            a = Decimal(after.get(k, "0") or "0")
            d = a - b
            mark = "  →  " if d == 0 else " ⚠ "
            print(f"  {k:<14} {b}  →  {a}    delta = {d}  {mark}")
        except Exception as e:
            print(f"  {k:<14} (compare failed: {e})")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--currency", default="BTC", help="currency to probe (default: BTC)")
    parser.add_argument("--execute", action="store_true",
                        help="actually POST /unified/loans (default: dry-run, only GET)")
    parser.add_argument("--repaid-all", choices=("true", "false"), default="false",
                        help="repaid_all flag (default: false → 部分还；true → 仅 available>=total_liab 时全清)")
    args = parser.parse_args()

    currency = args.currency.strip().upper()
    api_key, api_secret = load_credentials()

    print(f"=== Gate unified-loans repay probe (currency={currency}, execute={args.execute}) ===")

    before = fetch_balance(api_key, api_secret, currency)
    print_snap(f"BEFORE ({currency})", before)
    usdt_before = None
    if currency != "USDT":
        try:
            usdt_before = fetch_balance(api_key, api_secret, "USDT")
            print_snap("BEFORE (USDT, 跨币种泄漏基线)", usdt_before)
        except Exception as e:
            print(f"\n[USDT before snapshot skipped: {e}]")

    if not args.execute:
        print(f"\n(dry-run; pass --execute to call POST /unified/loans for {currency})")
        return

    # 实测：Gate 不会自动 clamp。若 amount > available 直接报 BALANCE_NOT_ENOUGH。
    # 客户端必须送 min(available, total_liab)（也就是 Rust GateRepayer 的逻辑）。
    avail = Decimal(before.get("available", "0") or "0")
    liab = Decimal(before.get("total_liab", "0") or "0")
    amount_dec = min(avail, liab) if avail > 0 else Decimal("0")
    if amount_dec <= 0:
        print(f"\n[skip] available={avail} total_liab={liab} → 没东西可还")
        return
    # Gate 各 currency 精度上限 8 位；超出会 INVALID_PARAM_VALUE。
    quantized = amount_dec.quantize(Decimal("0.00000001"))
    s = format(quantized, "f").rstrip("0").rstrip(".")
    amount_str = s if s else "0"
    payload = {
        "currency": currency,
        "amount": amount_str,
        "type": "repay",
        "repaid_all": args.repaid_all == "true",
        "text": f"t-probe-{int(time.time() * 1000)}",
    }
    print(f"\n[POST /unified/loans]")
    print(f"  payload    : {json.dumps(payload, separators=(',', ':'))}")
    out = call(api_key, api_secret, "POST", "/unified/loans", payload=payload)
    print(f"  http_status: {out['http_status']}")
    print(f"  body       : {json.dumps(out['body'], indent=2, default=str)}")

    time.sleep(1)
    after = fetch_balance(api_key, api_secret, currency)
    print_snap(f"AFTER ({currency})", after)
    diff_snap(before, after)

    if currency != "USDT" and usdt_before is not None:
        try:
            usdt_after = fetch_balance(api_key, api_secret, "USDT")
            print_snap("AFTER (USDT)", usdt_after)
            print("\n[USDT delta — 应该全是 0，否则说明端点跨币种动了 USDT]")
            diff_snap(usdt_before, usdt_after)
        except Exception as e:
            print(f"\n[USDT after snapshot skipped: {e}]")


if __name__ == "__main__":
    main()
