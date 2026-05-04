#!/usr/bin/env python3
"""Repay Gate unified-account loans by currency.

Default is dry-run. Use --execute to submit repay requests.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode

import requests


HOST = os.environ.get("GATE_API_BASE", "https://api.gateio.ws").rstrip("/")
PREFIX = "/api/v4"


def dec(value: Any, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return Decimal(default)


def fmt(value: Decimal) -> str:
    if value == 0:
        return "0"
    text = format(value.normalize(), "f")
    return "0" if text == "-0" else text


def load_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("GATE_API_KEY", "").strip()
    api_secret = os.environ.get("GATE_API_SECRET", "").strip()
    missing = [
        name
        for name, value in (
            ("GATE_API_KEY", api_key),
            ("GATE_API_SECRET", api_secret),
        )
        if not value
    ]
    if missing:
        raise SystemExit(f"missing env vars: {', '.join(missing)}")
    return api_key, api_secret


def build_query(params: Dict[str, Any]) -> str:
    items = [(key, value) for key, value in params.items() if value not in ("", None)]
    items.sort(key=lambda item: item[0])
    return urlencode(items, doseq=True)


def sign(method: str, path: str, query_string: str, body: str, api_secret: str) -> Dict[str, str]:
    timestamp = str(int(time.time()))
    body_hash = hashlib.sha512(body.encode("utf-8")).hexdigest()
    payload = f"{method.upper()}\n{path}\n{query_string}\n{body_hash}\n{timestamp}"
    signature = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha512).hexdigest()
    return {"Timestamp": timestamp, "SIGN": signature}


def private_request(
    api_key: str,
    api_secret: str,
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
) -> Tuple[int, Any]:
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True) if payload else ""
    query_string = build_query(params or {})
    url = f"{HOST}{PREFIX}{path}"
    if query_string:
        url = f"{url}?{query_string}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "KEY": api_key,
    }
    headers.update(sign(method, f"{PREFIX}{path}", query_string, body, api_secret))
    resp = requests.request(method.upper(), url, headers=headers, data=body, timeout=timeout)
    try:
        data = resp.json()
    except ValueError:
        data = {"raw": resp.text}
    return resp.status_code, data


def fetch_loans(api_key: str, api_secret: str, timeout: int) -> List[Dict[str, Any]]:
    status, data = private_request(api_key, api_secret, "GET", "/unified/loans", timeout=timeout)
    if status >= 300:
        raise SystemExit(f"Gate GET /unified/loans failed: http={status} body={data}")
    if not isinstance(data, list):
        raise SystemExit(f"Gate GET /unified/loans returned non-list body: {data}")
    rows: List[Dict[str, Any]] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        currency = str(entry.get("currency") or "").strip().upper()
        amount = dec(entry.get("amount"))
        if not currency or amount <= 0:
            continue
        rows.append(
            {
                "currency": currency,
                "amount": amount,
                "type": str(entry.get("type") or "").strip().lower(),
            }
        )
    rows.sort(key=lambda row: (row["currency"], row["type"]))
    return rows


def parse_assets(values: Iterable[str]) -> List[str]:
    assets = set()
    for value in values:
        for part in value.split(","):
            asset = part.strip().upper()
            if asset:
                assets.add(asset)
    return sorted(assets)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Repay Gate unified-account loans by currency.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--asset", action="append", default=[], help="Asset/currency allowlist, e.g. DOGE or DOGE,USDT.")
    parser.add_argument("--execute", action="store_true", help="Call Gate API. Without this, only prints the plan.")
    parser.add_argument("--timeout", type=int, default=10)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    assets = parse_assets(args.asset)
    api_key, api_secret = load_credentials()
    loans = fetch_loans(api_key, api_secret, args.timeout)
    if assets:
        loans = [row for row in loans if row["currency"] in assets]

    totals: Dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    counts: Dict[str, int] = defaultdict(int)
    for row in loans:
        totals[row["currency"]] += row["amount"]
        counts[row["currency"]] += 1

    if not totals:
        print("No unified loans to repay.")
        return

    print(f"[info] execute={args.execute} currencies={len(totals)}")
    for currency in sorted(totals):
        print(f"plan    {currency:<8} rows={counts[currency]:<3} amount={fmt(totals[currency])}")

    if not args.execute:
        print("\nDry-run mode. Add --execute to submit repayments.")
        return

    failures = 0
    for idx, currency in enumerate(sorted(totals), start=1):
        amount = totals[currency]
        payload = {
            "currency": currency,
            "amount": fmt(amount),
            "type": "repay",
            "repaid_all": True,
            "text": f"t-gate-repay{int(time.time() * 1000)}{idx:02d}",
        }
        print(f"\n[repay] {currency} amount={fmt(amount)} repaid_all=true")
        status, data = private_request(api_key, api_secret, "POST", "/unified/loans", payload=payload, timeout=args.timeout)
        ok = 200 <= status < 300
        print(f"  [{'OK' if ok else 'ERR'}] status={status} {json.dumps(data, ensure_ascii=True, sort_keys=True)}")
        if not ok:
            failures += 1

    if failures:
        print(f"\nWARN: {failures} repayments failed", file=sys.stderr)
        raise SystemExit(1)
    print("\nDone.")


if __name__ == "__main__":
    main()
