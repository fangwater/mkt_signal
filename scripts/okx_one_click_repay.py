#!/usr/bin/env python3
"""OKX one-click repay helper.

Default mode is dry-run. Use --execute to call the live OKX trade endpoint.

The script reads OKX_API_KEY, OKX_API_SECRET, and OKX_PASSPHRASE from the
environment. It intentionally has no credential fallback.
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
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple


BASE_URL = os.environ.get("OKX_BASE_URL", "https://www.okx.com")
EPS = 1e-12


@dataclass
class Debt:
    ccy: str
    amount: float
    source: str


@dataclass
class RepayCurrency:
    ccy: str
    amount: float


def compact_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def okx_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def sign(timestamp: str, method: str, request_path: str, body: str, secret: str) -> str:
    payload = f"{timestamp}{method.upper()}{request_path}{body}"
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def load_credentials() -> Tuple[str, str, str]:
    api_key = os.environ.get("OKX_API_KEY", "").strip()
    api_secret = os.environ.get("OKX_API_SECRET", "").strip()
    passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()
    missing = [
        name
        for name, value in (
            ("OKX_API_KEY", api_key),
            ("OKX_API_SECRET", api_secret),
            ("OKX_PASSPHRASE", passphrase),
        )
        if not value
    ]
    if missing:
        raise SystemExit(f"missing env vars: {', '.join(missing)}")
    return api_key, api_secret, passphrase


def okx_request(
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
) -> Dict[str, Any]:
    method = method.upper()
    query = urllib.parse.urlencode(params or {})
    request_path = f"{path}?{query}" if query else path
    body_str = "" if method == "GET" or body is None else compact_json(body)
    timestamp = okx_timestamp()
    signature = sign(timestamp, method, request_path, body_str, api_secret)

    req = urllib.request.Request(
        f"{BASE_URL.rstrip('/')}{request_path}",
        data=None if method == "GET" else body_str.encode("utf-8"),
        method=method,
    )
    req.add_header("Content-Type", "application/json")
    req.add_header("OK-ACCESS-KEY", api_key)
    req.add_header("OK-ACCESS-SIGN", signature)
    req.add_header("OK-ACCESS-TIMESTAMP", timestamp)
    req.add_header("OK-ACCESS-PASSPHRASE", passphrase)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace")
        return {"code": "-1", "msg": f"HTTP {exc.code}", "raw": raw}
    except Exception as exc:
        return {"code": "-1", "msg": str(exc)}

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"code": "-1", "msg": "non-json response", "raw": raw}


def as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def normalize_ccys(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in values:
        for part in str(raw).replace(",", " ").split():
            ccy = part.strip().upper()
            if ccy and ccy not in seen:
                seen.add(ccy)
                out.append(ccy)
    return out


def require_ok(v: Dict[str, Any], label: str) -> None:
    if str(v.get("code", "")).strip() != "0":
        raise SystemExit(f"{label} failed: code={v.get('code')} msg={v.get('msg')} raw={v.get('raw', '')[:300]}")


def fetch_balance_debts(api_key: str, api_secret: str, passphrase: str, timeout: int) -> List[Debt]:
    v = okx_request(
        "GET",
        "/api/v5/account/balance",
        api_key,
        api_secret,
        passphrase,
        timeout=timeout,
    )
    require_ok(v, "GET /api/v5/account/balance")
    debts: List[Debt] = []
    for account in v.get("data") or []:
        for detail in account.get("details") or []:
            ccy = str(detail.get("ccy") or "").upper()
            if not ccy:
                continue
            liab = max(
                abs(as_float(detail.get("liab"))),
                abs(as_float(detail.get("crossLiab"))),
                abs(as_float(detail.get("isoLiab"))),
            )
            if liab > EPS:
                debts.append(Debt(ccy=ccy, amount=liab, source="balance"))
    return debts


def fetch_one_click_list(
    api_key: str,
    api_secret: str,
    passphrase: str,
    timeout: int,
) -> Tuple[List[Debt], List[RepayCurrency]]:
    v = okx_request(
        "GET",
        "/api/v5/trade/one-click-repay-currency-list",
        api_key,
        api_secret,
        passphrase,
        params={"debtType": "cross"},
        timeout=timeout,
    )
    require_ok(v, "GET /api/v5/trade/one-click-repay-currency-list")

    debts: List[Debt] = []
    repay_ccys: List[RepayCurrency] = []
    for block in v.get("data") or []:
        for item in block.get("debtData") or []:
            ccy = str(item.get("debtCcy") or "").upper()
            amount = as_float(item.get("debtAmt"))
            if ccy and amount > EPS:
                debts.append(Debt(ccy=ccy, amount=amount, source="one-click-list"))
        for item in block.get("repayData") or []:
            ccy = str(item.get("repayCcy") or "").upper()
            amount = as_float(item.get("repayAmt"))
            if ccy:
                repay_ccys.append(RepayCurrency(ccy=ccy, amount=amount))
    return debts, repay_ccys


def merge_debts(primary: List[Debt], fallback: List[Debt]) -> List[Debt]:
    by_ccy: Dict[str, Debt] = {}
    for debt in fallback:
        by_ccy[debt.ccy] = debt
    for debt in primary:
        by_ccy[debt.ccy] = debt
    return sorted(by_ccy.values(), key=lambda d: d.ccy)


def select_debts(
    debts: List[Debt],
    requested_ccys: List[str],
    repay_ccy: str,
    min_amount: float,
    include_same_ccy: bool,
) -> List[Debt]:
    requested = set(requested_ccys)
    selected = []
    for debt in debts:
        if requested and debt.ccy not in requested:
            continue
        if debt.amount <= min_amount:
            continue
        if debt.ccy == repay_ccy and not include_same_ccy:
            continue
        selected.append(debt)
    return selected


def print_debts(title: str, debts: List[Debt]) -> None:
    print(title)
    if not debts:
        print("  (none)")
        return
    for debt in debts:
        print(f"  {debt.ccy:<8} amount={debt.amount:.12g} source={debt.source}")


def print_repay_ccys(repay_ccys: List[RepayCurrency]) -> None:
    print("Repay currencies reported by OKX:")
    if not repay_ccys:
        print("  (none)")
        return
    for item in sorted(repay_ccys, key=lambda x: x.ccy):
        print(f"  {item.ccy:<8} available={item.amount:.12g}")


def one_click_repay(
    api_key: str,
    api_secret: str,
    passphrase: str,
    debts: List[Debt],
    repay_ccy: str,
    timeout: int,
) -> Dict[str, Any]:
    body = {
        "debtCcy": [debt.ccy for debt in debts],
        "repayCcy": repay_ccy,
    }
    return okx_request(
        "POST",
        "/api/v5/trade/one-click-repay",
        api_key,
        api_secret,
        passphrase,
        body=body,
        timeout=timeout,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Dry-run or execute OKX cross-debt one-click repay.")
    parser.add_argument("--execute", action="store_true", help="submit live one-click repay")
    parser.add_argument("--repay-ccy", default="USDT", help="currency used to repay debts")
    parser.add_argument("--debt-ccy", action="append", default=[], help="debt ccy to repay; repeatable or comma-separated")
    parser.add_argument("--min-amount", type=float, default=1e-12, help="ignore debts at or below this amount")
    parser.add_argument("--include-same-ccy", action="store_true", help="allow debt ccy to equal repay ccy")
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--verify-delay", type=float, default=3.0)
    args = parser.parse_args()

    repay_ccy = args.repay_ccy.strip().upper()
    if not repay_ccy:
        raise SystemExit("--repay-ccy is empty")
    requested_ccys = normalize_ccys(args.debt_ccy)

    api_key, api_secret, passphrase = load_credentials()
    try:
        listed_debts, repay_ccys = fetch_one_click_list(api_key, api_secret, passphrase, args.timeout)
    except SystemExit as exc:
        print(f"[warn] one-click list unavailable: {exc}", file=sys.stderr)
        listed_debts, repay_ccys = [], []

    balance_debts = fetch_balance_debts(api_key, api_secret, passphrase, args.timeout)
    debts = merge_debts(listed_debts, balance_debts)

    print(f"Base URL: {BASE_URL}")
    print_debts("Detected debts:", debts)
    print_repay_ccys(repay_ccys)

    selected = select_debts(
        debts,
        requested_ccys=requested_ccys,
        repay_ccy=repay_ccy,
        min_amount=args.min_amount,
        include_same_ccy=args.include_same_ccy,
    )
    print_debts(f"Selected debts to repay with {repay_ccy}:", selected)
    if not selected:
        print("Nothing to repay.")
        return 0
    if len(selected) > 5:
        raise SystemExit("OKX one-click-repay allows at most 5 debt currencies per request")

    if repay_ccys and repay_ccy not in {item.ccy for item in repay_ccys}:
        raise SystemExit(f"{repay_ccy} is not in OKX repay currency list; refusing to execute")

    if not args.execute:
        print("Dry-run only. Add --execute to submit live one-click repay.")
        return 0

    print("Submitting POST /api/v5/trade/one-click-repay ...")
    result = one_click_repay(api_key, api_secret, passphrase, selected, repay_ccy, args.timeout)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    require_ok(result, "POST /api/v5/trade/one-click-repay")

    if args.verify_delay > 0:
        time.sleep(args.verify_delay)
    residual = fetch_balance_debts(api_key, api_secret, passphrase, args.timeout)
    print_debts("Residual balance debts after repay:", residual)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
