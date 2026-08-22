#!/usr/bin/env python3
"""Check Binance Portfolio Margin order balance/borrow capacity.

Read-only helper for diagnosing Binance PM margin order rejects such as:
  code=-2010 msg="Account has insufficient balance for requested action."

Examples:
  python3 scripts/binance_pm_order_capacity.py --symbol AEVOUSDT --side BUY --quantity 695 --price 0.1438
  python3 scripts/binance_pm_order_capacity.py --asset USDT --required 100.0
  python3 scripts/binance_pm_order_capacity.py --symbol AEVOUSDT --side SELL --quantity 695 --json

The script auto-sources ./env.sh when present. BINANCE_API_KEY and
BINANCE_API_SECRET from env.sh override the parent process env to avoid
accidentally querying the wrong account.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, Optional, Tuple


DEFAULT_BASE_URL = "https://papi.binance.com"
AUTHORITATIVE_KEYS = ("BINANCE_API_KEY", "BINANCE_API_SECRET")
COMMON_QUOTES = (
    "USDT",
    "FDUSD",
    "USDC",
    "TUSD",
    "BUSD",
    "BTC",
    "ETH",
    "BNB",
    "EUR",
    "TRY",
    "BRL",
    "DAI",
    "USD",
)


def now_ms() -> int:
    return int(time.time() * 1000)


def decimal_from(value: Any, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return Decimal(default)


def fmt_decimal(value: Decimal) -> str:
    if value.is_zero():
        return "0"
    normalized = value.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def auto_source_env_sh() -> None:
    env_path = os.path.join(os.getcwd(), "env.sh")
    if not os.path.isfile(env_path):
        return

    proc = subprocess.run(
        ["bash", "-lc", f"set -a; source {shlex.quote(env_path)} >/dev/null 2>&1; env -0"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        sys.stderr.write(f"[WARN] failed to source {env_path}: {proc.stderr.decode(errors='replace')}\n")
        return

    for item in proc.stdout.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key_b, value_b = item.split(b"=", 1)
        key = key_b.decode("utf-8", errors="ignore")
        value = value_b.decode("utf-8", errors="replace")
        if key in AUTHORITATIVE_KEYS:
            old_value = os.environ.get(key)
            if old_value and old_value != value:
                sys.stderr.write(
                    f"[WARN] env.sh overrides existing {key}; env.sh value wins for account safety\n"
                )
            os.environ[key] = value
        elif key not in os.environ:
            os.environ[key] = value


def load_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        raise SystemExit("[ERROR] missing BINANCE_API_KEY / BINANCE_API_SECRET")
    return api_key, api_secret


def sign_query(query: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def signed_request(
    base_url: str,
    path: str,
    params: Dict[str, Any],
    api_key: str,
    api_secret: str,
    *,
    method: str = "GET",
    timeout: float = 10.0,
    recv_window: int = 5000,
) -> Tuple[int, str, Dict[str, str]]:
    payload = dict(params)
    payload.setdefault("recvWindow", str(recv_window))
    payload["timestamp"] = str(now_ms())
    query = urllib.parse.urlencode(sorted(payload.items(), key=lambda kv: kv[0]), safe="-_.~")
    signature = sign_query(query, api_secret)
    url = f"{base_url.rstrip('/')}{path}?{query}&signature={signature}"
    req = urllib.request.Request(
        url,
        method=method.upper(),
        headers={"X-MBX-APIKEY": api_key},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.getcode(), body, dict(resp.headers.items())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return exc.code, body, dict(exc.headers.items())
    except Exception as exc:  # noqa: BLE001
        return 0, str(exc), {}


def parse_json_body(status: int, body: str, path: str) -> Any:
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{path} status={status} returned non-JSON body: {body[:500]}") from exc


def extract_error(body: str) -> str:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return body[:500]
    if isinstance(payload, dict):
        code = payload.get("code")
        msg = payload.get("msg") or payload.get("message")
        return f"code={code} msg={msg}" if code is not None or msg is not None else json.dumps(payload)
    return json.dumps(payload)


def find_balance_row(payload: Any, asset: str) -> Dict[str, Any]:
    asset_upper = asset.upper()
    if isinstance(payload, list):
        for row in payload:
            if isinstance(row, dict) and str(row.get("asset", "")).upper() == asset_upper:
                return row
        return {"asset": asset_upper}
    if isinstance(payload, dict):
        if str(payload.get("asset", "")).upper() == asset_upper:
            return payload
        # Be tolerant if Binance changes the shape to {"balances": [...]}.
        balances = payload.get("balances")
        if isinstance(balances, list):
            return find_balance_row(balances, asset_upper)
    return {"asset": asset_upper}


def fetch_balance(
    base_url: str,
    api_key: str,
    api_secret: str,
    asset: str,
    *,
    timeout: float,
    recv_window: int,
) -> Dict[str, Any]:
    status, body, _headers = signed_request(
        base_url,
        "/papi/v1/balance",
        {"asset": asset.upper()},
        api_key,
        api_secret,
        timeout=timeout,
        recv_window=recv_window,
    )
    if 200 <= status < 300:
        return find_balance_row(parse_json_body(status, body, "/papi/v1/balance"), asset)

    # Some deployments historically used full-balance snapshots; fall back once.
    status2, body2, _headers2 = signed_request(
        base_url,
        "/papi/v1/balance",
        {},
        api_key,
        api_secret,
        timeout=timeout,
        recv_window=recv_window,
    )
    if not (200 <= status2 < 300):
        raise RuntimeError(
            f"/papi/v1/balance failed: status={status} err={extract_error(body)}; "
            f"fallback status={status2} err={extract_error(body2)}"
        )
    return find_balance_row(parse_json_body(status2, body2, "/papi/v1/balance"), asset)


def fetch_max_borrowable(
    base_url: str,
    api_key: str,
    api_secret: str,
    asset: str,
    *,
    timeout: float,
    recv_window: int,
) -> Dict[str, Any]:
    status, body, _headers = signed_request(
        base_url,
        "/papi/v1/margin/maxBorrowable",
        {"asset": asset.upper()},
        api_key,
        api_secret,
        timeout=timeout,
        recv_window=recv_window,
    )
    if not (200 <= status < 300):
        raise RuntimeError(
            f"/papi/v1/margin/maxBorrowable failed: status={status} err={extract_error(body)}"
        )
    payload = parse_json_body(status, body, "/papi/v1/margin/maxBorrowable")
    return payload if isinstance(payload, dict) else {"raw": payload}


def infer_assets(symbol: str, quote_asset: Optional[str]) -> Tuple[str, str]:
    sym = symbol.strip().upper()
    if not sym:
        raise ValueError("symbol is empty")
    if quote_asset:
        quote = quote_asset.strip().upper()
        if not sym.endswith(quote) or len(sym) <= len(quote):
            raise ValueError(f"symbol {sym} does not end with quote asset {quote}")
        return sym[: -len(quote)], quote

    for quote in sorted(COMMON_QUOTES, key=len, reverse=True):
        if sym.endswith(quote) and len(sym) > len(quote):
            return sym[: -len(quote)], quote
    raise ValueError(
        f"cannot infer base/quote from symbol {sym}; pass --quote-asset or use --asset/--required"
    )


def resolve_check(
    args: argparse.Namespace,
) -> Tuple[str, Optional[str], Optional[str], Decimal]:
    if args.asset:
        if args.required is None:
            raise ValueError("--asset mode requires --required")
        return args.asset.upper(), None, None, decimal_from(args.required)

    if not args.symbol or not args.side:
        raise ValueError("provide either --asset/--required or --symbol/--side/--quantity/--price")
    if args.quantity is None:
        raise ValueError("--symbol mode requires --quantity")

    base, quote = infer_assets(args.symbol, args.quote_asset)
    side = args.side.upper()
    qty = decimal_from(args.quantity)
    if qty <= 0:
        raise ValueError("--quantity must be > 0")

    if side == "BUY":
        if args.price is None:
            raise ValueError("BUY --symbol mode requires --price to compute quote requirement")
        price = decimal_from(args.price)
        if price <= 0:
            raise ValueError("--price must be > 0")
        return quote, base, quote, qty * price
    if side == "SELL":
        return base, base, quote, qty
    raise ValueError("--side must be BUY or SELL")


def selected_fields(row: Dict[str, Any], keys: Iterable[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for key in keys:
        value = row.get(key)
        if value is not None:
            out[key] = str(value)
    return out


def build_report(args: argparse.Namespace) -> Dict[str, Any]:
    auto_source_env_sh()
    api_key, api_secret = load_credentials()
    base_url = (args.base_url or os.environ.get("BINANCE_PAPI_URL") or DEFAULT_BASE_URL).rstrip("/")

    check_asset, base_asset, quote_asset, required = resolve_check(args)
    balance = fetch_balance(
        base_url,
        api_key,
        api_secret,
        check_asset,
        timeout=args.timeout,
        recv_window=args.recv_window,
    )
    max_borrowable = fetch_max_borrowable(
        base_url,
        api_key,
        api_secret,
        check_asset,
        timeout=args.timeout,
        recv_window=args.recv_window,
    )

    free = decimal_from(balance.get("crossMarginFree"))
    locked = decimal_from(balance.get("crossMarginLocked"))
    borrowed = decimal_from(balance.get("crossMarginBorrowed"))
    interest = decimal_from(balance.get("crossMarginInterest"))
    wallet = decimal_from(
        balance.get("totalWalletBalance", balance.get("crossMarginAsset", balance.get("walletBalance")))
    )
    max_borrow = decimal_from(max_borrowable.get("amount"))
    borrow_limit = decimal_from(max_borrowable.get("borrowLimit"))
    buffer_rate = decimal_from(args.buffer_rate)
    required_with_buffer = required * (Decimal("1") + buffer_rate)
    capacity = free + max_borrow
    surplus = capacity - required_with_buffer

    return {
        "baseUrl": base_url,
        "cwd": os.getcwd(),
        "symbol": args.symbol.upper() if args.symbol else None,
        "side": args.side.upper() if args.side else None,
        "baseAsset": base_asset,
        "quoteAsset": quote_asset,
        "checkAsset": check_asset,
        "required": fmt_decimal(required),
        "bufferRate": fmt_decimal(buffer_rate),
        "requiredWithBuffer": fmt_decimal(required_with_buffer),
        "crossMarginFree": fmt_decimal(free),
        "crossMarginLocked": fmt_decimal(locked),
        "crossMarginBorrowed": fmt_decimal(borrowed),
        "crossMarginInterest": fmt_decimal(interest),
        "walletOrCrossMarginAsset": fmt_decimal(wallet),
        "maxBorrowableAmount": fmt_decimal(max_borrow),
        "borrowLimit": fmt_decimal(borrow_limit),
        "capacity": fmt_decimal(capacity),
        "surplus": fmt_decimal(surplus),
        "shortfall": fmt_decimal(max(Decimal("0"), -surplus)),
        "sufficient": surplus >= 0,
        "balanceRaw": balance,
        "maxBorrowableRaw": max_borrowable,
    }


def print_text(report: Dict[str, Any], *, show_raw: bool) -> None:
    print("Binance PM margin capacity check")
    print(f"  cwd: {report['cwd']}")
    print(f"  base_url: {report['baseUrl']}")
    if report["symbol"]:
        print(
            f"  order: symbol={report['symbol']} side={report['side']} "
            f"base={report['baseAsset']} quote={report['quoteAsset']}"
        )
    print(f"  check_asset: {report['checkAsset']}")
    print(f"  required: {report['required']}")
    if report["bufferRate"] != "0":
        print(f"  required_with_buffer: {report['requiredWithBuffer']} buffer_rate={report['bufferRate']}")
    print("")
    print("Asset state")
    print(f"  crossMarginFree: {report['crossMarginFree']}")
    print(f"  maxBorrowable.amount: {report['maxBorrowableAmount']}")
    print(f"  capacity_free_plus_borrow: {report['capacity']}")
    print(f"  crossMarginLocked: {report['crossMarginLocked']}")
    print(f"  crossMarginBorrowed: {report['crossMarginBorrowed']}")
    print(f"  crossMarginInterest: {report['crossMarginInterest']}")
    print(f"  wallet_or_crossMarginAsset: {report['walletOrCrossMarginAsset']}")
    if report["borrowLimit"] != "0":
        print(f"  borrowLimit: {report['borrowLimit']}")
    print("")
    if report["sufficient"]:
        print(f"decision=OK surplus={report['surplus']}")
    else:
        print(f"decision=INSUFFICIENT shortfall={report['shortfall']}")

    if show_raw:
        print("")
        print("Raw balance:")
        print(json.dumps(report["balanceRaw"], indent=2, sort_keys=True))
        print("Raw maxBorrowable:")
        print(json.dumps(report["maxBorrowableRaw"], indent=2, sort_keys=True))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check Binance Portfolio Margin free balance plus max borrowable for an order",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    order = parser.add_argument_group("order mode")
    order.add_argument("--symbol", help="Trading symbol, e.g. AEVOUSDT")
    order.add_argument("--side", choices=("BUY", "SELL", "buy", "sell"), help="Order side")
    order.add_argument("--quantity", help="Order quantity in base asset")
    order.add_argument("--price", help="Order price; required for BUY order mode")
    order.add_argument("--quote-asset", help="Override quote asset when symbol suffix is ambiguous")

    asset = parser.add_argument_group("asset mode")
    asset.add_argument("--asset", help="Directly check this asset, e.g. USDT or AEVO")
    asset.add_argument("--required", help="Required amount for --asset mode")

    parser.add_argument(
        "--base-url",
        default=None,
        help=f"Binance PAPI base URL; default env BINANCE_PAPI_URL or {DEFAULT_BASE_URL}",
    )
    parser.add_argument("--recv-window", type=int, default=5000, help="recvWindow in milliseconds")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout in seconds")
    parser.add_argument(
        "--buffer-rate",
        default="0",
        help="Extra requirement ratio, e.g. 0.001 means +0.1%%",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    parser.add_argument("--raw", action="store_true", help="Include raw REST payloads in text output")
    parser.add_argument(
        "--fail-if-insufficient",
        action="store_true",
        help="Exit 3 when query succeeds but capacity is insufficient",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        report = build_report(args)
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"[ERROR] {exc}\n")
        return 2

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print_text(report, show_raw=args.raw)

    if args.fail_if_insufficient and not report["sufficient"]:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
