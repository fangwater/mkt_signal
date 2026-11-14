#!/usr/bin/env python3
"""Transfer coins between Bitget UTA wallets via POST /api/v3/account/transfer.

Funding/spot cash sits in fromType=spot; the unified trading account is toType=uta.
Docs: https://www.bitget.com/api-doc/uta/account/transfer/

Examples:
  source /home/ubuntu/bitget_fr_arb01/env.sh
  python scripts/bitget_transfer.py
  python scripts/bitget_transfer.py --amount 250000 --execute

  python scripts/bitget_transfer.py --env-file /home/ubuntu/bitget_fr_arb01/env.sh \\
      --from spot --to uta --coin USDT --all --execute
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.request
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_BASE_URL = os.environ.get("BITGET_API_BASE", "https://api.bitget.com")
TRANSFER_PATH = "/api/v3/account/transfer"
FUNDING_ASSETS_PATH = "/api/v3/account/funding-assets"
UTA_ASSETS_PATH = "/api/v3/account/assets"
ACCOUNT_TYPES = (
    "spot",
    "p2p",
    "coin_futures",
    "usdt_futures",
    "usdc_futures",
    "crossed_margin",
    "isolated_margin",
    "uta",
)


def now_ms() -> str:
    return str(int(time.time() * 1000))


def sign(timestamp_ms: str, method: str, signed_path: str, body: str, secret: str) -> str:
    payload = f"{timestamp_ms}{method.upper()}{signed_path}{body}"
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def source_env_file(env_file: str) -> None:
    cmd = (
        "set -a && "
        f"source {shlex.quote(env_file)} >/dev/null 2>&1 && "
        "env -0"
    )
    proc = subprocess.run(["bash", "-lc", cmd], check=False, capture_output=True)
    if proc.returncode != 0:
        stderr = proc.stderr.decode("utf-8", errors="replace").strip()
        raise SystemExit(f"ERROR: failed to source env file: {env_file} stderr={stderr}")
    for item in proc.stdout.split(b"\x00"):
        if not item:
            continue
        key, sep, value = item.partition(b"=")
        if not sep:
            continue
        os.environ[key.decode("utf-8", errors="replace")] = value.decode(
            "utf-8", errors="replace"
        )


def load_credentials() -> Tuple[str, str, str]:
    api_key = os.environ.get("BITGET_API_KEY", "").strip()
    api_secret = os.environ.get("BITGET_API_SECRET", "").strip()
    passphrase = os.environ.get("BITGET_API_PASSPHRASE", "").strip()
    missing = [
        name
        for name, value in (
            ("BITGET_API_KEY", api_key),
            ("BITGET_API_SECRET", api_secret),
            ("BITGET_API_PASSPHRASE", passphrase),
        )
        if not value
    ]
    if missing:
        print(f"ERROR: please set {', '.join(missing)} in environment.", file=sys.stderr)
        raise SystemExit(1)
    return api_key, api_secret, passphrase


def do_request(
    base_url: str,
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    body: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
) -> Tuple[int, str]:
    method = method.upper()
    body_str = "" if body is None else json.dumps(body, separators=(",", ":"), ensure_ascii=True)
    timestamp_ms = now_ms()
    signature = sign(timestamp_ms, method, path, body_str, api_secret)
    url = f"{base_url.rstrip('/')}{path}"
    data = body_str.encode("utf-8") if body_str else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("ACCESS-KEY", api_key)
    req.add_header("ACCESS-SIGN", signature)
    req.add_header("ACCESS-TIMESTAMP", timestamp_ms)
    req.add_header("ACCESS-PASSPHRASE", passphrase)
    req.add_header("Content-Type", "application/json")
    req.add_header("locale", "en-US")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")


def parse_json_response(path: str, status: int, body: str) -> Dict[str, Any]:
    if status != 200:
        print(f"ERROR: {path} http_status={status} body={body[:512]}", file=sys.stderr)
        raise SystemExit(2)
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        print(f"ERROR: {path} returned non-JSON: {body[:512]}", file=sys.stderr)
        raise SystemExit(2)
    code = str(parsed.get("code", "-1"))
    if code != "00000":
        print(f"ERROR: {path} code={code} msg={parsed.get('msg')} body={body[:512]}", file=sys.stderr)
        raise SystemExit(2)
    return parsed


def request_json(
    base_url: str,
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    body: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
) -> Dict[str, Any]:
    status, raw = do_request(
        base_url, method, path, api_key, api_secret, passphrase, body=body, timeout=timeout
    )
    return parse_json_response(path, status, raw)


def to_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def format_amount(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def coin_row(coin: str, available: Any, frozen: Any = 0, extra: str = "") -> str:
    suffix = f" {extra}" if extra else ""
    return (
        f"  {coin}: available={available} frozen={frozen}{suffix}"
    )


def print_funding_assets(data: Any, coin_filter: Optional[str] = None) -> Dict[str, Decimal]:
    print("Funding / spot wallet (/api/v3/account/funding-assets):")
    available_by_coin: Dict[str, Decimal] = {}
    rows: List[Dict[str, Any]] = data if isinstance(data, list) else []
    shown = 0
    for item in rows:
        if not isinstance(item, dict):
            continue
        coin = str(item.get("coin") or "").upper()
        if not coin:
            continue
        available = to_decimal(item.get("available") or 0)
        frozen = to_decimal(item.get("frozen") or 0)
        balance = to_decimal(item.get("balance") or 0)
        available_by_coin[coin] = available
        if coin_filter and coin != coin_filter:
            continue
        if available == 0 and frozen == 0 and balance == 0:
            continue
        print(coin_row(coin, available, frozen, extra=f"balance={balance}"))
        shown += 1
    if shown == 0:
        print("  (empty)")
    return available_by_coin


def print_uta_assets(data: Any, coin_filter: Optional[str] = None) -> None:
    if not isinstance(data, dict):
        print("UTA trading wallet: (unexpected payload)")
        return
    print("UTA trading wallet (/api/v3/account/assets):")
    print(
        "  "
        f"accountEquity={data.get('accountEquity')} "
        f"usdtEquity={data.get('usdtEquity')} "
        f"effEquity={data.get('effEquity')} "
        f"unrealisedPnl={data.get('unrealisedPnl')}"
    )
    assets = data.get("assets") or []
    shown = 0
    if isinstance(assets, list):
        for item in assets:
            if not isinstance(item, dict):
                continue
            coin = str(item.get("coin") or item.get("coinName") or "").upper()
            if coin_filter and coin != coin_filter:
                continue
            available = item.get("available") or item.get("availableBalance") or 0
            frozen = item.get("frozen") or item.get("frozenBalance") or 0
            equity = item.get("equity") or item.get("usdValue") or ""
            extra = f"equity={equity}" if equity != "" else ""
            if to_decimal(available) == 0 and to_decimal(frozen) == 0 and to_decimal(equity) == 0:
                continue
            print(coin_row(coin or "?", available, frozen, extra=extra))
            shown += 1
    if shown == 0:
        print("  (no non-zero assets)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transfer coins between Bitget UTA wallets (funding/spot <-> uta)"
    )
    parser.add_argument(
        "--env-file",
        default="",
        help="Optional shell env file to source (BITGET_API_KEY/SECRET/PASSPHRASE)",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Bitget REST base URL")
    parser.add_argument("--coin", default="USDT", help="Coin to transfer")
    parser.add_argument(
        "--amount",
        default="",
        help="Transfer amount. Omit with --all to send full funding available",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Transfer the full available balance of --coin in the source wallet",
    )
    parser.add_argument("--from", dest="from_type", default="spot", choices=ACCOUNT_TYPES)
    parser.add_argument("--to", dest="to_type", default="uta", choices=ACCOUNT_TYPES)
    parser.add_argument(
        "--symbol",
        default="",
        help="Isolated margin trading pair, required only for isolated_margin",
    )
    parser.add_argument(
        "--client-oid",
        default="",
        help="Optional client order id (max 64 chars)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually submit the transfer; omit for dry-run",
    )
    parser.add_argument("--timeout", type=int, default=10, help="HTTP timeout seconds")
    return parser.parse_args()


def resolve_amount(args: argparse.Namespace, available: Optional[Decimal]) -> str:
    if args.all and args.amount:
        print("ERROR: use either --amount or --all, not both.", file=sys.stderr)
        raise SystemExit(1)
    if args.all:
        if available is None:
            print("ERROR: --all is only supported when --from spot (funding wallet).", file=sys.stderr)
            raise SystemExit(1)
        if available <= 0:
            print(f"ERROR: no available {args.coin.upper()} in funding wallet.", file=sys.stderr)
            raise SystemExit(1)
        # Keep 8 dp to stay inside USDT precision without sending dust tails.
        quantized = available.quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
        if quantized <= 0:
            print(f"ERROR: available {args.coin.upper()} is dust-only: {available}", file=sys.stderr)
            raise SystemExit(1)
        return format_amount(quantized)
    if not args.amount:
        print("ERROR: provide --amount or --all.", file=sys.stderr)
        raise SystemExit(1)
    amount = to_decimal(args.amount)
    if amount <= 0:
        print("ERROR: --amount must be > 0.", file=sys.stderr)
        raise SystemExit(1)
    return str(args.amount).strip()


def main() -> int:
    args = parse_args()
    if args.env_file:
        source_env_file(args.env_file)

    api_key, api_secret, passphrase = load_credentials()
    base_url = args.base_url.rstrip("/")
    coin = args.coin.upper().strip()
    if args.from_type == args.to_type:
        print("ERROR: --from and --to must differ.", file=sys.stderr)
        return 1

    funding = request_json(
        base_url, "GET", FUNDING_ASSETS_PATH, api_key, api_secret, passphrase, timeout=args.timeout
    )
    uta = request_json(
        base_url, "GET", UTA_ASSETS_PATH, api_key, api_secret, passphrase, timeout=args.timeout
    )
    available_by_coin = print_funding_assets(funding.get("data"), coin_filter=None)
    print()
    print_uta_assets(uta.get("data"), coin_filter=None)
    print()

    source_available = available_by_coin.get(coin) if args.from_type == "spot" else None
    amount = resolve_amount(args, source_available)
    payload: Dict[str, Any] = {
        "fromType": args.from_type,
        "toType": args.to_type,
        "coin": coin,
        "amount": amount,
    }
    if args.symbol:
        payload["symbol"] = args.symbol
    if args.client_oid:
        payload["clientOid"] = args.client_oid

    print(
        "Prepared Bitget UTA transfer: "
        f"from={payload['fromType']} to={payload['toType']} "
        f"coin={payload['coin']} amount={payload['amount']}"
    )
    if source_available is not None:
        print(f"Source funding available {coin}: {format_amount(source_available)}")

    if not args.execute:
        print("Dry-run only. Add --execute to submit the transfer.")
        return 0

    result = request_json(
        base_url,
        "POST",
        TRANSFER_PATH,
        api_key,
        api_secret,
        passphrase,
        body=payload,
        timeout=args.timeout,
    )
    print("Transfer result:")
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))

    funding_after = request_json(
        base_url, "GET", FUNDING_ASSETS_PATH, api_key, api_secret, passphrase, timeout=args.timeout
    )
    uta_after = request_json(
        base_url, "GET", UTA_ASSETS_PATH, api_key, api_secret, passphrase, timeout=args.timeout
    )
    print()
    print_funding_assets(funding_after.get("data"), coin_filter=None)
    print()
    print_uta_assets(uta_after.get("data"), coin_filter=None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
