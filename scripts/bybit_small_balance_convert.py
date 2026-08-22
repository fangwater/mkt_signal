#!/usr/bin/env python3
"""Convert Bybit UTA small balances via Convert Small Balances API.

Default mode is dry-run: list eligible small balances and request a quote.
Add --execute to confirm the quote.

Examples:
  python3 scripts/bybit_small_balance_convert.py --env-name bybit-intra-arb01
  python3 scripts/bybit_small_balance_convert.py --assets LIT,EIGEN,ETHFI --to-coin USDT
  python3 scripts/bybit_small_balance_convert.py --all-listed --to-coin USDT --execute
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
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_BASE_URL = os.environ.get("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
RECV_WINDOW_MS = "5000"
ACCOUNT_TYPE = "eb_convert_uta"
AUTHORITATIVE_KEYS = ("BYBIT_API_KEY", "BYBIT_API_SECRET", "BYBIT_API_BASE")


@dataclass
class SmallBalanceCoin:
    from_coin: str
    support_convert: int
    available_balance: Decimal
    base_value: Decimal
    raw: Dict[str, Any]


def dec(value: Any, default: str = "0") -> Decimal:
    if value in (None, ""):
        return Decimal(default)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal(default)


def format_decimal(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Bybit UTA small balances to MNT/USDT/USDC",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--env-name", help="Source $HOME/<env-name>/env.sh before calling Bybit")
    parser.add_argument("--env-dir", help="Source <env-dir>/env.sh before calling Bybit")
    parser.add_argument("--no-env-sh", action="store_true", help="Do not auto-source env.sh")
    parser.add_argument("--assets", help="Comma/space separated asset list to include")
    parser.add_argument("--asset", action="append", default=[], help="Append one asset; repeatable")
    parser.add_argument("--skip-assets", help="Comma/space separated assets to exclude")
    parser.add_argument(
        "--all-listed",
        action="store_true",
        help="Use all currently convertible coins returned by Bybit",
    )
    parser.add_argument(
        "--to-coin",
        choices=["MNT", "USDT", "USDC"],
        default="USDT",
        help="Target currency for conversion",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Bybit REST base URL")
    parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds")
    parser.add_argument("--max-batch", type=int, default=20, help="Max fromCoinList size per quote")
    parser.add_argument("--sleep-sec", type=float, default=1.0, help="Sleep between batches")
    parser.add_argument("--print-json", action="store_true", help="Print raw JSON responses")
    parser.add_argument("--execute", action="store_true", help="Confirm quoted conversions")
    return parser.parse_args()


def normalize_assets(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in values:
        for part in str(raw).replace(",", " ").split():
            asset = part.strip().upper()
            if asset.endswith("USDT") and len(asset) > 4:
                asset = asset[:-4]
            if asset and asset not in seen:
                seen.add(asset)
                out.append(asset)
    return out


def resolve_env_file(args: argparse.Namespace) -> Optional[Path]:
    if args.no_env_sh:
        return None
    if args.env_dir:
        return Path(args.env_dir).expanduser().resolve() / "env.sh"
    if args.env_name:
        return Path.home() / args.env_name / "env.sh"
    cwd_env = Path.cwd() / "env.sh"
    if cwd_env.is_file():
        return cwd_env
    return None


def auto_source_env_sh(env_path: Path) -> None:
    if not env_path.is_file():
        raise SystemExit(f"env.sh not found: {env_path}")
    quoted = shlex.quote(str(env_path))
    proc = subprocess.run(
        ["bash", "-lc", f"set -a; source {quoted} >/dev/null 2>&1; env -0"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        raise SystemExit(f"failed to source env.sh: {env_path}")
    for item in proc.stdout.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key_b, value_b = item.split(b"=", 1)
        key = key_b.decode("utf-8", errors="ignore")
        new_value = value_b.decode("utf-8", errors="replace")
        if key in AUTHORITATIVE_KEYS:
            old = os.environ.get(key)
            if old and old != new_value:
                print(
                    f"[WARN] env.sh overrides existing {key} from process env",
                    file=sys.stderr,
                )
            os.environ[key] = new_value
        elif key not in os.environ:
            os.environ[key] = new_value


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


def compact_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def sign(api_key: str, api_secret: str, payload: str) -> Tuple[str, str]:
    timestamp_ms = str(int(time.time() * 1000))
    raw = f"{timestamp_ms}{api_key}{RECV_WINDOW_MS}{payload}"
    signature = hmac.new(api_secret.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()
    return timestamp_ms, signature


def bybit_query(params: Dict[str, Any]) -> str:
    items = [(key, str(value)) for key, value in params.items() if value not in ("", None)]
    items.sort(key=lambda item: item[0])
    return urllib.parse.urlencode(items, safe="-_.~")


def request_bybit(
    base_url: str,
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    timeout: int = 15,
) -> Dict[str, Any]:
    method = method.upper()
    query = bybit_query(params or {}) if method == "GET" else ""
    body_text = "" if method == "GET" else compact_json(body or {})
    payload = query if method == "GET" else body_text
    timestamp_ms, signature = sign(api_key, api_secret, payload)
    url = f"{base_url.rstrip('/')}{path}"
    if query:
        url = f"{url}?{query}"
    req = urllib.request.Request(
        url,
        data=None if method == "GET" else body_text.encode("utf-8"),
        method=method,
    )
    req.add_header("Content-Type", "application/json")
    req.add_header("X-BAPI-API-KEY", api_key)
    req.add_header("X-BAPI-SIGN", signature)
    req.add_header("X-BAPI-SIGN-TYPE", "2")
    req.add_header("X-BAPI-TIMESTAMP", timestamp_ms)
    req.add_header("X-BAPI-RECV-WINDOW", RECV_WINDOW_MS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            text = resp.read().decode("utf-8", "replace")
            status = resp.getcode()
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", "replace")
        status = exc.code
    except Exception as exc:  # noqa: BLE001
        return {"http_status": 0, "retCode": None, "retMsg": str(exc), "result": {}}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = {"retCode": None, "retMsg": "non-json response", "raw": text}
    parsed["http_status"] = status
    return parsed


def require_ok(data: Dict[str, Any], label: str) -> None:
    if data.get("http_status") != 200 or data.get("retCode") != 0:
        raise SystemExit(
            f"{label} failed: http={data.get('http_status')} "
            f"retCode={data.get('retCode')} retMsg={data.get('retMsg')} "
            f"body={json.dumps(data, ensure_ascii=False)[:800]}"
        )


def parse_small_balance_list(data: Dict[str, Any]) -> Tuple[List[SmallBalanceCoin], List[str]]:
    result = data.get("result") if isinstance(data, dict) else {}
    coins: List[SmallBalanceCoin] = []
    for row in (result or {}).get("smallAssetCoins", []) or []:
        if not isinstance(row, dict):
            continue
        from_coin = str(row.get("fromCoin", "")).upper()
        if not from_coin:
            continue
        coins.append(
            SmallBalanceCoin(
                from_coin=from_coin,
                support_convert=int(row.get("supportConvert") or 0),
                available_balance=dec(row.get("availableBalance")),
                base_value=dec(row.get("baseValue")),
                raw=row,
            )
        )
    support_to = [str(item).upper() for item in (result or {}).get("supportToCoins", []) or []]
    return coins, support_to


def fetch_small_balance_list(
    base_url: str,
    api_key: str,
    api_secret: str,
    *,
    timeout: int,
    from_coin: str = "",
) -> Dict[str, Any]:
    params = {"accountType": ACCOUNT_TYPE}
    if from_coin:
        params["fromCoin"] = from_coin.upper()
    return request_bybit(
        base_url,
        "GET",
        "/v5/asset/covert/small-balance-list",
        api_key,
        api_secret,
        params=params,
        timeout=timeout,
    )


def request_quote(
    base_url: str,
    api_key: str,
    api_secret: str,
    *,
    from_coins: Sequence[str],
    to_coin: str,
    timeout: int,
) -> Dict[str, Any]:
    return request_bybit(
        base_url,
        "POST",
        "/v5/asset/covert/get-quote",
        api_key,
        api_secret,
        body={
            "accountType": ACCOUNT_TYPE,
            "fromCoinList": list(from_coins),
            "toCoin": to_coin,
        },
        timeout=timeout,
    )


def confirm_quote(
    base_url: str,
    api_key: str,
    api_secret: str,
    *,
    quote_id: str,
    timeout: int,
) -> Dict[str, Any]:
    return request_bybit(
        base_url,
        "POST",
        "/v5/asset/covert/small-balance-execute",
        api_key,
        api_secret,
        body={"quoteId": quote_id},
        timeout=timeout,
    )


def fetch_history(
    base_url: str,
    api_key: str,
    api_secret: str,
    *,
    quote_id: str,
    timeout: int,
) -> Dict[str, Any]:
    return request_bybit(
        base_url,
        "GET",
        "/v5/asset/covert/small-balance-history",
        api_key,
        api_secret,
        params={"accountType": ACCOUNT_TYPE, "quoteId": quote_id},
        timeout=timeout,
    )


def chunks(values: Sequence[str], size: int) -> Iterable[List[str]]:
    for idx in range(0, len(values), size):
        yield list(values[idx : idx + size])


def print_coin_table(coins: Sequence[SmallBalanceCoin], selected: Sequence[str]) -> None:
    selected_set = set(selected)
    print()
    print(f"{'Coin':<10} {'Support':>7} {'Avail':>22} {'BaseUSDT':>14} Selected")
    print("-" * 70)
    for coin in sorted(coins, key=lambda item: item.base_value, reverse=True):
        print(
            f"{coin.from_coin:<10} {coin.support_convert:>7} "
            f"{format_decimal(coin.available_balance):>22} "
            f"{format_decimal(coin.base_value):>14} "
            f"{'yes' if coin.from_coin in selected_set else ''}"
        )
    print("-" * 70)


def quote_exchange_rows(quote: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = quote.get("result") or {}
    detail = result.get("result") or {}
    rows = detail.get("exchangeCoins") or []
    return [row for row in rows if isinstance(row, dict)]


def print_quote(quote: Dict[str, Any]) -> Optional[str]:
    result = quote.get("result") or {}
    quote_id = str(result.get("quoteId") or "")
    detail = result.get("result") or {}
    rows = quote_exchange_rows(quote)
    print(f"\n[quote] quoteId={quote_id or '-'} expires={detail.get('quoteExpireTime', '-')}")
    print(f"{'Coin':<10} {'Support':>7} {'Avail':>22} {'BaseUSDT':>14} {'To':>6} {'ToAmount':>22}")
    print("-" * 92)
    for row in rows:
        print(
            f"{str(row.get('fromCoin', '')):<10} "
            f"{str(row.get('supportConvert', '')):>7} "
            f"{str(row.get('availableBalance', '')):>22} "
            f"{str(row.get('baseValue', '')):>14} "
            f"{str(row.get('toCoin', '')):>6} "
            f"{str(row.get('toAmount', '')):>22}"
        )
    total_fee = detail.get("totalFeeInfo") or {}
    print("-" * 92)
    print(
        f"total_fee={total_fee.get('amount', '0')} {total_fee.get('feeCoin', '')} "
        f"fee_rate={total_fee.get('feeRate', '')}"
    )
    return quote_id or None


def main() -> int:
    args = parse_args()
    env_file = resolve_env_file(args)
    if env_file is not None:
        auto_source_env_sh(env_file)
    api_key, api_secret = load_credentials()
    base_url = args.base_url.rstrip("/")

    include_assets = normalize_assets((args.assets or "").split() + args.asset)
    skip_assets = set(normalize_assets([args.skip_assets or ""]))
    if args.max_batch <= 0 or args.max_batch > 20:
        raise SystemExit("--max-batch must be in 1..20")

    raw_list = fetch_small_balance_list(
        base_url,
        api_key,
        api_secret,
        timeout=args.timeout,
    )
    require_ok(raw_list, "small-balance-list")
    if args.print_json:
        print(json.dumps(raw_list, ensure_ascii=False, indent=2))
    coins, support_to = parse_small_balance_list(raw_list)
    if support_to and args.to_coin not in support_to:
        raise SystemExit(f"--to-coin {args.to_coin} is not in supportToCoins={support_to}")

    by_coin = {coin.from_coin: coin for coin in coins}
    if include_assets:
        missing = [asset for asset in include_assets if asset not in by_coin]
        selected = [asset for asset in include_assets if asset in by_coin]
        if missing:
            print(f"[warn] requested assets not currently listed as small balances: {','.join(missing)}")
    elif args.all-listed:
        selected = [coin.from_coin for coin in coins]
    else:
        selected = [coin.from_coin for coin in coins]
    selected = [
        asset
        for asset in selected
        if asset not in skip_assets
        and by_coin[asset].support_convert == 1
        and by_coin[asset].available_balance > 0
        and by_coin[asset].base_value > 0
    ]

    print(
        f"[info] base={base_url} accountType={ACCOUNT_TYPE} "
        f"listed={len(coins)} selected={len(selected)} toCoin={args.to_coin} execute={args.execute}"
    )
    print_coin_table(coins, selected)
    if not selected:
        print("No convertible small balances selected.")
        return 0

    failures = 0
    for idx, batch in enumerate(chunks(selected, args.max_batch), start=1):
        print(f"\n[batch {idx}] coins={','.join(batch)}")
        quote = request_quote(
            base_url,
            api_key,
            api_secret,
            from_coins=batch,
            to_coin=args.to_coin,
            timeout=args.timeout,
        )
        if args.print_json:
            print(json.dumps(quote, ensure_ascii=False, indent=2))
        if quote.get("http_status") != 200 or quote.get("retCode") != 0:
            failures += 1
            print(
                f"[ERR] quote failed http={quote.get('http_status')} "
                f"retCode={quote.get('retCode')} retMsg={quote.get('retMsg')}"
            )
            time.sleep(args.sleep_sec)
            continue
        quote_id = print_quote(quote)
        if not args.execute:
            print("[dry-run] quote was not confirmed; pass --execute to convert this batch.")
            time.sleep(args.sleep_sec)
            continue
        if not quote_id:
            failures += 1
            print("[ERR] quote response missing quoteId")
            time.sleep(args.sleep_sec)
            continue
        confirm = confirm_quote(
            base_url,
            api_key,
            api_secret,
            quote_id=quote_id,
            timeout=args.timeout,
        )
        if args.print_json:
            print(json.dumps(confirm, ensure_ascii=False, indent=2))
        if confirm.get("http_status") != 200 or confirm.get("retCode") != 0:
            failures += 1
            print(
                f"[ERR] confirm failed quoteId={quote_id} http={confirm.get('http_status')} "
                f"retCode={confirm.get('retCode')} retMsg={confirm.get('retMsg')}"
            )
            time.sleep(args.sleep_sec)
            continue
        result = confirm.get("result") or {}
        print(
            f"[OK] confirmed quoteId={quote_id} "
            f"exchangeTxId={result.get('exchangeTxId', '')} status={result.get('status', '')}"
        )
        time.sleep(args.sleep_sec)
        history = fetch_history(
            base_url,
            api_key,
            api_secret,
            quote_id=quote_id,
            timeout=args.timeout,
        )
        if history.get("http_status") == 200 and history.get("retCode") == 0:
            records = (history.get("result") or {}).get("records") or []
            for record in records:
                print(
                    f"[history] quoteId={quote_id} status={record.get('status', '')} "
                    f"to={record.get('toAmount', '')} {record.get('toCoin', '')}"
                )
        elif args.print_json:
            print(json.dumps(history, ensure_ascii=False, indent=2))
        time.sleep(args.sleep_sec)

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
