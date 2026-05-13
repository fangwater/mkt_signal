#!/usr/bin/env python3
"""Cancel Binance Portfolio Margin open orders (UM + cross-margin).

Self-contained: no imports from other scripts in this repo.

Behavior:
  - Must run from an env dir whose basename matches ^binance_fr_ (CWD safety check).
  - Auto-sources ./env.sh from CWD if present; values already in process env take
    precedence. BINANCE_API_KEY / BINANCE_API_SECRET are then read from os.environ.
  - Lists open orders, prints plan; with --execute, calls the cancel-all endpoint
    per (scope, symbol).

Usage:
  python3 scripts/cancel_binance_pm_orders.py                                  # dry-run, all symbols, both scopes
  python3 scripts/cancel_binance_pm_orders.py --symbols BTCUSDT,ETHUSDT        # dry-run, two symbols
  python3 scripts/cancel_binance_pm_orders.py --scope um --execute             # cancel UM only
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple


BINANCE_PAPI_BASE = os.environ.get("BINANCE_PAPI_URL", "https://papi.binance.com")

UM_OPEN_ORDERS_PATH = "/papi/v1/um/openOrders"
UM_CANCEL_ALL_PATH = "/papi/v1/um/allOpenOrders"
MARGIN_OPEN_ORDERS_PATH = "/papi/v1/margin/openOrders"
MARGIN_CANCEL_ALL_PATH = "/papi/v1/margin/allOpenOrders"

ENV_DIR_PATTERN = re.compile(r"^binance_fr_")


def now_ms() -> int:
    return int(time.time() * 1000)


def http_request(
    url: str,
    *,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 10,
) -> Tuple[int, str]:
    req = urllib.request.Request(url, method=method.upper())
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001
        return 0, str(exc)


def binance_sign(query: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def signed_request(
    path: str,
    params: Dict[str, Any],
    api_key: str,
    api_secret: str,
    *,
    method: str = "GET",
    timeout: int = 10,
) -> Tuple[int, str]:
    payload = dict(params)
    payload.setdefault("recvWindow", "5000")
    payload["timestamp"] = str(now_ms())
    query = urllib.parse.urlencode(sorted(payload.items(), key=lambda kv: kv[0]), safe="-_.~")
    signature = binance_sign(query, api_secret)
    url = f"{BINANCE_PAPI_BASE}{path}?{query}&signature={signature}"
    return http_request(url, method=method, headers={"X-MBX-APIKEY": api_key}, timeout=timeout)


def check_env_safety() -> str:
    cwd_name = os.path.basename(os.path.normpath(os.getcwd()))
    if not ENV_DIR_PATTERN.match(cwd_name):
        sys.stderr.write(
            f"[ERROR] CWD basename must match ^binance_fr_, got {cwd_name!r} "
            f"(CWD={os.getcwd()}). Aborting for safety.\n"
        )
        sys.exit(2)
    return cwd_name


AUTHORITATIVE_KEYS = ("BINANCE_API_KEY", "BINANCE_API_SECRET")


def auto_source_env_sh() -> None:
    """Source ./env.sh into os.environ if it exists.

    For BINANCE_API_KEY / BINANCE_API_SECRET, env.sh always wins (overrides any
    pre-existing process env) — running in the wrong account silently is a much
    worse failure than ignoring an intentional manual override. A warning is
    printed when env.sh actually overrides a different pre-existing value.

    For every other key, only fills if not already present in os.environ.
    """
    env_path = os.path.join(os.getcwd(), "env.sh")
    if not os.path.isfile(env_path):
        return
    proc = subprocess.run(
        ["bash", "-lc", f"set -a; source {env_path} >/dev/null 2>&1; env -0"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        return
    for item in proc.stdout.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key_b, value_b = item.split(b"=", 1)
        key = key_b.decode("utf-8", errors="ignore")
        new_value = value_b.decode("utf-8", errors="replace")
        if key in AUTHORITATIVE_KEYS:
            old_value = os.environ.get(key)
            if old_value and old_value != new_value:
                sys.stderr.write(
                    f"[WARN] env.sh overrides existing {key} from process env "
                    f"(was set by calling shell; env.sh value wins to prevent cross-account ops)\n"
                )
            os.environ[key] = new_value
        else:
            if key in os.environ:
                continue
            os.environ[key] = new_value


def load_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        sys.stderr.write(
            "[ERROR] missing BINANCE_API_KEY / BINANCE_API_SECRET "
            "(checked process env and ./env.sh).\n"
        )
        sys.exit(2)
    return api_key, api_secret


def parse_symbols(raw: Optional[str]) -> Optional[List[str]]:
    if not raw:
        return None
    out: List[str] = []
    for tok in raw.split(","):
        sym = tok.strip().upper()
        if sym:
            out.append(sym)
    return out or None


def fetch_open_orders(
    path: str, api_key: str, api_secret: str, *, symbol: Optional[str] = None
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if symbol:
        params["symbol"] = symbol
    status, body = signed_request(path, params, api_key, api_secret, method="GET")
    if not (200 <= status < 300):
        sys.stderr.write(f"[ERROR] {path} status={status} body={body}\n")
        return []
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        sys.stderr.write(f"[ERROR] {path} returned non-JSON body: {body}\n")
        return []
    if not isinstance(data, list):
        sys.stderr.write(f"[ERROR] {path} expected list, got {type(data).__name__}: {body}\n")
        return []
    return data


def count_by_symbol(orders: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for order in orders:
        sym = str(order.get("symbol", "")).strip().upper()
        if not sym:
            continue
        counts[sym] = counts.get(sym, 0) + 1
    return counts


def filter_counts(counts: Dict[str, int], wanted: Optional[List[str]]) -> Dict[str, int]:
    if wanted is None:
        return counts
    wanted_set = set(wanted)
    return {sym: n for sym, n in counts.items() if sym in wanted_set}


def cancel_all(
    path: str,
    symbol: str,
    api_key: str,
    api_secret: str,
) -> Tuple[int, str]:
    return signed_request(
        path,
        {"symbol": symbol},
        api_key,
        api_secret,
        method="DELETE",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cancel Binance PM open orders (UM + margin)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--symbols",
        help="Comma-separated symbol whitelist (e.g. BTCUSDT,ETHUSDT). Omit to cancel all symbols with open orders.",
    )
    parser.add_argument(
        "--scope",
        choices=["um", "margin", "both"],
        default="both",
        help="Which order scope(s) to cancel",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually call cancel-all; default is dry-run",
    )
    return parser.parse_args()


def print_plan(
    env_name: str,
    um_counts: Dict[str, int],
    margin_counts: Dict[str, int],
    scope: str,
    execute: bool,
) -> List[str]:
    all_symbols = sorted(set(um_counts) | set(margin_counts))
    print(f"[info] env={env_name} scope={scope} execute={execute}")
    if not all_symbols:
        print("[plan] no open orders in scope. Nothing to do.")
        return []
    print()
    print(f"{'Symbol':<14} {'UM open':>8} {'Margin open':>12}")
    print("-" * 38)
    for sym in all_symbols:
        u = um_counts.get(sym, 0)
        m = margin_counts.get(sym, 0)
        print(f"{sym:<14} {u:>8} {m:>12}")
    print("-" * 38)
    print(f"Total symbols: {len(all_symbols)}")
    return all_symbols


def main() -> None:
    args = parse_args()
    env_name = check_env_safety()
    auto_source_env_sh()
    api_key, api_secret = load_credentials()
    wanted = parse_symbols(args.symbols)

    um_orders: List[Dict[str, Any]] = []
    margin_orders: List[Dict[str, Any]] = []
    if args.scope in ("um", "both"):
        um_orders = fetch_open_orders(UM_OPEN_ORDERS_PATH, api_key, api_secret)
    if args.scope in ("margin", "both"):
        margin_orders = fetch_open_orders(MARGIN_OPEN_ORDERS_PATH, api_key, api_secret)

    um_counts = filter_counts(count_by_symbol(um_orders), wanted)
    margin_counts = filter_counts(count_by_symbol(margin_orders), wanted)

    symbols = print_plan(env_name, um_counts, margin_counts, args.scope, args.execute)
    if not args.execute:
        if symbols:
            print("\nDry-run. Pass --execute to actually cancel.")
        return

    if not symbols:
        return

    print("\n" + "=" * 50)
    print("EXECUTING CANCELS")
    print("=" * 50)

    failures = 0
    total = 0
    for sym in symbols:
        if args.scope in ("um", "both") and um_counts.get(sym, 0) > 0:
            total += 1
            status, body = cancel_all(UM_CANCEL_ALL_PATH, sym, api_key, api_secret)
            tag = "OK" if 200 <= status < 300 else "ERR"
            print(f"  [{tag}] UM     cancel {sym} status={status}")
            if not (200 <= status < 300):
                print(f"    {body}")
                failures += 1
        if args.scope in ("margin", "both") and margin_counts.get(sym, 0) > 0:
            total += 1
            status, body = cancel_all(MARGIN_CANCEL_ALL_PATH, sym, api_key, api_secret)
            tag = "OK" if 200 <= status < 300 else "ERR"
            print(f"  [{tag}] MARGIN cancel {sym} status={status}")
            if not (200 <= status < 300):
                print(f"    {body}")
                failures += 1

    print()
    if failures:
        print(f"WARN: {failures}/{total} cancel requests failed", file=sys.stderr)
        sys.exit(1)
    print(f"All {total} cancel requests succeeded.")


if __name__ == "__main__":
    main()
