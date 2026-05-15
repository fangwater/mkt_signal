#!/usr/bin/env python3
"""Cancel Bitget UTA open orders (USDT-FUTURES + MARGIN spot).

Self-contained: no imports from other scripts in this repo.

Behavior:
  - CWD basename must match ^bitget_fr_ or ^bitget-intra-.
  - Auto-sources ./env.sh; BITGET_API_KEY/SECRET/PASSPHRASE — env.sh always wins.
  - POST /api/v3/trade/cancel-symbol-order per (category, symbol).

Usage:
  python3 scripts/cancel_bitget_pm_orders.py
  python3 scripts/cancel_bitget_pm_orders.py --symbols BTCUSDT --execute
"""

from __future__ import annotations

import argparse
import base64
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


BITGET_BASE = os.environ.get("BITGET_API_BASE", "https://api.bitget.com").rstrip("/")
ENV_DIR_PATTERN = re.compile(r"^(bitget_fr_|bitget[-_]intra[-_])")
AUTHORITATIVE_KEYS = ("BITGET_API_KEY", "BITGET_API_SECRET", "BITGET_PASSPHRASE", "BITGET_API_PASSPHRASE")


def http_request(url, *, method="GET", headers=None, data=None, timeout=15):
    req = urllib.request.Request(url, data=data, method=method.upper())
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001
        return 0, str(exc)


def bitget_sign(api_secret, ts_ms, method, request_path_with_query, body):
    payload = f"{ts_ms}{method.upper()}{request_path_with_query}{body}"
    return base64.b64encode(
        hmac.new(api_secret.encode(), payload.encode(), hashlib.sha256).digest()
    ).decode()


def bitget_private(method, path, api_key, api_secret, passphrase, *, query=None, body=None, timeout=15):
    method = method.upper()
    query_str = ""
    if query:
        query_str = urllib.parse.urlencode(sorted(query.items(), key=lambda kv: kv[0]))
    request_path = path
    if query_str:
        request_path = f"{path}?{query_str}"
    body_text = "" if (method == "GET" or body is None) else json.dumps(body, ensure_ascii=False, separators=(",", ":"))
    ts_ms = str(int(time.time() * 1000))
    sig = bitget_sign(api_secret, ts_ms, method, request_path, body_text)
    headers = {
        "ACCESS-KEY": api_key,
        "ACCESS-SIGN": sig,
        "ACCESS-TIMESTAMP": ts_ms,
        "ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
        "locale": "en-US",
    }
    return http_request(
        f"{BITGET_BASE}{request_path}",
        method=method, headers=headers,
        data=None if method == "GET" else body_text.encode("utf-8"),
        timeout=timeout,
    )


def bitget_ok(body: str) -> Tuple[bool, str]:
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return False, "non-json"
    code = str(parsed.get("code", ""))
    if code in ("0", "00000"):
        return True, ""
    return False, f"code={code} msg={parsed.get('msg', '')}"


def check_env_safety() -> str:
    cwd_name = os.path.basename(os.path.normpath(os.getcwd()))
    if not ENV_DIR_PATTERN.match(cwd_name):
        sys.stderr.write(
            f"[ERROR] CWD basename must match ^bitget_fr_ or ^bitget-intra-, got {cwd_name!r} "
            f"(CWD={os.getcwd()}). Aborting for safety.\n"
        )
        sys.exit(2)
    return cwd_name


def auto_source_env_sh() -> None:
    env_path = os.path.join(os.getcwd(), "env.sh")
    if not os.path.isfile(env_path):
        return
    proc = subprocess.run(
        ["bash", "-lc", f"set -a; source {env_path} >/dev/null 2>&1; env -0"],
        check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
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
            old = os.environ.get(key)
            if old and old != new_value:
                sys.stderr.write(
                    f"[WARN] env.sh overrides existing {key} from process env "
                    f"(env.sh wins to prevent cross-account ops)\n"
                )
            os.environ[key] = new_value
        else:
            if key in os.environ:
                continue
            os.environ[key] = new_value


def load_credentials() -> Tuple[str, str, str]:
    k = os.environ.get("BITGET_API_KEY", "").strip()
    s = os.environ.get("BITGET_API_SECRET", "").strip()
    p = (os.environ.get("BITGET_PASSPHRASE", "") or os.environ.get("BITGET_API_PASSPHRASE", "")).strip()
    if not k or not s or not p:
        sys.stderr.write(
            "[ERROR] missing BITGET_API_KEY / BITGET_API_SECRET / "
            "BITGET_PASSPHRASE (or BITGET_API_PASSPHRASE).\n"
        )
        sys.exit(2)
    return k, s, p


def parse_symbols(raw: Optional[str]) -> Optional[List[str]]:
    if not raw:
        return None
    out = []
    for tok in raw.split(","):
        s = tok.strip().upper()
        if s:
            out.append(s)
    return out or None


def fetch_unfilled(api_key, api_secret, passphrase, category: str) -> List[Dict[str, Any]]:
    status, body = bitget_private(
        "GET", "/api/v3/trade/unfilled-orders", api_key, api_secret, passphrase,
        query={"category": category},
    )
    if not (200 <= status < 300):
        sys.stderr.write(f"[WARN] unfilled-orders {category} status={status} body={body}\n")
        return []
    parsed = json.loads(body)
    if str(parsed.get("code", "")) not in ("0", "00000"):
        sys.stderr.write(f"[WARN] unfilled-orders {category}: {body}\n")
        return []
    data = parsed.get("data")
    if isinstance(data, dict):
        return data.get("orderList", []) or data.get("list", []) or []
    if isinstance(data, list):
        return data
    return []


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cancel Bitget UTA open orders (futures + margin spot)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--symbols", help="Comma-separated symbol whitelist; omit = all")
    parser.add_argument("--scope", choices=["um", "margin", "both"], default="both",
                        help="um→USDT-FUTURES, margin→MARGIN spot")
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    env_name = check_env_safety()
    auto_source_env_sh()
    api_key, api_secret, passphrase = load_credentials()
    wanted = parse_symbols(args.symbols)
    wanted_set = set(wanted) if wanted else None

    print(f"[info] env={env_name} scope={args.scope} execute={args.execute}")

    fut_count: Dict[str, int] = {}
    margin_count: Dict[str, int] = {}

    if args.scope in ("um", "both"):
        for o in fetch_unfilled(api_key, api_secret, passphrase, "USDT-FUTURES"):
            sym = str(o.get("symbol", "")).upper()
            if not sym:
                continue
            if wanted_set is not None and sym not in wanted_set:
                continue
            fut_count[sym] = fut_count.get(sym, 0) + 1
    if args.scope in ("margin", "both"):
        for o in fetch_unfilled(api_key, api_secret, passphrase, "MARGIN"):
            sym = str(o.get("symbol", "")).upper()
            if not sym:
                continue
            if wanted_set is not None and sym not in wanted_set:
                continue
            margin_count[sym] = margin_count.get(sym, 0) + 1

    all_syms = sorted(set(fut_count) | set(margin_count))
    if not all_syms:
        print("[plan] no open orders in scope. Nothing to do.")
        return

    print()
    print(f"{'Symbol':<14} {'Futures':>8} {'Margin':>8}")
    print("-" * 34)
    for s in all_syms:
        print(f"{s:<14} {fut_count.get(s, 0):>8} {margin_count.get(s, 0):>8}")
    print("-" * 34)

    if not args.execute:
        print("\nDry-run. Pass --execute to actually cancel.")
        return

    print("\n" + "=" * 50)
    print("EXECUTING CANCELS")
    print("=" * 50)

    failures = 0
    total = 0
    for sym in all_syms:
        if fut_count.get(sym, 0) > 0:
            total += 1
            status, body = bitget_private(
                "POST", "/api/v3/trade/cancel-symbol-order", api_key, api_secret, passphrase,
                body={"category": "USDT-FUTURES", "symbol": sym},
            )
            ok_ret, brief = bitget_ok(body)
            ok = (200 <= status < 300) and ok_ret
            print(f"  [{'OK' if ok else 'ERR'}] futures cancel {sym} status={status} {brief}")
            if not ok:
                failures += 1
                print(f"    {body}")
        if margin_count.get(sym, 0) > 0:
            total += 1
            status, body = bitget_private(
                "POST", "/api/v3/trade/cancel-symbol-order", api_key, api_secret, passphrase,
                body={"category": "MARGIN", "symbol": sym},
            )
            ok_ret, brief = bitget_ok(body)
            ok = (200 <= status < 300) and ok_ret
            print(f"  [{'OK' if ok else 'ERR'}] margin cancel {sym} status={status} {brief}")
            if not ok:
                failures += 1
                print(f"    {body}")

    print()
    if failures:
        print(f"WARN: {failures}/{total} cancels failed", file=sys.stderr)
        sys.exit(1)
    print(f"All {total} cancel requests succeeded.")


if __name__ == "__main__":
    main()
