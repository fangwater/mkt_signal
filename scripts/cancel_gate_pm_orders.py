#!/usr/bin/env python3
"""Cancel Gate Unified Account open orders (futures USDT + spot unified).

Self-contained: no imports from other scripts in this repo.

Behavior:
  - CWD basename must match ^gate_fr_ or ^gate-intra-.
  - Auto-sources ./env.sh; GATE_API_KEY/SECRET — env.sh always wins.
  - Lists open orders, cancels all by contract / currency_pair.

Usage:
  python3 scripts/cancel_gate_pm_orders.py
  python3 scripts/cancel_gate_pm_orders.py --symbols BTCUSDT,ETHUSDT --execute
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


GATE_HOST = "https://api.gateio.ws"
GATE_PREFIX = "/api/v4"

ENV_DIR_PATTERN = re.compile(r"^(gate_fr_|gate[-_]intra[-_])")
AUTHORITATIVE_KEYS = ("GATE_API_KEY", "GATE_API_SECRET")


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


def gate_sign(method, path, query, body, secret, timestamp):
    hashed_body = hashlib.sha512(body.encode("utf-8")).hexdigest()
    sign_string = f"{method}\n{path}\n{query}\n{hashed_body}\n{timestamp}"
    return hmac.new(secret.encode("utf-8"), sign_string.encode("utf-8"), hashlib.sha512).hexdigest()


def gate_private(method, path, api_key, api_secret, *, params=None, body=None, timeout=15):
    method = method.upper()
    params = params or {}
    query = urllib.parse.urlencode(sorted(params.items(), key=lambda kv: kv[0]), doseq=True)
    body_text = "" if body is None else json.dumps(body, ensure_ascii=False, separators=(",", ":"))
    ts = str(int(time.time()))
    request_path = f"{GATE_PREFIX}{path}"
    signature = gate_sign(method, request_path, query, body_text, api_secret, ts)
    url = f"{GATE_HOST}{request_path}"
    if query:
        url = f"{url}?{query}"
    return http_request(
        url, method=method,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "KEY": api_key,
            "Timestamp": ts,
            "SIGN": signature,
        },
        data=None if method == "GET" else body_text.encode("utf-8"),
        timeout=timeout,
    )


def check_env_safety() -> str:
    cwd_name = os.path.basename(os.path.normpath(os.getcwd()))
    if not ENV_DIR_PATTERN.match(cwd_name):
        sys.stderr.write(
            f"[ERROR] CWD basename must match ^gate_fr_ or ^gate-intra-, got {cwd_name!r} "
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


def load_credentials() -> Tuple[str, str]:
    k = os.environ.get("GATE_API_KEY", "").strip()
    s = os.environ.get("GATE_API_SECRET", "").strip()
    if not k or not s:
        sys.stderr.write("[ERROR] missing GATE_API_KEY / GATE_API_SECRET.\n")
        sys.exit(2)
    return k, s


def parse_symbols(raw: Optional[str]) -> Optional[List[str]]:
    if not raw:
        return None
    out = []
    for tok in raw.split(","):
        s = tok.strip().upper()
        if s:
            out.append(s)
    return out or None


def to_pair(symbol: str) -> Optional[str]:
    if not symbol.endswith("USDT"):
        return None
    return f"{symbol[:-4]}_USDT"


def fetch_futures_open(api_key, api_secret) -> List[Dict[str, Any]]:
    """Fetch all open futures orders across all contracts."""
    out: List[Dict[str, Any]] = []
    page = 1
    while True:
        params = {"status": "open", "page": page, "limit": 100}
        status, body = gate_private("GET", "/futures/usdt/orders", api_key, api_secret, params=params)
        if not (200 <= status < 300):
            sys.stderr.write(f"[WARN] futures open status={status} body={body}\n")
            break
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            break
        if not isinstance(data, list) or not data:
            break
        out.extend(data)
        if len(data) < 100:
            break
        page += 1
    return out


def fetch_spot_open(api_key, api_secret) -> List[Dict[str, Any]]:
    """Fetch all open spot orders (account=unified)."""
    out: List[Dict[str, Any]] = []
    page = 1
    while True:
        params = {"status": "open", "page": page, "limit": 100, "account": "unified"}
        status, body = gate_private("GET", "/spot/open_orders", api_key, api_secret, params=params)
        if not (200 <= status < 300):
            sys.stderr.write(f"[WARN] spot open status={status} body={body}\n")
            break
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            break
        if not isinstance(data, list) or not data:
            break
        # /spot/open_orders returns [{currency_pair, total, orders: [...]}]
        for grp in data:
            cp = grp.get("currency_pair", "")
            for o in grp.get("orders", []) or []:
                if isinstance(o, dict):
                    o.setdefault("currency_pair", cp)
                    out.append(o)
        if len(data) < 100:
            break
        page += 1
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cancel Gate Unified open orders (futures USDT + spot)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--symbols", help="Comma-separated symbol whitelist; omit = all")
    parser.add_argument("--scope", choices=["um", "margin", "both"], default="both",
                        help="um→futures, margin→spot-unified")
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    env_name = check_env_safety()
    auto_source_env_sh()
    api_key, api_secret = load_credentials()
    wanted = parse_symbols(args.symbols)
    wanted_pairs = {to_pair(s) for s in wanted} - {None} if wanted else None

    print(f"[info] env={env_name} scope={args.scope} execute={args.execute}")

    fut_count: Dict[str, int] = {}
    spot_count: Dict[str, int] = {}

    if args.scope in ("um", "both"):
        for o in fetch_futures_open(api_key, api_secret):
            contract = str(o.get("contract", ""))
            if not contract:
                continue
            if wanted_pairs is not None and contract not in wanted_pairs:
                continue
            fut_count[contract] = fut_count.get(contract, 0) + 1

    if args.scope in ("margin", "both"):
        for o in fetch_spot_open(api_key, api_secret):
            cp = str(o.get("currency_pair", ""))
            if not cp:
                continue
            if wanted_pairs is not None and cp not in wanted_pairs:
                continue
            spot_count[cp] = spot_count.get(cp, 0) + 1

    all_keys = sorted(set(fut_count) | set(spot_count))
    if not all_keys:
        print("[plan] no open orders in scope. Nothing to do.")
        return

    print()
    print(f"{'Pair/Contract':<22} {'Futures':>8} {'Spot':>8}")
    print("-" * 42)
    for k in all_keys:
        print(f"{k:<22} {fut_count.get(k, 0):>8} {spot_count.get(k, 0):>8}")
    print("-" * 42)

    if not args.execute:
        print("\nDry-run. Pass --execute to actually cancel.")
        return

    print("\n" + "=" * 50)
    print("EXECUTING CANCELS")
    print("=" * 50)

    failures = 0
    total = 0
    for contract in fut_count:
        total += 1
        status, body = gate_private(
            "DELETE", "/futures/usdt/orders", api_key, api_secret,
            params={"contract": contract},
        )
        ok = 200 <= status < 300
        print(f"  [{'OK' if ok else 'ERR'}] futures cancel {contract} status={status}")
        if not ok:
            failures += 1
            print(f"    {body}")
    for cp in spot_count:
        total += 1
        status, body = gate_private(
            "DELETE", "/spot/orders", api_key, api_secret,
            params={"currency_pair": cp, "account": "unified"},
        )
        ok = 200 <= status < 300
        print(f"  [{'OK' if ok else 'ERR'}] spot cancel {cp} status={status}")
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
