#!/usr/bin/env python3
"""Cancel OKEx Unified Account open orders (SWAP + MARGIN spot).

Self-contained: no imports from other scripts in this repo.

Behavior:
  - CWD basename must match ^okex_fr_ or ^okex-intra-.
  - Auto-sources ./env.sh; OKX_API_KEY/SECRET/PASSPHRASE — env.sh always wins.
  - Lists open orders via /api/v5/trade/orders-pending; cancels in batches of ≤20
    via /api/v5/trade/cancel-batch-orders.
  - Dry-run by default; --execute required.

Usage:
  python3 scripts/cancel_okex_pm_orders.py
  python3 scripts/cancel_okex_pm_orders.py --symbols BTCUSDT,ETHUSDT --execute
  python3 scripts/cancel_okex_pm_orders.py --scope um --execute
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


OKX_BASE = os.environ.get("OKX_BASE_URL", "https://openapi.okx.com").rstrip("/")
OKX_ORDERS_PENDING_PATH = "/api/v5/trade/orders-pending"
OKX_CANCEL_BATCH_PATH = "/api/v5/trade/cancel-batch-orders"

ENV_DIR_PATTERN = re.compile(r"^(okex_fr_|okex[-_]intra[-_])")
AUTHORITATIVE_KEYS = ("OKX_API_KEY", "OKX_API_SECRET", "OKX_PASSPHRASE")
BATCH_LIMIT = 20


def utc_timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


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


def okx_sign(timestamp, method, request_path, body, secret):
    payload = f"{timestamp}{method.upper()}{request_path}{body}"
    return base64.b64encode(
        hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest()
    ).decode()


def okx_private(method, path, api_key, api_secret, passphrase, *, params=None, body=None, timeout=15):
    method = method.upper()
    params = params or {}
    query = urllib.parse.urlencode(params) if params else ""
    request_path = f"{path}?{query}" if query else path
    body_text = "" if method == "GET" else json.dumps(body or [], ensure_ascii=False, separators=(",", ":"))
    timestamp = utc_timestamp()
    signature = okx_sign(timestamp, method, request_path, body_text, api_secret)
    headers = {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
    }
    return http_request(
        f"{OKX_BASE}{request_path}",
        method=method,
        headers=headers,
        data=None if method == "GET" else body_text.encode("utf-8"),
        timeout=timeout,
    )


def check_env_safety() -> str:
    cwd_name = os.path.basename(os.path.normpath(os.getcwd()))
    if not ENV_DIR_PATTERN.match(cwd_name):
        sys.stderr.write(
            f"[ERROR] CWD basename must match ^okex_fr_ or ^okex-intra-, got {cwd_name!r} "
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
    k = os.environ.get("OKX_API_KEY", "").strip()
    s = os.environ.get("OKX_API_SECRET", "").strip()
    p = os.environ.get("OKX_PASSPHRASE", "").strip()
    if not k or not s or not p:
        sys.stderr.write("[ERROR] missing OKX_API_KEY / OKX_API_SECRET / OKX_PASSPHRASE.\n")
        sys.exit(2)
    return k, s, p


def parse_symbols(raw: Optional[str]) -> Optional[List[str]]:
    if not raw:
        return None
    out = []
    for tok in raw.split(","):
        sym = tok.strip().upper()
        if sym:
            out.append(sym)
    return out or None


def to_inst_id(symbol: str, inst_type: str) -> Optional[str]:
    """BTCUSDT -> BTC-USDT (MARGIN) / BTC-USDT-SWAP (SWAP)."""
    if not symbol.endswith("USDT"):
        return None
    base = symbol[:-4]
    if inst_type == "SWAP":
        return f"{base}-USDT-SWAP"
    return f"{base}-USDT"


def fetch_open(api_key, api_secret, passphrase, inst_type: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    after = ""
    while True:
        params = {"instType": inst_type, "limit": "100"}
        if after:
            params["after"] = after
        status, body = okx_private("GET", OKX_ORDERS_PENDING_PATH, api_key, api_secret, passphrase, params=params)
        if not (200 <= status < 300):
            sys.stderr.write(f"[WARN] orders-pending {inst_type} status={status} body={body}\n")
            return out
        parsed = json.loads(body)
        if str(parsed.get("code", "")) != "0":
            sys.stderr.write(f"[WARN] orders-pending {inst_type}: {body}\n")
            return out
        rows = parsed.get("data", []) or []
        if not rows:
            break
        out.extend(rows)
        if len(rows) < 100:
            break
        after = str(rows[-1].get("ordId", ""))
        if not after:
            break
    return out


def cancel_batch(api_key, api_secret, passphrase, items: List[Dict[str, str]]) -> Tuple[int, str]:
    return okx_private(
        "POST", OKX_CANCEL_BATCH_PATH, api_key, api_secret, passphrase, body=items
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cancel OKEx Unified open orders (SWAP + MARGIN spot)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--symbols", help="Comma-separated symbol whitelist (BTCUSDT,...). Omit = all.")
    parser.add_argument(
        "--scope", choices=["um", "margin", "both"], default="both",
        help="um→SWAP, margin→spot MARGIN",
    )
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args()


def filter_orders(
    orders: List[Dict[str, Any]], wanted_inst_ids: Optional[set]
) -> List[Dict[str, str]]:
    """Return list of {instId, ordId}."""
    out: List[Dict[str, str]] = []
    for o in orders:
        inst = str(o.get("instId", ""))
        ord_id = str(o.get("ordId", ""))
        if not inst or not ord_id:
            continue
        if wanted_inst_ids is not None and inst not in wanted_inst_ids:
            continue
        out.append({"instId": inst, "ordId": ord_id})
    return out


def main() -> None:
    args = parse_args()
    env_name = check_env_safety()
    auto_source_env_sh()
    api_key, api_secret, passphrase = load_credentials()
    wanted_symbols = parse_symbols(args.symbols)

    print(f"[info] env={env_name} scope={args.scope} execute={args.execute}")

    swap_target_insts: Optional[set] = None
    margin_target_insts: Optional[set] = None
    if wanted_symbols:
        swap_target_insts = {to_inst_id(s, "SWAP") for s in wanted_symbols}
        swap_target_insts.discard(None)
        margin_target_insts = {to_inst_id(s, "MARGIN") for s in wanted_symbols}
        margin_target_insts.discard(None)

    swap_to_cancel: List[Dict[str, str]] = []
    margin_to_cancel: List[Dict[str, str]] = []
    if args.scope in ("um", "both"):
        swap_orders = fetch_open(api_key, api_secret, passphrase, "SWAP")
        swap_to_cancel = filter_orders(swap_orders, swap_target_insts)
    if args.scope in ("margin", "both"):
        margin_orders = fetch_open(api_key, api_secret, passphrase, "MARGIN")
        margin_to_cancel = filter_orders(margin_orders, margin_target_insts)

    all_items = swap_to_cancel + margin_to_cancel
    if not all_items:
        print("[plan] no open orders in scope. Nothing to do.")
        return

    # group + print by instId
    counts: Dict[str, Dict[str, int]] = {}
    for item in swap_to_cancel:
        counts.setdefault(item["instId"], {"swap": 0, "margin": 0})["swap"] += 1
    for item in margin_to_cancel:
        counts.setdefault(item["instId"], {"swap": 0, "margin": 0})["margin"] += 1

    print()
    print(f"{'InstId':<22} {'SWAP':>6} {'MARGIN':>8}")
    print("-" * 40)
    for inst in sorted(counts):
        c = counts[inst]
        print(f"{inst:<22} {c['swap']:>6} {c['margin']:>8}")
    print("-" * 40)
    print(f"Total orders: {len(all_items)}")

    if not args.execute:
        print("\nDry-run. Pass --execute to actually cancel.")
        return

    print("\n" + "=" * 50)
    print("EXECUTING CANCELS")
    print("=" * 50)

    failures = 0
    total = 0
    for i in range(0, len(all_items), BATCH_LIMIT):
        batch = all_items[i : i + BATCH_LIMIT]
        total += len(batch)
        status, body = cancel_batch(api_key, api_secret, passphrase, batch)
        ok = 200 <= status < 300
        tag = "OK" if ok else "ERR"
        print(f"  [{tag}] batch {i//BATCH_LIMIT + 1} (n={len(batch)}) status={status}")
        print(f"    {body}")
        if not ok:
            failures += len(batch)
            continue
        # per-order check
        try:
            parsed = json.loads(body)
            for row in parsed.get("data", []) or []:
                if str(row.get("sCode", "0")) != "0":
                    failures += 1
        except json.JSONDecodeError:
            pass

    print()
    if failures:
        print(f"WARN: {failures}/{total} cancels reported failure", file=sys.stderr)
        sys.exit(1)
    print(f"All {total} cancels submitted.")


if __name__ == "__main__":
    main()
