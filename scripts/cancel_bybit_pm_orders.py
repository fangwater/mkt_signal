#!/usr/bin/env python3
"""Cancel Bybit UTA open orders (linear perpetual + margin spot).

Self-contained: no imports from other scripts in this repo.

Behavior:
  - CWD basename must match ^bybit_fr_ or ^bybit-intra-.
  - Auto-sources ./env.sh; BYBIT_API_KEY/SECRET — env.sh always wins.
  - REST source IP from ./trade_engine.toml local_ips[0] (order NIC).
  - POST /v5/order/cancel-all per (category, symbol).
  - Query/auth failures exit non-zero; never treat them as an empty book.

Usage:
  python3 scripts/cancel_bybit_pm_orders.py
  python3 scripts/cancel_bybit_pm_orders.py --symbols BTCUSDT,ETHUSDT --execute
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import hmac
import http.client
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None  # type: ignore[assignment]


BYBIT_BASE = os.environ.get("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
RECV_WINDOW_MS = "5000"

ENV_DIR_PATTERN = re.compile(r"^(bybit_fr_|bybit[-_]intra[-_])")
AUTHORITATIVE_KEYS = ("BYBIT_API_KEY", "BYBIT_API_SECRET")
TRADE_ENGINE_CFG_NAMES = ("trade_engine.toml", "trade engine.toml")
CLOSED_ORDER_STATUSES = {
    "Cancelled",
    "Filled",
    "Rejected",
    "PartiallyFilledCanceled",
    "Deactivated",
}
SPOT_ORDER_FILTERS = (
    "Order",
    "StopOrder",
    "tpslOrder",
    "OcoOrder",
    "BidirectionalTpslOrder",
)

# Set by resolve_source_ip() before any private REST call.
_SOURCE_IP: Optional[str] = None
_SOURCE_IP_ORIGIN = "system-default"


class SourceAddressHTTPHandler(urllib.request.HTTPHandler):
    def __init__(self, local_address: str):
        super().__init__()
        self._source_address = (local_address, 0)

    def http_open(self, req):
        return self.do_open(
            lambda host, **kwargs: http.client.HTTPConnection(
                host, source_address=self._source_address, **kwargs
            ),
            req,
        )


class SourceAddressHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, local_address: str):
        super().__init__()
        self._source_address = (local_address, 0)

    def https_open(self, req):
        return self.do_open(
            lambda host, **kwargs: http.client.HTTPSConnection(
                host, source_address=self._source_address, **kwargs
            ),
            req,
        )


def normalize_local_address(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    trimmed = value.strip()
    if not trimmed or trimmed in {"0.0.0.0", "::"}:
        return None
    return trimmed


def find_trade_engine_config(base_dir: str) -> Optional[Path]:
    root = Path(base_dir)
    existing = [root / name for name in TRADE_ENGINE_CFG_NAMES if (root / name).is_file()]
    if not existing:
        return None
    if len(existing) > 1:
        joined = ", ".join(str(path) for path in existing)
        raise SystemExit(f"[ERROR] multiple trade_engine configs found: {joined}")
    return existing[0]


def load_trade_engine_local_ips(path: Path) -> List[str]:
    content = path.read_text(encoding="utf-8")
    if tomllib is not None:
        data = tomllib.loads(content)
        raw = data.get("local_ips", [])
        if raw is None:
            raw = []
        if not isinstance(raw, list):
            raise SystemExit(f"[ERROR] invalid local_ips in {path}: expected array")
        out = [str(v).strip() for v in raw]
    else:
        match = re.search(r"(?ms)^\s*local_ips\s*=\s*\[(.*?)\]", content)
        if not match:
            raise SystemExit(f"[ERROR] trade_engine config {path} must provide local_ips")
        tokens = re.findall(
            r'"(?:[^"\\]|\\.)*"|\'(?:[^\'\\]|\\.)*\'',
            match.group(1),
        )
        out = [str(ast.literal_eval(token)).strip() for token in tokens]
    if not out or any(not item for item in out):
        raise SystemExit(f"[ERROR] trade_engine config {path} must provide non-empty local_ips")
    return out


def resolve_source_ip(*, explicit: Optional[str] = None) -> Tuple[Optional[str], str]:
    manual = normalize_local_address(explicit)
    if manual is not None:
        return manual, "cli --local-address"

    cfg = find_trade_engine_config(os.getcwd())
    if cfg is None:
        raise SystemExit(
            "[ERROR] missing ./trade_engine.toml with local_ips; "
            "refusing to call Bybit without a bound order source IP"
        )
    local_ips = load_trade_engine_local_ips(cfg)
    local_address = normalize_local_address(local_ips[0])
    if local_address is None:
        raise SystemExit(
            f"[ERROR] {cfg} local_ips[0]={local_ips[0]!r} is not a bindable source IP"
        )
    return local_address, f"{cfg} local_ips[0]"


def http_request(url, *, method="GET", headers=None, data=None, timeout=15):
    req = urllib.request.Request(url, data=data, method=method.upper())
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        if _SOURCE_IP:
            opener = urllib.request.build_opener(
                SourceAddressHTTPHandler(_SOURCE_IP),
                SourceAddressHTTPSHandler(_SOURCE_IP),
            )
            with opener.open(req, timeout=timeout) as resp:
                return resp.getcode(), resp.read().decode("utf-8", errors="replace")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001
        return 0, str(exc)


def bybit_sign(api_key, api_secret, ts_ms, recv, payload):
    raw = f"{ts_ms}{api_key}{recv}{payload}"
    return hmac.new(api_secret.encode(), raw.encode(), hashlib.sha256).hexdigest()


def bybit_private(method, path, api_key, api_secret, *, query=None, body=None, timeout=15):
    method = method.upper()
    query_str = ""
    body_str = ""
    if method == "GET" and query:
        items = sorted(query.items(), key=lambda kv: kv[0])
        query_str = "&".join(f"{k}={v}" for k, v in items)
    elif method != "GET" and body is not None:
        body_str = json.dumps(body, ensure_ascii=False, separators=(",", ":"))
    payload = query_str if method == "GET" else body_str
    ts_ms = str(int(time.time() * 1000))
    sig = bybit_sign(api_key, api_secret, ts_ms, RECV_WINDOW_MS, payload)
    url = f"{BYBIT_BASE}{path}"
    if query_str:
        url = f"{url}?{query_str}"
    return http_request(
        url, method=method,
        headers={
            "X-BAPI-API-KEY": api_key,
            "X-BAPI-SIGN": sig,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": ts_ms,
            "X-BAPI-RECV-WINDOW": RECV_WINDOW_MS,
            "Content-Type": "application/json",
        },
        data=None if method == "GET" else body_str.encode("utf-8"),
        timeout=timeout,
    )


def bybit_ok(body: str) -> Tuple[bool, str]:
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return False, "non-json"
    if parsed.get("retCode") == 0:
        return True, ""
    return False, f"retCode={parsed.get('retCode')} retMsg={parsed.get('retMsg', '')}"


def check_env_safety() -> str:
    cwd_name = os.path.basename(os.path.normpath(os.getcwd()))
    if not ENV_DIR_PATTERN.match(cwd_name):
        sys.stderr.write(
            f"[ERROR] CWD basename must match ^bybit_fr_ or ^bybit-intra-, got {cwd_name!r} "
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
    k = os.environ.get("BYBIT_API_KEY", "").strip()
    s = os.environ.get("BYBIT_API_SECRET", "").strip()
    if not k or not s:
        sys.stderr.write("[ERROR] missing BYBIT_API_KEY / BYBIT_API_SECRET.\n")
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


def fetch_open(
    api_key,
    api_secret,
    category: str,
    *,
    order_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    cursor = ""
    while True:
        q: Dict[str, Any] = {"category": category, "limit": "50", "openOnly": "0"}
        # openOnly=0 returns active orders. openOnly=1 returns recent terminal records.
        if category == "linear":
            q["settleCoin"] = "USDT"
        if order_filter:
            q["orderFilter"] = order_filter
        if cursor:
            q["cursor"] = cursor
        status, body = bybit_private("GET", "/v5/order/realtime", api_key, api_secret, query=q)
        if not (200 <= status < 300):
            sys.stderr.write(
                f"[ERROR] order/realtime {category} status={status} body={body}\n"
            )
            sys.exit(1)
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            sys.stderr.write(f"[ERROR] order/realtime {category} non-json body={body}\n")
            sys.exit(1)
        if parsed.get("retCode") != 0:
            sys.stderr.write(f"[ERROR] order/realtime {category}: {body}\n")
            sys.exit(1)
        result = parsed.get("result", {})
        rows = result.get("list", []) or []
        for row in rows:
            status_text = str(row.get("orderStatus") or "")
            if status_text in CLOSED_ORDER_STATUSES:
                continue
            out.append(row)
        cursor = result.get("nextPageCursor", "") or ""
        if not cursor:
            break
    return out


def parse_spot_order_filters(raw: str) -> List[str]:
    value = raw.strip()
    if not value or value.lower() == "order":
        return ["Order"]
    if value.lower() == "all":
        return list(SPOT_ORDER_FILTERS)

    allowed = {item.lower(): item for item in SPOT_ORDER_FILTERS}
    out: List[str] = []
    for tok in value.split(","):
        key = tok.strip().lower()
        if not key:
            continue
        canonical = allowed.get(key)
        if not canonical:
            sys.stderr.write(
                f"[ERROR] unsupported spot orderFilter={tok!r}; "
                f"allowed: Order, StopOrder, tpslOrder, OcoOrder, BidirectionalTpslOrder, all\n"
            )
            sys.exit(2)
        if canonical not in out:
            out.append(canonical)
    return out or ["Order"]


def collect_open_counts(
    api_key,
    api_secret,
    scope: str,
    wanted_set: Optional[set],
    spot_filters: List[str],
) -> Tuple[Dict[str, int], Dict[Tuple[str, str], int]]:
    linear_count: Dict[str, int] = {}
    spot_count: Dict[Tuple[str, str], int] = {}

    if scope in ("um", "both"):
        for o in fetch_open(api_key, api_secret, "linear"):
            sym = str(o.get("symbol", ""))
            if not sym:
                continue
            if wanted_set is not None and sym not in wanted_set:
                continue
            linear_count[sym] = linear_count.get(sym, 0) + 1
    if scope in ("margin", "both"):
        for order_filter in spot_filters:
            for o in fetch_open(api_key, api_secret, "spot", order_filter=order_filter):
                sym = str(o.get("symbol", ""))
                if not sym:
                    continue
                if wanted_set is not None and sym not in wanted_set:
                    continue
                key = (sym, order_filter)
                spot_count[key] = spot_count.get(key, 0) + 1
    return linear_count, spot_count


def spot_totals_by_symbol(spot_count: Dict[Tuple[str, str], int]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for (sym, _order_filter), count in spot_count.items():
        out[sym] = out.get(sym, 0) + count
    return out


def print_plan(linear_count: Dict[str, int], spot_count: Dict[Tuple[str, str], int]) -> None:
    spot_total = spot_totals_by_symbol(spot_count)
    all_syms = sorted(set(linear_count) | set(spot_total))
    if not all_syms:
        return

    print()
    print(f"{'Symbol':<14} {'Linear':>8} {'Spot':>8}")
    print("-" * 32)
    for s in all_syms:
        print(f"{s:<14} {linear_count.get(s, 0):>8} {spot_total.get(s, 0):>8}")
    print("-" * 32)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cancel Bybit UTA open orders (linear + spot)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--symbols", help="Comma-separated symbol whitelist; omit = all")
    parser.add_argument("--scope", choices=["um", "margin", "both"], default="both",
                        help="um→linear, margin→spot")
    parser.add_argument(
        "--spot-order-filters",
        default="Order",
        help="Spot orderFilter list to query/cancel, or 'all'. Default cancels normal spot orders only.",
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--local-address",
        default=None,
        help="Explicit REST source IP; default = trade_engine.toml local_ips[0]",
    )
    parser.add_argument(
        "--verify-delay-sec",
        type=float,
        default=1.0,
        help="After --execute, wait this many seconds and re-query active orders.",
    )
    return parser.parse_args()


def main() -> None:
    global _SOURCE_IP, _SOURCE_IP_ORIGIN
    args = parse_args()
    env_name = check_env_safety()
    auto_source_env_sh()
    api_key, api_secret = load_credentials()
    _SOURCE_IP, _SOURCE_IP_ORIGIN = resolve_source_ip(explicit=args.local_address)
    wanted = parse_symbols(args.symbols)
    wanted_set = set(wanted) if wanted else None
    spot_filters = parse_spot_order_filters(args.spot_order_filters)

    print(
        f"[info] env={env_name} scope={args.scope} execute={args.execute} "
        f"spot_order_filters={','.join(spot_filters)} "
        f"source_ip={_SOURCE_IP} ({_SOURCE_IP_ORIGIN})"
    )

    linear_count, spot_count = collect_open_counts(
        api_key, api_secret, args.scope, wanted_set, spot_filters
    )
    spot_total = spot_totals_by_symbol(spot_count)

    all_syms = sorted(set(linear_count) | set(spot_total))
    if not all_syms:
        print("[plan] no open orders in scope. Nothing to do.")
        return

    print_plan(linear_count, spot_count)

    if not args.execute:
        print("\nDry-run. Pass --execute to actually cancel.")
        return

    print("\n" + "=" * 50)
    print("EXECUTING CANCELS")
    print("=" * 50)

    failures = 0
    total = 0
    for sym in all_syms:
        if linear_count.get(sym, 0) > 0:
            total += 1
            status, body = bybit_private(
                "POST", "/v5/order/cancel-all", api_key, api_secret,
                body={"category": "linear", "symbol": sym},
            )
            ok_ret, brief = bybit_ok(body)
            ok = (200 <= status < 300) and ok_ret
            print(f"  [{'OK' if ok else 'ERR'}] linear cancel-all {sym} status={status} {brief}")
            if not ok:
                failures += 1
                print(f"    {body}")
        for order_filter in spot_filters:
            if spot_count.get((sym, order_filter), 0) <= 0:
                continue
            total += 1
            status, body = bybit_private(
                "POST", "/v5/order/cancel-all", api_key, api_secret,
                body={"category": "spot", "symbol": sym, "orderFilter": order_filter},
            )
            ok_ret, brief = bybit_ok(body)
            ok = (200 <= status < 300) and ok_ret
            print(
                f"  [{'OK' if ok else 'ERR'}] spot cancel-all {sym} "
                f"orderFilter={order_filter} status={status} {brief}"
            )
            if not ok:
                failures += 1
                print(f"    {body}")

    print()
    if failures:
        print(f"WARN: {failures}/{total} cancels failed", file=sys.stderr)
        sys.exit(1)
    print(f"All {total} cancel requests succeeded.")

    if args.verify_delay_sec > 0:
        time.sleep(args.verify_delay_sec)
    verify_linear, verify_spot = collect_open_counts(
        api_key, api_secret, args.scope, wanted_set, spot_filters
    )
    if verify_linear or verify_spot:
        print("\nWARN: residual active orders after cancel-all verification:", file=sys.stderr)
        print_plan(verify_linear, verify_spot)
        sys.exit(1)
    print("Verification passed: no residual active orders in scope.")


if __name__ == "__main__":
    main()
