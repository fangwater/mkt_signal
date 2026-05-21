#!/usr/bin/env python3
"""Pre-flight verifier for cross account modes (OKX + Bybit).

Verifies — and optionally fixes — the account-level settings the cross
trade engine assumes:

  OKX
    - acctLv ∈ {3, 4}              (跨币种保证金 / 组合保证金, required by tdMode=cross)
    - posMode == "net_mode"        (engine sends orders without posSide)

  Bybit
    - unifiedMarginStatus ≥ 3      (UTA upgraded)
    - spotMarginMode == "1"        (spot margin enabled for isLeverage=1 orders)

Defaults to dry-run; pass --execute to apply fixes for the three reversible
items (UTA upgrade is irreversible and never auto-executed).

Usage:
  cd $HOME/bybit-okex-cross-arb01
  ./cross_scripts/verify_cross_account_modes.py
  ./cross_scripts/verify_cross_account_modes.py --execute
  ./cross_scripts/verify_cross_account_modes.py --exchange okex

  scripts/verify_cross_account_modes.py --env-name bybit-okex-cross-arb01
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
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import urllib.error
import urllib.request

SUPPORTED_EXCHANGES = {"binance", "okex", "gate", "bybit", "bitget"}
VERIFIABLE_EXCHANGES = {"okex", "bybit"}
MIN_UNIFIED_MARGIN_STATUS = 3
OKX_ACCT_LV_OK = {"3", "4"}
OKX_POS_MODE_OK = "net_mode"
BYBIT_SPOT_MARGIN_OK = "1"
BYBIT_RECV_WINDOW_MS = "5000"
OKX_BASE_URL_DEFAULT = "https://openapi.okx.com"
BYBIT_BASE_URL_DEFAULT = "https://api.bybit.com"

ACCT_LV_NAMES = {
    "1": "简单模式",
    "2": "单币种保证金",
    "3": "多币种保证金",
    "4": "组合保证金 (PM)",
}
UNIFIED_STATUS_LABEL = {
    1: "Classic",
    3: "UTA 1.0",
    4: "UTA 1.0 Pro",
    5: "UTA 2.0",
    6: "UTA 2.0 Pro",
}


# --- env / context helpers (mirrored from set_cross_futures_leverage.py) ---

def normalize_exchange(value: str) -> str:
    text = (value or "").strip().lower()
    return "okex" if text == "okx" else text


def parse_cross_env_name(value: str) -> Tuple[str, str, str]:
    text = (value or "").strip().lower()
    m = re.match(
        r"^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross[-_]([a-z0-9][a-z0-9_-]*?)(?:[-_](?:open|hedge))?$",
        text,
    )
    if not m:
        raise SystemExit(f"env-name must match <open>-<hedge>-cross-<suffix>: {value}")
    open_ex = normalize_exchange(m.group(1))
    hedge_ex = normalize_exchange(m.group(2))
    suffix = m.group(3)
    if open_ex == hedge_ex:
        raise SystemExit(f"cross requires distinct exchanges: {value}")
    if open_ex not in SUPPORTED_EXCHANGES or hedge_ex not in SUPPORTED_EXCHANGES:
        raise SystemExit(f"unsupported cross exchanges in env name: {value}")
    return open_ex, hedge_ex, suffix


def load_env_file(path: str) -> None:
    if not os.path.isfile(path):
        print(f"[warn] env file not found: {path}", file=sys.stderr)
        return
    env = dict(os.environ)
    env["ENV_FILE"] = path
    try:
        out = subprocess.check_output(
            ["bash", "-lc", 'set -a; source "$ENV_FILE"; env -0'],
            env=env,
        )
    except subprocess.CalledProcessError as exc:
        raise SystemExit(f"failed to source env file {path}: exit={exc.returncode}") from exc
    for item in out.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key, value = item.split(b"=", 1)
        try:
            os.environ[key.decode("utf-8")] = value.decode("utf-8")
        except UnicodeDecodeError:
            continue


def load_required(names: Sequence[str]) -> List[str]:
    out: List[str] = []
    missing: List[str] = []
    for n in names:
        v = os.environ.get(n, "").strip()
        if not v:
            missing.append(n)
        else:
            out.append(v)
    if missing:
        raise SystemExit(f"ERROR: 缺少环境变量: {', '.join(missing)}")
    return out


# --- HTTP layer ---

def _http_request(
    method: str,
    url: str,
    headers: Dict[str, str],
    body: Optional[str],
    timeout: int,
) -> Tuple[int, str]:
    data = body.encode() if body else None
    req = urllib.request.Request(url, data=data, method=method.upper())
    for k, v in headers.items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def _truncate(body: str, limit: int = 256) -> str:
    if len(body) <= limit:
        return body
    return body[:limit] + f"...({len(body)} bytes)"


# --- OKX signing ---

def okx_utc_timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def okx_sign(timestamp: str, method: str, request_path: str, body: str, secret: str) -> str:
    payload = f"{timestamp}{method.upper()}{request_path}{body}"
    digest = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest()
    return base64.b64encode(digest).decode()


def okx_signed_request(
    base_url: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    method: str,
    path: str,
    body: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
) -> Tuple[int, str]:
    body_str = "" if body is None else json.dumps(body, separators=(",", ":"))
    timestamp = okx_utc_timestamp()
    signature = okx_sign(timestamp, method, path, body_str, api_secret)
    headers = {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
    }
    url = f"{base_url.rstrip('/')}{path}"
    return _http_request(method, url, headers, body_str if body_str else None, timeout)


def okx_assert_ok(http_status: int, body: str, path: str) -> Dict[str, Any]:
    if http_status >= 300:
        raise SystemExit(f"ERROR: OKX {path} http_status={http_status} body={_truncate(body)}")
    try:
        v = json.loads(body)
    except Exception:
        raise SystemExit(f"ERROR: OKX {path} non-JSON body={_truncate(body)}")
    code = str(v.get("code", "-1"))
    if code != "0":
        raise SystemExit(f"ERROR: OKX {path} code={code} msg={v.get('msg')}")
    return v


# --- Bybit signing ---

def bybit_sign(api_secret: str, api_key: str, timestamp_ms: str, payload: str) -> str:
    raw = f"{timestamp_ms}{api_key}{BYBIT_RECV_WINDOW_MS}{payload}"
    return hmac.new(api_secret.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()


def bybit_signed_request(
    base_url: str,
    api_key: str,
    api_secret: str,
    method: str,
    path: str,
    *,
    query: str = "",
    body: str = "",
    timeout: int = 10,
) -> Tuple[int, str]:
    timestamp_ms = str(int(time.time() * 1000))
    payload_for_sign = body if method.upper() != "GET" else query
    signature = bybit_sign(api_secret, api_key, timestamp_ms, payload_for_sign)
    url = f"{base_url.rstrip('/')}{path}"
    if query:
        url = f"{url}?{query}"
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": timestamp_ms,
        "X-BAPI-RECV-WINDOW": BYBIT_RECV_WINDOW_MS,
        "Content-Type": "application/json",
    }
    return _http_request(method, url, headers, body or None, timeout)


def bybit_assert_ok(http_status: int, body: str, path: str) -> Dict[str, Any]:
    try:
        v = json.loads(body) if body else {}
    except Exception:
        raise SystemExit(f"ERROR: Bybit {path} non-JSON http={http_status} body={_truncate(body)}")
    if http_status >= 300:
        raise SystemExit(
            f"ERROR: Bybit {path} http_status={http_status} "
            f"retCode={v.get('retCode')} retMsg={v.get('retMsg')!r}"
        )
    if v.get("retCode") not in (0, "0"):
        raise SystemExit(
            f"ERROR: Bybit {path} retCode={v.get('retCode')} retMsg={v.get('retMsg')!r}"
        )
    return v


# --- Check results ---

PASS = "PASS"
FAIL = "FAIL"
FIXED = "FIXED"
SKIP = "SKIP"


@dataclass
class CheckResult:
    exchange: str
    name: str
    status: str
    actual: str
    expected: str
    fix_cmd: str = ""
    detail: str = ""


# --- OKX checker ---

class OkexChecker:
    def __init__(self, base_url: str, timeout: int):
        self.base_url = base_url
        self.timeout = timeout
        api_key, api_secret, passphrase = load_required(
            ("OKX_API_KEY", "OKX_API_SECRET", "OKX_PASSPHRASE")
        )
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase

    def _get_assert(self, path: str) -> Dict[str, Any]:
        status, body = okx_signed_request(
            self.base_url, self.api_key, self.api_secret, self.passphrase,
            "GET", path, body=None, timeout=self.timeout,
        )
        return okx_assert_ok(status, body, path)

    def _post_raw(self, path: str, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        status, raw = okx_signed_request(
            self.base_url, self.api_key, self.api_secret, self.passphrase,
            "POST", path, body=body, timeout=self.timeout,
        )
        try:
            v = json.loads(raw) if raw else {}
        except Exception:
            v = {"_raw": raw}
        return status, v

    def fetch_config(self) -> Dict[str, Any]:
        v = self._get_assert("/api/v5/account/config")
        return (v.get("data") or [{}])[0]

    def precheck_empty_account(self) -> Optional[str]:
        pos_v = self._get_assert("/api/v5/account/positions")
        positions = pos_v.get("data") or []
        non_zero = [p.get("instId", "?") for p in positions
                    if str(p.get("pos") or "0") not in ("", "0")]
        if non_zero:
            return f"open positions: {', '.join(non_zero[:8])}" + (
                f" (+{len(non_zero) - 8} more)" if len(non_zero) > 8 else ""
            )
        ord_v = self._get_assert("/api/v5/trade/orders-pending")
        pending = ord_v.get("data") or []
        if pending:
            ids = [str(o.get("ordId") or o.get("clOrdId") or "?") for o in pending[:5]]
            more = "" if len(pending) <= 5 else f" (+{len(pending) - 5} more)"
            return f"pending orders: {', '.join(ids)}{more}"
        return None

    def _try_fix(self, path: str, body: Dict[str, Any]) -> Tuple[bool, str]:
        status, v = self._post_raw(path, body)
        code = str(v.get("code", "-1"))
        if status < 300 and code == "0":
            return True, ""
        return False, f"http={status} code={code} msg={v.get('msg')}"

    def check_acct_lv(self, cfg: Dict[str, Any], execute: bool) -> CheckResult:
        actual = str(cfg.get("acctLv", "?"))
        expected = "∈ {3,4}"
        if actual in OKX_ACCT_LV_OK:
            return CheckResult("okex", "acctLv", PASS, actual, expected,
                              detail=ACCT_LV_NAMES.get(actual, ""))
        result = CheckResult(
            "okex", "acctLv", FAIL, actual, expected,
            fix_cmd="./cross_scripts/verify_cross_account_modes.py --execute --exchange okex",
            detail='POST /api/v5/account/set-account-level {"acctLv":"3"}',
        )
        if execute:
            blocker = self.precheck_empty_account()
            if blocker:
                result.detail = f"cannot fix: {blocker}; cancel orders + flatten positions first"
                return result
            ok, err = self._try_fix("/api/v5/account/set-account-level", {"acctLv": "3"})
            if not ok:
                result.detail = f"set-account-level rejected: {err}"
                return result
            new_actual = str(self.fetch_config().get("acctLv", "?"))
            if new_actual in OKX_ACCT_LV_OK:
                result.status = FIXED
                result.actual = new_actual
                result.detail = f"set acctLv → {new_actual}"
            else:
                result.detail = f"set-account-level returned OK but acctLv={new_actual}"
        return result

    def check_pos_mode(self, cfg: Dict[str, Any], execute: bool) -> CheckResult:
        actual = str(cfg.get("posMode", "?"))
        expected = OKX_POS_MODE_OK
        if actual == expected:
            return CheckResult("okex", "posMode", PASS, actual, expected)
        result = CheckResult(
            "okex", "posMode", FAIL, actual, expected,
            fix_cmd="./cross_scripts/verify_cross_account_modes.py --execute --exchange okex",
            detail='POST /api/v5/account/set-position-mode {"posMode":"net_mode"}',
        )
        if execute:
            blocker = self.precheck_empty_account()
            if blocker:
                result.detail = f"cannot fix: {blocker}; cancel orders + flatten positions first"
                return result
            ok, err = self._try_fix("/api/v5/account/set-position-mode", {"posMode": "net_mode"})
            if not ok:
                result.detail = f"set-position-mode rejected: {err}"
                return result
            new_actual = str(self.fetch_config().get("posMode", "?"))
            if new_actual == expected:
                result.status = FIXED
                result.actual = new_actual
                result.detail = f"set posMode → {new_actual}"
            else:
                result.detail = f"set-position-mode returned OK but posMode={new_actual}"
        return result

    def run(self, execute: bool) -> List[CheckResult]:
        cfg = self.fetch_config()
        return [
            self.check_acct_lv(cfg, execute),
            self.check_pos_mode(cfg, execute),
        ]


# --- Bybit checker ---

class BybitChecker:
    def __init__(self, base_url: str, timeout: int):
        self.base_url = base_url
        self.timeout = timeout
        api_key, api_secret = load_required(("BYBIT_API_KEY", "BYBIT_API_SECRET"))
        self.api_key = api_key
        self.api_secret = api_secret

    def _get_assert(self, path: str, query: str = "") -> Dict[str, Any]:
        status, body = bybit_signed_request(
            self.base_url, self.api_key, self.api_secret,
            "GET", path, query=query, timeout=self.timeout,
        )
        return bybit_assert_ok(status, body, path)

    def _post_raw(self, path: str, body_obj: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        body = json.dumps(body_obj, separators=(",", ":"))
        status, raw = bybit_signed_request(
            self.base_url, self.api_key, self.api_secret,
            "POST", path, body=body, timeout=self.timeout,
        )
        try:
            v = json.loads(raw) if raw else {}
        except Exception:
            v = {"_raw": raw}
        return status, v

    def fetch_account_info(self) -> Dict[str, Any]:
        return self._get_assert("/v5/account/info").get("result") or {}

    def fetch_spot_margin_state(self) -> Dict[str, Any]:
        return self._get_assert("/v5/spot-margin-trade/state").get("result") or {}

    def check_unified_margin_status(self, info: Dict[str, Any], execute: bool) -> CheckResult:
        actual = int(info.get("unifiedMarginStatus") or 0)
        expected = f"≥ {MIN_UNIFIED_MARGIN_STATUS}"
        label = UNIFIED_STATUS_LABEL.get(actual, f"unknown({actual})")
        if actual >= MIN_UNIFIED_MARGIN_STATUS:
            return CheckResult("bybit", "unifiedMarginStatus",
                              PASS, f"{actual} ({label})", expected)
        return CheckResult(
            "bybit", "unifiedMarginStatus", FAIL,
            f"{actual} ({label})", expected,
            fix_cmd="python3 scripts/setup_bybit_spot_margin.py --yes  # from repo root",
            detail="UTA upgrade is IRREVERSIBLE; never auto-executed even with --execute",
        )

    def check_spot_margin_mode(self, state: Dict[str, Any], execute: bool) -> CheckResult:
        actual = str(state.get("spotMarginMode") or "")
        expected = BYBIT_SPOT_MARGIN_OK
        if actual == expected:
            return CheckResult("bybit", "spotMarginMode", PASS, repr(actual), repr(expected))
        result = CheckResult(
            "bybit", "spotMarginMode", FAIL, repr(actual), repr(expected),
            fix_cmd="./cross_scripts/verify_cross_account_modes.py --execute --exchange bybit",
            detail='POST /v5/spot-margin-trade/switch-mode {"spotMarginMode":"1"}',
        )
        if execute:
            status, v = self._post_raw("/v5/spot-margin-trade/switch-mode",
                                       {"spotMarginMode": "1"})
            ret_code = v.get("retCode")
            if status >= 300 or ret_code not in (0, "0"):
                result.detail = (
                    f"switch-mode rejected: http={status} retCode={ret_code} "
                    f"retMsg={v.get('retMsg')!r} "
                    "(hint: complete the Bybit web/app quiz first)"
                )
                return result
            new_actual = str(self.fetch_spot_margin_state().get("spotMarginMode") or "")
            if new_actual == expected:
                result.status = FIXED
                result.actual = repr(new_actual)
                result.detail = "spotMarginMode → 1"
            else:
                result.detail = f"switch-mode returned OK but spotMarginMode={new_actual!r}"
        return result

    def run(self, execute: bool) -> List[CheckResult]:
        info = self.fetch_account_info()
        state = self.fetch_spot_margin_state()
        return [
            self.check_unified_margin_status(info, execute),
            self.check_spot_margin_mode(state, execute),
        ]


# --- Render ---

STATUS_ICON = {PASS: "✅", FAIL: "❌", FIXED: "🔧", SKIP: "⏭"}


def render_report(env_name: str, open_ex: str, hedge_ex: str,
                  results: List[CheckResult]) -> int:
    print(f"=== verify cross account modes: {env_name} ===")
    print(f"open  = {open_ex}-futures")
    print(f"hedge = {hedge_ex}-futures")
    print("")
    by_ex: Dict[str, List[CheckResult]] = {}
    for r in results:
        by_ex.setdefault(r.exchange, []).append(r)
    for ex in sorted(by_ex.keys()):
        print(f"[{ex.upper()}]")
        for r in by_ex[ex]:
            icon = STATUS_ICON.get(r.status, "?")
            line = f"  {icon} {r.name:<22} = {r.actual}  (expected {r.expected})"
            if r.detail:
                line += f"  -- {r.detail}"
            print(line)
            if r.status == FAIL and r.fix_cmd:
                print(f"        cmd: {r.fix_cmd}")
        print("")
    pass_n = sum(1 for r in results if r.status in (PASS, FIXED))
    fail_n = sum(1 for r in results if r.status == FAIL)
    print(f"result: {pass_n} OK / {fail_n} FAIL")
    return 0 if fail_n == 0 else 1


# --- main ---

def resolve_context(args: argparse.Namespace) -> Tuple[str, str, str, str]:
    env_name = (args.env_name or "").strip().lower()
    env_dir = (args.env_dir or "").strip()
    if not env_name and env_dir:
        env_name = os.path.basename(env_dir.rstrip("/")).strip().lower()
    if not env_name:
        cwd_name = os.path.basename(os.getcwd()).strip().lower()
        if re.match(r"^[a-z0-9]+[-_][a-z0-9]+[-_]cross[-_]", cwd_name):
            env_name = cwd_name
    if not env_name:
        raise SystemExit(
            "missing env name; pass --env-name <open>-<hedge>-cross-<suffix> "
            "or run from the env dir"
        )
    open_ex, hedge_ex, _ = parse_cross_env_name(env_name)
    if not env_dir:
        env_dir = os.path.join(os.path.expanduser("~"), env_name)
    if not args.no_env_sh:
        load_env_file(os.path.join(env_dir, "env.sh"))
    return env_name, open_ex, hedge_ex, env_dir


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verify (and optionally fix) cross account modes for OKX + Bybit.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--env-name", default="",
                        help="Env name, e.g. bybit-okex-cross-arb01. Default: infer from CWD.")
    parser.add_argument("--env-dir", default="",
                        help="Env directory (default: $HOME/<env-name>).")
    parser.add_argument("--no-env-sh", action="store_true",
                        help="Skip auto-source of env.sh.")
    parser.add_argument("--execute", action="store_true",
                        help="Apply fixes for fixable FAILs "
                             "(UTA upgrade never auto-executed).")
    parser.add_argument("--exchange", choices=("okex", "bybit"), default="",
                        help="Limit checks to one exchange "
                             "(default: both sides of the cross pair).")
    parser.add_argument("--timeout", type=int, default=10,
                        help="HTTP timeout per request (seconds).")
    parser.add_argument("--okx-base-url",
                        default=os.environ.get("OKX_BASE_URL", OKX_BASE_URL_DEFAULT))
    parser.add_argument("--bybit-base-url",
                        default=os.environ.get("BYBIT_API_BASE", BYBIT_BASE_URL_DEFAULT))
    args = parser.parse_args(argv)

    env_name, open_ex, hedge_ex, _env_dir = resolve_context(args)

    pair_set = {open_ex, hedge_ex}
    if args.exchange:
        if args.exchange not in pair_set:
            raise SystemExit(
                f"--exchange={args.exchange} not in pair {{{open_ex}, {hedge_ex}}}"
            )
        target = {args.exchange}
    else:
        target = pair_set & VERIFIABLE_EXCHANGES

    if not target:
        print(
            f"[warn] no verifiable exchanges in pair "
            f"(only {sorted(VERIFIABLE_EXCHANGES)} supported); "
            f"pair = {sorted(pair_set)}"
        )
        return 0

    results: List[CheckResult] = []
    if "okex" in target:
        results.extend(OkexChecker(args.okx_base_url, args.timeout).run(args.execute))
    if "bybit" in target:
        results.extend(BybitChecker(args.bybit_base_url, args.timeout).run(args.execute))

    return render_report(env_name, open_ex, hedge_ex, results)


if __name__ == "__main__":
    sys.exit(main())
