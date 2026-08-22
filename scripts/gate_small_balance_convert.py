#!/usr/bin/env python3
"""Convert Gate small balances to GT.

Default mode is dry-run: list every balance Gate currently considers
convertible. Add --execute to submit one all-currencies conversion request.

Examples:
  python3 scripts/gate_small_balance_convert.py --env-name gate-intra-arb01
  python3 scripts/gate_small_balance_convert.py --env-name gate-intra-arb01 --execute
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
import urllib.request
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_BASE_URL = "https://api.gateio.ws"
API_PREFIX = "/api/v4"
AUTHORITATIVE_KEYS = ("GATE_API_KEY", "GATE_API_SECRET", "GATE_API_BASE")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert every Gate-listed small balance to GT",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--env-name", help="Source $HOME/<env-name>/env.sh")
    parser.add_argument("--env-dir", help="Source <env-dir>/env.sh")
    parser.add_argument("--no-env-sh", action="store_true", help="Do not source env.sh")
    parser.add_argument("--base-url", help="Override Gate REST base URL")
    parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds")
    parser.add_argument(
        "--verify-attempts",
        type=int,
        default=5,
        help="Post-conversion small-balance verification attempts",
    )
    parser.add_argument(
        "--verify-sleep-sec",
        type=float,
        default=1.0,
        help="Delay between verification attempts",
    )
    parser.add_argument("--print-json", action="store_true", help="Print raw API responses")
    parser.add_argument("--execute", action="store_true", help="Actually convert all listed balances")
    return parser.parse_args()


def resolve_env_file(args: argparse.Namespace) -> Optional[Path]:
    if args.no_env_sh:
        return None
    if args.env_dir:
        return Path(args.env_dir).expanduser().resolve() / "env.sh"
    if args.env_name:
        if not args.env_name.startswith(("gate-intra-", "gate_fr_")):
            raise SystemExit(f"refusing non-Gate env name: {args.env_name}")
        return Path.home() / args.env_name / "env.sh"
    cwd_env = Path.cwd() / "env.sh"
    return cwd_env if cwd_env.is_file() else None


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
        value = value_b.decode("utf-8", errors="replace")
        if key in AUTHORITATIVE_KEYS:
            old = os.environ.get(key)
            if old and old != value:
                print(f"[WARN] env.sh overrides existing {key}", file=sys.stderr)
            os.environ[key] = value
        elif key not in os.environ:
            os.environ[key] = value


def load_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("GATE_API_KEY", "").strip()
    api_secret = os.environ.get("GATE_API_SECRET", "").strip()
    missing = [
        name
        for name, value in (("GATE_API_KEY", api_key), ("GATE_API_SECRET", api_secret))
        if not value
    ]
    if missing:
        raise SystemExit(f"missing env vars: {', '.join(missing)}")
    return api_key, api_secret


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))


def gate_request(
    base_url: str,
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    *,
    body: Optional[Dict[str, Any]] = None,
    timeout: int = 15,
) -> Tuple[int, str, Any]:
    method = method.upper()
    body_text = "" if body is None else compact_json(body)
    timestamp = str(int(time.time()))
    request_path = f"{API_PREFIX}{path}"
    body_hash = hashlib.sha512(body_text.encode("utf-8")).hexdigest()
    sign_text = f"{method}\n{request_path}\n\n{body_hash}\n{timestamp}"
    signature = hmac.new(
        api_secret.encode("utf-8"), sign_text.encode("utf-8"), hashlib.sha512
    ).hexdigest()
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}{request_path}",
        data=None if body is None else body_text.encode("utf-8"),
        method=method,
    )
    req.add_header("Accept", "application/json")
    req.add_header("Content-Type", "application/json")
    req.add_header("KEY", api_key)
    req.add_header("Timestamp", timestamp)
    req.add_header("SIGN", signature)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            status = response.getcode()
            response_text = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        status = exc.code
        response_text = exc.read().decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001
        return 0, str(exc), None
    if not response_text.strip():
        return status, response_text, None
    try:
        parsed = json.loads(response_text)
    except json.JSONDecodeError:
        parsed = None
    return status, response_text, parsed


def require_success(status: int, response_text: str, label: str) -> None:
    if 200 <= status < 300:
        return
    raise SystemExit(f"{label} failed: http={status} body={response_text[:800]}")


def fetch_small_balances(
    base_url: str, api_key: str, api_secret: str, timeout: int
) -> Tuple[List[Dict[str, Any]], str]:
    status, response_text, parsed = gate_request(
        base_url,
        "GET",
        "/wallet/small_balance",
        api_key,
        api_secret,
        timeout=timeout,
    )
    require_success(status, response_text, "GET /wallet/small_balance")
    if not isinstance(parsed, list):
        raise SystemExit(f"unexpected small-balance response: {response_text[:800]}")
    return [row for row in parsed if isinstance(row, dict)], response_text


def decimal_or_zero(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def print_balances(rows: List[Dict[str, Any]], label: str) -> None:
    print(f"\n[{label}] convertible={len(rows)}")
    print(f"{'Currency':<14} {'Available':>24} {'EstBTC':>18} {'ToGT':>20}")
    print("-" * 82)
    total_btc = Decimal("0")
    total_gt = Decimal("0")
    for row in sorted(rows, key=lambda item: decimal_or_zero(item.get("estimated_as_btc")), reverse=True):
        total_btc += decimal_or_zero(row.get("estimated_as_btc"))
        total_gt += decimal_or_zero(row.get("convertible_to_gt"))
        print(
            f"{str(row.get('currency', '')):<14} "
            f"{str(row.get('available_balance', '')):>24} "
            f"{str(row.get('estimated_as_btc', '')):>18} "
            f"{str(row.get('convertible_to_gt', '')):>20}"
        )
    print("-" * 82)
    print(f"total_est_btc={total_btc} total_to_gt={total_gt}")


def main() -> int:
    args = parse_args()
    env_file = resolve_env_file(args)
    if env_file is not None:
        auto_source_env_sh(env_file)
    api_key, api_secret = load_credentials()
    base_url = (args.base_url or os.environ.get("GATE_API_BASE") or DEFAULT_BASE_URL).rstrip("/")

    rows, raw_before = fetch_small_balances(base_url, api_key, api_secret, args.timeout)
    print(
        f"[info] env_file={env_file or '<none>'} base={base_url} "
        f"execute={args.execute} target=GT"
    )
    if args.print_json:
        print(raw_before)
    print_balances(rows, "before")
    if not rows:
        print("No convertible Gate small balances.")
        return 0
    if not args.execute:
        print("\nDry-run. Pass --execute to convert every listed balance to GT.")
        return 0

    status, response_text, parsed = gate_request(
        base_url,
        "POST",
        "/wallet/small_balance",
        api_key,
        api_secret,
        body={"is_all": True},
        timeout=args.timeout,
    )
    require_success(status, response_text, "POST /wallet/small_balance")
    if args.print_json and parsed is not None:
        print(compact_json(parsed))
    print(f"\n[OK] submitted all-currencies small-balance conversion: http={status}")

    after = rows
    for attempt in range(1, max(args.verify_attempts, 1) + 1):
        if args.verify_sleep_sec > 0:
            time.sleep(args.verify_sleep_sec)
        after, raw_after = fetch_small_balances(base_url, api_key, api_secret, args.timeout)
        if args.print_json:
            print(raw_after)
        if not after:
            break
        print(f"[verify {attempt}/{args.verify_attempts}] still convertible={len(after)}")
    print_balances(after, "after")
    if after:
        print("[WARN] Gate still reports convertible balances after conversion.", file=sys.stderr)
        return 1
    print("All Gate-listed small balances were converted to GT.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
