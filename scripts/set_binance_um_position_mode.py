#!/usr/bin/env python3
"""Safely set Binance UM accounts to one-way or hedge position mode.

The script defaults to dry-run. With --execute it refuses to change the
account-wide mode while any UM position or open UM order exists, then reads
the mode back from Binance after the update.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ApiPaths:
    base_url: str
    position_mode: str
    position_risk: str
    open_orders: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Set Binance UM account position mode with empty-account safeguards."
    )
    parser.add_argument(
        "--env-file",
        required=True,
        type=Path,
        help="Exec env.sh path; values are loaded without printing them.",
    )
    parser.add_argument(
        "--mode",
        choices=("ONE_WAY", "HEDGE"),
        default="ONE_WAY",
        help="ONE_WAY sends dualSidePosition=false; HEDGE sends true.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply the change. Omit for a read-only dry-run.",
    )
    parser.add_argument("--recv-window", type=int, default=5_000)
    parser.add_argument("--timeout", type=int, default=10)
    return parser.parse_args()


def load_exec_credentials(env_file: Path) -> tuple[str, str, str, str]:
    if not env_file.is_file():
        raise SystemExit(f"env file does not exist: {env_file}")
    command = 'set -a; source "$1"; printf "%s\\0%s\\0%s\\0%s\\0" "${BINANCE_API_KEY:-}" "${BINANCE_API_SECRET:-}" "${BINANCE_ACCOUNT_MODE:-}" "${BINANCE_FAPI_URL:-}"'
    completed = subprocess.run(
        ["bash", "-lc", command, "bash", str(env_file)],
        check=False,
        capture_output=True,
    )
    if completed.returncode != 0:
        raise SystemExit(f"failed to load Binance credentials from {env_file}")
    api_key, api_secret, account_mode, fapi_url, *_ = completed.stdout.split(b"\0")
    values = tuple(value.decode("utf-8", errors="strict").strip() for value in (api_key, api_secret, account_mode, fapi_url))
    if not values[0] or not values[1]:
        raise SystemExit("BINANCE_API_KEY and BINANCE_API_SECRET are required")
    mode = values[2].upper()
    if mode not in {"STANDARD", "UNIFIED"}:
        raise SystemExit(f"BINANCE_ACCOUNT_MODE must be STANDARD or UNIFIED, got {values[2]!r}")
    return values


def api_paths(account_mode: str, fapi_url: str) -> ApiPaths:
    if account_mode == "STANDARD":
        return ApiPaths(
            base_url=fapi_url or "https://fapi.binance.com",
            position_mode="/fapi/v1/positionSide/dual",
            position_risk="/fapi/v2/positionRisk",
            open_orders="/fapi/v1/openOrders",
        )
    return ApiPaths(
        base_url="https://papi.binance.com",
        position_mode="/papi/v1/um/positionSide/dual",
        position_risk="/papi/v1/um/positionRisk",
        open_orders="/papi/v1/um/openOrders",
    )


def signed_request(
    paths: ApiPaths,
    path: str,
    api_key: str,
    api_secret: str,
    params: dict[str, str],
    method: str,
    timeout: int,
) -> tuple[int, Any]:
    signed = dict(params)
    signed.setdefault("recvWindow", "5000")
    signed["timestamp"] = str(int(time.time() * 1_000))
    query = urllib.parse.urlencode(sorted(signed.items()), safe="-_.~")
    signature = hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    request = urllib.request.Request(
        f"{paths.base_url.rstrip('/')}{path}?{query}&signature={signature}",
        method=method,
        headers={"X-MBX-APIKEY": api_key},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.status
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as error:
        status = error.code
        body = error.read().decode("utf-8", errors="replace")
    except OSError as error:
        raise SystemExit(f"Binance request failed: {type(error).__name__}") from error
    try:
        return status, json.loads(body)
    except json.JSONDecodeError:
        return status, {"raw": body[:256]}


def require_ok(status: int, body: Any, path: str) -> Any:
    if 200 <= status < 300:
        return body
    if isinstance(body, dict):
        code = body.get("code", "?")
        message = body.get("msg", body.get("raw", ""))
        raise SystemExit(f"Binance {path} failed: HTTP {status}, code={code}, msg={message}")
    raise SystemExit(f"Binance {path} failed: HTTP {status}")


def nonzero_positions(rows: Any) -> list[str]:
    if not isinstance(rows, list):
        return ["unparseable position response"]
    symbols: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw = row.get("positionAmt", row.get("positionAmount", "0"))
        try:
            nonzero = float(raw) != 0.0
        except (TypeError, ValueError):
            nonzero = True
        if nonzero:
            symbols.append(str(row.get("symbol", row.get("asset", "?"))))
    return symbols


def main() -> None:
    args = parse_args()
    if args.recv_window <= 0 or args.timeout <= 0:
        raise SystemExit("recv-window and timeout must be positive")
    api_key, api_secret, account_mode, fapi_url = load_exec_credentials(args.env_file)
    paths = api_paths(account_mode, fapi_url)

    status, mode_body = signed_request(
        paths, paths.position_mode, api_key, api_secret, {"recvWindow": str(args.recv_window)}, "GET", args.timeout
    )
    mode_body = require_ok(status, mode_body, paths.position_mode)
    if not isinstance(mode_body, dict) or "dualSidePosition" not in mode_body:
        raise SystemExit("Binance position-mode response has no dualSidePosition")
    current_dual = bool(mode_body["dualSidePosition"])
    current_mode = "HEDGE" if current_dual else "ONE_WAY"

    status, positions = signed_request(
        paths, paths.position_risk, api_key, api_secret, {"recvWindow": str(args.recv_window)}, "GET", args.timeout
    )
    position_symbols = nonzero_positions(require_ok(status, positions, paths.position_risk))
    status, orders = signed_request(
        paths, paths.open_orders, api_key, api_secret, {"recvWindow": str(args.recv_window)}, "GET", args.timeout
    )
    orders = require_ok(status, orders, paths.open_orders)
    order_count = len(orders) if isinstance(orders, list) else -1

    print(f"account_mode={account_mode} current_mode={current_mode} target_mode={args.mode}")
    print(f"nonzero_positions={len(position_symbols)} open_orders={order_count}")
    if not args.execute or current_mode == args.mode:
        print("dry-run: no position-mode change submitted")
        return
    if position_symbols or order_count != 0:
        details = ",".join(position_symbols[:8]) if position_symbols else "none"
        raise SystemExit(
            f"refusing position-mode change with nonzero_positions={details} open_orders={order_count}"
        )

    target_dual = "true" if args.mode == "HEDGE" else "false"
    status, body = signed_request(
        paths,
        paths.position_mode,
        api_key,
        api_secret,
        {"dualSidePosition": target_dual, "recvWindow": str(args.recv_window)},
        "POST",
        args.timeout,
    )
    require_ok(status, body, paths.position_mode)
    status, verified = signed_request(
        paths, paths.position_mode, api_key, api_secret, {"recvWindow": str(args.recv_window)}, "GET", args.timeout
    )
    verified = require_ok(status, verified, paths.position_mode)
    verified_mode = "HEDGE" if bool(verified.get("dualSidePosition")) else "ONE_WAY"
    if verified_mode != args.mode:
        raise SystemExit(f"position mode verification failed: expected={args.mode} actual={verified_mode}")
    print(f"position_mode_updated={verified_mode}")


if __name__ == "__main__":
    main()
