#!/usr/bin/env python3
"""Check or change Bitget UTA hold mode by sourcing an env file.

Official docs:
- Change position mode:
  https://www.bitget.com/zh-CN/api-doc/uta/account/Change-Position-Mode

Usage:
  python scripts/bitget_set_hold_mode.py --env-file /path/to/env.sh
  python scripts/bitget_set_hold_mode.py --env-file /path/to/env.sh --execute
  python scripts/bitget_set_hold_mode.py --env-file /path/to/env.sh --mode hedge_mode --execute
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
from typing import Any, Dict, Tuple

import requests


DEFAULT_HOST = "https://api.bitget.com"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check or change Bitget UTA hold mode by sourcing an env file."
    )
    parser.add_argument(
        "--env-file",
        required=True,
        help="Shell env file to source before reading BITGET_API_* variables.",
    )
    parser.add_argument(
        "--mode",
        default="one_way_mode",
        choices=["one_way_mode", "hedge_mode"],
        help="Target Bitget UTA hold mode. Default: one_way_mode",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually call the change-mode API. Default is check-only.",
    )
    return parser.parse_args()


def source_env_file(env_file: str) -> Dict[str, str]:
    cmd = (
        "set -a && "
        f"source {shlex.quote(env_file)} >/dev/null 2>&1 && "
        "env -0"
    )
    proc = subprocess.run(
        ["bash", "-lc", cmd],
        check=False,
        capture_output=True,
    )
    if proc.returncode != 0:
        stderr = proc.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"failed to source env file: {env_file} stderr={stderr}")

    env: Dict[str, str] = {}
    for item in proc.stdout.split(b"\x00"):
        if not item:
            continue
        key, sep, value = item.partition(b"=")
        if not sep:
            continue
        env[key.decode("utf-8", errors="replace")] = value.decode(
            "utf-8", errors="replace"
        )
    return env


def load_credentials_from_env(env: Dict[str, str]) -> Tuple[str, str, str, str]:
    api_key = env.get("BITGET_API_KEY", "").strip()
    api_secret = env.get("BITGET_API_SECRET", "").strip()
    passphrase = env.get("BITGET_API_PASSPHRASE", "").strip()
    host = env.get("BITGET_API_BASE", DEFAULT_HOST).strip().rstrip("/")
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
        raise RuntimeError(f"missing env vars after sourcing: {', '.join(missing)}")
    if not host:
        host = DEFAULT_HOST
    return api_key, api_secret, passphrase, host


def sign(api_secret: str, timestamp_ms: str, method: str, path_with_query: str, body: str) -> str:
    payload = f"{timestamp_ms}{method.upper()}{path_with_query}{body}"
    raw = hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(raw).decode("utf-8")


def request_json(
    host: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    method: str,
    path: str,
    *,
    payload: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True) if payload else ""
    timestamp_ms = str(int(time.time() * 1000))
    signature = sign(api_secret, timestamp_ms, method, path, body)
    headers = {
        "ACCESS-KEY": api_key,
        "ACCESS-SIGN": signature,
        "ACCESS-TIMESTAMP": timestamp_ms,
        "ACCESS-PASSPHRASE": passphrase,
        "locale": "zh-CN",
        "Content-Type": "application/json",
    }
    url = f"{host}{path}"
    resp = requests.request(method.upper(), url, headers=headers, data=body, timeout=10)
    try:
        data = resp.json()
    except ValueError as exc:
        raise RuntimeError(
            f"Bitget {method} {path} returned non-JSON: http={resp.status_code} body={resp.text}"
        ) from exc

    if resp.status_code >= 300 or data.get("code") not in ("00000", "0", None):
        raise RuntimeError(
            f"Bitget {method} {path} failed: http={resp.status_code} body={json.dumps(data, ensure_ascii=True)}"
        )
    return data


def get_settings(
    host: str, api_key: str, api_secret: str, passphrase: str
) -> Dict[str, Any]:
    return request_json(
        host,
        api_key,
        api_secret,
        passphrase,
        "GET",
        "/api/v3/account/settings",
    )


def change_hold_mode(
    host: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    mode: str,
) -> Dict[str, Any]:
    return request_json(
        host,
        api_key,
        api_secret,
        passphrase,
        "POST",
        "/api/v3/account/set-hold-mode",
        payload={"holdMode": mode},
    )


def extract_hold_mode(settings: Dict[str, Any]) -> str | None:
    data = settings.get("data")
    if isinstance(data, dict):
        value = data.get("holdMode")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def print_json(label: str, payload: Dict[str, Any]) -> None:
    print(f"{label}:")
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))


def main() -> int:
    args = parse_args()

    env = source_env_file(args.env_file)
    api_key, api_secret, passphrase, host = load_credentials_from_env(env)

    print(f"[bitget] env_file={args.env_file}")
    print(f"[bitget] host={host}")
    print(f"[bitget] target_hold_mode={args.mode}")

    before = get_settings(host, api_key, api_secret, passphrase)
    before_mode = extract_hold_mode(before)
    print_json("current_settings", before)
    print(f"[bitget] current_hold_mode={before_mode or 'unknown'}")

    if not args.execute:
        print("[bitget] check-only mode. Re-run with --execute to change hold mode.")
        return 0

    if before_mode == args.mode:
        print(f"[bitget] already in target mode: {args.mode}")
        return 0

    result = change_hold_mode(host, api_key, api_secret, passphrase, args.mode)
    print_json("change_result", result)

    after = get_settings(host, api_key, api_secret, passphrase)
    after_mode = extract_hold_mode(after)
    print_json("updated_settings", after)
    print(f"[bitget] updated_hold_mode={after_mode or 'unknown'}")

    if after_mode != args.mode:
        print(
            f"[bitget] mode verification failed: expected={args.mode} actual={after_mode}",
            file=sys.stderr,
        )
        return 2

    print(f"[bitget] hold mode switched successfully to {args.mode}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
