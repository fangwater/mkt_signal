#!/usr/bin/env python3
"""Flatten Binance STANDARD intra margin + UM exposure.

This is the Binance STANDARD counterpart of flatten_binance_pm.py. It is an
intra-only wrapper around flatten_margin_and_um.py:
  - CWD basename must match ^binance-intra-.
  - Auto-sources ./env.sh.
  - Requires BINANCE_ACCOUNT_MODE=STANDARD.
  - Uses STANDARD endpoints directly: spot /api/v3/account + UM /fapi/v2/account.
  - Dry-run behavior is inherited from flatten_margin_and_um.py.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path


ENV_DIR_PATTERN = re.compile(r"^binance-intra-[a-z0-9][a-z0-9_-]*$")
AUTHORITATIVE_KEYS = ("BINANCE_API_KEY", "BINANCE_API_SECRET", "BINANCE_ACCOUNT_MODE")


def check_env_safety() -> str:
    cwd_name = os.path.basename(os.path.normpath(os.getcwd()))
    if not ENV_DIR_PATTERN.match(cwd_name):
        sys.stderr.write(
            "[ERROR] CWD basename must match ^binance-intra-, "
            f"got {cwd_name!r} (CWD={os.getcwd()}). Aborting for safety.\n"
        )
        sys.exit(2)
    return cwd_name


def auto_source_env_sh() -> None:
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
            old = os.environ.get(key)
            if old and old != new_value:
                sys.stderr.write(
                    f"[WARN] env.sh overrides existing {key} from process env "
                    f"(env.sh wins to prevent cross-account ops)\n"
                )
            os.environ[key] = new_value
        elif key not in os.environ:
            os.environ[key] = new_value


def require_standard_account() -> None:
    mode = os.environ.get("BINANCE_ACCOUNT_MODE", "").strip().upper()
    if mode != "STANDARD":
        sys.stderr.write(
            "[ERROR] Binance intra flatten requires BINANCE_ACCOUNT_MODE=STANDARD, "
            f"got {mode or '<unset>'}.\n"
        )
        sys.exit(2)
    if not os.environ.get("BINANCE_API_KEY", "").strip() or not os.environ.get(
        "BINANCE_API_SECRET", ""
    ).strip():
        sys.stderr.write("[ERROR] missing BINANCE_API_KEY / BINANCE_API_SECRET.\n")
        sys.exit(2)


def main() -> None:
    env_name = check_env_safety()
    auto_source_env_sh()
    require_standard_account()

    script_dir = Path(__file__).resolve().parent
    target = script_dir / "flatten_margin_and_um.py"
    cmd = [
        sys.executable or "python3",
        str(target),
        *sys.argv[1:],
        "--base-url",
        "https://api.binance.com",
        "--margin-base-url",
        "https://api.binance.com",
        "--margin-account-path",
        "/api/v3/account",
        "--margin-order-path",
        "/api/v3/order",
        "--margin-account-kind",
        "spot",
        "--um-base-url",
        "https://fapi.binance.com",
        "--um-account-path",
        "/fapi/v2/account",
        "--um-order-path",
        "/fapi/v1/order",
        "--no-fallback",
    ]
    print(f"[info] env={env_name} account_mode=STANDARD")
    print("[RUN] " + " ".join(cmd))
    raise SystemExit(subprocess.run(cmd, check=False).returncode)


if __name__ == "__main__":
    main()
