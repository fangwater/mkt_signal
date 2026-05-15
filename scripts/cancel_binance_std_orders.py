#!/usr/bin/env python3
"""Cancel Binance STANDARD intra open orders (spot + UM futures).

Behavior:
  - CWD basename must match ^binance-intra-.
  - Auto-sources ./env.sh; BINANCE_API_KEY/SECRET and BINANCE_ACCOUNT_MODE from
    env.sh win to prevent cross-account ops.
  - Uses standard-account APIs, not Portfolio Margin APIs.
  - Dry-run by default; --execute required for state changes.

Usage:
  python3 scripts/cancel_binance_std_orders.py
  python3 scripts/cancel_binance_std_orders.py --symbols BTCUSDT,ETHUSDT --execute
  python3 scripts/cancel_binance_std_orders.py --scope um --execute
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Optional


ENV_DIR_PATTERN = re.compile(r"^binance-intra-[a-z0-9][a-z0-9_-]*$")
AUTHORITATIVE_KEYS = ("BINANCE_API_KEY", "BINANCE_API_SECRET", "BINANCE_ACCOUNT_MODE")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cancel Binance STANDARD intra open orders (spot + UM futures)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--symbols",
        help="Comma-separated symbol whitelist, e.g. BTCUSDT,ETHUSDT. Omit = all.",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        default=[],
        help="Restrict to one symbol; repeatable.",
    )
    parser.add_argument(
        "--scope",
        choices=["um", "margin", "both"],
        default="both",
        help="um -> UM futures, margin -> standard spot leg",
    )
    parser.add_argument("--execute", action="store_true", help="Actually cancel orders")
    parser.add_argument("--recv-window", type=int, default=None)
    parser.add_argument("--timeout", type=int, default=None)
    parser.add_argument("--spot-base-url", default=None, help="Override spot REST base URL")
    parser.add_argument("--um-base-url", default=None, help="Override UM FAPI REST base URL")
    parser.add_argument("--um-ws-url", default=None, help="Override UM FAPI WebSocket API URL")
    parser.add_argument("--local-address", default=None, help="UM only: explicit source IP")
    parser.add_argument(
        "--trade-engine-config",
        default=None,
        help="UM only: explicit trade_engine.toml path for source IP resolution",
    )
    return parser.parse_args()


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
            "[ERROR] Binance intra cancel requires BINANCE_ACCOUNT_MODE=STANDARD, "
            f"got {mode or '<unset>'}.\n"
        )
        sys.exit(2)
    if not os.environ.get("BINANCE_API_KEY", "").strip() or not os.environ.get(
        "BINANCE_API_SECRET", ""
    ).strip():
        sys.stderr.write("[ERROR] missing BINANCE_API_KEY / BINANCE_API_SECRET.\n")
        sys.exit(2)


def normalize_symbols(raw_symbols: Optional[str], repeated: Iterable[str]) -> list[str]:
    values: list[str] = []
    if raw_symbols:
        values.extend(part.strip() for part in re.split(r"[,\s]+", raw_symbols) if part.strip())
    values.extend(part.strip() for part in repeated if part and part.strip())

    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        symbol = value.upper()
        if symbol and symbol not in seen:
            seen.add(symbol)
            out.append(symbol)
    return out


def append_common_args(cmd: list[str], args: argparse.Namespace, symbols: list[str]) -> None:
    for symbol in symbols:
        cmd.extend(["--symbol", symbol])
    if args.recv_window is not None:
        cmd.extend(["--recv-window", str(args.recv_window)])
    if args.timeout is not None:
        cmd.extend(["--timeout", str(args.timeout)])
    if args.execute:
        cmd.append("--execute")


def run_child(label: str, cmd: list[str]) -> int:
    print("")
    print(f"[INFO] === {label} ===")
    print("[RUN] " + " ".join(cmd))
    proc = subprocess.run(cmd, check=False)
    return proc.returncode


def main() -> None:
    args = parse_args()
    env_name = check_env_safety()
    auto_source_env_sh()
    require_standard_account()

    script_dir = Path(__file__).resolve().parent
    python_bin = sys.executable or "python3"
    symbols = normalize_symbols(args.symbols, args.symbol)
    print(f"[info] env={env_name} scope={args.scope} execute={args.execute}")

    exit_code = 0
    if args.scope in ("margin", "both"):
        spot_cmd = [python_bin, str(script_dir / "binance_cancel_all_std_spot_orders.py")]
        if args.spot_base_url:
            spot_cmd.extend(["--base-url", args.spot_base_url])
        append_common_args(spot_cmd, args, symbols)
        exit_code = max(exit_code, run_child("STANDARD spot open orders", spot_cmd))

    if args.scope in ("um", "both"):
        um_cmd = [
            python_bin,
            str(script_dir / "binance_cancel_all_std_um_ws_orders.py"),
            "--env-dir",
            os.getcwd(),
        ]
        if args.um_base_url:
            um_cmd.extend(["--base-url", args.um_base_url])
        if args.um_ws_url:
            um_cmd.extend(["--ws-url", args.um_ws_url])
        if args.local_address:
            um_cmd.extend(["--local-address", args.local_address])
        if args.trade_engine_config:
            um_cmd.extend(["--trade-engine-config", args.trade_engine_config])
        append_common_args(um_cmd, args, symbols)
        exit_code = max(exit_code, run_child("STANDARD UM futures open orders", um_cmd))

    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
