#!/usr/bin/env python3
"""Set OKX and Bybit intra futures leverage to a unified target and verify it.

Defaults:
  - exchanges: okex + bybit
  - source: intra fwd/bwd trade symbol lists from Redis
  - leverage: 10
  - mode: dry-run unless --execute is passed

Examples:
  python3 scripts/set_okx_bybit_intra_futures_leverage.py
  python3 scripts/set_okx_bybit_intra_futures_leverage.py --execute
  python3 scripts/set_okx_bybit_intra_futures_leverage.py --env-name okex-intra-arb01 --env-name bybit-intra-arb01 --execute
  python3 scripts/set_okx_bybit_intra_futures_leverage.py --suffix trade --suffix arb01 --home-dir /home/fanghaizhou --execute
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, List, Sequence, Tuple

from lib.exchange_signing import signed_get_bybit, signed_get_okx
import set_intra_futures_leverage as intra


SUPPORTED_EXCHANGES: Tuple[str, ...] = ("okex", "bybit")
DEFAULT_SUFFIXES: Tuple[str, ...] = ("trade", "arb01", "arb02", "arb03")
DEFAULT_HOME_CANDIDATES: Tuple[Path, ...] = (
    Path.home(),
    Path("/home/ecs-user"),
    Path("/home/fanghaizhou"),
)


@dataclass(frozen=True)
class EnvTarget:
    exchange: str
    suffix: str
    env_name: str
    env_dir: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch set OKX/Bybit intra futures leverage and verify the final state.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--env-name", action="append", default=[], help="Explicit env name, e.g. okex-intra-arb01. Repeatable.")
    parser.add_argument("--env-dir", action="append", default=[], help="Explicit env dir containing env.sh. Repeatable.")
    parser.add_argument("--home-dir", action="append", default=[], help="Home dir candidates to scan for okex/bybit intra envs. Repeatable.")
    parser.add_argument("--suffix", action="append", default=[], help="Only include these suffixes when auto-discovering envs. Repeatable.")
    parser.add_argument("--source", choices=["fwd-bwd", "dashboard", "both"], default="fwd-bwd", help="How to load target symbols when --symbol is omitted.")
    parser.add_argument("--all-trade-symbols", action="store_true", help="Alias for --source fwd-bwd.")
    parser.add_argument("--symbol", action="append", default=[], help="Optional asset/symbol filter, repeatable or CSV.")
    parser.add_argument("--leverage", default="10", help="Target leverage.")
    parser.add_argument("--okx-mgn-mode", choices=["cross", "isolated"], default="cross")
    parser.add_argument("--min-gross-usdt", type=float, default=0.0)
    parser.add_argument("--redis-host", default="")
    parser.add_argument("--redis-port", type=int, default=None)
    parser.add_argument("--redis-db", type=int, default=None)
    parser.add_argument("--redis-password", default=None)
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--sleep", type=float, default=0.15)
    parser.add_argument("--skip-verify", action="store_true", help="Skip post-change read-back verification.")
    parser.add_argument("--execute", action="store_true", help="Submit private API requests. Default is dry-run.")
    return parser.parse_args()


def parse_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, AttributeError, ValueError) as exc:
        raise SystemExit(f"invalid decimal value: {value!r}") from exc


def decimals_equal(left: Any, right: Any) -> bool:
    try:
        return parse_decimal(left) == parse_decimal(right)
    except SystemExit:
        return False


def normalize_env_name(value: str) -> Tuple[str, str, str]:
    match = re.match(r"^([a-z0-9]+)-intra-([a-z0-9][a-z0-9_-]*)$", value.strip().lower())
    if not match:
        raise SystemExit(f"env-name must match <exchange>-intra-<suffix>: {value}")
    exchange = intra.normalize_exchange(match.group(1))
    suffix = match.group(2)
    if exchange not in SUPPORTED_EXCHANGES:
        raise SystemExit(f"unsupported exchange in env-name: {value}")
    return exchange, suffix, f"{exchange}-intra-{suffix}"


def dedupe_targets(items: Iterable[EnvTarget]) -> List[EnvTarget]:
    seen = set()
    out: List[EnvTarget] = []
    for item in items:
        key = (item.exchange, item.suffix, str(item.env_dir))
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def discover_targets(args: argparse.Namespace) -> List[EnvTarget]:
    targets: List[EnvTarget] = []
    for raw in args.env_name:
        exchange, suffix, env_name = normalize_env_name(raw)
        targets.append(EnvTarget(exchange=exchange, suffix=suffix, env_name=env_name, env_dir=Path.home() / env_name))
    for raw in args.env_dir:
        env_dir = Path(raw).expanduser().resolve()
        exchange, suffix, env_name = normalize_env_name(env_dir.name)
        targets.append(EnvTarget(exchange=exchange, suffix=suffix, env_name=env_name, env_dir=env_dir))
    if targets:
        return dedupe_targets(targets)

    suffixes = [item.strip().lower() for item in args.suffix if item.strip()] or list(DEFAULT_SUFFIXES)
    home_dirs = [Path(raw).expanduser() for raw in args.home_dir if raw.strip()] or list(DEFAULT_HOME_CANDIDATES)
    for home_dir in home_dirs:
        for exchange in SUPPORTED_EXCHANGES:
            for suffix in suffixes:
                env_name = f"{exchange}-intra-{suffix}"
                env_dir = home_dir / env_name
                if (env_dir / "env.sh").is_file():
                    targets.append(EnvTarget(exchange=exchange, suffix=suffix, env_name=env_name, env_dir=env_dir))
    targets = dedupe_targets(targets)
    if targets:
        return targets
    suffix_hint = ", ".join(suffixes)
    home_hint = ", ".join(str(path) for path in home_dirs)
    raise SystemExit(f"no okex/bybit intra envs discovered. Scanned suffixes=[{suffix_hint}] under [{home_hint}]. Pass --env-name or --env-dir explicitly if the envs are elsewhere.")


def load_symbols_for_target(target: EnvTarget, args: argparse.Namespace) -> List[str]:
    source = "fwd-bwd" if args.all_trade_symbols else args.source
    symbols = intra.parse_symbol_args(target.exchange, args.symbol)
    if symbols:
        return symbols
    redis_host = args.redis_host or os.environ.get("REDIS_HOST", "127.0.0.1")
    redis_port = args.redis_port if args.redis_port is not None else int(os.environ.get("REDIS_PORT", "6379"))
    redis_db = args.redis_db if args.redis_db is not None else int(os.environ.get("REDIS_DB", "0"))
    redis_password = args.redis_password if args.redis_password is not None else os.environ.get("REDIS_PASSWORD", "")
    loaded = set()
    if source in ("dashboard", "both"):
        loaded.update(intra.fetch_dashboard_symbols(target.exchange, target.suffix, intra.dec(args.min_gross_usdt)))
    if source in ("fwd-bwd", "both"):
        loaded.update(intra.fetch_fwd_bwd_redis_symbols(target.exchange, redis_host=redis_host, redis_port=redis_port, redis_db=redis_db, redis_password=redis_password))
    return sorted(loaded)


def okx_verify_symbol(symbol: str, leverage: str, timeout: int, mgn_mode: str) -> Tuple[bool, str]:
    api_key, api_secret, passphrase = intra.require_env(("OKX_API_KEY", "OKX_API_SECRET", "OKX_PASSPHRASE"))
    base_url = os.environ.get("OKX_BASE_URL", "https://openapi.okx.com").rstrip("/")
    ok, status, body, err, _signed_path = signed_get_okx(base_url, "/api/v5/account/leverage-info", {"instId": symbol, "mgnMode": mgn_mode}, api_key, api_secret, passphrase, timeout)
    if not ok or status >= 300:
        return False, f"http={status} err={err} body={body[:300]}"
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return False, f"non-JSON body={body[:300]}"
    if str(payload.get("code", "")).strip() != "0":
        return False, f"code={payload.get('code')} msg={payload.get('msg')}"
    rows = payload.get("data") or []
    levers = sorted({str(row.get("lever", "")).strip() for row in rows if isinstance(row, dict) and str(row.get("lever", "")).strip()})
    if not levers:
        return False, f"no lever in response rows={rows!r}"
    if not all(decimals_equal(item, leverage) for item in levers):
        return False, f"returned_levers={levers}"
    return True, f"returned_levers={levers}"


def bybit_verify_symbol(symbol: str, leverage: str, timeout: int) -> Tuple[bool, str]:
    api_key, api_secret = intra.require_env(("BYBIT_API_KEY", "BYBIT_API_SECRET"))
    base_url = os.environ.get("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
    recv_window = int(os.environ.get("BYBIT_RECV_WINDOW_MS", "5000"))
    ok, status, body, err, _signed_path = signed_get_bybit(base_url, "/v5/position/list", {"category": "linear", "symbol": symbol}, api_key, api_secret, recv_window, timeout)
    if not ok or status >= 300:
        return False, f"http={status} err={err} body={body[:300]}"
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return False, f"non-JSON body={body[:300]}"
    if payload.get("retCode") not in (0, "0"):
        return False, f"retCode={payload.get('retCode')} retMsg={payload.get('retMsg')}"
    rows = (payload.get("result") or {}).get("list") or []
    levers = sorted({str(row.get("leverage", "")).strip() for row in rows if isinstance(row, dict) and str(row.get("symbol", "")).strip().upper() == symbol.upper() and str(row.get("leverage", "")).strip()})
    if not levers:
        return False, f"no leverage in response rows={rows!r}"
    if not all(decimals_equal(item, leverage) for item in levers):
        return False, f"returned_levers={levers}"
    return True, f"returned_levers={levers}"


def verify_symbol(exchange: str, symbol: str, leverage: str, timeout: int, okx_mgn_mode: str) -> Tuple[bool, str]:
    if exchange == "okex":
        return okx_verify_symbol(symbol, leverage, timeout, okx_mgn_mode)
    if exchange == "bybit":
        return bybit_verify_symbol(symbol, leverage, timeout)
    raise AssertionError(exchange)


def apply_symbol(exchange: str, symbol: str, leverage: str, timeout: int, okx_mgn_mode: str) -> Tuple[bool, int, Any]:
    if exchange == "okex":
        status, body = intra.update_okex(symbol, leverage, okx_mgn_mode, timeout)
    elif exchange == "bybit":
        status, body = intra.update_bybit(symbol, leverage, timeout)
    else:
        raise AssertionError(exchange)
    ok = intra.is_ok(exchange, status, body)
    return ok, status, body


def restore_env(snapshot: dict[str, str]) -> None:
    os.environ.clear()
    os.environ.update(snapshot)


def run_target(target: EnvTarget, args: argparse.Namespace) -> Tuple[int, int]:
    env_file = target.env_dir / "env.sh"
    if not env_file.is_file():
        raise SystemExit(f"env file not found: {env_file}")
    snapshot = dict(os.environ)
    try:
        intra.load_env_file(str(env_file))
        symbols = load_symbols_for_target(target, args)
        if not symbols:
            raise SystemExit(f"no symbols selected for {target.env_name}")
        print(f"[env] {target.env_name} exchange={target.exchange} symbols={len(symbols)} leverage={args.leverage} execute={args.execute}")
        failures = 0
        verified = 0
        for idx, symbol in enumerate(symbols, start=1):
            print(f"[set {target.env_name} {idx}/{len(symbols)}] {symbol} -> {args.leverage}")
            if args.execute:
                ok, status, body = apply_symbol(target.exchange, symbol, str(args.leverage), args.timeout, args.okx_mgn_mode)
                print(f"  [{'OK' if ok else 'ERR'}] status={status} {json.dumps(body, ensure_ascii=True, sort_keys=True)}")
                if not ok:
                    failures += 1
                    time.sleep(args.sleep)
                    continue
            else:
                print("  [DRY-RUN] skipped private API call")
            if args.skip_verify or not args.execute:
                time.sleep(args.sleep)
                continue
            ok, detail = verify_symbol(target.exchange, symbol, str(args.leverage), args.timeout, args.okx_mgn_mode)
            print(f"  [{'VERIFY-OK' if ok else 'VERIFY-ERR'}] {detail}")
            if ok:
                verified += 1
            else:
                failures += 1
            time.sleep(args.sleep)
        if args.execute and not args.skip_verify:
            print(f"[summary] {target.env_name} verified={verified}/{len(symbols)} failures={failures}")
        else:
            print(f"[summary] {target.env_name} planned={len(symbols)} failures={failures}")
        return len(symbols), failures
    finally:
        restore_env(snapshot)


def main() -> int:
    args = parse_args()
    if parse_decimal(args.leverage) <= 0:
        raise SystemExit("--leverage must be positive")
    targets = discover_targets(args)
    print(f"[info] targets={len(targets)} leverage={args.leverage} execute={args.execute}")
    for target in targets:
        print(f"  - {target.env_name} ({target.env_dir})")
    total_symbols = 0
    total_failures = 0
    for target in targets:
        symbols, failures = run_target(target, args)
        total_symbols += symbols
        total_failures += failures
    if total_failures:
        print(f"WARN: leverage update finished with failures={total_failures} across symbols={total_symbols}", file=sys.stderr)
        return 1
    print(f"Done. targets={len(targets)} symbols={total_symbols}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
