#!/usr/bin/env python3
"""Batch set futures leverage for intra arb environments.

Examples:
  scripts/set_intra_futures_leverage.py --env-name gate-intra-arb01 --leverage 5
  scripts/set_intra_futures_leverage.py --env-name gate-intra-arb01 --all-trade-symbols --leverage 5 --execute
  scripts/set_intra_futures_leverage.py --env-name bitget-intra-arb01 --leverage 5 --execute
  scripts/set_intra_futures_leverage.py --env-name bybit-intra-arb01 --symbol DOGE --leverage 5 --execute
  scripts/set_intra_futures_leverage.py --env-name okex-intra-arb01 --leverage 5 --execute

Default is dry-run. Add --execute to call private APIs.
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
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Set, Tuple
from urllib.parse import urlencode
import urllib.request

import requests

DASHBOARD_HOST = os.environ.get("DASHBOARD_HOST", "127.0.0.1")
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "4191"))
SUPPORTED_EXCHANGES = {"gate", "bitget", "bybit", "okex"}
NAMESPACE = "intra"


def dec(value: Any, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return Decimal(default)


def normalize_exchange(value: str) -> str:
    text = (value or "").strip().lower()
    return "okex" if text == "okx" else text


def parse_env_name(value: str) -> Tuple[str, str]:
    match = re.match(r"^([a-z0-9]+)-intra-([a-z0-9][a-z0-9_-]*)$", value.strip().lower())
    if not match:
        raise SystemExit(f"env-name must match <exchange>-intra-<suffix>: {value}")
    return normalize_exchange(match.group(1)), match.group(2)


def load_env_file(path: str) -> None:
    if not os.path.isfile(path):
        raise SystemExit(f"env file not found: {path}")
    env = dict(os.environ)
    env["ENV_FILE"] = path
    try:
        out = subprocess.check_output(["bash", "-lc", 'set -a; source "$ENV_FILE"; env -0'], env=env)
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


def normalize_asset(value: str) -> str:
    text = (value or "").strip().upper()
    if text.endswith("-USDT-SWAP"):
        text = text[: -len("-USDT-SWAP")]
    if text.endswith("_USDT"):
        text = text[: -len("_USDT")]
    cleaned = "".join(ch for ch in text if ch.isalnum())
    if cleaned.endswith("USDT") and len(cleaned) > 4:
        return cleaned[:-4]
    return cleaned


def exchange_symbol(exchange: str, asset_or_symbol: str) -> str:
    asset = normalize_asset(asset_or_symbol)
    if not asset:
        return ""
    if exchange == "gate":
        return f"{asset}_USDT"
    if exchange == "okex":
        return f"{asset}-USDT-SWAP"
    return f"{asset}USDT"


def parse_symbol_args(exchange: str, values: Iterable[str]) -> List[str]:
    symbols: Set[str] = set()
    for value in values:
        for part in re.split(r"[\s,]+", value.strip()):
            symbol = exchange_symbol(exchange, part)
            if symbol:
                symbols.add(symbol)
    return sorted(symbols)


def fetch_dashboard_symbols(exchange: str, suffix: str, min_gross_usdt: Decimal) -> List[str]:
    url = f"http://{DASHBOARD_HOST}:{DASHBOARD_PORT}/intra/{exchange}-intra-{suffix}/snapshot"
    try:
        with urllib.request.urlopen(urllib.request.Request(url), timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        raise SystemExit(f"failed to fetch snapshot from {url}: {exc}")

    symbols: Set[str] = set()
    for entry in data.get("entries", []):
        if entry.get("channel") != "pre_trade_exposure":
            continue
        for row in entry.get("entry", {}).get("rows", []):
            if row.get("is_total"):
                continue
            asset = normalize_asset(str(row.get("asset") or ""))
            if not asset:
                continue
            gross = abs(dec(row.get("open_usdt"))) + abs(dec(row.get("hedge_usdt"))) + abs(dec(row.get("net_usdt")))
            if gross < min_gross_usdt:
                continue
            symbols.add(exchange_symbol(exchange, asset))
    return sorted(symbols)


def require_env(names: Sequence[str]) -> List[str]:
    values = [os.environ.get(name, "").strip() for name in names]
    missing = [name for name, value in zip(names, values) if not value]
    if missing:
        raise SystemExit(f"missing env vars: {', '.join(missing)}")
    return values


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def decode_redis_symbol_list(raw: Any, key: str) -> List[str]:
    if not raw:
        return []
    text = raw.decode("utf-8", "ignore") if isinstance(raw, (bytes, bytearray)) else str(raw)
    try:
        parsed = json.loads(text)
    except Exception as exc:
        raise RuntimeError(f"failed to parse Redis JSON list {key}: {exc}") from exc
    if not isinstance(parsed, list):
        raise RuntimeError(f"Redis key is not a JSON list: {key}")
    return [str(item).strip() for item in parsed if str(item).strip()]


def fetch_fwd_bwd_redis_symbols(
    exchange: str,
    *,
    redis_host: str,
    redis_port: int,
    redis_db: int,
    redis_password: str,
) -> List[str]:
    redis = try_import_redis()
    if redis is None:
        raise SystemExit("redis package is not installed; run pip install redis")
    rds = redis.Redis(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        password=redis_password or None,
    )
    keys = [
        f"{NAMESPACE}_fwd_trade_symbols:{exchange}",
        f"{NAMESPACE}_bwd_trade_symbols:{exchange}",
    ]
    symbols: Set[str] = set()
    for key in keys:
        values = decode_redis_symbol_list(rds.get(key), key)
        print(f"[redis] {key}: {len(values)} symbols")
        for value in values:
            symbol = exchange_symbol(exchange, value)
            if symbol:
                symbols.add(symbol)
    return sorted(symbols)


def bitget_sign(secret: str, timestamp_ms: str, method: str, path: str, body: str) -> str:
    payload = f"{timestamp_ms}{method.upper()}{path}{body}"
    raw = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(raw).decode("utf-8")


def bitget_post(path: str, payload: Dict[str, Any], timeout: int) -> Tuple[int, Any]:
    api_key, api_secret, passphrase = require_env(("BITGET_API_KEY", "BITGET_API_SECRET", "BITGET_API_PASSPHRASE"))
    base = os.environ.get("BITGET_API_BASE", "https://api.bitget.com").rstrip("/")
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
    timestamp_ms = str(int(time.time() * 1000))
    headers = {
        "ACCESS-KEY": api_key,
        "ACCESS-SIGN": bitget_sign(api_secret, timestamp_ms, "POST", path, body),
        "ACCESS-TIMESTAMP": timestamp_ms,
        "ACCESS-PASSPHRASE": passphrase,
        "locale": "zh-CN",
        "Content-Type": "application/json",
    }
    resp = requests.post(f"{base}{path}", headers=headers, data=body, timeout=timeout)
    return parse_response(resp)


def bybit_sign(api_key: str, api_secret: str, timestamp_ms: str, recv_window: str, payload: str) -> str:
    raw = f"{timestamp_ms}{api_key}{recv_window}{payload}"
    return hmac.new(api_secret.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()


def bybit_post(path: str, payload: Dict[str, Any], timeout: int) -> Tuple[int, Any]:
    api_key, api_secret = require_env(("BYBIT_API_KEY", "BYBIT_API_SECRET"))
    base = os.environ.get("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
    timestamp_ms = str(int(time.time() * 1000))
    recv_window = "5000"
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": bybit_sign(api_key, api_secret, timestamp_ms, recv_window, body),
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": timestamp_ms,
        "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type": "application/json",
    }
    resp = requests.post(f"{base}{path}", headers=headers, data=body, timeout=timeout)
    return parse_response(resp)


def okx_timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def okx_sign(secret: str, timestamp: str, method: str, path: str, body: str) -> str:
    payload = f"{timestamp}{method.upper()}{path}{body}"
    raw = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(raw).decode("utf-8")


def okx_post(path: str, payload: Dict[str, Any], timeout: int) -> Tuple[int, Any]:
    api_key, api_secret, passphrase = require_env(("OKX_API_KEY", "OKX_API_SECRET", "OKX_PASSPHRASE"))
    base = os.environ.get("OKX_BASE_URL", "https://www.okx.com").rstrip("/")
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
    timestamp = okx_timestamp()
    headers = {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": okx_sign(api_secret, timestamp, "POST", path, body),
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
    }
    resp = requests.post(f"{base}{path}", headers=headers, data=body, timeout=timeout)
    return parse_response(resp)


def parse_response(resp: requests.Response) -> Tuple[int, Any]:
    try:
        data = resp.json()
    except ValueError:
        data = {"raw": resp.text}
    return resp.status_code, data


def update_bitget(symbol: str, leverage: str, timeout: int) -> Tuple[int, Any]:
    return bitget_post(
        "/api/v3/account/set-leverage",
        {"category": "USDT-FUTURES", "symbol": symbol, "leverage": leverage},
        timeout,
    )


def update_bybit(symbol: str, leverage: str, timeout: int) -> Tuple[int, Any]:
    return bybit_post(
        "/v5/position/set-leverage",
        {"category": "linear", "symbol": symbol, "buyLeverage": leverage, "sellLeverage": leverage},
        timeout,
    )


def update_okex(symbol: str, leverage: str, mgn_mode: str, timeout: int) -> Tuple[int, Any]:
    return okx_post(
        "/api/v5/account/set-leverage",
        {"instId": symbol, "lever": leverage, "mgnMode": mgn_mode},
        timeout,
    )


def is_ok(exchange: str, status: int, body: Any) -> bool:
    if not (200 <= status < 300):
        return False
    if not isinstance(body, dict):
        return False
    if exchange == "bybit":
        return body.get("retCode") in (0, "0", 110043, "110043")
    if exchange == "bitget":
        return body.get("code") in ("00000", "0", None)
    if exchange == "okex":
        return str(body.get("code", "")).strip() == "0"
    return False


def run_gate_child(
    symbols: List[str],
    leverage: str,
    execute: bool,
    timeout: int,
    *,
    set_missing_position: bool,
) -> int:
    script = Path(__file__).resolve().with_name("gate_set_futures_leverage.py")
    cmd = [
        sys.executable,
        str(script),
        "--contracts",
        ",".join(symbols),
        "--leverage",
        leverage,
        "--timeout",
        str(timeout),
    ]
    if set_missing_position:
        cmd.append("--set-missing-position")
    if execute:
        cmd.append("--execute")
    print(f"[gate] delegated: {' '.join(cmd)}")
    return subprocess.call(cmd)


def run_updates(
    exchange: str,
    symbols: List[str],
    leverage: str,
    *,
    execute: bool,
    timeout: int,
    sleep: float,
    okx_mgn_mode: str,
    gate_set_missing_position: bool,
) -> int:
    if exchange == "gate":
        return run_gate_child(
            symbols,
            leverage,
            execute,
            timeout,
            set_missing_position=gate_set_missing_position,
        )

    failures = 0
    for idx, symbol in enumerate(symbols, start=1):
        print(f"[{exchange} {idx}/{len(symbols)}] {symbol} -> {leverage} execute={execute}")
        if not execute:
            continue
        if exchange == "bitget":
            status, body = update_bitget(symbol, leverage, timeout)
        elif exchange == "bybit":
            status, body = update_bybit(symbol, leverage, timeout)
        elif exchange == "okex":
            status, body = update_okex(symbol, leverage, okx_mgn_mode, timeout)
        else:
            raise AssertionError(exchange)
        ok = is_ok(exchange, status, body)
        print(f"  [{'OK' if ok else 'ERR'}] status={status} {json.dumps(body, ensure_ascii=True, sort_keys=True)}")
        if not ok:
            failures += 1
        time.sleep(sleep)
    return failures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Set futures leverage for symbols in an intra arb environment.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--env-name", default="", help="Environment name, e.g. gate-intra-arb01.")
    parser.add_argument("--env-dir", default="", help="Environment directory containing env.sh.")
    parser.add_argument("--exchange", choices=sorted(SUPPORTED_EXCHANGES), default="", help="Exchange if env is omitted.")
    parser.add_argument("--suffix", default="", help="Intra suffix if env is omitted, e.g. arb01.")
    parser.add_argument("--symbol", action="append", default=[], help="Asset/symbol filter; repeatable or CSV.")
    parser.add_argument("--leverage", required=True, help="Target leverage, e.g. 5.")
    parser.add_argument(
        "--source",
        choices=["dashboard", "fwd-bwd", "both"],
        default="dashboard",
        help="Where to load symbols from when --symbol is omitted.",
    )
    parser.add_argument(
        "--all-trade-symbols",
        action="store_true",
        help="Alias for --source fwd-bwd; use all intra fwd+bwd trade symbols from Redis.",
    )
    parser.add_argument("--okx-mgn-mode", choices=["cross", "isolated"], default="cross")
    parser.add_argument("--min-gross-usdt", type=float, default=0.0, help="Dashboard row gross threshold.")
    parser.add_argument("--redis-host", default="", help="Redis host for --source fwd-bwd; defaults to env or 127.0.0.1.")
    parser.add_argument("--redis-port", type=int, default=None, help="Redis port for --source fwd-bwd; defaults to env or 6379.")
    parser.add_argument("--redis-db", type=int, default=None, help="Redis DB for --source fwd-bwd; defaults to env or 0.")
    parser.add_argument("--redis-password", default=None, help="Redis password for --source fwd-bwd; defaults to env.")
    parser.add_argument(
        "--gate-skip-missing-position",
        action="store_true",
        help="For Gate, skip contracts whose futures position is not found instead of attempting a leverage POST.",
    )
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--sleep", type=float, default=0.12)
    parser.add_argument("--execute", action="store_true", help="Submit private API requests.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if dec(args.leverage) <= 0:
        raise SystemExit("--leverage must be positive")

    exchange = normalize_exchange(args.exchange)
    suffix = args.suffix.strip()
    env_name = args.env_name.strip()
    env_dir = args.env_dir.strip()

    if env_name:
        exchange, suffix = parse_env_name(env_name)
    elif env_dir:
        env_name = os.path.basename(env_dir.rstrip("/"))
        exchange, suffix = parse_env_name(env_name)
    elif not exchange or not suffix:
        cwd = os.path.basename(os.getcwd())
        if re.match(r"^[a-z0-9]+-intra-[a-z0-9][a-z0-9_-]*$", cwd):
            exchange, suffix = parse_env_name(cwd)

    if exchange not in SUPPORTED_EXCHANGES:
        raise SystemExit(f"unsupported or missing exchange: {exchange or '<empty>'}")
    if not suffix:
        raise SystemExit("missing suffix; pass --env-name or --suffix")

    if env_name or env_dir:
        if not env_dir:
            env_dir = os.path.join(os.path.expanduser("~"), env_name)
        load_env_file(os.path.join(env_dir, "env.sh"))

    redis_host = args.redis_host or os.environ.get("REDIS_HOST", "127.0.0.1")
    redis_port = args.redis_port if args.redis_port is not None else int(os.environ.get("REDIS_PORT", "6379"))
    redis_db = args.redis_db if args.redis_db is not None else int(os.environ.get("REDIS_DB", "0"))
    redis_password = args.redis_password if args.redis_password is not None else os.environ.get("REDIS_PASSWORD", "")

    source = "fwd-bwd" if args.all_trade_symbols else args.source
    symbols = parse_symbol_args(exchange, args.symbol)
    if not symbols:
        loaded: Set[str] = set()
        if source in ("dashboard", "both"):
            loaded.update(fetch_dashboard_symbols(exchange, suffix, dec(args.min_gross_usdt)))
        if source in ("fwd-bwd", "both"):
            loaded.update(
                fetch_fwd_bwd_redis_symbols(
                    exchange,
                    redis_host=redis_host,
                    redis_port=redis_port,
                    redis_db=redis_db,
                    redis_password=redis_password,
                )
            )
        symbols = sorted(loaded)
    if not symbols:
        raise SystemExit("no symbols selected")

    print(f"[info] exchange={exchange} suffix={suffix} leverage={args.leverage} symbols={len(symbols)} execute={args.execute}")
    failures = run_updates(
        exchange,
        symbols,
        str(args.leverage),
        execute=args.execute,
        timeout=args.timeout,
        sleep=args.sleep,
        okx_mgn_mode=args.okx_mgn_mode,
        gate_set_missing_position=not args.gate_skip_missing_position,
    )
    if failures:
        print(f"WARN: {failures} leverage updates failed", file=sys.stderr)
        return 1
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
