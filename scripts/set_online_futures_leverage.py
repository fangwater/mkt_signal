#!/usr/bin/env python3
"""Set futures leverage for all online FR/intra symbols.

The online set is built from Redis symbol-list keys:
  dump_symbols + trade_symbols + fwd_trade_symbols + bwd_trade_symbols
  + unimmr_close_symbols

Default is dry-run. Add --execute to submit private API requests.

Examples:
  ./scripts/set_online_futures_leverage.py --execute
  ./scripts/set_online_futures_leverage.py --leverage 10 --execute
  ./scripts/set_online_futures_leverage.py --env-name bitget_fr_arb02 --leverage 5 --execute
  ./scripts/set_online_futures_leverage.py --env-name binance-intra-arb01 --execute
  ./scripts/set_online_futures_leverage.py --env-name binance_fr_arb02 --execute
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
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple


SUPPORTED_EXCHANGES = {"binance", "okex", "gate", "bybit", "bitget"}
SUPPORTED_MODES = {"fr", "intra"}


@dataclass(frozen=True)
class EnvContext:
    env_name: str
    mode: str
    exchange: str
    open_venue: str
    hedge_venue: str


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


def exchange_defaults(exchange: str) -> Tuple[str, str]:
    ex = normalize_exchange(exchange)
    if ex not in SUPPORTED_EXCHANGES:
        raise SystemExit(f"unsupported exchange: {exchange}")
    return f"{ex}-margin", f"{ex}-futures"


def parse_env_name(name: str) -> Tuple[Optional[str], Optional[str]]:
    text = (name or "").strip().lower()
    for mode in ("fr", "intra"):
        match = re.match(rf"^([a-z0-9]+)[-_]{mode}[-_][a-z0-9][a-z0-9_-]*$", text)
        if match:
            return mode, normalize_exchange(match.group(1))
    return None, None


def infer_env_name(args: argparse.Namespace) -> str:
    if args.env_name:
        return args.env_name.strip().lower()
    if args.env_dir:
        return Path(args.env_dir.rstrip("/")).name.strip().lower()
    return Path.cwd().name.strip().lower()


def resolve_context(args: argparse.Namespace) -> EnvContext:
    env_name = infer_env_name(args)
    parsed_mode, parsed_exchange = parse_env_name(env_name)

    mode = (args.mode or parsed_mode or "").strip().lower()
    exchange = normalize_exchange(args.exchange or parsed_exchange or "")
    if mode not in SUPPORTED_MODES:
        raise SystemExit(
            "missing/unsupported mode; pass --mode fr|intra or run under "
            "<exchange>_fr_<suffix> / <exchange>-intra-<suffix>"
        )
    if exchange not in SUPPORTED_EXCHANGES:
        raise SystemExit(
            "missing/unsupported exchange; pass --exchange or use an env name "
            "starting with binance/okex/gate/bybit/bitget"
        )

    default_open, default_hedge = exchange_defaults(exchange)
    open_venue = (args.open_venue or default_open).strip().lower()
    hedge_venue = (args.hedge_venue or default_hedge).strip().lower()
    if not env_name:
        raise SystemExit("missing env name; pass --env-name/--env-dir or run in env dir")
    return EnvContext(
        env_name=env_name,
        mode=mode,
        exchange=exchange,
        open_venue=open_venue,
        hedge_venue=hedge_venue,
    )


def auto_source_env(env_dir: str) -> None:
    env_path = Path(env_dir) / "env.sh"
    if not env_path.is_file():
        return
    env = dict(os.environ)
    env["ENV_FILE"] = str(env_path)
    proc = subprocess.run(
        ["bash", "-lc", 'set -a; source "$ENV_FILE" >/dev/null 2>&1; env -0'],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    if proc.returncode != 0:
        print(f"[warn] failed to source {env_path}: exit={proc.returncode}", file=sys.stderr)
        return
    for item in proc.stdout.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key_b, value_b = item.split(b"=", 1)
        try:
            key = key_b.decode("utf-8")
            value = value_b.decode("utf-8")
        except UnicodeDecodeError:
            continue
        os.environ[key] = value


def maybe_load_env(args: argparse.Namespace, ctx: EnvContext) -> None:
    if args.no_env_sh:
        return
    env_dir = args.env_dir.strip() if args.env_dir else ""
    if not env_dir and Path.cwd().name.strip().lower() == ctx.env_name:
        env_dir = str(Path.cwd())
    if not env_dir:
        candidate = Path.home() / ctx.env_name
        if candidate.is_dir():
            env_dir = str(candidate)
    if env_dir:
        auto_source_env(env_dir)


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def redis_client(args: argparse.Namespace):
    redis = try_import_redis()
    if redis is None:
        raise SystemExit("redis package is not installed; run pip install redis")
    host = args.redis_host or os.environ.get("REDIS_HOST", "127.0.0.1")
    port = args.redis_port if args.redis_port is not None else int(os.environ.get("REDIS_PORT", "6379"))
    db = args.redis_db if args.redis_db is not None else int(os.environ.get("REDIS_DB", "0"))
    password = args.redis_password if args.redis_password is not None else os.environ.get("REDIS_PASSWORD", "")
    return redis.Redis(host=host, port=port, db=db, password=password or None)


def decode_redis_list(raw: Any, key: str) -> List[str]:
    if not raw:
        return []
    text = raw.decode("utf-8", "ignore") if isinstance(raw, (bytes, bytearray)) else str(raw)
    try:
        parsed = json.loads(text)
    except Exception as exc:
        print(f"[warn] failed to parse Redis list {key}: {exc}", file=sys.stderr)
        return []
    if not isinstance(parsed, list):
        print(f"[warn] Redis key is not a JSON list: {key}", file=sys.stderr)
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


def fr_symbol_keys(ctx: EnvContext) -> List[str]:
    suffix = f"{ctx.open_venue}_{ctx.hedge_venue}"
    lists = [
        "dump_symbols",
        "trade_symbols",
        "fwd_trade_symbols",
        "bwd_trade_symbols",
        "unimmr_close_symbols",
    ]
    return [f"{ctx.env_name}:fr_{name}:{suffix}" for name in lists]


def intra_symbol_keys(ctx: EnvContext) -> List[str]:
    exchange_suffix = ctx.exchange
    venue_suffix = f"{ctx.open_venue}_{ctx.hedge_venue}"
    keys = [
        f"intra_dump_symbols:{exchange_suffix}",
        f"intra_trade_symbols:{exchange_suffix}",
        f"intra_fwd_trade_symbols:{exchange_suffix}",
        f"intra_bwd_trade_symbols:{exchange_suffix}",
        f"intra_unimmr_close_symbols:{exchange_suffix}",
        f"{ctx.env_name}:intra_unimmr_close_symbols:{venue_suffix}",
    ]
    return keys


def load_online_symbols(rds: Any, ctx: EnvContext) -> List[str]:
    keys = fr_symbol_keys(ctx) if ctx.mode == "fr" else intra_symbol_keys(ctx)
    symbols: Set[str] = set()
    for key in keys:
        values = decode_redis_list(rds.get(key), key)
        print(f"[redis] {key}: {len(values)}")
        for value in values:
            asset = normalize_asset(value)
            if asset:
                symbols.add(asset)
    return sorted(symbols)


def normalize_asset(value: str) -> str:
    text = (value or "").strip().upper()
    if not text:
        return ""
    if "@" in text:
        text = text.split("@", 1)[0].strip()
    if text.endswith("-USDT-SWAP"):
        text = text[: -len("-USDT-SWAP")]
    elif re.match(r"^[A-Z0-9]+-USDT-\d{6,8}$", text):
        text = text.split("-USDT-", 1)[0]
    elif text.endswith("-USDT"):
        text = text[: -len("-USDT")]
    elif text.endswith("_USDT"):
        text = text[: -len("_USDT")]
    else:
        cleaned = re.sub(r"[^A-Z0-9]+", "", text)
        if cleaned.endswith("USDT") and len(cleaned) > 4:
            text = cleaned[: -len("USDT")]
        else:
            text = cleaned
    return re.sub(r"[^A-Z0-9]+", "", text)


def symbol_for_exchange(exchange: str, asset: str) -> str:
    base = normalize_asset(asset)
    if not base:
        return ""
    if exchange == "okex":
        return f"{base}-USDT-SWAP"
    if exchange == "gate":
        return f"{base}_USDT"
    return f"{base}USDT"


def now_ms() -> int:
    return int(time.time() * 1000)


def load_required(names: Sequence[str]) -> List[str]:
    values = [os.environ.get(name, "").strip() for name in names]
    missing = [name for name, value in zip(names, values) if not value]
    if missing:
        raise SystemExit(f"missing env vars: {', '.join(missing)}")
    return values


def http_json_request(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    body: Optional[str] = None,
    timeout: int = 10,
) -> Tuple[int, str, Dict[str, str]]:
    data = None if body is None else body.encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method.upper(), headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read().decode("utf-8", "replace"), dict(resp.headers.items())
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", "replace")
        hdrs = dict(getattr(exc, "headers", {}).items()) if getattr(exc, "headers", None) else {}
        return exc.code, body_text, hdrs
    except Exception as exc:
        return 0, str(exc), {}


def binance_sign(query: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def binance_set_leverage(symbol: str, leverage: int, mode: str, timeout: int) -> Tuple[bool, int, str, str]:
    api_key, api_secret = load_required(("BINANCE_API_KEY", "BINANCE_API_SECRET"))
    if mode == "std":
        base = os.environ.get("BINANCE_FAPI_URL", "https://fapi.binance.com").rstrip("/")
        path = "/fapi/v1/leverage"
    elif mode == "pm":
        base = os.environ.get("BINANCE_PAPI_URL", "https://papi.binance.com").rstrip("/")
        path = "/papi/v1/um/leverage"
    else:
        raise AssertionError(mode)
    params = {
        "symbol": symbol,
        "leverage": str(leverage),
        "recvWindow": "5000",
        "timestamp": str(now_ms()),
    }
    query = urllib.parse.urlencode(sorted(params.items()), safe="-_.~")
    signature = binance_sign(query, api_secret)
    url = f"{base}{path}?{query}&signature={signature}"
    status, body, headers = http_json_request(
        "POST",
        url,
        headers={"X-MBX-APIKEY": api_key},
        timeout=timeout,
    )
    ok = 200 <= status < 300
    used_weight = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight") or ""
    return ok, status, body, f"base={base} used_weight={used_weight}"


def resolve_binance_mode(ctx: EnvContext, arg_mode: str) -> str:
    value = (arg_mode or "auto").strip().lower()
    if value in ("std", "pm"):
        return value
    account_mode = os.environ.get("BINANCE_ACCOUNT_MODE", "").strip().upper()
    if account_mode in ("STANDARD", "STD"):
        return "std"
    if account_mode in ("PORTFOLIO", "PORTFOLIO_MARGIN", "PM", "UNIFIED"):
        return "pm"
    raise SystemExit(
        "missing/unsupported BINANCE_ACCOUNT_MODE for Binance leverage update; "
        "set BINANCE_ACCOUNT_MODE=STANDARD for std futures or BINANCE_ACCOUNT_MODE=PM/PORTFOLIO_MARGIN for PM"
    )


def okx_timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def okx_set_leverage(symbol: str, leverage: int, mgn_mode: str, timeout: int) -> Tuple[bool, int, str, str]:
    api_key, api_secret, passphrase = load_required(("OKX_API_KEY", "OKX_API_SECRET", "OKX_PASSPHRASE"))
    base = os.environ.get("OKX_BASE_URL", "https://www.okx.com").rstrip("/")
    path = "/api/v5/account/set-leverage"
    payload = {"instId": symbol, "lever": str(leverage), "mgnMode": mgn_mode}
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
    ts = okx_timestamp()
    prehash = f"{ts}POST{path}{body}"
    signature = base64.b64encode(
        hmac.new(api_secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()
    ).decode("utf-8")
    headers = {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
    }
    status, resp_body, _ = http_json_request("POST", f"{base}{path}", headers=headers, body=body, timeout=timeout)
    ok = False
    brief = ""
    try:
        data = json.loads(resp_body)
        ok = (200 <= status < 300) and str(data.get("code", "")) == "0"
        if not ok:
            brief = f"code={data.get('code')} msg={data.get('msg')}"
    except Exception:
        brief = "non-json"
    return ok, status, resp_body, brief


def bybit_set_leverage(symbol: str, leverage: int, timeout: int) -> Tuple[bool, int, str, str]:
    api_key, api_secret = load_required(("BYBIT_API_KEY", "BYBIT_API_SECRET"))
    base = os.environ.get("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
    path = "/v5/position/set-leverage"
    body = json.dumps(
        {
            "category": "linear",
            "symbol": symbol,
            "buyLeverage": str(leverage),
            "sellLeverage": str(leverage),
        },
        separators=(",", ":"),
        ensure_ascii=True,
    )
    ts = str(now_ms())
    recv_window = "5000"
    prehash = f"{ts}{api_key}{recv_window}{body}"
    signature = hmac.new(api_secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).hexdigest()
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type": "application/json",
    }
    status, resp_body, _ = http_json_request("POST", f"{base}{path}", headers=headers, body=body, timeout=timeout)
    ok = False
    brief = ""
    try:
        data = json.loads(resp_body)
        ok = (200 <= status < 300) and data.get("retCode") in (0, "0", 110043, "110043")
        if not ok:
            brief = f"retCode={data.get('retCode')} retMsg={data.get('retMsg')}"
    except Exception:
        brief = "non-json"
    return ok, status, resp_body, brief


def bitget_sign(secret: str, ts_ms: str, method: str, request_path: str, body: str) -> str:
    payload = f"{ts_ms}{method.upper()}{request_path}{body}"
    return base64.b64encode(
        hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    ).decode("utf-8")


def bitget_set_leverage(symbol: str, leverage: int, timeout: int) -> Tuple[bool, int, str, str]:
    api_key = os.environ.get("BITGET_API_KEY", "").strip()
    api_secret = os.environ.get("BITGET_API_SECRET", "").strip()
    passphrase = (
        os.environ.get("BITGET_API_PASSPHRASE", "").strip()
        or os.environ.get("BITGET_PASSPHRASE", "").strip()
    )
    if not api_key or not api_secret or not passphrase:
        raise SystemExit("missing env vars: BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE")
    base = os.environ.get("BITGET_API_BASE", "https://api.bitget.com").rstrip("/")
    path = "/api/v3/account/set-leverage"
    body = json.dumps(
        {"category": "USDT-FUTURES", "symbol": symbol, "leverage": str(leverage)},
        separators=(",", ":"),
        ensure_ascii=True,
    )
    ts = str(now_ms())
    headers = {
        "ACCESS-KEY": api_key,
        "ACCESS-SIGN": bitget_sign(api_secret, ts, "POST", path, body),
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
        "locale": "en-US",
    }
    status, resp_body, _ = http_json_request("POST", f"{base}{path}", headers=headers, body=body, timeout=timeout)
    ok = False
    brief = ""
    try:
        data = json.loads(resp_body)
        ok = (200 <= status < 300) and str(data.get("code", "")) in ("00000", "0")
        if not ok:
            brief = f"code={data.get('code')} msg={data.get('msg')}"
    except Exception:
        brief = "non-json"
    return ok, status, resp_body, brief


def gate_query(params: Dict[str, Any]) -> str:
    items = [(key, value) for key, value in params.items() if value not in ("", None)]
    items.sort(key=lambda item: item[0])
    return urllib.parse.urlencode(items, doseq=True)


def gate_signature(method: str, path: str, query: str, body: str, secret: str) -> Dict[str, str]:
    timestamp = str(int(time.time()))
    body_hash = hashlib.sha512(body.encode("utf-8")).hexdigest()
    payload = f"{method.upper()}\n{path}\n{query}\n{body_hash}\n{timestamp}"
    signature = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha512).hexdigest()
    return {"Timestamp": timestamp, "SIGN": signature}


def gate_request(method: str, path: str, params: Dict[str, Any], timeout: int) -> Tuple[int, Any]:
    api_key, api_secret = load_required(("GATE_API_KEY", "GATE_API_SECRET"))
    host = os.environ.get("GATE_API_BASE", "https://api.gateio.ws").rstrip("/")
    prefix = "/api/v4"
    body = ""
    query = gate_query(params)
    url = f"{host}{prefix}{path}"
    if query:
        url = f"{url}?{query}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "KEY": api_key,
    }
    headers.update(gate_signature(method, f"{prefix}{path}", query, body, api_secret))
    status, text, _ = http_json_request(method, url, headers=headers, body=None, timeout=timeout)
    try:
        data = json.loads(text)
    except Exception:
        data = {"raw": text}
    return status, data


def gate_position_mode(position: Any) -> str:
    if not isinstance(position, dict):
        return "unknown"
    leverage = str(position.get("leverage", "")).strip()
    if leverage == "0":
        return "cross"
    if leverage:
        return "isolated"
    return "unknown"


def gate_leverage_params(position: Any, leverage: int, force_isolated: bool) -> Dict[str, str]:
    target = str(leverage)
    if force_isolated:
        return {"leverage": target}
    if gate_position_mode(position) == "cross":
        return {"leverage": "0", "cross_leverage_limit": target}
    return {"leverage": target}


def gate_missing_position(status: int, body: Any) -> bool:
    return status >= 300 and isinstance(body, dict) and str(body.get("label", "")).upper() == "POSITION_NOT_FOUND"


def gate_set_leverage(
    contract: str,
    leverage: int,
    timeout: int,
    *,
    skip_missing_position: bool,
    force_isolated: bool,
) -> Tuple[bool, int, str, str]:
    settle = os.environ.get("GATE_FUTURES_SETTLE", "usdt").lower()
    path = f"/futures/{settle}/positions/{contract}"
    status, before = gate_request("GET", path, {}, timeout)
    if status >= 300 and not gate_missing_position(status, before):
        return False, status, json.dumps(before, ensure_ascii=True), "GET position failed"
    if gate_missing_position(status, before) and skip_missing_position:
        return True, status, json.dumps(before, ensure_ascii=True), "skip missing position"
    position = None if status >= 300 else before
    params = gate_leverage_params(position, leverage, force_isolated)
    status, after = gate_request("POST", f"{path}/leverage", params, timeout)
    ok = status < 300
    return ok, status, json.dumps(after, ensure_ascii=True), f"settle={settle}"


def parse_symbol_args(values: Iterable[str]) -> List[str]:
    out: Set[str] = set()
    for value in values:
        for part in re.split(r"[\s,]+", (value or "").strip()):
            asset = normalize_asset(part)
            if asset:
                out.add(asset)
    return sorted(out)


def run_updates(args: argparse.Namespace, ctx: EnvContext, assets: List[str]) -> int:
    leverage = int(args.leverage)
    binance_mode = resolve_binance_mode(ctx, args.binance_mode) if ctx.exchange == "binance" else ""
    failures = 0
    print(
        f"[info] env={ctx.env_name} mode={ctx.mode} exchange={ctx.exchange} "
        f"leverage={leverage} assets={len(assets)} execute={args.execute}"
    )
    if binance_mode:
        print(f"[info] binance_mode={binance_mode}")

    for idx, asset in enumerate(assets, start=1):
        symbol = symbol_for_exchange(ctx.exchange, asset)
        print(f"[{idx}/{len(assets)}] {symbol} -> {leverage} execute={args.execute}")
        if not args.execute:
            continue
        if ctx.exchange == "binance":
            ok, status, body, brief = binance_set_leverage(symbol, leverage, binance_mode, args.timeout)
        elif ctx.exchange == "okex":
            ok, status, body, brief = okx_set_leverage(symbol, leverage, args.okx_mgn_mode, args.timeout)
        elif ctx.exchange == "bybit":
            ok, status, body, brief = bybit_set_leverage(symbol, leverage, args.timeout)
        elif ctx.exchange == "bitget":
            ok, status, body, brief = bitget_set_leverage(symbol, leverage, args.timeout)
        elif ctx.exchange == "gate":
            ok, status, body, brief = gate_set_leverage(
                symbol,
                leverage,
                args.timeout,
                skip_missing_position=args.gate_skip_missing_position,
                force_isolated=args.gate_force_isolated,
            )
        else:
            raise AssertionError(ctx.exchange)

        tag = "OK" if ok else "ERR"
        print(f"  [{tag}] status={status} {brief}")
        if not ok:
            failures += 1
            print(f"  body: {body}")
        time.sleep(args.sleep)
    return failures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Set futures leverage for all online FR/intra symbols.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--env-name", default="", help="Env name, e.g. bitget_fr_arb02 or bitget-intra-arb01.")
    parser.add_argument("--env-dir", default="", help="Env directory containing env.sh.")
    parser.add_argument("--mode", choices=sorted(SUPPORTED_MODES), default="", help="Override env mode.")
    parser.add_argument("--exchange", choices=sorted(SUPPORTED_EXCHANGES), default="", help="Override exchange.")
    parser.add_argument("--open-venue", default="", help="Override open venue for FR key suffix.")
    parser.add_argument("--hedge-venue", default="", help="Override hedge venue for FR key suffix.")
    parser.add_argument("--symbol", action="append", default=[], help="Optional asset/symbol CSV filter; skips Redis loading.")
    parser.add_argument("--leverage", type=int, choices=[5, 10], default=5, help="Target leverage. Only 5 or 10 allowed.")
    parser.add_argument(
        "--binance-mode",
        choices=["auto", "pm", "std"],
        default="auto",
        help="Binance endpoint mode. auto reads BINANCE_ACCOUNT_MODE from env.sh/environment.",
    )
    parser.add_argument("--okx-mgn-mode", choices=["cross", "isolated"], default="cross")
    parser.add_argument("--gate-skip-missing-position", action="store_true")
    parser.add_argument("--gate-force-isolated", action="store_true")
    parser.add_argument("--redis-host", default="")
    parser.add_argument("--redis-port", type=int, default=None)
    parser.add_argument("--redis-db", type=int, default=None)
    parser.add_argument("--redis-password", default=None)
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--sleep", type=float, default=0.12)
    parser.add_argument("--no-env-sh", action="store_true", help="Do not auto-source env.sh.")
    parser.add_argument("--execute", action="store_true", help="Submit private API requests.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ctx = resolve_context(args)
    maybe_load_env(args, ctx)

    assets = parse_symbol_args(args.symbol)
    if assets:
        print(f"[info] using CLI symbols: {len(assets)}")
    else:
        rds = redis_client(args)
        assets = load_online_symbols(rds, ctx)
    if not assets:
        raise SystemExit("no online symbols selected")

    failures = run_updates(args, ctx, assets)
    if failures:
        print(f"WARN: {failures} leverage updates failed", file=sys.stderr)
        return 1
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
