#!/usr/bin/env python3
"""Enable Bybit UTA collateral switch for all online FR/intra assets.

The online set is built from Redis symbol-list keys:
  FR:
    dump_symbols + trade_symbols + fwd_trade_symbols + bwd_trade_symbols
    + unimmr_close_symbols
  Intra:
    intra_dump_symbols + intra_trade_symbols + intra_fwd_trade_symbols

Default is dry-run. Add --execute to submit Bybit private API requests that
change account collateral settings.

Examples:
  cd ~/bybit-intra-arb01 && ./scripts/set_bybit_online_collateral.py
  cd ~/bybit-intra-arb01 && ./scripts/set_bybit_online_collateral.py --execute
  cd ~/bybit_fr_arb01 && ./scripts/set_bybit_online_collateral.py --symbol WLFI --execute
"""

from __future__ import annotations

import argparse
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
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple


SUPPORTED_MODES = {"fr", "intra"}
SKIP_SET_COINS = {"USDT", "USDC"}
RECV_WINDOW_MS = "5000"
ENV_DIR_PATTERN = re.compile(r"^(bybit_fr_|bybit[-_]intra[-_])")
AUTHORITATIVE_KEYS = ("BYBIT_API_KEY", "BYBIT_API_SECRET")


@dataclass(frozen=True)
class EnvContext:
    env_name: str
    mode: str
    exchange: str
    open_venue: str
    hedge_venue: str


def parse_env_name(name: str) -> Tuple[Optional[str], Optional[str]]:
    text = (name or "").strip().lower()
    if re.match(r"^bybit_fr_[a-z0-9][a-z0-9_-]*$", text):
        return "fr", "bybit"
    if re.match(r"^bybit[-_]intra[-_][a-z0-9][a-z0-9_-]*$", text):
        return "intra", "bybit"
    return None, None


def check_env_safety() -> Tuple[str, str, str]:
    env_dir = str(Path.cwd())
    env_name = Path(env_dir).name.strip().lower()
    mode, exchange = parse_env_name(env_name)
    if not ENV_DIR_PATTERN.match(env_name) or mode not in SUPPORTED_MODES or exchange != "bybit":
        raise SystemExit(
            f"CWD basename must match bybit_fr_* or bybit-intra-*, got {env_name!r} "
            f"(CWD={env_dir}). Aborting for account safety."
        )
    env_path = Path(env_dir) / "env.sh"
    if not env_path.is_file():
        raise SystemExit(f"missing required env.sh in {env_dir}")
    return env_dir, env_name, mode


def resolve_context(env_name: str, mode: str) -> EnvContext:
    open_venue = os.environ.get("OPEN_VENUE", "bybit-margin").strip().lower()
    hedge_venue = os.environ.get("HEDGE_VENUE", "bybit-futures").strip().lower()
    venues = {open_venue, hedge_venue}
    if venues != {"bybit-margin", "bybit-futures"}:
        raise SystemExit(
            "this script only supports Bybit FR/intra venues: "
            f"OPEN_VENUE={open_venue!r} HEDGE_VENUE={hedge_venue!r}"
        )
    return EnvContext(
        env_name=env_name,
        mode=mode,
        exchange="bybit",
        open_venue=open_venue,
        hedge_venue=hedge_venue,
    )


def auto_source_env(env_dir: str) -> None:
    env_path = Path(env_dir) / "env.sh"
    env = dict(os.environ)
    env["ENV_FILE"] = str(env_path)
    proc = subprocess.run(
        ["bash", "-lc", "set -a; source \"$ENV_FILE\" >/dev/null 2>&1; env -0"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    if proc.returncode != 0:
        raise SystemExit(f"failed to source {env_path}: exit={proc.returncode}")
    for item in proc.stdout.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key_b, value_b = item.split(b"=", 1)
        try:
            key = key_b.decode("utf-8")
            value = value_b.decode("utf-8")
        except UnicodeDecodeError:
            continue
        old = os.environ.get(key)
        if key in AUTHORITATIVE_KEYS and old and old != value:
            print(
                f"[WARN] env.sh overrides existing {key} from process env "
                "(env.sh wins to prevent cross-account ops)",
                file=sys.stderr,
            )
        os.environ[key] = value


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
    return [
        f"{ctx.env_name}:intra_dump_symbols:{exchange_suffix}",
        f"{ctx.env_name}:intra_trade_symbols:{exchange_suffix}",
        f"{ctx.env_name}:intra_fwd_trade_symbols:{exchange_suffix}",
        f"{ctx.env_name}:intra_bwd_trade_symbols:{exchange_suffix}",
    ]


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


def load_online_assets(rds: Any, ctx: EnvContext) -> List[str]:
    keys = fr_symbol_keys(ctx) if ctx.mode == "fr" else intra_symbol_keys(ctx)
    assets: Set[str] = set()
    for key in keys:
        values = decode_redis_list(rds.get(key), key)
        print(f"[redis] {key}: {len(values)}")
        for value in values:
            asset = normalize_asset(value)
            if asset:
                assets.add(asset)
    return sorted(assets)


def parse_symbol_args(values: Iterable[str]) -> List[str]:
    out: Set[str] = set()
    for value in values:
        for part in re.split(r"[\s,]+", (value or "").strip()):
            asset = normalize_asset(part)
            if asset:
                out.add(asset)
    return sorted(out)


def load_required(names: Sequence[str]) -> List[str]:
    values = [os.environ.get(name, "").strip() for name in names]
    missing = [name for name, value in zip(names, values) if not value]
    if missing:
        raise SystemExit(f"missing env vars: {', '.join(missing)}")
    return values


def now_ms() -> int:
    return int(time.time() * 1000)


def http_request(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    body: Optional[str] = None,
    timeout: int = 10,
) -> Tuple[int, str]:
    data = None if body is None else body.encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method.upper(), headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode(), resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")
    except Exception as exc:
        return 0, str(exc)


def bybit_query(params: Dict[str, Any]) -> str:
    items = [(key, str(value)) for key, value in params.items() if value not in ("", None)]
    items.sort(key=lambda item: item[0])
    return urllib.parse.urlencode(items, safe="-_.~")


def bybit_private(
    api_key: str,
    api_secret: str,
    method: str,
    path: str,
    *,
    query: Optional[Dict[str, Any]] = None,
    body_obj: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
) -> Tuple[int, Dict[str, Any], str]:
    method = method.upper()
    query_str = bybit_query(query or {}) if method == "GET" else ""
    body = ""
    if method != "GET" and body_obj is not None:
        body = json.dumps(body_obj, separators=(",", ":"), ensure_ascii=True)
    payload = query_str if method == "GET" else body
    ts = str(now_ms())
    prehash = f"{ts}{api_key}{RECV_WINDOW_MS}{payload}"
    signature = hmac.new(api_secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).hexdigest()
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW_MS,
        "Content-Type": "application/json",
    }
    base = os.environ.get("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
    url = f"{base}{path}"
    if query_str:
        url = f"{url}?{query_str}"
    status, text = http_request(method, url, headers=headers, body=None if method == "GET" else body, timeout=timeout)
    try:
        data = json.loads(text)
    except Exception:
        data = {"retCode": None, "retMsg": "non-json response", "raw": text}
    return status, data, text


def bybit_ok(status: int, data: Dict[str, Any]) -> bool:
    return 200 <= status < 300 and str(data.get("retCode")) == "0"


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def ret_brief(data: Dict[str, Any]) -> str:
    return f"retCode={data.get('retCode')} retMsg={data.get('retMsg', '')}"


def fetch_collateral_info(api_key: str, api_secret: str, asset: str, timeout: int) -> Tuple[int, Dict[str, Any], str]:
    return bybit_private(
        api_key,
        api_secret,
        "GET",
        "/v5/account/collateral-info",
        query={"currency": asset},
        timeout=timeout,
    )


def set_collateral_on(api_key: str, api_secret: str, asset: str, timeout: int) -> Tuple[int, Dict[str, Any], str]:
    return bybit_private(
        api_key,
        api_secret,
        "POST",
        "/v5/account/set-collateral-switch",
        body_obj={"coin": asset, "collateralSwitch": "ON"},
        timeout=timeout,
    )


def info_row(data: Dict[str, Any], asset: str) -> Optional[Dict[str, Any]]:
    result = data.get("result") if isinstance(data, dict) else None
    rows = result.get("list") if isinstance(result, dict) else None
    if not isinstance(rows, list):
        return None
    asset_upper = asset.upper()
    for row in rows:
        if isinstance(row, dict) and str(row.get("currency", "")).upper() == asset_upper:
            return row
    return rows[0] if rows and isinstance(rows[0], dict) else None


def run_updates(args: argparse.Namespace, ctx: EnvContext, assets: List[str]) -> int:
    api_key, api_secret = load_required(("BYBIT_API_KEY", "BYBIT_API_SECRET"))
    base = os.environ.get("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
    failures = 0
    skipped = 0
    already_on = 0
    enabled = 0
    would_enable = 0

    print(
        f"[info] env={ctx.env_name} mode={ctx.mode} exchange={ctx.exchange} "
        f"base={base} assets={len(assets)} execute={args.execute}"
    )

    for idx, asset in enumerate(assets, start=1):
        if asset in SKIP_SET_COINS:
            skipped += 1
            print(f"[{idx}/{len(assets)}] {asset}: SKIP Bybit does not allow setting this coin")
            continue

        status, data, body = fetch_collateral_info(api_key, api_secret, asset, args.timeout)
        if not bybit_ok(status, data):
            failures += 1
            print(f"[{idx}/{len(assets)}] {asset}: ERR collateral-info status={status} {ret_brief(data)}")
            print(f"  body: {body}")
            time.sleep(args.sleep)
            continue

        row = info_row(data, asset)
        if not row:
            skipped += 1
            print(f"[{idx}/{len(assets)}] {asset}: SKIP no collateral-info row")
            time.sleep(args.sleep)
            continue

        margin_collateral = as_bool(row.get("marginCollateral"))
        collateral_switch = as_bool(row.get("collateralSwitch"))
        borrowable = row.get("borrowable")
        if not margin_collateral:
            skipped += 1
            print(
                f"[{idx}/{len(assets)}] {asset}: SKIP marginCollateral=false "
                f"collateralSwitch={collateral_switch} borrowable={borrowable}"
            )
            time.sleep(args.sleep)
            continue
        if collateral_switch:
            already_on += 1
            print(f"[{idx}/{len(assets)}] {asset}: OK already ON borrowable={borrowable}")
            time.sleep(args.sleep)
            continue

        if not args.execute:
            would_enable += 1
            print(f"[{idx}/{len(assets)}] {asset}: DRY would set collateralSwitch=ON borrowable={borrowable}")
            time.sleep(args.sleep)
            continue

        set_status, set_data, set_body = set_collateral_on(api_key, api_secret, asset, args.timeout)
        if bybit_ok(set_status, set_data):
            enabled += 1
            print(f"[{idx}/{len(assets)}] {asset}: SET ON")
        else:
            failures += 1
            print(f"[{idx}/{len(assets)}] {asset}: ERR set-collateral status={set_status} {ret_brief(set_data)}")
            print(f"  body: {set_body}")
        time.sleep(args.sleep)

    print(
        "[summary] "
        f"assets={len(assets)} enabled={enabled} dry_would_enable={would_enable} "
        f"already_on={already_on} skipped={skipped} failures={failures}"
    )
    return 1 if failures else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enable Bybit UTA collateral switch for all online FR/intra assets.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--symbol", action="append", default=[], help="Optional asset/symbol CSV filter; skips Redis loading.")
    parser.add_argument("--redis-host", default="")
    parser.add_argument("--redis-port", type=int, default=None)
    parser.add_argument("--redis-db", type=int, default=None)
    parser.add_argument("--redis-password", default=None)
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--sleep", type=float, default=0.15)
    parser.add_argument("--execute", action="store_true", help="Submit private API requests that change collateral settings.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    env_dir, env_name, mode = check_env_safety()
    auto_source_env(env_dir)
    ctx = resolve_context(env_name, mode)

    assets = parse_symbol_args(args.symbol)
    if assets:
        print(f"[info] using CLI symbols: {len(assets)}")
    else:
        rds = redis_client(args)
        assets = load_online_assets(rds, ctx)
    if not assets:
        raise SystemExit("no online assets selected")

    return run_updates(args, ctx, assets)


if __name__ == "__main__":
    raise SystemExit(main())
