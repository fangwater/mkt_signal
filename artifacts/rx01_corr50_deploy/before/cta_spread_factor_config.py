#!/usr/bin/env python3
"""Read, validate or atomically replace per-symbol CTA factor weights and signs."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
from pathlib import Path
from typing import Any


def factor_key(env: str) -> str:
    if not re.fullmatch(r"binance-cta-spread-[a-z0-9][a-z0-9_-]*", env):
        raise ValueError("env must be binance-cta-spread-<tag>")
    return f"{env}:cta_symbol_factors"


def reject_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def normalize_table(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict) or len(raw) > 256:
        raise ValueError("symbol_factors must be an object with at most 256 symbols")
    allowlist = Path(__file__).with_name("factor_validation_allowlist.txt").read_text(encoding="utf-8")
    allowed = set(re.findall(r"(?:factor_\d{3}|baseline_\d{3}|T[DP]_[A-Z]+_\d{3})", allowlist)) | {"net_buy_medium"}
    table, services = {}, set()
    for raw_symbol, row in raw.items():
        if not isinstance(raw_symbol, str):
            raise ValueError("symbol must be a string")
        symbol = raw_symbol.strip().upper()
        if not re.fullmatch(r"[A-Z0-9]{1,28}USDT", symbol) or symbol in table:
            raise ValueError(f"invalid or duplicate symbol: {symbol}")
        if not isinstance(row, dict) or not 1 <= len(row) <= 32:
            raise ValueError(f"{symbol}: requires 1..32 factor -> signed weight entries")
        normalized = {}
        for raw_factor, weight in row.items():
            if not isinstance(raw_factor, str):
                raise ValueError(f"{symbol}: factor must be a string")
            factor = raw_factor.strip().lower()
            if factor.startswith(("td_", "tp_")):
                factor = factor.upper()
            if factor not in allowed or factor in normalized:
                raise ValueError(f"{symbol}: invalid or duplicate factor {factor}")
            if isinstance(weight, bool) or not isinstance(weight, (int, float)):
                raise ValueError(f"{symbol}/{factor}: signed weight must be a number")
            try:
                finite = math.isfinite(weight)
            except OverflowError:
                finite = False
            if not finite:
                raise ValueError(f"{symbol}/{factor}: signed weight must be finite")
            normalized[factor] = float(weight)
            if weight != 0:
                services.add(factor)
        if not any(w != 0 for w in normalized.values()):
            raise ValueError(f"{symbol}: absolute weights must have a positive total")
        table[symbol] = dict(sorted(normalized.items()))
    if len(services) > 32:
        raise ValueError("factor table requires more than 32 model services")
    return dict(sorted(table.items()))


def available_universe(path: Path = Path("config/cta_spread_factor_model_1m_pub.toml")) -> dict:
    import tomllib
    config = tomllib.loads(path.read_text())
    return {"factors": config["universe"]["factors"], "symbols": config["universe"]["symbols"]}


def validate_available(table: dict, universe: dict) -> None:
    factors, symbols = set(universe['factors']), set(universe['symbols'])
    for symbol, row in table.items():
        if symbol not in symbols:
            raise ValueError(f"{symbol}: symbol is not computed by the current publisher")
        for factor in row:
            if factor not in factors:
                raise ValueError(f"{symbol}/{factor}: factor is unavailable in the current publisher")


def _revision(table: dict) -> str:
    return hashlib.sha256(json.dumps(table, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def read_factor_table(client: Any, env: str) -> dict:
    key = factor_key(env)
    raw = client.get(key)
    if isinstance(raw, bytes):
        raw = raw.decode()
    table = normalize_table(json.loads(raw)) if raw is not None else {}
    return {"key": key, "exists": raw is not None, "symbol_factors": table, "revision": _revision(table)}


def save_factor_table(client: Any, env: str, raw: Any, expected_revision: str, universe: dict | None = None) -> dict:
    table = normalize_table(raw)
    validate_available(table, universe if universe is not None else available_universe())
    key = factor_key(env)
    current = client.get(key)
    if isinstance(current, bytes):
        current = current.decode()
    previous = normalize_table(json.loads(current)) if current is not None else {}
    if not expected_revision or expected_revision != _revision(previous):
        raise ValueError("configuration changed; reload before saving")
    payload = json.dumps(table, sort_keys=True, separators=(",", ":"))
    updated = client.eval(
        "if (redis.call('GET', KEYS[1]) or '') ~= ARGV[1] then return 0 end "
        "redis.call('SET', KEYS[1], ARGV[2]); return 1",
        1, key, current or "", payload,
    )
    if updated != 1:
        raise ValueError("configuration changed; reload before saving")
    return {"key": key, "exists": True, "symbol_factors": table, "revision": _revision(table)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-name", required=True)
    parser.add_argument("--config", type=Path, help="JSON factor table to replace; default is read-only")
    parser.add_argument("--execute", action="store_true", help="apply the replacement; without this flag only print the plan")
    parser.add_argument("--check", action="store_true", help="require an existing valid Redis table; read-only")
    args = parser.parse_args()
    if args.check and (args.config or args.execute):
        parser.error("--check cannot be combined with --config or --execute")
    factor_key(args.env_name)
    table = normalize_table(json.loads(args.config.read_text(), object_pairs_hook=reject_duplicate_keys)) if args.config else None
    if table is not None:
        validate_available(table, available_universe())
    if table is not None and not args.execute:
        print(json.dumps({"key": factor_key(args.env_name), "execute": False, "symbol_factors": table}, indent=2))
        return
    if args.execute and table is None:
        parser.error("--execute requires --config")
    import redis
    client = redis.Redis(
        host=os.getenv("REDIS_HOST", "127.0.0.1"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        username=os.getenv("REDIS_USERNAME"),
        password=os.getenv("REDIS_PASSWORD"),
        socket_connect_timeout=3, socket_timeout=3,
    )
    current = read_factor_table(client, args.env_name)
    if args.check and not current["exists"]:
        raise SystemExit(f"missing CTA factor configuration {current['key']}")
    result = save_factor_table(client, args.env_name, table, current["revision"]) if table is not None else current
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
