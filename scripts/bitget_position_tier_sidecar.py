#!/usr/bin/env python3
"""Bitget position-tier sidecar.

This process is intentionally outside the trading hot path. It reads a Redis
env pool, expands current online symbols from Redis, deduplicates Bitget
symbols, queries Bitget public position-tier metadata round-robin, and writes a
single materialized JSON cache back to Redis.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from lib.bitget_tier_pool import (
    DEFAULT_CACHE_KEY,
    DEFAULT_POOL_KEY,
    bitget_symbol,
    expand_pool_symbols,
    load_pool_from_redis,
    now_ms,
)


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def redis_client(args: argparse.Namespace) -> Any:
    redis = try_import_redis()
    if redis is None:
        raise SystemExit("redis package is not installed; run pip install redis")
    host = args.redis_host or os.environ.get("REDIS_HOST", "127.0.0.1")
    port = args.redis_port if args.redis_port is not None else int(os.environ.get("REDIS_PORT", "6379"))
    db = args.redis_db if args.redis_db is not None else int(os.environ.get("REDIS_DB", "0"))
    password = args.redis_password if args.redis_password is not None else os.environ.get("REDIS_PASSWORD", "")
    return redis.Redis(host=host, port=port, db=db, password=password or None)


def dec(value: Any, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return Decimal(default)


def int_or_zero(value: Any) -> int:
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return 0


def decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def parse_symbols(values: Iterable[str]) -> List[str]:
    out = set()
    for value in values:
        for part in str(value or "").replace(",", " ").split():
            symbol = bitget_symbol(part)
            if symbol:
                out.add(symbol)
    return sorted(out)


def load_existing_state(rds: Any, key: str) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str], Dict[str, int]]:
    raw = rds.get(key)
    if not raw:
        return {}, {}, {}
    text = raw.decode("utf-8", "ignore") if isinstance(raw, (bytes, bytearray)) else str(raw)
    try:
        payload = json.loads(text)
    except Exception:
        return {}, {}, {}
    if not isinstance(payload, dict):
        return {}, {}, {}

    records: Dict[str, Dict[str, Any]] = {}
    symbols = payload.get("symbols")
    if isinstance(symbols, dict):
        for symbol, record in symbols.items():
            if isinstance(record, dict):
                records[str(symbol).upper()] = record

    errors: Dict[str, str] = {}
    raw_errors = payload.get("errors")
    if isinstance(raw_errors, dict):
        for symbol, error in raw_errors.items():
            if isinstance(error, dict):
                message = str(error.get("message") or error.get("error") or "")
            else:
                message = str(error)
            if message:
                errors[str(symbol).upper()] = message

    last_attempt_ms: Dict[str, int] = {}
    raw_attempts = payload.get("last_attempt_ms")
    if isinstance(raw_attempts, dict):
        for symbol, ts in raw_attempts.items():
            value = int_or_zero(ts)
            if value > 0:
                last_attempt_ms[str(symbol).upper()] = value

    for symbol, record in records.items():
        value = int_or_zero(record.get("updated_at_ms"))
        if value > 0:
            last_attempt_ms[symbol] = max(last_attempt_ms.get(symbol, 0), value)
    return records, errors, last_attempt_ms


def request_json(url: str, timeout: int) -> Tuple[int, Dict[str, Any], str]:
    req = urllib.request.Request(
        url,
        headers={"locale": "en-US", "User-Agent": "mkt-signal-bitget-tier-sidecar"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            text = resp.read().decode("utf-8", "replace")
            return resp.getcode(), json.loads(text), text
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", "replace")
        try:
            payload = json.loads(text)
        except Exception:
            payload = {"raw": text}
        return exc.code, payload, text


def fetch_bitget_tiers(*, base_url: str, product_type: str, symbol: str, timeout: int) -> Dict[str, Any]:
    params = urllib.parse.urlencode({"symbol": symbol, "productType": product_type})
    url = f"{base_url.rstrip('/')}/api/v2/mix/market/query-position-lever?{params}"
    status, payload, body = request_json(url, timeout)
    code = str(payload.get("code", "")) if isinstance(payload, dict) else ""
    if not (200 <= status < 300 and code in ("0", "00000")):
        msg = payload.get("msg") if isinstance(payload, dict) else body[:300]
        raise RuntimeError(f"status={status} code={code or 'N/A'} msg={msg}")
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise RuntimeError(f"invalid data shape: {body[:300]}")
    return normalize_tier_record(symbol, rows)


def normalize_tier_record(symbol: str, rows: Sequence[Any]) -> Dict[str, Any]:
    tiers: List[Dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        level = str(row.get("level", "")).strip()
        start_unit = dec(row.get("startUnit"))
        end_unit = dec(row.get("endUnit"))
        leverage = dec(row.get("leverage"))
        keep_margin_rate = dec(row.get("keepMarginRate"))
        if end_unit <= 0 or leverage <= 0:
            continue
        tiers.append(
            {
                "level": level,
                "startUnit": decimal_text(start_unit),
                "endUnit": decimal_text(end_unit),
                "leverage": decimal_text(leverage),
                "keepMarginRate": decimal_text(keep_margin_rate),
            }
        )
    tiers.sort(key=lambda item: (dec(item["startUnit"]), dec(item["endUnit"])))
    if not tiers:
        raise RuntimeError(f"empty valid tiers for {symbol}")

    max_leverage = max(dec(item["leverage"]) for item in tiers)
    risk_limit_by_leverage: Dict[str, str] = {}
    for leverage in range(1, int(max_leverage) + 1):
        cap = Decimal("0")
        for item in tiers:
            if dec(item["leverage"]) >= Decimal(leverage):
                cap = max(cap, dec(item["endUnit"]))
        if cap > 0:
            risk_limit_by_leverage[str(leverage)] = decimal_text(cap)

    return {
        "symbol": symbol,
        "updated_at_ms": now_ms(),
        "max_leverage": decimal_text(max_leverage),
        "risk_limit_by_leverage": risk_limit_by_leverage,
        "tiers": tiers,
    }


def cache_payload(
    *,
    pool_key: str,
    cache_key: str,
    product_type: str,
    active_symbols: Sequence[str],
    records: Dict[str, Dict[str, Any]],
    errors: Dict[str, str],
    last_attempt_ms: Dict[str, int],
) -> str:
    active = sorted(set(active_symbols))
    active_set = set(active)
    active_records = {
        symbol: records[symbol]
        for symbol in sorted(records)
        if symbol in active_set and isinstance(records.get(symbol), dict)
    }
    missing = sorted(symbol for symbol in active if symbol not in active_records)
    payload = {
        "version": 1,
        "source": "bitget_position_tier_sidecar",
        "pool_key": pool_key,
        "cache_key": cache_key,
        "product_type": product_type,
        "updated_at_ms": now_ms(),
        "active_symbol_count": len(active),
        "cached_symbol_count": len(active_records),
        "missing_symbol_count": len(missing),
        "active_symbols": active,
        "missing_symbols": missing,
        "last_attempt_ms": {symbol: last_attempt_ms[symbol] for symbol in sorted(last_attempt_ms) if symbol in active_set},
        "errors": {symbol: errors[symbol] for symbol in sorted(errors) if symbol in active_set},
        "symbols": active_records,
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def write_cache(
    rds: Any,
    *,
    pool_key: str,
    cache_key: str,
    product_type: str,
    active_symbols: Sequence[str],
    records: Dict[str, Dict[str, Any]],
    errors: Dict[str, str],
    last_attempt_ms: Dict[str, int],
) -> None:
    payload = cache_payload(
        pool_key=pool_key,
        cache_key=cache_key,
        product_type=product_type,
        active_symbols=active_symbols,
        records=records,
        errors=errors,
        last_attempt_ms=last_attempt_ms,
    )
    rds.set(cache_key, payload)


def symbol_last_query_ms(symbol: str, records: Dict[str, Dict[str, Any]], last_attempt_ms: Dict[str, int]) -> int:
    record = records.get(symbol)
    record_ts = int_or_zero(record.get("updated_at_ms")) if isinstance(record, dict) else 0
    return max(record_ts, int_or_zero(last_attempt_ms.get(symbol)))


def symbol_ready(
    symbol: str,
    *,
    now_ms_value: int,
    cooldown_ms: int,
    records: Dict[str, Dict[str, Any]],
    last_attempt_ms: Dict[str, int],
) -> bool:
    if cooldown_ms <= 0:
        return True
    last_query = symbol_last_query_ms(symbol, records, last_attempt_ms)
    return last_query <= 0 or now_ms_value - last_query >= cooldown_ms


def eligible_symbol_count(
    symbols: Sequence[str],
    *,
    now_ms_value: int,
    cooldown_ms: int,
    records: Dict[str, Dict[str, Any]],
    last_attempt_ms: Dict[str, int],
) -> int:
    return sum(
        1
        for symbol in symbols
        if symbol_ready(
            symbol,
            now_ms_value=now_ms_value,
            cooldown_ms=cooldown_ms,
            records=records,
            last_attempt_ms=last_attempt_ms,
        )
    )


def ordered_eligible_batch(
    symbols: Sequence[str],
    cursor: int,
    batch_size: int,
    *,
    now_ms_value: int,
    cooldown_ms: int,
    records: Dict[str, Dict[str, Any]],
    last_attempt_ms: Dict[str, int],
) -> Tuple[List[str], int, int]:
    if not symbols:
        return [], 0, 0
    eligible_total = eligible_symbol_count(
        symbols,
        now_ms_value=now_ms_value,
        cooldown_ms=cooldown_ms,
        records=records,
        last_attempt_ms=last_attempt_ms,
    )
    if eligible_total <= 0:
        return [], cursor % len(symbols), 0

    size = max(1, batch_size)
    out: List[str] = []
    idx = cursor % len(symbols)
    scanned = 0
    while scanned < len(symbols) and len(out) < size:
        symbol = symbols[idx]
        idx = (idx + 1) % len(symbols)
        scanned += 1
        if symbol_ready(
            symbol,
            now_ms_value=now_ms_value,
            cooldown_ms=cooldown_ms,
            records=records,
            last_attempt_ms=last_attempt_ms,
        ):
            out.append(symbol)
    return out, idx, eligible_total


def refresh_active_symbols(args: argparse.Namespace, rds: Any) -> List[str]:
    specs = load_pool_from_redis(rds, args.pool_key)
    _expanded, query_symbols = expand_pool_symbols(rds, specs)
    manual = parse_symbols(args.symbol or [])
    symbols = sorted(set(query_symbols).union(manual))
    if args.max_symbols and args.max_symbols > 0:
        symbols = symbols[: args.max_symbols]
    return symbols


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Round-robin Bitget position-tier Redis sidecar.")
    parser.add_argument("--pool-key", default=DEFAULT_POOL_KEY, help=f"Redis env pool key (default: {DEFAULT_POOL_KEY})")
    parser.add_argument("--cache-key", default=DEFAULT_CACHE_KEY, help=f"Redis output cache key (default: {DEFAULT_CACHE_KEY})")
    parser.add_argument("--product-type", default="USDT-FUTURES")
    parser.add_argument("--base-url", default=os.environ.get("BITGET_API_BASE", "https://api.bitget.com"))
    parser.add_argument("--batch-size", type=int, default=3, help="Max eligible symbols queried per tick.")
    parser.add_argument("--interval-sec", type=float, default=20.0, help="Sleep seconds between ticks.")
    parser.add_argument("--symbol-cooldown-sec", type=float, default=1800.0, help="Minimum seconds between two queries of the same symbol.")
    parser.add_argument("--symbol-sleep-ms", type=int, default=150, help="Sleep between symbol requests inside a batch.")
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--once", action="store_true", help="Query one eligible batch and exit.")
    parser.add_argument("--dry-run", action="store_true", help="Expand symbols and print the planned eligible batch without HTTP/Redis SET.")
    parser.add_argument("--symbol", action="append", default=[], help="Additional symbol/asset to include. Can be repeated.")
    parser.add_argument("--max-symbols", type=int, default=0, help="Limit active symbols for tests.")
    parser.add_argument("--log-every-round", action="store_true", help="Log every tick, not only full cursor wraps/no-eligible ticks.")
    parser.add_argument("--redis-host", default=None)
    parser.add_argument("--redis-port", type=int, default=None)
    parser.add_argument("--redis-db", type=int, default=None)
    parser.add_argument("--redis-password", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be positive")
    if args.interval_sec < 0:
        raise SystemExit("--interval-sec must be non-negative")
    if args.symbol_cooldown_sec < 0:
        raise SystemExit("--symbol-cooldown-sec must be non-negative")
    if args.symbol_sleep_ms < 0:
        raise SystemExit("--symbol-sleep-ms must be non-negative")

    cooldown_ms = int(args.symbol_cooldown_sec * 1000)
    rds = redis_client(args)
    records, errors, last_attempt_ms = load_existing_state(rds, args.cache_key)
    cursor = 0
    tick = 0
    print(
        f"[start] pool_key={args.pool_key} cache_key={args.cache_key} "
        f"product_type={args.product_type} batch_size={args.batch_size} "
        f"interval_sec={args.interval_sec} symbol_cooldown_sec={args.symbol_cooldown_sec}",
        flush=True,
    )

    while True:
        tick += 1
        try:
            active_symbols = refresh_active_symbols(args, rds)
        except Exception as exc:
            print(f"[error] refresh active symbols failed: {exc}", file=sys.stderr, flush=True)
            if args.once:
                return 2
            time.sleep(max(args.interval_sec, 1.0))
            continue

        now_value = now_ms()
        if not active_symbols:
            print("[warn] active symbol set is empty", flush=True)
            if not args.dry_run:
                write_cache(
                    rds,
                    pool_key=args.pool_key,
                    cache_key=args.cache_key,
                    product_type=args.product_type,
                    active_symbols=[],
                    records=records,
                    errors=errors,
                    last_attempt_ms=last_attempt_ms,
                )
            if args.once:
                return 0
            time.sleep(max(args.interval_sec, 1.0))
            continue

        batch, next_cursor, eligible_total = ordered_eligible_batch(
            active_symbols,
            cursor,
            args.batch_size,
            now_ms_value=now_value,
            cooldown_ms=cooldown_ms,
            records=records,
            last_attempt_ms=last_attempt_ms,
        )
        if args.dry_run:
            print(
                json.dumps(
                    {
                        "active_symbol_count": len(active_symbols),
                        "eligible_symbol_count": eligible_total,
                        "cooling_symbol_count": len(active_symbols) - eligible_total,
                        "symbol_cooldown_sec": args.symbol_cooldown_sec,
                        "cursor": cursor,
                        "next_cursor": next_cursor,
                        "batch": batch,
                        "active_symbols": active_symbols,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0

        ok_count = 0
        for idx, symbol in enumerate(batch):
            attempt_ms = now_ms()
            last_attempt_ms[symbol] = attempt_ms
            try:
                record = fetch_bitget_tiers(
                    base_url=args.base_url,
                    product_type=args.product_type,
                    symbol=symbol,
                    timeout=args.timeout,
                )
                records[symbol] = record
                errors.pop(symbol, None)
                ok_count += 1
            except Exception as exc:
                errors[symbol] = str(exc)
                print(f"[warn] query failed symbol={symbol}: {exc}", file=sys.stderr, flush=True)
            if idx + 1 < len(batch) and args.symbol_sleep_ms > 0:
                time.sleep(args.symbol_sleep_ms / 1000.0)

        write_cache(
            rds,
            pool_key=args.pool_key,
            cache_key=args.cache_key,
            product_type=args.product_type,
            active_symbols=active_symbols,
            records=records,
            errors=errors,
            last_attempt_ms=last_attempt_ms,
        )

        wrapped = next_cursor <= cursor and len(active_symbols) > 0
        cursor = next_cursor
        if args.log_every_round or wrapped or args.once or not batch:
            active_set = set(active_symbols)
            cached_active = sum(1 for symbol in active_symbols if symbol in records)
            active_errors = sum(1 for symbol in errors if symbol in active_set)
            print(
                f"[tick] tick={tick} active={len(active_symbols)} eligible={eligible_total} "
                f"cooling={len(active_symbols) - eligible_total} batch={len(batch)} ok={ok_count} "
                f"cached_active={cached_active} missing={len(active_symbols) - cached_active} "
                f"errors={active_errors} cursor={cursor}",
                flush=True,
            )

        if args.once:
            return 0 if ok_count == len(batch) else 1
        time.sleep(args.interval_sec)


if __name__ == "__main__":
    raise SystemExit(main())
