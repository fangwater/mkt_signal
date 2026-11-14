#!/usr/bin/env python3
"""Publish audited monthly Binance/OKX trade-size CSVs; dry-run unless --execute."""
from __future__ import annotations

import argparse
import calendar
import csv
import datetime as dt
import hashlib
import io
import json
import os
from pathlib import Path
import re

from tlen_config_server import normalize_symbol_for_venue, parse_amount_threshold_value, redis_key

MARKETS = {"binance-futures": "binance-futures", "binance": "binance-margin", "okex-swap": "okex-futures"}
BINANCE_MARKETS = {"binance-futures", "binance-margin"}
UNITS = {"binance-futures": "USDT", "binance-margin": "USDT", "okex-futures": "price_x_contracts"}


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def load_csv(path: Path):
    raw = path.read_bytes()
    tables, months = {}, set()
    for line, row in enumerate(csv.DictReader(io.StringIO(raw.decode())), 2):
        try:
            venue = MARKETS[row["exchange"]]
            if row["unit"] != UNITS[venue]:
                raise ValueError(f"{venue}: expected unit {UNITS[venue]}")
            symbol = row["symbol"]
            if not re.fullmatch(r"[A-Z0-9]+USDT", symbol):
                raise ValueError("invalid USDT symbol")
            symbol = normalize_symbol_for_venue(symbol, venue)
            source = dt.datetime.strptime(row["source_month"], "%Y-%m").date()
            application = source.replace(day=28) + dt.timedelta(days=4)
            if row["apply_month"] != application.strftime("%Y-%m"):
                raise ValueError("apply_month must follow source_month")
            if (float(row["medium_quantile"]), float(row["large_quantile"])) != (0.5, 0.9):
                raise ValueError("requires P50/P90")
            hours = calendar.monthrange(source.year, source.month)[1] * 24
            if int(row["nonempty_hours"]) != hours or int(row["sample_count"]) < hours:
                raise ValueError("incomplete UTC month coverage")
            value = parse_amount_threshold_value(row)
            if value["medium_notional_threshold"] <= 0:
                raise ValueError("thresholds must be positive")
            table = tables.setdefault(venue, {})
            if symbol in table:
                raise ValueError("duplicate market/symbol")
            table[symbol] = encode(value)
            months.add((row["source_month"], row["apply_month"]))
        except (KeyError, ValueError, TypeError) as exc:
            raise ValueError(f"CSV line {line}: {exc}") from exc
    if len(months) != 1:
        raise ValueError("CSV must contain one nonempty source/apply month")
    source_month, apply_month = months.pop()
    return tables, {"source_month": source_month, "apply_month": apply_month,
                    "csv_sha256": hashlib.sha256(raw).hexdigest()}


def managed_keys(venues):
    return sorted({key for venue in venues
                   for key in (redis_key(venue, "amount_thresholds"),
                               redis_key(venue, "amount_thresholds") + ":meta",
                               redis_key(venue, "factor_plan"), redis_key(venue, "factor_plan_1m"))})


def validate_publication(tables, provenance, mode, current_month):
    if provenance["apply_month"] != current_month:
        raise ValueError(f"CSV apply_month must be current UTC month {current_month}")
    if mode == "replace":
        if (set(tables) != BINANCE_MARKETS or
                set(tables["binance-futures"]) != set(tables["binance-margin"])):
            raise ValueError("replace requires matching cash/futures symbol sets")
        if "BTCUSDT" not in tables["binance-futures"]:
            raise ValueError("replace must retain BTCUSDT for the live publisher heartbeat")


def build_replacement(tables, provenance, before, mode):
    after = {}
    for venue, incoming in tables.items():
        main = redis_key(venue, "amount_thresholds")
        current = before[main]["values"]
        merged = dict(incoming)
        if mode in ("backfill", "correct"):
            meta = before[main + ":meta"]["values"]
            if current and any(meta.get(k) != provenance[k] for k in ("source_month", "apply_month")):
                raise ValueError(f"{main}: backfill requires matching recorded source/apply month")
            if mode == "backfill":
                if current and meta.get("unit") != UNITS[venue]:
                    raise ValueError(f"{main}: backfill cannot mix quantity units")
                for symbol in current.keys() & incoming.keys():
                    if parse_amount_threshold_value(current[symbol]) != parse_amount_threshold_value(incoming[symbol]):
                        raise ValueError(f"{main}/{symbol}: backfill cannot overwrite an existing threshold")
            merged = {**current, **incoming}
        after[main] = merged
        old_meta = before[main + ":meta"]["values"]
        hashes = json.loads(old_meta.get("csv_sha256_history", "[]")) if mode in ("backfill", "correct") else []
        hashes = sorted(set(hashes + [provenance["csv_sha256"]]))
        after[main + ":meta"] = {
            **provenance, "csv_sha256_history": encode(hashes),
            "medium_quantile": "0.5", "large_quantile": "0.9", "unit": UNITS[venue],
        } if merged else {}
        if mode == "correct":
            after[main + ":meta"]["corrected_from_csv_sha256"] = old_meta["csv_sha256"]
        for family in ("factor_plan", "factor_plan_1m"):
            plan_key = redis_key(venue, family)
            current_plan = before[plan_key]["values"]
            after[plan_key] = (dict(current_plan) if mode in ("backfill", "correct") else
                               {s: v for s, v in current_plan.items() if s in merged})
    return after


def snapshot(client, keys):
    result = {}
    for key in keys:
        kind = client.type(key)
        if kind not in ("none", "hash"):
            raise ValueError(f"unexpected Redis type {key}: {kind}")
        ttl = client.pttl(key)
        if ttl >= 0:
            raise ValueError(f"refusing expiring configuration key: {key}")
        result[key] = {"type": kind, "values": client.hgetall(key), "pttl": ttl}
    return result


def describe(before, after):
    result = {}
    for key, new in after.items():
        old = before[key]["values"]
        if key.endswith(":meta"):
            continue
        result[key] = {"before": len(old), "after": len(new),
                       "added": sorted(new.keys() - old.keys()),
                       "removed": sorted(old.keys() - new.keys()),
                       "updated": sorted(s for s in old.keys() & new.keys() if old[s] != new[s])}
    return result


def publish(client, tables, provenance, mode, execute, backup, correction_from=None):
    keys = managed_keys(tables)
    with client.pipeline() as pipe:
        pipe.watch(*keys)
        before = snapshot(pipe, keys)
        if mode == "correct":
            if correction_from is None:
                raise ValueError("correct requires an owned previous publication backup")
            owned = json.loads(correction_from.read_text())
            if not owned.get("report", {}).get("execute"):
                raise ValueError("correction source must describe an executed publication")
            for key in keys:
                if key not in owned["after"] or before[key]["values"] != owned["after"][key]:
                    raise ValueError(f"{key}: changed since the owned publication; refusing correction")
            for venue, incoming in tables.items():
                main = redis_key(venue, "amount_thresholds")
                if set(incoming) != set(before[main]["values"]):
                    raise ValueError("correction must cover exactly the owned symbol set")
        after = build_replacement(tables, provenance, before, mode)
        report = {"execute": execute, "mode": mode, **provenance,
                  "units": {venue: UNITS[venue] for venue in tables}, "changes": describe(before, after)}
        if correction_from is not None:
            report["correction_from"] = str(correction_from)
        if not execute:
            return report
        # Back up exact hashes before the single atomic multi-key transaction.
        backup.parent.mkdir(parents=True, exist_ok=True)
        with backup.open("x", encoding="utf-8") as handle:
            os.chmod(backup, 0o600)
            json.dump({"created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                       "report": report, "before": before, "after": after}, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        pipe.multi()
        for key, value in after.items():
            pipe.delete(key)
            if value:
                pipe.hset(key, mapping=value)
        pipe.execute()  # WATCH aborts if an operator changed any key after the snapshot.
    actual = snapshot(client, keys)
    if any(actual[k]["values"] != v for k, v in after.items()):
        raise RuntimeError("post-write verification failed; inspect backup before any retry")
    report["verified"] = True
    report["backup"] = str(backup)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--mode", choices=("replace", "backfill", "correct"), default="backfill")
    parser.add_argument("--correct-from", type=Path, help="Exact before/after backup of the publication being corrected")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--backup", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    if args.execute and args.backup is None:
        parser.error("--execute requires a new --backup path")
    if (args.mode == "correct") != (args.correct_from is not None):
        parser.error("--correct-from is required only with --mode correct")
    tables, provenance = load_csv(args.csv)
    current_month = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m")
    try:
        validate_publication(tables, provenance, args.mode, current_month)
    except ValueError as exc:
        parser.error(str(exc))
    import redis
    client = redis.Redis(host=os.getenv("REDIS_HOST", "127.0.0.1"),
                         port=int(os.getenv("REDIS_PORT", "6379")),
                         db=int(os.getenv("REDIS_DB", "0")),
                         username=os.getenv("REDIS_USERNAME"), password=os.getenv("REDIS_PASSWORD"),
                         decode_responses=True, socket_connect_timeout=5, socket_timeout=5)
    report = publish(client, tables, provenance, args.mode, args.execute, args.backup, args.correct_from)
    if args.report:
        args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
