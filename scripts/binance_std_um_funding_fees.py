#!/usr/bin/env python3
"""Query Binance STANDARD UM futures funding-fee income for intra envs.

Examples:
  # From the runtime environment directory:
  cd ~/binance-intra-arb01
  ./scripts/binance_std_um_funding_fees.py

  # From this repository:
  python3 scripts/binance_std_um_funding_fees.py --env-name binance-intra-arb01

This is read-only. It uses GET /fapi/v1/income with incomeType=FUNDING_FEE.
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
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Sequence


ENV_DIR_PATTERN = re.compile(r"^binance-intra-[a-z0-9][a-z0-9_-]*$")
AUTHORITATIVE_KEYS = (
    "BINANCE_API_KEY",
    "BINANCE_API_SECRET",
    "BINANCE_ACCOUNT_MODE",
    "BINANCE_FAPI_URL",
)
DEFAULT_FAPI_BASE_URL = "https://fapi.binance.com"
INCOME_PATH = "/fapi/v1/income"
FUNDING_INCOME_TYPE = "FUNDING_FEE"
MS_PER_HOUR = 60 * 60 * 1000


@dataclass(frozen=True)
class RuntimeEnv:
    env_name: str
    env_dir: Path


@dataclass(frozen=True)
class FundingRecord:
    symbol: str
    income_type: str
    income: Decimal
    asset: str
    info: str
    time_ms: int
    tran_id: str
    trade_id: str
    raw: Dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query Binance STANDARD UM futures funding-fee income",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--env-name",
        default=os.environ.get("ENV_NAME", ""),
        help="Runtime env name, e.g. binance-intra-arb01. Defaults to cwd basename when it matches.",
    )
    parser.add_argument(
        "--env-dir",
        default="",
        help="Runtime env directory. Defaults to $HOME/<env-name> or cwd when cwd matches env-name.",
    )
    parser.add_argument(
        "--no-env-sh",
        action="store_true",
        help="Do not source env.sh before reading Binance credentials.",
    )
    parser.add_argument(
        "--base-url",
        default=None,
        help="Binance USD-M Futures REST base URL. Omit to use BINANCE_FAPI_URL after env.sh.",
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=24.0,
        help="Lookback hours when --start-ms is not supplied.",
    )
    parser.add_argument(
        "--start-ms",
        type=int,
        default=None,
        help="Inclusive start time in milliseconds. Overrides --hours.",
    )
    parser.add_argument(
        "--end-ms",
        type=int,
        default=None,
        help="Inclusive end time in milliseconds. Defaults to current time.",
    )
    parser.add_argument(
        "--symbols",
        default="",
        help="Comma/space separated symbol whitelist, e.g. BTCUSDT,ETHUSDT. Omit for all symbols.",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        default=[],
        help="Restrict to one symbol; repeatable.",
    )
    parser.add_argument("--limit", type=int, default=1000, help="Binance page size; max 1000.")
    parser.add_argument("--max-pages", type=int, default=20, help="Safety cap per symbol/all-symbol query.")
    parser.add_argument("--recv-window", type=int, default=5000, help="recvWindow in milliseconds.")
    parser.add_argument("--timeout", type=int, default=10, help="HTTP timeout seconds.")
    parser.add_argument(
        "--records-limit",
        type=int,
        default=200,
        help="Maximum detail records to print in table mode. Use --all-records for no cap.",
    )
    parser.add_argument("--all-records", action="store_true", help="Print every detail record.")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of tables.")
    return parser.parse_args()


def now_ms() -> int:
    return int(time.time() * 1000)


def utc_text(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def decimal_value(value: Any) -> Decimal:
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return Decimal("0")


def resolve_runtime_env(args: argparse.Namespace) -> RuntimeEnv:
    env_name = (args.env_name or "").strip()
    env_dir_text = (args.env_dir or "").strip()
    cwd = Path.cwd()
    cwd_name = cwd.name.strip()

    if not env_name and ENV_DIR_PATTERN.match(cwd_name):
        env_name = cwd_name

    if env_dir_text:
        env_dir = Path(env_dir_text).expanduser().resolve()
        if not env_name:
            env_name = env_dir.name
    else:
        if not env_name:
            raise SystemExit(
                "missing --env-name/--env-dir; run in a binance-intra-* env dir or pass --env-name"
            )
        if cwd_name == env_name:
            env_dir = cwd
        else:
            env_dir = (Path.home() / env_name).resolve()

    if not ENV_DIR_PATTERN.match(env_name):
        raise SystemExit(f"env name must match ^binance-intra-, got {env_name!r}")
    return RuntimeEnv(env_name=env_name, env_dir=env_dir)


def auto_source_env_sh(env_dir: Path) -> None:
    env_path = env_dir / "env.sh"
    if not env_path.is_file():
        raise SystemExit(f"env.sh not found: {env_path}")

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
        stderr = proc.stderr.decode("utf-8", errors="replace").strip()
        raise SystemExit(f"failed to source {env_path}: exit={proc.returncode} {stderr}")

    for item in proc.stdout.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key_b, value_b = item.split(b"=", 1)
        try:
            key = key_b.decode("utf-8")
            value = value_b.decode("utf-8")
        except UnicodeDecodeError:
            continue
        if key in AUTHORITATIVE_KEYS:
            old = os.environ.get(key)
            if old and old != value:
                print(
                    f"[warn] env.sh overrides existing {key} from process env",
                    file=sys.stderr,
                )
            os.environ[key] = value
        elif key not in os.environ:
            os.environ[key] = value


def require_standard_account() -> tuple[str, str]:
    mode = os.environ.get("BINANCE_ACCOUNT_MODE", "").strip().upper()
    if mode != "STANDARD":
        raise SystemExit(
            "this script is for Binance STANDARD account mode; "
            f"BINANCE_ACCOUNT_MODE={mode or '<unset>'}"
        )
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        raise SystemExit("missing BINANCE_API_KEY / BINANCE_API_SECRET")
    return api_key, api_secret


def normalize_symbols(raw_symbols: str, repeated: Iterable[str]) -> list[str]:
    values: list[str] = []
    if raw_symbols:
        values.extend(part for part in re.split(r"[,\s]+", raw_symbols) if part)
    values.extend(part for part in repeated if part)

    seen: set[str] = set()
    out: list[str] = []
    for raw in values:
        symbol = raw.strip().upper()
        if not symbol:
            continue
        cleaned = re.sub(r"[^A-Z0-9]", "", symbol)
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)
    return out


def sign_query(query: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def signed_get(
    *,
    base_url: str,
    path: str,
    params: Dict[str, Any],
    api_key: str,
    api_secret: str,
    timeout: int,
) -> tuple[int, str, Dict[str, str]]:
    payload = dict(params)
    payload["timestamp"] = str(now_ms())
    query = urllib.parse.urlencode(sorted((k, str(v)) for k, v in payload.items()), safe="-_.~")
    signature = sign_query(query, api_secret)
    url = f"{base_url.rstrip('/')}{path}?{query}&signature={signature}"
    req = urllib.request.Request(url, method="GET", headers={"X-MBX-APIKEY": api_key})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return (
                resp.getcode(),
                resp.read().decode("utf-8", errors="replace"),
                dict(resp.headers.items()),
            )
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        headers = dict(getattr(exc, "headers", {}).items()) if getattr(exc, "headers", None) else {}
        return exc.code, body, headers
    except Exception as exc:
        return 0, str(exc), {}


def parse_funding_records(body: str) -> list[FundingRecord]:
    payload = json.loads(body)
    if not isinstance(payload, list):
        raise ValueError(f"expected list response, got {type(payload).__name__}")

    records: list[FundingRecord] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        time_raw = item.get("time", 0)
        try:
            time_ms = int(time_raw)
        except (TypeError, ValueError):
            time_ms = 0
        records.append(
            FundingRecord(
                symbol=str(item.get("symbol", "")).strip().upper(),
                income_type=str(item.get("incomeType", "")).strip().upper(),
                income=decimal_value(item.get("income", "0")),
                asset=str(item.get("asset", "")).strip().upper(),
                info=str(item.get("info", "")).strip(),
                time_ms=time_ms,
                tran_id=str(item.get("tranId", "")).strip(),
                trade_id=str(item.get("tradeId", "")).strip(),
                raw=item,
            )
        )
    return records


def query_income_pages(
    *,
    base_url: str,
    api_key: str,
    api_secret: str,
    start_ms: int,
    end_ms: int,
    symbol: Optional[str],
    limit: int,
    max_pages: int,
    recv_window: int,
    timeout: int,
) -> tuple[list[FundingRecord], int, str]:
    records: list[FundingRecord] = []
    pages = 0
    last_weight = "-"
    for page in range(1, max_pages + 1):
        params: Dict[str, Any] = {
            "incomeType": FUNDING_INCOME_TYPE,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
            "page": page,
            "recvWindow": recv_window,
        }
        if symbol:
            params["symbol"] = symbol
        status, body, headers = signed_get(
            base_url=base_url,
            path=INCOME_PATH,
            params=params,
            api_key=api_key,
            api_secret=api_secret,
            timeout=timeout,
        )
        pages += 1
        last_weight = (
            headers.get("x-mbx-used-weight-1m")
            or headers.get("x-mbx-used-weight")
            or headers.get("X-MBX-USED-WEIGHT-1M")
            or "-"
        )
        if not (200 <= status < 300):
            tag = f" symbol={symbol}" if symbol else ""
            raise SystemExit(f"Binance income query failed{tag}: status={status} body={body}")
        try:
            page_records = parse_funding_records(body)
        except (json.JSONDecodeError, ValueError) as exc:
            raise SystemExit(f"failed to parse Binance income response: {exc}; body={body}") from exc
        records.extend(page_records)
        if len(page_records) < limit:
            break
    return records, pages, last_weight


def make_window(args: argparse.Namespace) -> tuple[int, int]:
    end_ms = args.end_ms if args.end_ms is not None else now_ms()
    if args.start_ms is not None:
        start_ms = args.start_ms
    else:
        if args.hours <= 0:
            raise SystemExit("--hours must be positive")
        start_ms = end_ms - int(args.hours * MS_PER_HOUR)
    if start_ms > end_ms:
        raise SystemExit(f"start_ms must be <= end_ms: {start_ms} > {end_ms}")
    return start_ms, end_ms


def dedupe_records(records: Sequence[FundingRecord]) -> list[FundingRecord]:
    seen: set[tuple[str, str, int, str, str]] = set()
    out: list[FundingRecord] = []
    for rec in records:
        key = (rec.symbol, rec.tran_id, rec.time_ms, rec.asset, str(rec.income))
        if key in seen:
            continue
        seen.add(key)
        out.append(rec)
    return out


def summarize(records: Sequence[FundingRecord]) -> tuple[dict[str, Dict[str, Any]], dict[str, Dict[str, Any]]]:
    by_asset: dict[str, Dict[str, Any]] = {}
    by_symbol: dict[str, Dict[str, Any]] = {}

    def add(bucket: dict[str, Dict[str, Any]], key: str, rec: FundingRecord) -> None:
        item = bucket.setdefault(
            key or "-",
            {
                "records": 0,
                "received": Decimal("0"),
                "paid": Decimal("0"),
                "net": Decimal("0"),
                "last_time_ms": 0,
            },
        )
        item["records"] += 1
        if rec.income > 0:
            item["received"] += rec.income
        elif rec.income < 0:
            item["paid"] += rec.income
        item["net"] += rec.income
        item["last_time_ms"] = max(item["last_time_ms"], rec.time_ms)

    for rec in records:
        add(by_asset, rec.asset, rec)
        add(by_symbol, rec.symbol, rec)

    return by_asset, by_symbol


def dec_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def json_decimal(value: Decimal) -> str:
    return format(value, "f")


def table_print(rows: Sequence[Sequence[str]], headers: Sequence[str]) -> None:
    widths = [len(h) for h in headers]
    for row in rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(value))
    fmt = "  ".join("{:<" + str(width) + "}" for width in widths)
    print(fmt.format(*headers))
    print(fmt.format(*["-" * width for width in widths]))
    for row in rows:
        print(fmt.format(*row))


def print_summary_table(title: str, summary: dict[str, Dict[str, Any]], key_header: str) -> None:
    print("")
    print(title)
    rows: list[list[str]] = []
    for key, item in sorted(summary.items(), key=lambda kv: kv[0]):
        last_time_ms = int(item["last_time_ms"] or 0)
        rows.append(
            [
                key,
                str(item["records"]),
                dec_text(item["received"]),
                dec_text(item["paid"]),
                dec_text(item["net"]),
                utc_text(last_time_ms) if last_time_ms else "-",
            ]
        )
    if rows:
        table_print(rows, [key_header, "records", "received", "paid", "net", "last_utc"])
    else:
        print("(none)")


def print_records(records: Sequence[FundingRecord], limit: int, all_records: bool) -> None:
    sorted_records = sorted(records, key=lambda rec: rec.time_ms, reverse=True)
    shown = sorted_records if all_records else sorted_records[: max(0, limit)]
    print("")
    suffix = "" if len(shown) == len(sorted_records) else f" (showing {len(shown)}/{len(sorted_records)})"
    print(f"Funding fee records{suffix}")
    if not shown:
        print("(none)")
        return
    rows = []
    for rec in shown:
        direction = "received" if rec.income > 0 else "paid" if rec.income < 0 else "zero"
        rows.append(
            [
                utc_text(rec.time_ms),
                rec.symbol or "-",
                rec.asset or "-",
                dec_text(rec.income),
                direction,
                rec.tran_id or "-",
            ]
        )
    table_print(rows, ["time_utc", "symbol", "asset", "income", "direction", "tran_id"])


def print_json(
    *,
    runtime: RuntimeEnv,
    start_ms: int,
    end_ms: int,
    records: Sequence[FundingRecord],
    by_asset: dict[str, Dict[str, Any]],
    by_symbol: dict[str, Dict[str, Any]],
    symbols: Sequence[str],
    pages: int,
    used_weight: str,
    base_url: str,
) -> None:
    def summary_json(summary: dict[str, Dict[str, Any]]) -> dict[str, Any]:
        return {
            key: {
                "records": value["records"],
                "received": json_decimal(value["received"]),
                "paid": json_decimal(value["paid"]),
                "net": json_decimal(value["net"]),
                "last_time_ms": value["last_time_ms"],
                "last_time_utc": utc_text(value["last_time_ms"]) if value["last_time_ms"] else None,
            }
            for key, value in summary.items()
        }

    payload = {
        "env_name": runtime.env_name,
        "env_dir": str(runtime.env_dir),
        "query": {
            "base_url": base_url,
            "income_type": FUNDING_INCOME_TYPE,
            "symbols": list(symbols),
            "start_ms": start_ms,
            "end_ms": end_ms,
            "start_utc": utc_text(start_ms),
            "end_utc": utc_text(end_ms),
            "pages": pages,
            "used_weight_1m": used_weight,
        },
        "summary_by_asset": summary_json(by_asset),
        "summary_by_symbol": summary_json(by_symbol),
        "records": [
            {
                "time_ms": rec.time_ms,
                "time_utc": utc_text(rec.time_ms),
                "symbol": rec.symbol,
                "asset": rec.asset,
                "income": json_decimal(rec.income),
                "direction": "received" if rec.income > 0 else "paid" if rec.income < 0 else "zero",
                "income_type": rec.income_type,
                "info": rec.info,
                "tran_id": rec.tran_id,
                "trade_id": rec.trade_id,
            }
            for rec in sorted(records, key=lambda item: item.time_ms, reverse=True)
        ],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def main() -> int:
    args = parse_args()
    runtime = resolve_runtime_env(args)
    if not args.no_env_sh:
        auto_source_env_sh(runtime.env_dir)
    api_key, api_secret = require_standard_account()
    base_url = (args.base_url or os.environ.get("BINANCE_FAPI_URL") or DEFAULT_FAPI_BASE_URL).rstrip("/")

    limit = min(max(args.limit, 1), 1000)
    max_pages = max(args.max_pages, 1)
    start_ms, end_ms = make_window(args)
    symbols = normalize_symbols(args.symbols, args.symbol)
    query_symbols: list[Optional[str]] = list(symbols) if symbols else [None]

    all_records: list[FundingRecord] = []
    total_pages = 0
    used_weight = "-"
    for symbol in query_symbols:
        records, pages, used_weight = query_income_pages(
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            start_ms=start_ms,
            end_ms=end_ms,
            symbol=symbol,
            limit=limit,
            max_pages=max_pages,
            recv_window=args.recv_window,
            timeout=args.timeout,
        )
        all_records.extend(records)
        total_pages += pages

    all_records = dedupe_records(all_records)
    by_asset, by_symbol = summarize(all_records)

    if args.json:
        print_json(
            runtime=runtime,
            start_ms=start_ms,
            end_ms=end_ms,
            records=all_records,
            by_asset=by_asset,
            by_symbol=by_symbol,
            symbols=symbols,
            pages=total_pages,
            used_weight=used_weight,
            base_url=base_url,
        )
        return 0

    print(f"[info] env={runtime.env_name} env_dir={runtime.env_dir}")
    print(f"[info] account_mode=STANDARD base_url={base_url}")
    print(
        f"[info] window_utc={utc_text(start_ms)} -> {utc_text(end_ms)} "
        f"({start_ms}..{end_ms})"
    )
    print(
        f"[info] symbols={','.join(symbols) if symbols else '<all>'} "
        f"records={len(all_records)} pages={total_pages} used_weight_1m={used_weight}"
    )

    print_summary_table("Summary by asset", by_asset, "asset")
    print_summary_table("Summary by symbol", by_symbol, "symbol")
    print_records(all_records, args.records_limit, args.all_records)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
