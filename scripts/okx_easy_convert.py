#!/usr/bin/env python3
"""Convert OKX small balances via the easy-convert API.

Default mode is dry-run: the script fetches the currently eligible small assets
and prints the convert plan. Add --execute to submit the conversion.

Examples:
  python3 scripts/okx_easy_convert.py --env-name okex-intra-arb01
  python3 scripts/okx_easy_convert.py --env-name okex-intra-arb01 --assets ADA,ANIME,LTC
  python3 scripts/okx_easy_convert.py --env-name okex-intra-arb01 --all-listed --to-ccy USDT --execute
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_BASE_URL = os.environ.get("OKX_BASE_URL", "https://www.okx.com")
AUTHORITATIVE_KEYS = (
    "OKX_API_KEY",
    "OKX_API_SECRET",
    "OKX_PASSPHRASE",
    "OKX_BASE_URL",
    "OKX_SIMULATED_TRADING",
)


@dataclass
class ConvertibleAsset:
    ccy: str
    avail: str = ""
    est_usd: str = ""
    raw: Dict[str, Any] | None = None


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "")
    if not raw:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Use OKX easy-convert to clean up small spot balances",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--env-name", help="Source $HOME/<env-name>/env.sh before calling OKX")
    parser.add_argument("--env-dir", help="Source <env-dir>/env.sh before calling OKX")
    parser.add_argument("--no-env-sh", action="store_true", help="Do not auto-source env.sh")
    parser.add_argument("--assets", help="Comma/space separated asset list, e.g. ADA,ANIME,LTC")
    parser.add_argument(
        "--asset",
        action="append",
        default=[],
        help="Append one asset to the convert set; repeatable",
    )
    parser.add_argument("--skip-assets", help="Comma/space separated assets to exclude")
    parser.add_argument(
        "--all-listed",
        action="store_true",
        help="Use every asset returned by easy-convert-currency-list",
    )
    parser.add_argument(
        "--to-ccy",
        default="USDT",
        help="Target currency for convert",
    )
    parser.add_argument(
        "--source",
        choices=["trading", "funding"],
        default="trading",
        help="Account source used by OKX easy-convert",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="OKX REST base URL")
    parser.add_argument("--timeout", type=int, default=10, help="HTTP timeout seconds")
    parser.add_argument(
        "--max-batch",
        type=int,
        default=5,
        help="Max fromCcy count per request; OKX documents 5 as the upper bound",
    )
    parser.add_argument(
        "--sleep-sec",
        type=float,
        default=2.1,
        help="Sleep between batches to stay under the documented rate limit",
    )
    parser.add_argument(
        "--simulate",
        action="store_true",
        default=env_flag("OKX_SIMULATED_TRADING", False),
        help="Add x-simulated-trading: 1 header",
    )
    parser.add_argument("--print-json", action="store_true", help="Print raw JSON responses")
    parser.add_argument("--execute", action="store_true", help="Actually submit easy-convert")
    return parser.parse_args()


def normalize_ccys(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in values:
        for part in str(raw).replace(",", " ").split():
            ccy = part.strip().upper()
            if ccy and ccy not in seen:
                seen.add(ccy)
                out.append(ccy)
    return out


def resolve_env_file(args: argparse.Namespace) -> Optional[Path]:
    if args.no_env_sh:
        return None
    if args.env_dir:
        return Path(args.env_dir).expanduser().resolve() / "env.sh"
    if args.env_name:
        return Path.home() / args.env_name / "env.sh"
    cwd_env = Path.cwd() / "env.sh"
    if cwd_env.is_file():
        return cwd_env
    return None


def auto_source_env_sh(env_path: Path) -> None:
    if not env_path.is_file():
        raise SystemExit(f"env.sh not found: {env_path}")
    quoted = shlex.quote(str(env_path))
    proc = subprocess.run(
        ["bash", "-lc", f"set -a; source {quoted} >/dev/null 2>&1; env -0"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        raise SystemExit(f"failed to source env.sh: {env_path}")
    for item in proc.stdout.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key_b, value_b = item.split(b"=", 1)
        key = key_b.decode("utf-8", errors="ignore")
        new_value = value_b.decode("utf-8", errors="replace")
        if key in AUTHORITATIVE_KEYS:
            old = os.environ.get(key)
            if old and old != new_value:
                print(
                    f"[WARN] env.sh overrides existing {key} from process env",
                    file=sys.stderr,
                )
            os.environ[key] = new_value
        elif key not in os.environ:
            os.environ[key] = new_value


def load_credentials() -> Tuple[str, str, str]:
    api_key = os.environ.get("OKX_API_KEY", "").strip()
    api_secret = os.environ.get("OKX_API_SECRET", "").strip()
    passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()
    missing = [
        name
        for name, value in (
            ("OKX_API_KEY", api_key),
            ("OKX_API_SECRET", api_secret),
            ("OKX_PASSPHRASE", passphrase),
        )
        if not value
    ]
    if missing:
        raise SystemExit(f"missing env vars: {', '.join(missing)}")
    return api_key, api_secret, passphrase


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def compact_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def sign(timestamp: str, method: str, request_path: str, body: str, secret: str) -> str:
    payload = f"{timestamp}{method.upper()}{request_path}{body}"
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def request_okx(
    base_url: str,
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
    simulated: bool = False,
) -> Tuple[int, str, Dict[str, str]]:
    method = method.upper()
    params = params or {}
    query = urllib.parse.urlencode(sorted((k, str(v)) for k, v in params.items()), safe="-_.~")
    request_path = path if not query else f"{path}?{query}"
    body_text = "" if method == "GET" else compact_json(body or {})
    timestamp = utc_timestamp()
    signature = sign(timestamp, method, request_path, body_text, api_secret)

    url = f"{base_url.rstrip('/')}{request_path}"
    data = None if method == "GET" else body_text.encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    req.add_header("OK-ACCESS-KEY", api_key)
    req.add_header("OK-ACCESS-SIGN", signature)
    req.add_header("OK-ACCESS-TIMESTAMP", timestamp)
    req.add_header("OK-ACCESS-PASSPHRASE", passphrase)
    if simulated:
        req.add_header("x-simulated-trading", "1")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            body_text = resp.read().decode("utf-8", "replace")
            headers = dict(resp.headers.items())
            return status, body_text, headers
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", "replace")
        headers = dict(getattr(exc, "headers", {}).items()) if getattr(exc, "headers", None) else {}
        return exc.code, body_text, headers
    except Exception as exc:  # pragma: no cover - network failure
        return 0, str(exc), {}


def parse_json(body_text: str) -> Dict[str, Any]:
    try:
        payload = json.loads(body_text)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"non-JSON response: {exc}: {body_text[:400]}") from exc
    if not isinstance(payload, dict):
        raise SystemExit(f"unexpected response type: {type(payload).__name__}")
    return payload


def require_ok(payload: Dict[str, Any], label: str) -> None:
    code = str(payload.get("code", "")).strip()
    if code == "0":
        return
    msg = str(payload.get("msg", "")).strip()
    raise SystemExit(f"{label} failed: code={code} msg={msg} raw={compact_json(payload)[:600]}")


def first_text(row: Dict[str, Any], keys: Sequence[str]) -> str:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def parse_convert_list(payload: Dict[str, Any]) -> Tuple[List[ConvertibleAsset], List[str]]:
    assets: Dict[str, ConvertibleAsset] = {}
    to_ccys: List[str] = []
    data = payload.get("data")
    blocks: List[Any]
    if isinstance(data, list):
        blocks = data
    elif isinstance(data, dict):
        blocks = [data]
    else:
        blocks = []

    for block in blocks:
        if not isinstance(block, dict):
            continue
        from_rows = block.get("fromData") or block.get("fromCcyData") or []
        for row in from_rows:
            if not isinstance(row, dict):
                continue
            ccy = first_text(row, ("ccy", "fromCcy", "coin", "asset", "currency")).upper()
            if not ccy:
                continue
            avail = first_text(
                row,
                (
                    "availBal",
                    "bal",
                    "amt",
                    "fromAmt",
                    "canConvertAmt",
                    "size",
                ),
            )
            est_usd = first_text(
                row,
                (
                    "usdVal",
                    "usdAmt",
                    "eqUsd",
                    "availEq",
                    "valueUsd",
                ),
            )
            assets[ccy] = ConvertibleAsset(ccy=ccy, avail=avail, est_usd=est_usd, raw=row)

        to_rows = block.get("toData") or block.get("toCcyData") or []
        for row in to_rows:
            if not isinstance(row, dict):
                continue
            ccy = first_text(row, ("ccy", "toCcy", "coin", "asset", "currency")).upper()
            if ccy and ccy not in to_ccys:
                to_ccys.append(ccy)

    return sorted(assets.values(), key=lambda item: item.ccy), to_ccys


def fetch_easy_convert_list(
    *,
    base_url: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    timeout: int,
    simulated: bool,
    source_code: str,
) -> Dict[str, Any]:
    status, body_text, _headers = request_okx(
        base_url,
        "GET",
        "/api/v5/trade/easy-convert-currency-list",
        api_key,
        api_secret,
        passphrase,
        params={"source": source_code},
        timeout=timeout,
        simulated=simulated,
    )
    if not (200 <= status < 300):
        raise SystemExit(f"GET easy-convert-currency-list failed: status={status} body={body_text[:600]}")
    payload = parse_json(body_text)
    require_ok(payload, "GET easy-convert-currency-list")
    return payload


def submit_easy_convert(
    *,
    base_url: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    timeout: int,
    simulated: bool,
    source_code: str,
    from_ccys: Sequence[str],
    to_ccy: str,
) -> Dict[str, Any]:
    status, body_text, _headers = request_okx(
        base_url,
        "POST",
        "/api/v5/trade/easy-convert",
        api_key,
        api_secret,
        passphrase,
        body={
            "fromCcy": list(from_ccys),
            "toCcy": to_ccy,
            "source": source_code,
        },
        timeout=timeout,
        simulated=simulated,
    )
    if not (200 <= status < 300):
        raise SystemExit(f"POST easy-convert failed: status={status} body={body_text[:600]}")
    payload = parse_json(body_text)
    require_ok(payload, "POST easy-convert")
    return payload


def chunked(values: Sequence[str], size: int) -> Iterable[List[str]]:
    for start in range(0, len(values), size):
        yield list(values[start : start + size])


def render_asset_line(asset: ConvertibleAsset) -> str:
    parts = [asset.ccy]
    if asset.avail:
        parts.append(f"avail={asset.avail}")
    if asset.est_usd:
        parts.append(f"est_usd={asset.est_usd}")
    return "  " + " ".join(parts)


def print_convert_list(
    assets: Sequence[ConvertibleAsset],
    to_ccys: Sequence[str],
    *,
    requested: Sequence[str],
    selected: Sequence[str],
    source_label: str,
    to_ccy: str,
) -> None:
    print(f"OKX easy-convert source={source_label} to={to_ccy}")
    if to_ccys:
        print(f"Target currencies reported by OKX: {', '.join(to_ccys)}")
    print(f"Eligible small assets: {len(assets)}")
    if not assets:
        print("  (none)")
    for asset in assets:
        print(render_asset_line(asset))
    if requested:
        print(f"Requested assets: {', '.join(requested)}")
    if selected:
        print(f"Selected for convert: {', '.join(selected)}")


def summarize_convert_response(payload: Dict[str, Any]) -> List[str]:
    lines: List[str] = []
    data = payload.get("data")
    rows = data if isinstance(data, list) else []
    if not rows:
        return lines
    for row in rows:
        if not isinstance(row, dict):
            continue
        from_ccy = first_text(row, ("fromCcy", "ccy", "coin", "asset")).upper()
        to_ccy = first_text(row, ("toCcy", "targetCcy")).upper()
        from_amt = first_text(row, ("fromAmt", "fromSz", "fillFromSz", "amt"))
        to_amt = first_text(row, ("toAmt", "toSz", "fillToSz", "obtainedAmt"))
        trade_id = first_text(row, ("tradeId", "id", "convertId"))
        pieces = [from_ccy or "<unknown>"]
        if from_amt:
            pieces.append(f"from={from_amt}")
        if to_ccy:
            pieces.append(f"to={to_ccy}")
        if to_amt:
            pieces.append(f"got={to_amt}")
        if trade_id:
            pieces.append(f"trade_id={trade_id}")
        lines.append("  " + " ".join(pieces))
    return lines


def main() -> None:
    args = parse_args()
    if args.max_batch < 1 or args.max_batch > 5:
        raise SystemExit("--max-batch must be in [1, 5]")

    env_path = resolve_env_file(args)
    if env_path is not None:
        auto_source_env_sh(env_path)

    api_key, api_secret, passphrase = load_credentials()
    source_code = "1" if args.source == "trading" else "2"
    requested = normalize_ccys([args.assets or "", *args.asset])
    skip_assets = set(normalize_ccys([args.skip_assets or ""]))
    to_ccy = args.to_ccy.strip().upper()

    payload = fetch_easy_convert_list(
        base_url=args.base_url,
        api_key=api_key,
        api_secret=api_secret,
        passphrase=passphrase,
        timeout=args.timeout,
        simulated=args.simulate,
        source_code=source_code,
    )
    assets, to_ccys = parse_convert_list(payload)
    if args.print_json:
        print(compact_json(payload))

    if to_ccys and to_ccy not in set(to_ccys):
        raise SystemExit(
            f"requested to-ccy {to_ccy} not reported by OKX; available targets: {', '.join(to_ccys)}"
        )

    by_ccy = {asset.ccy: asset for asset in assets}
    if requested:
        selected = [ccy for ccy in requested if ccy in by_ccy]
    elif args.all_listed:
        selected = [asset.ccy for asset in assets]
    else:
        selected = []
    selected = [ccy for ccy in selected if ccy not in skip_assets and ccy != to_ccy]

    print_convert_list(
        assets,
        to_ccys,
        requested=requested,
        selected=selected,
        source_label=args.source,
        to_ccy=to_ccy,
    )

    missing = [ccy for ccy in requested if ccy not in by_ccy]
    if missing:
        print(
            f"Requested but not currently convertible: {', '.join(missing)}",
            file=sys.stderr,
        )

    if not args.execute:
        if selected:
            print(f"Dry-run batches ({args.max_batch} per request):")
            for index, batch in enumerate(chunked(selected, args.max_batch), start=1):
                print(f"  batch {index}: from={','.join(batch)} -> {to_ccy}")
        else:
            print("Dry-run only. Add --assets ... or --all-listed --execute to submit convert.")
        return

    if not selected:
        raise SystemExit("nothing selected for conversion; pass --assets or --all-listed")

    batches = list(chunked(selected, args.max_batch))
    for index, batch in enumerate(batches, start=1):
        print(f"[EXECUTE] batch {index}/{len(batches)} from={','.join(batch)} -> {to_ccy}")
        result = submit_easy_convert(
            base_url=args.base_url,
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            timeout=args.timeout,
            simulated=args.simulate,
            source_code=source_code,
            from_ccys=batch,
            to_ccy=to_ccy,
        )
        if args.print_json:
            print(compact_json(result))
        summary_lines = summarize_convert_response(result)
        if summary_lines:
            for line in summary_lines:
                print(line)
        else:
            print("  OKX accepted the convert request.")
        if index < len(batches):
            time.sleep(args.sleep_sec)


if __name__ == "__main__":
    main()
