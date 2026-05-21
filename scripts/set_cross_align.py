#!/usr/bin/env python3
"""Reduce-only reconciliation for both legs of a cross arb env.

Mirrors set_cross_futures_leverage.py / set_cross_cancel_all.py: parses
``<open>-<hedge>-cross-<suffix>``, sources ``env.sh``, drives off the same
Redis symbol union (cross_dump/fwd/bwd/unimmr_close). All orders submitted are
**reduce-only** market orders — never opens new exposure.

Modes:
  --mode align (default)
      Bring the larger leg down so ``|open| == |hedge|`` per symbol. Queries
      both legs, computes ``delta = open_qty + hedge_qty``, and submits
      ``min(|delta|, |larger_leg_qty|)`` on the leg with the larger ``|qty|``
      (``--reduce-side`` overrides).
  --mode clear
      Reduce-only flatten BOTH legs to 0 per symbol. Submits one order per
      non-zero leg sized to its full ``|position|``.

Order placement is implemented for **bitget + gate** in this iteration; the
other three exchanges (binance, okex, bybit) get positions/diagnostics but
will refuse ``--execute``.

Default is dry-run; add ``--execute`` to submit reduce-only market orders.

Examples:
  scripts/set_cross_align.py --env-name bitget-gate-cross-arb01
  scripts/set_cross_align.py --env-name bitget-gate-cross-arb01 --symbol XRP --execute
  scripts/set_cross_align.py --env-name bitget-gate-cross-arb01 --reduce-side open --execute
  scripts/set_cross_align.py --env-name bitget-gate-cross-arb01 --mode clear
  scripts/set_cross_align.py --env-name bitget-gate-cross-arb01 --mode clear --symbol BNB --execute
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import math
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
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

SUPPORTED_EXCHANGES = {"binance", "okex", "gate", "bybit", "bitget"}
PLACE_ORDER_SUPPORTED = {"bitget", "gate"}
NAMESPACE = "cross"


# ----------------------------------------------------------------------------
# Env + Redis (shared shape with set_cross_futures_leverage.py)
# ----------------------------------------------------------------------------


@dataclass(frozen=True)
class CrossContext:
    env_name: str
    open_ex: str
    hedge_ex: str
    open_venue: str
    hedge_venue: str
    suffix: str

    @property
    def key_suffix(self) -> str:
        return f"{self.open_ex}-{self.hedge_ex}"

    @property
    def unimmr_close_suffix(self) -> str:
        return f"{self.open_venue}_{self.hedge_venue}"


def normalize_exchange(value: str) -> str:
    text = (value or "").strip().lower()
    return "okex" if text == "okx" else text


def exchange_from_venue(venue: str) -> str:
    raw = (venue or "").strip().lower()
    if not raw or "-" not in raw:
        return ""
    return normalize_exchange(raw.split("-", 1)[0])


def parse_cross_env_name(value: str) -> Tuple[str, str, str]:
    text = (value or "").strip().lower()
    match = re.match(
        r"^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross[-_]([a-z0-9][a-z0-9_-]*?)(?:[-_](?:open|hedge))?$",
        text,
    )
    if not match:
        raise SystemExit(f"env-name must match <open>-<hedge>-cross-<suffix>: {value}")
    open_ex = normalize_exchange(match.group(1))
    hedge_ex = normalize_exchange(match.group(2))
    suffix = match.group(3)
    if open_ex == hedge_ex:
        raise SystemExit(f"cross requires distinct exchanges: {value}")
    if open_ex not in SUPPORTED_EXCHANGES or hedge_ex not in SUPPORTED_EXCHANGES:
        raise SystemExit(f"unsupported cross exchanges in env name: {value}")
    return open_ex, hedge_ex, suffix


def load_env_file(path: Path) -> None:
    if not path.is_file():
        print(f"[warn] env file not found: {path}", file=sys.stderr)
        return
    env = dict(os.environ)
    env["ENV_FILE"] = str(path)
    try:
        out = subprocess.check_output(
            ["bash", "-lc", 'set -a; source "$ENV_FILE"; env -0'], env=env
        )
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


def resolve_context(args: argparse.Namespace) -> Tuple[CrossContext, Path]:
    env_name = (args.env_name or "").strip().lower()
    env_dir_arg = (args.env_dir or "").strip()
    if not env_name and env_dir_arg:
        env_name = os.path.basename(env_dir_arg.rstrip("/")).strip().lower()
    if not env_name:
        cwd_name = os.path.basename(os.getcwd()).strip().lower()
        if re.match(r"^[a-z0-9]+[-_][a-z0-9]+[-_]cross[-_]", cwd_name):
            env_name = cwd_name
    if not env_name:
        raise SystemExit(
            "missing env name; pass --env-name <open>-<hedge>-cross-<suffix> or run from the env dir"
        )

    open_ex, hedge_ex, suffix = parse_cross_env_name(env_name)
    env_dir = Path(env_dir_arg) if env_dir_arg else Path.home() / env_name
    if not args.no_env_sh:
        load_env_file(env_dir / "env.sh")

    open_venue = (args.open_venue or os.environ.get("OPEN_VENUE") or f"{open_ex}-futures").strip().lower()
    hedge_venue = (args.hedge_venue or os.environ.get("HEDGE_VENUE") or f"{hedge_ex}-futures").strip().lower()
    if exchange_from_venue(open_venue) != open_ex or exchange_from_venue(hedge_venue) != hedge_ex:
        raise SystemExit(
            f"open/hedge venue does not match env name: open={open_venue} hedge={hedge_venue} env={env_name}"
        )

    ctx = CrossContext(
        env_name=env_name,
        open_ex=open_ex,
        hedge_ex=hedge_ex,
        open_venue=open_venue,
        hedge_venue=hedge_venue,
        suffix=suffix,
    )
    return ctx, env_dir


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


def cross_symbol_keys(ctx: CrossContext) -> List[str]:
    suffix = ctx.key_suffix
    return [
        f"{NAMESPACE}_dump_symbols:{suffix}",
        f"{NAMESPACE}_fwd_trade_symbols:{suffix}",
        f"{NAMESPACE}_bwd_trade_symbols:{suffix}",
        f"{ctx.env_name}:{NAMESPACE}_unimmr_close_symbols:{ctx.unimmr_close_suffix}",
    ]


def load_online_assets(rds: Any, ctx: CrossContext) -> List[str]:
    assets: Set[str] = set()
    for key in cross_symbol_keys(ctx):
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


# ----------------------------------------------------------------------------
# HTTP helpers
# ----------------------------------------------------------------------------


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


def now_ms() -> int:
    return int(time.time() * 1000)


def load_required(names: Iterable[str]) -> List[str]:
    values = [os.environ.get(name, "").strip() for name in names]
    missing = [name for name, value in zip(names, values) if not value]
    if missing:
        raise SystemExit(f"missing env vars: {', '.join(missing)}")
    return values


def to_float(value: Any) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return 0.0


def format_decimal(value: float) -> str:
    text = f"{value:.12f}".rstrip("0").rstrip(".")
    return text if text else "0"


# ----------------------------------------------------------------------------
# Lot-size specs (all 5 exchanges)
# ----------------------------------------------------------------------------


@dataclass
class LotSpec:
    exchange: str
    symbol: str        # exchange-native symbol
    qty_step: float    # smallest qty increment (in exchange's quantity units)
    min_qty: float     # minimum order size (in exchange's quantity units)
    contract_size: float = 1.0  # how many base coins per 1 qty unit (OKX ctVal, Gate quanto_multiplier, Bitget sizeMultiplier)


def floor_to_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    # round down toward zero
    sign = -1.0 if value < 0 else 1.0
    return sign * math.floor(abs(value) / step) * step


def asset_qty_to_exchange_qty(asset_qty: float, spec: LotSpec) -> float:
    """Convert base-asset qty to the exchange's quantity unit (contracts)."""
    if spec.contract_size <= 0:
        return asset_qty
    return asset_qty / spec.contract_size


def exchange_qty_to_asset_qty(exch_qty: float, spec: LotSpec) -> float:
    return exch_qty * (spec.contract_size if spec.contract_size > 0 else 1.0)


class LotSpecCache:
    def __init__(self) -> None:
        self._cache: Dict[Tuple[str, str], LotSpec] = {}
        self._raw: Dict[str, List[Dict[str, Any]]] = {}

    def get(self, exchange: str, symbol: str, timeout: int) -> Optional[LotSpec]:
        key = (exchange, symbol)
        if key in self._cache:
            return self._cache[key]
        spec = self._fetch(exchange, symbol, timeout)
        if spec is not None:
            self._cache[key] = spec
        return spec

    def _fetch(self, exchange: str, symbol: str, timeout: int) -> Optional[LotSpec]:
        if exchange == "binance":
            return self._binance(symbol, timeout)
        if exchange == "okex":
            return self._okex(symbol, timeout)
        if exchange == "gate":
            return self._gate(symbol, timeout)
        if exchange == "bybit":
            return self._bybit(symbol, timeout)
        if exchange == "bitget":
            return self._bitget(symbol, timeout)
        return None

    def _binance(self, symbol: str, timeout: int) -> Optional[LotSpec]:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        status, body, _ = http_json_request("GET", url, timeout=timeout)
        if not (200 <= status < 300):
            return None
        try:
            data = json.loads(body)
        except Exception:
            return None
        for entry in data.get("symbols", []):
            if str(entry.get("symbol", "")).upper() != symbol:
                continue
            step = 0.0
            min_qty = 0.0
            for f in entry.get("filters", []):
                if f.get("filterType") == "LOT_SIZE":
                    step = to_float(f.get("stepSize"))
                    min_qty = to_float(f.get("minQty"))
                    break
            return LotSpec("binance", symbol, step or 0.001, min_qty, 1.0)
        return None

    def _okex(self, symbol: str, timeout: int) -> Optional[LotSpec]:
        url = f"https://openapi.okx.com/api/v5/public/instruments?instType=SWAP&instId={symbol}"
        status, body, _ = http_json_request("GET", url, timeout=timeout)
        if not (200 <= status < 300):
            return None
        try:
            data = json.loads(body)
        except Exception:
            return None
        for entry in data.get("data", []):
            if str(entry.get("instId", "")).upper() != symbol:
                continue
            lot = to_float(entry.get("lotSz"))
            min_sz = to_float(entry.get("minSz"))
            ct_val = to_float(entry.get("ctVal"))
            return LotSpec("okex", symbol, lot or 1.0, min_sz, ct_val if ct_val > 0 else 1.0)
        return None

    def _gate(self, symbol: str, timeout: int) -> Optional[LotSpec]:
        if "gate" not in self._raw:
            url = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
            status, body, _ = http_json_request("GET", url, timeout=timeout)
            if not (200 <= status < 300):
                return None
            try:
                self._raw["gate"] = json.loads(body)
            except Exception:
                return None
        for entry in self._raw.get("gate", []):
            if str(entry.get("name", "")).upper() != symbol:
                continue
            quanto = to_float(entry.get("quanto_multiplier"))
            min_sz = to_float(entry.get("order_size_min"))
            # Gate's `size` is in contracts; contract_size = quanto_multiplier (base coins per contract).
            return LotSpec("gate", symbol, 1.0, min_sz or 1.0, quanto if quanto > 0 else 1.0)
        return None

    def _bybit(self, symbol: str, timeout: int) -> Optional[LotSpec]:
        url = f"https://api.bybit.com/v5/market/instruments-info?category=linear&symbol={symbol}"
        status, body, _ = http_json_request("GET", url, timeout=timeout)
        if not (200 <= status < 300):
            return None
        try:
            data = json.loads(body)
        except Exception:
            return None
        for entry in (data.get("result") or {}).get("list", []):
            if str(entry.get("symbol", "")).upper() != symbol:
                continue
            f = entry.get("lotSizeFilter") or {}
            step = to_float(f.get("qtyStep"))
            min_qty = to_float(f.get("minOrderQty"))
            return LotSpec("bybit", symbol, step or 0.001, min_qty, 1.0)
        return None

    def _bitget(self, symbol: str, timeout: int) -> Optional[LotSpec]:
        if "bitget" not in self._raw:
            url = "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES"
            status, body, _ = http_json_request("GET", url, timeout=timeout)
            if not (200 <= status < 300):
                return None
            try:
                parsed = json.loads(body)
            except Exception:
                return None
            self._raw["bitget"] = parsed.get("data") or []
        for entry in self._raw.get("bitget", []):
            if str(entry.get("symbol", "")).upper() != symbol:
                continue
            size_mult = to_float(entry.get("sizeMultiplier"))
            min_trade = to_float(entry.get("minTradeNum"))
            volume_place = to_float(entry.get("volumePlace"))
            # Bitget USDT-FUTURES: position qty is in base asset units (not contracts).
            # `sizeMultiplier` is the smallest tradable increment; `volumePlace` is decimal digits.
            step = size_mult if size_mult > 0 else (10 ** (-volume_place) if volume_place > 0 else 0.001)
            return LotSpec("bitget", symbol, step, min_trade or step, 1.0)
        return None


# ----------------------------------------------------------------------------
# Position queries (signed net qty per asset) — all 5 exchanges
# ----------------------------------------------------------------------------


def _bitget_sign(secret: str, ts_ms: str, method: str, path_with_q: str, body: str) -> str:
    payload = f"{ts_ms}{method.upper()}{path_with_q}{body}"
    return base64.b64encode(
        hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    ).decode("utf-8")


def fetch_positions_bitget(timeout: int) -> Dict[str, float]:
    key, secret, passphrase = load_required(("BITGET_API_KEY", "BITGET_API_SECRET", "BITGET_API_PASSPHRASE"))
    base = os.environ.get("BITGET_API_BASE", "https://api.bitget.com").rstrip("/")
    path = "/api/v3/position/current-position"
    query = urllib.parse.urlencode({"category": "USDT-FUTURES"})
    path_q = f"{path}?{query}"
    ts = str(now_ms())
    headers = {
        "ACCESS-KEY": key,
        "ACCESS-SIGN": _bitget_sign(secret, ts, "GET", path_q, ""),
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
        "locale": "en-US",
    }
    status, body, _ = http_json_request("GET", f"{base}{path_q}", headers=headers, timeout=timeout)
    if not (200 <= status < 300):
        raise SystemExit(f"[bitget] positions http={status} body={body[:200]}")
    try:
        parsed = json.loads(body)
    except Exception:
        raise SystemExit(f"[bitget] positions non-JSON: {body[:200]}")
    out: Dict[str, float] = {}
    rows = (parsed.get("data") or {}).get("list") or []
    for row in rows:
        symbol = str(row.get("symbol") or "")
        asset = normalize_asset(symbol)
        if not asset:
            continue
        qty = to_float(row.get("available"))
        if qty == 0:
            qty = to_float(row.get("total"))
        if qty == 0:
            continue
        side = str(row.get("posSide") or "").lower()
        signed = qty if side == "long" else -qty if side == "short" else 0.0
        out[asset] = out.get(asset, 0.0) + signed
    return out


def _gate_sign(method: str, prefix_path: str, query: str, body: str, secret: str) -> Dict[str, str]:
    ts = str(int(time.time()))
    body_hash = hashlib.sha512(body.encode("utf-8")).hexdigest()
    payload = f"{method.upper()}\n{prefix_path}\n{query}\n{body_hash}\n{ts}"
    sig = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha512).hexdigest()
    return {"Timestamp": ts, "SIGN": sig}


def fetch_positions_gate(timeout: int) -> Dict[str, float]:
    key, secret = load_required(("GATE_API_KEY", "GATE_API_SECRET"))
    host = os.environ.get("GATE_API_BASE", "https://api.gateio.ws").rstrip("/")
    prefix = "/api/v4"
    path = "/futures/usdt/positions"
    headers = {"Accept": "application/json", "Content-Type": "application/json", "KEY": key}
    headers.update(_gate_sign("GET", f"{prefix}{path}", "", "", secret))
    status, body, _ = http_json_request("GET", f"{host}{prefix}{path}", headers=headers, timeout=timeout)
    if not (200 <= status < 300):
        raise SystemExit(f"[gate] positions http={status} body={body[:200]}")
    try:
        parsed = json.loads(body)
    except Exception:
        raise SystemExit(f"[gate] positions non-JSON: {body[:200]}")
    if not isinstance(parsed, list):
        return {}
    out: Dict[str, float] = {}
    for row in parsed:
        contract = str(row.get("contract") or "")
        asset = normalize_asset(contract)
        if not asset:
            continue
        size = to_float(row.get("size"))  # signed in contracts
        if size == 0:
            continue
        out[asset] = out.get(asset, 0.0) + size
    return out


def fetch_positions_binance(timeout: int) -> Dict[str, float]:
    key, secret = load_required(("BINANCE_API_KEY", "BINANCE_API_SECRET"))
    base = os.environ.get("BINANCE_FAPI_URL", "https://fapi.binance.com").rstrip("/")
    params = {"recvWindow": "5000", "timestamp": str(now_ms())}
    query = urllib.parse.urlencode(sorted(params.items()), safe="-_.~")
    sig = hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    url = f"{base}/fapi/v2/positionRisk?{query}&signature={sig}"
    status, body, _ = http_json_request("GET", url, headers={"X-MBX-APIKEY": key}, timeout=timeout)
    if not (200 <= status < 300):
        raise SystemExit(f"[binance] positions http={status} body={body[:200]}")
    try:
        rows = json.loads(body)
    except Exception:
        raise SystemExit(f"[binance] positions non-JSON: {body[:200]}")
    if not isinstance(rows, list):
        return {}
    out: Dict[str, float] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "")
        asset = normalize_asset(symbol)
        if not asset:
            continue
        amt = to_float(row.get("positionAmt"))
        if amt == 0:
            continue
        out[asset] = out.get(asset, 0.0) + amt
    return out


def fetch_positions_okex(timeout: int) -> Dict[str, float]:
    key, secret, passphrase = load_required(("OKX_API_KEY", "OKX_API_SECRET", "OKX_PASSPHRASE"))
    base = os.environ.get("OKX_BASE_URL", "https://openapi.okx.com").rstrip("/")
    path = "/api/v5/account/positions"
    query = "instType=SWAP"
    url_path = f"{path}?{query}"
    ts = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
    prehash = f"{ts}GET{url_path}"
    sig = base64.b64encode(
        hmac.new(secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()
    ).decode("utf-8")
    headers = {
        "OK-ACCESS-KEY": key,
        "OK-ACCESS-SIGN": sig,
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
    }
    status, body, _ = http_json_request("GET", f"{base}{url_path}", headers=headers, timeout=timeout)
    if not (200 <= status < 300):
        raise SystemExit(f"[okex] positions http={status} body={body[:200]}")
    try:
        parsed = json.loads(body)
    except Exception:
        raise SystemExit(f"[okex] positions non-JSON: {body[:200]}")
    out: Dict[str, float] = {}
    for entry in parsed.get("data", []) or []:
        inst = str(entry.get("instId") or "").upper()
        if not inst.endswith("-SWAP"):
            continue
        asset = normalize_asset(inst)
        if not asset:
            continue
        qty = to_float(entry.get("pos"))  # signed in contracts (net mode) or +/-
        if qty == 0:
            continue
        out[asset] = out.get(asset, 0.0) + qty
    return out


def fetch_positions_bybit(timeout: int) -> Dict[str, float]:
    key, secret = load_required(("BYBIT_API_KEY", "BYBIT_API_SECRET"))
    base = os.environ.get("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
    path = "/v5/position/list"
    out: Dict[str, float] = {}
    cursor = ""
    for _ in range(10):
        params = {"category": "linear", "settleCoin": "USDT", "limit": "200"}
        if cursor:
            params["cursor"] = cursor
        query = urllib.parse.urlencode(params)
        ts = str(now_ms())
        recv_window = "5000"
        prehash = f"{ts}{key}{recv_window}{query}"
        sig = hmac.new(secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).hexdigest()
        headers = {
            "X-BAPI-API-KEY": key,
            "X-BAPI-SIGN": sig,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": recv_window,
        }
        status, body, _ = http_json_request("GET", f"{base}{path}?{query}", headers=headers, timeout=timeout)
        if not (200 <= status < 300):
            raise SystemExit(f"[bybit] positions http={status} body={body[:200]}")
        try:
            parsed = json.loads(body)
        except Exception:
            raise SystemExit(f"[bybit] positions non-JSON: {body[:200]}")
        result = parsed.get("result") or {}
        for row in result.get("list") or []:
            symbol = str(row.get("symbol") or "")
            asset = normalize_asset(symbol)
            if not asset:
                continue
            qty = to_float(row.get("size"))
            if qty == 0:
                continue
            side = str(row.get("side") or "").lower()
            signed = qty if side == "buy" else -qty if side == "sell" else 0.0
            out[asset] = out.get(asset, 0.0) + signed
        cursor = str(result.get("nextPageCursor") or "")
        if not cursor:
            break
    return out


def fetch_positions(exchange: str, timeout: int) -> Dict[str, float]:
    if exchange == "bitget":
        return fetch_positions_bitget(timeout)
    if exchange == "gate":
        return fetch_positions_gate(timeout)
    if exchange == "binance":
        return fetch_positions_binance(timeout)
    if exchange == "okex":
        return fetch_positions_okex(timeout)
    if exchange == "bybit":
        return fetch_positions_bybit(timeout)
    raise SystemExit(f"unsupported exchange: {exchange}")


# ----------------------------------------------------------------------------
# Reduce-only market order placement (bitget + gate only)
# ----------------------------------------------------------------------------


def submit_reduce_bitget(symbol: str, side: str, qty: float, timeout: int) -> Tuple[bool, str]:
    """side: 'sell' (reduce long) or 'buy' (reduce short)."""
    key, secret, passphrase = load_required(("BITGET_API_KEY", "BITGET_API_SECRET", "BITGET_API_PASSPHRASE"))
    base = os.environ.get("BITGET_API_BASE", "https://api.bitget.com").rstrip("/")
    path = "/api/v3/trade/place-order"
    payload = {
        "category": "USDT-FUTURES",
        "symbol": symbol,
        "orderType": "market",
        "qty": format_decimal(qty),
        "side": side,
        "reduceOnly": "YES",
        "clientOid": f"align{now_ms()}",
    }
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
    ts = str(now_ms())
    headers = {
        "ACCESS-KEY": key,
        "ACCESS-SIGN": _bitget_sign(secret, ts, "POST", path, body),
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
        "locale": "en-US",
    }
    status, resp_body, _ = http_json_request("POST", f"{base}{path}", headers=headers, body=body, timeout=timeout)
    ok = False
    brief = resp_body[:200]
    try:
        data = json.loads(resp_body)
        ok = (200 <= status < 300) and str(data.get("code", "")) in ("00000", "0")
        if not ok:
            brief = f"code={data.get('code')} msg={data.get('msg')}"
        else:
            brief = json.dumps(data.get("data"), ensure_ascii=True)
    except Exception:
        pass
    return ok, brief


def submit_reduce_gate(contract: str, signed_size: float, timeout: int) -> Tuple[bool, str]:
    """signed_size: negative to reduce a long, positive to reduce a short (Gate convention)."""
    key, secret = load_required(("GATE_API_KEY", "GATE_API_SECRET"))
    host = os.environ.get("GATE_API_BASE", "https://api.gateio.ws").rstrip("/")
    prefix = "/api/v4"
    path = "/futures/usdt/orders"
    payload = {
        "contract": contract,
        "size": int(signed_size) if signed_size == int(signed_size) else signed_size,
        "price": "0",
        "tif": "ioc",
        "reduce_only": True,
        "text": f"t-align{now_ms()}",
    }
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
    headers = {"Accept": "application/json", "Content-Type": "application/json", "KEY": key}
    headers.update(_gate_sign("POST", f"{prefix}{path}", "", body, secret))
    status, resp_body, _ = http_json_request("POST", f"{host}{prefix}{path}", headers=headers, body=body, timeout=timeout)
    ok = 200 <= status < 300
    brief = resp_body[:300]
    return ok, brief


# ----------------------------------------------------------------------------
# Alignment flow
# ----------------------------------------------------------------------------


@dataclass
class AlignPlan:
    asset: str
    open_qty: float        # asset units
    hedge_qty: float       # asset units
    delta: float           # asset units
    reduce_ex: str
    reduce_native_symbol: str
    raw_reduce_qty: float  # asset units, |delta| capped to reducer leg
    rounded_qty: float     # exchange-native qty after lot-size floor
    side: str              # 'sell' or 'buy' (reduce-only)
    skip_reason: str = ""


def native_to_asset_qty(
    exchange: str,
    asset: str,
    native_qty: float,
    lot_cache: "LotSpecCache",
    timeout: int,
) -> float:
    """Convert an exchange's position qty (native units) to base-asset units.

    binance/bybit/bitget already report positions in base asset; gate/okex
    report in contracts and must be multiplied by contract_size.
    """
    if native_qty == 0:
        return 0.0
    if exchange in ("binance", "bybit", "bitget"):
        return native_qty
    native_symbol = symbol_for_exchange(exchange, asset)
    spec = lot_cache.get(exchange, native_symbol, timeout)
    if spec is None or spec.contract_size <= 0:
        return native_qty
    return native_qty * spec.contract_size


def pick_reduce_side(ctx: CrossContext, open_qty: float, hedge_qty: float, mode: str) -> Tuple[str, float, str]:
    mode = (mode or "larger").strip().lower()
    if mode == "open":
        return ctx.open_ex, open_qty, "open"
    if mode == "hedge":
        return ctx.hedge_ex, hedge_qty, "hedge"
    if abs(open_qty) >= abs(hedge_qty):
        return ctx.open_ex, open_qty, "open"
    return ctx.hedge_ex, hedge_qty, "hedge"


def _make_plan(
    *,
    asset: str,
    open_qty: float,
    hedge_qty: float,
    delta: float,
    reduce_ex: str,
    leg_qty: float,
    reduce_asset_qty: float,
    lot_cache: LotSpecCache,
    timeout: int,
) -> AlignPlan:
    order_side = "sell" if leg_qty > 0 else "buy"
    native_symbol = symbol_for_exchange(reduce_ex, asset)
    spec = lot_cache.get(reduce_ex, native_symbol, timeout)
    if spec is None:
        return AlignPlan(asset, open_qty, hedge_qty, delta, reduce_ex, native_symbol,
                         reduce_asset_qty, 0.0, order_side,
                         skip_reason=f"no lot spec for {reduce_ex}:{native_symbol}")
    exch_qty_raw = asset_qty_to_exchange_qty(reduce_asset_qty, spec)
    exch_qty = floor_to_step(exch_qty_raw, spec.qty_step)
    if exch_qty < spec.min_qty or exch_qty <= 0:
        return AlignPlan(asset, open_qty, hedge_qty, delta, reduce_ex, native_symbol,
                         reduce_asset_qty, exch_qty, order_side,
                         skip_reason=f"qty {exch_qty} < minQty {spec.min_qty}")
    return AlignPlan(asset, open_qty, hedge_qty, delta, reduce_ex, native_symbol,
                     reduce_asset_qty, exch_qty, order_side)


def build_plan(
    ctx: CrossContext,
    assets: List[str],
    open_pos: Dict[str, float],
    hedge_pos: Dict[str, float],
    *,
    mode: str,
    lot_cache: LotSpecCache,
    reduce_side: str,
    min_net_qty: float,
    timeout: int,
) -> List[AlignPlan]:
    plans: List[AlignPlan] = []
    for asset in assets:
        open_native = open_pos.get(asset, 0.0)
        hedge_native = hedge_pos.get(asset, 0.0)
        open_qty = native_to_asset_qty(ctx.open_ex, asset, open_native, lot_cache, timeout)
        hedge_qty = native_to_asset_qty(ctx.hedge_ex, asset, hedge_native, lot_cache, timeout)
        delta = open_qty + hedge_qty

        if mode == "clear":
            # Flatten each leg to 0 individually.
            for ex, qty in ((ctx.open_ex, open_qty), (ctx.hedge_ex, hedge_qty)):
                if abs(qty) <= max(min_net_qty, 0.0):
                    continue
                plans.append(_make_plan(
                    asset=asset,
                    open_qty=open_qty,
                    hedge_qty=hedge_qty,
                    delta=delta,
                    reduce_ex=ex,
                    leg_qty=qty,
                    reduce_asset_qty=abs(qty),
                    lot_cache=lot_cache,
                    timeout=timeout,
                ))
            continue

        # mode == "align": shrink the larger leg by |delta|.
        if abs(delta) <= max(min_net_qty, 0.0) or delta == 0.0:
            continue
        reduce_ex, reduce_leg_qty, _which = pick_reduce_side(ctx, open_qty, hedge_qty, reduce_side)
        if reduce_leg_qty == 0:
            other_ex = ctx.hedge_ex if reduce_ex == ctx.open_ex else ctx.open_ex
            plans.append(AlignPlan(asset, open_qty, hedge_qty, delta, reduce_ex, "", 0.0, 0.0, "",
                                   skip_reason=f"reduce side has zero position (other leg = {other_ex})"))
            continue
        raw_reduce_asset_qty = min(abs(delta), abs(reduce_leg_qty))
        plans.append(_make_plan(
            asset=asset,
            open_qty=open_qty,
            hedge_qty=hedge_qty,
            delta=delta,
            reduce_ex=reduce_ex,
            leg_qty=reduce_leg_qty,
            reduce_asset_qty=raw_reduce_asset_qty,
            lot_cache=lot_cache,
            timeout=timeout,
        ))
    return plans


def print_plan_table(plans: List[AlignPlan], *, mode: str = "align") -> None:
    if not plans:
        if mode == "clear":
            print("[info] no positions to clear on either leg.")
        else:
            print("[info] no alignment actions (all deltas below threshold).")
        return
    header = f"{'asset':<8} {'open':>14} {'hedge':>14} {'delta':>14} {'reduce_ex':<8} {'symbol':<18} {'side':<5} {'qty':>14}  note"
    print(header)
    print("-" * len(header))
    for p in plans:
        note = p.skip_reason or "ok"
        print(
            f"{p.asset:<8} {p.open_qty:>14.6f} {p.hedge_qty:>14.6f} {p.delta:>14.6f} "
            f"{p.reduce_ex:<8} {p.reduce_native_symbol:<18} {p.side:<5} {p.rounded_qty:>14.6f}  {note}"
        )


def execute_plans(plans: List[AlignPlan], *, sleep: float, timeout: int) -> int:
    failures = 0
    for plan in plans:
        if plan.skip_reason:
            continue
        if plan.reduce_ex not in PLACE_ORDER_SUPPORTED:
            print(f"[skip] {plan.asset}: --execute on {plan.reduce_ex} not implemented in this iteration")
            failures += 1
            continue
        if plan.reduce_ex == "bitget":
            ok, brief = submit_reduce_bitget(plan.reduce_native_symbol, plan.side, plan.rounded_qty, timeout)
        elif plan.reduce_ex == "gate":
            # Gate uses signed size on a single endpoint.
            signed = -plan.rounded_qty if plan.side == "sell" else plan.rounded_qty
            ok, brief = submit_reduce_gate(plan.reduce_native_symbol, signed, timeout)
        else:
            ok, brief = False, "unreachable"
        tag = "OK" if ok else "ERR"
        print(f"[{tag}] {plan.reduce_ex} {plan.reduce_native_symbol} {plan.side} qty={plan.rounded_qty}: {brief}")
        if not ok:
            failures += 1
        time.sleep(sleep)
    return failures


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Align both legs of a cross arb env so open_net + hedge_net ≈ 0 per symbol.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--env-name", default="", help="Env name, e.g. bitget-gate-cross-arb01.")
    parser.add_argument("--env-dir", default="", help="Env directory containing env.sh.")
    parser.add_argument("--open-venue", default="", help="Override open venue (e.g. bitget-futures).")
    parser.add_argument("--hedge-venue", default="", help="Override hedge venue (e.g. gate-futures).")
    parser.add_argument("--symbol", action="append", default=[], help="Asset/symbol filter; repeatable or CSV.")
    parser.add_argument(
        "--mode",
        choices=["align", "clear"],
        default="align",
        help="align: shrink larger leg by |delta|. clear: reduce both legs to 0.",
    )
    parser.add_argument(
        "--reduce-side",
        choices=["larger", "open", "hedge"],
        default="larger",
        help="(align only) which leg to reduce on. 'larger' picks the side with bigger |qty|.",
    )
    parser.add_argument("--min-net-qty", type=float, default=0.0, help="Skip if |delta| below this asset-qty threshold.")
    parser.add_argument("--redis-host", default="")
    parser.add_argument("--redis-port", type=int, default=None)
    parser.add_argument("--redis-db", type=int, default=None)
    parser.add_argument("--redis-password", default=None)
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--sleep", type=float, default=0.12)
    parser.add_argument("--no-env-sh", action="store_true", help="Do not auto-source env.sh.")
    parser.add_argument(
        "--dry-run-skip",
        action="store_true",
        help="Short-circuit: print plan only, do not even query positions.",
    )
    parser.add_argument("--execute", action="store_true", help="Actually place reduce-only market orders.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ctx, env_dir = resolve_context(args)

    assets = parse_symbol_args(args.symbol)
    if assets:
        print(f"[info] using CLI symbols: {len(assets)}")
    else:
        rds = redis_client(args)
        assets = load_online_assets(rds, ctx)
    if not assets:
        raise SystemExit("no symbols selected; pass --symbol or check redis online lists")

    print(
        f"[info] env={ctx.env_name} open={ctx.open_venue} hedge={ctx.hedge_venue} "
        f"key_suffix={ctx.key_suffix} assets={len(assets)} mode={args.mode} "
        f"reduce_side={args.reduce_side} execute={args.execute}"
    )

    if args.dry_run_skip:
        print("[info] --dry-run-skip set; not querying positions. Planned assets:")
        for asset in assets:
            if args.mode == "clear":
                print(f"  {asset}: would reduce-only both legs to 0")
            else:
                print(f"  {asset}: would compute delta and reduce-only on {args.reduce_side} side")
        return 0

    print(f"[info] fetching positions: open={ctx.open_ex} hedge={ctx.hedge_ex}")
    open_pos = fetch_positions(ctx.open_ex, args.timeout)
    hedge_pos = fetch_positions(ctx.hedge_ex, args.timeout)
    print(f"[info] positions: open_assets={len(open_pos)} hedge_assets={len(hedge_pos)}")

    lot_cache = LotSpecCache()
    plans = build_plan(
        ctx,
        assets,
        open_pos,
        hedge_pos,
        mode=args.mode,
        lot_cache=lot_cache,
        reduce_side=args.reduce_side,
        min_net_qty=max(args.min_net_qty, 0.0),
        timeout=args.timeout,
    )
    print_plan_table(plans, mode=args.mode)

    if not args.execute:
        print("\nDry-run only. Re-run with --execute to submit reduce-only market orders.")
        return 0

    actionable = [p for p in plans if not p.skip_reason]
    if not actionable:
        print("[info] no actionable plans to execute.")
        return 0

    unsupported = [p for p in actionable if p.reduce_ex not in PLACE_ORDER_SUPPORTED]
    if unsupported:
        print(
            f"[warn] {len(unsupported)} plan(s) target exchanges not yet supported for --execute: "
            f"{sorted({p.reduce_ex for p in unsupported})}",
            file=sys.stderr,
        )

    failures = execute_plans(actionable, sleep=args.sleep, timeout=args.timeout)
    if failures:
        print(f"WARN: {failures} alignment submission(s) failed/skipped", file=sys.stderr)
        return 1
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
