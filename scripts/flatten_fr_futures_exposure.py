#!/usr/bin/env python3
"""Flatten FR futures exposure by trading the uniform futures leg.

Reads the pre-trade exposure snapshot from the FR viz dashboard, finds symbols
with non-zero net_qty, and submits market orders on the futures leg to flatten
that exposure.

Supported exchanges:
  - binance: Binance uniform UM order via PAPI
  - okex:    OKX SWAP order with tdMode=cross
  - gate:    Gate USDT futures order with account=unified

Usage:
  # env.sh already sourced
  python scripts/flatten_fr_futures_exposure.py --exchange okex --suffix trade
  python scripts/flatten_fr_futures_exposure.py --exchange gate --suffix trade --execute
  python scripts/flatten_fr_futures_exposure.py --exchange binance --suffix trade01 --execute
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
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple


ZERO = Decimal("0")
SKIP_ASSETS = {"BNB"}

DASHBOARD_HOST = "127.0.0.1"
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "4191"))
OKEX_REMOTE_HOST = "47.238.128.48"
OKEX_REMOTE_SSH_TARGET = "fanghaizhou@47.238.128.48"

BINANCE_FAPI_BASE = "https://fapi.binance.com"
BINANCE_PAPI_BASE = "https://papi.binance.com"
BINANCE_EXCHANGE_INFO_URL = f"{BINANCE_FAPI_BASE}/fapi/v1/exchangeInfo"

OKX_BASE_URL = os.environ.get("OKX_BASE_URL", "https://www.okx.com")
OKX_INSTRUMENTS_URL = f"{OKX_BASE_URL}/api/v5/public/instruments?instType=SWAP"

GATE_HOST = "https://api.gateio.ws"
GATE_PREFIX = "/api/v4"
GATE_CONTRACTS_URL = f"{GATE_HOST}{GATE_PREFIX}/futures/usdt/contracts"


@dataclass
class ExposureRow:
    asset: str
    net_qty: Decimal
    net_usdt: Decimal
    open_qty: Decimal
    hedge_qty: Decimal


@dataclass
class PlannedOrder:
    asset: str
    symbol: str
    side: str
    net_qty: Decimal
    net_usdt: Decimal
    order_qty: Decimal
    order_unit: str
    request_body: Dict[str, Any]
    step_size: Decimal = ZERO
    min_qty: Decimal = ZERO


@dataclass
class BinanceSpec:
    symbol: str
    step_size: Decimal
    min_qty: Decimal


@dataclass
class OkxSpec:
    inst_id: str
    contract_size: Decimal
    lot_sz: Decimal
    min_sz: Decimal


@dataclass
class GateSpec:
    contract: str
    contract_size: Decimal
    step_contracts: Decimal
    min_contracts: Decimal


@dataclass
class GateChildOrder:
    qty: Decimal
    est_usdt: Decimal
    request_body: Dict[str, Any]


def normalize_exchange(raw: str) -> str:
    value = (raw or "").strip().lower()
    if value == "okx":
        return "okex"
    return value


def decimal_from(value: Any, field: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise SystemExit(f"invalid decimal for {field}: {value!r}") from exc


def decimal_or(value: Any, default: str) -> Decimal:
    if value in (None, ""):
        return Decimal(default)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal(default)


def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def format_decimal(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def now_ms() -> int:
    return int(time.time() * 1000)


def utc_timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def http_request(
    url: str,
    *,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    data: Optional[bytes] = None,
    timeout: int = 15,
) -> Tuple[int, str, Dict[str, str]]:
    req = urllib.request.Request(url, data=data, method=method.upper())
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
            return status, body, dict(resp.headers.items())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        hdrs = dict(getattr(exc, "headers", {}).items()) if getattr(exc, "headers", None) else {}
        return exc.code, body, hdrs
    except Exception as exc:
        return 0, str(exc), {}


def load_json_response(
    url: str, *, timeout: int = 15, headers: Optional[Dict[str, str]] = None
) -> Any:
    status, body, _headers = http_request(url, timeout=timeout, headers=headers)
    if not (200 <= status < 300):
        raise SystemExit(f"request failed: url={url} status={status} body={body}")
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid json from {url}: {exc}") from exc


def fetch_snapshot(snapshot_url: str) -> List[ExposureRow]:
    data = load_json_response(snapshot_url, timeout=10)
    rows: List[ExposureRow] = []
    for entry in data.get("entries", []):
        if entry.get("channel") != "pre_trade_exposure":
            continue
        for row in entry.get("entry", {}).get("rows", []):
            if row.get("is_total"):
                continue
            asset = str(row.get("asset", "")).strip().upper()
            if not asset:
                continue
            rows.append(
                ExposureRow(
                    asset=asset,
                    net_qty=decimal_or(row.get("net_qty"), "0"),
                    net_usdt=decimal_or(row.get("net_usdt"), "0"),
                    open_qty=decimal_or(row.get("open_qty"), "0"),
                    hedge_qty=decimal_or(row.get("hedge_qty"), "0"),
                )
            )
    return rows


def binance_sign(query: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def binance_signed_request(
    path: str,
    params: Dict[str, Any],
    api_key: str,
    api_secret: str,
    *,
    method: str = "POST",
    timeout: int = 10,
) -> Tuple[int, str, Dict[str, str]]:
    payload = dict(params)
    payload.setdefault("recvWindow", "5000")
    payload["timestamp"] = str(now_ms())
    query = urllib.parse.urlencode(sorted(payload.items(), key=lambda kv: kv[0]), safe="-_.~")
    signature = binance_sign(query, api_secret)
    url = f"{BINANCE_PAPI_BASE}{path}?{query}&signature={signature}"
    return http_request(
        url,
        method=method,
        headers={"X-MBX-APIKEY": api_key},
        timeout=timeout,
    )


def okx_sign(timestamp: str, method: str, request_path: str, body: str, secret: str) -> str:
    payload = f"{timestamp}{method.upper()}{request_path}{body}"
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def okx_private_request(
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
) -> Tuple[int, str, Dict[str, str]]:
    method = method.upper()
    params = params or {}
    query = urllib.parse.urlencode(params) if params else ""
    request_path = f"{path}?{query}" if query else path
    body_text = "" if method == "GET" else json.dumps(body or {}, ensure_ascii=False, separators=(",", ":"))
    timestamp = utc_timestamp()
    signature = okx_sign(timestamp, method, request_path, body_text, api_secret)
    return http_request(
        f"{OKX_BASE_URL.rstrip('/')}{request_path}",
        method=method,
        headers={
            "OK-ACCESS-KEY": api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": passphrase,
            "Content-Type": "application/json",
        },
        data=None if method == "GET" else body_text.encode("utf-8"),
        timeout=timeout,
    )


def okx_response_ok(body_text: str) -> Tuple[bool, str]:
    try:
        parsed = json.loads(body_text)
    except json.JSONDecodeError:
        return False, "non-json body"
    code = str(parsed.get("code", "")).strip()
    msg = str(parsed.get("msg", "")).strip()
    if code == "0":
        return True, ""
    brief = f"code={code} msg={msg}".strip()
    data = parsed.get("data")
    if isinstance(data, list) and data and isinstance(data[0], dict):
        s_code = str(data[0].get("sCode", "")).strip()
        s_msg = str(data[0].get("sMsg", "")).strip()
        if s_code or s_msg:
            brief = f"{brief} sCode={s_code} sMsg={s_msg}".strip()
    return False, brief


def gate_sign(method: str, path: str, query: str, body: str, secret: str, timestamp: str) -> str:
    hashed_body = hashlib.sha512(body.encode("utf-8")).hexdigest()
    sign_string = f"{method}\n{path}\n{query}\n{hashed_body}\n{timestamp}"
    return hmac.new(secret.encode("utf-8"), sign_string.encode("utf-8"), hashlib.sha512).hexdigest()


def gate_private_request(
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
) -> Tuple[int, str, Dict[str, str]]:
    method = method.upper()
    params = params or {}
    query = urllib.parse.urlencode(sorted(params.items(), key=lambda kv: kv[0]), doseq=True)
    body_text = "" if body is None else json.dumps(body, ensure_ascii=False, separators=(",", ":"))
    ts = str(int(time.time()))
    request_path = f"{GATE_PREFIX}{path}"
    signature = gate_sign(method, request_path, query, body_text, api_secret, ts)
    url = f"{GATE_HOST}{request_path}"
    if query:
        url = f"{url}?{query}"
    return http_request(
        url,
        method=method,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "KEY": api_key,
            "Timestamp": ts,
            "SIGN": signature,
        },
        data=None if method == "GET" else body_text.encode("utf-8"),
        timeout=timeout,
    )


def gate_fetch_open_orders(
    api_key: str,
    api_secret: str,
    *,
    settle: str,
    contract: str,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    orders: List[Dict[str, Any]] = []
    page = 1
    while True:
        status, body, _headers = gate_private_request(
            "GET",
            f"/futures/{settle}/orders",
            api_key,
            api_secret,
            params={
                "status": "open",
                "page": page,
                "limit": limit,
                "contract": contract,
            },
        )
        if status != 200:
            raise SystemExit(
                f"failed to fetch Gate open orders for {contract}: status={status} body={body}"
            )
        try:
            data = json.loads(body)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"invalid Gate open-orders response for {contract}: {body}") from exc
        if not isinstance(data, list):
            raise SystemExit(f"unexpected Gate open-orders response for {contract}: {data}")
        if not data:
            break
        orders.extend(data)
        if len(data) < limit:
            break
        page += 1
    return orders


def gate_cancel_open_orders(
    api_key: str,
    api_secret: str,
    *,
    settle: str,
    contract: str,
) -> int:
    open_orders = gate_fetch_open_orders(
        api_key,
        api_secret,
        settle=settle,
        contract=contract,
    )
    if not open_orders:
        print(f"[cancel] {contract}: no open orders")
        return 0

    print(f"[cancel] {contract}: canceling {len(open_orders)} open order(s)")
    canceled = 0
    for order in open_orders:
        order_id = str(order.get("id") or order.get("order_id") or "").strip()
        if not order_id:
            print(f"  [skip] {contract}: missing order id in {order}")
            continue
        status, body, _headers = gate_private_request(
            "DELETE",
            f"/futures/{settle}/orders/{order_id}",
            api_key,
            api_secret,
            params={"contract": contract},
        )
        tag = "OK" if status == 200 else "ERR"
        print(f"  [{tag}] cancel order_id={order_id} status={status}")
        try:
            print(f"  {json.dumps(json.loads(body), ensure_ascii=False)}")
        except json.JSONDecodeError:
            print(f"  {body}")
        if status == 200:
            canceled += 1
    return canceled


def dump_shell_env_from_file(path: str) -> Dict[str, str]:
    cmd = [
        "bash",
        "-lc",
        f"set -a; source {shlex.quote(path)} >/dev/null 2>&1; env -0",
    ]
    proc = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        stderr = proc.stderr.decode("utf-8", errors="replace").strip()
        raise SystemExit(f"failed to source env file {path}: {stderr or f'exit={proc.returncode}'}")
    return parse_env_dump(proc.stdout)


def dump_shell_env_over_ssh(target: str, path: str) -> Dict[str, str]:
    inner = f"set -a; source {shlex.quote(path)} >/dev/null 2>&1; env -0"
    remote_cmd = f"bash -lc {shlex.quote(inner)}"
    proc = subprocess.run(
        ["ssh", target, remote_cmd],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        stderr = proc.stderr.decode("utf-8", errors="replace").strip()
        raise SystemExit(
            f"failed to source remote env {target}:{path}: {stderr or f'exit={proc.returncode}'}"
        )
    return parse_env_dump(proc.stdout)


def parse_env_dump(raw: bytes) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for item in raw.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key, value = item.split(b"=", 1)
        result[key.decode("utf-8", errors="ignore")] = value.decode("utf-8", errors="replace")
    return result


def load_env_into_process(env_map: Dict[str, str]) -> None:
    for key, value in env_map.items():
        os.environ[key] = value


def parse_env_ssh(raw: str) -> Tuple[str, str]:
    value = (raw or "").strip()
    if ":" not in value:
        raise SystemExit("--env-ssh must look like user@host:/abs/path/env.sh")
    target, path = value.split(":", 1)
    target = target.strip()
    path = path.strip()
    if not target or not path:
        raise SystemExit("--env-ssh must look like user@host:/abs/path/env.sh")
    return target, path


def default_env_name(exchange: str, suffix: Optional[str]) -> str:
    if not suffix:
        raise SystemExit("either --suffix, --env-name, or --snapshot-url is required")
    return f"{exchange}_fr_{suffix.strip().lower()}"


def default_dashboard_host(exchange: str) -> str:
    if exchange == "okex":
        return OKEX_REMOTE_HOST
    return DASHBOARD_HOST


def default_env_ssh(exchange: str, env_name: str) -> Optional[str]:
    if exchange != "okex":
        return None
    return f"{OKEX_REMOTE_SSH_TARGET}:/home/fanghaizhou/{env_name}/env.sh"


def load_binance_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        raise SystemExit("missing BINANCE_API_KEY / BINANCE_API_SECRET")
    return api_key, api_secret


def load_okx_credentials() -> Tuple[str, str, str]:
    api_key = os.environ.get("OKX_API_KEY", "").strip()
    api_secret = os.environ.get("OKX_API_SECRET", "").strip()
    passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()
    if not api_key or not api_secret or not passphrase:
        raise SystemExit("missing OKX_API_KEY / OKX_API_SECRET / OKX_PASSPHRASE")
    return api_key, api_secret, passphrase


def load_gate_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("GATE_API_KEY", "").strip()
    api_secret = os.environ.get("GATE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        raise SystemExit("missing GATE_API_KEY / GATE_API_SECRET")
    return api_key, api_secret


def fetch_binance_specs() -> Dict[str, BinanceSpec]:
    data = load_json_response(BINANCE_EXCHANGE_INFO_URL, timeout=15)
    specs: Dict[str, BinanceSpec] = {}
    for item in data.get("symbols", []):
        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol.endswith("USDT"):
            continue
        step_size = Decimal("1")
        min_qty = ZERO
        for flt in item.get("filters", []):
            if flt.get("filterType") == "LOT_SIZE":
                step_size = decimal_or(flt.get("stepSize"), "1")
                min_qty = decimal_or(flt.get("minQty"), "0")
                break
        specs[symbol] = BinanceSpec(symbol=symbol, step_size=step_size, min_qty=min_qty)
    return specs


def fetch_okx_specs() -> Dict[str, OkxSpec]:
    data = load_json_response(OKX_INSTRUMENTS_URL, timeout=15)
    if str(data.get("code", "")) != "0":
        raise SystemExit(f"failed to fetch OKX instruments: {json.dumps(data, ensure_ascii=False)}")
    specs: Dict[str, OkxSpec] = {}
    for item in data.get("data", []):
        if item.get("ctType") != "linear" or item.get("settleCcy") != "USDT":
            continue
        inst_id = str(item.get("instId", "")).strip().upper()
        ct_val = decimal_or(item.get("ctVal"), "1")
        ct_mult = decimal_or(item.get("ctMult"), "1")
        contract_size = ct_val * ct_mult
        symbol = inst_id.replace("-SWAP", "").replace("-", "")
        specs[symbol] = OkxSpec(
            inst_id=inst_id,
            contract_size=contract_size,
            lot_sz=decimal_or(item.get("lotSz"), "1"),
            min_sz=decimal_or(item.get("minSz"), "0"),
        )
    return specs


def fetch_gate_specs() -> Dict[str, GateSpec]:
    data = load_json_response(
        GATE_CONTRACTS_URL,
        timeout=15,
        headers={"X-Gate-Size-Decimal": "1"},
    )
    if not isinstance(data, list):
        raise SystemExit(f"failed to fetch Gate contracts: {data}")
    specs: Dict[str, GateSpec] = {}
    for item in data:
        contract = str(item.get("name", "")).strip().upper()
        if not contract.endswith("_USDT"):
            continue
        symbol = contract.replace("_", "")
        min_contracts = decimal_or(item.get("order_size_min"), "0")
        step_contracts = decimal_or(item.get("order_size_step"), "0")
        if step_contracts <= 0 and bool(item.get("enable_decimal")) and ZERO < min_contracts < Decimal("1"):
            step_contracts = min_contracts
        if step_contracts <= 0:
            step_contracts = Decimal("1")
        specs[symbol] = GateSpec(
            contract=contract,
            contract_size=decimal_or(item.get("quanto_multiplier"), "1"),
            step_contracts=step_contracts,
            min_contracts=min_contracts,
        )
    return specs


def build_binance_orders(rows: List[ExposureRow]) -> Tuple[List[PlannedOrder], List[str]]:
    specs = fetch_binance_specs()
    orders: List[PlannedOrder] = []
    skipped: List[str] = []
    for row in rows:
        symbol = f"{row.asset}USDT"
        spec = specs.get(symbol)
        if spec is None:
            skipped.append(f"{row.asset}: symbol {symbol} not found in Binance exchangeInfo")
            continue
        raw_qty = abs(row.net_qty)
        order_qty = floor_to_step(raw_qty, spec.step_size)
        if order_qty < spec.min_qty:
            skipped.append(
                f"{row.asset}: aligned qty {format_decimal(order_qty)} < minQty {format_decimal(spec.min_qty)}"
            )
            continue
        side = "SELL" if row.net_qty > 0 else "BUY"
        orders.append(
            PlannedOrder(
                asset=row.asset,
                symbol=symbol,
                side=side,
                net_qty=row.net_qty,
                net_usdt=row.net_usdt,
                order_qty=order_qty,
                order_unit="base",
                request_body={
                    "symbol": symbol,
                    "side": side,
                    "type": "MARKET",
                    "quantity": format_decimal(order_qty),
                    "reduceOnly": "false",
                },
            )
        )
    return orders, skipped


def build_okx_orders(rows: List[ExposureRow]) -> Tuple[List[PlannedOrder], List[str]]:
    specs = fetch_okx_specs()
    orders: List[PlannedOrder] = []
    skipped: List[str] = []
    for row in rows:
        symbol = f"{row.asset}USDT"
        spec = specs.get(symbol)
        if spec is None:
            skipped.append(f"{row.asset}: symbol {symbol} not found in OKX instruments")
            continue
        if spec.contract_size <= 0:
            skipped.append(f"{row.asset}: invalid OKX contract_size for {spec.inst_id}")
            continue
        raw_contracts = abs(row.net_qty) / spec.contract_size
        order_qty = floor_to_step(raw_contracts, spec.lot_sz)
        if order_qty < spec.min_sz:
            skipped.append(
                f"{row.asset}: aligned contracts {format_decimal(order_qty)} < minSz {format_decimal(spec.min_sz)}"
            )
            continue
        side = "SELL" if row.net_qty > 0 else "BUY"
        orders.append(
            PlannedOrder(
                asset=row.asset,
                symbol=spec.inst_id,
                side=side,
                net_qty=row.net_qty,
                net_usdt=row.net_usdt,
                order_qty=order_qty,
                order_unit="contracts",
                request_body={
                    "instId": spec.inst_id,
                    "tdMode": "cross",
                    "side": side.lower(),
                    "ordType": "market",
                    "sz": format_decimal(order_qty),
                    "reduceOnly": False,
                },
            )
        )
    return orders, skipped


def build_gate_orders(rows: List[ExposureRow]) -> Tuple[List[PlannedOrder], List[str]]:
    specs = fetch_gate_specs()
    orders: List[PlannedOrder] = []
    skipped: List[str] = []
    now_tag = now_ms()
    for idx, row in enumerate(rows):
        symbol = f"{row.asset}USDT"
        spec = specs.get(symbol)
        if spec is None:
            skipped.append(f"{row.asset}: symbol {symbol} not found in Gate contracts")
            continue
        if spec.contract_size <= 0:
            skipped.append(f"{row.asset}: invalid Gate contract_size for {spec.contract}")
            continue
        raw_contracts = abs(row.net_qty) / spec.contract_size
        order_qty = floor_to_step(raw_contracts, spec.step_contracts)
        if order_qty < spec.min_contracts:
            skipped.append(
                f"{row.asset}: aligned contracts {format_decimal(order_qty)} < minContracts {format_decimal(spec.min_contracts)}"
            )
            continue
        side = "SELL" if row.net_qty > 0 else "BUY"
        signed_size = -order_qty if side == "SELL" else order_qty
        orders.append(
            PlannedOrder(
                asset=row.asset,
                symbol=spec.contract,
                side=side,
                net_qty=row.net_qty,
                net_usdt=row.net_usdt,
                order_qty=order_qty,
                order_unit="contracts",
                request_body={
                    "text": f"t-frflat-{now_tag}-{idx}",
                    "contract": spec.contract,
                    "size": format_decimal(signed_size),
                    "price": "0",
                    "tif": "ioc",
                    "account": "unified",
                },
                step_size=spec.step_contracts,
                min_qty=spec.min_contracts,
            )
        )
    return orders, skipped


def print_plan(orders: List[PlannedOrder], skipped: List[str]) -> None:
    if skipped:
        print("[skip]")
        for item in skipped:
            print(f"  - {item}")
        print()

    if not orders:
        print("No valid orders to submit.")
        return

    print(f"{'Asset':<10} {'Net Qty':>14} {'Net USDT':>12} {'Action':>8} {'Order Qty':>14} {'Unit':>10} {'Symbol':>18}")
    print("-" * 96)
    for order in orders:
        print(
            f"{order.asset:<10} "
            f"{format_decimal(order.net_qty):>14} "
            f"{format_decimal(order.net_usdt):>12} "
            f"{order.side:>8} "
            f"{format_decimal(order.order_qty):>14} "
            f"{order.order_unit:>10} "
            f"{order.symbol:>18}"
        )
    print("-" * 96)
    print(f"Total: {len(orders)} orders")


def submit_binance_orders(orders: List[PlannedOrder]) -> Tuple[int, int]:
    api_key, api_secret = load_binance_credentials()
    failures = 0
    total_requests = 0
    for order in orders:
        total_requests += 1
        print(f"\n[order] {order.symbol} {order.side} qty={order.request_body['quantity']}")
        status, body, headers = binance_signed_request(
            "/papi/v1/um/order",
            order.request_body,
            api_key,
            api_secret,
            method="POST",
        )
        weight = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
        tag = "OK" if 200 <= status < 300 else "ERR"
        print(f"  [{tag}] status={status} weight={weight}")
        try:
            print(f"  {json.dumps(json.loads(body), ensure_ascii=False)}")
        except json.JSONDecodeError:
            print(f"  {body}")
        if not (200 <= status < 300):
            failures += 1
    return failures, total_requests


def submit_okx_orders(orders: List[PlannedOrder]) -> Tuple[int, int]:
    api_key, api_secret, passphrase = load_okx_credentials()
    failures = 0
    total_requests = 0
    for order in orders:
        total_requests += 1
        print(f"\n[order] {order.symbol} {order.side} sz={order.request_body['sz']}")
        status, body, headers = okx_private_request(
            "POST",
            "/api/v5/trade/order",
            api_key,
            api_secret,
            passphrase,
            body=order.request_body,
        )
        okx_ok, brief = okx_response_ok(body)
        ok = (200 <= status < 300) and okx_ok
        tag = "OK" if ok else "ERR"
        print(f"  [{tag}] status={status} reqId={headers.get('x-request-id')}")
        try:
            print(f"  {json.dumps(json.loads(body), ensure_ascii=False)}")
        except json.JSONDecodeError:
            print(f"  {body}")
        if not ok:
            if brief:
                print(f"  {brief}")
            failures += 1
    return failures, total_requests


def build_gate_child_orders(
    order: PlannedOrder,
    *,
    max_order_usdt: Decimal,
) -> List[GateChildOrder]:
    total_usdt = abs(order.net_usdt)
    total_qty = order.order_qty
    if max_order_usdt <= 0 or total_usdt <= max_order_usdt or total_qty <= 0:
        return [GateChildOrder(qty=total_qty, est_usdt=total_usdt, request_body=dict(order.request_body))]

    step = order.step_size if order.step_size > 0 else Decimal("1")
    min_qty = order.min_qty if order.min_qty > 0 else step
    avg_usdt_per_contract = total_usdt / total_qty if total_qty > 0 else ZERO
    if avg_usdt_per_contract <= 0:
        return [GateChildOrder(qty=total_qty, est_usdt=total_usdt, request_body=dict(order.request_body))]

    chunk_qty = floor_to_step(max_order_usdt / avg_usdt_per_contract, step)
    if chunk_qty < min_qty:
        chunk_qty = floor_to_step(min_qty, step)
    if chunk_qty <= 0 or chunk_qty >= total_qty:
        return [GateChildOrder(qty=total_qty, est_usdt=total_usdt, request_body=dict(order.request_body))]

    children: List[GateChildOrder] = []
    remaining_qty = total_qty
    remaining_usdt = total_usdt
    base_text = str(order.request_body.get("text", f"t-frflat-{now_ms()}")).strip() or f"t-frflat-{now_ms()}"

    child_idx = 0
    while remaining_qty > 0:
        qty = chunk_qty if remaining_qty > chunk_qty else remaining_qty
        qty = floor_to_step(qty, step)
        if qty <= 0:
            qty = remaining_qty
        rem_after = remaining_qty - qty
        if rem_after > 0 and rem_after < min_qty:
            qty = remaining_qty
            rem_after = ZERO

        est_usdt = total_usdt * qty / total_qty
        if rem_after == 0:
            est_usdt = remaining_usdt

        signed_qty = -qty if order.side == "SELL" else qty
        body = dict(order.request_body)
        body["size"] = format_decimal(signed_qty)
        body["text"] = f"{base_text}-p{child_idx + 1}"
        children.append(GateChildOrder(qty=qty, est_usdt=est_usdt, request_body=body))

        remaining_qty = rem_after
        remaining_usdt -= est_usdt
        child_idx += 1

    return children


def parse_gate_error_label(body: str) -> str:
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return ""
    return str(parsed.get("label", "")).strip().upper()


def parse_gate_risk_limit_message(body: str) -> str:
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return ""
    return str(parsed.get("message", "")).strip()


def submit_gate_child_with_retry(
    api_key: str,
    api_secret: str,
    *,
    order: PlannedOrder,
    child: GateChildOrder,
) -> Tuple[int, int, bool]:
    failures = 0
    total_requests = 0
    qty = child.qty
    base_text = str(child.request_body.get("text", f"t-frflat-{now_ms()}")).strip() or f"t-frflat-{now_ms()}"
    attempt = 0

    while qty >= order.min_qty and qty > 0:
        attempt += 1
        total_requests += 1
        signed_qty = -qty if order.side == "SELL" else qty
        body = dict(child.request_body)
        body["size"] = format_decimal(signed_qty)
        body["text"] = f"{base_text}-r{attempt}"
        est_usdt = abs(order.net_usdt) * qty / order.order_qty if order.order_qty > 0 else ZERO

        print(
            f"\n[order] {order.symbol} {order.side} size={body['size']} "
            f"(retry {attempt} est_usdt={format_decimal(est_usdt)})"
        )
        status, resp_body, _headers = gate_private_request(
            "POST",
            "/futures/usdt/orders",
            api_key,
            api_secret,
            body=body,
        )
        tag = "OK" if 200 <= status < 300 else "ERR"
        print(f"  [{tag}] status={status}")
        try:
            print(f"  {json.dumps(json.loads(resp_body), ensure_ascii=False)}")
        except json.JSONDecodeError:
            print(f"  {resp_body}")

        if 200 <= status < 300:
            return failures, total_requests, True

        failures += 1
        if parse_gate_error_label(resp_body) != "RISK_LIMIT_EXCEEDED":
            return failures, total_requests, False

        next_qty = floor_to_step(qty / 2, order.step_size)
        if next_qty >= qty:
            next_qty = qty - order.step_size
        next_qty = floor_to_step(next_qty, order.step_size)
        if next_qty < order.min_qty or next_qty <= 0:
            risk_msg = parse_gate_risk_limit_message(resp_body)
            if risk_msg:
                print(f"  [stop] risk-limit saturated: {risk_msg}")
            return failures, total_requests, False

        print(
            f"  [retry] Gate risk-limit hit, reducing qty from {format_decimal(qty)} "
            f"to {format_decimal(next_qty)} contracts"
        )
        qty = next_qty

    return failures, total_requests, False


def submit_gate_orders(
    orders: List[PlannedOrder],
    *,
    cancel_open_orders: bool,
    max_order_usdt: Decimal,
) -> Tuple[int, int]:
    api_key, api_secret = load_gate_credentials()
    failures = 0
    total_requests = 0
    if cancel_open_orders:
        seen_contracts = set()
        for order in orders:
            if order.symbol in seen_contracts:
                continue
            gate_cancel_open_orders(
                api_key,
                api_secret,
                settle="usdt",
                contract=order.symbol,
            )
            seen_contracts.add(order.symbol)
    for order in orders:
        child_orders = build_gate_child_orders(order, max_order_usdt=max_order_usdt)
        if len(child_orders) > 1:
            print(
                f"[split] {order.symbol}: total_qty={format_decimal(order.order_qty)} "
                f"total_usdt={format_decimal(abs(order.net_usdt))} "
                f"max_chunk_usdt={format_decimal(max_order_usdt)} -> {len(child_orders)} child orders"
            )
        for idx, child in enumerate(child_orders, start=1):
            print(
                f"[child] {order.symbol} target {idx}/{len(child_orders)} "
                f"qty={format_decimal(child.qty)} est_usdt={format_decimal(child.est_usdt)}"
            )
            child_failures, child_requests, ok = submit_gate_child_with_retry(
                api_key,
                api_secret,
                order=order,
                child=child,
            )
            failures += child_failures
            total_requests += child_requests
            if not ok:
                print(f"[stop] {order.symbol}: stop sending more child orders after failed risk-limited retries")
                break
    return failures, total_requests


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Flatten FR futures exposure on uniform accounts",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--exchange", required=True, choices=["binance", "okex", "okx", "gate"])
    parser.add_argument(
        "--suffix",
        help="FR env suffix. Example: trade, trade01, trade02. Used with --exchange to build <exchange>_fr_<suffix>.",
    )
    parser.add_argument("--env-name", help="Override env name, e.g. binance_fr_trade01")
    parser.add_argument("--snapshot-url", help="Override snapshot URL directly")
    parser.add_argument("--host", help="Dashboard host for /fr/<env>/snapshot")
    parser.add_argument("--port", type=int, default=DASHBOARD_PORT, help="Dashboard port for /fr/<env>/snapshot")
    parser.add_argument(
        "--env-file",
        help="Source a local env.sh before reading exchange credentials",
    )
    parser.add_argument(
        "--env-ssh",
        help="Source a remote env.sh over ssh, format: user@host:/abs/path/env.sh",
    )
    parser.add_argument(
        "--min-net-usdt",
        type=Decimal,
        default=Decimal("5"),
        dest="min_net_usdt",
        help="Only process symbols with abs(net_usdt) above this threshold",
    )
    parser.add_argument(
        "--skip-assets",
        default="BNB",
        help="Comma-separated assets to skip",
    )
    parser.add_argument(
        "--cancel-open-orders",
        action="store_true",
        help="Gate only: cancel open futures orders on target contracts before submitting flatten orders",
    )
    parser.add_argument(
        "--gate-max-order-usdt",
        type=Decimal,
        default=Decimal("100"),
        help="Gate only: split one symbol into multiple child orders when abs(net_usdt) exceeds this value; set <=0 to disable splitting",
    )
    parser.add_argument("--execute", action="store_true", help="Actually submit orders")
    return parser.parse_args()


def resolve_snapshot_url(args: argparse.Namespace, exchange: str) -> str:
    if args.snapshot_url:
        return args.snapshot_url
    env_name = args.env_name or default_env_name(exchange, args.suffix)
    host = args.host or default_dashboard_host(exchange)
    return f"http://{host}:{args.port}/fr/{env_name}/snapshot"


def filter_rows(rows: List[ExposureRow], min_net_usdt: Decimal, skip_assets: set[str]) -> List[ExposureRow]:
    filtered: List[ExposureRow] = []
    for row in rows:
        if row.asset in skip_assets:
            continue
        if abs(row.net_usdt) < min_net_usdt:
            continue
        if row.net_qty == 0:
            continue
        filtered.append(row)
    filtered.sort(key=lambda item: abs(item.net_usdt), reverse=True)
    return filtered


def main() -> None:
    args = parse_args()
    exchange = normalize_exchange(args.exchange)
    env_name = args.env_name or (default_env_name(exchange, args.suffix) if not args.snapshot_url else "")

    if args.env_file and args.env_ssh:
        raise SystemExit("use only one of --env-file or --env-ssh")
    if args.env_file:
        load_env_into_process(dump_shell_env_from_file(args.env_file))
    elif args.env_ssh:
        ssh_target, ssh_path = parse_env_ssh(args.env_ssh)
        load_env_into_process(dump_shell_env_over_ssh(ssh_target, ssh_path))
    else:
        default_remote_env = default_env_ssh(exchange, env_name) if env_name else None
        if default_remote_env:
            ssh_target, ssh_path = parse_env_ssh(default_remote_env)
            load_env_into_process(dump_shell_env_over_ssh(ssh_target, ssh_path))

    snapshot_url = resolve_snapshot_url(args, exchange)
    skip_assets = {item.strip().upper() for item in args.skip_assets.split(",") if item.strip()}

    print(f"[info] exchange={exchange} account_mode=uniform")
    print(f"[info] snapshot_url={snapshot_url}")
    if exchange == "gate":
        print(f"[info] gate_max_order_usdt={format_decimal(args.gate_max_order_usdt)}")
    rows = fetch_snapshot(snapshot_url)
    if not rows:
        print("No exposure rows found.")
        return

    targets = filter_rows(rows, args.min_net_usdt, skip_assets)
    if not targets:
        print("No symbols to flatten after filtering.")
        return

    if exchange == "binance":
        orders, skipped = build_binance_orders(targets)
    elif exchange == "okex":
        orders, skipped = build_okx_orders(targets)
    elif exchange == "gate":
        orders, skipped = build_gate_orders(targets)
    else:
        raise SystemExit(f"unsupported exchange: {exchange}")

    print_plan(orders, skipped)
    if not orders:
        return

    if not args.execute:
        print("\nDry-run mode. Add --execute to actually submit orders.")
        return

    if args.cancel_open_orders and exchange != "gate":
        raise SystemExit("--cancel-open-orders is currently supported only for --exchange gate")

    print("\n" + "=" * 72)
    print("EXECUTING ORDERS")
    print("=" * 72)

    if exchange == "binance":
        failures, total_requests = submit_binance_orders(orders)
    elif exchange == "okex":
        failures, total_requests = submit_okx_orders(orders)
    else:
        failures, total_requests = submit_gate_orders(
            orders,
            cancel_open_orders=args.cancel_open_orders,
            max_order_usdt=args.gate_max_order_usdt,
        )

    print()
    if failures:
        print(f"WARN: {failures}/{total_requests} order requests failed", file=sys.stderr)
        sys.exit(1)
    print(f"All {total_requests} order requests submitted successfully.")


if __name__ == "__main__":
    main()
