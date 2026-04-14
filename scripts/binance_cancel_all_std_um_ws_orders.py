#!/usr/bin/env python3
"""Cancel all Binance standard-account UM futures open orders via WebSocket.

Default behavior is dry-run:
  1) query current open UM orders via REST
  2) print the cancel plan

Use --execute to actually send `order.cancel` over Binance FAPI WebSocket for each
open order. This is intended for Binance STANDARD account mode MM environments
where UM orders are placed over WS.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

from binance_local_ip import create_ws_connection, resolve_local_address, urlopen_with_local_address


DEFAULT_REST_BASE_URL = os.environ.get("BINANCE_FAPI_URL") or "https://fapi.binance.com"
DEFAULT_WS_URL = os.environ.get("BINANCE_FAPI_WS_URL") or "wss://ws-fapi.binance.com/ws-fapi/v1"
OPEN_ORDERS_PATH = "/fapi/v1/openOrders"


@dataclass(frozen=True)
class OpenOrder:
    symbol: str
    order_id: Optional[int]
    orig_client_order_id: str
    side: str
    order_type: str
    price: str
    orig_qty: str
    executed_qty: str
    status: str

    @property
    def key(self) -> tuple[str, Optional[int], str]:
        return (self.symbol, self.order_id, self.orig_client_order_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cancel all Binance standard-account UM futures open orders via WS",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_REST_BASE_URL,
        help="Binance FAPI REST base URL used to query open orders",
    )
    parser.add_argument(
        "--ws-url",
        default=DEFAULT_WS_URL,
        help="Binance FAPI WebSocket API URL used to cancel orders",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        default=[],
        help="Restrict to specific symbol(s); repeatable, e.g. --symbol BTCUSDT --symbol ETHUSDT",
    )
    parser.add_argument(
        "--recv-window",
        type=int,
        default=5000,
        help="recvWindow in milliseconds",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="HTTP/WS timeout in seconds",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually cancel orders; default is dry-run",
    )
    parser.add_argument(
        "--local-address",
        default=None,
        help="Explicit source IP for REST/WS; default auto-detects from trade_engine.toml / mkt_cfg.yaml",
    )
    parser.add_argument(
        "--trade-engine-config",
        default=None,
        help="Explicit trade_engine.toml path; overrides auto-discovery",
    )
    parser.add_argument(
        "--env-dir",
        default=None,
        help="MM env dir; resolves <env-dir>/trade_engine.toml before cwd fallback",
    )
    return parser.parse_args()


def try_import_ws():
    try:
        import websocket  # type: ignore

        return websocket
    except Exception:
        return None


def now_ms() -> int:
    return int(time.time() * 1000)


def sign_query(params: Dict[str, str], secret: str) -> str:
    items = sorted((k, str(v)) for k, v in params.items() if v is not None)
    query = urllib.parse.urlencode(items, safe="-_.~")
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def load_credentials() -> tuple[str, str]:
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: missing BINANCE_API_KEY / BINANCE_API_SECRET", file=sys.stderr)
        raise SystemExit(1)
    return api_key, api_secret


def normalize_symbols(raw_symbols: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in raw_symbols:
        symbol = raw.strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def signed_rest_request(
    *,
    base_url: str,
    path: str,
    params: Dict[str, str],
    api_key: str,
    api_secret: str,
    method: str,
    timeout: int,
    local_address: Optional[str],
) -> tuple[int, str, dict[str, str]]:
    query_params = dict(params)
    query_params.setdefault("timestamp", str(now_ms()))
    query = urllib.parse.urlencode(sorted(query_params.items()), safe="-_.~")
    signature = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    url = f"{base_url.rstrip('/')}{path}?{query}&signature={signature}"
    req = urllib.request.Request(url, method=method, headers={"X-MBX-APIKEY": api_key})
    try:
        with urlopen_with_local_address(req, timeout=timeout, local_address=local_address) as resp:
            status = resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
            headers = dict(resp.headers.items())
            return status, body, headers
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        headers = dict(getattr(exc, "headers", {}).items()) if getattr(exc, "headers", None) else {}
        return exc.code, body, headers
    except Exception as exc:  # pragma: no cover - network failure
        return 0, str(exc), {}


def header_value(headers: dict[str, str], key: str) -> str:
    return headers.get(key) or headers.get(key.lower()) or "-"


def parse_order_id(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def parse_open_orders(body: str) -> list[OpenOrder]:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []

    orders: list[OpenOrder] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        orders.append(
            OpenOrder(
                symbol=symbol,
                order_id=parse_order_id(item.get("orderId")),
                orig_client_order_id=str(item.get("clientOrderId", "")).strip(),
                side=str(item.get("side", "")).strip().upper(),
                order_type=str(item.get("type", "")).strip().upper(),
                price=str(item.get("price", "")).strip(),
                orig_qty=str(item.get("origQty", "")).strip(),
                executed_qty=str(item.get("executedQty", "")).strip(),
                status=str(item.get("status", "")).strip().upper(),
            )
        )
    return orders


def query_open_orders(
    *,
    base_url: str,
    api_key: str,
    api_secret: str,
    recv_window: int,
    timeout: int,
    requested_symbols: list[str],
    local_address: Optional[str],
) -> list[OpenOrder]:
    all_orders: list[OpenOrder] = []
    query_symbols = requested_symbols or [""]

    for symbol in query_symbols:
        params: Dict[str, str] = {"recvWindow": str(recv_window)}
        if symbol:
            params["symbol"] = symbol
        status, body, headers = signed_rest_request(
            base_url=base_url,
            path=OPEN_ORDERS_PATH,
            params=params,
            api_key=api_key,
            api_secret=api_secret,
            method="GET",
            timeout=timeout,
            local_address=local_address,
        )
        orders = parse_open_orders(body)
        scope = symbol or "ALL"
        print(
            f"[query] symbol={scope} status={status} open_orders={len(orders)} "
            f"used_weight_1m={header_value(headers, 'x-mbx-used-weight-1m')} "
            f"order_count_1m={header_value(headers, 'x-mbx-order-count-1m')}"
        )
        if status != 200:
            print(f"[query] failed for symbol={scope}: {body}", file=sys.stderr)
            raise SystemExit(1)
        all_orders.extend(orders)

    dedup: dict[tuple[str, Optional[int], str], OpenOrder] = {}
    for order in all_orders:
        dedup[order.key] = order
    return sorted(dedup.values(), key=lambda order: (order.symbol, order.order_id or -1, order.orig_client_order_id))


def print_cancel_plan(orders: list[OpenOrder], execute: bool) -> None:
    print(f"[plan] open_orders={len(orders)} execute={execute}")
    for order in orders:
        order_ref = f"orderId={order.order_id}" if order.order_id is not None else f"clientOrderId={order.orig_client_order_id}"
        print(
            f"[plan] symbol={order.symbol} {order_ref} side={order.side} type={order.order_type} "
            f"price={order.price} origQty={order.orig_qty} executedQty={order.executed_qty} status={order.status}"
        )


def ws_call(ws, payload: Dict[str, Any]) -> Dict[str, Any]:
    ws.send(json.dumps(payload))
    raw = ws.recv()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {"raw": raw}
    return parsed if isinstance(parsed, dict) else {"raw": raw}


def cancel_one_order_ws(
    *,
    ws,
    ws_request_id: int,
    order: OpenOrder,
    api_key: str,
    api_secret: str,
    recv_window: int,
) -> Dict[str, Any]:
    params: Dict[str, str] = {
        "apiKey": api_key,
        "symbol": order.symbol,
        "recvWindow": str(recv_window),
        "timestamp": str(now_ms()),
    }
    if order.order_id is not None:
        params["orderId"] = str(order.order_id)
    elif order.orig_client_order_id:
        params["origClientOrderId"] = order.orig_client_order_id
    else:
        return {
            "status": 0,
            "error": {
                "msg": f"missing orderId/clientOrderId for symbol={order.symbol}",
            },
        }

    params["signature"] = sign_query(params, api_secret)
    payload = {
        "id": ws_request_id,
        "method": "order.cancel",
        "params": params,
    }
    return ws_call(ws, payload)


def response_ok(resp: Dict[str, Any]) -> bool:
    if not isinstance(resp, dict):
        return False
    if "error" in resp:
        return False
    status = resp.get("status")
    return isinstance(status, int) and 200 <= status < 300


def print_cancel_response(order: OpenOrder, resp: Dict[str, Any]) -> None:
    status = resp.get("status", 0)
    if response_ok(resp):
        result = resp.get("result")
        try:
            rendered = json.dumps(result, ensure_ascii=False, sort_keys=True)
        except TypeError:
            rendered = str(result)
        print(f"[cancel] symbol={order.symbol} orderId={order.order_id} status={status} result={rendered}")
        return

    err = resp.get("error")
    if isinstance(err, dict):
        code = err.get("code", "-")
        msg = err.get("msg", resp)
        print(
            f"[cancel] symbol={order.symbol} orderId={order.order_id} status={status} error_code={code} error_msg={msg}",
            file=sys.stderr,
        )
    else:
        print(f"[cancel] symbol={order.symbol} orderId={order.order_id} failed: {resp}", file=sys.stderr)


def execute_cancels(
    *,
    ws_url: str,
    timeout: int,
    orders: list[OpenOrder],
    api_key: str,
    api_secret: str,
    recv_window: int,
    local_address: Optional[str],
) -> int:
    websocket = try_import_ws()
    if websocket is None:
        print("ERROR: missing dependency websocket-client. Install via: pip install websocket-client", file=sys.stderr)
        return 1

    try:
        ws = create_ws_connection(
            websocket,
            ws_url=ws_url,
            timeout=timeout,
            local_address=local_address,
            sslopt={"cert_reqs": ssl.CERT_REQUIRED},
        )
    except Exception as exc:
        print(f"ERROR: websocket connect failed: {exc}", file=sys.stderr)
        return 1

    exit_code = 0
    try:
        for idx, order in enumerate(orders, start=1):
            resp = cancel_one_order_ws(
                ws=ws,
                ws_request_id=idx,
                order=order,
                api_key=api_key,
                api_secret=api_secret,
                recv_window=recv_window,
            )
            print_cancel_response(order, resp)
            if not response_ok(resp):
                exit_code = 1
    finally:
        try:
            ws.close()
        except Exception:
            pass
    return exit_code


def main() -> None:
    args = parse_args()
    api_key, api_secret = load_credentials()
    symbols = normalize_symbols(args.symbol)
    local_address, local_address_source = resolve_local_address(
        explicit_local_address=args.local_address,
        trade_engine_config=args.trade_engine_config,
        env_dir=args.env_dir,
    )
    print(f"[config] local_address={local_address or 'system-default'} source={local_address_source}")

    orders = query_open_orders(
        base_url=args.base_url,
        api_key=api_key,
        api_secret=api_secret,
        recv_window=args.recv_window,
        timeout=args.timeout,
        requested_symbols=symbols,
        local_address=local_address,
    )

    if not orders:
        print("[plan] no open UM futures orders found")
        raise SystemExit(0)

    print_cancel_plan(orders, execute=args.execute)
    if not args.execute:
        raise SystemExit(0)

    raise SystemExit(
        execute_cancels(
            ws_url=args.ws_url,
            timeout=args.timeout,
            orders=orders,
            api_key=api_key,
            api_secret=api_secret,
            recv_window=args.recv_window,
            local_address=local_address,
        )
    )


if __name__ == "__main__":
    main()
