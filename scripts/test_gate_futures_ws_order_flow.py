#!/usr/bin/env python3
"""Run:
  export GATE_API_KEY="..."
  export GATE_API_SECRET="..."
  python scripts/test_gate_futures_ws_order_flow.py \
    --contract BTC_USDT --side buy --size 1 --auto-offset-bps 100

Test Gate futures WS trade response vs private order/trade pushes.

Goal:
  Validate the full Gate futures websocket flow for a maker-style order:
  1. trade API WS response for futures.order_place
  2. private push on futures.orders
  3. private push on futures.usertrades (if any fill happens)
  4. trade API WS response for futures.order_cancel
  5. private push on futures.orders for cancel

Default behavior:
  - Fetch best bid/ask from REST
  - Place LIMIT + POC order far from the book so it should rest as maker
  - Wait a bit
  - Cancel the order
  - Print raw websocket messages with short labels

Dependencies:
  pip install websocket-client requests
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import ssl
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

import requests

try:
    import websocket  # type: ignore
except ImportError:
    print("缺少依赖：pip install websocket-client", file=sys.stderr)
    sys.exit(1)


HOST = "https://api.gateio.ws"
PREFIX = "/api/v4"
FUTURES_WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"


def load_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("GATE_API_KEY", "").strip()
    api_secret = os.environ.get("GATE_API_SECRET", "").strip()
    missing = [name for name, value in [
        ("GATE_API_KEY", api_key),
        ("GATE_API_SECRET", api_secret),
    ] if not value]
    if missing:
        print(f"请设置环境变量: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret


def now_s() -> int:
    return int(time.time())


def now_ms() -> int:
    return int(time.time() * 1000)


def gen_private_sub_sign(channel: str, event: str, timestamp: int, secret: str) -> str:
    msg = f"channel={channel}&event={event}&time={timestamp}"
    return hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha512).hexdigest()


def gen_api_sign(secret: str, event: str, channel: str, req_param: str, timestamp: int) -> str:
    msg = f"{event}\n{channel}\n{req_param}\n{timestamp}"
    return hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha512).hexdigest()


def gen_rest_sign(method: str, url: str, query_string: str, body: str, secret: str, timestamp: str) -> str:
    hashed_body = hashlib.sha512(body.encode("utf-8")).hexdigest()
    sign_string = f"{method}\n{url}\n{query_string}\n{hashed_body}\n{timestamp}"
    return hmac.new(secret.encode("utf-8"), sign_string.encode("utf-8"), hashlib.sha512).hexdigest()


def requests_json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except ValueError:
        return {"raw": resp.text}


def rest_get(api_key: str, api_secret: str, path: str) -> Any:
    ts = str(int(time.time()))
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "KEY": api_key,
        "Timestamp": ts,
        "SIGN": gen_rest_sign("GET", PREFIX + path, "", "", api_secret, ts),
    }
    url = f"{HOST}{PREFIX}{path}"
    resp = requests.get(url, headers=headers, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"GET {path} failed: {resp.status_code} {requests_json(resp)}")
    return requests_json(resp)


def fetch_best_bid_ask(contract: str) -> Tuple[float, float]:
    url = f"{HOST}{PREFIX}/futures/usdt/order_book"
    resp = requests.get(url, params={"contract": contract, "limit": 1}, timeout=10)
    data = requests_json(resp)
    if resp.status_code != 200 or not isinstance(data, dict):
        raise RuntimeError(f"fetch order book failed: status={resp.status_code} body={data}")
    asks = data.get("asks") or []
    bids = data.get("bids") or []
    if not asks or not bids:
        raise RuntimeError(f"empty order book for {contract}: {data}")
    best_ask = float(asks[0]["p"] if isinstance(asks[0], dict) else asks[0][0])
    best_bid = float(bids[0]["p"] if isinstance(bids[0], dict) else bids[0][0])
    return best_bid, best_ask


def fetch_contract_info(contract: str) -> Dict[str, Any]:
    url = f"{HOST}{PREFIX}/futures/usdt/contracts/{contract}"
    resp = requests.get(url, timeout=10)
    data = requests_json(resp)
    if resp.status_code != 200 or not isinstance(data, dict):
        raise RuntimeError(f"fetch contract info failed: status={resp.status_code} body={data}")
    return data


def detect_price_scale(contract_info: Dict[str, Any]) -> int:
    for key in ("order_price_round", "mark_price_round", "price_round"):
        val = contract_info.get(key)
        if val is None:
            continue
        try:
            return int(val)
        except Exception:
            continue
    return 4


def format_price(value: float, scale: int) -> str:
    text = f"{value:.{max(scale, 0)}f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def format_size(value: float) -> str:
    text = f"{value:.12f}".rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def compute_maker_price(side: str, best_bid: float, best_ask: float, offset_bps: float, scale: int) -> str:
    ratio = max(offset_bps, 1.0) / 10000.0
    if side.lower() == "buy":
        px = best_bid * (1.0 - ratio)
    else:
        px = best_ask * (1.0 + ratio)
    return format_price(px, scale)


def extract_order_id(msg: Dict[str, Any]) -> Optional[int]:
    payload = msg.get("payload")
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            result = data.get("result")
            if isinstance(result, dict):
                for key in ("id", "order_id"):
                    value = result.get(key)
                    if value is not None:
                        try:
                            return int(value)
                        except Exception:
                            pass
    result = msg.get("result")
    if isinstance(result, dict):
        for key in ("id", "order_id"):
            value = result.get(key)
            if value is not None:
                try:
                    return int(value)
                except Exception:
                    pass
    data = msg.get("data")
    if isinstance(data, dict):
        result = data.get("result")
        if isinstance(result, dict):
            for key in ("id", "order_id"):
                value = result.get(key)
                if value is not None:
                    try:
                        return int(value)
                    except Exception:
                        pass
    return None


def summarize_gate_order_push(msg: Dict[str, Any]) -> str:
    result = msg.get("result")
    if not isinstance(result, list) or not result:
        return ""
    order = result[0]
    if not isinstance(order, dict):
        return ""
    return (
        f"status={order.get('status')} finish_as={order.get('finish_as')} "
        f"id={order.get('id')} text={order.get('text')} "
        f"size={order.get('size')} left={order.get('left')} "
        f"price={order.get('price')} fill_price={order.get('fill_price')}"
    )


def summarize_user_trade_push(msg: Dict[str, Any]) -> str:
    result = msg.get("result")
    if not isinstance(result, list) or not result:
        return ""
    trade = result[0]
    if not isinstance(trade, dict):
        return ""
    return (
        f"id={trade.get('id')} order_id={trade.get('order_id')} "
        f"text={trade.get('text')} size={trade.get('size')} price={trade.get('price')} "
        f"role={trade.get('role')}"
    )


def compact_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


@dataclass
class SharedState:
    client_order_id: int
    contract: str
    side: str
    size: str
    price: str
    cancel_after: float
    wait_after_cancel: float
    place_sent_at: float = 0.0
    cancel_sent_at: float = 0.0
    done: threading.Event = field(default_factory=threading.Event)
    order_id: Optional[int] = None
    saw_order_new_like: bool = False
    saw_order_cancel_like: bool = False
    saw_usertrade: bool = False


class PrivateWsClient:
    def __init__(self, api_key: str, api_secret: str, state: SharedState):
        self.api_key = api_key
        self.api_secret = api_secret
        self.state = state
        self.ws: Optional[websocket.WebSocketApp] = None

    def build_subscribe(self, channel: str) -> Dict[str, Any]:
        ts = now_s()
        return {
            "time": ts,
            "channel": channel,
            "event": "subscribe",
            "payload": ["!all"],
            "auth": {
                "method": "api_key",
                "KEY": self.api_key,
                "SIGN": gen_private_sub_sign(channel, "subscribe", ts, self.api_secret),
            },
        }

    def on_open(self, ws) -> None:
        req1 = self.build_subscribe("futures.orders")
        req2 = self.build_subscribe("futures.usertrades")
        print(f"[private] >>> {compact_json(req1)}")
        ws.send(json.dumps(req1))
        print(f"[private] >>> {compact_json(req2)}")
        ws.send(json.dumps(req2))

    def on_message(self, ws, raw_msg: str) -> None:
        if isinstance(raw_msg, bytes):
            raw_msg = raw_msg.decode("utf-8", "replace")
        try:
            msg = json.loads(raw_msg)
        except json.JSONDecodeError:
            print(f"[private] <<< raw={raw_msg}")
            return

        channel = msg.get("channel")
        event = msg.get("event")
        prefix = f"[private] <<< channel={channel} event={event}"

        if channel == "futures.orders" and event == "update":
            summary = summarize_gate_order_push(msg)
            print(f"{prefix} {summary}")
            result = msg.get("result")
            if isinstance(result, list):
                for item in result:
                    if not isinstance(item, dict):
                        continue
                    if str(item.get("text")) not in {str(self.state.client_order_id), f"t-{self.state.client_order_id}"}:
                        continue
                    status = str(item.get("status", "")).lower()
                    finish_as = str(item.get("finish_as", "")).lower()
                    if status == "open" and finish_as in {"_new", "_update", ""}:
                        self.state.saw_order_new_like = True
                    if status == "finished" and finish_as in {"cancelled", "canceled"}:
                        self.state.saw_order_cancel_like = True
        elif channel == "futures.usertrades" and event == "update":
            summary = summarize_user_trade_push(msg)
            print(f"{prefix} {summary}")
            result = msg.get("result")
            if isinstance(result, list):
                for item in result:
                    if not isinstance(item, dict):
                        continue
                    item_text = str(item.get("text"))
                    item_order_id = item.get("order_id")
                    matches_text = item_text in {
                        str(self.state.client_order_id),
                        f"t-{self.state.client_order_id}",
                    }
                    matches_order_id = self.state.order_id is not None and str(item_order_id) == str(self.state.order_id)
                    if matches_text or matches_order_id:
                        self.state.saw_usertrade = True
        else:
            print(f"{prefix} raw={compact_json(msg)}")

    def on_error(self, ws, error) -> None:
        print(f"[private] error: {error}", file=sys.stderr)

    def on_close(self, ws, code, reason) -> None:
        print(f"[private] closed code={code} reason={reason}")

    def run(self) -> None:
        sslopt = {"cert_reqs": ssl.CERT_REQUIRED}
        self.ws = websocket.WebSocketApp(
            FUTURES_WS_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        self.ws.run_forever(ping_interval=20, ping_timeout=10, sslopt=sslopt)


class TradeWsClient:
    def __init__(self, api_key: str, api_secret: str, state: SharedState):
        self.api_key = api_key
        self.api_secret = api_secret
        self.state = state
        self.ws: Optional[websocket.WebSocketApp] = None
        self.login_req_id = f"login-{now_ms()}"
        self.place_transport_id = self.state.client_order_id
        self.cancel_transport_id = self.state.client_order_id + 1

    def build_login(self) -> Dict[str, Any]:
        ts = now_s()
        return {
            "time": ts,
            "channel": "futures.login",
            "event": "api",
            "payload": {
                "req_id": self.login_req_id,
                "api_key": self.api_key,
                "signature": gen_api_sign(self.api_secret, "api", "futures.login", "", ts),
                "timestamp": str(ts),
            },
        }

    def build_place(self) -> Dict[str, Any]:
        req_param = {
            "text": f"t-{self.state.client_order_id}",
            "contract": self.state.contract,
            "account": "unified",
            "size": self.state.size if self.state.side.lower() == "buy" else format_size(-float(self.state.size)),
            "price": self.state.price,
            "tif": "poc",
        }
        req_param_str = compact_json(req_param)
        ts = now_s()
        return {
            "time": ts,
            "channel": "futures.order_place",
            "event": "api",
            "payload": {
                "req_id": str(self.place_transport_id),
                "req_param": req_param,
                "api_key": self.api_key,
                "signature": gen_api_sign(self.api_secret, "api", "futures.order_place", req_param_str, ts),
                "timestamp": str(ts),
            },
        }

    def build_cancel(self, order_id: int) -> Dict[str, Any]:
        req_param = {
            "order_id": str(order_id),
            "contract": self.state.contract,
        }
        req_param_str = compact_json(req_param)
        ts = now_s()
        return {
            "time": ts,
            "channel": "futures.order_cancel",
            "event": "api",
            "payload": {
                "req_id": str(self.cancel_transport_id),
                "req_param": req_param,
                "api_key": self.api_key,
                "signature": gen_api_sign(self.api_secret, "api", "futures.order_cancel", req_param_str, ts),
                "timestamp": str(ts),
            },
        }

    def schedule_cancel(self, ws) -> None:
        def worker() -> None:
            time.sleep(self.state.cancel_after)
            deadline = time.time() + 10.0
            while self.state.order_id is None and time.time() < deadline:
                time.sleep(0.2)
            if self.state.order_id is None:
                print("[trade] cancel skipped: no order_id captured from place response", file=sys.stderr)
                self.state.done.set()
                return
            req = self.build_cancel(self.state.order_id)
            self.state.cancel_sent_at = time.time()
            print(f"[trade] >>> {compact_json(req)}")
            ws.send(json.dumps(req))
            time.sleep(self.state.wait_after_cancel)
            self.state.done.set()

        threading.Thread(target=worker, daemon=True).start()

    def on_open(self, ws) -> None:
        login = self.build_login()
        print(f"[trade] >>> {compact_json(login)}")
        ws.send(json.dumps(login))

    def on_message(self, ws, raw_msg: str) -> None:
        if isinstance(raw_msg, bytes):
            raw_msg = raw_msg.decode("utf-8", "replace")
        try:
            msg = json.loads(raw_msg)
        except json.JSONDecodeError:
            print(f"[trade] <<< raw={raw_msg}")
            return

        header = msg.get("header") if isinstance(msg.get("header"), dict) else {}
        channel = msg.get("channel") or header.get("channel")
        payload = msg.get("payload")
        ack = payload.get("ack") if isinstance(payload, dict) else None
        print(f"[trade] <<< channel={channel} ack={ack} raw={compact_json(msg)}")

        login_ok = False
        if channel == "futures.login":
            if isinstance(payload, dict) and payload.get("response_time") is not None:
                login_ok = True
            elif isinstance(header, dict) and header.get("response_time") is not None:
                login_ok = True
        if login_ok:
            req = self.build_place()
            self.state.place_sent_at = time.time()
            print(f"[trade] >>> {compact_json(req)}")
            ws.send(json.dumps(req))
            self.schedule_cancel(ws)
            return

        if channel == "futures.order_place":
            order_id = extract_order_id(msg)
            if order_id is not None and self.state.order_id is None:
                self.state.order_id = order_id
                print(f"[trade] captured order_id={order_id}")

    def on_error(self, ws, error) -> None:
        print(f"[trade] error: {error}", file=sys.stderr)
        self.state.done.set()

    def on_close(self, ws, code, reason) -> None:
        print(f"[trade] closed code={code} reason={reason}")

    def run(self) -> None:
        sslopt = {"cert_reqs": ssl.CERT_REQUIRED}
        self.ws = websocket.WebSocketApp(
            FUTURES_WS_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        self.ws.run_forever(ping_interval=20, ping_timeout=10, sslopt=sslopt)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test Gate futures WS trade response and private pushes.")
    parser.add_argument("--contract", default="BTC_USDT", help="Gate futures contract, e.g. BTC_USDT")
    parser.add_argument("--side", choices=["buy", "sell"], default="buy", help="Order side")
    parser.add_argument("--size", default="1", help="Contracts size, positive number")
    parser.add_argument("--price", default="", help="Manual limit price. If empty, auto compute maker price.")
    parser.add_argument("--auto-offset-bps", type=float, default=100.0, help="Offset from best bid/ask for auto maker price")
    parser.add_argument("--cancel-after", type=float, default=3.0, help="Seconds to wait before cancel")
    parser.add_argument("--wait-after-cancel", type=float, default=5.0, help="Seconds to continue listening after cancel")
    parser.add_argument("--timeout", type=float, default=20.0, help="Hard timeout for the whole test")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_key, api_secret = load_credentials()

    contract = args.contract.upper().strip()
    side = args.side.lower().strip()
    size = format_size(abs(float(args.size)))

    best_bid, best_ask = fetch_best_bid_ask(contract)
    contract_info = fetch_contract_info(contract)
    price_scale = detect_price_scale(contract_info)
    price = args.price.strip() or compute_maker_price(side, best_bid, best_ask, args.auto_offset_bps, price_scale)

    client_order_id = now_ms()
    state = SharedState(
        client_order_id=client_order_id,
        contract=contract,
        side=side,
        size=size,
        price=price,
        cancel_after=args.cancel_after,
        wait_after_cancel=args.wait_after_cancel,
    )

    print("Gate futures WS order-flow test")
    print(f"contract={contract} side={side} size={size} price={price} client_order_id={client_order_id}")
    print(f"best_bid={best_bid} best_ask={best_ask} price_scale={price_scale}")
    print("Expected timeline:")
    print("  1. trade WS: futures.login")
    print("  2. trade WS: futures.order_place ack=true then ack=false/result")
    print("  3. private WS: futures.orders update, ideally open + _new for resting maker")
    print("  4. trade WS: futures.order_cancel ack=true then ack=false/result")
    print("  5. private WS: futures.orders update, ideally finished + cancelled")
    print("  6. private WS: futures.usertrades only if any fill actually happened")
    print("-" * 80)

    private_client = PrivateWsClient(api_key, api_secret, state)
    trade_client = TradeWsClient(api_key, api_secret, state)

    private_thread = threading.Thread(target=private_client.run, daemon=True)
    trade_thread = threading.Thread(target=trade_client.run, daemon=True)
    private_thread.start()
    time.sleep(0.5)
    trade_thread.start()

    finished = state.done.wait(timeout=args.timeout)
    time.sleep(1.0)

    print("-" * 80)
    print("Summary")
    print(f"order_id={state.order_id}")
    print(f"saw_futures_orders_new_like={state.saw_order_new_like}")
    print(f"saw_futures_orders_cancel_like={state.saw_order_cancel_like}")
    print(f"saw_futures_usertrades={state.saw_usertrade}")
    if not finished:
        print("timeout=true")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
