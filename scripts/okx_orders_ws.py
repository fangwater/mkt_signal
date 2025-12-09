#!/usr/bin/env python3
"""订阅 OKX 私有 WS orders 频道，方便对比 SPOT / MARGIN 的订单推送。

用法示例：
  # 订阅全部订单（instType=ANY）
  python scripts/okx_orders_ws.py
  # 仅订阅币币（SPOT）
  python scripts/okx_orders_ws.py --inst-type SPOT
  # 仅订阅币币杠杆（MARGIN）
  python scripts/okx_orders_ws.py --inst-type MARGIN
  # 指定某个 instId
  python scripts/okx_orders_ws.py --inst-type MARGIN --inst-id BTC-USDT
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import sys
import threading
import time
from typing import Any, Dict, List, Optional

try:
    import websocket  # type: ignore
except ImportError:
    print("缺少依赖：请先安装 websocket-client (pip install websocket-client)", file=sys.stderr)
    sys.exit(1)

DEFAULT_WS_URL = os.environ.get("OKX_WS_PRIVATE_URL", "wss://ws.okx.com:8443/ws/v5/private")


def ts_sec_local() -> str:
    """返回秒级时间戳字符串（带毫秒小数，符合 OKX 要求）。"""
    return f"{time.time():.3f}"


def sign_login(ts: str, secret: str) -> str:
    payload = f"{ts}GET/users/self/verify"
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def load_credentials() -> tuple[str, str, str]:
    api_key = os.environ.get("OKX_API_KEY", "").strip()
    api_secret = os.environ.get("OKX_API_SECRET", "").strip()
    passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()
    missing = [name for name, value in [("OKX_API_KEY", api_key), ("OKX_API_SECRET", api_secret), ("OKX_PASSPHRASE", passphrase)] if not value]
    if missing:
        print(f"请设置环境变量: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret, passphrase


class WSState:
    def __init__(self, duration: float, subscribe_args: List[Dict[str, Any]], close_after_first: bool) -> None:
        self.duration = duration
        self.subscribe_args = subscribe_args
        self.close_after_first = close_after_first
        self.first_data = False
        self.close_timer: Optional[threading.Timer] = None

    def schedule_close(self, ws) -> None:
        if self.duration <= 0 or self.close_timer is not None:
            return
        self.close_timer = threading.Timer(self.duration, lambda: ws.close())
        self.close_timer.daemon = True
        self.close_timer.start()


def send_json(ws, payload: Dict[str, Any]) -> None:
    ws.send(json.dumps(payload))


def handle_open(ws, api_key: str, api_secret: str, passphrase: str) -> None:
    ts = ts_sec_local()
    signature = sign_login(ts, api_secret)
    login = {
        "op": "login",
        "args": [
            {
                "apiKey": api_key,
                "passphrase": passphrase,
                "timestamp": ts,
                "sign": signature,
            }
        ],
    }
    print(f"Connecting to OKX private WS, sending login at ts={ts}")
    send_json(ws, login)


def handle_ping(ws, msg: Dict[str, Any]) -> bool:
    if msg.get("event") == "ping" or msg.get("op") == "ping":
        send_json(ws, {"op": "pong"})
        return True
    return False


def handle_message(ws, raw_msg, state: WSState) -> None:
    if isinstance(raw_msg, bytes):
        raw_msg = raw_msg.decode("utf-8", "replace")
    try:
        msg: Dict[str, Any] = json.loads(raw_msg)
    except json.JSONDecodeError:
        print(f"Received non-JSON message: {raw_msg}")
        return

    if handle_ping(ws, msg):
        return

    event = msg.get("event") or msg.get("op")
    if event == "error":
        print(f"Error from server: {msg}")
        return
    if event == "pong":
        return
    if event == "login":
        if msg.get("code") != "0":
            print(f"Login failed: {msg}")
            ws.close()
            return
        print("Login success, subscribing orders ...")
        subscribe_payload = {"op": "subscribe", "args": state.subscribe_args}
        print("Subscribe payload:", json.dumps(subscribe_payload, ensure_ascii=False))
        send_json(ws, subscribe_payload)
        state.schedule_close(ws)
        return
    if event == "subscribe":
        print("Subscribe ACK:", json.dumps(msg, ensure_ascii=False))
        return

    arg = msg.get("arg", {})
    channel = arg.get("channel")
    if channel == "orders":
        data = msg.get("data") or []
        print("\n=== orders message ===")
        print(json.dumps(data, indent=2, ensure_ascii=False))
        state.first_data = True
        if state.close_after_first:
            ws.close()
        return

    if channel:
        print(f"Unhandled channel message: {msg}")


def handle_error(_ws, error) -> None:
    print(f"WebSocket error: {error}", file=sys.stderr)


def handle_close(_ws, close_status_code, close_msg) -> None:
    print(f"WebSocket closed code={close_status_code} reason={close_msg}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OKX orders WS 订阅脚本，支持 SPOT/MARGIN/SWAP/FUTURES/OPTION/ANY")
    parser.add_argument("--inst-type", dest="inst_type", default="ANY", help="orders.instType，SPOT/MARGIN/SWAP/FUTURES/OPTION/ANY")
    parser.add_argument("--inst-family", dest="inst_family", help="orders.instFamily（交割/永续品种，例如 BTC-USD）")
    parser.add_argument("--inst-id", dest="inst_id", help="orders.instId（例如 BTC-USDT 或 BTC-USDT-SWAP）")
    parser.add_argument("--duration", type=float, default=0.0, help="登录后保持连接的秒数（0 表示不定时关闭）")
    parser.add_argument("--ws-url", default=DEFAULT_WS_URL, help="OKX 私有 WS URL")
    parser.add_argument("--keep-open", action="store_true", help="收到首条 orders 数据后不自动关闭")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    api_key, api_secret, passphrase = load_credentials()

    sub_arg: Dict[str, Any] = {"channel": "orders", "instType": args.inst_type}
    if args.inst_family:
        sub_arg["instFamily"] = args.inst_family
    if args.inst_id:
        sub_arg["instId"] = args.inst_id

    state = WSState(duration=args.duration, subscribe_args=[sub_arg], close_after_first=not args.keep_open)

    ws = websocket.WebSocketApp(
        args.ws_url,
        on_open=lambda ws: handle_open(ws, api_key, api_secret, passphrase),
        on_message=lambda ws, msg: handle_message(ws, msg, state),
        on_error=handle_error,
        on_close=handle_close,
    )

    print(f"Connecting to {args.ws_url} ...")
    ws.run_forever(ping_interval=20, ping_timeout=10)


if __name__ == "__main__":
    main()
