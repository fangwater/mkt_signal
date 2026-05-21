#!/usr/bin/env python3
"""订阅 OKX positions 频道，打印首条快照后退出。"""

import argparse
import base64
import hashlib
import hmac
import json
import os
import sys
import threading
import time
import urllib.request
from typing import Any, Dict, Optional

try:
    import websocket  # type: ignore
except ImportError:
    print("缺少依赖：请先安装 websocket-client (pip install websocket-client)", file=sys.stderr)
    sys.exit(1)

DEFAULT_WS_URL = os.environ.get("OKX_WS_PRIVATE_URL", "wss://ws.okx.com:8443/ws/v5/private")
DEFAULT_REST_URL = os.environ.get("OKX_BASE_URL", "https://openapi.okx.com")


def ts_sec_local() -> str:
    """返回秒级时间戳字符串（带毫秒小数，符合 OKX 要求）。"""
    return f"{time.time():.3f}"


def sign_login(ts: str, secret: str) -> str:
    payload = f"{ts}GET/users/self/verify"
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def fetch_server_ts(base_url: str, timeout: float = 5.0) -> Optional[str]:
    """调用 /public/time 获取秒级时间戳字符串（带毫秒小数），失败返回 None。"""
    url = f"{base_url.rstrip('/')}/api/v5/public/time"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", "replace")
            data = json.loads(body)
            ts_str = (data.get("data") or [{}])[0].get("ts")
            if not ts_str:
                return None
            # /public/time 返回 13 位毫秒，需要转为秒级字符串
            ts_ms = float(ts_str)
            ts_sec = ts_ms / 1000.0
            return f"{ts_sec:.3f}"
    except Exception as exc:  # pragma: no cover - 网络异常
        print(f"Warning: 获取服务器时间失败: {exc}", file=sys.stderr)
        return None


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
    def __init__(self, duration: float, subscribe_args: list[Dict[str, Any]]) -> None:
        self.positions_printed = False
        self.duration = duration
        self.subscribe_args = subscribe_args
        self.close_timer: Optional[threading.Timer] = None

    def schedule_close(self, ws) -> None:
        if self.close_timer is None:
            self.close_timer = threading.Timer(self.duration, lambda: ws.close())
            self.close_timer.daemon = True
            self.close_timer.start()


def send_json(ws, payload: Dict[str, Any]) -> None:
    ws.send(json.dumps(payload))


def handle_open(ws, api_key: str, api_secret: str, passphrase: str, ts_override: Optional[str]) -> None:
    ts = ts_override or ts_sec_local()
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
        print("Login success, subscribing positions ...")
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
    if channel == "positions" and not state.positions_printed:
        data = msg.get("data") or []
        if data:
            print("=== positions snapshot ===")
            print(json.dumps(data, indent=2, ensure_ascii=False))
            state.positions_printed = True
        return

    if channel:
        print(f"Unhandled channel message: {msg}")


def handle_error(_ws, error) -> None:
    print(f"WebSocket error: {error}", file=sys.stderr)


def handle_close(_ws, close_status_code, close_msg) -> None:
    print(f"WebSocket closed code={close_status_code} reason={close_msg}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OKX positions WS 订阅脚本，打印首条快照后退出")
    parser.add_argument("--inst-type", dest="inst_type", default="ANY", help="positions.instType，例如 FUTURES/ANY/MARGIN/SWAP")
    parser.add_argument("--inst-family", dest="inst_family", help="positions.instFamily，例如 BTC-USD")
    parser.add_argument(
        "--update-interval",
        dest="update_interval",
        default="0",
        help='positions.extraParams.updateInterval，合法值 0/2000/3000/4000；默认 "0"',
    )
    parser.add_argument("--duration", type=float, default=5.0, help="登录后保持连接的秒数")
    parser.add_argument("--ws-url", default=DEFAULT_WS_URL, help="自定义 OKX 私有 WS URL")
    parser.add_argument("--base-url", default=DEFAULT_REST_URL, help="OKX REST 基础 URL，用于获取服务器时间")
    parser.add_argument("--local-time", action="store_true", help="强制使用本地时间签名（默认优先用服务器时间）")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    api_key, api_secret, passphrase = load_credentials()

    pos_arg: Dict[str, Any] = {"channel": "positions", "instType": args.inst_type}
    if args.inst_family:
        pos_arg["instFamily"] = args.inst_family
    if args.update_interval is not None:
        extra = {"updateInterval": str(args.update_interval)}
        pos_arg["extraParams"] = json.dumps(extra, separators=(",", ":"), ensure_ascii=False)
    subscribe_args = [pos_arg]

    server_ts = None
    if not args.local_time:
        server_ts = fetch_server_ts(args.base_url)
        if server_ts:
            print(f"使用服务器时间（秒）: {server_ts}")
        else:
            print("服务器时间获取失败，改用本地时间。")

    state = WSState(duration=args.duration, subscribe_args=subscribe_args)

    ws_app = websocket.WebSocketApp(
        args.ws_url,
        on_open=lambda ws: handle_open(ws, api_key, api_secret, passphrase, server_ts),
        on_message=lambda ws, msg: handle_message(ws, msg, state),
        on_error=handle_error,
        on_close=handle_close,
    )

    try:
        ws_app.run_forever(ping_interval=20, ping_timeout=10)
    except KeyboardInterrupt:
        print("Interrupted, closing...", file=sys.stderr)


if __name__ == "__main__":
    main()
