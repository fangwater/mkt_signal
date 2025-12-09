#!/usr/bin/env python3
"""订阅 OKX balance_and_position 频道，打印首条快照后退出。"""

import argparse
import base64
import hashlib
import hmac
import json
import os
import sys
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional
import urllib.error
import urllib.request

try:
    import websocket  # type: ignore
except ImportError:
    print("Missing dependency: websocket-client. Install with `pip install websocket-client`.", file=sys.stderr)
    sys.exit(1)

DEFAULT_WS_URL = os.environ.get("OKX_WS_PRIVATE_URL", "wss://ws.okx.com:8443/ws/v5/private")
DEFAULT_REST_URL = os.environ.get("OKX_BASE_URL", "https://www.okx.com")


def utc_timestamp() -> str:
    """返回秒级时间戳字符串（含毫秒小数），用于 OKX 签名。"""
    ts = datetime.now(timezone.utc).timestamp()
    return f"{ts:.3f}"


def sign_login(ts: str, secret: str) -> str:
    payload = f"{ts}GET/users/self/verify"
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


class WSState:
    def __init__(self, duration: float) -> None:
        self.balance_printed = False
        self.positions_printed = False
        self.duration = duration
        self.close_timer: Optional[threading.Timer] = None
        self.subscribe_args: list[Dict[str, Any]] = []

    def schedule_close(self, ws) -> None:
        if self.close_timer is None:
            self.close_timer = threading.Timer(self.duration, lambda: close_ws(ws))
            self.close_timer.daemon = True
            self.close_timer.start()


def load_credentials() -> tuple[str, str, str]:
    api_key = os.environ.get("OKX_API_KEY", "").strip()
    api_secret = os.environ.get("OKX_API_SECRET", "").strip()
    passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()
    missing = [name for name, value in [("OKX_API_KEY", api_key), ("OKX_API_SECRET", api_secret), ("OKX_PASSPHRASE", passphrase)] if not value]
    if missing:
        print(f"Please set environment variables: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret, passphrase


def close_ws(ws) -> None:
    print("Closing websocket after duration elapsed...", flush=True)
    ws.close()


def send_json(ws, payload: Dict[str, Any]) -> None:
    ws.send(json.dumps(payload))


def fetch_server_timestamp(base_url: str, timeout: float = 5.0) -> tuple[Optional[str], Optional[str]]:
    """调用 /public/time 获取服务器时间；返回 (秒级时间戳字符串, 预览ISO)"""
    url = f"{base_url.rstrip('/')}/api/v5/public/time"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", "replace")
            data = json.loads(body)
            ts_str = (data.get("data") or [{}])[0].get("ts")
            if not ts_str:
                return None, None
            ts_ms = float(ts_str)
            ts_seconds = ts_ms / 1000.0
            ts_seconds_str = f"{ts_seconds:.3f}"
            iso_preview = datetime.fromtimestamp(ts_seconds, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            return ts_seconds_str, iso_preview
    except Exception as exc:  # pragma: no cover - network issue
        print(f"Warning: 获取服务器时间失败: {exc}", file=sys.stderr)
        return None, None


def handle_open(ws, api_key: str, api_secret: str, passphrase: str, ts_override: Optional[str]) -> None:
    ts = ts_override or utc_timestamp()
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
    print(f"Connecting to OKX private WS, sending login at {ts}")
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
        print("Login success, subscribing ...")
        send_json(ws, {"op": "subscribe", "args": state.subscribe_args})
        state.schedule_close(ws)
        return

    if event == "subscribe":
        print(f"Subscribed: {msg.get('arg')}")
        return

    arg = msg.get("arg", {})
    channel = arg.get("channel")
    if channel == "balance_and_position" and not state.balance_printed:
        data = msg.get("data") or []
        if data:
            print("=== balance_and_position snapshot ===")
            print(json.dumps(data[0], indent=2, ensure_ascii=False))
            state.balance_printed = True
        return

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
    parser = argparse.ArgumentParser(description="快速订阅 OKX balance_and_position 频道并打印首条快照")
    parser.add_argument("--duration", type=float, default=5.0, help="登录后保持连接的秒数")
    parser.add_argument("--ws-url", default=DEFAULT_WS_URL, help="自定义 OKX 私有 WS URL")
    parser.add_argument("--base-url", default=DEFAULT_REST_URL, help="OKX REST 基础 URL，用于获取服务器时间")
    parser.add_argument("--use-server-time", action="store_true", help="调用 /public/time，用服务器时间签名；默认用本地时间")
    parser.add_argument("--positions", action="store_true", help="额外订阅 positions 频道")
    parser.add_argument("--pos-inst-type", dest="pos_inst_type", help="positions.instType，例如 FUTURES/ANY/MARGIN/SWAP")
    parser.add_argument("--pos-inst-family", dest="pos_inst_family", help="positions.instFamily，例如 BTC-USD")
    parser.add_argument(
        "--pos-update-interval",
        dest="pos_update_interval",
        help='positions.extraParams.updateInterval，设置为 "0" 获取最快推送',
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    api_key, api_secret, passphrase = load_credentials()
    state = WSState(duration=args.duration)
    # 构造订阅参数
    state.subscribe_args: list[Dict[str, Any]] = [{"channel": "balance_and_position"}]
    if args.positions:
        pos_arg: Dict[str, Any] = {"channel": "positions"}
        if args.pos_inst_type:
            pos_arg["instType"] = args.pos_inst_type
        if args.pos_inst_family:
            pos_arg["instFamily"] = args.pos_inst_family
        extra: Dict[str, Any] = {}
        if args.pos_update_interval is not None:
            extra["updateInterval"] = str(args.pos_update_interval)
        if extra:
            pos_arg["extraParams"] = json.dumps(extra, separators=(",", ":"), ensure_ascii=False)
        state.subscribe_args.append(pos_arg)

    server_ts = None
    server_ts_iso = None
    if args.use_server_time:
        server_ts, server_ts_iso = fetch_server_timestamp(args.base_url)
        if server_ts:
            preview = server_ts_iso or server_ts
            print(f"使用服务器时间: {preview} (timestamp={server_ts})")
        else:
            print("服务器时间获取失败，改用本地时间。")

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
