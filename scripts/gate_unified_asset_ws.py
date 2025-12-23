#!/usr/bin/env python3
"""订阅 Gate.io 统一账户资产详情 unified.asset_detail。

用法：
  export GATE_API_KEY="your_api_key"
  export GATE_API_SECRET="your_api_secret"
  python scripts/gate_unified_asset_ws.py
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
import time

try:
    import websocket  # type: ignore
except ImportError:
    print("缺少依赖：pip install websocket-client", file=sys.stderr)
    sys.exit(1)

GATE_UNIFIED_WS_URL = "wss://ws.gate.com/v4/ws/unified"


def gen_sign(channel: str, event: str, timestamp: int, secret: str) -> str:
    message = f"channel={channel}&event={event}&time={timestamp}"
    h = hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha512)
    return h.hexdigest()


def load_credentials() -> tuple[str, str]:
    api_key = os.environ.get("GATE_API_KEY", "").strip()
    api_secret = os.environ.get("GATE_API_SECRET", "").strip()
    missing = [name for name, value in [
        ("GATE_API_KEY", api_key),
        ("GATE_API_SECRET", api_secret)
    ] if not value]
    if missing:
        print(f"请设置环境变量: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret


def main() -> None:
    api_key, api_secret = load_credentials()
    channel = "unified.asset_detail"

    print(f"Gate.io Unified Asset WebSocket Test")
    print(f"URL: {GATE_UNIFIED_WS_URL}")
    print(f"Channel: {channel}")
    print("-" * 50)

    def on_open(ws):
        print(f"Connected to {GATE_UNIFIED_WS_URL}")
        timestamp = int(time.time())
        sign = gen_sign(channel, "subscribe", timestamp, api_secret)
        req = {
            "time": timestamp,
            "channel": channel,
            "event": "subscribe",
            "payload": ["!all"],
            "auth": {
                "method": "api_key",
                "KEY": api_key,
                "SIGN": sign,
            }
        }
        print(f">>> {json.dumps(req, ensure_ascii=False)}")
        ws.send(json.dumps(req))

    def on_message(ws, msg):
        if isinstance(msg, bytes):
            msg = msg.decode("utf-8", "replace")
        print(f"<<< {msg}")

    def on_error(ws, error):
        print(f"Error: {error}", file=sys.stderr)

    def on_close(ws, code, reason):
        print(f"Closed code={code} reason={reason}")

    ws = websocket.WebSocketApp(
        GATE_UNIFIED_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever(ping_interval=20, ping_timeout=10)


if __name__ == "__main__":
    main()
