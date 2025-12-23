#!/usr/bin/env python3
"""订阅 Gate.io 私有 WS orders 频道，同时订阅 spot.orders_v2 和 futures.orders。

用法：
  export GATE_API_KEY="your_api_key"
  export GATE_API_SECRET="your_api_secret"
  python scripts/gate_orders_ws.py

默认使用 !all 订阅全部交易对。
"""

from __future__ import annotations

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

# Gate WebSocket URLs
GATE_SPOT_WS_URL = "wss://api.gateio.ws/ws/v4/"
GATE_FUTURES_WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"


def gen_sign(channel: str, event: str, timestamp: int, secret: str) -> str:
    """生成 Gate.io WebSocket 签名。

    签名格式: HMAC-SHA512(channel=xxx&event=xxx&time=xxx, secret)
    """
    message = f"channel={channel}&event={event}&time={timestamp}"
    h = hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha512)
    return h.hexdigest()


def load_credentials() -> tuple[str, str]:
    """加载 Gate API 凭证。"""
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


class GateWSClient:
    """Gate.io WebSocket 客户端。"""

    def __init__(
        self,
        ws_url: str,
        channel: str,
        api_key: str,
        api_secret: str,
        pairs: List[str],
    ):
        self.ws_url = ws_url
        self.channel = channel
        self.api_key = api_key
        self.api_secret = api_secret
        self.pairs = pairs
        self.ws: Optional[websocket.WebSocketApp] = None

    def build_subscribe_request(self) -> Dict[str, Any]:
        """构建订阅请求。"""
        timestamp = int(time.time())
        sign = gen_sign(self.channel, "subscribe", timestamp, self.api_secret)

        return {
            "time": timestamp,
            "channel": self.channel,
            "event": "subscribe",
            "payload": self.pairs,
            "auth": {
                "method": "api_key",
                "KEY": self.api_key,
                "SIGN": sign,
            }
        }

    def on_open(self, ws) -> None:
        """连接成功后发送订阅请求。"""
        print(f"[{self.channel}] Connected to {self.ws_url}")
        req = self.build_subscribe_request()
        print(f"[{self.channel}] Subscribe request: {json.dumps(req, ensure_ascii=False)}")
        ws.send(json.dumps(req))

    def on_message(self, ws, raw_msg: str) -> None:
        """处理收到的消息。"""
        if isinstance(raw_msg, bytes):
            raw_msg = raw_msg.decode("utf-8", "replace")
        print(f"[{self.channel}] <<< {raw_msg}")

    def on_error(self, ws, error) -> None:
        print(f"[{self.channel}] Error: {error}", file=sys.stderr)

    def on_close(self, ws, close_status_code, close_msg) -> None:
        print(f"[{self.channel}] Closed code={close_status_code} reason={close_msg}")

    def run(self) -> None:
        """启动 WebSocket 连接。"""
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        print(f"[{self.channel}] Connecting to {self.ws_url} ...")
        self.ws.run_forever(ping_interval=20, ping_timeout=10)


def main() -> None:
    api_key, api_secret = load_credentials()
    pairs = ["!all"]

    print("Gate.io Orders WebSocket Test")
    print(f"Subscribing: spot.orders_v2 + futures.orders")
    print(f"Pairs: {pairs}")
    print("-" * 50)

    # 现货订单 spot.orders_v2
    spot_client = GateWSClient(
        ws_url=GATE_SPOT_WS_URL,
        channel="spot.orders_v2",
        api_key=api_key,
        api_secret=api_secret,
        pairs=pairs,
    )
    spot_thread = threading.Thread(target=spot_client.run, daemon=True)
    spot_thread.start()

    # 期货订单 futures.orders
    futures_client = GateWSClient(
        ws_url=GATE_FUTURES_WS_URL,
        channel="futures.orders",
        api_key=api_key,
        api_secret=api_secret,
        pairs=pairs,
    )
    futures_thread = threading.Thread(target=futures_client.run, daemon=True)
    futures_thread.start()

    # 等待
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nCtrl+C, exiting...")


if __name__ == "__main__":
    main()
