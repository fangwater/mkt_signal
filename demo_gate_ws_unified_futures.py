#!/usr/bin/env python3
import argparse
import asyncio
import hashlib
import hmac
import json
import os
import time

import websockets


def sign_ws_api(secret: str, event: str, channel: str, req_param: str, timestamp: int) -> str:
    sign_str = f"{event}\n{channel}\n{req_param}\n{timestamp}"
    return hmac.new(secret.encode(), sign_str.encode(), hashlib.sha512).hexdigest()


def build_login_message(api_key: str, secret: str, channel: str) -> dict:
    ts_s = int(time.time())
    req_id = f"login-{int(time.time() * 1000)}"
    signature = sign_ws_api(secret, "api", channel, "", ts_s)
    return {
        "time": ts_s,
        "channel": channel,
        "event": "api",
        "payload": {
            "req_id": req_id,
            "api_key": api_key,
            "signature": signature,
            "timestamp": str(ts_s),
        },
    }


def build_order_message(channel: str, req_id: str, req_param: dict) -> dict:
    ts_s = int(time.time())
    return {
        "time": ts_s,
        "channel": channel,
        "event": "api",
        "payload": {
            "req_id": str(req_id),
            "req_param": req_param,
        },
    }


async def recv_for(ws, seconds: float) -> None:
    deadline = time.time() + seconds
    while True:
        timeout = deadline - time.time()
        if timeout <= 0:
            break
        try:
            msg = await asyncio.wait_for(ws.recv(), timeout=timeout)
        except asyncio.TimeoutError:
            break
        print(msg)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gate WS order tester (use unified URL with futures channel if needed)."
    )
    parser.add_argument(
        "--url",
        default="wss://ws.gate.com/v4/ws/unified",
        help="WebSocket URL",
    )
    parser.add_argument(
        "--login-channel",
        default="futures.login",
        help="Login channel (e.g. futures.login/spot.login/unified.login)",
    )
    parser.add_argument(
        "--order-channel",
        default="futures.order_place",
        help="Order channel (e.g. futures.order_place)",
    )
    parser.add_argument("--contract", default="APT_USDT", help="Contract symbol")
    parser.add_argument(
        "--size",
        default="-20",
        help="Signed size (string, sell is negative; keep integer for futures)",
    )
    parser.add_argument("--price", default="1.72", help="Order price as string")
    parser.add_argument("--tif", default="poc", help="Time in force")
    parser.add_argument(
        "--account",
        default="unified",
        help="Account field (optional for some channels)",
    )
    parser.add_argument(
        "--req-param-json",
        default="",
        help="Raw JSON string to override req_param",
    )
    parser.add_argument(
        "--listen-seconds",
        type=float,
        default=5.0,
        help="Seconds to listen after sending order",
    )
    return parser.parse_args()


async def main() -> int:
    args = parse_args()
    api_key = os.getenv("GATE_API_KEY", "").strip()
    secret = os.getenv("GATE_API_SECRET", "").strip()
    if not api_key or not secret:
        print("GATE_API_KEY / GATE_API_SECRET env vars are required.")
        return 1

    if args.req_param_json:
        req_param = json.loads(args.req_param_json)
    else:
        req_param = {
            "contract": args.contract,
            "price": args.price,
            "size": args.size,
            "tif": args.tif,
            "text": f"t-{int(time.time() * 1000)}",
        }
        if args.account:
            req_param["account"] = args.account

    login_msg = build_login_message(api_key, secret, args.login_channel)
    order_msg = build_order_message(args.order_channel, int(time.time() * 1000), req_param)

    async with websockets.connect(args.url, ping_interval=None) as ws:
        print(">>> login")
        await ws.send(json.dumps(login_msg))
        await recv_for(ws, 1.0)

        print(">>> order")
        await ws.send(json.dumps(order_msg))
        await recv_for(ws, args.listen_seconds)

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
