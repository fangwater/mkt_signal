#!/usr/bin/env python3
"""Aster futures WS smoke-test: probe @trade / @kline_1m / @depth on a single combined stream.

Doc says futures only documents @aggTrade. We send @trade anyway to see what the server
does — silently drops? error frame? actually delivers?
"""

import json
import sys
import time
from collections import defaultdict

import websocket

SYMBOL = "btcusdt"
STREAMS = [
    f"{SYMBOL}@trade",        # individual trade — undocumented for futures
    f"{SYMBOL}@kline_1m",     # documented
    f"{SYMBOL}@depth",        # documented diff depth (default 250ms)
]
URL = "wss://fstream.asterdex.com/stream?streams=" + "/".join(STREAMS)
DURATION_SECS = 30


def main() -> int:
    print(f"[connect] {URL}")
    ws = websocket.create_connection(URL, timeout=10)
    # Combined stream is auto-enabled by /stream path, no SUBSCRIBE needed.

    counts: dict[str, int] = defaultdict(int)
    samples: dict[str, dict] = {}
    errors: list[dict] = []
    deadline = time.time() + DURATION_SECS

    while time.time() < deadline:
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        ws.settimeout(min(remaining, 5))
        try:
            raw = ws.recv()
        except websocket.WebSocketTimeoutException:
            continue
        except Exception as exc:
            print(f"[recv-err] {exc!r}")
            break

        if not raw:
            continue

        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            print(f"[non-json] {raw[:120]!r}")
            continue

        # Error / ack frames have no "stream" key.
        if "stream" not in msg:
            errors.append(msg)
            print(f"[ctrl] {msg}")
            continue

        stream = msg["stream"]
        counts[stream] += 1
        samples.setdefault(stream, msg["data"])

    ws.close()

    print("\n========== summary ==========")
    for s in STREAMS:
        n = counts.get(s, 0)
        verdict = "OK" if n > 0 else "NO DATA"
        print(f"  {s:<24} -> {n:>5} msg  [{verdict}]")
    if errors:
        print(f"  control/error frames: {len(errors)}")

    print("\n========== first sample per stream ==========")
    for s in STREAMS:
        sample = samples.get(s)
        if sample is None:
            print(f"\n--- {s}: <none> ---")
            continue
        print(f"\n--- {s} ---")
        print(json.dumps(sample, indent=2)[:800])

    return 0 if counts.get(f"{SYMBOL}@trade", 0) > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
