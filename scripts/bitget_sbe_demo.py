#!/usr/bin/env python3
"""
Bitget SBE 验证 demo。

同时连接 v3 SBE 端口 (`wss://ws.bitget.com/v3/ws/public/sbe`) 订阅：
  - books1     (templateId 1002, BBO)
  - publicTrade(templateId 1003, Trade batch)

并行连接 v3 JSON 端口 (`wss://ws.bitget.com/v3/ws/public`) 订阅 books1，
按 (symbol, seqId) 配对 SBE/JSON 两侧 books1 帧，打印字段差异 + 两档延迟。

不需要鉴权 (公开行情)。心跳 text "ping"。
schema 来源: https://www.bitget.com/api-doc/uta/sbe/sbe-bbo
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import struct
import sys
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

import websockets

WS_SBE_URL = "wss://ws.bitget.com/v3/ws/public/sbe"
WS_JSON_URL = "wss://ws.bitget.com/v3/ws/public"

DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
DEFAULT_INST_TYPE = "usdt-futures"

# SBE header: blockLength u16, templateId u16, schemaId u16, version u16
SBE_HEADER = struct.Struct("<HHHH")
assert SBE_HEADER.size == 8

# BBO root (templateId=1002, schemaVer=3): 实测 blockLength=64
#   ts u64, bid1Px i64, bid1Sz i64, ask1Px i64, ask1Sz i64,
#   px_exp i8, sz_exp i8, seq u64, sts u64, category u8  = 59 bytes (no padding)
# blockLength=64 含 5B padding 到对齐，symbol 紧跟其后 (varString8)
SBE_BBO_ROOT = struct.Struct("<QqqqqbbQQB")
assert SBE_BBO_ROOT.size == 59

# Trade root (templateId=1003, schemaVer=3): 实测 blockLength=16
#   px_exp i8, sz_exp i8, sts u64 (gateway 推送时刻),  + 6B mystery/padding
# Trade group: entryBlockLength=40, entry = ts u64, exec_id u64, price i64, size i64, side u8 + 7B pad
SBE_TRADE_ENTRY = struct.Struct("<QQqqB")  # 33 bytes; entryBlockLength=40 含 7B padding


@dataclass
class Bbo:
    source: str           # "json" or "sbe"
    symbol: str
    seq_id: int
    ts_us: int            # SBE: sts (gateway 推送); JSON: ts*1000
    sbe_quote_ts_us: Optional[int] = None   # SBE 独有: ts (撮合时刻)
    recv_us: int = 0
    bid_px: float = 0.0
    ask_px: float = 0.0
    bid_sz: float = 0.0
    ask_sz: float = 0.0
    raw_bytes: int = 0


@dataclass
class TradeBatch:
    symbol: str
    recv_us: int
    n_trades: int
    first_ts_us: int
    first_sts_us: int
    first_price: float
    first_size: float
    first_side: int      # 0=Buy 1=Sell
    raw_bytes: int


def now_us() -> int:
    return time.time_ns() // 1000


def mantissa(m: int, exp: int) -> float:
    return float(m) * (10.0 ** exp)


def parse_sbe_bbo(frame: bytes, recv_us: int) -> Optional[Bbo]:
    if len(frame) < SBE_HEADER.size:
        raise ValueError(f"frame too short: {len(frame)}")
    bl, tid, sid, ver = SBE_HEADER.unpack_from(frame, 0)
    if sid != 1:
        raise ValueError(f"unexpected schemaId={sid}")
    if tid != 1002:
        return None
    body_off = SBE_HEADER.size
    if len(frame) < body_off + bl:
        raise ValueError(f"truncated: have {len(frame)} need {body_off + bl}")
    ts, b1p, b1s, a1p, a1s, pe, se, seq, sts, category = SBE_BBO_ROOT.unpack_from(frame, body_off)
    # 跳到 blockLength 之后 (含 padding)
    sym_off = body_off + bl
    sym_len = frame[sym_off]
    symbol = frame[sym_off + 1: sym_off + 1 + sym_len].decode("utf-8", errors="replace")
    return Bbo(
        source="sbe",
        symbol=symbol,
        seq_id=seq,
        ts_us=sts,            # gateway 推送时刻
        sbe_quote_ts_us=ts,   # 撮合时刻
        recv_us=recv_us,
        bid_px=mantissa(b1p, pe),
        ask_px=mantissa(a1p, pe),
        bid_sz=mantissa(b1s, se),
        ask_sz=mantissa(a1s, se),
        raw_bytes=len(frame),
    )


def parse_sbe_trade(frame: bytes, recv_us: int) -> Optional[TradeBatch]:
    if len(frame) < SBE_HEADER.size:
        raise ValueError(f"trade frame too short: {len(frame)}")
    bl, tid, sid, ver = SBE_HEADER.unpack_from(frame, 0)
    if sid != 1:
        raise ValueError(f"trade unexpected schemaId={sid}")
    if tid != 1003:
        return None
    body_off = SBE_HEADER.size
    # root: px_exp(1) + sz_exp(1) + sts u64(8) at offset body_off+2
    px_exp = struct.unpack_from("<b", frame, body_off)[0]
    sz_exp = struct.unpack_from("<b", frame, body_off + 1)[0]
    sts = struct.unpack_from("<Q", frame, body_off + 2)[0]
    # 跳过 root padding 到 group header
    off = body_off + bl
    entry_bl, num = struct.unpack_from("<HH", frame, off)
    off += 4
    if num == 0:
        return None
    ts, exec_id, price_m, size_m, side = SBE_TRADE_ENTRY.unpack_from(frame, off)
    # 跳过整个 group 到 symbol
    sym_off = off + entry_bl * num
    sym_len = frame[sym_off]
    symbol = frame[sym_off + 1: sym_off + 1 + sym_len].decode("utf-8", errors="replace")
    return TradeBatch(
        symbol=symbol,
        recv_us=recv_us,
        n_trades=num,
        first_ts_us=ts,
        first_sts_us=sts,
        first_price=mantissa(price_m, px_exp),
        first_size=mantissa(size_m, sz_exp),
        first_side=side,
        raw_bytes=len(frame),
    )


def parse_json_bbo(msg: str, recv_us: int) -> list[Bbo]:
    val = json.loads(msg)
    arg = val.get("arg") or {}
    if arg.get("topic") != "books1":
        return []
    symbol = (arg.get("symbol") or "").upper()
    data = val.get("data") or []
    out: list[Bbo] = []
    for entry in data:
        seq = int(entry["seq"])
        ts_ms = int(entry["ts"])
        bid = entry["b"][0]
        ask = entry["a"][0]
        out.append(Bbo(
            source="json",
            symbol=symbol,
            seq_id=seq,
            ts_us=ts_ms * 1000,
            recv_us=recv_us,
            bid_px=float(bid[0]),
            bid_sz=float(bid[1]),
            ask_px=float(ask[0]),
            ask_sz=float(ask[1]),
            raw_bytes=len(msg),
        ))
    return out


def build_subscribe(inst_type: str, topic: str, symbols: list[str]) -> str:
    return json.dumps({
        "op": "subscribe",
        "args": [{"instType": inst_type, "topic": topic, "symbol": s} for s in symbols],
    })


async def ping_loop(ws):
    try:
        while True:
            await asyncio.sleep(20)
            await ws.send("ping")
    except asyncio.CancelledError:
        return


async def ws_sbe_task(symbols: list[str], inst_type: str,
                     bbo_q: asyncio.Queue, trade_q: asyncio.Queue,
                     stop: asyncio.Event):
    async with websockets.connect(WS_SBE_URL, max_size=2**24, compression=None) as ws:
        # 一并订阅 books1 + publicTrade
        await ws.send(build_subscribe(inst_type, "books1", symbols))
        await ws.send(build_subscribe(inst_type, "publicTrade", symbols))
        ka = asyncio.create_task(ping_loop(ws))
        try:
            while not stop.is_set():
                msg = await ws.recv()
                recv = now_us()
                if isinstance(msg, str):
                    if msg == "pong":
                        continue
                    try:
                        ev = json.loads(msg)
                    except json.JSONDecodeError:
                        logging.warning("SBE non-json text: %s", msg[:200])
                        continue
                    if ev.get("event") == "subscribe":
                        logging.info("SBE subscribed: %s", ev.get("arg"))
                    elif ev.get("event") == "error":
                        logging.error("SBE error: %s", msg[:300])
                    else:
                        logging.info("SBE text: %s", msg[:200])
                    continue
                # binary
                if len(msg) < SBE_HEADER.size:
                    logging.warning("short SBE frame: %d", len(msg))
                    continue
                tid = struct.unpack_from("<H", msg, 2)[0]
                try:
                    if tid == 1002:
                        bbo = parse_sbe_bbo(msg, recv)
                        if bbo is not None:
                            await bbo_q.put(bbo)
                    elif tid == 1003:
                        tb = parse_sbe_trade(msg, recv)
                        if tb is not None:
                            await trade_q.put(tb)
                    else:
                        logging.debug("SBE unknown templateId=%d", tid)
                except Exception as e:
                    logging.warning("SBE decode err (tid=%d): %s len=%d hex=%s",
                                    tid, e, len(msg), msg[:32].hex())
        finally:
            ka.cancel()


async def ws_json_task(symbols: list[str], inst_type: str, bbo_q: asyncio.Queue, stop: asyncio.Event):
    async with websockets.connect(WS_JSON_URL, max_size=2**24, compression=None) as ws:
        await ws.send(build_subscribe(inst_type, "books1", symbols))
        ka = asyncio.create_task(ping_loop(ws))
        try:
            while not stop.is_set():
                msg = await ws.recv()
                recv = now_us()
                if isinstance(msg, bytes):
                    logging.warning("JSON conn got binary, ignoring")
                    continue
                if msg == "pong":
                    continue
                try:
                    bbos = parse_json_bbo(msg, recv)
                except (KeyError, ValueError, json.JSONDecodeError) as e:
                    try:
                        val = json.loads(msg)
                        if isinstance(val, dict) and ("event" in val or "code" in val):
                            logging.info("JSON control: %s", msg[:200])
                            continue
                    except json.JSONDecodeError:
                        pass
                    logging.warning("JSON parse err: %s msg=%r", e, msg[:200])
                    continue
                for b in bbos:
                    await bbo_q.put(b)
        finally:
            ka.cancel()


def histogram(samples: list[int], label: str):
    if not samples:
        print(f"# {label}: <no samples>")
        return
    xs = sorted(samples)
    n = len(xs)
    def pct(p): return xs[min(n - 1, int(n * p))]
    print(f"# {label}: n={n} min={xs[0]} p25={pct(.25)} p50={pct(.5)} "
          f"p75={pct(.75)} p95={pct(.95)} p99={pct(.99)} max={xs[-1]}", flush=True)


async def comparator(bbo_q: asyncio.Queue, trade_q: asyncio.Queue,
                    stop: asyncio.Event, pair_timeout_s: float = 5.0):
    pending: Dict[Tuple[str, int], Bbo] = {}
    pending_ts: Dict[Tuple[str, int], float] = {}
    stats = {"matched": 0, "mismatched": 0, "json_only": 0, "sbe_only": 0,
             "trade_frames": 0, "trade_entries": 0}
    recv_deltas: list[int] = []
    sbe_wire: list[int] = []        # SBE recv - sts (推送→本地)
    sbe_quote: list[int] = []       # SBE recv - ts  (撮合→本地)
    sbe_gw: list[int] = []          # sts - ts (gateway 内部)
    json_wire: list[int] = []       # JSON recv - ts (现行口径)
    trade_wire: list[int] = []      # Trade recv - first.sts
    trade_quote: list[int] = []     # Trade recv - first.ts
    last_gc = time.monotonic()

    def fields_equal(sbe: Bbo, jsn: Bbo) -> Tuple[bool, list[str]]:
        diffs: list[str] = []
        def almost(x, y):
            return abs(x - y) <= max(abs(x), abs(y)) * 1e-9 + 1e-12
        if not almost(sbe.bid_px, jsn.bid_px):
            diffs.append(f"bid_px {sbe.bid_px} vs {jsn.bid_px}")
        if not almost(sbe.ask_px, jsn.ask_px):
            diffs.append(f"ask_px {sbe.ask_px} vs {jsn.ask_px}")
        if not almost(sbe.bid_sz, jsn.bid_sz):
            diffs.append(f"bid_sz {sbe.bid_sz} vs {jsn.bid_sz}")
        if not almost(sbe.ask_sz, jsn.ask_sz):
            diffs.append(f"ask_sz {sbe.ask_sz} vs {jsn.ask_sz}")
        return (not diffs, diffs)

    print("# cols: sym/seq | recv_sbe-recv_json | wireLat(recv-sts) | quoteLat(recv-ts) | "
          "gwLat(sts-ts) | bid×sz @ ask×sz | json/sbe bytes | match", flush=True)

    while not stop.is_set():
        bbo: Optional[Bbo] = None
        tb: Optional[TradeBatch] = None
        try:
            bbo = await asyncio.wait_for(bbo_q.get(), timeout=0.3)
        except asyncio.TimeoutError:
            pass
        try:
            tb = trade_q.get_nowait()
        except asyncio.QueueEmpty:
            pass

        if tb is not None:
            stats["trade_frames"] += 1
            stats["trade_entries"] += tb.n_trades
            trade_wire.append(tb.recv_us - tb.first_sts_us)
            trade_quote.append(tb.recv_us - tb.first_ts_us)

        if bbo is not None:
            key = (bbo.symbol, bbo.seq_id)
            other = pending.pop(key, None)
            pending_ts.pop(key, None)
            if other is None:
                pending[key] = bbo
                pending_ts[key] = time.monotonic()
            else:
                sbe = bbo if bbo.source == "sbe" else other
                jsn = bbo if bbo.source == "json" else other
                ok, diffs = fields_equal(sbe, jsn)
                if ok: stats["matched"] += 1
                else: stats["mismatched"] += 1
                recv_deltas.append(sbe.recv_us - jsn.recv_us)
                sbe_wire.append(sbe.recv_us - sbe.ts_us)
                if sbe.sbe_quote_ts_us:
                    sbe_quote.append(sbe.recv_us - sbe.sbe_quote_ts_us)
                    sbe_gw.append(sbe.ts_us - sbe.sbe_quote_ts_us)
                json_wire.append(jsn.recv_us - jsn.ts_us)
                qlat = sbe.recv_us - (sbe.sbe_quote_ts_us or 0) if sbe.sbe_quote_ts_us else 0
                wlat = sbe.recv_us - sbe.ts_us
                glat = (sbe.ts_us - sbe.sbe_quote_ts_us) if sbe.sbe_quote_ts_us else 0
                d = sbe.recv_us - jsn.recv_us
                mark = "OK " if ok else "DIFF"
                print(
                    f"{bbo.symbol:10s} seq={bbo.seq_id:>20d} | "
                    f"{'-' if d<0 else '+'}{abs(d):>6d}µs | "
                    f"w={wlat:>6d} q={qlat:>6d} gw={glat:>5d} | "
                    f"{sbe.bid_px:>10.4f}×{sbe.bid_sz:<8.4f} @ {sbe.ask_px:>10.4f}×{sbe.ask_sz:<8.4f} | "
                    f"json={jsn.raw_bytes:>4d}B sbe={sbe.raw_bytes:>3d}B | {mark}"
                    + (f" ::: {';'.join(diffs)}" if diffs else ""),
                    flush=True,
                )

        now = time.monotonic()
        if now - last_gc > 2.0:
            stale = [k for k, t in pending_ts.items() if now - t > pair_timeout_s]
            for k in stale:
                lone = pending.pop(k)
                pending_ts.pop(k, None)
                if lone.source == "json":
                    stats["json_only"] += 1
                else:
                    stats["sbe_only"] += 1
            last_gc = now

    print(f"\n# summary: matched={stats['matched']} mismatched={stats['mismatched']} "
          f"json_only={stats['json_only']} sbe_only={stats['sbe_only']}", flush=True)
    print(f"# trade SBE: frames={stats['trade_frames']} entries={stats['trade_entries']}", flush=True)
    histogram(recv_deltas, "sbe_recv - json_recv µs (负=SBE 先到)")
    histogram(sbe_wire,    "wireLat (BBO)   = recv - SBE.sts µs (≈ binance E)")
    histogram(sbe_quote,   "quoteLat(BBO)   = recv - SBE.ts  µs (≈ binance T, SBE 独有)")
    histogram(sbe_gw,      "gwLat   (BBO)   = sts - ts       µs (交易所内部 gateway)")
    histogram(json_wire,   "jsonWireLat     = recv - JSON.ts µs (现行 spread_pbs bitget 测的)")
    histogram(trade_wire,  "wireLat (Trade) = recv - first.sts µs")
    histogram(trade_quote, "quoteLat(Trade) = recv - first.ts  µs")


async def amain(symbols: list[str], inst_type: str, duration_s: float):
    bbo_q: asyncio.Queue = asyncio.Queue(maxsize=4096)
    trade_q: asyncio.Queue = asyncio.Queue(maxsize=4096)
    stop = asyncio.Event()

    sbe_t = asyncio.create_task(ws_sbe_task(symbols, inst_type, bbo_q, trade_q, stop))
    json_t = asyncio.create_task(ws_json_task(symbols, inst_type, bbo_q, stop))
    cmp_t = asyncio.create_task(comparator(bbo_q, trade_q, stop))

    async def stopper():
        await asyncio.sleep(duration_s)
        stop.set()
        sbe_t.cancel()
        json_t.cancel()

    timer = asyncio.create_task(stopper()) if duration_s > 0 else None

    done, pending = await asyncio.wait({sbe_t, json_t, cmp_t}, return_when=asyncio.FIRST_EXCEPTION)
    stop.set()
    for t in pending:
        t.cancel()
    if timer:
        timer.cancel()
    for t in done:
        if t.cancelled():
            continue
        exc = t.exception()
        if exc and not isinstance(exc, asyncio.CancelledError):
            logging.error("task crashed: %r", exc)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS)
    p.add_argument("--inst-type", default=DEFAULT_INST_TYPE,
                   choices=["spot", "usdt-futures", "coin-futures"])
    p.add_argument("--duration", type=float, default=30.0)
    p.add_argument("-v", "--verbose", action="count", default=0)
    args = p.parse_args()
    level = logging.WARNING - 10 * args.verbose
    logging.basicConfig(level=max(level, logging.DEBUG),
                        format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(amain(args.symbols, args.inst_type, args.duration))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    sys.exit(main() or 0)
