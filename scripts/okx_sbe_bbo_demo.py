#!/usr/bin/env python3
"""
OKX SBE vs JSON bbo-tbt 对照 demo。

同时连接两条 ws：
  - wss://ws.okx.com:8443/ws/v5/public          (现有 JSON 通路)
  - wss://ws.okx.com:8443/ws/v5/public-sbe      (新的 SBE 通路, 二进制)

两条都订阅同一批 inst_id 的 bbo-tbt。SBE 帧手写 struct 解码 (templateId=1000)。
按 (symbol, seqId) 配对后打印字段差异 + 两侧到达时刻的 µs 差。

依赖: pip install --user websockets aiohttp (已在 demo 环境装好)
schema 参考: https://www.okx.com/docs-v5/log_en/xml/okx_sbe_1_0.xml
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import struct
import sys
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import aiohttp
import websockets

INSTRUMENTS_URL = "https://www.okx.com/api/v5/public/instruments"
WS_JSON_URL = "wss://ws.okx.com:8443/ws/v5/public"
WS_SBE_URL = "wss://ws.okx.com:8443/ws/v5/public-sbe"

DEFAULT_SYMBOLS = ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"]
DEFAULT_INST_TYPE = "SWAP"

SBE_SCHEMA_ID = 1
SBE_TEMPLATE_BBO_TBT = 1000

# Header: blockLength u16, templateId u16, schemaId u16, version u16
SBE_HEADER = struct.Struct("<HHHH")
# bbo-tbt body (template 1000): 8*int64 + 2*int32 + 2*int8 = 74 bytes
SBE_BBO_BODY = struct.Struct("<qqqqqqqqiibb")
assert SBE_HEADER.size == 8 and SBE_BBO_BODY.size == 74


@dataclass
class Bbo:
    source: str           # "json" or "sbe"
    symbol: str
    seq_id: int
    exch_ts_us: int       # SBE: tsUs (生成时刻);  JSON: ts(ms)*1000 (发出时刻, ms 精度)
    recv_us: int          # local recv timestamp before decode (µs)
    bid_px: float
    ask_px: float
    bid_sz: float
    ask_sz: float
    out_time_us: Optional[int] = None   # SBE 独有: gateway 发出时刻 (µs); JSON 没有
    bid_ord_count: Optional[int] = None
    ask_ord_count: Optional[int] = None
    raw_bytes: int = 0    # size of raw payload (chars for JSON, bytes for SBE)


def now_us() -> int:
    return time.time_ns() // 1000


async def fetch_inst_id_codes(symbols: list[str], inst_type: str) -> Dict[int, str]:
    """REST 拉一次 instruments，建 instIdCode → instId 映射 (只保留我们关心的)。"""
    want = set(symbols)
    async with aiohttp.ClientSession() as s:
        async with s.get(INSTRUMENTS_URL, params={"instType": inst_type}) as r:
            r.raise_for_status()
            payload = await r.json()
    if payload.get("code") != "0":
        raise RuntimeError(f"instruments fetch failed: {payload}")
    mapping: Dict[int, str] = {}
    for row in payload["data"]:
        inst_id = row["instId"]
        if inst_id not in want:
            continue
        code = row.get("instIdCode")
        if code is None:
            raise RuntimeError(f"instrument {inst_id} has no instIdCode in response")
        mapping[int(code)] = inst_id
    missing = want - set(mapping.values())
    if missing:
        raise RuntimeError(f"these instIds not found in REST response: {missing}")
    return mapping


def parse_json_bbo(msg: str, recv_us: int) -> list[Bbo]:
    val = json.loads(msg)
    arg = val.get("arg") or {}
    if arg.get("channel") != "bbo-tbt":
        return []
    inst_id = arg.get("instId", "")
    data = val.get("data") or []
    out: list[Bbo] = []
    for entry in data:
        ts_us = int(entry["ts"]) * 1000
        seq = int(entry["seqId"])
        bid_arr = entry["bids"][0]
        ask_arr = entry["asks"][0]
        out.append(Bbo(
            source="json",
            symbol=inst_id,
            seq_id=seq,
            exch_ts_us=ts_us,
            recv_us=recv_us,
            bid_px=float(bid_arr[0]),
            bid_sz=float(bid_arr[1]),
            ask_px=float(ask_arr[0]),
            ask_sz=float(ask_arr[1]),
            bid_ord_count=int(bid_arr[3]) if len(bid_arr) > 3 else None,
            ask_ord_count=int(ask_arr[3]) if len(ask_arr) > 3 else None,
            raw_bytes=len(msg),
        ))
    return out


def parse_sbe_bbo(frame: bytes, code_to_symbol: Dict[int, str], recv_us: int) -> Optional[Bbo]:
    """解一帧 SBE 二进制 bbo-tbt (templateId=1000)。其他 template 返回 None。"""
    if len(frame) < SBE_HEADER.size:
        raise ValueError(f"SBE frame too short: {len(frame)} bytes")
    block_len, template_id, schema_id, version = SBE_HEADER.unpack_from(frame, 0)
    if schema_id != SBE_SCHEMA_ID:
        raise ValueError(f"unexpected schemaId={schema_id} (want {SBE_SCHEMA_ID})")
    if template_id != SBE_TEMPLATE_BBO_TBT:
        # 其他 template (1001/1002/1003/1005/1006…) — demo 不处理，原样跳过
        return None
    body_off = SBE_HEADER.size
    if len(frame) < body_off + block_len:
        raise ValueError(
            f"SBE frame truncated: got {len(frame)} need {body_off + block_len}"
        )
    # 注意只读 block_len 字节而不是 struct.size, 这样 schema 后续小改 (追加字段) 不会炸
    if block_len < SBE_BBO_BODY.size:
        raise ValueError(f"SBE bbo blockLength={block_len} < expected {SBE_BBO_BODY.size}")
    (
        inst_id_code, ts_us, out_time, seq_id,
        ask_px_m, ask_sz_m, bid_px_m, bid_sz_m,
        ask_ord, bid_ord,
        px_exp, sz_exp,
    ) = SBE_BBO_BODY.unpack_from(frame, body_off)
    symbol = code_to_symbol.get(inst_id_code)
    if symbol is None:
        raise ValueError(f"unknown instIdCode={inst_id_code} (mapping incomplete?)")
    px_scale = 10.0 ** px_exp
    sz_scale = 10.0 ** sz_exp
    return Bbo(
        source="sbe",
        symbol=symbol,
        seq_id=seq_id,
        exch_ts_us=ts_us,
        out_time_us=out_time,
        recv_us=recv_us,
        bid_px=bid_px_m * px_scale,
        bid_sz=bid_sz_m * sz_scale,
        ask_px=ask_px_m * px_scale,
        ask_sz=ask_sz_m * sz_scale,
        bid_ord_count=bid_ord,
        ask_ord_count=ask_ord,
        raw_bytes=len(frame),
    )


def build_json_subscribe(symbols: list[str]) -> str:
    return json.dumps({
        "op": "subscribe",
        "args": [{"channel": "bbo-tbt", "instId": s} for s in symbols],
    })


def build_sbe_subscribe(symbol_to_code: Dict[str, int]) -> str:
    """SBE 端 subscribe 用 instIdCode (int) 而不是 instId 字符串。"""
    return json.dumps({
        "op": "subscribe",
        "args": [{"channel": "bbo-tbt", "instIdCode": code}
                 for _, code in symbol_to_code.items()],
    })


async def text_keepalive(ws):
    """JSON 端发 text 'ping' (OKX 习惯)，SBE 端不能发 text ping (会被拒)。"""
    try:
        while True:
            await asyncio.sleep(20)
            await ws.send("ping")
    except asyncio.CancelledError:
        return


async def ws_json_task(symbols: list[str], queue: asyncio.Queue, stop: asyncio.Event):
    async with websockets.connect(WS_JSON_URL, max_size=2**24, compression=None) as ws:
        await ws.send(build_json_subscribe(symbols))
        ka = asyncio.create_task(text_keepalive(ws))
        try:
            while not stop.is_set():
                msg = await ws.recv()
                recv = now_us()
                if isinstance(msg, bytes):
                    logging.warning("JSON conn unexpectedly got binary frame, ignoring")
                    continue
                if msg == "pong":
                    continue
                try:
                    bbos = parse_json_bbo(msg, recv)
                except (KeyError, ValueError, json.JSONDecodeError) as e:
                    # subscribe ack / error event 都会走到这里
                    val = None
                    try:
                        val = json.loads(msg)
                    except Exception:
                        pass
                    if isinstance(val, dict) and ("event" in val or "code" in val):
                        logging.info("JSON control: %s", msg[:200])
                    else:
                        logging.warning("JSON parse err: %s msg=%r", e, msg[:200])
                    continue
                for b in bbos:
                    await queue.put(b)
        finally:
            ka.cancel()


def build_sbe_handshake_headers(api_key: str, secret: str, passphrase: str) -> Dict[str, str]:
    """SBE 端口的 ws handshake 需要 REST 风格 OK-ACCESS-* 头；
    prehash = `<unix_ts_seconds>GET/users/self/verify`，sign = base64(HMAC-SHA256(secret, prehash))。
    ts 用 unix seconds (不是 REST 那种 ISO8601 ms) — 实测得到的组合。
    """
    ts = str(int(time.time()))
    prehash = f"{ts}GET/users/self/verify"
    sig = base64.b64encode(
        hmac.new(secret.encode(), prehash.encode(), hashlib.sha256).digest()
    ).decode()
    return {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": sig,
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": passphrase,
    }


async def ws_sbe_task(
    symbols: list[str],
    code_to_symbol: Dict[int, str],
    queue: asyncio.Queue,
    stop: asyncio.Event,
    creds: Tuple[str, str, str],
):
    headers = build_sbe_handshake_headers(*creds)
    symbol_to_code = {sym: code for code, sym in code_to_symbol.items() if sym in symbols}
    # websockets 默认每 20s 发 protocol-level ping (SBE 端不接受 text "ping")
    async with websockets.connect(
        WS_SBE_URL, max_size=2**24, compression=None,
        additional_headers=headers, ping_interval=20, ping_timeout=10,
    ) as ws:
        await ws.send(build_sbe_subscribe(symbol_to_code))
        while not stop.is_set():
            msg = await ws.recv()
            recv = now_us()
            if isinstance(msg, str):
                # text frame: subscribe ack / error
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
            try:
                bbo = parse_sbe_bbo(msg, code_to_symbol, recv)
            except ValueError as e:
                logging.warning("SBE decode err: %s frame_len=%d", e, len(msg))
                continue
            if bbo is None:
                continue
            await queue.put(bbo)


def fmt_delta(sbe_recv_us: int, json_recv_us: int) -> str:
    diff = sbe_recv_us - json_recv_us
    sign = "-" if diff < 0 else "+"
    return f"{sign}{abs(diff):>6d}µs"


async def comparator(queue: asyncio.Queue, stop: asyncio.Event, pair_timeout_s: float = 5.0):
    pending: Dict[Tuple[str, int], Bbo] = {}
    pending_ts: Dict[Tuple[str, int], float] = {}
    stats = {"matched": 0, "mismatched": 0, "json_only": 0, "sbe_only": 0}
    recv_deltas_us: list[int] = []     # sbe_recv_us - json_recv_us (负=SBE 先到)
    quote_latency_us: list[int] = []   # SBE.recv - SBE.tsUs (撮合→recv, "盘口延迟")
    wire_latency_us: list[int] = []    # SBE.recv - SBE.outTime (推送→recv, "网络延迟", ≈ binance E)
    gateway_latency_us: list[int] = [] # SBE.outTime - SBE.tsUs (交易所内部 gateway 延迟)
    # JSON 端独立统计 "recv - JSON.ts" —— 这是 spread_pbs 现行 okex 测的延迟值
    json_wire_latency_us: list[int] = []
    last_print_loop_clean = time.monotonic()

    def fields_equal(sbe: Bbo, jsn: Bbo) -> Tuple[bool, list[str]]:
        """字段一致性 = bid/ask px/sz 数值对齐。
        ts 不参与比较：JSON.ts ≈ SBE.outTime/1000 是同语义但精度不同, SBE.tsUs 是
        独立的撮合时刻 (JSON 拿不到), 因此 ts 字段「不一致」不是错。
        seqId 由配对 key 保证一致。
        """
        diffs: list[str] = []
        def almost(x: float, y: float) -> bool:
            return abs(x - y) <= max(abs(x), abs(y)) * 1e-9 + 1e-12

        if not almost(sbe.bid_px, jsn.bid_px):
            diffs.append(f"bid_px {sbe.bid_px} vs {jsn.bid_px}")
        if not almost(sbe.ask_px, jsn.ask_px):
            diffs.append(f"ask_px {sbe.ask_px} vs {jsn.ask_px}")
        if not almost(sbe.bid_sz, jsn.bid_sz):
            diffs.append(f"bid_sz {sbe.bid_sz} vs {jsn.bid_sz}")
        if not almost(sbe.ask_sz, jsn.ask_sz):
            diffs.append(f"ask_sz {sbe.ask_sz} vs {jsn.ask_sz}")
        # outTime/JSON.ts 同语义：差应 ≤ 1ms (ms 截断)
        if sbe.out_time_us is not None:
            otv = sbe.out_time_us - jsn.exch_ts_us
            if not (0 <= otv < 1000):
                diffs.append(
                    f"outTime vs JSON.ts off {otv}µs "
                    f"(SBE.outTime={sbe.out_time_us} JSON.ts={jsn.exch_ts_us})"
                )
        return (not diffs, diffs)

    print(
        "# cols: sym/seq | recv_sbe-recv_json | quoteLat (recv-tsUs) | wireLat (recv-outTime) | "
        "gwLat (outTime-tsUs) | bid×sz @ ask×sz | json/sbe bytes | match",
        flush=True,
    )

    while not stop.is_set():
        try:
            bbo = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            bbo = None

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
                if ok:
                    stats["matched"] += 1
                else:
                    stats["mismatched"] += 1
                recv_deltas_us.append(sbe.recv_us - jsn.recv_us)
                qlat = sbe.recv_us - sbe.exch_ts_us
                wlat = (sbe.recv_us - sbe.out_time_us) if sbe.out_time_us else 0
                glat = (sbe.out_time_us - sbe.exch_ts_us) if sbe.out_time_us else 0
                quote_latency_us.append(qlat)
                if sbe.out_time_us:
                    wire_latency_us.append(wlat)
                    gateway_latency_us.append(glat)
                json_wire_latency_us.append(jsn.recv_us - jsn.exch_ts_us)
                mark = "OK " if ok else "DIFF"
                print(
                    f"{bbo.symbol:14s} seq={bbo.seq_id:>10d} | "
                    f"{fmt_delta(sbe.recv_us, jsn.recv_us)} | "
                    f"q={qlat:>7d}µs w={wlat:>7d}µs gw={glat:>5d}µs | "
                    f"{sbe.bid_px:>10.4f}×{sbe.bid_sz:<8.4f} @ {sbe.ask_px:>10.4f}×{sbe.ask_sz:<8.4f} | "
                    f"json={jsn.raw_bytes:>4d}B sbe={sbe.raw_bytes:>3d}B | {mark}"
                    + (f" ::: {'; '.join(diffs)}" if diffs else ""),
                    flush=True,
                )

        # 周期性 gc 过期 pending (单边出现/另一边没到)
        now = time.monotonic()
        if now - last_print_loop_clean > 2.0:
            stale = [k for k, t in pending_ts.items() if now - t > pair_timeout_s]
            for k in stale:
                lone = pending.pop(k, None)
                pending_ts.pop(k, None)
                if lone is not None:
                    if lone.source == "json":
                        stats["json_only"] += 1
                    else:
                        stats["sbe_only"] += 1
            last_print_loop_clean = now

    print(
        f"\n# summary: matched={stats['matched']} mismatched={stats['mismatched']} "
        f"json_only={stats['json_only']} sbe_only={stats['sbe_only']}",
        flush=True,
    )
    def histogram(samples: list[int], label: str):
        if not samples:
            return
        xs = sorted(samples)
        n = len(xs)
        def pct(p): return xs[min(n - 1, int(n * p))]
        print(
            f"# {label}: n={n} min={xs[0]} p25={pct(.25)} p50={pct(.5)} "
            f"p75={pct(.75)} p95={pct(.95)} p99={pct(.99)} max={xs[-1]}",
            flush=True,
        )

    histogram(recv_deltas_us,    "sbe_recv - json_recv µs   (负=SBE 路径先到, 反映两条 ws 路径差)")
    histogram(quote_latency_us,  "quoteLat  = recv - SBE.tsUs    µs (盘口延迟 = 撮合→本地, ≈ binance T)")
    histogram(wire_latency_us,   "wireLat   = recv - SBE.outTime µs (网络延迟 = gateway 推送→本地, ≈ binance E)")
    histogram(gateway_latency_us, "gwLat     = outTime - tsUs    µs (交易所内部 gateway 延迟)")
    histogram(json_wire_latency_us,
                                "jsonWireLat = recv - JSON.ts   µs (现行 spread_pbs okex 测的就是它)")
    print(
        "# note: sbe_only / json_only 不算异常 — SBE 推 tick-by-tick, JSON 10ms 节流, "
        "seqId 不必每条对应。重点是 matched 这部分 0 mismatched 即解码正确。",
        flush=True,
    )


def _load_creds() -> Optional[Tuple[str, str, str]]:
    k = os.environ.get("OKX_API_KEY")
    s = os.environ.get("OKX_API_SECRET")
    p = os.environ.get("OKX_PASSPHRASE")
    if not (k and s and p):
        return None
    return (k, s, p)


async def amain(symbols: list[str], inst_type: str, duration_s: float):
    logging.info("fetching instruments for instType=%s ...", inst_type)
    code_to_symbol = await fetch_inst_id_codes(symbols, inst_type)
    logging.info("instIdCode map: %s", code_to_symbol)

    creds = _load_creds()
    if creds is None:
        raise SystemExit(
            "OKX_API_KEY/SECRET/PASSPHRASE 未设置；SBE 端口需要 REST 风格 handshake "
            "签名。请先 `source /home/fanghaizhou/okex_fr_trade/env.sh` 再跑。"
        )
    logging.info("OKX creds detected, SBE handshake will be signed")

    queue: asyncio.Queue[Bbo] = asyncio.Queue(maxsize=4096)
    stop = asyncio.Event()

    json_t = asyncio.create_task(ws_json_task(symbols, queue, stop))
    sbe_t = asyncio.create_task(ws_sbe_task(symbols, code_to_symbol, queue, stop, creds))
    cmp_t = asyncio.create_task(comparator(queue, stop))

    async def stop_after():
        await asyncio.sleep(duration_s)
        stop.set()
        # 直接 cancel 网络任务 (它们卡在 ws.recv())
        json_t.cancel()
        sbe_t.cancel()

    timer = asyncio.create_task(stop_after()) if duration_s > 0 else None

    done, pending = await asyncio.wait(
        {json_t, sbe_t, cmp_t}, return_when=asyncio.FIRST_EXCEPTION
    )
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
                   choices=["SWAP", "SPOT", "FUTURES", "OPTION", "MARGIN"])
    p.add_argument("--duration", type=float, default=30.0,
                   help="跑多少秒后停 (<=0 表示永久跑直到 Ctrl-C)")
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
