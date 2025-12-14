#!/usr/bin/env python3
"""Probe OKX post-only (maker-only) rejection code.

Goal: submit a post-only order that would immediately take liquidity, and print
OKX's error code/message (both top-level `code` and per-order `sCode`/`sMsg`).

Typical use (paper trading by default):
  OKX_API_KEY=... OKX_API_SECRET=... OKX_PASSPHRASE=... \\
    python scripts/okx_post_only_reject_test.py --inst-id SOL-USDT-SWAP --px 200 --notional-usdt 1000 --execute

If you really want to hit the real trading endpoint, add `--real`.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any, Dict, Optional, Tuple
import re

DEFAULT_BASE_URL = os.environ.get("OKX_BASE_URL", "https://www.okx.com")


def utc_timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def json_body(data: Optional[Dict[str, Any]]) -> str:
    if not data:
        return ""
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def sign(timestamp: str, method: str, request_path: str, body: str, secret: str) -> str:
    payload = f"{timestamp}{method.upper()}{request_path}{body}"
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def request_okx_public(
    base_url: str,
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
) -> Tuple[int, str, Dict[str, str]]:
    method = method.upper()
    params = params or {}
    query = urllib.parse.urlencode(params) if params else ""
    request_path = f"{path}?{query}" if query else path

    url = f"{base_url.rstrip('/')}{request_path}"
    req = urllib.request.Request(url, data=None, method=method)
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            body_text = resp.read().decode("utf-8", "replace")
            headers = dict(resp.headers.items())
            return status, body_text, headers
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", "replace")
        headers = dict(getattr(exc, "headers", {}).items()) if getattr(exc, "headers", None) else {}
        return exc.code, body_text, headers
    except Exception as exc:  # pragma: no cover
        return 0, str(exc), {}


def request_okx_private(
    base_url: str,
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
    simulated: bool = True,
) -> Tuple[int, str, Dict[str, str]]:
    method = method.upper()
    params = params or {}
    query = urllib.parse.urlencode(params) if params else ""
    request_path = f"{path}?{query}" if query else path

    body_str = "" if method == "GET" else json_body(body)
    timestamp = utc_timestamp()
    signature = sign(timestamp, method, request_path, body_str, api_secret)

    url = f"{base_url.rstrip('/')}{request_path}"
    data = None if method == "GET" else body_str.encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("OK-ACCESS-KEY", api_key)
    req.add_header("OK-ACCESS-SIGN", signature)
    req.add_header("OK-ACCESS-TIMESTAMP", timestamp)
    req.add_header("OK-ACCESS-PASSPHRASE", passphrase)
    req.add_header("Content-Type", "application/json")
    if simulated:
        req.add_header("x-simulated-trading", "1")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            body_text = resp.read().decode("utf-8", "replace")
            headers = dict(resp.headers.items())
            return status, body_text, headers
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", "replace")
        headers = dict(getattr(exc, "headers", {}).items()) if getattr(exc, "headers", None) else {}
        return exc.code, body_text, headers
    except Exception as exc:  # pragma: no cover
        return 0, str(exc), {}


def load_credentials() -> tuple[str, str, str]:
    api_key = os.environ.get("OKX_API_KEY", "").strip()
    api_secret = os.environ.get("OKX_API_SECRET", "").strip()
    passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()
    missing = [
        name
        for name, value in [
            ("OKX_API_KEY", api_key),
            ("OKX_API_SECRET", api_secret),
            ("OKX_PASSPHRASE", passphrase),
        ]
        if not value
    ]
    if missing:
        print(f"请设置环境变量: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret, passphrase


def detect_inst_type(inst_id: str) -> str:
    inst_id = inst_id.upper().strip()
    if inst_id.endswith("-SWAP"):
        return "SWAP"
    return "FUTURES"


def parse_decimal(value: Any, field: str) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"invalid decimal for {field}: {value!r}") from exc


def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def align_to_step(value: Decimal, step: Decimal, rounding) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=rounding) * step


def fetch_instrument(base_url: str, inst_id: str, inst_type: str, timeout: int) -> Dict[str, Any]:
    status, body, _ = request_okx_public(
        base_url=base_url,
        method="GET",
        path="/api/v5/public/instruments",
        params={"instType": inst_type, "instId": inst_id},
        timeout=timeout,
    )
    if status != 200:
        raise RuntimeError(f"fetch instruments failed: http={status} body={body}")
    parsed = json.loads(body)
    data = parsed.get("data") or []
    if not data:
        raise RuntimeError(f"instrument not found: instType={inst_type} instId={inst_id}")
    return data[0]


def fetch_price_limit(base_url: str, inst_id: str, timeout: int) -> Dict[str, Any]:
    status, body, _ = request_okx_public(
        base_url=base_url,
        method="GET",
        path="/api/v5/public/price-limit",
        params={"instId": inst_id},
        timeout=timeout,
    )
    if status != 200:
        raise RuntimeError(f"fetch price-limit failed: http={status} body={body}")
    parsed = json.loads(body)
    data = parsed.get("data") or []
    if not data:
        raise RuntimeError(f"price-limit not found: instId={inst_id}")
    return data[0]


def fetch_books_best_px(base_url: str, inst_id: str, timeout: int) -> tuple[Decimal, Decimal]:
    status, body, _ = request_okx_public(
        base_url=base_url,
        method="GET",
        path="/api/v5/market/books",
        params={"instId": inst_id, "sz": "1"},
        timeout=timeout,
    )
    if status != 200:
        raise RuntimeError(f"fetch books failed: http={status} body={body}")
    parsed = json.loads(body)
    data = parsed.get("data") or []
    if not data:
        raise RuntimeError(f"books not found: instId={inst_id}")
    first = data[0] or {}
    asks = first.get("asks") or []
    bids = first.get("bids") or []
    if not asks or not bids:
        raise RuntimeError(f"books empty: instId={inst_id} payload={first}")
    best_ask = parse_decimal(asks[0][0], "bestAsk")
    best_bid = parse_decimal(bids[0][0], "bestBid")
    return best_bid, best_ask


def compute_sz_from_notional(
    notional_usdt: Decimal,
    px: Decimal,
    ct_val: Decimal,
    lot_sz: Decimal,
    min_sz: Optional[Decimal],
) -> Decimal:
    if px <= 0:
        raise ValueError("--px must be > 0")
    if ct_val <= 0:
        raise ValueError("ctVal must be > 0")
    raw_sz = notional_usdt / (px * ct_val)
    sz = floor_to_step(raw_sz, lot_sz)
    if min_sz is not None and sz < min_sz:
        sz = min_sz
    if sz <= 0:
        raise ValueError(f"computed sz <= 0 (raw_sz={raw_sz}, lotSz={lot_sz})")
    return sz


def extract_okx_codes(body_text: str) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    try:
        parsed = json.loads(body_text)
    except json.JSONDecodeError:
        return None, None, None, None
    top_code = str(parsed.get("code")) if "code" in parsed else None
    top_msg = str(parsed.get("msg")) if "msg" in parsed else None
    data = parsed.get("data") or []
    if not data:
        return top_code, top_msg, None, None
    first = data[0] or {}
    s_code = str(first.get("sCode")) if "sCode" in first else None
    s_msg = str(first.get("sMsg")) if "sMsg" in first else None
    return top_code, top_msg, s_code, s_msg


def extract_okx_ord_id(body_text: str) -> Optional[str]:
    try:
        parsed = json.loads(body_text)
    except json.JSONDecodeError:
        return None
    data = parsed.get("data") or []
    if not data:
        return None
    first = data[0] or {}
    ord_id = first.get("ordId")
    return str(ord_id) if ord_id else None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="OKX post-only rejection code probe (default: simulated trading + dry-run)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--inst-id", default="SOL-USDT-SWAP", help="Instrument id, e.g. SOL-USDT-SWAP")
    parser.add_argument(
        "--inst-type",
        choices=["SWAP", "FUTURES"],
        help="Instrument type; default auto-detect from --inst-id",
    )
    parser.add_argument("--side", choices=["buy", "sell"], default="buy", help="Order side")
    parser.add_argument(
        "--pos-side",
        dest="pos_side",
        choices=["long", "short", "net"],
        help="Required for hedge mode accounts (long/short); net for one-way mode",
    )
    parser.add_argument("--td-mode", dest="td_mode", choices=["cross", "isolated"], default="cross", help="tdMode")
    parser.add_argument("--px", default="200", help="Limit price (should cross to trigger post-only reject)")
    parser.add_argument("--notional-usdt", dest="notional_usdt", default="1000", help="Approx notional in USDT")
    parser.add_argument(
        "--tag",
        help="Optional OKX order tag (建议用 1-16 位字母数字；不填则不传该字段)",
    )
    parser.add_argument(
        "--force-cross",
        action="store_true",
        help="Ignore --px and derive a crossing price from best bid/ask (safer for triggering reject)",
    )
    parser.add_argument(
        "--cross-ticks",
        type=int,
        default=0,
        help="When using --force-cross, adjust price by N ticks (buy:+ticks, sell:-ticks)",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="OKX REST base url")
    parser.add_argument("--timeout", type=int, default=10, help="HTTP timeout seconds")
    parser.add_argument("--real", action="store_true", help="Send to real trading (disable x-simulated-trading)")
    parser.add_argument("--execute", action="store_true", help="Actually submit the order (otherwise dry-run)")
    parser.add_argument(
        "--cancel-if-accepted",
        action="store_true",
        help="If the order is unexpectedly accepted, immediately cancel it (recommended with --real)",
    )
    parser.add_argument(
        "--expect",
        choices=["reject", "accept", "either"],
        default="reject",
        help="Exit nonzero if the outcome is not as expected",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    inst_id = args.inst_id.upper().strip()
    inst_type = (args.inst_type or detect_inst_type(inst_id)).upper()
    base_url = args.base_url.rstrip("/")
    simulated = not args.real

    instrument = fetch_instrument(base_url=base_url, inst_id=inst_id, inst_type=inst_type, timeout=args.timeout)
    ct_val = parse_decimal(instrument.get("ctVal"), "ctVal")
    lot_sz = parse_decimal(instrument.get("lotSz"), "lotSz")
    tick_sz_raw = instrument.get("tickSz")
    tick_sz = parse_decimal(tick_sz_raw, "tickSz") if tick_sz_raw not in (None, "") else Decimal("0")
    min_sz_raw = instrument.get("minSz")
    min_sz = parse_decimal(min_sz_raw, "minSz") if min_sz_raw not in (None, "") else None

    px: Decimal
    if args.force_cross:
        best_bid, best_ask = fetch_books_best_px(base_url=base_url, inst_id=inst_id, timeout=args.timeout)
        price_limit = fetch_price_limit(base_url=base_url, inst_id=inst_id, timeout=args.timeout)
        buy_lmt = parse_decimal(price_limit.get("buyLmt") or "0", "buyLmt")
        sell_lmt = parse_decimal(price_limit.get("sellLmt") or "0", "sellLmt")

        if args.side == "buy":
            px = best_ask + (tick_sz * Decimal(args.cross_ticks) if tick_sz > 0 else Decimal("0"))
            px = align_to_step(px, tick_sz, ROUND_UP) if tick_sz > 0 else px
            if buy_lmt > 0 and px > buy_lmt:
                px = align_to_step(buy_lmt, tick_sz, ROUND_DOWN) if tick_sz > 0 else buy_lmt
                if px < best_ask:
                    print(
                        f"WARNING: buy price limit too low to guarantee crossing: bestAsk={best_ask} buyLmt={buy_lmt}",
                        file=sys.stderr,
                    )
        else:
            px = best_bid - (tick_sz * Decimal(args.cross_ticks) if tick_sz > 0 else Decimal("0"))
            px = align_to_step(px, tick_sz, ROUND_DOWN) if tick_sz > 0 else px
            if sell_lmt > 0 and px < sell_lmt:
                px = align_to_step(sell_lmt, tick_sz, ROUND_UP) if tick_sz > 0 else sell_lmt
                if px > best_bid:
                    print(
                        f"WARNING: sell price limit too high to guarantee crossing: bestBid={best_bid} sellLmt={sell_lmt}",
                        file=sys.stderr,
                    )
        if px <= 0:
            raise RuntimeError("derived px <= 0")
    else:
        px = parse_decimal(args.px, "--px")
        if tick_sz > 0:
            px = align_to_step(px, tick_sz, ROUND_UP if args.side == "buy" else ROUND_DOWN)

    notional_usdt = parse_decimal(args.notional_usdt, "--notional-usdt")
    sz = compute_sz_from_notional(
        notional_usdt=notional_usdt,
        px=px,
        ct_val=ct_val,
        lot_sz=lot_sz,
        min_sz=min_sz,
    )

    order_payload: Dict[str, Any] = {
        "instId": inst_id,
        "tdMode": args.td_mode,
        "side": args.side,
        "ordType": "post_only",
        "px": str(px),
        "sz": str(sz),
    }
    if args.pos_side:
        order_payload["posSide"] = args.pos_side
    if args.tag:
        tag = args.tag.strip()
        if not (1 <= len(tag) <= 16) or not re.fullmatch(r"[0-9A-Za-z]+", tag):
            raise ValueError("--tag 建议为 1-16 位字母数字（OKX 对 tag 有格式限制）")
        order_payload["tag"] = tag

    print("Instrument:")
    print(
        json.dumps(
            {
                "instId": inst_id,
                "instType": inst_type,
                "ctVal": str(ct_val),
                "lotSz": str(lot_sz),
                "minSz": str(min_sz) if min_sz else None,
                "tickSz": str(tick_sz) if tick_sz else None,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    print("Order payload:")
    print(json.dumps(order_payload, ensure_ascii=False, indent=2, sort_keys=True))
    print(f"Mode: {'REAL' if args.real else 'SIMULATED'}; {'EXECUTE' if args.execute else 'DRY-RUN'}")

    if not args.execute:
        return

    api_key, api_secret, passphrase = load_credentials()
    status, body_text, headers = request_okx_private(
        base_url=base_url,
        method="POST",
        path="/api/v5/trade/order",
        api_key=api_key,
        api_secret=api_secret,
        passphrase=passphrase,
        body=order_payload,
        timeout=args.timeout,
        simulated=simulated,
    )

    req_id = headers.get("x-request-id") or headers.get("X-Request-Id")
    print(f"HTTP: {status}; reqId={req_id}")
    print("Body:")
    print(body_text)

    top_code, top_msg, s_code, s_msg = extract_okx_codes(body_text)
    print(f"Parsed codes: code={top_code!r} msg={top_msg!r} sCode={s_code!r} sMsg={s_msg!r}")

    if top_code == "50101":
        print(
            "Hint: code=50101 means your API key environment doesn't match the request.\n"
            "- If this is a REAL trading API key, rerun with `--real`.\n"
            "- If this is a DEMO/simulated key, rerun without `--real` (default).",
            file=sys.stderr,
        )

    accepted = False
    if 200 <= status < 300:
        if (top_code in (None, "0")) and (s_code in (None, "0")):
            accepted = True

    if accepted and args.cancel_if_accepted:
        ord_id = extract_okx_ord_id(body_text)
        if ord_id:
            cancel_payload = {"instId": inst_id, "ordId": ord_id}
            c_status, c_body, _ = request_okx_private(
                base_url=base_url,
                method="POST",
                path="/api/v5/trade/cancel-order",
                api_key=api_key,
                api_secret=api_secret,
                passphrase=passphrase,
                body=cancel_payload,
                timeout=args.timeout,
                simulated=simulated,
            )
            print(f"Cancel result: HTTP {c_status}")
            print(c_body)
        else:
            print("Cancel skipped: cannot find ordId in response.", file=sys.stderr)

    if args.expect == "either":
        return
    if args.expect == "reject" and accepted:
        print("Unexpected: order accepted (expected reject).", file=sys.stderr)
        sys.exit(2)
    if args.expect == "accept" and not accepted:
        print("Unexpected: order rejected (expected accept).", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
