#!/usr/bin/env python3
"""Cancel Bybit intra orders, then either full-exit or align futures to spot.

Modes:
  - full-exit: close the Bybit margin spot leg and USDT linear futures leg to zero.
  - align-futures-to-spot: leave spot unchanged and trade futures so hedge_qty
    becomes -open_qty.

Default is dry-run. Add --execute to cancel orders and submit IOC/market orders.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.request
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR, getcontext
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests

getcontext().prec = 36

DASHBOARD_HOST = os.environ.get("DASHBOARD_HOST", "127.0.0.1")
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "4191"))
HOST = os.environ.get("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
RECV_WINDOW_MS = "5000"


def dec(value: Any, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return Decimal(default)


def fmt_dec(value: Decimal) -> str:
    if value == 0:
        return "0"
    text = format(value.normalize(), "f")
    return "0" if text == "-0" else text


def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


def load_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("BYBIT_API_KEY", "").strip()
    api_secret = os.environ.get("BYBIT_API_SECRET", "").strip()
    missing = [
        name
        for name, value in (
            ("BYBIT_API_KEY", api_key),
            ("BYBIT_API_SECRET", api_secret),
        )
        if not value
    ]
    if missing:
        raise SystemExit(f"missing env vars: {', '.join(missing)}")
    return api_key, api_secret


def bybit_sign(api_key: str, api_secret: str, timestamp_ms: str, payload: str) -> str:
    raw = f"{timestamp_ms}{api_key}{RECV_WINDOW_MS}{payload}"
    return hmac.new(api_secret.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()


def private_request(
    api_key: str,
    api_secret: str,
    method: str,
    path: str,
    *,
    query: str = "",
    payload: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
) -> Dict[str, Any]:
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True) if payload is not None else ""
    timestamp_ms = str(int(time.time() * 1000))
    sign_payload = body if method.upper() != "GET" else query
    signature = bybit_sign(api_key, api_secret, timestamp_ms, sign_payload)
    url = f"{HOST}{path}"
    if query:
        url = f"{url}?{query}"
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": timestamp_ms,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW_MS,
        "Content-Type": "application/json",
    }
    resp = requests.request(method.upper(), url, headers=headers, data=body, timeout=timeout)
    try:
        data = resp.json()
    except ValueError as exc:
        raise RuntimeError(f"Bybit {method} {path} returned non-JSON: {resp.status_code} {resp.text}") from exc
    if resp.status_code >= 300 or data.get("retCode") not in (0, "0", None):
        raise RuntimeError(f"Bybit {method} {path} failed: http={resp.status_code} body={data}")
    return data


def public_get(path: str, *, params: Dict[str, Any], timeout: int = 10) -> Dict[str, Any]:
    resp = requests.get(f"{HOST}{path}", params=params, timeout=timeout)
    try:
        data = resp.json()
    except ValueError as exc:
        raise RuntimeError(f"Bybit GET {path} returned non-JSON: {resp.status_code} {resp.text}") from exc
    if resp.status_code >= 300 or data.get("retCode") not in (0, "0", None):
        raise RuntimeError(f"Bybit GET {path} failed: http={resp.status_code} body={data}")
    return data


@dataclass
class ExposureRow:
    asset: str
    open_qty: Decimal
    hedge_qty: Decimal
    net_qty: Decimal
    net_usdt: Decimal


def fetch_snapshot(suffix: str) -> List[ExposureRow]:
    url = f"http://{DASHBOARD_HOST}:{DASHBOARD_PORT}/intra/bybit-intra-{suffix}/snapshot"
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        raise SystemExit(f"failed to fetch snapshot from {url}: {exc}")

    rows: List[ExposureRow] = []
    for entry in data.get("entries", []):
        if entry.get("channel") != "pre_trade_exposure":
            continue
        for row in entry.get("entry", {}).get("rows", []):
            if row.get("is_total"):
                continue
            asset = str(row.get("asset") or "").strip().upper()
            if not asset:
                continue
            rows.append(
                ExposureRow(
                    asset=asset,
                    open_qty=dec(row.get("open_qty")),
                    hedge_qty=dec(row.get("hedge_qty")),
                    net_qty=dec(row.get("net_qty")),
                    net_usdt=dec(row.get("net_usdt")),
                )
            )
    return rows


@dataclass
class InstrumentSpec:
    symbol: str
    qty_step: Decimal
    min_qty: Decimal
    price_step: Decimal
    min_notional: Decimal


def normalize_asset(value: str) -> str:
    text = value.strip().upper()
    cleaned = "".join(ch for ch in text if ch.isalnum())
    if cleaned.endswith("USDT") and len(cleaned) > 4:
        return cleaned[:-4]
    return cleaned


def asset_to_symbol(asset: str) -> str:
    return f"{asset.upper()}USDT"


def parse_lot_decimal(entry: Dict[str, Any], *keys: str) -> Decimal:
    lot = entry.get("lotSizeFilter") or {}
    for key in keys:
        value = lot.get(key)
        parsed = dec(value, "0")
        if parsed > 0:
            return parsed
    return Decimal("0")


def fetch_specs(category: str, timeout: int) -> Dict[str, InstrumentSpec]:
    specs: Dict[str, InstrumentSpec] = {}
    cursor = ""
    while True:
        params: Dict[str, Any] = {"category": category}
        if category == "linear":
            params["settleCoin"] = "USDT"
            params["limit"] = "1000"
        if cursor:
            params["cursor"] = cursor
        data = public_get("/v5/market/instruments-info", params=params, timeout=timeout)
        result = data.get("result") or {}
        raw = result.get("list") or []
        if not isinstance(raw, list):
            break
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            symbol = str(entry.get("symbol") or "").strip().upper()
            if not symbol.endswith("USDT"):
                continue
            status = str(entry.get("status") or "").strip().lower()
            if status != "trading":
                continue
            if category == "linear":
                contract_type = str(entry.get("contractType") or "").strip().lower()
                if contract_type and contract_type != "linearperpetual":
                    continue
            quote_coin = str(entry.get("quoteCoin") or "").strip().upper()
            if quote_coin and quote_coin != "USDT":
                continue
            price_filter = entry.get("priceFilter") or {}
            specs[symbol] = InstrumentSpec(
                symbol=symbol,
                qty_step=parse_lot_decimal(entry, "qtyStep", "basePrecision"),
                min_qty=parse_lot_decimal(entry, "minOrderQty"),
                price_step=dec(price_filter.get("tickSize"), "0"),
                min_notional=parse_lot_decimal(entry, "minNotionalValue", "minOrderAmt"),
            )
        next_cursor = str(result.get("nextPageCursor") or "").strip()
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    return specs


def fetch_best_bid_ask(symbol: str, timeout: int) -> Tuple[Decimal, Decimal]:
    data = public_get(
        "/v5/market/orderbook",
        params={"category": "spot", "symbol": symbol, "limit": "1"},
        timeout=timeout,
    )
    book = data.get("result") or {}
    asks = book.get("a") or book.get("asks") or []
    bids = book.get("b") or book.get("bids") or []
    if not asks or not bids:
        raise RuntimeError(f"empty Bybit orderbook for {symbol}: {data}")
    bid = dec(bids[0][0] if isinstance(bids[0], list) and bids[0] else None)
    ask = dec(asks[0][0] if isinstance(asks[0], list) and asks[0] else None)
    if bid <= 0 or ask <= 0:
        raise RuntimeError(f"invalid Bybit orderbook for {symbol}: bid={bid} ask={ask}")
    return bid, ask


@dataclass
class SpotAction:
    symbol: str
    side: str
    qty: Decimal
    qty_str: str
    price_str: str
    raw_qty: Decimal


@dataclass
class FuturesAction:
    symbol: str
    side: str
    qty: Decimal
    qty_str: str
    raw_qty: Decimal
    reduce_only: bool


def build_spot_action(
    delta_open_qty: Decimal,
    spec: InstrumentSpec,
    *,
    slippage_bps: Decimal,
    timeout: int,
) -> Tuple[Optional[SpotAction], str]:
    if delta_open_qty == 0:
        return None, "spot_delta=0"
    side = "Buy" if delta_open_qty > 0 else "Sell"
    raw = abs(delta_open_qty)
    qty = floor_to_step(raw, spec.qty_step)
    if qty <= 0 or (spec.min_qty > 0 and qty < spec.min_qty):
        return None, f"|spot_delta|={fmt_dec(raw)} below min_qty={fmt_dec(spec.min_qty)}"

    bid, ask = fetch_best_bid_ask(spec.symbol, timeout)
    slip = slippage_bps / Decimal("10000")
    if side == "Buy":
        price = ceil_to_step(ask * (Decimal("1") + slip), spec.price_step)
    else:
        price = floor_to_step(bid * (Decimal("1") - slip), spec.price_step)
    if price <= 0:
        return None, f"invalid aggressive IOC price for {spec.symbol}"
    if spec.min_notional > 0 and qty * price < spec.min_notional:
        return None, f"notional={fmt_dec(qty * price)} below min_notional={fmt_dec(spec.min_notional)}"

    return (
        SpotAction(
            symbol=spec.symbol,
            side=side,
            qty=qty,
            qty_str=fmt_dec(qty),
            price_str=fmt_dec(price),
            raw_qty=raw,
        ),
        "",
    )


def build_futures_action(
    delta_hedge_qty: Decimal,
    spec: InstrumentSpec,
    *,
    reduce_only: bool,
) -> Tuple[Optional[FuturesAction], str]:
    if delta_hedge_qty == 0:
        return None, "hedge_delta=0"
    raw = abs(delta_hedge_qty)
    qty = floor_to_step(raw, spec.qty_step)
    if qty <= 0 or (spec.min_qty > 0 and qty < spec.min_qty):
        return None, f"|hedge_delta|={fmt_dec(raw)} below min_qty={fmt_dec(spec.min_qty)}"
    side = "Buy" if delta_hedge_qty > 0 else "Sell"
    return (
        FuturesAction(
            symbol=spec.symbol,
            side=side,
            qty=qty,
            qty_str=fmt_dec(qty),
            raw_qty=raw,
            reduce_only=reduce_only,
        ),
        "",
    )


def fetch_open_orders(
    api_key: str,
    api_secret: str,
    category: str,
    *,
    timeout: int,
) -> List[Dict[str, Any]]:
    cursor = ""
    orders: List[Dict[str, Any]] = []
    while True:
        params = [("category", category), ("limit", "50")]
        if category == "linear":
            params.append(("settleCoin", "USDT"))
        if cursor:
            params.append(("cursor", cursor))
        query = urlencode(params)
        data = private_request(api_key, api_secret, "GET", "/v5/order/realtime", query=query, timeout=timeout)
        result = data.get("result") or {}
        batch = result.get("list") or []
        if not isinstance(batch, list):
            break
        orders.extend(batch)
        next_cursor = str(result.get("nextPageCursor") or "").strip()
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    return orders


def cancel_all_orders(api_key: str, api_secret: str, *, execute: bool, timeout: int) -> None:
    for category in ("spot", "linear"):
        try:
            orders = fetch_open_orders(api_key, api_secret, category, timeout=timeout)
        except Exception as exc:
            print(f"[cancel] WARN fetch {category} open orders failed: {exc}", file=sys.stderr)
            orders = []
        label = "SPOT" if category == "spot" else "linear futures"
        print(f"[cancel] Bybit {label} open orders: {len(orders)}")
        for order in orders:
            print(
                json.dumps(
                    {
                        "category": category,
                        "symbol": order.get("symbol"),
                        "orderId": order.get("orderId"),
                        "orderLinkId": order.get("orderLinkId"),
                        "side": order.get("side"),
                        "orderType": order.get("orderType"),
                        "price": order.get("price"),
                        "qty": order.get("qty"),
                        "isLeverage": order.get("isLeverage"),
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                )
            )
        if not execute:
            continue
        payload: Dict[str, Any] = {"category": category}
        if category == "linear":
            payload["settleCoin"] = "USDT"
        try:
            data = private_request(
                api_key,
                api_secret,
                "POST",
                "/v5/order/cancel-all",
                payload=payload,
                timeout=timeout,
            )
            print(f"[cancel] {label} [OK] {json.dumps(data.get('result'), ensure_ascii=True, sort_keys=True)}")
        except Exception as exc:
            print(f"[cancel] {label} [ERR] {exc}", file=sys.stderr)


def submit_spot_order(
    api_key: str,
    api_secret: str,
    action: SpotAction,
    *,
    timeout: int,
    idx: int,
) -> bool:
    payload: Dict[str, Any] = {
        "category": "spot",
        "symbol": action.symbol,
        "side": action.side,
        "orderType": "Limit",
        "qty": action.qty_str,
        "price": action.price_str,
        "timeInForce": "IOC",
        "isLeverage": 1,
        "orderFilter": "Order",
        "orderLinkId": f"intrabyspot{int(time.time() * 1000)}{idx:02d}",
    }
    print(f"\n[spot] {action.symbol} {action.side} qty={action.qty_str} price={action.price_str} IOC")
    try:
        data = private_request(
            api_key,
            api_secret,
            "POST",
            "/v5/order/create",
            payload=payload,
            timeout=timeout,
        )
        print(f"  [OK] {json.dumps(data.get('result'), ensure_ascii=True, sort_keys=True)}")
        return True
    except Exception as exc:
        print(f"  [ERR] {exc}", file=sys.stderr)
        return False


def submit_futures_order(
    api_key: str,
    api_secret: str,
    action: FuturesAction,
    *,
    timeout: int,
    idx: int,
) -> bool:
    payload: Dict[str, Any] = {
        "category": "linear",
        "symbol": action.symbol,
        "side": action.side,
        "orderType": "Market",
        "qty": action.qty_str,
        "reduceOnly": bool(action.reduce_only),
        "orderLinkId": f"intrabyfut{int(time.time() * 1000)}{idx:02d}",
    }
    print(f"\n[futures] {action.symbol} {action.side} qty={action.qty_str} reduceOnly={action.reduce_only}")
    try:
        data = private_request(
            api_key,
            api_secret,
            "POST",
            "/v5/order/create",
            payload=payload,
            timeout=timeout,
        )
        print(f"  [OK] {json.dumps(data.get('result'), ensure_ascii=True, sort_keys=True)}")
        return True
    except Exception as exc:
        print(f"  [ERR] {exc}", file=sys.stderr)
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cancel orders and close/align exposure for Bybit intra arb",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--suffix", required=True, help="env suffix, e.g. arb01 -> bybit-intra-arb01")
    parser.add_argument(
        "--mode",
        choices=["full-exit", "align-futures-to-spot", "align-exposure"],
        default="full-exit",
        help="full-exit closes both legs; align-futures-to-spot leaves spot unchanged and adjusts futures",
    )
    parser.add_argument("--symbol", default="", help="Optional asset/symbol filter, e.g. BTC or BTCUSDT")
    parser.add_argument("--skip-assets", default="", help="Comma-separated assets to skip")
    parser.add_argument("--min-net-usdt", type=float, default=5.0)
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--spot-slippage-bps", type=float, default=50.0, help="Aggressive spot IOC price offset")
    parser.add_argument("--execute", action="store_true", help="actually cancel and submit orders")
    parser.add_argument("--skip-cancel", action="store_true", help="skip the up-front cancel-all step")
    parser.add_argument("--cancel-settle-sec", type=float, default=1.5)
    parser.add_argument("--post-execute-sleep-sec", type=float, default=1.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    api_key, api_secret = load_credentials()
    mode = "align-futures-to-spot" if args.mode == "align-exposure" else args.mode
    only_asset = normalize_asset(args.symbol) if args.symbol else ""
    skip = {normalize_asset(part) for part in args.skip_assets.split(",") if part.strip()}

    print(f"[info] suffix={args.suffix} mode={mode} execute={args.execute}")

    if not args.skip_cancel:
        cancel_all_orders(api_key, api_secret, execute=args.execute, timeout=args.timeout)
        if args.execute and args.cancel_settle_sec > 0:
            print(f"\n[info] sleeping {args.cancel_settle_sec}s for cancellations to settle ...")
            time.sleep(args.cancel_settle_sec)

    print("\n[info] fetching dashboard snapshot ...")
    rows = fetch_snapshot(args.suffix)
    if not rows:
        print("No exposure rows found. Nothing to close.")
        return

    print("[info] fetching Bybit linear futures specs ...")
    futures_specs = fetch_specs("linear", args.timeout)
    spot_specs: Dict[str, InstrumentSpec] = {}
    if mode == "full-exit":
        print("[info] fetching Bybit spot margin specs ...")
        spot_specs = fetch_specs("spot", args.timeout)

    plans: List[Tuple[ExposureRow, Optional[SpotAction], str, Optional[FuturesAction], str]] = []

    print()
    print(
        f"{'Asset':<8} {'OpenQty':>14} {'HedgeQty':>14} {'NetUSDT':>10} "
        f"{'SpotSide':>8} {'SpotQty':>14} {'FutSide':>8} {'FutQty':>14}"
    )
    print("-" * 100)

    for row in rows:
        if only_asset and row.asset != only_asset:
            continue
        if row.asset in skip:
            print(f"  [skip] {row.asset} (in skip list)")
            continue

        symbol = asset_to_symbol(row.asset)
        spot_action: Optional[SpotAction] = None
        spot_note = "--"
        futures_action: Optional[FuturesAction] = None
        futures_note = "--"

        fut_spec = futures_specs.get(symbol)
        if mode == "full-exit":
            spot_spec = spot_specs.get(symbol)
            if spot_spec is None:
                spot_note = f"no spot {symbol}"
            else:
                spot_action, spot_note = build_spot_action(
                    -row.open_qty,
                    spot_spec,
                    slippage_bps=dec(args.spot_slippage_bps),
                    timeout=args.timeout,
                )
            if fut_spec is None:
                futures_note = f"no futures {symbol}"
            else:
                futures_action, futures_note = build_futures_action(
                    -row.hedge_qty,
                    fut_spec,
                    reduce_only=True,
                )
        else:
            if abs(row.net_usdt) < dec(args.min_net_usdt) and row.net_qty == 0:
                continue
            if fut_spec is None:
                futures_note = f"no futures {symbol}"
            else:
                delta_hedge = -row.open_qty - row.hedge_qty
                futures_action, futures_note = build_futures_action(
                    delta_hedge,
                    fut_spec,
                    reduce_only=False,
                )

        print(
            f"{row.asset:<8} {fmt_dec(row.open_qty):>14} {fmt_dec(row.hedge_qty):>14} "
            f"{fmt_dec(row.net_usdt):>10} "
            f"{(spot_action.side if spot_action else '--'):>8} "
            f"{(spot_action.qty_str if spot_action else spot_note):>14} "
            f"{(futures_action.side if futures_action else '--'):>8} "
            f"{(futures_action.qty_str if futures_action else futures_note):>14}"
        )

        if spot_action is not None or futures_action is not None:
            plans.append((row, spot_action, spot_note, futures_action, futures_note))

    print("-" * 100)
    spot_count = sum(1 for _, spot, _, _, _ in plans if spot is not None)
    futures_count = sum(1 for _, _, _, fut, _ in plans if fut is not None)
    print(f"Plan: {spot_count} margin spot orders, {futures_count} futures orders, {len(plans)} assets")

    if not plans:
        print("Nothing to submit.")
        return
    if not args.execute:
        print("\nDry-run mode. Add --execute to actually cancel and submit orders.")
        return

    print("\n" + "=" * 100)
    print("EXECUTING ORDERS")
    print("=" * 100)
    failures = 0
    for idx, (_, spot_action, _, futures_action, _) in enumerate(plans, start=1):
        if spot_action is not None and not submit_spot_order(api_key, api_secret, spot_action, timeout=args.timeout, idx=idx):
            failures += 1
        if futures_action is not None and not submit_futures_order(
            api_key, api_secret, futures_action, timeout=args.timeout, idx=idx
        ):
            failures += 1

    if args.post_execute_sleep_sec > 0:
        print(f"\n[info] sleeping {args.post_execute_sleep_sec}s before residual check ...")
        time.sleep(args.post_execute_sleep_sec)

    print("\n[info] residual exposure snapshot:")
    residual = fetch_snapshot(args.suffix)
    print(f"{'Asset':<8} {'OpenQty':>14} {'HedgeQty':>14} {'NetUSDT':>10}")
    print("-" * 52)
    nonzero = 0
    min_net = dec(args.min_net_usdt)
    for row in residual:
        if only_asset and row.asset != only_asset:
            continue
        if row.asset in skip:
            continue
        if row.open_qty == 0 and row.hedge_qty == 0 and abs(row.net_usdt) < min_net:
            continue
        nonzero += 1
        print(
            f"{row.asset:<8} {fmt_dec(row.open_qty):>14} {fmt_dec(row.hedge_qty):>14} "
            f"{fmt_dec(row.net_usdt):>10}"
        )
    if nonzero == 0:
        print("(all tracked assets cleared/aligned within threshold)")

    if failures:
        print(f"WARN: {failures} order submissions failed", file=sys.stderr)
        raise SystemExit(1)
    print("All orders submitted successfully.")


if __name__ == "__main__":
    main()
