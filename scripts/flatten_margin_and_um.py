#!/usr/bin/env python3
"""Query Binance margin/spot + UM 合约仓位，按对齐规则平敞口。

默认仅打印计划，添加 --execute 后提交市价单：
- 优先调用 /papi/v1/margin/account 与 /papi/v1/um/account；若 404 会 fallback 到 /sapi/v1/margin/account 或 /fapi/v2/account（可用 --no-fallback 禁用）。
- 显式指定 --margin-account-kind spot 时，现货腿按 /api/v3/account 的 balances 解析，不做 margin fallback。
- margin/UM 方向相反时，仅平掉多出的敞口（对齐两边）。
- margin/UM 同向或单边时，平掉该方向的全部敞口。
- margin netAsset > 0 走 SELL、< 0 走 BUY（忽略与 quote 相同的资产）。
- spot free+locked > 0 走 SELL（与 STANDARD 面板快照口径一致）。
- UM 按 positionSide 归一为净标的数量：BOTH 使用 positionAmt，LONG 记正，SHORT 记负。
  净数量 > 0 走 SELL reduceOnly，< 0 走 BUY reduceOnly。

依赖环境变量 BINANCE_API_KEY / BINANCE_API_SECRET。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sell_margin_spot import request_papi

DEFAULT_BASE_URL = (
    os.environ.get("BINANCE_PAPI_URL")
    or os.environ.get("BINANCE_FAPI_URL")
    or "https://papi.binance.com"
)
# marginAccount：PAPI -> SAPI；UM account：PAPI -> FAPI v2
DEFAULT_MARGIN_FALLBACK = "https://api.binance.com"
DEFAULT_UM_FALLBACK = "https://fapi.binance.com"
DEFAULT_PAPI_MARGIN_ACCOUNT_PATH = "/papi/v1/margin/account"
DEFAULT_PAPI_UM_ACCOUNT_PATH = "/papi/v1/um/account"
DEFAULT_MARGIN_ACCOUNT_FALLBACK_PATH = "/sapi/v1/margin/account"
DEFAULT_UM_ACCOUNT_FALLBACK_PATH = "/fapi/v2/account"

# 下单路径（若落到 FAPI/SAPI 也会调整）
DEFAULT_MARGIN_ORDER_PATH = "/papi/v1/margin/order"
DEFAULT_UM_ORDER_PATH = "/papi/v1/um/order"
DEFAULT_MARGIN_ORDER_FALLBACK_PATH = "/sapi/v1/margin/order"
DEFAULT_UM_ORDER_FALLBACK_PATH = "/fapi/v1/order"
DEFAULT_SPOT_EXCHANGE_INFO_BASE_URL = os.environ.get("BINANCE_SPOT_API_URL") or "https://api.binance.com"
DEFAULT_UM_EXCHANGE_INFO_BASE_URL = os.environ.get("BINANCE_FAPI_URL") or "https://fapi.binance.com"
DEFAULT_SAPI_BASE_URL = os.environ.get("BINANCE_SAPI_URL") or "https://api.binance.com"
SPOT_EXCHANGE_INFO_PATH = "/api/v3/exchangeInfo"
UM_EXCHANGE_INFO_PATH = "/fapi/v1/exchangeInfo"
SAPI_DUST_PATH = "/sapi/v1/asset/dust"

FALLBACK_STATUSES = {0, 404}

Endpoint = Tuple[str, str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="列出当前 margin spot 资产与 UM 仓位，按对齐规则平敞口（默认 dry-run）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="默认请求域名（margin/UM 都会用，除非下方单独提供 base-url）",
    )
    parser.add_argument(
        "--margin-base-url",
        help="仅对 margin account 请求使用的 base URL，未提供时沿用 --base-url",
    )
    parser.add_argument(
        "--um-base-url",
        help="仅对 UM account 请求使用的 base URL，未提供时沿用 --base-url",
    )
    parser.add_argument(
        "--quote-asset",
        default="USDT",
        help="卖出现货时拼接的报价资产，例如 USDT -> BTCUSDT",
    )
    parser.add_argument(
        "--quantity-precision",
        type=int,
        default=6,
        help="数量精度，ROUND_DOWN 处理，避免因过多小数被拒单",
    )
    parser.add_argument(
        "--min-qty",
        type=Decimal,
        default=Decimal("0"),
        help="低于此数量的订单跳过",
    )
    parser.add_argument(
        "--recv-window",
        type=int,
        dest="recv_window",
        help="可选 recvWindow（毫秒）",
    )
    parser.add_argument(
        "--symbols",
        help="仅处理指定交易对，逗号或空格分隔，例如 BTCUSDT,ETHUSDT",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        default=[],
        help="追加单个交易对，可重复传入，例如 --symbol BTCUSDT --symbol ETHUSDT",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="处理所有发现的 spot/margin 与 UM 仓位，按当前对齐规则全量调整",
    )
    parser.add_argument(
        "--mode",
        choices=["align", "clear"],
        default="align",
        help="align: 调整多出的单腿使两边对齐；clear: spot/margin 与 UM 两腿分别清到 0",
    )
    parser.add_argument(
        "--margin-account-kind",
        choices=["margin", "spot"],
        default="margin",
        help="现货腿账户响应类型；margin 解析 userAssets.netAsset，spot 解析 balances.free+locked",
    )
    parser.add_argument(
        "--margin-account-path",
        help="自定义 margin account API path（默认按 base-url 自动推断）",
    )
    parser.add_argument(
        "--um-account-path",
        help="自定义 UM account API path（默认按 base-url 自动推断）",
    )
    parser.add_argument(
        "--margin-order-path",
        help="自定义 margin 下单 API path（默认按 base-url 自动推断）",
    )
    parser.add_argument(
        "--um-order-path",
        help="自定义 UM 下单 API path（默认按 base-url 自动推断）",
    )
    parser.add_argument(
        "--spot-exchange-info-base-url",
        default=DEFAULT_SPOT_EXCHANGE_INFO_BASE_URL,
        help="spot/margin 数量过滤器 exchangeInfo base URL",
    )
    parser.add_argument(
        "--um-exchange-info-base-url",
        default=DEFAULT_UM_EXCHANGE_INFO_BASE_URL,
        help="UM 数量过滤器 exchangeInfo base URL",
    )
    parser.add_argument(
        "--sapi-base-url",
        default=DEFAULT_SAPI_BASE_URL,
        help="SAPI base URL，用于 clear 模式下 dust→BNB",
    )
    parser.add_argument(
        "--skip-margin",
        action="store_true",
        help="仅处理 UM，不提交 margin spot 卖单",
    )
    parser.add_argument(
        "--skip-um",
        action="store_true",
        help="仅处理 margin spot，不提交 UM 平仓单",
    )
    parser.add_argument(
        "--isolated",
        action="store_true",
        help="margin spot 下单使用逐仓（默认全仓）",
    )
    parser.add_argument(
        "--side-effect",
        dest="side_effect",
        choices=["AUTO_REPAY", "MARGIN_BUY", "NO_SIDE_EFFECT", "AUTO_BORROW_REPAY"],
        default="AUTO_BORROW_REPAY",
        help="margin spot 下单 sideEffectType，默认 AUTO_BORROW_REPAY（自动借币+归还）",
    )
    parser.add_argument(
        "--um-qty-precision",
        type=int,
        default=4,
        help="UM 数量精度（ROUND_DOWN），如需更细粒度自行调整",
    )
    parser.add_argument(
        "--no-reduce-only",
        dest="reduce_only",
        action="store_false",
        help="UM 平仓不带 reduceOnly（默认开启 reduceOnly）",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="实际提交订单；默认仅打印计划",
    )
    parser.add_argument(
        "--no-fallback",
        action="store_true",
        help="禁用自动 fallback 到 SAPI/FAPI，当 PAPI 404 时直接报错",
    )
    return parser.parse_args()


def load_credentials() -> tuple[str, str]:
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("请设置环境变量 BINANCE_API_KEY / BINANCE_API_SECRET", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret


def format_qty(value: Decimal, precision: int) -> Decimal:
    if precision < 0:
        raise ValueError("precision 必须为非负整数")
    quant = Decimal(1).scaleb(-precision)
    try:
        return value.quantize(quant, rounding=ROUND_DOWN)
    except InvalidOperation:
        steps = (value / quant).to_integral_value(rounding=ROUND_DOWN)
        return steps * quant


def format_str(qty: Decimal) -> str:
    normalized = qty.normalize()
    if normalized == normalized.to_integral():
        normalized = normalized.quantize(Decimal("1"))
    return format(normalized, "f")


@dataclass
class MarginPosition:
    asset: str
    symbol: str
    quantity: Decimal


@dataclass
class UmPosition:
    symbol: str
    position_side: str
    quantity: Decimal


@dataclass
class MarginOrder:
    asset: str
    symbol: str
    side: str
    quantity: Decimal


@dataclass
class UmOrder:
    symbol: str
    side: str
    position_side: str
    quantity: Decimal


def normalize_um_quantity(position_side: str, amount: Decimal) -> Decimal:
    side = position_side.upper()
    if side == "LONG":
        return abs(amount)
    if side == "SHORT":
        return -abs(amount)
    return amount


@dataclass(frozen=True)
class QuantityRule:
    min_qty: Decimal
    step_size: Decimal


def decimal_or_zero(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return Decimal("0")


def now_ms() -> int:
    import time

    return int(time.time() * 1000)


def sign_query(query: str, secret: str) -> str:
    import hashlib
    import hmac

    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def round_down_to_step(value: Decimal, step_size: Decimal) -> Decimal:
    if step_size <= 0:
        return value
    steps = (value / step_size).to_integral_value(rounding=ROUND_DOWN)
    return steps * step_size


def round_up_to_step(value: Decimal, step_size: Decimal) -> Decimal:
    if step_size <= 0:
        return value
    steps = (value / step_size).to_integral_value(rounding=ROUND_DOWN)
    if steps * step_size < value:
        steps += 1
    return steps * step_size


def apply_quantity_rule(
    value: Decimal,
    precision: int,
    min_qty: Decimal,
    qty_rule: Optional[QuantityRule],
) -> Tuple[Decimal, Decimal]:
    qty = format_qty(value, precision)
    effective_min_qty = min_qty
    if qty_rule is not None:
        qty = round_down_to_step(qty, qty_rule.step_size)
        effective_min_qty = max(effective_min_qty, qty_rule.min_qty)
    return qty, effective_min_qty


def apply_quantity_rule_nearest(
    value: Decimal,
    max_qty: Decimal,
    precision: int,
    min_qty: Decimal,
    qty_rule: Optional[QuantityRule],
) -> Tuple[Decimal, Decimal]:
    qty = format_qty(value, precision)
    effective_min_qty = min_qty
    if qty_rule is None:
        if qty > max_qty:
            qty = format_qty(max_qty, precision)
        return qty, effective_min_qty

    effective_min_qty = max(effective_min_qty, qty_rule.min_qty)
    floor_qty = round_down_to_step(qty, qty_rule.step_size)
    ceil_qty = round_up_to_step(qty, qty_rule.step_size)
    max_aligned = round_down_to_step(max_qty, qty_rule.step_size)

    candidates = []
    for candidate in (floor_qty, ceil_qty, max_aligned):
        if candidate <= 0 or candidate < effective_min_qty or candidate > max_aligned:
            continue
        candidates.append(candidate)
    if not candidates:
        return Decimal("0"), effective_min_qty

    best = min(candidates, key=lambda candidate: (abs(value - candidate), candidate))
    return best, effective_min_qty


def public_get_json(base_url: str, path: str, params: Dict[str, str]) -> Tuple[int, str, Any]:
    query = urllib.parse.urlencode(sorted(params.items()), safe="-_.~")
    url = f"{base_url.rstrip('/')}{path}"
    if query:
        url = f"{url}?{query}"
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.getcode(), body, json.loads(body)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return exc.code, body, None
    except json.JSONDecodeError as exc:
        raise SystemExit(f"解析 exchangeInfo 响应失败: {exc}") from exc
    except Exception as exc:
        return 0, str(exc), None


def quantity_rule_from_symbol_info(info: Dict[str, Any]) -> Optional[QuantityRule]:
    min_qty = Decimal("0")
    step_size = Decimal("0")
    for filt in info.get("filters") or []:
        if not isinstance(filt, dict):
            continue
        if filt.get("filterType") not in {"LOT_SIZE", "MARKET_LOT_SIZE"}:
            continue
        filt_min_qty = decimal_or_zero(filt.get("minQty", "0"))
        filt_step_size = decimal_or_zero(filt.get("stepSize", "0"))
        if filt_min_qty > min_qty:
            min_qty = filt_min_qty
        if filt_step_size > step_size:
            step_size = filt_step_size
    if min_qty <= 0 and step_size <= 0:
        return None
    return QuantityRule(min_qty=min_qty, step_size=step_size)


def fetch_quantity_rules(
    base_url: str,
    path: str,
    symbols: Iterable[str],
    label: str,
) -> Dict[str, QuantityRule]:
    wanted = {s.strip().upper() for s in symbols if s and s.strip()}
    if not wanted:
        return {}
    params: Dict[str, str]
    if len(wanted) == 1:
        params = {"symbol": next(iter(wanted))}
    else:
        params = {"symbols": json.dumps(sorted(wanted), separators=(",", ":"))}

    status, body, payload = public_get_json(base_url, path, params)
    if status != 200 or not isinstance(payload, dict):
        raise SystemExit(f"获取 {label} exchangeInfo 失败 status={status} body={body[:300]}")
    raw_symbols = payload.get("symbols")
    if not isinstance(raw_symbols, list):
        raise SystemExit(f"{label} exchangeInfo 响应缺少 symbols 字段")

    rules: Dict[str, QuantityRule] = {}
    for info in raw_symbols:
        if not isinstance(info, dict):
            continue
        symbol = str(info.get("symbol", "")).strip().upper()
        if symbol not in wanted:
            continue
        rule = quantity_rule_from_symbol_info(info)
        if rule is not None:
            rules[symbol] = rule

    missing = sorted(wanted - set(rules))
    if missing:
        raise SystemExit(f"{label} exchangeInfo 缺少数量过滤器: {', '.join(missing)}")
    return rules


def fetch_margin_positions(
    base_url: str,
    account_path: str,
    api_key: str,
    api_secret: str,
    quote_asset: str,
    recv_window: Optional[int],
    account_kind: str,
) -> Tuple[int, str, List[MarginPosition]]:
    params: Dict[str, Any] = {}
    if recv_window is not None:
        params["recvWindow"] = str(recv_window)
    status, body, _ = request_papi(base_url.rstrip("/"), account_path, params, api_key, api_secret, method="GET")
    if status != 200:
        return status, body, []
    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"解析 margin 账户响应失败: {exc}") from exc

    account_kind = account_kind.lower()
    if account_kind == "spot":
        assets = data.get("balances")
        if not isinstance(assets, list):
            raise SystemExit("spot 响应缺少 balances 字段")
    else:
        assets = data.get("userAssets")
        if not isinstance(assets, list):
            raise SystemExit("margin 响应缺少 userAssets 字段")

    positions: List[MarginPosition] = []
    for entry in assets:
        if not isinstance(entry, dict):
            continue
        asset = str(entry.get("asset", "")).strip().upper()
        if not asset or asset == quote_asset.upper():
            continue
        if account_kind == "spot":
            try:
                free_qty = Decimal(str(entry.get("free", "0")))
                locked_qty = Decimal(str(entry.get("locked", "0")))
                net_qty = free_qty + locked_qty
            except InvalidOperation:
                continue
        else:
            net_raw = entry.get("netAsset")
            if net_raw is None:
                continue
            try:
                net_qty = Decimal(str(net_raw))
            except InvalidOperation:
                continue
        if net_qty == 0:
            continue
        symbol = f"{asset}{quote_asset.upper()}"
        positions.append(MarginPosition(asset=asset, symbol=symbol, quantity=net_qty))
    return status, body, positions


def fetch_um_positions(
    base_url: str,
    account_path: str,
    api_key: str,
    api_secret: str,
    recv_window: Optional[int],
) -> Tuple[int, str, List[UmPosition]]:
    params: Dict[str, Any] = {}
    if recv_window is not None:
        params["recvWindow"] = str(recv_window)
    status, body, _ = request_papi(base_url.rstrip("/"), account_path, params, api_key, api_secret, method="GET")
    if status != 200:
        return status, body, []
    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"解析 UM 响应失败: {exc}") from exc

    raw_positions = data.get("positions")
    if not isinstance(raw_positions, list):
        raise SystemExit("UM 响应缺少 positions 字段")

    positions: List[UmPosition] = []
    for entry in raw_positions:
        if not isinstance(entry, dict):
            continue
        symbol = str(entry.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        amt_raw = entry.get("positionAmt")
        if amt_raw is None:
            continue
        try:
            amt = Decimal(str(amt_raw))
        except InvalidOperation:
            continue
        if amt == 0:
            continue
        pos_side = str(entry.get("positionSide") or "BOTH").strip().upper() or "BOTH"
        positions.append(
            UmPosition(
                symbol=symbol,
                position_side=pos_side,
                quantity=normalize_um_quantity(pos_side, amt),
            )
        )
    return status, body, positions


def format_signed(qty: Decimal) -> str:
    if qty == 0:
        return "0"
    sign = "+" if qty > 0 else "-"
    return f"{sign}{format_str(abs(qty))}"


def parse_symbol_filter(raw: Optional[str], symbols_list: Iterable[str]) -> Optional[set[str]]:
    items: List[str] = []
    if raw:
        items.extend(s.strip().upper() for s in re.split(r"[,\s]+", raw) if s.strip())
    items.extend(s.strip().upper() for s in symbols_list if s and s.strip())
    if not items:
        return None
    return set(items)


def build_margin_order(
    pos: MarginPosition,
    reduce_qty: Decimal,
    precision: int,
    min_qty: Decimal,
    qty_rule: Optional[QuantityRule],
) -> Tuple[Optional[MarginOrder], Decimal]:
    if reduce_qty <= 0:
        return None, Decimal("0")
    qty, effective_min_qty = apply_quantity_rule(reduce_qty, precision, min_qty, qty_rule)
    if qty <= 0 or qty < effective_min_qty:
        return None, reduce_qty
    side = "SELL" if pos.quantity > 0 else "BUY"
    return MarginOrder(asset=pos.asset, symbol=pos.symbol, side=side, quantity=qty), reduce_qty - qty


def build_um_orders(
    positions: List[UmPosition],
    reduce_qty: Decimal,
    precision: int,
    min_qty: Decimal,
    reduce_sign: int,
    qty_rule: Optional[QuantityRule],
) -> Tuple[List[UmOrder], Decimal]:
    if reduce_qty <= 0:
        return [], Decimal("0")
    if reduce_sign == 0:
        return [], reduce_qty

    want_positive = reduce_sign > 0
    candidates = [p for p in positions if (p.quantity > 0) == want_positive]
    candidates.sort(key=lambda p: abs(p.quantity), reverse=True)

    remaining = reduce_qty
    orders: List[UmOrder] = []
    for pos in candidates:
        if remaining <= 0:
            break
        available = abs(pos.quantity)
        use_qty = min(available, remaining)
        qty, effective_min_qty = apply_quantity_rule_nearest(
            use_qty,
            available,
            precision,
            min_qty,
            qty_rule,
        )
        if qty <= 0 or qty < effective_min_qty:
            continue
        side = "SELL" if pos.quantity > 0 else "BUY"
        orders.append(
            UmOrder(
                symbol=pos.symbol,
                side=side,
                position_side=pos.position_side,
                quantity=qty,
            )
        )
        remaining -= qty
    return orders, remaining


def build_um_clear_orders(
    positions: List[UmPosition],
    precision: int,
    min_qty: Decimal,
    qty_rule: Optional[QuantityRule],
) -> Tuple[List[UmOrder], Decimal]:
    orders: List[UmOrder] = []
    remaining = Decimal("0")
    for pos in sorted(positions, key=lambda p: abs(p.quantity), reverse=True):
        available = abs(pos.quantity)
        qty, effective_min_qty = apply_quantity_rule_nearest(
            available,
            available,
            precision,
            min_qty,
            qty_rule,
        )
        if qty <= 0 or qty < effective_min_qty:
            remaining += available
            continue
        side = "SELL" if pos.quantity > 0 else "BUY"
        orders.append(
            UmOrder(
                symbol=pos.symbol,
                side=side,
                position_side=pos.position_side,
                quantity=qty,
            )
        )
        remaining += available - qty
    return orders, remaining


def submit_margin_order(
    order: MarginOrder,
    base_url: str,
    order_path: str,
    api_key: str,
    api_secret: str,
    recv_window: Optional[int],
    isolated: bool,
    side_effect: Optional[str],
    account_kind: str,
) -> int:
    params: Dict[str, Any] = {
        "symbol": order.symbol,
        "side": order.side,
        "type": "MARKET",
        "quantity": format_str(order.quantity),
    }
    is_spot = account_kind.lower() == "spot"
    if isolated and not is_spot:
        params["isIsolated"] = "TRUE"
    if side_effect and not is_spot:
        params["sideEffectType"] = side_effect
    if recv_window is not None:
        params["recvWindow"] = str(recv_window)
    status, body, headers = request_papi(base_url.rstrip("/"), order_path, params, api_key, api_secret, method="POST")
    weight = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
    order_count = headers.get("x-mbx-order-count-1m") or headers.get("x-mbx-order-count")
    leg = "spot" if is_spot else "margin"
    print(f"[{leg} {order.symbol}] status={status} used_weight={weight} order_count={order_count}")
    print(body)
    return status


def submit_um_order(
    order: UmOrder,
    base_url: str,
    order_path: str,
    api_key: str,
    api_secret: str,
    recv_window: Optional[int],
    reduce_only: bool,
) -> int:
    params: Dict[str, Any] = {
        "symbol": order.symbol,
        "side": order.side,
        "type": "MARKET",
        "quantity": format_str(order.quantity),
        "positionSide": order.position_side,
    }
    if reduce_only:
        params["reduceOnly"] = "true"
    if recv_window is not None:
        params["recvWindow"] = str(recv_window)
    status, body, headers = request_papi(base_url.rstrip("/"), order_path, params, api_key, api_secret, method="POST")
    weight = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
    order_count = headers.get("x-mbx-order-count-1m") or headers.get("x-mbx-order-count")
    print(f"[UM {order.symbol}] status={status} used_weight={weight} order_count={order_count}")
    print(body)
    return status


def submit_dust_convert(
    base_url: str,
    assets: Iterable[str],
    api_key: str,
    api_secret: str,
    recv_window: Optional[int],
) -> Tuple[int, str]:
    deduped = sorted(
        {asset.strip().upper() for asset in assets if asset and asset.strip() and asset.strip().upper() != "BNB"}
    )
    if not deduped:
        return 200, ""

    params: List[Tuple[str, str]] = [("asset", asset) for asset in deduped]
    params.append(("recvWindow", str(recv_window or 5000)))
    params.append(("timestamp", str(now_ms())))
    query = urllib.parse.urlencode(sorted(params, key=lambda kv: kv[0]), safe="-_.~")
    signature = sign_query(query, api_secret)
    url = f"{base_url.rstrip('/')}{SAPI_DUST_PATH}?{query}&signature={signature}"
    req = urllib.request.Request(url, method="POST", headers={"X-MBX-APIKEY": api_key})
    print(f"[dust] assets={','.join(deduped)}")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            status = resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
            headers = dict(resp.headers.items())
    except urllib.error.HTTPError as exc:
        status = exc.code
        body = exc.read().decode("utf-8", errors="replace")
        headers = dict(getattr(exc, "headers", {}).items()) if getattr(exc, "headers", None) else {}
    except Exception as exc:
        status = 0
        body = str(exc)
        headers = {}
    weight = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
    print(f"[dust] status={status} used_weight={weight}")
    print(body)
    return status, body


def main() -> None:
    args = parse_args()
    api_key, api_secret = load_credentials()
    base_url = args.base_url.rstrip("/")
    margin_base_url = (args.margin_base_url or base_url).rstrip("/")
    um_base_url = (args.um_base_url or base_url).rstrip("/")
    margin_account_path = args.margin_account_path or DEFAULT_PAPI_MARGIN_ACCOUNT_PATH
    um_account_path = args.um_account_path or DEFAULT_PAPI_UM_ACCOUNT_PATH
    margin_order_path = args.margin_order_path or DEFAULT_MARGIN_ORDER_PATH
    um_order_path = args.um_order_path or DEFAULT_UM_ORDER_PATH
    symbol_filter = parse_symbol_filter(args.symbols, args.symbol)

    if args.all and symbol_filter is not None:
        raise SystemExit("--all 不能和 --symbol/--symbols 同时使用")

    if args.margin_account_kind == "spot" and not args.no_fallback:
        raise SystemExit("spot 模式必须显式禁用 fallback（请传 --no-fallback）")

    if args.skip_margin and args.skip_um:
        print("skip-margin 与 skip-um 同时开启，无事可做", file=sys.stderr)
        sys.exit(0)

    margin_positions: List[MarginPosition] = []
    um_positions: List[UmPosition] = []

    margin_used_fallback = False
    if not args.skip_margin:
        m_status, m_body, margin_positions = fetch_margin_positions(
            base_url=margin_base_url,
            account_path=margin_account_path,
            api_key=api_key,
            api_secret=api_secret,
            quote_asset=args.quote_asset,
            recv_window=args.recv_window,
            account_kind=args.margin_account_kind,
        )
        if (
            m_status in FALLBACK_STATUSES
            and not args.no_fallback
            and args.margin_account_path is None
        ):
            margin_base_url = DEFAULT_MARGIN_FALLBACK
            margin_account_path = DEFAULT_MARGIN_ACCOUNT_FALLBACK_PATH
            margin_order_path = args.margin_order_path or DEFAULT_MARGIN_ORDER_FALLBACK_PATH
            print(f"margin account fallback -> {margin_base_url}{margin_account_path}")
            margin_used_fallback = True
            m_status, m_body, margin_positions = fetch_margin_positions(
                base_url=margin_base_url,
                account_path=margin_account_path,
                api_key=api_key,
                api_secret=api_secret,
                quote_asset=args.quote_asset,
                recv_window=args.recv_window,
                account_kind=args.margin_account_kind,
            )
        if m_status != 200:
            raise SystemExit(
                f"获取 {args.margin_account_kind} 账户失败 status={m_status} body={m_body}"
            )

    if not args.skip_um:
        if (
            margin_used_fallback
            and not args.no_fallback
            and args.um_base_url is None
            and args.um_account_path is None
        ):
            um_base_url = DEFAULT_UM_FALLBACK
            um_account_path = DEFAULT_UM_ACCOUNT_FALLBACK_PATH
            um_order_path = args.um_order_path or DEFAULT_UM_ORDER_FALLBACK_PATH
            print(f"UM account fallback -> {um_base_url}{um_account_path} (margin 使用 SAPI)")
        u_status, u_body, um_positions = fetch_um_positions(
            base_url=um_base_url,
            account_path=um_account_path,
            api_key=api_key,
            api_secret=api_secret,
            recv_window=args.recv_window,
        )
        if (
            u_status in FALLBACK_STATUSES
            and not args.no_fallback
            and args.um_account_path is None
        ):
            um_base_url = DEFAULT_UM_FALLBACK
            um_account_path = DEFAULT_UM_ACCOUNT_FALLBACK_PATH
            um_order_path = args.um_order_path or DEFAULT_UM_ORDER_FALLBACK_PATH
            print(f"UM account fallback -> {um_base_url}{um_account_path}")
            u_status, u_body, um_positions = fetch_um_positions(
                base_url=um_base_url,
                account_path=um_account_path,
                api_key=api_key,
                api_secret=api_secret,
                recv_window=args.recv_window,
            )
        if u_status != 200:
            raise SystemExit(f"获取 UM 仓位失败 status={u_status} body={u_body}")

    if symbol_filter is not None:
        margin_positions = [p for p in margin_positions if p.symbol in symbol_filter]
        um_positions = [p for p in um_positions if p.symbol in symbol_filter]
        if symbol_filter:
            print(f"仅处理 symbols: {', '.join(sorted(symbol_filter))}")
    elif args.all:
        print("处理全部 symbols")

    margin_by_symbol = {pos.symbol: pos for pos in margin_positions}
    um_by_symbol: Dict[str, List[UmPosition]] = {}
    for pos in um_positions:
        um_by_symbol.setdefault(pos.symbol, []).append(pos)

    symbols = sorted(set(margin_by_symbol) | set(um_by_symbol))
    if not symbols:
        print("未找到需要处理的仓位")
        return

    margin_qty_rules: Dict[str, QuantityRule] = {}
    if not args.skip_margin and margin_by_symbol:
        margin_qty_rules = fetch_quantity_rules(
            args.spot_exchange_info_base_url,
            SPOT_EXCHANGE_INFO_PATH,
            margin_by_symbol.keys(),
            "spot/margin",
        )

    um_qty_rules: Dict[str, QuantityRule] = {}
    if not args.skip_um and um_by_symbol:
        um_qty_rules = fetch_quantity_rules(
            args.um_exchange_info_base_url,
            UM_EXCHANGE_INFO_PATH,
            um_by_symbol.keys(),
            "UM",
        )

    margin_orders: List[MarginOrder] = []
    um_orders: List[UmOrder] = []
    dust_assets: List[str] = []
    plan_rows: List[
        Tuple[
            str,
            Decimal,
            Decimal,
            Decimal,
            List[MarginOrder],
            List[UmOrder],
            Decimal,
            Decimal,
        ]
    ] = []

    for symbol in symbols:
        margin_pos = margin_by_symbol.get(symbol)
        margin_qty = margin_pos.quantity if margin_pos else Decimal("0")
        um_list = um_by_symbol.get(symbol, [])
        um_qty = sum((p.quantity for p in um_list), Decimal("0"))
        net_qty = margin_qty + um_qty

        symbol_margin_orders: List[MarginOrder] = []
        symbol_um_orders: List[UmOrder] = []
        margin_remaining = Decimal("0")
        um_remaining = Decimal("0")

        if args.mode == "clear":
            if margin_qty != 0 and margin_pos is not None:
                order, margin_remaining = build_margin_order(
                    margin_pos,
                    abs(margin_qty),
                    args.quantity_precision,
                    args.min_qty,
                    margin_qty_rules.get(symbol),
                )
                if order:
                    symbol_margin_orders.append(order)
            if um_list:
                symbol_um_orders, um_remaining = build_um_clear_orders(
                    um_list,
                    args.um_qty_precision,
                    args.min_qty,
                    um_qty_rules.get(symbol),
                )
        elif margin_qty != 0 and um_qty != 0 and margin_qty * um_qty < 0:
            diff = abs(margin_qty) - abs(um_qty)
            if diff > 0 and margin_pos is not None:
                order, margin_remaining = build_margin_order(
                    margin_pos,
                    diff,
                    args.quantity_precision,
                    args.min_qty,
                    margin_qty_rules.get(symbol),
                )
                if order:
                    symbol_margin_orders.append(order)
            elif diff < 0:
                symbol_um_orders, um_remaining = build_um_orders(
                    um_list,
                    abs(diff),
                    args.um_qty_precision,
                    args.min_qty,
                    1 if um_qty > 0 else -1,
                    um_qty_rules.get(symbol),
                )
        else:
            if margin_qty != 0 and margin_pos is not None:
                order, margin_remaining = build_margin_order(
                    margin_pos,
                    abs(margin_qty),
                    args.quantity_precision,
                    args.min_qty,
                    margin_qty_rules.get(symbol),
                )
                if order:
                    symbol_margin_orders.append(order)
            if um_qty != 0:
                symbol_um_orders, um_remaining = build_um_orders(
                    um_list,
                    abs(um_qty),
                    args.um_qty_precision,
                    args.min_qty,
                    1 if um_qty > 0 else -1,
                    um_qty_rules.get(symbol),
                )

        plan_rows.append(
            (
                symbol,
                margin_qty,
                um_qty,
                net_qty,
                symbol_margin_orders,
                symbol_um_orders,
                margin_remaining,
                um_remaining,
            )
        )
        margin_orders.extend(symbol_margin_orders)
        um_orders.extend(symbol_um_orders)
        if (
            args.mode == "clear"
            and margin_pos is not None
            and margin_qty > 0
            and margin_remaining > 0
            and margin_pos.asset != "BNB"
        ):
            dust_assets.append(margin_pos.asset)

    for (
        symbol,
        margin_qty,
        um_qty,
        net_qty,
        symbol_margin_orders,
        symbol_um_orders,
        margin_remaining,
        um_remaining,
    ) in plan_rows:
        print(f"\n=== {symbol} ===")
        print(
            f"pos: margin={format_signed(margin_qty)} um={format_signed(um_qty)} net={format_signed(net_qty)}"
        )
        if not symbol_margin_orders and not symbol_um_orders:
            print("  无需调整（已对齐或无订单可下）")
        for order in symbol_margin_orders:
            leg = "spot" if args.margin_account_kind == "spot" else "margin"
            print(
                f"  {leg} -> {order.side} {format_str(order.quantity)} (asset={order.asset})"
            )
        for order in symbol_um_orders:
            print(
                f"  um -> {order.side} {format_str(order.quantity)} positionSide={order.position_side}"
            )
        if args.mode == "clear":
            for order in symbol_margin_orders:
                if order.asset in dust_assets:
                    print(f"  dust -> convert {order.asset} to BNB")
            if not symbol_margin_orders and margin_remaining > 0:
                margin_pos = margin_by_symbol.get(symbol)
                if margin_pos is not None and margin_pos.asset in dust_assets:
                    print(f"  dust -> convert {margin_pos.asset} to BNB")
        if margin_remaining > 0 or um_remaining > 0:
            warn = []
            if margin_remaining > 0:
                warn.append(f"margin 剩余 {format_str(margin_remaining)}")
            if um_remaining > 0:
                warn.append(f"UM 剩余 {format_str(um_remaining)}")
            print("  注意: " + ", ".join(warn) + " 可能因精度或 min_qty 未完全对齐")

    if not args.execute:
        print("dry-run：未提交任何订单，添加 --execute 执行")
        return

    failures = 0
    for order in margin_orders:
        status = submit_margin_order(
            order,
            base_url=margin_base_url,
            order_path=margin_order_path,
            api_key=api_key,
            api_secret=api_secret,
            recv_window=args.recv_window,
            isolated=args.isolated,
            side_effect=args.side_effect,
            account_kind=args.margin_account_kind,
        )
        if not (200 <= status < 300):
            failures += 1
    for order in um_orders:
        status = submit_um_order(
            order,
            base_url=um_base_url,
            order_path=um_order_path,
            api_key=api_key,
            api_secret=api_secret,
            recv_window=args.recv_window,
            reduce_only=args.reduce_only,
        )
        if not (200 <= status < 300):
            failures += 1

    if args.mode == "clear" and dust_assets:
        status, body = submit_dust_convert(
            args.sapi_base_url,
            dust_assets,
            api_key,
            api_secret,
            args.recv_window,
        )
        if not (200 <= status < 300):
            is_no_balance = False
            try:
                parsed = json.loads(body)
                is_no_balance = str(parsed.get("code", "")) == "-31002"
            except json.JSONDecodeError:
                pass
            if is_no_balance:
                print("[dust] skip: no eligible dust balance (-31002)")
            else:
                failures += 1

    if failures:
        print(f"有 {failures} 笔订单失败", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
