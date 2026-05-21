#!/usr/bin/env python3
"""Flatten current OKX SWAP exposure with close-position requests.

默认仅打印计划，添加 --execute 后才会实际调用 OKX close-position API。
依赖环境变量 OKX_API_KEY / OKX_API_SECRET / OKX_PASSPHRASE。
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional

DEFAULT_BASE_URL = os.environ.get("OKX_BASE_URL", "https://openapi.okx.com")


@dataclass
class SwapPosition:
    inst_id: str
    pos_side: str
    quantity: Decimal
    mgn_mode: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="按 OKX 当前 SWAP 持仓，使用 close-position 全部平仓（默认 dry-run）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="OKX REST base URL")
    parser.add_argument("--symbols", help="仅处理指定 symbol，逗号或空格分隔，例如 BTCUSDT,ETHUSDT")
    parser.add_argument(
        "--symbol",
        action="append",
        default=[],
        help="追加单个 symbol，可重复传入，例如 --symbol BTCUSDT --symbol ETHUSDT",
    )
    parser.add_argument(
        "--inst-id",
        action="append",
        default=[],
        help="追加单个 instId，可重复传入，例如 --inst-id BTC-USDT-SWAP",
    )
    parser.add_argument("--min-qty", default="0", help="小于该仓位数量的持仓跳过（按 OKX pos 单位）")
    parser.add_argument(
        "--default-mgn-mode",
        choices=["cross", "isolated"],
        default="cross",
        help="当持仓响应缺少 mgnMode 时使用的默认值",
    )
    parser.add_argument(
        "--auto-cancel-pending",
        action="store_true",
        help="平仓时设置 autoCxl=true，允许 OKX 自动撤掉冲突挂单",
    )
    parser.add_argument("--tag", help="可选 OKX tag（1-16 位字母数字）")
    parser.add_argument("--timeout", type=int, default=10, help="HTTP 超时秒数")
    parser.add_argument("--simulate", action="store_true", help="x-simulated-trading: 1 纸上交易")
    parser.add_argument("--execute", action="store_true", help="实际提交 close-position；默认仅打印计划")
    return parser.parse_args()


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
        print(f"ERROR: 请先设置环境变量: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret, passphrase


def parse_decimal(value: Any, *, field: str) -> Decimal:
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise SystemExit(f"无法解析 {field}: {value!r}") from exc


def format_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        normalized = normalized.quantize(Decimal("1"))
    return format(normalized, "f")


def utc_timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def sign(timestamp: str, method: str, request_path: str, body: str, secret: str) -> str:
    payload = f"{timestamp}{method.upper()}{request_path}{body}"
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def json_body(data: Dict[str, Any] | None) -> str:
    if not data:
        return ""
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def request_okx_private(
    base_url: str,
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
    simulated: bool = False,
) -> tuple[int, str, Dict[str, str]]:
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
    except Exception as exc:  # pragma: no cover - network failure
        return 0, str(exc), {}


def okx_response_ok(body: str) -> tuple[bool, str]:
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return False, "non-JSON response body"

    code = str(parsed.get("code", "")).strip()
    msg = str(parsed.get("msg", "")).strip()
    if code == "0":
        return True, ""

    brief = f"code={code} msg={msg}".strip()
    data = parsed.get("data")
    if isinstance(data, list) and data:
        first = data[0] if isinstance(data[0], dict) else None
        if first:
            s_code = str(first.get("sCode", "")).strip()
            s_msg = str(first.get("sMsg", "")).strip()
            if s_code or s_msg:
                brief = f"{brief} sCode={s_code} sMsg={s_msg}".strip()
    return False, brief


def canonical_symbol_from_any(raw: str) -> str:
    s = (raw or "").strip().upper()
    if not s:
        return ""
    if "@" in s:
        s = s.split("@", 1)[0].strip()

    m = re.match(r"^([A-Z0-9]+)-USDT-(SWAP)$", s)
    if m:
        return f"{m.group(1)}USDT"

    m = re.match(r"^([A-Z0-9]+)-USDT$", s)
    if m:
        return f"{m.group(1)}USDT"

    s = re.sub(r"[^A-Z0-9]+", "", s)
    return s


def normalize_okx_um_inst_id(raw: str) -> str:
    s = (raw or "").strip().upper()
    if not s:
        raise ValueError("empty symbol")
    if "@" in s:
        s = s.split("@", 1)[0].strip()
    if re.match(r"^[A-Z0-9]+-USDT-SWAP$", s):
        return s

    sym = canonical_symbol_from_any(s)
    if not sym.endswith("USDT") or len(sym) <= 4:
        raise ValueError(f"OKX 仅支持 USDT 币对（例如 BTCUSDT 或 BTC-USDT-SWAP），实际: {raw}")
    return f"{sym[:-4]}-USDT-SWAP"


def parse_symbol_filters(
    symbols_arg: Optional[str],
    symbols_list: Iterable[str],
    inst_ids: Iterable[str],
) -> Optional[set[str]]:
    values: List[str] = []
    if symbols_arg:
        values.extend(item.strip() for item in re.split(r"[,\s]+", symbols_arg) if item.strip())
    values.extend(item.strip() for item in symbols_list if item and item.strip())
    values.extend(item.strip() for item in inst_ids if item and item.strip())
    if not values:
        return None
    return {normalize_okx_um_inst_id(item) for item in values}


def fetch_positions(
    base_url: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    *,
    timeout: int,
    simulated: bool,
) -> List[SwapPosition]:
    status, body, _headers = request_okx_private(
        base_url=base_url,
        method="GET",
        path="/api/v5/account/positions",
        api_key=api_key,
        api_secret=api_secret,
        passphrase=passphrase,
        params={"instType": "SWAP"},
        timeout=timeout,
        simulated=simulated,
    )
    ok, brief = okx_response_ok(body)
    if not (200 <= status < 300) or not ok:
        raise SystemExit(f"获取 OKX SWAP 持仓失败 status={status} body={brief or body}")

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"解析 OKX 持仓响应失败: {exc}") from exc

    data = parsed.get("data")
    if not isinstance(data, list):
        raise SystemExit("OKX 持仓响应缺少 data 列表")

    positions: List[SwapPosition] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        inst_id = str(entry.get("instId", "")).strip().upper()
        if not inst_id.endswith("-SWAP"):
            continue
        qty_raw = entry.get("pos")
        if qty_raw in (None, ""):
            continue
        qty = parse_decimal(qty_raw, field=f"{inst_id}.pos")
        if qty == 0:
            continue
        pos_side = str(entry.get("posSide") or "net").strip().lower() or "net"
        mgn_mode = str(entry.get("mgnMode") or "").strip().lower()
        positions.append(
            SwapPosition(
                inst_id=inst_id,
                pos_side=pos_side,
                quantity=qty,
                mgn_mode=mgn_mode,
            )
        )
    return positions


def filter_positions(
    positions: Iterable[SwapPosition],
    symbol_filter: Optional[set[str]],
    min_qty: Decimal,
    default_mgn_mode: str,
) -> List[SwapPosition]:
    selected: List[SwapPosition] = []
    for pos in positions:
        if symbol_filter is not None and pos.inst_id not in symbol_filter:
            continue
        if abs(pos.quantity) < min_qty:
            continue
        mgn_mode = pos.mgn_mode or default_mgn_mode
        selected.append(
            SwapPosition(
                inst_id=pos.inst_id,
                pos_side=pos.pos_side or "net",
                quantity=pos.quantity,
                mgn_mode=mgn_mode,
            )
        )
    return selected


def validate_tag(tag: Optional[str]) -> Optional[str]:
    if not tag:
        return None
    value = tag.strip()
    if not re.fullmatch(r"[0-9A-Za-z]{1,16}", value):
        raise SystemExit("--tag 必须是 1-16 位字母数字")
    return value


def submit_close_position(
    base_url: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    position: SwapPosition,
    *,
    auto_cancel_pending: bool,
    tag: Optional[str],
    timeout: int,
    simulated: bool,
) -> int:
    body: Dict[str, Any] = {
        "instId": position.inst_id,
        "mgnMode": position.mgn_mode,
        "posSide": position.pos_side or "net",
    }
    if auto_cancel_pending:
        body["autoCxl"] = True
    if tag:
        body["tag"] = tag

    status, resp_body, headers = request_okx_private(
        base_url=base_url,
        method="POST",
        path="/api/v5/trade/close-position",
        api_key=api_key,
        api_secret=api_secret,
        passphrase=passphrase,
        body=body,
        timeout=timeout,
        simulated=simulated,
    )
    ok, brief = okx_response_ok(resp_body)
    final_ok = (200 <= status < 300) and ok
    tag_name = "OK" if final_ok else "ERR"
    print(
        f"[{position.inst_id}] {tag_name} status={status} reqId={headers.get('x-request-id')} "
        f"posSide={position.pos_side} mgnMode={position.mgn_mode}"
    )
    try:
        print(json.dumps(json.loads(resp_body), ensure_ascii=False, indent=2, sort_keys=True))
    except json.JSONDecodeError:
        print(resp_body)
    if not final_ok and brief:
        print(f"[{position.inst_id}] {brief}", file=sys.stderr)
    return status if final_ok else 0


def main() -> None:
    args = parse_args()
    api_key, api_secret, passphrase = load_credentials()
    min_qty = parse_decimal(args.min_qty, field="min_qty")
    symbol_filter = parse_symbol_filters(args.symbols, args.symbol, args.inst_id)
    base_url = args.base_url.rstrip("/")
    tag = validate_tag(args.tag)

    positions = fetch_positions(
        base_url=base_url,
        api_key=api_key,
        api_secret=api_secret,
        passphrase=passphrase,
        timeout=args.timeout,
        simulated=args.simulate,
    )
    positions = filter_positions(
        positions=positions,
        symbol_filter=symbol_filter,
        min_qty=min_qty,
        default_mgn_mode=args.default_mgn_mode,
    )

    if symbol_filter:
        print(f"symbols filter: {', '.join(sorted(symbol_filter))}")
    print(f"base_url={base_url}")
    print(f"mode={'SIMULATED' if args.simulate else 'REAL'}")
    print(f"auto_cancel_pending={str(args.auto_cancel_pending).lower()}")

    if not positions:
        print("未找到需要平仓的 OKX SWAP 持仓")
        return

    print(f"positions_to_close={len(positions)}")
    for pos in positions:
        print(
            f"- {pos.inst_id}: pos={format_decimal(pos.quantity)} "
            f"posSide={pos.pos_side} mgnMode={pos.mgn_mode}"
        )

    if not args.execute:
        print("dry-run：未提交任何平仓请求，添加 --execute 执行")
        return

    failures = 0
    for pos in positions:
        status = submit_close_position(
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            position=pos,
            auto_cancel_pending=args.auto_cancel_pending,
            tag=tag,
            timeout=args.timeout,
            simulated=args.simulate,
        )
        if status == 0:
            failures += 1

    if failures:
        print(f"有 {failures} 笔 OKX 平仓请求失败", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
