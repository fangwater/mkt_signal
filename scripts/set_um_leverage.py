#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
批量设置 U 本位合约杠杆（仅支持 binance / okex）。

默认逻辑（推荐，适配跨所目录/部署）：
  - 按照 <open-exchange>-<hedge-exchange> 规则推断 key_suffix（例如 okex-binance）
  - 从 Redis 读取:
      cross_fwd_trade_symbols:{key_suffix}
      cross_bwd_trade_symbols:{key_suffix}
    取并集后，对涉及的交易所（open/hedge）批量设置杠杆。

也支持手动指定单个/多个 symbol：
  - --symbol BTCUSDT --symbol ETHUSDT
  - 或指定单边：--exchange binance --symbol BTCUSDT

环境变量：
  - Binance: BINANCE_API_KEY / BINANCE_API_SECRET / BINANCE_PAPI_URL(BINANCE_FAPI_URL)
  - OKX: OKX_API_KEY / OKX_API_SECRET / OKX_PASSPHRASE / OKX_BASE_URL
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
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

SUPPORTED_EXCHANGES = {"binance", "okex"}
NAMESPACE = "cross"


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def normalize_exchange(ex: str) -> str:
    ex = (ex or "").strip().lower()
    if ex == "okx":
        ex = "okex"
    return ex


def exchange_from_venue(venue: str) -> Optional[str]:
    v = (venue or "").strip().lower()
    if not v:
        return None
    ex = v.split("-", 1)[0]
    ex = normalize_exchange(ex)
    return ex or None


def infer_pair_from_name(name: str) -> Optional[Tuple[str, str]]:
    n = (name or "").strip().lower()
    m = re.match(r"^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross([_-].*)?$", n)
    if not m:
        return None
    open_ex = normalize_exchange(m.group(1))
    hedge_ex = normalize_exchange(m.group(2))
    if not open_ex or not hedge_ex:
        return None
    return open_ex, hedge_ex


def infer_pair_from_cwd() -> Optional[Tuple[str, str]]:
    return infer_pair_from_name(Path.cwd().name)


def resolve_open_hedge(args: argparse.Namespace) -> Optional[Tuple[str, str]]:
    if args.open_venue and args.hedge_venue:
        open_ex = exchange_from_venue(args.open_venue)
        hedge_ex = exchange_from_venue(args.hedge_venue)
        if not open_ex or not hedge_ex:
            return None
        return open_ex, hedge_ex

    if args.env_name:
        pair = infer_pair_from_name(args.env_name)
        if pair:
            return pair

    return infer_pair_from_cwd()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Set U-margined leverage for cross symbols (binance/okex only)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--open-venue", help="开仓 venue（例如 okex-futures）")
    p.add_argument("--hedge-venue", help="对冲 venue（例如 binance-futures）")
    p.add_argument("--env-name", help="环境目录名（例如 okex-binance-cross-trade）")
    p.add_argument(
        "--exchange",
        choices=sorted(SUPPORTED_EXCHANGES),
        help="仅设置单边交易所（与 --symbol 搭配使用；不读取 cross 目录/Redis）",
    )
    p.add_argument("--symbol", action="append", help="指定单个币对（可重复）。例如 BTCUSDT / btcusdt")
    p.add_argument("--leverage", type=int, default=4, help="目标杠杆倍数（整数）")
    p.add_argument("--sleep", type=float, default=0.12, help="每个请求之间 sleep 秒数")
    p.add_argument("--timeout", type=int, default=10, help="HTTP 超时秒数")

    p.add_argument(
        "--binance-base-url",
        default=(
            os.environ.get("BINANCE_PAPI_URL")
            or os.environ.get("BINANCE_FAPI_URL")
            or "https://papi.binance.com"
        ),
        help="Binance UM REST base url",
    )
    p.add_argument("--okx-base-url", default=os.environ.get("OKX_BASE_URL", "https://www.okx.com"))
    p.add_argument("--okx-mgn-mode", choices=["cross", "isolated"], default="cross", help="OKX mgnMode")
    return p.parse_args()


def connect_redis(args: argparse.Namespace):
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        sys.exit(2)
    return redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)


def load_redis_list(rds, key: str) -> List[str]:
    raw = rds.get(key)
    if not raw:
        return []
    text = raw.decode("utf-8", "ignore") if isinstance(raw, (bytes, bytearray)) else str(raw)
    try:
        parsed = json.loads(text)
    except Exception as exc:
        print(f"⚠️  解析 Redis key 失败: {key}: {exc}", file=sys.stderr)
        return []
    if not isinstance(parsed, list):
        print(f"⚠️  Redis key 非 JSON 数组: {key}", file=sys.stderr)
        return []
    out: List[str] = []
    for item in parsed:
        if item is None:
            continue
        s = str(item).strip()
        if s:
            out.append(s)
    return out


def canonical_symbol_from_any(raw: str) -> str:
    """
    Canonical symbol form for cross-exchange processing: BTCUSDT / 1000PEPEUSDT.

    Accepts inputs like:
      - BTCUSDT
      - btc-usdt / btc_usdt / btc/usdt
      - BTC-USDT-SWAP (OKX)
      - BTC-USDT-240927 (OKX futures)
    """
    s = (raw or "").strip().upper()
    if not s:
        return ""
    if "@" in s:
        s = s.split("@", 1)[0].strip()

    m = re.match(r"^([A-Z0-9]+)-USDT-(SWAP)$", s)
    if m:
        return f"{m.group(1)}USDT"

    m = re.match(r"^([A-Z0-9]+)-USDT-(\d{6,8})$", s)
    if m:
        return f"{m.group(1)}USDT"

    m = re.match(r"^([A-Z0-9]+)-USDT$", s)
    if m:
        return f"{m.group(1)}USDT"

    # Generic cleanup: keep alnum only (e.g., BTC/USDT -> BTCUSDT)
    s = re.sub(r"[^A-Z0-9]+", "", s)
    return s


def normalize_binance_um_symbol(raw: str) -> str:
    sym = canonical_symbol_from_any(raw)
    if not sym or not sym.endswith("USDT"):
        raise ValueError(f"Binance UM 仅支持 USDT 币对（例如 BTCUSDT），实际: {raw}")
    return sym


def normalize_okx_um_inst_id(raw: str) -> str:
    """
    OKX U 本位 instId:
      - 永续: BTC-USDT-SWAP
      - 交割: BTC-USDT-240927 (允许用户显式传入)
    """
    s = (raw or "").strip().upper()
    if not s:
        raise ValueError("empty symbol")
    if "@" in s:
        s = s.split("@", 1)[0].strip()

    if re.match(r"^[A-Z0-9]+-USDT-SWAP$", s):
        return s
    if re.match(r"^[A-Z0-9]+-USDT-\d{6,8}$", s):
        return s

    sym = canonical_symbol_from_any(s)
    if not sym.endswith("USDT") or len(sym) <= 4:
        raise ValueError(f"OKX 仅支持 USDT 币对（例如 BTCUSDT 或 BTC-USDT-SWAP），实际: {raw}")
    base = sym[: -len("USDT")]
    return f"{base}-USDT-SWAP"


def load_cross_union_symbols(rds, key_suffix: str) -> List[str]:
    fwd_key = f"{NAMESPACE}_fwd_trade_symbols:{key_suffix}"
    bwd_key = f"{NAMESPACE}_bwd_trade_symbols:{key_suffix}"
    fwd = load_redis_list(rds, fwd_key)
    bwd = load_redis_list(rds, bwd_key)

    symbols_set: Set[str] = set()
    for sym in fwd + bwd:
        ns = canonical_symbol_from_any(sym)
        if ns:
            symbols_set.add(ns)

    symbols = sorted(symbols_set)
    print(f"[INFO] Redis fwd={len(fwd)} bwd={len(bwd)} union={len(symbols)} (key_suffix={key_suffix})")
    return symbols


def now_ms() -> int:
    return int(time.time() * 1000)


def binance_sign(query: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def binance_post(
    base_url: str,
    path: str,
    params: Dict[str, Any],
    api_key: str,
    api_secret: str,
    timeout: int,
) -> Tuple[int, str, Dict[str, str]]:
    q = dict(params)
    q.setdefault("recvWindow", "5000")
    q["timestamp"] = str(now_ms())
    items = sorted((k, str(v)) for k, v in q.items())
    query = urllib.parse.urlencode(items, safe="-_.~")
    sig = binance_sign(query, api_secret)
    url = f"{base_url.rstrip('/')}{path}?{query}&signature={sig}"
    req = urllib.request.Request(url, method="POST", headers={"X-MBX-APIKEY": api_key})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
            headers = dict(resp.headers.items())
            return status, body, headers
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        headers = dict(getattr(exc, "headers", {}).items()) if getattr(exc, "headers", None) else {}
        return exc.code, body, headers
    except Exception as exc:  # pragma: no cover - 网络异常
        return 0, str(exc), {}


def okx_utc_timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def okx_json_body(data: Dict[str, Any] | None) -> str:
    if not data:
        return ""
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def okx_sign(timestamp: str, method: str, request_path: str, body: str, secret: str) -> str:
    payload = f"{timestamp}{method.upper()}{request_path}{body}"
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def okx_request(
    base_url: str,
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    body: Dict[str, Any] | None,
    timeout: int,
) -> Tuple[int, str, Dict[str, str]]:
    method = method.upper()
    request_path = path
    body_str = "" if method == "GET" else okx_json_body(body)
    timestamp = okx_utc_timestamp()
    signature = okx_sign(timestamp, method, request_path, body_str, api_secret)

    url = f"{base_url.rstrip('/')}{request_path}"
    data = None if method == "GET" else body_str.encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("OK-ACCESS-KEY", api_key)
    req.add_header("OK-ACCESS-SIGN", signature)
    req.add_header("OK-ACCESS-TIMESTAMP", timestamp)
    req.add_header("OK-ACCESS-PASSPHRASE", passphrase)
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
    except Exception as exc:  # pragma: no cover - 网络异常
        return 0, str(exc), {}


def okx_response_ok(body: str) -> Tuple[bool, str]:
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


def okx_response_code_msg(body: str) -> Tuple[str, str]:
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return "", ""
    code = str(parsed.get("code", "")).strip()
    msg = str(parsed.get("msg", "")).strip()
    return code, msg


def load_binance_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("❌ 请设置 BINANCE_API_KEY 与 BINANCE_API_SECRET", file=sys.stderr)
        sys.exit(2)
    return api_key, api_secret


def load_okx_credentials() -> Tuple[str, str, str]:
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
        print(f"❌ 请设置环境变量: {', '.join(missing)}", file=sys.stderr)
        sys.exit(2)
    return api_key, api_secret, passphrase


def set_binance_um_leverage(
    base_url: str,
    api_key: str,
    api_secret: str,
    symbol: str,
    leverage: int,
    timeout: int,
) -> Tuple[bool, int, str, Dict[str, str]]:
    params = {"symbol": normalize_binance_um_symbol(symbol), "leverage": str(leverage)}
    status, body, headers = binance_post(
        base_url,
        "/papi/v1/um/leverage",
        params,
        api_key,
        api_secret,
        timeout=timeout,
    )
    ok = 200 <= status < 300
    return ok, status, body, headers


def set_okx_um_leverage(
    base_url: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    inst_id: str,
    leverage: int,
    mgn_mode: str,
    timeout: int,
) -> Tuple[bool, int, str, str]:
    inst_id = normalize_okx_um_inst_id(inst_id)
    payload: Dict[str, Any] = {"instId": inst_id, "lever": str(leverage), "mgnMode": mgn_mode}
    status, body, headers = okx_request(
        base_url,
        "POST",
        "/api/v5/account/set-leverage",
        api_key,
        api_secret,
        passphrase,
        body=payload,
        timeout=timeout,
    )
    okx_ok, okx_brief = okx_response_ok(body)
    ok = (200 <= status < 300) and okx_ok
    req_id = headers.get("x-request-id", "")
    return ok, status, body, (okx_brief or req_id)


def ensure_supported_exchange(exchanges: Sequence[str]) -> None:
    unsupported = [ex for ex in exchanges if ex not in SUPPORTED_EXCHANGES]
    if unsupported:
        print(f"❌ 仅支持交易所: {sorted(SUPPORTED_EXCHANGES)}，不支持: {unsupported}", file=sys.stderr)
        sys.exit(2)


def main() -> int:
    args = parse_args()

    if args.leverage <= 0:
        print(f"❌ leverage 必须为正整数，实际: {args.leverage}", file=sys.stderr)
        return 2

    symbol_inputs: List[str] = []
    if args.symbol:
        seen: Set[str] = set()
        for sym in args.symbol:
            raw = (sym or "").strip()
            if not raw:
                continue
            key = raw.upper()
            if key in seen:
                continue
            seen.add(key)
            symbol_inputs.append(raw)
    else:
        pair = resolve_open_hedge(args)
        if not pair:
            print(
                "❌ 需要 --open-venue/--hedge-venue 或 --env-name，或在目录名包含 '<open>-<hedge>-cross-...' 以自动推断",
                file=sys.stderr,
            )
            return 2
        key_suffix = f"{pair[0]}-{pair[1]}"
        rds = connect_redis(args)
        # Redis 列表通常是“通用”币对（例如 solusdt），这里使用 canonical form 作为输入。
        symbol_inputs = load_cross_union_symbols(rds, key_suffix)

    if not symbol_inputs:
        print("❌ 未找到任何 symbols", file=sys.stderr)
        return 2

    exchanges: List[str] = []
    if args.exchange:
        exchanges = [normalize_exchange(args.exchange)]
    else:
        pair = resolve_open_hedge(args)
        if not pair:
            print("❌ 无法推断 open/hedge 交易所", file=sys.stderr)
            return 2
        exchanges = [pair[0], pair[1]]
        exchanges = [normalize_exchange(ex) for ex in exchanges]
        exchanges = list(dict.fromkeys(exchanges).keys())

    ensure_supported_exchange(exchanges)

    # 先按交易所规范化（binance: BTCUSDT；okex: BTC-USDT-SWAP / BTC-USDT-240927）
    targets_by_ex: Dict[str, List[str]] = {}
    for ex in exchanges:
        targets_set: Set[str] = set()
        invalid: List[str] = []
        for raw in symbol_inputs:
            try:
                if ex == "binance":
                    targets_set.add(normalize_binance_um_symbol(raw))
                elif ex == "okex":
                    targets_set.add(normalize_okx_um_inst_id(raw))
                else:
                    invalid.append(raw)
            except Exception:
                invalid.append(raw)

        if invalid:
            print(f"❌ {ex}: 存在无法识别/不支持的 symbol: {invalid}", file=sys.stderr)
            return 2
        targets_by_ex[ex] = sorted(targets_set)

    total_targets = sum(len(v) for v in targets_by_ex.values())
    print(f"[INFO] leverage={args.leverage} inputs={len(symbol_inputs)} exchanges={exchanges} targets={total_targets}")

    for ex in exchanges:
        if ex == "binance":
            api_key, api_secret = load_binance_credentials()
            base_url = args.binance_base_url.rstrip("/")
            symbols = targets_by_ex.get("binance", [])
            print(f"\n[INFO] Binance base_url={base_url} symbols={len(symbols)}")
            ok_count = 0
            for i, sym in enumerate(symbols, start=1):
                ok, status, body, headers = set_binance_um_leverage(
                    base_url,
                    api_key,
                    api_secret,
                    sym,
                    args.leverage,
                    timeout=args.timeout,
                )
                used_w = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
                ord_cnt = headers.get("x-mbx-order-count-1m") or headers.get("x-mbx-order-count")
                tag = "OK" if ok else "ERR"
                print(
                    f"[binance {i}/{len(symbols)}] {sym} -> {args.leverage}: {tag} {status}; used_weight={used_w}, order_count={ord_cnt}"
                )
                if not ok:
                    print(f"  body: {body}")
                else:
                    ok_count += 1
                time.sleep(args.sleep)
            print(f"[INFO] Binance done. success={ok_count}/{len(symbols)}")
            continue

        if ex == "okex":
            api_key, api_secret, passphrase = load_okx_credentials()
            base_url = args.okx_base_url.rstrip("/")
            symbols = targets_by_ex.get("okex", [])
            print(f"\n[INFO] OKX base_url={base_url} mgnMode={args.okx_mgn_mode} symbols={len(symbols)}")
            ok_count = 0
            for i, sym in enumerate(symbols, start=1):
                try:
                    ok, status, body, brief = set_okx_um_leverage(
                        base_url,
                        api_key,
                        api_secret,
                        passphrase,
                        sym,
                        args.leverage,
                        mgn_mode=args.okx_mgn_mode,
                        timeout=args.timeout,
                    )
                except Exception as exc:
                    ok = False
                    status = 0
                    body = str(exc)
                    brief = "invalid symbol"

                tag = "OK" if ok else "ERR"
                print(f"[okex {i}/{len(symbols)}] {sym} -> {args.leverage}: {tag} {status}; {brief}")
                if not ok:
                    print(f"  body: {body}")
                    code, msg = okx_response_code_msg(body)
                    if code == "51039":
                        print(
                            "  hint: OKX PM 账户在 cross 下不允许调整 SWAP/交割合约杠杆；可尝试切换非 PM 账户，"
                            "或仅设置 Binance：--exchange binance",
                            file=sys.stderr,
                        )
                    elif code and msg:
                        pass
                else:
                    ok_count += 1
                time.sleep(args.sleep)
            print(f"[INFO] OKX done. success={ok_count}/{len(symbols)}")
            continue

        print(f"❌ 未支持交易所: {ex}", file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
