#!/usr/bin/env python3
"""
拉取 Binance UM (USDT-M) exchangeInfo，统计合约面的 contractSize 是否全部为 1。

用法：
  python scripts/check_binance_um_contract_size.py
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from collections import Counter
from typing import Any, Dict, List


EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"


def fetch_exchange_info() -> Dict[str, Any]:
    req = urllib.request.Request(EXCHANGE_INFO_URL, headers={"User-Agent": "binance-contract-check"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        body = resp.read().decode("utf-8", "replace")
        return json.loads(body)


def parse_contract_sizes(symbols: List[Dict[str, Any]]) -> tuple[Counter, List[str]]:
    counter: Counter = Counter()
    missing: List[str] = []
    for sym in symbols:
        if "contractSize" not in sym:
            missing.append(sym.get("symbol", "<unknown>"))
            continue
        # USDT-M 永续/交割合约的 contractSize 字段（字符串或数字）
        size_raw = sym.get("contractSize")
        try:
            size = float(size_raw)
        except (TypeError, ValueError):
            continue
        counter[size] += 1
    return counter, missing


def main() -> int:
    try:
        data = fetch_exchange_info()
    except urllib.error.URLError as exc:
        print(f"[ERR] fetch exchangeInfo failed: {exc}", file=sys.stderr)
        return 1
    symbols: List[Dict[str, Any]] = data.get("symbols", [])
    if not symbols:
        print("[ERR] no symbols in exchangeInfo", file=sys.stderr)
        return 1

    counter, missing = parse_contract_sizes(symbols)
    total = sum(counter.values())
    uniq = len(counter)

    print(f"Total UM symbols: {total}")
    print(f"Unique contractSize values: {uniq}")
    for size, cnt in sorted(counter.items()):
        pct = cnt * 100 / total if total else 0
        print(f"  contractSize={size:g} -> {cnt} ({pct:.2f}%)")

    if missing:
        print(f"\n缺少 contractSize 字段的合约: {len(missing)} 个（只列前 10）")
        print("  " + ", ".join(missing[:10]))

    if uniq == 1 and next(iter(counter)) == 1.0:
        print("结论：当前所有 UM 合约 contractSize = 1")
    else:
        print("结论：存在非 1 的 contractSize，上面已列出分布")

    sample_syms = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BCHUSDT", "AVAXUSDT"]
    print("\n示例（含原始字段）：")
    for sym in sample_syms:
        item = next((s for s in symbols if s.get("symbol") == sym), None)
        if not item:
            print(f"  {sym}: 未找到")
            continue
        subset = {k: item.get(k) for k in [
            "symbol",
            "contractType",
            "baseAsset",
            "quoteAsset",
            "contractSize",
            "deliveryDate",
            "onboardDate",
            "status",
        ]}
        print(f"  {sym}: {subset}")
    return 0 


if __name__ == "__main__":
    raise SystemExit(main())
