#!/usr/bin/env python3
"""Compare Binance spot/margin vs. USDT-margined futures filters for given symbols."""

import sys
from typing import Dict, Any

import requests

SPOT_URL = "https://api.binance.com/api/v3/exchangeInfo"
FUTURES_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"


def fetch_exchange_info(url: str) -> Dict[str, Any]:
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return {sym["symbol"].upper(): sym for sym in data["symbols"]}


def extract_filters(symbol_info: Dict[str, Any]) -> Dict[str, Any]:
    filters = {f.get("filterType"): f for f in symbol_info.get("filters", [])}
    price = filters.get("PRICE_FILTER", {})
    lot = filters.get("LOT_SIZE", {})
    notional = filters.get("MIN_NOTIONAL", {})
    return {
        "price_tick": price.get("tickSize"),
        "min_price": price.get("minPrice"),
        "max_price": price.get("maxPrice"),
        "step_size": lot.get("stepSize"),
        "min_qty": lot.get("minQty"),
        "max_qty": lot.get("maxQty"),
        "min_notional": notional.get("notional") or notional.get("minNotional"),
    }


def main(symbols):
    spot_map = fetch_exchange_info(SPOT_URL)
    fut_map = fetch_exchange_info(FUTURES_URL)

    for sym in symbols:
        key = sym.upper()
        spot_info = spot_map.get(key)
        futures_info = fut_map.get(key)

        print(f"\n=== {key} ===")

        if spot_info:
            print("Spot/Margin:")
            for k, v in extract_filters(spot_info).items():
                print(f"  {k:12s}: {v}")
        else:
            print("Spot/Margin: symbol not found")

        if futures_info:
            print("Perp Futures:")
            for k, v in extract_filters(futures_info).items():
                print(f"  {k:12s}: {v}")
        else:
            print("Perp Futures: symbol not found")


if __name__ == "__main__":
    targets = sys.argv[1:] or ["BTCUSDT"]
    main(targets)
