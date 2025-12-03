#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Fetch Okx /public/interest-rate-loan-quota and print ccy→rate table."""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, List

API = "https://www.okx.com/api/v5/public/interest-rate-loan-quota"


def fetch_data(ccy: str | None) -> dict:
    params = {}
    if ccy:
        params["ccy"] = ccy
    url = API
    if params:
        url = f"{API}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def build_hash_map(rows: List[dict]) -> Dict[str, float]:
    """Only keep the basic ccy→daily_rate mapping."""
    return {row["ccy"].upper(): float(row["rate"]) for row in rows if "ccy" in row and "rate" in row}


def print_table(hash_map: Dict[str, float]) -> None:
    """Tri-line table (top/bottom borders + header separator)."""
    header_ccy = "ccy"
    header_rate = "rate(%)"
    print("┌──────────┬──────────────┐")
    print(f"│ {header_ccy:<8} │ {header_rate:>12} │")
    print("├──────────┼──────────────┤")
    for ccy in sorted(hash_map):
        print(f"│ {ccy:<8} │ {hash_map[ccy] * 100:>12.8f} │")
    print("└──────────┴──────────────┘")


def main() -> int:
    parser = argparse.ArgumentParser(description="Dump Okx interest loan quota rates (ccy→rate).")
    parser.add_argument("--ccy", help="Optional currency filter. When omitted, Okx currently returns USDT list.")
    args = parser.parse_args()

    try:
        payload = fetch_data(args.ccy)
    except urllib.error.URLError as exc:
        print(f"Failed to fetch Okx API: {exc}", file=sys.stderr)
        return 2

    if payload.get("code") != "0":
        print(f"Okx API error: {payload.get('msg')}", file=sys.stderr)
        return 1

    data = payload.get("data") or []
    if not data:
        print("Okx API returned empty data", file=sys.stderr)
        return 1

    hash_map = build_hash_map(data[0].get("basic", []))
    if not hash_map:
        print("No rate data parsed from Okx response", file=sys.stderr)
        return 1

    print(f"✅ parsed {len(hash_map)} currencies")
    print(f"   example: ZK -> {hash_map.get('ZK')}")
    print_table(hash_map)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
