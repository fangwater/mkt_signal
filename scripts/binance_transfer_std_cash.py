#!/usr/bin/env python3
"""Transfer cash between Binance standard spot and UM futures via universal transfer API.

Examples:
  source /home/ubuntu/binance-intra-trade01/env.sh
  python scripts/binance_transfer_std_cash.py

  BINANCE_API_KEY=... BINANCE_API_SECRET=... \
    python scripts/binance_transfer_std_cash.py --amount 35000 --execute
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from sell_margin_spot import request_papi


DEFAULT_BASE_URL = (
    os.environ.get("BINANCE_SAPI_URL")
    or os.environ.get("BINANCE_API_URL")
    or "https://api.binance.com"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transfer USDT between Binance UM futures and standard spot"
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="Binance SAPI base URL",
    )
    parser.add_argument(
        "--asset",
        default="USDT",
        help="Transfer asset",
    )
    parser.add_argument(
        "--amount",
        default="35000",
        help="Transfer amount",
    )
    parser.add_argument(
        "--type",
        default="UMFUTURE_MAIN",
        choices=["UMFUTURE_MAIN", "MAIN_UMFUTURE"],
        help="Transfer direction: UMFUTURE_MAIN means futures -> spot",
    )
    parser.add_argument(
        "--recv-window",
        type=int,
        default=5000,
        help="recvWindow in milliseconds",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually submit the transfer; omit for dry-run",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="HTTP timeout seconds",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: please set BINANCE_API_KEY / BINANCE_API_SECRET in environment.", file=sys.stderr)
        return 1

    params = {
        "type": args.type,
        "asset": args.asset.upper().strip(),
        "amount": str(args.amount).strip(),
        "recvWindow": str(args.recv_window),
    }

    base_url = args.base_url.rstrip("/")
    print(
        "Prepared Binance universal transfer: "
        f"type={params['type']} asset={params['asset']} amount={params['amount']} base_url={base_url}"
    )

    if not args.execute:
        print("Dry-run only. Add --execute to submit the transfer.")
        return 0

    status, body, headers = request_papi(
        base_url,
        "/sapi/v1/asset/transfer",
        params,
        api_key,
        api_secret,
        method="POST",
        timeout=args.timeout,
    )

    used_weight = headers.get("x-sapi-used-ip-weight-1m") or headers.get("x-mbx-used-weight-1m")
    tag = "OK" if 200 <= status < 300 else "ERR"
    print(f"Result: {tag} {status}; used_weight={used_weight}")

    try:
        parsed = json.loads(body)
        print(json.dumps(parsed, ensure_ascii=False, indent=2, sort_keys=True))
    except json.JSONDecodeError:
        print(body)

    return 0 if 200 <= status < 300 else 1


if __name__ == "__main__":
    raise SystemExit(main())
