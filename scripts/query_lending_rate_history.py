#!/usr/bin/env python3
"""Query Binance margin lending rate history (requires API authentication).

Example:
  BINANCE_API_KEY=... BINANCE_API_SECRET=... \\
  python scripts/query_lending_rate_history.py --asset BTC

  # Query multiple assets
  BINANCE_API_KEY=... BINANCE_API_SECRET=... \\
  python scripts/query_lending_rate_history.py --asset BTC --start-time 1704067200000 --end-time 1704153600000

Note:
  - This endpoint requires API signature authentication
  - startTime and endTime are in milliseconds (optional)
  - Default limit is 100, max is 100
"""

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query Binance margin lending rate history",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("BINANCE_API_URL", "https://api.binance.com"),
        help="Base REST endpoint",
    )
    parser.add_argument(
        "--asset",
        required=True,
        help="Asset symbol, e.g. BTC, ETH, BNB",
    )
    parser.add_argument(
        "--vip-level",
        dest="vip_level",
        type=int,
        help="VIP level (0-9), optional",
    )
    parser.add_argument(
        "--start-time",
        dest="start_time",
        help="Start timestamp in milliseconds (optional)",
    )
    parser.add_argument(
        "--end-time",
        dest="end_time",
        help="End timestamp in milliseconds (optional)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Number of records to return (default 100, max 100)",
    )
    parser.add_argument(
        "--recv-window",
        dest="recv_window",
        type=int,
        default=5000,
        help="recvWindow in milliseconds (default 5000)",
    )
    return parser.parse_args()


def now_ms() -> int:
    """Get current timestamp in milliseconds."""
    return int(time.time() * 1000)


def sign(query: str, secret: str) -> str:
    """Generate HMAC SHA256 signature."""
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def request_binance_api(
    base_url: str,
    path: str,
    params: dict,
    api_key: str,
    api_secret: str,
    method: str = "GET",
    timeout: int = 10,
):
    """Make authenticated request to Binance API."""
    q = dict(params)
    q["timestamp"] = str(now_ms())

    # Sort parameters and generate query string
    items = sorted(q.items(), key=lambda kv: kv[0])
    query = urllib.parse.urlencode(items, safe="-_.~")

    # Generate signature
    signature = sign(query, api_secret)

    # Build URL
    url = f"{base_url}{path}?{query}&signature={signature}"

    # Create request
    req = urllib.request.Request(
        url,
        method=method,
        headers={"X-MBX-APIKEY": api_key}
    )

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
    except Exception as exc:
        return 0, str(exc), {}


def format_timestamp(ts_ms: int) -> str:
    """Convert millisecond timestamp to readable format."""
    return datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")


def main() -> None:
    args = parse_args()

    # Get API credentials from environment
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()

    if not api_key or not api_secret:
        print("ERROR: please set BINANCE_API_KEY / BINANCE_API_SECRET in environment.", file=sys.stderr)
        sys.exit(1)

    # Build request parameters
    params = {
        "asset": args.asset.upper().strip(),
        "recvWindow": str(args.recv_window),
    }

    if args.vip_level is not None:
        params["vipLevel"] = str(args.vip_level)
    if args.start_time:
        params["startTime"] = args.start_time
    if args.end_time:
        params["endTime"] = args.end_time
    if args.limit:
        params["size"] = str(min(args.limit, 100))

    base_url = args.base_url.rstrip("/")

    print(f"Querying lending rate history for {params['asset']}...")
    print(f"  Base URL: {base_url}")
    print(f"  Parameters: {json.dumps({k: v for k, v in params.items() if k != 'recvWindow'}, indent=2)}")
    print()

    # Make API request
    status, body, headers = request_binance_api(
        base_url,
        "/sapi/v1/margin/interestRateHistory",
        params,
        api_key,
        api_secret,
        method="GET",
    )

    # Extract rate limit headers
    weight = headers.get("x-mbx-used-weight-1m") or headers.get("x-sapi-used-ip-weight-1m")

    # Print result
    tag = "OK" if 200 <= status < 300 else "ERR"
    print(f"Result: {tag} {status}; used_weight={weight}")
    print()

    # Parse and format response
    try:
        parsed = json.loads(body)

        if 200 <= status < 300:
            # Pretty print with formatted timestamps
            if isinstance(parsed, list) and len(parsed) > 0:
                print(f"Found {len(parsed)} records:")
                print("=" * 80)
                for record in parsed:
                    if "timestamp" in record:
                        record["timestamp_readable"] = format_timestamp(int(record["timestamp"]))
                    print(json.dumps(record, ensure_ascii=False, indent=2))
                    print("-" * 80)
            else:
                formatted = json.dumps(parsed, ensure_ascii=False, indent=2, sort_keys=True)
                print(formatted)
        else:
            # Error response
            formatted = json.dumps(parsed, ensure_ascii=False, indent=2, sort_keys=True)
            print(formatted, file=sys.stderr)
    except json.JSONDecodeError:
        print(body)

    if not (200 <= status < 300):
        sys.exit(1)


if __name__ == "__main__":
    main()
