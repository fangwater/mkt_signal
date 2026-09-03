#!/usr/bin/env python3
"""Query, subscribe, and redeem Binance BFUSD.

Binance moved BFUSD subscription and redemption to Simple Earn in August 2025.
Both account modes therefore use the same Spot-account BFUSD endpoints. Optional
wallet moves adapt that Spot-only contract to this repository's trading modes:

* STANDARD: Spot <-> USD-M Futures (UMFUTURE)
* PM/UNIFIED: Spot <-> Portfolio Margin, collecting the asset first on outflow

All mutating commands are dry-run unless ``--execute`` is supplied. Mutating
requests are never retried automatically because a timeout can leave the remote
result ambiguous. The API key needs Spot & Margin Trading permission. Moving
BFUSD to Standard UM assumes Multi-Assets Mode is already enabled; this script
does not change the futures account mode.

Examples:
  # Read-only BFUSD account and quota queries.
  python3 scripts/binance_bfusd.py account
  python3 scripts/binance_bfusd.py quota

  # Subscribe using USDT already in Spot; leave BFUSD in Spot.
  python3 scripts/binance_bfusd.py subscribe --amount 1000 --execute

  # Move USDT from Standard UM to Spot, subscribe, then move BFUSD back to UM.
  python3 scripts/binance_bfusd.py subscribe --amount 1000 \
      --account-mode STANDARD --from-trading --to-trading --execute

  # Collect PM BFUSD, move it to Spot, then request fast redemption to Spot USDT.
  python3 scripts/binance_bfusd.py redeem --amount 1000 --type FAST \
      --account-mode PM --from-trading --execute
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple


DEFAULT_SAPI_URL = "https://api.binance.com"
DEFAULT_PAPI_URL = "https://papi.binance.com"
BFUSD_ACCOUNT_PATH = "/sapi/v1/bfusd/account"
BFUSD_QUOTA_PATH = "/sapi/v1/bfusd/quota"
BFUSD_SUBSCRIBE_PATH = "/sapi/v1/bfusd/subscribe"
BFUSD_REDEEM_PATH = "/sapi/v1/bfusd/redeem"
UNIVERSAL_TRANSFER_PATH = "/sapi/v1/asset/transfer"
PM_ASSET_COLLECTION_PATH = "/papi/v1/asset-collection"
RECEIVED_BFUSD = "<received-bfusd>"

AUTHORITATIVE_ENV_KEYS = (
    "BINANCE_API_KEY",
    "BINANCE_API_SECRET",
    "BINANCE_ACCOUNT_MODE",
    "BINANCE_SAPI_URL",
    "BINANCE_API_URL",
    "BINANCE_PAPI_URL",
)


class ApiError(RuntimeError):
    """A Binance request failed or returned an invalid response."""


class WorkflowError(RuntimeError):
    """A multi-step mutation failed after zero or more completed steps."""

    def __init__(self, message: str, completed: Sequence[str]) -> None:
        super().__init__(message)
        self.completed = tuple(completed)


@dataclass(frozen=True)
class Step:
    name: str
    description: str
    method: str
    api: str
    path: str
    params: Mapping[str, str]


def now_ms() -> int:
    return int(time.time() * 1000)


def source_env_file(path: Path) -> None:
    if not path.is_file():
        raise SystemExit(f"ERROR: env file not found: {path}")
    command = (
        "set -a && "
        f"source {shlex.quote(str(path))} >/dev/null 2>&1 && "
        "env -0"
    )
    proc = subprocess.run(
        ["bash", "-lc", command],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        detail = proc.stderr.decode("utf-8", errors="replace").strip()
        raise SystemExit(f"ERROR: failed to source {path}: {detail}")

    loaded: Dict[str, str] = {}
    for item in proc.stdout.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key_raw, value_raw = item.split(b"=", 1)
        key = key_raw.decode("utf-8", errors="ignore")
        if key in AUTHORITATIVE_ENV_KEYS:
            loaded[key] = value_raw.decode("utf-8", errors="replace")

    for key, value in loaded.items():
        old_value = os.environ.get(key)
        if old_value and old_value != value:
            print(
                f"WARN: {path} overrides existing {key}; env-file value wins for account safety",
                file=sys.stderr,
            )
        os.environ[key] = value


def maybe_source_env_file(explicit_path: Optional[str]) -> None:
    if explicit_path:
        source_env_file(Path(explicit_path).expanduser().resolve())
        return
    local = Path.cwd() / "env.sh"
    if local.is_file():
        source_env_file(local)


def normalize_account_mode(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.strip().upper().replace("-", "_")
    if not normalized or normalized == "AUTO":
        return None
    if normalized in {"STANDARD", "STD"}:
        return "STANDARD"
    if normalized in {"PM", "UNIFIED", "PORTFOLIO", "PORTFOLIO_MARGIN"}:
        return "PM"
    raise ValueError(
        "account mode must be AUTO, STANDARD/STD, or PM/UNIFIED/PORTFOLIO_MARGIN"
    )


def resolve_account_mode(cli_value: str, trading_move_requested: bool) -> Optional[str]:
    cli_mode = normalize_account_mode(cli_value)
    env_mode = normalize_account_mode(os.environ.get("BINANCE_ACCOUNT_MODE"))
    if cli_mode is not None and env_mode is not None and cli_mode != env_mode:
        raise ValueError(
            f"--account-mode={cli_mode} conflicts with "
            f"BINANCE_ACCOUNT_MODE={env_mode}; refusing to select a wallet path"
        )
    mode = cli_mode or env_mode
    if trading_move_requested and mode is None:
        raise ValueError(
            "--account-mode is required for --from-trading/--to-trading when "
            "BINANCE_ACCOUNT_MODE is not set"
        )
    return mode


def normalize_amount(value: str) -> str:
    try:
        amount = Decimal(value.strip())
    except (InvalidOperation, ValueError, AttributeError) as exc:
        raise ValueError(f"invalid amount: {value!r}") from exc
    if not amount.is_finite() or amount <= 0:
        raise ValueError("amount must be a finite number greater than zero")
    text = format(amount, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def load_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    missing = [
        name
        for name, value in (
            ("BINANCE_API_KEY", api_key),
            ("BINANCE_API_SECRET", api_secret),
        )
        if not value
    ]
    if missing:
        raise ValueError(f"missing {', '.join(missing)}")
    return api_key, api_secret


def sign_query(query: str, api_secret: str) -> str:
    return hmac.new(
        api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def signed_payload(
    params: Mapping[str, Any],
    api_secret: str,
    recv_window: int,
    timestamp_ms: Optional[int] = None,
) -> str:
    items = [(key, str(value)) for key, value in params.items() if value is not None]
    items.append(("recvWindow", str(recv_window)))
    items.append(("timestamp", str(now_ms() if timestamp_ms is None else timestamp_ms)))
    # Binance requires signing the percent-encoded payload exactly as transmitted.
    query = urllib.parse.urlencode(items, safe="-_.~")
    return f"{query}&signature={sign_query(query, api_secret)}"


class BinanceClient:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        sapi_url: str,
        papi_url: str,
        recv_window: int,
        timeout: float,
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.sapi_url = sapi_url.rstrip("/")
        self.papi_url = papi_url.rstrip("/")
        self.recv_window = recv_window
        self.timeout = timeout

    def request(
        self,
        api: str,
        method: str,
        path: str,
        params: Mapping[str, Any],
    ) -> Any:
        base_url = self.sapi_url if api == "sapi" else self.papi_url
        method = method.upper()
        payload = signed_payload(params, self.api_secret, self.recv_window)
        url = f"{base_url}{path}"
        data: Optional[bytes] = None
        if method == "GET":
            url = f"{url}?{payload}"
        else:
            data = payload.encode("ascii")

        request = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={
                "X-MBX-APIKEY": self.api_key,
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                status = response.getcode()
                raw = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            status = exc.code
            raw = exc.read().decode("utf-8", errors="replace")
        except urllib.error.URLError as exc:
            raise ApiError(f"{method} {path} transport error: {exc.reason}") from exc
        except TimeoutError as exc:
            raise ApiError(f"{method} {path} timed out; remote result may be ambiguous") from exc

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ApiError(
                f"{method} {path} HTTP {status} returned non-JSON: {raw[:500]}"
            ) from exc
        if not 200 <= status < 300:
            if isinstance(parsed, dict):
                code = parsed.get("code")
                message = parsed.get("msg") or parsed.get("message")
                detail = f"code={code} msg={message}"
            else:
                detail = json.dumps(parsed, ensure_ascii=True)
            raise ApiError(f"{method} {path} HTTP {status}: {detail}")
        return parsed


def universal_transfer_step(
    name: str,
    description: str,
    transfer_type: str,
    asset: str,
    amount: str,
) -> Step:
    return Step(
        name=name,
        description=description,
        method="POST",
        api="sapi",
        path=UNIVERSAL_TRANSFER_PATH,
        params={"type": transfer_type, "asset": asset, "amount": amount},
    )


def pm_collection_step(asset: str) -> Step:
    return Step(
        name=f"collect_{asset.lower()}_in_pm",
        description=f"collect {asset} from PM futures wallets into the Portfolio Margin wallet",
        method="POST",
        api="papi",
        path=PM_ASSET_COLLECTION_PATH,
        params={"asset": asset},
    )


def build_subscribe_plan(
    amount: str,
    asset: str,
    account_mode: Optional[str],
    from_trading: bool,
    to_trading: bool,
) -> list[Step]:
    amount = normalize_amount(amount)
    asset = asset.strip().upper()
    if asset not in {"USDT", "USDC"}:
        raise ValueError("BFUSD subscription asset must be USDT or USDC")
    if (from_trading or to_trading) and account_mode not in {"STANDARD", "PM"}:
        raise ValueError("STANDARD or PM account mode is required for trading-wallet moves")

    steps: list[Step] = []
    if from_trading:
        if account_mode == "PM":
            steps.append(pm_collection_step(asset))
            transfer_type = "PORTFOLIO_MARGIN_MAIN"
            source = "PM"
        else:
            transfer_type = "UMFUTURE_MAIN"
            source = "Standard UM"
        steps.append(
            universal_transfer_step(
                f"transfer_{asset.lower()}_to_spot",
                f"transfer {amount} {asset} from {source} to Spot",
                transfer_type,
                asset,
                amount,
            )
        )

    steps.append(
        Step(
            name="subscribe_bfusd",
            description=f"subscribe BFUSD with {amount} {asset} from Spot",
            method="POST",
            api="sapi",
            path=BFUSD_SUBSCRIBE_PATH,
            params={"asset": asset, "amount": amount},
        )
    )

    if to_trading:
        if account_mode == "PM":
            transfer_type = "MAIN_PORTFOLIO_MARGIN"
            destination = "PM"
        else:
            transfer_type = "MAIN_UMFUTURE"
            destination = "Standard UM"
        steps.append(
            universal_transfer_step(
                "transfer_bfusd_to_trading",
                f"transfer the received BFUSD from Spot to {destination}",
                transfer_type,
                "BFUSD",
                RECEIVED_BFUSD,
            )
        )
    return steps


def build_redeem_plan(
    amount: str,
    redemption_type: str,
    account_mode: Optional[str],
    from_trading: bool,
) -> list[Step]:
    amount = normalize_amount(amount)
    redemption_type = redemption_type.strip().upper()
    if redemption_type not in {"FAST", "STANDARD"}:
        raise ValueError("redemption type must be FAST or STANDARD")
    if from_trading and account_mode not in {"STANDARD", "PM"}:
        raise ValueError("STANDARD or PM account mode is required for trading-wallet moves")

    steps: list[Step] = []
    if from_trading:
        if account_mode == "PM":
            steps.append(pm_collection_step("BFUSD"))
            transfer_type = "PORTFOLIO_MARGIN_MAIN"
            source = "PM"
        else:
            transfer_type = "UMFUTURE_MAIN"
            source = "Standard UM"
        steps.append(
            universal_transfer_step(
                "transfer_bfusd_to_spot",
                f"transfer {amount} BFUSD from {source} to Spot",
                transfer_type,
                "BFUSD",
                amount,
            )
        )

    steps.append(
        Step(
            name="redeem_bfusd",
            description=f"redeem {amount} BFUSD ({redemption_type}) to Spot USDT",
            method="POST",
            api="sapi",
            path=BFUSD_REDEEM_PATH,
            params={"amount": amount, "type": redemption_type},
        )
    )
    return steps


def printable_params(params: Mapping[str, str]) -> str:
    return urllib.parse.urlencode(list(params.items()), safe="<>-_.~")


def print_plan(steps: Iterable[Step], sapi_url: str, papi_url: str) -> None:
    print("BFUSD workflow plan:")
    for index, step in enumerate(steps, start=1):
        base_url = sapi_url if step.api == "sapi" else papi_url
        print(f"  {index}. {step.description}")
        print(
            f"     {step.method} {base_url.rstrip('/')}{step.path} "
            f"[{printable_params(step.params)}]"
        )


def response_succeeded(step: Step, payload: Any) -> None:
    if step.name in {"subscribe_bfusd", "redeem_bfusd"}:
        if not isinstance(payload, dict) or payload.get("success") is not True:
            raise ApiError(f"{step.path} returned an unsuccessful payload: {payload!r}")


def execute_plan(client: BinanceClient, steps: Sequence[Step]) -> list[Tuple[Step, Any]]:
    completed: list[str] = []
    results: list[Tuple[Step, Any]] = []
    received_bfusd: Optional[str] = None

    for step in steps:
        params = dict(step.params)
        if params.get("amount") == RECEIVED_BFUSD:
            if received_bfusd is None:
                raise WorkflowError(
                    "subscription response did not provide a positive bfusdAmount; "
                    "received BFUSD may remain in Spot",
                    completed,
                )
            params["amount"] = received_bfusd
        try:
            payload = client.request(step.api, step.method, step.path, params)
            response_succeeded(step, payload)
            if step.name == "subscribe_bfusd":
                if not isinstance(payload, dict):
                    raise ApiError("BFUSD subscription response is not an object")
                received_bfusd = normalize_amount(str(payload.get("bfusdAmount", "")))
        except (ApiError, ValueError) as exc:
            raise WorkflowError(f"step {step.name} failed: {exc}", completed) from exc

        completed.append(step.name)
        results.append((step, payload))
        print(f"OK: {step.description}")
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return results


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--env-file",
        help="source this env.sh first; otherwise auto-source ./env.sh when present",
    )
    parser.add_argument(
        "--sapi-url",
        help="Binance Spot/SAPI base URL (default: BINANCE_SAPI_URL or api.binance.com)",
    )
    parser.add_argument(
        "--papi-url",
        help="Binance Portfolio Margin base URL (default: BINANCE_PAPI_URL or papi.binance.com)",
    )
    parser.add_argument(
        "--account-mode",
        default="AUTO",
        help="AUTO, STANDARD/STD, or PM/UNIFIED; required only for trading-wallet moves",
    )
    parser.add_argument("--recv-window", type=int, default=5000, help="recvWindow in milliseconds")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout in seconds")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query, subscribe, and redeem Binance BFUSD",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name, help_text in (
        ("account", "query BFUSD account information (read-only)"),
        ("quota", "query BFUSD subscription/redemption quotas (read-only)"),
    ):
        subparser = subparsers.add_parser(name, help=help_text)
        add_common_args(subparser)

    subscribe = subparsers.add_parser("subscribe", help="subscribe BFUSD using Spot USDT/USDC")
    add_common_args(subscribe)
    subscribe.add_argument("--amount", required=True, help="USDT/USDC amount to subscribe")
    subscribe.add_argument(
        "--asset",
        default="USDT",
        help="subscription payment asset: USDT or USDC",
    )
    subscribe.add_argument(
        "--from-trading",
        action="store_true",
        help="first move the payment asset from Standard UM or PM to Spot",
    )
    subscribe.add_argument(
        "--to-trading",
        action="store_true",
        help="move the received BFUSD from Spot to Standard UM or PM",
    )
    subscribe.add_argument("--execute", action="store_true", help="submit all mutating steps")

    redeem = subparsers.add_parser("redeem", help="redeem Spot BFUSD to Spot USDT")
    add_common_args(redeem)
    redeem.add_argument("--amount", required=True, help="BFUSD amount to redeem")
    redeem.add_argument(
        "--type",
        dest="redemption_type",
        default="STANDARD",
        help="FAST or STANDARD redemption",
    )
    redeem.add_argument(
        "--from-trading",
        action="store_true",
        help="first move BFUSD from Standard UM or PM to Spot",
    )
    redeem.add_argument("--execute", action="store_true", help="submit all mutating steps")
    return parser.parse_args(argv)


def resolved_urls(args: argparse.Namespace) -> Tuple[str, str]:
    sapi_url = (
        args.sapi_url
        or os.environ.get("BINANCE_SAPI_URL")
        or os.environ.get("BINANCE_API_URL")
        or DEFAULT_SAPI_URL
    )
    papi_url = args.papi_url or os.environ.get("BINANCE_PAPI_URL") or DEFAULT_PAPI_URL
    return sapi_url.rstrip("/"), papi_url.rstrip("/")


def run_read_only(args: argparse.Namespace, sapi_url: str, papi_url: str) -> int:
    api_key, api_secret = load_credentials()
    client = BinanceClient(
        api_key,
        api_secret,
        sapi_url,
        papi_url,
        args.recv_window,
        args.timeout,
    )
    path = BFUSD_ACCOUNT_PATH if args.command == "account" else BFUSD_QUOTA_PATH
    payload = client.request("sapi", "GET", path, {})
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    try:
        if args.recv_window <= 0 or args.recv_window > 60000:
            raise ValueError("recv-window must be in 1..60000 milliseconds")
        if args.timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        maybe_source_env_file(args.env_file)
        sapi_url, papi_url = resolved_urls(args)

        if args.command in {"account", "quota"}:
            return run_read_only(args, sapi_url, papi_url)

        trading_move = args.from_trading or (
            args.command == "subscribe" and args.to_trading
        )
        account_mode = resolve_account_mode(args.account_mode, trading_move)
        if args.command == "subscribe":
            steps = build_subscribe_plan(
                args.amount,
                args.asset,
                account_mode,
                args.from_trading,
                args.to_trading,
            )
        else:
            steps = build_redeem_plan(
                args.amount,
                args.redemption_type,
                account_mode,
                args.from_trading,
            )

        print(f"account_mode={account_mode or 'SPOT_ONLY'} execute={args.execute}")
        print_plan(steps, sapi_url, papi_url)
        if not args.execute:
            print("Dry-run only. Add --execute to submit this workflow.")
            return 0

        api_key, api_secret = load_credentials()
        client = BinanceClient(
            api_key,
            api_secret,
            sapi_url,
            papi_url,
            args.recv_window,
            args.timeout,
        )
        execute_plan(client, steps)
        return 0
    except WorkflowError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        if exc.completed:
            print(
                "WARNING: remote state changed before failure; completed steps: "
                + ", ".join(exc.completed),
                file=sys.stderr,
            )
        return 1
    except (ApiError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
