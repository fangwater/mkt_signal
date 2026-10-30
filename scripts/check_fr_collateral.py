#!/usr/bin/env python3
"""Check/enable online-asset collateral before same-exchange FR startup.

Run from a deployed FR directory, or pass --env-dir. Default is read-only;
--execute enables missing eligible collateral and verifies the resulting state.
Both FR start scripts execute this before touching existing processes.
Requires Python >= 3.11 and the existing redis Python package.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import http.client
import ipaddress
import json
import os
import re
import subprocess
import sys
import time
import tomllib
import urllib.parse
from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path

from lib.exchange_signing import (
    sign_binance, sign_bitget, sign_bybit, sign_gate, sign_okx,
    utc_timestamp_iso_ms,
)
from lib.intra_symbols import normalize_asset


EXCHANGES = ("bybit", "gate", "bitget", "okex", "binance")
LIST_NAMES = ("dump_symbols", "pos_dump_symbols", "trade_symbols",
              "fwd_trade_symbols", "bwd_trade_symbols", "unimmr_close_symbols")
HOSTS = {"bybit": "api.bybit.com", "gate": "api.gateio.ws",
         "bitget": "api.bitget.com", "okex": "www.okx.com",
         "binance": "papi.binance.com"}
# Restrict reads separately from the collateral-only writes below.
PATHS = {
    "bybit": {"/v5/account/info", "/v5/account/collateral-info"},
    "gate": {"/api/v4/unified/accounts", "/api/v4/unified/currency_discount_tiers"},
    "bitget": {"/api/v3/account/settings", "/api/v3/account/collateral-type",
               "/api/v3/account/custom-collateral-coins"},
    "okex": {"/api/v5/account/config", "/api/v5/account/collateral-assets"},
    "binance": {"/papi/v1/account", "/fapi/v2/account"},
}
POST_PATHS = {"bybit": {"/v5/account/set-collateral-switch"},
              "gate": {"/api/v4/unified/collateral_currencies"},
              "bitget": {"/api/v3/account/set-collateral-type"},
              "okex": {"/api/v5/account/set-collateral-assets"}, "binance": set()}
BITGET_MAINSTREAM = {"USDT", "USDC", "BTC", "DOGE", "ETH", "SOL"}


class PreflightError(RuntimeError):
    """Safe-to-print error: never include raw exchange responses or signed URLs."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise PreflightError(message)


def boolean(value, label: str) -> bool:
    # Do not mistake the string "false" for True, or absent fields for OFF.
    require(type(value) is bool, f"missing/invalid boolean: {label}")
    return value


def rows_by(rows, name: str) -> dict:
    require(isinstance(rows, list) and bool(rows), f"empty/invalid {name} list")
    result = {}
    for row in rows:
        require(isinstance(row, dict), f"invalid {name} row")
        coin = row.get(name)
        require(isinstance(coin, str) and bool(coin.strip()), f"missing {name}")
        coin = coin.strip().upper()
        require(coin not in result, f"duplicate {name}: {coin}")
        result[coin] = row
    return result


@dataclass
class Report:
    env: str
    exchange: str
    assets: list[str]
    mode: str = ""
    disabled: list[str] = field(default_factory=list)
    unsupported: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    enabled: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.disabled and not self.errors


def source_env(env_dir: Path) -> dict[str, str]:
    require((env_dir / "env.sh").is_file(), "missing deployment env.sh")
    inherited = dict(os.environ)
    # A missing key in this env must never silently select a different account.
    for key in list(inherited):
        if key.startswith(("BYBIT_", "GATE_", "BITGET_", "OKX_", "BINANCE_", "REDIS_")):
            inherited.pop(key)
    proc = subprocess.run(
        ["bash", "--noprofile", "--norc", "-c",
         'set -ae; source "$1" >/dev/null 2>&1; env -0', "bash", str(env_dir / "env.sh")],
        cwd=env_dir, env=inherited, capture_output=True, timeout=15, check=False,
    )
    require(proc.returncode == 0, "failed to load deployment env.sh")
    return dict(item.decode().split("=", 1) for item in proc.stdout.split(b"\0") if b"=" in item)


def source_ip(env_dir: Path) -> str:
    configs = [env_dir / n for n in ("trade_engine.toml", "trade engine.toml")
               if (env_dir / n).is_file()]
    require(len(configs) == 1, "expected exactly one deployment trade_engine.toml")
    with configs[0].open("rb") as stream:
        config = tomllib.load(stream)
    ips = config.get("local_ips")
    require(isinstance(ips, list) and bool(ips), "trade_engine.toml: missing local_ips")
    address = ipaddress.ip_address(ips[0])
    require(not address.is_unspecified and not address.is_multicast,
            "trade_engine.toml: local_ips[0] must be a concrete source IP")
    return str(address)


def redis_client(env: dict):
    import redis
    return redis.Redis(host=env.get("REDIS_HOST", "127.0.0.1"),
                       port=int(env.get("REDIS_PORT", "6379")),
                       db=int(env.get("REDIS_DB", "0")),
                       username=env.get("REDIS_USERNAME") or None,
                       password=env.get("REDIS_PASSWORD") or None,
                       socket_timeout=5, socket_connect_timeout=5)


def symbol_list_keys(env_name: str, open_venue: str, hedge_venue: str) -> list[str]:
    return [f"{env_name}:fr_{name}:{open_venue}_{hedge_venue}" for name in LIST_NAMES]


def assets_from_lists(keys: list[str], raw_lists: list) -> list[str]:
    require(len(raw_lists) == len(keys), "incomplete Redis symbol-list response")
    require(any(raw is not None for raw in raw_lists), "FR symbol-list configuration is missing")
    assets = {"USDT"}  # Quote collateral matters even with empty trade lists.
    for key, raw in zip(keys, raw_lists):
        if raw is None:
            continue
        try:
            values = json.loads(raw)
        except (ValueError, TypeError):
            raise PreflightError(f"invalid JSON symbol list: {key}") from None
        require(isinstance(values, list), f"expected JSON symbol list: {key}")
        for value in values:
            require(isinstance(value, str) and bool(value.strip()), f"invalid symbol in {key}")
            asset = normalize_asset(value.split("@", 1)[0])
            require(bool(re.fullmatch(r"[A-Z0-9]+", asset)), f"invalid symbol in {key}")
            assets.add(asset)
    return sorted(assets)


def load_assets(env_name: str, open_venue: str, hedge_venue: str, env: dict, rds=None) -> list[str]:
    rds = rds if rds is not None else redis_client(env)
    keys = symbol_list_keys(env_name, open_venue, hedge_venue)
    return assets_from_lists(keys, rds.mget(keys))


def deployment_context(env_dir: Path, exchange=None, open_venue=None, hedge_venue=None):
    env_dir = env_dir.resolve()
    match = re.fullmatch(r"(bybit|gate|bitget|okex|binance)[-_]fr[-_][a-z0-9][a-z0-9_-]*", env_dir.name)
    require(match is not None, "expected an exchange FR deployment directory")
    actual_exchange = match.group(1)
    require(exchange in {None, actual_exchange}, "exchange/deployment mismatch")
    env = source_env(env_dir)
    require(bool(open_venue) == bool(hedge_venue), "both venues must be provided together")
    open_venue = (open_venue or env.get("OPEN_VENUE") or f"{actual_exchange}-margin").strip().lower()
    hedge_venue = (hedge_venue or env.get("HEDGE_VENUE") or f"{actual_exchange}-futures").strip().lower()
    require({open_venue, hedge_venue} == {f"{actual_exchange}-margin", f"{actual_exchange}-futures"},
            "collateral preflight supports same-exchange spot/futures FR only")
    return env_dir, env, actual_exchange, open_venue, hedge_venue


@contextmanager
def collateral_lock(env_dir: Path):
    # Serialize startup and config-server/CLI writes for this account. Bitget's
    # custom-collateral endpoint replaces its list and must not race our writers.
    run_dir = env_dir / "run"
    run_dir.mkdir(exist_ok=True)
    with (run_dir / "fr_collateral.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


class Client:
    def __init__(self, exchange: str, env: dict, local_ip: str, execute: bool = False):
        self.exchange = exchange
        self.env = env
        self.local_ip = local_ip
        self.execute = execute

    def credential(self, name: str) -> str:
        value = self.env.get(name, "").strip()
        require(bool(value), f"missing credential: {name}")
        return value

    def get(self, path: str, params: dict | None = None):
        require(path in PATHS[self.exchange], "endpoint outside collateral read-only allowlist")
        return self.request("GET", path, params or {}, None)

    def post(self, path: str, body: dict):
        require(self.execute, "collateral writes require --execute")
        require(path in POST_PATHS[self.exchange], "endpoint outside collateral enable allowlist")
        # Every write may only enable collateral. Account mode, leverage,
        # borrowing and transfers are deliberately outside this interface.
        if self.exchange == "bybit":
            require(set(body) == {"coin", "collateralSwitch"} and body["collateralSwitch"] == "ON",
                    "Bybit collateral writes must be ON only")
        elif self.exchange == "gate":
            require(set(body) == {"collateral_type", "enable_list"} and body["collateral_type"] == 1,
                    "Gate collateral writes must only add enabled currencies")
        elif self.exchange == "okex":
            require(set(body) == {"type", "ccyList", "collateralEnabled"}
                    and body["type"] == "custom" and body["collateralEnabled"] is True,
                    "OKX collateral writes must enable custom currencies only")
        else:
            require(set(body) == {"collateralType", "collateralCoins"} and body["collateralType"] == "custom",
                    "Bitget collateral writes must preserve and extend the custom list")
        return self.request("POST", path, {}, body)

    def request(self, method: str, path: str, params: dict, body: dict | None):
        ex = self.exchange
        require((method == "GET" and path in PATHS[ex])
                or (method == "POST" and self.execute and path in POST_PATHS[ex]),
                "request outside collateral allowlist")
        body_text = "" if body is None else json.dumps(body, separators=(",", ":"))
        prefix = "OKX" if ex == "okex" else ex.upper()
        key, secret = (self.credential(f"{prefix}_API_{name}") for name in ("KEY", "SECRET"))
        query = urllib.parse.urlencode(sorted((params or {}).items()))
        target = path + (f"?{query}" if query else "")
        host = HOSTS[ex]
        timestamp = str(int(time.time() * 1000))
        headers = {"Accept": "application/json", "Content-Type": "application/json",
                   "User-Agent": "mkt-signal-fr-preflight"}
        if ex == "bybit":
            headers.update({"X-BAPI-API-KEY": key, "X-BAPI-TIMESTAMP": timestamp,
                            "X-BAPI-RECV-WINDOW": "5000",
                            "X-BAPI-SIGN": sign_bybit(timestamp, key, "5000",
                                                     query if method == "GET" else body_text, secret)})
        elif ex == "gate":
            timestamp = str(int(time.time()))
            headers.update({"KEY": key, "Timestamp": timestamp,
                            "SIGN": sign_gate(method, path, query, body_text, secret, timestamp)})
        elif ex == "okex":
            timestamp = utc_timestamp_iso_ms()
            headers.update({"OK-ACCESS-KEY": key, "OK-ACCESS-TIMESTAMP": timestamp,
                            "OK-ACCESS-PASSPHRASE": self.credential("OKX_PASSPHRASE"),
                            "OK-ACCESS-SIGN": sign_okx(timestamp, method, target, body_text, secret)})
        elif ex == "bitget":
            phrase = self.env.get("BITGET_API_PASSPHRASE") or self.credential("BITGET_PASSPHRASE")
            headers.update({"ACCESS-KEY": key, "ACCESS-TIMESTAMP": timestamp,
                            "ACCESS-PASSPHRASE": phrase,
                            "ACCESS-SIGN": sign_bitget(timestamp, method, target, body_text, secret)})
        else:
            if path.startswith("/fapi/"):
                host = "fapi.binance.com"
            query = urllib.parse.urlencode(sorted({**(params or {}), "timestamp": timestamp,
                                                   "recvWindow": "5000"}.items()))
            target = f"{path}?{query}&signature={sign_binance(query, secret)}"
            headers["X-MBX-APIKEY"] = key
        conn = http.client.HTTPSConnection(host, timeout=10, source_address=(self.local_ip, 0))
        try:
            conn.request(method, target, body=body_text.encode() if body is not None else None, headers=headers)
            response = conn.getresponse()
            body = response.read()
            require(response.status in {200, 204}, f"{ex} {method} {path}: HTTP {response.status}")
            if response.status == 204:
                return None
            data = json.loads(body)
        except PreflightError:
            raise
        except (OSError, http.client.HTTPException, ValueError):
            raise PreflightError(f"{ex} {method} {path}: network/JSON failure") from None
        finally:
            conn.close()
        require(isinstance(data, (dict, list)), f"{ex} GET {path}: invalid response shape")
        if ex == "bybit":
            require(isinstance(data, dict) and data.get("retCode") == 0,
                    f"{ex} GET {path}: exchange rejected request")
            return data.get("result")
        if ex in {"bitget", "okex"}:
            code = "00000" if ex == "bitget" else "0"
            require(isinstance(data, dict) and str(data.get("code")) == code,
                    f"{ex} GET {path}: exchange rejected request")
            return data.get("data")
        return data


def check_bybit(client: Client, report: Report) -> None:
    info = client.get("/v5/account/info")
    require(isinstance(info, dict), "Bybit: invalid account info")
    report.mode = str(info.get("marginMode", ""))
    require(info.get("unifiedMarginStatus") in {3, 4, 5, 6}, "Bybit: account must be UTA")
    require(report.mode in {"REGULAR_MARGIN", "PORTFOLIO_MARGIN"}, "Bybit: cross/portfolio margin required")
    if report.mode == "PORTFOLIO_MARGIN":
        require(info.get("spotHedgingStatus") == "ON", "Bybit: portfolio spot hedging is not ON")
    data = client.get("/v5/account/collateral-info")
    require(isinstance(data, dict), "Bybit: invalid collateral info")
    rows = rows_by(data.get("list"), "currency")
    for coin in report.assets:
        require(coin in rows, f"Bybit: missing collateral status for {coin}")
        row = rows[coin]
        if not boolean(row.get("marginCollateral"), f"{coin}.marginCollateral"):
            report.unsupported.append(coin)
        elif not boolean(row.get("collateralSwitch"), f"{coin}.collateralSwitch"):
            report.disabled.append(coin)


def check_gate(client: Client, report: Report) -> None:
    account = client.get("/api/v4/unified/accounts")
    require(isinstance(account, dict), "Gate: invalid unified account")
    report.mode = str(account.get("mode", ""))
    require(report.mode in {"multi_currency", "portfolio"}, "Gate FR requires multi_currency/portfolio mode")
    require(not boolean(account.get("locked"), "Gate.locked"), "Gate: unified account is locked")
    if report.mode == "portfolio":
        require(boolean(account.get("spot_hedge"), "Gate.spot_hedge"), "Gate: portfolio spot hedging is OFF")
    if boolean(account.get("is_all_collateral"), "Gate.is_all_collateral"):
        report.notes.append("all collateral enabled; platform haircuts still apply")
        return
    balances = account.get("balances")
    require(isinstance(balances, dict), "Gate: missing per-currency collateral status")
    catalog = client.get("/api/v4/unified/currency_discount_tiers")
    # The documented response groups discount rows in an outer array.
    if isinstance(catalog, list) and len(catalog) == 1 and isinstance(catalog[0], list):
        catalog = catalog[0]
    supported = rows_by(catalog, "currency")
    # An eligible coin can have no wallet row before its first buy. Query it
    # explicitly instead of mistaking no inventory for enabled collateral.
    for coin in report.assets:
        if coin not in supported:
            report.unsupported.append(coin)
            continue
        row = balances.get(coin)
        if row is None:
            one = client.get("/api/v4/unified/accounts", {"currency": coin})
            require(isinstance(one, dict) and isinstance(one.get("balances"), dict),
                    f"Gate: missing collateral status for {coin}")
            row = one["balances"].get(coin)
        require(isinstance(row, dict), f"Gate: missing collateral status for {coin}")
        if not boolean(row.get("enabled_collateral"), f"{coin}.enabled_collateral"):
            report.disabled.append(coin)


def check_bitget(client: Client, report: Report) -> None:
    settings = client.get("/api/v3/account/settings")
    require(isinstance(settings, dict), "Bitget: invalid account settings")
    report.mode = str(settings.get("accountLevel", ""))
    require(settings.get("accountMode") in {"unified", "hybrid"}, "Bitget: UTA required")
    require(report.mode == "advanced", "Bitget FR margin trading requires advanced account level")
    require(settings.get("holdMode") == "one_way_mode", "Bitget: one_way_mode required")
    config = client.get("/api/v3/account/collateral-type")
    require(isinstance(config, dict), "Bitget: missing collateral configuration")
    kind = config.get("collateralType")
    require(kind in {"all", "mainstream", "custom"}, "Bitget: unknown collateral type")
    report.notes.append(f"collateralType={kind}")
    if kind == "all":
        return
    platform = rows_by(client.get("/api/v3/account/custom-collateral-coins"), "collateralCoin")
    if kind == "custom":
        coins = config.get("collateralCoins")
        require(isinstance(coins, str), "Bitget: missing custom collateralCoins")
        enabled = {c.strip().upper() for c in coins.split(",") if c.strip()}
    else:
        enabled = BITGET_MAINSTREAM
    for coin in report.assets:
        if coin not in platform:
            report.unsupported.append(coin)
        elif coin not in enabled:
            report.disabled.append(coin)


def check_okex(client: Client, report: Report) -> None:
    config = client.get("/api/v5/account/config")
    require(isinstance(config, list) and len(config) == 1 and isinstance(config[0], dict),
            "OKX: invalid account configuration")
    report.mode = str(config[0].get("acctLv", ""))
    require(report.mode in {"3", "4"}, "OKX FR requires multi-currency/portfolio margin")
    if report.mode == "4":
        # OKX documents collateralEnabled only for multi-currency margin.
        report.notes.append("portfolio mode: per-coin collateral switch is not applicable")
        return
    rows = rows_by(client.get("/api/v5/account/collateral-assets"), "ccy")
    for coin in report.assets:
        require(coin in rows, f"OKX: missing collateral status for {coin}")
        if not boolean(rows[coin].get("collateralEnabled"), f"{coin}.collateralEnabled"):
            report.disabled.append(coin)


def check_binance(client: Client, report: Report) -> None:
    mode = client.env.get("BINANCE_ACCOUNT_MODE", "").strip().upper()
    require(mode in {"UNIFIED", "STANDARD"}, "Binance: explicit BINANCE_ACCOUNT_MODE is required")
    report.mode = mode
    if mode == "UNIFIED":
        data = client.get("/papi/v1/account")
        require(isinstance(data, dict) and data.get("accountStatus") == "NORMAL",
                "Binance: portfolio margin account status is not NORMAL")
        try:
            ratio = Decimal(data["uniMMR"])
            require(ratio.is_finite() and ratio > 0, "Binance: invalid uniMMR")
        except (KeyError, InvalidOperation, TypeError):
            raise PreflightError("Binance: missing/invalid uniMMR") from None
        report.notes.append("portfolio margin verified; no per-coin collateral toggle; haircuts still apply")
    else:
        data = client.get("/fapi/v2/account")
        require(isinstance(data, dict) and boolean(data.get("canTrade"), "Binance.canTrade"),
                "Binance: standard futures account cannot trade")
        report.notes.append("standard Spot/Margin and UM wallets are separate; no shared spot-collateral assertion")


CHECKS = {"bybit": check_bybit, "gate": check_gate, "bitget": check_bitget,
          "okex": check_okex, "binance": check_binance}


def enable_disabled(client: Client, coins: list[str]) -> None:
    """No retries: a failed/unknown write must be followed by a fresh read."""
    if client.exchange == "bybit":
        for coin in coins:
            require(coin not in {"USDT", "USDC"}, "Bybit: stablecoin collateral cannot be switched")
            client.post("/v5/account/set-collateral-switch", {"coin": coin, "collateralSwitch": "ON"})
    elif client.exchange == "gate":
        data = client.post("/api/v4/unified/collateral_currencies",
                           {"collateral_type": 1, "enable_list": coins})
        require(isinstance(data, dict) and data.get("is_success") is True,
                "Gate: collateral update was not acknowledged")
    elif client.exchange == "okex":
        for start in range(0, len(coins), 20):
            client.post("/api/v5/account/set-collateral-assets",
                        {"type": "custom", "ccyList": coins[start:start + 20], "collateralEnabled": True})
    elif client.exchange == "bitget":
        # This endpoint replaces the custom list. Re-read immediately before
        # merging, retaining every existing coin (including those outside FR).
        current = client.get("/api/v3/account/collateral-type")
        require(isinstance(current, dict), "Bitget: cannot re-read collateral configuration")
        kind = current.get("collateralType")
        if kind == "all":
            return
        require(kind in {"mainstream", "custom"}, "Bitget: unknown collateral type before update")
        if kind == "mainstream":
            existing = BITGET_MAINSTREAM
        else:
            raw = current.get("collateralCoins")
            require(isinstance(raw, str), "Bitget: missing existing collateralCoins; refusing replacement")
            existing = {c.strip() for c in raw.split(",") if c.strip()}
        client.post("/api/v3/account/set-collateral-type",
                    {"collateralType": "custom", "collateralCoins": ",".join(sorted(existing | set(coins)))})
    else:
        raise PreflightError("exchange has no supported collateral enable operation")


def ensure_collateral(client: Client, report: Report, execute: bool = False) -> None:
    check = CHECKS[report.exchange]
    check(client, report)
    if not execute or not report.disabled:
        return
    planned = list(report.disabled)
    write_error = None
    try:
        enable_disabled(client, planned)
    except PreflightError as exc:
        write_error = str(exc)
    # Verify even after an unknown write outcome; never blindly resubmit.
    verified = Report(report.env, report.exchange, report.assets)
    check(client, verified)
    report.enabled = sorted(set(planned) - set(verified.disabled) - set(verified.unsupported))
    report.mode, report.disabled = verified.mode, verified.disabled
    report.unsupported, report.notes = verified.unsupported, verified.notes
    if write_error and (report.disabled or report.unsupported):
        raise PreflightError(write_error + "; state re-read, no automatic retry")
    if write_error:
        report.notes.append("write outcome uncertain; subsequent read verified collateral enabled")


def publish_symbol_lists(rds, env_dir: Path, exchange: str, open_venue: str,
                         hedge_venue: str, updates: dict[str, list[str]]) -> Report:
    """Enable collateral for the proposed union before atomically publishing it.

    WATCH protects all six lists while the exchange calls run. A concurrent
    Redis change aborts the publication; it never causes an unverified merge.
    Missing fields are retained and explicit [] remains a valid clear.
    """
    require(bool(updates) and set(updates) <= set(LIST_NAMES), "invalid FR symbol-list update fields")
    for name, values in updates.items():
        require(isinstance(values, list) and all(isinstance(v, str) and v.strip() for v in values),
                f"invalid FR symbol list: {name}")
    env_dir, env, exchange, open_venue, hedge_venue = deployment_context(
        env_dir, exchange, open_venue, hedge_venue)
    keys = symbol_list_keys(env_dir.name, open_venue, hedge_venue)
    with collateral_lock(env_dir), rds.pipeline() as pipe:
        pipe.watch(*keys)
        proposed = list(pipe.mget(keys))
        require(len(proposed) == len(keys), "incomplete Redis symbol-list response")
        for name, values in updates.items():
            proposed[LIST_NAMES.index(name)] = json.dumps(values, ensure_ascii=False)
        report = Report(env_dir.name, exchange, assets_from_lists(keys, proposed))
        ensure_collateral(Client(exchange, env, source_ip(env_dir), execute=True), report, execute=True)
        require(report.ok, "collateral verification failed for: " + ",".join(report.disabled))
        pipe.multi()
        for name, values in updates.items():
            pipe.set(keys[LIST_NAMES.index(name)], json.dumps(values, ensure_ascii=False))
        pipe.execute()
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-dir", type=Path, default=Path.cwd())
    parser.add_argument("--exchange", choices=EXCHANGES)
    parser.add_argument("--open-venue")
    parser.add_argument("--hedge-venue")
    parser.add_argument("--execute", action="store_true", help="enable missing eligible collateral, then re-read")
    args = parser.parse_args()
    report = Report(args.env_dir.name, args.exchange or "", [])
    try:
        env_dir, env, exchange, open_venue, hedge_venue = deployment_context(
            args.env_dir, args.exchange, args.open_venue, args.hedge_venue)
        report.env, report.exchange = env_dir.name, exchange
        with collateral_lock(env_dir):
            report.assets = load_assets(report.env, open_venue, hedge_venue, env)
            ensure_collateral(Client(exchange, env, source_ip(env_dir), execute=args.execute), report, args.execute)
    except PreflightError as exc:
        report.errors.append(str(exc))
    except Exception as exc:
        # Avoid leaking credentials through Redis errors, env.sh output or URLs.
        report.errors.append(f"preflight could not complete ({type(exc).__name__})")
    print(json.dumps({**asdict(report), "ok": report.ok}, ensure_ascii=False, sort_keys=True))
    if not report.ok:
        print("[ERROR] FR collateral preflight failed; no process restart was attempted. "
              "Resolve disabled collateral/account settings or query errors, then retry.", file=sys.stderr)
    if report.unsupported:
        print("[WARN] Platform does not support collateral for: " + ",".join(report.unsupported), file=sys.stderr)
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
