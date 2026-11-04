#!/usr/bin/env python3
"""Read-only FR audit. Excludes gate_fr_arb04 and never submits mutations."""
import hashlib
import http.client
import importlib.util
import json
import os
import re
import socket
import sys
import time
import tomllib
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal as D
from pathlib import Path

sys.dont_write_bytecode = True
ROOTS = sorted(p for p in Path.home().glob("*_fr_arb*") if p.is_dir() and re.fullmatch(r"(binance|bitget|gate|bybit|okex)_fr_arb\d+", p.name) and p.name != "gate_fr_arb04")
HELPER_ROOT = next(p for p in ROOTS if (p / "scripts/check_fr_collateral.py").is_file())
sys.path.insert(0, str(HELPER_ROOT / "scripts"))
spec = importlib.util.spec_from_file_location("audit_collateral_helper", HELPER_ROOT / "scripts/check_fr_collateral.py")
helper = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = helper
spec.loader.exec_module(helper)
assert hashlib.sha256((HELPER_ROOT / "scripts/check_fr_collateral.py").read_bytes()).hexdigest() == "a7d598ea1c2c91a39973b4cc4e95bedcbf5493a724ca2379f5cbfcbb0c4585af"

PATHS = {
    "binance": {"/papi/v1/account", "/papi/v1/balance", "/papi/v1/um/account", "/papi/v1/um/openOrders", "/papi/v1/margin/openOrders", "/papi/v1/margin/myTrades", "/papi/v1/um/userTrades"},
    "gate": {"/api/v4/unified/accounts", "/api/v4/unified/delta_neutral", "/api/v4/futures/usdt/positions", "/api/v4/futures/usdt/orders", "/api/v4/spot/open_orders"},
    "bitget": {"/api/v3/account/assets", "/api/v3/account/settings", "/api/v3/account/collateral-type", "/api/v3/position/current-position", "/api/v3/trade/unfilled-orders"},
    "bybit": {"/v5/account/info", "/v5/account/user-setting-config", "/v5/account/wallet-balance", "/v5/account/collateral-info", "/v5/position/list", "/v5/order/realtime"},
}


def dec(value):
    return D(str(value or 0))


def process_inventory():
    result = {str(p): [] for p in ROOTS}
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            cwd = os.readlink(proc / "cwd")
            if cwd in result:
                result[cwd].append({"pid": proc.name, "comm": (proc / "comm").read_text().strip(), "stderr": os.readlink(proc / "fd/2")})
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            pass
    return result


PROCESSES = process_inventory()


class Reader:
    def __init__(self, exchange, env, local_ip):
        self.exchange, self.env, self.local_ip = exchange, env, local_ip

    def get(self, path, params=None):
        ex, env = self.exchange, self.env
        assert path in PATHS[ex], "Read outside explicit audit scope"
        query = urllib.parse.urlencode(sorted((params or {}).items()))
        target = path + ("?" + query if query else "")
        timestamp = str(int(time.time() * 1000))
        key, secret = env[f"{ex.upper()}_API_KEY"], env[f"{ex.upper()}_API_SECRET"]
        headers = {"Accept": "application/json", "User-Agent": "mkt-signal-read-only-fr-audit"}
        if ex == "binance":
            query = urllib.parse.urlencode(sorted({**(params or {}), "timestamp": timestamp, "recvWindow": "5000"}.items()))
            target = path + "?" + query + "&signature=" + helper.sign_binance(query, secret)
            headers["X-MBX-APIKEY"] = key
        elif ex == "gate":
            timestamp = str(int(time.time()))
            headers.update({"KEY": key, "Timestamp": timestamp, "SIGN": helper.sign_gate("GET", path, query, "", secret, timestamp), "X-Gate-Size-Decimal": "1"})
        elif ex == "bitget":
            headers.update({"ACCESS-KEY": key, "ACCESS-TIMESTAMP": timestamp, "ACCESS-PASSPHRASE": env.get("BITGET_API_PASSPHRASE") or env["BITGET_PASSPHRASE"], "ACCESS-SIGN": helper.sign_bitget(timestamp, "GET", target, "", secret)})
        elif ex == "bybit":
            headers.update({"X-BAPI-API-KEY": key, "X-BAPI-TIMESTAMP": timestamp, "X-BAPI-RECV-WINDOW": "5000", "X-BAPI-SIGN": helper.sign_bybit(timestamp, key, "5000", query, secret)})
        conn = http.client.HTTPSConnection(helper.HOSTS[ex], timeout=12, source_address=(self.local_ip, 0))
        try:
            conn.request("GET", target, headers=headers)
            response = conn.getresponse()
            data = json.loads(response.read())
            if response.status != 200:
                raise RuntimeError(f"{path}: HTTP {response.status}; code={data.get('code', data.get('label')) if isinstance(data, dict) else None}")
            if ex == "bybit":
                if data.get("retCode") != 0:
                    raise RuntimeError(f"{path}: retCode={data.get('retCode')}")
                return data["result"]
            if ex == "bitget":
                if data.get("code") != "00000":
                    raise RuntimeError(f"{path}: code={data.get('code')}")
                return data["data"]
            return data
        finally:
            conn.close()


def rows(data):
    if isinstance(data, list):
        return data
    for key in ["list", "positions", "orders", "orderList", "entrustedList"]:
        if isinstance(data, dict) and isinstance(data.get(key), list):
            return data[key]
    raise RuntimeError("Unrecognized response rows")


def bybit_pages(reader, path, params):
    result, cursor = [], ""
    for _ in range(20):
        page = reader.get(path, {**params, **({"cursor": cursor} if cursor else {})})
        result.extend(rows(page))
        next_cursor = page.get("nextPageCursor", "")
        if not next_cursor:
            return result
        assert next_cursor != cursor, "Repeated pagination cursor"
        cursor = next_cursor
    raise RuntimeError("Pagination incomplete")


def audit(root):
    ex = root.name.split("_", 1)[0]
    out = {"host": socket.getfqdn(), "env": root.name, "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "processes": PROCESSES[str(root)], "errors": []}
    snapshot = None
    try:
        config = tomllib.loads((root / "config/viz.toml").read_text())
        out["viz_port"] = config["servers"][0]["http"]["port"]
        with urllib.request.urlopen(f"http://127.0.0.1:{out['viz_port']}/snapshot", timeout=5) as response:
            snapshot = json.load(response)
        out["snapshot"] = snapshot
    except Exception as error:
        out["snapshot_error"] = str(error)
    try:
        engines = [p for p in PROCESSES[str(root)] if p["comm"] == "trade_engine"]
        if engines:
            assert len(engines) == 1
            env = dict(x.split("=", 1) for x in (Path("/proc") / engines[0]["pid"] / "environ").read_bytes().decode().split("\0") if "=" in x)
        else:
            env = helper.source_env(root)
        reader = Reader(ex, env, helper.source_ip(root))
        out["source_ip"] = reader.local_ip
        calls = {
            "binance": [("account", "/papi/v1/account", {}), ("balances", "/papi/v1/balance", {}), ("positions", "/papi/v1/um/account", {}), ("futures_orders", "/papi/v1/um/openOrders", {}), ("spot_orders", "/papi/v1/margin/openOrders", {})],
            "gate": [("account", "/api/v4/unified/accounts", {}), ("delta", "/api/v4/unified/delta_neutral", {}), ("positions", "/api/v4/futures/usdt/positions", {"holding": "true"}), ("futures_orders", "/api/v4/futures/usdt/orders", {"status": "open", "limit": 100, "page": 1}), ("spot_orders", "/api/v4/spot/open_orders", {"limit": 100, "page": 1})],
            "bitget": [("account", "/api/v3/account/assets", {}), ("settings", "/api/v3/account/settings", {}), ("collateral", "/api/v3/account/collateral-type", {}), ("positions", "/api/v3/position/current-position", {"category": "USDT-FUTURES"}), ("futures_orders", "/api/v3/trade/unfilled-orders", {"category": "USDT-FUTURES", "limit": 100}), ("spot_orders", "/api/v3/trade/unfilled-orders", {"category": "SPOT", "limit": 100})],
            "bybit": [("account", "/v5/account/wallet-balance", {"accountType": "UNIFIED"}), ("settings", "/v5/account/info", {}), ("collateral", "/v5/account/collateral-info", {})],
        }[ex]
        data = {}
        for name, path, params in calls:
            try:
                data[name] = reader.get(path, params)
            except Exception as error:
                out["errors"].append(f"{name}: {error}")
        if ex == "bybit":
            for name, path, params in [
                ("positions", "/v5/position/list", {"category": "linear", "settleCoin": "USDT", "limit": 200}),
                ("futures_orders", "/v5/order/realtime", {"category": "linear", "settleCoin": "USDT", "openOnly": 0, "limit": 50}),
                ("spot_orders", "/v5/order/realtime", {"category": "spot", "openOnly": 0, "limit": 50}),
            ]:
                try:
                    data[name] = bybit_pages(reader, path, params)
                except Exception as error:
                    out["errors"].append(f"{name}: {error}")
        if ex == "gate":
            for name, path, params in calls:
                if name not in {"futures_orders", "spot_orders"} or name not in data:
                    continue
                page = data[name]
                for page_index in range(2, 21):
                    if len(page) < 100:
                        break
                    page = reader.get(path, {**params, "page": page_index})
                    data[name].extend(page)
                else:
                    out["errors"].append(name + ": pagination cap reached")
        if ex == "binance" and "positions" in data:
            account = data["positions"]
            data["positions"] = [p for p in account["positions"] if dec(p.get("positionAmt"))]
            data["um_account_summary"] = {k:v for k,v in account.items() if k not in {"positions", "assets"}}
            data["balances"] = [b for b in data.get("balances", []) if any(dec(b.get(k)) for k in ["totalWalletBalance", "crossMarginAsset", "crossMarginBorrowed", "crossMarginInterest"])]
        out["exchange"] = data
    except Exception as error:
        out["errors"].append(str(error))
    out["logs"] = []
    cutoff = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(time.time() - 900))
    for proc in PROCESSES[str(root)]:
        if proc["comm"] not in {"pre_trade", "trade_engine", "account_monitor", "persist_manager"}:
            continue
        path = Path(proc["stderr"])
        if not path.is_file():
            continue
        with path.open("rb") as stream:
            stream.seek(max(0, path.stat().st_size - 1500000))
            lines = stream.read().decode(errors="replace").splitlines()
        warnings = [line for line in lines if re.match(r"\[\d{4}-", line) and line[1:20] >= cutoff and ("WARN" in line[:100] or "ERROR" in line[:100])]
        out["logs"].append({"comm": proc["comm"], "path": str(path), "mtime": path.stat().st_mtime, "bytes": path.stat().st_size, "recent_warning_count_in_tail": len(warnings), "recent_warning_samples": warnings[-15:]})
    return out


if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=3) as pool:
        for result in pool.map(audit, ROOTS):
            print(json.dumps(result, default=str), flush=True)
