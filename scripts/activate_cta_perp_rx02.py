#!/usr/bin/env python3
"""Operator-run RX02 cutover. Default is read-only; --execute can cause trades."""
from __future__ import annotations

import argparse
import copy
import datetime
import fcntl
import getpass
import hashlib
import http.cookiejar
import json
import math
import os
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def read(path):
    return json.loads(path.read_text())


def write(path, value):
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, indent=2) + "\n")
    os.replace(temp, path)


def positions(path):
    state = read(path)
    require(isinstance(state, dict) and state, "missing old position state")
    for factor, rows in state.items():
        require(isinstance(rows, dict), f"invalid positions: {factor}")
        for symbol, row in rows.items():
            require(row["flag"] in (-1, 0, 1), f"invalid flag: {factor}/{symbol}")
            require(all(math.isfinite(row.get(k, 0)) for k in ("qty", "entry_notional")),
                    f"invalid quantity: {factor}/{symbol}")
    return state


def pids(env, executable):
    found = []
    for proc in Path("/proc").glob("[0-9]*"):
        try:
            if proc.joinpath("cwd").resolve() == env and proc.joinpath("exe").resolve().name == executable:
                found.append(int(proc.name))
        except (OSError, RuntimeError):
            pass
    return found


def wrapper(env, name):
    subprocess.run([str(env / "scripts" / (name + ".sh"))], cwd=env, check=True)


def wait_for(check, message, timeout=30):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        if check():
            return
        time.sleep(1)
    raise RuntimeError(message)


def warm_rows(env, table):
    files = sorted((env / "data/cta_perp/factor_history").glob("*.jsonl"))
    require(files, "factor publisher has no history yet")
    latest = {}
    # The last 1 MiB covers multiple complete minutes without loading days of history.
    with files[-1].open("rb") as stream:
        stream.seek(max(0, files[-1].stat().st_size - 1024 * 1024))
        if stream.tell():
            stream.readline()
        for line in stream:
            try:
                row = json.loads(line)
                latest[row["symbol"]] = row
            except json.JSONDecodeError:
                continue
    now = time.time() * 1000
    for symbol, factors in table.items():
        row = latest.get(symbol, {})
        ts = row.get("model_ts_ms", 0)
        stamp = datetime.datetime.fromtimestamp(ts / 1000, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        require(0 <= now - row.get("model_ts_ms", 0) <= 180_000,
                f"Kafka history not live: {symbol}; latest={stamp}; lag={(now-ts)/60000:.1f} min; wait for catch-up")
        require(row.get("nq", {}).get("ready"), f"NQ not ready: {symbol}")
        for factor in factors:
            require(row.get("inputs", {}).get(factor, {}).get("ready"),
                    f"factor not ready: {symbol}/{factor}")


def check_preview(env):
    require(read(env / "config/cta_perp.json")["enabled"] is False,
            "new publication is already enabled")
    if pids(env, "cta_perp_signal"):
        require(read(env / "run/cta_perp_status.json")["enabled"] is False,
                "running preview has not confirmed publication disabled")


def inherit_positions(old, new):
    require(not pids(new, "cta_perp_signal"), "stop the preview before replacing its positions")
    require(not pids(old, "cta_special_signal"), "stop the old signal before taking its final positions")
    state = positions(old / "run/cta_special_position_flags.json")
    preview = new / "run/cta_perp_position_flags.json"
    if preview.exists():
        write(new / "run/cta_perp_position_flags.preview.json", read(preview))
    write(new / "run/cta_special_position_flags.before.json", state)
    write(preview, state)


def binding_plan(account, factors, prefix):
    require(account["can_configure"], "Manager account configuration permission required")
    require(account["name"] == "lgq_cta", "unexpected virtual account alias")
    followers = account["followers"]
    require(len(followers) == 1 and followers[0]["source_id"] == "binance_exec_trade05"
            and followers[0]["multiplier"] == 1 and not followers[0]["pending_publishes"],
            "unexpected followers, multiplier, or pending publishes; inspect Manager first")
    old = {"factor_036", "factor_051", "factor_170", "TD_TI_002", "baseline_004"}
    rows = copy.deepcopy(account["bindings"])
    require(len(rows) == 5 and {r["position_strategy_name"] for r in rows} == {prefix + f for f in old},
            "old Manager bindings changed; inspect before migration")
    require(all(r["shares"] == 1 and r["order_strategy_name"] == "lgq_cta_ctaker" for r in rows),
            "old Manager shares or execution template changed")
    desired = {prefix + f for f in factors}
    for row in rows:
        if row["position_strategy_name"] not in desired:
            row["shares"] = 0
    present = {r["position_strategy_name"] for r in rows}
    for name in sorted(desired - present):
        require(name not in {r["binding_name"] for r in rows}, "binding name collision")
        rows.append(dict(binding_name=name, position_strategy_name=name,
                         order_strategy_name="lgq_cta_ctaker", shares=1))
    return {"name": account["name"], "bindings": rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    new = Path(__file__).resolve().parents[1]
    require(new.name == "binance-cta-perp-rx02", "run the installed environment-local script")
    old = new.parent / "binance-cta-special-rx02"
    os.chdir(new)
    require(not (new / "run/cutover_started.json").exists(),
            "cutover already attempted; inspect state before any retry")
    subprocess.run(["sha256sum", "--check", "--status", "release.sha256"], check=True)
    table = read(new / "config/cta_perp_symbol_factors.json")
    cfg = read(old / "config/cta_special.json")
    staged = read(new / "config/cta_perp.json")
    require(cfg["enabled"] is True and staged["enabled"] is False, "unexpected enabled state")
    require({**cfg, "enabled": False} == staged and cfg["max_pos_u"] == 100,
            "live parameters changed since staging")
    require(os.getenv("CTA_POSITION_STRATEGY_PREFIX") == "lgq_cta_", "source the staged env.sh first")
    require(pids(old, "cta_special_signal"), "old RX02 signal is not running")
    check_preview(new)
    state = positions(old / "run/cta_special_position_flags.json")
    factors = sorted({f for row in table.values() for f in row})
    print("[PLAN] JP RX02 Binance futures -> binance_exec_trade05; 9 symbols / 24 bindings; 100 USDT per binding")
    print("[PLAN] stop old signal; copy ALL old positions; retain lgq_cta_ names; retire old bindings at zero shares")
    print("[PLAN] add four factor bindings at one share; start CTA Perp; new targets may CLOSE or OPEN real positions")
    print("[PLAN] no rollback to the old publisher after cutover starts; old shared factor publisher stays running")
    print(f"[STATE] old factors={len(state)} positions={sum(len(v) for v in state.values())}")
    if not args.execute:
        try:
            warm_rows(new, table)
            print("[READY] all configured factors and NQ are live")
        except RuntimeError as error:
            print(f"[WAIT] {error}")
        print("[DRY-RUN] no process, config, Redis or Manager changes; --execute prompts for Manager login")
        return
    lock = (new / "run/cutover.lock").open("w")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    require(not (new / "run/cutover_started.json").exists(), "another cutover already started")
    require(pids(new, "cta_perp_factor_model_1m_pub"), "new publisher is not running")
    warm_rows(new, table)
    from cta_perp_factor_config import read_factor_table
    import redis
    client = redis.Redis(host=os.getenv("REDIS_HOST", "127.0.0.1"), port=int(os.getenv("REDIS_PORT", "6379")),
                         db=int(os.getenv("REDIS_DB", "0")), username=os.getenv("REDIS_USERNAME"),
                         password=os.getenv("REDIS_PASSWORD"), socket_timeout=3)
    require(read_factor_table(client, new.name)["symbol_factors"] == table, "staged Redis table changed")
    base = os.environ["CTA_MANAGER_API_URL"].rstrip("/")
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPCookieProcessor(jar))

    def api(path, payload=None, method=None):
        request = urllib.request.Request(base + "/" + path,
            data=None if payload is None else json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"}, method=method)
        try:
            with opener.open(request, timeout=20) as response:
                body = response.read()
                return json.loads(body) if body else None
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"Manager {path}: HTTP {error.code}") from None

    session = api("auth/login", {"username": input("Manager username: "), "password": getpass.getpass("Manager password: ")})
    require(session["user"]["role"] == "admin", "an admin login is required to inspect all affected bindings")
    accounts = [a for a in api("catalog/virtual-accounts") if a["name"] == "lgq_cta"]
    require(len(accounts) == 1, "expected exactly one lgq_cta virtual account")
    account = accounts[0]
    plan = binding_plan(account, factors, "lgq_cta_")
    catalog = api("catalog/position-strategies")
    existing = {r["strategy_name"] for r in catalog}
    new_names = {"lgq_cta_" + f for f in factors} - {r["position_strategy_name"] for r in account["bindings"]}
    require(not new_names.intersection(existing), "a new strategy already exists; inspect its ownership and targets")
    # Verify the prepared proxy file still applies before stopping any signal.
    nginx = Path("/etc/nginx/sites-available/crypto_proxy_4191.conf")
    require(hashlib.sha256(nginx.read_bytes()).hexdigest() == (new / "run/nginx.before.sha256").read_text().strip(),
            "Nginx changed since staging; regenerate the candidate")
    subprocess.run(["sudo", "-n", "nginx", "-t"], check=True)
    warm_rows(new, table)
    require(read(old / "config/cta_special.json") == cfg, "old parameters changed during preflight")
    require(next(a for a in api("catalog/virtual-accounts") if a["virtual_id"] == account["virtual_id"]) == account,
            "Manager configuration changed during preflight")
    write(new / "run/cutover_started.json", {"time": time.time(), "manager_before": account, "manager_after": plan})
    try:
        check_preview(new)
        wrapper(new, "stop_cta_perp_signal")
        wait_for(lambda: not pids(new, "cta_perp_signal"), "preview signal did not stop")
        wrapper(old, "stop_cta_special_signal")
        wait_for(lambda: not pids(old, "cta_special_signal"), "old signal did not stop")
        inherit_positions(old, new)
        # Register only previously absent strategies at zero. Never overwrite retained NEAR inventory.
        for factor in factors:
            name = "lgq_cta_" + factor
            if name in new_names:
                api("catalog/position-strategies", {"strategy_name": name,
                    "targets": {s: {"qty": 0, "signal": 0} for s, row in table.items() if factor in row}})
        api("catalog/virtual-accounts/" + account["virtual_id"], plan, "PUT")
        wrapper(old, "stop_cta_special_config_server")
        wrapper(old, "stop_cta_special_dashboard")
        wrapper(new, "start_cta_perp_config_server")
        wrapper(new, "start_cta_perp_dashboard")
        # Start enabled so the signal's historical-bar guard preserves inherited quantities.
        # Starting disabled would allow simulated observations to rewrite the inherited state.
        write(new / "config/cta_perp.json", cfg)
        started_us = time.time() * 1_000_000
        wrapper(new, "start_cta_perp_signal")

        def healthy():
            try:
                status = read(new / "run/cta_perp_status.json")
                return (bool(pids(new, "cta_perp_signal")) and status["enabled"]
                        and status["updated_ts_us"] > started_us + 3_000_000
                        and status.get("publication_error") is None)
            except (FileNotFoundError, json.JSONDecodeError):
                return False

        wait_for(healthy, "new signal failed publication health check", timeout=60)
        subprocess.run(["sudo", "-n", "cp", str(nginx), str(new / "run/nginx.before.conf")], check=True)
        subprocess.run(["sudo", "-n", "install", "-m", "644", str(new / "run/nginx.after.conf"), str(nginx) + ".rx02.tmp"], check=True)
        subprocess.run(["sudo", "-n", "mv", str(nginx) + ".rx02.tmp", str(nginx)], check=True)
        try:
            subprocess.run(["sudo", "-n", "nginx", "-t"], check=True)
        except subprocess.CalledProcessError:
            subprocess.run(["sudo", "-n", "cp", str(new / "run/nginx.before.conf"), str(nginx)], check=True)
            raise
        subprocess.run(["sudo", "-n", "systemctl", "reload", "nginx"], check=True)
        write(new / "run/cutover_completed.json", {"time": time.time()})
        print("[DONE] CTA Perp enabled. Check Manager deliveries and actual positions; old shared publisher retained.")
    except BaseException:
        write(new / "config/cta_perp.json", {**cfg, "enabled": False})
        wrapper(new, "stop_cta_perp_signal")
        print("[STOPPED] new publication disabled; some Manager targets may already have executed. Do NOT restart old signal blindly.")
        raise


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as error:
        raise SystemExit(f"[BLOCKED] {error}") from None
