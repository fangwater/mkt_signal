"""RX01 removed-coin cleanup through its existing Manager/Exec order ledger."""
import argparse
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument("phase", choices=["plan", "install", "verify"])
parser.add_argument("--execute", action="store_true")
args = parser.parse_args()
root = Path.home() / "binance-cta-spread-rx01"
exe = Path.home() / "binance_exec_trade06"
stage = root / "run/factor_update_20261026/coin_retirement"
prefix = "binance_cta_spread_rx01_"
sys.path.insert(0, str(root / "run/reenable_20261019"))
import ops_helpers as ops


def read(path):
    return json.loads(path.read_text())


def save(name, value):
    stage.mkdir(exist_ok=True, mode=0o700)
    path = stage / name
    temp = path.with_suffix(path.suffix + ".next")
    with temp.open("w") as stream:
        os.chmod(temp, 0o600)
        json.dump(value, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def snapshot():
    with urllib.request.urlopen("http://127.0.0.1:10046/snapshot", timeout=15) as response:
        data = json.load(response)
    state = next(row["entry"] for row in data["entries"] if row["channel"] == "exec_pre_trade_state")
    assert state["position_ready"] and time.time() * 1000 - state["ts_ms"] < 15_000
    return state


def unchanged_processes():
    return {"signal_aux": [row for row in ops.live_pids(root) if row["comm"] != "cta_spread_sign"],
            "exec": ops.live_pids(exe)}


table_path = root / "config/cta_spread_symbol_factors.json"
table = read(table_path)
assert len(table) == 17 and sum(map(len, table.values())) == 55
coins = {symbol.removesuffix("USDT") for symbol in table}

if args.phase == "plan":
    assert not args.execute, "plan is read-only"
    positions = read(root / "run/cta_spread_position_flags.json")
    state = snapshot()
    with ops.session() as api:
        virtual = next(row for row in api("/manager/api/catalog/virtual-accounts") if row["virtual_id"] == "virtual03")
        assert [(row["source_id"], row["multiplier"]) for row in virtual["followers"]] == [(exe.name, 1.0)]
        account = api("/manager/api/catalog/accounts/" + exe.name)
        strategies = [row for row in api("/manager/api/catalog/position-strategies") if row["strategy_name"].startswith(prefix)]
    assert not account["pending_publishes"]
    old_symbols = {symbol for rows in positions.values() for symbol in rows}
    retired = sorted(old_symbols - table.keys())
    nonzero_other = [row for row in state["rows"] if row["strategy_name"] != "SYSTEM_POSITION_CLOSE"
                     and not row["strategy_name"].startswith(prefix) and abs(row["current_qty"]) > 1e-10]
    assert not nonzero_other, "unrelated strategies own account positions"
    plan = {"at_ms": time.time_ns() // 1_000_000, "retained_symbols": sorted(table),
            "retired_signal_symbols": retired,
            "retired_signal_bindings": sum(symbol not in table for rows in positions.values() for symbol in rows),
            "processes": unchanged_processes(), "table_sha256": sha(table_path),
            "config_sha256": sha(root / "config/cta_spread.json"), "env_sha256": sha(root / "env.sh"),
            "old_binary_sha256": sha(root / "cta_spread_signal")}
    save("plan.json", plan)
    save("before_positions.json", positions)
    save("before_exec.json", state)
    save("before_strategies.json", strategies)
    save("before_account.json", account)
    print(json.dumps({k: v for k, v in plan.items() if not k.endswith("sha256") and k != "processes"}))

elif args.phase == "install":
    assert args.execute and not (stage / "installed.json").exists()
    plan = read(stage / "plan.json")
    manifest = read(stage / "manifest.json")
    candidate = stage / "cta_spread_signal.upload"
    assert sha(candidate) == manifest["sha256"]
    assert sha(root / "cta_spread_signal") == plan["old_binary_sha256"]
    assert sha(table_path) == plan["table_sha256"]
    assert sha(root / "config/cta_spread.json") == plan["config_sha256"]
    assert sha(root / "env.sh") == plan["env_sha256"]
    assert unchanged_processes() == plan["processes"]
    shutil.copy2(root / "cta_spread_signal", stage / "cta_spread_signal.before")
    save("positions_before_stop.json", read(root / "run/cta_spread_position_flags.json"))
    subprocess.run([str(root / "scripts/stop_cta_spread_signal.sh")], cwd=root, check=True)
    assert not any(row["comm"] == "cta_spread_sign" for row in ops.live_pids(root))
    save("positions_at_restart.json", read(root / "run/cta_spread_position_flags.json"))
    os.chmod(candidate, 0o755)
    os.replace(candidate, root / "cta_spread_signal")
    assert sha(root / "cta_spread_signal") == manifest["sha256"]
    subprocess.run([str(root / "scripts/start_cta_spread_signal.sh")], cwd=root, check=True)
    assert unchanged_processes() == plan["processes"]
    # The independent inventory sleeve owns only historical account dust.
    # Preserve every target in the new coin universe; explicitly clear the rest.
    with ops.session() as api:
        inventory = next(row for row in api("/manager/api/catalog/position-strategies")
                         if row["strategy_name"] == prefix + "inventory")
        save("inventory_before.json", inventory)
        targets = {symbol: value if symbol.removesuffix("USDT") in coins else {"qty": "clear", "signal": 1}
                   for symbol, value in inventory["targets"].items()}
        changed = api("/manager/api/catalog/position-strategies", {
            "strategy_name": inventory["strategy_name"], "targets": targets,
            "symbol_order_strategy_overrides": inventory["symbol_order_strategy_overrides"]})
        save("inventory_after.json", changed)
    receipt = {"at_ms": time.time_ns() // 1_000_000, "sha256": manifest["sha256"],
               "signal_process": [row for row in ops.live_pids(root) if row["comm"] == "cta_spread_sign"],
               "other_processes_unchanged": unchanged_processes() == plan["processes"]}
    assert len(receipt["signal_process"]) == 1
    save("installed.json", receipt)
    print(json.dumps(receipt))

else:
    state = snapshot()
    positions = read(root / "run/cta_spread_position_flags.json")
    status = read(root / "run/cta_spread_status.json")
    with ops.session() as api:
        strategies = {row["strategy_name"]: row for row in api("/manager/api/catalog/position-strategies") if row["strategy_name"].startswith(prefix)}
        account = api("/manager/api/catalog/accounts/" + exe.name)
    errors = []
    cleared_targets = 0
    for factor, rows in positions.items():
        for symbol, position in rows.items():
            if symbol in table:
                continue
            if any(position.values()):
                errors.append([factor, symbol, "persisted state nonzero"])
            for market in [symbol, symbol.removesuffix("USDT")]:
                target = strategies[prefix + factor]["targets"][market]
                if target != {"qty": "clear", "signal": 1}:
                    errors.append([factor, market, "missing clear target"])
                cleared_targets += 1
    by_market = {}
    pending = []
    for row in state["rows"]:
        coin = row["symbol"].removesuffix("USDT")
        if coin in coins:
            continue
        by_market[row["symbol"]] = {"coin": coin, "symbol": row["symbol"],
                                   "qty": row["account_position_qty"], "usdt": row["account_position_usdt"]}
        if row["strategy_name"].startswith(prefix) and (row["active_batches"] or row["pending_qty"] or row["live_order_qty"]):
            pending.append({key: row[key] for key in ["strategy_name", "symbol", "current_qty", "target_qty", "pending_qty", "live_order_qty", "active_batches"]})
    nonzero = [row for row in by_market.values() if abs(row["qty"]) > 1e-10]
    plan = read(stage / "plan.json")
    result = {"at_ms": time.time_ns() // 1_000_000, "signal_pid": [row for row in ops.live_pids(root) if row["comm"] == "cta_spread_sign"],
              "retired_clear_targets": cleared_targets, "errors": errors,
              "publication_error": status["publication_error"], "journal_healthy": status["journal_healthy"],
              "pending_publishes": account["pending_publishes"], "retired_pending": pending,
              "nonzero_retired_markets": sorted(nonzero, key=lambda row: abs(row["usdt"]), reverse=True),
              "retired_gross_usdt": sum(abs(row["usdt"]) for row in nonzero),
              "other_processes_unchanged": unchanged_processes() == plan["processes"],
              "config_unchanged": sha(root / "config/cta_spread.json") == plan["config_sha256"],
              "table_unchanged": sha(table_path) == plan["table_sha256"],
              "env_unchanged": sha(root / "env.sh") == plan["env_sha256"]}
    save("verification.json", result)
    save("latest_exec.json", state)
    print(json.dumps(result))
