"""Read-only final removed-coin, preserved-target and process verification."""
from pathlib import Path
import hashlib
import json
import math
import sys
import time
import urllib.request
import redis

root = Path.home() / "binance-cta-spread-rx01"
exe = Path.home() / "binance_exec_trade06"
stage = root / "run/factor_update_20261026/coin_retirement"
sys.path.insert(0, str(root / "run/reenable_20261019"))
import ops_helpers as ops

read = lambda p: json.loads(p.read_text())
plan = read(stage / "plan.json")
before = read(stage / "positions_at_restart.json")
positions = read(root / "run/cta_spread_position_flags.json")
table = read(root / "config/cta_spread_symbol_factors.json")
coins = {s.removesuffix("USDT") for s in table}
with urllib.request.urlopen("http://127.0.0.1:10046/snapshot") as f:
    snapshot = json.load(f)
state = next(x["entry"] for x in snapshot["entries"] if x["channel"] == "exec_pre_trade_state")
assert state["position_ready"] and time.time() * 1000 - state["ts_ms"] < 15000
with ops.session() as api:
    catalog = api("/manager/api/catalog/position-strategies")
    inventory = next(x for x in catalog if x["strategy_name"] == "binance_cta_spread_rx01_inventory")
    grid = next(x for x in api("/manager/api/catalog/order-strategies") if x["strategy_name"] == "lgq_grid_spread")
assert grid["order_parameters"] == read(root / "run/factor_update_20261026/before/grid_template.json")["order_parameters"]
values = ops.env_for(exe)
client = redis.Redis(host=values.get("REDIS_HOST", "127.0.0.1"), port=int(values.get("REDIS_PORT", 6379)),
                     db=int(values.get("REDIS_DB", 0)), username=values.get("REDIS_USERNAME"),
                     password=values.get("REDIS_PASSWORD"), decode_responses=True)
caches = {venue: json.loads(client.get("binance_exec_trade06:" + venue + ":batch_exec:binance_cta_spread_rx01_inventory"))
          for venue in ["binance-margin", "binance-futures"]}
checks = []
for symbol, target in inventory["targets"].items():
    if symbol.removesuffix("USDT") in coins:
        continue
    assert target == {"qty": "clear", "signal": 1}
    venue = "binance-futures" if symbol.endswith("USDT") else "binance-margin"
    assert caches[venue]["targets"][symbol]["qty"] == "clear"
    assert caches[venue]["symbol_overrides"][symbol]["algorithm"] == "ctaker"
    checks.append(symbol)
kept = 0
max_preserved_quantity_rounding = 0.0
for factor, rows in before.items():
    for symbol, position in rows.items():
        if symbol in table and factor not in table[symbol]:
            current = positions[factor][symbol]
            assert current["flag"] == position["flag"]
            for field in ["qty", "cash_qty", "perpetual_qty", "entry_notional"]:
                assert math.isclose(current[field], position[field], rel_tol=1e-13, abs_tol=1e-10), (factor, symbol, field)
                max_preserved_quantity_rounding = max(max_preserved_quantity_rounding, abs(current[field] - position[field]))
            kept += 1
retired = [r for r in state["rows"] if r["symbol"].removesuffix("USDT") not in coins]
assert not any(r["pending_qty"] or r["live_order_qty"] or r["active_batches"] for r in retired)
assert not any(abs(r["current_qty"]) > 1e-7 for r in retired
               if r["strategy_name"] not in {"binance_cta_spread_rx01_inventory", "SYSTEM_POSITION_CLOSE"})
system_residuals = [{k: r[k] for k in ["symbol", "current_qty", "current_usdt", "completion_reason"]}
                    for r in retired if r["strategy_name"] == "SYSTEM_POSITION_CLOSE" and abs(r["current_qty"]) > 1e-10]
assert all(abs(r["current_usdt"]) < 0.01 for r in system_residuals)
remaining = [{k: r[k] for k in ["symbol", "current_qty", "current_usdt", "completion_reason"]}
             for r in retired if r["strategy_name"] == "binance_cta_spread_rx01_inventory" and abs(r["current_qty"]) > 1e-10]
assert all(r["completion_reason"] == "exchange_minimum" for r in remaining)
account_markets = {r["symbol"]: r for r in retired}
processes = ops.live_pids(exe)
pt = [r for r in processes if r["comm"] == "exec-pre-trade"]
assert len(pt) == 1
assert [r for r in processes if r["comm"] != "exec-pre-trade"] == [r for r in plan["processes"]["exec"] if r["comm"] != "exec-pre-trade"]
assert [r for r in ops.live_pids(root) if r["comm"] != "cta_spread_sign"] == plan["processes"]["signal_aux"]
status = read(root / "run/cta_spread_status.json")
assert status["publication_error"] is None and status["journal_healthy"]
signal_pid = next(r["pid"] for r in ops.live_pids(root) if r["comm"] == "cta_spread_sign")


def mask(pid):
    return next(line.split(":")[1].strip() for line in Path(f"/proc/{pid}/status").read_text().splitlines()
                if line.startswith("Cpus_allowed_list:"))


receipt = {
    "verified_at_ms": time.time_ns() // 1_000_000,
    "retained_symbols": sorted(table), "retained_symbol_count": len(table),
    "retained_factor_combinations": sum(map(len, table.values())),
    "retained_coin_legacy_bindings_preserved": kept,
    "retained_legacy_max_quantity_rounding": max_preserved_quantity_rounding,
    "retired_signal_combinations_cleared": plan["retired_signal_bindings"],
    "retired_signal_market_clear_targets": plan["retired_signal_bindings"] * 2,
    "retired_inventory_clear_targets": len(checks),
    "inventory_manager_redis_clear_targets_match": True,
    "retired_factor_positions_zero": True, "retired_pending_or_live_orders": 0,
    "retired_gross_usdt": sum(abs(r["account_position_usdt"]) for r in account_markets.values()),
    "max_retired_market_usdt": max(abs(r["account_position_usdt"]) for r in account_markets.values()),
    "system_rounding_and_fee_residuals": system_residuals,
    "remaining_markets": remaining, "signal_pid": signal_pid, "exec_pre_trade_pid": pt[0]["pid"],
    "other_exec_processes_unchanged": True, "signal_aux_processes_unchanged": True,
    "grid_template_unchanged": True,
    "main_config_unchanged": hashlib.sha256((root / "config/cta_spread.json").read_bytes()).hexdigest() == plan["config_sha256"],
    "factor_table_unchanged": hashlib.sha256((root / "config/cta_spread_symbol_factors.json").read_bytes()).hexdigest() == plan["table_sha256"],
    "env_unchanged": hashlib.sha256((root / "env.sh").read_bytes()).hexdigest() == plan["env_sha256"],
    "signal_binary_sha256": hashlib.sha256((root / "cta_spread_signal").read_bytes()).hexdigest(),
    "signal_cpu_mask": mask(signal_pid), "exec_cpu_mask": mask(pt[0]["pid"]),
}
(stage / "final_result.json").write_text(json.dumps(receipt, indent=2) + "\n")
print(json.dumps(receipt, indent=2))
