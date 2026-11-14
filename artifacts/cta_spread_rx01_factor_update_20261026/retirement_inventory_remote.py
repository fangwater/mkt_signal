"""Consolidate only retired coins using Exec internal crosses, then exit sequentially."""
import argparse
import copy
import json
import math
import os
from pathlib import Path
import sys
import time
import tomllib
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument("phase", choices=["absorb", "refresh", "status", "exit"])
parser.add_argument("--execute", action="store_true")
args = parser.parse_args()
root = Path.home() / "binance-cta-spread-rx01"
exe = Path.home() / "binance_exec_trade06"
stage = root / "run/factor_update_20261026/coin_retirement"
sys.path.insert(0, str(root / "run/reenable_20261019"))
import ops_helpers as ops
sys.path.insert(0, str(Path.home() / "binance-cta-rx01/scripts"))
from neutralize_rapidx_binance import Client

prefix = "binance_cta_spread_rx01_"
inventory_name = prefix + "inventory"
template_name = prefix + "coin_exit"
coins = {s.removesuffix("USDT") for s in json.loads((root / "config/cta_spread_symbol_factors.json").read_text())}
os.environ.update(ops.env_for(exe))
local_ip = tomllib.loads((exe / "trade_engine.toml").read_text())["local_ips"][0]
exchange = Client(local_ip, execute=False)


def save(name, value):
    path = stage / name
    temp = path.with_suffix(path.suffix + ".next")
    with temp.open("w") as f:
        os.chmod(temp, 0o600)
        json.dump(value, f, indent=2)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)


def state():
    with urllib.request.urlopen("http://127.0.0.1:10046/snapshot", timeout=10) as response:
        result = json.load(response)
    return next(x["entry"] for x in result["entries"] if x["channel"] == "exec_pre_trade_state")


def save_inventory(api, current, targets, overrides):
    return api("/manager/api/catalog/position-strategies", {
        "strategy_name": inventory_name, "targets": targets,
        "symbol_order_strategy_overrides": overrides})


if args.phase in {"absorb", "refresh"}:
    assert args.execute
    if args.phase == "absorb":
        assert not any(p["comm"] == "exec-pre-trade" for p in ops.live_pids(exe))
        assert not (stage / "inventory_absorbed.json").exists()
    else:
        fresh = state()
        assert fresh["position_ready"] and time.time() * 1000 - fresh["ts_ms"] < 15000
        assert not any((r["pending_qty"] or r["live_order_qty"]) for r in fresh["rows"]
                       if r["symbol"].removesuffix("USDT") not in coins)
    cash, perp, marks = exchange.snapshot()
    factual = {s: float(q) for s, q in cash.items() if s not in coins and s != "USDT"}
    factual.update({s + "USDT": float(q) for s, q in perp.items() if s not in coins})
    with ops.session() as api:
        current = next(x for x in api("/manager/api/catalog/position-strategies") if x["strategy_name"] == inventory_name)
        save("inventory_before_" + args.phase + ".json", current)
        reference = next(x for x in api("/manager/api/catalog/order-strategies") if x["strategy_name"] == "binance_cta_perp_rx03_ctaker")
        parameters = copy.deepcopy(reference["order_parameters"])
        assert parameters["algorithm"] == "ctaker"
        parameters["orders_per_batch"] = 1
        parameters["max_batch"] = 1
        parameters["batch_interval_ms"] = 2000
        parameters["target_tolerance_usdt"] = 0.0
        if args.phase == "absorb":
            created = api("/manager/api/catalog/order-strategies", {"strategy_name": template_name, "order_parameters": parameters})
            save("cleanup_order_template.json", created)
        targets = {s: v for s, v in current["targets"].items() if s.removesuffix("USDT") in coins}
        targets.update({s: {"qty": q, "signal": 0} for s, q in factual.items()})
        overrides = {s: v for s, v in current["symbol_order_strategy_overrides"].items() if s.removesuffix("USDT") in coins}
        overrides.update({s: template_name for s in factual})
        result = save_inventory(api, current, targets, overrides)
    receipt = {"at_ms": time.time_ns() // 1_000_000, "factual": factual, "inventory": result,
               "retained_targets": {s: v for s, v in targets.items() if s.removesuffix("USDT") in coins}}
    save("inventory_absorbed.json" if args.phase == "absorb" else "inventory_refreshed.json", receipt)
    print(json.dumps({"prepared_at_ms": receipt["at_ms"], "retired_market_count": len(factual),
                      "exchange_orders_submitted": 0, "method": "Existing Exec internal crosses to inventory, no ledger reset"}), flush=True)

elif args.phase == "status":
    cash, perp, marks = exchange.snapshot()
    s = state()
    retired = [r for r in s["rows"] if r["symbol"].removesuffix("USDT") not in coins]
    non_inventory = [r for r in retired if r["strategy_name"] != inventory_name and abs(r["current_usdt"]) > 0.05]
    value = {"at_ms": time.time_ns() // 1_000_000, "exec_ts_ms": s["ts_ms"],
             "other_owner_rows": [{k: r[k] for k in ["strategy_name", "symbol", "current_qty", "current_usdt", "pending_qty", "active_batches"]} for r in non_inventory],
             "factual_retired": [{"coin": c, "cash_qty": float(cash.get(c, 0)), "perp_qty": float(perp.get(c, 0)),
                                   "cash_usdt": float(cash.get(c, 0) * marks[c]), "perp_usdt": float(perp.get(c, 0) * marks[c])}
                                  for c in sorted((cash.keys() | perp.keys()) - coins - {"USDT"})
                                  if cash.get(c, 0) or perp.get(c, 0)]}
    save("inventory_status.json", value)
    print(json.dumps({"at_ms": value["at_ms"], "other_owner_count": len(non_inventory),
                      "other_owner_usdt": sum(abs(r["current_usdt"]) for r in non_inventory),
                      "exchange_retired_gross_usdt": sum(abs(r["cash_usdt"]) + abs(r["perp_usdt"]) for r in value["factual_retired"]),
                      "other_owner_examples": value["other_owner_rows"][:8]}), flush=True)

else:
    assert args.execute
    assert any(p["comm"] == "exec-pre-trade" for p in ops.live_pids(exe))
    s = state()
    assert s["position_ready"] and time.time() * 1000 - s["ts_ms"] < 15_000
    original = json.loads((stage / "inventory_absorbed.json").read_text())
    with ops.session() as api:
        current = next(x for x in api("/manager/api/catalog/position-strategies") if x["strategy_name"] == inventory_name)
        assert {s: v for s, v in current["targets"].items() if s.removesuffix("USDT") in coins} == original["retained_targets"]
        targets = current["targets"]
        retired_coins = sorted({s.removesuffix("USDT") for s in original["factual"]})
        for coin in retired_coins:
            market_symbols = [s for s in targets if s in [coin, coin + "USDT"]]
            if all(targets[s] == {"qty": "clear", "signal": 1} for s in market_symbols):
                continue
            # Do not start a second asset while a previous asset still has live orders.
            for attempt in range(12):
                fresh = state()
                pending = [r for r in fresh["rows"] if r["strategy_name"] == inventory_name
                           and r["symbol"].removesuffix("USDT") not in coins
                           and (r["pending_qty"] or r["live_order_qty"] or r["active_batches"])]
                if not pending:
                    break
                time.sleep(2)
            assert not pending, "previous inventory exit still pending; inspect before continuing"
            for symbol in market_symbols:
                targets[symbol] = {"qty": "clear", "signal": 1}
            save("inventory_exit_intent.json", {"at_ms": time.time_ns() // 1_000_000, "coin": coin, "targets": targets})
            result = save_inventory(api, current, targets, current["symbol_order_strategy_overrides"])
            save("inventory_exit_latest.json", result)
            print(json.dumps({"at_ms": time.time_ns() // 1_000_000, "clear_coin": coin, "market_count": len(market_symbols)}), flush=True)
            if any(abs(r["current_usdt"]) >= 4.9 for r in fresh["rows"]
                   if r["strategy_name"] == inventory_name and r["symbol"] in market_symbols):
                time.sleep(3)
    print("All retired inventory targets request clear; verify actual exchange balances and minimum-order residuals.", flush=True)
