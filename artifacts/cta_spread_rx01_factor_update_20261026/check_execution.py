"""Read-only RX01 target/execution audit; optional factual RapidX account query."""
import argparse
from collections import Counter, defaultdict
import json
import math
import os
from pathlib import Path
import sys
import time
import tomllib
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument("--exchange", action="store_true")
parser.add_argument("--label", default="execution_audit")
args = parser.parse_args()
root = Path.home() / "binance-cta-spread-rx01"
stage = root / "run/factor_update_20261026"
sys.path.insert(0, str(root / "run/reenable_20261019"))
import ops_helpers as ops
import redis


def read(path):
    return json.loads(path.read_text())


def equal(a, b):
    return math.isclose(a, b, rel_tol=1e-10, abs_tol=1e-10)


def target_quantity(value):
    return 0.0 if value == "clear" else value


def get_snapshot():
    with urllib.request.urlopen("http://127.0.0.1:10046/snapshot", timeout=10) as response:
        data = json.load(response)
    return next(item["entry"] for item in data["entries"] if item["channel"] == "exec_pre_trade_state")


values = ops.env_for(Path.home() / "binance_exec_trade06")
client = redis.Redis(host=values.get("REDIS_HOST", "127.0.0.1"), port=int(values.get("REDIS_PORT", 6379)),
                     db=int(values.get("REDIS_DB", 0)), username=values.get("REDIS_USERNAME"),
                     password=values.get("REDIS_PASSWORD"), decode_responses=True, socket_timeout=5)
prefix = "binance_cta_spread_rx01_"
positions = read(root / "run/cta_spread_position_flags.json")
candidate = read(stage / "candidate/cta_spread_symbol_factors.json")
before = read(stage / ("before/positions_at_switch.json" if (stage / "activated.json").exists() else "before/positions.json"))
with ops.session() as api:
    strategies = {row["strategy_name"]: row for row in api("/manager/api/catalog/position-strategies")}
    account = api("/manager/api/catalog/accounts/binance_exec_trade06")

keys = sorted(client.scan_iter(match="binance_exec_trade06:*:batch_exec:" + prefix + "*"))
configs = {key: json.loads(value) for key, value in zip(keys, client.mget(keys)) if value is not None}
redis_targets = {}
for key, config in configs.items():
    strategy = key.split(":batch_exec:", 1)[1]
    for symbol, target in config.get("targets", {}).items():
        redis_targets[strategy, symbol] = target_quantity(target["qty"])

state = get_snapshot()
assert state["position_ready"] and time.time() * 1000 - state["ts_ms"] < 15_000, "stale/unready Exec state"
execution = {(row["strategy_name"], row["symbol"]): row for row in state["rows"]}
manager_mismatches, redis_mismatches, exec_mismatches = [], [], []
changed_targets = []
desired = {}
for factor, symbols in positions.items():
    strategy = prefix + factor
    for symbol, position in symbols.items():
        old = before.get(factor, {}).get(symbol, {})
        for market, field in [(symbol, "perpetual_qty"), (symbol.removesuffix("USDT"), "cash_qty")]:
            expected = position[field]
            desired[strategy, market] = expected
            manager = target_quantity(strategies.get(strategy, {}).get("targets", {}).get(market, {}).get("qty", 0.0))
            cached = redis_targets.get((strategy, market), 0.0)
            row = execution.get((strategy, market), {})
            target = row.get("target_qty", 0.0)
            if not equal(manager, expected):
                manager_mismatches.append([factor, market, expected, manager])
            if not equal(cached, expected):
                redis_mismatches.append([factor, market, expected, cached])
            if not equal(target, expected):
                exec_mismatches.append([factor, market, expected, target])
            if not equal(old.get(field, 0.0), expected):
                changed_targets.append({"factor": factor, "market": market, "previous_target": old.get(field, 0.0),
                                        "target": expected, "current_qty": row.get("current_qty"),
                                        "delta_usdt": row.get("delta_usdt"), "execution_complete": row.get("execution_complete"),
                                        "completion_reason": row.get("completion_reason"),
                                        "source_updated_at_ms": row.get("source_updated_at_ms")})

rows = [row for row in state["rows"] if row["strategy_name"].startswith(prefix)]
requested_keys = {(prefix + factor, market) for symbol, factors in candidate.items()
                  for factor in factors for market in [symbol, symbol.removesuffix("USDT")]}
outside_tolerance = []
for row in rows:
    venue = "binance-futures" if row["symbol"].endswith(("USDT", "USDC")) else "binance-margin"
    config = configs.get("binance_exec_trade06:" + venue + ":batch_exec:" + row["strategy_name"], {})
    override = config.get("symbol_overrides", {}).get(row["symbol"], {})
    tolerance = override.get("target_tolerance_usdt", config.get("target_tolerance_usdt", 10.0))
    if config.get("targets", {}).get(row["symbol"], {}).get("qty") == "clear":
        tolerance = 0.0
    if abs(row["delta_usdt"]) > tolerance + 1e-6:
        outside_tolerance.append({**row, "target_tolerance_usdt": tolerance})

result = {"checked_at_ms": time.time_ns() // 1_000_000, "exec_state_ts_ms": state["ts_ms"],
          "source_id": "binance_exec_trade06", "signal_manager_redis_exec_checked_targets": len(desired),
          "manager_mismatches": manager_mismatches, "redis_mismatches": redis_mismatches, "exec_target_mismatches": exec_mismatches,
          "changed_targets": changed_targets, "changed_target_count": len(changed_targets),
          "requested_combinations": sum(map(len, candidate.values())), "exec_rows": len(rows),
          "completion_reasons": dict(Counter(row["completion_reason"] for row in rows)),
          "incomplete": [row for row in rows if not row["execution_complete"]],
          "outside_tolerance": outside_tolerance,
          "live_orders_or_pending": [row for row in rows if abs(row["live_order_qty"]) > 1e-10 or abs(row["pending_qty"]) > 1e-10 or row["active_batches"]],
          "max_abs_delta_usdt": max((abs(row["delta_usdt"]) for row in rows), default=0),
          "manager_pending_publishes": account.get("pending_publishes"),
          "exec_processes_unchanged": ops.live_pids(Path.home() / "binance_exec_trade06") == read(stage / "before/processes.json")["exec"]}
result["requested_market_targets"] = len(requested_keys)
result["requested_outside_tolerance"] = [row for row in outside_tolerance if (row["strategy_name"], row["symbol"]) in requested_keys]
result["requested_live_orders_or_pending"] = [row for row in result["live_orders_or_pending"] if (row["strategy_name"], row["symbol"]) in requested_keys]
result["requested_directional_positions"] = [
    {"factor": factor, "symbol": symbol, **positions.get(factor, {}).get(symbol, {})}
    for symbol, factors in candidate.items() for factor in factors
    if positions.get(factor, {}).get(symbol, {}).get("flag", 0) != 0
]

if args.exchange:
    sys.path.insert(0, str(root / "scripts"))
    if not (root / "scripts/neutralize_rapidx_binance.py").exists():
        sys.path.insert(0, str(Path.home() / "binance-cta-rx01/scripts"))
    from neutralize_rapidx_binance import Client
    os.environ.update(values)
    local_ip = tomllib.loads((Path.home() / "binance_exec_trade06/trade_engine.toml").read_text())["local_ips"][0]
    api = Client(local_ip, execute=False)
    cash, perp, marks = api.snapshot()
    factual = {coin: float(qty) for coin, qty in cash.items() if coin != "USDT"}
    factual.update({coin + "USDT": float(qty) for coin, qty in perp.items()})
    # Take a fresh state after the signed account query to reduce poll skew.
    fresh = get_snapshot()
    account_qty = {}
    for row in fresh["rows"]:
        account_qty[row["symbol"]] = row["account_position_qty"]
    differences = []
    for symbol in sorted(set(factual) | set(account_qty)):
        expected = factual.get(symbol, 0.0)
        observed = account_qty.get(symbol, 0.0)
        if not math.isclose(expected, observed, rel_tol=1e-7, abs_tol=1e-7):
            coin = symbol.removesuffix("USDT")
            difference_usdt = abs(expected - observed) * float(marks.get(coin, 0))
            differences.append({"symbol": symbol, "exchange_qty": expected, "exec_account_qty": observed,
                                "difference_usdt": difference_usdt})
    result["exchange"] = {"read_only": True, "observed_markets": len(factual),
                          "account_state_differences": differences, "positions": factual,
                          "exec_state_ts_ms": fresh["ts_ms"]}

path = stage / (args.label + ".json")
path.write_text(json.dumps(result, indent=2) + "\n")
os.chmod(path, 0o600)
summary = {key: value for key, value in result.items() if key not in {"changed_targets", "exchange", "incomplete", "outside_tolerance", "live_orders_or_pending", "requested_outside_tolerance", "requested_live_orders_or_pending"}}
for key in ["incomplete", "outside_tolerance", "live_orders_or_pending"]:
    summary[key + "_count"] = len(result[key])
summary["requested_outside_tolerance_count"] = len(result["requested_outside_tolerance"])
summary["requested_live_orders_or_pending_count"] = len(result["requested_live_orders_or_pending"])
summary["requested_outside_tolerance"] = [
    {key: row.get(key) for key in ["strategy_name", "symbol", "target_qty", "current_qty", "delta_usdt", "completion_reason", "target_tolerance_usdt"]}
    for row in result["requested_outside_tolerance"]
]
summary["outside_tolerance_examples"] = [
    {key: row.get(key) for key in ["strategy_name", "symbol", "delta_usdt", "completion_reason", "target_tolerance_usdt"]}
    for row in outside_tolerance[:8]
]
summary["changed_target_examples"] = changed_targets[:8]
if "exchange" in result:
    summary["exchange"] = {key: value for key, value in result["exchange"].items() if key != "positions"}
print(json.dumps(summary))
