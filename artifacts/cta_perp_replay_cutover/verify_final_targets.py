"""Verify captured publication evidence without contacting live services."""
import collections
import importlib.util
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("cutover", HERE / "manager_cutover.py")
cutover = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cutover)
before = json.loads((HERE / "before_execution.json").read_text())
binding_before = json.loads((HERE / "manager_before_prepare.json").read_text())
after = json.loads((HERE / "verified_targets_snapshot.json").read_text())
plan = json.loads((HERE / "manager_binding_plan.json").read_text())
cutover.scope_check(after["catalog"], plan["bindings_after_final_targets_verified"])
result = {"captured_ts_us": after["captured_ts_us"], "environments": {}, "execution": {}}

for suffix, algorithm, source, prefix, symbol_count, pair_count in (
    ("02", "v1", "binance_exec_trade05", "lgq_cta_", 47, 192),
    ("03", "v0", "binance_exec_trade07", "binance_cta_perp_rx03_", 14, 34),
):
    env = after["environments"]["binance-cta-perp-rx" + suffix]
    status = env["run/cta_perp_status.json"]
    progress = env["data/cta_perp/factor_history/recovery.json"]
    completed = env["run/cta_perp_recovery_completed.json"]
    table = env["config/cta_perp_symbol_factors.json"]
    assert "run/cta_perp_recovery_request.json" not in env
    assert progress["caught_up"] and completed["generation"] == progress["generation"]
    captured = env["captured_ts_us"]
    assert abs(captured / 1000 - progress["updated_ms"]) < 10000
    assert abs(captured - status["updated_ts_us"]) < 10_000_000
    assert status["enabled"] and not status["recovering"] and status["journal_healthy"]
    assert not any(status.get(k) for k in ("recovery_error", "configuration_error", "publication_error"))
    assert status["position_algorithm"] == algorithm
    assert env["config/cta_perp.json"] == json.loads((HERE / ("rx" + suffix) / "cta_perp.json").read_text())
    assert table == json.loads((HERE / ("rx" + suffix) / "cta_perp_symbol_factors.json").read_text())
    assert len(table) == symbol_count and sum(map(len, table.values())) == pair_count
    checked = clear = nonzero = 0
    for strategy, symbols in status["strategies"].items():
        targets = after["runtime_targets"][source][prefix + strategy]["targets"]
        assert set(targets) == set(symbols)
        for symbol, observed in symbols.items():
            assert observed["score_ready"] and observed["signal"]["filter_ready"]
            assert 0 <= captured / 1000 - observed["model_ts_ms"] < 130000
            target = targets[symbol]["qty"]
            expected = observed["position_qty"]
            if expected == 0:
                assert target == "clear", (suffix, strategy, symbol, target)
                clear += 1
            else:
                assert cutover.same_quantity(target, expected), (suffix, strategy, symbol, target, expected)
                nonzero += 1
            checked += 1
    assert checked == (symbol_count if algorithm == "v1" else pair_count)
    result["environments"]["rx" + suffix] = {
        "algorithm": algorithm, "symbols": symbol_count, "factor_pairs": pair_count,
        "recovery_completed": True, "generation": completed["generation"],
        "runtime_target_count": checked, "clear_count": clear, "nonzero_count": nonzero,
    }

old = [b for b in plan["bindings_after_final_targets_verified"] if b["binding_name"] != cutover.MEAN]
old_clear_count = 0
for binding in old:
    targets = after["runtime_targets"][cutover.SOURCE][binding["binding_name"]]["targets"]
    assert binding["shares"] == 0 and targets
    assert all(v["qty"] == "clear" and v["signal"] == 0 for v in targets.values())
    old_clear_count += len(targets)
result["retired"] = {"strategies": len(old), "explicit_clears": old_clear_count, "shares": 0}

rx01 = "binance-cta-spread-rx01"
rx01_env = after["environments"][rx01]
assert all(rx01_env["configuration_hashes"].get(k) == v for k, v in before["environments"][rx01]["hashes"].items())
assert rx01_env["run/cta_spread_status.json"]["publication_error"] is None
protected = [p for p in before["protected"] if p["cwd"] == "/home/ubuntu/" + rx01 or "/binance_exec_trade" in p["cwd"]]
now = {p["pid"]: p for p in after["processes"]}
for component in ("exec05_leverage_cap", "exec05_engine_match"):
    replacement = HERE / component / "installed.json"
    if not replacement.exists():
        continue
    installed = json.loads(replacement.read_text())
    protected = [p for p in protected if p["pid"] != installed["old_pid"]]
    assert installed["old_pid"] not in now
    assert now[installed["new_pid"]]["cwd"] == "/home/ubuntu/binance_exec_trade05"
    assert now[installed["new_pid"]]["cpus"] == installed["affinity"]
    result.setdefault("authorized_exec05_restarts", {})[component] = {
        "old_pid": installed["old_pid"], "new_pid": installed["new_pid"]}
assert all(all(now.get(p["pid"], {}).get(k) == v for k, v in p.items()) for p in protected)
for collection, key, excluded in (("bindings", "source_id", cutover.SOURCE), ("virtuals", "virtual_id", cutover.VIRTUAL)):
    assert [r for r in binding_before[collection] if r[key] != excluded] == [r for r in after["catalog"][collection] if r[key] != excluded]
result["protected"] = {"rx01_configuration_unchanged": True, "processes_unchanged": len(protected), "rx01_rx03_bindings_unchanged": True}

for source, snapshot in after["exec"].items():
    entry = snapshot["entry"]
    assert entry["position_ready"]
    rows = entry["rows"]
    result["execution"][source] = {
        "position_ready": True,
        "completion_reasons": dict(collections.Counter(r["completion_reason"] for r in rows)),
        "unfinished": [{k: r[k] for k in ("strategy_name", "symbol", "target_qty", "current_qty", "active_batches", "delta_usdt")} for r in rows if not r["execution_complete"]],
        "flat_target_residuals": [{k: r[k] for k in ("strategy_name", "symbol", "current_qty", "delta_usdt", "completion_reason")} for r in rows if r["target_qty"] == 0 and abs(r["current_qty"]) > 1e-9],
    }
(HERE / "final_target_verification.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result, indent=2))
