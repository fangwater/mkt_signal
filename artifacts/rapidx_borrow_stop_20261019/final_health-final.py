"""Read-only Manager/source checks after RX01 resumes."""
import importlib.util
import json
from pathlib import Path
import time
import urllib.request

home = Path.home()
root = home / "binance-cta-spread-rx01"
stage = home / "binance_exec_trade06/run/borrow_stop_20261019T130500Z"
spec = importlib.util.spec_from_file_location("ops", root / "run/reenable_20261019/ops_helpers.py")
ops = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ops)
with ops.session() as api:
    health = api("/manager/api/health")
    account = api("/manager/api/catalog/accounts/binance_exec_trade06")
    assert health["status"] == "ok" and not health["last_refresh_error"]
    assert not account["pending_publishes"]
counts = ops.sql("SELECT (SELECT count(*) FROM cta_position_snapshot_entries WHERE source_id='binance_exec_trade06' AND snapshot_ts_us=1792404818623482), (SELECT count(*) FROM cta_strategy_position_snapshot_entries WHERE source_id='binance_exec_trade06' AND snapshot_ts_us=1792404818623482);")
assert counts == "20|20"
status = json.loads((root / "run/cta_spread_status.json").read_text())
assert status["enabled"] and status["publication_error"] is None and status["journal_healthy"]
warmup = json.loads((root / "data/cta_spread/factor_history/warmup.json").read_text())
assert warmup["caught_up"] and time.time() * 1000 - warmup["updated_ms"] < 10000
before = json.loads((stage / "before.json").read_text())
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
with opener.open("http://127.0.0.1:19174/api/config", timeout=10) as response:
    config = json.load(response)
assert config["config"] == before["config"]["config"]
observation = json.loads((stage / "observation.json").read_text())
assert observation["publication_enabled"] and observation["position_ready"]
assert observation["rate_limit_errors"] == 0
assert all(row["execution_complete"] and row["active_batches"] == 0
           and row["pending_qty"] == 0 and row["live_order_qty"] == 0
           for row in observation["borrow_stopped_rows"])
result = dict(ts_us=time.time_ns() // 1000, manager_health=health,
    pending_publishes=account["pending_publishes"], enabled=True, config_preserved=True,
    publication_error=None, journal_healthy=True, publisher_caught_up=True,
    snapshot_ts_us=1792404818623482, snapshot_counts=counts,
    borrow_stopped_targets=len(observation["borrow_stopped_rows"]),
    borrow_pool_errors=observation["borrow_pool_errors"], rate_limit_errors=0)
ops.save(stage / "final_health.json", result)
print(json.dumps(result))
