"""Verify the completed Kafka replay and the still-disabled deployment."""
from collections import Counter
import datetime
import hashlib
import json
import math
from pathlib import Path
import socket
import subprocess
import sys
import time
import tomllib

home = Path.home()
root = home / "binance-cta-perp-rx01"
run = root / "run/depth_factors_20261030"
stage = run
manifest = json.loads((stage / "manifest.json").read_text())
probe_source_commit = manifest["source_commit"]
probe_binary_sha256 = manifest["files"]["cta_perp_factor_model_15m_pub"]["sha256"]
boundary_manifest = stage / "boundary_fix_manifest.json"
if boundary_manifest.exists():
    boundary = json.loads(boundary_manifest.read_text())
    manifest["source_commit"] = boundary["source_commit"]
    manifest["files"].update(boundary["files"])
probe = tomllib.loads((stage / "probe.toml").read_text())
names = set(probe["universe"]["factors"])
symbols = set(probe["universe"]["symbols"])
assert len(names) == 23 and symbols == {"BTCUSDT", "ETHUSDT"}
assert (run / "probe_exit_code").read_text().strip() == "0"
log = (run / "probe.log").read_text()
assert "CTA history replay complete; no IPC publishers created" in log
assert "ERROR" not in log and "evaluation failed" not in log

rows = Counter()
finite = {symbol: Counter() for symbol in symbols}
ready = {symbol: Counter() for symbol in symbols}
latest = {}
for path in sorted((run / "probe_history").glob("*.jsonl")):
    for line in path.open():
        row = json.loads(line)
        symbol = row["symbol"]
        assert symbol in symbols and set(row["inputs"]) == names
        assert row["bar_interval"] == "15m"
        assert row["bar_start_ms"] + 900_000 == row["model_ts_ms"]
        assert row["model_ts_ms"] % 900_000 == 0
        assert symbol not in latest or row["model_ts_ms"] > latest[symbol]["model_ts_ms"]
        for name, observation in row["inputs"].items():
            if observation["score"] is not None and math.isfinite(observation["score"]):
                finite[symbol][name] += 1
            if observation["ready"]:
                assert observation["quantile"] is not None
                ready[symbol][name] += 1
        rows[symbol] += 1
        latest[symbol] = row
now_ms = int(time.time() * 1000)
for symbol in symbols:
    assert rows[symbol] >= 180, (symbol, rows[symbol])
    # The two net-flow formulas intentionally emit NaN when bid_net + ask_net
    # is non-positive. Preserve the production 67/96 normalization requirement;
    # do not manufacture readiness when that leaves too few valid samples.
    required = names - {"depth_flow_net", "depth_touch_net"}
    assert set(finite[symbol]) >= required, (symbol, required - set(finite[symbol]))
    assert set(ready[symbol]) >= required, (symbol, required - set(ready[symbol]))
    assert 0 <= now_ms - latest[symbol]["model_ts_ms"] < 1_800_000
    assert latest[symbol]["nq"]["ready"]

for name, value in manifest["files"].items():
    assert hashlib.sha256((root / name).read_bytes()).hexdigest() == value["sha256"]
for name, value in manifest["protected_hashes"].items():
    assert hashlib.sha256((root / name).read_bytes()).hexdigest() == value
config = json.loads((root / "config/cta_perp.json").read_text())
assert config["enabled"] is False and config["factor_rules"] == []
assert json.loads((root / "config/cta_perp_symbol_factors.json").read_text()) == {}
runtime = tomllib.loads((root / "config/cta_perp_factor_model_15m_pub.toml").read_text())
assert runtime["universe"] == {"symbols": [], "factors": []}
guard_history = run / "empty_universe_must_not_create_history"
guard = subprocess.run([str(root / "cta_perp_factor_model_15m_pub"), "--config",
    str(root / "config/cta_perp_factor_model_15m_pub.toml"), "--replay-only",
    "--history-path", str(guard_history)], cwd=root, capture_output=True, text=True)
assert guard.returncode != 0 and "universe.symbols must not be empty" in guard.stderr
assert not guard_history.exists()
before = json.loads((run / "before_processes.json").read_text())
for pid, expected in before.items():
    process = Path("/proc") / pid
    assert str((process / "cwd").resolve(strict=True)) == expected["cwd"]
    assert (process / "stat").read_text().rsplit(") ", 1)[1].split()[19] == expected["start_ticks"]
for port in [10195, 19185]:
    with socket.socket() as connection:
        connection.settimeout(0.2)
        assert connection.connect_ex(("127.0.0.1", port)) != 0
sys.path.insert(0, str(root / "scripts"))
from cta_perp_factor_config import normalize_table
assert len(normalize_table({"BTCUSDT": {name: 1 for name in names}})["BTCUSDT"]) == 23

result = {"verified_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
          "source_commit": manifest["source_commit"], "probe_exit_code": 0,
          "probe_source_commit": probe_source_commit, "probe_binary_sha256": probe_binary_sha256,
          "ipc_publishers_created": False, "rows_by_symbol": dict(rows),
          "finite_scores_by_symbol": {symbol: dict(values) for symbol, values in finite.items()},
          "ready_scores_by_symbol": {symbol: dict(values) for symbol, values in ready.items()},
          "never_ready_by_symbol": {symbol: sorted(names - set(ready[symbol])) for symbol in symbols},
          "latest_samples": latest, "protected_processes_unchanged": len(before),
          "runtime_enabled": False, "runtime_operator_list_empty": True,
          "final_binary_rejects_empty_universe_before_io": True,
          "dashboard_and_config_ports_closed": True}
(run / "verification.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps({key: value for key, value in result.items() if key not in
                  {"finite_scores_by_symbol", "ready_scores_by_symbol", "latest_samples"}}))
