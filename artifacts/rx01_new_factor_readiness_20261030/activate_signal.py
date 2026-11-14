"""Enable only the new RX01 configuration after complete readiness and clear follow targets.

Default is read-only. Does not start processes or send orders itself.
"""
import argparse
import datetime
import json
from pathlib import Path
import urllib.request

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--execute", action="store_true")
args = parser.parse_args()
home = Path.home()
root = home / "binance-cta-perp-rx01"
run = home / "binance_exec_trade06/run/restore_audit_20261030"
ready = json.loads((root / "run/operator_list_20261030/formal_readiness.json").read_text())
assert ready["caught_up"] and ready["consistent_model_timestamp"]
assert ready["ready_rules"] == 146 and ready["nq_ready_symbols"] == 38
assert 0 <= datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000 - ready["model_ts_ms"] <= 1800000
catalog = json.loads((run / "catalog_verified.json").read_text())
assert not catalog["account"]["pending_publishes"]
assert catalog["account"]["configuration"] == {"mode": "follow", "follows": [{"virtual_id": "virtual06", "multiplier": 1.0}]}
assert len(catalog["positions"]) == 91
assert all(t["qty"] == "clear" and t.get("signal", 0) == 0 for p in catalog["positions"] for t in p["targets"].values())
assert not (root / "run/cta_perp_position_flags.json").exists()
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
url = "http://127.0.0.1:19195/api/config"
with opener.open(url, timeout=10) as r:
    current = json.load(r)
config = current["config"]
assert config["enabled"] is False and config["bar_interval"] == "15m"
assert config["max_pos_u"] == 200 and len(config["factor_rules"]) == 146
if args.execute:
    backup = run / "signal_config.before.json"
    with backup.open("x") as f:
        backup.chmod(0o600)
        json.dump(current, f, indent=2)
    config["enabled"] = True
    request = urllib.request.Request(url, data=json.dumps({"config": config, "expected_revision": current["revision"]}).encode(), headers={"Content-Type": "application/json"}, method="PUT")
    with opener.open(request, timeout=10) as r:
        assert json.load(r)["config"]["enabled"] is True
print(json.dumps({"execute": args.execute, "new_signal_enabled": args.execute, "rules": 146, "initial_targets": "all clear", "per_rule_usdt": 200}))
