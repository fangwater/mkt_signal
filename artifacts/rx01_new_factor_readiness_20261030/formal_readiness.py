"""Read-only validation of every configured RX01 rule against the latest closed bar."""
import datetime
import json
from pathlib import Path
import sys

root = Path.home() / "binance-cta-perp-rx01"
config = json.loads((root / "config/cta_perp.json").read_text())
history = root / "data/cta_perp_15m/factor_history"
recovery = json.loads((history / "recovery.json").read_text())
latest = {}
for f in sorted(history.glob("*.jsonl")):
    for line in f.open():
        row = json.loads(line)
        if row["generation"] == recovery["generation"]:
            latest[row["symbol"]] = row
rules = config["factor_rules"]
unready = []
for rule in rules:
    row = latest.get(rule["标的"], {})
    value = row.get("inputs", {}).get(rule["因子"], {})
    if not value.get("ready"):
        unready.append({"symbol": rule["标的"], "factor": rule["因子"], "nq": rule["nq"]})
times = {row["model_ts_ms"] for row in latest.values()}
summary = {
    "observed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "generation": recovery["generation"], "caught_up": recovery["caught_up"],
    "symbols": len(latest), "configured_rules": len(rules), "ready_rules": len(rules) - len(unready),
    "nq_ready_symbols": sum(r["nq"]["ready"] for r in latest.values()),
    "consistent_model_timestamp": len(times) == 1,
    "model_ts_ms": min(times) if times else None,
    "unready": unready,
    "latest": latest,
}
out = root / "run/operator_list_20261030/formal_readiness.json"
out.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
print(json.dumps({k: v for k, v in summary.items() if k != "latest"}, ensure_ascii=False))
if "--require-ready" in sys.argv:
    assert len(rules) == 146 and summary["ready_rules"] == 146 and len(latest) == 38
    assert summary["nq_ready_symbols"] == 38 and summary["consistent_model_timestamp"] and summary["caught_up"]
    assert datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000 - summary["model_ts_ms"] <= 1800000
