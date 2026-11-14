"""Read-only deployment evidence; never prints credentials or signed requests."""
import datetime as dt
import hashlib
import json
import os
import time
import urllib.request
from pathlib import Path

root = Path.home() / "binance_fr_arb03"
now = time.time()
cutoff = dt.datetime.fromtimestamp(now - 120, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
out = {"utc": dt.datetime.now(dt.timezone.utc).isoformat(), "cutoff": cutoff, "processes": []}
for p in Path("/proc").iterdir():
    if not p.name.isdigit():
        continue
    try:
        if os.readlink(p / "cwd") != str(root):
            continue
        comm = (p / "comm").read_text().strip()
        if comm in ["bash", "sh"]:
            continue
        out["processes"].append({"pid": int(p.name), "comm": comm, "cpus": next(
            line.split(":", 1)[1].strip() for line in (p / "status").read_text().splitlines()
            if line.startswith("Cpus_allowed_list:"))})
    except (OSError, StopIteration):
        pass
out["hashes"] = {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in [
    "env.sh", "trade_engine.toml", "config/viz.toml", "www/index.html",
    "pre_trade", "trade_engine", "trade_signal", "account_monitor", "viz_server", "persist_manager"]}
snapshot = json.load(urllib.request.urlopen("http://127.0.0.1:20133/snapshot", timeout=10))
out["snapshot_age_sec"] = now - snapshot["ts_ms"] / 1000
for entry in snapshot["entries"]:
    if entry["channel"] == "pre_trade_risk":
        out["risk"] = entry["entry"]
    if entry["channel"] == "pre_trade_exposure":
        out["net_exposure_usdt"] = sum(abs(row.get("net_usdt") or 0) for row in entry["entry"].get("rows", []) if not row.get("is_total"))
for name, path in [("engine", ".pmdaemon/logs/fr_te_bn_arb03-error.log"),
                   ("pre_trade", ".pmdaemon/logs/fr_pt_bn_arb03-error.log"),
                   ("signal", ".pm2/logs/fr-ts-bn-arb03-error.log")]:
    p = Path.home() / path
    with p.open("rb") as f:
        f.seek(max(0, p.stat().st_size - 12_000_000))
        lines = [line for line in f.read().decode(errors="replace").splitlines() if line.startswith("[2026-") and line[1:20] >= cutoff]
    out[name] = {"lines": len(lines), "51042": sum("51042" in x for x in lines),
                 "51014": sum("51014" in x for x in lines),
                 "panics": sum("panicked" in x for x in lines),
                 "capacity_query_errors": sum("BinancePmBuyCapacity:" in x and ("failed" in x or "invalid/error" in x) for x in lines),
                 "volatility_suppression": sum("dual venue volatility is not ready" in x for x in lines),
                 "capacity_samples": [x for x in lines if "BinancePmBuyCapacity: cash=" in x][-3:]}
print(json.dumps(out, ensure_ascii=False, indent=2))
