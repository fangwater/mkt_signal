#!/usr/bin/env python3
"""Read-only post-publish checks; print summaries without private log payloads."""

import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import time
import urllib.request

import redis


def recent_lines(path, cutoff, size=8_000_000):
    if not path.exists():
        return []
    with path.open("rb") as stream:
        stream.seek(max(0, path.stat().st_size - size))
        lines = stream.read().decode(errors="replace").splitlines()
    return [line for line in lines if line.startswith("[2026-") and line[1:20] >= cutoff]


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


now = time.time()
cutoff = dt.datetime.fromtimestamp(now - 60, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
base = Path.home() / "binance_fr_arb03"
factor = Path.home() / "trade_flow_feature/binance-futures"
result = {
    "utc": dt.datetime.fromtimestamp(now, dt.timezone.utc).isoformat(),
    "log_window_since": cutoff,
    "processes": [],
    "config_sha256": {
        name: sha256(base / name)
        for name in ["env.sh", "trade_engine.toml", "config/viz.toml"]
    },
    "factor_sha256": {
        name: sha256(factor / name)
        for name in ["trade_flow_feature_pub", "env.sh", "config/trade_flow_feature_pub.yaml"]
    },
}
for proc in Path("/proc").iterdir():
    if not proc.name.isdigit():
        continue
    try:
        cwd = os.readlink(proc / "cwd")
        if cwd not in [str(base), str(factor)]:
            continue
        comm = (proc / "comm").read_text().strip()
        if comm in ["bash", "sh", "python3"] and cwd == str(factor):
            continue
        affinity = next(
            line.split(":", 1)[1].strip()
            for line in (proc / "status").read_text().splitlines()
            if line.startswith("Cpus_allowed_list:")
        )
        result["processes"].append({"pid": int(proc.name), "comm": comm, "cwd": cwd, "cpus": affinity})
    except (OSError, StopIteration):
        continue

snapshot = json.load(urllib.request.urlopen("http://127.0.0.1:20133/snapshot", timeout=10))
result["snapshot_age_sec"] = now - snapshot["ts_ms"] / 1000
result["snapshot_channels"] = [entry["channel"] for entry in snapshot["entries"]]
for entry in snapshot["entries"]:
    if "risk" in entry["channel"]:
        result[entry["channel"]] = entry["entry"]
    if entry["channel"] == "pre_trade_exposure":
        rows = [row for row in entry["entry"].get("rows", []) if not row.get("is_total")]
        rows.sort(key=lambda row: abs(row.get("net_usdt") or 0), reverse=True)
        result["largest_net_exposures"] = [
            {key: row.get(key) for key in ["asset", "net_qty", "net_usdt"]}
            for row in rows[:8]
        ]

factor_lines = recent_lines(Path.home() / ".pmdaemon/logs/tff_bn_fu-error.log", cutoff)
result["factor_publish"] = [line for line in factor_lines if "publish_outcome_10s" in line][-3:]
result["factor_fatal_count"] = sum(" ERROR " in line or "panicked" in line for line in factor_lines)
signal_lines = recent_lines(Path.home() / ".pm2/logs/fr-ts-bn-arb03-error.log", cutoff)
result["signal_vol_suppress_count"] = sum("dual venue volatility is not ready" in line for line in signal_lines)
result["signal_missing_factor_count"] = sum("missing_ipc_snapshot" in line for line in signal_lines)
result["signal_last_suppression_utc"] = max(
    (line[1:21] for line in signal_lines if "dual venue volatility is not ready" in line),
    default=None,
)
result["signal_not_ready_symbols"] = sorted({
    match.group(1)
    for line in signal_lines
    if "dual venue volatility is not ready" in line
    for match in [re.search(r"open=([A-Z0-9]+)", line)]
    if match
})
for process, filename in [
    ("engine", ".pmdaemon/logs/fr_te_bn_arb03-error.log"),
    ("pre_trade", ".pmdaemon/logs/fr_pt_bn_arb03-error.log"),
]:
    lines = recent_lines(Path.home() / filename, cutoff)
    result[process] = {
        "51042_line_count": sum("51042" in line for line in lines),
        "dispatch_reject_count": sum("51042" in line and "Dispatch BINANCE" in line for line in lines),
        "position_limit_reject_count": sum("position-limit" in line and ("reject" in line or "exceeded" in line) for line in lines),
        "fatal_count": sum("panicked" in line for line in lines),
    }

client = redis.Redis(decode_responses=True, socket_timeout=10)
result["bitget_shared_key_exists"] = {
    key: bool(client.exists(key))
    for key in ["bitget_position_tier_pool:envs", "bitget_position_tier_cache:USDT-FUTURES"]
}
result["futures_amount_threshold_count"] = client.hlen("binance-futures:amount-thresholds")
print(json.dumps(result, ensure_ascii=False))
