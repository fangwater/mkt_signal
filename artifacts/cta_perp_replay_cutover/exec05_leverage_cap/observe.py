"""Read-only Exec05 startup, requested leverage and verified-response evidence."""
from pathlib import Path
import datetime
import hashlib
import importlib.util
import json
import os
import re
import time

stage = Path.home() / "cta_perp_replay_cutover_stage_20261015"
installed = json.loads((stage / "exec05_leverage_cap/installed.json").read_text())
pid = installed["new_pid"]
proc = Path("/proc") / str(pid)
assert proc.exists() and hashlib.sha256((proc / "exe").read_bytes()).hexdigest() == installed["binary_sha256"]
assert sorted(os.sched_getaffinity(pid)) == installed["affinity"]
spec = importlib.util.spec_from_file_location("cutover", stage / "manager_cutover.py")
cutover = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cutover)
import redis
values = cutover.env_values(Path.home() / "binance_exec_trade05")
client = redis.Redis.from_url(values.get("REDIS_URL", "redis://127.0.0.1:6379/0"), decode_responses=True, socket_timeout=10)
try:
    target = json.loads(client.get("binance_exec_trade05:binance-futures:batch_exec:lgq_cta_symbol_mean"))
finally:
    client.close()
required = {s for s, t in target["targets"].items() if type(t["qty"]) in (float, int) and t["qty"] != 0}
log = Path.home() / ".pmdaemon/logs/exec_pt_binance_exec_trade05-error.log"
with log.open("rb") as file:
    file.seek(max(0, log.stat().st_size - 2_000_000))
    lines = file.read().decode(errors="replace").splitlines()
confirmed = {}
capped = {}
blocked = set()
errors = 0
for line in lines:
    timestamp = re.search(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z", line)
    if not timestamp or datetime.datetime.fromisoformat(timestamp[0].replace("Z", "+00:00")).timestamp() * 1e6 < installed["restart_ts_us"]:
        continue
    errors += "ERROR" in line
    match = re.search(r"RapidX leverage confirmed: sym=BINANCE_PERP_([A-Z0-9]+)_USDT leverage=(\d+)", line)
    if match:
        confirmed[match[1] + "USDT"] = int(match[2])
        blocked.discard(match[1] + "USDT")
    match = re.search(r"RapidX Exec default leverage capped by venue: sym=BINANCE_PERP_([A-Z0-9]+)_USDT requested=(\d+) confirmed_limit=(\d+)", line)
    if match:
        capped[match[1] + "USDT"] = {"requested": int(match[2]), "limit": int(match[3])}
    match = re.search(r"symbol activation blocked by leverage initialization: symbol=([A-Z0-9]+)", line)
    if match:
        blocked.add(match[1])
print(json.dumps({"checked_ts_us": time.time_ns() // 1000, "pid": pid,
    "binary_sha256": installed["binary_sha256"], "required_symbols": sorted(required),
    "confirmed_leverages": confirmed, "capped": capped,
    "required_unconfirmed": sorted(required - confirmed.keys()),
    "blocked_since_startup": sorted(blocked), "error_lines_since_startup": errors}, indent=2))
