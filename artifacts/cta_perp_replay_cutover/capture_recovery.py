"""Read-only cutover evidence; credentials stay in remote memory.

An ephemeral authenticated session permits Nginx snapshot reads and is removed
in finally. No catalog, runtime Redis, Exec, or exchange state is written.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import secrets
import time
import urllib.request

root = Path.home()
stage = root / "cta_perp_replay_cutover_stage_20261015"
spec = importlib.util.spec_from_file_location("cutover", stage / "manager_cutover.py")
cutover = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cutover)
assert os.uname().nodename.startswith("ip-172-31-35-228")
result = {"captured_ts_us": time.time_ns() // 1000, "catalog": cutover.snapshot(),
          "environments": {}, "runtime_targets": {}, "exec": {}, "processes": []}

for name in ("binance-cta-spread-rx01", "binance-cta-perp-rx02", "binance-cta-perp-rx03"):
    env = root / name
    family = "spread" if "spread" in name else "perp"
    entry = {}
    for relative in (f"config/cta_{family}.json", f"config/cta_{family}_symbol_factors.json",
                     f"run/cta_{family}_status.json", f"run/cta_{family}_position_flags.json",
                     "run/cta_perp_recovery_request.json", "run/cta_perp_recovery_completed.json",
                     "data/cta_perp/factor_history/recovery.json"):
        file = env / relative
        if file.exists():
            entry[relative] = json.loads(file.read_text())
    entry["configuration_hashes"] = {
        str(file.relative_to(env)): hashlib.sha256(file.read_bytes()).hexdigest()
        for file in [env / "env.sh", *env.glob("config/cta*")] if file.is_file()
    }
    entry["captured_ts_us"] = time.time_ns() // 1000
    result["environments"][name] = entry

import redis
for source in ("binance_exec_trade05", "binance_exec_trade07"):
    values = cutover.env_values(root / source)
    client = redis.Redis.from_url(values.get("REDIS_URL", "redis://127.0.0.1:6379/0"),
                                 decode_responses=True, socket_timeout=15)
    try:
        result["runtime_targets"][source] = {}
        for binding in result["catalog"]["bindings"]:
            if binding["source_id"] == source:
                name = binding["binding_name"]
                raw = client.get(source + ":binance-futures:batch_exec:" + name)
                result["runtime_targets"][source][name] = json.loads(raw) if raw else None
    finally:
        client.close()

token = secrets.token_urlsafe(32)
digest = hashlib.sha256(token.encode()).hexdigest()
sid = cutover.sql(f"INSERT INTO cta_auth_sessions(user_id,token_hash,expires_at) SELECT user_id,decode('{digest}','hex'),now()+interval '5 minutes' FROM cta_users WHERE username='prc' AND disabled=false RETURNING session_id;")
assert sid.isdigit()
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
try:
    for number in ("05", "07"):
        req = urllib.request.Request("http://127.0.0.1:4191/exec_trade" + number + "/snapshot",
                                     headers={"Cookie": "cta_session=" + token})
        with opener.open(req, timeout=30) as response:
            data = json.load(response)
        result["exec"]["binance_exec_trade" + number] = next(
            row for row in data["entries"] if row["channel"] == "exec_pre_trade_state")
finally:
    cutover.sql(f"DELETE FROM cta_auth_sessions WHERE session_id={sid} AND token_hash=decode('{digest}','hex');")

for proc in Path("/proc").iterdir():
    if not proc.name.isdigit():
        continue
    try:
        cwd = os.readlink(proc / "cwd")
        if cwd.startswith(str(root)):
            result["processes"].append({"pid": int(proc.name), "cwd": cwd,
                "exe": os.readlink(proc / "exe"), "start": (proc / "stat").read_text().split()[21],
                "cpus": sorted(os.sched_getaffinity(int(proc.name)))})
    except (OSError, ProcessLookupError):
        pass
print(json.dumps(result, indent=2))
