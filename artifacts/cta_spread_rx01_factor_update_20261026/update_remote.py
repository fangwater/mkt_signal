"""Deploy the reviewed RX01 factor configuration; credentials stay on the host."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import tomllib
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument("phase", choices=["prepare", "install", "progress", "handoff", "activate", "verify"])
parser.add_argument("--execute", action="store_true")
args = parser.parse_args()
root = Path.home() / "binance-cta-spread-rx01"
stage = root / "run/factor_update_20261026"
candidate = stage / "candidate"
sys.path.insert(0, str(root / "run/reenable_20261019"))
import ops_helpers as ops

prefix = "binance_cta_spread_rx01_"
new_process = "cta_spread_factor_rx01_20261026"
binary = root / "cta_spread_factor_model_1m_pub"


def read(path):
    return json.loads(path.read_text())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".next")
    temp.write_text(json.dumps(value, indent=2) + "\n")
    os.chmod(temp, 0o600)
    os.replace(temp, path)


def get(url, body=None):
    request = urllib.request.Request(url, data=None if body is None else json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)


def config_api(path, body=None):
    return get("http://127.0.0.1:19174/api/" + path, body)


def publisher_pids():
    found = []
    for proc in Path("/proc").glob("[0-9]*"):
        try:
            command = proc.joinpath("cmdline").read_bytes().split(b"\0")
            if proc.joinpath("cwd").resolve() == root and command and Path(os.fsdecode(command[0])).name == binary.name:
                found.append(int(proc.name))
        except OSError:
            pass
    return sorted(found)


def logfile():
    path = Path.home() / ".pmdaemon/logs" / (new_process + "-error.log")
    return path.read_text(errors="replace") if path.exists() else ""


def market_rules():
    import redis
    values = ops.env_for(Path.home() / "binance_exec_trade06")
    client = redis.Redis(host=values.get("REDIS_HOST", "127.0.0.1"), port=int(values.get("REDIS_PORT", 6379)),
                         db=int(values.get("REDIS_DB", 0)), username=values.get("REDIS_USERNAME"),
                         password=values.get("REDIS_PASSWORD"), decode_responses=True, socket_timeout=5)
    result = {}
    for venue in ["binance-margin", "binance-futures"]:
        data = json.loads(client.get("binance_exec_trade06:" + venue + ":market_rules"))
        assert time.time_ns() // 1000 - data["fetched_at_us"] < 180_000_000, "stale market rules"
        statuses = {symbol: data["symbols"].get(symbol, {}).get("status", "MISSING") for symbol in table}
        assert set(statuses.values()) == {"TRADING"}, statuses
        result[venue] = {"fetched_at_us": data["fetched_at_us"], "symbols": statuses}
    return result


table = read(candidate / "cta_spread_symbol_factors.json")
factors = sorted({factor for row in table.values() for factor in row})
assert len(table) == 17 and len(factors) == 7 and sum(map(len, table.values())) == 55
assert "ICXUSDT" not in table

if args.phase == "prepare":
    current = config_api("symbol-factors")
    cfg = config_api("config")
    positions = read(root / "run/cta_spread_position_flags.json")
    rules = market_rules()
    with ops.session() as api:
        virtual = next(v for v in api("/manager/api/catalog/virtual-accounts") if v["virtual_id"] == "virtual03")
        account = api("/manager/api/catalog/accounts/binance_exec_trade06")
        assert virtual["name"] == "cta_spread_rx01"
        assert [(v["source_id"], v["multiplier"]) for v in virtual["followers"]] == [("binance_exec_trade06", 1.0)]
        bindings = {b["position_strategy_name"]: b for b in virtual["bindings"] if b["shares"] != 0}
        assert all(bindings[prefix + factor]["shares"] == 1.0 and
                   bindings[prefix + factor]["order_strategy_name"] == "lgq_grid_spread" for factor in factors)
        orders = api("/manager/api/catalog/order-strategies")
        grid = next(v for v in orders if v["strategy_name"] == "lgq_grid_spread")
    old = publisher_pids()
    assert len(old) == 1, old
    removed = [(factor, symbol) for symbol, row in current["symbol_factors"].items()
               for factor in row if factor not in table.get(symbol, {})]
    plan = {"symbols": 17, "factors": 7, "combinations": 55, "excluded_symbols": ["ICXUSDT"],
            "removed_combinations": len(removed),
            "removed_directional": sum(positions.get(f, {}).get(s, {}).get("flag", 0) != 0 for f, s in removed),
            "new_strategies": [], "config": cfg["config"], "old_publisher_pid": old[0],
            "release_sha256": sha(binary), "reuse_verified_release": True, "execute": args.execute}
    print(json.dumps(plan), flush=True)
    if args.execute:
        assert not (stage / "prepared.json").exists()
        before = stage / "before"
        before.mkdir(mode=0o700, exist_ok=True)
        for name, value in [("factor_api.json", current), ("config_api.json", cfg), ("virtual.json", virtual),
                            ("account.json", account), ("grid_template.json", grid), ("market_rules.json", rules),
                            ("positions.json", positions), ("exec_snapshot.json", get("http://127.0.0.1:10046/snapshot"))]:
            save(before / name, value)
        for name in ["cta_spread.json", "cta_spread_factor_model_1m_pub.toml", "cta_spread_symbol_factors.json", "cta_spread_deployment.json"]:
            shutil.copy2(root / "config" / name, before / name)
        for name in ["cta_spread_status.json", "cta_spread_position_flags.json"]:
            shutil.copy2(root / "run" / name, before / name)
        shutil.copy2(root / "env.sh", before / "env.sh")
        os.chmod(before / "env.sh", 0o600)
        save(before / "warmup.json", read(root / "data/cta_spread/factor_history/warmup.json"))
        save(before / "processes.json", {"signal": ops.live_pids(root), "exec": ops.live_pids(Path.home() / "binance_exec_trade06")})
        save(stage / "prepared.json", plan)

elif args.phase == "install":
    assert args.execute and (stage / "prepared.json").exists()
    assert not (stage / "installed.json").exists()
    manifest = read(stage / "manifest.json")
    for name, expected in manifest.items():
        assert sha(candidate / name) == expected, name
    assert sha(binary) == manifest[binary.name] == read(stage / "prepared.json")["release_sha256"]
    destination = root / "config/cta_spread_factor_model_1m_pub.toml"
    assert sha(destination) == sha(stage / "before" / destination.name), "operator changed publisher config"
    previous = tomllib.loads(destination.read_text())
    next_config = tomllib.loads((candidate / destination.name).read_text())
    assert {k: v for k, v in previous.items() if k != "universe"} == {k: v for k, v in next_config.items() if k != "universe"}
    temp = destination.with_suffix(".toml.next")
    shutil.copy2(candidate / destination.name, temp)
    os.replace(temp, destination)
    subprocess.run([str(root / "scripts/start_cta_spread_factor_model_1m_pub.sh")], cwd=root,
                   env={**os.environ, "PMDAEMON_NAME": new_process, "CTA_SPREAD_FACTOR_WAIT_FOR_PUBLISHERS": "1"}, check=True)
    save(stage / "installed.json", {"manifest": manifest, "started_ms": time.time_ns() // 1_000_000,
                                     "process_name": new_process, "binary_reused": True})
    print("Standby publisher started through the environment-local script; original publisher remains live.")

elif args.phase == "progress":
    log = logfile()
    pids = publisher_pids()
    rows = []
    for pid in pids:
        proc = Path("/proc") / str(pid)
        status = proc.joinpath("status").read_text()
        rows.append({"pid": pid, "cpu_list": re.search(r"Cpus_allowed_list:\s*(.*)", status)[1],
                     "rss_kb": re.search(r"VmRSS:\s*(\d+)", status)[1]})
    lines = log.splitlines()
    print(json.dumps({"publishers": rows, "standby_replay_complete": "CTA spread standby replay complete" in log,
                      "new_publisher_ready": "CTA spread shared factor publisher ready" in log,
                      "replay_failed": "Kafka replay had decode/evaluation errors" in log,
                      "recent_log": lines[-4:]}))

elif args.phase == "handoff":
    assert args.execute and (stage / "installed.json").exists()
    assert not (stage / "handoff.json").exists()
    assert "CTA spread standby replay complete" in logfile(), "standby replay still running"
    previous = read(stage / "prepared.json")
    old_pid = previous["old_publisher_pid"]
    candidates = [pid for pid in publisher_pids() if pid != old_pid]
    assert len(candidates) == 1, candidates
    keep_pid = candidates[0]
    assert sha(root / "env.sh") == sha(stage / "before/env.sh"), "operator changed env.sh"
    old_process = ops.env_for(root)["CTA_SPREAD_FACTOR_PROCESS_NAME"]
    assert old_process == "cta_spread_factor_rx01_20261022"
    save(stage / "before/warmup_at_handoff.json", read(root / "data/cta_spread/factor_history/warmup.json"))
    subprocess.run([str(root / "scripts/stop_cta_spread_factor_model_1m_pub.sh")], cwd=root,
                   env={**os.environ, "PMDAEMON_NAME": old_process, "CTA_SPREAD_FACTOR_KEEP_PID": str(keep_pid)}, check=True)
    assert Path(f"/proc/{keep_pid}").exists()
    env_path = root / "env.sh"
    text, count = re.subn(r"^export CTA_SPREAD_FACTOR_PROCESS_NAME=.*$",
                         "export CTA_SPREAD_FACTOR_PROCESS_NAME='" + new_process + "'", env_path.read_text(), flags=re.M)
    assert count == 1
    temp = env_path.with_suffix(".sh.next")
    temp.write_text(text)
    shutil.copymode(env_path, temp)
    os.replace(temp, env_path)
    save(stage / "handoff.json", {"pid": keep_pid, "old_pid": old_pid, "old_process": old_process,
                                  "time_ms": time.time_ns() // 1_000_000})
    print(json.dumps({"handoff_pid": keep_pid, "process_name": new_process}))

elif args.phase == "activate":
    assert args.execute and (stage / "handoff.json").exists()
    assert not (stage / "activated.json").exists()
    warm = read(root / "data/cta_spread/factor_history/warmup.json")
    assert warm["caught_up"] and warm["size_dependent_factors"] == []
    assert set(warm["latest"]) == set(table)
    assert time.time() * 1000 - min(warm["latest"].values()) < 180_000
    assert all(warm[k] == 0 for k in ["decode_errors", "evaluation_errors", "publish_failed"]), warm
    latest = {}
    history = sorted((root / "data/cta_spread/factor_history").glob("*.jsonl"))[-1]
    with history.open("rb") as stream:
        stream.seek(max(0, history.stat().st_size - 4 * 1024 * 1024))
        if stream.tell():
            stream.readline()
        for line in stream:
            row = json.loads(line)
            if row["generation"] == warm["generation"]:
                latest[row["symbol"]] = row
    for symbol, names in table.items():
        row = latest[symbol]
        assert time.time() * 1000 - row["model_ts_ms"] < 180_000
        assert row["spread"]["ready"] and row["nq"]["ready"], symbol
        assert all(row["inputs"][name]["ready"] for name in names), symbol
    market_rules()
    current = config_api("symbol-factors")
    assert current["revision"] == read(stage / "before/factor_api.json")["revision"], "operator changed factor table"
    assert config_api("config")["config"] == read(stage / "before/config_api.json")["config"], "operator changed signal config"
    save(stage / "before/positions_at_switch.json", read(root / "run/cta_spread_position_flags.json"))
    for name in ["cta_spread_symbol_factors.json", "cta_spread_deployment.json"]:
        destination = root / "config" / name
        temp = destination.with_suffix(destination.suffix + ".next")
        shutil.copy2(candidate / name, temp)
        os.replace(temp, destination)
    saved = config_api("symbol-factors", {"symbol_factors": table, "expected_revision": current["revision"]})
    assert saved["symbol_factors"] == table
    save(stage / "activated.json", {"generation": warm["generation"], "revision": saved["revision"],
                                    "activated_ms": time.time_ns() // 1_000_000, "symbols": 17, "factors": 7, "combinations": 55})
    print(json.dumps({"activated": True, "generation": warm["generation"], "symbols": 17, "factors": 7, "combinations": 55}))

elif args.phase == "verify":
    status = read(root / "run/cta_spread_status.json")
    warm = read(root / "data/cta_spread/factor_history/warmup.json")
    positions = read(root / "run/cta_spread_position_flags.json")
    requested = [(f, s) for s, fs in table.items() for f in fs]
    unavailable = [(f, s) for f, s in requested if not status["strategies"].get(f, {}).get(s, {}).get("score_ready")]
    removed_active = [(f, s) for f, rows in positions.items() for s, p in rows.items()
                      if f not in table.get(s, {}) and p["flag"] != 0]
    with ops.session() as api:
        virtual = next(v for v in api("/manager/api/catalog/virtual-accounts") if v["virtual_id"] == "virtual03")
        strategies = {v["strategy_name"]: v for v in api("/manager/api/catalog/position-strategies")}
        bound = {v["position_strategy_name"] for v in virtual["bindings"] if v["shares"] == 1}
        account = api("/manager/api/catalog/accounts/binance_exec_trade06")
    mismatches = []
    target_rows = 0
    for factor, rows in positions.items():
        targets = strategies.get(prefix + factor, {}).get("targets", {})
        for symbol, position in rows.items():
            for market, field in [(symbol, "perpetual_qty"), (symbol.removesuffix("USDT"), "cash_qty")]:
                actual = targets.get(market, {}).get("qty", 0.0)
                target_rows += 1
                if not isinstance(actual, (int, float)) or not math.isclose(actual, position[field], rel_tol=1e-10, abs_tol=1e-12):
                    mismatches.append([factor, market, position[field], actual])
    snapshot = get("http://127.0.0.1:10046/snapshot")
    state = next(v["entry"] for v in snapshot["entries"] if v["channel"] == "exec_pre_trade_state")
    rows = [r for r in state["rows"] if r["strategy_name"].startswith(prefix)]
    incomplete = [r for r in rows if not r["execution_complete"]]
    live = [r for r in rows if abs(r["live_order_qty"]) > 1e-10 or abs(r["pending_qty"]) > 1e-10 or r["active_batches"]]
    result = {"symbols": len(status["symbol_factors"]), "factors": len(factors), "combinations": len(requested),
              "table_matches": status["symbol_factors"] == table, "enabled": status["enabled"], "max_pos_u": status["max_pos_u"],
              "publication_error": status["publication_error"], "journal_healthy": status["journal_healthy"],
              "unavailable": unavailable, "retired_directional_positions": removed_active,
              "all_requested_bound": all(prefix + f in bound for f in factors), "manager_checked_target_rows": target_rows,
              "manager_target_mismatches": mismatches,
              "config_unchanged": config_api("config")["config"] == read(stage / "before/config_api.json")["config"],
              "followers": virtual["followers"], "account_pending_publishes": account.get("pending_publishes"),
              "exec_state_ts_ms": state["ts_ms"], "exec_position_ready": state["position_ready"], "exec_rows": len(rows),
              "exec_incomplete": incomplete, "exec_live_orders_or_pending": live,
              "new_nonzero_directional_combinations": sum(positions.get(f, {}).get(s, {}).get("flag", 0) != 0 for f, s in requested),
              "warmup": warm}
    save(stage / "verification.json", result)
    save(stage / "exec_snapshot.after.json", snapshot)
    summary = {k: v for k, v in result.items() if k not in {"warmup", "exec_incomplete", "exec_live_orders_or_pending"}}
    summary["exec_incomplete_count"] = len(incomplete)
    summary["exec_live_orders_or_pending_count"] = len(live)
    print(json.dumps(summary))
