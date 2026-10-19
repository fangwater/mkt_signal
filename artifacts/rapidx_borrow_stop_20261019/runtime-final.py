"""Publish the verified RX01 borrow-stop release; mutations require --execute."""
import argparse
import collections
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import tomllib
import urllib.request

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("phase", choices=["audit", "install", "resume", "observe", "enable"])
parser.add_argument("--execute", action="store_true")
args = parser.parse_args()
assert os.uname().nodename == "ip-172-31-35-228"
home = Path.home()
exe = home / "binance_exec_trade06"
signal = home / "binance-cta-spread-rx01"
stage = exe / "run/borrow_stop_20261019T130500Z"
stage.mkdir(mode=0o700, exist_ok=True)
spec = importlib.util.spec_from_file_location("ops_helpers", signal / "run/reenable_20261019/ops_helpers.py")
ops = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ops)
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
config_url = "http://127.0.0.1:19174/api/config"
log_path = home / ".pmdaemon/logs/exec_pt_binance_exec_trade06-error.log"

def digest(path):
    return hashlib.file_digest(path.open("rb"), "sha256").hexdigest()

def save(name, value):
    ops.save(stage / name, value)
    os.chmod(stage / name, 0o600)

def get(url):
    with opener.open(url, timeout=20) as response:
        return json.load(response)

def config_put(config, revision):
    req = urllib.request.Request(config_url,
        data=json.dumps(dict(config=config, expected_revision=revision)).encode(),
        headers={"Content-Type": "application/json"}, method="PUT")
    with opener.open(req, timeout=20) as response:
        return json.load(response)

def protected_files():
    paths = [exe / "env.sh", exe / "trade_engine.toml", signal / "env.sh",
             signal / "cta_spread_signal", signal / "cta_spread_factor_model_1m_pub",
             *sorted((exe / "config").glob("*"))]
    return {str(p): digest(p) for p in paths if p.is_file()}

def processes(root):
    rows = ops.live_pids(root)
    for row in rows:
        row["affinity"] = sorted(os.sched_getaffinity(row["pid"]))
    return rows

def wrapper(script):
    with (stage / (script + ".log")).open("ab") as log:
        os.chmod(stage / (script + ".log"), 0o600)
        result = subprocess.run([str(exe / "scripts" / script)], cwd=exe,
            env=dict(os.environ, PATH=str(home / ".local/bin") + ":" + os.environ["PATH"]),
            stdout=log, stderr=subprocess.STDOUT, timeout=180)
    assert result.returncode == 0, script + " failed; inspect private host log"

def state_snapshot(min_ts_ms=0):
    # New target generations briefly pause ledger allocation. Sample a settled
    # snapshot rather than treating that normal transition as startup failure.
    deadline = time.monotonic() + 45
    while True:
        snapshot = get("http://127.0.0.1:10046/snapshot")
        state = next(e for e in snapshot["entries"] if e["channel"] == "exec_pre_trade_state")
        if state["entry"]["position_ready"] and state["ts_ms"] > min_ts_ms and time.time() * 1000 - state["ts_ms"] < 15000:
            return state
        assert time.monotonic() < deadline, "Exec position snapshot did not settle"
        time.sleep(1)

if args.phase == "audit":
    assert not (stage / "before.json").exists(), "audit already captured"
    before = dict(ts_us=time.time_ns() // 1000, config=get(config_url),
        processes=processes(exe), signal_processes=processes(signal),
        protected_files=protected_files(),
        binaries={name: digest(exe / name) for name in ["exec-pre-trade", "viz_server"]},
        state=state_snapshot(), log_offset=log_path.stat().st_size)
    assert before["state"]["entry"]["position_ready"]
    assert tomllib.loads((exe / "trade_engine.toml").read_text())["local_ips"] == ["172.31.35.228"]
    save("before.json", before)
    print(json.dumps(dict(env=exe.name, observed_only=True,
        enabled=before["config"]["config"]["enabled"], processes=before["processes"],
        band_rows=[r for r in before["state"]["entry"]["rows"] if r["symbol"] in ["BAND", "BANDUSDT"]])))

elif args.phase in ["install", "resume"]:
    before = json.loads((stage / "before.json").read_text())
    manifest = json.loads((stage / "manifest.json").read_text())
    assert protected_files() == before["protected_files"], "protected configuration changed"
    if args.phase == "install":
        assert processes(exe) == before["processes"], "Exec processes changed"
        assert get(config_url) == before["config"], "signal config changed"
    else:
        assert args.execute
        assert not (stage / "installed.json").exists()
        assert get(config_url) == json.loads((stage / "pause.json").read_text())
        assert not any(p["comm"] in manifest["binaries"] for p in processes(exe))
    for name, sha in manifest["binaries"].items():
        assert digest(stage / name) == sha, name + " upload checksum mismatch"
        assert digest(exe / name) == (before["binaries"][name] if args.phase == "install" else sha), name + " live version changed"
    if not args.execute:
        print(json.dumps(dict(dry_run=True, env=exe.name, checksum_verified=True,
            binaries=manifest["binaries"], restart=list(manifest["binaries"]), default_leverage=4)))
        sys.exit(0)
    if args.phase == "install":
        assert not (stage / "pause.json").exists(), "installation already started"
        for name in manifest["binaries"]:
            shutil.copy2(exe / name, stage / (name + ".before"))
        shutil.copy2(exe / "release.json", stage / "release.json.before")
        pause = config_put(dict(before["config"]["config"], enabled=False), before["config"]["revision"])
        save("pause.json", pause)
        wrapper("stop_exec_pre_trade.sh")
        if "viz_server" in manifest["binaries"]:
            wrapper("stop_exec_viz_server.sh")
        assert not any(p["comm"] in manifest["binaries"] for p in processes(exe))
        for name, sha in manifest["binaries"].items():
            temporary = exe / ("." + name + ".borrow-stop.next")
            assert not temporary.exists()
            shutil.copy2(stage / name, temporary)
            temporary.chmod(0o755)
            assert digest(temporary) == sha
            os.replace(temporary, exe / name)
    started = dict(ts_us=time.time_ns() // 1000, log_offset=log_path.stat().st_size if log_path.exists() else 0)
    save("started.json", started)
    if "viz_server" in manifest["binaries"]:
        wrapper("start_exec_viz_server.sh")
    wrapper("start_exec_pre_trade.sh")
    current = processes(exe)
    for name, sha in manifest["binaries"].items():
        row = next(p for p in current if p["comm"] == name)
        assert digest(Path("/proc") / str(row["pid"]) / "exe") == sha
        assert row["affinity"] == next(p["affinity"] for p in before["processes"] if p["comm"] == name)
    assert protected_files() == before["protected_files"]
    unchanged = [p for p in before["processes"] if p["comm"] not in manifest["binaries"]]
    assert all(p in current for p in unchanged), "another Exec component restarted"
    assert processes(signal) == before["signal_processes"], "signal/publisher processes changed"
    release = json.loads((exe / "release.json").read_text())
    release.setdefault("binaries", {}).update(manifest["binaries"])
    release.update(pre_trade_release_id=manifest["release_id"], pre_trade_base_commit=manifest["base_commit"],
        pre_trade_binary_sha256=manifest["binaries"]["exec-pre-trade"],
        pre_trade_source_patch_sha256=manifest["source_patch_sha256"],
        pre_trade_deployed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        rapidx_exec_default_leverage=4)
    ops.save(exe / "release.json", release)
    save("installed.json", dict(manifest=manifest, processes=current, protected_configuration_preserved=True,
        signal_and_publisher_preserved=True, started=started))
    print(json.dumps(dict(installed=True, processes=current, publication_enabled=False)))

elif args.phase == "observe":
    before = json.loads((stage / "before.json").read_text())
    installed = json.loads((stage / "installed.json").read_text())
    started = installed["started"]
    state = state_snapshot(started["ts_us"] / 1000)
    assert state["entry"]["position_ready"]
    assert protected_files() == before["protected_files"]
    current = processes(exe)
    assert current == installed["processes"], "Exec process changed after installation"
    assert processes(signal) == before["signal_processes"]
    for name, sha in installed["manifest"]["binaries"].items():
        row = next(p for p in current if p["comm"] == name)
        assert digest(Path("/proc") / str(row["pid"]) / "exe") == sha
    with log_path.open("rb") as log:
        log.seek(started["log_offset"])
        content = log.read().decode(errors="replace")
    confirmed = dict((coin + "USDT", int(leverage)) for coin, leverage in
        re.findall(r"RapidX leverage confirmed: sym=BINANCE_PERP_([A-Z0-9]+)_USDT leverage=(\d+)", content))
    blocked = sorted(set(re.findall(r"symbol activation blocked by leverage initialization: symbol=([A-Z0-9]+)", content)) - confirmed.keys())
    assert confirmed and all(value <= 4 for value in confirmed.values())
    assert not blocked, "unconfirmed leverage activation"
    values = ops.env_for(exe)
    os.environ.update(values)
    sys.path.insert(0, str(home / "binance-cta-rx01/scripts"))
    from neutralize_rapidx_binance import Client
    client = Client("172.31.35.228", execute=False)
    band = client.request("GET", "/api/v1/trading/perp/leverage", {"sym": "BINANCE_PERP_BAND_USDT"})
    assert len(band) == 1 and band[0]["sym"] == "BINANCE_PERP_BAND_USDT" and band[0]["leverage"] == "4"
    with opener.open("http://127.0.0.1:10046/", timeout=15) as response:
        assert "借币不足，已放弃本次执行" in response.read().decode()
    rows = state["entry"]["rows"]
    result = dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        processes=current, position_ready=True, state_ts_ms=state["ts_ms"],
        confirmed_leverages=confirmed, blocked_leverages=blocked, band_leverage=4,
        completion_reasons=dict(collections.Counter(r.get("completion_reason", "") for r in rows)),
        borrow_stopped_rows=[r for r in rows if r.get("completion_reason") == "borrow_unavailable"],
        rate_limit_errors=content.count("code=429"),
        borrow_pool_errors=content.count("code=401126") + content.count("code=406017"),
        publication_enabled=get(config_url)["config"]["enabled"],
        protected_configuration_preserved=True, signal_and_publisher_preserved=True,
        band_rows=[r for r in rows if r["symbol"] in ["BAND", "BANDUSDT"]])
    save("observation.json", result)
    save("exec_snapshot.json", state)
    print(json.dumps(result))

elif args.phase == "enable":
    assert args.execute, "enable requires --execute"
    observation = json.loads((stage / "observation.json").read_text())
    assert time.time() * 1000 - observation["state_ts_ms"] < 60000, "reobserve before enabling"
    before = json.loads((stage / "before.json").read_text())
    pause = json.loads((stage / "pause.json").read_text())
    assert get(config_url) == pause, "operator changed configuration during deployment"
    enabled = config_put(before["config"]["config"], pause["revision"])
    save("enabled.json", enabled)
    assert enabled["config"] == before["config"]["config"]
    print(json.dumps(dict(enabled=enabled["config"]["enabled"], max_pos_u=enabled["config"]["max_pos_u"], restored_config=True)))
