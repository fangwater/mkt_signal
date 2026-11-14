"""Publish the verified Exec05 leverage fix; dry-run unless --execute is given."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--execute", action="store_true")
args = parser.parse_args()
assert os.uname().nodename.startswith("ip-172-31-35-228")
root = Path.home() / "binance_exec_trade05"
stage = Path.home() / "cta_perp_replay_cutover_stage_20261015/exec05_leverage_cap"
manifest = json.loads((stage / "manifest.json").read_text())
binary = root / "exec-pre-trade"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def processes():
    found = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            if os.readlink(proc / "cwd") == str(root) and os.readlink(proc / "exe").removesuffix(" (deleted)") == str(binary):
                found.append(int(proc.name))
        except OSError:
            pass
    return found


assert processes() == [manifest["old_pid"]], "Exec05 pre-trade process changed"
assert digest(binary) == manifest["previous_binary_sha256"], "live binary changed"
assert digest(stage / "exec-pre-trade") == manifest["binary_sha256"], "upload checksum mismatch"
protected_paths = [root / "env.sh", root / "trade_engine.toml", *sorted((root / "config").glob("*"))]
protected = {str(p): digest(p) for p in protected_paths if p.is_file()}
old_affinity = sorted(os.sched_getaffinity(manifest["old_pid"]))
if not args.execute:
    print(json.dumps({"dry_run": True, "env": root.name, "old_pid": manifest["old_pid"], "checksum_verified": True, "affinity": old_affinity}))
    raise SystemExit(0)

backup = root / "run" / ("cta_leverage_cap_" + manifest["release_id"])
backup.parent.mkdir(mode=0o700, exist_ok=True)
backup.mkdir(mode=0o700)
shutil.copy2(binary, backup / "exec-pre-trade.before")
release_path = root / "release.json"
if release_path.exists():
    shutil.copy2(release_path, backup / "release.json.before")
shutil.copy2(stage / "manifest.json", backup / "manifest.json")
next_binary = root / (".exec-pre-trade." + manifest["release_id"] + ".next")
assert not next_binary.exists()
shutil.copy2(stage / "exec-pre-trade", next_binary)
next_binary.chmod(0o755)
assert digest(next_binary) == manifest["binary_sha256"]
os.replace(next_binary, binary)
restart_ts_us = time.time_ns() // 1000
with (backup / "restart.log").open("xb") as log:
    os.chmod(backup / "restart.log", 0o600)
    result = subprocess.run([str(root / "scripts/start_exec_pre_trade.sh")], cwd=root,
                            stdout=log, stderr=subprocess.STDOUT, timeout=240)
assert result.returncode == 0, "env-local restart failed; inspect private restart.log"
new_pid = None
for _ in range(60):
    current = processes()
    if len(current) == 1 and current[0] != manifest["old_pid"] and digest(Path("/proc") / str(current[0]) / "exe") == manifest["binary_sha256"]:
        new_pid = current[0]
        break
    time.sleep(0.5)
assert new_pid, "verified replacement process is not running"
assert protected == {str(p): digest(p) for p in protected_paths if p.is_file()}, "live configuration changed"
assert sorted(os.sched_getaffinity(new_pid)) == old_affinity, "CPU affinity changed"
release = json.loads(release_path.read_text()) if release_path.exists() else {}
release.update(pre_trade_release_id=manifest["release_id"], pre_trade_base_commit=manifest["base_commit"],
               pre_trade_binary_sha256=manifest["binary_sha256"],
               pre_trade_source_patch_sha256=manifest["source_patch_sha256"],
               pre_trade_deployed_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
temporary = root / ".release.json.leverage-cap-next"
temporary.write_text(json.dumps(release, indent=2) + "\n")
os.replace(temporary, release_path)
installed = {"release_id": manifest["release_id"], "old_pid": manifest["old_pid"], "new_pid": new_pid,
             "binary_sha256": digest(binary), "restart_ts_us": restart_ts_us,
             "affinity": old_affinity, "configuration_unchanged": True, "backup": str(backup)}
(stage / "installed.json").write_text(json.dumps(installed, indent=2) + "\n")
print(json.dumps(installed))
