"""Guarded atomic installation of the RX01 operator configuration; starts no services."""
from pathlib import Path
import datetime
import hashlib
import json
import os
import shutil
import socket
import tarfile

home = Path.home()
root = home / "binance-cta-perp-rx01"
stage = home / "deploy-staging/rx01-15m-live-20261030"
run = root / "run/operator_list_20261030"
assert socket.getfqdn() == "ip-172-31-35-228.ap-northeast-1.compute.internal"
assert (home / "spread_pbs").is_dir() and root.is_dir() and not run.exists()
manifest = json.loads((stage / "manifest.json").read_text())


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def processes():
    result = {}
    scopes = ("binance-cta-perp-", "binance_exec_trade", "exec_volatility_binance", "spread_pbs", "depth_pub")
    for p in Path("/proc").glob("[0-9]*"):
        try:
            cwd = (p / "cwd").resolve(strict=True)
            relative = cwd.relative_to(home)
            if relative.parts and relative.parts[0].startswith(scopes):
                stat = (p / "stat").read_text().rsplit(") ", 1)[1].split()
                result[p.name] = {"cwd": str(cwd), "start_ticks": stat[19]}
        except (OSError, ValueError):
            pass
    return result


before = processes()
assert not any(p["cwd"] == str(root) for p in before.values())
old = json.loads((root / "config/cta_perp.json").read_text())
assert old["enabled"] is False and not old["factor_rules"]
assert (root / "run/PENDING_OPERATOR_LIST").exists()
env_hash = digest(root / "env.sh")
with tarfile.open(stage / "bundle.tar") as bundle:
    assert set(bundle.getnames()) == set(manifest["files"])
    bundle.extractall(stage / "files", filter="data")
for name, record in manifest["files"].items():
    assert digest(stage / "files" / name) == record["sha256"], name
(run / "backup").mkdir(parents=True)
(run / "before_processes.json").write_text(json.dumps(before, indent=2) + "\n")
for name in [*manifest["files"], "release.json", "run/PENDING_OPERATOR_LIST"]:
    if (root / name).exists():
        target = run / "backup" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(root / name, target)
for name in manifest["files"]:
    target = root / name
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".rx01-20261030.new")
    assert not temporary.exists()
    shutil.copy2(stage / "files" / name, temporary)
    os.replace(temporary, target)
release = json.loads((root / "release.json").read_text())
release.update(source_commit=manifest["source_commit"], state="configured_observation", symbols=38,
               factor_count=89, position_rule_count=146, model_pair_count=145)
release["checksums"].update({k: v["sha256"] for k, v in manifest["files"].items()})
temporary = root / "release.json.rx01-20261030.new"
temporary.write_text(json.dumps(release, indent=2) + "\n")
os.replace(temporary, root / "release.json")
assert digest(root / "env.sh") == env_hash
assert processes() == before
(root / "run/PENDING_OPERATOR_LIST").rename(run / "pending_marker_before_install")
receipt = {"installed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
           "source_commit": manifest["source_commit"], "environment": root.name,
           "symbols": 38, "factors": 89, "position_rules": 146, "model_pairs": 145,
           "protected_processes_unchanged": len(before), "env_unchanged": True,
           "enabled": False, "services_started_by_installer": False,
           "sha256": {name: digest(root / name) for name in manifest["files"]}}
(run / "install_receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
print(json.dumps({k: v for k, v in receipt.items() if k != "sha256"}))
