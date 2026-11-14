"""One-time guarded publish of the disabled RX01 framework; starts no services."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket

home = Path.home()
root = home / "binance-cta-perp-rx01"
stage = home / "deploy-staging/rx01-depth-20261030"
run = root / "run/depth_factors_20261030"
manifest = json.loads((stage / "manifest.json").read_text())
assert socket.getfqdn() == "ip-172-31-35-228.ap-northeast-1.compute.internal"
assert (home / "spread_pbs").is_dir() and root.is_dir()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def processes():
    observed = {}
    scopes = ("binance-cta-perp-", "binance_exec_trade", "exec_volatility_binance",
              "spread_pbs", "depth_pub")
    for entry in Path("/proc").glob("[0-9]*"):
        try:
            cwd = str((entry / "cwd").resolve(strict=True))
            relative = Path(cwd).relative_to(home)
            if not relative.parts or not relative.parts[0].startswith(scopes):
                continue
            stat = (entry / "stat").read_text().rsplit(") ", 1)[1].split()
            observed[entry.name] = {"cwd": cwd, "start_ticks": stat[19]}
        except (OSError, ValueError):
            continue
    return observed


config = json.loads((root / "config/cta_perp.json").read_text())
assert config["enabled"] is False and config["factor_rules"] == []
assert json.loads((root / "config/cta_perp_symbol_factors.json").read_text()) == {}
assert (root / "run/PENDING_OPERATOR_LIST").exists()
before = processes()
assert not any(value["cwd"].startswith(str(root)) for value in before.values())
for name, expected in manifest["protected_hashes"].items():
    assert digest(root / name) == expected, f"Operator state changed: {name}"
for name, metadata in manifest["files"].items():
    assert digest(stage / name) == metadata["sha256"], f"Upload checksum: {name}"
    assert digest(root / name) == metadata["expected_previous_sha256"], f"Remote file changed: {name}"

assert not run.exists(), "This publish has already been attempted; inspect its receipt"
(run / "backup").mkdir(parents=True)
(run / "before_processes.json").write_text(json.dumps(before, indent=2) + "\n")
for name in [*manifest["files"], "release.json"]:
    backup = run / "backup" / name
    backup.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(root / name, backup)
for name, metadata in manifest["files"].items():
    target = root / name
    temporary = target.with_name(target.name + ".depth-20261030.new")
    assert not temporary.exists()
    shutil.copy2(stage / name, temporary)
    temporary.chmod(target.stat().st_mode & 0o777)
    assert digest(temporary) == metadata["sha256"]
    os.replace(temporary, target)

release = json.loads((root / "release.json").read_text())
release["source_commit"] = manifest["source_commit"]
release["depth_factor_count"] = 23
release["checksums"].update({name: value["sha256"] for name, value in manifest["files"].items()})
temporary = root / "release.json.depth-20261030.new"
temporary.write_text(json.dumps(release, indent=2) + "\n")
os.replace(temporary, root / "release.json")
for name, expected in manifest["protected_hashes"].items():
    assert digest(root / name) == expected, f"Protected state changed: {name}"
after = processes()
assert after == before, "Protected process set changed during publish"
receipt = {"source_commit": manifest["source_commit"], "environment": root.name,
           "installed_sha256": {name: digest(root / name) for name in manifest["files"]},
           "protected_config_unchanged": True, "protected_processes_unchanged": len(before),
           "enabled": False, "runtime_services_started": False, "operator_list_empty": True}
(run / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
print(json.dumps(receipt))
