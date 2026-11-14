"""Verify and install RX01's locally built publisher and native baseline seed.

Dry-run by default. Stops/starts only RX01 observation services through local
wrappers. The account Exec remains stopped and Manager publication disabled.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile

home = Path.home()
root = home / "binance-cta-perp-rx01"
stage = home / "deploy-staging/rx01-seed-20261030"
run = root / "run/operator_list_20261030"
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--execute", action="store_true")
args = parser.parse_args()
manifest = json.loads((stage / "release.json").read_text())
sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
for name, digest in manifest["files"].items():
    assert sha(stage / name) == digest, name
config = json.loads((root / "config/cta_perp.json").read_text())
assert config["enabled"] is False and len(config["factor_rules"]) == 146
assert "export CTA_MANAGER_API_URL" not in (root / "env.sh").read_text()
with tarfile.open(stage / "baseline_seed.tar") as bundle:
    expected = {r["标的"] + ".seed" for r in config["factor_rules"]} | {"manifest.json"}
    assert set(bundle.getnames()) == expected
    bundle.extractall(stage / "seed", filter="data")
seed_manifest = json.loads((stage / "seed/manifest.json").read_text())
assert len(seed_manifest["symbols"]) == 38
for symbol, record in seed_manifest["symbols"].items():
    assert record["bars"] == 672 and record["late_trades"] == 0
    assert sha(stage / "seed" / (symbol + ".seed")) == record["sha256"]
assert not (run / "before_seed").exists(), "seed already installed"
if args.execute:
    os.environ["PATH"] = str(home / ".local/bin") + ":" + os.environ["PATH"]
    def wrapper(name):
        subprocess.run([str(root / "scripts" / name)], cwd=root, check=True)
    wrapper("stop_cta_perp_15m_signal.sh")
    wrapper("stop_cta_perp_factor_model_15m_pub.sh")
    backup = run / "before_seed"
    backup.mkdir()
    for name in ("cta_perp_factor_model_15m_pub", "config/cta_perp_factor_model_15m_pub.toml", "env.sh", "release.json"):
        dest = backup / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(root / name, dest)
    for name in ("run/cta_perp_position_flags.json", "run/cta_perp_status.json", "data/cta_perp_15m/factor_history"):
        path = root / name
        if path.exists():
            dest = backup / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            path.rename(dest)
    target = root / "data/cta_perp_15m/baseline_seed"
    assert not target.exists()
    (stage / "seed").rename(target)
    for name in ("cta_perp_factor_model_15m_pub", "cta_perp_factor_model_15m_pub.toml"):
        dest = root / ("config/" + name if name.endswith(".toml") else name)
        temp = dest.with_name(dest.name + ".rx01-next")
        shutil.copy2(stage / name, temp)
        os.replace(temp, dest)
    env = root / "env.sh"
    text, count = re.subn(r"(?m)^export CTA_PERP_CONFIG_PORT=['\"]?19185['\"]?$", "export CTA_PERP_CONFIG_PORT=19195", env.read_text())
    assert count == 1
    temp = root / "env.sh.rx01-next"
    temp.write_text(text)
    temp.chmod(0o600)
    os.replace(temp, env)
    release = json.loads((root / "release.json").read_text())
    release.update(source_commit=manifest["source_commit"], state="seeded_observation", baseline_seed_end=seed_manifest["end"])
    release["checksums"].update({"cta_perp_factor_model_15m_pub": manifest["files"]["cta_perp_factor_model_15m_pub"]})
    (root / "release.json").write_text(json.dumps(release, indent=2) + "\n")
    wrapper("start_cta_perp_factor_model_15m_pub.sh")
    wrapper("stop_cta_perp_config_server.sh")
    wrapper("start_cta_perp_config_server.sh")
    # Start the 15m signal separately after formal factor readiness succeeds.
receipt = {"execute": args.execute, "installed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
           "source_commit": manifest["source_commit"], "symbols": 38, "seed_bars_per_symbol": 672,
           "seed_end": seed_manifest["end"], "config_port": 19195, "enabled": False, "signal_started": False,
           "publisher_sha256": manifest["files"]["cta_perp_factor_model_15m_pub"]}
if args.execute:
    (run / "seed_install_receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
print(json.dumps(receipt))
