"""Publish RX01's new authenticated route and Manager configuration link only."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile

home = Path.home()
stage = home / "deploy-staging/rx01-web-20261030"
run = home / "binance_exec_trade06/run/restore_audit_20261030/web"
manager = home / "crypto_cta_manager"
site = Path("/etc/nginx/sites-available/crypto_proxy_4191.conf")
locations = home / "nginx_locations.txt"
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--execute", action="store_true")
args = parser.parse_args()
manifest = json.loads((stage / "manifest.json").read_text())
for name, digest in manifest["files"].items():
    assert hashlib.sha256((stage / name).read_bytes()).hexdigest() == digest
before = site.read_text()
assert "/etc/nginx/snippets/cta_perp_rx01.conf" not in before
reference = "    include /etc/nginx/snippets/cta_perp_rx03.conf;"
assert before.count(reference) == 1
candidate = before.replace(reference, reference + "\n    include /etc/nginx/snippets/cta_perp_rx01.conf;")
assert "cta_rx01_retired.conf" in candidate
map_before = locations.read_text()
map_candidate = map_before + "\n# BEGIN managed: cta perp rx01\n/cta/binance-cta-perp-rx01/ external:/etc/nginx/snippets/cta_perp_rx01.conf\n# END managed: cta perp rx01\n"
assert "/cta/binance-cta-perp-rx01/" not in map_before
webroot_before = os.readlink(manager / "webroot")
assert webroot_before == "web-releases/rx01-retired-20261030", "Manager frontend changed; inspect first"
if args.execute:
    assert not run.exists()
    run.mkdir()
    shutil.copy2(site, run / "nginx_site.before")
    shutil.copy2(locations, run / "nginx_locations.before")
    (run / "manager_webroot.before").write_text(webroot_before)
    release = manager / "web-releases/rx01-perp-20261030"
    assert not release.exists()
    release.mkdir()
    with tarfile.open(stage / "frontend.tar") as bundle:
        bundle.extractall(release, filter="data")
    for name, digest in manifest["frontend_files"].items():
        assert hashlib.sha256((release / name).read_bytes()).hexdigest() == digest
    command = lambda *cmd: subprocess.run(cmd, check=True)
    command("sudo", "-n", "install", "-m", "644", str(stage / "cta_perp_rx01.conf"), "/etc/nginx/snippets/cta_perp_rx01.conf")
    (stage / "nginx_site.candidate").write_text(candidate)
    assert site.read_text() == before
    command("sudo", "-n", "install", "-m", "644", str(stage / "nginx_site.candidate"), str(site) + ".rx01-next")
    command("sudo", "-n", "mv", str(site) + ".rx01-next", str(site))
    try:
        command("sudo", "-n", "nginx", "-t")
    except BaseException:
        command("sudo", "-n", "install", "-m", "644", str(run / "nginx_site.before"), str(site))
        raise
    temporary = locations.with_suffix(".rx01-next")
    temporary.write_text(map_candidate)
    temporary.chmod(0o600)
    os.replace(temporary, locations)
    command("sudo", "-n", "systemctl", "reload", "nginx")
    assert os.readlink(manager / "webroot") == webroot_before
    temporary = manager / "webroot.rx01-next"
    temporary.symlink_to("web-releases/rx01-perp-20261030")
    os.replace(temporary, manager / "webroot")
receipt = {"execute": args.execute, "route": "/cta/binance-cta-perp-rx01/", "config_port": 19195,
           "dashboard_port": 10195, "auth_source": "binance_exec_trade06", "frontend_commit": manifest["frontend_commit"],
           "old_spread_routes": "remain 410", "manager_backend_replaced": False}
if args.execute:
    (run / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
print(json.dumps(receipt))
