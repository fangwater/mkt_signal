#!/usr/bin/env python3
"""Environment-local lifecycle for the SDK-free domestic futures target worker."""
import argparse
import datetime as dt
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

BASE = Path(__file__).resolve().parent.parent
CONFIG = BASE / "config/cta_cn_futures.json"
POSITION_FILES = ("positions.json", "positions_1min.json", "positions_5min.json", "positions_15min.json")
TARGET_FILES = ("targets.json", "targets_1min.json", "targets_5min.json", "targets_15min.json")


def environment_name(cfg):
    name = cfg["name"]
    if not re.fullmatch(r"cta-cn_futures-(?:0[1-9]|[1-9][0-9])", name):
        raise RuntimeError("native CTA name must be cta-cn_futures-01..99")
    if re.fullmatch(r"cta-cn_futures-\d{2}", BASE.name) and BASE.name != name:
        raise RuntimeError("configuration belongs to another native CTA environment")
    return name


def run(args, **kwargs):
    try:
        return subprocess.run([str(x) for x in args], cwd=BASE, check=True, **kwargs)
    except subprocess.CalledProcessError as error:
        if error.stderr:
            print(error.stderr, file=sys.stderr)
        raise


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".new")
    with temporary.open("w") as output:
        json.dump(value, output, indent=2)
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)


def product_configs(cfg, config_path):
    return {
        (config_path.parent / entry).resolve(): json.loads(
            (config_path.parent / entry).read_text())
        for entry in cfg.get("additional_products", ())
    }


def copy_snapshots(source, dest, filenames):
    for name in filenames:
        path = source / name
        if path.is_file():
            dest.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, dest / name)


def preflight(cfg, binary, config_path=None):
    products = product_configs(cfg, config_path or CONFIG)
    data = BASE / cfg["data_directory"]
    # Only the factual ledger is copied. All market inputs come from gRPC;
    # this validation never locks or changes the running generation's ledger.
    with tempfile.TemporaryDirectory(prefix="cta-cn-preflight-", dir=BASE) as temp:
        stage = Path(temp)
        copy_snapshots(data, stage, POSITION_FILES)
        for product in products.values():
            directory = Path("products") / product["symbol"]
            copy_snapshots(data / directory, stage / directory, POSITION_FILES)
        candidate = dict(cfg, data_directory=str(stage))
        if products:
            candidate["additional_products"] = [str(path) for path in products]
        save(stage / "config.json", candidate)
        result = run([binary, "--config", stage / "config.json", "--preflight"],
                     capture_output=True, text=True)
        receipt = json.loads(result.stdout)
        if (not receipt["positions_preserved"] or
                (cfg.get("enabled", True) and receipt["ready_rules"] != receipt["total_rules"])):
            raise RuntimeError("replacement preflight did not preserve a ready factual ledger")
        for product in products.values():
            result = receipt.get("products", {}).get(product["symbol"])
            if (not result or not result["positions_preserved"] or
                    (product.get("enabled", True) and result["ready_rules"] != result["total_rules"])):
                raise RuntimeError(f'preflight failed for {product["symbol"]}')
        receipt["binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
        save(data / "preflight.json", receipt)
        print(json.dumps(receipt))


def processes():
    output = run(["pm2", "jlist"], capture_output=True, text=True).stdout
    return json.loads(output[output.index("["):])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "stop", "restart", "status",
                                           "preflight"))
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--binary", type=Path, help="candidate binary, preflight only")
    parser.add_argument("--config", type=Path, help="candidate config, preflight only")
    args = parser.parse_args()
    if (args.binary or args.config) and args.action != "preflight":
        parser.error("candidate paths are supported only for preflight")
    binary = (args.binary or BASE / "cta-cn-futures").resolve()
    config_path = args.config or CONFIG
    cfg = json.loads(config_path.read_text())
    name = environment_name(cfg)
    products = product_configs(cfg, config_path)
    markets = ",".join(f'{product["exchange"]}/{product["symbol"]}'
                       for product in [cfg, *products.values()])
    if args.action != "status" and not args.execute:
        print(f"[DRY-RUN] env={BASE} symbol_scope={markets} "
              f"action={args.action} role=absolute-lot-targets")
        return
    if args.action == "preflight":
        preflight(cfg, binary, config_path)
        return
    rows = [p for p in processes() if p["name"] == name]
    for p in rows:
        if Path(p["pm2_env"]["pm_cwd"]).resolve() != BASE:
            raise RuntimeError("PM2 name belongs to another environment")
    if args.action == "status":
        print(json.dumps([dict(name=p["name"], pid=p["pid"],
                               status=p["pm2_env"]["status"]) for p in rows]))
        return
    if args.action in ("start", "restart"):
        preflight(cfg, binary, config_path)
    if args.action == "start" and any(p["pm2_env"]["status"] == "online" for p in rows):
        print("[INFO] already online; use restart to replace this generation")
        return
    if rows:
        backup = BASE / "data/stops" / dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup.mkdir(parents=True, exist_ok=True)
        for p in rows:
            for key in ("pm_out_log_path", "pm_err_log_path"):
                path = Path(p["pm2_env"][key])
                if path.is_file():
                    shutil.copy2(path, backup / path.name)
        data = BASE / cfg["data_directory"]
        files = (*POSITION_FILES, *TARGET_FILES, "manager_publication.json")
        copy_snapshots(data, backup, files)
        for product in products.values():
            directory = Path("products") / product["symbol"]
            copy_snapshots(data / directory, backup / directory, files)
    if args.action == "stop":
        if rows:
            run(["pm2", "stop", name])
    else:
        save(BASE / "data/published-binary.json", {
            "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()})
        ecology = {"apps": [{
            "name": name, "namespace": "cta-native-futures", "cwd": str(BASE),
            "script": str(BASE / "scripts/run_cta_cn_futures.sh"),
            "interpreter": "none", "instances": 1, "exec_mode": "fork",
            "autorestart": True, "restart_delay": 30000, "min_uptime": 1000,
            "max_restarts": 100000, "kill_timeout": 15000,
            "out_file": str(BASE / "logs/worker.log"),
            "error_file": str(BASE / "logs/worker.error.log"),
            "env": {"RUST_BACKTRACE": "1"},
        }]}
        save(BASE / "config/ecosystem.json", ecology)
        run(["pm2", "startOrRestart", BASE / "config/ecosystem.json", "--only", name])
    run(["pm2", "save", "--force"], stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
