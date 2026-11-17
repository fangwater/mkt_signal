#!/usr/bin/env python3
"""Environment-local lifecycle for the SDK-free SHFE/SN target worker."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

BASE = Path(__file__).resolve().parent.parent
CONFIG = BASE / "config/cta_cn_futures.json"
NAME = "cta-cn_futures-01"


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


def preflight(cfg, binary):
    data = BASE / cfg["data_directory"]
    # Only the factual ledger is copied. All market inputs come from gRPC;
    # this validation never locks or changes the running generation's ledger.
    with tempfile.TemporaryDirectory(prefix="cta-cn-preflight-", dir=BASE) as temp:
        stage = Path(temp)
        source = data / "positions.json"
        if source.is_file():
            shutil.copy2(source, stage / source.name)
        save(stage / "config.json", dict(cfg, data_directory=str(stage)))
        result = run([binary, "--config", stage / "config.json", "--preflight"],
                     capture_output=True, text=True)
        receipt = json.loads(result.stdout)
        if not receipt["positions_preserved"] or receipt["ready_rules"] != receipt["total_rules"]:
            raise RuntimeError("replacement preflight did not preserve a ready factual ledger")
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
    cfg = json.loads((args.config or CONFIG).read_text())
    if (cfg["name"], cfg["exchange"], cfg["symbol"]) != (NAME, "SHFE", "SN"):
        raise RuntimeError("this lifecycle owns only cta-cn_futures-01 / SHFE/SN")
    if args.action != "status" and not args.execute:
        print(f"[DRY-RUN] env={BASE} exchange=SHFE symbol_scope=SN "
              f"action={args.action} role=absolute-lot-targets")
        return
    if args.action == "preflight":
        preflight(cfg, binary)
        return
    rows = [p for p in processes() if p["name"] == NAME]
    for p in rows:
        if Path(p["pm2_env"]["pm_cwd"]).resolve() != BASE:
            raise RuntimeError("PM2 name belongs to another environment")
    if args.action == "status":
        print(json.dumps([dict(name=p["name"], pid=p["pid"],
                               status=p["pm2_env"]["status"]) for p in rows]))
        return
    if args.action in ("start", "restart"):
        preflight(cfg, binary)
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
        for name in ("positions.json", "targets.json"):
            source = BASE / cfg["data_directory"] / name
            if source.exists():
                shutil.copy2(source, backup / name)
    if args.action == "stop":
        if rows:
            run(["pm2", "stop", NAME])
    else:
        save(BASE / "data/published-binary.json", {
            "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()})
        ecology = {"apps": [{
            "name": NAME, "namespace": "cta-native-futures", "cwd": str(BASE),
            "script": str(BASE / "scripts/run_cta_cn_futures.sh"),
            "interpreter": "none", "instances": 1, "exec_mode": "fork",
            "autorestart": True, "restart_delay": 30000, "min_uptime": 1000,
            "max_restarts": 100000, "kill_timeout": 15000,
            "out_file": str(BASE / "logs/worker.log"),
            "error_file": str(BASE / "logs/worker.error.log"),
            "env": {"RUST_BACKTRACE": "1"},
        }]}
        save(BASE / "config/ecosystem.json", ecology)
        run(["pm2", "startOrRestart", BASE / "config/ecosystem.json", "--only", NAME])
    run(["pm2", "save", "--force"], stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
