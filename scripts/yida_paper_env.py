#!/usr/bin/env python3
"""Environment-local supervision for a read-only Yida paper account and Viz."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import time
import tomllib
import urllib.request

BASE = Path(__file__).resolve().parent.parent
COMPONENTS = ("trader", "viz")


def config(name):
    return tomllib.loads((BASE / "config" / f"{name}.toml").read_text())


def unit(component):
    return f"{BASE.name}-{component}.service"


def systemctl(*args):
    subprocess.run(["systemctl", "--user", *args], check=True)


def validate_trader():
    cfg = config("yida")
    if cfg.get("public_simulation") is not True:
        raise RuntimeError("this launcher only accepts explicit official public_simulation")
    required = [cfg[key] for key in ("user_id_env", "password_env", "app_id_env", "auth_code_env")]
    if any(not os.environ.get(key) for key in required):
        raise RuntimeError("paper credentials are missing; source this environment's private env.sh")
    return cfg


def send(command):
    path = BASE / "run" / "commands.fifo"
    info = path.lstat()
    if not stat.S_ISFIFO(info.st_mode) or info.st_uid != os.getuid():
        raise RuntimeError("invalid private command FIFO")
    fd = os.open(path, os.O_WRONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    try:
        os.write(fd, (json.dumps({"op": command}) + "\n").encode())
    finally:
        os.close(fd)


def install():
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", BASE.name) or not re.fullmatch(r"[/a-zA-Z0-9_.-]+", str(BASE)):
        raise RuntimeError("unsupported environment path")
    for name in ("logs", "run", "data", "data/stops"):
        (BASE / name).mkdir(mode=0o700, parents=True, exist_ok=True)
    directory = Path.home() / ".config/systemd/user"
    directory.mkdir(parents=True, exist_ok=True)
    for component in COMPONENTS:
        extra = f"ExecStop=/usr/bin/python3 {BASE}/scripts/yida_paper_env.py drain\n" if component == "trader" else "CPUAffinity=3\n"
        content = (
            f"[Unit]\nDescription={BASE.name} read-only paper {component}\n"
            "After=network-online.target\nWants=network-online.target\n\n[Service]\nType=simple\n"
            f"WorkingDirectory={BASE}\nExecStart=/usr/bin/bash {BASE}/scripts/yida_paper_env.sh run-{component}\n"
            f"{extra}Restart={'no' if component == 'trader' else 'on-failure'}\nRestartSec=3\n"
            "TimeoutStopSec=35\nKillMode=control-group\nUMask=0077\nLimitNOFILE=65536\n"
            f"StandardOutput=append:{BASE}/logs/{component}.log\nStandardError=append:{BASE}/logs/{component}.log\n"
            "\n[Install]\nWantedBy=default.target\n"
        )
        path = directory / unit(component)
        if path.exists() and path.read_text() != content:
            raise RuntimeError(f"existing unit differs: {path}; preserve operator configuration")
        path.write_text(content)
    systemctl("daemon-reload")
    systemctl("enable", *(unit(c) for c in COMPONENTS))


def run_trader():
    validate_trader()
    run = BASE / "run"
    run.mkdir(mode=0o700, exist_ok=True)
    path = run / "commands.fifo"
    if not path.exists():
        os.mkfifo(path, mode=0o600)
    info = path.lstat()
    if not stat.S_ISFIFO(info.st_mode) or info.st_uid != os.getuid():
        raise RuntimeError("invalid private command FIFO")
    fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
    os.dup2(fd, 0)
    if fd != 0:
        os.close(fd)
    (run / "trader.pid").write_text(str(os.getpid()))
    # No --execute: the environment cannot place/cancel any orders.
    os.execv(BASE / "bin/yida-trader", [str(BASE / "bin/yida-trader"), "--config", str(BASE / "config/yida.toml"), "--connect"])


def drain():
    try:
        pid = int((BASE / "run/trader.pid").read_text())
    except (FileNotFoundError, ValueError):
        return
    executable = BASE / "bin/yida-trader"
    def running():
        try:
            return Path(os.readlink(f"/proc/{pid}/exe")) == executable
        except (OSError, ValueError):
            return False
    if not running():
        return
    send("shutdown")
    deadline = time.monotonic() + 25
    while running() and time.monotonic() < deadline:
        time.sleep(0.1)
    if running():
        raise RuntimeError("paper runtime did not finish graceful shutdown")


def status():
    result = {"environment": BASE.name, "mode": "paper_read_only", "services": {}}
    for component in COMPONENTS:
        proc = subprocess.run(["systemctl", "--user", "show", unit(component), "--property=ActiveState,SubState,MainPID,ExecMainStatus"], capture_output=True, text=True, check=True)
        result["services"][component] = dict(line.split("=", 1) for line in proc.stdout.splitlines() if "=" in line)
    http = config("viz")["http"]
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{http['port']}/healthz", timeout=3) as response:
            result["observation"] = json.load(response)["sources"]
    except (OSError, ValueError, KeyError) as error:
        result["viz_error"] = type(error).__name__
    print(json.dumps(result, ensure_ascii=False, indent=2))


def main():
    os.umask(0o077)
    os.chdir(BASE)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["install", "start", "stop", "status", "state", "drain", "run-trader", "run-viz"])
    parser.add_argument("component", choices=["all", *COMPONENTS], nargs="?", default="all")
    args = parser.parse_args()
    selected = COMPONENTS if args.component == "all" else (args.component,)
    if args.action == "install":
        install()
    elif args.action == "run-trader":
        run_trader()
    elif args.action == "run-viz":
        cfg = config("yida")
        for key in ("user_id_env", "password_env", "app_id_env", "auth_code_env"):
            os.environ.pop(cfg[key], None)
        os.execv(BASE / "bin/cn-futures-viz", [str(BASE / "bin/cn-futures-viz"), "--config", str(BASE / "config/viz.toml")])
    elif args.action == "start":
        if "trader" in selected:
            validate_trader()
        for component in reversed(selected):
            systemctl("start", unit(component))
    elif args.action == "stop":
        archive = BASE / "data/stops" / time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        archive.mkdir(mode=0o700, parents=True, exist_ok=True)
        for component in selected:
            log = BASE / "logs" / f"{component}.log"
            if log.is_file():
                shutil.copy2(log, archive / log.name)
            systemctl("stop", unit(component))
    elif args.action == "state":
        send("state")
        print("read-only state request queued; response remains in private runtime logs")
    elif args.action == "drain":
        drain()
    else:
        status()


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        raise SystemExit(str(error)) from None
