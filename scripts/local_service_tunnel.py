#!/usr/bin/env python3
"""Persistent reverse SSH TCP tunnels, with one supervised connection per service."""
import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import subprocess
import time
import tomllib

BASE = Path(__file__).resolve().parent.parent


def config():
    cfg = tomllib.loads((BASE / "config/tunnel.toml").read_text())
    if set(cfg) != {"ssh_config", "peer_alias", "forwards"}:
        raise RuntimeError("expected ssh_config, peer_alias and named forwards")
    ssh_config = Path(cfg["ssh_config"])
    if not ssh_config.is_absolute() or not ssh_config.is_file():
        raise RuntimeError("private SSH config is missing")
    if ssh_config.stat().st_mode & 0o077:
        raise RuntimeError("private SSH config must not be group/world accessible")
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", cfg["peer_alias"]):
        raise RuntimeError("invalid SSH peer alias")
    ports, names = set(), set()
    if not cfg.get("forwards"):
        raise RuntimeError("at least one explicit TCP forward is required")
    for forward in cfg["forwards"]:
        if set(forward) != {"name", "local_host", "local_port", "remote_port"}:
            raise RuntimeError("each forward needs name, local_host, local_port and remote_port")
        name = forward["name"]
        if not re.fullmatch(r"[a-z][a-z0-9-]{0,31}", name) or name in names:
            raise RuntimeError("service names must be unique lower-case names, at most 32 characters")
        host = ipaddress.IPv4Address(forward["local_host"])
        if not (host.is_private or host.is_loopback) or host.is_unspecified or host.is_multicast:
            raise RuntimeError("local_host must be an explicit private or loopback IPv4 address")
        for key in ("local_port", "remote_port"):
            if type(forward[key]) is not int or not 1024 <= forward[key] <= 65535:
                raise RuntimeError("forward ports must be integers in 1024..65535")
        if forward["remote_port"] in ports:
            raise RuntimeError("remote listen ports must be unique")
        ports.add(forward["remote_port"])
        names.add(name)
    return cfg


def select(cfg, name):
    if name is None:
        return cfg["forwards"]
    forwards = [f for f in cfg["forwards"] if f["name"] == name]
    if not forwards:
        raise RuntimeError("service is not in this environment's forward list")
    return forwards


def unit(name):
    return f"{BASE.name}@{name}.service"


def systemctl(*args):
    subprocess.run(["systemctl", "--user", *args], check=True)


def properties(name):
    result = subprocess.run(["systemctl", "--user", "show", unit(name),
                             "--property=ActiveState,SubState,MainPID,NRestarts,ExecMainStatus"],
                            capture_output=True, text=True, check=True)
    return dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)


def install(cfg):
    if not re.fullmatch(r"[/a-zA-Z0-9_.-]+", str(BASE)):
        raise RuntimeError("unsupported environment path")
    (BASE / "logs").mkdir(mode=0o700, exist_ok=True)
    units = Path.home() / ".config/systemd/user"
    units.mkdir(parents=True, exist_ok=True)
    content = (
        f"[Unit]\nDescription={BASE.name} TCP service tunnel %i\n"
        "After=network-online.target\nWants=network-online.target\nStartLimitIntervalSec=0\n\n"
        "[Service]\nType=simple\n"
        f"WorkingDirectory={BASE}\nExecStart=/usr/bin/bash {BASE}/scripts/local_service_tunnel.sh run --service %i\n"
        "Restart=always\nRestartSec=5\nTimeoutStopSec=15\nKillMode=control-group\n"
        "UMask=0077\nNoNewPrivileges=true\n"
        f"StandardOutput=append:{BASE}/logs/%i.log\n"
        f"StandardError=append:{BASE}/logs/%i.log\n\n"
        "[Install]\nWantedBy=default.target\n"
    )
    path = units / f"{BASE.name}@.service"
    if path.exists() and path.read_text() != content:
        raise RuntimeError("existing tunnel unit differs; preserve operator changes")
    path.write_text(content)
    systemctl("daemon-reload")
    systemctl("enable", *(unit(f["name"]) for f in cfg["forwards"]))


def ssh_args(cfg, forward):
    destination = f"127.0.0.1:{forward['remote_port']}:{forward['local_host']}:{forward['local_port']}"
    return ["/usr/bin/ssh", "-F", cfg["ssh_config"], "-N", "-T",
            "-o", "BatchMode=yes", "-o", "ExitOnForwardFailure=yes",
            "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
            "-o", "ConnectTimeout=10", "-o", "TCPKeepAlive=yes",
            "-o", "ControlMaster=no", "-o", "ControlPath=none",
            "-R", destination, cfg["peer_alias"]]


def check_reconnect(cfg, name):
    before = properties(name)
    siblings = {f["name"]: properties(f["name"])["MainPID"]
                for f in cfg["forwards"] if f["name"] != name}
    if before.get("ActiveState") != "active" or before.get("MainPID") == "0":
        raise RuntimeError("tunnel must be active before the reconnect check")
    systemctl("kill", "--kill-whom=main", "--signal=KILL", unit(name))
    deadline = time.monotonic() + 25
    while time.monotonic() < deadline:
        after = properties(name)
        if (after.get("ActiveState") == "active"
                and after.get("MainPID") not in ("0", before["MainPID"])
                and int(after.get("NRestarts", "0")) > int(before.get("NRestarts", "0"))):
            if any(properties(other)["MainPID"] != pid for other, pid in siblings.items()):
                raise RuntimeError("another service unexpectedly restarted")
            print(json.dumps({"automatic_restart_verified": True, "name": name,
                              "other_service_pids_unchanged": True, "service": after}, indent=2))
            return
        time.sleep(0.5)
    raise RuntimeError("tunnel did not automatically recover within 25 seconds")


def main():
    os.umask(0o077)
    os.chdir(BASE)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("install", "start", "stop", "restart", "status", "run", "check-reconnect"))
    parser.add_argument("--service", help="one named service; omit for all on start/stop/restart/status")
    args = parser.parse_args()
    cfg = config()
    forwards = select(cfg, args.service)
    if args.action in ("run", "check-reconnect") and args.service is None:
        raise RuntimeError("this action requires --service NAME")
    if args.action == "install":
        install(cfg)
    elif args.action == "run":
        command = ssh_args(cfg, forwards[0])
        os.execv(command[0], command)
    elif args.action == "check-reconnect":
        check_reconnect(cfg, args.service)
    elif args.action in ("start", "stop", "restart"):
        systemctl(args.action, *(unit(f["name"]) for f in forwards))
    else:
        print(json.dumps({"remote_bind": "127.0.0.1", "services": [
            {**f, "unit": unit(f["name"]), "service": properties(f["name"])} for f in forwards
        ]}, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, KeyError, ValueError, TypeError, subprocess.CalledProcessError) as error:
        raise SystemExit(str(error)) from None
