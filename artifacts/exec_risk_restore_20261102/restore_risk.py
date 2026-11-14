#!/usr/bin/env python3
"""Restore three absent Exec risk hashes from their current process startup logs.

Default is read-only. --execute uses WATCH/MULTI to create all three keys only
if they are still absent and each original process and startup log is unchanged.
"""
import argparse
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
import time

import redis

ROOT = pathlib.Path.home()
NAMES = ("binance_exec_trade06", "binance_exec_trade07", "binance_exec_trade08")
EXPECTED = {
    "max_pending_limit_orders": "10",
    "exec_max_pending_limit_buy_orders": "10",
    "exec_max_pending_limit_sell_orders": "10",
    "exec_order_rate_limit_per_min": "400",
    "exec_order_rate_limit_10s": "300",
}


def running_process(env):
    expected_exe = (ROOT / env / "exec-pre-trade").resolve(strict=True)
    expected_cwd = (ROOT / env).resolve(strict=True)
    found = []
    for proc in pathlib.Path("/proc").glob("[0-9]*"):
        try:
            if pathlib.Path(os.readlink(proc / "exe")) != expected_exe:
                continue
            if pathlib.Path(os.readlink(proc / "cwd")) != expected_cwd:
                continue
            cmd = (proc / "cmdline").read_bytes().split(b"\0")
            if b"--venue" not in cmd or b"binance-futures" not in cmd:
                continue
            log = pathlib.Path(os.readlink(proc / "fd/1"))
            expected_parent = (ROOT / ".pmdaemon/logs").resolve(strict=True)
            if log.parent.resolve(strict=True) != expected_parent:
                raise RuntimeError(f"{env}: stdout is not a pmdaemon log")
            if not log.is_file() or log.stat().st_size > 1024 * 1024:
                raise RuntimeError(f"{env}: startup log missing or exceeds 1 MiB")
            data = log.read_bytes()
            values = {}
            for line in data.decode("utf-8", "strict").splitlines():
                parts = line.split()
                if len(parts) == 2 and parts[0] in EXPECTED:
                    values[parts[0]] = parts[1]
            if values != EXPECTED:
                raise RuntimeError(f"{env}: startup values do not match the user's chosen values")
            pid = int(proc.name)
            started = subprocess.check_output(["ps", "-o", "lstart=", "-p", str(pid)], text=True).strip()
            if not started:
                raise RuntimeError(f"{env}: process disappeared")
            found.append({"env": env, "pid": pid, "started": started, "stdout_sha256": hashlib.sha256(data).hexdigest(), "values": values})
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    if len(found) != 1:
        raise RuntimeError(f"{env}: expected exactly one running Exec pre-trade process; found {len(found)}")
    return found[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--audit-dir", required=True)
    args = parser.parse_args()
    audit = pathlib.Path(args.audit_dir)
    if not audit.is_absolute() or audit.exists():
        raise RuntimeError("audit directory must be a new absolute path")
    keys = [f"{env}:binance-futures:pre_trade_risk_params" for env in NAMES]
    db = redis.Redis(host="127.0.0.1", port=6379, db=0, socket_timeout=5, socket_connect_timeout=5, decode_responses=True)
    processes = [running_process(env) for env in NAMES]
    if any(db.exists(key) for key in keys):
        raise RuntimeError("one or more risk keys already exist; no write attempted")
    if not args.execute:
        print(json.dumps({"mode":"read-only", "redis":"127.0.0.1:6379/0", "keys_absent":keys, "processes":processes}, ensure_ascii=False))
        return
    audit.mkdir(mode=0o700)
    preflight = {"redis":"127.0.0.1:6379/0", "created_at_utc":time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "keys_absent":keys, "processes":processes}
    fd = os.open(audit / "preflight.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(preflight, f, ensure_ascii=False, indent=2); f.flush(); os.fsync(f.fileno())
    with db.pipeline() as pipe:
        pipe.watch(*keys)
        if any(pipe.exists(key) for key in keys):
            raise RuntimeError("a risk key appeared during preflight; no write attempted")
        if [running_process(env) for env in NAMES] != processes:
            raise RuntimeError("an Exec process or startup log changed; no write attempted")
        pipe.multi()
        for key in keys:
            pipe.hset(key, mapping=EXPECTED)
        written = pipe.execute()
    if written != [len(EXPECTED)] * len(keys):
        raise RuntimeError(f"unexpected transaction result: {written!r}")
    current = {key: db.hgetall(key) for key in keys}
    if any(value != EXPECTED for value in current.values()):
        raise RuntimeError("read-back mismatch; inspect audit and Redis immediately")
    receipt = {"completed_at_utc":time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "written_fields":current, "unchanged_processes":[running_process(env) for env in NAMES]}
    fd = os.open(audit / "receipt.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(receipt, f, ensure_ascii=False, indent=2); f.flush(); os.fsync(f.fileno())
    print(json.dumps({"mode":"executed", "verified_keys":keys, "fields":EXPECTED, "audit_dir":str(audit)}, ensure_ascii=False))

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
