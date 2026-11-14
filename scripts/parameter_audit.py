#!/usr/bin/env python3
"""Durable parameter-change journal shared by configuration services and Delist.

Only validated trading-parameter Redis keys are captured. Credentials, request
bodies and cookies are never journalled. Pending records never restore values.
"""
from __future__ import annotations

import contextlib
import functools
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import sys
import threading
import time
from urllib.parse import urlsplit
import uuid
import fcntl

_LOCAL = threading.local()
_VENUE = r"(?:binance|okex|gate|bitget|bybit)-(?:margin|futures)"
_PATHS = {
    "funding_rate": {"symbol-lists", "risk-params", "strategy-params", "funding-thresholds", "rolling-params", "spread-thresholds", "spread-thresholds/sync", "amount-u", "max-pos-u", "hedge-offset-limits", "open-offset-lower", "fr-open-spread-limit"},
    "market_making": {"symbol-list", "strategy-params", "amount-u", "hedge-offset-upper", "hedge-offset-lower", "hedge-offset-limits", "max-pos-u", "open-offset-lower", "risk-params", "model-score-rolling-params"},
}


def default_root() -> Path:
    return Path(os.environ.get("PARAMETER_AUDIT_DIR") or Path.home()/".local/state/mkt_signal/parameter_changes")


def allowed_key(family: str, environment: str, key: str) -> bool:
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", environment):
        return False
    env = re.escape(environment)
    if family == "funding_rate":
        expressions = [
            rf"{env}:fr_(?:dump_symbols|pos_dump_symbols|trade_symbols|fwd_trade_symbols|bwd_trade_symbols|unimmr_close_symbols):{_VENUE}_{_VENUE}",
            rf"{env}:funding_rate_thresholds_{_VENUE}_{_VENUE}",
            rf"{env}:{_VENUE}:{_VENUE}:(?:pre_trade_risk_params|amount_u_overrides|max_pos_u_overrides|taker_decsion_model_overrides|fr_open_spread_limit_overrides|hedge_price_offset_limits|hedge_price_offset_limit_upper|hedge_price_offset_limit_lower|open_offset_lower_overrides)",
            rf"(?:fr_strategy_params|fr_spread_threshold_mapping|fr_spread_thresholds|rolling_metrics_params)_{_VENUE}_{_VENUE}",
        ]
    elif family == "market_making":
        expressions = [
            rf"(?:mm_trade_symbols:|mm_strategy_params_){_VENUE}",
            rf"{env}:{_VENUE}:{_VENUE}:pre_trade_risk_params",
            rf"{env}:{_VENUE}:mm:(?:amount_u|max_pos_u|open_offset_lower|hedge_price_offset_limits|hedge_price_offset_limit_upper|hedge_price_offset_limit_lower)",
            r"model_score_rolling_params_[A-Za-z0-9][A-Za-z0-9._-]{0,255}",
        ]
    else:
        return False
    return any(re.fullmatch(pattern, key) for pattern in expressions)


def _text(value):
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _compound(value):
    value = _text(value)
    if value.lstrip().startswith(("{", "[")):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            pass
    return value


def redact(value):
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            name = re.sub(r"[^a-z0-9]", "", key.lower())
            sensitive = any(word in name for word in ("password", "secret", "credential", "apikey", "privatekey", "webhook")) or name in {"token", "accesstoken", "authorization", "cookie"}
            result[key] = "[redacted]" if sensitive else redact(item)
        return result
    if isinstance(value, list): return [redact(item) for item in value]
    return value


def snapshot(client, keys):
    keys = sorted(set(keys))
    pipe = client.pipeline(transaction=True)
    for key in keys:
        pipe.type(key)
    kinds = [_text(value) for value in pipe.execute()]
    pipe = client.pipeline(transaction=True)
    for key, kind in zip(keys, kinds):
        if kind == "hash":
            pipe.hgetall(key)
        elif kind in ("none", "string"):
            pipe.get(key)
        else:
            raise ValueError("unsupported parameter Redis type")
    values = pipe.execute()
    result = {}
    for key, kind, value in zip(keys, kinds, values):
        if value is None:
            kind, value = "none", None
        elif kind == "hash":
            value = {_text(k): _compound(v) for k, v in value.items()}
        else:
            value = _compound(value)
        result[key] = redact({"type": kind, "value": value})
    return result


def _atomic(path: Path, value):
    temp = path.with_name("."+path.name+"."+uuid.uuid4().hex+".tmp")
    descriptor = os.open(temp, os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(value, output, ensure_ascii=False, sort_keys=True, allow_nan=False)
            output.write("\n"); output.flush(); os.fsync(output.fileno())
        os.replace(temp, path)
        directory = os.open(path.parent, os.O_RDONLY|os.O_DIRECTORY)
        try: os.fsync(directory)
        finally: os.close(directory)
    finally:
        if temp.exists(): temp.unlink()


@contextlib.contextmanager
def journal_lock(root: Path):
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    stat = root.lstat()
    if not root.is_dir() or root.is_symlink() or stat.st_uid != os.geteuid() or stat.st_mode & 0o077:
        raise ValueError("parameter journal directory must be private and owned by service user")
    fd = os.open(root/".lock", os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN); os.close(fd)


def _writer():
    return {"pid": os.getpid(), "start_ticks": Path("/proc/self/stat").read_text().rsplit(")", 1)[1].split()[19]}


def audited_write(client, keys, execute):
    meta = getattr(_LOCAL, "request", None)
    if meta is None or not keys:
        return execute()
    keys = sorted(set(_text(key) for key in keys))
    if any(not allowed_key(meta["family"], meta["environment"], key) for key in keys):
        raise ValueError("parameter write is outside the audited configuration scope")
    root = default_root()
    with journal_lock(root):
        before = snapshot(client, keys)
        counter = root/".sequence"
        sequence = int(json.loads(counter.read_text()))+1 if counter.exists() else 1
        _atomic(counter, sequence)
        record = {**meta, "id": sequence, "time_us": time.time_ns()//1000, "storage": "redis", "writer": _writer(),
                  "objects": {key: {"before": value, "after": value} for key, value in before.items()}}
        pending = root/f"{sequence:020}.pending"
        _atomic(pending, record)

        def finish():
            after = snapshot(client, keys)
            record["objects"] = {key: {"before": before[key], "after": after[key]} for key in keys if before[key] != after[key]}
            _atomic(root/f"{sequence:020}.json", record)
            pending.unlink()

        try:
            result = execute()
        except BaseException:
            record["source"] = "failed_config_request"
            try: finish()
            except Exception: pass  # The durable pending record is retained for reconciliation.
            raise
        try:
            finish()
        except Exception as error:
            raise RuntimeError("parameter write completed but its audit acknowledgement is pending; reload before retrying") from error
        return result


def _command_keys(commands):
    result = []
    for command, _options in commands:
        name = _text(command[0]).upper()
        if name in ("SET", "HSET", "HMSET", "HDEL", "GETSET", "SETEX", "PSETEX"):
            result.append(command[1])
        elif name in ("DEL", "UNLINK"):
            result.extend(command[1:])
        elif name == "MSET":
            result.extend(command[1::2])
    return result


class AuditedPipeline:
    def __init__(self, pipeline, client):
        self.pipeline, self.client = pipeline, client

    def __enter__(self):
        self.pipeline.__enter__(); return self

    def __exit__(self, *args):
        return self.pipeline.__exit__(*args)

    def execute(self, *args, **kwargs):
        keys = _command_keys(self.pipeline.command_stack)
        return audited_write(self.client, keys, lambda: self.pipeline.execute(*args, **kwargs))

    def __getattr__(self, name):
        method = getattr(self.pipeline, name)
        if not callable(method): return method
        def call(*args, **kwargs):
            result = method(*args, **kwargs)
            return self if result is self.pipeline else result
        return call


class AuditedRedis:
    def __init__(self, client): self.client = client

    def pipeline(self, *args, **kwargs):
        return AuditedPipeline(self.client.pipeline(*args, **kwargs), self.client)

    def __getattr__(self, name):
        method = getattr(self.client, name)
        if name not in {"set", "hset", "hmset", "hdel", "delete", "unlink", "mset", "getset", "setex", "psetex"}:
            return method
        def call(*args, **kwargs):
            if name == "mset": keys = list(args[0] if args else kwargs["mapping"])
            elif name in ("delete", "unlink"): keys = list(args)
            else: keys = [args[0] if args else kwargs.get("name")]
            return audited_write(self.client, keys, lambda: method(*args, **kwargs))
        return call


def wrap_redis(client):
    return client if isinstance(client, AuditedRedis) else AuditedRedis(client)


def audit_post(family, environment):
    def decorate(function):
        @functools.wraps(function)
        def call(handler, *args, **kwargs):
            path = urlsplit(handler.path).path
            if path.removeprefix("/api/") not in _PATHS[family]:
                return function(handler, *args, **kwargs)
            old = getattr(_LOCAL, "request", None)
            peer = handler.client_address[0]
            actor = handler.headers.get("X-Parameter-Actor") if ipaddress.ip_address(peer).is_loopback else None
            source_ip = handler.headers.get("X-Real-IP") if ipaddress.ip_address(peer).is_loopback else peer
            try: source_ip = str(ipaddress.ip_address(source_ip or peer))
            except ValueError: source_ip = peer
            cookie = handler.headers.get("Cookie", "")
            _LOCAL.request = {"family": family, "environment": environment(handler), "source": "config_page",
                "actor": actor[:128] if actor else None, "source_ip": source_ip,
                "session_id": hashlib.sha256(cookie.encode()).hexdigest()[:16] if cookie else None,
                "request_path": path[:256]}
            try: return function(handler, *args, **kwargs)
            finally: _LOCAL.request = old
        return call
    return decorate


def read_batch(root, cursor):
    """Bounded read for the central collector; never imports Redis or changes it."""
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    stat = root.lstat()
    if root.is_symlink() or not root.is_dir() or stat.st_uid != os.geteuid() or stat.st_mode & 0o077:
        raise ValueError("invalid private journal directory")
    fd = os.open(root/".lock", os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW, 0o600)
    try:
        try: fcntl.flock(fd, fcntl.LOCK_SH|fcntl.LOCK_NB)
        except BlockingIOError: return {"busy": True}
        counter = root/".sequence"
        watermark = int(json.loads(counter.read_text())) if counter.exists() else 0
        paths = {}
        for path in root.iterdir():
            match = re.fullmatch(r"(\d{20})\.(json|pending)", path.name)
            if not match or int(match[1]) <= cursor: continue
            number = int(match[1])
            if match[2] == "json" or number not in paths: paths[number] = path
        records, size = [], 0
        for number, path in sorted(paths.items()):
            stat = path.lstat()
            if path.is_symlink() or not path.is_file() or stat.st_mode & 0o077 or stat.st_size > 8*1024*1024:
                raise ValueError("invalid private parameter record")
            if len(records) >= 128 or size+stat.st_size > 32*1024*1024: break
            size += stat.st_size
            event = json.loads(path.read_text())
            if event["id"] != number: raise ValueError("invalid parameter ID")
            records.append({"event": event, "pending": path.suffix == ".pending"})
        return {"watermark": watermark, "complete": len(records) == len(paths), "records": records}
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN); os.close(fd)


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "--read":
        raise SystemExit("usage: parameter_audit.py --read CURSOR")
    print(json.dumps(read_batch(default_root(), int(sys.argv[2])), ensure_ascii=False, allow_nan=False))
