#!/usr/bin/env python3
"""
Web dashboard for OKX/Bybit market-making and intra-exchange arbitrage processes.

The server reads process manager state from pmdaemon and PM2, then inspects recent
stderr/stdout log tails to explain the most likely current issue. PM2 output is
sanitized before it reaches the API; environment variables are never returned.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 6338
DEFAULT_POLL_SECS = 8
DEFAULT_LOG_BYTES = 96 * 1024
DEFAULT_LOG_LINES = 120
SUPPORTED_EXCHANGES = {"okex", "okx", "bybit"}
TARGET_ORDER = {"mm": 0, "intra": 1}


@dataclass(frozen=True)
class Target:
    dir_name: str
    base_dir: str
    kind: str
    exchange: str
    env_tag: str

    @property
    def display_name(self) -> str:
        kind_name = "做市" if self.kind == "mm" else "期现套利"
        return f"{self.exchange.upper()} {kind_name} {self.env_tag}"


@dataclass(frozen=True)
class ExpectedProcess:
    target: Target
    manager: str
    name: str
    role: str
    role_label: str
    namespace: Optional[str] = None
    required: bool = True


@dataclass
class CommandResult:
    ok: bool
    stdout: str = ""
    stderr: str = ""
    returncode: int = 0
    error: Optional[str] = None


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def normalize_exchange(exchange: str) -> str:
    exchange = exchange.lower()
    return "okex" if exchange == "okx" else exchange


def sanitize_tag(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def parse_target_dir(path_or_name: str, home: Path) -> Optional[Target]:
    raw = Path(path_or_name)
    dir_name = raw.name
    base_dir = str(raw if raw.is_absolute() else home / dir_name)
    lower = dir_name.lower()

    mm_match = re.match(r"^([a-z0-9]+)[-_]mm(?:[_-](.+))?$", lower)
    if mm_match:
        exchange = normalize_exchange(mm_match.group(1))
        if exchange not in {"okex", "bybit"}:
            return None
        env_tag = sanitize_tag(mm_match.group(2) or "mm")
        return Target(dir_name=dir_name, base_dir=base_dir, kind="mm", exchange=exchange, env_tag=env_tag)

    intra_match = re.match(r"^([a-z0-9]+)[-_]intra(?:[-_](.+))?$", lower)
    if intra_match:
        exchange = normalize_exchange(intra_match.group(1))
        if exchange not in {"okex", "bybit"}:
            return None
        env_tag = sanitize_tag(intra_match.group(2) or "intra")
        return Target(dir_name=dir_name, base_dir=base_dir, kind="intra", exchange=exchange, env_tag=env_tag)

    return None


def discover_targets(home: Path, requested: Iterable[str]) -> List[Target]:
    targets: Dict[Tuple[str, str, str], Target] = {}

    requested = list(requested)
    if requested:
        for item in requested:
            target = parse_target_dir(item, home)
            if target:
                targets[(target.kind, target.exchange, target.env_tag)] = target
        return sort_targets(targets.values())

    if home.exists():
        for child in home.iterdir():
            if not child.is_dir():
                continue
            target = parse_target_dir(child.name, home)
            if target:
                targets[(target.kind, target.exchange, target.env_tag)] = target

    if not targets:
        for name in ("okex_mm_alpha", "bybit_mm_alpha", "okex-intra-arb01", "bybit-intra-arb01"):
            target = parse_target_dir(name, home)
            if target:
                targets[(target.kind, target.exchange, target.env_tag)] = target

    return sort_targets(targets.values())


def sort_targets(targets: Iterable[Target]) -> List[Target]:
    return sorted(targets, key=lambda t: (TARGET_ORDER.get(t.kind, 9), t.exchange, t.env_tag, t.dir_name))


def expected_processes(targets: Iterable[Target]) -> List[ExpectedProcess]:
    processes: List[ExpectedProcess] = []
    role_labels = {
        "am": "账户监控",
        "te": "交易引擎",
        "pt": "下单前置",
        "pm": "持久化",
        "viz": "可视化",
        "signal": "信号服务",
        "cfg": "参数服务",
    }

    for target in targets:
        prefix = "mm" if target.kind == "mm" else "intra"
        for role in ("am", "te", "pt", "pm", "viz"):
            processes.append(
                ExpectedProcess(
                    target=target,
                    manager="pmdaemon",
                    name=f"{prefix}_{role}_{target.exchange}_{target.env_tag}",
                    role=role,
                    role_label=role_labels[role],
                )
            )

        if target.kind == "mm":
            signal_name = f"mm_{target.exchange}_futures_{target.env_tag}_trade_signal"
            cfg_name = f"mm_cfg_{target.exchange}_{target.env_tag}"
        else:
            signal_name = f"intra_{target.exchange}_{target.env_tag}_trade_signal"
            cfg_name = f"intra_config_server_{target.dir_name}"

        processes.append(
            ExpectedProcess(
                target=target,
                manager="pm2",
                name=signal_name,
                namespace=target.dir_name,
                role="signal",
                role_label=role_labels["signal"],
            )
        )
        processes.append(
            ExpectedProcess(
                target=target,
                manager="pm2",
                name=cfg_name,
                namespace=target.dir_name,
                role="cfg",
                role_label=role_labels["cfg"],
            )
        )

    return processes


def run_command(args: List[str], timeout: int = 6) -> CommandResult:
    try:
        proc = subprocess.run(
            args,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        return CommandResult(ok=False, returncode=127, error=str(exc))
    except subprocess.TimeoutExpired:
        return CommandResult(ok=False, returncode=124, error=f"command timed out after {timeout}s")
    except Exception as exc:  # noqa: BLE001 - surface manager failures to the UI.
        return CommandResult(ok=False, returncode=1, error=str(exc))

    return CommandResult(
        ok=proc.returncode == 0,
        stdout=proc.stdout,
        stderr=proc.stderr,
        returncode=proc.returncode,
        error=None if proc.returncode == 0 else (proc.stderr.strip() or proc.stdout.strip() or f"exit {proc.returncode}"),
    )


def parse_int(value: str) -> Optional[int]:
    value = value.strip()
    if not value or value in {"-", "None"}:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_float(value: str) -> Optional[float]:
    value = value.strip().rstrip("%")
    if not value or value in {"-", "None"}:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def load_pmdaemon_list() -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]], Optional[str]]:
    result = run_command(["pmdaemon", "list"], timeout=8)
    if not result.ok:
        return {}, [], result.error or "pmdaemon list failed"

    processes: Dict[str, Dict[str, Any]] = {}
    raw_rows: List[Dict[str, Any]] = []
    for line in result.stdout.splitlines():
        if "┆" not in line:
            continue
        parts = [part.strip() for part in line.strip().strip("│").split("┆")]
        if len(parts) < 9 or parts[0] in {"ID", ""}:
            continue
        name = parts[1]
        if name == "Name":
            continue
        row = {
            "id": parts[0],
            "name": name,
            "status": parts[2].lower(),
            "pid": parse_int(parts[3]),
            "uptime": parts[4] if parts[4] and parts[4] != "-" else None,
            "restarts": parse_int(parts[5]),
            "cpu": parse_float(parts[6]),
            "memory": parts[7] if parts[7] and parts[7] != "-" else None,
            "port": parts[8] if parts[8] and parts[8] != "-" else None,
        }
        raw_rows.append(row)
        if "..." not in name:
            processes[name] = row
    return processes, raw_rows, None


def parse_pmdaemon_info(text: str, name: str) -> Dict[str, Any]:
    def search(pattern: str, flags: int = 0) -> Optional[str]:
        match = re.search(pattern, text, flags)
        return match.group(1).strip() if match else None

    state = search(r"state:\s*([A-Za-z]+),") or "unknown"
    pid = search(r"pid:\s*Some\(\s*(\d+)\s*,?\s*\)", re.S)
    restarts = search(r"restarts:\s*(\d+),")
    cpu = search(r"cpu_usage:\s*([0-9.]+),")
    memory = search(r"memory_usage:\s*(\d+),")
    exit_code = search(r"exit_code:\s*Some\(\s*(-?\d+)\s*\)", re.S)
    error = search(r"error:\s*Some\((.*?)\),\s*namespace:", re.S)

    return {
        "name": name,
        "status": state.lower(),
        "pid": int(pid) if pid else None,
        "uptime": None,
        "restarts": int(restarts) if restarts else None,
        "cpu": float(cpu) if cpu else None,
        "memory": format_bytes(int(memory)) if memory and int(memory) > 0 else None,
        "port": None,
        "exit_code": int(exit_code) if exit_code else None,
        "manager_error": redact_text(error) if error else None,
    }


def load_pmdaemon_info(name: str) -> Dict[str, Any]:
    result = run_command(["pmdaemon", "info", name], timeout=6)
    if result.ok:
        return parse_pmdaemon_info(result.stdout, name)

    combined = f"{result.stdout}\n{result.stderr}\n{result.error or ''}"
    if "ProcessNotFound" in combined:
        return {
            "name": name,
            "status": "missing",
            "pid": None,
            "uptime": None,
            "restarts": None,
            "cpu": None,
            "memory": None,
            "port": None,
            "manager_error": "pmdaemon 中未找到该进程配置",
        }

    return {
        "name": name,
        "status": "unknown",
        "pid": None,
        "uptime": None,
        "restarts": None,
        "cpu": None,
        "memory": None,
        "port": None,
        "manager_error": redact_text(result.error or "pmdaemon info failed"),
    }


def load_pm2_processes() -> Tuple[Dict[Tuple[str, str], Dict[str, Any]], Optional[str]]:
    result = run_command(["npx", "pm2", "jlist"], timeout=10)
    if not result.ok:
        return {}, result.error or "pm2 jlist failed"

    try:
        apps = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        return {}, f"pm2 jlist JSON 解析失败: {exc}"

    processes: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for app in apps:
        if not isinstance(app, dict):
            continue
        name = str(app.get("name") or "")
        if not name:
            continue
        pm2_env = app.get("pm2_env") or {}
        monit = app.get("monit") or {}
        namespace = str(pm2_env.get("namespace") or "default")
        pm_uptime = pm2_env.get("pm_uptime")
        status = {
            "name": name,
            "namespace": namespace,
            "status": str(pm2_env.get("status") or "unknown").lower(),
            "pid": app.get("pid") if isinstance(app.get("pid"), int) else None,
            "uptime": format_epoch_age(pm_uptime),
            "restarts": pm2_env.get("restart_time") if isinstance(pm2_env.get("restart_time"), int) else None,
            "cpu": monit.get("cpu") if isinstance(monit.get("cpu"), (int, float)) else None,
            "memory": format_bytes(monit.get("memory")) if isinstance(monit.get("memory"), int) else None,
            "port": None,
            "pm_err_log_path": str(pm2_env.get("pm_err_log_path") or ""),
            "pm_out_log_path": str(pm2_env.get("pm_out_log_path") or ""),
            "manager_error": None,
        }
        processes[(namespace, name)] = status
    return processes, None


def format_epoch_age(value: Any) -> Optional[str]:
    if not isinstance(value, (int, float)) or value <= 0:
        return None
    age = max(0, int(time.time() - (value / 1000.0)))
    return format_duration(age)


def format_duration(seconds: Optional[int]) -> Optional[str]:
    if seconds is None:
        return None
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    if hours < 48:
        return f"{hours}h"
    return f"{hours // 24}d"


def format_bytes(value: Optional[int]) -> Optional[str]:
    if value is None:
        return None
    size = float(value)
    units = ("B", "KB", "MB", "GB")
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)}{unit}"
            return f"{size:.1f}{unit}"
        size /= 1024
    return None


SECRET_PATTERNS = [
    re.compile(r"(?i)(api[_-]?key|api[_-]?secret|passphrase|password|passwd|token|signature|secret)([\"']?\s*[:=]\s*[\"']?)([^,\s\"']+)"),
    re.compile(r"(?i)(authorization|x-mbx-apikey)([\"']?\s*[:=]\s*[\"']?)([^,\s\"']+)"),
]


def redact_text(text: Optional[str]) -> str:
    if not text:
        return ""
    text = re.sub(r"\x1b\[[0-9;]*m", "", str(text))
    for pattern in SECRET_PATTERNS:
        text = pattern.sub(lambda match: f"{match.group(1)}{match.group(2)}***", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > 320:
        text = text[:317] + "..."
    return text


LOG_CLASSIFIERS = [
    ("fatal", "warn", re.compile(r"(?i)\b(panic|panicked|fatal|segmentation fault|assertion failed)\b"), "近期出现崩溃/致命错误"),
    ("auth", "warn", re.compile(r"(?i)\b(auth|login|credential|permission|unauthori[sz]ed|api key|passphrase|signature)\b.*\b(fail|error|invalid|denied|expired)\b"), "近期出现认证或权限错误"),
    ("redis", "warn", re.compile(r"(?i)\b(redis|valkey)\b.*\b(error|fail|refused|timeout|noauth|denied)\b"), "近期 Redis 连接或认证异常"),
    ("ws", "warn", re.compile(r"(?i)\b(websocket|web socket|\bws\b|connection)\b.*\b(disconnect|closed|reset|refused|timeout|reconnect|broken pipe)\b"), "近期 WebSocket/网络连接异常"),
    ("balance", "info", re.compile(r"(?i)(insufficient balance|not enough balance|margin insufficient|51008)"), "近期交易所拒单: 余额或保证金不足"),
    ("rate_limit", "info", re.compile(r"(?i)(rate limit|too many requests|50011|50013|system[s]? are busy|systems are busy)"), "近期交易所限频或系统繁忙"),
    ("cancel_not_found", "info", re.compile(r"(?i)(order not exists|order does not exist|too late to cancel|filled/canceled/not exist|51400|110001|170213|order update not found)"), "近期撤单/订单回报已过期，不等同于进程断开"),
    ("reject", "info", re.compile(r"(?i)(reject|rejected|cancel failed|order failed|request failed|exchange error)"), "近期交易所返回业务错误"),
    ("generic_error", "info", re.compile(r"(?i)\b(error|warn|failed|failure|timeout)\b"), "近期有错误或告警日志"),
]


def tail_text(path: str, max_bytes: int) -> str:
    if not path:
        return ""
    try:
        stat = os.stat(path)
        with open(path, "rb") as handle:
            if stat.st_size > max_bytes:
                handle.seek(-max_bytes, os.SEEK_END)
            data = handle.read()
    except OSError:
        return ""
    return data.decode("utf-8", errors="replace")


def candidate_log_paths(process: ExpectedProcess, pm2_status: Optional[Dict[str, Any]] = None) -> List[str]:
    if process.manager == "pmdaemon":
        log_dir = Path.home() / ".pmdaemon" / "logs"
        return [str(log_dir / f"{process.name}-error.log"), str(log_dir / f"{process.name}-out.log")]

    paths: List[str] = []
    if pm2_status:
        for key in ("pm_err_log_path", "pm_out_log_path"):
            value = pm2_status.get(key)
            if isinstance(value, str) and value:
                paths.append(value)
    if not paths:
        log_name = process.name.replace("_", "-")
        paths = [
            str(Path.home() / ".pm2" / "logs" / f"{log_name}-error.log"),
            str(Path.home() / ".pm2" / "logs" / f"{log_name}-out.log"),
        ]
    return paths


def inspect_logs(paths: List[str], max_bytes: int, max_lines: int) -> Dict[str, Any]:
    existing: List[Tuple[str, float]] = []
    all_lines: List[str] = []
    for path in paths:
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            continue
        existing.append((path, mtime))
        text = tail_text(path, max_bytes)
        if text:
            all_lines.extend(text.splitlines()[-max_lines:])

    newest_mtime = max((mtime for _, mtime in existing), default=None)
    event = detect_log_event(all_lines)
    return {
        "paths": [path for path, _ in existing],
        "last_mtime": datetime.fromtimestamp(newest_mtime).astimezone().isoformat(timespec="seconds") if newest_mtime else None,
        "last_age_secs": int(time.time() - newest_mtime) if newest_mtime else None,
        "last_age": format_duration(int(time.time() - newest_mtime)) if newest_mtime else None,
        "event": event,
    }


def detect_log_event(lines: List[str]) -> Optional[Dict[str, str]]:
    for raw_line in reversed(lines):
        line = redact_text(raw_line)
        if not line:
            continue
        for category, severity, pattern, label in LOG_CLASSIFIERS:
            if pattern.search(line):
                return {
                    "category": category,
                    "severity": severity,
                    "label": label,
                    "line": line,
                }
    return None


def status_health(status: str) -> str:
    if status == "online":
        return "ok"
    if status in {"missing", "stopped", "errored", "error", "failed", "crashed"}:
        return "down"
    return "unknown"


def reason_for(process: ExpectedProcess, status: Dict[str, Any], log_info: Dict[str, Any], manager_error: Optional[str]) -> str:
    proc_status = str(status.get("status") or "unknown").lower()
    event = log_info.get("event")

    if manager_error and proc_status == "unknown":
        return manager_error
    if proc_status == "missing":
        return f"{process.manager} 中未找到该进程；可能未部署、未启动或进程名变更"
    if proc_status != "online":
        if manager_error:
            return f"管理器状态为 {proc_status}: {manager_error}"
        if event:
            return f"管理器状态为 {proc_status}; {event['label']}: {event['line']}"
        return f"管理器状态为 {proc_status}"
    if event:
        return f"{event['label']}: {event['line']}"
    return "运行中"


def build_process_row(
    process: ExpectedProcess,
    manager_status: Dict[str, Any],
    log_info: Dict[str, Any],
    manager_error: Optional[str],
) -> Dict[str, Any]:
    proc_status = str(manager_status.get("status") or "unknown").lower()
    health = status_health(proc_status)
    event = log_info.get("event")
    if health == "ok" and event and event.get("severity") == "warn":
        health = "warn"

    return {
        "group": process.target.display_name,
        "dir_name": process.target.dir_name,
        "kind": process.target.kind,
        "exchange": process.target.exchange,
        "env_tag": process.target.env_tag,
        "manager": process.manager,
        "namespace": process.namespace,
        "name": process.name,
        "role": process.role,
        "role_label": process.role_label,
        "required": process.required,
        "status": proc_status,
        "health": health,
        "pid": manager_status.get("pid"),
        "uptime": manager_status.get("uptime"),
        "restarts": manager_status.get("restarts"),
        "cpu": manager_status.get("cpu"),
        "memory": manager_status.get("memory"),
        "port": manager_status.get("port"),
        "reason": reason_for(process, manager_status, log_info, manager_error),
        "last_log_at": log_info.get("last_mtime"),
        "last_log_age": log_info.get("last_age"),
        "log_event": event,
        "log_paths": log_info.get("paths", []),
    }


def make_missing_status(name: str, manager_error: Optional[str] = None) -> Dict[str, Any]:
    return {
        "name": name,
        "status": "missing" if not manager_error else "unknown",
        "pid": None,
        "uptime": None,
        "restarts": None,
        "cpu": None,
        "memory": None,
        "port": None,
        "manager_error": manager_error,
    }


def build_extra_rows(
    raw_pmdaemon_rows: List[Dict[str, Any]],
    expected_names: set[str],
    max_bytes: int,
    max_lines: int,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in raw_pmdaemon_rows:
        name = str(item.get("name") or "")
        status = str(item.get("status") or "").lower()
        if not name or status == "online" or name in expected_names:
            continue
        if not re.search(r"(mm_|intra_).*(okex|bybit)", name):
            continue

        fake_target = Target(dir_name="extra", base_dir="", kind="extra", exchange="", env_tag="")
        fake_process = ExpectedProcess(
            target=fake_target,
            manager="pmdaemon",
            name=name,
            role="extra",
            role_label="额外进程",
            required=False,
        )
        log_info = inspect_logs(candidate_log_paths(fake_process), max_bytes, max_lines)
        row = build_process_row(fake_process, item, log_info, item.get("manager_error"))
        row["group"] = "额外/兼容进程"
        row["dir_name"] = "extra"
        row["reason"] = "不在当前预期清单中；如果不是兼容旧进程，请检查部署脚本或进程命名"
        if row["health"] == "down":
            row["health"] = "warn"
        rows.append(row)
    return rows


def build_status(config: argparse.Namespace) -> Dict[str, Any]:
    targets = discover_targets(Path(config.home), config.target)
    expected = expected_processes(targets)
    expected_names = {proc.name for proc in expected}

    pmdaemon_processes, raw_pmdaemon_rows, pmdaemon_error = load_pmdaemon_list()
    pm2_processes, pm2_error = load_pm2_processes()

    rows: List[Dict[str, Any]] = []
    for process in expected:
        manager_error: Optional[str] = None
        if process.manager == "pmdaemon":
            status = pmdaemon_processes.get(process.name)
            if status is None and not pmdaemon_error:
                status = load_pmdaemon_info(process.name)
            elif pmdaemon_error:
                status = make_missing_status(process.name, pmdaemon_error)
            manager_error = status.get("manager_error") if status else None
            if status is None:
                status = make_missing_status(process.name)
        else:
            status = pm2_processes.get((process.namespace or "default", process.name))
            if status is None:
                same_name = [item for (namespace, name), item in pm2_processes.items() if name == process.name]
                status = same_name[0] if same_name else make_missing_status(process.name, pm2_error)
            manager_error = pm2_error if status.get("status") == "unknown" else status.get("manager_error")

        logs = inspect_logs(candidate_log_paths(process, status), config.log_bytes, config.log_lines)
        rows.append(build_process_row(process, status, logs, manager_error))

    if config.include_extra:
        rows.extend(build_extra_rows(raw_pmdaemon_rows, expected_names, config.log_bytes, config.log_lines))

    groups: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        group = row["group"]
        groups.setdefault(group, {"name": group, "health": "ok", "down": 0, "warn": 0, "unknown": 0, "ok": 0, "rows": []})
        groups[group]["rows"].append(row)
        health = row["health"]
        groups[group][health] = groups[group].get(health, 0) + 1
        groups[group]["health"] = merge_health(groups[group]["health"], health)

    summary = {"ok": 0, "warn": 0, "down": 0, "unknown": 0, "total": len(rows)}
    for row in rows:
        summary[row["health"]] = summary.get(row["health"], 0) + 1

    return {
        "generated_at": now_iso(),
        "poll_secs": config.poll_secs,
        "home": str(config.home),
        "summary": summary,
        "targets": [target.__dict__ for target in targets],
        "groups": list(groups.values()),
        "rows": rows,
        "manager_errors": {
            "pmdaemon": pmdaemon_error,
            "pm2": pm2_error,
        },
    }


def merge_health(left: str, right: str) -> str:
    order = {"ok": 0, "unknown": 1, "warn": 2, "down": 3}
    return right if order.get(right, 0) > order.get(left, 0) else left


INDEX_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>进程监控</title>
  <style>
    :root {
      --bg: #f6f7f9;
      --panel: #ffffff;
      --panel-soft: #f0f3f5;
      --text: #172026;
      --muted: #66727d;
      --line: #d7dde3;
      --ok: #16794f;
      --ok-bg: #e8f5ef;
      --warn: #a66300;
      --warn-bg: #fff4db;
      --down: #c73636;
      --down-bg: #fdeaea;
      --unknown: #596579;
      --unknown-bg: #edf0f4;
      --accent: #285c7d;
      --radius: 8px;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--text);
      background: var(--bg);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Noto Sans SC", sans-serif;
      font-size: 14px;
    }
    .wrap {
      max-width: 1480px;
      margin: 0 auto;
      padding: 18px;
    }
    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 14px;
    }
    h1 {
      margin: 0;
      font-size: 22px;
      font-weight: 700;
      letter-spacing: 0;
    }
    .muted { color: var(--muted); }
    .small { font-size: 12px; }
    .actions {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }
    button {
      min-height: 34px;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: var(--panel);
      color: var(--text);
      padding: 0 12px;
      cursor: pointer;
      font: inherit;
    }
    button.active {
      border-color: var(--accent);
      background: #e8f1f7;
      color: #17445f;
    }
    .summary {
      display: grid;
      grid-template-columns: repeat(4, minmax(120px, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }
    .metric {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      padding: 12px;
      min-height: 74px;
    }
    .metric strong {
      display: block;
      font-size: 28px;
      line-height: 1.1;
      letter-spacing: 0;
    }
    .metric span {
      display: block;
      margin-top: 5px;
      color: var(--muted);
      font-size: 12px;
    }
    .groups {
      display: grid;
      gap: 12px;
    }
    .group {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      overflow: hidden;
    }
    .group-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      padding: 12px 14px;
      background: var(--panel-soft);
      border-bottom: 1px solid var(--line);
    }
    .group-title {
      display: flex;
      align-items: center;
      gap: 10px;
      min-width: 0;
      font-weight: 700;
    }
    .group-counts {
      color: var(--muted);
      font-size: 12px;
      white-space: nowrap;
    }
    .table-wrap { overflow-x: auto; }
    table {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
    }
    th, td {
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
      overflow-wrap: anywhere;
    }
    th {
      color: var(--muted);
      background: #fbfcfd;
      font-size: 12px;
      font-weight: 600;
      white-space: nowrap;
    }
    tr:last-child td { border-bottom: 0; }
    .col-status { width: 86px; }
    .col-role { width: 92px; }
    .col-manager { width: 92px; }
    .col-pid { width: 88px; }
    .col-restarts { width: 72px; }
    .col-usage { width: 116px; }
    .col-log { width: 130px; }
    .badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 58px;
      min-height: 24px;
      padding: 3px 8px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      white-space: nowrap;
    }
    .badge.ok { color: var(--ok); background: var(--ok-bg); }
    .badge.warn { color: var(--warn); background: var(--warn-bg); }
    .badge.down { color: var(--down); background: var(--down-bg); }
    .badge.unknown { color: var(--unknown); background: var(--unknown-bg); }
    .proc {
      font-family: "SFMono-Regular", Consolas, monospace;
      font-size: 12px;
      line-height: 1.45;
    }
    .reason {
      color: #28343d;
      line-height: 1.45;
    }
    .empty {
      padding: 24px;
      text-align: center;
      color: var(--muted);
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
    }
    .manager-error {
      margin-bottom: 12px;
      padding: 10px 12px;
      border-radius: var(--radius);
      border: 1px solid var(--down);
      background: var(--down-bg);
      color: var(--down);
      display: none;
    }
    @media (max-width: 900px) {
      .topbar { align-items: flex-start; flex-direction: column; }
      .summary { grid-template-columns: repeat(2, minmax(120px, 1fr)); }
      table { min-width: 980px; }
    }
  </style>
</head>
<body>
  <main class="wrap">
    <div class="topbar">
      <div>
        <h1>进程监控</h1>
        <div class="muted small" id="meta">加载中</div>
      </div>
      <div class="actions">
        <button type="button" data-filter="all" class="active">全部</button>
        <button type="button" data-filter="bad">异常</button>
        <button type="button" data-filter="pmdaemon">pmdaemon</button>
        <button type="button" data-filter="pm2">PM2</button>
        <button type="button" id="refresh">刷新</button>
      </div>
    </div>
    <div class="manager-error" id="manager-error"></div>
    <section class="summary">
      <div class="metric"><strong id="down-count">0</strong><span>断开/缺失</span></div>
      <div class="metric"><strong id="warn-count">0</strong><span>告警</span></div>
      <div class="metric"><strong id="unknown-count">0</strong><span>未知</span></div>
      <div class="metric"><strong id="ok-count">0</strong><span>正常</span></div>
    </section>
    <section class="groups" id="groups"></section>
  </main>
  <script>
    const state = { filter: "all", data: null, timer: null };
    const statusLabel = { ok: "正常", warn: "告警", down: "断开", unknown: "未知" };

    function esc(value) {
      return String(value ?? "").replace(/[&<>"']/g, ch => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
      }[ch]));
    }

    function valueOrDash(value) {
      return value === null || value === undefined || value === "" ? "-" : esc(value);
    }

    function rowVisible(row) {
      if (state.filter === "all") return true;
      if (state.filter === "bad") return row.health !== "ok";
      return row.manager === state.filter;
    }

    async function loadStatus() {
      const res = await fetch("/api/process-status", { cache: "no-store" });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      state.data = await res.json();
      render();
      clearTimeout(state.timer);
      state.timer = setTimeout(loadStatus, Math.max(2000, (state.data.poll_secs || 8) * 1000));
    }

    function render() {
      const data = state.data;
      if (!data) return;
      document.getElementById("down-count").textContent = data.summary.down || 0;
      document.getElementById("warn-count").textContent = data.summary.warn || 0;
      document.getElementById("unknown-count").textContent = data.summary.unknown || 0;
      document.getElementById("ok-count").textContent = data.summary.ok || 0;
      document.getElementById("meta").textContent =
        `${data.generated_at} · ${data.summary.total} 个进程 · ${data.home}`;

      const managerErrors = Object.entries(data.manager_errors || {})
        .filter(([, value]) => value)
        .map(([key, value]) => `${key}: ${value}`);
      const managerError = document.getElementById("manager-error");
      if (managerErrors.length) {
        managerError.style.display = "block";
        managerError.textContent = managerErrors.join(" | ");
      } else {
        managerError.style.display = "none";
      }

      const groupsEl = document.getElementById("groups");
      const html = (data.groups || []).map(group => renderGroup(group)).filter(Boolean).join("");
      groupsEl.innerHTML = html || `<div class="empty">当前过滤条件下没有进程</div>`;
    }

    function renderGroup(group) {
      const rows = (group.rows || []).filter(rowVisible);
      if (!rows.length) return "";
      const counts = `断开 ${group.down || 0} · 告警 ${group.warn || 0} · 未知 ${group.unknown || 0} · 正常 ${group.ok || 0}`;
      return `
        <article class="group">
          <div class="group-head">
            <div class="group-title">
              <span class="badge ${esc(group.health)}">${statusLabel[group.health] || group.health}</span>
              <span>${esc(group.name)}</span>
            </div>
            <div class="group-counts">${esc(counts)}</div>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th class="col-status">状态</th>
                  <th>进程</th>
                  <th class="col-role">角色</th>
                  <th class="col-manager">管理器</th>
                  <th class="col-pid">PID</th>
                  <th class="col-restarts">重启</th>
                  <th class="col-usage">资源</th>
                  <th class="col-log">最近日志</th>
                  <th>原因</th>
                </tr>
              </thead>
              <tbody>${rows.map(renderRow).join("")}</tbody>
            </table>
          </div>
        </article>
      `;
    }

    function renderRow(row) {
      const usage = [
        row.cpu === null || row.cpu === undefined ? null : `CPU ${row.cpu}%`,
        row.memory ? `MEM ${row.memory}` : null
      ].filter(Boolean).join("<br>") || "-";
      const manager = row.namespace ? `${row.manager}<br><span class="muted small">${esc(row.namespace)}</span>` : esc(row.manager);
      const uptime = row.uptime ? `<br><span class="muted small">${esc(row.uptime)}</span>` : "";
      const logAge = row.last_log_age ? `${esc(row.last_log_age)}前` : "-";
      return `
        <tr>
          <td><span class="badge ${esc(row.health)}">${statusLabel[row.health] || row.health}</span></td>
          <td><div class="proc">${esc(row.name)}</div></td>
          <td>${esc(row.role_label)}</td>
          <td>${manager}</td>
          <td>${valueOrDash(row.pid)}${uptime}</td>
          <td>${valueOrDash(row.restarts)}</td>
          <td>${usage}</td>
          <td>${logAge}</td>
          <td><div class="reason">${esc(row.reason)}</div></td>
        </tr>
      `;
    }

    document.querySelectorAll("button[data-filter]").forEach(btn => {
      btn.addEventListener("click", () => {
        state.filter = btn.dataset.filter;
        document.querySelectorAll("button[data-filter]").forEach(item => item.classList.toggle("active", item === btn));
        render();
      });
    });
    document.getElementById("refresh").addEventListener("click", () => {
      clearTimeout(state.timer);
      loadStatus().catch(showLoadError);
    });

    function showLoadError(err) {
      const error = document.getElementById("manager-error");
      error.style.display = "block";
      error.textContent = `监控接口读取失败: ${err.message}`;
      clearTimeout(state.timer);
      state.timer = setTimeout(() => loadStatus().catch(showLoadError), 5000);
    }

    loadStatus().catch(showLoadError);
  </script>
</body>
</html>
"""


def make_handler(config: argparse.Namespace):
    class ProcessStatusHandler(BaseHTTPRequestHandler):
        cache_data: Optional[Dict[str, Any]] = None
        cache_ts: float = 0.0

        def do_GET(self) -> None:  # noqa: N802 - stdlib handler API.
            parsed = urlparse(self.path)
            if parsed.path == "/healthz":
                self.send_json({"ok": True, "generated_at": now_iso()})
                return
            if parsed.path == "/api/process-status":
                self.send_json(self.cached_status())
                return
            if parsed.path in {"/", "/index.html"}:
                body = INDEX_HTML.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_error(404)

        def cached_status(self) -> Dict[str, Any]:
            now = time.time()
            if self.__class__.cache_data is None or now - self.__class__.cache_ts >= config.poll_secs:
                self.__class__.cache_data = build_status(config)
                self.__class__.cache_ts = now
            return self.__class__.cache_data

        def send_json(self, payload: Dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args: Any) -> None:
            print(f"{now_iso()} {self.address_string()} {fmt % args}")

    return ProcessStatusHandler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process status dashboard for OKX/Bybit MM and intra-arb.")
    parser.add_argument("--host", default=os.getenv("HOST", DEFAULT_HOST))
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", str(DEFAULT_PORT))))
    parser.add_argument("--home", default=os.getenv("MONITOR_HOME", str(Path.home())))
    parser.add_argument("--poll-secs", type=int, default=int(os.getenv("POLL_SECS", str(DEFAULT_POLL_SECS))))
    parser.add_argument("--log-bytes", type=int, default=int(os.getenv("LOG_BYTES", str(DEFAULT_LOG_BYTES))))
    parser.add_argument("--log-lines", type=int, default=int(os.getenv("LOG_LINES", str(DEFAULT_LOG_LINES))))
    parser.add_argument(
        "--target",
        action="append",
        default=[],
        help="Deployment directory name or absolute path. Repeat to override auto-discovery.",
    )
    parser.add_argument("--include-extra", action="store_true", help="Show non-expected stopped pmdaemon items.")
    args = parser.parse_args()
    args.home = str(Path(args.home).expanduser())
    args.poll_secs = max(2, args.poll_secs)
    args.log_bytes = max(4096, args.log_bytes)
    args.log_lines = max(20, args.log_lines)
    return args


def main() -> int:
    args = parse_args()
    server = ThreadingHTTPServer((args.host, args.port), make_handler(args))
    print(f"{now_iso()} process_status_server listening on http://{args.host}:{args.port}")
    print(f"{now_iso()} targets: {', '.join(target.dir_name for target in discover_targets(Path(args.home), args.target))}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"{now_iso()} shutting down")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
