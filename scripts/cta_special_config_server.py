#!/usr/bin/env python3
"""Standalone file-backed configuration server for disposable CTA special envs."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

MAX_BODY_BYTES = 64 * 1024
RULE_NAMES = {"tp_vpi_018", "baseline_104"}
SYMBOL_RE = re.compile(r"^[A-Z0-9][A-Z0-9._-]{0,31}$")

TOP_FIELDS = {"enabled", "rule_name", "symbols", "entry", "execution"}
ENTRY_FIELDS = {"nq_change_enabled"}
EXECUTION_FIELDS = {
    "order_notional_usdt",
    "factor_exit_quantile_long", "factor_exit_quantile_short",
    "trailing_stop_trigger_step", "trailing_stop_move_step",
}


def _object(raw: Any, field: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"{field} must be an object")
    return raw


def _reject_unknown(raw: dict[str, Any], allowed: set[str], field: str) -> None:
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ValueError(f"{field} contains unknown fields: {', '.join(unknown)}")


def _bool(raw: Any, field: str) -> bool:
    if not isinstance(raw, bool):
        raise ValueError(f"{field} must be a boolean")
    return raw


def _number(raw: Any, field: str) -> float:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(f"{field} must be a number")
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError(f"{field} must be finite")
    return value


def normalize_config(raw: Any) -> dict[str, Any]:
    root = _object(raw, "config")
    _reject_unknown(root, TOP_FIELDS, "config")
    for required in ("rule_name", "symbols", "execution"):
        if required not in root:
            raise ValueError(f"config.{required} is required")

    rule_name = str(root["rule_name"]).strip().lower()
    if rule_name not in RULE_NAMES:
        raise ValueError("rule_name must be tp_vpi_018 or baseline_104")

    symbols_raw = root["symbols"]
    if not isinstance(symbols_raw, list):
        raise ValueError("symbols must be an array")
    symbols = sorted({str(symbol).strip().upper() for symbol in symbols_raw})
    if not symbols or any(not SYMBOL_RE.fullmatch(symbol) for symbol in symbols):
        raise ValueError("symbols must contain valid non-empty symbols of at most 32 bytes")

    entry = _object(root.get("entry", {}), "entry")
    _reject_unknown(entry, ENTRY_FIELDS, "entry")

    execution = _object(root["execution"], "execution")
    _reject_unknown(execution, EXECUTION_FIELDS, "execution")
    for required in (
        "factor_exit_quantile_long", "factor_exit_quantile_short",
        "trailing_stop_trigger_step", "trailing_stop_move_step",
    ):
        if required not in execution:
            raise ValueError(f"execution.{required} is required")
    notional = _number(
        execution.get("order_notional_usdt", 100.0), "execution.order_notional_usdt"
    )
    if notional <= 0:
        raise ValueError("execution.order_notional_usdt must be positive")
    exit_long = _number(
        execution["factor_exit_quantile_long"], "execution.factor_exit_quantile_long"
    )
    exit_short = _number(
        execution["factor_exit_quantile_short"], "execution.factor_exit_quantile_short"
    )
    if not 0.0 <= exit_long < 0.9:
        raise ValueError("execution.factor_exit_quantile_long must be in [0, 0.9)")
    if not 0.1 < exit_short <= 1.0:
        raise ValueError("execution.factor_exit_quantile_short must be in (0.1, 1]")
    trigger = _number(
        execution["trailing_stop_trigger_step"], "execution.trailing_stop_trigger_step"
    )
    move = _number(
        execution["trailing_stop_move_step"], "execution.trailing_stop_move_step"
    )
    if not (trigger > 0.0 and 0.0 < move < trigger):
        raise ValueError("trailing move must be positive and below trigger")

    return {
        "enabled": _bool(root.get("enabled", False), "enabled"),
        "rule_name": rule_name,
        "symbols": symbols,
        "entry": {
            "nq_change_enabled": _bool(
                entry.get("nq_change_enabled", True), "entry.nq_change_enabled"
            ),
        },
        "execution": {
            "order_notional_usdt": notional,
            "factor_exit_quantile_long": exit_long,
            "factor_exit_quantile_short": exit_short,
            "trailing_stop_trigger_step": trigger,
            "trailing_stop_move_step": move,
        },
    }


class ConfigConflict(ValueError):
    pass


class ConfigStore:
    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()

    @staticmethod
    def _revision(config: dict[str, Any]) -> str:
        canonical = json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(canonical).hexdigest()

    def load(self) -> tuple[dict[str, Any], str]:
        with self._lock:
            return self._load_unlocked()

    def _load_unlocked(self) -> tuple[dict[str, Any], str]:
        config = normalize_config(json.loads(self.path.read_text(encoding="utf-8")))
        return config, self._revision(config)

    def save(self, raw: Any, expected_revision: str) -> tuple[dict[str, Any], str]:
        config = normalize_config(raw)
        with self._lock:
            current, current_revision = self._load_unlocked()
            if not expected_revision or expected_revision != current_revision:
                raise ConfigConflict("configuration changed; reload before saving")
            immutable_paths = (
                ("rule_name",),
            )
            for path in immutable_paths:
                old_value: Any = current
                new_value: Any = config
                for key in path:
                    old_value = old_value[key]
                    new_value = new_value[key]
                if old_value != new_value:
                    raise ValueError(
                        f"{'.'.join(path)} is immutable for a running environment; redeploy or restart with an edited file"
                    )
            payload = (json.dumps(config, indent=2, ensure_ascii=True) + "\n").encode()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd, temp_name = tempfile.mkstemp(prefix=f".{self.path.name}.", dir=self.path.parent)
            try:
                if self.path.exists():
                    os.fchmod(fd, self.path.stat().st_mode & 0o777)
                with os.fdopen(fd, "wb") as output:
                    output.write(payload)
                    output.flush()
                    os.fsync(output.fileno())
                os.replace(temp_name, self.path)
                directory_fd = os.open(self.path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            except Exception:
                try:
                    os.close(fd)
                except OSError:
                    pass
                try:
                    os.unlink(temp_name)
                except FileNotFoundError:
                    pass
                raise
            return config, self._revision(config)


def make_handler(store: ConfigStore, index_path: Path):
    class Handler(BaseHTTPRequestHandler):
        server_version = "cta-special-config"

        def log_message(self, message: str, *args: Any) -> None:
            print(f"[cta-special-config] {self.address_string()} {message % args}")

        def _send(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, status: int, value: Any) -> None:
            self._send(
                status,
                json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode(),
                "application/json; charset=utf-8",
            )

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            try:
                if path == "/api/healthz":
                    config, revision = store.load()
                    self._json(200, {"ok": True, "enabled": config["enabled"], "revision": revision})
                    return
                if path == "/api/config":
                    config, revision = store.load()
                    self._json(200, {"config": config, "revision": revision})
                    return
                if path in {"/", "/index.html"}:
                    self._send(200, index_path.read_bytes(), "text/html; charset=utf-8")
                    return
                self._json(404, {"error": "not found"})
            except Exception as exc:
                self._json(500, {"error": str(exc)})

        def do_PUT(self) -> None:  # noqa: N802
            if urlparse(self.path).path != "/api/config":
                self._json(404, {"error": "not found"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0 or length > MAX_BODY_BYTES:
                    raise ValueError("invalid request body size")
                request = _object(json.loads(self.rfile.read(length)), "request")
                _reject_unknown(request, {"config", "expected_revision"}, "request")
                config, revision = store.save(
                    request.get("config"), str(request.get("expected_revision", ""))
                )
                self._json(200, {"config": config, "revision": revision})
            except ConfigConflict as exc:
                self._json(409, {"error": str(exc)})
            except (ValueError, json.JSONDecodeError) as exc:
                self._json(400, {"error": str(exc)})
            except Exception as exc:
                self._json(500, {"error": str(exc)})

    return Handler


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19182)
    parser.add_argument("--config", type=Path, default=Path("config/cta_special.json"))
    parser.add_argument("--index", type=Path, default=root / "web/cta_special_config/index.html")
    parser.add_argument("--check", action="store_true", help="validate config and exit")
    args = parser.parse_args()
    store = ConfigStore(args.config.resolve())
    store.load()
    if args.check:
        print(f"[cta-special-config] valid: {args.config}")
        return
    server = ThreadingHTTPServer((args.bind, args.port), make_handler(store, args.index.resolve()))
    print(f"[cta-special-config] listening on http://{args.bind}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
