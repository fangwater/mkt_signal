#!/usr/bin/env python3
"""CTA Spread configuration with NQ/basis entry gates and independent sleeves."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from cta_spread_factor_config import read_factor_table, save_factor_table, available_universe, reject_duplicate_keys  # noqa: E402

OPEN_VENUE = "binance-futures"
HEDGE_VENUE = "binance-futures"

MAX_BODY_BYTES = 512 * 1024

TOP_FIELDS = {"enabled", "max_pos_u", "entry", "execution"}
ENTRY_FIELDS = {
    "factor_long_quantile", "factor_short_quantile",
    "spread_long_quantile", "spread_short_quantile",
    "nq_long_quantile", "nq_short_quantile",
}
EXECUTION_FIELDS = {"factor_exit_quantile_long", "factor_exit_quantile_short"}


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
    if "execution" not in root:
        raise ValueError("config.execution is required")

    amount = _number(root.get("max_pos_u", 200.0), "max_pos_u")
    if amount <= 0:
        raise ValueError("max_pos_u must be a positive finite USDT amount")

    entry = _object(root.get("entry", {}), "entry")
    _reject_unknown(entry, ENTRY_FIELDS, "entry")
    factor_long = _number(entry.get("factor_long_quantile", 0.9), "entry.factor_long_quantile")
    factor_short = _number(entry.get("factor_short_quantile", 0.1), "entry.factor_short_quantile")
    if not (0.0 <= factor_short < factor_long <= 1.0):
        raise ValueError("entry factor quantiles must satisfy 0 <= short < long <= 1")
    gates = {name: _number(entry.get(name, default), f"entry.{name}") for name, default in [
        ("spread_long_quantile", 0.7), ("spread_short_quantile", 0.3),
        ("nq_long_quantile", 0.5), ("nq_short_quantile", 0.5),
    ]}
    if not all(0 <= value <= 1 for value in gates.values()):
        raise ValueError("entry spread and NQ quantiles must be in [0, 1]")

    execution = _object(root["execution"], "execution")
    _reject_unknown(execution, EXECUTION_FIELDS, "execution")
    exit_long = _number(
        execution.get("factor_exit_quantile_long", 0.3),
        "execution.factor_exit_quantile_long",
    )
    exit_short = _number(
        execution.get("factor_exit_quantile_short", 0.7),
        "execution.factor_exit_quantile_short",
    )
    if not 0.0 <= exit_long < factor_long:
        raise ValueError(
            "execution.factor_exit_quantile_long must be below entry.factor_long_quantile"
        )
    if not factor_short < exit_short <= 1.0:
        raise ValueError(
            "execution.factor_exit_quantile_short must be above entry.factor_short_quantile"
        )

    return {
        "enabled": _bool(root.get("enabled", False), "enabled"),
        "max_pos_u": amount,
        "entry": {
            "factor_long_quantile": factor_long,
            "factor_short_quantile": factor_short,
            **gates,
        },
        "execution": {
            "factor_exit_quantile_long": exit_long,
            "factor_exit_quantile_short": exit_short,
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


def make_handler(
    store: ConfigStore,
    index_path: Path,
    redis_client: Any = None,
    env_name: str | None = None,
):
    class Handler(BaseHTTPRequestHandler):
        server_version = "cta-spread-config"

        def log_message(self, message: str, *args: Any) -> None:
            print(f"[cta-spread-config] {self.address_string()} {message % args}")

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
                if path == "/api/symbol-factors":
                    if redis_client is None or not env_name:
                        self._json(503, {"error": "Redis factor configuration is unavailable"})
                        return
                    self._json(200, {**read_factor_table(redis_client, env_name), "available": available_universe(store.path.parent / "cta_spread_factor_model_1m_pub.toml")})
                    return
                if path in {"/", "/index.html"}:
                    self._send(200, index_path.read_bytes(), "text/html; charset=utf-8")
                    return
                self._json(404, {"error": "not found"})
            except Exception as exc:
                self._json(500, {"error": str(exc)})

        def do_POST(self) -> None:  # noqa: N802
            if urlparse(self.path).path == "/api/deployment-preview":
                try:
                    from cta_spread_deployment import prepare_deployment
                    length = int(self.headers.get("Content-Length", "0"))
                    if not 0 < length <= MAX_BODY_BYTES:
                        raise ValueError("invalid request body size")
                    raw = json.loads(self.rfile.read(length), object_pairs_hook=reject_duplicate_keys)
                    prepared = prepare_deployment(raw, store.load()[0])
                    from cta_spread_factor_config import validate_available
                    validate_available(prepared["symbol_factors"], available_universe(store.path.parent / "cta_spread_factor_model_1m_pub.toml"))
                    self._json(200, prepared)
                except (ValueError, KeyError, TypeError) as exc:
                    self._json(400, {"error": str(exc)})
                return
            if urlparse(self.path).path == "/api/symbol-factors":
                if redis_client is None or not env_name:
                    self._json(503, {"error": "Redis factor configuration is unavailable"})
                    return
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    if length <= 0 or length > MAX_BODY_BYTES:
                        raise ValueError("invalid request body size")
                    request = _object(json.loads(self.rfile.read(length), object_pairs_hook=reject_duplicate_keys), "request")
                    _reject_unknown(request, {"symbol_factors", "expected_revision"}, "request")
                    universe = available_universe(store.path.parent / "cta_spread_factor_model_1m_pub.toml")
                    saved = save_factor_table(
                        redis_client, env_name,
                        request.get("symbol_factors"), request.get("expected_revision", ""),
                        universe,
                    )
                    self._json(200, saved | {"available": universe})
                except ValueError as exc:
                    self._json(409 if "configuration changed" in str(exc) else 400, {"error": str(exc)})
                except Exception as exc:
                    self._json(500, {"error": str(exc)})
                return
            self._json(404, {"error": "not found"})

        def do_PUT(self) -> None:  # noqa: N802
            if urlparse(self.path).path != "/api/config":
                self._json(404, {"error": "not found"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0 or length > MAX_BODY_BYTES:
                    raise ValueError("invalid request body size")
                request = _object(json.loads(self.rfile.read(length), object_pairs_hook=reject_duplicate_keys), "request")
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
    parser.add_argument("--port", type=int, default=19174)
    parser.add_argument("--config", type=Path, default=Path("config/cta_spread.json"))
    parser.add_argument("--index", type=Path, default=root / "web/cta_spread_config/index.html")
    parser.add_argument("--check", action="store_true", help="validate config and exit")
    args = parser.parse_args()
    store = ConfigStore(args.config.resolve())
    store.load()
    if args.check:
        print(f"[cta-spread-config] valid: {args.config}")
        return
    import redis

    env_name = os.path.basename(os.getcwd()).strip().lower()
    redis_client = redis.Redis(
        host=os.getenv("REDIS_HOST", "127.0.0.1"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        username=os.getenv("REDIS_USERNAME"),
        password=os.getenv("REDIS_PASSWORD"),
        socket_connect_timeout=3, socket_timeout=3,
    )
    redis_client.ping()
    server = ThreadingHTTPServer(
        (args.bind, args.port),
        make_handler(store, args.index.resolve(), redis_client, env_name),
    )
    print(f"[cta-spread-config] listening on http://{args.bind}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
