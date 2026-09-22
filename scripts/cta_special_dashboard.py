#!/usr/bin/env python3
"""Read-only dashboard server for a CTA special environment."""

from __future__ import annotations

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

MAX_HEALTHY_STATUS_AGE_MS = 90_000


def _read_json(path: Path) -> tuple[Any, str | None]:
    try:
        return json.loads(path.read_text(encoding="utf-8")), None
    except FileNotFoundError:
        return None, None
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"{path.name}: {exc}"


def _feed_state(value: Any, error: str | None, age_ms: int | None) -> str:
    if error:
        return "error"
    if not isinstance(value, dict):
        return "offline"
    if age_ms is None or age_ms > MAX_HEALTHY_STATUS_AGE_MS:
        return "stale"
    return "online"


def build_snapshot(
    config_path: Path, status_path: Path, execution_status_path: Path
) -> dict[str, Any]:
    config, config_error = _read_json(config_path)
    status, status_error = _read_json(status_path)
    execution, execution_error = _read_json(execution_status_path)
    now_us = time.time_ns() // 1000
    updated_ts_us = status.get("updated_ts_us", 0) if isinstance(status, dict) else 0
    execution_updated_ts_us = (
        execution.get("updated_ts_us", 0) if isinstance(execution, dict) else 0
    )
    status_age_ms = max(0, (now_us - updated_ts_us) // 1000) if updated_ts_us > 0 else None
    execution_age_ms = (
        max(0, (now_us - execution_updated_ts_us) // 1000)
        if execution_updated_ts_us > 0
        else None
    )
    signal_state = _feed_state(status, status_error, status_age_ms)
    execution_state = _feed_state(execution, execution_error, execution_age_ms)
    return {
        "server_ts_us": now_us,
        "status_age_ms": status_age_ms,
        "execution_age_ms": execution_age_ms,
        "signal_state": signal_state,
        "execution_state": execution_state,
        "healthy": config_error is None
        and signal_state == "online"
        and execution_state == "online",
        "config": config,
        "status": status,
        "execution": execution,
        "errors": [
            error for error in (config_error, status_error, execution_error) if error
        ],
    }


def make_handler(
    config_path: Path, status_path: Path, execution_status_path: Path, index_path: Path
):
    class Handler(BaseHTTPRequestHandler):
        server_version = "cta-special-dashboard"

        def log_message(self, message: str, *args: Any) -> None:
            print(f"[cta-special-dashboard] {self.address_string()} {message % args}")

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
            if path == "/api/snapshot":
                self._json(200, build_snapshot(config_path, status_path, execution_status_path))
                return
            if path == "/api/healthz":
                snapshot = build_snapshot(config_path, status_path, execution_status_path)
                self._json(200 if snapshot["healthy"] else 503, snapshot)
                return
            if path in {"/", "/index.html"}:
                try:
                    self._send(200, index_path.read_bytes(), "text/html; charset=utf-8")
                except OSError as exc:
                    self._json(500, {"error": str(exc)})
                return
            self._json(404, {"error": "not found"})

    return Handler


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=10192)
    parser.add_argument("--config", type=Path, default=Path("config/cta_special.json"))
    parser.add_argument("--status", type=Path, default=Path("run/cta_special_status.json"))
    parser.add_argument(
        "--execution-status",
        type=Path,
        default=Path("run/cta_special_execution_status.json"),
    )
    parser.add_argument("--index", type=Path, default=root / "web/cta_special/index.html")
    args = parser.parse_args()
    server = ThreadingHTTPServer(
        (args.bind, args.port),
        make_handler(
            args.config.resolve(),
            args.status.resolve(),
            args.execution_status.resolve(),
            args.index.resolve(),
        ),
    )
    print(f"[cta-special-dashboard] listening on http://{args.bind}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
