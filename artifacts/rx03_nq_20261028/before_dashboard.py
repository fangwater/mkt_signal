#!/usr/bin/env python3
"""Read-only dashboard server for a CTA perp environment."""

from __future__ import annotations

import argparse
import gzip
import sys
import json
import hashlib
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
from cta_perp_signal_history import (  # noqa: E402
    BinanceMinuteClient, SignalHistoryStore, SignalPanelCollector, configured_targets,
)
from cta_perp_position_replay import PositionReplay
from cta_perp_config_server import position_strategy_factors

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


def factor_strategies(status_path: Path, config_path: Path) -> dict[str, list[str]]:
    status, _ = _read_json(status_path)
    table = status.get("symbol_factors") if isinstance(status, dict) else None
    if not isinstance(table, dict):
        table, _ = _read_json(config_path.with_name("cta_perp_symbol_factors.json"))
    if not isinstance(table, dict):
        return {}
    config, _ = _read_json(config_path)
    algorithm = config.get("position_algorithm", "v0") if isinstance(config, dict) else "v0"
    return {symbol: sorted(position_strategy_factors(row, algorithm))
            for symbol, row in table.items() if isinstance(row, dict)}


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
        and signal_state == "online",
        "config": config,
        "status": status,
        "execution": execution,
        "errors": [
            error for error in (config_error, status_error, execution_error) if error
        ],
    }


def make_handler(
    config_path: Path, status_path: Path, execution_status_path: Path, index_path: Path,
    history: SignalHistoryStore | None = None, collector: SignalPanelCollector | None = None,
    targets=None, panel_path: Path | None = None, vendor_path: Path | None = None, replay=None,
):
    class Handler(BaseHTTPRequestHandler):
        server_version = "cta-perp-dashboard"

        def log_message(self, message: str, *args: Any) -> None:
            print(f"[cta-perp-dashboard] {self.address_string()} {message % args}")

        def _send(self, status: int, body: bytes, content_type: str, encoding: str | None = None, etag: str | None = None) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            if encoding:
                self.send_header("Content-Encoding", encoding)
                self.send_header("Vary", "Accept-Encoding")
            if etag:
                self.send_header("ETag", etag)
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'")
            try:
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                # Switching factors intentionally aborts the previous request.
                self.close_connection = True

        def _json(self, status: int, value: Any, etag: str | None = None) -> None:
            body = json.dumps(value, ensure_ascii=True, separators=(",", ":"), allow_nan=False).encode()
            compressed = len(body) > 4096 and "gzip" in self.headers.get("Accept-Encoding", "")
            self._send(status, gzip.compress(body) if compressed else body,
                       "application/json; charset=utf-8", "gzip" if compressed else None, etag)

        def _asset(self, body: bytes, content_type: str) -> None:
            compressed = len(body) > 4096 and "gzip" in self.headers.get("Accept-Encoding", "")
            self._send(200, gzip.compress(body) if compressed else body,
                       content_type, "gzip" if compressed else None)

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path == "/signals":
                # Relative Location keeps a reverse proxy's environment prefix.
                self.send_response(302)
                self.send_header("Location", "signals/")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if path == "/signals/panel.js":
                try:
                    page = panel_path or index_path.parent.parent / "cta_perp_signals/index.html"
                    self._asset(page.with_name("panel.js").read_bytes(), "text/javascript; charset=utf-8")
                except OSError as exc:
                    self._json(500, {"error":str(exc)})
                return
            if path in {"/signals/", "/signals/index.html"}:
                try:
                    page = panel_path or index_path.parent.parent / "cta_perp_signals/index.html"
                    self._asset(page.read_bytes(), "text/html; charset=utf-8")
                except OSError as exc:
                    self._json(500, {"error":str(exc)})
                return
            if path == "/assets/echarts.min.js":
                try:
                    asset = vendor_path or index_path.parent.parent / "vendor/echarts.min.js"
                    self._asset(asset.read_bytes(), "text/javascript; charset=utf-8")
                except OSError as exc:
                    self._json(500, {"error":str(exc)})
                return
            if path in {"/api/signal-panel", "/api/signal-history"}:
                if history is None:
                    self._json(503, {"error":"signal history is unavailable"})
                    return
                try:
                    active = targets() if targets else []
                    if path == "/api/signal-panel":
                        body = history.overview(active)
                        signal_status, error = _read_json(status_path)
                        body["journal_error"] = collector.journal_error if collector else None
                        body["signal_journal_healthy"] = signal_status.get("journal_healthy") if isinstance(signal_status, dict) else None
                        body["signal_status_age_ms"] = build_snapshot(config_path, status_path, execution_status_path)["status_age_ms"]
                        body["factor_strategies"] = factor_strategies(status_path, config_path)
                        config, _ = _read_json(config_path)
                        body["trading_enabled"] = config.get("enabled") is True if isinstance(config, dict) else None
                        if replay and replay.settings_path.exists():
                            body['simulation'] = replay.overview()
                        self._json(200, body)
                    else:
                        query = parse_qs(urlparse(self.path).query)
                        if set(query) - {"symbol", "hours", "strategy"}:
                            raise ValueError("unknown signal-history query fields")
                        symbol = query.get("symbol", [""])[0]
                        hours = int(query.get("hours", ["24"])[0])
                        known = {row["symbol"] for row in history.overview(active)["symbols"]}
                        if symbol not in known:
                            raise ValueError("symbol is outside the configured or recorded universe")
                        body = history.series(symbol, hours)
                        strategy = query.get('strategy', [''])[0]
                        available = factor_strategies(status_path, config_path).get(symbol, [])
                        if replay and replay.settings_path.exists():
                            body['simulation'] = replay.series(symbol,body['start_ms'],strategy or None)
                            available = sorted(set(available) | set(body['simulation'].get('available_strategies', [])))
                        if strategy:
                            if strategy not in available:
                                raise ValueError("factor strategy is not available for this symbol")
                            body['signals'] = [row for row in body['signals'] if row['snapshot'].get('strategy_name') == strategy]
                        body['strategy'] = strategy
                        body['available_strategies'] = available
                        body['revision'] = hashlib.sha256((body['revision']+str(replay.key if replay else '')+strategy).encode()).hexdigest()
                        etag = '"' + body["revision"] + '"'
                        if self.headers.get("If-None-Match") == etag:
                            self._send(304, b"", "application/json", etag=etag)
                        else:
                            self._json(200, body, etag)
                except ValueError as exc:
                    self._json(400, {"error":str(exc)})
                except Exception as exc:
                    self._json(500, {"error":str(exc)})
                return
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
    parser.add_argument("--config", type=Path, default=Path("config/cta_perp.json"))
    parser.add_argument("--status", type=Path, default=Path("run/cta_perp_status.json"))
    parser.add_argument(
        "--execution-status",
        type=Path,
        default=Path("run/cta_perp_execution_status.json"),
    )
    parser.add_argument("--index", type=Path, default=root / "web/cta_perp/index.html")
    parser.add_argument("--history-db", type=Path, default=Path("data/cta_perp/signal_history.sqlite3"))
    parser.add_argument("--signal-journal", type=Path, default=Path("data/cta_perp/signal_events"))
    parser.add_argument("--symbols-config", type=Path, default=Path("config/cta_perp_symbol_factors.json"))
    parser.add_argument("--kline-poll-seconds", type=float, default=20)
    parser.add_argument("--no-collector", action="store_true", help="serve previously saved history without contacting the exchange")
    parser.add_argument('--factor-history', type=Path, default=Path('data/cta_perp/factor_history'))
    parser.add_argument('--replay-settings', type=Path, default=Path('config/cta_perp_replay.json'))
    args = parser.parse_args()
    if args.kline_poll_seconds < 10:
        parser.error("--kline-poll-seconds must be at least 10")
    history = SignalHistoryStore(args.history_db)
    replay = PositionReplay(history,args.config,args.status,args.replay_settings)
    targets = lambda: configured_targets(args.status, args.symbols_config)
    collector = None
    if not args.no_collector:
        collector = SignalPanelCollector(history, BinanceMinuteClient(), targets, args.signal_journal, args.kline_poll_seconds,args.factor_history,replay)
        collector.start()
    server = ThreadingHTTPServer(
        (args.bind, args.port),
        make_handler(
            args.config.resolve(),
            args.status.resolve(),
            args.execution_status.resolve(),
            args.index.resolve(), history, collector, targets, replay=replay,
        ),
    )
    print(f"[cta-perp-dashboard] listening on http://{args.bind}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        if collector:
            collector.close()
        history.close()


if __name__ == "__main__":
    main()
