#!/usr/bin/env python3
"""Read-only CTA Spread targets and signal evidence. Factual PnL lives in Manager."""
from __future__ import annotations
import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


def build_snapshot(config_path: Path, status_path: Path) -> dict:
    result = {"config": None, "status": None, "errors": []}
    for key, path in [("config", config_path), ("status", status_path)]:
        try:
            result[key] = json.loads(path.read_text())
        except (OSError, ValueError) as error:
            result["errors"].append(f"{path.name}: {error}")
    now = time.time_ns() // 1000
    ts = (result["status"] or {}).get("updated_ts_us", 0)
    age = max(0, (now - ts) // 1000) if ts else None
    result.update(server_ts_us=now, status_age_ms=age,
                  signal_state="offline" if age is None else "online" if age <= 90000 else "stale")
    return result


def make_handler(config_path: Path, status_path: Path, index_path: Path):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = urlparse(self.path).path
            if path == "/":
                body, kind = index_path.read_bytes(), "text/html; charset=utf-8"
            elif path == "/api/snapshot":
                body = json.dumps(build_snapshot(config_path, status_path), allow_nan=False).encode()
                kind = "application/json; charset=utf-8"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                self.close_connection = True
    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=10186)
    parser.add_argument("--config", type=Path, default=Path("config/cta_spread.json"))
    parser.add_argument("--status", type=Path, default=Path("run/cta_spread_status.json"))
    parser.add_argument("--index", type=Path, default=Path("web/cta_spread/index.html"))
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.bind, args.port), make_handler(args.config, args.status, args.index))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
