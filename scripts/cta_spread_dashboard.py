#!/usr/bin/env python3
"""Read-only CTA Spread targets and signal evidence. Factual PnL lives in Manager."""
from __future__ import annotations
import argparse
import json
import gzip
import logging
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from cta_spread_signal_history import SpreadHistory, SpreadCollector, configured_table, finite, quantile


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


def live_ready(row, current_ms, heartbeat_online):
    signal = row.get('signal') or {}
    model_ts = row.get('model_ts_ms')
    return (heartbeat_online and bool(row.get('score_ready')) and bool(signal.get('filter_ready'))
            and finite(model_ts) and 0 <= current_ms - model_ts <= 180_000)


def make_handler(config_path: Path, status_path: Path, index_path: Path,
                 history=None, factors_path=None, collector=None):
    factors_path = factors_path or config_path.with_name("cta_spread_symbol_factors.json")
    web = index_path.parent.parent

    def panel():
        result = build_snapshot(config_path, status_path)
        table = configured_table(status_path, factors_path)
        latest = history.latest() if history else {}
        strategies = []
        status = result['status'] or {}
        for factor in sorted({f for row in table.values() for f, w in row.items() if w}):
            symbols = []
            for symbol, weights in sorted(table.items()):
                if not weights.get(factor):
                    continue
                observation = latest.get(symbol, {})
                sample = observation.get('inputs', {}).get(factor, {})
                spread = observation.get('spread', {})
                q = sample.get('quantile')
                sign = 1 if weights[factor] > 0 else -1
                live = status.get('strategies', {}).get(factor, {}).get(symbol, {})
                symbols.append(dict(symbol=symbol, sign=sign,
                    quantile=(q if sign > 0 else 1-q) if quantile(q) else None,
                    ready=bool(sample.get('ready')) and quantile(q) and finite(sample.get('score')),
                    spread_ready=bool(spread.get('ready')) and quantile(spread.get('quantile')),
                    spread_quantile=spread.get('quantile'), model_ts_ms=observation.get('model_ts_ms'),
                    live_ready=live_ready(live, result['server_ts_us'] // 1000, result['signal_state'] == 'online'), position_flag=live.get('position_flag', 0)))
            strategies.append(dict(factor=factor, symbols=symbols))
        times = [row['model_ts_ms'] for row in latest.values()]
        result.update(strategies=strategies, latest_model_ms=max(times, default=None),
                      oldest_model_ms=min(times, default=None),
                      history_error=collector.error if collector else None)
        return result
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path
            try:
                if path == "/signals":
                    self.send_response(302)
                    self.send_header("Location", "signals/")
                    self.end_headers()
                    return
                assets = {
                    "/": (index_path, "text/html; charset=utf-8"),
                    "/signals/": (web / "cta_spread_signals/index.html", "text/html; charset=utf-8"),
                    "/signals/panel.js": (web / "cta_spread_signals/panel.js", "text/javascript; charset=utf-8"),
                    "/assets/echarts.min.js": (web / "vendor/echarts.min.js", "text/javascript; charset=utf-8"),
                }
                if path in assets:
                    file, kind = assets[path]
                    body = file.read_bytes()
                elif path == "/api/snapshot":
                    body = json.dumps(build_snapshot(config_path, status_path), allow_nan=False).encode()
                    kind = "application/json; charset=utf-8"
                elif path == "/api/signal-panel":
                    body = json.dumps(panel(), allow_nan=False).encode()
                    kind = "application/json; charset=utf-8"
                elif path == "/api/signal-history" and history is not None:
                    query = parse_qs(parsed.query)
                    symbol, factor = query.get('symbol', [''])[0], query.get('factor', [''])[0]
                    table = configured_table(status_path, factors_path)
                    weight = table.get(symbol, {}).get(factor, 0)
                    if not weight:
                        raise ValueError('symbol × factor is not configured')
                    config = json.loads(config_path.read_text())
                    result = history.series(symbol, factor, 1 if weight > 0 else -1,
                                            config, int(query.get('hours', ['24'])[0]))
                    body = json.dumps(result, allow_nan=False, separators=(',', ':')).encode()
                    kind = "application/json; charset=utf-8"
                else:
                    self.send_error(404)
                    return
            except ValueError as error:
                self.send_error(400, str(error))
                return
            except (OSError, KeyError) as error:
                logging.warning('Spread dashboard read: %s', error)
                self.send_error(503, 'Spread data unavailable')
                return
            compressed = len(body) > 2048 and 'gzip' in self.headers.get('Accept-Encoding', '')
            if compressed:
                body = gzip.compress(body)
            self.send_response(200)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Vary", "Accept-Encoding")
            if compressed:
                self.send_header("Content-Encoding", "gzip")
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
    parser.add_argument("--db", type=Path, default=Path("data/cta_spread/signal_history.sqlite3"))
    parser.add_argument("--factor-history", type=Path, default=Path("data/cta_spread/factor_history"))
    parser.add_argument("--symbols-config", type=Path, default=Path("config/cta_spread_symbol_factors.json"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    history = SpreadHistory(args.db)
    collector = SpreadCollector(history, args.factor_history,
                                lambda: configured_table(args.status, args.symbols_config))
    server = ThreadingHTTPServer((args.bind, args.port),
        make_handler(args.config, args.status, args.index, history, args.symbols_config, collector))
    collector.start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        collector.close()
        history.close()


if __name__ == "__main__":
    main()
