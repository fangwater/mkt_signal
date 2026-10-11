#!/usr/bin/env python3
"""Persist Binance futures 1m candles and immutable CTA signal snapshots."""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import hashlib
import logging
import math
import os
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable

MINUTE_MS = 60_000
DAY_MS = 24 * 60 * MINUTE_MS
RETENTION_MS = 2 * DAY_MS
STATES = {"observed", "scheduled", "published", "partial", "failed", "skipped", "cancelled"}
LOG = logging.getLogger("cta-signal-history")


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def symbol_name(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Z0-9]{1,32}", value):
        raise ValueError("symbol must be an uppercase venue symbol")
    return value


def normalize_kline(row: Any, current_ms: int) -> dict[str, Any]:
    if not isinstance(row, list) or len(row) < 11:
        raise ValueError("invalid Binance kline row")
    open_time, close_time = int(row[0]), int(row[6])
    if open_time <= 0 or open_time % MINUTE_MS or close_time != open_time + MINUTE_MS - 1:
        raise ValueError("invalid 1m kline timestamps")
    values = [float(row[i]) for i in (1, 2, 3, 4, 5, 7, 9, 10)]
    if not all(math.isfinite(v) for v in values):
        raise ValueError("non-finite kline values")
    opening, high, low, close, volume, quote_volume, buy_volume, buy_quote_volume = values
    if (low <= 0 or low > min(opening, close) or high < max(opening, close)
            or any(v < 0 for v in (volume, quote_volume, buy_volume, buy_quote_volume))):
        raise ValueError("invalid kline OHLC or volumes")
    return dict(open_time_ms=open_time, close_time_ms=close_time, open=opening,
                high=high, low=low, close=close, volume=volume, quote_volume=quote_volume,
                trades=int(row[8]), taker_buy_volume=buy_volume,
                taker_buy_quote_volume=buy_quote_volume, closed=close_time < current_ms)


class SignalHistoryStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False, timeout=10)
        self.db.row_factory = sqlite3.Row
        with self.db:
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=NORMAL")
            self.db.executescript("""
                CREATE TABLE IF NOT EXISTS klines (
                    symbol TEXT NOT NULL, open_time_ms INTEGER NOT NULL,
                    close_time_ms INTEGER NOT NULL, open REAL NOT NULL,
                    high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL,
                    volume REAL NOT NULL, quote_volume REAL NOT NULL, trades INTEGER NOT NULL,
                    taker_buy_volume REAL NOT NULL, taker_buy_quote_volume REAL NOT NULL,
                    closed INTEGER NOT NULL, updated_ms INTEGER NOT NULL,
                    PRIMARY KEY(symbol, open_time_ms));
                CREATE TABLE IF NOT EXISTS signal_events (
                    event_id TEXT PRIMARY KEY, signal_id TEXT NOT NULL, symbol TEXT NOT NULL,
                    model_ts_ms INTEGER NOT NULL, event_ts_us INTEGER NOT NULL,
                    payload TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS signal_events_symbol_time ON signal_events(symbol, model_ts_ms);
                CREATE TABLE IF NOT EXISTS signals (
                    signal_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, model_ts_ms INTEGER NOT NULL,
                    event_ts_us INTEGER NOT NULL, published INTEGER NOT NULL, payload TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS signals_symbol_time ON signals(symbol, model_ts_ms);
                CREATE TABLE IF NOT EXISTS journal_offsets (
                    path TEXT PRIMARY KEY, identity TEXT NOT NULL, offset INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS feeds (
                    symbol TEXT PRIMARY KEY, last_success_ms INTEGER, error TEXT);
                CREATE TABLE IF NOT EXISTS factor_observations (
                    symbol TEXT NOT NULL, model_ts_ms INTEGER NOT NULL, payload TEXT NOT NULL,
                    PRIMARY KEY(symbol,model_ts_ms));
            """)

    def close(self) -> None:
        with self.lock:
            self.db.close()

    def save_klines(self, symbol: str, rows: list[dict], current_ms: int) -> None:
        symbol_name(symbol)
        keys = ("open_time_ms", "close_time_ms", "open", "high", "low", "close",
                "volume", "quote_volume", "trades", "taker_buy_volume", "taker_buy_quote_volume", "closed")
        with self.lock, self.db:
            self.db.executemany("""
                INSERT INTO klines VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(symbol,open_time_ms) DO UPDATE SET
                    close_time_ms=excluded.close_time_ms, open=excluded.open, high=excluded.high,
                    low=excluded.low, close=excluded.close, volume=excluded.volume,
                    quote_volume=excluded.quote_volume, trades=excluded.trades,
                    taker_buy_volume=excluded.taker_buy_volume,
                    taker_buy_quote_volume=excluded.taker_buy_quote_volume,
                    closed=excluded.closed, updated_ms=excluded.updated_ms
            """, [(symbol, *(row[key] for key in keys), current_ms) for row in rows])

    def feed_result(self, symbol: str, error: str | None, current_ms: int) -> None:
        with self.lock, self.db:
            self.db.execute("""INSERT INTO feeds VALUES (?,?,?) ON CONFLICT(symbol) DO UPDATE SET
                last_success_ms=CASE WHEN excluded.error IS NULL THEN excluded.last_success_ms ELSE feeds.last_success_ms END,
                error=excluded.error""", (symbol, current_ms if error is None else None, error))

    def missing_ranges(self, symbol: str, start_ms: int, last_open_ms: int) -> list[tuple[int, int]]:
        with self.lock:
            present = {row[0] for row in self.db.execute(
                "SELECT open_time_ms FROM klines WHERE symbol=? AND open_time_ms BETWEEN ? AND ?",
                (symbol, start_ms, last_open_ms))}
        # Always refresh the two latest buckets so a forming candle becomes final.
        missing = [ts for ts in range(start_ms, last_open_ms + 1, MINUTE_MS)
                   if ts not in present or ts >= last_open_ms - MINUTE_MS]
        ranges: list[tuple[int, int]] = []
        for ts in missing:
            if ranges and ranges[-1][1] + MINUTE_MS == ts:
                ranges[-1] = (ranges[-1][0], ts)
            else:
                ranges.append((ts, ts))
        return ranges

    @staticmethod
    def validate_record(record: Any) -> None:
        if not isinstance(record, dict) or record.get("state") not in STATES:
            raise ValueError("invalid signal journal state")
        symbol_name(record.get("symbol"))
        for key in ("event_id", "signal_id"):
            if not isinstance(record.get(key), str) or not 1 <= len(record[key]) <= 200:
                raise ValueError(f"invalid journal {key}")
        for key in ("model_ts_ms", "event_ts_us"):
            if not isinstance(record.get(key), int) or record[key] <= 0:
                raise ValueError(f"invalid journal {key}")
        if not isinstance(record.get("snapshot"), dict):
            raise ValueError("missing immutable signal snapshot")
        publication = record.get("publication")
        if not isinstance(publication, dict) or not isinstance(publication.get("levels"), list):
            raise ValueError("invalid signal publication")
        levels = publication["levels"]
        if bool(levels) != (record["state"] in {"published", "partial"}):
            raise ValueError("published signal must carry factual published levels")
        for level in levels:
            if (not isinstance(level, dict) or any(not isinstance(level.get(key), (int, float))
                    or not math.isfinite(level[key]) or level[key] <= 0 for key in ("price", "qty", "notional", "published_ts_us"))):
                raise ValueError("invalid published level")

    def import_journal(self, directory: Path, current_ms: int | None = None) -> int:
        imported = 0
        current_ms = now_ms() if current_ms is None else current_ms
        for path in sorted(directory.glob("????????.jsonl")):
            if not re.fullmatch(r"\d{8}\.jsonl", path.name) or path.is_symlink():
                continue
            stat = path.stat()
            identity = f"{stat.st_dev}:{stat.st_ino}"
            key = str(path.resolve())
            with self.lock:
                cursor = self.db.execute("SELECT identity,offset FROM journal_offsets WHERE path=?", (key,)).fetchone()
            offset = cursor["offset"] if cursor and cursor["identity"] == identity and cursor["offset"] <= stat.st_size else 0
            records = []
            with path.open("rb") as stream:
                stream.seek(offset)
                # Bound one ingestion batch; leave an unfinished tail for the next poll.
                for _ in range(2048):
                    line = stream.readline(128 * 1024)
                    if not line or not line.endswith(b"\n"):
                        if len(line) == 128 * 1024:
                            raise ValueError(f"{path.name}:{offset}: signal record exceeds 128 KiB")
                        break
                    try:
                        record = json.loads(line)
                        self.validate_record(record)
                        records.append(record)
                    except (ValueError, UnicodeDecodeError) as exc:
                        # Do not advance across corrupt evidence silently.
                        raise ValueError(f"{path.name}:{offset}: {exc}") from exc
                    offset = stream.tell()
            with self.lock, self.db:
                for record in records:
                    if record["event_ts_us"] // 1000 < current_ms - RETENTION_MS:
                        continue
                    payload = json.dumps(record, separators=(",", ":"), allow_nan=False)
                    result = self.db.execute("INSERT OR IGNORE INTO signal_events VALUES (?,?,?,?,?,?)",
                        (record["event_id"], record["signal_id"], record["symbol"], record["model_ts_ms"], record["event_ts_us"], payload))
                    if result.rowcount:
                        imported += 1
                        self.db.execute("""INSERT INTO signals VALUES (?,?,?,?,?,?)
                            ON CONFLICT(signal_id) DO UPDATE SET event_ts_us=excluded.event_ts_us,
                            published=excluded.published,payload=excluded.payload
                            WHERE excluded.event_ts_us >= signals.event_ts_us""",
                            (record["signal_id"], record["symbol"], record["model_ts_ms"], record["event_ts_us"],
                             int(bool(record["publication"]["levels"] or record["publication"].get("target"))), payload))
                self.db.execute("INSERT INTO journal_offsets VALUES (?,?,?) ON CONFLICT(path) DO UPDATE SET identity=excluded.identity,offset=excluded.offset", (key, identity, offset))
        return imported

    def prune(self, current_ms: int) -> None:
        cutoff = current_ms - RETENTION_MS
        with self.lock, self.db:
            self.db.execute("DELETE FROM klines WHERE open_time_ms < ?", (cutoff,))
            self.db.execute("DELETE FROM signals WHERE event_ts_us < ?", (cutoff * 1000,))
            self.db.execute("DELETE FROM signal_events WHERE event_ts_us < ?", (cutoff * 1000,))

    def import_factor_history(self, directory: Path) -> int:
        imported = 0
        for path in sorted(directory.glob('????????.jsonl')):
            if path.is_symlink():
                continue
            stat = path.stat()
            identity, key = f'{stat.st_dev}:{stat.st_ino}', str(path.resolve())
            with self.lock:
                cursor = self.db.execute('SELECT identity,offset FROM journal_offsets WHERE path=?',(key,)).fetchone()
            offset = cursor['offset'] if cursor and cursor['identity']==identity and cursor['offset']<=stat.st_size else 0
            rows = []
            with path.open('rb') as stream:
                stream.seek(offset)
                for _ in range(4096):
                    line = stream.readline(128*1024)
                    if not line or not line.endswith(b'\n'):
                        if len(line)==128*1024: raise ValueError('factor record exceeds 128 KiB')
                        break
                    record = json.loads(line)
                    symbol_name(record['symbol'])
                    if not isinstance(record['model_ts_ms'],int) or not isinstance(record['inputs'],dict) or not isinstance(record['nq'],dict):
                        raise ValueError('invalid factor history record')
                    rows.append((record['symbol'],record['model_ts_ms'],json.dumps(record,separators=(',',':'),allow_nan=False)))
                    offset = stream.tell()
            with self.lock,self.db:
                self.db.executemany('INSERT INTO factor_observations VALUES (?,?,?) ON CONFLICT(symbol,model_ts_ms) DO UPDATE SET payload=excluded.payload WHERE excluded.payload!=factor_observations.payload',rows)
                self.db.execute('INSERT INTO journal_offsets VALUES (?,?,?) ON CONFLICT(path) DO UPDATE SET identity=excluded.identity,offset=excluded.offset',(key,identity,offset))
            imported += len(rows)
        return imported

    def series(self, symbol: str, hours: int = 24, current_ms: int | None = None) -> dict:
        symbol_name(symbol)
        if not 1 <= hours <= 24:
            raise ValueError("hours must be in 1..24")
        current_ms = now_ms() if current_ms is None else current_ms
        last_open = current_ms // MINUTE_MS * MINUTE_MS
        start = last_open - hours * 60 * MINUTE_MS
        with self.lock:
            klines = [dict(row) for row in self.db.execute(
                "SELECT * FROM klines WHERE symbol=? AND open_time_ms BETWEEN ? AND ? ORDER BY open_time_ms",
                (symbol, start, last_open))]
            signals = [json.loads(row[0]) for row in self.db.execute(
                "SELECT payload FROM signals WHERE symbol=? AND model_ts_ms>=? AND event_ts_us<=? ORDER BY model_ts_ms,event_ts_us",
                (symbol, start, current_ms * 1000))]
            feed = self.db.execute("SELECT * FROM feeds WHERE symbol=?", (symbol,)).fetchone()
        present = {row["open_time_ms"] for row in klines}
        gaps = [ts for ts in range(start, last_open, MINUTE_MS) if ts not in present]
        return {"symbol":symbol, "venue":"binance-futures", "interval":"1m",
                "server_ts_ms":current_ms, "start_ms":start, "end_ms":current_ms,
                "klines":klines, "signals":signals, "expected_closed_bars":hours * 60,
                "closed_bars":sum(row["open_time_ms"] < last_open for row in klines),
                "missing_minutes":gaps, "feed":dict(feed) if feed else None,
                "revision":hashlib.sha256(json.dumps([start,
                    max((row["updated_ms"] for row in klines), default=0),
                    [(record["signal_id"], record["event_id"]) for record in signals],
                    dict(feed) if feed else None], sort_keys=True).encode()).hexdigest()}

    def overview(self, targets: list[str], current_ms: int | None = None) -> dict:
        current_ms = now_ms() if current_ms is None else current_ms
        start = current_ms // MINUTE_MS * MINUTE_MS - DAY_MS
        with self.lock:
            candles = {row["symbol"]: dict(row) for row in self.db.execute("""
                SELECT symbol,count(*) bars,max(open_time_ms) latest_bar_ms FROM klines WHERE open_time_ms>=? GROUP BY symbol""", (start,))}
            signals = {row["symbol"]: dict(row) for row in self.db.execute("""
                SELECT symbol,count(*) observations,sum(published) published,max(event_ts_us) latest_event_us
                FROM signals WHERE model_ts_ms>=? GROUP BY symbol""", (start,))}
            feeds = {row["symbol"]: dict(row) for row in self.db.execute("SELECT * FROM feeds")}
        names = sorted(set(targets) | set(candles) | set(signals))
        return {"server_ts_ms":current_ms, "venue":"binance-futures", "interval":"1m", "symbols":[
            {"symbol":name, "active":name in targets, **candles.get(name, {}), **signals.get(name, {}),
             "feed":feeds.get(name)} for name in names]}


class BinanceMinuteClient:
    def __init__(self, base_url: str | None = None, opener: Callable = urllib.request.urlopen):
        self.base_url = (base_url or os.getenv("BINANCE_FUTURES_PUBLIC_API_BASE", "https://fapi.binance.com")).rstrip("/")
        parsed = urllib.parse.urlparse(self.base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("invalid public futures API base URL")
        self.opener = opener
        self.lock = threading.Lock()
        self.retry_after = 0.0

    def fetch(self, symbol: str, start_ms: int, end_ms: int, limit: int = 499) -> list:
        symbol_name(symbol)
        with self.lock:
            if time.monotonic() < self.retry_after:
                raise RuntimeError("Binance public API is in rate-limit backoff")
        query = urllib.parse.urlencode({"symbol":symbol, "interval":"1m", "startTime":start_ms, "endTime":end_ms, "limit":limit})
        request = urllib.request.Request(self.base_url + "/fapi/v1/klines?" + query,
                                        headers={"User-Agent":"mkt-signal-cta-panel"})
        try:
            with self.opener(request, timeout=10) as response:
                result = json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code in {418, 429}:
                try:
                    delay = float(exc.headers.get("Retry-After", "60"))
                except ValueError:
                    delay = 60.0
                with self.lock:
                    self.retry_after = time.monotonic() + max(60.0, min(delay, 900.0))
            exc.close()
            raise RuntimeError(f"Binance public kline HTTP {exc.code}") from exc
        if not isinstance(result, list):
            raise ValueError("Binance kline response is not an array")
        return result


def collect_symbol(store: SignalHistoryStore, client: BinanceMinuteClient, symbol: str, current_ms: int, stop: threading.Event | None = None) -> None:
    last_open = current_ms // MINUTE_MS * MINUTE_MS
    try:
        for beginning, last in store.missing_ranges(symbol, last_open - DAY_MS, last_open):
            cursor = beginning
            while cursor <= last:
                if stop is not None and stop.is_set():
                    return
                end = min(last + MINUTE_MS - 1, current_ms)
                raw = client.fetch(symbol, cursor, end)
                rows = [normalize_kline(row, current_ms) for row in raw]
                if not rows or any(row["open_time_ms"] < cursor or row["open_time_ms"] > last for row in rows):
                    raise ValueError("empty or out-of-range Binance kline page")
                times = [row["open_time_ms"] for row in rows]
                if times != sorted(set(times)):
                    raise ValueError("unordered or duplicate Binance kline page")
                store.save_klines(symbol, rows, current_ms)
                cursor = times[-1] + MINUTE_MS
        store.feed_result(symbol, None, current_ms)
    except Exception as exc:
        store.feed_result(symbol, str(exc), current_ms)
        raise


class SignalPanelCollector:
    def __init__(self, store: SignalHistoryStore, client: BinanceMinuteClient,
                 targets: Callable[[], list[str]], journal_dir: Path, poll_seconds: float = 20,
                 factor_history: Path | None = None, replay=None):
        self.store, self.client, self.targets = store, client, targets
        self.journal_dir, self.poll_seconds = journal_dir, poll_seconds
        self.stop = threading.Event()
        self.journal_error: str | None = None
        self.workers: list[threading.Thread] = []
        self.factor_history, self.replay = factor_history, replay

    def start(self) -> None:
        for name, function in [("cta-journal-import", self.import_loop), ("cta-1m-collector", self.kline_loop)]:
            worker = threading.Thread(target=function, name=name, daemon=True)
            worker.start()
            self.workers.append(worker)

    def close(self) -> None:
        self.stop.set()
        for worker in self.workers:
            worker.join(timeout=25)

    def import_loop(self) -> None:
        last_prune = 0
        while not self.stop.is_set():
            try:
                self.store.import_journal(self.journal_dir)
                if self.factor_history is not None:
                    self.store.import_factor_history(self.factor_history)
                if self.replay is not None:
                    self.replay.refresh()
                self.journal_error = None
                if now_ms() - last_prune > 60_000:
                    self.store.prune(now_ms())
                    last_prune = now_ms()
                    # The SQLite history retains 48h; leave seven full UTC journal days.
                    for path in self.journal_dir.glob("????????.jsonl"):
                        if re.fullmatch(r"\d{8}\.jsonl", path.name) and not path.is_symlink() and time.time() - path.stat().st_mtime > 8 * 86400:
                            path.unlink()
            except Exception as exc:
                self.journal_error = str(exc)
                LOG.warning("signal journal ingestion: %s", exc)
            self.stop.wait(1)

    def kline_loop(self) -> None:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="cta-kline") as pool:
            while not self.stop.is_set():
                try:
                    futures = {pool.submit(collect_symbol, self.store, self.client, symbol, now_ms(), self.stop):symbol for symbol in self.targets()}
                    for future, symbol in futures.items():
                        try:
                            future.result()
                        except Exception as exc:
                            LOG.warning("%s kline collection: %s", symbol, exc)
                except Exception as exc:
                    LOG.warning("kline target reload: %s", exc)
                self.stop.wait(self.poll_seconds)


def configured_targets(status_path: Path, factors_path: Path) -> list[str]:
    try:
        status = json.loads(status_path.read_text())
        table = status.get("symbol_factors") if isinstance(status, dict) else None
        if isinstance(table, dict):
            return sorted(symbol_name(symbol) for symbol in table)
    except (OSError, ValueError):
        pass
    from cta_special_factor_config import normalize_table
    table = normalize_table(json.loads(factors_path.read_text()))
    return sorted(table)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("data/cta_special/signal_history.sqlite3"))
    parser.add_argument("--journal", type=Path, default=Path("data/cta_special/signal_events"))
    parser.add_argument("--status", type=Path, default=Path("run/cta_special_status.json"))
    parser.add_argument("--symbols-config", type=Path, default=Path("config/cta_special_symbol_factors.json"))
    parser.add_argument("--once", action="store_true", help="collect one 24h snapshot and exit")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    store = SignalHistoryStore(args.db)
    targets = lambda: configured_targets(args.status, args.symbols_config)
    client = BinanceMinuteClient()
    if args.once:
        store.import_journal(args.journal)
        for symbol in targets():
            collect_symbol(store, client, symbol, now_ms())
        print(json.dumps(store.overview(targets()), indent=2))
        store.close()
        return
    collector = SignalPanelCollector(store, client, targets, args.journal)
    collector.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        collector.close()
        store.close()


if __name__ == "__main__":
    main()
