#!/usr/bin/env python3
"""Read-only Spread observation replay and public spot candles; never publish targets."""
from __future__ import annotations

import json
import logging
import math
import os
import re
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

MINUTE = 60_000
DAY = 1440 * MINUTE
EPSILON = 4 * sys.float_info.epsilon
LOG = logging.getLogger('cta-spread-history')


def now_ms():
    return time.time_ns() // 1_000_000


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def quantile(value):
    return finite(value) and 0 <= value <= 1


def symbol_name(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Z0-9]{1,28}USDT', value):
        raise ValueError('invalid USDT symbol')
    return value


def configured_table(status_path, factors_path):
    from cta_spread_factor_config import normalize_table
    try:
        status = json.loads(status_path.read_text())
        if isinstance(status.get('symbol_factors'), dict):
            return normalize_table(status['symbol_factors'])
    except (OSError, ValueError):
        pass
    return normalize_table(json.loads(factors_path.read_text()))


def replay(records, factor, sign, config):
    """Causal directional intent, starting flat at the first available observation.

    Mirrors positions.rs thresholds, including signed ties and blocked reversals.
    This is NOT inventory or fills: historical BBO freshness/quantity rules and
    the publication switch cannot be reconstructed from these observations.
    """
    entry, execution = config['entry'], config['execution']
    direction, points, events = 0, [], []
    for row in records:
        sample = row['inputs'].get(factor, {})
        spread = row['spread']
        raw = sample.get('quantile')
        q = (raw if sign > 0 else 1 - raw) if quantile(raw) else None
        sq = spread.get('quantile')
        ready = bool(sample.get('ready')) and q is not None and finite(sample.get('score'))
        spread_ready = bool(spread.get('ready')) and quantile(sq)
        vote, next_direction, reason = 0, direction, 'factor_unready'
        if ready:
            vote = 1 if q - entry['factor_long_quantile'] > EPSILON else -1 if entry['factor_short_quantile'] - q > EPSILON else 0
            allowed = spread_ready and ((vote > 0 and entry['spread_lower_quantile'] - sq > EPSILON)
                                        or (vote < 0 and sq - entry['spread_upper_quantile'] > EPSILON))
            if direction and vote == -direction:
                next_direction = vote if allowed else 0
                reason = 'reversal' if allowed else 'blocked_reversal_exit'
            elif ((direction > 0 and execution['factor_exit_quantile_long'] - q > EPSILON)
                  or (direction < 0 and q - execution['factor_exit_quantile_short'] > EPSILON)):
                next_direction, reason = 0, 'factor_exit'
            elif direction:
                reason = 'hold'
            elif allowed:
                next_direction, reason = vote, 'cash_entry'
            else:
                reason = 'spread_blocked' if vote else 'no_entry'
        point = dict(ts=row['model_ts_ms'], quantile=q, spread_quantile=sq,
                     spread_value=spread.get('value'), ready=ready, spread_ready=spread_ready,
                     direction=next_direction, vote=vote, reason=reason)
        if next_direction != direction:
            if direction:
                events.append(dict(point, market='perpetual', side='S' if direction > 0 else 'B',
                                   action='hedge_exit', previous_direction=direction))
            if next_direction:
                events.append(dict(point, market='cash', side='B' if next_direction > 0 else 'S',
                                   action='cash_entry', previous_direction=direction))
        points.append(point)
        direction = next_direction
    return points, events


class SpreadHistory:
    def __init__(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False, timeout=10)
        self.db.row_factory = sqlite3.Row
        with self.db:
            self.db.executescript('''
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS observations(symbol TEXT, ts INTEGER, payload TEXT, PRIMARY KEY(symbol,ts));
                CREATE TABLE IF NOT EXISTS offsets(path TEXT PRIMARY KEY, identity TEXT, offset INTEGER);
                CREATE TABLE IF NOT EXISTS candles(symbol TEXT, ts INTEGER, payload TEXT, PRIMARY KEY(symbol,ts));
                CREATE TABLE IF NOT EXISTS feeds(symbol TEXT PRIMARY KEY, success_ms INTEGER, error TEXT);
            ''')

    def close(self):
        with self.lock:
            self.db.close()

    def import_history(self, directory):
        count = 0
        for path in sorted(directory.glob('????????.jsonl')):
            if path.is_symlink() or not re.fullmatch(r'\d{8}\.jsonl', path.name):
                continue
            stat = path.stat()
            key, identity = str(path.resolve()), f'{stat.st_dev}:{stat.st_ino}'
            with self.lock:
                old = self.db.execute('SELECT * FROM offsets WHERE path=?', (key,)).fetchone()
            offset = old['offset'] if old and old['identity'] == identity and old['offset'] <= stat.st_size else 0
            rows = []
            with path.open('rb') as stream:
                stream.seek(offset)
                for _ in range(8192):
                    line = stream.readline(128 * 1024)
                    if not line or not line.endswith(b'\n'):
                        if len(line) == 128 * 1024:
                            raise ValueError(f'{path.name}: record exceeds 128 KiB')
                        break
                    row = json.loads(line)
                    symbol_name(row['symbol'])
                    ts = row['model_ts_ms']
                    if (not isinstance(ts, int) or isinstance(ts, bool) or ts <= 0 or ts % MINUTE
                            or not isinstance(row['inputs'], dict) or not isinstance(row['spread'], dict)):
                        raise ValueError(f'{path.name}: invalid observation')
                    rows.append((row['symbol'], ts, json.dumps(row, allow_nan=False, separators=(',', ':'))))
                    offset = stream.tell()
            with self.lock, self.db:
                self.db.executemany('INSERT INTO observations VALUES (?,?,?) ON CONFLICT(symbol,ts) DO UPDATE SET payload=excluded.payload WHERE payload!=excluded.payload', rows)
                self.db.execute('INSERT INTO offsets VALUES (?,?,?) ON CONFLICT(path) DO UPDATE SET identity=excluded.identity,offset=excluded.offset', (key, identity, offset))
            count += len(rows)
        return count

    def latest(self):
        with self.lock:
            return {r['symbol']: json.loads(r['payload']) for r in self.db.execute('''
                SELECT o.symbol,o.payload FROM observations o JOIN
                (SELECT symbol,max(ts) ts FROM observations GROUP BY symbol) t
                ON o.symbol=t.symbol AND o.ts=t.ts''')}

    def series(self, symbol, factor, sign, config, hours=24):
        if not 1 <= hours <= 24:
            raise ValueError('hours must be in 1..24')
        with self.lock:
            records = [json.loads(r[0]) for r in self.db.execute('SELECT payload FROM observations WHERE symbol=? ORDER BY ts', (symbol,))]
        current = now_ms()
        latest = records[-1]['model_ts_ms'] if records else None
        # During Kafka catch-up inspect the historical window that actually exists.
        end = min(current // MINUTE * MINUTE, latest) if latest else current // MINUTE * MINUTE
        start = end - hours * 60 * MINUTE
        points, events = replay(records, factor, sign, config)
        with self.lock:
            candles = [json.loads(r[0]) for r in self.db.execute('SELECT payload FROM candles WHERE symbol=? AND ts BETWEEN ? AND ? ORDER BY ts', (symbol, start, end))]
            feed = self.db.execute('SELECT * FROM feeds WHERE symbol=?', (symbol,)).fetchone()
        present = {r['ts'] for r in candles}
        visible = [e for e in events if start <= e['ts'] <= end]
        return dict(symbol=symbol, factor=factor, sign=sign, config=config,
                    start_ms=start, end_ms=end, server_ts_ms=current, latest_model_ms=latest,
                    replay_start_ms=records[0]['model_ts_ms'] if records else None,
                    initial_state='flat', mode='historical_signal_replay', price_market='binance-spot',
                    klines=candles, points=[p for p in points if start <= p['ts'] <= end], events=visible,
                    missing_candles=sum(ts not in present for ts in range(start, end + MINUTE, MINUTE)),
                    feed=dict(feed) if feed else None)

    def save_candles(self, symbol, rows):
        with self.lock, self.db:
            self.db.executemany('INSERT INTO candles VALUES (?,?,?) ON CONFLICT(symbol,ts) DO UPDATE SET payload=excluded.payload',
                                [(symbol, r['ts'], json.dumps(r, allow_nan=False)) for r in rows])

    def feed_result(self, symbol, error):
        with self.lock, self.db:
            self.db.execute('''INSERT INTO feeds VALUES (?,?,?) ON CONFLICT(symbol) DO UPDATE SET
                success_ms=CASE WHEN excluded.error IS NULL THEN excluded.success_ms ELSE feeds.success_ms END,error=excluded.error''',
                (symbol, now_ms() if error is None else None, error))


class SpotMinuteClient:
    """Unauthenticated public GET only; shared backoff and bounded request rate."""
    def __init__(self, opener=urllib.request.urlopen):
        self.base = os.getenv('BINANCE_SPOT_PUBLIC_API_BASE', 'https://api.binance.com').rstrip('/')
        parsed = urllib.parse.urlparse(self.base)
        if (parsed.scheme not in {'http', 'https'} or not parsed.hostname or parsed.username
                or parsed.password or parsed.query or parsed.fragment or parsed.path):
            raise ValueError('invalid public spot API base URL')
        self.opener, self.retry_after, self.next_request = opener, 0.0, 0.0

    def fetch(self, symbol, start, end):
        symbol_name(symbol)
        if time.monotonic() < self.retry_after:
            raise RuntimeError('Binance public API rate-limit backoff')
        time.sleep(max(0, self.next_request - time.monotonic()))
        self.next_request = time.monotonic() + .25
        query = urllib.parse.urlencode(dict(symbol=symbol, interval='1m', startTime=start, endTime=end + MINUTE - 1, limit=1000))
        request = urllib.request.Request(self.base + '/api/v3/klines?' + query, headers={'User-Agent': 'mkt-signal-cta-spread-panel'})
        try:
            with self.opener(request, timeout=10) as response:
                data = json.load(response)
        except urllib.error.HTTPError as error:
            if error.code in {418, 429}:
                try:
                    delay = float(error.headers.get('Retry-After', '60'))
                except (TypeError, ValueError):
                    delay = 60
                self.retry_after = time.monotonic() + min(900, max(60, delay))
            error.close()
            raise RuntimeError(f'Binance spot klines HTTP {error.code}') from None
        if not isinstance(data, list):
            raise ValueError('invalid spot kline response')
        rows, previous = [], start - MINUTE
        for raw in data:
            if not isinstance(raw, list) or len(raw) < 7:
                raise ValueError('invalid spot kline row')
            ts, close_ts = int(raw[0]), int(raw[6])
            opening, high, low, close = map(float, raw[1:5])
            if (ts % MINUTE or ts <= previous or not start <= ts <= end or close_ts != ts + MINUTE - 1
                    or not all(finite(v) for v in [opening, high, low, close])
                    or low <= 0 or low > min(opening, close) or high < max(opening, close)):
                raise ValueError('invalid spot kline timestamps/OHLC')
            rows.append(dict(ts=ts, open=opening, high=high, low=low, close=close, closed=close_ts < now_ms()))
            previous = ts
        return rows


class SpreadCollector:
    def __init__(self, store, directory, table, client=None):
        self.store, self.directory, self.table = store, directory, table
        self.client = client or SpotMinuteClient()
        self.stop = threading.Event()
        self.error = None
        self.workers = []

    def start(self):
        for name, run in [('spread-history', self.import_loop), ('spread-candles', self.candle_loop)]:
            worker = threading.Thread(target=run, name=name, daemon=True)
            worker.start()
            self.workers.append(worker)

    def close(self):
        self.stop.set()
        for worker in self.workers:
            worker.join(timeout=15)

    def import_loop(self):
        while not self.stop.is_set():
            try:
                self.store.import_history(self.directory)
                self.error = None
            except Exception as error:
                self.error = str(error)
                LOG.warning('Spread history import: %s', error)
            self.stop.wait(1)

    def collect_symbol(self, symbol, end):
        start = end - DAY
        with self.store.lock:
            present = {r[0] for r in self.store.db.execute('SELECT ts FROM candles WHERE symbol=? AND ts BETWEEN ? AND ?', (symbol, start, end))}
        # Refresh forming candles, and fill missing history without fetching it on HTTP requests.
        missing = [t for t in range(start, end + MINUTE, MINUTE) if t not in present or t >= end - MINUTE]
        while missing and not self.stop.is_set():
            first = missing[0]
            last = first
            for t in missing[1:1000]:
                if t != last + MINUTE:
                    break
                last = t
            rows = self.client.fetch(symbol, first, last)
            if not rows:
                raise ValueError('spot candle range returned no data')
            self.store.save_candles(symbol, rows)
            received = {r['ts'] for r in rows}
            if any(t not in received for t in range(first, rows[-1]['ts'] + MINUTE, MINUTE)):
                raise ValueError('spot candle range has missing minutes')
            missing = [t for t in missing if t > rows[-1]['ts']]
        if not self.stop.is_set():
            self.store.feed_result(symbol, None)
            with self.store.lock, self.store.db:
                self.store.db.execute('DELETE FROM candles WHERE symbol=? AND ts<?', (symbol, end - 2 * DAY))

    def candle_loop(self):
        while not self.stop.is_set():
            try:
                latest = self.store.latest()
                for symbol in self.table():
                    if self.stop.is_set():
                        break
                    end = min(now_ms() // MINUTE * MINUTE, latest.get(symbol, {}).get('model_ts_ms', now_ms() // MINUTE * MINUTE))
                    try:
                        self.collect_symbol(symbol, end)
                    except Exception as error:
                        self.store.feed_result(symbol, str(error))
                        LOG.warning('%s spot candles: %s', symbol, error)
            except Exception as error:
                LOG.warning('Spread candle collector: %s', error)
            self.stop.wait(20)
