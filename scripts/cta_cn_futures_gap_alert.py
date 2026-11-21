#!/usr/bin/env python3
"""Send DingTalk notices for domestic CTA Redis minute gaps."""

import argparse
from datetime import datetime, timedelta, timezone
from itertools import groupby
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import time
from urllib.request import ProxyHandler, build_opener


SCHEMA = """
CREATE TABLE IF NOT EXISTS cursors (
    device INTEGER, inode INTEGER, offset INTEGER, PRIMARY KEY (device, inode)
);
CREATE TABLE IF NOT EXISTS gaps (
    trading_day INTEGER, contract TEXT COLLATE NOCASE, start_ms INTEGER,
    sent INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (trading_day, contract, start_ms)
);
CREATE TABLE IF NOT EXISTS delivery (
    id INTEGER PRIMARY KEY, attempts INTEGER NOT NULL, retry_at INTEGER NOT NULL
);
"""
SHANGHAI = timezone(timedelta(hours=8))


def record(db, day, contract, start_ms):
    if (type(day) is int and 20000101 <= day <= 99991231
            and isinstance(contract, str) and re.fullmatch(r"[A-Za-z0-9]{1,32}", contract)
            and type(start_ms) is int and 0 < start_ms < 253402214400000):
        db.execute(
            "INSERT OR IGNORE INTO gaps (trading_day, contract, start_ms) VALUES (?, ?, ?)",
            (day, contract, start_ms),
        )


def ingest_logs(db, directory, bootstrap):
    for path in sorted(directory.glob("worker.error*.log")):
        try:
            log = path.open("rb")
        except FileNotFoundError:
            continue  # PM2 may replace a log during enumeration.
        with log:
            info = os.fstat(log.fileno())
            key = (info.st_dev, info.st_ino)
            cursor = db.execute(
                "SELECT offset FROM cursors WHERE device = ? AND inode = ?", key
            ).fetchone()
            if bootstrap:
                # Existing unresolved gaps are seeded from CTA state. Start
                # following the log before querying state to cover new events.
                offset = info.st_size
            else:
                offset = cursor[0] if cursor and cursor[0] <= info.st_size else 0
                log.seek(offset)
                for line in log:
                    if not line.endswith(b"\n"):
                        break
                    offset += len(line)
                    if b'"redis_minute_gap"' not in line:
                        continue
                    try:
                        event = json.loads(line)
                    except (UnicodeError, json.JSONDecodeError):
                        continue
                    if isinstance(event, dict) and event.get("type") == "redis_minute_gap":
                        record(db, event.get("trading_day"), event.get("contract"), event.get("start_ms"))
            db.execute("INSERT OR REPLACE INTO cursors VALUES (?, ?, ?)", (*key, offset))


def seed_current_gaps(db, environment):
    config = json.loads((environment / "config/cta_cn_futures.json").read_text())
    port = int(config["bind"].rsplit(":", 1)[1])
    opener = build_opener(ProxyHandler({}))
    with opener.open(f"http://127.0.0.1:{port}/state", timeout=3) as response:
        state = json.load(response)
    if state.get("name") != config["name"]:
        raise ValueError("CTA status belongs to another instance")
    for report in (state, *state.get("products", {}).values()):
        dominant = report.get("dominant") or {}
        record(db, dominant.get("trading_day"), dominant.get("contract"), report.get("redis_gap_since_ms"))


def message(name, gaps):
    lines = [f"【国内 CTA 行情缺口】{name}"]
    for (day, start), rows in groupby(gaps, key=lambda gap: (gap[0], gap[2])):
        start_time = datetime.fromtimestamp(start / 1000, SHANGHAI)
        contracts = "、".join(row[1] for row in rows)
        lines.append(f"{start_time:%Y-%m-%d %H:%M} 北京时间（交易日 {day}） {contracts}")
    lines.append("盘中使用后续完整 Redis 分钟继续计算信号。休市后等待 Market Replay 回补并重算。")
    return "\n".join(lines) + "\n"


def deliver(db, name, sender, dry_run):
    attempts, retry_at = db.execute("SELECT attempts, retry_at FROM delivery WHERE id = 1").fetchone()
    if retry_at > time.time():
        return
    gaps = db.execute("""SELECT trading_day, contract, start_ms FROM gaps WHERE sent = 0
                         ORDER BY trading_day, start_ms, contract LIMIT 40""").fetchall()
    if not gaps:
        return
    notice = message(name, gaps)
    if dry_run:
        print(notice, end="")
        return
    try:
        # The shared sender exits successfully when the variable is missing.
        # Require it here so that a disabled sender cannot acknowledge a gap.
        if not os.environ.get("CN_FUTURES_DB_DINGTALK_WEBHOOK_URL", "").strip():
            raise RuntimeError("DingTalk webhook is unconfigured")
        subprocess.run(
            [sender], input=notice.encode("utf-8"), check=True, timeout=15,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError, RuntimeError):
        delay = min(15 * 2 ** min(attempts, 6), 600)
        with db:
            db.execute("UPDATE delivery SET attempts = ?, retry_at = ? WHERE id = 1",
                       (attempts + 1, int(time.time()) + delay))
        raise SystemExit(f"DingTalk delivery failed for {len(gaps)} gap(s), retry in {delay}s")
    with db:
        db.executemany("UPDATE gaps SET sent = 1 WHERE trading_day = ? AND contract = ? AND start_ms = ?", gaps)
        db.execute("UPDATE delivery SET attempts = 0, retry_at = 0 WHERE id = 1")
    print(f"DingTalk accepted {len(gaps)} Redis minute gap(s)", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", type=Path, default=Path.cwd())
    parser.add_argument("--sender", type=Path, default=os.environ.get("CTA_GAP_DINGTALK_SEND"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not args.dry_run and (args.sender is None or not args.sender.is_absolute()):
        parser.error("an absolute path to dingtalk-send is required")

    environment = args.environment.resolve()
    store = ":memory:" if args.dry_run else environment / "data/redis-gap-alert.sqlite3"
    db = sqlite3.connect(store)
    db.executescript(SCHEMA)
    # Commit the cursor and queued events together before attempting delivery.
    with db:
        bootstrap = db.execute("SELECT 1 FROM delivery").fetchone() is None
        ingest_logs(db, environment / "logs", bootstrap)
        if bootstrap:
            seed_current_gaps(db, environment)
            db.execute("INSERT INTO delivery VALUES (1, 0, 0)")
    deliver(db, environment.name, args.sender, args.dry_run)
    db.close()


if __name__ == "__main__":
    main()
