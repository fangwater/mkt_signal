#!/usr/bin/env python3
"""Pull hourly order_export_server snapshots from multiple envs into a local mirror."""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

LOG = logging.getLogger("order_export_puller")

ALLOWED_FILES = (
    "uniform_orders.parquet",
    "order_updates_unmatched.parquet",
    "trade_updates_unmatched.parquet",
)


@dataclass
class EnvCfg:
    name: str
    base_url: str
    timeout_sec: float


@dataclass
class ClientCfg:
    out_root: Path
    poll_minute: int
    http_timeout_sec: float
    envs: list[EnvCfg]


def load_config(path: Path) -> ClientCfg:
    with path.open("rb") as fp:
        raw = tomllib.load(fp)
    client_raw = raw.get("client", {})
    out_root = Path(client_raw["out_root"]).expanduser()
    poll_minute = int(client_raw.get("poll_minute", 5))
    if not 0 <= poll_minute <= 59:
        raise ValueError(f"poll_minute must be 0..59, got {poll_minute}")
    default_timeout = float(client_raw.get("http_timeout_sec", 30.0))
    envs_raw = raw.get("envs") or []
    if not envs_raw:
        raise ValueError("config must define at least one [[envs]] entry")
    envs: list[EnvCfg] = []
    seen: set[str] = set()
    for entry in envs_raw:
        name = entry["name"]
        if name in seen:
            raise ValueError(f"duplicate env name: {name}")
        seen.add(name)
        envs.append(
            EnvCfg(
                name=name,
                base_url=entry["base_url"].rstrip("/"),
                timeout_sec=float(entry.get("timeout_sec", default_timeout)),
            )
        )
    return ClientCfg(
        out_root=out_root,
        poll_minute=poll_minute,
        http_timeout_sec=default_timeout,
        envs=envs,
    )


def list_snapshots(env: EnvCfg) -> list[dict]:
    url = f"{env.base_url}/snapshots"
    with urllib.request.urlopen(url, timeout=env.timeout_sec) as resp:
        body = resp.read()
    snaps = json.loads(body.decode("utf-8"))
    if not isinstance(snaps, list):
        raise RuntimeError(f"{env.name}: /snapshots returned non-list payload")
    return snaps


def download_file(env: EnvCfg, name: str, file_name: str, dst: Path) -> int:
    url = f"{env.base_url}/snapshots/{urllib.parse.quote(name, safe='')}/{file_name}"
    tmp = dst.with_suffix(dst.suffix + ".part")
    try:
        with urllib.request.urlopen(url, timeout=env.timeout_sec) as resp, tmp.open("wb") as out:
            shutil.copyfileobj(resp, out)
        size = tmp.stat().st_size
        tmp.replace(dst)
        return size
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass


def sync_env(cfg: ClientCfg, env: EnvCfg) -> dict:
    env_dir = cfg.out_root / env.name
    env_dir.mkdir(parents=True, exist_ok=True)
    snaps = list_snapshots(env)
    new = skipped = failed = 0
    for snap in snaps:
        name = snap.get("name")
        if not isinstance(name, str) or "__" not in name:
            LOG.warning("[%s] skip malformed snapshot entry: %r", env.name, snap)
            continue
        files = snap.get("files") or list(ALLOWED_FILES)
        snap_dir = env_dir / name
        snap_dir.mkdir(parents=True, exist_ok=True)
        for f in files:
            if f not in ALLOWED_FILES:
                LOG.warning("[%s] %s: unexpected file %s, skipping", env.name, name, f)
                continue
            dst = snap_dir / f
            if dst.exists() and dst.stat().st_size > 0:
                skipped += 1
                continue
            try:
                size = download_file(env, name, f, dst)
                LOG.info("[%s] %s/%s downloaded %d bytes", env.name, name, f, size)
                new += 1
            except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
                LOG.warning("[%s] %s/%s download failed: %s", env.name, name, f, exc)
                failed += 1
    return {"snapshots": len(snaps), "new": new, "skipped": skipped, "failed": failed}


def run_once(cfg: ClientCfg) -> int:
    total_failed = 0
    for env in cfg.envs:
        try:
            stats = sync_env(cfg, env)
            LOG.info(
                "[%s] done snapshots=%d new=%d skipped=%d failed=%d",
                env.name, stats["snapshots"], stats["new"], stats["skipped"], stats["failed"],
            )
            total_failed += stats["failed"]
        except Exception as exc:
            LOG.error("[%s] sync aborted: %s", env.name, exc)
            total_failed += 1
    return total_failed


def sleep_until_next_run(poll_minute: int) -> None:
    now = datetime.now(timezone.utc)
    target = now.replace(minute=poll_minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(hours=1)
    delta = (target - now).total_seconds()
    LOG.info("sleeping %.1fs until %s UTC", delta, target.isoformat(timespec="seconds"))
    time.sleep(delta)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", required=True, type=Path)
    ap.add_argument("--once", action="store_true", help="run a single sync pass and exit")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    cfg = load_config(args.config)
    cfg.out_root.mkdir(parents=True, exist_ok=True)

    if args.once:
        return 0 if run_once(cfg) == 0 else 1

    LOG.info("loop mode, syncing %d env(s) at HH:%02d UTC", len(cfg.envs), cfg.poll_minute)
    run_once(cfg)
    while True:
        sleep_until_next_run(cfg.poll_minute)
        try:
            cfg = load_config(args.config)
            cfg.out_root.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            LOG.error("config reload failed, keeping previous config: %s", exc)
        run_once(cfg)


if __name__ == "__main__":
    sys.exit(main())
