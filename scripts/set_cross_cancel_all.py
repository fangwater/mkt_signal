#!/usr/bin/env python3
"""Cancel all open USDT futures orders on BOTH sides of a cross arb environment.

Mirrors set_cross_futures_leverage.py: parses ``<open>-<hedge>-cross-<suffix>``,
sources env.sh from the env dir, unions the online symbols from Redis:

  cross_dump_symbols:{open_ex}-{hedge_ex}
  cross_fwd_trade_symbols:{open_ex}-{hedge_ex}
  cross_bwd_trade_symbols:{open_ex}-{hedge_ex}
  {env_name}:cross_unimmr_close_symbols:{open_venue}_{hedge_venue}

and then delegates to the per-exchange cancel scripts for BOTH the open and the
hedge exchange (双边). Default is dry-run; add ``--execute`` to actually cancel.

Examples:
  scripts/set_cross_cancel_all.py --env-name bitget-gate-cross-arb01
  scripts/set_cross_cancel_all.py --env-name bitget-gate-cross-arb01 --execute
  scripts/set_cross_cancel_all.py --env-name okex-binance-cross-trade --symbol BTC,ETH --execute
  scripts/set_cross_cancel_all.py --env-name bitget-gate-cross-arb01 --side open --execute
  # 不按 redis symbol 过滤，整账户全部撤单
  scripts/set_cross_cancel_all.py --env-name bitget-gate-cross-arb01 --all-symbols --execute
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, List, Optional, Set, Tuple

SUPPORTED_EXCHANGES = {"binance", "okex", "gate", "bybit", "bitget"}
NAMESPACE = "cross"


@dataclass(frozen=True)
class CrossContext:
    env_name: str
    env_dir: Path
    open_ex: str
    hedge_ex: str
    open_venue: str
    hedge_venue: str
    suffix: str

    @property
    def key_suffix(self) -> str:
        return f"{self.open_ex}-{self.hedge_ex}"

    @property
    def unimmr_close_suffix(self) -> str:
        return f"{self.open_venue}_{self.hedge_venue}"


def normalize_exchange(value: str) -> str:
    text = (value or "").strip().lower()
    return "okex" if text == "okx" else text


def exchange_from_venue(venue: str) -> str:
    raw = (venue or "").strip().lower()
    if not raw or "-" not in raw:
        return ""
    return normalize_exchange(raw.split("-", 1)[0])


def parse_cross_env_name(value: str) -> Tuple[str, str, str]:
    text = (value or "").strip().lower()
    match = re.match(
        r"^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross[-_]([a-z0-9][a-z0-9_-]*?)(?:[-_](?:open|hedge))?$",
        text,
    )
    if not match:
        raise SystemExit(f"env-name must match <open>-<hedge>-cross-<suffix>: {value}")
    open_ex = normalize_exchange(match.group(1))
    hedge_ex = normalize_exchange(match.group(2))
    suffix = match.group(3)
    if open_ex == hedge_ex:
        raise SystemExit(f"cross requires distinct exchanges: {value}")
    if open_ex not in SUPPORTED_EXCHANGES or hedge_ex not in SUPPORTED_EXCHANGES:
        raise SystemExit(f"unsupported cross exchanges in env name: {value}")
    return open_ex, hedge_ex, suffix


def load_env_file(path: Path) -> None:
    if not path.is_file():
        print(f"[warn] env file not found: {path}", file=sys.stderr)
        return
    env = dict(os.environ)
    env["ENV_FILE"] = str(path)
    try:
        out = subprocess.check_output(
            ["bash", "-lc", 'set -a; source "$ENV_FILE"; env -0'],
            env=env,
        )
    except subprocess.CalledProcessError as exc:
        raise SystemExit(f"failed to source env file {path}: exit={exc.returncode}") from exc
    for item in out.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key, value = item.split(b"=", 1)
        try:
            os.environ[key.decode("utf-8")] = value.decode("utf-8")
        except UnicodeDecodeError:
            continue


def resolve_context(args: argparse.Namespace) -> CrossContext:
    env_name = (args.env_name or "").strip().lower()
    env_dir_arg = (args.env_dir or "").strip()
    if not env_name and env_dir_arg:
        env_name = os.path.basename(env_dir_arg.rstrip("/")).strip().lower()
    if not env_name:
        cwd_name = os.path.basename(os.getcwd()).strip().lower()
        if re.match(r"^[a-z0-9]+[-_][a-z0-9]+[-_]cross[-_]", cwd_name):
            env_name = cwd_name
    if not env_name:
        raise SystemExit(
            "missing env name; pass --env-name <open>-<hedge>-cross-<suffix> or run from the env dir"
        )

    open_ex, hedge_ex, suffix = parse_cross_env_name(env_name)
    env_dir = Path(env_dir_arg) if env_dir_arg else Path.home() / env_name
    if not args.no_env_sh:
        load_env_file(env_dir / "env.sh")

    open_venue = (args.open_venue or os.environ.get("OPEN_VENUE") or f"{open_ex}-futures").strip().lower()
    hedge_venue = (args.hedge_venue or os.environ.get("HEDGE_VENUE") or f"{hedge_ex}-futures").strip().lower()
    if exchange_from_venue(open_venue) != open_ex or exchange_from_venue(hedge_venue) != hedge_ex:
        raise SystemExit(
            f"open/hedge venue does not match env name: open={open_venue} hedge={hedge_venue} env={env_name}"
        )

    return CrossContext(
        env_name=env_name,
        env_dir=env_dir,
        open_ex=open_ex,
        hedge_ex=hedge_ex,
        open_venue=open_venue,
        hedge_venue=hedge_venue,
        suffix=suffix,
    )


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def redis_client(args: argparse.Namespace):
    redis = try_import_redis()
    if redis is None:
        raise SystemExit("redis package is not installed; run pip install redis")
    host = args.redis_host or os.environ.get("REDIS_HOST", "127.0.0.1")
    port = args.redis_port if args.redis_port is not None else int(os.environ.get("REDIS_PORT", "6379"))
    db = args.redis_db if args.redis_db is not None else int(os.environ.get("REDIS_DB", "0"))
    password = args.redis_password if args.redis_password is not None else os.environ.get("REDIS_PASSWORD", "")
    return redis.Redis(host=host, port=port, db=db, password=password or None)


def decode_redis_list(raw: Any, key: str) -> List[str]:
    if not raw:
        return []
    text = raw.decode("utf-8", "ignore") if isinstance(raw, (bytes, bytearray)) else str(raw)
    try:
        parsed = json.loads(text)
    except Exception as exc:
        print(f"[warn] failed to parse Redis list {key}: {exc}", file=sys.stderr)
        return []
    if not isinstance(parsed, list):
        print(f"[warn] Redis key is not a JSON list: {key}", file=sys.stderr)
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


def normalize_asset(value: str) -> str:
    text = (value or "").strip().upper()
    if not text:
        return ""
    if "@" in text:
        text = text.split("@", 1)[0].strip()
    if text.endswith("-USDT-SWAP"):
        text = text[: -len("-USDT-SWAP")]
    elif re.match(r"^[A-Z0-9]+-USDT-\d{6,8}$", text):
        text = text.split("-USDT-", 1)[0]
    elif text.endswith("-USDT"):
        text = text[: -len("-USDT")]
    elif text.endswith("_USDT"):
        text = text[: -len("_USDT")]
    else:
        cleaned = re.sub(r"[^A-Z0-9]+", "", text)
        if cleaned.endswith("USDT") and len(cleaned) > 4:
            text = cleaned[: -len("USDT")]
        else:
            text = cleaned
    return re.sub(r"[^A-Z0-9]+", "", text)


def symbol_for_exchange(exchange: str, asset: str) -> str:
    base = normalize_asset(asset)
    if not base:
        return ""
    if exchange == "okex":
        return f"{base}-USDT-SWAP"
    if exchange == "gate":
        return f"{base}_USDT"
    return f"{base}USDT"


def cross_symbol_keys(ctx: CrossContext) -> List[str]:
    suffix = ctx.key_suffix
    return [
        f"{NAMESPACE}_dump_symbols:{suffix}",
        f"{NAMESPACE}_fwd_trade_symbols:{suffix}",
        f"{NAMESPACE}_bwd_trade_symbols:{suffix}",
        f"{ctx.env_name}:{NAMESPACE}_unimmr_close_symbols:{ctx.unimmr_close_suffix}",
    ]


def load_online_assets(rds: Any, ctx: CrossContext) -> List[str]:
    assets: Set[str] = set()
    for key in cross_symbol_keys(ctx):
        values = decode_redis_list(rds.get(key), key)
        print(f"[redis] {key}: {len(values)}")
        for value in values:
            asset = normalize_asset(value)
            if asset:
                assets.add(asset)
    return sorted(assets)


def parse_symbol_args(values: Iterable[str]) -> List[str]:
    out: Set[str] = set()
    for value in values:
        for part in re.split(r"[\s,]+", (value or "").strip()):
            asset = normalize_asset(part)
            if asset:
                out.add(asset)
    return sorted(out)


def find_canceler(name: str) -> Path:
    here = Path(__file__).resolve().parent
    candidates = [
        here / name,                       # sibling (deployed cross_scripts/)
        here.parent / "scripts" / name,   # repo root: scripts/<name>
        here / "scripts" / name,
    ]
    for path in candidates:
        if path.is_file():
            return path
    raise SystemExit(f"cancel helper not found: {name} (looked under {candidates})")


def build_command(
    exchange: str,
    assets: List[str],
    *,
    env_dir: Path,
    execute: bool,
    all_symbols: bool,
    extra: List[str],
) -> List[List[str]]:
    python_bin = sys.executable or "python3"
    if exchange == "binance":
        script = find_canceler("binance_cancel_all_std_um_ws_orders.py")
        cmd = [python_bin, str(script), "--env-dir", str(env_dir)]
        if not all_symbols:
            for asset in assets:
                cmd.extend(["--symbol", symbol_for_exchange("binance", asset)])
        if execute:
            cmd.append("--execute")
        cmd.extend(extra)
        return [cmd]

    if exchange == "okex":
        script = find_canceler("okx_swap_open_orders.py")
        base = [python_bin, str(script), "--fetch-all"]
        if execute:
            base.append("--cancel")
        if all_symbols or not assets:
            return [[*base, *extra]]
        # OKX helper accepts one --inst-id per call; loop per symbol.
        return [[*base, "--inst-id", symbol_for_exchange("okex", asset), *extra] for asset in assets]

    helper = {
        "gate": "gate_cancel_all_um_orders.py",
        "bybit": "bybit_cancel_all_um_orders.py",
        "bitget": "bitget_cancel_all_um_orders.py",
    }.get(exchange)
    if not helper:
        raise SystemExit(f"unsupported exchange: {exchange}")
    script = find_canceler(helper)
    cmd = [python_bin, str(script)]
    if not all_symbols and assets:
        cmd.extend(["--symbols", ",".join(symbol_for_exchange(exchange, a) for a in assets)])
    if execute:
        cmd.append("--execute")
    cmd.extend(extra)
    return [cmd]


def run_side(
    exchange: str,
    assets: List[str],
    *,
    args: argparse.Namespace,
    ctx: CrossContext,
    extra: List[str],
) -> int:
    commands = build_command(
        exchange,
        assets,
        env_dir=ctx.env_dir,
        execute=args.execute,
        all_symbols=args.all_symbols,
        extra=extra,
    )
    print(
        f"\n[info] === side exchange={exchange} symbols={len(assets) if not args.all_symbols else 'ALL'} "
        f"execute={args.execute} cmds={len(commands)} ==="
    )
    failures = 0
    for cmd in commands:
        print("[RUN] " + " ".join(shlex.quote(part) for part in cmd))
        sys.stdout.flush()
        if not args.execute and args.dry_run_skip:
            continue
        proc = subprocess.run(cmd, cwd=str(ctx.env_dir), env=os.environ.copy(), check=False)
        if proc.returncode != 0:
            failures += 1
            print(f"  [ERR] exit={proc.returncode}")
    return failures


def select_sides(ctx: CrossContext, side: str) -> List[str]:
    side = (side or "both").strip().lower()
    if side == "open":
        return [ctx.open_ex]
    if side == "hedge":
        return [ctx.hedge_ex]
    if side == "both":
        return list(dict.fromkeys([ctx.open_ex, ctx.hedge_ex]))
    raise SystemExit(f"invalid --side: {side}")


def parse_args() -> Tuple[argparse.Namespace, List[str]]:
    parser = argparse.ArgumentParser(
        description="Cancel all open USDT futures orders on BOTH sides of a cross arb env.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--env-name", default="", help="Env name, e.g. bitget-gate-cross-arb01.")
    parser.add_argument("--env-dir", default="", help="Env directory containing env.sh.")
    parser.add_argument("--open-venue", default="", help="Override open venue (e.g. bitget-futures).")
    parser.add_argument("--hedge-venue", default="", help="Override hedge venue (e.g. gate-futures).")
    parser.add_argument("--symbol", action="append", default=[], help="Asset/symbol filter; repeatable or CSV.")
    parser.add_argument(
        "--all-symbols",
        action="store_true",
        help="Do not filter by Redis online symbols; cancel ALL open orders on each side.",
    )
    parser.add_argument(
        "--side",
        choices=["both", "open", "hedge"],
        default="both",
        help="Which side(s) to cancel on. Default: both.",
    )
    parser.add_argument("--redis-host", default="")
    parser.add_argument("--redis-port", type=int, default=None)
    parser.add_argument("--redis-db", type=int, default=None)
    parser.add_argument("--redis-password", default=None)
    parser.add_argument("--no-env-sh", action="store_true", help="Do not auto-source env.sh.")
    parser.add_argument(
        "--dry-run-skip",
        action="store_true",
        help="On dry-run, only print commands without invoking the per-exchange helpers.",
    )
    parser.add_argument("--execute", action="store_true", help="Actually submit cancellations.")
    return parser.parse_known_args()


def main() -> int:
    args, extra = parse_args()
    ctx = resolve_context(args)
    sides = select_sides(ctx, args.side)

    assets: List[str] = []
    if not args.all_symbols:
        assets = parse_symbol_args(args.symbol)
        if assets:
            print(f"[info] using CLI symbols: {len(assets)}")
        else:
            rds = redis_client(args)
            assets = load_online_assets(rds, ctx)
        if not assets:
            raise SystemExit("no symbols selected; pass --symbol or use --all-symbols")

    print(
        f"[info] env={ctx.env_name} open={ctx.open_venue} hedge={ctx.hedge_venue} "
        f"key_suffix={ctx.key_suffix} sides={sides} all_symbols={args.all_symbols} "
        f"assets={len(assets)} execute={args.execute}"
    )

    failures = 0
    for exchange in sides:
        failures += run_side(exchange, assets, args=args, ctx=ctx, extra=list(extra))

    if failures:
        print(f"WARN: {failures} cancel side(s) reported failure", file=sys.stderr)
        return 1
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
