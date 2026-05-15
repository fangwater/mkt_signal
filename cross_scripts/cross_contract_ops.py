#!/usr/bin/env python3
"""Unified cross-contract cancel/flatten entrypoint.

Cross mode policy:
  - Binance always uses STANDARD UM futures scripts.
  - OKX/Gate/Bybit/Bitget always use unified-account futures/swap scripts.
  - This entrypoint is futures-only; it never routes to spot/margin cleanup.
"""

from __future__ import annotations

import argparse
import os
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


SUPPORTED_EXCHANGES = {"binance", "okex", "gate", "bybit", "bitget"}
SIDE_NAMES = {"open", "hedge"}
ALL_NAMES = {"both", "all"}
CROSS_SIDE_DIR_RE = re.compile(
    r"^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross(?:[-_][a-z0-9][a-z0-9_-]*)[-_](open|hedge)$"
)
CROSS_DIR_RE = re.compile(r"^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross(?:[-_].*)?$")
FUTURES_VENUE_RE = re.compile(r"^([a-z0-9]+)-(futures|swap|perp|perpetual)$")

ENV_KEYS = {
    "OPEN_VENUE",
    "HEDGE_VENUE",
    "CROSS_SIDE",
    "IPC_NAMESPACE",
    "BINANCE_API_KEY",
    "BINANCE_API_SECRET",
    "BINANCE_ACCOUNT_MODE",
    "OKX_API_KEY",
    "OKX_API_SECRET",
    "OKX_PASSPHRASE",
    "GATE_API_KEY",
    "GATE_API_SECRET",
    "BYBIT_API_KEY",
    "BYBIT_API_SECRET",
    "BITGET_API_KEY",
    "BITGET_API_SECRET",
    "BITGET_API_PASSPHRASE",
    "BITGET_PASSPHRASE",
}


@dataclass(frozen=True)
class CrossContext:
    env_dir: Path
    open_venue: str
    hedge_venue: str
    open_exchange: str
    hedge_exchange: str
    side_scope: str

    @property
    def exchanges(self) -> list[str]:
        out: list[str] = []
        for exchange in (self.open_exchange, self.hedge_exchange):
            if exchange not in out:
                out.append(exchange)
        return out


def parse_args() -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(
        description=(
            "Cancel or flatten cross-mode futures legs. Binance is forced to "
            "STANDARD; non-Binance exchanges use unified-account futures APIs."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        allow_abbrev=False,
    )
    parser.add_argument(
        "action",
        choices=["cancel", "flatten", "flat", "flate"],
        help="Operation. flat/flate are aliases for flatten.",
    )
    parser.add_argument(
        "target",
        help="open, hedge, both/all, or exchange name: binance, okex/okx, gate, bybit, bitget.",
    )
    parser.add_argument(
        "--env-dir",
        default=None,
        help="Cross deploy directory. Defaults to cwd, or parent when cwd is cross_scripts.",
    )
    parser.add_argument("--symbol", action="append", default=[], help="Filter one symbol; repeatable.")
    parser.add_argument(
        "--symbols",
        action="append",
        default=[],
        help="Comma/space separated symbol filters, e.g. BTCUSDT,ETHUSDT.",
    )
    parser.add_argument("--execute", action="store_true", help="Actually submit cancels/orders.")
    parser.add_argument(
        "--python",
        default=sys.executable or "python3",
        help="Python interpreter used for child scripts.",
    )
    args, extra = parser.parse_known_args()
    if extra and extra[0] == "--":
        extra = extra[1:]
    blocked_extra = {
        "--scope": "cross_contract_ops is futures-only; scope is fixed to UM/SWAP/futures.",
        "--execute": "pass --execute to cross_contract_ops, not through child args.",
        "--cancel": "pass --execute to cross_contract_ops; OKX child --cancel is managed by the router.",
    }
    for flag, message in blocked_extra.items():
        if flag in extra:
            raise SystemExit(f"[ERROR] {message}")
    return args, extra


def normalize_exchange(value: str) -> str:
    exchange = (value or "").strip().lower().replace("_", "-")
    if exchange.startswith("okx"):
        exchange = "okex" + exchange[3:]
    if "-" in exchange:
        exchange = exchange.split("-", 1)[0]
    if exchange == "okx":
        exchange = "okex"
    return exchange


def normalize_venue(value: str) -> str:
    venue = (value or "").strip().lower().replace("_", "-")
    if venue.startswith("okx-"):
        venue = "okex-" + venue[4:]
    match = FUTURES_VENUE_RE.match(venue)
    if not match:
        raise SystemExit(
            f"[ERROR] invalid cross venue {value!r}; expected -futures/-swap/-perp/-perpetual."
        )
    exchange = normalize_exchange(match.group(1))
    if exchange not in SUPPORTED_EXCHANGES:
        raise SystemExit(f"[ERROR] unsupported cross exchange in venue {value!r}: {exchange}")
    return f"{exchange}-{match.group(2)}"


def default_env_dir() -> Path:
    cwd = Path.cwd()
    if cwd.name == "cross_scripts":
        return cwd.parent
    return cwd


def load_env_sh(env_dir: Path) -> None:
    env_path = env_dir / "env.sh"
    if not env_path.is_file():
        return
    cmd = f"set -a; source {shlex.quote(str(env_path))} >/dev/null 2>&1; env -0"
    proc = subprocess.run(
        ["bash", "-lc", cmd],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        print(f"[WARN] failed to source {env_path}", file=sys.stderr)
        return
    for item in proc.stdout.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key_b, value_b = item.split(b"=", 1)
        key = key_b.decode("utf-8", errors="ignore")
        value = value_b.decode("utf-8", errors="replace")
        if key in ENV_KEYS:
            old = os.environ.get(key)
            if old and old != value:
                print(
                    f"[WARN] env.sh overrides existing {key} from process env",
                    file=sys.stderr,
                )
            os.environ[key] = value
        elif key not in os.environ:
            os.environ[key] = value


def infer_from_dir(env_dir: Path) -> tuple[str, str, str]:
    name = env_dir.name.lower()
    match = CROSS_SIDE_DIR_RE.match(name)
    if match:
        open_exchange = normalize_exchange(match.group(1))
        hedge_exchange = normalize_exchange(match.group(2))
        side_scope = match.group(3) or ""
        return f"{open_exchange}-futures", f"{hedge_exchange}-futures", side_scope

    match = CROSS_DIR_RE.match(name)
    if not match:
        raise SystemExit(
            f"[ERROR] env dir name must look like <open>-<hedge>-cross-..., got {env_dir.name!r}."
        )
    open_exchange = normalize_exchange(match.group(1))
    hedge_exchange = normalize_exchange(match.group(2))
    return f"{open_exchange}-futures", f"{hedge_exchange}-futures", ""


def build_context(env_dir_arg: str | None) -> CrossContext:
    env_dir = Path(env_dir_arg).expanduser().resolve() if env_dir_arg else default_env_dir().resolve()
    load_env_sh(env_dir)

    inferred_open, inferred_hedge, inferred_side = infer_from_dir(env_dir)
    open_venue = normalize_venue(os.environ.get("OPEN_VENUE") or inferred_open)
    hedge_venue = normalize_venue(os.environ.get("HEDGE_VENUE") or inferred_hedge)
    side_scope = (os.environ.get("CROSS_SIDE") or inferred_side or "").strip().lower()

    open_exchange = normalize_exchange(open_venue)
    hedge_exchange = normalize_exchange(hedge_venue)
    dir_open_exchange = normalize_exchange(inferred_open)
    dir_hedge_exchange = normalize_exchange(inferred_hedge)
    if (open_exchange, hedge_exchange) != (dir_open_exchange, dir_hedge_exchange):
        raise SystemExit(
            "[ERROR] env.sh OPEN_VENUE/HEDGE_VENUE do not match env dir: "
            f"env=({open_venue},{hedge_venue}) dir=({inferred_open},{inferred_hedge})."
        )
    if open_exchange == hedge_exchange:
        raise SystemExit(f"[ERROR] cross contract mode requires different exchanges: {open_exchange}")
    if side_scope and side_scope not in SIDE_NAMES:
        raise SystemExit(f"[ERROR] invalid CROSS_SIDE={side_scope!r}; expected open/hedge or empty.")

    return CrossContext(
        env_dir=env_dir,
        open_venue=open_venue,
        hedge_venue=hedge_venue,
        open_exchange=open_exchange,
        hedge_exchange=hedge_exchange,
        side_scope=side_scope,
    )


def parse_symbols(symbols_args: Iterable[str], symbol_args: Iterable[str]) -> list[str]:
    values: list[str] = []
    for chunk in symbols_args:
        values.extend(part for part in re.split(r"[\s,]+", chunk.strip()) if part)
    values.extend(symbol_args)

    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        symbol = canonical_symbol(raw)
        if symbol and symbol not in seen:
            seen.add(symbol)
            out.append(symbol)
    return out


def canonical_symbol(raw: str) -> str:
    value = (raw or "").strip().upper()
    if not value:
        return ""
    if "@" in value:
        value = value.split("@", 1)[0].strip()
    match = re.match(r"^([A-Z0-9]+)-USDT-SWAP$", value)
    if match:
        return f"{match.group(1)}USDT"
    match = re.match(r"^([A-Z0-9]+)[-_]USDT$", value)
    if match:
        return f"{match.group(1)}USDT"
    return re.sub(r"[^A-Z0-9]", "", value)


def okx_inst_id(symbol: str) -> str:
    if not symbol.endswith("USDT") or len(symbol) <= 4:
        raise SystemExit(f"[ERROR] OKX cross script only supports USDT symbols, got {symbol!r}.")
    return f"{symbol[:-4]}-USDT-SWAP"


def resolve_targets(ctx: CrossContext, target: str) -> list[str]:
    raw = target.strip().lower().replace("_", "-")
    raw = "okex" if raw == "okx" else raw

    if raw in SIDE_NAMES:
        if ctx.side_scope and raw != ctx.side_scope:
            raise SystemExit(
                f"[ERROR] this env is side-scoped to {ctx.side_scope}; target {raw!r} is not allowed."
            )
        return [ctx.open_exchange if raw == "open" else ctx.hedge_exchange]

    if raw in ALL_NAMES:
        if ctx.side_scope == "open":
            print("[info] side-scoped env CROSS_SIDE=open; all resolves to open side only.")
            return [ctx.open_exchange]
        if ctx.side_scope == "hedge":
            print("[info] side-scoped env CROSS_SIDE=hedge; all resolves to hedge side only.")
            return [ctx.hedge_exchange]
        return ctx.exchanges

    exchange = normalize_exchange(raw)
    if exchange not in SUPPORTED_EXCHANGES:
        raise SystemExit(f"[ERROR] unsupported target {target!r}.")
    if exchange not in ctx.exchanges:
        raise SystemExit(
            f"[ERROR] target exchange {exchange} is not in this cross pair: "
            f"{ctx.open_exchange}/{ctx.hedge_exchange}."
        )
    if ctx.side_scope == "open" and exchange != ctx.open_exchange:
        raise SystemExit(
            f"[ERROR] this env is side-scoped to open({ctx.open_exchange}); target {exchange} is not allowed."
        )
    if ctx.side_scope == "hedge" and exchange != ctx.hedge_exchange:
        raise SystemExit(
            f"[ERROR] this env is side-scoped to hedge({ctx.hedge_exchange}); target {exchange} is not allowed."
        )
    return [exchange]


def require_env(name: str) -> None:
    if not os.environ.get(name, "").strip():
        raise SystemExit(f"[ERROR] missing required env var: {name}")


def validate_exchange_env(exchange: str) -> None:
    if exchange == "binance":
        mode = os.environ.get("BINANCE_ACCOUNT_MODE", "").strip().upper()
        if mode and mode not in {"STANDARD", "STD"}:
            raise SystemExit(
                f"[ERROR] cross Binance must be STANDARD, got BINANCE_ACCOUNT_MODE={mode!r}."
            )
        os.environ["BINANCE_ACCOUNT_MODE"] = "STANDARD"
        require_env("BINANCE_API_KEY")
        require_env("BINANCE_API_SECRET")
        return

    if exchange == "okex":
        require_env("OKX_API_KEY")
        require_env("OKX_API_SECRET")
        require_env("OKX_PASSPHRASE")
    elif exchange == "gate":
        require_env("GATE_API_KEY")
        require_env("GATE_API_SECRET")
    elif exchange == "bybit":
        require_env("BYBIT_API_KEY")
        require_env("BYBIT_API_SECRET")
    elif exchange == "bitget":
        if not os.environ.get("BITGET_API_PASSPHRASE") and os.environ.get("BITGET_PASSPHRASE"):
            os.environ["BITGET_API_PASSPHRASE"] = os.environ["BITGET_PASSPHRASE"]
        if not os.environ.get("BITGET_PASSPHRASE") and os.environ.get("BITGET_API_PASSPHRASE"):
            os.environ["BITGET_PASSPHRASE"] = os.environ["BITGET_API_PASSPHRASE"]
        require_env("BITGET_API_KEY")
        require_env("BITGET_API_SECRET")
        require_env("BITGET_API_PASSPHRASE")
    else:
        raise SystemExit(f"[ERROR] unsupported exchange: {exchange}")


def find_script(script_name: str) -> Path:
    script_dir = Path(__file__).resolve().parent
    candidates = [
        script_dir / script_name,
        script_dir.parent / "scripts" / script_name,
        script_dir.parent / "cross_scripts" / script_name,
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    searched = ", ".join(str(path) for path in candidates)
    raise SystemExit(f"[ERROR] required child script not found: {script_name}; searched {searched}")


def add_symbol_args(cmd: list[str], symbols: list[str], *, repeated: bool) -> None:
    if not symbols:
        return
    if repeated:
        for symbol in symbols:
            cmd.extend(["--symbol", symbol])
    else:
        cmd.extend(["--symbols", ",".join(symbols)])


def build_commands(
    *,
    action: str,
    exchange: str,
    symbols: list[str],
    execute: bool,
    env_dir: Path,
    python_bin: str,
    extra: list[str],
) -> list[list[str]]:
    normalized_action = "flatten" if action in {"flat", "flate"} else action
    commands: list[list[str]] = []

    if exchange == "binance":
        script = "binance_cancel_all_std_um_ws_orders.py" if normalized_action == "cancel" else "flatten_binance_std_um.py"
        cmd = [python_bin, str(find_script(script)), "--env-dir", str(env_dir)]
        add_symbol_args(cmd, symbols, repeated=(normalized_action == "cancel"))
        if execute:
            cmd.append("--execute")
        cmd.extend(extra)
        return [cmd]

    if exchange == "okex":
        if normalized_action == "cancel":
            base = [python_bin, str(find_script("okx_swap_open_orders.py")), "--fetch-all"]
            if execute:
                base.append("--cancel")
            if symbols:
                for symbol in symbols:
                    commands.append([*base, "--inst-id", okx_inst_id(symbol), *extra])
            else:
                commands.append([*base, *extra])
            return commands

        cmd = [python_bin, str(find_script("flatten_okx_swap_exposure.py"))]
        add_symbol_args(cmd, symbols, repeated=False)
        if execute:
            cmd.append("--execute")
        cmd.extend(extra)
        return [cmd]

    script_by_exchange = {
        ("gate", "cancel"): "gate_cancel_all_um_orders.py",
        ("gate", "flatten"): "flatten_gate_um_exposure.py",
        ("bybit", "cancel"): "bybit_cancel_all_um_orders.py",
        ("bybit", "flatten"): "flatten_bybit_um_exposure.py",
        ("bitget", "cancel"): "bitget_cancel_all_um_orders.py",
        ("bitget", "flatten"): "flatten_bitget_um_exposure.py",
    }
    script = script_by_exchange.get((exchange, normalized_action))
    if not script:
        raise SystemExit(f"[ERROR] unsupported operation: {normalized_action} {exchange}")
    cmd = [python_bin, str(find_script(script))]
    add_symbol_args(cmd, symbols, repeated=False)
    if execute:
        cmd.append("--execute")
    cmd.extend(extra)
    return [cmd]


def run_child(label: str, cmd: list[str], env_dir: Path) -> int:
    print()
    print(f"[INFO] === {label} ===")
    print("[RUN] " + " ".join(shlex.quote(part) for part in cmd))
    sys.stdout.flush()
    proc = subprocess.run(cmd, cwd=str(env_dir), env=os.environ.copy(), check=False)
    return proc.returncode


def main() -> int:
    args, extra = parse_args()
    ctx = build_context(args.env_dir)
    symbols = parse_symbols(args.symbols, args.symbol)
    targets = resolve_targets(ctx, args.target)

    for exchange in targets:
        validate_exchange_env(exchange)

    print(
        "[info] cross env="
        f"{ctx.env_dir.name} open={ctx.open_venue} hedge={ctx.hedge_venue} "
        f"target={','.join(targets)} action={args.action} execute={args.execute}"
    )
    if symbols:
        print(f"[info] symbols={','.join(symbols)}")
    for exchange in targets:
        mode = "STANDARD" if exchange == "binance" else "UNIFORM"
        print(f"[info] account_policy {exchange}={mode}")
    sys.stdout.flush()

    exit_code = 0
    for exchange in targets:
        commands = build_commands(
            action=args.action,
            exchange=exchange,
            symbols=symbols,
            execute=args.execute,
            env_dir=ctx.env_dir,
            python_bin=args.python,
            extra=extra,
        )
        for cmd in commands:
            label = f"{args.action} {exchange}"
            exit_code = max(exit_code, run_child(label, cmd, ctx.env_dir))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
