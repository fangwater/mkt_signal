#!/usr/bin/env python3
"""Shared helpers for Bitget position-tier sidecar pool scripts."""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple


DEFAULT_POOL_KEY = "bitget_position_tier_pool:envs"
DEFAULT_CACHE_KEY = "bitget_position_tier_cache:USDT-FUTURES"
POOL_VERSION = 1
SUPPORTED_EXCHANGES = {"binance", "okex", "gate", "bybit", "bitget"}


@dataclass(frozen=True)
class EnvSpec:
    env_name: str
    mode: str
    open_exchange: str
    hedge_exchange: str
    open_venue: str
    hedge_venue: str
    env_dir: str = ""

    @property
    def has_bitget(self) -> bool:
        return self.open_exchange == "bitget" or self.hedge_exchange == "bitget"

    def has_bitget_product(self, product_type: str) -> bool:
        product_type = (product_type or "USDT-FUTURES").strip().upper()
        expected = (
            "bitget-coin-futures"
            if product_type == "COIN-FUTURES"
            else "bitget-futures"
        )
        return self.open_venue == expected or self.hedge_venue == expected

    @property
    def bitget_side(self) -> str:
        if self.open_exchange == "bitget" and self.hedge_exchange == "bitget":
            return "both"
        if self.open_exchange == "bitget":
            return "open"
        if self.hedge_exchange == "bitget":
            return "hedge"
        return "none"

    def redis_record(self, *, updated_at_ms: Optional[int] = None) -> Dict[str, Any]:
        return {
            "env_name": self.env_name,
            "mode": self.mode,
            "open_exchange": self.open_exchange,
            "hedge_exchange": self.hedge_exchange,
            "open_venue": self.open_venue,
            "hedge_venue": self.hedge_venue,
            "bitget_side": self.bitget_side,
            "env_dir": self.env_dir,
            "updated_at_ms": updated_at_ms if updated_at_ms is not None else now_ms(),
        }


@dataclass(frozen=True)
class EnvSymbols:
    spec: EnvSpec
    keys: Tuple[str, ...]
    key_counts: Dict[str, int]
    assets: Tuple[str, ...]
    bitget_symbols: Tuple[str, ...]


def now_ms() -> int:
    return int(time.time() * 1000)


def normalize_exchange(value: str) -> str:
    text = (value or "").strip().lower()
    return "okex" if text == "okx" else text


def exchange_from_venue(value: str) -> str:
    text = (value or "").strip().lower()
    if not text or "-" not in text:
        return ""
    return normalize_exchange(text.split("-", 1)[0])


def exchange_defaults(exchange: str) -> Tuple[str, str]:
    ex = normalize_exchange(exchange)
    if ex not in SUPPORTED_EXCHANGES:
        raise ValueError(f"unsupported exchange: {exchange}")
    return f"{ex}-margin", f"{ex}-futures"


def normalize_asset(value: str) -> str:
    text = (value or "").strip().upper()
    if not text:
        return ""
    if "@" in text:
        text = text.split("@", 1)[0].strip()
    cleaned = re.sub(r"[^A-Z0-9]+", "", text)
    if cleaned.endswith("USDCM") and len(cleaned) > len("USDCM"):
        text = cleaned[: -len("USDCM")]
    elif text.endswith("-USDT-SWAP"):
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


def bitget_symbol(asset_or_symbol: str, product_type: str = "USDT-FUTURES") -> str:
    asset = normalize_asset(asset_or_symbol)
    if not asset:
        return ""
    if (product_type or "").strip().upper() == "COIN-FUTURES":
        return f"{asset}USD_CM"
    return f"{asset}USDT"


def parse_env_name(env_name: str) -> Tuple[str, str, str, str]:
    text = (env_name or "").strip().lower()
    cross = re.match(
        r"^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross[-_]([a-z0-9][a-z0-9_-]*?)(?:[-_](?:open|hedge))?$",
        text,
    )
    if cross:
        open_ex = normalize_exchange(cross.group(1))
        hedge_ex = normalize_exchange(cross.group(2))
        if open_ex == hedge_ex:
            raise ValueError(f"cross requires distinct exchanges: {env_name}")
        if open_ex not in SUPPORTED_EXCHANGES or hedge_ex not in SUPPORTED_EXCHANGES:
            raise ValueError(f"unsupported cross exchanges in env name: {env_name}")
        return "cross", open_ex, hedge_ex, cross.group(3)

    for mode in ("fr", "intra"):
        match = re.match(rf"^([a-z0-9]+)[-_]{mode}[-_][a-z0-9][a-z0-9_-]*$", text)
        if match:
            exchange = normalize_exchange(match.group(1))
            if exchange not in SUPPORTED_EXCHANGES:
                raise ValueError(f"unsupported exchange in env name: {env_name}")
            return mode, exchange, exchange, ""

    raise ValueError(
        "env-name must match <exchange>-intra-<suffix>, "
        "<exchange>_fr_<suffix>, or <open>-<hedge>-cross-<suffix>: "
        f"{env_name}"
    )


def load_env_file(env_dir: str) -> Dict[str, str]:
    env_path = Path(env_dir) / "env.sh"
    if not env_path.is_file():
        return {}
    env = dict(os.environ)
    env["ENV_FILE"] = str(env_path)
    out = subprocess.check_output(
        ["bash", "-lc", 'set -a; source "$ENV_FILE" >/dev/null 2>&1; env -0'],
        env=env,
    )
    loaded: Dict[str, str] = {}
    for item in out.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key_b, value_b = item.split(b"=", 1)
        try:
            loaded[key_b.decode("utf-8")] = value_b.decode("utf-8")
        except UnicodeDecodeError:
            continue
    return loaded


def resolve_env_spec(
    env_name: str,
    *,
    home_dir: str,
    no_env_sh: bool = False,
    env_dir: str = "",
) -> EnvSpec:
    normalized = (env_name or "").strip().lower()
    if not normalized:
        raise ValueError("empty env name")

    mode, open_ex, hedge_ex, _suffix = parse_env_name(normalized)
    resolved_env_dir = env_dir.strip() if env_dir else str(Path(home_dir).expanduser() / normalized)
    loaded_env = {} if no_env_sh else load_env_file(resolved_env_dir)

    if mode == "cross":
        open_venue = (loaded_env.get("OPEN_VENUE") or f"{open_ex}-futures").strip().lower()
        hedge_venue = (loaded_env.get("HEDGE_VENUE") or f"{hedge_ex}-futures").strip().lower()
        resolved_open_ex = exchange_from_venue(open_venue) or open_ex
        resolved_hedge_ex = exchange_from_venue(hedge_venue) or hedge_ex
        if resolved_open_ex != open_ex or resolved_hedge_ex != hedge_ex:
            raise ValueError(
                f"venue exchange mismatch for {normalized}: "
                f"open={open_venue} hedge={hedge_venue}"
            )
    else:
        default_open, default_hedge = exchange_defaults(open_ex)
        open_venue = (loaded_env.get("OPEN_VENUE") or default_open).strip().lower()
        hedge_venue = (loaded_env.get("HEDGE_VENUE") or default_hedge).strip().lower()

    return EnvSpec(
        env_name=normalized,
        mode=mode,
        open_exchange=open_ex,
        hedge_exchange=hedge_ex,
        open_venue=open_venue,
        hedge_venue=hedge_venue,
        env_dir=resolved_env_dir,
    )


def env_spec_from_record(record: Dict[str, Any]) -> EnvSpec:
    env_name = str(record.get("env_name") or "").strip().lower()
    mode = str(record.get("mode") or "").strip().lower()
    open_exchange = normalize_exchange(str(record.get("open_exchange") or ""))
    hedge_exchange = normalize_exchange(str(record.get("hedge_exchange") or ""))
    open_venue = str(record.get("open_venue") or "").strip().lower()
    hedge_venue = str(record.get("hedge_venue") or "").strip().lower()
    env_dir = str(record.get("env_dir") or "").strip()
    if not all([env_name, mode, open_exchange, hedge_exchange, open_venue, hedge_venue]):
        raise ValueError(f"invalid pool env record: {record!r}")
    return EnvSpec(
        env_name=env_name,
        mode=mode,
        open_exchange=open_exchange,
        hedge_exchange=hedge_exchange,
        open_venue=open_venue,
        hedge_venue=hedge_venue,
        env_dir=env_dir,
    )


def read_env_names_from_file(path: str) -> List[str]:
    text = Path(path).read_text(encoding="utf-8")
    stripped = text.strip()
    if not stripped:
        return []
    if stripped[0] in "[{":
        parsed = json.loads(stripped)
        if isinstance(parsed, dict):
            parsed = parsed.get("envs", [])
        if not isinstance(parsed, list):
            raise ValueError(f"env set file must contain a JSON list or object.envs: {path}")
        names: List[str] = []
        for item in parsed:
            if isinstance(item, dict):
                value = item.get("env_name") or item.get("name") or item.get("env")
            else:
                value = item
            if str(value or "").strip():
                names.append(str(value).strip())
        return names

    names = []
    for line in text.splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            names.extend(part for part in re.split(r"[\s,]+", line) if part)
    return names


def dedup_env_specs(specs: Iterable[EnvSpec]) -> List[EnvSpec]:
    by_name: Dict[str, EnvSpec] = {}
    for spec in specs:
        by_name[spec.env_name] = spec
    return [by_name[name] for name in sorted(by_name)]


def build_pool_payload(specs: Sequence[EnvSpec]) -> Dict[str, Any]:
    ts = now_ms()
    return {
        "version": POOL_VERSION,
        "updated_at_ms": ts,
        "envs": [spec.redis_record(updated_at_ms=ts) for spec in specs],
    }


def decode_redis_json(raw: Any, key: str) -> Any:
    if raw is None:
        raise KeyError(f"Redis key not found: {key}")
    text = raw.decode("utf-8", "ignore") if isinstance(raw, (bytes, bytearray)) else str(raw)
    return json.loads(text)


def load_pool_from_redis(rds: Any, key: str = DEFAULT_POOL_KEY) -> List[EnvSpec]:
    payload = decode_redis_json(rds.get(key), key)
    envs = payload.get("envs") if isinstance(payload, dict) else payload
    if not isinstance(envs, list):
        raise ValueError(f"Redis pool key has invalid envs shape: {key}")
    specs: List[EnvSpec] = []
    for item in envs:
        if not isinstance(item, dict):
            raise ValueError(f"Redis pool env entry is not an object: {item!r}")
        specs.append(env_spec_from_record(item))
    return dedup_env_specs(specs)


def dump_pool_json(specs: Sequence[EnvSpec]) -> str:
    return json.dumps(build_pool_payload(specs), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def decode_redis_list(raw: Any, key: str) -> List[str]:
    if not raw:
        return []
    text = raw.decode("utf-8", "ignore") if isinstance(raw, (bytes, bytearray)) else str(raw)
    parsed = json.loads(text)
    if not isinstance(parsed, list):
        raise ValueError(f"Redis key is not a JSON list: {key}")
    return [str(item).strip() for item in parsed if str(item).strip()]


def online_symbol_keys(spec: EnvSpec) -> List[str]:
    if spec.mode == "fr":
        suffix = f"{spec.open_venue}_{spec.hedge_venue}"
        names = [
            "dump_symbols",
            "trade_symbols",
            "fwd_trade_symbols",
            "bwd_trade_symbols",
            "unimmr_close_symbols",
        ]
        return [f"{spec.env_name}:fr_{name}:{suffix}" for name in names]

    if spec.mode == "intra":
        exchange_suffix = spec.open_exchange
        venue_suffix = f"{spec.open_venue}_{spec.hedge_venue}"
        return [
            f"{spec.env_name}:intra_dump_symbols:{exchange_suffix}",
            f"{spec.env_name}:intra_trade_symbols:{exchange_suffix}",
            f"{spec.env_name}:intra_fwd_trade_symbols:{exchange_suffix}",
            f"{spec.env_name}:intra_bwd_trade_symbols:{exchange_suffix}",
        ]

    if spec.mode == "cross":
        key_suffix = f"{spec.open_exchange}-{spec.hedge_exchange}"
        unimmr_suffix = f"{spec.open_venue}_{spec.hedge_venue}"
        return [
            f"cross_dump_symbols:{key_suffix}",
            f"cross_fwd_trade_symbols:{key_suffix}",
            f"cross_bwd_trade_symbols:{key_suffix}",
            f"{spec.env_name}:cross_unimmr_close_symbols:{unimmr_suffix}",
        ]

    raise ValueError(f"unsupported env mode: {spec.mode}")


def expand_env_symbols(
    rds: Any, spec: EnvSpec, product_type: str = "USDT-FUTURES"
) -> EnvSymbols:
    keys = tuple(online_symbol_keys(spec))
    assets: Set[str] = set()
    key_counts: Dict[str, int] = {}
    for key in keys:
        try:
            values = decode_redis_list(rds.get(key), key)
        except Exception:
            values = []
        key_counts[key] = len(values)
        for value in values:
            asset = normalize_asset(value)
            if asset:
                assets.add(asset)
    bitget_symbols = {
        bitget_symbol(asset, product_type)
        for asset in assets
        if bitget_symbol(asset, product_type)
    }
    return EnvSymbols(
        spec=spec,
        keys=keys,
        key_counts=key_counts,
        assets=tuple(sorted(assets)),
        bitget_symbols=tuple(sorted(bitget_symbols)),
    )


def expand_pool_symbols(
    rds: Any,
    specs: Sequence[EnvSpec],
    product_type: str = "USDT-FUTURES",
) -> Tuple[List[EnvSymbols], List[str]]:
    expanded = [
        expand_env_symbols(rds, spec, product_type)
        for spec in specs
        if spec.has_bitget_product(product_type)
    ]
    union: Set[str] = set()
    for item in expanded:
        union.update(item.bitget_symbols)
    return expanded, sorted(union)
