#!/usr/bin/env python3
"""Standalone file-backed configuration server for disposable CTA special envs."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

_INTRA_SCRIPTS = Path(__file__).resolve().parents[1] / "intra_scripts"
if str(_INTRA_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_INTRA_SCRIPTS))

import sync_cta_risk_params as risk_defaults  # noqa: E402

OPEN_VENUE = "binance-futures"
HEDGE_VENUE = "binance-futures"
BINANCE_HEDGE_RATE_10S = "300"
BINANCE_HEDGE_RATE_10S_KEY = "arb_hedge_order_rate_limit_10s"

MAX_BODY_BYTES = 64 * 1024
RULE_NAMES = {"tp_vpi_018", "baseline_104"}
SYMBOL_RE = re.compile(r"^[A-Z0-9][A-Z0-9._-]{0,31}$")

TOP_FIELDS = {"enabled", "rule_name", "symbols", "entry", "execution"}
ENTRY_FIELDS = {
    "trade_sides", "factor_long_quantile", "factor_short_quantile",
    "cooldown_seconds", "signal_delay_seconds", "nq_change_enabled",
    "nq_long_quantile", "nq_short_quantile",
}
EXECUTION_FIELDS = {
    "order_notional_usdt", "open_offsets", "maker_ttl_seconds",
    "factor_exit_enabled",
    "factor_exit_quantile_long", "factor_exit_quantile_short",
    "trailing_stop_enabled", "trailing_stop_trigger_step", "trailing_stop_move_step",
    "max_holding_seconds",
}


def _object(raw: Any, field: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"{field} must be an object")
    return raw


def _reject_unknown(raw: dict[str, Any], allowed: set[str], field: str) -> None:
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ValueError(f"{field} contains unknown fields: {', '.join(unknown)}")


def _bool(raw: Any, field: str) -> bool:
    if not isinstance(raw, bool):
        raise ValueError(f"{field} must be a boolean")
    return raw


def _number(raw: Any, field: str) -> float:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(f"{field} must be a number")
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError(f"{field} must be finite")
    return value


def _integer(raw: Any, field: str) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ValueError(f"{field} must be an integer")
    return raw


def normalize_config(raw: Any) -> dict[str, Any]:
    root = _object(raw, "config")
    _reject_unknown(root, TOP_FIELDS, "config")
    for required in ("rule_name", "symbols", "execution"):
        if required not in root:
            raise ValueError(f"config.{required} is required")

    rule_name = str(root["rule_name"]).strip().lower()
    if rule_name not in RULE_NAMES:
        raise ValueError("rule_name must be tp_vpi_018 or baseline_104")

    symbols_raw = root["symbols"]
    if not isinstance(symbols_raw, list):
        raise ValueError("symbols must be an array")
    symbols = sorted({str(symbol).strip().upper() for symbol in symbols_raw})
    if not symbols or any(not SYMBOL_RE.fullmatch(symbol) for symbol in symbols):
        raise ValueError("symbols must contain valid non-empty symbols of at most 32 bytes")

    entry = _object(root.get("entry", {}), "entry")
    _reject_unknown(entry, ENTRY_FIELDS, "entry")
    trade_sides = str(entry.get("trade_sides", "both")).strip().lower()
    if trade_sides in {"combine", "long_short", "long,short", "short,long"}:
        trade_sides = "both"
    if trade_sides not in {"long", "short", "both"}:
        raise ValueError("entry.trade_sides must be long, short, or both")
    factor_long = _number(entry.get("factor_long_quantile", 0.9), "entry.factor_long_quantile")
    factor_short = _number(entry.get("factor_short_quantile", 0.1), "entry.factor_short_quantile")
    if not (0.0 <= factor_short < factor_long <= 1.0):
        raise ValueError("entry factor quantiles must satisfy 0 <= short < long <= 1")
    cooldown = _integer(entry.get("cooldown_seconds", 0), "entry.cooldown_seconds")
    signal_delay = _integer(
        entry.get("signal_delay_seconds", 1), "entry.signal_delay_seconds"
    )
    if cooldown < 0 or signal_delay < 0:
        raise ValueError("entry cooldown_seconds and signal_delay_seconds cannot be negative")
    nq_long = _number(entry.get("nq_long_quantile", 0.5), "entry.nq_long_quantile")
    nq_short = _number(entry.get("nq_short_quantile", 0.5), "entry.nq_short_quantile")
    if not (0.0 <= nq_long <= 1.0 and 0.0 <= nq_short <= 1.0):
        raise ValueError("entry NQ quantiles must be in [0, 1]")

    execution = _object(root["execution"], "execution")
    _reject_unknown(execution, EXECUTION_FIELDS, "execution")
    notional = _number(
        execution.get("order_notional_usdt", 100.0), "execution.order_notional_usdt"
    )
    if notional <= 0:
        raise ValueError("execution.order_notional_usdt must be positive")
    offsets_raw = execution.get("open_offsets", [0.0, 0.0001, 0.0003, 0.0005])
    if not isinstance(offsets_raw, list) or not 1 <= len(offsets_raw) <= 8:
        raise ValueError("execution.open_offsets must be an array with 1..=8 levels")
    offsets = [
        _number(value, f"execution.open_offsets[{index}]")
        for index, value in enumerate(offsets_raw)
    ]
    if any(not 0.0 <= value <= 0.01 for value in offsets):
        raise ValueError("execution.open_offsets values must be in [0, 0.01]")
    if any(current <= previous for previous, current in zip(offsets, offsets[1:])):
        raise ValueError("execution.open_offsets must be strictly increasing")
    maker_ttl = _integer(
        execution.get("maker_ttl_seconds", 120), "execution.maker_ttl_seconds"
    )
    if maker_ttl <= 0:
        raise ValueError("execution.maker_ttl_seconds must be positive")
    factor_exit_enabled = _bool(
        execution.get("factor_exit_enabled", True), "execution.factor_exit_enabled"
    )
    exit_long = _number(
        execution.get("factor_exit_quantile_long", 0.3),
        "execution.factor_exit_quantile_long",
    )
    exit_short = _number(
        execution.get("factor_exit_quantile_short", 0.7),
        "execution.factor_exit_quantile_short",
    )
    if not 0.0 <= exit_long < factor_long:
        raise ValueError(
            "execution.factor_exit_quantile_long must be below entry.factor_long_quantile"
        )
    if not factor_short < exit_short <= 1.0:
        raise ValueError(
            "execution.factor_exit_quantile_short must be above entry.factor_short_quantile"
        )
    trailing_enabled = _bool(
        execution.get("trailing_stop_enabled", True), "execution.trailing_stop_enabled"
    )
    trigger = _number(
        execution.get("trailing_stop_trigger_step", 0.02),
        "execution.trailing_stop_trigger_step",
    )
    move = _number(
        execution.get("trailing_stop_move_step", 0.01),
        "execution.trailing_stop_move_step",
    )
    if trailing_enabled and not (trigger > 0.0 and 0.0 < move < trigger):
        raise ValueError("trailing move must be positive and below trigger")
    max_holding = _integer(
        execution.get("max_holding_seconds", 0), "execution.max_holding_seconds"
    )
    if max_holding < 0:
        raise ValueError("execution.max_holding_seconds cannot be negative")

    return {
        "enabled": _bool(root.get("enabled", False), "enabled"),
        "rule_name": rule_name,
        "symbols": symbols,
        "entry": {
            "trade_sides": trade_sides,
            "factor_long_quantile": factor_long,
            "factor_short_quantile": factor_short,
            "cooldown_seconds": cooldown,
            "signal_delay_seconds": signal_delay,
            "nq_change_enabled": _bool(
                entry.get("nq_change_enabled", True), "entry.nq_change_enabled"
            ),
            "nq_long_quantile": nq_long,
            "nq_short_quantile": nq_short,
        },
        "execution": {
            "order_notional_usdt": notional,
            "open_offsets": offsets,
            "maker_ttl_seconds": maker_ttl,
            "factor_exit_enabled": factor_exit_enabled,
            "factor_exit_quantile_long": exit_long,
            "factor_exit_quantile_short": exit_short,
            "trailing_stop_enabled": trailing_enabled,
            "trailing_stop_trigger_step": trigger,
            "trailing_stop_move_step": move,
            "max_holding_seconds": max_holding,
        },
    }


class ConfigConflict(ValueError):
    pass


class ConfigStore:
    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()

    @staticmethod
    def _revision(config: dict[str, Any]) -> str:
        canonical = json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(canonical).hexdigest()

    def load(self) -> tuple[dict[str, Any], str]:
        with self._lock:
            return self._load_unlocked()

    def _load_unlocked(self) -> tuple[dict[str, Any], str]:
        config = normalize_config(json.loads(self.path.read_text(encoding="utf-8")))
        return config, self._revision(config)

    def save(self, raw: Any, expected_revision: str) -> tuple[dict[str, Any], str]:
        config = normalize_config(raw)
        with self._lock:
            current, current_revision = self._load_unlocked()
            if not expected_revision or expected_revision != current_revision:
                raise ConfigConflict("configuration changed; reload before saving")
            immutable_paths = (
                ("rule_name",),
            )
            for path in immutable_paths:
                old_value: Any = current
                new_value: Any = config
                for key in path:
                    old_value = old_value[key]
                    new_value = new_value[key]
                if old_value != new_value:
                    raise ValueError(
                        f"{'.'.join(path)} is immutable for a running environment; redeploy or restart with an edited file"
                    )
            payload = (json.dumps(config, indent=2, ensure_ascii=True) + "\n").encode()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd, temp_name = tempfile.mkstemp(prefix=f".{self.path.name}.", dir=self.path.parent)
            try:
                if self.path.exists():
                    os.fchmod(fd, self.path.stat().st_mode & 0o777)
                with os.fdopen(fd, "wb") as output:
                    output.write(payload)
                    output.flush()
                    os.fsync(output.fileno())
                os.replace(temp_name, self.path)
                directory_fd = os.open(self.path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            except Exception:
                try:
                    os.close(fd)
                except OSError:
                    pass
                try:
                    os.unlink(temp_name)
                except FileNotFoundError:
                    pass
                raise
            return config, self._revision(config)


def risk_schema() -> tuple[dict[str, str], dict[str, str], list[str]]:
    defaults = {key: str(value) for key, value in risk_defaults.RISK_PARAMS.items()}
    defaults[BINANCE_HEDGE_RATE_10S_KEY] = BINANCE_HEDGE_RATE_10S
    comments = dict(risk_defaults.PARAM_COMMENTS)
    comments[BINANCE_HEDGE_RATE_10S_KEY] = "Binance futures 10 秒对冲下单上限，固定 300"
    order = list(risk_defaults.PARAM_PRINT_ORDER)
    for key in defaults:
        if key not in order:
            order.append(key)
    return defaults, comments, order


def risk_params_key(env_name: str) -> str:
    return f"{env_name}:{OPEN_VENUE}:{HEDGE_VENUE}:pre_trade_risk_params"


def _decode_redis(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "ignore")
    return str(value)


def read_risk_hash(redis_client: Any, key: str) -> dict[str, str]:
    raw = redis_client.hgetall(key) or {}
    return {_decode_redis(name): _decode_redis(value) for name, value in raw.items()}


def normalize_risk_values(values: Any) -> dict[str, str]:
    defaults, _comments, order = risk_schema()
    if not isinstance(values, dict):
        raise ValueError("values must be an object")
    allowed = set(defaults)
    unknown = sorted(str(key) for key in values if str(key) not in allowed)
    if unknown:
        raise ValueError("unknown risk fields: " + ", ".join(unknown))
    mapping = dict(defaults)
    for key in order:
        if key in values and values[key] is not None and str(values[key]).strip() != "":
            mapping[key] = str(values[key]).strip()
    for key, raw in mapping.items():
        try:
            number = float(raw)
        except ValueError as exc:
            raise ValueError(f"{key} must be a number") from exc
        if not math.isfinite(number):
            raise ValueError(f"{key} must be finite")
        mapping[key] = f"{number:g}"
    trigger = float(mapping["unimmr_trigger_line"])
    recover = float(mapping["unimmr_recover_line"])
    if not (1.5 <= trigger < recover):
        raise ValueError("unimmr control lines must satisfy 1.5 <= unimmr_trigger_line < unimmr_recover_line")
    if float(mapping[BINANCE_HEDGE_RATE_10S_KEY]) != float(BINANCE_HEDGE_RATE_10S):
        raise ValueError(f"{BINANCE_HEDGE_RATE_10S_KEY} must be {BINANCE_HEDGE_RATE_10S}")
    mapping[BINANCE_HEDGE_RATE_10S_KEY] = BINANCE_HEDGE_RATE_10S
    if float(mapping["max_pos_u"]) <= 0:
        raise ValueError("max_pos_u must be positive")
    return mapping


def replace_risk_hash(redis_client: Any, key: str, mapping: dict[str, str]) -> dict[str, Any]:
    existing = {_decode_redis(item) for item in (redis_client.hkeys(key) or [])}
    stale = sorted(existing - set(mapping))
    pipe = redis_client.pipeline()
    pipe.hset(key, mapping=mapping)
    if stale:
        pipe.hdel(key, *stale)
    pipe.execute()
    return {"key": key, "count": len(mapping), "values": mapping, "removed_fields": stale}


def make_handler(
    store: ConfigStore,
    index_path: Path,
    redis_client: Any = None,
    env_name: str | None = None,
):
    class Handler(BaseHTTPRequestHandler):
        server_version = "cta-special-config"

        def log_message(self, message: str, *args: Any) -> None:
            print(f"[cta-special-config] {self.address_string()} {message % args}")

        def _send(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, status: int, value: Any) -> None:
            self._send(
                status,
                json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode(),
                "application/json; charset=utf-8",
            )

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            try:
                if path == "/api/healthz":
                    config, revision = store.load()
                    self._json(200, {"ok": True, "enabled": config["enabled"], "revision": revision})
                    return
                if path == "/api/config":
                    config, revision = store.load()
                    self._json(200, {"config": config, "revision": revision})
                    return
                if path == "/api/risk-schema":
                    defaults, comments, order = risk_schema()
                    self._json(
                        200,
                        {
                            "key": risk_params_key(env_name or ""),
                            "defaults": defaults,
                            "comments": comments,
                            "order": order,
                        },
                    )
                    return
                if path == "/api/risk-params":
                    if redis_client is None or not env_name:
                        self._json(503, {"error": "redis risk params are unavailable"})
                        return
                    key = risk_params_key(env_name)
                    raw_values = read_risk_hash(redis_client, key)
                    defaults, _comments, order = risk_schema()
                    values = {name: raw_values[name] for name in order if name in raw_values}
                    stale = {name: value for name, value in raw_values.items() if name not in defaults}
                    if not values and not raw_values:
                        self._json(404, {"error": f"risk params not found: {key}", "key": key})
                        return
                    self._json(
                        200,
                        {
                            "key": key,
                            "values": values,
                            "count": len(values),
                            "stale_count": len(stale),
                            "stale_values": stale,
                        },
                    )
                    return
                if path in {"/", "/index.html"}:
                    self._send(200, index_path.read_bytes(), "text/html; charset=utf-8")
                    return
                self._json(404, {"error": "not found"})
            except Exception as exc:
                self._json(500, {"error": str(exc)})

        def do_POST(self) -> None:  # noqa: N802
            if urlparse(self.path).path != "/api/risk-params":
                self._json(404, {"error": "not found"})
                return
            if redis_client is None or not env_name:
                self._json(503, {"error": "redis risk params are unavailable"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0 or length > MAX_BODY_BYTES:
                    raise ValueError("invalid request body size")
                request = _object(json.loads(self.rfile.read(length)), "request")
                _reject_unknown(request, {"values"}, "request")
                mapping = normalize_risk_values(request.get("values"))
                self._json(200, replace_risk_hash(redis_client, risk_params_key(env_name), mapping))
            except (ValueError, json.JSONDecodeError) as exc:
                self._json(400, {"error": str(exc)})
            except Exception as exc:
                self._json(500, {"error": str(exc)})

        def do_PUT(self) -> None:  # noqa: N802
            if urlparse(self.path).path != "/api/config":
                self._json(404, {"error": "not found"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0 or length > MAX_BODY_BYTES:
                    raise ValueError("invalid request body size")
                request = _object(json.loads(self.rfile.read(length)), "request")
                _reject_unknown(request, {"config", "expected_revision"}, "request")
                config, revision = store.save(
                    request.get("config"), str(request.get("expected_revision", ""))
                )
                self._json(200, {"config": config, "revision": revision})
            except ConfigConflict as exc:
                self._json(409, {"error": str(exc)})
            except (ValueError, json.JSONDecodeError) as exc:
                self._json(400, {"error": str(exc)})
            except Exception as exc:
                self._json(500, {"error": str(exc)})

    return Handler


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19182)
    parser.add_argument("--config", type=Path, default=Path("config/cta_special.json"))
    parser.add_argument("--index", type=Path, default=root / "web/cta_special_config/index.html")
    parser.add_argument("--check", action="store_true", help="validate config and exit")
    args = parser.parse_args()
    store = ConfigStore(args.config.resolve())
    store.load()
    if args.check:
        print(f"[cta-special-config] valid: {args.config}")
        return
    import redis

    env_name = os.path.basename(os.getcwd()).strip().lower()
    redis_client = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)
    redis_client.ping()
    server = ThreadingHTTPServer(
        (args.bind, args.port),
        make_handler(store, args.index.resolve(), redis_client, env_name),
    )
    print(f"[cta-special-config] listening on http://{args.bind}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
