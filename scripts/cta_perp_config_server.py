#!/usr/bin/env python3
"""Standalone file-backed configuration server for disposable CTA perp envs."""

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

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from cta_perp_factor_config import read_factor_table, save_factor_table, available_universe, reject_duplicate_keys, normalize_table, validate_available  # noqa: E402

OPEN_VENUE = "binance-futures"
HEDGE_VENUE = "binance-futures"

MAX_BODY_BYTES = 64 * 1024

TOP_FIELDS = {"bar_interval", "enabled", "max_pos_u", "entry", "execution", "position_algorithm", "symbol_overrides", "factor_rules"}
ENTRY_FIELDS = {
    "factor_long_quantile", "factor_short_quantile", "nq_change_enabled",
    "nq_long_quantile", "nq_short_quantile",
}
EXECUTION_FIELDS = {"factor_exit_quantile_long", "factor_exit_quantile_short"}
OVERRIDE_FIELDS = {"factor_long_quantile", "factor_short_quantile", "nq_long_quantile", "nq_short_quantile"} | EXECUTION_FIELDS
SYMBOL_MEAN_STRATEGY = "symbol_mean"


def position_strategy_factors(row: dict, algorithm: str) -> dict:
    active = {factor: weight for factor, weight in row.items() if weight != 0}
    if algorithm == "v1":
        return {SYMBOL_MEAN_STRATEGY: active} if active else {}
    return {factor: {factor: weight} for factor, weight in active.items()}


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


def position_decision_value(x: float, algorithm: str) -> float:
    if algorithm == "v0":
        return x
    if algorithm == "v1":
        return 2 * x - 1
    raise ValueError("position_algorithm must be v0 or v1")


def effective_config(config: dict, symbol: str, factor: str | None = None) -> dict:
    result = {**config, "entry": dict(config["entry"]),
              "execution": dict(config["execution"]), "symbol_overrides": {}, "factor_rules": []}
    for field, value in config.get("symbol_overrides", {}).get(symbol, {}).items():
        if value is not None:
            result["execution" if field in EXECUTION_FIELDS else "entry"][field] = value
    for rule in config.get("factor_rules", []):
        if rule["标的"] == symbol and rule["因子"] == factor:
            thresholds = parse_nq(rule["nq"])
            result["strict_nq"] = True
            result["entry"]["nq_change_enabled"] = thresholds is not None
            if thresholds is not None:
                result["entry"]["nq_long_quantile"], result["entry"]["nq_short_quantile"] = thresholds
    return result


def parse_nq(value):
    if value == "关闭": return None
    if not isinstance(value, str): raise ValueError("nq 必须是分位数字符串、long/short 或关闭")
    parts = value.split("/")
    if len(parts) not in (1, 2): raise ValueError("nq 格式必须为 0.50、0.2/0.8 或关闭")
    values = [float(part.strip()) for part in parts]
    if not all(math.isfinite(v) and 0 <= v <= 1 for v in values): raise ValueError("nq 分位必须在 [0,1]")
    return values[0], values[-1]


def normalize_factor_rules(raw):
    if not isinstance(raw, list) or len(raw) > 8192: raise ValueError("factor_rules 必须是 JSON 数组，最多8192行")
    table, result = {}, []
    for index, row in enumerate(raw):
        if not isinstance(row, dict) or set(row) != {"标的", "nq", "符号", "因子"}: raise ValueError(f"第{index+1}行需要标的、nq、符号、因子")
        symbol, factor, sign = row["标的"], row["因子"], row["符号"]
        if not isinstance(symbol, str) or not re.fullmatch(r"[A-Z0-9]{1,28}", symbol): raise ValueError("标的必须是大写资产或USDT合约")
        symbol = symbol if symbol.endswith("USDT") else symbol + "USDT"
        if not isinstance(factor, str) or isinstance(sign, bool) or sign not in (-1, 1): raise ValueError("因子须为字符串，符号须为+1或-1")
        normalized = normalize_table({symbol: {factor: sign}})[symbol]
        factor = next(iter(normalized))
        if factor in table.setdefault(symbol, {}): raise ValueError(f"重复标的×因子：{symbol}/{factor}")
        parse_nq(row["nq"])
        table[symbol][factor] = sign
        result.append({"标的": symbol, "nq": row["nq"], "符号": int(sign), "因子": factor})
    normalize_table(table)
    return result, table


def validate_position_thresholds(config: dict) -> None:
    minimum = position_decision_value(0.0, config["position_algorithm"])
    entry, execution = config["entry"], config["execution"]
    if not all(0 <= entry[field] <= 1 for field in ("nq_long_quantile", "nq_short_quantile")):
        raise ValueError("entry NQ quantiles must be in [0, 1]")
    if config["bar_interval"] in ("5m", "15m") and not config.get("strict_nq") and any(entry[field] != 0.5 for field in ("nq_long_quantile", "nq_short_quantile")):
        raise ValueError(f"{config['bar_interval']} NQ uses fixed 0.50 linear quantile thresholds")
    long, short = entry["factor_long_quantile"], entry["factor_short_quantile"]
    if not minimum <= short < long <= 1.0:
        raise ValueError(f"entry factor thresholds must satisfy {minimum} <= short < long <= 1")
    if not minimum <= execution["factor_exit_quantile_long"] < long:
        raise ValueError("execution.factor_exit_quantile_long must be below entry.factor_long_quantile and within the algorithm range")
    if not short < execution["factor_exit_quantile_short"] <= 1.0:
        raise ValueError("execution.factor_exit_quantile_short must be above entry.factor_short_quantile and within the algorithm range")


def normalize_config(raw: Any) -> dict[str, Any]:
    root = _object(raw, "config")
    _reject_unknown(root, TOP_FIELDS, "config")
    if "execution" not in root:
        raise ValueError("config.execution is required")
    interval = root.get("bar_interval", "1m")
    if interval not in ("1m", "5m", "15m"):
        raise ValueError("bar_interval must be 1m, 5m or 15m")
    algorithm = root.get("position_algorithm", "v0")
    if algorithm not in ("v0", "v1"):
        raise ValueError("position_algorithm must be v0 or v1")

    amount = _number(root.get("max_pos_u", 200.0), "max_pos_u")
    if amount <= 0:
        raise ValueError("max_pos_u must be a positive finite USDT amount")

    entry = _object(root.get("entry", {}), "entry")
    _reject_unknown(entry, ENTRY_FIELDS, "entry")
    factor_long = _number(entry.get("factor_long_quantile", 0.9), "entry.factor_long_quantile")
    factor_short = _number(entry.get("factor_short_quantile", 0.1), "entry.factor_short_quantile")
    nq_long = _number(entry.get("nq_long_quantile", 0.5), "entry.nq_long_quantile")
    nq_short = _number(entry.get("nq_short_quantile", 0.5), "entry.nq_short_quantile")
    if not (0.0 <= nq_long <= 1.0 and 0.0 <= nq_short <= 1.0):
        raise ValueError("entry NQ quantiles must be in [0, 1]")

    if interval in ("5m", "15m") and (nq_long != 0.5 or nq_short != 0.5):
        raise ValueError(f"{interval} NQ uses fixed 0.50 linear quantile thresholds")

    execution = _object(root["execution"], "execution")
    _reject_unknown(execution, EXECUTION_FIELDS, "execution")
    exit_long = _number(
        execution.get("factor_exit_quantile_long", 0.3),
        "execution.factor_exit_quantile_long",
    )
    exit_short = _number(
        execution.get("factor_exit_quantile_short", 0.7),
        "execution.factor_exit_quantile_short",
    )
    result = {
        "bar_interval": interval,
        "enabled": _bool(root.get("enabled", False), "enabled"),
        "max_pos_u": amount,
        "position_algorithm": algorithm,
        "symbol_overrides": {},
        "factor_rules": [],
        "entry": {
            "factor_long_quantile": factor_long,
            "factor_short_quantile": factor_short,
            "nq_change_enabled": _bool(
                entry.get("nq_change_enabled", True), "entry.nq_change_enabled"
            ),
            "nq_long_quantile": nq_long,
            "nq_short_quantile": nq_short,
        },
        "execution": {
            "factor_exit_quantile_long": exit_long,
            "factor_exit_quantile_short": exit_short,
        },
    }
    validate_position_thresholds(result)
    overrides = _object(root.get("symbol_overrides", {}), "symbol_overrides")
    if len(overrides) > 256:
        raise ValueError("symbol_overrides supports at most 256 symbols")
    for symbol, raw_row in sorted(overrides.items()):
        if not isinstance(symbol, str) or not re.fullmatch(r"[A-Z0-9]{1,32}", symbol):
            raise ValueError(f"invalid symbol_overrides symbol: {symbol}")
        field = f"symbol_overrides.{symbol}"
        row = _object(raw_row, field)
        _reject_unknown(row, OVERRIDE_FIELDS, field)
        result["symbol_overrides"][symbol] = {
            key: _number(value, f"{field}.{key}") for key, value in row.items() if value is not None
        }
        try:
            validate_position_thresholds(effective_config(result, symbol))
        except ValueError as error:
            raise ValueError(f"{field}: {error}") from error
    rules, table = normalize_factor_rules(root.get("factor_rules", []))
    if rules and (algorithm != "v0" or interval == "5m"):
        raise ValueError("factor_rules 仅支持1m或15m v0独立因子模式")
    result["factor_rules"] = rules
    for rule in rules:
        validate_position_thresholds(effective_config(result, rule["标的"], rule["因子"]))
    return result


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
        if config["factor_rules"]:
            validate_available(normalize_factor_rules(config["factor_rules"])[1],
                available_universe(self.path.parent / f"cta_perp_factor_model_{config['bar_interval']}_pub.toml"))
        with self._lock:
            current, current_revision = self._load_unlocked()
            if not expected_revision or expected_revision != current_revision:
                raise ConfigConflict("configuration changed; reload before saving")
            if current["bar_interval"] != config["bar_interval"]:
                raise ValueError("bar_interval is fixed for this environment")
            if current["position_algorithm"] != config["position_algorithm"]:
                positions_path = self.path.parent.parent / "run/cta_perp_position_flags.json"
                if positions_path.exists():
                    positions = json.loads(positions_path.read_text())
                    if any(p.get("flag", 0) != 0 or p.get("qty", 0) != 0
                           for rows in positions.values() for p in rows.values()):
                        raise ValueError("算法切换会改变持仓归属，请先关闭现有仓位；单份金额与阈值可直接调整")
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


def make_handler(
    store: ConfigStore,
    index_path: Path,
    redis_client: Any = None,
    env_name: str | None = None,
):
    class Handler(BaseHTTPRequestHandler):
        server_version = "cta-perp-config"

        def log_message(self, message: str, *args: Any) -> None:
            print(f"[cta-perp-config] {self.address_string()} {message % args}")

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
                if path == "/api/symbol-factors":
                    if redis_client is None or not env_name:
                        self._json(503, {"error": "Redis factor configuration is unavailable"})
                        return
                    config, config_revision = store.load()
                    if config["factor_rules"]:
                        table = normalize_factor_rules(config["factor_rules"])[1]
                        body = {"key": f"{env_name}:cta_symbol_factors", "exists": True,
                            "symbol_factors": table, "revision": store._revision(table)}
                    else:
                        body = read_factor_table(redis_client, env_name)
                    self._json(200, {**body, "factor_rules": config["factor_rules"], "config_revision": config_revision,
                        "available": available_universe(store.path.parent / f"cta_perp_factor_model_{config['bar_interval']}_pub.toml")})
                    return
                if path in {"/", "/index.html"}:
                    self._send(200, index_path.read_bytes(), "text/html; charset=utf-8")
                    return
                self._json(404, {"error": "not found"})
            except Exception as exc:
                self._json(500, {"error": str(exc)})

        def do_POST(self) -> None:  # noqa: N802
            if urlparse(self.path).path == "/api/symbol-factors":
                if redis_client is None or not env_name:
                    self._json(503, {"error": "Redis factor configuration is unavailable"})
                    return
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    if length <= 0 or length > MAX_BODY_BYTES:
                        raise ValueError("invalid request body size")
                    request = _object(json.loads(self.rfile.read(length), object_pairs_hook=reject_duplicate_keys), "request")
                    _reject_unknown(request, {"symbol_factors", "expected_revision", "expected_config_revision"}, "request")
                    universe = available_universe(store.path.parent / f"cta_perp_factor_model_{store.load()[0]['bar_interval']}_pub.toml")
                    current_config, _ = store.load()
                    if isinstance(request.get("symbol_factors"), list):
                        rules, table = normalize_factor_rules(request["symbol_factors"])
                        if not rules: raise ValueError("规则数组不能为空，请使用币种对象配置清空组合")
                        validate_available(table, universe)
                        config, config_revision = store.save({**current_config, "factor_rules": rules}, request.get("expected_config_revision", ""))
                        self._json(200, {"key": f"{env_name}:cta_symbol_factors", "exists": True, "revision": store._revision(table), "symbol_factors": table,
                            "factor_rules": rules, "config_revision": config_revision, "available": universe})
                        return
                    if current_config["factor_rules"]:
                        raise ValueError("当前使用标的×因子规则，请用JSON数组编辑；如需切回币种对象，先清空config.factor_rules")
                    saved = save_factor_table(
                        redis_client, env_name,
                        request.get("symbol_factors"), request.get("expected_revision", ""),
                        universe,
                    )
                    self._json(200, saved | {"available": universe})
                except ValueError as exc:
                    self._json(409 if "configuration changed" in str(exc) else 400, {"error": str(exc)})
                except Exception as exc:
                    self._json(500, {"error": str(exc)})
                return
            self._json(404, {"error": "not found"})

        def do_PUT(self) -> None:  # noqa: N802
            if urlparse(self.path).path != "/api/config":
                self._json(404, {"error": "not found"})
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0 or length > MAX_BODY_BYTES:
                    raise ValueError("invalid request body size")
                request = _object(json.loads(self.rfile.read(length), object_pairs_hook=reject_duplicate_keys), "request")
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
    parser.add_argument("--config", type=Path, default=Path("config/cta_perp.json"))
    parser.add_argument("--index", type=Path, default=root / "web/cta_perp_config/index.html")
    parser.add_argument("--check", action="store_true", help="validate config and exit")
    args = parser.parse_args()
    store = ConfigStore(args.config.resolve())
    store.load()
    if args.check:
        print(f"[cta-perp-config] valid: {args.config}")
        return
    import redis

    env_name = os.path.basename(os.getcwd()).strip().lower()
    redis_client = redis.Redis(
        host=os.getenv("REDIS_HOST", "127.0.0.1"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        username=os.getenv("REDIS_USERNAME"),
        password=os.getenv("REDIS_PASSWORD"),
        socket_connect_timeout=3, socket_timeout=3,
    )
    redis_client.ping()
    server = ThreadingHTTPServer(
        (args.bind, args.port),
        make_handler(store, args.index.resolve(), redis_client, env_name),
    )
    print(f"[cta-perp-config] listening on http://{args.bind}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
