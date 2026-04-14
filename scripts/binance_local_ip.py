#!/usr/bin/env python3
from __future__ import annotations

import http.client
import os
import re
import socket
import ssl
import ast
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional, Tuple

try:
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None  # type: ignore[assignment]


TRADE_ENGINE_CFG_NAMES = ("trade_engine.toml", "trade engine.toml")
DEFAULT_MKT_CFG_REL_PATH = "dat_pbs/config/mkt_cfg.yaml"


class SourceAddressHTTPHandler(urllib.request.HTTPHandler):
    def __init__(self, local_address: str):
        super().__init__()
        self._source_address = (local_address, 0)

    def http_open(self, req):
        return self.do_open(
            lambda host, **kwargs: http.client.HTTPConnection(
                host, source_address=self._source_address, **kwargs
            ),
            req,
        )


class SourceAddressHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, local_address: str):
        super().__init__()
        self._source_address = (local_address, 0)

    def https_open(self, req):
        return self.do_open(
            lambda host, **kwargs: http.client.HTTPSConnection(
                host, source_address=self._source_address, **kwargs
            ),
            req,
        )


def normalize_local_address(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    trimmed = value.strip()
    if not trimmed or trimmed in {"0.0.0.0", "::"}:
        return None
    return trimmed


def home_mkt_cfg_path() -> Path:
    home = os.environ.get("HOME", "").strip()
    if home:
        return Path(home) / DEFAULT_MKT_CFG_REL_PATH

    user = os.environ.get("USER", "").strip()
    if user:
        return Path(f"/home/{user}") / DEFAULT_MKT_CFG_REL_PATH

    return Path("/home/ubuntu") / DEFAULT_MKT_CFG_REL_PATH


def find_trade_engine_config(base_dir: Optional[str]) -> Optional[Path]:
    if not base_dir:
        return None

    root = Path(base_dir).expanduser()
    existing = [root / name for name in TRADE_ENGINE_CFG_NAMES if (root / name).is_file()]
    if not existing:
        return None
    if len(existing) > 1:
        joined = ", ".join(str(path) for path in existing)
        raise SystemExit(f"multiple trade_engine configs found: {joined}")
    return existing[0]


def load_trade_engine_local_ips(path: Path) -> list[str]:
    content = path.read_text(encoding="utf-8")
    if tomllib is not None:
        data = tomllib.loads(content)
        return _extract_local_ips_from_mapping(data, path)
    return _parse_trade_engine_local_ips_fallback(content, path)


def _extract_local_ips_from_mapping(data, path: Path) -> list[str]:
    local_ips: list[str] = []

    raw_local_ips = data.get("local_ips", [])
    if raw_local_ips is None:
        raw_local_ips = []
    if not isinstance(raw_local_ips, list):
        raise SystemExit(f"invalid local_ips in {path}: expected array")

    for idx, value in enumerate(raw_local_ips):
        trimmed = str(value).strip()
        if not trimmed:
            raise SystemExit(f"local_ips[{idx}] is empty in {path}")
        local_ips.append(trimmed)

    for key in ("primary_local_ip", "secondary_local_ip"):
        value = data.get(key)
        if value is None:
            continue
        trimmed = str(value).strip()
        if not trimmed:
            raise SystemExit(f"{key} is empty in {path}")
        local_ips.append(trimmed)

    if not local_ips:
        raise SystemExit(f"trade_engine config {path} must provide local_ips")
    return local_ips


def _parse_trade_engine_local_ips_fallback(content: str, path: Path) -> list[str]:
    local_ips: list[str] = []

    local_ips_match = re.search(r"(?ms)^\s*local_ips\s*=\s*\[(.*?)\]", content)
    if local_ips_match:
        tokens = re.findall(
            r'"(?:[^"\\]|\\.)*"|\'(?:[^\'\\]|\\.)*\'',
            local_ips_match.group(1),
        )
        for idx, token in enumerate(tokens):
            trimmed = str(ast.literal_eval(token)).strip()
            if not trimmed:
                raise SystemExit(f"local_ips[{idx}] is empty in {path}")
            local_ips.append(trimmed)

    for key in ("primary_local_ip", "secondary_local_ip"):
        key_match = re.search(
            rf'(?m)^\s*{re.escape(key)}\s*=\s*("(?:[^"\\]|\\.)*"|\'(?:[^\'\\]|\\.)*\')',
            content,
        )
        if key_match is None:
            continue
        trimmed = str(ast.literal_eval(key_match.group(1))).strip()
        if not trimmed:
            raise SystemExit(f"{key} is empty in {path}")
        local_ips.append(trimmed)

    if not local_ips:
        raise SystemExit(f"trade_engine config {path} must provide local_ips")
    return local_ips


def load_primary_local_ip_from_mkt_cfg(path: Path) -> Optional[str]:
    if not path.is_file():
        return None

    values: dict[str, str] = {}
    pattern = re.compile(r"^([A-Za-z0-9_]+)\s*:\s*(.+?)\s*$")
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        match = pattern.match(line)
        if not match:
            continue
        key, raw_value = match.groups()
        value = raw_value.strip().strip("\"'")
        values[key] = value

    primary = values.get("primary_local_ip")
    return normalize_local_address(primary)


def resolve_local_address(
    *,
    explicit_local_address: Optional[str] = None,
    trade_engine_config: Optional[str] = None,
    env_dir: Optional[str] = None,
    cwd: Optional[str] = None,
) -> Tuple[Optional[str], str]:
    manual = normalize_local_address(explicit_local_address)
    if manual is not None:
        return manual, "cli --local-address"

    config_candidates = []
    if trade_engine_config:
        config_candidates.append(Path(trade_engine_config).expanduser())

    env_cfg = find_trade_engine_config(env_dir)
    if env_cfg is not None:
        config_candidates.append(env_cfg)

    cwd_cfg = find_trade_engine_config(cwd or os.getcwd())
    if cwd_cfg is not None:
        config_candidates.append(cwd_cfg)

    seen: set[Path] = set()
    for path in config_candidates:
        if path in seen:
            continue
        seen.add(path)
        local_ips = load_trade_engine_local_ips(path)
        local_address = normalize_local_address(local_ips[0])
        if local_address is None:
            return None, f"{path} local_ips[0]={local_ips[0]!r} -> system-default"
        return local_address, f"{path} local_ips[0]"

    mkt_cfg_path = home_mkt_cfg_path()
    local_address = load_primary_local_ip_from_mkt_cfg(mkt_cfg_path)
    if local_address is not None:
        return local_address, f"{mkt_cfg_path} primary_local_ip"

    return None, "system-default (no local ip config found)"


def urlopen_with_local_address(
    req: urllib.request.Request,
    *,
    timeout: int,
    local_address: Optional[str],
):
    if not local_address:
        return urllib.request.urlopen(req, timeout=timeout)

    opener = urllib.request.build_opener(
        SourceAddressHTTPHandler(local_address),
        SourceAddressHTTPSHandler(local_address),
    )
    return opener.open(req, timeout=timeout)


def create_ws_connection(
    websocket_module,
    *,
    ws_url: str,
    timeout: int,
    local_address: Optional[str],
    sslopt: Optional[dict] = None,
):
    if not local_address:
        return websocket_module.create_connection(
            ws_url,
            timeout=timeout,
            sslopt=sslopt or {},
        )

    parsed = urllib.parse.urlparse(ws_url)
    host = parsed.hostname
    if not host:
        raise ValueError(f"invalid websocket url: {ws_url}")
    port = parsed.port or (443 if parsed.scheme == "wss" else 80)

    raw_sock = socket.create_connection(
        (host, port),
        timeout=timeout,
        source_address=(local_address, 0),
    )
    raw_sock.settimeout(timeout)

    sock = raw_sock
    try:
        if parsed.scheme == "wss":
            options = sslopt or {}
            context = ssl.create_default_context()
            cert_reqs = options.get("cert_reqs")
            if cert_reqs is not None:
                context.check_hostname = cert_reqs != ssl.CERT_NONE
                context.verify_mode = cert_reqs

            ca_certs = options.get("ca_certs")
            if ca_certs:
                context.load_verify_locations(cafile=ca_certs)

            sock = context.wrap_socket(raw_sock, server_hostname=host)
            sock.settimeout(timeout)

        return websocket_module.create_connection(
            ws_url,
            timeout=timeout,
            socket=sock,
        )
    except Exception:
        try:
            sock.close()
        except Exception:
            pass
        if sock is not raw_sock:
            try:
                raw_sock.close()
            except Exception:
                pass
        raise
