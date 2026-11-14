#!/usr/bin/env python3
"""Query or set Delta-neutral mode on five supported exchanges.

Supported exchanges and current REST contracts:

* Binance: POST /sapi/v1/portfolio/delta-mode
* OKX: POST /api/v5/account/set-trading-config
* Gate: POST /api/v4/unified/delta_neutral
* Bybit: POST /v5/account/set-delta-mode
* Bitget: POST /api/v3/account/adjust-account-mode

The default path performs no state queries: without ``--execute`` it only prints
the setting plan, and with ``--execute`` it submits exactly one setting request.
Add ``--query`` to query before the change and verify afterward. Mutating
requests are never retried automatically because a timeout can leave the remote
result ambiguous.

Examples:
  # Print the enable plan without making any API request.
  python3 scripts/set_delta_neutral_mode.py --exchange bitget

  # Source one deployed environment, then enable the mode.
  python3 scripts/set_delta_neutral_mode.py --exchange bybit \
      --env-file ~/bybit-intra-arb01/env.sh --execute

  # Disable OKX Delta-neutral strategy mode.
  python3 scripts/set_delta_neutral_mode.py --exchange okex --state off --execute

  # --set-only remains available as an explicit spelling of the default.
  python3 scripts/set_delta_neutral_mode.py --exchange binance --set-only --execute

  # Opt in to state queries around the setting request.
  python3 scripts/set_delta_neutral_mode.py --exchange bybit --query --execute

  # A Bitget master account can operate one sub-account.
  python3 scripts/set_delta_neutral_mode.py --exchange bitget \
      --target-uid 123456789 --execute

Credential environment variables:
  Binance: BINANCE_API_KEY / BINANCE_API_SECRET
  OKX:     OKX_API_KEY / OKX_API_SECRET / OKX_PASSPHRASE
  Gate:    GATE_API_KEY / GATE_API_SECRET
  Bybit:   BYBIT_API_KEY / BYBIT_API_SECRET
  Bitget:  BITGET_API_KEY / BITGET_API_SECRET / BITGET_API_PASSPHRASE
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Sequence


BINANCE_PATH = "/sapi/v1/portfolio/delta-mode"
OKX_CONFIG_PATH = "/api/v5/account/config"
OKX_SET_PATH = "/api/v5/account/set-trading-config"
GATE_PREFIX = "/api/v4"
GATE_PATH = "/unified/delta_neutral"
BYBIT_QUERY_PATH = "/v5/account/user-setting-config"
BYBIT_SET_PATH = "/v5/account/set-delta-mode"
BITGET_SETTINGS_PATH = "/api/v3/account/settings"
BITGET_DELTA_INFO_PATH = "/api/v3/account/delta-info"
BITGET_SET_PATH = "/api/v3/account/adjust-account-mode"
BITGET_TRANSIENT_CODES = {"40725", "40808", "45001"}

DEFAULT_BASE_URLS = {
    "binance": "https://api.binance.com",
    "okex": "https://www.okx.com",
    "gate": "https://api.gateio.ws",
    "bybit": "https://api.bybit.com",
    "bitget": "https://api.bitget.com",
}

AUTHORITATIVE_ENV_KEYS = (
    "BINANCE_API_KEY",
    "BINANCE_API_SECRET",
    "BINANCE_SAPI_URL",
    "BINANCE_API_URL",
    "OKX_API_KEY",
    "OKX_API_SECRET",
    "OKX_PASSPHRASE",
    "OKX_BASE_URL",
    "OKX_SIMULATED_TRADING",
    "GATE_API_KEY",
    "GATE_API_SECRET",
    "GATE_API_BASE",
    "BYBIT_API_KEY",
    "BYBIT_API_SECRET",
    "BYBIT_API_BASE",
    "BITGET_API_KEY",
    "BITGET_API_SECRET",
    "BITGET_API_PASSPHRASE",
    "BITGET_API_BASE",
)


class ApiError(RuntimeError):
    """An exchange request failed or returned an invalid response."""


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: str


@dataclass(frozen=True)
class ModeState:
    enabled: Optional[bool]
    detail: str = ""


Transport = Callable[[str, str, Mapping[str, str], Optional[bytes], int], HttpResponse]


def now_ms() -> int:
    return int(time.time() * 1000)


def utc_timestamp_iso_ms() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def compact_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True)


def short_body(body: str, limit: int = 512) -> str:
    text = body.replace("\n", " ").strip()
    return text if len(text) <= limit else f"{text[:limit]}..."


def http_request(
    method: str,
    url: str,
    headers: Mapping[str, str],
    data: Optional[bytes],
    timeout: int,
) -> HttpResponse:
    request = urllib.request.Request(url, data=data, method=method.upper(), headers=dict(headers))
    parsed_url = urllib.parse.urlsplit(url)
    safe_url = urllib.parse.urlunsplit(
        (parsed_url.scheme, parsed_url.netloc, parsed_url.path, "", "")
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return HttpResponse(
                status=response.getcode(),
                body=response.read().decode("utf-8", errors="replace"),
            )
    except urllib.error.HTTPError as exc:
        return HttpResponse(
            status=exc.code,
            body=exc.read().decode("utf-8", errors="replace"),
        )
    except urllib.error.URLError as exc:
        raise ApiError(
            f"network error calling {method.upper()} {safe_url}: {exc.reason}; "
            "the request was not retried"
        ) from exc
    except TimeoutError as exc:
        raise ApiError(
            f"timeout calling {method.upper()} {safe_url}; the request was not retried"
        ) from exc


def parse_http_json(exchange: str, path: str, response: HttpResponse) -> Dict[str, Any]:
    if not 200 <= response.status < 300:
        raise ApiError(
            f"{exchange} {path} failed: http={response.status} "
            f"body={short_body(response.body)}"
        )
    try:
        payload = json.loads(response.body)
    except json.JSONDecodeError as exc:
        raise ApiError(
            f"{exchange} {path} returned non-JSON: {short_body(response.body)}"
        ) from exc
    if not isinstance(payload, dict):
        raise ApiError(f"{exchange} {path} returned a non-object JSON response")
    return payload


def parse_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on", "enabled"}:
            return True
        if normalized in {"0", "false", "no", "off", "disabled"}:
            return False
    return None


def require_env(names: Sequence[str]) -> tuple[str, ...]:
    values = tuple(os.environ.get(name, "").strip() for name in names)
    missing = [name for name, value in zip(names, values) if not value]
    if missing:
        raise ApiError(f"missing environment variables: {', '.join(missing)}")
    return values


def source_env_file(path: Path) -> None:
    if not path.is_file():
        raise ApiError(f"env file not found: {path}")
    command = f"set -a && source {shlex.quote(str(path))} >/dev/null 2>&1 && env -0"
    process = subprocess.run(
        ["bash", "-lc", command],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if process.returncode != 0:
        detail = process.stderr.decode("utf-8", errors="replace").strip()
        raise ApiError(f"failed to source {path}: {detail}")

    loaded: Dict[str, str] = {}
    for item in process.stdout.split(b"\0"):
        if not item or b"=" not in item:
            continue
        raw_key, raw_value = item.split(b"=", 1)
        key = raw_key.decode("utf-8", errors="ignore")
        if key in AUTHORITATIVE_ENV_KEYS:
            loaded[key] = raw_value.decode("utf-8", errors="replace")

    for key, value in loaded.items():
        old_value = os.environ.get(key)
        if old_value and old_value != value:
            print(
                f"WARN: {path} overrides existing {key}; env-file value wins for account safety",
                file=sys.stderr,
            )
        os.environ[key] = value


def maybe_source_env_file(explicit_path: Optional[str]) -> None:
    if explicit_path:
        source_env_file(Path(explicit_path).expanduser().resolve())
        return
    local_path = Path.cwd() / "env.sh"
    if local_path.is_file():
        source_env_file(local_path)


class BaseClient:
    exchange = "unknown"

    def __init__(self, base_url: str, timeout: int, transport: Transport = http_request) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.transport = transport

    def query(self) -> ModeState:
        raise NotImplementedError

    def set_enabled(self, enabled: bool) -> None:
        raise NotImplementedError

    def mutation_summary(self, enabled: bool) -> str:
        raise NotImplementedError


class BinanceClient(BaseClient):
    exchange = "binance"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str,
        timeout: int,
        transport: Transport = http_request,
    ) -> None:
        super().__init__(base_url, timeout, transport)
        self.api_key = api_key
        self.api_secret = api_secret

    def _request(self, method: str, params: Mapping[str, str]) -> Dict[str, Any]:
        signed_params = dict(params)
        signed_params["recvWindow"] = "5000"
        signed_params["timestamp"] = str(now_ms())
        unsigned = urllib.parse.urlencode(sorted(signed_params.items()), safe="-_.~")
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            unsigned.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        encoded = f"{unsigned}&signature={signature}"
        headers = {"X-MBX-APIKEY": self.api_key}
        if method.upper() == "GET":
            url = f"{self.base_url}{BINANCE_PATH}?{encoded}"
            data = None
        else:
            url = f"{self.base_url}{BINANCE_PATH}"
            data = encoded.encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        payload = parse_http_json(
            self.exchange,
            BINANCE_PATH,
            self.transport(method, url, headers, data, self.timeout),
        )
        if "code" in payload and str(payload.get("code")) not in {"0", "200"}:
            raise ApiError(
                f"binance {BINANCE_PATH} failed: code={payload.get('code')} "
                f"msg={payload.get('msg')!r}"
            )
        return payload

    def query(self) -> ModeState:
        payload = self._request("GET", {})
        enabled = parse_bool(payload.get("deltaEnabled"))
        if enabled is None:
            raise ApiError(f"binance {BINANCE_PATH} response has no valid deltaEnabled field")
        return ModeState(enabled, "Portfolio Margin Delta Mode")

    def set_enabled(self, enabled: bool) -> None:
        self._request("POST", {"deltaEnabled": str(enabled).lower()})

    def mutation_summary(self, enabled: bool) -> str:
        value = str(enabled).lower()
        return f"POST {BINANCE_PATH} deltaEnabled={value}"


class OkxClient(BaseClient):
    exchange = "okex"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str,
        base_url: str,
        timeout: int,
        simulated: bool = False,
        transport: Transport = http_request,
    ) -> None:
        super().__init__(base_url, timeout, transport)
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.simulated = simulated

    def _request(
        self,
        method: str,
        path: str,
        body: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        body_text = "" if body is None else compact_json(body)
        timestamp = utc_timestamp_iso_ms()
        signature = base64.b64encode(
            hmac.new(
                self.api_secret.encode("utf-8"),
                f"{timestamp}{method.upper()}{path}{body_text}".encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")
        headers = {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
        }
        if self.simulated:
            headers["x-simulated-trading"] = "1"
        response = self.transport(
            method,
            f"{self.base_url}{path}",
            headers,
            body_text.encode("utf-8") if body_text else None,
            self.timeout,
        )
        payload = parse_http_json(self.exchange, path, response)
        if str(payload.get("code", "")) != "0":
            raise ApiError(
                f"okex {path} failed: code={payload.get('code')} msg={payload.get('msg')!r}"
            )
        return payload

    def query(self) -> ModeState:
        payload = self._request("GET", OKX_CONFIG_PATH)
        rows = payload.get("data")
        if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
            raise ApiError(f"okex {OKX_CONFIG_PATH} returned invalid data")
        strategy_type = str(rows[0].get("stgyType", ""))
        if strategy_type not in {"0", "1"}:
            raise ApiError(f"okex {OKX_CONFIG_PATH} returned invalid stgyType={strategy_type!r}")
        detail = f"acctLv={rows[0].get('acctLv', '?')} stgyType={strategy_type}"
        return ModeState(strategy_type == "1", detail)

    def set_enabled(self, enabled: bool) -> None:
        self._request(
            "POST",
            OKX_SET_PATH,
            {"type": "stgyType", "stgyType": "1" if enabled else "0"},
        )

    def mutation_summary(self, enabled: bool) -> str:
        value = "1" if enabled else "0"
        return f"POST {OKX_SET_PATH} type=stgyType stgyType={value}"


class GateClient(BaseClient):
    exchange = "gate"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str,
        timeout: int,
        transport: Transport = http_request,
    ) -> None:
        super().__init__(base_url, timeout, transport)
        self.api_key = api_key
        self.api_secret = api_secret

    def _request(self, method: str, body: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
        body_text = "" if body is None else compact_json(body)
        timestamp = str(int(time.time()))
        signed_path = f"{GATE_PREFIX}{GATE_PATH}"
        body_hash = hashlib.sha512(body_text.encode("utf-8")).hexdigest()
        signing_payload = f"{method.upper()}\n{signed_path}\n\n{body_hash}\n{timestamp}"
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            signing_payload.encode("utf-8"),
            hashlib.sha512,
        ).hexdigest()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "KEY": self.api_key,
            "Timestamp": timestamp,
            "SIGN": signature,
        }
        response = self.transport(
            method,
            f"{self.base_url}{signed_path}",
            headers,
            body_text.encode("utf-8") if body_text else None,
            self.timeout,
        )
        return parse_http_json(self.exchange, signed_path, response)

    def query(self) -> ModeState:
        payload = self._request("GET")
        enabled = parse_bool(payload.get("enabled"))
        if enabled is None:
            raise ApiError(f"gate {GATE_PATH} response has no valid enabled field")
        return ModeState(enabled, "Unified Account cross-currency margin")

    def set_enabled(self, enabled: bool) -> None:
        self._request("POST", {"enabled": enabled})

    def mutation_summary(self, enabled: bool) -> str:
        value = str(enabled).lower()
        return f"POST {GATE_PREFIX}{GATE_PATH} enabled={value}"


class BybitClient(BaseClient):
    exchange = "bybit"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str,
        timeout: int,
        transport: Transport = http_request,
    ) -> None:
        super().__init__(base_url, timeout, transport)
        self.api_key = api_key
        self.api_secret = api_secret
        self.recv_window = "5000"

    def _request(
        self,
        method: str,
        path: str,
        body: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        body_text = "" if body is None else compact_json(body)
        timestamp = str(now_ms())
        signing_payload = f"{timestamp}{self.api_key}{self.recv_window}{body_text}"
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            signing_payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": self.recv_window,
            "Content-Type": "application/json",
        }
        response = self.transport(
            method,
            f"{self.base_url}{path}",
            headers,
            body_text.encode("utf-8") if body_text else None,
            self.timeout,
        )
        payload = parse_http_json(self.exchange, path, response)
        if str(payload.get("retCode", "")) != "0":
            raise ApiError(
                f"bybit {path} failed: retCode={payload.get('retCode')} "
                f"retMsg={payload.get('retMsg')!r}"
            )
        return payload

    def query(self) -> ModeState:
        payload = self._request("GET", BYBIT_QUERY_PATH)
        result = payload.get("result")
        if isinstance(result, list) and result:
            result = result[0]
        if not isinstance(result, dict):
            raise ApiError(f"bybit {BYBIT_QUERY_PATH} returned invalid result")
        enabled = parse_bool(result.get("deltaEnable"))
        if enabled is None:
            raise ApiError(f"bybit {BYBIT_QUERY_PATH} response has no valid deltaEnable field")
        return ModeState(enabled, "Unified Trading Account Delta Neutral Mode")

    def set_enabled(self, enabled: bool) -> None:
        self._request("POST", BYBIT_SET_PATH, {"deltaEnable": "1" if enabled else "0"})

    def mutation_summary(self, enabled: bool) -> str:
        value = "1" if enabled else "0"
        return f"POST {BYBIT_SET_PATH} deltaEnable={value}"


class BitgetClient(BaseClient):
    exchange = "bitget"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str,
        base_url: str,
        timeout: int,
        target_uid: Optional[str] = None,
        transport: Transport = http_request,
    ) -> None:
        super().__init__(base_url, timeout, transport)
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.target_uid = target_uid

    def _send(
        self,
        method: str,
        path: str,
        body: Optional[Mapping[str, Any]] = None,
    ) -> HttpResponse:
        body_text = "" if body is None else compact_json(body)
        timestamp = str(now_ms())
        signing_payload = f"{timestamp}{method.upper()}{path}{body_text}"
        signature = base64.b64encode(
            hmac.new(
                self.api_secret.encode("utf-8"),
                signing_payload.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")
        headers = {
            "ACCESS-KEY": self.api_key,
            "ACCESS-SIGN": signature,
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
            "locale": "en-US",
        }
        return self.transport(
            method,
            f"{self.base_url}{path}",
            headers,
            body_text.encode("utf-8") if body_text else None,
            self.timeout,
        )

    def _request_raw(
        self,
        method: str,
        path: str,
        body: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        return parse_http_json(self.exchange, path, self._send(method, path, body))

    @staticmethod
    def _require_success(path: str, payload: Mapping[str, Any]) -> None:
        if str(payload.get("code", "")) != "00000":
            raise ApiError(
                f"bitget {path} failed: code={payload.get('code')} msg={payload.get('msg')!r}"
            )

    def query(self) -> ModeState:
        if self.target_uid:
            return ModeState(
                None,
                "Bitget does not expose targetUid on its Delta-info query; target state is not queryable",
            )

        settings_payload = self._request_raw("GET", BITGET_SETTINGS_PATH)
        self._require_success(BITGET_SETTINGS_PATH, settings_payload)
        settings = settings_payload.get("data")
        if not isinstance(settings, dict):
            raise ApiError(f"bitget {BITGET_SETTINGS_PATH} returned invalid data")

        account_level = str(settings.get("accountLevel", ""))
        explicit_switch = parse_bool(settings.get("deltaSwitch"))
        detail = f"accountMode={settings.get('accountMode', '?')} accountLevel={account_level or '?'}"
        if explicit_switch is not None:
            return ModeState(explicit_switch, f"{detail} deltaSwitch={explicit_switch}")
        if account_level == "delta":
            return ModeState(True, f"{detail} legacy Delta account mode")
        if account_level != "advanced":
            return ModeState(False, detail)

        delta_response = self._send("GET", BITGET_DELTA_INFO_PATH)
        try:
            delta_payload = json.loads(delta_response.body)
        except json.JSONDecodeError as exc:
            raise ApiError(
                f"bitget {BITGET_DELTA_INFO_PATH} returned non-JSON: "
                f"{short_body(delta_response.body)}"
            ) from exc
        if not isinstance(delta_payload, dict):
            raise ApiError(f"bitget {BITGET_DELTA_INFO_PATH} returned invalid JSON data")
        if str(delta_payload.get("code", "")) == "00000":
            if not 200 <= delta_response.status < 300:
                raise ApiError(
                    f"bitget {BITGET_DELTA_INFO_PATH} returned success code with "
                    f"http={delta_response.status}"
                )
            return ModeState(True, f"{detail} delta-info available")
        code = str(delta_payload.get("code", ""))
        if (
            code in BITGET_TRANSIENT_CODES
            or delta_response.status in {401, 403, 429}
            or delta_response.status >= 500
        ):
            raise ApiError(
                f"bitget {BITGET_DELTA_INFO_PATH} failed: http={delta_response.status} "
                f"code={code} msg={delta_payload.get('msg')!r}"
            )
        return ModeState(
            False,
            f"{detail} delta-info unavailable http={delta_response.status} "
            f"code={delta_payload.get('code')} "
            f"msg={delta_payload.get('msg')!r}",
        )

    def set_enabled(self, enabled: bool) -> None:
        body: Dict[str, Any] = {
            "mode": "advanced",
            "deltaSwitch": "yes" if enabled else "no",
        }
        if self.target_uid:
            body["targetUid"] = self.target_uid
        payload = self._request_raw("POST", BITGET_SET_PATH, body)
        self._require_success(BITGET_SET_PATH, payload)

    def mutation_summary(self, enabled: bool) -> str:
        value = "yes" if enabled else "no"
        target = f" targetUid={self.target_uid}" if self.target_uid else ""
        return f"POST {BITGET_SET_PATH} mode=advanced deltaSwitch={value}{target}"


def normalize_exchange(value: str) -> str:
    normalized = value.strip().lower()
    return "okex" if normalized == "okx" else normalized


def resolve_base_url(exchange: str, override: Optional[str]) -> str:
    if override:
        return override.rstrip("/")
    env_names = {
        "binance": ("BINANCE_SAPI_URL", "BINANCE_API_URL"),
        "okex": ("OKX_BASE_URL",),
        "gate": ("GATE_API_BASE",),
        "bybit": ("BYBIT_API_BASE",),
        "bitget": ("BITGET_API_BASE",),
    }[exchange]
    for name in env_names:
        value = os.environ.get(name, "").strip()
        if value:
            return value.rstrip("/")
    return DEFAULT_BASE_URLS[exchange]


def create_client(
    exchange: str,
    base_url: str,
    timeout: int,
    target_uid: Optional[str],
    transport: Transport = http_request,
) -> BaseClient:
    if exchange == "binance":
        api_key, api_secret = require_env(("BINANCE_API_KEY", "BINANCE_API_SECRET"))
        return BinanceClient(api_key, api_secret, base_url, timeout, transport)
    if exchange == "okex":
        api_key, api_secret, passphrase = require_env(
            ("OKX_API_KEY", "OKX_API_SECRET", "OKX_PASSPHRASE")
        )
        return OkxClient(
            api_key,
            api_secret,
            passphrase,
            base_url,
            timeout,
            simulated=env_flag("OKX_SIMULATED_TRADING"),
            transport=transport,
        )
    if exchange == "gate":
        api_key, api_secret = require_env(("GATE_API_KEY", "GATE_API_SECRET"))
        return GateClient(api_key, api_secret, base_url, timeout, transport)
    if exchange == "bybit":
        api_key, api_secret = require_env(("BYBIT_API_KEY", "BYBIT_API_SECRET"))
        return BybitClient(api_key, api_secret, base_url, timeout, transport)
    if exchange == "bitget":
        api_key, api_secret, passphrase = require_env(
            ("BITGET_API_KEY", "BITGET_API_SECRET", "BITGET_API_PASSPHRASE")
        )
        return BitgetClient(
            api_key,
            api_secret,
            passphrase,
            base_url,
            timeout,
            target_uid=target_uid,
            transport=transport,
        )
    raise ApiError(f"unsupported exchange: {exchange}")


def describe_state(state: ModeState) -> str:
    label = "ON" if state.enabled is True else "OFF" if state.enabled is False else "UNKNOWN"
    return f"{label} ({state.detail})" if state.detail else label


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query or set Delta-neutral mode on Binance/OKX/Gate/Bybit/Bitget.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--exchange",
        required=True,
        choices=("binance", "okex", "okx", "gate", "bybit", "bitget"),
    )
    parser.add_argument("--state", choices=("on", "off"), default="on")
    parser.add_argument(
        "--env-file",
        help="env.sh to source; otherwise ./env.sh is sourced when present",
    )
    parser.add_argument(
        "--base-url",
        help="REST base URL override; exchange-specific environment variables are used otherwise",
    )
    parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout in seconds")
    parser.add_argument(
        "--target-uid",
        help="Bitget sub-account UID operated by the master account",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="submit the account setting change; default only prints the plan",
    )
    query_group = parser.add_mutually_exclusive_group()
    query_group.add_argument(
        "--query",
        dest="query",
        action="store_true",
        help="query current state before setting and verify afterward",
    )
    query_group.add_argument(
        "--set-only",
        dest="query",
        action="store_false",
        help=argparse.SUPPRESS,
    )
    parser.set_defaults(query=False)
    return parser.parse_args(argv)


def run(args: argparse.Namespace, transport: Transport = http_request) -> int:
    exchange = normalize_exchange(args.exchange)
    if args.timeout <= 0:
        raise ApiError("--timeout must be positive")
    if args.target_uid and exchange != "bitget":
        raise ApiError("--target-uid is supported only for Bitget")

    maybe_source_env_file(args.env_file)
    base_url = resolve_base_url(exchange, args.base_url)
    client = create_client(exchange, base_url, args.timeout, args.target_uid, transport)
    target_enabled = args.state == "on"
    target_label = "ON" if target_enabled else "OFF"

    print(
        f"exchange={exchange} base_url={base_url} target={target_label} "
        f"execute={args.execute}"
    )
    if not args.query:
        print(f"plan={client.mutation_summary(target_enabled)}")
        if not args.execute:
            print("dry-run: no API request was sent; add --execute to submit the plan")
            return 0
        print(
            f"LIVE ACCOUNT CHANGE: setting {exchange} Delta-neutral mode to {target_label} "
            "without state queries"
        )
        client.set_enabled(target_enabled)
        print("request accepted by exchange; state queries skipped by default set-only mode")
        return 0

    before = client.query()
    print(f"current={describe_state(before)}")

    if before.enabled is target_enabled:
        print(f"already set: Delta-neutral mode is {target_label}; no mutation needed")
        return 0

    print(f"plan={client.mutation_summary(target_enabled)}")
    if not args.execute:
        print("dry-run: no account setting was changed; add --execute to submit the plan")
        return 0

    print(f"LIVE ACCOUNT CHANGE: setting {exchange} Delta-neutral mode to {target_label}")
    client.set_enabled(target_enabled)
    print("request accepted by exchange")

    if exchange == "binance":
        print(
            "post-check skipped: Binance Delta Mode GET costs 1500 IP weight; "
            "run this script again without --execute to confirm later"
        )
        return 0
    if exchange == "bitget" and args.target_uid:
        print("post-check skipped: Bitget Delta-info does not accept targetUid")
        return 0

    after = client.query()
    print(f"after={describe_state(after)}")
    if after.enabled is not target_enabled:
        raise ApiError(
            "the mutation request was accepted, but the post-check did not confirm the target state"
        )
    print(f"verified: Delta-neutral mode is {target_label}")
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    try:
        return run(parse_args(argv))
    except ApiError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
