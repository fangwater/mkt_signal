"""Signed REST helpers for Binance / OKX / Bybit / Gate / Bitget.

Extracted from scripts/check_balance.py so that close/full-exit scripts and the
balance probe share one signing implementation.

Each `signed_get_*` helper returns a tuple
    (success: bool, status: int, body: str, error: Optional[str], signed_path: str)
to mirror the original check_balance.py shape. Binance keeps the four-tuple
shape (no signed_path) used by the original script.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple


def now_ms() -> int:
    return int(time.time() * 1000)


def utc_timestamp_iso_ms() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "")
    if not raw:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def to_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def parse_json_any(body: str) -> Optional[Any]:
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return None


# ----- Binance -----

def sign_binance(query: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def signed_get_binance(
    base_url: str,
    path: str,
    params: Dict[str, Any],
    api_key: str,
    api_secret: str,
    timeout: int,
) -> Tuple[bool, int, str, Optional[str]]:
    q = dict(params)
    q.setdefault("recvWindow", "5000")
    q["timestamp"] = str(now_ms())
    query = urllib.parse.urlencode(sorted((k, str(v)) for k, v in q.items()), safe="-_.~")
    signature = sign_binance(query, api_secret)
    url = f"{base_url.rstrip('/')}{path}?{query}&signature={signature}"

    req = urllib.request.Request(url, method="GET", headers={"X-MBX-APIKEY": api_key})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return True, resp.getcode(), body, None
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return False, exc.code, body, f"HTTPError: {exc.code} {exc.reason}"
    except urllib.error.URLError as exc:
        return False, 0, "", f"URLError: {exc.reason}"


# ----- Gate -----

def sign_gate(method: str, signed_path: str, query: str, body: str, secret: str, timestamp: str) -> str:
    body_hash = hashlib.sha512(body.encode("utf-8")).hexdigest()
    signing_payload = f"{method}\n{signed_path}\n{query}\n{body_hash}\n{timestamp}"
    return hmac.new(secret.encode("utf-8"), signing_payload.encode("utf-8"), hashlib.sha512).hexdigest()


def signed_get_gate(
    base_url: str,
    api_prefix: str,
    path: str,
    params: Dict[str, Any],
    api_key: str,
    api_secret: str,
    timeout: int,
) -> Tuple[bool, int, str, Optional[str], str]:
    query = urllib.parse.urlencode(sorted((k, str(v)) for k, v in params.items()), safe="-_.~")
    normalized_prefix = "/" + api_prefix.strip("/")
    signed_path = f"{normalized_prefix}{path}"
    timestamp = str(int(time.time()))
    signature = sign_gate("GET", signed_path, query, "", api_secret, timestamp)

    url = f"{base_url.rstrip('/')}{signed_path}"
    if query:
        url = f"{url}?{query}"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "KEY": api_key,
            "Timestamp": timestamp,
            "SIGN": signature,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return True, resp.getcode(), body, None, signed_path
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return False, exc.code, body, f"HTTPError: {exc.code} {exc.reason}", signed_path
    except urllib.error.URLError as exc:
        return False, 0, "", f"URLError: {exc.reason}", signed_path


# ----- OKX -----

def sign_okx(timestamp: str, method: str, signed_path: str, body: str, secret: str) -> str:
    payload = f"{timestamp}{method.upper()}{signed_path}{body}"
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def signed_get_okx(
    base_url: str,
    path: str,
    params: Dict[str, Any],
    api_key: str,
    api_secret: str,
    passphrase: str,
    timeout: int,
    simulated: bool = False,
) -> Tuple[bool, int, str, Optional[str], str]:
    query = urllib.parse.urlencode(sorted((k, str(v)) for k, v in params.items()), safe="-_.~")
    signed_path = path if not query else f"{path}?{query}"
    timestamp = utc_timestamp_iso_ms()
    signature = sign_okx(timestamp, "GET", signed_path, "", api_secret)

    url = f"{base_url.rstrip('/')}{signed_path}"
    headers = {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
    }
    if simulated:
        headers["x-simulated-trading"] = "1"

    req = urllib.request.Request(url, method="GET", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return True, resp.getcode(), body, None, signed_path
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return False, exc.code, body, f"HTTPError: {exc.code} {exc.reason}", signed_path
    except urllib.error.URLError as exc:
        return False, 0, "", f"URLError: {exc.reason}", signed_path


# ----- Bybit -----

def sign_bybit(timestamp_ms: str, api_key: str, recv_window: str, query: str, secret: str) -> str:
    payload = f"{timestamp_ms}{api_key}{recv_window}{query}"
    return hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


def signed_get_bybit(
    base_url: str,
    path: str,
    params: Dict[str, Any],
    api_key: str,
    api_secret: str,
    recv_window: int,
    timeout: int,
) -> Tuple[bool, int, str, Optional[str], str]:
    query = urllib.parse.urlencode(sorted((k, str(v)) for k, v in params.items()), safe="-_.~")
    timestamp_ms = str(now_ms())
    recv_window_str = str(recv_window)
    signature = sign_bybit(timestamp_ms, api_key, recv_window_str, query, api_secret)

    url = f"{base_url.rstrip('/')}{path}"
    if query:
        url = f"{url}?{query}"
    signed_path = path if not query else f"{path}?{query}"

    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": timestamp_ms,
        "X-BAPI-RECV-WINDOW": recv_window_str,
        "Content-Type": "application/json",
    }
    req = urllib.request.Request(url, method="GET", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return True, resp.getcode(), body, None, signed_path
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return False, exc.code, body, f"HTTPError: {exc.code} {exc.reason}", signed_path
    except urllib.error.URLError as exc:
        return False, 0, "", f"URLError: {exc.reason}", signed_path


# ----- Bitget -----

def sign_bitget(timestamp_ms: str, method: str, signed_path: str, body: str, secret: str) -> str:
    payload = f"{timestamp_ms}{method.upper()}{signed_path}{body}"
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def signed_get_bitget(
    base_url: str,
    path: str,
    params: Dict[str, Any],
    api_key: str,
    api_secret: str,
    passphrase: str,
    timeout: int,
) -> Tuple[bool, int, str, Optional[str], str]:
    query = urllib.parse.urlencode(sorted((k, str(v)) for k, v in params.items()), safe="-_.~")
    signed_path = path if not query else f"{path}?{query}"
    timestamp_ms = str(now_ms())
    signature = sign_bitget(timestamp_ms, "GET", signed_path, "", api_secret)

    url = f"{base_url.rstrip('/')}{signed_path}"
    headers = {
        "ACCESS-KEY": api_key,
        "ACCESS-SIGN": signature,
        "ACCESS-TIMESTAMP": timestamp_ms,
        "ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json",
        "locale": "zh-CN",
    }
    req = urllib.request.Request(url, method="GET", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return True, resp.getcode(), body, None, signed_path
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return False, exc.code, body, f"HTTPError: {exc.code} {exc.reason}", signed_path
    except urllib.error.URLError as exc:
        return False, 0, "", f"URLError: {exc.reason}", signed_path


# ----- Public (unsigned) GET -----

def public_get(
    url: str,
    *,
    timeout: int = 10,
    headers: Optional[Dict[str, str]] = None,
) -> Tuple[bool, int, str, Optional[str]]:
    req = urllib.request.Request(url, method="GET", headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return True, resp.getcode(), body, None
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return False, exc.code, body, f"HTTPError: {exc.code} {exc.reason}"
    except urllib.error.URLError as exc:
        return False, 0, "", f"URLError: {exc.reason}"
