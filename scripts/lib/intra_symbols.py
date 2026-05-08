"""Intra arb fwd/bwd trade-symbol whitelist reader.

Mirrors the convention used by scripts/set_intra_futures_leverage.py
(`intra_<fwd|bwd>_trade_symbols:{exchange}` keys, NAMESPACE='intra').
"""

from __future__ import annotations

import json
import os
from typing import Any, List, Optional, Set


NAMESPACE = "intra"


def _try_import_redis():
    try:
        import redis  # type: ignore
        return redis
    except Exception:
        return None


def normalize_asset(value: str) -> str:
    """Reverse-map any of:
        BTCUSDT / BTC-USDT-SWAP / BTC_USDT / BTC
    to the base asset 'BTC' (uppercase). Empty if value is empty / quote-only.
    """
    text = (value or "").strip().upper()
    if text.endswith("-USDT-SWAP"):
        text = text[: -len("-USDT-SWAP")]
    if text.endswith("_USDT"):
        text = text[: -len("_USDT")]
    cleaned = "".join(ch for ch in text if ch.isalnum())
    if cleaned.endswith("USDT") and len(cleaned) > 4:
        return cleaned[:-4]
    return cleaned


def decode_redis_symbol_list(raw: Any, key: str) -> List[str]:
    if not raw:
        return []
    text = raw.decode("utf-8", "ignore") if isinstance(raw, (bytes, bytearray)) else str(raw)
    try:
        parsed = json.loads(text)
    except Exception as exc:
        raise RuntimeError(f"failed to parse Redis JSON list {key}: {exc}") from exc
    if not isinstance(parsed, list):
        raise RuntimeError(f"Redis key is not a JSON list: {key}")
    return [str(item).strip() for item in parsed if str(item).strip()]


def fetch_intra_in_scope_assets(
    exchange: str,
    *,
    redis_client: Optional[Any] = None,
    redis_host: Optional[str] = None,
    redis_port: Optional[int] = None,
    redis_db: Optional[int] = None,
    redis_password: Optional[str] = None,
    verbose: bool = False,
) -> Set[str]:
    """Return the union of fwd/bwd trade-symbol assets for `exchange`.

    Each symbol is normalized to its base asset (uppercase). USDT/quote is
    naturally absent from these lists.

    `redis_client` overrides connection params if provided. Otherwise host /
    port / db / password fall back to env vars REDIS_HOST / REDIS_PORT /
    REDIS_DB / REDIS_PASSWORD (defaults: 127.0.0.1 / 6379 / 0 / "").
    """
    client = redis_client
    if client is None:
        redis = _try_import_redis()
        if redis is None:
            raise SystemExit("redis package is not installed; pip install redis")
        host = redis_host or os.environ.get("REDIS_HOST", "127.0.0.1")
        port = redis_port if redis_port is not None else int(os.environ.get("REDIS_PORT", "6379"))
        db = redis_db if redis_db is not None else int(os.environ.get("REDIS_DB", "0"))
        password = redis_password if redis_password is not None else os.environ.get("REDIS_PASSWORD", "")
        client = redis.Redis(host=host, port=port, db=db, password=password or None)

    keys = [
        f"{NAMESPACE}_fwd_trade_symbols:{exchange}",
        f"{NAMESPACE}_bwd_trade_symbols:{exchange}",
    ]
    assets: Set[str] = set()
    for key in keys:
        values = decode_redis_symbol_list(client.get(key), key)
        if verbose:
            print(f"[redis] {key}: {len(values)} symbols")
        for value in values:
            base = normalize_asset(value)
            if base:
                assets.add(base)
    return assets
