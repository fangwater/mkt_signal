#!/usr/bin/env python3
"""Capture exchange instrument rules remotely and load an immutable PG snapshot."""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import shlex
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Sequence


SCHEMA_VERSION = 1
DEFAULT_COLLECTOR_HOST = "jp-meta-elvpn"
DEFAULT_DATABASE_HOST = "el_dev"
DEFAULT_DATABASE_ENV = "~/.config/crypto-cta-manager/database.env"
DEFAULT_DATABASE_URL_ENV = "CRYPTO_CTA_DATABASE_URL"
DEFAULT_OUTPUT_ROOT = Path("data/instrument_snapshots")
DEFAULT_SCHEMA_SQL = Path(__file__).with_name("instrument_snapshot_schema.sql")
DEFAULT_TIMEOUT_SEC = 20.0
DEFAULT_RETRIES = 3
USER_AGENT = "mkt-signal-instrument-snapshot/1"
SSH_OPTIONS = ("-o", "BatchMode=yes", "-o", "ConnectTimeout=15")


class SnapshotError(RuntimeError):
    pass


@dataclass(frozen=True)
class SourceSpec:
    source_id: str
    exchange: str
    market_types: tuple[str, ...]
    url: str
    parser: str
    headers: tuple[tuple[str, str], ...] = ()
    paginated: bool = False


SOURCES = (
    SourceSpec(
        "binance_spot",
        "binance",
        ("spot", "margin"),
        "https://api.binance.com/api/v3/exchangeInfo",
        "binance",
    ),
    SourceSpec(
        "binance_futures",
        "binance",
        ("futures",),
        "https://fapi.binance.com/fapi/v1/exchangeInfo",
        "binance",
    ),
    SourceSpec(
        "okx_spot",
        "okx",
        ("spot",),
        "https://www.okx.com/api/v5/public/instruments?instType=SPOT",
        "okx",
    ),
    SourceSpec(
        "okx_margin",
        "okx",
        ("margin",),
        "https://www.okx.com/api/v5/public/instruments?instType=MARGIN",
        "okx",
    ),
    SourceSpec(
        "okx_swap",
        "okx",
        ("futures",),
        "https://www.okx.com/api/v5/public/instruments?instType=SWAP",
        "okx",
    ),
    SourceSpec(
        "bybit_spot",
        "bybit",
        ("spot", "margin"),
        "https://api.bybit.com/v5/market/instruments-info?category=spot",
        "bybit",
        paginated=True,
    ),
    SourceSpec(
        "bybit_linear",
        "bybit",
        ("futures",),
        "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000",
        "bybit",
        paginated=True,
    ),
    SourceSpec(
        "gate_spot",
        "gate",
        ("spot", "margin"),
        "https://api.gateio.ws/api/v4/spot/currency_pairs",
        "gate_spot",
    ),
    SourceSpec(
        "gate_futures",
        "gate",
        ("futures",),
        "https://api.gateio.ws/api/v4/futures/usdt/contracts",
        "gate_futures",
        headers=(("X-Gate-Size-Decimal", "1"),),
    ),
    SourceSpec(
        "bitget_spot",
        "bitget",
        ("spot",),
        "https://api.bitget.com/api/v3/market/instruments?category=SPOT",
        "bitget",
    ),
    SourceSpec(
        "bitget_margin",
        "bitget",
        ("margin",),
        "https://api.bitget.com/api/v3/market/instruments?category=MARGIN",
        "bitget",
    ),
    SourceSpec(
        "bitget_futures",
        "bitget",
        ("futures",),
        "https://api.bitget.com/api/v3/market/instruments?category=USDT-FUTURES",
        "bitget",
    ),
)
VALID_EXCHANGES = tuple(sorted({source.exchange for source in SOURCES}))
VALID_MARKET_TYPES = ("spot", "margin", "futures")


@dataclass
class RawResponse:
    source: SourceSpec
    page: int
    market_types: tuple[str, ...]
    request_url: str
    request_headers: dict[str, str]
    response_headers: dict[str, str]
    http_status: int
    fetched_at: str
    body: bytes
    payload: Any
    filename: str = ""

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.body).hexdigest()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_utc(value: str, field: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SnapshotError(f"{field} must be an ISO-8601 timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise SnapshotError(f"{field} must include a timezone: {value!r}")
    return iso_utc(parsed)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    return value


def compact_json(value: Any) -> str:
    return json.dumps(jsonable(value), ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def decimal_text(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        return value if value.strip() else None
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    return None


def decimal_fields(
    value: Any,
    *,
    positive: bool = False,
    nonnegative: bool = False,
    integer_parts: bool = False,
) -> dict[str, Any]:
    raw = decimal_text(value)
    if raw is None:
        return {"raw": None, "value": None, "integer": None, "scale": None}
    try:
        number = Decimal(raw.strip())
    except InvalidOperation as exc:
        raise SnapshotError(f"invalid decimal value: {raw!r}") from exc
    if not number.is_finite():
        raise SnapshotError(f"non-finite decimal value: {raw!r}")
    if positive and number <= 0:
        return {"raw": None, "value": None, "integer": None, "scale": None}
    if nonnegative and number < 0:
        raise SnapshotError(f"negative decimal value is not allowed: {raw!r}")

    canonical = number.normalize()
    value_text = format(canonical, "f")
    integer: str | None = None
    scale: int | None = None
    if integer_parts:
        sign, digits, exponent = canonical.as_tuple()
        coefficient = int("".join(str(digit) for digit in digits) or "0")
        if sign:
            coefficient = -coefficient
        if exponent >= 0:
            coefficient *= 10**exponent
            scale = 0
        else:
            scale = -exponent
        integer = str(coefficient)
        if len(integer.lstrip("-")) > 78:
            raise SnapshotError(f"decimal coefficient exceeds NUMERIC(78): {raw!r}")
        if scale > 32767:
            raise SnapshotError(f"decimal scale exceeds SMALLINT: {raw!r}")
    return {"raw": raw, "value": value_text, "integer": integer, "scale": scale}


def set_decimal(
    row: dict[str, Any],
    name: str,
    value: Any,
    *,
    positive: bool = False,
    nonnegative: bool = False,
    integer_parts: bool = False,
    source: str | None = None,
) -> None:
    fields = decimal_fields(
        value,
        positive=positive,
        nonnegative=nonnegative,
        integer_parts=integer_parts,
    )
    row[f"{name}_raw"] = fields["raw"]
    row[name] = fields["value"]
    if integer_parts:
        row[f"{name}_integer"] = fields["integer"]
        row[f"{name}_scale"] = fields["scale"]
        row[f"{name}_source"] = source if fields["value"] is not None else None


def first_value(value: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        candidate = value.get(key)
        if candidate is not None and decimal_text(candidate) is not None:
            return candidate
    return None


def precision_step(value: Any) -> str | None:
    raw = decimal_text(value)
    if raw is None:
        return None
    try:
        precision = int(Decimal(raw))
    except (InvalidOperation, ValueError) as exc:
        raise SnapshotError(f"invalid precision: {raw!r}") from exc
    if precision < 0 or precision > 32767:
        raise SnapshotError(f"precision out of range: {precision}")
    return "1" if precision == 0 else f"0.{('0' * (precision - 1))}1"


def multiply_decimal(left: Any, right: Any) -> tuple[str | None, dict[str, Any]]:
    left_raw = decimal_text(left)
    right_raw = decimal_text(right)
    components = {"left_raw": left_raw, "right_raw": right_raw}
    if left_raw is None and right_raw is None:
        return None, components
    try:
        product = Decimal(left_raw or "1") * Decimal(right_raw or "1")
    except InvalidOperation as exc:
        raise SnapshotError(f"invalid multiplier components: {components}") from exc
    return format(product.normalize(), "f"), components


def normalized_symbol(instrument_id: str) -> str:
    return instrument_id.upper().replace("-SWAP", "").replace("-", "").replace("_", "")


def new_rule(
    source: SourceSpec,
    page: int,
    market_type: str,
    instrument_id: Any,
    raw_instrument: dict[str, Any],
) -> dict[str, Any]:
    instrument = str(instrument_id).upper()
    return {
        "exchange": source.exchange,
        "market_type": market_type,
        "instrument_id": instrument,
        "symbol": normalized_symbol(instrument),
        "base_asset": None,
        "quote_asset": None,
        "contract_type": None,
        "status": None,
        "contract_multiplier_components": {},
        "source_id": source.source_id,
        "source_page": page,
        "raw_instrument": jsonable(raw_instrument),
    }


def filter_map(filters: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for item in filters:
        filter_type = item.get("filterType")
        if filter_type is not None:
            result[str(filter_type)] = item
    return result


def normalize_binance(
    source: SourceSpec,
    pages: Sequence[RawResponse],
    market_types: Sequence[str],
) -> list[dict[str, Any]]:
    if len(pages) != 1 or not isinstance(pages[0].payload, dict):
        raise SnapshotError(f"{source.source_id}: unexpected Binance response shape")
    symbols = pages[0].payload.get("symbols")
    if not isinstance(symbols, list):
        raise SnapshotError(f"{source.source_id}: response missing symbols[]")
    rows: list[dict[str, Any]] = []
    for instrument in symbols:
        if not isinstance(instrument, dict):
            raise SnapshotError(f"{source.source_id}: symbol entry is not an object")
        instrument_id = instrument.get("symbol")
        if not instrument_id:
            raise SnapshotError(f"{source.source_id}: symbol entry missing symbol")
        filters = filter_map(instrument.get("filters") or [])
        lot = filters.get("LOT_SIZE", {})
        market_lot = filters.get("MARKET_LOT_SIZE", {})
        price = filters.get("PRICE_FILTER", {})
        notional = filters.get("NOTIONAL") or filters.get("MIN_NOTIONAL") or {}
        for market_type in market_types:
            row = new_rule(source, 1, market_type, instrument_id, instrument)
            row.update(
                base_asset=str(instrument.get("baseAsset") or "").upper() or None,
                quote_asset=str(instrument.get("quoteAsset") or "").upper() or None,
                contract_type=str(instrument.get("contractType") or "") or None,
                status=str(instrument.get("status") or "") or None,
            )
            set_decimal(row, "price_tick", price.get("tickSize"), positive=True, integer_parts=True, source="exchange")
            set_decimal(row, "qty_step", lot.get("stepSize"), positive=True, integer_parts=True, source="exchange")
            set_decimal(row, "min_qty", lot.get("minQty"), nonnegative=True)
            set_decimal(row, "max_qty", lot.get("maxQty"), positive=True)
            set_decimal(row, "market_qty_step", market_lot.get("stepSize"), positive=True)
            set_decimal(row, "market_min_qty", market_lot.get("minQty"), nonnegative=True)
            set_decimal(row, "market_max_qty", market_lot.get("maxQty"), positive=True)
            set_decimal(row, "min_notional", first_value(notional, "minNotional", "notional"), nonnegative=True)
            set_decimal(row, "max_notional", notional.get("maxNotional"), positive=True)
            set_decimal(row, "market_min_notional", first_value(notional, "minNotional", "notional"), nonnegative=True)
            set_decimal(row, "market_max_notional", notional.get("maxNotional"), positive=True)
            set_decimal(row, "contract_multiplier", "1", positive=True)
            rows.append(row)
    return rows


def normalize_okx(
    source: SourceSpec,
    pages: Sequence[RawResponse],
    market_types: Sequence[str],
) -> list[dict[str, Any]]:
    if len(pages) != 1 or not isinstance(pages[0].payload, dict):
        raise SnapshotError(f"{source.source_id}: unexpected OKX response shape")
    payload = pages[0].payload
    if str(payload.get("code")) != "0":
        raise SnapshotError(f"{source.source_id}: OKX API code={payload.get('code')} msg={payload.get('msg')}")
    instruments = payload.get("data")
    if not isinstance(instruments, list):
        raise SnapshotError(f"{source.source_id}: response missing data[]")
    rows: list[dict[str, Any]] = []
    for instrument in instruments:
        if not isinstance(instrument, dict) or not instrument.get("instId"):
            raise SnapshotError(f"{source.source_id}: malformed instrument")
        instrument_id = str(instrument["instId"])
        for market_type in market_types:
            if market_type in {"spot", "margin"} and not instrument_id.endswith("-USDT"):
                continue
            if market_type == "futures":
                if str(instrument.get("ctType") or "").lower() != "linear":
                    continue
                if str(instrument.get("settleCcy") or "").upper() != "USDT":
                    continue
            row = new_rule(source, 1, market_type, instrument_id, instrument)
            base_asset = instrument.get("baseCcy") or instrument_id.split("-")[0]
            quote_asset = instrument.get("quoteCcy") or ("USDT" if market_type == "futures" else None)
            row.update(
                base_asset=str(base_asset).upper() if base_asset else None,
                quote_asset=str(quote_asset).upper() if quote_asset else None,
                contract_type=str(instrument.get("ctType") or instrument.get("instType") or "") or None,
                status=str(instrument.get("state") or "") or None,
            )
            set_decimal(row, "price_tick", instrument.get("tickSz"), positive=True, integer_parts=True, source="exchange")
            set_decimal(row, "qty_step", instrument.get("lotSz"), positive=True, integer_parts=True, source="exchange")
            set_decimal(row, "min_qty", instrument.get("minSz"), nonnegative=True)
            set_decimal(row, "max_qty", first_value(instrument, "maxLmtSz", "maxTwapSz", "maxIcebergSz"), positive=True)
            set_decimal(row, "market_qty_step", instrument.get("lotSz"), positive=True)
            set_decimal(row, "market_min_qty", instrument.get("minSz"), nonnegative=True)
            set_decimal(row, "market_max_qty", instrument.get("maxMktSz"), positive=True)
            set_decimal(row, "min_notional", None, nonnegative=True)
            set_decimal(row, "max_notional", instrument.get("maxLmtAmt"), positive=True)
            set_decimal(row, "market_min_notional", None, nonnegative=True)
            set_decimal(row, "market_max_notional", instrument.get("maxMktAmt"), positive=True)
            if market_type == "futures":
                multiplier, components = multiply_decimal(instrument.get("ctVal"), instrument.get("ctMult"))
                components.update(ct_val_ccy=jsonable(instrument.get("ctValCcy")))
            else:
                multiplier, components = "1", {}
            row["contract_multiplier_components"] = components
            set_decimal(row, "contract_multiplier", multiplier, positive=True)
            rows.append(row)
    return rows


def normalize_bybit(
    source: SourceSpec,
    pages: Sequence[RawResponse],
    market_types: Sequence[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for response in pages:
        payload = response.payload
        if not isinstance(payload, dict) or int(payload.get("retCode", -1)) != 0:
            raise SnapshotError(
                f"{source.source_id}: Bybit API code={payload.get('retCode') if isinstance(payload, dict) else None}"
            )
        result = payload.get("result")
        instruments = result.get("list") if isinstance(result, dict) else None
        if not isinstance(instruments, list):
            raise SnapshotError(f"{source.source_id}: response missing result.list[]")
        for instrument in instruments:
            if not isinstance(instrument, dict) or not instrument.get("symbol"):
                raise SnapshotError(f"{source.source_id}: malformed instrument")
            if str(instrument.get("quoteCoin") or "").upper() != "USDT":
                continue
            lot = instrument.get("lotSizeFilter") or {}
            price = instrument.get("priceFilter") or {}
            for market_type in market_types:
                if market_type == "futures" and str(instrument.get("contractType") or "").lower() != "linearperpetual":
                    continue
                row = new_rule(source, response.page, market_type, instrument["symbol"], instrument)
                row.update(
                    base_asset=str(instrument.get("baseCoin") or "").upper() or None,
                    quote_asset=str(instrument.get("quoteCoin") or "").upper() or None,
                    contract_type=str(instrument.get("contractType") or "spot") or None,
                    status=str(instrument.get("status") or "") or None,
                )
                qty_step = lot.get("qtyStep") if market_type == "futures" else lot.get("basePrecision")
                min_notional = lot.get("minNotionalValue") if market_type == "futures" else lot.get("minOrderAmt")
                set_decimal(row, "price_tick", price.get("tickSize"), positive=True, integer_parts=True, source="exchange")
                set_decimal(row, "qty_step", qty_step, positive=True, integer_parts=True, source="exchange")
                set_decimal(row, "min_qty", lot.get("minOrderQty"), nonnegative=True)
                set_decimal(row, "max_qty", lot.get("maxOrderQty"), positive=True)
                set_decimal(row, "market_qty_step", qty_step, positive=True)
                set_decimal(row, "market_min_qty", lot.get("minOrderQty"), nonnegative=True)
                set_decimal(row, "market_max_qty", first_value(lot, "maxMktOrderQty", "maxMarketOrderQty"), positive=True)
                set_decimal(row, "min_notional", min_notional, nonnegative=True)
                set_decimal(row, "max_notional", lot.get("maxOrderAmt"), positive=True)
                set_decimal(row, "market_min_notional", min_notional, nonnegative=True)
                set_decimal(row, "market_max_notional", first_value(lot, "maxMktOrderAmt", "maxMarketOrderAmt"), positive=True)
                set_decimal(row, "contract_multiplier", "1", positive=True)
                rows.append(row)
    return rows


def normalize_gate_spot(
    source: SourceSpec,
    pages: Sequence[RawResponse],
    market_types: Sequence[str],
) -> list[dict[str, Any]]:
    if len(pages) != 1 or not isinstance(pages[0].payload, list):
        raise SnapshotError(f"{source.source_id}: unexpected Gate spot response shape")
    rows: list[dict[str, Any]] = []
    for instrument in pages[0].payload:
        if not isinstance(instrument, dict) or not instrument.get("id"):
            raise SnapshotError(f"{source.source_id}: malformed currency pair")
        price_step = precision_step(instrument.get("precision"))
        qty_step = precision_step(instrument.get("amount_precision"))
        for market_type in market_types:
            row = new_rule(source, 1, market_type, instrument["id"], instrument)
            row.update(
                base_asset=str(instrument.get("base") or "").upper() or None,
                quote_asset=str(instrument.get("quote") or "").upper() or None,
                contract_type="spot",
                status=str(instrument.get("trade_status") or "") or None,
            )
            set_decimal(row, "price_tick", price_step, positive=True, integer_parts=True, source="precision_derived")
            set_decimal(row, "qty_step", qty_step, positive=True, integer_parts=True, source="precision_derived")
            set_decimal(row, "min_qty", instrument.get("min_base_amount"), nonnegative=True)
            set_decimal(row, "max_qty", instrument.get("max_base_amount"), positive=True)
            set_decimal(row, "market_qty_step", qty_step, positive=True)
            set_decimal(row, "market_min_qty", instrument.get("min_base_amount"), nonnegative=True)
            set_decimal(row, "market_max_qty", instrument.get("max_base_amount"), positive=True)
            set_decimal(row, "min_notional", instrument.get("min_quote_amount"), nonnegative=True)
            set_decimal(row, "max_notional", instrument.get("max_quote_amount"), positive=True)
            set_decimal(row, "market_min_notional", instrument.get("min_quote_amount"), nonnegative=True)
            set_decimal(row, "market_max_notional", instrument.get("max_quote_amount"), positive=True)
            set_decimal(row, "contract_multiplier", "1", positive=True)
            rows.append(row)
    return rows


def normalize_gate_futures(
    source: SourceSpec,
    pages: Sequence[RawResponse],
    market_types: Sequence[str],
) -> list[dict[str, Any]]:
    if len(pages) != 1 or not isinstance(pages[0].payload, list):
        raise SnapshotError(f"{source.source_id}: unexpected Gate futures response shape")
    rows: list[dict[str, Any]] = []
    for instrument in pages[0].payload:
        if not isinstance(instrument, dict) or not instrument.get("name"):
            raise SnapshotError(f"{source.source_id}: malformed contract")
        instrument_id = str(instrument["name"])
        base, _, quote = instrument_id.partition("_")
        min_qty = instrument.get("order_size_min")
        step = instrument.get("order_size_step")
        step_source = "exchange"
        if decimal_text(step) is None:
            enable_decimal = bool(instrument.get("enable_decimal"))
            minimum = Decimal(decimal_text(min_qty) or "0")
            if enable_decimal and minimum > 0 and minimum < 1:
                step = min_qty
                step_source = "min_qty_fallback"
            else:
                step = "1"
                step_source = "contract_unit_fallback"
        status = instrument.get("status") or instrument.get("trade_status")
        if status is None:
            status = "delisting" if bool(instrument.get("in_delisting")) else "trading"
        for market_type in market_types:
            row = new_rule(source, 1, market_type, instrument_id, instrument)
            row.update(
                base_asset=base.upper() or None,
                quote_asset=(quote or "USDT").upper(),
                contract_type=str(instrument.get("type") or "perpetual"),
                status=str(status),
            )
            set_decimal(row, "price_tick", instrument.get("order_price_round"), positive=True, integer_parts=True, source="exchange")
            set_decimal(row, "qty_step", step, positive=True, integer_parts=True, source=step_source)
            set_decimal(row, "min_qty", min_qty, nonnegative=True)
            set_decimal(row, "max_qty", instrument.get("order_size_max"), positive=True)
            set_decimal(row, "market_qty_step", step, positive=True)
            set_decimal(row, "market_min_qty", min_qty, nonnegative=True)
            set_decimal(row, "market_max_qty", instrument.get("market_order_size_max"), positive=True)
            set_decimal(row, "min_notional", instrument.get("order_value_min"), nonnegative=True)
            set_decimal(row, "max_notional", instrument.get("order_value_max"), positive=True)
            set_decimal(row, "market_min_notional", instrument.get("market_order_value_min"), nonnegative=True)
            set_decimal(row, "market_max_notional", instrument.get("market_order_value_max"), positive=True)
            row["contract_multiplier_components"] = {"quanto_multiplier_raw": decimal_text(instrument.get("quanto_multiplier"))}
            set_decimal(row, "contract_multiplier", instrument.get("quanto_multiplier"), positive=True)
            rows.append(row)
    return rows


def normalize_bitget(
    source: SourceSpec,
    pages: Sequence[RawResponse],
    market_types: Sequence[str],
) -> list[dict[str, Any]]:
    if len(pages) != 1 or not isinstance(pages[0].payload, dict):
        raise SnapshotError(f"{source.source_id}: unexpected Bitget response shape")
    payload = pages[0].payload
    if str(payload.get("code")) != "00000":
        raise SnapshotError(f"{source.source_id}: Bitget API code={payload.get('code')} msg={payload.get('msg')}")
    instruments = payload.get("data")
    if not isinstance(instruments, list):
        raise SnapshotError(f"{source.source_id}: response missing data[]")
    rows: list[dict[str, Any]] = []
    for instrument in instruments:
        if not isinstance(instrument, dict) or not instrument.get("symbol"):
            raise SnapshotError(f"{source.source_id}: malformed instrument")
        explicit_price = first_value(instrument, "priceMultiplier", "priceStep", "tickSize")
        explicit_qty = first_value(instrument, "quantityMultiplier", "quantityStep", "qtyStep")
        price_step = explicit_price or precision_step(first_value(instrument, "pricePrecision", "priceScale"))
        qty_step = explicit_qty or precision_step(first_value(instrument, "quantityPrecision", "quantityScale"))
        price_source = "exchange" if explicit_price is not None else "precision_derived"
        qty_source = "exchange" if explicit_qty is not None else "precision_derived"
        for market_type in market_types:
            row = new_rule(source, 1, market_type, instrument["symbol"], instrument)
            row.update(
                base_asset=str(instrument.get("baseCoin") or instrument.get("baseCurrency") or "").upper() or None,
                quote_asset=str(instrument.get("quoteCoin") or instrument.get("quoteCurrency") or "").upper() or None,
                contract_type=str(instrument.get("symbolType") or instrument.get("contractType") or market_type),
                status=str(instrument.get("status") or instrument.get("symbolStatus") or "") or None,
            )
            set_decimal(row, "price_tick", price_step, positive=True, integer_parts=True, source=price_source)
            set_decimal(row, "qty_step", qty_step, positive=True, integer_parts=True, source=qty_source)
            set_decimal(row, "min_qty", first_value(instrument, "minOrderQty", "minTradeAmount"), nonnegative=True)
            set_decimal(row, "max_qty", first_value(instrument, "maxOrderQty", "maxTradeAmount"), positive=True)
            set_decimal(row, "market_qty_step", qty_step, positive=True)
            set_decimal(row, "market_min_qty", first_value(instrument, "minMarketOrderQty", "minOrderQty"), nonnegative=True)
            set_decimal(row, "market_max_qty", first_value(instrument, "maxMarketOrderQty", "maxMktOrderQty"), positive=True)
            set_decimal(row, "min_notional", first_value(instrument, "minOrderAmount", "minTradeUSDT"), nonnegative=True)
            set_decimal(row, "max_notional", instrument.get("maxOrderAmount"), positive=True)
            set_decimal(row, "market_min_notional", first_value(instrument, "minMarketOrderAmount", "minOrderAmount"), nonnegative=True)
            set_decimal(row, "market_max_notional", instrument.get("maxMarketOrderAmount"), positive=True)
            set_decimal(row, "contract_multiplier", "1", positive=True)
            rows.append(row)
    return rows


NORMALIZERS = {
    "binance": normalize_binance,
    "okx": normalize_okx,
    "bybit": normalize_bybit,
    "gate_spot": normalize_gate_spot,
    "gate_futures": normalize_gate_futures,
    "bitget": normalize_bitget,
}


ACTIVE_STATUSES = {
    "",
    "trading",
    "live",
    "normal",
    "online",
    "listed",
    "tradable",
    "buyable",
}


def finalize_rules(
    rows: list[dict[str, Any]],
    *,
    snapshot_id: str,
    captured_at: str,
    effective_from: str,
) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    finalized: list[dict[str, Any]] = []
    for row in rows:
        key = (row["exchange"], row["market_type"], row["instrument_id"])
        if key in seen:
            raise SnapshotError(f"duplicate normalized instrument: {key}")
        seen.add(key)
        status = str(row.get("status") or "").strip().lower()
        if status in ACTIVE_STATUSES:
            missing = [field for field in ("price_tick", "qty_step", "min_qty", "contract_multiplier") if row.get(field) is None]
            if missing:
                raise SnapshotError(f"active instrument {key} missing required fields: {', '.join(missing)}")
            if Decimal(str(row["min_qty"])) <= 0:
                raise SnapshotError(f"active instrument {key} has non-positive min_qty={row['min_qty']}")
        hash_payload = {
            key_name: value
            for key_name, value in row.items()
            if key_name not in {"source_id", "source_page", "raw_instrument"}
        }
        row.update(
            snapshot_id=snapshot_id,
            captured_at=captured_at,
            effective_from=effective_from,
            rule_sha256=hashlib.sha256(compact_json(hash_payload).encode("utf-8")).hexdigest(),
        )
        finalized.append(row)
    finalized.sort(key=lambda item: (item["exchange"], item["market_type"], item["instrument_id"]))
    return finalized
