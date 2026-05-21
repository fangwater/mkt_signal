"""Compute per-asset exposure rows for an intra arb environment by querying
the exchange directly, mirroring the field semantics defined in the Rust
`src/pre_trade/basic_exposure_manager.rs`.

The canonical entry shape (BasicExposureEntry) is the Python mirror of the
Rust struct of the same name:

    asset       : uppercase base coin
    balance     : spot net (= equity, already netted with debt)
    borrowed    : outstanding borrow (gross)
    interest    : accrued interest
    um_position : futures base qty (already converted via contract multiplier)
    exposure    : balance + um_position
    mark_price  : USDT per base; 0 if unavailable

Callers convert this to their local ExposureRow shape (which varies across
intra_scripts/full_exit_intra_*.py and intra_scripts/flatten_*.py).

Asset whitelist comes from intra_<fwd|bwd>_trade_symbols Redis keys; on
Binance, BNB is additionally skipped (fee discount coin).
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# Make sibling lib modules importable when this file is loaded by
# sys.path-injecting callers in intra_scripts/.
_LIB_DIR = str(Path(__file__).resolve().parent)
if _LIB_DIR not in sys.path:
    sys.path.insert(0, _LIB_DIR)

from exchange_signing import (  # noqa: E402
    parse_json_any,
    public_get,
    signed_get_binance,
    signed_get_bitget,
    signed_get_bybit,
    signed_get_gate,
    signed_get_okx,
)
from intra_symbols import fetch_intra_in_scope_assets  # noqa: E402


SUPPORTED_EXCHANGES = {"okex", "gate", "bybit", "bitget"}


@dataclass
class BasicExposureEntry:
    asset: str
    balance: Decimal       # spot net (Rust BasicBalance.balance)
    borrowed: Decimal      # outstanding borrow
    interest: Decimal      # accrued interest
    um_position: Decimal   # futures base qty (signed)
    exposure: Decimal      # balance + um_position
    mark_price: Decimal    # USDT per base; 0 if unavailable


def _dec(value: Any, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return Decimal(default)


def _normalize_exchange(value: str) -> str:
    text = (value or "").strip().lower()
    return "okex" if text == "okx" else text


def fetch_exchange_state(
    exchange: str,
    suffix: str,
    *,
    redis_client: Optional[Any] = None,
    redis_host: Optional[str] = None,
    redis_port: Optional[int] = None,
    redis_db: Optional[int] = None,
    redis_password: Optional[str] = None,
    timeout: int = 10,
    verbose: bool = False,
) -> List[BasicExposureEntry]:
    """Read the in-scope asset list (Redis fwd+bwd) and return one
    BasicExposureEntry per non-zero asset on the exchange."""
    ex = _normalize_exchange(exchange)
    if ex not in SUPPORTED_EXCHANGES:
        raise ValueError(f"unsupported exchange: {exchange}")

    in_scope = fetch_intra_in_scope_assets(
        ex,
        redis_client=redis_client,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_password=redis_password,
        verbose=verbose,
    )
    if ex == "binance":
        in_scope.discard("BNB")

    if not in_scope:
        return []

    if ex == "okex":
        balances, positions, marks = _fetch_okx_state(in_scope, timeout=timeout, verbose=verbose)
    elif ex == "gate":
        balances, positions, marks = _fetch_gate_state(in_scope, timeout=timeout, verbose=verbose)
    elif ex == "bybit":
        balances, positions, marks = _fetch_bybit_state(in_scope, timeout=timeout, verbose=verbose)
    elif ex == "bitget":
        balances, positions, marks = _fetch_bitget_state(in_scope, timeout=timeout, verbose=verbose)
    else:
        raise ValueError(f"unsupported exchange: {exchange}")

    entries: List[BasicExposureEntry] = []
    for asset in sorted(in_scope):
        bal, borrowed, interest = balances.get(asset, (Decimal(0), Decimal(0), Decimal(0)))
        um = positions.get(asset, Decimal(0))
        if bal == 0 and um == 0 and borrowed == 0:
            continue
        mark = marks.get(asset, Decimal(0))
        entries.append(
            BasicExposureEntry(
                asset=asset,
                balance=bal,
                borrowed=borrowed,
                interest=interest,
                um_position=um,
                exposure=bal + um,
                mark_price=mark,
            )
        )
    return entries


# =============================================================================
# OKX
# =============================================================================

def _fetch_okx_state(
    in_scope: Set[str],
    *,
    timeout: int,
    verbose: bool,
) -> Tuple[
    Dict[str, Tuple[Decimal, Decimal, Decimal]],  # balances: asset -> (balance, borrowed, interest)
    Dict[str, Decimal],                            # positions: asset -> um base qty
    Dict[str, Decimal],                            # marks: asset -> USDT/base
]:
    api_key = os.environ.get("OKX_API_KEY", "").strip()
    api_secret = os.environ.get("OKX_API_SECRET", "").strip()
    passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()
    if not api_key or not api_secret or not passphrase:
        raise SystemExit("OKX_API_KEY / OKX_API_SECRET / OKX_PASSPHRASE not set")
    base_url = os.environ.get("OKX_BASE_URL", "https://openapi.okx.com")
    simulated = os.environ.get("OKX_SIMULATED_TRADING", "").strip().lower() in {"1", "true", "yes"}

    # ---- Balance ----
    balances: Dict[str, Tuple[Decimal, Decimal, Decimal]] = {}
    ok, status, body, err, _ = signed_get_okx(
        base_url, "/api/v5/account/balance", {}, api_key, api_secret, passphrase, timeout, simulated=simulated,
    )
    if not ok or status >= 300:
        raise SystemExit(f"OKX balance fetch failed: status={status} err={err} body={body[:300]}")
    payload = parse_json_any(body) or {}
    if str(payload.get("code", "")) != "0":
        raise SystemExit(f"OKX balance API error: {payload.get('msg')}")
    data = payload.get("data") or []
    if data and isinstance(data[0], dict):
        for item in data[0].get("details") or []:
            if not isinstance(item, dict):
                continue
            asset = str(item.get("ccy") or "").strip().upper()
            if not asset or asset not in in_scope:
                continue
            # Rust uses cashBal as the net spot balance (already netted with liabilities).
            balance = _dec(item.get("cashBal"))
            borrowed = _dec(item.get("liab"))  # cross + iso liabilities
            interest = _dec(item.get("interest"))
            balances[asset] = (balance, borrowed, interest)
    if verbose:
        print(f"[okex] balances: {len(balances)} in-scope assets with details")

    # ---- Positions (SWAP, USDT-margined) ----
    # Need ctVal to convert contracts -> base qty.
    ok, status, body, err = public_get(
        f"{base_url.rstrip('/')}/api/v5/public/instruments?instType=SWAP",
        timeout=timeout,
    )
    if not ok or status >= 300:
        raise SystemExit(f"OKX instruments fetch failed: status={status} err={err}")
    inst_payload = parse_json_any(body) or {}
    if str(inst_payload.get("code", "")) != "0":
        raise SystemExit(f"OKX instruments API error: {inst_payload.get('msg')}")
    ct_val_by_inst: Dict[str, Decimal] = {}
    for entry in inst_payload.get("data") or []:
        if not isinstance(entry, dict):
            continue
        inst_id = str(entry.get("instId") or "").upper()
        if not inst_id.endswith("-USDT-SWAP"):
            continue
        ct_val = _dec(entry.get("ctVal"))
        if ct_val > 0:
            ct_val_by_inst[inst_id] = ct_val

    ok, status, body, err, _ = signed_get_okx(
        base_url, "/api/v5/account/positions", {"instType": "SWAP"}, api_key, api_secret, passphrase, timeout,
        simulated=simulated,
    )
    if not ok or status >= 300:
        raise SystemExit(f"OKX positions fetch failed: status={status} err={err} body={body[:300]}")
    pos_payload = parse_json_any(body) or {}
    if str(pos_payload.get("code", "")) != "0":
        raise SystemExit(f"OKX positions API error: {pos_payload.get('msg')}")

    positions: Dict[str, Decimal] = {}
    for item in pos_payload.get("data") or []:
        if not isinstance(item, dict):
            continue
        inst_id = str(item.get("instId") or "").upper()
        if not inst_id.endswith("-USDT-SWAP"):
            continue
        asset = inst_id[: -len("-USDT-SWAP")]
        if asset not in in_scope:
            continue
        ct_val = ct_val_by_inst.get(inst_id, Decimal(0))
        if ct_val <= 0:
            continue
        # Net mode: pos is signed; long-short mode: posSide=long/short, pos is positive.
        pos = _dec(item.get("pos"))
        side = str(item.get("posSide") or "").lower()
        if side == "short" and pos > 0:
            pos = -pos
        positions[asset] = positions.get(asset, Decimal(0)) + pos * ct_val
    if verbose:
        print(f"[okex] positions: {len(positions)} in-scope assets with non-zero pos")

    # ---- Marks (use spot last in bulk) ----
    marks: Dict[str, Decimal] = {"USDT": Decimal(1)}
    ok, status, body, err = public_get(
        f"{base_url.rstrip('/')}/api/v5/market/tickers?instType=SPOT",
        timeout=timeout,
    )
    if ok and status < 300:
        ticker_payload = parse_json_any(body) or {}
        if str(ticker_payload.get("code", "")) == "0":
            for entry in ticker_payload.get("data") or []:
                if not isinstance(entry, dict):
                    continue
                inst_id = str(entry.get("instId") or "").upper()
                if not inst_id.endswith("-USDT"):
                    continue
                base = inst_id[: -len("-USDT")]
                if base not in in_scope:
                    continue
                last = _dec(entry.get("last"))
                if last > 0:
                    marks[base] = last
    return balances, positions, marks


# =============================================================================
# Gate / Bybit / Bitget / Binance — TODO in subsequent tasks
# =============================================================================

def _fetch_gate_state(
    in_scope: Set[str],
    *,
    timeout: int,
    verbose: bool,
) -> Tuple[
    Dict[str, Tuple[Decimal, Decimal, Decimal]],
    Dict[str, Decimal],
    Dict[str, Decimal],
]:
    api_key = os.environ.get("GATE_API_KEY", "").strip()
    api_secret = os.environ.get("GATE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        raise SystemExit("GATE_API_KEY / GATE_API_SECRET not set")
    base_url = os.environ.get("GATE_API_URL", "https://api.gateio.ws")
    api_prefix = os.environ.get("GATE_API_PREFIX", "/api/v4")

    # ---- Balance: /unified/accounts -> balances map ----
    balances: Dict[str, Tuple[Decimal, Decimal, Decimal]] = {}
    ok, status, body, err, _ = signed_get_gate(
        base_url, api_prefix, "/unified/accounts", {}, api_key, api_secret, timeout,
    )
    if not ok or status >= 300:
        raise SystemExit(f"Gate unified accounts fetch failed: status={status} err={err} body={body[:300]}")
    payload = parse_json_any(body)
    if not isinstance(payload, dict):
        raise SystemExit(f"Gate unified accounts: invalid response: {body[:300]}")
    bal_map = payload.get("balances") if isinstance(payload.get("balances"), dict) else {}
    for raw_asset, detail in bal_map.items():
        if not isinstance(detail, dict):
            continue
        asset = str(raw_asset).strip().upper()
        if asset not in in_scope:
            continue
        # Rust convention: balance = equity (net of liabilities). Gate exposes
        # `equity` directly on each row.
        equity = _dec(detail.get("equity"))
        # Outstanding borrow: prefer total_liab, fall back to borrowed + negative_liab.
        total_liab = _dec(detail.get("total_liab"))
        if total_liab > 0:
            borrowed = total_liab
        else:
            borrowed = _dec(detail.get("borrowed")) + _dec(detail.get("negative_liab"))
        interest = _dec(detail.get("unrealised_pnl"))  # not exact, but Gate doesn't surface accrued interest separately
        balances[asset] = (equity, borrowed, Decimal(0))  # leave interest=0 — not tracked here
    if verbose:
        print(f"[gate] balances: {len(balances)} in-scope assets with details")

    # ---- Futures contracts (for quanto_multiplier) ----
    ok, status, body, err = public_get(
        f"{base_url.rstrip('/')}{api_prefix}/futures/usdt/contracts",
        timeout=timeout,
    )
    if not ok or status >= 300:
        raise SystemExit(f"Gate futures contracts fetch failed: status={status} err={err}")
    contracts_payload = parse_json_any(body) or []
    quanto_by_contract: Dict[str, Decimal] = {}
    if isinstance(contracts_payload, list):
        for entry in contracts_payload:
            if not isinstance(entry, dict):
                continue
            contract = str(entry.get("name") or entry.get("contract") or "").strip().upper()
            if not contract.endswith("_USDT"):
                continue
            quanto = _dec(entry.get("quanto_multiplier"))
            if quanto > 0:
                quanto_by_contract[contract] = quanto

    # ---- Futures positions ----
    ok, status, body, err, _ = signed_get_gate(
        base_url, api_prefix, "/futures/usdt/positions", {}, api_key, api_secret, timeout,
    )
    if not ok or status >= 300:
        raise SystemExit(f"Gate futures positions fetch failed: status={status} err={err} body={body[:300]}")
    pos_payload = parse_json_any(body)
    positions: Dict[str, Decimal] = {}
    if isinstance(pos_payload, list):
        for entry in pos_payload:
            if not isinstance(entry, dict):
                continue
            contract = str(entry.get("contract") or "").strip().upper()
            if not contract.endswith("_USDT"):
                continue
            asset = contract[: -len("_USDT")]
            if asset not in in_scope:
                continue
            quanto = quanto_by_contract.get(contract, Decimal(0))
            if quanto <= 0:
                continue
            size = _dec(entry.get("size"))  # signed: positive=long, negative=short
            positions[asset] = positions.get(asset, Decimal(0)) + size * quanto
    if verbose:
        print(f"[gate] positions: {len(positions)} in-scope assets with non-zero pos")

    # ---- Marks (spot tickers in bulk) ----
    marks: Dict[str, Decimal] = {"USDT": Decimal(1)}
    ok, status, body, err = public_get(
        f"{base_url.rstrip('/')}{api_prefix}/spot/tickers",
        timeout=timeout,
    )
    if ok and status < 300:
        ticker_payload = parse_json_any(body)
        if isinstance(ticker_payload, list):
            for entry in ticker_payload:
                if not isinstance(entry, dict):
                    continue
                pair = str(entry.get("currency_pair") or "").strip().upper()
                if not pair.endswith("_USDT"):
                    continue
                base = pair[: -len("_USDT")]
                if base not in in_scope:
                    continue
                last = _dec(entry.get("last"))
                if last > 0:
                    marks[base] = last
    return balances, positions, marks


def _fetch_bybit_state(
    in_scope: Set[str],
    *,
    timeout: int,
    verbose: bool,
) -> Tuple[
    Dict[str, Tuple[Decimal, Decimal, Decimal]],
    Dict[str, Decimal],
    Dict[str, Decimal],
]:
    api_key = os.environ.get("BYBIT_API_KEY", "").strip()
    api_secret = os.environ.get("BYBIT_API_SECRET", "").strip()
    if not api_key or not api_secret:
        raise SystemExit("BYBIT_API_KEY / BYBIT_API_SECRET not set")
    base_url = os.environ.get("BYBIT_REST_URL", "https://api.bybit.com")
    recv_window = int(os.environ.get("BYBIT_RECV_WINDOW_MS", "5000"))

    # ---- Wallet balance (UNIFIED) ----
    balances: Dict[str, Tuple[Decimal, Decimal, Decimal]] = {}
    ok, status, body, err, _ = signed_get_bybit(
        base_url, "/v5/account/wallet-balance", {"accountType": "UNIFIED"},
        api_key, api_secret, recv_window, timeout,
    )
    if not ok or status >= 300:
        raise SystemExit(f"Bybit wallet-balance fetch failed: status={status} err={err} body={body[:300]}")
    payload = parse_json_any(body)
    if not isinstance(payload, dict) or payload.get("retCode") != 0:
        raise SystemExit(f"Bybit wallet-balance API error: retCode={payload.get('retCode') if isinstance(payload, dict) else 'N/A'} body={body[:300]}")
    result = payload.get("result") or {}
    for account in result.get("list") or []:
        if not isinstance(account, dict):
            continue
        for coin in account.get("coin") or []:
            if not isinstance(coin, dict):
                continue
            asset = str(coin.get("coin") or "").strip().upper()
            if not asset or asset not in in_scope:
                continue
            balance = _dec(coin.get("walletBalance"))
            borrowed = _dec(coin.get("borrowAmount"))
            interest = _dec(coin.get("accruedInterest"))
            balances[asset] = (balance, borrowed, interest)
    if verbose:
        print(f"[bybit] balances: {len(balances)} in-scope assets")

    # ---- Position (linear, USDT) ----
    ok, status, body, err, _ = signed_get_bybit(
        base_url, "/v5/position/list", {"category": "linear", "settleCoin": "USDT"},
        api_key, api_secret, recv_window, timeout,
    )
    if not ok or status >= 300:
        raise SystemExit(f"Bybit position list fetch failed: status={status} err={err} body={body[:300]}")
    pos_payload = parse_json_any(body)
    if not isinstance(pos_payload, dict) or pos_payload.get("retCode") != 0:
        raise SystemExit(f"Bybit position list API error: retCode={pos_payload.get('retCode') if isinstance(pos_payload, dict) else 'N/A'} body={body[:300]}")
    pos_result = pos_payload.get("result") or {}
    positions: Dict[str, Decimal] = {}
    for item in pos_result.get("list") or []:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").strip().upper()
        if not symbol.endswith("USDT"):
            continue
        asset = symbol[: -len("USDT")]
        if asset not in in_scope:
            continue
        size = _dec(item.get("size"))
        side = str(item.get("side") or "").strip()
        # Bybit one-way mode: side is 'Buy' (long) / 'Sell' (short) / '' (no position).
        # Hedge mode would have positionIdx 1/2 with separate rows.
        signed_size = size if side != "Sell" else -size
        positions[asset] = positions.get(asset, Decimal(0)) + signed_size
    if verbose:
        print(f"[bybit] positions: {len(positions)} in-scope assets with non-zero pos")

    # ---- Marks (spot tickers in bulk) ----
    marks: Dict[str, Decimal] = {"USDT": Decimal(1)}
    ok, status, body, err = public_get(
        f"{base_url.rstrip('/')}/v5/market/tickers?category=spot",
        timeout=timeout,
    )
    if ok and status < 300:
        ticker_payload = parse_json_any(body)
        if isinstance(ticker_payload, dict) and ticker_payload.get("retCode") == 0:
            for entry in (ticker_payload.get("result") or {}).get("list") or []:
                if not isinstance(entry, dict):
                    continue
                symbol = str(entry.get("symbol") or "").strip().upper()
                if not symbol.endswith("USDT"):
                    continue
                base = symbol[: -len("USDT")]
                if base not in in_scope:
                    continue
                last = _dec(entry.get("lastPrice"))
                if last > 0:
                    marks[base] = last
    return balances, positions, marks


def _fetch_bitget_state(
    in_scope: Set[str],
    *,
    timeout: int,
    verbose: bool,
) -> Tuple[
    Dict[str, Tuple[Decimal, Decimal, Decimal]],
    Dict[str, Decimal],
    Dict[str, Decimal],
]:
    api_key = os.environ.get("BITGET_API_KEY", "").strip()
    api_secret = os.environ.get("BITGET_API_SECRET", "").strip()
    passphrase = (
        os.environ.get("BITGET_API_PASSPHRASE", "").strip()
        or os.environ.get("BITGET_PASSPHRASE", "").strip()
    )
    if not api_key or not api_secret or not passphrase:
        raise SystemExit("BITGET_API_KEY / BITGET_API_SECRET / BITGET_API_PASSPHRASE not set")
    base_url = os.environ.get("BITGET_REST_URL", "https://api.bitget.com")

    # ---- Account assets (UTA) ----
    balances: Dict[str, Tuple[Decimal, Decimal, Decimal]] = {}
    ok, status, body, err, _ = signed_get_bitget(
        base_url, "/api/v3/account/assets", {}, api_key, api_secret, passphrase, timeout,
    )
    if not ok or status >= 300:
        raise SystemExit(f"Bitget account assets fetch failed: status={status} err={err} body={body[:300]}")
    payload = parse_json_any(body)
    if not isinstance(payload, dict):
        raise SystemExit(f"Bitget account assets: invalid response: {body[:300]}")
    code = str(payload.get("code", ""))
    if code not in {"00000", "0"}:
        raise SystemExit(f"Bitget account assets API error: code={code} msg={payload.get('msg')}")
    data = payload.get("data")
    if isinstance(data, dict):
        assets = data.get("assets")
    elif isinstance(data, list):
        assets = data
    else:
        assets = []
    for entry in assets or []:
        if not isinstance(entry, dict):
            continue
        coin = str(entry.get("coin") or "").strip().upper()
        if not coin or coin not in in_scope:
            continue
        # Rust convention: balance = equity (net), fall back to balance.
        balance = _dec(entry.get("equity"))
        if balance == 0:
            balance = _dec(entry.get("balance"))
        # borrow / debt / debts: prefer borrow (principal), debts (gross),
        # interest = max(0, debts - borrow).
        borrowed = _dec(entry.get("borrow"))
        if borrowed == 0:
            borrowed = _dec(entry.get("debt"))
        if borrowed == 0:
            borrowed = _dec(entry.get("debts"))
        debt_total = _dec(entry.get("debts"))
        if debt_total == 0:
            debt_total = _dec(entry.get("debt"))
        if debt_total == 0:
            debt_total = borrowed
        interest = debt_total - borrowed if debt_total > borrowed else Decimal(0)
        balances[coin] = (balance, borrowed, interest)
    if verbose:
        print(f"[bitget] balances: {len(balances)} in-scope assets")

    # ---- Positions (USDT-FUTURES) ----
    ok, status, body, err, _ = signed_get_bitget(
        base_url, "/api/v3/position/current-position", {"productType": "USDT-FUTURES"},
        api_key, api_secret, passphrase, timeout,
    )
    if not ok or status >= 300:
        raise SystemExit(f"Bitget positions fetch failed: status={status} err={err} body={body[:300]}")
    pos_payload = parse_json_any(body)
    if not isinstance(pos_payload, dict):
        raise SystemExit(f"Bitget positions: invalid response: {body[:300]}")
    code = str(pos_payload.get("code", ""))
    if code not in {"00000", "0"}:
        raise SystemExit(f"Bitget positions API error: code={code} msg={pos_payload.get('msg')}")
    pos_data = pos_payload.get("data")
    rows = []
    if isinstance(pos_data, dict):
        rows = pos_data.get("list") or []
    elif isinstance(pos_data, list):
        rows = pos_data

    positions: Dict[str, Decimal] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or row.get("instId") or "").strip().upper()
        if not symbol.endswith("USDT"):
            continue
        asset = symbol[: -len("USDT")]
        if asset not in in_scope:
            continue
        size = _dec(row.get("total") or row.get("size") or row.get("pos"))
        side = str(row.get("holdSide") or row.get("posSide") or row.get("side") or "").strip().lower()
        signed_size = -size if side == "short" else size
        positions[asset] = positions.get(asset, Decimal(0)) + signed_size
    if verbose:
        print(f"[bitget] positions: {len(positions)} in-scope assets with non-zero pos")

    # ---- Marks (spot tickers in bulk) ----
    marks: Dict[str, Decimal] = {"USDT": Decimal(1)}
    ok, status, body, err = public_get(
        f"{base_url.rstrip('/')}/api/v2/spot/market/tickers",
        timeout=timeout,
    )
    if ok and status < 300:
        ticker_payload = parse_json_any(body)
        if isinstance(ticker_payload, dict) and str(ticker_payload.get("code", "")) in {"00000", "0"}:
            for entry in ticker_payload.get("data") or []:
                if not isinstance(entry, dict):
                    continue
                symbol = str(entry.get("symbol") or "").strip().upper()
                if not symbol.endswith("USDT"):
                    continue
                base = symbol[: -len("USDT")]
                if base not in in_scope:
                    continue
                last = _dec(entry.get("lastPr") or entry.get("close") or entry.get("last"))
                if last > 0:
                    marks[base] = last
    return balances, positions, marks


def _fetch_binance_state(in_scope, *, suffix, timeout, verbose):
    # Binance is intentionally out of scope for this migration. Callers that
    # hit a Binance env should fall back to --source dashboard.
    raise NotImplementedError(
        "binance venue is not migrated to direct exchange queries; "
        "rerun with --source dashboard for Binance intra envs"
    )
