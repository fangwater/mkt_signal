"""Narrow CME volume-roll baseline for ES, NQ, RTY, YM, GC, and CL.

The module is deliberately independent of parquet, RocksDB, and PostgreSQL so
the roll decision and settlement-only adjustment can be tested directly.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
import math
from zoneinfo import ZoneInfo


DEFAULT_CONFIRMATION_DAYS = 2
RULE = "volume_two_day_later_month"
ADJUSTMENT_METHOD = "t1_settlement_spread"
CLOSE_FALLBACK_METHOD = "t1_close_spread"


@dataclass(frozen=True)
class ContractSnapshot:
    contract_id: str
    ric: str
    volume: float


@dataclass(frozen=True)
class DominantRecord:
    trading_day: date
    exchange: str
    product: str
    contract_id: str | None
    ric: str | None
    roll_flag: bool
    decision_reference_day: date
    scheduled_next_contract_id: str | None
    scheduled_next_ric: str | None
    dominant_volume: float
    root_volume: float
    challenger_contract_id: str | None
    challenger_volume: float | None
    confirmation_count: int
    rule: str = RULE


@dataclass(frozen=True)
class SettlementEvent:
    price: float
    published_ts_ns: int
    part: int
    seq: int


@dataclass(frozen=True)
class AdjustmentRecord:
    effective_trading_day: date
    reference_trading_day: date
    from_contract_id: str
    from_ric: str
    to_contract_id: str
    to_ric: str
    effective_session_open_ts_ns: int
    old_reference_price: float | None
    new_reference_price: float | None
    old_published_ts_ns: int | None
    new_published_ts_ns: int | None
    old_update_count: int | None
    new_update_count: int | None
    old_had_price_conflict: bool | None
    new_had_price_conflict: bool | None
    adjustment_value: float | None
    cumulative_factor: float | None
    cumulative_complete: bool
    skipped: bool
    skip_reason: str | None
    method: str = ADJUSTMENT_METHOD


@dataclass(frozen=True)
class _SettlementChoice:
    price: float
    published_ts_ns: int
    update_count: int
    had_price_conflict: bool


def delivery_month(contract_id: str) -> tuple[int, int]:
    """Parse the fixed-expiry ``EXCHANGE:ROOT:YYYY-MM`` contract suffix."""

    try:
        _, _, expiry = contract_id.split(":", 2)
        year_text, month_text = expiry.split("-", 1)
        year = int(year_text)
        month = int(month_text)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid fixed-expiry contract_id: {contract_id!r}") from exc
    if not 1 <= month <= 12:
        raise ValueError(f"invalid delivery month in {contract_id!r}")
    return year, month


def effective_daily_volume(value: float) -> float:
    """Non-positive and non-finite minute aggregates cannot win a roll."""

    return value if math.isfinite(value) and value > 0.0 else 0.0


def _ordered_calendar(calendar: Sequence[date]) -> list[date]:
    ordered = list(calendar)
    if ordered != sorted(set(ordered)):
        raise ValueError("trading calendar must be strictly increasing and unique")
    return ordered


def _snapshots_by_contract(rows: Sequence[ContractSnapshot]) -> dict[str, ContractSnapshot]:
    by_contract: dict[str, ContractSnapshot] = {}
    for row in rows:
        if row.contract_id in by_contract:
            raise ValueError(f"duplicate contract snapshot: {row.contract_id}")
        delivery_month(row.contract_id)
        by_contract[row.contract_id] = ContractSnapshot(
            contract_id=row.contract_id,
            ric=row.ric,
            volume=effective_daily_volume(row.volume),
        )
    return by_contract


def _winner(rows: Sequence[ContractSnapshot]) -> ContractSnapshot | None:
    eligible = [row for row in rows if row.volume > 0.0]
    if not eligible:
        return None
    return min(eligible, key=lambda row: (-row.volume, delivery_month(row.contract_id), row.ric))


def select_dominants(
    *,
    calendar: Sequence[date],
    snapshots_by_day: Mapping[date, Sequence[ContractSnapshot]],
    exchange: str,
    product: str,
    confirmation_days: int = DEFAULT_CONFIRMATION_DAYS,
) -> list[DominantRecord]:
    """Select a next-day dominant using only completed daily trade volume.

    The first assignment uses the previous day winner. Afterwards, a later
    delivery must beat the current contract for ``confirmation_days`` adjacent
    days before it becomes effective on the following trading day.
    """

    if confirmation_days < 1:
        raise ValueError("confirmation_days must be positive")
    days = _ordered_calendar(calendar)
    if not days:
        return []

    current_contract_id: str | None = None
    scheduled_contract_id: str | None = None
    candidate_contract_id: str | None = None
    candidate_count = 0
    known: dict[str, ContractSnapshot] = {}
    records: list[DominantRecord] = []

    for trading_day in days:
        rows = _snapshots_by_contract(snapshots_by_day.get(trading_day, ()))
        known.update(rows)
        previous_contract_id = current_contract_id
        current_contract_id = scheduled_contract_id
        current = (
            rows.get(current_contract_id) or known.get(current_contract_id)
            if current_contract_id is not None
            else None
        )
        root_volume = sum(row.volume for row in rows.values())
        current_volume = rows.get(current_contract_id).volume if current_contract_id in rows else 0.0

        challenger: ContractSnapshot | None = None
        scheduled_next = current_contract_id
        if current is None:
            seed = _winner(list(rows.values()))
            scheduled_next = seed.contract_id if seed is not None else None
            candidate_contract_id = None
            candidate_count = 0
        else:
            current_delivery = delivery_month(current.contract_id)
            later_rows = [
                row
                for row in rows.values()
                if delivery_month(row.contract_id) > current_delivery and row.volume > current_volume
            ]
            challenger = _winner(later_rows)
            if challenger is None:
                candidate_contract_id = None
                candidate_count = 0
            elif challenger.contract_id == candidate_contract_id:
                candidate_count += 1
            else:
                candidate_contract_id = challenger.contract_id
                candidate_count = 1
            if challenger is not None and candidate_count >= confirmation_days:
                scheduled_next = challenger.contract_id

        scheduled = known.get(scheduled_next) if scheduled_next is not None else None
        records.append(
            DominantRecord(
                trading_day=trading_day,
                exchange=exchange,
                product=product,
                contract_id=current.contract_id if current is not None else None,
                ric=current.ric if current is not None else None,
                roll_flag=(
                    previous_contract_id is not None
                    and current_contract_id is not None
                    and current_contract_id != previous_contract_id
                ),
                decision_reference_day=trading_day,
                scheduled_next_contract_id=scheduled_next,
                scheduled_next_ric=scheduled.ric if scheduled is not None else None,
                dominant_volume=current_volume,
                root_volume=root_volume,
                challenger_contract_id=challenger.contract_id if challenger is not None else None,
                challenger_volume=challenger.volume if challenger is not None else None,
                confirmation_count=candidate_count,
            )
        )
        scheduled_contract_id = scheduled_next

    return records


def chicago_preopen_ns(trading_day: date) -> int:
    """Standard CME Globex pre-open for a trading day, in UTC nanoseconds."""

    local = datetime.combine(
        trading_day - timedelta(days=1),
        time(17, 0),
        tzinfo=ZoneInfo("America/Chicago"),
    )
    return int(local.astimezone(timezone.utc).timestamp() * 1_000_000_000)


def _settlement_choice(
    events: Sequence[SettlementEvent],
    *,
    cutoff_ns: int,
) -> _SettlementChoice | None:
    eligible = [
        event
        for event in events
        if math.isfinite(event.price) and event.price > 0.0 and event.published_ts_ns < cutoff_ns
    ]
    if not eligible:
        return None
    chosen = max(eligible, key=lambda event: (event.published_ts_ns, event.part, event.seq))
    return _SettlementChoice(
        price=chosen.price,
        published_ts_ns=chosen.published_ts_ns,
        update_count=len(eligible),
        had_price_conflict=len({event.price for event in eligible}) > 1,
    )


def compute_adjustments(
    *,
    dominants: Sequence[DominantRecord],
    settlements: Mapping[tuple[str, date], Sequence[SettlementEvent]],
    close_prices: Mapping[tuple[str, date], float] | None = None,
    session_open_ns: Callable[[date], int] = chicago_preopen_ns,
) -> list[AdjustmentRecord]:
    """Build T-1 spreads, falling back from Settlement to same-day closes."""

    if not dominants:
        return []
    ordered = sorted(dominants, key=lambda record: record.trading_day)
    if [record.trading_day for record in ordered] != _ordered_calendar(
        [record.trading_day for record in ordered]
    ):
        raise ValueError("dominant rows must have unique, increasing trading days")

    factors: list[AdjustmentRecord] = []
    close_prices = close_prices or {}
    for index, current in enumerate(ordered[1:], start=1):
        previous = ordered[index - 1]
        if (
            previous.contract_id is None
            or previous.ric is None
            or current.contract_id is None
            or current.ric is None
            or previous.contract_id == current.contract_id
        ):
            continue
        reference_day = previous.trading_day
        cutoff_ns = session_open_ns(current.trading_day)
        old = _settlement_choice(
            settlements.get((previous.ric, reference_day), ()), cutoff_ns=cutoff_ns
        )
        new = _settlement_choice(
            settlements.get((current.ric, reference_day), ()), cutoff_ns=cutoff_ns
        )
        method = ADJUSTMENT_METHOD
        old_price = old.price if old is not None else None
        new_price = new.price if new is not None else None
        if old is None or new is None:
            old_close = close_prices.get((previous.ric, reference_day))
            new_close = close_prices.get((current.ric, reference_day))
            if (
                old_close is not None
                and new_close is not None
                and math.isfinite(old_close)
                and math.isfinite(new_close)
                and old_close > 0.0
                and new_close > 0.0
            ):
                method = CLOSE_FALLBACK_METHOD
                old_price = old_close
                new_price = new_close
        missing = []
        if old_price is None:
            missing.append("old")
        if new_price is None:
            missing.append("new")
        used_settlement = method == ADJUSTMENT_METHOD
        factors.append(
            AdjustmentRecord(
                effective_trading_day=current.trading_day,
                reference_trading_day=reference_day,
                from_contract_id=previous.contract_id,
                from_ric=previous.ric,
                to_contract_id=current.contract_id,
                to_ric=current.ric,
                effective_session_open_ts_ns=cutoff_ns,
                old_reference_price=old_price,
                new_reference_price=new_price,
                old_published_ts_ns=(
                    old.published_ts_ns if used_settlement and old is not None else None
                ),
                new_published_ts_ns=(
                    new.published_ts_ns if used_settlement and new is not None else None
                ),
                old_update_count=old.update_count if used_settlement and old is not None else None,
                new_update_count=new.update_count if used_settlement and new is not None else None,
                old_had_price_conflict=(
                    old.had_price_conflict if used_settlement and old is not None else None
                ),
                new_had_price_conflict=(
                    new.had_price_conflict if used_settlement and new is not None else None
                ),
                adjustment_value=(new_price - old_price)
                if old_price is not None and new_price is not None
                else None,
                cumulative_factor=None,
                cumulative_complete=False,
                skipped=bool(missing),
                skip_reason=("missing_t1_" + "_and_".join(missing) + "_settlement") if missing else None,
                method=method,
            )
        )

    running = 0.0
    complete = True
    resolved: list[AdjustmentRecord] = []
    for factor in reversed(factors):
        if factor.skipped:
            complete = False
            resolved.append(factor)
            continue
        if complete:
            assert factor.adjustment_value is not None
            running += factor.adjustment_value
            resolved.append(
                replace(factor, cumulative_factor=running, cumulative_complete=True)
            )
        else:
            resolved.append(factor)
    return list(reversed(resolved))
