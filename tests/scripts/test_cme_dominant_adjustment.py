from datetime import date
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from cme_legacy.cme_dominant_adjustment import (
    ContractSnapshot,
    SettlementEvent,
    compute_adjustments,
    select_dominants,
)


def snapshot(contract_id: str, ric: str, volume: float) -> ContractSnapshot:
    return ContractSnapshot(contract_id=contract_id, ric=ric, volume=volume)


def test_later_delivery_requires_two_completed_days_then_rolls_next_day() -> None:
    days = [date(2024, 1, day) for day in range(2, 6)]
    old = "CME:ES:2024-03"
    new = "CME:ES:2024-06"
    records = select_dominants(
        calendar=days,
        snapshots_by_day={
            days[0]: [snapshot(old, "ESH24", 100), snapshot(new, "ESM24", 50)],
            days[1]: [snapshot(old, "ESH24", 100), snapshot(new, "ESM24", 120)],
            days[2]: [snapshot(old, "ESH24", 90), snapshot(new, "ESM24", 130)],
            days[3]: [snapshot(old, "ESH24", 500), snapshot(new, "ESM24", 20)],
        },
        exchange="CME",
        product="ES",
    )

    assert [record.contract_id for record in records] == [None, old, old, new]
    assert records[2].scheduled_next_contract_id == new
    assert records[2].confirmation_count == 2
    assert records[3].roll_flag
    assert records[3].scheduled_next_contract_id == new


def test_pre_window_day_seeds_first_delivery_day_without_lookahead() -> None:
    warmup_day = date(2019, 12, 31)
    delivery_day = date(2020, 1, 2)
    old = "CME:ES:2020-03"
    later = "CME:ES:2020-06"
    records = select_dominants(
        calendar=[warmup_day, delivery_day],
        snapshots_by_day={
            warmup_day: [snapshot(old, "ESH0", 100), snapshot(later, "ESM0", 50)],
            delivery_day: [snapshot(old, "ESH0", 10), snapshot(later, "ESM0", 1_000)],
        },
        exchange="CME",
        product="ES",
    )

    delivery = records[1]
    assert delivery.contract_id == old
    assert delivery.ric == "ESH0"
    assert delivery.scheduled_next_contract_id == old
    assert delivery.challenger_contract_id == later
    assert delivery.confirmation_count == 1


def test_nonpositive_volume_cannot_seed_or_challenge() -> None:
    day = date(2024, 1, 2)
    records = select_dominants(
        calendar=[day],
        snapshots_by_day={
            day: [
                snapshot("CME:NQ:2024-03", "NQH24", 0),
                snapshot("CME:NQ:2024-06", "NQM24", -5),
            ]
        },
        exchange="CME",
        product="NQ",
    )
    assert records[0].contract_id is None
    assert records[0].scheduled_next_contract_id is None


def test_adjustment_uses_latest_causal_t1_settlement() -> None:
    days = [date(2024, 1, day) for day in range(2, 6)]
    old = "CME:ES:2024-03"
    new = "CME:ES:2024-06"
    dominants = select_dominants(
        calendar=days,
        snapshots_by_day={
            days[0]: [snapshot(old, "ESH24", 100), snapshot(new, "ESM24", 50)],
            days[1]: [snapshot(old, "ESH24", 100), snapshot(new, "ESM24", 120)],
            days[2]: [snapshot(old, "ESH24", 90), snapshot(new, "ESM24", 130)],
            days[3]: [snapshot(new, "ESM24", 140)],
        },
        exchange="CME",
        product="ES",
    )
    factors = compute_adjustments(
        dominants=dominants,
        settlements={
            ("ESH24", days[2]): [
                SettlementEvent(price=100.0, published_ts_ns=10, part=0, seq=1),
                SettlementEvent(price=101.0, published_ts_ns=20, part=0, seq=2),
                SettlementEvent(price=999.0, published_ts_ns=101, part=0, seq=3),
            ],
            ("ESM24", days[2]): [
                SettlementEvent(price=105.0, published_ts_ns=30, part=0, seq=1)
            ],
        },
        session_open_ns=lambda _: 100,
    )

    assert len(factors) == 1
    factor = factors[0]
    assert factor.reference_trading_day == days[2]
    assert factor.old_reference_price == 101.0
    assert factor.new_reference_price == 105.0
    assert factor.old_update_count == 2
    assert factor.old_had_price_conflict
    assert factor.adjustment_value == 4.0
    assert factor.cumulative_factor == 4.0
    assert factor.cumulative_complete


def test_missing_settlement_keeps_the_roll_auditable_but_skipped() -> None:
    days = [date(2024, 1, day) for day in range(2, 6)]
    old = "CME:ES:2024-03"
    new = "CME:ES:2024-06"
    dominants = select_dominants(
        calendar=days,
        snapshots_by_day={
            days[0]: [snapshot(old, "ESH24", 100), snapshot(new, "ESM24", 50)],
            days[1]: [snapshot(old, "ESH24", 100), snapshot(new, "ESM24", 120)],
            days[2]: [snapshot(old, "ESH24", 90), snapshot(new, "ESM24", 130)],
            days[3]: [snapshot(new, "ESM24", 140)],
        },
        exchange="CME",
        product="ES",
    )
    factors = compute_adjustments(
        dominants=dominants,
        settlements={("ESH24", days[2]): [SettlementEvent(100.0, 10, 0, 1)]},
        session_open_ns=lambda _: 100,
    )

    assert len(factors) == 1
    assert factors[0].skipped
    assert factors[0].skip_reason == "missing_t1_new_settlement"
    assert factors[0].cumulative_factor is None


def test_missing_settlement_uses_both_t1_closes_without_mixing_sources() -> None:
    days = [date(2024, 1, day) for day in range(2, 6)]
    old = "CME:ES:2024-03"
    new = "CME:ES:2024-06"
    dominants = select_dominants(
        calendar=days,
        snapshots_by_day={
            days[0]: [snapshot(old, "ESH24", 100), snapshot(new, "ESM24", 50)],
            days[1]: [snapshot(old, "ESH24", 100), snapshot(new, "ESM24", 120)],
            days[2]: [snapshot(old, "ESH24", 90), snapshot(new, "ESM24", 130)],
            days[3]: [snapshot(new, "ESM24", 140)],
        },
        exchange="CME",
        product="ES",
    )
    factors = compute_adjustments(
        dominants=dominants,
        settlements={
            ("ESH24", days[2]): [SettlementEvent(price=999.0, published_ts_ns=10, part=0, seq=1)]
        },
        close_prices={("ESH24", days[2]): 101.0, ("ESM24", days[2]): 105.0},
        session_open_ns=lambda _: 100,
    )

    assert len(factors) == 1
    factor = factors[0]
    assert factor.method == "t1_close_spread"
    assert factor.old_reference_price == 101.0
    assert factor.new_reference_price == 105.0
    assert factor.adjustment_value == 4.0
    assert factor.cumulative_factor == 4.0
    assert factor.cumulative_complete
    assert not factor.skipped
    assert factor.old_published_ts_ns is None
    assert factor.new_published_ts_ns is None
