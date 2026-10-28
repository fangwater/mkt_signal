import copy
import importlib.util
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("cme_causal_roll", ROOT / "scripts/cme_causal_roll.py")
roll = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = roll
spec.loader.exec_module(roll)
FIXTURE = ROOT / "crates/cme_tas_replay/tests/fixtures/causal_roll.json"


def sample():
    return json.loads(FIXTURE.read_text())


def future_calendar_sample():
    return json.loads(FIXTURE.with_name('causal_roll_future_calendar.json').read_text())


def test_known_risk_date_can_precede_precise_exit_calendar():
    data = future_calendar_sample()
    rows = roll.replay(data)
    assert [r['reason'] for r in rows] == ['seed', 'hold', 'deadline', 'hold', 'hold']
    assert [r['selected_ric'] for r in rows] == ['TYH26', 'TYH26', 'TYM26', 'TYM26', 'TYM26']
    assert rows[0]['selected_roll_by_ns'] is None
    assert rows[0]['selected_risk_deadline_ns'] == data['contracts'][0]['risk_deadline_ns']
    assert rows[1]['current_roll_by_ns'] is None
    assert rows[2]['current_roll_by_ns'] == rows[2]['effective_ns']
    assert rows[2]['offset_e9'] == 62_500_000
    assert rows[2]['selected_roll_by_ns'] is None
    assert all(r['deadline_buffer_session']['known_at_ns'] < r['decision_ns'] for r in rows)
    assert rows[0]['candidate_comparisons'][0]['deadline_basis'] == 'known_future_session'


def test_precise_exit_revision_and_future_calendar_preserve_every_prefix():
    data = future_calendar_sample()
    expected = roll.replay(data)
    for end, packet in enumerate(data['decisions'], 1):
        prefix = copy.deepcopy(data)
        prefix['decisions'] = prefix['decisions'][:end]
        for name, stamp in [('contracts', 'known_at_ns'), ('volumes', 'available_at_ns'), ('settlements', 'published_ns')]:
            prefix[name] = [r for r in prefix[name] if r[stamp] < packet['decision_ns']]
        assert roll.replay(prefix) == expected[:end]
    data['contracts'].append(dict(data['contracts'][0], known_at_ns=data['decisions'][-1]['decision_ns']+1,
                                  roll_by_ns=data['decisions'][0]['effective_ns'], verified=False))
    assert roll.replay(data) == expected


@pytest.mark.parametrize('missing', ['session', 'at_cutoff', 'after_cutoff', 'risk_date', 'risk_instant'])
def test_missing_or_future_buffer_evidence_cannot_keep_current(missing):
    data = future_calendar_sample()
    p = data['decisions'][1]
    if missing == 'session':
        del p['deadline_buffer_session']
    elif missing in ('at_cutoff', 'after_cutoff'):
        p['deadline_buffer_session']['known_at_ns'] = p['decision_ns'] + int(missing == 'after_cutoff')
    elif missing == 'risk_instant':
        data['contracts'].append(dict(data['contracts'][0], known_at_ns=p['decision_ns']-1,
                                      risk_deadline_ns=None))
    else:
        data['contracts'].append(dict(data['contracts'][0], known_at_ns=p['decision_ns']-1,
                                      risk_date=None))
    r = roll.replay(data)[1]
    assert r['detail'] == 'current_deadline_unknown_or_invalid'
    assert r['exit_ric'] == 'TYH26' and r['selected_ric'] is None


def test_future_buffer_cannot_override_known_forced_exit():
    data = future_calendar_sample()
    data['contracts'][-1]['risk_deadline_ns'] = data['contracts'][1]['risk_deadline_ns']
    r = roll.replay(data)[2]
    assert r['reason'] == 'deadline' and r['exit_ric'] == 'TYH26'


@pytest.mark.parametrize('delta', [0, -1])
def test_entire_buffer_day_must_close_before_risk_date(delta):
    data = future_calendar_sample()
    p = data['decisions'][0]
    # A future multi-session official day only counts when its LAST segment ends.
    session = p['deadline_buffer_session']
    lo, hi = session['segments'][0]
    session['segments'] = [[lo, lo+60_000_000_000], [lo+120_000_000_000, hi]]
    data['contracts'][0]['risk_deadline_ns'] = hi + delta
    r = roll.replay(data)[0]
    assert r['selected_ric'] == 'TYM26'
    assert all(c['ric'] != 'TYH26' for c in r['candidate_comparisons'])


def test_risk_day_itself_is_not_the_prior_day_buffer_even_before_intent_deadline():
    data = future_calendar_sample()
    session = data['decisions'][0]['deadline_buffer_session']
    data['contracts'][0]['risk_date'] = session['day']
    data['contracts'][0]['risk_deadline_ns'] = session['segments'][-1][1] + 2*3600*10**9
    r = roll.replay(data)[0]
    assert r['selected_ric'] == 'TYM26'
    assert all(c['ric'] != 'TYH26' for c in r['candidate_comparisons'])


@pytest.mark.parametrize('mutation', ['same_day', 'current_open', 'empty', 'overlap', 'unaligned', 'blank_source'])
def test_invalid_buffer_calendar_rejected(mutation):
    data = future_calendar_sample()
    p = data['decisions'][0]
    session = p['deadline_buffer_session']
    if mutation == 'same_day':
        session['day'] = p['trading_day']
    elif mutation == 'current_open':
        session['segments'][0][0] = p['effective_ns']
    elif mutation == 'empty':
        session['segments'] = []
    elif mutation == 'overlap':
        session['segments'].append(session['segments'][0][:])
    elif mutation == 'unaligned':
        session['segments'][0][1] += 1
    else:
        session['source'] = ' '
    with pytest.raises(ValueError):
        roll.replay(data)


def test_fixed_deadline_case_and_integer_bridge():
    result = roll.replay(sample())
    assert result == json.loads(FIXTURE.with_suffix(".expected.json").read_text())
    assert [r["selected_ric"] for r in result] == ["TYH26", "TYH26", "TYM26", "TYM26", "TYM26"]
    r = result[2]
    assert r["reason"] == "deadline"
    assert r["offset_e9"] == 62_500_000
    assert r["new_price_e9"] + r["offset_e9"] == r["old_price_e9"]


def test_volume_confirmation_is_next_day_and_only_forward():
    data = sample()
    data["contracts"][0]["roll_by_ns"] = data["contracts"][1]["roll_by_ns"]
    data["contracts"][0]["risk_deadline_ns"] = data["contracts"][1]["risk_deadline_ns"]
    rows = roll.replay(data)
    assert [r["reason"] for r in rows] == ["seed", "hold", "hold", "hold", "volume"]
    assert rows[-1]["previous_day"] == "2026-02-26"
    assert rows[-1]["confirmation_count"] == 2


@pytest.mark.parametrize("mutation", ["missing_current", "null_current", "incomplete", "late", "summary"])
def test_bad_volume_never_becomes_zero_or_confirms(mutation):
    data = sample()
    b = data["volumes"][1]
    if mutation == "missing_current":
        b["rows"] = b["rows"][1:]
    elif mutation == "null_current":
        b["rows"][0]["volume"] = None
    elif mutation == "incomplete":
        b["complete"] = False
    elif mutation == "late":
        b["available_at_ns"] = data["decisions"][1]["decision_ns"]
    else:
        b["source"] = "summary"
    rows = roll.replay(data)
    assert rows[1]["reason"] == "blocked"
    assert rows[1]["selected_ric"] is None
    assert rows[2]["reason"] == "deadline"
    assert rows[2]["adjustment_status"] == "anchor"


def test_known_zero_can_lose_but_missing_cannot():
    data = sample()
    data["volumes"][0]["rows"][0]["volume"] = 0
    assert roll.replay(data)[0]["selected_ric"] == "TYM26"


@pytest.mark.parametrize("mutation", ["missing_volume", "zero_successor", "unsafe_successor"])
def test_forced_exit_never_waits_for_missing_new_leg(mutation):
    data = sample()
    if mutation == "missing_volume":
        data["volumes"][2]["complete"] = False
    elif mutation == "zero_successor":
        data["volumes"][2]["rows"][1]["volume"] = 0
    else:
        data["contracts"][1]["roll_by_ns"] = data["contracts"][0]["roll_by_ns"]
    r = roll.replay(data)[2]
    assert r["reason"] == "exit_only" and r["exit_ric"] == "TYH26"
    assert r["selected_ric"] is None


def test_missing_expected_settlement_splits_but_does_not_delay_deadline():
    data = sample()
    data["settlements"] = [r for r in data["settlements"] if not (r["ric"] == "TYM26" and r["reference_day"] == "2026-02-24")]
    r = roll.replay(data)[2]
    assert r["reason"] == "deadline" and r["selected_ric"] == "TYM26"
    assert r["adjustment_status"] == "missing_bridge_new_segment" and r["segment_id"] == 2


def test_retracted_settlement_does_not_resurrect_old_price():
    data = sample()
    old = next(r for r in data["settlements"] if r["reference_day"] == "2026-02-24" and r["ric"] == "TYM26")
    data["settlements"].append(dict(old, price_e9=None, published_ns=old["published_ns"] + 1))
    assert roll.replay(data)[2]["adjustment_status"] == "missing_bridge_new_segment"


def test_holiday_uses_common_prior_settlement_with_age_limit():
    data = sample()
    data["decisions"][2]["reference_days"][0]["settlement_expected"] = False
    result = roll.replay(data)[2]
    assert result["reference_day"] == "2026-02-23" and result["reference_age"] == 2
    data["config"]["max_reference_age"] = 1
    assert roll.replay(data)[2]["adjustment_status"] == "missing_bridge_new_segment"


def test_different_valuation_basis_cannot_bridge():
    data = sample()
    for r in data["settlements"]:
        if r["ric"] == "TYM26":
            r["basis"] = "final_settlement"
    assert roll.replay(data)[2]["adjustment_status"] == "missing_bridge_new_segment"


def test_every_prefix_matches_complete_replay():
    data = sample()
    expected = roll.replay(data)
    for end, packet in enumerate(data["decisions"], 1):
        cutoff = packet["decision_ns"]
        prefix = copy.deepcopy(data)
        prefix["decisions"] = prefix["decisions"][:end]
        for name, field in [("contracts", "known_at_ns"), ("volumes", "available_at_ns"), ("settlements", "published_ns")]:
            prefix[name] = [r for r in prefix[name] if r[field] is not None and r[field] < cutoff]
        assert roll.replay(prefix) == expected[:end]


def test_future_volume_price_listing_and_deadline_cannot_rewrite_past():
    data = sample()
    expected = roll.replay(data)[:3]
    cutoff = data["decisions"][2]["decision_ns"]
    data["contracts"].append(dict(data["contracts"][0], known_at_ns=cutoff + 1, verified=False))
    data["contracts"].append(dict(data["contracts"][1], ric="TYU26", delivery_month="2026-09", known_at_ns=cutoff + 1))
    for b in data["volumes"]:
        if b["available_at_ns"] > cutoff:
            b["rows"][0]["volume"] = 999999999
    data["volumes"].append(dict(data["volumes"][2], available_at_ns=cutoff + 1, complete=False))
    old = next(r for r in data["settlements"] if r["reference_day"] == "2026-02-24")
    data["settlements"].append(dict(old, published_ns=cutoff + 1, price_e9=9999999999999))
    assert roll.replay(data)[:3] == expected


def test_metadata_invalidation_does_not_resurrect_old_version():
    data = sample()
    data["contracts"].append(dict(data["contracts"][0], known_at_ns=data["decisions"][1]["decision_ns"] - 1, verified=False))
    r = roll.replay(data)[1]
    assert r["reason"] == "blocked" and r["exit_ric"] == "TYH26"


def test_announced_future_effective_revision_does_not_invalidate_today():
    data = sample()
    expected = roll.replay(data)
    data["contracts"].append(dict(data["contracts"][0], known_at_ns=data["decisions"][0]["decision_ns"] - 1,
                                  valid_from_ns=data["decisions"][-1]["effective_ns"] + 1, verified=False))
    assert roll.replay(data) == expected


def test_unknown_calendar_and_unknown_availability_block():
    data = sample()
    data["decisions"][0]["calendar_known_at_ns"] = None
    assert roll.replay(data)[0]["detail"] == "calendar_not_known"
    data = sample()
    data["volumes"][0]["available_at_ns"] = None
    assert roll.replay(data)[0]["reason"] == "blocked"


def test_nonadjacent_decisions_reset_confirmation():
    data = sample()
    data["contracts"][0].update(roll_by_ns=data["contracts"][1]["roll_by_ns"], risk_deadline_ns=data["contracts"][1]["risk_deadline_ns"])
    data["volumes"][2]["rows"][1]["volume"] = 9999999
    del data["decisions"][3]
    r = roll.replay(data)[-1]
    assert r["reason"] == "hold" and r["confirmation_count"] == 1


def test_post_gap_seed_never_moves_backwards():
    data = sample()
    data["contracts"].append(dict(data["contracts"][1], known_at_ns=data["decisions"][3]["decision_ns"] - 1, verified=False))
    # Even if the old March contract were safe again, the month floor is June.
    data["contracts"].append(dict(data["contracts"][0], known_at_ns=data["decisions"][3]["decision_ns"] - 1,
                                  roll_by_ns=data["contracts"][1]["roll_by_ns"], risk_deadline_ns=data["contracts"][1]["risk_deadline_ns"]))
    rows = roll.replay(data)
    assert rows[3]["selected_ric"] is None and rows[4]["selected_ric"] is None


@pytest.mark.parametrize("value", [-1, 1.5, float("nan"), True])
def test_invalid_quantity_rejected(value):
    data = sample()
    data["volumes"][0]["rows"][0]["volume"] = value
    with pytest.raises(ValueError):
        roll.replay(data)


def test_duplicate_and_unsorted_input_rejected():
    for key in ("contracts", "volumes", "settlements", "decisions"):
        data = sample()
        data[key].append(copy.deepcopy(data[key][0]))
        with pytest.raises(ValueError):
            roll.replay(data)
    data = sample()
    data["decisions"].reverse()
    with pytest.raises(ValueError):
        roll.replay(data)


def test_year_zero_is_not_a_valid_trading_date():
    data = sample()
    data['volumes'][0]['trading_day'] = '0000-01-01'
    with pytest.raises(ValueError):
        roll.replay(data)


def test_randomized_prefixes_preserve_all_historical_outputs():
    import random
    rng = random.Random(20261023)
    for _ in range(100):
        data = sample()
        for batch in data["volumes"]:
            for row in batch["rows"]:
                row["volume"] = rng.randrange(1000000)
            batch["complete"] = rng.random() > .15
        for event in data["settlements"]:
            event["price_e9"] = rng.randrange(100, 200) * 10**9 + rng.randrange(10**9)
        expected = roll.replay(data)
        for n, packet in enumerate(data["decisions"], 1):
            prefix = copy.deepcopy(data)
            prefix["decisions"] = prefix["decisions"][:n]
            cutoff = packet["decision_ns"]
            for key, stamp in [("volumes", "available_at_ns"), ("settlements", "published_ns"), ("contracts", "known_at_ns")]:
                prefix[key] = [r for r in prefix[key] if r[stamp] < cutoff]
            assert roll.replay(prefix) == expected[:n]


def test_holiday_multiple_sessions_count_as_one_official_trading_day():
    from datetime import datetime
    from zoneinfo import ZoneInfo
    def ns(value):
        return int(datetime.fromisoformat(value).replace(tzinfo=ZoneInfo('America/Chicago')).timestamp()) * 10**9
    data = sample()
    data['decisions'], data['volumes'], data['settlements'] = [], [], []
    for c in data['contracts']:
        c.update(known_at_ns=ns('2023-01-01T00:00:00'), listed_ns=ns('2023-01-01T00:00:00'),
                 roll_by_ns=ns('2026-02-24T17:00:00'), risk_deadline_ns=ns('2026-02-26T00:00:00'))
    # Dates reproduce the official holiday trading-day relationship; the RICs
    # and quantities are synthetic. Monday Sep 4 is NOT another decision/day.
    for trading, opening, reference, quantities in [
        ('2023-09-01', '2023-08-31', '2023-08-31', (100, 50)),
        ('2023-09-05', '2023-09-03', '2023-09-01', (100, 150)),
        ('2023-09-06', '2023-09-05', '2023-09-05', (100, 200)),
    ]:
        data['decisions'].append(dict(product='TY',trading_day=trading,previous_day=reference,
            decision_ns=ns(opening+'T16:55:00'),effective_ns=ns(opening+'T17:00:00'),
            previous_close_ns=ns(reference+'T16:00:00'),calendar_known_at_ns=ns('2023-01-01T00:00:00'),
            calendar_source='synthetic_fixture_2023_labor_day_shape',
            reference_days=[dict(day=reference,settlement_expected=True)]))
        data['volumes'].append(dict(product='TY',trading_day=reference,available_at_ns=ns(reference+'T16:50:00'),
            complete=True,source='tas_printable',rows=[dict(ric=r,volume=q) for r,q in zip(['TYH26','TYM26'],quantities)]))
    actual = roll.replay(data)
    assert [r['reason'] for r in actual] == ['seed','hold','volume']
    assert actual[-1]['trading_day'] == '2023-09-06'
    assert actual[1]['confirmation_count'] == 1
