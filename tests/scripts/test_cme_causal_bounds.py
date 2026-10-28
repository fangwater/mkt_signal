import copy
import itertools
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import cme_causal_roll as roll


def replay(data):
    return roll.replay(data)


def sample():
    return json.loads((ROOT / 'crates/cme_tas_replay/tests/fixtures/causal_roll.json').read_text())


def bounded(row, lower, upper):
    row.update(volume=lower if lower == upper else None, volume_lower=lower, volume_upper=upper)


def test_seed_interval_certificate_matches_every_integer_assignment():
    intervals = [(lower,upper) for lower in range(4) for upper in range(lower,4)]
    for left,right in itertools.product(intervals, repeat=2):
        data = sample()
        data['decisions'] = data['decisions'][:1]
        for row,pair in zip(data['volumes'][0]['rows'], (left,right)):
            bounded(row,*pair)
        actual = replay(data)[0]
        outcomes = set()
        for a,b in itertools.product(range(left[0],left[1]+1),range(right[0],right[1]+1)):
            exact = copy.deepcopy(data)
            exact['volumes'][0]['rows'] = [dict(ric='TYH26',volume=a),dict(ric='TYM26',volume=b)]
            result = replay(exact)[0]
            outcomes.add(result['selected_ric'])
        if len(outcomes) == 1 and None not in outcomes:
            assert actual['selected_ric'] == next(iter(outcomes)), (left,right)
        else:
            assert actual['reason'] == 'blocked' and actual['selected_ric'] is None, (left,right)


def confirmation_sample():
    data = sample()
    end = data['decisions'][-1]['effective_ns']
    for contract in data['contracts']:
        if contract['ric'] == 'TYH26':
            contract.update(roll_by_ns=end+10*86400*10**9, risk_deadline_ns=end+11*86400*10**9)
    for i,batch in enumerate(data['volumes']):
        bounded(batch['rows'][0],10,11)
        bounded(batch['rows'][1],3 if i == 0 else 20,4 if i == 0 else 21)
    return data


def test_bounded_challenger_still_needs_two_days_and_preserves_every_prefix():
    data = confirmation_sample()
    rows = replay(data)
    assert [r['reason'] for r in rows] == ['seed','hold','volume','hold','hold']
    assert rows[2]['confirmation_count'] == 2
    assert rows[2]['selected_volume'] is None
    assert (rows[2]['selected_volume_lower'],rows[2]['selected_volume_upper']) == (20,21)
    for i,d in enumerate(data['decisions'],1):
        prefix = copy.deepcopy(data)
        prefix['decisions'] = prefix['decisions'][:i]
        for name,key in [('volumes','available_at_ns'),('contracts','known_at_ns'),('settlements','published_ns')]:
            prefix[name] = [r for r in prefix[name] if r[key] < d['decision_ns']]
        assert replay(prefix) == rows[:i]
    data['volumes'].append(dict(data['volumes'][1],available_at_ns=data['decisions'][-1]['decision_ns']+1,
                                complete=False))
    assert replay(data) == rows


def test_ambiguous_day_resets_confirmation_and_does_not_invent_a_roll():
    data = confirmation_sample()
    bounded(data['volumes'][2]['rows'][0],10,30)
    rows = replay(data)
    assert rows[2]['reason'] == 'blocked' and rows[2]['detail'] == 'ambiguous_volume_ranking'
    assert rows[2]['exit_ric'] is None
    assert rows[3]['reason'] == 'hold' and rows[3]['confirmation_count'] == 1
    assert rows[4]['reason'] == 'volume' and rows[4]['confirmation_count'] == 2


def test_forced_exit_filters_old_leg_before_bounded_comparison():
    data = sample()
    bounded(data['volumes'][2]['rows'][0],1_000_000,2_000_000)
    bounded(data['volumes'][2]['rows'][1],10,20)
    row = replay(data)[2]
    assert row['reason'] == 'deadline' and row['selected_ric'] == 'TYM26'
    assert row['selected_volume'] is None and row['selected_volume_lower'] == 10


def test_uncertain_positive_successor_never_delays_deadline_exit():
    data = sample()
    bounded(data['volumes'][2]['rows'][1],0,20)
    row = replay(data)[2]
    assert row['reason'] == 'exit_only' and row['exit_ric'] == 'TYH26'
    assert row['selected_ric'] is None and row['detail'] == 'deadline_ambiguous_volume_ranking'


def test_missing_source_cannot_be_replaced_by_even_decisive_bounds():
    data = confirmation_sample()
    data['volumes'][1]['complete'] = False
    row = replay(data)[1]
    assert row['reason'] == 'blocked' and row['detail'] == 'volume_missing_incomplete_or_unavailable'


def test_chinese_roll_report_keeps_unknown_quantities_and_prior_confirmation_bounds():
    from cme_causal_roll_tables import chinese_report, daily_rows
    data = confirmation_sample()
    decisions = replay(data)
    assert len(daily_rows(decisions)) == 5
    report = chinese_report('TY',decisions,data['contracts'])
    assert '[20, 21]（精确值未知）' in report
    assert '较早确认日 2026-02-23' in report
    assert '严格高于旧腿上界并连续确认' in report
    assert '新腿 None 手' not in report


@pytest.mark.parametrize('bad', [
    dict(volume=None,volume_lower=1),
    dict(volume=None,volume_lower=-1,volume_upper=2),
    dict(volume=None,volume_lower=2,volume_upper=1),
    dict(volume=None,volume_lower=1.0,volume_upper=2),
    dict(volume=None,volume_lower=True,volume_upper=2),
    dict(volume=None,volume_lower=1,volume_upper=2**63),
    dict(volume=1,volume_lower=1,volume_upper=2),
])
def test_invalid_bounds_are_rejected(bad):
    data = sample()
    data['volumes'][0]['rows'][0] = dict(ric='TYH26',**bad)
    with pytest.raises(ValueError):
        replay(data)
