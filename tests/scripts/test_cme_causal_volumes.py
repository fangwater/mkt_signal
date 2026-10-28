import copy
import json
import sys
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
from cme_causal_volumes import freeze

CASES = json.loads((ROOT/'crates/cme_tas_replay/tests/fixtures/causal_volumes.json').read_text())


@pytest.mark.parametrize('case', CASES, ids=lambda c:c['name'])
def test_frozen_volumes(case):
    args = case['request'], case['events'], case['completed_rics']
    if case['valid']:
        assert freeze(*args) == case['expected']
    else:
        with pytest.raises((ValueError,KeyError,TypeError)):
            freeze(*args)


def test_completed_source_prefix_can_feed_the_roll_engine():
    from cme_causal_roll import replay
    fixture = json.loads((ROOT/'crates/cme_tas_replay/tests/fixtures/causal_roll.json').read_text())
    fixture['decisions'] = fixture['decisions'][:1]
    decision = fixture['decisions'][0]
    expected = next(b for b in fixture['volumes'] if b['trading_day'] == decision['previous_day'])
    request = copy.deepcopy(CASES[0]['request'])
    close = decision['previous_close_ns']
    request.update(trading_day=decision['previous_day'], segments=[[close-60*10**9,close]],
                   freeze_ns=decision['decision_ns']-10**9,decision_ns=decision['decision_ns'])
    request['contracts']=[dict(ric=r['ric'],listed_ns=0,known_at_ns=0,source='SYNTHETIC') for r in expected['rows']]
    events=[dict(ric=r['ric'],published_ns=close-10**9,exchange_hms_ns=None,
                 price_e9=100000000000,volume=r['volume'],part=0,seq=0) for r in expected['rows'] if r['volume']]
    frozen=freeze(request,events,[c['ric'] for c in request['contracts']])
    fixture['volumes']=[frozen['batch']]
    result=replay(fixture)[0]
    assert result['reason']=='seed'
    assert result['selected_ric']==max(expected['rows'],key=lambda r:r['volume'])['ric']
