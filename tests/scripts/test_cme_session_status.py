import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('status', ROOT/'scripts/cme_session_status.py')
status = importlib.util.module_from_spec(spec)
spec.loader.exec_module(status)
CASES = json.loads((ROOT/'crates/cme_tas_replay/tests/fixtures/session_status.json').read_text())


@pytest.mark.parametrize('case', CASES, ids=lambda x:x['name'])
def test_status_cases(case):
    args = case['planned'], case['events'], case['product'], case['ric']
    if case['valid']:
        assert status.project(*args) == case['expected']
    else:
        with pytest.raises((ValueError, KeyError, TypeError)):
            status.project(*args)


def minute_starts(spans, closed_before):
    return {t for lo, hi in spans for t in range(lo, hi, status.MINUTE_NS)
            if t + status.MINUTE_NS <= closed_before}


@pytest.mark.parametrize('case', [c for c in CASES if c['valid']], ids=lambda x:x['name'])
def test_future_status_messages_do_not_change_closed_minutes(case):
    full = status.project(case['planned'], case['events'], case['product'], case['ric'])
    cutoffs = set(range(0, 601_000_000_000, status.MINUTE_NS))
    for e in case['events']:
        cutoffs.update((e['known_at_ns'], e['known_at_ns']+1, e['effective_ns'], e['effective_ns']+1))
    for cutoff in sorted(cutoffs):
        visible = [e for e in case['events'] if e['known_at_ns'] < cutoff]
        prefix = status.project(case['planned'], visible, case['product'], case['ric'])
        assert minute_starts(prefix, cutoff) == minute_starts(full, cutoff)
