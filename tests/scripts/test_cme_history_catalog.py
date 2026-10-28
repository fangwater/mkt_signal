import importlib.util
import json
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('catalog', ROOT/'scripts/cme_history_catalog.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


@pytest.mark.parametrize('case', json.loads((ROOT/'crates/cme_tas_replay/tests/fixtures/history_catalog.json').read_text()), ids=lambda c:c['name'])
def test_reviewed_history(case):
    args = case['catalog'],case['products'],case['start'],case['end']
    if case['valid']:
        output = module.project(*args)
        assert output == case['expected']
        assert {r['trading_day'] for r in output} == {'2021-11-24','2021-11-26'}
    else:
        with pytest.raises((ValueError,KeyError,TypeError)):
            module.project(*args)
