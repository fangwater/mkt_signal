import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
from cme_trade_timing import bar_time, timing_audit, MINUTE_NS


def test_fixed_source_timestamps_and_no_future_bar():
    cases = json.loads((ROOT/'crates/cme_tas_replay/tests/fixtures/trade_timing.json').read_text())
    for c in cases:
        for basis in ('exchange','publication'):
            result = bar_time(c['published_ns'],c['exchange_hms_ns'],basis)
            assert result == c[basis+'_ns']
            if basis == 'publication':
                assert c['published_ns'] < (result//MINUTE_NS+1)*MINUTE_NS
        assert timing_audit(c['published_ns'],c['exchange_hms_ns'],c['volume']) == c['audit']


def test_late_print_is_not_in_the_old_bar_prefix():
    source = 1764044430849697706
    exchange_hms = 58963000000000
    t = bar_time(source,exchange_hms,'publication')
    assert t > 1764024900000000000  # Already past the earlier roll decision.
    assert t != bar_time(source,exchange_hms,'exchange')
