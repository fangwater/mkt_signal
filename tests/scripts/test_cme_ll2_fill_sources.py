"""Same-minute donor priority and validity use independent wire fixtures."""
import importlib.util
import struct
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('raw_reference', ROOT / 'scripts/cme_raw_ll2_reference.py')
reference = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reference)


def row():
    fields = {f'{side}{level}{field}': None for side in ('bid', 'ask')
              for level in range(10) for field in ('p', 'v')}
    return dict(fields, ric='TUH26', ts=1767312000, mid_price=None, volume=123)


def snapshot(bid=100, ask=101, known=27, second=30):
    source = row()
    value = bytearray(432)
    value[:4] = b'L2\x01\x01'
    struct.pack_into('<QQ', value, 4, (source['ts']+second)*10**9, 1)
    for i in range(10):
        struct.pack_into('<qqqqII', value, 32+40*i,
                         reference.MISSING, reference.MISSING,
                         reference.MISSING, reference.MISSING, 0, 0)
    struct.pack_into('<qqqq', value, 32, bid*10**9, 2*10**9, ask*10**9, 3*10**9)
    return dict(ric=source['ric'], seconds=60, known=known,
                key_hex=(source['ric'].encode().ljust(16,b'\0') + struct.pack('>Q', source['ts']*10**9)).hex(),
                value_hex=value.hex())


def test_first_valid_source_wins_without_filling_missing_levels_from_next_source():
    first, second = snapshot(), snapshot(200, 201)
    result = reference.fill_baseline_sources([row()], [[first], [second]], True)[0]
    assert result['bid0p'] == 100 and result['mid_price'] == 100.5
    assert result['bid1p'] is None and result['volume'] == 123


def test_crossed_or_unknown_l1_does_not_hide_valid_lower_priority_donor():
    for bad in (snapshot(102, 101), snapshot(100, 101, 3)):
        result = reference.fill_baseline_sources([row()], [[bad], [snapshot(200, 201)]], True)[0]
        assert result['bid0p'] == 200 and result['mid_price'] == 200.5


def test_missing_current_minute_and_other_ric_do_not_fill():
    past = snapshot()
    original = row()
    original['ts'] += 60
    assert reference.fill_baseline_sources([original], [[past]], True) == [original]
    original = row()
    original['ric'] = 'TUM26'
    assert reference.fill_baseline_sources([original], [[past]], True) == [original]


def test_future_source_timestamp_is_rejected():
    import pytest
    with pytest.raises(ValueError, match='outside baseline minute'):
        reference.fill_baseline_sources([row()], [[snapshot(second=60)]], True)
