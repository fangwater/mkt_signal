import csv
import importlib.util
import io
import json
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('raw_reference', ROOT/'scripts/cme_raw_ll2_reference.py')
raw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(raw)
FIXTURE = ROOT/'crates/cme_tas_replay/tests/fixtures/raw_ll2_small.csv'


def test_fixed_contract_and_no_fill_of_empty_buckets():
    result = raw.extract([FIXTURE], 15)
    assert result == json.loads(FIXTURE.with_suffix('.expected.json').read_text())
    assert result['census']['messages'] == 9
    assert result['census']['depth_messages'] == 7
    es = [r for r in raw.minute_snapshots(result['snapshots']) if r['seconds']==60 and r['ric']=='ESH24']
    assert len(es)==3  # minute 2 and status-only minute 5 produce no data
    first = es[0]
    assert first['known'] & (1<<6)  # explicitly empty bid2 price is known
    assert not first['known'] & (1<<7)  # bid2 size has never been initialised
    import struct
    value = bytes.fromhex(first['value_hex'])
    assert struct.unpack_from('<q', value, 40)[0] == 7_000_000_000
    assert struct.unpack_from('<q', value, 72)[0] == raw.MISSING
    assert struct.unpack_from('<q', value, 80)[0] == raw.MISSING


def test_cross_part_message_and_state(tmp_path):
    text = FIXTURE.read_text()
    cut = text.index(',,,,FID,')
    a,b = tmp_path/'a.csv',tmp_path/'b.csv'
    a.write_text(text[:cut]);b.write_text(text[cut:])
    assert raw.extract([a,b],15) == raw.extract([FIXTURE],15)


def test_malformed_input_and_time_reversal_fail(tmp_path):
    text = FIXTURE.read_text()
    for invalid in [text.replace(',UPDATE,',',UNSUPPORTED,'), text.replace('00:05:00.000000000Z','00:00:00.000000000Z'), text[:text.index(',,,,FID,')], raw.HEADER[0]+'\n']:
        p=tmp_path/'invalid.csv';p.write_text(invalid)
        with pytest.raises(ValueError):raw.extract([p])


def test_numeric_precision_and_invalid_frequency():
    assert raw.scaled('4798.000000001') == 4_798_000_000_001
    assert raw.scaled('') == raw.MISSING
    assert raw.scaled('0') == 0
    for bad in ['0.1234567891','NaN','1e-9','-9223372036.854775808']:
        with pytest.raises(ValueError):raw.scaled(bad)
    with pytest.raises(ValueError):raw.extract([FIXTURE],30)


def test_baseline_overlay_preserves_keys_trades_conflicts_and_missing_minutes():
    fields={f'{side}{i}{field}':None for side in ('bid','ask') for i in range(10) for field in ('p','v')}
    ts=1704153600
    rows=[dict(fields,ric='ESH24',ts=ts+minute*60,mid_price=None,volume=17) for minute in (0,1,2)]
    rows[1]['bid0p']=999.0
    result=raw.fill_baseline_records(rows,raw.extract([FIXTURE],15)['snapshots'])
    assert result[0]['bid0v']==7.0 and result[0]['ask0v']==9.0
    assert result[0]['mid_price'] is not None
    assert result[0]['bid1p'] is None
    assert result[1]['bid0p']==999.0 and result[1]['mid_price'] is None
    assert result[2]==rows[2]
    assert all(r['volume']==17 for r in result)
    assert rows[0]['bid0v'] is None


def test_real_refresh_tenth_level_aliases_and_zero_fid_closing(tmp_path):
    source=FIXTURE.with_name('raw_ll2_refresh.csv')
    expected=json.loads(source.with_suffix('.expected.json').read_text())
    assert raw.extract([source],15)==expected
    assert expected['census']['message_classes']=={'UPDATE':4,'REFRESH':3}
    assert expected['census']['update_types']['CLOSING_RUN']==1
    closing_ns=raw.timestamp_ns('2026-05-10T16:00:00.713347307Z')
    assert all(int.from_bytes(bytes.fromhex(r['key_hex'])[-8:],'big')<closing_ns for r in expected['snapshots'])
    rows=list(csv.reader(source.open()))
    paths=[]
    for i,row in enumerate(rows):
        p=tmp_path/f'{i}.csv'
        with p.open('w',newline='') as out:csv.writer(out).writerow(row)
        paths.append(p)
    assert raw.extract(paths,15)==expected
    by_part=raw.extract(paths,15,partition_order=True)
    import struct
    assert all(struct.unpack_from('<Q',bytes.fromhex(r['value_hex']),12)[0]>>48>0 for r in by_part['snapshots'])


def test_real_zero_field_status_creates_no_snapshot():
    source=FIXTURE.with_name('raw_ll2_status.csv')
    expected=json.loads(source.with_suffix('.expected.json').read_text())
    assert raw.extract([source],15)==expected
    assert expected['census']['message_classes']=={'REFRESH':3,'STATUS':1}
    assert expected['census']['messages']==4 and expected['census']['depth_messages']==3
    assert len(expected['snapshots'])==2  # two refreshes fall into the same 15s/minute bucket


def test_numeric_update_type_preserves_fields_and_classification(tmp_path):
    base=raw.extract([FIXTURE],15)
    text=FIXTURE.read_text()
    for code in ('127','128','255'):
        source=tmp_path/f'{code}.csv';source.write_text(text.replace(',UPDATE,UNSPECIFIED,',f',UPDATE,{code},'))
        result=raw.extract([source],15)
        assert result['snapshots']==base['snapshots']
        assert result['census']['update_types']=={code:9}
    source=tmp_path/'bad.csv';source.write_text(text.replace(',UPDATE,UNSPECIFIED,',',UPDATE,256,'))
    with pytest.raises(ValueError):raw.extract([source],15)


def test_partial_refresh_discards_old_knowledge_and_keeps_explicit_nulls():
    import struct
    source=FIXTURE.with_name('raw_ll2_partial_refresh.csv')
    result=raw.extract([source],15)
    assert result==json.loads(source.with_suffix('.expected.json').read_text())
    rows=[r for r in result['snapshots'] if r['seconds']==15]
    before,refresh,patch,metadata=rows
    assert before['known'] & (1<<54)  # old L10 bid price existed
    assert refresh['known']==((1<<30)-1)|(1<<60)
    assert struct.unpack_from('<q',bytes.fromhex(refresh['value_hex']),32)[0]==raw.MISSING
    assert not refresh['known'] & (1<<54)  # absent L10 is unknown, not inherited
    assert patch['known']==refresh['known']|(1<<54)
    assert struct.unpack_from('<q',bytes.fromhex(patch['value_hex']),392)[0]==123_000_000_000
    assert metadata['known']==0  # zero-depth REFRESH invalidates prior knowledge
    assert result['census']['message_classes']=={'UPDATE':2,'REFRESH':2}


def test_refresh_invalidation_replaces_earlier_depth_in_same_bucket(tmp_path):
    rows=list(csv.reader(FIXTURE.with_name('raw_ll2_partial_refresh.csv').open()))
    counter=0
    for row in rows[1:]:
        if row[0]:
            counter+=1
            row[2]=f'2026-01-02T00:00:0{counter}Z'
    source=tmp_path/'same_bucket.csv'
    with source.open('w',newline='') as f:csv.writer(f).writerows(rows)
    result=raw.extract([source],15)
    assert len(result['snapshots'])==1
    assert all(r['known']==0 for r in result['snapshots'])
    # A zero-FID REFRESH is also a knowledge boundary.
    rows[-2][-1]='0';rows.pop()
    with source.open('w',newline='') as f:csv.writer(f).writerows(rows)
    result=raw.extract([source],15)
    assert len(result['snapshots'])==1
    assert all(r['known']==0 for r in result['snapshots'])


@pytest.mark.parametrize('name', ['small', 'refresh', 'status', 'partial_refresh'])
def test_15s_derives_exact_direct_minute_state_including_invalidation(name):
    source=FIXTURE.with_name(f'raw_ll2_{name}.csv')
    fine=raw.extract([source],15)
    minute=raw.extract([source],60)
    assert all(row['seconds']==15 for row in fine['snapshots'])
    assert raw.minute_snapshots(fine['snapshots'])==minute['snapshots']
    assert fine['census']==minute['census']
    if name=='partial_refresh':
        assert raw.minute_snapshots(fine['snapshots'])[-1]['known']==0
