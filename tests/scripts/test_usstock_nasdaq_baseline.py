"""Hand-calculated atomic-event, trade conservation and source timing checks."""
import csv
import gzip
import importlib.util
import json
import math
import subprocess
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('nasdaq_reference',ROOT/'scripts/usstock_nasdaq_baseline.py')
ref=importlib.util.module_from_spec(spec);spec.loader.exec_module(ref)


def source(tmp_path,day='2021-01-29',extra=None):
    start=int(datetime.fromisoformat(day+'T14:30:00+00:00').timestamp())
    path=tmp_path/'XNAS.ITCH'/'TEST'/f'{day}.csv.gz';path.parent.mkdir(parents=True,exist_ok=True)
    rows=[]
    def add(offset,action,side='N',price='',size=0,order=0,flags=128,delta=0):
        ts=datetime.fromtimestamp(start+offset,tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')+'.000000000+00:00'
        rows.append([ts,160,2,123,action,side,price,size,1,order,flags,delta,len(rows),'TEST'])
    add(-60,'R',flags=8)
    add(-1,'A','B','100',10,1);add(-1,'A','A','101',20,2)
    add(1,'A','B','99',30,3);add(1,'A','A','102',40,4)
    add(10,'T','B','100',3,flags=0);add(10,'F','A','101',3,2,flags=0);add(10,'C','A','101',3,2)
    add(20,'T','N','100.5',2);add(35,'M','B','100',15,1)
    add(59,'T','A','100',1,flags=0,delta=1_100_000_000)
    add(59,'F','B','100',1,1,flags=0,delta=1_100_000_000)
    add(59,'C','B','100',1,1,delta=1_100_000_000)
    add(180,'R')
    if extra:extra(add)
    add(241,'T','B','109',100)
    with gzip.open(path,'wt',newline='') as f:
        w=csv.writer(f);w.writerow(ref.HEADER);w.writerows(rows)
    return dict(day=day,source=str(path),symbol='TEST',open_ts=start,close_ts=start+240)


def extract(day):
    return ref.extract(day['source'],day['symbol'],day['open_ts'],day['close_ts'])


def test_atomic_timing_dedup_and_no_fill(tmp_path):
    x=extract(source(tmp_path));a,b,c,d=x['minutes']
    assert [m['book_depth'] for m in x['minutes']]==[2,2,0,0]
    assert a['ask'][0]==[101*ref.NS,17]
    assert a['bid'][0]==[100*ref.NS,15]
    assert b['bid'][0]==[100*ref.NS,14]
    assert [len(m['trades']) for m in x['minutes']]==[2,1,0,0]
    assert b['trades'][0][0]==(b['ts']*ref.NS+100_000_000)
    rows=ref.aggregate('TEST.O',x)
    assert rows[0]['amount']==501 and rows[0]['volume']==5
    assert rows[0]['unknown_amount']==201 and rows[0]['off_exchange_amount']==0
    assert math.isclose(rows[0]['twap'],5020/60,rel_tol=1e-14)
    assert rows[2]['bid0p'] is None and rows[2]['vwap']==100


@pytest.mark.parametrize('extra,match',[
    (lambda a:a(181,'C','A','101',1,999),'cancel unknown'),
    (lambda a:a(181,'N',flags=144),'quality flags'),
    (lambda a:a(181,'N',flags=160),'quality flags'),
])
def test_reject_bad_source(tmp_path,extra,match):
    with pytest.raises(ValueError,match=match):extract(source(tmp_path,extra=extra))


def test_gap_is_sticky_until_clean_clear_and_does_not_invent_orders(tmp_path):
    def extra(add):
        add(181,'N',flags=132)
        add(182,'C','B','100',10,999)  # lost order: unknown after the gap
        add(183,'T','B','100',2)
        add(184,'A','B','100',10,11)  # a later clean flag cannot recover a book
        add(184,'A','A','101',10,12)
        add(240,'N')
        add(300,'R')
        add(301,'A','B','100',10,21)
        add(301,'A','A','101',10,22)
    day=source(tmp_path,extra=extra);day['close_ts']=day['open_ts']+360
    # Keep physical source order when extending the fixture beyond its old close.
    with gzip.open(day['source'],'rt') as f:rows=list(csv.reader(f))
    body=sorted(rows[1:],key=lambda r:ref.timestamp(r[0])+int(r[11]))
    with gzip.open(day['source'],'wt',newline='') as f:
        w=csv.writer(f);w.writerow(rows[0]);w.writerows(body)
    x=extract(day)
    assert [m['source_quality_flags'] for m in x['minutes']]==[0,0,0,4,4,0]
    assert x['minutes'][3]['book_depth']==0 and x['minutes'][4]['book_depth']==0
    assert x['minutes'][5]['book_depth']==1
    assert len(x['minutes'][3]['trades'])==1
    assert x['bad_book_intervals_ns']==[[(day['open_ts']+181)*ref.NS,(day['open_ts']+300)*ref.NS]]
    binary=ROOT/'target/release/usstock_nasdaq_baseline'
    jobs=tmp_path/'gap_jobs.json';jobs.write_text(json.dumps([dict(ric='TEST.O',days=[day])]))
    out=tmp_path/'gap_out';integers=tmp_path/'gap_integers'
    subprocess.run([str(binary),'--jobs',str(jobs),'--output-root',str(out),
                    '--reference-output',str(integers),'--workers','2'],check=True,capture_output=True,text=True)
    rust=json.loads((integers/'TEST.O_2021-01-29.json').read_text())
    assert rust['minutes']==x['minutes'] and rust['bad_book_intervals_ns']==x['bad_book_intervals_ns']


def test_rust_shared_reference_and_month_threshold(tmp_path):
    binary=ROOT/'target/release/usstock_nasdaq_baseline'
    assert binary.exists(),'build the release binary before this integration check'
    days=[source(tmp_path,'2021-01-28'),source(tmp_path),source(tmp_path,'2021-02-01')]
    jobs=tmp_path/'jobs.json';jobs.write_text(json.dumps([dict(ric='TEST.O',days=days)]))
    out=tmp_path/'out';integer=tmp_path/'integers'
    cmd=[str(binary),'--jobs',str(jobs),'--output-root',str(out),'--reference-output',str(integer),'--workers','4']
    subprocess.run(cmd,check=True,capture_output=True,text=True)
    for i,day in enumerate(days):
        expected=extract(day)
        actual=json.loads((integer/f"TEST.O_{day['day']}.json").read_text())
        for field in ['minutes','actions','flags','rows','instrument_id']:assert actual[field]==expected[field]
        thresholds=None if i<2 else (Decimal(201),Decimal(300))
        expected_rows=ref.aggregate('TEST.O',expected,thresholds)
        actual_rows=pq.read_table(out/'NASDAQ/TEST.O'/f"{day['day'].replace('-','')}.parquet").to_pylist()
        assert len(expected_rows)==len(actual_rows)
        for e,a in zip(expected_rows,actual_rows):
            assert set(e)==set(a)
            for k,v in e.items():
                if isinstance(v,float):assert a[k]==pytest.approx(v,rel=2e-14,abs=1e-12),(k,a[k],v)
                else:assert a[k]==v,(k,a[k],v)
    # Resume must reproduce exact cached trade census and detect output changes.
    subprocess.run(cmd+['--resume'],check=True,capture_output=True,text=True)
    p=out/'NASDAQ/TEST.O/20210129.parquet';p.write_bytes(p.read_bytes()+b'changed')
    failed=subprocess.run(cmd+['--resume'],capture_output=True,text=True)
    assert failed.returncode and 'resume output/cache changed' in failed.stderr


def test_supervised_validation_and_atomic_publication(tmp_path):
    day=source(tmp_path);jobs=tmp_path/'jobs.json'
    jobs.write_text(json.dumps([dict(ric='TEST.O',days=[day])]))
    staging=tmp_path/'building';published=tmp_path/'baseline_data_1min';status=tmp_path/'status.json'
    command=['python',str(ROOT/'scripts/usstock_nasdaq_run.py'),
             '--binary',str(ROOT/'target/release/usstock_nasdaq_baseline'),'--jobs',str(jobs),
             '--staging',str(staging),'--publish',str(published),'--status',str(status),'--workers','2']
    subprocess.run(command,check=True,capture_output=True,text=True)
    assert not staging.exists() and published.exists()
    report=json.loads(status.read_text())
    assert report['state']=='published' and report['validation']['totals']['rows']==4
    assert report['validation']['all_columns_read'] is True
    # A second run must leave the published result intact.
    file=published/'NASDAQ/TEST.O/20210129.parquet';before=file.read_bytes()
    failed=subprocess.run(command,capture_output=True,text=True)
    assert failed.returncode and file.read_bytes()==before


def test_isolate_bad_date_continue_and_do_not_use_incomplete_month(tmp_path):
    days=[source(tmp_path,'2021-01-28',extra=lambda a:a(181,'N',flags=136)),
          source(tmp_path),source(tmp_path,'2021-02-01'),source(tmp_path,'2021-03-01')]
    jobs=tmp_path/'jobs.json';jobs.write_text(json.dumps([dict(ric='TEST.O',days=days)]))
    out=tmp_path/'out';binary=ROOT/'target/release/usstock_nasdaq_baseline'
    command=[str(binary),'--jobs',str(jobs),'--output-root',str(out),'--workers','4']
    first=subprocess.run(command,capture_output=True,text=True)
    assert first.returncode and 'quarantined' in first.stderr
    assert not (out/'NASDAQ/TEST.O/20210128.parquet').exists()
    for date in ['20210129','20210201','20210301']:
        assert (out/f'NASDAQ/TEST.O/{date}.parquet').exists()
    feb=json.loads((out/'_audit/days/NASDAQ/TEST.O/20210201.json').read_text())
    mar=json.loads((out/'_audit/days/NASDAQ/TEST.O/20210301.json').read_text())
    assert feb['threshold_previous_month_sample_trades']==0
    assert mar['threshold_previous_month_sample_trades']==3
    # An explicitly compatible build can reuse exact source/output/cache hashes.
    old_id='0'*64;path=out/'_audit/days/NASDAQ/TEST.O/20210129.json'
    audit=json.loads(path.read_text());audit['build_id']=old_id;path.write_text(json.dumps(audit))
    before={p.name:p.read_bytes() for p in (out/'NASDAQ/TEST.O').glob('*.parquet')}
    resumed=subprocess.run(command+['--resume','--resume-build-id',old_id],capture_output=True,text=True)
    assert resumed.returncode and 'resume input/code changed' not in resumed.stderr
    assert before=={p.name:p.read_bytes() for p in (out/'NASDAQ/TEST.O').glob('*.parquet')}


def test_supervisor_keeps_source_exceptions_unpublished(tmp_path):
    days=[source(tmp_path,'2021-01-28',extra=lambda a:a(181,'N',flags=136)),source(tmp_path)]
    jobs=tmp_path/'jobs.json';jobs.write_text(json.dumps([dict(ric='TEST.O',days=days)]))
    staging=tmp_path/'building';published=tmp_path/'published';status=tmp_path/'status.json'
    subprocess.run(['python',str(ROOT/'scripts/usstock_nasdaq_run.py'),
                    '--binary',str(ROOT/'target/release/usstock_nasdaq_baseline'),'--jobs',str(jobs),
                    '--staging',str(staging),'--publish',str(published),'--status',str(status),
                    '--workers','2'],check=True,capture_output=True,text=True)
    state=json.loads(status.read_text())
    assert state['state']=='needs_source_review'
    assert state['completed_files']==1 and state['quarantined_dates']==1
    assert staging.exists() and not published.exists()


@pytest.mark.parametrize('incomplete_offset,allowed',[(241,True),(239,False)])
def test_unfinished_group_must_be_entirely_after_rth(tmp_path,incomplete_offset,allowed):
    day=source(tmp_path)
    with gzip.open(day['source'],'rt') as f:rows=list(csv.reader(f))
    rows[-1][0]=datetime.fromtimestamp(day['open_ts']+incomplete_offset,tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')+'.000000000+00:00'
    rows[-1][10]='0'
    with gzip.open(day['source'],'wt',newline='') as f:csv.writer(f).writerows(rows)
    jobs=tmp_path/'jobs.json';jobs.write_text(json.dumps([dict(ric='TEST.O',days=[day])]))
    out=tmp_path/'out';integers=tmp_path/'integers'
    command=[str(ROOT/'target/release/usstock_nasdaq_baseline'),'--jobs',str(jobs),
             '--output-root',str(out),'--reference-output',str(integers),'--workers','1']
    r=subprocess.run(command,capture_output=True,text=True)
    if allowed:
        expected=extract(day);actual=json.loads((integers/'TEST.O_2021-01-29.json').read_text())
        assert r.returncode==0 and actual['minutes']==expected['minutes']
        assert expected['ignored_post_session_incomplete_rows']==actual['ignored_post_session_incomplete_rows']==1
    else:
        with pytest.raises(ValueError,match='within/before'):extract(day)
        assert r.returncode and 'within/before' in r.stderr


@pytest.mark.parametrize('trade_event',[False,True])
def test_bound_bad_timestamp_to_one_minute(tmp_path,trade_event):
    day=source(tmp_path)
    with gzip.open(day['source'],'rt') as f:rows=list(csv.reader(f))
    if trade_event:
        next(r for r in rows[1:] if r[4]=='T' and r[5]=='B')[10]='8'
    else:
        at=next(i for i,r in enumerate(rows) if r[4]=='M');rows[at][10]='136'
        witness=rows[at].copy();witness[0]=witness[0].replace('14:30:35','14:30:36')
        witness[4]='N';witness[5]='N';witness[6]='';witness[7]='0';witness[9]='0';witness[10]='128'
        rows.insert(at+1,witness)
    with gzip.open(day['source'],'wt',newline='') as f:csv.writer(f).writerows(rows)
    expected=extract(day);a,b=expected['minutes'][:2]
    assert a['source_quality_flags']==8 and a['book_depth']==0
    assert b['source_quality_flags']==0 and b['book_depth']==2
    bars=ref.aggregate('TEST.O',expected)
    assert bars[0]['volume']==5 and bars[0]['amount']==501
    assert (bars[0]['twap'] is None)==trade_event
    assert bars[1]['twap']==pytest.approx(100.00083333333333)
    jobs=tmp_path/'jobs.json';jobs.write_text(json.dumps([dict(ric='TEST.O',days=[day])]))
    out=tmp_path/'out';integers=tmp_path/'integers'
    subprocess.run([str(ROOT/'target/release/usstock_nasdaq_baseline'),'--jobs',str(jobs),
                    '--output-root',str(out),'--reference-output',str(integers),'--workers','1'],check=True,capture_output=True,text=True)
    actual=json.loads((integers/'TEST.O_2021-01-29.json').read_text())
    assert expected['minutes']==actual['minutes']
    assert expected['bounded_bad_timestamp_intervals_ns']==actual['bounded_bad_timestamp_intervals_ns']


def test_restore_day_rebuilds_following_month_thresholds(tmp_path):
    bad=source(tmp_path,'2021-01-28',extra=lambda a:a(181,'N',flags=136))
    days=[bad,source(tmp_path),source(tmp_path,'2021-02-01')]
    jobs=tmp_path/'jobs.json';jobs.write_text(json.dumps([dict(ric='TEST.O',days=days)]))
    out=tmp_path/'out';command=[str(ROOT/'target/release/usstock_nasdaq_baseline'),
                              '--jobs',str(jobs),'--output-root',str(out),'--workers','2']
    assert subprocess.run(command,capture_output=True).returncode
    feb=out/'NASDAQ/TEST.O/20210201.parquet';old=feb.read_bytes()
    assert sum(pq.read_table(feb)['large_order'].to_pylist())==0
    source(tmp_path,'2021-01-28')  # corrected source only existed in quarantine
    subprocess.run(command+['--resume'],check=True,capture_output=True,text=True)
    assert sum(pq.read_table(feb)['large_order'].to_pylist())==300
    archived=list((out/'_audit/replaced/NASDAQ/TEST.O/20210201').glob('*/baseline.parquet'))
    assert len(archived)==1 and archived[0].read_bytes()==old
