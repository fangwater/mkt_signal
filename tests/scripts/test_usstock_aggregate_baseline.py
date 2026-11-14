"""Hand-calculated US RTH aggregation and sparse-minute boundary cases."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import zstandard as zstd

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('us_aggregate', ROOT/'scripts/usstock_aggregate_baseline.py')
ref = importlib.util.module_from_spec(spec); spec.loader.exec_module(ref)


def fixture(n=125):
    start = 1704205800  # 2024-01-02, 09:30 New York
    floats = ['volume','amount','buy_volume','buy_amount','sell_volume','sell_amount',
              'open','high','low','close','vwap','buy_vwap','sell_vwap','twap',
              'avg_amount','net_buy_volume','net_buy_amount','net_buy_pct','buy_high','sell_low',
              'midp','bid0p','bid0v','ask0p','ask0v']
    floats += [f'l2_{side}{level}{pv}' for side in ['bid','ask'] for level in range(10) for pv in ['p','v']]
    counts = ['count','buy_count','sell_count','n_seconds']
    cols = {k:[0.0]*n for k in floats}
    cols.update({k:[0]*n for k in counts})
    cols.update(ric=['TEST.N']*n, venue=['NYSE']*n, ts=[start+60*i for i in range(n)],
                book_depth=[10]*n, source_depth_ts_utc_ns=[(start+60*i)*10**9+1 for i in range(n)],
                source_quality_flags=[0]*n)
    for i in range(n):
        cols['n_seconds'][i] = 60
        cols['twap'][i] = 100.0
        # Empty-minute placeholder prices must not contaminate trade extrema.
        for k in ['open','high','low','close']:cols[k][i] = 1000.0
        for side in ['bid','ask']:
            for level in range(10):
                cols[f'l2_{side}{level}p'][i] = 99-level if side=='bid' else 101+level
                cols[f'l2_{side}{level}v'][i] = 10.0
        cols['bid0p'][i], cols['ask0p'][i] = 99.0, 101.0
        cols['bid0v'][i], cols['ask0v'][i] = 10.0, 10.0
        cols['midp'][i] = 100.0
    for i,price,quantity in [(0,100.0,1.0),(59,110.0,3.0)]:
        for k in ['count','buy_count']:cols[k][i] = 1
        for k in ['volume','buy_volume']:cols[k][i] = quantity
        for k in ['amount','buy_amount']:cols[k][i] = price*quantity
        for k in ['open','high','low','close','vwap','buy_vwap','buy_high']:cols[k][i] = price
    schema = pa.schema([(k,pa.large_string()) for k in ['ric','venue']]+
                       [(k,pa.int64()) for k in ['ts']+counts+['source_depth_ts_utc_ns']]+
                       [(k,pa.int8()) for k in ['book_depth','source_quality_flags']]+
                       [(k,pa.float64()) for k in floats])
    return pa.Table.from_pydict(cols,schema=schema)


def test_rth_hours_short_tail_and_trade_fields():
    source=fixture(); bars,meta=ref.aggregate_table(source,'1h','20240102')
    assert bars['ts'].to_pylist()==[1704205800,1704209400,1704213000]
    assert bars['n_seconds'].to_pylist()==[3600,3600,300]
    assert [b['source_minutes'] for b in meta['bars']]==[60,60,5]
    assert bars['volume'][0].as_py()==4
    assert bars['amount'][0].as_py()==430
    assert bars['open'][0].as_py()==100 and bars['close'][0].as_py()==110
    assert bars['high'][0].as_py()==110 and bars['low'][0].as_py()==100
    assert bars['vwap'][0].as_py()==107.5
    assert bars['avg_amount'][0].as_py()==215
    assert bars['buy_high'][0].as_py()==110
    assert bars['net_buy_pct'][0].as_py()==1


def test_gap_restarts_hour_and_day_combines_only_observed_minutes():
    source=fixture(); source=source.take(pa.array(list(range(61))+list(range(63,125))))
    bars,meta=ref.aggregate_table(source,'1h','20240102')
    assert [b['source_minutes'] for b in meta['bars']]==[60,1,60,2]
    assert [b['segment_start'] for b in meta['bars']]==[True,False,True,False]
    day,meta=ref.aggregate_table(source,'1day','20240102')
    assert len(day)==1 and day['n_seconds'][0].as_py()==123*60
    assert meta['bars'][0]['end_ts']==source['ts'][-1].as_py()+60


def test_final_book_and_nulls_are_atomic_and_quality_any_is_separate():
    source=fixture().to_pydict()
    source['source_quality_flags'][10]=8
    source['twap'][10]=None
    for k in source:
        if k.startswith('l2_') or k in ['midp','bid0p','bid0v','ask0p','ask0v','source_depth_ts_utc_ns']:
            source[k][59]=None
    source['book_depth'][59]=0
    source=pa.Table.from_pydict(source,schema=fixture().schema)
    out,meta=ref.aggregate_table(source,'1h','20240102')
    assert out['l2_bid0p'][0].as_py() is None
    assert out['source_depth_ts_utc_ns'][0].as_py() is None
    assert out['book_depth'][0].as_py()==0
    assert out['twap'][0].as_py() is None
    assert out['source_quality_flags'][0].as_py()==0
    assert meta['bars'][0]['source_quality_flags_any']==8


def test_hfq_vwap_weights_adjusted_price_and_rejects_duplicate_keys():
    source=fixture().to_pydict(); source['vwap'][0]=80; source['vwap'][59]=90
    table=pa.Table.from_pydict(source,schema=fixture().schema)
    out,_=ref.aggregate_table(table,'1h','20240102')
    assert out['vwap'][0].as_py()==87.5
    source['ts'][1]=source['ts'][0]
    with pytest.raises(ValueError,match='increase strictly'):
        ref.aggregate_table(pa.Table.from_pydict(source,schema=table.schema),'1h','20240102')


@pytest.mark.parametrize('case',['plain','gap','last_null_book','weighted_missing'])
def test_rust_matches_all_fields_types_nulls_and_bar_metadata(tmp_path,case):
    table=fixture()
    if case=='gap':table=table.take(pa.array(list(range(61))+list(range(63,125))))
    if case=='last_null_book':
        data=table.to_pydict()
        for k in data:
            if k.startswith('l2_') or k in ['midp','bid0p','bid0v','ask0p','ask0v','source_depth_ts_utc_ns']:
                data[k][59]=None
        data['book_depth'][59]=0;data['source_quality_flags'][10]=8;data['twap'][10]=None
        table=pa.Table.from_pydict(data,schema=table.schema)
    if case=='weighted_missing':
        data=table.to_pydict();data['vwap'][0]=None;data['twap'][60]=float('nan')
        table=pa.Table.from_pydict(data,schema=table.schema)
    src=tmp_path/'20240102.parquet';pq.write_table(table,src);stat=src.stat()
    job=dict(input=str(src),ric='TEST.N',venue='NYSE',day='20240102',hfq=False,
             open_ts=1704205800,close_ts=1704205800+390*60,source_bytes=stat.st_size,source_mtime_ns=stat.st_mtime_ns)
    jobs=tmp_path/'jobs.json';jobs.write_text(json.dumps([job]));out=tmp_path/'out'
    result=subprocess.run([str(ROOT/'target/release/usstock_aggregate_baseline'),'--jobs',str(jobs),
                           '--out-root',str(out),'--workers','2','--execute'],capture_output=True,text=True,
                          env={**os.environ,'POLARS_MAX_THREADS':'1'})
    assert result.returncode==0,result.stderr
    for freq in ['1h','1day']:
        expected,meta=ref.aggregate_table(table,freq,'20240102')
        family=out/f'baseline_data_{freq}_raw'
        actual=pq.read_table(family/'NYSE/TEST.N/20240102.parquet')
        assert expected.schema.equals(actual.schema,check_metadata=False)
        for k in expected.column_names:
            assert expected[k].is_null().to_pylist()==actual[k].is_null().to_pylist(),k
            if pa.types.is_floating(expected.schema.field(k).type):
                np.testing.assert_allclose(expected[k].to_numpy(),actual[k].to_numpy(),rtol=1e-14,atol=1e-12,equal_nan=True)
            else:assert expected[k].to_pylist()==actual[k].to_pylist(),k
        with (family/'_bars/NYSE/TEST.N/20240102.json.zst').open('rb') as f,zstd.ZstdDecompressor().stream_reader(f) as d:
            assert json.load(d)==meta
