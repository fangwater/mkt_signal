"""Fixed Python/Rust regressions for native CME schemas and awkward bar edges."""
import csv,io,json,os,subprocess
from pathlib import Path
import numpy as np
import pyarrow.parquet as pq
import pytest
import zstandard as zstd
import importlib.util
spec=importlib.util.spec_from_file_location('cme_fixed_fixture',Path(__file__).with_name('test_cme_aggregate_baseline.py'))
fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)
source,replace,ref=fixture.source,fixture.replace,fixture.ref

BINARY=Path(__file__).resolve().parents[2]/'target/release/cme_aggregate_baseline'

@pytest.mark.parametrize('case',['missing_last_book','weighted_missing','holiday_long_day','offset_split','raw_schema'])
def test_native_fields_and_metadata_match_independent_python(tmp_path,case):
    assert BINARY.is_file(),'build --release -p cme_tas_replay --bin cme_aggregate_baseline'
    t=source([86340+i*60 for i in range(17)]+[90000,90060])
    donor={}
    if case=='missing_last_book':
        n=len(t);t=replace(t,'bid0p',[-100.]+[None]*(n-1));t=replace(t,'bid0v',[7.]+[None]*(n-1));donor={('HGH25',86340):86340*10**9+50*10**9}
    elif case=='weighted_missing':
        t=replace(t,'vwap',[None]+t['vwap'].to_pylist()[1:]);t=replace(t,'count',[0.]+[1.]*(len(t)-1));t=replace(t,'high',[999.]+t['high'].to_pylist()[1:])
    elif case=='holiday_long_day':t=source(list(range(0,1500*60,60)))
    elif case=='offset_split':t=replace(t,'offset_e9',[-200*10**9]*7+[-199*10**9]*(len(t)-7))
    else:t=t.drop(['segment_id','offset_e9'])
    p=tmp_path/'20250102.parquet';pq.write_table(t,p)
    audit=tmp_path/'donor.csv.zst';s=io.StringIO();w=csv.writer(s);w.writerow(['ric','ts','source_ts_utc_ns'])
    for (ric,ts),ns in donor.items():w.writerow([ric,ts,ns])
    audit.write_bytes(zstd.ZstdCompressor().compress(s.getvalue().encode()))
    jobs=tmp_path/'jobs.json';jobs.write_text(json.dumps([dict(input=str(p),depth_audit=str(audit),exchange='TEST',product='HG',day=20250102,hfq=case!='raw_schema')]))
    out=tmp_path/'out';r=subprocess.run([str(BINARY),'--jobs',str(jobs),'--out-root',str(out),'--workers','2','--execute'],env={**os.environ,'POLARS_MAX_THREADS':'1','RAYON_NUM_THREADS':'1'},capture_output=True,text=True)
    assert r.returncode==0,r.stderr
    for freq in ref.FREQUENCIES:
        expected,m=ref.aggregate_table(t,freq,20250102,donor)
        root=out/(f'baseline_data_{freq}'+('' if case=='raw_schema' else '_hfq'))
        actual=pq.read_table(root/'TEST/HG/20250102.parquet')
        assert expected.schema.equals(actual.schema,check_metadata=False)
        for name in expected.column_names:
            a,b=expected[name].to_numpy(),actual[name].to_numpy()
            if a.dtype.kind=='f':np.testing.assert_allclose(a,b,rtol=1e-14,atol=1e-13,equal_nan=True)
            else:assert np.array_equal(a,b)
        with (root/'_bars/TEST/HG/20250102.json.zst').open('rb') as f,zstd.ZstdDecompressor().stream_reader(f) as d:assert json.load(d)==m
