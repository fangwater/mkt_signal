import importlib.util
from pathlib import Path
import numpy as np
import pyarrow as pa
import pytest

spec=importlib.util.spec_from_file_location('cme_aggregate',Path(__file__).resolve().parents[2]/'scripts/cme_aggregate_baseline.py')
ref=importlib.util.module_from_spec(spec);spec.loader.exec_module(ref)


def source(times,*,offset=-200):
    names=['contract_id','ric','ts']+ref.SUM+ref.DERIVED+ref.PRICE+ref.BOOK+['segment_id','offset_e9']
    data={name:[] for name in names}
    for i,t in enumerate(times):
        row=dict.fromkeys(names,0.0)
        row.update(contract_id='HG',ric='HGH25',ts=t,segment_id=1,offset_e9=int(offset*1e9),
                   volume=1.0,count=1.0,amount=100.0+i,open=100.0+i+offset,high=101.0+i+offset,
                   low=99.0+i+offset,close=100.0+i+offset,vwap=100.0+i+offset,
                   buy_vwap=None,sell_vwap=None,implied_vwap=None,twap=100.0+i+offset,implied_twap=None)
        for name in ref.BOOK:row[name]=None
        for name in names:data[name].append(row[name])
    types={name:(pa.large_string() if name in ['contract_id','ric'] else pa.int64() if name in ['ts','segment_id','offset_e9'] else pa.float64()) for name in names}
    return pa.table({name:pa.array(value,type=types[name]) for name,value in data.items()})


def replace(table,name,values):
    i=table.schema.get_field_index(name);return table.set_column(i,table.schema.field(i),pa.array(values,type=table[name].type))


def test_gaps_midnight_and_partial_tails_are_explicit():
    t=source([86340,86400,86460,86520,86580,86640,90000,90060])
    out,m=ref.aggregate_table(t,'5min',20250102)
    assert out['ts'].to_pylist()==[86340,86640,90000]
    assert m['source_minutes']==[5,1,2] and m['segment_start']==[True,False,True]
    assert m['end_ts']==[86640,86700,90120]


def test_hfq_vwap_uses_adjusted_quote_and_source_weights():
    t=source([0,60]);t=replace(t,'volume',[1.,3.])
    out,_=ref.aggregate_table(t,'5min',20250102)
    assert out['vwap'][0].as_py()==(-100.-99.*3)/4
    assert out['amount'][0].as_py()==201.0
    assert out['avg_amount'][0].as_py()==100.5


def test_no_trade_carried_ohlc_cannot_dominate_real_trades():
    t=source([0,60,120],offset=0)
    t=replace(t,'count',[0.,1.,0.])
    for name in ['open','high','low','close']:t=replace(t,name,[900.,102.,800.])
    out,_=ref.aggregate_table(t,'5min',20250102)
    assert [out[n][0].as_py() for n in ['open','high','low','close']]==[102.]*4


def test_last_minute_invalid_book_stays_missing():
    t=source([0,60]);t=replace(t,'bid0p',[1.,None]);t=replace(t,'bid0v',[2.,None])
    out,m=ref.aggregate_table(t,'5min',20250102,{('HGH25',0):30*10**9})
    assert out['bid0p'][0].as_py() is None and m['source_depth_ts_utc_ns']==[None]


def test_source_time_is_the_same_last_minute_not_bar_start():
    t=source([0,60]);t=replace(t,'bid0p',[None,1.]);t=replace(t,'bid0v',[None,2.])
    out,m=ref.aggregate_table(t,'5min',20250102,{('HGH25',60):90*10**9})
    assert out['bid0p'][0].as_py()==1 and m['source_depth_ts_utc_ns']==[90*10**9]
    with pytest.raises(ValueError):ref.aggregate_table(t,'5min',20250102,{('HGH25',60):59*10**9})


def test_daily_bar_uses_official_day_across_multiple_sessions():
    t=source([0,60,7200,7260]);out,m=ref.aggregate_table(t,'1day',20250102)
    assert len(out)==1 and m['source_minutes']==[4] and m['end_ts']==[7320]
    assert out['twap'][0].as_py()==-98.5


def test_missing_twap_propagates_and_zero_weight_missing_vwap_does_not():
    t=source([0,60]);t=replace(t,'volume',[0.,1.]);t=replace(t,'vwap',[None,-99.]);t=replace(t,'twap',[None,-99.])
    out,_=ref.aggregate_table(t,'5min',20250102)
    assert out['vwap'][0].as_py()==-99 and out['twap'][0].as_py() is None


def test_contract_identity_and_offset_do_not_mix():
    t=source([0,60]);t=replace(t,'ric',['HGH25','HGK25'])
    out,_=ref.aggregate_table(t,'1day',20250102)
    assert len(out)==2 and out['volume'].to_pylist()==[1.,1.]
    with pytest.raises(ValueError):ref.aggregate_table(source([0,0]),'5min',20250102)


@pytest.mark.parametrize('frequency',['5min','15min','1h'])
def test_future_prices_do_not_change_completed_bars(frequency):
    n=ref.FREQUENCIES[frequency];t=source(list(range(0,(n+3)*60,60)))
    out,_=ref.aggregate_table(t,frequency,20250102)
    prefix,_=ref.aggregate_table(t.slice(0,n+1),frequency,20250102)
    assert out.slice(0,1).equals(prefix.slice(0,1))


def test_holiday_official_day_can_exceed_1440_open_minutes():
    t=source(list(range(0,1500*60,60)))
    out,m=ref.aggregate_table(t,'1day',20211126)
    assert len(out)==1 and m['source_minutes']==[1500]
