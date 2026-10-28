"""Real CLI checks against the independent Python reference, including boundaries."""
import csv
import concurrent.futures
import datetime as dt
import gzip
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import shutil
import tempfile
import sys
import unittest

import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('tradfi_reference', ROOT/'scripts/crypto_tradfi_reference.py')
reference=importlib.util.module_from_spec(spec);spec.loader.exec_module(reference)
BIN=Path(os.environ.get('TRADFI_BINARY', ROOT/'target/release/crypto_tradfi_baseline'))


class IndependentVerifier(unittest.TestCase):
    def test_vectorized_checks_match_scalar_formulas_at_gaps_and_partial_books(self):
        spec=importlib.util.spec_from_file_location('tradfi_runner',ROOT/'scripts/run_crypto_tradfi.py')
        runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
        rng=np.random.default_rng(20261026)
        for length in [0,1,2,3,4,5,10000]:
            values=rng.normal(size=(length,7));values[:,0]=np.arange(length)*60
            values[:,1]=values[:,0]+60
            values[:,2]=rng.choice([0,1,10,18,20],size=length,p=[.02,.01,.01,.01,.95])
            if length>5:values[5,0]+=1
            expected={name:np.full(length,np.nan) for name in ['factor_005','factor_007','factor_013','factor_081']}
            for i in range(1,length):
                if values[i-1,1]!=values[i,0]:continue
                if values[i-1,2]==20:
                    expected['factor_007'][i]=values[i-1,4];expected['factor_013'][i]=values[i-1,5]
                if i>=2 and values[i-2,1]==values[i-1,0] and np.all(values[i-2:i,2]==20):
                    expected['factor_005'][i]=values[i-1,3]-values[i-2,3]
                if i>=3 and np.all(values[i-3:i-1,1]==values[i-2:i,0]) and np.all(values[i-3:i,2]==20):
                    expected['factor_081'][i]=values[i-3:i,6].mean()
            actual=runner.independent_four_formulas(values)
            for name in expected:
                np.testing.assert_array_equal(actual[name],expected[name],err_msg=f'{length} {name}')


class TradfiReplay(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.day='2026-01-31';self.sym='XAUUSDT'
        self.start=int(dt.datetime(2026,1,31,tzinfo=dt.timezone.utc).timestamp())
        self.manifest=self.root/'manifest.json'
        self.manifest.write_text(json.dumps({'symbols':[self.sym],'end_date':self.day}))
        self.paths={}
        for kind in ['trades','incremental_book_L2']:
            p=self.root/'source/binance_usd_xauusdt'/kind/f'binance-futures_{kind}_{self.day}_XAUUSDT.csv.gz'
            p.parent.mkdir(parents=True);self.paths[kind]=p
        trades=[];books=[];base=self.start*10**6
        for minute in range(90):
            recv=base+minute*60000000
            # Trade exchange time crosses a minute boundary; receive time wins.
            trades.append(['binance-futures',self.sym,recv-1000000,recv+1000000,minute*2+1,'buy' if minute%2 else 'sell',100+minute%7,0.1+minute%3])
            if minute%3:
                trades.append(['binance-futures',self.sym,recv+30000000,recv+30000000,minute*2+2,'sell',101+minute%5,0.2])
            # A complete atomic snapshot every minute must clear all old levels.
            levels=12 if minute==8 else 20
            mid=100+minute/10
            for side in ['ask','bid']:
                for level in range(levels):
                    price=mid+(level+1)*(1 if side=='ask' else -1)*0.01
                    books.append(['binance-futures',self.sym,recv-1000000,recv+500000,'true',side,price,level+1+minute%4])
        self.write('trades',trades);self.write('incremental_book_L2',books)

    def tearDown(self):self.tmp.cleanup()

    def write(self,kind,rows):
        header='exchange,symbol,timestamp,local_timestamp,'+('id,side,price,amount' if kind=='trades' else 'is_snapshot,side,price,amount')
        with gzip.open(self.paths[kind],'wt',newline='') as f:
            w=csv.writer(f);w.writerow(header.split(','));w.writerows(rows)

    def run_cli(self,phase='baseline',expect=0,extra=(),out=None):
        r=subprocess.run([str(BIN),'--manifest',str(self.manifest),'--source-root',str(self.root/'source'),
                          '--out-root',str(out or self.root/'out'),'--phase',phase,'--workers','1','--execute',*extra],capture_output=True,text=True)
        self.assertEqual(r.returncode,expect,r.stdout+r.stderr);return r

    def test_all_columns_and_coarse_bars(self):
        self.run_cli()
        rows=reference.day(self.paths['trades'],self.paths['incremental_book_L2'],self.sym,self.start,self.start+86400)
        for n in [1,5,15]:
            expected=rows if n==1 else reference.aggregate(rows,n)
            actual=pq.read_table(self.root/f'out/baseline_data_{n}min/binance-futures/XAUUSDT/20260131.parquet').to_pandas()
            self.assertEqual(set(actual.columns),set(expected[0]))
            self.assertEqual(len(actual),len(expected))
            for col in actual:
                if col=='symbol':self.assertEqual(actual[col].tolist(),[self.sym]*len(expected));continue
                wanted=np.array([np.nan if r[col] is None else r[col] for r in expected],dtype=float)
                np.testing.assert_allclose(actual[col].to_numpy(dtype=float),wanted,rtol=2e-13,atol=1e-10,equal_nan=True,err_msg=f'{n}min {col}')
        self.assertEqual(rows[8]['book_depth'],12)
        self.assertIsNone(rows[8]['bid12p'])
        self.assertIsNone(rows[0]['twap'])
        self.assertEqual(rows[0]['count'],1)

    def test_factor_schema_and_prior_bar_alignment(self):
        self.run_cli();self.run_cli('factors')
        for n in [1,5,15]:
            baseline=pq.read_table(self.root/f'out/baseline_data_{n}min/binance-futures/XAUUSDT/20260131.parquet').to_pandas()
            factors=pq.read_table(self.root/f'out/baseline_factor_{n}min/binance-futures/XAUUSDT/20260131.parquet').to_pandas()
            self.assertEqual(factors.shape,(len(baseline),634))
            self.assertTrue(factors.iloc[0,2:].isna().all())
            self.assertEqual(factors.ts.tolist(),baseline.ts.tolist())
            self.assertFalse(np.isinf(factors.iloc[:,2:].to_numpy()).any())
            complete=baseline.book_depth.eq(20)
            expected=baseline.mid_price.diff().where(complete & complete.shift(1,fill_value=False)).shift(1)
            np.testing.assert_allclose(factors.factor_005,expected,atol=1e-12,equal_nan=True)
            incomplete_previous=baseline.book_depth.shift(1).ne(20)
            self.assertTrue(factors.loc[incomplete_previous].iloc[:,2:].isna().all().all())
            if n==1:
                # The partial closing book at minute 8 cannot erase factors
                # already usable at its start; minute 9 must be all null.
                self.assertTrue(np.isfinite(factors.factor_007.iloc[8]))
                self.assertTrue(factors.iloc[9,2:].isna().all())

    def test_independent_verification_in_isolated_processes(self):
        self.run_cli();self.run_cli('factors')
        copied=self.root/'copied';shutil.copytree(self.root/'out',copied)
        sys.path.insert(0,str(ROOT/'scripts'))
        try:
            import run_crypto_tradfi as runner
            jobs=[(self.root/'out',self.sym),(copied,self.sym)]
            with concurrent.futures.ProcessPoolExecutor(max_workers=2,initializer=runner.single_thread_arrow) as pool:
                results=list(pool.map(runner.verify_symbol,jobs))
        finally:sys.path.pop(0)
        self.assertEqual(results[0],results[1])
        self.assertEqual(results[0]['output_hash_checks'],6)
        self.assertEqual(results[0]['source_stat_checks'],2)
        self.assertEqual(set(results[0]['finite_counts']),{'1','5','15'})

    def test_all_31_completed_formulas_against_authoritative_python(self):
        pool_spec=importlib.util.spec_from_file_location('pool',ROOT/'final_factor_pool_update20260123.py')
        pool=importlib.util.module_from_spec(pool_spec);pool_spec.loader.exec_module(pool)
        count=800;x=np.arange(count,dtype=float)
        frame=pd.DataFrame({name:np.zeros(count) for name in reference.FIELDS})
        frame['close']=100+np.sin(x*.1)+x*.002;frame['open']=frame.close+np.cos(x*.13)*.1
        frame['high']=np.maximum(frame.open,frame.close)+.2;frame['low']=np.minimum(frame.open,frame.close)-.2
        frame['volume']=20+x%13;frame['amount']=frame.close*frame.volume;frame['count']=5+x%7
        frame['avg_amount']=frame.amount/frame['count']
        for name,weight in [('buy',.6),('sell',.4)]:
            frame[name+'_amount']=frame.amount*weight;frame[name+'_volume']=frame.volume*weight
            frame[name+'_vwap']=frame.close;frame[name+'_count']=frame['count']*weight
        frame['vwap']=frame.close
        for size,weight in [('large',.5),('medium',.3),('small',.2)]:
            frame[size+'_order']=frame.amount*weight
            frame[size+'_buy']=frame.buy_amount*weight;frame[size+'_sell']=frame.sell_amount*weight
            frame['net_buy_'+size]=frame[size+'_buy']-frame[size+'_sell']
        for i in range(20):
            frame[f'bid{i}p']=frame.close-.01*(i+1);frame[f'ask{i}p']=frame.close+.01*(i+1)
            frame[f'bid{i}v']=10+i+x%3;frame[f'ask{i}v']=11+i+x%5
        frame['symbol']=self.sym;frame['ts']=self.start+x.astype('int64')*60
        frame['end_ts']=frame.ts+60;frame['book_depth']=np.full(count,20,dtype=np.uint32)
        for n in [1,5,15]:
            p=self.root/f'out/baseline_data_{n}min/binance-futures/XAUUSDT/20260131.parquet';p.parent.mkdir(parents=True)
            pq.write_table(pa.Table.from_pandas(frame,preserve_index=False),p)
        self.run_cli('factors')
        for period,label in [(60,'5m'),(2880,'240m')]:
            b=frame.buy_amount.rolling(period,min_periods=1).sum();s=frame.sell_amount.rolling(period,min_periods=1).sum()
            frame['active_buy_ratio_'+label]=(b/(b+s)).where(b+s!=0,.5)
        for period,label in [(360,'30m'),(1440,'120m')]:
            denom=(frame.large_order+frame.medium_order+frame.small_order).rolling(period,min_periods=1).sum()
            for size in ['large','small']:frame[size+'_pct_'+label]=(frame[size+'_order'].rolling(period,min_periods=1).sum()/denom).where(denom!=0,0)
        denom=frame.small_order.rolling(180,min_periods=1).sum()
        frame['net_buy_small_pct_15m']=(frame.net_buy_small.rolling(180,min_periods=1).sum()/denom).where(denom!=0,0)
        names=[f'baseline_{i:03}' for i in [23,30,33,37,41,44,48,50,56,64,75,78,84,89,94,95,97,102,106,108,130,142,144,147,150,155,165,176,183,186,197]]
        actual=pq.read_table(self.root/'out/baseline_factor_1min/binance-futures/XAUUSDT/20260131.parquet').to_pandas()
        for name in names:
            expected=pd.Series(getattr(pool,name)(frame)).shift(1).replace([np.inf,-np.inf],np.nan)
            np.testing.assert_allclose(actual[name],expected,rtol=1e-8,atol=1e-9,equal_nan=True,err_msg=name)

    def test_prior_month_thresholds_and_cross_day_carry(self):
        next_day='2026-02-01'
        for kind,old in list(self.paths.items()):
            with gzip.open(old,'rt') as f:lines=list(csv.reader(f))
            rows=lines[1:]
            for row in rows:row[2]=str(int(row[2])+86400000000);row[3]=str(int(row[3])+86400000000)
            p=old.parent/f'binance-futures_{kind}_{next_day}_XAUUSDT.csv.gz'
            with gzip.open(p,'wt',newline='') as f:writer=csv.writer(f);writer.writerow(lines[0]);writer.writerows(rows)
        self.run_cli(extra=['--end',next_day])
        audit=json.loads((self.root/'out/_audit/days/XAUUSDT/20260201.json').read_text())
        self.assertIsNotNone(audit['trades']['threshold'])
        first=pq.read_table(self.root/'out/baseline_data_1min/binance-futures/XAUUSDT/20260131.parquet').to_pandas()
        second=pq.read_table(self.root/'out/baseline_data_1min/binance-futures/XAUUSDT/20260201.parquet').to_pandas()
        self.assertEqual(first[['large_order','medium_order','small_order']].to_numpy().sum(),0)
        np.testing.assert_allclose(second.large_order+second.medium_order+second.small_order,second.amount)
        self.assertTrue(np.isfinite(second.twap.iloc[0]))

    def test_future_closing_book_cannot_change_already_usable_factors(self):
        self.run_cli();self.run_cli('factors')
        changed=self.root/'changed';changed.mkdir()
        for n in [1,5,15]:
            src=self.root/f'out/baseline_data_{n}min'
            shutil.copytree(src,changed/src.name)
            path=changed/f'baseline_data_{n}min/binance-futures/XAUUSDT/20260131.parquet'
            frame=pq.read_table(path).to_pandas();frame.loc[frame.ts>=self.start+1800,'book_depth']=np.uint32(0)
            pq.write_table(pa.Table.from_pandas(frame,preserve_index=False),path)
        self.run_cli('factors',out=changed)
        for n in [1,5,15]:
            rel=f'baseline_factor_{n}min/binance-futures/XAUUSDT/20260131.parquet'
            before=pq.read_table(self.root/'out'/rel).to_pandas()
            after=pq.read_table(changed/rel).to_pandas()
            mask=before.ts<=self.start+1800
            np.testing.assert_allclose(before.loc[mask].iloc[:,2:],after.loc[mask].iloc[:,2:],equal_nan=True)

    def test_bad_identity_refuses_publication(self):
        self.write('trades',[['binance-futures','WRONG',self.start*10**6,self.start*10**6,1,'buy',100,1]])
        self.run_cli(expect=1)
        self.assertFalse((self.root/'out/baseline_data_1min').exists())

    def test_gap_after_listing_is_an_error(self):
        self.run_cli(expect=1,extra=['--end','2026-02-01'])
        audit=json.loads((self.root/'out/_audit/baseline_run.json').read_text())
        self.assertEqual(len(audit['failures']),1)
        self.assertFalse((self.root/'out/_audit/baseline_XAUUSDT.json').exists())


if __name__=='__main__':unittest.main()
