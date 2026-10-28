"""Read every baseline column and validate frozen keys, hashes and invariants."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq


def sha(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        while b:=f.read(1024*1024):h.update(b)
    return h.hexdigest()


def check(condition,message):
    if not condition:raise ValueError(message)


def validate_day(item):
    root,ric,day=item;date=day['day'].replace('-','')
    file=root/'NASDAQ'/ric/f'{date}.parquet'
    audit=json.loads((root/'_audit/days/NASDAQ'/ric/f'{date}.json').read_text())
    check(audit['input']==day and audit['ric']==ric,'input identity')
    check(audit['gzip_read_to_eof_crc_verified'] is True,'source completeness')
    check(audit['output_sha256']==sha(file),'output hash')
    cache=root/'_work/notionals/NASDAQ'/ric/f'{date}.i128.zst'
    check(audit['notional_cache_sha256']==sha(cache),'notional cache hash')
    table=pq.read_table(file,use_threads=False)
    check(table.num_columns==94,'column count')
    data={k:table[k].combine_chunks().to_numpy(zero_copy_only=False) for k in table.column_names}
    n=table.num_rows
    check(np.array_equal(data['ts'],np.arange(day['open_ts'],day['close_ts'],60)),'minute grid')
    check(np.all(data['ric']==ric) and np.all(data['venue']=='NASDAQ'),'output identity')
    check(np.all(data['n_seconds']==60),'bar duration')
    for name in ['volume','amount','count']:
        check(np.allclose(data[name],sum(data[p+name] for p in ['buy_','sell_','unknown_']),rtol=1e-13,atol=1e-7),'trade conservation '+name)
        for p in ['','buy_','sell_','unknown_','off_exchange_']:
            check(np.all(np.isfinite(data[p+name]) & (data[p+name]>=0)),'invalid trade field '+p+name)
        check(np.all(data['off_exchange_'+name]==0),'venue trade misclassified as off exchange')
    check(int(data['count'].sum())==audit['trades'],'trade census')
    for field in ['open','high','low','close']:
        check(np.array_equal(np.isnan(data[field]),data['count']==0),'empty minute OHLC '+field)
    traded=data['count']>0
    check(np.all(data['low'][traded]<=data['open'][traded]) and np.all(data['open'][traded]<=data['high'][traded]),'OHLC range')
    check(np.all(data['low'][traded]<=data['close'][traded]) and np.all(data['close'][traded]<=data['high'][traded]),'close range')
    depth=data['book_depth'];check(np.all((depth>=0)&(depth<=10)),'depth range')
    quality=data['source_quality_flags'];check(np.all(np.isin(quality,[0,4,8,12])),'unexpected source quality')
    check(np.all(depth[quality!=0]==0),'untrusted book published as valid')
    quality_minutes=int(np.sum(quality!=0))
    check(quality_minutes==audit['source_quality_affected_minutes'],'source quality census')
    uncertain_twap=audit.get('twap_null_due_to_bad_timestamp_minutes',[])
    for ts in uncertain_twap:
        index=(ts-day['open_ts'])//60
        check(0<=index<n and quality[index]&8 and np.isnan(data['twap'][index]),'unreliable TWAP published')
    valid=depth>0;stamp=data['source_depth_ts_utc_ns']
    check(np.array_equal(np.isnan(stamp),~valid),'depth witness nulls')
    # Integer comparison avoids rounding a nanosecond timestamp at minute edges.
    for ts,t in zip(data['ts'][valid],table['source_depth_ts_utc_ns'].filter(table['book_depth'].combine_chunks().cast('int64').to_numpy()>0).to_pylist()):
        check(int(ts)*10**9<=t<(int(ts)+60)*10**9,'depth witness outside minute')
    actual=[]
    for side in ['bid','ask']:
        present=[]
        for level in range(10):
            price=data[f'l2_{side}{level}p'];size=data[f'l2_{side}{level}v'];mask=~np.isnan(price)
            check(np.array_equal(mask,~np.isnan(size)),'partial price/size')
            check(np.all(price[mask]>0)&np.all(size[mask]>0),'nonpositive depth')
            if level:
                prior=data[f'l2_{side}{level-1}p']
                check(np.all(~mask|~np.isnan(prior)),'hole in price levels')
                check(np.all(price[mask]<prior[mask]) if side=='bid' else np.all(price[mask]>prior[mask]),'unsorted levels')
            present.append(mask)
        actual.append(sum(present))
        for pv in ['p','v']:
            check(np.allclose(data[f'{side}0{pv}'],data[f'l2_{side}0{pv}'],equal_nan=True),'BBO/depth disagreement')
    check(np.array_equal(depth,np.minimum(*actual)),'actual depth count')
    check(np.all((actual[0]>0)==valid) and np.all((actual[1]>0)==valid),'one sided book')
    check(np.all(data['bid0p'][valid]<=data['ask0p'][valid]),'crossed book')
    check(np.allclose(data['midp'],(data['bid0p']+data['ask0p'])/2,rtol=1e-14,equal_nan=True),'midpoint')
    check(np.allclose(data['midp'],data['mid_price'],equal_nan=True),'midpoint aliases')
    check(np.allclose(data['net_buy_amount'],data['buy_amount']-data['sell_amount'],rtol=1e-12,atol=1e-7),'net notional')
    if audit['threshold_previous_month_sample_trades']:
        check(np.allclose(data['amount'],sum(data[s+'_order'] for s in ['large','medium','small']),rtol=1e-13,atol=1e-7),'bucket conservation')
    else:
        check(all(np.all(data[s+'_'+t]==0) for s in ['large','medium','small'] for t in ['order','buy','sell']),'first month bucket warmup')
    ten=int(np.sum(depth==10));missing=int(np.sum(depth==0));partial=n-ten-missing
    check(ten==audit['complete_ten_minutes'] and missing==len(audit['missing_depth_minutes']) and partial==len(audit['partial_depth_minutes']),'depth audit census')
    return dict(ric=ric,day=day['day'],rows=n,ten=ten,missing=missing,partial=partial,quality_minutes=quality_minutes,
                source_bytes=audit['source_bytes'],source_rows=audit['rows'],trades=audit['trades'],build_id=audit['build_id'])


def validate(root,jobs_file,workers=8,accepted_build_ids=None):
    root=Path(root);jobs_file=Path(jobs_file)
    jobs=json.loads(jobs_file.read_text());items=[(root,j['ric'],d) for j in jobs for d in j['days']]
    check(len({(r,d['day']) for _,r,d in items})==len(items),'duplicate job key')
    expected={f'NASDAQ/{r}/{d["day"].replace("-","")}.parquet' for _,r,d in items}
    actual={str(p.relative_to(root)) for p in root.glob('NASDAQ/*/*.parquet')}
    check(actual==expected,'output file set mismatch')
    stats={};builds=set();count=0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for r in pool.map(validate_day,items):
            builds.add(r.pop('build_id'));ric=r.pop('ric');day=r.pop('day')
            t=stats.setdefault(ric,dict(files=0,rows=0,ten=0,missing=0,partial=0,quality_minutes=0,source_bytes=0,source_rows=0,trades=0,first=day,last=day,
                                      quality_affected_days=[],non_ten_depth_days=[]))
            t['files']+=1;t['last']=day
            if r['quality_minutes']:t['quality_affected_days'].append(day)
            if r['ten']!=r['rows']:t['non_ten_depth_days'].append(day)
            for k,v in r.items():t[k]+=v
            count+=1
            if count%1000==0:print(f'validated={count}/{len(items)}',flush=True)
    if accepted_build_ids is None:check(len(builds)==1,'mixed binary versions')
    else:check(builds<=set(accepted_build_ids),'unapproved binary version')
    for job in jobs:
        t=stats[job['ric']];days=[d['day'] for d in job['days']]
        last_bad=t['non_ten_depth_days'][-1] if t['non_ten_depth_days'] else None
        index=days.index(last_bad)+1 if last_bad else 0
        t['full_ten_continuous_tail_start']=days[index] if index<len(days) else None
        t['full_ten_continuous_tail_days']=len(days)-index
    report=dict(files=len(items),columns=94,jobs_sha256=sha(jobs_file),build_id=next(iter(builds)) if len(builds)==1 else 'explicitly compatible builds',build_ids=sorted(builds),
                all_columns_read=True,output_and_cache_hashes_verified=True,minute_keys_verified=True,
                invariants_verified=True,census=stats)
    report['totals']={k:sum(t[k] for t in stats.values()) for k in ['files','rows','ten','missing','partial','quality_minutes','source_bytes','source_rows','trades']}
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',type=Path,required=True);p.add_argument('--jobs',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--workers',type=int,default=8)
    p.add_argument('--accepted-build-id',action='append')
    a=p.parse_args();report=validate(a.root,a.jobs,a.workers,a.accepted_build_id)
    a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report['totals']))


if __name__=='__main__':main()
