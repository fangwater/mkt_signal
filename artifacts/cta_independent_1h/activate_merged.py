"""Activate confirmed 1h allocations in the merged Prep runtime; default is read-only."""
import argparse
import json
from pathlib import Path
import sys
import time
import tomllib
import urllib.request
sys.path.insert(0,'/home/ubuntu/crypto_cta_manager/maintenance-backups/cta_symbol_groups_20261104T093943Z')
import ops
MAIN=Path('/home/ubuntu/cta-prep-binance-futures')
AUDIT=Path('/home/ubuntu/cta-hourly-history/merged-deployment')


def execution_ready(source):
    config=tomllib.loads((ops.M/'config/cta-manager.toml').read_text())
    item=next(s for s in config['sources'] if s['id']==source)
    url=item['exec_viz_url'].rstrip('/')+'/snapshot'
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(url,timeout=20) as response:value=json.load(response)
    entry=next(e['entry'] for e in value['entries'] if e['channel']=='exec_pre_trade_state')
    ops.require(entry['position_ready'] and 0<=time.time_ns()//1000000-entry['ts_ms']<20000,'Exec snapshot unavailable: '+source)
    return entry


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--execute',action='store_true')
    parser.add_argument('--resume-before-create',action='store_true',help='Resume only a failed attempt with no created strategies, bindings or receipts')
    args=parser.parse_args()
    state=ops.state();ops.require(state['generation'] and not state['warming'] and not state['error'] and not state['positions_error'],'merged Prep still replaying or unhealthy')
    hourly={k:v for k,v in state['plan']['calculations'].items() if v['id']['interval']=='1h'}
    ops.require(len(hourly)==265 and not(set(hourly)-set(state['latest'])),'hourly calculations missing')
    unready=[k for k in hourly if not all(f['ready'] for f in state['latest'][k]['fields'].values())]
    ops.require(set(unready)<={'binance-futures-dashusdt-1h-depth_imb_bps1'},'unexpected unready hourly formula')
    docs={n:d for n,d in state['plan']['combinations'].items() if d['interval']=='1h'}
    ops.require(len(docs)==4 and sum(len(d['factor_rules']) for d in docs.values())==326,'hourly rule scope changed')
    for d in docs.values():
        ops.require(not d['signal']['enabled'],'hourly rule already enabled')
        ops.require(d['signal']['max_pos_u']==(50 if d['strategy_group']=='prep01' else 100),'confirmed amount differs')
    print(json.dumps({'ready':True,'hourly_calculations':len(hourly),'ready_hourly_calculations':len(hourly)-len(unready),'unready':unready,'execute':args.execute}),flush=True)
    if not args.execute:return
    started=AUDIT/'activation-started.json'
    if args.resume_before_create:
        ops.require(started.exists() and not (AUDIT/'activation-receipts.json').exists() and not (AUDIT/'activated.json').exists(),'resume requires a verified failure before the first mutation')
    else:
        ops.require(not started.exists(),'inspect previous activation attempt before resuming')
    before=ops.read(AUDIT/'before.json')
    with ops.session('lgq') as api:
        accounts={source:api('/manager/api/catalog/accounts/'+source) for source in before['accounts']}
        for source,account in accounts.items():
            ops.require(account['bindings']==before['accounts'][source]['bindings'],'existing bindings changed: '+source)
            execution_ready(source)
        catalog={p['strategy_name']:p for p in api('/manager/api/catalog/position-strategies')}
        destinations={};mapping={}
        for d in docs.values():
            pool=d.get('symbol_group');prefix=('cta-'+pool if pool else 'cta')+'-'+d['strategy_group']+'-bn-'
            strategy=prefix+'1h-pos';old=prefix+'15min-pos';source='binance_exec_rx01' if d['strategy_group']=='prep01' else 'binance_exec_rx03'
            ops.require(strategy not in catalog,'hourly catalog definition already exists: '+strategy)
            destinations[strategy]=source;mapping[old]=[strategy]
        if args.resume_before_create:
            previous=ops.read(started)
            ops.require(previous['destinations']==destinations and previous['mapping']==mapping,'activation scope changed')
        else:
            ops.save(started,{'at_ms':time.time_ns()//1000000,'destinations':destinations,'mapping':mapping,'accounts':accounts})
        # The legacy session helper writes PostgreSQL directly. The new publish
        # handler reads an in-memory session cache, refreshed on catalog edits.
        # Confirming an already-absent new binding returns 404 without changing
        # configuration or targets and schedules that refresh; retain this same
        # session for creation. Never apply this to an existing binding.
        first=next(iter(destinations));source=destinations[first]
        ops.require(all(b['binding_name']!=first for b in accounts[source]['bindings']),'new binding unexpectedly exists')
        try:
            api('/manager/api/catalog/accounts/'+source+'/bindings/'+first,method='DELETE')
        except RuntimeError as error:
            ops.require('HTTP 404:' in str(error) and 'binding was not found' in str(error),'session cache refresh failed')
        else:
            raise RuntimeError('expected absent-binding confirmation; inspect concurrent edits')
        time.sleep(2)
        receipts=[]
        for strategy in destinations:
            result=api('/manager/api/catalog/position-strategies',{'strategy_name':strategy,'targets':{}})
            receipts.append({'action':'create:'+strategy,'result':result});ops.save(AUDIT/'activation-receipts.json',receipts)
    ops.clone_access(mapping)
    with ops.session('lgq') as api:
        for strategy,source in destinations.items():
            result=api('/manager/api/catalog/accounts/'+source+'/bindings',{'binding_name':strategy,'position_strategy_name':strategy,'order_strategy_name':'lgq_grid_prep','shares':1})
            receipts.append({'action':'bind:'+strategy,'result':result});ops.save(AUDIT/'activation-receipts.json',receipts)
    for name,d in docs.items():
        current=ops.local_api('/api/config?worker=cta-prep-binance-futures')
        ops.require(current['plan']['combinations'][name]==d,'hourly combination changed: '+name)
        enabled=dict(d);enabled['signal']=dict(d['signal']);enabled['signal']['enabled']=True
        result=ops.local_api('/api/combinations',{'worker':'cta-prep-binance-futures','expected_revision':current['revision'],'combination':enabled})
        receipts.append({'action':'enable:'+name,'revision':result['revision']});ops.save(AUDIT/'activation-receipts.json',receipts)
    ops.save(AUDIT/'activated.json',{'at_ms':time.time_ns()//1000000,'destinations':destinations,'grid':True,'shares':1,'amounts':{'prep01':50,'prep02':100},'unready':unready})
    print(json.dumps({'activated':True,'destinations':destinations,'amounts':{'prep01':50,'prep02':100},'grid':True,'shares':1}),flush=True)


if __name__=='__main__':main()
