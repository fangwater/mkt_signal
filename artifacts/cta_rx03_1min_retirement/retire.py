"""Retire only RX03's three Prep02 1min pools; default is inspection."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
import time
import urllib.request

sys.path.insert(0, '/home/ubuntu/crypto_cta_manager/maintenance-backups/cta_symbol_groups_20261104T093943Z')
import ops

NAMES = {f'cta-{pool}-prep02-bn-1min-pos' for pool in ('top-5', 'top-20', 'last-20')}
WORKER = 'cta-prep-binance-futures'


def request(path, body=None, method=None):
    req = urllib.request.Request('http://127.0.0.1:19220' + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={'Content-Type': 'application/json'}, method=method)
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req, timeout=30) as response:
        raw=response.read()
        return json.loads(raw) if raw else {'status':response.status}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute',action='store_true')
    args=parser.parse_args()
    catalog=ops.local_api('/api/config?worker='+WORKER)
    docs=catalog['plan']['combinations']
    removed={n:d for n,d in docs.items() if d.get('strategy_group')=='prep02' and d['interval']=='1min'}
    ops.require(len(removed)==3,'expected exactly three active Prep02 1min pool configs')
    ops.require({d['symbol_group'] for d in removed.values()}=={'top-5','top-20','last-20'},'unexpected pools')
    kept={n:d for n,d in docs.items() if n not in removed}
    with ops.session('lgq') as api:
        account=api('/manager/api/catalog/accounts/'+ops.SOURCE)
    bindings={b['position_strategy_name']:b for b in account['bindings']}
    ops.require(account['configuration']['mode']=='independent','source ownership changed')
    ops.require(NAMES<=bindings.keys() and all(bindings[n]['binding_name']==n for n in NAMES),'binding identities changed')
    _,execution=ops.snapshot()
    ops.require(execution['position_ready'],'Exec position state not ready')
    rows=[r for r in execution['rows'] if r['strategy_name'] in NAMES]
    summary={'source':ops.SOURCE,'market':'binance-futures','strategies':sorted(NAMES),
        'removed_rules':sum(len(d['factor_rules']) for d in removed.values()),
        'symbols':sorted({r['标的'] for d in removed.values() for r in d['factor_rules']}),
        'current_abs_usdt':sum(abs(float(r.get('current_usdt',0))) for r in rows),
        'retained_combinations':len(kept),'execute':args.execute}
    print(json.dumps(summary,ensure_ascii=False),flush=True)
    if not args.execute:return
    audit=ops.M/'maintenance-backups'/('cta_rx03_1min_retire_'+time.strftime('%Y%m%dT%H%M%SZ',time.gmtime()))
    audit.mkdir(mode=0o700)
    before={'catalog':catalog,'account':account,'execution':execution,'ledgers':ops.ledgers(ops.client())}
    before['config_sha256']={str(p):ops.sha(p) for p in (ops.PREP/'config/cta_prep_binance_futures').glob('*.json')}
    ops.save(audit/'before.json',before)
    for p in (ops.PREP/'data/cta_prep_binance_futures/positions').glob('*prep02*1min*.json'):
        ops.save(audit/'signal-ledgers'/p.name,ops.read(p))
    receipts=[]
    def record(action,result):
        receipts.append({'action':action,'at_ms':time.time_ns()//1_000_000,'result':result})
        ops.save(audit/'receipts.json',receipts)
    # shares=0 clears through each original strategy, then blocks all subsequent entry publishes.
    with ops.session('lgq') as api:
        for name in sorted(NAMES):
            result=api('/manager/api/catalog/accounts/'+ops.SOURCE+'/bindings/'+name+'/shares',{'shares':0},'PUT')
            record('stop:'+name,result)
    for name in sorted(removed):
        current=ops.local_api('/api/config?worker='+WORKER)
        ops.require(current['plan']['combinations'].get(name)==removed[name],'minute configuration changed during retirement')
        result=request('/api/combinations/'+name,{'worker':WORKER,'expected_revision':current['revision']},'DELETE')
        record('remove_combination:'+name,result)
    current=ops.local_api('/api/config?worker='+WORKER)
    ops.require(current['plan']['combinations']==kept,'unrelated combination changed')
    summary['audit']=str(audit)
    ops.save(audit/'retirement_started.json',summary)
    print(json.dumps({'audit':str(audit),'minute_shares':0,'minute_configurations':0,'other_combinations_unchanged':True}),flush=True)


if __name__=='__main__':main()
