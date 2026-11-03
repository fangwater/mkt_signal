"""Retire unused legacy CTA catalog definitions. Never publishes targets or submits orders."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

H=Path.home();M=H/'crypto_cta_manager'
sys.path.insert(0,str(M/'maintenance-backups/rx03_rx04_taker_clear_20261102T144523Z/scripts'))
from manager_ops import session,sql
from manager_entries import protected_processes

OWN={f'cta-{group}-bn-{period}-pos' for group in ['prep01','prep02'] for period in ['1min','5min','15min']}|{'cta-spread01-okx-bn-1min-pos'}
PRC={'CTA_TOP_V1','CTA_TOP_V2','funding','rbf_big','rbf_mid_1min','rbf_small'}
PREFIXES={'binance_cta_perp_rx01_':91,'binance_cta_perp_rx03_':28,'binance_cta_perp_rx04_':35,'binance_cta_spread_rx01_':65,'lgq_cta_':10}
OLD_ENVS=['binance-cta-perp-rx01','binance-cta-perp-rx02','binance-cta-perp-rx03','binance-cta-perp-rx04','binance-cta-spread-rx01','okex-binance-cta-spread-rx01']


def require(ok,message):
    if not ok:raise RuntimeError(message)


def save(path,data):
    temporary=path.with_name(path.name+'.next')
    with temporary.open('w') as f:
        os.fchmod(f.fileno(),0o600);json.dump(data,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
    os.replace(temporary,path)


def rows(table):
    require(table in ['cta_position_strategies','cta_account_strategy_bindings','cta_virtual_account_bindings','cta_account_follows','cta_follow_publish_queue','cta_position_strategy_grants','cta_position_strategy_managers'],'unexpected table')
    return json.loads(sql('SELECT coalesce(jsonb_agg(to_jsonb(t)),\'[]\'::jsonb) FROM '+table+' t'))


def keyed(items,key):return {v[key]:v for v in items}

def canonical(data):
    if isinstance(data,list):return sorted([canonical(v) for v in data],key=lambda v:json.dumps(v,sort_keys=True))
    if isinstance(data,dict):return {k:canonical(v) for k,v in data.items()}
    return data


def retained_config(catalog):
    return {name:{k:v for k,v in catalog[name].items() if k not in ['targets','updated_at_us']} for name in OWN|PRC}


def runtime_config():
    result={}
    for name,directory in [('cta-prep-binance-futures','cta_prep_binance_futures'),('cta-factor-spread','cta_factor_spread')]:
        root=H/name
        for path in [root/'env.sh',root/'config'/(directory+'.toml'),*(root/'config'/directory).glob('*.json')]:result[str(path)]=hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (root/'config'/directory).glob('*.json'):
            require(json.loads(path.read_text())['signal']['enabled'] is False,'position publishing was enabled')
    return result


def require_retired():
    for proc in Path('/proc').glob('[0-9]*'):
        try:
            cwd=(proc/'cwd').resolve()
        except OSError:continue
        require(cwd.name not in OLD_ENVS,'retired environment has a running process')


def safe_state(legacy):
    bindings=rows('cta_account_strategy_bindings')
    require(not any(r['position_strategy_name'] in legacy|OWN for r in bindings),'legacy or new CTA account binding exists')
    require(not rows('cta_account_follows'),'account follows changed; review before removing virtual templates')
    require(not rows('cta_follow_publish_queue'),'pending publish queue is not empty')
    require_retired()
    return bindings


def verify(audit):
    before=json.loads((audit/'private-before.json').read_text());plan=json.loads((audit/'plan.json').read_text())
    current=keyed(rows('cta_position_strategies'),'strategy_name');require(set(current)==OWN|PRC,'catalog does not contain exactly thirteen retained strategies')
    require(retained_config(current)==before['retained_config'],'retained strategy configuration changed')
    require(canonical(rows('cta_account_strategy_bindings'))==canonical(before['account_bindings']),'real account bindings changed')
    require(canonical(rows('cta_virtual_account_bindings'))==canonical([r for r in before['virtual_bindings'] if r['virtual_id'] not in plan['virtuals']]),'retained virtual template changed')
    for table in ['cta_position_strategy_grants','cta_position_strategy_managers']:
        require(canonical(rows(table))==canonical([r for r in before[table] if r['strategy_name'] in OWN|PRC]),'retained strategy access changed')
    safe_state(set(plan['remove']))
    require(runtime_config()==before['runtime_config'],'factor or environment configuration changed')
    now=protected_processes();require(all(now.get(pid)==r for pid,r in before['processes'].items()),'protected process changed')
    with session('prc') as api:
        require({r['strategy_name'] for r in api('/manager/api/catalog/position-strategies')}==OWN|PRC,'Manager API catalog differs')
        virtuals=api('/manager/api/catalog/virtual-accounts');require(not {v['virtual_id'] for v in virtuals}&set(plan['virtuals']),'legacy virtual template remains')
    result={'total_position_strategies':13,'own_position_strategies':sorted(OWN),'prc_position_strategies':sorted(PRC),'removed_legacy_strategies':len(plan['remove']),'removed_unused_virtual_templates':plan['virtuals'],'removed_legacy_virtual_bindings':sum(r['virtual_id'] in plan['virtuals'] for r in before['virtual_bindings']),'real_account_bindings_unchanged':len(before['account_bindings']),'retained_strategy_permissions_and_tokens_unchanged':True,'all_seven_publishing_disabled':True,'own_execution_bindings':0,'runtime_configs_unchanged':True,'protected_processes_unchanged':len(before['processes']),'orders_or_target_updates_sent':0,'checked_at_ms':time.time_ns()//1000000}
    save(audit/'verified.json',result);print(json.dumps(result),flush=True)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('phase',choices=['prepare','execute','verify']);p.add_argument('--audit',type=Path,required=True);p.add_argument('--execute',action='store_true');a=p.parse_args();audit=a.audit
    os.umask(0o077);require(audit.parent==M/'maintenance-backups' and audit.name.startswith('cta_catalog_cleanup_'),'unexpected audit path')
    if a.phase=='verify':verify(audit);return
    if a.phase=='prepare':
        require(not (audit/'plan.json').exists(),'plan already prepared')
        current=keyed(rows('cta_position_strategies'),'strategy_name');legacy={name for name in current if any(name.startswith(prefix) for prefix in PREFIXES)}
        require(set(current)==legacy|OWN|PRC,'unexpected catalog strategy; do not remove it')
        require(all(sum(name.startswith(prefix) for name in legacy)==count for prefix,count in PREFIXES.items()),'legacy catalog changed')
        require(all(all(v['qty']==0 for v in current[name]['targets'].values()) for name in OWN),'new CTA strategies have nonzero targets')
        bindings=safe_state(legacy);virtual_bindings=rows('cta_virtual_account_bindings')
        with session('prc') as api:virtuals=api('/manager/api/catalog/virtual-accounts')
        remove_virtuals=[]
        for v in virtuals:
            references={r['position_strategy_name'] for r in v['bindings']}
            if references & legacy:
                require(references<=legacy and not v['followers'],'legacy template has retained strategies or followers')
                remove_virtuals.append(v['virtual_id'])
        require(set(remove_virtuals)=={'virtual02','virtual03'},'unexpected virtual retirement scope')
        before={'catalog':current,'retained_config':retained_config(current),'account_bindings':bindings,'virtual_bindings':virtual_bindings,'virtual_accounts':virtuals,'runtime_config':runtime_config(),'processes':protected_processes()}
        for table in ['cta_position_strategy_grants','cta_position_strategy_managers']:before[table]=rows(table)
        save(audit/'private-before.json',before)
        plan={'remove':sorted(legacy),'virtuals':sorted(remove_virtuals),'retain':sorted(OWN|PRC),'prefix_counts':PREFIXES}
        save(audit/'plan.json',plan);print(json.dumps({'prepared':True,'remove_legacy':len(legacy),'remove_unused_virtuals':sorted(remove_virtuals),'retain':sorted(OWN|PRC),'orders_or_targets_to_publish':0}),flush=True);return
    require(a.execute,'mutation requires --execute')
    with (audit/'cleanup.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        require(not (audit/'verified.json').exists(),'cleanup already complete; use verify')
        plan=json.loads((audit/'plan.json').read_text());before=json.loads((audit/'private-before.json').read_text());legacy=set(plan['remove'])
        safe_state(legacy);require(runtime_config()==before['runtime_config'],'runtime configuration changed')
        current=keyed(rows('cta_position_strategies'),'strategy_name')
        require(set(current)==legacy|OWN|PRC,'catalog changed after preparation')
        require(all(current[name]==before['catalog'][name] for name in legacy),'a legacy publisher or operator changed target definitions')
        require(retained_config(current)==before['retained_config'],'retained configuration changed')
        receipts=[]
        def record(kind,name):
            receipts.append({'kind':kind,'name':name,'at_ms':time.time_ns()//1000000});save(audit/'receipts.json',receipts)
        with session('prc') as api:
            virtuals={v['virtual_id']:v for v in api('/manager/api/catalog/virtual-accounts')}
            for vid in plan['virtuals']:
                require(not virtuals[vid]['followers'] and all(b['position_strategy_name'] in legacy for b in virtuals[vid]['bindings']),'virtual retirement scope changed')
                api('/manager/api/catalog/virtual-accounts/'+vid,None,'DELETE');record('virtual',vid)
            require(not any(r['position_strategy_name'] in legacy for r in rows('cta_virtual_account_bindings')),'legacy virtual reference remains')
            for i,name in enumerate(plan['remove'],1):
                # These canonical strategy names contain only letters, digits and underscores.
                require(all(c.isalnum() or c=='_' for c in name),'noncanonical legacy name')
                value=json.loads(sql("SELECT to_jsonb(p) FROM cta_position_strategies p WHERE strategy_name='"+name+"'"))
                require(value==before['catalog'][name],'legacy strategy changed before delete')
                api('/manager/api/catalog/position-strategies/'+name,None,'DELETE');record('strategy',name)
                if i%50==0:print(json.dumps({'removed_legacy':i,'planned':len(legacy)}),flush=True)
    verify(audit)


if __name__=='__main__':main()
