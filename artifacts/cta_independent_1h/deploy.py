"""JP hourly Prep deployment; default is a read-only preflight."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

sys.path.insert(0,'/home/ubuntu/crypto_cta_manager/maintenance-backups/cta_symbol_groups_20261104T093943Z')
import ops
HOME=Path('/home/ubuntu');MAIN=HOME/'cta-prep-binance-futures';HOURLY=HOME/'cta-prep-binance-futures-1h'
UPLOAD=HOME/'cta-hourly-history/independent-upload';AUDIT=HOME/'cta-hourly-history/independent-deployment'


def sha(p):
    with p.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def checked(command,cwd,log):
    result=subprocess.run(command,cwd=cwd,capture_output=True,timeout=90)
    (AUDIT/(log+'.stdout')).write_bytes(result.stdout);(AUDIT/(log+'.stderr')).write_bytes(result.stderr)
    ops.require(result.returncode==0,'operation failed: '+log+'; inspect private audit')
    return result


def identity(root):
    rows=[]
    for p in Path('/proc').glob('[0-9]*'):
        try:
            if (p/'cwd').resolve()!=root:continue
            comm=(p/'comm').read_text().strip()
            if comm not in ['cta-prep-binanc','cta-factor-spre','cta-factor-conf']:continue
            rows.append({'pid':int(p.name),'start':(p/'stat').read_text().split()[21],'comm':comm})
        except OSError:pass
    return rows


def prepare():
    ops.require(not HOURLY.exists(),'hourly directory already exists; inspect existing deployment')
    ops.require(not AUDIT.exists(),'deployment audit already exists')
    AUDIT.mkdir(mode=0o700)
    hashes=ops.read(UPLOAD/'sha256.json')
    for name,digest in hashes.items():ops.require(sha(UPLOAD/name)==digest,'upload checksum mismatch: '+name)
    catalog=ops.local_api('/api/config?worker=cta-prep-binance-futures')
    ops.require(not any(c['interval']=='1h' for c in catalog['plan']['combinations'].values()),'main runtime already has hourly definitions')
    before={'main_processes':identity(MAIN),'spread_processes':identity(ops.SPREAD),'main_state':ops.state(),
        'catalog':catalog,'protected_files':{},'uploaded_sha256':hashes}
    for p in [MAIN/'env.sh',MAIN/'scripts/cta_factor_process.sh',MAIN/'cta-factor-config']:
        target=AUDIT/'original'/p.relative_to(HOME);target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,target)
    for p in [MAIN/'cta-prep-binance-futures',MAIN/'config/cta_prep_binance_futures.toml',MAIN/'env.sh',ops.SPREAD/'cta-factor-spread',ops.SPREAD/'env.sh']:
        before['protected_files'][str(p)]=sha(p)
    with ops.session('lgq') as api:
        before['accounts']={source:api('/manager/api/catalog/accounts/'+source) for source in ['binance_exec_rx01','binance_exec_rx03']}
    ops.save(AUDIT/'before.json',before)
    (HOURLY/'config/cta_prep_binance_futures_1h').mkdir(parents=True)
    (HOURLY/'scripts').mkdir();(HOURLY/'data').mkdir()
    shutil.copy2(UPLOAD/'cta-prep-binance-futures',HOURLY/'cta-prep-binance-futures')
    for name in ['cta_factor_process.sh','start_cta_prep_binance_futures.sh','stop_cta_prep_binance_futures.sh','process_match_lib.sh']:
        shutil.copy2(UPLOAD/name,HOURLY/'scripts'/name)
    shutil.copy2(MAIN/'config/iceoryx2.toml',HOURLY/'config/iceoryx2.toml') if (MAIN/'config/iceoryx2.toml').exists() else shutil.copy2(UPLOAD/'iceoryx2.toml',HOURLY/'config/iceoryx2.toml')
    shutil.copy2(HOURLY/'config/iceoryx2.toml',HOURLY/'iceoryx2.toml')
    shutil.copy2(UPLOAD/'cta_prep_binance_futures_1h.toml',HOURLY/'config/cta_prep_binance_futures.toml')
    for p in (UPLOAD/'combinations').glob('*.json'):
        d=ops.read(p);ops.require(d['interval']=='1h' and not d['signal']['enabled'],'staged table must be hourly and disabled')
        shutil.copy2(p,HOURLY/'config/cta_prep_binance_futures_1h'/p.name)
    (HOURLY/'config/cta_prep_binance_futures_1h/symbol_groups.json').symlink_to(MAIN/'config/cta_prep_binance_futures/symbol_groups.json')
    env='\n'.join(['#!/usr/bin/env bash',f'source "{MAIN}/env.sh"',
        'export CTA_PREP_BINANCE_FUTURES_CORE=19','export CTA_FACTOR_PROCESS_NAME=cta-prep-binance-futures-1h',
        'export CTA_FACTOR_OUTPUT_SERVICE=factor_output/cta-prep-binance-futures-1h',
        'export CTA_HOURLY_HISTORY_DIR=/home/ubuntu/cta-hourly-history/current/data',''])
    (HOURLY/'env.sh').write_text(env);(HOURLY/'env.sh').chmod(0o600)
    checked([str(HOURLY/'cta-prep-binance-futures'),'--config','config/cta_prep_binance_futures.toml','--output-service','factor_output/cta-prep-binance-futures-1h','--check'],HOURLY,'hourly-check')
    checked([str(UPLOAD/'cta-factor-config'),'--config',str(MAIN/'config/cta_prep_binance_futures.toml'),'--spread-config',str(ops.SPREAD/'config/cta_factor_spread.toml'),'--hourly-config',str(HOURLY/'config/cta_prep_binance_futures.toml'),'--check'],MAIN,'shared-config-check')
    checked([str(HOURLY/'scripts/start_cta_prep_binance_futures.sh')],HOURLY,'hourly-dry-run')
    ops.save(AUDIT/'prepared.json',{'at_ms':time.time_ns()//1000000,'hourly_env':str(HOURLY),'binary_sha256':sha(HOURLY/'cta-prep-binance-futures')})
    print(json.dumps({'prepared':True,'env':str(HOURLY),'hourly_tables':4,'rules':326,'enabled':False,'core':19}),flush=True)


def start():
    ops.require((AUDIT/'prepared.json').exists(),'prepare first')
    checked([str(HOURLY/'scripts/start_cta_prep_binance_futures.sh'),'--execute'],HOURLY,'hourly-start')
    print(json.dumps({'started':True,'env':str(HOURLY),'trading_enabled':False}),flush=True)


def config_service():
    before=ops.read(AUDIT/'before.json')
    ops.require(sha(MAIN/'scripts/cta_factor_process.sh')==sha(AUDIT/'original/cta-prep-binance-futures/scripts/cta_factor_process.sh'),'operator lifecycle script changed')
    for name,source in [('cta-factor-config',UPLOAD/'cta-factor-config'),('scripts/cta_factor_process.sh',UPLOAD/'cta_factor_process.sh')]:
        destination=MAIN/name;temp=destination.with_name('.'+destination.name+'.hourly.next')
        shutil.copy2(source,temp);os.replace(temp,destination)
    env=MAIN/'env.sh';text=env.read_text();ops.require('CTA_PREP_HOURLY_CONFIG' not in text,'hourly catalog already configured')
    text+='\nexport CTA_PREP_HOURLY_CONFIG="'+str(HOURLY/'config/cta_prep_binance_futures.toml')+'"\n'
    temp=env.with_name('.env.hourly.next');temp.write_text(text);temp.chmod(env.stat().st_mode & 0o777);os.replace(temp,env)
    checked([str(MAIN/'scripts/start_cta_factor_config.sh'),'--execute'],MAIN,'shared-config-start')
    current=ops.local_api('/api/config?worker=cta-prep-binance-futures')
    hours={n:d for n,d in current['plan']['combinations'].items() if d['interval']=='1h'}
    kept={n:d for n,d in current['plan']['combinations'].items() if d['interval']!='1h'}
    ops.require(kept==before['catalog']['plan']['combinations'] and len(hours)==4,'shared catalog differs')
    ops.require([r for r in identity(MAIN) if r['comm']=='cta-prep-binanc']==[r for r in before['main_processes'] if r['comm']=='cta-prep-binanc'],'minute worker restarted')
    ops.require(ops.state()['generation']==before['main_state']['generation'] and not ops.state()['warming'],'minute factor windows were replaced')
    ops.save(AUDIT/'shared-config-installed.json',{'at_ms':time.time_ns()//1000000,'main_processes':identity(MAIN),'hourly_tables':sorted(hours),'minute_generation_preserved':True})
    print(json.dumps({'shared_config_server':True,'hourly_tables':4,'minute_generation_preserved':True}),flush=True)


def activate():
    before=ops.read(AUDIT/'before.json');state=ops.read(HOURLY/'data/cta_prep_binance_futures_1h/state.json')
    ops.require(state['generation'] and not state['warming'] and not state['error'] and not state['positions_error'],'hourly generation not ready')
    missing=set(state['plan']['calculations'])-set(state['latest'] or {})
    ops.require(not missing,'hourly calculations missing')
    unready=[k for k,p in state['latest'].items() if not all(f['ready'] for f in p['fields'].values())]
    ops.require(set(unready)<={'binance-futures-dashusdt-1h-depth_imb_bps1'},'unexpected unready calculation')
    docs={p.stem:ops.read(p) for p in (HOURLY/'config/cta_prep_binance_futures_1h').glob('*.json') if p.name!='symbol_groups.json'}
    mapping={};destinations={}
    with ops.session('lgq') as api:
        existing={p['strategy_name']:p for p in api('/manager/api/catalog/position-strategies')}
        for name,d in docs.items():
            pool=d.get('symbol_group');prefix=('cta-'+pool if pool else 'cta')+'-'+d['strategy_group']+'-bn-'
            strategy=prefix+'1h-pos';old=prefix+'15min-pos';source='binance_exec_rx01' if d['strategy_group']=='prep01' else 'binance_exec_rx03'
            ops.require(strategy not in existing,'hourly strategy already exists')
            api('/manager/api/catalog/position-strategies',{'strategy_name':strategy,'targets':{}})
            mapping[old]=[strategy];destinations[strategy]=source
    ops.clone_access(mapping)
    with ops.session('lgq') as api:
        for strategy,source in destinations.items():
            account=api('/manager/api/catalog/accounts/'+source)
            ops.require(account['bindings']==before['accounts'][source]['bindings'],'existing account bindings changed')
            api('/manager/api/catalog/accounts/'+source+'/bindings',{'binding_name':strategy,'position_strategy_name':strategy,'order_strategy_name':'lgq_grid_prep','shares':1})
            # Permit subsequent additions to this same source while preserving its original bindings.
            before['accounts'][source]=api('/manager/api/catalog/accounts/'+source)
    for name,d in docs.items():
        current=ops.local_api('/api/config?worker=cta-prep-binance-futures')
        ops.require(current['plan']['combinations'][name]==d,'hourly configuration changed')
        enabled=dict(d);enabled['signal']=dict(d['signal']);enabled['signal']['enabled']=True
        ops.local_api('/api/combinations',{'worker':'cta-prep-binance-futures','expected_revision':current['revision'],'combination':enabled})
    ops.save(AUDIT/'activated.json',{'at_ms':time.time_ns()//1000000,'strategies':destinations,'unready':unready,'grid':True,'shares':1,'max_pos_u':{'prep01':50,'prep02':100}})
    print(json.dumps({'activated':True,'strategies':destinations,'max_pos_u':{'prep01':50,'prep02':100},'unready':unready}),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('phase',choices=['prepare','start','config-service','activate']);parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    print(json.dumps({'phase':args.phase,'env':str(HOURLY),'market':'binance-futures','symbol_scope':'34 supplied hourly symbols','execute':args.execute}),flush=True)
    if args.execute:{'prepare':prepare,'start':start,'config-service':config_service,'activate':activate}[args.phase]()
