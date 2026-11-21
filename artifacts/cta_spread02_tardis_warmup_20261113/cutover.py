"""Publish the validated Tardis hourly bundle and worker. Dry-run by default."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def atomic_copy(source, destination):
    temporary=destination.with_name('.'+destination.name+'.tardis-next')
    shutil.copy2(source,temporary)
    assert digest(source)==digest(temporary)
    os.replace(temporary,destination)


def lifecycle(root, action, before, suffix='', check=True):
    # Supervisor diagnostics may include inherited environment settings.
    with (before/(action+suffix+'.log')).open('w') as log:
        return subprocess.run([str(root/('scripts/'+action+'_cta_factor_spread.sh')),'--execute'],
            cwd=root,stdout=log,stderr=subprocess.STDOUT,check=check)


def scoped_processes():
    envs=['cta-factor-spread','okex_exec_trade01','binance_exec_spread01','cta-prep-binance-futures','delist_risk_server']
    found={}
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():continue
        try:
            cwd=str((proc/'cwd').resolve(strict=True))
            env=next((name for name in envs if cwd==str(Path.home()/name)),None)
            if env:
                fields=(proc/'stat').read_text().rsplit(')',1)[1].split()
                cpus=next(line.split(':',1)[1].strip() for line in (proc/'status').read_text().splitlines() if line.startswith('Cpus_allowed_list:'))
                found[proc.name]={'env':env,'exe':str((proc/'exe').resolve(strict=True)),'starttime':fields[19],'cpus':cpus}
        except (OSError,StopIteration):continue
    return found


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute',action='store_true')
    args=parser.parse_args()
    stage=Path(__file__).resolve().parent
    candidate=stage/'candidate'
    root=Path.home()/'cta-factor-spread'
    assert subprocess.check_output(['hostname','-f'],text=True).startswith('ip-172-31-35-228.')
    manifest=json.loads((stage/'cutover_manifest.json').read_text())
    for filename in ['cta-factor-spread.upload','cta-hourly-history.upload','cta_factor_process.sh.upload']:
        assert digest(candidate/filename)==manifest[filename],filename
    assert digest(root/'cta-factor-spread')==manifest['original_worker_sha256']
    assert digest(root/'scripts/cta_factor_process.sh')==manifest['original_wrapper_sha256']
    env_pattern=r'(?m)^\s*(?:export\s+)?CTA_HOURLY_HISTORY_DIR=.*$'
    assert len(re.findall(env_pattern,(root/'env.sh').read_text()))==1,'unexpected hourly-history env declaration'
    original=json.loads((stage/'config_snapshot.json').read_text())
    for filename,value in original.items():
        assert json.loads((root/'config/cta_factor_spread'/filename).read_text())==value,filename
    combinations=[v for k,v in original.items() if k!='symbol_groups.json']
    assert len(combinations)==8
    assert all(c['signal']['enabled']==(c['strategy_group']=='spread01') for c in combinations)
    assert all(c['signal']['max_pos_u']==100 for c in combinations if c['strategy_group']=='spread02')
    replay=json.loads((stage/'continuation_replay.json').read_text())
    assert not replay['missing']
    assert all(p['fields'] and all(f['ready'] for f in p['fields'].values()) for i,p in replay['latest'].items() if '-1h-' in i)
    current_state=json.loads((root/'data/cta_factor_spread/state.json').read_text())
    generation=current_state['generation']
    assert generation and all(c in '0123456789abcdef-' for c in generation)
    minute=candidate/'minute_formula_state.json'
    exported=subprocess.run([str(candidate/'cta-hourly-history.upload'),'minute-state','--config',str(root/'config/cta_factor_spread.toml'),
        '--generation',str(root/'data/cta_factor_spread/generations'/generation),
        '--control-snapshot',str(stage/'control_snapshot.json'),'--output',str(minute)],capture_output=True,text=True)
    assert exported.returncode==0,exported.stderr
    save(stage/'minute_state_export.json',json.loads(exported.stdout))
    checkpoint=json.loads(minute.read_text())
    for name in ['binance-futures-adausdt-15min-baseline_104','binance-futures-trbusdt-5min-tp_vpi_001','binance-futures-trbusdt-5min-tp_vpi_005']:
        assert checkpoint['factors'][name]['latest']['fields']['value']['ready'],name
    history=candidate/'extended_history'
    for market in ['binance-futures','okex-futures']:
        base=history/market;meta=json.loads((base/'manifest.json').read_text())
        assert meta['market']==market and meta['end_ms']==1794441600000
        assert len(meta['symbols'])==17
        for symbol,v in meta['symbols'].items():assert digest(base/v['file'])==v['sha256'],symbol
    check=[str(candidate/'cta-factor-spread.upload'),'--config',str(root/'config/cta_factor_spread.toml'),
           '--check','--validate-hourly-history','--validate-kafka-continuity','--hourly-history',str(history),'--minute-state',str(minute)]
    checked=subprocess.run(check,cwd=root,text=True,capture_output=True)
    assert checked.returncode==0,checked.stderr
    save(stage/'cutover_checked_plan.json',json.loads(checked.stdout))
    protected=scoped_processes()
    old=[pid for pid,item in protected.items() if item['exe']==str(root/'cta-factor-spread')]
    assert old==['1965122'],old
    assert protected[old[0]]['cpus']=='18'
    assert all(sum(item['env']==name for item in protected.values())==6 for name in ['okex_exec_trade01','binance_exec_spread01'])
    state=json.loads((root/'data/cta_factor_spread/state.json').read_text())
    assert state['generation'] and not state['warming'] and state['error'] is None
    assert state['positions_error'] is None and state['position_tables']['publication_error'] is None
    assert not any(p['pending_strategies'] for p in state['position_tables']['manager_publications'])
    print(json.dumps({'execute':args.execute,'env':str(root),'markets':['okex-futures','binance-futures'],
        'hourly_symbols':17,'shared_calculations':532,'restart':['cta-factor-spread'],'cpu':'18',
        'spread02_enabled':False,'replay_records':replay['records']}),flush=True)
    if not args.execute:return
    before=stage/('before_cutover_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    before.mkdir(mode=0o700)
    for source,name in [(root/'env.sh','env.sh'),(root/'cta-factor-spread','cta-factor-spread'),
                         (root/'scripts/cta_factor_process.sh','cta_factor_process.sh'),
                         (root/'data/cta_factor_spread/state.json','state.json')]:shutil.copy2(source,before/name)
    os.chmod(before/'env.sh',0o600)
    shutil.copytree(root/'config',before/'config')
    shutil.copytree(root/'data/cta_factor_spread/positions',before/'positions_running')
    minute_destination=root/'data/cta_factor_spread/minute_formula_state.json'
    if minute_destination.exists():shutil.copy2(minute_destination,before/'minute_formula_state.json')
    (before/'logs').mkdir()
    for file in (Path.home()/'.pmdaemon/logs').glob('cta-factor-spread-*.log'):shutil.copy2(file,before/'logs'/file.name)
    save(before/'processes.json',protected)
    final=Path.home()/'cta-hourly-history/spread01_spread02_20261112/data'
    assert not final.parent.exists()
    temporary=final.parent.with_name('.'+final.parent.name+'.pending')
    assert not temporary.exists()
    shutil.copytree(history,temporary/'data')
    os.replace(temporary,final.parent)
    lifecycle(root,'stop',before)
    deadline=time.monotonic()+20
    while any(v['exe']==str(root/'cta-factor-spread') for v in scoped_processes().values()) and time.monotonic()<deadline:
        time.sleep(0.5)
    assert not any(v['exe']==str(root/'cta-factor-spread') for v in scoped_processes().values()),'old worker still alive'
    shutil.copytree(root/'data/cta_factor_spread/positions',before/'positions_stopped')
    ledgers={p.name:json.loads(p.read_text())['ledger'] for p in (root/'data/cta_factor_spread/positions').glob('*.json')}
    save(before/'position_ledgers.json',ledgers)
    try:
        atomic_copy(candidate/'cta-factor-spread.upload',root/'cta-factor-spread')
        atomic_copy(candidate/'cta_factor_process.sh.upload',root/'scripts/cta_factor_process.sh')
        atomic_copy(minute,minute_destination)
        env=(root/'env.sh').read_text()
        assert len(re.findall(env_pattern,env))==1,'unexpected hourly-history env declaration'
        env=re.sub(env_pattern,'export CTA_HOURLY_HISTORY_DIR='+str(final),env)
        temporary_env=root/'.env.sh.tardis-next'
        temporary_env.write_text(env);temporary_env.chmod((root/'env.sh').stat().st_mode & 0o777)
        os.replace(temporary_env,root/'env.sh')
        assert all(json.loads((root/'data/cta_factor_spread/positions'/name).read_text())['ledger']==value for name,value in ledgers.items())
        lifecycle(root,'start',before)
        time.sleep(3)
        current=scoped_processes()
        new=[(pid,item) for pid,item in current.items() if item['exe']==str(root/'cta-factor-spread')]
        assert len(new)==1 and new[0][1]['cpus']=='18'
        changed={pid:item for pid,item in protected.items() if pid not in old and current.get(pid)!=item}
        assert not changed,changed
        assert all(json.loads((root/'data/cta_factor_spread/positions'/name).read_text())['ledger']==value for name,value in ledgers.items())
        save(stage/'cutover_installed.json',{'installed_utc':datetime.now(timezone.utc).isoformat(),'backup':str(before),
            'worker_pid':int(new[0][0]),'cpu':'18','history':str(final),'worker_sha256':digest(root/'cta-factor-spread'),
            'minute_formulas_restored':json.loads(checked.stdout)['minute_formulas_restored'],
            'preserved_position_ledgers':list(ledgers),'protected_process_changes':changed,'spread02_enabled':False})
        print('INSTALLED: only factor worker restarted; ledger preserved; Spread02 publication disabled',flush=True)
    except Exception:
        (before/'failed_start_logs').mkdir(exist_ok=True)
        for file in (Path.home()/'.pmdaemon/logs').glob('cta-factor-spread-*.log'):shutil.copy2(file,before/'failed_start_logs'/file.name)
        lifecycle(root,'stop',before,'-rollback',check=False)
        atomic_copy(before/'cta-factor-spread',root/'cta-factor-spread')
        atomic_copy(before/'cta_factor_process.sh',root/'scripts/cta_factor_process.sh')
        atomic_copy(before/'env.sh',root/'env.sh')
        if (before/'minute_formula_state.json').exists():atomic_copy(before/'minute_formula_state.json',minute_destination)
        else:minute_destination.unlink(missing_ok=True)
        lifecycle(root,'start',before,'-rollback')
        raise


if __name__=='__main__':main()
