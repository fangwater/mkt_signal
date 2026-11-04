#!/usr/bin/env python3
"""Replace the auxiliary portfolio with one managed account. Never trade."""
import argparse, ast, hashlib, json, os, re, shutil, subprocess, tarfile, time, tomllib
from pathlib import Path

p=argparse.ArgumentParser();p.add_argument('phase',choices=['prepare','activate','verify']);p.add_argument('--stage',required=True);args=p.parse_args()
manager=Path.home()/'crypto_cta_manager';stage=Path(args.stage).resolve();assert stage.parent==manager/'deploy-staging'
audit=manager/'maintenance-backups'/stage.name;account='cta_cross_spread_rx01';members=['okex_exec_trade01','binance_exec_spread01']
manifest=json.loads((stage/'manifest.json').read_text());config_path=manager/'config/cta-manager.toml'
helper=Path.home()/'binance_exec_trade06/run/rx01_activation_ops.py';tree=ast.parse(helper.read_text())
ns={'MANAGER':manager,'ORIGIN':'http://127.0.0.1:4191'}
nodes=[n for n in tree.body if isinstance(n,(ast.Import,ast.ImportFrom)) or isinstance(n,ast.FunctionDef) and n.name in ['db_env','sql','session']]
exec(compile(ast.Module(body=nodes,type_ignores=[]),str(helper),'exec'),ns);sql=ns['sql'];session=ns['session']

def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def write(path,data):
    path=Path(path);path.parent.mkdir(exist_ok=True,parents=True)
    with path.open('x') as f: f.write(data);f.flush();os.fsync(f.fileno())
    path.chmod(0o600)
def receipt(name,data): write(audit/name,json.dumps(data,indent=2,ensure_ascii=False)+'\n')
def install(src,dst,mode=0o755):
    dst=Path(dst);tmp=dst.with_name(dst.name+'.managed-account-next');assert not tmp.exists();shutil.copyfile(src,tmp);tmp.chmod(mode);assert sha(src)==sha(tmp);os.replace(tmp,dst)
def processes():
    names={'trade_engine','exec-pre-trade','pre_trade','trade_signal','account_monitor','binance_account','okex_account_mo','persist_manager','viz_server','cta-factor-spre','cta_signal'}
    out={}
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():continue
        try:
            comm=(proc/'comm').read_text().strip()
            if comm in names:out[proc.name]={'comm':comm,'cwd':os.readlink(proc/'cwd'),'start_ticks':(proc/'stat').read_text().split(') ',1)[1].split()[19]}
        except (OSError,PermissionError):pass
    return out

def invariants():
    out={}
    for table in ['cta_position_snapshots','cta_strategy_position_snapshots','cta_strategy_allocation_snapshots','cta_account_strategy_bindings']:
        out[table]=sql(f"SELECT count(*) FROM {table} WHERE source_id IN ('okex_exec_trade01','binance_exec_spread01');")
        assert out[table]=='0',(table,out[table])
    return out

def compare_config():
    old=tomllib.loads((audit/'cta-manager.toml.before').read_text());new=tomllib.loads((audit/'cta-manager.toml.candidate').read_text())
    assert old['dashboard']['nav_portfolios'].pop(account)['source_ids']==members
    if not old['dashboard']['nav_portfolios']:old['dashboard'].pop('nav_portfolios')
    assert new['accounts'].pop(account)=={'name':'CTA Spread · OKX / Binance','source_ids':members,'created_at_us':1793625556000000}
    if not new['accounts']:new.pop('accounts')
    assert new==old,'unexpected configuration changes'

if args.phase=='prepare':
    assert not audit.exists();audit.mkdir(mode=0o700)
    for name,digest in manifest['artifacts'].items():assert sha(stage/'bin'/name)==digest
    assert sha(stage/'frontend.tar.gz')==manifest['frontend_sha256']
    raw=config_path.read_text();write(audit/'cta-manager.toml.before',raw)
    # The value includes an array, so remove the exact parsed section boundary.
    begin=raw.index('[dashboard.nav_portfolios.cta_cross_spread_rx01]');end=raw.find('\n[',begin+1);assert end>begin
    candidate=raw[:begin]+raw[end:]+ '\n[accounts.cta_cross_spread_rx01]\nname = "CTA Spread · OKX / Binance"\nsource_ids = ["okex_exec_trade01", "binance_exec_spread01"]\ncreated_at_us = 1793625556000000\n'
    write(audit/'cta-manager.toml.candidate',candidate);compare_config()
    write(audit/'webroot.before',os.readlink(manager/'webroot'))
    (audit/'bin.before').mkdir()
    for name in manifest['artifacts']:shutil.copy2(manager/'bin'/name,audit/'bin.before'/name)
    receipt('processes.before.json',processes())
    paths=[]
    old=tomllib.loads(raw)
    for source in old['sources']:
        if source.get('env_path'):paths.extend([Path(source['env_path']),Path(source['env_path']).parent/'trade_engine.toml'])
    paths.extend(Path.home()/env/file for env in ['okex_mm_alpha','binance_exec_trade08','okex-binance-cta-spread-rx01'] for file in ['env.sh','trade_engine.toml'])
    receipt('protected-files.json',{str(p):sha(p) for p in set(paths) if p.is_file()})
    receipt('invariants.before.json',invariants())
    with session('lgq') as api:
        dash=api('/manager/api/dashboard')
        assert len([a for a in dash['accounts'] if a['source_id'] in members])==2
        baseline={s['source_id']:s for s in dash['report']['sources'] if s['source_id'] in members}
        assert len(baseline)==2
        for s in baseline.values():assert s['order_event_count']==0 and s['fill_count']==0 and s['initial_position_snapshot_ts_us'] is None
        receipt('fresh-ledgers.before.json',baseline)
        receipt('risk.before.json',{id:api(f'/manager/api/catalog/accounts/{id}/exec-order-rate-limits') for id in members})
    pid=subprocess.check_output(['systemctl','--user','show','crypto-cta-manager-web.service','-p','MainPID','--value'],text=True).strip()
    env=dict(os.environ);env.update(x.decode().split('=',1) for x in Path(f'/proc/{pid}/environ').read_bytes().split(b'\0') if b'=' in x)
    checked=subprocess.run([str(stage/'bin/cta_web'),'--config',str(audit/'cta-manager.toml.candidate'),'--check-allocations'],env=env,text=True,capture_output=True)
    write(audit/'allocation-preflight.log',checked.stdout+checked.stderr);assert checked.returncode==0,'immutable-allocation preflight failed; see protected audit log'
    print('Prepared one-account configuration; five-source allocation validation and fresh-ledger checks passed.',flush=True)

elif args.phase=='activate':
    compare_config();assert sha(config_path)==sha(audit/'cta-manager.toml.before')
    assert json.loads((audit/'processes.before.json').read_text())==processes()
    assert sha(stage/'frontend.tar.gz')==manifest['frontend_sha256']
    release=manager/'web-releases'/stage.name
    if release.exists():
        assert (manager/'webroot').resolve()!=release.resolve()
        unactivated=audit/'webroot.first-attempt';assert not unactivated.exists();release.rename(unactivated)
    release.mkdir()
    with tarfile.open(stage/'frontend.tar.gz') as tar:
        for item in tar.getmembers():assert item.isfile() or item.isdir();assert '..' not in Path(item.name).parts and not Path(item.name).is_absolute()
        tar.extractall(release,filter='data')
    try:
        for name,digest in manifest['artifacts'].items():
            assert sha(stage/'bin'/name)==digest;install(stage/'bin'/name,manager/'bin'/name)
        next_link=manager/'webroot.managed-account-next';next_link.symlink_to(release.relative_to(manager));os.replace(next_link,manager/'webroot')
        install(audit/'cta-manager.toml.candidate',config_path,0o600)
        subprocess.run(['systemctl','--user','restart','crypto-cta-manager-web.service'],check=True)
        print('Only Manager restarted; waiting for authenticated gateway health.',flush=True)
        for attempt in range(300):
            try:
                with session('lgq') as api:
                    health=api('/manager/api/health')
                    assert health['status']=='ok'
                    dash=api('/manager/api/dashboard');assert any(a['source_id']==account for a in dash['accounts'])
                break
            except Exception:
                if attempt==299:raise
                time.sleep(2)
        print('Manager healthy with one Spread account.',flush=True)
    except Exception:
        for name in manifest['artifacts']:install(audit/'bin.before'/name,manager/'bin'/name)
        install(audit/'cta-manager.toml.before',config_path,0o600)
        next_link=manager/'webroot.rollback-next';next_link.symlink_to((audit/'webroot.before').read_text());os.replace(next_link,manager/'webroot')
        subprocess.run(['systemctl','--user','restart','crypto-cta-manager-web.service'],check=True)
        raise RuntimeError('Manager activation failed; previous binary/config/webroot restored') from None

elif args.phase=='verify':
    with session('lgq') as api:
        dash=api('/manager/api/dashboard');accounts=dash['accounts'];report=dash['report']
        group=[a for a in accounts if a['source_id']==account];assert len(group)==1
        assert not any(a['source_id'] in members for a in accounts)
        assert [a['source_id'] for a in group[0]['execution_sources']]==members
        assert group[0]['account_pnl_start_ts_us']==1793625556000000
        aggregate=next(s for s in report['sources'] if s['source_id']==account)
        assert len(aggregate['execution_sources'])==2 and aggregate['fill_count']==0 and aggregate['nav_change_after_fee_quote']==0
        assert aggregate['order_event_count']==0 and aggregate['initial_position_count']==0 and aggregate['initial_position_snapshot_ts_us'] is None
        assert aggregate['maker_fee_rate'] is None and aggregate['taker_fee_rate'] is None
        studio=api(f'/manager/api/catalog/accounts/{account}');assert [s['source_id'] for s in studio['execution_sources']]==members
        assert all(not s['bindings'] for s in studio['execution_sources'])
        risk={id:api(f'/manager/api/catalog/accounts/{id}/exec-order-rate-limits') for id in members};assert risk==json.loads((audit/'risk.before.json').read_text())
        grants=api(f'/manager/api/catalog/accounts/{account}/grants');assert any(g['username']=='lgq' and g['access_level']=='configure' for g in grants)
        end_ms=int(time.time()*1000)-60000
        query=f'sourceIds={account}&startMs=1793625556000&endMs={end_ms}&includeTheoretical=false'
        for path in ['/timeline','/account-timeline']:
            timeline=api('/manager/api'+path+'?'+query)
            assert set(timeline['report']['selected_source_ids'])==set(members)
            assert timeline['report']['summary']['nav_change_after_fee_quote']==0 and timeline['report']['summary']['fill_count']==0
            assert not timeline['report']['strategy_points']
        summary=api(f'/manager/api/pnl/strategy/summary?sourceId={account}&strategyName=spread&startMs=1793625556000&endMs={end_ms}')
        assert summary['source_id']==account and summary['totals']['fill_count']==0 and summary['totals']['nav_change_after_fee_quote']==0
        symbols=api(f'/manager/api/symbols?sourceIds={account}');assert {s['source_id'] for s in symbols['sources']}==set(members)
        overview=api('/manager/api/account-overview');assert len([r for r in overview['accounts'] if r['source_id']==account])==1
        assert not any(r['source_id'] in members for r in overview['accounts'])
        exchange=api('/manager/api/nav/exchange');assert len([r for r in exchange['accounts'] if r['source_id']==account])==1
        assert not any(r['source_id'] in members for r in exchange['accounts'])
        cost=api(f'/manager/api/catalog/acquisition-cost?sourceIds={account}&startMs=1793625556000&endMs={end_ms}')
        receipt('account.after.json',{'account':group[0],'nav':aggregate,'studio':studio,'risk':risk,'grants':grants,'strategy_summary':summary})
    denied=[]
    with session('prc') as api:
        dash=api('/manager/api/dashboard');assert not any(a['source_id'] in members+[account] for a in dash['accounts'])
        for path in [f'/manager/api/catalog/accounts/{account}',f'/manager/api/timeline?sourceIds={account}', '/exec_okex01/snapshot','/exec_binance_spread01/snapshot']:
            try:api(path)
            except RuntimeError as error:assert 'HTTP 403' in str(error);denied.append(path)
            else:raise AssertionError('hidden account leaked')
    assert json.loads((audit/'processes.before.json').read_text())==processes(),'protected trading process changed'
    for path,digest in json.loads((audit/'protected-files.json').read_text()).items():assert sha(path)==digest,path
    assert invariants()==json.loads((audit/'invariants.before.json').read_text())
    for row in processes().values():
        if row['cwd'] in [str(Path.home()/m) for m in members]+[str(Path.home()/'okex-binance-cta-spread-rx01')]:
            assert row['comm'] not in {'trade_engine','exec-pre-trade','pre_trade','trade_signal','cta-factor-spre','cta_signal'}
    pid=subprocess.check_output(['systemctl','--user','show','crypto-cta-manager-web.service','-p','MainPID','--value'],text=True).strip()
    result={'account_id':account,'public_account_count':1,'execution_sources':members,'trading_nav':0,'order_count':0,'initial_snapshots':0,'bindings':0,'risk_unchanged':True,'protected_processes_unchanged':len(processes()),'protected_configs_unchanged':True,'prc_forbidden':len(denied),'manager_pid':int(pid),'trading_enabled':False,'physical_binance_residual':'IOTAUSDT -0.1 (previous signed observation; not seeded or closed)'}
    receipt('verification.json',result);print(json.dumps(result),flush=True)
