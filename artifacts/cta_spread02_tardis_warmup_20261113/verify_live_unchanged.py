"""Read-only final audit. Never source env.sh or print credentials."""
import hashlib
import json
from pathlib import Path
import time


def main():
    root=Path.home()/'cta-factor-spread'
    stage=Path.home()/'cta-audits/spread02_tardis_warmup_20261113'
    state=json.loads((root/'data/cta_factor_spread/state.json').read_text())
    original=json.loads((stage/'config_snapshot.json').read_text())
    changed=[]
    for name, expected in original.items():
        path=root/'config/cta_factor_spread'/name
        if not path.is_file() or json.loads(path.read_text())!=expected:
            changed.append(name)
    tables=[]
    for name,c in state['plan']['combinations'].items():
        ids=[i for i,v in state['plan']['calculations'].items() if name in v['combinations']]
        missing=[i for i in ids if i not in state['latest'] or not all(v['ready'] for v in state['latest'][i]['fields'].values())]
        tables.append({'group':c['strategy_group'],'interval':c['interval'],'enabled':c['signal']['enabled'],
                       'rules':len(c['factor_rules']),'calculations':len(ids),'ready':len(ids)-len(missing)})
    envs=['cta-factor-spread','okex_exec_trade01','binance_exec_spread01']
    processes={env:[] for env in envs}
    for path in Path('/proc').iterdir():
        if not path.name.isdigit():continue
        try:
            cwd=str((path/'cwd').resolve(strict=True))
            owned=next((env for env in envs if cwd==str(Path.home()/env)),None)
            if owned:
                cpus=next(line.split(':',1)[1].strip() for line in (path/'status').read_text().splitlines() if line.startswith('Cpus_allowed_list:'))
                processes[owned].append({'pid':int(path.name),'binary':(path/'exe').resolve(strict=True).name,'cpus':cpus})
        except (OSError,StopIteration):continue
    pubs=state['position_tables']
    result={'checked_ms':int(time.time()*1000),'updated_ms':state['updated_ms'],'generation':state['generation'],
        'warming':state['warming'],'error':state['error'],'positions_error':state['positions_error'],
        'publication_error':pubs.get('publication_error'),
        'manager_pending':sum(len(p['pending_strategies']) for p in pubs.get('manager_publications',[])),
        'config_differences_from_start':changed,'tables':tables,'processes':processes,
        'live_worker_sha256':hashlib.sha256((root/'cta-factor-spread').read_bytes()).hexdigest(),
        'spread02_disabled':all(not c['enabled'] for c in tables if c['group']=='spread02')}
    print(json.dumps(result,indent=2))
    assert result['spread02_disabled']
    assert time.time()*1000-state['updated_ms']<15000
    assert state['generation']=='67181a32-7d5a-4ed9-9daf-156517a13136'
    assert any(p['pid']==1965122 for p in processes['cta-factor-spread'])


if __name__=='__main__':main()
