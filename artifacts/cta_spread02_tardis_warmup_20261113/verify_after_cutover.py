"""Read-only verification after the environment-local worker cutover."""
import hashlib
import json
from pathlib import Path
import sys
import time


def main():
    stage=Path(__file__).resolve().parent
    sys.path.insert(0,str(stage))
    import cutover
    installed=json.loads((stage/'cutover_installed.json').read_text())
    root=Path.home()/'cta-factor-spread'
    state=json.loads((root/'data/cta_factor_spread/state.json').read_text())
    assert time.time()*1000-state['updated_ms']<15000
    assert state['generation'] and not state['warming'],'worker is still replaying'
    assert state['generation']!='67181a32-7d5a-4ed9-9daf-156517a13136'
    assert state['error'] is None and state['positions_error'] is None
    assert state['position_tables']['publication_error'] is None
    assert all(not p['pending_strategies'] and p['error'] is None for p in state['position_tables']['manager_publications'])
    assert len(state['plan']['calculations'])==532
    assert len(state['latest'])==532
    assert state['minute_formulas_restored']['active']==227
    for name in ['binance-futures-adausdt-15min-baseline_104','binance-futures-trbusdt-5min-tp_vpi_001','binance-futures-trbusdt-5min-tp_vpi_005']:
        assert all(v['ready'] for v in state['latest'][name]['fields'].values()),name
    original=json.loads((stage/'config_snapshot.json').read_text())
    for name,value in original.items():assert json.loads((root/'config/cta_factor_spread'/name).read_text())==value,name
    tables={t['strategy_name']:t for t in state['position_tables']['tables']}
    position_names={v['combination_name']:v['strategy_name'] for v in json.loads((stage/'cutover_checked_plan.json').read_text())['position_tables']}
    output=[]
    for name,combination in state['plan']['combinations'].items():
        ids=[i for i,c in state['plan']['calculations'].items() if name in c['combinations']]
        unready=[i for i in ids if not all(v['ready'] for v in state['latest'][i]['fields'].values())]
        expected_enabled=combination['strategy_group']=='spread01'
        assert combination['signal']['enabled']==expected_enabled
        if combination['strategy_group']=='spread02':
            assert combination['signal']['max_pos_u']==100
            strategy=position_names[name]
            persisted=json.loads((root/'data/cta_factor_spread/positions'/(strategy+'.json')).read_text())
            assert not persisted['ever_enabled'] and not persisted['clock']
            assert not tables[strategy]['observations']
            assert all(v['qty']==0 for v in tables[strategy]['targets'].values())
        if combination['interval']=='1h':assert not unready,unready
        output.append({'group':combination['strategy_group'],'interval':combination['interval'],
            'enabled':expected_enabled,'calculations':len(ids),'ready':len(ids)-len(unready),'unready':unready})
    current=cutover.scoped_processes()
    before=json.loads((Path(installed['backup'])/'processes.json').read_text())
    changed={pid:item for pid,item in before.items() if item['exe']!=str(root/'cta-factor-spread') and current.get(pid)!=item}
    assert not changed,changed
    proc=current[str(installed['worker_pid'])]
    assert proc['cpus']=='18' and proc['exe']==str(root/'cta-factor-spread')
    assert hashlib.sha256((root/'cta-factor-spread').read_bytes()).hexdigest()==installed['worker_sha256']
    hourly=state['hourly_history_warmup']['active']
    assert set(hourly)=={'binance-futures','okex-futures'}
    assert all(v['cutoff_ms']==1794441600000 for v in hourly.values())
    result={'checked_ms':int(time.time()*1000),'generation':state['generation'],'worker_pid':installed['worker_pid'],
        'cpu':'18','warming':False,'error':None,'positions_error':None,'manager_pending':0,
        'configuration_unchanged':True,'tables':output,'hourly_history':hourly,'protected_process_changes':changed,
        'preserved_ledgers_checkpoint':str(Path(installed['backup'])/'position_ledgers.json'),
        'worker_sha256':installed['worker_sha256'],'spread02_publish_enabled':False}
    (stage/'continuation_verified.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:result[k] for k in ['generation','worker_pid','cpu','warming','error','configuration_unchanged','spread02_publish_enabled']}))
    print(json.dumps(output))


if __name__=='__main__':main()
