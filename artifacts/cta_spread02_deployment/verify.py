#!/usr/bin/env python3
"""Read-only post-deployment verification on JP; emits a credential-free receipt."""
import hashlib
import json
from pathlib import Path
import sys
import time
import urllib.request


def main():
    stage = Path(__file__).resolve().parent
    sys.path.insert(0, str(stage))
    import deploy
    root = Path.home() / 'cta-factor-spread'
    state = json.loads((root / 'data/cta_factor_spread/state.json').read_text())
    assert 0 <= time.time()*1000-state['updated_ms'] < 15000
    assert state['generation'] and not state['warming'], 'wait for shared replay to complete'
    assert state['error'] is None and state['positions_error'] is None
    assert state['position_tables']['publication_error'] is None
    assert all(not p['pending_strategies'] and p['error'] is None
               for p in state['position_tables']['manager_publications'])
    with urllib.request.urlopen('http://127.0.0.1:19220/api/config?worker=cta-factor-spread') as response:
        catalog = json.load(response)
    assert len(catalog['plan']['combinations']) == 8
    assert catalog['unique_factor_count'] == 532
    before = json.loads((stage / 'before/configuration_hashes.json').read_text())
    for name, expected in before.items():
        if name != 'env.sh':
            assert hashlib.sha256((root / name).read_bytes()).hexdigest() == expected
    checkpoint = json.loads((stage / 'recovery_checkpoint.json').read_text())
    assert all(checkpoint['spread01_ledger_matches'].values())
    current = deploy.processes()
    original = json.loads((stage / 'before/processes.json').read_text())
    changed_executables = {
        str(root / 'cta-factor-spread'),
        str(Path.home() / 'cta-prep-binance-futures/cta-factor-config'),
        str(Path.home() / 'delist_risk_server/delist_risk_server'),
    }
    changed = {pid: item for pid, item in original.items()
               if item['exe'] not in changed_executables and current.get(pid) != item}
    completed_batch = {pid: item for pid, item in changed.items()
                       if item['exe'] == str(Path.home() / 'crypto_nav_manager/target/release/sync_history')
                       and pid not in current}
    changed = {pid: item for pid, item in changed.items() if pid not in completed_batch}
    assert not changed, changed
    services = {exe: [(pid, item) for pid, item in current.items() if item['exe'] == exe]
                for exe in changed_executables}
    assert all(len(matches) == 1 for matches in services.values())
    assert services[str(root / 'cta-factor-spread')][0][1]['cpus'] == '18'
    assert services[str(Path.home() / 'delist_risk_server/delist_risk_server')][0][1]['cpus'] == '28'
    manifest = json.loads((stage / 'upload_manifest.json').read_text())
    for source, destination in [('cta-factor-spread', root / 'cta-factor-spread'),
                                ('cta-factor-config', Path.home() / 'cta-prep-binance-futures/cta-factor-config')]:
        assert hashlib.sha256(destination.read_bytes()).hexdigest() == manifest[source]
    counts = {'1min': 48, '5min': 79, '15min': 64, '1h': 128}
    tables = {table['strategy_name']: table for table in state['position_tables']['tables']}
    summary = []
    all_ids = set()
    for name, combination in catalog['plan']['combinations'].items():
        assert combination == state['plan']['combinations'][name]
        table = tables[catalog['position_strategies'][name]]
        if combination['strategy_group'] == 'spread01':
            assert table['enabled'] and combination['signal']['enabled']
            continue
        assert combination['strategy_group'] == 'spread02'
        interval = combination['interval']
        assert len(combination['factor_rules']) == counts[interval]
        assert not combination['signal']['enabled'] and not table['enabled']
        assert combination['signal']['max_pos_u'] == 100
        assert all(row['venue'] == 'okex-futures' for row in combination['factor_rules'])
        persisted = json.loads((root / 'data/cta_factor_spread/positions' /
                                (table['strategy_name'] + '.json')).read_text())
        assert not persisted['ever_enabled'] and not persisted['clock']
        assert all(target['qty'] == 0 for target in table['targets'].values())
        assert not table['observations']
        ids = [key for key, calculation in state['plan']['calculations'].items()
               if name in calculation['combinations']]
        all_ids.update(ids)
        unready = [key for key in ids if key not in state['latest'] or
                   not all(field['ready'] for field in state['latest'][key]['fields'].values())]
        summary.append({'interval': interval, 'rules': counts[interval], 'enabled': False,
                        'max_pos_u': 100, 'strategy': table['strategy_name'],
                        'calculations': len(ids), 'ready': len(ids)-len(unready), 'unready': unready})
    assert len(summary) == 4 and len(all_ids) == 341
    report = {'verified_at_ms': int(time.time()*1000), 'host': 'jp-meta-elvpn',
              'environment': str(root), 'generation': state['generation'], 'warming': False,
              'symbols': 18, 'rules': 319, 'tables': summary, 'merged_calculations': 532,
              'error': state['error'], 'positions_error': state['positions_error'],
              'manager_publications': state['position_tables']['manager_publications'],
              'spread01_restored_ledger_checkpoint': checkpoint,
              'stable_service_process_changes': changed,
              'completed_scheduled_nav_batch_processes': completed_batch,
              'services': services, 'threshold_symbols_loaded': state['amount_threshold_symbols'],
              'private_rollback': str(stage / 'before')}
    (stage / 'verified.json').write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps({key: report[key] for key in ['verified_at_ms', 'generation', 'rules',
          'warming', 'error', 'positions_error', 'stable_service_process_changes']}))
    print(json.dumps([{key: table[key] for key in ['interval', 'rules', 'enabled', 'ready',
                                                'calculations']} for table in summary]))


if __name__ == '__main__':
    main()
