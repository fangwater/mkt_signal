"""Publish the LocalSet fix without changing the six enabled Prep tables."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).parent))
import activate as a


def compute_processes():
    found = []
    for p in Path('/proc').glob('[0-9]*'):
        try:
            if (p / 'cwd').resolve() == a.PREP and (p / 'exe').resolve().name == a.PREP.name:
                found.append(int(p.name))
        except OSError:
            pass
    return found


def configs():
    result = a.immutable_hashes()
    for p in (a.PREP / 'config/cta_prep_binance_futures').glob('*.json'):
        result[str(p)] = a.sha(p)
        a.require(json.loads(p.read_text())['signal']['enabled'], 'Prep enable changed')
    return result


def prepare(stage, audit):
    a.require(not compute_processes(), 'old Prep calculation is still running')
    a.require(not (audit / 'recovery-before.json').exists(), 'recovery already prepared')
    manifest = json.loads((stage / 'manifest.json').read_text())
    for name, digest in manifest.items():
        a.require(a.sha(stage / name) == digest, 'uploaded binary digest mismatch')
        shutil.copyfile(a.H / name / name, audit / (name + '.before-localset'))
    a.save(audit / 'recovery-before.json', {
        'configs': configs(), 'bindings': a.rows('cta_account_strategy_bindings'),
        'processes': a.processes(), 'manifest': manifest,
        'old_generation': a.state()['generation']})
    a.save(audit / 'recovery-manifest.json', manifest)
    print('Recovery prepared; uploaded hashes match and enabled settings backed up.', flush=True)


def apply(stage, audit):
    before = json.loads((audit / 'recovery-before.json').read_text())
    a.require(configs() == before['configs'], 'configuration changed after preparation')
    a.require(a.binding_contract(a.rows('cta_account_strategy_bindings')) == a.binding_contract(before['bindings']),
              'binding changed after preparation')
    a.require(not compute_processes(), 'Prep calculation unexpectedly running')
    for name, digest in before['manifest'].items():
        root = a.H / name
        temp = root / (name + '.localset-next')
        a.require(not temp.exists(), 'staged live replacement exists')
        shutil.copyfile(stage / name, temp)
        temp.chmod(0o755)
        a.require(a.sha(temp) == digest, 'replacement checksum mismatch')
        os.replace(temp, root / name)
    output_path = audit / 'start-prep-localset.private.log'
    with output_path.open('wb') as output:
        result = subprocess.run([str(a.PREP / 'scripts/start_cta_prep_binance_futures.sh'), '--execute'],
                                cwd=a.PREP, stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                                env=dict(os.environ, PATH=str(a.H / '.local/bin') + ':' + os.environ['PATH']),
                                timeout=100)
    a.require(result.returncode == 0, 'environment-local Prep start failed; see private diagnostics')
    print('LocalSet artifacts installed atomically; Prep started through environment script.', flush=True)


def verify(audit):
    before = json.loads((audit / 'recovery-before.json').read_text())
    current_configs = configs()
    a.require(a.scoped_hashes(current_configs) == a.scoped_hashes(before['configs']),
              'Prep configuration changed during recovery')
    a.require(a.nonspread_binding_contract(a.rows('cta_account_strategy_bindings')) ==
              a.nonspread_binding_contract(before['bindings']), 'non-Spread bindings changed during recovery')
    current = a.processes()
    protected = {pid: row for pid, row in before['processes'].items() if not a.parallel_spread_compute(row)}
    a.require(all(current.get(pid) == value for pid, value in protected.items()),
              'protected execution or other process changed')
    for name, digest in before['manifest'].items():
        a.require(a.sha(a.H / name / name) == digest, 'live artifact differs')
    pids = compute_processes()
    a.require(len(pids) == 1, 'Prep process missing or duplicated')
    s = a.state()
    a.require(s['generation'] and s['generation'] != before['old_generation'] and not s['warming'],
              'new replay not complete')
    a.require(not s['error'] and not s['positions_error'], 'calculation or position error')
    a.require(len(s['latest']) == 404 and s['amount_threshold_symbols'] == 51, 'factor outputs or thresholds missing')
    nq = [p for name, p in s['latest'].items() if name.endswith('-nq')]
    a.require(len(nq) == 94 and all(all(f['ready'] for f in p['fields'].values()) for p in nq), 'NQ not ready')
    tables = s['position_tables']['tables']
    a.require(len(tables) == 6 and all(t['enabled'] for t in tables), 'six runtime flags not enabled')
    publications = s['position_tables']['manager_publications']
    a.require(publications and all(p['error'] is None and p['last_success_ts_us'] is not None
                                  and not p['pending_strategies'] for p in publications), 'Manager delivery not acknowledged')
    result = {'generation': s['generation'], 'prep_pid': pids[0],
              'cpus': sorted(os.sched_getaffinity(pids[0])), 'factor_outputs': len(s['latest']),
              'ready_factors': sum(all(f['ready'] for f in p['fields'].values()) for p in s['latest'].values()),
              'nq_ready': len(nq), 'amount_threshold_symbols': 51,
              'six_prep_tables_enabled': True, 'prep_configs_and_nonspread_bindings_unchanged': True,
              'outside_scope_config_changes': [key for key, digest in before['configs'].items()
                                               if current_configs.get(key) != digest],
              'protected_processes_unchanged': len(protected),
              'outside_scope_process_changes': [pid for pid, row in before['processes'].items()
                                                if current.get(pid) != row and a.parallel_spread_compute(row)],
              'binary_sha256': before['manifest'],
              'manager_publications': publications, 'checked_at_ms': time.time_ns() // 1000000}
    a.save(audit / 'calculation_recovery.json', result)
    print(json.dumps(result), flush=True)


def watch(audit):
    deadline = time.monotonic() + 5400
    while time.monotonic() < deadline:
        a.require(len(compute_processes()) == 1, 'Prep calculation exited during replay')
        s = a.state()
        print(json.dumps({'warming': s['warming'], 'warm_records': s['warm_records'],
                          'generation': s['generation'], 'error': s['error'],
                          'positions_error': s['positions_error'],
                          'checked_at_ms': time.time_ns() // 1000000}), flush=True)
        if s['generation'] and not s['warming']:
            publications = s['position_tables']['manager_publications']
            if publications and all(p['error'] is None and p['last_success_ts_us'] is not None
                                    and not p['pending_strategies'] for p in publications):
                verify(audit)
                return
        time.sleep(20)
    raise RuntimeError('Replay or Manager acknowledgement exceeded the catchup window')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('phase', choices=['prepare', 'apply', 'verify', 'watch'])
    p.add_argument('--stage', type=Path, required=True)
    p.add_argument('--audit', type=Path, required=True)
    p.add_argument('--execute', action='store_true')
    v = p.parse_args()
    os.umask(0o077)
    a.require(v.stage.parent == a.M / 'deploy-staging' and v.stage.name.startswith('cta_prep_live_localset_'),
              'unexpected stage directory')
    a.require(v.audit.parent == a.M / 'maintenance-backups' and v.audit.name.startswith('cta_prep_live_'),
              'unexpected audit directory')
    if v.phase == 'apply':
        a.require(v.execute, 'apply requires --execute')
    if v.phase in {'verify', 'watch'}:
        globals()[v.phase](v.audit)
    else:
        globals()[v.phase](v.stage, v.audit)


if __name__ == '__main__':
    main()
