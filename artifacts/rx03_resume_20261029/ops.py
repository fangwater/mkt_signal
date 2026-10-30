"""RX03 scoped resume. Credentials are only handled in remote process memory."""
import contextlib
import datetime
import hashlib
import json
import os
import pathlib
import secrets
import shutil
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request

HOME = pathlib.Path.home()
SIGNAL = HOME / 'binance-cta-perp-rx03'
EXEC = HOME / 'binance_exec_trade07'
MANAGER = HOME / 'crypto_cta_manager'
STAGE = SIGNAL / 'run/resume_20261029'
PREFIX = 'binance_cta_perp_rx03_'
VIRTUAL = 'virtual04'
ORIGIN = 'http://127.0.0.1:4191'


def save(name, value):
    target = STAGE / name
    tmp = target.with_suffix(target.suffix + '.next')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    os.replace(tmp, target)


def env_for(path):
    raw = subprocess.check_output(['bash', '-c', 'source "$1/env.sh"; env -0', 'bash', str(path)])
    return dict(row.decode().split('=', 1) for row in raw.split(b'\0') if b'=' in row)


def db_env(read_only=False):
    config = tomllib.loads((MANAGER / 'config/cta-manager.toml').read_text())
    pid = subprocess.check_output(['systemctl', '--user', 'show', 'crypto-cta-manager-web.service', '-p', 'MainPID', '--value'], text=True).strip()
    values = dict(row.split(b'=', 1) for row in pathlib.Path('/proc', pid, 'environ').read_bytes().split(b'\0') if b'=' in row)
    url = urllib.parse.urlsplit(values[config['database'].get('url_env', 'CRYPTO_CTA_LOCAL_DATABASE_URL').encode()].decode())
    result = dict(os.environ, PGHOST=url.hostname, PGPORT=str(url.port or 5432), PGUSER=urllib.parse.unquote(url.username), PGPASSWORD=urllib.parse.unquote(url.password or ''), PGDATABASE=url.path[1:])
    if read_only:
        result['PGOPTIONS'] = '-c default_transaction_read_only=on'
    return result


def sql(query, read_only=True):
    result = subprocess.run(['psql', '-X', '-q', '-A', '-t', '-v', 'ON_ERROR_STOP=1'], input=query, text=True, capture_output=True, env=db_env(read_only))
    if result.returncode:
        raise RuntimeError('database request failed; credential-bearing details suppressed')
    return result.stdout.strip()


@contextlib.contextmanager
def session(username):
    assert username in ['lgq', 'prc']
    token = secrets.token_urlsafe(32)
    digest = hashlib.sha256(token.encode()).hexdigest()
    sid = sql(f"INSERT INTO cta_auth_sessions(user_id,token_hash,expires_at) SELECT user_id,decode('{digest}','hex'),now()+interval '20 minutes' FROM cta_users WHERE username='{username}' AND disabled=false RETURNING session_id;", False)
    assert sid.isdigit()
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def api(path, payload=None, method=None):
        request = urllib.request.Request(ORIGIN + path, data=None if payload is None else json.dumps(payload).encode(), headers={'Cookie': 'cta_session=' + token, 'Content-Type': 'application/json'}, method=method)
        try:
            with opener.open(request, timeout=30) as response:
                raw = response.read()
                return json.loads(raw) if raw and 'json' in response.headers.get('Content-Type', '') else {'status': response.status}
        except urllib.error.HTTPError as error:
            raise RuntimeError(f'{path}: HTTP {error.code}: {error.read().decode()[:500]}') from None

    try:
        yield api
    finally:
        sql(f"DELETE FROM cta_auth_sessions WHERE session_id={sid} AND token_hash=decode('{digest}','hex');", False)


def pids(env=None):
    result = []
    for proc in pathlib.Path('/proc').glob('[0-9]*'):
        try:
            cwd = proc.joinpath('cwd').resolve()
            if env is not None and cwd != env:
                continue
            if env is None and not (str(cwd).startswith(str(HOME) + '/') and proc.joinpath('exe').exists()):
                continue
            result.append({'pid': int(proc.name), 'comm': proc.joinpath('comm').read_text().strip(), 'cwd': str(cwd), 'start': proc.joinpath('stat').read_text().split()[21]})
        except (OSError, IndexError):
            pass
    return result


def stopped():
    assert not any(p['comm'] == 'exec-pre-trade' for p in pids(EXEC)), 'Exec pre-trade is running'


def config():
    return json.loads((SIGNAL / 'config/cta_perp.json').read_text())


def status():
    return json.loads((SIGNAL / 'run/cta_perp_status.json').read_text())


def journal_positions():
    files = list((EXEC / 'data/rapidx_account').glob('*/*.jsonl'))
    path = max(files, key=lambda p: p.stat().st_mtime)
    latest = None
    # Active order streams can append several MB between complete position
    # snapshots. Expand the bounded tail and scan newest-first.
    for tail_bytes in [1_000_000, 4_000_000, 16_000_000, 64_000_000]:
        with path.open('rb') as stream:
            stream.seek(max(0, path.stat().st_size - tail_bytes))
            lines = stream.read().decode(errors='replace').splitlines()[1:]
        for line in reversed(lines):
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if row.get('message', {}).get('channel') == 'Positions':
                latest = row
                break
        if latest:
            break
    assert latest and time.time_ns() // 1000 - latest['received_us'] < 60_000_000, 'stale account positions'
    rows = []
    for row in latest['message']['data']:
        if float(row['positionQty']) != 0:
            rows.append({'symbol': row['sym'].removeprefix('BINANCE_PERP_').replace('_', ''), 'qty': row['positionQty'], 'value_usdt': row['positionValue']})
    return {'received_us': latest['received_us'], 'positions': rows}


def redis_client():
    import redis
    return redis.Redis.from_url(env_for(EXEC).get('REDIS_URL', 'redis://127.0.0.1:6379/0'), decode_responses=True)


def preview():
    stopped()
    assert not config()['enabled']
    assert not any(p['comm'] == 'cta_perp_signal' for p in pids(SIGNAL))
    assert not STAGE.exists(), 'resume audit already exists'
    STAGE.mkdir(mode=0o700)
    for rel in ['config/cta_perp.json', 'run/cta_perp_position_flags.json', 'run/cta_perp_status.json']:
        shutil.copy2(SIGNAL / rel, STAGE / (pathlib.Path(rel).name + '.before'))
    save('protected_processes.before.json', [p for p in pids() if p['cwd'] not in [str(SIGNAL), str(EXEC)]])
    save('account.before.json', journal_positions())
    query = "SELECT json_build_object('bindings',(SELECT json_agg(x) FROM (SELECT source_id,binding_name,position_strategy_name,order_strategy_name,shares FROM cta_account_strategy_bindings WHERE source_id='binance_exec_trade07')x),'virtual',(SELECT json_agg(x) FROM (SELECT virtual_id,binding_name,position_strategy_name,order_strategy_name,shares FROM cta_virtual_account_bindings WHERE virtual_id='virtual04')x),'positions',(SELECT json_agg(x) FROM (SELECT strategy_name,targets,symbol_order_strategy_overrides,created_by_user_id,publish_token_hash IS NOT NULL AS has_token FROM cta_position_strategies WHERE strategy_name LIKE 'binance_cta_perp_rx03_%')x));"
    save('catalog.before.json', json.loads(sql(query)))
    redis = redis_client()
    save('runtime.before.json', {key: json.loads(redis.get(key)) for key in redis.scan_iter(match=EXEC.name + ':binance-futures:batch_exec:*')})
    subprocess.run([str(SIGNAL / 'scripts/start_cta_perp_signal.sh')], cwd=SIGNAL, check=True)
    print(json.dumps({'preview_started': True, 'publication_enabled': False, 'pre_trade_stopped': True, 'audit_dir': str(STAGE)}))


def inspect():
    current = status()
    cfg = config()
    active = {(row['因子'], row['标的']) for row in cfg['factor_rules']}
    rows = []
    for factor, symbol in sorted(active):
        row = current['strategies'].get(factor, {}).get(symbol, {})
        rows.append({'factor': factor, 'symbol': symbol, 'qty': row.get('position_qty'), 'ready': row.get('score_ready'), 'model_ts_ms': row.get('model_ts_ms'), 'nq_ready': (row.get('signal') or {}).get('filter_ready')})
    positions = json.loads((SIGNAL / 'run/cta_perp_position_flags.json').read_text())
    retired = [{'factor': f, 'symbol': s, 'qty': p['qty']} for f, symbols in positions.items() for s, p in symbols.items() if (f, s) not in active]
    result = {'checked_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'enabled': cfg['enabled'], 'max_pos_u': cfg['max_pos_u'], 'status_age_seconds': (time.time_ns() // 1000 - current['updated_ts_us']) / 1e6, 'configuration_error': current['configuration_error'], 'publication_error': current['publication_error'], 'recovery_error': current['recovery_error'], 'recovering': current['recovering'], 'active_pairs': rows, 'retired_pairs': retired, 'account': journal_positions()}
    save('preview.json', result)
    print(json.dumps(result, ensure_ascii=False))


def catalog():
    stopped()
    cfg = config()
    assert not cfg['enabled']
    current = status()
    assert time.time_ns() // 1000 - current['updated_ts_us'] < 5_000_000
    assert not current['configuration_error'] and not current['recovery_error'] and not current['recovering']
    before = json.loads((STAGE / 'catalog.before.json').read_text())
    expected_order = {b['order_strategy_name'] for b in before['bindings']}
    assert expected_order == {'lgq_grid_prep'}, 'operator order template changed'
    factors = {row['因子'] for row in cfg['factor_rules']}
    desired_names = {PREFIX + factor for factor in factors}
    token = env_for(SIGNAL)['CTA_PUBLISH_TOKEN']
    with session('lgq') as user_api, session('prc') as admin_api:
        virtual = next(v for v in user_api('/manager/api/catalog/virtual-accounts') if v['virtual_id'] == VIRTUAL)
        assert virtual['name'] == 'cta_perp_rx03' and virtual['can_configure']
        if not (STAGE / 'virtual.before.json').exists():
            save('virtual.before.json', virtual)
        existing = {p['strategy_name']: p for p in user_api('/manager/api/catalog/position-strategies')}
        old_names = {row['position_strategy_name'] for row in before['bindings']}
        assert {row['position_strategy_name'] for row in virtual['bindings']} in [old_names, old_names | desired_names]
        created = []
        for factor in sorted(factors):
            name = PREFIX + factor
            if name in existing:
                continue
            targets = {row['标的']: {'qty': 'clear', 'signal': 0} for row in cfg['factor_rules'] if row['因子'] == factor}
            user_api('/manager/api/catalog/position-strategies', {'strategy_name': name, 'targets': targets})
            admin_api('/manager/api/catalog/position-strategies/' + name + '/publish-token', {'publish_token': token}, 'PUT')
            created.append(name)
        all_names = desired_names | old_names
        # Keep explicit zero targets bound under the original names so retired
        # positions close through their factual strategy ledgers.
        bindings = [{'binding_name': name, 'position_strategy_name': name, 'order_strategy_name': 'lgq_grid_prep', 'shares': 1.0} for name in sorted(all_names)]
        user_api('/manager/api/catalog/virtual-accounts/' + VIRTUAL, {'name': virtual['name'], 'bindings': bindings}, 'PUT')
        account = user_api('/manager/api/catalog/accounts/' + EXEC.name)
        save('account_catalog.after.json', account)
        assert {b['position_strategy_name'] for b in account['bindings']} == all_names
        assert all(b['shares'] == 1 and b['order_strategy_name'] == 'lgq_grid_prep' for b in account['bindings'])
        assert account['configuration']['mode'] == 'follow'
        assert account['configuration']['follows'] == [{'virtual_id': VIRTUAL, 'multiplier': 1.0}]
    result = {'created': sorted(desired_names - old_names), 'active_factors': len(factors), 'all_bound_strategies': len(all_names), 'retired_strategies_kept_for_clear': sorted(old_names - desired_names), 'order_strategy': 'lgq_grid_prep', 'max_pos_u': cfg['max_pos_u']}
    save('catalog_result.json', result)
    print(json.dumps(result))


def enable_publication():
    stopped()
    cfg = config()
    assert not cfg['enabled']
    current = status()
    assert time.time_ns() // 1000 - current['updated_ts_us'] < 5_000_000
    assert not current['recovering'] and not current['recovery_error'] and not current['configuration_error']
    active = {(row['因子'], row['标的']) for row in cfg['factor_rules']}
    for factor, symbol in active:
        row = current['strategies'].get(factor, {}).get(symbol, {})
        assert row.get('score_ready'), f'unready factor {factor}/{symbol}'
        assert 0 <= time.time() * 1000 - row.get('model_ts_ms', 0) < 150_000, 'stale factor'
    url = 'http://127.0.0.1:19187/api/config'
    response = json.load(urllib.request.urlopen(url, timeout=10))
    assert response['config'] == cfg
    cfg['enabled'] = True
    request = urllib.request.Request(url, data=json.dumps({'config': cfg, 'expected_revision': response['revision']}).encode(), headers={'Content-Type': 'application/json'}, method='PUT')
    result = json.load(urllib.request.urlopen(request, timeout=10))
    assert result['config'] == cfg
    save('enabled_config.json', result)
    print(json.dumps({'publication_enabled': True, 'execution_still_stopped': True, 'max_pos_u': cfg['max_pos_u']}))


def check_targets():
    current = status()
    assert current['enabled'] and not current['publication_error'] and not current['configuration_error']
    assert current['journal_healthy'] and not current['recovering'] and not current['recovery_error']
    redis = redis_client()
    prefix = EXEC.name + ':binance-futures:'
    names = json.loads(redis.get(prefix + 'batch_exec:strategy_names'))
    shadow = json.loads((SIGNAL / 'run/cta_perp_position_flags.json').read_text())
    active = {(row['因子'], row['标的']) for row in config()['factor_rules']}
    results = []
    for factor, symbols in shadow.items():
        name = PREFIX + factor
        assert name in names, 'missing runtime strategy ' + name
        runtime = json.loads(redis.get(prefix + 'batch_exec:' + name))
        assert runtime['algorithm'] == 'grid'
        for symbol, position in symbols.items():
            target = runtime['targets'].get(symbol)
            assert target is not None, 'missing runtime target ' + name + '/' + symbol
            qty = target['qty']
            if position['qty'] == 0:
                assert qty == 'clear', 'zero target lost explicit clear'
            else:
                assert abs(float(qty) - position['qty']) < 1e-10
            if (factor, symbol) not in active:
                assert qty == 'clear', 'retired target is not clear'
            results.append({'factor': factor, 'symbol': symbol, 'qty': qty, 'active': (factor, symbol) in active})
    with session('lgq') as api:
        account = api('/manager/api/catalog/accounts/' + EXEC.name)
        assert not account['pending_publishes'], 'pending Manager publishes'
    result = {'checked_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'strategies': len(names), 'targets': results, 'publication_error': current['publication_error']}
    save('verified_targets.json', result)
    print(json.dumps(result, ensure_ascii=False))


def start_execution():
    stopped()
    check_targets()
    redis = redis_client()
    rules = json.loads(redis.get(EXEC.name + ':binance-futures:market_rules'))
    assert time.time_ns() // 1000 - rules['fetched_at_us'] < 180_000_000
    assert all(rules['symbols'].get(row['标的'], {}).get('status') == 'TRADING' for row in config()['factor_rules'])
    subprocess.run([str(EXEC / 'scripts/start_exec_pre_trade.sh'), '--venue', 'binance-futures'], cwd=EXEC, check=True)
    save('execution_started.json', {'started_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'scope': EXEC.name, 'venue': 'binance-futures'})


def verify():
    current = status()
    cfg = config()
    before = json.loads((STAGE / 'cta_perp.json.before').read_text())
    assert cfg == {**before, 'enabled': True}, 'operator settings changed during resume'
    assert current['enabled'] and current['journal_healthy']
    assert not any(current[k] for k in ['configuration_error', 'publication_error', 'recovery_error', 'recovering'])
    assert time.time_ns() // 1000 - current['updated_ts_us'] < 5_000_000
    pairs = [(r['因子'], r['标的']) for r in cfg['factor_rules']]
    assert len(pairs) == 45 and all(current['strategies'][f][s]['score_ready'] for f, s in pairs)
    data = json.load(urllib.request.urlopen('http://127.0.0.1:10047/snapshot', timeout=10))
    state = next(e for e in data['entries'] if e['channel'] == 'exec_pre_trade_state')
    assert state['entry']['position_ready'] and time.time() * 1000 - state['ts_ms'] < 5_000
    active = set(pairs)
    retired = {(p['factor'], p['symbol']) for p in json.loads((STAGE / 'verified_targets.json').read_text())['targets'] if not p['active']}
    for row in state['entry']['rows']:
        factor = row['strategy_name'].removeprefix(PREFIX)
        if (factor, row['symbol']) in retired:
            assert row['target_qty'] == 0
    account = journal_positions()
    finance_path = next((EXEC / 'data/rapidx_account').glob('*/latest_financial.json'))
    finance = json.loads(finance_path.read_text())
    assert finance['recovery_ready'] and finance['execution_persistence_pending'] == 0 and finance['statement_persistence_pending'] == 0
    assert time.time() - finance_path.stat().st_mtime < 15
    processes = pids(SIGNAL) + pids(EXEC)
    masks = {}
    for row in processes:
        masks[row['pid']] = sorted({next(line.split(':', 1)[1].strip() for line in task.joinpath('status').read_text().splitlines() if line.startswith('Cpus_allowed_list:')) for task in pathlib.Path('/proc', str(row['pid']), 'task').iterdir()})
        assert masks[row['pid']] == (['19'] if row['comm'] == 'cta_perp_factor' else ['0-5,48-95'])
    assert len(pids(SIGNAL)) == 4 and len(pids(EXEC)) == 6
    before_processes = json.loads((STAGE / 'protected_processes.before.json').read_text())
    after = {p['pid']: p for p in pids()}
    protected = [p for p in before_processes if p['comm'] not in ['bash', 'sshd', 'sshd-session', 'sh', 'python3']]
    changed = [p for p in protected if p['pid'] not in after or p['start'] != after[p['pid']]['start']]
    with session('lgq') as api:
        manager = api('/manager/api/catalog/accounts/' + EXEC.name)
        assert len(manager['bindings']) == 28 and not manager['pending_publishes']
    old_symbols = sorted({r['symbol'] for r in json.loads((STAGE / 'account.before.json').read_text())['positions']} - {r['标的'] for r in cfg['factor_rules']})
    positions = {p['symbol']: p for p in account['positions']}
    result = {'checked_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'enabled': True, 'max_pos_u': cfg['max_pos_u'], 'ready_pairs': len(pairs), 'active_factors': len({f for f, _ in pairs}), 'manager_bindings': len(manager['bindings']), 'retired_clear_pairs': len(retired), 'manager_pending': manager['pending_publishes'], 'execution_position_ready': True, 'account_recovery_ready': finance['recovery_ready'], 'persistence_pending': [finance['execution_persistence_pending'], finance['statement_persistence_pending']], 'processes': processes, 'cpu_masks': masks, 'protected_processes_checked': len(protected), 'protected_processes_changed': changed, 'old_symbols_remaining': {symbol: positions.get(symbol, {'qty': '0', 'value_usdt': '0'}) for symbol in old_symbols}, 'live_order_rows': sum(bool(r.get('live_order_qty')) for r in state['entry']['rows']), 'execution_state': state, 'account': account}
    save('validation.json', result)
    print(json.dumps({k: v for k, v in result.items() if k not in ['processes', 'cpu_masks', 'execution_state', 'account']}, ensure_ascii=False))


if __name__ == '__main__':
    {'preview': preview, 'inspect': inspect, 'catalog': catalog, 'enable-publication': enable_publication, 'check-targets': check_targets, 'start-execution': start_execution, 'verify': verify}[sys.argv[1]]()
