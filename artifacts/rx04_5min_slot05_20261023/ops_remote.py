"""Prepare RX04 in retired slot 05 without enabling trading. Never print secrets."""
import contextlib
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import shutil
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path('/home/ubuntu')
MANAGER = ROOT / 'crypto_cta_manager'
OLD = ROOT / 'binance_exec_trade05'
EXEC = ROOT / 'binance_exec_rx04_5m05'
SIGNAL = ROOT / 'binance-cta-perp-rx04'
STAGE = SIGNAL / 'run/slot05_20261023'
PREFIX = 'binance_cta_perp_rx04_'
ALIAS = 'RX04 5min'
ORIGIN = 'http://127.0.0.1:4191'


def save(path, value):
    path = Path(path)
    with path.open('x') as handle:
        os.chmod(path, 0o600)
        json.dump(value, handle, indent=2)
        handle.write('\n')


def env_for(path):
    raw = subprocess.check_output(['bash', '-c', 'source "$1/env.sh"; env -0', 'bash', str(path)])
    return dict(x.decode().split('=', 1) for x in raw.split(b'\0') if b'=' in x)


def sql(query):
    config = tomllib.loads((MANAGER / 'config/cta-manager.toml').read_text())
    pid = subprocess.check_output(['systemctl', '--user', 'show', 'crypto-cta-manager-web.service', '-p', 'MainPID', '--value'], text=True).strip()
    values = dict(x.decode().split('=', 1) for x in Path(f'/proc/{pid}/environ').read_bytes().split(b'\0') if b'=' in x)
    url = urllib.parse.urlsplit(values[config['database'].get('url_env', 'CRYPTO_CTA_LOCAL_DATABASE_URL')])
    env = dict(os.environ, PGHOST=url.hostname, PGPORT=str(url.port or 5432), PGUSER=urllib.parse.unquote(url.username), PGPASSWORD=urllib.parse.unquote(url.password or ''), PGDATABASE=url.path[1:])
    result = subprocess.run(['psql', '-X', '-q', '-A', '-t', '-v', 'ON_ERROR_STOP=1'], input=query, text=True, capture_output=True, env=env)
    if result.returncode:
        raise RuntimeError('database request failed; details suppressed')
    return result.stdout.strip()


@contextlib.contextmanager
def session(username='prc'):
    assert username in ('prc', 'lgq')
    token = secrets.token_urlsafe(32)
    digest = hashlib.sha256(token.encode()).hexdigest()
    sid = sql(f"INSERT INTO cta_auth_sessions(user_id,token_hash,expires_at) SELECT user_id,decode('{digest}','hex'),now()+interval '20 minutes' FROM cta_users WHERE username='{username}' AND disabled=false RETURNING session_id;")
    assert sid.isdigit()
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def api(path, payload=None, method=None):
        request = urllib.request.Request(ORIGIN + path, data=None if payload is None else json.dumps(payload).encode(), headers={'Cookie': 'cta_session=' + token, 'Content-Type': 'application/json'}, method=method)
        try:
            with opener.open(request, timeout=170) as response:
                raw = response.read()
                return json.loads(raw) if raw and 'json' in response.headers.get('Content-Type', '') else {'status': response.status, 'bytes': len(raw)}
        except urllib.error.HTTPError as exc:
            # Error payloads for catalog requests contain no credential material.
            raise RuntimeError(f'{path}: HTTP {exc.code}: {exc.read().decode()[:500]}') from None

    try:
        yield api
    finally:
        sql(f"DELETE FROM cta_auth_sessions WHERE session_id={sid} AND token_hash=decode('{digest}','hex');")


def processes():
    result = []
    for path in Path('/proc').glob('[0-9]*'):
        try:
            cwd = path.joinpath('cwd').resolve()
            if cwd.parent == ROOT and (cwd.name.startswith(('binance', 'okex'))):
                result.append({'pid': int(path.name), 'cwd': str(cwd), 'comm': path.joinpath('comm').read_text().strip(), 'start': path.joinpath('stat').read_text().split()[21]})
        except OSError:
            pass
    return sorted(result, key=lambda row: row['pid'])


def stopped():
    assert not any(row['cwd'] in (str(OLD), str(EXEC)) for row in processes())
    config = json.loads((SIGNAL / 'config/cta_perp.json').read_text())
    assert config['enabled'] is False and config['bar_interval'] == '5m'


def inspect():
    stopped()
    with session() as api:
        result = {
            'old_account': {
                'bindings': json.loads(sql("SELECT COALESCE(json_agg(row_to_json(t)),'[]'::json) FROM cta_account_strategy_bindings t WHERE source_id='binance_exec_trade05';")),
                'configuration': json.loads(sql("SELECT COALESCE(json_agg(row_to_json(t)),'[]'::json) FROM cta_account_follows t WHERE source_id='binance_exec_trade05';")),
            },
            'rx02_account': api('/manager/api/catalog/accounts/binance_exec_trade08'),
            'virtuals': api('/manager/api/catalog/virtual-accounts'),
            'orders': [row for row in api('/manager/api/catalog/order-strategies') if row['strategy_name'] in ('lgq_cta_ctaker', 'lgq_grid_prep')],
            'users': json.loads(sql("SELECT json_agg(row_to_json(t)) FROM (SELECT user_id,username FROM cta_users WHERE username IN ('prc','lgq')) t;")),
            'processes': processes(),
        }
    save(STAGE / 'preflight.json', result)
    print(json.dumps({'old_bindings': len(result['old_account']['bindings']), 'old_configuration': result['old_account']['configuration'], 'rx02_configuration': result['rx02_account']['configuration'], 'virtuals': [{'id': row['virtual_id'], 'name': row['name']} for row in result['virtuals']], 'order_templates': [row['strategy_name'] for row in result['orders']], 'users': result['users']}))


def capture_protected():
    files = {}
    for folder in ROOT.glob('binance*'):
        if not folder.is_dir() or folder == EXEC:
            continue
        for path in [folder / 'env.sh', folder / 'trade_engine.toml', *folder.glob('config/cta*.json')]:
            if path.is_file() and path != SIGNAL / 'env.sh':
                files[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    return {'files': files, 'processes': processes()}


def provision():
    stopped()
    assert not EXEC.exists()
    keys = json.loads(sys.stdin.readline())
    assert re.fullmatch(r'[A-Fa-f0-9]{32}', keys['key'])
    assert re.fullmatch(r'[A-Fa-f0-9]{32}', keys['secret'])
    # Authenticate from the existing trading source IP before storing credentials.
    import http.client
    import hmac
    ip = tomllib.loads((OLD / 'trade_engine.toml').read_text())['local_ips']
    assert ip == ['172.31.35.228']
    now = time.time_ns()
    nonce = str(now // 1_000_000_000)
    headers = {'X-MBX-APIKEY': keys['key'], 'nonce': nonce, 'ts': str(now // 1000), 'signature': hmac.new(keys['secret'].encode(), ('&' + nonce).encode(), hashlib.sha256).hexdigest()}
    conn = http.client.HTTPSConnection('api.liquiditytech.com', timeout=15, source_address=(ip[0], 0))
    conn.request('GET', '/api/v1/trading/account', headers=headers)
    response = conn.getresponse()
    data = json.loads(response.read())
    conn.close()
    assert response.status == 200 and data.get('code') == 200000
    account = [r for r in data['data'] if r.get('exchangeType') == 'BINANCE']
    assert len(account) == 1 and account[0]['positionMode'] == 'NET' and account[0]['accountStatus'] == 'NORMAL'
    portfolio = str(account[0]['portfolioId'])
    assert re.fullmatch(r'[0-9]{1,64}', portfolio)
    for folder in ROOT.glob('binance_exec_*'):
        if (folder / 'env.sh').exists():
            assert env_for(folder).get('LTP_PORTFOLIO_ID') != portfolio, 'new portfolio already belongs to an Exec'
    save(STAGE / 'protected_before.json', capture_protected())
    for path, name in [(MANAGER / 'config/cta-manager.toml', 'manager.before.toml'), (Path('/etc/nginx/snippets/crypto_cta_manager.conf'), 'nginx.before.conf'), (SIGNAL / 'env.sh', 'signal.before.env')]:
        shutil.copy2(path, STAGE / name)
        os.chmod(STAGE / name, 0o600)
    EXEC.mkdir(mode=0o700)
    for name in ['scripts', 'config', 'run', 'logs', 'data']:
        (EXEC / name).mkdir()
    old = env_for(OLD)
    allowed = ['TRADE_SIGNAL_ENABLE_QUEUE_POSITION', 'OPEN_VENUE', 'HEDGE_VENUE', 'TRADE_ENGINE_EXEC_BACKEND_MAP', 'RAPIDX_BINANCE_CASH_BUSINESS_TYPE', 'RUST_LOG', 'ACCOUNT_MONITOR_CORE', 'PERSIST_MANAGER_CORE', 'EXEC_VENUE', 'VENUE', 'EXEC_CONFIG_RELOAD_MS', 'enable_ipc_fast_poll', 'ENABLE_IPC_FAST_POLL']
    settings = {k: old[k] for k in allowed if k in old}
    settings.update(IPC_NAMESPACE=EXEC.name, EXEC_INSTANCE='05', EXEC_VIZ_PORT='10045', EXEC_CONFIG_PORT='18165', PYTHON_BIN='/usr/bin/python3', PRE_TRADE_NOTIFICATION_URL='', LTP_API_KEY=keys['key'], LTP_API_SECRET=keys['secret'], LTP_PORTFOLIO_ID=portfolio)
    with (EXEC / 'env.sh').open('x') as handle:
        os.chmod(EXEC / 'env.sh', 0o600)
        handle.write('#!/usr/bin/env bash\n# RX04 5min, slot 05. Trading intentionally not started.\n')
        for key, value in settings.items():
            handle.write('export ' + key + '=' + shlex.quote(value) + '\n')
    for name in ['trade_engine.toml', 'config/iceoryx2.toml', 'config/persist_sync_distribution.toml', 'config/exec_viz.toml', 'config/exec_config_server.env']:
        text = (OLD / name).read_text().replace(OLD.name, EXEC.name)
        (EXEC / name).write_text(text)
    save(STAGE / 'provisioned.json', {'environment': EXEC.name, 'source_id': EXEC.name, 'alias': ALIAS, 'portfolio_id': portfolio, 'old_portfolio_preserved': old['LTP_PORTFOLIO_ID'], 'source_ip': ip[0], 'gateway': '/exec_trade05', 'trading_started': False, 'signal_enabled': False})
    print(json.dumps({'provisioned': EXEC.name, 'credentials_verified': True, 'new_ledger': True, 'trading_started': False}))


def configure_source():
    stopped()
    cfgpath = MANAGER / 'config/cta-manager.toml'
    nginxpath = Path('/etc/nginx/snippets/crypto_cta_manager.conf')
    assert cfgpath.read_bytes() == (STAGE / 'manager.before.toml').read_bytes()
    assert nginxpath.read_bytes() == (STAGE / 'nginx.before.conf').read_bytes()
    before = tomllib.loads(cfgpath.read_text())
    old = next(row for row in before['sources'] if row['id'] == OLD.name)
    assert old['enabled'] is False and old['gateway_prefix'] == '/exec_trade05'
    assert not any(row['id'] == EXEC.name for row in before['sources'])
    blocks = re.split(r'(?=^\[\[sources\]\]\s*$)', cfgpath.read_text(), flags=re.M)
    changed = 0
    for index, block in enumerate(blocks):
        if re.search(r'^id\s*=\s*"' + re.escape(OLD.name) + r'"\s*$', block, flags=re.M):
            # The old source keeps its factual archive and identity, with no live gateway.
            blocks[index] = re.sub(r'^(?:gateway_prefix|exec_config_url|exec_viz_url)\s*=.*\n', '', block, flags=re.M)
            changed += 1
    assert changed == 1
    source = {key: value for key, value in old.items() if key in ['venue', 'maker_fee_rate', 'taker_fee_rate', 'theoretical_twap_fee_rate', 'estimated_fee_rate', 'quote_fee_rates']}
    assert all(not isinstance(value, dict) for value in source.values()), 'fee map needs explicit TOML handling'
    source.update(id=EXEC.name, account='trade05', alias=ALIAS, rocksdb_path=str(EXEC / 'data/persist_manager'), enabled=True, monitor_enabled=False, gateway_prefix='/exec_trade05', exec_config_url='http://127.0.0.1:18165/', exec_viz_url='http://127.0.0.1:10045/', ipc_namespace=EXEC.name, account_ipc_service='account_pubs/rapidx_binance_' + env_for(EXEC)['LTP_PORTFOLIO_ID'] + '_pm')
    text = ''.join(blocks)
    old_path = json.dumps(str(OLD / 'trade_engine.toml'))
    assert text.count(old_path) >= 1
    text = text.replace(old_path, old_path + ',\n    ' + json.dumps(str(EXEC / 'trade_engine.toml')), 1)
    # Registration is enabled for configuration only; no Exec process is started.
    text += '\n# RX04 5min: reused gateway slot 05, fresh account ledger; trading stopped.\n[[sources]]\n'
    text += '\n'.join(key + ' = ' + json.dumps(value, ensure_ascii=False) for key, value in source.items()) + '\n'
    parsed = tomllib.loads(text)
    assert len(parsed['sources']) == len(before['sources']) + 1
    assert set(parsed['kline']['trade_engine_configs']) == set(before['kline']['trade_engine_configs']) | {str(EXEC / 'trade_engine.toml')}
    for row in before['sources']:
        expected = dict(row)
        if row['id'] == OLD.name:
            for key in ['gateway_prefix', 'exec_config_url', 'exec_viz_url']:
                expected.pop(key, None)
        assert next(s for s in parsed['sources'] if s['id'] == row['id']) == expected
    candidate = STAGE / 'manager.after.toml'
    candidate.write_text(text)
    os.chmod(candidate, 0o600)
    marker = 'proxy_set_header X-CTA-Source-ID ' + OLD.name + ';'
    nginx = nginxpath.read_text()
    assert nginx.count(marker) == 1
    nginx = nginx.replace(marker, 'proxy_set_header X-CTA-Source-ID ' + EXEC.name + ';')
    nginx = nginx.replace('# RX02 standard Exec trade05;', '# RX04 5min Exec slot 05;')
    (STAGE / 'nginx.after.conf').write_text(nginx)
    for command in [
        ['sudo', '-n', 'install', '-m', '644', str(STAGE / 'nginx.after.conf'), str(nginxpath) + '.next'],
        ['sudo', '-n', 'mv', str(nginxpath) + '.next', str(nginxpath)],
        ['sudo', '-n', 'nginx', '-t'],
    ]:
        subprocess.run(command, check=True)
    shutil.copy2(candidate, str(cfgpath) + '.next')
    os.replace(str(cfgpath) + '.next', cfgpath)
    subprocess.run(['sudo', '-n', 'systemctl', 'reload', 'nginx'], check=True)
    subprocess.run(['systemctl', '--user', 'restart', 'crypto-cta-manager-web.service'], check=True)
    save(STAGE / 'source_configured.json', {'source': source, 'old_source_preserved': True, 'gateway_reused': True, 'exec_processes_started': False})
    print(json.dumps({'source_registered': EXEC.name, 'alias': ALIAS, 'exec_processes_started': False}))


def catalog():
    stopped()
    table = json.loads((SIGNAL / 'config/cta_perp_symbol_factors.json').read_text())
    factors = sorted({factor for row in table.values() for factor in row})
    assert len(table) == 15 and len(factors) == 35 and sum(map(len, table.values())) == 48
    assert (SIGNAL / 'env.sh').read_bytes() == (STAGE / 'signal.before.env').read_bytes()
    token = secrets.token_urlsafe(32)
    bindings = []
    with session() as api:
        account = api('/manager/api/catalog/accounts/' + EXEC.name)
        assert not account['bindings'] and account['configuration']['mode'] == 'independent'
        existing = api('/manager/api/catalog/position-strategies')
        assert not any(row['strategy_name'].startswith(PREFIX) for row in existing)
        params = next(row['order_parameters'] for row in api('/manager/api/catalog/order-strategies') if row['strategy_name'] == 'lgq_cta_ctaker')
        order_name = PREFIX + 'ctaker'
        api('/manager/api/catalog/order-strategies', {'strategy_name': order_name, 'order_parameters': params})
        for factor in factors:
            name = PREFIX + factor
            targets = {symbol: {'qty': 0.0, 'signal': 0} for symbol, row in table.items() if factor in row}
            api('/manager/api/catalog/position-strategies', {'strategy_name': name, 'targets': targets})
            api('/manager/api/catalog/position-strategies/' + name + '/managers', {'user_ids': [3]}, 'PUT')
            api('/manager/api/catalog/position-strategies/' + name + '/publish-token', {'publish_token': token}, 'PUT')
            bindings.append({'binding_name': name, 'position_strategy_name': name, 'order_strategy_name': order_name, 'shares': 1.0})
        virtual = api('/manager/api/catalog/virtual-accounts', {'name': ALIAS, 'bindings': bindings})
        vid = virtual['virtual_id']
        api('/manager/api/catalog/virtual-accounts/' + vid + '/grants', {'user_ids': [3]}, 'PUT')
        api('/manager/api/catalog/accounts/' + EXEC.name + '/grants', {'grants': [{'user_id': 3, 'access_level': 'configure'}]}, 'PUT')
        configuration = api('/manager/api/catalog/accounts/' + EXEC.name + '/configuration', {'mode': 'follow', 'follows': [{'virtual_id': vid, 'multiplier': 0.0}]}, 'PUT')
        save(STAGE / 'catalog_configured.json', {'virtual': virtual, 'account': configuration, 'order_parameters': params, 'trading_enabled': False})
    values = {'CTA_MANAGER_API_URL': ORIGIN + '/manager/api', 'CTA_FOLLOWER_SOURCE_ID': EXEC.name, 'CTA_VIRTUAL_ACCOUNT_NAME': ALIAS, 'CTA_PUBLISH_TOKEN': token}
    text = (SIGNAL / 'env.sh').read_text()
    for key, value in values.items():
        line = 'export ' + key + '=' + shlex.quote(value)
        pattern = r'^(?:export\s+)?' + key + r'=.*$'
        if re.search(pattern, text, flags=re.M):
            text = re.sub(pattern, lambda match: line, text, flags=re.M)
        else:
            text += '\n' + line + '\n'
    candidate = SIGNAL / 'env.sh.next'
    candidate.write_text(text)
    os.chmod(candidate, 0o600)
    os.replace(candidate, SIGNAL / 'env.sh')
    # Only reload the already-running observation signal. enabled=false is unchanged.
    subprocess.run([str(SIGNAL / 'scripts/start_cta_perp_5m_signal.sh')], cwd=SIGNAL, check=True)
    print(json.dumps({'virtual_id': vid, 'name': ALIAS, 'factors': len(factors), 'symbols': len(table), 'multiplier': 0, 'signal_enabled': False, 'exec_processes_started': False}))


def verify():
    stopped()
    config = tomllib.loads((MANAGER / 'config/cta-manager.toml').read_text())
    source = next(row for row in config['sources'] if row['id'] == EXEC.name)
    old = next(row for row in config['sources'] if row['id'] == OLD.name)
    assert source['alias'] == ALIAS and source['monitor_enabled'] is False
    assert source['gateway_prefix'] == '/exec_trade05'
    assert old['enabled'] is False and 'gateway_prefix' not in old
    assert old['rocksdb_path'] == str(OLD / 'data/persist_manager')
    before = json.loads((STAGE / 'protected_before.json').read_text())
    now = capture_protected()
    assert before['files'] == now['files'], 'protected environment config changed'
    protected_before = [row for row in before['processes'] if not (row['cwd'] == str(SIGNAL) and row['comm'] == 'cta_perp_5m_sig')]
    protected_now = [row for row in now['processes'] if not (row['cwd'] == str(SIGNAL) and row['comm'] == 'cta_perp_5m_sig')]
    assert protected_before == protected_now, 'protected process identity changed'
    status = json.loads((SIGNAL / 'run/cta_perp_status.json').read_text())
    assert status['enabled'] is False and status['bar_interval'] == '5m'
    assert status['follower_source_id'] == EXEC.name and status['virtual_account_name'] == ALIAS
    assert status['publication_error'] is None
    recorded = json.loads((STAGE / 'catalog_configured.json').read_text())
    vid = recorded['virtual']['virtual_id']
    with session('lgq') as api:
        account = api('/manager/api/catalog/accounts/' + EXEC.name)
        assert account['configuration'] == {'mode': 'follow', 'follows': [{'virtual_id': vid, 'multiplier': 0.0}]}
        assert len(account['bindings']) == 35 and all(row['shares'] == 0 for row in account['bindings'])
        assert not account['pending_publishes'], 'pending disabled target publication'
        virtual = next(row for row in api('/manager/api/catalog/virtual-accounts') if row['virtual_id'] == vid)
        assert virtual['name'] == ALIAS and len(virtual['bindings']) == 35
        positions = [row for row in api('/manager/api/catalog/position-strategies') if row['strategy_name'].startswith(PREFIX)]
        assert len(positions) == 35
        assert all(target['qty'] in (0, 'clear') for row in positions for target in row['targets'].values())
        dashboard = api('/manager/api/dashboard')
        rows = dashboard['accounts']
        assert next(row for row in rows if row['source_id'] == EXEC.name)['account'] == ALIAS
        rx02 = api('/manager/api/catalog/accounts/binance_exec_trade08')
        original_rx02 = json.loads((STAGE / 'preflight.json').read_text())['rx02_account']
        assert rx02['configuration'] == original_rx02['configuration']
        assert rx02['bindings'] == original_rx02['bindings']
    import redis
    client = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
    names = json.loads(client.get(EXEC.name + ':binance-futures:batch_exec:strategy_names'))
    assert len(names) == 35 and all(name.startswith(PREFIX) for name in names)
    for name in names:
        runtime = json.loads(client.get(EXEC.name + ':binance-futures:batch_exec:' + name))
        assert all(target['qty'] in (0, 'clear') for target in runtime['targets'].values())
    # No Exec process or listener is started as part of preparing this account.
    result = {'verified_at_us': time.time_ns() // 1000, 'source_id': EXEC.name, 'alias': ALIAS, 'gateway': '/exec_trade05', 'virtual_id': vid, 'virtual_bindings': 35, 'symbols': 15, 'effective_shares': 0, 'signal_enabled': False, 'bar_interval': '5m', 'pending_publishes': [], 'runtime_targets_all_zero': True, 'exec_processes': [], 'old_exec_processes': [], 'protected_processes_unchanged': len(protected_before), 'protected_config_files_unchanged': len(before['files']), 'old_history_preserved': True, 'rx02_trade08_configuration_unchanged': True, 'signal_status': {key: status.get(key) for key in ['enabled', 'bar_interval', 'virtual_account_name', 'follower_source_id', 'manager_publications', 'publication_error', 'updated_ts_us']}}
    save(STAGE / 'verification.json', result)
    print(json.dumps(result))


if __name__ == '__main__':
    {'inspect': inspect, 'provision': provision, 'configure-source': configure_source, 'catalog': catalog, 'verify': verify}[sys.argv[1]]()
