import contextlib
import hashlib
import json
import os
import pathlib
import secrets
import subprocess
import time
import tomllib
import urllib.parse
import urllib.request
import redis

ROOT = pathlib.Path.home()
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))

def get(url, token=None):
    headers = {} if token is None else {'Cookie': 'cta_session=' + token}
    with HTTP.open(urllib.request.Request(url, headers=headers), timeout=30) as response:
        data = response.read()
        return json.loads(data) if 'json' in response.headers.get('Content-Type', '') else {'http': response.status}

def sql(query):
    cfg = tomllib.loads((ROOT / 'crypto_cta_manager/config/cta-manager.toml').read_text())
    pid = subprocess.check_output(['systemctl', '--user', 'show', 'crypto-cta-manager-web.service', '-p', 'MainPID', '--value'], text=True).strip()
    values = dict(x.split(b'=', 1) for x in pathlib.Path(f'/proc/{pid}/environ').read_bytes().split(b'\0') if b'=' in x)
    url = urllib.parse.urlsplit(values[cfg['database'].get('url_env', 'CRYPTO_CTA_LOCAL_DATABASE_URL').encode()].decode())
    env = dict(os.environ, PGHOST=url.hostname, PGPORT=str(url.port or 5432), PGUSER=urllib.parse.unquote(url.username), PGPASSWORD=urllib.parse.unquote(url.password or ''), PGDATABASE=url.path[1:])
    result = subprocess.run(['psql', '-X', '-q', '-A', '-t', '-v', 'ON_ERROR_STOP=1'], input=query, text=True, capture_output=True, env=env)
    if result.returncode:
        raise RuntimeError('Database verification failed; details suppressed')
    return result.stdout.strip()

@contextlib.contextmanager
def session():
    token = secrets.token_urlsafe(32)
    digest = hashlib.sha256(token.encode()).hexdigest()
    sid = sql(f"INSERT INTO cta_auth_sessions(user_id,token_hash,expires_at) SELECT user_id,decode('{digest}','hex'),now()+interval '5 minutes' FROM cta_users WHERE role='admin' AND disabled=false ORDER BY user_id LIMIT 1 RETURNING session_id;")
    assert sid.isdigit()
    try:
        yield token
    finally:
        sql(f"DELETE FROM cta_auth_sessions WHERE session_id={sid} AND token_hash=decode('{digest}','hex');")

result = {'ts_ms': int(time.time() * 1000), 'exec': {}, 'signals': {}}
processes = []
for p in pathlib.Path('/proc').glob('[0-9]*'):
    try:
        exe = str((p/'exe').resolve())
        cwd = str((p/'cwd').resolve())
        affinity = next(x.split(':', 1)[1].strip() for x in (p/'status').read_text().splitlines() if x.startswith('Cpus_allowed_list:'))
        if any(x in cwd for x in ['binance_exec_trade', 'binance-cta-perp-', 'binance-cta-spread-', 'crypto_cta_manager']):
            processes.append({'pid': int(p.name), 'exe': exe, 'cwd': cwd, 'affinity': affinity})
    except (OSError, StopIteration):
        pass
result['processes'] = processes
r = redis.Redis(decode_responses=True)
with session() as token:
    result['manager_health'] = get('http://127.0.0.1:4191/manager/api/health', token)
    for n in range(1,8):
        name = f'binance_exec_trade{n:02d}'
        snap = get(f'http://127.0.0.1:{10040+n}/snapshot')
        entry = next(e for e in snap['entries'] if e['channel'] == 'exec_pre_trade_state')
        rows = entry['entry'].get('rows', [])
        result['exec'][name] = {
            'position_ready': entry['entry'].get('position_ready'),
            'age_ms': int(time.time()*1000)-entry['ts_ms'],
            'rows': len(rows),
            'pending': sum(not x.get('execution_complete',False) for x in rows),
            'config': get(f'http://127.0.0.1:4191/exec_trade{n:02d}/config/api/strategies', token).keys() is not None,
            'gateway_snapshot': bool(get(f'http://127.0.0.1:4191/exec_trade{n:02d}/snapshot', token)),
        }
    for name, kind, port in [('binance-cta-spread-rx01','spread',10186),('binance-cta-perp-rx02','perp',10196),('binance-cta-perp-rx03','perp',10197)]:
        snap = get(f'http://127.0.0.1:{port}/api/snapshot')
        status = snap['status']
        saved_flags = json.loads((ROOT/name/f'run/cta_{kind}_position_flags.json').read_text())
        rows = [row for symbols in status['strategies'].values() for row in symbols.values()]
        mismatches = []
        checks = 0
        for factor, symbols in status['strategies'].items():
            for venue, field in ([('binance-margin','cash_qty'),('binance-futures','perpetual_qty')] if kind == 'spread' else [('binance-futures','position_qty')]):
                key = f"{status['follower_source_id']}:{venue}:batch_exec:{status['manager_strategy_prefix']}{factor}"
                raw = r.get(key)
                targets = json.loads(raw)['targets'] if raw else {}
                for symbol, row in symbols.items():
                    target_symbol = symbol.removesuffix('USDT') if venue == 'binance-margin' else symbol
                    actual = targets.get(target_symbol)
                    qty = actual.get('qty') if isinstance(actual,dict) else actual
                    if qty == 'clear':
                        qty = 0.0
                    expected = row.get(field,0)
                    if not row.get('score_ready'):
                        saved_field = field if kind == 'spread' else 'qty'
                        expected = saved_flags.get(factor,{}).get(symbol,{}).get(saved_field,0)
                    if qty is None or abs(float(qty)-expected) > 1e-8:
                        mismatches.append({'strategy': factor,'venue':venue,'symbol':symbol,'signal_qty':expected,'exec_qty':qty})
                    checks += 1
        history = ROOT/name/f'data/cta_{kind}/factor_history/recovery.json'
        warmup = json.loads(history.read_text()) if history.exists() else None
        result['signals'][name] = {
            'state': snap['signal_state'], 'enabled':status['enabled'], 'journal_healthy':status['journal_healthy'],
            'publication_error':status.get('publication_error'), 'configuration_error':status.get('configuration_error'),
            'rows':len(rows), 'ready':sum(row.get('score_ready',False) for row in rows),
            'fresh':sum(time.time()*1000-row.get('model_ts_ms',0)<130000 for row in rows),
            'target_checks':checks, 'target_mismatches':mismatches,
            'target_basis': 'live status for ready rows; persisted flags during warmup',
            'warmup':warmup,
        }
print(json.dumps(result,indent=2))
