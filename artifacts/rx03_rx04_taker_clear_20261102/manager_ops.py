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

def env_for(path):
    raw = subprocess.check_output(['bash', '-c', 'source "$1/env.sh"; env -0', 'bash', str(path)])
    return dict((row.decode().split('=', 1) for row in raw.split(b'\x00') if b'=' in row))

def db_env(read_only=False):
    config = tomllib.loads((MANAGER / 'config/cta-manager.toml').read_text())
    pid = subprocess.check_output(['systemctl', '--user', 'show', 'crypto-cta-manager-web.service', '-p', 'MainPID', '--value'], text=True).strip()
    values = dict((row.split(b'=', 1) for row in pathlib.Path('/proc', pid, 'environ').read_bytes().split(b'\x00') if b'=' in row))
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
MANAGER=pathlib.Path.home()/'crypto_cta_manager'
ORIGIN='http://127.0.0.1:4191'
