#!/usr/bin/env python3
"""Exercise the real Nginx gateway with disposable auth/editor upstreams."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import shutil
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request

root = Path(__file__).resolve().parent
checks = []


class Upstream(BaseHTTPRequestHandler):
    def log_message(self, *unused):
        pass

    def respond(self):
        if self.path == '/auth/verify-login':
            authorized = 'delist_session=fixture' in self.headers.get('Cookie', '')
            self.send_response(204 if authorized else 401)
            if authorized:
                self.send_header('X-Parameter-Actor', 'fixture_delist')
            self.end_headers()
            return
        if self.path == '/api/auth/verify':
            self.send_response(204 if 'cta_session=fixture' in self.headers.get('Cookie', '') else 401)
            self.end_headers()
            return
        body = self.rfile.read(int(self.headers.get('Content-Length', '0')))
        value = json.dumps({'path': self.path, 'actor': self.headers.get('X-Parameter-Actor'),
                            'method': self.command, 'body': body.decode()}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(value)))
        self.end_headers()
        self.wfile.write(value)

    do_GET = respond
    do_POST = respond
    do_DELETE = respond


upstream = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
threading.Thread(target=upstream.serve_forever, daemon=True).start()
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
origin = f'http://127.0.0.1:{port}'
probe = root / 'nginx-probe'
probe.mkdir(exist_ok=True)
snippet = (root / 'cta_factor_config.conf').read_text()
for value in [8787, 18201, 19220]:
    snippet = snippet.replace(f'http://127.0.0.1:{value}', f'http://127.0.0.1:{upstream.server_port}')
(probe / 'locations.conf').write_text(snippet)
(probe / 'nginx.conf').write_text(f'''daemon off;
pid {probe}/nginx.pid;
error_log {probe}/error.log;
events {{ worker_connections 32; }}
http {{ access_log off; client_body_temp_path {probe}/body;
server {{ listen 127.0.0.1:{port}; include {probe}/locations.conf; }} }}
''')
nginx = shutil.which('nginx') or '/usr/sbin/nginx'
subprocess.run([nginx, '-t', '-p', str(probe), '-c', str(probe / 'nginx.conf')], check=True)
process = subprocess.Popen([nginx, '-p', str(probe), '-c', str(probe / 'nginx.conf')])
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def request(path, cookie='', method='GET', request_origin=None, actor='spoofed'):
    headers = {'Cookie': cookie, 'X-Parameter-Actor': actor}
    if request_origin is not None:
        headers['Origin'] = request_origin
    req = urllib.request.Request(origin+path, headers=headers, method=method,
                                 data=b'{}' if method == 'POST' else None)
    try:
        response = opener.open(req, timeout=5)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        data = response.read()
        return response.status, json.loads(data) if response.headers.get('Content-Type') == 'application/json' else None


try:
    for _ in range(100):
        try:
            request('/missing')
            break
        except OSError:
            time.sleep(.05)
    new = '/delist/cta/config/'
    old = '/cta/cta-prep-binance-futures/config/'
    for path in [new, new+'api/config?worker=cta-factor-spread']:
        status, data = request(path, 'delist_session=fixture')
        assert status == 200 and data['actor'] == 'fixture_delist'
        assert data['path'] == '/'+path.removeprefix(new)
        checks.append({'path': path, 'delist_login_status': status, 'actor_verified': True})
    assert request(new)[0] == 401
    assert request(new, 'cta_session=fixture')[0] == 401
    assert request(old, 'delist_session=fixture')[0] == 401
    assert request(old, 'cta_session=fixture')[0] == 200
    assert request('/delist-auth/cta-factor-config', 'delist_session=fixture')[0] == 404
    for request_origin in [None, 'https://other.example']:
        assert request(new+'api/preview', 'delist_session=fixture', 'POST', request_origin)[0] == 403
    status, data = request(new+'api/preview', 'delist_session=fixture', 'POST', origin)
    assert status == 200 and data['actor'] == 'fixture_delist' and data['body'] == '{}'
    report = {'isolated_nginx': True, 'checks': checks, 'anonymous_denied': True,
              'manager_flow_preserved': True, 'cross_origin_and_missing_origin_denied': True,
              'spoofed_actor_overwritten': True, 'internal_verifier_hidden': True}
    (root / 'gateway_tests.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report))
finally:
    process.terminate()
    process.wait(timeout=5)
    upstream.shutdown()
