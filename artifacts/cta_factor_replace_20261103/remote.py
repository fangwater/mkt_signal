"""Install published CTA factor workers and replace legacy configuration routes.

Manager/Exec orders are not started or submitted. Four position switches must
remain disabled; the RX01 account must already be flat and unbound.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import sys
import tarfile
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request

HOME_ROOT = Path.home()
MANAGER = HOME_ROOT / 'crypto_cta_manager'
HELPERS = MANAGER / 'maintenance-backups/rx03_rx04_taker_clear_20261102T144523Z/scripts'
sys.path.insert(0, str(HELPERS))
from manager_ops import env_for, session, sql
from clear_positions import exchange_snapshot, save
from manager_entries import protected_processes
import neutralize_rapidx_binance as n

NEW = ['cta-prep-binance-futures', 'cta-factor-spread']
OLD = ['binance-cta-perp-rx01', 'binance-cta-perp-rx03', 'binance-cta-perp-rx04',
       'okex-binance-cta-spread-rx01']
SITE = Path('/etc/nginx/sites-available/crypto_proxy_4191.conf')
NGINX = Path('/etc/nginx/snippets')
DELIST_WEB = HOME_ROOT / 'delist_risk_server/web/delist_risk/index.html'
STRATEGIES = ['cta-prep-bn-1min-pos', 'cta-prep-bn-5min-pos', 'cta-prep-bn-15min-pos',
              'cta-spread-okx-bn-1min-pos']


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x') as f:
        path.chmod(0o600)
        f.write(text)
        f.flush()
        os.fsync(f.fileno())


def stop_or_start(env, script, extra, audit):
    root = HOME_ROOT / env
    result = subprocess.run([str(root / 'scripts' / script), *extra], cwd=root,
                            capture_output=True, timeout=55)
    (audit / (env + '.' + script + '.log')).write_bytes(result.stdout + result.stderr)
    n.require(result.returncode == 0, 'env-local process script failed; see private audit log')
    print(json.dumps({'env': env, 'script': script, 'completed': True}), flush=True)


def config_documents(audit):
    documents = []
    for worker in NEW:
        directory = 'cta_prep_binance_futures' if worker == NEW[0] else 'cta_factor_spread'
        for path in sorted((audit / 'stage' / worker / 'config' / directory).glob('*.json')):
            doc = json.loads(path.read_text())
            n.require(doc['signal']['enabled'] is False, 'new position table must be disabled')
            frequency = doc['interval']
            name = 'cta-spread-okx-bn-1min-pos' if worker == NEW[1] else f'cta-prep-bn-{frequency}-pos'
            symbols = {row['标的'].upper() for row in doc['factor_rules']}
            n.require(all(s.endswith('USDT') for s in symbols), 'expected explicit USDT symbols')
            targets = {symbol: {'qty': 0, 'signal': 0} for symbol in symbols}
            if worker == NEW[1]:
                targets = {venue + ':' + symbol: value for symbol, value in targets.items()
                           for venue in ('okex-futures', 'binance-futures')}
            documents.append({'worker': worker, 'path': str(path), 'strategy_name': name, 'targets': targets})
    n.require({r['strategy_name'] for r in documents} == set(STRATEGIES), 'expected four independent tables')
    return documents


def confirm_flat():
    n.assert_stopped(HOME_ROOT / 'binance_exec_trade06')
    snap = exchange_snapshot('binance_exec_trade06')
    n.require(not snap['nonzero_positions'] and snap['open_orders'] == snap['algo_orders'] == 0,
              'RX01 is not flat')
    n.require(sql("SELECT count(*) FROM cta_account_strategy_bindings WHERE source_id='binance_exec_trade06'") == '0',
              'RX01 old bindings remain')


def remove_location(text, marker):
    start = text.index(marker)
    brace = text.index('{', start)
    depth = 1
    end = brace + 1
    while depth:
        n.require(end < len(text), 'unterminated nginx location')
        depth += (text[end] == '{') - (text[end] == '}')
        end += 1
    return text[:start] + text[end:]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=['prepare', 'install', 'switch', 'gateway', 'verify'])
    parser.add_argument('--audit', type=Path, required=True)
    parser.add_argument('--payload-sha256')
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    audit = args.audit
    os.umask(0o077)
    n.require(audit.parent == MANAGER / 'maintenance-backups'
              and audit.name.startswith('cta_factor_replace_'), 'unexpected audit path')
    if args.phase in ('install', 'switch', 'gateway'):
        n.require(args.execute, 'mutation requires --execute')
    if args.phase != 'prepare':
        deadline = time.monotonic() + 300
        next_progress = time.monotonic() + 20
        while True:
            try:
                with socket.create_connection(('127.0.0.1', 18201), timeout=1):
                    pass
                break
            except OSError:
                n.require(time.monotonic() < deadline, 'Manager API is not ready')
                if time.monotonic() >= next_progress:
                    print('Waiting for the current Manager reload to expose its API.', flush=True)
                    next_progress = time.monotonic() + 20
                time.sleep(1)
    if args.phase == 'prepare':
        confirm_flat()
        n.require(sha(audit / 'payload.tar') == args.payload_sha256, 'payload SHA-256 mismatch')
        stage = audit / 'stage'
        stage.mkdir()
        with tarfile.open(audit / 'payload.tar') as archive:
            for item in archive.getmembers():
                path = Path(item.name)
                n.require(not path.is_absolute() and '..' not in path.parts and (item.isfile() or item.isdir()),
                          'unsafe payload entry')
            archive.extractall(stage, filter='data')
        manifest = json.loads((stage / 'manifest.json').read_text())
        for relative, expected in manifest['files'].items():
            n.require(sha(stage / relative) == expected, 'published file checksum mismatch')
        config_documents(audit)
        n.require(all(not (HOME_ROOT / name).exists() for name in NEW), 'new runtime already exists')
        excluded = {str(HOME_ROOT / name) for name in OLD}
        save(audit / 'protected.before.json', {k: v for k, v in protected_processes().items() if v['cwd'] not in excluded})
        paths = [SITE, NGINX / 'crypto_cta_manager.conf', DELIST_WEB]
        paths += [NGINX / ('cta_perp_' + rx + '.conf') for rx in ('rx01', 'rx03', 'rx04')]
        backups = {}
        for i, path in enumerate(paths):
            backup = audit / ('original.' + str(i))
            shutil.copy2(path, backup)
            backups[str(path)] = {'backup': str(backup), 'sha256': sha(path)}
        save(audit / 'files.before.json', backups)
        for name in OLD:
            root = HOME_ROOT / name
            shutil.copytree(root / 'config', audit / 'legacy-configs' / name)
        changed = {}
        for rx in ('rx01', 'rx03', 'rx04'):
            path = NGINX / ('cta_perp_' + rx + '.conf')
            legacy = 'binance-cta-perp-' + rx
            changed[str(path)] = (f'location = /cta/{legacy} {{ return 410; }}\n'
                                  f'location ^~ /cta/{legacy}/ {{ return 410; }}\n')
        path = NGINX / 'crypto_cta_manager.conf'
        changed[str(path)] = remove_location(path.read_text(), 'location /cta/okex-binance-cta-spread-rx01/config/ {')
        site = SITE.read_text()
        n.require(site.count('server {') == 1 and 'cta_factor_config.conf' not in site, 'unexpected vhost shape')
        pos = site.rfind('}')
        changed[str(SITE)] = site[:pos] + '    include /etc/nginx/snippets/cta_factor_config.conf;\n' + site[pos:]
        changed[str(NGINX / 'cta_factor_config.conf')] = (stage / 'unified-nginx.conf').read_text()
        html = DELIST_WEB.read_text()
        anchor = '      <section class="exchange-list" id="cta-strategies"'
        n.require(html.count(anchor) == 1 and 'cta-unified-live-nav' not in html, 'unexpected Delist CTA markup')
        navigation = '''      <nav id="cta-unified-live-nav" aria-label="CTA统一配置" style="display:flex;gap:18px;flex-wrap:wrap;margin:12px 0 20px">
        <a href="/cta/cta-prep-binance-futures/config/?worker=cta-prep-binance-futures">CTA Prep：1min / 5min / 15min 配置与仓位表 ↗</a>
        <a href="/cta/cta-prep-binance-futures/config/?worker=cta-factor-spread">CTA Spread：1min 配置与仓位表 ↗</a>
      </nav>
'''
        html = html.replace(anchor, navigation + anchor, 1)
        html = html.replace('</head>', '<style>#cta-strategies{display:none}</style></head>', 1)
        changed[str(DELIST_WEB)] = html
        candidates = {}
        for i, (target, contents) in enumerate(changed.items()):
            path = audit / ('candidate.' + str(i))
            write(path, contents)
            candidates[target] = str(path)
        save(audit / 'candidates.json', candidates)
        current = json.loads(sql("SELECT coalesce(jsonb_agg(strategy_name),'[]'::jsonb) FROM cta_position_strategies WHERE strategy_name IN ('cta-prep-bn-1min-pos','cta-prep-bn-5min-pos','cta-prep-bn-15min-pos','cta-spread-okx-bn-1min-pos')"))
        n.require(not current, 'new strategy identities already exist; review before replacing')
        print(json.dumps({'prepared': True, 'position_tables': 4, 'old_envs': OLD, 'new_envs': NEW}), flush=True)
        return
    manifest = json.loads((audit / 'stage/manifest.json').read_text())
    if args.phase == 'install':
        confirm_flat()
        for worker in NEW:
            source = audit / 'stage' / worker
            destination = HOME_ROOT / worker
            if destination.exists():
                values = env_for(destination)
                token = values.get('CTA_PUBLISH_TOKEN', '')
                n.require(len(token) >= 32 and values.get('CTA_MANAGER_API_URL') == 'http://127.0.0.1:18201/api',
                          'existing runtime is not this prepared installation')
            else:
                shutil.copytree(source, destination)
                token = secrets.token_urlsafe(48)
                env = ("#!/usr/bin/env bash\nexport CTA_MANAGER_API_URL='http://127.0.0.1:18201/api'\n"
                       + 'export CTA_PUBLISH_TOKEN=' + token + '\n'
                       + ("export CTA_PREP_BINANCE_FUTURES_CORE=17\nexport CTA_SPREAD_FACTOR_CONFIG='" + str(HOME_ROOT / NEW[1] / 'config/cta_factor_spread.toml') + "'\n" if worker == NEW[0]
                          else 'export CTA_FACTOR_SPREAD_CORE=18\n')
                       + 'export RUST_LOG=info\n')
                write(destination / 'env.sh', env)
            for relative, expected in manifest['files'].items():
                if relative.startswith(worker + '/'):
                    n.require(sha(HOME_ROOT / relative) == expected, 'runtime file checksum mismatch')
            documents = [r for r in config_documents(audit) if r['worker'] == worker]
            n.require(sql("SELECT count(*) FROM cta_account_strategy_bindings WHERE position_strategy_name IN ('cta-prep-bn-1min-pos','cta-prep-bn-5min-pos','cta-prep-bn-15min-pos','cta-spread-okx-bn-1min-pos')") == '0',
                      'new strategy was bound during installation')
            with session('lgq') as api, session('prc') as token_api:
                for doc in documents:
                    body = {'strategy_name': doc['strategy_name'], 'targets': doc['targets']}
                    result = api('/manager/api/catalog/position-strategies', body, 'POST')
                    n.require(result['strategy_name'] == doc['strategy_name'] and not result['publishes'],
                              'unexpected initial position publication')
                    token_api('/manager/api/catalog/position-strategies/' + doc['strategy_name'] + '/publish-token',
                              {'publish_token': token}, 'PUT')
            print(json.dumps({'installed': worker, 'tables_registered': len(documents), 'all_disabled': True}), flush=True)
        for worker in NEW:
            binary = HOME_ROOT / worker / worker
            config = HOME_ROOT / worker / 'config' / ('cta_prep_binance_futures.toml' if worker == NEW[0] else 'cta_factor_spread.toml')
            checked = subprocess.run([str(binary), '--config', str(config), '--check'], capture_output=True, timeout=30)
            n.require(checked.returncode == 0, 'published worker configuration check failed')
            (audit / (worker + '.checked.json')).write_bytes(checked.stdout)
        print('Runtime artifacts verified; no processes started.', flush=True)
        return
    if args.phase in ('switch', 'gateway'):
        confirm_flat()
        backups = json.loads((audit / 'files.before.json').read_text())
        for target, backup in backups.items():
            n.require(sha(target) == backup['sha256'], 'operator changed a protected gateway file; do not overwrite')
        scripts = {
            OLD[0]: ['stop_cta_perp_15m_signal.sh', 'stop_cta_perp_factor_model_15m_pub.sh', 'stop_cta_perp_config_server.sh', 'stop_cta_perp_dashboard.sh'],
            OLD[1]: ['stop_cta_perp_signal.sh', 'stop_cta_perp_factor_model_1m_pub.sh', 'stop_cta_perp_config_server.sh', 'stop_cta_perp_dashboard.sh'],
            OLD[2]: ['stop_cta_perp_5m_signal.sh', 'stop_cta_perp_factor_model_5m_pub.sh', 'stop_cta_perp_config_server.sh', 'stop_cta_perp_dashboard.sh'],
            OLD[3]: ['stop_cta_cross_spread_signal.sh', 'stop_cta_cross_spread_factor_model_1m_pub.sh', 'stop_cta_cross_spread_config_server.sh'],
        }
        if args.phase == 'switch':
            for env, names in scripts.items():
                for name in names:
                    stop_or_start(env, name, [], audit)
            stop_or_start(NEW[0], 'start_cta_factor_config.sh', ['--execute'], audit)
            stop_or_start(NEW[0], 'start_cta_prep_binance_futures.sh', ['--execute'], audit)
            stop_or_start(NEW[1], 'start_cta_factor_spread.sh', ['--execute'], audit)
        candidates = json.loads((audit / 'candidates.json').read_text())
        site_check = Path(candidates[str(SITE)]).read_text()
        for target, candidate in candidates.items():
            if target.startswith('/etc/nginx/snippets/'):
                site_check = site_check.replace(target, candidate)
        (audit / 'nginx-site-check.conf').write_text(site_check)
        main_check = Path('/etc/nginx/nginx.conf').read_text()
        n.require(main_check.count('include /etc/nginx/sites-enabled/*;') == 1,
                  'unexpected nginx include layout')
        main_check = main_check.replace('include /etc/nginx/sites-enabled/*;',
                                        'include ' + str(audit / 'nginx-site-check.conf') + ';')
        (audit / 'nginx-check.conf').write_text(main_check)
        checked = subprocess.run(['sudo', '-n', 'nginx', '-t', '-c', str(audit / 'nginx-check.conf')], capture_output=True)
        (audit / 'gateway-candidate-check.log').write_bytes(checked.stdout + checked.stderr)
        n.require(checked.returncode == 0, 'gateway candidate invalid; see gateway-candidate-check.log')
        installed = []
        try:
            for target, candidate_path in candidates.items():
                target_path = Path(target)
                if str(target_path).startswith('/etc/nginx/'):
                    subprocess.run(['sudo', '-n', 'install', '-m', '0644', candidate_path, target + '.cta-next'], check=True, capture_output=True)
                    subprocess.run(['sudo', '-n', 'mv', target + '.cta-next', target], check=True, capture_output=True)
                else:
                    shutil.copyfile(candidate_path, target + '.cta-next')
                    Path(target + '.cta-next').chmod(0o644)
                    os.replace(target + '.cta-next', target)
                installed.append(target)
            checked = subprocess.run(['sudo', '-n', 'nginx', '-t'], capture_output=True)
            (audit / 'gateway-installed-check.log').write_bytes(checked.stdout + checked.stderr)
            n.require(checked.returncode == 0, 'nginx candidate validation failed')
            subprocess.run(['sudo', '-n', 'systemctl', 'reload', 'nginx'], check=True, capture_output=True)
        except Exception as error:
            (audit / 'gateway-failure.log').write_text(str(error))
            for target in reversed(installed):
                if target in backups:
                    subprocess.run(['sudo', '-n', 'install', '-m', '0644', backups[target]['backup'], target], check=True, capture_output=True)
                elif Path(target).exists():
                    subprocess.run(['sudo', '-n', 'rm', '--', target], check=True, capture_output=True)
            subprocess.run(['sudo', '-n', 'nginx', '-t'], check=True, capture_output=True)
            subprocess.run(['sudo', '-n', 'systemctl', 'reload', 'nginx'], check=True, capture_output=True)
            raise RuntimeError('Gateway update failed; prior gateway files restored; execution remains stopped') from None
        print('Old CTA processes retired; unified gateway installed.', flush=True)
    before = json.loads((audit / 'protected.before.json').read_text())
    now = protected_processes()
    n.require(all(now.get(pid) == info for pid, info in before.items()), 'protected non-CTA process changed')
    confirm_flat()
    with session('lgq') as api:
        profiles = {}
        for worker in NEW:
            path = '/cta/cta-prep-binance-futures/config/api/config?worker=' + worker
            data = api(path)
            combinations = data['plan']['combinations']
            n.require(all(not c['signal']['enabled'] for c in combinations.values()), 'unexpected enabled position table')
            profiles[worker] = {name: {'enabled': c['signal']['enabled'], 'max_pos_u': c['signal']['max_pos_u'],
                                     'rules': len(c['factor_rules'])} for name, c in combinations.items()}
    with session('prc') as api:
        try:
            api('/cta/cta-prep-binance-futures/config/api/config')
        except RuntimeError as error:
            n.require('HTTP 403' in str(error), 'unexpected configure boundary response')
        else:
            raise RuntimeError('CTA configuration scope leaked to prc')
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for env in OLD:
        url = 'http://127.0.0.1:4191/cta/' + env + '/config/'
        try:
            opener.open(url, timeout=10)
        except urllib.error.HTTPError as error:
            n.require(error.code == 410, 'legacy config not retired')
        else:
            raise RuntimeError('legacy config still available')
    result = {'profiles': profiles, 'protected_processes_unchanged': len(before),
              'old_config_http_status': 410, 'new_config_http_status': 200, 'prc_http_status': 403,
              'exec_orders_started': False, 'rx01_perpetual_positions': 0, 'rx01_bindings': 0,
              'checked_at_ms': time.time_ns() // 1_000_000}
    save(audit / 'verified.json', result)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
