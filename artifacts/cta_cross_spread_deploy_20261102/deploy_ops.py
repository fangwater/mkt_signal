#!/usr/bin/env python3
"""JP fresh cross-spread deployment, split into reviewable phases; no orders."""
import argparse
import ast
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tarfile
import tomllib

parser = argparse.ArgumentParser()
parser.add_argument('phase', choices=['prepare', 'start-observers', 'activate-manager', 'grant'])
parser.add_argument('--stage', required=True)
args = parser.parse_args()
home = Path.home()
stage = Path(args.stage).resolve()
manager = home / 'crypto_cta_manager'
assert stage.parent == manager / 'deploy-staging'
audit = manager / 'maintenance-backups' / stage.name
envs = ['okex_exec_trade01', 'binance_exec_spread01']
manifest = json.loads((stage / 'release_manifest.json').read_text())


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def db_helpers():
    helper = home / 'binance_exec_trade06/run/rx01_activation_ops.py'
    tree = ast.parse(helper.read_text())
    nodes = [n for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom)) or isinstance(n, ast.FunctionDef) and n.name in ['db_env', 'sql']]
    ns = {'MANAGER': manager}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(helper), 'exec'), ns)
    return ns


def write_new(path, text, mode=0o600):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(fd, 'w') as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())


def install(source, destination, mode=0o755):
    destination = Path(destination)
    temporary = destination.with_name(destination.name + '.cross-spread-next')
    assert not temporary.exists()
    shutil.copyfile(source, temporary)
    os.chmod(temporary, mode)
    assert sha(source) == sha(temporary)
    os.replace(temporary, destination)


def source_env(path):
    raw = subprocess.run(['bash', '-c', 'set -a; source "$1" >/dev/null 2>&1; env -0', 'bash', str(path)], check=True, capture_output=True).stdout
    return dict(x.decode().split('=', 1) for x in raw.split(b'\0') if b'=' in x)


def protected_processes():
    names = {'trade_engine', 'exec-pre-trade', 'pre_trade', 'trade_signal', 'account_monitor', 'binance_account', 'okex_account_mo', 'persist_manager', 'viz_server'}
    result = {}
    for p in Path('/proc').iterdir():
        if not p.name.isdigit():
            continue
        try:
            comm = (p / 'comm').read_text().strip()
            if comm in names:
                result[p.name] = {'comm': comm, 'cwd': os.readlink(p / 'cwd'), 'start_ticks': (p / 'stat').read_text().split(') ', 1)[1].split()[19]}
        except OSError:
            pass
    return result


if args.phase == 'prepare':
    assert not audit.exists()
    assert not (home / envs[1]).exists()
    assert not (home / envs[0] / 'data/persist_manager').exists()
    for kind, entries in [('mkt', manifest['mkt_artifacts']), ('manager', manifest['manager_artifacts'])]:
        for name, metadata in entries.items():
            assert sha(stage / 'bin' / kind / name) == metadata['sha256'], name
    audit.mkdir(mode=0o700)
    write_new(audit / 'processes.before.json', json.dumps(protected_processes(), indent=2) + '\n')
    protected = [home / n / f for n in ['okex_mm_alpha', 'binance_exec_trade08'] for f in ['env.sh', 'trade_engine.toml']]
    write_new(audit / 'protected-files.json', json.dumps({str(p): sha(p) for p in protected}, indent=2) + '\n')
    raw = (manager / 'config/cta-manager.toml').read_text()
    before = tomllib.loads(raw)
    assert not before.get('monitor', {}).get('enabled', False)
    assert all(not any(s['id'] == n for s in before['sources']) for n in envs)
    write_new(audit / 'cta-manager.toml.before', raw)
    write_new(audit / 'webroot.before', os.readlink(manager / 'webroot') + '\n')
    shutil.copy2('/etc/nginx/snippets/crypto_cta_manager.conf', audit / 'nginx.before')
    os.chmod(audit / 'nginx.before', 0o600)
    (audit / 'bin.before').mkdir()
    for name in manifest['manager_artifacts']:
        shutil.copy2(manager / 'bin' / name, audit / 'bin.before' / name)
    with tarfile.open(stage / 'runtime_payload.tar.gz') as tar:
        for member in tar.getmembers():
            parts = Path(member.name).parts
            assert parts and parts[0] in envs + ['okex-binance-cta-spread-rx01'] and '..' not in parts and not Path(member.name).is_absolute()
            assert member.isfile() or member.isdir()
        tar.extractall(home, filter='data')

    # Preserve the prepared OKX credentials, namespace and source IPs.
    okx_env = home / envs[0] / 'env.sh'
    shutil.copy2(okx_env, audit / 'okex-env.before')
    with okx_env.open('a') as f:
        f.write("\nexport PERSIST_MANAGER_CORE='15'\nexport PYTHON_BIN='/usr/bin/python3'\n")
    old = source_env(home / 'binance_exec_trade08/env.sh')
    assert old['BINANCE_ACCOUNT_MODE'] == 'STANDARD'
    settings = {
        'IPC_NAMESPACE': envs[1], 'EXEC_VENUE': 'binance-futures', 'VENUE': 'binance-futures',
        'EXEC_INSTANCE': '01', 'TRADE_ENGINE_EXEC_BACKEND_MAP': 'binance=native',
        'EXEC_VIZ_PORT': '10050', 'EXEC_CONFIG_PORT': '18170', 'EXEC_CONFIG_RELOAD_MS': '30000',
        'ACCOUNT_MONITOR_CORE': '25', 'EXEC_PRE_TRADE_CORE': '26', 'PRE_TRADE_CORE': '26', 'TRADE_ENGINE_CORE': '27',
        'PERSIST_MANAGER_CORE': '15', 'PYTHON_BIN': '/usr/bin/python3', 'BINANCE_ACCOUNT_MODE': 'STANDARD',
        'TRADE_SIGNAL_ENABLE_QUEUE_POSITION': '0', 'enable_ipc_fast_poll': '0', 'ENABLE_IPC_FAST_POLL': '0',
        'BINANCE_API_KEY': old['BINANCE_API_KEY'], 'BINANCE_API_SECRET': old['BINANCE_API_SECRET'],
    }
    write_new(home / envs[1] / 'env.sh', '#!/usr/bin/env bash\n' + ''.join(f'export {k}={shlex.quote(v)}\n' for k, v in settings.items()))
    for env, account_monitor in zip(envs, ['okex_account_monitor', 'binance_account_monitor']):
        root = home / env
        (root / 'run').mkdir(exist_ok=True)
        assert not (root / 'data/persist_manager').exists()
        for name in ['exec-pre-trade', 'trade_engine', 'trade_signal', 'persist_manager', 'viz_server']:
            install(stage / 'bin/mkt' / name, root / name)
        install(stage / 'bin/mkt' / account_monitor, root / 'account_monitor')
        write_new(root / 'release.json', json.dumps({'source_commit': manifest['mkt_commit'], 'trading_enabled': False, 'new_account': True}, indent=2) + '\n')
    signal = home / 'okex-binance-cta-spread-rx01'
    for name in ['cta_cross_spread_signal', 'cta_cross_spread_factor_model_1m_pub']:
        install(stage / 'bin/mkt' / name, signal / name)
    assert json.loads((signal / 'config/cta_cross_spread.json').read_text())['enabled'] is False
    assert json.loads((signal / 'config/cta_cross_spread_symbol_factors.json').read_text()) == {}

    fragment_text = (stage / 'cta-cross-spread.toml').read_text()
    fragment = tomllib.loads(fragment_text)
    expected = copy.deepcopy(before)
    expected['sources'].extend(fragment['sources'])
    expected['dashboard']['source_viewers'].update(fragment['dashboard']['source_viewers'])
    expected['dashboard'].setdefault('nav_portfolios', {}).update(fragment['dashboard']['nav_portfolios'])
    views = '\n'.join(f'{k} = {json.dumps(v)}' for k, v in fragment['dashboard']['source_viewers'].items())
    candidate, count = re.subn(r'(?m)^\[dashboard\.source_viewers\][ \t]*$', '[dashboard.source_viewers]\n' + views, raw)
    assert count == 1
    portfolio_text = fragment_text[fragment_text.index('[dashboard.nav_portfolios.'):fragment_text.index('[[sources]]')]
    candidate = candidate.rstrip() + '\n\n' + portfolio_text + fragment_text[fragment_text.index('[[sources]]'):]
    for section in ['kline', 'funding']:
        paths = expected.get(section, {}).get('trade_engine_configs')
        if paths is None:
            continue
        for env in envs:
            path = str(home / env / 'trade_engine.toml')
            if path not in paths:
                paths.append(path)
        match = re.search(rf'(?ms)(^\[{section}\][ \t]*\n)(.*?)(?=^\[|\Z)', candidate)
        body, count = re.subn(r'(?ms)^trade_engine_configs\s*=\s*\[.*?\]', 'trade_engine_configs = ' + json.dumps(paths), match.group(2), count=1)
        assert count == 1
        candidate = candidate[:match.start(2)] + body + candidate[match.end(2):]
    assert tomllib.loads(candidate) == expected
    write_new(audit / 'cta-manager.toml.candidate', candidate)
    # With monitor.enabled=false this validates AppConfig and exits before IPC,
    # venue calls, database access or any notification sender is constructed.
    subprocess.run([str(stage / 'bin/manager/cta_monitor'), '--config', str(audit / 'cta-manager.toml.candidate'), '--once'], check=True, stdout=subprocess.DEVNULL)
    nginx = (audit / 'nginx.before').read_text()
    addition = (stage / 'nginx.additions.conf').read_text()
    assert '/exec_okex01/' not in nginx and '/exec_binance_spread01/' not in nginx
    write_new(audit / 'nginx.candidate', nginx.rstrip() + '\n\n' + addition)
    write_new(audit / 'release_manifest.json', json.dumps(manifest, indent=2) + '\n')
    print('Prepared fresh Execs, validated Manager candidate, preserved old sources; no process started.')

elif args.phase == 'start-observers':
    for env in envs:
        root = home / env
        for script in ['start_exec_persist_manager.sh', 'start_exec_config_server.sh', 'start_exec_viz_server.sh', 'start_account_monitor.sh']:
            result = subprocess.run([str(root / 'scripts' / script)], cwd=root, text=True, capture_output=True)
            write_new(audit / f'{env}-{script}.log', result.stdout + result.stderr)
            if result.returncode:
                raise RuntimeError(f'{env} {script} failed; inspect the protected audit log')
        subprocess.run(['python3', 'scripts/sync_exec_risk_params.py', '--env-name', env, '--exec-venue', 'okex-futures' if env.startswith('okex') else 'binance-futures'], cwd=root, check=True, stdout=subprocess.DEVNULL)
        print('Started observers, persist, Viz and Config only:', env)
    signal = home / 'okex-binance-cta-spread-rx01'
    subprocess.run(['python3', 'scripts/cta_spread_factor_config.py', '--env-name', signal.name, '--config', 'config/cta_cross_spread_symbol_factors.json', '--execute'], cwd=signal, check=True, stdout=subprocess.DEVNULL)
    result = subprocess.run([str(signal / 'scripts/start_cta_cross_spread_config_server.sh')], cwd=signal, text=True, capture_output=True)
    write_new(audit / 'signal-config-start.log', result.stdout + result.stderr)
    assert result.returncode == 0
    print('Started disabled signal configuration page; signal/factor publisher/trade_engine/pre-trade remain stopped.')

elif args.phase == 'activate-manager':
    assert sha(manager / 'config/cta-manager.toml') == sha(audit / 'cta-manager.toml.before')
    for name in manifest['manager_artifacts']:
        install(stage / 'bin/manager' / name, manager / 'bin' / name)
    release = manager / 'web-releases' / stage.name
    assert not release.exists()
    release.mkdir()
    shutil.copytree(stage / 'manager-web', release / 'manager')
    (manager / 'webroot.cross-spread-next').symlink_to(release.relative_to(manager))
    os.replace(manager / 'webroot.cross-spread-next', manager / 'webroot')
    install(audit / 'cta-manager.toml.candidate', manager / 'config/cta-manager.toml', 0o600)
    subprocess.run(['sudo', '-n', 'install', '-m', '0644', str(audit / 'nginx.candidate'), '/etc/nginx/snippets/crypto_cta_manager.conf'], check=True)
    checked = subprocess.run(['sudo', '-n', 'nginx', '-t'], text=True, capture_output=True)
    write_new(audit / 'nginx-test.log', checked.stdout + checked.stderr)
    if checked.returncode:
        subprocess.run(['sudo', '-n', 'install', '-m', '0644', str(audit / 'nginx.before'), '/etc/nginx/snippets/crypto_cta_manager.conf'], check=True)
        raise RuntimeError('nginx validation failed; original snippet restored')
    subprocess.run(['systemctl', '--user', 'restart', 'crypto-cta-manager-web.service'], check=True)
    subprocess.run(['systemctl', '--user', 'is-active', '--quiet', 'crypto-cta-manager-web.service'], check=True)
    subprocess.run(['sudo', '-n', 'systemctl', 'reload', 'nginx'], check=True)
    print('Activated Manager/API/frontend and authenticated gateways; no trading process started.')

elif args.phase == 'grant':
    sql = db_helpers()['sql']
    result = sql("""BEGIN;
DO $$ BEGIN
 IF (SELECT count(*) FROM cta_order_sources WHERE source_id IN ('okex_exec_trade01','binance_exec_spread01') AND enabled) <> 2
 OR EXISTS (SELECT 1 FROM cta_position_snapshots WHERE source_id IN ('okex_exec_trade01','binance_exec_spread01'))
 OR EXISTS (SELECT 1 FROM cta_strategy_position_snapshots WHERE source_id IN ('okex_exec_trade01','binance_exec_spread01'))
 OR EXISTS (SELECT 1 FROM cta_account_strategy_bindings WHERE source_id IN ('okex_exec_trade01','binance_exec_spread01'))
 THEN RAISE EXCEPTION 'new source initialization mismatch'; END IF;
END $$;
INSERT INTO cta_user_source_permissions(user_id,source_id,access_level)
SELECT user_id,source_id,'configure' FROM cta_users CROSS JOIN (VALUES ('okex_exec_trade01'),('binance_exec_spread01')) AS s(source_id)
WHERE username='lgq' AND disabled=false ON CONFLICT (user_id,source_id) DO NOTHING;
COMMIT;""")
    assert not result
    print('Granted lgq both independent accounts; no initial snapshot or strategy binding created.')
