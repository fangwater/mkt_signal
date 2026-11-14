"""Role-scoped operations must preserve the other live Binance feed role."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class RoleScripts(unittest.TestCase):
    def run_script(self, action, role):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            venue = root / 'binance-futures'
            scripts = venue / 'scripts'
            scripts.mkdir(parents=True)
            fakebin = root / 'bin'
            fakebin.mkdir()
            script = scripts / f'{action}_spread_pbs.sh'
            shutil.copy(ROOT / 'scripts/spread_pbs' / script.name, script)
            (venue / 'env.sh').write_text(
                'export SPREAD_PBS_BINANCE_FUTURES_ROLE=split\n'
                'export SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE=14\n'
                'export SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE=9\n')
            (venue / 'spread_pbs').write_text('#!/bin/sh\nexit 0\n')
            (venue / 'spread_pbs').chmod(0o755)
            other = 'market' if role == 'bookticker' else 'bookticker'
            stubs = {
                'pmdaemon': '#!/usr/bin/env python3\nimport json,os,sys\n'
                'with open(os.environ["CALLS"],"a") as f: f.write(json.dumps(sys.argv[1:])+"\\n")\n'
                'if "--config" in sys.argv:\n'
                ' import shutil; shutil.copy(sys.argv[sys.argv.index("--config")+1],os.environ["CAPTURE"])\n',
                'taskset': '#!/bin/sh\nexit 0\n',
                # Other-role, venue-prefix and non-binary matches must all be ignored.
                'ps': f'''#!/bin/sh
cat <<'EOF'
999991 spread_pbs /runtime/spread_pbs --venue binance-futures --binance-futures-role {other}
999992 python python probe_spread_pbs --venue binance-futures --binance-futures-role {role}
999993 spread_pbs /runtime/spread_pbs --venue binance-futures-test --binance-futures-role {role}
EOF
''',
            }
            for name, content in stubs.items():
                (fakebin / name).write_text(content)
                (fakebin / name).chmod(0o755)
            calls, capture = root / 'calls', root / 'config.json'
            env = {**os.environ, 'PATH': f'{fakebin}:{os.environ["PATH"]}',
                   'CALLS': str(calls), 'CAPTURE': str(capture), 'KILL_WAIT_SECS': '0',
                   'PMDAEMON_BIN': str(fakebin / 'pmdaemon')}
            run = subprocess.run(['bash', str(script), '--role', role], env=env,
                                 text=True, capture_output=True, timeout=10)
            self.assertEqual(run.returncode, 0, run.stderr + run.stdout)
            self.assertNotIn('SIGTERM', run.stdout)
            self.assertNotIn('SIGKILL', run.stdout)
            commands = [json.loads(line) for line in calls.read_text().splitlines()]
            self.assertEqual(commands[0], ['delete', f'spp_bn_fu_{role}'])
            if action == 'start':
                self.assertEqual(len(commands), 2)
                self.assertEqual(commands[1][-2:], ['--name', f'spp_bn_fu_{role}'])
                apps = json.loads(capture.read_text())['apps']
                self.assertEqual(len(apps), 1)
                self.assertEqual(apps[0]['args'][-2:], ['--binance-futures-role', role])
            else:
                self.assertEqual(len(commands), 1)

    def test_start_bookticker(self): self.run_script('start', 'bookticker')
    def test_stop_bookticker(self): self.run_script('stop', 'bookticker')
    def test_start_market(self): self.run_script('start', 'market')
    def test_stop_market(self): self.run_script('stop', 'market')


if __name__ == '__main__':
    unittest.main()
