"""Exercise the environment-local wrapper with a fake supervisor and binaries."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[2]


class HourlyLifecycle(unittest.TestCase):
    def test_hourly_supervisor_identity_and_seed_environment_are_isolated(self):
        with tempfile.TemporaryDirectory(prefix='cta-hourly-lifecycle-') as temporary:
            base=Path(temporary);scripts=base/'scripts';scripts.mkdir()
            for name in ['cta_factor_process.sh','start_cta_prep_binance_futures.sh','process_match_lib.sh']:
                shutil.copy2(ROOT/'scripts'/name,scripts/name)
            binary=base/'cta-prep-binance-futures';binary.write_text("#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > preflight.txt\nexit 0\n");binary.chmod(0o755)
            pm=base/'fake-supervisor';pm.write_text('''#!/usr/bin/env python3
import json,pathlib,sys
base=pathlib.Path(__file__).parent
with (base/'calls.jsonl').open('a') as f:f.write(json.dumps(sys.argv[1:])+'\\n')
if '--config' in sys.argv:
 p=pathlib.Path(sys.argv[sys.argv.index('--config')+1]);(base/'captured.json').write_text(p.read_text())
''');pm.chmod(0o755)
            (base/'env.sh').write_text('\n'.join([f'export PMDAEMON_BIN="{pm}"',
                'export CTA_FACTOR_PROCESS_NAME=cta-prep-binance-futures-1h',
                'export CTA_FACTOR_OUTPUT_SERVICE=factor_output/cta-prep-binance-futures-1h',
                'export CTA_HOURLY_HISTORY_DIR=/history/hourly',
                'export CTA_PREP_BINANCE_FUTURES_CORE=""','']))
            result=subprocess.run([str(scripts/'start_cta_prep_binance_futures.sh'),'--execute'],cwd=base,text=True,capture_output=True)
            self.assertEqual(result.returncode,0,result.stderr)
            app=json.loads((base/'captured.json').read_text())['apps'][0]
            self.assertEqual(app['name'],'cta-prep-binance-futures-1h')
            self.assertEqual(app['cwd'],str(base))
            self.assertEqual(app['args'],['--config','config/cta_prep_binance_futures.toml','--output-service','factor_output/cta-prep-binance-futures-1h'])
            self.assertEqual(app['env']['CTA_HOURLY_HISTORY_DIR'],'/history/hourly')
            self.assertEqual(app['env']['CTA_PREP_BINANCE_FUTURES_CORE'],'')
            self.assertIn('--validate-hourly-history',(base/'preflight.txt').read_text())
            self.assertIn('--validate-kafka-continuity',(base/'preflight.txt').read_text())
            calls=[json.loads(line) for line in (base/'calls.jsonl').read_text().splitlines()]
            self.assertEqual(calls[0],['delete','cta-prep-binance-futures-1h'])
            self.assertNotIn(['delete','cta-prep-binance-futures'],calls)

    def test_failed_history_preflight_does_not_touch_supervisor_or_live_process(self):
        with tempfile.TemporaryDirectory(prefix='cta-reject-history-') as temporary:
            base=Path(temporary); scripts=base/'scripts'; scripts.mkdir()
            for name in ['cta_factor_process.sh','start_cta_factor_spread.sh','process_match_lib.sh']:
                shutil.copy2(ROOT/'scripts'/name,scripts/name)
            binary=base/'cta-factor-spread'
            binary.write_text('#!/usr/bin/env bash\necho "missing OKX Tardis history" >&2\nexit 1\n'); binary.chmod(0o755)
            supervisor=base/'supervisor'
            supervisor.write_text('#!/usr/bin/env bash\ntouch supervisor-called\n'); supervisor.chmod(0o755)
            (base/'env.sh').write_text(f'export PMDAEMON_BIN="{supervisor}"\n')
            result=subprocess.run([str(scripts/'start_cta_factor_spread.sh'),'--execute'],cwd=base,text=True,capture_output=True)
            self.assertNotEqual(result.returncode,0)
            self.assertIn('missing OKX Tardis history',result.stderr)
            self.assertFalse((base/'supervisor-called').exists())


if __name__=='__main__':unittest.main()
