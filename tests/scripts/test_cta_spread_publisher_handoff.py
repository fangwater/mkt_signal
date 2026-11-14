import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class PublisherHandoffTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="spread-handoff-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        scripts = self.root / "scripts"
        scripts.mkdir()
        repo = Path(__file__).resolve().parents[2]
        for action in ["start", "stop"]:
            name = f"{action}_cta_spread_factor_model_1m_pub.sh"
            shutil.copy2(repo / "scripts" / name, scripts / name)
        (scripts / "process_match_lib.sh").write_text(
            "safe_find_running_pids() { printf '11111\\n22222\\n'; }\n"
        )
        (self.root / "config").mkdir()
        (self.root / "config/cta_spread_factor_model_1m_pub.toml").touch()
        binary = self.root / "cta_spread_factor_model_1m_pub"
        binary.write_text("#!/bin/sh\nexit 0\n")
        binary.chmod(0o755)
        (self.root / "env.sh").write_text(
            "export CTA_SPREAD_FACTOR_MODEL_1M_CORE=17\n"
            "export CTA_SPREAD_FACTOR_PROCESS_NAME=factor_current\n"
        )
        mock = self.root / "mock_pm"
        mock.write_text(
            "#!/usr/bin/env python3\n"
            "import json,os,pathlib,sys\n"
            "root=pathlib.Path(os.environ['HANDOFF_TEST_ROOT'])\n"
            "with (root/'calls').open('a') as f: f.write(json.dumps(sys.argv[1:])+'\\n')\n"
            "if '--config' in sys.argv:\n"
            " (root/'process.json').write_text(pathlib.Path(sys.argv[sys.argv.index('--config')+1]).read_text())\n"
        )
        mock.chmod(0o755)
        hook = self.root / "bash_env"
        hook.write_text('kill() { printf "%s\\n" "$*" >> "$HANDOFF_TEST_ROOT/kills"; }\n')
        self.env = {
            **os.environ,
            "HANDOFF_TEST_ROOT": str(self.root),
            "BASH_ENV": str(hook),
            "PMDAEMON_BIN": str(mock),
            "CTA_SPREAD_RUN_FACTOR_PUBLISHER": "1",
            "CTA_SPREAD_FACTOR_WAIT_FOR_PUBLISHERS": "0",
            "CTA_SPREAD_FACTOR_KEEP_PID": "",
            "PMDAEMON_NAME": "",
        }

    def run_script(self, action, **env):
        return subprocess.run(
            ["bash", str(self.root / "scripts" / f"{action}_cta_spread_factor_model_1m_pub.sh")],
            env={**self.env, **env}, capture_output=True, text=True,
        )

    def test_standby_start_leaves_live_process_running(self):
        result = self.run_script("start", PMDAEMON_NAME="factor_replacement",
                                 CTA_SPREAD_FACTOR_WAIT_FOR_PUBLISHERS="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((self.root / "kills").exists())
        calls = [json.loads(line) for line in (self.root / "calls").read_text().splitlines()]
        self.assertEqual(calls[0], ["delete", "factor_replacement"])
        app = json.loads((self.root / "process.json").read_text())["apps"][0]
        self.assertEqual(app["name"], "factor_replacement")
        self.assertIn("--wait-for-publishers", app["args"])
        self.assertEqual(app["args"][app["args"].index("--core") + 1], "17")

    def test_handoff_stop_preserves_replacement_pid(self):
        result = self.run_script("stop", PMDAEMON_NAME="factor_old", CTA_SPREAD_FACTOR_KEEP_PID="22222")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.root / "kills").read_text().splitlines(), ["11111"])
        self.assertEqual(json.loads((self.root / "calls").read_text()), ["delete", "factor_old"])

    def test_normal_stop_uses_persisted_name_and_cleans_all_pids(self):
        result = self.run_script("stop")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.root / "kills").read_text().splitlines(), ["11111", "22222"])
        self.assertEqual(json.loads((self.root / "calls").read_text()), ["delete", "factor_current"])

    def test_invalid_keep_pid_does_not_stop_any_process(self):
        result = self.run_script("stop", CTA_SPREAD_FACTOR_KEEP_PID="-1")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.root / "calls").exists())
        self.assertFalse((self.root / "kills").exists())


if __name__ == "__main__":
    unittest.main()
