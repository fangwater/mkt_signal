"""Retired CTA environments must fail before deployment can mutate a host."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class RetiredCtaEntrypointsTests(unittest.TestCase):
    def test_intra_deploy_rejects_legacy_name_even_with_explicit_exchange(self):
        with tempfile.TemporaryDirectory() as temporary:
            stubs = Path(temporary)
            for command in ("cargo", "mkdir", "cp", "mv", "install", "scp", "ssh", "rsync"):
                stub = stubs / command
                stub.write_text('#!/bin/sh\necho "UNEXPECTED_MUTATION_ATTEMPT" >&2\nexit 91\n')
                stub.chmod(0o755)
            env = {**os.environ, "PATH": f"{stubs}:{os.environ['PATH']}"}
            for entrypoint in (
                "deploy_intra_trade_signal.sh", "deploy_intra_pre_trade.sh",
                "deploy_intra_trade_engine.sh", "deploy_intra_monitors.sh",
                "deploy_intra_persist_manager.sh", "deploy_intra_viz_server.sh",
                "deploy_setup_env_intra.sh", "deploy_intra_config_server.sh",
            ):
                selectors = [["--env-name", "binance-cta-rx01"]]
                if entrypoint != "deploy_intra_config_server.sh":
                    selectors.append(["--env-suffix", "cta-rx01"])
                for selector in selectors:
                    with self.subTest(entrypoint=entrypoint, selector=selector):
                        result = subprocess.run(
                            ["bash", str(ROOT / "scripts" / entrypoint),
                             "--exchange", "binance", *selector],
                            cwd=ROOT, env=env, text=True, capture_output=True, timeout=10,
                        )
                        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                        self.assertNotIn("UNEXPECTED_MUTATION_ATTEMPT", result.stdout + result.stderr)
                        self.assertRegex(result.stderr, r"Legacy CTA execution is retired|env-name must match")
