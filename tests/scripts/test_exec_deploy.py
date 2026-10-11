from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class ExecDeployTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.temp_path = Path(self.temp_dir.name)
        self.deploy_root = self.temp_path / "deploy"
        self.local_repo = self.temp_path / "repo"
        shutil.copytree(
            ROOT / "scripts", self.local_repo / "scripts",
            ignore=shutil.ignore_patterns("__pycache__", "tests"),
        )
        (self.local_repo / "config").mkdir()
        for name in ("iceoryx2.toml", "persist_sync_distribution.toml", "mkt_cfg.yaml"):
            shutil.copy2(ROOT / "config" / name, self.local_repo / "config" / name)
        release = self.local_repo / "target/release"
        release.mkdir(parents=True)
        for name in (
            "exec-pre-trade", "trade_signal", "trade_engine", "persist_manager",
            "viz_server", "spread_pbs", "binance_account_monitor",
            "okex_account_monitor", "rapidx_account_monitor",
        ):
            (release / name).write_text(f"artifact:{name}\n", encoding="utf-8")

    def run_deploy(self, env_name: str, venue: str, *args: str) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env["DEPLOY_ROOT"] = str(self.deploy_root)
        # A publishing shell's backend must not override the deployed account.
        env["TRADE_ENGINE_EXEC_BACKEND"] = "rapidx"
        return subprocess.run(
            ["bash", str(self.local_repo / "scripts/deploy_exec.sh"),
             "--env-name", env_name, "--venue", venue, *args],
            env=env, capture_output=True, text=True, timeout=10,
        )

    def test_prebuilt_deploy_installs_backend_specific_monitor(self) -> None:
        for exchange in ("binance", "okex"):
            for backend in ("native", "rapidx"):
                with self.subTest(exchange=exchange, backend=backend):
                    name = f"{exchange}_exec_{backend}01"
                    args = ["--skip-build"]
                    if backend == "rapidx":
                        args.extend(["--exec-backend", "rapidx"])
                    result = self.run_deploy(name, f"{exchange}-futures", *args)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    target = self.deploy_root / name
                    destination = "rapidx_account_monitor" if backend == "rapidx" else "account_monitor"
                    source = "rapidx_account_monitor" if backend == "rapidx" else f"{exchange}_account_monitor"
                    self.assertEqual((target / destination).read_text(), f"artifact:{source}\n")
                    self.assertTrue(os.access(target / destination, os.X_OK))
                    self.assertFalse((target / ("account_monitor" if backend == "rapidx" else "rapidx_account_monitor")).exists())
                    env = (target / "env.sh").read_text()
                    expected_backend = "ltp" if backend == "rapidx" else "native"
                    self.assertIn(f'TRADE_ENGINE_EXEC_BACKEND_MAP="{exchange}={expected_backend}"', env)
                    if backend == "rapidx":
                        self.assertIn("LTP_API_KEY, LTP_API_SECRET and LTP_PORTFOLIO_ID", env)
                    self.assertIn("Processes were not started", result.stdout)

    def test_existing_rapidx_env_is_preserved_and_selects_monitor_automatically(self) -> None:
        target = self.deploy_root / "binance_exec_rx01"
        target.mkdir(parents=True)
        original = (
            f"export IPC_NAMESPACE={target.name}\n"
            "export EXEC_VENUE=binance-futures\n"
            "export TRADE_ENGINE_EXEC_BACKEND_MAP='*=native,binance=liquiditytech'\n"
            "export LTP_API_SECRET=fixture-secret\n"
        )
        (target / "env.sh").write_text(original)
        result = self.run_deploy(target.name, "binance-futures", "--skip-build")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((target / "env.sh").read_text(), original)
        self.assertTrue((target / "rapidx_account_monitor").is_file())
        self.assertFalse((target / "account_monitor").exists())
        self.assertNotIn("fixture-secret", result.stdout + result.stderr)
        before = (target / "rapidx_account_monitor").read_bytes()
        mismatch = self.run_deploy(
            target.name, "binance-futures", "--skip-build", "--exec-backend", "native",
        )
        self.assertNotEqual(mismatch.returncode, 0)
        self.assertIn("must match existing env.sh", mismatch.stderr)
        self.assertEqual((target / "env.sh").read_text(), original)
        self.assertEqual((target / "rapidx_account_monitor").read_bytes(), before)

    def test_scripts_only_rapidx_initialization_installs_no_binaries(self) -> None:
        result = self.run_deploy(
            "binance_exec_rx01", "binance-futures", "--scripts-only", "--exec-backend", "ltp",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        target = self.deploy_root / "binance_exec_rx01"
        self.assertTrue((target / "scripts/start_account_monitor.sh").is_file())
        self.assertTrue((target / "config/exec_viz.toml").is_file())
        self.assertIn('TRADE_ENGINE_EXEC_BACKEND_MAP="binance=ltp"', (target / "env.sh").read_text())
        self.assertFalse((target / "rapidx_account_monitor").exists())
        self.assertFalse((target / "exec-pre-trade").exists())

    def test_rapidx_coin_and_invalid_backend_are_rejected_before_mutation(self) -> None:
        for venue, backend in (("binance-coin-futures", "rapidx"), ("binance-futures", "typo")):
            with self.subTest(venue=venue, backend=backend):
                result = self.run_deploy(
                    "binance_exec_rx01", venue, "--scripts-only", "--exec-backend", backend,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.deploy_root.exists())


if __name__ == "__main__":
    unittest.main()
