from __future__ import annotations

import contextlib
import io
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import bybit_cancel_all_um_orders as cancel_bybit  # noqa: E402


class BybitCancelAllUMOrdersTests(unittest.TestCase):
    def test_build_session_binds_http_and_https_pool_source_address(self) -> None:
        session = cancel_bybit.build_session("172.31.7.123")

        for prefix in ("http://", "https://"):
            adapter = session.adapters[prefix]
            self.assertIsInstance(adapter, cancel_bybit.SourceAddressAdapter)
            self.assertEqual(adapter._source_address, ("172.31.7.123", 0))
            self.assertEqual(
                adapter.poolmanager.connection_pool_kw["source_address"],
                ("172.31.7.123", 0),
            )

    def test_main_resolves_trade_engine_source_before_read_only_query(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_dir = Path(temp_dir)
            (env_dir / "trade_engine.toml").write_text(
                'local_ips = ["172.31.7.123", "172.31.7.123"]\n',
                encoding="utf-8",
            )
            observed: dict[str, object] = {}

            def fake_fetch(session, api_key, api_secret, symbol):
                observed["api_key"] = api_key
                observed["api_secret"] = api_secret
                observed["symbol"] = symbol
                observed["source_address"] = session.adapters[
                    "https://"
                ].poolmanager.connection_pool_kw["source_address"]
                return []

            stdout = io.StringIO()
            with (
                mock.patch.dict(
                    os.environ,
                    {"BYBIT_API_KEY": "key", "BYBIT_API_SECRET": "secret"},
                    clear=False,
                ),
                mock.patch.object(
                    sys,
                    "argv",
                    [
                        "bybit_cancel_all_um_orders.py",
                        "--env-dir",
                        str(env_dir),
                        "--require-local-address",
                    ],
                ),
                mock.patch.object(cancel_bybit, "fetch_open_orders", side_effect=fake_fetch),
                contextlib.redirect_stdout(stdout),
            ):
                status = cancel_bybit.main()

            self.assertEqual(status, 0)
            self.assertEqual(observed["source_address"], ("172.31.7.123", 0))
            self.assertEqual(observed["api_key"], "key")
            self.assertEqual(observed["api_secret"], "secret")
            self.assertIsNone(observed["symbol"])
            self.assertIn("[bybit] local source address: 172.31.7.123", stdout.getvalue())
            self.assertIn("[bybit] open linear orders: 0", stdout.getvalue())

    def test_required_source_address_failure_happens_before_api_query(self) -> None:
        stderr = io.StringIO()
        with (
            mock.patch.dict(
                os.environ,
                {"BYBIT_API_KEY": "key", "BYBIT_API_SECRET": "secret"},
                clear=False,
            ),
            mock.patch.object(
                sys,
                "argv",
                ["bybit_cancel_all_um_orders.py", "--require-local-address"],
            ),
            mock.patch.object(
                cancel_bybit,
                "resolve_local_address",
                return_value=(None, "test: no configured address"),
            ),
            mock.patch.object(cancel_bybit, "fetch_open_orders") as fetch,
            contextlib.redirect_stderr(stderr),
        ):
            status = cancel_bybit.main()

        self.assertEqual(status, 2)
        fetch.assert_not_called()
        self.assertIn("no local source IP resolved", stderr.getvalue())

    def test_mm_wrapper_passes_environment_directory_to_bybit_helper(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            env_dir = temp_path / "bybit_mm_beta"
            env_dir.mkdir()
            (env_dir / "env.sh").write_text(
                "export BYBIT_API_KEY='key'\nexport BYBIT_API_SECRET='secret'\n",
                encoding="utf-8",
            )
            arg_log = temp_path / "args.log"
            fake_python = temp_path / "fake-python"
            fake_python.write_text(
                "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" >\"$FAKE_ARG_LOG\"\n",
                encoding="utf-8",
            )
            fake_python.chmod(0o755)
            env = os.environ.copy()
            env["FAKE_ARG_LOG"] = str(arg_log)

            result = subprocess.run(
                [
                    "bash",
                    str(SCRIPTS_DIR / "close_mm_all_um_ws_orders.sh"),
                    "--env-name",
                    "bybit_mm_beta",
                    "--env-dir",
                    str(env_dir),
                    "--python",
                    str(fake_python),
                    "--require-local-address",
                    "--execute",
                ],
                cwd=ROOT,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stdout)
            args = arg_log.read_text(encoding="utf-8").strip()
            self.assertIn("bybit_cancel_all_um_orders.py", args)
            self.assertIn(f"--env-dir {env_dir}", args)
            self.assertIn("--require-local-address", args)
            self.assertIn("--execute", args)


if __name__ == "__main__":
    unittest.main()
