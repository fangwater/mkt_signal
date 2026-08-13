from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PUBLISH_SCRIPT = ROOT / "scripts" / "publish-jp-meta-mm.sh"
START_SCRIPT = ROOT / "scripts" / "start-jp-meta-mm.sh"
STOP_SCRIPT = ROOT / "scripts" / "stop-jp-meta-mm.sh"


COMMON_PUBLISH_FILES = [
    "target/release/trade_signal",
    "target/release/viz_server",
    "target/release/pre_trade",
    "target/release/trade_engine",
    "target/release/persist_manager",
    "scripts/mm_config_server.py",
    "scripts/mm_process_name.sh",
    "scripts/process_match_lib.sh",
    "scripts/start_mm_config_server.sh",
    "scripts/stop_mm_config_server.sh",
    "scripts/start_account_monitor.sh",
    "scripts/stop_account_monitor.sh",
    "scripts/start_trade_signal.sh",
    "scripts/stop_trade_signal.sh",
    "scripts/close_mm_all_um_ws_orders.sh",
    "scripts/print_mm_risk_params.py",
    "scripts/sync_mm_risk_params.py",
    "scripts/print_mm_strategy_params.py",
    "scripts/sync_mm_strategy_params.py",
    "scripts/print_mm_amount_u.py",
    "scripts/sync_mm_amount_u.py",
    "scripts/print_mm_symbol_list.py",
    "scripts/sync_mm_symbol_list.py",
    "mm_scripts/start_mm_viz_server.sh",
    "mm_scripts/stop_mm_viz_server.sh",
    "mm_scripts/start_mm_persist_manager.sh",
    "mm_scripts/stop_mm_persist_manager.sh",
    "mm_scripts/start_mm_trade_engine.sh",
    "mm_scripts/stop_mm_trade_engine.sh",
    "mm_scripts/start_mm_pre_trade.sh",
    "mm_scripts/stop_mm_pre_trade.sh",
    "mm_scripts/stop_manual_mm_signal.sh",
]


class JPMetaMMOrchestrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)
        self.remote_home = self.temp_path / "remote_home"
        self.fake_bin = self.temp_path / "fake_bin"
        self.remote_home.mkdir()
        self.fake_bin.mkdir()
        self._write_fake_transport()

        self.base_env = os.environ.copy()
        self.base_env["PATH"] = f"{self.fake_bin}{os.pathsep}{self.base_env['PATH']}"
        self.base_env["FAKE_REMOTE_HOME"] = str(self.remote_home)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    @staticmethod
    def _write(path: Path, contents: str, executable: bool = False) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding="utf-8")
        if executable:
            path.chmod(0o755)

    def _write_fake_transport(self) -> None:
        self._write(
            self.fake_bin / "ssh",
            textwrap.dedent(
                """\
                #!/usr/bin/env bash
                set -euo pipefail
                while [[ "${1:-}" == "-o" ]]; do
                  shift 2
                done
                [[ $# -ge 1 ]]
                shift
                export HOME="${FAKE_REMOTE_HOME:?}"
                if [[ $# -eq 1 ]]; then
                  exec bash -c "$1"
                fi
                exec "$@"
                """
            ),
            executable=True,
        )
        self._write(
            self.fake_bin / "scp",
            textwrap.dedent(
                """\
                #!/usr/bin/env bash
                set -euo pipefail
                while [[ "${1:-}" == "-o" ]]; do
                  shift 2
                done
                [[ $# -ge 2 ]]
                sources=()
                while [[ $# -gt 1 ]]; do
                  sources+=("$1")
                  shift
                done
                destination="$1"
                remote_path="${destination#*:}"
                if [[ "${#sources[@]}" -gt 1 || "$remote_path" == */ ]]; then
                  mkdir -p "$remote_path"
                  cp "${sources[@]}" "$remote_path/"
                else
                  mkdir -p "$(dirname "$remote_path")"
                  cp "${sources[0]}" "$remote_path"
                fi
                """
            ),
            executable=True,
        )
        for command in ("pmdaemon", "npx"):
            self._write(
                self.fake_bin / command,
                "#!/usr/bin/env bash\nexit 0\n",
                executable=True,
            )
        self._write(
            self.fake_bin / "ss",
            "#!/usr/bin/env bash\nexec /usr/bin/ss \"$@\"\n",
            executable=True,
        )

    def _run(
        self,
        script: Path,
        *args: str,
        env_overrides: dict[str, str] | None = None,
        timeout: int = 20,
    ) -> subprocess.CompletedProcess[str]:
        env = self.base_env.copy()
        if env_overrides:
            env.update(env_overrides)
        return subprocess.run(
            ["bash", str(script), "--host", "fake-host", *args],
            cwd=ROOT,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
            timeout=timeout,
        )

    def _make_remote_env(self, exchange: str, suffix: str = "test") -> Path:
        env_name = f"{exchange}_mm_{suffix}"
        target = self.remote_home / env_name
        (target / "config").mkdir(parents=True)
        (target / "scripts").mkdir()
        (target / "mm_scripts").mkdir()

        if exchange == "binance":
            env_contents = textwrap.dedent(
                """\
                export BINANCE_API_KEY=test-key
                export BINANCE_API_SECRET=test-secret
                export BINANCE_ACCOUNT_MODE=STANDARD
                """
            )
        else:
            env_contents = textwrap.dedent(
                """\
                export OKX_API_KEY=test-key
                export OKX_API_SECRET=test-secret
                export OKX_PASSPHRASE=test-passphrase
                """
            )
        self._write(target / "env.sh", env_contents)
        self._write(target / "config" / "viz.toml", "")
        self._write(target / "config" / "mm_config_server.env", "PORT=19001\n")

        for binary in (
            "trade_signal",
            "viz_server",
            "persist_manager",
            "trade_engine",
            "pre_trade",
            "account_monitor",
        ):
            self._write(
                target / binary,
                "#!/usr/bin/env bash\nexit 0\n",
                executable=True,
            )

        script_names = (
            "mm_config_server.py",
            "mm_process_name.sh",
            "process_match_lib.sh",
            "start_mm_config_server.sh",
            "stop_mm_config_server.sh",
            "start_account_monitor.sh",
            "stop_account_monitor.sh",
            "start_trade_signal.sh",
            "stop_trade_signal.sh",
            "close_mm_all_um_ws_orders.sh",
        )
        for name in script_names:
            self._write(
                target / "scripts" / name,
                "#!/usr/bin/env bash\nexit 0\n",
                executable=True,
            )

        mm_script_names = (
            "start_mm_viz_server.sh",
            "stop_mm_viz_server.sh",
            "start_mm_persist_manager.sh",
            "stop_mm_persist_manager.sh",
            "start_mm_trade_engine.sh",
            "stop_mm_trade_engine.sh",
            "start_mm_pre_trade.sh",
            "stop_mm_pre_trade.sh",
        )
        for name in mm_script_names:
            self._write(
                target / "mm_scripts" / name,
                "#!/usr/bin/env bash\nexit 0\n",
                executable=True,
            )

        helper = (
            "binance_cancel_all_std_um_ws_orders.py"
            if exchange == "binance"
            else "okx_swap_open_orders.py"
        )
        self._write(target / "scripts" / helper, "")
        if exchange == "binance":
            self._write(target / "scripts" / "binance_local_ip.py", "")
        return target

    @staticmethod
    def _free_tcp_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    @staticmethod
    def _find_exact_pids(path: Path) -> list[int]:
        result = subprocess.run(
            ["ps", "-eo", "pid="],
            text=True,
            stdout=subprocess.PIPE,
            check=True,
        )
        pids: list[int] = []
        expected = str(path)
        for raw_pid in result.stdout.splitlines():
            pid = int(raw_pid.strip())
            try:
                executable = os.readlink(f"/proc/{pid}/exe")
            except (FileNotFoundError, PermissionError):
                continue
            if executable.removesuffix(" (deleted)") == expected:
                pids.append(pid)
        return pids

    def _kill_exact_processes(self, paths: list[Path]) -> None:
        for path in paths:
            for pid in self._find_exact_pids(path):
                try:
                    os.kill(pid, 15)
                except ProcessLookupError:
                    pass

    def test_check_only_accepts_binance_and_okex(self) -> None:
        for exchange in ("binance", "okex"):
            with self.subTest(exchange=exchange):
                target = self._make_remote_env(exchange)
                env_name = target.name

                start = self._run(
                    START_SCRIPT, "--env-name", env_name, "--check-only"
                )
                self.assertEqual(start.returncode, 0, start.stdout)
                self.assertIn("no process was started or restarted", start.stdout)

                stop = self._run(
                    STOP_SCRIPT, "--env-name", env_name, "--check-only"
                )
                self.assertEqual(stop.returncode, 0, stop.stdout)
                self.assertIn("no process or order changes were made", stop.stdout)

                publish_args = ["--env-name", env_name, "--check-only"]
                if exchange == "okex":
                    publish_args.extend(["--exchange", "okx"])
                publish = self._run(PUBLISH_SCRIPT, *publish_args)
                self.assertEqual(publish.returncode, 0, publish.stdout)
                self.assertIn("no files were uploaded or replaced", publish.stdout)

    def test_publish_maps_exchange_files_and_account_monitor(self) -> None:
        local_repo = self.temp_path / "local_repo"
        local_script = local_repo / "scripts" / PUBLISH_SCRIPT.name
        local_script.parent.mkdir(parents=True)
        shutil.copy2(PUBLISH_SCRIPT, local_script)

        all_files = set(COMMON_PUBLISH_FILES)
        all_files.update(
            {
                "target/release/binance_account_monitor",
                "target/release/okex_account_monitor",
                "scripts/binance_cancel_all_std_um_ws_orders.py",
                "scripts/binance_local_ip.py",
                "scripts/okx_swap_open_orders.py",
            }
        )
        for relative in all_files:
            self._write(local_repo / relative, f"payload:{relative}\n")

        for exchange in ("binance", "okex"):
            with self.subTest(exchange=exchange):
                env_name = f"{exchange}_mm_publish"
                target = self.remote_home / env_name
                target.mkdir()
                result = self._run(
                    local_script,
                    "--env-name",
                    env_name,
                    "--skip-build",
                )
                self.assertEqual(result.returncode, 0, result.stdout)
                self.assertEqual(
                    (target / "account_monitor").read_text(encoding="utf-8"),
                    f"payload:target/release/{exchange}_account_monitor\n",
                )
                self.assertEqual(
                    (target / "scripts" / "mm_config_server.py").read_text(
                        encoding="utf-8"
                    ),
                    "payload:scripts/mm_config_server.py\n",
                )
                helper = (
                    "binance_cancel_all_std_um_ws_orders.py"
                    if exchange == "binance"
                    else "okx_swap_open_orders.py"
                )
                self.assertTrue((target / "scripts" / helper).is_file())
                self.assertFalse(any(target.glob(".publish-jp-meta-mm.*")))

    def test_publish_refuses_running_target(self) -> None:
        target = self._make_remote_env("binance", suffix="running")
        shutil.copy2("/bin/sleep", target / "trade_engine")
        process = subprocess.Popen([str(target / "trade_engine"), "20"])
        try:
            result = self._run(
                PUBLISH_SCRIPT,
                "--env-name",
                target.name,
                "--check-only",
            )
            self.assertEqual(result.returncode, 3, result.stdout)
            self.assertIn("publish aborted", result.stdout)
        finally:
            process.terminate()
            process.wait(timeout=5)

    def test_rejects_unsupported_or_mismatched_environment(self) -> None:
        for script in (PUBLISH_SCRIPT, START_SCRIPT, STOP_SCRIPT):
            with self.subTest(script=script.name):
                result = self._run(
                    script,
                    "--env-name",
                    "okx_mm_test",
                    "--check-only",
                )
                self.assertEqual(result.returncode, 2, result.stdout)
                self.assertIn("env-name must match", result.stdout)

        mismatch = self._run(
            PUBLISH_SCRIPT,
            "--env-name",
            "binance_mm_test",
            "--exchange",
            "okx",
            "--check-only",
        )
        self.assertEqual(mismatch.returncode, 2, mismatch.stdout)
        self.assertIn("exchange/env-name mismatch", mismatch.stdout)

    def test_live_start_order_and_signal_exclusion_with_fake_remote(self) -> None:
        target = self._make_remote_env("okex", suffix="start")
        port = self._free_tcp_port()
        self._write(
            target / "config" / "mm_config_server.env",
            f"PORT={port}\n",
        )
        action_log = self.temp_path / "start-actions.log"
        action_log.write_text("", encoding="utf-8")

        components = {
            "viz_server": "start_viz_server",
            "persist_manager": "start_persist_manager",
            "trade_engine": "start_trade_engine",
            "pre_trade": "start_pre_trade",
            "account_monitor": "start_account_monitor",
        }
        start_paths = {
            "viz_server": target / "mm_scripts" / "start_mm_viz_server.sh",
            "persist_manager": target / "mm_scripts" / "start_mm_persist_manager.sh",
            "trade_engine": target / "mm_scripts" / "start_mm_trade_engine.sh",
            "pre_trade": target / "mm_scripts" / "start_mm_pre_trade.sh",
            "account_monitor": target / "scripts" / "start_account_monitor.sh",
        }
        process_paths: list[Path] = []
        log_dir = self.remote_home / ".pmdaemon" / "logs"
        log_dir.mkdir(parents=True)
        for component, action in components.items():
            binary = target / component
            shutil.copy2("/bin/sleep", binary)
            binary.chmod(0o755)
            process_paths.append(binary)
            self._write(
                start_paths[component],
                textwrap.dedent(
                    f"""\
                    #!/usr/bin/env bash
                    printf "%s\\n" "{action}" >>"$FAKE_ACTION_LOG"
                    "{binary}" 30 >/dev/null 2>&1 &
                    """
                ),
                executable=True,
            )
            process_name = {
                "viz_server": "mm_viz_okex_start",
                "persist_manager": "mm_pm_okex_start",
                "trade_engine": "mm_te_okex_start",
                "pre_trade": "mm_pt_okex_start",
                "account_monitor": "mm_am_okex_start",
            }[component]
            self._write(log_dir / f"{process_name}-out.log", "")
            self._write(log_dir / f"{process_name}-error.log", "")

        config_script = target / "scripts" / "mm_config_server.py"
        self._write(
            config_script,
            textwrap.dedent(
                """\
                import http.server
                import sys

                class Handler(http.server.BaseHTTPRequestHandler):
                    def do_GET(self):
                        self.send_response(200)
                        self.end_headers()

                    def log_message(self, _format, *_args):
                        pass

                http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
                """
            ),
        )
        pid_file = target / "config-server.pid"
        self._write(
            target / "scripts" / "start_mm_config_server.sh",
            textwrap.dedent(
                f"""\
                #!/usr/bin/env bash
                printf "%s\\n" "start_config_server" >>"$FAKE_ACTION_LOG"
                python3 "{config_script}" "{port}" >/dev/null 2>&1 &
                printf "%s\\n" "$!" >"{pid_file}"
                """
            ),
            executable=True,
        )
        self._write(
            target / "scripts" / "stop_mm_config_server.sh",
            textwrap.dedent(
                f"""\
                #!/usr/bin/env bash
                if [[ -f "{pid_file}" ]]; then
                  kill "$(cat "{pid_file}")" >/dev/null 2>&1 || true
                  rm -f "{pid_file}"
                fi
                """
            ),
            executable=True,
        )

        try:
            result = self._run(
                START_SCRIPT,
                "--env-name",
                target.name,
                env_overrides={
                    "FAKE_ACTION_LOG": str(action_log),
                    "MM_START_WAIT_SECONDS": "5",
                    "MM_START_SETTLE_SECONDS": "1",
                    "MM_START_LOG_LINES": "5",
                },
                timeout=30,
            )
            self.assertEqual(result.returncode, 0, result.stdout)
            self.assertEqual(
                action_log.read_text(encoding="utf-8").splitlines(),
                [
                    "start_config_server",
                    "start_viz_server",
                    "start_persist_manager",
                    "start_trade_engine",
                    "start_pre_trade",
                    "start_account_monitor",
                ],
            )
            self.assertIn("signal_processes_started=false", result.stdout)
            self.assertFalse(self._find_exact_pids(target / "trade_signal"))
            for path in process_paths:
                self.assertEqual(len(self._find_exact_pids(path)), 1)
        finally:
            self._kill_exact_processes(process_paths)
            if pid_file.is_file():
                pid = int(pid_file.read_text(encoding="utf-8").strip())
                try:
                    os.kill(pid, 15)
                except ProcessLookupError:
                    pass

    def _install_stop_fakes(self, target: Path, exchange: str, log: Path) -> None:
        actions = {
            target / "mm_scripts" / "stop_mm_trade_engine.sh": "stop_trade_engine",
            target / "scripts" / "stop_trade_signal.sh": "stop_trade_signal",
            target / "mm_scripts" / "stop_manual_mm_signal.sh": "stop_manual_mm_signal",
            target / "mm_scripts" / "stop_mm_pre_trade.sh": "stop_pre_trade",
            target / "scripts" / "stop_account_monitor.sh": "stop_account_monitor",
            target / "scripts" / "stop_mm_config_server.sh": "stop_config_server",
            target / "mm_scripts" / "stop_mm_persist_manager.sh": "stop_persist_manager",
            target / "mm_scripts" / "stop_mm_viz_server.sh": "stop_viz_server",
        }
        for path, action in actions.items():
            self._write(
                path,
                f'#!/usr/bin/env bash\nprintf "%s\\n" "{action}" >>"$FAKE_ACTION_LOG"\n',
                executable=True,
            )

        empty_message = (
            "[plan] no open UM futures orders found"
            if exchange == "binance"
            else "Open orders count: 0"
        )
        self._write(
            target / "scripts" / "close_mm_all_um_ws_orders.sh",
            textwrap.dedent(
                f"""\
                #!/usr/bin/env bash
                mode=query
                for arg in "$@"; do
                  if [[ "$arg" == "--execute" ]]; then
                    mode=execute
                  fi
                done
                printf "cancel_%s\\n" "$mode" >>"$FAKE_ACTION_LOG"
                printf "%s\\n" "{empty_message}"
                """
            ),
            executable=True,
        )
        log.write_text("", encoding="utf-8")

    def test_live_stop_order_for_binance_and_okex_with_fake_remote(self) -> None:
        expected = [
            "stop_trade_engine",
            "cancel_execute",
            "cancel_query",
            "stop_trade_signal",
            "stop_manual_mm_signal",
            "stop_pre_trade",
            "stop_account_monitor",
            "stop_config_server",
            "stop_persist_manager",
            "stop_viz_server",
        ]
        for exchange in ("binance", "okex"):
            with self.subTest(exchange=exchange):
                target = self._make_remote_env(exchange, suffix="stop")
                log = self.temp_path / f"{exchange}-actions.log"
                self._install_stop_fakes(target, exchange, log)
                result = self._run(
                    STOP_SCRIPT,
                    "--env-name",
                    target.name,
                    env_overrides={"FAKE_ACTION_LOG": str(log)},
                )
                self.assertEqual(result.returncode, 0, result.stdout)
                self.assertEqual(log.read_text(encoding="utf-8").splitlines(), expected)
                self.assertIn("orders_empty=true processes_stopped=true", result.stdout)

    def test_stop_never_cancels_while_trade_engine_is_running(self) -> None:
        target = self._make_remote_env("binance", suffix="guard")
        log = self.temp_path / "guard-actions.log"
        self._install_stop_fakes(target, "binance", log)
        shutil.copy2("/bin/sleep", target / "trade_engine")
        process = subprocess.Popen([str(target / "trade_engine"), "20"])
        try:
            result = self._run(
                STOP_SCRIPT,
                "--env-name",
                target.name,
                env_overrides={"FAKE_ACTION_LOG": str(log)},
            )
            self.assertEqual(result.returncode, 1, result.stdout)
            self.assertIn("refusing to cancel", result.stdout)
            self.assertEqual(
                log.read_text(encoding="utf-8").splitlines(),
                ["stop_trade_engine"],
            )
        finally:
            process.terminate()
            process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
