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
PUBLISH_SCRIPT = ROOT / "scripts" / "publish-exec.sh"
START_SCRIPT = ROOT / "scripts" / "start-exec.sh"
STOP_SCRIPT = ROOT / "scripts" / "stop-exec.sh"


COMMON_PUBLISH_FILES = [
    "target/release/exec-pre-trade",
    "target/release/trade_signal",
    "target/release/viz_server",
    "target/release/trade_engine",
    "target/release/persist_manager",
    "scripts/exec_config_server.py",
    "scripts/mm_process_name.sh",
    "scripts/process_match_lib.sh",
    "scripts/start_exec_config_server.sh",
    "scripts/stop_exec_config_server.sh",
    "scripts/start_exec_viz_server.sh",
    "scripts/stop_exec_viz_server.sh",
    "scripts/start_exec_persist_manager.sh",
    "scripts/stop_exec_persist_manager.sh",
    "scripts/start_exec_trade_engine.sh",
    "scripts/stop_exec_trade_engine.sh",
    "scripts/start_exec_pre_trade.sh",
    "scripts/stop_exec_pre_trade.sh",
    "scripts/start_exec_trade_signal.sh",
    "scripts/stop_exec_trade_signal.sh",
    "scripts/start_account_monitor.sh",
    "scripts/stop_account_monitor.sh",
    "scripts/start_trade_engine.sh",
    "scripts/stop_trade_engine.sh",
    "scripts/start_fr_persist_manager.sh",
    "scripts/stop_fr_persist_manager.sh",
    "scripts/print_exec_risk_params.py",
    "scripts/sync_exec_risk_params.py",
    "scripts/print_exec_max_pos_u.py",
    "scripts/sync_exec_max_pos_u.py",
]


class ExecOrchestrationTests(unittest.TestCase):
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
                if [[ $# -eq 0 ]]; then
                  echo "unexpected empty ssh command" >&2
                  exit 97
                fi
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

    def _make_remote_env(self, exchange: str, suffix: str = "trade01") -> Path:
        env_name = f"{exchange}_exec_{suffix}"
        venue = f"{exchange}-futures"
        target = self.remote_home / env_name
        (target / "config").mkdir(parents=True)
        (target / "scripts").mkdir()

        if exchange == "binance":
            env_contents = textwrap.dedent(
                f"""\
                export IPC_NAMESPACE={env_name}
                export EXEC_VENUE={venue}
                export VENUE={venue}
                export BINANCE_API_KEY=test-key
                export BINANCE_API_SECRET=test-secret
                """
            )
        else:
            env_contents = textwrap.dedent(
                f"""\
                export IPC_NAMESPACE={env_name}
                export EXEC_VENUE={venue}
                export VENUE={venue}
                export OKX_API_KEY=test-key
                export OKX_API_SECRET=test-secret
                export OKX_PASSPHRASE=test-passphrase
                """
            )
        self._write(target / "env.sh", env_contents)
        self._write(target / "config" / "exec_viz.toml", "")
        self._write(target / "config" / "exec_config_server.env", "PORT=18161\n")

        for binary in (
            "exec-pre-trade",
            "trade_signal",
            "viz_server",
            "persist_manager",
            "trade_engine",
            "account_monitor",
        ):
            self._write(
                target / binary,
                "#!/usr/bin/env bash\nexit 0\n",
                executable=True,
            )

        script_names = (
            "exec_config_server.py",
            "mm_process_name.sh",
            "process_match_lib.sh",
            "start_exec_config_server.sh",
            "stop_exec_config_server.sh",
            "start_exec_viz_server.sh",
            "stop_exec_viz_server.sh",
            "start_exec_persist_manager.sh",
            "stop_exec_persist_manager.sh",
            "start_exec_trade_engine.sh",
            "stop_exec_trade_engine.sh",
            "start_exec_pre_trade.sh",
            "stop_exec_pre_trade.sh",
            "start_exec_trade_signal.sh",
            "stop_exec_trade_signal.sh",
            "start_account_monitor.sh",
            "stop_account_monitor.sh",
        )
        for name in script_names:
            self._write(
                target / "scripts" / name,
                "#!/usr/bin/env bash\nexit 0\n",
                executable=True,
            )
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

    def test_invalid_env_name_exits_2_before_ssh(self) -> None:
        ssh_log = self.temp_path / "ssh.log"
        self._write(
            self.fake_bin / "ssh",
            textwrap.dedent(
                """\
                #!/usr/bin/env bash
                printf 'unexpected\\n' >>"$FAKE_SSH_LOG"
                exit 99
                """
            ),
            executable=True,
        )
        for script in (PUBLISH_SCRIPT, START_SCRIPT, STOP_SCRIPT):
            with self.subTest(script=script.name):
                ssh_log.unlink(missing_ok=True)
                result = self._run(
                    script,
                    "--env-name",
                    "okx_exec_trade01",
                    "--check-only",
                    env_overrides={"FAKE_SSH_LOG": str(ssh_log)},
                )
                self.assertEqual(result.returncode, 2, result.stdout)
                self.assertIn("env-name must match", result.stdout)
                self.assertFalse(ssh_log.exists())

        mismatch = self._run(
            PUBLISH_SCRIPT,
            "--env-name",
            "binance_exec_trade01",
            "--venue",
            "okex-futures",
            "--check-only",
        )
        self.assertEqual(mismatch.returncode, 2, mismatch.stdout)
        self.assertIn("venue/env-name mismatch", mismatch.stdout)

        for script in (PUBLISH_SCRIPT, START_SCRIPT, STOP_SCRIPT):
            with self.subTest(invalid_host=script.name):
                ssh_log.unlink(missing_ok=True)
                result = self._run(
                    script,
                    "--env-name",
                    "binance_exec_trade01",
                    "--host",
                    "-badhost",
                    "--check-only",
                    env_overrides={"FAKE_SSH_LOG": str(ssh_log)},
                )
                self.assertEqual(result.returncode, 2, result.stdout)
                self.assertIn("invalid SSH host", result.stdout)
                self.assertFalse(ssh_log.exists())

    def test_check_only_start_and_stop_do_not_invoke_helpers(self) -> None:
        for exchange in ("binance", "okex"):
            with self.subTest(exchange=exchange):
                target = self._make_remote_env(exchange)
                action_log = self.temp_path / f"{exchange}-check.log"
                action_log.write_text("", encoding="utf-8")
                for name in (
                    "start_exec_persist_manager.sh",
                    "start_exec_trade_engine.sh",
                    "start_account_monitor.sh",
                    "start_exec_pre_trade.sh",
                    "start_exec_viz_server.sh",
                    "start_exec_config_server.sh",
                    "start_exec_trade_signal.sh",
                    "stop_exec_trade_engine.sh",
                    "stop_exec_pre_trade.sh",
                    "stop_exec_trade_signal.sh",
                    "stop_account_monitor.sh",
                    "stop_exec_persist_manager.sh",
                    "stop_exec_viz_server.sh",
                    "stop_exec_config_server.sh",
                ):
                    self._write(
                        target / "scripts" / name,
                        f'#!/usr/bin/env bash\nprintf "%s\\n" "{name}" >>"$FAKE_ACTION_LOG"\n',
                        executable=True,
                    )

                start = self._run(
                    START_SCRIPT,
                    "--env-name",
                    target.name,
                    "--check-only",
                    env_overrides={"FAKE_ACTION_LOG": str(action_log)},
                )
                self.assertEqual(start.returncode, 0, start.stdout)
                self.assertIn("no process was started or restarted", start.stdout)
                self.assertEqual(action_log.read_text(encoding="utf-8"), "")

                stop = self._run(
                    STOP_SCRIPT,
                    "--env-name",
                    target.name,
                    "--check-only",
                    env_overrides={"FAKE_ACTION_LOG": str(action_log)},
                )
                self.assertEqual(stop.returncode, 0, stop.stdout)
                self.assertIn("no process changes were made", stop.stdout)
                self.assertEqual(action_log.read_text(encoding="utf-8"), "")

    def test_publish_check_only_refuses_running_target_and_uploads_nothing(self) -> None:
        target = self._make_remote_env("binance", suffix="run01")
        marker = target / "account_monitor"
        before = marker.read_text(encoding="utf-8")
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
            self.assertFalse(any(target.glob(".publish-exec.*")))
            self.assertEqual(marker.read_text(encoding="utf-8"), before)
        finally:
            process.terminate()
            process.wait(timeout=5)

    def test_publish_skip_build_maps_exec_files_and_cleans_staging(self) -> None:
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
                "scripts/binance_cancel_all_unified_open_orders.py",
                "scripts/binance_local_ip.py",
                "scripts/sell_margin_spot.py",
                "scripts/okx_swap_open_orders.py",
            }
        )
        for relative in all_files:
            self._write(local_repo / relative, f"payload:{relative}\n")

        for exchange in ("binance", "okex"):
            with self.subTest(exchange=exchange):
                env_name = f"{exchange}_exec_pub01"
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
                    (target / "exec-pre-trade").read_text(encoding="utf-8"),
                    "payload:target/release/exec-pre-trade\n",
                )
                self.assertEqual(
                    (target / "scripts" / "exec_config_server.py").read_text(
                        encoding="utf-8"
                    ),
                    "payload:scripts/exec_config_server.py\n",
                )
                helper = (
                    "binance_cancel_all_std_um_ws_orders.py"
                    if exchange == "binance"
                    else "okx_swap_open_orders.py"
                )
                self.assertTrue((target / "scripts" / helper).is_file())
                self.assertFalse((target / "env.sh").exists())
                self.assertFalse(any(target.glob(".publish-exec.*")))

    def test_live_start_order_and_signal_exclusion_with_fake_remote(self) -> None:
        target = self._make_remote_env("okex", suffix="start01")
        port = self._free_tcp_port()
        self._write(
            target / "config" / "exec_config_server.env",
            f"PORT={port}\n",
        )
        action_log = self.temp_path / "start-actions.log"
        action_log.write_text("", encoding="utf-8")

        components = {
            "persist_manager": "start_persist_manager",
            "trade_engine": "start_trade_engine",
            "account_monitor": "start_account_monitor",
            "exec-pre-trade": "start_exec_pre_trade",
            "viz_server": "start_viz_server",
        }
        start_paths = {
            "persist_manager": target / "scripts" / "start_exec_persist_manager.sh",
            "trade_engine": target / "scripts" / "start_exec_trade_engine.sh",
            "account_monitor": target / "scripts" / "start_account_monitor.sh",
            "exec-pre-trade": target / "scripts" / "start_exec_pre_trade.sh",
            "viz_server": target / "scripts" / "start_exec_viz_server.sh",
        }
        process_paths: list[Path] = []
        log_dir = self.remote_home / ".pmdaemon" / "logs"
        log_dir.mkdir(parents=True)
        process_names = {
            "persist_manager": "exec_pm_okex_exec_start01",
            "trade_engine": "exec_te_okex_exec_start01",
            "account_monitor": "exec_am_ok_start01",
            "exec-pre-trade": "exec_pt_okex_exec_start01",
            "viz_server": "exec_vz_okex_exec_start01",
        }
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
            self._write(log_dir / f"{process_names[component]}-out.log", "")
            self._write(log_dir / f"{process_names[component]}-error.log", "")

        config_script = target / "scripts" / "exec_config_server.py"
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
            target / "scripts" / "start_exec_config_server.sh",
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
            target / "scripts" / "stop_exec_config_server.sh",
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
        self._write(
            target / "scripts" / "start_exec_trade_signal.sh",
            textwrap.dedent(
                """\
                #!/usr/bin/env bash
                printf "%s\\n" "start_trade_signal" >>"$FAKE_ACTION_LOG"
                exit 1
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
                    "EXEC_START_WAIT_SECONDS": "3",
                    "EXEC_START_SETTLE_SECONDS": "1",
                    "EXEC_START_LOG_LINES": "5",
                },
                timeout=20,
            )
            self.assertEqual(result.returncode, 0, result.stdout)
            self.assertEqual(
                action_log.read_text(encoding="utf-8").splitlines(),
                [
                    "start_persist_manager",
                    "start_trade_engine",
                    "start_account_monitor",
                    "start_exec_pre_trade",
                    "start_viz_server",
                    "start_config_server",
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

    def test_live_stop_order_with_fake_remote(self) -> None:
        target = self._make_remote_env("binance", suffix="stop01")
        action_log = self.temp_path / "stop-actions.log"
        action_log.write_text("", encoding="utf-8")
        actions = {
            target / "scripts" / "stop_exec_trade_engine.sh": "stop_trade_engine",
            target / "scripts" / "stop_exec_pre_trade.sh": "stop_exec_pre_trade",
            target / "scripts" / "stop_exec_trade_signal.sh": "stop_trade_signal",
            target / "scripts" / "stop_account_monitor.sh": "stop_account_monitor",
            target / "scripts" / "stop_exec_persist_manager.sh": "stop_persist_manager",
            target / "scripts" / "stop_exec_viz_server.sh": "stop_viz_server",
            target / "scripts" / "stop_exec_config_server.sh": "stop_config_server",
        }
        for path, action in actions.items():
            self._write(
                path,
                f'#!/usr/bin/env bash\nprintf "%s\\n" "{action}" >>"$FAKE_ACTION_LOG"\n',
                executable=True,
            )
        result = self._run(
            STOP_SCRIPT,
            "--env-name",
            target.name,
            env_overrides={"FAKE_ACTION_LOG": str(action_log)},
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(
            action_log.read_text(encoding="utf-8").splitlines(),
            [
                "stop_trade_engine",
                "stop_exec_pre_trade",
                "stop_trade_signal",
                "stop_account_monitor",
                "stop_persist_manager",
                "stop_viz_server",
                "stop_config_server",
            ],
        )
        self.assertIn("processes_stopped=true", result.stdout)


if __name__ == "__main__":
    unittest.main()
