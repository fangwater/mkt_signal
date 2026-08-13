from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BUILD_SCRIPT = ROOT / "scripts" / "build-sg-mm-binaries.sh"
PUBLISH_SCRIPT = ROOT / "scripts" / "publish-sg-mm.sh"
START_SCRIPT = ROOT / "scripts" / "start-sg-mm.sh"
STOP_SCRIPT = ROOT / "scripts" / "stop-sg-mm.sh"
UPDATE_SCRIPT = ROOT / "scripts" / "update-sg-mm.sh"


class SGMMOrchestrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)
        self.fake_bin = self.temp_path / "fake-bin"
        self.fake_bin.mkdir()
        self.key = self.temp_path / "aws-sg.pem"
        self.key.write_text("test-only\n", encoding="utf-8")
        self.key.chmod(0o600)
        self.action_log = self.temp_path / "actions.log"
        self.base_env = os.environ.copy()
        self.base_env["PATH"] = f"{self.fake_bin}{os.pathsep}{self.base_env['PATH']}"
        self.base_env["FAKE_ACTION_LOG"] = str(self.action_log)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    @staticmethod
    def _write(path: Path, contents: str, executable: bool = False) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents, encoding="utf-8")
        if executable:
            path.chmod(0o755)

    @staticmethod
    def _copy_script(source: Path, repo: Path) -> Path:
        destination = repo / "scripts" / source.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        return destination

    def _run(
        self,
        script: Path,
        *args: str,
        env_overrides: dict[str, str] | None = None,
        timeout: int = 30,
    ) -> subprocess.CompletedProcess[str]:
        env = self.base_env.copy()
        if env_overrides:
            env.update(env_overrides)
        return subprocess.run(
            ["bash", str(script), *args],
            cwd=script.parent.parent,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
            timeout=timeout,
        )

    def _install_remote_ssh(self, *, execute_remote: bool, check_status: int = 0) -> None:
        execute = "1" if execute_remote else "0"
        self._write(
            self.fake_bin / "ssh",
            textwrap.dedent(
                f"""\
                #!/usr/bin/env bash
                set -euo pipefail
                printf 'ssh:%s\\n' "$*" >>"$FAKE_ACTION_LOG.ssh"
                while [[ $# -gt 0 ]]; do
                  case "$1" in
                    -i|-o) shift 2 ;;
                    --) shift; break ;;
                    -*) shift ;;
                    *) break ;;
                  esac
                done
                [[ $# -gt 0 ]]
                shift
                if [[ $# -eq 1 && "$1" == *'printf'*HOME* ]]; then
                  printf '%s\\n' "$FAKE_REMOTE_HOME"
                  exit 0
                fi
                if [[ $# -eq 1 && "$1" == readlink* ]]; then
                  printf '%s/%s\\n' "$FAKE_REMOTE_HOME" "$FAKE_ENV_NAME"
                  exit 0
                fi
                if [[ "${{1:-}}" == "bash" && "${{2:-}}" == "-s" ]]; then
                  shift 2
                  if [[ "{execute}" == "1" ]]; then
                    exec /bin/bash -s "$@"
                  fi
                  /bin/cat >/dev/null
                  echo "[INFO] fake remote process check"
                  exit {check_status}
                fi
                echo "unexpected fake ssh command: $*" >&2
                exit 97
                """
            ),
            executable=True,
        )

    def _remote_env(self, env_name: str = "bybit_mm_beta") -> tuple[Path, dict[str, str]]:
        remote_home = self.temp_path / "remote-home"
        remote_dir = remote_home / env_name
        remote_dir.mkdir(parents=True, exist_ok=True)
        return remote_dir, {
            "FAKE_REMOTE_HOME": str(remote_home),
            "FAKE_REMOTE_DIR": str(remote_dir),
            "FAKE_ENV_NAME": env_name,
        }

    def test_builds_all_release_binaries_including_runtime_persist_manager(self) -> None:
        repo = self.temp_path / "build-repo"
        script = self._copy_script(BUILD_SCRIPT, repo)
        cargo_log = self.temp_path / "cargo.log"
        self._write(
            self.fake_bin / "cargo",
            textwrap.dedent(
                """\
                #!/usr/bin/env bash
                set -euo pipefail
                printf '%s\n' "$*" >>"$FAKE_CARGO_LOG"
                target_dir=""
                bins=()
                while [[ $# -gt 0 ]]; do
                  case "$1" in
                    --target-dir) target_dir="$2"; shift 2 ;;
                    --bin) bins+=("$2"); shift 2 ;;
                    *) shift ;;
                  esac
                done
                mkdir -p "$target_dir/release"
                for binary in "${bins[@]}"; do
                  printf '#!/usr/bin/env bash\nexit 0\n' >"$target_dir/release/$binary"
                  chmod 755 "$target_dir/release/$binary"
                done
                """
            ),
            executable=True,
        )

        result = self._run(script, env_overrides={"FAKE_CARGO_LOG": str(cargo_log)})
        self.assertEqual(result.returncode, 0, result.stdout)
        commands = cargo_log.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(commands), 4)
        self.assertIn("--bin bybit_account_monitor", commands[0])
        self.assertIn("--bin pre_trade", commands[0])
        self.assertIn("--bin trade_engine", commands[0])
        self.assertIn("-p trade_signal --bin trade_signal", commands[1])
        self.assertIn("-p viz_server --bin viz_server", commands[2])
        self.assertIn("-p persist_manager --features runtime --bin persist_manager", commands[3])

    def test_every_entrypoint_rejects_non_whitelisted_environment_before_ssh(self) -> None:
        self._write(
            self.fake_bin / "ssh",
            "#!/usr/bin/env bash\nprintf 'unexpected\n' >>\"$FAKE_ACTION_LOG\"\nexit 99\n",
            executable=True,
        )
        for script in (PUBLISH_SCRIPT, START_SCRIPT, STOP_SCRIPT, UPDATE_SCRIPT):
            with self.subTest(script=script.name):
                self.action_log.unlink(missing_ok=True)
                result = self._run(
                    script,
                    "--key",
                    str(self.key),
                    "--env-name",
                    "bybit_mm_gamma",
                )
                self.assertEqual(result.returncode, 2, result.stdout)
                self.assertFalse(self.action_log.exists())

    def _install_update_fakes(
        self, repo: Path, *, build_status: int = 0, stop_status: int = 0
    ) -> None:
        statuses = {
            BUILD_SCRIPT.name: build_status,
            STOP_SCRIPT.name: stop_status,
            PUBLISH_SCRIPT.name: 0,
            START_SCRIPT.name: 0,
        }
        for name, status in statuses.items():
            self._write(
                repo / "scripts" / name,
                textwrap.dedent(
                    f"""\
                    #!/usr/bin/env bash
                    printf '%s:%s\\n' '{name.removesuffix('.sh')}' "$*" >>"$FAKE_ACTION_LOG"
                    exit {status}
                    """
                ),
                executable=True,
            )

    def test_update_all_builds_once_then_updates_beta_and_alpha(self) -> None:
        repo = self.temp_path / "update-repo"
        self._install_update_fakes(repo)
        script = self._copy_script(UPDATE_SCRIPT, repo)

        result = self._run(
            script,
            "--host",
            "fake-sg",
            "--key",
            str(self.key),
            "--all",
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        key = str(self.key.resolve())
        self.assertEqual(
            self.action_log.read_text(encoding="utf-8").splitlines(),
            [
                "build-sg-mm-binaries:",
                f"stop-sg-mm:--host fake-sg --key {key} --env-name bybit_mm_beta",
                f"publish-sg-mm:--host fake-sg --key {key} --env-name bybit_mm_beta --skip-build",
                f"start-sg-mm:--host fake-sg --key {key} --env-name bybit_mm_beta",
                f"stop-sg-mm:--host fake-sg --key {key} --env-name bybit_mm_alpha",
                f"publish-sg-mm:--host fake-sg --key {key} --env-name bybit_mm_alpha --skip-build",
                f"start-sg-mm:--host fake-sg --key {key} --env-name bybit_mm_alpha",
            ],
        )

    def test_update_failure_does_not_advance_to_next_live_phase(self) -> None:
        cases = (
            (23, 0, ["build-sg-mm-binaries:"]),
            (
                0,
                24,
                [
                    "build-sg-mm-binaries:",
                    f"stop-sg-mm:--host fake-sg --key {self.key.resolve()} --env-name bybit_mm_beta",
                ],
            ),
        )
        for build_status, stop_status, expected in cases:
            with self.subTest(build_status=build_status, stop_status=stop_status):
                self.action_log.unlink(missing_ok=True)
                repo = self.temp_path / f"failed-{build_status}-{stop_status}"
                self._install_update_fakes(
                    repo, build_status=build_status, stop_status=stop_status
                )
                script = self._copy_script(UPDATE_SCRIPT, repo)
                result = self._run(
                    script,
                    "--host",
                    "fake-sg",
                    "--key",
                    str(self.key),
                    "--env-name",
                    "bybit_mm_beta",
                )
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertEqual(
                    self.action_log.read_text(encoding="utf-8").splitlines(), expected
                )

    def test_publish_build_failure_occurs_before_first_ssh(self) -> None:
        repo = self.temp_path / "publish-repo"
        script = self._copy_script(PUBLISH_SCRIPT, repo)
        self._write(
            repo / "scripts" / BUILD_SCRIPT.name,
            "#!/usr/bin/env bash\nprintf 'build\n' >>\"$FAKE_ACTION_LOG\"\nexit 17\n",
            executable=True,
        )
        self._write(
            self.fake_bin / "ssh",
            "#!/usr/bin/env bash\nprintf 'ssh\n' >>\"$FAKE_ACTION_LOG\"\nexit 99\n",
            executable=True,
        )

        result = self._run(
            script,
            "--key",
            str(self.key),
            "--env-name",
            "bybit_mm_beta",
        )
        self.assertEqual(result.returncode, 17, result.stdout)
        self.assertEqual(self.action_log.read_text(encoding="utf-8").splitlines(), ["build"])

    def test_publish_check_only_has_no_build_or_copy_side_effect(self) -> None:
        _, remote_env = self._remote_env()
        self._write(
            self.fake_bin / "scp",
            "#!/usr/bin/env bash\nprintf 'scp\n' >>\"$FAKE_ACTION_LOG\"\nexit 99\n",
            executable=True,
        )
        self._install_remote_ssh(execute_remote=False)
        result = self._run(
            PUBLISH_SCRIPT,
            "--host",
            "fake-sg",
            "--key",
            str(self.key),
            "--env-name",
            "bybit_mm_beta",
            "--check-only",
            env_overrides=remote_env,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertFalse(self.action_log.exists())

        self._install_remote_ssh(execute_remote=False, check_status=3)
        failed = self._run(
            PUBLISH_SCRIPT,
            "--host",
            "fake-sg",
            "--key",
            str(self.key),
            "--env-name",
            "bybit_mm_beta",
            "--check-only",
            env_overrides=remote_env,
        )
        self.assertEqual(failed.returncode, 3, failed.stdout)
        self.assertFalse(self.action_log.exists())

    def _prepare_stop_remote(
        self, *, cancel_mode: str = "success"
    ) -> tuple[Path, dict[str, str]]:
        remote_dir, remote_env = self._remote_env()
        scripts_dir = remote_dir / "scripts"
        mm_dir = remote_dir / "mm_scripts"
        self._write(
            remote_dir / "env.sh",
            "export BYBIT_API_KEY='test-key'\nexport BYBIT_API_SECRET='test-secret'\n",
        )
        self._write(remote_dir / "trade_engine.toml", 'local_ips = ["172.31.7.123"]\n')
        self._write(scripts_dir / "mm_process_name.sh", "#!/usr/bin/env bash\n")
        self._write(scripts_dir / "process_match_lib.sh", "#!/usr/bin/env bash\n")
        self._write(scripts_dir / "bybit_cancel_all_um_orders.py", "# test marker\n")
        self._write(scripts_dir / "binance_local_ip.py", "# test marker\n")
        self._write(
            scripts_dir / "close_mm_all_um_ws_orders.sh",
            textwrap.dedent(
                """\
                #!/usr/bin/env bash
                set -euo pipefail
                printf 'cancel:%s\n' "$*" >>"$FAKE_ACTION_LOG"
                execute=0
                for arg in "$@"; do
                  [[ "$arg" == "--execute" ]] && execute=1
                done
                if [[ "$execute" -eq 1 ]]; then
                  case "${FAKE_CANCEL_MODE:-success}" in
                    warning) echo '[WARN] fake Bybit cancel warning' ;;
                    failure) echo '[ERROR] fake Bybit cancel failure'; exit 19 ;;
                    *) echo '[bybit] open linear orders: 2' ;;
                  esac
                elif [[ "${FAKE_CANCEL_MODE:-success}" == "residual" ]]; then
                  echo '[bybit] open linear orders: 1'
                else
                  echo '[bybit] open linear orders: 0'
                fi
                """
            ),
            executable=True,
        )
        wrappers = {
            mm_dir / "stop_mm_trade_engine.sh": "stop-engine",
            scripts_dir / "stop_trade_signal.sh": "stop-signal",
            mm_dir / "stop_manual_mm_signal.sh": "stop-manual-signal",
            mm_dir / "stop_mm_pre_trade.sh": "stop-pre-trade",
            scripts_dir / "stop_account_monitor.sh": "stop-monitor",
            scripts_dir / "stop_mm_config_server.sh": "stop-config",
            mm_dir / "stop_mm_persist_manager.sh": "stop-persist",
            mm_dir / "stop_mm_viz_server.sh": "stop-viz",
        }
        for path, action in wrappers.items():
            self._write(
                path,
                f"#!/usr/bin/env bash\nprintf '%s:%s\\n' '{action}' \"$*\" >>\"$FAKE_ACTION_LOG\"\n",
                executable=True,
            )
        for command in ("pmdaemon", "npx"):
            self._write(self.fake_bin / command, "#!/usr/bin/env bash\nexit 0\n", executable=True)
        self._write(
            self.fake_bin / "ps",
            textwrap.dedent(
                """\
                #!/usr/bin/env bash
                case "$*" in
                  "-eo pid=,comm="|"-eo pid=,args=") exit 0 ;;
                  *) exec /bin/ps "$@" ;;
                esac
                """
            ),
            executable=True,
        )
        self._write(self.fake_bin / "sleep", "#!/usr/bin/env bash\nexit 0\n", executable=True)
        self._install_remote_ssh(execute_remote=True)
        remote_env["FAKE_CANCEL_MODE"] = cancel_mode
        return remote_dir, remote_env

    def test_stop_confirms_engine_then_executes_and_verifies_cancel(self) -> None:
        _, remote_env = self._prepare_stop_remote()
        result = self._run(
            STOP_SCRIPT,
            "--host",
            "fake-sg",
            "--key",
            str(self.key),
            "--env-name",
            "bybit_mm_beta",
            env_overrides=remote_env,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        target = remote_env["FAKE_REMOTE_DIR"]
        self.assertEqual(
            self.action_log.read_text(encoding="utf-8").splitlines(),
            [
                "stop-engine:",
                f"cancel:--env-name bybit_mm_beta --env-dir {target} --require-local-address --execute",
                f"cancel:--env-name bybit_mm_beta --env-dir {target} --require-local-address",
                "stop-signal:bybit",
                "stop-manual-signal:",
                "stop-pre-trade:",
                "stop-monitor:",
                "stop-config:",
                "stop-persist:",
                "stop-viz:--exchange bybit",
            ],
        )
        self.assertLess(
            result.stdout.index("trade_engine confirmed stopped"),
            result.stdout.index("cancel all Bybit linear"),
        )
        self.assertIn("all Bybit linear open orders confirmed empty", result.stdout)

    def test_cancel_warning_aborts_before_stopping_remaining_components(self) -> None:
        _, remote_env = self._prepare_stop_remote(cancel_mode="warning")
        result = self._run(
            STOP_SCRIPT,
            "--host",
            "fake-sg",
            "--key",
            str(self.key),
            "--env-name",
            "bybit_mm_beta",
            env_overrides=remote_env,
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertEqual(
            self.action_log.read_text(encoding="utf-8").splitlines()[:1],
            ["stop-engine:"],
        )
        self.assertEqual(len(self.action_log.read_text(encoding="utf-8").splitlines()), 2)
        self.assertIn("--execute", self.action_log.read_text(encoding="utf-8"))
        self.assertIn("cancel script reported a warning/error", result.stdout)

    def test_residual_orders_are_retried_and_block_remaining_stop(self) -> None:
        _, remote_env = self._prepare_stop_remote(cancel_mode="residual")
        result = self._run(
            STOP_SCRIPT,
            "--host",
            "fake-sg",
            "--key",
            str(self.key),
            "--env-name",
            "bybit_mm_beta",
            env_overrides=remote_env,
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)
        actions = self.action_log.read_text(encoding="utf-8").splitlines()
        self.assertEqual(actions[0], "stop-engine:")
        self.assertEqual(sum("--execute" in action for action in actions), 1)
        self.assertEqual(sum(action.startswith("cancel:") for action in actions), 4)
        self.assertFalse(any(action.startswith("stop-signal") for action in actions))
        self.assertIn("open linear orders remain", result.stdout)

    def _prepare_start_remote(
        self, env_name: str = "bybit_mm_beta"
    ) -> tuple[Path, dict[str, str]]:
        remote_dir, remote_env = self._remote_env(env_name)
        marker_dir = self.temp_path / f"markers-{env_name}"
        marker_dir.mkdir()
        scripts_dir = remote_dir / "scripts"
        mm_dir = remote_dir / "mm_scripts"
        config_port = 18141 if env_name == "bybit_mm_beta" else 18142
        viz_port = 10241 if env_name == "bybit_mm_beta" else 10242
        self._write(
            remote_dir / "env.sh",
            "export IPC_NAMESPACE='test-mm'\n"
            "export BYBIT_API_KEY='test-key'\n"
            "export BYBIT_API_SECRET='test-secret'\n",
        )
        self._write(remote_dir / "trade_engine.toml", 'local_ips = ["172.31.7.123"]\n')
        self._write(
            remote_dir / "config" / "viz.toml",
            f"[[servers]]\n[servers.http]\nbind = \"0.0.0.0\"\nport = {viz_port}\n",
        )
        self._write(
            remote_dir / "config" / "mm_config_server.env", f"PORT={config_port}\n"
        )
        self._write(scripts_dir / "mm_config_server.py", "# test marker\n")
        self._write(scripts_dir / "mm_process_name.sh", "#!/usr/bin/env bash\n")
        self._write(scripts_dir / "process_match_lib.sh", "#!/usr/bin/env bash\n")

        for binary in (
            "trade_signal",
            "account_monitor",
            "viz_server",
            "pre_trade",
            "trade_engine",
            "persist_manager",
        ):
            self._write(remote_dir / binary, "#!/usr/bin/env bash\nexit 0\n", executable=True)

        start_actions = {
            scripts_dir / "start_mm_config_server.sh": ("config", "config"),
            mm_dir / "start_mm_viz_server.sh": ("viz", "viz"),
            mm_dir / "start_mm_persist_manager.sh": ("persist", "persist"),
            mm_dir / "start_mm_trade_engine.sh": ("engine", "engine"),
            mm_dir / "start_mm_pre_trade.sh": ("pre", "pre-trade"),
            scripts_dir / "start_account_monitor.sh": ("monitor", "monitor"),
            scripts_dir / "start_trade_signal.sh": ("signal", "signal"),
        }
        for path, (marker, action) in start_actions.items():
            self._write(
                path,
                textwrap.dedent(
                    f"""\
                    #!/usr/bin/env bash
                    touch "$FAKE_MARKER_DIR/{marker}"
                    printf '%s:%s\\n' '{action}' "$*" >>"$FAKE_ACTION_LOG"
                    """
                ),
                executable=True,
            )
        for name in (
            "stop_mm_config_server.sh",
            "stop_account_monitor.sh",
            "stop_trade_signal.sh",
        ):
            self._write(scripts_dir / name, "#!/usr/bin/env bash\nexit 0\n", executable=True)
        for name in (
            "stop_mm_viz_server.sh",
            "stop_mm_persist_manager.sh",
            "stop_mm_trade_engine.sh",
            "stop_mm_pre_trade.sh",
        ):
            self._write(mm_dir / name, "#!/usr/bin/env bash\nexit 0\n", executable=True)

        self._write(
            self.fake_bin / "ps",
            textwrap.dedent(
                """\
                #!/usr/bin/env bash
                set -euo pipefail
                if [[ "$*" == "-eo pid=,comm=" ]]; then
                  [[ -f "$FAKE_MARKER_DIR/viz" ]] && echo '901 viz_server'
                  [[ -f "$FAKE_MARKER_DIR/persist" ]] && echo '902 persist_manager'
                  [[ -f "$FAKE_MARKER_DIR/engine" ]] && echo '903 trade_engine'
                  [[ -f "$FAKE_MARKER_DIR/pre" ]] && echo '904 pre_trade'
                  [[ -f "$FAKE_MARKER_DIR/monitor" ]] && echo '905 account_monitor'
                  [[ -f "$FAKE_MARKER_DIR/signal" ]] && echo '906 trade_signal'
                  exit 0
                fi
                if [[ "$*" == "-eo pid=,args=" ]]; then
                  if [[ -f "$FAKE_MARKER_DIR/config" ]]; then
                    echo "900 python3 $FAKE_REMOTE_DIR/scripts/mm_config_server.py"
                  fi
                  exit 0
                fi
                exec /bin/ps "$@"
                """
            ),
            executable=True,
        )
        self._write(
            self.fake_bin / "readlink",
            textwrap.dedent(
                """\
                #!/usr/bin/env bash
                set -euo pipefail
                case "${1:-}" in
                  /proc/901/exe) echo "$FAKE_REMOTE_DIR/viz_server" ;;
                  /proc/902/exe) echo "$FAKE_REMOTE_DIR/persist_manager" ;;
                  /proc/903/exe) echo "$FAKE_REMOTE_DIR/trade_engine" ;;
                  /proc/904/exe) echo "$FAKE_REMOTE_DIR/pre_trade" ;;
                  /proc/905/exe) echo "$FAKE_REMOTE_DIR/account_monitor" ;;
                  /proc/906/exe) echo "$FAKE_REMOTE_DIR/trade_signal" ;;
                  *) exec /usr/bin/readlink "$@" ;;
                esac
                """
            ),
            executable=True,
        )
        self._write(
            self.fake_bin / "ss",
            textwrap.dedent(
                f"""\
                #!/usr/bin/env bash
                if [[ -f "$FAKE_MARKER_DIR/config" ]]; then
                  echo 'LISTEN 0 128 0.0.0.0:{config_port} 0.0.0.0:*'
                fi
                if [[ -f "$FAKE_MARKER_DIR/viz" ]]; then
                  echo 'LISTEN 0 128 0.0.0.0:{viz_port} 0.0.0.0:*'
                fi
                """
            ),
            executable=True,
        )
        self._write(
            self.fake_bin / "curl",
            textwrap.dedent(
                """\
                #!/usr/bin/env bash
                if [[ -f "$FAKE_MARKER_DIR/config" ]]; then
                  printf '200'
                  exit 0
                fi
                exit 1
                """
            ),
            executable=True,
        )
        self._write(self.fake_bin / "sleep", "#!/usr/bin/env bash\nexit 0\n", executable=True)
        for command in ("pmdaemon", "npx"):
            self._write(self.fake_bin / command, "#!/usr/bin/env bash\nexit 0\n", executable=True)

        self._install_remote_ssh(execute_remote=True)
        remote_env.update(
            {
                "FAKE_MARKER_DIR": str(marker_dir),
                "SG_MM_START_WAIT_SECONDS": "1",
                "SG_MM_START_SETTLE_SECONDS": "1",
            }
        )
        return remote_dir, remote_env

    def test_start_health_checks_base_stack_and_keeps_signals_stopped(self) -> None:
        _, remote_env = self._prepare_start_remote()
        result = self._run(
            START_SCRIPT,
            "--host",
            "fake-sg",
            "--key",
            str(self.key),
            "--env-name",
            "bybit_mm_beta",
            env_overrides=remote_env,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(
            self.action_log.read_text(encoding="utf-8").splitlines(),
            [
                "config:",
                "viz:--exchange bybit",
                "persist:",
                "engine:bybit",
                "pre-trade:",
                "monitor:",
            ],
        )
        self.assertIn("signal_processes_started=false", result.stdout)
        self.assertNotIn("trade_signal health check passed", result.stdout)
        self.assertIn("persist_manager=included", result.stdout)

    def test_start_refuses_running_trade_signal_before_starting_base_stack(self) -> None:
        _, remote_env = self._prepare_start_remote()
        (Path(remote_env["FAKE_MARKER_DIR"]) / "signal").touch()
        result = self._run(
            START_SCRIPT,
            "--host",
            "fake-sg",
            "--key",
            str(self.key),
            "--env-name",
            "bybit_mm_beta",
            env_overrides=remote_env,
        )
        self.assertEqual(result.returncode, 3, result.stdout)
        self.assertFalse(self.action_log.exists())
        self.assertIn("trade_signal is already running", result.stdout)

    def test_start_rejects_wrong_fixed_ports_before_any_process(self) -> None:
        remote_dir, remote_env = self._prepare_start_remote("bybit_mm_alpha")
        self._write(remote_dir / "config" / "mm_config_server.env", "PORT=18141\n")
        result = self._run(
            START_SCRIPT,
            "--host",
            "fake-sg",
            "--key",
            str(self.key),
            "--env-name",
            "bybit_mm_alpha",
            env_overrides=remote_env,
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertFalse(self.action_log.exists())
        self.assertIn("config server port mismatch", result.stdout)

        self._write(remote_dir / "config" / "mm_config_server.env", "PORT=18142\n")
        self._write(
            remote_dir / "config" / "viz.toml",
            "[[servers]]\n[servers.http]\nport = 10241\n",
        )
        failed_viz = self._run(
            START_SCRIPT,
            "--host",
            "fake-sg",
            "--key",
            str(self.key),
            "--env-name",
            "bybit_mm_alpha",
            env_overrides=remote_env,
        )
        self.assertNotEqual(failed_viz.returncode, 0, failed_viz.stdout)
        self.assertFalse(self.action_log.exists())
        self.assertIn("viz server port mismatch", failed_viz.stdout)

    def test_publish_manifest_maps_monitor_and_preserves_runtime_configuration(self) -> None:
        publish = PUBLISH_SCRIPT.read_text(encoding="utf-8")
        local_manifest = publish.split("LOCAL_RELATIVE=(", 1)[1].split(")", 1)[0]
        upload_manifest = publish.split("UPLOAD_NAMES=(", 1)[1].split(")", 1)[0]
        destinations = publish.split("DESTINATIONS=(", 1)[1].split(")", 1)[0]

        self.assertIn('"target/release/bybit_account_monitor"', local_manifest)
        self.assertIn('"bybit_account_monitor"', upload_manifest)
        self.assertIn('"account_monitor"', destinations)
        self.assertIn('"target/release/persist_manager"', local_manifest)
        self.assertIn('"persist_manager"', destinations)
        self.assertIn('"scripts/close_mm_all_um_ws_orders.sh"', local_manifest)
        self.assertIn('"scripts/bybit_cancel_all_um_orders.py"', local_manifest)
        self.assertIn('"scripts/binance_local_ip.py"', local_manifest)
        self.assertNotIn("env.sh", local_manifest)
        self.assertNotIn("config/", local_manifest)
        self.assertNotIn("trade_engine.toml", local_manifest)
        self.assertNotIn("data/", local_manifest)
        self.assertNotIn("logs/", local_manifest)


if __name__ == "__main__":
    unittest.main()
