from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BUILD_SCRIPT = ROOT / "scripts" / "build-sg-intra-binaries.sh"
PUBLISH_SCRIPT = ROOT / "scripts" / "publish-sg-intra.sh"
START_SCRIPT = ROOT / "scripts" / "start-sg-intra.sh"
STOP_SCRIPT = ROOT / "scripts" / "stop-sg-intra.sh"
UPDATE_SCRIPT = ROOT / "scripts" / "update-sg-intra.sh"


class SGIntraOrchestrationTests(unittest.TestCase):
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

    def _remote_env(self, env_name: str = "bybit-intra-arb01") -> tuple[Path, dict[str, str]]:
        remote_home = self.temp_path / "remote-home"
        remote_dir = remote_home / env_name
        remote_dir.mkdir(parents=True, exist_ok=True)
        return remote_dir, {
            "FAKE_REMOTE_HOME": str(remote_home),
            "FAKE_REMOTE_DIR": str(remote_dir),
            "FAKE_ENV_NAME": env_name,
        }

    def test_builds_all_release_binaries_including_persist_manager(self) -> None:
        repo = self.temp_path / "build-repo"
        script = self._copy_script(BUILD_SCRIPT, repo)
        cargo_log = self.temp_path / "cargo.log"
        self._write(
            self.fake_bin / "cargo",
            textwrap.dedent(
                """\
                #!/usr/bin/env bash
                set -euo pipefail
                printf '%s\\n' "$*" >>"$FAKE_CARGO_LOG"
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
                  printf '#!/usr/bin/env bash\\nexit 0\\n' >"$target_dir/release/$binary"
                  chmod 755 "$target_dir/release/$binary"
                done
                """
            ),
            executable=True,
        )

        result = self._run(
            script,
            env_overrides={"FAKE_CARGO_LOG": str(cargo_log)},
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        commands = cargo_log.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(commands), 4)
        self.assertIn("--bin bybit_account_monitor", commands[0])
        self.assertIn("--bin pre_trade", commands[0])
        self.assertIn("--bin trade_engine", commands[0])
        self.assertIn("-p trade_signal --bin trade_signal", commands[1])
        self.assertIn("-p viz_server --bin viz_server", commands[2])
        self.assertIn("-p persist_manager --features runtime --bin persist_manager", commands[3])
        self.assertIn("persist_manager=included", result.stdout)

    def test_every_entrypoint_rejects_arb03_before_ssh(self) -> None:
        self._write(
            self.fake_bin / "ssh",
            "#!/usr/bin/env bash\nprintf 'unexpected\\n' >>\"$FAKE_ACTION_LOG\"\nexit 99\n",
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
                    "bybit-intra-arb03",
                )
                self.assertEqual(result.returncode, 2, result.stdout)
                self.assertFalse(self.action_log.exists())

    def _install_update_fakes(self, repo: Path, *, build_status: int = 0, stop_status: int = 0) -> None:
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

    def test_update_all_builds_once_then_updates_arb01_and_arb02(self) -> None:
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
                "build-sg-intra-binaries:",
                f"stop-sg-intra:--host fake-sg --key {key} --env-name bybit-intra-arb01",
                f"publish-sg-intra:--host fake-sg --key {key} --env-name bybit-intra-arb01 --skip-build",
                f"start-sg-intra:--host fake-sg --key {key} --env-name bybit-intra-arb01",
                f"stop-sg-intra:--host fake-sg --key {key} --env-name bybit-intra-arb02",
                f"publish-sg-intra:--host fake-sg --key {key} --env-name bybit-intra-arb02 --skip-build",
                f"start-sg-intra:--host fake-sg --key {key} --env-name bybit-intra-arb02",
            ],
        )

    def test_update_failure_never_advances_to_live_next_step(self) -> None:
        for build_status, stop_status, expected in (
            (23, 0, ["build-sg-intra-binaries:"]),
            (
                0,
                24,
                [
                    "build-sg-intra-binaries:",
                    f"stop-sg-intra:--host fake-sg --key {self.key.resolve()} --env-name bybit-intra-arb01",
                ],
            ),
        ):
            with self.subTest(build_status=build_status, stop_status=stop_status):
                self.action_log.unlink(missing_ok=True)
                repo = self.temp_path / f"failed-{build_status}-{stop_status}"
                self._install_update_fakes(
                    repo,
                    build_status=build_status,
                    stop_status=stop_status,
                )
                script = self._copy_script(UPDATE_SCRIPT, repo)
                result = self._run(
                    script,
                    "--host",
                    "fake-sg",
                    "--key",
                    str(self.key),
                    "--env-name",
                    "bybit-intra-arb01",
                )
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertEqual(
                    self.action_log.read_text(encoding="utf-8").splitlines(),
                    expected,
                )

    def test_publish_build_failure_occurs_before_first_ssh(self) -> None:
        repo = self.temp_path / "publish-repo"
        script = self._copy_script(PUBLISH_SCRIPT, repo)
        self._write(
            repo / "scripts" / BUILD_SCRIPT.name,
            "#!/usr/bin/env bash\nprintf 'build\\n' >>\"$FAKE_ACTION_LOG\"\nexit 17\n",
            executable=True,
        )
        self._write(
            self.fake_bin / "ssh",
            "#!/usr/bin/env bash\nprintf 'ssh\\n' >>\"$FAKE_ACTION_LOG\"\nexit 99\n",
            executable=True,
        )

        result = self._run(
            script,
            "--key",
            str(self.key),
            "--env-name",
            "bybit-intra-arb01",
        )
        self.assertEqual(result.returncode, 17, result.stdout)
        self.assertEqual(self.action_log.read_text(encoding="utf-8").splitlines(), ["build"])

    def test_publish_check_only_never_builds_or_copies_and_propagates_running_failure(self) -> None:
        remote_dir, remote_env = self._remote_env()
        del remote_dir
        self._write(
            self.fake_bin / "scp",
            "#!/usr/bin/env bash\nprintf 'scp\\n' >>\"$FAKE_ACTION_LOG\"\nexit 99\n",
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
            "bybit-intra-arb01",
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
            "bybit-intra-arb01",
            "--check-only",
            env_overrides=remote_env,
        )
        self.assertEqual(failed.returncode, 3, failed.stdout)
        self.assertFalse(self.action_log.exists())

    def _prepare_stop_remote(self, *, warning: bool = False) -> tuple[Path, dict[str, str]]:
        remote_dir, remote_env = self._remote_env()
        scripts_dir = remote_dir / "scripts"
        intra_dir = remote_dir / "intra_scripts"
        self._write(
            remote_dir / "env.sh",
            "export BYBIT_API_KEY='test-key'\nexport BYBIT_API_SECRET='test-secret'\n",
        )
        self._write(scripts_dir / "process_match_lib.sh", "#!/usr/bin/env bash\n")
        cancel_body = "[WARN] fake Bybit query failure" if warning else "Verification passed: no residual active orders in scope."
        self._write(
            scripts_dir / "cancel_bybit_pm_orders.py",
            textwrap.dedent(
                f"""\
                import os
                import sys
                with open(os.environ["FAKE_ACTION_LOG"], "a", encoding="utf-8") as handle:
                    handle.write("cancel:" + " ".join(sys.argv[1:]) + "\\n")
                print({cancel_body!r})
                """
            ),
        )
        wrappers = {
            scripts_dir / "stop_intra_config_server.sh": "stop-config",
            intra_dir / "stop_intra_trade_engine.sh": "stop-engine",
            intra_dir / "stop_intra_trade_signal.sh": "stop-signal",
            intra_dir / "stop_intra_pre_trade.sh": "stop-pre-trade",
            intra_dir / "stop_intra_monitors.sh": "stop-monitor",
            intra_dir / "stop_intra_persist_manager.sh": "stop-persist",
            intra_dir / "stop_intra_viz_server.sh": "stop-viz",
        }
        for path, action in wrappers.items():
            self._write(
                path,
                f"#!/usr/bin/env bash\nprintf '%s\\n' '{action}' >>\"$FAKE_ACTION_LOG\"\n",
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
                  "-eo pid="|"-eo pid=,args=") exit 0 ;;
                  *) exec /bin/ps "$@" ;;
                esac
                """
            ),
            executable=True,
        )
        self._install_remote_ssh(execute_remote=True)
        return remote_dir, remote_env

    def test_stop_confirms_engine_before_execute_cancel_and_stops_persist_manager(self) -> None:
        _, remote_env = self._prepare_stop_remote()
        result = self._run(
            STOP_SCRIPT,
            "--host",
            "fake-sg",
            "--key",
            str(self.key),
            "--env-name",
            "bybit-intra-arb01",
            env_overrides=remote_env,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(
            self.action_log.read_text(encoding="utf-8").splitlines(),
            [
                "stop-engine",
                "cancel:--scope both --spot-order-filters all --execute",
                "stop-signal",
                "stop-pre-trade",
                "stop-monitor",
                "stop-config",
                "stop-persist",
                "stop-viz",
            ],
        )
        self.assertLess(
            result.stdout.index("trade_engine confirmed stopped"),
            result.stdout.index("cancel and verify all Bybit"),
        )

    def test_cancel_warning_aborts_remaining_stop_sequence(self) -> None:
        _, remote_env = self._prepare_stop_remote(warning=True)
        result = self._run(
            STOP_SCRIPT,
            "--host",
            "fake-sg",
            "--key",
            str(self.key),
            "--env-name",
            "bybit-intra-arb01",
            env_overrides=remote_env,
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertEqual(
            self.action_log.read_text(encoding="utf-8").splitlines(),
            [
                "stop-engine",
                "cancel:--scope both --spot-order-filters all --execute",
            ],
        )
        self.assertIn("cancel script reported a warning/error", result.stdout)

    def _prepare_start_remote(self) -> tuple[Path, dict[str, str]]:
        remote_dir, remote_env = self._remote_env()
        marker_dir = self.temp_path / "markers"
        marker_dir.mkdir()
        scripts_dir = remote_dir / "scripts"
        intra_dir = remote_dir / "intra_scripts"
        self._write(
            remote_dir / "env.sh",
            "export IPC_NAMESPACE='test-intra'\n"
            "export BYBIT_API_KEY='test-key'\n"
            "export BYBIT_API_SECRET='test-secret'\n",
        )
        self._write(
            remote_dir / "config" / "viz.toml",
            "[[servers]]\n[servers.http]\nbind = \"0.0.0.0\"\nport = 10174\n",
        )
        self._write(remote_dir / "config" / "intra_config_server.env", "PORT=19191\n")
        self._write(scripts_dir / "intra_config_server.py", "# test marker\n")
        self._write(scripts_dir / "process_match_lib.sh", "#!/usr/bin/env bash\n")

        binary_names = (
            "trade_signal",
            "account_monitor_bybit",
            "viz_server",
            "pre_trade",
            "trade_engine",
            "persist_manager",
        )
        for binary in binary_names:
            self._write(remote_dir / binary, "#!/usr/bin/env bash\nexit 0\n", executable=True)

        start_actions = {
            scripts_dir / "start_intra_config_server.sh": ("config", "config"),
            intra_dir / "start_intra_viz_server.sh": ("viz", "viz"),
            intra_dir / "start_intra_persist_manager.sh": ("persist", "persist"),
            intra_dir / "start_intra_trade_engine.sh": ("engine", "engine"),
            intra_dir / "start_intra_pre_trade.sh": ("pre", "pre-trade"),
            intra_dir / "start_intra_monitors.sh": ("monitor", "monitor"),
            intra_dir / "start_intra_trade_signal.sh": ("signal", "signal"),
        }
        for path, (marker, action) in start_actions.items():
            self._write(
                path,
                textwrap.dedent(
                    f"""\
                    #!/usr/bin/env bash
                    touch "$FAKE_MARKER_DIR/{marker}"
                    printf '%s\\n' '{action}' >>"$FAKE_ACTION_LOG"
                    """
                ),
                executable=True,
            )
        for name in (
            "stop_intra_config_server.sh",
            "stop_intra_viz_server.sh",
            "stop_intra_persist_manager.sh",
            "stop_intra_trade_engine.sh",
            "stop_intra_pre_trade.sh",
            "stop_intra_monitors.sh",
            "stop_intra_trade_signal.sh",
        ):
            directory = scripts_dir if name == "stop_intra_config_server.sh" else intra_dir
            self._write(directory / name, "#!/usr/bin/env bash\nexit 0\n", executable=True)

        self._write(
            self.fake_bin / "ps",
            textwrap.dedent(
                """\
                #!/usr/bin/env bash
                set -euo pipefail
                if [[ "$*" == "-eo pid=" ]]; then
                  [[ -f "$FAKE_MARKER_DIR/viz" ]] && echo 901
                  [[ -f "$FAKE_MARKER_DIR/persist" ]] && echo 902
                  [[ -f "$FAKE_MARKER_DIR/engine" ]] && echo 903
                  [[ -f "$FAKE_MARKER_DIR/pre" ]] && echo 904
                  [[ -f "$FAKE_MARKER_DIR/monitor" ]] && echo 905
                  [[ -f "$FAKE_MARKER_DIR/signal" ]] && echo 906
                  exit 0
                fi
                if [[ "$*" == "-eo pid=,args=" ]]; then
                  if [[ -f "$FAKE_MARKER_DIR/config" ]]; then
                    echo "900 python3 $FAKE_REMOTE_DIR/scripts/intra_config_server.py"
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
                  /proc/905/exe) echo "$FAKE_REMOTE_DIR/account_monitor_bybit" ;;
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
                """\
                #!/usr/bin/env bash
                if [[ -f "$FAKE_MARKER_DIR/config" ]]; then
                  echo 'LISTEN 0 128 0.0.0.0:19191 0.0.0.0:*'
                fi
                if [[ -f "$FAKE_MARKER_DIR/viz" ]]; then
                  echo 'LISTEN 0 128 0.0.0.0:10174 0.0.0.0:*'
                fi
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
                "SG_INTRA_START_WAIT_SECONDS": "1",
                "SG_INTRA_START_SETTLE_SECONDS": "1",
            }
        )
        return remote_dir, remote_env

    def test_start_health_checks_base_stack_and_keeps_signal_stopped(self) -> None:
        _, remote_env = self._prepare_start_remote()
        result = self._run(
            START_SCRIPT,
            "--host",
            "fake-sg",
            "--key",
            str(self.key),
            "--env-name",
            "bybit-intra-arb01",
            env_overrides=remote_env,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(
            self.action_log.read_text(encoding="utf-8").splitlines(),
            ["config", "viz", "persist", "engine", "pre-trade", "monitor"],
        )
        self.assertIn("trade_signal_started=false", result.stdout)
        self.assertNotIn("trade_signal health check passed", result.stdout)

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
            "bybit-intra-arb01",
            env_overrides=remote_env,
        )
        self.assertEqual(result.returncode, 3, result.stdout)
        self.assertFalse(self.action_log.exists())
        self.assertIn("trade_signal is already running", result.stdout)

    def test_start_rejects_wrong_config_port_before_starting_any_process(self) -> None:
        remote_dir, remote_env = self._prepare_start_remote()
        self._write(remote_dir / "config" / "intra_config_server.env", "PORT=19192\n")
        result = self._run(
            START_SCRIPT,
            "--host",
            "fake-sg",
            "--key",
            str(self.key),
            "--env-name",
            "bybit-intra-arb01",
            env_overrides=remote_env,
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertFalse(self.action_log.exists())
        self.assertIn("config server port mismatch", result.stdout)

    def test_start_rejects_wrong_viz_port_before_starting_any_process(self) -> None:
        remote_dir, remote_env = self._prepare_start_remote()
        self._write(
            remote_dir / "config" / "viz.toml",
            "[[servers]]\n[servers.http]\nport = 10175\n",
        )
        result = self._run(
            START_SCRIPT,
            "--host",
            "fake-sg",
            "--key",
            str(self.key),
            "--env-name",
            "bybit-intra-arb01",
            env_overrides=remote_env,
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertFalse(self.action_log.exists())
        self.assertIn("viz server port mismatch", result.stdout)

    def test_publish_maps_monitor_and_keeps_persist_manager_without_env_or_config_upload(self) -> None:
        publish = PUBLISH_SCRIPT.read_text(encoding="utf-8")
        self.assertIn('"target/release/bybit_account_monitor"', publish)
        self.assertIn("publish_file bybit_account_monitor account_monitor_bybit", publish)
        self.assertIn('"target/release/persist_manager"', publish)
        self.assertIn("publish_file persist_manager persist_manager", publish)
        for dependency in (
            "arb_per_symbol_overrides.py",
            "sync_intra_risk_params.py",
            "sync_intra_strategy_params.py",
            "sync_intra_funding_thresholds.py",
            "sync_intra_symbol_lists.py",
            "sync_intra_spread_thresholds.py",
            "sync_rolling_metrics_params.py",
        ):
            self.assertIn(f"scripts/{dependency}", publish)
        local_manifest = publish.split("LOCAL_RELATIVE=(", 1)[1].split(")", 1)[0]
        self.assertNotIn("env.sh", local_manifest)
        self.assertNotIn("config/viz.toml", local_manifest)
        self.assertNotIn("data/", local_manifest)


if __name__ == "__main__":
    unittest.main()
