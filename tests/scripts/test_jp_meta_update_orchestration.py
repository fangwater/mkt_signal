from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BUILD_SCRIPT = ROOT / "scripts" / "build-jp-meta-binaries.sh"
FR_PUBLISH_SCRIPT = ROOT / "scripts" / "publish-jp-meta-fr.sh"
MM_PUBLISH_SCRIPT = ROOT / "scripts" / "publish-jp-meta-mm.sh"
FR_UPDATE_SCRIPT = ROOT / "scripts" / "update-jp-meta-fr.sh"
MM_UPDATE_SCRIPT = ROOT / "scripts" / "update-jp-meta-mm.sh"


class JPMetaUpdateOrchestrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)
        self.fake_bin = self.temp_path / "fake-bin"
        self.fake_bin.mkdir()
        self.base_env = os.environ.copy()
        self.base_env["PATH"] = f"{self.fake_bin}{os.pathsep}{self.base_env['PATH']}"

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
            timeout=20,
        )

    def test_build_script_builds_every_binary_including_persist_manager(self) -> None:
        repo = self.temp_path / "build-repo"
        script = self._copy_script(BUILD_SCRIPT, repo)
        action_log = self.temp_path / "cargo-actions.log"
        self._write(
            self.fake_bin / "cargo",
            textwrap.dedent(
                """\
                #!/usr/bin/env bash
                set -euo pipefail
                printf "%s " "$@" >>"$FAKE_ACTION_LOG"
                printf "\\n" >>"$FAKE_ACTION_LOG"

                target_dir=""
                bins=()
                while [[ $# -gt 0 ]]; do
                  case "$1" in
                    --target-dir)
                      target_dir="$2"
                      shift 2
                      ;;
                    --bin)
                      bins+=("$2")
                      shift 2
                      ;;
                    *)
                      shift
                      ;;
                  esac
                done
                [[ -n "$target_dir" ]]
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
            "--exchange",
            "okx",
            env_overrides={"FAKE_ACTION_LOG": str(action_log)},
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        commands = action_log.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(commands), 4)
        self.assertIn("-p mkt_signal", commands[0])
        for binary in ("okex_account_monitor", "pre_trade", "trade_engine"):
            self.assertIn(f"--bin {binary}", commands[0])
        self.assertIn("-p trade_signal --bin trade_signal", commands[1])
        self.assertIn("-p viz_server --bin viz_server", commands[2])
        self.assertIn("-p persist_manager --features runtime --bin persist_manager", commands[3])
        self.assertIn("persist_manager=included", result.stdout)

        expected = {
            "okex_account_monitor",
            "pre_trade",
            "trade_engine",
            "trade_signal",
            "viz_server",
            "persist_manager",
        }
        self.assertEqual(
            {path.name for path in (repo / "target" / "release").iterdir()},
            expected,
        )

    def test_publish_build_failure_happens_before_any_ssh(self) -> None:
        self._write(
            self.fake_bin / "ssh",
            "#!/usr/bin/env bash\nprintf 'ssh\\n' >>\"$FAKE_ACTION_LOG\"\nexit 99\n",
            executable=True,
        )

        cases = (
            (FR_PUBLISH_SCRIPT, ("--env-name", "binance_fr_test")),
            (MM_PUBLISH_SCRIPT, ("--env-name", "binance_mm_test")),
        )
        for source, args in cases:
            with self.subTest(script=source.name):
                repo = self.temp_path / source.stem
                script = self._copy_script(source, repo)
                action_log = self.temp_path / f"{source.stem}.log"
                self._write(
                    repo / "scripts" / BUILD_SCRIPT.name,
                    textwrap.dedent(
                        """\
                        #!/usr/bin/env bash
                        printf "build:%s\\n" "$*" >>"$FAKE_ACTION_LOG"
                        exit 17
                        """
                    ),
                    executable=True,
                )

                result = self._run(
                    script,
                    *args,
                    env_overrides={"FAKE_ACTION_LOG": str(action_log)},
                )
                self.assertEqual(result.returncode, 17, result.stdout)
                self.assertEqual(
                    action_log.read_text(encoding="utf-8").splitlines(),
                    ["build:--exchange binance"],
                )

    def _install_update_fakes(self, repo: Path, *, fail_build: bool = False) -> None:
        fake_names = (
            BUILD_SCRIPT.name,
            "stop-jp-meta-fr.sh",
            "publish-jp-meta-fr.sh",
            "start-jp-meta-fr.sh",
            "stop-jp-meta-mm.sh",
            "publish-jp-meta-mm.sh",
            "start-jp-meta-mm.sh",
        )
        for name in fake_names:
            action = name.removesuffix(".sh")
            exit_status = 23 if fail_build and name == BUILD_SCRIPT.name else 0
            self._write(
                repo / "scripts" / name,
                textwrap.dedent(
                    f"""\
                    #!/usr/bin/env bash
                    printf "%s:%s\\n" "{action}" "$*" >>"$FAKE_ACTION_LOG"
                    exit {exit_status}
                    """
                ),
                executable=True,
            )

    def test_update_order_builds_before_stop_publish_and_start(self) -> None:
        cases = (
            (
                FR_UPDATE_SCRIPT,
                ("--host", "fake-host", "--env-name", "gate_fr_test"),
                [
                    "build-jp-meta-binaries:--exchange gate",
                    "stop-jp-meta-fr:--host fake-host --env-name gate_fr_test",
                    "publish-jp-meta-fr:--host fake-host --env-name gate_fr_test --exchange gate --skip-build",
                    "start-jp-meta-fr:--host fake-host --env-name gate_fr_test",
                ],
            ),
            (
                MM_UPDATE_SCRIPT,
                (
                    "--host",
                    "fake-host",
                    "--env-name",
                    "okex_mm_test",
                    "--exchange",
                    "okx",
                ),
                [
                    "build-jp-meta-binaries:--exchange okex",
                    "stop-jp-meta-mm:--host fake-host --env-name okex_mm_test",
                    "publish-jp-meta-mm:--host fake-host --env-name okex_mm_test --exchange okex --skip-build",
                    "start-jp-meta-mm:--host fake-host --env-name okex_mm_test",
                ],
            ),
        )
        for source, args, expected in cases:
            with self.subTest(script=source.name):
                repo = self.temp_path / f"order-{source.stem}"
                script = self._copy_script(source, repo)
                self._install_update_fakes(repo)
                shutil.copy2(source, script)
                action_log = self.temp_path / f"order-{source.stem}.log"

                result = self._run(
                    script,
                    *args,
                    env_overrides={"FAKE_ACTION_LOG": str(action_log)},
                )
                self.assertEqual(result.returncode, 0, result.stdout)
                self.assertEqual(
                    action_log.read_text(encoding="utf-8").splitlines(),
                    expected,
                )
                self.assertIn("persist_manager=included", result.stdout)

    def test_update_build_failure_does_not_call_stop(self) -> None:
        repo = self.temp_path / "failed-update"
        script = self._copy_script(MM_UPDATE_SCRIPT, repo)
        self._install_update_fakes(repo, fail_build=True)
        shutil.copy2(MM_UPDATE_SCRIPT, script)
        action_log = self.temp_path / "failed-update.log"

        result = self._run(
            script,
            "--host",
            "fake-host",
            "--env-name",
            "binance_mm_test",
            env_overrides={"FAKE_ACTION_LOG": str(action_log)},
        )
        self.assertEqual(result.returncode, 23, result.stdout)
        self.assertEqual(
            action_log.read_text(encoding="utf-8").splitlines(),
            ["build-jp-meta-binaries:--exchange binance"],
        )

    def test_fr_and_mm_keep_persist_manager_and_live_cancel_coverage(self) -> None:
        fr_publish = FR_PUBLISH_SCRIPT.read_text(encoding="utf-8")
        fr_start = (ROOT / "scripts" / "start-jp-meta-fr.sh").read_text(encoding="utf-8")
        fr_stop = (ROOT / "scripts" / "stop-jp-meta-fr.sh").read_text(encoding="utf-8")
        mm_publish = MM_PUBLISH_SCRIPT.read_text(encoding="utf-8")
        mm_start = (ROOT / "scripts" / "start-jp-meta-mm.sh").read_text(encoding="utf-8")
        mm_stop = (ROOT / "scripts" / "stop-jp-meta-mm.sh").read_text(encoding="utf-8")

        for publish in (fr_publish, mm_publish):
            self.assertIn('"target/release/persist_manager"', publish)
            self.assertIn('"$target/persist_manager"', publish)
        for start in (fr_start, mm_start):
            self.assertIn('"$target/persist_manager"', start)
            self.assertIn("persist_manager", start)
        for stop in (fr_stop, mm_stop):
            self.assertIn('"$target/persist_manager"', stop)
            self.assertIn('run_step "stop persist_manager"', stop)

        self.assertIn(
            'python3 "$cancel_script" --scope both --execute',
            fr_stop,
        )
        self.assertIn(
            'bash "$cancel_script" "${cancel_args[@]}" --execute',
            mm_stop,
        )


if __name__ == "__main__":
    unittest.main()
