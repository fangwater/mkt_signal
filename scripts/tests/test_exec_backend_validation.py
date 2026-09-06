from __future__ import annotations

import pathlib
import os
import subprocess
import tempfile
import unittest


SCRIPTS_DIR = pathlib.Path(__file__).resolve().parents[1]
START_EXEC = SCRIPTS_DIR / "start-exec.sh"
START_EXEC_PRE_TRADE = SCRIPTS_DIR / "start_exec_pre_trade.sh"


def extracted_validation() -> str:
    source = START_EXEC.read_text(encoding="utf-8")
    start = source.index("# Keep this parser aligned with runtime_common::execution_backend::ExecBackend.")
    end = source.index("\n\nexec_backend=\"$(\n", start)
    return source[start:end]


class ExecBackendValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.validation = extracted_validation()

    def run_validation(self, venue: str, env_lines: list[str]) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as tmp:
            exchange = venue.split("-")[0]
            target = pathlib.Path(tmp) / f"{exchange}_exec_test01"
            target.mkdir()
            (target / "env.sh").write_text("\n".join(env_lines) + "\n", encoding="utf-8")
            return subprocess.run(
                [
                    "bash",
                    "-c",
                    "set -euo pipefail\ntarget=$1\nvenue=$2\nexchange=$3\n" + self.validation,
                    "bash",
                    str(target),
                    venue,
                    exchange,
                ],
                env={"PATH": os.environ["PATH"], "LC_ALL": "C"},
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=False,
            )

    @staticmethod
    def base_env(venue: str) -> list[str]:
        return [
            f"export IPC_NAMESPACE={venue.split('-')[0]}_exec_test01",
            f"export EXEC_VENUE={venue}",
        ]

    def test_native_and_rapidx_alias_map_precedence_validate(self) -> None:
        native = self.base_env("binance-futures") + [
            "export BINANCE_API_KEY=key",
            "export BINANCE_API_SECRET=secret",
            "export TRADE_ENGINE_EXEC_BACKEND=direct",
        ]
        self.assertEqual(self.run_validation("binance-futures", native).returncode, 0)

        rapidx = self.base_env("binance-futures") + [
            "export TRADE_ENGINE_EXEC_BACKEND=exchange",
            "export TRADE_ENGINE_EXEC_BACKEND_MAP='*=native,binance=liquiditytech'",
            "export LTP_API_KEY=key",
            "export LTP_API_SECRET=secret",
            "export LTP_PORTFOLIO_ID=1702884522340000",
        ]
        self.assertEqual(self.run_validation("binance-futures", rapidx).returncode, 0)

    def test_rapidx_missing_credentials_and_coin_are_rejected(self) -> None:
        missing_credentials = self.base_env("binance-futures") + [
            "export TRADE_ENGINE_EXEC_BACKEND=rapidx",
            "export LTP_PORTFOLIO_ID=1702884522340000",
        ]
        self.assertNotEqual(self.run_validation("binance-futures", missing_credentials).returncode, 0)

        coin = self.base_env("binance-coin-futures") + [
            "export TRADE_ENGINE_EXEC_BACKEND=rapidx",
            "export LTP_API_KEY=key",
            "export LTP_API_SECRET=secret",
            "export LTP_PORTFOLIO_ID=1702884522340000",
        ]
        self.assertNotEqual(self.run_validation("binance-coin-futures", coin).returncode, 0)

    def test_okx_rapidx_needs_no_native_credentials(self) -> None:
        rapidx = self.base_env("okex-futures") + [
            "export TRADE_ENGINE_EXEC_BACKEND_MAP='okex=rapidx,*=native'",
            "export LTP_API_KEY=fixture", "export LTP_API_SECRET=fixture",
            "export LTP_PORTFOLIO_ID=123",
        ]
        self.assertEqual(self.run_validation("okex-futures", rapidx).returncode, 0)
        native = self.base_env("okex-futures") + [
            "export OKX_API_KEY=fixture", "export OKX_API_SECRET=fixture",
        ]
        self.assertNotEqual(self.run_validation("okex-futures", native).returncode, 0)
        native.append("export OKX_PASSPHRASE=fixture")
        self.assertEqual(self.run_validation("okex-futures", native).returncode, 0)

    def test_namespace_and_venue_mismatches_are_rejected(self) -> None:
        namespace_mismatch = self.base_env("binance-futures") + [
            "export IPC_NAMESPACE=wrong_namespace",
            "export BINANCE_API_KEY=key",
            "export BINANCE_API_SECRET=secret",
        ]
        self.assertNotEqual(self.run_validation("binance-futures", namespace_mismatch).returncode, 0)

        venue_mismatch = self.base_env("okex-futures") + [
            "export BINANCE_API_KEY=key",
            "export BINANCE_API_SECRET=secret",
        ]
        self.assertNotEqual(self.run_validation("binance-futures", venue_mismatch).returncode, 0)

    def test_invalid_and_multiline_backend_maps_are_rejected(self) -> None:
        invalid = self.base_env("binance-futures") + [
            "export BINANCE_API_KEY=key",
            "export BINANCE_API_SECRET=secret",
            "export TRADE_ENGINE_EXEC_BACKEND_MAP='binance=rapidx,binance=native'",
        ]
        self.assertNotEqual(self.run_validation("binance-futures", invalid).returncode, 0)

        multiline = self.base_env("binance-futures") + [
            "export BINANCE_API_KEY=key",
            "export BINANCE_API_SECRET=secret",
            "export TRADE_ENGINE_EXEC_BACKEND_MAP=$'binance=rapidx\\nokex=native'",
        ]
        self.assertNotEqual(self.run_validation("binance-futures", multiline).returncode, 0)

    def test_backend_parser_stays_identical_in_both_exec_wrappers(self) -> None:
        start_source = START_EXEC.read_text(encoding="utf-8")
        pre_trade_source = START_EXEC_PRE_TRADE.read_text(encoding="utf-8")
        start_parser = start_source[
            start_source.index("parse_exec_backend()") : start_source.index("\n\nif ! (", start_source.index("parse_exec_backend()"))
        ]
        pre_trade_parser = pre_trade_source[
            pre_trade_source.index("parse_exec_backend()") : pre_trade_source.index(
                "\n\nEXEC_BACKEND=", pre_trade_source.index("parse_exec_backend()")
            )
        ]
        account_source = START_EXEC_PRE_TRADE.with_name("start_account_monitor.sh").read_text(
            encoding="utf-8"
        )
        account_start = account_source.index("parse_exec_backend()")
        account_parser = account_source[
            account_start : account_source.index("\n\n  EXEC_BACKEND=", account_start)
        ]
        account_parser = "\n".join(
            line[2:] if line.startswith("  ") else line
            for line in account_parser.splitlines()
        )
        self.assertEqual(start_parser, pre_trade_parser)
        self.assertEqual(start_parser, account_parser)

    def test_rapidx_monitor_binary_and_exchange_argument_are_explicit(self) -> None:
        source = START_EXEC_PRE_TRADE.with_name("start_account_monitor.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn('"${BASE_DIR}/rapidx_account_monitor"', source)
        self.assertIn('ACCOUNT_MONITOR_ARGS=(--exchange "$EXCHANGE")', source)
        self.assertIn('ltp) account_monitor_binary="$target/rapidx_account_monitor" ;;', START_EXEC.read_text(encoding="utf-8"))

    def test_monitor_selection_never_falls_back_to_native_binary(self) -> None:
        source = START_EXEC_PRE_TRADE.with_name("start_account_monitor.sh").read_text(encoding="utf-8")
        start = source.index('EXEC_BACKEND="native"')
        block = source[start:source.index('\nif [[ "$MODE" == "mm" ]]; then', start)]
        with tempfile.TemporaryDirectory() as tmp:
            native = pathlib.Path(tmp) / "account_monitor"
            native.touch()
            native.chmod(0o700)
            script = ('set -euo pipefail\nBASE_DIR=$1\nSCRIPT_DIR=$1\nMODE=exec\n'
                      'EXCHANGE=okex\nTRADE_ENGINE_EXEC_BACKEND=rapidx\n' + block +
                      '\nprintf "%s\\n" "$BIN_PATH" "${ACCOUNT_MONITOR_ARGS[@]}"')
            def check():
                return subprocess.run(["bash", "-c", script, "bash", tmp],
                    env={"PATH": os.environ["PATH"], "LC_ALL": "C"}, capture_output=True, text=True)
            self.assertNotEqual(check().returncode, 0)
            rapidx = pathlib.Path(tmp) / "rapidx_account_monitor"
            rapidx.touch()
            rapidx.chmod(0o700)
            result = check()
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.splitlines(), [str(rapidx), "--exchange", "okex"])


if __name__ == "__main__":
    unittest.main()
