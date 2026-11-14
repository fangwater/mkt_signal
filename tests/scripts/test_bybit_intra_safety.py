from __future__ import annotations

import importlib.util
import subprocess
import sys
import unittest
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LIB_DIR = ROOT / "scripts" / "lib"
if str(LIB_DIR) not in sys.path:
    sys.path.insert(0, str(LIB_DIR))

import exchange_state  # noqa: E402

FLATTEN_SCRIPT = ROOT / "intra_scripts" / "flatten_intra_bybit_futures_exposure.py"
spec = importlib.util.spec_from_file_location("flatten_intra_bybit_futures_exposure", FLATTEN_SCRIPT)
flatten = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = flatten
spec.loader.exec_module(flatten)


class BybitIntraSafetyTests(unittest.TestCase):
    def test_exchange_state_uses_rust_spot_net_semantics(self) -> None:
        balance, borrowed, interest = exchange_state._bybit_spot_balance(
            {
                "walletBalance": "-33401.24347568",
                "spotBorrow": "803091.7684731",
                "borrowAmount": "836493.011948792006655924",
                "accruedInterest": "39.96414652",
            }
        )

        self.assertEqual(borrowed, Decimal("803091.7684731"))
        self.assertEqual(interest, Decimal("39.96414652"))
        self.assertEqual(balance, Decimal("-836532.97609530"))

    def test_exchange_state_falls_back_to_legacy_liability_fields(self) -> None:
        balance, borrowed, interest = exchange_state._bybit_spot_balance(
            {
                "walletBalance": "10",
                "spotBorrow": "",
                "borrowAmount": "3",
                "accruedInterest": "",
                "borrowInterest": "0.5",
            }
        )

        self.assertEqual((balance, borrowed, interest), (Decimal("6.5"), Decimal("3"), Decimal("0.5")))

    def test_flatten_requests_adapter_binds_source_address(self) -> None:
        adapter = flatten.SourceAddressAdapter("172.31.7.124", max_retries=0)

        self.assertEqual(adapter._source_address, ("172.31.7.124", 0))
        self.assertEqual(
            adapter.poolmanager.connection_pool_kw["source_address"],
            ("172.31.7.124", 0),
        )

    def test_probe_rejects_quick_repayment_endpoint(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts" / "bybit_repay_probe.py"),
                "--endpoint",
                "quick-repayment",
            ],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )

        self.assertEqual(result.returncode, 2, result.stdout)
        self.assertIn("invalid choice", result.stdout)


if __name__ == "__main__":
    unittest.main()
