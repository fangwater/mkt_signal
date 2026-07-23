import importlib.util
import pathlib
import unittest


SCRIPTS_DIR = pathlib.Path(__file__).resolve().parents[1]


def load_script(name):
    path = SCRIPTS_DIR / name
    spec = importlib.util.spec_from_file_location(name.removesuffix(".py"), path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class ExecRiskKeyTests(unittest.TestCase):
    def test_risk_key_matches_exec_pre_trade_prefix(self):
        sync = load_script("sync_exec_risk_params.py")
        printer = load_script("print_exec_risk_params.py")
        expected = "cta_exec_trade:binance-futures:pre_trade_risk_params"
        self.assertEqual(
            sync.build_risk_params_key("cta_exec_trade", "binance-futures"),
            expected,
        )
        self.assertEqual(
            printer.build_risk_params_key("cta_exec_trade", "binance-futures"),
            expected,
        )


if __name__ == "__main__":
    unittest.main()
