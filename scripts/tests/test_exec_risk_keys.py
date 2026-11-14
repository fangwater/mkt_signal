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

    def test_sync_contains_only_active_batch_exec_limits(self):
        sync = load_script("sync_exec_risk_params.py")
        printer = load_script("print_exec_risk_params.py")
        expected = {
            "max_pending_limit_orders": "10",
            "exec_max_pending_limit_buy_orders": "10",
            "exec_max_pending_limit_sell_orders": "10",
            "exec_order_rate_limit_per_min": "400",
            "exec_order_rate_limit_10s": "200",
        }

        self.assertEqual(sync.RISK_PARAMS, expected)
        self.assertEqual(sync.PARAM_ORDER, list(expected))
        for key, value in expected.items():
            self.assertEqual(sync.RISK_PARAMS[key], value)
            self.assertIn(key, sync.PARAM_ORDER)
            self.assertIn(key, printer.PARAM_ORDER)

    def test_sync_omits_non_batch_exec_risk_fields(self):
        sync = load_script("sync_exec_risk_params.py")
        for key in ("max_pos_u", "max_leverage", "exec_max_position_imbalance_ratio"):
            self.assertNotIn(key, sync.RISK_PARAMS)


if __name__ == "__main__":
    unittest.main()
