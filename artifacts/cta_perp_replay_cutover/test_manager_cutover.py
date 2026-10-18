"""Safety checks for the live RX02 binding migration, with no external services."""
import copy
import importlib.util
import json
import math
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
import tempfile

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("manager_cutover", HERE / "manager_cutover.py")
cutover = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cutover)


class MigrationChecks(unittest.TestCase):
    def test_quantity_round_trip_accepts_only_one_ulp_and_preserves_zero(self):
        self.assertTrue(cutover.same_quantity(-1358.0805794477135, -1358.0805794477137))
        value = -1358.0805794477135
        two_ulps = math.nextafter(math.nextafter(value, -math.inf), -math.inf)
        for left, right in [(value, two_ulps), (0, 1e-300), (1, -1),
                            (math.inf, math.inf), (math.nan, 1), ("clear", 0), (True, 1)]:
            self.assertFalse(cutover.same_quantity(left, right))
        self.assertTrue(cutover.same_quantity(0, 0.0))

    def setUp(self):
        self.plan = json.loads((HERE / "manager_binding_plan.json").read_text())
        self.before = json.loads((HERE / "before.json").read_text())
        self.old = [row for row in self.plan["bindings_during_recovery"]
                    if row["binding_name"] != cutover.MEAN]
        self.state = {
            "bindings": self.before["bindings"], "virtuals": self.before["virtuals"],
            "followers": [{"source_id": cutover.SOURCE, "virtual_id": cutover.VIRTUAL, "multiplier": 1}],
            "pending": [], "targets": self.before["targets"],
        }
        self.shadow = self.before["environments"]["binance-cta-perp-rx02"]["run/cta_perp_position_flags.json"]

    def clear_all(self):
        for row in self.state["targets"]:
            if row["strategy_name"].startswith(cutover.PREFIX):
                row["targets"] = {symbol: {"qty": "clear", "signal": 0} for symbol in row["targets"]}

    def test_reviewed_scope_preserves_rx03_and_all_nine_legacy_names(self):
        cutover.scope_check(self.state, self.old)
        self.assertEqual(len(self.old), 9)
        self.clear_all()
        cutover.verify_retired_targets(self.state, self.old, self.shadow)
        changed = copy.deepcopy(self.state)
        changed["bindings"] = [row for row in changed["bindings"] if row["source_id"] != cutover.SOURCE]
        changed["bindings"].extend(dict(row, source_id=cutover.SOURCE)
                                   for row in self.plan["bindings_after_final_targets_verified"])
        self.assertEqual(cutover.protected(changed), cutover.protected(self.state))

    def test_rejects_additional_follower_to_protect_other_accounts(self):
        self.state["followers"].append({"source_id": "binance_exec_trade06", "virtual_id": cutover.VIRTUAL, "multiplier": 1})
        with self.assertRaisesRegex(RuntimeError, "follower scope changed"):
            cutover.scope_check(self.state, self.old)

    def test_rejects_operator_share_changes_and_pending_publication(self):
        self.state["bindings"][0]["shares"] = 2
        with self.assertRaisesRegex(RuntimeError, "effective bindings differ"):
            cutover.scope_check(self.state, self.old)
        self.state["bindings"][0]["shares"] = self.old[0]["shares"]
        self.state["pending"] = [{"binding_name": self.old[0]["binding_name"], "error": None}]
        with self.assertRaisesRegex(RuntimeError, "publication is still pending"):
            cutover.scope_check(self.state, self.old)

    def test_retirement_requires_clear_even_for_previously_zero_share_bindings(self):
        self.clear_all()
        stopped = next(row for row in self.old if row["shares"] == 0)
        target = next(row for row in self.state["targets"] if row["strategy_name"] == stopped["position_strategy_name"])
        symbol = next(iter(target["targets"]))
        target["targets"][symbol]["qty"] = 0
        with self.assertRaisesRegex(RuntimeError, "explicit clears"):
            cutover.verify_retired_targets(self.state, self.old, self.shadow)
        target["targets"][symbol]["qty"] = 1
        with self.assertRaisesRegex(RuntimeError, "explicit clears"):
            cutover.verify_retired_targets(self.state, self.old, self.shadow)

    def test_retirement_rejects_missing_historical_symbol(self):
        self.clear_all()
        target = next(row for row in self.state["targets"] if row["strategy_name"] == self.old[0]["position_strategy_name"])
        del target["targets"][next(iter(target["targets"]))]
        with self.assertRaisesRegex(RuntimeError, "omits historical symbols"):
            cutover.verify_retired_targets(self.state, self.old, self.shadow)

    def test_protected_binding_changes_are_detected(self):
        previous = cutover.protected(self.state)
        changed = copy.deepcopy(self.state)
        next(row for row in changed["bindings"] if row["source_id"] == "binance_exec_trade07")["shares"] = 2
        self.assertNotEqual(previous, cutover.protected(changed))

    def test_runtime_verification_distinguishes_numeric_zero_from_explicit_clear(self):
        expected = {"BTCUSDT": {"qty": "clear", "signal": 0}}
        client = Mock()
        redis = Mock()
        redis.Redis.from_url.return_value = client
        with patch.dict("sys.modules", {"redis": redis}), patch.object(cutover, "env_values", return_value={}):
            client.get.return_value = json.dumps({"targets": {"BTCUSDT": {"qty": 0, "signal": 0}}, "updated_at_us": 123})
            with self.assertRaisesRegex(RuntimeError, "differs from the final published target"):
                cutover.verify_runtime_targets("legacy", expected, {"updated_at_us": 123})
            client.get.return_value = json.dumps({"targets": expected, "updated_at_us": 123})
            cutover.verify_runtime_targets("legacy", expected, {"updated_at_us": 123})
            with self.assertRaisesRegex(RuntimeError, "differs from the final published target"):
                cutover.verify_runtime_targets("legacy", expected, {"updated_at_us": 124})
            self.assertFalse(client.set.called)

    def test_retirement_never_disables_old_bindings_before_runtime_clear_is_verified(self):
        self.clear_all()
        row = self.old[0]
        target = next(item["targets"] for item in self.state["targets"]
                      if item["strategy_name"] == row["position_strategy_name"])
        api = Mock(return_value={"source_id": cutover.SOURCE, "strategy_name": row["binding_name"],
                                 "nonzero_target_count": 0, "target_count": len(target), "updated_at_us": 123})
        import contextlib
        with patch.object(cutover, "recovered_targets", return_value=self.old), \
                patch.object(cutover, "session", return_value=contextlib.nullcontext(api)), \
                patch.object(cutover, "verify_runtime_targets", side_effect=RuntimeError("clear not confirmed")):
            with self.assertRaisesRegex(RuntimeError, "clear not confirmed"):
                cutover.retire(self.state, self.plan)
        self.assertEqual(api.call_count, 1)
        self.assertTrue(api.call_args.args[0].endswith("/publish"))

    def test_legacy_publisher_gets_scoped_token_only_after_install_with_env_backup(self):
        with tempfile.TemporaryDirectory() as directory:
            env = Path(directory)
            original = b"export IPC_NAMESPACE='fixture'\n"
            (env / "env.sh").write_bytes(original)
            (env / "env.sh").chmod(0o600)
            with patch.object(cutover, "SIGNAL", env), patch.object(cutover, "env_values", return_value={}):
                with self.assertRaisesRegex(RuntimeError, "install the reviewed Perp release"):
                    cutover.publication_token()
                self.assertEqual((env / "env.sh").read_bytes(), original)
                (env / "run/cutover_before_20261015").mkdir(parents=True)
                token = cutover.publication_token()
                self.assertEqual((env / "run/cutover_before_20261015/env.sh").read_bytes(), original)
                self.assertTrue((env / "env.sh").read_bytes().startswith(original))
                self.assertIn("export CTA_PUBLISH_TOKEN='" + token + "'", (env / "env.sh").read_text())
                self.assertEqual((env / "env.sh").stat().st_mode & 0o777, 0o600)

    def test_existing_publish_token_is_reused_without_env_mutation(self):
        with patch.object(cutover, "env_values", return_value={"CTA_PUBLISH_TOKEN": "fixture"}):
            self.assertEqual(cutover.publication_token(), "fixture")


if __name__ == "__main__":
    unittest.main()
