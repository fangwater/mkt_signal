import copy
import json
from pathlib import Path
import tempfile
import time
import unittest

from activate_cta_perp_rx02 import binding_plan, positions, warm_rows


class CutoverTests(unittest.TestCase):
    def account(self):
        return {"name": "lgq_cta", "can_configure": True,
                "followers": [{"source_id": "binance_exec_trade05", "multiplier": 1, "pending_publishes": []}],
                "bindings": [{"binding_name": "existing_" + f, "position_strategy_name": "lgq_cta_" + f,
                              "order_strategy_name": "lgq_cta_ctaker", "shares": 1}
                             for f in ("factor_036", "factor_051", "factor_170", "TD_TI_002", "baseline_004")]}

    def test_retains_binding_identity_and_stops_retired_strategies(self):
        account = self.account()
        original = copy.deepcopy(account)
        plan = binding_plan(account, ["factor_036", "baseline_141", "baseline_200", "TD_TI_024", "baseline_164"], "lgq_cta_")
        self.assertEqual(account, original)
        self.assertEqual(len(plan["bindings"]), 9)
        self.assertEqual(sum(r["shares"] for r in plan["bindings"]), 5)
        self.assertEqual(plan["bindings"][0], original["bindings"][0])
        self.assertTrue(all(r["shares"] == 0 for r in plan["bindings"][1:5]))

    def test_refuses_changed_allocation_or_extra_follower(self):
        for mutate in (lambda a: a["bindings"][0].update(shares=2),
                       lambda a: a["followers"].append(copy.deepcopy(a["followers"][0])),
                       lambda a: a["followers"][0].update(pending_publishes=[{}])):
            account = self.account()
            mutate(account)
            with self.assertRaises(RuntimeError):
                binding_plan(account, ["factor_036"], "lgq_cta_")

    def test_state_keeps_retired_symbols_and_exact_quantities(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.json"
            state = {"factor_036": {"NEARUSDT": {"flag": -1, "qty": -31.4567, "entry_notional": 100}},
                     "factor_051": {"FILUSDT": {"flag": -1, "qty": -22.5, "entry_notional": 100}}}
            path.write_text(json.dumps(state))
            self.assertEqual(positions(path), state)
            state["factor_051"]["FILUSDT"]["qty"] = float("nan")
            path.write_text(json.dumps(state))
            with self.assertRaises(RuntimeError):
                positions(path)

    def test_warmup_rejects_stale_or_unready_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = Path(tmp)
            root = env / "data/cta_perp/factor_history"
            root.mkdir(parents=True)
            path = root / "20261013.jsonl"
            row = {"symbol": "NEARUSDT", "model_ts_ms": int(time.time() * 1000),
                   "nq": {"ready": True}, "inputs": {"factor_036": {"ready": True}}}
            path.write_text(json.dumps(row) + "\n")
            warm_rows(env, {"NEARUSDT": {"factor_036": -1}})
            row["inputs"]["factor_036"]["ready"] = False
            path.write_text(json.dumps(row) + "\n")
            with self.assertRaises(RuntimeError):
                warm_rows(env, {"NEARUSDT": {"factor_036": -1}})
            row["inputs"]["factor_036"]["ready"] = True
            row["model_ts_ms"] -= 300_000
            path.write_text(json.dumps(row) + "\n")
            with self.assertRaises(RuntimeError):
                warm_rows(env, {"NEARUSDT": {"factor_036": -1}})


if __name__ == "__main__":
    unittest.main()
