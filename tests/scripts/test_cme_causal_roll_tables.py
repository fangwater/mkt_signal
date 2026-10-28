import copy
import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from cme_causal_roll import replay
from cme_causal_roll_tables import daily_rows, export_tables, load_daily

FIXTURE = ROOT / "crates/cme_tas_replay/tests/fixtures/causal_roll.json"


class CausalTablesTest(unittest.TestCase):
    def setUp(self):
        self.data = json.loads(FIXTURE.read_text())

    def test_prefix_prices_and_offsets_are_immutable(self):
        full = daily_rows(replay(self.data))
        self.assertEqual([r["offset_e9"] for r in full], [0, 0, 62500000, 62500000, 62500000])
        self.assertEqual(full[2]["contract_id"], "CBOT:TY:2026-06")
        for n in range(1, len(full) + 1):
            prefix = copy.deepcopy(self.data)
            prefix["decisions"] = prefix["decisions"][:n]
            self.assertEqual(daily_rows(replay(prefix)), full[:n])

    def test_missing_bridge_starts_a_new_segment(self):
        self.data["settlements"] = []
        rows = daily_rows(replay(self.data))
        self.assertEqual([r["segment_id"] for r in rows], [1, 1, 2, 2, 2])
        self.assertEqual(rows[2]["adjustment_status"], "missing_bridge_new_segment")
        self.assertEqual(rows[2]["offset_e9"], 0)

    def test_exit_only_retains_an_explicit_empty_day(self):
        self.data["decisions"] = self.data["decisions"][:3]
        day = self.data["decisions"][-1]["previous_day"]
        for batch in self.data["volumes"]:
            if batch["trading_day"] == day:
                batch["complete"] = False
        rows = daily_rows(replay(self.data))
        self.assertEqual(rows[-1]["reason"], "exit_only")
        self.assertEqual(rows[-1]["ric"], "")
        self.assertIsNone(rows[-1]["offset_e9"])

    def test_blocked_input_never_writes_partial_tables(self):
        self.data["decisions"][2]["calendar_known_at_ns"] = None
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "roll"
            with self.assertRaisesRegex(ValueError, "blocked"):
                export_tables(self.data, output, "fixture")
            self.assertFalse(output.exists())
            self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_table_roundtrip_and_chinese_rule_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "roll"
            result = export_tables(self.data, output, "fixture")
            self.assertEqual(result["products"]["TY"]["selected_days"], 5)
            self.assertEqual(load_daily(output / "CBOT/TY/dominant.csv"), daily_rows(replay(self.data)))
            report = (output / "CBOT/TY/换月依据.md").read_text()
            self.assertIn("期限优先", report)
            self.assertIn("1161479", report)
            self.assertIn("62500000 e9", report)
            with self.assertRaises(FileExistsError):
                export_tables(self.data, output, "fixture")

    def test_known_future_session_is_explained_without_inventing_exit_time(self):
        data = json.loads(FIXTURE.with_name('causal_roll_future_calendar.json').read_text())
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'roll'
            export_tables(data, output, 'synthetic_fixture')
            self.assertEqual(load_daily(output / 'CBOT/TY/dominant.csv'), daily_rows(replay(self.data)))
            report = (output / 'CBOT/TY/换月依据.md').read_text()
            self.assertIn('官方交易日 2026-02-24 的全部计划区间', report)
            self.assertIn('未推测或提前填入退出时刻', report)
            saved = json.loads((output / 'decisions.json').read_text())
            self.assertIsNone(saved[0]['selected_roll_by_ns'])

    def test_missing_day_and_blocked_csv_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "roll"
            export_tables(self.data, output, "fixture")
            path = output / "CBOT/TY/dominant.csv"
            lines = path.read_text().splitlines()
            path.write_text("\n".join(lines[:2] + lines[3:]) + "\n")
            with self.assertRaisesRegex(ValueError, "missing/out-of-order"):
                load_daily(path)


if __name__ == "__main__":
    unittest.main()
