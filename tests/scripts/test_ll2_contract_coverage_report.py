import unittest

from scripts.build_ll2_contract_coverage_report import compare


def package(year, records, known=True):
    return {"period": f"{year}-01-01_{year + 1}-01-01", "directory": f"source-{year}",
            "report_present": known, "products": {"ES": {"active_rics": records}} if records else {}}


def record(ric, count):
    return {"ric": ric, "count": count, "start": "2024-01-01T00:00:00Z", "end": "2024-12-31T23:59:59Z"}


class ContractCoverageTests(unittest.TestCase):
    def test_unknown_raw_is_not_zero_or_normalized_only(self):
        rows, products, summaries = compare(
            {"packages": [package(2025, [record("ESH25", 100000)])]},
            {"packages": [package(2025, [], known=False)]}, [2025])
        self.assertEqual(rows[0]["coverage"], "raw_unknown")
        self.assertIsNone(rows[0]["raw_count"])
        self.assertIsNone(products[0]["raw_only_rics"])
        self.assertIsNone(summaries[0]["normalized_only_rics"])

    def test_extra_rics_and_counts_do_not_turn_into_extra_depth(self):
        rows, products, summaries = compare(
            {"packages": [package(2024, [record("ESH24", 100000)])]},
            {"packages": [package(2024, [record("ESH24", 900000), record("ESH28", 1555)])]}, [2024])
        self.assertEqual([r["coverage"] for r in rows], ["both", "raw_only"])
        self.assertEqual(products[0]["raw_only_ric_list"], "ESH28")
        self.assertEqual(summaries[0]["raw_only_report_count_bins"], {"1001-10000": 1})
        self.assertEqual(summaries[0]["raw_extra_pct_of_normalized"], 100.0)

    def test_overlapping_packages_cannot_double_count_a_contract(self):
        norm = {"packages": [package(2025, [record("ESH25", 10)]), package(2025, [record("ESH25", 20)])]}
        with self.assertRaisesRegex(ValueError, "Overlapping active RIC"):
            compare(norm, {"packages": [package(2025, [], known=False)]}, [2025])


if __name__ == "__main__":
    unittest.main()
