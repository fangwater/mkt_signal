import gzip
import unittest

from scripts.audit_cme_ll2_inventory import Links, parse_product, summarize_report


class Ll2InventoryTests(unittest.TestCase):
    def test_routes_live_contracts_and_keeps_source_aliases(self):
        fixtures = {"ESH25": "ES", "YMM25": "YM", "ADF26^2": "AD",
                    "HSIEAS": "HSIEAS", "Cc1": "Cc1", "VX01F0": "VX01",
                    ".NSEI": "NSEI", "VXF3:VE": "VXF3:VE"}
        self.assertEqual({ric: parse_product(ric) for ric in fixtures}, fixtures)

    def test_counts_only_active_rics_and_preserves_partial_ranges(self):
        source = ("#RIC,Domain,Start,End,Status,Count\n"
                  "ESH25,Market Price,2025-01-01T22:00:00Z,2025-03-21T21:00:00Z,Active,10\n"
                  "ESM25,Market Price,2025-03-01T22:00:00Z,2025-06-20T21:00:00Z,Active,20\n"
                  "YMM25,Market Price,2025-04-15T15:43:20Z,2025-06-20T21:00:00Z,Active,5\n"
                  "YMH25,Market Price,,,Inactive,0\n")
        result = summarize_report(gzip.compress(source.encode()))
        self.assertEqual((result["report_rows"], result["active_rics"], result["source_rows"]), (4, 3, 35))
        self.assertEqual(result["products"]["ES"]["source_rows"], 30)
        self.assertEqual(result["products"]["YM"]["first_event_utc"], "2025-04-15T15:43:20Z")

    def test_listing_decodes_spaces_without_including_parent(self):
        parser = Links()
        parser.feed('<a href="../">parent</a><a href="package/">p</a><a href="Source%20Notes.txt">notes</a>')
        self.assertEqual(parser.names, ["package/", "Source Notes.txt"])


if __name__ == "__main__":
    unittest.main()
