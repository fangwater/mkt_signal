import sys
from pathlib import Path
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from audit_bat_odd_side import force


class ForceTests(unittest.TestCase):
    def test_priority_and_labels(self):
        self.assertEqual(force('B','venue_bbo_touch','S','S'),('B','venue_bbo_touch'))
        self.assertEqual(force('N','locked_or_crossed','S','B'),('S','forced_tick_rule'))
        self.assertEqual(force('N','invalid_book','N','S'),('S','forced_previous_side'))
        self.assertEqual(force('N','invalid_book','N',None),('B','forced_default_buy'))


if __name__=='__main__':
    unittest.main()
