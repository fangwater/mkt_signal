import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from audit_single_side import classify, history, previous


class SingleSideTests(unittest.TestCase):
    def test_one_side(self):
        bid, ask = (100, 1, 'NAS', 1, 1), (102, 1, 'PSE', 1, 1)
        self.assertEqual(classify(102, 'PSE', bid, ask, 'single')[0], 'B')
        self.assertEqual(classify(100, 'NAS', bid, ask, 'single')[0], 'S')
        self.assertEqual(classify(102, 'PSE', bid, ask, 'both')[0], 'N')
        self.assertEqual(classify(101, 'PSE', bid, ask, 'single')[0], 'N')
        self.assertEqual(classify(102, 'BAT', bid, ask, 'single')[0], 'N')

    def test_invalid_book(self):
        for bid in [(102,1,'NAS',1,1), (103,1,'NAS',1,1)]:
            self.assertEqual(classify(102,'NAS',bid,(102,1,'NAS',1,1),'single')[0],'N')
        self.assertEqual(classify(102,'NAS',None,(102,0,'NAS',1,1),'single')[0],'N')

    def test_causality_and_contributor_replacement(self):
        q = {'ask': [(10,1,(102,1,'NAS',10,5)), (20,2,(103,1,'BAT',20,15))]}
        h = history(q, 'event_ms')['ask']
        self.assertEqual(previous(h,15,30,3)[2], 'NAS')
        self.assertEqual(previous(h,16,30,3)[2], 'BAT')
        self.assertEqual(previous(h,16,19,3)[2], 'NAS')
        self.assertIsNone(previous(h,5,30,3))


if __name__ == '__main__':
    unittest.main()
