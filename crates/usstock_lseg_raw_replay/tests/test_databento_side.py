import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'tools'))
from validate_databento_side import match, metrics, estimate


class MatchingTests(unittest.TestCase):
    def test_reciprocal_uniqueness(self):
        row = dict(price=100,size=10,ms=1000)
        self.assertEqual(match([row],[row])[0][:2],(0,'unique'))
        self.assertEqual(match([row,row],[row])[0][1],'ambiguous')
        self.assertEqual(match([row],[row,row])[0][1],'ambiguous')
        self.assertEqual(match([row],[dict(row,ms=1001)])[0][1],'unmatched')
        self.assertEqual(match([row],[dict(row,ms=1001)],1)[0][1],'unique')
        self.assertEqual(match([row],[dict(row,size=11)],1)[0][1],'unmatched')

    def test_unknown_not_ground_truth_and_fallback_scope(self):
        row = dict(truth='N',single='N',nbbo='B',single_reason='no_matching_contributor',volume=10)
        self.assertEqual(estimate(row,'hybrid'),'B')
        self.assertEqual(estimate(dict(row,single_reason='price_not_decisive'),'hybrid'),'N')
        result = metrics([row,dict(row,truth='B'),dict(row,truth='S')],'hybrid')
        self.assertEqual(result['truth_known'],2)
        self.assertEqual(result['accuracy'],0.5)


if __name__ == '__main__':
    unittest.main()
