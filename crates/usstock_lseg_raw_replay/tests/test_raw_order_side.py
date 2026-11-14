import sys
from pathlib import Path
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from validate_raw_order_side import sequence_match


class SequenceMatchTests(unittest.TestCase):
    def test_sequence_disambiguates_without_side(self):
        a=dict(sequence='10',price=100,size=1,ms=1000)
        b=dict(a,sequence='11')
        self.assertEqual([r[0] for r in sequence_match([a,b],[b,a])],[1,0])
        self.assertIsNone(sequence_match([a,a],[a])[0][0])
        self.assertIsNone(sequence_match([a],[dict(a,ms=1002)])[0][0])
        self.assertIsNone(sequence_match([a],[dict(a,price=101)])[0][0])
        self.assertIsNone(sequence_match([dict(a,sequence='')],[a])[0][0])


if __name__=='__main__':
    unittest.main()
