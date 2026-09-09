import sys
from pathlib import Path
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from audit_nbbo_fallback import fallback, tick


class FallbackTests(unittest.TestCase):
    def test_midpoint_and_tick(self):
        bid,ask=(100,1),(104,1)
        self.assertEqual(fallback(103,bid,ask,'N','S'),('B','nbbo_midpoint'))
        self.assertEqual(fallback(101,bid,ask,'N','B'),('S','nbbo_midpoint'))
        self.assertEqual(fallback(102,bid,ask,'N','S'),('S','tick_rule'))
        self.assertEqual(fallback(102,bid,ask,'N','N'),('N','tick_unavailable'))
        self.assertEqual(fallback(104,bid,ask,'B','S'),('B','nbbo_touch'))

    def test_invalid_book(self):
        self.assertEqual(fallback(100,(100,1),(100,1),'N','B')[0],'N')
        self.assertEqual(fallback(101,None,(102,1),'N','B')[0],'N')
        self.assertEqual(fallback(101,(100,0),(102,1),'N','B')[0],'N')

    def test_tick_causality_and_equal_prices(self):
        rows=[dict(event_ns=1,source_ns=1,row=1,price=100),
              dict(event_ns=2,source_ns=2,row=2,price=101),
              dict(event_ns=3,source_ns=20,row=3,price=102),
              dict(event_ns=4,source_ns=4,row=4,price=103)]
        self.assertEqual(tick(101,4,10,10,rows,[1,2,3,4]),('B',1))
        self.assertEqual(tick(100,1,10,10,rows,[1,2,3,4]),('N',None))


if __name__=='__main__':
    unittest.main()
