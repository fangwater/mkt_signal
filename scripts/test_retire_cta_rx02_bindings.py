import unittest

from retire_cta_rx02_bindings import retirement_plan, retired_targets_zero


class RetirementTests(unittest.TestCase):
    def test_symbol_reuse_does_not_preserve_old_factor(self):
        current = {'NEARUSDT': {'factor_036': -1, 'factor_051': -1},
                   'ETHUSDT': {'baseline_004': -1}}
        desired = {'NEARUSDT': {'factor_036': -1}, 'ETHUSDT': {'baseline_141': 1}}
        state = {'factor_170': {'DOTUSDT': {'qty': 2, 'flag': 1}}}
        retained, retired = retirement_plan(current, desired, state)
        self.assertEqual(retained, {'NEARUSDT': {'factor_036': -1}})
        self.assertEqual(set(retired), {('NEARUSDT', 'factor_051'), ('ETHUSDT', 'baseline_004'),
                                        ('DOTUSDT', 'factor_170')})

    def test_sign_change_is_not_silently_carried(self):
        with self.assertRaises(RuntimeError):
            retirement_plan({'NEARUSDT': {'factor_036': -1}},
                            {'NEARUSDT': {'factor_036': 1}}, {})

    def test_missing_or_nonzero_target_is_not_cleared(self):
        retired = [('ETHUSDT', 'baseline_004')]
        self.assertFalse(retired_targets_zero({}, retired))
        self.assertFalse(retired_targets_zero({'baseline_004': {'ETHUSDT': {'qty': -1, 'flag': -1}}}, retired))
        self.assertTrue(retired_targets_zero({'baseline_004': {'ETHUSDT': {'qty': 0, 'flag': 0}}}, retired))


if __name__ == '__main__':
    unittest.main()
