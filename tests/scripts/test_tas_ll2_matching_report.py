import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    'matching', Path(__file__).resolve().parents[2] / 'scripts/report_tas_ll2_matching.py')
matching = importlib.util.module_from_spec(spec)
spec.loader.exec_module(matching)


def fixture():
    return dict(year=2025, product_cf='p:2025:ES', ric='ESH25', is_index=False,
                first_trade_ns=60_000_000_001, last_trade_ns=90_000_000_000,
                raw_done=True, normalized_done=True,
                raw={'first': {'source_ns': 60_000_000_001, 'bucket_ns': 60_000_000_000},
                     'last': {'source_ns': 75_000_000_001, 'bucket_ns': 75_000_000_000}},
                normalized=None, minute_spot_checks=[{'minute_ns': 60_000_000_000, 'raw': {'valid_levels': 0}, 'normalized': None}])


def test_final_invalidation_is_present_but_unusable():
    result = matching.classify(fixture(), {'ESH25'})
    assert result['raw_present'] and result['raw_interval_overlaps_trade']
    assert result['raw_covers_trade_endpoint_minutes']
    assert result['raw_sample_snapshot_present'] == 1
    assert result['raw_sample_valid_1_levels'] == 0
    assert result['normalized_present'] is False


def test_unfinished_year_is_unknown_and_source_presence_is_separate():
    row = fixture(); row['raw_done'] = False
    result = matching.classify(row, {'ESH25'})
    assert result['raw_present'] is None and result['raw_source_present']
    assert result['raw_sample_minutes'] is None


def test_disjoint_actual_source_time_range_does_not_overlap():
    row = fixture()
    row['raw']['last']['source_ns'] = 60_000_000_000
    row['raw']['first']['source_ns'] = 60_000_000_000
    result = matching.classify(row, set())
    assert result['raw_present'] and not result['raw_interval_overlaps_trade']
    assert result['raw_source_present'] is False


def test_multiple_trade_times_in_the_same_minute_count_once():
    row = fixture()
    row['minute_spot_checks'].append(dict(row['minute_spot_checks'][0]))
    result = matching.classify(row, {'ESH25'})
    assert result['raw_sample_minutes'] == 1
    assert result['any_ll2_present'] is True


def test_product_exclusion_removes_whole_products_and_keeps_similar_roots():
    rows = [dict(fixture(), product_cf=f'p:2025:{p}') for p in ('YM', 'YR', 'YRA', 'ES')]
    selected, skipped = matching.partition_products(rows, ['YM', 'YR'])
    assert [r['product_cf'] for r in selected] == ['p:2025:YRA', 'p:2025:ES']
    assert [r['product_cf'] for r in skipped] == ['p:2025:YM', 'p:2025:YR']


def test_contract_exclusion_is_scoped_to_year_product_and_ric():
    rows = [dict(fixture(), year=year, product_cf=f'p:{year}:{product}', ric=ric)
            for year, product, ric in ((2026, 'FBTP', 'FBTPU26'),
                                       (2026, 'FBTP', 'FBTPH26'),
                                       (2025, 'FBTP', 'FBTPU26'),
                                       (2026, 'FGBS', 'FGBSU26'),
                                       (2026, 'FGBS', 'FGBSM26'))]
    selected, skipped = matching.partition_products(
        rows, [], ['2026:FBTP:FBTPU26', '2026:FGBS:FGBSU26'])
    assert len(selected) == 3 and len(skipped) == 2
    assert selected[1]['year'] == 2025
    assert {r['ric'] for r in skipped} == {'FBTPU26', 'FGBSU26'}
