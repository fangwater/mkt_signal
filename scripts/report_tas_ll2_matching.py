#!/usr/bin/env python3
"""Summarize bounded TAS/LL2 seeks; no dense coverage claims or baseline writes."""
import argparse
import csv
import gzip
import hashlib
import json
from collections import defaultdict
from pathlib import Path


def overlap(first_trade, last_trade, endpoints):
    if not endpoints or not endpoints.get('first'):
        return False
    return (endpoints['first']['source_ns'] <= last_trade
            and endpoints['last']['source_ns'] >= first_trade)


def classify(row, source_rics, normalized_source_rics=None):
    out = {k: row[k] for k in ('year', 'product_cf', 'ric', 'is_index', 'first_trade_ns', 'last_trade_ns')}
    out['raw_source_present'] = row['ric'] in source_rics
    out['normalized_source_present'] = row['ric'] in normalized_source_rics if normalized_source_rics is not None else None
    for kind in ('raw', 'normalized'):
        known = row[f'{kind}_done']
        endpoint = row[kind]
        out[f'{kind}_present'] = bool(endpoint and endpoint.get('first')) if known else None
        out[f'{kind}_interval_overlaps_trade'] = overlap(row['first_trade_ns'], row['last_trade_ns'], endpoint) if known else None
        if endpoint and endpoint.get('first'):
            first_minute = endpoint['first']['bucket_ns'] // 60_000_000_000
            last_minute = endpoint['last']['bucket_ns'] // 60_000_000_000
            trade_first = row['first_trade_ns'] // 60_000_000_000
            trade_last = row['last_trade_ns'] // 60_000_000_000
            out[f'{kind}_covers_trade_endpoint_minutes'] = first_minute <= trade_first and last_minute >= trade_last if known else None
            out[f'{kind}_leading_range_gap_days'] = max(0, first_minute - trade_first) / 1440 if known else None
            out[f'{kind}_trailing_range_gap_days'] = max(0, trade_last - last_minute) / 1440 if known else None
        else:
            out[f'{kind}_covers_trade_endpoint_minutes'] = False if known else None
            out[f'{kind}_leading_range_gap_days'] = None
            out[f'{kind}_trailing_range_gap_days'] = None
        points = list({s['minute_ns']: s[kind] for s in row['minute_spot_checks']}.values())
        out[f'{kind}_sample_minutes'] = len(points) if known else None
        out[f'{kind}_sample_snapshot_present'] = sum(p is not None for p in points) if known else None
        for levels in (1, 10):
            out[f'{kind}_sample_valid_{levels}_levels'] = sum(p is not None and p['valid_levels'] >= levels for p in points) if known else None
    present = [out['raw_present'], out['normalized_present']]
    out['any_ll2_present'] = True if any(p is True for p in present) else None if None in present else False
    return out


def write_csv(path, rows):
    if not rows:
        path.write_text('')
        return
    with path.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader(); w.writerows(rows)


def partition_products(rows, excluded_products, excluded_contracts=()):
    excluded = set(excluded_products)
    contracts = set(excluded_contracts)
    selected, skipped = [], []
    for row in rows:
        product = row['product_cf'].split(':', 2)[-1]
        contract = f"{row['year']}:{product}:{row['ric']}"
        target = skipped if product in excluded or contract in contracts else selected
        target.append(row)
    return selected, skipped


def markdown(totals, products, excluded_products=(), excluded_contracts=()):
    lines = ['# 2024–2025 TAS 成交与 LL2 匹配', '',
             '分母来自 TAS RocksDB 的真实 `kind=1` 成交 RIC，不使用 Report 的全部消息计数。主表排除指数，保留其他交易所及源报价别名；逻辑 product 不等于 CME 交易所品种。', '',
             '[全部成交合约](contracts.csv) · [逐品种汇总](products.csv) · [Raw 源包缺失合约](missing_raw_source.csv) · [已完成 Raw 库缺失合约](missing_raw_db.csv) · [完整审计](summary.json)', '',
             '| 年份 | 有成交的非指数 RIC | Raw 源包存在 | Raw 库有快照 | Normalized 库有快照 | Raw 首末分钟覆盖成交首末分钟 |',
             '| --- | ---: | ---: | ---: | ---: | ---: |']
    fmt = lambda n: '待处理/未知' if n is None else f'{n:,}'
    if excluded_products:
        lines.insert(3, 'LL2 补全不处理：' + '、'.join(f'`{x}`' for x in excluded_products) + '；这些品种从分母和缺失清单中剔除，明细保留于 [排除项](excluded_contracts.csv)。\n')
    if excluded_contracts:
        lines.insert(3, 'LL2 补全不处理的指定合约（年份:品种:RIC）：' + '、'.join(f'`{x}`' for x in excluded_contracts) + '；其他到期合约继续保留。\n')
    for t in totals:
        r = t['non_index']
        lines.append('| ' + ' | '.join([str(t['year'])] + [fmt(r[k]) for k in ('trade_rics', 'raw_source_rics', 'raw_present', 'normalized_present', 'raw_covers_trade_endpoint_minutes')]) + ' |')
    lines += ['', '2024 Raw 尚未完成时，源包对应关系仅表示后续候选覆盖，不能称为已经可读取的深度。合约有快照、首末区间重叠或包住成交首末分钟，都不能证明中间连续覆盖。2025 Normalized 补充包已下载但未接入此主库，ES/NQ/RTY/YM 的补充快照尚未入库；源包覆盖另外计入 normalized_source_present，不把未入库算为源包缺失。', '',
              '## 六个研究根', '', '| 年份 | Product | 成交 RIC | Raw 源包 | Raw 库 | Normalized 库 |', '| --- | --- | ---: | ---: | ---: | ---: |']
    for p in products:
        product = p['product_cf'].split(':', 2)[-1]
        if product in ('ES', 'NQ', 'RTY', 'YM', 'GC', 'CL'):
            lines.append('| ' + ' | '.join([str(p['year']), product] + [fmt(p[k]) for k in ('trade_rics', 'raw_source_rics', 'raw_present', 'normalized_present')]) + ' |')
    lines += ['', '## 确定性成交分钟抽查', '',
              '每个 RIC 取首笔、区间中点后首笔、末笔成交对应分钟，同一分钟去重。15s 库只取同分钟最后快照，保留失效状态；不跨分钟补值。这是抽查，不能作为全年逐分钟或逐笔匹配率。', '',
              '| 年份 | 来源 | 抽查分钟 | 有快照 | 有效双边一档 | 连续有效十档 |', '| --- | --- | ---: | ---: | ---: | ---: |']
    for t in totals:
        r = t['non_index']
        for kind in ('raw', 'normalized'):
            lines.append('| ' + ' | '.join([str(t['year']), kind] + [fmt(r[f'{kind}_{k}']) for k in ('sample_minutes', 'sample_snapshot_present', 'sample_valid_1_levels', 'sample_valid_10_levels')]) + ' |')
    lines += ['', '## 全部逻辑品种', '', '| 年份 | Product CF | 成交 RIC | Raw 源包 | Raw 库 | Normalized 库 |', '| --- | --- | ---: | ---: | ---: | ---: |']
    for p in products:
        lines.append('| ' + ' | '.join([str(p['year']), p['product_cf']] + [fmt(p[k]) for k in ('trade_rics', 'raw_source_rics', 'raw_present', 'normalized_present')]) + ' |')
    return '\n'.join(lines) + '\n'


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--seeks', type=Path, required=True)
    p.add_argument('--raw-inventory', type=Path, required=True)
    p.add_argument('--normalized-inventory', type=Path, required=True)
    p.add_argument('--tas-root', type=Path, required=True)
    p.add_argument('--output-root', type=Path, required=True)
    p.add_argument('--exclude-products', nargs='+', default=[], help='Logical products excluded from LL2 completion and matching statistics')
    p.add_argument('--exclude-contracts', nargs='+', default=[], help='Exact contracts to exclude: year:logical_product:RIC')
    a = p.parse_args()
    data = json.loads(a.seeks.read_text())
    inv = json.loads(a.raw_inventory.read_text())
    sources = {int(p['period'][:4]): {r['ric'] for v in p['products'].values() for r in v['active_rics']} for p in inv['packages']}
    norm_inv = json.loads(a.normalized_inventory.read_text())
    norm_sources = defaultdict(set)
    for package in norm_inv['packages']:
        norm_sources[int(package['period'][:4])].update(r['ric'] for v in package['products'].values() for r in v['active_rics'])
    selected, excluded = partition_products(data['rows'], a.exclude_products, a.exclude_contracts)
    rows = [classify(r, sources[r['year']], norm_sources[r['year']]) for r in selected]
    excluded_rows = [classify(r, sources[r['year']], norm_sources[r['year']]) for r in excluded]
    totals, products = [], []
    grouped = defaultdict(list)
    for row in rows:
        grouped[row['year'], row['product_cf']].append(row)
    def summary(group, year, product=None):
        result = {'year': year}
        if product is not None:
            result['product_cf'] = product
        result['trade_rics'] = len(group)
        result['raw_source_rics'] = sum(r['raw_source_present'] for r in group)
        result['normalized_source_rics'] = sum(r['normalized_source_present'] for r in group)
        result['any_ll2_present'] = sum(r['any_ll2_present'] is True for r in group)
        result['any_ll2_unknown'] = sum(r['any_ll2_present'] is None for r in group)
        for kind in ('raw', 'normalized'):
            known = all(r[f'{kind}_present'] is not None for r in group)
            for suffix in ('present', 'interval_overlaps_trade', 'covers_trade_endpoint_minutes', 'sample_minutes', 'sample_snapshot_present', 'sample_valid_1_levels', 'sample_valid_10_levels'):
                result[f'{kind}_{suffix}'] = sum(r[f'{kind}_{suffix}'] for r in group) if known else None
        return result
    for (year, product), group in sorted(grouped.items()):
        products.append(summary(group, year, product))
    for year in sorted({r['year'] for r in rows}):
        all_rows = [r for r in rows if r['year'] == year]
        total = summary(all_rows, year)
        futures = [r for r in all_rows if not r['is_index']]
        total['non_index'] = summary(futures, year)
        total['index_rics'] = sum(r['is_index'] for r in all_rows)
        total['trade_products'] = len({r['product_cf'] for r in futures})
        report = next(a.tas_root.glob(f'*tas_{year}-01-01_*/merged-Report.csv.gz'))
        with gzip.open(report, 'rt') as f:
            report_rics = {r['#RIC'] for r in csv.DictReader(f) if int(r['Count']) > 0}
        total['tas_report_active_rics'] = len(report_rics)
        total['tas_report_sha256'] = hashlib.sha256(report.read_bytes()).hexdigest()
        total['trade_rics_not_in_active_report'] = sorted({r['ric'] for r in all_rows} - report_rics)
        totals.append(total)
    a.output_root.mkdir(parents=True, exist_ok=True)
    write_csv(a.output_root/'contracts.csv', rows)
    write_csv(a.output_root/'excluded_contracts.csv', excluded_rows)
    write_csv(a.output_root/'products.csv', products)
    write_csv(a.output_root/'missing_raw_source.csv', [r for r in rows if not r['raw_source_present']])
    write_csv(a.output_root/'missing_raw_db.csv', [r for r in rows if r['raw_present'] is False])
    report = {'measured_at': data['measured_at'], 'years': totals, 'source_seeks': str(a.seeks),
              'excluded_products': sorted(set(a.exclude_products)), 'excluded_trade_rics': len(excluded_rows),
              'excluded_contracts': sorted(set(a.exclude_contracts)),
              'raw_inventory': str(a.raw_inventory), 'raw_inventory_sha256': hashlib.sha256(a.raw_inventory.read_bytes()).hexdigest(),
              'normalized_inventory': str(a.normalized_inventory), 'normalized_inventory_sha256': hashlib.sha256(a.normalized_inventory.read_bytes()).hexdigest(),
              'scope': 'Contract endpoints and up to three deterministic trade-minute samples per RIC; no full minute scan. Raw year not done is unknown.'}
    (a.output_root/'summary.json').write_text(json.dumps(report, indent=2)+'\n')
    (a.output_root/'coverage.md').write_text(markdown(totals, products, sorted(set(a.exclude_products)), sorted(set(a.exclude_contracts))))
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
