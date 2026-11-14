"""Reuse verified native Binance inputs; add newly required hourly BBO symbols."""
import hashlib
import json
from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parents[2]
TASK = Path(__file__).resolve().parent
OLD = ROOT / 'data/ops/cta_spread02_deployment/binance_hourly_current'
NEW = ROOT / 'data/ops/cta_spread02_tardis_warmup_20261113/history/binance-futures'


def main():
    previous = json.loads((OLD / 'manifest.json').read_text())
    built = json.loads((NEW / 'manifest.json').read_text())
    supplement = NEW.parents[1] / 'binance_reclassified'
    extra = json.loads((supplement / 'manifest.json').read_text())
    for key in ('market', 'interval', 'start_ms', 'end_ms'):
        assert extra[key] == built[key], key
    for symbol, meta in extra['symbols'].items():
        source = supplement / meta['file']
        assert hashlib.sha256(source.read_bytes()).hexdigest() == meta['sha256'], symbol
        assert symbol not in built['symbols']
        shutil.copy2(source, NEW / meta['file'])
        built['symbols'][symbol] = meta
    for directory in ['binance_eth_bbo', 'binance_xrp_bbo']:
        recent = NEW.parents[1] / directory
        bbo = json.loads((recent / 'manifest.json').read_text())
        for key in ('market', 'interval', 'end_ms'):
            assert bbo[key] == built[key], key
        for symbol, meta in bbo['symbols'].items():
            assert meta['rows'] >= 48
            source = recent / meta['file']
            assert hashlib.sha256(source.read_bytes()).hexdigest() == meta['sha256'], symbol
            assert symbol not in built['symbols']
            shutil.copy2(source, NEW / meta['file'])
            built['symbols'][symbol] = meta
    for key in ('market', 'interval', 'start_ms', 'end_ms'):
        assert previous[key] == built[key], key
    control = json.loads((TASK / 'control_snapshot.json').read_text())
    apply_month = control['threshold_metadata']['apply_month']
    reused = {}
    for symbol, meta in previous['symbols'].items():
        if symbol in built['symbols']:
            continue
        assert meta['source_book_events'] > 0, symbol
        if apply_month in meta['thresholds']:
            assert meta['thresholds'][apply_month] == control['amount_thresholds'][f'binance-futures:{symbol}'], symbol
        source = OLD / meta['file']
        assert hashlib.sha256(source.read_bytes()).hexdigest() == meta['sha256'], symbol
        assert not (NEW / meta['file']).exists(), symbol
        shutil.copy2(source, NEW / meta['file'])
        built['symbols'][symbol] = meta
        reused[symbol] = meta['sha256']
    assert len(built['symbols']) == 17
    built['symbols'] = dict(sorted(built['symbols'].items()))
    built['source'] += '; includes checksum-verified native inputs from the existing Binance archive'
    (NEW / 'manifest.json').write_text(json.dumps(built, indent=2) + '\n')
    (TASK / 'binance_reused_inputs.json').write_text(json.dumps({'source': str(OLD), 'manifest_sha256': hashlib.sha256((OLD / 'manifest.json').read_bytes()).hexdigest(), 'files': reused}, indent=2) + '\n')
    print(f'Binance archive: {len(built["symbols"])} symbols; reused {len(reused)} native files')


if __name__ == '__main__':
    main()
