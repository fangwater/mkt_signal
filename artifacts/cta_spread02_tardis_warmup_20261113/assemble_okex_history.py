"""Combine 16 long formula archives with ETH's complete 240-hour warm-up."""
import hashlib
import json
from pathlib import Path
import shutil

ROOT=Path(__file__).resolve().parents[2]
TASK=Path(__file__).resolve().parent
HISTORY=ROOT/'data/ops/cta_spread02_tardis_warmup_20261113/history/okex-futures'
RECENT=HISTORY.parents[1]/'okex_eth_recent'


def main():
    built=json.loads((HISTORY/'manifest.json').read_text())
    extra=json.loads((RECENT/'manifest.json').read_text())
    for key in ['market','interval','end_ms']:
        assert built[key]==extra[key],key
    assert len(built['symbols'])==16 and 'ETHUSDT' not in built['symbols']
    meta=extra['symbols']['ETHUSDT']
    assert meta['rows']==240
    assert hashlib.sha256((RECENT/meta['file']).read_bytes()).hexdigest()==meta['sha256']
    shutil.copy2(RECENT/meta['file'],HISTORY/meta['file'])
    built['symbols']['ETHUSDT']=meta
    built['symbols']=dict(sorted(built['symbols'].items()))
    built['source']+='; ETH native inputs cover 2026-11-01 through 2026-11-11 (240 closed hours)'
    (HISTORY/'manifest.json').write_text(json.dumps(built,indent=2)+'\n')
    print('OKX archive: 17 symbols; ETH 240 hours, other symbols 816 hours')


if __name__=='__main__':main()
