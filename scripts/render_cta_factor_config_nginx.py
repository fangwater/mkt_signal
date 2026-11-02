#!/usr/bin/env python3
"""Render the unified CTA config gateway. Printing only; never installs/reloads."""
import argparse
from pathlib import Path
import re

def render(source_ids: str, port: int) -> str:
    ids = source_ids.split(',')
    if not ids or any(not re.fullmatch(r'[a-zA-Z0-9_-]{1,128}', item) for item in ids):
        raise ValueError('source IDs must be a comma-separated explicit account scope')
    if len(set(ids)) != len(ids) or not 1 <= port <= 65535:
        raise ValueError('duplicate source IDs or invalid port')
    template = Path(__file__).resolve().parents[1] / 'config/nginx/cta_factor_config.conf.template'
    return template.read_text().replace('__CTA_CONFIG_SOURCE_IDS__', ','.join(ids)).replace('__CTA_CONFIG_PORT__', str(port))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-ids', required=True)
    parser.add_argument('--port', type=int, default=19220)
    args = parser.parse_args()
    print(render(args.source_ids, args.port), end='')
