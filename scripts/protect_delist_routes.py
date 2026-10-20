#!/usr/bin/env python3
"""Render a scoped auth patch for the existing FR Nginx routes; no live writes."""
import argparse
from pathlib import Path
import re

from configure_delist_access import FR_ENVS


GATE = 'include /etc/nginx/snippets/delist_fr_gate.conf;'
ACCESS = 'include /etc/nginx/snippets/delist_access.conf;'


def protect(text, envs):
    seen = {env: set() for env in envs}

    def patch(match):
        block = match.group()
        location = re.match(r'location\s+(?:=\s+)?(/[^\s{]+)', block)
        if not location:
            return block
        path = location[1]
        for env in envs:
            if path == f'/fr/{env}' or path.startswith(f'/fr/{env}/'):
                seen[env].add(path.removeprefix(f'/fr/{env}'))
                if 'return 301' in block or 'return 302' in block:
                    return block
                if GATE not in block:
                    block = block.replace('{', '{\n        ' + GATE, 1)
                # The gate owns no-store; remove the older static cache header.
                block = re.sub(r'\s*add_header Cache-Control "no-cache";', '', block)
                return block
        return block

    text = re.sub(r'location\s+[^\n{]+\{[^{}]*\}', patch, text)
    expected = {'/config', '/config/', '/', '/ws', '/healthz', '/snapshot', '/fr_ws'}
    for env, paths in seen.items():
        if missing := expected - paths:
            raise ValueError(f'{env}: missing routes {sorted(missing)}')
    if ACCESS not in text:
        end = text.rfind('\n}')
        if end < 0:
            raise ValueError('missing server closing brace')
        text = text[:end] + '\n    ' + ACCESS + '\n' + text[end:]
    return text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(protect(args.source.read_text(), FR_ENVS))
    print(f'Rendered auth gates for {len(FR_ENVS)} FR environments: {args.output}')


if __name__ == '__main__':
    main()
