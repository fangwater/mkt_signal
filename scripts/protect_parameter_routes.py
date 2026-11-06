#!/usr/bin/env python3
"""Render login gates for explicit MM config routes, preserving other locations."""
import argparse
from pathlib import Path
import re


def protect(text, environments):
    seen = set()
    def patch(match):
        block = match[0]
        path = re.match(r'location\s+(?:=\s+)?(/[^\s{]+)', block)
        if path and any(path[1] in (f'/mm/{env}/config', f'/mm/{env}/config/') for env in environments):
            seen.add(path[1])
            gate = 'include /etc/nginx/snippets/delist_parameter_gate.conf;'
            if gate not in block: block = block.replace('{', '{\n        '+gate, 1)
        return block
    result = re.sub(r'location\s+[^\n{]+\{[^{}]*\}', patch, text)
    expected = {f'/mm/{env}/config{suffix}' for env in environments for suffix in ('', '/')}
    if seen != expected: raise ValueError(f'missing MM config routes: {sorted(expected-seen)}')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--mm-env', action='append', required=True)
    args = parser.parse_args()
    if any(not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', env) for env in args.mm_env): parser.error('invalid environment')
    args.output.write_text(protect(args.source.read_text(), args.mm_env))
