#!/usr/bin/env python3
"""Create a private delist login config. Passwords never enter command arguments."""
import argparse
import getpass
import hashlib
import json
import os
from pathlib import Path
import secrets
import sys
import tempfile
from urllib.parse import urlsplit


FR_ENVS = [f'{venue}_fr_arb{instance:02}' for venue, instances in
           [('binance', [2, 3, 4]), ('gate', [1, 2, 3]), ('bitget', [1, 2, 3]), ('bybit', [1])]
           for instance in instances]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--username', required=True)
    parser.add_argument('--public-origin', required=True)
    parser.add_argument('--fr-env', action='append', dest='fr_envs')
    parser.add_argument('--output', type=Path, default=Path('config/delist_access.json'))
    parser.add_argument('--password-stdin', action='store_true')
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()
    origin = urlsplit(args.public_origin)
    if origin.scheme not in ('http', 'https') or not origin.hostname or origin.path or origin.query or origin.fragment or origin.username:
        parser.error('--public-origin must be an HTTP origin without a path')
    if args.output.exists() and not args.force:
        parser.error('config already exists; use --force to replace it')
    password = sys.stdin.readline().rstrip('\n') if args.password_stdin else getpass.getpass('Password: ')
    if len(password) < 8:
        parser.error('password must contain at least 8 characters')
    salt = secrets.token_bytes(32)
    config = {'username': args.username, 'public_origin': args.public_origin,
              'password_iterations': 600000, 'password_salt': salt.hex(),
              'password_hash': hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 600000).hex(),
              'fr_envs': args.fr_envs or FR_ENVS}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix='.delist-access-', dir=args.output.parent)
    try:
        with os.fdopen(fd, 'w') as output:
            json.dump(config, output, indent=2)
            output.write('\n')
            output.flush()
            os.fsync(output.fileno())
        os.replace(tmp, args.output)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    print(f'Wrote private login config for {len(config["fr_envs"])} FR environments: {args.output}')


if __name__ == '__main__':
    main()
