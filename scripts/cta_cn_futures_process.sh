#!/usr/bin/env bash
# Copy this wrapper and its Python helper into the native CTA environment.
set +x
set -euo pipefail
umask 077
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BASE_DIR"
[[ ! -f env.sh ]] || source env.sh
exec /usr/bin/python3 "$BASE_DIR/scripts/cta_cn_futures_process.py" "$@"
