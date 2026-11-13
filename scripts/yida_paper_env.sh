#!/usr/bin/env bash
set +x
set -euo pipefail
umask 077
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BASE_DIR"
if [[ -f env.sh ]]; then
    # Private environment-owned credentials; never enable shell tracing.
    source env.sh
fi
exec /usr/bin/python3 "$BASE_DIR/scripts/yida_paper_env.py" "$@"
