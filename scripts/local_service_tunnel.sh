#!/usr/bin/env bash
set +x
set -euo pipefail
umask 077
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BASE_DIR"
exec /usr/bin/python3 "$BASE_DIR/scripts/local_service_tunnel.py" "$@"
