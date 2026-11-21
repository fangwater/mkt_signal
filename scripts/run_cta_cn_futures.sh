#!/usr/bin/env bash
# PM2 entry for the SDK-free, network-fed target worker.
set +x
set -euo pipefail
umask 077
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BASE_DIR"
[[ ! -f env.sh ]] || source env.sh
exec ./cta-cn-futures --config config/cta_cn_futures.json
