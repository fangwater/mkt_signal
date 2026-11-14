#!/usr/bin/env bash
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ ! -f "$BASE_DIR/env.sh" ]] || source "$BASE_DIR/env.sh"
export PM2_HOME="${CTA_REPLAY_PM2_HOME:-$BASE_DIR/run/pm2}"
"${PM2_BIN:-pm2}" list
curl --noproxy '*' --silent --show-error "http://${CTA_REPLAY_BIND:-127.0.0.1}:${CTA_REPLAY_PORT:-10242}/api/healthz"
echo
