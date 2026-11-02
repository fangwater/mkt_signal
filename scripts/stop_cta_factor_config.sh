#!/usr/bin/env bash
set -euo pipefail
kind=perp
if [[ "${1:-}" == --spread ]]; then kind=spread; shift; fi
exec "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/cta_factor_process.sh" "$kind" stop --config-service "$@"
