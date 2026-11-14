#!/usr/bin/env bash
# Prepare the RX01 15m operator configuration without starting services.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/deploy_cta_signal.sh" --family perp --bar-interval 15m \
  --config-dir "$SCRIPT_DIR/../config/cta_perp_rx01" --env-name binance-cta-perp-rx01 "$@"
