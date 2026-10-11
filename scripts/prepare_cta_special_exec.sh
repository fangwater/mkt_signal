#!/usr/bin/env bash
# Prepare the standard Exec working directory; never starts a process or submits orders.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_NAME="$(basename "$BASE_DIR")"
[[ "$ENV_NAME" =~ ^binance-cta-special-[a-z0-9][a-z0-9_-]*$ ]] || { echo '[ERROR] run the environment-local script'; exit 1; }
EXEC_DIR="$(dirname "$BASE_DIR")/${ENV_NAME/binance-cta-special-/binance-exec-cta-special-}"
[[ $# -le 1 && "${1:-}" == "${1:+--execute}" ]] || { echo 'Usage: prepare_cta_special_exec.sh [--execute]'; exit 1; }
echo "[PLAN] env=$EXEC_DIR exchange=binance-futures scope=independent-factor-strategies mutation=prepare-files-only"
[[ "${1:-}" == '--execute' ]] || { echo '[DRY-RUN] no files changed'; exit 0; }
[[ -f "$BASE_DIR/env.sh" && -x "$BASE_DIR/exec-pre-trade" ]] || { echo '[ERROR] missing parent env or locally published Exec artifact'; exit 1; }
mkdir -p "$EXEC_DIR/scripts" "$EXEC_DIR/config" "$EXEC_DIR/run"
if [[ ! -f "$EXEC_DIR/env.sh" ]]; then
  printf '#!/usr/bin/env bash\nsource %q\nexport EXEC_VENUE=binance-futures\n' "$BASE_DIR/env.sh" > "$EXEC_DIR/env.sh"
  chmod 600 "$EXEC_DIR/env.sh"
fi
install -m 755 "$BASE_DIR/exec-pre-trade" "$EXEC_DIR/exec-pre-trade.tmp"
mv -f "$EXEC_DIR/exec-pre-trade.tmp" "$EXEC_DIR/exec-pre-trade"
for name in start_exec_pre_trade.sh stop_exec_pre_trade.sh process_match_lib.sh sync_exec_risk_params.py; do
  install -m 755 "$BASE_DIR/scripts/$name" "$EXEC_DIR/scripts/$name"
done
[[ -f "$EXEC_DIR/config/iceoryx2.toml" ]] || install -m 644 "$BASE_DIR/config/iceoryx2.toml" "$EXEC_DIR/config/iceoryx2.toml"
[[ -e "$EXEC_DIR/trade_engine.toml" ]] || ln -s "$BASE_DIR/trade_engine.toml" "$EXEC_DIR/trade_engine.toml"
echo "[INFO] standard Exec prepared and remains stopped: $EXEC_DIR"
