#!/usr/bin/env bash
# Install a signal-only suite. Never starts processes or creates an Exec companion.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FAMILY="" ENV_NAME="" ARTIFACT_DIR="" SKIP_BUILD=0 EXECUTE=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --family) FAMILY="$2"; shift 2 ;;
    --env-name) ENV_NAME="$2"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --artifact-dir) ARTIFACT_DIR="$2"; shift 2 ;;
    --execute) EXECUTE=1; shift ;;
    *) echo 'Usage: deploy_cta_signal.sh --family perp|spread --env-name binance-cta-<family>-<tag> [--skip-build --artifact-dir <release-dir>] [--execute]' >&2; exit 1 ;;
  esac
done
[[ "$FAMILY" == perp || "$FAMILY" == spread ]] || { echo '[ERROR] invalid CTA family' >&2; exit 1; }
[[ "$ENV_NAME" =~ ^binance-cta-${FAMILY}-[a-z0-9][a-z0-9_-]*$ ]] || { echo '[ERROR] invalid environment name' >&2; exit 1; }
TARGET_DIR="${HOME}/${ENV_NAME}"
echo "[PLAN] env=$TARGET_DIR exchange=binance symbol_scope=config/cta_${FAMILY}_symbol_factors.json mutation=install-signal-files-only"
[[ "$EXECUTE" == 1 ]] || { echo '[DRY-RUN] pass --execute to install; processes remain stopped'; exit 0; }
cd "$ROOT_DIR"
[[ "$(git branch --show-current)" == arbmm && -z "$(git status --porcelain)" ]] || { echo '[ERROR] require a clean arbmm worktree' >&2; exit 1; }
git fetch origin arbmm
[[ "$(git rev-parse HEAD)" == "$(git rev-parse origin/arbmm)" ]] || { echo '[ERROR] synchronize origin/arbmm first' >&2; exit 1; }
if [[ "$SKIP_BUILD" != 1 ]]; then
  [[ "$ROOT_DIR" == /home/fanghaizhou/mkt_signal ]] || { echo '[ERROR] build only in the maintained local worktree' >&2; exit 1; }
  cargo build --release --bin "cta_${FAMILY}_signal" --bin "cta_${FAMILY}_factor_model_1m_pub"
fi
source "$ROOT_DIR/scripts/deploy_intra_lib.sh"
if [[ "$SKIP_BUILD" == 1 ]]; then
  [[ -n "$ARTIFACT_DIR" ]] || { echo '[ERROR] --skip-build requires --artifact-dir containing uploaded local release artifacts' >&2; exit 1; }
  RELEASE_DIR="$ARTIFACT_DIR"
else
  RELEASE_DIR="$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')/release"
fi
[[ -d "$RELEASE_DIR" ]] || { echo '[ERROR] missing release directory' >&2; exit 1; }
for name in "cta_${FAMILY}_signal" "cta_${FAMILY}_factor_model_1m_pub"; do
  [[ -x "$RELEASE_DIR/$name" ]] || { echo "[ERROR] missing artifact $name" >&2; exit 1; }
done
if [[ -f "$TARGET_DIR/config/cta_${FAMILY}.json" ]]; then
  python3 "scripts/cta_${FAMILY}_config_server.py" --config "$TARGET_DIR/config/cta_${FAMILY}.json" --check
fi
mkdir -p "$TARGET_DIR" "$TARGET_DIR/scripts" "$TARGET_DIR/config" "$TARGET_DIR/run" "$TARGET_DIR/web"
for name in "cta_${FAMILY}_signal" "cta_${FAMILY}_factor_model_1m_pub"; do
  intra_atomic_install "$RELEASE_DIR/$name" "$TARGET_DIR/$name"
  [[ "$(sha256sum "$RELEASE_DIR/$name" | cut -d' ' -f1)" == "$(sha256sum "$TARGET_DIR/$name" | cut -d' ' -f1)" ]] || { echo "[ERROR] artifact checksum mismatch: $name" >&2; exit 1; }
done
for file in scripts/start_"cta_${FAMILY}"*.sh scripts/stop_"cta_${FAMILY}"*.sh scripts/"cta_${FAMILY}"*.py scripts/process_match_lib.sh scripts/factor_validation_allowlist.txt; do
  install -m 755 "$file" "$TARGET_DIR/scripts/$(basename "$file")"
done
for suffix in .json _symbol_factors.json _factor_model_1m_pub.toml; do
  file="config/cta_${FAMILY}${suffix}"
  [[ -f "$TARGET_DIR/$file" ]] || install -m 644 "$file" "$TARGET_DIR/$file"
done
[[ -f "$TARGET_DIR/config/iceoryx2.toml" ]] || install -m 644 config/iceoryx2.toml "$TARGET_DIR/config/iceoryx2.toml"
for directory in web/"cta_${FAMILY}"*; do
  mkdir -p "$TARGET_DIR/$directory"
  cp -a "$directory/." "$TARGET_DIR/$directory/"
done
if [[ "$FAMILY" == perp ]]; then
  mkdir -p "$TARGET_DIR/web/vendor"
  cp -a web/vendor/. "$TARGET_DIR/web/vendor/"
fi
if [[ ! -f "$TARGET_DIR/env.sh" ]]; then
  umask 077
  cat > "$TARGET_DIR/env.sh" <<ENV
export IPC_NAMESPACE='$ENV_NAME'
# Configure the host's Manager gateway before enabling position publication.
# export CTA_MANAGER_API_URL='http://127.0.0.1:4191/manager/api'
export CTA_POSITION_STRATEGY_PREFIX='${ENV_NAME//-/_}_'
export CTA_VIRTUAL_ACCOUNT_NAME='cta_${FAMILY}'
ENV
fi
echo "[INFO] installed CTA $FAMILY files; existing env/config/state retained; no process started"
