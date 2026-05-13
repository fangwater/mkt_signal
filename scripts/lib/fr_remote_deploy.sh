# shellcheck shell=bash
# FR remote-deploy helpers, sourced by scripts/deploy_fr_{binance,gate,bitget}.sh.
#
# Workflow:
#   1) entry script invokes existing sub-scripts locally (without --apply-nginx),
#      passing --nginx-mapping-file "$FR_NGINX_STAGING" so nginx upserts land in
#      a per-deploy staging file rather than $HOME/nginx_locations.txt.
#   2) entry script calls fr_remote_sync_env_dir to rsync $HOME/$ENV_NAME →
#      $FR_DEPLOY_HOST:$FR_REMOTE_HOME/$ENV_NAME (excluding env.sh / data /
#      logs so live state stays intact).
#   3) entry script calls fr_remote_apply_nginx to push the merged mapping
#      file and reload nginx on the remote host.
#
# Target host is fixed (matches current single-target deployment); override via
# env vars only when migrating.

FR_DEPLOY_HOST="${FR_DEPLOY_HOST:-ubuntu@54.64.147.69}"
FR_REMOTE_HOME="${FR_REMOTE_HOME:-/home/ubuntu}"
FR_NGINX_PORT="${FR_NGINX_PORT:-4191}"
FR_DEPLOY_KEY="${FR_DEPLOY_KEY:-}"
FR_NGINX_STAGING=""

_fr_ssh_opts() {
  echo "-i $FR_DEPLOY_KEY -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15"
}

fr_remote_init() {
  local root_dir="$1"
  local env_name="$2"
  if [[ -z "$FR_DEPLOY_KEY" ]]; then
    FR_DEPLOY_KEY="$root_dir/aws-jp-srv-1.pem"
  fi
  if [[ ! -f "$FR_DEPLOY_KEY" ]]; then
    echo "[ERROR] missing ssh key: $FR_DEPLOY_KEY" >&2
    return 1
  fi
  chmod 400 "$FR_DEPLOY_KEY" 2>/dev/null || true

  FR_NGINX_STAGING="$(mktemp -t "fr_nginx_locations.${env_name}.XXXXXX")"
  # Best-effort cleanup; entry scripts run with set -e so this fires on exit.
  trap 'rm -f "$FR_NGINX_STAGING"' EXIT

  echo "[INFO] remote target : $FR_DEPLOY_HOST:$FR_REMOTE_HOME"
  echo "[INFO] ssh key       : $FR_DEPLOY_KEY"
  echo "[INFO] nginx staging : $FR_NGINX_STAGING"

  local opts
  opts="$(_fr_ssh_opts)"
  # shellcheck disable=SC2086
  if ! ssh $opts "$FR_DEPLOY_HOST" 'echo ok' >/dev/null 2>&1; then
    echo "[ERROR] ssh probe to $FR_DEPLOY_HOST failed" >&2
    return 1
  fi
}

fr_remote_fetch_nginx_mapping() {
  local root_dir="$1"
  local opts remote_path
  opts="$(_fr_ssh_opts)"
  remote_path="$FR_REMOTE_HOME/nginx_locations.txt"
  # shellcheck disable=SC2086
  if rsync -a -e "ssh $opts" "$FR_DEPLOY_HOST:$remote_path" "$FR_NGINX_STAGING" 2>/dev/null \
       && [[ -s "$FR_NGINX_STAGING" ]]; then
    echo "[INFO] fetched remote nginx_locations.txt (size=$(wc -c <"$FR_NGINX_STAGING"))"
    return 0
  fi
  if [[ -f "$root_dir/config/nginx_locations.txt" ]]; then
    cp "$root_dir/config/nginx_locations.txt" "$FR_NGINX_STAGING"
    echo "[INFO] remote nginx_locations.txt absent; initialized from $root_dir/config/nginx_locations.txt"
    return 0
  fi
  echo "[ERROR] remote nginx_locations.txt absent and no $root_dir/config/nginx_locations.txt template" >&2
  return 1
}

fr_remote_sync_env_dir() {
  local env_name="$1"
  local local_dir="$HOME/$env_name/"
  local remote_dir="$FR_REMOTE_HOME/$env_name/"
  local opts
  opts="$(_fr_ssh_opts)"

  if [[ ! -d "$local_dir" ]]; then
    echo "[ERROR] local env dir missing: $local_dir" >&2
    return 1
  fi

  echo "[INFO] rsync $local_dir -> $FR_DEPLOY_HOST:$remote_dir"
  # Excludes preserve remote-managed state: env.sh (API keys), data/ (RocksDB),
  # logs/pids, python caches.
  # shellcheck disable=SC2086
  rsync -a --human-readable --info=stats1 \
    --exclude='env.sh' \
    --exclude='data/' \
    --exclude='persist/' \
    --exclude='*.rocksdb/' \
    --exclude='logs/' \
    --exclude='*.log' \
    --exclude='*.pid' \
    --exclude='__pycache__/' \
    -e "ssh $opts" \
    "$local_dir" "$FR_DEPLOY_HOST:$remote_dir"
}

fr_remote_sync_binaries() {
  # --bin mode: push only top-level files (binaries) from $HOME/$ENV_NAME,
  # leaving config/ scripts/ www/ data/ untouched. The "*/" exclude drops all
  # sub-directories so this works for FR (account_monitor) and intra
  # (account_monitor_<exchange>) without per-pipeline binary lists.
  local env_name="$1"
  local local_dir="$HOME/$env_name/"
  local remote_dir="$FR_REMOTE_HOME/$env_name/"
  local opts
  opts="$(_fr_ssh_opts)"

  if [[ ! -d "$local_dir" ]]; then
    echo "[ERROR] local env dir missing: $local_dir" >&2
    return 1
  fi

  echo "[INFO] rsync top-level binaries $local_dir -> $FR_DEPLOY_HOST:$remote_dir"
  # shellcheck disable=SC2086
  rsync -a --human-readable --info=stats1 \
    --exclude='env.sh' \
    --exclude='*/' \
    --exclude='*.log' \
    --exclude='*.pid' \
    -e "ssh $opts" \
    "$local_dir" "$FR_DEPLOY_HOST:$remote_dir"
}

fr_remote_upsert_dashboard_env_sh() {
  # Idempotently inject the managed "fr_signal_dashboard" block into the
  # remote $HOME/<env-name>/env.sh. Required because fr_remote_sync_env_dir
  # excludes env.sh, so any local FR_DASHBOARD_* writes never reach the host.
  local env_name="$1" exchange="$2" bind="$3" port="$4" ws_path="$5"
  local opts remote_env_sh
  opts="$(_fr_ssh_opts)"
  remote_env_sh="$FR_REMOTE_HOME/$env_name/env.sh"

  echo "[INFO] upsert fr_signal_dashboard block in $FR_DEPLOY_HOST:$remote_env_sh (port=$port)"

  # shellcheck disable=SC2086
  ssh $opts "$FR_DEPLOY_HOST" \
    EXCHANGE="$exchange" BIND="$bind" PORT="$port" WS_PATH="$ws_path" \
    ENV_SH="$remote_env_sh" \
    'bash -s' <<'REMOTE_EOF'
set -euo pipefail
if [[ ! -f "$ENV_SH" ]]; then
  : > "$ENV_SH"
  chmod 600 "$ENV_SH"
fi
TMP="$(mktemp)"
awk -v begin="# BEGIN managed: fr_signal_dashboard" \
    -v end="# END managed: fr_signal_dashboard" \
    -v exchange="$EXCHANGE" -v bind="$BIND" -v port="$PORT" -v ws_path="$WS_PATH" '
  function emit() {
    print begin
    print "export FR_DASHBOARD_EXCHANGE=\"" exchange "\""
    print "export FR_DASHBOARD_BIND=\"" bind "\""
    print "export FR_DASHBOARD_PORT=\"" port "\""
    print "export FR_DASHBOARD_WS_PATH=\"" ws_path "\""
    print end
  }
  BEGIN { in_block = 0; replaced = 0 }
  $0 == begin { in_block = 1; replaced = 1; next }
  in_block && $0 == end { in_block = 0; emit(); next }
  in_block { next }
  { print }
  END { if (!replaced) { print ""; emit() } }
' "$ENV_SH" > "$TMP"
mv "$TMP" "$ENV_SH"
chmod 600 "$ENV_SH"
echo "[remote] env.sh upserted; FR_DASHBOARD_PORT now:"
grep -E "^export FR_DASHBOARD_" "$ENV_SH" || true
REMOTE_EOF
}

fr_remote_apply_nginx() {
  local env_name="$1"
  local opts mapping_to_push rewritten_mapping
  opts="$(_fr_ssh_opts)"

  if [[ ! -s "$FR_NGINX_STAGING" ]]; then
    echo "[WARN] nginx staging file empty/missing; skipping nginx apply"
    return 0
  fi

  mapping_to_push="$FR_NGINX_STAGING"
  rewritten_mapping=""
  if [[ "$HOME" != "$FR_REMOTE_HOME" ]]; then
    rewritten_mapping="$(mktemp -t "fr_nginx_locations.remote.${env_name}.XXXXXX")"
    awk -v from="static:${HOME}/" -v to="static:${FR_REMOTE_HOME}/" '
      {
        while ((pos = index($0, from)) > 0) {
          $0 = substr($0, 1, pos - 1) to substr($0, pos + length(from))
        }
        print
      }
    ' "$FR_NGINX_STAGING" > "$rewritten_mapping"
    mapping_to_push="$rewritten_mapping"
  fi

  echo "[INFO] push nginx mapping -> $FR_DEPLOY_HOST:$FR_REMOTE_HOME/nginx_locations.txt"
  # shellcheck disable=SC2086
  rsync -a -e "ssh $opts" "$mapping_to_push" "$FR_DEPLOY_HOST:$FR_REMOTE_HOME/nginx_locations.txt"
  if [[ -n "$rewritten_mapping" ]]; then
    rm -f "$rewritten_mapping"
  fi

  echo "[INFO] reload nginx on remote (PORT=$FR_NGINX_PORT)"
  # shellcheck disable=SC2086,SC2029
  ssh $opts "$FR_DEPLOY_HOST" \
    "PORT=$FR_NGINX_PORT MAPPING_FILE=$FR_REMOTE_HOME/nginx_locations.txt bash $FR_REMOTE_HOME/$env_name/scripts/setup_nginx_4191.sh"
}

# Lightweight ssh-only init: load FR_DEPLOY_KEY, chmod, and ssh probe.
# Used by callers (dat_pbs / spread_pbs) that don't need nginx staging.
fr_remote_init_ssh() {
  local root_dir="$1"
  if [[ -z "$FR_DEPLOY_KEY" ]]; then
    FR_DEPLOY_KEY="$root_dir/aws-jp-srv-1.pem"
  fi
  if [[ ! -f "$FR_DEPLOY_KEY" ]]; then
    echo "[ERROR] missing ssh key: $FR_DEPLOY_KEY" >&2
    return 1
  fi
  chmod 400 "$FR_DEPLOY_KEY" 2>/dev/null || true

  echo "[INFO] remote target : $FR_DEPLOY_HOST:$FR_REMOTE_HOME"
  echo "[INFO] ssh key       : $FR_DEPLOY_KEY"

  local opts
  opts="$(_fr_ssh_opts)"
  # shellcheck disable=SC2086
  if ! ssh $opts "$FR_DEPLOY_HOST" 'echo ok' >/dev/null 2>&1; then
    echo "[ERROR] ssh probe to $FR_DEPLOY_HOST failed" >&2
    return 1
  fi
}

# Rsync $HOME/<rel_path>/ -> $FR_DEPLOY_HOST:$FR_REMOTE_HOME/<rel_path>/.
# Same excludes as fr_remote_sync_env_dir (env.sh / data / *.rocksdb / logs /
# pids / __pycache__).
fr_remote_sync_path() {
  local rel_path="$1"
  local local_dir="$HOME/$rel_path/"
  local remote_dir="$FR_REMOTE_HOME/$rel_path/"
  local opts
  opts="$(_fr_ssh_opts)"

  if [[ ! -d "$local_dir" ]]; then
    echo "[ERROR] local dir missing: $local_dir" >&2
    return 1
  fi

  echo "[INFO] rsync $local_dir -> $FR_DEPLOY_HOST:$remote_dir"
  # shellcheck disable=SC2086
  rsync -a --human-readable --info=stats1 \
    --exclude='env.sh' \
    --exclude='data/' \
    --exclude='persist/' \
    --exclude='*.rocksdb/' \
    --exclude='logs/' \
    --exclude='*.log' \
    --exclude='*.pid' \
    --exclude='__pycache__/' \
    -e "ssh $opts" \
    "$local_dir" "$FR_DEPLOY_HOST:$remote_dir"
}
