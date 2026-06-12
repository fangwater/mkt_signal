#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCAL_BIN="${LOCAL_BIN:-${ROOT_DIR}/target/release/order_export}"
OUT_ROOT="${OUT_ROOT:-${ROOT_DIR}/data/intra_order_export_backfill}"
START_TS="${START_TS:-1970-01-01T00:00:00Z}"
END_TS="${END_TS:-}"
REMOTE_TMP_ROOT="${REMOTE_TMP_ROOT:-/tmp/mkt_signal_order_export_backfill}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
SSH_CONNECT_TIMEOUT="${SSH_CONNECT_TIMEOUT:-15}"
SKIP_BUILD=0
KEEP_REMOTE=0
FETCH_FILES="${FETCH_FILES:-uniform_orders,order_updates_unmatched,trade_updates_unmatched}"
SKIP_EXISTING=0
SOURCES=()

usage() {
  cat <<'EOF'
Usage:
  scripts/fetch_intra_uniform_orders_backfill.sh [options] [source...]

Sources:
  okex-intra-arb01
  binance-intra-arb01
  gate-intra-arb01
  bitget-intra-arb01
  bitget-gate-cross-arb01
  bybit-intra-arb01

Description:
  Builds the latest order_export binary locally, runs it for each selected
  source, and fetches selected parquet files back to this machine. Local
  sources run without SSH; remote sources copy the binary and run read-only
  against the remote persist_manager DB.

  Bad-format uniform order records are dropped by order_export's parquet
  decoder. The currently running persist sync/read services are not touched.

Output:
  <out-root>/<run-id>/<source-id>/uniform_orders.parquet
  <out-root>/<run-id>/<source-id>/order_updates_unmatched.parquet
  <out-root>/<run-id>/<source-id>/trade_updates_unmatched.parquet

Options:
  --start <utc-ts>      UTC start, default 1970-01-01T00:00:00Z.
  --end <utc-ts>        UTC end, default current UTC when each remote runs.
  --out-root <path>     Local output root, default data/intra_order_export_backfill.
  --remote-tmp <path>   Remote tmp root, default /tmp/mkt_signal_order_export_backfill.
  --run-id <id>         Output run id, default current UTC timestamp.
  --files <names>       Comma-separated table names, default all 3 order export tables.
                        Allowed: uniform_orders,order_updates_unmatched,trade_updates_unmatched.
  --skip-existing       Do not fetch a file that already exists locally.
  --skip-build          Reuse target/release/order_export.
  --keep-remote         Do not remove remote tmp export dirs after fetching.
  -h, --help            Show this help.

Per-source SSH overrides:
  OKEX_SSH_TARGET, OKEX_SSH_KEY, OKEX_REMOTE_HOME
  BINANCE_SSH_TARGET, BINANCE_SSH_KEY, BINANCE_REMOTE_HOME
  GATE_SSH_TARGET, GATE_SSH_KEY, GATE_REMOTE_HOME
  BITGET_SSH_TARGET, BITGET_SSH_KEY, BITGET_REMOTE_HOME
  BITGET_GATE_CROSS_SSH_TARGET, BITGET_GATE_CROSS_SSH_KEY, BITGET_GATE_CROSS_REMOTE_HOME
  BYBIT_SSH_TARGET, BYBIT_SSH_KEY, BYBIT_REMOTE_HOME

Defaults:
  binance:        local, home /home/ubuntu
  gate:           local, home /home/ubuntu
  bitget:         local, home /home/ubuntu
  bitget-gate:    local, home /home/ubuntu
  okex:           fanghaizhou@47.238.128.48, key from OKEX_SSH_KEY, home /home/fanghaizhou
  bybit:          ubuntu@47.131.162.78, key aws-sg.pem, home /home/ubuntu
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start)
      START_TS="${2:-}"; shift 2 ;;
    --start=*)
      START_TS="${1#--start=}"; shift ;;
    --end)
      END_TS="${2:-}"; shift 2 ;;
    --end=*)
      END_TS="${1#--end=}"; shift ;;
    --out-root)
      OUT_ROOT="${2:-}"; shift 2 ;;
    --out-root=*)
      OUT_ROOT="${1#--out-root=}"; shift ;;
    --remote-tmp)
      REMOTE_TMP_ROOT="${2:-}"; shift 2 ;;
    --remote-tmp=*)
      REMOTE_TMP_ROOT="${1#--remote-tmp=}"; shift ;;
    --run-id)
      RUN_ID="${2:-}"; shift 2 ;;
    --run-id=*)
      RUN_ID="${1#--run-id=}"; shift ;;
    --files)
      FETCH_FILES="${2:-}"; shift 2 ;;
    --files=*)
      FETCH_FILES="${1#--files=}"; shift ;;
    --skip-existing)
      SKIP_EXISTING=1; shift ;;
    --skip-build)
      SKIP_BUILD=1; shift ;;
    --keep-remote)
      KEEP_REMOTE=1; shift ;;
    -h|--help)
      usage; exit 0 ;;
    --)
      shift
      while [[ $# -gt 0 ]]; do SOURCES+=("$1"); shift; done ;;
    -*)
      echo "[ERROR] unknown option: $1" >&2
      usage >&2
      exit 1 ;;
    *)
      SOURCES+=("$1"); shift ;;
  esac
done

if [[ ${#SOURCES[@]} -eq 0 ]]; then
  SOURCES=(binance-intra-arb01 gate-intra-arb01 bitget-intra-arb01 bitget-gate-cross-arb01)
fi

if [[ -z "$START_TS" ]]; then
  echo "[ERROR] --start cannot be empty" >&2
  exit 1
fi

if [[ -z "$RUN_ID" ]]; then
  echo "[ERROR] --run-id cannot be empty" >&2
  exit 1
fi

IFS=',' read -r -a REQUESTED_TABLES <<< "$FETCH_FILES"
if [[ ${#REQUESTED_TABLES[@]} -eq 0 ]]; then
  echo "[ERROR] --files cannot be empty" >&2
  exit 1
fi

parquet_name_for_table() {
  local table="$1"
  case "$table" in
    uniform_orders|order_updates_unmatched|trade_updates_unmatched)
      echo "${table}.parquet" ;;
    *)
      echo "[ERROR] unsupported parquet table: $table" >&2
      return 1 ;;
  esac
}

for table in "${REQUESTED_TABLES[@]}"; do
  parquet_name_for_table "$table" >/dev/null
done

source_target() {
  local source="$1"
  case "$source" in
    okex-intra-arb01)
      echo "${OKEX_SSH_TARGET:-fanghaizhou@47.238.128.48}" ;;
    binance-intra-arb01)
      echo "${BINANCE_SSH_TARGET:-local}" ;;
    gate-intra-arb01)
      echo "${GATE_SSH_TARGET:-local}" ;;
    bitget-intra-arb01)
      echo "${BITGET_SSH_TARGET:-local}" ;;
    bitget-gate-cross-arb01)
      echo "${BITGET_GATE_CROSS_SSH_TARGET:-local}" ;;
    bybit-intra-arb01)
      echo "${BYBIT_SSH_TARGET:-ubuntu@47.131.162.78}" ;;
    *)
      echo "[ERROR] unsupported source: $source" >&2
      return 1 ;;
  esac
}

source_key() {
  local source="$1"
  case "$source" in
    okex-intra-arb01)
      echo "${OKEX_SSH_KEY:-}" ;;
    binance-intra-arb01)
      echo "${BINANCE_SSH_KEY:-}" ;;
    gate-intra-arb01)
      echo "${GATE_SSH_KEY:-}" ;;
    bitget-intra-arb01)
      echo "${BITGET_SSH_KEY:-}" ;;
    bitget-gate-cross-arb01)
      echo "${BITGET_GATE_CROSS_SSH_KEY:-}" ;;
    bybit-intra-arb01)
      echo "${BYBIT_SSH_KEY:-${ROOT_DIR}/aws-sg.pem}" ;;
    *)
      echo "[ERROR] unsupported source: $source" >&2
      return 1 ;;
  esac
}

source_home() {
  local source="$1"
  case "$source" in
    okex-intra-arb01)
      echo "${OKEX_REMOTE_HOME:-/home/fanghaizhou}" ;;
    binance-intra-arb01)
      echo "${BINANCE_REMOTE_HOME:-/home/ubuntu}" ;;
    gate-intra-arb01)
      echo "${GATE_REMOTE_HOME:-/home/ubuntu}" ;;
    bitget-intra-arb01)
      echo "${BITGET_REMOTE_HOME:-/home/ubuntu}" ;;
    bitget-gate-cross-arb01)
      echo "${BITGET_GATE_CROSS_REMOTE_HOME:-/home/ubuntu}" ;;
    bybit-intra-arb01)
      echo "${BYBIT_REMOTE_HOME:-/home/ubuntu}" ;;
    *)
      echo "[ERROR] unsupported source: $source" >&2
      return 1 ;;
  esac
}

require_file() {
  local path="$1"
  local label="$2"
  if [[ ! -f "$path" ]]; then
    echo "[ERROR] missing ${label}: ${path}" >&2
    exit 1
  fi
}

ssh_base_opts() {
  local key="$1"
  printf '%s\n' \
    -i "$key" \
    -o StrictHostKeyChecking=accept-new \
    -o ConnectTimeout="${SSH_CONNECT_TIMEOUT}"
}

remote_quote() {
  printf "'%s'" "${1//\'/\'\\\'\'}"
}

is_local_target() {
  case "$1" in
    ""|local|localhost|127.0.0.1|ubuntu@localhost|ubuntu@127.0.0.1)
      return 0 ;;
    *)
      return 1 ;;
  esac
}

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  echo "[INFO] building latest order_export"
  cargo build --release -p persist_manager --features runtime --bin order_export
else
  echo "[INFO] --skip-build: reusing ${LOCAL_BIN}"
fi

require_file "$LOCAL_BIN" "order_export binary"
mkdir -p "$OUT_ROOT/$RUN_ID"

echo "[INFO] output root: ${OUT_ROOT}/${RUN_ID}"
echo "[INFO] window: ${START_TS} .. ${END_TS:-remote-now}"
echo "[INFO] files: ${FETCH_FILES}"

failed=0
for source in "${SOURCES[@]}"; do
  target="$(source_target "$source")" || exit 1
  key="$(source_key "$source")" || exit 1
  remote_home="$(source_home "$source")" || exit 1

  remote_env_dir="${remote_home}/${source}"
  remote_run_dir="${REMOTE_TMP_ROOT}/${RUN_ID}/${source}"
  remote_bin="${remote_run_dir}/order_export"
  remote_out_root="${remote_run_dir}/out"

  if is_local_target "$target"; then
    echo "[INFO] [${source}] target=local env=${remote_env_dir}"
    if [[ ! -d "$remote_env_dir" ]]; then
      echo "[ERROR] [${source}] local env dir not found: ${remote_env_dir}" >&2
      failed=$((failed + 1))
      continue
    fi

    mkdir -p "$remote_run_dir" "$remote_out_root"
    cp "$LOCAL_BIN" "$remote_bin"
    chmod +x "$remote_bin"

    local_end_ts="$END_TS"
    if [[ -z "$local_end_ts" ]]; then
      local_end_ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    fi

    echo "[INFO] [${source}] local export started"
    if ! (
      cd "$remote_env_dir"
      "$remote_bin" \
        --base-dir "$remote_home" \
        --env-name "$source" \
        --start "$START_TS" \
        --end "$local_end_ts" \
        --output-root "$remote_out_root"
    ); then
      echo "[ERROR] [${source}] local export failed" >&2
      failed=$((failed + 1))
      continue
    fi

    source_failed=0
    for table in "${REQUESTED_TABLES[@]}"; do
      parquet_name="$(parquet_name_for_table "$table")" || exit 1
      local_source_dir="${OUT_ROOT}/${RUN_ID}/${source}"
      mkdir -p "$local_source_dir"
      local_parquet="${local_source_dir}/${parquet_name}"
      if [[ "$SKIP_EXISTING" -eq 1 && -s "$local_parquet" ]]; then
        echo "[INFO] [${source}] skip existing ${local_parquet}"
        continue
      fi

      remote_parquet="$(find "$remote_out_root" -mindepth 2 -maxdepth 2 -name "$parquet_name" -type f | sort | tail -n 1)"
      if [[ -z "$remote_parquet" ]]; then
        echo "[ERROR] [${source}] local ${parquet_name} not found" >&2
        source_failed=1
        continue
      fi

      echo "[INFO] [${source}] copying ${remote_parquet}"
      if ! cp "$remote_parquet" "$local_parquet"; then
        echo "[ERROR] [${source}] failed to copy ${parquet_name}" >&2
        source_failed=1
        continue
      fi

      if [[ ! -s "$local_parquet" ]]; then
        echo "[ERROR] [${source}] fetched parquet is empty: ${local_parquet}" >&2
        source_failed=1
        continue
      fi

      size_bytes="$(wc -c < "$local_parquet" | tr -d ' ')"
      echo "[INFO] [${source}] done ${local_parquet} (${size_bytes} bytes)"
    done

    if [[ "$source_failed" -ne 0 ]]; then
      failed=$((failed + 1))
    fi

    if [[ "$KEEP_REMOTE" -eq 0 ]]; then
      rm -rf "$remote_run_dir"
    fi
    continue
  fi

  if [[ -z "$key" ]]; then
    echo "[ERROR] [${source}] SSH key is required for remote target ${target}" >&2
    failed=$((failed + 1))
    continue
  fi
  require_file "$key" "ssh key for ${source}"
  chmod 400 "$key" 2>/dev/null || true

  ssh_opts=()
  while IFS= read -r opt; do ssh_opts+=("$opt"); done < <(ssh_base_opts "$key")

  echo "[INFO] [${source}] target=${target} env=${remote_env_dir}"
  if ! ssh "${ssh_opts[@]}" "$target" "mkdir -p $(remote_quote "$remote_run_dir") $(remote_quote "$remote_out_root")"; then
    echo "[ERROR] [${source}] failed to create remote tmp dir" >&2
    failed=$((failed + 1))
    continue
  fi

  if ! scp "${ssh_opts[@]}" "$LOCAL_BIN" "${target}:${remote_bin}"; then
    echo "[ERROR] [${source}] failed to copy order_export" >&2
    failed=$((failed + 1))
    continue
  fi

  export_args=(
    "$(remote_quote "$remote_bin")"
    "--base-dir" "$(remote_quote "$remote_home")"
    "--env-name" "$(remote_quote "$source")"
    "--start" "$(remote_quote "$START_TS")"
  )
  if [[ -n "$END_TS" ]]; then
    export_args+=("--end" "$(remote_quote "$END_TS")")
  else
    export_args+=("--end" '"$(date -u +%Y-%m-%dT%H:%M:%SZ)"')
  fi
  export_args+=("--output-root" "$(remote_quote "$remote_out_root")")

  remote_script="set -eu
chmod +x $(remote_quote "$remote_bin")
cd $(remote_quote "$remote_env_dir")
$(IFS=' '; echo "${export_args[*]}")"
  echo "[INFO] [${source}] remote export started"
  if ! ssh "${ssh_opts[@]}" "$target" "$remote_script"; then
    echo "[ERROR] [${source}] remote export failed" >&2
    failed=$((failed + 1))
    continue
  fi

  source_failed=0
  for table in "${REQUESTED_TABLES[@]}"; do
    parquet_name="$(parquet_name_for_table "$table")" || exit 1
    local_source_dir="${OUT_ROOT}/${RUN_ID}/${source}"
    mkdir -p "$local_source_dir"
    local_parquet="${local_source_dir}/${parquet_name}"
    if [[ "$SKIP_EXISTING" -eq 1 && -s "$local_parquet" ]]; then
      echo "[INFO] [${source}] skip existing ${local_parquet}"
      continue
    fi

    remote_parquet="$(
      ssh "${ssh_opts[@]}" "$target" \
        "find $(remote_quote "$remote_out_root") -mindepth 2 -maxdepth 2 -name $(remote_quote "$parquet_name") -type f | sort | tail -n 1"
    )"
    if [[ -z "$remote_parquet" ]]; then
      echo "[ERROR] [${source}] remote ${parquet_name} not found" >&2
      source_failed=1
      continue
    fi

    echo "[INFO] [${source}] fetching ${remote_parquet}"
    if ! scp "${ssh_opts[@]}" "${target}:${remote_parquet}" "$local_parquet"; then
      echo "[ERROR] [${source}] failed to fetch ${parquet_name}" >&2
      source_failed=1
      continue
    fi

    if [[ ! -s "$local_parquet" ]]; then
      echo "[ERROR] [${source}] fetched parquet is empty: ${local_parquet}" >&2
      source_failed=1
      continue
    fi

    size_bytes="$(wc -c < "$local_parquet" | tr -d ' ')"
    echo "[INFO] [${source}] done ${local_parquet} (${size_bytes} bytes)"
  done

  if [[ "$source_failed" -ne 0 ]]; then
    failed=$((failed + 1))
  fi

  if [[ "$KEEP_REMOTE" -eq 0 ]]; then
    ssh "${ssh_opts[@]}" "$target" "rm -rf $(remote_quote "$remote_run_dir")" >/dev/null 2>&1 || true
  fi
done

echo "[INFO] complete: ${OUT_ROOT}/${RUN_ID}"
if [[ "$failed" -gt 0 ]]; then
  echo "[ERROR] ${failed} source(s) failed" >&2
  exit 1
fi
