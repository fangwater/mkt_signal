#!/usr/bin/env bash

INTRA_RELEASE_MANIFEST_NAME="intra-release.manifest"

intra_release_manifest_path() {
  local base_dir="${1:-}"
  printf '%s/%s\n' "$base_dir" "$INTRA_RELEASE_MANIFEST_NAME"
}

intra_release_expected_hash() {
  local base_dir="${1:-}"
  local binary_name="${2:-}"
  local manifest=""
  local hash=""

  manifest="$(intra_release_manifest_path "$base_dir")"
  if [[ ! -f "$manifest" ]]; then
    echo "[ERROR] Intra release manifest not found: $manifest" >&2
    return 1
  fi
  hash="$(awk -v name="$binary_name" '$1 == "binary" && $2 == name { print $3 }' "$manifest")"
  if [[ ! "$hash" =~ ^[0-9a-f]{64}$ ]]; then
    echo "[ERROR] invalid or missing release hash for binary=$binary_name manifest=$manifest" >&2
    return 1
  fi
  printf '%s\n' "$hash"
}

intra_release_id() {
  local base_dir="${1:-}"
  local manifest=""
  local release_id=""

  manifest="$(intra_release_manifest_path "$base_dir")"
  if [[ ! -f "$manifest" ]]; then
    echo "[ERROR] Intra release manifest not found: $manifest" >&2
    return 1
  fi
  release_id="$(awk '$1 == "release_id" { print $2 }' "$manifest")"
  if [[ ! "$release_id" =~ ^[0-9a-f]{64}$ ]]; then
    echo "[ERROR] invalid or missing release_id in $manifest" >&2
    return 1
  fi
  printf '%s\n' "$release_id"
}

intra_release_verify_file() {
  local base_dir="${1:-}"
  local binary_name="${2:-}"
  local binary_path="${3:-}"
  local expected=""
  local actual=""

  expected="$(intra_release_expected_hash "$base_dir" "$binary_name")" || return 1
  if [[ ! -x "$binary_path" ]]; then
    echo "[ERROR] release binary not found or not executable: $binary_path" >&2
    return 1
  fi
  actual="$(sha256sum "$binary_path" | awk '{print $1}')"
  if [[ "$actual" != "$expected" ]]; then
    echo "[ERROR] Intra release mismatch: binary=$binary_name path=$binary_path expected=$expected actual=$actual" >&2
    return 1
  fi
}

intra_release_verify_running_file() {
  local base_dir="${1:-}"
  local binary_name="${2:-}"
  local binary_path="${3:-}"
  local required="${4:-1}"
  local expected=""
  local pid=""
  local exe=""
  local actual=""
  local -a pids=()

  expected="$(intra_release_expected_hash "$base_dir" "$binary_name")" || return 1
  while read -r pid; do
    [[ -n "$pid" ]] || continue
    exe="$(readlink "/proc/$pid/exe" 2>/dev/null || true)"
    exe="${exe% (deleted)}"
    if [[ "$exe" == "$binary_path" ]]; then
      pids+=("$pid")
    fi
  done < <(ps -eo pid=)

  if [[ "${#pids[@]}" -eq 0 && "$required" == "0" ]]; then
    return 0
  fi
  if [[ "${#pids[@]}" -ne 1 ]]; then
    echo "[ERROR] expected exactly one running $binary_name at $binary_path; pids=${pids[*]:-none}" >&2
    return 1
  fi
  actual="$(sha256sum "/proc/${pids[0]}/exe" | awk '{print $1}')"
  if [[ "$actual" != "$expected" ]]; then
    echo "[ERROR] running Intra release mismatch: binary=$binary_name pid=${pids[0]} expected=$expected actual=$actual" >&2
    return 1
  fi
}

intra_release_account_monitor_name() {
  local exchange="${1:-}"
  local exec_backend="${2:-native}"
  if [[ "$exec_backend" == "ltp" ]]; then
    printf '%s\n' rapidx_account_monitor
  else
    printf '%s_account_monitor\n' "$exchange"
  fi
}
