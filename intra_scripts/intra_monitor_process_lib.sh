#!/usr/bin/env bash

if ! type safe_find_running_pids >/dev/null 2>&1; then
  safe_find_running_pids() {
    local target_comm="${1:-}"
    shift || true

    if [[ -z "$target_comm" ]]; then
      return 0
    fi

    local pids=()
    while IFS=$'\t' read -r pid comm args; do
      if [[ -z "$pid" || -z "$comm" ]]; then
        continue
      fi
      if [[ "$pid" == "$$" || "$pid" == "$PPID" ]]; then
        continue
      fi
      local argv0="${args%% *}"
      local argv0_base="${argv0##*/}"
      if [[ "$comm" != "$target_comm" && "$argv0_base" != "$target_comm" ]]; then
        continue
      fi

      local matched=true
      local needle
      for needle in "$@"; do
        if [[ -n "$needle" && "$args" != *"$needle"* ]]; then
          matched=false
          break
        fi
      done

      if [[ "$matched" == true ]]; then
        pids+=("$pid")
      fi
    done < <(
      ps -eo pid=,comm=,args= | awk '
        {
          pid = $1;
          comm = $2;
          $1 = "";
          $2 = "";
          sub(/^  */, "", $0);
          print pid "\t" comm "\t" $0;
        }
      '
    )

    if [[ ${#pids[@]} -gt 0 ]]; then
      printf '%s\n' "${pids[@]}"
    fi
  }
fi

intra_find_running_account_monitor_pids() {
  local base_dir="${1:-}"
  local exchange="${2:-}"
  local comm
  local -a matched=()
  local -a all_pids=()

  for comm in "account_monitor" "account_monitor_${exchange}" "${exchange}_account_monitor"; do
    mapfile -t matched < <(safe_find_running_pids "$comm" "$base_dir" || true)
    if [[ ${#matched[@]} -gt 0 ]]; then
      all_pids+=("${matched[@]}")
    fi
  done

  if [[ ${#all_pids[@]} -gt 0 ]]; then
    printf '%s\n' "${all_pids[@]}" | awk '!seen[$0]++'
  fi
}

intra_cleanup_leaked_account_monitor() {
  local base_dir="${1:-}"
  local exchange="${2:-}"
  local kill_wait_secs="${3:-6}"
  local -a leaked_pids=()

  mapfile -t leaked_pids < <(intra_find_running_account_monitor_pids "$base_dir" "$exchange" || true)
  if [[ ${#leaked_pids[@]} -eq 0 ]]; then
    return 0
  fi

  echo "[WARN] Found leaked account monitor process(es): ${leaked_pids[*]}"
  echo "[INFO] Sending SIGTERM to leaked process(es)"
  kill "${leaked_pids[@]}" >/dev/null 2>&1 || true

  local deadline=$((SECONDS + kill_wait_secs))
  while [[ $SECONDS -lt $deadline ]]; do
    mapfile -t leaked_pids < <(intra_find_running_account_monitor_pids "$base_dir" "$exchange" || true)
    if [[ ${#leaked_pids[@]} -eq 0 ]]; then
      break
    fi
    sleep 1
  done

  if [[ ${#leaked_pids[@]} -gt 0 ]]; then
    echo "[WARN] SIGTERM timeout, sending SIGKILL: ${leaked_pids[*]}"
    kill -9 "${leaked_pids[@]}" >/dev/null 2>&1 || true
    sleep 1
    mapfile -t leaked_pids < <(intra_find_running_account_monitor_pids "$base_dir" "$exchange" || true)
  fi

  if [[ ${#leaked_pids[@]} -gt 0 ]]; then
    echo "[ERROR] Failed to kill leaked account monitor process(es): ${leaked_pids[*]}" >&2
    exit 1
  fi

  echo "[INFO] Leaked account monitor cleanup done"
}
