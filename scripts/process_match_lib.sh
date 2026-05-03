#!/usr/bin/env bash

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
