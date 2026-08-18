# shellcheck shell=bash
# Shared helper for remote orchestration scripts.
#
# `ssh host bash -s <<'EOF'` deadlocks when a child such as `npx pm2 start`
# inherits that heredoc: PM2 waits for EOF, SSH waits for bash, bash waits
# for PM2. Materialize the script first, then exec it with stdin from /dev/null.
#
# Usage:
#   ssh_remote_bash SSH_ARRAY_NAME host arg1 arg2 <<'EOF'
#     echo "$1"
#   EOF
#
# SSH_ARRAY_NAME is the name of an array holding the ssh command, e.g. SSH
# or INTRA_SSH.

ssh_remote_bash() {
  local -n _ssh_remote_cmd="$1"
  local host="$2"
  shift 2
  if [[ ${#_ssh_remote_cmd[@]} -eq 0 || -z "$host" ]]; then
    echo "[ERROR] ssh_remote_bash requires an ssh command array and host" >&2
    return 2
  fi
  local quoted_args=""
  local arg
  for arg in "$@"; do
    quoted_args+=" $(printf '%q' "$arg")"
  done
  "${_ssh_remote_cmd[@]}" "$host" \
    "tmp=\$(mktemp /tmp/ssh-remote-bash.XXXXXX) && cat >\"\$tmp\" && bash \"\$tmp\"${quoted_args} </dev/null; st=\$?; rm -f \"\$tmp\"; exit \$st"
}
