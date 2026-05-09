# dat_pbs / spread_pbs Deploy Remote Redirect Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mirror FR/intra/MM remote-deploy pattern in `dat_pbs` and `spread_pbs` deploy scripts so binance/bitget/gate venues rsync to `ubuntu@54.64.147.69:/home/ubuntu/<root>/`, while okex/bybit/aster stay local.

**Architecture:** Add two thin helpers (`fr_remote_init_ssh`, `fr_remote_sync_path`) to existing `scripts/lib/fr_remote_deploy.sh`. Append a ~30-line "venue routing tail" to each deploy script after the existing local stage: it splits selected venues into local/remote arrays, ssh-probes once if any remote venue is selected, rsyncs each remote venue dir + the shared `config/` to remote, then prints a grouped summary.

**Tech Stack:** bash, rsync, ssh — no new deps. Spec at `docs/superpowers/specs/2026-05-09-pbs-deploy-remote-redirect-design.md`.

---

## File Structure

| File | Change |
|------|--------|
| `scripts/lib/fr_remote_deploy.sh` | **Modify** — append two new functions; existing API unchanged. |
| `scripts/dat_pbs/deploy_dat_pbs.sh` | **Modify** — replace trailing summary block with routing tail + grouped summary. |
| `scripts/spread_pbs/deploy_spread_pbs.sh` | **Modify** — same pattern as dat_pbs, with `spread_pbs` root and no `aster` consideration. |
| `scripts/deploy_dat_pbs.sh` / `scripts/deploy_spread_pbs.sh` | **No change** — top-level wrappers only `exec` the inner scripts. |

No tests files. The repo has no shell-script test harness; existing FR/intra/MM remote-deploy code shipped without unit tests. Validation is `bash -n` + parse smoke (`--help`) + a single end-to-end local-only `--exchange okex` run that exercises the routing branch with `REMOTE_VENUES` empty.

---

## Task 1: Add `fr_remote_init_ssh` and `fr_remote_sync_path` helpers

**Files:**
- Modify: `scripts/lib/fr_remote_deploy.sh` (append at end of file)

- [ ] **Step 1: Append two new helpers**

Open `scripts/lib/fr_remote_deploy.sh` and append after the existing `fr_remote_apply_nginx` function (i.e., at end of file):

```bash

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
```

- [ ] **Step 2: Syntax check**

Run: `bash -n scripts/lib/fr_remote_deploy.sh`
Expected: exit 0, no output.

- [ ] **Step 3: Source check (verifies functions are exported)**

Run:
```bash
bash -c 'source scripts/lib/fr_remote_deploy.sh && declare -F fr_remote_init_ssh fr_remote_sync_path fr_remote_init fr_remote_sync_env_dir fr_remote_apply_nginx'
```
Expected (exact 5 lines, in any order):
```
declare -f fr_remote_apply_nginx
declare -f fr_remote_init
declare -f fr_remote_init_ssh
declare -f fr_remote_sync_env_dir
declare -f fr_remote_sync_path
```

- [ ] **Step 4: Commit**

```bash
git add scripts/lib/fr_remote_deploy.sh
git -c commit.gpgsign=false commit -m "$(cat <<'EOF'
feat(remote-deploy): add fr_remote_init_ssh + fr_remote_sync_path helpers

Lightweight variants of fr_remote_init / fr_remote_sync_env_dir for callers
that don't need nginx staging and want a generic relative-path rsync (used
by upcoming dat_pbs / spread_pbs remote redirect).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: dat_pbs deploy — venue routing tail + grouped summary

**Files:**
- Modify: `scripts/dat_pbs/deploy_dat_pbs.sh` — replace trailing block (lines ~194-202 after Task 1, or whatever the current trailing block is).

- [ ] **Step 1: Replace trailing summary block with routing tail + grouped summary**

Use `Edit` to replace this exact block:

```bash
if [[ -f "$ROOT_DIR/config/iceoryx2.toml" ]]; then
  rsync -a "$ROOT_DIR/config/iceoryx2.toml" "${TARGET_ROOT%/}/config/"
fi

echo "[INFO] $BIN_NAME 部署完成"
echo "[INFO] root_dir: ${TARGET_ROOT%/}"
echo "[INFO] venues: ${VENUES[*]}"
echo "[INFO] config: ${TARGET_ROOT%/}/config/mkt_cfg.yaml"
echo "[INFO] 启动示例: cd ${TARGET_ROOT%/}/${VENUES[0]} && ./scripts/start_dat_pbs.sh"
```

with:

```bash
if [[ -f "$ROOT_DIR/config/iceoryx2.toml" ]]; then
  rsync -a "$ROOT_DIR/config/iceoryx2.toml" "${TARGET_ROOT%/}/config/"
fi

# 远端分流：binance/bitget/gate 的 venue 推到 AWS 远端主机
REMOTE_VENUE_REGEX='^(binance|bitget|gate)-(futures|margin)$'
REMOTE_VENUES=()
LOCAL_VENUES=()
for v in "${VENUES[@]}"; do
  if [[ "$v" =~ $REMOTE_VENUE_REGEX ]]; then
    REMOTE_VENUES+=("$v")
  else
    LOCAL_VENUES+=("$v")
  fi
done

if [[ ${#REMOTE_VENUES[@]} -gt 0 ]]; then
  # shellcheck source=../lib/fr_remote_deploy.sh
  source "$ROOT_DIR/scripts/lib/fr_remote_deploy.sh"
  fr_remote_init_ssh "$ROOT_DIR"
  for v in "${REMOTE_VENUES[@]}"; do
    fr_remote_sync_path "dat_pbs/$v"
  done
  fr_remote_sync_path "dat_pbs/config"
fi

echo "[INFO] $BIN_NAME 部署完成"
echo "[INFO] root_dir: ${TARGET_ROOT%/}"
if [[ ${#LOCAL_VENUES[@]} -gt 0 ]]; then
  echo "[INFO] local venues:"
  for v in "${LOCAL_VENUES[@]}"; do
    echo "  - ${v} -> ${TARGET_ROOT%/}/${v}/"
  done
fi
if [[ ${#REMOTE_VENUES[@]} -gt 0 ]]; then
  echo "[INFO] remote venues (${FR_DEPLOY_HOST}:${FR_REMOTE_HOME}/dat_pbs/):"
  for v in "${REMOTE_VENUES[@]}"; do
    echo "  - ${v}"
  done
fi
echo "[INFO] config: ${TARGET_ROOT%/}/config/mkt_cfg.yaml"
echo "[INFO] 启动:"
if [[ ${#LOCAL_VENUES[@]} -gt 0 ]]; then
  echo "  - 本地: cd ${TARGET_ROOT%/}/<venue> && ./scripts/start_dat_pbs.sh"
fi
if [[ ${#REMOTE_VENUES[@]} -gt 0 ]]; then
  echo "  - 远端: ssh ${FR_DEPLOY_HOST} 'cd ${FR_REMOTE_HOME}/dat_pbs/<venue> && ./scripts/start_dat_pbs.sh'"
fi
```

- [ ] **Step 2: Syntax check**

Run: `bash -n scripts/dat_pbs/deploy_dat_pbs.sh`
Expected: exit 0, no output.

- [ ] **Step 3: Parse smoke (validates arg parser still works)**

Run: `bash scripts/dat_pbs/deploy_dat_pbs.sh --help`
Expected: usage text printed; exit 0; no cargo build triggered.

- [ ] **Step 4: Commit**

```bash
git add scripts/dat_pbs/deploy_dat_pbs.sh
git -c commit.gpgsign=false commit -m "$(cat <<'EOF'
feat(dat-pbs-deploy): redirect binance/bitget/gate venues to remote AWS host

After local stage, split selected venues into local vs remote and rsync the
remote ones to ubuntu@54.64.147.69:/home/ubuntu/dat_pbs/<venue>/ (plus the
shared dat_pbs/config/) using fr_remote_sync_path. okex / bybit / aster stay
local. Final summary regroups output into local/remote sections.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: spread_pbs deploy — venue routing tail + grouped summary

**Files:**
- Modify: `scripts/spread_pbs/deploy_spread_pbs.sh` — replace trailing block.

- [ ] **Step 1: Replace trailing summary block with routing tail + grouped summary**

Use `Edit` to replace this exact block:

```bash
# 共享配置（mkt_cfg.yaml + iceoryx2.toml）部署到根目录
mkdir -p "${TARGET_ROOT%/}/config"
if [[ -f "$ROOT_DIR/config/mkt_cfg.yaml" ]]; then
  rsync -a "$ROOT_DIR/config/mkt_cfg.yaml" "${TARGET_ROOT%/}/config/"
fi
if [[ -f "$ROOT_DIR/config/iceoryx2.toml" ]]; then
  rsync -a "$ROOT_DIR/config/iceoryx2.toml" "${TARGET_ROOT%/}/config/"
fi

echo "[INFO] $BIN_NAME 部署完成"
echo "[INFO] root_dir: ${TARGET_ROOT%/}"
echo "[INFO] venues:   ${VENUES[*]}"
echo "[INFO] config:   ${TARGET_ROOT%/}/config/mkt_cfg.yaml"
echo "[INFO] 启动示例: cd ${TARGET_ROOT%/}/${VENUES[0]} && ./scripts/start_spread_pbs.sh"
```

with:

```bash
# 共享配置（mkt_cfg.yaml + iceoryx2.toml）部署到根目录
mkdir -p "${TARGET_ROOT%/}/config"
if [[ -f "$ROOT_DIR/config/mkt_cfg.yaml" ]]; then
  rsync -a "$ROOT_DIR/config/mkt_cfg.yaml" "${TARGET_ROOT%/}/config/"
fi
if [[ -f "$ROOT_DIR/config/iceoryx2.toml" ]]; then
  rsync -a "$ROOT_DIR/config/iceoryx2.toml" "${TARGET_ROOT%/}/config/"
fi

# 远端分流：binance/bitget/gate 的 venue 推到 AWS 远端主机
REMOTE_VENUE_REGEX='^(binance|bitget|gate)-(futures|margin)$'
REMOTE_VENUES=()
LOCAL_VENUES=()
for v in "${VENUES[@]}"; do
  if [[ "$v" =~ $REMOTE_VENUE_REGEX ]]; then
    REMOTE_VENUES+=("$v")
  else
    LOCAL_VENUES+=("$v")
  fi
done

if [[ ${#REMOTE_VENUES[@]} -gt 0 ]]; then
  # shellcheck source=../lib/fr_remote_deploy.sh
  source "$ROOT_DIR/scripts/lib/fr_remote_deploy.sh"
  fr_remote_init_ssh "$ROOT_DIR"
  for v in "${REMOTE_VENUES[@]}"; do
    fr_remote_sync_path "spread_pbs/$v"
  done
  fr_remote_sync_path "spread_pbs/config"
fi

echo "[INFO] $BIN_NAME 部署完成"
echo "[INFO] root_dir: ${TARGET_ROOT%/}"
if [[ ${#LOCAL_VENUES[@]} -gt 0 ]]; then
  echo "[INFO] local venues:"
  for v in "${LOCAL_VENUES[@]}"; do
    echo "  - ${v} -> ${TARGET_ROOT%/}/${v}/"
  done
fi
if [[ ${#REMOTE_VENUES[@]} -gt 0 ]]; then
  echo "[INFO] remote venues (${FR_DEPLOY_HOST}:${FR_REMOTE_HOME}/spread_pbs/):"
  for v in "${REMOTE_VENUES[@]}"; do
    echo "  - ${v}"
  done
fi
echo "[INFO] config:   ${TARGET_ROOT%/}/config/mkt_cfg.yaml"
echo "[INFO] 启动:"
if [[ ${#LOCAL_VENUES[@]} -gt 0 ]]; then
  echo "  - 本地: cd ${TARGET_ROOT%/}/<venue> && ./scripts/start_spread_pbs.sh"
fi
if [[ ${#REMOTE_VENUES[@]} -gt 0 ]]; then
  echo "  - 远端: ssh ${FR_DEPLOY_HOST} 'cd ${FR_REMOTE_HOME}/spread_pbs/<venue> && ./scripts/start_spread_pbs.sh'"
fi
```

- [ ] **Step 2: Syntax check**

Run: `bash -n scripts/spread_pbs/deploy_spread_pbs.sh`
Expected: exit 0, no output.

- [ ] **Step 3: Parse smoke (validates arg parser still works)**

Run: `bash scripts/spread_pbs/deploy_spread_pbs.sh --help`
Expected: usage text printed; exit 0; no cargo build triggered.

- [ ] **Step 4: Commit**

```bash
git add scripts/spread_pbs/deploy_spread_pbs.sh
git -c commit.gpgsign=false commit -m "$(cat <<'EOF'
feat(spread-pbs-deploy): redirect binance/bitget/gate venues to remote AWS host

After local stage, split selected venues into local vs remote and rsync the
remote ones to ubuntu@54.64.147.69:/home/ubuntu/spread_pbs/<venue>/ (plus the
shared spread_pbs/config/) using fr_remote_sync_path. okex / bybit stay local.
Final summary regroups output into local/remote sections.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: End-to-end local-only smoke test

Validates the routing branch with `REMOTE_VENUES` empty (no ssh required), exercising cargo build + local stage + grouped summary on both scripts.

**Files:** none modified.

- [ ] **Step 1: dat_pbs okex-only smoke**

Run: `bash scripts/dat_pbs/deploy_dat_pbs.sh --exchange okex`
Expected:
- `[INFO] 构建 dat_pbs (release)` then cargo build output (may take minutes on cold cache)
- `[INFO] 部署 dat_pbs 到 $HOME/dat_pbs/okex-futures` and `okex-margin`
- **No** `[INFO] remote target` line (remote branch skipped)
- **No** ssh probe / rsync to remote
- Final summary contains `[INFO] local venues:` block listing okex-futures + okex-margin
- Final summary does **not** contain `[INFO] remote venues:` block
- Exit code 0

- [ ] **Step 2: spread_pbs bybit-only smoke**

Run: `bash scripts/spread_pbs/deploy_spread_pbs.sh --exchange bybit`
Expected (analogous to step 1):
- cargo builds `spread_pbs`
- Stages `$HOME/spread_pbs/bybit-margin/` + `$HOME/spread_pbs/bybit-futures/`
- **No** remote target / ssh probe / rsync
- Final summary contains `[INFO] local venues:` block listing both bybit venues only
- Exit code 0

- [ ] **Step 3: Confirm helper file unchanged for non-remote runs**

Run: `git status`
Expected: clean tree (the smoke runs only write to `$HOME/dat_pbs/` and `$HOME/spread_pbs/`, not the repo).

- [ ] **Step 4: No commit**

This task only validates; no commit needed.

---

## Out-of-scope follow-ups (do NOT do as part of this plan)

- Actually pushing to the remote AWS host. That requires the user's `aws-jp-srv-1.pem` and access permission. The user will run e.g. `bash scripts/dat_pbs/deploy_dat_pbs.sh --exchange binance` themselves after this plan lands.
- Auto-starting remote processes. Mirroring FR/intra/MM, deploys are build+rsync only; ops manually `ssh ubuntu@54.64.147.69` to run `start_*.sh`.
- Removing stale remote venue dirs from a previous local-only era. rsync is incremental; leftover remote dirs do not affect new deploys.
