# order_export: dash env names + remote binary sync — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **No new Rust tests.** Per user preference, do not add `#[test]` cases for this change and do not run `cargo test` as part of the workflow. Validate via `cargo build --release` + `cargo clippy -- -D warnings` + a real-env smoke run. Existing tests in the file stay untouched.

**Goal:** Extend `order_export` to accept dash-form env names (`binance-intra-arb01` etc.) and have `deploy_order_export.sh` push the built binary to the remote host alongside the local install.

**Architecture:** Two independent edits — (1) refactor the validator in `src/bin/order_export.rs` to iterate over `(separator, tokens)` pairs and use a separator-aware suffix check; (2) extend `scripts/deploy_order_export.sh` to source the existing `lib/fr_remote_deploy.sh` helper and rsync the single binary to `$FR_DEPLOY_HOST:$FR_REMOTE_HOME/order_export/bin/order_export`. The `_cross_` branch and all CLI/path behavior are unchanged.

**Tech Stack:** Rust (clap, anyhow), bash, rsync, ssh.

**Reference:** [`docs/superpowers/specs/2026-05-13-order-export-dash-and-remote-design.md`](../specs/2026-05-13-order-export-dash-and-remote-design.md)

---

## Task 1: Validator — accept dash-form env names

**Files:**
- Modify: `src/bin/order_export.rs:239-281` (helpers + `validate_supported_env_name`)
- **Do not** touch `mod tests` — no new test cases.

### Step 1.1: Replace `is_valid_env_suffix` with separator-aware helper

- [ ] Open `src/bin/order_export.rs`. Locate `is_valid_env_suffix` at line 239-244. Replace it with the two functions below (the old name remains as a thin wrapper so the unchanged `_cross_` branch still compiles):

```rust
fn is_valid_env_suffix_for(suffix: &str, sep: char) -> bool {
    !suffix.is_empty()
        && suffix.chars().all(|ch| match ch {
            'a'..='z' | '0'..='9' | '-' => true,
            '_' => sep == '_',
            _ => false,
        })
}

fn is_valid_env_suffix(suffix: &str) -> bool {
    is_valid_env_suffix_for(suffix, '_')
}
```

Rationale: underscore mode keeps the original `[a-z0-9_-]` charset (so existing `okex_mm_beta-1`-style suffixes still pass); dash mode forbids `_` to prevent half-mixed `okex-intra-arb_01` writes from slipping through.

### Step 1.2: Rewrite `validate_supported_env_name` with `TOKEN_GROUPS`

- [ ] Replace the current function body at `src/bin/order_export.rs:262-281` with the version below. Place the `const TOKEN_GROUPS` immediately above the function:

```rust
const TOKEN_GROUPS: &[(char, [&str; 3])] = &[
    ('_', ["_mm_", "_fr_", "_intra_"]),
    ('-', ["-mm-", "-fr-", "-intra-"]),
];

fn validate_supported_env_name(env_name: &str) -> Result<()> {
    for (sep, tokens) in TOKEN_GROUPS {
        for token in tokens {
            if let Some((exchange, suffix)) = env_name.split_once(*token) {
                if is_valid_exchange_name(exchange) && is_valid_env_suffix_for(suffix, *sep) {
                    return Ok(());
                }
            }
        }
    }

    if let Some((exchange_pair, suffix)) = env_name.split_once("_cross_") {
        if is_valid_exchange_pair_name(exchange_pair) && is_valid_env_suffix(suffix) {
            return Ok(());
        }
    }

    Err(anyhow!(
        "env_name must match <exchange>{{_|-}}<mm|fr|intra>{{_|-}}<suffix> or <open_ex>-<hedge_ex>_cross_<suffix> (got: {})",
        env_name
    ))
}
```

### Step 1.3: Build + clippy + fmt

- [ ] Run:

```bash
cargo fmt --all
cargo build --release --bin order_export
cargo clippy --bin order_export -- -D warnings
```

Expected:
- `fmt` produces no diff (or trivial whitespace adjustments).
- `build --release` succeeds.
- `clippy` reports no warnings.

If clippy flags anything (e.g., unused `is_valid_env_suffix` because `_cross_` no longer routes through it — it should still, but double-check), fix inline.

### Step 1.4: Real-env smoke test

- [ ] Verify the change end-to-end against a dash-form intra env:

```bash
cd ~/okex-intra-arb01
RUST_LOG=info /home/fanghaizhou/mkt_signal/target/release/order_export --date 2026-05-12 \
  --base-dir /home/fanghaizhou 2>&1 | head -20
```

Expected: the validator no longer emits `env_name must match ...`. The next failure point becomes `input_dir does not exist: ...` (if `data/persist_manager` is absent) or a RocksDB-open error (if a live `persist_manager` holds the DB). Either outcome confirms the validator change is correct — a successful export is **not** required to pass this task.

If you see `env_name must match ...`, the validator regression is not fixed; recheck Step 1.2.

### Step 1.5: Commit

- [ ] Stage and commit:

```bash
git add src/bin/order_export.rs
git commit -m "$(cat <<'EOF'
feat(order_export): accept dash-form env names (binance-intra-arb01 etc.)

Validator now iterates over (separator, tokens) groups and applies a
separator-aware suffix check, so both `okex_intra_hf01` and
`okex-intra-arb01` pass while mixed-separator forms like
`binance_intra-arb01` are rejected. `_cross_` branch is unchanged.
EOF
)"
```

Expected: commit succeeds, working tree clean.

---

## Task 2: Deploy script — local install + remote rsync

**Files:**
- Modify: `scripts/deploy_order_export.sh` (full rewrite — currently 21 lines)
- Read-only reference: `scripts/lib/fr_remote_deploy.sh` (already provides `fr_remote_init_ssh`, `_fr_ssh_opts`, `FR_DEPLOY_HOST`, `FR_REMOTE_HOME`, `FR_DEPLOY_KEY`)

### Step 2.1: Replace `scripts/deploy_order_export.sh`

- [ ] Open `scripts/deploy_order_export.sh` and replace its entire contents with:

```bash
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-/home/${USER}/order_export}"
SKIP_LOCAL=0
SKIP_REMOTE=0

usage() {
  cat <<'EOF'
用法:
  scripts/deploy_order_export.sh [--skip-local] [--skip-remote]

说明:
  - 默认: cargo install 到本地 /home/$USER/order_export,
          然后 rsync 推送到 $FR_DEPLOY_HOST:/home/ubuntu/order_export/bin/.
  - --skip-local : 跳过本地 cargo install (复用现有 binary).
  - --skip-remote: 只装本地, 不推远端.
  - 远端 host/key 通过 scripts/lib/fr_remote_deploy.sh 的环境变量控制
    (FR_DEPLOY_HOST / FR_DEPLOY_KEY / FR_REMOTE_HOME).
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-local)  SKIP_LOCAL=1; shift ;;
    --skip-remote) SKIP_REMOTE=1; shift ;;
    -h|--help)     usage; exit 0 ;;
    *)             echo "[ERROR] unknown arg: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ $SKIP_LOCAL -eq 0 ]]; then
  echo "[INFO] installing order_export into ${INSTALL_ROOT}"
  cargo install --path "${BASE_DIR}" --bin order_export --root "${INSTALL_ROOT}" --force --locked
  echo "[INFO] local binary at ${INSTALL_ROOT}/bin/order_export"
else
  echo "[INFO] --skip-local: leaving ${INSTALL_ROOT}/bin/order_export untouched"
fi

if [[ $SKIP_REMOTE -eq 0 ]]; then
  # shellcheck source=lib/fr_remote_deploy.sh
  source "${BASE_DIR}/scripts/lib/fr_remote_deploy.sh"
  fr_remote_init_ssh "${BASE_DIR}"

  LOCAL_BIN="${INSTALL_ROOT}/bin/order_export"
  if [[ ! -f "${LOCAL_BIN}" ]]; then
    echo "[ERROR] local binary missing: ${LOCAL_BIN}" >&2
    echo "[ERROR] run without --skip-local first, or build it manually." >&2
    exit 1
  fi

  OPTS="$(_fr_ssh_opts)"
  REMOTE_BIN_DIR="${FR_REMOTE_HOME}/order_export/bin"

  echo "[INFO] mkdir -p remote: ${FR_DEPLOY_HOST}:${REMOTE_BIN_DIR}"
  # shellcheck disable=SC2086
  ssh ${OPTS} "${FR_DEPLOY_HOST}" "mkdir -p ${REMOTE_BIN_DIR}"

  echo "[INFO] rsync ${LOCAL_BIN} -> ${FR_DEPLOY_HOST}:${REMOTE_BIN_DIR}/order_export"
  # shellcheck disable=SC2086
  rsync -a --human-readable --info=stats1 \
    -e "ssh ${OPTS}" \
    "${LOCAL_BIN}" \
    "${FR_DEPLOY_HOST}:${REMOTE_BIN_DIR}/order_export"
else
  echo "[INFO] --skip-remote: not pushing to remote"
fi

cat <<EOF
[INFO] done.
[INFO] local : ${INSTALL_ROOT}/bin/order_export
[INFO] remote: ssh -i ${FR_DEPLOY_KEY:-${BASE_DIR}/aws-jp-srv-1.pem} \\
           ${FR_DEPLOY_HOST:-ubuntu@54.64.147.69} \\
           '/home/ubuntu/order_export/bin/order_export --date YYYY-MM-DD'
[INFO] env once:
  export ORDER_EXPORT_BASE_DIR=/home/\$USER
[INFO] example:
  cd ~/binance-intra-arb01
  ${INSTALL_ROOT}/bin/order_export --date 2026-05-12
EOF
```

Why these choices (recap from spec):
- `fr_remote_init_ssh` (not `fr_remote_init`) — we don't need nginx staging, only the key + ssh probe.
- Single-file rsync (not `fr_remote_sync_env_dir`) — that helper is directory-grained.
- `--skip-local` path: check the local binary exists before pushing, so we don't silently push a missing file.
- Failure: any failed step exits non-zero; local install is **not** rolled back if remote push fails.

### Step 2.2: Sanity-check via `bash -n`

- [ ] Run:

```bash
bash -n scripts/deploy_order_export.sh && echo OK
```

Expected: `OK`.

### Step 2.3: Help/usage smoke

- [ ] Run:

```bash
bash scripts/deploy_order_export.sh --help
```

Expected: usage text printed, exit 0, no cargo invocation, no ssh.

### Step 2.4: `--skip-local --skip-remote` no-op smoke

- [ ] Run:

```bash
bash scripts/deploy_order_export.sh --skip-local --skip-remote
```

Expected: prints `--skip-local: leaving ...`, `--skip-remote: not pushing to remote`, and the trailing `[INFO] done.` block. No cargo, no ssh.

### Step 2.5: `--skip-remote` local-only deploy

- [ ] Run:

```bash
bash scripts/deploy_order_export.sh --skip-remote
```

Expected: `cargo install` runs and overwrites `/home/fanghaizhou/order_export/bin/order_export`. The `[INFO] --skip-remote: not pushing to remote` line appears. No ssh attempted. Verify binary mtime:

```bash
ls -la /home/fanghaizhou/order_export/bin/order_export
```

Expected: mtime is fresh (within the last few minutes).

### Step 2.6: `--skip-local` remote-only push

- [ ] Run (after Step 2.5 so the local binary is fresh):

```bash
bash scripts/deploy_order_export.sh --skip-local
```

Expected:
- `[INFO] --skip-local: leaving /home/fanghaizhou/order_export/bin/order_export untouched`
- `[INFO] remote target : ubuntu@54.64.147.69:/home/ubuntu` (from `fr_remote_init_ssh`)
- ssh probe succeeds
- mkdir + rsync succeed
- final `[INFO] done.` block

Verify remote binary landed:

```bash
ssh -i ./aws-jp-srv-1.pem ubuntu@54.64.147.69 'ls -la /home/ubuntu/order_export/bin/order_export'
```

Expected: file present, mtime fresh, size ~21 MB.

If `fr_remote_init_ssh` fails with `missing ssh key`, the engineer must place `aws-jp-srv-1.pem` at the repo root (or export `FR_DEPLOY_KEY=/path/to/key.pem`). This is a precondition called out in the spec's Risks section.

### Step 2.7: Full deploy (the real workflow)

- [ ] Run:

```bash
bash scripts/deploy_order_export.sh
```

Expected: Step 2.5 and Step 2.6 happen in one shot. Exit 0. Both local and remote binaries are fresh.

### Step 2.8: Commit

- [ ] Stage and commit:

```bash
git add scripts/deploy_order_export.sh
git commit -m "$(cat <<'EOF'
feat(deploy): order_export script also rsyncs binary to remote host

deploy_order_export.sh now sources lib/fr_remote_deploy.sh, runs the
existing ssh-key probe via fr_remote_init_ssh, and rsyncs the freshly
installed binary to $FR_DEPLOY_HOST:$FR_REMOTE_HOME/order_export/bin.
--skip-local / --skip-remote flags allow running either half alone.
EOF
)"
```

Expected: commit succeeds, working tree clean.

---

## Acceptance — End-to-end

After both tasks are committed:

- [ ] `cargo build --release --bin order_export` — compiles cleanly.
- [ ] `cargo clippy --bin order_export -- -D warnings` — no warnings.
- [ ] `cd ~/okex-intra-arb01 && ~/order_export/bin/order_export --date 2026-05-12` — no validator error (RocksDB-open errors or missing-input-dir errors are acceptable; the validator no longer rejects).
- [ ] `bash scripts/deploy_order_export.sh --skip-remote` — exits 0, only local binary updated.
- [ ] `bash scripts/deploy_order_export.sh --skip-local` — exits 0, only remote binary updated (verify via `ssh ... ls -la /home/ubuntu/order_export/bin/order_export`).
- [ ] `bash scripts/deploy_order_export.sh` — full happy path: local + remote both updated.
