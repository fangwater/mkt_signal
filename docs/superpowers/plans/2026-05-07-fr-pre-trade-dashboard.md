# FR Pre-Trade Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把已实现的 `docs/fr_pre_trade_dashboard.html`（二合一 board）接到 FR 套利的部署链路：将 `fr_signal_dashboard` 改为 per-env-name 部署（与 viz_server 同 dir 共生），让 `deploy_fr_viz_server.sh` 拷贝新 HTML 并在 nginx 里加 `/fr_ws → viz_port + 1` 反代。

**Architecture:** 两个 deploy 脚本各自 hardcode 端口约定 `fr_dashboard_port = viz_port + 1`，不引入文件 / CLI 参数 indirection。`deploy_fr_signal_dashboard.sh` 接 `--env-name` + `--viz-port`，部署到 `$HOME/<env-name>/`，PM2 namespace = env-name；`deploy_fr_viz_server.sh` 直接 `local fr_port=$((PORT + 1))` 用于 nginx awk upsert。

**Tech Stack:** Bash, awk, nginx config (managed block in `nginx_locations.txt`)。HTML demo 已存在并 commit（`890b762`）。

**Spec:** `docs/superpowers/specs/2026-05-07-fr-pre-trade-dashboard-design.md`

**Files:**
- Modify: `scripts/deploy_fr_signal_dashboard.sh`（破坏性重构）
  - 必传 `--env-name`、`--viz-port`，废弃 `--target` `--port` 参数
  - TARGET_DIR 强制 `$HOME/<env-name>/`
  - PORT = `viz_port + 1`
  - PM2 namespace = env-name；proc_name = `${env_name}_fr_signal_dashboard`
  - exchange 可选，不传时从 env-name 第一段推断
- Modify: `scripts/deploy_fr_viz_server.sh`
  - HTML 拷贝段（line 396-402）：优先 `fr_pre_trade_dashboard.html`，回退通用版
  - awk upsert 段（line 134-178）：在 `/ws` `/snapshot` `/healthz` 后追加 `/fr_ws → 127.0.0.1:$((PORT+1))/ws`，端口在 awk 之前用 bash 算

无 Rust 代码改动，无新增文件，无 viz.toml 改动。

---

### Task 1: `deploy_fr_signal_dashboard.sh` 改 per-env-name

**Files:**
- Modify: `scripts/deploy_fr_signal_dashboard.sh`（整体重构，142 行 → 约 160 行）

- [ ] **Step 1: 整体重写**

把整个 `scripts/deploy_fr_signal_dashboard.sh` 替换为以下内容：

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="fr_signal_dashboard"
BIN_PATH="${ROOT_DIR}/target/release/${BIN_NAME}"

ENV_NAME=""
EXCHANGE=""
VIZ_PORT=""
BIND="0.0.0.0"
WS_PATH="/ws"
DO_BUILD=1
DO_START=1

usage() {
  cat <<'EOF'
Usage:
  scripts/deploy_fr_signal_dashboard.sh --env-name <exchange>_fr_<suffix>
                                        --viz-port <viz_server_port>
                                        [--exchange <binance|okex|gate|bybit|bitget|hyperliquid>]
                                        [--bind 0.0.0.0]
                                        [--ws-path /ws]
                                        [--bin-only|--scripts-only]
                                        [--no-start]

Notes:
  - Per-env deploy under $HOME/<env-name>/, parallel to viz_server.
  - fr_signal_dashboard port = viz-port + 1 (hardcoded convention,
    matched by deploy_fr_viz_server.sh nginx upsert).
  - PM2 namespace = env-name; proc_name = ${env_name}_fr_signal_dashboard.
  - Generates dashboard.env for start_fr_signal_dashboard.sh consumption.
  - --exchange optional; inferred from env-name prefix if omitted.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-name)
      ENV_NAME="${2:-}"
      shift 2
      ;;
    --exchange)
      EXCHANGE="${2:-}"
      shift 2
      ;;
    --viz-port)
      VIZ_PORT="${2:-}"
      shift 2
      ;;
    --bind)
      BIND="${2:-0.0.0.0}"
      shift 2
      ;;
    --ws-path)
      WS_PATH="${2:-/ws}"
      shift 2
      ;;
    --bin-only)
      DO_BUILD=1
      shift
      ;;
    --scripts-only)
      DO_BUILD=0
      shift
      ;;
    --no-start)
      DO_START=0
      shift
      ;;
    *)
      echo "[ERROR] unknown arg: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

normalize_exchange() {
  local ex="${1,,}"
  if [[ "$ex" == "okx" ]]; then
    ex="okex"
  fi
  echo "$ex"
}

infer_exchange_from_env_name() {
  local name="${1,,}"
  if [[ "$name" =~ ^([a-z0-9]+)[-_]fr([_-].*)?$ ]]; then
    echo "${BASH_REMATCH[1]}"
  fi
}

if [[ -z "$ENV_NAME" ]]; then
  echo "[ERROR] --env-name is required (e.g. binance_fr_trade01)" >&2
  usage >&2
  exit 1
fi
ENV_NAME="${ENV_NAME,,}"

if [[ -z "$VIZ_PORT" ]]; then
  echo "[ERROR] --viz-port is required (matches deploy_fr_viz_server.sh --port)" >&2
  usage >&2
  exit 1
fi
if [[ ! "$VIZ_PORT" =~ ^[0-9]+$ ]]; then
  echo "[ERROR] --viz-port must be numeric: $VIZ_PORT" >&2
  exit 1
fi

if [[ -z "$EXCHANGE" ]]; then
  EXCHANGE="$(infer_exchange_from_env_name "$ENV_NAME")"
fi
EXCHANGE="$(normalize_exchange "$EXCHANGE")"
case "$EXCHANGE" in
  binance|okex|gate|bybit|bitget|hyperliquid)
    ;;
  *)
    echo "[ERROR] unable to determine exchange (got '${EXCHANGE}'); pass --exchange explicitly" >&2
    exit 1
    ;;
esac

PORT=$((VIZ_PORT + 1))
TARGET_DIR="${HOME}/${ENV_NAME}"
PM2_NS="${ENV_NAME}"
PM2_NAME="${ENV_NAME}_fr_signal_dashboard"

if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[INFO] building ${BIN_NAME} (release)"
  cargo build --release --bin "${BIN_NAME}"
fi

mkdir -p "${TARGET_DIR}"

if [[ "$DO_BUILD" -eq 1 ]]; then
  cp -f "${BIN_PATH}" "${TARGET_DIR}/${BIN_NAME}"
  chmod +x "${TARGET_DIR}/${BIN_NAME}"
fi

cp -f "${ROOT_DIR}/scripts/start_fr_signal_dashboard.sh" "${TARGET_DIR}/start_fr_signal_dashboard.sh"
cp -f "${ROOT_DIR}/scripts/stop_fr_signal_dashboard.sh" "${TARGET_DIR}/stop_fr_signal_dashboard.sh"
chmod +x "${TARGET_DIR}/start_fr_signal_dashboard.sh" "${TARGET_DIR}/stop_fr_signal_dashboard.sh"

cat > "${TARGET_DIR}/dashboard.env" <<EOF
FR_DASHBOARD_EXCHANGE=${EXCHANGE}
FR_DASHBOARD_BIND=${BIND}
FR_DASHBOARD_PORT=${PORT}
FR_DASHBOARD_WS_PATH=${WS_PATH}
PM2_NAMESPACE=${PM2_NS}
PM2_NAME=${PM2_NAME}
RUST_LOG=info
EOF

echo "[INFO] deployed ${BIN_NAME} to ${TARGET_DIR} (port=${PORT}, viz_port=${VIZ_PORT})"
echo "[INFO] config: ${TARGET_DIR}/dashboard.env"
echo "[INFO] PM2: namespace=${PM2_NS} name=${PM2_NAME}"

if [[ "$DO_START" -eq 1 ]]; then
  (
    cd "${TARGET_DIR}"
    ./start_fr_signal_dashboard.sh
  )
else
  echo "[INFO] start manually: cd ${TARGET_DIR} && ./start_fr_signal_dashboard.sh"
fi
```

- [ ] **Step 2: 验证脚本语法**

```bash
bash -n scripts/deploy_fr_signal_dashboard.sh
echo "exit=$?"
```

Expected: 退出码 0，无输出。

- [ ] **Step 3: 验证 --help 渲染**

```bash
scripts/deploy_fr_signal_dashboard.sh --help 2>&1 | grep -E "env-name|viz-port|viz-port \+ 1"
```

Expected: 至少 3 行匹配（usage 行 + Notes 段两行）。

- [ ] **Step 4: 验证缺参数报错**

```bash
scripts/deploy_fr_signal_dashboard.sh 2>&1 | grep -E "env-name is required"
echo "exit=$?"
```

Expected: 退出码 0（grep 匹配到错误行）。

```bash
scripts/deploy_fr_signal_dashboard.sh --env-name binance_fr_trade01 2>&1 | grep -E "viz-port is required"
echo "exit=$?"
```

Expected: 退出码 0。

- [ ] **Step 5: 验证 dry-run（--scripts-only --no-start）写出 dashboard.env**

```bash
TMPHOME="$(mktemp -d)"
HOME="$TMPHOME" scripts/deploy_fr_signal_dashboard.sh \
  --env-name binance_fr_trade99 \
  --viz-port 10091 \
  --scripts-only --no-start

cat "$TMPHOME/binance_fr_trade99/dashboard.env"
```

Expected: 输出包含 `FR_DASHBOARD_PORT=10092`、`FR_DASHBOARD_EXCHANGE=binance`、`PM2_NAMESPACE=binance_fr_trade99`、`PM2_NAME=binance_fr_trade99_fr_signal_dashboard`。

- [ ] **Step 6: 清理**

```bash
rm -rf "$TMPHOME"
```

- [ ] **Step 7: 提交**

```bash
git add scripts/deploy_fr_signal_dashboard.sh
git commit -m "refactor(fr-deploy): per-env-name fr_signal_dashboard with viz_port+1 convention

- Required: --env-name, --viz-port (port = viz_port + 1)
- Deploy target: \$HOME/<env-name>/ (parallel to viz_server)
- PM2 namespace = env-name; proc_name = <env_name>_fr_signal_dashboard
- exchange auto-inferred from env-name prefix
- Removes legacy /ubuntu/dashboard global single-instance mode

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: `deploy_fr_viz_server.sh` HTML 拷贝优先 FR 二合一

**Files:**
- Modify: `scripts/deploy_fr_viz_server.sh:396-402`

- [ ] **Step 1: 改 HTML 拷贝逻辑**

修改 `scripts/deploy_fr_viz_server.sh` 的 line 396-402：

```bash
# 变更前
  if [[ -f "$ROOT_DIR/docs/pre_trade_dashboard.html" ]]; then
    cp "$ROOT_DIR/docs/pre_trade_dashboard.html" "$TARGET_DIR/www/pre_trade_dashboard.html"
    cp "$ROOT_DIR/docs/pre_trade_dashboard.html" "$TARGET_DIR/www/index.html"
    echo "[INFO] Synced dashboard: $TARGET_DIR/www/pre_trade_dashboard.html"
  else
    echo "[WARN] Missing dashboard: $ROOT_DIR/docs/pre_trade_dashboard.html"
  fi

# 变更后
  if [[ -f "$ROOT_DIR/docs/fr_pre_trade_dashboard.html" ]]; then
    cp "$ROOT_DIR/docs/fr_pre_trade_dashboard.html" "$TARGET_DIR/www/pre_trade_dashboard.html"
    cp "$ROOT_DIR/docs/fr_pre_trade_dashboard.html" "$TARGET_DIR/www/index.html"
    echo "[INFO] Synced dashboard: $TARGET_DIR/www/pre_trade_dashboard.html (FR 二合一)"
  elif [[ -f "$ROOT_DIR/docs/pre_trade_dashboard.html" ]]; then
    cp "$ROOT_DIR/docs/pre_trade_dashboard.html" "$TARGET_DIR/www/pre_trade_dashboard.html"
    cp "$ROOT_DIR/docs/pre_trade_dashboard.html" "$TARGET_DIR/www/index.html"
    echo "[INFO] Synced dashboard: $TARGET_DIR/www/pre_trade_dashboard.html (通用回退)"
  else
    echo "[WARN] Missing dashboard: $ROOT_DIR/docs/fr_pre_trade_dashboard.html"
  fi
```

- [ ] **Step 2: 验证脚本语法**

```bash
bash -n scripts/deploy_fr_viz_server.sh
echo "exit=$?"
```

Expected: 退出码 0。

- [ ] **Step 3: 验证 fr_pre_trade_dashboard.html 存在**

```bash
ls -la docs/fr_pre_trade_dashboard.html
```

Expected: 文件存在（commit `890b762` 已提交，约 26KB）。

- [ ] **Step 4: 提交**

```bash
git add scripts/deploy_fr_viz_server.sh
git commit -m "feat(fr-deploy): prefer fr_pre_trade_dashboard.html in www/ copy

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: `deploy_fr_viz_server.sh` nginx upsert 加 `/fr_ws` 反代（viz_port + 1）

**Files:**
- Modify: `scripts/deploy_fr_viz_server.sh:118-178`

- [ ] **Step 1: 在 awk 调用前加 fr 端口和 location 计算**

修改 line 118-122 这一段（在 `local static_dir=...` 后添加两行）：

```bash
# 变更前
  local ws_location="${base_prefix}${ws_path}"
  local health_location="${base_prefix}/healthz"
  local snapshot_location="${base_prefix}/snapshot"
  local static_dir="${TARGET_DIR}/www/"

  begin_marker="# BEGIN managed: fr viz ${base_prefix}"

# 变更后
  local ws_location="${base_prefix}${ws_path}"
  local health_location="${base_prefix}/healthz"
  local snapshot_location="${base_prefix}/snapshot"
  local fr_ws_location="${base_prefix}/fr_ws"
  local static_dir="${TARGET_DIR}/www/"
  local fr_port=$((PORT + 1))

  begin_marker="# BEGIN managed: fr viz ${base_prefix}"
```

- [ ] **Step 2: 在 awk 命令的 -v 参数列表追加 fr_ws_location 和 fr_port**

修改 line 134-143：

```bash
# 变更前
  awk -v begin="$begin_marker" \
      -v end="$end_marker" \
      -v prefix="$base_prefix" \
      -v static_prefix="$static_prefix" \
      -v static_dir="$static_dir" \
      -v ws_location="$ws_location" \
      -v health_location="$health_location" \
      -v snapshot_location="$snapshot_location" \
      -v port="$PORT" \
      -v ws_path="$ws_path" '

# 变更后
  awk -v begin="$begin_marker" \
      -v end="$end_marker" \
      -v prefix="$base_prefix" \
      -v static_prefix="$static_prefix" \
      -v static_dir="$static_dir" \
      -v ws_location="$ws_location" \
      -v health_location="$health_location" \
      -v snapshot_location="$snapshot_location" \
      -v fr_ws_location="$fr_ws_location" \
      -v port="$PORT" \
      -v fr_port="$fr_port" \
      -v ws_path="$ws_path" '
```

- [ ] **Step 3: 在 awk 替换路径输出 fr_ws 行**

修改 line 144-156（replace 分支）。在 `print snapshot_location ...` 后追加一行 fr_ws，并把头部注释行同步更新：

```awk
# 变更前
    in_block && $0 == end {
        in_block = 0;
        print begin;
        print "# fr pre_trade dashboard (static) + viz_server (WS/healthz/snapshot)";
        print static_prefix " static:" static_dir;
        print ws_location " http://127.0.0.1:" port ws_path;
        print health_location " http://127.0.0.1:" port "/healthz";
        print snapshot_location " http://127.0.0.1:" port "/snapshot";
        print end;
        next
    }

# 变更后
    in_block && $0 == end {
        in_block = 0;
        print begin;
        print "# fr pre_trade dashboard (static) + viz_server (WS/healthz/snapshot) + fr_signal_dashboard (fr_ws)";
        print static_prefix " static:" static_dir;
        print ws_location " http://127.0.0.1:" port ws_path;
        print health_location " http://127.0.0.1:" port "/healthz";
        print snapshot_location " http://127.0.0.1:" port "/snapshot";
        print fr_ws_location " http://127.0.0.1:" fr_port "/ws";
        print end;
        next
    }
```

- [ ] **Step 4: 在 awk append 路径同样输出 fr_ws 行**

修改 line 167-178（END 分支）：

```awk
# 变更前
    END {
        if (!replaced) {
            print "";
            print begin;
            print "# fr pre_trade dashboard (static) + viz_server (WS/healthz/snapshot)";
            print static_prefix " static:" static_dir;
            print ws_location " http://127.0.0.1:" port ws_path;
            print health_location " http://127.0.0.1:" port "/healthz";
            print snapshot_location " http://127.0.0.1:" port "/snapshot";
            print end;
        }
    }

# 变更后
    END {
        if (!replaced) {
            print "";
            print begin;
            print "# fr pre_trade dashboard (static) + viz_server (WS/healthz/snapshot) + fr_signal_dashboard (fr_ws)";
            print static_prefix " static:" static_dir;
            print ws_location " http://127.0.0.1:" port ws_path;
            print health_location " http://127.0.0.1:" port "/healthz";
            print snapshot_location " http://127.0.0.1:" port "/snapshot";
            print fr_ws_location " http://127.0.0.1:" fr_port "/ws";
            print end;
        }
    }
```

- [ ] **Step 5: 验证脚本语法**

```bash
bash -n scripts/deploy_fr_viz_server.sh
echo "exit=$?"
```

Expected: 退出码 0。

- [ ] **Step 6: 提交**

```bash
git add scripts/deploy_fr_viz_server.sh
git commit -m "feat(fr-deploy): add /fr_ws reverse proxy at viz_port+1 in nginx upsert

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: dry-run 验证 nginx mapping idempotent + 多 env-name 端口隔离

**Files:**
- Test: 临时 `/tmp/test_fr_nginx_*.txt`，无新增文件

- [ ] **Step 1: 准备空 mapping 文件，跑首次 deploy 触发 append 分支**

```bash
TMPDIR_T4="$(mktemp -d)"
TMP_MAPPING="${TMPDIR_T4}/nginx_locations.txt"

: > "$TMP_MAPPING"

scripts/deploy_fr_viz_server.sh \
  --env-name binance_fr_test01 \
  --port 10031 \
  --nginx-prefix /fr/binance_fr_test01 \
  --nginx-mapping-file "$TMP_MAPPING" \
  --scripts-only

cat "$TMP_MAPPING"
```

Expected: 包含一段 managed block，6 行内容，最后一行：
```
/fr/binance_fr_test01/fr_ws http://127.0.0.1:10032/ws
```

- [ ] **Step 2: grep 验证 fr_ws 行端口为 viz_port + 1 = 10032**

```bash
grep -E "/fr/binance_fr_test01/fr_ws[[:space:]]+http://127\.0\.0\.1:10032/ws" "$TMP_MAPPING"
echo "exit=$?"
```

Expected: 退出码 0，匹配到一行。

- [ ] **Step 3: 添加第二个 env-name 测试隔离**

```bash
scripts/deploy_fr_viz_server.sh \
  --env-name binance_fr_test02 \
  --port 10041 \
  --nginx-prefix /fr/binance_fr_test02 \
  --nginx-mapping-file "$TMP_MAPPING" \
  --scripts-only

# 两段 managed block 应共存
grep -c "BEGIN managed: fr viz" "$TMP_MAPPING"
```

Expected: 输出 `2`（两段 block）。

```bash
grep -E "/fr/binance_fr_test02/fr_ws[[:space:]]+http://127\.0\.0\.1:10042/ws" "$TMP_MAPPING"
echo "exit=$?"
```

Expected: 退出码 0（test02 端口 10042 = 10041 + 1）。

- [ ] **Step 4: 验证 idempotent —— 重跑 test01 同参数，文件不变**

```bash
md5_before="$(md5sum "$TMP_MAPPING" | awk '{print $1}')"
scripts/deploy_fr_viz_server.sh \
  --env-name binance_fr_test01 \
  --port 10031 \
  --nginx-prefix /fr/binance_fr_test01 \
  --nginx-mapping-file "$TMP_MAPPING" \
  --scripts-only
md5_after="$(md5sum "$TMP_MAPPING" | awk '{print $1}')"
[[ "$md5_before" == "$md5_after" ]] && echo "IDEMPOTENT" || echo "DRIFT"
```

Expected: 输出 `IDEMPOTENT`。

- [ ] **Step 5: 验证端口替换 —— 改 test01 的 viz_port，fr_ws 端口跟着变**

```bash
scripts/deploy_fr_viz_server.sh \
  --env-name binance_fr_test01 \
  --port 10071 \
  --nginx-prefix /fr/binance_fr_test01 \
  --nginx-mapping-file "$TMP_MAPPING" \
  --scripts-only

grep -E "/fr/binance_fr_test01/fr_ws[[:space:]]+http://127\.0\.0\.1:10072/ws" "$TMP_MAPPING"
echo "exit=$?"

# 旧端口 10032 应已消失
grep -c "fr_ws http://127.0.0.1:10032/ws" "$TMP_MAPPING"
```

Expected: 第一个 `exit=0`（新端口 10072 出现），第二个输出 `0`（旧端口已被替换）。

- [ ] **Step 6: 清理**

```bash
rm -rf "$TMPDIR_T4"
rm -rf "$HOME/binance_fr_test01" "$HOME/binance_fr_test02"
```

- [ ] **Step 7: 无需提交（仅验证）**

---

### Task 5: 本地 mock 浏览器验证 demo 仍工作

**Files:**
- Test: 浏览器手动验证 `docs/fr_pre_trade_dashboard.html`

- [ ] **Step 1: 起本地静态 server**

```bash
cd /home/fanghaizhou/mkt_signal/docs
python3 -m http.server 8765 >/dev/null 2>&1 &
PYHTTP_PID=$!
sleep 1
echo "PID=$PYHTTP_PID"
```

Expected: 输出 PID 数字。

- [ ] **Step 2: 浏览器手动验证（用户操作）**

打开 `http://localhost:8765/fr_pre_trade_dashboard.html`。检查：

| 检查项 | 预期 |
|---|---|
| 标题旁紫色 `DEMO` 徽章 | ✓ |
| 两路指示灯都显示 `MOCK` | ✓ |
| Summary 信号 chips 7 个，数字非 `--` | ✓ |
| 风险卡片 5 个 + chips 4 个，**无双腿子卡** | ✓ |
| 阈值图例抽屉 `(20)` 折叠态 | ✓ |
| Table 1 显示 5 行，共 20 列，FR 列底色淡蓝 | ✓ |
| hits 列 = 4 个 12px 色块，无字母 | ✓ |
| Table 2 默认折叠，展开后显示 ~18 行 | ✓ |
| 过滤框输入 `BTC` → 两表只剩 BTC 行 | ✓ |
| 抽屉折叠状态 F5 后保持 | ✓ |

- [ ] **Step 3: 关闭本地 server**

```bash
kill "$PYHTTP_PID" 2>/dev/null
wait "$PYHTTP_PID" 2>/dev/null
unset PYHTTP_PID
```

- [ ] **Step 4: 无需提交**

---

### Task 6: 部署到测试 env-name 并 live 端到端验证

**前置条件：** Task 1-5 完成；选定一个测试 env-name（如 `binance_fr_test01`）；该 env-name 的 trade_signal / pre_trade / trade_engine 已部署并运行；选定 viz_port（如 10071）。

**Files:**
- 无代码变更，仅运行部署脚本和浏览器验证

- [ ] **Step 1: 按部署顺序跑两个脚本**

```bash
# 先部署 fr_signal_dashboard（使用 viz-port 10071，自动算 fr_dash port = 10072）
scripts/deploy_fr_signal_dashboard.sh \
  --env-name binance_fr_test01 \
  --viz-port 10071

# 再部署 viz_server（同一 viz-port）
scripts/deploy_fr_viz_server.sh \
  --env-name binance_fr_test01 \
  --port 10071 \
  --nginx-prefix /fr/binance_fr_test01 \
  --apply-nginx
```

Expected:
- 第一步：输出 `[INFO] deployed fr_signal_dashboard to $HOME/binance_fr_test01 (port=10072, viz_port=10071)`
- 第二步：输出 `[INFO] Synced dashboard: ... (FR 二合一)` 和 `[INFO] nginx mapping updated: ...`
- nginx reload 成功

- [ ] **Step 2: 验证两个 PM2 进程运行**

```bash
npx pm2 list | grep -E "binance_fr_test01"
```

Expected: 至少看到 `binance_fr_test01_fr_signal_dashboard`（namespace `binance_fr_test01`，online）和 viz_server 进程。

- [ ] **Step 3: 验证 nginx_locations.txt**

```bash
grep "/fr/binance_fr_test01" "$HOME/nginx_locations.txt"
```

Expected: 5 行匹配（static / ws / healthz / snapshot / fr_ws），fr_ws 端口为 10072。

- [ ] **Step 4: curl 验证两路 HTTP 端点**

```bash
curl -sf http://127.0.0.1:10071/healthz | head -c 200
echo ""
curl -sf http://127.0.0.1:10072/healthz | head -c 200
echo ""
curl -sf "http://127.0.0.1:4191/fr/binance_fr_test01/snapshot" | head -c 200
echo ""
```

Expected: 三个 curl 都返回非空 JSON。`/healthz` 返回 `{"ok":true,...}` 形态。

- [ ] **Step 5: 浏览器 live 模式端到端验证**

打开 `http://<host>:4191/fr/binance_fr_test01/?live=1`。检查：

| 检查项 | 预期 |
|---|---|
| 徽章变为绿色 `LIVE` | ✓ |
| 两路指示灯 5s 内变绿 | ✓ |
| Summary 风险卡片显示真实账户数据 | ✓ |
| Table 1 行数 = 真实持仓资产数 | ✓ |
| Table 1 持仓资产 FR 叠加列有数据 | ✓ |
| 阈值图例展开 = 真实 Redis 阈值（与 fr_signal_dashboard 旧 UI 一致） | ✓ |
| Table 2 展开 = 评估中 symbol | ✓ |
| DevTools Network: `./ws` 连到 viz_server (10071) 且 `./fr_ws` 连到 fr_signal_dashboard (10072)，都是 101 | ✓ |

- [ ] **Step 6: 多 env-name 并存测试（可选）**

如果有第二个测试 env-name（如 `binance_fr_test02` viz=10081）：

```bash
scripts/deploy_fr_signal_dashboard.sh --env-name binance_fr_test02 --viz-port 10081
scripts/deploy_fr_viz_server.sh --env-name binance_fr_test02 --port 10081 \
  --nginx-prefix /fr/binance_fr_test02 --apply-nginx
```

浏览器分别访问 `/fr/binance_fr_test01/?live=1` 和 `/fr/binance_fr_test02/?live=1`，应显示两套独立数据（symbol 集合 / 阈值 / 持仓互不影响）。

- [ ] **Step 7: 降级测试**

```bash
npx pm2 stop binance_fr_test01_fr_signal_dashboard
```

刷新浏览器：

| 检查项 | 预期 |
|---|---|
| `fr` 指示灯转红 | ✓ |
| Table 1 仍显示持仓行，FR 叠加列变 `—` | ✓ |
| 阈值图例 `(0)` | ✓ |
| Table 2 折叠态 `(0)` 或空 | ✓ |
| `viz` 指示灯仍绿 | ✓ |

恢复：

```bash
npx pm2 start binance_fr_test01_fr_signal_dashboard
```

5s 内 `fr` 指示灯应自动变绿。

- [ ] **Step 8: 无需提交（仅部署 + 验证）**

---

## 完成判据

- [ ] Task 1-3 三个 commit 都在分支上
- [ ] Task 4 dry-run 验证全过（IDEMPOTENT + 多 env-name 端口隔离正确 + 端口替换正确）
- [ ] Task 5 本地 mock 浏览器验证全过
- [ ] Task 6 至少一个测试 env-name 的 live 模式两路 ws 都连上、降级测试通过

## 不在范围（再次声明）

- `fr_config_server.py` 改造：单独 spec
- 任何 Rust 代码改动 / 新 binary / `viz.toml` 改动
- 多 quote（非 USDT）支持、列搜索/排序、行级过滤
- 配置文件 indirection（dashboard.env source 等）：本次约定 hardcode `viz_port + 1` 即可
