# FR Pre-Trade Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把已实现的 `docs/fr_pre_trade_dashboard.html`（二合一 board）接到 FR 套利的部署链路：让 `scripts/deploy_fr_viz_server.sh` 拷贝新 HTML 并在 nginx 里加 `/fr_ws` 反代到 `fr_signal_dashboard`，使 `?live=1` 可在生产环境工作。

**Architecture:** 后端零改动。仅修改一个 shell 脚本（`deploy_fr_viz_server.sh`）。新增 `--fr-dashboard-port` CLI 参数（默认 6305，与 `deploy_fr_signal_dashboard.sh` 一致）；HTML 拷贝段优先用 `fr_pre_trade_dashboard.html`，回退到通用版；nginx awk upsert 段在 `/ws` `/snapshot` `/healthz` 三行后加一行 `/fr_ws`。

**Tech Stack:** Bash, awk, nginx config (managed block in `nginx_locations.txt`)。HTML demo 已存在并 commit（`890b762`）。

**Spec:** `docs/superpowers/specs/2026-05-07-fr-pre-trade-dashboard-design.md`

**Files:**
- Modify: `scripts/deploy_fr_viz_server.sh`
  - 变量声明区（line 38-58）：加 `FR_DASHBOARD_PORT="6305"`
  - usage 文本（line 7-31）：加 `--fr-dashboard-port`
  - awk upsert 段（line 134-178）：加 fr_ws 反代输出（两处：replace 路径 line 149-153 + append 路径 line 171-175）
  - CLI 解析 case 块（line 183-263）：加 `--fr-dashboard-port` 分支
  - HTML 拷贝段（line 396-402）：优先用 `docs/fr_pre_trade_dashboard.html`

无 Rust 代码改动，无新增文件，无 viz.toml 改动。

---

### Task 1: 加 `--fr-dashboard-port` CLI 参数与默认值

**Files:**
- Modify: `scripts/deploy_fr_viz_server.sh:38-58, 7-31, 183-263`

- [ ] **Step 1: 加变量默认值**

修改 `scripts/deploy_fr_viz_server.sh` 的变量声明区（在 `APPLY_NGINX="0"` 之后插入）：

```bash
# 变更前（line 55-58）
NGINX_PREFIX=""
NGINX_PORT="4191"
NGINX_MAPPING_FILE=""
APPLY_NGINX="0"

# 变更后
NGINX_PREFIX=""
NGINX_PORT="4191"
NGINX_MAPPING_FILE=""
APPLY_NGINX="0"

FR_DASHBOARD_PORT="6305"
```

- [ ] **Step 2: 在 usage 文本里加新参数说明**

修改 `usage()` 内的 heredoc，把 `[--apply-nginx]` 行后面加一行：

```bash
# 变更前（line 18-19）
                                      [--apply-nginx]
                                      [--scripts-only|--bin-only]

# 变更后
                                      [--apply-nginx]
                                      [--fr-dashboard-port 6305]
                                      [--scripts-only|--bin-only]
```

并在 Notes 段（line 28 附近）追加一条：

```bash
  - Updates nginx mapping file with static + ws + healthz entries (managed block).
  - --fr-dashboard-port (default 6305) is the port of the standalone fr_signal_dashboard
    process (deploy_fr_signal_dashboard.sh). nginx will reverse-proxy /fr_ws to it.
EOF
```

- [ ] **Step 3: 加 CLI 解析分支**

在 `--apply-nginx)` case 之后（line 233-236 之间）插入：

```bash
    --apply-nginx)
      APPLY_NGINX="1"
      shift
      ;;
    --fr-dashboard-port)
      FR_DASHBOARD_PORT="${2:-6305}"
      shift 2
      ;;
    --scripts-only)
```

- [ ] **Step 4: 验证脚本语法**

```bash
bash -n scripts/deploy_fr_viz_server.sh
echo "exit=$?"
```

Expected: 退出码 0，无输出。

- [ ] **Step 5: 验证 --help 渲染**

```bash
scripts/deploy_fr_viz_server.sh --help 2>&1 | grep -E "fr-dashboard-port|6305"
```

Expected: 至少 2 行输出（usage 行和 Notes 行）。

- [ ] **Step 6: 提交**

```bash
git add scripts/deploy_fr_viz_server.sh
git commit -m "feat(fr-deploy): add --fr-dashboard-port CLI arg (default 6305)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: HTML 拷贝段优先 `fr_pre_trade_dashboard.html`

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

- [ ] **Step 3: 验证文件确实存在**

```bash
ls -la docs/fr_pre_trade_dashboard.html
```

Expected: 文件存在，大小 > 20KB（demo 已 commit 在 `890b762`）。

- [ ] **Step 4: 提交**

```bash
git add scripts/deploy_fr_viz_server.sh
git commit -m "feat(fr-deploy): prefer fr_pre_trade_dashboard.html in www/ copy

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: nginx awk upsert 段加 `/fr_ws` 反代

**Files:**
- Modify: `scripts/deploy_fr_viz_server.sh:134-178`

- [ ] **Step 1: 在 awk 命令的 -v 参数后追加 fr 变量**

修改 line 134-143。在 `-v ws_path="$ws_path" '` 前增加两行变量传递：

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
  local fr_ws_location="${base_prefix}/fr_ws"
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
      -v fr_port="$FR_DASHBOARD_PORT" \
      -v ws_path="$ws_path" '
```

- [ ] **Step 2: 在 awk 替换路径输出 fr_ws 行**

修改 line 144-156（replace 分支）。在 `print snapshot_location ...` 后追加一行 fr_ws：

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

- [ ] **Step 3: 在 awk append 路径同样输出 fr_ws 行**

修改 line 167-178（END 分支）。同样在 `print snapshot_location ...` 后追加 fr_ws：

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

- [ ] **Step 4: 验证脚本语法**

```bash
bash -n scripts/deploy_fr_viz_server.sh
echo "exit=$?"
```

Expected: 退出码 0。

- [ ] **Step 5: 提交**

```bash
git add scripts/deploy_fr_viz_server.sh
git commit -m "feat(fr-deploy): add /fr_ws reverse proxy to fr_signal_dashboard in nginx upsert

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: dry-run 验证 nginx mapping 输出

测试 `upsert_main_nginx_mapping` 在隔离临时文件上的行为：append 路径（首次写入）和 replace 路径（已有 managed block 时 idempotent 替换）。

**Files:**
- Test: 临时 `/tmp/test_fr_nginx_*.txt`，无新增文件

- [ ] **Step 1: 准备空 mapping 文件，跑 scripts-only 触发 append 分支**

```bash
TMPDIR_T4="$(mktemp -d)"
TMP_MAPPING="${TMPDIR_T4}/nginx_locations.txt"
TMP_TARGET="${TMPDIR_T4}/binance_fr_test"

# 空文件，触发 append
: > "$TMP_MAPPING"

scripts/deploy_fr_viz_server.sh \
  --env-name binance_fr_test \
  --port 10031 \
  --nginx-prefix /fr/binance_fr_test \
  --nginx-mapping-file "$TMP_MAPPING" \
  --scripts-only

# 注：脚本会写到 $HOME/<env-name>/，不会污染 mapping 文件之外的位置
# 用 cat 看输出
cat "$TMP_MAPPING"
```

Expected: 末尾有一段 managed block，包含 6 行内容：
```
# BEGIN managed: fr viz /fr/binance_fr_test
# fr pre_trade dashboard (static) + viz_server (WS/healthz/snapshot) + fr_signal_dashboard (fr_ws)
/fr/binance_fr_test/ static:.../www/
/fr/binance_fr_test/ws http://127.0.0.1:10031/ws
/fr/binance_fr_test/healthz http://127.0.0.1:10031/healthz
/fr/binance_fr_test/snapshot http://127.0.0.1:10031/snapshot
/fr/binance_fr_test/fr_ws http://127.0.0.1:6305/ws
# END managed: fr viz /fr/binance_fr_test
```

- [ ] **Step 2: grep 验证 fr_ws 行存在且端口正确**

```bash
grep -E "/fr/binance_fr_test/fr_ws[[:space:]]+http://127\.0\.0\.1:6305/ws" "$TMP_MAPPING"
echo "exit=$?"
```

Expected: 退出码 0，匹配到一行。

- [ ] **Step 3: 用 --fr-dashboard-port 覆盖默认端口，重跑**

```bash
scripts/deploy_fr_viz_server.sh \
  --env-name binance_fr_test \
  --port 10031 \
  --nginx-prefix /fr/binance_fr_test \
  --nginx-mapping-file "$TMP_MAPPING" \
  --fr-dashboard-port 7777 \
  --scripts-only

# 这次走 replace 分支（block 已存在）
grep -E "/fr/binance_fr_test/fr_ws[[:space:]]+http://127\.0\.0\.1:7777/ws" "$TMP_MAPPING"
echo "exit=$?"
```

Expected: 退出码 0，端口已更新为 7777。同时确认旧 6305 行已被替换（不应共存）：

```bash
grep -c "fr_ws http://127.0.0.1:6305/ws" "$TMP_MAPPING"
```

Expected: 输出 `0`（旧行已被覆盖，不再存在）。

- [ ] **Step 4: 验证 idempotent —— 再跑一次同参数，输出应不变**

```bash
md5_before="$(md5sum "$TMP_MAPPING" | awk '{print $1}')"
scripts/deploy_fr_viz_server.sh \
  --env-name binance_fr_test \
  --port 10031 \
  --nginx-prefix /fr/binance_fr_test \
  --nginx-mapping-file "$TMP_MAPPING" \
  --fr-dashboard-port 7777 \
  --scripts-only
md5_after="$(md5sum "$TMP_MAPPING" | awk '{print $1}')"
[[ "$md5_before" == "$md5_after" ]] && echo "IDEMPOTENT" || echo "DRIFT"
```

Expected: 输出 `IDEMPOTENT`。

- [ ] **Step 5: 清理临时目录**

```bash
rm -rf "$TMPDIR_T4"
# 同时清理 deploy 副产物：$HOME/binance_fr_test
rm -rf "$HOME/binance_fr_test"
```

Expected: 无报错。

- [ ] **Step 6: 无需提交（仅验证）**

无代码变更。

---

### Task 5: 本地 mock 验证 demo 仍工作

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
| 两路指示灯都显示 `MOCK`（绿） | ✓ |
| Summary 信号 chips 7 个（active/fwd_o/fwd_c/bwd_o/bwd_c/neutral/total），数字非 `--` | ✓ |
| 风险卡片 5 个 + chips 4 个，**无双腿子卡** | ✓ |
| 阈值图例抽屉显示 `(20)` 折叠态 | ✓ |
| Table 1 显示 5 行（BTC/ETH/SOL/DOGE/AVAX），共 20 列，FR 列底色淡蓝 | ✓ |
| hits 列 = 4 个 12px 色块，无字母，命中按方向着色 | ✓ |
| Table 2 默认折叠，点击展开后显示 ~18 行评估中 symbol | ✓ |
| 过滤框输入 `BTC` → 两表只剩 BTC 行 | ✓ |
| 抽屉折叠状态在 F5 刷新后保持（localStorage） | ✓ |

- [ ] **Step 3: 关闭本地 server**

```bash
kill "$PYHTTP_PID" 2>/dev/null
wait "$PYHTTP_PID" 2>/dev/null
unset PYHTTP_PID
```

Expected: 进程结束。

- [ ] **Step 4: 无需提交（仅验证）**

---

### Task 6: 部署到测试 venue 并 live 端到端验证

**前置条件：** Task 1-5 完成；选定一个 FR 测试 venue（建议 `binance_fr_test` 或一个非生产 env-name）；该 venue 的 `fr_signal_dashboard` 进程已在 6305 端口运行。

**Files:**
- 无代码变更，仅运行部署脚本和浏览器验证

- [ ] **Step 1: 跑 deploy 脚本（带 --apply-nginx）**

```bash
scripts/deploy_fr_viz_server.sh \
  --env-name <chosen_env> \
  --port <viz_port> \
  --nginx-prefix /fr/<chosen_env> \
  --apply-nginx
```

Expected:
- `[INFO] Synced dashboard: ... (FR 二合一)` 行出现
- `[INFO] nginx mapping updated: ...` 行出现
- nginx reload 成功（无 `nginx -t` 错误）

- [ ] **Step 2: 验证 nginx_locations.txt 含 fr_ws 行**

```bash
grep "/fr/<chosen_env>/fr_ws" "$HOME/nginx_locations.txt"
```

Expected: 一行匹配，端口为 6305（或显式传入的 `--fr-dashboard-port`）。

- [ ] **Step 3: curl 测试 fr_ws 反代连通**

```bash
# 先确认 fr_signal_dashboard 在 6305 上是 healthy
curl -sf http://127.0.0.1:6305/healthz | head -c 200
echo ""

# 通过 nginx 反代访问（端口取决于 NGINX_PORT，默认 4191）
curl -sf "http://127.0.0.1:4191/fr/<chosen_env>/snapshot" | head -c 200
echo ""
```

Expected: 两个 curl 都返回非空 JSON（healthz 返回 `{"ok":true,...}`；snapshot 返回 viz_server 的 cached snapshot）。

注：fr_ws 是 WebSocket，不能直接 curl 测，下一步用浏览器验证。

- [ ] **Step 4: 浏览器 live 模式端到端验证**

打开 `http://<host>:4191/fr/<chosen_env>/?live=1`。检查：

| 检查项 | 预期 |
|---|---|
| 徽章变为绿色 `LIVE` | ✓ |
| 两路指示灯 5s 内变绿 | ✓ |
| Summary 风险卡片显示真实账户数据（equity / leverage） | ✓ |
| Table 1 行数 = 真实持仓资产数 | ✓ |
| Table 1 的 FR 叠加列对持仓资产显示数据，否则 `—` | ✓ |
| 阈值图例展开后 = 真实 Redis 中阈值（与 fr_signal_dashboard 旧 UI 一致） | ✓ |
| Table 2 展开后 = 评估中 symbol 列表 | ✓ |
| 浏览器 DevTools Network 看到两路 WebSocket 连接：`./ws` 和 `./fr_ws`，都是 101 状态 | ✓ |

- [ ] **Step 5: 关停 fr_signal_dashboard 测试降级**

```bash
# 在另一个终端
npx pm2 stop fr_signal_dashboard
```

刷新浏览器，检查：

| 检查项 | 预期 |
|---|---|
| `fr` 指示灯变红 | ✓ |
| Table 1 仍显示持仓行，但 FR 叠加列变为 `—` | ✓ |
| 阈值图例 `(0)` | ✓ |
| Table 2 折叠态显示 `(0)` 或空 | ✓ |
| `viz` 指示灯仍绿 | ✓ |

恢复：

```bash
npx pm2 start fr_signal_dashboard
```

5s 内 `fr` 指示灯应自动变绿。

- [ ] **Step 6: 无需提交（仅部署 + 验证）**

部署副产物在 `$HOME/<chosen_env>/`，不进 git。

---

## 完成判据

- [ ] Task 1-3 三个 commit 都在分支上
- [ ] Task 4 dry-run 验证全过（IDEMPOTENT + 端口替换正确）
- [ ] Task 5 本地 mock 浏览器验证全过
- [ ] Task 6 至少一个测试 venue 的 live 模式两路 ws 都连上、Table 1 与 Table 2 数据一致

## 不在范围（再次声明）

- `fr_config_server.py` 改造：单独 spec
- 任何 Rust 代码改动 / 新 binary / `viz.toml` 改动
- 多 quote（非 USDT）支持、列搜索/排序、行级过滤
