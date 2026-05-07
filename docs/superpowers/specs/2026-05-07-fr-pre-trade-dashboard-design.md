# FR Pre-Trade Dashboard（二合一）

**日期**: 2026-05-07
**目标**: 给资金费率（FR）套利做一个浏览器面板，把 intra dashboard 的实时持仓 / 对冲 / 风险信息和现有 `fr_signal_dashboard` 的信号视角（4 条规则、阈值、pred_fr/loan）合并到同一页面。架构与 intra/cross dashboard 对齐：单 HTML 文件、由 nginx serve、前端连相对路径 ws；后端 Rust 代码零改动；同时把 `fr_signal_dashboard` 进程改为 per-env-name 部署，与 viz_server 同 dir 共生。

## 背景与现状

| 数据 | 现状 | 谁在用 |
|---|---|---|
| intra 风格的 exposure + risk（`PreTradeExposureResampleEntry` / `PreTradeRiskResampleEntry`） | ✅ FR 套利的 pre_trade 已发布到 iceoryx2，`viz_server` 已转发；`config/viz.toml` 已为 `okex_fr_trade` / `gate_fr_trade` / `binance_fr_trade` 三个实例配好 | intra/cross dashboard |
| FR 信号字段（`FrDashboardSnapshot`：funding_rate / predicted / ma / loan / 4 条规则 hit / 阈值图例 / signal summary） | ✅ `fr_signal_dashboard` 进程在跑，自带 axum + WS（`/ws`） | fr_signal_dashboard 内嵌的旧 HTML |

## 多策略部署模型（新章节）

### 现状的部署单元

每套 FR 策略 = 一个 `env-name`（如 `binance_fr_trade01` / `_trade02` / `_trade03`），含一组进程：
- `trade_signal` / `pre_trade` / `trade_engine` / `viz_server`
- 这些进程都部署在 `$HOME/<env-name>/`，PM2 namespace = env-name
- 同一台机器可并跑多个 env-name（symbol 集合、阈值各自独立）
- `viz_server` 的端口由 `deploy_fr_viz_server.sh --port` 显式分配

`fr_signal_dashboard` 也是 per-strategy 的：从 CWD 推断 `(symbol_namespace, symbol_key_suffix, exchange, open_venue, hedge_venue)`（src/bin/fr_signal_dashboard.rs:119），每个进程绑定一对 venue + 一组 symbol_list。但当前 `deploy_fr_signal_dashboard.sh` 把它部署到全局 `/ubuntu/dashboard` + 端口 6305，**不支持多 env-name 共存**。本 spec 把它改为 per-env-name。

### 端口约定

```
fr_dashboard_port = viz_port + 1
```

- `viz_port` 由 `deploy_fr_viz_server.sh --port` 决定（已有 CLI 参数）
- `fr_dashboard_port` 不需要单独 CLI 参数，两个 deploy 脚本各自按 `viz_port + 1` 算
- `dashboard.env` 仍保留（fr_signal_dashboard 进程启动时需要 `FR_DASHBOARD_PORT` 等环境变量），但只由 `deploy_fr_signal_dashboard.sh` 写、由 `start_fr_signal_dashboard.sh` 读。`deploy_fr_viz_server.sh` 不读取它，自己计算 `viz_port + 1` 用于 nginx 反代

### 部署顺序

```
1. scripts/deploy_fr_signal_dashboard.sh --env-name binance_fr_trade01 \
     --exchange binance --viz-port 10031          # → fr_dash 端口 10032，部署到 $HOME/binance_fr_trade01/
2. scripts/deploy_fr_viz_server.sh --env-name binance_fr_trade01 \
     --port 10031 --apply-nginx                    # nginx /fr_ws 反代到 127.0.0.1:10032
```

两个脚本都接 `--env-name` 与 `--viz-port` / `--port`（同一含义不同名以匹配各自语义）；fr_dashboard_port 在两边都按 `viz_port + 1` 推算，无需文件 source。

## 不做（明确边界）

- 不动 Rust 代码：不改 `viz_server`、`fr_signal_dashboard`、`pre_trade`，不改 `viz.toml`，不发布 `FundingRateArbResampleEntry`（dead struct，留给未来）
- 不删旧 fr_signal_dashboard 内嵌 HTML：保留作为信号视角的纯净视图与降级备份
- 不做 `fr_config_server.py` 改造：单独下次 spec
- 不做行级搜索 / 列筛选 / 列点击排序 / 多 quote（非 USDT）支持：留给后续迭代
- 不做 MM 路径的 `hedge_offset_low/high` 展示：FR 套利只走 arb hedge 单档报单（与 intra 一致）
- 不引入 sibling-env source / 配置文件 indirection；端口约定 hardcode 在两脚本

## 架构与数据流

```
       浏览器（fr_pre_trade_dashboard.html，单文件 demo 已实现）
                │ 双 WS 长连接 + 各自独立的状态指示灯
        ┌───────┴───────┐
        ▼               ▼
    ./ws  (viz)     ./fr_ws  (fr)
        │               │
        │ nginx prefix/ws → 127.0.0.1:viz_port
        │ nginx prefix/fr_ws → 127.0.0.1:(viz_port + 1)
        ▼               ▼
   viz_server     fr_signal_dashboard
   (无 Rust 改动)  (无 Rust 改动；但 deploy 脚本改为 per-env-name)
        │               │
        │ iceoryx2      │ 1s 轮询
        │ exposure/risk │ funding_rate + ArbDecision
        ▼               ▼
   pre_trade FR    funding rate / predicted / MA /
   resample        loan / 4 条规则 hit
```

## 数据契约与合并规则

### 来自 viz_server 的消息（已存在）

```json
{ "type": "pre_trade_exposure", "namespace": "...", "channel": "...", "ts_ms": ..., "entry": { "ts_ms": ..., "rows": PreTradeExposureRow[] } }
{ "type": "pre_trade_risk",     "namespace": "...", "channel": "...", "ts_ms": ..., "entry": PreTradeRiskResampleEntry }
```

`PreTradeExposureRow` / `PreTradeRiskResampleEntry` 字段定义见 `src/viz/resample.rs`。

### 来自 fr_signal_dashboard 的消息（已存在）

整个 `FrDashboardSnapshot` 的 JSON，定义见 `src/fr_signal_dashboard.rs:55`：
- `summary: FrDashboardSummary` — 信号汇总（active_signals / forward_open / forward_close / backward_open / backward_close / neutral / total_symbols）
- `thresholds: FrDashboardThresholdLegend[]` — period × signal × {compare_op, expression, threshold_pct, tone}
- `rows: FrDashboardRow[]` — per-symbol（symbol / period / signal / latest_fr_pct / pred_fr_pct / fr_ma_pct / cur_loan_pct / pred_loan_pct / fr_plus_pred_loan_pct / ma_plus_cur_loan_pct / rules: FrDashboardRuleState[]）

### 合并规则

- **合并 key**: `base = symbol.replace(/USDT$/i, "").replace(/_PERP$/i, "").toUpperCase()`。MVP 假设 FR 套利 quote 全是 USDT，遇到非 USDT 在 console.warn 后跳过 FR 列叠加。
- **Table 1（已持仓）行集合** = viz exposure 中 `is_total === false` 且 `(open_qty || hedge_qty || arb_hedge_net_qty) ≠ 0` 的行，按 `|net_usdt|` 降序；右侧按 `base` 叠加同 base 的 fr row 字段，缺失时整组 FR 列显示 `—` 并打 `class="missing"`。
- **Table 2（评估中）行集合** = fr `rows` 中 `base` 不在 Table 1 的子集；排序：命中规则的优先（按 RULE_ORDER 索引），其次按 `|latest_fr_pct|` 降序。
- **过滤框**: 同时按 `r.asset` 和 `r.symbol` 大写包含匹配，作用于两表。

### 状态独立 / 降级

- 两路 ws 各自独立的指示灯（`viz` / `fr`），任一断开自动 5s 重连，不影响另一路渲染。
- 任一路缺失 → 对应区块字段显示 `--`，整体不报错。

## UI 布局规约

锚点：已实现的 demo `docs/fr_pre_trade_dashboard.html`（mock 模式）。布局自上而下：

1. **Header**: 标题 + DEMO/LIVE 徽章 + 两路 ws 指示灯 + 最后消息时间 + 过滤框 + 重连按钮
2. **Summary section**:
   - 信号 chips 一排（active / fwd_o / fwd_c / bwd_o / bwd_c / neutral / total），来源 `FrDashboardSnapshot.summary`，命中色调来源：fwd_open=橙、fwd_close=蓝、bwd_open=紫、bwd_close=绿
   - 风险卡片 5 个 + chips 4 个（与 intra 一致，**不再展示双腿子卡**）
3. **阈值图例抽屉**（默认折叠）：5 period × 4 signal = 20 张 thr-card，沿用 fr_signal_dashboard 现有图例配色
4. **Table 1 已持仓**（默认展开）：分组表头（敞口 6 列 / 对冲 4 列 / FR 信号 9 列），FR 叠加列底色淡蓝（`background: rgba(88, 166, 255, 0.04)`）
5. **Table 2 评估中**（默认折叠，点击展开）：仅 fr 字段 11 列；展开/折叠状态写入 `localStorage.fr_drawer_eval`

### 字段表

#### Table 1 — 已持仓（每行 = 一个 asset）

| 列分组 | 列 | 来源 |
|---|---|---|
| 标识 | `asset` | viz |
| 敞口 (6) | `open_qty / open_usdt / hedge_qty / hedge_usdt / net_qty / net_usdt` | viz |
| 对冲 (4) | `arb_time = arb_hedge_time_ms`（UTC+8 HH:MM:SS）/ `mode = arb_hedge_is_taker ? "taker" : "maker"` / `ret_qtl = arb_hedge_ret_qtl` / `offset = arb_hedge_offset` | viz |
| FR 信号 (9) | `period / latest_fr% / pred_fr% / fr_ma% / cur_loan% / pred_loan% / fr+pred_loan / ma+cur_loan / hits` | fr（按 base 匹配） |

#### Table 2 — 评估中（每行 = 一个 fr symbol）

`symbol / period / signal / latest_fr% / pred_fr% / fr_ma% / cur_loan% / pred_loan% / fr+pred_loan / ma+cur_loan / hits`

#### hits 色块

固定 4 个位置（`RULE_ORDER = [forward_open, forward_close, backward_open, backward_close]`），每个位置一个 12×12 圆角小方块：命中实色填充对应 tone（橙/蓝/紫/绿），未命中暗色（`rgba(255,255,255,0.06)`）+ 边框。hover tooltip 显示 `规则 比较符 阈值 | 当前值`。**不显示字母**。

### 格式约定

- `fmtPct`：4 位小数 `%`
- `fmtUsd`：2 位小数 千分位
- `fmtQty`：4 位小数（与 intra 的 6 位略缩，FR 资产数量级一致）
- `fmtLev` / `fmtQtl` / `fmtOffset`：3 位小数
- 时间戳：UTC+8 `HH:MM:SS`
- 数值正负着色：`positive` 绿 / `negative` 红 / 缺失 `muted`

### 查询参数

- `?live=1` — 走真实 ws；不传则 mock（自带假数据，便于本地浏览器直接打开）
- `?ws=...` / `?fr_ws=...` — 显式覆盖两路 ws URL（用于本地 `file://` 直连远程服务调试）

## 部署变更

### 1. `deploy_fr_signal_dashboard.sh` 改 per-env-name

破坏性改动：原"全局 `/ubuntu/dashboard`"模式弃用，强制 per-env-name。

| 项 | 现状 | 改后 |
|---|---|---|
| 必传参数 | `--exchange` | `--env-name` + `--viz-port` |
| TARGET_DIR | `/ubuntu/dashboard`（默认） | `$HOME/<env-name>/`（强制） |
| 端口 | `--port`（默认 6305） | `viz_port + 1`（自动算，不接 CLI 端口参数） |
| PM2 namespace | `dashboard` | env-name |
| PM2 name | `fr_signal_dashboard` | `${env_name}_fr_signal_dashboard` |
| exchange | 必传 | 可选；不传时从 env-name 第一段推断（`binance_fr_trade01` → `binance`），与 deploy_fr_viz_server.sh 的 `infer_exchange_from_env_name` 同源逻辑 |
| `dashboard.env` 写到 | `$TARGET_DIR/dashboard.env` | `$HOME/<env-name>/dashboard.env`（同 dir，PM2 启动时被 `start_fr_signal_dashboard.sh` source） |

### 2. `deploy_fr_viz_server.sh` 改 nginx upsert

只动 nginx awk 段。在 `/ws` `/snapshot` `/healthz` 三行后追加一行：

```
{prefix}/fr_ws  http://127.0.0.1:{viz_port + 1}/ws
```

`viz_port + 1` 直接在 awk 之前用 bash `local fr_port=$((PORT + 1))` 算出再传给 awk。**不增加 `--fr-dashboard-port` CLI 参数。**

### 3. HTML 拷贝优先 FR 二合一

`deploy_fr_viz_server.sh` 拷 HTML 段：优先 `docs/fr_pre_trade_dashboard.html`，回退 `docs/pre_trade_dashboard.html`（保险起见）。

### 4. 不改

- `config/viz.toml`：FR 三个实例已配好，不动
- Rust 源码：零改动
- `start_fr_signal_dashboard.sh` / `stop_fr_signal_dashboard.sh`：不动（已经支持从 sibling 路径 source `dashboard.env`）

## 测试计划

### 本地验证（mock）

`open file:///home/fanghaizhou/mkt_signal/docs/fr_pre_trade_dashboard.html`

预期：紫色 `DEMO` 徽章；两路指示灯 `MOCK`；Summary / 阈值图例 / Table 1（5 持仓行）/ Table 2（默认折叠，展开 18 行）；hits 色块按命中点亮；过滤框作用于两表；抽屉折叠状态在 F5 后保持。

### 部署后验证（live）

跑两个 deploy 脚本后浏览器访问 `https://<host>{prefix}/?live=1`：
- 徽章绿色 `LIVE`，5s 内两路指示灯变绿
- DevTools Network 看到两路 WebSocket：`./ws` 和 `./fr_ws`，端口分别为 `viz_port` 和 `viz_port + 1`，都是 101
- Summary / Table 1 用真实 viz 数据；Table 2 / 阈值图例用真实 fr 数据
- 阈值图例展开后与 fr_signal_dashboard 旧 UI 一致

### 多 env-name 验证

同机器上同时部署 `binance_fr_trade01` (viz=10031, fr=10032) 和 `binance_fr_trade02` (viz=10041, fr=10042)：
- 两个 nginx prefix 各自独立工作
- `nginx_locations.txt` 含两段 managed block（`/fr/binance_fr_trade01` 和 `/fr/binance_fr_trade02`），互不干扰
- 浏览器分别访问两个 URL 显示不同 symbol 集合 / 不同阈值

### 降级

- 关停 `${env_name}_fr_signal_dashboard` PM2 进程：`fr` 指示灯转红、Table 1 FR 列变 `—`、阈值图例变 `(0)`、Table 2 空；`viz` 指示灯仍绿
- 重启后 5s 内 `fr` 指示灯自动恢复绿

## 风险

- **端口约定 viz_port + 1**：约定简单清晰，但需要部署方自觉避免在 viz_port + 1 上放别的服务。多 env-name 部署 viz_port 间隔需 ≥ 2（如 10031 / 10041，不要 10031 / 10032）
- **per-env-name 改造的破坏性**：原"全局 `/ubuntu/dashboard`"部署点弃用。如果生产线已经用旧模式跑了 fr_signal_dashboard，需要先停旧进程再按新流程重部署
- **资产 ↔ symbol 映射**：MVP 仅支持 USDT quote，多 quote 路径全部走 console.warn 跳过。如未来 FR 接入非 USDT pair，需要在 fr row 加 `base` / `quote` 字段并改前端合并 key
