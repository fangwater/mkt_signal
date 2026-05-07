# FR Pre-Trade Dashboard（二合一）

**日期**: 2026-05-07
**目标**: 给资金费率（FR）套利做一个浏览器面板，把 intra dashboard 的实时持仓 / 对冲 / 风险信息和现有 `fr_signal_dashboard` 的信号视角（4 条规则、阈值、pred_fr/loan）合并到同一页面。架构与 intra/cross dashboard 对齐：单 HTML 文件、由 nginx serve、前端连相对路径 ws；后端零改动。

## 背景与现状

| 数据 | 现状 | 谁在用 |
|---|---|---|
| intra 风格的 exposure + risk（`PreTradeExposureResampleEntry` / `PreTradeRiskResampleEntry`） | ✅ FR 套利的 pre_trade 已发布到 iceoryx2，`viz_server` 已转发；`config/viz.toml` 已为 `okex_fr_trade` / `gate_fr_trade` / `binance_fr_trade` 三个实例配好 | intra/cross dashboard |
| FR 信号字段（`FrDashboardSnapshot`：funding_rate / predicted / ma / loan / 4 条规则 hit / 阈值图例 / signal summary） | ✅ `fr_signal_dashboard` 进程在跑，自带 axum + WS（`/ws`） | fr_signal_dashboard 内嵌的旧 HTML |

也就是说，新面板只是把两路已有 ws 在前端合并展示，不需要改 Rust 代码、不需要改 `viz.toml`。

## 不做（明确边界）

- 不动 Rust 代码：不改 `viz_server`、`fr_signal_dashboard`、`pre_trade`，不改 `viz.toml`，不发布 `FundingRateArbResampleEntry`（dead struct，留给未来）
- 不删旧 fr_signal_dashboard 内嵌 HTML：保留作为信号视角的纯净视图与降级备份
- 不做 `fr_config_server.py` 改造：单独下次 spec
- 不做行级搜索 / 列筛选 / 列点击排序 / 多 quote（非 USDT）支持：留给后续迭代
- 不做 MM 路径的 `hedge_offset_low/high` 展示：FR 套利只走 arb hedge 单档报单（与 intra 一致）

## 架构与数据流

```
       浏览器（fr_pre_trade_dashboard.html）
                │ 双 WS 长连接 + 各自独立的状态指示灯
        ┌───────┴───────┐
        ▼               ▼
    ./ws  (viz)     ./fr_ws  (fr)
        │               │
        │ nginx prefix/ws → 127.0.0.1:viz_port
        │ nginx prefix/fr_ws → 127.0.0.1:fr_dash_port
        ▼               ▼
   viz_server     fr_signal_dashboard
   (无改动)        (无改动)
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

只动一处：`scripts/deploy_fr_viz_server.sh`。

### 1. 拷贝 HTML

把 `docs/pre_trade_dashboard.html`（当前默认通用版本）替换为 `docs/fr_pre_trade_dashboard.html`：

```bash
# 现有逻辑（line ~396）
if [[ -f "$ROOT_DIR/docs/pre_trade_dashboard.html" ]]; then
  cp "$ROOT_DIR/docs/pre_trade_dashboard.html" "$TARGET_DIR/www/pre_trade_dashboard.html"
  cp "$ROOT_DIR/docs/pre_trade_dashboard.html" "$TARGET_DIR/www/index.html"

# 改为优先用 FR 专属 dashboard
if [[ -f "$ROOT_DIR/docs/fr_pre_trade_dashboard.html" ]]; then
  cp "$ROOT_DIR/docs/fr_pre_trade_dashboard.html" "$TARGET_DIR/www/pre_trade_dashboard.html"
  cp "$ROOT_DIR/docs/fr_pre_trade_dashboard.html" "$TARGET_DIR/www/index.html"
elif [[ -f "$ROOT_DIR/docs/pre_trade_dashboard.html" ]]; then
  ...保留旧 fallback...
```

### 2. nginx 加 `/fr_ws` 反代

`nginx_locations.txt` managed block 现状为 prefix 下三行（`/ws`, `/snapshot`, `/healthz`），需追加一行：

```
{prefix}/fr_ws  http://127.0.0.1:{fr_dashboard_port}/ws
```

`fr_dashboard_port` 从 `start_fr_signal_dashboard.sh` 当前实例端口取（每个 venue 各自的 fr_signal_dashboard 实例独立端口）。

实施侧需要：
- `deploy_fr_viz_server.sh` 新增 `--fr-dashboard-port` 参数（或从 sibling 配置文件中读取）
- 在 nginx mapping 生成代码（line ~118-175）加一段产出 `/fr_ws` 反代行

### 3. 不改

- `config/viz.toml`：FR 三个实例已配好，不动
- `Cargo.toml` / Rust 代码：零改动
- `fr_signal_dashboard` 部署脚本：不动

## 测试计划

### 本地验证（mock）

`open file:///home/fanghaizhou/mkt_signal/docs/fr_pre_trade_dashboard.html`

预期：
- 紫色 `DEMO` 徽章
- 两路指示灯都显示 `MOCK`
- Summary 区显示假数据（active>0、equity ~$1.2M、leverage ~2.3x）
- 阈值图例抽屉折叠态，点击展开看到 20 张卡片
- Table 1 显示 5 行（BTC/ETH/SOL/DOGE/AVAX），FR 列有数值且 hits 色块按规则点亮
- Table 2 折叠态，点击展开看到 ~18 行评估中 symbol，命中规则的 symbol 排前
- 过滤框输入 `BTC` 两表只剩 BTC 相关
- 抽屉展开 / 折叠状态在刷新后保持

### 部署后验证（live）

1. 运行 `scripts/deploy_fr_viz_server.sh --exchange okex --apply-nginx`，确认 nginx 配置中包含 `prefix/fr_ws`
2. 浏览器访问 `https://<host>{prefix}/?live=1`
3. 两路指示灯应在 5s 内变绿（已连接）
4. Summary / Table 1 数据来自 viz_server，Table 2 / 阈值图例数据来自 fr_signal_dashboard
5. 用 `?ws=...&fr_ws=...` 测试 URL override

### 边界

- 单路 ws 断开：另一路区块继续刷新，断开侧指示灯红，5s 后自动重连
- 持仓 symbol 不在 fr rows 中（罕见）：Table 1 该行 FR 列显示 `—`
- fr rows 全为 neutral：Table 2 仍展示但无色块点亮，summary `active=0`

## 风险

- **nginx mapping 生成逻辑改动**：`deploy_fr_viz_server.sh` 的 awk 脚本现有 4 个反代行（`/ws` `/snapshot` `/healthz` + static），新增 `/fr_ws` 不能破坏 idempotent upsert。实施时需要按现有 begin/end marker 模式扩展，单元测试用 dry-run + diff 验证。
- **fr_dashboard_port 来源**：当前 `start_fr_signal_dashboard.sh` 各 venue 实例端口可能不一致，需要 deploy 脚本能从 sibling 路径推断或显式参数传入。
- **资产 ↔ symbol 映射**：MVP 仅支持 USDT quote，多 quote 路径全部走 console.warn 跳过。如未来 FR 接入非 USDT pair，需要在 fr row 加 `base` / `quote` 字段并改前端合并 key。
