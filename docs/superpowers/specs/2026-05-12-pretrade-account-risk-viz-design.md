# pre_trade 接收 AccountRisk 并透出到 viz

**Date:** 2026-05-12
**Status:** Draft

## 背景

commit `3f08b60`（2026-05-12）让 5 个 account monitor（binance/okex/gate/bitget/bybit）开始发布统一的账户级风险快照 `BasicAccountRiskMsg`（event type `AccountRisk = 4007`），承载：

- `adj_equity_usd / actual_equity_usd`
- `maintenance_margin_usd / initial_margin_usd`
- `margin_ratio`（统一口径：`1.0` 为强平线，越大越安全）
- `borrowed_usd / notional_usd`
- 通过 `AccountRiskLevelProvider::account_risk_level(scope)` 给出 `AccountRiskLevel`：`FreeTrade / Warning / ReduceOnly / Liquidation`，阈值固定（>1.5 / >1.2 / >1.05）

消息复用既有 `BasicAccountEventMsg` 信封，走 `account_pubs/<exchange>_pm` iceoryx2 service。pre_trade 已经是订阅者：

- `src/pre_trade/monitor_channel.rs:1686` — `BasicAccountEventType::AccountRisk => {}`（空 handler）
- `src/pre_trade/query_eng_channel.rs:632` — 落入 `_ => {}`

也就是说**消息已经流到 pre_trade 了**，但目前还没有任何消费逻辑。本次目标：把 AccountRisk 接住、按账户 scope 维度暂存，再随 `pre_trade_risk` 重采样发布到 viz，前端 dashboard 直接展示。

## 目标

- pre_trade 在 `MonitorChannel` 内按 `BasicAccountScope` 缓存最新一份 AccountRisk 快照。
- 重采样时把当前作用域涉及到的快照（open_scope + hedge_scope 去重后）放进 `PreTradeRiskResampleEntry` 顶层，随 `pre_trade_risk` 频道发往 viz。
- 同步新增两个 process-level 风控阈值 `unimmr_trigger_line / unimmr_recover_line`，从 `pre_trade_risk_params` Redis hash 加载，挂在 risk entry 顶层用于前端着色。
- 改 5 个 `docs/*_pre_trade_dashboard.html` —— 每个 dashboard 渲染顶层 `account_risks` 数组，按账户维度出 1 或 2 张卡。

## 非目标

- 不基于 margin_ratio 做交易闸门 / order rejection / auto_repay / auto-close 等行为变更（后续 PR 再说）。
- 不修改账户监控端（producer 已经在 commit `3f08b60` 落地）。
- 不写新的 viz 频道（复用既有 `pre_trade_risk`）。
- 不修改 standard Binance（StdSpot/StdUm）的 AccountRisk producer 行为 —— 当前只有 unified 路径有 AccountRisk，standard 模式下卡片自然为空。

## 约束 / 已确认事实

1. `BasicAccountRiskMsg` 已带 `account_risk_level(scope)` trait 实现，state 文案直接用 `FreeTrade/Warning/ReduceOnly/Liquidation` 转 `free_trade/warning/reduce_only/liquidation`（snake_case）。
2. `BasicAccountScope` 已有 `as_str()`，但本次 view **不带 scope 字段**，前端按数组顺序显示。
3. 阈值约束：`PreTradeParamsLoader::normalize_unimmr_control_lines` 强制 `EXCHANGE_WARNING_MODE_UPPER_UNIMMR (=1.5) < trigger < recover`，不满足时回退到默认 `trigger=2.0 / recover=2.2`。consumer 永远拿到合法值，**没有"未配置"状态**。
4. 卡片粒度 = 账户 scope：MM / FR / Intra（open 与 hedge 在同一交易所同一 unified 账户）→ 1 张卡；Cross（跨交易所）→ 2 张卡。
5. bincode 是位置编码，新增字段需 pre_trade + viz_server **同周期发布**，否则解码失败。本仓库这两个 binary 同源同步发布，可接受。
6. `viz/subscribers.rs` 的 `pre_trade_risk` JSON 是**逐字段**拼装的（不是整体 pass-through），新增顶层字段必须显式加进 JSON 列表。

## 数据流

```
Redis: pre_trade_risk_params hash
  ├── 既有: max_pos_u / max_leverage / ...
  └── 新增: unimmr_trigger_line, unimmr_recover_line
                    |
                    v
       PreTradeParamsLoader（thread-local, 60s 刷新）
                    |
account_monitor x5  |
   |                v
   v        ResampleChannel::publish_resample_entries()
account_pubs/<ex>_pm                |
   |                                v
   v        PreTradeRiskResampleEntry {
MonitorChannel       ts_ms, ..., max_leverage,
   ↳ apply_account_risk(scope, msg)  unimmr_trigger_line,
   ↳ latest_account_risk: HashMap<   unimmr_recover_line,
       BasicAccountScope,            account_risks: Vec<PreTradeAccountRiskView>,
       BasicAccountRiskMsg>          open_leg, hedge_leg
                                    }
                                            |
                                            v
                                   pre_trade_risk iceoryx2 频道
                                            |
                                            v
                                   viz_server listener
                                            |
                                            v
                              WS broadcast `type: pre_trade_risk`
                                            |
                                            v
                                5 个 dashboard HTML 渲染 account_risks
```

## 设计

### 1. `src/common/basic_account_msg.rs`

新增辅助：

```rust
impl AccountRiskLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FreeTrade   => "free_trade",
            Self::Warning     => "warning",
            Self::ReduceOnly  => "reduce_only",
            Self::Liquidation => "liquidation",
        }
    }
}
```

### 2. `src/pre_trade/params_load.rs`（已在 WIP 落地）

WIP 已经实现完整：

- 字段 `unimmr_trigger_line: f64` / `unimmr_recover_line: f64`，默认 `2.0 / 2.2`
- 常量 `EXCHANGE_WARNING_MODE_UPPER_UNIMMR = 1.5`（交易所 warning 模式上沿），`DEFAULT_UNIMMR_TRIGGER_LINE = 2.0`，`DEFAULT_UNIMMR_RECOVER_LINE = 2.2`
- 校验函数 `normalize_unimmr_control_lines(t, r) -> Option<(f64, f64)>`：要求 `t.is_finite() && r.is_finite() && t > 1.5 && r > t`
- `load_from_redis`：读 `unimmr_trigger_line` / `unimmr_recover_line`，若 `normalize_unimmr_control_lines` 返回 None 则 `warn!` 并回退默认值
- 新增 getter `unimmr_trigger_line()` / `unimmr_recover_line()`
- `PreTradeParamsSnapshot` / `print_params_table` 同步加字段，对应单测覆盖

4 组 risk params 同步/打印脚本（MM / FR / CROSS / INTRA）也在 WIP 里加了这两个 key（默认 `"2.0" / "2.2"` 字符串）。

**本设计文档不再重复实现该部分**，仅声明 viz 端的 consumer 直接调用 `PreTradeParamsLoader::instance().unimmr_trigger_line()` / `unimmr_recover_line()`，永远得到合法值。

### 3. `src/pre_trade/monitor_channel.rs`

`MonitorChannelInner` 增加：

```rust
latest_account_risk: HashMap<BasicAccountScope, BasicAccountRiskMsg>,
```

`MonitorChannel` 新增方法：

```rust
pub fn apply_account_risk(&self, scope: BasicAccountScope, msg: BasicAccountRiskMsg);
pub fn account_risk_snapshot(&self, scope: BasicAccountScope) -> Option<BasicAccountRiskMsg>;
```

第 1686 行 handler：

```rust
BasicAccountEventType::AccountRisk => {
    match BasicAccountRiskMsg::from_bytes(data) {
        Ok(msg) => MonitorChannel::instance().apply_account_risk(account_scope, msg),
        Err(err) => warn!("AccountRisk decode failed: {err:#}"),
    }
}
```

### 4. `src/pre_trade/query_eng_channel.rs`

把 `match event_type` 里的 `_ => {}` 拆出一支 `BasicAccountEventType::AccountRisk` 镜像写入。两个 subscriber 收到的是同一个 IPC 流的相同 message，写入到同一个 thread-local map 是幂等的（后写覆盖前写，内容相同）。

### 5. `src/viz/resample.rs`

新增 view：

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreTradeAccountRiskView {
    pub ts_ms: i64,
    pub adj_equity_usd: f64,
    pub actual_equity_usd: f64,
    pub maintenance_margin_usd: f64,
    pub initial_margin_usd: f64,
    pub margin_ratio: f64,
    pub borrowed_usd: f64,
    pub notional_usd: f64,
    pub state: String,
}
```

扩展 `PreTradeRiskResampleEntry`（**注意：新字段加在 struct 末尾，bincode 位置兼容性靠同步发布保证**）：

```rust
pub struct PreTradeRiskResampleEntry {
    pub ts_ms: i64,
    pub signal_counts: HashMap<String, u64>,
    pub total_equity: f64,
    pub total_exposure: f64,
    pub total_position: f64,
    pub spot_equity_usd: f64,
    pub borrowed_usd: f64,
    pub interest_usd: f64,
    pub um_unrealized_usd: f64,
    pub leverage: f64,
    pub max_leverage: f64,
    pub open_leg: PreTradeVenueRiskResampleEntry,
    pub hedge_leg: PreTradeVenueRiskResampleEntry,
    // ↓↓↓ 新增
    pub unimmr_trigger_line: f64,
    pub unimmr_recover_line: f64,
    pub account_risks: Vec<PreTradeAccountRiskView>,
}
```

`PreTradeVenueRiskResampleEntry` **不变**。`impl_codec!` 宏继续生效。

### 6. `src/pre_trade/resample_channel.rs`

`publish_resample_entries` 内 risk 块新增：

```rust
let open_scope = mon.account_scope_for_venue(mon.open_venue());
let hedge_scope = mon.account_scope_for_venue(mon.hedge_venue());

// 顺序：open 优先；hedge 与 open 同 scope 时不再重复入数组
let mut scope_order: Vec<BasicAccountScope> = vec![open_scope];
if hedge_scope != open_scope {
    scope_order.push(hedge_scope);
}

let account_risks: Vec<PreTradeAccountRiskView> = scope_order
    .into_iter()
    .filter_map(|scope| {
        let msg = mon.account_risk_snapshot(scope)?;
        let state = msg
            .account_risk_level(scope)
            .map(|l| l.as_str().to_string())
            .unwrap_or_else(|| "unknown".to_string());
        Some(PreTradeAccountRiskView {
            ts_ms: msg.timestamp, // 5 个 producer 均填毫秒（见 docs/account_risk_msg_mapping.md 与各 parser 单测）
            adj_equity_usd: msg.adj_equity_usd,
            actual_equity_usd: msg.actual_equity_usd,
            maintenance_margin_usd: msg.maintenance_margin_usd,
            initial_margin_usd: msg.initial_margin_usd,
            margin_ratio: msg.margin_ratio,
            borrowed_usd: msg.borrowed_usd,
            notional_usd: msg.notional_usd,
            state,
        })
    })
    .collect();

let unimmr_trigger_line =
    PreTradeParamsLoader::instance().unimmr_trigger_line();
let unimmr_recover_line =
    PreTradeParamsLoader::instance().unimmr_recover_line();

let entry = PreTradeRiskResampleEntry {
    // ...既有字段
    max_leverage,
    open_leg,
    hedge_leg,
    unimmr_trigger_line,
    unimmr_recover_line,
    account_risks,
};
```

> 注：所有 5 家 producer（Binance/OKX/Gate/Bitget/Bybit）的 `BasicAccountRiskMsg.timestamp` 均填毫秒，view 字段直接透传，无需换算。落实现时如新增 producer 偏离此口径，需要在 view 端统一规整。

### 7. `src/viz/subscribers.rs`

`spawn_pre_trade_risk_listener` 里现有 JSON 拼装新增三项：

```rust
"unimmr_trigger_line": entry.unimmr_trigger_line,
"unimmr_recover_line": entry.unimmr_recover_line,
"account_risks": entry.account_risks,
```

`open_leg / hedge_leg` 仍整体 serialize，本次不动。

### 8. 5 个 dashboard 改动

目标文件：

- `docs/mm_pre_trade_dashboard.html`
- `docs/intra_pre_trade_dashboard.html`
- `docs/fr_pre_trade_dashboard.html`
- `docs/cross_pre_trade_dashboard.html`
- `docs/pre_trade_dashboard.html`

每个 dashboard 在 risk 信息区附近新增一段 `Account Risk` 区块，按 `entry.account_risks` 数组渲染 N 张卡（N ∈ {0, 1, 2}）。每张卡格式：

```
+------------ Account Risk ------------+
| state: warning                       |  ← 文字, 无颜色, snake_case 原文
| margin_ratio: 1.27                   |  ← 数字, 按阈值染色
| ts_utc: 2026-05-12 17:02:13          |
| adj_eq:    12,345.67                 |
| actual_eq: 12,200.00                 |
| mm:         1,000.00                 |
| im:         2,500.00                 |
| borrowed:   3,000.00                 |
| notional:  30,000.00                 |
+--------------------------------------+
```

**颜色规则**（仅作用于 `margin_ratio` 数字文本，其他字段保持默认色）：

设 `t = entry.unimmr_trigger_line`，`r = entry.unimmr_recover_line`。Backend 保证 `1.5 < t < r`，consumer 端无需再做合法性判断：

| 条件 | 颜色 |
|---|---|
| `margin_ratio > r` | 绿（`var(--accent-green)`） |
| `margin_ratio < t` | 红（`var(--accent-red)`） |
| `t <= margin_ratio <= r` | 橙（`var(--accent-warn)`） |

`account_risks` 数组为空 / N=0 时区块整体隐藏（避免空卡）。所有 5 个 dashboard 共用同一段 JS 渲染逻辑（同一个函数），通过 `document.getElementById('account-risks')` 找到容器，注入 `<div class="account-risk-card">…</div>` 卡片。

## 错误处理

- AccountRisk payload 解析失败 → `warn!`，map 不更新（保留旧值）。
- map 没记录该 scope（producer 尚未启动 / 拉取失败）→ 该 scope 从 `account_risks` 数组中省略。
- Redis 阈值非法 / 缺失 → `params_load.rs` 已经 `warn!` 并回退到 `(2.0, 2.2)` 默认值；consumer 端不需要再处理这种状态。
- producer 与 consumer bincode schema 错位 → viz_server 解码失败、整条 entry 丢弃，`warn!` 一条。靠"两 binary 同源同发"约束规避。

## 测试

- `params_load.rs`：单测已经在 WIP 里覆盖（`test_normalize_unimmr_control_lines` 与 default/snapshot 断言），本次不重复加。
- `monitor_channel.rs`：补单测覆盖 `apply_account_risk` → `account_risk_snapshot` 的 round-trip（每个 scope 独立桶，后写覆盖前写）。
- `viz/resample.rs`：补单测覆盖含 `account_risks + 两根线` 的 `PreTradeRiskResampleEntry` bincode round-trip。
- e2e 手测：
  1. 起 pre_trade + viz_server；
  2. `wscat -c ws://...`，检查 `pre_trade_risk` payload 包含 `account_risks` 数组、`unimmr_trigger_line` / `unimmr_recover_line`；
  3. 打开任一 dashboard，验证：
     - MM/fr/intra：1 张账户卡；cross：2 张账户卡；
     - 调整 Redis `pre_trade_risk_params` 两根线，60s 内前端着色随阈值变化（绿/橙/红 切换）；
     - 关闭 producer 一阵后再启动，前端从 `--` 恢复展示。

## 部署

- pre_trade 与 viz_server 必须同周期 rebuild + 重启（bincode schema 变更）。
- Redis `pre_trade_risk_params` hash 在切换前可用 MM / FR / CROSS / INTRA 的 sync 脚本 HSET `unimmr_trigger_line` / `unimmr_recover_line`（默认 `"2.0" / "2.2"`）；不配置或配置非法时进程会 `warn!` 并使用代码内默认值 `(2.0, 2.2)`，永不崩溃。
- 前端 5 个 dashboard HTML 是静态文件，按既有 deploy 流程一并发布。
