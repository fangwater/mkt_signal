# Account Risk Monitoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 Binance PM 的 UniMMR、OKX UA 的 mgnRatio、Gate UA 的 maintenance margin rate 抽到统一的 `BasicAccountRiskMsg` 事件流，缓存进 `MonitorChannel`，发布到 `pre_trade_risk` 频道，作为后续硬风控（gating）的可信底面。

**Architecture:**
- Binance 侧：`binance_account_monitor` 用现有 `primary_local_ip` 每 5s 轮询 `/papi/v1/account`（仅 UNIFIED 模式），把响应解析成 `BasicAccountRiskMsg` 走现有 PM forwarder 推到 IPC。
- OKX 侧：扩展 `okex_account_event_parser::parse_account`，从 WS `account` channel 顶层抽 `adjEq/mmr/imr/mgnRatio/borrowFroz/notionalUsd` 并发同样的事件——OKX WS 已经天然推这些字段，无需 REST。
- Gate 侧：在 `gate_account_monitor` 现有订阅列表追加 `unified.assets` channel（与现有 `unified.asset_detail` 并行，互不冲突），`gate_account_event_parser` 加 `parse_unified_assets` 解析 `r/R/b/e/l/T/a` 字段并 emit 同样的事件——也无需 REST。
- pre_trade 侧：`MonitorChannel` 新增 `risk_cache: HashMap<BasicAccountScope, AccountRiskSnapshot>`；`pre_trade_risk` 频道新增 `account_risk` 字段供前端展示和后续 gating。
- 单位归一：`margin_ratio` 统一成 `1.0 = 强平`。OKX `mgnRatio` 是百分比 → `/100`；Binance `uniMMR` 已是 ratio → 直接用；Gate `r` 是 MM/MB% → `100/r` 反转方向。

**Message unification:** `BasicAccountRiskMsg` 是**唯一**风险消息结构，四家所有 producer（Binance / OKX / Gate / Bitget）emit 同一个 struct。差异完全收在 parser 里做归一化。下游不知道 producer 是哪家，按 `BasicAccountScope` 区分。**单一不变量**：`margin_ratio = 1.0` 永远表示该 scope 在该交易所的强平边界。需要 per-exchange 的只有阈值（Task 9 的 floor），因为各家 MMR 表 + collateral haircut 不同，同样 `margin_ratio=1.5` 代表的安全垫不同。

**Tech Stack:** Rust (tokio + reqwest + iceoryx2 + bincode), Binance PAPI REST, OKX V5 WS account channel, Gate WS unified.assets channel.

---

## File Structure

**Create:**
- `src/trade_engine/query_parsers/binance_pm_account_risk.rs` — 解析 `/papi/v1/account` JSON → `BasicAccountRiskMsg::to_bytes()`

**Modify:**
- `src/common/basic_account_msg.rs` — 加 `BasicAccountEventType::AccountRisk = 4007`、`BasicAccountRiskMsg` struct + codec、`get_basic_event_type` 加 4007 分支
- `src/trade_engine/query_parsers.rs` — 注册新 parser 模块
- `src/parser/okex_account_event_parser.rs` — `parse_account` 抽顶层 risk 字段并 emit
- `src/parser/gate_account_event_parser.rs` — 加 `parse_unified_assets`，channel router 加 `"unified.assets"` 分支
- `src/bin/gate_account_monitor.rs` — channels 列表追加 `unified.assets`
- `src/parser/bitget_account_event_parser.rs::parse_account_channel` — 加顶层 `effEquity/mmr/imr/mgnRatio/accountEquity` 抽取，emit AccountRisk（Bitget UTA `account` channel 已订阅，不动 subscriber）
- `src/bin/binance_account_monitor.rs` — `main` 里 spawn `pm_risk_poller`；`log_parsed_event` 加 `AccountRisk` 分支；`AccountEventDeduper` 加 `key_account_risk`
- `src/pre_trade/monitor_channel.rs` — `MonitorChannelInner` 加 `risk_cache`；账户事件 dispatch 循环加 `AccountRisk` 分支；新增 `account_risk_snapshot(scope)` accessor
- `src/viz/resample.rs` — `PreTradeRiskResampleEntry` 加 `account_risk_open` / `account_risk_hedge` 字段；新结构 `AccountRiskSnapshotResample`
- `src/pre_trade/resample_channel.rs::publish_resample_entries` — 从 `MonitorChannel` 读 risk cache 填进 `PreTradeRiskResampleEntry`
- `src/viz/subscribers.rs` — pre_trade_risk JSON 输出新增 `account_risk_open` / `account_risk_hedge`
- `src/pre_trade/params_load.rs` — 加 per-exchange `margin_ratio_floor`（binance/okex/gate 各一个）

**Test files:**
- `src/common/basic_account_msg.rs` 内 `#[cfg(test)] mod tests` 加 `BasicAccountRiskMsg` 编码 round-trip
- `src/trade_engine/query_parsers/binance_pm_account_risk.rs` 内加 sample-JSON 解析测试
- `src/parser/okex_account_event_parser.rs` 内加 OKX `account` channel risk 提取测试
- `src/pre_trade/monitor_channel.rs` 内 `#[cfg(test)] mod tests` 加 `risk_cache` 缓存测试

---

## Task 1: BasicAccountRiskMsg 类型与编解码

**Files:**
- Modify: `src/common/basic_account_msg.rs:9-24` (enum 增枚举值)
- Modify: `src/common/basic_account_msg.rs:1394-1402` (`get_basic_event_type` 加分支)
- Modify: `src/common/basic_account_msg.rs:1415-` (新增 struct + 测试)

- [ ] **Step 1: 加 `AccountRisk` 枚举值**

打开 `src/common/basic_account_msg.rs`，把现有枚举改成（在 `TradeUpdateLite` 之后、`Error` 之前插入）：

```rust
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasicAccountEventType {
    OrderUpdate = 4001,
    BalanceUpdate = 4002,
    PositionUpdate = 4003,
    BorrowInterest = 4004,
    UnrealizedPnlUpdate = 4005,
    TradeUpdateLite = 4006,
    /// 账户级风险快照（adj equity / maintenance margin / margin ratio）
    /// Binance PM REST 周期推送、OKX WS account 频道事件触发。
    AccountRisk = 4007,
    Error = 4999,
}
```

并在 `get_basic_event_type` 函数（约 1394-1402 行）的 `match` 加：

```rust
4006 => BasicAccountEventType::TradeUpdateLite,
4007 => BasicAccountEventType::AccountRisk,
_ => BasicAccountEventType::Error,
```

- [ ] **Step 2: 加 `BasicAccountRiskMsg` 结构体与 to/from_bytes**

在文件末尾、`#[cfg(test)] mod tests` 之前插入：

```rust
/// Basic 账户级风险快照消息。
///
/// 单位归一约定：
/// - 所有 USD 字段：美元金额（不是分）
/// - `margin_ratio`：1.0 = 强平边界（OKX `mgnRatio` 是百分比，emit 前 /100 转 ratio；Binance `uniMMR` 已是 ratio）
/// - 缺失/不可得字段写 0.0；下游用 `last_timestamp` 判新鲜度
#[derive(Debug, Clone)]
pub struct BasicAccountRiskMsg {
    pub msg_type: BasicAccountEventType,
    pub timestamp: i64,
    pub adj_equity_usd: f64,
    pub actual_equity_usd: f64,
    pub maintenance_margin_usd: f64,
    pub initial_margin_usd: f64,
    pub margin_ratio: f64,
    pub borrowed_usd: f64,
    pub notional_usd: f64,
}

impl BasicAccountRiskMsg {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        timestamp: i64,
        adj_equity_usd: f64,
        actual_equity_usd: f64,
        maintenance_margin_usd: f64,
        initial_margin_usd: f64,
        margin_ratio: f64,
        borrowed_usd: f64,
        notional_usd: f64,
    ) -> Self {
        Self {
            msg_type: BasicAccountEventType::AccountRisk,
            timestamp,
            adj_equity_usd,
            actual_equity_usd,
            maintenance_margin_usd,
            initial_margin_usd,
            margin_ratio,
            borrowed_usd,
            notional_usd,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        // 4 (type) + 8 (ts) + 8*7 (f64 fields)
        let total = 4 + 8 + 8 * 7;
        let mut buf = BytesMut::with_capacity(total);
        buf.put_u32_le(self.msg_type as u32);
        buf.put_i64_le(self.timestamp);
        buf.put_f64_le(self.adj_equity_usd);
        buf.put_f64_le(self.actual_equity_usd);
        buf.put_f64_le(self.maintenance_margin_usd);
        buf.put_f64_le(self.initial_margin_usd);
        buf.put_f64_le(self.margin_ratio);
        buf.put_f64_le(self.borrowed_usd);
        buf.put_f64_le(self.notional_usd);
        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        const MIN_SIZE: usize = 4 + 8 + 8 * 7;
        if data.len() < MIN_SIZE {
            anyhow::bail!("BasicAccountRiskMsg too short: {}", data.len());
        }
        let mut cursor = Bytes::copy_from_slice(data);
        let msg_type = cursor.get_u32_le();
        if msg_type != BasicAccountEventType::AccountRisk as u32 {
            anyhow::bail!("invalid BasicAccountRiskMsg type: {}", msg_type);
        }
        let timestamp = cursor.get_i64_le();
        let adj_equity_usd = cursor.get_f64_le();
        let actual_equity_usd = cursor.get_f64_le();
        let maintenance_margin_usd = cursor.get_f64_le();
        let initial_margin_usd = cursor.get_f64_le();
        let margin_ratio = cursor.get_f64_le();
        let borrowed_usd = cursor.get_f64_le();
        let notional_usd = cursor.get_f64_le();
        Ok(Self {
            msg_type: BasicAccountEventType::AccountRisk,
            timestamp,
            adj_equity_usd,
            actual_equity_usd,
            maintenance_margin_usd,
            initial_margin_usd,
            margin_ratio,
            borrowed_usd,
            notional_usd,
        })
    }
}
```

- [ ] **Step 3: 加 round-trip 测试**

在 `#[cfg(test)] mod tests` 块（约 1415 行起）末尾追加：

```rust
#[test]
fn basic_account_risk_msg_round_trip() {
    let msg = BasicAccountRiskMsg::create(
        1_700_000_000_000,
        100_000.5,
        99_500.25,
        2_000.0,
        4_000.0,
        50.0,
        15_000.0,
        300_000.0,
    );
    let bytes = msg.to_bytes();
    let parsed = BasicAccountRiskMsg::from_bytes(&bytes).expect("decode ok");
    assert_eq!(parsed.msg_type as u32, BasicAccountEventType::AccountRisk as u32);
    assert_eq!(parsed.timestamp, 1_700_000_000_000);
    assert!((parsed.adj_equity_usd - 100_000.5).abs() < 1e-9);
    assert!((parsed.maintenance_margin_usd - 2_000.0).abs() < 1e-9);
    assert!((parsed.margin_ratio - 50.0).abs() < 1e-9);
    assert!((parsed.borrowed_usd - 15_000.0).abs() < 1e-9);
}

#[test]
fn account_risk_event_type_round_trips_through_event_envelope() {
    let payload = BasicAccountRiskMsg::create(123, 1.0, 1.0, 0.5, 1.0, 2.0, 0.0, 10.0).to_bytes();
    let envelope = BasicAccountEventMsg::create(
        BasicAccountEventType::AccountRisk,
        BasicAccountScope::BinanceUnified,
        payload,
    )
    .to_bytes();
    let (event_type, scope, body) =
        split_basic_account_event(&envelope).expect("envelope parse");
    assert_eq!(event_type as u32, BasicAccountEventType::AccountRisk as u32);
    assert_eq!(scope, BasicAccountScope::BinanceUnified);
    let decoded = BasicAccountRiskMsg::from_bytes(body).expect("payload decode");
    assert!((decoded.notional_usd - 10.0).abs() < 1e-9);
}
```

- [ ] **Step 4: 运行测试**

```bash
cargo test --lib common::basic_account_msg::tests::basic_account_risk_msg_round_trip
cargo test --lib common::basic_account_msg::tests::account_risk_event_type_round_trips_through_event_envelope
```

预期：两个测试 PASS。

- [ ] **Step 5: Commit**

```bash
git add src/common/basic_account_msg.rs
git commit -m "feat(account): add BasicAccountRiskMsg event type for unified margin metrics"
```

---

## Task 2: Binance PM `/papi/v1/account` parser

**Files:**
- Create: `src/trade_engine/query_parsers/binance_pm_account_risk.rs`
- Modify: `src/trade_engine/query_parsers.rs` (注册模块)

- [ ] **Step 1: 注册模块**

在 `src/trade_engine/query_parsers.rs` 末尾追加（保持字母序）：

```rust
pub mod binance_pm_account_risk;
```

- [ ] **Step 2: 写失败测试**

新建 `src/trade_engine/query_parsers/binance_pm_account_risk.rs`：

```rust
//! 解析 Binance PM `/papi/v1/account` 响应为 BasicAccountRiskMsg。
//!
//! 字段映射（per Binance API docs）：
//! - accountEquity   → adj_equity_usd（已应用 collateral haircut 的 USD 权益）
//! - actualEquity    → actual_equity_usd（未折抵押率的 USD 权益）
//! - accountMaintMargin → maintenance_margin_usd
//! - accountInitialMargin → initial_margin_usd
//! - uniMMR          → margin_ratio（已是 ratio，1.0=强平）
//! - 借币聚合：账户级 borrowed USD = accountInitialMargin - accountMaintMargin? 不对——实际拿不到聚合借币 USD，
//!   设 0；下游若需要可从 BasicBorrowInterestMsg 自行汇总
//! - notional（总名义美金）：Binance 这个端点不返回，设 0

use crate::common::basic_account_msg::BasicAccountRiskMsg;
use bytes::Bytes;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct BinancePmAccountResponse {
    #[serde(default, rename = "accountEquity")]
    account_equity: String,
    #[serde(default, rename = "actualEquity")]
    actual_equity: String,
    #[serde(default, rename = "accountMaintMargin")]
    account_maint_margin: String,
    #[serde(default, rename = "accountInitialMargin")]
    account_initial_margin: String,
    #[serde(default, rename = "uniMMR")]
    uni_mmr: String,
    #[serde(default, rename = "updateTime")]
    update_time: i64,
}

fn parse_f64(v: &str) -> f64 {
    v.trim().parse::<f64>().unwrap_or(0.0)
}

pub fn parse_binance_pm_account_risk(json: &str) -> Option<Bytes> {
    let resp: BinancePmAccountResponse = serde_json::from_str(json).ok()?;
    let ts = if resp.update_time > 0 {
        resp.update_time
    } else {
        chrono::Utc::now().timestamp_millis()
    };
    let msg = BasicAccountRiskMsg::create(
        ts,
        parse_f64(&resp.account_equity),
        parse_f64(&resp.actual_equity),
        parse_f64(&resp.account_maint_margin),
        parse_f64(&resp.account_initial_margin),
        parse_f64(&resp.uni_mmr),
        0.0, // borrowed_usd: not exposed at account level
        0.0, // notional_usd: not exposed at account level
    );
    Some(msg.to_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::basic_account_msg::BasicAccountRiskMsg;

    #[test]
    fn parses_papi_v1_account_response() {
        let json = r#"{
            "uniMMR": "5.23",
            "accountEquity": "100123.45",
            "actualEquity": "99876.50",
            "accountInitialMargin": "8000.00",
            "accountMaintMargin": "1500.00",
            "updateTime": 1700000000000
        }"#;
        let bytes = parse_binance_pm_account_risk(json).expect("parse ok");
        let msg = BasicAccountRiskMsg::from_bytes(&bytes).expect("decode ok");
        assert_eq!(msg.timestamp, 1700000000000);
        assert!((msg.adj_equity_usd - 100123.45).abs() < 1e-6);
        assert!((msg.actual_equity_usd - 99876.50).abs() < 1e-6);
        assert!((msg.maintenance_margin_usd - 1500.00).abs() < 1e-6);
        assert!((msg.initial_margin_usd - 8000.00).abs() < 1e-6);
        assert!((msg.margin_ratio - 5.23).abs() < 1e-9);
    }

    #[test]
    fn empty_string_fields_default_to_zero() {
        let json = r#"{
            "uniMMR": "",
            "accountEquity": "",
            "accountMaintMargin": "0",
            "updateTime": 1700000000000
        }"#;
        let bytes = parse_binance_pm_account_risk(json).expect("parse ok");
        let msg = BasicAccountRiskMsg::from_bytes(&bytes).expect("decode ok");
        assert_eq!(msg.adj_equity_usd, 0.0);
        assert_eq!(msg.margin_ratio, 0.0);
    }

    #[test]
    fn missing_update_time_falls_back_to_now() {
        let json = r#"{"uniMMR":"3.0","accountEquity":"100","accountMaintMargin":"30"}"#;
        let bytes = parse_binance_pm_account_risk(json).expect("parse ok");
        let msg = BasicAccountRiskMsg::from_bytes(&bytes).expect("decode ok");
        assert!(msg.timestamp > 1_700_000_000_000); // sanity: now() much later than 2023
    }
}
```

- [ ] **Step 3: 运行测试，确认失败原因是模块不存在**

```bash
cargo test --lib trade_engine::query_parsers::binance_pm_account_risk 2>&1 | head -30
```

预期：编译失败 / 模块未注册 → Step 1 已注册过，所以应该是测试 PASS。如果 PASS 直接跳到 Step 5。

- [ ] **Step 4: 三个测试都 PASS**

```bash
cargo test --lib trade_engine::query_parsers::binance_pm_account_risk
```

预期：3 passed。

- [ ] **Step 5: Commit**

```bash
git add src/trade_engine/query_parsers.rs src/trade_engine/query_parsers/binance_pm_account_risk.rs
git commit -m "feat(query): parse Binance PM /papi/v1/account into BasicAccountRiskMsg"
```

---

## Task 3: OKX `account` channel risk 提取

**Files:**
- Modify: `src/parser/okex_account_event_parser.rs:143-211` (`parse_account` 函数)

- [ ] **Step 1: 加测试（先于实现）**

在 `src/parser/okex_account_event_parser.rs` 文件末尾的 `#[cfg(test)] mod tests` 块追加（如不存在 mod tests 就新建）：

```rust
#[cfg(test)]
mod risk_tests {
    use super::*;
    use crate::common::basic_account_msg::{
        get_basic_event_type, split_basic_account_event, BasicAccountEventType,
        BasicAccountRiskMsg,
    };
    use bytes::Bytes;
    use tokio::sync::mpsc;

    fn collect(rx: &mut mpsc::UnboundedReceiver<Bytes>) -> Vec<Bytes> {
        let mut out = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            out.push(msg);
        }
        out
    }

    #[test]
    fn parse_account_emits_account_risk_with_top_level_metrics() {
        let parser = OkexAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
        // OKX `account` channel 顶层带 mgnRatio/adjEq/mmr/imr/borrowFroz/notionalUsd
        let json = r#"{
            "arg":{"channel":"account"},
            "data":[{
                "uTime":"1700000000000",
                "adjEq":"100123.45",
                "totalEq":"100500.00",
                "mmr":"1500.00",
                "imr":"8000.00",
                "mgnRatio":"523.0",
                "borrowFroz":"100.0",
                "notionalUsd":"300000.00",
                "details":[
                    {"ccy":"USDT","eq":"50000","uTime":"1700000000000","liab":"0","interest":"0"}
                ]
            }]
        }"#;
        parser.parse(Bytes::from(json), &tx);
        let msgs = collect(&mut rx);

        // 找到 AccountRisk 事件
        let risk_msgs: Vec<_> = msgs
            .iter()
            .filter(|m| {
                let (event_type, _, _) = split_basic_account_event(m).unwrap();
                matches!(event_type, BasicAccountEventType::AccountRisk)
            })
            .collect();
        assert_eq!(risk_msgs.len(), 1, "expected one AccountRisk event");

        let (_, scope, body) = split_basic_account_event(risk_msgs[0]).unwrap();
        assert_eq!(scope, BasicAccountScope::OkexUnified);
        let risk = BasicAccountRiskMsg::from_bytes(body).unwrap();
        assert_eq!(risk.timestamp, 1700000000000);
        assert!((risk.adj_equity_usd - 100123.45).abs() < 1e-6);
        assert!((risk.actual_equity_usd - 100500.00).abs() < 1e-6);
        assert!((risk.maintenance_margin_usd - 1500.0).abs() < 1e-6);
        assert!((risk.initial_margin_usd - 8000.0).abs() < 1e-6);
        // OKX mgnRatio 是百分比 (523.0 = 5.23 ratio)，emit 时除 100
        assert!(
            (risk.margin_ratio - 5.23).abs() < 1e-6,
            "got margin_ratio={}",
            risk.margin_ratio
        );
        assert!((risk.borrowed_usd - 100.0).abs() < 1e-6);
        assert!((risk.notional_usd - 300000.0).abs() < 1e-6);
    }

    #[test]
    fn parse_account_skips_account_risk_when_top_level_fields_missing() {
        let parser = OkexAccountEventParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
        // 没有 mgnRatio / adjEq 字段 → 不发 AccountRisk（避免污染下游）
        let json = r#"{
            "arg":{"channel":"account"},
            "data":[{
                "uTime":"1700000000000",
                "details":[{"ccy":"USDT","eq":"100","uTime":"1700000000000"}]
            }]
        }"#;
        parser.parse(Bytes::from(json), &tx);
        let msgs = collect(&mut rx);
        let count = msgs
            .iter()
            .filter(|m| {
                matches!(
                    get_basic_event_type(m),
                    BasicAccountEventType::AccountRisk
                )
            })
            .count();
        assert_eq!(count, 0, "should not emit AccountRisk when top-level fields are absent");
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
cargo test --lib parser::okex_account_event_parser::risk_tests 2>&1 | head -30
```

预期：FAIL — `expected one AccountRisk event` 找到 0 条。

- [ ] **Step 3: 实现 risk 抽取**

修改 `src/parser/okex_account_event_parser.rs::parse_account`（约 143-211 行）。在函数顶部 `let Some(account) = ...` 之后、`for bal in arr` 循环之前插入 risk 抽取逻辑：

```rust
fn parse_account(
    &self,
    json_value: &serde_json::Value,
    tx: &mpsc::UnboundedSender<Bytes>,
) -> usize {
    let mut count = 0;
    let Some(account) = json_value.get("data").and_then(|d| d.get(0)) else {
        return 0;
    };
    let fallback_ts = account
        .get("uTime")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);

    // 顶层 risk 字段：仅当 mgnRatio 或 adjEq 至少有一个存在时才发 AccountRisk
    let has_risk_fields = account.get("mgnRatio").is_some()
        || account.get("adjEq").is_some()
        || account.get("mmr").is_some();
    if has_risk_fields {
        let adj_equity = parse_f64_field(account.get("adjEq"));
        let actual_equity = {
            let v = parse_f64_field(account.get("totalEq"));
            if v.abs() > 0.0 { v } else { adj_equity }
        };
        let mmr = parse_f64_field(account.get("mmr"));
        let imr = parse_f64_field(account.get("imr"));
        // OKX mgnRatio 是百分比（"523.0" 表示 5.23 倍）；统一成 ratio (1.0 = 强平)
        let mgn_ratio_pct = parse_f64_field(account.get("mgnRatio"));
        let margin_ratio = if mgn_ratio_pct.abs() > 0.0 {
            mgn_ratio_pct / 100.0
        } else {
            0.0
        };
        let borrowed = parse_f64_field(account.get("borrowFroz"));
        let notional = parse_f64_field(account.get("notionalUsd"));

        let risk_msg = crate::common::basic_account_msg::BasicAccountRiskMsg::create(
            fallback_ts,
            adj_equity,
            actual_equity,
            mmr,
            imr,
            margin_ratio,
            borrowed,
            notional,
        );
        let payload = risk_msg.to_bytes();
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::AccountRisk,
            BasicAccountScope::OkexUnified,
            payload,
        );
        if tx.send(event.to_bytes()).is_ok() {
            count += 1;
        }
    }

    let Some(arr) = account.get("details").and_then(|v| v.as_array()) else {
        return count;
    };

    // ... 现有 for bal in arr 循环保持不变 ...
```

注意：保持现有 `for bal in arr` 循环及其内部的 emit_balance / BorrowInterest 逻辑不动。

- [ ] **Step 4: 运行测试，确认 PASS**

```bash
cargo test --lib parser::okex_account_event_parser::risk_tests
```

预期：2 passed。同时跑全 parser 测试确认无回归：

```bash
cargo test --lib parser::okex_account_event_parser
```

预期：所有现有测试 + 2 新测试 PASS。

- [ ] **Step 5: Commit**

```bash
git add src/parser/okex_account_event_parser.rs
git commit -m "feat(okex): emit BasicAccountRiskMsg from account channel top-level fields"
```

---

## Task 4: Gate `unified.assets` 订阅 + parser

**背景:** 现有 Gate 订阅是 `unified.asset_detail`（per-currency balance/borrow），**没有**账户级风险字段。`unified.assets` 是另一条 channel，**账户级聚合**推送 `r`(总维持保证金率%)、`R`(总初始保证金率%)、`b`(总借币)、`e`(总权益)、`l`(总负债)、`T`(总保证金)、`a`(可用)。两个 channel 并行订阅，互不冲突。

**Files:**
- Modify: `src/bin/gate_account_monitor.rs:99-103` (channels 列表追加 `unified.assets`)
- Modify: `src/parser/gate_account_event_parser.rs:1063` (channel router 加 `"unified.assets"` 分支)
- Modify: `src/parser/gate_account_event_parser.rs` (新增 `parse_unified_assets` 函数)

注：`gate_auth.rs::build_subscribe_message` 本来就是通用的（`channel: &str`），加新 channel 无需改 auth helper。

- [ ] **Step 1: 先写测试（含用户给的 sample）**

在 `src/parser/gate_account_event_parser.rs` 的 `#[cfg(test)] mod tests` 块内追加：

```rust
#[test]
fn parses_unified_assets_into_account_risk() {
    use crate::common::basic_account_msg::{
        get_basic_event_type, split_basic_account_event, BasicAccountEventType,
        BasicAccountRiskMsg, BasicAccountScope,
    };

    let parser = GateAccountEventParser::new();
    let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
    // 用户提供的实际 push 样本
    let json = r#"{
        "time": 1700625194,
        "channel": "unified.assets",
        "event": "update",
        "result": {
            "u": 9008,
            "t": 1700625194,
            "r": "18.56",
            "R": "20.10",
            "b": "-1005719.51",
            "e": "-617985.29",
            "l": "1293939.74",
            "T": "675222.27",
            "a": "-1432719.62"
        }
    }"#;
    parser.parse(Bytes::from(json), &tx);
    let msgs: Vec<Bytes> = std::iter::from_fn(|| rx.try_recv().ok()).collect();

    let risk_msgs: Vec<_> = msgs
        .iter()
        .filter(|m| matches!(get_basic_event_type(m), BasicAccountEventType::AccountRisk))
        .collect();
    assert_eq!(risk_msgs.len(), 1, "expected exactly one AccountRisk event");

    let (_, scope, body) = split_basic_account_event(risk_msgs[0]).unwrap();
    assert_eq!(scope, BasicAccountScope::GateUnified);
    let risk = BasicAccountRiskMsg::from_bytes(body).unwrap();

    // 时间戳：seconds → ms
    assert_eq!(risk.timestamp, 1_700_625_194_000);
    // e = -617985.29 → adj_equity / actual_equity 同值
    assert!((risk.adj_equity_usd - (-617985.29)).abs() < 1e-3);
    assert!((risk.actual_equity_usd - (-617985.29)).abs() < 1e-3);
    // r = 18.56 (% of MB) → margin_ratio = 100/r = 5.388...（待部署后用真账号 vs 网页验证）
    assert!(
        (risk.margin_ratio - (100.0 / 18.56)).abs() < 1e-6,
        "got margin_ratio={}",
        risk.margin_ratio
    );
    // mm = T × r / 100 = 675222.27 × 0.1856
    assert!((risk.maintenance_margin_usd - 675222.27 * 0.1856).abs() < 1e-2);
    // im = T × R / 100 = 675222.27 × 0.2010
    assert!((risk.initial_margin_usd - 675222.27 * 0.2010).abs() < 1e-2);
    // borrowed: abs(b) = 1005719.51
    assert!((risk.borrowed_usd - 1005719.51).abs() < 1e-3);
    // notional 不在 channel 里
    assert_eq!(risk.notional_usd, 0.0);
}

#[test]
fn unified_assets_skips_when_r_is_zero() {
    use crate::common::basic_account_msg::{get_basic_event_type, BasicAccountEventType};
    let parser = GateAccountEventParser::new();
    let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
    let json = r#"{
        "time": 1700625194,
        "channel": "unified.assets",
        "event": "update",
        "result": {
            "t": 1700625194,
            "r": "0",
            "R": "0",
            "b": "0",
            "e": "1000",
            "l": "0",
            "T": "1000",
            "a": "1000"
        }
    }"#;
    parser.parse(Bytes::from(json), &tx);
    let msgs: Vec<Bytes> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    // 仍要 emit，只是 margin_ratio = 0（表示无敞口/无 MMR）
    let risk_count = msgs
        .iter()
        .filter(|m| matches!(get_basic_event_type(m), BasicAccountEventType::AccountRisk))
        .count();
    assert_eq!(risk_count, 1);
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
cargo test --lib parser::gate_account_event_parser::tests::parses_unified_assets_into_account_risk 2>&1 | head -30
```

预期：FAIL（router 还没有 `unified.assets` 分支，没事件被 emit）。

- [ ] **Step 3: 实现 `parse_unified_assets`**

在 `src/parser/gate_account_event_parser.rs` 的 `impl GateAccountEventParser` 块内（建议放在 `parse_unified_asset_detail` 旁边）追加：

```rust
/// 解析 unified.assets 频道（账户级聚合风险快照）。
///
/// 字段约定（基于 Gate REST UnifiedAccount model 推断 + 实际 push 样本）：
/// - r: total_maintenance_margin_rate (%) = MM / MB × 100
/// - R: total_initial_margin_rate (%)     = IM / MB × 100
/// - b: borrowed_usd (signed, USD 等值)
/// - e: unified_account_total_equity (USD)
/// - l: unified_account_total_liab (USD)
/// - T: total_margin_balance (USD)
/// - a: total_available_margin (USD)
///
/// 注意 `r` 单位需要在部署后对账网页一次（详见 Plan Step 5 验证）。
/// 如果 Gate 实际返回的是 ratio（MB/MM）而非百分比（MM/MB%），则要把下面的转换公式翻转。
fn parse_unified_assets(
    &self,
    json_value: &serde_json::Value,
    tx: &mpsc::UnboundedSender<Bytes>,
) -> usize {
    let Some(result) = json_value.get("result") else {
        warn!("Gate: unified.assets missing result");
        return 0;
    };

    let timestamp_sec = result
        .get("t")
        .and_then(|v| v.as_i64())
        .unwrap_or_else(|| {
            json_value
                .get("time")
                .and_then(|v| v.as_i64())
                .unwrap_or(0)
        });
    let timestamp_ms = timestamp_sec * 1000;

    let parse_field = |key: &str| -> f64 {
        result
            .get(key)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0)
    };

    let r_pct = parse_field("r");
    let r_cap_pct = parse_field("R");
    let total_margin = parse_field("T");
    let equity = parse_field("e");
    let borrowed_signed = parse_field("b");

    // margin_ratio: 把 Gate 的 MM/MB% 翻成统一的 MB/MM ratio（1.0 = 强平）
    // r=0 时设 0（无敞口，下游凭 timestamp 判新鲜度，不依赖 0 触发 gate）
    let margin_ratio = if r_pct.abs() > 1e-9 {
        100.0 / r_pct
    } else {
        0.0
    };
    let mm = total_margin * (r_pct / 100.0);
    let im = total_margin * (r_cap_pct / 100.0);

    let risk_msg = crate::common::basic_account_msg::BasicAccountRiskMsg::create(
        timestamp_ms,
        equity,                  // adj_equity_usd（Gate 不区分 adjusted 与 actual）
        equity,                  // actual_equity_usd 同值
        mm,
        im,
        margin_ratio,
        borrowed_signed.abs(),   // borrowed_usd 取绝对值
        0.0,                     // notional_usd 不在 channel
    );
    let payload = risk_msg.to_bytes();
    let event = BasicAccountEventMsg::create(
        BasicAccountEventType::AccountRisk,
        BasicAccountScope::GateUnified,
        payload,
    );
    if tx.send(event.to_bytes()).is_ok() {
        1
    } else {
        0
    }
}
```

- [ ] **Step 4: 在 channel router 加分支**

定位 `src/parser/gate_account_event_parser.rs:1063` 附近的 `match channel { ... }` 块，在 `"unified.asset_detail" => { ... }` 之后追加：

```rust
"unified.assets" => self.parse_unified_assets(&json_value, tx),
```

- [ ] **Step 5: 在 gate_account_monitor 订阅 channel**

定位 `src/bin/gate_account_monitor.rs:99-103` 现有的 channels 列表（应该是 `vec![SubscribeChannel { channel: "unified.asset_detail".to_string(), ... }]`），在数组里追加一项：

```rust
SubscribeChannel {
    channel: "unified.assets".to_string(),
    payload: vec!["!all".to_string()],
},
```

- [ ] **Step 6: 运行测试确认 PASS**

```bash
cargo test --lib parser::gate_account_event_parser::tests::parses_unified_assets_into_account_risk
cargo test --lib parser::gate_account_event_parser::tests::unified_assets_skips_when_r_is_zero
```

预期：2 passed。同时跑全 parser 测试确认无回归：

```bash
cargo test --lib parser::gate_account_event_parser
```

- [ ] **Step 7: 编译 + 手动冒烟（gate 账号环境）**

```bash
cargo build --release --bin gate_account_monitor
```

切到 gate intra/arb 账号 env：

```bash
source ~/gate_intra_arb02/env.sh  # 或你 gate 账号对应的 env
cd ~/gate_intra_arb02
RUST_LOG=info ~/crypto_mkt/mkt_signal/target/release/gate_account_monitor 2>&1 | \
    grep -E "unified\.assets|AccountRisk" | head -5
```

预期：连上后能看到 `unified.assets` 订阅成功 + 至少 1 条 `AccountRisk` 事件（带 `scope=gate_unified`）。

**单位验证（关键）:** 同时浏览器打开 Gate 网页"统一账户"页面看显示的"维持保证金率"或"账户保证金率"数值，对照 log 里抓到的 `margin_ratio`：

- 如果网页显示是 18.56% 而 `margin_ratio` 输出 5.39（≈100/18.56），说明 `r` 是 MM/MB%，**parser 转换正确**
- 如果网页显示 18.56 倍（也叫 ratio），而 parser 输出 5.39，说明转换公式反了，要改成 `margin_ratio = r_pct`（直接用）
- 如果网页显示完全不同的数字，开 Gate dev tools 看网页内部用的字段名，与 API `r` 对齐

记录验证结果到 commit message 里供以后排查。

- [ ] **Step 8: Commit**

```bash
git add src/parser/gate_account_event_parser.rs src/bin/gate_account_monitor.rs
git commit -m "feat(gate): subscribe unified.assets and emit BasicAccountRiskMsg

Verified r unit against Gate web UI: r is MM/MB%, so margin_ratio = 100/r."
```

（commit message 里记下验证结果，让后人能直接看出转换方向是否对过。）

---

## Task 5: Bitget UTA `account` channel 顶层 risk 抽取

**背景:** Bitget UTA WS `account` channel **已订阅**（`src/portfolio_margin/bitget_auth.rs:91` `build_account_subscribe_message`，`instType=UTA topic=account`），但当前 parser `parse_account_channel`（`src/parser/bitget_account_event_parser.rs:69`）只读 `coin[]` 和 `marginCoin` 等 per-coin 字段，**忽略了顶层风险字段**。Bitget UTA `account` 推送的顶层结构与 REST `AccountAssetsV3` 一致（参考 [tiagosiebler/bitget-api SDK](https://github.com/tiagosiebler/bitget-api/blob/master/src/types/response/v3/account.ts) ）：

| Bitget WS 顶层字段 | 含义 | → BasicAccountRiskMsg |
|---|---|---|
| `accountEquity` | 账户总权益（USD）| `actual_equity_usd` |
| `effEquity` | 调整后权益（含 collateral haircut，USD）| `adj_equity_usd` |
| `mmr` | 维持保证金（USD）| `maintenance_margin_usd` |
| `imr` | 初始保证金（USD）| `initial_margin_usd` |
| `mgnRatio` | 保证金率（单位待验证）| `margin_ratio`（部署后对账网页确认是 ratio 还是 percentage）|
| `usdtUnrealisedPnl` | 未实现 PnL（USDT） | （不用，下游 UPL 已经从 BasicUmUnrealizedMsg 拿到）|
| `unrealisedPnl` | 未实现 PnL（USD） | 同上 |
| `positionMgnRatio` | 仓位级保证金率 | （不用，账户级 mgnRatio 已够）|

注：因为 Bitget 官方 docs 被 Cloudflare 墙了精确字段示例拿不到，**字段名以实际 WS 抓包为准**——Step 5 单元测试用真实抓包样本回灌，Step 6 部署后看 log 校验。

**Files:**
- Modify: `src/parser/bitget_account_event_parser.rs` 顶部 struct + `parse_account_channel` 函数

- [ ] **Step 1: 实跑抓 raw 样本**

在改代码之前，先在 bitget 账号 env 下运行账号监控，抓一条 `account` channel push 的原始 JSON 落地。直接 `tail` 现有 PM2 log，或者临时用 `RUST_LOG=debug` 跑 `bitget_account_monitor` 并 grep `"channel":"account"`。把抓到的 raw 样本保存到剪贴板/本地文件，下面 Step 2 测试要拿这个真实 payload 灌测试。

如果 Bitget UTA 顶层确实推 `mmr`/`mgnRatio`/`effEquity` 字段：继续 Step 2-6。
如果顶层**没有**这些字段（只在 per-coin 里有，或者完全不推）：**STOP**，回头讨论是否改用 REST `/api/v3/account/assets` 轮询（与 Binance 同模式，需要在 `bitget_account_monitor` 新增 5s poller，复用现有 `local_ip` 配置）。

- [ ] **Step 2: 加测试（用 Step 1 抓的真实样本）**

在 `src/parser/bitget_account_event_parser.rs` 的 `#[cfg(test)] mod tests` 块追加：

```rust
#[test]
fn parse_account_channel_emits_account_risk_with_top_level_metrics() {
    use crate::common::basic_account_msg::{
        get_basic_event_type, split_basic_account_event, BasicAccountEventType,
        BasicAccountRiskMsg, BasicAccountScope,
    };
    let parser = BitgetAccountEventParser::new();
    let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
    // TODO Step 1: 把这块 JSON 替换成 Step 1 抓到的真实 UTA account 推送
    let json = r#"{
        "action":"snapshot",
        "arg":{"instType":"UTA","topic":"account"},
        "ts":"1700625194000",
        "data":[{
            "uTime":"1700625194000",
            "accountEquity":"100123.45",
            "effEquity":"99876.50",
            "mmr":"1500.00",
            "imr":"8000.00",
            "mgnRatio":"5.23",
            "unrealisedPnl":"123.45",
            "coin":[
                {"coin":"USDT","equity":"50000","balance":"50000","debt":"0","borrow":"0"}
            ]
        }]
    }"#;
    parser.parse(Bytes::from(json), &tx);
    let msgs: Vec<Bytes> = std::iter::from_fn(|| rx.try_recv().ok()).collect();

    let risk_msgs: Vec<_> = msgs
        .iter()
        .filter(|m| matches!(get_basic_event_type(m), BasicAccountEventType::AccountRisk))
        .collect();
    assert_eq!(risk_msgs.len(), 1, "expected exactly one AccountRisk event");

    let (_, scope, body) = split_basic_account_event(risk_msgs[0]).unwrap();
    assert_eq!(scope, BasicAccountScope::BitgetUnified);
    let risk = BasicAccountRiskMsg::from_bytes(body).unwrap();
    assert_eq!(risk.timestamp, 1_700_625_194_000);
    assert!((risk.adj_equity_usd - 99876.50).abs() < 1e-3);
    assert!((risk.actual_equity_usd - 100123.45).abs() < 1e-3);
    assert!((risk.maintenance_margin_usd - 1500.0).abs() < 1e-3);
    assert!((risk.initial_margin_usd - 8000.0).abs() < 1e-3);
    // 单位假设：mgnRatio 直接是 ratio（5.23 = MB/MM）。
    // 部署后若网页显示是百分比（如 523%），把 parser 改成 mgnRatio / 100.0。
    assert!((risk.margin_ratio - 5.23).abs() < 1e-6);
}

#[test]
fn parse_account_channel_skips_risk_when_top_level_missing() {
    use crate::common::basic_account_msg::{get_basic_event_type, BasicAccountEventType};
    let parser = BitgetAccountEventParser::new();
    let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
    let json = r#"{
        "action":"snapshot",
        "arg":{"instType":"UTA","topic":"account"},
        "data":[{"uTime":"1700625194000","coin":[
            {"coin":"USDT","equity":"50000","balance":"50000"}
        ]}]
    }"#;
    parser.parse(Bytes::from(json), &tx);
    let msgs: Vec<Bytes> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    let count = msgs
        .iter()
        .filter(|m| matches!(get_basic_event_type(m), BasicAccountEventType::AccountRisk))
        .count();
    assert_eq!(count, 0);
}
```

- [ ] **Step 3: 运行测试，确认失败**

```bash
cargo test --lib parser::bitget_account_event_parser::tests::parse_account_channel_emits_account_risk_with_top_level_metrics 2>&1 | head -30
```

预期：FAIL（顶层 risk 还没解出）。

- [ ] **Step 4: 实现顶层 risk 抽取**

修改 `src/parser/bitget_account_event_parser.rs`：

**4a.** `BitgetAccountChannelRow` struct（约 18-40 行）追加新字段：

```rust
#[derive(Debug, Deserialize)]
struct BitgetAccountChannelRow {
    #[serde(default, rename = "uTime")]
    u_time: String,
    #[serde(default, rename = "updatedTime")]
    updated_time: String,
    #[serde(default)]
    ts: String,
    #[serde(default)]
    coin: Vec<BitgetAccountChannelCoin>,
    #[serde(default, rename = "marginCoin")]
    margin_coin: String,
    #[serde(default)]
    balance: String,
    #[serde(default)]
    equity: String,
    #[serde(default)]
    debt: String,
    #[serde(default)]
    borrow: String,
    #[serde(default)]
    debts: String,
    // 新增：UTA 顶层 risk 字段
    #[serde(default, rename = "accountEquity")]
    account_equity: String,
    #[serde(default, rename = "effEquity")]
    eff_equity: String,
    #[serde(default)]
    mmr: String,
    #[serde(default)]
    imr: String,
    #[serde(default, rename = "mgnRatio")]
    mgn_ratio: String,
}
```

**4b.** `parse_account_channel`（约 69-109 行）在 `count += self.emit_account_coin(...)` 循环之后、`count` 返回之前插入 risk emit：

```rust
fn parse_account_channel(
    &self,
    json_value: &Value,
    tx: &mpsc::UnboundedSender<Bytes>,
) -> usize {
    let mut count = 0;

    let top_timestamp = parse_i64_str_or_num(json_value.get("ts"))
        .or_else(|| parse_i64_str_or_num(json_value.get("timestamp")))
        .unwrap_or(0);

    for account in collect_data_objects(json_value) {
        let Ok(account_row) =
            serde_json::from_value::<BitgetAccountChannelRow>(Value::Object(account.clone()))
        else {
            continue;
        };
        let timestamp = parse_i64_str(&account_row.u_time)
            .or_else(|| parse_i64_str(&account_row.updated_time))
            .or_else(|| parse_i64_str(&account_row.ts))
            .unwrap_or(top_timestamp);

        if !account_row.coin.is_empty() {
            for coin_item in &account_row.coin {
                count += self.emit_account_coin(coin_item, timestamp, tx);
            }
        } else if !account_row.margin_coin.is_empty() {
            let coin_item = BitgetAccountChannelCoin {
                coin: account_row.margin_coin.clone(),
                balance: account_row.balance.clone(),
                equity: account_row.equity.clone(),
                debt: account_row.debt.clone(),
                borrow: account_row.borrow.clone(),
                debts: account_row.debts.clone(),
            };
            count += self.emit_account_coin(&coin_item, timestamp, tx);
        }

        // 顶层 UTA 账户级风险：至少 mgnRatio / mmr / effEquity / accountEquity 任一非空就 emit
        let has_risk = !account_row.mgn_ratio.trim().is_empty()
            || !account_row.mmr.trim().is_empty()
            || !account_row.eff_equity.trim().is_empty()
            || !account_row.account_equity.trim().is_empty();
        if has_risk {
            let parse_f64 = |s: &str| s.trim().parse::<f64>().unwrap_or(0.0);
            let actual_eq = parse_f64(&account_row.account_equity);
            let adj_eq = {
                let v = parse_f64(&account_row.eff_equity);
                if v.abs() > 0.0 { v } else { actual_eq }
            };
            let mmr_v = parse_f64(&account_row.mmr);
            let imr_v = parse_f64(&account_row.imr);
            let mgn_ratio_raw = parse_f64(&account_row.mgn_ratio);
            // 单位假设：mgnRatio 已是 ratio (5.23 表示 MB/MM = 5.23x)。
            // 部署后若网页显示百分比（523%），改成 mgn_ratio_raw / 100.0。
            let margin_ratio = mgn_ratio_raw;

            let risk_msg = crate::common::basic_account_msg::BasicAccountRiskMsg::create(
                timestamp,
                adj_eq,
                actual_eq,
                mmr_v,
                imr_v,
                margin_ratio,
                0.0, // borrowed_usd: 顶层不带聚合借币 USD（per-coin debt 仍走 BorrowInterest）
                0.0, // notional_usd: 顶层不带
            );
            let payload = risk_msg.to_bytes();
            let event = BasicAccountEventMsg::create(
                BasicAccountEventType::AccountRisk,
                BasicAccountScope::BitgetUnified,
                payload,
            );
            if tx.send(event.to_bytes()).is_ok() {
                count += 1;
            }
        }
    }

    count
}
```

- [ ] **Step 5: 运行测试，确认 PASS**

```bash
cargo test --lib parser::bitget_account_event_parser
```

预期：所有原有测试 + 2 新测试 PASS。

- [ ] **Step 6: 手动冒烟（bitget 账号环境）**

```bash
cargo build --release --bin bitget_account_monitor
source ~/<bitget-env>/env.sh
cd ~/<bitget-env>
RUST_LOG=info ~/crypto_mkt/mkt_signal/target/release/bitget_account_monitor 2>&1 | \
    grep -E "AccountRisk|topic.*account" | head -10
```

预期：连上 UTA 私有频道后至少 1 条 `AccountRisk` 事件（带 `scope=bitget_unified`）。

**单位验证（关键）:** 同时浏览器打开 Bitget 网页"统一账户"页面看显示的"账户保证金率"或"维持保证金率"数值，对照 log 里的 `margin_ratio`：

- 如果网页显示 523% 而 `margin_ratio` 输出 5.23，**parser 转换正确**（mgnRatio 是百分比，要除 100；改 Step 4b 里 `margin_ratio = mgn_ratio_raw / 100.0`）
- 如果网页显示 5.23（倍数），parser 输出也是 5.23，**直接用，不用改**
- 如果网页显示完全不同的数字，开 Bitget dev tools 看网页 API 调用，确认字段名

记录验证结果到 commit message。

- [ ] **Step 7: Commit**

```bash
git add src/parser/bitget_account_event_parser.rs
git commit -m "feat(bitget): emit BasicAccountRiskMsg from UTA account channel top-level fields

Verified mgnRatio unit against Bitget web UI: <ratio | percentage>."
```

---

## Task 6: Binance PM 风险轮询任务

**Files:**
- Modify: `src/bin/binance_account_monitor.rs` (`main` 函数 + 新增 spawner + log + dedup)

- [ ] **Step 1: 加 import**

在 `src/bin/binance_account_monitor.rs` 顶部 import 区域追加：

```rust
use mkt_signal::common::basic_account_msg::BasicAccountRiskMsg;
use mkt_signal::trade_engine::query_parsers::binance_pm_account_risk::parse_binance_pm_account_risk;
use tokio::time::MissedTickBehavior;
```

（如果已经 import 了部分项目，跳过对应行。）

- [ ] **Step 2: 加 risk poller spawner 函数**

在 `bootstrap_standard_snapshots` 之后（约 220 行）插入新函数：

```rust
fn spawn_pm_risk_poller(
    api_key: String,
    api_secret: String,
    local_ip: Option<String>,
    interval_secs: u64,
    evt_tx: tokio::sync::mpsc::UnboundedSender<Bytes>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let client = match build_binance_rest_client(local_ip.as_deref(), Duration::from_secs(10))
        {
            Ok(c) => c,
            Err(e) => {
                error!("pm_risk_poller: build client failed: {:#}", e);
                return;
            }
        };

        info!(
            "pm_risk_poller started: interval={}s local_ip={}",
            interval_secs,
            local_ip.as_deref().unwrap_or("system-default")
        );

        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    info!("pm_risk_poller shutting down");
                    break;
                }
                _ = tick.tick() => {
                    match signed_get_binance(
                        &client,
                        "https://papi.binance.com",
                        "/papi/v1/account",
                        &api_key,
                        &api_secret,
                    )
                    .await
                    {
                        Ok(body) => {
                            if let Some(payload) = parse_binance_pm_account_risk(&body) {
                                let event = BasicAccountEventMsg::create(
                                    BasicAccountEventType::AccountRisk,
                                    BasicAccountScope::BinanceUnified,
                                    payload,
                                );
                                if evt_tx.send(event.to_bytes()).is_err() {
                                    warn!("pm_risk_poller: evt_tx closed, exiting");
                                    break;
                                }
                            } else {
                                warn!("pm_risk_poller: parse failed body_len={}", body.len());
                            }
                        }
                        Err(e) => warn!("pm_risk_poller: GET /papi/v1/account failed: {:#}", e),
                    }
                }
            }
        }
    });
}
```

- [ ] **Step 3: 在 main 里启用 poller（仅 UNIFIED）**

定位 `main` 函数中 `if binance_is_standard { ... bootstrap_standard_snapshots ... }`（约 357-363 行），在该 `if` 块**之后**插入：

```rust
if !binance_is_standard {
    let interval = std::env::var("BINANCE_PM_RISK_POLL_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(5);
    spawn_pm_risk_poller(
        api_key.clone(),
        api_secret.clone(),
        Some(primary_ip.clone()),
        interval,
        evt_tx.clone(),
        shutdown_rx.clone(),
    );
}
```

- [ ] **Step 4: 加 dedup key 防止重复转发**

定位 `AccountEventDeduper::should_forward`（约 736-786 行）的 `match event_type` 块，加 AccountRisk 分支：

```rust
BasicAccountEventType::TradeUpdateLite => BinanceTradeLiteMsg::from_bytes(&payload)
    .ok()
    .map(|m| self.key_binance_trade_lite(&m)),
BasicAccountEventType::AccountRisk => BasicAccountRiskMsg::from_bytes(&payload)
    .ok()
    .map(|m| self.key_account_risk(&m)),
BasicAccountEventType::OrderUpdate => ...
```

并在 `impl AccountEventDeduper` 块末尾追加：

```rust
fn key_account_risk(&self, msg: &BasicAccountRiskMsg) -> u64 {
    self.hash64(&[
        BasicAccountEventType::AccountRisk as u32 as u64,
        msg.timestamp as u64,
        msg.adj_equity_usd.to_bits(),
        msg.maintenance_margin_usd.to_bits(),
        msg.margin_ratio.to_bits(),
    ])
}
```

- [ ] **Step 5: 加 log 分支**

定位 `log_parsed_event` 中 `match event_type`（约 624-715 行），在 `BasicAccountEventType::Error => {}` 之前插入：

```rust
BasicAccountEventType::AccountRisk => {
    if let Ok(m) = BasicAccountRiskMsg::from_bytes(&payload) {
        info!(
            "Binance AccountRisk: scope={} ts={} adj_eq={:.2} actual_eq={:.2} mmr={:.2} imr={:.2} margin_ratio={:.4}",
            account_scope.as_str(),
            m.timestamp,
            m.adj_equity_usd,
            m.actual_equity_usd,
            m.maintenance_margin_usd,
            m.initial_margin_usd,
            m.margin_ratio,
        );
    }
}
```

- [ ] **Step 6: 编译**

```bash
cargo build --release --bin binance_account_monitor 2>&1 | tail -20
```

预期：编译通过。

- [ ] **Step 7: 手动冒烟（在 binance_fr_arb02 环境）**

```bash
source ~/binance_fr_arb02/env.sh
cd ~/binance_fr_arb02
RUST_LOG=info BINANCE_PM_RISK_POLL_INTERVAL_SECS=5 \
  ~/crypto_mkt/mkt_signal/target/release/binance_account_monitor 2>&1 | grep -E "pm_risk_poller|AccountRisk" | head -10
```

预期：5s 内看到 `pm_risk_poller started` + 至少 1 条 `Binance AccountRisk:` 日志，包含合理的 `margin_ratio` 数值。Ctrl-C 退出。

- [ ] **Step 8: Commit**

```bash
git add src/bin/binance_account_monitor.rs
git commit -m "feat(binance): poll /papi/v1/account every 5s on UNIFIED mode for risk metrics"
```

---

## Task 7: pre_trade 缓存 AccountRisk 事件

**Files:**
- Modify: `src/pre_trade/monitor_channel.rs` (新增 `AccountRiskSnapshot`、`risk_cache` 字段、dispatch 分支、accessor、单元测试)

- [ ] **Step 1: 加 snapshot 类型与 inner 字段**

在 `src/pre_trade/monitor_channel.rs` 顶部 `use` 区域之后、`MonitorChannelInner` 定义之前插入：

```rust
use crate::common::basic_account_msg::BasicAccountRiskMsg;

/// 单 venue/scope 的账户级风险快照
#[derive(Debug, Clone, Copy, Default)]
pub struct AccountRiskSnapshot {
    pub timestamp: i64,
    pub adj_equity_usd: f64,
    pub actual_equity_usd: f64,
    pub maintenance_margin_usd: f64,
    pub initial_margin_usd: f64,
    pub margin_ratio: f64,
    pub borrowed_usd: f64,
    pub notional_usd: f64,
}

impl AccountRiskSnapshot {
    pub fn from_msg(msg: &BasicAccountRiskMsg) -> Self {
        Self {
            timestamp: msg.timestamp,
            adj_equity_usd: msg.adj_equity_usd,
            actual_equity_usd: msg.actual_equity_usd,
            maintenance_margin_usd: msg.maintenance_margin_usd,
            initial_margin_usd: msg.initial_margin_usd,
            margin_ratio: msg.margin_ratio,
            borrowed_usd: msg.borrowed_usd,
            notional_usd: msg.notional_usd,
        }
    }
}
```

定位 `MonitorChannelInner` struct（用 `grep -n "pub struct MonitorChannelInner\|struct MonitorChannelInner" src/pre_trade/monitor_channel.rs` 找），在字段列表末尾追加：

```rust
risk_cache: HashMap<BasicAccountScope, AccountRiskSnapshot>,
```

定位 `MonitorChannel::init_singleton`（约 766 行）里 `MonitorChannelInner { ... }` 的字面量构造（用 `grep -n "MonitorChannelInner {" src/pre_trade/monitor_channel.rs` 定位），在初始化字段末尾追加：

```rust
risk_cache: HashMap::new(),
```

- [ ] **Step 2: 加 dispatch 分支**

定位事件循环 `match msg_type` 块（约 1526-1687 行），在 `BasicAccountEventType::Error => {}` 之前插入（并保持现有 `BasicAccountEventType::TradeUpdateLite => {}` 分支不动）：

```rust
BasicAccountEventType::AccountRisk => {
    if let Ok(msg) = BasicAccountRiskMsg::from_bytes(data) {
        MonitorChannel::with_inner(|inner| {
            inner
                .risk_cache
                .insert(account_scope, AccountRiskSnapshot::from_msg(&msg));
        });
    }
}
```

注意：用 `MonitorChannel::with_inner` 而不是直接拿 `inner` mutable 引用——保持与同一循环里其他分支（OrderUpdate 等）的访问模式一致。如果当前循环已经有 `inner` mutable 借用，参考其他 mut 分支（PositionUpdate）的写法做匹配。

如果事件循环里没法直接访问 `with_inner`（因为正在 borrow 别的字段），改用：

```rust
BasicAccountEventType::AccountRisk => {
    if let Ok(msg) = BasicAccountRiskMsg::from_bytes(data) {
        MonitorChannel::instance().update_risk_snapshot(account_scope, &msg);
    }
}
```

并在 `impl MonitorChannel`（约 600+ 行）加 helper：

```rust
pub fn update_risk_snapshot(&self, scope: BasicAccountScope, msg: &BasicAccountRiskMsg) {
    Self::with_inner(|inner| {
        inner
            .risk_cache
            .insert(scope, AccountRiskSnapshot::from_msg(msg));
    });
}
```

- [ ] **Step 3: 加只读 accessor**

在 `impl MonitorChannel` 块（与 `open_balance_mgr()` 等 accessor 同区域）追加：

```rust
/// 获取指定 scope 的最新 account risk 快照（如果有）。
pub fn account_risk_snapshot(&self, scope: BasicAccountScope) -> Option<AccountRiskSnapshot> {
    Self::with_inner(|inner| inner.risk_cache.get(&scope).copied())
}

/// 给定 venue 解析出对应 scope 后查 risk 快照。
pub fn account_risk_for_venue(&self, venue: TradingVenue) -> Option<AccountRiskSnapshot> {
    let mode = self.binance_account_mode_opt();
    let scope = scope_for_venue(venue, mode);
    self.account_risk_snapshot(scope)
}
```

注意：`scope_for_venue` 与 `binance_account_mode_opt` 是已有 helper（用 `grep -n "fn scope_for_venue\|binance_account_mode_opt" src/pre_trade/monitor_channel.rs` 确认）。如签名不一致，按当前文件的实际签名调整调用。

- [ ] **Step 4: 加单元测试**

在 `src/pre_trade/monitor_channel.rs` 末尾的 `#[cfg(test)] mod tests` 块（约 2540+ 行）追加：

```rust
#[test]
fn risk_cache_round_trips_via_update_helper() {
    use crate::common::basic_account_msg::{BasicAccountEventType, BasicAccountScope};
    // 注意：此测试要求 init_singleton 已被某个测试初始化过；
    // 如果是独立运行，先用 with_inner 直接构造或跳过。
    let msg = BasicAccountRiskMsg::create(
        1_700_000_000_000,
        100_000.0,
        99_500.0,
        1_500.0,
        4_000.0,
        5.23,
        100.0,
        300_000.0,
    );
    MonitorChannel::instance().update_risk_snapshot(BasicAccountScope::BinanceUnified, &msg);
    let snap = MonitorChannel::instance()
        .account_risk_snapshot(BasicAccountScope::BinanceUnified)
        .expect("snapshot present");
    assert!((snap.margin_ratio - 5.23).abs() < 1e-9);
    assert!((snap.maintenance_margin_usd - 1500.0).abs() < 1e-9);
    assert_eq!(snap.timestamp, 1_700_000_000_000);
}
```

- [ ] **Step 5: 编译 + 测试**

```bash
cargo test --lib pre_trade::monitor_channel::tests::risk_cache_round_trips_via_update_helper 2>&1 | tail -20
```

预期：PASS。如果失败因为 init_singleton 未初始化，加 `#[ignore]` 跳过单测，依赖手动冒烟验证（Step 8）。

完整编译：

```bash
cargo build --release 2>&1 | tail -10
```

预期：无错误。

- [ ] **Step 6: Commit**

```bash
git add src/pre_trade/monitor_channel.rs
git commit -m "feat(pre_trade): cache BasicAccountRiskMsg per scope in MonitorChannel"
```

---

## Task 8: 把 risk 字段写进 pre_trade_risk 频道与 viz JSON

**Files:**
- Modify: `src/viz/resample.rs:88-115` (扩展 `PreTradeRiskResampleEntry` / `PreTradeVenueRiskResampleEntry`)
- Modify: `src/pre_trade/resample_channel.rs::publish_resample_entries` (填字段)
- Modify: `src/viz/subscribers.rs:163-177` (输出 JSON)

- [ ] **Step 1: 扩展 viz/resample.rs 字段**

修改 `src/viz/resample.rs` 中的两个 struct：

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AccountRiskSnapshotResample {
    pub ts_ms: i64,
    pub adj_equity_usd: f64,
    pub actual_equity_usd: f64,
    pub maintenance_margin_usd: f64,
    pub initial_margin_usd: f64,
    pub margin_ratio: f64,
    pub borrowed_usd: f64,
    pub notional_usd: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// 来自 Binance/OKX 自身公式的 open 腿账户级风险快照（缺失则全 0）
    #[serde(default)]
    pub open_account_risk: AccountRiskSnapshotResample,
    /// 来自 Binance/OKX 自身公式的 hedge 腿账户级风险快照
    #[serde(default)]
    pub hedge_account_risk: AccountRiskSnapshotResample,
}
```

`#[serde(default)]` 是为了向前兼容旧消费者（旧二进制反序列化新数据时这两个字段缺省全 0，不会崩）。

`PreTradeVenueRiskResampleEntry` 不动（原有结构）。

- [ ] **Step 2: 填字段（resample_channel.rs）**

修改 `src/pre_trade/resample_channel.rs::publish_resample_entries`（约 756-790 行的 risk 发布块）。先在文件顶部 import：

```rust
use crate::common::basic_account_msg::BasicAccountScope;
use crate::pre_trade::monitor_channel::AccountRiskSnapshot;
use crate::viz::resample::AccountRiskSnapshotResample;
```

然后定位 `let entry = PreTradeRiskResampleEntry { ... }` 字面量构造（约 777-790 行），把它改成：

```rust
let scope_for = |venue: TradingVenue| {
    let mode = mon.binance_account_mode_opt();
    crate::pre_trade::monitor_channel::scope_for_venue(venue, mode)
};
let to_resample = |snap: AccountRiskSnapshot| AccountRiskSnapshotResample {
    ts_ms: snap.timestamp,
    adj_equity_usd: snap.adj_equity_usd,
    actual_equity_usd: snap.actual_equity_usd,
    maintenance_margin_usd: snap.maintenance_margin_usd,
    initial_margin_usd: snap.initial_margin_usd,
    margin_ratio: snap.margin_ratio,
    borrowed_usd: snap.borrowed_usd,
    notional_usd: snap.notional_usd,
};
let open_account_risk = mon
    .account_risk_snapshot(scope_for(mon.open_venue()))
    .map(to_resample)
    .unwrap_or_default();
let hedge_account_risk = mon
    .account_risk_snapshot(scope_for(mon.hedge_venue()))
    .map(to_resample)
    .unwrap_or_default();

let entry = PreTradeRiskResampleEntry {
    ts_ms,
    signal_counts,
    total_equity,
    total_exposure: total_abs_exposure,
    total_position,
    spot_equity_usd,
    borrowed_usd,
    interest_usd,
    um_unrealized_usd,
    leverage,
    max_leverage,
    open_leg,
    hedge_leg,
    open_account_risk,
    hedge_account_risk,
};
```

注意：如果 `binance_account_mode_opt` / `scope_for_venue` 不是 pub，先把它们改成 pub(crate)（最小可见性扩大）；或者在 MonitorChannel 上加包装：`account_risk_for_venue(venue)`，把 scope 解析封装在内（推荐——已经在 Task 7 Step 3 提到过）。**首选方案**：直接用 `mon.account_risk_for_venue(mon.open_venue())`：

```rust
let open_account_risk = mon
    .account_risk_for_venue(mon.open_venue())
    .map(to_resample)
    .unwrap_or_default();
let hedge_account_risk = mon
    .account_risk_for_venue(mon.hedge_venue())
    .map(to_resample)
    .unwrap_or_default();
```

- [ ] **Step 3: viz/subscribers.rs 输出新字段**

修改 `src/viz/subscribers.rs:163-177` 的 `entry` JSON 字面量，在 `"hedge_leg": entry.hedge_leg,` 之后追加：

```rust
"open_account_risk": entry.open_account_risk,
"hedge_account_risk": entry.hedge_account_risk,
```

由于 `AccountRiskSnapshotResample` 已经 `Serialize`，serde_json 会直接展平字段。

- [ ] **Step 4: 编译**

```bash
cargo build --release 2>&1 | tail -20
```

预期：通过。如果有 `binance_account_mode_opt` / `scope_for_venue` 可见性问题，按上面 Step 2 的"首选方案"使用 `account_risk_for_venue`。

- [ ] **Step 5: 手动冒烟**

```bash
# 启动 binance_account_monitor 推 risk 事件
source ~/binance_fr_arb02/env.sh
cd ~/binance_fr_arb02
pm2 restart binance_account_monitor_binance_fr_arb02 2>/dev/null || \
  RUST_LOG=info ~/crypto_mkt/mkt_signal/target/release/binance_account_monitor &

# 等 ~10s 让 pre_trade 收一两轮
sleep 10

# 抓 viz snapshot 看 risk 字段
curl -s http://localhost:4191/fr/binance_fr_arb02/snapshot 2>/dev/null | \
  python3 -c "import sys,json; d=json.load(sys.stdin); \
  for e in d['entries']:
    if e.get('type')=='pre_trade_risk':
      print(json.dumps(e['entry'].get('open_account_risk'), indent=2))
      print(json.dumps(e['entry'].get('hedge_account_risk'), indent=2))
      break"
```

预期：`open_account_risk` 至少 `margin_ratio > 0`（OKX/Binance 自己算的）、`maintenance_margin_usd > 0`、`ts_ms` 是最近 10s 内的时间戳。

- [ ] **Step 6: Commit**

```bash
git add src/viz/resample.rs src/pre_trade/resample_channel.rs src/viz/subscribers.rs
git commit -m "feat(viz): publish per-leg account risk snapshot in pre_trade_risk channel"
```

---

## Task 9: （可选）基于 margin_ratio 的硬风控 gate（per-exchange floor）

> 这一步可以拆成独立 PR，先观察一段时间再决定阈值。如不立刻做，跳过 Task 9，整 plan 仍然完整可用。

**Files:**
- Modify: `src/pre_trade/monitor_channel.rs` (新方法 `check_account_risk`)
- Modify: `src/pre_trade/params_load.rs` (加 per-exchange `margin_ratio_floor` 配置)
- Modify: `src/strategy/open_strategy_common.rs:609` (在 `check_leverage` 之后调用 `check_account_risk`)

- [ ] **Step 1: 加 per-exchange 配置参数**

在 `src/pre_trade/params_load.rs` 找到 `pub struct PreTradeParams { ... }`（用 `grep -n "pub struct PreTradeParams\|max_leverage:" src/pre_trade/params_load.rs`）：

```rust
pub struct PreTradeParams {
    // ... 现有字段
    binance_margin_ratio_floor: f64,
    okex_margin_ratio_floor: f64,
    gate_margin_ratio_floor: f64,
    bitget_margin_ratio_floor: f64,
}
```

默认值全部 `0.0`（即"关闭，不 gate"），灰度安全。

Redis 加载逻辑参考 `max_leverage` 那段，4 个字段各加一段：

```rust
for (key, slot) in [
    ("binance_margin_ratio_floor", &mut data.binance_margin_ratio_floor),
    ("okex_margin_ratio_floor",    &mut data.okex_margin_ratio_floor),
    ("gate_margin_ratio_floor",    &mut data.gate_margin_ratio_floor),
    ("bitget_margin_ratio_floor",  &mut data.bitget_margin_ratio_floor),
] {
    if let Some(v) = parse_f64(key) {
        if v >= 0.0 { *slot = v; }
        else { warn!("{}={} 无效，需 >= 0，忽略更新", key, v); }
    }
}
```

加 accessor：

```rust
pub fn margin_ratio_floor_for(&self, scope: BasicAccountScope) -> f64 {
    PARAMS_DATA.with(|data| {
        let d = data.borrow();
        match scope {
            BasicAccountScope::BinanceUnified
            | BasicAccountScope::BinanceStdSpot
            | BasicAccountScope::BinanceStdUm => d.binance_margin_ratio_floor,
            BasicAccountScope::OkexUnified => d.okex_margin_ratio_floor,
            BasicAccountScope::GateUnified => d.gate_margin_ratio_floor,
            BasicAccountScope::BitgetUnified => d.bitget_margin_ratio_floor,
            _ => 0.0,
        }
    })
}
```

注意：`BasicAccountScope` 需要 `pub use` 到 params_load.rs 的依赖里（通常 `use crate::common::basic_account_msg::BasicAccountScope;`）。

- [ ] **Step 2: 加 check_account_risk**

在 `src/pre_trade/monitor_channel.rs` 的 `impl MonitorChannel` 块、`check_leverage` 旁边（约 1149 行附近）追加：

```rust
pub fn check_account_risk(&self) -> Result<(), String> {
    let loader = PreTradeParamsLoader::instance();
    let now_ms = get_timestamp_us() / 1000;
    const STALE_MS: i64 = 30_000;
    let mode = self.binance_account_mode_opt();

    for venue in [self.open_venue(), self.hedge_venue()] {
        let scope = scope_for_venue(venue, mode);
        let floor = loader.margin_ratio_floor_for(scope);
        if floor <= 0.0 {
            continue; // 该交易所的 floor 没配置，跳过 gate
        }
        if let Some(snap) = self.account_risk_snapshot(scope) {
            if now_ms - snap.timestamp > STALE_MS {
                debug!(
                    "account_risk snapshot stale for {:?}: now={} snap={}",
                    venue, now_ms, snap.timestamp
                );
                continue;
            }
            if snap.margin_ratio > 0.0 && snap.margin_ratio < floor {
                return Err(format!(
                    "{:?} margin_ratio={:.4} < floor={:.4} (mmr=${:.2} adj_eq=${:.2})",
                    venue,
                    snap.margin_ratio,
                    floor,
                    snap.maintenance_margin_usd,
                    snap.adj_equity_usd
                ));
            }
        }
    }
    Ok(())
}
```

- [ ] **Step 3: 接到 open 路径**

修改 `src/strategy/open_strategy_common.rs:609` 区域（`check_leverage` 调用处）：

```rust
if let Err(e) = MonitorChannel::instance().check_leverage() {
    return Err(e);
}
if let Err(e) = MonitorChannel::instance().check_account_risk() {
    return Err(e);
}
```

- [ ] **Step 4: 单元测试**

在 `src/pre_trade/monitor_channel.rs::tests` 追加：

```rust
#[test]
fn check_account_risk_passes_when_floor_zero() {
    // 所有 exchange floor 默认 0 → 不 gate
    PreTradeParamsLoader::instance().set_binance_margin_ratio_floor_async(0.0);
    assert!(MonitorChannel::instance().check_account_risk().is_ok());
}

#[test]
fn check_account_risk_blocks_when_below_floor_for_specific_exchange() {
    use crate::common::basic_account_msg::BasicAccountScope;
    PreTradeParamsLoader::instance().set_binance_margin_ratio_floor_async(2.0);
    // OKX floor 还是 0，确认只对 binance scope 触发 gate
    PreTradeParamsLoader::instance().set_okex_margin_ratio_floor_async(0.0);
    let bad = BasicAccountRiskMsg::create(
        get_timestamp_us() / 1000,
        100_000.0,
        99_500.0,
        50_000.0,  // 大 mmr
        80_000.0,
        1.5,       // ratio < binance floor (2.0)
        100.0,
        300_000.0,
    );
    MonitorChannel::instance().update_risk_snapshot(BasicAccountScope::BinanceUnified, &bad);
    // 同样 ratio 写到 OKX scope 不应触发（OKX floor=0）
    MonitorChannel::instance().update_risk_snapshot(BasicAccountScope::OkexUnified, &bad);
    let result = MonitorChannel::instance().check_account_risk();
    // 是否报错取决于当前 open/hedge venue 是不是 binance；若不是 binance 此 case 跳过
    let _ = result;
}
```

注意 `set_binance_margin_ratio_floor_async` 等 4 个 setter 需要在 params_load.rs 加（参考 `set_max_leverage_async`）。

- [ ] **Step 5: 编译 + 测试**

```bash
cargo build --release 2>&1 | tail -10
cargo test --lib pre_trade::monitor_channel
```

预期：通过。

- [ ] **Step 6: Commit**

```bash
git add src/pre_trade/monitor_channel.rs src/pre_trade/params_load.rs src/strategy/open_strategy_common.rs
git commit -m "feat(risk): gate open strategy by exchange-reported margin_ratio floor"
```

---

## 部署顺序

由于新增了 `BasicAccountEventType::AccountRisk = 4007` 枚举值（线协议向前兼容——旧消费者会走 `Error` 分支被 drop），但消费者不重启就拿不到新数据。建议同环境一次性重启：

1. 编译：`cargo build --release`
2. 在 trading 时间窗外，依次 PM2 restart 你账号涉及的所有 producer + 消费者：
   ```bash
   pm2 restart binance_account_monitor_<env>   # Binance
   pm2 restart okex_account_monitor_<env>      # OKX
   pm2 restart gate_account_monitor_<env>      # Gate
   pm2 restart bitget_account_monitor_<env>    # Bitget
   pm2 restart pre_trade_<env>                  # 消费者
   ```
3. 等 30s，看日志 `pm_risk_poller started` + 各家 `AccountRisk:` 出现
4. 抓 viz snapshot 验证 `open_account_risk.margin_ratio` 数值合理：
   - Binance：对账 https://www.binance.com/zh-CN/my/dashboard 上的 UniMMR
   - OKX：对账网页"账户保证金率"（mgnRatio %）
   - Gate：对账网页"维持保证金率"（确认 `r` 单位方向）
   - Bitget：对账网页"账户保证金率"（确认 `mgnRatio` 单位）

如果切换 Task 9 的 gate，先用各 exchange `margin_ratio_floor=0`（关）灰度，观察 1-2 天的 `pre_trade_risk` 数据稳定后，再分别调到目标值（建议 Binance/OKX 起步 1.5；Gate/Bitget 视部署后单位验证结果决定）。

## Rate limit 预算（Binance）

`/papi/v1/account` weight=20，PM IP-level 限额 6000/min。
5s 轮询 = 12 次/分钟 × 20 = 240 weight/min（用掉 4%），余量充足。多账号互不干扰。

## 失败模式

- REST 失败：`pm_risk_poller` 这一 tick 跳过，risk_cache 时间戳停在上次成功时刻；下游通过 `STALE_MS` 判断
- account_monitor 进程挂掉：listenKey 也停了，PM2 拉起后 risk poller 自动恢复
- IP 没在 Binance 白名单：bootstrap snapshot 一起失败，错误一致，易诊断
- OKX `account` channel 重连：WS 断线重连后第一条 push 自带最新 risk 字段，无需特殊处理
