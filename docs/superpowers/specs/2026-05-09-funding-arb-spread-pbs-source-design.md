# FundingArb 切换 ask_bid_spread 行情源至 spread_pbs

**Date:** 2026-05-09
**Status:** Draft

## 背景

`trade_signal` 二进制中的 `ArbDecision` 在判定开仓/撤单/平仓时依赖 `SpreadFactor` 持有的实时盘口（askbid / bidask / spread_rate）。`SpreadFactor` 由 `MktChannel` 驱动更新，`MktChannel` 当前订阅的是 `bridge/<slug>/ask_bid_spread` 服务，由 `bridge` 进程从 `dat_pbs` 转发而来。

仓内已存在独立的高速发布器 `spread_pbs`（`src/bin/spread_pbs.rs` + `src/spread_pbs/`），按 venue 部署，发布到 `spread_pbs/<venue_slug>/ask_bid_spread`，payload 为 128B `AskBidSpreadMsg`，与 bridge 路径完全 wire-compatible。

`spread_pbs` 路径相比 `dat_pbs → bridge` 少一跳转发，对延迟敏感的 FundingArb 模式（跨所 margin × futures 套利）有意义。其他模式（IntraArb、CrossArb、Mm）暂不切换。

## 目标

- `trade_signal` 当 `ArbMode == FundingArb` 时，open + hedge 两条腿的 `ask_bid_spread` 通道改从 `spread_pbs/<slug>/ask_bid_spread` 订阅。
- 其他模式（IntraArb / CrossArb / Mm）以及 readonly 路径（`fr_signal_dashboard`、`demo_rate_fetcher`）继续走 `bridge/<slug>/ask_bid_spread`。
- `derivatives`（funding rate / mark / index）通道不变，仍从 `bridge/<slug>/derivatives` 订阅。

## 非目标

- 不在 `spread_pbs` 增加 `derivatives` 发布通道。
- 不替换 readonly dashboard 的行情源（保持 bridge）。
- 不引入运行时 CLI/YAML 路由开关；路由由代码按 `ArbMode` 硬编码。
- 不加启动期 watchdog 强制 fail-fast；沿用现有 `RateFetcher::not_ready_detail` + 10 秒 degraded 日志机制。

## 约束与已确认事实

1. wire 格式两侧一致：bridge 与 spread_pbs 都发 128B `AskBidSpreadMsg`（`src/common/mkt_msg.rs`）。MktChannel 订阅器无需改 payload 类型。
2. slug 一致：`spread_pbs::publisher::SpreadPublisher::new` 用 `TradingVenue::data_pub_slug()`（`src/spread_pbs/app.rs:34`），MktChannel 也用 `data_pub_slug()`（`src/funding_rate/mkt_channel.rs:132`），不会出现命名错位。
3. mode 在 `MktChannel::init_singleton` 之前就已确定：`trade_signal.rs` 启动逻辑里 mode 与 `DecisionBranch` 一起设置（trade_signal.rs:432-543）。

## 设计

### 变更范围

只动两个文件：

- `src/funding_rate/mkt_channel.rs` — askbid 的 service root 从 const 改成按 mode 选择的纯函数。
- `src/bin/trade_signal.rs` — 把已有的 `arb_mode: Option<ArbMode>` 透传给 `MktChannel::init_singleton`。

readonly 路径调用点（`fr_signal_dashboard.rs`、`demo_rate_fetcher.rs`）签名不变，内部传 `None` 给私有 `init_singleton_with_mode`，保持现有 bridge 行为。

### 接口变更

```rust
// before
pub fn init_singleton(open_venue: TradingVenue, hedge_venue: TradingVenue) -> Result<()>
pub fn init_singleton_readonly(open_venue: TradingVenue, hedge_venue: TradingVenue) -> Result<()>
fn init_singleton_with_mode(open_venue, hedge_venue, trigger_decisions: bool) -> Result<()>

// after
pub fn init_singleton(
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    arb_mode: Option<ArbMode>,
) -> Result<()>
pub fn init_singleton_readonly(open_venue: TradingVenue, hedge_venue: TradingVenue) -> Result<()>  // 不变
fn init_singleton_with_mode(
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    trigger_decisions: bool,
    arb_mode: Option<ArbMode>,
) -> Result<()>
```

### 路由逻辑

`mkt_channel.rs` 处理：

- 删除 `const MARKET_SERVICE_ROOT: &str = "bridge";`
- `build_market_service(slug, channel)` 函数保留，但内部硬编码 `"bridge"` 前缀（仅服务于 derivatives 场景）：
  ```rust
  fn build_market_service(slug: &str, channel: &str) -> String {
      format!("bridge/{}/{}", slug, channel)
  }
  ```
- 新增专用于 askbid 的纯函数：
  ```rust
  fn askbid_service_root(arb_mode: Option<ArbMode>) -> &'static str {
      match arb_mode {
          Some(ArbMode::FundingArb) => "spread_pbs",
          _ => "bridge",
      }
  }
  ```

`init_singleton_with_mode` 内部组装服务名：

```rust
// askbid（按 mode 路由）
let askbid_root = askbid_service_root(arb_mode);
let open_askbid_service  = format!("{}/{}/ask_bid_spread", askbid_root, open_slug);
let hedge_askbid_service = format!("{}/{}/ask_bid_spread", askbid_root, hedge_slug);

// derivatives（始终 bridge；调用既有 helper）
// 现存代码已在条件块里只对 is_futures(...) 的腿订阅 derivatives，本次不动这块条件。
let open_derivatives_service  = build_market_service(open_slug, "derivatives");   // is_futures(open) 时
let hedge_derivatives_service = build_market_service(hedge_slug, "derivatives");  // is_futures(hedge) 时
```

`spawn_askbid_listener` / `spawn_derivatives_listener` 的签名不变，只是被传入的 `service_name` 字符串改了来源。

### 调用点更新

| 文件 | 行 | 变更 |
| --- | --- | --- |
| `src/bin/trade_signal.rs` | 327 | `MktChannel::init_singleton(open_venue, hedge_venue, arb_mode)` —— 复用已存在的 `arb_mode: Option<ArbMode>` 局部变量 |
| `src/fr_signal_dashboard.rs` | 166 | 不动（仍调 readonly） |
| `src/bin/demo_rate_fetcher.rs` | 40 | 增加显式 `None`：`MktChannel::init_singleton(open_venue, hedge_venue, None)` |

### 数据流（FundingArb 场景）

```
spread_pbs[venue_a]  → spread_pbs/<slug_a>/ask_bid_spread (128B AskBidSpreadMsg)
                                                     ↓
                                       MktChannel.spawn_askbid_listener (open)
                                                     ↓
spread_pbs[venue_b]  → spread_pbs/<slug_b>/ask_bid_spread
                                                     ↓
                                       MktChannel.spawn_askbid_listener (hedge)
                                                     ↓
                                          SpreadFactor.update
                                                     ↓
                                          ArbDecision (drive_spread_arb_decision)

dat_pbs → bridge → bridge/<slug>/derivatives
                                  ↓
                  MktChannel.spawn_derivatives_listener
                                  ↓
              funding_rates / mark_prices / index_prices
```

其他模式（IntraArb / CrossArb / Mm）下，open/hedge askbid 与 derivatives 全部继续走 `bridge/...`。

### 错误处理

- 订阅器创建失败：原代码已 `?` 上抛，trade_signal 启动失败。无变化。
- spread_pbs publisher 未启动 / 长期无消息：iceoryx2 subscriber `create()` 不会失败，listener 正常空轮询。`ArbDecision::trigger_decision` 在 FundingArb 模式下会调用 `RateFetcher::is_initial_ready`；当 BBO 缺失导致 `funding_open_inputs_ready` 长期 false，`decision_router.rs:54-87` 已有的 10 秒一次 `DecisionRouter: degraded mode` 日志会暴露问题。
- 不在本次实现里加启动期 watchdog；如未来 SLO 收紧，可再加。

### 测试策略

**单元测试**（新增到 `mkt_channel.rs` 内联 `#[cfg(test)] mod tests`）：

```rust
#[test]
fn askbid_root_funding_arb_uses_spread_pbs() {
    assert_eq!(askbid_service_root(Some(ArbMode::FundingArb)), "spread_pbs");
}

#[test]
fn askbid_root_other_modes_use_bridge() {
    assert_eq!(askbid_service_root(Some(ArbMode::IntraArb)), "bridge");
    assert_eq!(askbid_service_root(Some(ArbMode::CrossArb)), "bridge");
    assert_eq!(askbid_service_root(None), "bridge");
}
```

**集成验证**（手工，在实现 PR 上跑）：

1. `cargo build --release` 通过。
2. 启动一个 binance-margin × bybit-futures 的 FundingArb trade_signal，确认日志出现：
   - `订阅盘口: spread_pbs/binance-spot/ask_bid_spread`（或对应 margin slug）
   - `订阅盘口: spread_pbs/bybit-futures/ask_bid_spread`
   - `订阅衍生品数据: bridge/<slug>/derivatives`
3. 启动一个 IntraArb（如 okex-margin × okex-futures）trade_signal，确认日志仍是 `bridge/...`。
4. `fr_signal_dashboard` 启动，确认日志仍是 `bridge/...`。

**回归保护**：FundingArb 跑通后，肉眼对账 SpreadFactor 输出（已有的 `record_spread_observation_fwd/bwd` 调试日志）的频率与之前一致，无掉数据。

## 风险与缓解

| 风险 | 缓解 |
| --- | --- |
| spread_pbs 部署不齐全，FundingArb 启动后无 BBO | 现有 degraded 日志（10s 一次 warn）；部署 checklist 要求 spread_pbs 与 trade_signal FundingArb 同时上线 |
| dashboard 与 live trader 看到的盘口源不同，可能有微秒级延迟差 | 接受现状；如需统一，后续把 dashboard 也接到 spread_pbs（独立小改动） |
| 后续若 IntraArb / CrossArb 也想切 spread_pbs | `askbid_service_root` 是 `match` 分支，扩一行即可 |

## 兼容性 / 部署

- 部署顺序：先确保所有 FundingArb 涉及的 venue 都跑了 `spread_pbs` 进程（PM2 进程清单已支持），再发布带本次改动的 `trade_signal`。
- 回滚：撤销 `trade_signal.rs` 调用点的参数改动并把 `askbid_service_root` 改回返回 `"bridge"` 即可，无 schema / 持久化变更。

## 开放问题

无。设计已充分锁定。

## 后续工作（不在本 spec 范围）

- 若需要严格的启动期 fail-fast，可在 listener spawn 后加 watchdog 任务（N 秒未收到任何 sample 即 panic）。
- 若 IntraArb / CrossArb 也希望切换源，扩 `askbid_service_root` 一行；可能还需要为同所 IntraArb 的 spot/futures 都跑 spread_pbs。
- 让 `fr_signal_dashboard` 也跟随路由（给 readonly init 加 `Option<ArbMode>` 入参）。
