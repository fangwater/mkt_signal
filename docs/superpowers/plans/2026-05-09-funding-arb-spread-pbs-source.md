# FundingArb 切换 ask_bid_spread 行情源至 spread_pbs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 当 `ArbMode == FundingArb` 时，`MktChannel` 的 open + hedge 两条 askbid 订阅改为 `spread_pbs/<slug>/ask_bid_spread`；其他模式与 readonly 路径继续走 `bridge/...`；derivatives 不变。

**Architecture:** 在 `mkt_channel.rs` 引入私有纯函数 `askbid_service_root(Option<ArbMode>) -> &'static str`；扩展 `MktChannel::init_singleton` 签名增加 `Option<ArbMode>` 入参，由 `trade_signal.rs` 启动逻辑透传；`init_singleton_readonly` 与 `demo_rate_fetcher` 显式传 `None`。

**Tech Stack:** Rust 1.x、iceoryx2、tokio、anyhow、`AskBidSpreadMsg` (128B fixed-size payload)。

**Spec:** `docs/superpowers/specs/2026-05-09-funding-arb-spread-pbs-source-design.md`

---

## File Structure

| File | Role | Action |
| --- | --- | --- |
| `src/funding_rate/mkt_channel.rs` | askbid/derivatives 订阅入口 + service-name 组装 | Modify：删 `MARKET_SERVICE_ROOT` 常量；新增 `askbid_service_root`；改 `build_market_service` 硬编码 bridge；扩 `init_singleton{,_with_mode}` 签名；新增内联测试 |
| `src/bin/trade_signal.rs` | trade_signal 主流程 | Modify line 327：把已有的 `arb_mode` 透传给 `MktChannel::init_singleton` |
| `src/bin/demo_rate_fetcher.rs` | 本地调试 binary | Modify line 40：补 `None` 参数 |
| `src/fr_signal_dashboard.rs` | readonly dashboard | 不动（继续走 readonly 路径） |

---

## Task 1: 在 `mkt_channel.rs` 加 `askbid_service_root` 纯函数 + 单测（TDD）

**Files:**
- Modify: `src/funding_rate/mkt_channel.rs`（新增私有 fn + 内联 `mod tests`）

**Goal:** 用 TDD 锁定路由规则：`FundingArb -> spread_pbs`，其他模式与 `None` -> `bridge`。

- [ ] **Step 1: 在 `mkt_channel.rs` 末尾添加内联测试模块（先写，跑必败）**

在文件末尾追加：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::funding_rate::ArbMode;

    #[test]
    fn askbid_root_funding_arb_uses_spread_pbs() {
        assert_eq!(askbid_service_root(Some(ArbMode::FundingArb)), "spread_pbs");
    }

    #[test]
    fn askbid_root_intra_arb_uses_bridge() {
        assert_eq!(askbid_service_root(Some(ArbMode::IntraArb)), "bridge");
    }

    #[test]
    fn askbid_root_cross_arb_uses_bridge() {
        assert_eq!(askbid_service_root(Some(ArbMode::CrossArb)), "bridge");
    }

    #[test]
    fn askbid_root_none_uses_bridge() {
        assert_eq!(askbid_service_root(None), "bridge");
    }
}
```

- [ ] **Step 2: 跑测试，确认编译失败（`askbid_service_root` 还不存在）**

Run:
```
cargo test --lib funding_rate::mkt_channel::tests -- --nocapture
```

Expected: 编译失败，错误形如 `cannot find function 'askbid_service_root' in this scope`。

- [ ] **Step 3: 在 `mkt_channel.rs` 顶部 use 区域增加 `ArbMode` 引入**

在现有的 `use` 块（`mkt_channel.rs:7-23` 区域）末尾追加：

```rust
use super::ArbMode;
```

- [ ] **Step 4: 在 `build_market_service` 函数（约第 58 行）下方插入 `askbid_service_root`**

```rust
fn askbid_service_root(arb_mode: Option<ArbMode>) -> &'static str {
    match arb_mode {
        Some(ArbMode::FundingArb) => "spread_pbs",
        _ => "bridge",
    }
}
```

- [ ] **Step 5: 跑测试，确认 4 个 test 全部通过**

Run:
```
cargo test --lib funding_rate::mkt_channel::tests -- --nocapture
```

Expected:
```
test funding_rate::mkt_channel::tests::askbid_root_funding_arb_uses_spread_pbs ... ok
test funding_rate::mkt_channel::tests::askbid_root_intra_arb_uses_bridge ... ok
test funding_rate::mkt_channel::tests::askbid_root_cross_arb_uses_bridge ... ok
test funding_rate::mkt_channel::tests::askbid_root_none_uses_bridge ... ok
```

- [ ] **Step 6: 提交**

```bash
git add src/funding_rate/mkt_channel.rs
git commit -m "feat(mkt_channel): add askbid_service_root for FundingArb routing

引入按 ArbMode 选择 askbid 订阅 service root 的纯函数：
FundingArb -> spread_pbs；其他模式与 None -> bridge。
内联单测覆盖 4 个分支。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: 改 `MARKET_SERVICE_ROOT` 与 `build_market_service`

**Files:**
- Modify: `src/funding_rate/mkt_channel.rs:31` (删常量) + `:58-60` (改函数体) + `:170-173` (改 info! 日志，先占位，Task 3 会重写)

**Goal:** 让 `build_market_service` 只服务 derivatives 通道（硬编码 `"bridge"`），删掉与 askbid 共享的常量；为 Task 3 的 askbid 路由解耦做准备。

- [ ] **Step 1: 删除常量定义**

定位 `mkt_channel.rs:31`（精确文本：`const MARKET_SERVICE_ROOT: &str = "bridge";`），整行删除。

- [ ] **Step 2: 改写 `build_market_service`**

定位 `mkt_channel.rs:58-60`，将：

```rust
fn build_market_service(slug: &str, channel: &str) -> String {
    format!("{}/{}/{}", MARKET_SERVICE_ROOT, slug, channel)
}
```

替换为：

```rust
fn build_market_service(slug: &str, channel: &str) -> String {
    // bridge 用于 derivatives（funding/mark/index）；askbid 走 askbid_service_root
    format!("bridge/{}/{}", slug, channel)
}
```

- [ ] **Step 3: 修复 `MARKET_SERVICE_ROOT` 引用造成的编译失败**

`mkt_channel.rs:170-173` 的 `info!("MktChannel 初始化完成: service_root={}", MARKET_SERVICE_ROOT);` 现在引用了已删除的常量。临时改为：

```rust
info!("MktChannel 初始化完成");
```

（Task 3 会把这条日志重写为带 `askbid_root` 的版本，本步只需让代码编译通过。）

- [ ] **Step 4: 跑全量编译 + 单测**

Run:
```
cargo build --lib
cargo test --lib funding_rate::mkt_channel::tests
```

Expected: 编译通过；4 个 askbid_root 单测仍 PASS。

- [ ] **Step 5: 提交**

```bash
git add src/funding_rate/mkt_channel.rs
git commit -m "refactor(mkt_channel): scope build_market_service to bridge

删除 MARKET_SERVICE_ROOT 常量，build_market_service 改为
硬编码 'bridge' 前缀（仅服务 derivatives 通道）。
askbid 路由将由 askbid_service_root 单独决定。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: 扩展 `init_singleton{,_with_mode}` 签名 + 路由 askbid

**Files:**
- Modify: `src/funding_rate/mkt_channel.rs:113-225` 区域

**Goal:** 把 `Option<ArbMode>` 一路传到 askbid 的 service-name 组装处；derivatives 通道不动；readonly 入口仍传 `None`。

- [ ] **Step 1: 修改 `init_singleton` 签名**

定位 `mkt_channel.rs:113-115`，将：

```rust
pub fn init_singleton(open_venue: TradingVenue, hedge_venue: TradingVenue) -> Result<()> {
    Self::init_singleton_with_mode(open_venue, hedge_venue, true)
}
```

替换为：

```rust
pub fn init_singleton(
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    arb_mode: Option<ArbMode>,
) -> Result<()> {
    Self::init_singleton_with_mode(open_venue, hedge_venue, true, arb_mode)
}
```

- [ ] **Step 2: 修改 `init_singleton_readonly` 内部委托**

定位 `mkt_channel.rs:120-125`，将函数体内的 `Self::init_singleton_with_mode(open_venue, hedge_venue, false)` 改为：

```rust
Self::init_singleton_with_mode(open_venue, hedge_venue, false, None)
```

公共签名不变。

- [ ] **Step 3: 修改 `init_singleton_with_mode` 签名**

定位 `mkt_channel.rs:127-131`，将签名：

```rust
fn init_singleton_with_mode(
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    trigger_decisions: bool,
) -> Result<()> {
```

替换为：

```rust
fn init_singleton_with_mode(
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    trigger_decisions: bool,
    arb_mode: Option<ArbMode>,
) -> Result<()> {
```

- [ ] **Step 4: 在函数体内根据 `arb_mode` 选 askbid root**

定位 `mkt_channel.rs:135-136`（紧接 `let hedge_slug = ...;` 之后），将：

```rust
        let open_service = build_market_service(open_slug, "ask_bid_spread");
        let hedge_service = build_market_service(hedge_slug, "ask_bid_spread");
```

替换为：

```rust
        let askbid_root = askbid_service_root(arb_mode);
        let open_service = format!("{}/{}/ask_bid_spread", askbid_root, open_slug);
        let hedge_service = format!("{}/{}/ask_bid_spread", askbid_root, hedge_slug);
```

derivatives 路径继续用 `build_market_service(..., "derivatives")`，不动。

- [ ] **Step 5: 重写 `MktChannel 初始化完成` 日志，把两个 root 都打出来**

定位 Task 2 留下的临时日志 `info!("MktChannel 初始化完成");`，替换为：

```rust
        info!(
            "MktChannel 初始化完成: askbid_root={} derivatives_root=bridge arb_mode={:?}",
            askbid_root, arb_mode
        );
```

- [ ] **Step 6: 跑 lib 编译，预期 trade_signal 与 demo_rate_fetcher 还过不了（call site 未更新）**

Run:
```
cargo build --lib
```

Expected: lib crate 编译通过；单测仍通过。

Run:
```
cargo build --bins 2>&1 | grep -E "error\[|init_singleton"
```

Expected: 看到 `trade_signal.rs:327` 与 `demo_rate_fetcher.rs:40` 的 arity mismatch 报错（缺第 3 个参数）。这是预期，下两 task 修复。

- [ ] **Step 7: 提交**

```bash
git add src/funding_rate/mkt_channel.rs
git commit -m "feat(mkt_channel): plumb Option<ArbMode> through init_singleton

按 ArbMode 选择 askbid 订阅 service root；
init_singleton 公共签名增加 Option<ArbMode> 入参；
init_singleton_readonly 内部固定传 None；
derivatives 通道继续走 bridge。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: 更新 `trade_signal.rs` 调用点

**Files:**
- Modify: `src/bin/trade_signal.rs:327`

**Goal:** 把 run() 形参 `arb_mode: Option<ArbMode>` 透传给 `MktChannel::init_singleton`。

- [ ] **Step 1: 修改调用**

定位 `trade_signal.rs:327`，将：

```rust
    MktChannel::init_singleton(open_venue, hedge_venue)?;
```

替换为：

```rust
    MktChannel::init_singleton(open_venue, hedge_venue, arb_mode)?;
```

`arb_mode` 是同一函数 (`run`) 的形参（`trade_signal.rs:288`），无需 import 或额外处理。

- [ ] **Step 2: 跑 trade_signal 编译**

Run:
```
cargo build --bin trade_signal
```

Expected: 通过。

- [ ] **Step 3: 提交**

```bash
git add src/bin/trade_signal.rs
git commit -m "feat(trade_signal): pass arb_mode to MktChannel::init_singleton

让 FundingArb 触发 spread_pbs askbid 路径。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: 更新 `demo_rate_fetcher.rs` 调用点

**Files:**
- Modify: `src/bin/demo_rate_fetcher.rs:40`

**Goal:** 给 demo binary 显式传 `None`，保持现有 bridge 行为。

- [ ] **Step 1: 修改调用**

定位 `demo_rate_fetcher.rs:40`，将：

```rust
    MktChannel::init_singleton(open_venue, hedge_venue)?;
```

替换为：

```rust
    MktChannel::init_singleton(open_venue, hedge_venue, None)?;
```

- [ ] **Step 2: 跑 demo binary 编译**

Run:
```
cargo build --bin demo_rate_fetcher
```

Expected: 通过。

- [ ] **Step 3: 提交**

```bash
git add src/bin/demo_rate_fetcher.rs
git commit -m "chore(demo_rate_fetcher): pass None for arb_mode (keep bridge source)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: 全量构建 + lint + 完整测试

**Files:** N/A（仅运行检查）

**Goal:** 防回归。CLAUDE.md 要求 push 前必过 fmt + clippy。

- [ ] **Step 1: 全量 release build**

Run:
```
cargo build --release
```

Expected: 通过，无 warning。

- [ ] **Step 2: 跑全量测试**

Run:
```
cargo test
```

Expected: 全部 PASS。如果出现先前不相关的失败，先记录但不修，反馈给上层。

- [ ] **Step 3: cargo fmt**

Run:
```
cargo fmt --all -- --check
```

Expected: 无输出（已格式化）。如有 diff，跑 `cargo fmt --all` 后 amend 进上一个 commit 或单独提交格式 commit。

- [ ] **Step 4: cargo clippy**

Run:
```
cargo clippy -- -D warnings
```

Expected: 通过。如果出现 `unused import: super::ArbMode` 之类（说明 Task 1 的 `use super::ArbMode;` 在 release 构建下不需要——实际上签名引用了，应该是被使用的），先看具体警告再决定是否调整 use 语句位置（move 到 `#[cfg(test)]` 内）。

- [ ] **Step 5（如需）：清理 unused import**

如果 clippy 报 `super::ArbMode` 在非 test 路径下未使用（Task 3 后 init_singleton 签名已用 `ArbMode`，应该不会），按提示调整。如果只在 test 中使用，把 use 移到测试模块内：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::funding_rate::ArbMode;
    // ...
}
```

并删除文件顶部的 `use super::ArbMode;`。再跑 `cargo clippy -- -D warnings` 确认。

- [ ] **Step 6（如有清理）：提交**

```bash
git add src/funding_rate/mkt_channel.rs
git commit -m "chore(mkt_channel): satisfy clippy after refactor

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

如步骤 3-5 全部一次过，本 task 无需提交。

---

## Task 7: 手工集成验证（人在场）

**Files:** N/A（运行验证）

**Goal:** 在真实环境跑一次 FundingArb 与一次非 FundingArb，肉眼核对日志中的 service 名称符合预期。

> **注意：** 该 task 需要 `spread_pbs` 已在目标 venue 部署运行。若环境不具备，请把验证步骤记入 PR 描述并跳过实际启动，由值班同事在部署窗口执行。

- [ ] **Step 1: 启动 FundingArb 实例（举例：binance-margin × bybit-futures）**

启动方式按本地脚本，例如：
```
RUST_LOG=info ./target/release/trade_signal --venue ... <FundingArb 配置>
```

抓启动日志关键三行：

```
MktChannel 初始化完成: askbid_root=spread_pbs derivatives_root=bridge arb_mode=Some(FundingArb)
订阅盘口: spread_pbs/<open_slug>/ask_bid_spread (BinanceMargin)
订阅盘口: spread_pbs/<hedge_slug>/ask_bid_spread (BybitFutures)
```

并确认 derivatives 仍打：
```
订阅衍生品数据: bridge/<futures_slug>/derivatives
```

- [ ] **Step 2: 验证 SpreadFactor 实际收到行情**

跑 ≥ 2 分钟，确认 `record_spread_observation_fwd/bwd` 类日志或 `spread_factor.debug_print_stored_spreads` 输出非空。

- [ ] **Step 3: 启动一个 IntraArb 或 CrossArb 实例**

抓日志确认仍是：

```
MktChannel 初始化完成: askbid_root=bridge derivatives_root=bridge arb_mode=Some(IntraArb)
订阅盘口: bridge/<slug>/ask_bid_spread (...)
```

- [ ] **Step 4: 启动 fr_signal_dashboard 验证 readonly 路径未受影响**

```
订阅盘口: bridge/<slug>/ask_bid_spread (...)
```

应仍是 bridge。

- [ ] **Step 5: 把日志摘录贴到 PR 描述里**

无需 commit。

---

## 完成清单（self-check）

实施者完成上述 7 个 task 后请确认：

- [ ] `mkt_channel.rs` 不再含 `MARKET_SERVICE_ROOT` 常量
- [ ] `askbid_service_root` 4 个分支单测全部通过
- [ ] `cargo build --release` / `cargo test` / `cargo fmt --check` / `cargo clippy -- -D warnings` 全过
- [ ] FundingArb 启动日志 `askbid_root=spread_pbs`
- [ ] IntraArb / CrossArb / fr_signal_dashboard 启动日志 `askbid_root=bridge`
- [ ] `derivatives` 通道服务名前缀全程为 `bridge/`
- [ ] PR 描述包含 spec 链接与上面的日志样本
