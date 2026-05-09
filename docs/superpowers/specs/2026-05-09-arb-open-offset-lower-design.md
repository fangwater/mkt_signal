# Arb 新增 per-symbol `open_offset_lower` 策略参数

**Date:** 2026-05-09
**Status:** Draft

## 背景

目前所有 Arb 模式（FundingArb / IntraArb / CrossArb）的开仓 quote 都用 `vol_band_scale * volatility` 算 offset：内层 offset = `vol_band_scale[0] * volatility`，外层 offset = `vol_band_scale[1] * volatility`，N 个 level 在两者间均分（`src/funding_rate/arb_quote_plan.rs:41-59`）。

这套机制有一个低波动盲点：当某个 symbol 的现货 vol 很低时，`scale * vol` 算出的 offset 会非常贴近市价，开仓单容易被市价单瞬间撞上而失去 quote 优势。运维需要一种 per-symbol 的"硬下限"机制：无论实时 vol 多低，开仓 inner level 必须至少离市价 X bps。

仓内已有成熟的 per-symbol Arb 策略覆盖参数（`amount_u_overrides`、`hedge_price_offset_limit_overrides`），统一通过：
- Rust：`StrategyParams.arb_*_overrides: HashMap<String, f64>`，`ArbDecision::resolve_*` 方法消费
- Redis：`{env}:{open}:{hedge}:*_overrides` 单 STRING + JSON
- Python：`scripts/arb_per_symbol_overrides.py` 共享 helper
- 三个 config server（`intra_/cross_/fr_config_server.py`）各暴露一对 GET/POST endpoint
- 前端：嵌入 config server HTML 模板的 textarea 面板

本次新增 `open_offset_lower` 完全沿用上述模式。

## 目标

- 新增 per-symbol `open_offset_lower: f64`（价格分数，0.001 = 10 bps），用作 Arb 开仓 inner offset 的绝对下限。
- 适用范围：所有三种 Arb 模式（FundingArb / IntraArb / CrossArb）共用同一份 per-symbol map（与 `arb_amount_u_overrides` 一致）。
- 配置面：暴露于 3 个 config server（intra / cross / fr）的 HTML 面板，独立栏，与 `amount_u` 等同级。
- 默认值：`0.0`（不 clamp，保持现有行为）。
- 缺省 / 未配置 / venue 错配：回退到默认 0。

## 非目标

- 不引入 upper 上限（用户明确只要 lower）。
- 不影响 hedge 侧 offset（已有 `arb_hedge_price_offset_limit_*_overrides`，与本次无关）。
- 不影响 MM 模式（MM 有自己的 quote_plan 路径，不走 `build_arb_open_quote_plan`）。
- 不修改 `vol_band_scale` 全局配置；新参数只在 Rust 端做 `max(...)` 修正。

## 约束 / 已确认事实

1. `QuotePlanLevelSpec.offset` 单位是价格分数（`src/market_maker/quote_plan_levels.rs:74,82-83`）：`actual_price = base * (1 ± offset)`。新参数沿用同一单位。
2. `build_arb_open_quote_plan` 在 `arb_decision.rs:2134, 2364, 2583` 三处被调用（FR/Intra/Cross），每处都已经先 `resolve_order_amount_u(open_symbol)`，新参数走相同的 resolve 模式。
3. `arb_per_symbol_overrides.py` 已被三个 config server 共同 import（`intra_config_server.py:40` 类似），新增 helper 自动让三端都可用。
4. 现有 per-symbol map 用 STRING + JSON 存（不是 Redis HASH）；用户口语说的"hashtable"指 Rust 端 `HashMap`，与 Redis 存储无矛盾。

## 设计

### 存储

- Redis key：`{env}:{open_venue}:{hedge_venue}:open_offset_lower_overrides`
- Type：STRING
- Value：JSON 对象，symbol → float（价格分数）。Symbol 经统一规范化（uppercase、去 `-`/`_`、剥掉尾部 `SWAP`），与 `amount_u_overrides` 一致。
- 示例：`{"BTCUSDT":0.0008,"ETHUSDT":0.001}`

### Rust 改动

#### 1. `src/funding_rate/strategy_loader.rs`

- 新增 key 构造函数（紧邻 `arb_amount_u_override_key` 等同侪）：

  ```rust
  fn arb_open_offset_lower_override_key(env_name: &str, open: TradingVenue, hedge: TradingVenue) -> String {
      format!(
          "{}:{}:{}:open_offset_lower_overrides",
          env_name, open.data_pub_slug(), hedge.data_pub_slug()
      )
  }
  ```

- 新增 parser（沿用 `parse_mm_amount_u_overrides` 的 normalize/finite/positive 校验逻辑，但允许 `>= 0.0` 因为 0 是合法的"不 clamp"语义）：

  ```rust
  fn parse_arb_open_offset_lower_overrides(
      raw: &str, venue: TradingVenue,
  ) -> Result<HashMap<String, f64>, String> { ... }
  ```

- `StrategyParams` 增加字段（紧邻 `arb_amount_u_overrides`）：

  ```rust
  /// per-symbol open_offset_lower 覆盖。
  /// 缺省即不 clamp（等价 0.0）。所有三种 Arb 模式共用。
  pub arb_open_offset_lower_overrides: HashMap<String, f64>,
  ```

  默认值 `HashMap::new()`。

- 在 `load_from_redis()` 现有 arb 分支处加载（与 `arb_amount_u_overrides` 同位置）。
- 在 `apply()` 把 `arb_open_offset_lower_overrides` 用 `with_state_mut` clone 进 `ArbDecision`。

#### 2. `src/funding_rate/arb_decision.rs`

- `ArbDecisionState` 增加字段：

  ```rust
  pub arb_open_offset_lower_overrides: HashMap<String, f64>,
  ```

  默认 `HashMap::new()`。

- 新增 resolve 方法（与 `resolve_order_amount_u` 对称放置）：

  ```rust
  /// 取 per-symbol open_offset_lower；缺省 0.0（不 clamp）。
  /// symbol 会按 open venue 规范化匹配 strategy_loader 写入的 key。
  pub fn resolve_open_offset_lower(&self, symbol: &str, open_venue: TradingVenue) -> f64 {
      let normalized = normalize_symbol_for_venue(symbol, open_venue);
      self.arb_open_offset_lower_overrides
          .get(&normalized)
          .copied()
          .unwrap_or(0.0)
  }
  ```

- 三处 call site (`arb_decision.rs:2134, 2364, 2583`) 都补一行：

  ```rust
  let open_offset_lower =
      ArbDecision::with_state_mut(|arb| arb.resolve_open_offset_lower(open_symbol, open_venue))
          .expect("ArbDecisionState should be initialized");
  ```

  并把 `open_offset_lower` 透传到 `build_arb_open_quote_plan`（CrossArb 用 `spot_symbol` 作 symbol，与现有 `resolve_order_amount_u(spot_symbol)` 模式对齐）。

#### 3. `src/funding_rate/arb_quote_plan.rs`

- `build_arb_open_quote_plan` 增加参数 `open_offset_lower: f64`，并在 `build_arb_level_specs` 调用前 clamp `start`：

  ```rust
  // 当前
  let start = vol_band_scale[0] * volatility;
  let end = vol_band_scale[1] * volatility;

  // 改为
  let raw_start = vol_band_scale[0] * volatility;
  let end = vol_band_scale[1] * volatility;
  // 抬升 inner，但不超过 end（防 step<0）
  let start = raw_start.max(open_offset_lower).min(end);
  ```

  把 `start` 计算移出 `build_arb_level_specs`，让 `build_arb_open_quote_plan` 控制；`build_arb_level_specs` 改为接收 `start, end` 而非 `vol_band_scale + volatility`。或者保留现有签名，改在 `build_arb_open_quote_plan` 内对 `volatility` / `vol_band_scale` 做等价变换：把 `vol_band_scale[0]` 替换为 `max(vol_band_scale[0], open_offset_lower / volatility)`。后者在 `volatility == 0` 时退化（除零），所以**采用前者：把 `start, end` 算好后传入**。

  实施细节：新增内部函数 `build_arb_level_specs_from_band(side, inner_price, start, end, level_count)`，由 `build_arb_open_quote_plan` 直接调用。仓内 `grep -rn "build_arb_level_specs"` 仅在 `arb_quote_plan.rs::build_arb_open_quote_plan` 一处调用，**直接删除原 `build_arb_level_specs`，不留薄封装**（YAGNI）。如 plan 实施期发现外部用途，再保留。

- 输入校验：`open_offset_lower` 必须 `>= 0` 且 `is_finite`，否则视为 0（log warn 一行）。

### Python 改动

#### 1. `scripts/arb_per_symbol_overrides.py`

- 新增 key 函数：

  ```python
  def make_open_offset_lower_key(env_name, open_venue, hedge_venue):
      return f"{env_name}:{open_venue}:{hedge_venue}:open_offset_lower_overrides"
  ```

- 新增 normalize / dumps（沿用 amount_u 的实现，校验 `value >= 0`、`math.isfinite(value)`，symbol 规范化大写无 `-_`）：

  ```python
  def normalize_open_offset_lower_mapping(raw_text): ...
  def dumps_open_offset_lower_mapping(mapping): ...
  ```

- 新增 read / write：

  ```python
  def read_open_offset_lower(rds, env_name, open_venue, hedge_venue): ...
  def write_open_offset_lower(rds, env_name, open_venue, hedge_venue, values): ...
  ```

- 在 `render_per_symbol_panels_html()`（`arb_per_symbol_overrides.py:326-397`）增加第 4 个面板，紧邻 `hedge_offset_limits` 之后：

  ```html
  <section class="ps-section">
    <h3>per-symbol open offset lower（仅 Arb；价格分数，0.001=10bps）</h3>
    <p class="hint">未列出的 symbol 走全局 (vol_band_scale[0] * volatility)；列出的 symbol 取 max(vol-计算值, lower)。</p>
    <pre>{"BTCUSDT": 0.0008, "ETHUSDT": 0.0010}</pre>
    <textarea id="open-offset-lower-input" rows="6"></textarea>
    <div class="ps-buttons">
      <button id="open-offset-lower-load">读取</button>
      <button id="open-offset-lower-save">保存</button>
      <button id="open-offset-lower-reset">示例</button>
    </div>
    <div id="open-offset-lower-status"></div>
  </section>
  ```

- 在 `render_per_symbol_panels_js()` 增 `loadOpenOffsetLower / saveOpenOffsetLower / resetOpenOffsetLower` 三个函数 + 相应按钮绑定。

- 示例数据：`{"BTCUSDT": 0.0008, "ETHUSDT": 0.0010}`。

#### 2. `scripts/intra_config_server.py` / `cross_config_server.py` / `fr_config_server.py`

每个 server 增加一对 endpoint，body 委托给共享 helper：

```python
@app.get("/api/open-offset-lower")
def get_open_offset_lower():
    ctx = _get_ctx()
    return ps_overrides.read_open_offset_lower(
        rds, ctx.env_name, ctx.open_venue, ctx.hedge_venue,
    )

@app.post("/api/open-offset-lower")
def set_open_offset_lower():
    ctx = _get_ctx()
    payload = request.get_json(force=True) or {}
    ps_overrides.write_open_offset_lower(
        rds, ctx.env_name, ctx.open_venue, ctx.hedge_venue, payload,
    )
    return {"ok": True}
```

具体函数名 / 装饰器风格按现有 `/api/amount-u` 在每个 server 里的模式微调（intra_config_server.py:2254-2278, 2515-2543 是参考）。

### 数据流

```
Frontend textarea (JSON {symbol: number})
    ↓ POST /api/open-offset-lower
intra_/cross_/fr_config_server.py
    ↓ ps_overrides.write_open_offset_lower
Redis STRING: {env}:{open}:{hedge}:open_offset_lower_overrides
    ↑ 60s 周期 Rust strategy_loader 拉取
StrategyParams.arb_open_offset_lower_overrides (HashMap)
    ↓ apply()
ArbDecisionState.arb_open_offset_lower_overrides
    ↓ resolve_open_offset_lower(symbol, open_venue)
arb_decision.rs:2134/2364/2583 (FR/Intra/Cross)
    ↓
build_arb_open_quote_plan(..., open_offset_lower)
    ↓ start = max(vol_band_scale[0]*vol, lower).min(end)
build_arb_level_specs_from_band → QuotePlanLevelSpec[]
    ↓
build_quote_plan_levels → 实际订单价格
```

### 错误处理 / Edge Cases

| 场景 | 行为 |
| --- | --- |
| Redis key 不存在 | `arb_open_offset_lower_overrides` 为空 map；`resolve_*` 返回 0；行为与 PR 前一致 |
| JSON 解析失败 | strategy_loader 走 `?` 回 anyhow error，配 60s 重试；warn 日志（与现有 `amount_u_overrides` 失败行为一致） |
| symbol 不在 map | 返回 0，不 clamp |
| `open_offset_lower < 0` 或 NaN | parser 拒收（write_open_offset_lower 也会拒收） |
| `open_offset_lower > vol_band_scale[1] * volatility` | `start = end`，N 个 level 退化为同一价。`build_quote_plan_levels` 的 align/dedup 通常会去重为 1 个有效 level。可接受（运维信号：lower 设得太高） |
| `volatility == 0` | `raw_start = 0`、`end = 0`；`start = max(0, lower).min(0) = 0` if lower>0；level specs 此时在 `build_arb_level_specs_from_band` 内会 build 出 N 个 offset=0，`build_quote_plan_levels` 会 dedup。这是当前预期，不在本次扩展 |
| symbol 规范化错配（写入用了原 symbol，Rust 用 normalized） | 与 `amount_u_overrides` 共享 `normalize_symbol_for_venue` / Python `symbol_match` 工具，单测覆盖（见下） |

### 测试策略

#### Rust 单元测试

`arb_quote_plan.rs` 不新增单测：clamp 行为属于一行算式（`raw_start.max(lower).min(end)`），与下游 `build_quote_plan_levels` 的 align/dedup 强耦合，纯 unit 测覆盖性价比低。clamp 行为通过部署期集成 smoke 验证（见下）。

`arb_decision.rs::tests` 增加（验证 resolve 行为，不依赖完整 ArbDecision 初始化，用 helper 构造一个 minimal `ArbDecisionState`）：

| 用例 | 验证 |
| --- | --- |
| `resolve_open_offset_lower_normalizes_symbol` | overrides 写入 `"BTCUSDT" -> 0.001`；查询时传 `"btc-usdt"`、`"btc_usdt"`、`"BTCUSDTSWAP"` 都返回 0.001（依赖 `normalize_symbol_for_venue`） |
| `resolve_open_offset_lower_returns_zero_when_missing` | overrides 为空；查询任何 symbol 均返回 0.0 |

#### Python 单元测试

新增 `tests/test_arb_per_symbol_overrides_open_offset_lower.py`（如已有同侪测试文件就直接扩，否则新建）：

- normalize 接受合法 JSON、拒收负数 / 非 finite / 非数字
- dumps 输出 deterministic key 顺序
- read/write 往一个 fakeredis 跑一个 round-trip

#### 集成验证

1. 启动一个 Intra config server，前端面板显示 4 个独立栏，能独立 GET/POST `open_offset_lower`。
2. Redis 写入 `{env}:{open}:{hedge}:open_offset_lower_overrides = {"BTCUSDT": 0.005}`，启动 trade_signal Intra，确认 60 秒内 strategy_loader 拉到，下次 BTCUSDT 开仓的 inner level offset >= 0.005。
3. 删除 key，再次开仓时 inner offset 回到 `vol_band_scale[0]*volatility`。

## 风险与缓解

| 风险 | 缓解 |
| --- | --- |
| `open_offset_lower` 设过高，实际 levels 全压在同一价 | parser 不阻拦（合法路径），但运维侧文档提示；前端 hint 写明"建议 < vol_band_scale[1]*典型 vol" |
| 三个 config server 改动散开，容易漏一处 | 改 plan 时按 `intra → cross → fr` 顺序逐个跑端到端冒烟；plan 的 self-review 步骤会重读三处 |
| `build_arb_level_specs` 签名变化波及外部调用方 | grep 全仓只在 `arb_quote_plan.rs::build_arb_open_quote_plan` 一处使用，无外部调用方；plan 里手动确认一次 |
| 60s 配置生效延迟可能造成"刚改完但开仓未生效" | 与现有 `amount_u_overrides` 一致；不在本次解决，运维知悉 |

## 兼容性 / 部署

- 加完代码后，Rust strategy_loader 容忍 key 不存在 → 部署 Rust binary 不需要先创建 Redis key。
- Python config server 升级独立部署，不影响 Rust。
- 部署顺序：先 Python（前端可用） → 再 Rust（开始消费）。
- 回滚：撤销 Rust 端 `arb_quote_plan.rs::build_arb_open_quote_plan` 签名 + call site 改动 + state 字段 + resolve 方法 + strategy_loader 解析；删除 Redis key（可选）；前端面板撤回（独立提交，独立回滚）。

## 开放问题

无。设计已锁定。

## 后续工作（不在本 spec 范围）

- 如果运维反馈"lower 过高 → 全压同价"是常见事故模式，可在 Rust 端加 hard reject + warn（拒绝整轮 round），而不是静默退化为单价。
- 如果 IntraArb / CrossArb 想要不同的 lower（区别于 FundingArb），扩 Redis key 命名空间，不在本次。
- 给 `open_offset_lower` 加一个对应的 `upper`，如果将来有"开仓不能挂太远"的需求。本次明确不做。
