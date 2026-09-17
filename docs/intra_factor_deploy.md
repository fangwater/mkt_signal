# Intra Factor Deploy

最后更新：2026-09-17

## 结论

本文定义 Binance 同所期现 CTA 规则盘的回测一致性契约、当前实现状态和生产上线
门槛。规则来源是：

```text
/home/u171/research/crypto_research/CTA_research/version005_long_short_rust_two_exchange/
select_params/analysis_params_binancesymbol6_icir_vs_baseline489_train2024_2026_two.ipynb
```

信号、过滤和撮合语义以同目录 `src/signal.rs`、`src/filters.rs`、`src/engine.rs`
及 `factor_backtest_baseline489_train2024_2026.toml` 为准。

本次审核后的结论：

- 仓库代码已补齐 raw 因子线性分位阈值、右端 bar 时间、1 秒 delay、NQ 过滤、
  逐 lot maker TP、止损、移动止损、最长持仓和 TP 撤单竞态处理；重启恢复已明确
  排除并改由人工对齐；同 symbol opening maker 已采用单方向 pending 契约，research
  尚需按相同契约重跑验收。不得把代码完成度等同于可上线。
- 九个因子必须是九套独立 CTA 执行环境。一个环境最多加载一条规则；publisher
  可以共享，交易账户/子组合、IPC、订单账本、Redis 执行参数和持久化不能共享。
- 仓库当前只登记了 `binance-cta-rx01` 的远程编排。其余八套环境尚未分配
  RapidX sub-portfolio、端口、CPU 和环境名，不能声称九盘已经生产部署。
- 本次没有执行任何发布、启动、发单或远端配置写入。生产状态必须重新现场检查，
  不能沿用本文中的历史快照推断。
- 信号可以逐 bar 严格对齐；真实交易所的排队、延迟、部分成交、tick/qty 取整和
  实际费率无法与回测撮合器逐成交完全相同，必须作为执行偏差单独验收。

notebook 第 9 节把九路 ±1 信号做 `nanmean` 的组合是单独的诊断实验，且明确不是
Rust 逐笔 maker 链。本部署对应第 8 节筛出的九条逐笔规则，不实现第 9 节组合仓位。

## 回测契约

### 时间与信号

每个因子、每个 symbol 独立计算，不做九因子投票或 `nanmean`：

```text
bar frequency        = 60s
row timestamp t      = 已关闭区间 [t-60s, t) 的右端标签
rolling window       = 2880 个固定频率槽位
minimum samples      = 1440 个有限 raw 值
long threshold       = linear_quantile(window including current, 0.9)
short threshold      = linear_quantile(window including current, 0.1)
long                 = raw > long_threshold
short                = raw < short_threshold
application          = each_bar
signal delay         = 1s，即最早在 t+1s 决策
cooldown             = 0
conflict policy      = flat
```

分位必须使用 `f64` 线性插值。不能用 percentile rank 与 `0.9/0.1` 比较；重复值和
离散因子下两者不等价。非有限值占用时间窗口槽位但不进入有序样本，当前 bar 的
raw 值和 NQ 值都计入各自当期阈值。

过旧、乱序、warming-up、非有限或 factor/NQ 时间戳未对齐的消息不得开仓。
live 额外使用 `max_signal_age_seconds=120` 作为陈旧消息保护；它不改变正常 bar 的
回测决策。

### NQ 过滤

九条入选规则均启用 `nq_change`。它使用 Binance spot BBO，不使用 futures：

```text
close(t)             = 该 1min bar 内最后一个有效 (bid+ask)/2，不跨 bar ffill
lookback             = 60 bars，包含当前 close
BUY_NQ_CHANGE        = (close - rolling_min(close, 60)) / rolling_min
SELL_NQ_CHANGE       = (close - rolling_max(close, 60)) / rolling_max
quantile window      = 1440
minimum periods      = 720
threshold            = linear quantile 0.95，包含当前值
long allow           = BUY_NQ_CHANGE >= long_threshold
short allow          = SELL_NQ_CHANGE <= short_threshold
```

因子与 NQ 只能在相同 `(symbol, right_edge_ts)` 上合并发布。任一侧缺失时不允许用
上一根 NQ 或另一时间戳的阈值代替。publisher 对未配对项最多保留 6 小时，过期后
丢弃并报警，避免单边 topic 故障造成无界内存增长；这远长于 120 秒交易新鲜度窗口，
不会把旧消息重新变成可交易信号。

### Spread overlay

`spread=(spot_mid-swap_mid)/spot_mid` 只控制现货开仓挂单和撤单，不产生方向：

```text
long open            spread < q30
short open           spread > q70
long cancel          spread crosses q50
short cancel         spread crosses q50
```

Redis mapping 为：

```text
forward_open_mm=spread_30    forward_open_mt=bidask_30
forward_cancel_mm=spread_50  forward_cancel_mt=bidask_50
backward_open_mm=spread_70   backward_open_mt=askbid_70
backward_cancel_mm=spread_50 backward_cancel_mt=askbid_50
```

`rolling_metrics` 必须实际发布所引用的 q30/q50/q70；同步响应有 warning 时不能上线。

### 执行与退出

每次有效信号在 spot 腿同时挂四档 maker：

```text
offsets              [0.0, 0.0001, 0.0003, 0.0005]
notional per level   100 USDT
maker TTL            120s
max position         10000 USDT
```

spot 成交后，每个 `open_id` 独立持有 entry、数量、成交时间和退出参数：

- 同一 symbol 的 CTA opening maker 同时只允许一个方向。反向信号先撤旧方向全部
  opening maker，并丢弃本轮信号；必须等待撤单终态。若撤单前已有成交，反向开仓继续
  被该真实 lot 阻断，直到它按既有退出流程结束。
- swap maker TP：long 为 `entry*(1+tp)`，short 为 `entry*(1-tp)`，不过期。
- 初始 stop 距离：`take_profit/reward_risk_ratio`。
- 移动止损：`level=floor(max(progress,0)/trigger_step)`；stop 每级移动
  `move_step`，只能向盈利方向收紧。
- stop 使用 swap BBO：long 检查 bid，short 检查 ask；不使用 spot BBO。
- `max_holding_seconds=14400` 从该 spot lot 首次实际成交时间开始。
- stop/trailing/max-holding 先锁存退出原因。若该 lot 的 maker TP 已 reserved，先撤
  TP；终态按累计成交扣减，释放余量后再对相同 `open_id` 发 swap taker。
- CTA 的盈利退出只由 maker TP 负责，不再额外启用通用 intra taker take-profit。
- spot partial fill 立即进入原 `open_id` 的 lot。若单个 lot 按 futures `step_size` 量化后
  不满足 `min_qty` 或 `min_notional`，不发送必然被拒的 TP；等待后续同方向 lot 后按
  FIFO 合并到最小可执行数量。合并单保留每个原始 lot 的 allocation，卖单取各 lot 中
  最高 TP 价、买单取最低 TP 价，保证没有成分以劣于自身 TP 的价格退出；不足一个 step
  的尾量继续留在原 lot 等下一次合并。

回测的 maker 新单下一秒生效且要求严格穿价。live 只能保证订单方向、锚点、TTL、
逐 lot 身份和退出顺序一致；是否成交由交易所订单簿和队列决定。

### 九条入选规则

公共参数均为 `both / each_bar / q90-q10 / delay=1s / cooldown=0 / NQ=on`，以及上面的
四档、100U、TTL 120、max position 10000、max holding 14400。差异如下：

| rule / factor | replay parameter_id | TP | RR | trailing trigger | trailing move |
| --- | --- | ---: | ---: | ---: | ---: |
| `baseline_035` | `1d8936bf9b635502` | 0.005 | 1 | 0.001 | 0.0005 |
| `TD_PR_011` | `021fedbf5203acfa` | 0.005 | 1 | 0.001 | 0.0005 |
| `baseline_053` | `5c66278eed10fff3` | 0.010 | 1 | 0.002 | 0.0010 |
| `TP_VPI_006` | `058616d273700edc` | 0.010 | 2 | 0.002 | 0.0010 |
| `TD_PR_005` | `152fc40425ac03e5` | 0.010 | 1 | 0.001 | 0.0005 |
| `factor_116` | `4ca6e8bdc0265276` | 0.010 | 1 | 0.002 | 0.0010 |
| `net_buy_medium` | `bfc0bc6be19ef7ae` | 0.010 | 1 | 0.002 | 0.0005 |
| `factor_004` | `46daf86b057e4da3` | 0.010 | 1 | 0.001 | 0.0005 |
| `baseline_091` | `dd956fe644e25bf4` | 0.010 | 1 | 0.001 | 0.0005 |

这些 ID 来自 `parameter_groups_replay_live46.csv` 的原始 replay 参数。费率复跑会重铸
新的 `parameter_id`，因此审计时还要通过 `parameter.replay.parent_id` 关联，不能把两种
ID 混为同一命名空间。Rust loader 对上述九个 model service 的公共参数和逐因子退出
参数做强校验，防止某个环境因缺失 strategy hash 而静默回落到错误默认值。

## 实现审计

| 能力 | 审核结果 | 实现位置 / 说明 |
| --- | --- | --- |
| 九个 raw factor | 已实现 | `INTRA_FACTOR_NAMES` + `BaselineReplayState` |
| 60s 右端时间 | 已实现 | publisher 将闭合 bar 左端加 60s |
| q90/q10 数值阈值 | 已实现 | `ExactRollingWindow::quantile_linear(f64)` |
| 固定时间槽 NaN | 已实现 | `observe_slot` 使缺失值推进窗口但不入分布 |
| 1s signal delay | 已实现 | CTA 决策等待 `factor_ts+1s` |
| spot NQ | 已实现 | spot Kafka 增量簿、1min last BBO、60/1440/720/q95 |
| factor/NQ 原子对齐 | 已实现 | 相同 `(symbol, ts)` 才发布 `ModelMsg` |
| raw strict vote | 已实现 | raw 与消息内 q90/q10 严格 `>`/`<` |
| NQ 方向比较 | 已实现 | long `>=`，short `<=` |
| spread gate/cancel | 已实现 | CTA shell 复用 rolling spread thresholds |
| spot maker 四档 | 已实现 | offsets、notional、TTL 进入 ArbOpen context |
| 反向持仓互斥 | 已实现 | CTA lot 尚未退出时拒绝该 symbol 的反方向新开仓 |
| 双向 pending 同时成交 | 已实现单方向门禁 | 反向信号先撤旧 maker 并丢弃本轮；撤单终态前不反手，已有成交由 position gate 阻断 |
| per-lot maker TP | 已实现 | 正常量逐 lot；低于 venue 最小量/名义时按严格 TP 价合并，allocation 保留原 open_id |
| per-lot stop/trailing | 已实现 | CTA 参数从 open `from_key` 固化到 lot |
| max holding | 已实现 | 按首次 fill timestamp 计算 14400s |
| TP/stop 竞态 | 已实现 | reserved TP 先撤，释放后 targeted taker |
| force/lazy taker 隔离 | 已实现 | CTA lot 始终优先走 maker TP，不受通用开关改写 |
| 九规则状态隔离 | 已实现约束 | 一个 CTA env 允许 0 或 1 条规则，禁止多规则 |
| 执行参数 Redis 隔离 | 已实现 | `{env}:cta_strategy_params:{open}:{hedge}` |
| 生产发布 | 未执行 | 本次无远端变更、无启动、无发单 |
| 固定数据逐 bar parity | 待验收 | 必须在发布前生成 live/research 对账报告 |
| 重启持仓恢复 | 明确不在范围内 | 重启视为异常，由人工停止信号并对齐账户、挂单和 lot |

过去文档中以下说法已经失效：

- `score_quantile > 0.9` 不是当前开仓判定；`score_quantile` 仅保留观测用途。
- publisher 不再只读 futures topic；factor 来自 `binance-futures`，NQ 来自
  `binance-spot`。
- lookback 不再是 48h，而是 72h，用于覆盖 factor 内部 warm-up、48h 信号窗口和
  NQ warm-up。
- CTA 不再依赖 `intra_trailing_stop_overrides`；退出参数来自该 lot 的开仓上下文。
- TP 不再按组合加权均价，也不再选择“第一条 enabled rule”。
- 一个 CTA env 不得承载九条 rule；净额账户会破坏独立回测状态。

## 运行架构

```text
Kafka binance-futures PeriodMessage -----------+
                                                +-> intra_factor_model_1m_pub
Kafka binance-spot PeriodMessage -> spot BBO NQ +      -> 9 x model_output
                                                          |
                    one selected model_output per env -----+
                                                          v
                     trade_signal --mode cta -> ArbOpen / ArbCancel / ArbHedge
                                                          |
                                            pre_trade -> trade_engine
                                                          |
                                                Binance spot + futures
```

`intra_factor_model_1m_pub` 是主机共享 publisher，只运行一套。配置：

```text
config/intra_factor_model_1m_pub.toml
factor_topic=binance-futures
nq_topic=binance-spot
lookback_secs=259200
window_size=2880
min_samples=1440
```

输出 service：

```text
model_output/intra-binance-futures-1m-baseline_035
model_output/intra-binance-futures-1m-td_pr_011
model_output/intra-binance-futures-1m-baseline_053
model_output/intra-binance-futures-1m-tp_vpi_006
model_output/intra-binance-futures-1m-td_pr_005
model_output/intra-binance-futures-1m-factor_116
model_output/intra-binance-futures-1m-net_buy_medium
model_output/intra-binance-futures-1m-factor_004
model_output/intra-binance-futures-1m-baseline_091
```

允许 symbol 必须同时在线于 `amount_thresholds_1m`，且 `factor_plan_1m` 完整包含
九因子。当前研究 universe 是 `BTCUSDT/BNBUSDT/ETHUSDT/SOLUSDT/DOGEUSDT/XRPUSDT`；
生产名单必须从目标 env 读回确认。

## 配置契约

每个环境的当前 Redis key：

| 内容 | key |
| --- | --- |
| 信号规则 | `{env}:cta_rules` |
| 执行参数 | `{env}:cta_strategy_params:{open}:{hedge}` |
| trade symbols | `{env}:cta_trade_symbols:{exchange}` |
| dump symbols | `{env}:cta_dump_symbols:{exchange}` |
| spread mapping/value | `cta_spread_thresholds_config_*` / `cta_spread_thresholds_*` |
| pre-trade risk | `{env}:{open}:{hedge}:pre_trade_risk_params` |

信号对象示例：

```json
{
  "rule_id": "baseline_035",
  "model_service": "intra-binance-futures-1m-baseline_035",
  "enabled": true,
  "trade_sides": "both",
  "application": "each_bar",
  "long_quantile": 0.9,
  "short_quantile": 0.1,
  "frequency_seconds": 60,
  "rolling_window": 2880,
  "rolling_min_samples": 1440,
  "signal_delay_seconds": 1,
  "nq_change_enabled": true,
  "max_signal_age_seconds": 120,
  "cooldown_seconds": 0
}
```

执行 hash 字段：

```text
order_notional_usdt=100
open_offsets=[0.0,0.0001,0.0003,0.0005]
open_ttl_seconds=120
max_position_notional_usdt=10000
take_profit=<规则表>
reward_risk_ratio=<规则表>
trailing_stop_enabled=true
trailing_stop_trigger_step=<规则表>
trailing_stop_move_step=<规则表>
max_holding_seconds=14400
```

`sync_cta_rules.py` 和 Rust loader 都拒绝一个环境内多于一条规则；写 `[]` 可安全
停用规则。非法完整快照必须保留上一份已加载配置并报警。

旧的全局 `cta_strategy_params_<open>_<hedge>` hash 不再读取，也不能自动复制给九个
环境。每套环境启用规则前，必须通过 config server 把该规则表对应的执行参数显式写入
新的 env-scoped key，再用 `print_cta_rules.py --env-name <env>` 读回；缺字段或把一套
参数批量复用给九条规则都视为迁移失败。

`ModelMsg` 与 `ArbHedgeSignalQueryMsg` 使用的是当前唯一 IPC 合约；本次字段扩展要求
publisher、`trade_signal` 和 `pre_trade` 协调升级，不允许混跑新旧二进制，也不新增
平行版本协议。

每笔 CTA open 的 `from_key` 固化 rule、方向、factor 时间和值、方向阈值、方向 NQ
值/阈值及全部退出参数。后续部分成交、maker TP 和保护退出都必须保留该事实链，不能
在规则热更新后用新参数改写已成交 lot。

`max_position_notional_usdt=10000` 是回测执行参数和网格静态校验。live CTA 的
pre-trade `max_pos_u` 固定收紧为 `1000U`；该检查只计算已成交仓位，不累计未成交
maker，因此多档同时成交时允许相对 1000U 有有限超调。这是已接受的 live 风险偏差，
不改写 research 的 10000U 参数，也不能宣称两者仓位上限完全对齐。
CTA 环境初始化风险参数必须使用 `scripts/sync_cta_risk_params.py` 或 CTA config server；
不得运行普通 intra 的 `sync_intra_risk_params.py`，后者保留 10000U 默认。由于
env-scoped `max_pos_u_overrides` 命中时优先于基础值，每套 CTA 环境的该覆盖表必须为空，
或所有逐币种值都不超过 `1000U`。

## 环境隔离

九条规则需要九个 RapidX sub-portfolio 或九个事实独立的交易账户/组合。仅拆
`IPC_NAMESPACE` 而共用同一个净额账户不够：交易所持仓、maker TP、强平数量和费用
仍会净额化，无法重建九条独立 NAV。

推荐环境名使用稳定的因子缩写，例如：

```text
binance-cta-b035
binance-cta-tdpr011
binance-cta-b053
binance-cta-tpvpi006
binance-cta-tdpr005
binance-cta-f116
binance-cta-nbm
binance-cta-f004
binance-cta-b091
```

这些名称是目标规划，不是已存在环境。当前编排只登记 `binance-cta-rx01`。新增环境
前必须分配并记录：sub-portfolio、config/viz 端口、IPC namespace、CPU、源 IP
（如有）和持久化目录；部署时同步更新 `scripts/intra_orchestration_lib.sh`、
`docs/core_allocation.md`，涉及 `local_ips` 时再更新 `docs/jp-meta-elvpn_ip_binding.md`。

## 上线阻断项

以下项目未完成前不得启动规则发单：

重启恢复已被明确排除：重启属于异常操作，必须先停止新信号，再由操作人按账户持仓、
当前挂单和成交记录手动对齐；本项目不承诺自动恢复进程内逐 lot 状态。

1. **逐 bar parity 报告**：固定一段 Kafka 历史，逐 symbol 比较 research 与 live 的
   raw、q90、q10、BUY/SELL NQ、q95、allow、direction、decision timestamp。所有有限
   bar 必须数值一致或给出浮点容差；缺失 bar 的 readiness 必须逐项一致。报告必须包含
   输入 topic/partition/offset 边界、research commit、live commit、配置摘要、逐字段最大
   绝对误差、首个差异样本和逐 symbol mismatch 计数，并作为发布制品保留；只比较最终
   BUY/SELL 方向不足以发现阈值或时间标签偏移。
2. **单方向 pending 契约回测验收**：live 已选定不承载双向 gross lot。同一 symbol
   存在旧方向 opening maker 时，反向信号会先撤全部旧 maker、丢弃本轮开仓并等待终态；
   撤单前的成交会形成真实 lot，继续阻断反向开仓。research 必须加入相同契约后重跑
   九条参数选择和固定回放，确认撤单等待造成的漏单属于策略定义，而不是 live-only
   偏差。
3. **逐 lot 仿真**：覆盖四档同时成交、部分成交、TP 部分成交后 stop、撤单失败、查询
   timeout、`min_qty`/`step_size`/`min_notional` 不足后的跨 lot 合并、合并单部分成交及
   4h 超时。必须证明失败释放后每段数量回到原 `open_id`，且不会留下无 spot 身份的裸
   swap lot。
4. **九环境资源与账户**：完成八套缺失环境的账号、端口和 CPU 规划，并验证每套 Redis
   key、IPC、持久化和 client order id 空间独立；逐环境完成旧 strategy hash 到
   env-scoped key 的显式迁移和读回，不允许运行时回退到旧全局 key。
5. **生产现场复核**：确认 `arbmm` 与 `origin/arbmm` 同步、工作树可发布、publisher
   topics 有连续 72h retained 数据、目标 symbol plan 完整、spread q30/q50/q70 已物化；
   每套 env 的 pre-trade risk 必须读回并确认 `max_pos_u=1000`，并确认
   `max_pos_u_overrides` 为空或全部不超过 `1000U`；四档未成交 maker 不计入该上限、
   同时成交可能有限超调属于已接受偏差，不能只依赖规则对象里的 10000U
   静态回测参数。

## 验收与发布

先做本地静态和目标测试：

```bash
cargo fmt --check
cargo test -p rolling_common exact_rolling_window
cargo test -p mkt_parsers model_msg
cargo test -p signal_common hedge_signal
cargo test -p trade_signal cta_config
cargo test -p mkt_signal --lib intra_trailing_stop
cargo check --bin intra_factor_model_1m_pub
cargo check -p trade_signal --bin trade_signal
cargo check --bin pre_trade
```

生产操作只允许从同步后的 `arbmm` worktree 执行。首次只做观察和 check-only：

```bash
git branch --show-current
git status --short
scripts/publish-cta.sh --env-name <env> --check-only
scripts/start-cta.sh --env-name <env> --check-only
```

配置验收至少读回：

```bash
scripts/print_cta_rules.py --env-name <env>
```

启动顺序是 config/viz/persist/trade_engine/account_monitor/pre_trade，基础栈健康后才可
单独启动 `trade_signal --mode cta`。每次 live-impact 操作前都要明确目标 env、交易所、
symbol 范围和操作是否会发单。先单环境、单 symbol、小额度验证，确认 maker TP、保护
撤单和 taker 数量后再扩大。

发现异常时先停止新信号，保留订单更新、持久化和账户监控；随后按目标环境核对并处理
挂单与敞口。不要把回测 `close_on_end` 当作 live 自动回滚机制。

## 可接受执行偏差

以下偏差必须度量，但不是代码可以消除的信号错误：

- 回测 maker 在下一秒激活并按严格穿价成交；live 有网络延迟和真实队列优先级。
- live 可能部分成交、撤单在途或 cancel/fill 竞态；回测事件顺序是确定的。
- 回测 maker order 一次性全成；live 同一 `open_id` 的多次部分成交按该订单累计均价
  管理，属于订单级 lot，不会伪造多个回测中不存在的 order id。
- live 价格与数量按 venue tick/step 向可下单值量化。
- 回测使用固定 maker/taker fee；live 使用账户实际费率、返佣和资金费用。
- 行情 publisher 与交易所撮合时间戳存在传输延迟。

这些应进入 slippage/fill-rate/fee attribution，不得通过改变 raw 阈值、NQ 比较符或
signal delay 来“补偿”。
