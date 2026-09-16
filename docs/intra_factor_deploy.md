# Intra Factor Deploy

## 目的

本文是 intra 同所期现“纯规则盘”的部署约定。后续每个规则盘都应有明确的
`rule_id`、交易所、交易对集合、因子规则和风险参数，并能独立发布、启动、停止、
验收和平仓。

最后更新：2026-09-16

规则来源 notebook（选股/选参 + 规则语义的唯一事实源）：

```text
/home/u171/research/crypto_research/CTA_research/version005_long_short_rust_two_exchange/select_params/analysis_params_binancesymbol6_icir_vs_baseline489_train2024_2026_two.ipynb
```

对应的回测引擎在同目录 `src/signal.rs` / `src/engine.rs`，其
`SignalRule` + `engine` 是实盘语义的对照实现：

- 每个因子独立：`raw value` 对自身 `rolling(2880, min_periods=1440)` 的
  `quantile(0.9 / 0.1)` 严格 `>` / `<` 得到 ±1/0 方向；`trade_sides` 可限制
  只开多或只开空。
- 因子信号是唯一的开仓触发源；`spread_rate=(mid_spot-mid_swap)/mid_spot`
  只做挂单 gate 和撤单 overlay（默认 0.7/0.3 报单、0.5 撤同向未成交单），
  价差自身不会触发开仓。
- 信号触发后只在现货腿挂多档 maker（`quote*(1∓offset)`，TTL 120s）；
  现货成交后 swap 腿挂 `entry*(1±tp)` 的 maker 止盈单（即对冲单，不过期），
  止损/trailing/最长持仓在 swap 腿上走 taker 出场；出场成交后留下的
  spot+perp 对冲对按普通 intra 持仓管理。
- notebook §9 的 9 路 `nanmean` 组合只是一个诊断实验（结论为换手极高、
  方向不成立），**不是**实盘语义：实盘按"9 个因子 = 9 条独立 rule"落地，
  规则之间互不聚合、互不共享状态，组合方式由上层另行决定。

## 当前边界

当前规则盘的部署目标是 `jp-meta-elvpn`。该主机同时承载 Binance Futures 的行情、
trade-flow/factor publisher 和 intra 执行底座；发布或启动前仍需现场复核主机、环境、
symbol 范围和持仓状态。

当前仓库已经具备以下基础能力：

- `fusion_factor_pub` 从 `trade_flow_feature` 维护状态，并支持实时计算
  `baseline_001` 等 baseline，包括 `baseline_035`。
- `trade_flow_feature_pub` 以 5 秒基础 bar 聚合，并在 `enable_1m_bar: true` 时额外
  发布已关闭的 1 分钟 trade-flow bar；`fusion_factor_1m_pub` 已有独立的 1min 输入和
  输出链路。
- `fusion_factor_pub` 通过 TLen 服务读取每个 symbol 的 factor plan，计算结果经过
  z-score 归一化后发布到 `fusion_factor/<venue>`。
- 现有 intra `trade_signal`、`pre_trade`、`trade_engine`、account monitor、
  `persist_manager` 和 `viz_server` 可以复用为执行底座。

### 1min pub 现状

截至 2026-09-14，本次更新后在 `jp-meta-elvpn` 的 Binance Futures 运行链路为：

```text
trade
  -> trade_flow_feature_pub (5s bar)
  -> trade_flow_feature_1m (60s bar)
  -> fusion_factor_1m_pub
  -> fusion_factor_1m/binance-futures
```

对应配置和实现如下：

- `trade_flow_feature_pub` 当前配置为 `bar_ms: 5000`、`enable_1m_bar: true`，所以
  1min bar 已在 pub 内生成，不需要另起一个 1min trade 聚合器。
- `fusion_factor_1m_pub` 是实际运行的独立进程，订阅
  `factor_pub/binance-futures/trade_flow_feature_1m`，输出
  `fusion_factor_1m/binance-futures`。
- 普通 `fusion_factor_pub` 仍是 5 秒链路，不能把它的输出名称
  `fusion_factor/binance-futures` 当作 1min 输出。

本次已将 TLen 的 `factor_plan_1m` 收敛为 notebook 六个 symbol 的完全相同的 9 个因子：

```text
baseline_035, TD_PR_011, baseline_053, TP_VPI_006, TD_PR_005,
factor_116, net_buy_medium, factor_004, baseline_091
```

当前状态是：

- 1min baseline bar 和 `fusion_factor_1m_pub` 正在运行；
- `BNBUSDT`、`BTCUSDT`、`DOGEUSDT`、`ETHUSDT`、`SOLUSDT`、`XRPUSDT` 均已配置这 9
  个因子，包含 `BTCUSDT`；
- `fusion_factor_1m_pub` 已重新加载该 plan，并建立
  `fusion_factor_1m/binance-futures` 输出服务；rolling history 预热完成后才发布完整结果；
- 当前 10 个 `cta-*` `model_1m_pub` 已停止，本次更新不依赖旧 ONNX 模型。

### Raw factor model output

`intra_factor_model_1m_pub` 不消费 z-score 后的 `fusion_factor_1m/{venue}`，
也不再订阅 IPC 的 `factor_pub/{venue}/trade_flow_feature_1m`。自 commit
`bd829ab8` 起它直接消费 Kafka `PeriodMessage`（`config/intra_factor_model_1m_pub.toml`
的 `[kafka]` 段，当前 topic `binance-futures`）：进程内用
`LocalBaselineAggregator` 自行重建 60s trade-flow bar，启动时回放 retained 历史
（`lookback_secs=172800`，即 48 小时）预热 rolling window，随后继续消费 live
Kafka；每次重启用独立 group/client id 且不提交 offset，因此预热结果可复现。
它对 notebook 的 9 个因子逐 symbol 独立维护 raw-value
rolling percentile，并每根有效 1min bar 发布 9 条标准 `ModelMsg`：

- `score` 是 raw factor value；
- `score_quantile` 是该 raw value 在本因子、本 symbol 48 小时窗口内的 percentile rank；
- `score_ready` 仅在积累至少 1440 个有效 raw sample 后为 true；
- 非有限 raw value 不进入 rolling window，并发布 `score_ready=false`。

Binance Futures 的 service 名称固定为：

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

启动时和每 180 秒重载时，该进程只选择同时满足以下条件的 symbol：

- `amount_thresholds_1m` 中在线；
- `factor_plan_1m` 恰好包含上述 9 个因子。

这样 service 名称是 factor 的唯一标识，`ModelOutputHub` 可同时订阅 9 个服务而不会将
同一 symbol 的不同因子覆盖。运行配置在
`config/intra_factor_model_1m_pub.toml`；其中 `127.0.0.1:6322` 指的是部署到
`jp-meta-elvpn` 后主机内的 TLen 服务，并非开发机本地服务。9 个因子名硬编码在
`INTRA_FACTOR_NAMES`（`src/factor_pub/intra_factor_model_1m_pub/app.rs`），
symbol 缺任一因子即整 symbol 不启用。非有限 raw value 不进入 rolling window 且
发布 `score_ready=false`；`score` 恒为 raw 因子值，`score_quantile` 为该值在窗口内的
percentile rank。

2026-09-14 16:38 UTC 的链路检查结果：`trade_flow_feature_pub` 的 1min publisher
已恢复，最近一个统计周期为 `success=96`、`fail_total=1`（1 个 invalid）；
`fusion_factor_1m_pub` 收到 224 条原始消息，其中 15 条属于目标 6 个 symbol，
并完成 `factor_plan=135`、`factor_eval=135`，没有 decode、depth 或 invalid factor
错误。当前 `factor_ready=63`、`factor_warming_up=72`、`published=0`；这是重启后
rolling history 尚未预热完成，不能当作已经有完整因子输出。旧的 1min/5s/RL IPC
static/dynamic cache 已清理并按上游到下游顺序重建；活跃服务文件本身由进程保留。

本 notebook 的规则 rolling 是 `2880` 个 1min bar（48 小时），`min_periods=1440`
（24 小时），分位数为 0.9/0.1。live `zscore_1m` 同样配置为
`window_size=2880`、`min_samples=1440`，但 `intra_factor_model_1m_pub` 的 raw
percentile 不依赖 z-score 值；`baseline_035` 内部每层使用 30 个 1min 样本，首次完整值
至少需要约 59 个 1min 样本。

因子发布正常不等于规则已经进入可交易执行；后续仍需完成 cta 模式的信号
状态机与执行链路（见下节）。

需要特别注意：

1. `baseline_035` 在因子计算层已经存在，但现有 intra funding factor chain 只登记
   `hedge_premium_rate`、`spread_fr` 和历史别名 `premium_rate`。不能只把
   `baseline_035` 写进 Redis 的 `factor_chain` 就得到可交易信号；Rust 侧还没有从
   `fusion_factor` 读取该 baseline 并生成 ArbOpen 的入口。
2. notebook 中的 `baseline_035` 是原始因子滚动分位规则，而 live
   `fusion_factor_pub` 发布的是归一化后的 FeatureMsg。若要求实盘与 notebook
   严格一致，必须明确使用 raw factor，或在回测中复现完全相同的 z-score 过程，不能
   混用两套数值。
3. 当前 intra 的 symbol list 带环境前缀，但 strategy params、funding chain、spread
   mapping 和 rolling metrics 的主要 Redis key 按 venue pair 共享。多个规则盘在同一
   Redis DB 上运行前，必须完成配置作用域隔离；否则一个规则盘的参数更新可能覆盖另
   一个规则盘。

相关实现：

- [live baseline dispatch](/home/fanghaizhou/mkt_signal/src/factor_pub/fusion_factor_pub/app.rs:2048)
- [baseline_035 implementation](/home/fanghaizhou/mkt_signal/crates/factor_engine/src/baseline.rs:842)
- [feature normalization and publication](/home/fanghaizhou/mkt_signal/src/factor_pub/fusion_factor_pub/app.rs:1737)
- [current intra factor lookup](/home/fanghaizhou/mkt_signal/crates/trade_signal/src/arb_open_filter.rs:15)
- [current funding factor-chain AND gate](/home/fanghaizhou/mkt_signal/crates/trade_signal/src/arb_decision.rs:5463)

### cta 模式（trade_signal 第四 mode，进行中）

实盘落地方案已定为在 `trade_signal` 内新增 **`cta`** mode（与
`fr`/`intra`/`cross` 平级，命名全小写），而不是独立的 rule evaluator
进程。环境目录命名沿用 `<exchange>-(intra|cta)-<tag>` 约定，例如
`binance-cta-rx01`：CWD basename 解析出 namespace=`cta`、
key_suffix=`binance`，venue 按 intra 同款规则推断为
`(binance-margin, binance-futures)`。

**已落地（config server + 部署编排）**：

- `ArbMode::Cta` + `CtaShell` 骨架：复用 arb runtime（signal publisher /
  backward channel / min-qty table），独立的 `ModelOutputHub` 订阅。
- `crates/trade_signal/src/cta_config.rs`：规则 schema + 校验 + Redis
  加载。key 为 env 作用域 STRING（exchange 已编码在 env 名里，不再挂
  key_suffix）：

  ```text
  {env_dir}:cta_rules        # 例：binance-cta-rx01:cta_rules
  ```

  value 为 JSON 数组，每条 rule 字段（缺省值对齐引擎）：

  ```text
  rule_id                  必填，[a-zA-Z0-9_-]{1,32}，集合内唯一
  model_service            必填，裸名或 model_output/... 全名
  trade_sides              long|short|both（缺省 both；与引擎同义别名同集）
  long_quantile            0.9   — score_quantile > 此值 → 多方向 vote
  short_quantile           0.1   — score_quantile < 此值 → 空方向 vote
  spread_long_quantile     0.7   — spread_rate > 此值才允许挂 short
  spread_short_quantile    0.3   — spread_rate < 此值才允许挂 long
  spread_cancel_quantile   0.5   — 越过此值撤同向未成交挂单
  rolling_window           2880  — spread overlay 滚动窗口（1m 样本数）
  rolling_min_periods      1440
  frequency_seconds        60
  cooldown_seconds         0
  signal_delay_seconds     1
  application              each_bar | on_change
  order_notional_usdt      100.0 — 单档名义（引擎 order_notional_usdt）
  open_offsets             [0.0, 0.0001, 0.0003, 0.0005] — 网格各档相对 touch
                           价偏移；JSON 小数字面量与科学计数法（1e-4）均可
  open_ttl_seconds         120   — 挂单 TTL（引擎 maker_ttl_seconds）
  max_position_notional_usdt 10000 — 单向名义上限，必须 ≥ 档数×单档名义
  take_profit              0.0   — swap 腿 maker 止盈偏移；0 = 不挂止盈单
  reward_risk_ratio        1.0   — stop_loss = take_profit / rr
  trailing_stop_enabled    true
  trailing_stop_trigger_step 0.001 — trailing 触发步进
  trailing_stop_move_step  0.0005 — trailing 移动步进
  max_holding_seconds      14400 — 最长持仓；0 = 不限制
  enabled                  true
  ```

  校验规则与引擎 `ExecutionConfig`/`SignalRule` 对齐：分位数 ∈ [0,1] 且
  `short_quantile < long_quantile`；`max_position_notional_usdt` 必须覆盖
  一整组网格（`order_notional_usdt × open_offsets.len()`）；`take_profit`
  非负有限；`reward_risk_ratio` 正有限；trailing 开启时两个 step 必须为正；
  `max_holding_seconds ≥ 0`。

- `config_loader` 统一接入：启动 `load_all_once_with_namespace` 即加载，
  之后每 60s 随统一 reload 周期刷新；key 缺失/连接失败/解析失败均保留
  上一份已应用配置并告警；显式写 `[]` 会清空规则与订阅。
- `SymbolList` 对 `cta` namespace 同样走 env 前缀
  （`{env}:cta_fwd_trade_symbols:{suffix}` / `{env}:cta_bwd_trade_symbols:{suffix}`）；
  `sync_intra_symbol_lists.py`/`print_intra_symbol_lists.py` 与
  `sync_intra_strategy_params.py`/`print_intra_strategy_params.py` 已支持
  `--namespace cta`（缺省按 env-name/CWD 的 `-cta-` 段推断）。cta 同步时
  会额外把 bwd 列表镜像到 `{env}:intra_bwd_trade_symbols:{suffix}`——
  pre_trade 的现货借贷白名单固定读该 key（跨模式共享约定）。
- `strategy_params` 的 `return_model_service`/`environment_model_service`
  在 cta 模式下不再接管 model_output 订阅——订阅集完全由 cta rules 的
  `model_service` 去重生成并热更新；`intra_trailing_stop_overrides`、
  hedge 参数等仍随 `cta_strategy_params_{open}_{hedge}` hash 生效。
- `pre_trade` 识别 `<exchange>-cta-<tag>` → `ArbMode::Cta`（margin×futures），
  参与 bwd 借贷白名单刷新与 intra 同款 BBO 保护订阅。
- `intra_config_server.py` 按 CWD 推断命名空间：cta env 下 UI 读写
  `{env}:cta_*` symbol lists、`cta_strategy_params_*`、
  `cta_{spread,funding}_thresholds_*`，并在保存 symbol lists 时同步镜像
  bwd 白名单。
- `scripts/sync_cta_rules.py` / `scripts/print_cta_rules.py`：读写
  `{env}:cta_rules` 的运维工具；sync 在写入前做与 loader 一致的全量校验
  （`--dry-run` 只校验，`--clear` 写 `[]` 清空）。

**部署编排（binance-cta-rx01，RapidX/LTP）**：

```bash
# 本机环境（在 jp-meta-elvpn 上执行；只部署不启动）
scripts/deploy_cta_binance_std.sh rx01          # 或 --env-suffix rx01

# 发布 / 启动 / 停止（远端编排，复用 intra 安全流程）
scripts/publish-cta.sh --env-name binance-cta-rx01
scripts/start-cta.sh   --env-name binance-cta-rx01
scripts/stop-cta.sh    --env-name binance-cta-rx01
# 以上均支持 --check-only / --all
```

- env 注册在 `scripts/intra_orchestration_lib.sh` 的 `cta_configure_env`
  （host=`jp-meta-elvpn`，backend=`ltp`，config=19174，viz=10186，
  account_monitor=`rapidx_account_monitor`）。
- `deploy_cta_binance_std.sh` 固定 ltp 后端，core layout：
  account_monitor=32 / trade_signal=33 / pre_trade=34 / trade_engine=35 /
  persist_manager=15（共享）。
- publish 的 manifest 在 intra 文件集上追加 `sync_cta_rules.py` /
  `print_cta_rules.py`；staging 目录 `.publish-cta.*`；`--all` 只遍历
  `CTA_ORCHESTRATION_ENVS`，与 intra 的 `--all` 互不触碰。

**尚未实现（下一步）**：

- `drive_cta_decision` 目前是 no-op：逐 `(rule_id, symbol)` 的 vote→方向
  状态机、spread overlay 滚动分位（进程内自攒 ~24h 预热）、
  ArbOpen/ArbCancel 发放还没写。
- 现货成交后"按开仓价锚定 maker 止盈对冲 + 止损联动撤单"的 per-lot
  hedge 路径（1:1 映射已有 `IntraTrailingBook`/`HedgeAllocation`/`borrow_open_id`
  骨架，但聚合式 due-hedge query 需要改为按 open lot 维度）。
- `nq_change` filter；trailing/tp/rr/max_holding 的字段已在 config 中，
  执行侧消费路径（per-rule 参数下发到 `IntraTrailingBook`）待接。

## 目标架构

推荐将“因子计算”和“规则执行”分离（rule evaluator 已定为 trade_signal
的 `cta` mode，见上节）：

```text
market data
    -> trade_flow_feature_pub
    -> fusion_factor_pub (5s) / fusion_factor_1m_pub (1min)
    -> intra_factor_model_1m_pub (9 路 model_output raw 分位)
    -> trade_signal --mode cta (9 条独立 rule 状态机)
    -> existing ArbOpen / ArbClose / ArbCancel IPC
    -> pre_trade
    -> trade_engine
    -> spot + futures execution
```

同一交易所可以共享一个 `fusion_factor_pub`/`intra_factor_model_1m_pub`。
每个规则盘独立运行一套 `cta` mode 的 `trade_signal` + intra execution stack：

```text
rule_set=baseline035  -> <exchange>-cta-<tag>   （如 binance-cta-v005）
rule_set=baseline036  -> <exchange>-cta-<tag>
rule_set=rule_x       -> <exchange>-cta-<tag>
```

一个 cta env 内部可以承载多条 rule（同一进程按 `rule_id` 隔离状态）；不同
env 之间必须使用自己的 `IPC_NAMESPACE`、env 前缀的 symbol lists 与
`{env}:cta_rules` 规则集、策略参数、日志和持久化目录。不同规则盘
不能共享同一个 trade_signal 进程的运行时状态。

## 规则契约

每个规则盘上线前必须有一份不可歧义的规则配置，至少包括：

```text
rule_id
factor_source       # 例如 fusion_factor/binance-futures
factor_name         # 例如 baseline_035
factor_value_mode   # raw 或 zscore，必须与回测一致
bar_frequency       # 例如 60s
long_rule           # 例如 value > rolling_quantile(90%)
short_rule          # 例如 value < rolling_quantile(10%)
rolling_window
rolling_min_samples
signal_delay
entry_policy        # maker/taker、挂单偏移、超时
exit_policy         # neutral/反向/止盈止损/最长持仓
order_amount
symbols_fwd
symbols_bwd
```

规则执行的默认安全语义：

- 因子缺失、warming-up、时间戳过旧或非有限值时，不发开仓信号。
- 多空同时满足时，默认 flat 并记录冲突原因。
- 因子回到中性区不自动平仓，除非 `exit_policy` 明确配置为 neutral close。
- 开仓方向和退出方向分开记录，不能用“重新发反向开仓”隐式代替平仓。
- 每个信号必须携带 `rule_id`、symbol、factor timestamp、factor value、threshold、
  direction 和 decision reason，便于回测与实盘逐笔对账。

## 配置作用域

当前 key 形状需要先盘点，再扩展为统一的 deployment-scoped 形状。目标是同一
Redis 实例可以安全承载多个规则盘；不要通过 `v1`/`v2` 或临时后缀维护两套协议。

至少需要隔离以下内容：

| 配置 | 当前用途 | 多规则盘要求 |
| --- | --- | --- |
| symbol lists | fwd/bwd/dump/vol gate | 每个 `env_name` 独立 |
| factor rule | 因子、阈值、分位数和退出规则 | 每个 `rule_id` 独立 |
| strategy params | 下单量、超时、对冲和冷却 | 每个 `rule_id` 独立 |
| rolling params | rolling window、factor quantiles | 明确按规则或共享只读 |
| spread mapping | maker/taker 开仓和平仓阈值 | 每个规则盘明确归属 |
| risk overrides | amount、max position、hedge limits | 每个环境和规则盘独立 |

配置加载必须做到“完整快照替换”，缺失或非法快照保留旧配置并告警；不能把上一
个规则盘残留的 symbol 或阈值拼接到当前规则盘。

## 环境命名与部署

环境名统一使用：

```text
<exchange>-intra-<tag>        # 现有 intra 盘
<exchange>-cta-<tag>          # cta 规则盘（namespace=cta）
```

例如：

```text
binance-intra-arb01
binance-cta-v005
okex-intra-arb01
bybit-intra-arb01
```

仓库当前的远程编排入口只登记了固定环境和端口。新增纯规则盘时，必须同步检查
并更新以下位置后再部署：

- `scripts/intra_orchestration_lib.sh`：目标主机、exchange、端口和 execution backend。
- 对应的 `deploy_intra_<exchange>.sh`：环境初始化、凭证模式和 core layout。
- `start-intra.sh` / `stop-intra.sh` / `publish-intra.sh`：supported environment 列表。
- Nginx 的 config/viz 路由和 `docs` 中的端口记录。

环境目录由 `deploy_setup_env_intra.sh` 创建。它会生成 `env.sh`，包括
`IPC_NAMESPACE`、open/hedge venue 和执行后端。凭证只能存在环境目录的 `env.sh`，
不能写进仓库或文档。

## 标准发布流程

以下命令是操作顺序模板。生产操作前必须确认目标环境、交易所、symbol 范围和
当前是否有持仓；这些命令不替代上线审批。

### 1. 本地构建和静态检查

```bash
git branch --show-current
git status --short
cargo fmt --check
cargo check --bin trade_engine
cargo check --bin pre_trade
cargo check --bin trade_signal
scripts/build-intra-binaries.sh
```

生产发布只允许从同步后的 `arbmm` worktree 进行。

### 2. 创建或更新环境

使用对应交易所的 deploy-only 入口。示例：

```bash
scripts/deploy_intra_binance_std.sh arb03
```

该步骤只创建/更新 `$HOME/binance-intra-arb03/`，不会启动进程。OKX、Bybit、Gate
和 Bitget 使用各自的 `deploy_intra_<exchange>.sh` 入口。LTP backend 只能用于脚本
已支持的 exchange，并且必须在 `env.sh` 中配置完整的 LTP 凭证和 portfolio id。

### 3. 发布二进制和运行脚本

已有远程环境使用：

```bash
scripts/publish-intra.sh --env-name <exchange>-intra-<tag>
```

发布器会先确认目标进程全部停止，再进行 staging、SHA-256 校验和原子替换；不会
上传或覆盖 `env.sh`、凭证、数据和日志。只更新本地构建产物时可使用
`--skip-build`，但必须确认 `target/release` 是本次构建结果。

### 4. 启动基础执行栈

```bash
scripts/start-intra.sh --env-name <exchange>-intra-<tag>
```

启动顺序为 config server、viz server、persist manager、trade engine、account
monitor、pre-trade。该入口故意不启动 `trade_signal`，并要求它保持停止状态。

基础栈健康后，再单独启动已验收的规则信号进程：

```bash
cd "$HOME/<exchange>-intra-<tag>"
./intra_scripts/start_intra_trade_signal.sh
```

在纯规则执行器正式接入前，上述 `trade_signal` 只代表现有 intra 信号逻辑，不能
声称已经执行新的 baseline 规则。

## 因子规则上线前验收

### 回测一致性

- 因子名称、输入字段、时间频率、滚动窗口、最小样本数、分位数和 delay 完全一致。
- 对齐 raw/z-score 语义、NaN/warming-up、symbol 过滤和冲突处理。
- 明确中性区的持仓处理，不把 notebook 的简化 PnL 当成 maker/taker 执行结果。
- 逐 symbol 比较一段固定时间的 factor value、threshold、direction 和 signal timestamp。

### 实时数据链

- 确认 `trade_flow_feature` 持续更新，`fusion_factor_pub` 的 factor plan 包含目标
  factor，且目标因子不是 warming-up。
- 确认 `fusion_factor/<venue>` 有新消息，消息的 symbol、timestamp、status 和
  factor 顺序与规则 evaluator 的映射一致。
- 确认因子断流、旧消息和非法值只会阻止开仓，不会产生错误方向信号。

### 执行链

- 确认 ArbOpen/ArbClose 进入正确的 `IPC_NAMESPACE`。
- 确认 `pre_trade` 通过余额、借贷、杠杆、最大敞口和交易所规则检查。
- 确认现货腿和期货对冲腿的数量、价格精度、订单方向和 client order id 可追踪。
- 先使用无发单模式或隔离 symbol 做小规模验证，再扩大交易对集合。

## 运行观察与回滚

上线后至少记录以下指标：

```text
factor messages / stale messages / warming-up messages
rule evaluations / long / short / neutral / conflict
signal intercept reason
open orders / fills / partial fills / hedge latency
unhedged quantity / cancel retry / query repair
per-symbol position and realized PnL
```

发现因子映射、阈值或执行行为异常时，先停止规则信号进程，保留基础风控和订单
状态处理，再撤单并核对敞口。标准应急顺序见
[撤单与平敞口命令](/home/fanghaizhou/mkt_signal/docs/close_orders_and_exposure_cheatsheet.md)。

```bash
scripts/stop-intra.sh --env-name <exchange>-intra-<tag>
```

`stop-intra.sh` 会停止执行栈、撤销该环境订单并执行状态检查；实际生产使用前仍
必须人工确认 exchange、环境和 symbol 范围。

## 新增一个纯规则因子的最小变更集

新增规则不能只改一个 Redis JSON。至少需要完成：

1. 在 factor engine / fusion publisher 中实现并测试因子，确认 live 和 replay 一致。
2. 定义 factor name、输入字段、单位、频率、readiness、staleness 和 raw/z-score 语义。
3. 在 rule evaluator 中登记因子索引/映射和方向规则。
4. 增加该规则的配置作用域、symbol list、阈值和策略参数。
5. 增加固定数据回放、断流、warming-up、多空冲突、反向和重复消息测试。
6. 发布二进制后执行 check-only、基础栈健康检查、信号 dry-run 和小范围实盘验收。

不要把新的 baseline 追加到现有 funding factor chain，除非该因子确实是由
`arb_open_filter::lookup_factor_realtime_value` 提供的实时值，并且已经明确采用
funding filter 的 per-symbol rolling threshold 语义。
