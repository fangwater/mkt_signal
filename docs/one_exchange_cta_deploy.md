# 单所 CTA 部署（纯合约）

最后更新：2026-09-21

本文是 V007 单所纯合约 CTA 的**唯一权威实现文档**：研究契约、参考
notebook、部署因子、rx01 框架复用分析、代码级卡点、改造清单、部署
参数和待定问题全部在此。两所版（rx01）契约见 `intra_factor_deploy.md`。

## 目标

部署两个互相独立的单所 CTA 环境（框架沿用 rx01，去掉价差/现货腿，
只交易合约）：

| env（拟定名） | 因子 | model service（拟定） |
| --- | --- | --- |
| `binance-cta-special-rx02` | `TP_VPI_018` | `model_output/one-binance-futures-1m-tp_vpi_018` |
| `binance-cta-special-rx03` | `baseline_104` | `model_output/one-binance-futures-1m-baseline_104` |

两因子均为 baseline489 族成员，`BaselineReplayState` + `factor_plan_1m`
现成求值，不需要 ICIR/gplearn 求值器。每个 env 只加载**一条规则、
一组钉死的执行参数**（trig/move/exit_q_long/exit_q_short/nq）。

## 参考来源（只读）

规则与语义以 V007 单所研究树为准：

```text
/mnt/hdd-raid5-72t/liang_torch/share/version007_long_short_rust_one_exchange/
├── select_params/analysis_params_binancesymbol6_v007_factor_exit_train2024_2026.ipynb  ← 选参 notebook
├── src/signal.rs            信号判定（q90/q10、each_bar、conflict=flat）
├── src/filters.rs           NQ 过滤（已参数化，默认值即 v007 口径）
├── src/engine.rs            撮合/离场（因子平仓、trailing、无 TP）
├── src/config.rs            参数校验（禁 TP/RR/独立撤单分位；exit 分位须在入场带内侧）
├── factor_backtest_baseline489_train2024_2026.toml        ← symbol6 90 组主配置
└── factor_backtest_baseline489_train2024_2026_v008.toml   ← v008 干净重跑配置（未开跑）
```

rx01（两所版）历史参考，仅用于框架对照：

```text
/home/u171/research/crypto_research/CTA_research/version005_long_short_rust_two_exchange/
select_params/analysis_params_binancesymbol6_icir_vs_baseline489_train2024_2026_two.ipynb
```

线上对照环境：`jp-meta-elvpn:~/binance-cta-rx01`（config 19174 / viz 10186，
`td_pr_005`，open=binance-margin / hedge=binance-futures）。

## 入选因子状态

notebook §4b 最新一轮（2026-09-20，`valid/test return>0.03 && auc<0.15`）：

- com 线过线 32 组 / 4 因子：`TP_VPI_018`(22)、`baseline_076`(2)、
  `baseline_104`(3)、`baseline_138`(5)，全部 baseline489 族
- long / short 线 0 组（cell17 的 long 图是旧阈值残留输出）
- **每个因子仍有多组参数未定**——最终参数组要等费率复跑锁定
  （v005 流程是 §8 `fee_m0_t175` 复跑后定 `parameter_id`）

## 回测契约

### 时间与信号

每个因子、每个 symbol 独立计算：

```text
bar frequency        = 60s
row timestamp t      = 已关闭区间 [t-60s, t) 的右端标签
rolling window       = 2880 个固定频率槽位
normalization        = raw 因子先按 1440 窗口、1000 min_periods、clip_zscore=3.0 做滚动 z-score
minimum samples      = 归一化后 1440 个有限 score
long threshold       = linear_quantile(window including current, 0.9)
short threshold      = linear_quantile(window including current, 0.1)
long                 = zscore > long_threshold
short                = zscore < short_threshold
application          = each_bar
cooldown             = 0
signal_delay_seconds = 1（maker 在 cts+1 创建）
conflict policy      = flat
```

### NQ 过滤（与两所版不同）

单所版用**本合约自己的 mid**，不是 spot BBO（`filters.rs` 默认值）：

```text
price                = mid（(bid+ask)/2 本合约盘口价）
lookback_bars        = 1440 根有效 bar，包含当前 close
quantile_window      = 1440
min_periods          = 720
BUY_NQ_CHANGE        = (close - rolling_min(close, 1440)) / rolling_min
SELL_NQ_CHANGE       = (close - rolling_max(close, 1440)) / rolling_max
long/short quantile  = 0.5，包含当前值
long allow           = BUY_NQ_CHANGE >= q0.5
short allow          = SELL_NQ_CHANGE <= q0.5
```

NQ 只放行/拦截开仓信号，不产生方向。

### 无 spread overlay

没有 `spread=(spot_mid-swap_mid)/spot_mid` 概念。`cta_spread_thresholds_*`
Redis key 与 rolling q30/q50/q70 在纯合约版不消费、不物化。

### 开仓执行

```text
venue                = binance-futures（open 与 exit 同 venue）
每次有效信号挂四档 maker：
offsets              [0.0, 0.0001, 0.0003, 0.0005]
notional per level   100 USDT
maker TTL            120s
maker 从 order_ts+1 生效，严格穿价成交，不移动、不追价
本秒成交的 lot 从 t+1 才允许离场
```

反向信号只撤销该 symbol 未成交的反向 maker，不平已成交仓位；同一
symbol 的 opening maker 同时只允许一个方向。

### 离场（与两所版根本不同）

无固定止盈、无入场硬止损、`max_holding_seconds=0`。撤单与平仓共用
同一组因子分位阈值：

```text
long  position     percentile < factor_exit_quantile_long  -> Taker 平仓
short position     percentile > factor_exit_quantile_short -> Taker 平仓
未成交 maker       同一阈值 -> 撤销（衰减撤单）
```

移动止损保留（大步长版），只在浮盈达到 trigger 后锁利：

```text
trigger ∈ {0.01, 0.02, 0.05}   move ∈ {0.005, 0.01}   move < trigger（5 组）
level = floor(最大有利收益 / trigger)，只能增加
long_stop  = entry * (1 + level * move)   futures bid0 更新与判断
short_stop = entry * (1 - level * move)   futures ask0 更新与判断
触发后 Taker 平仓
```

所以按价格风控口径，V007 **没有初始止损**。trailing 在达到首个盈利
trigger 前完全未激活，价格反向本身不会触发平仓；因子退出虽然可能在亏损
时平仓，但它不是按入场价计算的止损。`max_holding_seconds=0` 也不提供超时
兜底，且交易所侧没有预挂保护止损单。

### 网格与校验（09-20 口径）

```text
每因子 90 组 = NQ 开/关 2 × trailing 5 × exit 分位 3×3
"execution.trailing_stop_trigger_step"  = [0.01, 0.02, 0.05]
"execution.trailing_stop_move_step"     = [0.005, 0.01]
"execution.factor_exit_quantile_long"   = [0.3, 0.5, 0.7]
"execution.factor_exit_quantile_short"  = [0.3, 0.5, 0.7]
"filter.nq_change.enabled"              = [true, false]
trade_sides 已移出搜索网格，固定 combine（代码仍支持 long/short/both）
```

`config.rs` 禁止 `take_profit`/`reward_risk_ratio`/
`take_profit_order_type`/`cancel_quantile_*` 作为策略参数，并校验
`factor_exit_quantile_long < 0.9`、`factor_exit_quantile_short > 0.1`。

## 当前实现：隔离的 `cta_special`

```text
Kafka binance-futures PeriodMessage（因子和 futures-mid NQ 同源）
        v
cta_special_factor_model_1m_pub
  raw -> clip + rolling z-score -> exact q90/q10 + score_quantile
        v
model_output/one-binance-futures-1m-<factor>
        |-- cta_special_signal（只判定开仓）
        |       `-- ArbOpen：四档 futures maker
        |
        `-- pre_trade 直接订阅 1m ModelMsg
                |-- 因子衰减撤销未成交 maker
                |-- 因子退出
                `-- BBO trailing + 逐 lot 状态
                         v
                 CtaSpecialStrategy
        v
ArbCloseStrategy（Market + reduce_only=true）
        v
trade_engine -> LTP/RapidX sub-portfolio -> Binance futures
```

旧 `trade_signal` CTA 规则引擎、`ArbHedgeStrategy`、现货腿、spread
overlay、旧 `cta_config_server.py` 和旧 viz 页面都不参与这条链路。
`exec-pre-trade`/BatchExec 也不参与。

### 代码边界

| 部分 | 实现 |
| --- | --- |
| 开仓 IPC | 只复用标准 `SignalType::ArbOpen`；没有 CTA special 因子 signal type |
| 因子 publisher | `src/factor_pub/cta_special_factor_model_1m_pub/` |
| 开仓信号进程 | `src/cta_special/`、`src/bin/cta_special_signal.rs` |
| pre-trade 因子订阅 | `src/pre_trade/cta_special_factor_channel.rs`，直接消费同一 `ModelMsg` |
| 平仓与执行状态 | `src/strategy/cta_special_strategy.rs` |
| pre-trade 模式 | `ArbMode::CtaSpecial`；目录必须为 `binance-cta-special-<tag>` |
| 配置服务 | `scripts/cta_special_config_server.py` + `web/cta_special_config/` |
| 只读 dashboard | `scripts/cta_special_dashboard.py` + `web/cta_special/` |
| 部署/总控 | `scripts/deploy_cta_special.sh`、`scripts/{start,stop}_cta_special.sh` |

配置服务只原子读写 `config/cta_special.json`，保存时做 revision 冲突
检查。signal 和 pre-trade 都每秒检测同一文件；非法配置保留上一份有效
配置。dashboard 只读 signal 状态和 pre-trade 执行状态，不具备交易写入口。

### 执行保护

- 只允许 `binance-futures`，且 `binance=ltp`；缺少
  `LTP_PORTFOLIO_ID` 直接拒绝启动。
- 专用 env 只接受带 `cta_special=1` 事实标记的 `ArbOpen`；其他模式
  同时拒绝该标记，避免信号串环境。
- publisher 回放历史只用于滚动窗口预热，追平 Kafka 高水位前不发布
  ModelMsg。signal 和 pre-trade 都拒绝超过 120 秒或超前 5 秒以上的消息；
  只有 signal 进程启动后新闭合的 bar 才能开仓，pre-trade 可消费启动时
  缓存的新鲜 bar 来恢复因子退出状态。
- 反向信号先撤对向未成交 maker；存在真实反向仓位时禁止新开仓。
- 因子退出与 trailing 退出统一走 `ArbCloseStrategy`，数量不超过真实
  仓位，向下按步长对齐，订单固定 `reduce_only=true`。
- CTA pre-trade 每次启动都复用 Exec 的启动撤单门禁：先撤销当前 RapidX
  portfolio 下全部 Binance PERP 挂单并确认 open orders 为空；撤单失败或
  超时则拒绝启动，不进入策略处理。
- pre-trade 重启后若本地 lot 账本为空但账户已有净仓位，会以当前 mark
  price 作为总体入场价，将全部净仓合并成一个恢复 lot；该 lot 同时支持
  因子退出和 trailing。mark price 尚不可用时不创建恢复 lot，也不使用
  BBO mid 代替。
- 默认 `enabled=false`；总控启动/停止脚本默认 dry-run，必须显式传
  `--execute`。启动前先校验 futures/futures、`binance=ltp`、
  `LTP_PORTFOLIO_ID` 和完整策略 JSON，失败时不会启动任何进程。

### 配置与 Redis

`config/cta_special.json` 只保留需要由操作员调整的策略变量：

```json
{
  "enabled": false,
  "rule_name": "tp_vpi_018",
  "symbols": ["BTCUSDT"],
  "entry": { "nq_change_enabled": true },
  "execution": {
    "order_notional_usdt": 100.0,
    "factor_exit_quantile_long": 0.3,
    "factor_exit_quantile_short": 0.7,
    "trailing_stop_trigger_step": 0.02,
    "trailing_stop_move_step": 0.01
  }
}
```

venue、model service、q90/q10 开仓阈值、1 秒信号延迟、四档 maker
offset、120 秒 TTL、trailing 开关、消息新鲜度和轮询参数都是此模式的
固定契约，由代码派生或固化，不再重复暴露为配置。配置解析严格拒绝这些
已删除字段，避免页面、signal 和 pre-trade 对同一语义出现不同值。
`rule_name` 决定 model service，运行中不可修改；其余字段支持热加载。
重复部署已有 env 时，部署脚本会原子迁移旧 JSON，只保留上述字段并立即
用正式解析器校验。

该模式不再使用旧 CTA 的 `cta_rules`、`cta_strategy_params`、symbol list
或 spread Redis key。pre-trade 通用风控仍使用独立 env 前缀的唯一 hash：

```text
{env}:binance-futures:binance-futures:pre_trade_risk_params
```

首次部署后使用 `intra_scripts/sync_cta_risk_params.py` 初始化，默认
`max_pos_u=1000`。该 key 只服务对应 `cta_special` env，不与 rx01 共用。

### 本地部署

```bash
scripts/deploy_cta_special.sh \
  --env-name binance-cta-special-rx02 \
  --factor tp_vpi_018 \
  --config-port 19182 \
  --dashboard-port 10192

cd ~/binance-cta-special-rx02
./intra_scripts/sync_cta_risk_params.py \
  --env-name binance-cta-special-rx02 \
  --open-venue binance-futures \
  --hedge-venue binance-futures
./scripts/start_cta_special.sh            # dry-run
./scripts/start_cta_special.sh --execute  # live process mutation
```

部署脚本只允许干净且与 `origin/arbmm` 同步的 `arbmm` worktree，不启动
任何进程，不覆盖已有 `env.sh`，新建策略配置始终为 disabled。

## 已定边界与部署待办

| # | 项目 | 当前结论 | 状态 |
| --- | --- | --- | --- |
| 1 | 每 env 的执行参数组 | TP_VPI_018 有 22 组过线、baseline_104 有 3 组；代码不设隐式默认交易参数 | **费率复跑后填写 JSON** |
| 2 | env 命名 | `binance-cta-special-rx02` / `binance-cta-special-rx03` | 已定 |
| 3 | 单 venue 路由 | `ArbMode::CtaSpecial`，futures open + futures reduce-only exit | 已实现 |
| 4 | exit 分位载体 | pre-trade 直接订阅 1m `ModelMsg`；不经过 trade signal | 已实现 |
| 5 | 因子平仓粒度 | 按实际成交 lot 记账，同阈值的同向 lot 聚合平仓 | 已实现 |
| 6 | model service | `one-binance-futures-1m-<factor>` | 已定 |
| 7 | LTP portfolio | 每个 env 独立 `LTP_PORTFOLIO_ID` | **部署时分配** |
| 8 | 端口/CPU | deploy 参数显式给出，不能与 rx01 冲突 | **部署时分配** |
| 9 | reduce_only | 代码固定设置；仍需 LTP/Binance 实盘最小量验证 | **上线前实测** |
| 10 | lot 参数快照 | 随 `ArbOpen.from_key` 进入成交 lot，热更新不改旧 lot | 已实现 |

## 上线阻断项

1. **逐 bar parity 报告**：固定 Kafka 历史逐 symbol 比较 research 与
   live 的 raw、zscore、q90、q10、score_quantile、BUY/SELL NQ（futures mid）、
   allow、direction、decision timestamp。
2. **因子离场对账**：同一持仓在 research 与 live 的 factor_exit 触发
   秒、撤单秒、trailing level 序列一致或可解释（浮点容差内）。
3. **同 venue 开平核验**：open/exit 同走 binance-futures 时逐 lot
   数量台账、client order id 空间、成交归并不混淆开平仓。
4. **reduce_only 实测**：平仓单在账户层面确实只减仓、不穿仓。
5. **费率口径**：Maker 0 / Taker 0.000175 与账户实际费率的偏差进入
   slippage/fee attribution，不得改阈值补偿。

## 验证命令

```bash
cargo fmt --check
cargo test --lib cta_special::
cargo test --lib pre_trade::cta_special_factor_channel::
cargo test --lib factor_pub::cta_special_factor_model_1m_pub::
cargo test --bin pre_trade infer_arb_mode_uses_dir_namespace_before_venue_shape
cargo check --bin cta_special_factor_model_1m_pub --bin cta_special_signal --bin pre_trade
python3 -m unittest scripts.tests.test_cta_special_servers
bash -n scripts/{deploy,start,stop}_cta_special.sh scripts/{start,stop}_cta_special_*.sh
```

上线阻断项中的逐 bar parity、LTP reduce-only 实盘和成交归并对账不能由
本地单元测试替代。
