# 单所 CTA 部署（纯合约）

历史部署档案（2026-10-13 归档）：以下路径、命令和运行状态仅记录旧实现，不能用于当前部署。旧 CTA 执行代码已移除，RX01 交易进程已停止；新 RX01 发布保持关闭。当前仅维护独立的 `cta_perp` / `cta_spread` 仓位发布套件，见 [当前命名与策略契约](cta_signal_families.md)。

最后更新：2026-10-12 06:05 UTC

本文说明 `cta_special_signal` 的单所纯合约 CTA 实现、按币独立因子配置和部署流程。两所版（rx01）见 `intra_factor_deploy.md`。以下研究来源保留用于核对基础因子与 NQ。历史网格、逐 lot 和移动止损规则不参与当前 special 路径；当前独立策略契约见下文。

## 当前计算和交易范围

当前部署机上的 special publisher 计算 16 个 USDT 合约 × 6 个因子，共 96 个币种/因子组合：

```text
FIL AVAX LTC DASH ZEC NEAR AAVE UNI LINK XLM SOL BNB DOGE DOT XRP ETH
factor_036 factor_051 factor_170 factor_004 TD_TI_002 baseline_004
```

`intra_factor_model_1m_pub` 服务普通 CTA，`cta_special_factor_model_1m_pub` 提供独立的 `model_output/one-binance-futures-1m-<factor>` 服务。各 env 从 Redis 选择自己的币种与组合。因子名称必须有实际求值实现，且已由共享 publisher 发布；Redis 修改不会自动扩大 publisher 的计算范围，扩大范围需修改 `[universe]` 并重新发布 publisher。

rx02 已配置指定的 16 币 32 项有符号权重，使用五个因子（不使用 `factor_004`）；
rx03 已停盘，原配置和历史保留。RX02 的执行迁入标准 `binance_exec_trade05`，
通过 Manager 跟随虚拟账户 `lgq_cta`，五个因子各一份、ctaker 执行。普通 CTA publisher 和 rx01 维持原部署，旧 TLen 元数据保留。
部署脚本不会自动改写旧策略文件或初始化 Redis。

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

## 历史研究回测契约（不代表当前 special 执行）

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
lookback_bars        = 1440 个分钟槽位，包含当前 close；缺失值占位但不参与统计
quantile_window      = 1440
min_periods          = 720
BUY_NQ_CHANGE        = (close - rolling_min(close, 1440)) / rolling_min
SELL_NQ_CHANGE       = (close - rolling_max(close, 1440)) / rolling_max
long/short quantile  = 0.5，包含当前值
long allow           = BUY_NQ_CHANGE 的历史分位排名 >= 0.5
short allow          = SELL_NQ_CHANGE 的历史分位排名 <= 0.5
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

## 当前实现：每因子独立策略

rx02 使用五个因子策略：`factor_036`、`factor_051`、`factor_170`、`TD_TI_002`、`baseline_004`，16 币共 32 项绑定。每个 factor 是一个标准 Exec strategy；同 factor 的不同币种分别持仓。策略之间的账本、qty 和 PnL 独立，special 发布端不再做因子轧差或增量加仓。rx03 整套信号、配置、图表、监控和执行停止。

```text
共享 special publisher（16 币 × 6 因子，原 rolling/NQ）
  → cta_special_signal（按 factor/symbol 独立读取）
  → Manager 仓位策略 lgq_cta_<factor>（数值 qty，退出为 0）
  → 虚拟账户 lgq_cta（virtual02，五个因子各 1 份）
  → RX02 / binance_exec_trade05（follow，multiplier=1，ctaker）
  → Manager 发布独立 Redis BatchExec targets
  → 标准 exec-pre-trade → 现有 trade_engine → RapidX Binance USDT PERP
```

RX02 当前 `enabled=true`，用户授权 trade05 实盘执行。signal 环境只运行共享 publisher、signal、config 和 dashboard；账户监控、持久化、trade_engine、exec-pre-trade、Viz、Exec Config 归 trade05。两个旧 companion 执行目录均停止，trade05 的普通 `trade_signal` 不启动。信号关闭发布时只维护观察状态，不主动清除已有执行目标；停交易或清仓须在 Manager/Exec 明确操作。

### 信号与仓位

`symbol → factor → signed weight` 的 Redis 整表保留，但独立策略只使用符号：正数取原分位，负数取 `1-q`，零禁用该项；绝对值不缩放、不归一化。保存检查因子实现及当前 publisher 的实际计算范围，重复 key、非法因子和币种均拒绝。

```text
空仓：修正 q > entry.long（当前 .9）并通过多 NQ → 满多
空仓：修正 q < entry.short（当前 .1）并通过空 NQ → 满空
多仓：修正 q < exit.long（当前 .3）→ clear
空头仓位：修正 q > exit.short（当前 .7）→ clear
其余：保持原 qty，不追加、不按价格或新额度重算
```

退出优先；一根 bar 上先发布 clear，下一根新 bar 才能重新开仓。NQ 只检查开仓，阈值仍为多 ≥ .5、空 ≤ .5，不限制平仓。rolling、NQ 的计算和预热窗口保持现有实现。一个因子未就绪不会阻塞其他因子的开平仓。

代码中的 `max_pos_u` 表示每个已启用 `symbol × factor` 组合的单份 USDT 开仓金额。每份独立使用完整金额，不按币种因子数平分，也不扣减其他因子的入场金额。多头 `qty=max_pos_u/ask`，空头 `qty=-max_pos_u/bid`，要求报价有效且新鲜。RX02 的 16 币、5 因子完整笛卡尔积为 80 项，现有配置仅启用其中 32 项；修改算法不会扩展绑定。单份配置为 200 USDT，LINK 四份各 200、LTC 三份各 200、ETH 一份 200，32 份全部开仓的入场金额绝对值合计为 6,400 USDT；同币种的账户净仓仍由执行端合并。持仓 qty 固定，修改金额在后续开仓生效，退出平原 qty；已有持仓不会因切换算法而自动补足或缩减。市价变动会使当前市值偏离入场金额。2026-10-12 06:05 UTC 已部署至 RX02 实盘，单份 200 USDT；旧仓按原 qty 持有到退出，后续开仓使用新额度。

### Redis 配置和普通 Exec 合约

信号参数统一保存在 `config/cta_special.json`，包含交易开关、单份满仓金额、四个开平仓分位及 NQ。币种与因子符号绑定仍为 Redis 整表 `<signal-env>:cta_symbol_factors`。旧 `<signal-env>:cta_factor_strategies` 金额表、逐因子金额 API 和对应脚本已移除，部署前在环境本地备份。

配置页面保存时检查币种和因子可用性，revision CAS 防止覆盖并发编辑。信号约一秒刷新配置；真实交易关闭时只更新观察状态，不读取或修改 Exec 文档。Exec 订单参数由 CTA Manager 统一管理，信号端不能设置这些参数。

单份金额统一配置，每个非零权重的币种与因子组合独立使用。配置键保留 `max_pos_u`，配置页显示“单份 USDT”。配置示例：

```json
{
  "enabled": false,
  "max_pos_u": 200,
  "entry": {
    "factor_long_quantile": 0.9,
    "factor_short_quantile": 0.1,
    "nq_change_enabled": true,
    "nq_long_quantile": 0.5,
    "nq_short_quantile": 0.5
  },
  "execution": {
    "factor_exit_quantile_long": 0.3,
    "factor_exit_quantile_short": 0.7
  }
}
```

信号通过 `CTA_MANAGER_API_URL=http://127.0.0.1:4191/manager/api` 的仓位策略发布 API 更新 targets，不再直接写 Exec Redis 文档。`CTA_POSITION_STRATEGY_PREFIX=lgq_cta_`；Manager 持有订单参数、虚拟份数和跟随关系，再写 `<exec-env>:binance-futures:batch_exec:<strategy>` 与 `strategy_names`。同一 targets 不重复发布；失败在下一次刷新重试，并在首页显示 publication_error。移除的因子/币仍发送零目标以清掉原策略。五份策略共用执行模板 `lgq_cta_ctaker`，算法 `ctaker`；`targets` 例如：

```json
{"BNBUSDT":{"qty":0.3,"signal":0},"FILUSDT":{"qty":0,"signal":0}}
```

Exec 的通用 `qty: "clear"` 约定仍保留。当前 Manager 只发布数值 qty，ctaker 对所有目标使用零 USDT 容差，因此退出目标 0 会按该策略实际已执行 qty 平仓；交易所量步长与最小量约束保留。ctaker 每次按完整仓位差直接提交 MARKET，不使用 maker，也不按 single_order_usdt 拆批。不同因子的执行账本和 PnL 保留，标准 Exec 仍负责账户级净执行。`+max_u_1000` 等字符串不属于执行合约。

标准 Exec 运行目录为 `/home/ubuntu/binance_exec_trade05`，使用 RX02 RapidX 账户，IPC_NAMESPACE、Redis 前缀和持久化目录均独立。Manager 每 60 秒刷新 RapidX 合约规则，签名私有请求绑定该账户的 `.228` trading IP，不能使用 `.93` K 线公网缓存出口；Exec 只热加载完整缓存。RapidX 账户不调用 Binance 原生 Multi-Assets/Portfolio Margin 模式检查；真实账户风险仍由 RapidX account_monitor 与 Exec 检查。首次启动前用环境本地 `scripts/sync_exec_risk_params.py` 初始化标准挂单/频率风控表。

配置页把 Strategy Params 合并到 CTA 信号，统一显示开多、开空、退多、退空四个分位和 NQ；删除 trade_sides、cooldown_seconds、signal_delay_seconds 与 factor_exit_enabled，方向始终允许多空，退出分位始终启用。Risk Params 和本地 Exec 参数编辑入口及对应 risk API 已移除，执行配置链接指向 `/manager/`。满仓金额直接在 CTA 信号设置一份，只负责 qty 换算；保存全部遇到校验失败会停止后续保存。现有 Exec 风控 Hash 和 Exec 算法没有修改。

### 图表和历史模拟

| 环境 | 配置页 | 信号与 PnL 页 | 服务端口 |
| --- | --- | --- | --- |
| rx02 | `/cta/binance-cta-special-rx02/config/` | `/cta/binance-cta-special-rx02/signals/` | config 19182 / dashboard 10192 |
| rx03（停止） | `/cta/binance-cta-special-rx03/config/` | `/cta/binance-cta-special-rx03/signals/` | 原 config 19185 / dashboard 10193，无监听 |
| trade05（RX02） | `/exec_trade05/config/` | `/exec_trade05/`（实际 Exec 仓位） | config 18165 / Viz 10045 |

页面位于部署机，既有 Nginx `http://13.115.227.29:4191` 代理保持原地址。RX02 首页重做为 32 项 factor/symbol 当前生成仓位表，显示方向、qty、当前 USDT、入场金额、单份 USDT、分位和 NQ；另有每币仓位与因子汇总。每币全部组合开仓金额为有效因子数乘以单份 USDT，页面同时显示有效组合总数。这是信号目标，真实成交仓位及 PnL 由 Manager/Exec 查看。信号页仍只看单因子配对 B/S，保留 NQ；图表脚本和历史接口支持 gzip。

K 线只标记模拟仓位实际变化，持仓期间重复的同向信号不生成 B/S。绿色 B 表示买入、红色 S 表示卖出，三角为开仓、圆点为平仓，同笔交易以虚线连接：多仓 B 开 → S 平，空仓 S 开 → B 平。交易账本一行对应一笔持仓，可点击账本或图中点查看开平快照、原 qty 和 PnL。尚未平仓的交易只保留开仓点，单独显示浮动 PnL，不虚构配对平仓点。不同交易之间可能连续出现相同字母，例如平多 S 后开空 S。

下方显示满多/满空/空仓阶梯与 NQ 分位曲线；NQ 仍只过滤开仓，快照使用当时冻结的阈值。两环境 NQ 多空均为 q50，rolling 开多 q90、开空 q10 保持不变；各自原有退出分位也保留（rx02 退多 q30 / 退空 q70，rx03 退多 q70 / 退空 q30）。

两环境回放参数为 `config/cta_special_replay.json`，从 2026-10-10 00:00 UTC 空仓开始；使用 publisher 落盘的因子/NQ 闭合历史和信号后首个有效 bid/ask，按无额外滑点 taker 模拟。回放使用当前 `config/cta_special.json` 中的单份 USDT，每因子每币独立使用，手续费设置为零时明确显示毛收益、毛胜率。没有订单队列、网格或移动止损，同向不加仓，退出平原 qty。仅已平交易计入胜率，未平单计入未实现 PnL。不同因子的相反仓位仍各自存在，不用 symbol 净仓决定退出。

rx03 只读共享 rx02 的 `data/cta_special/factor_history`，其 SQLite、实际发布记录和模拟结果独立。新版信号页展示历史模拟，使用当前配置重算；实际发布审计和 API 保留当时快照，不能把配置重算结果称为真实成交 PnL。

### 构建、发布与启动

本地 `/home/fanghaizhou/mkt_signal` 的 `arbmm` 同步 `origin/arbmm` 后构建 release，上传临时文件、校验 SHA-256，再原子安装。生产机不执行 Cargo。现有 env.sh、trade_engine.toml、publisher 和 CPU/source-IP 配置保留。

```bash
cargo fmt --check
cargo test --lib
cargo build --release --bin cta_special_signal --bin exec-pre-trade
python3 -m unittest scripts.tests.test_cta_special_servers scripts.tests.test_cta_special_position_replay scripts.tests.test_cta_special_signal_history scripts.tests.test_exec_config_server
node scripts/tests/test_cta_special_panel.js
```

`scripts/start_cta_special.sh --execute` 只启动 publisher、signal/config/dashboard；enabled=true 要求 Manager API 地址。标准 Exec 独立使用 `scripts/start-exec.sh --host jp-meta-elvpn --env-name binance_exec_trade05 --venue binance-futures`，内部调用环境本地脚本。旧 companion 准备脚本不属于当前启动路径。停盘 RX02 实盘必须单独停止 trade05；只停信号不会清仓。不要使用 raw pmdaemon/PM2 命令。

本次产物和前端证据为 `artifacts/lgq_cta_release_manifest.json`、`artifacts/lgq_cta_browser_validation.json`；既往观察版记录见下方。

2026-10-11 05:46 UTC trade05 修复发布：待下单数量按数量步长、minQty 和 minNotional 过滤，品种汇总采用执行端待下单量；不可执行余量显示“低于下单门槛”，真实仓位差额继续显示。修复 f32 账户数量造成的整手平仓尾差，仅在舍入后的策略账本总量等于真实账户仓位时恢复合法手数；AAVE 尾仓已清零。Manager 从专属 RapidX 账户 IPC 读取实际权益，公网页面及 WebSocket 更新验证通过。mkt_signal 1,241 项单元测试、Manager 284 项单元测试和 8 项集成测试通过。产物与线上证据：`artifacts/trade05_pending_equity_release_manifest.json`、`artifacts/trade05_pending_equity_live_validation.json`、`artifacts/trade05_pending_equity_live_browser.json`。

2026-10-11 03:00 UTC 上线验证：RX02 信号在线、Manager 发布无错误，trade05 跟随 `virtual02/lgq_cta`，五份因子策略各 1 份、multiplier=1，配置发布队列为空；16 币 32 项目标的每币入场金额不超过 500 USDT。RapidX 仓位快照已补齐现有 `SNAPSHOT_COMPLETE` 标记，非空零仓位快照也能完成 Exec 账本初始化，解析失败不会发完成标记。核验 5 笔新订单均为 exchangeOrderType=MARKET、orderState=FILLED，涉及 AAVE/DOGE/XRP；执行与账户流水持久化待确认数量均为零。两个旧 companion 和 RX03 无执行进程，既有其他策略 PID 保留。测试通过：mkt_signal 1,238 项、trade_engine 356 项、Manager 283 项单元测试，以及 86 项 Python、图表与实机浏览器验证。在线证据为 `artifacts/lgq_cta_live_validation.json`、`artifacts/lgq_cta_fill_validation.json`、`artifacts/lgq_cta_manager_browser_validation.json`。

2026-10-10 17:03 UTC 单份满仓金额发布：`max_pos_u` 统一放在 CTA 信号配置中，所有独立因子和币种使用同一金额，保留现有 1000 USDT。旧逐因子额度 Redis 表和接口移除，原始配置备份在各环境 `run/global_amount_backup_20261010T165827Z/`。本地 release 构建并校验 SHA-256 后原子安装，只重启 signal/config/dashboard（rx02 PID 2582314/2581462/2585918，rx03 PID 2587152/2587076/2587842）。1,237 项 Rust 单元测试通过（2 项忽略），46 项后端测试、配置与图表浏览器验证通过。相同历史输入的交易、qty、PnL 和胜率完全一致；原四个分位、NQ、币种因子绑定和 Manager Exec 文档保留，交易仍关闭。证据为 `artifacts/cta_special_global_amount_release_manifest.json`、`artifacts/cta_special_global_amount_live_validation.json`、`artifacts/cta_special_global_amount_live_browser.json`、`artifacts/cta_special_global_amount_chart_live_browser.json`。

2026-10-10 16:39 UTC 配置简化发布：本地 release 构建后校验 SHA-256，原子替换 signal 与三个 Python 文件、配置前端；两个环境保留原四个分位和 NQ，将 Redis 因子额度整表中的 exec 字段移除。通过环境本地脚本只重启 signal/config/dashboard（rx02 PID 2561169/2560891/2561832，rx03 PID 2562970/2562876/2563089）。1,236 项 Rust 单元测试通过（2 项忽略）、44 项后端测试及本地配置浏览器验证通过。相同历史输入在清理前后产生完全一致的交易、qty、PnL 和胜率；既有 Exec 文档 SHA-256 保持原样，交易仍关闭。证据为 `artifacts/cta_special_simplified_release_manifest.json`、`artifacts/cta_special_simplified_live_validation.json`、`artifacts/cta_special_simplified_config_browser.json`；上线浏览器检查为 `artifacts/cta_special_simplified_live_browser.json`。

2026-10-10 14:51 UTC 单因子交易面板发布：只更新 dashboard、position replay 和信号页两个前端文件，经 SHA-256 校验后原子安装，仅通过环境本地脚本重启 dashboard（rx02 PID 2456915、rx03 PID 2457787）。共享 publisher、signal、config 和执行进程均未启动或重启，交易配置原样保留 enabled=false。42 项后端测试及图表测试通过；线上逐项检查 32/52 项因子绑定，所有已平交易均有唯一开平动作且 qty 一致，开仓符合原 NQ 阈值。Nginx 地址下的图表点击、交易快照、因子切换及桌面/手机布局通过，无 JavaScript 错误。证据为 `artifacts/cta_special_factor_pairs_release_manifest.json`、`artifacts/cta_special_factor_pairs_live_validation.json`、`artifacts/cta_special_factor_pairs_live_browser.json`。

2026-10-10 10:30 UTC 部署验证：rx02 32 项、rx03 52 项分位/NQ 与历史输入全部逐项一致；真实持仓和挂单为零，交易关闭以来没有新实际发布。两个环境均为 5 个独立策略，所有可执行 targets 都是 clear。配置页动态修改额度和 Exec maker timeout 后，标准 Exec 文档订单参数自动更新；测试修改已恢复为原值，targets 始终 clear。浏览器验证因子筛选、独立 PnL、B/S/全平点和 JSON 保存，桌面/手机无页面溢出，无 JavaScript 错误。

2026-10-11 07:07 UTC trade05 完成状态修复：低于数量步长、minQty/minNotional 的余量统一显示“执行完成”，不再单列等待状态。修正执行器在仍有活动批次时因最小下单余量设置无限等待、最终成交后无法再次判断完成的问题；只有批次全部结束才停用新批次调度。空报单计划释放时也应用同一规则。覆盖 ctaker、Batch、POV 的成交回归测试，1,243 项单元测试通过；线上 8 项卡住记录已结束，全部 15 项后端记录完成。发布及验证：`artifacts/trade05_completion_release_manifest.json`、`artifacts/trade05_completion_live_validation.json`、`artifacts/trade05_completion_browser.json`。

2026-10-12 06:05 UTC 单份 USDT 发布：代码 `47becad0`，RX02 的 `max_pos_u` 从每币 500 USDT 平分改为每个有效 `symbol × factor` 组合独立 200 USDT，现有 16 币、5 因子、32 项绑定保留。同步 signal、回放、配置服务与三个前端文件；本地 release 经 SHA-256 校验后原子安装，通过环境脚本重启 signal（PID 140816）、config（147320）、dashboard（152544）。停稳后备份并保留原仓位状态；共享 publisher 与 trade05 六个执行进程 PID 均保留。线上 `enabled=true`、`allocation=per_symbol_factor`，32 项额度和 Manager/Exec 目标逐项匹配，发布无错误；公网页面确认 LINK/LTC/ETH 全部组合开仓金额分别为 800/600/200 USDT，手机页面无溢出、无 JavaScript 错误。1,243 项 Rust 库测试通过（2 项忽略），49 项 Python 检查与图表检查通过。产物、安装备份和核验记录见 `artifacts/cta_special_per_pair_200_release/{manifest,install_result,validation,browser_validation}.json`；远端备份为 `run/deployment_backups/per_pair_200_20261012T060014Z/`。
