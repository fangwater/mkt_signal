# roadmap
## 实现跨所套利算法平仓
- [1]dump symbol设置的symbol会进入算法平仓状态
- [2]处于算法平仓的symbol，不可以出现在正开、反开的list中，如果存在则同步阶段就会失败
- [3]dump symbol只关注价差率，满足即开仓，把当前仓位调整为0为止
- [4]对于okex、binance的合约(统一账户合约)，binance标准账户的合约，算法平仓会增加reduce-only标志，避免风控原因无法挂单。
- [5]所有算法平仓单，fromkey标志为ts:dump，表示非正常交易信号，而是强制调仓/平仓信号。
todo:
1、okex的非跨币种保证金模式，也增加相同功能。(普通账户模式)
2、币安现货的ws交易模式(非margin交易支持reduce-only)强平。（针对-期/现 套利算法）
3、其余交易所也应该支持强平算法

## 订单持久化
### 删除旧记录格式
- [1] 删除原先的signal记录、trade update记录、order update记录，删除不需要的http接口导出，persist_manager只关注纯二进制的记录方式
- [2] trade_update, order_update当且仅当strategy匹配失败的情况下，类似于无法，match到当前的订单，出现设计失误，作为补充记录和问题排查
- [3] 不再记录盘口，记录spread_factor等因子作为替代。
- [4] 删除订单的被动导出功能，主动按照小时进行主动导出。

### 订单记录架构调增
- [1] 不再使用signal + update匹配的方式，而是运行时直接产出订单记录
- [2] 为了signal通用化，记录盘口和基本的时间信息，直接合并到strategy
- [3] 盘中直接合成标准格式，基于回测订单框架的订单基础信息 + from key，from key的格式根据策略不同，适配不同的策略执行。

### 订单信息来源
- [1] 每一个trade update和order update自动折算成标准的订单格式
- [2] 交易所推送的订单变更时间，和binance合约接口的ws reponse事件，query查询事件，都会折算为trade update和order update。
- [3] 标准订单格式包括

| 字段 | 含义注释 |
| --- | --- |
| `symbol` | 交易标的，例如 `BTCUSDT`。 |
| `create_ts` | 订单创建时间戳（订单首次生成时间）。 |
| `update_ts` | 订单最近一次状态变更时间戳。 |
| `signal_ts` | 触发该订单的信号时间戳。 |
| `client_order_id` | 客户端自定义订单 ID，用于幂等和追踪。 |
| `venue` | 订单所属交易所/交易场所|
| `ttype` | 订单类型（如限价/市价等策略侧定义类型）。 |
| `side` | 买卖方向（`buy` 或 `sell`）。 |
| `price` | 下单价格。 |
| `price_offset` | 相对参考价的偏移量（用于策略定价修正）。 |
| `amount_init` | 初始下单数量。 |
| `amount_update` | 本次成交量 |
| `status` | 订单状态（如新建、部分成交、完全成交、撤单等）。 |

- [4] 订单信息补充 `from_key`（按规则场景拆分，`from_key` 与 `signal` 非一一对应）
  - [from_key 规则索引](docs/from_key_rules/README.md)
  - [ArbOpen（FR）](docs/from_key_rules/arb_open_fr.md)
  - [ArbOpen（XARB）](docs/from_key_rules/arb_open_xarb.md)
  - [ArbClose（FR）](docs/from_key_rules/arb_close_fr.md)
  - [ArbClose（XARB）](docs/from_key_rules/arb_close_xarb.md)
  - [ArbHedge（FR）](docs/from_key_rules/arb_hedge_fr.md)
  - [ArbHedge（XARB）](docs/from_key_rules/arb_hedge_xarb.md)
  - [MMOpen](docs/from_key_rules/mm_open.md)
  - [MMHedge](docs/from_key_rules/mm_hedge.md)
  

## 做市信号开发

### MM Hedge Query 重构（阶段 1：rl_return_volatility 中心范围）

#### 背景
- 现状：`MMHedge` query 回复使用固定 `open_price_offsets` 直接生成对冲价格层。
- 问题：不同波动阶段下，固定偏移无法反映当前市场“应挂在多远”的动态变化。
- 目标：参考 `xarb_decision` 中对 `rl_return_volatility` 的使用方式，在 MM 场景引入“中心偏移范围”。

#### 输入
- Query 输入（当前）：
  - `symbol`
  - `period_buy_qty`
  - `period_sell_qty`
  - `net_qty`
- 因子输入：`rl_return_volatility`（按 `hedge venue + symbol` 查询）
- 策略参数（新增）：
  - `mm_strategy_params_{venue}.hedge_vol_upper_scale`
  - `mm_strategy_params_{venue}.hedge_vol_lower_scale`

#### 计算定义
- 记 `rl_return_volatility` 最终值为 `x`（要求 `x > 0` 且可用）。
- 定义上/下界偏移：
  - `upper = x * (1 + hedge_vol_upper_scale)`
  - `lower = x * (1 + hedge_vol_lower_scale)`
- 对冲中心范围定义为：
  - `range = [min(lower, upper), max(lower, upper)]`
  - 约束：`range` 下界需要 `>= 0`（负值按 0 截断）。

#### 与 MM 挂单层的衔接（阶段 1 约定）
- 仍复用现有档位模板 `open_price_offsets` 的“层数与形状”。
- 将模板偏移映射到上述 `range`，得到 query 回复使用的实际对冲偏移序列。
- 即：
  - 模板负责“多少档 + 相对稀疏度”；
  - `rl_return_volatility + scale` 负责“整体中心位置与区间宽度”。

#### Fallback 规则
- 若因子缺失/未 ready/非法（非有限值、`<=0`），则回退到旧逻辑：
  - 直接使用模板偏移（`open_price_offsets`）生成对冲价格层。
- 若 `hedge_vol_upper_scale/hedge_vol_lower_scale` 缺失或解析失败：
  - 视为配置错误，启动阶段直接失败（fail fast）。

#### 日志与可观测性
- 每次 query 处理需要记录：
  - 因子 key、因子值、ready、ts、factor_index
  - `hedge_vol_upper_scale/hedge_vol_lower_scale`
  - `center/range` 及最终偏移序列来源（`factor_range` 或 `template_fallback`）

#### 示例
- 输入：`x = 0.0012, hedge_vol_upper_scale = 0.25, hedge_vol_lower_scale = -0.20`
- 计算：
  - `upper = 0.0012 * 1.25 = 0.0015`
  - `lower = 0.0012 * 0.80 = 0.00096`
- 对冲中心范围：`[0.00096, 0.00150]`

#### 后续阶段（未在本阶段实现）
- 在 query 中补充 request 元信息（如 `request_seq/query_ts`）以支持更强的幂等与回放诊断。
- 在真实（非 mock）MM 信号层复用同一 `mm_hedge_decision` 逻辑，避免双实现漂移。

## 单所做市回测

## 支持现货做市交易

## 支持期货现货套利交易
