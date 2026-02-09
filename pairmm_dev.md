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

## 单所做市回测

## 支持现货做市交易

## 支持期货现货套利交易
