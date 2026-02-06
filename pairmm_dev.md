# roadmap
## 订单持久化
### 删除旧记录格式
- [1] 删除原先的signal记录、trade update记录、order update记录，删除不需要的http接口导出，persist_manager只关注纯二进制的记录方式
- [2] trade_update, order_update当且仅当strategy匹配失败的情况下，类似于无法，match到当前的订单，出现设计失误，作为补充记录和问题排查
- [3] 不再记录盘口，记录spread_factor即可。
- [4] 删除订单的被动导出功能，主动按照小时直接盘中导出并rsync。

### 订单记录架构调增
- [1] 不再使用signal + update匹配的方式，而是运行时直接产出订单记录
- [2] 为了signal通用化，记录盘口和基本的时间信息，直接合并到strategy
- [3] 盘中直接合成标准格式，基于回测订单框架的订单基础信息 + from key，from key的格式根据策略不同，适配不同的策略执行。

### 订单信息来源
- [1] 每一个trade update和order update自动折算成标准的订单格式
- [2] 交易所推送的订单变更时间，和binance合约接口的ws reponse事件，query查询事件，都会折算为trade update和order update。
- [3] 折算规则:




## 做市信号开发

## 单所做市回测

## 支持现货做市交易

## 支持期货现货套利交易