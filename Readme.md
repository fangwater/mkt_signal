pairmm

新增账户模式，根据账户类型判断下单方式，新账户skill部署等功能

新增taker单止损机制
1、目的，现在的xarb跨所模式的decision下，会处理来自pre-trade发送的hedge query
2、query后根据redis的因子值，判断范围挂单的price offset
3、然后发送的hedge signal，订单的挂单方式为limit单

想要新增一个止损机制，处理极端情况，挂taker单，方式如下
1、修改当前的strategy，额外存储一下open signal的盘口信息(open和hedge的leg)
2、修改hedge的query msg，附带上strategy的open盘口(即open信号的时间 + leg信息)

增加一个判定机制。
1、目前的代码中，有mid-price的计算方法。

参考目前的xarb xarb_scripts/sync_xarb_strategy_params.py 和
在这个基础上加一个
max_hedge_price_pct_change:
默认值为5，修改脚本。

1、修改程序 trade signal初始化读取配置的时候，校验一下是这个参数是否存在，如果不存在就panic，且值需大于0，小于2、其他就是保持现在的动态生效逻辑。

3、修改trade signal处理hedge query msg的逻辑
增加一段优先执行的止损逻辑，根据query msg中提供的提供open signal时候的两侧盘口，计算一个open时候的midprice
此时获取当前hedge的两侧盘口，参考现在填充到hedge signal的逻辑，计算一个当前的midprice
计算两个midprice的差值abs / open时候的midprice，判断pct change
如果pct change 大于 max_hedge_price_pct_change 则最终挂taker单
pub maker_only: 写false，其他数据合理填充。
上述判断逻辑用一行日志打印，info级别

4、handle_arb_hedge_signal 检查一下，这样的情况下，可以用taker。如果发taker单要打印日志。
5

