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
2、修改trade signal处理hedge query msg的逻辑