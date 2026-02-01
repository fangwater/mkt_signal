现在处理第二个问题。这里非常总要，是做市模式的核心区别。对于pre-trade，处理某个mm open signal的时候，需要先判断这个signal
  是否存在。如果存在，需要创建一个mm_hedge_strategy

这是mm模式的绝对核心。
即只要一个symbol，存在mm的open strategy，就必须存在mm hedge strategy。
mm hedge strategy的最大区别是，每个symbol，只要一个hedge strataty即可。无论有多少open。



MarketMakerhedgestargey目前首先功能是
1、维护买卖方向的，当前累计的qty
每个MarketMakerOpenStrategy在ffilled 活着cancel后，目前是打印info日志。现在修改。
把自己的买、卖方向的，cum的成交量，累加到symbol的MarketMakerhedgestargey
buy记为正，sell计为负，MarketMakerhedgestargey记录程序开始运行后，程序的累积正负。

2、增加一个 期间累积成交，即不钆差，累积b/s方向各成交了多少

首先实现这个功能。
2、当收到mm hedge signal的时候，打印所有当前的MarketMakerhedgestargey 累积头寸的正负（即多空累积成交了多少），然后，strategy需要一个关键的功能。仿照目前的query，需要新增一个query msg。
msg的内容包括symbol， 和期间累积成交， b/s方向各成交了多少量。仿照目前pre-trade的query hedge msg。

query消息会返回一个distrute。即一个挂单范围。





