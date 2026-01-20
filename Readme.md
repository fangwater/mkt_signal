xarb v2
1、依然维护okex开，binance平的交易策略。

2、问题在于，okex开，binance平，现在交易决策发生在awsjp

价差触发交易决策--->awsjp触发交易指令--->HK下单
这种架构导致，判断价差出现到交易的gap太大。
主要吃开仓侧价差，则开仓侧下单。平仓侧目前不依赖价差，因此pre-trade要移动到hk来部署。

3、架构调整
(1) pre-trade要移动。即哪边报单吃spread，要移动到哪边。
需要汇总pre-trade涉及的所有http请求，分快速请求和慢速请求。
快速请求通过engine实现，慢速请求用nginx代理，使用一个jp的ip

(2) 行情进程不需要移动。直接在jp订阅，然后就是okex快，binance慢。
这一步暂时不用专线处理。之后对比，目前认为不会是主要的延迟来源。

(3) trade signal移动到hk。本地也需要配置一个redis和nginx

(4) account monitor，测试hk是否能通ws，印象中可以。listen key的续期可以走nginx即可。
account monitor直接也都迁移到okex来

(5) trade engine执行
现有盘子只需要做了nginx代理就可以移动到binance。

(6) 订单记录
移动到hk

总结
1、开仓测只需移动到hk就可以看到效果
2、平仓侧看下单是如何指导的。按8照之前的情况平仓侧关心盘口，需要进行特殊处理。


问题
是否需要出两版还是一次到位
1、xarb-v2 nginx代理部分http，整体迁移到hk
2、xarb-v3

平仓的优化
1、盘口挂单(快40ms的binance盘口)(Q:平仓是否要要考虑okex行情？还是只考虑binance的行情？正常我理解只考虑币安)
2、币安的挂单改为ws执行
