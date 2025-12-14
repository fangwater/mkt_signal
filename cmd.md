 python scripts/okx_margin_sell.py --inst-id SOL-USDT --sz 0.01 --ord-type market --execute
 python scripts/okx_margin_sell.py --inst-id SOL-USDT --sz 0.01 --ord-type market --buy --execute
 python scripts/okx_balance_and_positions_ws.py --inst-type MARGIN --duration 5
 python scripts/okx_post_only_reject_test.py --inst-id SOL-USDT-SWAP --px 200 --notional-usdt 1000 --execute

判断response是来自开仓侧，还是平仓侧
开仓侧，判断代码(增加一个借币失败的判断情况)

事件驱动
对于订单而言，trade请求只有撤单、报单两种
一、开仓侧报单
直接关闭strategy

二、开仓侧撤单失败
订单撤单失败的可能原因
1、完全成交，但是漏掉了订单的成交推送，此时订单已经不存在，无法撤单。
2、开仓侧已经cancel过了，但丢失了cancel的回报。因此无法推动后续的对冲执行。
3、订单挂单就失败了，这种情况下本地可以直接orderid不存在，无需处理。
4、rest api因为网络、频率限制等原因出错。

区分处理
1，2 => 使用rest api查询订单状态。判断订单状态。
(1)查询到trade，增加一个trade update。补充成交信息。然后触发对冲。
(2)查询到cancel，增加一个order update。补充cancel信息，然后触发对冲。
2 => 直接关闭strategy
3 => 重新撤单


是否需要timeout处理
100ms目前mgr会定时轮询一遍所有的strategy，如果处于开仓状态，会检查当前挂单时间，超出会触发撤单事件。因此会不断触发撤单，除非resp持续miss，相当于数据链路断裂，否则始终能收到eng的response。
因此不需要定时处理。


对冲侧开仓失败，分原因处理
(1)如果是post only挂单失败，则重新挂单
(2)如果其他错误也继续挂单，打印error log（限频等，之后再处理 频率限制有cooldwon的设计等，不要反复接受trade request）

对冲侧cancel失败
1、完全成交，但是漏掉了订单的成交推送，此时订单已经不存在，无法撤单。
2、对冲侧已经cancel过了，但丢失了cancel的回报。因此无法推动后续的对冲执行。
3、订单挂单就失败了，这种情况下本地可以直接orderid不存在，无需处理。
4、rest api因为网络、频率限制等原因出错。

区分处理
1，2 => 使用rest api查询订单状态。判断订单状态。
(1)查询到ffilled，增加一个trade update，更新对冲量。关闭strategy。
(2)查询到cancel，增加一个order update，更新对冲量，继续对冲。
3，4 => query盘口，重新触发对冲。

是否需要timeout处理
100ms目前mgr会定时轮询一遍所有的strategy。判断订单是否超时。当cancel失败，则一直处于挂单状态，会持续触发cancel。
