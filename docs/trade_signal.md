收到信号后，需要构造或者映射到一个Strategy，即这笔交易归属的策略
策略包含整个交易过程的交易订单和动作，比如买入，卖出，平仓，止损等，可能包含多个订单，可能立刻执行，也可能需要驻留在内存中

以目前的信号为例，币安fr策略有3种信号
BinSingleForwardArbOpen
BinSingleForwardArbHedge
BinSingleForwardArbClose

本质上对应的是同一个策略，即币安fr策略，open的时候创建，在Hedge和Close的时候需要映射

pub trait Strategy {
    fn get_id(&self) -> i32;
    fn handle_trade_signal(&mut self, signal_raws : &Bytes);
    fn handle_trade_response(&mut self, response_raws : &Bytes);
    fn handle_account_event(&mut self, account_event_msg_raws : &Bytes);
    fn hanle_period_clock(&mut self, current_tp: i64);
    fn is_active(&self) -> bool;
}

以币安正向套利为例子，构造BinSingleForwardArbStrategy，满足上述trait
目前只支持单个订单，然后基于这个拓展

1、关闭策略
BinSingleForwardArbStrategy的关闭存在两种情况
[1]订单超过时间限制, 触发超时，需要撤掉订单。如果此时没有任何订单，则关闭策略。
[2]平仓成功。此时没有任何订单等待处理，则关闭策略。

因此is_active(&self) -> bool;的实现对于BinSingleForwardArbStrategy，就是判断当前
Strategy是否还有在维护的订单，如果没有，就从manager中删除。

2、定时检查当前策略
类似于BinSingleForwardArbOpen这样的交易信号，会开启一个交易策略。这个Strategy会被
pretrade的Strategy manager维护。

策略在两种时候被检查，其中一种基于定时事件。对于BinSingleForwardArbStrategy，有一个需求是，如果杠杆下单的市价单在一定的时间内未能成交，则撤销并关闭当前Strategy。

因此preTrade会维护一个deqeue of Strategy id
然后tokio select基于定时器，进行检查。调用hanle_period_clock。
主要行为包括
1、淘汰内部超过时间限制，且status处于待成交的订单。
2、触发某些需要延迟执行的订单，例如exec_delay != 0
3、清理提交执行后，长时间无回报的单子。

完成后，如果active判定失败，就删除策略。

3、基于event处理Strategy
Strategy在创建后，需要处理3种event
[1]和自身相关联的信号
[2]和自身关联的trade_response
[3]和自身关联的account_event(成交回报，头寸风控相关等)

当preTrade收到trade engine的响应事件时，会提取事件的order id和
Strategy，然后调用handle_trade_response进行处理。

对于BinSingleForwardArbStrategy，主要进行以下几个处理。
和自身相关联的信号 
[1]BinSingleForwardArbHedge
收到后，创建订单的对冲单，执行下单操作，并且更新订单状态和时间。
对于这个策略，Hedge就是创建一个和杠杆下单方向相反，头寸相同的um合约订单做空。
且无需进行任何风控检查。

[2]BinSingleForwardArbClose
收到后，填充下单相关数据，同时对现货、合约进行平仓操作。对于这个策略，
就是创建


TradeResponse
[1] 处理杠杆下单的成功响应，更新订单状态。（TradeResponse）
[2] 处理杠杆下单市价单成功响应，更新订单状态(杠杆下单闭环)(AccountEvent)，创建BinSingleForwardArbHedge信号，并入队
[3] 

以及处理失败情况
[1] 现货杠杆下单失败 （TradeResponse）

4、创建策略实例
BinSingleForwardArbOpen 开仓信号
[1] 策略发起不需要风控，创建order才需要，风控在order层面而不在策略层面
[2] 创建会立刻创建order，但是order需要根据check来获得。
[3] 创建order失败，会被定时器淘汰掉。
[4] 创建订单后，会开启status和执行延迟，如果执行延迟为0的订单会被立刻派发到交易引擎。
[5] 如果有执行延迟，则需要等待足够数量的period后被执行。

订单创建
1、订单不允许直接创建，创建之前有一系列的风控检查。
2、不同的风控规则生效的阶段不同
3、基于头寸的风控不保证绝对实时性






