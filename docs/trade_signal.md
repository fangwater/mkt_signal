收到信号后，需要构造或者映射到一个Strategy，即这笔交易归属的策略
策略包含整个交易过程的交易订单和动作，比如买入，卖出，平仓，止损等，可能包含多个订单，可能立刻执行，也可能需要驻留在内存中

以目前的信号为例，币安fr策略有3种信号
BinSingleForwardArbOpen
BinSingleForwardArbHedge
BinSingleForwardArbClose

本质上对应的是同一个策略，即币安fr策略，open的时候创建，在Hedge和Close的时候需要映射

pub trait Strategy {
    fn get_id(&self) -> i32;
    fn handle_trade_response(&mut self, trade_msg_raws : &Bytes, success: bool);
    fn handle_account_event(&mut self, account_event_msg_raws : &Bytes);
    fn check_timeout(&mut self, current_tp: i64);
    fn get_orders(&self) -> &HashMap<i64, Order>;
    fn get_orders_mut(&mut self) -> &mut HashMap<i64, Order>;
    fn is_active(&self) -> bool;
}

以币安正向套利为例子，构造BinSingleForwardArbStrategy，满足上述trait
目前只支持单个订单，然后基于这个拓展

1、关闭策略
BinSingleForwardArbStrategy的关闭存在两种情况
[1]订单超过时间限制, 触发超时，需要撤掉订单。如果此时没有任何订单，则关闭策略。
[2]平仓成功。此时没有任何订单等待处理，则关闭策略。






