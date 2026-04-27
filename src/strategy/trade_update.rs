use crate::pre_trade::order_manager::Side;
use crate::signal::common::{OrderStatus, TradingVenue};

/// 成交更新trait - 提供成交信息的通用接口
///
/// ## 使用示例
///
/// ```ignore
/// use crate::strategy::trade_update::TradeUpdate;
/// use crate::strategy::binance_adapter::BinanceOrderUpdateAdapter;
///
/// // 当订单有成交时
/// fn handle_trade(msg: &BinanceBasicOrderMsg) {
///     let adapter = BinanceOrderUpdateAdapter::new(msg);
///
///     println!("订单ID: {}", adapter.order_id());
///     println!("成交价格: {}", adapter.price());
///     println!("是否Maker: {}", adapter.is_maker());
/// }
/// ```
pub trait TradeUpdate {
    /// 获取事件时间 (微秒时间戳)
    fn event_time(&self) -> i64;

    /// 获取成交时间 (微秒时间戳)
    fn trade_time(&self) -> i64;

    /// 获取交易对符号
    fn symbol(&self) -> &str;

    /// 获取订单ID
    fn order_id(&self) -> i64;

    /// 获取客户端订单ID
    fn client_order_id(&self) -> i64;

    /// 获取成交方向 (Buy/Sell)
    fn side(&self) -> Side;

    /// 获取成交价格
    fn price(&self) -> f64;

    /// 是否为做市商成交 (true=maker, false=taker)
    fn is_maker(&self) -> bool;

    /// 获取交易标的类型（交易所和市场类型）
    fn trading_venue(&self) -> TradingVenue;

    fn cumulative_filled_quantity(&self) -> f64;

    // 成交单状态，即对应的额order_status
    fn order_status(&self) -> Option<OrderStatus>;

    fn debug_summary(&self) -> String {
        format!(
            "kind=trade_update event_time={} trade_time={} venue={:?} symbol={} client_order_id={} order_id={} side={:?} price={:.8} cum_qty={:.8} is_maker={} order_status={:?}",
            self.event_time(),
            self.trade_time(),
            self.trading_venue(),
            self.symbol(),
            self.client_order_id(),
            self.order_id(),
            self.side(),
            self.price(),
            self.cumulative_filled_quantity(),
            self.is_maker(),
            self.order_status()
        )
    }
}
