use crate::pre_trade::order_manager::Side;
use crate::signal::common::TradingVenue;

/// 成交更新trait - 提供成交信息的通用接口
///
/// ## 使用示例
///
/// ```ignore
/// use crate::strategy::trade_update::TradeUpdate;
/// use crate::strategy::binance_adapter::BinanceOrderUpdateAdapter;
///
/// // 当订单有成交时
/// fn handle_trade(msg: &OrderTradeUpdateMsg) {
///     let adapter = BinanceOrderUpdateAdapter::new(msg);
///
///     // 检查是否有成交
///     if adapter.trade_id() > 0 {
///         println!("成交ID: {}", adapter.trade_id());
///         println!("成交价格: {}", adapter.price());
///         println!("成交数量: {}", adapter.quantity());
///         println!("手续费: {} {}", adapter.commission(), adapter.commission_asset());
///         println!("是否Maker: {}", adapter.is_maker());
///         println!("成交金额: {}", adapter.notional());
///     }
/// }
/// ```
pub trait TradeUpdate {
    /// 获取事件时间 (微秒时间戳)
    fn event_time(&self) -> i64;

    /// 获取成交时间 (微秒时间戳)
    fn trade_time(&self) -> i64;

    /// 获取交易对符号
    fn symbol(&self) -> &str;

    /// 获取成交ID (0表示非成交更新)
    fn trade_id(&self) -> i64;

    /// 获取订单ID
    fn order_id(&self) -> i64;

    /// 获取客户端订单ID
    fn client_order_id(&self) -> i64;

    /// 获取成交方向 (Buy/Sell)
    fn side(&self) -> Side;

    /// 获取成交价格
    fn price(&self) -> f64;

    /// 获取成交数量
    fn quantity(&self) -> f64;

    /// 获取手续费金额
    fn commission(&self) -> f64;

    /// 获取手续费资产
    fn commission_asset(&self) -> &str;

    /// 是否为做市商成交 (true=maker, false=taker)
    fn is_maker(&self) -> bool;

    /// 获取已实现盈亏 (期货特有，现货返回0.0)
    fn realized_pnl(&self) -> f64 {
        0.0
    }

    /// 获取交易标的类型（交易所和市场类型）
    fn trading_venue(&self) -> TradingVenue;

    /// 辅助方法：计算成交金额 (价格 × 数量)
    fn notional(&self) -> f64 {
        self.price() * self.quantity()
    }

    fn cumulative_filled_quantity(&self) -> f64;

    /// 辅助方法：检查是否为有效成交 (trade_id > 0)
    fn is_valid_trade(&self) -> bool {
        self.trade_id() > 0
    }

    
}
