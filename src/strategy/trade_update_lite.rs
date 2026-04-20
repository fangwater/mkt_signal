use crate::pre_trade::order_manager::Side;
use crate::signal::common::TradingVenue;

/// 轻量成交更新 trait。
///
/// 目前用于 Binance USD-M futures `TRADE_LITE` 事件。该事件只提供本次成交增量，
/// 不提供累计成交量，因此和 `TradeUpdate` 分离。
pub trait TradeUpdateLite {
    /// 获取事件时间 (微秒时间戳)
    fn event_time(&self) -> i64;

    /// 获取成交时间 (微秒时间戳)
    fn trade_time(&self) -> i64;

    /// 获取交易对符号
    fn symbol(&self) -> &str;

    /// 获取订单 ID
    fn order_id(&self) -> i64;

    /// 获取客户端订单 ID
    fn client_order_id(&self) -> i64;

    /// 获取成交 ID
    fn trade_id(&self) -> i64;

    /// 获取成交方向
    fn side(&self) -> Side;

    /// 获取本次成交价格
    fn price(&self) -> f64;

    /// 获取本次成交数量
    fn last_filled_quantity(&self) -> f64;

    /// 是否为 maker 成交
    fn is_maker(&self) -> bool;

    /// 获取交易 venue
    fn trading_venue(&self) -> TradingVenue;
}
