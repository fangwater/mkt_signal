use crate::common::basic_account_msg::TRADE_ID_LEN;
use crate::pre_trade::order_manager::Side;
use crate::signal::common::TradingVenue;

/// 轻量成交更新 trait。
///
/// 用于只提供本次成交增量、而不提供累计成交量的事件，因此和 `TradeUpdate` 分离。
pub trait TradeUpdateLite {
    /// 获取事件时间 (微秒时间戳)
    fn event_time(&self) -> i64;

    /// 获取成交时间 (微秒时间戳)
    fn trade_time(&self) -> i64;

    /// 获取交易对符号
    fn symbol(&self) -> &str;

    /// 获取客户端订单 ID
    fn client_order_id(&self) -> i64;

    /// 获取成交 ID（定长 36 字节，短于 36 的 id 右侧补 `\0`）
    fn trade_id(&self) -> &[u8; TRADE_ID_LEN];

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
