use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::common::{ExecutionType, OrderStatus, TimeInForce, TradingVenue};

pub trait OrderUpdate {
    /// 获取事件时间 (微秒时间戳)
    fn event_time(&self) -> i64;

    /// 获取交易对符号
    fn symbol(&self) -> &str;

    /// 获取订单ID
    fn order_id(&self) -> i64;

    /// 获取客户端订单ID
    fn client_order_id(&self) -> i64;

    /// 获取订单方向 (Buy/Sell)
    fn side(&self) -> Side;

    /// 获取订单类型 (Limit, Market, etc.)
    fn order_type(&self) -> OrderType;

    /// 获取有效期类型 (GTC, IOC, FOK, GTX)
    fn time_in_force(&self) -> TimeInForce;

    /// 获取订单价格
    fn price(&self) -> f64;

    /// 获取订单数量
    fn quantity(&self) -> f64;

    /// 订单末次成交量
    fn last_time_executed_qty(&self) -> f64;

    /// 获取累计成交数量
    fn cumulative_filled_quantity(&self) -> f64;

    /// 获取订单状态
    fn status(&self) -> OrderStatus;

    /// 获取原始订单状态字符串（保留交易所自定义状态）
    fn raw_status(&self) -> &str {
        self.status().as_str()
    }

    /// 获取执行类型 (NEW, TRADE, CANCELED, etc.)
    fn execution_type(&self) -> ExecutionType;

    /// 获取原始执行类型字符串
    fn raw_execution_type(&self) -> &str {
        self.execution_type().as_str()
    }

    /// 获取交易标的类型（交易所和市场类型）
    fn trading_venue(&self) -> TradingVenue;

    /// 辅助方法：检查订单是否已完成（成交或取消）
    fn is_order_finished(&self) -> bool {
        self.status().is_finished()
    }

    /// 获取平均成交价（若适用）
    fn average_price(&self) -> Option<f64> {
        None
    }

    /// 获取最近一次成交价格（若适用）
    fn last_executed_price(&self) -> Option<f64> {
        None
    }

    /// 获取业务单元标识（UM/CM 等，若存在）
    fn business_unit(&self) -> Option<&str> {
        None
    }

    /// 获取字符串形式的 client order id（若存在）
    fn client_order_id_str(&self) -> Option<&str> {
        None
    }
}
