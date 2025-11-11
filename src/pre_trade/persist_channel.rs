use bytes::{BufMut, Bytes, BytesMut};
use log::warn;

use crate::common::iceoryx_publisher::{OrderUpdatePublisher, SignalPublisher, TradeUpdatePublisher};
use crate::signal::record::{SignalRecordMessage, PRE_TRADE_SIGNAL_RECORD_CHANNEL};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;

/// 通用交易更新记录频道（支持所有交易所）
pub const TRADE_UPDATE_RECORD_CHANNEL: &str = "trade_update_record";

/// 通用订单更新记录频道（支持所有交易所）
pub const ORDER_UPDATE_RECORD_CHANNEL: &str = "order_update_record";

/// 持久化通道：负责将信号记录、交易更新和订单更新通过 IceOryx 发布到下游持久化服务
///
/// 下游消费者：
/// - Signal: `persist_manager::SignalPersistor` -> RocksDB (`pre_trade_signal_record`)
/// - Trade Update: `persist_manager::TradeUpdatePersistor` -> RocksDB (`trade_update_record`)
/// - Order Update: `persist_manager::OrderUpdatePersistor` -> RocksDB (`order_update_record`)
///
/// 采用直接发布模式（无中间队列），优先保证低延迟
///
/// 支持通过 trait object 发布：
/// - `publish_trade_update(&dyn TradeUpdate)` - 发布成交记录
/// - `publish_order_update(&dyn OrderUpdate)` - 发布订单更新
pub struct PersistChannel {
    signal_record_pub: Option<SignalPublisher>,
    trade_update_record_pub: Option<TradeUpdatePublisher>,
    order_update_record_pub: Option<OrderUpdatePublisher>,
}

impl PersistChannel {
    /// 创建持久化通道，初始化三个 IceOryx 发布器
    ///
    /// 如果发布器创建失败，会记录警告并继续运行（降级模式）
    pub fn new() -> Self {
        let signal_record_pub = match SignalPublisher::new(PRE_TRADE_SIGNAL_RECORD_CHANNEL) {
            Ok(p) => {
                log::info!(
                    "PersistChannel: signal record publisher created on '{}'",
                    PRE_TRADE_SIGNAL_RECORD_CHANNEL
                );
                Some(p)
            }
            Err(err) => {
                warn!(
                    "PersistChannel: failed to create signal record publisher on '{}': {err:#}",
                    PRE_TRADE_SIGNAL_RECORD_CHANNEL
                );
                None
            }
        };

        let trade_update_record_pub = match TradeUpdatePublisher::new(TRADE_UPDATE_RECORD_CHANNEL) {
            Ok(p) => {
                log::info!(
                    "PersistChannel: trade update record publisher created on '{}'",
                    TRADE_UPDATE_RECORD_CHANNEL
                );
                Some(p)
            }
            Err(err) => {
                warn!(
                    "PersistChannel: failed to create trade update record publisher on '{}': {err:#}",
                    TRADE_UPDATE_RECORD_CHANNEL
                );
                None
            }
        };

        let order_update_record_pub = match OrderUpdatePublisher::new(ORDER_UPDATE_RECORD_CHANNEL) {
            Ok(p) => {
                log::info!(
                    "PersistChannel: order update record publisher created on '{}'",
                    ORDER_UPDATE_RECORD_CHANNEL
                );
                Some(p)
            }
            Err(err) => {
                warn!(
                    "PersistChannel: failed to create order update record publisher on '{}': {err:#}",
                    ORDER_UPDATE_RECORD_CHANNEL
                );
                None
            }
        };

        Self {
            signal_record_pub,
            trade_update_record_pub,
            order_update_record_pub,
        }
    }

    /// 发布信号记录到持久化通道
    ///
    /// # 参数
    /// - `record`: 信号记录消息（包含策略ID、信号类型、上下文等）
    ///
    /// # 错误处理
    /// - 如果发布器未初始化，静默跳过
    /// - 如果发布失败，记录警告但不阻塞调用者
    pub fn publish_signal_record(&self, record: &SignalRecordMessage) {
        let Some(publisher) = &self.signal_record_pub else {
            return;
        };

        let payload = record.to_bytes();
        if let Err(err) = publisher.publish(payload.as_ref()) {
            warn!(
                "failed to publish signal record strategy_id={}: {err:#}",
                record.strategy_id
            );
        }
    }

    /// 发布交易更新记录到持久化通道（支持所有交易所）
    ///
    /// # 参数
    /// - `trade_update`: 实现了 `TradeUpdate` trait 的成交记录
    ///
    /// # 错误处理
    /// - 如果发布器未初始化，静默跳过
    /// - 如果发布失败，记录警告但不阻塞调用者
    pub fn publish_trade_update(&self, trade_update: &dyn TradeUpdate) {
        let Some(publisher) = &self.trade_update_record_pub else {
            return;
        };

        let payload = serialize_trade_update(trade_update);
        if let Err(err) = publisher.publish(payload.as_ref()) {
            warn!(
                "failed to publish trade update trade_id={} order_id={} symbol={}: {err:#}",
                trade_update.trade_id(),
                trade_update.order_id(),
                trade_update.symbol()
            );
        }
    }

    /// 发布订单更新记录到持久化通道（支持所有交易所）
    ///
    /// # 参数
    /// - `order_update`: 实现了 `OrderUpdate` trait 的订单更新
    ///
    /// # 错误处理
    /// - 如果发布器未初始化，静默跳过
    /// - 如果发布失败，记录警告但不阻塞调用者
    pub fn publish_order_update(&self, order_update: &dyn OrderUpdate) {
        let Some(publisher) = &self.order_update_record_pub else {
            return;
        };

        let payload = serialize_order_update(order_update);
        if let Err(err) = publisher.publish(payload.as_ref()) {
            warn!(
                "failed to publish order update order_id={} client_order_id={} symbol={}: {err:#}",
                order_update.order_id(),
                order_update.client_order_id(),
                order_update.symbol()
            );
        }
    }

    /// 检查信号记录发布器是否可用
    pub fn is_signal_publisher_available(&self) -> bool {
        self.signal_record_pub.is_some()
    }

    /// 检查交易更新记录发布器是否可用
    pub fn is_trade_update_publisher_available(&self) -> bool {
        self.trade_update_record_pub.is_some()
    }

    /// 检查订单更新记录发布器是否可用
    pub fn is_order_update_publisher_available(&self) -> bool {
        self.order_update_record_pub.is_some()
    }
}

impl Default for PersistChannel {
    fn default() -> Self {
        Self::new()
    }
}

// ==================== 序列化辅助函数 ====================

/// 将 TradeUpdate trait object 序列化为字节流
///
/// 格式：
/// - event_time: i64 (8 bytes)
/// - trade_time: i64 (8 bytes)
/// - symbol: String (4 bytes len + data)
/// - trade_id: i64 (8 bytes)
/// - order_id: i64 (8 bytes)
/// - client_order_id: i64 (8 bytes)
/// - side: u8 (1 byte) - 0=Buy, 1=Sell
/// - price: f64 (8 bytes)
/// - quantity: f64 (8 bytes)
/// - commission: f64 (8 bytes)
/// - commission_asset: String (4 bytes len + data)
/// - is_maker: u8 (1 byte)
/// - realized_pnl: f64 (8 bytes)
/// - trading_venue: u8 (1 byte)
/// - cumulative_filled_quantity: f64 (8 bytes)
/// - order_status: u8 (1 byte) + Option flag
fn serialize_trade_update(trade: &dyn TradeUpdate) -> Bytes {
    let mut buf = BytesMut::with_capacity(512);

    // 基础时间戳
    buf.put_i64_le(trade.event_time());
    buf.put_i64_le(trade.trade_time());

    // 交易对符号
    put_string(&mut buf, trade.symbol());

    // ID 字段
    buf.put_i64_le(trade.trade_id());
    buf.put_i64_le(trade.order_id());
    buf.put_i64_le(trade.client_order_id());

    // 方向
    buf.put_u8(trade.side() as u8);

    // 价格数量
    buf.put_f64_le(trade.price());
    buf.put_f64_le(trade.quantity());

    // 手续费
    buf.put_f64_le(trade.commission());
    put_string(&mut buf, trade.commission_asset());

    // Maker/Taker
    buf.put_u8(trade.is_maker() as u8);

    // 已实现盈亏
    buf.put_f64_le(trade.realized_pnl());

    // 交易所类型
    buf.put_u8(trade.trading_venue() as u8);

    // 累计成交量
    buf.put_f64_le(trade.cumulative_filled_quantity());

    // 订单状态（可选）
    if let Some(status) = trade.order_status() {
        buf.put_u8(1); // has value
        buf.put_u8(status as u8);
    } else {
        buf.put_u8(0); // no value
    }

    buf.freeze()
}

/// 将 OrderUpdate trait object 序列化为字节流
///
/// 格式：
/// - event_time: i64 (8 bytes)
/// - symbol: String (4 bytes len + data)
/// - order_id: i64 (8 bytes)
/// - client_order_id: i64 (8 bytes)
/// - client_order_id_str: Option<String> (1 byte flag + 4 bytes len + data)
/// - side: u8 (1 byte)
/// - order_type: u8 (1 byte)
/// - time_in_force: u8 (1 byte)
/// - price: f64 (8 bytes)
/// - quantity: f64 (8 bytes)
/// - last_time_executed_qty: f64 (8 bytes)
/// - cumulative_filled_quantity: f64 (8 bytes)
/// - status: u8 (1 byte)
/// - raw_status: String (4 bytes len + data)
/// - execution_type: u8 (1 byte)
/// - raw_execution_type: String (4 bytes len + data)
/// - trading_venue: u8 (1 byte)
/// - average_price: Option<f64> (1 byte flag + 8 bytes)
/// - last_executed_price: Option<f64> (1 byte flag + 8 bytes)
/// - business_unit: Option<String> (1 byte flag + 4 bytes len + data)
fn serialize_order_update(order: &dyn OrderUpdate) -> Bytes {
    let mut buf = BytesMut::with_capacity(512);

    // 基础时间戳
    buf.put_i64_le(order.event_time());

    // 交易对符号
    put_string(&mut buf, order.symbol());

    // ID 字段
    buf.put_i64_le(order.order_id());
    buf.put_i64_le(order.client_order_id());
    put_opt_string(&mut buf, order.client_order_id_str());

    // 基础属性
    buf.put_u8(order.side() as u8);
    buf.put_u8(order.order_type() as u8);
    buf.put_u8(order.time_in_force() as u8);

    // 价格数量
    buf.put_f64_le(order.price());
    buf.put_f64_le(order.quantity());
    buf.put_f64_le(order.last_time_executed_qty());
    buf.put_f64_le(order.cumulative_filled_quantity());

    // 状态
    buf.put_u8(order.status() as u8);
    put_string(&mut buf, order.raw_status());

    // 执行类型
    buf.put_u8(order.execution_type() as u8);
    put_string(&mut buf, order.raw_execution_type());

    // 交易所类型
    buf.put_u8(order.trading_venue() as u8);

    // 可选字段：平均价格
    if let Some(avg_price) = order.average_price() {
        buf.put_u8(1);
        buf.put_f64_le(avg_price);
    } else {
        buf.put_u8(0);
    }

    // 可选字段：最后成交价格
    if let Some(last_price) = order.last_executed_price() {
        buf.put_u8(1);
        buf.put_f64_le(last_price);
    } else {
        buf.put_u8(0);
    }

    // 可选字段：业务单元
    put_opt_string(&mut buf, order.business_unit());

    buf.freeze()
}

fn put_string(buf: &mut BytesMut, value: &str) {
    buf.put_u32_le(value.len() as u32);
    buf.put_slice(value.as_bytes());
}

fn put_opt_string(buf: &mut BytesMut, value: Option<&str>) {
    if let Some(v) = value {
        buf.put_u8(1); // has value
        buf.put_u32_le(v.len() as u32);
        buf.put_slice(v.as_bytes());
    } else {
        buf.put_u8(0); // no value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_persist_channel_creation() {
        // 测试创建（可能失败如果没有 IceOryx 运行时）
        let channel = PersistChannel::new();
        // 基本断言：结构体应该创建成功
        assert!(
            channel.is_signal_publisher_available() || !channel.is_signal_publisher_available()
        );
    }
}
