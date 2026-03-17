use bytes::{BufMut, Bytes, BytesMut};
use log::warn;
use std::cell::OnceCell;

use crate::common::iceoryx_publisher::{
    OrderUpdatePublisher, TradeUpdatePublisher, UniformOrderPublisher,
};
use crate::common::time_util::get_timestamp_us;
use crate::persist_manager::unified_order::UnifiedOrderRecord;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::signal::common::TradingVenue;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;

thread_local! {
    static PERSIST_CHANNEL: OnceCell<PersistChannel> = OnceCell::new();
}

/// 通用交易更新记录频道（支持所有交易所）
pub const TRADE_UPDATE_RECORD_CHANNEL: &str = "trade_update_record";
/// 未匹配到策略的交易更新记录频道
pub const TRADE_UPDATE_UNMATCHED_RECORD_CHANNEL: &str = "trade_update_unmatched_record";

/// 通用订单更新记录频道（支持所有交易所）
pub const ORDER_UPDATE_RECORD_CHANNEL: &str = "order_update_record";
/// 未匹配到策略的订单更新记录频道
pub const ORDER_UPDATE_UNMATCHED_RECORD_CHANNEL: &str = "order_update_unmatched_record";
/// 统一订单记录频道
pub const UNIFORM_ORDER_RECORD_CHANNEL: &str = "uniform_order_record";

/// 持久化通道：负责将交易更新和订单更新通过 IceOryx 发布到下游持久化服务
///
/// 下游消费者：
/// - Trade Update: `persist_manager::TradeUpdatePersistor` -> RocksDB (`trade_update_record`)
/// - Order Update: `persist_manager::OrderUpdatePersistor` -> RocksDB (`order_update_record`)
/// - Unmatched Trade Update: `persist_manager::TradeUpdateUnmatchedPersistor` -> RocksDB (`trade_updates_unmatched`)
/// - Unmatched Order Update: `persist_manager::OrderUpdateUnmatchedPersistor` -> RocksDB (`order_updates_unmatched`)
///
/// 采用直接发布模式（无中间队列），优先保证低延迟
///
/// 支持通过 trait object 发布：
/// - `publish_trade_update(&dyn TradeUpdate)` - 发布成交记录
/// - `publish_order_update(&dyn OrderUpdate)` - 发布订单更新
pub struct PersistChannel {
    trade_update_record_pub: Option<TradeUpdatePublisher>,
    order_update_record_pub: Option<OrderUpdatePublisher>,
    trade_update_unmatched_pub: Option<TradeUpdatePublisher>,
    order_update_unmatched_pub: Option<OrderUpdatePublisher>,
    uniform_order_record_pub: Option<UniformOrderPublisher>,
}

impl PersistChannel {
    /// 在当前线程的 PersistChannel 单例上执行操作
    ///
    /// 第一次调用时会自动初始化，后续调用直接使用已初始化的实例
    ///
    /// # 使用示例
    /// ```ignore
    /// use crate::pre_trade::PersistChannel;
    ///
    /// // 在任何地方直接使用，无需传递引用
    /// PersistChannel::with(|ch| ch.publish_trade_update(&trade));
    /// PersistChannel::with(|ch| ch.publish_order_update(&order));
    /// ```
    pub fn with<F, R>(f: F) -> R
    where
        F: FnOnce(&PersistChannel) -> R,
    {
        PERSIST_CHANNEL.with(|cell| {
            let channel = cell.get_or_init(|| {
                log::info!("Initializing thread-local PersistChannel singleton");
                PersistChannel::new()
            });
            f(channel)
        })
    }

    /// 创建持久化通道，初始化 IceOryx 发布器
    ///
    /// 注意：通常应使用 `PersistChannel::with()` 访问线程本地单例，
    /// 而不是直接调用 `new()` 创建多个实例
    ///
    /// 如果发布器创建失败，会记录警告并继续运行（降级模式）
    fn new() -> Self {
        let trade_update_record_pub =
            TradeUpdatePublisher::new_with_prefix("persist_pubs", TRADE_UPDATE_RECORD_CHANNEL)
                .map_err(|e| warn!("PersistChannel trade_update_record_pub failed: {e:#}"))
                .ok();

        let order_update_record_pub =
            OrderUpdatePublisher::new_with_prefix("persist_pubs", ORDER_UPDATE_RECORD_CHANNEL)
                .map_err(|e| warn!("PersistChannel order_update_record_pub failed: {e:#}"))
                .ok();

        let trade_update_unmatched_pub = TradeUpdatePublisher::new_with_prefix(
            "persist_pubs",
            TRADE_UPDATE_UNMATCHED_RECORD_CHANNEL,
        )
        .map_err(|e| warn!("PersistChannel trade_update_unmatched_pub failed: {e:#}"))
        .ok();

        let order_update_unmatched_pub = OrderUpdatePublisher::new_with_prefix(
            "persist_pubs",
            ORDER_UPDATE_UNMATCHED_RECORD_CHANNEL,
        )
        .map_err(|e| warn!("PersistChannel order_update_unmatched_pub failed: {e:#}"))
        .ok();

        let uniform_order_record_pub =
            UniformOrderPublisher::new_with_prefix("persist_pubs", UNIFORM_ORDER_RECORD_CHANNEL)
                .map_err(|e| warn!("PersistChannel uniform_order_record_pub failed: {e:#}"))
                .ok();

        Self {
            trade_update_record_pub,
            order_update_record_pub,
            trade_update_unmatched_pub,
            order_update_unmatched_pub,
            uniform_order_record_pub,
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
                "failed to publish trade update order_id={} client_order_id={} symbol={}: {err:#}",
                trade_update.order_id(),
                trade_update.client_order_id(),
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

    /// 发布未匹配到策略的交易更新记录
    pub fn publish_trade_update_unmatched(&self, trade_update: &dyn TradeUpdate) {
        let Some(publisher) = &self.trade_update_unmatched_pub else {
            return;
        };

        let payload = serialize_trade_update(trade_update);
        if let Err(err) = publisher.publish(payload.as_ref()) {
            warn!(
                "failed to publish unmatched trade update order_id={} client_order_id={} symbol={}: {err:#}",
                trade_update.order_id(),
                trade_update.client_order_id(),
                trade_update.symbol()
            );
        }
    }

    /// 发布未匹配到策略的订单更新记录
    pub fn publish_order_update_unmatched(&self, order_update: &dyn OrderUpdate) {
        let Some(publisher) = &self.order_update_unmatched_pub else {
            return;
        };

        let payload = serialize_order_update(order_update);
        if let Err(err) = publisher.publish(payload.as_ref()) {
            warn!(
                "failed to publish unmatched order update order_id={} client_order_id={} symbol={}: {err:#}",
                order_update.order_id(),
                order_update.client_order_id(),
                order_update.symbol()
            );
        }
    }

    /// 发布统一订单记录（二进制格式）
    pub fn publish_uniform_order(&self, record: &UnifiedOrderRecord) {
        let Some(publisher) = &self.uniform_order_record_pub else {
            return;
        };

        let payload = serialize_uniform_order(record);
        if let Err(err) = publisher.publish(payload.as_ref()) {
            warn!(
                "failed to publish uniform order update client_order_id={} symbol_len={} from_key_len={}: {err:#}",
                record.client_order_id,
                record.symbol_len,
                record.from_key_len
            );
        }
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

// 不实现 Default trait，鼓励使用 PersistChannel::global() 单例模式

// ==================== 序列化辅助函数 ====================

fn normalize_symbol_for_venue(venue: TradingVenue, symbol: &str) -> String {
    let upper = symbol.to_uppercase();
    match venue {
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
            upper.replace("-SWAP", "").replace('-', "")
        }
        TradingVenue::GateMargin | TradingVenue::GateFutures => {
            upper.replace('_', "").replace('-', "")
        }
        _ => upper,
    }
}

fn resolve_futures_qty_multiplier(venue: TradingVenue, normalized_symbol: &str) -> f64 {
    if !venue.is_futures() {
        return 1.0;
    }
    if venue == TradingVenue::BinanceFutures {
        return 1.0;
    }

    let Some(table) = MonitorChannel::instance().try_venue_min_qty_table(venue) else {
        return 1.0;
    };

    table
        .contract_multiplier_opt(normalized_symbol)
        .filter(|v| v.is_finite() && *v > 0.0)
        .unwrap_or(1.0)
}

fn normalize_symbol_and_qty(venue: TradingVenue, symbol: &str, qty: f64) -> (String, f64) {
    let normalized_symbol = normalize_symbol_for_venue(venue, symbol);
    let multiplier = resolve_futures_qty_multiplier(venue, normalized_symbol.as_str());
    let normalized_qty = if venue.is_futures() {
        qty * multiplier
    } else {
        qty
    };
    (normalized_symbol, normalized_qty)
}

/// 将 TradeUpdate trait object 序列化为字节流
///
/// 格式：
/// - receive_ts_us: i64 (8 bytes) - 接收时间戳（微秒）
/// - event_time: i64 (8 bytes)
/// - trade_time: i64 (8 bytes)
/// - symbol: String (4 bytes len + data)
/// - order_id: i64 (8 bytes)
/// - client_order_id: i64 (8 bytes)
/// - side: u8 (1 byte) - 0=Buy, 1=Sell
/// - price: f64 (8 bytes)
/// - is_maker: u8 (1 byte)
/// - trading_venue: u8 (1 byte)
/// - cumulative_filled_quantity: f64 (8 bytes)
/// - order_status: u8 (1 byte) + Option flag
fn serialize_trade_update(trade: &dyn TradeUpdate) -> Bytes {
    let mut buf = BytesMut::with_capacity(512);
    let venue = trade.trading_venue();
    let (normalized_symbol, normalized_cum_qty) =
        normalize_symbol_and_qty(venue, trade.symbol(), trade.cumulative_filled_quantity());

    // 接收时间戳（在发布时记录）
    buf.put_i64_le(get_timestamp_us());

    // 基础时间戳
    buf.put_i64_le(trade.event_time());
    buf.put_i64_le(trade.trade_time());

    // 交易对符号
    put_string(&mut buf, normalized_symbol.as_str());

    // ID 字段
    buf.put_i64_le(trade.order_id());
    buf.put_i64_le(trade.client_order_id());

    // 方向
    buf.put_u8(trade.side() as u8);

    // 价格
    buf.put_f64_le(trade.price());

    // Maker/Taker
    buf.put_u8(trade.is_maker() as u8);

    // 交易所类型
    buf.put_u8(venue as u8);

    // 累计成交量
    buf.put_f64_le(normalized_cum_qty);

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
/// - receive_ts_us: i64 (8 bytes) - 接收时间戳（微秒）
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
/// - cumulative_filled_quantity: f64 (8 bytes)
/// - status: u8 (1 byte)
/// - raw_status: String (4 bytes len + data)
/// - execution_type: u8 (1 byte)
/// - raw_execution_type: String (4 bytes len + data)
/// - trading_venue: u8 (1 byte)
fn serialize_order_update(order: &dyn OrderUpdate) -> Bytes {
    let mut buf = BytesMut::with_capacity(512);
    let venue = order.trading_venue();
    let (normalized_symbol, normalized_qty) =
        normalize_symbol_and_qty(venue, order.symbol(), order.quantity());
    let (_, normalized_cum_qty) =
        normalize_symbol_and_qty(venue, order.symbol(), order.cumulative_filled_quantity());

    // 接收时间戳（在发布时记录）
    buf.put_i64_le(get_timestamp_us());

    // 基础时间戳
    buf.put_i64_le(order.event_time());

    // 交易对符号
    put_string(&mut buf, normalized_symbol.as_str());

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
    buf.put_f64_le(normalized_qty);
    buf.put_f64_le(normalized_cum_qty);

    // 状态
    buf.put_u8(order.status() as u8);
    put_string(&mut buf, order.raw_status());

    // 执行类型
    buf.put_u8(order.execution_type() as u8);
    put_string(&mut buf, order.raw_execution_type());

    // 交易所类型
    buf.put_u8(venue as u8);

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

/// 将 UnifiedOrderRecord 序列化为字节流
///
/// 格式：
/// - recv_ts_us: i64
/// - symbol_len: u16
/// - symbol: [u8; symbol_len]
/// - create_ts: i64
/// - update_ts: i64
/// - signal_ts: i64
/// - client_order_id: i64
/// - venue: u8
/// - ttype: u8
/// - side: u8
/// - price: f64
/// - price_offset: f64
/// - amount_init: f64
/// - amount_update: f64
/// - status: u8
/// - from_key_len: u32
/// - from_key: [u8; from_key_len]
fn serialize_uniform_order(record: &UnifiedOrderRecord) -> Bytes {
    let mut buf = BytesMut::with_capacity(512);

    let venue = TradingVenue::from_u8(record.venue).unwrap_or(TradingVenue::BinanceMargin);
    let raw_symbol = String::from_utf8_lossy(&record.symbol);
    let (normalized_symbol, normalized_amount_init) =
        normalize_symbol_and_qty(venue, raw_symbol.as_ref(), record.amount_init);
    let (_, normalized_amount_update) =
        normalize_symbol_and_qty(venue, raw_symbol.as_ref(), record.amount_update);

    buf.put_i64_le(get_timestamp_us());

    let symbol_len = normalized_symbol.len() as u16;
    buf.put_u16_le(symbol_len);
    buf.put_slice(normalized_symbol.as_bytes());

    buf.put_i64_le(record.create_ts);
    buf.put_i64_le(record.update_ts);
    buf.put_i64_le(record.signal_ts);

    buf.put_i64_le(record.client_order_id);

    buf.put_u8(record.venue);
    buf.put_u8(record.ttype);
    buf.put_u8(record.side);

    buf.put_f64_le(record.price);
    buf.put_f64_le(record.price_offset);
    buf.put_f64_le(normalized_amount_init);
    buf.put_f64_le(normalized_amount_update);

    buf.put_u8(record.status);

    let from_key_len = record.from_key.len() as u32;
    buf.put_u32_le(from_key_len);
    buf.put_slice(&record.from_key);

    buf.freeze()
}

// NOTE: persist-channel unit tests removed per repo usage (requires IceOryx runtime/namespace).
