use crate::common::min_qty_table::{MarketType, MinQtyTable};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use log::debug;
use std::convert::TryFrom;

/// 交易标的枚举，表示不同交易所的不同交易类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TradingVenue {
    BinanceMargin = 0,
    BinanceUm = 1,
    BinanceSpot = 3,
    OkexSwap = 4,
    OkexSpot = 5,
    BitgetFutures = 6,
    BybitFutures = 7,
    GateFutures = 8,
}

/// 订单有效期类型枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeInForce {
    GTC, // Good Till Cancel - 成交为止
    IOC, // Immediate or Cancel - 无法立即成交的部分就撤销
    FOK, // Fill or Kill - 无法全部立即成交就撤销
    GTX, // Good Till Crossing - 成交为止，但只做 maker
}

impl TimeInForce {
    /// 从字符串解析
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "GTC" => Some(TimeInForce::GTC),
            "IOC" => Some(TimeInForce::IOC),
            "FOK" => Some(TimeInForce::FOK),
            "GTX" => Some(TimeInForce::GTX),
            _ => None,
        }
    }

    /// 转换为字符串
    pub fn as_str(&self) -> &'static str {
        match self {
            TimeInForce::GTC => "GTC",
            TimeInForce::IOC => "IOC",
            TimeInForce::FOK => "FOK",
            TimeInForce::GTX => "GTX",
        }
    }
}

/// 订单执行类型枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionType {
    /// 新订单已被引擎接受
    New,
    /// 订单被用户取消
    Canceled,
    /// 保留字段，当前未使用
    Replaced,
    /// 新订单被拒绝（这信息只会在撤消挂单再下单中发生，下新订单被拒绝但撤消挂单请求成功）
    Rejected,
    /// 订单有新成交
    Trade,
    /// 订单已根据 Time In Force 参数的规则取消（如没有成交的 LIMIT FOK 订单或部分成交的 LIMIT IOC 订单）
    /// 或者被交易所取消（如强平或维护期间取消的订单）
    Expired,
    /// 订单因 STP 触发而过期
    TradePrevention,
}

impl ExecutionType {
    /// 从字符串解析
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "NEW" => Some(ExecutionType::New),
            "CANCELED" | "CANCELLED" => Some(ExecutionType::Canceled),
            "REPLACED" => Some(ExecutionType::Replaced),
            "REJECTED" => Some(ExecutionType::Rejected),
            "TRADE" => Some(ExecutionType::Trade),
            "EXPIRED" => Some(ExecutionType::Expired),
            "TRADE_PREVENTION" => Some(ExecutionType::TradePrevention),
            _ => None,
        }
    }

    /// 转换为字符串
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecutionType::New => "NEW",
            ExecutionType::Canceled => "CANCELED",
            ExecutionType::Replaced => "REPLACED",
            ExecutionType::Rejected => "REJECTED",
            ExecutionType::Trade => "TRADE",
            ExecutionType::Expired => "EXPIRED",
            ExecutionType::TradePrevention => "TRADE_PREVENTION",
        }
    }

    /// 检查是否为成交事件
    pub fn is_trade(&self) -> bool {
        matches!(self, ExecutionType::Trade)
    }

    /// 检查订单是否已结束（不会再有更新）
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ExecutionType::Canceled
                | ExecutionType::Rejected
                | ExecutionType::Expired
                | ExecutionType::TradePrevention
        )
    }
}

/// 订单状态枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderStatus {
    /// 新订单，已被交易引擎接受
    New,
    /// 部分成交
    PartiallyFilled,
    /// 完全成交
    Filled,
    /// 订单被取消
    Canceled,
    /// 订单过期（根据 Time In Force 规则）
    Expired,
    /// 订单在撮合时过期
    ExpiredInMatch,
}

impl OrderStatus {
    /// 从字符串解析
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "NEW" => Some(OrderStatus::New),
            "PARTIALLY_FILLED" => Some(OrderStatus::PartiallyFilled),
            "FILLED" => Some(OrderStatus::Filled),
            "CANCELED" | "CANCELLED" => Some(OrderStatus::Canceled),
            "EXPIRED" => Some(OrderStatus::Expired),
            "EXPIRED_IN_MATCH" => Some(OrderStatus::ExpiredInMatch),
            _ => None,
        }
    }

    /// 转换为字符串
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderStatus::New => "NEW",
            OrderStatus::PartiallyFilled => "PARTIALLY_FILLED",
            OrderStatus::Filled => "FILLED",
            OrderStatus::Canceled => "CANCELED",
            OrderStatus::Expired => "EXPIRED",
            OrderStatus::ExpiredInMatch => "EXPIRED_IN_MATCH",
        }
    }

    /// 检查订单是否已完成（终态）
    pub fn is_finished(&self) -> bool {
        matches!(
            self,
            OrderStatus::Filled
                | OrderStatus::Canceled
                | OrderStatus::Expired
                | OrderStatus::ExpiredInMatch
        )
    }

    /// 检查订单是否部分成交
    pub fn is_partially_filled(&self) -> bool {
        matches!(self, OrderStatus::PartiallyFilled)
    }

    /// 检查订单是否已成交（完全或部分）
    pub fn has_filled(&self) -> bool {
        matches!(self, OrderStatus::PartiallyFilled | OrderStatus::Filled)
    }

    /// 检查订单是否为活跃状态（可能有后续更新）
    pub fn is_active(&self) -> bool {
        matches!(self, OrderStatus::New | OrderStatus::PartiallyFilled)
    }
}

fn to_fraction(value: f64) -> Option<(i64, i64)> {
    if !value.is_finite() || value <= 0.0 {
        return None;
    }
    let mut denom: i64 = 1;
    let mut scaled = value;
    for _ in 0..9 {
        let rounded = scaled.round();
        if (scaled - rounded).abs() < 1e-9 {
            return Some((rounded as i64, denom));
        }
        scaled *= 10.0;
        denom = denom.saturating_mul(10);
    }
    None
}

fn align_price_floor(price: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return price;
    }
    if let Some((tick_num, tick_den)) = to_fraction(tick) {
        if tick_num == 0 {
            return price;
        }
        let tick_num = tick_num as i128;
        let tick_den = tick_den as i128;
        let units = ((price * tick_den as f64) + 1e-9).floor() as i128;
        let aligned_units = (units / tick_num) * tick_num;
        return aligned_units as f64 / tick_den as f64;
    }
    let scaled = ((price / tick) + 1e-9).floor();
    scaled * tick
}

fn align_price_ceil(price: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return price;
    }
    if let Some((tick_num, tick_den)) = to_fraction(tick) {
        if tick_num == 0 {
            return price;
        }
        let tick_num = tick_num as i128;
        let tick_den = tick_den as i128;
        let units = ((price * tick_den as f64) - 1e-9).ceil() as i128;
        let aligned_units = ((units + tick_num - 1) / tick_num) * tick_num;
        return aligned_units as f64 / tick_den as f64;
    }
    let scaled = ((price / tick) - 1e-9).ceil();
    scaled * tick
}

impl TradingVenue {
    /// 针对币安 UM 永续限价单，将数量与价格按照交易所过滤器对齐。
    /// 返回 `(调整后的数量, 调整后的价格)`。
    pub fn align_um_order(
        symbol: &str,
        raw_qty: f64,
        raw_price: f64,
        min_qty_table: &MinQtyTable,
    ) -> Result<(f64, f64), String> {
        if raw_qty <= 0.0 {
            return Err(format!(
                "symbol={} 原始下单量无效 raw_qty={}",
                symbol, raw_qty
            ));
        }
        if raw_price <= 0.0 {
            return Err(format!(
                "symbol={} 原始价格无效 raw_price={}",
                symbol, raw_price
            ));
        }

        // 1. 按照 tick 对齐价格，交易所要求价格满足最小价格步进。
        let price_tick = min_qty_table
            .price_tick(MarketType::Futures, symbol)
            .unwrap_or(0.0);
        let price = if price_tick > 0.0 {
            align_price_floor(raw_price, price_tick)
        } else {
            raw_price
        };
        if price <= 0.0 {
            return Err(format!("symbol={} 对齐后价格无效 price={}", symbol, price));
        }

        // 2. 数量按 step 对齐，保证符合交易所最小数量步长。
        let step = min_qty_table
            .step_size(MarketType::Futures, symbol)
            .unwrap_or(0.0);
        let mut qty = if step > 0.0 {
            align_price_floor(raw_qty, step)
        } else {
            raw_qty
        };

        // 3. 补齐最小下单量要求。
        if let Some(min_qty) = min_qty_table.min_qty(MarketType::Futures, symbol) {
            if min_qty > 0.0 && qty < min_qty {
                qty = min_qty;
            }
        }

        // 4. 补齐最小名义金额要求，必要时抬高下单量。
        if let Some(min_notional) = min_qty_table.min_notional(MarketType::Futures, symbol) {
            if min_notional > 0.0 {
                let required_qty = min_notional / price;
                if qty < required_qty {
                    let before = qty;
                    qty = if step > 0.0 {
                        align_price_ceil(required_qty, step)
                    } else {
                        required_qty
                    };
                    debug!(
                        "symbol={} 名义金额要求从 {} 调整到 {} (min_notional={}, price={})",
                        symbol, before, qty, min_notional, price
                    );
                }
            }
        }

        if qty <= 0.0 {
            return Err(format!("symbol={} 对齐后数量无效 qty={}", symbol, qty));
        }

        Ok((qty, price))
    }

    /// 针对币安现货/保证金限价单，将数量与价格按照过滤器对齐。
    /// 现货与逐仓保证金共用一套过滤器，因此统一复用 margin_* 表。
    pub fn align_margin_order(
        symbol: &str,
        raw_qty: f64,
        raw_price: f64,
        min_qty_table: &MinQtyTable,
    ) -> Result<(f64, f64), String> {
        if raw_qty <= 0.0 {
            return Err(format!(
                "symbol={} 原始下单量无效 raw_qty={}",
                symbol, raw_qty
            ));
        }
        if raw_price <= 0.0 {
            return Err(format!(
                "symbol={} 原始价格无效 raw_price={}",
                symbol, raw_price
            ));
        }

        // 1. 价格对齐：币安现货/保证金沿用 spot tick。
        let price_tick = min_qty_table
            .price_tick(MarketType::Margin, symbol)
            .unwrap_or(0.0);
        let price = if price_tick > 0.0 {
            align_price_floor(raw_price, price_tick)
        } else {
            raw_price
        };
        if price <= 0.0 {
            return Err(format!("symbol={} 对齐后价格无效 price={}", symbol, price));
        }

        // 2. 数量按 step 对齐。
        let step = min_qty_table
            .step_size(MarketType::Margin, symbol)
            .unwrap_or(0.0);
        let mut qty = if step > 0.0 {
            align_price_floor(raw_qty, step)
        } else {
            raw_qty
        };

        // 3. 补齐最小下单量。
        if let Some(min_qty) = min_qty_table.min_qty(MarketType::Margin, symbol) {
            if min_qty > 0.0 && qty < min_qty {
                qty = min_qty;
            }
        }

        // 4. 补齐名义金额（部分币对会要求最小 notional）。
        if let Some(min_notional) = min_qty_table.min_notional(MarketType::Margin, symbol) {
            if min_notional > 0.0 {
                let required_qty = min_notional / price;
                if qty < required_qty {
                    let before = qty;
                    qty = if step > 0.0 {
                        align_price_ceil(required_qty, step)
                    } else {
                        required_qty
                    };
                    debug!(
                        "symbol={} 名义金额要求从 {} 调整到 {} (min_notional={}, price={})",
                        symbol, before, qty, min_notional, price
                    );
                }
            }
        }

        if qty <= 0.0 {
            return Err(format!("symbol={} 对齐后数量无效 qty={}", symbol, qty));
        }

        Ok((qty, price))
    }

    /// 转换为 u8
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    /// 从 u8 转换，返回 Option
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(TradingVenue::BinanceMargin),
            1 => Some(TradingVenue::BinanceUm),
            3 => Some(TradingVenue::BinanceSpot),
            4 => Some(TradingVenue::OkexSwap),
            5 => Some(TradingVenue::OkexSpot),
            6 => Some(TradingVenue::BitgetFutures),
            7 => Some(TradingVenue::BybitFutures),
            8 => Some(TradingVenue::GateFutures),
            _ => None,
        }
    }

    /// 转换为字符串表示
    pub fn as_str(&self) -> &'static str {
        match self {
            TradingVenue::BinanceMargin => "BinanceMargin",
            TradingVenue::BinanceUm => "BinanceUm",
            TradingVenue::BinanceSpot => "BinanceSpot",
            TradingVenue::OkexSwap => "OkexSwap",
            TradingVenue::OkexSpot => "OkexSpot",
            TradingVenue::BitgetFutures => "BitgetFutures",
            TradingVenue::BybitFutures => "BybitFutures",
            TradingVenue::GateFutures => "GateFutures",
        }
    }

    /// 获取交易所名称
    pub fn exchange_name(&self) -> &'static str {
        match self {
            TradingVenue::BinanceMargin | TradingVenue::BinanceUm => "binance_futures",
            TradingVenue::BinanceSpot => "binance",
            TradingVenue::OkexSwap => "okex_swap",
            TradingVenue::OkexSpot => "okex",
            TradingVenue::BitgetFutures => "bitget_futures",
            TradingVenue::BybitFutures => "bybit_futures",
            TradingVenue::GateFutures => "gate_futures",
        }
    }

    /// 获取交易类型
    pub fn venue_type(&self) -> &'static str {
        match self {
            TradingVenue::BinanceMargin => "margin",
            TradingVenue::BinanceUm => "um",
            TradingVenue::BinanceSpot => "spot",
            TradingVenue::OkexSwap => "swap",
            TradingVenue::OkexSpot => "spot",
            TradingVenue::BitgetFutures => "futures",
            TradingVenue::BybitFutures => "futures",
            TradingVenue::GateFutures => "futures",
        }
    }

    /// 是否是期货/合约类型
    pub fn is_futures(&self) -> bool {
        matches!(
            self,
            TradingVenue::BinanceUm
                | TradingVenue::OkexSwap
                | TradingVenue::BitgetFutures
                | TradingVenue::BybitFutures
                | TradingVenue::GateFutures
        )
    }

    /// 是否是现货类型
    pub fn is_spot(&self) -> bool {
        matches!(self, TradingVenue::BinanceSpot | TradingVenue::OkexSpot)
    }
}

/// 实现 TryFrom trait，用于类型安全的转换
impl TryFrom<u8> for TradingVenue {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::from_u8(value).ok_or_else(|| format!("Invalid TradingVenue value: {}", value))
    }
}

/// 实现 From trait，用于转换为 u8
impl From<TradingVenue> for u8 {
    fn from(venue: TradingVenue) -> Self {
        venue.to_u8()
    }
}

/// 交易腿信息（不包含序列化）
#[derive(Debug, Clone, Copy)]
pub struct TradingLeg {
    pub venue: u8, // TradingVenue as u8
    pub bid0: f64,
    pub ask0: f64,
}

impl TradingLeg {
    pub fn new(venue: TradingVenue, bid0: f64, ask0: f64) -> Self {
        Self {
            venue: venue.to_u8(),
            bid0,
            ask0,
        }
    }

    pub fn get_venue(&self) -> Option<TradingVenue> {
        TradingVenue::from_u8(self.venue)
    }
}

/// Trait for signal serialization/deserialization
pub trait SignalBytes: Sized {
    /// Serialize the signal to bytes
    fn to_bytes(&self) -> Bytes;

    /// Deserialize the signal from bytes
    fn from_bytes(bytes: Bytes) -> Result<Self, String>;
}

/// Helper functions for byte serialization
pub mod bytes_helper {
    use super::*;

    /// Write a fixed-size byte array (for symbol storage)
    pub fn write_fixed_bytes(buf: &mut BytesMut, bytes: &[u8; 32]) {
        // Find the actual length (until first zero or full length)
        let len = bytes.iter().position(|&b| b == 0).unwrap_or(32);
        buf.put_u8(len as u8);
        buf.put_slice(&bytes[..len]);
    }

    /// Read a fixed-size byte array
    pub fn read_fixed_bytes(bytes: &mut Bytes) -> Result<[u8; 32], String> {
        if bytes.remaining() < 1 {
            return Err("Not enough bytes for array length".to_string());
        }
        let len = bytes.get_u8() as usize;
        if len > 32 {
            return Err(format!("Invalid array length: {}", len));
        }
        if bytes.remaining() < len {
            return Err(format!(
                "Not enough bytes for array data: need {}, have {}",
                len,
                bytes.remaining()
            ));
        }

        let mut arr = [0u8; 32];
        bytes.copy_to_slice(&mut arr[..len]);
        Ok(arr)
    }

    /// Write an optional f64
    pub fn write_option_f64(buf: &mut BytesMut, value: f64) {
        // Use 0.0 to represent None (since we agreed to use 0 for no value)
        buf.put_f64_le(value);
    }

    /// Read an optional f64
    pub fn read_option_f64(bytes: &mut Bytes) -> Result<f64, String> {
        if bytes.remaining() < 8 {
            return Err("Not enough bytes for f64".to_string());
        }
        Ok(bytes.get_f64_le())
    }
}
