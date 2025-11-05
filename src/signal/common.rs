
use std::convert::TryFrom;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// 交易标的枚举，表示不同交易所的不同交易类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TradingVenue {
    BinanceMargin = 0,
    BinanceUm = 1,
    BinanceSwap = 2,
    BinanceSpot = 3,
    OkexSwap = 4,
    OkexSpot = 5,
}

impl TradingVenue {
    /// 根据资产类型，进行量和价格的对齐
     

    /// 转换为 u8
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    /// 从 u8 转换，返回 Option
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(TradingVenue::BinanceMargin),
            1 => Some(TradingVenue::BinanceUm),
            2 => Some(TradingVenue::BinanceSwap),
            3 => Some(TradingVenue::BinanceSpot),
            4 => Some(TradingVenue::OkexSwap),
            5 => Some(TradingVenue::OkexSpot),
            _ => None,
        }
    }

    /// 获取交易所名称
    pub fn exchange_name(&self) -> &'static str {
        match self {
            TradingVenue::BinanceMargin |
            TradingVenue::BinanceUm |
            TradingVenue::BinanceSwap => "binance_futures",
            TradingVenue::BinanceSpot => "binance",
            TradingVenue::OkexSwap => "okex_swap",
            TradingVenue::OkexSpot => "okex",
        }
    }

    /// 获取交易类型
    pub fn venue_type(&self) -> &'static str {
        match self {
            TradingVenue::BinanceMargin => "margin",
            TradingVenue::BinanceUm => "um",
            TradingVenue::BinanceSwap => "swap",
            TradingVenue::BinanceSpot => "spot",
            TradingVenue::OkexSwap => "swap",
            TradingVenue::OkexSpot => "spot",
        }
    }

    /// 是否是期货/合约类型
    pub fn is_futures(&self) -> bool {
        matches!(self,
            TradingVenue::BinanceUm |
            TradingVenue::BinanceSwap |
            TradingVenue::OkexSwap
        )
    }

    /// 是否是现货类型
    pub fn is_spot(&self) -> bool {
        matches!(self,
            TradingVenue::BinanceSpot |
            TradingVenue::OkexSpot
        )
    }

    /// 是否是杠杆交易
    pub fn is_margin(&self) -> bool {
        matches!(self, TradingVenue::BinanceMargin)
    }
}

/// 实现 TryFrom trait，用于类型安全的转换
impl TryFrom<u8> for TradingVenue {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::from_u8(value)
            .ok_or_else(|| format!("Invalid TradingVenue value: {}", value))
    }
}

/// 实现 From trait，用于转换为 u8
impl From<TradingVenue> for u8 {
    fn from(venue: TradingVenue) -> Self {
        venue.to_u8()
    }
}

/// 订单类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum OrderType {
    Limit = 0,
    Market = 1,
    PostOnly = 2,
}

impl OrderType {
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(OrderType::Limit),
            1 => Some(OrderType::Market),
            2 => Some(OrderType::PostOnly),
            _ => None,
        }
    }
}

/// 交易方向
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Side {
    Buy = 0,
    Sell = 1,
}

impl Side {
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Side::Buy),
            1 => Some(Side::Sell),
            _ => None,
        }
    }
}

/// 交易腿信息（不包含序列化）
#[derive(Debug, Clone, Copy)]
pub struct TradingLeg {
    pub venue: u8,  // TradingVenue as u8
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
            return Err(format!("Not enough bytes for array data: need {}, have {}", len, bytes.remaining()));
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