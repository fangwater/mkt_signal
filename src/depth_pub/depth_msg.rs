//! Depth 消息结构模块
//!
//! 定义 Depth5/Depth20/Depth50 消息格式

use bytes::{BufMut, Bytes, BytesMut};

/// 深度消息类型
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthMsgType {
    Depth5 = 2005,
    Depth20 = 2020,
    Depth50 = 2050,
}

/// 单个档位
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct DepthLevel {
    pub price: f64,
    pub amount: f64,
}

impl DepthLevel {
    pub fn new(price: f64, amount: f64) -> Self {
        Self { price, amount }
    }
}

/// 深度消息结构
///
/// 二进制布局:
/// - msg_type: u32 (4 bytes)
/// - symbol_length: u32 (4 bytes)
/// - symbol: [u8; symbol_length]
/// - timestamp: i64 (8 bytes)
/// - bids: [DepthLevel; N] (N * 16 bytes)
/// - asks: [DepthLevel; N] (N * 16 bytes)
#[derive(Debug, Clone)]
pub struct DepthMsg {
    pub msg_type: DepthMsgType,
    pub symbol: String,
    pub timestamp: i64,
    pub bids: Vec<DepthLevel>,
    pub asks: Vec<DepthLevel>,
}

impl DepthMsg {
    /// 创建 Depth5 消息
    pub fn depth5(symbol: String, timestamp: i64, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) -> Self {
        Self::new(DepthMsgType::Depth5, symbol, timestamp, bids, asks, 5)
    }

    /// 创建 Depth20 消息
    pub fn depth20(symbol: String, timestamp: i64, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) -> Self {
        Self::new(DepthMsgType::Depth20, symbol, timestamp, bids, asks, 20)
    }

    /// 创建 Depth50 消息
    pub fn depth50(symbol: String, timestamp: i64, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) -> Self {
        Self::new(DepthMsgType::Depth50, symbol, timestamp, bids, asks, 50)
    }

    fn new(
        msg_type: DepthMsgType,
        symbol: String,
        timestamp: i64,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>,
        max_levels: usize,
    ) -> Self {
        let bids: Vec<DepthLevel> = bids
            .into_iter()
            .take(max_levels)
            .map(|(p, a)| DepthLevel::new(p, a))
            .collect();
        let asks: Vec<DepthLevel> = asks
            .into_iter()
            .take(max_levels)
            .map(|(p, a)| DepthLevel::new(p, a))
            .collect();

        Self {
            msg_type,
            symbol,
            timestamp,
            bids,
            asks,
        }
    }

    /// 获取深度档数
    pub fn depth_level(&self) -> usize {
        match self.msg_type {
            DepthMsgType::Depth5 => 5,
            DepthMsgType::Depth20 => 20,
            DepthMsgType::Depth50 => 50,
        }
    }

    /// 计算消息字节大小
    pub fn byte_size(&self) -> usize {
        let n = self.depth_level();
        // msg_type(4) + symbol_length(4) + symbol + timestamp(8) + bids(n*16) + asks(n*16)
        4 + 4 + self.symbol.len() + 8 + n * 16 * 2
    }

    /// 转换为字节
    pub fn to_bytes(&self) -> Bytes {
        let n = self.depth_level();
        let symbol_len = self.symbol.len();
        let total_size = 4 + 4 + symbol_len + 8 + n * 16 * 2;

        let mut buf = BytesMut::with_capacity(total_size);

        // 写入消息类型
        buf.put_u32_le(self.msg_type as u32);

        // 写入 symbol 长度和内容
        buf.put_u32_le(symbol_len as u32);
        buf.put(self.symbol.as_bytes());

        // 写入时间戳
        buf.put_i64_le(self.timestamp);

        // 写入 bids (固定 n 档)
        for i in 0..n {
            let level = self.bids.get(i).copied().unwrap_or_default();
            buf.put_f64_le(level.price);
            buf.put_f64_le(level.amount);
        }

        // 写入 asks (固定 n 档)
        for i in 0..n {
            let level = self.asks.get(i).copied().unwrap_or_default();
            buf.put_f64_le(level.price);
            buf.put_f64_le(level.amount);
        }

        buf.freeze()
    }
}

/// Depth5 消息最大字节数
/// symbol 最长 20 字节: 4 + 4 + 20 + 8 + 5*16*2 = 196
pub const DEPTH5_MAX_BYTES: usize = 256;

/// Depth20 消息最大字节数
/// 4 + 4 + 20 + 8 + 20*16*2 = 676
pub const DEPTH20_MAX_BYTES: usize = 768;

/// Depth50 消息最大字节数
/// 4 + 4 + 20 + 8 + 50*16*2 = 1636
pub const DEPTH50_MAX_BYTES: usize = 2048;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_depth_msg() {
        let bids = vec![(100.0, 1.0), (99.0, 2.0)];
        let asks = vec![(101.0, 1.5), (102.0, 2.5)];

        let msg = DepthMsg::depth5("BTCUSDT".to_string(), 1234567890, bids, asks);

        assert_eq!(msg.msg_type, DepthMsgType::Depth5);
        assert_eq!(msg.depth_level(), 5);

        let bytes = msg.to_bytes();
        assert!(bytes.len() <= DEPTH5_MAX_BYTES);
    }
}
