//! 最优盘口价格维护模块
//! 
//! 该模块负责监听来自各交易所的 AskBidSpread 消息，
//! 维护每个交易对的最优买卖价格和数量。

use std::collections::HashMap;
use bytes::Bytes;
use crate::mkt_msg::{AskBidSpreadMsg, MktMsgType};
use crate::exchange::Exchange;

/// 最优盘口报价信息
#[derive(Debug, Clone)]
pub struct BestQuote {
    /// 交易对符号
    pub symbol: String,
    /// 交易所
    pub exchange: Exchange,
    /// 最优买价
    pub bid_price: f64,
    /// 最优买量
    pub bid_amount: f64,
    /// 最优卖价
    pub ask_price: f64,
    /// 最优卖量
    pub ask_amount: f64,
    /// 最后更新时间戳（毫秒）
    pub last_update_time: i64,
}

impl BestQuote {
    /// 创建新的最优报价
    pub fn new(
        symbol: String,
        exchange: Exchange,
        bid_price: f64,
        bid_amount: f64,
        ask_price: f64,
        ask_amount: f64,
        timestamp: i64,
    ) -> Self {
        Self {
            symbol,
            exchange,
            bid_price,
            bid_amount,
            ask_price,
            ask_amount,
            last_update_time: timestamp,
        }
    }

    /// 更新报价（仅当时间戳更新时）
    pub fn update(
        &mut self,
        bid_price: f64,
        bid_amount: f64,
        ask_price: f64,
        ask_amount: f64,
        timestamp: i64,
    ) -> bool {
        // 检查时间戳，如果是旧消息则丢弃
        if timestamp <= self.last_update_time && timestamp != 0 {
            return false;
        }

        // 更新数据
        self.bid_price = bid_price;
        self.bid_amount = bid_amount;
        self.ask_price = ask_price;
        self.ask_amount = ask_amount;
        self.last_update_time = timestamp;
        
        true
    }

    /// 计算买卖价差
    pub fn spread(&self) -> f64 {
        self.ask_price - self.bid_price
    }

    /// 计算买卖价差百分比
    pub fn spread_percentage(&self) -> f64 {
        if self.bid_price > 0.0 {
            (self.spread() / self.bid_price) * 100.0
        } else {
            0.0
        }
    }

    /// 计算中间价
    pub fn mid_price(&self) -> f64 {
        (self.bid_price + self.ask_price) / 2.0
    }
}

/// 最优盘口管理器
pub struct BestQuotesManager {
    /// 存储每个交易对的最优报价
    /// Key: (Exchange, Symbol)
    quotes: HashMap<(Exchange, String), BestQuote>,
    /// 统计信息
    stats: QuoteStats,
}

/// 统计信息
#[derive(Debug, Default)]
struct QuoteStats {
    /// 总接收消息数
    total_messages: u64,
    /// 丢弃的旧消息数
    dropped_old_messages: u64,
    /// 新创建的报价数
    new_quotes_created: u64,
    /// 更新的报价数
    quotes_updated: u64,
}

impl BestQuotesManager {
    /// 创建新的管理器
    pub fn new() -> Self {
        Self {
            quotes: HashMap::new(),
            stats: QuoteStats::default(),
        }
    }

    /// 处理 AskBidSpread 消息
    pub fn process_spread_message(&mut self, exchange: Exchange, data: &Bytes) -> Result<(), String> {
        // 解析消息
        let msg = Self::parse_spread_message(data)?;
        
        // 更新统计
        self.stats.total_messages += 1;

        // 获取或创建报价
        let key = (exchange, msg.symbol.clone());
        
        match self.quotes.get_mut(&key) {
            Some(quote) => {
                // 更新现有报价
                let updated = quote.update(
                    msg.bid_price,
                    msg.bid_amount,
                    msg.ask_price,
                    msg.ask_amount,
                    msg.timestamp,
                );
                
                if updated {
                    self.stats.quotes_updated += 1;
                } else {
                    self.stats.dropped_old_messages += 1;
                }
            }
            None => {
                // 创建新报价
                let quote = BestQuote::new(
                    msg.symbol.clone(),
                    exchange,
                    msg.bid_price,
                    msg.bid_amount,
                    msg.ask_price,
                    msg.ask_amount,
                    msg.timestamp,
                );
                
                self.quotes.insert(key, quote);
                self.stats.new_quotes_created += 1;
            }
        }
        
        Ok(())
    }

    /// 解析 AskBidSpread 消息
    fn parse_spread_message(data: &Bytes) -> Result<AskBidSpreadMsg, String> {
        if data.len() < 8 {
            return Err("Message too short".to_string());
        }

        // 读取消息类型和符号长度
        let msg_type = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        if msg_type != MktMsgType::AskBidSpread as u32 {
            return Err(format!("Invalid message type: {}", msg_type));
        }

        let symbol_length = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        
        // 检查消息长度
        let expected_len = 8 + symbol_length + 8 + 32; // header + symbol + timestamp + 4*f64
        if data.len() < expected_len {
            return Err(format!(
                "Message length mismatch: got {}, expected {}",
                data.len(),
                expected_len
            ));
        }

        // 读取符号
        let symbol = String::from_utf8(data[8..8 + symbol_length].to_vec())
            .map_err(|e| format!("Failed to parse symbol: {}", e))?;

        let offset = 8 + symbol_length;
        
        // 读取数据字段
        let timestamp = i64::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3],
            data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7],
        ]);
        
        let bid_price = f64::from_le_bytes([
            data[offset + 8], data[offset + 9], data[offset + 10], data[offset + 11],
            data[offset + 12], data[offset + 13], data[offset + 14], data[offset + 15],
        ]);
        
        let bid_amount = f64::from_le_bytes([
            data[offset + 16], data[offset + 17], data[offset + 18], data[offset + 19],
            data[offset + 20], data[offset + 21], data[offset + 22], data[offset + 23],
        ]);
        
        let ask_price = f64::from_le_bytes([
            data[offset + 24], data[offset + 25], data[offset + 26], data[offset + 27],
            data[offset + 28], data[offset + 29], data[offset + 30], data[offset + 31],
        ]);
        
        let ask_amount = f64::from_le_bytes([
            data[offset + 32], data[offset + 33], data[offset + 34], data[offset + 35],
            data[offset + 36], data[offset + 37], data[offset + 38], data[offset + 39],
        ]);

        Ok(AskBidSpreadMsg::create(
            symbol,
            timestamp,
            bid_price,
            bid_amount,
            ask_price,
            ask_amount,
        ))
    }

    /// 获取特定交易对的最优报价
    pub fn get_quote(&self, exchange: Exchange, symbol: &str) -> Option<BestQuote> {
        self.quotes.get(&(exchange, symbol.to_string())).cloned()
    }

    /// 获取所有报价
    pub fn get_all_quotes(&self) -> Vec<BestQuote> {
        self.quotes.values().cloned().collect()
    }

    /// 获取特定交易所的所有报价
    pub fn get_quotes_by_exchange(&self, exchange: Exchange) -> Vec<BestQuote> {
        self.quotes
            .iter()
            .filter(|((ex, _), _)| *ex == exchange)
            .map(|(_, quote)| quote.clone())
            .collect()
    }

    /// 获取特定交易对在所有交易所的报价
    pub fn get_quotes_by_symbol(&self, symbol: &str) -> Vec<BestQuote> {
        self.quotes
            .iter()
            .filter(|((_, sym), _)| sym == symbol)
            .map(|(_, quote)| quote.clone())
            .collect()
    }

    /// 获取统计信息
    pub fn get_stats(&self) -> String {
        format!(
            "BestQuotes Stats:\n\
             Active Quotes: {}\n\
             Total Messages: {}\n\
             Quotes Created: {}\n\
             Quotes Updated: {}\n\
             Dropped (Old): {}",
            self.quotes.len(),
            self.stats.total_messages,
            self.stats.new_quotes_created,
            self.stats.quotes_updated,
            self.stats.dropped_old_messages
        )
    }

    /// 重置统计信息
    pub fn reset_stats(&mut self) {
        self.stats = QuoteStats::default();
    }
}