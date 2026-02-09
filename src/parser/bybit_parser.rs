use crate::mkt_msg::{
    AskBidSpreadMsg, FundingRateMsg, IncMsg, IndexPriceMsg, KlineMsg, Level, LiquidationMsg,
    MarkPriceMsg, SignalMsg, SignalSource, TradeMsg,
};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct BybitSignalParser {
    source: SignalSource,
}

impl BybitSignalParser {
    pub fn new(is_ipc: bool) -> Self {
        Self {
            source: if is_ipc {
                SignalSource::Ipc
            } else {
                SignalSource::Tcp
            },
        }
    }
}

impl Parser for BybitSignalParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse Bybit depth message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Extract Bybit timestamp field "cts"
                if let Some(timestamp) = json_value.get("cts").and_then(|v| v.as_i64()) {
                    // Create signal message
                    let signal_msg = SignalMsg::create(self.source, timestamp);
                    let signal_bytes = signal_msg.to_bytes();

                    // Send signal
                    if tx.send(signal_bytes).is_ok() {
                        return 1;
                    }
                }
            }
        }
        0
    }
}

#[derive(Clone)]
pub struct BybitKlineParser;

impl BybitKlineParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BybitKlineParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse Bybit kline message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 检查是否是K线消息
                if let Some(topic) = json_value.get("topic").and_then(|v| v.as_str()) {
                    if topic.starts_with("kline.") {
                        // 检查data数组
                        if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array())
                        {
                            if let Some(kline_data) = data_array.first() {
                                // 检查confirm字段 - 只处理已确认的K线数据
                                if let Some(confirm) =
                                    kline_data.get("confirm").and_then(|v| v.as_bool())
                                {
                                    if confirm == false {
                                        return 0; // 未确认的K线，不处理
                                    }
                                } else {
                                    return 0; // confirm字段无效或缺失
                                }

                                // 从topic字段提取symbol
                                if let Some(symbol) = topic.split('.').last() {
                                    // 从kline_data对象中提取OHLCV数据
                                    if let (
                                        Some(open_str),
                                        Some(high_str),
                                        Some(low_str),
                                        Some(close_str),
                                        Some(volume_str),
                                        Some(timestamp),
                                    ) = (
                                        kline_data.get("open").and_then(|v| v.as_str()),
                                        kline_data.get("high").and_then(|v| v.as_str()),
                                        kline_data.get("low").and_then(|v| v.as_str()),
                                        kline_data.get("close").and_then(|v| v.as_str()),
                                        kline_data.get("volume").and_then(|v| v.as_str()),
                                        kline_data.get("timestamp").and_then(|v| v.as_i64()),
                                    ) {
                                        // 解析价格和成交量数据
                                        if let (
                                            Ok(open),
                                            Ok(high),
                                            Ok(low),
                                            Ok(close),
                                            Ok(volume),
                                        ) = (
                                            open_str.parse::<f64>(),
                                            high_str.parse::<f64>(),
                                            low_str.parse::<f64>(),
                                            close_str.parse::<f64>(),
                                            volume_str.parse::<f64>(),
                                        ) {
                                            // 将真实时间转换为opentime
                                            let closed_timestamp = (timestamp / 60000 - 1) * 60000;

                                            // 创建K线消息
                                            let kline_msg = KlineMsg::create(
                                                symbol.to_string(),
                                                open,
                                                high,
                                                low,
                                                close,
                                                volume,
                                                closed_timestamp,
                                            );

                                            // 发送K线消息
                                            if tx.send(kline_msg.to_bytes()).is_ok() {
                                                return 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        0
    }
}

#[derive(Clone)]
pub struct BybitDerivativesMetricsParser;

impl BybitDerivativesMetricsParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BybitDerivativesMetricsParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse Bybit derivatives metrics messages (liquidations + tickers)
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                if let Some(topic) = json_value.get("topic").and_then(|v| v.as_str()) {
                    // Route based on topic prefix
                    if topic.starts_with("allLiquidation.") {
                        return self.parse_liquidation_data(&json_value, tx);
                    } else if topic.starts_with("tickers.") {
                        return self.parse_ticker_data(&json_value, tx);
                    }
                }
            }
        }
        0
    }
}

impl BybitDerivativesMetricsParser {
    fn parse_liquidation_data(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        // Parse liquidation data array
        if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
            let mut parsed_count = 0;

            for liquidation_data in data_array {
                if let (
                    Some(symbol),
                    Some(side),
                    Some(volume_str),
                    Some(price_str),
                    Some(timestamp),
                ) = (
                    liquidation_data.get("s").and_then(|v| v.as_str()),
                    liquidation_data.get("S").and_then(|v| v.as_str()),
                    liquidation_data.get("v").and_then(|v| v.as_str()),
                    liquidation_data.get("p").and_then(|v| v.as_str()),
                    liquidation_data.get("T").and_then(|v| v.as_i64()),
                ) {
                    // Parse volume and price
                    if let (Ok(volume), Ok(price)) =
                        (volume_str.parse::<f64>(), price_str.parse::<f64>())
                    {
                        // Convert Bybit side to liquidation_side char
                        // Bybit: "Buy" = long liquidated, "Sell" = short liquidated
                        let liquidation_side = match side {
                            "Buy" => 'B',
                            "Sell" => 'S',
                            _ => continue,
                        };

                        // Create liquidation message
                        let liquidation_msg = LiquidationMsg::create(
                            symbol.to_string(),
                            liquidation_side,
                            volume,
                            price,
                            timestamp,
                        );

                        // Send liquidation message
                        if tx.send(liquidation_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }
                    }
                }
            }

            return parsed_count;
        }
        0
    }

    fn parse_ticker_data(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        // Parse ticker data - contains mark price, index price, and funding rate
        if let Some(data) = json_value.get("data") {
            if let (Some(symbol), Some(timestamp)) = (
                data.get("symbol").and_then(|v| v.as_str()),
                json_value.get("ts").and_then(|v| v.as_i64()),
            ) {
                let mut parsed_count = 0;

                // Parse mark price
                if let Some(mark_price_str) = data.get("markPrice").and_then(|v| v.as_str()) {
                    if let Ok(mark_price) = mark_price_str.parse::<f64>() {
                        let mark_price_msg =
                            MarkPriceMsg::create(symbol.to_string(), mark_price, timestamp);

                        if tx.send(mark_price_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }
                    }
                }

                // Parse index price
                if let Some(index_price_str) = data.get("indexPrice").and_then(|v| v.as_str()) {
                    if let Ok(index_price) = index_price_str.parse::<f64>() {
                        let index_price_msg =
                            IndexPriceMsg::create(symbol.to_string(), index_price, timestamp);

                        if tx.send(index_price_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }
                    }
                }

                // Parse funding rate
                if let (Some(funding_rate_str), Some(next_funding_time_str)) = (
                    data.get("fundingRate").and_then(|v| v.as_str()),
                    data.get("nextFundingTime").and_then(|v| v.as_str()),
                ) {
                    if let (Ok(funding_rate), Ok(next_funding_time)) = (
                        funding_rate_str.parse::<f64>(),
                        next_funding_time_str.parse::<i64>(),
                    ) {
                        // Build FundingRate message (prediction no longer embedded)
                        let funding_rate_msg = FundingRateMsg::create(
                            symbol.to_string(),
                            funding_rate,
                            next_funding_time,
                            timestamp,
                        );

                        if tx.send(funding_rate_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                            if symbol.to_ascii_uppercase().starts_with("BTC") {
                                log::debug!(
                                    "[Bybit][derivatives] funding_rate symbol={} rate={} next_time={} ts={}",
                                    symbol,
                                    funding_rate,
                                    next_funding_time,
                                    timestamp
                                );
                            }
                        }
                    }
                }

                return parsed_count;
            }
        }
        0
    }
}

// UUID处理辅助函数
fn hex_char_to_int(c: char) -> Result<u8, String> {
    match c {
        '0'..='9' => Ok(c as u8 - b'0'),
        'a'..='f' => Ok(10 + (c as u8 - b'a')),
        'A'..='F' => Ok(10 + (c as u8 - b'A')),
        _ => Err(format!("Invalid hex character: {}", c)),
    }
}

fn is_uuid_fast(s: &str) -> bool {
    // UUID 标准长度 36 字符（32 十六进制 + 4 短横线）
    if s.len() != 36 {
        return false;
    }

    // 检查短横线位置是否正确
    let chars: Vec<char> = s.chars().collect();
    chars[8] == '-' && chars[13] == '-' && chars[18] == '-' && chars[23] == '-'
}

fn is_numeric(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_digit())
}

// 解析 8 个十六进制字符 -> u64
fn parse_hex_u64(hex: &str) -> Result<u64, String> {
    if hex.len() < 8 {
        return Err("Hex string too short".to_string());
    }

    let mut result = 0u64;
    for (_i, c) in hex.chars().take(8).enumerate() {
        result = (result << 4) | (hex_char_to_int(c)? as u64);
    }
    Ok(result)
}

// 主函数：UUID -> int64_t（高低位混合）
fn uuid_to_int64_mixed(uuid: &str) -> Result<i64, String> {
    if uuid.len() < 36 {
        return Err("Invalid UUID format".to_string());
    }

    // 高位：前 8 字符（跳过第8位的 '-'）
    let high = parse_hex_u64(&uuid[0..8])?;

    // 低位：后 8 字符（从第24位开始，跳过版本标识）
    let low = parse_hex_u64(&uuid[24..32])?;

    // 混合高低位（异或减少冲突）
    Ok((high ^ low) as i64)
}

#[derive(Clone)]
pub struct BybitTradeParser;

impl BybitTradeParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BybitTradeParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse Bybit trade message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Check if this is a trade topic
                if let Some(topic) = json_value.get("topic").and_then(|v| v.as_str()) {
                    if topic.starts_with("publicTrade.") {
                        return self.parse_trade_event(&json_value, tx);
                    }
                }
            }
        }
        0
    }
}

impl BybitTradeParser {
    fn parse_trade_event(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        // Extract trade data from Bybit trade message
        // Bybit trade message format: data is an array with trade objects
        if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
            if let Some(trade_data) = data_array.first() {
                if let (
                    Some(symbol),
                    Some(side_str),
                    Some(price_str),
                    Some(volume_str),
                    Some(timestamp),
                    Some(id_str),
                ) = (
                    trade_data.get("s").and_then(|v| v.as_str()), // 交易对
                    trade_data.get("S").and_then(|v| v.as_str()), // 买卖方向
                    trade_data.get("p").and_then(|v| v.as_str()), // 成交价格
                    trade_data.get("v").and_then(|v| v.as_str()), // 成交数量
                    trade_data.get("T").and_then(|v| v.as_i64()), // 成交时间
                    trade_data.get("i").and_then(|v| v.as_str()), // 交易ID
                ) {
                    // Parse price and volume
                    if let (Ok(price), Ok(amount)) =
                        (price_str.parse::<f64>(), volume_str.parse::<f64>())
                    {
                        // Filter out zero values
                        if price <= 0.0 || amount <= 0.0 {
                            return 0;
                        }

                        // Convert Bybit side to char
                        let side = match side_str {
                            "Sell" => 'S',
                            "Buy" => 'B',
                            _ => {
                                eprintln!("Unknown side: {}", side_str);
                                return 0;
                            }
                        };

                        // Parse ID - could be UUID or numeric
                        let trade_id = if is_uuid_fast(id_str) {
                            match uuid_to_int64_mixed(id_str) {
                                Ok(id) => id,
                                Err(e) => {
                                    eprintln!("Failed to parse UUID {}: {}", id_str, e);
                                    return 0;
                                }
                            }
                        } else if is_numeric(id_str) {
                            match id_str.parse::<i64>() {
                                Ok(id) => id,
                                Err(e) => {
                                    eprintln!("Failed to parse numeric ID {}: {}", id_str, e);
                                    return 0;
                                }
                            }
                        } else {
                            eprintln!("Unknown ID format: {}", id_str);
                            return 0;
                        };

                        // Create trade message
                        let trade_msg = TradeMsg::create(
                            symbol.to_string(),
                            trade_id,
                            timestamp,
                            side,
                            price,
                            amount,
                        );

                        // Send trade message
                        if tx.send(trade_msg.to_bytes()).is_ok() {
                            return 1;
                        }
                    }
                }
            }
        }
        0
    }
}

#[derive(Clone)]
pub struct BybitAskBidSpreadParser;

impl BybitAskBidSpreadParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BybitAskBidSpreadParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse Bybit orderbook.1 message (same format as orderbook.500/200)
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Check if this is an orderbook.1 topic
                if let Some(topic) = json_value.get("topic").and_then(|v| v.as_str()) {
                    if topic.starts_with("orderbook.1.") {
                        // Extract symbol from topic (format: "orderbook.1.BTCUSDT")
                        let parts: Vec<&str> = topic.split('.').collect();
                        if parts.len() >= 3 {
                            let symbol = parts[2];

                            // Parse data
                            if let Some(data) = json_value.get("data").and_then(|v| v.as_object()) {
                                if let (Some(bids_array), Some(asks_array), Some(timestamp)) = (
                                    data.get("b").and_then(|v| v.as_array()),
                                    data.get("a").and_then(|v| v.as_array()),
                                    data.get("t").and_then(|v| v.as_i64()),
                                ) {
                                    // Parse best bid and ask (first element)
                                    if let (Some(bid_item), Some(ask_item)) =
                                        (bids_array.first(), asks_array.first())
                                    {
                                        if let (Some(bid_array), Some(ask_array)) =
                                            (bid_item.as_array(), ask_item.as_array())
                                        {
                                            if bid_array.len() >= 2 && ask_array.len() >= 2 {
                                                if let (
                                                    Some(bid_price_str),
                                                    Some(bid_amount_str),
                                                    Some(ask_price_str),
                                                    Some(ask_amount_str),
                                                ) = (
                                                    bid_array[0].as_str(),
                                                    bid_array[1].as_str(),
                                                    ask_array[0].as_str(),
                                                    ask_array[1].as_str(),
                                                ) {
                                                    // Parse prices and amounts
                                                    if let (
                                                        Ok(bid_price),
                                                        Ok(bid_amount),
                                                        Ok(ask_price),
                                                        Ok(ask_amount),
                                                    ) = (
                                                        bid_price_str.parse::<f64>(),
                                                        bid_amount_str.parse::<f64>(),
                                                        ask_price_str.parse::<f64>(),
                                                        ask_amount_str.parse::<f64>(),
                                                    ) {
                                                        // Filter out zero values
                                                        if bid_price <= 0.0
                                                            || bid_amount <= 0.0
                                                            || ask_price <= 0.0
                                                            || ask_amount <= 0.0
                                                        {
                                                            return 0;
                                                        }

                                                        // Create spread message
                                                        let spread_msg = AskBidSpreadMsg::create(
                                                            symbol.to_string(),
                                                            timestamp,
                                                            bid_price,
                                                            bid_amount,
                                                            ask_price,
                                                            ask_amount,
                                                        );

                                                        // Send message
                                                        if tx.send(spread_msg.to_bytes()).is_ok() {
                                                            return 1;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        0
    }
}

// 解析订单簿层级数据（支持偏移量）
fn parse_bybit_order_book_levels_with_offset(
    bids_array: &[serde_json::Value],
    asks_array: &[serde_json::Value],
    bids_start: usize,
    bids_count: usize,
    asks_start: usize,
    asks_count: usize,
    inc_msg: &mut IncMsg,
) {
    // 解析bids（从偏移量开始）
    for i in 0..bids_count {
        let src_idx = bids_start + i;
        if src_idx >= bids_array.len() {
            break;
        }
        if let Some(bid_array) = bids_array[src_idx].as_array() {
            if bid_array.len() >= 2 {
                if let (Some(price_str), Some(amount_str)) =
                    (bid_array[0].as_str(), bid_array[1].as_str())
                {
                    let level = Level::new(price_str, amount_str);
                    inc_msg.set_bid_level(i, level);
                }
            }
        }
    }

    // 解析asks（从偏移量开始）
    for i in 0..asks_count {
        let src_idx = asks_start + i;
        if src_idx >= asks_array.len() {
            break;
        }
        if let Some(ask_array) = asks_array[src_idx].as_array() {
            if ask_array.len() >= 2 {
                if let (Some(price_str), Some(amount_str)) =
                    (ask_array[0].as_str(), ask_array[1].as_str())
                {
                    let level = Level::new(price_str, amount_str);
                    inc_msg.set_ask_level(i, level);
                }
            }
        }
    }
}

/// 计算如何拆分 levels 成多个 chunk
fn split_levels(
    total_bids: usize,
    total_asks: usize,
    max_levels: Option<usize>,
) -> Vec<(usize, usize, usize, usize)> {
    let total = total_bids + total_asks;

    match max_levels {
        Some(max) if total > max && max > 0 => {
            let mut chunks = Vec::new();
            let mut bids_sent = 0;
            let mut asks_sent = 0;

            while bids_sent < total_bids || asks_sent < total_asks {
                let bids_remaining = total_bids - bids_sent;
                let asks_remaining = total_asks - asks_sent;
                let remaining = bids_remaining + asks_remaining;

                let chunk_bids = if remaining <= max {
                    bids_remaining
                } else {
                    let ratio = bids_remaining as f64 / remaining as f64;
                    ((max as f64 * ratio).round() as usize)
                        .max(1)
                        .min(bids_remaining)
                };
                let chunk_asks = (max - chunk_bids).min(asks_remaining);

                chunks.push((bids_sent, chunk_bids, asks_sent, chunk_asks));
                bids_sent += chunk_bids;
                asks_sent += chunk_asks;
            }

            chunks
        }
        _ => vec![(0, total_bids, 0, total_asks)],
    }
}

#[derive(Clone)]
pub struct BybitIncParser {
    max_levels: Option<usize>,
}

impl BybitIncParser {
    pub fn new() -> Self {
        Self { max_levels: None }
    }

    pub fn with_max_levels(max_levels: Option<usize>) -> Self {
        Self { max_levels }
    }
}

impl Parser for BybitIncParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // 解析Bybit增量/快照消息
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 检查是否是订单簿数据
                if let Some(topic) = json_value.get("topic").and_then(|v| v.as_str()) {
                    if topic.starts_with("orderbook.") {
                        return self.parse_orderbook_event(&json_value, tx);
                    }
                }
            }
        }
        0
    }
}

impl BybitIncParser {
    fn parse_orderbook_event(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        // 从Bybit订单簿数据中提取信息
        if let (Some(msg_type), Some(timestamp), Some(data)) = (
            json_value.get("type").and_then(|v| v.as_str()),
            json_value.get("cts").and_then(|v| v.as_i64()),
            json_value.get("data"),
        ) {
            if let (Some(symbol), Some(update_id), Some(bids_array), Some(asks_array)) = (
                data.get("s").and_then(|v| v.as_str()),
                data.get("u").and_then(|v| v.as_i64()),
                data.get("b").and_then(|v| v.as_array()),
                data.get("a").and_then(|v| v.as_array()),
            ) {
                // 判断是否为快照消息
                let is_snapshot = match msg_type {
                    "snapshot" => true,
                    "delta" => false,
                    _ => return 0,
                };

                // 计算拆分方案
                let chunks = split_levels(bids_array.len(), asks_array.len(), self.max_levels);
                let total_chunks = chunks.len();
                let mut sent_count = 0;

                for (chunk_idx, (bids_start, bids_count, asks_start, asks_count)) in
                    chunks.into_iter().enumerate()
                {
                    // 创建增量/快照消息
                    let mut inc_msg = IncMsg::create(
                        symbol.to_string(),
                        update_id,   // first_update_id
                        update_id,   // final_update_id (Bybit两者相同)
                        timestamp,   // 使用cts时间戳
                        is_snapshot, // 根据type字段确定
                        bids_count as u32,
                        asks_count as u32,
                    );

                    // 设置 chunk_index 和 is_last
                    inc_msg.set_chunk_index(chunk_idx as u8);
                    inc_msg.set_is_last(chunk_idx == total_chunks - 1);

                    // 解析订单簿层级（带偏移量）
                    parse_bybit_order_book_levels_with_offset(
                        bids_array,
                        asks_array,
                        bids_start,
                        bids_count,
                        asks_start,
                        asks_count,
                        &mut inc_msg,
                    );

                    // 发送消息
                    if tx.send(inc_msg.to_bytes()).is_ok() {
                        sent_count += 1;
                    }
                }
                return sent_count;
            }
        }
        0
    }
}
