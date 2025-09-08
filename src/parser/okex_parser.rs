use crate::mkt_msg::{SignalMsg, SignalSource, KlineMsg, LiquidationMsg, MarkPriceMsg, IndexPriceMsg, FundingRateMsg, TradeMsg, IncMsg, Level, AskBidSpreadMsg};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use tokio::sync::mpsc;
use std::collections::HashSet;
use crate::market_state::FundingRateManager;
use crate::exchange::Exchange;

#[derive(Clone)]
pub struct OkexSignalParser {
    source: SignalSource,
}

impl OkexSignalParser {
    pub fn new(is_ipc: bool) -> Self {
        Self {
            source: if is_ipc { SignalSource::Ipc } else { SignalSource::Tcp },
        }
    }
}

impl Parser for OkexSignalParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse OKEx depth message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Extract timestamp from data[0].ts
                if let Some(timestamp) = json_value
                    .get("data").and_then(|v| v.get(0))
                    .and_then(|item| item.get("ts"))
                    .and_then(|ts| ts.as_str())
                    .and_then(|s| s.parse::<i64>().ok()) {
                    
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
pub struct OkexKlineParser;

impl OkexKlineParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for OkexKlineParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse OKEx kline message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Extract symbol from arg.instId
                if let Some(symbol) = json_value
                    .get("arg")
                    .and_then(|arg| arg.get("instId"))
                    .and_then(|inst_id| inst_id.as_str()) {
                    
                    // Extract kline data from data array
                    if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
                        if let Some(kline_data) = data_array.first().and_then(|v| v.as_array()) {
                            if kline_data.len() >= 9 {
                                // 检查K线状态 - 只处理已完结的K线（状态为"1"）
                                if let Some(status) = kline_data[8].as_str() {
                                    if status != "1" {
                                        return 0; // 未完结的K线，不处理
                                    }
                                } else {
                                    return 0; // 状态字段无效
                                }
                                
                                // Parse kline data: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
                                if let (Some(ts_str), Some(o_str), Some(h_str), Some(l_str), Some(c_str), Some(vol_str)) = (
                                    kline_data[0].as_str(),
                                    kline_data[1].as_str(),
                                    kline_data[2].as_str(),
                                    kline_data[3].as_str(),
                                    kline_data[4].as_str(),
                                    kline_data[5].as_str(),
                                ) {
                                    // Parse all values
                                    if let (Ok(timestamp), Ok(open), Ok(high), Ok(low), Ok(close), Ok(volume)) = (
                                        ts_str.parse::<i64>(),
                                        o_str.parse::<f64>(),
                                        h_str.parse::<f64>(),
                                        l_str.parse::<f64>(),
                                        c_str.parse::<f64>(),
                                        vol_str.parse::<f64>(),
                                    ) {
                                        // Create kline message
                                        let kline_msg = KlineMsg::create(
                                            symbol.to_string(),
                                            open,
                                            high,
                                            low,
                                            close,
                                            volume,
                                            timestamp,
                                        );
                                        
                                        // Send kline message
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
        0
    }
}

#[derive(Clone)]
pub struct OkexDerivativesMetricsParser {
    symbols: HashSet<String>,
}

impl OkexDerivativesMetricsParser {
    pub fn new(symbols_set: HashSet<String>) -> Self {
        Self {
            symbols: symbols_set,
        }
    }
}

impl Parser for OkexDerivativesMetricsParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse OKEx derivatives metrics messages (liquidations + mark price + funding rate + index price)
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                if let Some(arg) = json_value.get("arg") {
                    if let Some(channel) = arg.get("channel").and_then(|v| v.as_str()) {
                        match channel {
                            "liquidation-orders" => return self.parse_liquidation_data(&json_value, tx),
                            "mark-price" => return self.parse_mark_price_data(&json_value, tx),
                            "funding-rate" => return self.parse_funding_rate_data(&json_value, tx),
                            "index-tickers" => return self.parse_index_price_data(&json_value, tx),
                            _ => return 0,
                        }
                    }
                }
            }
        }
        0
    }
}

impl OkexDerivativesMetricsParser {
    fn parse_liquidation_data(&self, json_value: &serde_json::Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse liquidation data array
        if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
            let mut parsed_count = 0;
            
            for data_item in data_array {
                if let Some(details_array) = data_item.get("details").and_then(|v| v.as_array()) {
                    // Extract symbol from instId
                    if let Some(inst_id) = data_item.get("instId").and_then(|v| v.as_str()) {
                        // Check if symbol is in the allowed list
                        if !self.symbols.contains(inst_id) {
                            continue;
                        }
                        
                        for detail in details_array {
                            if let (Some(side), Some(sz_str), Some(bk_px_str), Some(timestamp_str)) = (
                                detail.get("side").and_then(|v| v.as_str()),
                                detail.get("sz").and_then(|v| v.as_str()),
                                detail.get("bkPx").and_then(|v| v.as_str()),
                                detail.get("ts").and_then(|v| v.as_str()),
                            ) {
                                // Parse size, price and timestamp
                                if let (Ok(size), Ok(price), Ok(timestamp)) = (
                                    sz_str.parse::<f64>(),
                                    bk_px_str.parse::<f64>(),
                                    timestamp_str.parse::<i64>(),
                                ) {
                                    // Convert OKEx side to liquidation_side char
                                    let liquidation_side = match side {
                                        "buy" => 'B',   // 买入平仓
                                        "sell" => 'S',  // 卖出平仓
                                        _ => continue,
                                    };
                                    
                                    // Create liquidation message
                                    let liquidation_msg = LiquidationMsg::create(
                                        inst_id.to_string(),
                                        liquidation_side,
                                        size,
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
                    }
                }
            }
            
            return parsed_count;
        }
        0
    }
    
    fn parse_mark_price_data(&self, json_value: &serde_json::Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse mark price data array
        if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
            let mut parsed_count = 0;
            
            for data_item in data_array {
                if let (Some(inst_id), Some(mark_px_str), Some(timestamp_str)) = (
                    data_item.get("instId").and_then(|v| v.as_str()),
                    data_item.get("markPx").and_then(|v| v.as_str()),
                    data_item.get("ts").and_then(|v| v.as_str()),
                ) {
                    // Parse mark price and timestamp
                    if let (Ok(mark_price), Ok(timestamp)) = (
                        mark_px_str.parse::<f64>(),
                        timestamp_str.parse::<i64>(),
                    ) {
                        // Create mark price message
                        let mark_price_msg = MarkPriceMsg::create(
                            inst_id.to_string(),
                            mark_price,
                            timestamp,
                        );
                        
                        // Send mark price message
                        if tx.send(mark_price_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }
                    }
                }
            }
            
            return parsed_count;
        }
        0
    }
    
    fn parse_funding_rate_data(&self, json_value: &serde_json::Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse funding rate data array
        if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
            let mut parsed_count = 0;
            
            for data_item in data_array {
                if let (Some(inst_id), Some(funding_rate_str), Some(next_funding_time_str), Some(timestamp_str)) = (
                    data_item.get("instId").and_then(|v| v.as_str()),
                    data_item.get("fundingRate").and_then(|v| v.as_str()),
                    data_item.get("nextFundingTime").and_then(|v| v.as_str()),
                    data_item.get("ts").and_then(|v| v.as_str()),
                ) {
                    // Parse funding rate, next funding time and timestamp
                    if let (Ok(funding_rate), Ok(next_funding_time), Ok(timestamp)) = (
                        funding_rate_str.parse::<f64>(),
                        next_funding_time_str.parse::<i64>(),
                        timestamp_str.parse::<i64>(),
                    ) {
                        // Enrich with predicted rate and loan rate from manager
                        let rate_manager = FundingRateManager::instance();
                        // 使用完整的 OKX 合约ID（如 BTC-USDT-SWAP）并指定交易所为 OkexSwap
                        let rate_data = rate_manager.get_rates_sync(inst_id, Exchange::OkexSwap, timestamp);
                        let funding_rate_msg = FundingRateMsg::create(
                            inst_id.to_string(),
                            funding_rate,
                            next_funding_time,
                            timestamp,
                            rate_data.predicted_funding_rate,
                            rate_data.loan_rate_8h,
                        );
                        
                        // Send funding rate message
                        if tx.send(funding_rate_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }
                    }
                }
            }
            
            return parsed_count;
        }
        0
    }
    
    fn parse_index_price_data(&self, json_value: &serde_json::Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse index price data array
        if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
            let mut parsed_count = 0;
            
            for data_item in data_array {
                if let (Some(inst_id), Some(idx_px_str), Some(timestamp_str)) = (
                    data_item.get("instId").and_then(|v| v.as_str()),
                    data_item.get("idxPx").and_then(|v| v.as_str()),
                    data_item.get("ts").and_then(|v| v.as_str()),
                ) {
                    // Parse index price and timestamp
                    if let (Ok(index_price), Ok(timestamp)) = (
                        idx_px_str.parse::<f64>(),
                        timestamp_str.parse::<i64>(),
                    ) {
                        // Create index price message
                        let index_price_msg = IndexPriceMsg::create(
                            inst_id.to_string(),
                            index_price,
                            timestamp,
                        );
                        
                        // Send index price message
                        if tx.send(index_price_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }
                    }
                }
            }
            
            return parsed_count;
        }
        0
    }
}

#[derive(Clone)]
pub struct OkexTradeParser;

impl OkexTradeParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for OkexTradeParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse OKEx trade message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Check if this has data array
                if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
                    if !data_array.is_empty() {
                        return self.parse_trade_event(&data_array[0], tx);
                    }
                }
            }
        }
        0
    }
}

impl OkexTradeParser {
    fn parse_trade_event(&self, trade_data: &serde_json::Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Extract trade data from OKEx trade message
        if let (Some(symbol), Some(trade_id_str), Some(price_str), Some(size_str), Some(side_str), Some(timestamp_str)) = (
            trade_data.get("instId").and_then(|v| v.as_str()),       // 交易对
            trade_data.get("tradeId").and_then(|v| v.as_str()),      // 交易ID
            trade_data.get("px").and_then(|v| v.as_str()),           // 成交价格
            trade_data.get("sz").and_then(|v| v.as_str()),           // 成交数量
            trade_data.get("side").and_then(|v| v.as_str()),         // 买卖方向
            trade_data.get("ts").and_then(|v| v.as_str()),           // 时间戳
        ) {
            // Parse price, size, trade_id and timestamp
            if let (Ok(price), Ok(amount), Ok(trade_id), Ok(timestamp)) = (
                price_str.parse::<f64>(),
                size_str.parse::<f64>(),
                trade_id_str.parse::<i64>(),
                timestamp_str.parse::<i64>(),
            ) {
                // Filter out zero values
                if price <= 0.0 || amount <= 0.0 {
                    return 0;
                }
                
                // Convert OKEx side to char
                let side = match side_str {
                    "sell" => 'S',
                    "buy" => 'B',
                    _ => {
                        eprintln!("Unknown side: {}", side_str);
                        return 0;
                    }
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
        0
    }
}

#[derive(Clone)]
pub struct OkexAskBidSpreadParser;

impl OkexAskBidSpreadParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for OkexAskBidSpreadParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse OKEx bbo-tbt message (same format as books)
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Check if this is a bbo-tbt event
                if let Some(arg) = json_value.get("arg").and_then(|v| v.as_object()) {
                    if let Some(channel) = arg.get("channel").and_then(|v| v.as_str()) {
                        if channel == "bbo-tbt" {
                            // Parse data array
                            if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
                                for spread_data in data_array {
                                    // Extract spread data from OKEx bbo-tbt message
                                    if let (Some(symbol), Some(bids_array), Some(asks_array), Some(timestamp_str)) = (
                                        arg.get("instId").and_then(|v| v.as_str()),
                                        spread_data.get("bids").and_then(|v| v.as_array()),
                                        spread_data.get("asks").and_then(|v| v.as_array()),
                                        spread_data.get("ts").and_then(|v| v.as_str()),
                                    ) {
                                        // Parse timestamp
                                        let timestamp = timestamp_str.parse::<i64>().unwrap_or(0);
                                        
                                        // Parse best bid (first element)
                                        if let (Some(bid_item), Some(ask_item)) = (bids_array.first(), asks_array.first()) {
                                            if let (Some(bid_array), Some(ask_array)) = (bid_item.as_array(), ask_item.as_array()) {
                                                if bid_array.len() >= 2 && ask_array.len() >= 2 {
                                                    if let (Some(bid_price_str), Some(bid_amount_str), Some(ask_price_str), Some(ask_amount_str)) = (
                                                        bid_array[0].as_str(),
                                                        bid_array[1].as_str(),
                                                        ask_array[0].as_str(),
                                                        ask_array[1].as_str(),
                                                    ) {
                                                        // Parse prices and amounts
                                                        if let (Ok(bid_price), Ok(bid_amount), Ok(ask_price), Ok(ask_amount)) = (
                                                            bid_price_str.parse::<f64>(),
                                                            bid_amount_str.parse::<f64>(),
                                                            ask_price_str.parse::<f64>(),
                                                            ask_amount_str.parse::<f64>(),
                                                        ) {
                                                            // Filter out zero values
                                                            if bid_price <= 0.0 || bid_amount <= 0.0 || ask_price <= 0.0 || ask_amount <= 0.0 {
                                                                continue;
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
        }
        0
    }
}

// 公共函数：解析OKEx订单簿层级数据
fn parse_okex_order_book_levels(
    bids_array: &Vec<serde_json::Value>,
    asks_array: &Vec<serde_json::Value>,
    inc_msg: &mut IncMsg,
) {
    // 解析bids - OKEx格式：[price, amount, deprecated, order_count]
    for (i, bid_item) in bids_array.iter().enumerate() {
        if let Some(bid_array) = bid_item.as_array() {
            if bid_array.len() >= 2 {
                if let (Some(price_str), Some(amount_str)) = (
                    bid_array[0].as_str(),
                    bid_array[1].as_str(),
                ) {
                    let level = Level::new(price_str, amount_str);
                    inc_msg.set_bid_level(i, level);
                }
            }
        }
    }
    
    // 解析asks - OKEx格式：[price, amount, deprecated, order_count]
    for (i, ask_item) in asks_array.iter().enumerate() {
        if let Some(ask_array) = ask_item.as_array() {
            if ask_array.len() >= 2 {
                if let (Some(price_str), Some(amount_str)) = (
                    ask_array[0].as_str(),
                    ask_array[1].as_str(),
                ) {
                    let level = Level::new(price_str, amount_str);
                    inc_msg.set_ask_level(i, level);
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct OkexIncParser;

impl OkexIncParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for OkexIncParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // 解析OKEx增量/快照消息
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 检查是否是订单簿数据 - 通过arg.channel判断
                if let Some(arg) = json_value.get("arg") {
                    if let Some(channel) = arg.get("channel").and_then(|v| v.as_str()) {
                        if channel.starts_with("books") {
                            return self.parse_orderbook_event(&json_value, tx);
                        }
                    }
                }
            }
        }
        0
    }
}

impl OkexIncParser {
    fn parse_orderbook_event(&self, json_value: &serde_json::Value, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // 从OKEx订单簿数据中提取信息
        if let (Some(action), Some(data_array)) = (
            json_value.get("action").and_then(|v| v.as_str()),
            json_value.get("data").and_then(|v| v.as_array()),
        ) {
            if let Some(data) = data_array.first() {
                if let (Some(bids_array), Some(asks_array), Some(seq_id), Some(prev_seq_id), Some(timestamp_str)) = (
                    data.get("bids").and_then(|v| v.as_array()),
                    data.get("asks").and_then(|v| v.as_array()),
                    data.get("seqId").and_then(|v| v.as_i64()),
                    data.get("prevSeqId").and_then(|v| v.as_i64()),
                    data.get("ts").and_then(|v| v.as_str()),
                ) {
                    // 解析时间戳
                    let timestamp = match timestamp_str.parse::<i64>() {
                        Ok(ts) => ts,
                        Err(_) => return 0,
                    };
                    
                    // 从arg中获取symbol
                    let symbol = match json_value.get("arg")
                        .and_then(|arg| arg.get("instId"))
                        .and_then(|v| v.as_str()) {
                        Some(s) => s.to_string(),
                        _ => return 0,
                    };
                    
                    let bids_count = bids_array.len() as u32;
                    let asks_count = asks_array.len() as u32;
                    
                    // 判断是否为快照消息
                    let is_snapshot = action == "snapshot";
                    
                    // 创建增量/快照消息
                    let mut inc_msg = IncMsg::create(
                        symbol,
                        seq_id,             // first_update_id
                        prev_seq_id,        // final_update_id 
                        timestamp,          // 使用ts时间戳
                        is_snapshot,        // 根据action字段确定
                        bids_count,
                        asks_count,
                    );
                    
                    // 使用公共函数解析订单簿层级
                    parse_okex_order_book_levels(bids_array, asks_array, &mut inc_msg);
                    
                    // 发送消息
                    if tx.send(inc_msg.to_bytes()).is_ok() {
                        return 1;
                    }
                }
            }
        }
        0
    }
}
