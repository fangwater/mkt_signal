use crate::mkt_msg::{
    AskBidSpreadMsg, FundingRateMsg, IncMsg, IndexPriceMsg, KlineMsg, Level, LiquidationMsg,
    MarkPriceMsg, SignalMsg, SignalSource, TradeMsg,
};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use std::collections::HashSet;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct BinanceSignalParser {
    source: SignalSource,
}

impl BinanceSignalParser {
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

impl Parser for BinanceSignalParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse Binance depth message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Extract Binance timestamp field "E"
                if let Some(timestamp) = json_value.get("E").and_then(|v| v.as_i64()) {
                    // Create signal message
                    let signal_msg = SignalMsg::create(self.source, timestamp);
                    let signal_bytes = signal_msg.to_bytes();

                    // Send signal
                    if let Err(_) = tx.send(signal_bytes) {
                        return 0;
                    }

                    return 1;
                }
            }
        }
        0
    }
}

#[derive(Clone)]
pub struct BinanceKlineParser;

impl BinanceKlineParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BinanceKlineParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse Binance kline message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 从顶层s字段直接获取symbol
                if let Some(symbol) = json_value.get("s").and_then(|v| v.as_str()) {
                    // 获取k对象中的K线数据
                    if let Some(kline_obj) = json_value.get("k") {
                        // 检查x字段 - 只处理已关闭的K线
                        if let Some(is_closed) = kline_obj.get("x").and_then(|v| v.as_bool()) {
                            if !is_closed {
                                return 0; // K线未关闭，不处理
                            }
                        } else {
                            return 0; // x字段无效或缺失
                        }

                        // 从k对象中提取OHLCV数据
                        if let (
                            Some(open_str),
                            Some(high_str),
                            Some(low_str),
                            Some(close_str),
                            Some(volume_str),
                            Some(timestamp),
                        ) = (
                            kline_obj.get("o").and_then(|v| v.as_str()),
                            kline_obj.get("h").and_then(|v| v.as_str()),
                            kline_obj.get("l").and_then(|v| v.as_str()),
                            kline_obj.get("c").and_then(|v| v.as_str()),
                            kline_obj.get("v").and_then(|v| v.as_str()),
                            kline_obj.get("t").and_then(|v| v.as_i64()),
                        ) {
                            // 解析价格和成交量数据
                            if let (Ok(open), Ok(high), Ok(low), Ok(close), Ok(volume)) = (
                                open_str.parse::<f64>(),
                                high_str.parse::<f64>(),
                                low_str.parse::<f64>(),
                                close_str.parse::<f64>(),
                                volume_str.parse::<f64>(),
                            ) {
                                // 创建K线消息
                                let kline_msg = KlineMsg::create(
                                    symbol.to_string(),
                                    open,
                                    high,
                                    low,
                                    close,
                                    volume,
                                    timestamp,
                                );

                                // 发送K线消息
                                let kline_bytes = kline_msg.to_bytes();
                                if tx.send(kline_bytes).is_ok() {
                                    return 1;
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
pub struct BinanceDerivativesMetricsParser {
    symbols: HashSet<String>,
}

impl BinanceDerivativesMetricsParser {
    pub fn new(symbols_set: HashSet<String>) -> Self {
        Self {
            // Binance WS symbols are uppercase (e.g. "BTCUSDT"), while this parser uses
            // lowercase keys for lookups.
            symbols: symbols_set.into_iter().map(|s| s.to_lowercase()).collect(),
        }
    }
}

impl Parser for BinanceDerivativesMetricsParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse Binance derivatives metrics messages (liquidations + mark price)
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Handle mark price array format: [{e: "markPriceUpdate", ...}, ...]
                if let Some(data_array) = json_value.as_array() {
                    return self.parse_mark_price_array(data_array, tx);
                }

                // Handle single liquidation event format: {e: "forceOrder", ...}
                if let Some(event_type) = json_value.get("e").and_then(|v| v.as_str()) {
                    match event_type {
                        "forceOrder" => return self.parse_liquidation_event(&json_value, tx),
                        "markPriceUpdate" => return self.parse_single_mark_price(&json_value, tx),
                        _ => return 0,
                    }
                }
            }
        }
        0
    }
}

impl BinanceDerivativesMetricsParser {
    fn parse_liquidation_event(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        // Parse liquidation order data
        if let Some(order_data) = json_value.get("o") {
            if let (
                Some(symbol),
                Some(side),
                Some(quantity_str),
                Some(avg_price_str),
                Some(timestamp),
            ) = (
                order_data.get("s").and_then(|v| v.as_str()),
                order_data.get("S").and_then(|v| v.as_str()),
                order_data.get("z").and_then(|v| v.as_str()), // Order Filled Accumulated Quantity
                order_data.get("ap").and_then(|v| v.as_str()), // Average Price
                order_data.get("T").and_then(|v| v.as_i64()), // Order Trade Time
            ) {
                // Check if symbol is in the allowed list (case-insensitive)
                let symbol_lower = symbol.to_lowercase();
                if !self.symbols.contains(&symbol_lower) {
                    return 0;
                }
                // Parse quantity and price
                if let (Ok(quantity), Ok(avg_price)) =
                    (quantity_str.parse::<f64>(), avg_price_str.parse::<f64>())
                {
                    // Convert Binance side to liquidation_side char
                    let liquidation_side = match side {
                        "BUY" => 'B',  // 买入强平
                        "SELL" => 'S', // 卖出强平
                        _ => return 0,
                    };

                    // Create liquidation message
                    let liquidation_msg = LiquidationMsg::create(
                        symbol.to_string(),
                        liquidation_side,
                        quantity,
                        avg_price,
                        timestamp,
                    );

                    // Send liquidation message
                    if tx.send(liquidation_msg.to_bytes()).is_ok() {
                        return 1;
                    }
                }
            }
        }
        0
    }

    fn parse_mark_price_array(
        &self,
        data_array: &Vec<serde_json::Value>,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let mut total_parsed = 0;

        for item in data_array {
            total_parsed += self.parse_single_mark_price(item, tx);
        }
        total_parsed
    }

    fn parse_single_mark_price(
        &self,
        item: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        // Check if this is a markPriceUpdate event
        if let Some(event_type) = item.get("e").and_then(|v| v.as_str()) {
            if event_type == "markPriceUpdate" {
                if let (
                    Some(symbol),
                    Some(mark_price_str),
                    Some(index_price_str),
                    Some(funding_rate_str),
                    Some(event_time),
                    Some(next_funding_time),
                ) = (
                    item.get("s").and_then(|v| v.as_str()),
                    item.get("p").and_then(|v| v.as_str()),
                    item.get("i").and_then(|v| v.as_str()),
                    item.get("r").and_then(|v| v.as_str()),
                    item.get("E").and_then(|v| v.as_i64()),
                    item.get("T").and_then(|v| v.as_i64()),
                ) {
                    // Check if symbol is in the allowed list (case-insensitive)
                    let symbol_lower = symbol.to_lowercase();
                    if !self.symbols.contains(&symbol_lower) {
                        return 0;
                    }
                    // Parse price values
                    if let (Ok(mark_price), Ok(index_price), Ok(funding_rate)) = (
                        mark_price_str.parse::<f64>(),
                        index_price_str.parse::<f64>(),
                        funding_rate_str.parse::<f64>(),
                    ) {
                        let mut parsed_count = 0;

                        let s_lower = symbol.to_lowercase();
                        if matches!(s_lower.as_str(), "btcusdt" | "ethusdt" | "bnbusdt") {
                            log::info!(
                                "binance funding msg: symbol={} mark={} index={} funding={} next={} event={}",
                                symbol,
                                mark_price,
                                index_price,
                                funding_rate,
                                next_funding_time,
                                event_time
                            );
                        }

                        // Create and send MarkPriceMsg
                        let mark_price_msg =
                            MarkPriceMsg::create(symbol.to_string(), mark_price, event_time);
                        if tx.send(mark_price_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }

                        // Create and send IndexPriceMsg
                        let index_price_msg =
                            IndexPriceMsg::create(symbol.to_string(), index_price, event_time);
                        if tx.send(index_price_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                        }

                        // Create and send FundingRateMsg
                        let funding_rate_msg = FundingRateMsg::create(
                            symbol.to_string(),
                            funding_rate,
                            next_funding_time,
                            event_time,
                        );
                        if tx.send(funding_rate_msg.to_bytes()).is_ok() {
                            parsed_count += 1;
                            if matches!(s_lower.as_str(), "btcusdt" | "ethusdt" | "bnbusdt") {
                                log::info!(
                                    "mkt pub funding_rate_msg: symbol={} funding={} next={} event={}",
                                    symbol,
                                    funding_rate,
                                    next_funding_time,
                                    event_time
                                );
                            }
                        }

                        return parsed_count;
                    }
                }
            }
        }
        0
    }
}

#[derive(Clone)]
pub struct BinanceSnapshotParser {
    max_levels: Option<usize>,
}

impl BinanceSnapshotParser {
    pub fn new() -> Self {
        Self { max_levels: None }
    }

    pub fn with_max_levels(max_levels: Option<usize>) -> Self {
        Self { max_levels }
    }
}

impl Parser for BinanceSnapshotParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // 解析币安快照消息
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                return self.parse_snapshot_event(&json_value, tx);
            }
        }
        0
    }
}

// 公共函数：解析订单簿层级数据（支持偏移量）
fn parse_order_book_levels_with_offset(
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

// 公共函数：解析订单簿层级数据（f64 pairs, 支持偏移量）
fn parse_order_book_levels_from_pairs(
    bids: &[(f64, f64)],
    asks: &[(f64, f64)],
    bids_start: usize,
    bids_count: usize,
    asks_start: usize,
    asks_count: usize,
    inc_msg: &mut IncMsg,
) {
    for i in 0..bids_count {
        let src_idx = bids_start + i;
        if src_idx >= bids.len() {
            break;
        }
        let (price, amount) = bids[src_idx];
        inc_msg.set_bid_level(i, Level::from_values(price, amount));
    }

    for i in 0..asks_count {
        let src_idx = asks_start + i;
        if src_idx >= asks.len() {
            break;
        }
        let (price, amount) = asks[src_idx];
        inc_msg.set_ask_level(i, Level::from_values(price, amount));
    }
}

/// 计算如何拆分 levels 成多个 chunk
/// 返回 Vec<(bids_start, bids_count, asks_start, asks_count)>
/// 每个 chunk 的总档数不超过 max_levels
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

                // 按比例分配本次 chunk 的 bids 和 asks
                let chunk_bids = if remaining <= max {
                    bids_remaining
                } else {
                    // 按原始比例分配
                    let ratio = bids_remaining as f64 / remaining as f64;
                    ((max as f64 * ratio).round() as usize).max(1).min(bids_remaining)
                };
                let chunk_asks = (max - chunk_bids).min(asks_remaining);

                chunks.push((bids_sent, chunk_bids, asks_sent, chunk_asks));
                bids_sent += chunk_bids;
                asks_sent += chunk_asks;
            }

            chunks
        }
        _ => {
            // 不需要拆分，返回单个 chunk
            vec![(0, total_bids, 0, total_asks)]
        }
    }
}

#[derive(Clone)]
pub struct BinanceSbeDepthSnapshotParser {
    max_levels: Option<usize>,
}

impl BinanceSbeDepthSnapshotParser {
    pub fn with_max_levels(max_levels: Option<usize>) -> Self {
        Self { max_levels }
    }

    fn parse_snapshot(
        &self,
        msg: &[u8],
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let header = match read_sbe_header(msg) {
            Some(h) => h,
            None => return 0,
        };
        if header.template_id != 10002 {
            return 0;
        }

        let base = header.body_offset;
        if msg.len() < base + header.block_length {
            return 0;
        }

        let event_time = match read_i64_le(msg, base) {
            Some(v) => v,
            None => return 0,
        };
        let book_update_id = match read_i64_le(msg, base + 8) {
            Some(v) => v,
            None => return 0,
        };
        let price_exponent = match read_i8(msg, base + 16) {
            Some(v) => v,
            None => return 0,
        };
        let qty_exponent = match read_i8(msg, base + 17) {
            Some(v) => v,
            None => return 0,
        };

        let mut offset = base + header.block_length;
        let (bids, next_offset) =
            match read_group_levels(msg, offset, price_exponent, qty_exponent) {
                Some(v) => v,
                None => return 0,
            };
        offset = next_offset;
        let (asks, next_offset) =
            match read_group_levels(msg, offset, price_exponent, qty_exponent) {
                Some(v) => v,
                None => return 0,
            };
        offset = next_offset;

        let symbol = match read_var_string8(msg, offset) {
            Some((s, _)) => s.to_uppercase(),
            None => return 0,
        };

        // SBE timestamps are in microseconds; keep ms alignment with other parsers.
        let timestamp = event_time / 1000;
        let chunks = split_levels(bids.len(), asks.len(), self.max_levels);
        let total_chunks = chunks.len();
        let mut sent_count = 0;

        for (chunk_idx, (bids_start, bids_count, asks_start, asks_count)) in
            chunks.into_iter().enumerate()
        {
            let mut inc_msg = IncMsg::create(
                symbol.clone(),
                book_update_id,
                book_update_id,
                timestamp,
                true,
                bids_count as u32,
                asks_count as u32,
            );

            inc_msg.set_chunk_index(chunk_idx as u8);
            inc_msg.set_is_last(chunk_idx == total_chunks - 1);

            parse_order_book_levels_from_pairs(
                &bids,
                &asks,
                bids_start,
                bids_count,
                asks_start,
                asks_count,
                &mut inc_msg,
            );

            if tx.send(inc_msg.to_bytes()).is_ok() {
                sent_count += 1;
            }
        }

        sent_count
    }
}

impl Parser for BinanceSbeDepthSnapshotParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if msg.is_empty() {
            return 0;
        }
        if msg[0] == b'{' || msg[0] == b'[' {
            return 0;
        }
        self.parse_snapshot(&msg, tx)
    }
}

#[derive(Clone)]
pub struct BinanceSbeDepthDiffParser {
    max_levels: Option<usize>,
}

impl BinanceSbeDepthDiffParser {
    pub fn with_max_levels(max_levels: Option<usize>) -> Self {
        Self { max_levels }
    }

    fn parse_diff(&self, msg: &[u8], tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let header = match read_sbe_header(msg) {
            Some(h) => h,
            None => return 0,
        };
        if header.template_id != 10003 {
            return 0;
        }

        let base = header.body_offset;
        if msg.len() < base + header.block_length {
            return 0;
        }

        let event_time = match read_i64_le(msg, base) {
            Some(v) => v,
            None => return 0,
        };
        let first_update_id = match read_i64_le(msg, base + 8) {
            Some(v) => v,
            None => return 0,
        };
        let last_update_id = match read_i64_le(msg, base + 16) {
            Some(v) => v,
            None => return 0,
        };
        let price_exponent = match read_i8(msg, base + 24) {
            Some(v) => v,
            None => return 0,
        };
        let qty_exponent = match read_i8(msg, base + 25) {
            Some(v) => v,
            None => return 0,
        };

        let mut offset = base + header.block_length;
        let (bids, next_offset) =
            match read_group_levels(msg, offset, price_exponent, qty_exponent) {
                Some(v) => v,
                None => return 0,
            };
        offset = next_offset;
        let (asks, next_offset) =
            match read_group_levels(msg, offset, price_exponent, qty_exponent) {
                Some(v) => v,
                None => return 0,
            };
        offset = next_offset;

        let symbol = match read_var_string8(msg, offset) {
            Some((s, _)) => s.to_uppercase(),
            None => return 0,
        };

        let timestamp = event_time / 1000;
        let chunks = split_levels(bids.len(), asks.len(), self.max_levels);
        let total_chunks = chunks.len();
        let mut sent_count = 0;

        for (chunk_idx, (bids_start, bids_count, asks_start, asks_count)) in
            chunks.into_iter().enumerate()
        {
            let mut inc_msg = IncMsg::create(
                symbol.clone(),
                first_update_id,
                last_update_id,
                timestamp,
                false,
                bids_count as u32,
                asks_count as u32,
            );

            inc_msg.set_chunk_index(chunk_idx as u8);
            inc_msg.set_is_last(chunk_idx == total_chunks - 1);

            parse_order_book_levels_from_pairs(
                &bids,
                &asks,
                bids_start,
                bids_count,
                asks_start,
                asks_count,
                &mut inc_msg,
            );

            if tx.send(inc_msg.to_bytes()).is_ok() {
                sent_count += 1;
            }
        }

        sent_count
    }
}

impl Parser for BinanceSbeDepthDiffParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if msg.is_empty() {
            return 0;
        }
        if msg[0] == b'{' || msg[0] == b'[' {
            return 0;
        }
        self.parse_diff(&msg, tx)
    }
}

#[derive(Clone)]
pub struct BinanceSbeBestBidAskParser;

impl BinanceSbeBestBidAskParser {
    pub fn new() -> Self {
        Self
    }

    fn parse_bbo(&self, msg: &[u8], tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let header = match read_sbe_header(msg) {
            Some(h) => h,
            None => return 0,
        };
        if header.template_id != 10001 {
            return 0;
        }

        let base = header.body_offset;
        if msg.len() < base + header.block_length {
            return 0;
        }

        let event_time = match read_i64_le(msg, base) {
            Some(v) => v,
            None => return 0,
        };
        let _book_update_id = match read_i64_le(msg, base + 8) {
            Some(v) => v,
            None => return 0,
        };
        let price_exponent = match read_i8(msg, base + 16) {
            Some(v) => v,
            None => return 0,
        };
        let qty_exponent = match read_i8(msg, base + 17) {
            Some(v) => v,
            None => return 0,
        };
        let bid_price = match read_i64_le(msg, base + 18) {
            Some(v) => v,
            None => return 0,
        };
        let bid_qty = match read_i64_le(msg, base + 26) {
            Some(v) => v,
            None => return 0,
        };
        let ask_price = match read_i64_le(msg, base + 34) {
            Some(v) => v,
            None => return 0,
        };
        let ask_qty = match read_i64_le(msg, base + 42) {
            Some(v) => v,
            None => return 0,
        };

        let symbol = match read_var_string8(msg, base + header.block_length) {
            Some((s, _)) => s.to_uppercase(),
            None => return 0,
        };

        let timestamp = event_time / 1000;
        let bid_price = scale_mantissa(bid_price, price_exponent);
        let bid_amount = scale_mantissa(bid_qty, qty_exponent);
        let ask_price = scale_mantissa(ask_price, price_exponent);
        let ask_amount = scale_mantissa(ask_qty, qty_exponent);

        if bid_price <= 0.0 || bid_amount <= 0.0 || ask_price <= 0.0 || ask_amount <= 0.0 {
            return 0;
        }

        let spread_msg = AskBidSpreadMsg::create(
            symbol,
            timestamp,
            bid_price,
            bid_amount,
            ask_price,
            ask_amount,
        );
        if tx.send(spread_msg.to_bytes()).is_ok() {
            return 1;
        }
        0
    }
}

impl Parser for BinanceSbeBestBidAskParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if msg.is_empty() {
            return 0;
        }
        if msg[0] == b'{' || msg[0] == b'[' {
            return 0;
        }
        self.parse_bbo(&msg, tx)
    }
}

#[derive(Clone)]
pub struct BinanceSbeTradeParser;

impl BinanceSbeTradeParser {
    pub fn new() -> Self {
        Self
    }

    fn parse_trades(&self, msg: &[u8], tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let header = match read_sbe_header(msg) {
            Some(h) => h,
            None => return 0,
        };
        if header.template_id != 10000 {
            return 0;
        }

        let base = header.body_offset;
        if msg.len() < base + header.block_length {
            return 0;
        }

        let event_time = match read_i64_le(msg, base) {
            Some(v) => v,
            None => return 0,
        };
        let _transact_time = match read_i64_le(msg, base + 8) {
            Some(v) => v,
            None => return 0,
        };
        let price_exponent = match read_i8(msg, base + 16) {
            Some(v) => v,
            None => return 0,
        };
        let qty_exponent = match read_i8(msg, base + 17) {
            Some(v) => v,
            None => return 0,
        };

        let mut offset = base + header.block_length;
        if msg.len() < offset + 6 {
            return 0;
        }
        let block_length = match read_u16_le(msg, offset) {
            Some(v) => v as usize,
            None => return 0,
        };
        let num_in_group = match read_u32_le(msg, offset + 2) {
            Some(v) => v as usize,
            None => return 0,
        };
        offset += 6;

        let mut trades = Vec::with_capacity(num_in_group);
        for _ in 0..num_in_group {
            if msg.len() < offset + block_length || block_length < 25 {
                break;
            }
            let trade_id = match read_i64_le(msg, offset) {
                Some(v) => v,
                None => break,
            };
            let price = match read_i64_le(msg, offset + 8) {
                Some(v) => v,
                None => break,
            };
            let qty = match read_i64_le(msg, offset + 16) {
                Some(v) => v,
                None => break,
            };
            let is_buyer_maker = msg.get(offset + 24).copied().unwrap_or(0) != 0;
            trades.push((trade_id, price, qty, is_buyer_maker));
            offset += block_length;
        }

        let symbol = match read_var_string8(msg, offset) {
            Some((s, _)) => s.to_uppercase(),
            None => return 0,
        };

        let timestamp = event_time / 1000;
        let mut sent_count = 0;

        for (trade_id, price, qty, is_buyer_maker) in trades {
            let price = scale_mantissa(price, price_exponent);
            let amount = scale_mantissa(qty, qty_exponent);
            if price <= 0.0 || amount <= 0.0 {
                continue;
            }

            let side = if is_buyer_maker { 'S' } else { 'B' };
            let trade_msg =
                TradeMsg::create(symbol.clone(), trade_id, timestamp, side, price, amount);
            if tx.send(trade_msg.to_bytes()).is_ok() {
                sent_count += 1;
            }
        }

        sent_count
    }
}

impl Parser for BinanceSbeTradeParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if msg.is_empty() {
            return 0;
        }
        if msg[0] == b'{' || msg[0] == b'[' {
            return 0;
        }
        self.parse_trades(&msg, tx)
    }
}

struct SbeHeader {
    block_length: usize,
    template_id: u16,
    body_offset: usize,
}

fn read_sbe_header(msg: &[u8]) -> Option<SbeHeader> {
    if msg.len() < 8 {
        return None;
    }
    let block_length = read_u16_le(msg, 0)? as usize;
    let template_id = read_u16_le(msg, 2)?;
    Some(SbeHeader {
        block_length,
        template_id,
        body_offset: 8,
    })
}

fn read_u16_le(msg: &[u8], offset: usize) -> Option<u16> {
    if msg.len() < offset + 2 {
        return None;
    }
    Some(u16::from_le_bytes([msg[offset], msg[offset + 1]]))
}

fn read_u32_le(msg: &[u8], offset: usize) -> Option<u32> {
    if msg.len() < offset + 4 {
        return None;
    }
    Some(u32::from_le_bytes([
        msg[offset],
        msg[offset + 1],
        msg[offset + 2],
        msg[offset + 3],
    ]))
}

fn read_i64_le(msg: &[u8], offset: usize) -> Option<i64> {
    if msg.len() < offset + 8 {
        return None;
    }
    Some(i64::from_le_bytes([
        msg[offset],
        msg[offset + 1],
        msg[offset + 2],
        msg[offset + 3],
        msg[offset + 4],
        msg[offset + 5],
        msg[offset + 6],
        msg[offset + 7],
    ]))
}

fn read_i8(msg: &[u8], offset: usize) -> Option<i8> {
    msg.get(offset).map(|v| *v as i8)
}

fn scale_mantissa(mantissa: i64, exponent: i8) -> f64 {
    let factor = 10_f64.powi(exponent as i32);
    (mantissa as f64) * factor
}

fn read_group_levels(
    msg: &[u8],
    offset: usize,
    price_exponent: i8,
    qty_exponent: i8,
) -> Option<(Vec<(f64, f64)>, usize)> {
    if msg.len() < offset + 4 {
        return None;
    }
    let block_length = read_u16_le(msg, offset)? as usize;
    let num_in_group = read_u16_le(msg, offset + 2)? as usize;
    let mut pos = offset + 4;
    let mut levels = Vec::with_capacity(num_in_group);

    for _ in 0..num_in_group {
        if msg.len() < pos + block_length || block_length < 16 {
            break;
        }
        let price = read_i64_le(msg, pos)?;
        let qty = read_i64_le(msg, pos + 8)?;
        levels.push((scale_mantissa(price, price_exponent), scale_mantissa(qty, qty_exponent)));
        pos += block_length;
    }

    Some((levels, pos))
}

fn read_var_string8(msg: &[u8], offset: usize) -> Option<(String, usize)> {
    let len = msg.get(offset).copied()? as usize;
    let start = offset + 1;
    if msg.len() < start + len {
        return None;
    }
    let data = &msg[start..start + len];
    let s = std::str::from_utf8(data).ok()?.to_string();
    Some((s, start + len))
}

impl BinanceSnapshotParser {
    fn parse_snapshot_event(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        // 从快照数据中提取信息
        if let (Some(symbol), Some(last_update_id), Some(bids_array), Some(asks_array)) = (
            json_value.get("s").and_then(|v| v.as_str()),
            json_value.get("lastUpdateId").and_then(|v| v.as_i64()),
            json_value.get("bids").and_then(|v| v.as_array()),
            json_value.get("asks").and_then(|v| v.as_array()),
        ) {
            // 计算拆分方案
            let chunks = split_levels(bids_array.len(), asks_array.len(), self.max_levels);
            let total_chunks = chunks.len();
            let mut sent_count = 0;

            for (chunk_idx, (bids_start, bids_count, asks_start, asks_count)) in
                chunks.into_iter().enumerate()
            {
                // 创建快照消息
                let mut inc_msg = IncMsg::create(
                    symbol.to_string(),
                    last_update_id + 1, // first_update_id
                    last_update_id + 1, // final_update_id（对于快照相同）
                    0,                  // timestamp（快照没有实际时间戳）
                    true,               // is_snapshot = true
                    bids_count as u32,
                    asks_count as u32,
                );

                // 设置 chunk_index 和 is_last
                inc_msg.set_chunk_index(chunk_idx as u8);
                inc_msg.set_is_last(chunk_idx == total_chunks - 1);

                // 解析订单簿层级（带偏移量）
                parse_order_book_levels_with_offset(
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
        0
    }
}

#[derive(Clone)]
pub struct BinanceIncParser {
    max_levels: Option<usize>,
    is_snapshot: bool,
    mode: BinanceDepthMode,
}

impl BinanceIncParser {
    pub fn new() -> Self {
        Self::futures_incremental(None)
    }

    pub fn with_max_levels(max_levels: Option<usize>) -> Self {
        Self::futures_incremental(max_levels)
    }

    pub fn with_snapshot(max_levels: Option<usize>) -> Self {
        Self::futures_snapshot(max_levels)
    }

    pub fn futures_incremental(max_levels: Option<usize>) -> Self {
        Self {
            max_levels,
            is_snapshot: false,
            mode: BinanceDepthMode::FuturesDepthUpdate,
        }
    }

    pub fn futures_snapshot(max_levels: Option<usize>) -> Self {
        Self {
            max_levels,
            is_snapshot: true,
            mode: BinanceDepthMode::FuturesDepthUpdate,
        }
    }

    pub fn spot_incremental(max_levels: Option<usize>) -> Self {
        Self {
            max_levels,
            is_snapshot: false,
            mode: BinanceDepthMode::SpotDepthUpdate,
        }
    }

    pub fn spot_snapshot(max_levels: Option<usize>) -> Self {
        Self {
            max_levels,
            is_snapshot: true,
            mode: BinanceDepthMode::SpotSnapshot,
        }
    }
}

impl Parser for BinanceIncParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // 解析币安增量/深度20快照消息
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                let (payload, stream_symbol) = match (
                    json_value.get("data"),
                    json_value.get("stream").and_then(|v| v.as_str()),
                ) {
                    (Some(data), Some(stream)) => (data, parse_binance_stream_symbol(stream)),
                    _ => (&json_value, None),
                };

                match self.mode {
                    BinanceDepthMode::FuturesDepthUpdate | BinanceDepthMode::SpotDepthUpdate => {
                        let is_depth_update = payload
                            .get("e")
                            .and_then(|v| v.as_str())
                            .map(|e| e == "depthUpdate")
                            .unwrap_or(false)
                            || (payload.get("U").is_some() && payload.get("u").is_some());
                        if !is_depth_update {
                            return 0;
                        }
                        return self.parse_depth_update(payload, stream_symbol.as_deref(), tx);
                    }
                    BinanceDepthMode::SpotSnapshot => {
                        return self.parse_spot_snapshot(payload, stream_symbol.as_deref(), tx);
                    }
                }
            }
        }
        0
    }
}

#[derive(Clone, Copy)]
enum BinanceDepthMode {
    FuturesDepthUpdate,
    SpotDepthUpdate,
    SpotSnapshot,
}

fn parse_binance_stream_symbol(stream: &str) -> Option<String> {
    stream.split('@').next().map(|s| s.to_uppercase())
}

impl BinanceIncParser {
    fn parse_depth_update(
        &self,
        json_value: &serde_json::Value,
        symbol_override: Option<&str>,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        // futures/spot 增量：depthUpdate (U/u + b/a)
        let symbol = json_value
            .get("s")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| symbol_override.map(|s| s.to_uppercase()));
        if let (
            Some(symbol),
            Some(first_update_id),
            Some(final_update_id),
            Some(event_time),
            Some(bids_array),
            Some(asks_array),
        ) = (
            symbol,
            json_value.get("U").and_then(|v| v.as_i64()),
            json_value.get("u").and_then(|v| v.as_i64()),
            json_value
                .get("E")
                .and_then(|v| v.as_i64())
                .or_else(|| json_value.get("T").and_then(|v| v.as_i64())),
            json_value.get("b").and_then(|v| v.as_array()),
            json_value.get("a").and_then(|v| v.as_array()),
        ) {
            // 计算拆分方案
            let chunks = split_levels(bids_array.len(), asks_array.len(), self.max_levels);
            let total_chunks = chunks.len();
            let mut sent_count = 0;

            for (chunk_idx, (bids_start, bids_count, asks_start, asks_count)) in
                chunks.into_iter().enumerate()
            {
                // 创建增量消息
                let mut inc_msg = IncMsg::create(
                    symbol.clone(),
                    first_update_id,
                    final_update_id,
                    event_time,
                    self.is_snapshot,
                    bids_count as u32,
                    asks_count as u32,
                );

                // 设置 chunk_index 和 is_last
                inc_msg.set_chunk_index(chunk_idx as u8);
                inc_msg.set_is_last(chunk_idx == total_chunks - 1);

                // 解析订单簿层级（带偏移量）
                parse_order_book_levels_with_offset(
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
        0
    }

    fn parse_spot_snapshot(
        &self,
        json_value: &serde_json::Value,
        symbol_override: Option<&str>,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        // spot/margin depth20 快照：lastUpdateId + bids/asks
        let symbol = json_value
            .get("s")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| symbol_override.map(|s| s.to_uppercase()));

        if let (
            Some(symbol),
            Some(last_update_id),
            Some(bids_array),
            Some(asks_array),
        ) = (
            symbol,
            json_value.get("lastUpdateId").and_then(|v| v.as_i64()),
            json_value.get("bids").and_then(|v| v.as_array()),
            json_value.get("asks").and_then(|v| v.as_array()),
        ) {
            let chunks = split_levels(bids_array.len(), asks_array.len(), self.max_levels);
            let total_chunks = chunks.len();
            let mut sent_count = 0;
            let event_time = json_value
                .get("E")
                .and_then(|v| v.as_i64())
                .or_else(|| json_value.get("T").and_then(|v| v.as_i64()))
                .unwrap_or(0);

            for (chunk_idx, (bids_start, bids_count, asks_start, asks_count)) in
                chunks.into_iter().enumerate()
            {
                let mut inc_msg = IncMsg::create(
                    symbol.clone(),
                    last_update_id,
                    last_update_id,
                    event_time,
                    self.is_snapshot,
                    bids_count as u32,
                    asks_count as u32,
                );

                inc_msg.set_chunk_index(chunk_idx as u8);
                inc_msg.set_is_last(chunk_idx == total_chunks - 1);

                parse_order_book_levels_with_offset(
                    bids_array,
                    asks_array,
                    bids_start,
                    bids_count,
                    asks_start,
                    asks_count,
                    &mut inc_msg,
                );

                if tx.send(inc_msg.to_bytes()).is_ok() {
                    sent_count += 1;
                }
            }

            return sent_count;
        }

        0
    }
}

#[derive(Clone)]
pub struct BinanceTradeParser;

impl BinanceTradeParser {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Clone)]
pub struct BinanceAskBidSpreadParser;

impl BinanceAskBidSpreadParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BinanceTradeParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse Binance trade message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // Check if this is a trade event
                if let Some(event_type) = json_value.get("e").and_then(|v| v.as_str()) {
                    if event_type == "trade" {
                        return self.parse_trade_event(&json_value, tx);
                    }
                }
            }
        }
        0
    }
}

impl BinanceTradeParser {
    fn parse_trade_event(
        &self,
        json_value: &serde_json::Value,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        // Extract trade data from Binance trade message
        if let (
            Some(symbol),
            Some(trade_id),
            Some(price_str),
            Some(qty_str),
            Some(event_time),
            Some(is_maker),
        ) = (
            json_value.get("s").and_then(|v| v.as_str()),  // 交易对
            json_value.get("t").and_then(|v| v.as_i64()),  // 交易ID
            json_value.get("p").and_then(|v| v.as_str()),  // 成交价格
            json_value.get("q").and_then(|v| v.as_str()),  // 成交数量
            json_value.get("E").and_then(|v| v.as_i64()),  // 事件时间
            json_value.get("m").and_then(|v| v.as_bool()), // 买方是否是做市方
        ) {
            // Parse price and quantity
            if let (Ok(price), Ok(amount)) = (price_str.parse::<f64>(), qty_str.parse::<f64>()) {
                // Filter out zero values - 币安有时候price和amount会是0，过滤掉不发送
                if price <= 0.0 || amount <= 0.0 {
                    return 0;
                }

                // Determine side: 买方是否是做市方，'S'表示卖出，'B'表示买入
                // 如果买方是做市方(true)，那么这是一个主动卖出单，标记为'S'
                // 如果买方不是做市方(false)，那么这是一个主动买入单，标记为'B'
                let side = if is_maker { 'S' } else { 'B' };

                // Create trade message
                let trade_msg = TradeMsg::create(
                    symbol.to_string(),
                    trade_id,
                    event_time,
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

impl Parser for BinanceAskBidSpreadParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // Parse Binance bookTicker message
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 币安期货格式没有 E 和 T 字段，现货有
                // 统一处理，如果有 E 字段就用，没有就用 0
                if let (
                    Some(symbol),
                    Some(bid_price_str),
                    Some(bid_qty_str),
                    Some(ask_price_str),
                    Some(ask_qty_str),
                ) = (
                    json_value.get("s").and_then(|v| v.as_str()), // symbol
                    json_value.get("b").and_then(|v| v.as_str()), // best bid price
                    json_value.get("B").and_then(|v| v.as_str()), // best bid qty
                    json_value.get("a").and_then(|v| v.as_str()), // best ask price
                    json_value.get("A").and_then(|v| v.as_str()), // best ask qty
                ) {
                    // 获取时间戳：统一使用u字段（order book updateId）作为索引
                    // 币安现货和期货的bookTicker都有u字段
                    let timestamp = json_value.get("u").and_then(|v| v.as_i64()).unwrap_or(0);

                    // Parse prices and amounts
                    if let (Ok(bid_price), Ok(bid_amount), Ok(ask_price), Ok(ask_amount)) = (
                        bid_price_str.parse::<f64>(),
                        bid_qty_str.parse::<f64>(),
                        ask_price_str.parse::<f64>(),
                        ask_qty_str.parse::<f64>(),
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
        0
    }
}
