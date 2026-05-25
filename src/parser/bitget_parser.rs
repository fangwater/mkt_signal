use crate::common::mkt_msg::{
    AskBidSpreadMsg, FundingRateMsg, IncMsg, IndexPriceMsg, KlineMsg, Level, MarkPriceMsg, TradeMsg,
};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use log::debug;
use tokio::sync::mpsc;

/// Bitget 价差解析器：支持 ticker 和 books1
#[derive(Clone)]
pub struct BitgetAskBidSpreadParser;

impl Default for BitgetAskBidSpreadParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BitgetAskBidSpreadParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BitgetAskBidSpreadParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // 解析 Bitget books1 消息（最优买卖价）
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 检查是否是 books1 频道的消息
                if let Some(arg) = json_value.get("arg").and_then(|v| v.as_object()) {
                    let channel = arg.get("channel").and_then(|v| v.as_str()).unwrap_or("");
                    if channel != "books1" {
                        return 0;
                    }

                    // 获取 symbol
                    let symbol = match arg.get("instId").and_then(|v| v.as_str()) {
                        Some(s) => s,
                        None => return 0,
                    };

                    // 解析 data 数组
                    if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
                        let mut count = 0;
                        for item in data_array {
                            if let Some(obj) = item.as_object() {
                                // 深度频道，取第一档
                                let bids = obj.get("bids").and_then(|v| v.as_array());
                                let asks = obj.get("asks").and_then(|v| v.as_array());
                                let bid =
                                    bids.and_then(|arr| arr.first()).and_then(|v| v.as_array());
                                let ask =
                                    asks.and_then(|arr| arr.first()).and_then(|v| v.as_array());

                                let bid_price = bid
                                    .and_then(|entry| entry.first())
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<f64>().ok());
                                let bid_amount = bid
                                    .and_then(|entry| entry.get(1))
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<f64>().ok());
                                let ask_price = ask
                                    .and_then(|entry| entry.first())
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<f64>().ok());
                                let ask_amount = ask
                                    .and_then(|entry| entry.get(1))
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<f64>().ok());
                                let timestamp = obj
                                    .get("ts")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<i64>().ok())
                                    .unwrap_or(0);

                                if let (Some(bp), Some(ap), Some(ba), Some(aa)) =
                                    (bid_price, ask_price, bid_amount, ask_amount)
                                {
                                    if bp > 0.0 && ap > 0.0 && ba > 0.0 && aa > 0.0 {
                                        let spread_msg = AskBidSpreadMsg::create(
                                            symbol.to_string(),
                                            timestamp,
                                            bp,
                                            ba,
                                            ap,
                                            aa,
                                        );

                                        if tx.send(spread_msg.to_bytes()).is_ok() {
                                            count += 1;
                                        }
                                    }
                                }
                            }
                        }
                        return count;
                    }
                }
            }
        }
        0
    }
}

/// Bitget Signal Parser - 用于深度行情信号
#[derive(Clone)]
pub struct BitgetSignalParser;

impl Default for BitgetSignalParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BitgetSignalParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BitgetSignalParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 使用顶层 ts 或 data[0].ts 作为时间戳
                let ts = json_value
                    .get("ts")
                    .and_then(|v| v.as_i64())
                    .or_else(|| {
                        json_value
                            .get("data")
                            .and_then(|v| v.as_array())
                            .and_then(|arr| arr.first())
                            .and_then(|item| item.get("ts"))
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<i64>().ok())
                    })
                    .unwrap_or(0);

                if ts > 0 {
                    let signal_msg = crate::common::mkt_msg::SignalMsg::create(
                        crate::common::mkt_msg::SignalSource::Tcp,
                        ts,
                    );
                    if tx.send(signal_msg.to_bytes()).is_ok() {
                        return 1;
                    }
                }
                debug!("Bitget signal skipped: {}", json_str);
            }
        }
        0
    }
}

/// Bitget Kline Parser - 解析 K线数据
///
/// Bitget K线消息格式:
/// ```json
/// {
///     "action": "snapshot",
///     "arg": {
///         "instType": "USDT-FUTURES",
///         "channel": "candle1m",
///         "instId": "BTCUSDT"
///     },
///     "data": [
///         ["1695685500000", "27000", "27000.5", "27000", "27000.5", "0.057", "1539.0155", "1539.0155"]
///     ],
///     "ts": 1695715462250
/// }
/// ```
/// data 格式: [timestamp, open, high, low, close, baseVol, quoteVol, usdtVol]
#[derive(Clone)]
pub struct BitgetKlineParser;

impl Default for BitgetKlineParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BitgetKlineParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BitgetKlineParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // 解析 Bitget K线消息
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 检查是否是 candle 频道的消息
                if let Some(arg) = json_value.get("arg").and_then(|v| v.as_object()) {
                    let channel = arg.get("channel").and_then(|v| v.as_str()).unwrap_or("");
                    if !channel.starts_with("candle") {
                        return 0;
                    }

                    // 获取 symbol
                    let symbol = match arg.get("instId").and_then(|v| v.as_str()) {
                        Some(s) => s,
                        None => return 0,
                    };

                    // 解析 data 数组
                    if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
                        let mut count = 0;
                        for kline_data in data_array {
                            if let Some(kline_array) = kline_data.as_array() {
                                // data 格式: [ts, o, h, l, c, vol, quoteVol, usdtVol]
                                if kline_array.len() >= 6 {
                                    if let (
                                        Some(ts_str),
                                        Some(o_str),
                                        Some(h_str),
                                        Some(l_str),
                                        Some(c_str),
                                        Some(vol_str),
                                    ) = (
                                        kline_array[0].as_str(),
                                        kline_array[1].as_str(),
                                        kline_array[2].as_str(),
                                        kline_array[3].as_str(),
                                        kline_array[4].as_str(),
                                        kline_array[5].as_str(),
                                    ) {
                                        // 解析所有值
                                        if let (
                                            Ok(timestamp),
                                            Ok(open),
                                            Ok(high),
                                            Ok(low),
                                            Ok(close),
                                            Ok(volume),
                                        ) = (
                                            ts_str.parse::<i64>(),
                                            o_str.parse::<f64>(),
                                            h_str.parse::<f64>(),
                                            l_str.parse::<f64>(),
                                            c_str.parse::<f64>(),
                                            vol_str.parse::<f64>(),
                                        ) {
                                            // 创建 K线消息
                                            let kline_msg = KlineMsg::create(
                                                symbol.to_string(),
                                                open,
                                                high,
                                                low,
                                                close,
                                                volume,
                                                timestamp,
                                            );

                                            // 发送消息
                                            if tx.send(kline_msg.to_bytes()).is_ok() {
                                                count += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        return count;
                    }
                }
            }
        }
        0
    }
}

/// Bitget Derivatives Metrics Parser - 从 ticker 消息解析衍生品指标
///
/// Bitget USDT-FUTURES ticker 消息中包含 funding rate、mark price 和 index price：
/// ```json
/// {
///     "action": "snapshot",
///     "arg": {"instType": "USDT-FUTURES", "channel": "ticker", "instId": "BTCUSDT"},
///     "data": [{
///         "instId": "BTCUSDT",
///         "lastPr": "27000.5",
///         "fundingRate": "0.000010",
///         "nextFundingTime": "1695722400000",
///         "markPrice": "27000.0",
///         "indexPrice": "25702.4",
///         "ts": "1695715383021"
///     }],
///     "ts": 1695715383039
/// }
/// ```
#[derive(Clone)]
pub struct BitgetDerivativesMetricsParser;

impl Default for BitgetDerivativesMetricsParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BitgetDerivativesMetricsParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for BitgetDerivativesMetricsParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // 解析 Bitget ticker 消息中的衍生品指标
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 检查是否是 ticker 频道的消息
                if let Some(arg) = json_value.get("arg").and_then(|v| v.as_object()) {
                    let channel = arg.get("channel").and_then(|v| v.as_str()).unwrap_or("");
                    if channel != "ticker" {
                        return 0;
                    }

                    // 只处理 USDT-FUTURES 类型（衍生品）
                    let inst_type = arg.get("instType").and_then(|v| v.as_str()).unwrap_or("");
                    if inst_type != "USDT-FUTURES" {
                        return 0;
                    }

                    // 获取 symbol
                    let symbol = match arg.get("instId").and_then(|v| v.as_str()) {
                        Some(s) => s,
                        None => return 0,
                    };

                    // 解析 data 数组
                    if let Some(data_array) = json_value.get("data").and_then(|v| v.as_array()) {
                        let mut count = 0;
                        for item in data_array {
                            if let Some(obj) = item.as_object() {
                                let timestamp = obj
                                    .get("ts")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<i64>().ok())
                                    .unwrap_or(0);

                                // 解析 funding rate
                                if let (Some(funding_rate_str), Some(next_funding_time_str)) = (
                                    obj.get("fundingRate").and_then(|v| v.as_str()),
                                    obj.get("nextFundingTime").and_then(|v| v.as_str()),
                                ) {
                                    if let (Ok(funding_rate), Ok(next_funding_time)) = (
                                        funding_rate_str.parse::<f64>(),
                                        next_funding_time_str.parse::<i64>(),
                                    ) {
                                        let funding_rate_msg = FundingRateMsg::create(
                                            symbol.to_string(),
                                            funding_rate,
                                            next_funding_time,
                                            timestamp,
                                        );
                                        if tx.send(funding_rate_msg.to_bytes()).is_ok() {
                                            count += 1;
                                        }
                                    }
                                }

                                // 解析 mark price
                                if let Some(mark_price_str) =
                                    obj.get("markPrice").and_then(|v| v.as_str())
                                {
                                    if let Ok(mark_price) = mark_price_str.parse::<f64>() {
                                        if mark_price > 0.0 {
                                            let mark_price_msg = MarkPriceMsg::create(
                                                symbol.to_string(),
                                                mark_price,
                                                timestamp,
                                            );
                                            if tx.send(mark_price_msg.to_bytes()).is_ok() {
                                                count += 1;
                                            }
                                        }
                                    }
                                }

                                // 解析 index price
                                if let Some(index_price_str) =
                                    obj.get("indexPrice").and_then(|v| v.as_str())
                                {
                                    if let Ok(index_price) = index_price_str.parse::<f64>() {
                                        if index_price > 0.0 {
                                            let index_price_msg = IndexPriceMsg::create(
                                                symbol.to_string(),
                                                index_price,
                                                timestamp,
                                            );
                                            if tx.send(index_price_msg.to_bytes()).is_ok() {
                                                count += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        return count;
                    }
                }
            }
        }
        0
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

/// Bitget 增量深度解析（books snapshot/update）
#[derive(Clone)]
pub struct BitgetIncParser {
    max_levels: Option<usize>,
}

impl Default for BitgetIncParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BitgetIncParser {
    pub fn new() -> Self {
        Self { max_levels: None }
    }

    pub fn with_max_levels(max_levels: Option<usize>) -> Self {
        Self { max_levels }
    }

    fn parse_levels_with_offset(
        levels: &[serde_json::Value],
        inc_msg: &mut IncMsg,
        is_bid: bool,
        start: usize,
        count: usize,
    ) {
        for i in 0..count {
            let src_idx = start + i;
            if src_idx >= levels.len() {
                break;
            }
            if let Some(arr) = levels[src_idx].as_array() {
                if arr.len() >= 2 {
                    if let (Some(price), Some(amount)) = (arr[0].as_str(), arr[1].as_str()) {
                        let level = Level::new(price, amount);
                        if is_bid {
                            inc_msg.set_bid_level(i, level);
                        } else {
                            inc_msg.set_ask_level(i, level);
                        }
                    }
                }
            }
        }
    }
}

impl Parser for BitgetIncParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                let action = json_value
                    .get("action")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if action != "snapshot" && action != "update" {
                    return 0;
                }

                let arg = match json_value.get("arg").and_then(|v| v.as_object()) {
                    Some(a) => a,
                    None => return 0,
                };
                let symbol = match arg.get("instId").and_then(|v| v.as_str()) {
                    Some(s) => s,
                    None => return 0,
                };

                let data_array = match json_value.get("data").and_then(|v| v.as_array()) {
                    Some(arr) => arr,
                    None => return 0,
                };

                let mut sent = 0;
                for item in data_array {
                    if let Some(obj) = item.as_object() {
                        let bids = obj
                            .get("bids")
                            .and_then(|v| v.as_array())
                            .cloned()
                            .unwrap_or_default();
                        let asks = obj
                            .get("asks")
                            .and_then(|v| v.as_array())
                            .cloned()
                            .unwrap_or_default();
                        let seq = obj.get("seq").and_then(|v| v.as_i64()).unwrap_or(0);
                        let timestamp = obj
                            .get("ts")
                            .and_then(|v| v.as_i64())
                            .or_else(|| {
                                obj.get("ts")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<i64>().ok())
                            })
                            .or_else(|| json_value.get("ts").and_then(|v| v.as_i64()))
                            .or_else(|| {
                                json_value
                                    .get("ts")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<i64>().ok())
                            })
                            .unwrap_or(0);

                        // 计算拆分方案
                        let chunks = split_levels(bids.len(), asks.len(), self.max_levels);
                        let total_chunks = chunks.len();

                        for (chunk_idx, (bids_start, bids_count, asks_start, asks_count)) in
                            chunks.into_iter().enumerate()
                        {
                            let mut inc_msg = IncMsg::create(
                                symbol.to_string(),
                                seq,
                                seq,
                                timestamp,
                                action == "snapshot",
                                bids_count as u32,
                                asks_count as u32,
                            );

                            // 设置 chunk_index 和 is_last
                            inc_msg.set_chunk_index(chunk_idx as u8);
                            inc_msg.set_is_last(chunk_idx == total_chunks - 1);

                            Self::parse_levels_with_offset(
                                &bids,
                                &mut inc_msg,
                                true,
                                bids_start,
                                bids_count,
                            );
                            Self::parse_levels_with_offset(
                                &asks,
                                &mut inc_msg,
                                false,
                                asks_start,
                                asks_count,
                            );

                            if tx.send(inc_msg.to_bytes()).is_ok() {
                                sent += 1;
                            }
                        }
                    }
                }
                return sent;
            }
        }
        0
    }
}

/// Bitget Trade Parser — UTA v3 SBE `publicTrade` (templateId=1003)。
///
/// SBE frame layout (schemaId=1, schemaVer=3, littleEndian):
///   - header  8B: blockLength u16, templateId u16, schemaId u16, version u16
///   - root   16B: px_exp i8, sz_exp i8, sts u64 (gateway 推送时刻), +6B padding
///   - group  4B header (entryBlockLength u16, numInGroup u16) + N×40B entries
///   - entry 40B: ts u64, exec_id u64, price i64, size i64, side u8, +7B padding
///   - symbol varString8 (u8 length + UTF-8 bytes)
///
/// schema 参考: https://www.bitget.com/api-doc/uta/sbe/sbe-trade
#[derive(Clone)]
pub struct BitgetTradeParser;

impl Default for BitgetTradeParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BitgetTradeParser {
    pub fn new() -> Self {
        Self
    }
}

const SBE_HEADER_SIZE: usize = 8;
const SBE_SCHEMA_ID: u16 = 1;
const SBE_TEMPLATE_PUBLIC_TRADE: u16 = 1003;

impl Parser for BitgetTradeParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let raw = msg.as_ref();
        // 文本帧 (subscribe ack / error event / "pong"): 不业务，丢弃
        if raw
            .first()
            .is_some_and(|b| *b < 0x80 && (*b == b'{' || *b == b'"' || *b == b'p'))
        {
            return 0;
        }
        if raw.len() < SBE_HEADER_SIZE {
            return 0;
        }
        let block_length = u16::from_le_bytes([raw[0], raw[1]]) as usize;
        let template_id = u16::from_le_bytes([raw[2], raw[3]]);
        let schema_id = u16::from_le_bytes([raw[4], raw[5]]);
        if schema_id != SBE_SCHEMA_ID {
            debug!(
                "BitgetTradeParser: unexpected schemaId={} (want {})",
                schema_id, SBE_SCHEMA_ID
            );
            return 0;
        }
        if template_id != SBE_TEMPLATE_PUBLIC_TRADE {
            // 1001=Depth50 / 1002=BBO 这类不属本 parser, drop
            return 0;
        }
        let body_off = SBE_HEADER_SIZE;
        if raw.len() < body_off + block_length {
            debug!(
                "BitgetTradeParser: frame truncated have={} need={}",
                raw.len(),
                body_off + block_length
            );
            return 0;
        }
        // root: px_exp(1) + sz_exp(1) + sts(8) + padding
        if block_length < 10 {
            debug!(
                "BitgetTradeParser: trade root blockLength too small: {}",
                block_length
            );
            return 0;
        }
        let px_exp = raw[body_off] as i8;
        let sz_exp = raw[body_off + 1] as i8;
        // sts @ body_off+2 (u64); 暂未注入 TradeMsg (TradeMsg 没有该字段)

        // group header
        let grp_off = body_off + block_length;
        if raw.len() < grp_off + 4 {
            return 0;
        }
        let entry_bl = u16::from_le_bytes([raw[grp_off], raw[grp_off + 1]]) as usize;
        let num = u16::from_le_bytes([raw[grp_off + 2], raw[grp_off + 3]]) as usize;
        if entry_bl < 33 {
            debug!(
                "BitgetTradeParser: entryBlockLength too small: {}",
                entry_bl
            );
            return 0;
        }
        let entries_off = grp_off + 4;
        let entries_total = entry_bl.saturating_mul(num);
        if raw.len() < entries_off + entries_total {
            debug!(
                "BitgetTradeParser: trade entries truncated have={} need={}",
                raw.len(),
                entries_off + entries_total
            );
            return 0;
        }

        // varString8 symbol (在 entries 之后)
        let sym_off = entries_off + entries_total;
        if raw.len() <= sym_off {
            return 0;
        }
        let sym_len = raw[sym_off] as usize;
        if raw.len() < sym_off + 1 + sym_len {
            return 0;
        }
        let symbol = match std::str::from_utf8(&raw[sym_off + 1..sym_off + 1 + sym_len]) {
            Ok(s) => s.to_ascii_uppercase(),
            Err(_) => return 0,
        };

        let px_scale = 10_f64.powi(px_exp as i32);
        let sz_scale = 10_f64.powi(sz_exp as i32);

        let mut count = 0;
        for i in 0..num {
            let off = entries_off + i * entry_bl;
            let ts_us = read_i64_le(raw, off);
            let exec_id = read_i64_le(raw, off + 8);
            let price_m = read_i64_le(raw, off + 16);
            let size_m = read_i64_le(raw, off + 24);
            let side_raw = raw[off + 32];

            // 跟 v2 JSON 时代的口径一致：TradeMsg.timestamp 用 ms (downstream 假设)
            let ts_ms = ts_us / 1000;
            let side_char = match side_raw {
                0 => 'B',
                1 => 'S',
                _ => continue,
            };
            let price = (price_m as f64) * px_scale;
            let amount = (size_m as f64) * sz_scale;
            if price <= 0.0 || amount <= 0.0 {
                continue;
            }

            let trade_msg =
                TradeMsg::create(symbol.clone(), exec_id, ts_ms, side_char, price, amount);
            if tx.send(trade_msg.to_bytes()).is_ok() {
                count += 1;
            }
        }
        count
    }
}

#[inline]
fn read_i64_le(buf: &[u8], off: usize) -> i64 {
    i64::from_le_bytes([
        buf[off],
        buf[off + 1],
        buf[off + 2],
        buf[off + 3],
        buf[off + 4],
        buf[off + 5],
        buf[off + 6],
        buf[off + 7],
    ])
}

// NOTE: parser tests removed per repo usage (WS payloads are brittle and often change).
