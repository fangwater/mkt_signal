use crate::common::mkt_msg::{
    AskBidSpreadMsg, FundingRateMsg, IncMsg, IndexPriceMsg, KlineMsg, Level, MarkPriceMsg,
    SignalMsg, SignalSource, TradeMsg,
};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use tokio::sync::mpsc;

fn parse_json_f64(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn parse_json_i64(v: &serde_json::Value) -> Option<i64> {
    match v {
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(i)
            } else if let Some(u) = n.as_u64() {
                i64::try_from(u).ok()
            } else {
                n.as_f64().map(|f| f as i64)
            }
        }
        serde_json::Value::String(s) => s
            .parse::<i64>()
            .ok()
            .or_else(|| s.parse::<f64>().ok().map(|f| f as i64)),
        _ => None,
    }
}

fn normalize_timestamp_to_ms(ts: i64) -> i64 {
    if ts <= 0 {
        return 0;
    }
    if ts < 1_000_000_000_000 {
        ts * 1000
    } else {
        ts
    }
}

fn parse_gate_ts_ms(v: &serde_json::Value) -> Option<i64> {
    parse_json_i64(v).map(normalize_timestamp_to_ms)
}

fn parse_gate_level(level: &serde_json::Value) -> Option<Level> {
    if let Some(arr) = level.as_array() {
        if arr.len() >= 2 {
            let price = parse_json_f64(&arr[0])?;
            let amount = parse_json_f64(&arr[1])?;
            if price > 0.0 {
                return Some(Level::from_values(price, amount));
            }
        }
        return None;
    }

    if let Some(obj) = level.as_object() {
        let price = obj
            .get("p")
            .and_then(parse_json_f64)
            .or_else(|| obj.get("price").and_then(parse_json_f64))?;
        let amount = obj
            .get("s")
            .and_then(parse_json_f64)
            .or_else(|| obj.get("size").and_then(parse_json_f64))
            .or_else(|| obj.get("amount").and_then(parse_json_f64))
            .unwrap_or(0.0);
        if price > 0.0 {
            return Some(Level::from_values(price, amount));
        }
    }

    None
}

fn parse_gate_levels(raw: &serde_json::Value) -> Vec<Level> {
    raw.as_array()
        .map(|arr| arr.iter().filter_map(parse_gate_level).collect())
        .unwrap_or_default()
}

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

fn fill_levels_with_offset(
    bids: &[Level],
    asks: &[Level],
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
        inc_msg.set_bid_level(i, bids[src_idx]);
    }

    for i in 0..asks_count {
        let src_idx = asks_start + i;
        if src_idx >= asks.len() {
            break;
        }
        inc_msg.set_ask_level(i, asks[src_idx]);
    }
}

/// Gate.io Signal Parser - 从 ticker 消息中提取时间戳作为信号
#[derive(Clone)]
pub struct GateSignalParser {
    source: SignalSource,
}

impl GateSignalParser {
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

impl Parser for GateSignalParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 检查是否是 ticker 频道的 update 事件
                let channel = json_value
                    .get("channel")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let event = json_value
                    .get("event")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if !channel.ends_with(".tickers") || event != "update" {
                    return 0;
                }

                // 获取时间戳 (使用 time_ms)
                if let Some(timestamp) = json_value.get("time_ms").and_then(|v| v.as_i64()) {
                    let signal_msg = SignalMsg::create(self.source, timestamp);
                    if tx.send(signal_msg.to_bytes()).is_ok() {
                        return 1;
                    }
                }
            }
        }
        0
    }
}

/// Gate.io Book Ticker Parser - 解析 futures.book_ticker 提取最优买卖价
///
/// Gate.io ticker 消息格式:
/// ```json
/// {
///     "time": 1764559257,
///     "time_ms": 1764559257161,
///     "channel": "spot.tickers",
///     "event": "update",
///     "result": {
///         "currency_pair": "BTC_USDT",
///         "last": "86445.2",
///         "lowest_ask": "86445.2",
///         "highest_bid": "86445.1",
///         "change_percentage": "-4.4564",
///         "base_volume": "10381.529204",
///         "quote_volume": "931959066.3514661",
///         "high_24h": "91962.8",
///         "low_24h": "86325.7"
///     }
/// }
/// ```
#[derive(Clone)]
pub struct GateTickerParser;

impl GateTickerParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for GateTickerParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 检查是否是 ticker 频道的 update 事件
                let channel = json_value
                    .get("channel")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let event = json_value
                    .get("event")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if event != "update"
                    || !(channel.ends_with(".book_ticker") || channel.ends_with(".tickers"))
                {
                    return 0;
                }

                let result = json_value.get("result");

                // futures.book_ticker：字段 s/b/B/a/A
                // spot.tickers：字段 currency_pair/highest_bid/lowest_ask
                if let Some(res) = result.and_then(|v| v.as_object()) {
                    let symbol = match res
                        .get("s")
                        .and_then(|v| v.as_str())
                        .or_else(|| res.get("contract").and_then(|v| v.as_str()))
                        .or_else(|| res.get("currency_pair").and_then(|v| v.as_str()))
                    {
                        Some(s) => s,
                        None => return 0,
                    };
                    let ts = res
                        .get("t")
                        .and_then(parse_gate_ts_ms)
                        .or_else(|| res.get("time_ms").and_then(parse_gate_ts_ms))
                        .or_else(|| json_value.get("time_ms").and_then(parse_gate_ts_ms))
                        .or_else(|| json_value.get("time").and_then(parse_gate_ts_ms))
                        .unwrap_or(0);
                    let bid_price = res
                        .get("b")
                        .and_then(parse_json_f64)
                        .or_else(|| res.get("highest_bid").and_then(parse_json_f64));
                    let ask_price = res
                        .get("a")
                        .and_then(parse_json_f64)
                        .or_else(|| res.get("lowest_ask").and_then(parse_json_f64));
                    let bid_amount = res
                        .get("B")
                        .and_then(parse_json_f64)
                        .or_else(|| res.get("best_bid_size").and_then(parse_json_f64))
                        .unwrap_or(0.0);
                    let ask_amount = res
                        .get("A")
                        .and_then(parse_json_f64)
                        .or_else(|| res.get("best_ask_size").and_then(parse_json_f64))
                        .unwrap_or(0.0);

                    if let (Some(bp), Some(ap)) = (bid_price, ask_price) {
                        if bp > 0.0 && ap > 0.0 {
                            let spread_msg = AskBidSpreadMsg::create(
                                symbol.to_string(),
                                ts,
                                bp,
                                bid_amount,
                                ap,
                                ask_amount,
                            );
                            if tx.send(spread_msg.to_bytes()).is_ok() {
                                return 1;
                            }
                        }
                    }
                }
            }
        }
        0
    }
}

/// Gate.io Kline Parser - 解析 K线数据
///
/// Gate.io K线消息格式:
/// ```json
/// {
///     "time": 1764559363,
///     "time_ms": 1764559363408,
///     "channel": "spot.candlesticks",
///     "event": "update",
///     "result": {
///         "t": "1764559320",      // K线开始时间 (秒)
///         "v": "1146373.9657193", // 成交额 (quote volume)
///         "c": "86504.5",         // 收盘价
///         "h": "86577.9",         // 最高价
///         "l": "86504.5",         // 最低价
///         "o": "86525.1",         // 开盘价
///         "n": "1m_BTC_USDT",     // 周期_交易对
///         "a": "13.244502",       // 成交量 (base volume)
///         "w": false              // 是否为窗口期结束 (K线是否完结)
///     }
/// }
/// ```
#[derive(Clone)]
pub struct GateKlineParser {
    only_closed: bool, // 是否只处理已完结的K线
}

impl GateKlineParser {
    pub fn new(only_closed: bool) -> Self {
        Self { only_closed }
    }
}

impl Parser for GateKlineParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                // 检查是否是 candlesticks 频道的 update 事件
                let channel = json_value
                    .get("channel")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let event = json_value
                    .get("event")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if !channel.ends_with(".candlesticks") || event != "update" {
                    return 0;
                }

                // 解析 result 对象
                if let Some(result) = json_value.get("result").and_then(|v| v.as_object()) {
                    // 检查K线是否完结 (w: window closed)
                    if self.only_closed {
                        let is_closed = result.get("w").and_then(|v| v.as_bool()).unwrap_or(false);
                        if !is_closed {
                            return 0;
                        }
                    }

                    // 从 "n" 字段提取交易对: "1m_BTC_USDT" -> "BTC_USDT"
                    let symbol = match result.get("n").and_then(|v| v.as_str()) {
                        Some(n) => {
                            // 跳过周期前缀，如 "1m_"
                            if let Some(pos) = n.find('_') {
                                &n[pos + 1..]
                            } else {
                                n
                            }
                        }
                        None => return 0,
                    };

                    // 解析 OHLCV 数据
                    if let (
                        Some(t_str),
                        Some(o_str),
                        Some(h_str),
                        Some(l_str),
                        Some(c_str),
                        Some(a_str),
                    ) = (
                        result.get("t").and_then(|v| v.as_str()),
                        result.get("o").and_then(|v| v.as_str()),
                        result.get("h").and_then(|v| v.as_str()),
                        result.get("l").and_then(|v| v.as_str()),
                        result.get("c").and_then(|v| v.as_str()),
                        result.get("a").and_then(|v| v.as_str()), // base volume
                    ) {
                        // 解析所有值
                        if let (
                            Ok(timestamp_sec),
                            Ok(open),
                            Ok(high),
                            Ok(low),
                            Ok(close),
                            Ok(volume),
                        ) = (
                            t_str.parse::<i64>(),
                            o_str.parse::<f64>(),
                            h_str.parse::<f64>(),
                            l_str.parse::<f64>(),
                            c_str.parse::<f64>(),
                            a_str.parse::<f64>(),
                        ) {
                            // Gate 时间戳是秒，转换为毫秒
                            let timestamp = timestamp_sec * 1000;

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
                                return 1;
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
pub struct GateTradeParser;

impl GateTradeParser {
    pub fn new() -> Self {
        Self
    }

    fn parse_trade_item(
        &self,
        trade: &serde_json::Value,
        fallback_timestamp_ms: i64,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let symbol = match trade
            .get("contract")
            .and_then(|v| v.as_str())
            .or_else(|| trade.get("s").and_then(|v| v.as_str()))
            .or_else(|| trade.get("symbol").and_then(|v| v.as_str()))
            .or_else(|| trade.get("currency_pair").and_then(|v| v.as_str()))
        {
            Some(s) => s,
            None => return 0,
        };

        let price = match trade
            .get("price")
            .and_then(parse_json_f64)
            .or_else(|| trade.get("p").and_then(parse_json_f64))
            .or_else(|| trade.get("last").and_then(parse_json_f64))
        {
            Some(v) if v > 0.0 => v,
            _ => return 0,
        };

        let raw_size = trade
            .get("size")
            .and_then(parse_json_f64)
            .or_else(|| trade.get("amount").and_then(parse_json_f64))
            .or_else(|| trade.get("qty").and_then(parse_json_f64));

        let side = trade
            .get("side")
            .and_then(|v| v.as_str())
            .map(|s| s.to_ascii_lowercase())
            .and_then(|s| match s.as_str() {
                "buy" | "bid" => Some('B'),
                "sell" | "ask" => Some('S'),
                _ => None,
            })
            .or_else(|| raw_size.map(|sz| if sz >= 0.0 { 'B' } else { 'S' }))
            .unwrap_or('B');

        let amount = raw_size
            .map(|v| v.abs())
            .or_else(|| trade.get("q").and_then(parse_json_f64))
            .unwrap_or(0.0);
        if amount <= 0.0 {
            return 0;
        }

        let timestamp = trade
            .get("create_time_ms")
            .and_then(parse_gate_ts_ms)
            .or_else(|| trade.get("timestamp_ms").and_then(parse_gate_ts_ms))
            .or_else(|| trade.get("create_time").and_then(parse_gate_ts_ms))
            .or_else(|| trade.get("t").and_then(parse_gate_ts_ms))
            .unwrap_or(fallback_timestamp_ms);

        let trade_id = trade
            .get("id")
            .and_then(parse_json_i64)
            .or_else(|| trade.get("trade_id").and_then(parse_json_i64))
            .or_else(|| trade.get("i").and_then(parse_json_i64))
            .unwrap_or(0);

        let trade_msg =
            TradeMsg::create(symbol.to_string(), trade_id, timestamp, side, price, amount);
        if tx.send(trade_msg.to_bytes()).is_ok() {
            return 1;
        }
        0
    }
}

impl Parser for GateTradeParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                let channel = json_value
                    .get("channel")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let event = json_value
                    .get("event")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if !channel.ends_with(".trades") || event != "update" {
                    return 0;
                }

                let fallback_timestamp_ms = json_value
                    .get("time_ms")
                    .and_then(parse_gate_ts_ms)
                    .or_else(|| json_value.get("time").and_then(parse_gate_ts_ms))
                    .unwrap_or(0);

                let mut sent = 0;
                match json_value.get("result") {
                    Some(serde_json::Value::Array(arr)) => {
                        for item in arr {
                            sent += self.parse_trade_item(item, fallback_timestamp_ms, tx);
                        }
                    }
                    Some(item @ serde_json::Value::Object(_)) => {
                        sent += self.parse_trade_item(item, fallback_timestamp_ms, tx);
                    }
                    _ => {}
                }
                return sent;
            }
        }
        0
    }
}

#[derive(Clone)]
pub struct GateIncParser {
    max_levels: Option<usize>,
}

impl GateIncParser {
    pub fn new() -> Self {
        Self { max_levels: None }
    }

    pub fn with_max_levels(max_levels: Option<usize>) -> Self {
        Self { max_levels }
    }

    fn parse_inc_item(
        &self,
        root: &serde_json::Value,
        item: &serde_json::Value,
        channel: &str,
        event: &str,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        let symbol = match item
            .get("s")
            .and_then(|v| v.as_str())
            .or_else(|| item.get("contract").and_then(|v| v.as_str()))
            .or_else(|| item.get("symbol").and_then(|v| v.as_str()))
            .or_else(|| item.get("currency_pair").and_then(|v| v.as_str()))
        {
            Some(s) => s,
            None => return 0,
        };

        let bids = item
            .get("b")
            .map(parse_gate_levels)
            .or_else(|| item.get("bids").map(parse_gate_levels))
            .unwrap_or_default();
        let asks = item
            .get("a")
            .map(parse_gate_levels)
            .or_else(|| item.get("asks").map(parse_gate_levels))
            .unwrap_or_default();

        if bids.is_empty() && asks.is_empty() {
            return 0;
        }

        let final_update_id = item
            .get("u")
            .and_then(parse_json_i64)
            .or_else(|| item.get("last_update_id").and_then(parse_json_i64))
            .or_else(|| item.get("seq").and_then(parse_json_i64))
            .or_else(|| item.get("id").and_then(parse_json_i64))
            .unwrap_or(0);
        let first_update_id = item
            .get("U")
            .and_then(parse_json_i64)
            .or_else(|| item.get("first_update_id").and_then(parse_json_i64))
            .unwrap_or(final_update_id);

        let timestamp = item
            .get("t")
            .and_then(parse_gate_ts_ms)
            .or_else(|| item.get("timestamp").and_then(parse_gate_ts_ms))
            .or_else(|| item.get("time_ms").and_then(parse_gate_ts_ms))
            .or_else(|| root.get("time_ms").and_then(parse_gate_ts_ms))
            .or_else(|| root.get("time").and_then(parse_gate_ts_ms))
            .unwrap_or(0);

        let is_snapshot = item
            .get("is_snapshot")
            .and_then(|v| v.as_bool())
            .or_else(|| {
                item.get("type")
                    .and_then(|v| v.as_str())
                    .map(|tp| tp.eq_ignore_ascii_case("snapshot"))
            })
            .unwrap_or_else(|| {
                channel.ends_with(".order_book")
                    || event.eq_ignore_ascii_case("snapshot")
                    || event.eq_ignore_ascii_case("all")
            });

        let chunks = split_levels(bids.len(), asks.len(), self.max_levels);
        let total_chunks = chunks.len();
        let mut sent_count = 0;

        for (chunk_idx, (bids_start, bids_count, asks_start, asks_count)) in
            chunks.into_iter().enumerate()
        {
            let mut inc_msg = IncMsg::create(
                symbol.to_string(),
                first_update_id,
                final_update_id,
                timestamp,
                is_snapshot,
                bids_count as u32,
                asks_count as u32,
            );
            inc_msg.set_chunk_index(chunk_idx as u8);
            inc_msg.set_is_last(chunk_idx == total_chunks - 1);

            fill_levels_with_offset(
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

impl Parser for GateIncParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                let channel = json_value
                    .get("channel")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let event = json_value
                    .get("event")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if !(channel.ends_with(".order_book_update") || channel.ends_with(".order_book")) {
                    return 0;
                }
                if event == "subscribe" || event == "unsubscribe" {
                    return 0;
                }

                let mut sent = 0;
                match json_value.get("result") {
                    Some(serde_json::Value::Array(arr)) => {
                        for item in arr {
                            sent += self.parse_inc_item(&json_value, item, channel, event, tx);
                        }
                    }
                    Some(item @ serde_json::Value::Object(_)) => {
                        sent += self.parse_inc_item(&json_value, item, channel, event, tx);
                    }
                    _ => {}
                }
                return sent;
            }
        }
        0
    }
}

/// Gate.io 衍生品指标解析（基于 futures.tickers）
#[derive(Clone)]
pub struct GateDerivativesMetricsParser;

impl GateDerivativesMetricsParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for GateDerivativesMetricsParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if let Ok(json_str) = std::str::from_utf8(&msg) {
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                let channel = json_value
                    .get("channel")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let event = json_value
                    .get("event")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if !channel.ends_with(".tickers") || event != "update" {
                    return 0;
                }

                let timestamp = json_value
                    .get("time_ms")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);

                let mut parsed = 0;
                let results = json_value.get("result");

                // result 可能是数组或单个对象
                let items: Vec<&serde_json::Value> = match results {
                    Some(serde_json::Value::Array(arr)) => arr.iter().collect(),
                    Some(obj @ serde_json::Value::Object(_)) => vec![obj],
                    _ => Vec::new(),
                };

                for item in items {
                    let Some(symbol) = item.get("contract").and_then(|v| v.as_str()) else {
                        continue;
                    };

                    if let Some(mark_px) = item
                        .get("mark_price")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                    {
                        let msg = MarkPriceMsg::create(symbol.to_string(), mark_px, timestamp);
                        if tx.send(msg.to_bytes()).is_ok() {
                            parsed += 1;
                        }
                    }

                    if let Some(index_px) = item
                        .get("index_price")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                    {
                        let msg = IndexPriceMsg::create(symbol.to_string(), index_px, timestamp);
                        if tx.send(msg.to_bytes()).is_ok() {
                            parsed += 1;
                        }
                    }

                    if let Some(fr) = item
                        .get("funding_rate")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                    {
                        // Gate futures.tickers 未提供 next funding time，填 0 作为占位
                        let msg = FundingRateMsg::create(symbol.to_string(), fr, 0, timestamp);
                        if tx.send(msg.to_bytes()).is_ok() {
                            parsed += 1;
                        }
                    }
                }

                return parsed;
            }
        }
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gate_kline_parse() {
        let parser = GateKlineParser::new(false); // 不限制只处理完结K线
        let (tx, mut rx) = mpsc::unbounded_channel();

        let msg = r#"{
            "time": 1764559363,
            "time_ms": 1764559363408,
            "channel": "spot.candlesticks",
            "event": "update",
            "result": {
                "t": "1764559320",
                "v": "1146373.9657193",
                "c": "86504.5",
                "h": "86577.9",
                "l": "86504.5",
                "o": "86525.1",
                "n": "1m_BTC_USDT",
                "a": "13.244502",
                "w": false
            }
        }"#;

        let bytes = Bytes::from(msg);
        let count = parser.parse(bytes, &tx);
        assert_eq!(count, 1);

        // 验证消息被正确发送
        let received = rx.try_recv();
        assert!(received.is_ok());
    }

    #[test]
    fn test_gate_kline_parse_only_closed() {
        let parser = GateKlineParser::new(true); // 只处理完结K线
        let (tx, _rx) = mpsc::unbounded_channel();

        // 未完结的K线 (w: false)
        let msg_open = r#"{
            "time": 1764559363,
            "time_ms": 1764559363408,
            "channel": "spot.candlesticks",
            "event": "update",
            "result": {
                "t": "1764559320",
                "v": "1146373.9657193",
                "c": "86504.5",
                "h": "86577.9",
                "l": "86504.5",
                "o": "86525.1",
                "n": "1m_BTC_USDT",
                "a": "13.244502",
                "w": false
            }
        }"#;

        let bytes = Bytes::from(msg_open);
        let count = parser.parse(bytes, &tx);
        assert_eq!(count, 0); // 应该被过滤

        // 已完结的K线 (w: true)
        let msg_closed = r#"{
            "time": 1764559363,
            "time_ms": 1764559363408,
            "channel": "spot.candlesticks",
            "event": "update",
            "result": {
                "t": "1764559320",
                "v": "1146373.9657193",
                "c": "86504.5",
                "h": "86577.9",
                "l": "86504.5",
                "o": "86525.1",
                "n": "1m_BTC_USDT",
                "a": "13.244502",
                "w": true
            }
        }"#;

        let bytes = Bytes::from(msg_closed);
        let count = parser.parse(bytes, &tx);
        assert_eq!(count, 1); // 应该被处理
    }

    #[test]
    fn test_gate_trade_parse() {
        let parser = GateTradeParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let msg = r#"{
            "time": 1764559450,
            "time_ms": 1764559450123,
            "channel": "futures.trades",
            "event": "update",
            "result": {
                "id": 987654321,
                "create_time_ms": "1764559450123.42",
                "contract": "BTC_USDT",
                "size": "-12",
                "price": "86500.5"
            }
        }"#;

        let count = parser.parse(Bytes::from(msg), &tx);
        assert_eq!(count, 1);
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn test_gate_trade_parse_spot_currency_pair() {
        let parser = GateTradeParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let msg = r#"{
            "time": 1764559450,
            "time_ms": 1764559450123,
            "channel": "spot.trades",
            "event": "update",
            "result": {
                "id": 123456789,
                "create_time_ms": "1764559450123",
                "currency_pair": "BTC_USDT",
                "side": "sell",
                "amount": "0.12",
                "price": "86500.5"
            }
        }"#;

        let count = parser.parse(Bytes::from(msg), &tx);
        assert_eq!(count, 1);
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn test_gate_inc_parse() {
        let parser = GateIncParser::with_max_levels(Some(2));
        let (tx, mut rx) = mpsc::unbounded_channel();

        let msg = r#"{
            "time": 1764559550,
            "time_ms": 1764559550999,
            "channel": "futures.order_book_update",
            "event": "update",
            "result": {
                "s": "BTC_USDT",
                "U": 100,
                "u": 101,
                "t": 1764559550999,
                "b": [["86499.5", "3"], ["86499.4", "2"]],
                "a": [["86500.5", "1"], ["86500.6", "2"]]
            }
        }"#;

        let count = parser.parse(Bytes::from(msg), &tx);
        assert_eq!(count, 2);
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn test_gate_ticker_parse_spot_tickers() {
        let parser = GateTickerParser::new();
        let (tx, mut rx) = mpsc::unbounded_channel();

        let msg = r#"{
            "time": 1764559257,
            "time_ms": 1764559257161,
            "channel": "spot.tickers",
            "event": "update",
            "result": {
                "currency_pair": "BTC_USDT",
                "lowest_ask": "86445.2",
                "highest_bid": "86445.1"
            }
        }"#;

        let count = parser.parse(Bytes::from(msg), &tx);
        assert_eq!(count, 1);
        assert!(rx.try_recv().is_ok());
    }
}
