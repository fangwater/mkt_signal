use crate::common::mkt_msg::{
    AskBidSpreadMsg, FundingRateMsg, IncMsg, IndexPriceMsg, KlineMsg, Level, MarkPriceMsg,
    SignalMsg, SignalSource, TradeMsg,
};
use crate::parser::default_parser::Parser;
use bytes::Bytes;
use mkt_parsers::gate as gate_codec;
use tokio::sync::mpsc;

// ─── Gate SBE constants (schemaId=1, littleEndian) ───────────────────────────
const GATE_SBE_HDR: usize = 8;
const GATE_SBE_SCHEMA: u16 = 1;
const GATE_SBE_T_OBU: u16 = 3;
const GATE_SBE_T_BOOK: u16 = 4;
const GATE_SBE_T_BOOK_UPDATE: u16 = 5;
const GATE_SBE_T_KLINE: u16 = 8;
const GATE_SBE_T_TICKER: u16 = 9;

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

#[inline]
fn us_to_ms(timestamp_us: i64) -> i64 {
    if timestamp_us == 0 {
        0
    } else {
        timestamp_us / 1000
    }
}

#[inline]
fn codec_level_to_msg(level: gate_codec::Level) -> Level {
    Level::from_values(level.price, level.amount)
}

fn publish_gate_trades(
    trades: Vec<gate_codec::Trade>,
    timestamp_ms: bool,
    tx: &mpsc::UnboundedSender<Bytes>,
) -> usize {
    let mut sent = 0;
    for trade in trades {
        let timestamp = if timestamp_ms {
            us_to_ms(trade.timestamp_us)
        } else {
            trade.timestamp_us
        };
        let msg = TradeMsg::create(
            trade.symbol,
            trade.trade_id,
            timestamp,
            trade.side,
            trade.price,
            trade.amount,
        );
        if tx.send(msg.to_bytes()).is_ok() {
            sent += 1;
        }
    }
    sent
}

fn publish_gate_book(
    book: &gate_codec::Book,
    max_levels: Option<usize>,
    timestamp_ms: bool,
    tx: &mpsc::UnboundedSender<Bytes>,
) -> usize {
    if book.bids.is_empty() && book.asks.is_empty() {
        return 0;
    }
    let bids: Vec<Level> = book.bids.iter().copied().map(codec_level_to_msg).collect();
    let asks: Vec<Level> = book.asks.iter().copied().map(codec_level_to_msg).collect();
    let timestamp = if timestamp_ms {
        us_to_ms(book.timestamp_us)
    } else {
        book.timestamp_us
    };
    let chunks = split_levels(bids.len(), asks.len(), max_levels);
    let total_chunks = chunks.len();
    let mut sent = 0;
    for (chunk_idx, (bids_start, bids_count, asks_start, asks_count)) in
        chunks.into_iter().enumerate()
    {
        let mut inc_msg = IncMsg::create(
            book.symbol.clone(),
            book.first_update_id,
            book.final_update_id,
            timestamp,
            book.is_snapshot,
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
            sent += 1;
        }
    }
    sent
}

fn publish_gate_derivatives(
    derivatives: Vec<gate_codec::Derivative>,
    timestamp_ms: bool,
    tx: &mpsc::UnboundedSender<Bytes>,
) -> usize {
    let mut sent = 0;
    for derivative in derivatives {
        let msg = match derivative {
            gate_codec::Derivative::MarkPrice {
                symbol,
                price,
                timestamp_us,
            } => MarkPriceMsg::create(
                symbol,
                price,
                if timestamp_ms {
                    us_to_ms(timestamp_us)
                } else {
                    timestamp_us
                },
            )
            .to_bytes(),
            gate_codec::Derivative::IndexPrice {
                symbol,
                price,
                timestamp_us,
            } => IndexPriceMsg::create(
                symbol,
                price,
                if timestamp_ms {
                    us_to_ms(timestamp_us)
                } else {
                    timestamp_us
                },
            )
            .to_bytes(),
            gate_codec::Derivative::FundingRate {
                symbol,
                funding_rate,
                next_funding_time_us,
                timestamp_us,
            } => FundingRateMsg::create(
                symbol,
                funding_rate,
                if timestamp_ms {
                    us_to_ms(next_funding_time_us)
                } else {
                    next_funding_time_us
                },
                if timestamp_ms {
                    us_to_ms(timestamp_us)
                } else {
                    timestamp_us
                },
            )
            .to_bytes(),
        };
        if tx.send(msg).is_ok() {
            sent += 1;
        }
    }
    sent
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
        if msg.first() != Some(&b'{') {
            return self.parse_sbe(&msg, tx);
        }
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

impl Default for GateTickerParser {
    fn default() -> Self {
        Self::new()
    }
}

impl GateTickerParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for GateTickerParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if msg.first() != Some(&b'{') {
            return self.parse_sbe(&msg, tx);
        }
        let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&msg) else {
            return 0;
        };
        let Some(bbo) = gate_codec::parse_bbo_json(&json_value) else {
            return 0;
        };
        let spread_msg = AskBidSpreadMsg::create(
            bbo.symbol,
            us_to_ms(bbo.timestamp_us),
            bbo.bid_price,
            bbo.bid_amount,
            bbo.ask_price,
            bbo.ask_amount,
        );
        if tx.send(spread_msg.to_bytes()).is_ok() {
            1
        } else {
            0
        }
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
        if msg.first() != Some(&b'{') {
            return self.parse_sbe(&msg, tx);
        }
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

impl Default for GateTradeParser {
    fn default() -> Self {
        Self::new()
    }
}

impl GateTradeParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for GateTradeParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if msg.first() != Some(&b'{') {
            return self.parse_sbe(&msg, tx);
        }
        let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&msg) else {
            return 0;
        };
        publish_gate_trades(gate_codec::parse_trades_json(&json_value), true, tx)
    }
}

#[derive(Clone)]
pub struct GateIncParser {
    max_levels: Option<usize>,
}

impl Default for GateIncParser {
    fn default() -> Self {
        Self::new()
    }
}

impl GateIncParser {
    pub fn new() -> Self {
        Self { max_levels: None }
    }

    pub fn with_max_levels(max_levels: Option<usize>) -> Self {
        Self { max_levels }
    }
}

impl Parser for GateIncParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if msg.first() != Some(&b'{') {
            return self.parse_sbe(&msg, tx);
        }
        let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&msg) else {
            return 0;
        };
        gate_codec::parse_incremental_json(&json_value)
            .iter()
            .map(|book| publish_gate_book(book, self.max_levels, true, tx))
            .sum()
    }
}

/// Gate.io 衍生品指标解析（基于 futures.tickers）
#[derive(Clone)]
pub struct GateDerivativesMetricsParser;

impl Default for GateDerivativesMetricsParser {
    fn default() -> Self {
        Self::new()
    }
}

impl GateDerivativesMetricsParser {
    pub fn new() -> Self {
        Self
    }
}

impl Parser for GateDerivativesMetricsParser {
    fn parse(&self, msg: Bytes, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        if msg.first() != Some(&b'{') {
            return self.parse_sbe(&msg, tx);
        }
        let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&msg) else {
            return 0;
        };
        publish_gate_derivatives(gate_codec::parse_derivatives_json(&json_value), true, tx)
    }
}

// ─── Gate SBE parser implementations ─────────────────────────────────────────

impl GateSignalParser {
    fn parse_sbe(&self, buf: &[u8], tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let (tid, _bl) = match sbe_hdr(buf) {
            Some(h) => h,
            None => return 0,
        };
        if tid != GATE_SBE_T_TICKER {
            return 0;
        }
        // root.time (µs) @ body+0
        let time_us = match sbe_i64(buf, GATE_SBE_HDR) {
            Some(t) => t,
            None => return 0,
        };
        let signal = SignalMsg::create(self.source, time_us / 1000);
        if tx.send(signal.to_bytes()).is_ok() {
            1
        } else {
            0
        }
    }
}

impl GateTickerParser {
    fn parse_sbe(&self, buf: &[u8], tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let Some(bbo) = gate_codec::parse_sbe_bbo(buf) else {
            return 0;
        };
        let msg = AskBidSpreadMsg::create(
            bbo.symbol,
            us_to_ms(bbo.timestamp_us),
            bbo.bid_price,
            bbo.bid_amount,
            bbo.ask_price,
            bbo.ask_amount,
        );
        if tx.send(msg.to_bytes()).is_ok() {
            1
        } else {
            0
        }
    }
}

impl GateKlineParser {
    fn parse_sbe(&self, buf: &[u8], tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let (tid, bl) = match sbe_hdr(buf) {
            Some(h) => h,
            None => return 0,
        };
        if tid != GATE_SBE_T_KLINE {
            return 0;
        }
        let b = GATE_SBE_HDR;
        if buf.len() < b + bl {
            return 0;
        }
        // root: time@0 e@8 pxExp@9 szExp@10 amountExp@11
        let px_exp = buf.get(b + 9).map(|&v| v as i8).unwrap_or(0);
        let sz_exp = buf.get(b + 10).map(|&v| v as i8).unwrap_or(0);

        let (el, n, mut cur) = match sbe_grp(buf, b + bl) {
            Some(g) => g,
            None => return 0,
        };
        let mut sent = 0;
        for _ in 0..n {
            if buf.len() < cur + el {
                break;
            }
            // entry fixed block: t@0 open@8 high@16 low@24 close@32 vol@40 amount@48 complete@56
            let t_sec = sbe_i64(buf, cur).unwrap_or(0);
            let open_m = sbe_i64(buf, cur + 8).unwrap_or(0);
            let high_m = sbe_i64(buf, cur + 16).unwrap_or(0);
            let low_m = sbe_i64(buf, cur + 24).unwrap_or(0);
            let close_m = sbe_i64(buf, cur + 32).unwrap_or(0);
            let vol_m = sbe_i64(buf, cur + 40).unwrap_or(0);
            let complete = buf.get(cur + 56).copied().unwrap_or(0);
            cur += el;
            // varData within entry: name (e.g. "1m_BTC_USDT")
            let (name, next_cur) = match sbe_vs(buf, cur) {
                Some(s) => s,
                None => break,
            };
            cur = next_cur;
            if self.only_closed && complete != 1 {
                continue;
            }
            // "1m_BTC_USDT" → skip period prefix → "BTC_USDT"
            let symbol = match name.find('_') {
                Some(pos) => name[pos + 1..].to_string(),
                None => name,
            };
            let msg = KlineMsg::create(
                symbol,
                sbe_m(open_m, px_exp),
                sbe_m(high_m, px_exp),
                sbe_m(low_m, px_exp),
                sbe_m(close_m, px_exp),
                sbe_m(vol_m, sz_exp),
                t_sec * 1000,
            );
            if tx.send(msg.to_bytes()).is_ok() {
                sent += 1;
            }
        }
        sent
    }
}

impl GateTradeParser {
    fn parse_sbe(&self, buf: &[u8], tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        publish_gate_trades(gate_codec::parse_sbe_trades(buf), false, tx)
    }
}

impl GateIncParser {
    fn parse_sbe(&self, buf: &[u8], tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        let (tid, bl) = match sbe_hdr(buf) {
            Some(h) => h,
            None => return 0,
        };
        match tid {
            GATE_SBE_T_OBU => self.sbe_obu(buf, bl, tx),
            GATE_SBE_T_BOOK => self.sbe_book(buf, bl, tx),
            GATE_SBE_T_BOOK_UPDATE => self.sbe_book_update(buf, bl, tx),
            _ => 0,
        }
    }

    fn sbe_obu(&self, buf: &[u8], bl: usize, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // root: time@0 e@8 t@9 full@17 firstID@18 lastID@26 pxExp@34 szExp@35
        let b = GATE_SBE_HDR;
        if buf.len() < b + bl {
            return 0;
        }
        let t_us = sbe_i64(buf, b + 9).unwrap_or(0);
        let full = buf.get(b + 17).copied().unwrap_or(0);
        let first_id = sbe_i64(buf, b + 18).unwrap_or(0);
        let last_id = sbe_i64(buf, b + 26).unwrap_or(0);
        let px_exp = buf[b + 34] as i8;
        let sz_exp = buf[b + 35] as i8;
        // bids group (id=100) then asks group (id=101)
        let (el_b, n_b, cur_b) = match sbe_grp(buf, b + bl) {
            Some(g) => g,
            None => return 0,
        };
        let bids = sbe_levels(buf, cur_b, el_b, n_b, px_exp, sz_exp);
        let (el_a, n_a, cur_a) = match sbe_grp(buf, cur_b + el_b * n_b) {
            Some(g) => g,
            None => return 0,
        };
        let asks = sbe_levels(buf, cur_a, el_a, n_a, px_exp, sz_exp);
        let mut off = cur_a + el_a * n_a;
        off = match sbe_vs_skip(buf, off) {
            Some(o) => o,
            None => return 0,
        }; // channel
        let (sym, _) = match sbe_vs(buf, off) {
            Some(s) => s,
            None => return 0,
        };
        self.emit_inc(
            sym,
            first_id,
            last_id,
            t_us / 1000,
            full == 1,
            bids,
            asks,
            tx,
        )
    }

    fn sbe_book(&self, buf: &[u8], bl: usize, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // root: time@0 e@8 t@9 id@17 pxExp@25 szExp@26 level@27
        let b = GATE_SBE_HDR;
        if buf.len() < b + bl {
            return 0;
        }
        let t_us = sbe_i64(buf, b + 9).unwrap_or(0);
        let snap_id = sbe_i64(buf, b + 17).unwrap_or(0);
        let px_exp = buf[b + 25] as i8;
        let sz_exp = buf[b + 26] as i8;
        // asks group (id=100) then bids group (id=101)
        let (el_a, n_a, cur_a) = match sbe_grp(buf, b + bl) {
            Some(g) => g,
            None => return 0,
        };
        let asks = sbe_levels(buf, cur_a, el_a, n_a, px_exp, sz_exp);
        let (el_b, n_b, cur_b) = match sbe_grp(buf, cur_a + el_a * n_a) {
            Some(g) => g,
            None => return 0,
        };
        let bids = sbe_levels(buf, cur_b, el_b, n_b, px_exp, sz_exp);
        let mut off = cur_b + el_b * n_b;
        off = match sbe_vs_skip(buf, off) {
            Some(o) => o,
            None => return 0,
        };
        let (sym, _) = match sbe_vs(buf, off) {
            Some(s) => s,
            None => return 0,
        };
        self.emit_inc(sym, snap_id, snap_id, t_us / 1000, true, bids, asks, tx)
    }

    fn sbe_book_update(&self, buf: &[u8], bl: usize, tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        // root: time@0 e@8 t@9 firstID@17 lastID@25 pxExp@33 szExp@34 level@35
        let b = GATE_SBE_HDR;
        if buf.len() < b + bl {
            return 0;
        }
        let t_us = sbe_i64(buf, b + 9).unwrap_or(0);
        let first_id = sbe_i64(buf, b + 17).unwrap_or(0);
        let last_id = sbe_i64(buf, b + 25).unwrap_or(0);
        let px_exp = buf[b + 33] as i8;
        let sz_exp = buf[b + 34] as i8;
        let e_byte = buf.get(b + 8).copied().unwrap_or(2);
        let is_snapshot = e_byte == 3; // Event::All
                                       // asks group (id=100) then bids group (id=101)
        let (el_a, n_a, cur_a) = match sbe_grp(buf, b + bl) {
            Some(g) => g,
            None => return 0,
        };
        let asks = sbe_levels(buf, cur_a, el_a, n_a, px_exp, sz_exp);
        let (el_b, n_b, cur_b) = match sbe_grp(buf, cur_a + el_a * n_a) {
            Some(g) => g,
            None => return 0,
        };
        let bids = sbe_levels(buf, cur_b, el_b, n_b, px_exp, sz_exp);
        let mut off = cur_b + el_b * n_b;
        off = match sbe_vs_skip(buf, off) {
            Some(o) => o,
            None => return 0,
        };
        let (sym, _) = match sbe_vs(buf, off) {
            Some(s) => s,
            None => return 0,
        };
        self.emit_inc(
            sym,
            first_id,
            last_id,
            t_us / 1000,
            is_snapshot,
            bids,
            asks,
            tx,
        )
    }

    fn emit_inc(
        &self,
        symbol: String,
        first_id: i64,
        last_id: i64,
        ts_ms: i64,
        is_snapshot: bool,
        bids: Vec<Level>,
        asks: Vec<Level>,
        tx: &mpsc::UnboundedSender<Bytes>,
    ) -> usize {
        if bids.is_empty() && asks.is_empty() {
            return 0;
        }
        let chunks = split_levels(bids.len(), asks.len(), self.max_levels);
        let total = chunks.len();
        let mut sent = 0;
        for (idx, (b_start, b_count, a_start, a_count)) in chunks.into_iter().enumerate() {
            let mut msg = IncMsg::create(
                symbol.clone(),
                first_id,
                last_id,
                ts_ms,
                is_snapshot,
                b_count as u32,
                a_count as u32,
            );
            msg.set_chunk_index(idx as u8);
            msg.set_is_last(idx == total - 1);
            fill_levels_with_offset(&bids, &asks, b_start, b_count, a_start, a_count, &mut msg);
            if tx.send(msg.to_bytes()).is_ok() {
                sent += 1;
            }
        }
        sent
    }
}

impl GateDerivativesMetricsParser {
    fn parse_sbe(&self, buf: &[u8], tx: &mpsc::UnboundedSender<Bytes>) -> usize {
        publish_gate_derivatives(gate_codec::parse_sbe_derivatives(buf), true, tx)
    }
}

// ─── Gate SBE helpers ─────────────────────────────────────────────────────────

/// Parse SBE header. Returns (template_id, block_length) or None if wrong schema / too short.
fn sbe_hdr(buf: &[u8]) -> Option<(u16, usize)> {
    if buf.len() < GATE_SBE_HDR {
        return None;
    }
    let bl = u16::from_le_bytes([buf[0], buf[1]]) as usize;
    let tid = u16::from_le_bytes([buf[2], buf[3]]);
    let sid = u16::from_le_bytes([buf[4], buf[5]]);
    if sid != GATE_SBE_SCHEMA {
        return None;
    }
    Some((tid, bl))
}

#[inline]
fn sbe_i64(buf: &[u8], off: usize) -> Option<i64> {
    buf.get(off..off + 8)
        .and_then(|s| s.try_into().ok())
        .map(i64::from_le_bytes)
}

#[inline]
fn sbe_m(m: i64, exp: i8) -> f64 {
    (m as f64) * 10f64.powi(exp as i32)
}

/// varString8: 1B length + UTF-8 data. Returns (string, next_offset).
fn sbe_vs(buf: &[u8], off: usize) -> Option<(String, usize)> {
    let n = *buf.get(off)? as usize;
    let s = std::str::from_utf8(buf.get(off + 1..off + 1 + n)?)
        .ok()?
        .to_owned();
    Some((s, off + 1 + n))
}

/// Skip varString8, return next_offset.
fn sbe_vs_skip(buf: &[u8], off: usize) -> Option<usize> {
    let n = *buf.get(off)? as usize;
    buf.get(off + 1..off + 1 + n)?;
    Some(off + 1 + n)
}

/// groupSize16Encoding: (entry_block_len, num_in_group, off_after_header).
fn sbe_grp(buf: &[u8], off: usize) -> Option<(usize, usize, usize)> {
    if buf.len() < off + 4 {
        return None;
    }
    let el = u16::from_le_bytes([buf[off], buf[off + 1]]) as usize;
    let n = u16::from_le_bytes([buf[off + 2], buf[off + 3]]) as usize;
    Some((el, n, off + 4))
}

/// Decode `n` orderbook levels starting at `off`, each `el` bytes: pxMantissa i64 + szMantissa i64.
fn sbe_levels(buf: &[u8], off: usize, el: usize, n: usize, px_exp: i8, sz_exp: i8) -> Vec<Level> {
    let mut out = Vec::with_capacity(n);
    let mut cur = off;
    for _ in 0..n {
        if buf.len() < cur + 16 {
            break;
        }
        if let (Some(pm), Some(sm)) = (sbe_i64(buf, cur), sbe_i64(buf, cur + 8)) {
            let price = sbe_m(pm, px_exp);
            let amount = sbe_m(sm, sz_exp);
            if price > 0.0 {
                out.push(Level::from_values(price, amount));
            }
        }
        cur += el;
    }
    out
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
