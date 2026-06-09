//! Bitget UTA v3 SBE `books1` spread 适配器。
//!
//! - WS URL: `wss://ws.bitget.com/v3/ws/public/sbe`
//! - 鉴权:   **不需要**（公开行情，与 OKEx SBE 不同）
//! - 订阅:   JSON 文本，UTA v3 命名: `topic` + `symbol` + 小写 `instType`
//!           `{"op":"subscribe","args":[{"instType":"usdt-futures","topic":"books1","symbol":"BTCUSDT"}]}`
//! - 数据帧: WebSocket 二进制帧 (opcode=2)，littleEndian
//!           header 8B + root 64B + varString8 symbol  ≈ 80B/帧
//!           BBO templateId=1002, schemaId=1, schemaVer=3
//! - 时间戳: 使用 SBE `sts` 填 `BboFrame.ts_us`（gateway 推送时刻，与现行 JSON `ts*1000` 同语义）
//!           SBE 多出的 `ts` (撮合时刻，类似 binance T) 暂未消费
//! - 心跳:   text "ping" 每 25s（与 v2 一致, ws.rs 默认行为）
//!
//! schema 参考: https://www.bitget.com/api-doc/uta/sbe/sbe-bbo

use anyhow::Result;
use bytes::Bytes;
use mkt_parsers::bitget as bitget_codec;
use serde_json::Value;
use std::time::Duration;

use crate::spread_pbs::adapter::{
    BboFrame, IncrementalFrame, KeepaliveSpec, TradeFrame, VenueAdapter,
};
use mkt_parsers::msg::mkt_msg::{FundingRateMsg, IndexPriceMsg, Level, MarkPriceMsg};
use order_common::TradingVenue;

const BITGET_SBE_WS_URL: &str = "wss://ws.bitget.com/v3/ws/public/sbe";
const BITGET_V2_WS_URL: &str = "wss://ws.bitget.com/v2/ws/public";
const BITGET_SUBSCRIBE_CHUNK: usize = 50;

pub struct BitgetAdapter {
    venue: TradingVenue,
}

impl BitgetAdapter {
    pub fn new(venue: TradingVenue) -> Self {
        Self { venue }
    }

    /// UTA v3 小写 instType。
    fn inst_type(&self) -> &'static str {
        match self.venue {
            TradingVenue::BitgetMargin => "spot",
            TradingVenue::BitgetFutures => "usdt-futures",
            other => unreachable!("BitgetAdapter created with non-bitget venue: {:?}", other),
        }
    }
}

impl VenueAdapter for BitgetAdapter {
    fn name(&self) -> &'static str {
        "bitget"
    }

    fn ws_url(&self) -> String {
        BITGET_SBE_WS_URL.to_string()
    }

    fn build_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        self.build_channel_subscribe(symbols, "books1")
    }

    fn build_trade_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        self.build_channel_subscribe(symbols, "publicTrade")
    }

    fn build_incremental_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        build_v2_subscribe(self.venue, symbols, "books")
    }

    fn build_derivatives_subscribe(&self, symbols: &[String]) -> Vec<Value> {
        if self.venue != TradingVenue::BitgetFutures {
            return Vec::new();
        }
        build_v2_subscribe(self.venue, symbols, "ticker")
    }

    fn incremental_ws_url(&self) -> Option<String> {
        Some(BITGET_V2_WS_URL.to_string())
    }

    fn derivatives_ws_url(&self) -> Option<String> {
        if self.venue == TradingVenue::BitgetFutures {
            Some(BITGET_V2_WS_URL.to_string())
        } else {
            None
        }
    }

    fn parse_frame(&self, _value: &Value) -> Result<Vec<BboFrame>> {
        // SBE 端的 text 帧只有 subscribe ack / error event; 静默忽略
        Ok(Vec::new())
    }

    fn parse_incremental_frame(&self, value: &Value) -> Result<Vec<IncrementalFrame>> {
        parse_v2_incremental(value)
    }

    fn parse_derivatives_frame(&self, value: &Value) -> Result<Vec<Bytes>> {
        parse_v2_derivatives(value)
    }

    fn parse_binary_frame(&self, raw: &[u8]) -> Result<Vec<BboFrame>> {
        parse_sbe_books1(raw)
    }

    fn parse_trade_binary_frame(&self, raw: &[u8]) -> Result<Vec<TradeFrame>> {
        parse_sbe_public_trade(raw)
    }

    fn keepalive(&self) -> Option<KeepaliveSpec> {
        // text "ping" 每 25s; pong 文本 ws.rs::is_keepalive_response 已识别跳过
        Some(KeepaliveSpec::text(Duration::from_secs(25), "ping"))
    }
}

fn v2_inst_type(venue: TradingVenue) -> &'static str {
    match venue {
        TradingVenue::BitgetMargin => "SPOT",
        TradingVenue::BitgetFutures => "USDT-FUTURES",
        other => unreachable!("Bitget v2 called for non-bitget venue: {:?}", other),
    }
}

fn build_v2_subscribe(venue: TradingVenue, symbols: &[String], channel: &str) -> Vec<Value> {
    let inst_type = v2_inst_type(venue);
    let mut out = Vec::new();
    for chunk in symbols.chunks(BITGET_SUBSCRIBE_CHUNK.max(1)) {
        let args: Vec<Value> = chunk
            .iter()
            .map(|symbol| {
                serde_json::json!({
                    "instType": inst_type,
                    "channel": channel,
                    "instId": symbol,
                })
            })
            .collect();
        out.push(serde_json::json!({
            "op": "subscribe",
            "args": args,
        }));
    }
    out
}

impl BitgetAdapter {
    fn build_channel_subscribe(&self, symbols: &[String], topic: &str) -> Vec<Value> {
        let chunk_size = BITGET_SUBSCRIBE_CHUNK.max(1);
        let inst_type = self.inst_type();
        let mut out = Vec::new();
        for chunk in symbols.chunks(chunk_size) {
            let args: Vec<Value> = chunk
                .iter()
                .map(|sym| {
                    serde_json::json!({
                        "instType": inst_type,
                        "topic": topic,
                        "symbol": sym,
                    })
                })
                .collect();
            out.push(serde_json::json!({
                "op": "subscribe",
                "args": args,
            }));
        }
        out
    }
}

fn parse_v2_incremental(value: &Value) -> Result<Vec<IncrementalFrame>> {
    Ok(bitget_codec::parse_incremental_v2_json(value)
        .into_iter()
        .map(book_to_incremental)
        .collect())
}

fn parse_v2_derivatives(value: &Value) -> Result<Vec<Bytes>> {
    let mut out = Vec::new();
    for derivative in bitget_codec::parse_derivatives_v2_json(value) {
        out.push(derivative_to_bytes(derivative));
    }
    Ok(out)
}

/// SBE books1 (templateId=1002) 解码。其他 template 直接返回空 Vec。
fn parse_sbe_books1(raw: &[u8]) -> Result<Vec<BboFrame>> {
    bitget_codec::parse_sbe_books1(raw)
        .map(|frames| frames.into_iter().map(bbo_to_frame).collect())
        .map_err(Into::into)
}

/// SBE publicTrade (templateId=1003) 解码。其他 template 直接返回空 Vec。
fn parse_sbe_public_trade(raw: &[u8]) -> Result<Vec<TradeFrame>> {
    bitget_codec::parse_sbe_public_trades(raw)
        .map(|trades| trades.into_iter().map(trade_to_frame).collect())
        .map_err(Into::into)
}

fn bbo_to_frame(bbo: bitget_codec::Bbo) -> BboFrame {
    BboFrame {
        symbol: bbo.symbol,
        ts_us: bbo.timestamp_us,
        seq_id: bbo.seq_id,
        reset_seq: false,
        bid_price: bbo.bid_price,
        bid_amount: bbo.bid_amount,
        ask_price: bbo.ask_price,
        ask_amount: bbo.ask_amount,
    }
}

fn trade_to_frame(trade: bitget_codec::Trade) -> TradeFrame {
    TradeFrame {
        symbol: trade.symbol,
        timestamp_us: trade.timestamp_us,
        seq_id: trade.seq_id,
        trade_id: trade.trade_id,
        side: trade.side,
        price: trade.price,
        amount: trade.amount,
    }
}

fn book_to_incremental(book: bitget_codec::Book) -> IncrementalFrame {
    IncrementalFrame::Book {
        symbol: book.symbol,
        timestamp: book.timestamp_us,
        seq_id: book.seq_id,
        prev_seq_id: book.prev_seq_id,
        first_update_id: book.first_update_id,
        final_update_id: book.final_update_id,
        gap_check: book.gap_check,
        is_snapshot: book.is_snapshot,
        bids: book_levels_to_msg(book.bids),
        asks: book_levels_to_msg(book.asks),
    }
}

fn derivative_to_bytes(derivative: bitget_codec::Derivative) -> Bytes {
    match derivative {
        bitget_codec::Derivative::MarkPrice {
            symbol,
            price,
            timestamp_us,
        } => MarkPriceMsg::create(symbol, price, timestamp_us).to_bytes(),
        bitget_codec::Derivative::IndexPrice {
            symbol,
            price,
            timestamp_us,
        } => IndexPriceMsg::create(symbol, price, timestamp_us).to_bytes(),
        bitget_codec::Derivative::FundingRate {
            symbol,
            funding_rate,
            next_funding_time_us,
            timestamp_us,
        } => FundingRateMsg::create(symbol, funding_rate, next_funding_time_us, timestamp_us)
            .to_bytes(),
    }
}

fn book_levels_to_msg(levels: Vec<bitget_codec::Level>) -> Vec<Level> {
    levels
        .into_iter()
        .map(|level| Level::from_values(level.price, level.amount))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_sbe_bbo_frame(
        ts_us: i64,
        bid_px_m: i64,
        bid_sz_m: i64,
        ask_px_m: i64,
        ask_sz_m: i64,
        px_exp: i8,
        sz_exp: i8,
        seq_id: i64,
        sts_us: i64,
        symbol: &str,
    ) -> Vec<u8> {
        let block_length: u16 = 64;
        let mut buf = Vec::with_capacity(80);
        buf.extend_from_slice(&block_length.to_le_bytes());
        buf.extend_from_slice(&bitget_codec::SBE_TEMPLATE_BOOKS1.to_le_bytes());
        buf.extend_from_slice(&bitget_codec::SBE_SCHEMA_ID.to_le_bytes());
        buf.extend_from_slice(&3u16.to_le_bytes());
        buf.extend_from_slice(&ts_us.to_le_bytes());
        buf.extend_from_slice(&bid_px_m.to_le_bytes());
        buf.extend_from_slice(&bid_sz_m.to_le_bytes());
        buf.extend_from_slice(&ask_px_m.to_le_bytes());
        buf.extend_from_slice(&ask_sz_m.to_le_bytes());
        buf.push(px_exp as u8);
        buf.push(sz_exp as u8);
        buf.extend_from_slice(&seq_id.to_le_bytes());
        buf.extend_from_slice(&sts_us.to_le_bytes());
        buf.push(1);
        buf.extend_from_slice(&[0u8; 5]);
        let sym_bytes = symbol.as_bytes();
        buf.push(sym_bytes.len() as u8);
        buf.extend_from_slice(sym_bytes);
        buf
    }

    fn build_sbe_trade_frame(
        px_exp: i8,
        sz_exp: i8,
        entries: &[(i64, i64, i64, i64, u8)],
        symbol: &str,
    ) -> Vec<u8> {
        let block_length: u16 = 16;
        let entry_block_len: u16 = 40;
        let mut buf = Vec::with_capacity(128);
        buf.extend_from_slice(&block_length.to_le_bytes());
        buf.extend_from_slice(&bitget_codec::SBE_TEMPLATE_PUBLIC_TRADE.to_le_bytes());
        buf.extend_from_slice(&bitget_codec::SBE_SCHEMA_ID.to_le_bytes());
        buf.extend_from_slice(&3u16.to_le_bytes());
        buf.push(px_exp as u8);
        buf.push(sz_exp as u8);
        buf.extend_from_slice(&1_700_000_000_000_500i64.to_le_bytes());
        buf.extend_from_slice(&[0u8; 6]);
        buf.extend_from_slice(&entry_block_len.to_le_bytes());
        buf.extend_from_slice(&(entries.len() as u16).to_le_bytes());
        for (ts_us, exec_id, price_m, size_m, side) in entries {
            buf.extend_from_slice(&ts_us.to_le_bytes());
            buf.extend_from_slice(&exec_id.to_le_bytes());
            buf.extend_from_slice(&price_m.to_le_bytes());
            buf.extend_from_slice(&size_m.to_le_bytes());
            buf.push(*side);
            buf.extend_from_slice(&[0u8; 7]);
        }
        buf.push(symbol.len() as u8);
        buf.extend_from_slice(symbol.as_bytes());
        buf
    }

    #[test]
    fn decode_books1_with_negative_exponents() {
        // BTC: bid=77635.7, ask=77635.8; size 9.3708 / 3.3373
        let raw = build_sbe_bbo_frame(
            1_700_000_000_000_000,
            776_357, // bid_px_m  → 77635.7
            93_708,  // bid_sz_m  → 9.3708
            776_358, // ask_px_m  → 77635.8
            33_373,  // ask_sz_m  → 3.3373
            -1,
            -4,
            587_635_700_001,
            1_700_000_000_001_500,
            "BTCUSDT",
        );
        let frames = parse_sbe_books1(&raw).expect("decode ok");
        assert_eq!(frames.len(), 1);
        let f = &frames[0];
        assert_eq!(f.symbol, "BTCUSDT");
        assert_eq!(f.seq_id, 587_635_700_001);
        assert_eq!(f.ts_us, 1_700_000_000_001_500); // 取 sts，不取 ts
        assert!((f.bid_price - 77635.7).abs() < 1e-6);
        assert!((f.ask_price - 77635.8).abs() < 1e-6);
        assert!((f.bid_amount - 9.3708).abs() < 1e-9);
        assert!((f.ask_amount - 3.3373).abs() < 1e-9);
    }

    #[test]
    fn rejects_unknown_schema() {
        let mut raw = build_sbe_bbo_frame(0, 1, 1, 1, 1, 0, 0, 1, 0, "BTCUSDT");
        raw[4] = 9; // schemaId
        let err = parse_sbe_books1(&raw).unwrap_err();
        assert!(err.to_string().contains("schemaId"));
    }

    #[test]
    fn unknown_template_returns_empty() {
        let mut raw = build_sbe_bbo_frame(0, 1, 1, 1, 1, 0, 0, 1, 0, "BTCUSDT");
        raw[2] = 0xE9;
        raw[3] = 0x03; // → 1001 (Depth50)
        assert!(parse_sbe_books1(&raw).unwrap().is_empty());
    }

    #[test]
    fn subscribe_uses_uta_v3_naming() {
        let a = BitgetAdapter::new(TradingVenue::BitgetFutures);
        let msgs = a.build_subscribe(&["BTCUSDT".to_string()]);
        assert_eq!(msgs.len(), 1);
        let arg = &msgs[0]["args"][0];
        assert_eq!(arg["instType"], "usdt-futures");
        assert_eq!(arg["topic"], "books1");
        assert_eq!(arg["symbol"], "BTCUSDT");
        // 关键：不应该有 v2 风格的 channel/instId
        assert!(arg.get("channel").is_none());
        assert!(arg.get("instId").is_none());

        let a_spot = BitgetAdapter::new(TradingVenue::BitgetMargin);
        let msgs_spot = a_spot.build_subscribe(&["BTCUSDT".to_string()]);
        assert_eq!(msgs_spot[0]["args"][0]["instType"], "spot");
    }

    #[test]
    fn trade_subscribe_uses_public_trade_topic() {
        let a = BitgetAdapter::new(TradingVenue::BitgetFutures);
        let msgs = a.build_trade_subscribe(&["BTCUSDT".to_string()]);
        assert_eq!(msgs.len(), 1);
        let arg = &msgs[0]["args"][0];
        assert_eq!(arg["instType"], "usdt-futures");
        assert_eq!(arg["topic"], "publicTrade");
        assert_eq!(arg["symbol"], "BTCUSDT");
    }

    #[test]
    fn decodes_public_trade_with_microsecond_ts() {
        let raw = build_sbe_trade_frame(
            -1,
            -4,
            &[
                (1_700_000_000_123_456, 9001, 776_357, 93_708, 0),
                (1_700_000_000_123_789, 9002, 776_358, 33_373, 1),
            ],
            "BTCUSDT",
        );
        let trades = parse_sbe_public_trade(&raw).expect("decode trade ok");
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].symbol, "BTCUSDT");
        assert_eq!(trades[0].timestamp_us, 1_700_000_000_123_456);
        assert_eq!(trades[0].trade_id, 9001);
        assert_eq!(trades[0].seq_id, 9001);
        assert_eq!(trades[0].side, 'B');
        assert!((trades[0].price - 77635.7).abs() < 1e-6);
        assert!((trades[0].amount - 9.3708).abs() < 1e-9);
        assert_eq!(trades[1].side, 'S');
    }

    #[test]
    fn decodes_v2_incremental_with_microsecond_ts() {
        let raw = serde_json::json!({
            "action": "update",
            "arg": {"instType": "USDT-FUTURES", "channel": "books", "instId": "BTCUSDT"},
            "data": [{
                "ts": "1700000000123",
                "seq": 9001,
                "bids": [["100", "1"]],
                "asks": [["101", "2"]]
            }]
        });
        let frames = parse_v2_incremental(&raw).unwrap();
        assert_eq!(frames.len(), 1);
        let IncrementalFrame::Book {
            symbol,
            timestamp,
            seq_id,
            gap_check,
            bids,
            asks,
            ..
        } = &frames[0]
        else {
            panic!("expected book frame");
        };
        assert_eq!(symbol, "BTCUSDT");
        assert_eq!(*timestamp, 1_700_000_000_123_000);
        assert_eq!(*seq_id, 9001);
        assert!(!*gap_check);
        assert_eq!(bids.len(), 1);
        assert_eq!(asks.len(), 1);
    }

    #[test]
    fn decodes_v2_derivatives_with_microsecond_ts() {
        let raw = serde_json::json!({
            "arg": {"instType": "USDT-FUTURES", "channel": "ticker", "instId": "BTCUSDT"},
            "data": [{
                "ts": "1700000000123",
                "fundingRate": "0.0001",
                "nextFundingTime": "1700003600000",
                "markPrice": "100.1",
                "indexPrice": "99.9"
            }]
        });
        let bytes = parse_v2_derivatives(&raw).unwrap();
        assert_eq!(bytes.len(), 3);
        assert_eq!(
            FundingRateMsg::get_timestamp(&bytes[0]),
            1_700_000_000_123_000
        );
        assert_eq!(
            FundingRateMsg::get_next_funding_time(&bytes[0]),
            1_700_003_600_000_000
        );
        assert_eq!(
            MarkPriceMsg::get_timestamp(&bytes[1]),
            1_700_000_000_123_000
        );
        assert_eq!(
            IndexPriceMsg::get_timestamp(&bytes[2]),
            1_700_000_000_123_000
        );
    }

    #[test]
    fn subscribe_chunks_50() {
        let a = BitgetAdapter::new(TradingVenue::BitgetFutures);
        let symbols: Vec<String> = (0..120).map(|i| format!("SYM{}USDT", i)).collect();
        let msgs = a.build_subscribe(&symbols);
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[0]["args"].as_array().unwrap().len(), 50);
    }
}
