use anyhow::Result;
use bytes::Bytes;
use mkt_parsers::binance::{RawBbo, RawBookParse, RawTrade};
use serde_json::Value;

use mkt_parsers::msg::mkt_msg::Level;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

/// 各家 spread 解析后的统一中间表示。
#[derive(Debug, Clone)]
pub struct BboFrame {
    /// 归一化后的 symbol（去 `-`/`-SWAP`、统一大写，例如 `BTCUSDT`）。
    pub symbol: String,
    /// 服务器时间戳，微秒（µs）。各 adapter 在解析点把交易所原生 ms 升精度到 us，
    /// 全链路下游（Quote.ts / TradingLeg.ts / OrderTimeStamp.mkt_t）统一 µs。
    /// 部分 venue 没有事件时间时（例如 Binance 现货 bookTicker 缺 `E/T`）填 0。
    pub ts_us: i64,
    /// 单 symbol 内严格单调递增的序号（双路去重用）。
    /// 各 venue 字段各异：
    /// - OKex bbo-tbt:    `data[].seqId`
    /// - Binance depth5/bookTicker: `u`
    /// - Bybit  orderbook.1: `data.u`
    /// - Gate  *.book_ticker: `result.u`
    /// - Bitget books1:    `data[].seq` (字符串)
    pub seq_id: i64,
    /// True when this frame is a venue snapshot that may reset the global sequence baseline.
    /// Bybit can send a fresh snapshot with `u=1` after backend restart; the shared de-dup layer
    /// clears its high-water marks when such a snapshot moves below the previous `u`.
    pub reset_seq: bool,
    pub bid_price: f64,
    pub bid_amount: f64,
    pub ask_price: f64,
    pub ask_amount: f64,
}

/// 各家逐笔成交解析后的统一中间表示。
#[derive(Debug, Clone)]
pub struct TradeFrame {
    /// 归一化后的 symbol（去 `-`/`-SWAP`、统一大写，例如 `BTCUSDT`）。
    pub symbol: String,
    /// 交易所时间戳，微秒（us）。OKX SBE 使用 outTime 原样填入。
    pub timestamp_us: i64,
    pub seq_id: i64,
    pub trade_id: i64,
    pub side: char,
    pub price: f64,
    pub amount: f64,
}

/// SBE incremental orderbook event decoded to the existing dat_pbs IncMsg semantics.
#[derive(Debug, Clone)]
pub enum IncrementalFrame {
    Book {
        symbol: String,
        timestamp: i64,
        seq_id: i64,
        prev_seq_id: i64,
        first_update_id: i64,
        final_update_id: i64,
        gap_check: bool,
        is_snapshot: bool,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    SequenceOnly {
        symbol: String,
        timestamp: i64,
        seq_id: i64,
        prev_seq_id: i64,
    },
}

/// 心跳策略。`ws.rs` 按 `interval` 周期触发 `build()`，None 表示不主动 keepalive。
pub struct KeepaliveSpec {
    pub interval: Duration,
    pub build: Box<dyn Fn() -> Message>,
}

impl KeepaliveSpec {
    pub fn text(interval: Duration, payload: impl Into<String>) -> Self {
        let s = payload.into();
        Self {
            interval,
            build: Box::new(move || Message::Text(s.clone())),
        }
    }

    pub fn dynamic(interval: Duration, build: impl Fn() -> Message + 'static) -> Self {
        Self {
            interval,
            build: Box::new(build),
        }
    }
}

/// 单 venue 的连接 + 解析 + 心跳适配器。
///
/// `current_thread` runtime 下被 `Rc<dyn VenueAdapter>` 共享给两条 ws task，
/// 因此 trait 不要求 `Send`/`Sync`。
///
/// `parse_frame` 接收 `app.rs` 已经 `serde_json` 解析过的 `&Value`，
/// 避免双路竞速场景下同一帧被重复 `from_str`。BBO 走 emit callback，
/// 避免热路径为单条 frame 构造 `Vec<BboFrame>`。
/// 双路去重统一基于 `BboFrame.seq_id` 在 `process_frame` 内完成。
pub trait VenueAdapter {
    fn name(&self) -> &'static str;
    fn ws_url(&self) -> String;
    fn ws_headers(&self) -> Vec<(String, String)> {
        Vec::new()
    }
    fn build_subscribe(&self, symbols: &[String]) -> Vec<Value>;
    fn build_trade_subscribe(&self, _symbols: &[String]) -> Vec<Value> {
        Vec::new()
    }
    fn build_incremental_subscribe(&self, _symbols: &[String]) -> Vec<Value> {
        Vec::new()
    }
    fn build_derivatives_subscribe(&self, _symbols: &[String]) -> Vec<Value> {
        Vec::new()
    }
    /// Optional symbol-table hook for adapters that keep per-symbol hot-path state.
    fn seed_symbols(&self, _symbols: &[String]) {}
    fn symbol_slot_index(&self, _symbol: &str) -> Option<usize> {
        None
    }
    /// Some replacement channels live on a different public endpoint from BBO.
    /// None means use `ws_url()` and the main dual legs.
    fn trade_ws_url(&self) -> Option<String> {
        None
    }
    fn incremental_ws_url(&self) -> Option<String> {
        None
    }
    fn derivatives_ws_url(&self) -> Option<String> {
        None
    }
    fn inst_id_code(&self, _symbol: &str) -> Option<i64> {
        None
    }
    fn parse_frame(
        &self,
        value: &Value,
        emit: &mut dyn FnMut(BboFrame) -> Result<()>,
    ) -> Result<()>;
    /// Optional raw fast path for fixed-shape BBO frames. Return Ok(true) when the
    /// frame was fully handled and the caller can skip `serde_json::Value`.
    fn parse_bbo_raw(
        &self,
        _raw: &[u8],
        _emit: &mut dyn FnMut(BboFrame) -> Result<()>,
    ) -> Result<bool> {
        Ok(false)
    }
    /// Optional borrowed raw BBO parser for hot paths that can consume fields
    /// directly and avoid allocating the intermediate `BboFrame.symbol`.
    fn parse_bbo_raw_borrowed<'a>(&self, _raw: &'a [u8]) -> Option<RawBbo<'a>> {
        None
    }
    /// Optional borrowed raw trade parser for hot paths that can consume fields
    /// directly and avoid `serde_json::Value`, `Vec<TradeFrame>`, and symbol `String`.
    fn parse_trade_raw_borrowed<'a>(&self, _raw: &'a [u8]) -> Option<RawTrade<'a>> {
        None
    }
    /// Optional borrowed raw incremental parser for fixed-shape orderbook deltas.
    /// This lets callers publish directly from stack-backed levels.
    fn parse_incremental_raw<'a>(&self, _raw: &'a [u8]) -> Option<RawBookParse<'a>> {
        None
    }
    fn parse_trade_frame(&self, _value: &Value) -> Result<Vec<TradeFrame>> {
        Ok(Vec::new())
    }
    fn parse_incremental_frame(&self, _value: &Value) -> Result<Vec<IncrementalFrame>> {
        Ok(Vec::new())
    }
    fn parse_derivatives_frame(&self, _value: &Value) -> Result<Vec<Bytes>> {
        Ok(Vec::new())
    }
    /// Return true for JSON feeds whose hot path is intentionally raw-only. When
    /// a raw parser misses, callers should drop the frame instead of allocating
    /// a fallback `serde_json::Value`.
    fn skip_json_fallback_after_raw_miss(&self) -> bool {
        false
    }
    /// Optional raw derivatives hot path. Return encoded messages when the
    /// frame was fully handled so shared A/B dedup runs before IPC publication.
    fn parse_derivatives_raw(
        &self,
        _raw: &[u8],
        _symbol_slot: &mut dyn FnMut(&str) -> Option<usize>,
    ) -> Option<Vec<Bytes>> {
        None
    }
    fn parse_binary_frame(
        &self,
        _raw: &[u8],
        _emit: &mut dyn FnMut(BboFrame) -> Result<()>,
    ) -> Result<()> {
        Ok(())
    }
    fn parse_trade_binary_frame(&self, _raw: &[u8]) -> Result<Vec<TradeFrame>> {
        Ok(Vec::new())
    }
    fn parse_incremental_binary_frame(&self, _raw: &[u8]) -> Result<Vec<IncrementalFrame>> {
        Ok(Vec::new())
    }
    fn parse_derivatives_binary_frame(&self, _raw: &[u8]) -> Result<Vec<Bytes>> {
        Ok(Vec::new())
    }
    /// 返回 None 表示完全依赖服务端 ws-Ping/Pong；返回 Some 表示主动按 interval 发心跳。
    fn keepalive(&self) -> Option<KeepaliveSpec>;

    #[cfg(test)]
    fn collect_frame(&self, value: &Value) -> Result<Vec<BboFrame>>
    where
        Self: Sized,
    {
        let mut out = Vec::new();
        self.parse_frame(value, &mut |frame| {
            out.push(frame);
            Ok(())
        })?;
        Ok(out)
    }

    #[cfg(test)]
    fn collect_binary_frame(&self, raw: &[u8]) -> Result<Vec<BboFrame>>
    where
        Self: Sized,
    {
        let mut out = Vec::new();
        self.parse_binary_frame(raw, &mut |frame| {
            out.push(frame);
            Ok(())
        })?;
        Ok(out)
    }
}

/// 按 venue 创建对应 adapter；非支持的 venue 返回 Ok(None)。
///
/// 当前支持：OKex / Binance / Bybit / Gate / Bitget（spot+futures 各 2 个）。
/// Hyperliquid / Aster 为 DEX，spread_pbs 不接入。
///
/// OKex 必须 await：SBE 端订阅要先 REST 拉 instIdCode 映射；其他 venue 同步构造。
pub async fn create_adapter(
    venue: order_common::TradingVenue,
) -> Result<Option<Box<dyn VenueAdapter>>> {
    use order_common::TradingVenue;
    let adapter: Box<dyn VenueAdapter> = match venue {
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
            Box::new(crate::spread_pbs::okex::OkexAdapter::new(venue).await?)
        }
        TradingVenue::BinanceMargin | TradingVenue::BinanceFutures => {
            Box::new(crate::spread_pbs::binance::BinanceAdapter::new(venue))
        }
        TradingVenue::BybitMargin | TradingVenue::BybitFutures => {
            Box::new(crate::spread_pbs::bybit::BybitAdapter::new(venue))
        }
        TradingVenue::GateMargin => {
            Box::new(crate::spread_pbs::gate_sbe::GateSpotSbeAdapter::new())
        }
        TradingVenue::GateFutures => Box::new(crate::spread_pbs::gate_sbe::GateSbeAdapter::new()),
        TradingVenue::BitgetMargin | TradingVenue::BitgetFutures => {
            Box::new(crate::spread_pbs::bitget::BitgetAdapter::new(venue))
        }
        _ => return Ok(None),
    };
    Ok(Some(adapter))
}
