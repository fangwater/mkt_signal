use anyhow::Result;
use serde_json::Value;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

/// 各家 spread 解析后的统一中间表示。
#[derive(Debug, Clone)]
pub struct BboFrame {
    /// 归一化后的 symbol（去 `-`/`-SWAP`、统一大写，例如 `BTCUSDT`）。
    pub symbol: String,
    /// 服务器时间戳，毫秒（部分 venue 没有就填 0）。
    pub ts_ms: i64,
    /// 单 symbol 内严格单调递增的序号（双路去重用）。
    /// 各 venue 字段各异：
    /// - OKex bbo-tbt:    `data[].seqId`
    /// - Binance depth5/bookTicker: `u`
    /// - Bybit  orderbook.1: `data.u`
    /// - Gate  *.book_ticker: `result.u`
    /// - Bitget books1:    `data[].seq` (字符串)
    pub seq_id: i64,
    pub bid_price: f64,
    pub bid_amount: f64,
    pub ask_price: f64,
    pub ask_amount: f64,
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
pub trait VenueAdapter {
    fn name(&self) -> &'static str;
    fn ws_url(&self) -> String;
    fn build_subscribe(&self, symbols: &[String]) -> Vec<Value>;
    /// 轻量提取 `(symbol, seq_id)`，用于双路竞速时在完整解析前丢弃落后帧。
    fn seq_hint(&self, _raw: &str) -> Result<Option<(String, i64)>> {
        Ok(None)
    }
    fn parse_frame(&self, raw: &str) -> Result<Vec<BboFrame>>;
    /// 返回 None 表示完全依赖服务端 ws-Ping/Pong；返回 Some 表示主动按 interval 发心跳。
    fn keepalive(&self) -> Option<KeepaliveSpec>;
}

/// 按 venue 创建对应 adapter；非支持的 venue 返回 None。
///
/// 当前支持：OKex / Binance / Bybit / Gate / Bitget（spot+futures 各 2 个）。
/// Hyperliquid / Aster 为 DEX，spread_pbs 不接入。
pub fn create_adapter(venue: crate::signal::common::TradingVenue) -> Option<Box<dyn VenueAdapter>> {
    use crate::signal::common::TradingVenue;
    match venue {
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
            Some(Box::new(crate::spread_pbs::okex::OkexAdapter::new(venue)))
        }
        TradingVenue::BinanceMargin | TradingVenue::BinanceFutures => Some(Box::new(
            crate::spread_pbs::binance::BinanceAdapter::new(venue),
        )),
        TradingVenue::BybitMargin | TradingVenue::BybitFutures => {
            Some(Box::new(crate::spread_pbs::bybit::BybitAdapter::new(venue)))
        }
        TradingVenue::GateMargin | TradingVenue::GateFutures => {
            Some(Box::new(crate::spread_pbs::gate::GateAdapter::new(venue)))
        }
        TradingVenue::BitgetMargin | TradingVenue::BitgetFutures => Some(Box::new(
            crate::spread_pbs::bitget::BitgetAdapter::new(venue),
        )),
        _ => None,
    }
}
