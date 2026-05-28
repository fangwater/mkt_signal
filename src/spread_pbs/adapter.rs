use anyhow::Result;
use serde_json::Value;
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
/// 避免双路竞速场景下同一帧被重复 `from_str`。
/// 双路去重统一基于 `BboFrame.seq_id` 在 `process_frame` 内完成。
pub trait VenueAdapter {
    fn name(&self) -> &'static str;
    fn ws_url(&self) -> String;
    fn ws_headers(&self) -> Vec<(String, String)> {
        Vec::new()
    }
    fn build_subscribe(&self, symbols: &[String]) -> Vec<Value>;
    fn parse_frame(&self, value: &Value) -> Result<Vec<BboFrame>>;
    fn parse_binary_frame(&self, _raw: &[u8]) -> Result<Vec<BboFrame>> {
        Ok(Vec::new())
    }
    /// 返回 None 表示完全依赖服务端 ws-Ping/Pong；返回 Some 表示主动按 interval 发心跳。
    fn keepalive(&self) -> Option<KeepaliveSpec>;
}

/// 按 venue 创建对应 adapter；非支持的 venue 返回 Ok(None)。
///
/// 当前支持：OKex / Binance / Bybit / Gate / Bitget（spot+futures 各 2 个）。
/// Hyperliquid / Aster 为 DEX，spread_pbs 不接入。
///
/// OKex 必须 await：SBE 端订阅要先 REST 拉 instIdCode 映射；其他 venue 同步构造。
pub async fn create_adapter(
    venue: crate::signal::common::TradingVenue,
) -> Result<Option<Box<dyn VenueAdapter>>> {
    use crate::signal::common::TradingVenue;
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
        TradingVenue::GateMargin => Box::new(crate::spread_pbs::gate::GateAdapter::new(venue)),
        TradingVenue::GateFutures => Box::new(crate::spread_pbs::gate_sbe::GateSbeAdapter::new()),
        TradingVenue::BitgetMargin | TradingVenue::BitgetFutures => {
            Box::new(crate::spread_pbs::bitget::BitgetAdapter::new(venue))
        }
        _ => return Ok(None),
    };
    Ok(Some(adapter))
}
