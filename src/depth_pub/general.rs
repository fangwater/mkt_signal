//! Combined depth publisher for **jp-meta-elvpn**: 8× `depth_pubs/<venue>/depth25`.
//!
//! - Binance margin: SBE `@depth20`
//! - Binance futures: JSON `@depth20@100ms`
//! - Bitget margin/futures: SBE `books50`
//! - Gate margin/futures: SBE `order_book` 20-level snapshot
//! - OKX: existing `DepthPubApp` incremental path (spread_pbs 400-level books)
//!
//! **不含 Bybit**：Bybit 行情与交易栈在 `sg`，不在本机；本进程只覆盖本机
//! 已有的 Binance/OKX/Bitget/Gate。Bybit depth 继续由 sg 上的 `depth_pub` 发布。

use anyhow::Result;
use log::{info, warn};
use mkt_parsers::binance as binance_codec;
use mkt_parsers::bitget as bitget_codec;
use mkt_parsers::gate as gate_codec;
use runtime_common::fast_hash::{fast_hash_map, FastHashMap};
use std::cell::RefCell;
use std::rc::Rc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio_tungstenite_v030::tungstenite::Message;

use super::app::DepthPubApp;
use super::depth_msg::DepthMsg;
use super::publisher::DepthMsgPublisher;
use crate::mkt_pub::cfg::Config;
use crate::spread_pbs::adapter::KeepaliveSpec;
use crate::spread_pbs::binance::binance_futures_mm_ws_enabled;
use crate::spread_pbs::ws::{run_public_ws, FrameHandler, WsLoopParams};
use order_common::TradingVenue;

const BINANCE_SPOT_SBE_WS_URL: &str = "wss://stream-sbe.binance.com:9443/ws";
const BINANCE_FUTURES_WS_URL: &str = "wss://fstream.binance.com/public/stream";
const BINANCE_FUTURES_MM_WS_URL: &str = "wss://fstream-mm.binance.com/public/stream";
const BITGET_SBE_WS_URL: &str = "wss://ws.bitget.com/v3/ws/public/sbe";
const GATE_SBE_FUTURES_WS_URL: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt/sbe?sbe_schema_id=1";
const GATE_SBE_SPOT_WS_URL: &str = "wss://api.gateio.ws/ws/v4/ws/spot/sbe?sbe_schema_id=1";
const BINANCE_SUBSCRIBE_CHUNK: usize = 200;
const BITGET_SUBSCRIBE_CHUNK: usize = 50;
const IDLE_SLEEP_MICROS: u64 = 100;
const STATS_INTERVAL: Duration = Duration::from_secs(10);
const PUSH_INTERVAL: Duration = Duration::from_millis(50);
const WS_IDLE_TIMEOUT: Duration = Duration::from_secs(15);

struct SnapshotBook {
    symbol: String,
    timestamp_us: i64,
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
}

struct SnapshotState {
    msg: DepthMsg,
    last_push: Instant,
    dirty: bool,
}

struct SnapshotFeedApp {
    venue_slug: String,
    publisher: DepthMsgPublisher,
    symbols: FastHashMap<String, SnapshotState>,
    received: u64,
    published: u64,
    dropped: u64,
    last_stats: Instant,
}

impl SnapshotFeedApp {
    fn new(venue: TradingVenue) -> Result<Self> {
        let venue_slug = venue.data_pub_slug().to_string();
        let publisher = DepthMsgPublisher::new(&venue_slug)?;
        Ok(Self {
            venue_slug,
            publisher,
            symbols: fast_hash_map(),
            received: 0,
            published: 0,
            dropped: 0,
            last_stats: Instant::now(),
        })
    }

    fn apply_book(&mut self, book: SnapshotBook) {
        if book.bids.is_empty() || book.asks.is_empty() {
            return;
        }
        let msg = DepthMsg::depth25(book.symbol.clone(), book.timestamp_us, book.bids, book.asks);
        self.received = self.received.saturating_add(1);
        let symbol = book.symbol;
        let should_push = {
            let state = self
                .symbols
                .entry(symbol.clone())
                .or_insert_with(|| SnapshotState {
                    msg: msg.clone(),
                    last_push: Instant::now() - PUSH_INTERVAL,
                    dirty: true,
                });
            state.msg = msg;
            state.dirty = true;
            state.last_push.elapsed() >= PUSH_INTERVAL
        };
        if should_push {
            self.publish_symbol(&symbol);
        }
    }

    fn publish_due(&mut self) {
        let due: Vec<String> = self
            .symbols
            .iter()
            .filter(|(_, state)| state.dirty || state.last_push.elapsed() >= PUSH_INTERVAL)
            .map(|(symbol, _)| symbol.clone())
            .collect();
        for symbol in due {
            self.publish_symbol(&symbol);
        }
        if self.last_stats.elapsed() >= STATS_INTERVAL {
            info!(
                "depth_pub_general[{}] snapshot received={} published={} dropped={} symbols={}",
                self.venue_slug,
                self.received,
                self.published,
                self.dropped,
                self.symbols.len()
            );
            self.last_stats = Instant::now();
        }
    }

    fn publish_symbol(&mut self, symbol: &str) {
        let Some(state) = self.symbols.get_mut(symbol) else {
            return;
        };
        if self.publisher.publish_depth25(&state.msg) {
            self.published = self.published.saturating_add(1);
            state.dirty = false;
            state.last_push = Instant::now();
        } else {
            self.dropped = self.dropped.saturating_add(1);
        }
    }
}

pub struct DepthPubGeneralRunner {
    okex: Vec<DepthPubApp>,
    snapshots: Vec<Rc<RefCell<SnapshotFeedApp>>>,
    _ws_keepalive: Vec<watch::Sender<bool>>,
}

impl DepthPubGeneralRunner {
    pub async fn new(cfg_path: &str) -> Result<Self> {
        let okex_venues = [TradingVenue::OkexMargin, TradingVenue::OkexFutures];
        let mut okex = Vec::new();
        for venue in okex_venues {
            okex.push(DepthPubApp::new(venue).await?);
        }

        let snapshot_venues = [
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
            TradingVenue::BitgetMargin,
            TradingVenue::BitgetFutures,
            TradingVenue::GateMargin,
            TradingVenue::GateFutures,
        ];
        let mut snapshots = Vec::new();
        let mut ws_keepalive = Vec::new();
        for venue in snapshot_venues {
            let config = Config::load_config(cfg_path, venue).await?;
            let symbols = config.get_symbols().await?;
            if symbols.is_empty() {
                warn!(
                    "depth_pub_general[{}] symbol list empty; WS will still connect",
                    venue.data_pub_slug()
                );
            }
            let app = Rc::new(RefCell::new(SnapshotFeedApp::new(venue)?));
            ws_keepalive.push(spawn_snapshot_ws(venue, &config, symbols, app.clone()));
            snapshots.push(app);
        }

        info!("depth_pub_general ready: okex incremental + binance/bitget/gate snapshots");
        Ok(Self {
            okex,
            snapshots,
            _ws_keepalive: ws_keepalive,
        })
    }

    pub fn poll_once(&mut self) -> Result<bool> {
        let mut busy = false;
        for app in &mut self.okex {
            busy |= app.poll_once()?;
        }
        for app in &self.snapshots {
            app.borrow_mut().publish_due();
        }
        Ok(busy)
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            let busy = self.poll_once()?;
            if !busy {
                tokio::task::yield_now().await;
                thread::sleep(Duration::from_micros(IDLE_SLEEP_MICROS));
            } else {
                tokio::task::yield_now().await;
            }
        }
    }
}

fn spawn_snapshot_ws(
    venue: TradingVenue,
    config: &Config,
    symbols: Vec<String>,
    app: Rc<RefCell<SnapshotFeedApp>>,
) -> watch::Sender<bool> {
    let local_ip = config.primary_local_ip.clone();
    let (url, headers, subscribe_msgs, keepalive, label, handler) = match venue {
        TradingVenue::BinanceMargin | TradingVenue::BinanceFutures => {
            spawn_binance_params(venue, &symbols, app)
        }
        TradingVenue::BitgetMargin | TradingVenue::BitgetFutures => {
            spawn_bitget_params(venue, &symbols, app)
        }
        TradingVenue::GateMargin | TradingVenue::GateFutures => {
            spawn_gate_params(venue, &symbols, app)
        }
        other => unreachable!("snapshot feed on {other:?}"),
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::task::spawn_local(run_public_ws(
        WsLoopParams {
            label,
            url,
            local_ip,
            remote_ip: None,
            headers,
            subscribe_msgs,
            keepalive,
            parse_okex_notices: false,
            business_idle_timeout: Some(WS_IDLE_TIMEOUT),
            rolling_restart: None,
        },
        handler,
        shutdown_rx,
    ));
    shutdown_tx
}

fn spawn_binance_params(
    venue: TradingVenue,
    symbols: &[String],
    app: Rc<RefCell<SnapshotFeedApp>>,
) -> (
    String,
    Vec<(String, String)>,
    Vec<serde_json::Value>,
    Option<KeepaliveSpec>,
    String,
    FrameHandler,
) {
    let (url, headers, channel) = match venue {
        TradingVenue::BinanceMargin => (
            BINANCE_SPOT_SBE_WS_URL.to_string(),
            binance_sbe_headers(),
            "depth20",
        ),
        TradingVenue::BinanceFutures => (
            binance_futures_ws_url().to_string(),
            Vec::new(),
            "depth20@100ms",
        ),
        other => unreachable!("binance snapshot feed on {other:?}"),
    };
    if venue == TradingVenue::BinanceMargin && headers.is_empty() {
        warn!(
            "depth_pub_general[binance-margin] missing BINANCE_SBE_API_KEY/BINANCE_API_KEY; SBE depth20 will be rejected"
        );
    }
    let subscribe_msgs = build_depth20_subscribe(symbols, channel);
    let label = format!("binance-depth20-{}", venue.data_pub_slug());
    let binary = venue == TradingVenue::BinanceMargin;
    let handler: FrameHandler = Rc::new(move |_recv_us, raw| {
        let book = if binary {
            binance_codec::parse_sbe_incremental(raw).filter(|book| book.is_snapshot)
        } else {
            parse_binance_json_snapshot(raw)
        };
        let Some(book) = book else {
            return;
        };
        app.borrow_mut().apply_book(binance_book_to_snapshot(book));
    });
    (url, headers, subscribe_msgs, None, label, handler)
}

fn spawn_bitget_params(
    venue: TradingVenue,
    symbols: &[String],
    app: Rc<RefCell<SnapshotFeedApp>>,
) -> (
    String,
    Vec<(String, String)>,
    Vec<serde_json::Value>,
    Option<KeepaliveSpec>,
    String,
    FrameHandler,
) {
    let inst_type = match venue {
        TradingVenue::BitgetMargin => "spot",
        TradingVenue::BitgetFutures => "usdt-futures",
        other => unreachable!("bitget snapshot feed on {other:?}"),
    };
    let subscribe_msgs = build_bitget_books50_subscribe(symbols, inst_type);
    let label = format!("bitget-books50-{}", venue.data_pub_slug());
    let handler: FrameHandler = Rc::new(move |_recv_us, raw| {
        let Ok(books) = bitget_codec::parse_sbe_books50(raw) else {
            return;
        };
        for book in books {
            app.borrow_mut().apply_book(bitget_book_to_snapshot(book));
        }
    });
    (
        BITGET_SBE_WS_URL.to_string(),
        Vec::new(),
        subscribe_msgs,
        Some(KeepaliveSpec::text(Duration::from_secs(25), "ping")),
        label,
        handler,
    )
}

fn spawn_gate_params(
    venue: TradingVenue,
    symbols: &[String],
    app: Rc<RefCell<SnapshotFeedApp>>,
) -> (
    String,
    Vec<(String, String)>,
    Vec<serde_json::Value>,
    Option<KeepaliveSpec>,
    String,
    FrameHandler,
) {
    let (url, channel, ping_channel, futures) = match venue {
        TradingVenue::GateFutures => (
            GATE_SBE_FUTURES_WS_URL.to_string(),
            "futures.order_book",
            "futures.ping",
            true,
        ),
        TradingVenue::GateMargin => (
            GATE_SBE_SPOT_WS_URL.to_string(),
            "spot.order_book",
            "spot.ping",
            false,
        ),
        other => unreachable!("gate snapshot feed on {other:?}"),
    };
    let subscribe_msgs = build_gate_order_book_subscribe(symbols, channel, futures);
    let label = format!("gate-order-book-{}", venue.data_pub_slug());
    let handler: FrameHandler = Rc::new(move |_recv_us, raw| {
        let books = if futures {
            gate_codec::parse_futures_sbe_order_book(raw)
        } else {
            gate_codec::parse_spot_sbe_order_book(raw)
        };
        for book in books {
            app.borrow_mut().apply_book(gate_book_to_snapshot(book));
        }
    });
    let ping_channel = ping_channel.to_string();
    (
        url,
        Vec::new(),
        subscribe_msgs,
        Some(KeepaliveSpec::dynamic(Duration::from_secs(15), move || {
            let body = serde_json::json!({
                "time": now_unix_secs(),
                "channel": ping_channel,
            });
            Message::Text(body.to_string().into())
        })),
        label,
        handler,
    )
}

fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn binance_book_to_snapshot(book: binance_codec::Book) -> SnapshotBook {
    SnapshotBook {
        symbol: book.symbol,
        timestamp_us: book.timestamp_us,
        bids: book.bids.iter().map(|l| (l.price, l.amount)).collect(),
        asks: book.asks.iter().map(|l| (l.price, l.amount)).collect(),
    }
}

fn bitget_book_to_snapshot(book: bitget_codec::Book) -> SnapshotBook {
    SnapshotBook {
        symbol: book.symbol,
        timestamp_us: book.timestamp_us,
        bids: book.bids.iter().map(|l| (l.price, l.amount)).collect(),
        asks: book.asks.iter().map(|l| (l.price, l.amount)).collect(),
    }
}

fn gate_book_to_snapshot(book: gate_codec::Book) -> SnapshotBook {
    SnapshotBook {
        symbol: book.symbol,
        timestamp_us: book.timestamp_us,
        bids: book.bids.iter().map(|l| (l.price, l.amount)).collect(),
        asks: book.asks.iter().map(|l| (l.price, l.amount)).collect(),
    }
}

fn binance_futures_ws_url() -> &'static str {
    if binance_futures_mm_ws_enabled() {
        BINANCE_FUTURES_MM_WS_URL
    } else {
        BINANCE_FUTURES_WS_URL
    }
}

fn binance_sbe_headers() -> Vec<(String, String)> {
    std::env::var("BINANCE_SBE_API_KEY")
        .or_else(|_| std::env::var("BINANCE_API_KEY"))
        .ok()
        .filter(|key| !key.trim().is_empty())
        .map(|key| vec![("X-MBX-APIKEY".to_string(), key)])
        .unwrap_or_default()
}

fn build_depth20_subscribe(symbols: &[String], channel: &str) -> Vec<serde_json::Value> {
    let streams: Vec<String> = symbols
        .iter()
        .map(|sym| format!("{}@{}", sym.to_ascii_lowercase(), channel))
        .collect();
    streams
        .chunks(BINANCE_SUBSCRIBE_CHUNK.max(1))
        .enumerate()
        .map(|(i, chunk)| {
            serde_json::json!({
                "method": "SUBSCRIBE",
                "params": chunk,
                "id": (i as u64) + 1,
            })
        })
        .collect()
}

fn parse_binance_json_snapshot(raw: &[u8]) -> Option<binance_codec::Book> {
    let value: serde_json::Value = serde_json::from_slice(raw).ok()?;
    binance_codec::parse_incremental_json(&value)
}

fn build_bitget_books50_subscribe(symbols: &[String], inst_type: &str) -> Vec<serde_json::Value> {
    symbols
        .chunks(BITGET_SUBSCRIBE_CHUNK.max(1))
        .map(|chunk| {
            let args: Vec<serde_json::Value> = chunk
                .iter()
                .map(|sym| {
                    serde_json::json!({
                        "instType": inst_type,
                        "topic": "books50",
                        "symbol": sym,
                    })
                })
                .collect();
            serde_json::json!({
                "op": "subscribe",
                "args": args,
            })
        })
        .collect()
}

fn build_gate_order_book_subscribe(
    symbols: &[String],
    channel: &str,
    futures: bool,
) -> Vec<serde_json::Value> {
    let (level, interval) = if futures {
        ("20", "0")
    } else {
        ("20", "100ms")
    };
    symbols
        .iter()
        .map(|symbol| {
            serde_json::json!({
                "time": now_unix_secs(),
                "channel": channel,
                "event": "subscribe",
                "payload": [symbol, level, interval],
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_futures_partial_depth_as_snapshot() {
        let raw = br#"{"stream":"btcusdt@depth20@100ms","data":{"e":"depthUpdate","E":1700000000001,"T":1700000000000,"s":"BTCUSDT","U":1,"u":2,"b":[["100.0","1"],["99.0","2"]],"a":[["101.0","3"],["102.0","4"]]}}"#;
        let book = parse_binance_json_snapshot(raw).expect("json depth20");
        assert_eq!(book.symbol, "BTCUSDT");
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.asks.len(), 2);
        let msg = DepthMsg::depth25(
            book.symbol,
            book.timestamp_us,
            book.bids.iter().map(|l| (l.price, l.amount)).collect(),
            book.asks.iter().map(|l| (l.price, l.amount)).collect(),
        );
        assert_eq!(msg.bids.len(), 2);
        assert_eq!(msg.asks.len(), 2);
        assert_eq!(msg.byte_size(), 4 + 4 + 7 + 8 + 25 * 16 * 2);
    }

    #[test]
    fn subscribe_chunks_depth20_streams() {
        let symbols: Vec<String> = (0..201).map(|i| format!("S{i}USDT")).collect();
        let msgs = build_depth20_subscribe(&symbols, "depth20@100ms");
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0]["params"].as_array().unwrap().len(), 200);
        assert_eq!(msgs[1]["params"].as_array().unwrap().len(), 1);
        assert!(msgs[0]["params"][0]
            .as_str()
            .unwrap()
            .ends_with("@depth20@100ms"));
    }

    #[test]
    fn subscribe_chunks_bitget_books50() {
        let symbols: Vec<String> = (0..51).map(|i| format!("S{i}USDT")).collect();
        let msgs = build_bitget_books50_subscribe(&symbols, "usdt-futures");
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0]["args"].as_array().unwrap().len(), 50);
        assert_eq!(msgs[1]["args"].as_array().unwrap().len(), 1);
        assert_eq!(msgs[0]["args"][0]["topic"], "books50");
        assert_eq!(msgs[0]["args"][0]["instType"], "usdt-futures");
    }

    #[test]
    fn subscribe_gate_order_book_one_symbol_per_message() {
        let symbols = vec!["BTC_USDT".to_string(), "ETH_USDT".to_string()];
        let fut = build_gate_order_book_subscribe(&symbols, "futures.order_book", true);
        assert_eq!(fut.len(), 2);
        assert_eq!(
            fut[0]["payload"],
            serde_json::json!(["BTC_USDT", "20", "0"])
        );
        let spot = build_gate_order_book_subscribe(&symbols, "spot.order_book", false);
        assert_eq!(
            spot[1]["payload"],
            serde_json::json!(["ETH_USDT", "20", "100ms"])
        );
    }
}
