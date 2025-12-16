//! Manual mock for `fr_signal`.
//!
//! - Publishes manual signals to the same channel as `fr_signal` (`funding_rate_signal` by default).
//! - Subscribes `data_pubs/{open,hedge}/ask_bid_spread` to build bid/ask and compute prices.
//! - Subscribes `signal_query` (backward channel) and replies with `ArbHedge` pricing, to assist
//!   pre_trade's MM hedge-query flow.
//! - Serves a tiny web UI for symbol list + manual signal emission.

use anyhow::{Context, Result};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::Json;
use clap::Parser;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use mkt_signal::common::exchange::Exchange;
use mkt_signal::common::iceoryx_publisher::SignalPublisher;
use mkt_signal::common::iceoryx_subscriber::GenericSignalSubscriber;
use mkt_signal::common::ipc_service_name::build_service_name;
use mkt_signal::common::mkt_msg::{get_msg_type, AskBidSpreadMsg, MktMsgType};
use mkt_signal::common::redis_client::{RedisClient, RedisSettings};
use mkt_signal::common::time_util::get_timestamp_us;
use mkt_signal::funding_rate::common::Quote;
use mkt_signal::funding_rate::symbol_list::SymbolList;
use mkt_signal::pre_trade::order_manager::{OrderType, Side};
use mkt_signal::signal::cancel_signal::ArbCancelCtx;
use mkt_signal::signal::common::{SignalBytes, TradingLeg, TradingVenue};
use mkt_signal::signal::hedge_signal::{ArbHedgeCtx, ArbHedgeSignalQueryMsg};
use mkt_signal::signal::open_signal::ArbOpenCtx;
use mkt_signal::signal::trade_signal::{SignalType, TradeSignal};
use mkt_signal::symbol_match::normalize_symbol_for_whitelist;

const PROCESS_NAME: &str = "fr_manual_signal";
const DEFAULT_SIGNAL_CHANNEL: &str = "funding_rate_signal";
const DEFAULT_BACKWARD_CHANNEL: &str = "signal_query";
const ASKBID_PAYLOAD: usize = 64;

fn exchange_from_venue(venue: TradingVenue) -> Exchange {
    match venue {
        TradingVenue::BinanceMargin | TradingVenue::BinanceFutures => Exchange::Binance,
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => Exchange::Okex,
        TradingVenue::BybitMargin | TradingVenue::BybitFutures => Exchange::Bybit,
        TradingVenue::BitgetMargin | TradingVenue::BitgetFutures => Exchange::Bitget,
        TradingVenue::GateMargin | TradingVenue::GateFutures => Exchange::Gate,
    }
}

fn infer_default_venues(exchange: Exchange) -> (TradingVenue, TradingVenue) {
    match exchange {
        Exchange::Binance => (TradingVenue::BinanceMargin, TradingVenue::BinanceFutures),
        Exchange::Okex => (TradingVenue::OkexMargin, TradingVenue::OkexFutures),
        Exchange::Bybit => (TradingVenue::BybitMargin, TradingVenue::BybitFutures),
        Exchange::Bitget => (TradingVenue::BitgetMargin, TradingVenue::BitgetFutures),
        Exchange::Gate => (TradingVenue::GateMargin, TradingVenue::GateFutures),
    }
}

fn infer_venues_from_cwd() -> Option<(TradingVenue, TradingVenue)> {
    let cwd = std::env::current_dir().ok()?;
    let name = cwd.file_name()?.to_string_lossy().to_ascii_lowercase();
    let mut candidates = vec![name.as_str()];
    let prefix = name.split('_').next().unwrap_or("");
    if prefix != name {
        candidates.push(prefix);
    }

    for cand in candidates {
        for ex in [
            Exchange::Binance,
            Exchange::Okex,
            Exchange::Bybit,
            Exchange::Bitget,
            Exchange::Gate,
        ] {
            if cand.starts_with(ex.as_str()) {
                return Some(infer_default_venues(ex));
            }
        }
    }
    None
}

#[derive(Parser, Debug, Clone)]
#[command(name = "fr_manual_signal")]
#[command(about = "FR signal manual mock (web UI + hedge query responder)")]
struct CliArgs {
    /// Opening venue (active leg). If omitted, inferred from CWD (e.g. okex_fr_trade).
    #[arg(long, value_enum)]
    open: Option<TradingVenue>,

    /// Hedging venue (passive leg). If omitted, inferred from CWD (e.g. okex_fr_trade).
    #[arg(long, value_enum)]
    hedge: Option<TradingVenue>,

    /// Signal publish channel (same as fr_signal)
    #[arg(long, default_value = DEFAULT_SIGNAL_CHANNEL)]
    channel: String,

    /// Backward query channel (same as pre_trade)
    #[arg(long, default_value = DEFAULT_BACKWARD_CHANNEL)]
    backward_channel: String,

    /// HTTP bind host
    #[arg(long, default_value = "0.0.0.0")]
    bind: String,

    /// HTTP port
    #[arg(long, default_value_t = 8911)]
    port: u16,

    /// SymbolList reload interval seconds
    #[arg(long, default_value_t = 60)]
    symbol_reload_secs: u64,

    /// Default open order TTL (milliseconds) for manual signals
    #[arg(long, default_value_t = 120_000)]
    open_ttl_ms: u64,

    /// Default MM hedge timeout (milliseconds) carried in open/close context
    #[arg(long, default_value_t = 30_000)]
    mm_hedge_timeout_ms: u64,

    /// Hedge pricing offset used when replying to pre_trade hedge queries (maker)
    #[arg(long, default_value_t = 0.0003)]
    hedge_price_offset: f64,

    /// Hedge request seq >= threshold is considered aggressive (offset=0)
    #[arg(long, default_value_t = 6)]
    hedge_aggressive_seq_threshold: u32,
}

#[derive(Debug, Clone)]
struct Args {
    exchange: Exchange,
    open: TradingVenue,
    hedge: TradingVenue,
    channel: String,
    backward_channel: String,
    bind: String,
    port: u16,
    symbol_reload_secs: u64,
    open_ttl_ms: u64,
    mm_hedge_timeout_ms: u64,
    hedge_price_offset: f64,
    hedge_aggressive_seq_threshold: u32,
}

fn get_redis_settings() -> RedisSettings {
    let redis_host =
        std::env::var("FUNDING_RATE_REDIS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    RedisSettings {
        host: redis_host,
        port: 6379,
        db: 0,
        username: None,
        password: None,
        prefix: None,
    }
}

#[derive(Debug, Default)]
struct QuoteCache {
    by_venue: HashMap<TradingVenue, HashMap<String, Quote>>,
    stats: HashMap<TradingVenue, VenueStats>,
}

#[derive(Debug, Default, Clone)]
struct VenueStats {
    msg_count: u64,
    last_recv_ts_us: i64,
    last_symbol: String,
}

impl QuoteCache {
    fn upsert(&mut self, venue: TradingVenue, symbol: &str, bid: f64, ask: f64, ts: i64) {
        let key = normalize_symbol_for_whitelist(symbol, venue);
        let per_venue = self.by_venue.entry(venue).or_default();
        let entry = per_venue.entry(key.clone()).or_insert(Quote {
            bid: 0.0,
            ask: 0.0,
            ts: 0,
        });
        entry.update(bid, ask, ts);

        let stat = self.stats.entry(venue).or_default();
        stat.msg_count = stat.msg_count.saturating_add(1);
        stat.last_recv_ts_us = get_timestamp_us();
        stat.last_symbol = key;
    }

    fn get_valid(&self, venue: TradingVenue, symbol: &str) -> Option<Quote> {
        let key = normalize_symbol_for_whitelist(symbol, venue);
        self.by_venue
            .get(&venue)
            .and_then(|m| m.get(&key))
            .copied()
            .filter(|q| q.is_valid() && q.bid < q.ask)
    }
}

#[derive(Clone)]
struct AppCfg {
    exchange: Exchange,
    open: TradingVenue,
    hedge: TradingVenue,
    signal_channel: String,
    backward_channel: String,
    default_open_ttl_ms: u64,
    default_mm_hedge_timeout_ms: u64,
    hedge_price_offset: f64,
    hedge_aggressive_seq_threshold: u32,
}

#[derive(Clone)]
struct AppState {
    cfg: AppCfg,
    quotes: Arc<RwLock<QuoteCache>>,
    publish_tx: mpsc::UnboundedSender<PublishCmd>,
}

enum PublishCmd {
    PublishRaw {
        bytes: bytes::Bytes,
    },
    ManualSend {
        req: ManualSendRequest,
        reply: oneshot::Sender<Result<ManualSendResponse>>,
    },
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
enum ManualSignalKind {
    ForwardOpen,
    ForwardClose,
    BackwardOpen,
    BackwardClose,
    Cancel,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
enum HedgeMode {
    Mt,
    Mm,
}

#[derive(Debug, Deserialize, Clone)]
struct ManualSendRequest {
    /// Base symbol to use for both legs unless overridden.
    symbol: String,
    /// Optional override per leg (for edge cases like venue-specific symbol formats).
    opening_symbol: Option<String>,
    hedging_symbol: Option<String>,
    kind: ManualSignalKind,
    /// Limit price offset (e.g. 0.0002). For Cancel it is ignored.
    offset: Option<f64>,
    /// Qty for open/close (required). For Cancel it is ignored.
    qty: Option<f64>,
    /// Open order TTL override (ms)
    open_ttl_ms: Option<u64>,
    /// Hedge mode carried to pre_trade (MM triggers hedge query flow)
    hedge_mode: Option<HedgeMode>,
    /// MM hedge timeout override (ms) carried in ArbOpenCtx.hedge_timeout_us
    mm_hedge_timeout_ms: Option<u64>,
}

#[derive(Debug, Serialize, Clone)]
struct ManualSendResponse {
    ok: bool,
    published_at_us: i64,
    signal_type: String,
    opening_symbol: String,
    hedging_symbol: String,
    open: TradingVenue,
    hedge: TradingVenue,
    price: Option<f64>,
    qty: Option<f64>,
    offset: Option<f64>,
    hedge_timeout_us: Option<i64>,
}

#[derive(Debug, Serialize)]
struct SymbolsResponse {
    dump: Vec<String>,
    fwd_trade: Vec<String>,
    bwd_trade: Vec<String>,
    online: Vec<String>,
}

#[derive(Debug, Serialize)]
struct QuoteLegResponse {
    venue: TradingVenue,
    bid: f64,
    ask: f64,
    ts: i64,
}

#[derive(Debug, Serialize)]
struct QuoteResponse {
    symbol: String,
    open: Option<QuoteLegResponse>,
    hedge: Option<QuoteLegResponse>,
}

#[derive(Debug, Deserialize)]
struct QuoteQuery {
    symbol: String,
}

#[derive(Debug, Serialize)]
struct ConfigResponse {
    exchange: Exchange,
    open: TradingVenue,
    hedge: TradingVenue,
    signal_channel: String,
    backward_channel: String,
}

#[derive(Debug, Serialize)]
struct VenueStatsResponse {
    venue: TradingVenue,
    symbols_cached: usize,
    msg_count: u64,
    last_recv_ts_us: i64,
    last_symbol: String,
}

#[derive(Debug, Serialize)]
struct StatsResponse {
    open: VenueStatsResponse,
    hedge: VenueStatsResponse,
}

fn compute_open_price(open_quote: Quote, side: Side, offset: f64) -> f64 {
    let base_price = match side {
        Side::Buy => open_quote.bid,
        Side::Sell => open_quote.ask,
    };
    if base_price <= 0.0 {
        return 0.0;
    }
    match side {
        Side::Buy => base_price * (1.0 - offset),
        Side::Sell => base_price * (1.0 + offset),
    }
}

fn compute_hedge_limit_price(hedge_quote: Quote, side: Side, offset: f64) -> f64 {
    let base_price = match side {
        Side::Buy => hedge_quote.bid,
        Side::Sell => hedge_quote.ask,
    };
    if base_price <= 0.0 {
        return 0.0;
    }
    match side {
        Side::Buy => base_price * (1.0 - offset),
        Side::Sell => base_price * (1.0 + offset),
    }
}

fn side_for_kind(kind: &ManualSignalKind) -> Option<Side> {
    match kind {
        ManualSignalKind::ForwardOpen => Some(Side::Buy),
        ManualSignalKind::ForwardClose => Some(Side::Sell),
        ManualSignalKind::BackwardOpen => Some(Side::Sell),
        ManualSignalKind::BackwardClose => Some(Side::Buy),
        ManualSignalKind::Cancel => None,
    }
}

fn signal_type_for_kind(kind: &ManualSignalKind) -> SignalType {
    match kind {
        ManualSignalKind::ForwardOpen | ManualSignalKind::BackwardOpen => SignalType::ArbOpen,
        ManualSignalKind::ForwardClose | ManualSignalKind::BackwardClose => SignalType::ArbClose,
        ManualSignalKind::Cancel => SignalType::ArbCancel,
    }
}

async fn symbol_list_reload_loop(exchange: Exchange, interval_secs: u64, token: CancellationToken) {
    let redis = get_redis_settings();
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            _ = interval.tick() => {
                match RedisClient::connect(redis.clone()).await {
                    Ok(mut client) => {
                        let symbol_list = SymbolList::instance();
                        if let Err(err) = symbol_list.reload_from_redis(&mut client, exchange).await {
                            warn!("SymbolList reload failed: {err:#}");
                        }
                    }
                    Err(err) => warn!("SymbolList connect redis failed: {err:#}"),
                }
            }
        }
    }
}

fn spawn_askbid_listener(quotes: Arc<RwLock<QuoteCache>>, venue: TradingVenue) {
    let slug = venue.data_pub_slug();
    let node_name = format!("frmanual_{}_askbid", slug.replace('-', "_"));
    let service_name = format!("data_pubs/{}/ask_bid_spread", slug);

    tokio::task::spawn_local(async move {
        let result: Result<()> = async move {
            let node = NodeBuilder::new()
                .name(&NodeName::new(&node_name)?)
                .create::<ipc::Service>()?;

            let service = node
                .service_builder(&ServiceName::new(&service_name)?)
                .publish_subscribe::<[u8; ASKBID_PAYLOAD]>()
                .open_or_create()?;

            let subscriber: Subscriber<ipc::Service, [u8; ASKBID_PAYLOAD], ()> =
                service.subscriber_builder().create()?;

            info!(
                "frmanual subscribed ask_bid_spread: {} ({:?})",
                service_name, venue
            );

            loop {
                match subscriber.receive() {
                    Ok(Some(sample)) => {
                        let payload = sample.payload();
                        if payload.iter().all(|&b| b == 0) {
                            continue;
                        }
                        if get_msg_type(payload) != MktMsgType::AskBidSpread {
                            continue;
                        }
                        let symbol = AskBidSpreadMsg::get_symbol(payload).to_uppercase();
                        let bid = AskBidSpreadMsg::get_bid_price(payload);
                        let ask = AskBidSpreadMsg::get_ask_price(payload);
                        let ts = AskBidSpreadMsg::get_timestamp(payload);
                        quotes.write().upsert(venue, &symbol, bid, ask, ts);
                    }
                    Ok(None) => tokio::task::yield_now().await,
                    Err(err) => {
                        warn!("frmanual ask_bid receive error (venue={:?}): {err}", venue);
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        }
        .await;

        if let Err(err) = result {
            warn!("frmanual ask_bid listener exited: {err:#}");
        }
    });
}

fn spawn_backward_query_responder(
    cfg: AppCfg,
    quotes: Arc<RwLock<QuoteCache>>,
    publish_tx: mpsc::UnboundedSender<PublishCmd>,
    token: CancellationToken,
) {
    tokio::task::spawn_local(async move {
        let node_name = format!("{}_backward_sub_{}", PROCESS_NAME, cfg.exchange.as_str());
        let service_path = build_service_name(&format!("signal_pubs/{}", cfg.backward_channel));

        let result: Result<()> = async move {
            let node = NodeBuilder::new()
                .name(&NodeName::new(&node_name)?)
                .create::<ipc::Service>()?;

            let service = node
                .service_builder(&ServiceName::new(&service_path)?)
                .publish_subscribe::<[u8; mkt_signal::common::iceoryx_publisher::SIGNAL_PAYLOAD]>()
                .max_publishers(1)
                .max_subscribers(32)
                .history_size(128)
                .subscriber_max_buffer_size(256)
                .open_or_create()?;

            let subscriber: Subscriber<
                ipc::Service,
                [u8; mkt_signal::common::iceoryx_publisher::SIGNAL_PAYLOAD],
                (),
            > = service.subscriber_builder().create()?;

            let sub = GenericSignalSubscriber::Size4K(subscriber);

            info!(
                "frmanual subscribed backward channel: {} (service={})",
                cfg.backward_channel, service_path
            );

            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {
                        match sub.receive_msg() {
                            Ok(Some(data)) => {
                                let query = match ArbHedgeSignalQueryMsg::from_bytes(data) {
                                    Ok(q) => q,
                                    Err(err) => {
                                        warn!("frmanual: decode hedge query failed: {err}");
                                        continue;
                                    }
                                };

                                let Some(open_venue) = TradingVenue::from_u8(query.opening_venue) else {
                                    warn!("frmanual: hedge query invalid opening_venue={}", query.opening_venue);
                                    continue;
                                };
                                let Some(hedge_venue) = TradingVenue::from_u8(query.hedging_venue) else {
                                    warn!("frmanual: hedge query invalid hedging_venue={}", query.hedging_venue);
                                    continue;
                                };
                                if open_venue != cfg.open || hedge_venue != cfg.hedge {
                                    warn!(
                                        "frmanual: hedge query venue mismatch, configured_open={:?} configured_hedge={:?} but got open={:?} hedge={:?}, ignore",
                                        cfg.open, cfg.hedge, open_venue, hedge_venue
                                    );
                                    continue;
                                }

                                let Some(side) = query.get_side() else {
                                    warn!("frmanual: hedge query invalid side={}", query.hedge_side);
                                    continue;
                                };

                                let qty = query.hedge_qty;
                                if qty <= 0.0 {
                                    warn!("frmanual: hedge query qty<=0 strategy_id={} qty={}", query.strategy_id, qty);
                                    continue;
                                }

                                let opening_symbol = query.get_opening_symbol();
                                let hedging_symbol = query.get_hedging_symbol();
                                if opening_symbol.is_empty() || hedging_symbol.is_empty() {
                                    warn!("frmanual: hedge query missing symbols strategy_id={}", query.strategy_id);
                                    continue;
                                }

                                let open_quote = quotes.read().get_valid(open_venue, &opening_symbol);
                                let hedge_quote = quotes.read().get_valid(hedge_venue, &hedging_symbol);
                                let (Some(open_quote), Some(hedge_quote)) = (open_quote, hedge_quote) else {
                                    warn!(
                                        "frmanual: hedge query quote unavailable strategy_id={} open={} ({:?}) hedge={} ({:?})",
                                        query.strategy_id,
                                        opening_symbol, open_quote.is_some(),
                                        hedging_symbol, hedge_quote.is_some(),
                                    );
                                    continue;
                                };

                                let now = get_timestamp_us();
                                let aggressive = query.request_seq >= cfg.hedge_aggressive_seq_threshold;
                                let offset = if aggressive { 0.0 } else { cfg.hedge_price_offset.abs() };
                                let limit_price = compute_hedge_limit_price(hedge_quote, side, offset);
                                if limit_price <= 0.0 {
                                    warn!("frmanual: hedge query invalid limit_price strategy_id={}", query.strategy_id);
                                    continue;
                                }

                                let mut ctx = ArbHedgeCtx::new_maker(
                                    query.strategy_id,
                                    query.client_order_id,
                                    qty,
                                    side.to_u8(),
                                    limit_price,
                                    0.0,
                                    false,
                                    now + (cfg.default_mm_hedge_timeout_ms as i64) * 1000,
                                );
                                ctx.opening_leg = TradingLeg::new(open_venue, open_quote.bid, open_quote.ask);
                                ctx.set_opening_symbol(&opening_symbol);
                                ctx.hedging_leg = TradingLeg::new(hedge_venue, hedge_quote.bid, hedge_quote.ask);
                                ctx.set_hedging_symbol(&hedging_symbol);
                                ctx.market_ts = now;
                                ctx.price_offset = offset;

                                let signal = TradeSignal::create(SignalType::ArbHedge, now, 0.0, ctx.to_bytes());
                                let bytes = signal.to_bytes();
                                let _ = publish_tx.send(PublishCmd::PublishRaw { bytes });
                            }
                            Ok(None) => {}
                            Err(err) => {
                                warn!("frmanual: backward receive error: {err:#}");
                                tokio::time::sleep(Duration::from_millis(200)).await;
                            }
                        }
                    }
                }
            }

            Ok(())
        }
        .await;

        if let Err(err) = result {
            warn!("frmanual backward responder exited: {err:#}");
        }
    });
}

fn spawn_publisher_worker(
    cfg: AppCfg,
    quotes: Arc<RwLock<QuoteCache>>,
    mut rx: mpsc::UnboundedReceiver<PublishCmd>,
    token: CancellationToken,
) {
    tokio::task::spawn_local(async move {
        let publisher = SignalPublisher::new(&cfg.signal_channel)
            .with_context(|| format!("create SignalPublisher failed on '{}'", cfg.signal_channel))
            .expect("failed to create SignalPublisher");

        info!(
            "frmanual signal publisher ready: channel={} (open={:?} hedge={:?})",
            cfg.signal_channel, cfg.open, cfg.hedge
        );

        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                cmd = rx.recv() => {
                    let Some(cmd) = cmd else { break; };
                    match cmd {
                        PublishCmd::PublishRaw { bytes } => {
                            if let Err(err) = publisher.publish(&bytes) {
                                warn!("frmanual publish failed: {err:#}");
                            }
                        }
                        PublishCmd::ManualSend { req, reply } => {
                            let res = build_and_publish_manual(&cfg, &quotes, &publisher, req);
                            let _ = reply.send(res);
                        }
                    }
                }
            }
        }
    });
}

fn build_and_publish_manual(
    cfg: &AppCfg,
    quotes: &Arc<RwLock<QuoteCache>>,
    publisher: &SignalPublisher,
    req: ManualSendRequest,
) -> Result<ManualSendResponse> {
    let symbol = req.symbol.trim().to_uppercase();
    if symbol.is_empty() {
        anyhow::bail!("symbol is empty");
    }

    let opening_symbol = req
        .opening_symbol
        .as_deref()
        .unwrap_or(&symbol)
        .trim()
        .to_uppercase();
    let hedging_symbol = req
        .hedging_symbol
        .as_deref()
        .unwrap_or(&symbol)
        .trim()
        .to_uppercase();

    let now = get_timestamp_us();
    let signal_type = signal_type_for_kind(&req.kind);
    let signal_type_str = format!("{:?}", signal_type);

    match &signal_type {
        SignalType::ArbCancel => {
            let open_quote = quotes
                .read()
                .get_valid(cfg.open, &opening_symbol)
                .unwrap_or(Quote {
                    bid: 0.0,
                    ask: 0.0,
                    ts: 0,
                });
            let hedge_quote = quotes
                .read()
                .get_valid(cfg.hedge, &hedging_symbol)
                .unwrap_or(Quote {
                    bid: 0.0,
                    ask: 0.0,
                    ts: 0,
                });

            let mut ctx = ArbCancelCtx::new();
            ctx.opening_leg = TradingLeg::new(cfg.open, open_quote.bid, open_quote.ask);
            ctx.set_opening_symbol(&opening_symbol);
            ctx.hedging_leg = TradingLeg::new(cfg.hedge, hedge_quote.bid, hedge_quote.ask);
            ctx.set_hedging_symbol(&hedging_symbol);
            ctx.trigger_ts = now;

            let signal = TradeSignal::create(signal_type.clone(), now, 0.0, ctx.to_bytes());
            publisher.publish(&signal.to_bytes())?;

            return Ok(ManualSendResponse {
                ok: true,
                published_at_us: now,
                signal_type: signal_type_str,
                opening_symbol,
                hedging_symbol,
                open: cfg.open,
                hedge: cfg.hedge,
                price: None,
                qty: None,
                offset: None,
                hedge_timeout_us: None,
            });
        }
        SignalType::ArbOpen | SignalType::ArbClose => {}
        _ => anyhow::bail!("unsupported manual signal type: {:?}", signal_type),
    }

    let Some(side) = side_for_kind(&req.kind) else {
        anyhow::bail!("missing side mapping for kind");
    };

    let qty = req.qty.context("qty is required for open/close")?;
    if qty <= 0.0 {
        anyhow::bail!("qty must be > 0");
    }

    let offset = req.offset.unwrap_or(0.0).abs();
    let open_quote = quotes
        .read()
        .get_valid(cfg.open, &opening_symbol)
        .with_context(|| {
            format!(
                "open quote unavailable: venue={:?} symbol={}",
                cfg.open, opening_symbol
            )
        })?;
    let hedge_quote = quotes
        .read()
        .get_valid(cfg.hedge, &hedging_symbol)
        .with_context(|| {
            format!(
                "hedge quote unavailable: venue={:?} symbol={}",
                cfg.hedge, hedging_symbol
            )
        })?;

    let hedge_timeout_us = match req.hedge_mode.unwrap_or(HedgeMode::Mm) {
        HedgeMode::Mt => 0,
        HedgeMode::Mm => {
            let ms = req
                .mm_hedge_timeout_ms
                .unwrap_or(cfg.default_mm_hedge_timeout_ms);
            (ms as i64) * 1000
        }
    };
    let open_ttl_ms = req.open_ttl_ms.unwrap_or(cfg.default_open_ttl_ms);

    let price = compute_open_price(open_quote, side, offset);
    if price <= 0.0 {
        anyhow::bail!("computed price <= 0 (bid/ask invalid?)");
    }

    let mut ctx = ArbOpenCtx::new();
    ctx.opening_leg = TradingLeg::new(cfg.open, open_quote.bid, open_quote.ask);
    ctx.set_opening_symbol(&opening_symbol);
    ctx.hedging_leg = TradingLeg::new(cfg.hedge, hedge_quote.bid, hedge_quote.ask);
    ctx.set_hedging_symbol(&hedging_symbol);
    ctx.amount = qty as f32;
    ctx.set_side(side);
    ctx.set_order_type(OrderType::Limit);
    ctx.price = price;
    ctx.price_tick = 0.0;
    ctx.exp_time = now + (open_ttl_ms as i64) * 1000;
    ctx.create_ts = now;
    ctx.price_offset = offset;
    ctx.hedge_timeout_us = hedge_timeout_us;
    ctx.funding_ma = 0.0;
    ctx.predicted_funding_rate = 0.0;
    ctx.loan_rate = 0.0;

    let signal = TradeSignal::create(signal_type.clone(), now, 0.0, ctx.to_bytes());
    publisher.publish(&signal.to_bytes())?;

    Ok(ManualSendResponse {
        ok: true,
        published_at_us: now,
        signal_type: signal_type_str,
        opening_symbol,
        hedging_symbol,
        open: cfg.open,
        hedge: cfg.hedge,
        price: Some(price),
        qty: Some(qty),
        offset: Some(offset),
        hedge_timeout_us: Some(hedge_timeout_us),
    })
}

async fn api_symbols() -> impl IntoResponse {
    let symbol_list = SymbolList::instance();
    Json(SymbolsResponse {
        dump: symbol_list.get_dump_symbols(),
        fwd_trade: symbol_list.get_fwd_trade_symbols(),
        bwd_trade: symbol_list.get_bwd_trade_symbols(),
        online: symbol_list.get_online_symbols(),
    })
}

async fn api_config(State(st): State<AppState>) -> impl IntoResponse {
    Json(ConfigResponse {
        exchange: st.cfg.exchange,
        open: st.cfg.open,
        hedge: st.cfg.hedge,
        signal_channel: st.cfg.signal_channel.clone(),
        backward_channel: st.cfg.backward_channel.clone(),
    })
}

async fn api_stats(State(st): State<AppState>) -> impl IntoResponse {
    let cache = st.quotes.read();

    let open_symbols_cached = cache
        .by_venue
        .get(&st.cfg.open)
        .map(|m| m.len())
        .unwrap_or(0);
    let hedge_symbols_cached = cache
        .by_venue
        .get(&st.cfg.hedge)
        .map(|m| m.len())
        .unwrap_or(0);

    let open_stat = cache.stats.get(&st.cfg.open).cloned().unwrap_or_default();
    let hedge_stat = cache.stats.get(&st.cfg.hedge).cloned().unwrap_or_default();

    Json(StatsResponse {
        open: VenueStatsResponse {
            venue: st.cfg.open,
            symbols_cached: open_symbols_cached,
            msg_count: open_stat.msg_count,
            last_recv_ts_us: open_stat.last_recv_ts_us,
            last_symbol: open_stat.last_symbol,
        },
        hedge: VenueStatsResponse {
            venue: st.cfg.hedge,
            symbols_cached: hedge_symbols_cached,
            msg_count: hedge_stat.msg_count,
            last_recv_ts_us: hedge_stat.last_recv_ts_us,
            last_symbol: hedge_stat.last_symbol,
        },
    })
}

async fn api_quote(State(st): State<AppState>, Query(q): Query<QuoteQuery>) -> impl IntoResponse {
    let sym = q.symbol.trim().to_uppercase();
    let open = st
        .quotes
        .read()
        .get_valid(st.cfg.open, &sym)
        .map(|x| QuoteLegResponse {
            venue: st.cfg.open,
            bid: x.bid,
            ask: x.ask,
            ts: x.ts,
        });
    let hedge = st
        .quotes
        .read()
        .get_valid(st.cfg.hedge, &sym)
        .map(|x| QuoteLegResponse {
            venue: st.cfg.hedge,
            bid: x.bid,
            ask: x.ask,
            ts: x.ts,
        });

    Json(QuoteResponse {
        symbol: sym,
        open,
        hedge,
    })
}

async fn api_send(
    State(st): State<AppState>,
    Json(req): Json<ManualSendRequest>,
) -> impl IntoResponse {
    let (tx, rx) = oneshot::channel();
    if st
        .publish_tx
        .send(PublishCmd::ManualSend { req, reply: tx })
        .is_err()
    {
        return (StatusCode::SERVICE_UNAVAILABLE, "publisher unavailable").into_response();
    }

    match tokio::time::timeout(Duration::from_secs(2), rx).await {
        Ok(Ok(Ok(resp))) => Json(resp).into_response(),
        Ok(Ok(Err(err))) => (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
        Ok(Err(_closed)) => (StatusCode::SERVICE_UNAVAILABLE, "publisher closed").into_response(),
        Err(_timeout) => (StatusCode::GATEWAY_TIMEOUT, "publish timeout").into_response(),
    }
}

async fn index() -> impl IntoResponse {
    Html(INDEX_HTML)
}

const INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>fr_manual_signal</title>
    <style>
      body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; margin: 16px; }
      .row { display: flex; gap: 16px; }
      .col { flex: 1; min-width: 320px; }
      .col.symbols { flex: 0 0 360px; min-width: 280px; }
      .box { border: 1px solid #ddd; border-radius: 10px; padding: 12px; }
      input, select, button { padding: 8px; font-size: 14px; }
      input, select { width: 100%; box-sizing: border-box; }
      button { cursor: pointer; }
      .muted { color: #666; font-size: 12px; }
      .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
      ul { margin: 0; padding-left: 18px; max-height: 520px; overflow: auto; }
      li { cursor: pointer; padding: 2px 0; }
      li:hover { text-decoration: underline; }
      .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
      .ok { color: #0b7; }
      .err { color: #c33; white-space: pre-wrap; }
      textarea { width: 100%; box-sizing: border-box; padding: 10px; font-size: 12px; line-height: 1.35; }
    </style>
  </head>
  <body>
    <h2>fr_manual_signal</h2>
    <div class="muted" id="cfg"></div>
    <div class="row">
      <div class="col symbols box">
        <div class="grid">
          <div>
            <label class="muted">Search</label>
            <input id="q" placeholder="e.g. BTCUSDT" />
          </div>
          <div>
            <label class="muted">Selected</label>
            <input id="symbol" placeholder="click from list" />
          </div>
        </div>
        <div style="margin-top:10px;">
          <div class="muted">Online symbols (from SymbolList)</div>
          <ul id="list"></ul>
        </div>
      </div>
      <div class="col box">
        <div class="grid">
          <div>
            <label class="muted">Kind</label>
            <select id="kind">
              <option value="forward_open">forward_open</option>
              <option value="forward_close">forward_close</option>
              <option value="backward_open">backward_open</option>
              <option value="backward_close">backward_close</option>
              <option value="cancel">cancel</option>
            </select>
          </div>
          <div>
            <label class="muted">Hedge mode</label>
            <select id="hedgeMode">
              <option value="mm">mm</option>
              <option value="mt">mt</option>
            </select>
          </div>
          <div>
            <label class="muted">Offset</label>
            <input id="offset" value="0.0002" />
          </div>
          <div>
            <label class="muted">Qty</label>
            <input id="qty" value="0.01" />
          </div>
        </div>
        <div style="margin-top:10px;">
          <button id="send">Send</button>
          <span class="muted">Uses current open bid/ask to compute limit price.</span>
        </div>
        <div style="margin-top:12px;" class="mono" id="quote"></div>
        <div style="margin-top:12px;">
          <label class="muted">Result (pretty JSON)</label>
          <textarea id="resultBox" rows="10" class="mono" readonly></textarea>
        </div>
      </div>
    </div>
    <script>
      const $ = (id) => document.getElementById(id);
      const state = { symbols: [], selected: "" };
      const basePath = window.location.pathname.replace(/\/$/, "");
      const api = (path) => `${basePath}/api/${path}`;

      async function loadCfg() {
        const r = await fetch(api("config"));
        const j = await r.json();
        $("cfg").textContent = `exchange=${j.exchange} open=${j.open} hedge=${j.hedge} channel=${j.signal_channel} backward=${j.backward_channel}`;
      }

      async function refreshStats() {
        const r = await fetch(api("stats"));
        const j = await r.json();
        const o = `open symbols=${j.open.symbols_cached} msgs=${j.open.msg_count} last=${j.open.last_symbol} @${j.open.last_recv_ts_us}`;
        const h = `hedge symbols=${j.hedge.symbols_cached} msgs=${j.hedge.msg_count} last=${j.hedge.last_symbol} @${j.hedge.last_recv_ts_us}`;
        $("cfg").textContent = $("cfg").textContent.split(" | ")[0] + ` | ${o} | ${h}`;
      }

      function renderList() {
        const q = $("q").value.trim().toUpperCase();
        const ul = $("list");
        ul.innerHTML = "";
        const items = state.symbols.filter(s => !q || s.includes(q)).slice(0, 500);
        for (const s of items) {
          const li = document.createElement("li");
          li.textContent = s;
          li.onclick = () => { $("symbol").value = s; state.selected = s; refreshQuote(); };
          ul.appendChild(li);
        }
      }

      async function loadSymbols() {
        const r = await fetch(api("symbols"));
        const j = await r.json();
        state.symbols = (j.online || []).sort();
        renderList();
      }

      async function refreshQuote() {
        const sym = $("symbol").value.trim().toUpperCase();
        if (!sym) return;
        const r = await fetch(`${api("quote")}?symbol=${encodeURIComponent(sym)}`);
        const j = await r.json();
        const o = j.open ? `open(${j.open.venue}) bid=${j.open.bid} ask=${j.open.ask} ts=${j.open.ts}` : "open: N/A";
        const h = j.hedge ? `hedge(${j.hedge.venue}) bid=${j.hedge.bid} ask=${j.hedge.ask} ts=${j.hedge.ts}` : "hedge: N/A";
        $("quote").textContent = `${o}\n${h}`;
      }

      async function send() {
        const symbol = $("symbol").value.trim().toUpperCase();
        const kind = $("kind").value;
        const hedgeMode = $("hedgeMode").value;
        const offset = parseFloat($("offset").value);
        const qty = parseFloat($("qty").value);
        $("resultBox").value = "";
        if (!symbol) { $("resultBox").value = "symbol is empty"; return; }
        const body = { symbol, kind, hedge_mode: hedgeMode, offset, qty };
        const r = await fetch(api("send"), { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
        const text = await r.text();
        if (!r.ok) {
          $("resultBox").value = text;
          return;
        }
        try {
          const obj = JSON.parse(text);
          $("resultBox").value = JSON.stringify(obj, null, 2);
        } catch {
          $("resultBox").value = text;
        }
        refreshQuote();
      }

      $("q").addEventListener("input", renderList);
      $("send").addEventListener("click", send);
      setInterval(refreshQuote, 1000);
      setInterval(refreshStats, 2000);
      loadCfg();
      loadSymbols();
      refreshStats();
    </script>
  </body>
</html>
"#;

async fn serve_http(addr: SocketAddr, state: AppState, token: CancellationToken) -> Result<()> {
    let app = axum::Router::new()
        .route("/", get(index))
        .route(
            "/healthz",
            get(|| async { Json(serde_json::json!({"ok": true, "ts": get_timestamp_us()/1000})) }),
        )
        .route("/api/config", get(api_config))
        .route("/api/stats", get(api_stats))
        .route("/api/symbols", get(api_symbols))
        .route("/api/quote", get(api_quote))
        .route("/api/send", post(api_send))
        .with_state(state);

    info!("frmanual http listening at http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { token.cancelled().await })
        .await?;
    Ok(())
}

async fn run(args: Args, token: CancellationToken) -> Result<()> {
    if std::env::var("IPC_NAMESPACE").is_err() {
        anyhow::bail!("IPC_NAMESPACE is not set (required for signal_pubs namespace isolation)");
    }

    info!(
        "{} starting exchange={} open={:?} hedge={:?} http={}:{}",
        PROCESS_NAME, args.exchange, args.open, args.hedge, args.bind, args.port
    );

    SymbolList::init_singleton()?;
    let redis = get_redis_settings();
    {
        if let Ok(mut client) = RedisClient::connect(redis).await {
            let _ = SymbolList::instance()
                .reload_from_redis(&mut client, args.exchange)
                .await;
        }
    }

    let quotes = Arc::new(RwLock::new(QuoteCache::default()));
    spawn_askbid_listener(quotes.clone(), args.open);
    spawn_askbid_listener(quotes.clone(), args.hedge);

    let cfg = AppCfg {
        exchange: args.exchange,
        open: args.open,
        hedge: args.hedge,
        signal_channel: args.channel.clone(),
        backward_channel: args.backward_channel.clone(),
        default_open_ttl_ms: args.open_ttl_ms,
        default_mm_hedge_timeout_ms: args.mm_hedge_timeout_ms,
        hedge_price_offset: args.hedge_price_offset,
        hedge_aggressive_seq_threshold: args.hedge_aggressive_seq_threshold,
    };

    let (publish_tx, publish_rx) = mpsc::unbounded_channel();
    spawn_publisher_worker(cfg.clone(), quotes.clone(), publish_rx, token.clone());
    spawn_backward_query_responder(
        cfg.clone(),
        quotes.clone(),
        publish_tx.clone(),
        token.clone(),
    );

    tokio::task::spawn_local(symbol_list_reload_loop(
        args.exchange,
        args.symbol_reload_secs,
        token.clone(),
    ));

    let state = AppState {
        cfg: cfg.clone(),
        quotes,
        publish_tx,
    };

    let addr: SocketAddr = format!("{}:{}", args.bind, args.port).parse()?;
    serve_http(addr, state, token).await?;

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let args = CliArgs::parse();
    let (open, hedge) = match (args.open, args.hedge) {
        (Some(open), Some(hedge)) => (open, hedge),
        (None, None) => {
            infer_venues_from_cwd().unwrap_or_else(|| {
                panic!(
                    "missing --open/--hedge and failed to infer from CWD; name the directory like '<exchange>_fr_trade' (e.g. okex_fr_trade) or pass --open/--hedge explicitly"
                )
            })
        }
        _ => {
            anyhow::bail!("please provide both --open and --hedge, or omit both to infer from CWD")
        }
    };

    let exchange = exchange_from_venue(open);
    if exchange_from_venue(hedge) != exchange {
        anyhow::bail!(
            "open/hedge exchange mismatch: open={:?} hedge={:?}",
            open,
            hedge
        );
    }
    let args = Args {
        exchange,
        open,
        hedge,
        channel: args.channel,
        backward_channel: args.backward_channel,
        bind: args.bind,
        port: args.port,
        symbol_reload_secs: args.symbol_reload_secs,
        open_ttl_ms: args.open_ttl_ms,
        mm_hedge_timeout_ms: args.mm_hedge_timeout_ms,
        hedge_price_offset: args.hedge_price_offset,
        hedge_aggressive_seq_threshold: args.hedge_aggressive_seq_threshold,
    };
    let token = CancellationToken::new();

    let token_clone = token.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        token_clone.cancel();
    });

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            run(args, token).await?;
            Ok::<(), anyhow::Error>(())
        })
        .await?;

    Ok(())
}
