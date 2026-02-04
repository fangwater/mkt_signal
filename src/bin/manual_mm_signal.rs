//! Manual mock for market maker signals.
//!
//! - Publishes manual MMOpen signals to the normal signal channel.
//! - Subscribes ask_bid_spread to build quotes.
//! - Subscribes backward channel (signal_query) and replies with MMHedge.

use anyhow::{Context, Result};
use axum::extract::State;
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
use serde_json;
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use mkt_signal::common::iceoryx_publisher::SignalPublisher;
use mkt_signal::common::iceoryx_subscriber::GenericSignalSubscriber;
use mkt_signal::common::ipc_service_name::build_service_name;
use mkt_signal::common::mkt_msg::{get_msg_type, AskBidSpreadMsg, MktMsgType};
use mkt_signal::common::redis_client::{RedisClient, RedisSettings};
use mkt_signal::common::time_util::get_timestamp_us;
use mkt_signal::funding_rate::common::Quote;
use mkt_signal::pre_trade::order_manager::{OrderType, Side};
use mkt_signal::signal::common::{SignalBytes, TradingLeg, TradingVenue};
use mkt_signal::signal::hedge_signal::{MmHedgeCtx, MmHedgeSignalQueryMsg};
use mkt_signal::signal::open_signal::MmOpenCtx;
use mkt_signal::signal::trade_signal::{SignalType, TradeSignal};
use mkt_signal::signal::venue_min_qty_table::VenueMinQtyTable;

const PROCESS_NAME: &str = "manual_mm_signal";
const DEFAULT_SIGNAL_CHANNEL: &str = "funding_rate_signal";
const DEFAULT_BACKWARD_CHANNEL: &str = "signal_query";
const DEFAULT_BIND: &str = "0.0.0.0";
const DEFAULT_PORT: u16 = 6366;
const DEFAULT_OPEN_TTL_MS: u64 = 120_000;
const DEFAULT_NEXT_QUERY_DELAY_MS: u64 = 30_000;
const ASKBID_PAYLOAD: usize = 64;
const DEFAULT_SYMBOL_RELOAD_SECS: u64 = 60;
const MM_SYMBOL_NAMESPACE: &str = "mm";

#[derive(Parser, Debug, Clone)]
#[command(name = "manual_mm_signal")]
#[command(about = "Manual MM signal (web UI + mm hedge query responder)")]
struct CliArgs {
    /// Config file path
    #[arg(long, default_value = "manual_mm_signal.yaml")]
    config: String,
}

#[derive(Debug, Deserialize, Clone)]
struct ManualMmConfig {
    venue: String,
    port: Option<u16>,
    bind: Option<String>,
    signal_channel: Option<String>,
    backward_channel: Option<String>,
    open_ttl_ms: Option<u64>,
    order_amount_u: f64,
    price_offsets: Vec<f64>,
    next_query_delay_ms: Option<u64>,
    from_key: Option<String>,
}

#[derive(Debug, Clone)]
struct AppCfg {
    venue: TradingVenue,
    bind: String,
    port: u16,
    signal_channel: String,
    backward_channel: String,
    default_open_ttl_ms: u64,
    order_amount_u: f64,
    price_offsets: Vec<f64>,
    next_query_delay_ms: u64,
    from_key: Vec<u8>,
    symbol_key_suffix: String,
}

#[derive(Debug, Default, Clone)]
struct QuoteStats {
    msg_count: u64,
    last_recv_ts_us: i64,
    last_symbol: String,
}

#[derive(Debug, Default)]
struct QuoteCache {
    quotes: HashMap<String, Quote>,
    stats: QuoteStats,
}

impl QuoteCache {
    fn upsert(&mut self, symbol: &str, bid: f64, ask: f64, ts: i64) {
        let key = symbol.to_uppercase();
        let entry = self.quotes.entry(key.clone()).or_insert(Quote {
            bid: 0.0,
            ask: 0.0,
            ts: 0,
        });
        entry.update(bid, ask, ts);

        self.stats.msg_count = self.stats.msg_count.saturating_add(1);
        self.stats.last_recv_ts_us = get_timestamp_us();
        self.stats.last_symbol = key;
    }

    fn get_valid(&self, symbol: &str) -> Option<Quote> {
        let key = symbol.to_uppercase();
        self.quotes.get(&key).copied().filter(|q| q.is_valid())
    }
}

#[derive(Clone)]
struct AppState {
    cfg: AppCfg,
    quotes: Arc<RwLock<QuoteCache>>,
    symbols: Arc<RwLock<SymbolCache>>,
    publish_tx: mpsc::UnboundedSender<PublishCmd>,
}

enum PublishCmd {
    PublishRaw {
        bytes: bytes::Bytes,
    },
    ManualOpen {
        req: ManualOpenRequest,
        reply: oneshot::Sender<Result<ManualOpenResponse>>,
    },
}

#[derive(Debug, Deserialize, Clone)]
struct ManualOpenRequest {
    symbol: String,
    side: String,
    qty: f64,
    price: f64,
    order_type: Option<String>,
    exp_ms: Option<u64>,
    price_tick: Option<f64>,
    from_key: Option<String>,
}

#[derive(Debug, Serialize)]
struct ManualOpenResponse {
    ok: bool,
    published_at_us: i64,
    symbol: String,
    side: String,
    order_type: String,
    qty: f64,
    price: f64,
    exp_ms: u64,
}

#[derive(Debug, Serialize)]
struct ConfigResponse {
    venue: TradingVenue,
    bind: String,
    port: u16,
    signal_channel: String,
    backward_channel: String,
    default_open_ttl_ms: u64,
    order_amount_u: f64,
    price_offsets: Vec<f64>,
    next_query_delay_ms: u64,
    from_key: String,
    symbol_key_suffix: String,
}

#[derive(Debug, Default)]
struct SymbolCache {
    symbols: Vec<String>,
    last_reload_ts_us: i64,
}

#[derive(Debug, Serialize)]
struct SymbolsResponse {
    symbols: Vec<String>,
    last_reload_ts_us: i64,
}

#[derive(Debug, Serialize)]
struct QuoteResponse {
    symbol: String,
    venue: TradingVenue,
    bid: f64,
    ask: f64,
    ts: i64,
}

#[derive(Debug, Deserialize)]
struct QuoteQuery {
    symbol: String,
}

fn venue_from_slug(raw: &str) -> Option<TradingVenue> {
    let slug = raw.trim().to_ascii_lowercase().replace('_', "-");
    match slug.as_str() {
        "binance-margin" => Some(TradingVenue::BinanceMargin),
        "binance-futures" => Some(TradingVenue::BinanceFutures),
        "okex-margin" => Some(TradingVenue::OkexMargin),
        "okex-futures" => Some(TradingVenue::OkexFutures),
        "bybit-margin" => Some(TradingVenue::BybitMargin),
        "bybit-futures" => Some(TradingVenue::BybitFutures),
        "bitget-margin" => Some(TradingVenue::BitgetMargin),
        "bitget-futures" => Some(TradingVenue::BitgetFutures),
        "gate-margin" => Some(TradingVenue::GateMargin),
        "gate-futures" => Some(TradingVenue::GateFutures),
        _ => None,
    }
}

fn load_config(path: &str) -> Result<AppCfg> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("read config failed: {}", path))?;
    let cfg: ManualMmConfig = serde_yaml::from_str(&raw)
        .with_context(|| format!("parse yaml failed: {}", path))?;

    let venue = venue_from_slug(&cfg.venue)
        .with_context(|| format!("invalid venue: {}", cfg.venue))?;
    let port = cfg.port.unwrap_or(DEFAULT_PORT);
    let bind = cfg.bind.unwrap_or_else(|| DEFAULT_BIND.to_string());
    let signal_channel = cfg
        .signal_channel
        .unwrap_or_else(|| DEFAULT_SIGNAL_CHANNEL.to_string());
    let backward_channel = cfg
        .backward_channel
        .unwrap_or_else(|| DEFAULT_BACKWARD_CHANNEL.to_string());
    let default_open_ttl_ms = cfg.open_ttl_ms.unwrap_or(DEFAULT_OPEN_TTL_MS);
    let next_query_delay_ms = cfg
        .next_query_delay_ms
        .unwrap_or(DEFAULT_NEXT_QUERY_DELAY_MS);
    let from_key = cfg
        .from_key
        .unwrap_or_else(|| PROCESS_NAME.to_string())
        .into_bytes();

    if !(cfg.order_amount_u > 0.0) {
        anyhow::bail!("order_amount_u must be > 0");
    }
    if cfg.price_offsets.is_empty() {
        anyhow::bail!("price_offsets is empty");
    }

    Ok(AppCfg {
        venue,
        bind,
        port,
        signal_channel,
        backward_channel,
        default_open_ttl_ms,
        order_amount_u: cfg.order_amount_u,
        price_offsets: cfg.price_offsets,
        next_query_delay_ms,
        from_key,
        symbol_key_suffix: venue.data_pub_slug().to_string(),
    })
}

fn get_redis_settings() -> RedisSettings {
    RedisSettings {
        host: "127.0.0.1".to_string(),
        port: 6379,
        db: 0,
        username: None,
        password: None,
        prefix: None,
    }
}

async fn symbol_list_reload_loop(
    interval_secs: u64,
    key_suffix: String,
    symbols: Arc<RwLock<SymbolCache>>,
    token: CancellationToken,
) {
    let redis = get_redis_settings();
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
    let trade_key = format!("{MM_SYMBOL_NAMESPACE}_trade_symbols:{key_suffix}");
    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            _ = interval.tick() => {
                match RedisClient::connect(redis.clone()).await {
                    Ok(mut client) => {
                        let payload = client.get_string(&trade_key).await.ok().flatten();
                        let Some(value) = payload else {
                            panic!("missing redis key: {}", trade_key);
                        };
                        let list = serde_json::from_str::<Vec<String>>(&value)
                            .unwrap_or_else(|err| panic!("invalid mm symbol list {}: {}", trade_key, err));
                        let list: Vec<String> = list
                            .into_iter()
                            .map(|s| s.trim().to_uppercase())
                            .filter(|s| !s.is_empty())
                            .collect();
                        let mut cache = symbols.write();
                        cache.symbols = list;
                        cache.last_reload_ts_us = get_timestamp_us();
                    }
                    Err(err) => warn!("manual_mm: redis connect failed: {err:#}"),
                }
            }
        }
    }
}

fn min_qty_symbol_key(venue: TradingVenue, symbol: &str) -> String {
    match venue {
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => symbol
            .to_uppercase()
            .replace("-SWAP", "")
            .replace('-', ""),
        TradingVenue::GateMargin | TradingVenue::GateFutures => {
            symbol.to_uppercase().replace('_', "").replace('-', "")
        }
        _ => symbol.to_uppercase(),
    }
}

fn is_futures_venue(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BinanceFutures
            | TradingVenue::OkexFutures
            | TradingVenue::BybitFutures
            | TradingVenue::BitgetFutures
            | TradingVenue::GateFutures
    )
}

fn align_order_with_table(
    symbol_key: &str,
    raw_qty: f64,
    raw_price: f64,
    table: &VenueMinQtyTable,
    enforce_min_notional: bool,
) -> Result<(f64, f64), String> {
    if raw_qty <= 0.0 {
        return Err(format!(
            "symbol={} raw qty invalid raw_qty={}",
            symbol_key, raw_qty
        ));
    }
    if raw_price <= 0.0 {
        return Err(format!(
            "symbol={} raw price invalid raw_price={}",
            symbol_key, raw_price
        ));
    }

    let price_tick = table.price_tick(symbol_key).unwrap_or(0.0);
    let price = if price_tick > 0.0 {
        align_price_floor(raw_price, price_tick)
    } else {
        raw_price
    };
    if price <= 0.0 {
        return Err(format!("symbol={} aligned price invalid", symbol_key));
    }

    let step = table.step_size(symbol_key).unwrap_or(0.0);
    let mut qty = if step > 0.0 {
        align_price_floor(raw_qty, step)
    } else {
        raw_qty
    };

    if let Some(min_qty) = table.min_qty(symbol_key) {
        if min_qty > 0.0 && qty < min_qty {
            qty = min_qty;
        }
    }

    if enforce_min_notional {
        if let Some(min_notional) = table.min_notional(symbol_key) {
            if min_notional > 0.0 {
                let required_qty = min_notional / price;
                if qty < required_qty {
                    qty = if step > 0.0 {
                        align_price_ceil(required_qty, step)
                    } else {
                        required_qty
                    };
                }
            }
        }
    }

    if qty <= 0.0 {
        return Err(format!("symbol={} aligned qty invalid", symbol_key));
    }

    Ok((qty, price))
}

fn align_order_for_venue(
    venue: TradingVenue,
    symbol_key: &str,
    raw_qty_base: f64,
    raw_price: f64,
    table: &VenueMinQtyTable,
) -> Result<(f64, f64), String> {
    let enforce_min_notional = is_futures_venue(venue);
    let raw_qty = if venue == TradingVenue::OkexFutures {
        let contract_size = table.contract_multiplier_opt(symbol_key).ok_or_else(|| {
            format!(
                "symbol={} missing OKX contract multiplier, cannot convert base qty",
                symbol_key
            )
        })?;
        if contract_size <= 0.0 {
            return Err(format!(
                "symbol={} invalid OKX contract multiplier: {}",
                symbol_key, contract_size
            ));
        }
        raw_qty_base / contract_size
    } else {
        raw_qty_base
    };

    align_order_with_table(symbol_key, raw_qty, raw_price, table, enforce_min_notional)
}

fn to_fraction(value: f64) -> Option<(i64, i64)> {
    if value <= 0.0 {
        return None;
    }
    let mut denom: i64 = 1;
    let mut scaled = value;
    for _ in 0..9 {
        let rounded = scaled.round();
        if (scaled - rounded).abs() < 1e-9 {
            return Some((rounded as i64, denom));
        }
        scaled *= 10.0;
        denom = denom.saturating_mul(10);
    }
    None
}

fn align_price_floor(price: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return price;
    }
    if let Some((tick_num, tick_den)) = to_fraction(tick) {
        if tick_num == 0 {
            return price;
        }
        let tick_num = tick_num as i128;
        let tick_den = tick_den as i128;
        let units = ((price * tick_den as f64) + 1e-9).floor() as i128;
        let aligned_units = (units / tick_num) * tick_num;
        return aligned_units as f64 / tick_den as f64;
    }
    let scaled = ((price / tick) + 1e-9).floor();
    scaled * tick
}

fn align_price_ceil(price: f64, tick: f64) -> f64 {
    if tick <= 0.0 {
        return price;
    }
    if let Some((tick_num, tick_den)) = to_fraction(tick) {
        if tick_num == 0 {
            return price;
        }
        let tick_num = tick_num as i128;
        let tick_den = tick_den as i128;
        let units = ((price * tick_den as f64) - 1e-9).ceil() as i128;
        let aligned_units = ((units + tick_num - 1) / tick_num) * tick_num;
        return aligned_units as f64 / tick_den as f64;
    }
    let scaled = ((price / tick) - 1e-9).ceil();
    scaled * tick
}

fn tick_to_exp(tick: f64) -> Option<i32> {
    if tick <= 0.0 {
        return None;
    }
    let mut exp: i32 = 0;
    let mut val = tick;
    while val < 1.0 {
        val *= 10.0;
        exp -= 1;
        if exp < -12 {
            break;
        }
    }
    while val >= 10.0 {
        val /= 10.0;
        exp += 1;
        if exp > 12 {
            break;
        }
    }
    if (val - 1.0).abs() <= 1e-9 {
        Some(exp)
    } else {
        None
    }
}

fn build_mm_hedge_ctx(
    cfg: &AppCfg,
    quotes: &Arc<RwLock<QuoteCache>>,
    table: &Arc<RwLock<VenueMinQtyTable>>,
    query: &MmHedgeSignalQueryMsg,
) -> Result<MmHedgeCtx> {
    let symbol = query.get_symbol().to_uppercase();
    if symbol.is_empty() {
        anyhow::bail!("empty symbol");
    }

    let quote = quotes
        .read()
        .get_valid(&symbol)
        .context("quote unavailable")?;

    let net_qty = query.buy_qty - query.sell_qty;
    let side = if net_qty >= 0.0 { Side::Sell } else { Side::Buy };

    let base_price = match side {
        Side::Buy => quote.bid,
        Side::Sell => quote.ask,
    };
    if base_price <= 0.0 {
        anyhow::bail!("invalid base price");
    }

    let symbol_key = min_qty_symbol_key(cfg.venue, &symbol);
    let table_guard = table.read();
    let price_tick = table_guard.price_tick(&symbol_key).unwrap_or(0.0);
    let qty_tick = table_guard.step_size(&symbol_key).unwrap_or(0.0);
    if price_tick <= 0.0 || qty_tick <= 0.0 {
        anyhow::bail!(
            "missing tick for {} (price_tick={}, qty_tick={})",
            symbol_key,
            price_tick,
            qty_tick
        );
    }

    let price_tick_exp =
        tick_to_exp(price_tick).context("price_tick is not power-of-10")?;
    let qty_tick_exp = tick_to_exp(qty_tick).context("qty_tick is not power-of-10")?;

    let mut price_list: Vec<i32> = Vec::with_capacity(cfg.price_offsets.len());
    let mut amount_list: Vec<i64> = Vec::with_capacity(cfg.price_offsets.len());
    for offset in &cfg.price_offsets {
        let offset = offset.abs();
        let raw_price = match side {
            Side::Buy => base_price * (1.0 - offset),
            Side::Sell => base_price * (1.0 + offset),
        };
        if raw_price <= 0.0 {
            continue;
        }
        let raw_qty = cfg.order_amount_u / raw_price;
        let (aligned_qty, aligned_price) = align_order_for_venue(
            cfg.venue,
            &symbol_key,
            raw_qty,
            raw_price,
            &table_guard,
        )
        .map_err(anyhow::Error::msg)?;

        let price_level = (aligned_price / price_tick).round() as i32;
        let amount_level = (aligned_qty / qty_tick).round() as i64;
        if price_level == 0 || amount_level == 0 {
            continue;
        }
        price_list.push(price_level);
        amount_list.push(amount_level);
    }

    if price_list.is_empty() || amount_list.is_empty() {
        anyhow::bail!("empty price/amount list after alignment");
    }

    let now = get_timestamp_us();
    let mut ctx = MmHedgeCtx::new();
    ctx.opening_leg = TradingLeg::new(cfg.venue, quote.bid, quote.ask, quote.ts);
    ctx.set_opening_symbol(&symbol);
    ctx.price_tick_exp = price_tick_exp;
    ctx.qty_tick_exp = qty_tick_exp;
    ctx.price_list = price_list;
    ctx.amount_list = amount_list;
    ctx.signal_ts = now;
    ctx.next_query_ts = now + (cfg.next_query_delay_ms as i64) * 1000;
    ctx.set_from_key(cfg.from_key.clone());
    Ok(ctx)
}

fn spawn_ask_bid_listener(
    venue: TradingVenue,
    quotes: Arc<RwLock<QuoteCache>>,
    token: CancellationToken,
) {
    tokio::task::spawn_local(async move {
        let node_name = format!("{}_ask_bid_{}", PROCESS_NAME, venue.data_pub_slug());
        let service_path = build_service_name(&format!(
            "data_pubs/{}/ask_bid_spread",
            venue.data_pub_slug()
        ));

        let result: Result<()> = async move {
            let node = NodeBuilder::new()
                .name(&NodeName::new(&node_name)?)
                .create::<ipc::Service>()?;

            let service = node
                .service_builder(&ServiceName::new(&service_path)?)
                .publish_subscribe::<[u8; ASKBID_PAYLOAD]>()
                .max_publishers(1)
                .max_subscribers(10)
                .open_or_create()?;

            let subscriber: Subscriber<ipc::Service, [u8; ASKBID_PAYLOAD], ()> =
                service.subscriber_builder().create()?;

            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {
                        match subscriber.receive()? {
                            Some(sample) => {
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
                                quotes.write().upsert(&symbol, bid, ask, ts);
                            }
                            None => {}
                        }
                    }
                }
            }

            Ok(())
        }
        .await;

        if let Err(err) = result {
            warn!("manual_mm ask_bid listener exited: {err:#}");
        }
    });
}

fn spawn_backward_query_responder(
    cfg: AppCfg,
    quotes: Arc<RwLock<QuoteCache>>,
    min_qty_table: Arc<RwLock<VenueMinQtyTable>>,
    publish_tx: mpsc::UnboundedSender<PublishCmd>,
    token: CancellationToken,
) {
    tokio::task::spawn_local(async move {
        let node_name = format!("{}_backward_sub", PROCESS_NAME);
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
                "manual_mm subscribed backward channel: {} (service={})",
                cfg.backward_channel, service_path
            );

            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {
                        match sub.receive_msg() {
                            Ok(Some(data)) => {
                                let query = match MmHedgeSignalQueryMsg::from_bytes(data) {
                                    Ok(q) => q,
                                    Err(err) => {
                                        warn!("manual_mm: decode hedge query failed: {err}");
                                        continue;
                                    }
                                };

                                let ctx = match build_mm_hedge_ctx(&cfg, &quotes, &min_qty_table, &query) {
                                    Ok(ctx) => ctx,
                                    Err(err) => {
                                        warn!("manual_mm: build hedge ctx failed: {err:#}");
                                        continue;
                                    }
                                };

                                let symbol = ctx.get_opening_symbol();
                                info!(
                                    "manual_mm: hedge query reply symbol={} levels={} price_tick_exp={} qty_tick_exp={}",
                                    symbol,
                                    ctx.price_list.len(),
                                    ctx.price_tick_exp,
                                    ctx.qty_tick_exp
                                );

                                let signal = TradeSignal::create(
                                    SignalType::MMHedge,
                                    ctx.signal_ts,
                                    0.0,
                                    ctx.to_bytes(),
                                );
                                let bytes = signal.to_bytes();
                                let _ = publish_tx.send(PublishCmd::PublishRaw { bytes });
                            }
                            Ok(None) => {}
                            Err(err) => {
                                warn!("manual_mm: backward receive error: {err:#}");
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
            warn!("manual_mm backward responder exited: {err:#}");
        }
    });
}

fn spawn_publisher_worker(
    cfg: AppCfg,
    mut rx: mpsc::UnboundedReceiver<PublishCmd>,
    token: CancellationToken,
) {
    tokio::task::spawn_local(async move {
        let publisher = SignalPublisher::new(&cfg.signal_channel)
            .with_context(|| format!("create SignalPublisher failed on '{}'", cfg.signal_channel))
            .expect("failed to create SignalPublisher");

        info!(
            "manual_mm signal publisher ready: channel={} venue={:?}",
            cfg.signal_channel, cfg.venue
        );

        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                cmd = rx.recv() => {
                    let Some(cmd) = cmd else { break; };
                    match cmd {
                        PublishCmd::PublishRaw { bytes } => {
                            if let Err(err) = publisher.publish(&bytes) {
                                warn!("manual_mm publish failed: {err:#}");
                            }
                        }
                        PublishCmd::ManualOpen { req, reply } => {
                            let res = build_and_publish_open(&cfg, &publisher, req);
                            let _ = reply.send(res);
                        }
                    }
                }
            }
        }
    });
}

fn build_and_publish_open(
    cfg: &AppCfg,
    publisher: &SignalPublisher,
    req: ManualOpenRequest,
) -> Result<ManualOpenResponse> {
    let symbol = req.symbol.trim().to_uppercase();
    if symbol.is_empty() {
        anyhow::bail!("symbol is empty");
    }

    let side = Side::from_str(req.side.trim())
        .with_context(|| format!("invalid side: {}", req.side))?;

    let order_type_raw = req.order_type.unwrap_or_else(|| "LIMIT".to_string());
    let order_type_norm = order_type_raw.trim().to_ascii_uppercase();
    let order_type = OrderType::from_str(order_type_norm.as_str())
        .with_context(|| format!("invalid order_type: {}", order_type_raw))?;

    if req.qty <= 0.0 {
        anyhow::bail!("qty must be > 0");
    }
    if req.price <= 0.0 {
        anyhow::bail!("price must be > 0");
    }

    let exp_ms = req.exp_ms.unwrap_or(cfg.default_open_ttl_ms);
    let now = get_timestamp_us();
    let mut ctx = MmOpenCtx::new();
    ctx.opening_leg = TradingLeg::new(cfg.venue, req.price, req.price, now);
    ctx.set_opening_symbol(&symbol);
    ctx.amount = req.qty as f32;
    ctx.set_side(side);
    ctx.set_order_type(order_type);
    ctx.price = req.price;
    ctx.price_tick = req.price_tick.unwrap_or(0.0);
    ctx.exp_time = if exp_ms > 0 {
        now + (exp_ms as i64) * 1000
    } else {
        0
    };
    ctx.create_ts = now;
    ctx.price_offset = 0.0;
    if let Some(from_key) = req.from_key {
        ctx.set_from_key(from_key.into_bytes());
    } else {
        ctx.set_from_key(cfg.from_key.clone());
    }

    let signal = TradeSignal::create(SignalType::MMOpen, now, 0.0, ctx.to_bytes());
    publisher.publish(&signal.to_bytes())?;

    Ok(ManualOpenResponse {
        ok: true,
        published_at_us: now,
        symbol,
        side: side.as_str().to_string(),
        order_type: order_type_norm,
        qty: req.qty,
        price: req.price,
        exp_ms,
    })
}

async fn api_config(State(st): State<AppState>) -> impl IntoResponse {
    Json(ConfigResponse {
        venue: st.cfg.venue,
        bind: st.cfg.bind.clone(),
        port: st.cfg.port,
        signal_channel: st.cfg.signal_channel.clone(),
        backward_channel: st.cfg.backward_channel.clone(),
        default_open_ttl_ms: st.cfg.default_open_ttl_ms,
        order_amount_u: st.cfg.order_amount_u,
        price_offsets: st.cfg.price_offsets.clone(),
        next_query_delay_ms: st.cfg.next_query_delay_ms,
        from_key: String::from_utf8_lossy(&st.cfg.from_key).to_string(),
        symbol_key_suffix: st.cfg.symbol_key_suffix.clone(),
    })
}

async fn api_symbols(State(st): State<AppState>) -> impl IntoResponse {
    let cache = st.symbols.read();
    Json(SymbolsResponse {
        symbols: cache.symbols.clone(),
        last_reload_ts_us: cache.last_reload_ts_us,
    })
}

async fn api_quote(
    State(st): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<QuoteQuery>,
) -> impl IntoResponse {
    let sym = q.symbol.trim().to_uppercase();
    let quote = st.quotes.read().get_valid(&sym);
    if let Some(qt) = quote {
        Json(QuoteResponse {
            symbol: sym,
            venue: st.cfg.venue,
            bid: qt.bid,
            ask: qt.ask,
            ts: qt.ts,
        })
        .into_response()
    } else {
        (StatusCode::NOT_FOUND, "quote unavailable").into_response()
    }
}

async fn api_send(
    State(st): State<AppState>,
    Json(req): Json<ManualOpenRequest>,
) -> impl IntoResponse {
    let (tx, rx) = oneshot::channel();
    if st
        .publish_tx
        .send(PublishCmd::ManualOpen { req, reply: tx })
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

async fn serve_http(addr: SocketAddr, state: AppState, token: CancellationToken) -> Result<()> {
    let app = axum::Router::new()
        .route("/", get(index))
        .route("/api/config", get(api_config))
        .route("/api/symbols", get(api_symbols))
        .route("/api/quote", get(api_quote))
        .route("/api/send", post(api_send))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("manual_mm http listening on {}", addr);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            token.cancelled().await;
        })
        .await?;
    Ok(())
}

const INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>manual_mm_signal</title>
    <style>
      body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; margin: 16px; }
      .row { display: flex; gap: 16px; align-items: flex-start; }
      .col { flex: 1; min-width: 320px; }
      .box { border: 1px solid #ddd; border-radius: 10px; padding: 12px; }
      input, select, button { padding: 8px; font-size: 14px; }
      input, select { width: 100%; box-sizing: border-box; }
      button { cursor: pointer; }
      .muted { color: #666; font-size: 12px; }
      .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
      .ok { color: #0b7; }
      .err { color: #c33; white-space: pre-wrap; }
      textarea { width: 100%; box-sizing: border-box; padding: 10px; font-size: 12px; line-height: 1.35; }
    </style>
  </head>
  <body>
    <h2>manual_mm_signal</h2>
    <div class="muted" id="cfg"></div>
    <div class="row">
      <div class="col box">
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
          <div class="muted">Symbols</div>
          <ul id="list" style="max-height: 420px; overflow: auto; margin:0; padding-left: 18px;"></ul>
        </div>
      </div>
      <div class="col box">
        <div class="grid">
          <div>
            <label class="muted">Side</label>
            <select id="side">
              <option value="BUY">BUY</option>
              <option value="SELL">SELL</option>
            </select>
          </div>
          <div>
            <label class="muted">Qty</label>
            <input id="qty" type="number" step="0.0001" value="0.01" />
          </div>
          <div>
            <label class="muted">Price</label>
            <input id="price" type="number" step="0.01" value="1" />
          </div>
          <div>
            <label class="muted">Order Type</label>
            <select id="orderType">
              <option value="LIMIT">LIMIT</option>
              <option value="MARKET">MARKET</option>
            </select>
          </div>
          <div>
            <label class="muted">Exp ms (0 = no ttl)</label>
            <input id="expMs" type="number" step="1000" />
          </div>
          <div>
            <label class="muted">Price Tick (optional)</label>
            <input id="priceTick" type="number" step="0.0001" />
          </div>
          <div>
            <label class="muted">From Key (optional)</label>
            <input id="fromKey" />
          </div>
        </div>
        <div style="margin-top:10px;">
          <button id="send">Send MMOpen</button>
        </div>
        <div style="margin-top:12px;" class="mono" id="quote"></div>
        <div style="margin-top:10px;">
          <div id="resp" class="muted"></div>
        </div>
      </div>
      <div class="col box">
        <div class="muted">Last Response</div>
        <textarea id="log" rows="16" readonly></textarea>
      </div>
    </div>

    <script>
      const cfgEl = document.getElementById('cfg');
      const logEl = document.getElementById('log');
      const respEl = document.getElementById('resp');
      const state = { symbols: [], selected: "" };

      async function loadCfg() {
        const resp = await fetch('/api/config');
        const cfg = await resp.json();
        cfgEl.textContent = `mm_trade_symbols:${cfg.symbol_key_suffix} venue=${cfg.venue} channel=${cfg.signal_channel} backward=${cfg.backward_channel}`;
      }

      function renderList() {
        const q = document.getElementById("q").value.trim().toUpperCase();
        const ul = document.getElementById("list");
        ul.innerHTML = "";
        const items = state.symbols.filter(s => !q || s.includes(q)).slice(0, 500);
        for (const s of items) {
          const li = document.createElement("li");
          li.textContent = s;
          li.onclick = () => {
            document.getElementById("symbol").value = s;
            state.selected = s;
            refreshQuote();
          };
          ul.appendChild(li);
        }
      }

      async function loadSymbols() {
        const r = await fetch('/api/symbols');
        const j = await r.json();
        state.symbols = (j.symbols || []).sort();
        renderList();
      }

      async function refreshQuote() {
        const sym = document.getElementById("symbol").value.trim().toUpperCase();
        if (!sym) return;
        const r = await fetch(`/api/quote?symbol=${encodeURIComponent(sym)}`);
        if (!r.ok) {
          document.getElementById("quote").textContent = "quote: N/A";
          return;
        }
        const j = await r.json();
        document.getElementById("quote").textContent = `${j.venue} bid=${j.bid} ask=${j.ask} ts=${j.ts}`;
      }

      function log(obj, ok=true) {
        const txt = JSON.stringify(obj, null, 2);
        logEl.value = txt;
        respEl.className = ok ? 'ok' : 'err';
        respEl.textContent = ok ? 'OK' : 'ERROR';
      }

      document.getElementById('send').onclick = async () => {
        const payload = {
          symbol: document.getElementById('symbol').value.trim(),
          side: document.getElementById('side').value,
          qty: Number(document.getElementById('qty').value),
          price: Number(document.getElementById('price').value),
          order_type: document.getElementById('orderType').value,
        };
        const expMs = document.getElementById('expMs').value.trim();
        if (expMs) payload.exp_ms = Number(expMs);
        const priceTick = document.getElementById('priceTick').value.trim();
        if (priceTick) payload.price_tick = Number(priceTick);
        const fromKey = document.getElementById('fromKey').value.trim();
        if (fromKey) payload.from_key = fromKey;

        const resp = await fetch('/api/send', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });

        const text = await resp.text();
        if (!resp.ok) {
          log({ error: text }, false);
          return;
        }
        try {
          log(JSON.parse(text), true);
        } catch {
          log({ raw: text }, true);
        }
      };

      loadCfg();
      loadSymbols();
      document.getElementById("q").oninput = renderList;
    </script>
  </body>
</html>
"#;

async fn run(cfg: AppCfg, token: CancellationToken) -> Result<()> {
    let mut table = VenueMinQtyTable::new(cfg.venue);
    table.refresh().await?;
    let table = Arc::new(RwLock::new(table));

    let quotes = Arc::new(RwLock::new(QuoteCache::default()));
    let symbols = Arc::new(RwLock::new(SymbolCache::default()));
    spawn_ask_bid_listener(cfg.venue, quotes.clone(), token.clone());
    tokio::task::spawn_local(symbol_list_reload_loop(
        DEFAULT_SYMBOL_RELOAD_SECS,
        cfg.symbol_key_suffix.clone(),
        symbols.clone(),
        token.clone(),
    ));

    let (publish_tx, publish_rx) = mpsc::unbounded_channel();
    spawn_publisher_worker(cfg.clone(), publish_rx, token.clone());
    spawn_backward_query_responder(
        cfg.clone(),
        quotes.clone(),
        table.clone(),
        publish_tx.clone(),
        token.clone(),
    );

    let state = AppState {
        cfg: cfg.clone(),
        quotes,
        symbols,
        publish_tx,
    };

    let addr: SocketAddr = format!("{}:{}", cfg.bind, cfg.port).parse()?;
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
    let cfg = load_config(&args.config)?;
    let token = CancellationToken::new();
    let token_clone = token.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        token_clone.cancel();
    });

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            run(cfg, token).await?;
            Ok::<(), anyhow::Error>(())
        })
        .await?;

    Ok(())
}
