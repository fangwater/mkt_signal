//! Manual mock for market maker signals.
//!
//! - Publishes manual MMOpen signals to the normal signal channel.
//! - Subscribes ask_bid_spread to build quotes.
//! - Subscribes backward channel (trade_query) and replies with MMHedge.

use anyhow::{anyhow, Context, Result};
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
use mkt_signal::common::mkt_msg::{
    get_msg_type, AskBidSpreadMsg, MktMsgType, ModelMsg, MODEL_STATUS_OK,
};
use mkt_signal::common::redis_client::{RedisClient, RedisSettings};
use mkt_signal::common::time_util::get_timestamp_us;
use mkt_signal::depth_pub::query_client::DepthQueryClient;
use mkt_signal::factor_pub::fusion_factor_pub::app::ExtraFactorId;
use mkt_signal::factor_pub::fusion_factor_pub::fusion_factor_index_to_name;
use mkt_signal::factor_pub::model_pub::publisher::MODEL_PAYLOAD_MAX_BYTES;
use mkt_signal::funding_rate::common::build_decision_from_key_base;
use mkt_signal::funding_rate::common::Quote;
use mkt_signal::funding_rate::factor_value_hub::FactorValueHub;
use mkt_signal::funding_rate::mm_decision::from_key::append_mm_open_tlens_to_from_key;
use mkt_signal::market_maker::hedge_quote_plan::{
    build_mm_hedge_ctx as build_mm_hedge_ctx_core, resolve_mm_hedge_signal_inputs,
    MmHedgeBuildInput,
};
use mkt_signal::market_maker::open_quote_plan::{build_mm_open_quote_plan, MmOpenPlanLevel};
use mkt_signal::market_maker::order_align::ensure_supported_mm_open_venue as ensure_supported_mm_open_venue_raw;
use mkt_signal::pre_trade::order_manager::{OrderType, Side};
use mkt_signal::signal::common::{SignalBytes, TradingLeg, TradingVenue};
use mkt_signal::signal::hedge_signal::{MmHedgeCtx, MmHedgeSignalQueryMsg};
use mkt_signal::signal::open_signal::MmOpenCtx;
use mkt_signal::signal::trade_signal::{SignalType, TradeSignal};
use mkt_signal::signal::venue_min_qty_table::VenueMinQtyTable;

const PROCESS_NAME: &str = "manual_mm_signal";
const DEFAULT_SIGNAL_CHANNEL: &str = "trade_signal";
const DEFAULT_BACKWARD_CHANNEL: &str = "trade_query";
const DEFAULT_BIND: &str = "0.0.0.0";
const DEFAULT_PORT: u16 = 6366;
const ASKBID_PAYLOAD: usize = 128;
const DEFAULT_PNLU_REDIS_HOST: &str = "127.0.0.1";
const DEFAULT_PNLU_REDIS_PORT: u16 = 6379;
const DEFAULT_PNLU_REDIS_DB: i64 = 0;
const DEFAULT_PNLU_KEY_SUFFIX: &str = "_pnlu_factor_thresholds";
const PNLU_MAX_AGE_SECS: i64 = 30 * 60;
const TARGET_FACTOR_NAME: &str = "rl_return_volatility";
const TARGET_FACTOR_KEY_PREFIX: &str = TARGET_FACTOR_NAME;

#[derive(Parser, Debug, Clone)]
#[command(name = "manual_mm_signal")]
#[command(about = "Manual MM signal (web UI + mm hedge query responder)")]
struct CliArgs {
    /// Config file path
    #[arg(long, default_value = "config/manual_mm_signal.yaml")]
    config: String,
}

#[derive(Debug, Deserialize, Clone)]
struct ManualMmConfig {
    venue: String,
    port: Option<u16>,
    bind: Option<String>,
    from_key: Option<String>,
    model_service: String,
}

#[derive(Debug, Clone)]
struct AppCfg {
    venue: TradingVenue,
    bind: String,
    port: u16,
    signal_channel: String,
    backward_channel: String,
    default_open_ttl_ms: u64,
    open_order_ttl_us: i64,
    order_amount_u: f64,
    open_orders_per_round: u32,
    open_buy_vol_scale: [f64; 2],
    open_sell_vol_scale: [f64; 2],
    hedge_orders_per_round: u32,
    hedge_vol_multiplier: f64,
    hedge_offset_ratio: f64,
    enable_return_score_adjust_hedge: bool,
    hedge_price_offset_limit_upper: f64,
    hedge_price_offset_limit_lower: f64,
    next_query_delay_ms: u64,
    from_key: Vec<u8>,
    model_service: String,
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
    model_scores: Arc<RwLock<ModelScoreCache>>,
    mm_hedge_tlen: Arc<RwLock<Option<MmHedgeTlenSnapshot>>>,
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
struct ApiOpenRequest {
    symbol: String,
    from_key: Option<String>,
}

#[derive(Debug, Clone)]
struct ManualOpenRequest {
    symbol: String,
    bid: f64,
    ask: f64,
    quote_ts: i64,
    from_key: Option<String>,
    return_score: Option<f64>,
}

#[derive(Debug, Serialize)]
struct ManualOpenResponse {
    ok: bool,
    published_at_us: i64,
    symbol: String,
    venue: TradingVenue,
    bid: f64,
    ask: f64,
    quote_ts: i64,
    exp_time_us: i64,
    order_amount_u: f64,
    open_orders_per_round: u32,
    open_buy_vol_scale: [f64; 2],
    open_sell_vol_scale: [f64; 2],
    price_tick: f64,
    qty_tick: f64,
    from_key: String,
    level_from_keys: Vec<String>,
    mid_price: f64,
    volatility: f64,
    band_lower: f64,
    band_upper: f64,
    factor_ready: bool,
    factor_key: String,
    factor_note: String,
    levels: Vec<OpenPreviewLevel>,
    buy_orders: usize,
    sell_orders: usize,
}

#[derive(Debug, Serialize, Clone)]
struct OpenPreviewLevel {
    side: String,
    side_level_index: usize,
    offset: f64,
    base_price: f64,
    raw_price: f64,
    raw_qty: f64,
    aligned_price: f64,
    aligned_qty: f64,
    price_tick_count: i64,
    qty_tick_count: i64,
    tlen: Option<f64>,
    from_key: String,
}

#[derive(Debug, Serialize)]
struct ConfigResponse {
    venue: TradingVenue,
    bind: String,
    port: u16,
    default_open_ttl_ms: u64,
    order_amount_u: f64,
    open_orders_per_round: u32,
    open_buy_vol_scale: [f64; 2],
    open_sell_vol_scale: [f64; 2],
    hedge_orders_per_round: u32,
    hedge_vol_multiplier: f64,
    hedge_offset_ratio: f64,
    enable_return_score_adjust_hedge: bool,
    hedge_price_offset_limit_upper: f64,
    hedge_price_offset_limit_lower: f64,
    next_query_delay_ms: u64,
    from_key: String,
    model_service: String,
    target_factor: String,
}

#[derive(Debug, Clone, Copy)]
struct ModelScore {
    score: f64,
    fresh_ts_us: i64,
}

#[derive(Debug, Default)]
struct ModelScoreCache {
    scores: HashMap<String, ModelScore>,
}

#[derive(Debug, Serialize)]
struct SymbolsResponse {
    symbols: Vec<String>,
}

#[derive(Debug, Serialize)]
struct QuoteResponse {
    symbol: String,
    venue: TradingVenue,
    bid: f64,
    ask: f64,
    ts: i64,
}

#[derive(Debug, Serialize, Clone)]
struct MmHedgeTlenLevel {
    index: usize,
    price: f64,
    amount: f64,
    offset: f64,
    price_tick_i64: i64,
    price_tick_exp: i32,
    price_count: i64,
    tlen: Option<f64>,
}

#[derive(Debug, Serialize, Clone)]
struct MmHedgeTlenSnapshot {
    updated_at_us: i64,
    symbol: String,
    signal_ts: i64,
    next_query_ts: i64,
    levels: usize,
    from_key: String,
    query_ok: bool,
    query_err: Option<String>,
    rows: Vec<MmHedgeTlenLevel>,
}

#[derive(Debug, Deserialize)]
struct QuoteQuery {
    symbol: String,
}

fn build_mm_hedge_tlen_snapshot(
    symbol: &str,
    ctx: &MmHedgeCtx,
    tlen_values: Option<&[f64]>,
    query_err: Option<String>,
) -> MmHedgeTlenSnapshot {
    let mut rows = Vec::with_capacity(ctx.price_qv_list.len());
    for (index, (price_qv, amount_qv)) in ctx
        .price_qv_list
        .iter()
        .zip(ctx.amount_qv_list.iter())
        .enumerate()
    {
        let (price_tick_i64, price_tick_exp) = price_qv.get_tick_parts();
        let price_count = price_qv.get_count();
        let offset = ctx.price_offsets.get(index).copied().unwrap_or(0.0);
        let tlen = tlen_values.and_then(|values| values.get(index)).copied();
        rows.push(MmHedgeTlenLevel {
            index,
            price: price_qv.get_val(),
            amount: amount_qv.get_val(),
            offset,
            price_tick_i64,
            price_tick_exp,
            price_count,
            tlen,
        });
    }

    MmHedgeTlenSnapshot {
        updated_at_us: get_timestamp_us(),
        symbol: symbol.to_string(),
        signal_ts: ctx.signal_ts,
        next_query_ts: ctx.next_query_ts,
        levels: rows.len(),
        from_key: String::from_utf8_lossy(&ctx.from_key).to_string(),
        query_ok: query_err.is_none(),
        query_err,
        rows,
    }
}

fn format_mm_hedge_tlen_rows(rows: &[MmHedgeTlenLevel]) -> String {
    if rows.is_empty() {
        return "none".to_string();
    }
    rows.iter()
        .map(|row| {
            let tlen_text = match row.tlen {
                Some(v) => format!("{v:.8}"),
                None => "NA".to_string(),
            };
            format!(
                "#{} p={:.8}(tick={}/10^{},cnt={}) qty={:.8} offset={:.8} tlen={}",
                row.index,
                row.price,
                row.price_tick_i64,
                row.price_tick_exp,
                row.price_count,
                row.amount,
                row.offset,
                tlen_text
            )
        })
        .collect::<Vec<_>>()
        .join(" | ")
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

fn ensure_supported_mm_open_venue(venue: TradingVenue) -> Result<()> {
    ensure_supported_mm_open_venue_raw(venue).map_err(anyhow::Error::msg)
}

async fn load_config(path: &str) -> Result<AppCfg> {
    let raw = fs::read_to_string(path).with_context(|| format!("read config failed: {}", path))?;
    let cfg: ManualMmConfig =
        serde_yaml::from_str(&raw).with_context(|| format!("parse yaml failed: {}", path))?;

    let venue =
        venue_from_slug(&cfg.venue).with_context(|| format!("invalid venue: {}", cfg.venue))?;
    ensure_supported_mm_open_venue(venue)?;
    let port = cfg.port.unwrap_or(DEFAULT_PORT);
    let bind = cfg.bind.unwrap_or_else(|| DEFAULT_BIND.to_string());
    let signal_channel = DEFAULT_SIGNAL_CHANNEL.to_string();
    let backward_channel = DEFAULT_BACKWARD_CHANNEL.to_string();
    let from_key = cfg
        .from_key
        .unwrap_or_else(|| PROCESS_NAME.to_string())
        .into_bytes();

    let params_key = format!("mm_strategy_params_{}", venue.data_pub_slug());
    let mut redis = RedisClient::connect(get_redis_settings())
        .await
        .with_context(|| "connect redis failed")?;
    let params = redis
        .hgetall_map(&params_key)
        .await
        .with_context(|| format!("read redis hash failed: {}", params_key))?;
    if params.is_empty() {
        anyhow::bail!(
            "redis hash '{}' is empty; run scripts/sync_mm_strategy_params.py",
            params_key
        );
    }

    let order_amount_u = parse_required_f64(&params, "order_amount")?;
    let open_orders_per_round = parse_required_u32(&params, "open_orders_per_round")?;
    if open_orders_per_round == 0 {
        panic!(
            "redis hash '{}' open_orders_per_round invalid(need >0): {}",
            params_key, open_orders_per_round
        );
    }
    let hedge_orders_per_round = parse_required_u32(&params, "hedge_orders_per_round")?;
    if hedge_orders_per_round == 0 {
        panic!(
            "redis hash '{}' hedge_orders_per_round invalid(need >0): {}",
            params_key, hedge_orders_per_round
        );
    }
    let open_timeout_s = parse_required_u64(&params, "open_order_timeout")?;
    let open_buy_vol_scale = parse_required_f64_pair_json(&params, "open_buy_vol_scale")?;
    let open_sell_vol_scale = parse_required_f64_pair_json(&params, "open_sell_vol_scale")?;
    let next_query_delay_ms = parse_required_u64(&params, "next_query_delay_ms")?;
    let hedge_vol_multiplier = parse_required_f64(&params, "hedge_vol_multiplier")?;
    let hedge_offset_ratio = parse_required_f64(&params, "hedge_offset_ratio")?;
    let enable_return_score_adjust_hedge = params
        .get("enable_return_score_adjust_hegde")
        .or_else(|| params.get("enable_return_score_adjust_hedge"))
        .map(|raw| {
            matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "true" | "1" | "yes" | "on"
            )
        })
        .unwrap_or(true);
    let hedge_price_offset_limit_upper =
        parse_required_f64(&params, "hedge_price_offset_limit_upper")?;
    let hedge_price_offset_limit_lower =
        parse_required_f64(&params, "hedge_price_offset_limit_lower")?;
    let default_open_ttl_ms = open_timeout_s.saturating_mul(1000);
    let open_order_ttl_us = (open_timeout_s as i64).saturating_mul(1_000_000);

    if !(order_amount_u > 0.0) {
        anyhow::bail!("order_amount must be > 0");
    }
    if !(hedge_vol_multiplier.is_finite() && hedge_vol_multiplier > 0.0) {
        anyhow::bail!("hedge_vol_multiplier must be finite and > 0");
    }
    if !(hedge_offset_ratio.is_finite() && hedge_offset_ratio > 0.0) {
        anyhow::bail!("hedge_offset_ratio must be finite and > 0");
    }
    if !hedge_price_offset_limit_upper.is_finite() || !hedge_price_offset_limit_lower.is_finite() {
        anyhow::bail!("hedge price offset limits must be finite");
    }

    Ok(AppCfg {
        venue,
        bind,
        port,
        signal_channel,
        backward_channel,
        default_open_ttl_ms,
        open_order_ttl_us,
        order_amount_u,
        open_orders_per_round,
        open_buy_vol_scale,
        open_sell_vol_scale,
        hedge_orders_per_round,
        hedge_vol_multiplier,
        hedge_offset_ratio,
        enable_return_score_adjust_hedge,
        hedge_price_offset_limit_upper,
        hedge_price_offset_limit_lower,
        next_query_delay_ms,
        from_key,
        model_service: cfg.model_service,
    })
}

fn parse_required_f64(params: &HashMap<String, String>, key: &str) -> Result<f64> {
    let raw = params
        .get(key)
        .ok_or_else(|| anyhow::anyhow!("missing redis field: {}", key))?;
    raw.parse::<f64>()
        .with_context(|| format!("parse redis field '{}' failed: {}", key, raw))
}

fn parse_required_u64(params: &HashMap<String, String>, key: &str) -> Result<u64> {
    let raw = params
        .get(key)
        .ok_or_else(|| anyhow::anyhow!("missing redis field: {}", key))?;
    raw.parse::<u64>()
        .with_context(|| format!("parse redis field '{}' failed: {}", key, raw))
}

fn parse_required_u32(params: &HashMap<String, String>, key: &str) -> Result<u32> {
    let raw = params
        .get(key)
        .ok_or_else(|| anyhow::anyhow!("missing redis field: {}", key))?;
    raw.parse::<u32>()
        .with_context(|| format!("parse redis field '{}' failed: {}", key, raw))
}

fn parse_required_f64_pair_json(params: &HashMap<String, String>, key: &str) -> Result<[f64; 2]> {
    let raw = params
        .get(key)
        .ok_or_else(|| anyhow::anyhow!("missing redis field: {}", key))?;
    let values = serde_json::from_str::<Vec<f64>>(raw)
        .with_context(|| format!("parse redis field '{}' as JSON array failed: {}", key, raw))?;
    if values.len() != 2 {
        anyhow::bail!(
            "redis field '{}' must be a JSON array of length 2, got: {}",
            key,
            raw
        );
    }
    let low = values[0];
    let high = values[1];
    if !low.is_finite() || !high.is_finite() || low < 0.0 || high < low {
        anyhow::bail!(
            "redis field '{}' must satisfy 0<=low<=high, got [{}, {}]",
            key,
            low,
            high
        );
    }
    Ok([low, high])
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

fn spawn_model_listener(
    model_service: String,
    model_scores: Arc<RwLock<ModelScoreCache>>,
    token: CancellationToken,
) {
    tokio::task::spawn_local(async move {
        let node_name = format!("{}_model_listener", PROCESS_NAME);
        let result: Result<()> = async {
            let node = NodeBuilder::new()
                .name(&NodeName::new(&node_name)?)
                .create::<ipc::Service>()?;

            let service = node
                .service_builder(&ServiceName::new(&model_service)?)
                .publish_subscribe::<[u8; MODEL_PAYLOAD_MAX_BYTES]>()
                .max_publishers(1)
                .max_subscribers(10)
                .history_size(128)
                .open_or_create()?;

            let subscriber: Subscriber<ipc::Service, [u8; MODEL_PAYLOAD_MAX_BYTES], ()> =
                service.subscriber_builder().create()?;

            info!("manual_mm model listener ready: {}", model_service);

            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {
                        while let Some(sample) = subscriber.receive()? {
                            let payload = sample.payload();
                            match ModelMsg::from_bytes(payload) {
                                Ok(msg) => {
                                    if msg.status != MODEL_STATUS_OK {
                                        continue;
                                    }
                                    let symbol = msg.symbol.to_uppercase();

                                    // Log factor table for every message
                                    if msg.feature_dim > 0 {
                                        let mut table = format!(
                                            "ModelMsg {} score={:.6} dim={}\n  {:>4} | {:>30} | {:>12}\n  {:-<4}-+-{:-<30}-+-{:-<12}",
                                            symbol, msg.score, msg.feature_dim,
                                            "idx", "factor_name", "zscore",
                                            "", "", ""
                                        );
                                        for i in 0..msg.feature_dim as usize {
                                            let idx = msg.factor_indices[i];
                                            let name = fusion_factor_index_to_name(idx)
                                                .or_else(|| ExtraFactorId::index_to_name(idx))
                                                .unwrap_or("unknown");
                                            let val = msg.factor_values[i];
                                            table.push_str(&format!(
                                                "\n  {:>4} | {:>30} | {:>12.6}",
                                                idx, name, val
                                            ));
                                        }
                                        info!("{}", table);
                                    }

                                    let mut cache = model_scores.write();
                                    cache.scores.insert(symbol, ModelScore {
                                        score: msg.score,
                                        fresh_ts_us: get_timestamp_us(),
                                    });
                                }
                                Err(err) => {
                                    warn!("manual_mm: decode ModelMsg failed: {err}");
                                }
                            }
                        }
                    }
                }
            }

            Ok(())
        }
        .await;

        if let Err(err) = result {
            warn!("manual_mm model listener exited: {err:#}");
        }
    });
}

fn build_mm_hedge_ctx(
    cfg: &AppCfg,
    quotes: &Arc<RwLock<QuoteCache>>,
    table: &Arc<RwLock<VenueMinQtyTable>>,
    factor_value_hub: &mut FactorValueHub,
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
    let (signal, volatility) =
        resolve_mm_hedge_signal_inputs(factor_value_hub, &cfg.model_service, &symbol, cfg.venue)
            .map_err(anyhow::Error::msg)?;

    let table_guard = table.read();
    let input = MmHedgeBuildInput {
        venue: cfg.venue,
        symbol: &symbol,
        quote,
        volatility,
        signal,
        enable_return_score_adjust_hedge: cfg.enable_return_score_adjust_hedge,
        hedge_vol_multiplier: cfg.hedge_vol_multiplier,
        hedge_offset_ratio: cfg.hedge_offset_ratio,
        order_amount_u: cfg.order_amount_u,
        hedge_orders_per_round: cfg.hedge_orders_per_round,
        offset_low: cfg.hedge_price_offset_limit_lower,
        offset_high_limit: cfg.hedge_price_offset_limit_upper,
        next_query_delay_ms: cfg.next_query_delay_ms,
    };
    build_mm_hedge_ctx_core(input, &table_guard, query).map_err(anyhow::Error::msg)
}

fn spawn_ask_bid_listener(
    venue: TradingVenue,
    quotes: Arc<RwLock<QuoteCache>>,
    token: CancellationToken,
) {
    tokio::task::spawn_local(async move {
        let node_name = format!("{}_ask_bid_{}", PROCESS_NAME, venue.data_pub_slug());
        let service_path =
            build_service_name(&format!("bridge/{}/ask_bid_spread", venue.data_pub_slug()));

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
    mm_hedge_tlen: Arc<RwLock<Option<MmHedgeTlenSnapshot>>>,
    publish_tx: mpsc::UnboundedSender<PublishCmd>,
    token: CancellationToken,
) {
    tokio::task::spawn_local(async move {
        let node_name = format!("{}_backward_sub", PROCESS_NAME);
        let service_path = build_service_name(&format!("signal_pubs/{}", cfg.backward_channel));

        let result: Result<()> = async move {
            let depth_client = match DepthQueryClient::new(cfg.venue) {
                Ok(client) => Some(client),
                Err(err) => {
                    warn!(
                        "manual_mm: depth query client unavailable, continue without tlen: {err:#}"
                    );
                    None
                }
            };
            let factor_node_name = format!("{}_hedge_factor_hub", PROCESS_NAME);
            let factor_node = NodeBuilder::new()
                .name(&NodeName::new(&factor_node_name)?)
                .create::<ipc::Service>()?;
            let pnlu_settings = RedisSettings {
                host: DEFAULT_PNLU_REDIS_HOST.to_string(),
                port: DEFAULT_PNLU_REDIS_PORT,
                db: DEFAULT_PNLU_REDIS_DB,
                username: None,
                password: None,
                prefix: None,
            };
            let mut factor_value_hub = FactorValueHub::new(
                &factor_node,
                cfg.venue,
                cfg.venue,
                TARGET_FACTOR_NAME,
                TARGET_FACTOR_KEY_PREFIX,
                pnlu_settings,
                DEFAULT_PNLU_KEY_SUFFIX.to_string(),
                PNLU_MAX_AGE_SECS,
            )?;
            if !cfg.model_service.trim().is_empty() && cfg.model_service.trim() != "-" {
                factor_value_hub
                    .update_model_output_services(&factor_node, vec![cfg.model_service.clone()]);
            }

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

                                let mut ctx = match build_mm_hedge_ctx(
                                    &cfg,
                                    &quotes,
                                    &min_qty_table,
                                    &mut factor_value_hub,
                                    &query,
                                ) {
                                    Ok(ctx) => ctx,
                                    Err(err) => {
                                        warn!("manual_mm: build hedge ctx failed: {err:#}");
                                        continue;
                                    }
                                };

                                let symbol = ctx.get_opening_symbol();
                                let query_tick_indices: Vec<i64> = ctx
                                    .price_qv_list
                                    .iter()
                                    .map(|qv| qv.get_count())
                                    .collect();

                                let tlen_query_result = if let Some(client) = depth_client.as_ref() {
                                    client.query_batch_tick_indices(&symbol, &query_tick_indices)
                                } else {
                                    Err(anyhow!("depth query client unavailable"))
                                };
                                let snapshot = match tlen_query_result {
                                    Ok(tlens) => {
                                        ctx.tlen_values = tlens;
                                        build_mm_hedge_tlen_snapshot(
                                            &symbol,
                                            &ctx,
                                            Some(ctx.tlen_values.as_slice()),
                                            None,
                                        )
                                    }
                                    Err(err) => {
                                        ctx.tlen_values = vec![0.0; query_tick_indices.len()];
                                        build_mm_hedge_tlen_snapshot(
                                            &symbol,
                                            &ctx,
                                            Some(ctx.tlen_values.as_slice()),
                                            Some(err.to_string()),
                                        )
                                    }
                                };
                                let rows_text = format_mm_hedge_tlen_rows(&snapshot.rows);
                                *mm_hedge_tlen.write() = Some(snapshot.clone());

                                if let Some(err) = &snapshot.query_err {
                                    warn!(
                                        "manual_mm: hedge qv+tlen symbol={} levels={} query_err='{}' rows={}",
                                        symbol,
                                        snapshot.levels,
                                        err,
                                        rows_text
                                    );
                                } else {
                                    info!(
                                        "manual_mm: hedge qv+tlen symbol={} levels={} rows={}",
                                        symbol,
                                        snapshot.levels,
                                        rows_text
                                    );
                                }

                                info!(
                                    "manual_mm: hedge query reply symbol={} levels={}",
                                    symbol,
                                    ctx.price_qv_list.len(),
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
    min_qty_table: Arc<RwLock<VenueMinQtyTable>>,
    mut rx: mpsc::UnboundedReceiver<PublishCmd>,
    token: CancellationToken,
) {
    tokio::task::spawn_local(async move {
        let depth_client = match DepthQueryClient::new(cfg.venue) {
            Ok(client) => Some(client),
            Err(err) => {
                warn!("manual_mm: depth query client unavailable for open path: {err:#}");
                None
            }
        };
        let publisher = SignalPublisher::new(&cfg.signal_channel)
            .with_context(|| format!("create SignalPublisher failed on '{}'", cfg.signal_channel))
            .expect("failed to create SignalPublisher");
        let factor_node_name = format!("{}_factor_hub", PROCESS_NAME);
        let factor_node = NodeBuilder::new()
            .name(&NodeName::new(&factor_node_name).expect("invalid factor node name"))
            .create::<ipc::Service>()
            .expect("failed to create factor hub node");

        let pnlu_settings = RedisSettings {
            host: DEFAULT_PNLU_REDIS_HOST.to_string(),
            port: DEFAULT_PNLU_REDIS_PORT,
            db: DEFAULT_PNLU_REDIS_DB,
            username: None,
            password: None,
            prefix: None,
        };
        let mut factor_value_hub = FactorValueHub::new(
            &factor_node,
            cfg.venue,
            cfg.venue,
            TARGET_FACTOR_NAME,
            TARGET_FACTOR_KEY_PREFIX,
            pnlu_settings,
            DEFAULT_PNLU_KEY_SUFFIX.to_string(),
            PNLU_MAX_AGE_SECS,
        )
        .expect("failed to create FactorValueHub for manual_mm_signal");

        info!(
            "manual_mm signal publisher ready: channel={} venue={:?} factor={}",
            cfg.signal_channel, cfg.venue, TARGET_FACTOR_NAME
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
                            let res = build_and_publish_open(
                                &cfg,
                                &publisher,
                                depth_client.as_ref(),
                                &min_qty_table,
                                &mut factor_value_hub,
                                req,
                            );
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
    depth_client: Option<&DepthQueryClient>,
    min_qty_table: &Arc<RwLock<VenueMinQtyTable>>,
    factor_value_hub: &mut FactorValueHub,
    req: ManualOpenRequest,
) -> Result<ManualOpenResponse> {
    let symbol = req.symbol.trim().to_uppercase();
    if symbol.is_empty() {
        anyhow::bail!("symbol is empty");
    }
    if req.bid <= 0.0 || req.ask <= 0.0 || req.bid >= req.ask {
        anyhow::bail!("invalid quote bid={} ask={}", req.bid, req.ask);
    }

    let now_us = get_timestamp_us();
    let factor_lookup = factor_value_hub.lookup_target_factor_value(&symbol, cfg.venue);
    let factor_ready = factor_lookup.ready.unwrap_or(false);
    let volatility = factor_lookup
        .target_factor_value
        .filter(|v| v.is_finite())
        .ok_or_else(|| {
            anyhow!(
                "missing or invalid volatility factor key={} note={}",
                factor_lookup.key,
                factor_lookup.note
            )
        })?;

    let plan = {
        let table_guard = min_qty_table.read();
        build_mm_open_quote_plan(
            cfg.venue,
            &symbol,
            Quote {
                bid: req.bid,
                ask: req.ask,
                ts: req.quote_ts,
            },
            cfg.order_amount_u,
            cfg.open_orders_per_round,
            cfg.open_order_ttl_us,
            volatility,
            cfg.open_buy_vol_scale,
            cfg.open_sell_vol_scale,
            now_us,
            &table_guard,
        )
        .map_err(anyhow::Error::msg)?
    };

    let from_key_override = req
        .from_key
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let base_from_key = if let Some(v) = from_key_override {
        v
    } else {
        build_decision_from_key_base(
            plan.now_us,
            req.return_score.filter(|v| v.is_finite()),
            None,
            Some(plan.band.volatility),
            None,
            None,
        )
    };

    struct PreparedOpenSignal {
        tick_index: i64,
        ctx: MmOpenCtx,
    }

    let mut prepared = Vec::with_capacity(plan.levels.len());
    for level in &plan.levels {
        let mut ctx = MmOpenCtx::new();
        ctx.opening_leg = TradingLeg::new(cfg.venue, plan.quote.bid, plan.quote.ask, plan.quote.ts);
        ctx.set_opening_symbol(&plan.symbol);
        ctx.set_side(level.side);
        ctx.set_order_type(OrderType::Limit);
        let _ = ctx.set_amount_with_tick_floor(level.aligned_qty, plan.qty_tick);
        let _ = ctx.set_price_with_tick_floor(level.aligned_price, plan.price_tick);
        if ctx.amount_count() <= 0 || ctx.price_count() <= 0 {
            continue;
        }
        ctx.exp_time = plan.exp_time_us;
        ctx.create_ts = plan.now_us;
        ctx.price_offset = level.offset;
        prepared.push(PreparedOpenSignal {
            tick_index: ctx.price_count(),
            ctx,
        });
    }

    let mut level_tlens = Vec::with_capacity(prepared.len());
    let level_from_keys = if prepared.is_empty() {
        Vec::new()
    } else {
        let tick_indices: Vec<i64> = prepared.iter().map(|item| item.tick_index).collect();
        match depth_client {
            Some(client) => match client.query_batch_tick_indices(&plan.symbol, &tick_indices) {
                Ok(tlens) => {
                    level_tlens = tlens.iter().copied().map(Some).collect();
                    prepared
                        .iter()
                        .zip(tlens.iter().copied())
                        .map(|(_, level_tlen)| {
                            append_mm_open_tlens_to_from_key(&base_from_key, level_tlen)
                        })
                        .collect::<Vec<_>>()
                }
                Err(err) => {
                    warn!(
                        "manual_mm: MMOpen tlen batch query failed symbol={} levels={} err={:#}",
                        plan.symbol,
                        prepared.len(),
                        err
                    );
                    level_tlens = vec![None; prepared.len()];
                    prepared
                        .iter()
                        .map(|_| append_mm_open_tlens_to_from_key(&base_from_key, 0.0))
                        .collect::<Vec<_>>()
                }
            },
            None => {
                level_tlens = vec![None; prepared.len()];
                prepared
                    .iter()
                    .map(|_| append_mm_open_tlens_to_from_key(&base_from_key, 0.0))
                    .collect::<Vec<_>>()
            }
        }
    };

    for (item, level_from_key) in prepared.iter_mut().zip(level_from_keys.iter()) {
        item.ctx.set_from_key(level_from_key.clone().into_bytes());
        let signal = TradeSignal::create(SignalType::MMOpen, plan.now_us, 0.0, item.ctx.to_bytes());
        publisher.publish(&signal.to_bytes())?;
    }

    let (mut levels, buy_orders, sell_orders) = plan_to_levels_and_counts(&plan.levels);
    for ((level, level_from_key), tlen) in levels
        .iter_mut()
        .zip(level_from_keys.iter())
        .zip(level_tlens.into_iter())
    {
        level.from_key = level_from_key.clone();
        level.tlen = tlen;
    }

    Ok(ManualOpenResponse {
        ok: true,
        published_at_us: plan.now_us,
        symbol: plan.symbol,
        venue: cfg.venue,
        bid: plan.quote.bid,
        ask: plan.quote.ask,
        quote_ts: plan.quote.ts,
        exp_time_us: plan.exp_time_us,
        order_amount_u: plan.order_amount_u,
        open_orders_per_round: plan.orders_per_round,
        open_buy_vol_scale: cfg.open_buy_vol_scale,
        open_sell_vol_scale: cfg.open_sell_vol_scale,
        price_tick: plan.price_tick,
        qty_tick: plan.qty_tick,
        from_key: base_from_key,
        level_from_keys,
        mid_price: plan.band.mid_price,
        volatility: plan.band.volatility,
        band_lower: plan.band.lower_price,
        band_upper: plan.band.upper_price,
        factor_ready,
        factor_key: factor_lookup.key,
        factor_note: factor_lookup.note,
        levels,
        buy_orders,
        sell_orders,
    })
}

fn plan_to_levels_and_counts(levels: &[MmOpenPlanLevel]) -> (Vec<OpenPreviewLevel>, usize, usize) {
    let mut buy_orders = 0usize;
    let mut sell_orders = 0usize;
    let levels = levels
        .iter()
        .map(|order| {
            match order.side {
                Side::Buy => buy_orders += 1,
                Side::Sell => sell_orders += 1,
            }
            OpenPreviewLevel {
                side: order.side.as_str_lower().to_string(),
                side_level_index: order.side_level_index,
                offset: order.offset,
                base_price: order.base_price,
                raw_price: order.raw_price,
                raw_qty: order.raw_qty,
                aligned_price: order.aligned_price,
                aligned_qty: order.aligned_qty,
                price_tick_count: order.price_tick_count,
                qty_tick_count: order.qty_tick_count,
                tlen: None,
                from_key: String::new(),
            }
        })
        .collect::<Vec<_>>();
    (levels, buy_orders, sell_orders)
}

async fn api_config(State(st): State<AppState>) -> impl IntoResponse {
    Json(ConfigResponse {
        venue: st.cfg.venue,
        bind: st.cfg.bind.clone(),
        port: st.cfg.port,
        default_open_ttl_ms: st.cfg.default_open_ttl_ms,
        order_amount_u: st.cfg.order_amount_u,
        open_orders_per_round: st.cfg.open_orders_per_round,
        open_buy_vol_scale: st.cfg.open_buy_vol_scale,
        open_sell_vol_scale: st.cfg.open_sell_vol_scale,
        hedge_orders_per_round: st.cfg.hedge_orders_per_round,
        hedge_vol_multiplier: st.cfg.hedge_vol_multiplier,
        hedge_offset_ratio: st.cfg.hedge_offset_ratio,
        enable_return_score_adjust_hedge: st.cfg.enable_return_score_adjust_hedge,
        hedge_price_offset_limit_upper: st.cfg.hedge_price_offset_limit_upper,
        hedge_price_offset_limit_lower: st.cfg.hedge_price_offset_limit_lower,
        next_query_delay_ms: st.cfg.next_query_delay_ms,
        from_key: String::from_utf8_lossy(&st.cfg.from_key).to_string(),
        model_service: st.cfg.model_service.clone(),
        target_factor: TARGET_FACTOR_NAME.to_string(),
    })
}

async fn api_symbols(State(st): State<AppState>) -> impl IntoResponse {
    let model_cache = st.model_scores.read();
    let quote_cache = st.quotes.read();
    let mut symbols: Vec<String> = model_cache
        .scores
        .keys()
        .chain(quote_cache.quotes.keys())
        .cloned()
        .collect();
    symbols.sort();
    symbols.dedup();
    Json(SymbolsResponse { symbols })
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
    Json(req): Json<ApiOpenRequest>,
) -> impl IntoResponse {
    let symbol = req.symbol.trim().to_uppercase();
    if symbol.is_empty() {
        return (StatusCode::BAD_REQUEST, "symbol is empty").into_response();
    }
    let quote = st.quotes.read().get_valid(&symbol);
    let Some(qt) = quote else {
        return (StatusCode::BAD_REQUEST, "quote unavailable").into_response();
    };
    let return_score = st
        .model_scores
        .read()
        .scores
        .get(&symbol)
        .map(|ms| ms.score)
        .filter(|v| v.is_finite());
    let req = ManualOpenRequest {
        symbol,
        bid: qt.bid,
        ask: qt.ask,
        quote_ts: qt.ts,
        from_key: req.from_key,
        return_score,
    };
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

async fn api_mm_hedge_tlen(State(st): State<AppState>) -> impl IntoResponse {
    let snapshot = st.mm_hedge_tlen.read().clone();
    Json(snapshot).into_response()
}

#[derive(Debug, Serialize)]
struct ModelScoreEntry {
    symbol: String,
    score: f64,
    fresh_ts_us: i64,
}

async fn api_model_scores(State(st): State<AppState>) -> impl IntoResponse {
    let cache = st.model_scores.read();
    let mut entries: Vec<ModelScoreEntry> = cache
        .scores
        .iter()
        .map(|(symbol, ms)| ModelScoreEntry {
            symbol: symbol.clone(),
            score: ms.score,
            fresh_ts_us: ms.fresh_ts_us,
        })
        .collect();
    entries.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    Json(entries).into_response()
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
        .route("/api/mm_hedge_tlen", get(api_mm_hedge_tlen))
        .route("/api/model_scores", get(api_model_scores))
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
      :root {
        --fg: #111;
        --muted: #667085;
        --line: #dfe3e8;
        --bg: #f8fafc;
        --ok: #067647;
        --err: #b42318;
      }
      body {
        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
        margin: 14px;
        color: var(--fg);
        background: #fff;
      }
      h2, h3 {
        margin: 0 0 10px 0;
      }
      .muted { color: var(--muted); font-size: 12px; }
      .layout {
        display: grid;
        grid-template-columns: 320px 1fr;
        gap: 12px;
        align-items: start;
      }
      .panel {
        border: 1px solid var(--line);
        border-radius: 12px;
        background: var(--bg);
        padding: 12px;
      }
      .stack { display: grid; gap: 12px; }
      .row { display: flex; gap: 8px; align-items: center; }
      .row-between { display: flex; justify-content: space-between; align-items: center; gap: 8px; }
      .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
      input, button {
        font-size: 13px;
        padding: 8px;
        border: 1px solid var(--line);
        border-radius: 8px;
        box-sizing: border-box;
      }
      input { width: 100%; }
      button {
        cursor: pointer;
        background: #fff;
      }
      button:disabled {
        cursor: not-allowed;
        opacity: 0.55;
      }
      ul {
        margin: 0;
        padding-left: 18px;
        max-height: 360px;
        overflow: auto;
      }
      li {
        cursor: pointer;
        line-height: 1.45;
      }
      li:hover { text-decoration: underline; }
      .pill {
        font-size: 11px;
        padding: 3px 8px;
        border-radius: 999px;
        border: 1px solid #cdd5df;
        background: #fff;
      }
      .pill.ok {
        color: var(--ok);
        border-color: #a6f4c5;
        background: #ecfdf3;
      }
      .pill.err {
        color: var(--err);
        border-color: #fecdca;
        background: #fef3f2;
      }
      .kv {
        display: grid;
        grid-template-columns: 140px 1fr;
        gap: 4px 10px;
        font-size: 12px;
      }
      .kv .k { color: var(--muted); }
      .table-wrap {
        max-height: 360px;
        overflow: auto;
        border: 1px solid var(--line);
        border-radius: 10px;
        background: #fff;
      }
      table {
        width: 100%;
        border-collapse: collapse;
        font-size: 12px;
      }
      tfoot td {
        font-weight: 600;
        background: #f2f4f7;
      }
      th, td {
        border-bottom: 1px solid #edf0f3;
        padding: 6px 8px;
        text-align: right;
        white-space: nowrap;
      }
      th:first-child, td:first-child { text-align: left; }
      th:nth-child(2), td:nth-child(2) { text-align: left; }
      .buy { color: #175cd3; }
      .sell { color: #b42318; }
      textarea {
        width: 100%;
        box-sizing: border-box;
        border: 1px solid var(--line);
        border-radius: 10px;
        padding: 8px;
        font-size: 12px;
        line-height: 1.35;
        background: #fff;
      }
    </style>
  </head>
  <body>
    <h2>manual_mm_signal</h2>
    <div class="muted" id="cfgLine"></div>

    <div class="layout" style="margin-top: 12px;">
      <div class="stack">
        <section class="panel">
          <div class="row-between">
            <h3 style="font-size:15px;">Symbols</h3>
            <span class="pill" id="statusPill">IDLE</span>
          </div>
          <div class="muted">筛选并选中交易对，自动刷新 quote</div>
          <div style="margin-top:8px;">
            <input id="q" placeholder="Search e.g. BTCUSDT" />
          </div>
          <div style="margin-top:8px;">
            <input id="symbol" placeholder="Selected symbol" />
          </div>
          <div style="margin-top:8px;">
            <ul id="list"></ul>
          </div>
        </section>

        <section class="panel">
          <h3 style="font-size:15px;">Actions</h3>
          <div class="grid2">
            <div>
              <div class="muted">From Key (optional)</div>
              <input id="fromKey" />
            </div>
            <div>
              <div class="muted">Quote</div>
              <div id="quote" style="font-size:12px; padding-top:9px;">N/A</div>
            </div>
          </div>
          <div class="row" style="margin-top:10px;">
            <button id="refreshQuote">Refresh Quote</button>
            <button id="send">一键发号</button>
          </div>
          <div class="muted" id="actionHint" style="margin-top:8px;">请选择 symbol 后点击一键发号（默认 from_key 包含 ret_score/ret_thr/vol/env_score/env_thr，缺失时为 0）</div>
        </section>
      </div>

      <div class="stack">
        <section class="panel">
          <h3 style="font-size:15px;">Config Summary</h3>
          <div class="kv" id="cfgKv"></div>
        </section>

        <section class="panel">
          <div class="row-between">
            <h3 style="font-size:15px;">拆单报价结果</h3>
            <div class="muted" id="splitMeta">No send yet</div>
          </div>
          <div class="muted" style="margin-top:8px;">MMOpen from_key</div>
          <textarea id="openFromKeys" rows="5" readonly style="margin-top:6px;"></textarea>
          <div class="table-wrap" style="margin-top:8px;">
            <table>
              <thead>
                <tr>
                  <th>side</th>
                  <th>side_lvl</th>
                  <th>offset</th>
                  <th>base_price</th>
                  <th>raw_price</th>
                  <th>raw_qty</th>
                  <th>aligned_price</th>
                  <th>aligned_qty</th>
                  <th>aligned_notional_u</th>
                  <th>price_count</th>
                  <th>qty_count</th>
                </tr>
              </thead>
              <tbody id="splitRows">
                <tr><td colspan="11" class="muted" style="text-align:center;">No data</td></tr>
              </tbody>
              <tfoot id="splitSummaryRows"></tfoot>
            </table>
          </div>
        </section>

        <section class="panel">
          <h3 style="font-size:15px;">Last Response</h3>
          <textarea id="log" rows="14" readonly></textarea>
        </section>

        <section class="panel">
          <div class="row-between">
            <h3 style="font-size:15px;">MMHedge QV/TLen</h3>
            <button id="refreshHedgeTlen">Refresh</button>
          </div>
          <div class="muted" id="hedgeTlenMeta">No MMHedge query yet</div>
          <div class="muted" style="margin-top:8px;">MMHedge from_key</div>
          <textarea id="hedgeFromKey" rows="4" readonly style="margin-top:6px;"></textarea>
          <div class="table-wrap" style="margin-top:8px; max-height:260px;">
            <table>
              <thead>
                <tr>
                  <th>#</th>
                  <th>price</th>
                  <th>qty</th>
                  <th>offset</th>
                  <th>price_tick_i64</th>
                  <th>price_tick_exp</th>
                  <th>price_count</th>
                  <th>tlen</th>
                </tr>
              </thead>
              <tbody id="hedgeTlenRows">
                <tr><td colspan="8" class="muted" style="text-align:center;">No data</td></tr>
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </div>

    <script>
      const state = {
        cfg: null,
        symbols: [],
        selected: '',
      };

      const els = {
        cfgLine: document.getElementById('cfgLine'),
        cfgKv: document.getElementById('cfgKv'),
        q: document.getElementById('q'),
        symbol: document.getElementById('symbol'),
        list: document.getElementById('list'),
        quote: document.getElementById('quote'),
        fromKey: document.getElementById('fromKey'),
        statusPill: document.getElementById('statusPill'),
        actionHint: document.getElementById('actionHint'),
        splitMeta: document.getElementById('splitMeta'),
        openFromKeys: document.getElementById('openFromKeys'),
        splitRows: document.getElementById('splitRows'),
        splitSummaryRows: document.getElementById('splitSummaryRows'),
        hedgeTlenMeta: document.getElementById('hedgeTlenMeta'),
        hedgeFromKey: document.getElementById('hedgeFromKey'),
        hedgeTlenRows: document.getElementById('hedgeTlenRows'),
        log: document.getElementById('log'),
        refreshQuote: document.getElementById('refreshQuote'),
        refreshHedgeTlen: document.getElementById('refreshHedgeTlen'),
        send: document.getElementById('send'),
      };

      function fmtNum(v, d=8) {
        if (typeof v !== 'number' || !Number.isFinite(v)) return '-';
        return v.toFixed(d);
      }

      function setStatus(label, isErr = false) {
        els.statusPill.textContent = label;
        els.statusPill.className = `pill ${isErr ? 'err' : 'ok'}`;
      }

      function setLog(obj) {
        els.log.value = JSON.stringify(obj, null, 2);
      }

      function updateButtons() {
        const symbol = els.symbol.value.trim().toUpperCase();
        const enabled = !!symbol;
        els.send.disabled = !enabled;
        els.actionHint.textContent = enabled
          ? '点击一键发号（默认 from_key 包含 ret_score/ret_thr/vol/env_score/env_thr，缺失时为 0）'
          : '请选择 symbol 后点击一键发号（默认 from_key 包含 ret_score/ret_thr/vol/env_score/env_thr，缺失时为 0）';
      }

      function renderCfg(cfg) {
        state.cfg = cfg;
        els.cfgLine.textContent = `model_service:${cfg.model_service} | venue=${cfg.venue}`;
        els.fromKey.value = '';
        els.fromKey.placeholder = cfg.from_key || 'optional';

        const kv = [
          ['venue', cfg.venue],
          ['bind', `${cfg.bind}:${cfg.port}`],
          ['model_service', cfg.model_service || ''],
          ['target_factor', cfg.target_factor || 'rl_return_volatility'],
          ['order_amount_u', cfg.order_amount_u],
          ['open_orders_per_round', cfg.open_orders_per_round],
          ['open_buy_vol_scale', JSON.stringify(cfg.open_buy_vol_scale)],
          ['open_sell_vol_scale', JSON.stringify(cfg.open_sell_vol_scale)],
          ['hedge_orders_per_round', cfg.hedge_orders_per_round],
          ['hedge_vol_multiplier', cfg.hedge_vol_multiplier],
          ['hedge_offset_ratio', cfg.hedge_offset_ratio],
          ['enable_return_score_adjust_hedge', cfg.enable_return_score_adjust_hedge],
          ['hedge_price_offset_limit_upper', cfg.hedge_price_offset_limit_upper],
          ['hedge_price_offset_limit_lower', cfg.hedge_price_offset_limit_lower],
          ['default_open_ttl_ms', cfg.default_open_ttl_ms],
          ['next_query_delay_ms', cfg.next_query_delay_ms],
          ['default_from_key', cfg.from_key || ''],
        ];
        els.cfgKv.innerHTML = kv
          .map(([k, v]) => `<div class="k">${k}</div><div>${v}</div>`)
          .join('');
      }

      function renderList() {
        const q = els.q.value.trim().toUpperCase();
        const items = state.symbols.filter((s) => !q || s.includes(q)).slice(0, 600);
        els.list.innerHTML = '';
        for (const s of items) {
          const li = document.createElement('li');
          li.textContent = s;
          li.onclick = async () => {
            els.symbol.value = s;
            state.selected = s;
            updateButtons();
            await refreshQuote();
          };
          els.list.appendChild(li);
        }
      }

      function renderSplitResult(resp) {
        const rows = resp.levels || [];
        if (!rows.length) {
          els.splitRows.innerHTML = '<tr><td colspan="11" class="muted" style="text-align:center;">No levels</td></tr>';
          els.splitSummaryRows.innerHTML = '';
          els.openFromKeys.value = resp.from_key || '';
          els.splitMeta.textContent = `symbol=${resp.symbol || '-'} no levels`;
          return;
        }

        const levelFromKeyText = (rows || []).map((row, idx) =>
          `#${idx} side=${row.side} lvl=${row.side_level_index} tlen=${row.tlen == null ? 'NA' : fmtNum(row.tlen, 8)}\n${row.from_key || ''}`
        ).join('\n\n');
        els.openFromKeys.value =
          `base_from_key:\n${resp.from_key || ''}\n\nlevel_from_keys:\n${levelFromKeyText}`;

        const buyNotional = rows.reduce((sum, row) => {
          if (row.side !== 'buy') return sum;
          const notional = Number(row.aligned_price) * Number(row.aligned_qty);
          return Number.isFinite(notional) ? sum + notional : sum;
        }, 0);
        const sellNotional = rows.reduce((sum, row) => {
          if (row.side !== 'sell') return sum;
          const notional = Number(row.aligned_price) * Number(row.aligned_qty);
          return Number.isFinite(notional) ? sum + notional : sum;
        }, 0);
        const totalAlignedNotional = buyNotional + sellNotional;

        els.splitRows.innerHTML = rows.map((row) => {
          const sideCls = row.side === 'buy' ? 'buy' : 'sell';
          const alignedNotional = Number(row.aligned_price) * Number(row.aligned_qty);
          return `
            <tr>
              <td class="${sideCls}">${row.side}</td>
              <td>${row.side_level_index}</td>
              <td>${fmtNum(row.offset, 6)}</td>
              <td>${fmtNum(row.base_price, 6)}</td>
              <td>${fmtNum(row.raw_price, 6)}</td>
              <td>${fmtNum(row.raw_qty, 8)}</td>
              <td>${fmtNum(row.aligned_price, 6)}</td>
              <td>${fmtNum(row.aligned_qty, 8)}</td>
              <td>${fmtNum(alignedNotional, 4)}</td>
              <td>${row.price_tick_count}</td>
              <td>${row.qty_tick_count}</td>
            </tr>
          `;
        }).join('');

        els.splitSummaryRows.innerHTML = `
          <tr>
            <td class="buy">buy_subtotal</td>
            <td colspan="7">-</td>
            <td>${fmtNum(buyNotional, 4)}</td>
            <td colspan="2">-</td>
          </tr>
          <tr>
            <td class="sell">sell_subtotal</td>
            <td colspan="7">-</td>
            <td>${fmtNum(sellNotional, 4)}</td>
            <td colspan="2">-</td>
          </tr>
          <tr>
            <td>total</td>
            <td colspan="7">-</td>
            <td>${fmtNum(totalAlignedNotional, 4)}</td>
            <td colspan="2">-</td>
          </tr>
        `;

        els.splitMeta.textContent =
          `symbol=${resp.symbol} buy=${resp.buy_orders} sell=${resp.sell_orders} total_notional_u=${fmtNum(totalAlignedNotional, 4)} mid=${fmtNum(resp.mid_price, 6)} vol=${fmtNum(resp.volatility, 6)} band=[${fmtNum(resp.band_lower, 6)},${fmtNum(resp.band_upper, 6)}] factor_ready=${resp.factor_ready} quote_ts=${resp.quote_ts}`;
      }

      function renderHedgeTlen(snapshot) {
        if (!snapshot) {
          els.hedgeTlenMeta.textContent = 'No MMHedge query yet';
          els.hedgeFromKey.value = '';
          els.hedgeTlenRows.innerHTML = '<tr><td colspan="7" class="muted" style="text-align:center;">No data</td></tr>';
          return;
        }

        const rows = snapshot.rows || [];
        const errText = snapshot.query_err ? ` err=${snapshot.query_err}` : '';
        els.hedgeTlenMeta.textContent =
          `symbol=${snapshot.symbol} levels=${snapshot.levels} signal_ts=${snapshot.signal_ts} next_query_ts=${snapshot.next_query_ts} query_ok=${snapshot.query_ok}${errText}`;
        els.hedgeFromKey.value = snapshot.from_key || '';

        if (!rows.length) {
          els.hedgeTlenRows.innerHTML = '<tr><td colspan="8" class="muted" style="text-align:center;">No levels</td></tr>';
          return;
        }

        els.hedgeTlenRows.innerHTML = rows.map((row) => {
          const tlenText = row.tlen == null ? 'NA' : fmtNum(row.tlen, 8);
          return `
            <tr>
              <td>${row.index}</td>
              <td>${fmtNum(row.price, 8)}</td>
              <td>${fmtNum(row.amount, 8)}</td>
              <td>${fmtNum(row.offset, 8)}</td>
              <td>${row.price_tick_i64}</td>
              <td>${row.price_tick_exp}</td>
              <td>${row.price_count}</td>
              <td>${tlenText}</td>
            </tr>
          `;
        }).join('');
      }

      async function refreshHedgeTlen() {
        const resp = await fetch('/api/mm_hedge_tlen');
        if (!resp.ok) {
          setLog({ error: `hedge_tlen fetch failed: ${await resp.text()}` });
          return;
        }
        renderHedgeTlen(await resp.json());
      }

      async function loadCfg() {
        const resp = await fetch('/api/config');
        if (!resp.ok) {
          throw new Error(await resp.text());
        }
        renderCfg(await resp.json());
      }

      async function loadSymbols() {
        const resp = await fetch('/api/symbols');
        if (!resp.ok) {
          throw new Error(await resp.text());
        }
        const data = await resp.json();
        state.symbols = (data.symbols || []).slice().sort();
        renderList();
      }

      async function refreshQuote() {
        const symbol = els.symbol.value.trim().toUpperCase();
        if (!symbol) {
          els.quote.textContent = 'N/A';
          return;
        }

        const resp = await fetch(`/api/quote?symbol=${encodeURIComponent(symbol)}`);
        if (!resp.ok) {
          els.quote.textContent = 'quote unavailable';
          setStatus('QUOTE_MISSING', true);
          return;
        }

        const data = await resp.json();
        els.quote.textContent = `${data.venue} bid=${fmtNum(data.bid, 6)} ask=${fmtNum(data.ask, 6)} ts=${data.ts}`;
        setStatus('READY');
      }

      function buildPayload() {
        const payload = { symbol: els.symbol.value.trim().toUpperCase() };
        const fromKey = els.fromKey.value.trim();
        if (fromKey) payload.from_key = fromKey;
        return payload;
      }

      async function sendOpen() {
        const payload = buildPayload();
        const resp = await fetch('/api/send', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });

        const text = await resp.text();
        if (!resp.ok) {
          setStatus('SEND_ERR', true);
          setLog({ error: text });
          return;
        }

        const data = JSON.parse(text);
        renderSplitResult(data);
        setStatus('SEND_OK');
        setLog(data);
      }

      els.q.oninput = renderList;
      els.symbol.oninput = () => {
        updateButtons();
      };
      els.symbol.onchange = async () => {
        await refreshQuote();
      };
      els.refreshQuote.onclick = refreshQuote;
      els.refreshHedgeTlen.onclick = refreshHedgeTlen;
      els.send.onclick = sendOpen;

      (async () => {
        try {
          await loadCfg();
          await loadSymbols();
          await refreshHedgeTlen();
          updateButtons();
          setStatus('READY');
        } catch (err) {
          setStatus('INIT_ERR', true);
          setLog({ error: String(err) });
        }
      })();
    </script>
  </body>
</html>
"#;

async fn run(cfg: AppCfg, token: CancellationToken) -> Result<()> {
    let mut table = VenueMinQtyTable::new(cfg.venue);
    table.refresh().await?;
    let table = Arc::new(RwLock::new(table));

    let quotes = Arc::new(RwLock::new(QuoteCache::default()));
    let model_scores = Arc::new(RwLock::new(ModelScoreCache::default()));
    let mm_hedge_tlen = Arc::new(RwLock::new(None));
    spawn_ask_bid_listener(cfg.venue, quotes.clone(), token.clone());
    if cfg.model_service.trim() != "-" {
        spawn_model_listener(
            cfg.model_service.clone(),
            model_scores.clone(),
            token.clone(),
        );
    } else {
        info!("manual_mm: model_service disabled ('-'), symbol list only from manual input");
    }

    let (publish_tx, publish_rx) = mpsc::unbounded_channel();
    spawn_publisher_worker(cfg.clone(), table.clone(), publish_rx, token.clone());
    spawn_backward_query_responder(
        cfg.clone(),
        quotes.clone(),
        table.clone(),
        mm_hedge_tlen.clone(),
        publish_tx.clone(),
        token.clone(),
    );

    let state = AppState {
        cfg: cfg.clone(),
        quotes,
        model_scores,
        mm_hedge_tlen,
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
    let cfg = load_config(&args.config).await?;
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
