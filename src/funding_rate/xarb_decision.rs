//! Trade signal decision module (xarb cross-venue).
//!
//! Compared with FR decision, xarb decision uses spread as the first trigger,
//! then applies funding / return-score / environment / volatility gating for Open decisions.
//! It only emits Open/Cancel and supports backward hedge queries.

use anyhow::{Context, Result};
use bytes::Bytes;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use std::cell::{OnceCell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use super::common::{
    build_decision_from_key_base, compute_spread_rate, Quote, ReturnScoreThresholdsResolved,
    ThresholdKey, VenuePair,
};
use super::factor_value_hub::{
    EnvironmentSignalResult, EnvironmentSignalSource, FactorValueHub, FactorValueLookupResult,
    ModelOutputScoreLookupResult,
};
use super::mkt_channel::MktChannel;
use super::spread_factor::SpreadFactor;
use super::symbol_list::SymbolList;
use super::xarb_funding_threshold_loader::XarbFundingThresholdsResolved;
use crate::common::bbo::Bbo;
use crate::common::iceoryx_publisher::{SignalPublisher, SIGNAL_PAYLOAD};
use crate::common::iceoryx_subscriber::GenericSignalSubscriber;
use crate::common::ipc_service_name::build_service_name;
use crate::common::redis_client::RedisSettings;
use crate::common::symbol_util::normalize_symbol_for_venue;
use crate::common::time_util::get_timestamp_us;
use crate::depth_pub::query_client::DepthQueryClient;
use crate::depth_pub::query_msg::price_to_tick_index;
use crate::market_maker::quote_plan_levels::{
    build_quote_plan_levels, QuotePlanLevel, QuotePlanLevelSpec,
};
use crate::pre_trade::order_manager::{OrderType, Side};
use crate::signal::cancel_signal::ArbCancelCtx;
use crate::signal::common::{
    align_price_ceil, align_price_floor, SignalBytes, TradingLeg, TradingVenue,
};
use crate::signal::hedge_signal::{ArbHedgeCtx, ArbHedgeSignalQueryMsg};
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::signal::venue_min_qty_table::VenueMinQtyTable;
use crate::symbol_match::normalize_symbol_for_whitelist;

use super::fr_decision::{DEFAULT_BACKWARD_CHANNEL, DEFAULT_SIGNAL_CHANNEL};

const DEFAULT_PNLU_REDIS_HOST: &str = "127.0.0.1";
const DEFAULT_PNLU_REDIS_PORT: u16 = 6379;
const DEFAULT_PNLU_REDIS_DB: i64 = 0;
const DEFAULT_PNLU_KEY_SUFFIX: &str = "_pnlu_factor_thresholds";
const PNLU_MAX_AGE_SECS: i64 = 30 * 60;
const TARGET_FACTOR_NAME: &str = "rl_return_volatility";
const TARGET_FACTOR_KEY_PREFIX: &str = TARGET_FACTOR_NAME;
const ENV_MODEL_TRUE_THRESHOLD_DEFAULT: f64 = 0.0;
const PANIC_ON_FIRST_OPEN_DRY_RUN_ENV: &str = "TRADE_SIGNAL_PANIC_ON_FIRST_OPEN_DRY_RUN";
const INTERCEPT_SUMMARY_INTERVAL_SECS: u64 = 30;

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

#[derive(Debug, Clone)]
struct XarbOpenQuotePlan {
    side: Side,
    inner_price: f64,
    outer_price: f64,
    price_tick: f64,
    qty_tick: f64,
    levels: Vec<QuotePlanLevel>,
}

fn build_xarb_level_specs(
    side: Side,
    inner_price: f64,
    volatility: f64,
    level_count: usize,
) -> Vec<QuotePlanLevelSpec> {
    if level_count == 0 || !inner_price.is_finite() || inner_price <= 0.0 {
        return Vec::new();
    }
    if !volatility.is_finite() || volatility < 0.0 {
        return Vec::new();
    }
    if level_count == 1 || volatility <= 0.0 {
        return vec![QuotePlanLevelSpec {
            side,
            side_level_index: 1,
            offset: 0.0,
            base_price: inner_price,
        }];
    }

    let step = volatility / (level_count - 1) as f64;
    (0..level_count)
        .map(|idx| QuotePlanLevelSpec {
            side,
            side_level_index: idx + 1,
            offset: step * idx as f64,
            base_price: inner_price,
        })
        .collect()
}

fn build_xarb_open_quote_plan(
    venue: TradingVenue,
    symbol: &str,
    quote: Quote,
    order_amount_u: f64,
    orders_per_round: u32,
    side: Side,
    volatility: f64,
    table: &VenueMinQtyTable,
) -> Result<XarbOpenQuotePlan, String> {
    if symbol.trim().is_empty() {
        return Err("symbol is empty".to_string());
    }
    if quote.bid <= 0.0 || quote.ask <= 0.0 || quote.bid >= quote.ask {
        return Err(format!(
            "invalid quote bid={} ask={} symbol={}",
            quote.bid, quote.ask, symbol
        ));
    }
    if !(order_amount_u.is_finite() && order_amount_u > 0.0) {
        return Err(format!(
            "invalid order_amount_u={} (must be finite and >0)",
            order_amount_u
        ));
    }
    if orders_per_round == 0 {
        return Err("orders_per_round must be > 0".to_string());
    }
    if !volatility.is_finite() || volatility < 0.0 {
        return Err(format!(
            "invalid volatility symbol={} volatility={}",
            symbol, volatility
        ));
    }

    let inner_price = match side {
        Side::Buy => quote.bid,
        Side::Sell => quote.ask,
    };
    let outer_price = match side {
        Side::Buy => inner_price * (1.0 - volatility),
        Side::Sell => inner_price * (1.0 + volatility),
    };
    let specs = build_xarb_level_specs(side, inner_price, volatility, orders_per_round as usize);
    if specs.is_empty() {
        return Err(format!(
            "empty xarb level specs symbol={} side={:?} inner={:.8} volatility={:.8} levels={}",
            symbol, side, inner_price, volatility, orders_per_round
        ));
    }

    let (price_tick, qty_tick, levels) =
        build_quote_plan_levels(venue, symbol, order_amount_u, &specs, table)?;
    if levels.is_empty() {
        return Err(format!(
            "empty levels after alignment symbol={} side={:?} levels={}",
            symbol, side, orders_per_round
        ));
    }

    Ok(XarbOpenQuotePlan {
        side,
        inner_price,
        outer_price,
        price_tick,
        qty_tick,
        levels,
    })
}

fn min_qty_symbol_key(venue: TradingVenue, trade_symbol: &str) -> String {
    match venue {
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => trade_symbol
            .to_uppercase()
            .replace("-SWAP", "")
            .replace('-', ""),
        TradingVenue::GateMargin | TradingVenue::GateFutures => trade_symbol
            .to_uppercase()
            .replace('_', "")
            .replace('-', ""),
        _ => trade_symbol.to_uppercase(),
    }
}

fn venue_qty_is_contracts(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BinanceFutures | TradingVenue::OkexFutures | TradingVenue::GateFutures
    )
}

fn contract_qty_multiplier(
    table: &VenueMinQtyTable,
    venue: TradingVenue,
    symbol_key: &str,
) -> Option<f64> {
    match venue {
        TradingVenue::BinanceFutures => Some(1.0),
        TradingVenue::OkexFutures | TradingVenue::GateFutures => table
            .contract_multiplier_opt(symbol_key)
            .filter(|v| v.is_finite() && *v > 0.0),
        _ => Some(1.0),
    }
}

fn base_step_size(
    table: &VenueMinQtyTable,
    venue: TradingVenue,
    trade_symbol: &str,
) -> (Option<f64>, Option<f64>) {
    let symbol_key = min_qty_symbol_key(venue, trade_symbol);
    let step = table.step_size(&symbol_key);
    let min_qty = table.min_qty(&symbol_key);

    if venue_qty_is_contracts(venue) {
        let mult = contract_qty_multiplier(table, venue, &symbol_key);
        let step_base = step.zip(mult).map(|(s, m)| s * m);
        let min_qty_base = min_qty.zip(mult).map(|(q, m)| q * m);
        (step_base, min_qty_base)
    } else {
        (step, min_qty)
    }
}

thread_local! {
    static XARB_DECISION: OnceCell<RefCell<XarbDecision>> = OnceCell::new();
}

pub struct XarbDecision {
    signal_pub: SignalPublisher,
    backward_sub: GenericSignalSubscriber,
    factor_value_hub: FactorValueHub,
    channel_name: String,
    node: Node<ipc::Service>,

    open_orders_per_round: u32,

    open_min_qty_table: VenueMinQtyTable,
    hedge_min_qty_table: VenueMinQtyTable,
    venues: VenuePair,
    open_depth_query_client: DepthQueryClient,
    hedge_depth_query_client: DepthQueryClient,

    order_amount: f32,
    open_scale: f64,
    open_order_ttl_us: i64,
    hedge_timeout_mm_us: i64,
    hedge_price_offset: f64,
    hedge_aggressive_seq_threshold: u32,
    max_hedge_price_pct_change: f64, // percent, (0, 99]
    enable_environment_model: bool,
    enable_volatility_limit: bool,
    open_volatility_limit: f64,
    enable_return_score_model: bool,
    return_model_service: Option<String>,
    environment_model_service: Option<String>,
    environment_model_true_threshold: f64,
    return_score_thresholds: HashMap<String, ReturnScoreThresholdsResolved>,
    funding_open_thresholds: HashMap<String, XarbFundingThresholdsResolved>,
    open_volatility_thresholds: HashMap<String, f64>,

    signal_cooldown_us: i64,
    last_open_ts: Rc<RefCell<HashMap<ThresholdKey, i64>>>,
    last_close_ts: Rc<RefCell<HashMap<ThresholdKey, i64>>>,
    last_cancel_ts: Rc<RefCell<HashMap<ThresholdKey, i64>>>,
    intercept_summary: InterceptSummary,
}

struct InterceptSummary {
    window_start: Instant,
    counts: HashMap<String, u64>,
}

impl InterceptSummary {
    fn new() -> Self {
        Self {
            window_start: Instant::now(),
            counts: HashMap::new(),
        }
    }

    fn record(&mut self, reason: impl Into<String>) {
        *self.counts.entry(reason.into()).or_insert(0) += 1;
    }

    fn flush_if_due(&mut self) {
        if self.window_start.elapsed() < Duration::from_secs(INTERCEPT_SUMMARY_INTERVAL_SECS)
            || self.counts.is_empty()
        {
            return;
        }

        let mut items = self.counts.iter().collect::<Vec<_>>();
        items.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
        let total: u64 = items.iter().map(|(_, count)| **count).sum();
        let summary = items
            .into_iter()
            .map(|(reason, count)| format!("{reason}={count}"))
            .collect::<Vec<_>>()
            .join(" ");

        info!(
            "XarbDecision: intercept_summary window={}s total={} {}",
            INTERCEPT_SUMMARY_INTERVAL_SECS, total, summary
        );
        self.counts.clear();
        self.window_start = Instant::now();
    }
}

fn compact_environment_direction(forward_open: bool, backward_open: bool) -> &'static str {
    match (forward_open, backward_open) {
        (true, true) => "2",
        (true, false) => "f",
        (false, true) => "b",
        (false, false) => "0",
    }
}

fn compact_environment_source(source: EnvironmentSignalSource) -> &'static str {
    match source {
        EnvironmentSignalSource::ModelOutput => "mo",
        EnvironmentSignalSource::PnluFallback => "pf",
    }
}

fn compact_environment_note(note: &str) -> String {
    let model_note = match note {
        "model_score_ge_threshold" => Some("score_ge"),
        "model_score_lt_threshold" => Some("score_lt"),
        "invalid_model_score" => Some("score_nan"),
        "missing_model_score" => Some("score_miss"),
        _ => None,
    };
    if let Some(label) = model_note {
        return label.to_string();
    }

    let Some(rest) = note.strip_prefix("pnlu_fallback:") else {
        return note.to_string();
    };
    let pnlu_note = match rest {
        "missing_key" => "miss_key",
        "missing_ts" => "miss_ts",
        "stale_ts" => "stale",
        "missing_factor_or_threshold" => "miss_fac_thr",
        "factor_not_gt_threshold" => "fac_le_thr",
        "ts_in_future" => "future_ts",
        _ if rest.starts_with("redis_error:") => "redis_err",
        _ if rest.starts_with("invalid_json:") => "json_err",
        _ => rest,
    };
    format!("pf_{pnlu_note}")
}

fn compact_environment_intercept_reason(
    forward_open: bool,
    backward_open: bool,
    cooldown_hit: bool,
    result: &EnvironmentSignalResult,
) -> String {
    let direction = compact_environment_direction(forward_open, backward_open);
    let source = compact_environment_source(result.source);
    let note = compact_environment_note(&result.note);
    let cooldown_flag = if cooldown_hit { 1 } else { 0 };
    format!("env_blk:d={direction}:src={source}:n={note}:cd={cooldown_flag}")
}

impl XarbDecision {
    fn normalize_symbol_key(symbol: &str) -> String {
        normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures)
    }

    fn poll_model_output_updates(&mut self) {
        self.factor_value_hub.poll_model_output_updates();
    }

    fn lookup_target_factor_value(
        &mut self,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
    ) -> FactorValueLookupResult {
        self.factor_value_hub
            .lookup_target_factor_value(hedge_symbol, hedge_venue)
    }

    fn lookup_return_model_score_lookup(
        &mut self,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
    ) -> Option<ModelOutputScoreLookupResult> {
        let Some(service_name) = self.return_model_service.clone() else {
            return None;
        };
        Some(self.factor_value_hub.lookup_model_output_score(
            &service_name,
            hedge_symbol,
            hedge_venue,
        ))
    }

    fn lookup_return_score_thresholds(
        &self,
        model_symbol_key: &str,
    ) -> Option<ReturnScoreThresholdsResolved> {
        self.return_score_thresholds
            .get(&model_symbol_key.to_ascii_uppercase())
            .copied()
    }

    fn lookup_funding_open_thresholds(
        &self,
        symbol_key: &str,
    ) -> Option<XarbFundingThresholdsResolved> {
        self.funding_open_thresholds
            .get(&symbol_key.to_ascii_uppercase())
            .copied()
    }

    fn lookup_open_volatility_threshold(&self, symbol_key: &str) -> Option<f64> {
        self.open_volatility_thresholds
            .get(&symbol_key.to_ascii_uppercase())
            .copied()
    }

    fn record_intercept_summary(&mut self, reason: impl Into<String>) {
        self.intercept_summary.record(reason);
        self.intercept_summary.flush_if_due();
    }

    fn select_open_return_threshold(
        &self,
        side: Side,
        thresholds: ReturnScoreThresholdsResolved,
    ) -> f64 {
        match side {
            Side::Buy => thresholds.forward_open,
            Side::Sell => thresholds.backward_open,
        }
    }

    fn select_open_return_threshold_by_hedge_side(
        &self,
        hedge_side: Side,
        thresholds: ReturnScoreThresholdsResolved,
    ) -> f64 {
        match hedge_side {
            Side::Sell => thresholds.forward_open,
            Side::Buy => thresholds.backward_open,
        }
    }

    fn select_open_filter_threshold(
        &self,
        side: Side,
        thresholds: XarbFundingThresholdsResolved,
    ) -> f64 {
        match side {
            Side::Buy => thresholds.forward_open,
            Side::Sell => thresholds.backward_open,
        }
    }

    fn lookup_realtime_open_filter_value(
        &self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Option<(f64, &'static str)> {
        let mkt_channel = MktChannel::instance();

        if open_venue.is_futures() && hedge_venue.is_futures() {
            let open_fr = mkt_channel.get_latest_funding_rate(open_symbol, open_venue)?;
            let hedge_fr = mkt_channel.get_latest_funding_rate(hedge_symbol, hedge_venue)?;
            return Some((hedge_fr - open_fr, "spread_fr"));
        }

        if hedge_venue.is_futures() {
            return mkt_channel
                .get_premium_rate(hedge_symbol, hedge_venue)
                .map(|value| (value, "hedge_premium_rate"));
        }

        None
    }

    fn evaluate_environment_signal(
        &mut self,
        open_symbol_key: &str,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
        now_us: i64,
    ) -> EnvironmentSignalResult {
        self.factor_value_hub.evaluate_environment_signal(
            self.environment_model_service.as_deref(),
            hedge_symbol,
            hedge_venue,
            self.environment_model_true_threshold,
            open_symbol_key,
            now_us,
        )
    }

    pub fn is_initialized() -> bool {
        XARB_DECISION.with(|cell| cell.get().is_some())
    }

    pub fn with<F, R>(f: F) -> R
    where
        F: FnOnce(&XarbDecision) -> R,
    {
        XARB_DECISION.with(|cell| {
            let decision_ref = cell
                .get()
                .expect("XarbDecision not initialized. Call init_singleton() first");
            f(&decision_ref.borrow())
        })
    }

    pub fn with_mut<F, R>(f: F) -> R
    where
        F: FnOnce(&mut XarbDecision) -> R,
    {
        XARB_DECISION.with(|cell| {
            let decision_ref = cell
                .get()
                .expect("XarbDecision not initialized. Call init_singleton() first");
            f(&mut decision_ref.borrow_mut())
        })
    }

    pub fn try_with_mut<F, R>(f: F) -> Option<R>
    where
        F: FnOnce(&mut XarbDecision) -> R,
    {
        XARB_DECISION.with(|cell| {
            let decision_ref = cell.get()?;
            Some(f(&mut decision_ref.borrow_mut()))
        })
    }

    pub async fn init_singleton(open_venue: TradingVenue, hedge_venue: TradingVenue) -> Result<()> {
        let venues = (open_venue, hedge_venue);
        let result: Result<()> = XARB_DECISION.with(|cell| {
            if cell.get().is_some() {
                return Ok(());
            }
            let decision = Self::new_sync(venues)?;
            cell.set(RefCell::new(decision))
                .map_err(|_| anyhow::anyhow!("Failed to initialize XarbDecision singleton"))?;
            info!(
                "XarbDecision singleton initialized, open={:?} hedge={:?}",
                open_venue, hedge_venue
            );
            Ok(())
        });
        result?;

        Self::refresh_min_qty_async(venues).await;
        Self::spawn_backward_listener();
        info!("XarbDecision backward listener started");

        Ok(())
    }

    fn new_sync(venues: VenuePair) -> Result<Self> {
        let node_name = NodeName::new("xarb_decision")?;
        let node = NodeBuilder::new()
            .name(&node_name)
            .create::<ipc::Service>()?;

        let signal_pub = SignalPublisher::new(DEFAULT_SIGNAL_CHANNEL)?;
        let backward_sub = Self::create_subscriber(&node, DEFAULT_BACKWARD_CHANNEL)?;
        let pnlu_settings = RedisSettings {
            host: DEFAULT_PNLU_REDIS_HOST.to_string(),
            port: DEFAULT_PNLU_REDIS_PORT,
            db: DEFAULT_PNLU_REDIS_DB,
            username: None,
            password: None,
            prefix: None,
        };
        let pnlu_key_suffix = DEFAULT_PNLU_KEY_SUFFIX.to_string();
        let factor_value_hub = FactorValueHub::new(
            &node,
            venues.0,
            venues.1,
            TARGET_FACTOR_NAME,
            TARGET_FACTOR_KEY_PREFIX,
            pnlu_settings.clone(),
            pnlu_key_suffix.clone(),
            PNLU_MAX_AGE_SECS,
        )?;
        let pnlu_profile = format!("{}-{}", venues.0.data_pub_slug(), venues.1.data_pub_slug());

        let open_min_qty_table = VenueMinQtyTable::new(venues.0);
        let hedge_min_qty_table = VenueMinQtyTable::new(venues.1);
        let open_depth_query_client = DepthQueryClient::new(venues.0)?;
        let hedge_depth_query_client = DepthQueryClient::new(venues.1)?;

        info!(
            "XarbDecision: pnlu redis configured host={} port={} db={} key_pattern=<symbol>{}_{}",
            pnlu_settings.host, pnlu_settings.port, pnlu_settings.db, pnlu_key_suffix, pnlu_profile
        );

        Ok(Self {
            signal_pub,
            backward_sub,
            factor_value_hub,
            channel_name: DEFAULT_SIGNAL_CHANNEL.to_string(),
            node,
            open_orders_per_round: 5,
            open_min_qty_table,
            hedge_min_qty_table,
            venues,
            open_depth_query_client,
            hedge_depth_query_client,
            order_amount: 100.0,
            open_scale: 1.0,
            open_order_ttl_us: 120_000_000,
            hedge_timeout_mm_us: 30_000_000,
            hedge_price_offset: 0.0003,
            hedge_aggressive_seq_threshold: 6,
            max_hedge_price_pct_change: 5.0,
            enable_environment_model: true,
            enable_volatility_limit: true,
            open_volatility_limit: 70.0,
            enable_return_score_model: false,
            return_model_service: None,
            environment_model_service: None,
            environment_model_true_threshold: ENV_MODEL_TRUE_THRESHOLD_DEFAULT,
            return_score_thresholds: HashMap::new(),
            funding_open_thresholds: HashMap::new(),
            open_volatility_thresholds: HashMap::new(),
            signal_cooldown_us: 5_000_000,
            last_open_ts: Rc::new(RefCell::new(HashMap::new())),
            last_close_ts: Rc::new(RefCell::new(HashMap::new())),
            last_cancel_ts: Rc::new(RefCell::new(HashMap::new())),
            intercept_summary: InterceptSummary::new(),
        })
    }

    async fn refresh_min_qty_async(venues: VenuePair) {
        let mut open_table = VenueMinQtyTable::new(venues.0);
        let mut hedge_table = VenueMinQtyTable::new(venues.1);

        let open_res = open_table.refresh().await;
        let hedge_res = hedge_table.refresh().await;

        Self::with_mut(|decision| {
            if open_res.is_ok() {
                decision.open_min_qty_table = open_table;
            }
            if hedge_res.is_ok() {
                decision.hedge_min_qty_table = hedge_table;
            }
        });

        match open_res {
            Ok(_) => info!(
                "XarbDecision: open venue min_qty_table loaded, venue={:?}",
                venues.0
            ),
            Err(err) => warn!(
                "XarbDecision: failed to refresh open venue filters for {:?}, price_tick may be zero: {err:#}",
                venues.0
            ),
        }
        match hedge_res {
            Ok(_) => info!(
                "XarbDecision: hedge venue min_qty_table loaded, venue={:?}",
                venues.1
            ),
            Err(err) => warn!(
                "XarbDecision: failed to refresh hedge venue filters for {:?}, price_tick may be zero: {err:#}",
                venues.1
            ),
        }
    }

    fn create_subscriber(
        node: &Node<ipc::Service>,
        channel_name: &str,
    ) -> Result<GenericSignalSubscriber> {
        let service_name = build_service_name(&format!("signal_pubs/{}", channel_name));
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; SIGNAL_PAYLOAD]>()
            .max_publishers(1)
            .max_subscribers(32)
            .history_size(128)
            .subscriber_max_buffer_size(256)
            .open_or_create()
            .with_context(|| {
                format!("failed to open/create backward signal service={service_name}")
            })?;

        let subscriber = service.subscriber_builder().create()?;
        Ok(GenericSignalSubscriber::Size4K(subscriber))
    }

    pub fn update_model_output_services(&mut self, services: Vec<String>) {
        self.factor_value_hub
            .update_model_output_services(&self.node, services);
    }

    pub fn update_model_service_roles(
        &mut self,
        return_model_service: String,
        environment_model_service: String,
    ) {
        let return_trimmed = return_model_service.trim();
        self.return_model_service = if return_trimmed.is_empty() || return_trimmed == "-" {
            None
        } else {
            Some(return_trimmed.to_string())
        };
        let env_trimmed = environment_model_service.trim();
        self.environment_model_service = if env_trimmed.is_empty() || env_trimmed == "-" {
            None
        } else {
            Some(env_trimmed.to_string())
        };
        info!(
            "XarbDecision: model roles updated enable_return_score_model={} return={:?} environment={:?} env_true_threshold={:.6}",
            self.enable_return_score_model,
            self.return_model_service,
            self.environment_model_service,
            self.environment_model_true_threshold
        );
    }

    pub fn update_enable_return_score_model(&mut self, enabled: bool) {
        self.enable_return_score_model = enabled;
        info!(
            "XarbDecision: enable_return_score_model updated enabled={}",
            self.enable_return_score_model
        );
    }

    pub fn update_enable_environment_model(&mut self, enabled: bool) {
        self.enable_environment_model = enabled;
        info!(
            "XarbDecision: enable_environment_model updated enabled={}",
            self.enable_environment_model
        );
    }

    pub fn update_enable_volatility_limit(&mut self, enabled: bool) {
        self.enable_volatility_limit = enabled;
        info!(
            "XarbDecision: enable_volatility_limit updated enabled={}",
            self.enable_volatility_limit
        );
    }

    pub fn update_open_volatility_limit(&mut self, percentile: f64) {
        if !(percentile.is_finite() && percentile >= 0.0 && percentile <= 100.0) {
            warn!(
                "XarbDecision: 忽略无效的 open_volatility_limit={}",
                percentile
            );
            return;
        }
        self.open_volatility_limit = percentile;
        info!(
            "XarbDecision: open_volatility_limit 已更新为 {}",
            self.open_volatility_limit
        );
    }

    pub fn update_open_orders_per_round(&mut self, open_orders_per_round: u32) {
        if open_orders_per_round == 0 {
            warn!("XarbDecision: 忽略无效的 open_orders_per_round=0 更新请求");
            return;
        }
        self.open_orders_per_round = open_orders_per_round;
        info!(
            "XarbDecision: open_orders_per_round 已更新，总档位 {}",
            self.open_orders_per_round
        );
    }

    pub fn update_open_scale(&mut self, open_scale: f64) {
        if !(open_scale.is_finite() && open_scale > 0.0) {
            warn!("XarbDecision: 忽略无效的 open_scale={}", open_scale);
            return;
        }
        self.open_scale = open_scale;
        info!("XarbDecision: open_scale 已更新为 {:.6}", self.open_scale);
    }

    pub fn update_return_score_thresholds(
        &mut self,
        thresholds: HashMap<String, ReturnScoreThresholdsResolved>,
    ) {
        self.return_score_thresholds = thresholds;
        info!(
            "XarbDecision: return score thresholds updated symbols={}",
            self.return_score_thresholds.len(),
        );
    }

    pub fn update_funding_open_thresholds(
        &mut self,
        thresholds: HashMap<String, XarbFundingThresholdsResolved>,
    ) {
        self.funding_open_thresholds = thresholds;
        info!(
            "XarbDecision: funding open thresholds updated symbols={}",
            self.funding_open_thresholds.len(),
        );
    }

    pub fn update_open_volatility_thresholds(&mut self, thresholds: HashMap<String, f64>) {
        self.open_volatility_thresholds = thresholds;
        info!(
            "XarbDecision: open volatility thresholds updated symbols={}",
            self.open_volatility_thresholds.len(),
        );
    }

    pub fn update_pnlu_key_suffix(&mut self, key_suffix: String) {
        self.factor_value_hub.update_pnlu_key_suffix(key_suffix);
    }

    pub fn make_spread_only_decision(
        &mut self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<Option<SignalType>> {
        self.poll_model_output_updates();
        let spread_factor = SpreadFactor::instance();
        let now = get_timestamp_us();
        let symbol_list = SymbolList::instance();
        let open_symbol_key = Self::normalize_symbol_key(open_symbol);
        let hedge_symbol_key = Self::normalize_symbol_key(hedge_symbol);

        // 1) Cancel (spread-only)
        if spread_factor.satisfy_forward_cancel(
            open_venue,
            open_symbol_key.as_str(),
            hedge_venue,
            hedge_symbol_key.as_str(),
        ) || spread_factor.satisfy_backward_cancel(
            open_venue,
            open_symbol_key.as_str(),
            hedge_venue,
            hedge_symbol_key.as_str(),
        ) {
            let key = Self::threshold_key(
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                open_venue,
                hedge_venue,
            );
            if !self.is_cooldown_hit(&self.last_cancel_ts, &key, now) {
                self.emit_cancel_signal(
                    open_symbol_key.as_str(),
                    hedge_symbol_key.as_str(),
                    open_venue,
                    hedge_venue,
                )?;
                self.update_last_ts(&self.last_cancel_ts, key, now);
                return Ok(Some(SignalType::ArbCancel));
            }
        }

        let in_dump = symbol_list.is_in_dump_list(open_symbol_key.as_str());
        if in_dump {
            let forward_close = spread_factor.satisfy_forward_close(
                open_venue,
                open_symbol_key.as_str(),
                hedge_venue,
                hedge_symbol_key.as_str(),
            );
            let backward_close = spread_factor.satisfy_backward_close(
                open_venue,
                open_symbol_key.as_str(),
                hedge_venue,
                hedge_symbol_key.as_str(),
            );
            let side = if forward_close {
                Some(Side::Sell)
            } else if backward_close {
                Some(Side::Buy)
            } else {
                None
            };
            let Some(side) = side else {
                return Ok(None);
            };
            let key = Self::threshold_key(
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                open_venue,
                hedge_venue,
            );
            if !self.is_cooldown_hit(&self.last_close_ts, &key, now) {
                self.emit_close_signals(
                    open_symbol_key.as_str(),
                    hedge_symbol_key.as_str(),
                    open_venue,
                    hedge_venue,
                    side,
                )?;
                self.update_last_ts(&self.last_close_ts, key, now);
                return Ok(Some(SignalType::ArbClose));
            }
            return Ok(None);
        }

        let forward_open = !in_dump
            && spread_factor.satisfy_forward_open(
                open_venue,
                open_symbol_key.as_str(),
                hedge_venue,
                hedge_symbol_key.as_str(),
            )
            && symbol_list.is_in_fwd_trade_list(open_symbol_key.as_str());
        let backward_open = !in_dump
            && spread_factor.satisfy_backward_open(
                open_venue,
                open_symbol_key.as_str(),
                hedge_venue,
                hedge_symbol_key.as_str(),
            )
            && symbol_list.is_in_bwd_trade_list(open_symbol_key.as_str());

        let side = if forward_open {
            Some(Side::Buy)
        } else if backward_open {
            Some(Side::Sell)
        } else {
            None
        };

        let Some(side) = side else {
            return Ok(None);
        };

        let key = Self::threshold_key(
            open_symbol_key.as_str(),
            hedge_symbol_key.as_str(),
            open_venue,
            hedge_venue,
        );
        let cooldown_hit = self.is_cooldown_hit(&self.last_open_ts, &key, now);
        if cooldown_hit {
            return Ok(None);
        }
        let Some(open_filter_thresholds) =
            self.lookup_funding_open_thresholds(open_symbol_key.as_str())
        else {
            self.record_intercept_summary("drop_open_missing_filter_thresholds");
            return Ok(None);
        };
        let Some((open_filter_value, open_filter_source)) = self.lookup_realtime_open_filter_value(
            open_symbol_key.as_str(),
            hedge_symbol_key.as_str(),
            open_venue,
            hedge_venue,
        ) else {
            self.record_intercept_summary("drop_open_filter_not_ready");
            return Ok(None);
        };
        let open_filter_threshold = self.select_open_filter_threshold(side, open_filter_thresholds);
        let open_filter_hit = match side {
            Side::Buy => open_filter_value > open_filter_threshold,
            Side::Sell => open_filter_value < open_filter_threshold,
        };
        if !open_filter_hit {
            self.record_intercept_summary(format!(
                "skip_open_by_filter:source={open_filter_source}"
            ));
            return Ok(None);
        }
        let return_lookup = self.lookup_return_model_score_lookup(hedge_symbol, hedge_venue);
        let mut return_score = return_lookup
            .as_ref()
            .and_then(|lookup| lookup.score)
            .filter(|v| v.is_finite());
        let mut return_threshold = return_lookup.as_ref().and_then(|lookup| {
            self.lookup_return_score_thresholds(&lookup.symbol_key)
                .map(|thresholds| self.select_open_return_threshold(side, thresholds))
        });

        if let Some(service_name) = self.return_model_service.clone() {
            let Some(score_lookup) = return_lookup.as_ref() else {
                self.record_intercept_summary(format!(
                    "drop_open_missing_return_model_service:service={service_name}"
                ));
                return Ok(None);
            };
            let Some(score_value) = score_lookup.score.filter(|v| v.is_finite()) else {
                self.record_intercept_summary(format!(
                    "drop_open_score_not_ready:note={}",
                    score_lookup.note
                ));
                return Ok(None);
            };
            return_score = Some(score_value);
        }

        if self.enable_return_score_model {
            let score_lookup = return_lookup
                .as_ref()
                .expect("return_model_service should exist when enable_return_score_model=true");
            let score_value = return_score
                .expect("return_score should be ready when enable_return_score_model=true");
            let Some(threshold_value) = self
                .lookup_return_score_thresholds(&score_lookup.symbol_key)
                .map(|thresholds| self.select_open_return_threshold(side, thresholds))
            else {
                self.record_intercept_summary("drop_open_missing_return_score_thresholds");
                return Ok(None);
            };
            let return_open_hit = match side {
                Side::Buy => score_value > threshold_value,
                Side::Sell => score_value < threshold_value,
            };
            if !return_open_hit {
                self.record_intercept_summary("skip_open_by_return_score");
                return Ok(None);
            }
            return_threshold = Some(threshold_value);
        }
        let environment_signal = self.evaluate_environment_signal(
            open_symbol_key.as_str(),
            hedge_symbol,
            hedge_venue,
            now,
        );
        if self.enable_environment_model && !environment_signal.allow_open {
            self.record_environment_intercept_summary(
                forward_open,
                backward_open,
                false,
                &environment_signal,
            );
            return Ok(None);
        }
        let environment_score = environment_signal
            .score
            .unwrap_or(environment_signal.class_label as f64);
        let target_factor_lookup = self.lookup_target_factor_value(hedge_symbol, hedge_venue);
        let rl_return_volatility_factor = match (
            target_factor_lookup.ready,
            target_factor_lookup.target_factor_value,
        ) {
            (Some(true), Some(value)) if value.is_finite() => value,
            _ => {
                self.record_intercept_summary(format!(
                    "drop_open_target_factor_not_ready:note={}",
                    target_factor_lookup.note
                ));
                return Ok(None);
            }
        };
        if self.enable_volatility_limit {
            let Some(open_volatility_threshold) =
                self.lookup_open_volatility_threshold(open_symbol_key.as_str())
            else {
                self.record_intercept_summary("drop_open_missing_open_volatility_threshold");
                return Ok(None);
            };
            if rl_return_volatility_factor > open_volatility_threshold {
                self.record_intercept_summary(format!(
                    "skip_open_by_volatility_limit:value={:.8}:threshold={:.8}",
                    rl_return_volatility_factor, open_volatility_threshold
                ));
                return Ok(None);
            }
        }

        self.emit_open_signals(
            open_symbol_key.as_str(),
            hedge_symbol_key.as_str(),
            open_venue,
            hedge_venue,
            side,
            return_score,
            return_threshold,
            Some(open_filter_value),
            Some(open_filter_threshold),
            environment_score,
            environment_signal.threshold,
            rl_return_volatility_factor,
        )?;

        self.update_last_ts(&self.last_open_ts, key, now);

        Ok(Some(SignalType::ArbOpen))
    }

    fn handle_backward_query(&mut self, data: Bytes) {
        let query = match ArbHedgeSignalQueryMsg::from_bytes(data) {
            Ok(q) => q,
            Err(err) => {
                warn!("XarbDecision: 解析 hedge query 失败: {err}");
                return;
            }
        };

        let Some(side) = query.get_side() else {
            warn!("XarbDecision: hedge query side 无效: {}", query.hedge_side);
            return;
        };

        let Some(hedge_venue) = TradingVenue::from_u8(query.hedging_venue) else {
            warn!(
                "XarbDecision: hedge query venue 无效: {}",
                query.hedging_venue
            );
            return;
        };

        let hedge_symbol = query.get_hedging_symbol();
        if hedge_symbol.is_empty() {
            warn!("XarbDecision: hedge query 未携带对冲 symbol");
            return;
        }

        let open_symbol = query.get_opening_symbol();
        if open_symbol.is_empty() {
            warn!("XarbDecision: hedge query 未携带开仓 symbol");
            return;
        }

        let Some(open_venue) = TradingVenue::from_u8(query.opening_venue) else {
            warn!(
                "XarbDecision: hedge query opening venue 无效: {}",
                query.opening_venue
            );
            return;
        };

        let hedge_base_qty = query.hedge_base_qty;
        if hedge_base_qty <= 0.0 {
            warn!(
                "XarbDecision: hedge query quantity <= 0 strategy_id={} qty={:.8}",
                query.strategy_id, hedge_base_qty
            );
            return;
        }

        let mkt_channel = MktChannel::instance();

        let Some(open_quote) = mkt_channel.get_quote(&open_symbol, open_venue) else {
            warn!(
                "XarbDecision: hedge query 开仓侧无行情 strategy_id={} symbol={} venue={:?}",
                query.strategy_id, open_symbol, open_venue
            );
            return;
        };

        let Some(hedge_quote) = mkt_channel.get_quote(&hedge_symbol, hedge_venue) else {
            warn!(
                "XarbDecision: hedge query 无行情 strategy_id={} symbol={} venue={:?}",
                query.strategy_id, hedge_symbol, hedge_venue
            );
            return;
        };

        let table = self.table_for(hedge_venue);
        let symbol_key = min_qty_symbol_key(hedge_venue, &hedge_symbol);
        let qty_tick = table.step_size(&symbol_key).unwrap_or(0.0);
        let price_tick = table.price_tick(&symbol_key).unwrap_or(0.0);
        let spread_rate = compute_spread_rate(&open_quote, &hedge_quote);

        let now = get_timestamp_us();
        let open_symbol_key = Self::normalize_symbol_key(&open_symbol);
        let environment_signal = self.evaluate_environment_signal(
            open_symbol_key.as_str(),
            &hedge_symbol,
            hedge_venue,
            now,
        );
        let return_lookup = self.lookup_return_model_score_lookup(&hedge_symbol, hedge_venue);
        let return_score = return_lookup
            .as_ref()
            .and_then(|lookup| lookup.score)
            .filter(|v| v.is_finite());
        let return_threshold = return_lookup.as_ref().and_then(|lookup| {
            self.lookup_return_score_thresholds(&lookup.symbol_key)
                .map(|thresholds| self.select_open_return_threshold_by_hedge_side(side, thresholds))
        });
        let environment_score = environment_signal
            .score
            .unwrap_or(environment_signal.class_label as f64);
        let target_factor_lookup = self.lookup_target_factor_value(&hedge_symbol, hedge_venue);
        let seq_threshold = self.hedge_aggressive_seq_threshold;
        let aggressive = query.request_seq >= seq_threshold;
        let default_offset = self.hedge_price_offset.abs();
        let open_bbo = Bbo::new(query.hedge_bid0, query.hedge_ask0, query.hedge_leg_ts);
        let hedge_bbo = Bbo::new(hedge_quote.bid, hedge_quote.ask, hedge_quote.ts);
        let open_mid = open_bbo.get_mid_price().unwrap_or(0.0);
        let hedge_mid = hedge_bbo.get_mid_price().unwrap_or(0.0);
        let mut pct_change = 0.0;
        let mut stop_loss_triggered = false;
        let mut stop_loss_valid = false;
        let threshold_pct = self.max_hedge_price_pct_change;
        let threshold_ratio = threshold_pct / 100.0;
        if open_mid > 0.0 && hedge_mid > 0.0 {
            pct_change = (hedge_mid - open_mid).abs() / open_mid;
            stop_loss_valid = true;
            stop_loss_triggered = pct_change > threshold_ratio;
        }
        info!(
            "XarbDecision: hedge stop-loss check strategy_id={} open_mid={:.6} hedge_mid={:.6} pct_change={:.6} threshold_pct={:.2} trigger={} valid={}",
            query.strategy_id,
            open_mid,
            hedge_mid,
            pct_change,
            threshold_pct,
            stop_loss_triggered,
            stop_loss_valid
        );
        if stop_loss_triggered {
            let market_price = match side {
                Side::Buy => hedge_quote.ask,
                Side::Sell => hedge_quote.bid,
            };
            let aligned_hedge_qty = self.convert_aligned_base_qty_to_open_venue_qty(
                hedge_venue,
                &hedge_symbol,
                market_price,
                hedge_base_qty,
            );
            if aligned_hedge_qty <= 0.0 {
                warn!(
                    "XarbDecision: stop-loss taker aligned qty invalid strategy_id={} symbol_key={} base_qty={:.8}",
                    query.strategy_id,
                    symbol_key,
                    hedge_base_qty
                );
                return;
            }
            let mut ctx = ArbHedgeCtx::new_taker(
                query.strategy_id,
                query.client_order_id,
                side.to_u8(),
                aligned_hedge_qty,
                qty_tick,
            );
            ctx.opening_leg =
                TradingLeg::new(open_venue, open_quote.bid, open_quote.ask, open_quote.ts);
            ctx.set_opening_symbol(&open_symbol);
            ctx.hedging_leg = TradingLeg::new(
                hedge_venue,
                hedge_quote.bid,
                hedge_quote.ask,
                hedge_quote.ts,
            );
            ctx.set_hedging_symbol(&hedge_symbol);
            ctx.market_ts = now;
            ctx.price_offset = 0.0;
            ctx.spread_rate = spread_rate;
            ctx.maker_only = false;
            let from_key = self.build_hedge_from_key(
                now,
                return_score,
                return_threshold,
                environment_score,
                environment_signal.threshold,
                target_factor_lookup.target_factor_value,
                pct_change,
                spread_rate,
                hedge_venue,
                &hedge_symbol,
                market_price,
            );
            ctx.set_from_key(from_key);

            let signal = TradeSignal::create(SignalType::ArbHedge, now, 0.0, ctx.to_bytes());
            if let Err(err) = self.signal_pub.publish(&signal.to_bytes()) {
                warn!(
                    "XarbDecision: 发送 stop-loss taker hedge 失败 strategy_id={} err={:?}",
                    query.strategy_id, err
                );
                return;
            }
            info!(
                "XarbDecision: 触发 stop-loss taker hedge strategy_id={} hedge_symbol={} qty={:.6} side={:?} pct_change={:.6} threshold_pct={:.2} spread_rate={:.6}",
                query.strategy_id,
                hedge_symbol,
                ctx.hedge_qty_value(),
                side,
                pct_change,
                threshold_pct,
                spread_rate
            );
            return;
        }
        let mut offset = default_offset;
        let mut offset_source = "config";
        let mut offset_note = String::new();

        let ready = target_factor_lookup.ready.unwrap_or(false);

        if ready {
            if let Some(value) = target_factor_lookup.target_factor_value {
                if value.is_finite() && value > 0.0 {
                    offset = value;
                    offset_source = "rl_return_volatility_factor";
                } else {
                    offset_note = "invalid_factor".to_string();
                }
            } else {
                offset_note = "missing_factor".to_string();
            }
        } else if target_factor_lookup.note == "ok" {
            offset_note = "not_ready".to_string();
        } else {
            offset_note = target_factor_lookup.note.clone();
        }

        if aggressive {
            offset = 0.0;
            offset_source = "aggressive";
            if offset_note.is_empty() {
                offset_note = "aggressive_override".to_string();
            } else {
                offset_note = format!("aggressive_override({})", offset_note);
            }
        }

        info!(
            "XarbDecision: hedge query offset source={} key={} symbol={} venue={:?} norm_symbol={} ready={} factor={:?} factor_index={:?} ts_ms={:?} offset={:.6} default_offset={:.6} aggressive={} note={}",
            offset_source,
            target_factor_lookup.key,
            hedge_symbol,
            hedge_venue,
            target_factor_lookup.symbol_key,
            ready,
            target_factor_lookup.target_factor_value,
            target_factor_lookup.factor_index,
            target_factor_lookup.ts_ms,
            offset,
            default_offset,
            aggressive,
            offset_note
        );

        let base_price = match side {
            Side::Buy => hedge_quote.bid,
            Side::Sell => hedge_quote.ask,
        };
        let limit_price = if base_price > 0.0 {
            match side {
                Side::Buy => base_price * (1.0 - offset),
                Side::Sell => base_price * (1.0 + offset),
            }
        } else {
            0.0
        };

        if limit_price <= 0.0 {
            warn!(
                "XarbDecision: hedge query limit_price 无效 strategy_id={} price={:.8}",
                query.strategy_id, limit_price
            );
            return;
        }

        let aligned_hedge_qty = self.convert_aligned_base_qty_to_open_venue_qty(
            hedge_venue,
            &hedge_symbol,
            limit_price,
            hedge_base_qty,
        );
        if aligned_hedge_qty <= 0.0 {
            warn!(
                "XarbDecision: hedge query aligned qty invalid strategy_id={} symbol_key={} base_qty={:.8}",
                query.strategy_id,
                symbol_key,
                hedge_base_qty
            );
            return;
        }

        let mut ctx = ArbHedgeCtx::new_maker(
            query.strategy_id,
            query.client_order_id,
            side.to_u8(),
            aligned_hedge_qty,
            qty_tick,
            limit_price,
            price_tick,
            false,
            now + self.hedge_timeout_mm_us,
        );
        if ctx.hedge_qty_count() <= 0 || ctx.hedge_price_count() <= 0 {
            warn!(
                "XarbDecision: hedge query qv invalid strategy_id={} qty={:.8} price={:.8}",
                query.strategy_id,
                ctx.hedge_qty_value(),
                ctx.hedge_price_value()
            );
            return;
        }
        ctx.opening_leg =
            TradingLeg::new(open_venue, open_quote.bid, open_quote.ask, open_quote.ts);
        ctx.set_opening_symbol(&open_symbol);
        ctx.hedging_leg = TradingLeg::new(
            hedge_venue,
            hedge_quote.bid,
            hedge_quote.ask,
            hedge_quote.ts,
        );
        ctx.set_hedging_symbol(&hedge_symbol);
        ctx.market_ts = now;
        ctx.price_offset = offset;
        ctx.spread_rate = spread_rate;
        let from_key = self.build_hedge_from_key(
            now,
            return_score,
            return_threshold,
            environment_score,
            environment_signal.threshold,
            target_factor_lookup.target_factor_value,
            pct_change,
            spread_rate,
            hedge_venue,
            &hedge_symbol,
            ctx.hedge_price_value(),
        );
        ctx.set_from_key(from_key);

        let signal = TradeSignal::create(SignalType::ArbHedge, now, 0.0, ctx.to_bytes());

        if let Err(err) = self.signal_pub.publish(&signal.to_bytes()) {
            warn!(
                "XarbDecision: 发送 hedge 信号失败 strategy_id={} err={:?}",
                query.strategy_id, err
            );
            return;
        }

        info!(
            "XarbDecision: 回复 hedge query strategy_id={} hedge_symbol={} qty={:.6} side={:?} seq={} aggressive={} limit_price={:.8} offset={:.6} spread_rate={:.6} (maker)",
            query.strategy_id,
            hedge_symbol,
            ctx.hedge_qty_value(),
            side,
            query.request_seq,
            aggressive,
            ctx.hedge_price_value(),
            offset,
            spread_rate
        );
    }

    fn load_valid_quotes(
        &self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Option<(Quote, Quote)> {
        let mkt_channel = MktChannel::instance();
        let open_quote = mkt_channel.get_quote(open_symbol, open_venue);
        let hedge_quote = mkt_channel.get_quote(hedge_symbol, hedge_venue);

        if open_quote.is_none() || hedge_quote.is_none() {
            warn!(
                "XarbDecision: quote unavailable open={} ({:?}) hedge={} ({:?})",
                open_symbol,
                open_quote.is_some(),
                hedge_symbol,
                hedge_quote.is_some()
            );
            return None;
        }

        let open_quote = open_quote.unwrap();
        let hedge_quote = hedge_quote.unwrap();

        if open_quote.bid >= open_quote.ask {
            warn!(
                "XarbDecision: invalid open quote bid={:.8} >= ask={:.8} for {}",
                open_quote.bid, open_quote.ask, open_symbol
            );
            return None;
        }
        if hedge_quote.bid >= hedge_quote.ask {
            warn!(
                "XarbDecision: invalid hedge quote bid={:.8} >= ask={:.8} for {}",
                hedge_quote.bid, hedge_quote.ask, hedge_symbol
            );
            return None;
        }

        Some((open_quote, hedge_quote))
    }

    fn emit_open_signals(
        &mut self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        side: Side,
        return_score: Option<f64>,
        return_threshold: Option<f64>,
        open_filter_value: Option<f64>,
        open_filter_threshold: Option<f64>,
        environment_score: f64,
        environment_threshold: Option<f64>,
        rl_return_volatility_factor: f64,
    ) -> Result<()> {
        let (open_quote, hedge_quote) =
            match self.load_valid_quotes(open_symbol, hedge_symbol, open_venue, hedge_venue) {
                Some(quotes) => quotes,
                None => return Ok(()),
            };
        let panic_on_first_open_dry_run = env_flag(PANIC_ON_FIRST_OPEN_DRY_RUN_ENV);
        // Use one batch timestamp for all grid offsets in this emit call.
        let batch_ts = get_timestamp_us();
        let spread_rate = compute_spread_rate(&open_quote, &hedge_quote);
        let scaled_volatility = (rl_return_volatility_factor * self.open_scale).max(0.0);
        let base_from_key = build_decision_from_key_base(
            batch_ts,
            return_score,
            return_threshold,
            Some(rl_return_volatility_factor),
            Some(environment_score),
            environment_threshold,
        );
        let from_key = format!(
            "{base_from_key}:open_signal={}:open_signal_thr={}:spread={spread_rate:.6}:open_scale={:.6}",
            open_filter_value
                .map(|value| format!("{value:.6}"))
                .unwrap_or_else(|| "NA".to_string()),
            open_filter_threshold
                .map(|value| format!("{value:.6}"))
                .unwrap_or_else(|| "NA".to_string()),
            self.open_scale,
        );

        let plan = match build_xarb_open_quote_plan(
            open_venue,
            open_symbol,
            open_quote,
            self.order_amount as f64,
            self.open_orders_per_round,
            side,
            scaled_volatility,
            self.table_for(open_venue),
        ) {
            Ok(plan) => plan,
            Err(err) => {
                warn!(
                    "XarbDecision: build open quote plan failed open={} hedge={} side={:?} err={}",
                    open_symbol, hedge_symbol, side, err
                );
                return Ok(());
            }
        };

        for level in &plan.levels {
            let ctx = self.build_open_context_from_level(
                open_symbol,
                hedge_symbol,
                open_venue,
                hedge_venue,
                &open_quote,
                &hedge_quote,
                level,
                batch_ts,
                from_key.as_str(),
            );

            if panic_on_first_open_dry_run {
                let from_key_str = String::from_utf8_lossy(&ctx.from_key);
                warn!(
                    "XarbDecision: dry-run trap hit first ArbOpen open={} hedge={} side={:?} price={:.8} qty={:.8} from_key='{}'",
                    open_symbol,
                    hedge_symbol,
                    plan.side,
                    ctx.price_value(),
                    ctx.amount_value(),
                    from_key_str
                );
                panic!(
                    "{} triggered: first ArbOpen open={} hedge={} side={:?} from_key='{}'",
                    PANIC_ON_FIRST_OPEN_DRY_RUN_ENV,
                    open_symbol,
                    hedge_symbol,
                    plan.side,
                    from_key_str
                );
            }

            let signal = TradeSignal::create(SignalType::ArbOpen, batch_ts, 0.0, ctx.to_bytes());
            self.signal_pub.publish(&signal.to_bytes())?;
        }

        info!(
            "XarbDecision: emitted {} {:?} signal(s) to '{}' open={} hedge={} side={:?} inner={:.8} outer={:.8} vol={:.8} open_scale={:.6} scaled_vol={:.8} price_tick={:.8} qty_tick={:.8}",
            plan.levels.len(),
            SignalType::ArbOpen,
            self.channel_name,
            open_symbol,
            hedge_symbol,
            plan.side,
            plan.inner_price,
            plan.outer_price,
            rl_return_volatility_factor,
            self.open_scale,
            scaled_volatility,
            plan.price_tick,
            plan.qty_tick
        );

        Ok(())
    }

    fn emit_close_signals(
        &mut self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        side: Side,
    ) -> Result<()> {
        let (open_quote, hedge_quote) =
            match self.load_valid_quotes(open_symbol, hedge_symbol, open_venue, hedge_venue) {
                Some(quotes) => quotes,
                None => return Ok(()),
            };
        let batch_ts = get_timestamp_us();
        let spread_rate = compute_spread_rate(&open_quote, &hedge_quote);
        let from_key = format!("{batch_ts}:dump:{spread_rate:.6}");

        let volatility = self
            .lookup_target_factor_value(hedge_symbol, hedge_venue)
            .target_factor_value
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(0.0);
        let plan = match build_xarb_open_quote_plan(
            open_venue,
            open_symbol,
            open_quote,
            self.order_amount as f64,
            self.open_orders_per_round,
            side,
            volatility,
            self.table_for(open_venue),
        ) {
            Ok(plan) => plan,
            Err(err) => {
                warn!(
                    "XarbDecision: build close quote plan failed open={} hedge={} side={:?} err={}",
                    open_symbol, hedge_symbol, side, err
                );
                return Ok(());
            }
        };

        for level in &plan.levels {
            let ctx = self.build_open_context_from_level(
                open_symbol,
                hedge_symbol,
                open_venue,
                hedge_venue,
                &open_quote,
                &hedge_quote,
                level,
                batch_ts,
                from_key.as_str(),
            );

            let signal = TradeSignal::create(SignalType::ArbClose, batch_ts, 0.0, ctx.to_bytes());
            self.signal_pub.publish(&signal.to_bytes())?;
        }

        info!(
            "XarbDecision: emitted {} {:?} signal(s) to '{}' open={} hedge={} side={:?} inner={:.8} outer={:.8} vol={:.8} price_tick={:.8} qty_tick={:.8}",
            plan.levels.len(),
            SignalType::ArbClose,
            self.channel_name,
            open_symbol,
            hedge_symbol,
            plan.side,
            plan.inner_price,
            plan.outer_price,
            volatility,
            plan.price_tick,
            plan.qty_tick
        );

        Ok(())
    }

    fn emit_cancel_signal(
        &mut self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<()> {
        let (open_quote, hedge_quote) =
            match self.load_valid_quotes(open_symbol, hedge_symbol, open_venue, hedge_venue) {
                Some(quotes) => quotes,
                None => return Ok(()),
            };
        let batch_ts = get_timestamp_us();
        let spread_rate = compute_spread_rate(&open_quote, &hedge_quote);
        let return_score = self
            .lookup_return_model_score_lookup(hedge_symbol, hedge_venue)
            .and_then(|lookup| lookup.score)
            .filter(|v| v.is_finite());
        let environment_signal =
            self.evaluate_environment_signal(open_symbol, hedge_symbol, hedge_venue, batch_ts);
        let environment_score = environment_signal
            .score
            .unwrap_or(environment_signal.class_label as f64);
        let volatility = self
            .lookup_target_factor_value(hedge_symbol, hedge_venue)
            .target_factor_value;
        let from_key = build_decision_from_key_base(
            batch_ts,
            return_score,
            None,
            volatility,
            Some(environment_score),
            environment_signal.threshold,
        );
        let from_key = format!("{from_key}:spread={spread_rate:.6}");

        let ctx = self.build_cancel_context(
            open_symbol,
            hedge_symbol,
            open_venue,
            hedge_venue,
            &open_quote,
            &hedge_quote,
            batch_ts,
            &from_key,
        );

        let signal = TradeSignal::create(SignalType::ArbCancel, batch_ts, 0.0, ctx.to_bytes());
        self.signal_pub.publish(&signal.to_bytes())?;
        Ok(())
    }

    fn build_open_context_from_level(
        &self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        open_quote: &Quote,
        hedge_quote: &Quote,
        level: &QuotePlanLevel,
        now: i64,
        from_key: &str,
    ) -> ArbOpenCtx {
        let mut ctx = ArbOpenCtx::new();
        let open_trade_symbol = normalize_symbol_for_venue(open_symbol, open_venue);
        let hedge_trade_symbol = normalize_symbol_for_venue(hedge_symbol, hedge_venue);

        ctx.opening_leg =
            TradingLeg::new(open_venue, open_quote.bid, open_quote.ask, open_quote.ts);
        ctx.set_opening_symbol(&open_trade_symbol);

        ctx.hedging_leg = TradingLeg::new(
            hedge_venue,
            hedge_quote.bid,
            hedge_quote.ask,
            hedge_quote.ts,
        );
        ctx.set_hedging_symbol(&hedge_trade_symbol);

        ctx.set_side(level.side);
        ctx.set_order_type(OrderType::Limit);
        let aligned_open_price = level.aligned_price;

        let base_qty = self.convert_order_amount_to_aligned_base_qty(
            open_venue,
            &open_trade_symbol,
            hedge_venue,
            &hedge_trade_symbol,
            open_quote,
            hedge_quote,
            aligned_open_price,
            level.side,
        );
        let aligned_open_qty = self.convert_aligned_base_qty_to_open_venue_qty(
            open_venue,
            &open_trade_symbol,
            aligned_open_price,
            base_qty,
        );

        let table = self.table_for(open_venue);
        let symbol_key = min_qty_symbol_key(open_venue, &open_trade_symbol);

        let raw_price_tick = table.price_tick(&symbol_key).unwrap_or(0.0);
        let price_fallback = ctx.set_price_with_tick_floor(aligned_open_price, raw_price_tick);
        if raw_price_tick <= 0.0 || price_fallback {
            warn!(
                "XarbDecision: missing price_tick for {:?} symbol_key={}, fallback to full-price tick",
                open_venue, symbol_key
            );
        }
        if ctx.price_count() <= 0 {
            warn!(
                "XarbDecision: invalid aligned_open_price for {:?} symbol_key={}, fallback to zero",
                open_venue, symbol_key
            );
        }

        let raw_amount_tick = table.step_size(&symbol_key).unwrap_or(0.0);
        let amount_fallback = ctx.set_amount_with_tick_floor(aligned_open_qty, raw_amount_tick);
        if raw_amount_tick <= 0.0 || amount_fallback {
            warn!(
                "XarbDecision: missing step_size for {:?} symbol_key={}, fallback to full-qty tick",
                open_venue, symbol_key
            );
        }
        if ctx.amount_count() <= 0 {
            warn!(
                "XarbDecision: invalid aligned_open_qty for {:?} symbol_key={}, fallback to zero",
                open_venue, symbol_key
            );
        }

        ctx.exp_time = now + self.open_order_ttl_us;
        ctx.create_ts = now;
        ctx.price_offset = level.offset;
        ctx.spread_rate = compute_spread_rate(open_quote, hedge_quote);

        let spread_factor = SpreadFactor::instance();
        let mode = spread_factor.get_mode();
        ctx.hedge_timeout_us = match mode {
            super::common::FactorMode::MT => 0,
            super::common::FactorMode::MM => self.hedge_timeout_mm_us,
        };

        let from_key_with_tlen = format!("{from_key}:tlen=0.00000000");

        ctx.set_from_key(from_key_with_tlen.into_bytes());

        ctx
    }

    fn build_hedge_from_key(
        &self,
        now: i64,
        return_score: Option<f64>,
        return_threshold: Option<f64>,
        environment_score: f64,
        environment_threshold: Option<f64>,
        rl_return_volatility_factor: Option<f64>,
        pct_change: f64,
        spread_rate: f64,
        hedge_venue: TradingVenue,
        hedge_symbol: &str,
        hedge_price: f64,
    ) -> Vec<u8> {
        let base_from_key = build_decision_from_key_base(
            now,
            return_score,
            return_threshold,
            rl_return_volatility_factor,
            Some(environment_score),
            environment_threshold,
        );
        let base_from_key =
            format!("{base_from_key}:spread={spread_rate:.6}:pct_change={pct_change:.6}");
        let hedge_trade_symbol = normalize_symbol_for_venue(hedge_symbol, hedge_venue);
        let table = self.table_for(hedge_venue);
        let symbol_key = min_qty_symbol_key(hedge_venue, &hedge_trade_symbol);
        let raw_price_tick = table.price_tick(&symbol_key).unwrap_or(0.0);

        if !(raw_price_tick.is_finite() && raw_price_tick > 0.0) {
            warn!(
                "XarbDecision: hedge from_key missing price_tick hedge={} venue={:?} symbol_key={}",
                hedge_trade_symbol, hedge_venue, symbol_key
            );
            return format!("{base_from_key}:tlen_query_err=missing_price_tick").into_bytes();
        }
        if hedge_price <= 0.0 {
            warn!(
                "XarbDecision: hedge from_key invalid price hedge={} venue={:?} price={:.8}",
                hedge_trade_symbol, hedge_venue, hedge_price
            );
            return format!("{base_from_key}:tlen_query_err=invalid_price").into_bytes();
        }
        let Some(tick_index) = price_to_tick_index(hedge_price, raw_price_tick) else {
            warn!(
                "XarbDecision: hedge from_key tick conversion failed hedge={} venue={:?} price={:.8} price_tick={:.8}",
                hedge_trade_symbol, hedge_venue, hedge_price, raw_price_tick
            );
            return format!("{base_from_key}:tlen_query_err=invalid_price_or_tick").into_bytes();
        };

        let depth_query_client = if hedge_venue == self.venues.0 {
            &self.open_depth_query_client
        } else {
            &self.hedge_depth_query_client
        };
        match depth_query_client.query_single_tick_index(&hedge_trade_symbol, tick_index) {
            Ok(tlen) => format!("{base_from_key}:tlen={tlen:.8}").into_bytes(),
            Err(err) => {
                warn!(
                    "XarbDecision: hedge from_key tlen query failed hedge={} venue={:?} tick_index={} err={err:#}",
                    hedge_trade_symbol, hedge_venue, tick_index
                );
                format!("{base_from_key}:tlen_query_err=single_query_failed").into_bytes()
            }
        }
    }

    fn build_cancel_context(
        &self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        open_quote: &Quote,
        hedge_quote: &Quote,
        now: i64,
        from_key: &str,
    ) -> ArbCancelCtx {
        let mut ctx = ArbCancelCtx::new();
        let open_trade_symbol = normalize_symbol_for_venue(open_symbol, open_venue);
        let hedge_trade_symbol = normalize_symbol_for_venue(hedge_symbol, hedge_venue);

        ctx.opening_leg =
            TradingLeg::new(open_venue, open_quote.bid, open_quote.ask, open_quote.ts);
        ctx.set_opening_symbol(&open_trade_symbol);

        ctx.hedging_leg = TradingLeg::new(
            hedge_venue,
            hedge_quote.bid,
            hedge_quote.ask,
            hedge_quote.ts,
        );
        ctx.set_hedging_symbol(&hedge_trade_symbol);

        ctx.trigger_ts = now;
        ctx.set_from_key(from_key.as_bytes().to_vec());
        ctx
    }

    fn table_for(&self, venue: TradingVenue) -> &VenueMinQtyTable {
        if venue == self.venues.0 {
            &self.open_min_qty_table
        } else {
            &self.hedge_min_qty_table
        }
    }

    fn convert_aligned_base_qty_to_open_venue_qty(
        &self,
        open_venue: TradingVenue,
        open_symbol: &str,
        open_price: f64,
        aligned_base_qty: f64,
    ) -> f64 {
        if aligned_base_qty <= 0.0 || open_price <= 0.0 {
            return 0.0;
        }

        let table = self.table_for(open_venue);
        let symbol_key = min_qty_symbol_key(open_venue, open_symbol);

        let qty_multiplier = if venue_qty_is_contracts(open_venue) {
            let Some(multiplier) = contract_qty_multiplier(table, open_venue, &symbol_key) else {
                warn!(
                    "XarbDecision: missing/invalid contract_multiplier for {:?} symbol_key={}, cannot convert aligned base qty to venue qty",
                    open_venue, symbol_key
                );
                return 0.0;
            };
            multiplier
        } else {
            1.0
        };

        let mut venue_qty = aligned_base_qty / qty_multiplier;
        let step = table.step_size(&symbol_key).unwrap_or(0.0);
        if step > 0.0 {
            venue_qty = align_price_floor(venue_qty, step);
            if venue_qty <= 0.0 {
                venue_qty = step;
            }
        }

        if let Some(min_qty) = table.min_qty(&symbol_key) {
            if min_qty > 0.0 && venue_qty < min_qty {
                venue_qty = min_qty;
            }
        }

        if open_venue.is_futures() {
            if let Some(min_notional) = table.min_notional(&symbol_key) {
                if min_notional > 0.0 {
                    let required_base_qty = min_notional / open_price;
                    let required_venue_qty = required_base_qty / qty_multiplier;
                    if venue_qty + 1e-12 < required_venue_qty {
                        venue_qty = if step > 0.0 {
                            align_price_ceil(required_venue_qty, step)
                        } else {
                            required_venue_qty
                        };
                    }
                }
            }
        }

        venue_qty
    }

    fn convert_order_amount_to_aligned_base_qty(
        &self,
        open_venue: TradingVenue,
        open_symbol: &str,
        hedge_venue: TradingVenue,
        hedge_symbol: &str,
        open_quote: &Quote,
        hedge_quote: &Quote,
        open_price: f64,
        open_side: Side,
    ) -> f64 {
        if !(self.order_amount > 0.0) {
            warn!(
                "XarbDecision: order_amount <= 0 when building signal for {}, skip",
                open_symbol
            );
            return 0.0;
        }
        if open_price <= 0.0 {
            warn!(
                "XarbDecision: price for {} <= 0 when converting order amount, fallback to 0",
                open_symbol
            );
            return 0.0;
        }

        let open_table = self.table_for(open_venue);
        let hedge_table = self.table_for(hedge_venue);

        let raw_base_qty = self.order_amount as f64 / open_price;
        let (open_base_step, open_min_base_qty) =
            base_step_size(open_table, open_venue, open_symbol);
        let (hedge_base_step, hedge_min_base_qty) =
            base_step_size(hedge_table, hedge_venue, hedge_symbol);

        let align_step = open_base_step
            .unwrap_or(0.0)
            .max(hedge_base_step.unwrap_or(0.0));
        if align_step <= 0.0 {
            return raw_base_qty;
        }

        let mut base_qty = align_price_floor(raw_base_qty, align_step);
        if base_qty <= 0.0 {
            base_qty = align_step;
        }

        let mut required_min_base = align_step;
        if let Some(v) = open_min_base_qty {
            if v > required_min_base {
                required_min_base = v;
            }
        }
        if let Some(v) = hedge_min_base_qty {
            if v > required_min_base {
                required_min_base = v;
            }
        }

        let open_symbol_key = min_qty_symbol_key(open_venue, open_symbol);
        let hedge_symbol_key = min_qty_symbol_key(hedge_venue, hedge_symbol);
        if let Some(min_notional) = open_table.min_notional(&open_symbol_key) {
            if min_notional > 0.0 && open_price > 0.0 {
                required_min_base = required_min_base.max(min_notional / open_price);
            }
        }
        let hedge_side = if open_side == Side::Buy {
            Side::Sell
        } else {
            Side::Buy
        };
        let hedge_price_for_min_notional = match hedge_side {
            Side::Buy => hedge_quote.ask,
            Side::Sell => hedge_quote.bid,
        };
        if let Some(min_notional) = hedge_table.min_notional(&hedge_symbol_key) {
            if min_notional > 0.0 && hedge_price_for_min_notional > 0.0 {
                required_min_base =
                    required_min_base.max(min_notional / hedge_price_for_min_notional);
            }
        }

        if base_qty + 1e-12 < required_min_base {
            base_qty = align_price_ceil(required_min_base, align_step);
        }

        // 对合约腿（Binance/OKX/Gate）确保 multiplier 可用：Binance 固定为 1，OKX/Gate 需已加载
        for (venue, table, symbol_key) in [
            (open_venue, open_table, &open_symbol_key),
            (hedge_venue, hedge_table, &hedge_symbol_key),
        ] {
            if venue_qty_is_contracts(venue)
                && contract_qty_multiplier(table, venue, symbol_key).is_none()
            {
                warn!(
                    "XarbDecision: missing contract_multiplier for {:?} symbol_key={} (BinanceFutures fixed=1, OKX/Gate need table), qty alignment may be inaccurate",
                    venue, symbol_key
                );
            }
        }

        // 打印一次关键对齐信息（用于排障）
        if log::log_enabled!(log::Level::Debug) {
            let open_step = open_base_step.unwrap_or(0.0);
            let hedge_step = hedge_base_step.unwrap_or(0.0);
            let open_mid = (open_quote.bid + open_quote.ask) * 0.5;
            let hedge_mid = (hedge_quote.bid + hedge_quote.ask) * 0.5;
            log::debug!(
                "XarbDecision qty_align: open={:?} {} step_base={:.10} hedge={:?} {} step_base={:.10} align_step={:.10} raw_base_qty={:.10} base_qty={:.10} required_min_base={:.10} open_price={:.6} hedge_mid={:.6} open_mid={:.6}",
                open_venue,
                open_symbol_key,
                open_step,
                hedge_venue,
                hedge_symbol_key,
                hedge_step,
                align_step,
                raw_base_qty,
                base_qty,
                required_min_base,
                open_price,
                hedge_mid,
                open_mid
            );
        }

        base_qty
    }

    pub fn update_open_order_timeout(&mut self, open_secs: u64) {
        if open_secs == 0 {
            warn!("XarbDecision: open_secs=0 无效，忽略更新");
            return;
        }
        let ttl = open_secs.saturating_mul(1_000_000).min(i64::MAX as u64);
        self.open_order_ttl_us = ttl as i64;
        info!("XarbDecision: open_order_ttl 更新为 {}s", open_secs);
    }

    pub fn update_hedge_timeout(&mut self, hedge_secs: u64) {
        if hedge_secs == 0 {
            warn!("XarbDecision: hedge_secs=0 无效，忽略更新");
            return;
        }
        let ttl = hedge_secs.saturating_mul(1_000_000).min(i64::MAX as u64);
        self.hedge_timeout_mm_us = ttl as i64;
        info!("XarbDecision: hedge_timeout_mm 更新为 {}s", hedge_secs);
    }

    pub fn update_order_amount(&mut self, amount: f32) {
        if amount <= 0.0 {
            warn!("XarbDecision: amount <= 0 无效，忽略更新");
            return;
        }
        self.order_amount = amount;
        info!("XarbDecision: order_amount 更新为 {:.4}", self.order_amount);
    }

    pub fn update_hedge_price_offset(&mut self, offset: f64) {
        if offset <= 0.0 {
            warn!("XarbDecision: hedge offset <= 0 无效，忽略更新");
            return;
        }
        self.hedge_price_offset = offset;
        info!("XarbDecision: hedge_price_offset 更新为 {:.6}", offset);
    }

    pub fn update_hedge_aggressive_seq_threshold(&mut self, threshold: u32) {
        if threshold == 0 {
            warn!("XarbDecision: hedge_aggressive_seq_threshold=0 无效，忽略更新");
            return;
        }
        self.hedge_aggressive_seq_threshold = threshold;
        info!(
            "XarbDecision: hedge_aggressive_seq_threshold 更新为 {}",
            threshold
        );
    }

    pub fn update_max_hedge_price_pct_change(&mut self, pct_change: f64) {
        if !(pct_change.is_finite() && pct_change > 0.0 && pct_change <= 99.0) {
            warn!(
                "XarbDecision: max_hedge_price_pct_change 无效(需在(0,99])，忽略更新 value={}",
                pct_change
            );
            return;
        }
        self.max_hedge_price_pct_change = pct_change;
        info!(
            "XarbDecision: max_hedge_price_pct_change 更新为 {:.2}",
            pct_change
        );
    }

    pub fn update_signal_cooldown(&mut self, cooldown_secs: u64) {
        if cooldown_secs == 0 {
            warn!("XarbDecision: cooldown_secs=0 无效，忽略更新");
            return;
        }
        let cooldown_us = cooldown_secs.saturating_mul(1_000_000).min(i64::MAX as u64);
        self.signal_cooldown_us = cooldown_us as i64;
        info!(
            "XarbDecision: signal_cooldown 更新为 {}s ({}us)",
            cooldown_secs, self.signal_cooldown_us
        );
    }

    fn threshold_key(
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> ThresholdKey {
        (
            open_venue,
            open_symbol.to_uppercase(),
            hedge_venue,
            hedge_symbol.to_uppercase(),
        )
    }

    fn is_cooldown_hit(
        &self,
        last_ts_map: &RefCell<HashMap<ThresholdKey, i64>>,
        key: &ThresholdKey,
        now: i64,
    ) -> bool {
        if let Some(&last_ts) = last_ts_map.borrow().get(key) {
            let elapsed = now - last_ts;
            if elapsed < self.signal_cooldown_us {
                return true;
            }
        }
        false
    }

    fn update_last_ts(
        &self,
        last_ts_map: &RefCell<HashMap<ThresholdKey, i64>>,
        key: ThresholdKey,
        now: i64,
    ) {
        last_ts_map.borrow_mut().insert(key, now);
    }

    fn record_environment_intercept_summary(
        &mut self,
        forward_open: bool,
        backward_open: bool,
        cooldown_hit: bool,
        result: &EnvironmentSignalResult,
    ) {
        self.record_intercept_summary(compact_environment_intercept_reason(
            forward_open,
            backward_open,
            cooldown_hit,
            result,
        ));
    }

    pub fn spawn_backward_listener() {
        tokio::task::spawn_local(async move {
            info!("XarbDecision backward 监听任务启动");

            loop {
                let has_message = XARB_DECISION.with(|cell| {
                    let decision_ref = cell.get();
                    if decision_ref.is_none() {
                        return false;
                    }
                    let mut decision = decision_ref.unwrap().borrow_mut();
                    decision.poll_model_output_updates();
                    match decision.backward_sub.receive_msg() {
                        Ok(Some(data)) => {
                            decision.handle_backward_query(data);
                            true
                        }
                        Ok(None) => false,
                        Err(err) => {
                            warn!("XarbDecision: backward_sub 接收错误: {}", err);
                            false
                        }
                    }
                });

                if !has_message {
                    tokio::task::yield_now().await;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_xarb_level_specs, compact_environment_intercept_reason, compact_environment_note,
    };
    use crate::funding_rate::factor_value_hub::{EnvironmentSignalResult, EnvironmentSignalSource};
    use crate::pre_trade::order_manager::Side;

    #[test]
    fn xarb_buy_plan_starts_from_bbo_and_reaches_vol_edge() {
        let specs = build_xarb_level_specs(Side::Buy, 100.0, 0.01, 4);
        assert_eq!(specs.len(), 4);
        assert!((specs[0].offset - 0.0).abs() < 1e-12);
        assert!((specs[1].offset - (0.01 / 3.0)).abs() < 1e-12);
        assert!((specs[3].offset - 0.01).abs() < 1e-12);
    }

    #[test]
    fn xarb_single_level_plan_joins_bbo() {
        let specs = build_xarb_level_specs(Side::Sell, 100.0, 0.02, 1);
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].side, Side::Sell);
        assert!((specs[0].offset - 0.0).abs() < 1e-12);
    }

    #[test]
    fn xarb_environment_intercept_reason_is_compact_for_model_output() {
        let result = EnvironmentSignalResult {
            source: EnvironmentSignalSource::ModelOutput,
            allow_open: false,
            class_label: 0,
            service_name: Some(
                "model_output/binance-futures/some-very-long-service-name".to_string(),
            ),
            symbol_key: "BTCUSDT".to_string(),
            score: Some(-0.1),
            threshold: Some(0.0),
            note: "model_score_lt_threshold".to_string(),
        };

        let reason = compact_environment_intercept_reason(true, false, false, &result);
        assert_eq!(reason, "env_blk:d=f:src=mo:n=score_lt:cd=0");
    }

    #[test]
    fn xarb_environment_intercept_reason_compacts_pnlu_note() {
        assert_eq!(
            compact_environment_note("pnlu_fallback:stale_ts"),
            "pf_stale"
        );
        assert_eq!(
            compact_environment_note("pnlu_fallback:redis_error: timeout"),
            "pf_redis_err"
        );
    }
}
