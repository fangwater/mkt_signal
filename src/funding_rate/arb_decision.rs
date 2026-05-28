use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use iceoryx2::config::Config;
use iceoryx2::prelude::{Node, NodeBuilder, NodeName, ServiceName};
use iceoryx2::service::ipc;
use std::cell::{OnceCell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::OnceLock;
use std::thread::LocalKey;
use std::time::{Duration, Instant};

use crate::common::bbo::Bbo;
use crate::common::exchange::Exchange;
use crate::common::iceoryx_publisher::SignalPublisher;
use crate::common::iceoryx_subscriber::GenericSignalSubscriber;
use crate::common::ipc_service_name::build_service_name;
use crate::common::redis_client::RedisSettings;
use crate::common::symbol_util::{min_qty_symbol_key, normalize_symbol_for_venue};
use crate::common::tick_math::QuantizedValue;
use crate::common::time_util::get_timestamp_us;
use crate::funding_rate::FundingRatePeriod;
use crate::market_maker::order_align::align_order_for_venue;
use crate::pre_trade::order_manager::Side;
use crate::signal::arb_signal::{ArbBackwardQueryMsg, ArbCancelCandidateQueryMsg};
use crate::signal::common::{SignalBytes, TradingLeg, TradingVenue};
use crate::signal::hedge_signal::{ArbHedgeCtx, ArbHedgeSignalQueryMsg};
use crate::signal::open_signal::ArbOpenCtx;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::signal::venue_min_qty_table::VenueMinQtyTable;
use crate::strategy::inventory_hedge_quote_plan::{
    build_inventory_hedge_from_key, build_inventory_hedge_quote_plan,
    resolve_inventory_hedge_signal_inputs, InventoryHedgeBuildInput,
};
use crate::symbol_match::normalize_symbol_for_whitelist;

use super::arb_cooldown::is_cooldown_hit;
use super::arb_cooldown::threshold_key;
use super::arb_cooldown::update_last_ts;
use super::arb_mode::ArbMode;
use super::arb_open_filter::{
    lookup_factor_realtime_value, lookup_realtime_open_filter_value, select_factor_threshold,
};
use super::common::normalize_tlens_for_compare;
use super::common::Quote;
use super::common::{ArbDirection, OperationType, ThresholdKey, VenuePair};
use super::factor_value_hub::{EnvironmentSignalResult, FactorValueHub, FactorValueLookupResult};
use super::funding_rate_factor::FundingRateFactor;
use super::funding_threshold_loader::FundingThresholdsResolved;
use super::inline_volatility::{snapshot_inline_volatility, INLINE_VOLATILITY_MIN_SAMPLES};
use super::mkt_channel::MktChannel;
use super::model_output_hub::{ModelOutputHub, ModelOutputScoreLookupResult};
use super::rate_fetcher::RateFetcher;
use super::rolling_threshold_sync::StoredFactorChainEntry;

pub const DEFAULT_ARBITRAGE_SIGNAL_CHANNEL: &str = "trade_signal";
pub const DEFAULT_ARBITRAGE_BACKWARD_CHANNEL: &str = "trade_query";
pub const DEFAULT_PNLU_KEY_SUFFIX: &str = "_pnlu_factor_thresholds";
pub const DEFAULT_ENV_MODEL_TRUE_THRESHOLD: f64 = 0.0;
const TARGET_FACTOR_MAX_AGE_MS: i64 = 30_000;
const FUNDING_ARB_SHELL_NAME: &str = "ArbDecision(FundingArb)";
const SPREAD_ARB_SHELL_NAME: &str = "ArbDecision(SpreadArb)";
const MISSING_TICK_RELOAD_COOLDOWN_US: i64 = 5_000_000;
const ARB_CLOSE_MIN_NOTIONAL_U: f64 = 25.0;
// Arb hedge 是 open terminal 触发的高频对冲闭环，不走 MM hedge 的多档拆单模型；
// 单轮只出一笔，剩余 pending 会由后续触发继续滚动处理。
const ARB_HEDGE_ORDERS_PER_ROUND: u32 = 1;

fn arb_close_notional_meets_min(ctx: &ArbOpenCtx) -> bool {
    let notional = ctx.amount_value() * ctx.price_value();
    notional.is_finite() && notional >= ARB_CLOSE_MIN_NOTIONAL_U
}

fn is_corrupted_service_err(err_text: &str) -> bool {
    err_text.contains("ServiceInCorruptedState")
}

fn is_subscriber_capacity_err(err_text: &str) -> bool {
    err_text.contains("ExceedsMaxSupportedSubscribers")
}

pub fn default_pnlu_redis_settings() -> RedisSettings {
    RedisSettings {
        host: "127.0.0.1".to_string(),
        port: 6379,
        db: 0,
        username: None,
        password: None,
        prefix: None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum ArbSignalKind {
    ForwardOpen,
    ForwardClose,
    BackwardOpen,
    BackwardClose,
}

impl ArbSignalKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ForwardOpen => "FwdOpen",
            Self::ForwardClose => "FwdClose",
            Self::BackwardOpen => "BwdOpen",
            Self::BackwardClose => "BwdClose",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ArbFundingSignalFlags {
    pub period: FundingRatePeriod,
    pub forward_open: bool,
    pub forward_close: bool,
    pub backward_open: bool,
    pub backward_close: bool,
}

pub fn evaluate_funding_signal_flags(
    hedge_symbol: &str,
    hedge_venue: TradingVenue,
) -> ArbFundingSignalFlags {
    let fr_factor = FundingRateFactor::instance();
    let rate_fetcher = RateFetcher::instance();
    let period = rate_fetcher.get_period(hedge_symbol, hedge_venue);
    ArbFundingSignalFlags {
        period,
        forward_open: fr_factor.satisfy_forward_open(hedge_symbol, period, hedge_venue),
        forward_close: fr_factor.satisfy_forward_close(hedge_symbol, period, hedge_venue),
        backward_open: fr_factor.satisfy_backward_open(hedge_symbol, period, hedge_venue),
        backward_close: fr_factor.satisfy_backward_close(hedge_symbol, period, hedge_venue),
    }
}

pub fn resolve_funding_signal_from_flags(
    forward_open: bool,
    forward_close: bool,
    backward_open: bool,
    backward_close: bool,
) -> Option<ArbSignalKind> {
    if forward_close && backward_open {
        return Some(ArbSignalKind::BackwardOpen);
    }
    if backward_close && forward_open {
        return Some(ArbSignalKind::ForwardOpen);
    }
    if forward_close {
        return Some(ArbSignalKind::ForwardClose);
    }
    if backward_close {
        return Some(ArbSignalKind::BackwardClose);
    }
    if forward_open {
        return Some(ArbSignalKind::ForwardOpen);
    }
    if backward_open {
        return Some(ArbSignalKind::BackwardOpen);
    }
    None
}

pub fn evaluate_funding_signal(
    hedge_symbol: &str,
    hedge_venue: TradingVenue,
) -> Result<Option<ArbSignalKind>> {
    ArbDecision::evaluate_funding_rate_signal(hedge_symbol, hedge_venue)
}

pub fn normalize_arb_symbol_key(symbol: &str) -> String {
    normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures)
}

#[derive(Debug, Clone, Default)]
struct MissingTickReloadState {
    last_reload_by_key: HashMap<String, i64>,
}

impl MissingTickReloadState {
    fn should_reload(&mut self, venue: TradingVenue, symbol: &str, now_us: i64) -> bool {
        let key = format!(
            "{}:{}",
            venue.data_pub_slug(),
            normalize_arb_symbol_key(symbol)
        );
        let Some(last_us) = self.last_reload_by_key.get(&key).copied() else {
            self.last_reload_by_key.insert(key, now_us);
            return true;
        };
        if now_us.saturating_sub(last_us) >= MISSING_TICK_RELOAD_COOLDOWN_US {
            self.last_reload_by_key.insert(key, now_us);
            true
        } else {
            false
        }
    }
}

fn missing_tick_symbol(err: &str) -> Option<&str> {
    let rest = err.strip_prefix("missing tick for ")?;
    rest.split_whitespace()
        .next()
        .filter(|symbol| !symbol.is_empty())
}

fn trigger_missing_tick_table_reload(
    source: &'static str,
    runtime: &mut ArbShellRuntime,
    venue: TradingVenue,
    symbol: &str,
    err: &str,
) -> bool {
    let now_us = get_timestamp_us();
    if !runtime
        .missing_tick_reload_state
        .should_reload(venue, symbol, now_us)
    {
        return true;
    }

    log::warn!(
        "{source}: missing tick for symbol={} venue={:?}; suppressing repeats for {}s and refreshing venue filters err={}",
        symbol,
        venue,
        MISSING_TICK_RELOAD_COOLDOWN_US / 1_000_000,
        err
    );

    tokio::task::spawn_local(async move {
        let mut table = VenueMinQtyTable::new(venue);
        match table.refresh().await {
            Ok(()) => {
                let refreshed_venue = table.venue();
                let applied = apply_missing_tick_table_refresh(source, refreshed_venue, table);
                log::info!(
                    "{source}: missing tick refresh completed venue={:?} applied={}",
                    refreshed_venue,
                    applied
                );
            }
            Err(refresh_err) => {
                log::warn!(
                    "{source}: missing tick refresh failed venue={:?} err={:#}",
                    venue,
                    refresh_err
                );
            }
        }
    });

    true
}

fn apply_refreshed_table(
    runtime: &mut ArbShellRuntime,
    venue: TradingVenue,
    table: VenueMinQtyTable,
) -> bool {
    if venue == runtime.venues.0 {
        runtime.open_min_qty_table = table;
        true
    } else if venue == runtime.venues.1 {
        runtime.hedge_min_qty_table = table;
        true
    } else {
        false
    }
}

fn apply_missing_tick_table_refresh(
    source: &'static str,
    venue: TradingVenue,
    table: VenueMinQtyTable,
) -> bool {
    if source == FUNDING_ARB_SHELL_NAME {
        return FUNDING_ARB_SHELL.with(|cell| {
            let Some(shell) = cell.get() else {
                return false;
            };
            let mut shell = shell.borrow_mut();
            apply_refreshed_table(&mut shell.runtime, venue, table)
        });
    }
    if source == SPREAD_ARB_SHELL_NAME {
        return SPREAD_ARB_SHELL.with(|cell| {
            let Some(shell) = cell.get() else {
                return false;
            };
            let mut shell = shell.borrow_mut();
            apply_refreshed_table(&mut shell.runtime, venue, table)
        });
    }
    false
}

fn handle_quote_plan_error(
    source: &'static str,
    runtime: &mut ArbShellRuntime,
    venue: TradingVenue,
    symbol: &str,
    err: &str,
) -> bool {
    if let Some(missing_symbol) = missing_tick_symbol(err) {
        trigger_missing_tick_table_reload(source, runtime, venue, missing_symbol, err)
    } else {
        let _ = symbol;
        false
    }
}

pub fn with_thread_local_shell<T, F, R>(
    cell: &'static LocalKey<OnceCell<RefCell<T>>>,
    name: &str,
    f: F,
) -> R
where
    F: FnOnce(&T) -> R,
{
    cell.with(|cell| {
        let decision_ref = cell
            .get()
            .unwrap_or_else(|| panic!("{name} not initialized. Call init_singleton() first"));
        f(&decision_ref.borrow())
    })
}

pub fn with_thread_local_shell_mut<T, F, R>(
    cell: &'static LocalKey<OnceCell<RefCell<T>>>,
    name: &str,
    f: F,
) -> R
where
    F: FnOnce(&mut T) -> R,
{
    cell.with(|cell| {
        let decision_ref = cell
            .get()
            .unwrap_or_else(|| panic!("{name} not initialized. Call init_singleton() first"));
        f(&mut decision_ref.borrow_mut())
    })
}

pub fn try_with_thread_local_shell_mut<T, F, R>(
    cell: &'static LocalKey<OnceCell<RefCell<T>>>,
    f: F,
) -> Option<R>
where
    F: FnOnce(&mut T) -> R,
{
    cell.with(|cell| {
        let decision_ref = cell.get()?;
        Some(f(&mut decision_ref.borrow_mut()))
    })
}

pub fn init_thread_local_shell<T, F>(
    cell: &'static LocalKey<OnceCell<RefCell<T>>>,
    name: &str,
    build: F,
) -> Result<()>
where
    F: FnOnce() -> Result<T>,
{
    cell.with(|cell| {
        if cell.get().is_some() {
            return Ok(());
        }
        let decision = build()?;
        cell.set(RefCell::new(decision))
            .map_err(|_| anyhow::anyhow!("Failed to initialize {name} singleton"))?;
        Ok(())
    })
}

pub fn create_backward_subscriber(
    node: &Node<ipc::Service>,
    channel_name: &str,
    source: &str,
) -> Result<GenericSignalSubscriber> {
    let service_name = build_service_name(&format!("signal_pubs/{}", channel_name));
    let service_name_obj = ServiceName::new(&service_name)?;
    let service_builder = || {
        node.service_builder(&service_name_obj)
            .publish_subscribe::<[u8; crate::common::iceoryx_publisher::SIGNAL_PAYLOAD]>()
            .max_publishers(1)
            .max_subscribers(32)
            .history_size(128)
            .subscriber_max_buffer_size(256)
    };

    let service = match service_builder().open_or_create() {
        Ok(service) => service,
        Err(create_err) => {
            let create_text = format!("{:?}", create_err);
            if is_corrupted_service_err(&create_text) {
                log::warn!(
                    "{source}: backward subscriber hit ServiceInCorruptedState, attempting dead-node cleanup: service={} err={:?}",
                    service_name,
                    create_err
                );
                let cleanup = Node::<ipc::Service>::cleanup_dead_nodes(Config::global_config());
                log::warn!(
                    "{source}: dead-node cleanup completed for backward subscriber: service={} cleanups={} failed_cleanups={}",
                    service_name,
                    cleanup.cleanups,
                    cleanup.failed_cleanups
                );
                service_builder().open_or_create().map_err(|retry_err| {
                    anyhow!(
                        "{source}: failed to open/create backward signal service after dead-node cleanup: service={}, err={:?}, retry_err={:?}",
                        service_name,
                        create_err,
                        retry_err
                    )
                })?
            } else {
                return Err(create_err).with_context(|| {
                    format!(
                        "{source}: failed to open/create backward signal service={service_name}"
                    )
                });
            }
        }
    };

    let subscriber = match service.subscriber_builder().create() {
        Ok(subscriber) => subscriber,
        Err(err) => {
            let err_text = format!("{:?}", err);
            if is_subscriber_capacity_err(&err_text) {
                log::warn!(
                    "{source}: backward subscriber hit max-subscribers, attempting dead-node cleanup: service={} err={:?}",
                    service_name,
                    err
                );
                let cleanup = Node::<ipc::Service>::cleanup_dead_nodes(Config::global_config());
                log::warn!(
                    "{source}: dead-node cleanup completed for backward subscriber: service={} cleanups={} failed_cleanups={}",
                    service_name,
                    cleanup.cleanups,
                    cleanup.failed_cleanups
                );
                service.subscriber_builder().create().map_err(|retry_err| {
                    anyhow!(
                        "{source}: failed to create backward signal subscriber after dead-node cleanup: service={}, err={:?}, retry_err={:?}",
                        service_name,
                        err,
                        retry_err
                    )
                })?
            } else {
                return Err(err.into());
            }
        }
    };
    Ok(GenericSignalSubscriber::Size4K(subscriber))
}

pub fn create_arb_runtime_components(
    node_name: &str,
    source: &str,
    signal_channel: &str,
    backward_channel: &str,
    venues: VenuePair,
    pnlu_settings: RedisSettings,
    pnlu_key_suffix: String,
) -> Result<(
    Node<ipc::Service>,
    SignalPublisher,
    GenericSignalSubscriber,
    FactorValueHub,
    FactorValueHub,
)> {
    let node_name = NodeName::new(node_name)?;
    let node = NodeBuilder::new()
        .name(&node_name)
        .create::<ipc::Service>()?;
    let signal_pub = SignalPublisher::new(signal_channel)?;
    let backward_sub = create_backward_subscriber(&node, backward_channel, source)?;
    let open_factor_value_hub = FactorValueHub::new(
        &node,
        venues.0,
        venues.0,
        "rl_return_volatility",
        "rl_return_volatility",
        None,
        pnlu_settings.clone(),
        pnlu_key_suffix.clone(),
        30 * 60,
        TARGET_FACTOR_MAX_AGE_MS,
    )?;
    let hedge_factor_value_hub = FactorValueHub::new(
        &node,
        venues.0,
        venues.1,
        "rl_return_volatility",
        "rl_return_volatility",
        None,
        pnlu_settings,
        pnlu_key_suffix,
        30 * 60,
        TARGET_FACTOR_MAX_AGE_MS,
    )?;
    Ok((
        node,
        signal_pub,
        backward_sub,
        open_factor_value_hub,
        hedge_factor_value_hub,
    ))
}

pub struct ArbShellRuntime {
    pub signal_pub: SignalPublisher,
    pub backward_sub: GenericSignalSubscriber,
    pub node: Node<ipc::Service>,
    pub open_min_qty_table: VenueMinQtyTable,
    pub hedge_min_qty_table: VenueMinQtyTable,
    missing_tick_reload_state: MissingTickReloadState,
    pub venues: VenuePair,
    pub open_depth_query_client: crate::depth_pub::query_client::DepthQueryClient,
    pub hedge_depth_query_client: Option<crate::depth_pub::query_client::DepthQueryClient>,
}

pub fn create_shell_venue_resources(
    venues: VenuePair,
    include_hedge_depth: bool,
) -> Result<(
    VenueMinQtyTable,
    VenueMinQtyTable,
    crate::depth_pub::query_client::DepthQueryClient,
    Option<crate::depth_pub::query_client::DepthQueryClient>,
)> {
    let open_min_qty_table = VenueMinQtyTable::new(venues.0);
    let hedge_min_qty_table = VenueMinQtyTable::new(venues.1);
    let open_depth_query_client = crate::depth_pub::query_client::DepthQueryClient::new(venues.0)?;
    let hedge_depth_query_client = if include_hedge_depth {
        Some(crate::depth_pub::query_client::DepthQueryClient::new(
            venues.1,
        )?)
    } else {
        None
    };
    Ok((
        open_min_qty_table,
        hedge_min_qty_table,
        open_depth_query_client,
        hedge_depth_query_client,
    ))
}

pub fn create_arb_shell_runtime(
    node_name: &str,
    source: &str,
    signal_channel: &str,
    backward_channel: &str,
    venues: VenuePair,
    include_hedge_depth: bool,
    pnlu_settings: RedisSettings,
    pnlu_key_suffix: String,
) -> Result<(ArbShellRuntime, FactorValueHub, FactorValueHub)> {
    let (node, signal_pub, backward_sub, open_factor_value_hub, hedge_factor_value_hub) =
        create_arb_runtime_components(
            node_name,
            source,
            signal_channel,
            backward_channel,
            venues,
            pnlu_settings,
            pnlu_key_suffix,
        )?;
    let (
        open_min_qty_table,
        hedge_min_qty_table,
        open_depth_query_client,
        hedge_depth_query_client,
    ) = create_shell_venue_resources(venues, include_hedge_depth)?;
    Ok((
        ArbShellRuntime {
            signal_pub,
            backward_sub,
            node,
            open_min_qty_table,
            hedge_min_qty_table,
            missing_tick_reload_state: MissingTickReloadState::default(),
            venues,
            open_depth_query_client,
            hedge_depth_query_client,
        },
        open_factor_value_hub,
        hedge_factor_value_hub,
    ))
}

fn build_funding_arb_shell(venues: VenuePair) -> Result<FundingArbShell> {
    let pnlu_settings = default_pnlu_redis_settings();
    let (runtime, open_factor_value_hub, hedge_factor_value_hub) = create_arb_shell_runtime(
        "arb_funding_shell",
        FUNDING_ARB_SHELL_NAME,
        DEFAULT_ARBITRAGE_SIGNAL_CHANNEL,
        DEFAULT_ARBITRAGE_BACKWARD_CHANNEL,
        venues,
        false,
        pnlu_settings,
        DEFAULT_PNLU_KEY_SUFFIX.to_string(),
    )?;

    log_shell_runtime_ready(FUNDING_ARB_SHELL_NAME);

    let state = FundingArbShell { runtime };
    let _ = ArbDecision::with_state_mut(|arb| {
        arb.open_factor_value_hub = Some(open_factor_value_hub);
        arb.hedge_factor_value_hub = Some(hedge_factor_value_hub);
        arb.model_output_hub = Some(ModelOutputHub::new(venues.1));
        arb.apply_shared_bootstrap(ArbDecisionState::default_shared_bootstrap(1));
    });
    Ok(state)
}

fn build_spread_arb_shell(venues: VenuePair) -> Result<SpreadArbShell> {
    let pnlu_settings = default_pnlu_redis_settings();
    let pnlu_key_suffix = DEFAULT_PNLU_KEY_SUFFIX.to_string();
    let (runtime, open_factor_value_hub, hedge_factor_value_hub) = create_arb_shell_runtime(
        "arb_spread_shell",
        SPREAD_ARB_SHELL_NAME,
        DEFAULT_ARBITRAGE_SIGNAL_CHANNEL,
        DEFAULT_ARBITRAGE_BACKWARD_CHANNEL,
        venues,
        true,
        pnlu_settings.clone(),
        pnlu_key_suffix.clone(),
    )?;
    log_shell_runtime_ready(SPREAD_ARB_SHELL_NAME);

    let state = SpreadArbShell { runtime };
    let _ = ArbDecision::with_state_mut(|arb| {
        arb.open_factor_value_hub = Some(open_factor_value_hub);
        arb.hedge_factor_value_hub = Some(hedge_factor_value_hub);
        arb.model_output_hub = Some(ModelOutputHub::new(venues.1));
        arb.apply_shared_bootstrap(ArbDecisionState::default_shared_bootstrap(5));
        arb.enable_environment_model = true;
        arb.return_model_service = None;
        arb.environment_model_service = None;
        arb.environment_model_true_threshold = DEFAULT_ENV_MODEL_TRUE_THRESHOLD;
        arb.funding_open_thresholds = HashMap::new();
    });
    Ok(state)
}

struct FundingArbShell {
    pub(crate) runtime: ArbShellRuntime,
}

struct SpreadArbShell {
    pub(crate) runtime: ArbShellRuntime,
}

thread_local! {
    static FUNDING_ARB_SHELL: OnceCell<RefCell<FundingArbShell>> = const { OnceCell::new() };
    static SPREAD_ARB_SHELL: OnceCell<RefCell<SpreadArbShell>> = const { OnceCell::new() };
}

impl FundingArbShell {
    pub async fn init_singleton(exchange: Exchange) -> Result<()> {
        let venues = super::common::venue_pair_for_exchange(exchange);
        init_shell_runtime(
            &FUNDING_ARB_SHELL,
            FUNDING_ARB_SHELL_NAME,
            FUNDING_ARB_SHELL_NAME,
            venues,
            || build_funding_arb_shell(venues),
            |open_table, hedge_table| {
                with_thread_local_shell_mut(
                    &FUNDING_ARB_SHELL,
                    FUNDING_ARB_SHELL_NAME,
                    |decision| {
                        if let Some(open_table) = open_table {
                            decision.runtime.open_min_qty_table = open_table;
                        }
                        if let Some(hedge_table) = hedge_table {
                            decision.runtime.hedge_min_qty_table = hedge_table;
                        }
                    },
                );
            },
            || {
                FUNDING_ARB_SHELL.with(|cell| {
                    let decision_ref = cell.get();
                    if decision_ref.is_none() {
                        return false;
                    }
                    let mut decision = decision_ref.unwrap().borrow_mut();
                    let mut has_message = false;
                    loop {
                        match decision.runtime.backward_sub.receive_msg() {
                            Ok(Some(data)) => {
                                has_message = true;
                                if let Some(query) =
                                    dispatch_arb_backward_query(FUNDING_ARB_SHELL_NAME, data)
                                {
                                    match query {
                                        ArbBackwardQueryMsg::CancelCandidates(query) => {
                                            drive_funding_cancel_candidate_query(
                                                &mut decision,
                                                query,
                                            )
                                        }
                                        ArbBackwardQueryMsg::Hedge(query) => {
                                            drive_funding_arb_hedge_query(&mut decision, query)
                                        }
                                    }
                                }
                            }
                            Ok(None) => break,
                            Err(err) => {
                                log::warn!(
                                    "{FUNDING_ARB_SHELL_NAME}: backward_sub 接收错误: {}",
                                    err
                                );
                                break;
                            }
                        }
                    }
                    has_message
                })
            },
            |now_us| {
                try_with_thread_local_shell_mut(&FUNDING_ARB_SHELL, |decision| {
                    (
                        ArbDecision::with_state_mut(|arb| arb.tlen_threshold_reload_due(now_us))
                            .unwrap_or(true),
                        decision.runtime.venues.0,
                    )
                })
            },
            |_thresholds, _now_us| {},
        )
        .await?;

        log::info!(
            "{FUNDING_ARB_SHELL_NAME} singleton initialized, exchange={}",
            exchange
        );
        Ok(())
    }
}

impl SpreadArbShell {
    pub async fn init_singleton(open_venue: TradingVenue, hedge_venue: TradingVenue) -> Result<()> {
        let venues = (open_venue, hedge_venue);
        init_shell_runtime(
            &SPREAD_ARB_SHELL,
            SPREAD_ARB_SHELL_NAME,
            SPREAD_ARB_SHELL_NAME,
            venues,
            || build_spread_arb_shell(venues),
            |open_table, hedge_table| {
                with_thread_local_shell_mut(&SPREAD_ARB_SHELL, SPREAD_ARB_SHELL_NAME, |decision| {
                    if let Some(open_table) = open_table {
                        decision.runtime.open_min_qty_table = open_table;
                    }
                    if let Some(hedge_table) = hedge_table {
                        decision.runtime.hedge_min_qty_table = hedge_table;
                    }
                });
            },
            || {
                SPREAD_ARB_SHELL.with(|cell| {
                    let decision_ref = cell.get();
                    if decision_ref.is_none() {
                        return false;
                    }
                    let mut decision = decision_ref.unwrap().borrow_mut();
                    let mut has_message = false;
                    loop {
                        let _ = ArbDecision::with_state_mut(|arb| arb.poll_model_output_updates());
                        match decision.runtime.backward_sub.receive_msg() {
                            Ok(Some(data)) => {
                                has_message = true;
                                if let Some(query) =
                                    dispatch_arb_backward_query(SPREAD_ARB_SHELL_NAME, data)
                                {
                                    match query {
                                        ArbBackwardQueryMsg::CancelCandidates(query) => {
                                            drive_spread_arb_cancel_candidate_query(
                                                &mut decision,
                                                query,
                                            )
                                        }
                                        ArbBackwardQueryMsg::Hedge(query) => {
                                            drive_spread_arb_hedge_query(&mut decision, query)
                                        }
                                    }
                                }
                            }
                            Ok(None) => break,
                            Err(err) => {
                                log::warn!(
                                    "{SPREAD_ARB_SHELL_NAME}: backward_sub 接收错误: {}",
                                    err
                                );
                                break;
                            }
                        }
                    }
                    has_message
                })
            },
            |now_us| {
                try_with_thread_local_shell_mut(&SPREAD_ARB_SHELL, |decision| {
                    (
                        ArbDecision::with_state_mut(|arb| arb.tlen_threshold_reload_due(now_us))
                            .unwrap_or(false),
                        decision.runtime.venues.0,
                    )
                })
            },
            |_thresholds, _now_us| {},
        )
        .await?;

        log::info!(
            "{SPREAD_ARB_SHELL_NAME} singleton initialized, open={:?} hedge={:?}",
            open_venue,
            hedge_venue
        );
        Ok(())
    }
}

pub fn spawn_backward_listener_loop<F>(source: &'static str, mut drain_pending: F)
where
    F: FnMut() -> bool + 'static,
{
    tokio::task::spawn_local(async move {
        log::info!("{source} backward listener started");
        loop {
            let has_message = drain_pending();
            if !has_message {
                tokio::task::yield_now().await;
            }
        }
    });
}

pub fn spawn_tlen_threshold_reload_loop<FGet, FSet>(
    source: &'static str,
    mut get_state: FGet,
    mut update_local: FSet,
) where
    FGet: FnMut(i64) -> Option<(bool, TradingVenue)> + 'static,
    FSet: FnMut(HashMap<String, f64>, i64) + 'static,
{
    tokio::task::spawn_local(async move {
        let redis = crate::common::redis_client::RedisSettings::default();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            let now_us = get_timestamp_us();
            let Some((due, open_venue)) = get_state(now_us) else {
                continue;
            };
            if !due {
                continue;
            }
            match super::tlen_threshold_loader::load_from_redis(&redis, open_venue).await {
                Ok((redis_key, thresholds, bad_fields)) => {
                    let symbols = thresholds.len();
                    let cloned = thresholds.clone();
                    update_local(cloned, now_us);
                    let _ = ArbDecision::with_state_mut(|arb| {
                        arb.tlen_thresholds = thresholds;
                        arb.last_tlen_threshold_reload_ts_us = now_us;
                    });
                    log::info!(
                        "{source}: tlen thresholds loaded key={} symbols={} bad_fields={}",
                        redis_key,
                        symbols,
                        bad_fields
                    );
                }
                Err(err) => log::warn!(
                    "{source}: tlen threshold reload failed venue={:?} err={:#}",
                    open_venue,
                    err
                ),
            }
        }
    });
}

pub async fn refresh_min_qty_tables<F>(source: &str, venues: VenuePair, mut apply: F)
where
    F: FnMut(Option<VenueMinQtyTable>, Option<VenueMinQtyTable>),
{
    let mut open_table = VenueMinQtyTable::new(venues.0);
    let mut hedge_table = VenueMinQtyTable::new(venues.1);

    let open_res = open_table.refresh().await;
    let hedge_res = hedge_table.refresh().await;

    apply(
        if open_res.is_ok() {
            Some(open_table)
        } else {
            None
        },
        if hedge_res.is_ok() {
            Some(hedge_table)
        } else {
            None
        },
    );

    match open_res {
        Ok(_) => log::info!("{source}: open venue min_qty_table loaded, venue={:?}", venues.0),
        Err(err) => log::warn!(
            "{source}: failed to refresh open venue filters for {:?}, price_tick may be zero: {err:#}",
            venues.0
        ),
    }
    match hedge_res {
        Ok(_) => log::info!("{source}: hedge venue min_qty_table loaded, venue={:?}", venues.1),
        Err(err) => log::warn!(
            "{source}: failed to refresh hedge venue filters for {:?}, price_tick may be zero: {err:#}",
            venues.1
        ),
    }
}

pub async fn attach_runtime_shell<FApply, FReceive, FReloadGet, FReloadSet>(
    source: &'static str,
    venues: VenuePair,
    apply_tables: FApply,
    drain_pending: FReceive,
    reload_state: FReloadGet,
    reload_set_local: FReloadSet,
) where
    FApply: FnMut(Option<VenueMinQtyTable>, Option<VenueMinQtyTable>),
    FReceive: FnMut() -> bool + 'static,
    FReloadGet: FnMut(i64) -> Option<(bool, TradingVenue)> + 'static,
    FReloadSet: FnMut(HashMap<String, f64>, i64) + 'static,
{
    refresh_min_qty_tables(source, venues, apply_tables).await;
    spawn_backward_listener_loop(source, drain_pending);
    spawn_tlen_threshold_reload_loop(source, reload_state, reload_set_local);
    log::info!("{source} backward listener started");
}

pub async fn init_shell_runtime<T, FBuild, FApply, FReceive, FReloadGet, FReloadSet>(
    cell: &'static LocalKey<OnceCell<RefCell<T>>>,
    name: &'static str,
    source: &'static str,
    venues: VenuePair,
    build: FBuild,
    apply_tables: FApply,
    drain_pending: FReceive,
    reload_state: FReloadGet,
    reload_set_local: FReloadSet,
) -> Result<()>
where
    FBuild: FnOnce() -> Result<T>,
    FApply: FnMut(Option<VenueMinQtyTable>, Option<VenueMinQtyTable>),
    FReceive: FnMut() -> bool + 'static,
    FReloadGet: FnMut(i64) -> Option<(bool, TradingVenue)> + 'static,
    FReloadSet: FnMut(HashMap<String, f64>, i64) + 'static,
{
    let result = init_thread_local_shell(cell, name, build);
    if result.is_ok() {
        log::info!("{name} singleton initialized");
    }
    result?;
    attach_runtime_shell(
        source,
        venues,
        apply_tables,
        drain_pending,
        reload_state,
        reload_set_local,
    )
    .await;
    Ok(())
}

pub fn update_model_output_services_for_arb(node: &Node<ipc::Service>, services: Vec<String>) {
    let _ = ArbDecision::with_state_mut(|arb| {
        if let Some(hub) = arb.model_output_hub.as_mut() {
            hub.update_services(node, services);
        }
    });
}

pub fn try_update_arb_model_output_services(services: Vec<String>) -> bool {
    if try_with_thread_local_shell_mut(&SPREAD_ARB_SHELL, |decision| {
        update_model_output_services_for_arb(&decision.runtime.node, services.clone());
    })
    .is_some()
    {
        return true;
    }
    try_with_thread_local_shell_mut(&FUNDING_ARB_SHELL, |decision| {
        update_model_output_services_for_arb(&decision.runtime.node, services);
    })
    .is_some()
}

fn log_shell_runtime_ready(source: &str) {
    log::info!(
        "{source}: signal publisher created on '{}'",
        DEFAULT_ARBITRAGE_SIGNAL_CHANNEL
    );
    log::info!(
        "{source}: backward subscriber created on '{}'",
        DEFAULT_ARBITRAGE_BACKWARD_CHANNEL
    );
}

pub fn funding_open_inputs_ready(hedge_symbol: &str, hedge_venue: TradingVenue) -> bool {
    let rate_fetcher = RateFetcher::instance();
    let has_predict_fr = rate_fetcher
        .get_predicted_funding_rate(hedge_symbol, hedge_venue)
        .is_some();
    let has_predict_loan = rate_fetcher
        .get_predict_loan_rate(hedge_symbol, hedge_venue)
        .is_some();
    has_predict_fr && has_predict_loan
}

pub fn funding_rate_symbol_inputs_ready(hedge_symbol: &str, hedge_venue: TradingVenue) -> bool {
    let rate_fetcher = RateFetcher::instance();
    let missing_reason = if rate_fetcher
        .get_predicted_funding_rate(hedge_symbol, hedge_venue)
        .is_none()
    {
        Some("missing_predict_fr")
    } else if rate_fetcher
        .get_predict_loan_rate(hedge_symbol, hedge_venue)
        .is_none()
    {
        Some("missing_predict_loan")
    } else if !MktChannel::is_initialized()
        || MktChannel::instance()
            .get_funding_rate_mean(hedge_symbol, hedge_venue)
            .is_none()
    {
        Some("missing_current_fr_ma")
    } else if rate_fetcher
        .get_current_loan_rate(hedge_symbol, hedge_venue)
        .is_none()
    {
        Some("missing_current_loan")
    } else {
        None
    };

    if let Some(reason) = missing_reason {
        RateFetcher::mark_missing(hedge_venue, hedge_symbol, reason);
        return false;
    }

    RateFetcher::mark_available(hedge_venue, hedge_symbol);
    true
}

pub fn dispatch_arb_backward_query(source: &str, data: Bytes) -> Option<ArbBackwardQueryMsg> {
    match ArbBackwardQueryMsg::from_bytes(data) {
        Ok(query) => Some(query),
        Err(err) => {
            log::warn!("{source}: parse backward query failed: {err}");
            None
        }
    }
}

pub fn evaluate_funding_mode_signal(
    spread_factor: &super::spread_factor::SpreadFactor,
    open_symbol_key: &str,
    hedge_symbol_key: &str,
    hedge_venue_symbol: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    in_dump: bool,
    rate_ready: bool,
) -> Result<Option<ArbSignalKind>> {
    let spread_close_signal = || {
        ArbDecisionState::evaluate_close_side(
            spread_factor,
            open_symbol_key,
            hedge_symbol_key,
            open_venue,
            hedge_venue,
        )
        .map(|side| match side {
            Side::Sell => ArbSignalKind::ForwardClose,
            Side::Buy => ArbSignalKind::BackwardClose,
        })
    };

    if in_dump {
        let signal = spread_close_signal();
        if signal.is_none() {
            log::debug!(
                "ArbDecision funding mode dump close not satisfied open={} hedge={} open_venue={:?} hedge_venue={:?}",
                open_symbol_key,
                hedge_symbol_key,
                open_venue,
                hedge_venue
            );
        }
        return Ok(signal);
    }

    if !rate_ready {
        log::debug!(
            "ArbDecision funding mode rate not fully ready, suppress non-dump signal open={} hedge={} open_venue={:?} hedge_venue={:?}",
            open_symbol_key,
            hedge_symbol_key,
            open_venue,
            hedge_venue
        );
        return Ok(None);
    }
    evaluate_funding_signal(hedge_venue_symbol, hedge_venue)
}

fn drive_funding_decision(
    decision: &mut FundingArbShell,
    open_symbol: &str,
    hedge_symbol: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Result<Option<SignalType>> {
    let _ = ArbDecision::with_state_mut(|arb| {
        arb.maybe_log_intercept_summary(FUNDING_ARB_SHELL_NAME);
    });
    let spread_factor = super::spread_factor::SpreadFactor::instance();
    let now = get_timestamp_us();
    let symbol_list = super::symbol_list::SymbolList::instance();
    let open_symbol_key = normalize_arb_symbol_key(open_symbol);
    let hedge_symbol_key = normalize_arb_symbol_key(hedge_symbol);
    // 心跳计数：活跃 symbol 每 tick 记一条 fr_seen，保证 summary 不会空表早退。
    if symbol_list.is_in_fwd_trade_list(open_symbol_key.as_str())
        || symbol_list.is_in_bwd_trade_list(open_symbol_key.as_str())
        || symbol_list.is_in_dump_list(open_symbol_key.as_str())
    {
        let _ = ArbDecision::with_state_mut(|arb| {
            arb.record_intercept_summary("fr_seen");
        });
    }
    if log::log_enabled!(log::Level::Debug) {
        log::debug!(
            "{FUNDING_ARB_SHELL_NAME} decision start open={} hedge={} open_venue={:?} hedge_venue={:?}",
            open_symbol_key,
            hedge_symbol_key,
            open_venue,
            hedge_venue
        );
    }

    let in_dump = symbol_list.is_in_dump_list(open_symbol_key.as_str());
    if in_dump {
        if let Some(CloseGateOutcome::Pass(close_gate)) = ArbDecision::with_state_mut(|arb| {
            arb.evaluate_close_gate(
                spread_factor,
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                open_venue,
                hedge_venue,
                now,
            )
        }) {
            let emit_allowed = emit_funding_open_close_signals(
                decision,
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                open_venue,
                hedge_venue,
                SignalType::ArbClose,
                close_gate.side,
                None,
            )?;
            if !emit_allowed {
                return Ok(None);
            }
            let _ = ArbDecision::with_state_mut(|arb| {
                arb.mark_close_triggered(close_gate.key, close_gate.side, now);
            });
            return Ok(Some(SignalType::ArbClose));
        }
    }

    let forward_cancel = spread_factor.satisfy_forward_cancel(
        open_venue,
        open_symbol_key.as_str(),
        hedge_venue,
        hedge_symbol_key.as_str(),
    );
    let backward_cancel = spread_factor.satisfy_backward_cancel(
        open_venue,
        open_symbol_key.as_str(),
        hedge_venue,
        hedge_symbol_key.as_str(),
    );
    // Cancel fwd/bwd 拆开独立处理（参考 drive_spread_arb_decision 同结构）：
    // - 各自只在本方向触发时发对应 Side 的 ArbCancel
    // - 不早退，落到 open 评估让本 tick 还能开新仓
    let mut emitted_signal: Option<SignalType> = None;
    if forward_cancel {
        if let Some(cancel_gate) = ArbDecision::with_state_mut(|arb| {
            arb.evaluate_cancel_gate(
                true,
                false,
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                open_venue,
                hedge_venue,
            )
        })
        .flatten()
        {
            log::debug!(
                "{FUNDING_ARB_SHELL_NAME} fwd cancel triggered open={} hedge={} open_venue={:?} hedge_venue={:?}",
                open_symbol_key,
                hedge_symbol_key,
                open_venue,
                hedge_venue
            );
            emit_funding_spread_cancel(
                decision,
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                open_venue,
                hedge_venue,
                Side::Buy,
            )?;
            let _ = ArbDecision::with_state_mut(|arb| {
                arb.record_intercept_summary("cancel_fwd");
                arb.mark_signal_triggered(&SignalType::ArbCancel, cancel_gate.key, Side::Buy, now);
            });
            emitted_signal = Some(SignalType::ArbCancel);
        }
    }
    if backward_cancel {
        if let Some(cancel_gate) = ArbDecision::with_state_mut(|arb| {
            arb.evaluate_cancel_gate(
                false,
                true,
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                open_venue,
                hedge_venue,
            )
        })
        .flatten()
        {
            log::debug!(
                "{FUNDING_ARB_SHELL_NAME} bwd cancel triggered open={} hedge={} open_venue={:?} hedge_venue={:?}",
                open_symbol_key,
                hedge_symbol_key,
                open_venue,
                hedge_venue
            );
            emit_funding_spread_cancel(
                decision,
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                open_venue,
                hedge_venue,
                Side::Sell,
            )?;
            let _ = ArbDecision::with_state_mut(|arb| {
                arb.record_intercept_summary("cancel_bwd");
                arb.mark_signal_triggered(&SignalType::ArbCancel, cancel_gate.key, Side::Sell, now);
            });
            emitted_signal = Some(SignalType::ArbCancel);
        }
    }

    if in_dump {
        return Ok(emitted_signal);
    }

    let rate_ready = funding_rate_symbol_inputs_ready(hedge_symbol_key.as_str(), hedge_venue);
    let open_inputs_ready = funding_open_inputs_ready(hedge_symbol_key.as_str(), hedge_venue);
    let fr_signal = evaluate_funding_mode_signal(
        spread_factor,
        open_symbol_key.as_str(),
        hedge_symbol_key.as_str(),
        hedge_symbol_key.as_str(),
        open_venue,
        hedge_venue,
        in_dump,
        rate_ready,
    )?;

    // FR 阈值判定结果细分：summary 里区分出哪条信号被触发、还是全 miss。
    // 仅活跃 symbol 记，避免 6300/30s 的全量 trigger 噪音。
    {
        let in_active = symbol_list.is_in_fwd_trade_list(open_symbol_key.as_str())
            || symbol_list.is_in_bwd_trade_list(open_symbol_key.as_str())
            || symbol_list.is_in_dump_list(open_symbol_key.as_str());
        if in_active {
            let flags = evaluate_funding_signal_flags(hedge_symbol_key.as_str(), hedge_venue);
            let _ = ArbDecision::with_state_mut(|arb| {
                if !rate_ready {
                    arb.record_intercept_summary("fr_rate_not_ready");
                }
                if !open_inputs_ready {
                    arb.record_intercept_summary("fr_inputs_not_ready");
                }
                if flags.forward_open {
                    arb.record_intercept_summary("fr_fwd_open_hit");
                }
                if flags.backward_open {
                    arb.record_intercept_summary("fr_bwd_open_hit");
                }
                if flags.forward_close {
                    arb.record_intercept_summary("fr_fwd_close_hit");
                }
                if flags.backward_close {
                    arb.record_intercept_summary("fr_bwd_close_hit");
                }
                if !flags.forward_open
                    && !flags.backward_open
                    && !flags.forward_close
                    && !flags.backward_close
                {
                    arb.record_intercept_summary("fr_no_threshold_hit");
                }
                if fr_signal.is_none() {
                    arb.record_intercept_summary("fr_no_signal");
                }
            });
        }
    }

    let fr_signal = match fr_signal {
        Some(signal) => signal,
        None => {
            log::debug!(
                "{FUNDING_ARB_SHELL_NAME} no effective signal open={} hedge={} open_venue={:?} hedge_venue={:?}",
                open_symbol_key,
                hedge_symbol_key,
                open_venue,
                hedge_venue
            );
            return Ok(emitted_signal);
        }
    };

    // 此处 fr_signal 已是 Some(ArbSignalKind)。evaluate_funding_final_signal 还会过
    // open_inputs_ready / in_dump / spread / in_trade_list 四关。把每一关的拦截原因
    // 计数到 summary 里，方便定位是哪一关在挡 signal。
    {
        let reason: Option<&'static str> = match fr_signal {
            ArbSignalKind::ForwardOpen => {
                if !open_inputs_ready {
                    Some("final_inputs_not_ready")
                } else if in_dump {
                    Some("final_in_dump")
                } else if !spread_factor.satisfy_forward_open(
                    open_venue,
                    open_symbol_key.as_str(),
                    hedge_venue,
                    hedge_symbol_key.as_str(),
                ) {
                    Some("final_fwd_spread_miss")
                } else if !symbol_list.is_in_fwd_trade_list(open_symbol_key.as_str()) {
                    Some("final_not_in_fwd_list")
                } else {
                    None
                }
            }
            ArbSignalKind::BackwardOpen => {
                if !open_inputs_ready {
                    Some("final_inputs_not_ready")
                } else if in_dump {
                    Some("final_in_dump")
                } else if !spread_factor.satisfy_backward_open(
                    open_venue,
                    open_symbol_key.as_str(),
                    hedge_venue,
                    hedge_symbol_key.as_str(),
                ) {
                    Some("final_bwd_spread_miss")
                } else if !symbol_list.is_in_bwd_trade_list(open_symbol_key.as_str()) {
                    Some("final_not_in_bwd_list")
                } else {
                    None
                }
            }
            ArbSignalKind::ForwardClose => {
                if !spread_factor.satisfy_forward_close(
                    open_venue,
                    open_symbol_key.as_str(),
                    hedge_venue,
                    hedge_symbol_key.as_str(),
                ) {
                    Some("final_fwd_close_spread_miss")
                } else {
                    None
                }
            }
            ArbSignalKind::BackwardClose => {
                if !spread_factor.satisfy_backward_close(
                    open_venue,
                    open_symbol_key.as_str(),
                    hedge_venue,
                    hedge_symbol_key.as_str(),
                ) {
                    Some("final_bwd_close_spread_miss")
                } else {
                    None
                }
            }
        };
        let _ = ArbDecision::with_state_mut(|arb| match reason {
            Some(r) => arb.record_intercept_summary(r),
            None => arb.record_intercept_summary("final_pass"),
        });
    }

    let Some(control) = ArbDecision::with_state_mut(|arb| {
        arb.evaluate_funding_control(
            fr_signal,
            spread_factor,
            &symbol_list,
            open_symbol_key.as_str(),
            hedge_symbol_key.as_str(),
            open_venue,
            hedge_venue,
            open_inputs_ready,
            in_dump,
            now,
        )
    })
    .flatten() else {
        return Ok(emitted_signal);
    };

    let emit_allowed = emit_funding_open_close_signals(
        decision,
        open_symbol_key.as_str(),
        hedge_symbol_key.as_str(),
        open_venue,
        hedge_venue,
        control.final_signal.clone(),
        control.side,
        control.gate.as_ref(),
    )?;
    if !emit_allowed {
        return Ok(emitted_signal);
    }
    let _ = ArbDecision::with_state_mut(|arb| {
        arb.mark_signal_triggered(&control.final_signal, control.key, control.side, now);
    });

    Ok(Some(control.final_signal))
}

fn drive_spread_arb_decision(
    decision: &mut SpreadArbShell,
    open_symbol: &str,
    hedge_symbol: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Result<Option<SignalType>> {
    let _ = ArbDecision::with_state_mut(|arb| {
        arb.maybe_log_intercept_summary(SPREAD_ARB_SHELL_NAME);
    });
    let _ = ArbDecision::with_state_mut(|arb| arb.poll_model_output_updates());
    let spread_factor = super::spread_factor::SpreadFactor::instance();
    let now = get_timestamp_us();
    let symbol_list = super::symbol_list::SymbolList::instance();
    let open_symbol_key = normalize_arb_symbol_key(open_symbol);
    let hedge_symbol_key = normalize_arb_symbol_key(hedge_symbol);
    if symbol_list.is_in_fwd_trade_list(open_symbol_key.as_str())
        || symbol_list.is_in_bwd_trade_list(open_symbol_key.as_str())
        || symbol_list.is_in_dump_list(open_symbol_key.as_str())
    {
        let _ = ArbDecision::with_state_mut(|arb| {
            let _ = spread_factor.satisfy_forward_open(
                open_venue,
                open_symbol_key.as_str(),
                hedge_venue,
                hedge_symbol_key.as_str(),
            );
            let _ = spread_factor.satisfy_backward_open(
                open_venue,
                open_symbol_key.as_str(),
                hedge_venue,
                hedge_symbol_key.as_str(),
            );
            arb.record_intercept_summary("spread_seen");
            // 记录本 tick 的 forward/backward open 实际值与阈值，summary 表后用于
            // 打印 spread 观测带，直观核对"信号设计是否合理"。
            if let Some((value, threshold, _, _)) = spread_factor.get_spread_check_detail(
                open_venue,
                open_symbol_key.as_str(),
                hedge_venue,
                hedge_symbol_key.as_str(),
                ArbDirection::Forward,
                OperationType::Open,
            ) {
                arb.record_spread_observation_fwd(open_symbol_key.as_str(), value, threshold);
            }
            if let Some((value, threshold, _, _)) = spread_factor.get_spread_check_detail(
                open_venue,
                open_symbol_key.as_str(),
                hedge_venue,
                hedge_symbol_key.as_str(),
                ArbDirection::Backward,
                OperationType::Open,
            ) {
                arb.record_spread_observation_bwd(open_symbol_key.as_str(), value, threshold);
            }
        });
    }

    let mut emitted_signal: Option<SignalType> = None;
    let in_dump = symbol_list.is_in_dump_list(open_symbol_key.as_str());
    if in_dump {
        if let Some(CloseGateOutcome::Pass(close_gate)) = ArbDecision::with_state_mut(|arb| {
            arb.evaluate_close_gate(
                spread_factor,
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                open_venue,
                hedge_venue,
                now,
            )
        }) {
            let _ = ArbDecision::with_state_mut(|arb| {
                arb.record_intercept_summary("spread_close");
            });
            let emit_allowed = emit_spread_arb_close_signals(
                decision,
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                open_venue,
                hedge_venue,
                close_gate.side,
            )?;
            if !emit_allowed {
                return Ok(None);
            }
            let _ = ArbDecision::with_state_mut(|arb| {
                arb.mark_close_triggered(close_gate.key, close_gate.side, now);
            });
            return Ok(Some(SignalType::ArbClose));
        }
    }

    let forward_cancel = spread_factor.satisfy_forward_cancel(
        open_venue,
        open_symbol_key.as_str(),
        hedge_venue,
        hedge_symbol_key.as_str(),
    );
    let backward_cancel = spread_factor.satisfy_backward_cancel(
        open_venue,
        open_symbol_key.as_str(),
        hedge_venue,
        hedge_symbol_key.as_str(),
    );
    if forward_cancel {
        let Some(cancel_gate) = ArbDecision::with_state_mut(|arb| {
            arb.evaluate_cancel_gate(
                true,
                false,
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                open_venue,
                hedge_venue,
            )
        })
        .flatten() else {
            return Ok(emitted_signal);
        };
        let cancel_cooldown_hit = ArbDecision::with_state_mut(|arb| {
            arb.is_spread_cancel_cooldown_hit(&cancel_gate.key, Side::Buy, now)
        })
        .unwrap_or(false);
        if cancel_cooldown_hit {
            let _ = ArbDecision::with_state_mut(|arb| {
                arb.record_intercept_summary("spread_cancel_cooldown");
            });
        } else {
            let _ = ArbDecision::with_state_mut(|arb| {
                arb.record_intercept_summary("spread_cancel");
            });
            emit_spread_arb_spread_cancel(
                decision,
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                open_venue,
                hedge_venue,
                Side::Buy,
            )?;
            let cancel_key = cancel_gate.key.clone();
            let _ = ArbDecision::with_state_mut(|arb| {
                arb.mark_signal_triggered(&SignalType::ArbCancel, cancel_gate.key, Side::Buy, now);
                arb.mark_spread_cancel_triggered(cancel_key, Side::Buy, now);
            });
            emitted_signal = Some(SignalType::ArbCancel);
        }
    }

    if backward_cancel {
        let Some(cancel_gate) = ArbDecision::with_state_mut(|arb| {
            arb.evaluate_cancel_gate(
                false,
                true,
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                open_venue,
                hedge_venue,
            )
        })
        .flatten() else {
            return Ok(emitted_signal);
        };
        let cancel_cooldown_hit = ArbDecision::with_state_mut(|arb| {
            arb.is_spread_cancel_cooldown_hit(&cancel_gate.key, Side::Sell, now)
        })
        .unwrap_or(false);
        if cancel_cooldown_hit {
            let _ = ArbDecision::with_state_mut(|arb| {
                arb.record_intercept_summary("spread_cancel_cooldown");
            });
        } else {
            let _ = ArbDecision::with_state_mut(|arb| {
                arb.record_intercept_summary("spread_cancel");
            });
            emit_spread_arb_spread_cancel(
                decision,
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                open_venue,
                hedge_venue,
                Side::Sell,
            )?;
            let cancel_key = cancel_gate.key.clone();
            let _ = ArbDecision::with_state_mut(|arb| {
                arb.mark_signal_triggered(&SignalType::ArbCancel, cancel_gate.key, Side::Sell, now);
                arb.mark_spread_cancel_triggered(cancel_key, Side::Sell, now);
            });
            emitted_signal = Some(SignalType::ArbCancel);
        }
    }

    if in_dump {
        return Ok(emitted_signal);
    }

    let Some(open_control) = ArbDecision::with_state_mut(|arb| {
        arb.evaluate_open_control(
            spread_factor,
            &symbol_list,
            open_symbol_key.as_str(),
            hedge_symbol_key.as_str(),
            hedge_symbol,
            open_venue,
            hedge_venue,
            now,
            in_dump,
        )
    })
    .flatten() else {
        return Ok(emitted_signal);
    };

    emit_spread_arb_open_signals(
        decision,
        open_symbol_key.as_str(),
        hedge_symbol_key.as_str(),
        open_venue,
        hedge_venue,
        open_control.side,
        open_control.gate.return_qtl,
        open_control.gate.return_threshold,
        open_control.gate.open_filter_value,
        open_control.gate.environment_score,
        open_control.gate.environment_threshold,
        open_control.gate.open_volatility_factor,
    )?;
    let _ = ArbDecision::with_state_mut(|arb| {
        arb.mark_open_triggered(open_control.key, open_control.side, now);
    });
    Ok(Some(SignalType::ArbOpen))
}

fn drive_funding_arb_hedge_query(decision: &mut FundingArbShell, query: ArbHedgeSignalQueryMsg) {
    drive_shared_arb_hedge_query(FUNDING_ARB_SHELL_NAME, &decision.runtime, query);
}

fn drive_spread_arb_hedge_query(decision: &mut SpreadArbShell, query: ArbHedgeSignalQueryMsg) {
    drive_shared_arb_hedge_query(SPREAD_ARB_SHELL_NAME, &decision.runtime, query);
}

#[derive(Debug, Clone)]
struct ArbHedgeBuildParams {
    signal: f64,
    signal_qtl: Option<f64>,
    volatility: f64,
    amount_cap_u: f64,
    hedge_vol_multiplier: f64,
    hedge_offset_ratio: f64,
    hedge_price_offset_limit_lower: f64,
    hedge_price_offset_limit_upper: f64,
    hedge_window_scale_low: f64,
    hedge_window_scale_high: f64,
    hedge_timeout_mm_us: i64,
    max_hedge_price_pct_change: f64,
    enable_return_score_adjust_hedge: bool,
}

fn resolve_arb_hedge_build_params(
    source: &'static str,
    symbol: &str,
    hedge_venue: TradingVenue,
) -> Option<ArbHedgeBuildParams> {
    match ArbDecision::with_state_mut(|arb| {
        let model_service = arb
            .return_model_service
            .clone()
            .ok_or_else(|| "return_model_service unavailable".to_string())?;
        let enable_return_score_adjust_hedge = arb.enable_return_score_adjust_hedge;
        let amount_cap_u = arb.resolve_order_amount_u(symbol);
        let hedge_vol_multiplier = arb.hedge_vol_multiplier;
        let hedge_offset_ratio = arb.hedge_offset_ratio;
        let (hedge_price_offset_limit_lower, hedge_price_offset_limit_upper) =
            arb.resolve_hedge_price_offset_limits(symbol);
        let hedge_window_scale_low = arb.hedge_window_scale_low;
        let hedge_window_scale_high = arb.hedge_window_scale_high;
        let hedge_timeout_mm_us = arb.hedge_timeout_mm_us;
        let max_hedge_price_pct_change = arb.max_hedge_price_pct_change;
        let hedge_factor_value_hub = arb
            .hedge_factor_value_hub
            .as_mut()
            .ok_or_else(|| "hedge_factor_value_hub unavailable".to_string())?;
        let model_output_hub = arb
            .model_output_hub
            .as_mut()
            .ok_or_else(|| "model_output_hub unavailable".to_string())?;
        let (signal, signal_qtl, volatility) = resolve_inventory_hedge_signal_inputs(
            hedge_factor_value_hub,
            model_output_hub,
            &model_service,
            symbol,
            hedge_venue,
            enable_return_score_adjust_hedge,
        )?;
        Ok::<_, String>(ArbHedgeBuildParams {
            signal,
            signal_qtl,
            volatility,
            amount_cap_u,
            hedge_vol_multiplier,
            hedge_offset_ratio,
            hedge_price_offset_limit_lower,
            hedge_price_offset_limit_upper,
            hedge_window_scale_low,
            hedge_window_scale_high,
            hedge_timeout_mm_us,
            max_hedge_price_pct_change,
            enable_return_score_adjust_hedge,
        })
    }) {
        Some(Ok(params)) => Some(params),
        Some(Err(err)) => {
            log::warn!(
                "{source}: ArbHedge factor lookup failed symbol={} venue={:?} err={}",
                symbol,
                hedge_venue,
                err
            );
            None
        }
        None => {
            log::warn!(
                "{source}: ArbHedge factor lookup failed symbol={} venue={:?} err=ArbDecisionState unavailable",
                symbol,
                hedge_venue
            );
            None
        }
    }
}

/// 全局开关：env `ARB_HEDGE_FORCE_TAKER=1` 时，arb hedge 始终以 taker 报单
/// （price=0、offset=0、exp_time=0），且跳过 model/factor lookup + offset plan。
/// 进程启动时读一次，缓存在 OnceLock。
static ARB_HEDGE_FORCE_TAKER: OnceLock<bool> = OnceLock::new();

fn arb_hedge_force_taker() -> bool {
    *ARB_HEDGE_FORCE_TAKER.get_or_init(|| {
        let enabled = std::env::var("ARB_HEDGE_FORCE_TAKER")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "True" | "on" | "ON"))
            .unwrap_or(false);
        if enabled {
            log::warn!(
                "ARB_HEDGE_FORCE_TAKER=on: arb hedge will always submit as taker \
                 (price=0, offset=0, exp_time=0); model + offset plan bypassed"
            );
        }
        enabled
    })
}

fn should_use_arb_hedge_taker(
    weighted_inventory_price: f64,
    bid: f64,
    ask: f64,
    threshold_pct: f64,
) -> Option<(f64, f64)> {
    if !(weighted_inventory_price.is_finite() && weighted_inventory_price > 0.0) {
        return None;
    }
    let hedge_mid = Bbo::new(bid, ask, 0).get_mid_price().unwrap_or(0.0);
    if !(hedge_mid.is_finite() && hedge_mid > 0.0) {
        return None;
    }
    let pct_change = (hedge_mid - weighted_inventory_price).abs() / weighted_inventory_price;
    Some((pct_change, threshold_pct / 100.0))
}

fn cap_arb_hedge_due_qty_by_amount_u(due_hedge_qty: f64, mid_price: f64, amount_cap_u: f64) -> f64 {
    if !(due_hedge_qty.is_finite() && due_hedge_qty.abs() > 0.0) {
        return due_hedge_qty;
    }
    if !(mid_price.is_finite() && mid_price > 0.0) {
        return due_hedge_qty;
    }
    if !(amount_cap_u.is_finite() && amount_cap_u > 0.0) {
        return due_hedge_qty;
    }

    let max_base_qty = amount_cap_u / mid_price;
    if !(max_base_qty.is_finite() && max_base_qty > 0.0) {
        return due_hedge_qty;
    }
    let capped_abs = due_hedge_qty.abs().min(max_base_qty);
    if due_hedge_qty.is_sign_negative() {
        -capped_abs
    } else {
        capped_abs
    }
}

/// Fast path for `ARB_HEDGE_FORCE_TAKER=on`: skip model/factor lookup and offset
/// plan entirely, just align qty for the hedge venue and publish a taker ArbHedge
/// signal (price=0, offset=0, exp_time=0).
fn emit_arb_taker_hedge_fast(
    source: &'static str,
    runtime: &ArbShellRuntime,
    query: &ArbHedgeSignalQueryMsg,
    symbol: &str,
    quote: &Quote,
    hedge_venue: TradingVenue,
) {
    let hedge_side = if query.due_hedge_qty >= 0.0 {
        Side::Sell
    } else {
        Side::Buy
    };
    let mid_price = Bbo::new(quote.bid, quote.ask, quote.ts)
        .get_mid_price()
        .unwrap_or_else(|| ((quote.bid + quote.ask) * 0.5).max(0.0));
    if !(mid_price.is_finite() && mid_price > 0.0) {
        log::warn!(
            "{source}: ArbHedge force-taker invalid mid_price strategy_id={} symbol={} bid={} ask={}",
            query.strategy_id, symbol, quote.bid, quote.ask
        );
        return;
    }

    let amount_cap_u = ArbDecision::with_state_mut(|arb| arb.resolve_order_amount_u(symbol))
        .unwrap_or(f64::INFINITY);
    let capped_due_hedge_qty =
        cap_arb_hedge_due_qty_by_amount_u(query.due_hedge_qty, mid_price, amount_cap_u);
    if capped_due_hedge_qty.abs() <= 1e-12 {
        log::debug!(
            "{source}: ArbHedge force-taker capped qty zero strategy_id={} symbol={}",
            query.strategy_id,
            symbol
        );
        return;
    }

    let table = if hedge_venue == runtime.venues.0 {
        &runtime.open_min_qty_table
    } else {
        &runtime.hedge_min_qty_table
    };
    let trade_symbol = normalize_symbol_for_venue(symbol, hedge_venue);
    let symbol_key = min_qty_symbol_key(hedge_venue, &trade_symbol);
    let qty_tick = table.step_size(&symbol_key).unwrap_or(0.0);
    if !(qty_tick.is_finite() && qty_tick > 0.0) {
        log::warn!(
            "{source}: ArbHedge force-taker missing qty_tick strategy_id={} symbol={} venue={:?} symbol_key={}",
            query.strategy_id, symbol, hedge_venue, symbol_key
        );
        return;
    }
    let price_for_notional = match hedge_side {
        Side::Buy => quote.ask,
        Side::Sell => quote.bid,
    };
    let aligned_qty = match align_order_for_venue(
        hedge_venue,
        &symbol_key,
        capped_due_hedge_qty.abs(),
        price_for_notional.max(mid_price),
        table,
    ) {
        Ok((qty, _)) => qty,
        Err(err) => {
            log::warn!(
                "{source}: ArbHedge force-taker align failed strategy_id={} symbol={} target_qty={:.8} err={}",
                query.strategy_id, symbol, capped_due_hedge_qty.abs(), err
            );
            return;
        }
    };
    if !(aligned_qty.is_finite() && aligned_qty > 0.0) {
        log::info!(
            "{source}: ArbHedge force-taker zero aligned_qty strategy_id={} symbol={} target_qty={:.8}",
            query.strategy_id, symbol, capped_due_hedge_qty.abs()
        );
        return;
    }
    let Some(amount_qv) = QuantizedValue::encode_floor(aligned_qty, qty_tick) else {
        log::warn!(
            "{source}: ArbHedge force-taker amount_qv encode failed strategy_id={} symbol={} qty={:.8} qty_tick={:.8}",
            query.strategy_id, symbol, aligned_qty, qty_tick
        );
        return;
    };

    let now_us = get_timestamp_us();
    let mut ctx = ArbHedgeCtx::new();
    ctx.strategy_id = query.strategy_id;
    ctx.set_side(hedge_side);
    ctx.hedging_leg = TradingLeg::new(hedge_venue, quote.bid, quote.ask, quote.ts);
    ctx.set_hedging_symbol(symbol);
    ctx.price_qv = QuantizedValue::zero();
    ctx.amount_qv = amount_qv;
    ctx.price_offset = 0.0;
    ctx.signal_ts = now_us;
    ctx.exp_time = 0;
    ctx.request_seq = query.request_seq;
    ctx.set_from_key(build_inventory_hedge_from_key(now_us, None, 0.0));

    let signal = TradeSignal::create(
        SignalType::ArbHedge,
        get_timestamp_us(),
        0.0,
        ctx.to_bytes(),
    );
    if let Err(err) = runtime.signal_pub.publish(&signal.to_bytes()) {
        log::warn!(
            "{source}: publish ArbHedge force-taker failed strategy_id={} symbol={} err={:#}",
            query.strategy_id,
            symbol,
            err
        );
        return;
    }

    log::info!(
        "{source}: ArbHedge force-taker strategy_id={} symbol={} side={:?} aligned_qty={:.8} mid={:.8} request_seq={} due_hedge_qty={:.8} capped_due_hedge_qty={:.8}",
        query.strategy_id, symbol, hedge_side, aligned_qty, mid_price,
        query.request_seq, query.due_hedge_qty, capped_due_hedge_qty
    );
}

fn drive_shared_arb_hedge_query(
    source: &'static str,
    runtime: &ArbShellRuntime,
    query: ArbHedgeSignalQueryMsg,
) {
    let symbol = query.get_symbol().to_uppercase();
    log::info!(
        "{source}: ArbHedge query received strategy_id={} symbol={} request_seq={} net_qty={:.8} due_hedge_qty={:.8} pending_hedge_qty={:.8}",
        query.strategy_id,
        symbol,
        query.request_seq,
        query.net_qty,
        query.due_hedge_qty,
        query.pending_hedge_qty
    );
    if symbol.is_empty() {
        log::warn!("{source}: ArbHedge query missing symbol");
        return;
    }
    if query.due_hedge_qty.abs() <= 1e-12 {
        log::debug!(
            "{source}: ArbHedge query skip zero due qty strategy_id={} symbol={} request_seq={}",
            query.strategy_id,
            symbol,
            query.request_seq
        );
        return;
    }

    let hedge_venue = runtime.venues.1;
    let Some(quote) = MktChannel::instance().get_quote(&symbol, hedge_venue) else {
        log::warn!(
            "{source}: ArbHedge quote unavailable strategy_id={} symbol={} venue={:?}",
            query.strategy_id,
            symbol,
            hedge_venue
        );
        return;
    };
    if arb_hedge_force_taker() {
        emit_arb_taker_hedge_fast(source, runtime, &query, &symbol, &quote, hedge_venue);
        return;
    }
    let Some(params) = resolve_arb_hedge_build_params(source, &symbol, hedge_venue) else {
        return;
    };

    let hedge_side = if query.due_hedge_qty >= 0.0 {
        Side::Sell
    } else {
        Side::Buy
    };
    let mid_price = Bbo::new(quote.bid, quote.ask, quote.ts)
        .get_mid_price()
        .unwrap_or_else(|| ((quote.bid + quote.ask) * 0.5).max(0.0));
    let capped_due_hedge_qty =
        cap_arb_hedge_due_qty_by_amount_u(query.due_hedge_qty, mid_price, params.amount_cap_u);
    if capped_due_hedge_qty.abs() + 1e-12 < query.due_hedge_qty.abs() {
        log::info!(
            "{source}: ArbHedge due qty capped strategy_id={} symbol={} request_seq={} due_hedge_qty={:.8} capped_due_hedge_qty={:.8} mid_price={:.8} amount_cap_u={:.8}",
            query.strategy_id,
            symbol,
            query.request_seq,
            query.due_hedge_qty,
            capped_due_hedge_qty,
            mid_price,
            params.amount_cap_u
        );
    }
    let table = if hedge_venue == runtime.venues.0 {
        &runtime.open_min_qty_table
    } else {
        &runtime.hedge_min_qty_table
    };
    let input = InventoryHedgeBuildInput {
        venue: hedge_venue,
        symbol: &symbol,
        quote,
        volatility: params.volatility,
        signal: params.signal,
        signal_qtl: params.signal_qtl,
        enable_return_score_adjust_hedge: params.enable_return_score_adjust_hedge,
        hedge_vol_multiplier: params.hedge_vol_multiplier,
        hedge_offset_ratio: params.hedge_offset_ratio,
        order_amount_u: capped_due_hedge_qty.abs() * mid_price,
        hedge_target_qty: capped_due_hedge_qty,
        target_base_qty: Some(capped_due_hedge_qty),
        inventory_net_qty: query.net_qty,
        symbol_exposure_u: query.symbol_exposure_u,
        hedge_orders_per_round: ARB_HEDGE_ORDERS_PER_ROUND,
        offset_low: params.hedge_price_offset_limit_lower,
        offset_high_limit: params.hedge_price_offset_limit_upper,
        hedge_window_scale_low: params.hedge_window_scale_low,
        hedge_window_scale_high: params.hedge_window_scale_high,
        next_query_delay_ms: 60_000,
        clock_shift_ms: 0,
    };
    let plan = match build_inventory_hedge_quote_plan(input, table) {
        Ok(plan) => plan,
        Err(err) => {
            log::warn!(
                "{source}: build ArbHedge plan failed strategy_id={} symbol={} err={}",
                query.strategy_id,
                symbol,
                err
            );
            return;
        }
    };
    log::info!(
        "{source}: ArbHedge single-order plan strategy_id={} symbol={} request_seq={} split_policy=single_order_high_frequency hedge_orders_per_round={} plan_levels={} target_base_qty={:.8} due_hedge_qty={:.8} capped_due_hedge_qty={:.8} pending_hedge_qty={:.8} mid_price={:.8} amount_cap_u={:.8}",
        query.strategy_id,
        symbol,
        query.request_seq,
        ARB_HEDGE_ORDERS_PER_ROUND,
        plan.levels.len(),
        capped_due_hedge_qty,
        query.due_hedge_qty,
        capped_due_hedge_qty,
        query.pending_hedge_qty,
        mid_price,
        params.amount_cap_u
    );
    let Some(level) = plan.levels.first() else {
        log::warn!(
            "{source}: ArbHedge plan empty levels strategy_id={} symbol={} request_seq={}",
            query.strategy_id,
            symbol,
            query.request_seq
        );
        return;
    };
    let Some(amount_qv) =
        crate::common::tick_math::QuantizedValue::encode_floor(level.aligned_qty, plan.qty_tick)
    else {
        log::warn!(
            "{source}: ArbHedge amount qv invalid strategy_id={} symbol={} request_seq={} qty={:.8} qty_tick={:.8}",
            query.strategy_id,
            symbol,
            query.request_seq,
            level.aligned_qty,
            plan.qty_tick
        );
        return;
    };
    let Some(price_qv) = crate::common::tick_math::QuantizedValue::encode_floor(
        level.aligned_price,
        plan.price_tick,
    ) else {
        log::warn!(
            "{source}: ArbHedge price qv invalid strategy_id={} symbol={} request_seq={} price={:.8} price_tick={:.8}",
            query.strategy_id,
            symbol,
            query.request_seq,
            level.aligned_price,
            plan.price_tick
        );
        return;
    };

    let use_taker = should_use_arb_hedge_taker(
        query.weighted_inventory_price,
        quote.bid,
        quote.ask,
        params.max_hedge_price_pct_change,
    )
    .map(|(pct_change, threshold)| pct_change > threshold)
    .unwrap_or(false);

    let mut ctx = ArbHedgeCtx::new();
    ctx.strategy_id = query.strategy_id;
    ctx.set_side(hedge_side);
    ctx.hedging_leg = TradingLeg::new(hedge_venue, quote.bid, quote.ask, quote.ts);
    ctx.set_hedging_symbol(&symbol);
    ctx.price_qv = if use_taker {
        crate::common::tick_math::QuantizedValue::zero()
    } else {
        price_qv
    };
    ctx.amount_qv = amount_qv;
    ctx.price_offset = if use_taker { 0.0 } else { level.offset };
    ctx.signal_ts = get_timestamp_us();
    ctx.exp_time = if use_taker {
        0
    } else {
        ctx.signal_ts.saturating_add(params.hedge_timeout_mm_us)
    };
    ctx.request_seq = query.request_seq;
    ctx.set_from_key(build_inventory_hedge_from_key(
        plan.now_us,
        plan.signal_qtl,
        plan.volatility,
    ));

    let signal = TradeSignal::create(
        SignalType::ArbHedge,
        get_timestamp_us(),
        0.0,
        ctx.to_bytes(),
    );
    if let Err(err) = runtime.signal_pub.publish(&signal.to_bytes()) {
        log::warn!(
            "{source}: publish ArbHedge failed strategy_id={} symbol={} err={:#}",
            query.strategy_id,
            symbol,
            err
        );
        return;
    }

    log::info!(
        "{source}: ArbHedge reply strategy_id={} symbol={} side={:?} qty={:.8} price={:.8} mode={} request_seq={} net_qty={:.8} due_hedge_qty={:.8} capped_due_hedge_qty={:.8} pending_hedge_qty={:.8}",
        query.strategy_id,
        symbol,
        hedge_side,
        ctx.amount_value(),
        ctx.price_value(),
        if use_taker { "taker" } else { "maker" },
        query.request_seq,
        query.net_qty,
        query.due_hedge_qty,
        capped_due_hedge_qty,
        query.pending_hedge_qty
    );
}

fn drive_funding_cancel_candidate_query(
    decision: &mut FundingArbShell,
    query: ArbCancelCandidateQueryMsg,
) {
    if query.groups.is_empty() {
        return;
    }
    let now_us = get_timestamp_us();
    let venues = ArbDecision::with_state_mut(|arb| arb.venues)
        .expect("ArbDecisionState should be initialized");
    let mut cancel_sent = 0usize;
    let mut matched_symbols = 0usize;
    for group in query.groups {
        let open_symbol = group.get_symbol().to_uppercase();
        if open_symbol.is_empty() || group.items.is_empty() {
            continue;
        }
        let Some(threshold) =
            ArbDecision::with_state_mut(|arb| arb.tlen_thresholds.get(&open_symbol).copied())
                .flatten()
        else {
            log::debug!(
                "{FUNDING_ARB_SHELL_NAME}: missing tlen threshold symbol={}",
                open_symbol
            );
            continue;
        };
        let tick_indices: Vec<i64> = group
            .items
            .iter()
            .map(|item| item.price_qv.get_count())
            .collect();
        let tlens = match decision
            .runtime
            .open_depth_query_client
            .query_batch_tick_indices(&open_symbol, &tick_indices)
        {
            Ok(values) => normalize_tlens_for_compare(
                FUNDING_ARB_SHELL_NAME,
                &decision.runtime.open_min_qty_table,
                venues.0,
                &open_symbol,
                values,
            ),
            Err(err) => {
                log::warn!(
                    "{FUNDING_ARB_SHELL_NAME}: ArbCancel tlen batch query failed symbol={} levels={} err={:#}",
                    open_symbol,
                    tick_indices.len(),
                    err
                );
                continue;
            }
        };
        let preview = super::arb_tlen_cancel::build_group_eval_preview(&group, threshold, &tlens);
        let hedge_symbol =
            crate::common::symbol_util::normalize_symbol_for_venue(&open_symbol, venues.1)
                .to_ascii_uppercase();
        let mut matched_preview: Vec<String> = Vec::new();
        let mut group_cancel_sent = 0usize;
        for (item, tlen) in group.items.iter().zip(tlens.iter().copied()) {
            if tlen >= threshold {
                continue;
            }
            super::arb_tlen_cancel::push_match_preview(
                &mut matched_preview,
                item.strategy_id,
                item.price_qv.get_count(),
                tlen,
                threshold,
            );
            if let Err(err) = emit_funding_precise_tlen_cancel(
                decision,
                &open_symbol,
                &hedge_symbol,
                venues.0,
                venues.1,
                item.strategy_id,
                tlen,
                threshold,
                now_us,
            ) {
                log::warn!(
                    "{FUNDING_ARB_SHELL_NAME}: emit precise ArbCancel failed symbol={} strategy_id={} err={:#}",
                    open_symbol,
                    item.strategy_id,
                    err
                );
                continue;
            }
            cancel_sent += 1;
            group_cancel_sent += 1;
        }
        log::debug!(
            "{FUNDING_ARB_SHELL_NAME}: ArbCancel tlen compare symbol={} trigger_ts={} candidates={} threshold={:.4} min_tlen={:.4} max_tlen={:.4} details={}",
            open_symbol,
            query.trigger_ts,
            preview.tick_indices.len(),
            threshold,
            preview.min_tlen,
            preview.max_tlen,
            if preview.compared_preview.is_empty() {
                "-".to_string()
            } else {
                preview.compared_preview.join(",")
            }
        );
        if group_cancel_sent > 0 {
            matched_symbols += 1;
            let _ = ArbDecision::with_state_mut(|arb| {
                arb.record_tlen_cancel_summary(
                    FUNDING_ARB_SHELL_NAME,
                    &open_symbol,
                    query.trigger_ts,
                    preview.tick_indices.len(),
                    group_cancel_sent,
                    threshold,
                    preview.min_tlen,
                    preview.max_tlen,
                    &matched_preview,
                );
            });
        }
    }
    if cancel_sent > 0 {
        log::debug!(
            "{FUNDING_ARB_SHELL_NAME}: ArbCancel candidate query processed trigger_ts={} matched_symbols={} cancels_sent={}",
            query.trigger_ts, matched_symbols, cancel_sent
        );
    }
    let _ = ArbDecision::with_state_mut(|arb| arb.maybe_log_tlen_cancel_summary());
}

fn drive_spread_arb_cancel_candidate_query(
    decision: &mut SpreadArbShell,
    query: ArbCancelCandidateQueryMsg,
) {
    if query.groups.is_empty() {
        return;
    }
    let now_us = get_timestamp_us();
    let venues = ArbDecision::with_state_mut(|arb| arb.venues)
        .expect("ArbDecisionState should be initialized");
    let mut cancel_sent = 0usize;
    let mut matched_symbols = 0usize;
    for group in query.groups {
        let open_symbol = group.get_symbol().to_uppercase();
        if open_symbol.is_empty() || group.items.is_empty() {
            continue;
        }
        let Some(threshold) =
            ArbDecision::with_state_mut(|arb| arb.tlen_thresholds.get(&open_symbol).copied())
                .flatten()
        else {
            log::debug!(
                "{SPREAD_ARB_SHELL_NAME}: missing tlen threshold symbol={}",
                open_symbol
            );
            continue;
        };
        let tick_indices: Vec<i64> = group
            .items
            .iter()
            .map(|item| item.price_qv.get_count())
            .collect();
        let tlens = match decision
            .runtime
            .open_depth_query_client
            .query_batch_tick_indices(&open_symbol, &tick_indices)
        {
            Ok(values) => normalize_tlens_for_compare(
                SPREAD_ARB_SHELL_NAME,
                &decision.runtime.open_min_qty_table,
                venues.0,
                &open_symbol,
                values,
            ),
            Err(err) => {
                log::warn!(
                    "{SPREAD_ARB_SHELL_NAME}: ArbCancel tlen batch query failed symbol={} levels={} err={:#}",
                    open_symbol,
                    tick_indices.len(),
                    err
                );
                continue;
            }
        };
        let preview = super::arb_tlen_cancel::build_group_eval_preview(&group, threshold, &tlens);
        let hedge_symbol =
            crate::common::symbol_util::normalize_symbol_for_venue(&open_symbol, venues.1)
                .to_ascii_uppercase();
        let mut matched_preview: Vec<String> = Vec::new();
        let mut group_cancel_sent = 0usize;
        for (item, tlen) in group.items.iter().zip(tlens.iter().copied()) {
            if tlen >= threshold {
                continue;
            }
            super::arb_tlen_cancel::push_match_preview(
                &mut matched_preview,
                item.strategy_id,
                item.price_qv.get_count(),
                tlen,
                threshold,
            );
            if let Err(err) = emit_spread_arb_precise_tlen_cancel(
                decision,
                &open_symbol,
                &hedge_symbol,
                venues.0,
                venues.1,
                item.strategy_id,
                tlen,
                threshold,
                now_us,
            ) {
                log::warn!(
                    "{SPREAD_ARB_SHELL_NAME}: emit precise ArbCancel failed symbol={} strategy_id={} err={:#}",
                    open_symbol,
                    item.strategy_id,
                    err
                );
                continue;
            }
            cancel_sent += 1;
            group_cancel_sent += 1;
        }
        log::debug!(
            "{SPREAD_ARB_SHELL_NAME}: ArbCancel tlen compare symbol={} trigger_ts={} candidates={} threshold={:.4} min_tlen={:.4} max_tlen={:.4} details={}",
            open_symbol,
            query.trigger_ts,
            preview.tick_indices.len(),
            threshold,
            preview.min_tlen,
            preview.max_tlen,
            if preview.compared_preview.is_empty() {
                "-".to_string()
            } else {
                preview.compared_preview.join(",")
            }
        );
        if group_cancel_sent > 0 {
            matched_symbols += 1;
            let _ = ArbDecision::with_state_mut(|arb| {
                arb.record_tlen_cancel_summary(
                    SPREAD_ARB_SHELL_NAME,
                    &open_symbol,
                    query.trigger_ts,
                    preview.tick_indices.len(),
                    group_cancel_sent,
                    threshold,
                    preview.min_tlen,
                    preview.max_tlen,
                    &matched_preview,
                );
            });
        }
    }
    if cancel_sent > 0 {
        log::debug!(
            "{SPREAD_ARB_SHELL_NAME}: ArbCancel candidate query processed trigger_ts={} matched_symbols={} cancels_sent={}",
            query.trigger_ts, matched_symbols, cancel_sent
        );
    }
    let _ = ArbDecision::with_state_mut(|arb| arb.maybe_log_tlen_cancel_summary());
}

fn emit_funding_precise_tlen_cancel(
    decision: &FundingArbShell,
    open_symbol: &str,
    hedge_symbol: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    strategy_id: i32,
    tlen: f64,
    threshold: f64,
    now_us: i64,
) -> Result<()> {
    let (open_quote, hedge_quote) =
        match ArbDecision::load_valid_quotes(open_symbol, hedge_symbol, open_venue, hedge_venue) {
            Some(quotes) => quotes,
            None => return Ok(()),
        };
    let spread_rate = super::common::compute_spread_rate(&open_quote, &hedge_quote);
    let snapshot = ArbDecision::with_state_mut(|arb| {
        arb.snapshot_open_from_key_fields(
            open_symbol,
            hedge_symbol,
            open_venue,
            hedge_venue,
            now_us,
        )
    })
    .unwrap_or_default();
    let premium_rate = super::arb_open_filter::lookup_realtime_open_filter_value(
        open_symbol,
        hedge_symbol,
        open_venue,
        hedge_venue,
    )
    .map(|(value, _)| value);
    let from_key = super::arb_from_key::build_funding_tlen_cancel_from_key(
        now_us,
        snapshot.return_qtl,
        snapshot.return_threshold,
        snapshot.volatility,
        snapshot.vol_band_scale,
        snapshot.env_score,
        snapshot.env_threshold,
        spread_rate,
        premium_rate,
        tlen,
        threshold,
    );
    super::arb_cancel_emit::emit_precise_arb_cancel(super::arb_cancel_emit::ArbCancelEmitInput {
        signal_pub: &decision.runtime.signal_pub,
        open_symbol,
        hedge_symbol,
        open_venue,
        hedge_venue,
        open_quote: &open_quote,
        hedge_quote: &hedge_quote,
        now: now_us,
        from_key: &from_key,
        reason: crate::signal::cancel_signal::ArbCancelReason::Tlen,
        side: Side::Buy,
        strategy_id,
    })
}

fn emit_spread_arb_precise_tlen_cancel(
    decision: &SpreadArbShell,
    open_symbol: &str,
    hedge_symbol: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    strategy_id: i32,
    tlen: f64,
    threshold: f64,
    now_us: i64,
) -> Result<()> {
    let (open_quote, hedge_quote) =
        match ArbDecision::load_valid_quotes(open_symbol, hedge_symbol, open_venue, hedge_venue) {
            Some(quotes) => quotes,
            None => return Ok(()),
        };
    let spread_rate = super::common::compute_spread_rate(&open_quote, &hedge_quote);
    let return_qtl = ArbDecision::with_state_mut(|arb| {
        arb.lookup_return_model_score_lookup(hedge_symbol, hedge_venue)
            .and_then(|lookup| lookup.score_quantile)
            .filter(|v| v.is_finite())
    })
    .flatten();
    let environment_signal = ArbDecision::with_state_mut(|arb| {
        arb.evaluate_environment_signal(open_symbol, hedge_symbol, hedge_venue, now_us)
    })
    .expect("ArbDecisionState should be initialized");
    let environment_score = environment_signal
        .score
        .unwrap_or(environment_signal.class_label as f64);
    let volatility = ArbDecision::with_state_mut(|arb| {
        arb.lookup_hedge_factor_value(hedge_symbol, hedge_venue)
            .target_factor_value
    })
    .flatten();
    let from_key = super::arb_from_key::build_spread_arb_tlen_cancel_from_key(
        now_us,
        return_qtl,
        None,
        environment_score,
        environment_signal.threshold,
        volatility,
        ArbDecision::with_state_mut(|arb| Some(arb.vol_band_scale)).flatten(),
        spread_rate,
        super::arb_open_filter::lookup_realtime_open_filter_value(
            open_symbol,
            hedge_symbol,
            open_venue,
            hedge_venue,
        )
        .map(|(value, _)| value),
        tlen,
        threshold,
    );
    super::arb_cancel_emit::emit_precise_arb_cancel(super::arb_cancel_emit::ArbCancelEmitInput {
        signal_pub: &decision.runtime.signal_pub,
        open_symbol,
        hedge_symbol,
        open_venue,
        hedge_venue,
        open_quote: &open_quote,
        hedge_quote: &hedge_quote,
        now: now_us,
        from_key: &from_key,
        reason: crate::signal::cancel_signal::ArbCancelReason::Tlen,
        side: Side::Buy,
        strategy_id,
    })
}

fn emit_spread_arb_spread_cancel(
    decision: &SpreadArbShell,
    open_symbol: &str,
    hedge_symbol: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    side: Side,
) -> Result<()> {
    let (open_quote, hedge_quote) =
        match ArbDecision::load_valid_quotes(open_symbol, hedge_symbol, open_venue, hedge_venue) {
            Some(quotes) => quotes,
            None => return Ok(()),
        };
    let batch_ts = get_timestamp_us();
    let spread_rate = super::common::compute_spread_rate(&open_quote, &hedge_quote);
    let return_qtl = ArbDecision::with_state_mut(|arb| {
        arb.lookup_return_model_score_lookup(hedge_symbol, hedge_venue)
            .and_then(|lookup| lookup.score_quantile)
            .filter(|v| v.is_finite())
    })
    .flatten();
    let environment_signal = ArbDecision::with_state_mut(|arb| {
        arb.evaluate_environment_signal(open_symbol, hedge_symbol, hedge_venue, batch_ts)
    })
    .expect("ArbDecisionState should be initialized");
    let environment_score = environment_signal
        .score
        .unwrap_or(environment_signal.class_label as f64);
    let volatility = ArbDecision::with_state_mut(|arb| {
        arb.lookup_hedge_factor_value(hedge_symbol, hedge_venue)
            .target_factor_value
    })
    .flatten();
    let from_key = super::arb_from_key::build_spread_arb_cancel_from_key(
        batch_ts,
        return_qtl,
        None,
        environment_score,
        environment_signal.threshold,
        volatility,
        ArbDecision::with_state_mut(|arb| Some(arb.vol_band_scale)).flatten(),
        spread_rate,
        super::arb_open_filter::lookup_realtime_open_filter_value(
            open_symbol,
            hedge_symbol,
            open_venue,
            hedge_venue,
        )
        .map(|(value, _)| value),
    );
    super::arb_cancel_emit::emit_precise_arb_cancel(super::arb_cancel_emit::ArbCancelEmitInput {
        signal_pub: &decision.runtime.signal_pub,
        open_symbol,
        hedge_symbol,
        open_venue,
        hedge_venue,
        open_quote: &open_quote,
        hedge_quote: &hedge_quote,
        now: batch_ts,
        from_key: &from_key,
        reason: crate::signal::cancel_signal::ArbCancelReason::Spread,
        side,
        strategy_id: 0,
    })
}

pub fn publish_arb_cancel_trigger(
    signal_pub: &crate::common::iceoryx_publisher::SignalPublisher,
    now_us: i64,
) -> Result<u64> {
    let freq_ms = ArbDecision::with_state_mut(|arb| arb.tlen_cancel_freq_ms)
        .expect("ArbDecisionState should be initialized");
    let ctx = crate::signal::arb_signal::ArbCancelTriggerCtx {
        trigger_ts: now_us,
        freq_ms,
    };
    let signal = crate::signal::trade_signal::TradeSignal::create(
        SignalType::ArbCancelTrigger,
        now_us,
        0.0,
        ctx.to_bytes(),
    );
    signal_pub.publish(&signal.to_bytes())?;
    Ok(freq_ms)
}

fn drive_cancel_trigger_interval(source: &str, signal_pub: &SignalPublisher) {
    let now_us = get_timestamp_us();
    if !ArbDecision::with_state_mut(|arb| arb.should_emit_cancel_trigger(now_us)).unwrap_or(false) {
        return;
    }
    match publish_arb_cancel_trigger(signal_pub, now_us) {
        Ok(freq_ms) => {
            let _ = ArbDecision::with_state_mut(|arb| {
                arb.last_cancel_trigger_ts_us = now_us;
            });
            log::debug!("{source}: ArbCancelTrigger emitted freq_ms={}", freq_ms);
        }
        Err(err) => log::warn!("{source}: publish ArbCancelTrigger failed: {err:#}"),
    }
}

fn drive_funding_cancel_trigger_interval(decision: &FundingArbShell) {
    drive_cancel_trigger_interval(FUNDING_ARB_SHELL_NAME, &decision.runtime.signal_pub);
}

fn drive_spread_arb_cancel_trigger_interval(decision: &SpreadArbShell) {
    drive_cancel_trigger_interval(SPREAD_ARB_SHELL_NAME, &decision.runtime.signal_pub);
}

#[allow(clippy::too_many_arguments)]
fn emit_spread_arb_open_signals(
    decision: &mut SpreadArbShell,
    open_symbol: &str,
    hedge_symbol: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    side: Side,
    return_qtl: Option<f64>,
    return_threshold: Option<f64>,
    open_filter_value: Option<f64>,
    environment_score: f64,
    environment_threshold: Option<f64>,
    open_volatility_factor: f64,
) -> Result<()> {
    let (open_quote, hedge_quote) =
        match ArbDecision::load_valid_quotes(open_symbol, hedge_symbol, open_venue, hedge_venue) {
            Some(quotes) => quotes,
            None => return Ok(()),
        };
    let panic_on_first_open_dry_run = std::env::var("TRADE_SIGNAL_PANIC_ON_FIRST_OPEN_DRY_RUN")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);
    let batch_ts = get_timestamp_us();
    let spread_rate = super::common::compute_spread_rate(&open_quote, &hedge_quote);
    let vol_band_scale = ArbDecision::with_state_mut(|arb| arb.vol_band_scale)
        .expect("ArbDecisionState should be initialized");
    let plan_volatility = open_volatility_factor.max(0.0);
    let base_from_key = super::common::build_open_from_key_base(
        batch_ts,
        return_qtl,
        return_threshold,
        Some(open_volatility_factor),
        Some(vol_band_scale),
        Some(environment_score),
        environment_threshold,
        spread_rate,
    );
    let from_key = super::common::append_key_value_fields(
        base_from_key,
        &[(
            "spread_fr",
            super::common::format_from_key_optional_value(open_filter_value, 6),
        )],
    );
    let order_amount = ArbDecision::with_state_mut(|arb| arb.resolve_order_amount_u(open_symbol))
        .expect("ArbDecisionState should be initialized");
    let open_orders_per_round = ArbDecision::with_state_mut(|arb| arb.open_orders_per_round)
        .expect("ArbDecisionState should be initialized");
    let open_order_ttl_us = ArbDecision::with_state_mut(|arb| arb.open_order_ttl_us)
        .expect("ArbDecisionState should be initialized");
    let hedge_timeout_mm_us = ArbDecision::with_state_mut(|arb| arb.hedge_timeout_mm_us)
        .expect("ArbDecisionState should be initialized");
    let factor_mode = super::spread_factor::SpreadFactor::instance().get_mode();

    let open_offset_lower =
        ArbDecision::with_state_mut(|arb| arb.resolve_open_offset_lower(open_symbol))
            .expect("ArbDecisionState should be initialized");
    let plan = match super::arb_quote_plan::build_arb_open_quote_plan(
        open_venue,
        open_symbol,
        open_quote,
        order_amount,
        open_orders_per_round,
        side,
        plan_volatility,
        vol_band_scale,
        if open_venue == decision.runtime.venues.0 {
            &decision.runtime.open_min_qty_table
        } else {
            &decision.runtime.hedge_min_qty_table
        },
        open_offset_lower,
    ) {
        Ok(plan) => plan,
        Err(err) => {
            if !handle_quote_plan_error(
                SPREAD_ARB_SHELL_NAME,
                &mut decision.runtime,
                open_venue,
                open_symbol,
                &err,
            ) {
                log::warn!(
                    "{SPREAD_ARB_SHELL_NAME}: build open quote plan failed open={} hedge={} side={:?} err={}",
                    open_symbol, hedge_symbol, side, err
                );
            }
            return Ok(());
        }
    };

    let build_ctx = |level: &crate::market_maker::quote_plan_levels::QuotePlanLevel| {
        super::arb_open_context::build_arb_open_context_from_level_with_tables(
            super::arb_open_context::ArbOpenContextTablesInput {
                open_symbol,
                hedge_symbol,
                open_venue,
                hedge_venue,
                open_quote: &open_quote,
                hedge_quote: &hedge_quote,
                level,
                now: batch_ts,
                from_key: from_key.as_str(),
                open_order_ttl_us,
                hedge_timeout_mm_us,
                factor_mode,
                order_amount,
                open_table: if open_venue == decision.runtime.venues.0 {
                    &decision.runtime.open_min_qty_table
                } else {
                    &decision.runtime.hedge_min_qty_table
                },
                hedge_table: if hedge_venue == decision.runtime.venues.0 {
                    &decision.runtime.open_min_qty_table
                } else {
                    &decision.runtime.hedge_min_qty_table
                },
            },
        )
    };
    let mut contexts: Vec<_> = plan.levels.iter().map(build_ctx).collect();
    if !contexts.is_empty() {
        let tick_indices: Vec<i64> = contexts.iter().map(|ctx| ctx.price_count()).collect();
        let query_symbol = contexts[0].get_opening_symbol();
        let open_depth_query_client = if open_venue == decision.runtime.venues.0 {
            &decision.runtime.open_depth_query_client
        } else {
            decision
                .runtime
                .hedge_depth_query_client
                .as_ref()
                .expect("missing open-side depth query client")
        };
        let tlen_gate = ArbDecision::with_state_mut(|arb| {
            if arb.enable_tlen_cancel {
                arb.tlen_thresholds
                    .get(&query_symbol.to_ascii_uppercase())
                    .copied()
            } else {
                None
            }
        })
        .flatten();
        let (from_keys, filtered_levels) = super::common::apply_open_tlen_gate_and_build_from_keys(
            SPREAD_ARB_SHELL_NAME,
            open_depth_query_client,
            if open_venue == decision.runtime.venues.0 {
                &decision.runtime.open_min_qty_table
            } else {
                &decision.runtime.hedge_min_qty_table
            },
            open_venue,
            &query_symbol,
            &tick_indices,
            &from_key,
            tlen_gate,
        );
        contexts = from_keys
            .into_iter()
            .zip(contexts)
            .filter_map(|(from_key_bytes, mut ctx)| {
                let from_key_bytes = from_key_bytes?;
                ctx.set_from_key(from_key_bytes);
                Some(ctx)
            })
            .collect();
        if filtered_levels > 0 {
            let _ = ArbDecision::with_state_mut(|arb| {
                arb.record_intercept_summary_by("tlen_gate", filtered_levels as u64);
            });
            log::debug!(
                "{SPREAD_ARB_SHELL_NAME}: ArbOpen tlen gated symbol={} threshold={:?} filtered_levels={} kept_levels={}",
                query_symbol,
                tlen_gate,
                filtered_levels,
                contexts.len()
            );
        }
    }

    if panic_on_first_open_dry_run {
        if let Some(first_ctx) = contexts.first() {
            let from_key_str = String::from_utf8_lossy(&first_ctx.from_key);
            log::warn!(
                "{SPREAD_ARB_SHELL_NAME}: dry-run trap hit first ArbOpen open={} hedge={} side={:?} price={:.8} qty={:.8} from_key='{}'",
                open_symbol,
                hedge_symbol,
                plan.side,
                first_ctx.price_value(),
                first_ctx.amount_value(),
                from_key_str
            );
            panic!(
                "TRADE_SIGNAL_PANIC_ON_FIRST_OPEN_DRY_RUN triggered: first ArbOpen open={} hedge={} side={:?} from_key='{}'",
                open_symbol,
                hedge_symbol,
                plan.side,
                from_key_str
            );
        }
    }

    let planned_levels = plan.levels.len();
    let sent = super::arb_emit::emit_levels_as_signals(
        &decision.runtime.signal_pub,
        SignalType::ArbOpen,
        batch_ts,
        contexts,
        |ctx| ctx.to_bytes(),
    )?;

    log::info!(
        "{SPREAD_ARB_SHELL_NAME}: emitted {}/{} {:?} signal(s) to '{}' open={} hedge={} side={:?} inner={:.8} outer={:.8} vol={:.8} vol_band_scale=[{:.4},{:.4}] price_tick={:.8} qty_tick={:.8}",
        sent,
        planned_levels,
        SignalType::ArbOpen,
        DEFAULT_ARBITRAGE_SIGNAL_CHANNEL,
        open_symbol,
        hedge_symbol,
        plan.side,
        plan.inner_price,
        plan.outer_price,
        open_volatility_factor,
        vol_band_scale[0],
        vol_band_scale[1],
        plan.price_tick,
        plan.qty_tick
    );
    Ok(())
}

fn emit_spread_arb_close_signals(
    decision: &mut SpreadArbShell,
    open_symbol: &str,
    hedge_symbol: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    side: Side,
) -> Result<bool> {
    let (open_quote, hedge_quote) =
        match ArbDecision::load_valid_quotes(open_symbol, hedge_symbol, open_venue, hedge_venue) {
            Some(quotes) => quotes,
            None => return Ok(false),
        };
    let batch_ts = get_timestamp_us();
    let spread_rate = super::common::compute_spread_rate(&open_quote, &hedge_quote);
    let snapshot = ArbDecision::with_state_mut(|arb| {
        arb.snapshot_open_from_key_fields(
            open_symbol,
            hedge_symbol,
            open_venue,
            hedge_venue,
            batch_ts,
        )
    })
    .unwrap_or_default();
    let spread_fr = super::arb_open_filter::lookup_realtime_open_filter_value(
        open_symbol,
        hedge_symbol,
        open_venue,
        hedge_venue,
    )
    .map(|(value, _)| value);
    let from_key = super::common::append_dump_suffix(super::common::append_key_value_fields(
        super::common::build_open_from_key_base(
            batch_ts,
            snapshot.return_qtl,
            snapshot.return_threshold,
            snapshot.volatility,
            snapshot.vol_band_scale,
            snapshot.env_score,
            snapshot.env_threshold,
            spread_rate,
        ),
        &[(
            "spread_fr",
            super::common::format_from_key_optional_value(spread_fr, 6),
        )],
    ));
    let volatility = ArbDecision::with_state_mut(|arb| {
        arb.lookup_hedge_factor_value(hedge_symbol, hedge_venue)
            .target_factor_value
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(0.0)
    })
    .expect("ArbDecisionState should be initialized");
    let order_amount = ArbDecision::with_state_mut(|arb| arb.resolve_order_amount_u(open_symbol))
        .expect("ArbDecisionState should be initialized");
    let open_orders_per_round = ArbDecision::with_state_mut(|arb| arb.open_orders_per_round)
        .expect("ArbDecisionState should be initialized");
    let open_order_ttl_us = ArbDecision::with_state_mut(|arb| arb.open_order_ttl_us)
        .expect("ArbDecisionState should be initialized");
    let hedge_timeout_mm_us = ArbDecision::with_state_mut(|arb| arb.hedge_timeout_mm_us)
        .expect("ArbDecisionState should be initialized");
    let factor_mode = super::spread_factor::SpreadFactor::instance().get_mode();

    let open_offset_lower =
        ArbDecision::with_state_mut(|arb| arb.resolve_open_offset_lower(open_symbol))
            .expect("ArbDecisionState should be initialized");
    let plan = match super::arb_quote_plan::build_arb_open_quote_plan(
        open_venue,
        open_symbol,
        open_quote,
        order_amount,
        open_orders_per_round,
        side,
        volatility,
        [0.0, 1.0],
        if open_venue == decision.runtime.venues.0 {
            &decision.runtime.open_min_qty_table
        } else {
            &decision.runtime.hedge_min_qty_table
        },
        open_offset_lower,
    ) {
        Ok(plan) => plan,
        Err(err) => {
            if !handle_quote_plan_error(
                SPREAD_ARB_SHELL_NAME,
                &mut decision.runtime,
                open_venue,
                open_symbol,
                &err,
            ) {
                log::warn!(
                    "{SPREAD_ARB_SHELL_NAME}: build close quote plan failed open={} hedge={} side={:?} err={}",
                    open_symbol, hedge_symbol, side, err
                );
            }
            return Ok(false);
        }
    };

    let contexts = plan.levels.iter().map(|level| {
        super::arb_open_context::build_arb_open_context_from_level_with_tables(
            super::arb_open_context::ArbOpenContextTablesInput {
                open_symbol,
                hedge_symbol,
                open_venue,
                hedge_venue,
                open_quote: &open_quote,
                hedge_quote: &hedge_quote,
                level,
                now: batch_ts,
                from_key: from_key.as_str(),
                open_order_ttl_us,
                hedge_timeout_mm_us,
                factor_mode,
                order_amount,
                open_table: if open_venue == decision.runtime.venues.0 {
                    &decision.runtime.open_min_qty_table
                } else {
                    &decision.runtime.hedge_min_qty_table
                },
                hedge_table: if hedge_venue == decision.runtime.venues.0 {
                    &decision.runtime.open_min_qty_table
                } else {
                    &decision.runtime.hedge_min_qty_table
                },
            },
        )
    });
    let mut contexts: Vec<_> = contexts.collect();
    if !contexts.is_empty() {
        let tick_indices: Vec<i64> = contexts.iter().map(|ctx| ctx.price_count()).collect();
        let query_symbol = contexts[0].get_opening_symbol();
        let open_depth_query_client = if open_venue == decision.runtime.venues.0 {
            &decision.runtime.open_depth_query_client
        } else {
            decision
                .runtime
                .hedge_depth_query_client
                .as_ref()
                .expect("missing open-side depth query client")
        };
        let tlens = super::common::query_batch_tlens_or_zero(
            SPREAD_ARB_SHELL_NAME,
            open_depth_query_client,
            &query_symbol,
            &tick_indices,
        );
        for (ctx, level_tlen) in contexts.iter_mut().zip(tlens.into_iter()) {
            ctx.set_from_key(
                super::common::append_tlen_to_from_key(&from_key, level_tlen).into_bytes(),
            );
        }
    }
    contexts.retain(arb_close_notional_meets_min);
    if contexts.is_empty() {
        return Ok(false);
    }
    let planned_levels = contexts.len();
    let sent = super::arb_emit::emit_levels_as_signals(
        &decision.runtime.signal_pub,
        SignalType::ArbClose,
        batch_ts,
        contexts,
        |ctx| ctx.to_bytes(),
    )?;

    log::info!(
        "{SPREAD_ARB_SHELL_NAME}: emitted {}/{} {:?} signal(s) to '{}' open={} hedge={} side={:?} inner={:.8} outer={:.8} vol={:.8} price_tick={:.8} qty_tick={:.8}",
        sent,
        planned_levels,
        SignalType::ArbClose,
        DEFAULT_ARBITRAGE_SIGNAL_CHANNEL,
        open_symbol,
        hedge_symbol,
        plan.side,
        plan.inner_price,
        plan.outer_price,
        volatility,
        plan.price_tick,
        plan.qty_tick
    );
    Ok(sent > 0)
}

fn emit_funding_open_close_signals(
    decision: &mut FundingArbShell,
    spot_symbol: &str,
    futures_symbol: &str,
    spot_venue: TradingVenue,
    futures_venue: TradingVenue,
    signal_type: SignalType,
    side: Side,
    gate: Option<&ArbOpenGatePassed>,
) -> Result<bool> {
    if matches!(signal_type, SignalType::ArbOpen | SignalType::ArbClose) {
        let readiness = ArbDecision::with_state_mut(|arb| {
            arb.lookup_dual_venue_volatility_readiness(
                spot_symbol,
                futures_symbol,
                spot_venue,
                futures_venue,
            )
        })
        .expect("ArbDecisionState should be initialized");
        if !readiness.is_ready() {
            let reason = readiness.missing_reason();
            let _ = ArbDecision::with_state_mut(|arb| {
                arb.record_intercept_summary(match signal_type {
                    SignalType::ArbOpen => "dual_vol_block_open",
                    SignalType::ArbClose => "dual_vol_block_close",
                    _ => "dual_vol_block",
                });
            });
            log::warn!(
                "{FUNDING_ARB_SHELL_NAME}: suppress {:?} because dual venue volatility is not ready open={} hedge={} open_venue={:?} hedge_venue={:?} reason={}",
                signal_type,
                spot_symbol,
                futures_symbol,
                spot_venue,
                futures_venue,
                reason
            );
            return Ok(false);
        }
    }

    let (spot_quote, futures_quote) = match ArbDecision::load_valid_quotes(
        spot_symbol,
        futures_symbol,
        spot_venue,
        futures_venue,
    ) {
        Some(quotes) => quotes,
        None => return Ok(false),
    };
    let batch_ts = get_timestamp_us();
    let spread_rate = super::common::compute_spread_rate(&spot_quote, &futures_quote);
    let vol_band_scale = ArbDecision::with_state_mut(|arb| arb.vol_band_scale)
        .expect("ArbDecisionState should be initialized");
    let premium_rate = if matches!(signal_type, SignalType::ArbOpen) {
        gate.and_then(|v| v.open_filter_value)
    } else {
        super::arb_open_filter::lookup_realtime_open_filter_value(
            spot_symbol,
            futures_symbol,
            spot_venue,
            futures_venue,
        )
        .map(|(value, _)| value)
    };
    let from_key = if matches!(signal_type, SignalType::ArbOpen) {
        super::arb_from_key::build_funding_decision_from_key_with_gate(
            batch_ts,
            futures_symbol,
            futures_venue,
            spread_rate,
            gate.and_then(|v| v.return_qtl),
            gate.and_then(|v| v.return_threshold),
            gate.map(|v| v.open_volatility_factor),
            gate.map(|v| v.environment_score),
            gate.and_then(|v| v.environment_threshold),
            premium_rate,
            Some(vol_band_scale),
        )
    } else {
        super::arb_from_key::build_funding_decision_from_key(
            batch_ts,
            futures_symbol,
            futures_venue,
            spread_rate,
            premium_rate,
        )
        .to_vec()
    };
    let from_key = if matches!(signal_type, SignalType::ArbClose) {
        let snapshot = ArbDecision::with_state_mut(|arb| {
            arb.snapshot_open_from_key_fields(
                spot_symbol,
                futures_symbol,
                spot_venue,
                futures_venue,
                batch_ts,
            )
        })
        .unwrap_or_default();
        super::common::append_dump_suffix(
            super::arb_from_key::build_funding_decision_from_key_base(
                batch_ts,
                snapshot.return_qtl,
                snapshot.return_threshold,
                snapshot.volatility,
                snapshot.vol_band_scale,
                snapshot.env_score,
                snapshot.env_threshold,
                futures_symbol,
                futures_venue,
                spread_rate,
                premium_rate,
            ),
        )
        .into_bytes()
    } else {
        from_key
    };
    let raw_volatility = gate.map(|v| v.open_volatility_factor).unwrap_or_else(|| {
        ArbDecision::with_state_mut(|arb| {
            arb.lookup_open_factor_value(spot_symbol, spot_venue)
                .target_factor_value
                .filter(|v| v.is_finite() && *v >= 0.0)
                .unwrap_or(0.0)
        })
        .expect("ArbDecisionState should be initialized")
    });
    let plan_volatility = raw_volatility.max(0.0);
    let plan_vol_band_scale = if matches!(signal_type, SignalType::ArbOpen) {
        vol_band_scale
    } else {
        [0.0, 1.0]
    };
    let order_amount = ArbDecision::with_state_mut(|arb| arb.resolve_order_amount_u(spot_symbol))
        .expect("ArbDecisionState should be initialized");
    let open_orders_per_round = ArbDecision::with_state_mut(|arb| arb.open_orders_per_round)
        .expect("ArbDecisionState should be initialized");
    let open_order_ttl_us = ArbDecision::with_state_mut(|arb| arb.open_order_ttl_us)
        .expect("ArbDecisionState should be initialized");
    let hedge_timeout_mm_us = ArbDecision::with_state_mut(|arb| arb.hedge_timeout_mm_us)
        .expect("ArbDecisionState should be initialized");
    let factor_mode = super::spread_factor::SpreadFactor::instance().get_mode();

    let open_offset_lower =
        ArbDecision::with_state_mut(|arb| arb.resolve_open_offset_lower(spot_symbol))
            .expect("ArbDecisionState should be initialized");
    let plan = match super::arb_quote_plan::build_arb_open_quote_plan(
        spot_venue,
        spot_symbol,
        spot_quote,
        order_amount,
        open_orders_per_round,
        side,
        plan_volatility,
        plan_vol_band_scale,
        if spot_venue == decision.runtime.venues.0 {
            &decision.runtime.open_min_qty_table
        } else {
            &decision.runtime.hedge_min_qty_table
        },
        open_offset_lower,
    ) {
        Ok(plan) => plan,
        Err(err) => {
            if !handle_quote_plan_error(
                FUNDING_ARB_SHELL_NAME,
                &mut decision.runtime,
                spot_venue,
                spot_symbol,
                &err,
            ) {
                log::warn!(
                    "{FUNDING_ARB_SHELL_NAME}: build quote plan failed open={} hedge={} side={:?} signal={:?} err={}",
                    spot_symbol, futures_symbol, side, signal_type, err
                );
            }
            return Ok(false);
        }
    };

    let contexts = plan.levels.iter().map(|level| {
        super::arb_open_context::build_arb_open_context_from_level_with_tables(
            super::arb_open_context::ArbOpenContextTablesInput {
                open_symbol: spot_symbol,
                hedge_symbol: futures_symbol,
                open_venue: spot_venue,
                hedge_venue: futures_venue,
                open_quote: &spot_quote,
                hedge_quote: &futures_quote,
                level,
                now: batch_ts,
                from_key: std::str::from_utf8(&from_key).unwrap_or(""),
                open_order_ttl_us,
                hedge_timeout_mm_us,
                factor_mode,
                order_amount,
                open_table: if spot_venue == decision.runtime.venues.0 {
                    &decision.runtime.open_min_qty_table
                } else {
                    &decision.runtime.hedge_min_qty_table
                },
                hedge_table: if futures_venue == decision.runtime.venues.0 {
                    &decision.runtime.open_min_qty_table
                } else {
                    &decision.runtime.hedge_min_qty_table
                },
            },
        )
    });
    let mut contexts: Vec<_> = contexts.collect();
    if matches!(signal_type, SignalType::ArbOpen) && !contexts.is_empty() {
        let tick_indices: Vec<i64> = contexts.iter().map(|ctx| ctx.price_count()).collect();
        let query_symbol = contexts[0].get_opening_symbol();
        let open_depth_query_client = if spot_venue == decision.runtime.venues.0 {
            &decision.runtime.open_depth_query_client
        } else {
            decision
                .runtime
                .hedge_depth_query_client
                .as_ref()
                .expect("missing open-side depth query client")
        };
        let tlen_gate = ArbDecision::with_state_mut(|arb| {
            if arb.enable_tlen_cancel {
                arb.tlen_thresholds
                    .get(&query_symbol.to_ascii_uppercase())
                    .copied()
            } else {
                None
            }
        })
        .flatten();
        let from_key_str = std::str::from_utf8(&from_key).unwrap_or("");
        let (from_keys, filtered_levels) = super::common::apply_open_tlen_gate_and_build_from_keys(
            FUNDING_ARB_SHELL_NAME,
            open_depth_query_client,
            if spot_venue == decision.runtime.venues.0 {
                &decision.runtime.open_min_qty_table
            } else {
                &decision.runtime.hedge_min_qty_table
            },
            spot_venue,
            &query_symbol,
            &tick_indices,
            from_key_str,
            tlen_gate,
        );
        contexts = from_keys
            .into_iter()
            .zip(contexts)
            .filter_map(|(from_key_bytes, mut ctx)| {
                let from_key_bytes = from_key_bytes?;
                ctx.set_from_key(from_key_bytes);
                Some(ctx)
            })
            .collect();
        if filtered_levels > 0 {
            let _ = ArbDecision::with_state_mut(|arb| {
                arb.record_intercept_summary_by("tlen_gate", filtered_levels as u64);
            });
            log::debug!(
                "{FUNDING_ARB_SHELL_NAME}: ArbOpen tlen gated symbol={} threshold={:?} filtered_levels={} kept_levels={}",
                query_symbol,
                tlen_gate,
                filtered_levels,
                contexts.len()
            );
        }
    }
    if matches!(signal_type, SignalType::ArbClose) {
        contexts.retain(arb_close_notional_meets_min);
        if contexts.is_empty() {
            return Ok(false);
        }
    }
    let planned_levels = if matches!(signal_type, SignalType::ArbClose) {
        contexts.len()
    } else {
        plan.levels.len()
    };
    let sent = super::arb_emit::emit_levels_as_signals(
        &decision.runtime.signal_pub,
        signal_type.clone(),
        batch_ts,
        contexts,
        |ctx| ctx.to_bytes(),
    )?;

    log::info!(
        "{FUNDING_ARB_SHELL_NAME}: emitted {}/{} {:?} signal(s) to '{}' open={} hedge={} side={:?} inner={:.8} outer={:.8} vol={:.8} vol_band_scale=[{:.4},{:.4}] plan_vol={:.8} price_tick={:.8} qty_tick={:.8}",
        sent,
        planned_levels,
        signal_type,
        DEFAULT_ARBITRAGE_SIGNAL_CHANNEL,
        spot_symbol,
        futures_symbol,
        plan.side,
        plan.inner_price,
        plan.outer_price,
        raw_volatility,
        plan_vol_band_scale[0],
        plan_vol_band_scale[1],
        plan_volatility,
        plan.price_tick,
        plan.qty_tick
    );
    Ok(sent > 0)
}

fn emit_funding_spread_cancel(
    decision: &FundingArbShell,
    spot_symbol: &str,
    futures_symbol: &str,
    spot_venue: TradingVenue,
    futures_venue: TradingVenue,
    side: Side,
) -> Result<()> {
    let (spot_quote, futures_quote) = match ArbDecision::load_valid_quotes(
        spot_symbol,
        futures_symbol,
        spot_venue,
        futures_venue,
    ) {
        Some(quotes) => quotes,
        None => return Ok(()),
    };
    let batch_ts = get_timestamp_us();
    let spread_rate = super::common::compute_spread_rate(&spot_quote, &futures_quote);
    let snapshot = ArbDecision::with_state_mut(|arb| {
        arb.snapshot_open_from_key_fields(
            spot_symbol,
            futures_symbol,
            spot_venue,
            futures_venue,
            batch_ts,
        )
    })
    .unwrap_or_default();
    let premium_rate = super::arb_open_filter::lookup_realtime_open_filter_value(
        spot_symbol,
        futures_symbol,
        spot_venue,
        futures_venue,
    )
    .map(|(value, _)| value);
    let from_key = super::arb_from_key::build_funding_decision_from_key_base(
        batch_ts,
        snapshot.return_qtl,
        snapshot.return_threshold,
        snapshot.volatility,
        snapshot.vol_band_scale,
        snapshot.env_score,
        snapshot.env_threshold,
        futures_symbol,
        futures_venue,
        spread_rate,
        premium_rate,
    );
    super::arb_cancel_emit::emit_precise_arb_cancel(super::arb_cancel_emit::ArbCancelEmitInput {
        signal_pub: &decision.runtime.signal_pub,
        open_symbol: spot_symbol,
        hedge_symbol: futures_symbol,
        open_venue: spot_venue,
        hedge_venue: futures_venue,
        open_quote: &spot_quote,
        hedge_quote: &futures_quote,
        now: batch_ts,
        from_key: &from_key,
        reason: crate::signal::cancel_signal::ArbCancelReason::Spread,
        side,
        strategy_id: 0,
    })
}

thread_local! {
    static ARB_MODE: OnceCell<ArbMode> = const { OnceCell::new() };
    static ARB_DECISION: OnceCell<RefCell<ArbDecisionState>> = const { OnceCell::new() };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArbBackend {
    Funding,
    Spread,
}

pub struct ArbDecision;

struct ArbSignalTableEntry {
    symbol: String,
    pred_fr_pct: f64,
    fr_ma_pct: f64,
    pred_loan_pct: f64,
    cur_loan_pct: f64,
    fr_plus_pred_loan_pct: f64,
    ma_plus_cur_loan_pct: f64,
    fr_sig: &'static str,
    spread_sig: &'static str,
    final_sig: &'static str,
}

#[derive(Debug, Clone)]
pub(crate) struct ArbOpenGatePassed {
    pub return_qtl: Option<f64>,
    pub return_threshold: Option<f64>,
    pub open_filter_value: Option<f64>,
    pub environment_score: f64,
    pub environment_threshold: Option<f64>,
    pub open_volatility_factor: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct ArbOpenControlPassed {
    pub side: Side,
    pub key: ThresholdKey,
    pub gate: ArbOpenGatePassed,
}

#[derive(Debug, Clone, Default)]
struct OpenFromKeySnapshot {
    return_qtl: Option<f64>,
    return_threshold: Option<f64>,
    volatility: Option<f64>,
    env_score: Option<f64>,
    env_threshold: Option<f64>,
    vol_band_scale: Option<[f64; 2]>,
}

#[derive(Debug, Clone)]
struct VenueVolatilityReadiness {
    venue: TradingVenue,
    symbol_key: String,
    key: String,
    value: Option<f64>,
    ready: Option<bool>,
    note: String,
}

impl VenueVolatilityReadiness {
    fn is_ready(&self) -> bool {
        self.ready == Some(true) && self.value.is_some_and(|v| v.is_finite() && v >= 0.0)
    }

    fn reason(&self) -> String {
        format!(
            "venue={:?} symbol={} key={} ready={:?} value={} note={}",
            self.venue,
            self.symbol_key,
            self.key,
            self.ready,
            self.value
                .filter(|v| v.is_finite())
                .map(|v| format!("{v:.8}"))
                .unwrap_or_else(|| "-".to_string()),
            self.note
        )
    }
}

#[derive(Debug, Clone)]
struct DualVenueVolatilityReadiness {
    open: VenueVolatilityReadiness,
    hedge: VenueVolatilityReadiness,
}

impl DualVenueVolatilityReadiness {
    fn is_ready(&self) -> bool {
        self.open.is_ready() && self.hedge.is_ready()
    }

    fn missing_reason(&self) -> String {
        let mut reasons = Vec::new();
        if !self.open.is_ready() {
            reasons.push(format!("open({})", self.open.reason()));
        }
        if !self.hedge.is_ready() {
            reasons.push(format!("hedge({})", self.hedge.reason()));
        }
        reasons.join("; ")
    }
}

#[derive(Debug, Clone)]
struct XarbOpenBlockerRow {
    symbol: String,
    vol_tr: Option<f64>,
    reason: String,
}

fn extract_threshold_from_reason(reason: &str) -> Option<f64> {
    let key = "threshold=";
    let start = reason.find(key)? + key.len();
    let rest = &reason[start..];
    let end = rest.find([',', ')']).unwrap_or(rest.len());
    rest[..end].trim().parse::<f64>().ok()
}

fn format_opt_opt_f64(value: Option<f64>) -> String {
    match value {
        Some(v) if v.is_finite() => format!("{v:.6}"),
        _ => "-".to_string(),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ArbCloseGatePassed {
    pub side: Side,
    pub key: ThresholdKey,
}

#[derive(Debug, Clone)]
pub(crate) enum CloseGateOutcome {
    Pass(ArbCloseGatePassed),
    NoSide,
    Cooldown,
}

#[derive(Debug, Clone)]
pub(crate) struct ArbCancelGatePassed {
    pub key: ThresholdKey,
}

pub(crate) type SignalCooldownKey = (ThresholdKey, u8);

#[derive(Debug, Clone)]
pub(crate) struct ArbFundingControlPassed {
    pub final_signal: SignalType,
    pub side: Side,
    pub key: ThresholdKey,
    pub gate: Option<ArbOpenGatePassed>,
}

#[derive(Debug, Clone)]
pub(crate) struct ArbSharedBootstrap {
    pub vol_band_scale: [f64; 2],
    pub open_orders_per_round: u32,
    pub order_amount: f32,
    pub amount_u_overrides: HashMap<String, f64>,
    pub open_offset_lower_overrides: HashMap<String, f64>,
    pub open_order_ttl_us: i64,
    pub hedge_timeout_mm_us: i64,
    pub hedge_aggressive_seq_threshold: u32,
    pub enable_tlen_cancel: bool,
    pub tlen_cancel_freq_ms: u64,
    pub signal_cooldown_us: i64,
    pub spread_cancel_cooldown_us: i64,
    pub last_open_ts: Rc<RefCell<HashMap<SignalCooldownKey, i64>>>,
    pub last_close_ts: Rc<RefCell<HashMap<SignalCooldownKey, i64>>>,
}

pub(crate) struct ArbDecisionState {
    pub venues: VenuePair,
    pub open_factor_value_hub: Option<FactorValueHub>,
    pub hedge_factor_value_hub: Option<FactorValueHub>,
    pub model_output_hub: Option<ModelOutputHub>,
    pub vol_band_scale: [f64; 2],
    pub open_orders_per_round: u32,
    pub order_amount: f32,
    /// per-symbol amount_u 覆盖（USDT），由 strategy_loader 从 Redis 拉取。
    /// 命中即覆盖 order_amount；未命中按 order_amount 走默认。
    pub amount_u_overrides: HashMap<String, f64>,
    /// per-symbol open_offset_lower 覆盖（价格分数）。
    /// 命中即抬升 build_arb_open_quote_plan 的内层 start = max(scale[0]*vol, lower)；
    /// 未命中按全局走（lower=0，行为与本字段加入前一致）。
    pub open_offset_lower_overrides: HashMap<String, f64>,
    pub open_order_ttl_us: i64,
    pub hedge_timeout_mm_us: i64,
    pub hedge_vol_multiplier: f64,
    pub hedge_offset_ratio: f64,
    pub hedge_price_offset_limit_lower: f64,
    pub hedge_price_offset_limit_upper: f64,
    /// per-symbol hedge_price_offset_limit_lower 覆盖（合并 STRING / 拆分 STRING 加载）。
    /// 命中即覆盖 hedge_price_offset_limit_lower；未命中按全局默认走。
    pub hedge_price_offset_limit_lower_overrides: HashMap<String, f64>,
    /// per-symbol hedge_price_offset_limit_upper 覆盖。
    pub hedge_price_offset_limit_upper_overrides: HashMap<String, f64>,
    pub hedge_window_scale_low: f64,
    pub hedge_window_scale_high: f64,
    pub enable_return_score_adjust_hedge: bool,
    pub hedge_aggressive_seq_threshold: u32,
    pub max_hedge_price_pct_change: f64,
    pub enable_tlen_cancel: bool,
    pub tlen_cancel_freq_ms: u64,
    pub enable_funding_open_filter: bool,
    pub enable_environment_model: bool,
    /// 是否启用 open vol 阈值 gate；与 mm 同语义：开仓时若 inline vol percentile 阈值
    /// 已 warm up 且当前 vol 超阈值，则拦截开仓。
    pub enable_volatility_limit: bool,
    /// open vol 阈值采样使用的分位数（0-100）。
    pub open_volatility_limit: f64,
    pub return_model_service: Option<String>,
    pub environment_model_service: Option<String>,
    pub environment_model_true_threshold: f64,
    pub funding_open_thresholds: HashMap<String, FundingThresholdsResolved>,
    /// Funding 因子链。每次 reload threshold 时由 config_loader 整体替换。链上 enabled=false
    /// 的因子在评估时跳过。因子名必须在 `arb_open_filter::lookup_factor_realtime_value`
    /// 中有对应取数路径,否则报 `miss_<factor>_value`。
    pub funding_factor_chain: Vec<StoredFactorChainEntry>,
    pub tlen_thresholds: HashMap<String, f64>,
    pub signal_cooldown_us: i64,
    pub spread_cancel_cooldown_us: i64,
    pub last_open_ts: Rc<RefCell<HashMap<SignalCooldownKey, i64>>>,
    pub last_close_ts: Rc<RefCell<HashMap<SignalCooldownKey, i64>>>,
    pub last_spread_cancel_ts: Rc<RefCell<HashMap<(ThresholdKey, u8), i64>>>,
    pub last_tlen_threshold_reload_ts_us: i64,
    pub last_cancel_trigger_ts_us: i64,
    pub intercept_counts: HashMap<String, u64>,
    pub last_intercept_log: Instant,
    pub tlen_cancel_summaries: HashMap<(String, String), TlenCancelSummary>,
    pub last_tlen_cancel_log: Instant,
    /// 30s 窗口内的 spread 观测：每个 symbol 记录 forward/backward 两个方向的
    /// 实际 spread 值 min/max，并保留最新看到的 open 阈值，便于在 summary 后打表
    /// 直观判断"信号设计是否合理（区间能否覆盖阈值）"。
    pub spread_observation: HashMap<String, SpreadObservation>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SpreadObservation {
    pub fwd_min: Option<f64>,
    pub fwd_max: Option<f64>,
    pub fwd_threshold: Option<f64>,
    pub bwd_min: Option<f64>,
    pub bwd_max: Option<f64>,
    pub bwd_threshold: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct TlenCancelSummary {
    pub trigger_count: u64,
    pub candidate_count: u64,
    pub matched_count: u64,
    pub threshold: Option<f64>,
    pub min_tlen: Option<f64>,
    pub max_tlen: Option<f64>,
    pub last_trigger_ts: i64,
    pub sample_matches: Vec<String>,
}

impl SpreadObservation {
    fn record_fwd(&mut self, value: f64, threshold: f64) {
        self.fwd_threshold = Some(threshold);
        self.fwd_min = Some(self.fwd_min.map_or(value, |m| m.min(value)));
        self.fwd_max = Some(self.fwd_max.map_or(value, |m| m.max(value)));
    }

    fn record_bwd(&mut self, value: f64, threshold: f64) {
        self.bwd_threshold = Some(threshold);
        self.bwd_min = Some(self.bwd_min.map_or(value, |m| m.min(value)));
        self.bwd_max = Some(self.bwd_max.map_or(value, |m| m.max(value)));
    }
}

impl ArbDecisionState {
    pub fn new(_mode: ArbMode, venues: VenuePair) -> Self {
        Self {
            venues,
            open_factor_value_hub: None,
            hedge_factor_value_hub: None,
            model_output_hub: None,
            vol_band_scale: [0.0, 1.0],
            open_orders_per_round: 1,
            order_amount: 100.0,
            amount_u_overrides: HashMap::new(),
            open_offset_lower_overrides: HashMap::new(),
            open_order_ttl_us: 120_000_000,
            hedge_timeout_mm_us: 30_000_000,
            hedge_vol_multiplier: 2.0,
            hedge_offset_ratio: 1.3,
            hedge_price_offset_limit_lower: 0.0003,
            hedge_price_offset_limit_upper: 0.005,
            hedge_price_offset_limit_lower_overrides: HashMap::new(),
            hedge_price_offset_limit_upper_overrides: HashMap::new(),
            hedge_window_scale_low: 0.5,
            hedge_window_scale_high: 1.5,
            enable_return_score_adjust_hedge: true,
            hedge_aggressive_seq_threshold: 6,
            max_hedge_price_pct_change: 5.0,
            enable_tlen_cancel: false,
            tlen_cancel_freq_ms: 3_000,
            enable_funding_open_filter: true,
            enable_environment_model: true,
            enable_volatility_limit: true,
            open_volatility_limit: 70.0,
            return_model_service: None,
            environment_model_service: None,
            environment_model_true_threshold: 0.0,
            funding_open_thresholds: HashMap::new(),
            funding_factor_chain: Vec::new(),
            tlen_thresholds: HashMap::new(),
            signal_cooldown_us: 5_000_000,
            spread_cancel_cooldown_us: 0,
            last_open_ts: Rc::new(RefCell::new(HashMap::new())),
            last_close_ts: Rc::new(RefCell::new(HashMap::new())),
            last_spread_cancel_ts: Rc::new(RefCell::new(HashMap::new())),
            last_tlen_threshold_reload_ts_us: 0,
            last_cancel_trigger_ts_us: 0,
            intercept_counts: HashMap::new(),
            last_intercept_log: Instant::now(),
            tlen_cancel_summaries: HashMap::new(),
            last_tlen_cancel_log: Instant::now(),
            spread_observation: HashMap::new(),
        }
    }

    pub fn record_spread_observation_fwd(&mut self, symbol: &str, value: f64, threshold: f64) {
        self.spread_observation
            .entry(symbol.to_string())
            .or_default()
            .record_fwd(value, threshold);
    }

    pub fn record_spread_observation_bwd(&mut self, symbol: &str, value: f64, threshold: f64) {
        self.spread_observation
            .entry(symbol.to_string())
            .or_default()
            .record_bwd(value, threshold);
    }

    pub fn poll_model_output_updates(&mut self) {
        if let Some(hub) = self.model_output_hub.as_mut() {
            hub.poll_updates();
        }
    }

    pub fn poll_factor_value_updates(&mut self) {
        // open hub 把 percentile 传进去，让 FactorValueHub 在每次推进 vol 时
        // 顺手把 (symbol, value) 喂给 inline_volatility 的 thread_local store；
        // hedge hub 不参与 gate，保持 None。
        let percentile = if self.enable_volatility_limit {
            Some(self.open_volatility_limit)
        } else {
            None
        };
        if let Some(hub) = self.open_factor_value_hub.as_mut() {
            let _ = hub.poll_factor_value_updates_with_inline_sampling(percentile);
        }
        if let Some(hub) = self.hedge_factor_value_hub.as_mut() {
            let _ = hub.poll_factor_value_updates_with_inline_sampling(None);
        }
    }

    pub fn update_enable_volatility_limit(&mut self, enabled: bool) {
        self.enable_volatility_limit = enabled;
        log::debug!(
            "ArbDecision: enable_volatility_limit updated enabled={}",
            self.enable_volatility_limit
        );
    }

    pub fn update_open_volatility_limit(&mut self, percentile: f64) {
        if !(percentile.is_finite() && (0.0..=100.0).contains(&percentile)) {
            log::warn!(
                "ArbDecision: open_volatility_limit must be finite and within [0,100], got {}; keep previous {}",
                percentile, self.open_volatility_limit
            );
            return;
        }
        self.open_volatility_limit = percentile;
        log::debug!(
            "ArbDecision: open_volatility_limit updated percentile={}",
            self.open_volatility_limit
        );
    }

    pub fn evaluate_close_side(
        spread_factor: &super::spread_factor::SpreadFactor,
        open_symbol_key: &str,
        hedge_symbol_key: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Option<Side> {
        let forward_close = spread_factor.satisfy_forward_close(
            open_venue,
            open_symbol_key,
            hedge_venue,
            hedge_symbol_key,
        );
        let backward_close = spread_factor.satisfy_backward_close(
            open_venue,
            open_symbol_key,
            hedge_venue,
            hedge_symbol_key,
        );
        if forward_close {
            Some(Side::Sell)
        } else if backward_close {
            Some(Side::Buy)
        } else {
            None
        }
    }

    pub fn evaluate_close_gate(
        &self,
        spread_factor: &super::spread_factor::SpreadFactor,
        open_symbol_key: &str,
        hedge_symbol_key: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        now: i64,
    ) -> CloseGateOutcome {
        let Some(side) = Self::evaluate_close_side(
            spread_factor,
            open_symbol_key,
            hedge_symbol_key,
            open_venue,
            hedge_venue,
        ) else {
            return CloseGateOutcome::NoSide;
        };
        let key =
            Self::build_threshold_key(open_symbol_key, hedge_symbol_key, open_venue, hedge_venue);
        if self.is_close_cooldown_hit(&key, side, now) {
            return CloseGateOutcome::Cooldown;
        }
        CloseGateOutcome::Pass(ArbCloseGatePassed { side, key })
    }

    pub fn evaluate_cancel_gate(
        &self,
        forward_cancel: bool,
        backward_cancel: bool,
        open_symbol_key: &str,
        hedge_symbol_key: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Option<ArbCancelGatePassed> {
        if !forward_cancel && !backward_cancel {
            return None;
        }
        let key =
            Self::build_threshold_key(open_symbol_key, hedge_symbol_key, open_venue, hedge_venue);
        Some(ArbCancelGatePassed { key })
    }

    /// 解析 per-symbol amount_u：命中覆盖即返回覆盖值，否则回退到 order_amount。
    /// 与 MM 的 resolve_order_amount_u 同语义；symbol 会按 open venue 规范化以匹配
    /// strategy_loader 里写入 overrides 时使用的同一规范键。
    pub fn resolve_order_amount_u(&self, symbol: &str) -> f64 {
        let symbol_key = normalize_symbol_for_venue(symbol, self.venues.0);
        self.amount_u_overrides
            .get(&symbol_key)
            .copied()
            .unwrap_or(self.order_amount as f64)
    }

    /// 解析 per-symbol open_offset_lower：命中覆盖即返回覆盖值，否则回退到 0.0（不 clamp）。
    /// symbol 按 open venue 规范化以匹配 strategy_loader 写入 overrides 时使用的同一规范键。
    pub fn resolve_open_offset_lower(&self, symbol: &str) -> f64 {
        let symbol_key = normalize_symbol_for_venue(symbol, self.venues.0);
        self.open_offset_lower_overrides
            .get(&symbol_key)
            .copied()
            .unwrap_or(0.0)
    }

    /// 解析 per-symbol hedge_price_offset_limit (lower, upper)：命中覆盖即返回覆盖值，
    /// 未命中回退到全局 hedge_price_offset_limit_lower / hedge_price_offset_limit_upper。
    /// 与 MM 的 resolve_hedge_price_offset_limits 同语义。
    pub fn resolve_hedge_price_offset_limits(&self, symbol: &str) -> (f64, f64) {
        let symbol_key = normalize_symbol_for_venue(symbol, self.venues.0);
        let lower = self
            .hedge_price_offset_limit_lower_overrides
            .get(&symbol_key)
            .copied()
            .unwrap_or(self.hedge_price_offset_limit_lower);
        let upper = self
            .hedge_price_offset_limit_upper_overrides
            .get(&symbol_key)
            .copied()
            .unwrap_or(self.hedge_price_offset_limit_upper);
        (lower, upper)
    }

    /// 设置 per-symbol hedge_price_offset_limit 覆盖表，并对每个符号验证有效区间。
    /// 不通过 panic 是为了不让"配错的 redis 数据"打死整个进程；非法记录被丢弃并 warn。
    pub fn update_hedge_price_offset_limit_overrides(
        &mut self,
        lower_overrides: HashMap<String, f64>,
        upper_overrides: HashMap<String, f64>,
    ) {
        let mut valid_lower: HashMap<String, f64> = HashMap::with_capacity(lower_overrides.len());
        let mut valid_upper: HashMap<String, f64> = HashMap::with_capacity(upper_overrides.len());
        let mut warned = 0usize;
        let mut symbols: std::collections::HashSet<String> = std::collections::HashSet::new();
        symbols.extend(lower_overrides.keys().cloned());
        symbols.extend(upper_overrides.keys().cloned());
        for symbol in symbols {
            if symbol.trim().is_empty() {
                warned += 1;
                continue;
            }
            let lower = lower_overrides
                .get(&symbol)
                .copied()
                .unwrap_or(self.hedge_price_offset_limit_lower);
            let upper = upper_overrides
                .get(&symbol)
                .copied()
                .unwrap_or(self.hedge_price_offset_limit_upper);
            if !(lower.is_finite() && upper.is_finite() && lower > 0.0 && upper >= lower) {
                log::warn!(
                    "ArbDecision: drop invalid hedge_price_offset_limit override symbol={} lower={} upper={} (need 0<lower<=upper)",
                    symbol, lower, upper
                );
                warned += 1;
                continue;
            }
            if let Some(v) = lower_overrides.get(&symbol) {
                valid_lower.insert(symbol.clone(), *v);
            }
            if let Some(v) = upper_overrides.get(&symbol) {
                valid_upper.insert(symbol, *v);
            }
        }
        self.hedge_price_offset_limit_lower_overrides = valid_lower;
        self.hedge_price_offset_limit_upper_overrides = valid_upper;
        log::debug!(
            "ArbDecision: hedge_price_offset_limit_overrides updated lower_symbols={} upper_symbols={} dropped={}",
            self.hedge_price_offset_limit_lower_overrides.len(),
            self.hedge_price_offset_limit_upper_overrides.len(),
            warned
        );
    }

    pub fn apply_shared_bootstrap(&mut self, bootstrap: ArbSharedBootstrap) {
        self.vol_band_scale = bootstrap.vol_band_scale;
        self.open_orders_per_round = bootstrap.open_orders_per_round;
        self.order_amount = bootstrap.order_amount;
        self.amount_u_overrides = bootstrap.amount_u_overrides;
        self.open_offset_lower_overrides = bootstrap.open_offset_lower_overrides;
        self.open_order_ttl_us = bootstrap.open_order_ttl_us;
        self.hedge_timeout_mm_us = bootstrap.hedge_timeout_mm_us;
        self.hedge_aggressive_seq_threshold = bootstrap.hedge_aggressive_seq_threshold;
        self.enable_tlen_cancel = bootstrap.enable_tlen_cancel;
        self.tlen_cancel_freq_ms = bootstrap.tlen_cancel_freq_ms;
        self.signal_cooldown_us = bootstrap.signal_cooldown_us;
        self.spread_cancel_cooldown_us = bootstrap.spread_cancel_cooldown_us;
        self.last_open_ts = bootstrap.last_open_ts;
        self.last_close_ts = bootstrap.last_close_ts;
    }

    pub fn default_shared_bootstrap(open_orders_per_round: u32) -> ArbSharedBootstrap {
        ArbSharedBootstrap {
            vol_band_scale: [0.0, 1.0],
            open_orders_per_round,
            order_amount: 100.0,
            amount_u_overrides: HashMap::new(),
            open_offset_lower_overrides: HashMap::new(),
            open_order_ttl_us: 120_000_000,
            hedge_timeout_mm_us: 30_000_000,
            hedge_aggressive_seq_threshold: 6,
            enable_tlen_cancel: false,
            tlen_cancel_freq_ms: 3_000,
            signal_cooldown_us: 5_000_000,
            spread_cancel_cooldown_us: 0,
            last_open_ts: Rc::new(RefCell::new(HashMap::new())),
            last_close_ts: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    pub fn evaluate_open_side(
        spread_factor: &super::spread_factor::SpreadFactor,
        symbol_list: &super::symbol_list::SymbolList,
        open_symbol_key: &str,
        hedge_symbol_key: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        in_dump: bool,
    ) -> (bool, bool, Option<Side>) {
        let forward_open = !in_dump
            && spread_factor.satisfy_forward_open(
                open_venue,
                open_symbol_key,
                hedge_venue,
                hedge_symbol_key,
            )
            && symbol_list.is_in_fwd_trade_list(open_symbol_key);
        let backward_open = !in_dump
            && spread_factor.satisfy_backward_open(
                open_venue,
                open_symbol_key,
                hedge_venue,
                hedge_symbol_key,
            )
            && symbol_list.is_in_bwd_trade_list(open_symbol_key);

        let side = if forward_open {
            Some(Side::Buy)
        } else if backward_open {
            Some(Side::Sell)
        } else {
            None
        };
        (forward_open, backward_open, side)
    }

    pub fn evaluate_open_control(
        &mut self,
        spread_factor: &super::spread_factor::SpreadFactor,
        symbol_list: &super::symbol_list::SymbolList,
        open_symbol_key: &str,
        hedge_symbol_key: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        now: i64,
        in_dump: bool,
    ) -> Option<ArbOpenControlPassed> {
        let (forward_open, backward_open, side) = Self::evaluate_open_side(
            spread_factor,
            symbol_list,
            open_symbol_key,
            hedge_symbol_key,
            open_venue,
            hedge_venue,
            in_dump,
        );
        if side.is_none()
            && !in_dump
            && (symbol_list.is_in_fwd_trade_list(open_symbol_key)
                || symbol_list.is_in_bwd_trade_list(open_symbol_key))
        {
            self.record_intercept_summary("spread_block");
        }
        let side = side?;
        self.record_intercept_summary("spread_hit");
        let key =
            Self::build_threshold_key(open_symbol_key, hedge_symbol_key, open_venue, hedge_venue);
        if self.is_open_cooldown_hit(&key, side, now) {
            self.record_intercept_summary("cooldown");
            return None;
        }
        let gate = self.evaluate_open_gate(
            open_symbol_key,
            hedge_symbol_key,
            hedge_symbol,
            open_venue,
            hedge_venue,
            side,
            now,
            forward_open,
            backward_open,
        );
        let gate = gate?;
        Some(ArbOpenControlPassed { side, key, gate })
    }

    pub fn evaluate_funding_final_signal(
        fr_signal: ArbSignalKind,
        spread_factor: &super::spread_factor::SpreadFactor,
        symbol_list: &super::symbol_list::SymbolList,
        open_symbol_key: &str,
        hedge_symbol_key: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        open_inputs_ready: bool,
        in_dump: bool,
    ) -> Option<crate::signal::trade_signal::SignalType> {
        match fr_signal {
            ArbSignalKind::ForwardOpen => {
                if !open_inputs_ready || in_dump {
                    return None;
                }
                let spread_ok = spread_factor.satisfy_forward_open(
                    open_venue,
                    open_symbol_key,
                    hedge_venue,
                    hedge_symbol_key,
                );
                let in_trade_list = symbol_list.is_in_fwd_trade_list(open_symbol_key);
                if spread_ok && in_trade_list {
                    Some(crate::signal::trade_signal::SignalType::ArbOpen)
                } else {
                    None
                }
            }
            ArbSignalKind::ForwardClose => spread_factor
                .satisfy_forward_close(open_venue, open_symbol_key, hedge_venue, hedge_symbol_key)
                .then_some(crate::signal::trade_signal::SignalType::ArbClose),
            ArbSignalKind::BackwardOpen => {
                if !open_inputs_ready || in_dump {
                    return None;
                }
                let spread_ok = spread_factor.satisfy_backward_open(
                    open_venue,
                    open_symbol_key,
                    hedge_venue,
                    hedge_symbol_key,
                );
                let in_trade_list = symbol_list.is_in_bwd_trade_list(open_symbol_key);
                if spread_ok && in_trade_list {
                    Some(crate::signal::trade_signal::SignalType::ArbOpen)
                } else {
                    None
                }
            }
            ArbSignalKind::BackwardClose => spread_factor
                .satisfy_backward_close(open_venue, open_symbol_key, hedge_venue, hedge_symbol_key)
                .then_some(crate::signal::trade_signal::SignalType::ArbClose),
        }
    }

    pub fn side_from_funding_signal(fr_signal: ArbSignalKind) -> Side {
        match fr_signal {
            ArbSignalKind::ForwardOpen => Side::Buy,
            ArbSignalKind::BackwardOpen => Side::Sell,
            ArbSignalKind::ForwardClose => Side::Sell,
            ArbSignalKind::BackwardClose => Side::Buy,
        }
    }

    pub fn evaluate_funding_control(
        &mut self,
        fr_signal: ArbSignalKind,
        spread_factor: &super::spread_factor::SpreadFactor,
        symbol_list: &super::symbol_list::SymbolList,
        open_symbol_key: &str,
        hedge_symbol_key: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        open_inputs_ready: bool,
        in_dump: bool,
        now: i64,
    ) -> Option<ArbFundingControlPassed> {
        let final_signal = Self::evaluate_funding_final_signal(
            fr_signal,
            spread_factor,
            symbol_list,
            open_symbol_key,
            hedge_symbol_key,
            open_venue,
            hedge_venue,
            open_inputs_ready,
            in_dump,
        )?;
        let side = Self::side_from_funding_signal(fr_signal);
        let key =
            Self::build_threshold_key(open_symbol_key, hedge_symbol_key, open_venue, hedge_venue);
        if self.is_signal_cooldown_hit(&final_signal, &key, side, now) {
            return None;
        }
        let gate = if matches!(final_signal, SignalType::ArbOpen) {
            Some(self.evaluate_open_gate(
                open_symbol_key,
                hedge_symbol_key,
                hedge_symbol_key,
                open_venue,
                hedge_venue,
                side,
                now,
                matches!(fr_signal, ArbSignalKind::ForwardOpen),
                matches!(fr_signal, ArbSignalKind::BackwardOpen),
            )?)
        } else {
            None
        };
        Some(ArbFundingControlPassed {
            final_signal,
            side,
            key,
            gate,
        })
    }

    pub fn is_open_cooldown_hit(&self, key: &ThresholdKey, side: Side, now: i64) -> bool {
        is_cooldown_hit(
            &self.last_open_ts,
            &(key.clone(), side.to_u8()),
            now,
            self.signal_cooldown_us,
        )
    }

    pub fn is_close_cooldown_hit(&self, key: &ThresholdKey, side: Side, now: i64) -> bool {
        is_cooldown_hit(
            &self.last_close_ts,
            &(key.clone(), side.to_u8()),
            now,
            self.signal_cooldown_us,
        )
    }

    pub fn is_spread_cancel_cooldown_hit(&self, key: &ThresholdKey, side: Side, now: i64) -> bool {
        if self.spread_cancel_cooldown_us <= 0 {
            return false;
        }
        self.last_spread_cancel_ts
            .borrow()
            .get(&(key.clone(), side.to_u8()))
            .is_some_and(|&last_ts| now - last_ts < self.spread_cancel_cooldown_us)
    }

    pub fn build_threshold_key(
        open_symbol_key: &str,
        hedge_symbol_key: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> ThresholdKey {
        threshold_key(open_symbol_key, hedge_symbol_key, open_venue, hedge_venue)
    }

    pub fn mark_open_triggered(&self, key: ThresholdKey, side: Side, now: i64) {
        update_last_ts(&self.last_open_ts, (key, side.to_u8()), now);
    }

    pub fn mark_close_triggered(&self, key: ThresholdKey, side: Side, now: i64) {
        update_last_ts(&self.last_close_ts, (key, side.to_u8()), now);
    }

    pub fn mark_spread_cancel_triggered(&self, key: ThresholdKey, side: Side, now: i64) {
        self.last_spread_cancel_ts
            .borrow_mut()
            .insert((key, side.to_u8()), now);
    }

    pub fn is_signal_cooldown_hit(
        &self,
        signal_type: &SignalType,
        key: &ThresholdKey,
        side: Side,
        now: i64,
    ) -> bool {
        match signal_type {
            SignalType::ArbOpen => self.is_open_cooldown_hit(key, side, now),
            SignalType::ArbClose => self.is_close_cooldown_hit(key, side, now),
            _ => false,
        }
    }

    pub fn mark_signal_triggered(
        &self,
        signal_type: &SignalType,
        key: ThresholdKey,
        side: Side,
        now: i64,
    ) {
        match signal_type {
            SignalType::ArbOpen => self.mark_open_triggered(key, side, now),
            SignalType::ArbClose => self.mark_close_triggered(key, side, now),
            _ => {}
        }
    }

    pub fn tlen_threshold_reload_due(&self, now_us: i64) -> bool {
        const RELOAD_INTERVAL_US: i64 = 30 * 60 * 1_000_000;
        self.last_tlen_threshold_reload_ts_us == 0
            || now_us.saturating_sub(self.last_tlen_threshold_reload_ts_us) >= RELOAD_INTERVAL_US
    }

    pub fn should_emit_cancel_trigger(&self, now_us: i64) -> bool {
        if !self.enable_tlen_cancel || self.tlen_cancel_freq_ms == 0 {
            return false;
        }
        let interval_us = (self.tlen_cancel_freq_ms as i64).saturating_mul(1_000);
        self.last_cancel_trigger_ts_us == 0
            || now_us.saturating_sub(self.last_cancel_trigger_ts_us) >= interval_us
    }

    pub fn lookup_open_factor_value(
        &mut self,
        open_symbol: &str,
        open_venue: TradingVenue,
    ) -> FactorValueLookupResult {
        self.open_factor_value_hub
            .as_mut()
            .expect("ArbDecisionState.open_factor_value_hub must be initialized")
            .lookup_factor_value(open_symbol, open_venue)
    }

    pub fn lookup_hedge_factor_value(
        &mut self,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
    ) -> FactorValueLookupResult {
        self.hedge_factor_value_hub
            .as_mut()
            .expect("ArbDecisionState.hedge_factor_value_hub must be initialized")
            .lookup_factor_value_with_last_valid_fallback(hedge_symbol, hedge_venue)
    }

    fn lookup_dual_venue_volatility_readiness(
        &mut self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> DualVenueVolatilityReadiness {
        let open_lookup = self.lookup_open_factor_value(open_symbol, open_venue);
        let hedge_lookup = self.lookup_hedge_factor_value(hedge_symbol, hedge_venue);
        DualVenueVolatilityReadiness {
            open: VenueVolatilityReadiness {
                venue: open_venue,
                symbol_key: open_lookup.symbol_key,
                key: open_lookup.key,
                value: open_lookup.target_factor_value,
                ready: open_lookup.ready,
                note: open_lookup.note,
            },
            hedge: VenueVolatilityReadiness {
                venue: hedge_venue,
                symbol_key: hedge_lookup.symbol_key,
                key: hedge_lookup.key,
                value: hedge_lookup.target_factor_value,
                ready: hedge_lookup.ready,
                note: hedge_lookup.note,
            },
        }
    }

    pub fn lookup_return_model_score_lookup(
        &mut self,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
    ) -> Option<ModelOutputScoreLookupResult> {
        let service_name = self.return_model_service.clone()?;
        Some(
            self.model_output_hub
                .as_mut()
                .expect("ArbDecisionState.model_output_hub must be initialized")
                .lookup_score(&service_name, hedge_symbol, hedge_venue),
        )
    }

    pub fn lookup_funding_open_thresholds(
        &self,
        symbol_key: &str,
    ) -> Option<&FundingThresholdsResolved> {
        self.funding_open_thresholds
            .get(&symbol_key.to_ascii_uppercase())
    }

    fn snapshot_open_from_key_fields(
        &mut self,
        open_symbol_key: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        now_us: i64,
    ) -> OpenFromKeySnapshot {
        let return_lookup = self.lookup_return_model_score_lookup(hedge_symbol, hedge_venue);
        let return_qtl = return_lookup
            .as_ref()
            .and_then(|lookup| lookup.score_quantile)
            .filter(|v| v.is_finite());
        let return_threshold = None;
        let environment_signal =
            self.evaluate_environment_signal(open_symbol_key, hedge_symbol, hedge_venue, now_us);
        let env_score = environment_signal
            .score
            .or(Some(environment_signal.class_label as f64))
            .filter(|v| v.is_finite());
        let volatility = self
            .lookup_open_factor_value(open_symbol_key, open_venue)
            .target_factor_value
            .filter(|v| v.is_finite());

        OpenFromKeySnapshot {
            return_qtl,
            return_threshold,
            volatility,
            env_score,
            env_threshold: environment_signal.threshold,
            vol_band_scale: Some(self.vol_band_scale),
        }
    }

    pub fn evaluate_environment_signal(
        &mut self,
        open_symbol_key: &str,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
        now_us: i64,
    ) -> EnvironmentSignalResult {
        let env_service = self.environment_model_service.clone();
        let threshold = self.environment_model_true_threshold;
        let model_hub = self
            .model_output_hub
            .as_ref()
            .expect("ArbDecisionState.model_output_hub must be initialized");
        let hedge_hub = self
            .hedge_factor_value_hub
            .as_mut()
            .expect("ArbDecisionState.hedge_factor_value_hub must be initialized");
        hedge_hub.evaluate_environment_signal(
            model_hub,
            env_service.as_deref(),
            hedge_symbol,
            hedge_venue,
            threshold,
            open_symbol_key,
            now_us,
        )
    }

    pub fn record_intercept_summary(&mut self, reason: impl Into<String>) {
        self.record_intercept_summary_by(reason, 1);
    }

    pub fn record_intercept_summary_by(&mut self, reason: impl Into<String>, count: u64) {
        if count == 0 {
            return;
        }
        *self.intercept_counts.entry(reason.into()).or_insert(0) += count;
    }

    pub fn record_tlen_cancel_summary(
        &mut self,
        source: &str,
        symbol: &str,
        trigger_ts: i64,
        candidates: usize,
        matched: usize,
        threshold: f64,
        min_tlen: f64,
        max_tlen: f64,
        matched_preview: &[String],
    ) {
        let summary = self
            .tlen_cancel_summaries
            .entry((source.to_string(), symbol.to_string()))
            .or_default();
        summary.trigger_count += 1;
        summary.candidate_count += candidates as u64;
        summary.matched_count += matched as u64;
        if threshold.is_finite() {
            summary.threshold = Some(threshold);
        }
        if min_tlen.is_finite() {
            summary.min_tlen = Some(
                summary
                    .min_tlen
                    .map_or(min_tlen, |value| value.min(min_tlen)),
            );
        }
        if max_tlen.is_finite() {
            summary.max_tlen = Some(
                summary
                    .max_tlen
                    .map_or(max_tlen, |value| value.max(max_tlen)),
            );
        }
        summary.last_trigger_ts = trigger_ts;

        let remaining = 12usize.saturating_sub(summary.sample_matches.len());
        for item in matched_preview.iter().take(remaining) {
            summary.sample_matches.push(item.clone());
        }
    }

    pub fn maybe_log_tlen_cancel_summary(&mut self) {
        const TLEN_CANCEL_LOG_INTERVAL_SECS: u64 = 30;
        if self.tlen_cancel_summaries.is_empty() {
            return;
        }
        if self.last_tlen_cancel_log.elapsed() < Duration::from_secs(TLEN_CANCEL_LOG_INTERVAL_SECS)
        {
            return;
        }

        self.tlen_cancel_summaries.clear();
        self.last_tlen_cancel_log = Instant::now();
    }

    pub fn maybe_log_intercept_summary(&mut self, source: &str) {
        const INTERCEPT_LOG_INTERVAL_SECS: u64 = 30;
        if self.intercept_counts.is_empty() {
            return;
        }
        if self.last_intercept_log.elapsed() < Duration::from_secs(INTERCEPT_LOG_INTERVAL_SECS) {
            return;
        }

        let mut rows: Vec<(String, u64)> = self
            .intercept_counts
            .iter()
            .map(|(reason, count)| (reason.clone(), *count))
            .collect();
        if rows.is_empty() {
            self.intercept_counts.clear();
            self.last_intercept_log = Instant::now();
            return;
        }
        rows.sort_by(|a, b| {
            let priority = |reason: &str| match reason {
                "spread_seen" => 0u8,
                "spread_cancel" => 1u8,
                "spread_cancel_cooldown" => 2u8,
                "spread_close" => 3u8,
                "spread_block" => 4u8,
                "spread_hit" => 5u8,
                "tlen_gate" => 6u8,
                "cooldown" => 7u8,
                _ => 8u8,
            };
            priority(&a.0)
                .cmp(&priority(&b.0))
                .then_with(|| b.1.cmp(&a.1))
                .then_with(|| a.0.cmp(&b.0))
        });

        let reason_width = rows
            .iter()
            .map(|(reason, _)| reason.len())
            .max()
            .unwrap_or(6)
            .max("reason".len());
        let count_width = rows
            .iter()
            .map(|(_, count)| count.to_string().len())
            .max()
            .unwrap_or(5)
            .max("count".len());

        let fmt_row = |reason: &str, count: &str| -> String {
            format!(
                "{:<reason_width$}  {:>count_width$}",
                reason,
                count,
                reason_width = reason_width,
                count_width = count_width
            )
        };

        let header = fmt_row("reason", "count");
        let rule = "=".repeat(header.len());
        let mid = "-".repeat(header.len());

        log::info!(
            "{source}: xarb open summary (last {}s)",
            INTERCEPT_LOG_INTERVAL_SECS
        );
        log::info!("{rule}");
        log::info!("{header}");
        log::info!("{mid}");
        for (reason, count) in rows {
            log::info!("{}", fmt_row(&reason, &count.to_string()));
        }
        log::info!("{rule}");

        self.log_vol_coverage(source);
        self.log_xarb_open_blocker_table(source);
        self.log_spread_observation_table(source);

        self.intercept_counts.clear();
        self.spread_observation.clear();
        self.last_intercept_log = Instant::now();
    }

    /// 直接从 open 端 FactorValueHub 缓存里数：fresh / total_seen / stale 名单。
    /// total_seen = 曾收到过 IPC snapshot 的 (target_factor, symbol) 数量；
    /// fresh = 其中 ready=true 且未过期；stale = 进过 cache 但当前不 fresh 的 symbol（全列）。
    /// blocker 表里报 `vol` 的 symbol 应该是"从来没进过 cache"的，跟 stale 是两组。
    fn log_vol_coverage(&mut self, source: &str) {
        let Some(hub) = self.open_factor_value_hub.as_ref() else {
            return;
        };
        let service = hub.factor_value_service_name().to_string();
        let now_ms = get_timestamp_us() / 1000;
        let (fresh, total, stale) = hub.vol_ready_summary(now_ms);
        log::info!(
            "{source}: vol coverage service={} fresh={}/{} stale_count={} stale=[{}]",
            service,
            fresh,
            total,
            stale.len(),
            stale.join(",")
        );
    }

    fn log_spread_observation_table(&self, source: &str) {
        if self.spread_observation.is_empty() {
            return;
        }

        let mut symbols: Vec<&String> = self.spread_observation.keys().collect();
        symbols.sort();

        let fmt_opt = |v: Option<f64>| -> String {
            match v {
                Some(value) if value.is_finite() => format!("{value:.6}"),
                _ => "-".to_string(),
            }
        };

        let headers = [
            "symbol", "fwd_min", "fwd_max", "fwd_open", "bwd_min", "bwd_max", "bwd_open",
        ];
        let mut widths = headers.iter().map(|h| h.len()).collect::<Vec<_>>();
        let rows: Vec<[String; 7]> = symbols
            .iter()
            .map(|sym| {
                let obs = &self.spread_observation[*sym];
                [
                    (*sym).clone(),
                    fmt_opt(obs.fwd_min),
                    fmt_opt(obs.fwd_max),
                    fmt_opt(obs.fwd_threshold),
                    fmt_opt(obs.bwd_min),
                    fmt_opt(obs.bwd_max),
                    fmt_opt(obs.bwd_threshold),
                ]
            })
            .collect();
        for row in &rows {
            for (i, cell) in row.iter().enumerate() {
                if cell.len() > widths[i] {
                    widths[i] = cell.len();
                }
            }
        }

        let fmt_row = |cells: &[String; 7]| -> String {
            (0..7)
                .map(|i| format!("{:<width$}", cells[i], width = widths[i]))
                .collect::<Vec<_>>()
                .join("  ")
        };
        let header_cells: [String; 7] = std::array::from_fn(|i| headers[i].to_string());
        let header = fmt_row(&header_cells);
        let rule = "=".repeat(header.len());
        let mid = "-".repeat(header.len());

        log::info!("{source}: xarb spread observation (last 30s, MM thresholds)");
        log::info!("{rule}");
        log::info!("{header}");
        log::info!("{mid}");
        for row in &rows {
            log::info!("{}", fmt_row(row));
        }
        log::info!("{rule}");
    }

    fn build_xarb_open_blocker_reason(&mut self, symbol: &str, side: Side) -> Option<String> {
        let hedge_symbol = symbol;
        let open_venue = self.venues.0;
        let hedge_venue = self.venues.1;

        if self.enable_funding_open_filter {
            // 因子链 AND 串联:每个 enabled 因子都要拿到阈值 + 实时值 + 通过比较,否则 reject。
            let Some(open_filter_thresholds) = self.lookup_funding_open_thresholds(symbol) else {
                return Some("miss_funding_thresholds".to_string());
            };
            for entry in &self.funding_factor_chain {
                if !entry.enabled {
                    continue;
                }
                let Some(per_factor) = open_filter_thresholds.factor(&entry.factor) else {
                    return Some(format!("miss_{}_threshold", entry.factor));
                };
                let Some(value) = lookup_factor_realtime_value(
                    &entry.factor,
                    symbol,
                    hedge_symbol,
                    open_venue,
                    hedge_venue,
                ) else {
                    return Some(format!("miss_{}_value", entry.factor));
                };
                let threshold = select_factor_threshold(side, per_factor);
                let pass = match side {
                    Side::Buy => value > threshold,
                    Side::Sell => value < threshold,
                };
                if !pass {
                    return Some(format!("filter_{}", entry.factor));
                }
            }
        }

        let environment_signal =
            self.evaluate_environment_signal(symbol, hedge_symbol, hedge_venue, get_timestamp_us());
        if self.enable_environment_model && !environment_signal.allow_open {
            return Some("env_block".to_string());
        }

        let open_volatility_factor = match self.lookup_open_factor_value(symbol, open_venue) {
            lookup if lookup.ready == Some(true) => {
                let Some(value) = lookup.target_factor_value.filter(|v| v.is_finite()) else {
                    return Some("vol".to_string());
                };
                value
            }
            _ => return Some("vol".to_string()),
        };
        let _ = open_volatility_factor;

        let key = Self::build_threshold_key(symbol, hedge_symbol, open_venue, hedge_venue);
        if self.is_open_cooldown_hit(&key, side, get_timestamp_us()) {
            return Some("cooldown".to_string());
        }

        None
    }

    fn build_xarb_open_blocker_rows(&mut self) -> Vec<XarbOpenBlockerRow> {
        let symbol_list = super::symbol_list::SymbolList::instance();
        let mut symbols = symbol_list.get_online_symbols();
        symbols.sort();

        let mut rows = Vec::new();
        for symbol in symbols {
            let in_fwd = symbol_list.is_in_fwd_trade_list(&symbol);
            let in_bwd = symbol_list.is_in_bwd_trade_list(&symbol);

            let fwd_reason = if in_fwd {
                self.build_xarb_open_blocker_reason(&symbol, Side::Buy)
            } else {
                None
            };
            let bwd_reason = if in_bwd {
                self.build_xarb_open_blocker_reason(&symbol, Side::Sell)
            } else {
                None
            };

            let reason = match (fwd_reason, bwd_reason) {
                (None, None) => continue,
                (Some(reason), None) => format!("fwd:{reason}"),
                (None, Some(reason)) => format!("bwd:{reason}"),
                (Some(left), Some(right)) if left == right => left,
                (Some(left), Some(right)) => format!("fwd:{left}|bwd:{right}"),
            };

            rows.push(XarbOpenBlockerRow {
                symbol,
                vol_tr: extract_threshold_from_reason(&reason),
                reason,
            });
        }
        rows
    }

    fn log_xarb_open_blocker_table(&mut self, source: &str) {
        let rows = self.build_xarb_open_blocker_rows();
        if rows.is_empty() {
            return;
        }

        let symbol_width = rows
            .iter()
            .map(|row| row.symbol.len())
            .max()
            .unwrap_or(6)
            .max("symbol".len());
        let reason_width = rows
            .iter()
            .map(|row| row.reason.len())
            .max()
            .unwrap_or(6)
            .max("reason".len());
        let vol_tr_width = rows
            .iter()
            .map(|row| format_opt_opt_f64(row.vol_tr).len())
            .max()
            .unwrap_or(6)
            .max("vol_tr".len());

        let fmt_row = |symbol: &str, vol_tr: &str, reason: &str| -> String {
            format!(
                "{:<symbol_width$}  {:<vol_tr_width$}  {:<reason_width$}",
                symbol,
                vol_tr,
                reason,
                symbol_width = symbol_width,
                vol_tr_width = vol_tr_width,
                reason_width = reason_width
            )
        };

        let header = fmt_row("symbol", "vol_tr", "reason");
        let rule = "=".repeat(header.len());
        let mid = "-".repeat(header.len());

        log::info!("{source}: xarb open blocker table (last 30s)");
        log::info!("{rule}");
        log::info!("{header}");
        log::info!("{mid}");
        for row in rows {
            log::info!(
                "{}",
                fmt_row(&row.symbol, &format_opt_opt_f64(row.vol_tr), &row.reason)
            );
        }
        log::info!("{rule}");
    }

    pub fn record_environment_intercept_summary(
        &mut self,
        forward_open: bool,
        backward_open: bool,
        cooldown_hit: bool,
        result: &EnvironmentSignalResult,
    ) {
        let reason = match (forward_open, backward_open, cooldown_hit, result.allow_open) {
            (_, _, true, _) => "cooldown".to_string(),
            (_, _, _, false) => "env_block".to_string(),
            (true, false, false, true) => "forward_open".to_string(),
            (false, true, false, true) => "backward_open".to_string(),
            (true, true, false, true) => "both_open".to_string(),
            _ => "neutral".to_string(),
        };
        self.record_intercept_summary(reason);
    }

    pub fn evaluate_open_gate(
        &mut self,
        open_symbol_key: &str,
        hedge_symbol_key: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        side: Side,
        now: i64,
        forward_open: bool,
        backward_open: bool,
    ) -> Option<ArbOpenGatePassed> {
        // 仅供 logging:保留按 venue 类型推断的单一因子值快照(spread_fr 或 premium_rate)。
        // 真正的 funding filter 检查走下面的因子链 AND 串联。
        let open_filter_lookup = lookup_realtime_open_filter_value(
            open_symbol_key,
            hedge_symbol_key,
            open_venue,
            hedge_venue,
        );
        let open_filter_value = open_filter_lookup.map(|(value, _)| value);

        if self.enable_funding_open_filter {
            let Some(open_filter_thresholds) = self.lookup_funding_open_thresholds(open_symbol_key)
            else {
                self.record_intercept_summary("miss_funding_thresholds");
                return None;
            };
            for entry in &self.funding_factor_chain {
                if !entry.enabled {
                    continue;
                }
                let Some(per_factor) = open_filter_thresholds.factor(&entry.factor) else {
                    self.record_intercept_summary(format!("miss_{}_threshold", entry.factor));
                    return None;
                };
                let Some(value) = lookup_factor_realtime_value(
                    &entry.factor,
                    open_symbol_key,
                    hedge_symbol_key,
                    open_venue,
                    hedge_venue,
                ) else {
                    self.record_intercept_summary(format!("miss_{}_value", entry.factor));
                    return None;
                };
                let threshold = select_factor_threshold(side, per_factor);
                let pass = match side {
                    Side::Buy => value > threshold,
                    Side::Sell => value < threshold,
                };
                if !pass {
                    self.record_intercept_summary(format!("filter_{}", entry.factor));
                    return None;
                }
            }
        }

        let return_lookup = self.lookup_return_model_score_lookup(hedge_symbol, hedge_venue);
        let return_qtl = return_lookup
            .as_ref()
            .and_then(|lookup| lookup.score_quantile)
            .filter(|v| v.is_finite());
        let return_threshold = None;

        let environment_signal =
            self.evaluate_environment_signal(open_symbol_key, hedge_symbol, hedge_venue, now);
        if self.enable_environment_model && !environment_signal.allow_open {
            self.record_environment_intercept_summary(
                forward_open,
                backward_open,
                false,
                &environment_signal,
            );
            return None;
        }
        let environment_score = environment_signal
            .score
            .unwrap_or(environment_signal.class_label as f64);
        let vol_lookup = self.lookup_open_factor_value(open_symbol_key, open_venue);
        let open_volatility_factor = match vol_lookup.ready {
            Some(true) => {
                let Some(value) = vol_lookup.target_factor_value.filter(|v| v.is_finite()) else {
                    self.record_intercept_summary("vol");
                    return None;
                };
                value
            }
            _ => {
                self.record_intercept_summary("vol");
                return None;
            }
        };

        // Inline vol gate（与 mm_decision/open.rs 同语义）：threshold 来源是 thread_local
        // store 里同 symbol、同 percentile 的 rolling sample；warming up 时直接拦截，
        // 阈值就绪后只放行 vol <= threshold 的开仓。
        if self.enable_volatility_limit {
            let snapshot = snapshot_inline_volatility(
                &vol_lookup.symbol_key,
                open_volatility_factor,
                self.open_volatility_limit,
            );
            let Some(threshold) = snapshot.threshold else {
                log::debug!(
                    "ArbDecision: open intercept volatility_warming_up symbol={} samples={} min_samples={} percentile={:.2}",
                    open_symbol_key,
                    snapshot.sample_count,
                    INLINE_VOLATILITY_MIN_SAMPLES,
                    snapshot.percentile
                );
                self.record_intercept_summary("vol_warming_up");
                return None;
            };
            if open_volatility_factor > threshold {
                self.record_intercept_summary("vol_limited");
                return None;
            }
        }

        Some(ArbOpenGatePassed {
            return_qtl,
            return_threshold,
            open_filter_value,
            environment_score,
            environment_threshold: environment_signal.threshold,
            open_volatility_factor,
        })
    }
}

impl ArbDecision {
    pub(crate) fn try_update_model_output_services(services: Vec<String>) -> bool {
        try_update_arb_model_output_services(services)
    }

    pub fn init_mode(mode: ArbMode) -> Result<()> {
        ARB_MODE.with(|cell| {
            if let Some(existing) = cell.get().copied() {
                if existing != mode {
                    bail!(
                        "ArbMode already initialized to {:?}, cannot change to {:?}",
                        existing,
                        mode
                    );
                }
                return Ok(());
            }
            cell.set(mode)
                .map_err(|_| anyhow::anyhow!("Failed to initialize ArbMode"))?;
            Ok(())
        })
    }

    pub fn mode() -> Option<ArbMode> {
        ARB_MODE.with(|cell| cell.get().copied())
    }

    pub fn signal_cooldown_us() -> Option<i64> {
        Self::with_state_mut(|arb| arb.signal_cooldown_us)
    }

    pub(crate) fn with_state_mut<F, R>(f: F) -> Option<R>
    where
        F: FnOnce(&mut ArbDecisionState) -> R,
    {
        ARB_DECISION.with(|cell| {
            let decision_ref = cell.get()?;
            Some(f(&mut decision_ref.borrow_mut()))
        })
    }

    fn backend_for_mode(mode: ArbMode) -> ArbBackend {
        match mode {
            ArbMode::FundingArb => ArbBackend::Funding,
            ArbMode::IntraArb | ArbMode::CrossArb => ArbBackend::Spread,
        }
    }

    pub async fn init_singleton(
        mode: ArbMode,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        exchange: Option<Exchange>,
    ) -> Result<()> {
        Self::init_mode(mode)?;
        if arb_hedge_force_taker() {
            super::spread_factor::SpreadFactor::instance().set_mode(super::common::FactorMode::MT);
        }
        ARB_DECISION.with(|cell| {
            if cell.get().is_none() {
                let _ = cell.set(RefCell::new(ArbDecisionState::new(
                    mode,
                    (open_venue, hedge_venue),
                )));
            }
        });
        match Self::backend_for_mode(mode) {
            ArbBackend::Funding => {
                let exchange = exchange
                    .context("FundingArb mode requires exchange for arb shell initialization")?;
                FundingArbShell::init_singleton(exchange).await
            }
            ArbBackend::Spread => SpreadArbShell::init_singleton(open_venue, hedge_venue).await,
        }
    }

    pub fn trigger_decision(
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) {
        let Some(mode) = Self::mode() else {
            return;
        };
        match Self::backend_for_mode(mode) {
            ArbBackend::Funding => {
                if !RateFetcher::is_initial_ready(hedge_venue) {
                    // Funding mode still tolerates degraded readiness; decision_router keeps logging.
                }
                with_thread_local_shell_mut(
                    &FUNDING_ARB_SHELL,
                    FUNDING_ARB_SHELL_NAME,
                    |decision| {
                        let _ = drive_funding_decision(
                            decision,
                            open_symbol,
                            hedge_symbol,
                            open_venue,
                            hedge_venue,
                        );
                    },
                );
            }
            ArbBackend::Spread => {
                with_thread_local_shell_mut(&SPREAD_ARB_SHELL, SPREAD_ARB_SHELL_NAME, |decision| {
                    let _ = drive_spread_arb_decision(
                        decision,
                        open_symbol,
                        hedge_symbol,
                        open_venue,
                        hedge_venue,
                    );
                });
            }
        }
    }

    pub fn process_cancel_trigger_interval() {
        let Some(mode) = Self::mode() else {
            return;
        };
        let _ = Self::with_state_mut(|arb| arb.poll_factor_value_updates());
        match Self::backend_for_mode(mode) {
            ArbBackend::Funding => {
                with_thread_local_shell_mut(
                    &FUNDING_ARB_SHELL,
                    FUNDING_ARB_SHELL_NAME,
                    |decision| {
                        drive_funding_cancel_trigger_interval(decision);
                    },
                );
            }
            ArbBackend::Spread => {
                with_thread_local_shell_mut(&SPREAD_ARB_SHELL, SPREAD_ARB_SHELL_NAME, |decision| {
                    drive_spread_arb_cancel_trigger_interval(decision);
                });
            }
        }
    }

    pub fn evaluate_funding_rate_signal(
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
    ) -> Result<Option<ArbSignalKind>> {
        let flags = evaluate_funding_signal_flags(hedge_symbol, hedge_venue);
        if log::log_enabled!(log::Level::Debug) {
            log::debug!(
                "ArbDecision funding-rate flags hedge={} venue={:?} period={:?} fwd_open={} fwd_close={} bwd_open={} bwd_close={}",
                hedge_symbol,
                hedge_venue,
                flags.period,
                flags.forward_open,
                flags.forward_close,
                flags.backward_open,
                flags.backward_close
            );
        }

        let signal = resolve_funding_signal_from_flags(
            flags.forward_open,
            flags.forward_close,
            flags.backward_open,
            flags.backward_close,
        );
        if let Some(signal) = signal {
            log::debug!(
                "ArbDecision funding-rate signal={} hedge={} venue={:?}",
                signal.as_str(),
                hedge_symbol,
                hedge_venue
            );
        } else {
            log::debug!(
                "ArbDecision no funding-rate condition met hedge={} venue={:?}",
                hedge_symbol,
                hedge_venue
            );
        }
        Ok(signal)
    }

    pub fn print_signal_table(symbols: &[String]) {
        use super::common::{ArbDirection, OperationType};
        use super::funding_rate_factor::FundingRateFactor;

        let fr_factor = FundingRateFactor::instance();
        let default_period = FundingRatePeriod::Hours8;
        let fwd_open_config = fr_factor.get_threshold_config(
            default_period,
            ArbDirection::Forward,
            OperationType::Open,
        );
        let fwd_close_config = fr_factor.get_threshold_config(
            default_period,
            ArbDirection::Forward,
            OperationType::Close,
        );
        let bwd_open_config = fr_factor.get_threshold_config(
            default_period,
            ArbDirection::Backward,
            OperationType::Open,
        );
        let bwd_close_config = fr_factor.get_threshold_config(
            default_period,
            ArbDirection::Backward,
            OperationType::Close,
        );

        log::info!("FR 阈值配置:");
        if let Some(cfg) = &fwd_open_config {
            log::info!(
                "  ForwardOpen:   预测FR {:?} {:.4}%",
                cfg.compare_op,
                cfg.threshold * 100.0
            );
        }
        if let Some(cfg) = &fwd_close_config {
            log::info!(
                "  ForwardClose:  当前FR_MA {:?} {:.4}%",
                cfg.compare_op,
                cfg.threshold * 100.0
            );
        }
        if let Some(cfg) = &bwd_open_config {
            log::info!(
                "  BackwardOpen:  预测FR+Loan {:?} {:.4}%",
                cfg.compare_op,
                cfg.threshold * 100.0
            );
        }
        if let Some(cfg) = &bwd_close_config {
            log::info!(
                "  BackwardClose: 当前FR_MA+CurLoan {:?} {:.4}%",
                cfg.compare_op,
                cfg.threshold * 100.0
            );
        }
        log::info!("");

        let mut entries = Self::collect_funding_signal_table_entries(symbols);
        entries.sort_unstable_by(|a, b| a.symbol.cmp(&b.symbol));

        log::info!("┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐");
        log::info!("│ Symbol         │ 预测FR% │ FR_MA% │ PredLoan% │ CurLoan% │ FR+PLoan% │ MA+CLoan% │ FR Sig     │ Spread Sig │ Final Sig  │");
        log::info!("├───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤");
        for e in entries {
            log::info!(
                "│ {:<14} │ {:>7.3} │ {:>6.3} │ {:>9.3} │ {:>8.3} │ {:>9.3} │ {:>9.3} │ {:<10} │ {:<10} │ {:<10} │",
                e.symbol,
                e.pred_fr_pct,
                e.fr_ma_pct,
                e.pred_loan_pct,
                e.cur_loan_pct,
                e.fr_plus_pred_loan_pct,
                e.ma_plus_cur_loan_pct,
                e.fr_sig,
                e.spread_sig,
                e.final_sig
            );
        }
        log::info!("└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘");
    }

    pub(crate) fn load_valid_quotes(
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Option<(Quote, Quote)> {
        let mkt_channel = MktChannel::instance();
        let open_quote = mkt_channel.get_quote(open_symbol, open_venue);
        let hedge_quote = mkt_channel.get_quote(hedge_symbol, hedge_venue);
        if open_quote.is_none() || hedge_quote.is_none() {
            return None;
        }
        let open_quote = open_quote.unwrap();
        let hedge_quote = hedge_quote.unwrap();
        if open_quote.bid >= open_quote.ask || hedge_quote.bid >= hedge_quote.ask {
            return None;
        }
        Some((open_quote, hedge_quote))
    }

    fn collect_funding_signal_table_entries(symbols: &[String]) -> Vec<ArbSignalTableEntry> {
        let spread_factor = super::spread_factor::SpreadFactor::instance();
        let rate_fetcher = RateFetcher::instance();
        let mkt_channel = MktChannel::instance();
        let symbol_list = super::symbol_list::SymbolList::instance();
        let (open_venue, hedge_venue) = ArbDecision::with_state_mut(|arb| arb.venues)
            .unwrap_or((TradingVenue::OkexMargin, TradingVenue::OkexFutures));

        symbols
            .iter()
            .map(|symbol| {
                let fr = rate_fetcher
                    .get_predicted_funding_rate(symbol, hedge_venue)
                    .map(|(_, v)| v)
                    .unwrap_or(0.0);
                let predict_loan = rate_fetcher
                    .get_predict_loan_rate(symbol, hedge_venue)
                    .map(|(_, v)| v)
                    .unwrap_or(0.0);
                let current_loan = rate_fetcher
                    .get_current_loan_rate(symbol, hedge_venue)
                    .map(|(_, v)| v)
                    .unwrap_or(0.0);
                let fr_ma = mkt_channel
                    .get_funding_rate_mean(symbol, hedge_venue)
                    .unwrap_or(0.0);

                let fr_predict_loan = fr + predict_loan * 1.2;
                let fr_ma_cur_loan = fr_ma + current_loan;
                let flags = evaluate_funding_signal_flags(symbol, hedge_venue);
                let fr_signal = resolve_funding_signal_from_flags(
                    flags.forward_open,
                    flags.forward_close,
                    flags.backward_open,
                    flags.backward_close,
                );
                let fr_signal_label = fr_signal.map(|signal| signal.as_str()).unwrap_or("-");

                let spread_fwd_open =
                    spread_factor.satisfy_forward_open(open_venue, symbol, hedge_venue, symbol);
                let spread_bwd_open =
                    spread_factor.satisfy_backward_open(open_venue, symbol, hedge_venue, symbol);
                let spread_fwd_cancel =
                    spread_factor.satisfy_forward_cancel(open_venue, symbol, hedge_venue, symbol);
                let spread_bwd_cancel =
                    spread_factor.satisfy_backward_cancel(open_venue, symbol, hedge_venue, symbol);

                let spread_signal_label = if spread_fwd_cancel {
                    "FwdCancel"
                } else if spread_bwd_cancel {
                    "BwdCancel"
                } else if spread_fwd_open {
                    "FwdOpen"
                } else if spread_bwd_open {
                    "BwdOpen"
                } else {
                    "-"
                };

                let final_signal_label = if spread_fwd_cancel {
                    "FwdCancel"
                } else if spread_bwd_cancel {
                    "BwdCancel"
                } else {
                    fr_signal
                        .and_then(|signal| {
                            ArbDecisionState::evaluate_funding_final_signal(
                                signal,
                                spread_factor,
                                &symbol_list,
                                symbol,
                                symbol,
                                open_venue,
                                hedge_venue,
                                true,
                                false,
                            )
                            .map(|_| signal.as_str())
                        })
                        .unwrap_or("-")
                };

                ArbSignalTableEntry {
                    symbol: symbol.clone(),
                    pred_fr_pct: fr * 100.0,
                    fr_ma_pct: fr_ma * 100.0,
                    pred_loan_pct: predict_loan * 100.0,
                    cur_loan_pct: current_loan * 100.0,
                    fr_plus_pred_loan_pct: fr_predict_loan * 100.0,
                    ma_plus_cur_loan_pct: fr_ma_cur_loan * 100.0,
                    fr_sig: fr_signal_label,
                    spread_sig: spread_signal_label,
                    final_sig: final_signal_label,
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod funding_mode_signal_tests {
    use super::*;
    use crate::funding_rate::common::FactorMode;
    use crate::funding_rate::spread_factor::SpreadFactor;

    fn setup_spread_factor_with_forward_close(symbol: &str) -> &'static SpreadFactor {
        let spread_factor = SpreadFactor::instance();
        spread_factor.clear_thresholds();
        spread_factor.set_mode(FactorMode::MM);
        spread_factor.set_backward_open_threshold(
            TradingVenue::BitgetMargin,
            symbol,
            TradingVenue::BitgetFutures,
            symbol,
            0.0001,
            0.0001,
        );
        let _ = spread_factor.update(
            TradingVenue::BitgetMargin,
            symbol,
            TradingVenue::BitgetFutures,
            symbol,
            100.0,
            100.0,
            99.95,
            99.95,
        );
        assert!(ArbDecisionState::evaluate_close_side(
            &spread_factor,
            symbol,
            symbol,
            TradingVenue::BitgetMargin,
            TradingVenue::BitgetFutures,
        )
        .is_some());
        spread_factor
    }

    #[test]
    fn dump_symbol_allows_spread_close_without_rate_inputs() {
        let symbol = "UNITDUMPUSDT";
        let spread_factor = setup_spread_factor_with_forward_close(symbol);

        let signal = evaluate_funding_mode_signal(
            &spread_factor,
            symbol,
            symbol,
            symbol,
            TradingVenue::BitgetMargin,
            TradingVenue::BitgetFutures,
            true,
            false,
        )
        .expect("mode signal");

        assert_eq!(signal, Some(ArbSignalKind::ForwardClose));
    }

    #[test]
    fn non_dump_symbol_suppresses_spread_close_until_rate_inputs_ready() {
        let symbol = "UNITNONDUMPUSDT";
        let spread_factor = setup_spread_factor_with_forward_close(symbol);

        let signal = evaluate_funding_mode_signal(
            &spread_factor,
            symbol,
            symbol,
            symbol,
            TradingVenue::BitgetMargin,
            TradingVenue::BitgetFutures,
            false,
            false,
        )
        .expect("mode signal");

        assert_eq!(signal, None);
    }
}

#[cfg(test)]
mod hedge_offset_overrides_tests {
    use super::*;
    use crate::signal::common::TradingVenue;

    fn fresh_state() -> ArbDecisionState {
        ArbDecisionState::new(
            ArbMode::IntraArb,
            (TradingVenue::OkexMargin, TradingVenue::OkexFutures),
        )
    }

    #[test]
    fn resolve_falls_back_to_global_when_no_overrides() {
        let mut state = fresh_state();
        state.hedge_price_offset_limit_lower = 0.0003;
        state.hedge_price_offset_limit_upper = 0.005;
        let (lower, upper) = state.resolve_hedge_price_offset_limits("BTCUSDT");
        assert!((lower - 0.0003).abs() < 1e-12);
        assert!((upper - 0.005).abs() < 1e-12);
    }

    #[test]
    fn resolve_uses_per_symbol_override_when_present() {
        let mut state = fresh_state();
        state.hedge_price_offset_limit_lower = 0.0003;
        state.hedge_price_offset_limit_upper = 0.005;
        // 直接调用 update_* 走规范化校验路径
        let mut lower = HashMap::new();
        let mut upper = HashMap::new();
        // 注意：normalize_symbol_for_venue(open_venue=OkexMargin) 会把 "BTCUSDT" 映射为
        // "BTC-USDT"（margin 形态），所以这里直接用规范化后的 key 作为 override key。
        let symbol_key = normalize_symbol_for_venue("BTCUSDT", state.venues.0);
        lower.insert(symbol_key.clone(), 0.0010);
        upper.insert(symbol_key.clone(), 0.008);
        state.update_hedge_price_offset_limit_overrides(lower, upper);
        let (l, u) = state.resolve_hedge_price_offset_limits("BTCUSDT");
        assert!((l - 0.0010).abs() < 1e-12);
        assert!((u - 0.008).abs() < 1e-12);
    }

    #[test]
    fn defaults_match_mm_volatility_gate_settings() {
        let state = fresh_state();
        assert!(state.enable_volatility_limit);
        assert!((state.open_volatility_limit - 70.0).abs() < 1e-12);
    }

    #[test]
    fn update_open_volatility_limit_rejects_invalid_and_keeps_previous() {
        let mut state = fresh_state();
        state.update_open_volatility_limit(60.0);
        assert!((state.open_volatility_limit - 60.0).abs() < 1e-12);
        // NaN / 越界 / 负值 全被拒绝，保留原值
        state.update_open_volatility_limit(f64::NAN);
        assert!((state.open_volatility_limit - 60.0).abs() < 1e-12);
        state.update_open_volatility_limit(150.0);
        assert!((state.open_volatility_limit - 60.0).abs() < 1e-12);
        state.update_open_volatility_limit(-1.0);
        assert!((state.open_volatility_limit - 60.0).abs() < 1e-12);
    }

    #[test]
    fn update_enable_volatility_limit_toggles() {
        let mut state = fresh_state();
        state.update_enable_volatility_limit(false);
        assert!(!state.enable_volatility_limit);
        state.update_enable_volatility_limit(true);
        assert!(state.enable_volatility_limit);
    }

    #[test]
    fn dual_venue_volatility_readiness_requires_both_sides() {
        let readiness = DualVenueVolatilityReadiness {
            open: VenueVolatilityReadiness {
                venue: TradingVenue::BitgetMargin,
                symbol_key: "BTCUSDT".to_string(),
                key: "rl_return_volatility_bitget-margin_BTCUSDT".to_string(),
                value: Some(0.001),
                ready: Some(true),
                note: "ok".to_string(),
            },
            hedge: VenueVolatilityReadiness {
                venue: TradingVenue::BitgetFutures,
                symbol_key: "BTCUSDT".to_string(),
                key: "rl_return_volatility_bitget-futures_BTCUSDT".to_string(),
                value: None,
                ready: None,
                note: "missing_ipc_snapshot".to_string(),
            },
        };

        assert!(!readiness.is_ready());
        let reason = readiness.missing_reason();
        assert!(reason.contains("hedge("));
        assert!(reason.contains("missing_ipc_snapshot"));
        assert!(!reason.contains("open("));
    }

    #[test]
    fn dual_venue_volatility_readiness_passes_with_finite_non_negative_values() {
        let readiness = DualVenueVolatilityReadiness {
            open: VenueVolatilityReadiness {
                venue: TradingVenue::BitgetMargin,
                symbol_key: "BTCUSDT".to_string(),
                key: "rl_return_volatility_bitget-margin_BTCUSDT".to_string(),
                value: Some(0.0),
                ready: Some(true),
                note: "ok".to_string(),
            },
            hedge: VenueVolatilityReadiness {
                venue: TradingVenue::BitgetFutures,
                symbol_key: "BTCUSDT".to_string(),
                key: "rl_return_volatility_bitget-futures_BTCUSDT".to_string(),
                value: Some(0.002),
                ready: Some(true),
                note: "fallback_last_valid(factor_ipc_timeout(age_ms=31000 max_age_ms=30000))"
                    .to_string(),
            },
        };

        assert!(readiness.is_ready());
        assert!(readiness.missing_reason().is_empty());
    }

    #[test]
    fn update_drops_invalid_overrides_keeping_state_consistent() {
        let mut state = fresh_state();
        state.hedge_price_offset_limit_lower = 0.0003;
        state.hedge_price_offset_limit_upper = 0.005;
        let mut lower = HashMap::new();
        let mut upper = HashMap::new();
        // 一个有效，一个非法（lower > upper）
        let ok_key = normalize_symbol_for_venue("BTCUSDT", state.venues.0);
        let bad_key = normalize_symbol_for_venue("ETHUSDT", state.venues.0);
        lower.insert(ok_key.clone(), 0.001);
        upper.insert(ok_key.clone(), 0.008);
        lower.insert(bad_key.clone(), 0.01);
        upper.insert(bad_key.clone(), 0.005);
        state.update_hedge_price_offset_limit_overrides(lower, upper);
        // ok_key 保留
        assert_eq!(
            state.hedge_price_offset_limit_lower_overrides.get(&ok_key),
            Some(&0.001)
        );
        assert_eq!(
            state.hedge_price_offset_limit_upper_overrides.get(&ok_key),
            Some(&0.008)
        );
        // bad_key 全部丢弃
        assert!(!state
            .hedge_price_offset_limit_lower_overrides
            .contains_key(&bad_key));
        assert!(!state
            .hedge_price_offset_limit_upper_overrides
            .contains_key(&bad_key));
    }

    #[test]
    fn resolve_open_offset_lower_returns_zero_when_missing() {
        let mut state = fresh_state();
        state.open_offset_lower_overrides.clear();
        assert_eq!(state.resolve_open_offset_lower("BTCUSDT"), 0.0);
        assert_eq!(state.resolve_open_offset_lower("anything"), 0.0);
    }

    #[test]
    fn resolve_open_offset_lower_normalizes_symbol() {
        let mut state = fresh_state();
        // strategy_loader stores the normalized symbol key. We mimic that here.
        let key = normalize_symbol_for_venue("BTCUSDT", state.venues.0);
        state.open_offset_lower_overrides.insert(key, 0.001);
        assert!((state.resolve_open_offset_lower("BTCUSDT") - 0.001).abs() < 1e-9);
        assert!((state.resolve_open_offset_lower("btc-usdt") - 0.001).abs() < 1e-9);
        assert!((state.resolve_open_offset_lower("btc_usdt") - 0.001).abs() < 1e-9);
    }

    #[test]
    fn arb_hedge_due_qty_cap_limits_positive_qty_by_amount_u() {
        let capped = cap_arb_hedge_due_qty_by_amount_u(5.0, 100.0, 250.0);
        assert!((capped - 2.5).abs() < 1e-12);
    }

    #[test]
    fn arb_hedge_due_qty_cap_preserves_negative_sign() {
        let capped = cap_arb_hedge_due_qty_by_amount_u(-5.0, 100.0, 250.0);
        assert!((capped + 2.5).abs() < 1e-12);
    }

    #[test]
    fn arb_hedge_due_qty_cap_keeps_qty_below_amount_u() {
        let capped = cap_arb_hedge_due_qty_by_amount_u(2.0, 100.0, 250.0);
        assert!((capped - 2.0).abs() < 1e-12);
    }

    #[test]
    fn arb_hedge_due_qty_cap_ignores_invalid_inputs() {
        assert_eq!(cap_arb_hedge_due_qty_by_amount_u(5.0, 0.0, 250.0), 5.0);
        assert_eq!(cap_arb_hedge_due_qty_by_amount_u(5.0, 100.0, 0.0), 5.0);
        assert_eq!(cap_arb_hedge_due_qty_by_amount_u(5.0, 100.0, f64::NAN), 5.0);
    }
}
