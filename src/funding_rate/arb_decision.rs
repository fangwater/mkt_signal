use anyhow::{bail, Context, Result};
use bytes::Bytes;
use iceoryx2::prelude::{Node, NodeBuilder, NodeName, ServiceName};
use iceoryx2::service::ipc;
use std::cell::{OnceCell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::thread::LocalKey;

use crate::common::bbo::Bbo;
use crate::common::exchange::Exchange;
use crate::common::iceoryx_publisher::SignalPublisher;
use crate::common::iceoryx_subscriber::GenericSignalSubscriber;
use crate::common::ipc_service_name::build_service_name;
use crate::common::redis_client::RedisSettings;
use crate::common::time_util::get_timestamp_us;
use crate::funding_rate::FundingRatePeriod;
use crate::pre_trade::order_manager::Side;
use crate::signal::arb_signal::{ArbBackwardQueryMsg, ArbCancelCandidateQueryMsg};
use crate::signal::common::{SignalBytes, TradingVenue};
use crate::signal::hedge_signal::ArbHedgeSignalQueryMsg;
use crate::signal::trade_signal::SignalType;
use crate::signal::venue_min_qty_table::VenueMinQtyTable;
use crate::symbol_match::normalize_symbol_for_whitelist;

use super::arb_cooldown::is_cooldown_hit;
use super::arb_cooldown::threshold_key;
use super::arb_cooldown::update_last_ts;
use super::arb_mode::ArbMode;
use super::arb_open_filter::{
    lookup_realtime_open_filter_value, select_open_filter_threshold, select_open_return_threshold,
    select_open_return_threshold_by_hedge_side,
};
use super::common::Quote;
use super::common::{ReturnScoreThresholdsResolved, ThresholdKey, VenuePair};
use super::factor_value_hub::{
    EnvironmentSignalResult, FactorValueHub, FactorValueLookupResult, ModelOutputScoreLookupResult,
};
use super::funding_rate_factor::FundingRateFactor;
use super::mkt_channel::MktChannel;
use super::rate_fetcher::RateFetcher;
use super::xarb_funding_threshold_loader::XarbFundingThresholdsResolved;

pub const DEFAULT_ARBITRAGE_SIGNAL_CHANNEL: &str = "trade_signal";
pub const DEFAULT_ARBITRAGE_BACKWARD_CHANNEL: &str = "trade_query";
pub const DEFAULT_PNLU_KEY_SUFFIX: &str = "_pnlu_factor_thresholds";
pub const DEFAULT_ENV_MODEL_TRUE_THRESHOLD: f64 = 0.0;
const FUNDING_ARB_SHELL_NAME: &str = "ArbDecision(FundingArb)";
const SPREAD_ARB_SHELL_NAME: &str = "ArbDecision(SpreadArb)";

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
    let service = node
        .service_builder(&ServiceName::new(&service_name)?)
        .publish_subscribe::<[u8; crate::common::iceoryx_publisher::SIGNAL_PAYLOAD]>()
        .max_publishers(1)
        .max_subscribers(32)
        .history_size(128)
        .subscriber_max_buffer_size(256)
        .open_or_create()
        .with_context(|| {
            format!("{source}: failed to open/create backward signal service={service_name}")
        })?;

    let subscriber = service.subscriber_builder().create()?;
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
        pnlu_settings.clone(),
        pnlu_key_suffix.clone(),
        30 * 60,
    )?;
    let hedge_factor_value_hub = FactorValueHub::new(
        &node,
        venues.0,
        venues.1,
        "rl_return_volatility",
        "rl_return_volatility",
        pnlu_settings,
        pnlu_key_suffix,
        30 * 60,
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
        arb.apply_shared_bootstrap(ArbDecisionState::default_shared_bootstrap(5));
        arb.enable_environment_model = true;
        arb.enable_volatility_limit = true;
        arb.open_volatility_limit = 70.0;
        arb.return_model_service = None;
        arb.environment_model_service = None;
        arb.environment_model_true_threshold = DEFAULT_ENV_MODEL_TRUE_THRESHOLD;
        arb.return_score_thresholds = HashMap::new();
        arb.funding_open_thresholds = HashMap::new();
        arb.open_volatility_thresholds = HashMap::new();
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
    static FUNDING_ARB_SHELL: OnceCell<RefCell<FundingArbShell>> = OnceCell::new();
    static SPREAD_ARB_SHELL: OnceCell<RefCell<SpreadArbShell>> = OnceCell::new();
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
                    match decision.runtime.backward_sub.receive_msg() {
                        Ok(Some(data)) => {
                            if let Some(query) =
                                dispatch_arb_backward_query(FUNDING_ARB_SHELL_NAME, data)
                            {
                                match query {
                                    ArbBackwardQueryMsg::Hedge(query) => {
                                        drive_funding_hedge_query(&mut decision, query)
                                    }
                                    ArbBackwardQueryMsg::CancelCandidates(query) => {
                                        drive_funding_cancel_candidate_query(&mut decision, query)
                                    }
                                }
                            }
                            true
                        }
                        Ok(None) => false,
                        Err(err) => {
                            log::warn!("{FUNDING_ARB_SHELL_NAME}: backward_sub 接收错误: {}", err);
                            false
                        }
                    }
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
                    let _ = ArbDecision::with_state_mut(|arb| arb.poll_model_output_updates());
                    match decision.runtime.backward_sub.receive_msg() {
                        Ok(Some(data)) => {
                            if let Some(query) =
                                dispatch_arb_backward_query(SPREAD_ARB_SHELL_NAME, data)
                            {
                                match query {
                                    ArbBackwardQueryMsg::Hedge(query) => {
                                        drive_spread_arb_hedge_query(&mut decision, query)
                                    }
                                    ArbBackwardQueryMsg::CancelCandidates(query) => {
                                        drive_spread_arb_cancel_candidate_query(
                                            &mut decision,
                                            query,
                                        )
                                    }
                                }
                            }
                            true
                        }
                        Ok(None) => false,
                        Err(err) => {
                            log::warn!("{SPREAD_ARB_SHELL_NAME}: backward_sub 接收错误: {}", err);
                            false
                        }
                    }
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

pub fn spawn_backward_listener_loop<F>(source: &'static str, mut receive_one: F)
where
    F: FnMut() -> bool + 'static,
{
    tokio::task::spawn_local(async move {
        log::info!("{source} backward listener started");
        loop {
            let has_message = receive_one();
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
    receive_one: FReceive,
    reload_state: FReloadGet,
    reload_set_local: FReloadSet,
) where
    FApply: FnMut(Option<VenueMinQtyTable>, Option<VenueMinQtyTable>),
    FReceive: FnMut() -> bool + 'static,
    FReloadGet: FnMut(i64) -> Option<(bool, TradingVenue)> + 'static,
    FReloadSet: FnMut(HashMap<String, f64>, i64) + 'static,
{
    refresh_min_qty_tables(source, venues, apply_tables).await;
    spawn_backward_listener_loop(source, receive_one);
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
    receive_one: FReceive,
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
        receive_one,
        reload_state,
        reload_set_local,
    )
    .await;
    Ok(())
}

pub fn update_model_output_services_for_arb(node: &Node<ipc::Service>, services: Vec<String>) {
    let _ = ArbDecision::with_state_mut(|arb| {
        if let Some(hub) = arb.hedge_factor_value_hub.as_mut() {
            hub.update_model_output_services(node, services);
        }
    });
}

pub fn try_update_spread_arb_model_output_services(services: Vec<String>) -> bool {
    try_with_thread_local_shell_mut(&SPREAD_ARB_SHELL, |decision| {
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
    let loan_required = matches!(
        hedge_venue,
        TradingVenue::BinanceMargin
            | TradingVenue::BinanceFutures
            | TradingVenue::OkexMargin
            | TradingVenue::OkexFutures
            | TradingVenue::GateMargin
            | TradingVenue::GateFutures
    );
    let has_predict_fr = rate_fetcher
        .get_predicted_funding_rate(hedge_symbol, hedge_venue)
        .is_some();
    let has_predict_loan = if loan_required {
        rate_fetcher
            .get_predict_loan_rate(hedge_symbol, hedge_venue)
            .is_some()
    } else {
        true
    };
    has_predict_fr && has_predict_loan
}

fn prepare_arb_hedge_query(
    source: &str,
    query: &ArbHedgeSignalQueryMsg,
) -> Option<ArbHedgePreparedQuery> {
    let Some(side) = query.get_side() else {
        log::warn!("{source}: hedge query side invalid: {}", query.hedge_side);
        return None;
    };

    let Some(hedge_venue) = TradingVenue::from_u8(query.hedging_venue) else {
        log::warn!(
            "{source}: hedge query hedge venue invalid: {}",
            query.hedging_venue
        );
        return None;
    };

    let hedge_symbol = query.get_hedging_symbol();
    if hedge_symbol.is_empty() {
        log::warn!("{source}: hedge query missing hedge symbol");
        return None;
    }

    let open_symbol = query.get_opening_symbol();
    if open_symbol.is_empty() {
        log::warn!("{source}: hedge query missing open symbol");
        return None;
    }

    let Some(open_venue) = TradingVenue::from_u8(query.opening_venue) else {
        log::warn!(
            "{source}: hedge query opening venue invalid: {}",
            query.opening_venue
        );
        return None;
    };

    let hedge_base_qty = query.hedge_base_qty;
    if hedge_base_qty <= 0.0 {
        log::warn!(
            "{source}: hedge query quantity <= 0 strategy_id={} qty={:.8}",
            query.strategy_id,
            hedge_base_qty
        );
        return None;
    }

    let mkt_channel = MktChannel::instance();
    let Some(open_quote) = mkt_channel.get_quote(&open_symbol, open_venue) else {
        log::warn!(
            "{source}: hedge query missing open quote strategy_id={} symbol={} venue={:?}",
            query.strategy_id,
            open_symbol,
            open_venue
        );
        return None;
    };
    let Some(hedge_quote) = mkt_channel.get_quote(&hedge_symbol, hedge_venue) else {
        log::warn!(
            "{source}: hedge query missing hedge quote strategy_id={} symbol={} venue={:?}",
            query.strategy_id,
            hedge_symbol,
            hedge_venue
        );
        return None;
    };

    Some(ArbHedgePreparedQuery {
        side,
        open_symbol,
        hedge_symbol,
        open_venue,
        hedge_venue,
        hedge_base_qty,
        open_quote,
        hedge_quote,
        now: get_timestamp_us(),
    })
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

fn evaluate_hedge_stop_loss(
    query: &ArbHedgeSignalQueryMsg,
    hedge_quote: &Quote,
    threshold_pct: f64,
) -> ArbHedgeStopLossDecision {
    let open_bbo = Bbo::new(query.hedge_bid0, query.hedge_ask0, query.hedge_leg_ts);
    let hedge_bbo = Bbo::new(hedge_quote.bid, hedge_quote.ask, hedge_quote.ts);
    let open_mid = open_bbo.get_mid_price().unwrap_or(0.0);
    let hedge_mid = hedge_bbo.get_mid_price().unwrap_or(0.0);
    let mut pct_change = 0.0;
    let mut valid = false;
    let mut triggered = false;
    let threshold_ratio = threshold_pct / 100.0;
    if open_mid > 0.0 && hedge_mid > 0.0 {
        pct_change = (hedge_mid - open_mid).abs() / open_mid;
        valid = true;
        triggered = pct_change > threshold_ratio;
    }
    ArbHedgeStopLossDecision {
        pct_change,
        threshold_pct,
        valid,
        triggered,
    }
}

pub fn hedge_taker_market_price(side: Side, hedge_quote: &Quote) -> f64 {
    match side {
        Side::Buy => hedge_quote.ask,
        Side::Sell => hedge_quote.bid,
    }
}

pub fn hedge_maker_limit_price(side: Side, hedge_quote: &Quote, offset: f64) -> f64 {
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

pub fn evaluate_funding_mode_signal(
    spread_factor: &super::spread_factor::SpreadFactor,
    open_symbol_key: &str,
    hedge_symbol_key: &str,
    hedge_venue_symbol: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    in_dump: bool,
    rate_ready: bool,
    open_inputs_ready: bool,
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

    if !rate_ready && !open_inputs_ready {
        let signal = spread_close_signal();
        if signal.is_none() {
            log::debug!(
                "ArbDecision funding mode rate not ready and open inputs incomplete, no spread-close signal open={} hedge={} open_venue={:?} hedge_venue={:?}",
                open_symbol_key,
                hedge_symbol_key,
                open_venue,
                hedge_venue
            );
        } else {
            log::debug!(
                "ArbDecision funding mode rate not ready and open inputs incomplete, allow spread-close open={} hedge={} open_venue={:?} hedge_venue={:?}",
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
            "ArbDecision funding mode rate not fully ready, but symbol open inputs are ready; run full funding decision open={} hedge={} open_venue={:?} hedge_venue={:?}",
            open_symbol_key,
            hedge_symbol_key,
            open_venue,
            hedge_venue
        );
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
    let spread_factor = super::spread_factor::SpreadFactor::instance();
    let now = get_timestamp_us();
    let symbol_list = super::symbol_list::SymbolList::instance();
    let open_symbol_key = normalize_arb_symbol_key(open_symbol);
    let hedge_symbol_key = normalize_arb_symbol_key(hedge_symbol);
    if log::log_enabled!(log::Level::Debug) {
        log::debug!(
            "{FUNDING_ARB_SHELL_NAME} decision start open={} hedge={} open_venue={:?} hedge_venue={:?}",
            open_symbol_key,
            hedge_symbol_key,
            open_venue,
            hedge_venue
        );
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
    if let Some(cancel_gate) = ArbDecision::with_state_mut(|arb| {
        arb.evaluate_cancel_gate(
            forward_cancel,
            backward_cancel,
            open_symbol_key.as_str(),
            hedge_symbol_key.as_str(),
            open_venue,
            hedge_venue,
        )
    })
    .flatten()
    {
        log::debug!(
            "{FUNDING_ARB_SHELL_NAME} cancel triggered open={} hedge={} open_venue={:?} hedge_venue={:?} forward_cancel={} backward_cancel={}",
            open_symbol_key,
            hedge_symbol_key,
            open_venue,
            hedge_venue,
            forward_cancel,
            backward_cancel
        );
        emit_funding_spread_cancel(
            decision,
            open_symbol_key.as_str(),
            hedge_symbol_key.as_str(),
            open_venue,
            hedge_venue,
        )?;
        let _ = ArbDecision::with_state_mut(|arb| {
            arb.mark_signal_triggered(&SignalType::ArbCancel, cancel_gate.key, now);
        });
        return Ok(Some(SignalType::ArbCancel));
    }

    let in_dump = symbol_list.is_in_dump_list(open_symbol_key.as_str());
    if in_dump {
        log::debug!(
            "{FUNDING_ARB_SHELL_NAME} symbol in dump list open={} hedge={} open_venue={:?} hedge_venue={:?}",
            open_symbol_key,
            hedge_symbol_key,
            open_venue,
            hedge_venue
        );
    }

    let rate_ready = RateFetcher::is_initial_ready(hedge_venue);
    let open_inputs_ready = funding_open_inputs_ready(hedge_symbol_key.as_str(), hedge_venue);
    let fr_signal = evaluate_funding_mode_signal(
        &spread_factor,
        open_symbol_key.as_str(),
        hedge_symbol_key.as_str(),
        hedge_symbol_key.as_str(),
        open_venue,
        hedge_venue,
        in_dump,
        rate_ready,
        open_inputs_ready,
    )?;

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
            return Ok(None);
        }
    };

    let Some(control) = ArbDecision::with_state_mut(|arb| {
        arb.evaluate_funding_control(
            fr_signal,
            &spread_factor,
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
        return Ok(None);
    };

    emit_funding_open_close_signals(
        decision,
        open_symbol_key.as_str(),
        hedge_symbol_key.as_str(),
        open_venue,
        hedge_venue,
        control.final_signal.clone(),
        control.side,
        control.gate.as_ref(),
    )?;
    let _ = ArbDecision::with_state_mut(|arb| {
        arb.mark_signal_triggered(&control.final_signal, control.key, now);
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
    let _ = ArbDecision::with_state_mut(|arb| arb.poll_model_output_updates());
    let spread_factor = super::spread_factor::SpreadFactor::instance();
    let now = get_timestamp_us();
    let symbol_list = super::symbol_list::SymbolList::instance();
    let open_symbol_key = normalize_arb_symbol_key(open_symbol);
    let hedge_symbol_key = normalize_arb_symbol_key(hedge_symbol);

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
    if let Some(cancel_gate) = ArbDecision::with_state_mut(|arb| {
        arb.evaluate_cancel_gate(
            forward_cancel,
            backward_cancel,
            open_symbol_key.as_str(),
            hedge_symbol_key.as_str(),
            open_venue,
            hedge_venue,
        )
    })
    .flatten()
    {
        if let Err(err) = emit_spread_arb_spread_cancel(
            decision,
            open_symbol_key.as_str(),
            hedge_symbol_key.as_str(),
            open_venue,
            hedge_venue,
        ) {
            return Err(err);
        }
        let _ = ArbDecision::with_state_mut(|arb| {
            arb.mark_signal_triggered(&SignalType::ArbCancel, cancel_gate.key, now);
        });
        return Ok(Some(SignalType::ArbCancel));
    }

    let in_dump = symbol_list.is_in_dump_list(open_symbol_key.as_str());
    if in_dump {
        let Some(close_gate) = ArbDecision::with_state_mut(|arb| {
            arb.evaluate_close_gate(
                &spread_factor,
                open_symbol_key.as_str(),
                hedge_symbol_key.as_str(),
                open_venue,
                hedge_venue,
                now,
            )
        })
        .flatten() else {
            return Ok(None);
        };
        emit_spread_arb_close_signals(
            decision,
            open_symbol_key.as_str(),
            hedge_symbol_key.as_str(),
            open_venue,
            hedge_venue,
            close_gate.side,
        )?;
        let _ = ArbDecision::with_state_mut(|arb| {
            arb.mark_close_triggered(close_gate.key, now);
        });
        return Ok(Some(SignalType::ArbClose));
    }

    let Some(open_control) = ArbDecision::with_state_mut(|arb| {
        arb.evaluate_open_control(
            &spread_factor,
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
        return Ok(None);
    };

    emit_spread_arb_open_signals(
        decision,
        open_symbol_key.as_str(),
        hedge_symbol_key.as_str(),
        open_venue,
        hedge_venue,
        open_control.side,
        open_control.gate.return_score,
        open_control.gate.return_threshold,
        open_control.gate.open_filter_value,
        open_control.gate.environment_score,
        open_control.gate.environment_threshold,
        open_control.gate.open_volatility_factor,
    )?;
    let _ = ArbDecision::with_state_mut(|arb| {
        arb.mark_open_triggered(open_control.key, now);
    });
    Ok(Some(SignalType::ArbOpen))
}

fn drive_funding_hedge_query(decision: &mut FundingArbShell, query: ArbHedgeSignalQueryMsg) {
    drive_shared_arb_hedge_query(FUNDING_ARB_SHELL_NAME, &decision.runtime, query);
}

fn drive_spread_arb_hedge_query(decision: &mut SpreadArbShell, query: ArbHedgeSignalQueryMsg) {
    drive_shared_arb_hedge_query(SPREAD_ARB_SHELL_NAME, &decision.runtime, query);
}

fn drive_shared_arb_hedge_query(
    source: &'static str,
    runtime: &ArbShellRuntime,
    query: ArbHedgeSignalQueryMsg,
) {
    let Some(prepared) = prepare_arb_hedge_query(source, &query) else {
        return;
    };
    let side = prepared.side;
    let hedge_venue = prepared.hedge_venue;
    let hedge_symbol = prepared.hedge_symbol;
    let open_symbol = prepared.open_symbol;
    let open_venue = prepared.open_venue;
    let hedge_base_qty = prepared.hedge_base_qty;
    let open_quote = prepared.open_quote;
    let hedge_quote = prepared.hedge_quote;
    let now = prepared.now;
    let table = if hedge_venue == runtime.venues.0 {
        &runtime.open_min_qty_table
    } else {
        &runtime.hedge_min_qty_table
    };
    let symbol_key = super::arb_qty_align::min_qty_symbol_key(hedge_venue, &hedge_symbol);
    let qty_tick = table.step_size(&symbol_key).unwrap_or(0.0);
    let price_tick = table.price_tick(&symbol_key).unwrap_or(0.0);
    let spread_rate = super::common::compute_spread_rate(&open_quote, &hedge_quote);
    let open_symbol_key = normalize_arb_symbol_key(&open_symbol);

    let hedge_inputs = ArbDecision::with_state_mut(|arb| {
        arb.evaluate_shared_hedge_inputs(
            open_symbol_key.as_str(),
            &hedge_symbol,
            hedge_venue,
            side,
            now,
            &query,
            &hedge_quote,
        )
    })
    .expect("ArbDecisionState should be initialized");
    let environment_signal = hedge_inputs.environment_signal;
    let environment_score = hedge_inputs.environment_score;
    let return_score = hedge_inputs.return_score;
    let return_threshold = hedge_inputs.return_threshold;
    let target_factor_lookup = hedge_inputs.target_factor_lookup;
    let aggressive = hedge_inputs.aggressive;
    let default_offset = hedge_inputs.default_offset;
    let stop_loss = hedge_inputs.stop_loss;
    let (premium_rate, spread_fr) =
        match super::arb_open_filter::lookup_realtime_open_filter_value(
            &open_symbol,
            &hedge_symbol,
            open_venue,
            hedge_venue,
        ) {
            Some((value, "premium_rate")) => (Some(value), None),
            Some((value, "spread_fr")) => (None, Some(value)),
            _ => (None, None),
        };

    log::info!(
        "{source}: hedge stop-loss check strategy_id={} pct_change={:.6} threshold_pct={:.2} trigger={} valid={}",
        query.strategy_id,
        stop_loss.pct_change,
        stop_loss.threshold_pct,
        stop_loss.triggered,
        stop_loss.valid
    );

    if stop_loss.triggered {
        let market_price = hedge_taker_market_price(side, &hedge_quote);
        let aligned_hedge_qty = super::arb_qty_align::convert_aligned_base_qty_to_open_venue_qty(
            if hedge_venue == runtime.venues.0 {
                &runtime.open_min_qty_table
            } else {
                &runtime.hedge_min_qty_table
            },
            hedge_venue,
            &hedge_symbol,
            market_price,
            hedge_base_qty,
        );
        if aligned_hedge_qty <= 0.0 {
            log::warn!(
                "{source}: stop-loss taker aligned qty invalid strategy_id={} symbol_key={} base_qty={:.8}",
                query.strategy_id,
                symbol_key,
                hedge_base_qty
            );
            return;
        }

        let from_key = super::arb_hedge_context::build_spread_arb_hedge_from_key(
            source,
            now,
            return_score,
            return_threshold,
            environment_score,
            environment_signal.threshold,
            target_factor_lookup.target_factor_value,
            stop_loss.pct_change,
            spread_rate,
            premium_rate,
            spread_fr,
            hedge_venue,
            &hedge_symbol,
            market_price,
            if hedge_venue == runtime.venues.0 {
                &runtime.open_min_qty_table
            } else {
                &runtime.hedge_min_qty_table
            },
            if hedge_venue == runtime.venues.0 {
                &runtime.open_depth_query_client
            } else {
                runtime
                    .hedge_depth_query_client
                    .as_ref()
                    .expect("missing hedge depth query client")
            },
        );
        let ctx = match super::arb_hedge_context::build_fill_and_publish_arb_taker_hedge(
            super::arb_hedge_context::ArbTakerHedgeSignalInput {
                signal_pub: &runtime.signal_pub,
                now,
                strategy_id: query.strategy_id,
                client_order_id: query.client_order_id,
                side: side.to_u8(),
                hedge_qty: aligned_hedge_qty,
                qty_tick,
                common: super::arb_hedge_context::ArbHedgeContextCommonInput {
                    open_symbol: &open_symbol,
                    hedge_symbol: &hedge_symbol,
                    open_venue,
                    hedge_venue,
                    open_quote: &open_quote,
                    hedge_quote: &hedge_quote,
                    now,
                    price_offset: 0.0,
                    spread_rate,
                    from_key,
                },
            },
        ) {
            Ok(ctx) => ctx,
            Err(err) => {
                log::warn!(
                    "{source}: send stop-loss taker hedge failed strategy_id={} err={:?}",
                    query.strategy_id,
                    err
                );
                return;
            }
        };
        log::info!(
            "{source}: trigger stop-loss taker hedge strategy_id={} hedge_symbol={} qty={:.6} side={:?} pct_change={:.6} threshold_pct={:.2} spread_rate={:.6}",
            query.strategy_id,
            hedge_symbol,
            ctx.hedge_qty_value(),
            side,
            stop_loss.pct_change,
            stop_loss.threshold_pct,
            spread_rate
        );
        return;
    }

    let offset_decision =
        ArbDecisionState::resolve_hedge_offset(&target_factor_lookup, default_offset, aggressive);
    let offset = offset_decision.offset;

    log::info!(
        "{source}: hedge query offset source={} key={} symbol={} venue={:?} norm_symbol={} ready={} factor={:?} factor_index={:?} ts_ms={:?} offset={:.6} default_offset={:.6} aggressive={} note={}",
        offset_decision.source,
        target_factor_lookup.key,
        hedge_symbol,
        hedge_venue,
        target_factor_lookup.symbol_key,
        offset_decision.ready,
        target_factor_lookup.target_factor_value,
        target_factor_lookup.factor_index,
        target_factor_lookup.ts_ms,
        offset,
        default_offset,
        aggressive,
        offset_decision.note
    );

    let limit_price = hedge_maker_limit_price(side, &hedge_quote, offset);
    if limit_price <= 0.0 {
        log::warn!(
            "{source}: hedge query limit_price invalid strategy_id={} price={:.8}",
            query.strategy_id,
            limit_price
        );
        return;
    }

    let aligned_hedge_qty = super::arb_qty_align::convert_aligned_base_qty_to_open_venue_qty(
        if hedge_venue == runtime.venues.0 {
            &runtime.open_min_qty_table
        } else {
            &runtime.hedge_min_qty_table
        },
        hedge_venue,
        &hedge_symbol,
        limit_price,
        hedge_base_qty,
    );
    if aligned_hedge_qty <= 0.0 {
        log::warn!(
            "{source}: hedge query aligned qty invalid strategy_id={} symbol_key={} base_qty={:.8}",
            query.strategy_id,
            symbol_key,
            hedge_base_qty
        );
        return;
    }

    let hedge_timeout_mm_us = ArbDecision::with_state_mut(|arb| arb.hedge_timeout_mm_us)
        .expect("ArbDecisionState should be initialized");
    let Some(mut ctx) = super::arb_hedge_context::build_and_fill_arb_maker_hedge(
        super::arb_hedge_context::ArbMakerHedgeBuildInput {
            strategy_id: query.strategy_id,
            client_order_id: query.client_order_id,
            side: side.to_u8(),
            hedge_qty: aligned_hedge_qty,
            qty_tick,
            hedge_price: limit_price,
            price_tick,
            expire_ts: now + hedge_timeout_mm_us,
            common: super::arb_hedge_context::ArbHedgeContextCommonInput {
                open_symbol: &open_symbol,
                hedge_symbol: &hedge_symbol,
                open_venue,
                hedge_venue,
                open_quote: &open_quote,
                hedge_quote: &hedge_quote,
                now,
                price_offset: offset,
                spread_rate,
                from_key: Vec::new(),
            },
        },
    ) else {
        log::warn!(
            "{source}: hedge query qv invalid strategy_id={} qty={:.8} price={:.8}",
            query.strategy_id,
            aligned_hedge_qty,
            limit_price
        );
        return;
    };
    let from_key = super::arb_hedge_context::build_spread_arb_hedge_from_key(
        source,
        now,
        return_score,
        return_threshold,
        environment_score,
        environment_signal.threshold,
        target_factor_lookup.target_factor_value,
        stop_loss.pct_change,
        spread_rate,
        premium_rate,
        spread_fr,
        hedge_venue,
        &hedge_symbol,
        ctx.hedge_price_value(),
        if hedge_venue == runtime.venues.0 {
            &runtime.open_min_qty_table
        } else {
            &runtime.hedge_min_qty_table
        },
        if hedge_venue == runtime.venues.0 {
            &runtime.open_depth_query_client
        } else {
            runtime
                .hedge_depth_query_client
                .as_ref()
                .expect("missing hedge depth query client")
        },
    );
    ctx.set_from_key(from_key);
    if let Err(err) =
        super::arb_hedge_context::publish_arb_hedge_signal(&runtime.signal_pub, now, &ctx)
    {
        log::warn!(
            "{source}: send hedge signal failed strategy_id={} err={:?}",
            query.strategy_id,
            err
        );
        return;
    }

    log::info!(
        "{source}: reply hedge query strategy_id={} hedge_symbol={} qty={:.6} side={:?} seq={} aggressive={} limit_price={:.8} offset={:.6} spread_rate={:.6} (maker)",
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
            Ok(values) => values,
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
        log::info!(
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
            log::info!(
                "{FUNDING_ARB_SHELL_NAME}: ArbCancel tlen hits symbol={} trigger_ts={} candidates={} matched={} threshold={:.4} strategies={}",
                open_symbol,
                query.trigger_ts,
                preview.tick_indices.len(),
                group_cancel_sent,
                threshold,
                matched_preview.join(",")
            );
        }
    }
    if cancel_sent > 0 {
        log::info!(
            "{FUNDING_ARB_SHELL_NAME}: ArbCancel candidate query processed trigger_ts={} matched_symbols={} cancels_sent={}",
            query.trigger_ts, matched_symbols, cancel_sent
        );
    }
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
            Ok(values) => values,
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
        log::info!(
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
            log::info!(
                "{SPREAD_ARB_SHELL_NAME}: ArbCancel tlen hits symbol={} trigger_ts={} candidates={} matched={} threshold={:.4} strategies={}",
                open_symbol,
                query.trigger_ts,
                preview.tick_indices.len(),
                group_cancel_sent,
                threshold,
                matched_preview.join(",")
            );
        }
    }
    if cancel_sent > 0 {
        log::info!(
            "{SPREAD_ARB_SHELL_NAME}: ArbCancel candidate query processed trigger_ts={} matched_symbols={} cancels_sent={}",
            query.trigger_ts, matched_symbols, cancel_sent
        );
    }
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
            Side::Buy,
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
        snapshot.return_score,
        snapshot.return_threshold,
        snapshot.volatility,
        snapshot.open_scale,
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
    let return_score = ArbDecision::with_state_mut(|arb| {
        arb.lookup_return_model_score_lookup(hedge_symbol, hedge_venue)
            .and_then(|lookup| lookup.score)
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
        return_score,
        None,
        environment_score,
        environment_signal.threshold,
        volatility,
        ArbDecision::with_state_mut(|arb| Some(arb.open_scale)).flatten(),
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
        strategy_id,
    })
}

fn emit_spread_arb_spread_cancel(
    decision: &SpreadArbShell,
    open_symbol: &str,
    hedge_symbol: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> Result<()> {
    let (open_quote, hedge_quote) =
        match ArbDecision::load_valid_quotes(open_symbol, hedge_symbol, open_venue, hedge_venue) {
            Some(quotes) => quotes,
            None => return Ok(()),
        };
    let batch_ts = get_timestamp_us();
    let spread_rate = super::common::compute_spread_rate(&open_quote, &hedge_quote);
    let return_score = ArbDecision::with_state_mut(|arb| {
        arb.lookup_return_model_score_lookup(hedge_symbol, hedge_venue)
            .and_then(|lookup| lookup.score)
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
        return_score,
        None,
        environment_score,
        environment_signal.threshold,
        volatility,
        ArbDecision::with_state_mut(|arb| Some(arb.open_scale)).flatten(),
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
    decision: &SpreadArbShell,
    open_symbol: &str,
    hedge_symbol: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    side: Side,
    return_score: Option<f64>,
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
    let open_scale = ArbDecision::with_state_mut(|arb| arb.open_scale)
        .expect("ArbDecisionState should be initialized");
    let scaled_volatility = (open_volatility_factor * open_scale).max(0.0);
    let base_from_key = super::common::build_open_from_key_base(
        batch_ts,
        return_score,
        return_threshold,
        Some(open_volatility_factor),
        Some(open_scale),
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
    let order_amount = ArbDecision::with_state_mut(|arb| arb.order_amount)
        .expect("ArbDecisionState should be initialized") as f64;
    let open_orders_per_round = ArbDecision::with_state_mut(|arb| arb.open_orders_per_round)
        .expect("ArbDecisionState should be initialized");
    let open_order_ttl_us = ArbDecision::with_state_mut(|arb| arb.open_order_ttl_us)
        .expect("ArbDecisionState should be initialized");
    let hedge_timeout_mm_us = ArbDecision::with_state_mut(|arb| arb.hedge_timeout_mm_us)
        .expect("ArbDecisionState should be initialized");
    let factor_mode = super::spread_factor::SpreadFactor::instance().get_mode();

    let plan = match super::arb_quote_plan::build_arb_open_quote_plan(
        open_venue,
        open_symbol,
        open_quote,
        order_amount,
        open_orders_per_round,
        side,
        scaled_volatility,
        if open_venue == decision.runtime.venues.0 {
            &decision.runtime.open_min_qty_table
        } else {
            &decision.runtime.hedge_min_qty_table
        },
    ) {
        Ok(plan) => plan,
        Err(err) => {
            log::warn!(
                "{SPREAD_ARB_SHELL_NAME}: build open quote plan failed open={} hedge={} side={:?} err={}",
                open_symbol, hedge_symbol, side, err
            );
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
                arb.tlen_thresholds.get(&query_symbol.to_ascii_uppercase()).copied()
            } else {
                None
            }
        })
        .flatten();
        let (from_keys, filtered_levels) = super::common::apply_open_tlen_gate_and_build_from_keys(
            SPREAD_ARB_SHELL_NAME,
            open_depth_query_client,
            &query_symbol,
            &tick_indices,
            &from_key,
            tlen_gate,
        );
        contexts = from_keys
            .into_iter()
            .zip(contexts.into_iter())
            .filter_map(|(from_key_bytes, mut ctx)| {
                let from_key_bytes = from_key_bytes?;
                ctx.set_from_key(from_key_bytes);
                Some(ctx)
            })
            .collect();
        if filtered_levels > 0 {
            log::info!(
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

    let _ = super::arb_emit::emit_levels_as_signals(
        &decision.runtime.signal_pub,
        SignalType::ArbOpen,
        batch_ts,
        contexts,
        |ctx| ctx.to_bytes(),
    )?;

    log::info!(
        "{SPREAD_ARB_SHELL_NAME}: emitted {} {:?} signal(s) to '{}' open={} hedge={} side={:?} inner={:.8} outer={:.8} vol={:.8} open_scale={:.6} scaled_vol={:.8} price_tick={:.8} qty_tick={:.8}",
        plan.levels.len(),
        SignalType::ArbOpen,
        DEFAULT_ARBITRAGE_SIGNAL_CHANNEL,
        open_symbol,
        hedge_symbol,
        plan.side,
        plan.inner_price,
        plan.outer_price,
        open_volatility_factor,
        open_scale,
        scaled_volatility,
        plan.price_tick,
        plan.qty_tick
    );
    Ok(())
}

fn emit_spread_arb_close_signals(
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
    let snapshot = ArbDecision::with_state_mut(|arb| {
        arb.snapshot_open_from_key_fields(
            open_symbol,
            hedge_symbol,
            open_venue,
            hedge_venue,
            side,
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
            snapshot.return_score,
            snapshot.return_threshold,
            snapshot.volatility,
            snapshot.open_scale,
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
    let order_amount = ArbDecision::with_state_mut(|arb| arb.order_amount)
        .expect("ArbDecisionState should be initialized") as f64;
    let open_orders_per_round = ArbDecision::with_state_mut(|arb| arb.open_orders_per_round)
        .expect("ArbDecisionState should be initialized");
    let open_order_ttl_us = ArbDecision::with_state_mut(|arb| arb.open_order_ttl_us)
        .expect("ArbDecisionState should be initialized");
    let hedge_timeout_mm_us = ArbDecision::with_state_mut(|arb| arb.hedge_timeout_mm_us)
        .expect("ArbDecisionState should be initialized");
    let factor_mode = super::spread_factor::SpreadFactor::instance().get_mode();

    let plan = match super::arb_quote_plan::build_arb_open_quote_plan(
        open_venue,
        open_symbol,
        open_quote,
        order_amount,
        open_orders_per_round,
        side,
        volatility,
        if open_venue == decision.runtime.venues.0 {
            &decision.runtime.open_min_qty_table
        } else {
            &decision.runtime.hedge_min_qty_table
        },
    ) {
        Ok(plan) => plan,
        Err(err) => {
            log::warn!(
                "{SPREAD_ARB_SHELL_NAME}: build close quote plan failed open={} hedge={} side={:?} err={}",
                open_symbol, hedge_symbol, side, err
            );
            return Ok(());
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
            ctx.set_from_key(super::common::append_tlen_to_from_key(&from_key, level_tlen).into_bytes());
        }
    }
    let _ = super::arb_emit::emit_levels_as_signals(
        &decision.runtime.signal_pub,
        SignalType::ArbClose,
        batch_ts,
        contexts,
        |ctx| ctx.to_bytes(),
    )?;

    log::info!(
        "{SPREAD_ARB_SHELL_NAME}: emitted {} {:?} signal(s) to '{}' open={} hedge={} side={:?} inner={:.8} outer={:.8} vol={:.8} price_tick={:.8} qty_tick={:.8}",
        plan.levels.len(),
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
    Ok(())
}

fn emit_funding_open_close_signals(
    decision: &FundingArbShell,
    spot_symbol: &str,
    futures_symbol: &str,
    spot_venue: TradingVenue,
    futures_venue: TradingVenue,
    signal_type: SignalType,
    side: Side,
    gate: Option<&ArbOpenGatePassed>,
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
    let open_scale = ArbDecision::with_state_mut(|arb| arb.open_scale)
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
            gate.and_then(|v| v.return_score),
            gate.and_then(|v| v.return_threshold),
            gate.map(|v| v.open_volatility_factor),
            gate.map(|v| v.environment_score),
            gate.and_then(|v| v.environment_threshold),
            premium_rate,
            Some(open_scale),
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
                side,
                batch_ts,
            )
        })
        .unwrap_or_default();
        super::common::append_dump_suffix(
            super::arb_from_key::build_funding_decision_from_key_base(
                batch_ts,
                snapshot.return_score,
                snapshot.return_threshold,
                snapshot.volatility,
                snapshot.open_scale,
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
    let plan_volatility = if matches!(signal_type, SignalType::ArbOpen) {
        (raw_volatility * open_scale).max(0.0)
    } else {
        raw_volatility
    };
    let order_amount = ArbDecision::with_state_mut(|arb| arb.order_amount)
        .expect("ArbDecisionState should be initialized") as f64;
    let open_orders_per_round = ArbDecision::with_state_mut(|arb| arb.open_orders_per_round)
        .expect("ArbDecisionState should be initialized");
    let open_order_ttl_us = ArbDecision::with_state_mut(|arb| arb.open_order_ttl_us)
        .expect("ArbDecisionState should be initialized");
    let hedge_timeout_mm_us = ArbDecision::with_state_mut(|arb| arb.hedge_timeout_mm_us)
        .expect("ArbDecisionState should be initialized");
    let factor_mode = super::spread_factor::SpreadFactor::instance().get_mode();

    let plan = match super::arb_quote_plan::build_arb_open_quote_plan(
        spot_venue,
        spot_symbol,
        spot_quote,
        order_amount,
        open_orders_per_round,
        side,
        plan_volatility,
        if spot_venue == decision.runtime.venues.0 {
            &decision.runtime.open_min_qty_table
        } else {
            &decision.runtime.hedge_min_qty_table
        },
    ) {
        Ok(plan) => plan,
        Err(err) => {
            log::warn!(
                "{FUNDING_ARB_SHELL_NAME}: build quote plan failed open={} hedge={} side={:?} signal={:?} err={}",
                spot_symbol, futures_symbol, side, signal_type, err
            );
            return Ok(());
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
                arb.tlen_thresholds.get(&query_symbol.to_ascii_uppercase()).copied()
            } else {
                None
            }
        })
        .flatten();
        let from_key_str = std::str::from_utf8(&from_key).unwrap_or("");
        let (from_keys, filtered_levels) = super::common::apply_open_tlen_gate_and_build_from_keys(
            FUNDING_ARB_SHELL_NAME,
            open_depth_query_client,
            &query_symbol,
            &tick_indices,
            from_key_str,
            tlen_gate,
        );
        contexts = from_keys
            .into_iter()
            .zip(contexts.into_iter())
            .filter_map(|(from_key_bytes, mut ctx)| {
                let from_key_bytes = from_key_bytes?;
                ctx.set_from_key(from_key_bytes);
                Some(ctx)
            })
            .collect();
        if filtered_levels > 0 {
            log::info!(
                "{FUNDING_ARB_SHELL_NAME}: ArbOpen tlen gated symbol={} threshold={:?} filtered_levels={} kept_levels={}",
                query_symbol,
                tlen_gate,
                filtered_levels,
                contexts.len()
            );
        }
    }
    let _ = super::arb_emit::emit_levels_as_signals(
        &decision.runtime.signal_pub,
        signal_type.clone(),
        batch_ts,
        contexts,
        |ctx| ctx.to_bytes(),
    )?;

    log::info!(
        "{FUNDING_ARB_SHELL_NAME}: emitted {} {:?} signal(s) to '{}' open={} hedge={} side={:?} inner={:.8} outer={:.8} vol={:.8} open_scale={:.6} plan_vol={:.8} price_tick={:.8} qty_tick={:.8}",
        plan.levels.len(),
        signal_type,
        DEFAULT_ARBITRAGE_SIGNAL_CHANNEL,
        spot_symbol,
        futures_symbol,
        plan.side,
        plan.inner_price,
        plan.outer_price,
        raw_volatility,
        open_scale,
        plan_volatility,
        plan.price_tick,
        plan.qty_tick
    );
    Ok(())
}

fn emit_funding_spread_cancel(
    decision: &FundingArbShell,
    spot_symbol: &str,
    futures_symbol: &str,
    spot_venue: TradingVenue,
    futures_venue: TradingVenue,
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
            Side::Buy,
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
        snapshot.return_score,
        snapshot.return_threshold,
        snapshot.volatility,
        snapshot.open_scale,
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
    pub return_score: Option<f64>,
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
    return_score: Option<f64>,
    return_threshold: Option<f64>,
    volatility: Option<f64>,
    env_score: Option<f64>,
    env_threshold: Option<f64>,
    open_scale: Option<f64>,
}

#[derive(Debug, Clone)]
pub(crate) struct ArbCloseGatePassed {
    pub side: Side,
    pub key: ThresholdKey,
}

#[derive(Debug, Clone)]
pub(crate) struct ArbCancelGatePassed {
    pub key: ThresholdKey,
}

#[derive(Debug, Clone)]
pub(crate) struct ArbHedgePreparedQuery {
    pub side: Side,
    pub open_symbol: String,
    pub hedge_symbol: String,
    pub open_venue: TradingVenue,
    pub hedge_venue: TradingVenue,
    pub hedge_base_qty: f64,
    pub open_quote: Quote,
    pub hedge_quote: Quote,
    pub now: i64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ArbHedgeStopLossDecision {
    pub pct_change: f64,
    pub threshold_pct: f64,
    pub valid: bool,
    pub triggered: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct ArbFundingControlPassed {
    pub final_signal: SignalType,
    pub side: Side,
    pub key: ThresholdKey,
    pub gate: Option<ArbOpenGatePassed>,
}

#[derive(Debug, Clone)]
pub(crate) struct ArbSharedBootstrap {
    pub open_scale: f64,
    pub open_orders_per_round: u32,
    pub order_amount: f32,
    pub open_order_ttl_us: i64,
    pub hedge_timeout_mm_us: i64,
    pub hedge_price_offset: f64,
    pub hedge_aggressive_seq_threshold: u32,
    pub enable_tlen_cancel: bool,
    pub tlen_cancel_freq_ms: u64,
    pub signal_cooldown_us: i64,
    pub last_open_ts: Rc<RefCell<HashMap<ThresholdKey, i64>>>,
    pub last_close_ts: Rc<RefCell<HashMap<ThresholdKey, i64>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct ArbXarbHedgeInputs {
    pub environment_signal: EnvironmentSignalResult,
    pub environment_score: f64,
    pub return_score: Option<f64>,
    pub return_threshold: Option<f64>,
    pub target_factor_lookup: FactorValueLookupResult,
    pub aggressive: bool,
    pub default_offset: f64,
    pub stop_loss: ArbHedgeStopLossDecision,
}

#[derive(Debug, Clone)]
pub(crate) struct ArbSharedHedgeInputs {
    pub environment_signal: EnvironmentSignalResult,
    pub environment_score: f64,
    pub return_score: Option<f64>,
    pub return_threshold: Option<f64>,
    pub target_factor_lookup: FactorValueLookupResult,
    pub aggressive: bool,
    pub default_offset: f64,
    pub stop_loss: ArbHedgeStopLossDecision,
}

#[derive(Debug, Clone)]
pub(crate) struct ArbHedgeOffsetDecision {
    pub offset: f64,
    pub source: &'static str,
    pub note: String,
    pub ready: bool,
}

pub(crate) struct ArbDecisionState {
    pub venues: VenuePair,
    pub open_factor_value_hub: Option<FactorValueHub>,
    pub hedge_factor_value_hub: Option<FactorValueHub>,
    pub open_scale: f64,
    pub open_orders_per_round: u32,
    pub order_amount: f32,
    pub open_order_ttl_us: i64,
    pub hedge_timeout_mm_us: i64,
    pub hedge_price_offset: f64,
    pub hedge_aggressive_seq_threshold: u32,
    pub max_hedge_price_pct_change: f64,
    pub enable_tlen_cancel: bool,
    pub tlen_cancel_freq_ms: u64,
    pub enable_environment_model: bool,
    pub enable_volatility_limit: bool,
    pub open_volatility_limit: f64,
    pub return_model_service: Option<String>,
    pub environment_model_service: Option<String>,
    pub environment_model_true_threshold: f64,
    pub return_score_thresholds: HashMap<String, ReturnScoreThresholdsResolved>,
    pub funding_open_thresholds: HashMap<String, XarbFundingThresholdsResolved>,
    pub open_volatility_thresholds: HashMap<String, f64>,
    pub tlen_thresholds: HashMap<String, f64>,
    pub signal_cooldown_us: i64,
    pub last_open_ts: Rc<RefCell<HashMap<ThresholdKey, i64>>>,
    pub last_close_ts: Rc<RefCell<HashMap<ThresholdKey, i64>>>,
    pub last_tlen_threshold_reload_ts_us: i64,
    pub last_cancel_trigger_ts_us: i64,
    pub intercept_counts: HashMap<String, u64>,
}

impl ArbDecisionState {
    pub fn new(_mode: ArbMode, venues: VenuePair) -> Self {
        Self {
            venues,
            open_factor_value_hub: None,
            hedge_factor_value_hub: None,
            open_scale: 1.0,
            open_orders_per_round: 1,
            order_amount: 100.0,
            open_order_ttl_us: 120_000_000,
            hedge_timeout_mm_us: 30_000_000,
            hedge_price_offset: 0.0003,
            hedge_aggressive_seq_threshold: 6,
            max_hedge_price_pct_change: 5.0,
            enable_tlen_cancel: false,
            tlen_cancel_freq_ms: 3_000,
            enable_environment_model: true,
            enable_volatility_limit: true,
            open_volatility_limit: 70.0,
            return_model_service: None,
            environment_model_service: None,
            environment_model_true_threshold: 0.0,
            return_score_thresholds: HashMap::new(),
            funding_open_thresholds: HashMap::new(),
            open_volatility_thresholds: HashMap::new(),
            tlen_thresholds: HashMap::new(),
            signal_cooldown_us: 5_000_000,
            last_open_ts: Rc::new(RefCell::new(HashMap::new())),
            last_close_ts: Rc::new(RefCell::new(HashMap::new())),
            last_tlen_threshold_reload_ts_us: 0,
            last_cancel_trigger_ts_us: 0,
            intercept_counts: HashMap::new(),
        }
    }

    pub fn poll_model_output_updates(&mut self) {
        if let Some(hub) = self.hedge_factor_value_hub.as_mut() {
            hub.poll_model_output_updates();
        }
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
    ) -> Option<ArbCloseGatePassed> {
        let side = Self::evaluate_close_side(
            spread_factor,
            open_symbol_key,
            hedge_symbol_key,
            open_venue,
            hedge_venue,
        )?;
        let key =
            Self::build_threshold_key(open_symbol_key, hedge_symbol_key, open_venue, hedge_venue);
        if self.is_close_cooldown_hit(&key, now) {
            return None;
        }
        Some(ArbCloseGatePassed { side, key })
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

    pub fn resolve_hedge_offset(
        target_factor_lookup: &FactorValueLookupResult,
        default_offset: f64,
        aggressive: bool,
    ) -> ArbHedgeOffsetDecision {
        let mut offset = default_offset;
        let mut source = "config";
        let mut note = String::new();
        let ready = target_factor_lookup.ready.unwrap_or(false);

        if ready {
            if let Some(value) = target_factor_lookup.target_factor_value {
                if value.is_finite() && value > 0.0 {
                    offset = value;
                    source = "hedge_volatility_factor";
                } else {
                    note = "invalid_factor".to_string();
                }
            } else {
                note = "missing_factor".to_string();
            }
        } else if target_factor_lookup.note == "ok" {
            note = "not_ready".to_string();
        } else {
            note = target_factor_lookup.note.clone();
        }

        if aggressive {
            offset = 0.0;
            source = "aggressive";
            if note.is_empty() {
                note = "aggressive_override".to_string();
            } else {
                note = format!("aggressive_override({})", note);
            }
        }

        ArbHedgeOffsetDecision {
            offset,
            source,
            note,
            ready,
        }
    }

    pub fn apply_shared_bootstrap(&mut self, bootstrap: ArbSharedBootstrap) {
        self.open_scale = bootstrap.open_scale;
        self.open_orders_per_round = bootstrap.open_orders_per_round;
        self.order_amount = bootstrap.order_amount;
        self.open_order_ttl_us = bootstrap.open_order_ttl_us;
        self.hedge_timeout_mm_us = bootstrap.hedge_timeout_mm_us;
        self.hedge_price_offset = bootstrap.hedge_price_offset;
        self.hedge_aggressive_seq_threshold = bootstrap.hedge_aggressive_seq_threshold;
        self.enable_tlen_cancel = bootstrap.enable_tlen_cancel;
        self.tlen_cancel_freq_ms = bootstrap.tlen_cancel_freq_ms;
        self.signal_cooldown_us = bootstrap.signal_cooldown_us;
        self.last_open_ts = bootstrap.last_open_ts;
        self.last_close_ts = bootstrap.last_close_ts;
    }

    pub fn default_shared_bootstrap(open_orders_per_round: u32) -> ArbSharedBootstrap {
        ArbSharedBootstrap {
            open_scale: 1.0,
            open_orders_per_round,
            order_amount: 100.0,
            open_order_ttl_us: 120_000_000,
            hedge_timeout_mm_us: 30_000_000,
            hedge_price_offset: 0.0003,
            hedge_aggressive_seq_threshold: 6,
            enable_tlen_cancel: false,
            tlen_cancel_freq_ms: 3_000,
            signal_cooldown_us: 5_000_000,
            last_open_ts: Rc::new(RefCell::new(HashMap::new())),
            last_close_ts: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    pub fn evaluate_xarb_hedge_inputs(
        &mut self,
        open_symbol_key: &str,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
        side: Side,
        now: i64,
        query: &ArbHedgeSignalQueryMsg,
        hedge_quote: &Quote,
    ) -> ArbXarbHedgeInputs {
        let environment_signal =
            self.evaluate_environment_signal(open_symbol_key, hedge_symbol, hedge_venue, now);
        let return_lookup = self.lookup_return_model_score_lookup(hedge_symbol, hedge_venue);
        let return_score = return_lookup
            .as_ref()
            .and_then(|lookup| lookup.score)
            .filter(|v| v.is_finite());
        let return_threshold = return_lookup.as_ref().and_then(|lookup| {
            self.lookup_return_score_thresholds(&lookup.symbol_key)
                .map(|thresholds| select_open_return_threshold_by_hedge_side(side, thresholds))
        });
        let environment_score = environment_signal
            .score
            .unwrap_or(environment_signal.class_label as f64);
        let target_factor_lookup = self.lookup_hedge_factor_value(hedge_symbol, hedge_venue);
        let aggressive = query.request_seq >= self.hedge_aggressive_seq_threshold;
        let default_offset = self.hedge_price_offset.abs();
        let stop_loss =
            evaluate_hedge_stop_loss(query, hedge_quote, self.max_hedge_price_pct_change);
        ArbXarbHedgeInputs {
            environment_signal,
            environment_score,
            return_score,
            return_threshold,
            target_factor_lookup,
            aggressive,
            default_offset,
            stop_loss,
        }
    }

    pub fn evaluate_shared_hedge_inputs(
        &mut self,
        open_symbol_key: &str,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
        side: Side,
        now: i64,
        query: &ArbHedgeSignalQueryMsg,
        hedge_quote: &Quote,
    ) -> ArbSharedHedgeInputs {
        let xarb = self.evaluate_xarb_hedge_inputs(
            open_symbol_key,
            hedge_symbol,
            hedge_venue,
            side,
            now,
            query,
            hedge_quote,
        );
        ArbSharedHedgeInputs {
            environment_signal: xarb.environment_signal,
            environment_score: xarb.environment_score,
            return_score: xarb.return_score,
            return_threshold: xarb.return_threshold,
            target_factor_lookup: xarb.target_factor_lookup,
            aggressive: xarb.aggressive,
            default_offset: xarb.default_offset,
            stop_loss: xarb.stop_loss,
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
        let side = side?;
        let key =
            Self::build_threshold_key(open_symbol_key, hedge_symbol_key, open_venue, hedge_venue);
        if self.is_open_cooldown_hit(&key, now) {
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
        )?;
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
        if self.is_signal_cooldown_hit(&final_signal, &key, now) {
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

    pub fn is_open_cooldown_hit(&self, key: &ThresholdKey, now: i64) -> bool {
        is_cooldown_hit(&self.last_open_ts, key, now, self.signal_cooldown_us)
    }

    pub fn is_close_cooldown_hit(&self, key: &ThresholdKey, now: i64) -> bool {
        is_cooldown_hit(&self.last_close_ts, key, now, self.signal_cooldown_us)
    }

    pub fn build_threshold_key(
        open_symbol_key: &str,
        hedge_symbol_key: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> ThresholdKey {
        threshold_key(open_symbol_key, hedge_symbol_key, open_venue, hedge_venue)
    }

    pub fn mark_open_triggered(&self, key: ThresholdKey, now: i64) {
        update_last_ts(&self.last_open_ts, key, now);
    }

    pub fn mark_close_triggered(&self, key: ThresholdKey, now: i64) {
        update_last_ts(&self.last_close_ts, key, now);
    }

    pub fn is_signal_cooldown_hit(
        &self,
        signal_type: &SignalType,
        key: &ThresholdKey,
        now: i64,
    ) -> bool {
        match signal_type {
            SignalType::ArbOpen => self.is_open_cooldown_hit(key, now),
            SignalType::ArbClose => self.is_close_cooldown_hit(key, now),
            _ => false,
        }
    }

    pub fn mark_signal_triggered(&self, signal_type: &SignalType, key: ThresholdKey, now: i64) {
        match signal_type {
            SignalType::ArbOpen => self.mark_open_triggered(key, now),
            SignalType::ArbClose => self.mark_close_triggered(key, now),
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
            .lookup_factor_value(hedge_symbol, hedge_venue)
    }

    pub fn lookup_return_model_score_lookup(
        &mut self,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
    ) -> Option<ModelOutputScoreLookupResult> {
        let Some(service_name) = self.return_model_service.clone() else {
            return None;
        };
        Some(
            self.hedge_factor_value_hub
                .as_mut()
                .expect("ArbDecisionState.hedge_factor_value_hub must be initialized")
                .lookup_model_output_score(&service_name, hedge_symbol, hedge_venue),
        )
    }

    pub fn lookup_return_score_thresholds(
        &self,
        model_symbol_key: &str,
    ) -> Option<ReturnScoreThresholdsResolved> {
        self.return_score_thresholds
            .get(&model_symbol_key.to_ascii_uppercase())
            .copied()
    }

    pub fn lookup_funding_open_thresholds(
        &self,
        symbol_key: &str,
    ) -> Option<XarbFundingThresholdsResolved> {
        self.funding_open_thresholds
            .get(&symbol_key.to_ascii_uppercase())
            .copied()
    }

    pub fn lookup_open_volatility_threshold(&self, symbol_key: &str) -> Option<f64> {
        self.open_volatility_thresholds
            .get(&symbol_key.to_ascii_uppercase())
            .copied()
    }

    fn snapshot_open_from_key_fields(
        &mut self,
        open_symbol_key: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        side: Side,
        now_us: i64,
    ) -> OpenFromKeySnapshot {
        let return_lookup = self.lookup_return_model_score_lookup(hedge_symbol, hedge_venue);
        let return_score = return_lookup
            .as_ref()
            .and_then(|lookup| lookup.score)
            .filter(|v| v.is_finite());
        let return_threshold = return_lookup.as_ref().and_then(|lookup| {
            self.lookup_return_score_thresholds(&lookup.symbol_key)
                .map(|thresholds| select_open_return_threshold(side, thresholds))
        });
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
            return_score,
            return_threshold,
            volatility,
            env_score,
            env_threshold: environment_signal.threshold,
            open_scale: Some(self.open_scale),
        }
    }

    pub fn evaluate_environment_signal(
        &mut self,
        open_symbol_key: &str,
        hedge_symbol: &str,
        hedge_venue: TradingVenue,
        now_us: i64,
    ) -> EnvironmentSignalResult {
        self.hedge_factor_value_hub
            .as_mut()
            .expect("ArbDecisionState.hedge_factor_value_hub must be initialized")
            .evaluate_environment_signal(
                self.environment_model_service.as_deref(),
                hedge_symbol,
                hedge_venue,
                self.environment_model_true_threshold,
                open_symbol_key,
                now_us,
            )
    }

    pub fn record_intercept_summary(&mut self, reason: impl Into<String>) {
        *self.intercept_counts.entry(reason.into()).or_insert(0) += 1;
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
            (_, _, _, false) => format!("env_block:{}", result.note),
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
        let open_filter_thresholds = self.lookup_funding_open_thresholds(open_symbol_key)?;
        let (open_filter_value, open_filter_source) = lookup_realtime_open_filter_value(
            open_symbol_key,
            hedge_symbol_key,
            open_venue,
            hedge_venue,
        )?;
        let open_filter_threshold = select_open_filter_threshold(side, open_filter_thresholds);
        let open_filter_hit = match side {
            Side::Buy => open_filter_value > open_filter_threshold,
            Side::Sell => open_filter_value < open_filter_threshold,
        };
        if !open_filter_hit {
            self.record_intercept_summary(format!(
                "skip_open_by_filter:source={open_filter_source}"
            ));
            return None;
        }

        let return_lookup = self.lookup_return_model_score_lookup(hedge_symbol, hedge_venue);
        let return_score = return_lookup
            .as_ref()
            .and_then(|lookup| lookup.score)
            .filter(|v| v.is_finite());
        let return_threshold = return_lookup.as_ref().and_then(|lookup| {
            self.lookup_return_score_thresholds(&lookup.symbol_key)
                .map(|thresholds| select_open_return_threshold(side, thresholds))
        });

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
        let open_volatility_factor =
            match self.lookup_open_factor_value(open_symbol_key, open_venue) {
                lookup if lookup.ready == Some(true) => {
                    let Some(value) = lookup.target_factor_value.filter(|v| v.is_finite()) else {
                        self.record_intercept_summary(format!(
                            "drop_open_target_factor_not_ready:note={}",
                            lookup.note
                        ));
                        return None;
                    };
                    value
                }
                lookup => {
                    self.record_intercept_summary(format!(
                        "drop_open_target_factor_not_ready:note={}",
                        lookup.note
                    ));
                    return None;
                }
            };

        if self.enable_volatility_limit {
            let Some(open_volatility_threshold) =
                self.lookup_open_volatility_threshold(open_symbol_key)
            else {
                self.record_intercept_summary("drop_open_missing_open_volatility_threshold");
                return None;
            };
            if open_volatility_factor > open_volatility_threshold {
                self.record_intercept_summary(format!(
                    "skip_open_by_volatility_limit:value={:.8}:threshold={:.8}",
                    open_volatility_factor, open_volatility_threshold
                ));
                return None;
            }
        }

        Some(ArbOpenGatePassed {
            return_score,
            return_threshold,
            open_filter_value: Some(open_filter_value),
            environment_score,
            environment_threshold: environment_signal.threshold,
            open_volatility_factor,
        })
    }
}

impl ArbDecision {
    pub(crate) fn try_update_spread_arb_model_output_services(services: Vec<String>) -> bool {
        try_update_spread_arb_model_output_services(services)
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
            ArbMode::SpotFuturesXarb | ArbMode::FuturesPairXarb => ArbBackend::Spread,
        }
    }

    pub async fn init_singleton(
        mode: ArbMode,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        exchange: Option<Exchange>,
    ) -> Result<()> {
        Self::init_mode(mode)?;
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
                                &spread_factor,
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
