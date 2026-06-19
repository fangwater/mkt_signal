use anyhow::{bail, Context, Result};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::env;
use std::rc::Rc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

use crate::mkt_pub::cfg::Config;
use order_common::TradingVenue;
use rolling_common::latency_kll::LatencyStats;
use rolling_common::latency_snapshot::{
    LatencyBucketStat, LatencySnapshotMsg, ACTION_ID_MARKET_DATA, METRIC_ID_SPREAD_E2E,
};
use runtime_common::time_util::get_timestamp_us;

use crate::spread_pbs::adapter::{
    create_adapter, BboFrame, IncrementalFrame, KeepaliveSpec, TradeFrame, VenueAdapter,
};
use crate::spread_pbs::binance::{
    binance_futures_mm_ws_enabled, binance_futures_standard_ws_url,
    ENV_BINANCE_FUTURES_MM_WS_LOCAL_IP, ENV_BINANCE_FUTURES_MM_WS_MODE,
};
use crate::spread_pbs::latency::LatencyKll;
use crate::spread_pbs::okex_derivatives::{
    build_okex_derivatives_subscribe_msgs, parse_okex_derivatives_frame, OKEX_PUBLIC_WS_URL,
};
use crate::spread_pbs::publisher::{
    SpreadDerivativesPublisher, SpreadIncrementalPublisher, SpreadLatencyPublisher,
    SpreadPublisher, SpreadTradePublisher,
};
use crate::spread_pbs::ws::{run_public_ws, FrameHandler, WsLoopParams};

const DEDUP_RESET_INTERVAL_US: i64 = 5 * 60 * 1_000_000;
const ENV_ENABLE_TRADE: &str = "SPREAD_PBS_ENABLE_TRADE";
const ENV_ENABLE_INCREMENTAL: &str = "SPREAD_PBS_ENABLE_INCREMENTAL";
const ENV_ENABLE_DERIVATIVES: &str = "SPREAD_PBS_ENABLE_DERIVATIVES";
const ENV_SYMBOLS: &str = "SPREAD_PBS_SYMBOLS";

pub struct SpreadPbsApp {
    config: Config,
}

fn is_okex_venue(venue: order_common::TradingVenue) -> bool {
    matches!(
        venue,
        order_common::TradingVenue::OkexMargin | order_common::TradingVenue::OkexFutures
    )
}

fn is_bitget_venue(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BitgetMargin | TradingVenue::BitgetFutures
    )
}

fn is_bybit_venue(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BybitMargin | TradingVenue::BybitFutures
    )
}

fn is_binance_venue(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BinanceMargin | TradingVenue::BinanceFutures
    )
}

fn is_okex_derivatives_venue(venue: TradingVenue) -> bool {
    matches!(venue, order_common::TradingVenue::OkexFutures)
}

fn direct_trade_replacement_enabled(venue: TradingVenue) -> bool {
    is_okex_venue(venue)
        || is_bitget_venue(venue)
        || is_bybit_venue(venue)
        || is_binance_venue(venue)
        || matches!(venue, TradingVenue::GateMargin | TradingVenue::GateFutures)
}

fn direct_incremental_replacement_enabled(venue: TradingVenue) -> bool {
    is_okex_venue(venue)
        || is_bitget_venue(venue)
        || is_bybit_venue(venue)
        || is_binance_venue(venue)
        || matches!(venue, TradingVenue::GateMargin | TradingVenue::GateFutures)
}

fn direct_derivatives_replacement_enabled(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::OkexFutures
            | TradingVenue::BinanceFutures
            | TradingVenue::BitgetFutures
            | TradingVenue::GateFutures
            | TradingVenue::BybitFutures
    )
}

fn binance_futures_mm_ws_local_ip_override(venue: TradingVenue) -> Option<String> {
    if venue != TradingVenue::BinanceFutures || !binance_futures_mm_ws_enabled() {
        return None;
    }

    let whitelist_ip = env::var(ENV_BINANCE_FUTURES_MM_WS_LOCAL_IP)
        .ok()
        .as_deref()
        .map(str::trim)
        .filter(|ip| !ip.is_empty())
        .unwrap_or_else(|| {
            panic!(
                "spread_pbs: {}=on requires {}",
                ENV_BINANCE_FUTURES_MM_WS_MODE, ENV_BINANCE_FUTURES_MM_WS_LOCAL_IP
            )
        })
        .to_string();
    log::info!(
        "spread_pbs[binance-futures] {}=on; using {}={} for whitelist leg",
        ENV_BINANCE_FUTURES_MM_WS_MODE,
        ENV_BINANCE_FUTURES_MM_WS_LOCAL_IP,
        whitelist_ip
    );
    Some(whitelist_ip)
}

fn binance_futures_mm_ws_race_enabled(venue: TradingVenue) -> bool {
    venue == TradingVenue::BinanceFutures && binance_futures_mm_ws_enabled()
}

fn env_enabled_or(name: &str, default: bool) -> bool {
    match env::var(name) {
        Ok(raw) => parse_env_bool(&raw).unwrap_or_else(|| {
            log::warn!(
                "spread_pbs ignoring invalid boolean env {}={:?}; using default={}",
                name,
                raw,
                default
            );
            default
        }),
        Err(_) => default,
    }
}

fn parse_env_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "t" | "yes" | "y" | "on" | "enable" | "enabled" => Some(true),
        "0" | "false" | "f" | "no" | "n" | "off" | "disable" | "disabled" => Some(false),
        _ => None,
    }
}

fn apply_symbol_filter(mut symbols: Vec<String>, venue_slug: &str) -> Vec<String> {
    let Ok(raw) = env::var(ENV_SYMBOLS) else {
        return symbols;
    };
    let wanted: HashSet<String> = raw
        .split(',')
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    if wanted.is_empty() {
        log::warn!(
            "spread_pbs[{}] ignoring empty {}={:?}; keeping {} symbols",
            venue_slug,
            ENV_SYMBOLS,
            raw,
            symbols.len()
        );
        return symbols;
    }
    let before = symbols.len();
    symbols.retain(|symbol| wanted.contains(&symbol.to_ascii_uppercase()));
    log::info!(
        "spread_pbs[{}] {} filter applied: requested={} before={} after={} raw={:?}",
        venue_slug,
        ENV_SYMBOLS,
        wanted.len(),
        before,
        symbols.len(),
        raw
    );
    symbols
}

fn build_market_subscribe(
    adapter: &Rc<dyn VenueAdapter>,
    symbols: &[String],
    include_trade: bool,
    include_incremental: bool,
    include_derivatives: bool,
) -> Vec<serde_json::Value> {
    let mut out = adapter.build_subscribe(symbols);
    if include_trade && adapter.trade_ws_url().is_none() {
        out.extend(adapter.build_trade_subscribe(symbols));
    }
    if include_incremental && adapter.incremental_ws_url().is_none() {
        out.extend(adapter.build_incremental_subscribe(symbols));
    }
    if include_derivatives && adapter.derivatives_ws_url().is_none() {
        out.extend(adapter.build_derivatives_subscribe(symbols));
    }
    out
}

/// 一条 ws 连接的运行态：shutdown 通道 + JoinHandle，外加重启用得上的 local_ip。
struct WsLeg {
    label: &'static str,
    local_ip: String,
    url: String,
    source: MarketSource,
    shutdown_tx: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

/// 跨 leg 共享的上下文：adapter / publisher / state / ws url 在 spread_pbs 整个生命周期不变。
struct LegCtx {
    adapter: Rc<dyn VenueAdapter>,
    publisher: Rc<SpreadPublisher>,
    trade_publisher: Option<Rc<SpreadTradePublisher>>,
    incremental_publisher: Option<Rc<SpreadIncrementalPublisher>>,
    derivatives_publisher: Option<Rc<SpreadDerivativesPublisher>>,
    incremental_max_levels: Option<usize>,
    state: Rc<RefCell<SharedState>>,
    url: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MarketSource {
    Whitelist,
    Normal,
    Other,
}

impl MarketSource {
    fn label(self) -> &'static str {
        match self {
            MarketSource::Whitelist => "whitelist",
            MarketSource::Normal => "normal",
            MarketSource::Other => "other",
        }
    }
}

impl SpreadPbsApp {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub async fn run(self) -> Result<()> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let ctrl_c_task = tokio::spawn(async move {
            if let Err(e) = tokio::signal::ctrl_c().await {
                log::warn!("spread_pbs ctrl-c listener failed: {:#}", e);
            }
            let _ = shutdown_tx.send(true);
        });
        let result = self.run_with_shutdown(shutdown_rx).await;
        ctrl_c_task.abort();
        result
    }

    /// 主入口：拉 symbol → 起双路 ws → 帧在 ws task 内同步处理（无 mpsc）。
    /// 与 dat_pbs 对齐：两条 ws 按错开半周期定时重启，重启前重新拉 symbol。
    ///
    /// 必须在 `LocalSet` 上下文里 await（`main` 用 `LocalSet::run_until`）。
    pub async fn run_with_shutdown(self, mut shutdown_rx: watch::Receiver<bool>) -> Result<()> {
        let venue = self.config.venue;
        let venue_slug: &'static str = venue.data_pub_slug();

        let adapter = match create_adapter(venue).await? {
            Some(a) => Rc::<dyn VenueAdapter>::from(a),
            None => bail!(
                "spread_pbs 当前不支持 venue {:?}（仅 OKex/Binance/Bybit/Gate/Bitget × spot+futures）",
                venue
            ),
        };
        log::info!(
            "spread_pbs starting venue={} adapter={}",
            venue_slug,
            adapter.name()
        );
        let local_ip_override = binance_futures_mm_ws_local_ip_override(venue);
        let primary_local_ip = local_ip_override
            .clone()
            .unwrap_or_else(|| self.config.primary_local_ip.clone());
        let secondary_local_ip = if binance_futures_mm_ws_race_enabled(venue) {
            self.config.secondary_local_ip.clone()
        } else {
            local_ip_override
                .clone()
                .unwrap_or_else(|| self.config.secondary_local_ip.clone())
        };

        // ---- 首次拉 symbol（含 BinanceFutures，无硬编码） ----
        // spread_pbs 不归 pm2 管，启动期 REST 抖动不能直接退出；用退避循环等到拿到非空列表
        let initial_symbols = apply_symbol_filter(self.config.wait_for_symbols().await, venue_slug);
        if initial_symbols.is_empty() {
            bail!(
                "spread_pbs[{}] no symbols left after {} filter",
                venue_slug,
                ENV_SYMBOLS
            );
        }
        adapter.seed_symbols(&initial_symbols);
        let mut current_symbols: HashSet<String> = initial_symbols.iter().cloned().collect();
        let enable_trade = env_enabled_or(ENV_ENABLE_TRADE, self.config.data_types.enable_trade);
        let enable_incremental = env_enabled_or(
            ENV_ENABLE_INCREMENTAL,
            self.config.data_types.enable_incremental,
        );
        let enable_derivatives = env_enabled_or(
            ENV_ENABLE_DERIVATIVES,
            self.config.data_types.enable_derivatives,
        );
        let direct_trade_enabled = enable_trade && direct_trade_replacement_enabled(venue);
        let direct_incremental_enabled = enable_incremental
            && direct_incremental_replacement_enabled(venue)
            && !adapter
                .build_incremental_subscribe(&initial_symbols)
                .is_empty();
        let direct_derivatives_enabled = enable_derivatives
            && direct_derivatives_replacement_enabled(venue)
            && (is_okex_derivatives_venue(venue)
                || !adapter
                    .build_derivatives_subscribe(&initial_symbols)
                    .is_empty());
        log::info!(
            "spread_pbs[{}] data_types askbid=true trade={} incremental={} derivatives={} env_overrides {}={:?} {}={:?} {}={:?}",
            venue_slug,
            direct_trade_enabled,
            direct_incremental_enabled,
            direct_derivatives_enabled,
            ENV_ENABLE_TRADE,
            env::var(ENV_ENABLE_TRADE).ok(),
            ENV_ENABLE_INCREMENTAL,
            env::var(ENV_ENABLE_INCREMENTAL).ok(),
            ENV_ENABLE_DERIVATIVES,
            env::var(ENV_ENABLE_DERIVATIVES).ok(),
        );
        let initial_subs = build_market_subscribe(
            &adapter,
            &initial_symbols,
            direct_trade_enabled,
            direct_incremental_enabled,
            direct_derivatives_enabled,
        );
        if initial_subs.is_empty() {
            bail!(
                "adapter.build_subscribe 返回空（{} symbols 数={}）",
                venue_slug,
                initial_symbols.len()
            );
        }
        log::info!(
            "spread_pbs[{}] initial symbols={} subscribe_batches={}",
            venue_slug,
            initial_symbols.len(),
            initial_subs.len()
        );

        // ---- IceOryx publisher + 共享态（Rc<RefCell> 单线程零锁，跨重启复用）----
        let publisher = Rc::new(
            SpreadPublisher::new(venue_slug)
                .with_context(|| format!("create iceoryx publisher for {}", venue_slug))?,
        );
        let trade_publisher = if direct_trade_enabled {
            Some(Rc::new(
                SpreadTradePublisher::new_open_or_create(venue_slug).unwrap_or_else(|e| {
                    panic!(
                        "spread_pbs[{}] failed to open/create replacement trade ipc channel dat_pbs/{}/trade: {:#}",
                        venue_slug, venue_slug, e
                    )
                }),
            ))
        } else {
            None
        };
        let incremental_publisher = if direct_incremental_enabled {
            Some(Rc::new(
                SpreadIncrementalPublisher::new_open_or_create(venue_slug).unwrap_or_else(|e| {
                    panic!(
                        "spread_pbs[{}] failed to open/create replacement incremental ipc channel dat_pbs/{}/incremental: {:#}",
                        venue_slug, venue_slug, e
                    )
                }),
            ))
        } else {
            None
        };
        let derivatives_publisher = if direct_derivatives_enabled {
            Some(Rc::new(
                SpreadDerivativesPublisher::new_open_or_create(venue_slug).unwrap_or_else(|e| {
                    panic!(
                        "spread_pbs[{}] failed to open/create replacement derivatives ipc channel dat_pbs/{}/derivatives: {:#}",
                        venue_slug, venue_slug, e
                    )
                }),
            ))
        } else {
            None
        };
        let latency_publisher = Rc::new(
            SpreadLatencyPublisher::new(venue_slug)
                .with_context(|| format!("create iceoryx latency publisher for {}", venue_slug))?,
        );
        let ipc_label = format!("{}-ipc", venue_slug);
        let state: Rc<RefCell<SharedState>> = Rc::new(RefCell::new(SharedState {
            symbol_state: SymbolSeqState::with_symbols(&initial_symbols),
            latency_e2e: LatencyKll::new(venue_slug),
            latency_ipc: LatencyKll::new(ipc_label),
            published: 0,
            trades_published: 0,
            incremental_published: 0,
            incremental_dropped_by_seq: 0,
            incremental_gap_warnings: 0,
            derivatives_published: 0,
            selected_whitelist: 0,
            selected_normal: 0,
            selected_other: 0,
            dropped_by_seq: 0,
            trades_dropped_by_seq: 0,
            last_dedup_reset_us: get_timestamp_us(),
        }));

        let main_trade_publisher = if adapter.trade_ws_url().is_none() {
            trade_publisher.clone()
        } else {
            None
        };
        let main_incremental_publisher = if adapter.incremental_ws_url().is_none() {
            incremental_publisher.clone()
        } else {
            None
        };
        let main_derivatives_publisher =
            if !is_okex_derivatives_venue(venue) && adapter.derivatives_ws_url().is_none() {
                derivatives_publisher.clone()
            } else {
                None
            };

        let ctx = LegCtx {
            adapter: adapter.clone(),
            publisher: publisher.clone(),
            trade_publisher: main_trade_publisher,
            incremental_publisher: main_incremental_publisher,
            derivatives_publisher: main_derivatives_publisher,
            incremental_max_levels: self.config.data_types.max_levels_per_msg,
            state: state.clone(),
            url: adapter.ws_url(),
        };
        let primary_url = ctx.url.clone();
        let secondary_url = if binance_futures_mm_ws_race_enabled(venue) {
            binance_futures_standard_ws_url().to_string()
        } else {
            ctx.url.clone()
        };
        let primary_source = if binance_futures_mm_ws_race_enabled(venue) {
            MarketSource::Whitelist
        } else {
            MarketSource::Other
        };
        let secondary_source = if binance_futures_mm_ws_race_enabled(venue) {
            MarketSource::Normal
        } else {
            MarketSource::Other
        };
        if binance_futures_mm_ws_race_enabled(venue) {
            log::info!(
                "spread_pbs[binance-futures] BBO source race enabled: primary {} ip={} url={} secondary {} ip={} url={}",
                primary_source.label(),
                primary_local_ip,
                primary_url,
                secondary_source.label(),
                secondary_local_ip,
                secondary_url,
            );
        }

        let derivatives_symbols: Rc<RefCell<HashSet<String>>> =
            Rc::new(RefCell::new(initial_symbols.iter().cloned().collect()));
        let derivatives_leg = derivatives_publisher.as_ref().map(|publisher| {
            if is_okex_derivatives_venue(venue) {
                Some(spawn_okex_derivatives_leg(
                    primary_local_ip.clone(),
                    build_okex_derivatives_subscribe_msgs(
                        &initial_symbols,
                        self.config.get_batch_size(),
                    ),
                    publisher.clone(),
                    derivatives_symbols.clone(),
                    state.clone(),
                ))
            } else {
                None
            }
        });
        let mut derivatives_leg = derivatives_leg.flatten();
        let mut direct_extra_legs = spawn_direct_extra_legs(
            &adapter,
            &initial_symbols,
            &self.config,
            &primary_local_ip,
            trade_publisher.clone(),
            incremental_publisher.clone(),
            derivatives_publisher.clone(),
            state.clone(),
        );

        // ---- 起两条 leg：primary / secondary，独立 shutdown 通道 ----
        let mut primary = spawn_leg(
            "primary",
            primary_local_ip,
            primary_url,
            primary_source,
            initial_subs.clone(),
            &ctx,
        );
        let mut secondary = spawn_leg(
            "secondary",
            secondary_local_ip,
            secondary_url,
            secondary_source,
            initial_subs,
            &ctx,
        );

        // ---- 错开半周期：primary t+T，secondary t+T/2，与 src/mkt_pub/app.rs 一致 ----
        let restart_duration = Duration::from_secs(self.config.restart_duration_secs);
        let mut next_primary_restart = Instant::now() + restart_duration;
        let mut next_secondary_restart = Instant::now() + restart_duration / 2;
        log::info!(
            "spread_pbs[{}] restart period={}s; first primary at +{}s, secondary at +{}s",
            venue_slug,
            self.config.restart_duration_secs,
            self.config.restart_duration_secs,
            self.config.restart_duration_secs / 2,
        );

        let mut stats_ticker = tokio::time::interval(Duration::from_secs(30));
        stats_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        stats_ticker.tick().await;
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        log::info!("spread_pbs[{}] shutdown requested", venue_slug);
                    } else {
                        continue;
                    }
                    let _ = primary.shutdown_tx.send(true);
                    let _ = secondary.shutdown_tx.send(true);
                    if let Some(leg) = derivatives_leg.as_mut() {
                        let _ = leg.shutdown_tx.send(true);
                    }
                    for leg in &mut direct_extra_legs {
                        let _ = leg.shutdown_tx.send(true);
                    }
                    let _ = (&mut primary.handle).await;
                    let _ = (&mut secondary.handle).await;
                    if let Some(leg) = derivatives_leg.as_mut() {
                        let _ = (&mut leg.handle).await;
                    }
                    for leg in &mut direct_extra_legs {
                        let _ = (&mut leg.handle).await;
                    }
                    break;
                }
                _ = stats_ticker.tick() => {
                    let mut s = state.borrow_mut();
                    if let Some(msg) = take_latency_snapshot(&mut s, venue.to_u8() as u32) {
                        if let Err(e) = latency_publisher.publish(msg.into_bytes()) {
                            log::warn!("spread_pbs[{}] latency snapshot publish failed: {:#}", venue_slug, e);
                        }
                    }
                    log::info!(
                        "spread_pbs[{}] stats published={} trades_published={} incremental_published={} derivatives_published={} dropped_by_seq={} trades_dropped_by_seq={} incremental_dropped_by_seq={} incremental_gap_warnings={} symbols_seen={} trade_symbols_seen={} incremental_symbols_seen={}",
                        venue_slug,
                        s.published,
                        s.trades_published,
                        s.incremental_published,
                        s.derivatives_published,
                        s.dropped_by_seq,
                        s.trades_dropped_by_seq,
                        s.incremental_dropped_by_seq,
                        s.incremental_gap_warnings,
                        s.symbol_state.bbo_seen(),
                        s.symbol_state.trade_seen(),
                        s.symbol_state.incremental_seen()
                    );
                    s.log_and_reset_selected_source_stats(venue_slug);
                }
                _ = tokio::time::sleep_until(next_primary_restart) => {
                    restart_leg(
                        venue_slug,
                        &mut primary,
                        &self.config,
                        &ctx,
                        &mut current_symbols,
                    ).await;
                    next_primary_restart = Instant::now() + restart_duration;
                }
                _ = tokio::time::sleep_until(next_secondary_restart) => {
                    restart_leg(
                        venue_slug,
                        &mut secondary,
                        &self.config,
                        &ctx,
                        &mut current_symbols,
                    ).await;
                    next_secondary_restart = Instant::now() + restart_duration;
                }
            }
        }

        Ok(())
    }
}

fn spawn_leg(
    label: &'static str,
    local_ip: String,
    url: String,
    source: MarketSource,
    subs: Vec<serde_json::Value>,
    ctx: &LegCtx,
) -> WsLeg {
    let (tx, rx) = watch::channel(false);
    let handler = make_handler(
        label,
        ctx.adapter.clone(),
        ctx.publisher.clone(),
        ctx.trade_publisher.clone(),
        ctx.incremental_publisher.clone(),
        ctx.derivatives_publisher.clone(),
        ctx.incremental_max_levels,
        ctx.state.clone(),
        source,
    );
    let handle = tokio::task::spawn_local(run_public_ws(
        WsLoopParams {
            label,
            url: url.clone(),
            local_ip: local_ip.clone(),
            headers: ctx.adapter.ws_headers(),
            subscribe_msgs: subs,
            keepalive: ctx.adapter.keepalive(),
        },
        handler,
        rx,
    ));
    WsLeg {
        label,
        local_ip,
        url,
        source,
        shutdown_tx: tx,
        handle,
    }
}

fn spawn_okex_derivatives_leg(
    local_ip: String,
    subscribe_msgs: Vec<serde_json::Value>,
    publisher: Rc<SpreadDerivativesPublisher>,
    active_symbols: Rc<RefCell<HashSet<String>>>,
    state: Rc<RefCell<SharedState>>,
) -> WsLeg {
    let (tx, rx) = watch::channel(false);
    let handler = make_okex_derivatives_handler(publisher, active_symbols, state);
    let handle = tokio::task::spawn_local(run_public_ws(
        WsLoopParams {
            label: "okex-derivatives",
            url: OKEX_PUBLIC_WS_URL.to_string(),
            local_ip: local_ip.clone(),
            headers: Vec::new(),
            subscribe_msgs,
            keepalive: Some(KeepaliveSpec::text(Duration::from_secs(25), "ping")),
        },
        handler,
        rx,
    ));
    WsLeg {
        label: "okex-derivatives",
        local_ip,
        url: OKEX_PUBLIC_WS_URL.to_string(),
        source: MarketSource::Other,
        shutdown_tx: tx,
        handle,
    }
}

fn spawn_direct_extra_legs(
    adapter: &Rc<dyn VenueAdapter>,
    symbols: &[String],
    config: &Config,
    local_ip: &str,
    trade_publisher: Option<Rc<SpreadTradePublisher>>,
    incremental_publisher: Option<Rc<SpreadIncrementalPublisher>>,
    derivatives_publisher: Option<Rc<SpreadDerivativesPublisher>>,
    state: Rc<RefCell<SharedState>>,
) -> Vec<WsLeg> {
    let mut legs = Vec::new();

    if let (Some(url), Some(publisher)) = (adapter.trade_ws_url(), trade_publisher.clone()) {
        spawn_direct_replacement_batch_legs(
            &mut legs,
            "direct-trade",
            url,
            local_ip.to_string(),
            adapter.build_trade_subscribe(symbols),
            adapter,
            Some(publisher),
            None,
            None,
            config.data_types.max_levels_per_msg,
            state.clone(),
        );
    }
    if let (Some(url), Some(publisher)) =
        (adapter.incremental_ws_url(), incremental_publisher.clone())
    {
        spawn_direct_replacement_batch_legs(
            &mut legs,
            "direct-incremental",
            url,
            local_ip.to_string(),
            adapter.build_incremental_subscribe(symbols),
            adapter,
            None,
            Some(publisher),
            None,
            config.data_types.max_levels_per_msg,
            state.clone(),
        );
    }
    if let (Some(url), Some(publisher)) =
        (adapter.derivatives_ws_url(), derivatives_publisher.clone())
    {
        spawn_direct_replacement_batch_legs(
            &mut legs,
            "direct-derivatives",
            url,
            local_ip.to_string(),
            adapter.build_derivatives_subscribe(symbols),
            adapter,
            None,
            None,
            Some(publisher),
            config.data_types.max_levels_per_msg,
            state.clone(),
        );
    }

    legs
}

fn spawn_direct_replacement_batch_legs(
    legs: &mut Vec<WsLeg>,
    label: &'static str,
    url: String,
    local_ip: String,
    subscribe_msgs: Vec<serde_json::Value>,
    adapter: &Rc<dyn VenueAdapter>,
    trade_publisher: Option<Rc<SpreadTradePublisher>>,
    incremental_publisher: Option<Rc<SpreadIncrementalPublisher>>,
    derivatives_publisher: Option<Rc<SpreadDerivativesPublisher>>,
    incremental_max_levels: Option<usize>,
    state: Rc<RefCell<SharedState>>,
) {
    if subscribe_msgs.is_empty() {
        return;
    }

    log::info!(
        "spread_pbs {} spawning {} direct replacement ws batches url={}",
        label,
        subscribe_msgs.len(),
        url
    );
    for subscribe_msg in subscribe_msgs {
        legs.push(spawn_direct_replacement_leg(
            label,
            url.clone(),
            local_ip.clone(),
            vec![subscribe_msg],
            adapter.clone(),
            trade_publisher.clone(),
            incremental_publisher.clone(),
            derivatives_publisher.clone(),
            incremental_max_levels,
            state.clone(),
        ));
    }
}

fn spawn_direct_replacement_leg(
    label: &'static str,
    url: String,
    local_ip: String,
    subscribe_msgs: Vec<serde_json::Value>,
    adapter: Rc<dyn VenueAdapter>,
    trade_publisher: Option<Rc<SpreadTradePublisher>>,
    incremental_publisher: Option<Rc<SpreadIncrementalPublisher>>,
    derivatives_publisher: Option<Rc<SpreadDerivativesPublisher>>,
    incremental_max_levels: Option<usize>,
    state: Rc<RefCell<SharedState>>,
) -> WsLeg {
    let (tx, rx) = watch::channel(false);
    let handler = make_replacement_handler(
        label,
        adapter.clone(),
        trade_publisher,
        incremental_publisher,
        derivatives_publisher,
        incremental_max_levels,
        state,
    );
    let handle = tokio::task::spawn_local(run_public_ws(
        WsLoopParams {
            label,
            url: url.clone(),
            local_ip: local_ip.clone(),
            headers: adapter.ws_headers(),
            subscribe_msgs,
            keepalive: adapter.keepalive(),
        },
        handler,
        rx,
    ));
    WsLeg {
        label,
        local_ip,
        url,
        source: MarketSource::Other,
        shutdown_tx: tx,
        handle,
    }
}

fn make_okex_derivatives_handler(
    publisher: Rc<SpreadDerivativesPublisher>,
    active_symbols: Rc<RefCell<HashSet<String>>>,
    state: Rc<RefCell<SharedState>>,
) -> FrameHandler {
    Rc::new(move |_recv_us: i64, raw: &[u8]| {
        let value = match serde_json::from_slice::<serde_json::Value>(raw) {
            Ok(value) => value,
            Err(_) => return,
        };
        let bytes = {
            let symbols = active_symbols.borrow();
            parse_okex_derivatives_frame(&value, &symbols)
        };
        if bytes.is_empty() {
            return;
        }
        let mut s = state.borrow_mut();
        for msg in bytes {
            if let Err(e) = publisher.publish(&msg) {
                log::warn!("spread_pbs derivatives publish failed: {:#}", e);
                continue;
            }
            s.derivatives_published += 1;
        }
    })
}

async fn restart_leg(
    venue_slug: &'static str,
    leg: &mut WsLeg,
    config: &Config,
    ctx: &LegCtx,
    current_symbols: &mut HashSet<String>,
) {
    log::info!("spread_pbs[{}] leg={} restart begin", venue_slug, leg.label);

    // 先拉新 symbol；失败/空一律保留旧 leg，不重启。
    let new_symbols = match config.get_symbols().await {
        Ok(v) if !v.is_empty() => apply_symbol_filter(v, venue_slug),
        Ok(_) => {
            log::error!(
                "spread_pbs[{}] leg={} restart skipped: get_symbols() returned empty",
                venue_slug,
                leg.label
            );
            return;
        }
        Err(e) => {
            log::error!(
                "spread_pbs[{}] leg={} restart skipped: get_symbols() failed: {:#}",
                venue_slug,
                leg.label,
                e
            );
            return;
        }
    };
    if new_symbols.is_empty() {
        log::error!(
            "spread_pbs[{}] leg={} restart skipped: no symbols left after {} filter",
            venue_slug,
            leg.label,
            ENV_SYMBOLS
        );
        return;
    }
    let new_subs = build_market_subscribe(
        &ctx.adapter,
        &new_symbols,
        ctx.trade_publisher.is_some(),
        ctx.incremental_publisher.is_some(),
        ctx.derivatives_publisher.is_some(),
    );
    if new_subs.is_empty() {
        log::error!(
            "spread_pbs[{}] leg={} restart skipped: adapter.build_subscribe empty (symbols={})",
            venue_slug,
            leg.label,
            new_symbols.len()
        );
        return;
    }
    let new_subs_len = new_subs.len();

    let new_set: HashSet<String> = new_symbols.iter().cloned().collect();
    let added = new_set.difference(current_symbols).count();
    let removed = current_symbols.difference(&new_set).count();
    if added > 0 {
        let mut s = ctx.state.borrow_mut();
        s.symbol_state.ensure_symbols(&new_symbols);
        ctx.adapter.seed_symbols(&new_symbols);
    }
    *current_symbols = new_set;

    // 关旧、等真正退出，再起新。错开半周期保证此刻另一条 leg 仍在工作。
    let _ = leg.shutdown_tx.send(true);
    let _ = (&mut leg.handle).await;

    let new_leg = spawn_leg(
        leg.label,
        leg.local_ip.clone(),
        leg.url.clone(),
        leg.source,
        new_subs,
        ctx,
    );
    leg.shutdown_tx = new_leg.shutdown_tx;
    leg.handle = new_leg.handle;

    log::info!(
        "spread_pbs[{}] leg={} restart done symbols={} added={} removed={} subscribe_batches={}",
        venue_slug,
        leg.label,
        new_symbols.len(),
        added,
        removed,
        new_subs_len,
    );
}

struct SymbolSeqState {
    index_by_symbol: HashMap<String, usize>,
    bbo_seq: Vec<i64>,
    bbo_ts_us: Vec<i64>,
    latency_measurement_symbol: Vec<bool>,
    trade_seq: Vec<i64>,
    incremental_seq: Vec<i64>,
    bbo_seen: usize,
    trade_seen: usize,
    incremental_seen: usize,
}

impl SymbolSeqState {
    fn with_symbols(symbols: &[String]) -> Self {
        let mut state = Self {
            index_by_symbol: HashMap::with_capacity(symbols.len().max(2048)),
            bbo_seq: Vec::with_capacity(symbols.len()),
            bbo_ts_us: Vec::with_capacity(symbols.len()),
            latency_measurement_symbol: Vec::with_capacity(symbols.len()),
            trade_seq: Vec::with_capacity(symbols.len()),
            incremental_seq: Vec::with_capacity(symbols.len()),
            bbo_seen: 0,
            trade_seen: 0,
            incremental_seen: 0,
        };
        state.ensure_symbols(symbols);
        state
    }

    fn ensure_symbols(&mut self, symbols: &[String]) {
        for symbol in symbols {
            self.ensure_symbol(symbol);
        }
    }

    fn ensure_symbol(&mut self, symbol: &str) -> usize {
        if let Some(&idx) = self.index_by_symbol.get(symbol) {
            return idx;
        }
        let idx = self.bbo_seq.len();
        self.index_by_symbol.insert(symbol.to_string(), idx);
        self.bbo_seq.push(i64::MIN);
        self.bbo_ts_us.push(0);
        self.latency_measurement_symbol
            .push(is_latency_measurement_symbol(symbol));
        self.trade_seq.push(i64::MIN);
        self.incremental_seq.push(i64::MIN);
        idx
    }

    fn bbo_seen(&self) -> usize {
        self.bbo_seen
    }

    fn trade_seen(&self) -> usize {
        self.trade_seen
    }

    fn incremental_seen(&self) -> usize {
        self.incremental_seen
    }

    #[cfg(test)]
    fn bbo_prev(&self, symbol: &str) -> i64 {
        self.index_by_symbol
            .get(symbol)
            .map(|&idx| self.bbo_seq[idx])
            .unwrap_or(i64::MIN)
    }

    fn bbo_slot(&mut self, symbol: &str) -> SymbolSlot {
        let idx = self.ensure_symbol(symbol);
        SymbolSlot {
            idx,
            prev: self.bbo_seq[idx],
            prev_ts_us: self.bbo_ts_us[idx],
            latency_measurement_symbol: self.latency_measurement_symbol[idx],
        }
    }

    fn set_bbo_slot(&mut self, slot: SymbolSlot, seq_id: i64, ts_us: i64) {
        if slot.prev == i64::MIN {
            self.bbo_seen += 1;
        }
        self.bbo_seq[slot.idx] = seq_id;
        if ts_us > 0 {
            self.bbo_ts_us[slot.idx] = ts_us;
        }
    }

    fn trade_slot(&mut self, symbol: &str) -> SymbolSlot {
        let idx = self.ensure_symbol(symbol);
        SymbolSlot {
            idx,
            prev: self.trade_seq[idx],
            prev_ts_us: 0,
            latency_measurement_symbol: false,
        }
    }

    fn set_trade_slot(&mut self, slot: SymbolSlot, seq_id: i64) {
        if slot.prev == i64::MIN {
            self.trade_seen += 1;
        }
        self.trade_seq[slot.idx] = seq_id;
    }

    fn incremental_slot(&mut self, symbol: &str) -> SymbolSlot {
        let idx = self.ensure_symbol(symbol);
        SymbolSlot {
            idx,
            prev: self.incremental_seq[idx],
            prev_ts_us: 0,
            latency_measurement_symbol: false,
        }
    }

    fn incremental_prev_seen(&mut self, symbol: &str) -> Option<i64> {
        let slot = self.incremental_slot(symbol);
        (slot.prev != i64::MIN).then_some(slot.prev)
    }

    fn set_incremental_slot(&mut self, slot: SymbolSlot, seq_id: i64) {
        if slot.prev == i64::MIN {
            self.incremental_seen += 1;
        }
        self.incremental_seq[slot.idx] = seq_id;
    }

    fn clear_bbo(&mut self) -> usize {
        let cleared = self.bbo_seen;
        self.bbo_seq.fill(i64::MIN);
        self.bbo_ts_us.fill(0);
        self.bbo_seen = 0;
        cleared
    }
}

#[derive(Clone, Copy)]
struct SymbolSlot {
    idx: usize,
    prev: i64,
    prev_ts_us: i64,
    latency_measurement_symbol: bool,
}

struct SharedState {
    symbol_state: SymbolSeqState,
    /// 被采纳消息：`accepted_us - event_ts_us`（u 最新判断通过后立刻采样）。
    latency_e2e: LatencyKll,
    /// IPC latency snapshot buckets. Kept separate so periodic IPC snapshots do not reset log KLLs.
    latency_ipc: LatencyKll,
    published: u64,
    trades_published: u64,
    incremental_published: u64,
    incremental_dropped_by_seq: u64,
    incremental_gap_warnings: u64,
    derivatives_published: u64,
    selected_whitelist: u64,
    selected_normal: u64,
    selected_other: u64,
    dropped_by_seq: u64,
    trades_dropped_by_seq: u64,
    last_dedup_reset_us: i64,
}

impl SharedState {
    fn record_selected_source(&mut self, source: MarketSource) {
        match source {
            MarketSource::Whitelist => self.selected_whitelist += 1,
            MarketSource::Normal => self.selected_normal += 1,
            MarketSource::Other => self.selected_other += 1,
        }
    }

    fn log_and_reset_selected_source_stats(&mut self, venue_slug: &str) {
        let total = self.selected_whitelist + self.selected_normal;
        if total == 0 {
            return;
        }
        let whitelist_pct = self.selected_whitelist as f64 * 100.0 / total as f64;
        let normal_pct = self.selected_normal as f64 * 100.0 / total as f64;
        log::info!(
            "spread_pbs[{}] selected_source whitelist={} normal={} whitelist_pct={:.2} normal_pct={:.2}",
            venue_slug,
            self.selected_whitelist,
            self.selected_normal,
            whitelist_pct,
            normal_pct,
        );
        self.selected_whitelist = 0;
        self.selected_normal = 0;
        self.selected_other = 0;
    }
}

#[derive(Default)]
struct ReplacementBatch {
    trades: Vec<TradeFrame>,
    incrementals: Vec<IncrementalFrame>,
    derivatives: Vec<bytes::Bytes>,
}

impl ReplacementBatch {
    fn is_empty(&self) -> bool {
        self.trades.is_empty() && self.incrementals.is_empty() && self.derivatives.is_empty()
    }
}

fn parse_replacement_batch(
    label: &'static str,
    adapter: &dyn VenueAdapter,
    raw: &[u8],
    include_bbo: bool,
    include_trade: bool,
    include_incremental: bool,
    include_derivatives: bool,
    emit_bbo: &mut dyn FnMut(BboFrame) -> Result<()>,
) -> ReplacementBatch {
    if looks_like_json(raw) {
        if include_bbo {
            match adapter.parse_bbo_raw(raw, emit_bbo) {
                Ok(true) => return ReplacementBatch::default(),
                Ok(false) => {}
                Err(e) => {
                    log::error!(
                        "spread_pbs[{}] adapter.parse_bbo_raw failed: {:#}",
                        label,
                        e
                    );
                }
            }
        }
        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(raw) {
            return parse_json_replacement_batch(
                label,
                adapter,
                &value,
                include_bbo,
                include_trade,
                include_incremental,
                include_derivatives,
                emit_bbo,
            );
        }
    }
    parse_binary_replacement_batch(
        label,
        adapter,
        raw,
        include_bbo,
        include_trade,
        include_incremental,
        include_derivatives,
        emit_bbo,
    )
}

fn looks_like_json(raw: &[u8]) -> bool {
    matches!(
        raw.iter().copied().find(|b| !b.is_ascii_whitespace()),
        Some(b'{' | b'[')
    )
}

fn parse_json_replacement_batch(
    label: &'static str,
    adapter: &dyn VenueAdapter,
    value: &serde_json::Value,
    include_bbo: bool,
    include_trade: bool,
    include_incremental: bool,
    include_derivatives: bool,
    emit_bbo: &mut dyn FnMut(BboFrame) -> Result<()>,
) -> ReplacementBatch {
    if include_bbo {
        match adapter.parse_frame(value, emit_bbo) {
            Ok(()) => {}
            Err(e) => {
                log::error!(
                    "spread_pbs[{}] adapter.parse_frame failed: {:#} payload={}",
                    label,
                    e,
                    value
                );
            }
        }
    }
    let trades = if include_trade {
        match adapter.parse_trade_frame(value) {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "spread_pbs[{}] adapter.parse_trade_frame failed: {:#} payload={}",
                    label,
                    e,
                    value
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };
    let incrementals = if include_incremental {
        match adapter.parse_incremental_frame(value) {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "spread_pbs[{}] adapter.parse_incremental_frame failed: {:#} payload={}",
                    label,
                    e,
                    value
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };
    let derivatives = if include_derivatives {
        match adapter.parse_derivatives_frame(value) {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "spread_pbs[{}] adapter.parse_derivatives_frame failed: {:#} payload={}",
                    label,
                    e,
                    value
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    ReplacementBatch {
        trades,
        incrementals,
        derivatives,
    }
}

fn parse_binary_replacement_batch(
    label: &'static str,
    adapter: &dyn VenueAdapter,
    raw: &[u8],
    include_bbo: bool,
    include_trade: bool,
    include_incremental: bool,
    include_derivatives: bool,
    emit_bbo: &mut dyn FnMut(BboFrame) -> Result<()>,
) -> ReplacementBatch {
    if include_bbo {
        match adapter.parse_binary_frame(raw, emit_bbo) {
            Ok(()) => {}
            Err(e) => {
                log::error!(
                    "spread_pbs[{}] adapter.parse_binary_frame failed: {:#}",
                    label,
                    e
                );
            }
        }
    }
    let trades = if include_trade {
        match adapter.parse_trade_binary_frame(raw) {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "spread_pbs[{}] adapter.parse_trade_binary_frame failed: {:#}",
                    label,
                    e
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };
    let incrementals = if include_incremental {
        match adapter.parse_incremental_binary_frame(raw) {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "spread_pbs[{}] adapter.parse_incremental_binary_frame failed: {:#}",
                    label,
                    e
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };
    let derivatives = if include_derivatives {
        match adapter.parse_derivatives_binary_frame(raw) {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "spread_pbs[{}] adapter.parse_derivatives_binary_frame failed: {:#}",
                    label,
                    e
                );
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    ReplacementBatch {
        trades,
        incrementals,
        derivatives,
    }
}

fn make_replacement_handler(
    label: &'static str,
    adapter: Rc<dyn VenueAdapter>,
    trade_publisher: Option<Rc<SpreadTradePublisher>>,
    incremental_publisher: Option<Rc<SpreadIncrementalPublisher>>,
    derivatives_publisher: Option<Rc<SpreadDerivativesPublisher>>,
    incremental_max_levels: Option<usize>,
    state: Rc<RefCell<SharedState>>,
) -> FrameHandler {
    Rc::new(move |_recv_us: i64, raw: &[u8]| {
        let mut emit_noop = |_frame: BboFrame| Ok(());
        let batch = parse_replacement_batch(
            label,
            adapter.as_ref(),
            raw,
            false,
            trade_publisher.is_some(),
            incremental_publisher.is_some(),
            derivatives_publisher.is_some(),
            &mut emit_noop,
        );
        if batch.is_empty() {
            return;
        }
        let mut s = state.borrow_mut();
        if let Some(trade_publisher) = trade_publisher.as_ref() {
            for trade in batch.trades {
                process_trade_frame(&mut s, trade_publisher, trade);
            }
        }
        if let Some(incremental_publisher) = incremental_publisher.as_ref() {
            for incremental in batch.incrementals {
                process_incremental_frame(
                    &mut s,
                    incremental_publisher,
                    incremental,
                    incremental_max_levels,
                );
            }
        }
        if let Some(derivatives_publisher) = derivatives_publisher.as_ref() {
            for bytes in batch.derivatives {
                process_derivatives_bytes(&mut s, derivatives_publisher, &bytes);
            }
        }
    })
}

fn make_handler(
    label: &'static str,
    adapter: Rc<dyn VenueAdapter>,
    publisher: Rc<SpreadPublisher>,
    trade_publisher: Option<Rc<SpreadTradePublisher>>,
    incremental_publisher: Option<Rc<SpreadIncrementalPublisher>>,
    derivatives_publisher: Option<Rc<SpreadDerivativesPublisher>>,
    incremental_max_levels: Option<usize>,
    state: Rc<RefCell<SharedState>>,
    source: MarketSource,
) -> FrameHandler {
    Rc::new(move |recv_us: i64, raw: &[u8]| {
        let mut accepted_us = 0;
        let mut bbo_count = 0usize;
        let mut s = state.borrow_mut();
        let mut emit_bbo = |frame: BboFrame| {
            if accepted_us == 0 {
                accepted_us = get_timestamp_us();
            }
            bbo_count += 1;
            process_frame(&mut s, &publisher, recv_us, accepted_us, frame, source);
            Ok(())
        };
        let batch = parse_replacement_batch(
            label,
            adapter.as_ref(),
            raw,
            true,
            trade_publisher.is_some(),
            incremental_publisher.is_some(),
            derivatives_publisher.is_some(),
            &mut emit_bbo,
        );
        drop(emit_bbo);
        if bbo_count == 0 && batch.is_empty() {
            return;
        }
        if let Some(trade_publisher) = trade_publisher.as_ref() {
            for trade in batch.trades {
                process_trade_frame(&mut s, trade_publisher, trade);
            }
        }
        if let Some(incremental_publisher) = incremental_publisher.as_ref() {
            for incremental in batch.incrementals {
                process_incremental_frame(
                    &mut s,
                    incremental_publisher,
                    incremental,
                    incremental_max_levels,
                );
            }
        }
        if let Some(derivatives_publisher) = derivatives_publisher.as_ref() {
            for bytes in batch.derivatives {
                process_derivatives_bytes(&mut s, derivatives_publisher, &bytes);
            }
        }
    })
}

fn process_trade_frame(
    state: &mut SharedState,
    publisher: &Rc<SpreadTradePublisher>,
    f: TradeFrame,
) {
    let slot = state.symbol_state.trade_slot(&f.symbol);
    if f.seq_id <= slot.prev {
        state.trades_dropped_by_seq += 1;
        return;
    }
    state.symbol_state.set_trade_slot(slot, f.seq_id);

    if let Err(e) = publisher.publish_trade(
        &f.symbol,
        f.trade_id,
        f.timestamp_us,
        f.side,
        f.price,
        f.amount,
    ) {
        log::warn!("spread_pbs trade publish failed: {:#}", e);
        return;
    }
    state.trades_published += 1;
}

fn process_incremental_frame(
    state: &mut SharedState,
    publisher: &Rc<SpreadIncrementalPublisher>,
    frame: IncrementalFrame,
    max_levels: Option<usize>,
) {
    let (
        symbol,
        timestamp,
        seq_id,
        prev_seq_id,
        first_update_id,
        final_update_id,
        gap_check,
        is_snapshot,
        bids,
        asks,
    ) = match frame {
        IncrementalFrame::Book {
            symbol,
            timestamp,
            seq_id,
            prev_seq_id,
            first_update_id,
            final_update_id,
            gap_check,
            is_snapshot,
            bids,
            asks,
        } => (
            symbol,
            timestamp,
            seq_id,
            prev_seq_id,
            first_update_id,
            final_update_id,
            gap_check,
            is_snapshot,
            bids,
            asks,
        ),
        IncrementalFrame::SequenceOnly {
            symbol,
            timestamp,
            seq_id,
            prev_seq_id,
        } => {
            let slot = state.symbol_state.incremental_slot(&symbol);
            if seq_id <= slot.prev {
                state.incremental_dropped_by_seq += 1;
                let _ = timestamp;
                return;
            }
            warn_incremental_gap_if_needed(state, &symbol, seq_id, prev_seq_id, false);
            state.symbol_state.set_incremental_slot(slot, seq_id);
            let _ = timestamp;
            return;
        }
    };

    let slot = state.symbol_state.incremental_slot(&symbol);
    if !is_snapshot && seq_id <= slot.prev {
        state.incremental_dropped_by_seq += 1;
        return;
    }

    if gap_check {
        warn_incremental_gap_if_needed(state, &symbol, seq_id, prev_seq_id, is_snapshot);
    }

    let total_levels = bids.len() + asks.len();
    match max_levels {
        Some(max) if total_levels > max && max > 0 => {
            let chunks = split_levels(bids.len(), asks.len(), max);
            let total_chunks = chunks.len();
            for (chunk_idx, (bids_start, bids_count, asks_start, asks_count)) in
                chunks.into_iter().enumerate()
            {
                if !publish_incremental_chunk(
                    state,
                    publisher,
                    &symbol,
                    first_update_id,
                    final_update_id,
                    timestamp,
                    is_snapshot,
                    &bids,
                    bids_start,
                    bids_count,
                    &asks,
                    asks_start,
                    asks_count,
                    chunk_idx,
                    total_chunks,
                ) {
                    return;
                }
            }
        }
        _ => {
            if !publish_incremental_chunk(
                state,
                publisher,
                &symbol,
                first_update_id,
                final_update_id,
                timestamp,
                is_snapshot,
                &bids,
                0,
                bids.len(),
                &asks,
                0,
                asks.len(),
                0,
                1,
            ) {
                return;
            }
        }
    }
    state.symbol_state.set_incremental_slot(slot, seq_id);
}

fn publish_incremental_chunk(
    state: &mut SharedState,
    publisher: &Rc<SpreadIncrementalPublisher>,
    symbol: &str,
    first_update_id: i64,
    final_update_id: i64,
    timestamp: i64,
    is_snapshot: bool,
    bids: &[mkt_parsers::msg::mkt_msg::Level],
    bids_start: usize,
    bids_count: usize,
    asks: &[mkt_parsers::msg::mkt_msg::Level],
    asks_start: usize,
    asks_count: usize,
    chunk_idx: usize,
    total_chunks: usize,
) -> bool {
    if let Err(e) = publisher.publish_chunk(
        symbol,
        first_update_id,
        final_update_id,
        timestamp,
        is_snapshot,
        bids,
        bids_start,
        bids_count,
        asks,
        asks_start,
        asks_count,
        chunk_idx,
        total_chunks,
    ) {
        log::warn!("spread_pbs incremental publish failed: {:#}", e);
        return false;
    }
    state.incremental_published += 1;
    true
}

fn process_derivatives_bytes(
    state: &mut SharedState,
    publisher: &Rc<SpreadDerivativesPublisher>,
    bytes: &[u8],
) {
    if let Err(e) = publisher.publish(bytes) {
        log::warn!("spread_pbs derivatives publish failed: {:#}", e);
        return;
    }
    state.derivatives_published += 1;
}

fn warn_incremental_gap_if_needed(
    state: &mut SharedState,
    symbol: &str,
    seq_id: i64,
    prev_seq_id: i64,
    is_snapshot: bool,
) {
    if is_snapshot {
        return;
    }
    let prev = state.symbol_state.incremental_prev_seen(symbol);
    if seq_id < prev_seq_id {
        state.incremental_gap_warnings += 1;
        log::warn!(
            "spread_pbs OKX books sequence reset observed symbol={} prevSeqId={} seqId={}; not resyncing",
            symbol,
            prev_seq_id,
            seq_id
        );
        return;
    }
    if seq_id == prev_seq_id {
        return;
    }
    if let Some(last_seq) = prev {
        if prev_seq_id != last_seq {
            state.incremental_gap_warnings += 1;
            log::warn!(
                "spread_pbs OKX books gap observed symbol={} local_seq={} prevSeqId={} seqId={}; not resyncing",
                symbol,
                last_seq,
                prev_seq_id,
                seq_id
            );
        }
    }
}

fn split_levels(
    total_bids: usize,
    total_asks: usize,
    max: usize,
) -> Vec<(usize, usize, usize, usize)> {
    let total = total_bids + total_asks;
    if total <= max || max == 0 {
        return vec![(0, total_bids, 0, total_asks)];
    }
    let mut chunks = Vec::new();
    let mut bids_sent = 0;
    let mut asks_sent = 0;
    while bids_sent < total_bids || asks_sent < total_asks {
        let bids_remaining = total_bids - bids_sent;
        let asks_remaining = total_asks - asks_sent;
        let remaining = bids_remaining + asks_remaining;
        let chunk_bids = if remaining <= max {
            bids_remaining
        } else {
            let ratio = bids_remaining as f64 / remaining as f64;
            ((max as f64 * ratio).round() as usize)
                .max(1)
                .min(bids_remaining)
        };
        let chunk_asks = (max - chunk_bids).min(asks_remaining);
        chunks.push((bids_sent, chunk_bids, asks_sent, chunk_asks));
        bids_sent += chunk_bids;
        asks_sent += chunk_asks;
    }
    chunks
}

fn process_frame(
    state: &mut SharedState,
    publisher: &Rc<SpreadPublisher>,
    recv_us: i64,
    accepted_us: i64,
    f: BboFrame,
    source: MarketSource,
) {
    reset_dedup_high_water_if_needed(state, accepted_us);

    let slot = state.symbol_state.bbo_slot(&f.symbol);
    if should_drop_bbo_frame(&slot, &f) {
        state.dropped_by_seq += 1;
        return;
    }
    state.symbol_state.set_bbo_slot(slot, f.seq_id, f.ts_us);
    state.record_selected_source(source);

    record_latency_measurement_if_needed(
        state,
        slot.latency_measurement_symbol,
        recv_us,
        accepted_us,
        f.ts_us,
    );

    if let Err(e) = publisher.publish_bbo(
        &f.symbol,
        f.ts_us,
        f.bid_price,
        f.bid_amount,
        f.ask_price,
        f.ask_amount,
    ) {
        log::warn!("spread_pbs publish failed: {:#}", e);
        return;
    }
    state.published += 1;
}

fn record_latency_measurement_if_needed(
    state: &mut SharedState,
    latency_measurement_symbol: bool,
    recv_us: i64,
    accepted_us: i64,
    event_ts_us: i64,
) {
    if event_ts_us <= 0 || !latency_measurement_symbol {
        return;
    }
    let e2e_us = (accepted_us - event_ts_us) as f64;
    let _ = recv_us;
    state.latency_e2e.push(e2e_us);
    state.latency_ipc.push(e2e_us);
}

fn is_latency_measurement_symbol(symbol: &str) -> bool {
    let upper = symbol.trim().to_ascii_uppercase();
    if upper.is_empty() {
        return false;
    }
    let without_swap = upper.strip_suffix("-SWAP").unwrap_or(&upper);
    let base = without_swap
        .strip_suffix("_USDT")
        .or_else(|| without_swap.strip_suffix("-USDT"))
        .or_else(|| without_swap.strip_suffix("USDT"))
        .unwrap_or(without_swap);
    matches!(base, "BTC" | "ETH" | "SOL")
}

fn take_latency_snapshot(state: &mut SharedState, venue_id: u32) -> Option<LatencySnapshotMsg> {
    let mut msg = LatencySnapshotMsg::new(venue_id, get_timestamp_us());
    let mut idx = 0usize;

    snap_latency_bucket(
        &mut msg,
        &mut idx,
        METRIC_ID_SPREAD_E2E,
        state.latency_ipc.snapshot_and_reset(),
    );

    if idx == 0 {
        None
    } else {
        msg.n_buckets = idx as u32;
        Some(msg)
    }
}

fn snap_latency_bucket(
    msg: &mut LatencySnapshotMsg,
    idx: &mut usize,
    metric_id: u8,
    stats: Option<LatencyStats>,
) {
    let Some(stats) = stats else {
        return;
    };
    if *idx >= msg.buckets.len() {
        return;
    }
    msg.buckets[*idx] = LatencyBucketStat {
        metric_id,
        action_id: ACTION_ID_MARKET_DATA,
        _pad: [0; 6],
        n: stats.n,
        p50_us: stats.p50_us,
        p90_us: stats.p90_us,
        p95_us: stats.p95_us,
        p99_us: stats.p99_us,
    };
    *idx += 1;
}

fn reset_dedup_high_water_if_needed(state: &mut SharedState, accepted_us: i64) {
    if accepted_us.saturating_sub(state.last_dedup_reset_us) >= DEDUP_RESET_INTERVAL_US {
        let cleared = state.symbol_state.clear_bbo();
        state.last_dedup_reset_us = accepted_us;
        log::warn!(
            "spread_pbs dedup high-water reset by interval cleared_symbols={} interval_us={}",
            cleared,
            DEDUP_RESET_INTERVAL_US
        );
    }
}

fn should_drop_bbo_frame(slot: &SymbolSlot, f: &BboFrame) -> bool {
    if slot.prev == i64::MIN {
        return false;
    }

    if f.ts_us > 0 && slot.prev_ts_us > 0 && f.ts_us < slot.prev_ts_us {
        return true;
    }

    if f.reset_seq && f.seq_id == 1 && slot.prev > f.seq_id && f.ts_us > slot.prev_ts_us {
        return false;
    }

    f.seq_id <= slot.prev
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state(now_us: i64) -> SharedState {
        SharedState {
            symbol_state: SymbolSeqState::with_symbols(&[]),
            latency_e2e: LatencyKll::new("test-e2e"),
            latency_ipc: LatencyKll::new("test-ipc"),
            published: 0,
            trades_published: 0,
            incremental_published: 0,
            incremental_dropped_by_seq: 0,
            incremental_gap_warnings: 0,
            derivatives_published: 0,
            selected_whitelist: 0,
            selected_normal: 0,
            selected_other: 0,
            dropped_by_seq: 0,
            trades_dropped_by_seq: 0,
            last_dedup_reset_us: now_us,
        }
    }

    fn frame_at(symbol: &str, seq_id: i64, reset_seq: bool, ts_us: i64) -> BboFrame {
        BboFrame {
            symbol: symbol.to_string(),
            ts_us,
            seq_id,
            reset_seq,
            bid_price: 1.0,
            bid_amount: 1.0,
            ask_price: 2.0,
            ask_amount: 1.0,
        }
    }

    #[test]
    fn latency_measurement_symbols_are_limited_to_major_assets() {
        for symbol in [
            "BTCUSDT",
            "ETHUSDT",
            "SOLUSDT",
            "btc_usdt",
            "ETH-USDT",
            "SOL-USDT-SWAP",
        ] {
            assert!(
                is_latency_measurement_symbol(symbol),
                "expected {symbol} to be measured"
            );
        }

        for symbol in ["XRPUSDT", "DOGE_USDT", "BNB-USDT", "BTCUSDC", ""] {
            assert!(
                !is_latency_measurement_symbol(symbol),
                "expected {symbol} to be skipped"
            );
        }
    }

    #[test]
    fn latency_measurements_skip_non_major_assets_for_both_buckets() {
        let mut state = test_state(1_000_000);
        record_latency_measurement_if_needed(&mut state, false, 120, 130, 100);
        assert!(take_latency_snapshot(&mut state, 7).is_none());

        record_latency_measurement_if_needed(&mut state, true, 120, 130, 100);
        let msg = take_latency_snapshot(&mut state, 7).expect("snapshot");
        assert_eq!(msg.n_buckets, 1);
        assert_eq!(msg.buckets[0].metric_id, METRIC_ID_SPREAD_E2E);
        assert_eq!(msg.buckets[0].n, 1);
    }

    #[test]
    fn symbol_seq_state_reuses_symbol_slot_across_streams() {
        let mut state = SymbolSeqState::with_symbols(&["BTCUSDT".to_string()]);
        let bbo = state.bbo_slot("BTCUSDT");
        state.set_bbo_slot(bbo, 10, 1_000);
        let trade = state.trade_slot("BTCUSDT");
        state.set_trade_slot(trade, 20);
        let inc = state.incremental_slot("BTCUSDT");
        state.set_incremental_slot(inc, 30);

        assert_eq!(state.index_by_symbol.len(), 1);
        assert_eq!(state.bbo_prev("BTCUSDT"), 10);
        assert_eq!(state.trade_seen(), 1);
        assert_eq!(state.incremental_seen(), 1);
        assert_eq!(state.clear_bbo(), 1);
        assert_eq!(state.bbo_seen(), 0);
        assert_eq!(state.trade_seen(), 1);
        assert_eq!(state.incremental_prev_seen("BTCUSDT"), Some(30));
    }

    #[test]
    fn looks_like_json_skips_binary_sbe_frames() {
        assert!(looks_like_json(br#" {"topic":"orderbook.1.BTCUSDT"}"#));
        assert!(looks_like_json(b"\n\t[1,2,3]"));
        assert!(!looks_like_json(&[64, 0, 1, 0, 1, 0, 3, 0]));
        assert!(!looks_like_json(b"pong"));
        assert!(!looks_like_json(b""));
    }

    #[test]
    fn parse_env_bool_accepts_common_on_off_values() {
        assert_eq!(parse_env_bool("1"), Some(true));
        assert_eq!(parse_env_bool("true"), Some(true));
        assert_eq!(parse_env_bool("on"), Some(true));
        assert_eq!(parse_env_bool("0"), Some(false));
        assert_eq!(parse_env_bool("false"), Some(false));
        assert_eq!(parse_env_bool("off"), Some(false));
        assert_eq!(parse_env_bool("maybe"), None);
    }

    #[test]
    fn spawn_direct_replacement_batch_legs_keeps_one_subscribe_batch_per_ws() {
        struct NoopAdapter;

        impl VenueAdapter for NoopAdapter {
            fn name(&self) -> &'static str {
                "noop"
            }

            fn ws_url(&self) -> String {
                "wss://example.invalid/ws".to_string()
            }

            fn build_subscribe(&self, _symbols: &[String]) -> Vec<serde_json::Value> {
                Vec::new()
            }

            fn parse_frame(
                &self,
                _value: &serde_json::Value,
                _emit: &mut dyn FnMut(BboFrame) -> Result<()>,
            ) -> Result<()> {
                Ok(())
            }

            fn keepalive(&self) -> Option<KeepaliveSpec> {
                None
            }
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        local.block_on(&rt, async {
            let adapter: Rc<dyn VenueAdapter> = Rc::new(NoopAdapter);
            let state = Rc::new(RefCell::new(test_state(0)));
            let mut legs = Vec::new();
            let subscribe_msgs = vec![
                serde_json::json!({"method":"SUBSCRIBE","params":["a@trade"],"id":1}),
                serde_json::json!({"method":"SUBSCRIBE","params":["b@trade"],"id":2}),
                serde_json::json!({"method":"SUBSCRIBE","params":["c@trade"],"id":3}),
            ];

            spawn_direct_replacement_batch_legs(
                &mut legs,
                "direct-test",
                "wss://example.invalid/ws".to_string(),
                "0.0.0.0".to_string(),
                subscribe_msgs,
                &adapter,
                None,
                None,
                None,
                None,
                state,
            );

            assert_eq!(legs.len(), 3);
            for leg in legs {
                let _ = leg.shutdown_tx.send(true);
                leg.handle.abort();
            }
        });
    }

    #[test]
    fn bybit_forced_snapshot_same_u_is_dropped_without_resetting_high_water() {
        let now_us = 1_000_000;
        let mut state = test_state(now_us);
        let btc = state.symbol_state.bbo_slot("BTCUSDT");
        state.symbol_state.set_bbo_slot(btc, 100, 1_000);
        let eth = state.symbol_state.bbo_slot("ETHUSDT");
        state.symbol_state.set_bbo_slot(eth, 200, 1_000);

        reset_dedup_high_water_if_needed(&mut state, now_us + 1);
        let slot = state.symbol_state.bbo_slot("BTCUSDT");

        assert!(should_drop_bbo_frame(
            &slot,
            &frame_at("BTCUSDT", 100, true, 2_000)
        ));
        assert_eq!(state.symbol_state.bbo_prev("BTCUSDT"), 100);
        assert_eq!(state.symbol_state.bbo_prev("ETHUSDT"), 200);
        assert_eq!(state.last_dedup_reset_us, now_us);
    }

    #[test]
    fn bybit_restart_snapshot_u_one_with_new_ts_is_accepted() {
        let mut state = test_state(1_000_000);
        let btc = state.symbol_state.bbo_slot("BTCUSDT");
        state.symbol_state.set_bbo_slot(btc, 100, 1_000);

        let slot = state.symbol_state.bbo_slot("BTCUSDT");

        assert!(!should_drop_bbo_frame(
            &slot,
            &frame_at("BTCUSDT", 1, true, 2_000)
        ));
    }

    #[test]
    fn bybit_restart_u_one_forced_snapshot_same_u_is_dropped() {
        let mut state = test_state(1_000_000);
        let btc = state.symbol_state.bbo_slot("BTCUSDT");
        state.symbol_state.set_bbo_slot(btc, 1, 2_000);

        let slot = state.symbol_state.bbo_slot("BTCUSDT");

        assert!(should_drop_bbo_frame(
            &slot,
            &frame_at("BTCUSDT", 1, true, 5_000)
        ));
    }

    #[test]
    fn bybit_stale_high_u_after_restart_is_dropped_by_ts() {
        let mut state = test_state(1_000_000);
        let btc = state.symbol_state.bbo_slot("BTCUSDT");
        state.symbol_state.set_bbo_slot(btc, 1, 2_000);

        let slot = state.symbol_state.bbo_slot("BTCUSDT");

        assert!(should_drop_bbo_frame(
            &slot,
            &frame_at("BTCUSDT", 101, false, 1_000)
        ));
    }

    #[test]
    fn dedup_high_water_marks_reset_periodically() {
        let now_us = 1_000_000;
        let mut state = test_state(now_us);
        let btc = state.symbol_state.bbo_slot("BTCUSDT");
        state.symbol_state.set_bbo_slot(btc, 100, 1_000);
        let eth = state.symbol_state.bbo_slot("ETHUSDT");
        state.symbol_state.set_bbo_slot(eth, 200, 1_000);

        reset_dedup_high_water_if_needed(&mut state, now_us + DEDUP_RESET_INTERVAL_US);

        assert_eq!(state.symbol_state.bbo_seen(), 0);
        assert_eq!(state.last_dedup_reset_us, now_us + DEDUP_RESET_INTERVAL_US);
    }

    #[test]
    fn latency_snapshot_contains_single_spread_e2e_bucket() {
        let mut state = test_state(1_000_000);
        state.latency_ipc.push(12.0);
        state.latency_ipc.push(22.0);

        let msg = take_latency_snapshot(&mut state, 7).expect("snapshot");
        assert_eq!(msg.venue_id, 7);
        assert_eq!(msg.n_buckets, 1);
        assert_eq!(msg.buckets[0].metric_id, METRIC_ID_SPREAD_E2E);
        assert_eq!(msg.buckets[0].action_id, ACTION_ID_MARKET_DATA);
        assert_eq!(msg.buckets[0].n, 2);
        assert!(take_latency_snapshot(&mut state, 7).is_none());
    }
}
