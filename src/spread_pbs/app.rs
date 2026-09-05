use anyhow::{bail, Context, Result};
use runtime_common::fast_hash::{
    fast_hash_map_with_capacity, fast_hash_set_with_capacity, FastHashMap, FastHashSet,
};
use std::cell::RefCell;
use std::collections::{HashSet, VecDeque};
use std::env;
use std::rc::Rc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

use crate::mkt_pub::cfg::Config;
use order_common::TradingVenue;
use runtime_common::time_util::get_timestamp_us;

use crate::spread_pbs::adapter::{
    create_adapter, BboDedupPolicy, BboFrame, IncrementalDedupPolicy, IncrementalFrame,
    KeepaliveSpec, RawIncremental, RawIncrementalView, TradeDedupPolicy, TradeFrame, VenueAdapter,
};
use crate::spread_pbs::binance::{
    binance_futures_mm_ws_enabled, binance_futures_standard_ws_url,
    ENV_BINANCE_FUTURES_MM_WS_LOCAL_IP, ENV_BINANCE_FUTURES_MM_WS_MODE,
};
use crate::spread_pbs::binance_fix_sbe::{
    decode_market_frame, new_bbo_state, run_fix_sbe_md, BinanceSpotTransport, FixMdLoopParams,
    FixMdStreamKind, FixSbeMarketEvent, ENV_BINANCE_SPOT_TRANSPORT, MAX_FIX_SBE_LEVELS,
};
use crate::spread_pbs::okex_derivatives::{
    build_okex_derivatives_subscribe_msgs, parse_okex_derivatives_frame, OKEX_PUBLIC_WS_URL,
};
use crate::spread_pbs::publisher::{
    PayloadLevel, SpreadDerivativesPublisher, SpreadIncrementalPublisher, SpreadPbsPublishRoots,
    SpreadPublisher, SpreadTradePublisher,
};
use crate::spread_pbs::ws::{
    run_public_ws, run_public_ws_with_ack_policy, FrameHandler, RollingRestartSpec, WsLoopParams,
};

mod hyperliquid_shards;

const DEDUP_RESET_INTERVAL_US: i64 = 5 * 60 * 1_000_000;
const BBO_IDENTITY_DEDUP_PER_SYMBOL: usize = 1_024;
const TRADE_IDENTITY_DEDUP_PER_SYMBOL: usize = 1_024;
const INCREMENTAL_IDENTITY_DEDUP_PER_SYMBOL: usize = 128;
const DERIVATIVES_DEDUP_WINDOW_US: i64 = 30_000_000;
const DERIVATIVES_DEDUP_PRUNE_EVERY: usize = 4_096;
const WS_BUSINESS_IDLE_TIMEOUT: Duration = Duration::from_secs(10);
const INCREMENTAL_HEALTH_STARTUP_GRACE: Duration = Duration::from_secs(30);
const INCREMENTAL_CRITICAL_STALE: Duration = Duration::from_secs(15);
const INCREMENTAL_STALE_LOG_INTERVAL: Duration = Duration::from_secs(30);
const HYPERLIQUID_STALE_RESTART_COOLDOWN: Duration = Duration::from_secs(15);
const HEALTH_WALL_CLOCK_JUMP_THRESHOLD: Duration = Duration::from_secs(1);
const INCREMENTAL_CRITICAL_SYMBOLS: [&str; 3] = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];
const COIN_INCREMENTAL_CRITICAL_SYMBOLS: [&str; 2] = ["BTCUSD_PERP", "ETHUSD_PERP"];
const ENV_ENABLE_TRADE: &str = "SPREAD_PBS_ENABLE_TRADE";
const ENV_ENABLE_INCREMENTAL: &str = "SPREAD_PBS_ENABLE_INCREMENTAL";
const ENV_ENABLE_DERIVATIVES: &str = "SPREAD_PBS_ENABLE_DERIVATIVES";
const ENV_SYMBOLS: &str = "SPREAD_PBS_SYMBOLS";
pub const ENV_BINANCE_FUTURES_MARKET_CORE: &str = "SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE";
pub const ENV_BINANCE_FUTURES_BOOKTICKER_CORE: &str = "SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinanceFuturesRole {
    Full,
    Market,
    BookTicker,
}

impl BinanceFuturesRole {
    pub fn parse(raw: &str) -> std::result::Result<Self, String> {
        match raw.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "" | "full" | "all" => Ok(Self::Full),
            "market" | "trade-depth" | "trade-depth-derivatives" => Ok(Self::Market),
            "bookticker" | "book-ticker" | "bbo" => Ok(Self::BookTicker),
            other => Err(format!(
                "invalid Binance futures spread_pbs role {:?}; expected full/market/bookticker",
                other
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Market => "market",
            Self::BookTicker => "bookticker",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BybitRole {
    Full,
    Market,
    BookTicker,
}

impl BybitRole {
    pub fn parse(raw: &str) -> std::result::Result<Self, String> {
        match raw.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "" | "full" | "all" => Ok(Self::Full),
            "market" | "trade-depth" | "trade-depth-derivatives" => Ok(Self::Market),
            "bookticker" | "book-ticker" | "bbo" => Ok(Self::BookTicker),
            other => Err(format!(
                "invalid Bybit spread_pbs role {:?}; expected full/market/bookticker",
                other
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Market => "market",
            Self::BookTicker => "bookticker",
        }
    }
}

pub struct SpreadPbsApp {
    config: Config,
    publish_roots: SpreadPbsPublishRoots,
    binance_futures_role: BinanceFuturesRole,
    bybit_role: BybitRole,
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
        TradingVenue::BitgetMargin | TradingVenue::BitgetFutures | TradingVenue::BitgetCoinFutures
    )
}

fn is_bybit_venue(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BybitMargin | TradingVenue::BybitFutures
    )
}

fn is_hyperliquid_venue(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::HyperliquidMargin | TradingVenue::HyperliquidFutures
    )
}

fn critical_incremental_symbols_for_venue(
    venue: TradingVenue,
    current_symbols: &HashSet<String>,
    static_symbols: &[String],
) -> Vec<String> {
    if !is_hyperliquid_venue(venue) {
        return static_symbols.to_vec();
    }
    let mut symbols = current_symbols.iter().cloned().collect::<Vec<_>>();
    symbols.sort_unstable();
    symbols
}

fn role_stream_policy(
    venue: TradingVenue,
    binance_futures_role: BinanceFuturesRole,
    bybit_role: BybitRole,
) -> (bool, bool) {
    if is_hyperliquid_venue(venue) {
        return (true, true);
    }
    let market_only = (venue == TradingVenue::BinanceFutures
        && binance_futures_role == BinanceFuturesRole::Market)
        || (is_bybit_venue(venue) && bybit_role == BybitRole::Market);
    let bookticker_only = (venue == TradingVenue::BinanceFutures
        && binance_futures_role == BinanceFuturesRole::BookTicker)
        || (is_bybit_venue(venue) && bybit_role == BybitRole::BookTicker);
    (!market_only, !bookticker_only)
}

fn is_binance_venue(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BinanceMargin
            | TradingVenue::BinanceFutures
            | TradingVenue::BinanceCoinFutures
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
        || is_hyperliquid_venue(venue)
        || matches!(venue, TradingVenue::GateMargin | TradingVenue::GateFutures)
}

fn direct_incremental_replacement_enabled(venue: TradingVenue) -> bool {
    is_okex_venue(venue)
        || is_bitget_venue(venue)
        || is_bybit_venue(venue)
        || is_binance_venue(venue)
        || is_hyperliquid_venue(venue)
        || matches!(venue, TradingVenue::GateMargin | TradingVenue::GateFutures)
}

fn direct_derivatives_replacement_enabled(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::OkexFutures
            | TradingVenue::BinanceFutures
            | TradingVenue::BinanceCoinFutures
            | TradingVenue::BitgetFutures
            | TradingVenue::BitgetCoinFutures
            | TradingVenue::GateFutures
            | TradingVenue::BybitFutures
            | TradingVenue::HyperliquidFutures
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

async fn get_symbols_for_role(
    config: &Config,
    binance_futures_role: BinanceFuturesRole,
) -> Result<Vec<String>> {
    if is_hyperliquid_venue(config.venue) {
        crate::spread_pbs::hyperliquid::refresh_symbols(config.venue).await
    } else if uses_all_binance_futures_symbols(config.venue, binance_futures_role) {
        Config::get_all_binance_futures_symbols().await
    } else {
        config.get_symbols().await
    }
}

fn uses_all_binance_futures_symbols(
    venue: TradingVenue,
    binance_futures_role: BinanceFuturesRole,
) -> bool {
    venue == TradingVenue::BinanceFutures && binance_futures_role == BinanceFuturesRole::BookTicker
}

async fn wait_for_symbols_for_role(
    config: &Config,
    binance_futures_role: BinanceFuturesRole,
) -> Vec<String> {
    let mut backoff = Duration::from_secs(2);
    let cap = Duration::from_secs(30);
    loop {
        match get_symbols_for_role(config, binance_futures_role).await {
            Ok(symbols) if !symbols.is_empty() => return symbols,
            Ok(_) => log::error!(
                "spread_pbs[{}] symbol lookup returned empty for role={}; retry in {:?}",
                config.venue.data_pub_slug(),
                binance_futures_role.as_str(),
                backoff,
            ),
            Err(err) => log::error!(
                "spread_pbs[{}] symbol lookup failed for role={}: {:#}; retry in {:?}",
                config.venue.data_pub_slug(),
                binance_futures_role.as_str(),
                err,
                backoff,
            ),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(cap);
    }
}

fn build_market_subscribe(
    adapter: &Rc<dyn VenueAdapter>,
    symbols: &[String],
    include_bbo: bool,
    include_trade: bool,
    include_incremental: bool,
    include_derivatives: bool,
) -> Vec<serde_json::Value> {
    let mut out = if include_bbo {
        adapter.build_subscribe(symbols)
    } else {
        Vec::new()
    };
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
    label: String,
    local_ip: String,
    url: String,
    source: MarketSource,
    shutdown_tx: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

struct FixMdLeg {
    label: String,
    kind: FixMdStreamKind,
    local_ip: String,
    shutdown_tx: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

/// 跨 leg 共享的上下文：adapter / publisher / state / ws url 在 spread_pbs 整个生命周期不变。
struct LegCtx {
    adapter: Rc<dyn VenueAdapter>,
    publisher: Option<Rc<SpreadPublisher>>,
    trade_publisher: Option<Rc<SpreadTradePublisher>>,
    incremental_publisher: Option<Rc<SpreadIncrementalPublisher>>,
    derivatives_publisher: Option<Rc<SpreadDerivativesPublisher>>,
    incremental_max_levels: Option<usize>,
    state: Rc<RefCell<SharedState>>,
    url: String,
    parse_okex_notices: bool,
    hyperliquid_subscription_budget:
        Option<crate::spread_pbs::hyperliquid::HyperliquidSubscriptionBudget>,
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
        Self {
            config,
            publish_roots: SpreadPbsPublishRoots::production(),
            binance_futures_role: BinanceFuturesRole::Full,
            bybit_role: BybitRole::Full,
        }
    }

    pub fn new_with_publish_roots(config: Config, publish_roots: SpreadPbsPublishRoots) -> Self {
        Self {
            config,
            publish_roots,
            binance_futures_role: BinanceFuturesRole::Full,
            bybit_role: BybitRole::Full,
        }
    }

    pub fn new_with_publish_roots_and_binance_futures_role(
        config: Config,
        publish_roots: SpreadPbsPublishRoots,
        binance_futures_role: BinanceFuturesRole,
    ) -> Self {
        Self {
            config,
            publish_roots,
            binance_futures_role,
            bybit_role: BybitRole::Full,
        }
    }

    pub fn new_with_publish_roots_and_roles(
        config: Config,
        publish_roots: SpreadPbsPublishRoots,
        binance_futures_role: BinanceFuturesRole,
        bybit_role: BybitRole,
    ) -> Self {
        Self {
            config,
            publish_roots,
            binance_futures_role,
            bybit_role,
        }
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
        // Capture this before metadata and IPC initialization so Hyperliquid's
        // subscription backfill can cover all trades that occur during startup.
        let app_started_at_us = get_timestamp_us().div_euclid(1_000) * 1_000;
        let venue = self.config.venue;
        let venue_slug: &'static str = venue.data_pub_slug();
        let binance_spot_transport = if venue == TradingVenue::BinanceMargin {
            BinanceSpotTransport::from_env()?
        } else {
            BinanceSpotTransport::WsSbe
        };
        let publish_roots = self.publish_roots.clone();
        let binance_futures_role = if venue == TradingVenue::BinanceFutures {
            self.binance_futures_role
        } else {
            BinanceFuturesRole::Full
        };
        let bybit_role = if is_bybit_venue(venue) {
            self.bybit_role
        } else {
            BybitRole::Full
        };

        let adapter = match create_adapter(venue).await? {
            Some(a) => Rc::<dyn VenueAdapter>::from(a),
            None => bail!(
                "spread_pbs 当前不支持 venue {:?}（仅 OKex/Binance/Bybit/Gate/Bitget/Hyperliquid × spot+futures）",
                venue
            ),
        };
        log::info!(
            "spread_pbs starting venue={} adapter={} spread_root={} dat_root={} binance_futures_role={} bybit_role={} binance_spot_transport={:?}",
            venue_slug,
            adapter.name(),
            publish_roots.spread_root(),
            publish_roots.dat_root(),
            binance_futures_role.as_str(),
            bybit_role.as_str(),
            binance_spot_transport,
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
        let initial_symbols = apply_symbol_filter(
            wait_for_symbols_for_role(&self.config, binance_futures_role).await,
            venue_slug,
        );
        if initial_symbols.is_empty() {
            bail!(
                "spread_pbs[{}] no symbols left after {} filter",
                venue_slug,
                ENV_SYMBOLS
            );
        }
        let static_critical_incremental_symbols: Vec<String> = if is_hyperliquid_venue(venue) {
            Vec::new()
        } else if venue == TradingVenue::BinanceCoinFutures {
            COIN_INCREMENTAL_CRITICAL_SYMBOLS
                .iter()
                .map(|symbol| (*symbol).to_string())
                .collect()
        } else {
            INCREMENTAL_CRITICAL_SYMBOLS
                .iter()
                .map(|symbol| (*symbol).to_string())
                .collect()
        };
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
        let is_binance_futures_market_role = venue == TradingVenue::BinanceFutures
            && binance_futures_role == BinanceFuturesRole::Market;
        let (bbo_enabled, replacement_enabled) =
            role_stream_policy(venue, binance_futures_role, bybit_role);
        let direct_trade_enabled =
            replacement_enabled && enable_trade && direct_trade_replacement_enabled(venue);
        let direct_incremental_enabled = enable_incremental
            && replacement_enabled
            && direct_incremental_replacement_enabled(venue)
            && !adapter
                .build_incremental_subscribe(&initial_symbols)
                .is_empty();
        let direct_derivatives_enabled = enable_derivatives
            && replacement_enabled
            && direct_derivatives_replacement_enabled(venue)
            && (is_okex_derivatives_venue(venue)
                || !adapter
                    .build_derivatives_subscribe(&initial_symbols)
                    .is_empty());
        log::info!(
            "spread_pbs[{}] data_types askbid={} trade={} incremental={} derivatives={} env_overrides {}={:?} {}={:?} {}={:?}",
            venue_slug,
            bbo_enabled,
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
        let initial_subs = if is_binance_futures_market_role {
            Vec::new()
        } else {
            build_market_subscribe(
                &adapter,
                &initial_symbols,
                bbo_enabled,
                direct_trade_enabled,
                direct_incremental_enabled,
                direct_derivatives_enabled,
            )
        };
        if initial_subs.is_empty() && !is_binance_futures_market_role {
            bail!(
                "adapter.build_subscribe 返回空（{} symbols 数={}）",
                venue_slug,
                initial_symbols.len()
            );
        }
        let hyperliquid_shards = if is_hyperliquid_venue(venue) {
            hyperliquid_shards::sources_from_env()?
        } else {
            None
        };
        let hyperliquid_subscription_budget = if is_hyperliquid_venue(venue)
            && hyperliquid_shards.is_none()
        {
            let budget = crate::spread_pbs::hyperliquid::HyperliquidSubscriptionBudget::reserve(
                initial_subs.len(),
                &primary_local_ip,
                &secondary_local_ip,
            )?;
            log::info!(
                "spread_pbs[{}] Hyperliquid subscription budget checked for this process; \
                 distinct local IPs must have distinct public egress, and other collectors/private \
                 streams sharing those egress IPs also consume the venue limit",
                venue_slug
            );
            Some(budget)
        } else {
            None
        };
        log::info!(
            "spread_pbs[{}] initial symbols={} subscribe_batches={}",
            venue_slug,
            initial_symbols.len(),
            initial_subs.len()
        );

        // ---- IceOryx publisher + 共享态（Rc<RefCell> 单线程零锁，跨重启复用）----
        let publisher = if bbo_enabled {
            let publisher = Rc::new(
                SpreadPublisher::new_with_root(venue_slug, publish_roots.spread_root())
                    .with_context(|| format!("create iceoryx publisher for {}", venue_slug))?,
            );
            publisher
                .seed_symbols(&initial_symbols)
                .with_context(|| format!("seed BBO payload prefixes for {}", venue_slug))?;
            Some(publisher)
        } else {
            None
        };
        let trade_publisher = if direct_trade_enabled {
            let publisher = Rc::new(
                SpreadTradePublisher::new_open_or_create_with_root(
                    venue_slug,
                    publish_roots.dat_root(),
                )
                .unwrap_or_else(|e| {
                    panic!(
                        "spread_pbs[{}] failed to open/create replacement trade ipc channel {}/{}/trade: {:#}",
                        venue_slug,
                        publish_roots.dat_root(),
                        venue_slug,
                        e
                    )
                }),
            );
            publisher
                .seed_symbols(&initial_symbols)
                .with_context(|| format!("seed trade payload prefixes for {}", venue_slug))?;
            Some(publisher)
        } else {
            None
        };
        let incremental_publisher = if direct_incremental_enabled {
            let publisher = Rc::new(
                SpreadIncrementalPublisher::new_open_or_create_with_root(
                    venue_slug,
                    publish_roots.dat_root(),
                )
                .unwrap_or_else(|e| {
                    panic!(
                        "spread_pbs[{}] failed to open/create replacement incremental ipc channel {}/{}/incremental: {:#}",
                        venue_slug,
                        publish_roots.dat_root(),
                        venue_slug,
                        e
                    )
                }),
            );
            publisher
                .seed_symbols(&initial_symbols)
                .with_context(|| format!("seed incremental payload prefixes for {}", venue_slug))?;
            Some(publisher)
        } else {
            None
        };
        let derivatives_publisher = if direct_derivatives_enabled {
            let publisher = Rc::new(
                SpreadDerivativesPublisher::new_open_or_create_with_root(
                    venue_slug,
                    publish_roots.dat_root(),
                )
                .unwrap_or_else(|e| {
                    panic!(
                        "spread_pbs[{}] failed to open/create replacement derivatives ipc channel {}/{}/derivatives: {:#}",
                        venue_slug,
                        publish_roots.dat_root(),
                        venue_slug,
                        e
                    )
                }),
            );
            publisher
                .seed_symbols(&initial_symbols)
                .with_context(|| format!("seed derivatives payload prefixes for {}", venue_slug))?;
            Some(publisher)
        } else {
            None
        };
        let state: Rc<RefCell<SharedState>> = Rc::new(RefCell::new(SharedState {
            symbol_state: SymbolSeqState::with_symbols(&initial_symbols),
            bbo_dedup_policy: adapter.bbo_dedup_policy(),
            trade_publish_not_before_us: (adapter.trade_dedup_policy()
                == TradeDedupPolicy::RecentIdentity)
                .then_some(app_started_at_us),
            published: 0,
            trades_published: 0,
            incremental_published: 0,
            incremental_dropped_by_seq: 0,
            incremental_gap_warnings: 0,
            derivatives_published: 0,
            derivatives_dropped_duplicate: 0,
            derivatives_recent: fast_hash_map_with_capacity(2048),
            derivatives_since_prune: 0,
            recent_bbo_identities: Vec::with_capacity(initial_symbols.len()),
            recent_trade_identities: Vec::with_capacity(initial_symbols.len()),
            recent_incremental_identities: Vec::with_capacity(initial_symbols.len()),
            selected_whitelist: 0,
            selected_normal: 0,
            selected_other: 0,
            dropped_by_seq: 0,
            trades_dropped_by_seq: 0,
            last_dedup_reset_us: get_timestamp_us(),
        }));

        if venue == TradingVenue::BinanceMargin
            && binance_spot_transport == BinanceSpotTransport::FixSbe
        {
            log::info!(
                "spread_pbs[{}] {}=fix_sbe; replacing Binance Spot WebSocket/SBE legs with FIX/SBE market-data sessions",
                venue_slug,
                ENV_BINANCE_SPOT_TRANSPORT,
            );
            return self
                .run_binance_spot_fix_sbe_with_shutdown(
                    adapter,
                    initial_symbols,
                    primary_local_ip,
                    secondary_local_ip,
                    publisher.expect("BBO publisher must exist for Binance Spot"),
                    trade_publisher,
                    incremental_publisher,
                    state,
                    shutdown_rx,
                )
                .await;
        }

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
            parse_okex_notices: is_okex_venue(venue),
            hyperliquid_subscription_budget,
        };
        let primary_url = ctx.url.clone();
        if let Some(sources) = hyperliquid_shards {
            return hyperliquid_shards::run(
                ctx,
                &self.config,
                initial_subs,
                initial_symbols,
                sources,
                shutdown_rx,
            )
            .await;
        }
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
        let mut primary = if is_binance_futures_market_role {
            None
        } else if !bbo_enabled {
            Some(spawn_replacement_leg_on_main_ws(
                "primary",
                primary_local_ip,
                primary_url,
                initial_subs.clone(),
                &ctx,
            ))
        } else {
            let publisher = publisher
                .clone()
                .expect("BBO publisher must exist for non-market spread_pbs role");
            Some(spawn_leg(
                "primary",
                primary_local_ip,
                primary_url,
                primary_source,
                initial_subs.clone(),
                &ctx,
                publisher,
            ))
        };
        let mut secondary = if is_binance_futures_market_role {
            None
        } else if !bbo_enabled {
            Some(spawn_replacement_leg_on_main_ws(
                "secondary",
                secondary_local_ip,
                secondary_url,
                initial_subs,
                &ctx,
            ))
        } else {
            let publisher = publisher
                .clone()
                .expect("BBO publisher must exist for non-market spread_pbs role");
            Some(spawn_leg(
                "secondary",
                secondary_local_ip,
                secondary_url,
                secondary_source,
                initial_subs,
                &ctx,
                publisher,
            ))
        };

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
        let health_started_at = Instant::now();
        let mut health_ticker = tokio::time::interval(Duration::from_secs(1));
        health_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        health_ticker.tick().await;
        let mut reported_stale_incremental_symbols = Vec::<String>::new();
        let mut next_incremental_stale_log_at = health_started_at;
        let mut incremental_stale_started_at = None::<Instant>;
        let mut next_incremental_recovery_restart_at = health_started_at;
        let mut restart_primary_for_stale = true;
        let mut last_health_sample_at = health_started_at;
        let mut last_health_wall_us = get_timestamp_us();
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        log::info!("spread_pbs[{}] shutdown requested", venue_slug);
                    } else {
                        continue;
                    }
                    if let Some(primary) = primary.as_mut() {
                        let _ = primary.shutdown_tx.send(true);
                    }
                    if let Some(secondary) = secondary.as_mut() {
                        let _ = secondary.shutdown_tx.send(true);
                    }
                    if let Some(leg) = derivatives_leg.as_mut() {
                        let _ = leg.shutdown_tx.send(true);
                    }
                    for leg in &mut direct_extra_legs {
                        let _ = leg.shutdown_tx.send(true);
                    }
                    if let Some(primary) = primary.as_mut() {
                        let _ = (&mut primary.handle).await;
                    }
                    if let Some(secondary) = secondary.as_mut() {
                        let _ = (&mut secondary.handle).await;
                    }
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
                    log::info!(
                        "spread_pbs[{}] stats published={} trades_published={} incremental_published={} derivatives_published={} derivatives_dropped_duplicate={} dropped_by_seq={} trades_dropped_by_seq={} incremental_dropped_by_seq={} incremental_gap_warnings={} symbols_seen={} trade_symbols_seen={} incremental_symbols_seen={}",
                        venue_slug,
                        s.published,
                        s.trades_published,
                        s.incremental_published,
                        s.derivatives_published,
                        s.derivatives_dropped_duplicate,
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
                _ = health_ticker.tick(), if direct_incremental_enabled => {
                    if health_started_at.elapsed() >= INCREMENTAL_HEALTH_STARTUP_GRACE {
                        let now = Instant::now();
                        let wall_now_us = get_timestamp_us();
                        let clock_delta_us = wall_clock_delta_us(
                            last_health_wall_us,
                            wall_now_us,
                            now.duration_since(last_health_sample_at),
                        );
                        if clock_delta_us.unsigned_abs()
                            >= HEALTH_WALL_CLOCK_JUMP_THRESHOLD.as_micros()
                        {
                            log::error!(
                                "spread_pbs[{}] wall clock jump detected delta_us={}",
                                venue_slug,
                                clock_delta_us,
                            );
                        }
                        last_health_sample_at = now;
                        last_health_wall_us = wall_now_us;
                        let critical_incremental_symbols_storage =
                            critical_incremental_symbols_for_venue(
                                venue,
                                &current_symbols,
                                &static_critical_incremental_symbols,
                            );
                        let critical_incremental_symbols = critical_incremental_symbols_storage
                            .iter()
                            .map(String::as_str)
                            .collect::<Vec<_>>();
                        let stale = state
                            .borrow()
                            .symbol_state
                            .stale_incremental_symbols(
                                &critical_incremental_symbols,
                                now,
                                INCREMENTAL_CRITICAL_STALE,
                            );
                        if stale.is_empty() {
                            if !reported_stale_incremental_symbols.is_empty() {
                                let stale_duration_ms = incremental_stale_started_at
                                    .take()
                                    .map(|started_at| now.duration_since(started_at).as_millis())
                                    .unwrap_or(0);
                                let s = state.borrow();
                                let symbol_ages = s.symbol_state.critical_stream_age_summary(&critical_incremental_symbols, now);
                                log::warn!(
                                    "spread_pbs[{}] critical incremental input recovered symbols={} threshold_ms={} stale_duration_ms={} symbol_ages={} incremental_published={} trades_published={}",
                                    venue_slug,
                                    reported_stale_incremental_symbols.join(","),
                                    INCREMENTAL_CRITICAL_STALE.as_millis(),
                                    stale_duration_ms,
                                    symbol_ages,
                                    s.incremental_published,
                                    s.trades_published,
                                );
                            }
                            reported_stale_incremental_symbols.clear();
                            next_incremental_stale_log_at = now;
                            next_incremental_recovery_restart_at = now;
                            restart_primary_for_stale = true;
                        } else {
                            incremental_stale_started_at.get_or_insert(now);
                            let stale_changed = stale != reported_stale_incremental_symbols;
                            if stale_changed || now >= next_incremental_stale_log_at {
                                let s = state.borrow();
                                let symbol_ages = s.symbol_state.critical_stream_age_summary(&critical_incremental_symbols, now);
                                log::error!(
                                    "spread_pbs[{}] critical incremental input stale symbols={} threshold_ms={} symbol_ages={} incremental_published={} trades_published={} incremental_dropped_by_seq={} trades_dropped_by_seq={}; keeping process alive while websocket legs reconnect",
                                    venue_slug,
                                    stale.join(","),
                                    INCREMENTAL_CRITICAL_STALE.as_millis(),
                                    symbol_ages,
                                    s.incremental_published,
                                    s.trades_published,
                                    s.incremental_dropped_by_seq,
                                    s.trades_dropped_by_seq,
                                );
                                next_incremental_stale_log_at =
                                    now + INCREMENTAL_STALE_LOG_INTERVAL;
                            }
                            reported_stale_incremental_symbols = stale;
                            if is_hyperliquid_venue(venue)
                                && now >= next_incremental_recovery_restart_at
                            {
                                let restarted_label = if restart_primary_for_stale {
                                    if let Some(primary) = primary.as_mut() {
                                        log::warn!(
                                            "spread_pbs[{}] restarting primary websocket leg to recover stale Hyperliquid incremental input",
                                            venue_slug,
                                        );
                                        restart_leg(
                                            venue_slug,
                                            primary,
                                            &self.config,
                                            binance_futures_role,
                                            &ctx,
                                            &mut current_symbols,
                                        ).await;
                                        next_primary_restart = Instant::now() + restart_duration;
                                        Some("primary")
                                    } else {
                                        None
                                    }
                                } else if let Some(secondary) = secondary.as_mut() {
                                    log::warn!(
                                        "spread_pbs[{}] restarting secondary websocket leg to recover stale Hyperliquid incremental input",
                                        venue_slug,
                                    );
                                    restart_leg(
                                        venue_slug,
                                        secondary,
                                        &self.config,
                                        binance_futures_role,
                                        &ctx,
                                        &mut current_symbols,
                                    ).await;
                                    next_secondary_restart = Instant::now() + restart_duration;
                                    Some("secondary")
                                } else {
                                    None
                                };
                                if restarted_label.is_some() {
                                    restart_primary_for_stale = !restart_primary_for_stale;
                                }
                                next_incremental_recovery_restart_at =
                                    Instant::now() + HYPERLIQUID_STALE_RESTART_COOLDOWN;
                            }
                        }
                    }
                }
                _ = tokio::time::sleep_until(next_primary_restart) => {
                    if let Some(primary) = primary.as_mut() {
                        restart_leg(
                            venue_slug,
                            primary,
                            &self.config,
                            binance_futures_role,
                            &ctx,
                            &mut current_symbols,
                        ).await;
                    }
                    next_primary_restart = Instant::now() + restart_duration;
                }
                _ = tokio::time::sleep_until(next_secondary_restart) => {
                    if let Some(secondary) = secondary.as_mut() {
                        restart_leg(
                            venue_slug,
                            secondary,
                            &self.config,
                            binance_futures_role,
                            &ctx,
                            &mut current_symbols,
                        ).await;
                    }
                    next_secondary_restart = Instant::now() + restart_duration;
                }
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_binance_spot_fix_sbe_with_shutdown(
        self,
        adapter: Rc<dyn VenueAdapter>,
        symbols: Vec<String>,
        primary_local_ip: String,
        secondary_local_ip: String,
        publisher: Rc<SpreadPublisher>,
        trade_publisher: Option<Rc<SpreadTradePublisher>>,
        incremental_publisher: Option<Rc<SpreadIncrementalPublisher>>,
        state: Rc<RefCell<SharedState>>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> Result<()> {
        let depth = self
            .config
            .data_types
            .max_levels_per_msg
            .unwrap_or(20)
            .clamp(2, MAX_FIX_SBE_LEVELS) as u16;
        let mut legs = Vec::with_capacity(6);
        for (track, local_ip) in [
            ("primary", primary_local_ip),
            ("secondary", secondary_local_ip),
        ] {
            legs.push(spawn_fix_md_leg(
                format!("{track}-bbo"),
                local_ip.clone(),
                symbols.clone(),
                FixMdStreamKind::Bbo,
                depth,
                make_fix_md_handler(
                    format!("{track}-bbo"),
                    FixMdStreamKind::Bbo,
                    adapter.clone(),
                    Some(publisher.clone()),
                    None,
                    None,
                    self.config.data_types.max_levels_per_msg,
                    state.clone(),
                ),
            ));
            if let Some(trade_publisher) = trade_publisher.as_ref() {
                legs.push(spawn_fix_md_leg(
                    format!("{track}-trade"),
                    local_ip.clone(),
                    symbols.clone(),
                    FixMdStreamKind::Trade,
                    depth,
                    make_fix_md_handler(
                        format!("{track}-trade"),
                        FixMdStreamKind::Trade,
                        adapter.clone(),
                        None,
                        Some(trade_publisher.clone()),
                        None,
                        self.config.data_types.max_levels_per_msg,
                        state.clone(),
                    ),
                ));
            }
            if let Some(incremental_publisher) = incremental_publisher.as_ref() {
                legs.push(spawn_fix_md_leg(
                    format!("{track}-depth"),
                    local_ip,
                    symbols.clone(),
                    FixMdStreamKind::Depth,
                    depth,
                    make_fix_md_handler(
                        format!("{track}-depth"),
                        FixMdStreamKind::Depth,
                        adapter.clone(),
                        None,
                        None,
                        Some(incremental_publisher.clone()),
                        self.config.data_types.max_levels_per_msg,
                        state.clone(),
                    ),
                ));
            }
        }

        log::info!(
            "spread_pbs[binance-margin] FIX/SBE mode started sessions={} symbols={} streams=bbo{}{}",
            legs.len(),
            symbols.len(),
            if trade_publisher.is_some() { ",trade" } else { "" },
            if incremental_publisher.is_some() {
                ",depth"
            } else {
                ""
            },
        );

        let mut stats_ticker = tokio::time::interval(Duration::from_secs(30));
        stats_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        stats_ticker.tick().await;
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        log::info!("spread_pbs[binance-margin] FIX/SBE shutdown requested");
                        shutdown_fix_md_legs(&mut legs).await;
                        break;
                    }
                }
                _ = stats_ticker.tick() => {
                    let shared = state.borrow();
                    log::info!(
                        "spread_pbs[binance-margin] FIX/SBE stats published={} trades_published={} incremental_published={} dropped_by_seq={} trades_dropped_by_seq={} incremental_dropped_by_seq={}",
                        shared.published,
                        shared.trades_published,
                        shared.incremental_published,
                        shared.dropped_by_seq,
                        shared.trades_dropped_by_seq,
                        shared.incremental_dropped_by_seq,
                    );
                }
            }
        }
        Ok(())
    }
}

fn spawn_leg(
    label: impl Into<String>,
    local_ip: String,
    url: String,
    source: MarketSource,
    subs: Vec<serde_json::Value>,
    ctx: &LegCtx,
    publisher: Rc<SpreadPublisher>,
) -> WsLeg {
    let label = label.into();
    let (tx, rx) = watch::channel(false);
    let handler = make_handler(
        label.clone(),
        ctx.adapter.clone(),
        publisher,
        ctx.trade_publisher.clone(),
        ctx.incremental_publisher.clone(),
        ctx.derivatives_publisher.clone(),
        ctx.incremental_max_levels,
        ctx.state.clone(),
        source,
    );
    let handle = tokio::task::spawn_local(run_public_ws_with_ack_policy(
        WsLoopParams {
            label: label.clone(),
            url: url.clone(),
            local_ip: local_ip.clone(),
            remote_ip: None,
            headers: ctx.adapter.ws_headers(),
            subscribe_msgs: subs,
            keepalive: ctx.adapter.keepalive(),
            parse_okex_notices: ctx.parse_okex_notices,
            business_idle_timeout: Some(WS_BUSINESS_IDLE_TIMEOUT),
            rolling_restart: None,
        },
        handler,
        rx,
        ctx.adapter.subscription_ack_policy(),
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

fn spawn_fix_md_leg(
    label: String,
    local_ip: String,
    symbols: Vec<String>,
    kind: FixMdStreamKind,
    depth: u16,
    handler: FrameHandler,
) -> FixMdLeg {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = tokio::task::spawn_local(run_fix_sbe_md(
        FixMdLoopParams {
            label: label.clone(),
            local_ip: local_ip.clone(),
            symbols,
            kind,
            depth,
        },
        handler,
        shutdown_rx,
    ));
    FixMdLeg {
        label,
        kind,
        local_ip,
        shutdown_tx,
        handle,
    }
}

async fn shutdown_fix_md_legs(legs: &mut [FixMdLeg]) {
    for leg in legs.iter() {
        log::info!(
            "spread_pbs fix-md[{}] stopping kind={:?} local_ip={}",
            leg.label,
            leg.kind,
            leg.local_ip,
        );
        let _ = leg.shutdown_tx.send(true);
    }
    for leg in legs.iter_mut() {
        let _ = (&mut leg.handle).await;
    }
}

#[allow(clippy::too_many_arguments)]
fn make_fix_md_handler(
    label: String,
    kind: FixMdStreamKind,
    adapter: Rc<dyn VenueAdapter>,
    publisher: Option<Rc<SpreadPublisher>>,
    trade_publisher: Option<Rc<SpreadTradePublisher>>,
    incremental_publisher: Option<Rc<SpreadIncrementalPublisher>>,
    incremental_max_levels: Option<usize>,
    state: Rc<RefCell<SharedState>>,
) -> FrameHandler {
    let bbo_state = Rc::new(RefCell::new(new_bbo_state()));
    Rc::new(move |recv_us, raw| {
        let mut decoded_bbo_state = bbo_state.borrow_mut();
        let result = decode_market_frame(
            raw,
            kind,
            &mut decoded_bbo_state,
            &mut |event| match event {
                FixSbeMarketEvent::Bbo {
                    symbol,
                    timestamp_us,
                    seq_id,
                    bid_price,
                    bid_amount,
                    ask_price,
                    ask_amount,
                } => {
                    let Some(publisher) = publisher.as_ref() else {
                        return;
                    };
                    let accepted_us = get_timestamp_us();
                    let slot_index = adapter.symbol_slot_index(symbol);
                    process_bbo_fields(
                        &mut state.borrow_mut(),
                        publisher,
                        slot_index,
                        recv_us,
                        accepted_us,
                        symbol,
                        timestamp_us,
                        seq_id,
                        false,
                        bid_price,
                        bid_amount,
                        ask_price,
                        ask_amount,
                        MarketSource::Other,
                    );
                }
                FixSbeMarketEvent::Trade {
                    symbol,
                    timestamp_us,
                    seq_id,
                    trade_id,
                    side,
                    price,
                    amount,
                } => {
                    let Some(trade_publisher) = trade_publisher.as_ref() else {
                        return;
                    };
                    let slot_index = adapter.symbol_slot_index(symbol);
                    process_trade_fields(
                        &mut state.borrow_mut(),
                        trade_publisher,
                        slot_index,
                        adapter.trade_dedup_policy(),
                        symbol,
                        seq_id,
                        trade_id,
                        timestamp_us,
                        side,
                        price,
                        amount,
                    );
                }
                FixSbeMarketEvent::Book {
                    symbol,
                    timestamp_us,
                    seq_id,
                    first_update_id,
                    final_update_id,
                    is_snapshot,
                    bids,
                    asks,
                } => {
                    let Some(incremental_publisher) = incremental_publisher.as_ref() else {
                        return;
                    };
                    let slot_index = adapter.symbol_slot_index(symbol);
                    process_incremental_fields(
                        &mut state.borrow_mut(),
                        incremental_publisher,
                        slot_index,
                        adapter.incremental_dedup_policy(),
                        symbol,
                        timestamp_us,
                        seq_id,
                        first_update_id.saturating_sub(1),
                        first_update_id,
                        final_update_id,
                        false,
                        is_snapshot,
                        bids,
                        asks,
                        incremental_max_levels,
                    );
                }
            },
        );
        if let Err(err) = result {
            log::warn!("spread_pbs fix-md[{}] decode failed: {err:#}", label);
        }
        Ok(())
    })
}

fn spawn_replacement_leg_on_main_ws(
    label: impl Into<String>,
    local_ip: String,
    url: String,
    subs: Vec<serde_json::Value>,
    ctx: &LegCtx,
) -> WsLeg {
    let label = label.into();
    let (tx, rx) = watch::channel(false);
    let handler = make_replacement_handler(
        label.clone(),
        ctx.adapter.clone(),
        ctx.trade_publisher.clone(),
        ctx.incremental_publisher.clone(),
        ctx.derivatives_publisher.clone(),
        ctx.incremental_max_levels,
        ctx.state.clone(),
    );
    let handle = tokio::task::spawn_local(run_public_ws_with_ack_policy(
        WsLoopParams {
            label: label.clone(),
            url: url.clone(),
            local_ip: local_ip.clone(),
            remote_ip: None,
            headers: ctx.adapter.ws_headers(),
            subscribe_msgs: subs,
            keepalive: ctx.adapter.keepalive(),
            parse_okex_notices: ctx.parse_okex_notices,
            business_idle_timeout: Some(WS_BUSINESS_IDLE_TIMEOUT),
            rolling_restart: None,
        },
        handler,
        rx,
        ctx.adapter.subscription_ack_policy(),
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
            label: "okex-derivatives".to_string(),
            url: OKEX_PUBLIC_WS_URL.to_string(),
            local_ip: local_ip.clone(),
            remote_ip: None,
            headers: Vec::new(),
            subscribe_msgs,
            keepalive: Some(KeepaliveSpec::text(Duration::from_secs(25), "ping")),
            parse_okex_notices: true,
            business_idle_timeout: None,
            rolling_restart: None,
        },
        handler,
        rx,
    ));
    WsLeg {
        label: "okex-derivatives".to_string(),
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
            Duration::from_secs(config.restart_duration_secs),
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
            Duration::from_secs(config.restart_duration_secs),
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
            Duration::from_secs(config.restart_duration_secs),
            state.clone(),
        );
    }

    legs
}

fn spawn_direct_replacement_batch_legs(
    legs: &mut Vec<WsLeg>,
    label: &str,
    url: String,
    local_ip: String,
    subscribe_msgs: Vec<serde_json::Value>,
    adapter: &Rc<dyn VenueAdapter>,
    trade_publisher: Option<Rc<SpreadTradePublisher>>,
    incremental_publisher: Option<Rc<SpreadIncrementalPublisher>>,
    derivatives_publisher: Option<Rc<SpreadDerivativesPublisher>>,
    incremental_max_levels: Option<usize>,
    rolling_restart_interval: Duration,
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
    for (batch_index, subscribe_msg) in subscribe_msgs.into_iter().enumerate() {
        for (replica_index, replica) in ["a", "b"].into_iter().enumerate() {
            let leg_label = format!("{label}-{batch_index}-{replica}");
            let first_after = if replica_index == 0 {
                rolling_restart_interval
            } else {
                rolling_restart_interval / 2
            };
            legs.push(spawn_direct_replacement_leg(
                leg_label,
                url.clone(),
                local_ip.clone(),
                vec![subscribe_msg.clone()],
                adapter.clone(),
                trade_publisher.clone(),
                incremental_publisher.clone(),
                derivatives_publisher.clone(),
                incremental_max_levels,
                Some(RollingRestartSpec {
                    interval: rolling_restart_interval,
                    first_after,
                }),
                state.clone(),
            ));
        }
    }
}

fn spawn_direct_replacement_leg(
    label: String,
    url: String,
    local_ip: String,
    subscribe_msgs: Vec<serde_json::Value>,
    adapter: Rc<dyn VenueAdapter>,
    trade_publisher: Option<Rc<SpreadTradePublisher>>,
    incremental_publisher: Option<Rc<SpreadIncrementalPublisher>>,
    derivatives_publisher: Option<Rc<SpreadDerivativesPublisher>>,
    incremental_max_levels: Option<usize>,
    rolling_restart: Option<RollingRestartSpec>,
    state: Rc<RefCell<SharedState>>,
) -> WsLeg {
    let business_idle_timeout = derivatives_publisher
        .is_none()
        .then_some(WS_BUSINESS_IDLE_TIMEOUT);
    let (tx, rx) = watch::channel(false);
    let handler = make_replacement_handler(
        label.clone(),
        adapter.clone(),
        trade_publisher,
        incremental_publisher,
        derivatives_publisher,
        incremental_max_levels,
        state,
    );
    let handle = tokio::task::spawn_local(run_public_ws_with_ack_policy(
        WsLoopParams {
            label: label.clone(),
            url: url.clone(),
            local_ip: local_ip.clone(),
            remote_ip: None,
            headers: adapter.ws_headers(),
            subscribe_msgs,
            keepalive: adapter.keepalive(),
            parse_okex_notices: adapter.name() == "okex",
            business_idle_timeout,
            rolling_restart,
        },
        handler,
        rx,
        adapter.subscription_ack_policy(),
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
            Err(_) => return Ok(()),
        };
        let bytes = {
            let symbols = active_symbols.borrow();
            parse_okex_derivatives_frame(&value, &symbols)
        };
        if bytes.is_empty() {
            return Ok(());
        }
        let mut s = state.borrow_mut();
        for msg in bytes {
            if let Err(e) = publisher.publish(&msg) {
                log::warn!("spread_pbs derivatives publish failed: {:#}", e);
                continue;
            }
            s.derivatives_published += 1;
        }
        Ok(())
    })
}

async fn restart_leg(
    venue_slug: &'static str,
    leg: &mut WsLeg,
    config: &Config,
    binance_futures_role: BinanceFuturesRole,
    ctx: &LegCtx,
    current_symbols: &mut HashSet<String>,
) {
    log::info!("spread_pbs[{}] leg={} restart begin", venue_slug, leg.label);

    // 先拉新 symbol；失败/空一律保留旧 leg，不重启。
    let new_symbols = match get_symbols_for_role(config, binance_futures_role).await {
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
            leg.label.clone(),
            ENV_SYMBOLS
        );
        return;
    }
    let new_subs = build_market_subscribe(
        &ctx.adapter,
        &new_symbols,
        ctx.publisher.is_some(),
        ctx.trade_publisher.is_some(),
        ctx.incremental_publisher.is_some(),
        ctx.derivatives_publisher.is_some(),
    );
    if new_subs.is_empty() {
        log::error!(
            "spread_pbs[{}] leg={} restart skipped: adapter.build_subscribe empty (symbols={})",
            venue_slug,
            leg.label.clone(),
            new_symbols.len()
        );
        return;
    }
    let new_subs_len = new_subs.len();

    if let Some(budget) = ctx.hyperliquid_subscription_budget.as_ref() {
        if let Err(err) = budget.grow(new_subs_len) {
            log::error!(
                "spread_pbs[{}] leg={} symbol refresh rejected; retaining existing subscriptions: {err:#}",
                venue_slug,
                leg.label,
            );
            return;
        }
    }

    let new_set: HashSet<String> = new_symbols.iter().cloned().collect();
    let added = new_set.difference(current_symbols).count();
    let removed = current_symbols.difference(&new_set).count();
    if matches!(
        config.venue,
        TradingVenue::HyperliquidMargin | TradingVenue::HyperliquidFutures
    ) {
        ctx.adapter.seed_symbols(&new_symbols);
    }
    if added > 0 {
        let mut s = ctx.state.borrow_mut();
        s.symbol_state.ensure_symbols(&new_symbols);
        ctx.adapter.seed_symbols(&new_symbols);
        if let Some(publisher) = ctx.publisher.as_ref() {
            if let Err(e) = publisher.seed_symbols(&new_symbols) {
                log::warn!(
                    "spread_pbs[{}] failed to seed BBO payload prefixes: {:#}",
                    venue_slug,
                    e
                );
            }
        }
        if let Some(trade_publisher) = ctx.trade_publisher.as_ref() {
            if let Err(e) = trade_publisher.seed_symbols(&new_symbols) {
                log::warn!(
                    "spread_pbs[{}] failed to seed trade payload prefixes: {:#}",
                    venue_slug,
                    e
                );
            }
        }
        if let Some(incremental_publisher) = ctx.incremental_publisher.as_ref() {
            if let Err(e) = incremental_publisher.seed_symbols(&new_symbols) {
                log::warn!(
                    "spread_pbs[{}] failed to seed incremental payload prefixes: {:#}",
                    venue_slug,
                    e
                );
            }
        }
        if let Some(derivatives_publisher) = ctx.derivatives_publisher.as_ref() {
            if let Err(e) = derivatives_publisher.seed_symbols(&new_symbols) {
                log::warn!(
                    "spread_pbs[{}] failed to seed derivatives payload prefixes: {:#}",
                    venue_slug,
                    e
                );
            }
        }
    }
    *current_symbols = new_set;

    // 关旧、等真正退出，再起新。错开半周期保证此刻另一条 leg 仍在工作。
    let _ = leg.shutdown_tx.send(true);
    let _ = (&mut leg.handle).await;

    let new_leg = if let Some(publisher) = ctx.publisher.as_ref().cloned() {
        spawn_leg(
            leg.label.clone(),
            leg.local_ip.clone(),
            leg.url.clone(),
            leg.source,
            new_subs,
            ctx,
            publisher,
        )
    } else {
        spawn_replacement_leg_on_main_ws(
            leg.label.clone(),
            leg.local_ip.clone(),
            leg.url.clone(),
            new_subs,
            ctx,
        )
    };
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

fn wall_clock_delta_us(
    previous_wall_us: i64,
    current_wall_us: i64,
    monotonic_elapsed: Duration,
) -> i128 {
    (current_wall_us as i128 - previous_wall_us as i128) - monotonic_elapsed.as_micros() as i128
}

fn stream_age_ms(last_seen_at: Option<Instant>, now: Instant) -> String {
    last_seen_at
        .map(|last_seen| now.duration_since(last_seen).as_millis().to_string())
        .unwrap_or_else(|| "never".to_string())
}

struct SymbolSeqState {
    index_by_symbol: FastHashMap<String, usize>,
    bbo_seq: Vec<i64>,
    bbo_ts_us: Vec<i64>,
    trade_seq: Vec<i64>,
    trade_last_seen_at: Vec<Option<Instant>>,
    incremental_seq: Vec<i64>,
    incremental_last_seen_at: Vec<Option<Instant>>,
    bbo_seen: usize,
    trade_seen: usize,
    incremental_seen: usize,
}

impl SymbolSeqState {
    fn with_symbols(symbols: &[String]) -> Self {
        let mut state = Self {
            index_by_symbol: fast_hash_map_with_capacity(symbols.len().max(2048)),
            bbo_seq: Vec::with_capacity(symbols.len()),
            bbo_ts_us: Vec::with_capacity(symbols.len()),
            trade_seq: Vec::with_capacity(symbols.len()),
            trade_last_seen_at: Vec::with_capacity(symbols.len()),
            incremental_seq: Vec::with_capacity(symbols.len()),
            incremental_last_seen_at: Vec::with_capacity(symbols.len()),
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
        self.trade_seq.push(i64::MIN);
        self.trade_last_seen_at.push(None);
        self.incremental_seq.push(i64::MIN);
        self.incremental_last_seen_at.push(None);
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
        self.bbo_slot_by_index(idx)
    }

    fn bbo_slot_by_index(&self, idx: usize) -> SymbolSlot {
        SymbolSlot {
            idx,
            prev: self.bbo_seq[idx],
            prev_ts_us: self.bbo_ts_us[idx],
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
        self.trade_slot_by_index(idx)
    }

    fn trade_slot_by_index(&self, idx: usize) -> SymbolSlot {
        SymbolSlot {
            idx,
            prev: self.trade_seq[idx],
            prev_ts_us: 0,
        }
    }

    fn set_trade_slot(&mut self, slot: SymbolSlot, seq_id: i64) {
        self.set_trade_slot_at(slot, seq_id, Instant::now());
    }

    fn set_trade_slot_at(&mut self, slot: SymbolSlot, seq_id: i64, seen_at: Instant) {
        if slot.prev == i64::MIN {
            self.trade_seen += 1;
        }
        self.trade_seq[slot.idx] = seq_id;
        self.trade_last_seen_at[slot.idx] = Some(seen_at);
    }

    fn incremental_slot(&mut self, symbol: &str) -> SymbolSlot {
        let idx = self.ensure_symbol(symbol);
        self.incremental_slot_by_index(idx)
    }

    fn incremental_slot_by_index(&self, idx: usize) -> SymbolSlot {
        SymbolSlot {
            idx,
            prev: self.incremental_seq[idx],
            prev_ts_us: 0,
        }
    }

    #[cfg(test)]
    fn incremental_prev_seen(&mut self, symbol: &str) -> Option<i64> {
        let slot = self.incremental_slot(symbol);
        (slot.prev != i64::MIN).then_some(slot.prev)
    }

    fn set_incremental_slot(&mut self, slot: SymbolSlot, seq_id: i64) {
        self.set_incremental_slot_at(slot, seq_id, Instant::now());
    }

    fn set_incremental_slot_at(&mut self, slot: SymbolSlot, seq_id: i64, seen_at: Instant) {
        if slot.prev == i64::MIN {
            self.incremental_seen += 1;
        }
        self.incremental_seq[slot.idx] = seq_id;
        self.incremental_last_seen_at[slot.idx] = Some(seen_at);
    }

    fn stale_incremental_symbols(
        &self,
        critical_symbols: &[&str],
        now: Instant,
        stale_after: Duration,
    ) -> Vec<String> {
        critical_symbols
            .iter()
            .filter_map(|symbol| {
                let idx = *self.index_by_symbol.get(*symbol)?;
                self.incremental_last_seen_at[idx]
                    .map_or(true, |last_seen| {
                        now.duration_since(last_seen) > stale_after
                    })
                    .then(|| (*symbol).to_string())
            })
            .collect()
    }

    fn critical_stream_age_summary(&self, critical_symbols: &[&str], now: Instant) -> String {
        critical_symbols
            .iter()
            .filter_map(|symbol| {
                let idx = *self.index_by_symbol.get(*symbol)?;
                let incremental_age = stream_age_ms(self.incremental_last_seen_at[idx], now);
                let trade_age = stream_age_ms(self.trade_last_seen_at[idx], now);
                Some(format!(
                    "{}:incremental_age_ms={}:trade_age_ms={}",
                    symbol, incremental_age, trade_age
                ))
            })
            .collect::<Vec<_>>()
            .join(",")
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
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct BboIdentity {
    timestamp_us: i64,
    bid_price_bits: u64,
    bid_amount_bits: u64,
    ask_price_bits: u64,
    ask_amount_bits: u64,
}

impl BboIdentity {
    fn new(
        timestamp_us: i64,
        bid_price: f64,
        bid_amount: f64,
        ask_price: f64,
        ask_amount: f64,
    ) -> Self {
        Self {
            timestamp_us,
            bid_price_bits: bid_price.to_bits(),
            bid_amount_bits: bid_amount.to_bits(),
            ask_price_bits: ask_price.to_bits(),
            ask_amount_bits: ask_amount.to_bits(),
        }
    }
}

struct RecentBboIdentities {
    identities: FastHashSet<BboIdentity>,
    order: VecDeque<BboIdentity>,
}

impl RecentBboIdentities {
    fn new() -> Self {
        Self {
            identities: fast_hash_set_with_capacity(BBO_IDENTITY_DEDUP_PER_SYMBOL),
            order: VecDeque::with_capacity(BBO_IDENTITY_DEDUP_PER_SYMBOL),
        }
    }

    fn contains(&self, identity: &BboIdentity) -> bool {
        self.identities.contains(identity)
    }

    fn insert(&mut self, identity: BboIdentity) {
        if !self.identities.insert(identity) {
            return;
        }
        self.order.push_back(identity);
        if self.order.len() > BBO_IDENTITY_DEDUP_PER_SYMBOL {
            if let Some(expired) = self.order.pop_front() {
                self.identities.remove(&expired);
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct TradeIdentity {
    timestamp_us: i64,
    trade_id: i64,
}

struct RecentTradeIdentities {
    identities: FastHashSet<TradeIdentity>,
    order: VecDeque<TradeIdentity>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct IncrementalLevelIdentity {
    price_bits: u64,
    amount_bits: u64,
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct IncrementalSnapshotIdentity {
    timestamp_us: i64,
    bids: Vec<IncrementalLevelIdentity>,
    asks: Vec<IncrementalLevelIdentity>,
}

impl IncrementalSnapshotIdentity {
    fn from_levels<L: PayloadLevel>(timestamp_us: i64, bids: &[L], asks: &[L]) -> Self {
        let convert = |level: &L| IncrementalLevelIdentity {
            price_bits: level.price().to_bits(),
            amount_bits: level.amount().to_bits(),
        };
        Self {
            timestamp_us,
            bids: bids.iter().map(convert).collect(),
            asks: asks.iter().map(convert).collect(),
        }
    }
}

struct RecentIncrementalIdentities {
    identities: FastHashSet<Rc<IncrementalSnapshotIdentity>>,
    order: VecDeque<Rc<IncrementalSnapshotIdentity>>,
}

impl RecentIncrementalIdentities {
    fn new() -> Self {
        Self {
            identities: fast_hash_set_with_capacity(INCREMENTAL_IDENTITY_DEDUP_PER_SYMBOL),
            order: VecDeque::with_capacity(INCREMENTAL_IDENTITY_DEDUP_PER_SYMBOL),
        }
    }

    fn contains(&self, identity: &Rc<IncrementalSnapshotIdentity>) -> bool {
        self.identities.contains(identity)
    }

    fn insert(&mut self, identity: Rc<IncrementalSnapshotIdentity>) {
        if !self.identities.insert(Rc::clone(&identity)) {
            return;
        }
        self.order.push_back(identity);
        if self.order.len() > INCREMENTAL_IDENTITY_DEDUP_PER_SYMBOL {
            if let Some(expired) = self.order.pop_front() {
                self.identities.remove(&expired);
            }
        }
    }
}

impl RecentTradeIdentities {
    fn new() -> Self {
        Self {
            identities: fast_hash_set_with_capacity(TRADE_IDENTITY_DEDUP_PER_SYMBOL),
            order: VecDeque::with_capacity(TRADE_IDENTITY_DEDUP_PER_SYMBOL),
        }
    }

    fn contains(&self, identity: &TradeIdentity) -> bool {
        self.identities.contains(identity)
    }

    fn insert(&mut self, identity: TradeIdentity) {
        if !self.identities.insert(identity) {
            return;
        }
        self.order.push_back(identity);
        if self.order.len() > TRADE_IDENTITY_DEDUP_PER_SYMBOL {
            if let Some(expired) = self.order.pop_front() {
                self.identities.remove(&expired);
            }
        }
    }
}

struct SharedState {
    symbol_state: SymbolSeqState,
    bbo_dedup_policy: BboDedupPolicy,
    // Hyperliquid replays recent public trades in the first subscription batch
    // without an isSnapshot marker. Keep only trades at/after this process run.
    trade_publish_not_before_us: Option<i64>,
    published: u64,
    trades_published: u64,
    incremental_published: u64,
    incremental_dropped_by_seq: u64,
    incremental_gap_warnings: u64,
    derivatives_published: u64,
    derivatives_dropped_duplicate: u64,
    derivatives_recent: FastHashMap<Vec<u8>, i64>,
    derivatives_since_prune: usize,
    recent_bbo_identities: Vec<RecentBboIdentities>,
    recent_trade_identities: Vec<RecentTradeIdentities>,
    recent_incremental_identities: Vec<RecentIncrementalIdentities>,
    selected_whitelist: u64,
    selected_normal: u64,
    selected_other: u64,
    dropped_by_seq: u64,
    trades_dropped_by_seq: u64,
    last_dedup_reset_us: i64,
}

impl SharedState {
    fn contains_recent_bbo(&self, symbol_slot: usize, identity: &BboIdentity) -> bool {
        self.recent_bbo_identities
            .get(symbol_slot)
            .is_some_and(|recent| recent.contains(identity))
    }

    fn record_recent_bbo(&mut self, symbol_slot: usize, identity: BboIdentity) {
        while self.recent_bbo_identities.len() <= symbol_slot {
            self.recent_bbo_identities.push(RecentBboIdentities::new());
        }
        self.recent_bbo_identities[symbol_slot].insert(identity);
    }

    fn contains_recent_trade(&self, symbol_slot: usize, timestamp_us: i64, trade_id: i64) -> bool {
        self.recent_trade_identities
            .get(symbol_slot)
            .is_some_and(|recent| {
                recent.contains(&TradeIdentity {
                    timestamp_us,
                    trade_id,
                })
            })
    }

    fn record_recent_trade(&mut self, symbol_slot: usize, timestamp_us: i64, trade_id: i64) {
        while self.recent_trade_identities.len() <= symbol_slot {
            self.recent_trade_identities
                .push(RecentTradeIdentities::new());
        }
        self.recent_trade_identities[symbol_slot].insert(TradeIdentity {
            timestamp_us,
            trade_id,
        });
    }

    fn contains_recent_incremental(
        &self,
        symbol_slot: usize,
        identity: &Rc<IncrementalSnapshotIdentity>,
    ) -> bool {
        self.recent_incremental_identities
            .get(symbol_slot)
            .is_some_and(|recent| recent.contains(identity))
    }

    fn record_recent_incremental(
        &mut self,
        symbol_slot: usize,
        identity: Rc<IncrementalSnapshotIdentity>,
    ) {
        while self.recent_incremental_identities.len() <= symbol_slot {
            self.recent_incremental_identities
                .push(RecentIncrementalIdentities::new());
        }
        self.recent_incremental_identities[symbol_slot].insert(identity);
    }

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
    label: &str,
    adapter: &dyn VenueAdapter,
    raw: &[u8],
    include_bbo: bool,
    include_trade: bool,
    include_incremental: bool,
    include_derivatives: bool,
    emit_bbo: &mut dyn FnMut(BboFrame) -> Result<()>,
) -> Result<ReplacementBatch> {
    if looks_like_json(raw) {
        if include_bbo {
            match adapter.parse_bbo_raw(raw, emit_bbo) {
                Ok(true) => return Ok(ReplacementBatch::default()),
                Ok(false) => {}
                Err(e) => {
                    handle_adapter_parse_error(label, adapter, "parse_bbo_raw", e, None)?;
                }
            }
        }
        if adapter.skip_json_fallback_after_raw_miss() {
            return Ok(ReplacementBatch::default());
        }
        match serde_json::from_slice::<serde_json::Value>(raw) {
            Ok(value) => {
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
            Err(err) if adapter.reconnect_on_parse_error() => {
                adapter.on_fatal_parse_error();
                return Err(err).with_context(|| {
                    format!("spread_pbs[{label}] malformed JSON market-data frame")
                });
            }
            Err(_) => {}
        }
    }
    Ok(parse_binary_replacement_batch(
        label,
        adapter,
        raw,
        include_bbo,
        include_trade,
        include_incremental,
        include_derivatives,
        emit_bbo,
    ))
}

fn handle_adapter_parse_error(
    label: &str,
    adapter: &dyn VenueAdapter,
    operation: &str,
    error: anyhow::Error,
    payload: Option<&serde_json::Value>,
) -> Result<()> {
    if let Some(payload) = payload {
        log::error!(
            "spread_pbs[{}] adapter.{} failed: {:#} payload={}",
            label,
            operation,
            error,
            payload
        );
    } else {
        log::error!(
            "spread_pbs[{}] adapter.{} failed: {:#}",
            label,
            operation,
            error
        );
    }
    if adapter.reconnect_on_parse_error() {
        adapter.on_fatal_parse_error();
        return Err(error)
            .with_context(|| format!("spread_pbs[{label}] fatal adapter.{operation} failure"));
    }
    Ok(())
}

fn looks_like_json(raw: &[u8]) -> bool {
    matches!(
        raw.iter().copied().find(|b| !b.is_ascii_whitespace()),
        Some(b'{' | b'[')
    )
}

fn should_drop_json_after_raw_miss(adapter: &dyn VenueAdapter, raw: &[u8]) -> bool {
    adapter.skip_json_fallback_after_raw_miss() && looks_like_json(raw)
}

fn parse_json_replacement_batch(
    label: &str,
    adapter: &dyn VenueAdapter,
    value: &serde_json::Value,
    include_bbo: bool,
    include_trade: bool,
    include_incremental: bool,
    include_derivatives: bool,
    emit_bbo: &mut dyn FnMut(BboFrame) -> Result<()>,
) -> Result<ReplacementBatch> {
    if include_bbo {
        match adapter.parse_frame(value, emit_bbo) {
            Ok(()) => {}
            Err(e) => {
                handle_adapter_parse_error(label, adapter, "parse_frame", e, Some(value))?;
            }
        }
    }
    let trades = if include_trade {
        match adapter.parse_trade_frame(value) {
            Ok(v) => v,
            Err(e) => {
                handle_adapter_parse_error(label, adapter, "parse_trade_frame", e, Some(value))?;
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
                handle_adapter_parse_error(
                    label,
                    adapter,
                    "parse_incremental_frame",
                    e,
                    Some(value),
                )?;
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
                handle_adapter_parse_error(
                    label,
                    adapter,
                    "parse_derivatives_frame",
                    e,
                    Some(value),
                )?;
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };

    Ok(ReplacementBatch {
        trades,
        incrementals,
        derivatives,
    })
}

fn parse_binary_replacement_batch(
    label: &str,
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
    label: String,
    adapter: Rc<dyn VenueAdapter>,
    trade_publisher: Option<Rc<SpreadTradePublisher>>,
    incremental_publisher: Option<Rc<SpreadIncrementalPublisher>>,
    derivatives_publisher: Option<Rc<SpreadDerivativesPublisher>>,
    incremental_max_levels: Option<usize>,
    state: Rc<RefCell<SharedState>>,
) -> FrameHandler {
    Rc::new(move |_recv_us: i64, raw: &[u8]| {
        if process_raw_replacement_frame(
            adapter.as_ref(),
            raw,
            trade_publisher.as_ref(),
            incremental_publisher.as_ref(),
            derivatives_publisher.as_ref(),
            incremental_max_levels,
            &state,
        ) {
            return Ok(());
        }
        if should_drop_json_after_raw_miss(adapter.as_ref(), raw) {
            return Ok(());
        }

        let mut emit_noop = |_frame: BboFrame| Ok(());
        let batch = parse_replacement_batch(
            &label,
            adapter.as_ref(),
            raw,
            false,
            trade_publisher.is_some(),
            incremental_publisher.is_some(),
            derivatives_publisher.is_some(),
            &mut emit_noop,
        )?;
        if batch.is_empty() {
            return Ok(());
        }
        let mut s = state.borrow_mut();
        if let Some(trade_publisher) = trade_publisher.as_ref() {
            for trade in batch.trades {
                let slot_index = adapter.symbol_slot_index(&trade.symbol);
                process_trade_frame(
                    &mut s,
                    trade_publisher,
                    slot_index,
                    adapter.trade_dedup_policy(),
                    trade,
                );
            }
        }
        if let Some(incremental_publisher) = incremental_publisher.as_ref() {
            for incremental in batch.incrementals {
                let slot_index = adapter.symbol_slot_index(incremental_frame_symbol(&incremental));
                process_incremental_frame(
                    &mut s,
                    incremental_publisher,
                    slot_index,
                    adapter.incremental_dedup_policy(),
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
        Ok(())
    })
}

fn process_raw_replacement_frame(
    adapter: &dyn VenueAdapter,
    raw: &[u8],
    trade_publisher: Option<&Rc<SpreadTradePublisher>>,
    incremental_publisher: Option<&Rc<SpreadIncrementalPublisher>>,
    derivatives_publisher: Option<&Rc<SpreadDerivativesPublisher>>,
    incremental_max_levels: Option<usize>,
    state: &Rc<RefCell<SharedState>>,
) -> bool {
    if let Some(publisher) = derivatives_publisher {
        let mut symbol_slot = |symbol: &str| adapter.symbol_slot_index(symbol);
        if let Some(encoded) = adapter.parse_derivatives_raw(raw, &mut symbol_slot) {
            let mut s = state.borrow_mut();
            for bytes in encoded {
                process_derivatives_bytes(&mut s, publisher, &bytes);
            }
            return true;
        }
    }

    if let Some(publisher) = incremental_publisher {
        if let Some(book) = adapter.parse_incremental_raw(raw) {
            match book {
                RawIncremental::Parsed(book) => {
                    let slot_index = adapter.symbol_slot_index(book.symbol);
                    let mut s = state.borrow_mut();
                    process_incremental_fields(
                        &mut s,
                        publisher,
                        slot_index,
                        adapter.incremental_dedup_policy(),
                        book.symbol,
                        book.timestamp_us,
                        book.seq_id,
                        book.prev_seq_id,
                        book.first_update_id,
                        book.final_update_id,
                        book.gap_check,
                        book.is_snapshot,
                        book.bids.as_slice(),
                        book.asks.as_slice(),
                        incremental_max_levels,
                    );
                }
                RawIncremental::View(book) => {
                    let slot_index = adapter.symbol_slot_index(book.symbol);
                    let mut s = state.borrow_mut();
                    process_incremental_view(
                        &mut s,
                        publisher,
                        slot_index,
                        adapter.incremental_dedup_policy(),
                        book,
                        incremental_max_levels,
                    );
                }
            }
            return true;
        }
    }

    if let Some(publisher) = trade_publisher {
        let mut emit = |trade: crate::spread_pbs::adapter::RawTradeFrame<'_>| {
            let slot_index = adapter.symbol_slot_index(trade.symbol);
            let mut s = state.borrow_mut();
            process_trade_fields(
                &mut s,
                publisher,
                slot_index,
                adapter.trade_dedup_policy(),
                trade.symbol,
                trade.seq_id,
                trade.trade_id,
                trade.timestamp_us,
                trade.side,
                trade.price,
                trade.amount,
            );
        };
        if adapter.parse_trades_raw_borrowed(raw, &mut emit) {
            return true;
        }
    }

    false
}

fn make_handler(
    label: String,
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
        if let Some(bbo) = adapter.parse_bbo_raw_borrowed(raw) {
            let accepted_us = get_timestamp_us();
            let slot_index = adapter.symbol_slot_index(bbo.symbol);
            let mut s = state.borrow_mut();
            process_bbo_fields(
                &mut s,
                &publisher,
                slot_index,
                recv_us,
                accepted_us,
                bbo.symbol,
                bbo.timestamp_us,
                bbo.seq_id,
                bbo.reset_seq,
                bbo.bid_price,
                bbo.bid_amount,
                bbo.ask_price,
                bbo.ask_amount,
                source,
            );
            return Ok(());
        }
        if process_raw_replacement_frame(
            adapter.as_ref(),
            raw,
            trade_publisher.as_ref(),
            incremental_publisher.as_ref(),
            derivatives_publisher.as_ref(),
            incremental_max_levels,
            &state,
        ) {
            return Ok(());
        }
        if should_drop_json_after_raw_miss(adapter.as_ref(), raw) {
            return Ok(());
        }

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
            &label,
            adapter.as_ref(),
            raw,
            true,
            trade_publisher.is_some(),
            incremental_publisher.is_some(),
            derivatives_publisher.is_some(),
            &mut emit_bbo,
        )?;
        drop(emit_bbo);
        if bbo_count == 0 && batch.is_empty() {
            return Ok(());
        }
        if let Some(trade_publisher) = trade_publisher.as_ref() {
            for trade in batch.trades {
                let slot_index = adapter.symbol_slot_index(&trade.symbol);
                process_trade_frame(
                    &mut s,
                    trade_publisher,
                    slot_index,
                    adapter.trade_dedup_policy(),
                    trade,
                );
            }
        }
        if let Some(incremental_publisher) = incremental_publisher.as_ref() {
            for incremental in batch.incrementals {
                let slot_index = adapter.symbol_slot_index(incremental_frame_symbol(&incremental));
                process_incremental_frame(
                    &mut s,
                    incremental_publisher,
                    slot_index,
                    adapter.incremental_dedup_policy(),
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
        Ok(())
    })
}

fn process_trade_frame(
    state: &mut SharedState,
    publisher: &Rc<SpreadTradePublisher>,
    slot_index: Option<usize>,
    dedup_policy: TradeDedupPolicy,
    f: TradeFrame,
) {
    process_trade_fields(
        state,
        publisher,
        slot_index,
        dedup_policy,
        &f.symbol,
        f.seq_id,
        f.trade_id,
        f.timestamp_us,
        f.side,
        f.price,
        f.amount,
    );
}

fn should_drop_trade(
    state: &mut SharedState,
    slot: &SymbolSlot,
    seq_id: i64,
    trade_id: i64,
    timestamp_us: i64,
    policy: TradeDedupPolicy,
) -> bool {
    match policy {
        TradeDedupPolicy::MonotonicSequence => seq_id <= slot.prev,
        TradeDedupPolicy::RecentIdentity => {
            state
                .trade_publish_not_before_us
                .is_some_and(|cutoff_us| timestamp_us < cutoff_us)
                || state.contains_recent_trade(slot.idx, timestamp_us, trade_id)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn process_trade_fields(
    state: &mut SharedState,
    publisher: &Rc<SpreadTradePublisher>,
    slot_index: Option<usize>,
    dedup_policy: TradeDedupPolicy,
    symbol: &str,
    seq_id: i64,
    trade_id: i64,
    timestamp_us: i64,
    side: char,
    price: f64,
    amount: f64,
) {
    let slot = slot_index
        .map(|idx| state.symbol_state.trade_slot_by_index(idx))
        .unwrap_or_else(|| state.symbol_state.trade_slot(symbol));
    if should_drop_trade(state, &slot, seq_id, trade_id, timestamp_us, dedup_policy) {
        state.trades_dropped_by_seq += 1;
        return;
    }
    let publish_result =
        publisher.publish_trade(symbol, trade_id, timestamp_us, side, price, amount);
    if let Err(e) = publish_result {
        log::warn!("spread_pbs trade publish failed: {:#}", e);
        return;
    }
    if dedup_policy == TradeDedupPolicy::RecentIdentity {
        state.record_recent_trade(slot.idx, timestamp_us, trade_id);
    }
    state.symbol_state.set_trade_slot(slot, seq_id);
    state.trades_published += 1;
}

fn should_drop_incremental(
    slot: &SymbolSlot,
    seq_id: i64,
    is_snapshot: bool,
    policy: IncrementalDedupPolicy,
) -> bool {
    match policy {
        IncrementalDedupPolicy::SnapshotCanReset => !is_snapshot && seq_id <= slot.prev,
        IncrementalDedupPolicy::MonotonicIncludingSnapshots => seq_id <= slot.prev,
        IncrementalDedupPolicy::RecentSnapshotIdentity => seq_id < slot.prev,
    }
}

fn incremental_frame_symbol(frame: &IncrementalFrame) -> &str {
    match frame {
        IncrementalFrame::Book { symbol, .. } | IncrementalFrame::SequenceOnly { symbol, .. } => {
            symbol
        }
    }
}

fn process_incremental_frame(
    state: &mut SharedState,
    publisher: &Rc<SpreadIncrementalPublisher>,
    slot_index: Option<usize>,
    dedup_policy: IncrementalDedupPolicy,
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
            let slot = slot_index
                .map(|idx| state.symbol_state.incremental_slot_by_index(idx))
                .unwrap_or_else(|| state.symbol_state.incremental_slot(&symbol));
            if should_drop_incremental(&slot, seq_id, false, dedup_policy) {
                state.incremental_dropped_by_seq += 1;
                let _ = timestamp;
                return;
            }
            warn_incremental_gap_if_needed(state, &symbol, slot.prev, seq_id, prev_seq_id, false);
            state.symbol_state.set_incremental_slot(slot, seq_id);
            let _ = timestamp;
            return;
        }
    };

    process_incremental_fields(
        state,
        publisher,
        slot_index,
        dedup_policy,
        &symbol,
        timestamp,
        seq_id,
        prev_seq_id,
        first_update_id,
        final_update_id,
        gap_check,
        is_snapshot,
        &bids,
        &asks,
        max_levels,
    );
}

#[allow(clippy::too_many_arguments)]
fn process_incremental_fields<L: PayloadLevel>(
    state: &mut SharedState,
    publisher: &Rc<SpreadIncrementalPublisher>,
    slot_index: Option<usize>,
    dedup_policy: IncrementalDedupPolicy,
    symbol: &str,
    timestamp: i64,
    seq_id: i64,
    prev_seq_id: i64,
    first_update_id: i64,
    final_update_id: i64,
    gap_check: bool,
    is_snapshot: bool,
    bids: &[L],
    asks: &[L],
    max_levels: Option<usize>,
) {
    let slot = slot_index
        .map(|idx| state.symbol_state.incremental_slot_by_index(idx))
        .unwrap_or_else(|| state.symbol_state.incremental_slot(symbol));
    let recent_identity =
        (dedup_policy == IncrementalDedupPolicy::RecentSnapshotIdentity).then(|| {
            Rc::new(IncrementalSnapshotIdentity::from_levels(
                timestamp, bids, asks,
            ))
        });
    if should_drop_incremental(&slot, seq_id, is_snapshot, dedup_policy)
        || recent_identity
            .as_ref()
            .is_some_and(|identity| state.contains_recent_incremental(slot.idx, identity))
    {
        state.incremental_dropped_by_seq += 1;
        return;
    }

    if gap_check {
        warn_incremental_gap_if_needed(state, symbol, slot.prev, seq_id, prev_seq_id, is_snapshot);
    }

    let total_levels = bids.len() + asks.len();
    match max_levels {
        Some(max) if total_levels > max && max > 0 => {
            let total_chunks = level_chunk_count(bids.len(), asks.len(), max);
            let mut bids_start = 0usize;
            let mut asks_start = 0usize;
            for chunk_idx in 0..total_chunks {
                let (bids_count, asks_count) =
                    next_level_chunk(bids.len() - bids_start, asks.len() - asks_start, max);
                if !publish_incremental_chunk(
                    state,
                    publisher,
                    slot_index,
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
                    return;
                }
                bids_start += bids_count;
                asks_start += asks_count;
            }
        }
        _ => {
            if !publish_incremental_chunk(
                state,
                publisher,
                slot_index,
                symbol,
                first_update_id,
                final_update_id,
                timestamp,
                is_snapshot,
                bids,
                0,
                bids.len(),
                asks,
                0,
                asks.len(),
                0,
                1,
            ) {
                return;
            }
        }
    }
    if let Some(identity) = recent_identity {
        state.record_recent_incremental(slot.idx, identity);
    }
    state.symbol_state.set_incremental_slot(slot, seq_id);
}

fn process_incremental_view(
    state: &mut SharedState,
    publisher: &Rc<SpreadIncrementalPublisher>,
    slot_index: Option<usize>,
    dedup_policy: IncrementalDedupPolicy,
    book: RawIncrementalView<'_>,
    max_levels: Option<usize>,
) {
    let slot = slot_index
        .map(|idx| state.symbol_state.incremental_slot_by_index(idx))
        .unwrap_or_else(|| state.symbol_state.incremental_slot(book.symbol));
    let recent_identity = if dedup_policy == IncrementalDedupPolicy::RecentSnapshotIdentity {
        let Some(bids) = mkt_parsers::raw_json_levels_iter(book.bids_raw) else {
            return;
        };
        let Some(asks) = mkt_parsers::raw_json_levels_iter(book.asks_raw) else {
            return;
        };
        Some(Rc::new(IncrementalSnapshotIdentity::from_levels(
            book.timestamp_us,
            &bids.collect::<Vec<_>>(),
            &asks.collect::<Vec<_>>(),
        )))
    } else {
        None
    };
    if should_drop_incremental(&slot, book.seq_id, book.is_snapshot, dedup_policy)
        || recent_identity
            .as_ref()
            .is_some_and(|identity| state.contains_recent_incremental(slot.idx, identity))
    {
        state.incremental_dropped_by_seq += 1;
        return;
    }

    if book.gap_check {
        warn_incremental_gap_if_needed(
            state,
            book.symbol,
            slot.prev,
            book.seq_id,
            book.prev_seq_id,
            book.is_snapshot,
        );
    }

    let total_levels = book.bids_count + book.asks_count;
    let Some(mut bids_iter) = mkt_parsers::raw_json_levels_iter(book.bids_raw) else {
        return;
    };
    let Some(mut asks_iter) = mkt_parsers::raw_json_levels_iter(book.asks_raw) else {
        return;
    };
    match max_levels {
        Some(max) if total_levels > max && max > 0 => {
            let total_chunks = level_chunk_count(book.bids_count, book.asks_count, max);
            let mut bids_start = 0usize;
            let mut asks_start = 0usize;
            for chunk_idx in 0..total_chunks {
                let (bids_count, asks_count) = next_level_chunk(
                    book.bids_count - bids_start,
                    book.asks_count - asks_start,
                    max,
                );
                if !publish_incremental_view_chunk(
                    state,
                    publisher,
                    &book,
                    slot_index,
                    &mut bids_iter,
                    bids_count,
                    &mut asks_iter,
                    asks_count,
                    chunk_idx,
                    total_chunks,
                ) {
                    return;
                }
                bids_start += bids_count;
                asks_start += asks_count;
            }
        }
        _ => {
            if !publish_incremental_view_chunk(
                state,
                publisher,
                &book,
                slot_index,
                &mut bids_iter,
                book.bids_count,
                &mut asks_iter,
                book.asks_count,
                0,
                1,
            ) {
                return;
            }
        }
    }
    if let Some(identity) = recent_identity {
        state.record_recent_incremental(slot.idx, identity);
    }
    state.symbol_state.set_incremental_slot(slot, book.seq_id);
}

fn publish_incremental_chunk<B: PayloadLevel, A: PayloadLevel>(
    state: &mut SharedState,
    publisher: &Rc<SpreadIncrementalPublisher>,
    _slot_index: Option<usize>,
    symbol: &str,
    first_update_id: i64,
    final_update_id: i64,
    timestamp: i64,
    is_snapshot: bool,
    bids: &[B],
    bids_start: usize,
    bids_count: usize,
    asks: &[A],
    asks_start: usize,
    asks_count: usize,
    chunk_idx: usize,
    total_chunks: usize,
) -> bool {
    let publish_result = publisher.publish_chunk_from_levels(
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
    );
    if let Err(e) = publish_result {
        log::warn!("spread_pbs incremental publish failed: {:#}", e);
        return false;
    }
    state.incremental_published += 1;
    true
}

fn publish_incremental_view_chunk(
    state: &mut SharedState,
    publisher: &Rc<SpreadIncrementalPublisher>,
    book: &RawIncrementalView<'_>,
    _slot_index: Option<usize>,
    bids: &mut mkt_parsers::RawJsonLevelIter<'_>,
    bids_count: usize,
    asks: &mut mkt_parsers::RawJsonLevelIter<'_>,
    asks_count: usize,
    chunk_idx: usize,
    total_chunks: usize,
) -> bool {
    let publish_result = publisher.publish_chunk_from_iter(
        book.symbol,
        book.first_update_id,
        book.final_update_id,
        book.timestamp_us,
        book.is_snapshot,
        bids.by_ref().take(bids_count),
        bids_count,
        asks.by_ref().take(asks_count),
        asks_count,
        chunk_idx,
        total_chunks,
    );
    if let Err(e) = publish_result {
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
    let now_us = get_timestamp_us();
    if state
        .derivatives_recent
        .get(bytes)
        .is_some_and(|last_us| now_us.saturating_sub(*last_us) <= DERIVATIVES_DEDUP_WINDOW_US)
    {
        state.derivatives_dropped_duplicate += 1;
        return;
    }
    if let Err(e) = publisher.publish(bytes) {
        log::warn!("spread_pbs derivatives publish failed: {:#}", e);
        return;
    }
    state.derivatives_recent.insert(bytes.to_vec(), now_us);
    state.derivatives_since_prune += 1;
    if state.derivatives_since_prune >= DERIVATIVES_DEDUP_PRUNE_EVERY {
        state
            .derivatives_recent
            .retain(|_, seen_us| now_us.saturating_sub(*seen_us) <= DERIVATIVES_DEDUP_WINDOW_US);
        state.derivatives_since_prune = 0;
    }
    state.derivatives_published += 1;
}

fn warn_incremental_gap_if_needed(
    state: &mut SharedState,
    symbol: &str,
    local_prev_seq_id: i64,
    seq_id: i64,
    prev_seq_id: i64,
    is_snapshot: bool,
) {
    if is_snapshot {
        return;
    }
    let prev = (local_prev_seq_id != i64::MIN).then_some(local_prev_seq_id);
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

fn level_chunk_count(total_bids: usize, total_asks: usize, max: usize) -> usize {
    if max == 0 {
        return 1;
    }
    (total_bids + total_asks).max(1).div_ceil(max)
}

fn next_level_chunk(bids_remaining: usize, asks_remaining: usize, max: usize) -> (usize, usize) {
    let remaining = bids_remaining + asks_remaining;
    if remaining <= max || max == 0 {
        return (bids_remaining, asks_remaining);
    }
    let chunk_bids = if bids_remaining == 0 {
        0
    } else if asks_remaining == 0 {
        max.min(bids_remaining)
    } else {
        let ratio = bids_remaining as f64 / remaining as f64;
        ((max as f64 * ratio).round() as usize)
            .max(1)
            .min(bids_remaining)
    };
    let chunk_asks = (max - chunk_bids).min(asks_remaining);
    (chunk_bids, chunk_asks)
}

fn process_frame(
    state: &mut SharedState,
    publisher: &Rc<SpreadPublisher>,
    recv_us: i64,
    accepted_us: i64,
    f: BboFrame,
    source: MarketSource,
) {
    process_bbo_fields(
        state,
        publisher,
        None,
        recv_us,
        accepted_us,
        &f.symbol,
        f.ts_us,
        f.seq_id,
        f.reset_seq,
        f.bid_price,
        f.bid_amount,
        f.ask_price,
        f.ask_amount,
        source,
    );
}

#[allow(clippy::too_many_arguments)]
fn process_bbo_fields(
    state: &mut SharedState,
    publisher: &Rc<SpreadPublisher>,
    slot_index: Option<usize>,
    _recv_us: i64,
    accepted_us: i64,
    symbol: &str,
    ts_us: i64,
    seq_id: i64,
    reset_seq: bool,
    bid_price: f64,
    bid_amount: f64,
    ask_price: f64,
    ask_amount: f64,
    source: MarketSource,
) {
    reset_dedup_high_water_if_needed(state, accepted_us);

    let slot = slot_index
        .map(|idx| state.symbol_state.bbo_slot_by_index(idx))
        .unwrap_or_else(|| state.symbol_state.bbo_slot(symbol));
    let identity = BboIdentity::new(ts_us, bid_price, bid_amount, ask_price, ask_amount);
    if should_drop_bbo(state, &slot, &identity, ts_us, seq_id, reset_seq) {
        state.dropped_by_seq += 1;
        return;
    }
    state.symbol_state.set_bbo_slot(slot, seq_id, ts_us);
    state.record_selected_source(source);

    if let Err(e) =
        publisher.publish_bbo(symbol, ts_us, bid_price, bid_amount, ask_price, ask_amount)
    {
        log::warn!("spread_pbs publish failed: {:#}", e);
        return;
    }
    if state.bbo_dedup_policy == BboDedupPolicy::RecentIdentity {
        state.record_recent_bbo(slot.idx, identity);
    }
    state.published += 1;
}

fn reset_dedup_high_water_if_needed(state: &mut SharedState, accepted_us: i64) {
    if accepted_us.saturating_sub(state.last_dedup_reset_us) >= DEDUP_RESET_INTERVAL_US {
        state.last_dedup_reset_us = accepted_us;
        if state.bbo_dedup_policy == BboDedupPolicy::RecentIdentity {
            return;
        }
        let cleared = state.symbol_state.clear_bbo();
        log::warn!(
            "spread_pbs dedup high-water reset by interval cleared_symbols={} interval_us={}",
            cleared,
            DEDUP_RESET_INTERVAL_US
        );
    }
}

#[cfg(test)]
fn should_drop_bbo_frame(slot: &SymbolSlot, f: &BboFrame) -> bool {
    should_drop_bbo_fields(slot, f.ts_us, f.seq_id, f.reset_seq)
}

fn should_drop_bbo(
    state: &SharedState,
    slot: &SymbolSlot,
    identity: &BboIdentity,
    ts_us: i64,
    seq_id: i64,
    reset_seq: bool,
) -> bool {
    match state.bbo_dedup_policy {
        BboDedupPolicy::MonotonicSequence => should_drop_bbo_fields(slot, ts_us, seq_id, reset_seq),
        BboDedupPolicy::RecentIdentity => {
            (slot.prev != i64::MIN && ts_us > 0 && slot.prev_ts_us > 0 && ts_us < slot.prev_ts_us)
                || state.contains_recent_bbo(slot.idx, identity)
        }
    }
}

fn should_drop_bbo_fields(slot: &SymbolSlot, ts_us: i64, seq_id: i64, reset_seq: bool) -> bool {
    if slot.prev == i64::MIN {
        return false;
    }

    if ts_us > 0 && slot.prev_ts_us > 0 && ts_us < slot.prev_ts_us {
        return true;
    }

    if reset_seq && seq_id == 1 && slot.prev > seq_id && ts_us > slot.prev_ts_us {
        return false;
    }

    seq_id <= slot.prev
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spread_pbs::bybit::BybitAdapter;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn unique_test_service_root(label: &str) -> String {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        format!(
            "{}_{}_{}",
            label,
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn test_state(now_us: i64) -> SharedState {
        SharedState {
            symbol_state: SymbolSeqState::with_symbols(&[]),
            bbo_dedup_policy: BboDedupPolicy::MonotonicSequence,
            trade_publish_not_before_us: None,
            published: 0,
            trades_published: 0,
            incremental_published: 0,
            incremental_dropped_by_seq: 0,
            incremental_gap_warnings: 0,
            derivatives_published: 0,
            derivatives_dropped_duplicate: 0,
            derivatives_recent: fast_hash_map_with_capacity(16),
            derivatives_since_prune: 0,
            recent_bbo_identities: Vec::new(),
            recent_trade_identities: Vec::new(),
            recent_incremental_identities: Vec::new(),
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
    fn binance_bookticker_role_keeps_bbo_and_disables_replacement_streams() {
        assert_eq!(
            role_stream_policy(
                TradingVenue::BinanceFutures,
                BinanceFuturesRole::Full,
                BybitRole::Full,
            ),
            (true, true)
        );
        assert_eq!(
            role_stream_policy(
                TradingVenue::BinanceFutures,
                BinanceFuturesRole::Market,
                BybitRole::Full,
            ),
            (false, true)
        );
        assert_eq!(
            role_stream_policy(
                TradingVenue::BinanceFutures,
                BinanceFuturesRole::BookTicker,
                BybitRole::Full,
            ),
            (true, false)
        );
        assert!(uses_all_binance_futures_symbols(
            TradingVenue::BinanceFutures,
            BinanceFuturesRole::BookTicker,
        ));
        assert!(!uses_all_binance_futures_symbols(
            TradingVenue::BinanceFutures,
            BinanceFuturesRole::Market,
        ));
    }

    #[test]
    fn hyperliquid_uses_bbo_trade_incremental_and_perp_derivatives() {
        for venue in [
            TradingVenue::HyperliquidMargin,
            TradingVenue::HyperliquidFutures,
        ] {
            assert_eq!(
                role_stream_policy(venue, BinanceFuturesRole::Full, BybitRole::Full),
                (true, true)
            );
            assert!(direct_trade_replacement_enabled(venue));
            assert!(direct_incremental_replacement_enabled(venue));
        }
        assert!(!direct_derivatives_replacement_enabled(
            TradingVenue::HyperliquidMargin
        ));
        assert!(direct_derivatives_replacement_enabled(
            TradingVenue::HyperliquidFutures
        ));
    }

    #[test]
    fn hyperliquid_bbo_identity_accepts_same_millisecond_changes_and_deduplicates_exact_frames() {
        let mut state = test_state(0);
        state.bbo_dedup_policy = BboDedupPolicy::RecentIdentity;
        let timestamp_us = 1_700_000_000_123_000;
        let seq_id = 1_700_000_000_123;
        let first = BboIdentity::new(timestamp_us, 39.0, 10.0, 39.1, 8.0);
        let first_slot = state.symbol_state.bbo_slot("HYPEUSDC");
        assert!(!should_drop_bbo(
            &state,
            &first_slot,
            &first,
            timestamp_us,
            seq_id,
            false,
        ));
        state.record_recent_bbo(first_slot.idx, first);
        state
            .symbol_state
            .set_bbo_slot(first_slot, seq_id, timestamp_us);

        let changed = BboIdentity::new(timestamp_us, 39.0, 9.0, 39.1, 8.0);
        let changed_slot = state.symbol_state.bbo_slot("HYPEUSDC");
        assert!(!should_drop_bbo(
            &state,
            &changed_slot,
            &changed,
            timestamp_us,
            seq_id,
            false,
        ));
        assert!(should_drop_bbo(
            &state,
            &changed_slot,
            &first,
            timestamp_us,
            seq_id,
            false,
        ));
        state.record_recent_bbo(changed_slot.idx, changed);

        let clear = BboIdentity::new(timestamp_us, 0.0, 0.0, 0.0, 0.0);
        assert!(!should_drop_bbo(
            &state,
            &changed_slot,
            &clear,
            timestamp_us,
            seq_id,
            false,
        ));
        state.record_recent_bbo(changed_slot.idx, clear);
        assert!(should_drop_bbo(
            &state,
            &changed_slot,
            &clear,
            timestamp_us,
            seq_id,
            false,
        ));

        let older = BboIdentity::new(timestamp_us - 1_000, 40.0, 1.0, 40.1, 1.0);
        assert!(should_drop_bbo(
            &state,
            &changed_slot,
            &older,
            timestamp_us - 1_000,
            seq_id - 1,
            false,
        ));
    }

    #[test]
    fn recent_bbo_identity_cache_is_bounded_per_symbol() {
        let mut recent = RecentBboIdentities::new();
        for offset in 0..=BBO_IDENTITY_DEDUP_PER_SYMBOL {
            recent.insert(BboIdentity::new(1_000_000, offset as f64, 1.0, 2.0, 1.0));
        }

        assert_eq!(recent.identities.len(), BBO_IDENTITY_DEDUP_PER_SYMBOL);
        assert_eq!(recent.order.len(), BBO_IDENTITY_DEDUP_PER_SYMBOL);
        assert!(!recent.contains(&BboIdentity::new(1_000_000, 0.0, 1.0, 2.0, 1.0)));
    }

    #[test]
    fn recent_bbo_policy_keeps_timestamp_high_water_across_sequence_reset_interval() {
        let mut state = test_state(1_000_000);
        state.bbo_dedup_policy = BboDedupPolicy::RecentIdentity;
        let slot = state.symbol_state.bbo_slot("HYPEUSDC");
        state.symbol_state.set_bbo_slot(slot, 1_000, 1_000_000);
        let identity = BboIdentity::new(1_000_000, 39.0, 1.0, 39.1, 1.0);
        state.record_recent_bbo(slot.idx, identity);

        reset_dedup_high_water_if_needed(&mut state, 1_000_000 + DEDUP_RESET_INTERVAL_US);

        assert_eq!(state.symbol_state.bbo_prev("HYPEUSDC"), 1_000);
        assert!(state.contains_recent_bbo(slot.idx, &identity));
    }

    #[test]
    fn recent_trade_identity_accepts_non_monotonic_tids_and_drops_exact_duplicates() {
        let mut state = test_state(0);
        let first_slot = state.symbol_state.trade_slot("HYPEUSDC");
        assert!(!should_drop_trade(
            &mut state,
            &first_slot,
            900,
            900,
            1_000_000,
            TradeDedupPolicy::RecentIdentity,
        ));
        state.record_recent_trade(first_slot.idx, 1_000_000, 900);
        state.symbol_state.set_trade_slot(first_slot, 900);

        let next_slot = state.symbol_state.trade_slot("HYPEUSDC");
        assert!(!should_drop_trade(
            &mut state,
            &next_slot,
            100,
            100,
            1_000_000,
            TradeDedupPolicy::RecentIdentity,
        ));
        assert!(should_drop_trade(
            &mut state,
            &next_slot,
            900,
            900,
            1_000_000,
            TradeDedupPolicy::RecentIdentity,
        ));
        assert!(!should_drop_trade(
            &mut state,
            &next_slot,
            900,
            900,
            1_001_000,
            TradeDedupPolicy::RecentIdentity,
        ));

        assert!(should_drop_trade(
            &mut state,
            &next_slot,
            100,
            100,
            1_000_000,
            TradeDedupPolicy::MonotonicSequence,
        ));
    }

    #[test]
    fn recent_trade_identity_drops_hyperliquid_subscription_backfill() {
        let mut state = test_state(0);
        state.trade_publish_not_before_us = Some(2_000_000);
        let slot = state.symbol_state.trade_slot("HYPEUSDC");

        assert!(should_drop_trade(
            &mut state,
            &slot,
            10,
            10,
            1_999_000,
            TradeDedupPolicy::RecentIdentity,
        ));
        assert!(!should_drop_trade(
            &mut state,
            &slot,
            11,
            11,
            2_000_000,
            TradeDedupPolicy::RecentIdentity,
        ));
        assert!(!should_drop_trade(
            &mut state,
            &slot,
            1,
            1,
            1,
            TradeDedupPolicy::MonotonicSequence,
        ));
    }

    #[test]
    fn hyperliquid_snapshot_time_is_part_of_incremental_dedup() {
        let slot = SymbolSlot {
            idx: 0,
            prev: 1_000,
            prev_ts_us: 0,
        };
        assert!(!should_drop_incremental(
            &slot,
            999,
            true,
            IncrementalDedupPolicy::SnapshotCanReset,
        ));
        assert!(should_drop_incremental(
            &slot,
            999,
            true,
            IncrementalDedupPolicy::MonotonicIncludingSnapshots,
        ));
        assert!(should_drop_incremental(
            &slot,
            1_000,
            true,
            IncrementalDedupPolicy::MonotonicIncludingSnapshots,
        ));
        assert!(!should_drop_incremental(
            &slot,
            1_001,
            true,
            IncrementalDedupPolicy::MonotonicIncludingSnapshots,
        ));
        assert!(should_drop_incremental(
            &slot,
            999,
            true,
            IncrementalDedupPolicy::RecentSnapshotIdentity,
        ));
        assert!(!should_drop_incremental(
            &slot,
            1_000,
            true,
            IncrementalDedupPolicy::RecentSnapshotIdentity,
        ));
    }

    #[test]
    fn hyperliquid_incremental_identity_accepts_same_millisecond_book_changes() {
        let mut state = test_state(1_000_000);
        let slot = state.symbol_state.incremental_slot("BTCUSDC");
        let bids = [mkt_parsers::msg::mkt_msg::Level {
            price: 60_000.0,
            amount: 1.0,
        }];
        let asks = [mkt_parsers::msg::mkt_msg::Level {
            price: 60_001.0,
            amount: 2.0,
        }];
        let first = Rc::new(IncrementalSnapshotIdentity::from_levels(
            1_700_000_000_123_000,
            &bids,
            &asks,
        ));
        state.record_recent_incremental(slot.idx, Rc::clone(&first));
        state
            .symbol_state
            .set_incremental_slot(slot, 1_700_000_000_123);

        let same_slot = state.symbol_state.incremental_slot("BTCUSDC");
        assert!(state.contains_recent_incremental(same_slot.idx, &first));
        assert!(!should_drop_incremental(
            &same_slot,
            1_700_000_000_123,
            true,
            IncrementalDedupPolicy::RecentSnapshotIdentity,
        ));

        let changed_asks = [mkt_parsers::msg::mkt_msg::Level {
            price: 60_002.0,
            amount: 2.0,
        }];
        let changed = Rc::new(IncrementalSnapshotIdentity::from_levels(
            1_700_000_000_123_000,
            &bids,
            &changed_asks,
        ));
        assert!(!state.contains_recent_incremental(same_slot.idx, &changed));
        assert_ne!(first, changed);
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
    fn incremental_staleness_uses_monotonic_time() {
        let symbols = vec![
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "SOLUSDT".to_string(),
        ];
        let mut state = SymbolSeqState::with_symbols(&symbols);
        let seen_at = Instant::now();

        assert_eq!(
            state.stale_incremental_symbols(
                &INCREMENTAL_CRITICAL_SYMBOLS,
                seen_at,
                INCREMENTAL_CRITICAL_STALE,
            ),
            symbols
        );

        for (seq_id, symbol) in symbols.iter().enumerate() {
            let slot = state.incremental_slot(symbol);
            state.set_incremental_slot_at(slot, seq_id as i64, seen_at);
        }

        assert!(state
            .stale_incremental_symbols(
                &INCREMENTAL_CRITICAL_SYMBOLS,
                seen_at + INCREMENTAL_CRITICAL_STALE,
                INCREMENTAL_CRITICAL_STALE,
            )
            .is_empty());
        assert_eq!(
            state.stale_incremental_symbols(
                &INCREMENTAL_CRITICAL_SYMBOLS,
                seen_at + INCREMENTAL_CRITICAL_STALE + Duration::from_micros(1),
                INCREMENTAL_CRITICAL_STALE,
            ),
            symbols
        );
    }

    #[test]
    fn hyperliquid_incremental_health_covers_all_current_symbols() {
        let current_symbols = ["HYPEUSDC", "ETHUSDC", "BTCUSDC"]
            .into_iter()
            .map(str::to_string)
            .collect::<HashSet<_>>();
        let critical = critical_incremental_symbols_for_venue(
            TradingVenue::HyperliquidFutures,
            &current_symbols,
            &[],
        );
        assert_eq!(critical, ["BTCUSDC", "ETHUSDC", "HYPEUSDC"]);

        let mut state = SymbolSeqState::with_symbols(&critical);
        let seen_at = Instant::now();
        let hype = state.incremental_slot("HYPEUSDC");
        state.set_incremental_slot_at(hype, 1, seen_at);
        let critical_refs = critical.iter().map(String::as_str).collect::<Vec<_>>();

        assert_eq!(
            state.stale_incremental_symbols(&critical_refs, seen_at, INCREMENTAL_CRITICAL_STALE,),
            ["BTCUSDC", "ETHUSDC"],
        );
    }

    #[test]
    fn wall_clock_jump_delta_compares_system_and_monotonic_time() {
        assert_eq!(
            wall_clock_delta_us(1_000_000, 18_000_000, Duration::from_secs(1)),
            16_000_000
        );
        assert_eq!(
            wall_clock_delta_us(18_000_000, 1_000_000, Duration::from_secs(1)),
            -18_000_000
        );
    }

    #[test]
    fn critical_stream_age_summary_reports_incremental_and_trade_freshness() {
        let symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        let mut state = SymbolSeqState::with_symbols(&symbols);
        let seen_at = Instant::now();

        let trade = state.trade_slot("BTCUSDT");
        state.set_trade_slot_at(trade, 1, seen_at);
        let incremental = state.incremental_slot("BTCUSDT");
        state.set_incremental_slot_at(incremental, 2, seen_at);

        assert_eq!(
            state.critical_stream_age_summary(&["BTCUSDT", "ETHUSDT"], seen_at + Duration::from_secs(2)),
            "BTCUSDT:incremental_age_ms=2000:trade_age_ms=2000,ETHUSDT:incremental_age_ms=never:trade_age_ms=never"
        );
    }

    #[test]
    fn incremental_level_chunking_is_allocation_free_and_balanced() {
        assert_eq!(level_chunk_count(7, 3, 4), 3);

        let mut bids_start = 0usize;
        let mut asks_start = 0usize;
        let mut chunks = Vec::new();
        for _ in 0..level_chunk_count(7, 3, 4) {
            let (bids, asks) = next_level_chunk(7 - bids_start, 3 - asks_start, 4);
            chunks.push((bids_start, bids, asks_start, asks));
            bids_start += bids;
            asks_start += asks;
        }

        assert_eq!(chunks, vec![(0, 3, 0, 1), (3, 3, 1, 1), (6, 1, 2, 1)]);
        assert_eq!((bids_start, asks_start), (7, 3));
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
    fn replacement_batch_can_skip_json_fallback_after_raw_miss() {
        struct JsonTradeAdapter {
            skip_json_fallback: bool,
        }

        impl VenueAdapter for JsonTradeAdapter {
            fn name(&self) -> &'static str {
                "json-trade"
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

            fn parse_trade_frame(&self, _value: &serde_json::Value) -> Result<Vec<TradeFrame>> {
                Ok(vec![TradeFrame {
                    symbol: "BTCUSDT".to_string(),
                    timestamp_us: 1,
                    seq_id: 1,
                    trade_id: 1,
                    side: 'B',
                    price: 1.0,
                    amount: 1.0,
                }])
            }

            fn skip_json_fallback_after_raw_miss(&self) -> bool {
                self.skip_json_fallback
            }

            fn keepalive(&self) -> Option<KeepaliveSpec> {
                None
            }
        }

        let raw = br#"{"data":{"e":"trade","s":"BTCUSDT"}}"#;
        let mut emit_bbo = |_frame: BboFrame| Ok(());
        let fallback_batch = parse_replacement_batch(
            "test",
            &JsonTradeAdapter {
                skip_json_fallback: false,
            },
            raw,
            false,
            true,
            false,
            false,
            &mut emit_bbo,
        )
        .unwrap();
        assert_eq!(fallback_batch.trades.len(), 1);

        let raw_only_batch = parse_replacement_batch(
            "test",
            &JsonTradeAdapter {
                skip_json_fallback: true,
            },
            raw,
            false,
            true,
            false,
            false,
            &mut emit_bbo,
        )
        .unwrap();
        assert!(raw_only_batch.is_empty());

        let raw_only = JsonTradeAdapter {
            skip_json_fallback: true,
        };
        assert!(should_drop_json_after_raw_miss(&raw_only, raw));
        assert!(!should_drop_json_after_raw_miss(
            &raw_only,
            &[64, 0, 1, 0, 1, 0, 3, 0]
        ));
    }

    #[test]
    fn fatal_adapter_parse_error_is_returned_to_the_ws_handler() {
        struct FatalAdapter {
            invalidated: std::cell::Cell<bool>,
        }

        impl VenueAdapter for FatalAdapter {
            fn name(&self) -> &'static str {
                "fatal-test"
            }

            fn ws_url(&self) -> String {
                "wss://example.invalid/ws".to_string()
            }

            fn build_subscribe(&self, _symbols: &[String]) -> Vec<serde_json::Value> {
                Vec::new()
            }

            fn reconnect_on_parse_error(&self) -> bool {
                true
            }

            fn on_fatal_parse_error(&self) {
                self.invalidated.set(true);
            }

            fn parse_frame(
                &self,
                _value: &serde_json::Value,
                _emit: &mut dyn FnMut(BboFrame) -> Result<()>,
            ) -> Result<()> {
                Ok(())
            }

            fn parse_trade_frame(&self, _value: &serde_json::Value) -> Result<Vec<TradeFrame>> {
                bail!("malformed subscribed trade frame")
            }

            fn keepalive(&self) -> Option<KeepaliveSpec> {
                None
            }
        }

        let adapter = FatalAdapter {
            invalidated: std::cell::Cell::new(false),
        };
        let mut emit_bbo = |_frame: BboFrame| Ok(());
        let result = parse_replacement_batch(
            "fatal",
            &adapter,
            br#"{"channel":"trades","data":[{}]}"#,
            false,
            true,
            false,
            false,
            &mut emit_bbo,
        );
        assert!(result.is_err());
        assert!(adapter.invalidated.get());
    }

    #[test]
    fn bybit_shared_raw_handler_keeps_trade_and_incremental_frames() {
        let symbols = vec!["BTCUSDT".to_string()];
        let adapter = BybitAdapter::new(TradingVenue::BybitFutures);
        adapter.seed_symbols(&symbols);

        let root = unique_test_service_root("bybit_raw_mix");
        let trade_publisher = Rc::new(
            SpreadTradePublisher::new_open_or_create_with_root("bybit-test", &root)
                .expect("trade publisher"),
        );
        let incremental_publisher = Rc::new(
            SpreadIncrementalPublisher::new_open_or_create_with_root("bybit-test", &root)
                .expect("incremental publisher"),
        );
        trade_publisher.seed_symbols(&symbols).unwrap();
        incremental_publisher.seed_symbols(&symbols).unwrap();

        let mut initial_state = test_state(1_000_000);
        initial_state.symbol_state.ensure_symbols(&symbols);
        let state = Rc::new(RefCell::new(initial_state));

        let trades = br#"{
            "topic":"publicTrade.BTCUSDT",
            "data":[
                {"T":1700000000123,"s":"BTCUSDT","S":"Buy","v":"0.1","p":"100.5","i":"9001","seq":77},
                {"T":1700000000124,"s":"BTCUSDT","S":"Sell","v":"0.2","p":"100.6","i":"9002","seq":77}
            ]
        }"#;
        assert!(process_raw_replacement_frame(
            &adapter,
            trades,
            Some(&trade_publisher),
            Some(&incremental_publisher),
            None,
            None,
            &state,
        ));
        assert_eq!(state.borrow().trades_published, 2);
        assert_eq!(state.borrow().incremental_published, 0);

        let depth = br#"{
            "topic":"orderbook.1000.BTCUSDT","type":"delta",
            "ts":1700000000999,"cts":1700000000123,
            "data":{"s":"BTCUSDT","b":[["100","1"]],"a":[["101","3"]],"u":12345}
        }"#;
        assert!(process_raw_replacement_frame(
            &adapter,
            depth,
            Some(&trade_publisher),
            Some(&incremental_publisher),
            None,
            None,
            &state,
        ));
        assert_eq!(state.borrow().trades_published, 2);
        assert_eq!(state.borrow().incremental_published, 1);
    }

    #[test]
    fn spawn_direct_replacement_batch_legs_creates_two_unique_replicas_per_batch() {
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
                Duration::from_secs(1_800),
                state,
            );

            assert_eq!(legs.len(), 6);
            assert_eq!(
                legs.iter()
                    .map(|leg| leg.label.as_str())
                    .collect::<Vec<_>>(),
                vec![
                    "direct-test-0-a",
                    "direct-test-0-b",
                    "direct-test-1-a",
                    "direct-test-1-b",
                    "direct-test-2-a",
                    "direct-test-2-b",
                ]
            );
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
}
