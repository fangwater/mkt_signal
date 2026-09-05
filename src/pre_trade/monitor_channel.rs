use anyhow::{Context, Result};
use bytes::Bytes;
use iceoryx2::port::publisher::Publisher;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, error, info, warn};
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::collections::{HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::common::min_qty_table::MinQtyTable;
use crate::pre_trade::basic_balance_manager::BasicBalanceManager;
use crate::pre_trade::basic_um_manager::BasicUmManager;
use crate::pre_trade::binance_std_cm_margin_guard::BinanceStdCmMarginGuard;
use crate::pre_trade::binance_std_um_margin_guard::BinanceStdUmMarginGuard;
use crate::pre_trade::close_inventory::{CloseInventoryLedger, CloseReservationGrant};
use crate::pre_trade::net_position::NetPosition;
use crate::pre_trade::order_manager::Side;
use crate::pre_trade::price_table::PriceTable;
use crate::pre_trade::rebalance_usdt::RebalanceUsdtService;
use crate::pre_trade::response_reconcile::{
    clear_deferred_hyperliquid_terminal, take_ready_deferred_hyperliquid_terminal,
};
use crate::pre_trade::symbol_mapper::create_symbol_mapper;
use crate::pre_trade::symbol_util::{extract_base_asset, is_exposure_exempt_asset};
use crate::pre_trade::usdt_balance_manager::{UsdtBalanceManager, UsdtBalanceSnapshot};
use crate::pre_trade::PersistChannel;
use account_common::pm_ipc::{PM_HISTORY_SIZE, PM_MAX_SUBSCRIBERS, PM_SUBSCRIBER_MAX_BUFFER_SIZE};
use account_monitor_common::hyperliquid_account::HyperliquidAccountMode;
use mkt_parsers::msg::basic_account_msg::{
    split_basic_account_event, BasicAccountEventType, BasicAccountRiskMsg, BasicAccountScope,
    BasicBalanceMsg, BasicBorrowInterestMsg, BasicPositionMsg, BasicTradeLiteMsg,
    BasicUmUnrealizedMsg, BinanceBasicOrderMsg, BinanceStdUmWalletSnapshotMsg, GateBasicOrderMsg,
    OkexOrderMsg, BASIC_ACCOUNT_EVENT_HEADER_LEN,
};
use mkt_parsers::msg::bitget_account_msg::BitgetBasicOrderMsg;
use mkt_parsers::msg::bybit_account_msg::BybitBasicOrderMsg;
use mkt_parsers::msg::hyperliquid_account_msg::{
    hyperliquid_account_identity_hash, HyperliquidBasicFillMsg, HyperliquidBasicOrderMsg,
    HyperliquidFactIdentity, HyperliquidFactReplayControlMsg, HyperliquidFactReplayPhase,
    HyperliquidFactReplayRequestMsg, HyperliquidFundingMsg, HyperliquidLedgerMsg,
    HyperliquidPerpDexStateMsg, HyperliquidSnapshotCompleteMsg, HyperliquidSnapshotPath,
    HyperliquidSnapshotPhase, HyperliquidSpotBalanceMsg, HyperliquidTwapHistoryMsg,
    HyperliquidTwapSliceFillMsg, HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN,
    HYPERLIQUID_FACT_REPLAY_REQUEST_PAYLOAD_LEN, HYPERLIQUID_FACT_REPLAY_REQUEST_SERVICE,
};
use mkt_parsers::msg::hyperliquid_native_msg::HyperliquidNativeEventMsg;
use order_common::{hyperliquid_time_in_force, ExecutionType, Order, OrderStatus, TradingVenue};
use persist_common::{
    hyperliquid_account_fact_value_digest, HyperliquidAccountFactAck, UnifiedOrderRecord,
    HYPERLIQUID_ACCOUNT_FACT_MAX_BYTES, HYPERLIQUID_ACCOUNT_FACT_STABLE_KEY_BYTES,
};
use runtime_common::exchange::Exchange;
use runtime_common::ipc_service_name::build_service_name;
use runtime_common::symbol_util::{
    extract_assets_from_internal_symbol, min_qty_symbol_key, normalize_symbol_for_internal,
    normalize_symbol_for_venue,
};
use runtime_common::time_util::get_timestamp_us;
use signal_common::cancel_signal::{ArbCancelCtx, ArbCancelReason, MmCancelCtx, MmCancelReason};
use signal_common::common::{SignalBytes, TradingLeg};
use signal_common::hyperliquid::HyperliquidEndpoints;
use signal_common::trade_signal::{SignalType, TradeSignal};
use trade_signal::ArbMode;

const ACCOUNT_PAYLOAD: usize = 16_384;
const DERIVATIVES_PAYLOAD: usize = 128;
const DERIVATIVES_HISTORY_SIZE: usize = 50;
const DERIVATIVES_MAX_SUBSCRIBERS: usize = 64;
const DERIVATIVES_SUBSCRIBER_MAX_BUFFER: usize = 8192;
const BINANCE_DIRECT_DERIVATIVES_SERVICE: &str = "dat_pbs/binance-futures/derivatives";
const BINANCE_COIN_DERIVATIVES_SERVICE: &str = "dat_pbs/binance-coin-futures/derivatives";
const BITGET_COIN_DERIVATIVES_SERVICE: &str = "dat_pbs/bitget-coin-futures/derivatives";
const OKEX_DERIVATIVES_SERVICE: &str = "dat_pbs/okex-futures/derivatives";
const BYBIT_DERIVATIVES_SERVICE: &str = "dat_pbs/bybit-futures/derivatives";
const BITGET_DERIVATIVES_SERVICE: &str = "dat_pbs/bitget-futures/derivatives";
const GATE_DERIVATIVES_SERVICE: &str = "dat_pbs/gate-futures/derivatives";
const HYPERLIQUID_DERIVATIVES_SERVICE: &str = "dat_pbs/hyperliquid-futures/derivatives";
const DEFAULT_NODE_PRE_TRADE_DERIVATIVES: &str = "pre_trade_derivatives";
const ARB_STARTUP_NET_EXPOSURE_WARN_USDT: f64 = 500.0;
const BASIC_STATE_REFRESH_MIN_INTERVAL_US: i64 = 5_000_000;
const POSITION_MARK_QTY_EPSILON: f64 = 1e-6;
const SMALL_SYMBOL_NET_EXPOSURE_LOG_SKIP_USDT: f64 = 100.0;
const HYPERLIQUID_FACT_REQUEST_RETRY: Duration = Duration::from_secs(1);
const HYPERLIQUID_FACT_PERSIST_RETRY: Duration = Duration::from_secs(1);
const HYPERLIQUID_FACT_ACK_DRAIN_LIMIT: usize = 64;
const HYPERLIQUID_FACT_COMMIT_STEP_LIMIT: usize = 256;
const HYPERLIQUID_FACT_REPLAY_BUFFER_MESSAGE_CAPACITY: usize = 32_768;
const HYPERLIQUID_FACT_REPLAY_BUFFER_BYTE_CAPACITY: usize = 64 * 1024 * 1024;
const HYPERLIQUID_FACT_REQUEST_HISTORY_SIZE: usize = 64;
const HYPERLIQUID_FACT_REQUEST_MAX_PUBLISHERS: usize = 8;
const HYPERLIQUID_FACT_REQUEST_SUBSCRIBER_BUFFER: usize = 256;
const HYPERLIQUID_FACT_CURSOR_PATH_ENV: &str = "HYPERLIQUID_FACT_CURSOR_PATH";
const HYPERLIQUID_FACT_CURSOR_MAGIC: &[u8; 8] = b"HLFACTCR";
const HYPERLIQUID_FACT_CURSOR_PREFIX_LEN: usize =
    HYPERLIQUID_FACT_CURSOR_MAGIC.len() + HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN + 8 + 8;
const HYPERLIQUID_FACT_CURSOR_ENCODED_LEN: usize = HYPERLIQUID_FACT_CURSOR_PREFIX_LEN + 32;

// ==================== Helper Functions ====================

fn is_margin_venue(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BinanceMargin
            | TradingVenue::OkexMargin
            | TradingVenue::GateMargin
            | TradingVenue::BitgetMargin
            | TradingVenue::BybitMargin
            | TradingVenue::HyperliquidMargin
    )
}

fn is_futures_venue(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::BinanceFutures
            | TradingVenue::BinanceCoinFutures
            | TradingVenue::OkexFutures
            | TradingVenue::GateFutures
            | TradingVenue::BitgetFutures
            | TradingVenue::BitgetCoinFutures
            | TradingVenue::BybitFutures
            | TradingVenue::HyperliquidFutures
    )
}

fn exchange_from_venue(venue: TradingVenue) -> Exchange {
    match venue {
        TradingVenue::BinanceMargin
        | TradingVenue::BinanceFutures
        | TradingVenue::BinanceCoinFutures => Exchange::Binance,
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => Exchange::Okex,
        TradingVenue::GateMargin | TradingVenue::GateFutures => Exchange::Gate,
        TradingVenue::BitgetMargin
        | TradingVenue::BitgetFutures
        | TradingVenue::BitgetCoinFutures => Exchange::Bitget,
        TradingVenue::BybitMargin | TradingVenue::BybitFutures => Exchange::Bybit,
        TradingVenue::HyperliquidMargin | TradingVenue::HyperliquidFutures => Exchange::Hyperliquid,
        _ => panic!("unsupported venue for pre_trade: {:?}", venue),
    }
}

fn scope_for_venue(
    venue: TradingVenue,
    binance_account_mode: Option<BinanceAccountMode>,
    hyperliquid_account_mode: Option<HyperliquidAccountMode>,
) -> BasicAccountScope {
    match venue {
        TradingVenue::BinanceMargin => {
            if binance_account_mode == Some(BinanceAccountMode::Standard) {
                BasicAccountScope::BinanceStdSpot
            } else {
                BasicAccountScope::BinanceUnified
            }
        }
        TradingVenue::BinanceFutures => {
            if binance_account_mode == Some(BinanceAccountMode::Standard) {
                BasicAccountScope::BinanceStdUm
            } else {
                BasicAccountScope::BinanceUnified
            }
        }
        TradingVenue::BinanceCoinFutures => {
            if binance_account_mode == Some(BinanceAccountMode::Standard) {
                BasicAccountScope::BinanceStdCm
            } else {
                BasicAccountScope::BinanceUnifiedCm
            }
        }
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => BasicAccountScope::OkexUnified,
        TradingVenue::GateMargin | TradingVenue::GateFutures => BasicAccountScope::GateUnified,
        TradingVenue::BitgetMargin | TradingVenue::BitgetFutures => {
            BasicAccountScope::BitgetUnified
        }
        TradingVenue::BitgetCoinFutures => BasicAccountScope::BitgetUnifiedCoinFutures,
        TradingVenue::BybitMargin | TradingVenue::BybitFutures => BasicAccountScope::BybitUnified,
        TradingVenue::HyperliquidMargin => hyperliquid_account_mode
            .map(HyperliquidAccountMode::spot_scope)
            .unwrap_or(BasicAccountScope::Unknown),
        TradingVenue::HyperliquidFutures => hyperliquid_account_mode
            .map(HyperliquidAccountMode::perp_scope)
            .unwrap_or(BasicAccountScope::Unknown),
        _ => BasicAccountScope::Unknown,
    }
}

fn scope_matches_venue(
    incoming_scope: BasicAccountScope,
    source_exchange: Exchange,
    venue: TradingVenue,
    binance_account_mode: Option<BinanceAccountMode>,
    hyperliquid_account_mode: Option<HyperliquidAccountMode>,
) -> bool {
    if incoming_scope == BasicAccountScope::Unknown {
        return exchange_from_venue(venue) == source_exchange;
    }
    incoming_scope == scope_for_venue(venue, binance_account_mode, hyperliquid_account_mode)
}

fn exchange_scoped_total_equity_scope(
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    binance_account_mode: Option<BinanceAccountMode>,
    hyperliquid_account_mode: Option<HyperliquidAccountMode>,
) -> Option<BasicAccountScope> {
    let open_exchange = exchange_from_venue(open_venue);
    let hedge_exchange = exchange_from_venue(hedge_venue);
    if open_exchange != hedge_exchange {
        return None;
    }

    match open_exchange {
        Exchange::Binance => {
            let open_scope = scope_for_venue(open_venue, binance_account_mode, None);
            let hedge_scope = scope_for_venue(hedge_venue, binance_account_mode, None);
            (open_scope == hedge_scope).then_some(open_scope)
        }
        Exchange::Okex => Some(BasicAccountScope::OkexUnified),
        Exchange::Bybit => Some(BasicAccountScope::BybitUnified),
        Exchange::Bitget => Some(BasicAccountScope::BitgetUnified),
        Exchange::Gate => Some(BasicAccountScope::GateUnified),
        Exchange::Hyperliquid => {
            let open_scope = scope_for_venue(open_venue, None, hyperliquid_account_mode);
            let hedge_scope = scope_for_venue(hedge_venue, None, hyperliquid_account_mode);
            (open_scope != BasicAccountScope::Unknown && open_scope == hedge_scope)
                .then_some(open_scope)
        }
        _ => None,
    }
}

fn trade_update_lite_enabled_for_venues(
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) -> bool {
    let open_exchange = exchange_from_venue(open_venue);
    let hedge_exchange = exchange_from_venue(hedge_venue);
    open_exchange == hedge_exchange
        && matches!(
            open_exchange,
            Exchange::Binance
                | Exchange::Bybit
                | Exchange::Okex
                | Exchange::Bitget
                | Exchange::Hyperliquid
        )
}

// ==================== Deduplication Cache ====================

/// 简单的去重缓存（固定容量，FIFO 淘汰）
pub struct DedupCache {
    set: HashSet<u64>,
    queue: VecDeque<u64>,
    capacity: usize,
}

impl DedupCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            set: HashSet::new(),
            queue: VecDeque::new(),
            capacity: capacity.max(1024),
        }
    }

    /// 插入并返回是否为新条目；false 表示重复，应丢弃
    pub fn insert_check(&mut self, key: u64) -> bool {
        if self.set.contains(&key) {
            return false;
        }
        if self.queue.len() >= self.capacity {
            if let Some(old) = self.queue.pop_front() {
                self.set.remove(&old);
            }
        }
        self.queue.push_back(key);
        self.set.insert(key);
        true
    }
}

/// 组合多个 u64 片段生成稳定的 64 位哈希
pub fn hash64(parts: &[u64]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for p in parts {
        p.hash(&mut hasher);
    }
    hasher.finish()
}

// ==================== Monitor Channel ====================

use crate::common::msg_parser::{get_msg_type, parse_index_price, parse_mark_price, MktMsgType};
use crate::pre_trade::order_manager::OrderManager;
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::pre_trade::reactor_latency::{record_stage_latency, ReactorStage};
use crate::strategy::OrphanStrategyManager;
use account_common::BinanceAccountMode;
use order_common::{OrderUpdate, TradeUpdate, TradeUpdateLite};
use signal_common::common::{align_price_ceil, align_price_floor};
use signal_common::venue_min_qty_table::VenueMinQtyTable;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

const MONITOR_FAST_POLL_NORMAL_WEIGHT: usize = 16;
const MONITOR_FAST_POLL_LOW_WEIGHT: usize = 1;
const MONITOR_FAST_POLL_RAW_MULTIPLIER: usize = 8;

/// 合约腿管理器句柄对：(UmManager, MinQtyTable)。
type UmMgrPair = (Rc<RefCell<BasicUmManager>>, Rc<RefCell<MinQtyTable>>);

#[derive(Default)]
struct PendingRiskChecks {
    mm_position_symbols: HashMap<String, i64>,
    arb_position_symbols: HashMap<String, i64>,
    arb_margin_assets: HashMap<String, i64>,
    arb_startup_ready_ts: i64,
}

impl PendingRiskChecks {
    fn insert_latest(map: &mut HashMap<String, i64>, key: String, e_ts: i64) {
        map.entry(key)
            .and_modify(|current| *current = (*current).max(e_ts))
            .or_insert(e_ts);
    }
}

// Thread-local 单例存储
thread_local! {
    static MONITOR_CHANNEL: RefCell<Option<MonitorChannelInner>> = const { RefCell::new(None) };
    static MONITOR_STATE_LISTENERS: RefCell<Option<MonitorStateListeners>> = const { RefCell::new(None) };
    static BASIC_STATE_CACHE: RefCell<Option<(usize, BasicState)>> = const { RefCell::new(None) };
    static BASIC_STATE_DIRTY: Cell<bool> = const { Cell::new(true) };
    static BASIC_STATE_PRICE_DIRTY: Cell<bool> = const { Cell::new(false) };
    static BASIC_STATE_LAST_REFRESH_US: Cell<i64> = const { Cell::new(0) };
    static PENDING_RISK_CHECKS: RefCell<PendingRiskChecks> = RefCell::new(PendingRiskChecks::default());
    static EXEC_POSITION_SNAPSHOT_READY: Cell<bool> = const { Cell::new(false) };
    static HYPERLIQUID_EXEC_SNAPSHOT_VALID_UNTIL_MS: Cell<i64> = const { Cell::new(0) };
    static HYPERLIQUID_FACT_STREAM_READY: Cell<bool> = const { Cell::new(false) };
}

/// MonitorChannel 单例访问器（零大小类型）
pub struct MonitorChannel;

/// 每条腿的基础管理器（类似 C++ variant）
#[derive(Clone)]
enum LegMgr {
    /// 现货/保证金腿，sz=标的资产数量
    Margin {
        bal: Rc<RefCell<BasicBalanceManager>>,
    },
    /// U 本位合约腿：Binance 按 contracts(mult=1) 处理，OKX/Gate 按 contracts(需合约乘数)处理
    Futures {
        exchange: Exchange,
        um: Rc<RefCell<BasicUmManager>>,
        min_qty_table: Rc<RefCell<MinQtyTable>>,
    },
}

impl LegMgr {
    fn as_balance_mgr(&self) -> Option<Rc<RefCell<BasicBalanceManager>>> {
        match self {
            LegMgr::Margin { bal, .. } => Some(bal.clone()),
            _ => None,
        }
    }

    fn as_um_mgr(&self) -> Option<UmMgrPair> {
        match self {
            LegMgr::Futures {
                um, min_qty_table, ..
            } => Some((um.clone(), min_qty_table.clone())),
            _ => None,
        }
    }
}

struct MonitorStateListeners {
    account_listeners: Vec<BasicAccountListener>,
    derivatives_listener: DerivativesPriceListener,
}

impl MonitorStateListeners {
    fn drain_pending_limit(&mut self, max_messages: usize) -> (bool, usize) {
        let mut has_message = false;
        let token_limit = monitor_fast_poll_token_limit(max_messages);
        let mut consumed_tokens = 0usize;
        let mut raw_remaining = monitor_fast_poll_raw_limit(max_messages);
        for listener in &mut self.account_listeners {
            if consumed_tokens >= token_limit || raw_remaining == 0 {
                break;
            }
            let (listener_has_message, listener_weight, listener_raw_received) =
                listener.drain_pending_limit(token_limit - consumed_tokens, raw_remaining);
            consumed_tokens += listener_weight;
            raw_remaining = raw_remaining.saturating_sub(listener_raw_received);
            has_message |= listener_has_message;
        }
        if consumed_tokens < token_limit && raw_remaining > 0 {
            let (listener_has_message, listener_weight, listener_raw_received) = self
                .derivatives_listener
                .drain_pending_limit(token_limit - consumed_tokens, raw_remaining);
            consumed_tokens += listener_weight;
            let _ = listener_raw_received;
            has_message |= listener_has_message;
        }
        (has_message, consumed_tokens)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HyperliquidSnapshotIdentity {
    monitor_id: u64,
    generation: u64,
    batch_id: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct HyperliquidPathSnapshotState {
    monitor_id: u64,
    generation: u64,
    latest_batch_id: u64,
    active_batch: Option<HyperliquidSnapshotIdentity>,
    complete_valid_until_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, Default)]
struct HyperliquidStreamSnapshotState {
    paths: [HyperliquidPathSnapshotState; 2],
}

impl HyperliquidStreamSnapshotState {
    fn path_index(path: HyperliquidSnapshotPath) -> usize {
        match path {
            HyperliquidSnapshotPath::Primary => 0,
            HyperliquidSnapshotPath::Secondary => 1,
        }
    }

    fn apply_control(
        &mut self,
        msg: &HyperliquidSnapshotCompleteMsg,
        now_ms: i64,
    ) -> Result<HyperliquidSnapshotPhase> {
        let phase = msg
            .phase()
            .ok_or_else(|| anyhow::anyhow!("invalid snapshot phase {}", msg.phase))?;
        let path = msg
            .path()
            .ok_or_else(|| anyhow::anyhow!("invalid snapshot path {}", msg.path))?;
        if msg.monitor_id == 0 || msg.generation == 0 {
            anyhow::bail!("snapshot control monitor_id and generation must be nonzero");
        }
        if msg.timestamp <= 0 || msg.valid_until <= msg.timestamp {
            anyhow::bail!(
                "invalid snapshot freshness window timestamp={} valid_until={}",
                msg.timestamp,
                msg.valid_until
            );
        }
        if msg.valid_until <= now_ms {
            anyhow::bail!(
                "expired snapshot control valid_until={} now={}",
                msg.valid_until,
                now_ms
            );
        }

        let path_index = Self::path_index(path);
        let mut paths = self.paths;
        {
            let state = &mut paths[path_index];
            if state.monitor_id != msg.monitor_id {
                *state = HyperliquidPathSnapshotState {
                    monitor_id: msg.monitor_id,
                    generation: msg.generation,
                    ..HyperliquidPathSnapshotState::default()
                };
            } else if msg.generation < state.generation {
                anyhow::bail!(
                    "stale snapshot generation {} < {}",
                    msg.generation,
                    state.generation
                );
            } else if msg.generation > state.generation {
                *state = HyperliquidPathSnapshotState {
                    monitor_id: msg.monitor_id,
                    generation: msg.generation,
                    ..HyperliquidPathSnapshotState::default()
                };
            }
        }

        let identity = HyperliquidSnapshotIdentity {
            monitor_id: msg.monitor_id,
            generation: msg.generation,
            batch_id: msg.batch_id,
        };
        match phase {
            HyperliquidSnapshotPhase::Invalidate => {
                if msg.batch_id != 0 {
                    anyhow::bail!("snapshot INVALIDATE batch_id must be zero");
                }
                let state = &mut paths[path_index];
                state.active_batch = None;
                state.complete_valid_until_ms = None;
            }
            HyperliquidSnapshotPhase::Begin => {
                let state = &paths[path_index];
                if msg.batch_id == 0 || msg.batch_id <= state.latest_batch_id {
                    anyhow::bail!(
                        "snapshot BEGIN batch_id={} is not newer than {}",
                        msg.batch_id,
                        state.latest_batch_id
                    );
                }
                // Snapshot rows are applied directly to one shared manager. Once a
                // replacement starts, no lease from either path still describes it.
                for path_state in &mut paths {
                    path_state.active_batch = None;
                    path_state.complete_valid_until_ms = None;
                }
                let state = &mut paths[path_index];
                state.latest_batch_id = msg.batch_id;
                state.active_batch = Some(identity);
            }
            HyperliquidSnapshotPhase::Complete => {
                let state = &mut paths[path_index];
                if state.active_batch != Some(identity) {
                    anyhow::bail!(
                        "snapshot COMPLETE has no matching BEGIN: batch_id={}",
                        msg.batch_id
                    );
                }
                state.active_batch = None;
                state.complete_valid_until_ms = Some(msg.valid_until);
            }
        }
        self.paths = paths;
        Ok(phase)
    }

    fn ready_until_ms(&self, now_ms: i64) -> Option<i64> {
        if self.paths.iter().any(|state| state.active_batch.is_some()) {
            return None;
        }
        self.paths
            .iter()
            .filter_map(|state| state.complete_valid_until_ms)
            .filter(|valid_until| *valid_until > now_ms)
            .max()
    }

    fn has_active_batch(&self) -> bool {
        self.paths.iter().any(|state| state.active_batch.is_some())
    }

    fn fail_closed(&mut self) {
        for state in &mut self.paths {
            state.active_batch = None;
            state.complete_valid_until_ms = None;
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct HyperliquidLiveSnapshotReadiness {
    monitor_id: u64,
    spot: HyperliquidStreamSnapshotState,
    perp: HyperliquidStreamSnapshotState,
}

fn hyperliquid_snapshot_owns_risk(
    mode: Option<HyperliquidAccountMode>,
    venue: TradingVenue,
) -> bool {
    mode != Some(HyperliquidAccountMode::PortfolioMargin)
        || venue == TradingVenue::HyperliquidMargin
}

fn decode_hyperliquid_portfolio_risk(data: &[u8]) -> Result<BasicAccountRiskMsg> {
    let msg = BasicAccountRiskMsg::from_bytes(data)?;
    if msg.timestamp < 0 || !msg.margin_ratio.is_finite() || msg.margin_ratio < 0.0 {
        anyhow::bail!("invalid Hyperliquid portfolio safe margin ratio or timestamp");
    }
    Ok(msg)
}

impl HyperliquidLiveSnapshotReadiness {
    fn stream_mut(&mut self, venue: TradingVenue) -> Option<&mut HyperliquidStreamSnapshotState> {
        match venue {
            TradingVenue::HyperliquidMargin => Some(&mut self.spot),
            TradingVenue::HyperliquidFutures => Some(&mut self.perp),
            _ => None,
        }
    }

    fn apply_control(
        &mut self,
        venue: TradingVenue,
        msg: &HyperliquidSnapshotCompleteMsg,
        now_ms: i64,
    ) -> Result<HyperliquidSnapshotPhase> {
        if msg.monitor_id == 0 {
            anyhow::bail!("snapshot control monitor_id must be nonzero");
        }
        if self.monitor_id != msg.monitor_id {
            *self = Self {
                monitor_id: msg.monitor_id,
                ..Self::default()
            };
        }
        self.stream_mut(venue)
            .ok_or_else(|| anyhow::anyhow!("invalid Hyperliquid snapshot venue {venue:?}"))?
            .apply_control(msg, now_ms)
    }

    fn exec_ready_until_ms(
        &self,
        account_mode: Option<HyperliquidAccountMode>,
        exec_venue: TradingVenue,
        now_ms: i64,
        risk_present: bool,
    ) -> Option<i64> {
        match account_mode {
            Some(HyperliquidAccountMode::Standard) => match exec_venue {
                TradingVenue::HyperliquidMargin => self.spot.ready_until_ms(now_ms),
                TradingVenue::HyperliquidFutures if risk_present => {
                    self.perp.ready_until_ms(now_ms)
                }
                _ => None,
            },
            Some(HyperliquidAccountMode::Unified | HyperliquidAccountMode::PortfolioMargin)
                if risk_present =>
            {
                Some(
                    self.spot
                        .ready_until_ms(now_ms)?
                        .min(self.perp.ready_until_ms(now_ms)?),
                )
            }
            Some(HyperliquidAccountMode::Unified | HyperliquidAccountMode::PortfolioMargin)
            | None => None,
        }
    }

    fn arb_ready_until_ms(
        &self,
        account_mode: Option<HyperliquidAccountMode>,
        venue: TradingVenue,
        now_ms: i64,
        risk_present: bool,
    ) -> Option<i64> {
        match account_mode {
            Some(HyperliquidAccountMode::Standard) if venue == TradingVenue::HyperliquidMargin => {
                self.spot.ready_until_ms(now_ms)
            }
            Some(HyperliquidAccountMode::Standard)
                if venue == TradingVenue::HyperliquidFutures && risk_present =>
            {
                self.perp.ready_until_ms(now_ms)
            }
            Some(HyperliquidAccountMode::Unified | HyperliquidAccountMode::PortfolioMargin)
                if risk_present =>
            {
                Some(
                    self.spot
                        .ready_until_ms(now_ms)?
                        .min(self.perp.ready_until_ms(now_ms)?),
                )
            }
            _ => None,
        }
    }

    fn is_snapshot_row(
        &self,
        event_type: BasicAccountEventType,
        account_scope: BasicAccountScope,
        account_mode: Option<HyperliquidAccountMode>,
    ) -> bool {
        let Some(account_mode) = account_mode else {
            return false;
        };
        match event_type {
            BasicAccountEventType::BalanceUpdate
            | BasicAccountEventType::BorrowInterest
            | BasicAccountEventType::HyperliquidSpotBalance => {
                account_scope == account_mode.spot_scope() && self.spot.has_active_batch()
            }
            BasicAccountEventType::PositionUpdate
            | BasicAccountEventType::UnrealizedPnlUpdate
            | BasicAccountEventType::HyperliquidPerpDexState => {
                account_scope == account_mode.perp_scope() && self.perp.has_active_batch()
            }
            BasicAccountEventType::AccountRisk => {
                account_scope == account_mode.perp_scope()
                    && (self.spot.has_active_batch()
                        || (account_mode != HyperliquidAccountMode::PortfolioMargin
                            && self.perp.has_active_batch()))
            }
            _ => false,
        }
    }

    fn validate_perp_dex_state_row(&mut self, data: &[u8]) -> Result<()> {
        match HyperliquidPerpDexStateMsg::from_bytes(data) {
            Ok(_) => Ok(()),
            Err(err) => {
                self.perp.fail_closed();
                Err(err)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct HyperliquidFactCursor {
    monitor_id: u64,
    last_fact_seq: u64,
}

#[derive(Debug)]
struct HyperliquidFactCursorStore {
    path: PathBuf,
    account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
}

impl HyperliquidFactCursorStore {
    fn from_env(account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN]) -> Self {
        let path = std::env::var_os(HYPERLIQUID_FACT_CURSOR_PATH_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from("data").join(format!(
                    "hyperliquid_fact_cursor_{}.bin",
                    hex::encode(account_hash)
                ))
            });
        Self { path, account_hash }
    }

    #[cfg(test)]
    fn at_path(path: PathBuf, account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN]) -> Self {
        Self { path, account_hash }
    }

    fn load(&self) -> Result<HyperliquidFactCursor> {
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Ok(HyperliquidFactCursor::default())
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("read Hyperliquid fact cursor {}", self.path.display())
                })
            }
        };
        decode_hyperliquid_fact_cursor(&bytes, self.account_hash)
            .with_context(|| format!("decode Hyperliquid fact cursor {}", self.path.display()))
    }

    fn persist(&self, cursor: HyperliquidFactCursor) -> Result<()> {
        if cursor.monitor_id == 0 && cursor.last_fact_seq != 0 {
            anyhow::bail!(
                "refuse to persist Hyperliquid fact cursor with zero monitor_id and nonzero seq"
            );
        }
        let parent = self
            .path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "create Hyperliquid fact cursor directory {}",
                parent.display()
            )
        })?;

        let file_name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .context("Hyperliquid fact cursor path has no valid UTF-8 file name")?;
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let temporary = parent.join(format!(".{file_name}.tmp-{}-{nonce}", std::process::id()));
        let encoded = encode_hyperliquid_fact_cursor(self.account_hash, cursor);
        let write_result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .with_context(|| {
                    format!(
                        "create temporary Hyperliquid fact cursor {}",
                        temporary.display()
                    )
                })?;
            file.write_all(&encoded).with_context(|| {
                format!(
                    "write temporary Hyperliquid fact cursor {}",
                    temporary.display()
                )
            })?;
            file.sync_all().with_context(|| {
                format!(
                    "sync temporary Hyperliquid fact cursor {}",
                    temporary.display()
                )
            })?;
            Ok(())
        })();
        if let Err(err) = write_result {
            let _ = fs::remove_file(&temporary);
            return Err(err);
        }
        if let Err(err) = fs::rename(&temporary, &self.path) {
            let _ = fs::remove_file(&temporary);
            return Err(err).with_context(|| {
                format!(
                    "atomically replace Hyperliquid fact cursor {}",
                    self.path.display()
                )
            });
        }
        File::open(parent)
            .with_context(|| format!("open fact cursor directory {}", parent.display()))?
            .sync_all()
            .with_context(|| format!("sync fact cursor directory {}", parent.display()))?;
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        self.persist(HyperliquidFactCursor::default())
    }
}

fn encode_hyperliquid_fact_cursor(
    account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
    cursor: HyperliquidFactCursor,
) -> [u8; HYPERLIQUID_FACT_CURSOR_ENCODED_LEN] {
    let mut encoded = [0_u8; HYPERLIQUID_FACT_CURSOR_ENCODED_LEN];
    encoded[..HYPERLIQUID_FACT_CURSOR_MAGIC.len()].copy_from_slice(HYPERLIQUID_FACT_CURSOR_MAGIC);
    let hash_start = HYPERLIQUID_FACT_CURSOR_MAGIC.len();
    let monitor_start = hash_start + HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN;
    let seq_start = monitor_start + 8;
    encoded[hash_start..monitor_start].copy_from_slice(&account_hash);
    encoded[monitor_start..seq_start].copy_from_slice(&cursor.monitor_id.to_le_bytes());
    encoded[seq_start..HYPERLIQUID_FACT_CURSOR_PREFIX_LEN]
        .copy_from_slice(&cursor.last_fact_seq.to_le_bytes());
    let checksum = Sha256::digest(&encoded[..HYPERLIQUID_FACT_CURSOR_PREFIX_LEN]);
    encoded[HYPERLIQUID_FACT_CURSOR_PREFIX_LEN..].copy_from_slice(&checksum);
    encoded
}

fn decode_hyperliquid_fact_cursor(
    bytes: &[u8],
    expected_account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
) -> Result<HyperliquidFactCursor> {
    if bytes.len() != HYPERLIQUID_FACT_CURSOR_ENCODED_LEN {
        anyhow::bail!(
            "invalid cursor length: expected={} actual={}",
            HYPERLIQUID_FACT_CURSOR_ENCODED_LEN,
            bytes.len()
        );
    }
    if &bytes[..HYPERLIQUID_FACT_CURSOR_MAGIC.len()] != HYPERLIQUID_FACT_CURSOR_MAGIC {
        anyhow::bail!("invalid Hyperliquid fact cursor magic");
    }
    let checksum = Sha256::digest(&bytes[..HYPERLIQUID_FACT_CURSOR_PREFIX_LEN]);
    if bytes[HYPERLIQUID_FACT_CURSOR_PREFIX_LEN..] != checksum[..] {
        anyhow::bail!("Hyperliquid fact cursor checksum mismatch");
    }
    let hash_start = HYPERLIQUID_FACT_CURSOR_MAGIC.len();
    let monitor_start = hash_start + HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN;
    let seq_start = monitor_start + 8;
    let mut account_hash = [0_u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN];
    account_hash.copy_from_slice(&bytes[hash_start..monitor_start]);
    if account_hash != expected_account_hash {
        anyhow::bail!("Hyperliquid fact cursor account/network identity mismatch");
    }
    let monitor_id = u64::from_le_bytes(
        bytes[monitor_start..seq_start]
            .try_into()
            .expect("validated fixed cursor monitor range"),
    );
    let last_fact_seq = u64::from_le_bytes(
        bytes[seq_start..HYPERLIQUID_FACT_CURSOR_PREFIX_LEN]
            .try_into()
            .expect("validated fixed cursor sequence range"),
    );
    if monitor_id == 0 && last_fact_seq != 0 {
        anyhow::bail!("invalid Hyperliquid fact cursor with zero monitor_id and nonzero seq");
    }
    Ok(HyperliquidFactCursor {
        monitor_id,
        last_fact_seq,
    })
}

#[derive(Debug)]
enum HyperliquidFactReplayState {
    Idle,
    Awaiting {
        request_id: u64,
    },
    Replaying {
        request_id: u64,
        monitor_id: u64,
        first_seq: u64,
        last_seq: u64,
        head_seq: u64,
        next_seq: u64,
        buffered_bytes: usize,
        events: Vec<Bytes>,
    },
    Committing {
        cursor: HyperliquidFactCursor,
        caught_up: bool,
    },
    Ready,
    Gap,
}

#[derive(Debug)]
struct PendingHyperliquidFactBatch {
    events: VecDeque<Bytes>,
    caught_up: bool,
    recovery_required: bool,
    next_publish_at: Instant,
}

impl PendingHyperliquidFactBatch {
    fn new(events: Vec<Bytes>, caught_up: bool) -> Self {
        Self {
            events: events.into(),
            caught_up,
            recovery_required: false,
            next_publish_at: Instant::now(),
        }
    }

    fn require_recovery(&mut self) -> bool {
        let newly_required = !self.recovery_required;
        self.recovery_required = true;
        newly_required
    }
}

#[derive(Debug)]
struct HyperliquidFactReplayProtocol {
    account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
    consumer_id: u64,
    next_request_id: u64,
    monitor_id: u64,
    last_fact_seq: u64,
    state: HyperliquidFactReplayState,
}

#[derive(Debug)]
enum HyperliquidFactDisposition {
    Apply,
    Drop,
    Recover(&'static str),
    FailClosed(&'static str),
}

#[derive(Debug)]
enum HyperliquidFactControlDisposition {
    Ignore,
    Waiting,
    Commit { events: Vec<Bytes>, caught_up: bool },
    ResetProducerEpoch,
    Recover(&'static str),
    FailClosed(&'static str),
}

struct HyperliquidFactReplayRequester {
    publisher: Publisher<ipc::Service, [u8; HYPERLIQUID_FACT_REPLAY_REQUEST_PAYLOAD_LEN], ()>,
}

struct HyperliquidFactReplayConsumer {
    protocol: HyperliquidFactReplayProtocol,
    requester: HyperliquidFactReplayRequester,
    cursor_store: HyperliquidFactCursorStore,
    next_request_at: Instant,
}

impl HyperliquidFactReplayProtocol {
    #[cfg(test)]
    fn new(account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN], consumer_id: u64) -> Self {
        Self::new_with_cursor(account_hash, consumer_id, HyperliquidFactCursor::default())
    }

    fn new_with_cursor(
        account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
        consumer_id: u64,
        cursor: HyperliquidFactCursor,
    ) -> Self {
        Self {
            account_hash,
            consumer_id,
            next_request_id: 1,
            monitor_id: cursor.monitor_id,
            last_fact_seq: cursor.last_fact_seq,
            state: HyperliquidFactReplayState::Idle,
        }
    }

    fn begin_request(&mut self) -> HyperliquidFactReplayRequestMsg {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.checked_add(1).unwrap_or(1);
        self.state = HyperliquidFactReplayState::Awaiting { request_id };
        HyperliquidFactReplayRequestMsg {
            account_hash: self.account_hash,
            consumer_id: self.consumer_id,
            request_id,
            last_monitor_id: self.monitor_id,
            last_fact_seq: self.last_fact_seq,
        }
    }

    fn is_ready(&self) -> bool {
        matches!(self.state, HyperliquidFactReplayState::Ready)
    }

    fn is_gap(&self) -> bool {
        matches!(self.state, HyperliquidFactReplayState::Gap)
    }

    fn is_committing(&self) -> bool {
        matches!(self.state, HyperliquidFactReplayState::Committing { .. })
    }

    fn active_request_id(&self) -> Option<u64> {
        match self.state {
            HyperliquidFactReplayState::Awaiting { request_id }
            | HyperliquidFactReplayState::Replaying { request_id, .. } => Some(request_id),
            _ => None,
        }
    }

    fn observe_fact(
        &mut self,
        identity: HyperliquidFactIdentity,
        payload: &[u8],
    ) -> HyperliquidFactDisposition {
        if identity.account_hash != self.account_hash {
            self.state = HyperliquidFactReplayState::Gap;
            return HyperliquidFactDisposition::FailClosed("account identity mismatch");
        }
        if identity.monitor_id == 0 || identity.fact_seq == 0 {
            self.state = HyperliquidFactReplayState::Gap;
            return HyperliquidFactDisposition::FailClosed("zero factual identity");
        }

        match &mut self.state {
            HyperliquidFactReplayState::Ready => {
                if identity.monitor_id != self.monitor_id {
                    return HyperliquidFactDisposition::Recover("producer epoch changed");
                }
                let expected = match self.last_fact_seq.checked_add(1) {
                    Some(expected) => expected,
                    None => {
                        self.state = HyperliquidFactReplayState::Gap;
                        return HyperliquidFactDisposition::FailClosed("factual cursor exhausted");
                    }
                };
                if identity.fact_seq == expected {
                    self.state = HyperliquidFactReplayState::Committing {
                        cursor: HyperliquidFactCursor {
                            monitor_id: identity.monitor_id,
                            last_fact_seq: identity.fact_seq,
                        },
                        caught_up: true,
                    };
                    HyperliquidFactDisposition::Apply
                } else if identity.fact_seq <= self.last_fact_seq {
                    HyperliquidFactDisposition::Drop
                } else {
                    HyperliquidFactDisposition::Recover("live factual sequence gap")
                }
            }
            HyperliquidFactReplayState::Replaying {
                monitor_id,
                first_seq,
                last_seq,
                next_seq,
                buffered_bytes,
                events,
                ..
            } => {
                if identity.monitor_id != *monitor_id {
                    return HyperliquidFactDisposition::Recover("replay producer epoch changed");
                }
                if identity.fact_seq < *first_seq || identity.fact_seq < *next_seq {
                    return HyperliquidFactDisposition::Drop;
                }
                if identity.fact_seq != *next_seq || identity.fact_seq > *last_seq {
                    return HyperliquidFactDisposition::Recover("non-contiguous replay fact");
                }
                let next_bytes = match buffered_bytes.checked_add(payload.len()) {
                    Some(value) => value,
                    None => {
                        self.state = HyperliquidFactReplayState::Gap;
                        return HyperliquidFactDisposition::FailClosed(
                            "replay buffer byte count overflow",
                        );
                    }
                };
                if events.len() >= HYPERLIQUID_FACT_REPLAY_BUFFER_MESSAGE_CAPACITY
                    || next_bytes > HYPERLIQUID_FACT_REPLAY_BUFFER_BYTE_CAPACITY
                {
                    self.state = HyperliquidFactReplayState::Gap;
                    return HyperliquidFactDisposition::FailClosed(
                        "replay exceeds consumer buffer bounds",
                    );
                }
                events.push(Bytes::copy_from_slice(payload));
                *buffered_bytes = next_bytes;
                *next_seq = match next_seq.checked_add(1) {
                    Some(value) => value,
                    None => {
                        self.state = HyperliquidFactReplayState::Gap;
                        return HyperliquidFactDisposition::FailClosed("replay sequence exhausted");
                    }
                };
                HyperliquidFactDisposition::Drop
            }
            HyperliquidFactReplayState::Idle
            | HyperliquidFactReplayState::Awaiting { .. }
            | HyperliquidFactReplayState::Committing { .. }
            | HyperliquidFactReplayState::Gap => HyperliquidFactDisposition::Drop,
        }
    }

    fn observe_control(
        &mut self,
        control: HyperliquidFactReplayControlMsg,
    ) -> HyperliquidFactControlDisposition {
        if control.consumer_id != self.consumer_id {
            return HyperliquidFactControlDisposition::Ignore;
        }
        if self.active_request_id() != Some(control.request_id) {
            return HyperliquidFactControlDisposition::Ignore;
        }
        if control.account_hash != self.account_hash {
            self.state = HyperliquidFactReplayState::Gap;
            return HyperliquidFactControlDisposition::FailClosed(
                "replay control account identity mismatch",
            );
        }
        if control.monitor_id == 0 {
            self.state = HyperliquidFactReplayState::Gap;
            return HyperliquidFactControlDisposition::FailClosed(
                "replay control has zero producer epoch",
            );
        }
        let Some(phase) = control.phase() else {
            self.state = HyperliquidFactReplayState::Gap;
            return HyperliquidFactControlDisposition::FailClosed("invalid replay control phase");
        };
        match phase {
            HyperliquidFactReplayPhase::Gap => {
                if self.monitor_id != 0 && self.monitor_id != control.monitor_id {
                    self.state = HyperliquidFactReplayState::Committing {
                        cursor: HyperliquidFactCursor::default(),
                        caught_up: false,
                    };
                    return HyperliquidFactControlDisposition::ResetProducerEpoch;
                }
                self.state = HyperliquidFactReplayState::Gap;
                HyperliquidFactControlDisposition::FailClosed(
                    "producer replay ring cannot cover requested cursor",
                )
            }
            HyperliquidFactReplayPhase::Begin => {
                let expected_first = if self.monitor_id == control.monitor_id {
                    match self.last_fact_seq.checked_add(1) {
                        Some(value) => value,
                        None => {
                            self.state = HyperliquidFactReplayState::Gap;
                            return HyperliquidFactControlDisposition::FailClosed(
                                "committed factual cursor exhausted",
                            );
                        }
                    }
                } else if self.monitor_id == 0 && self.last_fact_seq == 0 {
                    1
                } else {
                    self.state = HyperliquidFactReplayState::Gap;
                    return HyperliquidFactControlDisposition::FailClosed(
                        "producer epoch changed after a committed factual cursor",
                    );
                };
                if control.first_seq != expected_first
                    || control.first_seq > control.last_seq.saturating_add(1)
                    || control.last_seq > control.head_seq
                {
                    self.state = HyperliquidFactReplayState::Gap;
                    return HyperliquidFactControlDisposition::FailClosed(
                        "producer announced an invalid replay range",
                    );
                }
                self.state = HyperliquidFactReplayState::Replaying {
                    request_id: control.request_id,
                    monitor_id: control.monitor_id,
                    first_seq: control.first_seq,
                    last_seq: control.last_seq,
                    head_seq: control.head_seq,
                    next_seq: control.first_seq,
                    buffered_bytes: 0,
                    events: Vec::new(),
                };
                HyperliquidFactControlDisposition::Waiting
            }
            HyperliquidFactReplayPhase::Complete => {
                let HyperliquidFactReplayState::Replaying {
                    request_id,
                    monitor_id,
                    first_seq,
                    last_seq,
                    head_seq,
                    next_seq,
                    events,
                    ..
                } = &mut self.state
                else {
                    return HyperliquidFactControlDisposition::Recover(
                        "replay COMPLETE without matching BEGIN",
                    );
                };
                if *request_id != control.request_id
                    || *monitor_id != control.monitor_id
                    || *first_seq != control.first_seq
                    || *last_seq != control.last_seq
                    || *head_seq != control.head_seq
                    || *next_seq != control.last_seq.saturating_add(1)
                {
                    return HyperliquidFactControlDisposition::Recover(
                        "replay COMPLETE does not match the validated range",
                    );
                }
                let committed_events = std::mem::take(events);
                self.state = HyperliquidFactReplayState::Committing {
                    cursor: HyperliquidFactCursor {
                        monitor_id: control.monitor_id,
                        last_fact_seq: control.last_seq,
                    },
                    caught_up: control.last_seq == control.head_seq,
                };
                HyperliquidFactControlDisposition::Commit {
                    events: committed_events,
                    caught_up: control.last_seq == control.head_seq,
                }
            }
        }
    }

    fn pending_commit(&self) -> Option<(HyperliquidFactCursor, bool)> {
        match self.state {
            HyperliquidFactReplayState::Committing { cursor, caught_up } => {
                Some((cursor, caught_up))
            }
            _ => None,
        }
    }

    fn complete_commit(&mut self) -> bool {
        let Some((cursor, caught_up)) = self.pending_commit() else {
            return false;
        };
        self.monitor_id = cursor.monitor_id;
        self.last_fact_seq = cursor.last_fact_seq;
        self.state = if caught_up {
            HyperliquidFactReplayState::Ready
        } else {
            HyperliquidFactReplayState::Idle
        };
        true
    }

    fn reset_producer_epoch(&mut self) -> bool {
        let Some((cursor, _)) = self.pending_commit() else {
            return false;
        };
        if cursor != HyperliquidFactCursor::default() {
            return false;
        }
        self.monitor_id = 0;
        self.last_fact_seq = 0;
        self.state = HyperliquidFactReplayState::Idle;
        true
    }

    fn fail_closed(&mut self) {
        self.state = HyperliquidFactReplayState::Gap;
    }
}

impl HyperliquidFactReplayRequester {
    fn new() -> Result<Self> {
        let service_name = build_service_name(HYPERLIQUID_FACT_REPLAY_REQUEST_SERVICE);
        let node = NodeBuilder::new()
            .name(&NodeName::new("pre_trade_hyperliquid_fact_requester")?)
            .create::<ipc::Service>()?;
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; HYPERLIQUID_FACT_REPLAY_REQUEST_PAYLOAD_LEN]>()
            .max_publishers(HYPERLIQUID_FACT_REQUEST_MAX_PUBLISHERS)
            .max_subscribers(1)
            .history_size(HYPERLIQUID_FACT_REQUEST_HISTORY_SIZE)
            .subscriber_max_buffer_size(HYPERLIQUID_FACT_REQUEST_SUBSCRIBER_BUFFER)
            .open_or_create()?;
        Ok(Self {
            publisher: service.publisher_builder().create()?,
        })
    }

    fn send(&self, request: &HyperliquidFactReplayRequestMsg) -> bool {
        let sample = match self.publisher.loan_uninit() {
            Ok(sample) => sample,
            Err(err) => {
                warn!("loan Hyperliquid fact replay request failed: {err}");
                return false;
            }
        };
        let sample = sample.write_payload(request.to_ipc_payload());
        if let Err(err) = sample.send() {
            warn!("publish Hyperliquid fact replay request failed: {err}");
            return false;
        }
        true
    }
}

impl HyperliquidFactReplayConsumer {
    fn new(account_hash: [u8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN]) -> Result<Self> {
        let cursor_store = HyperliquidFactCursorStore::from_env(account_hash);
        let cursor = cursor_store.load()?;
        if cursor.monitor_id != 0 {
            info!(
                "loaded Hyperliquid factual delivery cursor: monitor_id={} last_fact_seq={} path={}",
                cursor.monitor_id,
                cursor.last_fact_seq,
                cursor_store.path.display()
            );
        }
        Ok(Self {
            protocol: HyperliquidFactReplayProtocol::new_with_cursor(
                account_hash,
                fact_consumer_instance_id(),
                cursor,
            ),
            requester: HyperliquidFactReplayRequester::new()?,
            cursor_store,
            next_request_at: Instant::now(),
        })
    }

    fn commit_applied(&mut self) -> Result<bool> {
        let (cursor, caught_up) = self
            .protocol
            .pending_commit()
            .context("Hyperliquid fact commit is not prepared")?;
        self.cursor_store.persist(cursor)?;
        if !self.protocol.complete_commit() {
            anyhow::bail!("Hyperliquid fact protocol rejected a persisted commit");
        }
        Ok(caught_up)
    }

    fn clear_cursor_for_new_epoch(&mut self) -> Result<()> {
        self.cursor_store.clear()?;
        if !self.protocol.reset_producer_epoch() {
            anyhow::bail!("Hyperliquid fact protocol rejected producer epoch reset");
        }
        Ok(())
    }

    fn restart_handshake(&mut self, reason: &'static str) {
        let request = self.protocol.begin_request();
        self.next_request_at = Instant::now() + HYPERLIQUID_FACT_REQUEST_RETRY;
        let sent = self.requester.send(&request);
        warn!(
            "Hyperliquid factual readiness revoked; replay requested: reason={} consumer_id={} request_id={} last_monitor_id={} last_fact_seq={} request_sent={}",
            reason,
            request.consumer_id,
            request.request_id,
            request.last_monitor_id,
            request.last_fact_seq,
            sent
        );
    }

    fn drive(&mut self, now: Instant) {
        if self.protocol.is_ready()
            || self.protocol.is_gap()
            || self.protocol.is_committing()
            || now < self.next_request_at
        {
            return;
        }
        let request = self.protocol.begin_request();
        self.next_request_at = now + HYPERLIQUID_FACT_REQUEST_RETRY;
        let _ = self.requester.send(&request);
    }
}

fn fact_consumer_instance_id() -> u64 {
    let now = get_timestamp_us().unsigned_abs();
    (now ^ u64::from(std::process::id()).rotate_left(32)).max(1)
}

#[derive(Debug, Clone, Copy)]
struct HyperliquidAuditFactMetadata {
    identity: HyperliquidFactIdentity,
    stable_key: [u8; HYPERLIQUID_ACCOUNT_FACT_STABLE_KEY_BYTES],
    used_len: usize,
}

fn hyperliquid_audit_fact_metadata(payload: &[u8]) -> Result<Option<HyperliquidAuditFactMetadata>> {
    let (event_type, _, body) =
        split_basic_account_event(payload).context("invalid Hyperliquid audit fact envelope")?;
    let (identity, venue_key) = match event_type {
        BasicAccountEventType::HyperliquidNativeEvent => {
            let msg = HyperliquidNativeEventMsg::from_bytes(body)?;
            (msg.identity, msg.stable_venue_key())
        }
        BasicAccountEventType::OrderUpdate => {
            let msg = HyperliquidBasicOrderMsg::from_bytes(body)
                .context("decode Hyperliquid order persistence request")?;
            (msg.fact_identity(), msg.stable_venue_key())
        }
        BasicAccountEventType::HyperliquidFill => {
            let msg = HyperliquidBasicFillMsg::from_bytes(body)
                .context("decode Hyperliquid fill persistence request")?;
            (msg.fact_identity(), msg.stable_venue_key())
        }
        BasicAccountEventType::HyperliquidFunding => {
            let msg = HyperliquidFundingMsg::from_bytes(body)
                .context("decode Hyperliquid funding persistence request")?;
            (msg.fact_identity(), msg.stable_venue_key())
        }
        BasicAccountEventType::HyperliquidLedger => {
            let msg = HyperliquidLedgerMsg::from_bytes(body)
                .context("decode Hyperliquid ledger persistence request")?;
            (msg.fact_identity(), msg.stable_venue_key())
        }
        BasicAccountEventType::HyperliquidTwapSliceFill => {
            let msg = HyperliquidTwapSliceFillMsg::from_bytes(body)
                .context("decode Hyperliquid TWAP slice persistence request")?;
            (msg.fact_identity(), msg.stable_venue_key())
        }
        BasicAccountEventType::HyperliquidTwapHistory => {
            let msg = HyperliquidTwapHistoryMsg::from_bytes(body)
                .context("decode Hyperliquid TWAP history persistence request")?;
            (msg.fact_identity(), msg.stable_venue_key())
        }
        _ => return Ok(None),
    };
    let mut stable_key = [0_u8; HYPERLIQUID_ACCOUNT_FACT_STABLE_KEY_BYTES];
    stable_key[..4].copy_from_slice(&(event_type as u32).to_be_bytes());
    stable_key[4..].copy_from_slice(&venue_key);
    Ok(Some(HyperliquidAuditFactMetadata {
        identity,
        stable_key,
        used_len: BASIC_ACCOUNT_EVENT_HEADER_LEN + body.len(),
    }))
}

fn hyperliquid_fact_ack_matches_payload(
    ack: &HyperliquidAccountFactAck,
    payload: &[u8],
) -> Result<bool> {
    let Some(metadata) = hyperliquid_audit_fact_metadata(payload)? else {
        return Ok(false);
    };
    let exact_value = &payload[..metadata.used_len];
    Ok(ack.account_hash == metadata.identity.account_hash
        && ack.monitor_id == metadata.identity.monitor_id
        && ack.fact_seq == metadata.identity.fact_seq
        && ack.stable_key == metadata.stable_key
        && ack.value_digest
            == hyperliquid_account_fact_value_digest(&metadata.stable_key, exact_value))
}

fn account_service_requires_non_overflow(exchange: Exchange) -> bool {
    exchange == Exchange::Hyperliquid
}

fn is_incompatible_overflow_behavior(error: &impl std::fmt::Debug) -> bool {
    format!("{error:?}").contains("IncompatibleOverflowBehavior")
}

fn monitor_fast_poll_token_limit(message_limit: usize) -> usize {
    message_limit
        .saturating_mul(MONITOR_FAST_POLL_NORMAL_WEIGHT)
        .max(MONITOR_FAST_POLL_LOW_WEIGHT)
}

fn monitor_fast_poll_raw_limit(message_limit: usize) -> usize {
    message_limit
        .saturating_mul(MONITOR_FAST_POLL_RAW_MULTIPLIER)
        .max(1)
}

struct BasicAccountListener {
    service_name: String,
    exchange: Exchange,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    open_leg: LegMgr,
    hedge_leg: LegMgr,
    usdt_mgrs: HashMap<BasicAccountScope, Rc<RefCell<UsdtBalanceManager>>>,
    binance_account_mode: Option<BinanceAccountMode>,
    hyperliquid_account_mode: Option<HyperliquidAccountMode>,
    hyperliquid_snapshot_readiness: HyperliquidLiveSnapshotReadiness,
    hyperliquid_snapshot_risk_present: bool,
    hyperliquid_fact_replay: Option<HyperliquidFactReplayConsumer>,
    hyperliquid_fact_commit: Option<PendingHyperliquidFactBatch>,
    strategy_mgr: Rc<RefCell<crate::strategy::StrategyManager>>,
    dedup: DedupCache,
    require_existing_service: bool,
    node: Node<ipc::Service>,
    subscriber: Option<Subscriber<ipc::Service, [u8; ACCOUNT_PAYLOAD], ()>>,
    next_open_attempt_at: Instant,
}

impl BasicAccountListener {
    fn new(
        service_name: String,
        node_name: String,
        exchange: Exchange,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        open_leg: LegMgr,
        hedge_leg: LegMgr,
        usdt_mgrs: HashMap<BasicAccountScope, Rc<RefCell<UsdtBalanceManager>>>,
        binance_account_mode: Option<BinanceAccountMode>,
        hyperliquid_account_mode: Option<HyperliquidAccountMode>,
        strategy_mgr: Rc<RefCell<crate::strategy::StrategyManager>>,
    ) -> Result<Self> {
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;
        let require_existing_service = exchange == Exchange::Gate
            || exchange == Exchange::Hyperliquid
            || (exchange == Exchange::Binance
                && binance_account_mode == Some(BinanceAccountMode::Standard));
        let hyperliquid_fact_replay = if exchange == Exchange::Hyperliquid {
            let address = std::env::var("HYPERLIQUID_ACCOUNT_ADDRESS")
                .map_err(|_| anyhow::anyhow!("HYPERLIQUID_ACCOUNT_ADDRESS is required"))?;
            let endpoints = HyperliquidEndpoints::from_env()?;
            let account_hash = hyperliquid_account_identity_hash(&address, endpoints.testnet)?;
            Some(HyperliquidFactReplayConsumer::new(account_hash)?)
        } else {
            None
        };
        Ok(Self {
            service_name,
            exchange,
            open_venue,
            hedge_venue,
            open_leg,
            hedge_leg,
            usdt_mgrs,
            binance_account_mode,
            hyperliquid_account_mode,
            hyperliquid_snapshot_readiness: HyperliquidLiveSnapshotReadiness::default(),
            hyperliquid_snapshot_risk_present: false,
            hyperliquid_fact_replay,
            hyperliquid_fact_commit: None,
            strategy_mgr,
            dedup: DedupCache::new(8192),
            require_existing_service,
            node,
            subscriber: None,
            next_open_attempt_at: Instant::now(),
        })
    }

    fn refresh_hyperliquid_readiness(&mut self) {
        if self.exchange != Exchange::Hyperliquid {
            return;
        }
        let factual_monitor_id = self
            .hyperliquid_fact_replay
            .as_ref()
            .filter(|consumer| consumer.protocol.is_ready())
            .map(|consumer| consumer.protocol.monitor_id);
        let factual_ready = factual_monitor_id.is_some();
        let snapshot_epoch_matches = factual_monitor_id
            .is_some_and(|monitor_id| self.hyperliquid_snapshot_readiness.monitor_id == monitor_id);
        HYPERLIQUID_FACT_STREAM_READY.with(|ready| ready.set(factual_ready));
        let now_ms = get_timestamp_us() / 1_000;
        for venue in [
            TradingVenue::HyperliquidMargin,
            TradingVenue::HyperliquidFutures,
        ] {
            if venue != self.open_venue && venue != self.hedge_venue {
                continue;
            }
            let valid_until = snapshot_epoch_matches
                .then(|| {
                    self.hyperliquid_snapshot_readiness.arb_ready_until_ms(
                        self.hyperliquid_account_mode,
                        venue,
                        now_ms,
                        self.hyperliquid_snapshot_risk_present,
                    )
                })
                .flatten();
            MonitorChannel::instance().set_hyperliquid_arb_snapshot_readiness(venue, valid_until);
        }
        if self.open_venue == self.hedge_venue {
            let valid_until = snapshot_epoch_matches
                .then(|| {
                    self.hyperliquid_snapshot_readiness.exec_ready_until_ms(
                        self.hyperliquid_account_mode,
                        self.open_venue,
                        now_ms,
                        self.hyperliquid_snapshot_risk_present,
                    )
                })
                .flatten();
            MonitorChannel::instance().set_hyperliquid_exec_snapshot_readiness(
                valid_until,
                "hyperliquid_snapshot_and_fact_replay",
            );
        }
    }

    fn restart_hyperliquid_fact_handshake(&mut self, reason: &'static str) {
        if let Some(pending) = self.hyperliquid_fact_commit.as_mut() {
            if pending.require_recovery() {
                warn!(
                    "defer Hyperliquid factual replay until the durable in-flight fact commits: reason={reason}"
                );
            }
            self.refresh_hyperliquid_readiness();
            return;
        }
        if let Some(consumer) = self.hyperliquid_fact_replay.as_mut() {
            if consumer.protocol.is_gap() {
                error!(
                    "Hyperliquid factual replay GAP is latched until pre_trade restart; refusing handshake retry: reason={reason}"
                );
                self.refresh_hyperliquid_readiness();
                return;
            }
            consumer.restart_handshake(reason);
        }
        self.refresh_hyperliquid_readiness();
    }

    fn drive_hyperliquid_fact_handshake(&mut self) {
        self.drive_hyperliquid_fact_commit();
        if self.hyperliquid_fact_commit.is_some() {
            return;
        }
        if let Some(consumer) = self.hyperliquid_fact_replay.as_mut() {
            consumer.drive(Instant::now());
        }
    }

    fn begin_hyperliquid_fact_commit(&mut self, events: Vec<Bytes>, caught_up: bool) {
        if self.hyperliquid_fact_commit.is_some()
            || !self
                .hyperliquid_fact_replay
                .as_ref()
                .is_some_and(|consumer| consumer.protocol.pending_commit().is_some())
        {
            self.fail_hyperliquid_fact_commit(
                "attempted to overlap Hyperliquid durable fact transactions",
            );
            return;
        }
        self.hyperliquid_fact_commit = Some(PendingHyperliquidFactBatch::new(events, caught_up));
        self.refresh_hyperliquid_readiness();
        self.drive_hyperliquid_fact_commit();
    }

    fn fail_hyperliquid_fact_commit(&mut self, reason: &str) {
        error!("Hyperliquid durable fact transaction failed closed: {reason}");
        self.hyperliquid_fact_commit = None;
        if let Some(consumer) = self.hyperliquid_fact_replay.as_mut() {
            consumer.protocol.fail_closed();
        }
        self.refresh_hyperliquid_readiness();
    }

    fn drive_hyperliquid_fact_commit(&mut self) {
        if self.hyperliquid_fact_commit.is_none() {
            return;
        }

        let (mut acks, ack_receive_error) = PersistChannel::with(|channel| {
            let mut acks = Vec::new();
            let mut receive_error = None;
            for _ in 0..HYPERLIQUID_FACT_ACK_DRAIN_LIMIT {
                match channel.receive_hyperliquid_account_fact_ack() {
                    Ok(Some(ack)) => acks.push(ack),
                    Ok(None) => break,
                    Err(err) => {
                        receive_error = Some(err);
                        break;
                    }
                }
            }
            (acks, receive_error)
        });
        if let Some(err) = ack_receive_error {
            debug!("Hyperliquid fact ACK receive unavailable; retained request will retry: {err}");
        }

        let now = Instant::now();
        for _ in 0..HYPERLIQUID_FACT_COMMIT_STEP_LIMIT {
            let Some(payload) = self
                .hyperliquid_fact_commit
                .as_ref()
                .and_then(|pending| pending.events.front().cloned())
            else {
                let pending = self
                    .hyperliquid_fact_commit
                    .take()
                    .expect("checked Hyperliquid fact commit");
                let commit_result = self
                    .hyperliquid_fact_replay
                    .as_mut()
                    .context("Hyperliquid factual replay consumer is missing")
                    .and_then(HyperliquidFactReplayConsumer::commit_applied);
                match commit_result {
                    Ok(committed_caught_up) if committed_caught_up == pending.caught_up => {
                        let recovery_required = pending.recovery_required || !pending.caught_up;
                        if recovery_required {
                            self.restart_hyperliquid_fact_handshake(
                                "facts arrived while durable persistence was in flight",
                            );
                        } else if let Some(consumer) = self.hyperliquid_fact_replay.as_ref() {
                            info!(
                                "Hyperliquid factual commit durable: monitor_id={} last_fact_seq={}",
                                consumer.protocol.monitor_id, consumer.protocol.last_fact_seq
                            );
                        }
                        self.refresh_hyperliquid_readiness();
                    }
                    Ok(_) => self.fail_hyperliquid_fact_commit(
                        "durable cursor caught-up state did not match the prepared transaction",
                    ),
                    Err(err) => self.fail_hyperliquid_fact_commit(&format!(
                        "persist cursor after durable fact ACK failed: {err:#}"
                    )),
                }
                return;
            };

            let metadata = match hyperliquid_audit_fact_metadata(&payload) {
                Ok(metadata) => metadata,
                Err(err) => {
                    self.fail_hyperliquid_fact_commit(&format!(
                        "decode retained persistence request failed: {err:#}"
                    ));
                    return;
                }
            };
            let Some(metadata) = metadata else {
                if let Err(err) = self.apply_replayed_hyperliquid_fact(&payload) {
                    self.fail_hyperliquid_fact_commit(&format!(
                        "apply validated non-audit replay fact failed: {err:#}"
                    ));
                    return;
                }
                self.hyperliquid_fact_commit
                    .as_mut()
                    .expect("pending batch remains while applying front")
                    .events
                    .pop_front();
                continue;
            };
            if metadata.used_len > HYPERLIQUID_ACCOUNT_FACT_MAX_BYTES {
                self.fail_hyperliquid_fact_commit(
                    "retained Hyperliquid fact exceeds persistence envelope",
                );
                return;
            }
            let exact_payload = &payload[..metadata.used_len];
            let matching_ack = acks.iter().position(|ack| {
                hyperliquid_fact_ack_matches_payload(ack, exact_payload).unwrap_or(false)
            });
            if let Some(index) = matching_ack {
                acks.swap_remove(index);
                if let Err(err) = self.apply_replayed_hyperliquid_fact(exact_payload) {
                    self.fail_hyperliquid_fact_commit(&format!(
                        "apply durable Hyperliquid fact after ACK failed: {err:#}"
                    ));
                    return;
                }
                let pending = self
                    .hyperliquid_fact_commit
                    .as_mut()
                    .expect("pending batch remains after durable apply");
                pending.events.pop_front();
                pending.next_publish_at = now;
                continue;
            }

            let pending = self
                .hyperliquid_fact_commit
                .as_mut()
                .expect("pending batch remains before persistence request");
            if now >= pending.next_publish_at {
                if let Err(err) = PersistChannel::with(|channel| {
                    channel.try_publish_hyperliquid_account_fact(exact_payload)
                }) {
                    warn!(
                        "publish retained Hyperliquid fact failed; will retry without cursor advance: monitor_id={} fact_seq={} err={}",
                        metadata.identity.monitor_id, metadata.identity.fact_seq, err
                    );
                }
                pending.next_publish_at = now + HYPERLIQUID_FACT_PERSIST_RETRY;
            }
            return;
        }
    }

    fn ensure_subscriber(&mut self) -> bool {
        if self.subscriber.is_some() {
            return true;
        }
        let now = Instant::now();
        if now < self.next_open_attempt_at {
            return false;
        }

        let service_name_obj = match ServiceName::new(&self.service_name) {
            Ok(name) => name,
            Err(err) => {
                warn!(
                    "invalid account_monitor service name: service={} err={:?}",
                    self.service_name, err
                );
                self.next_open_attempt_at = Instant::now() + Duration::from_secs(1);
                return false;
            }
        };
        let service_builder = || {
            let builder = self
                .node
                .service_builder(&service_name_obj)
                .publish_subscribe::<[u8; ACCOUNT_PAYLOAD]>()
                .max_publishers(1)
                .max_subscribers(PM_MAX_SUBSCRIBERS)
                .history_size(PM_HISTORY_SIZE)
                .subscriber_max_buffer_size(PM_SUBSCRIBER_MAX_BUFFER_SIZE);
            if account_service_requires_non_overflow(self.exchange) {
                builder.enable_safe_overflow(false)
            } else {
                builder
            }
        };

        let service = if self.require_existing_service {
            match service_builder().open() {
                Ok(service) => service,
                Err(err) => {
                    if account_service_requires_non_overflow(self.exchange)
                        && is_incompatible_overflow_behavior(&err)
                    {
                        error!(
                            "account_monitor service has incompatible safe-overflow policy; Hyperliquid requires safe_overflow=false: service={} exchange={:?} err={:?}",
                            self.service_name, self.exchange, err
                        );
                    } else {
                        warn!(
                            "waiting for account_monitor service: service={} exchange={:?} err={:?}",
                            self.service_name, self.exchange, err
                        );
                    }
                    self.next_open_attempt_at = Instant::now() + Duration::from_secs(1);
                    return false;
                }
            }
        } else {
            match service_builder().open() {
                Ok(service) => service,
                Err(err) => {
                    warn!(
                        "account_monitor service missing, continue with open_or_create: service={} err={:?}",
                        self.service_name, err
                    );
                    match service_builder().open_or_create() {
                        Ok(service) => service,
                        Err(err) => {
                            if account_service_requires_non_overflow(self.exchange)
                                && is_incompatible_overflow_behavior(&err)
                            {
                                error!(
                                    "账户 IceOryx service safe-overflow 配置不兼容；Hyperliquid 要求 safe_overflow=false: service={} err={:?}",
                                    self.service_name, err
                                );
                            } else {
                                warn!(
                                    "创建账户 IceOryx service 失败: service={} err={:?}",
                                    self.service_name, err
                                );
                            }
                            self.next_open_attempt_at = Instant::now() + Duration::from_secs(1);
                            return false;
                        }
                    }
                }
            }
        };

        match service.subscriber_builder().create() {
            Ok(subscriber) => {
                info!(
                    "basic account stream subscribed: service={} exchange={:?}",
                    self.service_name, self.exchange
                );
                self.subscriber = Some(subscriber);
                if self.exchange == Exchange::Hyperliquid {
                    self.restart_hyperliquid_fact_handshake("account IPC subscriber attached");
                }
                true
            }
            Err(err) => {
                warn!(
                    "创建账户 IceOryx subscriber 失败: service={} err={:?}",
                    self.service_name, err
                );
                self.next_open_attempt_at = Instant::now() + Duration::from_secs(1);
                false
            }
        }
    }

    fn drain_pending_limit(
        &mut self,
        max_weight: usize,
        max_raw_messages: usize,
    ) -> (bool, usize, usize) {
        self.drive_hyperliquid_fact_commit();
        if !self.ensure_subscriber() {
            return (false, 0, 0);
        }
        self.drive_hyperliquid_fact_handshake();
        let mut has_message = false;
        let mut consumed_weight = 0usize;
        let mut received = 0usize;
        while consumed_weight < max_weight && received < max_raw_messages {
            let receive_result = self
                .subscriber
                .as_ref()
                .expect("account subscriber should exist after ensure_subscriber")
                .receive();
            match receive_result {
                Ok(Some(sample)) => {
                    received += 1;
                    has_message = true;
                    let weight = self.process_payload(sample.payload());
                    consumed_weight = consumed_weight.saturating_add(weight);
                }
                Ok(None) => break,
                Err(err) => {
                    warn!("account stream receive error: {err}");
                    self.subscriber = None;
                    self.next_open_attempt_at = Instant::now() + Duration::from_millis(200);
                    if self.exchange == Exchange::Hyperliquid {
                        self.restart_hyperliquid_fact_handshake(
                            "account IPC subscriber receive failed",
                        );
                    }
                    break;
                }
            }
        }
        self.drive_hyperliquid_fact_commit();
        (has_message, consumed_weight, received)
    }

    fn clear_hyperliquid_snapshot_scope(
        &mut self,
        account_scope: BasicAccountScope,
        venue: TradingVenue,
    ) {
        if hyperliquid_snapshot_owns_risk(self.hyperliquid_account_mode, venue) {
            self.hyperliquid_snapshot_risk_present = false;
        }
        if scope_matches_venue(
            account_scope,
            self.exchange,
            venue,
            self.binance_account_mode,
            self.hyperliquid_account_mode,
        ) {
            match venue {
                TradingVenue::HyperliquidMargin => {
                    if self.open_venue == venue {
                        if let LegMgr::Margin { bal } = &self.open_leg {
                            bal.borrow_mut().clear();
                        }
                    }
                    if self.hedge_venue == venue && self.hedge_venue != self.open_venue {
                        if let LegMgr::Margin { bal } = &self.hedge_leg {
                            bal.borrow_mut().clear();
                        }
                    }
                    if let Some(settlement) = self.usdt_mgrs.get(&account_scope) {
                        settlement.borrow_mut().clear();
                    }
                }
                TradingVenue::HyperliquidFutures => {
                    if self.open_venue == venue {
                        if let LegMgr::Futures { um, .. } = &self.open_leg {
                            um.borrow_mut().clear();
                        }
                    }
                    if self.hedge_venue == venue && self.hedge_venue != self.open_venue {
                        if let LegMgr::Futures { um, .. } = &self.hedge_leg {
                            um.borrow_mut().clear();
                        }
                    }
                    if account_scope == BasicAccountScope::HyperliquidStdPerp {
                        if let Some(settlement) = self.usdt_mgrs.get(&account_scope) {
                            settlement.borrow_mut().clear();
                        }
                    }
                }
                _ => {}
            }
            // PM risk is owned exclusively by the spot snapshot. A position
            // refresh must not erase a still-current portfolio ratio.
            if hyperliquid_snapshot_owns_risk(self.hyperliquid_account_mode, venue) {
                MonitorChannel::with_inner_mut(|inner| {
                    inner.latest_account_risk.remove(&account_scope);
                });
            }
            MonitorChannel::mark_basic_state_dirty();
        }
    }

    fn process_payload(&mut self, payload: &[u8]) -> usize {
        self.process_payload_inner(payload, false)
    }

    fn apply_replayed_hyperliquid_fact(&mut self, payload: &[u8]) -> Result<usize> {
        let (msg_type, _, data) = split_basic_account_event(payload)
            .context("replayed Hyperliquid fact has an invalid account envelope")?;
        let identity = match msg_type {
            BasicAccountEventType::HyperliquidNativeEvent => {
                HyperliquidNativeEventMsg::from_bytes(data)?.identity
            }
            BasicAccountEventType::OrderUpdate => HyperliquidBasicOrderMsg::from_bytes(data)
                .context("decode replayed Hyperliquid order fact")?
                .fact_identity(),
            BasicAccountEventType::HyperliquidFill => HyperliquidBasicFillMsg::from_bytes(data)
                .context("decode replayed Hyperliquid fill fact")?
                .fact_identity(),
            BasicAccountEventType::HyperliquidFunding => HyperliquidFundingMsg::from_bytes(data)
                .context("decode replayed Hyperliquid funding fact")?
                .fact_identity(),
            BasicAccountEventType::HyperliquidLedger => HyperliquidLedgerMsg::from_bytes(data)
                .context("decode replayed Hyperliquid ledger fact")?
                .fact_identity(),
            BasicAccountEventType::HyperliquidTwapSliceFill => {
                HyperliquidTwapSliceFillMsg::from_bytes(data)
                    .context("decode replayed Hyperliquid TWAP slice fact")?
                    .fact_identity()
            }
            BasicAccountEventType::HyperliquidTwapHistory => {
                HyperliquidTwapHistoryMsg::from_bytes(data)
                    .context("decode replayed Hyperliquid TWAP history fact")?
                    .fact_identity()
            }
            _ => anyhow::bail!("replay transaction contains non-factual event {msg_type:?}"),
        };
        let expected_account_hash = self
            .hyperliquid_fact_replay
            .as_ref()
            .map(|consumer| consumer.protocol.account_hash)
            .context("Hyperliquid factual replay consumer is missing")?;
        if identity.account_hash != expected_account_hash {
            anyhow::bail!("replayed fact account/network identity changed before apply");
        }
        Ok(self.process_payload_inner(payload, true))
    }

    fn process_payload_inner(&mut self, payload: &[u8], factual_prevalidated: bool) -> usize {
        let Some((msg_type, account_scope, data)) = split_basic_account_event(payload) else {
            return MONITOR_FAST_POLL_LOW_WEIGHT;
        };
        let exact_payload = &payload[..BASIC_ACCOUNT_EVENT_HEADER_LEN + data.len()];
        if self.exchange == Exchange::Hyperliquid && !factual_prevalidated {
            let fact_identity = match msg_type {
                BasicAccountEventType::HyperliquidNativeEvent => {
                    match HyperliquidNativeEventMsg::from_bytes(data) {
                        Ok(msg) => Some(msg.identity),
                        Err(err) => {
                            error!(
                                "invalid Hyperliquid native fact; readiness failed closed: {err:#}"
                            );
                            if let Some(consumer) = self.hyperliquid_fact_replay.as_mut() {
                                consumer.protocol.fail_closed();
                            }
                            self.refresh_hyperliquid_readiness();
                            return MONITOR_FAST_POLL_LOW_WEIGHT;
                        }
                    }
                }
                BasicAccountEventType::OrderUpdate => {
                    match HyperliquidBasicOrderMsg::from_bytes(data) {
                        Ok(msg) => Some(msg.fact_identity()),
                        Err(err) => {
                            error!(
                                "invalid sequenced Hyperliquid order fact; readiness failed closed: {err:#}"
                            );
                            if let Some(consumer) = self.hyperliquid_fact_replay.as_mut() {
                                consumer.protocol.fail_closed();
                            }
                            self.refresh_hyperliquid_readiness();
                            return MONITOR_FAST_POLL_LOW_WEIGHT;
                        }
                    }
                }
                BasicAccountEventType::HyperliquidFill => {
                    match HyperliquidBasicFillMsg::from_bytes(data) {
                        Ok(msg) => Some(msg.fact_identity()),
                        Err(err) => {
                            error!(
                                "invalid sequenced Hyperliquid fill fact; readiness failed closed: {err:#}"
                            );
                            if let Some(consumer) = self.hyperliquid_fact_replay.as_mut() {
                                consumer.protocol.fail_closed();
                            }
                            self.refresh_hyperliquid_readiness();
                            return MONITOR_FAST_POLL_LOW_WEIGHT;
                        }
                    }
                }
                BasicAccountEventType::HyperliquidFunding => {
                    match HyperliquidFundingMsg::from_bytes(data) {
                        Ok(msg) => Some(msg.fact_identity()),
                        Err(err) => {
                            error!(
                                "invalid sequenced Hyperliquid funding fact; readiness failed closed: {err:#}"
                            );
                            if let Some(consumer) = self.hyperliquid_fact_replay.as_mut() {
                                consumer.protocol.fail_closed();
                            }
                            self.refresh_hyperliquid_readiness();
                            return MONITOR_FAST_POLL_LOW_WEIGHT;
                        }
                    }
                }
                BasicAccountEventType::HyperliquidLedger => {
                    match HyperliquidLedgerMsg::from_bytes(data) {
                        Ok(msg) => Some(msg.fact_identity()),
                        Err(err) => {
                            error!(
                                "invalid sequenced Hyperliquid ledger fact; readiness failed closed: {err:#}"
                            );
                            if let Some(consumer) = self.hyperliquid_fact_replay.as_mut() {
                                consumer.protocol.fail_closed();
                            }
                            self.refresh_hyperliquid_readiness();
                            return MONITOR_FAST_POLL_LOW_WEIGHT;
                        }
                    }
                }
                BasicAccountEventType::HyperliquidTwapSliceFill => {
                    match HyperliquidTwapSliceFillMsg::from_bytes(data) {
                        Ok(msg) => Some(msg.fact_identity()),
                        Err(err) => {
                            error!(
                                "invalid sequenced Hyperliquid TWAP slice fact; readiness failed closed: {err:#}"
                            );
                            if let Some(consumer) = self.hyperliquid_fact_replay.as_mut() {
                                consumer.protocol.fail_closed();
                            }
                            self.refresh_hyperliquid_readiness();
                            return MONITOR_FAST_POLL_LOW_WEIGHT;
                        }
                    }
                }
                BasicAccountEventType::HyperliquidTwapHistory => {
                    match HyperliquidTwapHistoryMsg::from_bytes(data) {
                        Ok(msg) => Some(msg.fact_identity()),
                        Err(err) => {
                            error!(
                                "invalid sequenced Hyperliquid TWAP history fact; readiness failed closed: {err:#}"
                            );
                            if let Some(consumer) = self.hyperliquid_fact_replay.as_mut() {
                                consumer.protocol.fail_closed();
                            }
                            self.refresh_hyperliquid_readiness();
                            return MONITOR_FAST_POLL_LOW_WEIGHT;
                        }
                    }
                }
                _ => None,
            };
            if let Some(identity) = fact_identity {
                let expected_account_hash = self
                    .hyperliquid_fact_replay
                    .as_ref()
                    .map(|consumer| consumer.protocol.account_hash)
                    .unwrap_or_default();
                if self.hyperliquid_fact_commit.is_some() {
                    if identity.account_hash != expected_account_hash {
                        self.fail_hyperliquid_fact_commit(
                            "fact account identity changed while persistence ACK was pending",
                        );
                    } else if self
                        .hyperliquid_fact_commit
                        .as_mut()
                        .expect("checked pending Hyperliquid fact commit")
                        .require_recovery()
                    {
                        warn!(
                            "defer sequenced Hyperliquid fact while persistence ACK is pending; replay from committed cursor will recover monitor_id={} fact_seq={}",
                            identity.monitor_id, identity.fact_seq
                        );
                    }
                    return MONITOR_FAST_POLL_LOW_WEIGHT;
                }
                let disposition = self
                    .hyperliquid_fact_replay
                    .as_mut()
                    .map(|consumer| consumer.protocol.observe_fact(identity, exact_payload))
                    .unwrap_or(HyperliquidFactDisposition::FailClosed(
                        "factual replay consumer is missing",
                    ));
                match disposition {
                    HyperliquidFactDisposition::Apply => {
                        self.begin_hyperliquid_fact_commit(
                            vec![Bytes::copy_from_slice(exact_payload)],
                            true,
                        );
                        return MONITOR_FAST_POLL_LOW_WEIGHT;
                    }
                    HyperliquidFactDisposition::Drop => return MONITOR_FAST_POLL_LOW_WEIGHT,
                    HyperliquidFactDisposition::Recover(reason) => {
                        self.restart_hyperliquid_fact_handshake(reason);
                        return MONITOR_FAST_POLL_LOW_WEIGHT;
                    }
                    HyperliquidFactDisposition::FailClosed(reason) => {
                        error!("Hyperliquid factual stream failed closed: {reason}");
                        self.refresh_hyperliquid_readiness();
                        return MONITOR_FAST_POLL_LOW_WEIGHT;
                    }
                }
            }

            if msg_type == BasicAccountEventType::HyperliquidFactReplayControl {
                let control = match HyperliquidFactReplayControlMsg::from_bytes(data) {
                    Ok(control) => control,
                    Err(err) => {
                        error!(
                            "invalid Hyperliquid factual replay control; readiness failed closed: {err:#}"
                        );
                        if let Some(consumer) = self.hyperliquid_fact_replay.as_mut() {
                            consumer.protocol.fail_closed();
                        }
                        self.refresh_hyperliquid_readiness();
                        return MONITOR_FAST_POLL_LOW_WEIGHT;
                    }
                };
                let disposition = self
                    .hyperliquid_fact_replay
                    .as_mut()
                    .map(|consumer| consumer.protocol.observe_control(control))
                    .unwrap_or(HyperliquidFactControlDisposition::FailClosed(
                        "factual replay consumer is missing",
                    ));
                match disposition {
                    HyperliquidFactControlDisposition::Ignore
                    | HyperliquidFactControlDisposition::Waiting => {}
                    HyperliquidFactControlDisposition::Commit { events, caught_up } => {
                        self.begin_hyperliquid_fact_commit(events, caught_up);
                        return MONITOR_FAST_POLL_LOW_WEIGHT;
                    }
                    HyperliquidFactControlDisposition::ResetProducerEpoch => {
                        let reset_result = self
                            .hyperliquid_fact_replay
                            .as_mut()
                            .context("Hyperliquid factual replay consumer is missing")
                            .and_then(HyperliquidFactReplayConsumer::clear_cursor_for_new_epoch);
                        match reset_result {
                            Ok(()) => {
                                warn!(
                                    "Hyperliquid producer epoch changed; cleared the durable delivery cursor and requesting a complete replay from seq=1: old request now answered by monitor_id={}",
                                    control.monitor_id
                                );
                                self.restart_hyperliquid_fact_handshake(
                                    "producer epoch reset after explicit GAP",
                                );
                            }
                            Err(err) => {
                                error!(
                                    "failed to clear stale Hyperliquid factual cursor; readiness failed closed: {err:#}"
                                );
                                if let Some(consumer) = self.hyperliquid_fact_replay.as_mut() {
                                    consumer.protocol.fail_closed();
                                }
                                self.refresh_hyperliquid_readiness();
                            }
                        }
                        return MONITOR_FAST_POLL_LOW_WEIGHT;
                    }
                    HyperliquidFactControlDisposition::Recover(reason) => {
                        self.restart_hyperliquid_fact_handshake(reason);
                        return MONITOR_FAST_POLL_LOW_WEIGHT;
                    }
                    HyperliquidFactControlDisposition::FailClosed(reason) => {
                        error!(
                            "Hyperliquid factual replay failed closed: reason={} monitor_id={} range={}..={}",
                            reason, control.monitor_id, control.first_seq, control.last_seq
                        );
                    }
                }
                self.refresh_hyperliquid_readiness();
                return MONITOR_FAST_POLL_LOW_WEIGHT;
            }
        }

        let is_hyperliquid_snapshot_row = self.exchange == Exchange::Hyperliquid
            && self.hyperliquid_snapshot_readiness.is_snapshot_row(
                msg_type,
                account_scope,
                self.hyperliquid_account_mode,
            );
        let is_hyperliquid_state_event = self.exchange == Exchange::Hyperliquid
            && matches!(
                msg_type,
                BasicAccountEventType::BalanceUpdate
                    | BasicAccountEventType::BorrowInterest
                    | BasicAccountEventType::HyperliquidSpotBalance
                    | BasicAccountEventType::HyperliquidPerpDexState
                    | BasicAccountEventType::PositionUpdate
                    | BasicAccountEventType::UnrealizedPnlUpdate
                    | BasicAccountEventType::AccountRisk
            );
        let is_hyperliquid_fact = self.exchange == Exchange::Hyperliquid
            && matches!(
                msg_type,
                BasicAccountEventType::OrderUpdate
                    | BasicAccountEventType::HyperliquidFill
                    | BasicAccountEventType::HyperliquidFunding
                    | BasicAccountEventType::HyperliquidLedger
                    | BasicAccountEventType::HyperliquidTwapSliceFill
                    | BasicAccountEventType::HyperliquidTwapHistory
                    | BasicAccountEventType::HyperliquidNativeEvent
            );
        if is_hyperliquid_state_event && !is_hyperliquid_snapshot_row {
            warn!(
                "drop Hyperliquid account-state row outside an active snapshot transaction: event={msg_type:?} scope={}",
                account_scope.as_str()
            );
            return MONITOR_FAST_POLL_LOW_WEIGHT;
        }
        if !is_hyperliquid_snapshot_row && !is_hyperliquid_fact {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            payload.hash(&mut hasher);
            let key = hasher.finish();
            if !self.dedup.insert_check(key) {
                return MONITOR_FAST_POLL_LOW_WEIGHT;
            }
        }

        let weight = match msg_type {
            BasicAccountEventType::BalanceUpdate => {
                let mut weight = MONITOR_FAST_POLL_LOW_WEIGHT;
                if let Ok(msg) = BasicBalanceMsg::from_bytes(data) {
                    if let Some(mgr) = self.usdt_mgrs.get(&account_scope) {
                        let is_settlement_asset = {
                            let mgr = mgr.borrow();
                            msg.symbol.eq_ignore_ascii_case(mgr.settlement_asset())
                        };
                        if is_settlement_asset {
                            mgr.borrow_mut().apply_balance(&msg);
                        }
                    }
                    if scope_matches_venue(
                        account_scope,
                        self.exchange,
                        self.open_venue,
                        self.binance_account_mode,
                        self.hyperliquid_account_mode,
                    ) {
                        if let LegMgr::Margin { bal, .. } = &self.open_leg {
                            bal.borrow_mut().apply_balance(&msg);
                            MonitorChannel::instance().mark_arb_startup_net_seen_for_venue(
                                self.open_venue,
                                "account_balance",
                            );
                            if MonitorChannel::queue_arb_margin_net_risk_check(
                                &msg.symbol,
                                msg.timestamp.max(0).saturating_mul(1000),
                            ) {
                                weight = MONITOR_FAST_POLL_NORMAL_WEIGHT;
                            }
                        }
                    }
                    if scope_matches_venue(
                        account_scope,
                        self.exchange,
                        self.hedge_venue,
                        self.binance_account_mode,
                        self.hyperliquid_account_mode,
                    ) {
                        if let LegMgr::Margin { bal, .. } = &self.hedge_leg {
                            bal.borrow_mut().apply_balance(&msg);
                            MonitorChannel::instance().mark_arb_startup_net_seen_for_venue(
                                self.hedge_venue,
                                "account_balance",
                            );
                        }
                    }
                    MonitorChannel::mark_basic_state_dirty();
                    if msg.symbol.eq_ignore_ascii_case("USDT")
                        && account_scope == BasicAccountScope::BinanceStdSpot
                    {
                        RebalanceUsdtService::drive_from_account_update("spot_usdt_balance");
                    }
                }
                weight
            }
            BasicAccountEventType::PositionUpdate => {
                if let Ok(msg) = BasicPositionMsg::from_bytes(data) {
                    if self.exchange == Exchange::Okex
                        && !msg.inst_id.contains('-')
                        && !msg.inst_id.contains("-SWAP")
                    {
                        warn!(
                            "drop malformed OKX position update (unexpected inst_id format): exchange={:?} inst_id={} side={} amt={} ts={}",
                            self.exchange,
                            msg.inst_id,
                            msg.position_side,
                            msg.position_amount,
                            msg.timestamp
                        );
                        return MONITOR_FAST_POLL_NORMAL_WEIGHT;
                    }
                    if scope_matches_venue(
                        account_scope,
                        self.exchange,
                        self.open_venue,
                        self.binance_account_mode,
                        self.hyperliquid_account_mode,
                    ) {
                        if let LegMgr::Futures { um, .. } = &self.open_leg {
                            um.borrow_mut().apply_position(&msg);
                            MonitorChannel::instance().mark_arb_startup_net_seen_for_venue(
                                self.open_venue,
                                "account_position",
                            );
                        }
                    }
                    if scope_matches_venue(
                        account_scope,
                        self.exchange,
                        self.hedge_venue,
                        self.binance_account_mode,
                        self.hyperliquid_account_mode,
                    ) {
                        if let LegMgr::Futures { um, .. } = &self.hedge_leg {
                            um.borrow_mut().apply_position(&msg);
                            MonitorChannel::instance().mark_arb_startup_net_seen_for_venue(
                                self.hedge_venue,
                                "account_position",
                            );
                        }
                    }
                    let symbol = normalize_symbol_for_internal(&msg.inst_id);
                    if !symbol.is_empty() {
                        // 交易所侧事件时间 ms→µs（0/负数视作无上下文）
                        let e_ts = msg.timestamp.max(0).saturating_mul(1000);
                        if self.open_venue == self.hedge_venue {
                            MonitorChannel::queue_mm_position_risk_check(&symbol, e_ts);
                        } else {
                            MonitorChannel::queue_arb_position_risk_check(&symbol, e_ts);
                        }
                    }
                    MonitorChannel::mark_basic_state_dirty();
                }
                MONITOR_FAST_POLL_NORMAL_WEIGHT
            }
            BasicAccountEventType::UnrealizedPnlUpdate => {
                if let Ok(msg) = BasicUmUnrealizedMsg::from_bytes(data) {
                    if scope_matches_venue(
                        account_scope,
                        self.exchange,
                        self.open_venue,
                        self.binance_account_mode,
                        self.hyperliquid_account_mode,
                    ) {
                        if let LegMgr::Futures { um, .. } = &self.open_leg {
                            um.borrow_mut().apply_unrealized_pnl(&msg);
                        }
                    }
                    if scope_matches_venue(
                        account_scope,
                        self.exchange,
                        self.hedge_venue,
                        self.binance_account_mode,
                        self.hyperliquid_account_mode,
                    ) {
                        if let LegMgr::Futures { um, .. } = &self.hedge_leg {
                            um.borrow_mut().apply_unrealized_pnl(&msg);
                        }
                    }
                    MonitorChannel::mark_basic_state_dirty();
                }
                MONITOR_FAST_POLL_LOW_WEIGHT
            }
            BasicAccountEventType::BorrowInterest => {
                let mut weight = MONITOR_FAST_POLL_LOW_WEIGHT;
                if let Ok(msg) = BasicBorrowInterestMsg::from_bytes(data) {
                    if let Some(mgr) = self.usdt_mgrs.get(&account_scope) {
                        let is_settlement_asset = {
                            let mgr = mgr.borrow();
                            msg.symbol.eq_ignore_ascii_case(mgr.settlement_asset())
                        };
                        if is_settlement_asset {
                            mgr.borrow_mut().apply_borrow_interest(&msg);
                        }
                    }
                    if scope_matches_venue(
                        account_scope,
                        self.exchange,
                        self.open_venue,
                        self.binance_account_mode,
                        self.hyperliquid_account_mode,
                    ) {
                        if let LegMgr::Margin { bal, .. } = &self.open_leg {
                            bal.borrow_mut().apply_borrow_interest(&msg);
                            MonitorChannel::instance().mark_arb_startup_net_seen_for_venue(
                                self.open_venue,
                                "account_borrow_interest",
                            );
                            if MonitorChannel::queue_arb_margin_net_risk_check(
                                &msg.symbol,
                                msg.timestamp.max(0).saturating_mul(1000),
                            ) {
                                weight = MONITOR_FAST_POLL_NORMAL_WEIGHT;
                            }
                        }
                    }
                    if scope_matches_venue(
                        account_scope,
                        self.exchange,
                        self.hedge_venue,
                        self.binance_account_mode,
                        self.hyperliquid_account_mode,
                    ) {
                        if let LegMgr::Margin { bal, .. } = &self.hedge_leg {
                            bal.borrow_mut().apply_borrow_interest(&msg);
                            MonitorChannel::instance().mark_arb_startup_net_seen_for_venue(
                                self.hedge_venue,
                                "account_borrow_interest",
                            );
                        }
                    }
                    MonitorChannel::mark_basic_state_dirty();
                }
                weight
            }
            BasicAccountEventType::OrderUpdate => {
                match self.exchange {
                    Exchange::Okex => {
                        if let Ok(msg) = OkexOrderMsg::from_bytes(data) {
                            dispatch_order_update_generic(&self.strategy_mgr, &msg);
                        }
                    }
                    Exchange::Binance => {
                        if let Ok(msg) = BinanceBasicOrderMsg::from_bytes(data) {
                            if msg.external_order_label().is_some() {
                                dispatch_binance_external_order_update(&msg);
                            } else {
                                dispatch_order_update_generic(&self.strategy_mgr, &msg);
                            }
                        }
                    }
                    Exchange::Gate => {
                        if let Ok(msg) = GateBasicOrderMsg::from_bytes(data) {
                            dispatch_order_update_generic(&self.strategy_mgr, &msg);
                        }
                    }
                    Exchange::Bitget => {
                        if let Ok(msg) = BitgetBasicOrderMsg::from_bytes(data) {
                            dispatch_order_update_generic(&self.strategy_mgr, &msg);
                        }
                    }
                    Exchange::Bybit => {
                        if let Ok(msg) = BybitBasicOrderMsg::from_bytes(data) {
                            dispatch_order_update_generic(&self.strategy_mgr, &msg);
                        }
                    }
                    Exchange::Hyperliquid => {
                        if let Ok(mut msg) = HyperliquidBasicOrderMsg::from_bytes(data) {
                            let local_order =
                                MonitorChannel::try_order_manager().and_then(|order_manager| {
                                    order_manager.borrow().get(msg.client_order_id)
                                });
                            override_hyperliquid_order_intent_from_local(
                                &mut msg,
                                local_order.as_ref(),
                            );
                            if order_common::OrderUpdate::status(&msg).is_finished() {
                                clear_deferred_hyperliquid_terminal(msg.client_order_id);
                            }
                            dispatch_order_update_generic(&self.strategy_mgr, &msg);
                        }
                    }
                    _ => {}
                }
                MONITOR_FAST_POLL_NORMAL_WEIGHT
            }
            BasicAccountEventType::TradeUpdateLite => {
                // 仅在已验证轻量成交频道的同所路径启用 TradeLite 派发。
                if trade_update_lite_enabled_for_venues(self.open_venue, self.hedge_venue) {
                    if let Ok(msg) = BasicTradeLiteMsg::from_bytes(data) {
                        dispatch_trade_update_lite_generic(&self.strategy_mgr, &msg);
                    }
                }
                MONITOR_FAST_POLL_NORMAL_WEIGHT
            }
            BasicAccountEventType::HyperliquidFill => {
                match HyperliquidBasicFillMsg::from_bytes(data) {
                    Ok(msg) => {
                        let matched = if msg.client_order_id > 0 {
                            dispatch_trade_update_generic(&self.strategy_mgr, &msg)
                        } else {
                            MonitorChannel::instance().bump_trade_update_seq();
                            false
                        };
                        if !matched {
                            let forced_close = build_hyperliquid_external_uniform_order(&msg);
                            PersistChannel::with(|channel| {
                                channel.publish_trade_update_unmatched(&msg);
                                if let Some(record) = &forced_close {
                                    channel.publish_uniform_order(record);
                                }
                            });
                            if forced_close.is_some() {
                                warn!(
                                    "Hyperliquid exchange liquidation fill persisted: method={} venue={:?} symbol={} oid={} tid={} tx_hash={} price={:.8} qty={:.8}",
                                    msg.liquidation_method,
                                    order_common::TradeUpdate::trading_venue(&msg),
                                    msg.symbol,
                                    msg.order_id,
                                    msg.venue_trade_id,
                                    msg.transaction_hash,
                                    msg.price,
                                    msg.last_filled_quantity,
                                );
                            } else {
                                warn!(
                                    "Hyperliquid factual fill persisted unmatched: venue={:?} symbol={} oid={} client_order_id={} tid={} tx_hash={} price={:.8} qty={:.8}",
                                    order_common::TradeUpdate::trading_venue(&msg),
                                    msg.symbol,
                                    msg.order_id,
                                    msg.client_order_id,
                                    msg.venue_trade_id,
                                    msg.transaction_hash,
                                    msg.price,
                                    msg.last_filled_quantity,
                                );
                            }
                        }
                    }
                    Err(err) => warn!("Hyperliquid fill decode failed: {err:#}"),
                }
                MONITOR_FAST_POLL_NORMAL_WEIGHT
            }
            BasicAccountEventType::HyperliquidFunding => {
                if let Err(err) = HyperliquidFundingMsg::from_bytes(data) {
                    warn!("Hyperliquid funding fact decode failed after sequencing: {err:#}");
                }
                MONITOR_FAST_POLL_LOW_WEIGHT
            }
            BasicAccountEventType::HyperliquidLedger => {
                if let Err(err) = HyperliquidLedgerMsg::from_bytes(data) {
                    warn!("Hyperliquid ledger fact decode failed after sequencing: {err:#}");
                }
                MONITOR_FAST_POLL_LOW_WEIGHT
            }
            BasicAccountEventType::HyperliquidTwapSliceFill => {
                if let Err(err) = HyperliquidTwapSliceFillMsg::from_bytes(data) {
                    warn!(
                        "Hyperliquid TWAP slice association decode failed after sequencing: {err:#}"
                    );
                }
                MONITOR_FAST_POLL_LOW_WEIGHT
            }
            BasicAccountEventType::HyperliquidTwapHistory => {
                if let Err(err) = HyperliquidTwapHistoryMsg::from_bytes(data) {
                    warn!("Hyperliquid TWAP history decode failed after sequencing: {err:#}");
                }
                MONITOR_FAST_POLL_LOW_WEIGHT
            }
            BasicAccountEventType::HyperliquidNativeEvent => {
                // Durable account evidence only. Canonical order/fill/state
                // channels own strategy lifecycle, balances, and positions.
                MONITOR_FAST_POLL_LOW_WEIGHT
            }
            BasicAccountEventType::HyperliquidSpotBalance => {
                if let Err(err) = HyperliquidSpotBalanceMsg::from_bytes(data) {
                    warn!("Hyperliquid spot balance row decode failed: {err:#}");
                }
                MONITOR_FAST_POLL_LOW_WEIGHT
            }
            BasicAccountEventType::HyperliquidPerpDexState => {
                if let Err(err) = self
                    .hyperliquid_snapshot_readiness
                    .validate_perp_dex_state_row(data)
                {
                    error!(
                        "invalid Hyperliquid perpetual DEX state row; snapshot batch failed closed: {err:#}"
                    );
                    self.hyperliquid_snapshot_risk_present = false;
                    self.refresh_hyperliquid_readiness();
                }
                MONITOR_FAST_POLL_LOW_WEIGHT
            }
            BasicAccountEventType::HyperliquidSnapshotComplete => {
                match HyperliquidSnapshotCompleteMsg::from_bytes(data) {
                    Ok(msg) => {
                        let expected_account_hash = self
                            .hyperliquid_fact_replay
                            .as_ref()
                            .map(|consumer| consumer.protocol.account_hash);
                        if expected_account_hash != Some(msg.account_hash) {
                            error!(
                                "drop Hyperliquid snapshot control with mismatched account identity; readiness failed closed"
                            );
                            if let Some(consumer) = self.hyperliquid_fact_replay.as_mut() {
                                consumer.protocol.fail_closed();
                            }
                            self.hyperliquid_snapshot_readiness =
                                HyperliquidLiveSnapshotReadiness::default();
                            self.hyperliquid_snapshot_risk_present = false;
                            self.refresh_hyperliquid_readiness();
                            return MONITOR_FAST_POLL_LOW_WEIGHT;
                        }
                        let producer_epoch_changed = self
                            .hyperliquid_fact_replay
                            .as_ref()
                            .is_some_and(|consumer| {
                                consumer.protocol.is_ready()
                                    && consumer.protocol.monitor_id != msg.monitor_id
                            });
                        if producer_epoch_changed {
                            self.restart_hyperliquid_fact_handshake(
                                "snapshot producer epoch changed",
                            );
                        }
                        let venue = TradingVenue::from_u8(msg.venue);
                        if let Some(
                            venue @ (TradingVenue::HyperliquidMargin
                            | TradingVenue::HyperliquidFutures),
                        ) = venue
                        {
                            if scope_matches_venue(
                                account_scope,
                                self.exchange,
                                venue,
                                self.binance_account_mode,
                                self.hyperliquid_account_mode,
                            ) {
                                let now_ms = get_timestamp_us() / 1_000;
                                if self.hyperliquid_snapshot_readiness.monitor_id != msg.monitor_id
                                {
                                    self.hyperliquid_snapshot_risk_present = false;
                                }
                                let control_result = self
                                    .hyperliquid_snapshot_readiness
                                    .apply_control(venue, &msg, now_ms);
                                match control_result {
                                    Ok(HyperliquidSnapshotPhase::Begin) => {
                                        self.clear_hyperliquid_snapshot_scope(account_scope, venue);
                                    }
                                    Ok(HyperliquidSnapshotPhase::Invalidate) => {}
                                    Ok(HyperliquidSnapshotPhase::Complete) => {}
                                    Err(err) => {
                                        warn!(
                                            "drop invalid Hyperliquid snapshot control: venue={venue:?} scope={} err={err:#}",
                                            account_scope.as_str()
                                        );
                                        return MONITOR_FAST_POLL_LOW_WEIGHT;
                                    }
                                }

                                self.refresh_hyperliquid_readiness();
                            } else {
                                warn!(
                                    "drop Hyperliquid snapshot marker with mismatched scope: venue={venue:?} scope={}",
                                    account_scope.as_str()
                                );
                            }
                        } else {
                            warn!(
                                "drop Hyperliquid snapshot marker with invalid venue={}",
                                msg.venue
                            );
                        }
                    }
                    Err(err) => warn!("Hyperliquid snapshot marker decode failed: {err:#}"),
                }
                MONITOR_FAST_POLL_LOW_WEIGHT
            }
            BasicAccountEventType::AccountRisk => match if self.exchange == Exchange::Hyperliquid
                && account_scope == BasicAccountScope::HyperliquidPortfolioMargin
            {
                decode_hyperliquid_portfolio_risk(data)
            } else {
                BasicAccountRiskMsg::from_bytes(data)
            } {
                Ok(msg) => {
                    if self.exchange == Exchange::Hyperliquid
                        && matches!(
                            account_scope,
                            BasicAccountScope::HyperliquidStdPerp
                                | BasicAccountScope::HyperliquidUnified
                                | BasicAccountScope::HyperliquidPortfolioMargin
                        )
                    {
                        self.hyperliquid_snapshot_risk_present = true;
                    }
                    crate::pre_trade::account_open_block::apply_bitget_unified_account_risk(&msg);
                    crate::pre_trade::account_open_block::apply_bybit_unified_account_risk(&msg);
                    crate::pre_trade::unimmr_open_lock::UnimmrOpenLock::apply_account_risk(
                        account_scope,
                        &msg,
                    );
                    crate::pre_trade::unimmr_force_close::UnimmrForceClose::apply_account_risk(
                        account_scope,
                        &msg,
                    );
                    MonitorChannel::instance().apply_account_risk(account_scope, msg);
                    MonitorChannel::mark_basic_state_dirty();
                    MONITOR_FAST_POLL_LOW_WEIGHT
                }
                Err(err) => {
                    warn!(
                        "AccountRisk decode failed: scope={} err={err:#}",
                        account_scope.as_str()
                    );
                    if self.exchange == Exchange::Hyperliquid
                        && account_scope == BasicAccountScope::HyperliquidPortfolioMargin
                    {
                        self.hyperliquid_snapshot_readiness.spot.fail_closed();
                        self.hyperliquid_snapshot_risk_present = false;
                        self.refresh_hyperliquid_readiness();
                    }
                    MONITOR_FAST_POLL_LOW_WEIGHT
                }
            },
            BasicAccountEventType::BinanceStdUmWalletSnapshot => {
                match BinanceStdUmWalletSnapshotMsg::from_bytes(data) {
                    Ok(msg) => {
                        if account_scope == BasicAccountScope::BinanceStdCm {
                            BinanceStdCmMarginGuard::apply_wallet_snapshot(&msg);
                        } else {
                            BinanceStdUmMarginGuard::apply_wallet_snapshot(&msg);
                            MonitorChannel::instance().apply_binance_std_um_wallet_snapshot(msg);
                            RebalanceUsdtService::drive_from_account_update("um_wallet_snapshot");
                        }
                        MonitorChannel::mark_basic_state_dirty();
                    }
                    Err(err) => {
                        warn!("Binance std UM wallet snapshot decode failed: {err:#}");
                    }
                }
                MONITOR_FAST_POLL_LOW_WEIGHT
            }
            BasicAccountEventType::HyperliquidFactReplayControl | BasicAccountEventType::Error => {
                MONITOR_FAST_POLL_LOW_WEIGHT
            }
        };
        weight
    }
}

struct DerivativesPriceListener {
    price_table: Rc<RefCell<PriceTable>>,
    node_name: String,
    service_name: String,
    print_each_mark_price: bool,
    mark_price_log_interval: Duration,
    last_mark_price_log_at: Instant,
    mark_price_samples_since_log: u64,
    last_mark_price: Option<(String, f64, i64)>,
    node: Node<ipc::Service>,
    subscriber: Option<Subscriber<ipc::Service, [u8; DERIVATIVES_PAYLOAD], ()>>,
    next_open_attempt_at: Instant,
}

impl DerivativesPriceListener {
    fn new(
        price_table: Rc<RefCell<PriceTable>>,
        node_name: String,
        service_name: String,
    ) -> Result<Self> {
        let print_each_mark_price = std::env::var_os("PRE_TRADE_PRINT_EACH_MARKPRICE").is_some();
        let mark_price_log_interval = std::env::var("PRE_TRADE_MARKPRICE_LOG_INTERVAL_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(|secs| Duration::from_secs(secs.max(1)))
            .unwrap_or_else(|| Duration::from_secs(5));
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()?;
        Ok(Self {
            price_table,
            node_name,
            service_name,
            print_each_mark_price,
            mark_price_log_interval,
            last_mark_price_log_at: Instant::now(),
            mark_price_samples_since_log: 0,
            last_mark_price: None,
            node,
            subscriber: None,
            next_open_attempt_at: Instant::now(),
        })
    }

    fn ensure_subscriber(&mut self) -> bool {
        if self.subscriber.is_some() {
            return true;
        }
        let now = Instant::now();
        if now < self.next_open_attempt_at {
            return false;
        }
        let service_name_obj = match ServiceName::new(&self.service_name) {
            Ok(name) => name,
            Err(err) => {
                warn!(
                    "invalid derivatives service name: service={} err={:?}",
                    self.service_name, err
                );
                self.next_open_attempt_at = Instant::now() + Duration::from_millis(500);
                return false;
            }
        };
        let service = match self
            .node
            .service_builder(&service_name_obj)
            .publish_subscribe::<[u8; DERIVATIVES_PAYLOAD]>()
            .max_publishers(1)
            .max_subscribers(DERIVATIVES_MAX_SUBSCRIBERS)
            .history_size(DERIVATIVES_HISTORY_SIZE)
            .subscriber_max_buffer_size(DERIVATIVES_SUBSCRIBER_MAX_BUFFER)
            .open()
        {
            Ok(service) => service,
            Err(err) => {
                warn!(
                    "waiting for derivatives service: node={} service={} err={:?}",
                    self.node_name, self.service_name, err
                );
                self.next_open_attempt_at = Instant::now() + Duration::from_millis(500);
                return false;
            }
        };
        match service.subscriber_builder().create() {
            Ok(subscriber) => {
                info!(
                    "derivatives price stream subscribed: node={} service={}",
                    self.node_name, self.service_name
                );
                self.subscriber = Some(subscriber);
                true
            }
            Err(err) => {
                warn!(
                    "derivatives subscriber create failed: node={} service={} err={:?}",
                    self.node_name, self.service_name, err
                );
                self.next_open_attempt_at = Instant::now() + Duration::from_millis(500);
                false
            }
        }
    }

    fn drain_pending_limit(
        &mut self,
        max_tokens: usize,
        max_raw_messages: usize,
    ) -> (bool, usize, usize) {
        if !self.ensure_subscriber() {
            return (false, 0, 0);
        }
        let mut has_message = false;
        let mut consumed_tokens = 0usize;
        let mut received = 0usize;
        while consumed_tokens < max_tokens && received < max_raw_messages {
            let receive_result = self
                .subscriber
                .as_ref()
                .expect("derivatives subscriber should exist after ensure_subscriber")
                .receive();
            match receive_result {
                Ok(Some(sample)) => {
                    received += 1;
                    has_message = true;
                    consumed_tokens =
                        consumed_tokens.saturating_add(self.process_payload(sample.payload()));
                }
                Ok(None) => break,
                Err(err) => {
                    warn!(
                        "derivatives stream receive error, reconnecting: node={} service={} err={}",
                        self.node_name, self.service_name, err
                    );
                    self.subscriber = None;
                    self.next_open_attempt_at = Instant::now() + Duration::from_millis(200);
                    break;
                }
            }
        }
        (has_message, consumed_tokens, received)
    }

    fn process_payload(&mut self, payload: &[u8]) -> usize {
        if payload.is_empty() {
            return MONITOR_FAST_POLL_LOW_WEIGHT;
        }
        let Some(msg_type) = get_msg_type(payload) else {
            return MONITOR_FAST_POLL_LOW_WEIGHT;
        };
        match msg_type {
            MktMsgType::MarkPrice => match parse_mark_price(payload) {
                Ok(msg) => {
                    self.mark_price_samples_since_log += 1;
                    let is_first_mark_price = self.last_mark_price.is_none();
                    self.last_mark_price =
                        Some((msg.symbol.clone(), msg.mark_price, msg.timestamp));
                    if self.print_each_mark_price {
                        info!(
                            "mark price received: symbol={} mark_price={} ts={}",
                            msg.symbol, msg.mark_price, msg.timestamp
                        );
                    } else if is_first_mark_price {
                        let (symbol, mark_price, ts) = self
                            .last_mark_price
                            .as_ref()
                            .expect("last mark price set above");
                        info!(
                            "mark price stream live: samples={} last_symbol={} last_mark_price={} last_ts={}",
                            self.mark_price_samples_since_log, symbol, mark_price, ts
                        );
                        self.mark_price_samples_since_log = 0;
                        self.last_mark_price_log_at = Instant::now();
                    } else if self.last_mark_price_log_at.elapsed() >= self.mark_price_log_interval
                    {
                        let (symbol, mark_price, ts) = self
                            .last_mark_price
                            .as_ref()
                            .expect("last mark price set above");
                        debug!(
                            "mark price stream live: samples={} last_symbol={} last_mark_price={} last_ts={}",
                            self.mark_price_samples_since_log, symbol, mark_price, ts
                        );
                        self.mark_price_samples_since_log = 0;
                        self.last_mark_price_log_at = Instant::now();
                    }

                    let mut table = self.price_table.borrow_mut();
                    table.update_mark_price(&msg.symbol, msg.mark_price, msg.timestamp);
                    MonitorChannel::mark_basic_state_price_dirty();
                    MONITOR_FAST_POLL_LOW_WEIGHT
                }
                Err(err) => {
                    warn!("parse mark price failed: {err:?}");
                    MONITOR_FAST_POLL_LOW_WEIGHT
                }
            },
            MktMsgType::IndexPrice => match parse_index_price(payload) {
                Ok(msg) => {
                    let mut table = self.price_table.borrow_mut();
                    table.update_index_price(&msg.symbol, msg.index_price, msg.timestamp);
                    MONITOR_FAST_POLL_LOW_WEIGHT
                }
                Err(err) => {
                    warn!("parse index price failed: {err:?}");
                    MONITOR_FAST_POLL_LOW_WEIGHT
                }
            },
            _ => MONITOR_FAST_POLL_LOW_WEIGHT,
        }
    }
}

/// MonitorChannel 内部实现，包含所有状态
struct MonitorChannelInner {
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    arb_mode: ArbMode,
    binance_account_mode: Option<BinanceAccountMode>,
    hyperliquid_account_mode: Option<HyperliquidAccountMode>,
    open_leg: LegMgr,
    hedge_leg: LegMgr,
    /// USDT 单独维护：account_scope -> manager（Binance standard 下 margin/futures 分离）
    usdt_mgrs: HashMap<BasicAccountScope, Rc<RefCell<UsdtBalanceManager>>>,
    /// 价格表（仍使用 Binance mark/index 价格作为统一估值源）
    price_table: Rc<RefCell<PriceTable>>,
    /// 各交易场所的最小下单量/步进信息
    venue_min_qty_tables: HashMap<TradingVenue, Rc<VenueMinQtyTable>>,
    /// 策略管理器
    strategy_mgr: Rc<RefCell<crate::strategy::StrategyManager>>,
    /// orphan 策略管理器（统一承载 mm orphan / arb orphan）
    orphan_strategy_mgr: Rc<RefCell<OrphanStrategyManager>>,
    /// 订单管理器，所有订单维护在其中，完全成交或者撤单会被移除
    order_manager: Rc<RefCell<OrderManager>>,
    /// 本地 ArbClose 可平库存账本。启动/首次访问用账户快照 seed，运行中只相信本地订单回报。
    close_inventory: Rc<RefCell<CloseInventoryLedger>>,
    /// Monotonic counter incremented when a TradeUpdate is received.
    trade_update_seq: u64,
    /// 各账户 scope 最新一份风险快照，由 account_monitor 端 AccountRisk 消息驱动。
    latest_account_risk: HashMap<BasicAccountScope, BasicAccountRiskMsg>,
    /// Binance 标准账户 UM 钱包快照，由 account_monitor WS API balance poll 驱动。
    latest_binance_std_um_wallet: Option<BinanceStdUmWalletSnapshotMsg>,
    arb_startup_net_gate: ArbStartupNetGate,
}

#[derive(Debug, Clone, Default)]
pub struct ArbStartupNetGateStatus {
    pub enabled: bool,
    pub ready: bool,
    pub open_ready: bool,
    pub hedge_ready: bool,
    pub open_ts_us: i64,
    pub hedge_ts_us: i64,
    pub dropped_signals: u64,
}

#[derive(Debug, Clone)]
struct ArbStartupNetGate {
    enabled: bool,
    open_ready: bool,
    hedge_ready: bool,
    open_ts_us: i64,
    hedge_ts_us: i64,
    open_valid_until_ms: Option<i64>,
    hedge_valid_until_ms: Option<i64>,
    dropped_signals: u64,
}

impl ArbStartupNetGate {
    fn new(enabled: bool) -> Self {
        Self {
            enabled,
            open_ready: !enabled,
            hedge_ready: !enabled,
            open_ts_us: 0,
            hedge_ts_us: 0,
            open_valid_until_ms: None,
            hedge_valid_until_ms: None,
            dropped_signals: 0,
        }
    }

    fn ready(&self) -> bool {
        if !self.enabled {
            return true;
        }
        let now_ms = get_timestamp_us() / 1_000;
        let open_fresh = self
            .open_valid_until_ms
            .is_none_or(|deadline| deadline > now_ms);
        let hedge_fresh = self
            .hedge_valid_until_ms
            .is_none_or(|deadline| deadline > now_ms);
        self.open_ready && open_fresh && self.hedge_ready && hedge_fresh
    }

    fn status(&self) -> ArbStartupNetGateStatus {
        let now_ms = get_timestamp_us() / 1_000;
        let open_ready = self.open_ready
            && self
                .open_valid_until_ms
                .is_none_or(|deadline| deadline > now_ms);
        let hedge_ready = self.hedge_ready
            && self
                .hedge_valid_until_ms
                .is_none_or(|deadline| deadline > now_ms);
        ArbStartupNetGateStatus {
            enabled: self.enabled,
            ready: !self.enabled || (open_ready && hedge_ready),
            open_ready,
            hedge_ready,
            open_ts_us: self.open_ts_us,
            hedge_ts_us: self.hedge_ts_us,
            dropped_signals: self.dropped_signals,
        }
    }
}

#[derive(Clone)]
struct BasicState {
    // asset -> (open_qty, hedge_qty), both in base units
    exposures: HashMap<String, (f64, f64)>,
    // account scope -> non-USDT margin net balances by asset, in base units.
    margin_balances_by_scope: HashMap<BasicAccountScope, HashMap<String, f64>>,
    // account scope -> USDT net position.
    usdt_equity_by_scope: HashMap<BasicAccountScope, f64>,
    // account scope -> futures UPL that should be included in eq.
    um_unrealized_equity_by_scope: HashMap<BasicAccountScope, f64>,
    // AccountRisk actual equity overrides local mark-to-market eq for one shared account scope.
    account_risk_equity_override: Option<(BasicAccountScope, f64)>,
    // asset -> net exposure valued in USDT at cache-refresh time.
    exposure_usdt_by_asset: HashMap<String, f64>,
    // asset -> mark price in USDT at cache-refresh time.
    mark_usdt_by_asset: HashMap<String, f64>,
    // asset -> gross position (|open_qty| + |hedge_qty|) valued in USDT.
    position_usdt_by_asset: HashMap<String, f64>,
    total_equity_usdt: f64,
    abs_total_exposure_usdt: f64,
    total_position_usdt: f64,
    total_um_unrealized_usdt: f64,
}

struct BasicStatePriceUpdate {
    exposure_usdt_by_asset: HashMap<String, f64>,
    mark_usdt_by_asset: HashMap<String, f64>,
    position_usdt_by_asset: HashMap<String, f64>,
    total_equity_usdt: f64,
    abs_total_exposure_usdt: f64,
    total_position_usdt: f64,
}

fn missing_position_mark_assets(
    exposures: &HashMap<String, (f64, f64)>,
    mark_usdt_by_asset: &HashMap<String, f64>,
) -> Vec<String> {
    let mut missing = exposures
        .iter()
        .filter_map(|(asset, (open_qty, hedge_qty))| {
            if is_exposure_exempt_asset(asset)
                || open_qty.abs() + hedge_qty.abs() <= POSITION_MARK_QTY_EPSILON
            {
                return None;
            }
            let has_mark = mark_usdt_by_asset
                .get(asset)
                .is_some_and(|mark| mark.is_finite() && *mark > 0.0);
            (!has_mark).then(|| asset.clone())
        })
        .collect::<Vec<_>>();
    missing.sort_unstable();
    missing
}

fn top_two_gross_position_symbols(positions_by_asset: &HashMap<String, f64>) -> Vec<String> {
    const POSITION_EPSILON_USDT: f64 = 1e-6;
    const LIMIT: usize = 2;

    let mut positions_by_symbol = BTreeMap::new();
    for (asset, position_usdt) in positions_by_asset {
        if !(position_usdt.is_finite() && *position_usdt > POSITION_EPSILON_USDT) {
            continue;
        }
        let asset = asset.trim().to_ascii_uppercase();
        if asset.is_empty() || is_exposure_exempt_asset(&asset) {
            continue;
        }
        let symbol = if asset.ends_with("USDT") {
            normalize_symbol_for_internal(&asset)
        } else {
            normalize_symbol_for_internal(&format!("{asset}USDT"))
        };
        *positions_by_symbol.entry(symbol).or_insert(0.0) += position_usdt;
    }

    let mut ranked = positions_by_symbol.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|(symbol_a, position_a), (symbol_b, position_b)| {
        position_b
            .total_cmp(position_a)
            .then_with(|| symbol_a.cmp(symbol_b))
    });
    ranked
        .into_iter()
        .take(LIMIT)
        .map(|(symbol, _)| symbol)
        .collect()
}

impl BasicState {
    fn apply_price_update(&mut self, update: BasicStatePriceUpdate) {
        self.exposure_usdt_by_asset = update.exposure_usdt_by_asset;
        self.mark_usdt_by_asset = update.mark_usdt_by_asset;
        self.position_usdt_by_asset = update.position_usdt_by_asset;
        self.total_equity_usdt = update.total_equity_usdt;
        self.abs_total_exposure_usdt = update.abs_total_exposure_usdt;
        self.total_position_usdt = update.total_position_usdt;
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ExecPositionImbalanceProjection {
    pub current_long_usdt: f64,
    pub current_short_usdt: f64,
    pub next_long_usdt: f64,
    pub next_short_usdt: f64,
    pub current_total_usdt: f64,
    pub next_total_usdt: f64,
    pub current_imbalance_ratio: f64,
    pub next_imbalance_ratio: f64,
    pub limit_ratio: f64,
}

#[derive(Debug, Clone, Copy)]
struct ArbHedgeExposureProjection {
    symbol_current_exposure_usdt: f64,
    symbol_next_exposure_usdt: f64,
    symbol_limit_usdt: f64,
    total_current_exposure_usdt: f64,
    total_next_exposure_usdt: f64,
    total_limit_usdt: f64,
}

#[derive(Debug, Clone)]
pub enum OpenExposureRiskError {
    Symbol(String),
    Total(String),
}

struct MaxPosUCheckCtx<'a> {
    symbol: &'a str,
    base_asset: &'a str,
    venue: TradingVenue,
    price_source: &'static str,
    mark_symbol: &'a str,
    price: f64,
    qty_unit: &'static str,
    raw_qty: f64,
    fut_symbol_key: Option<&'a str>,
    qty_multiplier: Option<f64>,
    current_open_qty: f64,
    add_base_qty: f64,
    max_pos_u: f64,
}

impl MonitorChannel {
    /// 获取全局单例实例
    pub fn instance() -> Self {
        MonitorChannel
    }

    pub fn drain_pending_state_updates() -> bool {
        Self::drain_pending_state_updates_with_refresh().0
    }

    pub fn drain_pending_state_updates_with_refresh() -> (bool, bool) {
        Self::drain_pending_state_updates_with_refresh_limit(usize::MAX)
    }

    pub fn drain_pending_state_updates_limit(max_messages: usize) -> bool {
        let (has_message, _) = MONITOR_STATE_LISTENERS.with(|listeners| {
            let mut listeners = listeners.borrow_mut();
            match listeners.as_mut() {
                Some(listeners) => listeners.drain_pending_limit(max_messages),
                None => (false, 0),
            }
        });
        has_message
    }

    pub fn drain_pending_state_updates_with_refresh_limit(max_messages: usize) -> (bool, bool) {
        let mut has_message = Self::drain_pending_state_updates_limit(max_messages);
        let refreshed = Self::refresh_basic_state_if_due_after_monitor_drain(has_message);
        if refreshed {
            has_message = true;
        }
        (has_message, refreshed)
    }

    pub fn refresh_basic_state_if_due_after_fast_poll(has_monitor_message: bool) -> bool {
        Self::refresh_basic_state_if_due_after_monitor_drain(has_monitor_message)
    }

    fn refresh_basic_state_if_due_after_monitor_drain(has_message: bool) -> bool {
        let mut refreshed = false;
        let state_dirty = Self::basic_state_any_dirty();
        if state_dirty && (has_message || Self::basic_state_cache_present()) {
            refreshed = Self::refresh_basic_state_cache_if_due(false);
            if refreshed {
                let risk_start_us = get_timestamp_us();
                Self::drain_pending_risk_checks_after_refresh();
                record_stage_latency(
                    ReactorStage::MonitorPendingRisk,
                    risk_start_us,
                    get_timestamp_us(),
                );
            }
        }
        refreshed
    }

    pub(crate) fn mark_basic_state_dirty() {
        BASIC_STATE_DIRTY.with(|dirty| dirty.set(true));
        BASIC_STATE_PRICE_DIRTY.with(|dirty| dirty.set(false));
    }

    fn mark_basic_state_price_dirty() {
        if BASIC_STATE_DIRTY.with(|dirty| dirty.get()) {
            return;
        }
        BASIC_STATE_PRICE_DIRTY.with(|dirty| dirty.set(true));
    }

    fn basic_state_any_dirty() -> bool {
        BASIC_STATE_DIRTY.with(|dirty| dirty.get())
            || BASIC_STATE_PRICE_DIRTY.with(|dirty| dirty.get())
            || Self::basic_state_cache_stale()
    }

    fn clear_basic_state_runtime_cache() {
        BASIC_STATE_CACHE.with(|cache| {
            *cache.borrow_mut() = None;
        });
        BASIC_STATE_LAST_REFRESH_US.with(|last| last.set(0));
        BASIC_STATE_DIRTY.with(|dirty| dirty.set(true));
        BASIC_STATE_PRICE_DIRTY.with(|dirty| dirty.set(false));
        PENDING_RISK_CHECKS.with(|pending| {
            *pending.borrow_mut() = PendingRiskChecks::default();
        });
    }

    fn leg_cache_key(leg: &LegMgr) -> usize {
        match leg {
            LegMgr::Margin { bal, .. } => Rc::as_ptr(bal) as usize,
            LegMgr::Futures {
                um, min_qty_table, ..
            } => (Rc::as_ptr(um) as usize) ^ (Rc::as_ptr(min_qty_table) as usize).rotate_left(17),
        }
    }

    fn basic_state_cache_key(inner: &MonitorChannelInner) -> usize {
        let min_non_trading_position_bits = PreTradeParamsLoader::instance()
            .min_non_trading_position_usdt()
            .to_bits();
        (inner.open_venue.to_u8() as usize)
            ^ ((inner.hedge_venue.to_u8() as usize) << 8)
            ^ (Rc::as_ptr(&inner.price_table) as usize).rotate_left(3)
            ^ (Rc::as_ptr(&inner.order_manager) as usize).rotate_left(7)
            ^ Self::leg_cache_key(&inner.open_leg).rotate_left(11)
            ^ Self::leg_cache_key(&inner.hedge_leg).rotate_left(19)
            ^ (hash64(&[min_non_trading_position_bits]) as usize).rotate_left(23)
    }

    fn basic_state_cache_stale() -> bool {
        let Some(key) = Self::try_with_inner(Self::basic_state_cache_key) else {
            return false;
        };
        BASIC_STATE_CACHE.with(|cache| {
            cache
                .borrow()
                .as_ref()
                .is_some_and(|(cached_key, _)| *cached_key != key)
        })
    }

    fn ensure_basic_state_cache_current() {
        if Self::basic_state_cache_stale() {
            Self::refresh_basic_state_cache();
        }
    }

    fn refresh_basic_state_cache() {
        let refresh_start_us = get_timestamp_us();
        let (key, state) = Self::with_inner(|inner| {
            (
                Self::basic_state_cache_key(inner),
                Self::compute_basic_state(inner),
            )
        });
        BASIC_STATE_CACHE.with(|cache| {
            *cache.borrow_mut() = Some((key, state));
        });
        BASIC_STATE_DIRTY.with(|dirty| dirty.set(false));
        BASIC_STATE_PRICE_DIRTY.with(|dirty| dirty.set(false));
        BASIC_STATE_LAST_REFRESH_US.with(|last| last.set(get_timestamp_us()));
        record_stage_latency(
            ReactorStage::MonitorRefreshBasicState,
            refresh_start_us,
            get_timestamp_us(),
        );
    }

    fn basic_state_cache_present() -> bool {
        BASIC_STATE_CACHE.with(|cache| cache.borrow().is_some())
    }

    fn refresh_basic_state_price_cache() -> bool {
        let refresh_start_us = get_timestamp_us();
        let updated = Self::with_inner(|inner| {
            let key = Self::basic_state_cache_key(inner);
            let price_table = inner.price_table.borrow();
            BASIC_STATE_CACHE.with(|cache| {
                let mut cache = cache.borrow_mut();
                let Some((cached_key, state)) = cache.as_mut() else {
                    return false;
                };
                if *cached_key != key {
                    return false;
                }
                let price_update = Self::compute_basic_state_price_update_from_parts(
                    inner,
                    &price_table,
                    &state.exposures,
                    &state.margin_balances_by_scope,
                    &state.usdt_equity_by_scope,
                    &state.um_unrealized_equity_by_scope,
                    state.account_risk_equity_override,
                );
                state.apply_price_update(price_update);
                true
            })
        });
        if updated {
            BASIC_STATE_PRICE_DIRTY.with(|dirty| dirty.set(false));
            BASIC_STATE_LAST_REFRESH_US.with(|last| last.set(get_timestamp_us()));
            record_stage_latency(
                ReactorStage::MonitorRefreshBasicState,
                refresh_start_us,
                get_timestamp_us(),
            );
        }
        updated
    }

    fn refresh_basic_state_cache_if_due(force: bool) -> bool {
        if force {
            Self::refresh_basic_state_cache();
            return true;
        }
        let cache_stale = Self::basic_state_cache_stale();
        if Self::basic_state_cache_present() && !cache_stale {
            let now_us = get_timestamp_us();
            let last_us = BASIC_STATE_LAST_REFRESH_US.with(|last| last.get());
            if last_us > 0 && now_us.saturating_sub(last_us) < BASIC_STATE_REFRESH_MIN_INTERVAL_US {
                return false;
            }
        }
        let full_dirty = BASIC_STATE_DIRTY.with(|dirty| dirty.get());
        let price_dirty = BASIC_STATE_PRICE_DIRTY.with(|dirty| dirty.get());
        if !full_dirty && !cache_stale {
            if !price_dirty {
                return false;
            }
            if Self::refresh_basic_state_price_cache() {
                return true;
            }
        }
        Self::refresh_basic_state_cache();
        true
    }

    fn basic_state_cached() -> BasicState {
        Self::ensure_basic_state_cache_current();
        let key = Self::with_inner(Self::basic_state_cache_key);
        BASIC_STATE_CACHE.with(|cache| {
            cache
                .borrow()
                .as_ref()
                .and_then(|(cached_key, state)| (*cached_key == key).then(|| state.clone()))
                .expect("BasicState cache missing; refresh_basic_state_cache must run after init/drain before risk checks")
        })
    }

    fn with_basic_state_cached<R>(f: impl FnOnce(&BasicState) -> R) -> R {
        Self::ensure_basic_state_cache_current();
        let key = Self::with_inner(Self::basic_state_cache_key);
        BASIC_STATE_CACHE.with(|cache| {
            let cache = cache.borrow();
            let state = cache
                .as_ref()
                .and_then(|(cached_key, state)| (*cached_key == key).then_some(state))
                .expect("BasicState cache missing; refresh_basic_state_cache must run after init/drain before risk checks");
            f(state)
        })
    }

    fn queue_mm_position_risk_check(symbol: &str, e_ts: i64) {
        let symbol = normalize_symbol_for_internal(symbol);
        if symbol.is_empty() {
            return;
        }
        PENDING_RISK_CHECKS.with(|pending| {
            let mut pending = pending.borrow_mut();
            PendingRiskChecks::insert_latest(&mut pending.mm_position_symbols, symbol, e_ts);
        });
    }

    fn queue_arb_position_risk_check(symbol: &str, e_ts: i64) {
        let symbol = normalize_symbol_for_internal(symbol);
        if symbol.is_empty() {
            return;
        }
        PENDING_RISK_CHECKS.with(|pending| {
            let mut pending = pending.borrow_mut();
            PendingRiskChecks::insert_latest(&mut pending.arb_position_symbols, symbol, e_ts);
        });
    }

    fn queue_arb_margin_net_risk_check(asset: &str, e_ts: i64) -> bool {
        let asset = asset.trim().to_uppercase();
        if asset.is_empty() || is_exposure_exempt_asset(&asset) {
            return false;
        }
        PENDING_RISK_CHECKS.with(|pending| {
            let mut pending = pending.borrow_mut();
            PendingRiskChecks::insert_latest(&mut pending.arb_margin_assets, asset, e_ts);
        });
        true
    }

    fn queue_arb_startup_net_check(ready_ts: i64) {
        PENDING_RISK_CHECKS.with(|pending| {
            let mut pending = pending.borrow_mut();
            pending.arb_startup_ready_ts = pending.arb_startup_ready_ts.max(ready_ts);
        });
    }

    fn drain_pending_risk_checks_after_refresh() {
        let pending =
            PENDING_RISK_CHECKS.with(|pending| std::mem::take(&mut *pending.borrow_mut()));

        if pending.arb_startup_ready_ts > 0 {
            let checked = Self::with_inner(|inner| {
                Self::initialize_arb_startup_stable_net_pending_inner(
                    inner,
                    pending.arb_startup_ready_ts,
                )
            });
            info!(
                "Arb startup stable net post-refresh check completed: checked_symbols={}",
                checked
            );
        }

        let mon = Self::instance();
        for (symbol, e_ts) in pending.mm_position_symbols {
            mon.handle_mm_position_risk_after_update(&symbol, e_ts);
        }
        for (symbol, e_ts) in pending.arb_position_symbols {
            mon.handle_arb_position_risk_after_update(&symbol, e_ts);
        }
        for (asset, e_ts) in pending.arb_margin_assets {
            mon.handle_arb_open_margin_net_risk_after_update(&asset, e_ts);
        }
    }

    /// 访问内部状态的辅助方法（内部使用）
    fn with_inner<F, R>(f: F) -> R
    where
        F: FnOnce(&MonitorChannelInner) -> R,
    {
        MONITOR_CHANNEL.with(|mc| {
            let mc_ref = mc.borrow();
            let inner = mc_ref.as_ref().expect("MonitorChannel not initialized");
            f(inner)
        })
    }

    fn try_with_inner<F, R>(f: F) -> Option<R>
    where
        F: FnOnce(&MonitorChannelInner) -> R,
    {
        MONITOR_CHANNEL
            .try_with(|mc| {
                let mc_ref = mc.borrow();
                mc_ref.as_ref().map(f)
            })
            .ok()
            .flatten()
    }

    fn with_inner_mut<F, R>(f: F) -> R
    where
        F: FnOnce(&mut MonitorChannelInner) -> R,
    {
        MONITOR_CHANNEL.with(|mc| {
            let mut mc_ref = mc.borrow_mut();
            let inner = mc_ref.as_mut().expect("MonitorChannel not initialized");
            f(inner)
        })
    }

    fn mark_arb_startup_net_seen_for_venue_inner(
        inner: &mut MonitorChannelInner,
        venue: TradingVenue,
        source: &'static str,
    ) {
        if !inner.arb_startup_net_gate.enabled || inner.arb_startup_net_gate.ready() {
            return;
        }

        let now = get_timestamp_us();
        let mut changed = false;
        if venue == inner.open_venue && !inner.arb_startup_net_gate.open_ready {
            inner.arb_startup_net_gate.open_ready = true;
            inner.arb_startup_net_gate.open_ts_us = now;
            inner.arb_startup_net_gate.open_valid_until_ms = None;
            changed = true;
            info!(
                "Arb startup net gate: open leg net initialized venue={:?} source={}",
                venue, source
            );
        }
        if venue == inner.hedge_venue && !inner.arb_startup_net_gate.hedge_ready {
            inner.arb_startup_net_gate.hedge_ready = true;
            inner.arb_startup_net_gate.hedge_ts_us = now;
            inner.arb_startup_net_gate.hedge_valid_until_ms = None;
            changed = true;
            info!(
                "Arb startup net gate: hedge leg net initialized venue={:?} source={}",
                venue, source
            );
        }

        if changed && inner.arb_startup_net_gate.ready() {
            Self::queue_arb_startup_net_check(now);
            info!(
                "Arb startup net gate released: 双边net已初始化 open_venue={:?} hedge_venue={:?} open_ts_us={} hedge_ts_us={} dropped_signals={} startup_net_check_queued=true pending_write=false",
                inner.open_venue,
                inner.hedge_venue,
                inner.arb_startup_net_gate.open_ts_us,
                inner.arb_startup_net_gate.hedge_ts_us,
                inner.arb_startup_net_gate.dropped_signals
            );
        }
    }

    pub fn mark_arb_startup_net_seen_for_venue(&self, venue: TradingVenue, source: &'static str) {
        if matches!(
            venue,
            TradingVenue::HyperliquidMargin | TradingVenue::HyperliquidFutures
        ) {
            return;
        }
        Self::with_inner_mut(|inner| {
            Self::mark_arb_startup_net_seen_for_venue_inner(inner, venue, source);
        });
    }

    fn set_hyperliquid_arb_snapshot_readiness(
        &self,
        venue: TradingVenue,
        valid_until_ms: Option<i64>,
    ) {
        Self::with_inner_mut(|inner| {
            if exchange_from_venue(venue) != Exchange::Hyperliquid {
                return;
            }
            let was_ready = inner.arb_startup_net_gate.ready();
            let factual_ready = HYPERLIQUID_FACT_STREAM_READY.with(Cell::get);
            let ready = factual_ready && valid_until_ms.is_some();
            let valid_until_ms = ready.then_some(valid_until_ms).flatten();
            let now_us = get_timestamp_us();
            if venue == inner.open_venue {
                inner.arb_startup_net_gate.open_ready = ready;
                inner.arb_startup_net_gate.open_valid_until_ms = valid_until_ms;
                if ready {
                    inner.arb_startup_net_gate.open_ts_us = now_us;
                }
            }
            if venue == inner.hedge_venue {
                inner.arb_startup_net_gate.hedge_ready = ready;
                inner.arb_startup_net_gate.hedge_valid_until_ms = valid_until_ms;
                if ready {
                    inner.arb_startup_net_gate.hedge_ts_us = now_us;
                }
            }
            let is_ready = inner.arb_startup_net_gate.ready();
            if !was_ready && is_ready {
                Self::queue_arb_startup_net_check(now_us);
                info!(
                    "Hyperliquid arb account snapshot gate released: venue={venue:?} valid_until_ms={:?}",
                    valid_until_ms
                );
            } else if was_ready && !is_ready {
                warn!("Hyperliquid arb account snapshot gate revoked: venue={venue:?}");
            }
        });
    }

    pub fn arb_startup_net_gate_status(&self) -> ArbStartupNetGateStatus {
        Self::with_inner(|inner| inner.arb_startup_net_gate.status())
    }

    pub fn record_arb_startup_net_gate_signal_drop(&self) -> ArbStartupNetGateStatus {
        Self::with_inner_mut(|inner| {
            inner.arb_startup_net_gate.dropped_signals =
                inner.arb_startup_net_gate.dropped_signals.saturating_add(1);
            inner.arb_startup_net_gate.status()
        })
    }

    fn initialize_arb_startup_stable_net_pending_inner(
        inner: &MonitorChannelInner,
        ready_ts: i64,
    ) -> usize {
        let state = Self::basic_state_cached();
        if state.exposures.is_empty() {
            return 0;
        }

        let price_mapper = create_symbol_mapper(Self::mark_price_exchange_for_venues(
            inner.open_venue,
            inner.hedge_venue,
        ));
        let price_table = inner.price_table.borrow();
        let mut checked = 0usize;
        let mut rows: Vec<(String, f64, f64, f64)> = state
            .exposures
            .into_iter()
            .filter_map(|(asset, (open_qty, hedge_qty))| {
                if is_exposure_exempt_asset(&asset) {
                    return None;
                }
                let net_qty = open_qty + hedge_qty;
                if net_qty.abs() <= 1e-12 {
                    return None;
                }
                Some((asset, open_qty, hedge_qty, net_qty))
            })
            .collect();
        rows.sort_by(|lhs, rhs| lhs.0.cmp(&rhs.0));

        for (asset, open_qty, hedge_qty, net_qty) in rows {
            let symbol = normalize_symbol_for_internal(&price_mapper.asset_to_price_symbol(&asset));
            if symbol.is_empty() {
                continue;
            }
            let price_symbol = price_mapper.asset_to_price_symbol(&asset);
            let price = price_table.mark_price(&price_symbol).unwrap_or(0.0);
            if price <= 0.0 {
                warn!(
                    "Arb startup stable net check skipped: symbol={} asset={} open_qty={:.8} hedge_qty={:.8} net_qty={:.8} missing mark price, threshold_usdt={:.2} ready_ts={}",
                    symbol,
                    asset,
                    open_qty,
                    hedge_qty,
                    net_qty,
                    ARB_STARTUP_NET_EXPOSURE_WARN_USDT,
                    ready_ts
                );
                continue;
            }
            let exposure_usdt = net_qty.abs() * price;
            if exposure_usdt > ARB_STARTUP_NET_EXPOSURE_WARN_USDT {
                warn!(
                    "Arb startup stable net exposure too large; startup continues: symbol={} asset={} open_qty={:.8} hedge_qty={:.8} net_qty={:.8} price={:.8} exposure_usdt={:.8} threshold_usdt={:.2} ready_ts={}",
                    symbol,
                    asset,
                    open_qty,
                    hedge_qty,
                    net_qty,
                    price,
                    exposure_usdt,
                    ARB_STARTUP_NET_EXPOSURE_WARN_USDT,
                    ready_ts
                );
                checked += 1;
                continue;
            }
            checked += 1;
            info!(
                "Arb startup stable net checked: symbol={} asset={} open_qty={:.8} hedge_qty={:.8} net_qty={:.8} price={:.8} exposure_usdt={:.8} threshold_usdt={:.2} pending_write=false ready_ts={}",
                symbol,
                asset,
                open_qty,
                hedge_qty,
                net_qty,
                price,
                exposure_usdt,
                ARB_STARTUP_NET_EXPOSURE_WARN_USDT,
                ready_ts
            );
        }
        checked
    }

    pub fn bump_trade_update_seq(&self) {
        Self::with_inner_mut(|inner| {
            inner.trade_update_seq = inner.trade_update_seq.saturating_add(1);
        });
    }

    pub fn trade_update_seq(&self) -> u64 {
        Self::with_inner(|inner| inner.trade_update_seq)
    }

    pub fn reserve_close_inventory(
        &self,
        venue: TradingVenue,
        symbol: &str,
        side: Side,
        requested_base_qty: f64,
        client_order_id: i64,
    ) -> CloseReservationGrant {
        self.reserve_close_inventory_inner(
            venue,
            symbol,
            side,
            requested_base_qty,
            client_order_id,
            true,
        )
    }

    pub fn reserve_close_inventory_silent(
        &self,
        venue: TradingVenue,
        symbol: &str,
        side: Side,
        requested_base_qty: f64,
        client_order_id: i64,
    ) -> CloseReservationGrant {
        self.reserve_close_inventory_inner(
            venue,
            symbol,
            side,
            requested_base_qty,
            client_order_id,
            false,
        )
    }

    fn reserve_close_inventory_inner(
        &self,
        venue: TradingVenue,
        symbol: &str,
        side: Side,
        requested_base_qty: f64,
        client_order_id: i64,
        log_reserve: bool,
    ) -> CloseReservationGrant {
        Self::with_inner(|inner| {
            let snapshot_pos_base = Self::get_position_qty_inner(inner, symbol, venue);
            let first_grant = if log_reserve {
                inner.close_inventory.borrow_mut().reserve_close(
                    venue,
                    symbol,
                    side,
                    requested_base_qty,
                    client_order_id,
                    snapshot_pos_base,
                )
            } else {
                inner.close_inventory.borrow_mut().reserve_close_silent(
                    venue,
                    symbol,
                    side,
                    requested_base_qty,
                    client_order_id,
                    snapshot_pos_base,
                )
            };
            if first_grant.granted_base_qty > 1e-12 {
                return first_grant;
            }

            let snapshot_can_close = match side {
                Side::Sell => snapshot_pos_base > 1e-12,
                Side::Buy => snapshot_pos_base < -1e-12,
            };
            if !snapshot_can_close {
                return first_grant;
            }

            let symbol = normalize_symbol_for_internal(symbol);
            let pending_limit_count = inner
                .order_manager
                .borrow()
                .get_symbol_pending_limit_order_count(&symbol);
            if pending_limit_count != 0 {
                return first_grant;
            }

            let mut close_inventory = inner.close_inventory.borrow_mut();
            close_inventory.force_sync_from_snapshot(
                venue,
                &symbol,
                snapshot_pos_base,
                "close_reserve_fallback_no_pending_limit",
            );
            let retry_grant = if log_reserve {
                close_inventory.reserve_close(
                    venue,
                    &symbol,
                    side,
                    requested_base_qty,
                    client_order_id,
                    snapshot_pos_base,
                )
            } else {
                close_inventory.reserve_close_silent(
                    venue,
                    &symbol,
                    side,
                    requested_base_qty,
                    client_order_id,
                    snapshot_pos_base,
                )
            };
            if log_reserve {
                if retry_grant.granted_base_qty > 1e-12 {
                    info!(
                        "CloseInventory: reserve retry after force sync symbol={} venue={:?} side={:?} client_order_id={} requested={:.8} snapshot_pos={:.8} first_available={:.8} first_inventory={:.8} retry_granted={:.8} retry_available={:.8} retry_inventory={:.8}",
                        symbol,
                        venue,
                        side,
                        client_order_id,
                        requested_base_qty,
                        snapshot_pos_base,
                        first_grant.available_before_base,
                        first_grant.closable_inventory_base,
                        retry_grant.granted_base_qty,
                        retry_grant.available_before_base,
                        retry_grant.closable_inventory_base
                    );
                } else {
                    debug!(
                        "CloseInventory: reserve retry after force sync symbol={} venue={:?} side={:?} client_order_id={} requested={:.8} snapshot_pos={:.8} first_available={:.8} first_inventory={:.8} retry_granted={:.8} retry_available={:.8} retry_inventory={:.8}",
                        symbol,
                        venue,
                        side,
                        client_order_id,
                        requested_base_qty,
                        snapshot_pos_base,
                        first_grant.available_before_base,
                        first_grant.closable_inventory_base,
                        retry_grant.granted_base_qty,
                        retry_grant.available_before_base,
                        retry_grant.closable_inventory_base
                    );
                }
            }
            retry_grant
        })
    }

    pub fn seed_close_inventory_if_absent(&self, venue: TradingVenue, symbol: &str) {
        Self::with_inner(|inner| {
            let snapshot_pos_base = Self::get_position_qty_inner(inner, symbol, venue);
            inner
                .close_inventory
                .borrow_mut()
                .seed_if_absent(venue, symbol, snapshot_pos_base);
        });
    }

    pub fn apply_open_inventory_fill_delta(
        &self,
        venue: TradingVenue,
        symbol: &str,
        side: Side,
        filled_base_delta: f64,
    ) {
        Self::with_inner(|inner| {
            inner.close_inventory.borrow_mut().apply_open_fill_delta(
                venue,
                symbol,
                side,
                filled_base_delta,
            );
        });
    }

    pub fn apply_close_inventory_fill_delta(&self, client_order_id: i64, filled_base_delta: f64) {
        Self::with_inner(|inner| {
            inner
                .close_inventory
                .borrow_mut()
                .apply_close_fill_delta(client_order_id, filled_base_delta);
        });
    }

    pub fn release_close_inventory_unfilled(&self, client_order_id: i64, reason: &str) {
        Self::with_inner(|inner| {
            inner
                .close_inventory
                .borrow_mut()
                .release_close_unfilled(client_order_id, reason);
        });
    }

    pub fn release_close_inventory_unfilled_silent(&self, client_order_id: i64, reason: &str) {
        Self::with_inner(|inner| {
            inner
                .close_inventory
                .borrow_mut()
                .release_close_unfilled_silent(client_order_id, reason);
        });
    }

    pub fn close_inventory_has_reservation(&self, client_order_id: i64) -> bool {
        Self::with_inner(|inner| {
            inner
                .close_inventory
                .borrow()
                .has_reservation(client_order_id)
        })
    }

    /// 获取指定交易场所的最小下单量表
    pub fn venue_min_qty_table(&self, venue: TradingVenue) -> Option<Rc<VenueMinQtyTable>> {
        Self::with_inner(|inner| inner.venue_min_qty_tables.get(&venue).cloned())
    }

    /// 尝试获取指定交易场所的最小下单量表（若 MonitorChannel 未初始化则返回 None）
    pub fn try_venue_min_qty_table(&self, venue: TradingVenue) -> Option<Rc<VenueMinQtyTable>> {
        Self::try_with_inner(|inner| inner.venue_min_qty_tables.get(&venue).cloned()).flatten()
    }

    /// 获取 venue qty -> base qty 的乘数。需要合约乘数的交易所缺失配置时返回错误。
    pub fn qty_multiplier_for_venue(
        &self,
        venue: TradingVenue,
        symbol: &str,
    ) -> Result<f64, String> {
        Self::with_inner(|inner| Self::qty_multiplier_for_venue_inner(inner, venue, symbol))
    }

    /// Resolve venue qty -> base qty at the price used for sizing or execution.
    /// Binance COIN-M is inverse, so its multiplier is contractSize / price.
    pub fn qty_multiplier_for_venue_at_price(
        &self,
        venue: TradingVenue,
        symbol: &str,
        price: f64,
    ) -> Result<f64, String> {
        Self::with_inner(|inner| {
            Self::qty_multiplier_for_venue_at_price_inner(inner, venue, symbol, price)
        })
    }

    /// 获取 order_manager 的引用
    pub fn order_manager(&self) -> Rc<RefCell<OrderManager>> {
        Self::with_inner(|inner| inner.order_manager.clone())
    }

    pub fn try_order_manager() -> Option<Rc<RefCell<OrderManager>>> {
        Self::try_with_inner(|inner| inner.order_manager.clone())
    }

    /// 获取 price_table 的引用
    pub fn price_table(&self) -> Rc<RefCell<PriceTable>> {
        Self::with_inner(|inner| inner.price_table.clone())
    }

    pub fn try_price_table(&self) -> Option<Rc<RefCell<PriceTable>>> {
        Self::try_with_inner(|inner| inner.price_table.clone())
    }

    pub fn mark_price_for_symbol(&self, symbol: &str) -> Option<f64> {
        Self::with_inner(|inner| {
            let base_asset = extract_base_asset_key(symbol)?;
            let price_mapper = create_symbol_mapper(Self::mark_price_exchange_for_venues(
                inner.open_venue,
                inner.hedge_venue,
            ));
            let price_table = inner.price_table.borrow();
            let mark_price =
                Self::mark_price_for_asset(&*price_mapper, &price_table, base_asset.as_ref());
            (mark_price.is_finite() && mark_price > 0.0).then_some(mark_price)
        })
    }

    pub fn open_venue(&self) -> TradingVenue {
        Self::with_inner(|inner| inner.open_venue)
    }

    pub fn hedge_venue(&self) -> TradingVenue {
        Self::with_inner(|inner| inner.hedge_venue)
    }

    pub fn try_venues() -> Option<(TradingVenue, TradingVenue)> {
        Self::try_with_inner(|inner| (inner.open_venue, inner.hedge_venue))
    }

    pub fn arb_mode(&self) -> ArbMode {
        Self::with_inner(|inner| inner.arb_mode)
    }

    /// `e_ts`：见 `cancel_arb_open_strategies_for_symbol_side`，交易所侧事件时间(µs)，
    /// 经 leg.ts → mkt_ts 落到 order；0 表示无上下文，不覆写已有 mkt_t。
    fn cancel_mm_open_strategies_for_symbol_side(
        &self,
        symbol: &str,
        side: Side,
        trigger_ts: i64,
        e_ts: i64,
        reason: MmCancelReason,
    ) -> usize {
        let normalized_symbol = normalize_symbol_for_internal(symbol);
        if normalized_symbol.is_empty() {
            return 0;
        }

        let strategy_mgr = self.strategy_mgr();
        let candidate_ids: Vec<i32> = {
            let mgr = strategy_mgr.borrow();
            mgr.ids_for_symbol(&normalized_symbol)
                .map(|set| set.iter().copied().collect())
                .unwrap_or_default()
        };
        if candidate_ids.is_empty() {
            return 0;
        }

        let open_venue = self.open_venue();
        let mut cancelled = 0usize;
        for strategy_id in candidate_ids {
            let mut strategy = {
                let mut mgr = strategy_mgr.borrow_mut();
                let Some(entry) = mgr.mm_open_price_map_entry(strategy_id).cloned() else {
                    continue;
                };
                if entry.side != side {
                    continue;
                }
                match mgr.take(strategy_id) {
                    Some(strategy) => strategy,
                    None => continue,
                }
            };

            let mut cancel_ctx = MmCancelCtx::new();
            cancel_ctx.opening_leg = TradingLeg {
                venue: open_venue.to_u8(),
                bid0: 0.0,
                bid_qty0: 0.0,
                ask0: 0.0,
                ask_qty0: 0.0,
                ts: e_ts,
            };
            cancel_ctx.set_opening_symbol(&normalized_symbol);
            cancel_ctx.set_side(side);
            cancel_ctx.set_reason(reason);
            cancel_ctx.trigger_ts = trigger_ts;
            cancel_ctx.set_from_key(b"mm_position_risk".to_vec());
            if let Some(entry) = strategy_mgr.borrow().mm_open_price_map_entry(strategy_id) {
                cancel_ctx.set_target_strategy(strategy_id, entry.client_order_id);
            } else {
                cancel_ctx.set_target_strategy(strategy_id, 0);
            }

            let signal = TradeSignal::create(
                SignalType::MMCancel,
                trigger_ts,
                trigger_ts as f64,
                cancel_ctx.to_bytes(),
            );
            strategy.handle_signal(&signal);
            if strategy.is_active() {
                strategy_mgr.borrow_mut().insert(strategy);
            }
            cancelled += 1;
        }

        cancelled
    }

    fn handle_mm_position_risk_after_update(&self, symbol: &str, e_ts: i64) {
        let normalized_symbol = normalize_symbol_for_internal(symbol);
        if normalized_symbol.is_empty() {
            return;
        }
        if self.open_venue() != self.hedge_venue() {
            return;
        }
        if self.check_symbol_exposure(&normalized_symbol).is_ok() {
            return;
        }

        let venue = self.open_venue();
        let net_qty = self.get_position_qty(&normalized_symbol, venue);
        let Some(cancel_side) = (if net_qty > 0.0 {
            Some(Side::Buy)
        } else if net_qty < 0.0 {
            Some(Side::Sell)
        } else {
            None
        }) else {
            return;
        };

        let trigger_ts = get_timestamp_us();
        let cancelled = self.cancel_mm_open_strategies_for_symbol_side(
            &normalized_symbol,
            cancel_side,
            trigger_ts,
            e_ts,
            MmCancelReason::PositionRisk,
        );
        if cancelled > 0 {
            warn!(
                "MM position risk cancel triggered: symbol={} venue={:?} net_qty={:.8} cancel_side={:?} cancelled_strategies={} trigger_ts={}",
                normalized_symbol, venue, net_qty, cancel_side, cancelled, trigger_ts
            );
        }
    }

    /// `e_ts`：触发本次撤单的账户/头寸事件在交易所侧的事件时间(µs)，0 表示无上下文。
    /// 它经 leg.ts → `OpenCancelInput.mkt_ts` → `set_mkt_time` 落到 order 的 mkt_t 维度；
    /// `trigger_ts`（本地墙钟）保留作 signal 生成时间，用于 construct→submit 延迟测度。
    fn cancel_arb_open_strategies_for_symbol_side(
        &self,
        symbol: &str,
        side: Side,
        trigger_ts: i64,
        e_ts: i64,
        reason: ArbCancelReason,
    ) -> usize {
        let normalized_symbol = normalize_symbol_for_internal(symbol);
        if normalized_symbol.is_empty() {
            return 0;
        }

        let strategy_mgr = self.strategy_mgr();
        let candidate_ids: Vec<i32> = {
            let mgr = strategy_mgr.borrow();
            mgr.ids_for_symbol(&normalized_symbol)
                .map(|set| set.iter().copied().collect())
                .unwrap_or_default()
        };
        if candidate_ids.is_empty() {
            return 0;
        }

        let open_venue = self.open_venue();
        let hedge_venue = self.hedge_venue();
        let mut cancelled = 0usize;
        for strategy_id in candidate_ids {
            let mut strategy = {
                let mut mgr = strategy_mgr.borrow_mut();
                let Some(entry) = mgr.arb_open_price_map_entry(strategy_id).cloned() else {
                    continue;
                };
                if entry.side != side {
                    continue;
                }
                match mgr.take(strategy_id) {
                    Some(strategy) => strategy,
                    None => continue,
                }
            };

            let mut cancel_ctx = ArbCancelCtx::new();
            cancel_ctx.opening_leg = TradingLeg {
                venue: open_venue.to_u8(),
                bid0: 0.0,
                bid_qty0: 0.0,
                ask0: 0.0,
                ask_qty0: 0.0,
                ts: e_ts,
            };
            cancel_ctx.set_opening_symbol(&normalized_symbol);
            cancel_ctx.hedging_leg = TradingLeg {
                venue: hedge_venue.to_u8(),
                bid0: 0.0,
                bid_qty0: 0.0,
                ask0: 0.0,
                ask_qty0: 0.0,
                ts: e_ts,
            };
            cancel_ctx.set_hedging_symbol(&normalized_symbol);
            cancel_ctx.set_side(side);
            cancel_ctx.set_reason(reason);
            cancel_ctx.trigger_ts = trigger_ts;
            cancel_ctx.set_from_key(b"arb_position_risk".to_vec());
            cancel_ctx.set_target_strategy(strategy_id);

            let signal = TradeSignal::create(
                SignalType::ArbCancel,
                trigger_ts,
                trigger_ts as f64,
                cancel_ctx.to_bytes(),
            );
            strategy.handle_signal(&signal);
            if strategy.is_active() {
                strategy_mgr.borrow_mut().insert(strategy);
            }
            cancelled += 1;
        }

        cancelled
    }

    fn handle_arb_position_risk_after_update(&self, symbol: &str, e_ts: i64) {
        let normalized_symbol = normalize_symbol_for_internal(symbol);
        if normalized_symbol.is_empty() {
            return;
        }
        if self.open_venue() == self.hedge_venue() {
            return;
        }
        if self.check_symbol_exposure(&normalized_symbol).is_ok() {
            return;
        }

        let open_venue = self.open_venue();
        let hedge_venue = self.hedge_venue();
        let net_qty = self.get_position_qty(&normalized_symbol, open_venue);
        let Some(cancel_side) = (if net_qty > 0.0 {
            Some(Side::Buy)
        } else if net_qty < 0.0 {
            Some(Side::Sell)
        } else {
            None
        }) else {
            return;
        };

        let trigger_ts = get_timestamp_us();
        let cancelled = self.cancel_arb_open_strategies_for_symbol_side(
            &normalized_symbol,
            cancel_side,
            trigger_ts,
            e_ts,
            ArbCancelReason::PositionRisk,
        );
        if cancelled > 0 {
            warn!(
                "Arb position risk cancel triggered: symbol={} open_venue={:?} hedge_venue={:?} net_qty={:.8} cancel_side={:?} cancelled_strategies={} trigger_ts={}",
                normalized_symbol,
                open_venue,
                hedge_venue,
                net_qty,
                cancel_side,
                cancelled,
                trigger_ts
            );
        }
    }

    fn handle_arb_open_margin_net_risk_after_update(&self, asset: &str, e_ts: i64) {
        let asset_upper = asset.trim().to_uppercase();
        if asset_upper.is_empty() || is_exposure_exempt_asset(&asset_upper) {
            return;
        }
        if self.open_venue() == self.hedge_venue() {
            return;
        }
        let mapper = create_symbol_mapper(exchange_from_venue(self.open_venue()));
        let symbol =
            normalize_symbol_for_internal(&mapper.balance_asset_to_um_symbol(&asset_upper));
        self.handle_arb_position_risk_after_update(&symbol, e_ts);
    }

    pub fn mark_price_exchange(&self) -> Exchange {
        Self::with_inner(|inner| {
            Self::mark_price_exchange_for_venues(inner.open_venue, inner.hedge_venue)
        })
    }

    pub fn try_mark_price_exchange(&self) -> Option<Exchange> {
        Self::try_with_inner(|inner| {
            Self::mark_price_exchange_for_venues(inner.open_venue, inner.hedge_venue)
        })
    }

    fn mark_price_exchange_for_venues(
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Exchange {
        let open_exchange = exchange_from_venue(open_venue);
        let hedge_exchange = exchange_from_venue(hedge_venue);
        if open_exchange == hedge_exchange {
            return open_exchange;
        }

        if is_futures_venue(open_venue) && is_futures_venue(hedge_venue) {
            return hedge_exchange;
        }

        for preferred in [Exchange::Okex, Exchange::Bybit, Exchange::Binance] {
            if open_exchange == preferred || hedge_exchange == preferred {
                return preferred;
            }
        }

        Exchange::Binance
    }

    fn derivatives_service_for_mark_price_source(
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        _arb_mode: ArbMode,
    ) -> &'static str {
        if open_venue == TradingVenue::BinanceCoinFutures
            || hedge_venue == TradingVenue::BinanceCoinFutures
        {
            return BINANCE_COIN_DERIVATIVES_SERVICE;
        }
        if open_venue == TradingVenue::BitgetCoinFutures
            || hedge_venue == TradingVenue::BitgetCoinFutures
        {
            return BITGET_COIN_DERIVATIVES_SERVICE;
        }
        match Self::mark_price_exchange_for_venues(open_venue, hedge_venue) {
            Exchange::Okex => OKEX_DERIVATIVES_SERVICE,
            Exchange::Bybit => BYBIT_DERIVATIVES_SERVICE,
            Exchange::Bitget => BITGET_DERIVATIVES_SERVICE,
            Exchange::Gate => GATE_DERIVATIVES_SERVICE,
            Exchange::Hyperliquid => HYPERLIQUID_DERIVATIVES_SERVICE,
            _ => BINANCE_DIRECT_DERIVATIVES_SERVICE,
        }
    }

    pub fn usdt_mgr(&self, scope: BasicAccountScope) -> Option<Rc<RefCell<UsdtBalanceManager>>> {
        Self::with_inner(|inner| inner.usdt_mgrs.get(&scope).cloned())
    }

    pub fn usdt_snapshot_all(&self) -> Vec<(BasicAccountScope, UsdtBalanceSnapshot)> {
        Self::with_inner(|inner| {
            let mut out: Vec<(BasicAccountScope, UsdtBalanceSnapshot)> = inner
                .usdt_mgrs
                .iter()
                .map(|(scope, mgr)| (*scope, mgr.borrow().snapshot()))
                .collect();
            out.sort_by_key(|(scope, _)| *scope as u32);
            out
        })
    }

    pub fn usdt_snapshot_for_venue(&self, venue: TradingVenue) -> Option<UsdtBalanceSnapshot> {
        Self::with_inner(|inner| {
            let binance_mode = if inner.order_manager.borrow().binance_is_standard() {
                Some(BinanceAccountMode::Standard)
            } else {
                Some(BinanceAccountMode::Unified)
            };
            let scope = scope_for_venue(venue, binance_mode, inner.hyperliquid_account_mode);
            inner
                .usdt_mgrs
                .get(&scope)
                .map(|mgr| mgr.borrow().snapshot())
        })
    }

    pub fn account_scope_for_venue(&self, venue: TradingVenue) -> BasicAccountScope {
        Self::with_inner(|inner| {
            let binance_mode = if inner.order_manager.borrow().binance_is_standard() {
                Some(BinanceAccountMode::Standard)
            } else {
                Some(BinanceAccountMode::Unified)
            };
            scope_for_venue(venue, binance_mode, inner.hyperliquid_account_mode)
        })
    }

    /// 写入某个账户 scope 的最新风险快照（后写覆盖前写）。
    pub fn apply_account_risk(&self, scope: BasicAccountScope, msg: BasicAccountRiskMsg) {
        Self::with_inner_mut(|inner| {
            inner.latest_account_risk.insert(scope, msg);
        });
    }

    /// 读取指定 scope 的最新风险快照；未到货则返回 None。
    pub fn account_risk_snapshot(&self, scope: BasicAccountScope) -> Option<BasicAccountRiskMsg> {
        Self::with_inner(|inner| inner.latest_account_risk.get(&scope).cloned())
    }

    pub fn apply_binance_std_um_wallet_snapshot(&self, msg: BinanceStdUmWalletSnapshotMsg) {
        Self::with_inner_mut(|inner| {
            inner.latest_binance_std_um_wallet = Some(msg);
        });
    }

    pub fn binance_std_um_wallet_snapshot(&self) -> Option<BinanceStdUmWalletSnapshotMsg> {
        Self::with_inner(|inner| inner.latest_binance_std_um_wallet.clone())
    }

    fn exec_position_imbalance_ratio(long_usdt: f64, short_usdt: f64) -> f64 {
        let total_usdt = long_usdt + short_usdt;
        if total_usdt <= f64::EPSILON {
            0.0
        } else {
            (long_usdt - short_usdt).abs() / total_usdt
        }
    }

    fn exec_position_imbalance_projection_inner(
        inner: &MonitorChannelInner,
        symbol: &str,
        venue: TradingVenue,
        signed_base_qty: f64,
        limit_ratio: f64,
    ) -> Result<Option<ExecPositionImbalanceProjection>, String> {
        if limit_ratio <= 0.0 {
            return Ok(None);
        }
        if !(limit_ratio.is_finite() && limit_ratio <= 1.0) {
            return Err(format!(
                "exec_max_position_imbalance_ratio 非法: {:.8}",
                limit_ratio
            ));
        }

        let symbol_upper = symbol.to_uppercase();
        let base_asset = extract_base_asset(&symbol_upper).ok_or_else(|| {
            format!(
                "无法识别 symbol={} 的基础资产，无法校验 Exec 截面失衡",
                symbol
            )
        })?;
        let base_asset_upper = base_asset.to_uppercase();
        if is_exposure_exempt_asset(&base_asset_upper) {
            return Ok(None);
        }
        if venue != inner.open_venue && venue != inner.hedge_venue {
            return Err(format!(
                "Exec venue {:?} 不匹配 open={:?} hedge={:?}",
                venue, inner.open_venue, inner.hedge_venue
            ));
        }

        let state = Self::basic_state_cached();
        let price_mapper = create_symbol_mapper(Self::mark_price_exchange_for_venues(
            inner.open_venue,
            inner.hedge_venue,
        ));
        let mut current_long_usdt = 0.0;
        let mut current_short_usdt = 0.0;
        let mut current_asset_usdt = 0.0;

        for (asset, (open_qty, hedge_qty)) in &state.exposures {
            if is_exposure_exempt_asset(asset) {
                continue;
            }
            let mark = state.mark_usdt_by_asset.get(asset).copied().unwrap_or(0.0);
            if !(mark.is_finite() && mark > 0.0) {
                continue;
            }
            let net_usdt = (open_qty + hedge_qty) * mark;
            if *asset == base_asset_upper {
                current_asset_usdt = net_usdt;
            }
            if net_usdt > 0.0 {
                current_long_usdt += net_usdt;
            } else if net_usdt < 0.0 {
                current_short_usdt += -net_usdt;
            }
        }

        let mark_symbol = price_mapper.asset_to_price_symbol(&base_asset_upper);
        let mark = state
            .mark_usdt_by_asset
            .get(&base_asset_upper)
            .copied()
            .ok_or_else(|| {
                format!(
                    "symbol={} 缺少 USDT 标记价格，无法校验 Exec 截面失衡",
                    symbol
                )
            })?;
        if !(mark.is_finite() && mark > 0.0) {
            return Err(format!(
                "symbol={} 标记价格无效 mark_symbol={} mark={:.8}",
                symbol, mark_symbol, mark
            ));
        }

        let next_asset_usdt = current_asset_usdt + signed_base_qty * mark;
        let mut next_long_usdt = current_long_usdt;
        let mut next_short_usdt = current_short_usdt;
        if current_asset_usdt > 0.0 {
            next_long_usdt -= current_asset_usdt;
        } else if current_asset_usdt < 0.0 {
            next_short_usdt -= -current_asset_usdt;
        }
        if next_asset_usdt > 0.0 {
            next_long_usdt += next_asset_usdt;
        } else if next_asset_usdt < 0.0 {
            next_short_usdt += -next_asset_usdt;
        }
        next_long_usdt = next_long_usdt.max(0.0);
        next_short_usdt = next_short_usdt.max(0.0);

        let current_total_usdt = current_long_usdt + current_short_usdt;
        let next_total_usdt = next_long_usdt + next_short_usdt;
        let current_imbalance_ratio =
            Self::exec_position_imbalance_ratio(current_long_usdt, current_short_usdt);
        let next_imbalance_ratio =
            Self::exec_position_imbalance_ratio(next_long_usdt, next_short_usdt);

        Ok(Some(ExecPositionImbalanceProjection {
            current_long_usdt,
            current_short_usdt,
            next_long_usdt,
            next_short_usdt,
            current_total_usdt,
            next_total_usdt,
            current_imbalance_ratio,
            next_imbalance_ratio,
            limit_ratio,
        }))
    }

    fn evaluate_exec_position_imbalance_projection(
        symbol: &str,
        projection: ExecPositionImbalanceProjection,
    ) -> Result<(), String> {
        let eps = 1e-9_f64;
        if projection.next_imbalance_ratio <= projection.current_imbalance_ratio + eps {
            return Ok(());
        }
        if projection.next_imbalance_ratio > projection.limit_ratio + eps {
            return Err(format!(
                "symbol={} Exec 截面持仓失衡比例扩大后超限: current_ratio={:.6} next_ratio={:.6} limit={:.6} current_long={:.4}USDT current_short={:.4}USDT next_long={:.4}USDT next_short={:.4}USDT current_total={:.4}USDT next_total={:.4}USDT",
                symbol,
                projection.current_imbalance_ratio,
                projection.next_imbalance_ratio,
                projection.limit_ratio,
                projection.current_long_usdt,
                projection.current_short_usdt,
                projection.next_long_usdt,
                projection.next_short_usdt,
                projection.current_total_usdt,
                projection.next_total_usdt
            ));
        }
        Ok(())
    }

    pub fn exec_position_imbalance_projection(
        &self,
        symbol: &str,
        venue: TradingVenue,
        signed_base_qty: f64,
    ) -> Result<Option<ExecPositionImbalanceProjection>, String> {
        Self::with_inner(|inner| {
            let limit_ratio = PreTradeParamsLoader::instance().exec_max_position_imbalance_ratio();
            Self::exec_position_imbalance_projection_inner(
                inner,
                symbol,
                venue,
                signed_base_qty,
                limit_ratio,
            )
        })
    }

    pub fn check_exec_position_imbalance_risk(
        &self,
        symbol: &str,
        venue: TradingVenue,
        signed_base_qty: f64,
    ) -> Result<(), String> {
        let Some(projection) =
            self.exec_position_imbalance_projection(symbol, venue, signed_base_qty)?
        else {
            return Ok(());
        };
        Self::evaluate_exec_position_imbalance_projection(symbol, projection)
    }

    /// 获取当前基础风控口径的快照（用于 resample/viz）
    ///
    /// 返回：
    /// - `exposures`: asset -> (open_qty, hedge_qty)，都按标的数量（base qty）表达
    /// - `total_equity_usdt`: USDT 总权益（eq 口径；若涉及合约 venue，会叠加 UPL）
    /// - `abs_total_exposure_usdt`: 各资产净敞口按 USDT 估值后取绝对值求和
    /// - `total_position_usdt`: 各资产现货/合约头寸按 USDT 估值后取绝对值求和
    /// - `total_um_unrealized_usdt`: 合约未实现盈亏（USDT 计价）
    pub fn basic_state_snapshot(&self) -> (HashMap<String, (f64, f64)>, f64, f64, f64, f64) {
        Self::with_inner(|_inner| {
            let state = Self::basic_state_cached();
            (
                state.exposures,
                state.total_equity_usdt,
                state.abs_total_exposure_usdt,
                state.total_position_usdt,
                state.total_um_unrealized_usdt,
            )
        })
    }

    /// Returns gross per-asset positions and their total using the same cached
    /// valuation as `total_position_usdt`.
    pub fn gross_position_usdt_snapshot(&self) -> (HashMap<String, f64>, f64) {
        Self::with_inner(|_inner| {
            let state = Self::basic_state_cached();
            (state.position_usdt_by_asset, state.total_position_usdt)
        })
    }

    /// Returns the two largest current gross positions in normalized symbols.
    ///
    /// The force-close path uses this live snapshot instead of a Redis list.
    pub fn unimmr_force_close_symbols(&self) -> Vec<String> {
        let (positions_by_asset, _) = self.gross_position_usdt_snapshot();
        top_two_gross_position_symbols(&positions_by_asset)
    }

    /// Returns materially non-zero position assets that are still missing a
    /// usable mark price from the derivatives stream.
    pub fn missing_gross_position_mark_assets(&self) -> Vec<String> {
        Self::with_inner(|_inner| {
            let state = Self::basic_state_cached();
            missing_position_mark_assets(&state.exposures, &state.mark_usdt_by_asset)
        })
    }

    /// 获取 strategy_mgr 的引用
    pub fn strategy_mgr(&self) -> Rc<RefCell<crate::strategy::StrategyManager>> {
        Self::with_inner(|inner| inner.strategy_mgr.clone())
    }

    pub fn try_strategy_mgr() -> Option<Rc<RefCell<crate::strategy::StrategyManager>>> {
        Self::try_with_inner(|inner| inner.strategy_mgr.clone())
    }

    pub fn orphan_strategy_mgr(&self) -> Rc<RefCell<OrphanStrategyManager>> {
        Self::with_inner(|inner| inner.orphan_strategy_mgr.clone())
    }

    pub fn try_orphan_strategy_mgr() -> Option<Rc<RefCell<OrphanStrategyManager>>> {
        Self::try_with_inner(|inner| inner.orphan_strategy_mgr.clone())
    }

    /// 获取开仓腿的基础余额管理器（margin/spot）
    pub fn open_balance_mgr(&self) -> Option<Rc<RefCell<BasicBalanceManager>>> {
        Self::with_inner(|inner| inner.open_leg.as_balance_mgr())
    }

    /// 获取对冲腿的基础余额管理器（margin/spot）
    pub fn hedge_balance_mgr(&self) -> Option<Rc<RefCell<BasicBalanceManager>>> {
        Self::with_inner(|inner| inner.hedge_leg.as_balance_mgr())
    }

    /// 获取开仓腿的基础合约管理器（futures）
    pub fn open_um_mgr(&self) -> Option<UmMgrPair> {
        Self::with_inner(|inner| inner.open_leg.as_um_mgr())
    }

    /// 获取对冲腿的基础合约管理器（futures）
    pub fn hedge_um_mgr(&self) -> Option<UmMgrPair> {
        Self::with_inner(|inner| inner.hedge_leg.as_um_mgr())
    }

    /// 查询指定 venue+asset 的现货/保证金净头寸（base qty），非 margin venue 返回 0
    pub fn balance_position_for_venue(&self, venue: TradingVenue, asset: &str) -> f64 {
        Self::with_inner(|inner| Self::balance_position_for_venue_inner(inner, venue, asset))
    }

    fn balance_position_for_venue_inner(
        inner: &MonitorChannelInner,
        venue: TradingVenue,
        asset: &str,
    ) -> f64 {
        let leg = if venue == inner.open_venue {
            &inner.open_leg
        } else if venue == inner.hedge_venue {
            &inner.hedge_leg
        } else {
            return 0.0;
        };
        let settlement_asset = if exchange_from_venue(venue) == Exchange::Hyperliquid {
            "USDC"
        } else {
            "USDT"
        };
        if asset.eq_ignore_ascii_case(settlement_asset) {
            let scope = scope_for_venue(
                venue,
                inner.binance_account_mode,
                inner.hyperliquid_account_mode,
            );
            return inner
                .usdt_mgrs
                .get(&scope)
                .map(|m| m.borrow().net_usdt_position())
                .unwrap_or(0.0);
        }
        match leg {
            LegMgr::Margin { bal, .. } => bal.borrow().net_position(asset, None),
            _ => 0.0,
        }
    }

    /// 初始化 pre-trade 的账户与风控管理器（仅 open/hedge 两条腿）
    ///
    /// - 按 venue 的 market type 映射到 BasicBalanceManager / BasicUmManager
    /// - 订阅 account_pubs/<exchange>_pm（期望收到 BasicAccountEventMsg）
    /// - 初始化各 venue 的 min_qty/price_tick 表用于对齐
    pub async fn init_singleton(
        strategy_mgr: Rc<RefCell<crate::strategy::StrategyManager>>,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        arb_mode: ArbMode,
        binance_account_mode: Option<BinanceAccountMode>,
        hyperliquid_account_mode: Option<HyperliquidAccountMode>,
        refresh_order_rules_from_venue: bool,
    ) -> Result<()> {
        // 仅支持当前已接入 pre_trade 的交易所
        for v in [open_venue, hedge_venue] {
            if !matches!(
                v,
                TradingVenue::BinanceMargin
                    | TradingVenue::BinanceFutures
                    | TradingVenue::BinanceCoinFutures
                    | TradingVenue::OkexMargin
                    | TradingVenue::OkexFutures
                    | TradingVenue::BybitMargin
                    | TradingVenue::BybitFutures
                    | TradingVenue::BitgetMargin
                    | TradingVenue::BitgetFutures
                    | TradingVenue::BitgetCoinFutures
                    | TradingVenue::GateMargin
                    | TradingVenue::GateFutures
                    | TradingVenue::HyperliquidMargin
                    | TradingVenue::HyperliquidFutures
            ) {
                panic!("pre_trade does not support venue {:?}", v);
            }
        }

        let open_exchange = exchange_from_venue(open_venue);
        let hedge_exchange = exchange_from_venue(hedge_venue);

        // 初始化 USDT 管理器（按账户 scope 维度，Binance standard 下 margin/futures 分离）
        let mut usdt_mgrs: HashMap<BasicAccountScope, Rc<RefCell<UsdtBalanceManager>>> =
            HashMap::new();
        for (scope, ex) in [
            (
                scope_for_venue(open_venue, binance_account_mode, hyperliquid_account_mode),
                open_exchange,
            ),
            (
                scope_for_venue(hedge_venue, binance_account_mode, hyperliquid_account_mode),
                hedge_exchange,
            ),
        ] {
            usdt_mgrs
                .entry(scope)
                .or_insert_with(|| Rc::new(RefCell::new(UsdtBalanceManager::new(ex))));
        }

        // 初始化开仓腿基础管理器
        let open_leg = if is_margin_venue(open_venue) {
            LegMgr::Margin {
                bal: Rc::new(RefCell::new(BasicBalanceManager::new(open_exchange))),
            }
        } else if is_futures_venue(open_venue) {
            let mut min_qty_table = MinQtyTable::new(open_exchange);
            if refresh_order_rules_from_venue {
                if let Err(err) = min_qty_table.refresh().await {
                    warn!(
                        "failed to refresh min_qty_table for {:?}: {err:#}",
                        open_exchange
                    );
                }
            }
            LegMgr::Futures {
                exchange: open_exchange,
                um: Rc::new(RefCell::new(BasicUmManager::new(open_exchange))),
                min_qty_table: Rc::new(RefCell::new(min_qty_table)),
            }
        } else {
            unreachable!()
        };

        // 初始化对冲腿基础管理器
        let hedge_leg = if is_margin_venue(hedge_venue) {
            LegMgr::Margin {
                bal: Rc::new(RefCell::new(BasicBalanceManager::new(hedge_exchange))),
            }
        } else if is_futures_venue(hedge_venue) {
            let mut min_qty_table = MinQtyTable::new(hedge_exchange);
            if refresh_order_rules_from_venue {
                if let Err(err) = min_qty_table.refresh().await {
                    warn!(
                        "failed to refresh min_qty_table for {:?}: {err:#}",
                        hedge_exchange
                    );
                }
            }
            LegMgr::Futures {
                exchange: hedge_exchange,
                um: Rc::new(RefCell::new(BasicUmManager::new(hedge_exchange))),
                min_qty_table: Rc::new(RefCell::new(min_qty_table)),
            }
        } else {
            unreachable!()
        };

        // 创建价格表（价格由 derivatives stream 持续更新）
        let price_table = Rc::new(RefCell::new(PriceTable::new()));

        // 加载交易对 LOT_SIZE/PRICE_FILTER（按 venue 区分），用于数量/价格对齐
        let mut venue_min_qty_tables: HashMap<TradingVenue, Rc<VenueMinQtyTable>> = HashMap::new();
        for venue in [open_venue, hedge_venue] {
            if venue_min_qty_tables.contains_key(&venue) {
                continue;
            }
            let mut table = VenueMinQtyTable::new(venue);
            if refresh_order_rules_from_venue {
                if let Err(err) = table.refresh().await {
                    warn!("failed to refresh filters for venue {:?}: {err:#}", venue);
                }
            }
            venue_min_qty_tables.insert(venue, Rc::new(table));
        }

        // 为涉及的交易所创建 basic 账户 listener（可能是一个或两个），由 pre_trade reactor 统一 drain。
        let mut exchanges: HashSet<Exchange> = HashSet::new();
        exchanges.insert(open_exchange);
        exchanges.insert(hedge_exchange);
        let mut account_listeners = Vec::with_capacity(exchanges.len());
        for ex in exchanges {
            let service_name = build_service_name(&format!("account_pubs/{}_pm", ex.as_str()));
            let node_name = format!("pre_trade_account_pubs_{}_pm", ex.as_str());
            account_listeners.push(BasicAccountListener::new(
                service_name,
                node_name,
                ex,
                open_venue,
                hedge_venue,
                open_leg.clone(),
                hedge_leg.clone(),
                usdt_mgrs.clone(),
                binance_account_mode,
                hyperliquid_account_mode,
                strategy_mgr.clone(),
            )?);
        }

        // 创建衍生品价格 listener（mark_price, index_price），由 pre_trade reactor 统一 drain。
        //
        // 约定：默认使用 Binance Futures 的衍生品指标；当 open/hedge 两腿属于同一交易所时，
        // 切换到对应 venue 的 mark/index price。所有交易所均直连 dat_pbs。
        let node_name = DEFAULT_NODE_PRE_TRADE_DERIVATIVES.to_string();
        let service_name =
            Self::derivatives_service_for_mark_price_source(open_venue, hedge_venue, arb_mode)
                .to_string();
        let derivatives_listener =
            DerivativesPriceListener::new(price_table.clone(), node_name, service_name)?;

        // 创建内部实例并保存到 thread-local
        let inner = MonitorChannelInner {
            open_venue,
            hedge_venue,
            arb_mode,
            binance_account_mode,
            hyperliquid_account_mode,
            open_leg,
            hedge_leg,
            usdt_mgrs,
            price_table,
            venue_min_qty_tables,
            strategy_mgr,
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(binance_account_mode))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk: HashMap::new(),
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(open_venue != hedge_venue),
        };

        Self::clear_basic_state_runtime_cache();
        EXEC_POSITION_SNAPSHOT_READY.with(|ready| ready.set(false));
        HYPERLIQUID_EXEC_SNAPSHOT_VALID_UNTIL_MS.with(|deadline| deadline.set(0));
        HYPERLIQUID_FACT_STREAM_READY.with(|ready| ready.set(false));
        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });
        Self::refresh_basic_state_cache();
        MONITOR_STATE_LISTENERS.with(|listeners| {
            *listeners.borrow_mut() = Some(MonitorStateListeners {
                account_listeners,
                derivatives_listener,
            });
        });

        Ok(())
    }

    pub fn replace_manager_order_rules(
        venue: TradingVenue,
        market_type: signal_common::min_qty_table::MarketType,
        filters: HashMap<String, signal_common::min_qty_table::MinQtyEntry>,
        contract_multipliers: HashMap<String, f64>,
        tradable_symbols: std::collections::HashSet<String>,
    ) -> Result<(), String> {
        let legacy_market_type = match market_type {
            signal_common::min_qty_table::MarketType::Spot => {
                crate::common::min_qty_table::MarketType::Spot
            }
            signal_common::min_qty_table::MarketType::Futures => {
                crate::common::min_qty_table::MarketType::Futures
            }
            signal_common::min_qty_table::MarketType::CoinFutures => {
                crate::common::min_qty_table::MarketType::Futures
            }
            signal_common::min_qty_table::MarketType::Margin => {
                crate::common::min_qty_table::MarketType::Margin
            }
        };
        let legacy_filters = filters
            .iter()
            .map(|(symbol, entry)| {
                (
                    symbol.clone(),
                    crate::common::min_qty_table::MinQtyEntry {
                        symbol: entry.symbol.clone(),
                        base_asset: entry.base_asset.clone(),
                        quote_asset: entry.quote_asset.clone(),
                        min_qty: entry.min_qty,
                        step_size: entry.step_size,
                        price_tick: entry.price_tick,
                        min_notional: entry.min_notional,
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        Self::with_inner_mut(|inner| {
            let table = inner
                .venue_min_qty_tables
                .get_mut(&venue)
                .ok_or_else(|| format!("missing venue order-rules table: {venue:?}"))?;
            Rc::get_mut(table)
                .ok_or_else(|| format!("venue order-rules table is shared: {venue:?}"))?
                .replace_snapshot(
                    filters.clone(),
                    contract_multipliers.clone(),
                    tradable_symbols,
                );

            for leg in [&inner.open_leg, &inner.hedge_leg] {
                let LegMgr::Futures { min_qty_table, .. } = leg else {
                    continue;
                };
                min_qty_table.borrow_mut().replace_market_snapshot(
                    legacy_market_type,
                    legacy_filters.clone(),
                    contract_multipliers.clone(),
                );
            }
            Ok(())
        })
    }

    /// 将订单数量（按 venue 语义）转换为 base qty（标的数量）
    fn order_qty_to_base(
        inner: &MonitorChannelInner,
        venue: TradingVenue,
        symbol: &str,
        qty: f64,
    ) -> f64 {
        match venue {
            TradingVenue::BinanceFutures => qty,
            venue if venue.is_inverse_futures() => {
                Self::qty_multiplier_for_venue_inner(inner, venue, symbol)
                    .map(|multiplier| qty * multiplier)
                    .unwrap_or(0.0)
            }
            TradingVenue::OkexFutures | TradingVenue::GateFutures => {
                let symbol_key = min_qty_symbol_key(venue, symbol);
                let mult = inner
                    .venue_min_qty_tables
                    .get(&venue)
                    .map(|t| t.contract_multiplier(&symbol_key))
                    .unwrap_or(1.0);
                qty * mult
            }
            _ => qty,
        }
    }

    /// 将订单数量（按 venue 语义）转换为 base qty（标的数量）
    ///
    /// 用于风控等关键路径：对于需要合约乘数的 venue，若乘数缺失则直接返回错误，避免默认 1 导致风险口径失真。
    fn order_qty_to_base_checked(
        inner: &MonitorChannelInner,
        venue: TradingVenue,
        symbol: &str,
        qty: f64,
        price: f64,
    ) -> Result<f64, String> {
        match venue {
            TradingVenue::BinanceFutures => Ok(qty),
            venue if venue.is_inverse_futures() => {
                let mult =
                    Self::qty_multiplier_for_venue_at_price_inner(inner, venue, symbol, price)?;
                Ok(qty * mult)
            }
            TradingVenue::OkexFutures | TradingVenue::GateFutures => {
                let mult = Self::qty_multiplier_for_venue_inner(inner, venue, symbol)?;
                Ok(qty * mult)
            }
            _ => Ok(qty),
        }
    }

    fn qty_multiplier_for_venue_inner(
        inner: &MonitorChannelInner,
        venue: TradingVenue,
        symbol: &str,
    ) -> Result<f64, String> {
        match venue {
            TradingVenue::BinanceFutures => Ok(1.0),
            venue if venue.is_inverse_futures() => {
                let symbol_key = min_qty_symbol_key(venue, symbol);
                let price = inner
                    .price_table
                    .borrow()
                    .mark_price(&symbol_key)
                    .filter(|price| price.is_finite() && *price > 0.0)
                    .ok_or_else(|| {
                        format!(
                            "symbol={} 缺少 {:?} mark price，无法转换 inverse qty",
                            symbol_key, venue
                        )
                    })?;
                Self::qty_multiplier_for_venue_at_price_inner(inner, venue, symbol, price)
            }
            TradingVenue::OkexFutures | TradingVenue::GateFutures => {
                let symbol_key = min_qty_symbol_key(venue, symbol);
                let Some(table) = inner.venue_min_qty_tables.get(&venue) else {
                    return Err(format!(
                        "未初始化 {:?} 的最小下单量表，无法获取乘数 symbol={}",
                        venue, symbol_key
                    ));
                };
                let Some(multiplier) = table.contract_multiplier_opt(&symbol_key) else {
                    return Err(format!(
                        "symbol={} 缺少 {:?} 合约乘数，无法转换 qty 口径",
                        symbol_key, venue
                    ));
                };
                if multiplier <= 0.0 {
                    return Err(format!(
                        "symbol={} {:?} contract multiplier invalid: {}",
                        symbol_key, venue, multiplier
                    ));
                }
                Ok(multiplier)
            }
            _ => Ok(1.0),
        }
    }

    fn qty_multiplier_for_venue_at_price_inner(
        inner: &MonitorChannelInner,
        venue: TradingVenue,
        symbol: &str,
        price: f64,
    ) -> Result<f64, String> {
        if !venue.is_inverse_futures() {
            return Self::qty_multiplier_for_venue_inner(inner, venue, symbol);
        }
        if !price.is_finite() || price <= 0.0 {
            return Err(format!(
                "symbol={} {:?} inverse qty requires positive price, got {}",
                symbol, venue, price
            ));
        }
        let symbol_key = min_qty_symbol_key(venue, symbol);
        let table = inner.venue_min_qty_tables.get(&venue).ok_or_else(|| {
            format!(
                "未初始化 {:?} 的最小下单量表，无法获取 contractSize symbol={}",
                venue, symbol_key
            )
        })?;
        let contract_size = table.contract_multiplier_opt(&symbol_key).ok_or_else(|| {
            format!(
                "symbol={} 缺少 {:?} inverse face value，无法转换 qty",
                symbol_key, venue
            )
        })?;
        if !contract_size.is_finite() || contract_size <= 0.0 {
            return Err(format!(
                "symbol={} {:?} inverse face value invalid: {}",
                symbol_key, venue, contract_size
            ));
        }
        Ok(contract_size / price)
    }

    /// 将订单数量（按 venue 语义）转换为 base qty（标的数量）
    ///
    /// - Binance futures: qty 按 contracts(mult=1) 处理，等价于 base qty
    /// - OKX/Gate futures: qty 是 contracts，需要乘以合约面值（contract multiplier）
    pub fn qty_to_base(&self, venue: TradingVenue, symbol: &str, qty: f64) -> f64 {
        Self::with_inner(|inner| Self::order_qty_to_base(inner, venue, symbol, qty))
    }

    pub fn qty_to_base_at_price(
        &self,
        venue: TradingVenue,
        symbol: &str,
        qty: f64,
        price: f64,
    ) -> Result<f64, String> {
        self.qty_multiplier_for_venue_at_price(venue, symbol, price)
            .map(|multiplier| qty * multiplier)
    }

    /// 基于 open/hedge 两腿的基础管理器计算敞口与总量指标
    fn compute_basic_state(inner: &MonitorChannelInner) -> BasicState {
        let price_table = inner.price_table.borrow();
        // MM 模式下 open_venue == hedge_venue 时，两条腿实际指向同一账户数据，
        // 若同时统计会造成敞口翻倍；此时仅以 open 单边为准。
        let same_venue = inner.open_venue == inner.hedge_venue;

        fn collect_leg_exposure(
            leg: &LegMgr,
            venue: TradingVenue,
            price_table: &PriceTable,
            venue_min_qty_tables: &HashMap<TradingVenue, Rc<VenueMinQtyTable>>,
            exposures: &mut HashMap<String, (f64, f64)>,
            leg_idx: usize,
        ) {
            let mut add_exposure = |asset: String, qty: f64| {
                if qty.abs() <= 1e-12 || is_exposure_exempt_asset(&asset) {
                    return;
                }
                let entry = exposures
                    .entry(asset.to_ascii_uppercase())
                    .or_insert((0.0, 0.0));
                if leg_idx == 0 {
                    entry.0 += qty;
                } else {
                    entry.1 += qty;
                }
            };
            match leg {
                LegMgr::Margin { bal, .. } => {
                    let mgr = bal.borrow();
                    for balance in mgr.balances_iter() {
                        add_exposure(balance.symbol.clone(), balance.net());
                    }
                }
                LegMgr::Futures {
                    exchange,
                    um,
                    min_qty_table,
                } => {
                    let symbol_mapper = create_symbol_mapper(*exchange);
                    let um_mgr = um.borrow();
                    let min_qty = min_qty_table.borrow();
                    for (symbol, net_contracts) in um_mgr.net_contracts_iter() {
                        if net_contracts == 0.0 {
                            continue;
                        }
                        let Some(base_asset) = symbol_mapper.inst_id_to_base_asset(symbol) else {
                            continue;
                        };
                        let base_qty = if venue.is_inverse_futures() {
                            let symbol_key = min_qty_symbol_key(venue, symbol);
                            let Some(face_value) = venue_min_qty_tables
                                .get(&venue)
                                .and_then(|table| table.contract_multiplier_opt(&symbol_key))
                            else {
                                continue;
                            };
                            let Some(mark_price) = price_table.mark_price(&symbol_key) else {
                                continue;
                            };
                            net_contracts as f64 * face_value / mark_price
                        } else {
                            net_contracts as f64 * min_qty.contract_multiplier(symbol)
                        };
                        add_exposure(base_asset, base_qty);
                    }
                }
            }
        }

        let mut exposures: HashMap<String, (f64, f64)> = HashMap::new();
        collect_leg_exposure(
            &inner.open_leg,
            inner.open_venue,
            &price_table,
            &inner.venue_min_qty_tables,
            &mut exposures,
            0,
        );
        if !same_venue {
            collect_leg_exposure(
                &inner.hedge_leg,
                inner.hedge_venue,
                &price_table,
                &inner.venue_min_qty_tables,
                &mut exposures,
                1,
            );
        }

        let binance_mode = if inner.order_manager.borrow().binance_is_standard() {
            Some(BinanceAccountMode::Standard)
        } else {
            Some(BinanceAccountMode::Unified)
        };
        let mut margin_balances_by_scope: HashMap<BasicAccountScope, HashMap<String, f64>> =
            HashMap::new();
        for (idx, (venue, leg)) in [
            (inner.open_venue, &inner.open_leg),
            (inner.hedge_venue, &inner.hedge_leg),
        ]
        .iter()
        .enumerate()
        {
            if same_venue && idx == 1 {
                continue;
            }
            if let LegMgr::Margin { bal, .. } = leg {
                let mgr = bal.borrow();
                let scope = scope_for_venue(*venue, binance_mode, inner.hyperliquid_account_mode);
                let scope_balances = margin_balances_by_scope.entry(scope).or_default();
                for bal in mgr.balances_iter() {
                    let net = bal.net();
                    if net.abs() <= 1e-12 {
                        continue;
                    }
                    *scope_balances
                        .entry(bal.symbol.to_ascii_uppercase())
                        .or_insert(0.0) += net;
                }
            }
        }

        Self::filter_non_trading_dust_positions(
            inner,
            &price_table,
            &mut exposures,
            &mut margin_balances_by_scope,
        );

        // total_equity(eq) 口径：
        // - 非 USDT 资产：从 balance manager 统计净资产估值
        // - USDT：按交易所维度单独维护
        // - Binance/Bitget 等 futures UPL 单独来自 BasicUmManager 并叠加
        // - OKX/Gate unified 的 balance/equity 已隐含账户级合约影响，因此只保留 UPL 展示，不再重复叠加
        let mut usdt_equity_by_scope: HashMap<BasicAccountScope, f64> = HashMap::new();
        // 加上各账户 scope 的 USDT 净头寸（Binance standard 下 margin/futures 分离）
        for (scope, mgr) in &inner.usdt_mgrs {
            let net = mgr.borrow().net_usdt_position();
            if net.abs() <= 1e-12 {
                continue;
            }
            debug!("USDT net position: scope={} net={:.6}", scope.as_str(), net);
            *usdt_equity_by_scope.entry(*scope).or_insert(0.0) += net;
        }

        let mut total_um_unrealized_usdt = 0.0;
        let mut um_unrealized_equity_by_scope: HashMap<BasicAccountScope, f64> = HashMap::new();
        for (idx, (venue, leg)) in [
            (inner.open_venue, &inner.open_leg),
            (inner.hedge_venue, &inner.hedge_leg),
        ]
        .iter()
        .enumerate()
        {
            if same_venue && idx == 1 {
                continue;
            }
            if let LegMgr::Futures { exchange, um, .. } = leg {
                let um_ref = um.borrow();
                let upl = if venue.is_inverse_futures() {
                    um_ref
                        .positions_iter()
                        .filter_map(|position| {
                            let symbol_key = min_qty_symbol_key(*venue, &position.inst_id);
                            price_table
                                .mark_price(&symbol_key)
                                .map(|mark| position.unrealized_pnl_usdt * mark)
                        })
                        .sum()
                } else {
                    um_ref.total_unrealized_pnl_usdt()
                };
                total_um_unrealized_usdt += upl;
                if !matches!(*exchange, Exchange::Gate | Exchange::Okex) {
                    let scope =
                        scope_for_venue(*venue, binance_mode, inner.hyperliquid_account_mode);
                    *um_unrealized_equity_by_scope.entry(scope).or_insert(0.0) += upl;
                }
            }
        }

        // 同一账户 scope 优先使用交易所账户级总权益，避免本地估值遗漏
        // 多资产抵押、合约、期权或折算细节。跨 scope 组合仍保留各 scope 的本地路径。
        let account_risk_equity_override = Self::account_risk_equity_override_for_inner(inner);

        let price_update = Self::compute_basic_state_price_update_from_parts(
            inner,
            &price_table,
            &exposures,
            &margin_balances_by_scope,
            &usdt_equity_by_scope,
            &um_unrealized_equity_by_scope,
            account_risk_equity_override,
        );

        BasicState {
            exposures,
            margin_balances_by_scope,
            usdt_equity_by_scope,
            um_unrealized_equity_by_scope,
            account_risk_equity_override,
            exposure_usdt_by_asset: price_update.exposure_usdt_by_asset,
            mark_usdt_by_asset: price_update.mark_usdt_by_asset,
            position_usdt_by_asset: price_update.position_usdt_by_asset,
            total_equity_usdt: price_update.total_equity_usdt,
            abs_total_exposure_usdt: price_update.abs_total_exposure_usdt,
            total_position_usdt: price_update.total_position_usdt,
            total_um_unrealized_usdt,
        }
    }

    fn account_risk_equity_override_for_inner(
        inner: &MonitorChannelInner,
    ) -> Option<(BasicAccountScope, f64)> {
        let scope = exchange_scoped_total_equity_scope(
            inner.open_venue,
            inner.hedge_venue,
            inner.binance_account_mode,
            inner.hyperliquid_account_mode,
        )?;
        let risk = inner.latest_account_risk.get(&scope)?;
        (risk.actual_equity_usd.is_finite() && risk.actual_equity_usd.abs() > f64::EPSILON)
            .then_some((scope, risk.actual_equity_usd))
    }

    fn active_strategy_base_assets(inner: &MonitorChannelInner) -> HashSet<String> {
        let mut assets = HashSet::new();
        let strategy_mgr = inner.strategy_mgr.borrow();
        for strategy_id in strategy_mgr.iter_ids() {
            let Some(strategy) = strategy_mgr.get(*strategy_id) else {
                continue;
            };
            let Some(symbol) = strategy.symbol() else {
                continue;
            };
            let symbol = normalize_symbol_for_internal(symbol);
            if let Some(asset) = extract_base_asset(&symbol) {
                assets.insert(asset.to_uppercase());
            }
        }
        assets
    }

    fn should_keep_non_trading_position(
        asset: &str,
        gross_qty: f64,
        min_non_trading_position_usdt: f64,
        active_assets: &HashSet<String>,
        price_mapper: &dyn crate::pre_trade::symbol_mapper::SymbolMapper,
        price_table: &PriceTable,
    ) -> bool {
        if is_exposure_exempt_asset(asset) || active_assets.contains(asset) {
            return true;
        }
        let mark = Self::mark_price_for_asset(price_mapper, price_table, asset);
        if mark <= 0.0 {
            return true;
        }
        gross_qty.abs() * mark >= min_non_trading_position_usdt
    }

    fn filter_non_trading_dust_positions(
        inner: &MonitorChannelInner,
        price_table: &PriceTable,
        exposures: &mut HashMap<String, (f64, f64)>,
        margin_balances_by_scope: &mut HashMap<BasicAccountScope, HashMap<String, f64>>,
    ) {
        let min_non_trading_position_usdt =
            PreTradeParamsLoader::instance().min_non_trading_position_usdt();
        if !(min_non_trading_position_usdt.is_finite() && min_non_trading_position_usdt > 0.0) {
            return;
        }

        let price_mapper = create_symbol_mapper(Self::mark_price_exchange_for_venues(
            inner.open_venue,
            inner.hedge_venue,
        ));
        let active_assets = Self::active_strategy_base_assets(inner);
        let mut retained_exposure_assets = HashSet::with_capacity(exposures.len());

        exposures.retain(|asset, (open_qty, hedge_qty)| {
            let gross_qty = open_qty.abs() + hedge_qty.abs();
            let keep = Self::should_keep_non_trading_position(
                asset,
                gross_qty,
                min_non_trading_position_usdt,
                &active_assets,
                &*price_mapper,
                price_table,
            );
            if keep {
                retained_exposure_assets.insert(asset.clone());
            }
            keep
        });

        for balances in margin_balances_by_scope.values_mut() {
            balances.retain(|asset, qty| {
                retained_exposure_assets.contains(asset)
                    || Self::should_keep_non_trading_position(
                        asset,
                        qty.abs(),
                        min_non_trading_position_usdt,
                        &active_assets,
                        &*price_mapper,
                        price_table,
                    )
            });
        }
        margin_balances_by_scope.retain(|_, balances| !balances.is_empty());
    }

    fn mark_price_for_asset(
        price_mapper: &dyn crate::pre_trade::symbol_mapper::SymbolMapper,
        price_table: &PriceTable,
        asset: &str,
    ) -> f64 {
        if asset.eq_ignore_ascii_case("USDT") || asset.eq_ignore_ascii_case("USDC") {
            1.0
        } else {
            let symbol = price_mapper.asset_to_price_symbol(asset);
            price_table
                .mark_price(&symbol)
                .or_else(|| price_table.mark_price(&format!("{}USD_PERP", asset.to_uppercase())))
                .unwrap_or(0.0)
        }
    }

    fn compute_basic_state_price_update_from_parts(
        inner: &MonitorChannelInner,
        price_table: &PriceTable,
        exposures: &HashMap<String, (f64, f64)>,
        margin_balances_by_scope: &HashMap<BasicAccountScope, HashMap<String, f64>>,
        usdt_equity_by_scope: &HashMap<BasicAccountScope, f64>,
        um_unrealized_equity_by_scope: &HashMap<BasicAccountScope, f64>,
        account_risk_equity_override: Option<(BasicAccountScope, f64)>,
    ) -> BasicStatePriceUpdate {
        let price_mapper = create_symbol_mapper(Self::mark_price_exchange_for_venues(
            inner.open_venue,
            inner.hedge_venue,
        ));

        let mut scope_equity_usdt: HashMap<BasicAccountScope, f64> = HashMap::new();
        for (scope, balances) in margin_balances_by_scope {
            for (asset, qty) in balances {
                let mark = Self::mark_price_for_asset(&*price_mapper, price_table, asset);
                if mark <= 0.0 {
                    continue;
                }
                *scope_equity_usdt.entry(*scope).or_insert(0.0) += qty * mark;
            }
        }
        for (scope, usdt) in usdt_equity_by_scope {
            *scope_equity_usdt.entry(*scope).or_insert(0.0) += *usdt;
        }
        for (scope, upl) in um_unrealized_equity_by_scope {
            *scope_equity_usdt.entry(*scope).or_insert(0.0) += *upl;
        }
        if let Some((scope, actual_equity_usd)) = account_risk_equity_override {
            scope_equity_usdt.insert(scope, actual_equity_usd);
        }

        let total_equity_usdt: f64 = scope_equity_usdt.values().sum();
        let mut exposure_usdt_by_asset = HashMap::new();
        let mut mark_usdt_by_asset = HashMap::new();
        for (symbol, price) in price_table.iter() {
            if !(price.mark_price.is_finite() && price.mark_price > 0.0) {
                continue;
            }
            if let Some(asset) = extract_base_asset(symbol) {
                mark_usdt_by_asset.insert(asset.to_uppercase(), price.mark_price);
            }
        }

        let mut total_position_usdt = 0.0;
        let mut abs_total_exposure_usdt = 0.0;
        let mut position_usdt_by_asset = HashMap::new();
        for (asset, (open_qty, hedge_qty)) in exposures {
            if is_exposure_exempt_asset(asset) {
                continue;
            }
            let mark = Self::mark_price_for_asset(&*price_mapper, price_table, asset);
            if mark <= 0.0 {
                continue;
            }
            let net_exposure_usdt = (open_qty + hedge_qty) * mark;
            mark_usdt_by_asset.insert(asset.clone(), mark);
            exposure_usdt_by_asset.insert(asset.clone(), net_exposure_usdt);
            let position_usdt = (open_qty.abs() + hedge_qty.abs()) * mark;
            position_usdt_by_asset.insert(asset.clone(), position_usdt);
            total_position_usdt += position_usdt;
            abs_total_exposure_usdt += net_exposure_usdt.abs();
        }

        BasicStatePriceUpdate {
            exposure_usdt_by_asset,
            mark_usdt_by_asset,
            position_usdt_by_asset,
            total_equity_usdt,
            abs_total_exposure_usdt,
            total_position_usdt,
        }
    }

    // 检查杠杆率是否超过配置阈值
    pub fn check_leverage(&self) -> Result<(), String> {
        Self::with_inner(|_inner| {
            let limit = PreTradeParamsLoader::instance().max_leverage();
            if limit <= 0.0 {
                return Ok(());
            }

            let (total_equity, um_unrealized, total_position) =
                Self::with_basic_state_cached(|state| {
                    (
                        state.total_equity_usdt,
                        state.total_um_unrealized_usdt,
                        state.total_position_usdt,
                    )
                });

            if total_equity <= f64::EPSILON {
                return Err("账户总权益(eq，含UPL如有合约)近似为 0，无法计算杠杆率".to_string());
            }

            let leverage = total_position / total_equity;
            if leverage > limit {
                debug!(
                    "当前杠杆 {:.4} 超过阈值 {:.4} (仓位={:.6}, 权益eq={:.6}, UPL={:.6})",
                    leverage, limit, total_position, total_equity, um_unrealized
                );
                return Err(format!("杠杆率 {:.2} 超过限制 {:.2}", leverage, limit));
            }

            Ok(())
        })
    }

    fn align_order_with_table(
        symbol: &str,
        raw_qty: f64,
        raw_price: f64,
        table: &VenueMinQtyTable,
        enforce_min_notional: bool,
    ) -> Result<(f64, f64), String> {
        if raw_qty <= 0.0 {
            return Err(format!(
                "symbol={} 原始下单量无效 raw_qty={}",
                symbol, raw_qty
            ));
        }
        if raw_price <= 0.0 {
            return Err(format!(
                "symbol={} 原始价格无效 raw_price={}",
                symbol, raw_price
            ));
        }

        // 1. 价格按 tick 对齐
        let price_tick = table.price_tick(symbol).unwrap_or(0.0);
        let price = if price_tick > 0.0 {
            align_price_floor(raw_price, price_tick)
        } else {
            raw_price
        };
        if price <= 0.0 {
            return Err(format!("symbol={} 对齐后价格无效 price={}", symbol, price));
        }

        // 2. 数量按 step 对齐
        let step = table.step_size(symbol).unwrap_or(0.0);
        let mut qty = if step > 0.0 {
            align_price_floor(raw_qty, step)
        } else {
            raw_qty
        };

        // 3. 补齐最小下单量
        if let Some(min_qty) = table.min_qty(symbol) {
            if min_qty > 0.0 && qty < min_qty {
                qty = min_qty;
            }
        }

        // 4. 补齐最小名义金额（仅限 futures 场景）
        if enforce_min_notional {
            if let Some(min_notional) = table.min_notional(symbol) {
                if min_notional > 0.0 {
                    let required_qty = min_notional / price;
                    if qty < required_qty {
                        let before = qty;
                        qty = if step > 0.0 {
                            align_price_ceil(required_qty, step)
                        } else {
                            required_qty
                        };
                        debug!(
                            "symbol={} 名义金额要求从 {} 调整到 {} (min_notional={}, price={})",
                            symbol, before, qty, min_notional, price
                        );
                    }
                }
            }
        }

        if qty <= 0.0 {
            return Err(format!("symbol={} 对齐后数量无效 qty={}", symbol, qty));
        }

        Ok((qty, price))
    }

    pub fn align_close_order_by_venue(
        &self,
        venue: TradingVenue,
        symbol: &str,
        raw_qty: f64,
        raw_price: f64,
    ) -> Result<Option<(f64, f64)>, String> {
        Self::with_inner(|inner| {
            let symbol_key = min_qty_symbol_key(venue, symbol);
            let Some(table) = inner.venue_min_qty_tables.get(&venue) else {
                return Err(format!(
                    "未初始化 {:?} 的最小下单量表，请检查启动参数",
                    venue
                ));
            };
            if raw_qty <= 0.0 {
                return Ok(None);
            }
            if raw_price <= 0.0 {
                return Err(format!(
                    "symbol={} close 原始价格无效 raw_price={}",
                    symbol_key, raw_price
                ));
            }

            let price_tick = table.price_tick(&symbol_key).unwrap_or(0.0);
            let price = if price_tick > 0.0 {
                align_price_floor(raw_price, price_tick)
            } else {
                raw_price
            };
            if price <= 0.0 {
                return Err(format!(
                    "symbol={} close 对齐后价格无效 price={}",
                    symbol_key, price
                ));
            }

            let step = table.step_size(&symbol_key).unwrap_or(0.0);
            let qty = if step > 0.0 {
                align_price_floor(raw_qty, step)
            } else {
                raw_qty
            };
            if qty <= 0.0 {
                return Ok(None);
            }

            let min_qty = table.min_qty(&symbol_key).unwrap_or(0.0);
            if min_qty > 0.0 && qty + 1e-12 < min_qty {
                return Ok(None);
            }

            Ok(Some((qty, price)))
        })
    }

    /// 根据交易场所对齐订单量和价格
    /// 返回 (对齐后的数量, 对齐后的价格)
    pub fn align_order_by_venue(
        &self,
        venue: TradingVenue,
        symbol: &str,
        raw_qty: f64,
        raw_price: f64,
    ) -> Result<(f64, f64), String> {
        Self::with_inner(|inner| {
            let symbol_key = min_qty_symbol_key(venue, symbol);

            let Some(table) = inner.venue_min_qty_tables.get(&venue) else {
                return Err(format!(
                    "未初始化 {:?} 的最小下单量表，请检查启动参数",
                    venue
                ));
            };

            match venue {
                TradingVenue::BinanceFutures => {
                    // Binance U 本地统一按 contracts(multiplier=1.0) 处理
                    Self::align_order_with_table(
                        &symbol_key,
                        raw_qty,
                        raw_price,
                        table.as_ref(),
                        true,
                    )
                }
                TradingVenue::BinanceCoinFutures | TradingVenue::BitgetCoinFutures => {
                    let contract_size = table
                        .contract_multiplier_opt(&symbol_key)
                        .ok_or_else(|| {
                            format!(
                                "symbol={} 缺少 {:?} inverse face value，无法将 base qty 转成 venue qty",
                                symbol_key, venue
                            )
                        })?;
                    if !raw_price.is_finite() || raw_price <= 0.0 {
                        return Err(format!(
                            "symbol={} {:?} 下单数量对齐需要正价格，got {}",
                            symbol_key, venue, raw_price
                        ));
                    }
                    if !contract_size.is_finite() || contract_size <= 0.0 {
                        return Err(format!(
                            "symbol={} {:?} inverse face value invalid: {}",
                            symbol_key, venue, contract_size
                        ));
                    }
                    let raw_contracts = raw_qty * raw_price / contract_size;
                    let (mut contracts, aligned_price) = Self::align_order_with_table(
                        &symbol_key,
                        raw_contracts,
                        raw_price,
                        table.as_ref(),
                        false,
                    )?;
                    if let Some(min_notional) = table.min_notional(&symbol_key) {
                        if min_notional > 0.0 {
                            let required_contracts = min_notional / contract_size;
                            if contracts < required_contracts {
                                let step = table.step_size(&symbol_key).unwrap_or(0.0);
                                contracts = if step > 0.0 {
                                    align_price_ceil(required_contracts, step)
                                } else {
                                    required_contracts
                                };
                            }
                        }
                    }
                    Ok((contracts, aligned_price))
                }
                TradingVenue::BinanceMargin => Self::align_order_with_table(
                    &symbol_key,
                    raw_qty,
                    raw_price,
                    table.as_ref(),
                    false,
                ),
                TradingVenue::OkexMargin => {
                    // OKX 现货/保证金 sz 使用标的资产数量，与 BinanceMargin 语义一致
                    Self::align_order_with_table(
                        &symbol_key,
                        raw_qty,
                        raw_price,
                        table.as_ref(),
                        false,
                    )
                }
                TradingVenue::OkexFutures | TradingVenue::GateFutures => {
                    // OKX/Gate 永续/交割合约 sz 使用“张数”，需要用合约乘数将 base qty 转成 contracts
                    let contract_size = table.contract_multiplier_opt(&symbol_key).ok_or_else(|| {
                        format!(
                            "symbol={} 缺少 {:?} 合约乘数，无法将 base qty 转成 contracts（请刷新 filters/multipliers）",
                            symbol_key,
                            venue
                        )
                    })?;
                    if contract_size <= 0.0 {
                        return Err(format!(
                            "symbol={} {:?} contract multiplier invalid: {}",
                            symbol_key, venue, contract_size
                        ));
                    }
                    let raw_contracts = raw_qty / contract_size;
                    debug!(
                        "futures qty convert: venue={:?} symbol={} raw_base_qty={:.8} contract_size={:.8} -> raw_contracts={:.8}",
                        venue, symbol_key, raw_qty, contract_size, raw_contracts
                    );
                    Self::align_order_with_table(
                        &symbol_key,
                        raw_contracts,
                        raw_price,
                        table.as_ref(),
                        true,
                    )
                }
                TradingVenue::BitgetMargin => Self::align_order_with_table(
                    &symbol_key,
                    raw_qty,
                    raw_price,
                    table.as_ref(),
                    false,
                ),
                TradingVenue::BitgetFutures => Self::align_order_with_table(
                    &symbol_key,
                    raw_qty,
                    raw_price,
                    table.as_ref(),
                    true,
                ),
                TradingVenue::BybitMargin => Self::align_order_with_table(
                    &symbol_key,
                    raw_qty,
                    raw_price,
                    table.as_ref(),
                    false,
                ),
                TradingVenue::BybitFutures => Self::align_order_with_table(
                    &symbol_key,
                    raw_qty,
                    raw_price,
                    table.as_ref(),
                    true,
                ),
                TradingVenue::GateMargin => Self::align_order_with_table(
                    &symbol_key,
                    raw_qty,
                    raw_price,
                    table.as_ref(),
                    false,
                ),
                TradingVenue::HyperliquidMargin => Self::align_order_with_table(
                    &symbol_key,
                    raw_qty,
                    raw_price,
                    table.as_ref(),
                    true,
                ),
                TradingVenue::HyperliquidFutures => Self::align_order_with_table(
                    &symbol_key,
                    raw_qty,
                    raw_price,
                    table.as_ref(),
                    true,
                ),
                TradingVenue::AsterMargin | TradingVenue::AsterFutures => {
                    Err("尚未实现 Aster 的订单对齐".to_string())
                }
            }
        })
    }

    /// 检查交易量是否满足最小要求
    /// 包括最小下单量和最小名义金额检查
    pub fn check_min_trading_requirements(
        &self,
        venue: TradingVenue,
        symbol: &str,
        qty: f64,
        price_hint: Option<f64>,
    ) -> Result<(), String> {
        Self::with_inner(|inner| {
            let symbol_key = min_qty_symbol_key(venue, symbol);

            let Some(table) = inner.venue_min_qty_tables.get(&venue) else {
                return Err(format!(
                    "未初始化 {:?} 的最小下单量表，请检查启动参数",
                    venue
                ));
            };

            // 1. 检查最小下单量
            let min_qty = table.min_qty(&symbol_key).unwrap_or(0.0);

            if min_qty > 0.0 && qty + 1e-12 < min_qty {
                return Err(format!("交易量 {:.8} 小于最小下单量 {:.8}", qty, min_qty));
            }

            // 2. 检查最小名义金额。Margin 现货 close 不能向上补数量，否则可能反向开仓；
            // 因此这里对表里提供 min_notional 的 venue 直接拒绝小额订单。
            if matches!(
                venue,
                TradingVenue::BinanceMargin
                    | TradingVenue::BinanceFutures
                    | TradingVenue::BinanceCoinFutures
                    | TradingVenue::OkexMargin
                    | TradingVenue::OkexFutures
                    | TradingVenue::BitgetMargin
                    | TradingVenue::BitgetFutures
                    | TradingVenue::BitgetCoinFutures
                    | TradingVenue::BybitMargin
                    | TradingVenue::BybitFutures
                    | TradingVenue::GateMargin
                    | TradingVenue::GateFutures
            ) {
                let min_notional = table.min_notional(&symbol_key).unwrap_or(0.0);

                if min_notional > 0.0 {
                    // 如果没有提供价格提示，尝试从价格表获取
                    let price = if let Some(p) = price_hint {
                        p
                    } else {
                        inner
                            .price_table
                            .borrow()
                            .mark_price(&symbol_key)
                            .unwrap_or(0.0)
                    };

                    if price <= 0.0 {
                        return Err(format!("缺少 {} 的价格信息，无法验证名义金额", symbol));
                    }

                    let notional = if venue.is_inverse_futures() {
                        let contract_size =
                            table.contract_multiplier_opt(&symbol_key).ok_or_else(|| {
                                format!(
                                    "symbol={} 缺少 {:?} inverse face value，无法验证名义金额",
                                    symbol_key, venue
                                )
                            })?;
                        qty * contract_size
                    } else {
                        price * qty
                    };
                    if notional + 1e-8 < min_notional {
                        return Err(format!(
                            "名义金额 {:.8} 低于最小要求 {:.8} (价格={:.8} 数量={:.8})",
                            notional, min_notional, price, qty
                        ));
                    }
                }
            }

            Ok(())
        })
    }

    // ==================== 风控方法（从 RiskChecker 迁移） ====================

    /// 检查当前 symbol 的限价挂单数量（MM 路径，使用 max_pending_limit_buy/sell_orders）
    pub fn check_pending_limit_order(&self, symbol: &str, side: Side) -> Result<(), String> {
        let params = PreTradeParamsLoader::instance();
        let side_limit = match side {
            Side::Buy => params.max_pending_limit_buy_orders(),
            Side::Sell => params.max_pending_limit_sell_orders(),
        };
        Self::check_pending_limit_order_with_side_limit(symbol, side, side_limit)
    }

    /// 检查当前 symbol 的限价挂单数量（套利路径，使用 arb_max_pending_limit_buy/sell_orders）
    pub fn check_pending_limit_order_for_arb(
        &self,
        symbol: &str,
        side: Side,
    ) -> Result<(), String> {
        let params = PreTradeParamsLoader::instance();
        let side_limit = match side {
            Side::Buy => params.arb_max_pending_limit_buy_orders(),
            Side::Sell => params.arb_max_pending_limit_sell_orders(),
        };
        Self::check_pending_limit_order_with_side_limit(symbol, side, side_limit)
    }

    /// 检查 ArbClose 当前 symbol/side 的限价挂单数量。
    /// Close 使用独立计数和独立方向上限，不占用 open/MM/exec 的挂单额度。
    pub fn check_pending_limit_order_for_arb_close(
        &self,
        symbol: &str,
        side: Side,
    ) -> Result<(), String> {
        let params = PreTradeParamsLoader::instance();
        let side_limit = match side {
            Side::Buy => params.arb_close_max_pending_limit_buy_orders(),
            Side::Sell => params.arb_close_max_pending_limit_sell_orders(),
        };
        Self::check_pending_arb_close_limit_order_with_side_limit(symbol, side, side_limit)
    }

    /// 检查当前 symbol 的限价挂单数量（Exec 路径，使用总上限和独立方向上限）
    pub fn check_pending_limit_order_for_exec(
        &self,
        symbol: &str,
        side: Side,
    ) -> Result<(), String> {
        let params = PreTradeParamsLoader::instance();
        let side_limit = match side {
            Side::Buy => params.exec_max_pending_limit_buy_orders(),
            Side::Sell => params.exec_max_pending_limit_sell_orders(),
        };
        Self::check_pending_limit_order_with_side_limit(symbol, side, side_limit)
    }

    fn check_pending_limit_order_with_side_limit(
        symbol: &str,
        side: Side,
        side_limit: i32,
    ) -> Result<(), String> {
        Self::with_inner(|inner| {
            let params = PreTradeParamsLoader::instance();
            let max_pending_limit_orders = params.max_pending_limit_orders();

            let symbol_upper = uppercase_symbol_key(symbol);
            let order_manager = inner.order_manager.borrow();

            if max_pending_limit_orders > 0 {
                let count =
                    order_manager.get_symbol_pending_limit_order_count_normalized(&symbol_upper);
                if count >= max_pending_limit_orders {
                    return Err(format!(
                        "symbol={} 当前限价挂单数={}，达到总上限 {}",
                        symbol, count, max_pending_limit_orders
                    ));
                }
            }

            if side_limit > 0 {
                let side_count = order_manager
                    .get_symbol_pending_limit_order_count_by_side_normalized(&symbol_upper, side);
                if side_count >= side_limit {
                    return Err(format!(
                        "symbol={} side={} 当前限价挂单数={}，达到方向上限 {}",
                        symbol,
                        side.as_str(),
                        side_count,
                        side_limit
                    ));
                }
            }

            Ok(())
        })
    }

    fn check_pending_arb_close_limit_order_with_side_limit(
        symbol: &str,
        side: Side,
        side_limit: i32,
    ) -> Result<(), String> {
        Self::with_inner(|inner| {
            if side_limit > 0 {
                let symbol_upper = uppercase_symbol_key(symbol);
                let side_count = inner
                    .order_manager
                    .borrow()
                    .get_symbol_pending_arb_close_limit_order_count_by_side_normalized(
                        &symbol_upper,
                        side,
                    );
                if side_count >= side_limit {
                    return Err(format!(
                        "symbol={} side={} 当前平仓限价挂单数={}，达到平仓方向上限 {}",
                        symbol,
                        side.as_str(),
                        side_count,
                        side_limit
                    ));
                }
            }

            Ok(())
        })
    }

    /// 当前 symbol 净敞口的绝对值（USDT）。缺价格或账户状态时返回 None。
    pub fn try_abs_symbol_net_exposure_usdt(symbol: &str) -> Option<f64> {
        let base_asset = extract_base_asset_key(symbol)?;
        if Self::try_with_inner(|_| ()).is_none() || !Self::basic_state_cache_present() {
            return None;
        }
        Self::with_basic_state_cached(|state| {
            if is_exposure_exempt_asset(base_asset.as_ref()) {
                Some(0.0)
            } else {
                state
                    .exposure_usdt_by_asset
                    .get(base_asset.as_ref())
                    .copied()
                    .map(f64::abs)
            }
        })
    }

    /// `max_pos_u` 被压得很小时，1% 单币敞口检查会拦下几乎对冲完的残仓。
    /// 净敞口不到 100 USDT 时跳过这类 ERROR / 未激活 summary，风控本身仍拒绝开仓。
    pub fn should_skip_small_symbol_exposure_risk_log(symbol: &str) -> bool {
        Self::try_abs_symbol_net_exposure_usdt(symbol)
            .is_some_and(|usdt| usdt.is_finite() && usdt < SMALL_SYMBOL_NET_EXPOSURE_LOG_SKIP_USDT)
    }

    /// 检查当前symbol的敞口是否超过总资产比例限制
    pub fn check_symbol_exposure(&self, symbol: &str) -> Result<(), String> {
        Self::with_inner(|inner| {
            Self::check_symbol_exposure_cached_inner(
                inner.open_venue,
                symbol,
                PreTradeParamsLoader::instance().max_symbol_exposure_ratio(),
            )
        })
    }

    /// 检查总敞口是否超过配置阈值（分母为 eq，若涉及合约 venue 则含 UPL）
    pub fn check_total_exposure(&self) -> Result<(), String> {
        Self::with_inner(|_inner| {
            let limit = PreTradeParamsLoader::instance().max_total_exposure_ratio();
            Self::with_basic_state_cached(|state| {
                Self::check_total_exposure_from_state(limit, state)
            })
        })
    }

    /// 检查 open 热路径所需的 symbol/total exposure。只读取一次 BasicState cache。
    pub fn check_open_exposure(&self, symbol: &str) -> Result<(), OpenExposureRiskError> {
        Self::with_inner(|inner| {
            let loader = PreTradeParamsLoader::instance();
            let symbol_limit = loader.max_symbol_exposure_ratio();
            let total_limit = loader.max_total_exposure_ratio();
            let max_pos_u = if symbol_limit > 0.0 {
                let max_pos_u = loader.max_pos_u_for_symbol(inner.open_venue, symbol);
                if max_pos_u <= f64::EPSILON {
                    return Err(OpenExposureRiskError::Symbol(
                        "max_pos_u 配置无效，无法校验敞口比例".to_string(),
                    ));
                }
                Some(max_pos_u)
            } else {
                None
            };
            let base_asset = if symbol_limit > 0.0 {
                Some(extract_base_asset_key(symbol).ok_or_else(|| {
                    OpenExposureRiskError::Symbol(format!(
                        "无法识别 symbol={} 的基础资产，无法校验敞口比例",
                        symbol
                    ))
                })?)
            } else {
                None
            };

            Self::with_basic_state_cached(|state| {
                if let (Some(base_asset), Some(max_pos_u)) = (base_asset.as_ref(), max_pos_u) {
                    Self::check_symbol_exposure_from_state(
                        symbol,
                        base_asset.as_ref(),
                        base_asset.as_ref(),
                        symbol_limit,
                        max_pos_u,
                        state,
                    )
                    .map_err(OpenExposureRiskError::Symbol)?;
                }
                Self::check_total_exposure_from_state(total_limit, state)
                    .map_err(OpenExposureRiskError::Total)
            })
        })
    }

    fn check_symbol_exposure_cached_inner(
        open_venue: TradingVenue,
        symbol: &str,
        limit: f64,
    ) -> Result<(), String> {
        if limit <= 0.0 {
            return Ok(());
        }
        let max_pos_u = PreTradeParamsLoader::instance().max_pos_u_for_symbol(open_venue, symbol);
        if max_pos_u <= f64::EPSILON {
            return Err("max_pos_u 配置无效，无法校验敞口比例".to_string());
        }

        let Some(base_asset) = extract_base_asset_key(symbol) else {
            return Err(format!(
                "无法识别 symbol={} 的基础资产，无法校验敞口比例",
                symbol
            ));
        };

        Self::with_basic_state_cached(|state| {
            Self::check_symbol_exposure_from_state(
                symbol,
                base_asset.as_ref(),
                base_asset.as_ref(),
                limit,
                max_pos_u,
                state,
            )
        })
    }

    fn check_symbol_exposure_from_state(
        symbol: &str,
        base_asset: &str,
        base_asset_upper: &str,
        limit: f64,
        max_pos_u: f64,
        state: &BasicState,
    ) -> Result<(), String> {
        if is_exposure_exempt_asset(base_asset) {
            return Ok(());
        }
        let net_exposure = state
            .exposures
            .get(base_asset_upper)
            .map(|(open, hedge)| open + hedge)
            .unwrap_or(0.0);
        let exposure_usdt = state.exposure_usdt_by_asset.get(base_asset_upper).copied();

        let Some(exposure_usdt) = exposure_usdt else {
            let ratio = net_exposure.abs() / max_pos_u;
            if ratio > limit {
                debug!(
                    "资产 {} 敞口占比(数量) {:.4}% 超过阈值 {:.2}% (敞口qty={:.6}, max_pos_u={:.6})",
                    base_asset,
                    ratio * 100.0,
                    limit * 100.0,
                    net_exposure,
                    max_pos_u
                );
                return Err(format!("symbol={} 敞口比例超过限制 {}", symbol, limit));
            }
            return Ok(());
        };

        let ratio = exposure_usdt.abs() / max_pos_u;
        if ratio > limit {
            debug!(
                "资产 {} 敞口占比 {:.4}% 超过阈值 {:.2}% (敞口USDT={:.6}, max_pos_u={:.6})",
                base_asset,
                ratio * 100.0,
                limit * 100.0,
                exposure_usdt,
                max_pos_u
            );
            return Err(format!("symbol={} 敞口比例超过限制 {}", symbol, limit));
        }

        Ok(())
    }

    fn check_total_exposure_from_state(limit: f64, state: &BasicState) -> Result<(), String> {
        if limit <= 0.0 {
            return Ok(());
        }

        let total_equity = state.total_equity_usdt;
        let abs_total_usdt = state.abs_total_exposure_usdt;

        if total_equity <= f64::EPSILON {
            return Err("账户总权益(eq，含UPL如有合约)近似为 0，无法计算总敞口占比".to_string());
        }

        let ratio = abs_total_usdt / total_equity;
        if ratio > limit {
            debug!(
                "总敞口占比 {:.4}% 超过阈值 {:.2}% (总敞口USDT={:.6}, 权益eq={:.6})",
                ratio * 100.0,
                limit * 100.0,
                abs_total_usdt,
                total_equity
            );
            return Err(format!(
                "总敞口比例 {:.2}% 超过限制 {:.2}%",
                ratio * 100.0,
                limit * 100.0
            ));
        }

        Ok(())
    }

    fn arb_hedge_exposure_projection_inner(
        inner: &MonitorChannelInner,
        state: &BasicState,
        symbol: &str,
        hedge_venue: TradingVenue,
        hedge_signed_base_qty: f64,
    ) -> Result<ArbHedgeExposureProjection, String> {
        let loader = PreTradeParamsLoader::instance();
        let symbol_limit_ratio = loader.max_symbol_exposure_ratio();
        let total_limit_ratio = loader.max_total_exposure_ratio();
        let base_asset = extract_base_asset_key(symbol).ok_or_else(|| {
            format!(
                "无法识别 symbol={} 的基础资产，无法校验 ArbHedge 敞口",
                symbol
            )
        })?;
        if is_exposure_exempt_asset(base_asset.as_ref()) {
            return Ok(ArbHedgeExposureProjection {
                symbol_current_exposure_usdt: 0.0,
                symbol_next_exposure_usdt: 0.0,
                symbol_limit_usdt: f64::INFINITY,
                total_current_exposure_usdt: state.abs_total_exposure_usdt,
                total_next_exposure_usdt: state.abs_total_exposure_usdt,
                total_limit_usdt: f64::INFINITY,
            });
        }
        let mark = state
            .mark_usdt_by_asset
            .get(base_asset.as_ref())
            .copied()
            .unwrap_or(0.0);
        if mark <= 0.0 {
            return Err(format!(
                "symbol={} 缺少 USDT 标记价格，无法校验 ArbHedge 敞口",
                symbol
            ));
        }
        let (open_qty, hedge_qty) = state
            .exposures
            .get(base_asset.as_ref())
            .copied()
            .unwrap_or((0.0, 0.0));
        let current_net_qty = open_qty + hedge_qty;
        let next_net_qty = if hedge_venue == inner.open_venue || hedge_venue == inner.hedge_venue {
            current_net_qty + hedge_signed_base_qty
        } else {
            return Err(format!(
                "ArbHedge venue {:?} 不匹配 open={:?} hedge={:?}",
                hedge_venue, inner.open_venue, inner.hedge_venue
            ));
        };
        let symbol_current_exposure_usdt = current_net_qty.abs() * mark;
        let symbol_next_exposure_usdt = next_net_qty.abs() * mark;
        let max_pos_u = loader.max_pos_u_for_symbol(inner.open_venue, symbol);
        if max_pos_u <= f64::EPSILON && symbol_limit_ratio > 0.0 {
            return Err("max_pos_u 配置无效，无法校验 ArbHedge 单币敞口".to_string());
        }
        let symbol_limit_usdt = if symbol_limit_ratio > 0.0 {
            max_pos_u * symbol_limit_ratio
        } else {
            f64::INFINITY
        };

        let total_current_exposure_usdt = state.abs_total_exposure_usdt;
        let total_next_exposure_usdt = (total_current_exposure_usdt - symbol_current_exposure_usdt
            + symbol_next_exposure_usdt)
            .max(0.0);
        let total_limit_usdt = if total_limit_ratio > 0.0 {
            let total_equity = state.total_equity_usdt;
            if total_equity <= f64::EPSILON {
                return Err(
                    "账户总权益(eq，含UPL如有合约)近似为 0，无法校验 ArbHedge 总敞口".to_string(),
                );
            }
            total_equity * total_limit_ratio
        } else {
            f64::INFINITY
        };

        Ok(ArbHedgeExposureProjection {
            symbol_current_exposure_usdt,
            symbol_next_exposure_usdt,
            symbol_limit_usdt,
            total_current_exposure_usdt,
            total_next_exposure_usdt,
            total_limit_usdt,
        })
    }

    /// ArbHedge 报单只做敞口风控：如果本单降低敞口则放行；否则当前敞口必须仍在阈值内。
    pub fn check_arb_hedge_exposure_risk(
        &self,
        symbol: &str,
        hedge_venue: TradingVenue,
        hedge_signed_base_qty: f64,
    ) -> Result<(), String> {
        Self::with_inner(|inner| {
            Self::with_basic_state_cached(|state| {
                let projection = Self::arb_hedge_exposure_projection_inner(
                    inner,
                    state,
                    symbol,
                    hedge_venue,
                    hedge_signed_base_qty,
                )?;
                let eps = 1e-6_f64;
                if projection.symbol_next_exposure_usdt
                    > projection.symbol_current_exposure_usdt + eps
                    && projection.symbol_current_exposure_usdt > projection.symbol_limit_usdt + eps
                {
                    return Err(format!(
                        "symbol={} ArbHedge 单币敞口扩大且当前已超限: current={:.4}USDT next={:.4}USDT limit={:.4}USDT",
                        symbol,
                        projection.symbol_current_exposure_usdt,
                        projection.symbol_next_exposure_usdt,
                        projection.symbol_limit_usdt
                    ));
                }
                if projection.total_next_exposure_usdt
                    > projection.total_current_exposure_usdt + eps
                    && projection.total_current_exposure_usdt > projection.total_limit_usdt + eps
                {
                    return Err(format!(
                        "symbol={} ArbHedge 总敞口扩大且当前已超限: current={:.4}USDT next={:.4}USDT limit={:.4}USDT",
                        symbol,
                        projection.total_current_exposure_usdt,
                        projection.total_next_exposure_usdt,
                        projection.total_limit_usdt
                    ));
                }
                Ok(())
            })
        })
    }

    /// 检查最大持仓限制
    pub fn ensure_max_pos_u(
        &self,
        symbol: &str,
        additional_qty: f64,
        price_hint: f64,
    ) -> Result<(), String> {
        self.ensure_max_pos_u_for_venue(symbol, None, additional_qty, price_hint)
    }

    pub fn ensure_max_pos_u_for_venue(
        &self,
        symbol: &str,
        venue_override: Option<TradingVenue>,
        additional_qty: f64,
        price_hint: f64,
    ) -> Result<(), String> {
        Self::with_inner(|inner| {
            let venue = venue_override.unwrap_or(inner.open_venue);
            let base_asset = extract_base_asset_key(symbol).ok_or_else(|| {
                format!("无法识别 symbol={} 的基础资产，无法校验 max_pos_u", symbol)
            })?;
            let symbol_upper = uppercase_symbol_key(symbol);
            let (qty_unit, fut_symbol_key, qty_multiplier) = match venue {
                TradingVenue::BinanceFutures => {
                    ("contracts(mult=1)", Some(symbol_upper), Some(1.0))
                }
                TradingVenue::BinanceCoinFutures | TradingVenue::BitgetCoinFutures => {
                    let symbol_key = min_qty_symbol_key(venue, symbol_upper.as_ref());
                    let mult = Self::qty_multiplier_for_venue_at_price_inner(
                        inner,
                        venue,
                        &symbol_key,
                        price_hint,
                    )
                    .ok();
                    ("inverse_contracts", Some(Cow::Owned(symbol_key)), mult)
                }
                TradingVenue::OkexFutures | TradingVenue::GateFutures => {
                    let symbol_key = min_qty_symbol_key(venue, symbol_upper.as_ref());
                    let mult = inner
                        .venue_min_qty_tables
                        .get(&venue)
                        .and_then(|t| t.contract_multiplier_opt(&symbol_key));
                    ("contracts", Some(Cow::Owned(symbol_key)), mult)
                }
                _ => ("base_qty", None, None),
            };

            let add_base_qty = match Self::order_qty_to_base_checked(
                inner,
                venue,
                symbol,
                additional_qty,
                price_hint,
            ) {
                Ok(v) => v,
                Err(e) => {
                    info!(
                        "max_pos_u check qty convert failed: symbol={} base_asset={} venue={:?} qty_unit={} raw_qty={:.8} fut_symbol_key={:?} qty_multiplier={:?} err={}",
                        symbol,
                        base_asset,
                        venue,
                        qty_unit,
                        additional_qty,
                        fut_symbol_key,
                        qty_multiplier,
                        e
                    );
                    return Err(e);
                }
            };
            let current_open_qty = Self::get_position_qty_inner(inner, symbol, venue);
            Self::ensure_max_pos_u_base_delta_inner(
                inner,
                symbol,
                venue,
                current_open_qty,
                add_base_qty,
                price_hint,
                qty_unit,
                additional_qty,
                fut_symbol_key.as_deref(),
                qty_multiplier,
            )
        })
    }

    pub fn ensure_max_pos_u_for_base_delta(
        &self,
        symbol: &str,
        venue: TradingVenue,
        current_open_qty: f64,
        add_base_qty: f64,
        price_hint: f64,
        raw_qty: f64,
        qty_multiplier: f64,
    ) -> Result<(), String> {
        Self::with_inner(|inner| {
            let symbol_upper = uppercase_symbol_key(symbol);
            let fut_symbol_key = match venue {
                TradingVenue::BinanceFutures => Some(symbol_upper),
                TradingVenue::BinanceCoinFutures | TradingVenue::BitgetCoinFutures => {
                    Some(Cow::Owned(min_qty_symbol_key(venue, symbol_upper.as_ref())))
                }
                TradingVenue::OkexFutures | TradingVenue::GateFutures => {
                    Some(Cow::Owned(min_qty_symbol_key(venue, symbol_upper.as_ref())))
                }
                _ => None,
            };
            let qty_unit = match venue {
                TradingVenue::BinanceFutures => "contracts(mult=1)",
                TradingVenue::BinanceCoinFutures | TradingVenue::BitgetCoinFutures => {
                    "inverse_contracts"
                }
                TradingVenue::OkexFutures | TradingVenue::GateFutures => "contracts",
                _ => "base_qty",
            };
            Self::ensure_max_pos_u_base_delta_inner(
                inner,
                symbol,
                venue,
                current_open_qty,
                add_base_qty,
                price_hint,
                qty_unit,
                raw_qty,
                fut_symbol_key.as_deref(),
                Some(qty_multiplier),
            )
        })
    }

    fn ensure_max_pos_u_base_delta_inner(
        inner: &MonitorChannelInner,
        symbol: &str,
        venue: TradingVenue,
        current_open_qty: f64,
        add_base_qty: f64,
        price_hint: f64,
        qty_unit: &'static str,
        raw_qty: f64,
        fut_symbol_key: Option<&str>,
        qty_multiplier: Option<f64>,
    ) -> Result<(), String> {
        let max_pos_u = PreTradeParamsLoader::instance().max_pos_u_for_symbol(venue, symbol);
        if max_pos_u.is_nan() || max_pos_u <= 0.0 {
            panic!("max_pos_u not set!!");
        }

        let base_asset = extract_base_asset_key(symbol)
            .ok_or_else(|| format!("无法识别 symbol={} 的基础资产，无法校验 max_pos_u", symbol))?;
        let price_mapper = create_symbol_mapper(Self::mark_price_exchange_for_venues(
            inner.open_venue,
            inner.hedge_venue,
        ));
        let mark_symbol = price_mapper.asset_to_price_symbol(base_asset.as_ref());
        let price_from_table = if base_asset.as_ref() == "USDT" {
            Some(1.0)
        } else {
            Self::with_basic_state_cached(|state| {
                state.mark_usdt_by_asset.get(base_asset.as_ref()).copied()
            })
        };
        let price = price_from_table.or({
            if price_hint > 0.0 {
                Some(price_hint)
            } else {
                None
            }
        });

        let Some(price) = price else {
            warn!("symbol={} 缺少 USDT 标记价格，无法校验 max_pos_u", symbol);
            return Err(format!(
                "symbol={} 缺少价格信息，无法校验 max_pos_u",
                symbol
            ));
        };

        let price_source = if price_from_table.is_some() {
            "mark_price_table"
        } else {
            "price_hint"
        };
        Self::ensure_max_pos_u_projected(MaxPosUCheckCtx {
            symbol,
            base_asset: base_asset.as_ref(),
            venue,
            price_source,
            mark_symbol: &mark_symbol,
            price,
            qty_unit,
            raw_qty,
            fut_symbol_key,
            qty_multiplier,
            current_open_qty,
            add_base_qty,
            max_pos_u,
        })
    }

    fn ensure_max_pos_u_projected(ctx: MaxPosUCheckCtx<'_>) -> Result<(), String> {
        let next_qty = ctx.current_open_qty + ctx.add_base_qty;
        let current_usdt = ctx.current_open_qty.abs() * ctx.price;
        let order_usdt = ctx.add_base_qty.abs() * ctx.price;
        let next_usdt = next_qty.abs() * ctx.price;
        let limit_eps = 1e-6_f64;

        if next_usdt <= current_usdt + limit_eps {
            return Ok(());
        }

        if next_usdt > ctx.max_pos_u + limit_eps {
            info!(
                "max_pos_u check reject detail: symbol={} base_asset={} venue={:?} price_source={} mark_symbol={} price={:.8} qty_unit={} raw_qty={:.8} fut_symbol_key={:?} qty_multiplier={:?} current_open_qty(base)={:.8} add_base_qty={:.8} next_qty(base)={:.8} current_usdt={:.4} order_usdt={:.4} next_usdt={:.4} max_pos_u={:.4}",
                ctx.symbol,
                ctx.base_asset,
                ctx.venue,
                ctx.price_source,
                ctx.mark_symbol,
                ctx.price,
                ctx.qty_unit,
                ctx.raw_qty,
                ctx.fut_symbol_key,
                ctx.qty_multiplier,
                ctx.current_open_qty,
                ctx.add_base_qty,
                next_qty,
                current_usdt,
                order_usdt,
                next_usdt,
                ctx.max_pos_u
            );
            warn!(
                "symbol={} 当前持仓={:.6}({:.4}USDT) 下单数量={:.6}({:.4}USDT) 下单后持仓={:.4}USDT 超过阈值 {:.4}USDT",
                ctx.symbol,
                ctx.current_open_qty,
                current_usdt,
                ctx.add_base_qty,
                order_usdt,
                next_usdt,
                ctx.max_pos_u
            );
            return Err(format!(
                "symbol={} 下单后持仓 {:.4}USDT 超过阈值 {:.4}USDT",
                ctx.symbol, next_usdt, ctx.max_pos_u
            ));
        }

        Ok(())
    }

    /// 获取指定交易对和交易场所的持仓数量（带符号）
    /// 返回持仓数量，正数表示多头，负数表示空头
    pub fn get_position_qty(&self, symbol: &str, venue: TradingVenue) -> f64 {
        Self::with_inner(|inner| Self::get_position_qty_inner(inner, symbol, venue))
    }

    pub fn mark_exec_position_snapshot_ready(&self, source: &'static str) {
        let changed = EXEC_POSITION_SNAPSHOT_READY.with(|ready| {
            let changed = !ready.get();
            ready.set(true);
            changed
        });
        if changed {
            info!("exec position snapshot ready: source={source}");
        }
    }

    fn set_hyperliquid_exec_snapshot_readiness(
        &self,
        valid_until_ms: Option<i64>,
        source: &'static str,
    ) {
        let factual_ready = HYPERLIQUID_FACT_STREAM_READY.with(Cell::get);
        let deadline = if factual_ready {
            valid_until_ms.unwrap_or(0)
        } else {
            0
        };
        HYPERLIQUID_EXEC_SNAPSHOT_VALID_UNTIL_MS.with(|value| value.set(deadline));
        if deadline > get_timestamp_us() / 1_000 {
            self.mark_exec_position_snapshot_ready(source);
        } else {
            let changed = EXEC_POSITION_SNAPSHOT_READY.with(|ready| {
                let changed = ready.get();
                ready.set(false);
                changed
            });
            if changed {
                warn!("exec position snapshot readiness revoked: source={source}");
            }
        }
    }

    pub fn exec_position_snapshot_ready(&self) -> bool {
        let ready = EXEC_POSITION_SNAPSHOT_READY.with(Cell::get);
        if !ready {
            return false;
        }
        let hyperliquid_exec = Self::try_with_inner(|inner| {
            exchange_from_venue(inner.open_venue) == Exchange::Hyperliquid
                && inner.open_venue == inner.hedge_venue
        })
        .unwrap_or(false);
        if !hyperliquid_exec {
            return true;
        }
        if !HYPERLIQUID_FACT_STREAM_READY.with(Cell::get) {
            EXEC_POSITION_SNAPSHOT_READY.with(|ready| ready.set(false));
            return false;
        }
        let valid_until = HYPERLIQUID_EXEC_SNAPSHOT_VALID_UNTIL_MS.with(Cell::get);
        if valid_until > get_timestamp_us() / 1_000 {
            true
        } else {
            EXEC_POSITION_SNAPSHOT_READY.with(|ready| ready.set(false));
            false
        }
    }

    pub fn refresh_exec_risk_state(&self) {
        Self::refresh_basic_state_cache();
    }

    // ==================== 内部辅助方法 ====================

    fn get_position_qty_inner(
        inner: &MonitorChannelInner,
        symbol: &str,
        venue: TradingVenue,
    ) -> f64 {
        let leg = if venue == inner.open_venue {
            &inner.open_leg
        } else if venue == inner.hedge_venue {
            &inner.hedge_leg
        } else {
            return 0.0;
        };

        match leg {
            LegMgr::Margin { bal, .. } => {
                let Some(base_asset) = extract_base_asset_key(symbol) else {
                    return 0.0;
                };
                bal.borrow().net_position(&base_asset, None)
            }
            LegMgr::Futures {
                um, min_qty_table, ..
            } => {
                if venue.is_inverse_futures() {
                    let venue_symbol = normalize_symbol_for_venue(symbol, venue);
                    let contracts = um.borrow().net_position(&venue_symbol, None);
                    let symbol_key = min_qty_symbol_key(venue, symbol);
                    let contract_size = inner
                        .venue_min_qty_tables
                        .get(&venue)
                        .and_then(|table| table.contract_multiplier_opt(&symbol_key));
                    let mark_price = inner.price_table.borrow().mark_price(&symbol_key);
                    return match (contract_size, mark_price) {
                        (Some(contract_size), Some(mark_price))
                            if contract_size.is_finite()
                                && contract_size > 0.0
                                && mark_price.is_finite()
                                && mark_price > 0.0 =>
                        {
                            contracts * contract_size / mark_price
                        }
                        _ => 0.0,
                    };
                }
                let table_ref = min_qty_table.borrow();
                um.borrow().net_position(symbol, Some(&table_ref))
            }
        }
    }
}

// ==================== Helper Functions ====================

/// 通用订单/成交回报分发：适用于实现了 OrderUpdate + TradeUpdate 的消息
struct NormalizedUpdate<'a, T> {
    inner: &'a T,
    symbol: Cow<'a, str>,
}

impl<'a, T> NormalizedUpdate<'a, T>
where
    T: OrderUpdate + TradeUpdate,
{
    fn new(inner: &'a T) -> Self {
        let raw_symbol = OrderUpdate::symbol(inner);
        let symbol = if is_internal_symbol_key(raw_symbol) {
            Cow::Borrowed(raw_symbol)
        } else {
            Cow::Owned(normalize_symbol_for_internal(raw_symbol))
        };
        Self { inner, symbol }
    }
}

fn is_internal_symbol_key(symbol: &str) -> bool {
    !symbol.is_empty()
        && !symbol.ends_with("SWAP")
        && symbol
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

fn uppercase_symbol_key(symbol: &str) -> Cow<'_, str> {
    if is_internal_symbol_key(symbol) {
        Cow::Borrowed(symbol)
    } else {
        Cow::Owned(symbol.to_uppercase())
    }
}

fn extract_base_asset_key(symbol: &str) -> Option<Cow<'_, str>> {
    if is_internal_symbol_key(symbol) {
        let (base, quote) = extract_assets_from_internal_symbol(symbol);
        if base.is_empty() || base.len() == symbol.len() || quote.is_empty() {
            None
        } else {
            Some(Cow::Borrowed(base))
        }
    } else {
        extract_base_asset(symbol).map(Cow::Owned)
    }
}

impl<T> OrderUpdate for NormalizedUpdate<'_, T>
where
    T: OrderUpdate + TradeUpdate,
{
    fn event_time(&self) -> i64 {
        OrderUpdate::event_time(self.inner)
    }

    fn symbol(&self) -> &str {
        self.symbol.as_ref()
    }

    fn order_id(&self) -> i64 {
        OrderUpdate::order_id(self.inner)
    }

    fn client_order_id(&self) -> i64 {
        OrderUpdate::client_order_id(self.inner)
    }

    fn side(&self) -> crate::pre_trade::order_manager::Side {
        OrderUpdate::side(self.inner)
    }

    fn order_type(&self) -> crate::pre_trade::order_manager::OrderType {
        OrderUpdate::order_type(self.inner)
    }

    fn time_in_force(&self) -> order_common::TimeInForce {
        OrderUpdate::time_in_force(self.inner)
    }

    fn price(&self) -> f64 {
        OrderUpdate::price(self.inner)
    }

    fn quantity(&self) -> f64 {
        OrderUpdate::quantity(self.inner)
    }

    fn cumulative_filled_quantity(&self) -> f64 {
        OrderUpdate::cumulative_filled_quantity(self.inner)
    }

    fn status(&self) -> OrderStatus {
        OrderUpdate::status(self.inner)
    }

    fn raw_status(&self) -> &str {
        OrderUpdate::raw_status(self.inner)
    }

    fn execution_type(&self) -> ExecutionType {
        OrderUpdate::execution_type(self.inner)
    }

    fn raw_execution_type(&self) -> &str {
        OrderUpdate::raw_execution_type(self.inner)
    }

    fn trading_venue(&self) -> TradingVenue {
        OrderUpdate::trading_venue(self.inner)
    }

    fn client_order_id_str(&self) -> Option<&str> {
        OrderUpdate::client_order_id_str(self.inner)
    }
}

impl<T> TradeUpdate for NormalizedUpdate<'_, T>
where
    T: OrderUpdate + TradeUpdate,
{
    fn event_time(&self) -> i64 {
        TradeUpdate::event_time(self.inner)
    }

    fn trade_time(&self) -> i64 {
        TradeUpdate::trade_time(self.inner)
    }

    fn symbol(&self) -> &str {
        self.symbol.as_ref()
    }

    fn order_id(&self) -> i64 {
        TradeUpdate::order_id(self.inner)
    }

    fn client_order_id(&self) -> i64 {
        TradeUpdate::client_order_id(self.inner)
    }

    fn side(&self) -> crate::pre_trade::order_manager::Side {
        TradeUpdate::side(self.inner)
    }

    fn price(&self) -> f64 {
        TradeUpdate::price(self.inner)
    }

    fn is_maker(&self) -> bool {
        TradeUpdate::is_maker(self.inner)
    }

    fn trading_venue(&self) -> TradingVenue {
        TradeUpdate::trading_venue(self.inner)
    }

    fn cumulative_filled_quantity(&self) -> f64 {
        TradeUpdate::cumulative_filled_quantity(self.inner)
    }

    fn order_status(&self) -> Option<OrderStatus> {
        TradeUpdate::order_status(self.inner)
    }
}

fn dispatch_trade_update_lite_generic<T>(
    strategy_mgr: &Rc<RefCell<crate::strategy::StrategyManager>>,
    trade: &T,
) -> bool
where
    T: TradeUpdateLite,
{
    MonitorChannel::instance().bump_trade_update_seq();

    let order_id = trade.client_order_id();
    let strategy_id = (order_id >> 32) as i32;
    let matched = dispatch_trade_update_lite_to_strategy(strategy_mgr, strategy_id, trade)
        || dispatch_trade_update_lite_fallback_scan(strategy_mgr, trade);

    if !matched {
        debug!(
            "trade lite unmatched: sym={} cli_id={} trade_id={:?}",
            trade.symbol(),
            trade.client_order_id(),
            trade.trade_id()
        );
    }
    matched
}

fn dispatch_trade_update_generic<T>(
    strategy_mgr: &Rc<RefCell<crate::strategy::StrategyManager>>,
    trade: &T,
) -> bool
where
    T: TradeUpdate,
{
    MonitorChannel::instance().bump_trade_update_seq();

    let order_id = trade.client_order_id();
    let strategy_id = (order_id >> 32) as i32;
    let matched = dispatch_trade_update_to_strategy(strategy_mgr, strategy_id, trade)
        || dispatch_trade_update_fallback_scan(strategy_mgr, trade);
    if matched {
        dispatch_ready_deferred_hyperliquid_terminal(strategy_mgr, order_id);
        return true;
    }

    let adopted_by_orphan = MonitorChannel::instance()
        .orphan_strategy_mgr()
        .borrow_mut()
        .apply_trade_update(trade);
    if !adopted_by_orphan {
        debug!(
            "trade update unmatched: sym={} cli_id={}",
            trade.symbol(),
            trade.client_order_id()
        );
    }
    if adopted_by_orphan {
        dispatch_ready_deferred_hyperliquid_terminal(strategy_mgr, order_id);
    }
    adopted_by_orphan
}

fn dispatch_ready_deferred_hyperliquid_terminal(
    strategy_mgr: &Rc<RefCell<crate::strategy::StrategyManager>>,
    client_order_id: i64,
) {
    if let Some(update) = take_ready_deferred_hyperliquid_terminal(client_order_id) {
        dispatch_lifecycle_order_update_generic(strategy_mgr, &update);
    }
}

fn dispatch_lifecycle_order_update_generic<T>(
    strategy_mgr: &Rc<RefCell<crate::strategy::StrategyManager>>,
    update: &T,
) where
    T: OrderUpdate,
{
    let order_id = update.client_order_id();
    let strategy_id = (order_id >> 32) as i32;
    let matched = dispatch_lifecycle_order_update_to_strategy(strategy_mgr, strategy_id, update)
        || dispatch_lifecycle_order_update_fallback_scan(strategy_mgr, update);

    if matched {
        return;
    }

    let adopted_by_orphan = MonitorChannel::instance()
        .orphan_strategy_mgr()
        .borrow_mut()
        .apply_order_update(update);
    if !adopted_by_orphan {
        PersistChannel::with(|channel| channel.publish_order_update_unmatched(update));
    }
    debug!(
        "lifecycle order update unmatched: sym={} cli_id={} ord_id={} x={:?} X={:?} orphan_adopted={}",
        update.symbol(),
        update.client_order_id(),
        update.order_id(),
        update.execution_type(),
        update.status(),
        adopted_by_orphan
    );
}

fn override_hyperliquid_order_intent_from_local(
    update: &mut HyperliquidBasicOrderMsg,
    local_order: Option<&Order>,
) -> bool {
    if update.client_order_id <= 0 {
        return false;
    }
    let Some(local_order) = local_order else {
        return false;
    };
    if local_order.client_order_id != update.client_order_id {
        warn!(
            "skip Hyperliquid order-intent override for client id mismatch: update={} local={}",
            update.client_order_id, local_order.client_order_id
        );
        return false;
    }
    if TradingVenue::from_u8(update.venue) != Some(local_order.venue) {
        warn!(
            "skip Hyperliquid order-intent override for venue mismatch: client_order_id={} update_venue={} local_venue={:?}",
            update.client_order_id, update.venue, local_order.venue
        );
        return false;
    }
    let Some(time_in_force) = hyperliquid_time_in_force(local_order.venue, local_order.order_type)
    else {
        warn!(
            "skip Hyperliquid order-intent override for unsupported local type: client_order_id={} order_type={:?}",
            update.client_order_id, local_order.order_type
        );
        return false;
    };
    update.order_type = local_order.order_type.to_u8();
    update.time_in_force = time_in_force.to_u8();
    true
}

fn dispatch_lifecycle_order_update_to_strategy<T>(
    strategy_mgr: &Rc<RefCell<crate::strategy::StrategyManager>>,
    strategy_id: i32,
    update: &T,
) -> bool
where
    T: OrderUpdate,
{
    let order_id = update.client_order_id();
    let strategy_opt = {
        let mut mgr = strategy_mgr.borrow_mut();
        if mgr.contains(strategy_id) {
            mgr.take(strategy_id)
        } else {
            None
        }
    };

    let Some(mut strategy) = strategy_opt else {
        return false;
    };
    let matched = strategy.is_strategy_order(order_id);
    if matched {
        strategy.apply_order_update(update);
    }
    if strategy.is_active() {
        strategy_mgr.borrow_mut().insert(strategy);
    }
    matched
}

fn dispatch_lifecycle_order_update_fallback_scan<T>(
    strategy_mgr: &Rc<RefCell<crate::strategy::StrategyManager>>,
    update: &T,
) -> bool
where
    T: OrderUpdate,
{
    let strategy_ids: Vec<i32> = strategy_mgr.borrow().iter_ids().cloned().collect();
    for strategy_id in strategy_ids {
        if dispatch_lifecycle_order_update_to_strategy(strategy_mgr, strategy_id, update) {
            return true;
        }
    }
    false
}

fn dispatch_trade_update_to_strategy<T>(
    strategy_mgr: &Rc<RefCell<crate::strategy::StrategyManager>>,
    strategy_id: i32,
    trade: &T,
) -> bool
where
    T: TradeUpdate,
{
    let order_id = trade.client_order_id();
    let strategy_opt = {
        let mut mgr = strategy_mgr.borrow_mut();
        if mgr.contains(strategy_id) {
            mgr.take(strategy_id)
        } else {
            None
        }
    };

    let Some(mut strategy) = strategy_opt else {
        return false;
    };

    let matched = strategy.is_strategy_order(order_id);
    if matched {
        strategy.apply_trade_update(trade);
    }
    if strategy.is_active() {
        strategy_mgr.borrow_mut().insert(strategy);
    }
    matched
}

fn dispatch_trade_update_fallback_scan<T>(
    strategy_mgr: &Rc<RefCell<crate::strategy::StrategyManager>>,
    trade: &T,
) -> bool
where
    T: TradeUpdate,
{
    let strategy_ids: Vec<i32> = strategy_mgr.borrow().iter_ids().cloned().collect();
    for strategy_id in strategy_ids {
        if dispatch_trade_update_to_strategy(strategy_mgr, strategy_id, trade) {
            return true;
        }
    }
    false
}

fn dispatch_trade_update_lite_to_strategy<T>(
    strategy_mgr: &Rc<RefCell<crate::strategy::StrategyManager>>,
    strategy_id: i32,
    trade: &T,
) -> bool
where
    T: TradeUpdateLite,
{
    let order_id = trade.client_order_id();
    let strategy_opt = {
        let mut mgr = strategy_mgr.borrow_mut();
        if mgr.contains(strategy_id) {
            mgr.take(strategy_id)
        } else {
            None
        }
    };

    let Some(mut strategy) = strategy_opt else {
        return false;
    };

    let matched = strategy.is_strategy_order(order_id);
    if matched {
        strategy.apply_trade_update_lite(trade);
    }
    if strategy.is_active() {
        strategy_mgr.borrow_mut().insert(strategy);
    }
    matched
}

fn dispatch_trade_update_lite_fallback_scan<T>(
    strategy_mgr: &Rc<RefCell<crate::strategy::StrategyManager>>,
    trade: &T,
) -> bool
where
    T: TradeUpdateLite,
{
    let strategy_ids: Vec<i32> = strategy_mgr.borrow().iter_ids().cloned().collect();
    for strategy_id in strategy_ids {
        if dispatch_trade_update_lite_to_strategy(strategy_mgr, strategy_id, trade) {
            return true;
        }
    }
    false
}

fn dispatch_order_update_generic<T>(
    strategy_mgr: &Rc<RefCell<crate::strategy::StrategyManager>>,
    update: &T,
) where
    T: OrderUpdate + TradeUpdate,
{
    let normalized_update = NormalizedUpdate::new(update);

    if normalized_update.execution_type() == ExecutionType::Trade {
        MonitorChannel::instance().bump_trade_update_seq();
    }

    let order_id = OrderUpdate::client_order_id(&normalized_update);
    let strategy_id = (order_id >> 32) as i32;
    let matched = dispatch_order_update_to_strategy(strategy_mgr, strategy_id, &normalized_update)
        || dispatch_order_update_fallback_scan(strategy_mgr, &normalized_update);

    if !matched {
        let orphan_strategy_mgr = MonitorChannel::instance().orphan_strategy_mgr();
        let adopted_by_orphan = if normalized_update.execution_type() == ExecutionType::Trade {
            orphan_strategy_mgr
                .borrow_mut()
                .apply_trade_update(&normalized_update)
        } else {
            orphan_strategy_mgr
                .borrow_mut()
                .apply_order_update(&normalized_update)
        };

        if !adopted_by_orphan {
            PersistChannel::with(|ch| {
                if normalized_update.execution_type() == ExecutionType::Trade {
                    ch.publish_trade_update_unmatched(&normalized_update);
                } else {
                    ch.publish_order_update_unmatched(&normalized_update);
                }
            });
        }
        debug!(
            "order update unmatched: sym={} cli_id={} ord_id={} x={:?} X={:?} orphan_adopted={}",
            OrderUpdate::symbol(&normalized_update),
            OrderUpdate::client_order_id(&normalized_update),
            OrderUpdate::order_id(&normalized_update),
            normalized_update.execution_type(),
            normalized_update.status(),
            adopted_by_orphan
        );
    }
}

fn dispatch_binance_external_order_update(update: &BinanceBasicOrderMsg) {
    use order_common::{OrderUpdate, TradeUpdate};

    let Some(reason) = update.external_order_label() else {
        return;
    };
    let execution_type = OrderUpdate::execution_type(update);
    let venue = OrderUpdate::trading_venue(update);
    let price = if update.last_executed_price.is_finite() && update.last_executed_price > 0.0 {
        update.last_executed_price
    } else {
        update.average_price
    };

    if let Some(record) = build_binance_external_uniform_order(update) {
        MonitorChannel::instance().bump_trade_update_seq();
        PersistChannel::with(|channel| {
            channel.publish_trade_update_unmatched(update);
            channel.publish_uniform_order(&record);
        });
        warn!(
            "Binance exchange forced-close fill persisted: reason={} venue={:?} symbol={} order_id={} trade_id={} side={} price={:.8} qty={:.8}",
            reason,
            venue,
            update.symbol,
            update.order_id,
            update.trade_id,
            TradeUpdate::side(update).as_str(),
            price,
            update.last_executed_quantity
        );
    } else {
        PersistChannel::with(|channel| channel.publish_order_update_unmatched(update));
        debug!(
            "Binance exchange forced-close non-fill persisted as unmatched order: reason={} venue={:?} symbol={} order_id={} x={:?} X={:?}",
            reason,
            venue,
            update.symbol,
            update.order_id,
            execution_type,
            OrderUpdate::status(update)
        );
    }
}

fn build_binance_external_uniform_order(
    update: &BinanceBasicOrderMsg,
) -> Option<UnifiedOrderRecord> {
    use order_common::OrderUpdate;

    let reason = update.external_order_label()?;
    let price = if update.last_executed_price.is_finite() && update.last_executed_price > 0.0 {
        update.last_executed_price
    } else {
        update.average_price
    };
    if OrderUpdate::execution_type(update) != ExecutionType::Trade
        || !update.last_executed_quantity.is_finite()
        || update.last_executed_quantity <= 0.0
        || !price.is_finite()
        || price <= 0.0
    {
        return None;
    }

    let event_ts = OrderUpdate::event_time(update);
    let mut record = UnifiedOrderRecord {
        symbol_len: 0,
        symbol: update.symbol.as_bytes().to_vec(),
        create_ts: event_ts,
        update_ts: event_ts,
        signal_ts: 0,
        submit_ts: 0,
        local_ts: get_timestamp_us(),
        mkt_ts: 0,
        client_order_id: update.client_order_id,
        venue: OrderUpdate::trading_venue(update) as u8,
        ttype: update.order_type,
        side: update.side,
        price,
        price_offset: 0.0,
        amount_init: update.quantity,
        amount_update: update.last_executed_quantity,
        status: update.order_status,
        from_key_len: 0,
        from_key: format!(
            "exchange_forced_close:{reason}:order={}:trade={}",
            update.order_id, update.trade_id
        )
        .into_bytes(),
        signal_bbo: None,
    };
    record.refresh_lengths();
    Some(record)
}

fn build_hyperliquid_external_uniform_order(
    update: &HyperliquidBasicFillMsg,
) -> Option<UnifiedOrderRecord> {
    use order_common::{OrderType, Side, TradeUpdate};

    if update.client_order_id != 0
        || TradeUpdate::trading_venue(update) != TradingVenue::HyperliquidFutures
        || update.order_id <= 0
        || !update.price.is_finite()
        || update.price <= 0.0
        || !update.last_filled_quantity.is_finite()
        || update.last_filled_quantity <= 0.0
    {
        return None;
    }
    let method = update.liquidation_method.trim();
    if method.is_empty()
        || !method
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return None;
    }
    let side = Side::from_u8(update.side)?;
    let synthetic_client_order_id = update.order_id.checked_neg()?;
    let event_ts = TradeUpdate::event_time(update);
    let mut record = UnifiedOrderRecord {
        symbol_len: 0,
        symbol: update.symbol.as_bytes().to_vec(),
        create_ts: event_ts,
        update_ts: event_ts,
        signal_ts: 0,
        submit_ts: 0,
        local_ts: get_timestamp_us(),
        mkt_ts: 0,
        client_order_id: synthetic_client_order_id,
        venue: TradingVenue::HyperliquidFutures.to_u8(),
        ttype: OrderType::Market.to_u8(),
        side: side.to_u8(),
        price: update.price,
        price_offset: 0.0,
        amount_init: update.last_filled_quantity,
        amount_update: update.last_filled_quantity,
        status: OrderStatus::Filled.to_u8(),
        from_key_len: 0,
        from_key: format!(
            "exchange_forced_close:liquidation:method={method}:order={}:trade={}:tx={}",
            update.order_id, update.venue_trade_id, update.transaction_hash
        )
        .into_bytes(),
        signal_bbo: None,
    };
    record.refresh_lengths();
    Some(record)
}

fn dispatch_order_update_to_strategy<T>(
    strategy_mgr: &Rc<RefCell<crate::strategy::StrategyManager>>,
    strategy_id: i32,
    normalized_update: &NormalizedUpdate<'_, T>,
) -> bool
where
    T: OrderUpdate + TradeUpdate,
{
    let order_id = OrderUpdate::client_order_id(normalized_update);
    let strategy_opt = {
        let mut mgr = strategy_mgr.borrow_mut();
        if mgr.contains(strategy_id) {
            mgr.take(strategy_id)
        } else {
            None
        }
    };

    let Some(mut strategy) = strategy_opt else {
        return false;
    };

    let matched = strategy.is_strategy_order(order_id);
    if matched {
        match normalized_update.execution_type() {
            ExecutionType::New | ExecutionType::Canceled => {
                strategy.apply_order_update(normalized_update);
            }
            ExecutionType::Trade => {
                strategy.apply_trade_update(normalized_update);
            }
            ExecutionType::Expired | ExecutionType::Rejected | ExecutionType::TradePrevention => {
                warn!(
                    "Unexpected execution type: {:?}, sym={} cli_id={} ord_id={}",
                    normalized_update.execution_type(),
                    OrderUpdate::symbol(normalized_update),
                    OrderUpdate::client_order_id(normalized_update),
                    OrderUpdate::order_id(normalized_update)
                );
                strategy.apply_order_update(normalized_update);
            }
            _ => {
                log::error!(
                    "Unhandled execution type: {:?}, sym={} cli_id={} ord_id={}",
                    normalized_update.execution_type(),
                    OrderUpdate::symbol(normalized_update),
                    OrderUpdate::client_order_id(normalized_update),
                    OrderUpdate::order_id(normalized_update)
                );
            }
        }
    }
    if strategy.is_active() {
        strategy_mgr.borrow_mut().insert(strategy);
    }
    matched
}

fn dispatch_order_update_fallback_scan<T>(
    strategy_mgr: &Rc<RefCell<crate::strategy::StrategyManager>>,
    normalized_update: &NormalizedUpdate<'_, T>,
) -> bool
where
    T: OrderUpdate + TradeUpdate,
{
    let strategy_ids: Vec<i32> = strategy_mgr.borrow().iter_ids().cloned().collect();
    for strategy_id in strategy_ids {
        if dispatch_order_update_to_strategy(strategy_mgr, strategy_id, normalized_update) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::min_qty_table::MinQtyTable;
    use crate::pre_trade::price_table::PriceTable;
    use crate::pre_trade::usdt_balance_manager::UsdtBalanceManager;
    use crate::strategy::manager::OpenPriceMapEntry;
    use crate::strategy::{Strategy, StrategyManager};
    use mkt_parsers::msg::basic_account_msg::{
        BasicAccountEventMsg, BasicAccountScope, BasicBalanceMsg, BasicPositionMsg,
        BinanceBasicOrderMsg, GateBasicOrderMsg,
    };
    use signal_common::cancel_signal::{ArbCancelCtx, MmCancelCtx};
    use signal_common::min_qty_table::MinQtyEntry;
    use signal_common::min_qty_table::MinQtyEntry as VenueMinQtyEntry;
    use signal_common::tick_math::QuantizedValue;
    use signal_common::trade_signal::{SignalType, TradeSignal};
    use std::any::Any;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;

    fn hyperliquid_test_order_update(
        venue: TradingVenue,
        client_order_id: i64,
    ) -> HyperliquidBasicOrderMsg {
        HyperliquidBasicOrderMsg::create(
            venue.to_u8(),
            1_725_000_000_123,
            "BTCUSDC".to_string(),
            99,
            client_order_id,
            "0x0000000000000000000000000000002a".to_string(),
            Side::Buy.to_u8(),
            order_common::OrderType::Limit.to_u8(),
            order_common::TimeInForce::GTC.to_u8(),
            ExecutionType::Canceled.to_u8(),
            OrderStatus::Canceled.to_u8(),
            100.0,
            1.0,
            0.0,
            "canceled".to_string(),
        )
    }

    fn hyperliquid_test_local_order(
        venue: TradingVenue,
        client_order_id: i64,
        order_type: order_common::OrderType,
    ) -> Order {
        Order::new(
            venue,
            client_order_id,
            order_type,
            "BTCUSDC".to_string(),
            Side::Buy,
            1.0,
            100.0,
            false,
            1.0,
            None,
            false,
        )
    }

    #[test]
    fn hyperliquid_private_update_uses_local_order_intent() {
        for venue in [
            TradingVenue::HyperliquidMargin,
            TradingVenue::HyperliquidFutures,
        ] {
            for (order_type, expected_tif) in [
                (
                    order_common::OrderType::Limit,
                    order_common::TimeInForce::GTX,
                ),
                (
                    order_common::OrderType::Market,
                    order_common::TimeInForce::IOC,
                ),
            ] {
                let mut update = hyperliquid_test_order_update(venue, 42);
                let order = hyperliquid_test_local_order(venue, 42, order_type);

                assert!(override_hyperliquid_order_intent_from_local(
                    &mut update,
                    Some(&order)
                ));
                assert_eq!(update.order_type, order_type.to_u8());
                assert_eq!(update.time_in_force, expected_tif.to_u8());
            }
        }
    }

    #[test]
    fn hyperliquid_terminal_without_local_order_is_not_guessed() {
        let mut update = hyperliquid_test_order_update(TradingVenue::HyperliquidFutures, 42);
        let original_order_type = update.order_type;
        let original_tif = update.time_in_force;

        assert!(!override_hyperliquid_order_intent_from_local(
            &mut update,
            None
        ));
        assert_eq!(update.order_type, original_order_type);
        assert_eq!(update.time_in_force, original_tif);
    }

    #[test]
    fn only_hyperliquid_account_service_requires_non_overflow() {
        assert!(account_service_requires_non_overflow(Exchange::Hyperliquid));
        assert!(!account_service_requires_non_overflow(Exchange::Binance));
        assert!(!account_service_requires_non_overflow(Exchange::Gate));
    }

    fn temporary_fact_cursor_path(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir()
            .join(format!(
                "mkt_signal_hyperliquid_cursor_test_{}_{}_{}",
                std::process::id(),
                label,
                nonce
            ))
            .join("cursor.bin")
    }

    #[test]
    fn hyperliquid_fact_cursor_persists_atomically_and_is_identity_bound() {
        let account_hash = [31; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN];
        let path = temporary_fact_cursor_path("roundtrip");
        let parent = path.parent().unwrap().to_path_buf();
        let store = HyperliquidFactCursorStore::at_path(path.clone(), account_hash);
        assert_eq!(store.load().unwrap(), HyperliquidFactCursor::default());

        let cursor = HyperliquidFactCursor {
            monitor_id: 901,
            last_fact_seq: 73,
        };
        store.persist(cursor).unwrap();
        assert_eq!(store.load().unwrap(), cursor);
        assert_eq!(fs::read_dir(&parent).unwrap().count(), 1);

        let wrong_identity =
            HyperliquidFactCursorStore::at_path(path, [32; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN]);
        assert!(wrong_identity.load().is_err());
        store.clear().unwrap();
        assert_eq!(store.load().unwrap(), HyperliquidFactCursor::default());
        fs::remove_dir_all(parent).unwrap();
    }

    #[test]
    fn hyperliquid_portfolio_readiness_requires_both_fresh_snapshots_and_risk() {
        let mode = Some(HyperliquidAccountMode::PortfolioMargin);
        let mut readiness = HyperliquidLiveSnapshotReadiness::default();
        for venue in [
            TradingVenue::HyperliquidMargin,
            TradingVenue::HyperliquidFutures,
        ] {
            for phase in [
                HyperliquidSnapshotPhase::Begin,
                HyperliquidSnapshotPhase::Complete,
            ] {
                let control = HyperliquidSnapshotCompleteMsg::create_control(
                    phase,
                    HyperliquidSnapshotPath::Primary,
                    venue.to_u8(),
                    7,
                    11,
                    1,
                    900,
                    2000,
                );
                readiness.apply_control(venue, &control, 1000).unwrap();
            }
            if venue == TradingVenue::HyperliquidMargin {
                assert_eq!(readiness.exec_ready_until_ms(mode, venue, 1000, true), None);
            }
        }
        for venue in [
            TradingVenue::HyperliquidMargin,
            TradingVenue::HyperliquidFutures,
        ] {
            assert_eq!(
                readiness.exec_ready_until_ms(mode, venue, 1000, false),
                None
            );
            assert_eq!(readiness.arb_ready_until_ms(mode, venue, 1000, false), None);
            assert_eq!(
                readiness.exec_ready_until_ms(mode, venue, 1000, true),
                Some(2000)
            );
            assert_eq!(
                readiness.arb_ready_until_ms(mode, venue, 1000, true),
                Some(2000)
            );
            assert_eq!(readiness.arb_ready_until_ms(mode, venue, 2000, true), None);
        }
        let begin = HyperliquidSnapshotCompleteMsg::create_control(
            HyperliquidSnapshotPhase::Begin,
            HyperliquidSnapshotPath::Secondary,
            TradingVenue::HyperliquidFutures.to_u8(),
            7,
            12,
            2,
            1100,
            2500,
        );
        readiness
            .apply_control(TradingVenue::HyperliquidFutures, &begin, 1100)
            .unwrap();
        assert_eq!(
            readiness.exec_ready_until_ms(mode, TradingVenue::HyperliquidFutures, 1100, true),
            None
        );
        assert!(!readiness.is_snapshot_row(
            BasicAccountEventType::AccountRisk,
            BasicAccountScope::HyperliquidPortfolioMargin,
            mode
        ));
        assert!(!hyperliquid_snapshot_owns_risk(
            mode,
            TradingVenue::HyperliquidFutures
        ));
        assert!(hyperliquid_snapshot_owns_risk(
            mode,
            TradingVenue::HyperliquidMargin
        ));
        let mut complete = begin.clone();
        complete.phase = HyperliquidSnapshotPhase::Complete as u8;
        readiness
            .apply_control(TradingVenue::HyperliquidFutures, &complete, 1100)
            .unwrap();
        assert_eq!(
            readiness.exec_ready_until_ms(mode, TradingVenue::HyperliquidFutures, 1100, true),
            Some(2000)
        );
        readiness.spot.fail_closed();
        assert_eq!(
            readiness.exec_ready_until_ms(mode, TradingVenue::HyperliquidFutures, 1100, true),
            None
        );
    }

    #[test]
    fn hyperliquid_portfolio_ipc_risk_rejects_invalid_ratio() {
        for value in [f64::NAN, f64::INFINITY, -0.1] {
            let msg = BasicAccountRiskMsg::create(1000, 0.0, 0.0, 0.0, 0.0, value, 0.0, 0.0);
            assert!(decode_hyperliquid_portfolio_risk(&msg.to_bytes()).is_err());
        }
        assert!(decode_hyperliquid_portfolio_risk(&[0; 2]).is_err());
        let msg = BasicAccountRiskMsg::create(1000, 0.0, 0.0, 0.0, 0.0, 2.0, 0.0, 0.0);
        assert_eq!(
            decode_hyperliquid_portfolio_risk(&msg.to_bytes())
                .unwrap()
                .margin_ratio,
            2.0
        );
    }

    #[test]
    fn hyperliquid_native_fact_ack_binds_exact_payload_and_identity() {
        use mkt_parsers::msg::hyperliquid_native_msg::HyperliquidNativeSource;
        let identity = HyperliquidFactIdentity {
            account_hash: [41; 32],
            monitor_id: 52,
            fact_seq: 63,
        };
        let msg = HyperliquidNativeEventMsg::create(
            1000,
            HyperliquidNativeSource::NonUserCancel,
            "BTC:7".into(),
            &serde_json::json!({"coin":"BTC","oid":7}),
        )
        .unwrap()
        .with_fact_identity(identity);
        let encode = |msg: &HyperliquidNativeEventMsg| {
            BasicAccountEventMsg::create(
                BasicAccountEventType::HyperliquidNativeEvent,
                BasicAccountScope::HyperliquidUnified,
                msg.to_bytes(),
            )
            .to_bytes()
        };
        let envelope = encode(&msg);
        let metadata = hyperliquid_audit_fact_metadata(&envelope).unwrap().unwrap();
        let ack = HyperliquidAccountFactAck {
            account_hash: identity.account_hash,
            monitor_id: 52,
            fact_seq: 63,
            stable_key: metadata.stable_key,
            value_digest: hyperliquid_account_fact_value_digest(&metadata.stable_key, &envelope),
        };
        assert!(hyperliquid_fact_ack_matches_payload(&ack, &envelope).unwrap());
        let mut changed = msg.clone();
        changed.payload_json = r#"{"coin":"ETH","oid":7}"#.into();
        assert!(!hyperliquid_fact_ack_matches_payload(&ack, &encode(&changed)).unwrap());
        changed = msg;
        changed.identity.fact_seq += 1;
        assert!(!hyperliquid_fact_ack_matches_payload(&ack, &encode(&changed)).unwrap());
    }

    #[test]
    fn hyperliquid_fact_ack_must_match_identity_stable_key_and_exact_value() {
        let identity = HyperliquidFactIdentity {
            account_hash: [41; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
            monitor_id: 52,
            fact_seq: 63,
        };
        let fact = HyperliquidFundingMsg::create(
            1_725_000_000_000,
            "BTC".to_string(),
            "-1.25".to_string(),
            "2.0".to_string(),
            "0.0001".to_string(),
        )
        .with_fact_identity(identity);
        let envelope = BasicAccountEventMsg::create(
            BasicAccountEventType::HyperliquidFunding,
            BasicAccountScope::HyperliquidUnified,
            fact.to_bytes(),
        )
        .to_bytes();
        let metadata = hyperliquid_audit_fact_metadata(&envelope).unwrap().unwrap();
        let ack = HyperliquidAccountFactAck {
            account_hash: identity.account_hash,
            monitor_id: identity.monitor_id,
            fact_seq: identity.fact_seq,
            stable_key: metadata.stable_key,
            value_digest: hyperliquid_account_fact_value_digest(&metadata.stable_key, &envelope),
        };
        let mut padded = envelope.to_vec();
        padded.resize(HYPERLIQUID_ACCOUNT_FACT_MAX_BYTES, 0);
        assert!(hyperliquid_fact_ack_matches_payload(&ack, &padded).unwrap());

        let mut wrong_key = ack;
        wrong_key.stable_key[0] ^= 1;
        assert!(!hyperliquid_fact_ack_matches_payload(&wrong_key, &padded).unwrap());
        let mut wrong_value = ack;
        wrong_value.value_digest[0] ^= 1;
        assert!(!hyperliquid_fact_ack_matches_payload(&wrong_value, &padded).unwrap());
    }

    #[test]
    fn pending_hyperliquid_fact_marks_post_ack_replay_once() {
        let mut pending = PendingHyperliquidFactBatch::new(Vec::new(), true);
        assert!(pending.require_recovery());
        assert!(!pending.require_recovery());
        assert!(pending.recovery_required);
    }

    #[test]
    fn hyperliquid_fact_restart_resumes_cursor_and_explicit_epoch_gap_resets_once() {
        let account_hash = [33; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN];
        let cursor = HyperliquidFactCursor {
            monitor_id: 910,
            last_fact_seq: 44,
        };
        let mut protocol =
            HyperliquidFactReplayProtocol::new_with_cursor(account_hash, 920, cursor);
        let resumed = protocol.begin_request();
        assert_eq!(resumed.last_monitor_id, 910);
        assert_eq!(resumed.last_fact_seq, 44);

        let changed_epoch_gap = HyperliquidFactReplayControlMsg::create(
            HyperliquidFactReplayPhase::Gap,
            account_hash,
            911,
            920,
            resumed.request_id,
            1,
            0,
            0,
        );
        assert!(matches!(
            protocol.observe_control(changed_epoch_gap),
            HyperliquidFactControlDisposition::ResetProducerEpoch
        ));
        assert!(protocol.reset_producer_epoch());
        let from_origin = protocol.begin_request();
        assert_eq!(from_origin.last_monitor_id, 0);
        assert_eq!(from_origin.last_fact_seq, 0);

        let origin_not_retained = HyperliquidFactReplayControlMsg::create(
            HyperliquidFactReplayPhase::Gap,
            account_hash,
            911,
            920,
            from_origin.request_id,
            2,
            44,
            44,
        );
        assert!(matches!(
            protocol.observe_control(origin_not_retained),
            HyperliquidFactControlDisposition::FailClosed(
                "producer replay ring cannot cover requested cursor"
            )
        ));
        assert!(protocol.is_gap());
    }

    #[test]
    fn hyperliquid_fact_replay_commits_only_after_complete_then_enforces_live_sequence() {
        let account_hash = [9; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN];
        let mut protocol = HyperliquidFactReplayProtocol::new(account_hash, 71);
        let request = protocol.begin_request();
        let begin = HyperliquidFactReplayControlMsg::create(
            HyperliquidFactReplayPhase::Begin,
            account_hash,
            81,
            71,
            request.request_id,
            1,
            2,
            2,
        );
        assert!(matches!(
            protocol.observe_control(begin),
            HyperliquidFactControlDisposition::Waiting
        ));
        for (seq, payload) in [(1, b"fact-1".as_slice()), (2, b"fact-2".as_slice())] {
            assert!(matches!(
                protocol.observe_fact(
                    HyperliquidFactIdentity {
                        account_hash,
                        monitor_id: 81,
                        fact_seq: seq,
                    },
                    payload,
                ),
                HyperliquidFactDisposition::Drop
            ));
            assert!(!protocol.is_ready());
        }
        let complete = HyperliquidFactReplayControlMsg::create(
            HyperliquidFactReplayPhase::Complete,
            account_hash,
            81,
            71,
            request.request_id,
            1,
            2,
            2,
        );
        let HyperliquidFactControlDisposition::Commit { events, caught_up } =
            protocol.observe_control(complete)
        else {
            panic!("expected a validated replay commit");
        };
        assert!(caught_up);
        assert_eq!(
            events,
            vec![Bytes::from_static(b"fact-1"), Bytes::from_static(b"fact-2")]
        );
        assert!(!protocol.is_ready());
        assert_eq!(protocol.monitor_id, 0);
        assert_eq!(protocol.last_fact_seq, 0);
        assert!(protocol.complete_commit());
        assert!(protocol.is_ready());
        assert_eq!(protocol.monitor_id, 81);
        assert_eq!(protocol.last_fact_seq, 2);

        let live = HyperliquidFactIdentity {
            account_hash,
            monitor_id: 81,
            fact_seq: 3,
        };
        assert!(matches!(
            protocol.observe_fact(live, b"live"),
            HyperliquidFactDisposition::Apply
        ));
        assert_eq!(protocol.last_fact_seq, 2);
        assert!(protocol.complete_commit());
        assert_eq!(protocol.last_fact_seq, 3);
        assert!(matches!(
            protocol.observe_fact(live, b"duplicate"),
            HyperliquidFactDisposition::Drop
        ));
        assert!(matches!(
            protocol.observe_fact(
                HyperliquidFactIdentity {
                    fact_seq: 5,
                    ..live
                },
                b"gap",
            ),
            HyperliquidFactDisposition::Recover("live factual sequence gap")
        ));
    }

    #[test]
    fn hyperliquid_fact_replay_rejects_account_mismatch_and_cross_epoch_resume() {
        let account_hash = [4; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN];
        let mut protocol = HyperliquidFactReplayProtocol::new(account_hash, 72);
        let request = protocol.begin_request();
        let begin = HyperliquidFactReplayControlMsg::create(
            HyperliquidFactReplayPhase::Begin,
            account_hash,
            82,
            72,
            request.request_id,
            1,
            0,
            0,
        );
        assert!(matches!(
            protocol.observe_control(begin),
            HyperliquidFactControlDisposition::Waiting
        ));
        let complete = HyperliquidFactReplayControlMsg::create(
            HyperliquidFactReplayPhase::Complete,
            account_hash,
            82,
            72,
            request.request_id,
            1,
            0,
            0,
        );
        assert!(matches!(
            protocol.observe_control(complete),
            HyperliquidFactControlDisposition::Commit { events, caught_up: true }
                if events.is_empty()
        ));
        assert!(protocol.complete_commit());

        let request = protocol.begin_request();
        let new_epoch = HyperliquidFactReplayControlMsg::create(
            HyperliquidFactReplayPhase::Begin,
            account_hash,
            83,
            72,
            request.request_id,
            1,
            0,
            0,
        );
        assert!(matches!(
            protocol.observe_control(new_epoch),
            HyperliquidFactControlDisposition::FailClosed(
                "producer epoch changed after a committed factual cursor"
            )
        ));
        assert!(protocol.is_gap());

        let mut protocol = HyperliquidFactReplayProtocol::new(account_hash, 73);
        let request = protocol.begin_request();
        let wrong_account = HyperliquidFactReplayControlMsg::create(
            HyperliquidFactReplayPhase::Gap,
            [5; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN],
            84,
            73,
            request.request_id,
            1,
            9,
            9,
        );
        assert!(matches!(
            protocol.observe_control(wrong_account),
            HyperliquidFactControlDisposition::FailClosed(
                "replay control account identity mismatch"
            )
        ));
        assert!(protocol.is_gap());
    }

    #[test]
    fn hyperliquid_fact_replay_partial_transaction_advances_cursor_without_readiness() {
        let account_hash = [8; HYPERLIQUID_ACCOUNT_IDENTITY_HASH_LEN];
        let mut protocol = HyperliquidFactReplayProtocol::new(account_hash, 74);
        let request = protocol.begin_request();
        let begin = HyperliquidFactReplayControlMsg::create(
            HyperliquidFactReplayPhase::Begin,
            account_hash,
            85,
            74,
            request.request_id,
            1,
            2,
            5,
        );
        assert!(matches!(
            protocol.observe_control(begin),
            HyperliquidFactControlDisposition::Waiting
        ));
        for seq in 1..=2 {
            assert!(matches!(
                protocol.observe_fact(
                    HyperliquidFactIdentity {
                        account_hash,
                        monitor_id: 85,
                        fact_seq: seq,
                    },
                    &[seq as u8],
                ),
                HyperliquidFactDisposition::Drop
            ));
        }
        let complete = HyperliquidFactReplayControlMsg::create(
            HyperliquidFactReplayPhase::Complete,
            account_hash,
            85,
            74,
            request.request_id,
            1,
            2,
            5,
        );
        assert!(matches!(
            protocol.observe_control(complete),
            HyperliquidFactControlDisposition::Commit {
                events,
                caught_up: false
            } if events.len() == 2
        ));
        assert!(!protocol.is_ready());
        assert_eq!(protocol.monitor_id, 0);
        assert_eq!(protocol.last_fact_seq, 0);
        assert!(protocol.complete_commit());
        assert!(!protocol.is_ready());
        let next = protocol.begin_request();
        assert_eq!(next.last_monitor_id, 85);
        assert_eq!(next.last_fact_seq, 2);
    }

    #[test]
    fn hyperliquid_snapshot_controls_require_matched_batches_and_fresh_scopes() {
        fn control(
            phase: HyperliquidSnapshotPhase,
            path: HyperliquidSnapshotPath,
            venue: TradingVenue,
            batch_id: u64,
            valid_until: i64,
        ) -> HyperliquidSnapshotCompleteMsg {
            HyperliquidSnapshotCompleteMsg::create_control(
                phase,
                path,
                venue.to_u8(),
                7,
                11,
                batch_id,
                900,
                valid_until,
            )
        }

        let mut readiness = HyperliquidLiveSnapshotReadiness::default();
        let spot = readiness
            .stream_mut(TradingVenue::HyperliquidMargin)
            .unwrap();
        spot.apply_control(
            &control(
                HyperliquidSnapshotPhase::Begin,
                HyperliquidSnapshotPath::Primary,
                TradingVenue::HyperliquidMargin,
                1,
                2_000,
            ),
            1_000,
        )
        .unwrap();
        assert_eq!(spot.ready_until_ms(1_000), None);
        assert!(spot
            .apply_control(
                &control(
                    HyperliquidSnapshotPhase::Complete,
                    HyperliquidSnapshotPath::Primary,
                    TradingVenue::HyperliquidMargin,
                    2,
                    2_000,
                ),
                1_000,
            )
            .is_err());
        spot.apply_control(
            &control(
                HyperliquidSnapshotPhase::Complete,
                HyperliquidSnapshotPath::Primary,
                TradingVenue::HyperliquidMargin,
                1,
                2_000,
            ),
            1_000,
        )
        .unwrap();
        assert_eq!(spot.ready_until_ms(1_000), Some(2_000));
        assert_eq!(spot.ready_until_ms(2_000), None);

        assert_eq!(
            readiness.exec_ready_until_ms(
                Some(HyperliquidAccountMode::Standard),
                TradingVenue::HyperliquidMargin,
                1_000,
                false,
            ),
            Some(2_000)
        );
        assert_eq!(
            readiness.exec_ready_until_ms(
                Some(HyperliquidAccountMode::Unified),
                TradingVenue::HyperliquidMargin,
                1_000,
                true,
            ),
            None
        );
    }

    #[test]
    fn hyperliquid_begin_invalidates_every_preexisting_path_lease() {
        let mut stream = HyperliquidStreamSnapshotState::default();
        let make = |phase, path, batch_id, valid_until| {
            HyperliquidSnapshotCompleteMsg::create_control(
                phase,
                path,
                TradingVenue::HyperliquidFutures.to_u8(),
                9,
                3,
                batch_id,
                1_000,
                valid_until,
            )
        };
        for phase in [
            HyperliquidSnapshotPhase::Begin,
            HyperliquidSnapshotPhase::Complete,
        ] {
            stream
                .apply_control(
                    &make(phase, HyperliquidSnapshotPath::Secondary, 1, 3_000),
                    1_500,
                )
                .unwrap();
        }
        assert_eq!(stream.ready_until_ms(1_500), Some(3_000));

        stream
            .apply_control(
                &make(
                    HyperliquidSnapshotPhase::Begin,
                    HyperliquidSnapshotPath::Primary,
                    1,
                    4_000,
                ),
                1_500,
            )
            .unwrap();
        assert_eq!(stream.ready_until_ms(1_500), None);
        stream
            .apply_control(
                &make(
                    HyperliquidSnapshotPhase::Invalidate,
                    HyperliquidSnapshotPath::Primary,
                    0,
                    4_000,
                ),
                1_500,
            )
            .unwrap();
        assert_eq!(stream.ready_until_ms(1_500), None);

        for phase in [
            HyperliquidSnapshotPhase::Begin,
            HyperliquidSnapshotPhase::Complete,
        ] {
            stream
                .apply_control(
                    &make(phase, HyperliquidSnapshotPath::Secondary, 2, 5_000),
                    1_500,
                )
                .unwrap();
        }
        assert_eq!(stream.ready_until_ms(1_500), Some(5_000));
    }

    #[test]
    fn hyperliquid_state_rows_require_an_active_scope_matched_transaction() {
        let mut readiness = HyperliquidLiveSnapshotReadiness::default();
        assert!(!readiness.is_snapshot_row(
            BasicAccountEventType::BalanceUpdate,
            BasicAccountScope::HyperliquidUnified,
            Some(HyperliquidAccountMode::Unified),
        ));
        let begin = HyperliquidSnapshotCompleteMsg::create_control(
            HyperliquidSnapshotPhase::Begin,
            HyperliquidSnapshotPath::Primary,
            TradingVenue::HyperliquidMargin.to_u8(),
            41,
            1,
            1,
            1_000,
            2_000,
        );
        readiness
            .apply_control(TradingVenue::HyperliquidMargin, &begin, 1_100)
            .unwrap();
        assert!(readiness.is_snapshot_row(
            BasicAccountEventType::BalanceUpdate,
            BasicAccountScope::HyperliquidUnified,
            Some(HyperliquidAccountMode::Unified),
        ));
        assert!(readiness.is_snapshot_row(
            BasicAccountEventType::HyperliquidSpotBalance,
            BasicAccountScope::HyperliquidUnified,
            Some(HyperliquidAccountMode::Unified),
        ));
        assert!(!readiness.is_snapshot_row(
            BasicAccountEventType::BalanceUpdate,
            BasicAccountScope::HyperliquidStdSpot,
            Some(HyperliquidAccountMode::Unified),
        ));
        assert!(!readiness.is_snapshot_row(
            BasicAccountEventType::PositionUpdate,
            BasicAccountScope::HyperliquidUnified,
            Some(HyperliquidAccountMode::Unified),
        ));
    }

    #[test]
    fn malformed_hyperliquid_perp_dex_row_invalidates_snapshot_batch() {
        let mut readiness = HyperliquidLiveSnapshotReadiness::default();
        let control = |phase| {
            HyperliquidSnapshotCompleteMsg::create_control(
                phase,
                HyperliquidSnapshotPath::Primary,
                TradingVenue::HyperliquidFutures.to_u8(),
                41,
                1,
                1,
                1_000,
                2_000,
            )
        };
        readiness
            .apply_control(
                TradingVenue::HyperliquidFutures,
                &control(HyperliquidSnapshotPhase::Begin),
                1_100,
            )
            .unwrap();
        assert!(readiness.is_snapshot_row(
            BasicAccountEventType::HyperliquidPerpDexState,
            BasicAccountScope::HyperliquidUnified,
            Some(HyperliquidAccountMode::Unified),
        ));

        let valid = HyperliquidPerpDexStateMsg::create(
            1_000,
            "xyz".to_string(),
            2,
            "100.000".to_string(),
            "25.00".to_string(),
            "75.000".to_string(),
            "2.500".to_string(),
            "90.000".to_string(),
            "25.00".to_string(),
            "65.000".to_string(),
            "2.500".to_string(),
            "1.250".to_string(),
            "70.125".to_string(),
        )
        .to_bytes();
        assert!(readiness.validate_perp_dex_state_row(&valid).is_ok());

        let malformed = &valid[..valid.len() - 1];
        assert!(readiness.validate_perp_dex_state_row(malformed).is_err());
        assert!(!readiness.is_snapshot_row(
            BasicAccountEventType::HyperliquidPerpDexState,
            BasicAccountScope::HyperliquidUnified,
            Some(HyperliquidAccountMode::Unified),
        ));
        assert!(readiness
            .apply_control(
                TradingVenue::HyperliquidFutures,
                &control(HyperliquidSnapshotPhase::Complete),
                1_100,
            )
            .is_err());
        assert_eq!(readiness.perp.ready_until_ms(1_100), None);
    }

    #[test]
    fn new_monitor_identity_revokes_all_old_path_completions() {
        let mut readiness = HyperliquidLiveSnapshotReadiness::default();
        for venue in [
            TradingVenue::HyperliquidMargin,
            TradingVenue::HyperliquidFutures,
        ] {
            for phase in [
                HyperliquidSnapshotPhase::Begin,
                HyperliquidSnapshotPhase::Complete,
            ] {
                readiness
                    .apply_control(
                        venue,
                        &HyperliquidSnapshotCompleteMsg::create_control(
                            phase,
                            HyperliquidSnapshotPath::Primary,
                            venue.to_u8(),
                            10,
                            1,
                            1,
                            1_000,
                            3_000,
                        ),
                        1_500,
                    )
                    .unwrap();
            }
        }
        assert_eq!(readiness.spot.ready_until_ms(1_500), Some(3_000));
        assert_eq!(readiness.perp.ready_until_ms(1_500), Some(3_000));

        readiness
            .apply_control(
                TradingVenue::HyperliquidMargin,
                &HyperliquidSnapshotCompleteMsg::create_control(
                    HyperliquidSnapshotPhase::Invalidate,
                    HyperliquidSnapshotPath::Secondary,
                    TradingVenue::HyperliquidMargin.to_u8(),
                    11,
                    1,
                    0,
                    1_600,
                    3_000,
                ),
                1_700,
            )
            .unwrap();
        assert_eq!(readiness.spot.ready_until_ms(1_700), None);
        assert_eq!(readiness.perp.ready_until_ms(1_700), None);
    }

    #[test]
    fn binance_total_equity_override_requires_shared_account_scope() {
        assert_eq!(
            exchange_scoped_total_equity_scope(
                TradingVenue::BinanceMargin,
                TradingVenue::BinanceFutures,
                Some(BinanceAccountMode::Unified),
                None,
            ),
            Some(BasicAccountScope::BinanceUnified)
        );
        assert_eq!(
            exchange_scoped_total_equity_scope(
                TradingVenue::BinanceMargin,
                TradingVenue::BinanceFutures,
                Some(BinanceAccountMode::Standard),
                None,
            ),
            None
        );
        assert_eq!(
            exchange_scoped_total_equity_scope(
                TradingVenue::BinanceFutures,
                TradingVenue::BinanceFutures,
                Some(BinanceAccountMode::Standard),
                None,
            ),
            Some(BasicAccountScope::BinanceStdUm)
        );
        assert_eq!(
            exchange_scoped_total_equity_scope(
                TradingVenue::BinanceMargin,
                TradingVenue::BinanceCoinFutures,
                Some(BinanceAccountMode::Unified),
                None,
            ),
            None
        );

        assert_eq!(
            exchange_scoped_total_equity_scope(
                TradingVenue::HyperliquidMargin,
                TradingVenue::HyperliquidFutures,
                None,
                Some(HyperliquidAccountMode::Unified),
            ),
            Some(BasicAccountScope::HyperliquidUnified)
        );
        assert_eq!(
            exchange_scoped_total_equity_scope(
                TradingVenue::HyperliquidMargin,
                TradingVenue::HyperliquidFutures,
                None,
                Some(HyperliquidAccountMode::Standard),
            ),
            None
        );
    }

    #[test]
    fn binance_standard_exec_uses_exchange_valued_multi_asset_equity() {
        let open_leg = LegMgr::Futures {
            exchange: Exchange::Binance,
            um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Binance))),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Binance))),
        };
        let hedge_leg = open_leg.clone();

        let mut usdt_mgr = UsdtBalanceManager::new(Exchange::Binance);
        usdt_mgr.apply_balance(&BasicBalanceMsg::create(0, "USDT".to_string(), 10_000.0));
        let usdt_mgrs = HashMap::from([(
            BasicAccountScope::BinanceStdUm,
            Rc::new(RefCell::new(usdt_mgr)),
        )]);
        let inner = MonitorChannelInner {
            open_venue: TradingVenue::BinanceFutures,
            hedge_venue: TradingVenue::BinanceFutures,
            arb_mode: ArbMode::CrossArb,
            binance_account_mode: Some(BinanceAccountMode::Standard),
            hyperliquid_account_mode: None,
            open_leg,
            hedge_leg,
            usdt_mgrs,
            price_table: Rc::new(RefCell::new(PriceTable::new())),
            venue_min_qty_tables: HashMap::new(),
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Standard,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk: HashMap::from([(
                BasicAccountScope::BinanceStdUm,
                BasicAccountRiskMsg::create(
                    0, 98_765.43, 98_765.43, 2_500.0, 12_000.0, 39.506172, 0.0, 0.0,
                ),
            )]),
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(false),
        };

        let state = MonitorChannel::compute_basic_state(&inner);
        assert!((state.total_equity_usdt - 98_765.43).abs() < 1e-9);
    }

    #[test]
    fn top_two_gross_positions_are_ranked_by_usdt_value() {
        let positions = HashMap::from([
            ("BTC".to_string(), 100.0),
            ("ETH".to_string(), 300.0),
            ("SOL".to_string(), 200.0),
        ]);

        assert_eq!(
            top_two_gross_position_symbols(&positions),
            vec!["ETHUSDT".to_string(), "SOLUSDT".to_string()]
        );
    }

    #[test]
    fn top_two_gross_positions_filter_invalid_and_zero_values() {
        let positions = HashMap::from([
            ("BTC".to_string(), f64::NAN),
            ("ETH".to_string(), f64::INFINITY),
            ("SOL".to_string(), 0.0),
            ("XRP".to_string(), 1e-6),
            ("USDT".to_string(), 1_000_000.0),
            ("USDC".to_string(), 900_000.0),
            ("BFUSD".to_string(), 800_000.0),
            ("DOGE".to_string(), 50.0),
        ]);

        assert_eq!(
            top_two_gross_position_symbols(&positions),
            vec!["DOGEUSDT".to_string()]
        );
    }

    #[test]
    fn top_two_gross_positions_use_stable_symbol_tie_break() {
        let positions = HashMap::from([
            ("sol".to_string(), 100.0),
            ("BTCUSDT".to_string(), 100.0),
            ("eth".to_string(), 100.0),
        ]);

        assert_eq!(
            top_two_gross_position_symbols(&positions),
            vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
        );
    }

    #[test]
    fn missing_position_marks_ignore_cash_and_dust() {
        let exposures = HashMap::from([
            ("USDT".to_string(), (10_000.0, 0.0)),
            ("USDC".to_string(), (20_000.0, 0.0)),
            ("BFUSD".to_string(), (50_000.0, 0.0)),
            ("DUST".to_string(), (0.0000004, 0.0000004)),
            ("BTC".to_string(), (0.1, -0.1)),
            ("ETH".to_string(), (10.0, -10.0)),
        ]);
        let mut marks = HashMap::from([("ETH".to_string(), 3_000.0)]);
        assert_eq!(
            missing_position_mark_assets(&exposures, &marks),
            vec!["BTC".to_string()]
        );
        marks.insert("BTC".to_string(), 100_000.0);
        assert!(missing_position_mark_assets(&exposures, &marks).is_empty());
    }

    #[test]
    fn gate_mark_price_lookup_maps_internal_symbol_to_contract_symbol() {
        let mapper = create_symbol_mapper(Exchange::Gate);
        let mut price_table = PriceTable::new();
        price_table.update_mark_price("PIPPIN_USDT", 0.01856, 0);

        assert_eq!(
            MonitorChannel::mark_price_for_asset(&*mapper, &price_table, "PIPPIN"),
            0.01856
        );
    }

    #[test]
    fn monitor_fast_poll_budget_maps_messages_to_tokens() {
        assert_eq!(
            monitor_fast_poll_token_limit(8),
            8 * MONITOR_FAST_POLL_NORMAL_WEIGHT
        );
        assert_eq!(
            monitor_fast_poll_raw_limit(8),
            8 * MONITOR_FAST_POLL_RAW_MULTIPLIER
        );
        assert_eq!(
            monitor_fast_poll_token_limit(0),
            MONITOR_FAST_POLL_LOW_WEIGHT
        );
        assert_eq!(monitor_fast_poll_raw_limit(0), 1);
    }

    #[test]
    fn normalized_update_borrows_internal_symbol_key() {
        let update = BinanceBasicOrderMsg::create(
            BinanceBasicOrderMsg::VENUE_UM,
            1,
            1,
            "BTCUSDT".to_string(),
            100,
            42,
            7,
            Side::Buy.to_u8(),
            order_common::OrderType::Limit.to_u8(),
            order_common::TimeInForce::GTC.to_u8(),
            ExecutionType::New.to_u8(),
            OrderStatus::New.to_u8(),
            false,
            1.0,
            2.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            "USDT".to_string(),
        );

        let normalized = NormalizedUpdate::new(&update);

        assert_eq!(order_common::OrderUpdate::symbol(&normalized), "BTCUSDT");
        assert!(matches!(normalized.symbol, Cow::Borrowed("BTCUSDT")));
    }

    #[test]
    fn external_settlement_builds_nav_uniform_fill() {
        let mut update = BinanceBasicOrderMsg::create(
            BinanceBasicOrderMsg::VENUE_UM,
            1_787_734_851_000,
            1_787_734_850_999,
            "STORJUSDT".to_string(),
            998_877,
            -998_877,
            556_677,
            Side::Sell.to_u8(),
            order_common::OrderType::Market.to_u8(),
            order_common::TimeInForce::IOC.to_u8(),
            ExecutionType::Trade.to_u8(),
            OrderStatus::Filled.to_u8(),
            false,
            0.0,
            43_407.86712966,
            43_407.86712966,
            43_407.86712966,
            0.1832,
            0.1832,
            0.0,
            -123.45,
            "USDT".to_string(),
        );
        update.external_order_kind = BinanceBasicOrderMsg::EXTERNAL_SETTLEMENT;

        let record = build_binance_external_uniform_order(&update).expect("uniform fill");

        assert_eq!(record.symbol, b"STORJUSDT");
        assert_eq!(record.side, Side::Sell.to_u8());
        assert_eq!(record.status, OrderStatus::Filled.to_u8());
        assert_eq!(
            record.from_key,
            b"exchange_forced_close:settlement:order=998877:trade=556677"
        );
        assert!((record.price - 0.1832).abs() < 1e-12);
        assert!((record.amount_update - 43_407.86712966).abs() < 1e-9);
        assert!(record.length_fields_consistent());
    }

    #[test]
    fn hyperliquid_liquidation_builds_auditable_uniform_fill() {
        let update = HyperliquidBasicFillMsg::create(
            TradingVenue::HyperliquidFutures.to_u8(),
            1_787_734_851_000,
            1_787_734_851_000,
            "BTCUSDC".to_string(),
            998_877,
            0,
            String::new(),
            "hl:fixture",
            556_677,
            "0xabc123".to_string(),
            "backstop".to_string(),
            order_common::Side::Sell.to_u8(),
            false,
            95_000.0,
            0.25,
            0.25,
            None,
        );

        let record =
            build_hyperliquid_external_uniform_order(&update).expect("uniform liquidation fill");

        assert_eq!(record.symbol, b"BTCUSDC");
        assert_eq!(record.client_order_id, -998_877);
        assert_eq!(record.side, order_common::Side::Sell.to_u8());
        assert_eq!(record.status, OrderStatus::Filled.to_u8());
        assert_eq!(record.create_ts, 1_787_734_851_000_000);
        assert_eq!(
            record.from_key,
            b"exchange_forced_close:liquidation:method=backstop:order=998877:trade=556677:tx=0xabc123"
        );
        assert_eq!(record.amount_init, 0.25);
        assert_eq!(record.amount_update, 0.25);
        assert!(record.length_fields_consistent());
    }

    #[test]
    fn hyperliquid_manual_fill_is_not_attributed_as_forced_close() {
        let update = HyperliquidBasicFillMsg::create(
            TradingVenue::HyperliquidFutures.to_u8(),
            1,
            1,
            "BTCUSDC".to_string(),
            42,
            0,
            String::new(),
            "hl:fixture",
            7,
            "0xmanual".to_string(),
            String::new(),
            order_common::Side::Buy.to_u8(),
            false,
            100.0,
            1.0,
            1.0,
            None,
        );

        assert!(build_hyperliquid_external_uniform_order(&update).is_none());
    }

    #[test]
    fn normalized_update_allocates_exchange_symbol_format() {
        let update = GateBasicOrderMsg::create(
            GateBasicOrderMsg::VENUE_SPOT,
            1,
            "BTC_USDT".to_string(),
            100,
            42,
            Side::Buy.to_u8(),
            order_common::OrderType::Limit.to_u8(),
            order_common::TimeInForce::GTC.to_u8(),
            ExecutionType::New.to_u8(),
            OrderStatus::New.to_u8(),
            0,
            1.0,
            2.0,
            0.0,
            0.0,
            "USDT".to_string(),
        );

        let normalized = NormalizedUpdate::new(&update);

        assert_eq!(order_common::OrderUpdate::symbol(&normalized), "BTCUSDT");
        assert!(matches!(
            normalized.symbol,
            Cow::Owned(ref symbol) if symbol == "BTCUSDT"
        ));
    }

    struct TestMmOpenStrategy {
        id: i32,
        symbol: String,
        side: Side,
        client_order_id: i64,
        cancel_trigger_count: usize,
        arb_cancel_trigger_count: usize,
        last_trigger_ts: i64,
        active: bool,
    }

    impl TestMmOpenStrategy {
        fn new(id: i32, symbol: &str, side: Side, client_order_id: i64) -> Self {
            Self {
                id,
                symbol: normalize_symbol_for_internal(symbol),
                side,
                client_order_id,
                cancel_trigger_count: 0,
                arb_cancel_trigger_count: 0,
                last_trigger_ts: 0,
                active: true,
            }
        }
    }

    impl Strategy for TestMmOpenStrategy {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn get_id(&self) -> i32 {
            self.id
        }

        fn is_strategy_order(&self, order_id: i64) -> bool {
            order_id == self.client_order_id
        }

        fn handle_signal(&mut self, signal: &TradeSignal) {
            if signal.signal_type.clone() as u32 == SignalType::MMCancel as u32 {
                let ctx = MmCancelCtx::from_slice(signal.context.as_ref()).expect("mm cancel ctx");
                self.cancel_trigger_count += 1;
                self.last_trigger_ts = ctx.trigger_ts;
            } else if signal.signal_type.clone() as u32 == SignalType::ArbCancel as u32 {
                let ctx =
                    ArbCancelCtx::from_slice(signal.context.as_ref()).expect("arb cancel ctx");
                self.arb_cancel_trigger_count += 1;
                self.last_trigger_ts = ctx.trigger_ts;
            }
        }

        fn apply_order_update(&mut self, _update: &dyn order_common::OrderUpdate) {}

        fn apply_trade_update(&mut self, _trade: &dyn order_common::TradeUpdate) {}

        fn handle_period_clock(&mut self, _current_tp: i64) {}

        fn is_active(&self) -> bool {
            self.active
        }

        fn symbol(&self) -> Option<&str> {
            Some(&self.symbol)
        }

        fn mm_open_price_map_entry(&self) -> Option<OpenPriceMapEntry> {
            Some(OpenPriceMapEntry {
                symbol: self.symbol.clone(),
                side: self.side,
                client_order_id: self.client_order_id,
                price_qv: QuantizedValue::from_parts(1, 0, 1).into(),
            })
        }

        fn arb_open_price_map_entry(&self) -> Option<OpenPriceMapEntry> {
            Some(OpenPriceMapEntry {
                symbol: self.symbol.clone(),
                side: self.side,
                client_order_id: self.client_order_id,
                price_qv: QuantizedValue::from_parts(1, 0, 1).into(),
            })
        }
    }

    #[test]
    fn okex_futures_qty_to_base_uses_contract_multiplier() {
        let mut okx_table = VenueMinQtyTable::new(TradingVenue::OkexFutures);
        okx_table.set_contract_multiplier_for_test("FILUSDT", 0.1);

        let open_leg = LegMgr::Futures {
            exchange: Exchange::Okex,
            um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Okex))),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Okex))),
        };
        let hedge_leg = LegMgr::Margin {
            bal: Rc::new(RefCell::new(BasicBalanceManager::new(Exchange::Binance))),
        };

        let mut venue_min_qty_tables: HashMap<TradingVenue, Rc<VenueMinQtyTable>> = HashMap::new();
        venue_min_qty_tables.insert(TradingVenue::OkexFutures, Rc::new(okx_table));

        let mut usdt_mgrs: HashMap<BasicAccountScope, Rc<RefCell<UsdtBalanceManager>>> =
            HashMap::new();
        usdt_mgrs.insert(
            BasicAccountScope::OkexUnified,
            Rc::new(RefCell::new(UsdtBalanceManager::new(Exchange::Okex))),
        );

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::OkexFutures,
            hedge_venue: TradingVenue::BinanceFutures,
            arb_mode: ArbMode::CrossArb,
            binance_account_mode: Some(BinanceAccountMode::Unified),
            hyperliquid_account_mode: None,
            open_leg,
            hedge_leg,
            usdt_mgrs,
            price_table: Rc::new(RefCell::new(PriceTable::new())),
            venue_min_qty_tables,
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk: HashMap::new(),
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(false),
        };

        MonitorChannel::clear_basic_state_runtime_cache();
        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });
        MonitorChannel::refresh_basic_state_cache();

        let base_qty =
            MonitorChannel::instance().qty_to_base(TradingVenue::OkexFutures, "FIL-USDT-SWAP", 1.0);
        assert!((base_qty - 0.1).abs() < 1e-12);
        let overhedge_factor = 1.0 / base_qty;
        assert!((overhedge_factor - 10.0).abs() < 1e-12);

        let replacement = HashMap::from([(
            "FILUSDT".to_string(),
            VenueMinQtyEntry {
                symbol: "FILUSDT".to_string(),
                base_asset: "FIL".to_string(),
                quote_asset: "USDT".to_string(),
                min_qty: 1.0,
                step_size: 1.0,
                price_tick: Some(0.001),
                min_notional: None,
            },
        )]);
        MonitorChannel::replace_manager_order_rules(
            TradingVenue::OkexFutures,
            signal_common::min_qty_table::MarketType::Futures,
            replacement,
            HashMap::from([("FILUSDT".to_string(), 0.2)]),
            std::collections::HashSet::from(["FILUSDT".to_string()]),
        )
        .unwrap();

        let venue_table = MonitorChannel::instance()
            .try_venue_min_qty_table(TradingVenue::OkexFutures)
            .unwrap();
        assert_eq!(venue_table.price_tick("FILUSDT"), Some(0.001));
        assert_eq!(venue_table.contract_multiplier_opt("FILUSDT"), Some(0.2));
        let legacy_multiplier = MonitorChannel::with_inner(|inner| match &inner.open_leg {
            LegMgr::Futures { min_qty_table, .. } => {
                min_qty_table.borrow().contract_multiplier("FILUSDT")
            }
            LegMgr::Margin { .. } => unreachable!(),
        });
        assert_eq!(legacy_multiplier, 0.2);
    }

    #[test]
    fn ensure_max_pos_u_rejects_when_okex_multiplier_missing() {
        let okx_table = VenueMinQtyTable::new(TradingVenue::OkexFutures);

        let open_leg = LegMgr::Futures {
            exchange: Exchange::Okex,
            um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Okex))),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Okex))),
        };
        let hedge_leg = LegMgr::Margin {
            bal: Rc::new(RefCell::new(BasicBalanceManager::new(Exchange::Binance))),
        };

        let mut venue_min_qty_tables: HashMap<TradingVenue, Rc<VenueMinQtyTable>> = HashMap::new();
        venue_min_qty_tables.insert(TradingVenue::OkexFutures, Rc::new(okx_table));

        let mut price_table = PriceTable::new();
        price_table.update_mark_price("FILUSDT", 100.0, 0);

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::OkexFutures,
            hedge_venue: TradingVenue::BinanceFutures,
            arb_mode: ArbMode::CrossArb,
            binance_account_mode: Some(BinanceAccountMode::Unified),
            hyperliquid_account_mode: None,
            open_leg,
            hedge_leg,
            usdt_mgrs: HashMap::new(),
            price_table: Rc::new(RefCell::new(price_table)),
            venue_min_qty_tables,
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk: HashMap::new(),
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(false),
        };

        MonitorChannel::clear_basic_state_runtime_cache();
        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });
        MonitorChannel::refresh_basic_state_cache();

        let err = MonitorChannel::instance()
            .ensure_max_pos_u("FIL-USDT-SWAP", 2.0, 100.0)
            .unwrap_err();
        assert!(err.contains("缺少 OkexFutures 合约乘数"), "err={err}");
    }

    #[test]
    fn ensure_max_pos_u_uses_okex_multiplier_in_risk_calc() {
        let mut okx_table = VenueMinQtyTable::new(TradingVenue::OkexFutures);
        okx_table.set_contract_multiplier_for_test("FILUSDT", 10.0);

        let open_leg = LegMgr::Futures {
            exchange: Exchange::Okex,
            um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Okex))),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Okex))),
        };
        let hedge_leg = LegMgr::Margin {
            bal: Rc::new(RefCell::new(BasicBalanceManager::new(Exchange::Binance))),
        };

        let mut venue_min_qty_tables: HashMap<TradingVenue, Rc<VenueMinQtyTable>> = HashMap::new();
        venue_min_qty_tables.insert(TradingVenue::OkexFutures, Rc::new(okx_table));

        let mut price_table = PriceTable::new();
        price_table.update_mark_price("FILUSDT", 100.0, 0);

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::OkexFutures,
            hedge_venue: TradingVenue::BinanceFutures,
            arb_mode: ArbMode::CrossArb,
            binance_account_mode: Some(BinanceAccountMode::Unified),
            hyperliquid_account_mode: None,
            open_leg,
            hedge_leg,
            usdt_mgrs: HashMap::new(),
            price_table: Rc::new(RefCell::new(price_table)),
            venue_min_qty_tables,
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk: HashMap::new(),
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(false),
        };

        MonitorChannel::clear_basic_state_runtime_cache();
        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });
        MonitorChannel::refresh_basic_state_cache();

        // max_pos_u default = 1000.0 (PreTradeParamsLoader::default)
        // FIL mark = 100.0, contracts=2, mult=10 => base=20 => notional=2000 > 1000
        assert!(MonitorChannel::instance()
            .ensure_max_pos_u("FIL-USDT-SWAP", 2.0, 100.0)
            .is_err());
    }

    #[test]
    fn ensure_max_pos_u_base_delta_uses_cached_mark_until_refresh() {
        let open_leg = LegMgr::Futures {
            exchange: Exchange::Binance,
            um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Binance))),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Binance))),
        };
        let hedge_leg = LegMgr::Margin {
            bal: Rc::new(RefCell::new(BasicBalanceManager::new(Exchange::Binance))),
        };

        let mut price_table = PriceTable::new();
        price_table.update_mark_price("FILUSDT", 100.0, 0);

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::BinanceFutures,
            hedge_venue: TradingVenue::BinanceFutures,
            arb_mode: ArbMode::CrossArb,
            binance_account_mode: Some(BinanceAccountMode::Unified),
            hyperliquid_account_mode: None,
            open_leg,
            hedge_leg,
            usdt_mgrs: HashMap::new(),
            price_table: Rc::new(RefCell::new(price_table)),
            venue_min_qty_tables: HashMap::new(),
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk: HashMap::new(),
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(false),
        };

        MonitorChannel::clear_basic_state_runtime_cache();
        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });
        MonitorChannel::refresh_basic_state_cache();

        MonitorChannel::with_inner(|inner| {
            inner
                .price_table
                .borrow_mut()
                .update_mark_price("FILUSDT", 1_000.0, 1);
        });
        MonitorChannel::mark_basic_state_dirty();

        assert!(MonitorChannel::instance()
            .ensure_max_pos_u_for_base_delta(
                "FILUSDT",
                TradingVenue::BinanceFutures,
                0.0,
                2.0,
                100.0,
                2.0,
                1.0,
            )
            .is_ok());

        MonitorChannel::refresh_basic_state_cache();
        let err = MonitorChannel::instance()
            .ensure_max_pos_u_for_base_delta(
                "FILUSDT",
                TradingVenue::BinanceFutures,
                0.0,
                2.0,
                100.0,
                2.0,
                1.0,
            )
            .unwrap_err();
        assert!(err.contains("下单后持仓"), "err={err}");
    }

    #[test]
    fn ensure_max_pos_u_allows_reducing_when_over_limit() {
        let mut um_mgr = BasicUmManager::new(Exchange::Binance);
        let pos_msg = BasicPositionMsg::create(0, "FILUSDT".to_string(), 'L', 20.0);
        um_mgr.apply_position(&pos_msg);

        let open_leg = LegMgr::Futures {
            exchange: Exchange::Binance,
            um: Rc::new(RefCell::new(um_mgr)),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Binance))),
        };
        let hedge_leg = LegMgr::Margin {
            bal: Rc::new(RefCell::new(BasicBalanceManager::new(Exchange::Binance))),
        };

        let mut price_table = PriceTable::new();
        price_table.update_mark_price("FILUSDT", 100.0, 0);

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::BinanceFutures,
            hedge_venue: TradingVenue::BinanceFutures,
            arb_mode: ArbMode::CrossArb,
            binance_account_mode: Some(BinanceAccountMode::Unified),
            hyperliquid_account_mode: None,
            open_leg,
            hedge_leg,
            usdt_mgrs: HashMap::new(),
            price_table: Rc::new(RefCell::new(price_table)),
            venue_min_qty_tables: HashMap::new(),
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk: HashMap::new(),
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(false),
        };

        MonitorChannel::clear_basic_state_runtime_cache();
        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });
        MonitorChannel::refresh_basic_state_cache();

        // 当前持仓 20 * 100 = 2000 > max_pos_u(1000)，但减少仓位应放行。
        assert!(MonitorChannel::instance()
            .ensure_max_pos_u("FILUSDT", -5.0, 100.0)
            .is_ok());
    }

    #[test]
    fn gate_margin_min_notional_rejects_dust_close_qty() {
        let mut gate_table = VenueMinQtyTable::new(TradingVenue::GateMargin);
        gate_table.set_entry_for_test(VenueMinQtyEntry {
            symbol: "CCUSDT".to_string(),
            base_asset: "CC".to_string(),
            quote_asset: "USDT".to_string(),
            min_qty: 1.0,
            step_size: 1.0,
            price_tick: Some(0.00001),
            min_notional: Some(5.0),
        });

        let open_leg = LegMgr::Margin {
            bal: Rc::new(RefCell::new(BasicBalanceManager::new(Exchange::Gate))),
        };
        let hedge_leg = LegMgr::Futures {
            exchange: Exchange::Gate,
            um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Gate))),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Gate))),
        };

        let mut venue_min_qty_tables: HashMap<TradingVenue, Rc<VenueMinQtyTable>> = HashMap::new();
        venue_min_qty_tables.insert(TradingVenue::GateMargin, Rc::new(gate_table));

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::GateMargin,
            hedge_venue: TradingVenue::GateFutures,
            arb_mode: ArbMode::FundingArb,
            binance_account_mode: Some(BinanceAccountMode::Unified),
            hyperliquid_account_mode: None,
            open_leg,
            hedge_leg,
            usdt_mgrs: HashMap::new(),
            price_table: Rc::new(RefCell::new(PriceTable::new())),
            venue_min_qty_tables,
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk: HashMap::new(),
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(false),
        };

        MonitorChannel::clear_basic_state_runtime_cache();
        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });
        MonitorChannel::refresh_basic_state_cache();

        let err = MonitorChannel::instance()
            .check_min_trading_requirements(TradingVenue::GateMargin, "CCUSDT", 7.0, Some(0.14653))
            .unwrap_err();
        assert!(err.contains("名义金额"), "err={err}");

        assert!(MonitorChannel::instance()
            .check_min_trading_requirements(TradingVenue::GateMargin, "CCUSDT", 40.0, Some(0.14653))
            .is_ok());
    }

    fn install_binance_arb_hedge_exposure_fixture(
        open_qty: f32,
        hedge_qty: f32,
        startup_gate_enabled: bool,
    ) -> Rc<RefCell<StrategyManager>> {
        let mut open_um = BasicUmManager::new(Exchange::Binance);
        if open_qty != 0.0 {
            let side = if open_qty > 0.0 { 'L' } else { 'S' };
            open_um.apply_position(&BasicPositionMsg::create(
                0,
                "FILUSDT".to_string(),
                side,
                open_qty.abs(),
            ));
        }
        let mut hedge_um = BasicUmManager::new(Exchange::Binance);
        if hedge_qty != 0.0 {
            let side = if hedge_qty > 0.0 { 'L' } else { 'S' };
            hedge_um.apply_position(&BasicPositionMsg::create(
                0,
                "FILUSDT".to_string(),
                side,
                hedge_qty.abs(),
            ));
        }

        let open_leg = LegMgr::Futures {
            exchange: Exchange::Binance,
            um: Rc::new(RefCell::new(open_um)),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Binance))),
        };
        let hedge_leg = LegMgr::Futures {
            exchange: Exchange::Binance,
            um: Rc::new(RefCell::new(hedge_um)),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Binance))),
        };

        let mut price_table = PriceTable::new();
        price_table.update_mark_price("FILUSDT", 100.0, 0);

        let mut usdt_mgr = UsdtBalanceManager::new(Exchange::Binance);
        usdt_mgr.apply_balance(&BasicBalanceMsg::create(0, "USDT".to_string(), 10_000.0));
        let mut usdt_mgrs: HashMap<BasicAccountScope, Rc<RefCell<UsdtBalanceManager>>> =
            HashMap::new();
        usdt_mgrs.insert(
            BasicAccountScope::BinanceUnified,
            Rc::new(RefCell::new(usdt_mgr)),
        );
        let strategy_mgr = Rc::new(RefCell::new(StrategyManager::new()));

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::BinanceMargin,
            hedge_venue: TradingVenue::BinanceFutures,
            arb_mode: ArbMode::FundingArb,
            binance_account_mode: Some(BinanceAccountMode::Unified),
            hyperliquid_account_mode: None,
            open_leg,
            hedge_leg,
            usdt_mgrs,
            price_table: Rc::new(RefCell::new(price_table)),
            venue_min_qty_tables: HashMap::new(),
            strategy_mgr: strategy_mgr.clone(),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk: HashMap::new(),
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(startup_gate_enabled),
        };

        MonitorChannel::clear_basic_state_runtime_cache();
        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });
        MonitorChannel::refresh_basic_state_cache();
        strategy_mgr
    }

    fn install_binance_exec_cross_section_fixture() {
        let mut open_um = BasicUmManager::new(Exchange::Binance);
        open_um.apply_position(&BasicPositionMsg::create(
            0,
            "FILUSDT".to_string(),
            'L',
            2.0,
        ));
        open_um.apply_position(&BasicPositionMsg::create(
            0,
            "ETHUSDT".to_string(),
            'S',
            2.0,
        ));

        let open_leg = LegMgr::Futures {
            exchange: Exchange::Binance,
            um: Rc::new(RefCell::new(open_um)),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Binance))),
        };
        let hedge_leg = LegMgr::Futures {
            exchange: Exchange::Binance,
            um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Binance))),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Binance))),
        };

        let mut price_table = PriceTable::new();
        price_table.update_mark_price("FILUSDT", 100.0, 0);
        price_table.update_mark_price("ETHUSDT", 50.0, 0);

        let mut usdt_mgr = UsdtBalanceManager::new(Exchange::Binance);
        usdt_mgr.apply_balance(&BasicBalanceMsg::create(0, "USDT".to_string(), 10_000.0));
        let mut usdt_mgrs: HashMap<BasicAccountScope, Rc<RefCell<UsdtBalanceManager>>> =
            HashMap::new();
        usdt_mgrs.insert(
            BasicAccountScope::BinanceUnified,
            Rc::new(RefCell::new(usdt_mgr)),
        );

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::BinanceFutures,
            hedge_venue: TradingVenue::BinanceMargin,
            arb_mode: ArbMode::CrossArb,
            binance_account_mode: Some(BinanceAccountMode::Unified),
            hyperliquid_account_mode: None,
            open_leg,
            hedge_leg,
            usdt_mgrs,
            price_table: Rc::new(RefCell::new(price_table)),
            venue_min_qty_tables: HashMap::new(),
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk: HashMap::new(),
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(false),
        };

        MonitorChannel::clear_basic_state_runtime_cache();
        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });
        MonitorChannel::refresh_basic_state_cache();
    }

    fn install_binance_arb_margin_open_fixture() -> (
        Rc<RefCell<StrategyManager>>,
        Rc<RefCell<BasicBalanceManager>>,
    ) {
        let open_bal = Rc::new(RefCell::new(BasicBalanceManager::new(Exchange::Binance)));
        let open_leg = LegMgr::Margin {
            bal: open_bal.clone(),
        };
        let hedge_leg = LegMgr::Futures {
            exchange: Exchange::Binance,
            um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Binance))),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Binance))),
        };

        let mut price_table = PriceTable::new();
        price_table.update_mark_price("FILUSDT", 100.0, 0);

        let mut usdt_mgr = UsdtBalanceManager::new(Exchange::Binance);
        usdt_mgr.apply_balance(&BasicBalanceMsg::create(0, "USDT".to_string(), 10_000.0));
        let mut usdt_mgrs: HashMap<BasicAccountScope, Rc<RefCell<UsdtBalanceManager>>> =
            HashMap::new();
        usdt_mgrs.insert(
            BasicAccountScope::BinanceUnified,
            Rc::new(RefCell::new(usdt_mgr)),
        );

        let strategy_mgr = Rc::new(RefCell::new(StrategyManager::new()));
        let inner = MonitorChannelInner {
            open_venue: TradingVenue::BinanceMargin,
            hedge_venue: TradingVenue::BinanceFutures,
            arb_mode: ArbMode::FundingArb,
            binance_account_mode: Some(BinanceAccountMode::Unified),
            hyperliquid_account_mode: None,
            open_leg,
            hedge_leg,
            usdt_mgrs,
            price_table: Rc::new(RefCell::new(price_table)),
            venue_min_qty_tables: HashMap::new(),
            strategy_mgr: strategy_mgr.clone(),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk: HashMap::new(),
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(false),
        };

        MonitorChannel::clear_basic_state_runtime_cache();
        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });
        MonitorChannel::refresh_basic_state_cache();
        (strategy_mgr, open_bal)
    }

    #[test]
    fn basic_state_filters_non_trading_dust_by_position_usdt() {
        let open_bal = Rc::new(RefCell::new(BasicBalanceManager::new(Exchange::Binance)));
        open_bal
            .borrow_mut()
            .apply_balance(&BasicBalanceMsg::create(0, "DOGE".to_string(), 10.0));
        open_bal
            .borrow_mut()
            .apply_balance(&BasicBalanceMsg::create(0, "FIL".to_string(), 1.0));
        open_bal
            .borrow_mut()
            .apply_balance(&BasicBalanceMsg::create(0, "USDC".to_string(), 20_000.0));
        open_bal
            .borrow_mut()
            .apply_balance(&BasicBalanceMsg::create(0, "BFUSD".to_string(), 50_000.0));

        let open_leg = LegMgr::Margin { bal: open_bal };
        let hedge_leg = LegMgr::Futures {
            exchange: Exchange::Binance,
            um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Binance))),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Binance))),
        };

        let mut price_table = PriceTable::new();
        price_table.update_mark_price("DOGEUSDT", 1.0, 0);
        price_table.update_mark_price("FILUSDT", 100.0, 0);

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::BinanceMargin,
            hedge_venue: TradingVenue::BinanceFutures,
            arb_mode: ArbMode::FundingArb,
            binance_account_mode: Some(BinanceAccountMode::Unified),
            hyperliquid_account_mode: None,
            open_leg,
            hedge_leg,
            usdt_mgrs: HashMap::new(),
            price_table: Rc::new(RefCell::new(price_table)),
            venue_min_qty_tables: HashMap::new(),
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk: HashMap::from([(
                BasicAccountScope::BinanceUnified,
                BasicAccountRiskMsg::create(
                    0, 69_000.0, 70_000.0, 1_000.0, 2_000.0, 70.0, 0.0, 0.0,
                ),
            )]),
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(false),
        };

        let state = MonitorChannel::compute_basic_state(&inner);
        assert!(!state.exposures.contains_key("DOGE"));
        assert!(state.exposures.contains_key("FIL"));
        assert!(!state.exposures.contains_key("USDC"));
        assert!(!state.exposures.contains_key("BFUSD"));
        assert!(!state
            .margin_balances_by_scope
            .values()
            .any(|balances| balances.contains_key("DOGE")));
        let collateral_balances = state
            .margin_balances_by_scope
            .get(&BasicAccountScope::BinanceUnified)
            .expect("Binance PM collateral balances");
        assert_eq!(collateral_balances.get("USDC"), Some(&20_000.0));
        assert_eq!(collateral_balances.get("BFUSD"), Some(&50_000.0));
        assert!((state.total_equity_usdt - 70_000.0).abs() < 1e-12);
        assert!((state.total_position_usdt - 100.0).abs() < 1e-12);
    }

    #[test]
    fn basic_state_keeps_active_strategy_dust_position() {
        let open_bal = Rc::new(RefCell::new(BasicBalanceManager::new(Exchange::Binance)));
        open_bal
            .borrow_mut()
            .apply_balance(&BasicBalanceMsg::create(0, "DOGE".to_string(), 10.0));

        let open_leg = LegMgr::Margin { bal: open_bal };
        let hedge_leg = LegMgr::Futures {
            exchange: Exchange::Binance,
            um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Binance))),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Binance))),
        };

        let mut price_table = PriceTable::new();
        price_table.update_mark_price("DOGEUSDT", 1.0, 0);

        let strategy_mgr = Rc::new(RefCell::new(StrategyManager::new()));
        strategy_mgr
            .borrow_mut()
            .insert(Box::new(TestMmOpenStrategy::new(
                301,
                "DOGEUSDT",
                Side::Buy,
                301_0001,
            )));

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::BinanceMargin,
            hedge_venue: TradingVenue::BinanceFutures,
            arb_mode: ArbMode::FundingArb,
            binance_account_mode: Some(BinanceAccountMode::Unified),
            hyperliquid_account_mode: None,
            open_leg,
            hedge_leg,
            usdt_mgrs: HashMap::new(),
            price_table: Rc::new(RefCell::new(price_table)),
            venue_min_qty_tables: HashMap::new(),
            strategy_mgr,
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk: HashMap::new(),
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(false),
        };

        let state = MonitorChannel::compute_basic_state(&inner);
        assert_eq!(state.exposures.get("DOGE"), Some(&(10.0, 0.0)));
        assert!((state.total_position_usdt - 10.0).abs() < 1e-12);
    }

    #[test]
    fn bybit_total_equity_uses_wallet_account_risk_actual_equity() {
        let mut bybit_bal = BasicBalanceManager::new(Exchange::Bybit);
        bybit_bal.apply_balance(&BasicBalanceMsg::create(0, "BTC".to_string(), 1.0));
        let open_leg = LegMgr::Margin {
            bal: Rc::new(RefCell::new(bybit_bal)),
        };
        let hedge_leg = LegMgr::Futures {
            exchange: Exchange::Bybit,
            um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Bybit))),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Bybit))),
        };

        let mut price_table = PriceTable::new();
        price_table.update_mark_price("BTCUSDT", 50_000.0, 0);

        let mut usdt_mgr = UsdtBalanceManager::new(Exchange::Bybit);
        usdt_mgr.apply_balance(&BasicBalanceMsg::create(0, "USDT".to_string(), 10_000.0));
        let mut usdt_mgrs: HashMap<BasicAccountScope, Rc<RefCell<UsdtBalanceManager>>> =
            HashMap::new();
        usdt_mgrs.insert(
            BasicAccountScope::BybitUnified,
            Rc::new(RefCell::new(usdt_mgr)),
        );

        let mut latest_account_risk = HashMap::new();
        latest_account_risk.insert(
            BasicAccountScope::BybitUnified,
            BasicAccountRiskMsg::create(0, 59_000.0, 60_000.0, 1_000.0, 2_000.0, 60.0, 0.0, 0.0),
        );

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::BybitMargin,
            hedge_venue: TradingVenue::BybitFutures,
            arb_mode: ArbMode::IntraArb,
            binance_account_mode: Some(BinanceAccountMode::Unified),
            hyperliquid_account_mode: None,
            open_leg,
            hedge_leg,
            usdt_mgrs,
            price_table: Rc::new(RefCell::new(price_table)),
            venue_min_qty_tables: HashMap::new(),
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk,
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(false),
        };

        let state = MonitorChannel::compute_basic_state(&inner);
        assert!((state.total_equity_usdt - 60_000.0).abs() < 1e-9);
    }

    #[test]
    fn okex_intra_total_equity_uses_account_risk_actual_equity() {
        let mut okex_bal = BasicBalanceManager::new(Exchange::Okex);
        okex_bal.apply_balance(&BasicBalanceMsg::create(0, "BTC".to_string(), 1.0));
        let open_leg = LegMgr::Margin {
            bal: Rc::new(RefCell::new(okex_bal)),
        };
        let hedge_leg = LegMgr::Futures {
            exchange: Exchange::Okex,
            um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Okex))),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Okex))),
        };

        let mut price_table = PriceTable::new();
        price_table.update_mark_price("BTCUSDT", 50_000.0, 0);

        let mut usdt_mgr = UsdtBalanceManager::new(Exchange::Okex);
        usdt_mgr.apply_balance(&BasicBalanceMsg::create(0, "USDT".to_string(), 10_000.0));
        let mut usdt_mgrs: HashMap<BasicAccountScope, Rc<RefCell<UsdtBalanceManager>>> =
            HashMap::new();
        usdt_mgrs.insert(
            BasicAccountScope::OkexUnified,
            Rc::new(RefCell::new(usdt_mgr)),
        );

        let mut latest_account_risk = HashMap::new();
        latest_account_risk.insert(
            BasicAccountScope::OkexUnified,
            BasicAccountRiskMsg::create(0, 58_000.0, 61_000.0, 1_000.0, 2_000.0, 61.0, 0.0, 0.0),
        );

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::OkexMargin,
            hedge_venue: TradingVenue::OkexFutures,
            arb_mode: ArbMode::IntraArb,
            binance_account_mode: Some(BinanceAccountMode::Unified),
            hyperliquid_account_mode: None,
            open_leg,
            hedge_leg,
            usdt_mgrs,
            price_table: Rc::new(RefCell::new(price_table)),
            venue_min_qty_tables: HashMap::new(),
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk,
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(false),
        };

        let state = MonitorChannel::compute_basic_state(&inner);
        assert!((state.total_equity_usdt - 61_000.0).abs() < 1e-9);
    }

    #[test]
    fn bitget_intra_total_equity_uses_account_risk_actual_equity() {
        let mut bitget_bal = BasicBalanceManager::new(Exchange::Bitget);
        bitget_bal.apply_balance(&BasicBalanceMsg::create(0, "BTC".to_string(), 1.0));
        let open_leg = LegMgr::Margin {
            bal: Rc::new(RefCell::new(bitget_bal)),
        };
        let hedge_leg = LegMgr::Futures {
            exchange: Exchange::Bitget,
            um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Bitget))),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Bitget))),
        };

        let mut price_table = PriceTable::new();
        price_table.update_mark_price("BTCUSDT", 50_000.0, 0);

        let mut usdt_mgr = UsdtBalanceManager::new(Exchange::Bitget);
        usdt_mgr.apply_balance(&BasicBalanceMsg::create(0, "USDT".to_string(), 10_000.0));
        let mut usdt_mgrs: HashMap<BasicAccountScope, Rc<RefCell<UsdtBalanceManager>>> =
            HashMap::new();
        usdt_mgrs.insert(
            BasicAccountScope::BitgetUnified,
            Rc::new(RefCell::new(usdt_mgr)),
        );

        let mut latest_account_risk = HashMap::new();
        latest_account_risk.insert(
            BasicAccountScope::BitgetUnified,
            BasicAccountRiskMsg::create(0, 57_000.0, 62_000.0, 1_000.0, 2_000.0, 62.0, 0.0, 0.0),
        );

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::BitgetMargin,
            hedge_venue: TradingVenue::BitgetFutures,
            arb_mode: ArbMode::IntraArb,
            binance_account_mode: Some(BinanceAccountMode::Unified),
            hyperliquid_account_mode: None,
            open_leg,
            hedge_leg,
            usdt_mgrs,
            price_table: Rc::new(RefCell::new(price_table)),
            venue_min_qty_tables: HashMap::new(),
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk,
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(false),
        };

        let state = MonitorChannel::compute_basic_state(&inner);
        assert!((state.total_equity_usdt - 62_000.0).abs() < 1e-9);
    }

    #[test]
    fn gate_intra_total_equity_uses_account_risk_actual_equity() {
        let mut gate_bal = BasicBalanceManager::new(Exchange::Gate);
        gate_bal.apply_balance(&BasicBalanceMsg::create(0, "BTC".to_string(), 1.0));
        let open_leg = LegMgr::Margin {
            bal: Rc::new(RefCell::new(gate_bal)),
        };
        let hedge_leg = LegMgr::Futures {
            exchange: Exchange::Gate,
            um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Gate))),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Gate))),
        };

        let mut price_table = PriceTable::new();
        price_table.update_mark_price("BTC_USDT", 50_000.0, 0);

        let mut usdt_mgr = UsdtBalanceManager::new(Exchange::Gate);
        usdt_mgr.apply_balance(&BasicBalanceMsg::create(0, "USDT".to_string(), 10_000.0));
        let mut usdt_mgrs: HashMap<BasicAccountScope, Rc<RefCell<UsdtBalanceManager>>> =
            HashMap::new();
        usdt_mgrs.insert(
            BasicAccountScope::GateUnified,
            Rc::new(RefCell::new(usdt_mgr)),
        );

        let mut latest_account_risk = HashMap::new();
        latest_account_risk.insert(
            BasicAccountScope::GateUnified,
            BasicAccountRiskMsg::create(0, 56_000.0, 63_000.0, 1_000.0, 2_000.0, 63.0, 0.0, 0.0),
        );

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::GateMargin,
            hedge_venue: TradingVenue::GateFutures,
            arb_mode: ArbMode::IntraArb,
            binance_account_mode: Some(BinanceAccountMode::Unified),
            hyperliquid_account_mode: None,
            open_leg,
            hedge_leg,
            usdt_mgrs,
            price_table: Rc::new(RefCell::new(price_table)),
            venue_min_qty_tables: HashMap::new(),
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk,
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(false),
        };

        let state = MonitorChannel::compute_basic_state(&inner);
        assert!((state.total_equity_usdt - 63_000.0).abs() < 1e-9);
    }

    #[test]
    fn exec_position_imbalance_projection_uses_cross_section_net_values() {
        install_binance_exec_cross_section_fixture();

        let projection = MonitorChannel::with_inner(|inner| {
            MonitorChannel::exec_position_imbalance_projection_inner(
                inner,
                "FILUSDT",
                TradingVenue::BinanceFutures,
                -1.0,
                0.1,
            )
        })
        .unwrap()
        .expect("projection");
        assert!((projection.current_long_usdt - 200.0).abs() < 1e-9);
        assert!((projection.current_short_usdt - 100.0).abs() < 1e-9);
        assert!((projection.next_long_usdt - 100.0).abs() < 1e-9);
        assert!((projection.next_short_usdt - 100.0).abs() < 1e-9);
        assert!((projection.current_imbalance_ratio - (1.0 / 3.0)).abs() < 1e-9);
        assert!((projection.next_imbalance_ratio - 0.0).abs() < 1e-9);
    }

    #[test]
    fn exec_position_imbalance_risk_rejects_when_ratio_expands_over_limit() {
        install_binance_arb_hedge_exposure_fixture(1.0, -1.0, false);

        let projection = MonitorChannel::with_inner(|inner| {
            MonitorChannel::exec_position_imbalance_projection_inner(
                inner,
                "FILUSDT",
                TradingVenue::BinanceFutures,
                -1.0,
                0.5,
            )
        })
        .unwrap()
        .expect("projection");
        let err =
            MonitorChannel::evaluate_exec_position_imbalance_projection("FILUSDT", projection)
                .unwrap_err();
        assert!(err.contains("Exec 截面持仓失衡比例扩大后超限"), "err={err}");
    }

    #[test]
    fn exec_position_imbalance_risk_allows_ratio_reducing_when_current_over_limit() {
        install_binance_arb_hedge_exposure_fixture(2.0, -1.0, false);

        let projection = MonitorChannel::with_inner(|inner| {
            MonitorChannel::exec_position_imbalance_projection_inner(
                inner,
                "FILUSDT",
                TradingVenue::BinanceFutures,
                -1.0,
                0.2,
            )
        })
        .unwrap()
        .expect("projection");
        MonitorChannel::evaluate_exec_position_imbalance_projection("FILUSDT", projection).unwrap();
    }

    #[test]
    fn arb_hedge_exposure_risk_allows_reducing_when_current_over_limit() {
        install_binance_arb_hedge_exposure_fixture(20.0, 0.0, false);

        assert!(MonitorChannel::instance()
            .check_arb_hedge_exposure_risk("FILUSDT", TradingVenue::BinanceFutures, -5.0)
            .is_ok());
    }

    #[test]
    fn arb_hedge_exposure_risk_rejects_expanding_when_current_over_limit() {
        install_binance_arb_hedge_exposure_fixture(20.0, 0.0, false);

        let err = MonitorChannel::instance()
            .check_arb_hedge_exposure_risk("FILUSDT", TradingVenue::BinanceFutures, 5.0)
            .unwrap_err();
        assert!(err.contains("单币敞口扩大且当前已超限"), "err={err}");
    }

    #[test]
    fn arb_hedge_exposure_risk_allows_expanding_when_current_within_limit() {
        install_binance_arb_hedge_exposure_fixture(1.0, 0.0, false);

        assert!(MonitorChannel::instance()
            .check_arb_hedge_exposure_risk("FILUSDT", TradingVenue::BinanceFutures, 5.0)
            .is_ok());
    }

    #[test]
    fn arb_startup_net_gate_checks_small_net_without_pending_write() {
        let strategy_mgr = install_binance_arb_hedge_exposure_fixture(1.0, 0.0, true);

        MonitorChannel::instance()
            .mark_arb_startup_net_seen_for_venue(TradingVenue::BinanceMargin, "test-open");
        MonitorChannel::instance()
            .mark_arb_startup_net_seen_for_venue(TradingVenue::BinanceFutures, "test-hedge");

        assert!(
            MonitorChannel::instance()
                .arb_startup_net_gate_status()
                .ready
        );
        assert_eq!(strategy_mgr.borrow().len(), 0);
    }

    #[test]
    fn arb_startup_net_gate_warns_and_releases_when_net_exposure_over_500u() {
        let strategy_mgr = install_binance_arb_hedge_exposure_fixture(6.0, 0.0, true);

        MonitorChannel::instance()
            .mark_arb_startup_net_seen_for_venue(TradingVenue::BinanceMargin, "test-open");
        MonitorChannel::instance()
            .mark_arb_startup_net_seen_for_venue(TradingVenue::BinanceFutures, "test-hedge");

        assert!(
            MonitorChannel::instance()
                .arb_startup_net_gate_status()
                .ready
        );
        assert_eq!(strategy_mgr.borrow().len(), 0);
    }

    #[test]
    fn symbol_exposure_risk_reads_cached_basic_state_until_refresh() {
        let (strategy_mgr, open_bal) = install_binance_arb_margin_open_fixture();
        assert_eq!(strategy_mgr.borrow().len(), 0);

        MonitorChannel::refresh_basic_state_cache();
        assert!(MonitorChannel::instance()
            .check_symbol_exposure("FILUSDT")
            .is_ok());

        open_bal
            .borrow_mut()
            .apply_balance(&BasicBalanceMsg::create(0, "FIL".to_string(), 20.0));
        MonitorChannel::mark_basic_state_dirty();

        assert!(MonitorChannel::instance()
            .check_symbol_exposure("FILUSDT")
            .is_ok());

        MonitorChannel::refresh_basic_state_cache();
        let err = MonitorChannel::instance()
            .check_symbol_exposure("FILUSDT")
            .unwrap_err();
        assert!(err.contains("敞口比例超过限制"), "err={err}");
    }

    #[test]
    fn symbol_exposure_risk_uses_cached_usdt_value_until_refresh() {
        let (_, open_bal) = install_binance_arb_margin_open_fixture();
        open_bal
            .borrow_mut()
            .apply_balance(&BasicBalanceMsg::create(0, "FIL".to_string(), 1.0));

        MonitorChannel::refresh_basic_state_cache();
        assert!(MonitorChannel::instance()
            .check_symbol_exposure("FILUSDT")
            .is_ok());

        MonitorChannel::with_inner(|inner| {
            inner
                .price_table
                .borrow_mut()
                .update_mark_price("FILUSDT", 1_000.0, 1);
        });
        MonitorChannel::mark_basic_state_dirty();

        assert!(MonitorChannel::instance()
            .check_symbol_exposure("FILUSDT")
            .is_ok());

        MonitorChannel::refresh_basic_state_cache();
        let err = MonitorChannel::instance()
            .check_symbol_exposure("FILUSDT")
            .unwrap_err();
        assert!(err.contains("敞口比例超过限制"), "err={err}");
    }

    #[test]
    fn small_symbol_net_exposure_skips_symbol_risk_log_below_100u() {
        let (_, open_bal) = install_binance_arb_margin_open_fixture();
        open_bal
            .borrow_mut()
            .apply_balance(&BasicBalanceMsg::create(0, "FIL".to_string(), 0.5));
        MonitorChannel::refresh_basic_state_cache();

        assert_eq!(
            MonitorChannel::try_abs_symbol_net_exposure_usdt("FILUSDT"),
            Some(50.0)
        );
        assert!(MonitorChannel::should_skip_small_symbol_exposure_risk_log(
            "FILUSDT"
        ));

        open_bal
            .borrow_mut()
            .apply_balance(&BasicBalanceMsg::create(1, "FIL".to_string(), 1.5));
        MonitorChannel::mark_basic_state_dirty();
        MonitorChannel::refresh_basic_state_cache();

        assert_eq!(
            MonitorChannel::try_abs_symbol_net_exposure_usdt("FILUSDT"),
            Some(150.0)
        );
        assert!(!MonitorChannel::should_skip_small_symbol_exposure_risk_log(
            "FILUSDT"
        ));
    }

    #[test]
    fn price_dirty_refresh_revalues_cached_exposure_without_full_recompute() {
        let (_, open_bal) = install_binance_arb_margin_open_fixture();
        open_bal
            .borrow_mut()
            .apply_balance(&BasicBalanceMsg::create(0, "FIL".to_string(), 1.0));
        MonitorChannel::refresh_basic_state_cache();

        open_bal
            .borrow_mut()
            .apply_balance(&BasicBalanceMsg::create(1, "FIL".to_string(), 20.0));
        MonitorChannel::with_inner(|inner| {
            inner
                .price_table
                .borrow_mut()
                .update_mark_price("FILUSDT", 200.0, 1);
        });
        MonitorChannel::mark_basic_state_price_dirty();

        assert!(MonitorChannel::refresh_basic_state_price_cache());
        let (exposures, _equity, abs_total_exposure, _position, _upl) =
            MonitorChannel::instance().basic_state_snapshot();

        assert_eq!(exposures.get("FIL").copied(), Some((1.0, 0.0)));
        assert!((abs_total_exposure - 200.0).abs() < 1e-12);
    }

    #[test]
    fn arb_open_margin_net_risk_cancel_targets_same_direction_open_strategies() {
        let (strategy_mgr, open_bal) = install_binance_arb_margin_open_fixture();
        strategy_mgr
            .borrow_mut()
            .insert(Box::new(TestMmOpenStrategy::new(
                201,
                "FILUSDT",
                Side::Buy,
                201_0001,
            )));
        strategy_mgr
            .borrow_mut()
            .insert(Box::new(TestMmOpenStrategy::new(
                202,
                "FILUSDT",
                Side::Sell,
                202_0001,
            )));
        strategy_mgr
            .borrow_mut()
            .insert(Box::new(TestMmOpenStrategy::new(
                203,
                "BTCUSDT",
                Side::Buy,
                203_0001,
            )));

        open_bal
            .borrow_mut()
            .apply_balance(&BasicBalanceMsg::create(0, "FIL".to_string(), 20.0));
        MonitorChannel::refresh_basic_state_cache();
        MonitorChannel::instance().handle_arb_open_margin_net_risk_after_update("FIL", 0);

        let mut mgr = strategy_mgr.borrow_mut();
        let buy = mgr
            .take(201)
            .expect("buy strategy")
            .as_any()
            .downcast_ref::<TestMmOpenStrategy>()
            .expect("buy strategy type")
            .arb_cancel_trigger_count;
        let sell = mgr
            .take(202)
            .expect("sell strategy")
            .as_any()
            .downcast_ref::<TestMmOpenStrategy>()
            .expect("sell strategy type")
            .arb_cancel_trigger_count;
        let other = mgr
            .take(203)
            .expect("other strategy")
            .as_any()
            .downcast_ref::<TestMmOpenStrategy>()
            .expect("other strategy type")
            .arb_cancel_trigger_count;

        assert_eq!(buy, 1);
        assert_eq!(sell, 0);
        assert_eq!(other, 0);
    }

    #[test]
    fn arb_open_margin_borrow_interest_risk_cancel_targets_sell_side_when_net_short() {
        let (strategy_mgr, open_bal) = install_binance_arb_margin_open_fixture();
        strategy_mgr
            .borrow_mut()
            .insert(Box::new(TestMmOpenStrategy::new(
                211,
                "FILUSDT",
                Side::Buy,
                211_0001,
            )));
        strategy_mgr
            .borrow_mut()
            .insert(Box::new(TestMmOpenStrategy::new(
                212,
                "FILUSDT",
                Side::Sell,
                212_0001,
            )));

        open_bal
            .borrow_mut()
            .apply_balance(&BasicBalanceMsg::create(0, "FIL".to_string(), 0.0));
        open_bal
            .borrow_mut()
            .apply_borrow_interest(&BasicBorrowInterestMsg::create(
                0,
                "FIL".to_string(),
                20.0,
                0.0,
            ));
        MonitorChannel::refresh_basic_state_cache();
        MonitorChannel::instance().handle_arb_open_margin_net_risk_after_update("FIL", 0);

        let mut mgr = strategy_mgr.borrow_mut();
        let buy = mgr
            .take(211)
            .expect("buy strategy")
            .as_any()
            .downcast_ref::<TestMmOpenStrategy>()
            .expect("buy strategy type")
            .arb_cancel_trigger_count;
        let sell = mgr
            .take(212)
            .expect("sell strategy")
            .as_any()
            .downcast_ref::<TestMmOpenStrategy>()
            .expect("sell strategy type")
            .arb_cancel_trigger_count;

        assert_eq!(buy, 0);
        assert_eq!(sell, 1);
    }

    #[test]
    fn mm_position_risk_cancel_targets_open_strategies_by_side() {
        let mut um_mgr = BasicUmManager::new(Exchange::Binance);
        um_mgr.apply_position(&BasicPositionMsg::create(
            0,
            "FILUSDT".to_string(),
            'L',
            20.0,
        ));

        let open_leg = LegMgr::Futures {
            exchange: Exchange::Binance,
            um: Rc::new(RefCell::new(um_mgr)),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Binance))),
        };
        let hedge_leg = LegMgr::Futures {
            exchange: Exchange::Binance,
            um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Binance))),
            min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Binance))),
        };

        let mut price_table = PriceTable::new();
        price_table.update_mark_price("FILUSDT", 100.0, 0);

        let strategy_mgr = Rc::new(RefCell::new(StrategyManager::new()));
        strategy_mgr
            .borrow_mut()
            .insert(Box::new(TestMmOpenStrategy::new(
                101,
                "FILUSDT",
                Side::Buy,
                101_0001,
            )));
        strategy_mgr
            .borrow_mut()
            .insert(Box::new(TestMmOpenStrategy::new(
                102,
                "FILUSDT",
                Side::Sell,
                102_0001,
            )));
        strategy_mgr
            .borrow_mut()
            .insert(Box::new(TestMmOpenStrategy::new(
                103,
                "BTCUSDT",
                Side::Buy,
                103_0001,
            )));

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::BinanceFutures,
            hedge_venue: TradingVenue::BinanceFutures,
            arb_mode: ArbMode::CrossArb,
            binance_account_mode: Some(BinanceAccountMode::Unified),
            hyperliquid_account_mode: None,
            open_leg,
            hedge_leg,
            usdt_mgrs: HashMap::new(),
            price_table: Rc::new(RefCell::new(price_table)),
            venue_min_qty_tables: HashMap::new(),
            strategy_mgr: strategy_mgr.clone(),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk: HashMap::new(),
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(false),
        };

        MonitorChannel::clear_basic_state_runtime_cache();
        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });
        MonitorChannel::refresh_basic_state_cache();

        MonitorChannel::instance().handle_mm_position_risk_after_update("FILUSDT", 0);

        let mut mgr = strategy_mgr.borrow_mut();
        let buy = mgr
            .take(101)
            .expect("buy strategy")
            .as_any()
            .downcast_ref::<TestMmOpenStrategy>()
            .expect("buy strategy type")
            .cancel_trigger_count;
        let sell = mgr
            .take(102)
            .expect("sell strategy")
            .as_any()
            .downcast_ref::<TestMmOpenStrategy>()
            .expect("sell strategy type")
            .cancel_trigger_count;
        let other = mgr
            .take(103)
            .expect("other strategy")
            .as_any()
            .downcast_ref::<TestMmOpenStrategy>()
            .expect("other strategy type")
            .cancel_trigger_count;

        assert_eq!(buy, 1);
        assert_eq!(sell, 0);
        assert_eq!(other, 0);
    }

    #[test]
    fn binance_intra_derivatives_service_uses_direct_dat_pbs() {
        assert_eq!(
            MonitorChannel::derivatives_service_for_mark_price_source(
                TradingVenue::BinanceMargin,
                TradingVenue::BinanceFutures,
                ArbMode::IntraArb,
            ),
            "dat_pbs/binance-futures/derivatives"
        );
    }

    #[test]
    fn pre_trade_derivatives_services_use_direct_dat_pbs() {
        assert_eq!(
            MonitorChannel::derivatives_service_for_mark_price_source(
                TradingVenue::BinanceMargin,
                TradingVenue::BinanceFutures,
                ArbMode::FundingArb,
            ),
            "dat_pbs/binance-futures/derivatives"
        );
        assert_eq!(
            MonitorChannel::derivatives_service_for_mark_price_source(
                TradingVenue::BinanceFutures,
                TradingVenue::GateFutures,
                ArbMode::CrossArb,
            ),
            "dat_pbs/gate-futures/derivatives"
        );
        assert_eq!(
            MonitorChannel::derivatives_service_for_mark_price_source(
                TradingVenue::GateMargin,
                TradingVenue::GateFutures,
                ArbMode::IntraArb,
            ),
            "dat_pbs/gate-futures/derivatives"
        );
        assert_eq!(
            MonitorChannel::derivatives_service_for_mark_price_source(
                TradingVenue::BitgetMargin,
                TradingVenue::BitgetFutures,
                ArbMode::IntraArb,
            ),
            "dat_pbs/bitget-futures/derivatives"
        );
    }

    #[test]
    fn mark_price_source_uses_okex_when_both_venues_are_okex() {
        assert_eq!(
            MonitorChannel::mark_price_exchange_for_venues(
                TradingVenue::OkexMargin,
                TradingVenue::OkexFutures,
            ),
            Exchange::Okex
        );
        assert_eq!(
            MonitorChannel::derivatives_service_for_mark_price_source(
                TradingVenue::OkexMargin,
                TradingVenue::OkexFutures,
                ArbMode::IntraArb,
            ),
            OKEX_DERIVATIVES_SERVICE
        );
    }

    #[test]
    fn mark_price_source_uses_bybit_when_both_venues_are_bybit() {
        assert_eq!(
            MonitorChannel::mark_price_exchange_for_venues(
                TradingVenue::BybitMargin,
                TradingVenue::BybitFutures,
            ),
            Exchange::Bybit
        );
        assert_eq!(
            MonitorChannel::derivatives_service_for_mark_price_source(
                TradingVenue::BybitMargin,
                TradingVenue::BybitFutures,
                ArbMode::IntraArb,
            ),
            BYBIT_DERIVATIVES_SERVICE
        );
    }

    #[test]
    fn trade_update_lite_enabled_for_binance_bybit_okex_and_bitget_intra_only() {
        assert!(trade_update_lite_enabled_for_venues(
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        ));
        assert!(trade_update_lite_enabled_for_venues(
            TradingVenue::BybitMargin,
            TradingVenue::BybitFutures,
        ));
        assert!(trade_update_lite_enabled_for_venues(
            TradingVenue::OkexMargin,
            TradingVenue::OkexFutures,
        ));
        assert!(trade_update_lite_enabled_for_venues(
            TradingVenue::BitgetMargin,
            TradingVenue::BitgetFutures,
        ));
        assert!(!trade_update_lite_enabled_for_venues(
            TradingVenue::OkexMargin,
            TradingVenue::BybitFutures,
        ));
        assert!(!trade_update_lite_enabled_for_venues(
            TradingVenue::GateMargin,
            TradingVenue::GateFutures,
        ));
    }

    #[test]
    fn mark_price_source_uses_gate_when_both_venues_are_gate() {
        assert_eq!(
            MonitorChannel::mark_price_exchange_for_venues(
                TradingVenue::GateMargin,
                TradingVenue::GateFutures,
            ),
            Exchange::Gate
        );
        assert_eq!(
            MonitorChannel::derivatives_service_for_mark_price_source(
                TradingVenue::GateMargin,
                TradingVenue::GateFutures,
                ArbMode::IntraArb,
            ),
            GATE_DERIVATIVES_SERVICE
        );
    }

    #[test]
    fn mark_price_source_uses_bitget_when_both_venues_are_bitget() {
        assert_eq!(
            MonitorChannel::mark_price_exchange_for_venues(
                TradingVenue::BitgetMargin,
                TradingVenue::BitgetFutures,
            ),
            Exchange::Bitget
        );
        assert_eq!(
            MonitorChannel::derivatives_service_for_mark_price_source(
                TradingVenue::BitgetMargin,
                TradingVenue::BitgetFutures,
                ArbMode::IntraArb,
            ),
            BITGET_DERIVATIVES_SERVICE
        );
    }

    #[test]
    fn mark_price_source_uses_hedge_exchange_for_cross_futures_pair() {
        assert_eq!(
            MonitorChannel::mark_price_exchange_for_venues(
                TradingVenue::BitgetFutures,
                TradingVenue::GateFutures,
            ),
            Exchange::Gate
        );
        assert_eq!(
            MonitorChannel::derivatives_service_for_mark_price_source(
                TradingVenue::BitgetFutures,
                TradingVenue::GateFutures,
                ArbMode::CrossArb,
            ),
            GATE_DERIVATIVES_SERVICE
        );
    }

    #[test]
    fn mark_price_source_uses_hedge_exchange_for_reversed_cross_futures_pair() {
        assert_eq!(
            MonitorChannel::mark_price_exchange_for_venues(
                TradingVenue::GateFutures,
                TradingVenue::BitgetFutures,
            ),
            Exchange::Bitget
        );
        assert_eq!(
            MonitorChannel::derivatives_service_for_mark_price_source(
                TradingVenue::GateFutures,
                TradingVenue::BitgetFutures,
                ArbMode::CrossArb,
            ),
            BITGET_DERIVATIVES_SERVICE
        );
    }

    #[test]
    fn bitget_margin_align_order_by_venue_uses_base_qty_filters() {
        let mut bitget_table = VenueMinQtyTable::new(TradingVenue::BitgetMargin);
        bitget_table.set_entry_for_test(MinQtyEntry {
            symbol: "XRPUSDT".to_string(),
            base_asset: "XRP".to_string(),
            quote_asset: "USDT".to_string(),
            min_qty: 10.0,
            step_size: 0.1,
            price_tick: Some(0.0001),
            min_notional: Some(5.0),
        });

        let mut venue_min_qty_tables: HashMap<TradingVenue, Rc<VenueMinQtyTable>> = HashMap::new();
        venue_min_qty_tables.insert(TradingVenue::BitgetMargin, Rc::new(bitget_table));

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::BitgetMargin,
            hedge_venue: TradingVenue::BitgetFutures,
            arb_mode: ArbMode::IntraArb,
            binance_account_mode: Some(BinanceAccountMode::Unified),
            hyperliquid_account_mode: None,
            open_leg: LegMgr::Margin {
                bal: Rc::new(RefCell::new(BasicBalanceManager::new(Exchange::Bitget))),
            },
            hedge_leg: LegMgr::Futures {
                exchange: Exchange::Bitget,
                um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Bitget))),
                min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Bitget))),
            },
            usdt_mgrs: HashMap::new(),
            price_table: Rc::new(RefCell::new(PriceTable::new())),
            venue_min_qty_tables,
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk: HashMap::new(),
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(false),
        };

        MonitorChannel::clear_basic_state_runtime_cache();
        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });

        let (qty, price) = MonitorChannel::instance()
            .align_order_by_venue(TradingVenue::BitgetMargin, "XRPUSDT", 44.37, 1.13087)
            .expect("bitget margin align");
        assert!((qty - 44.3).abs() < 1e-9, "qty={qty}");
        assert!((price - 1.1308).abs() < 1e-9, "price={price}");
    }

    #[test]
    fn bitget_futures_align_order_by_venue_enforces_min_notional() {
        let mut bitget_table = VenueMinQtyTable::new(TradingVenue::BitgetFutures);
        bitget_table.set_entry_for_test(MinQtyEntry {
            symbol: "XRPUSDT".to_string(),
            base_asset: "XRP".to_string(),
            quote_asset: "USDT".to_string(),
            min_qty: 10.0,
            step_size: 0.1,
            price_tick: Some(0.0001),
            min_notional: Some(50.0),
        });
        bitget_table.set_contract_multiplier_for_test("XRPUSDT", 1.0);

        let mut venue_min_qty_tables: HashMap<TradingVenue, Rc<VenueMinQtyTable>> = HashMap::new();
        venue_min_qty_tables.insert(TradingVenue::BitgetFutures, Rc::new(bitget_table));

        let inner = MonitorChannelInner {
            open_venue: TradingVenue::BitgetMargin,
            hedge_venue: TradingVenue::BitgetFutures,
            arb_mode: ArbMode::IntraArb,
            binance_account_mode: Some(BinanceAccountMode::Unified),
            hyperliquid_account_mode: None,
            open_leg: LegMgr::Margin {
                bal: Rc::new(RefCell::new(BasicBalanceManager::new(Exchange::Bitget))),
            },
            hedge_leg: LegMgr::Futures {
                exchange: Exchange::Bitget,
                um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Bitget))),
                min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Bitget))),
            },
            usdt_mgrs: HashMap::new(),
            price_table: Rc::new(RefCell::new(PriceTable::new())),
            venue_min_qty_tables,
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk: HashMap::new(),
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(false),
        };

        MonitorChannel::clear_basic_state_runtime_cache();
        MONITOR_CHANNEL.with(|mc| {
            *mc.borrow_mut() = Some(inner);
        });

        let (qty, price) = MonitorChannel::instance()
            .align_order_by_venue(TradingVenue::BitgetFutures, "XRPUSDT", 44.37, 1.13087)
            .expect("bitget futures align");
        assert!((qty - 44.3).abs() < 1e-9, "qty={qty}");
        assert!((price - 1.1308).abs() < 1e-9, "price={price}");

        let (qty_bumped, price_bumped) = MonitorChannel::instance()
            .align_order_by_venue(TradingVenue::BitgetFutures, "XRPUSDT", 10.0, 1.13087)
            .expect("bitget futures min notional align");
        assert!((qty_bumped - 44.3).abs() < 1e-9, "qty_bumped={qty_bumped}");
        assert!(
            (price_bumped - 1.1308).abs() < 1e-9,
            "price_bumped={price_bumped}"
        );
    }

    #[test]
    fn bitget_coin_futures_min_notional_uses_face_value() {
        let mut bitget_table = VenueMinQtyTable::new(TradingVenue::BitgetCoinFutures);
        bitget_table.set_entry_for_test(MinQtyEntry {
            symbol: "BTCUSD_CM".to_string(),
            base_asset: "BTC".to_string(),
            quote_asset: "USD".to_string(),
            min_qty: 1.0,
            step_size: 1.0,
            price_tick: Some(0.1),
            min_notional: Some(5.0),
        });
        bitget_table.set_contract_multiplier_for_test("BTCUSD_CM", 1.0);

        let mut venue_min_qty_tables: HashMap<TradingVenue, Rc<VenueMinQtyTable>> = HashMap::new();
        venue_min_qty_tables.insert(TradingVenue::BitgetCoinFutures, Rc::new(bitget_table));
        let inner = MonitorChannelInner {
            open_venue: TradingVenue::BitgetMargin,
            hedge_venue: TradingVenue::BitgetCoinFutures,
            arb_mode: ArbMode::IntraArb,
            binance_account_mode: Some(BinanceAccountMode::Unified),
            hyperliquid_account_mode: None,
            open_leg: LegMgr::Margin {
                bal: Rc::new(RefCell::new(BasicBalanceManager::new(Exchange::Bitget))),
            },
            hedge_leg: LegMgr::Futures {
                exchange: Exchange::Bitget,
                um: Rc::new(RefCell::new(BasicUmManager::new(Exchange::Bitget))),
                min_qty_table: Rc::new(RefCell::new(MinQtyTable::new(Exchange::Bitget))),
            },
            usdt_mgrs: HashMap::new(),
            price_table: Rc::new(RefCell::new(PriceTable::new())),
            venue_min_qty_tables,
            strategy_mgr: Rc::new(RefCell::new(StrategyManager::new())),
            orphan_strategy_mgr: Rc::new(RefCell::new(OrphanStrategyManager::new())),
            order_manager: Rc::new(RefCell::new(OrderManager::new(Some(
                BinanceAccountMode::Unified,
            )))),
            close_inventory: Rc::new(RefCell::new(CloseInventoryLedger::new())),
            trade_update_seq: 0,
            latest_account_risk: HashMap::new(),
            latest_binance_std_um_wallet: None,
            arb_startup_net_gate: ArbStartupNetGate::new(false),
        };
        MonitorChannel::clear_basic_state_runtime_cache();
        MONITOR_CHANNEL.with(|mc| *mc.borrow_mut() = Some(inner));

        let (contracts, price) = MonitorChannel::instance()
            .align_order_by_venue(
                TradingVenue::BitgetCoinFutures,
                "BTCUSDT",
                0.00002,
                50_000.0,
            )
            .expect("bitget coin futures align");
        assert_eq!(contracts, 5.0);
        assert_eq!(price, 50_000.0);
    }
}
