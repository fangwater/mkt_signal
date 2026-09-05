use anyhow::{Context, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use runtime_common::fast_hash::{
    fast_hash_map, fast_hash_set, fast_hash_set_from_iter, FastHashMap, FastHashSet,
};
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::{hash_map::Entry, BTreeMap};
use std::time::{Duration, Instant};

use depth_pub_common::query_msg::{price_to_tick_index, TLEN_QUERY_AMOUNT_EMPTY};
use mkt_parsers::msg::mkt_msg::{get_msg_type, MktMsgType};
use mkt_parsers::symbol_match::normalize_symbol_for_whitelist;
use order_common::TradingVenue;
use queue_position_engine::{BookSide, Side};
use signal_common::venue_min_qty_table::VenueMinQtyTable;

use super::decision_router::{decision_branch, DecisionBranch};
use super::symbol_list::SymbolList;

const INC_PAYLOAD: usize = 2048;
const LOCAL_TLEN_MODE_ENV: &str = "TRADE_SIGNAL_TLEN_QUERY_MODE";
const QUEUE_POSITION_ENABLED_ENV: &str = "TRADE_SIGNAL_ENABLE_QUEUE_POSITION";
const ONLINE_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const STATS_LOG_INTERVAL: Duration = Duration::from_secs(10);
const INC_DRAIN_BUDGET: usize = 2048;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlenQueryMode {
    Remote,
    Local,
}

#[derive(Debug, Clone, Copy, Default)]
struct LevelEntry {
    amount: f64,
    update_id: i64,
}

#[derive(Debug, Clone, Copy, Default)]
struct BboEntry {
    bid_tick: Option<i64>,
    bid_amount: f64,
    ask_tick: Option<i64>,
    ask_amount: f64,
    timestamp_us: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SymbolScope {
    Online,
    All,
}

impl SymbolScope {
    fn label(self) -> &'static str {
        match self {
            Self::Online => "online",
            Self::All => "all",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HyperliquidSnapshotGeneration {
    first_update_id: i64,
    final_update_id: i64,
    timestamp: i64,
}

#[derive(Debug)]
struct HyperliquidSnapshotChunk {
    is_last: bool,
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
}

impl HyperliquidSnapshotChunk {
    fn matches(&self, other: &Self) -> bool {
        self.is_last == other.is_last && self.bids == other.bids && self.asks == other.asks
    }
}

#[derive(Debug)]
struct PendingHyperliquidSnapshot {
    generation: HyperliquidSnapshotGeneration,
    chunks: BTreeMap<u8, HyperliquidSnapshotChunk>,
    last_chunk_index: Option<u8>,
}

impl PendingHyperliquidSnapshot {
    fn new(generation: HyperliquidSnapshotGeneration) -> Self {
        Self {
            generation,
            chunks: BTreeMap::new(),
            last_chunk_index: None,
        }
    }

    fn is_complete(&self) -> bool {
        let Some(last_chunk_index) = self.last_chunk_index else {
            return false;
        };
        self.chunks.len() == usize::from(last_chunk_index) + 1
            && (0..=last_chunk_index).all(|index| self.chunks.contains_key(&index))
    }

    fn into_complete(self) -> CompleteHyperliquidSnapshot {
        let total_bids = self.chunks.values().map(|chunk| chunk.bids.len()).sum();
        let total_asks = self.chunks.values().map(|chunk| chunk.asks.len()).sum();
        let mut bids = Vec::with_capacity(total_bids);
        let mut asks = Vec::with_capacity(total_asks);
        for (_, chunk) in self.chunks {
            bids.extend(chunk.bids);
            asks.extend(chunk.asks);
        }
        CompleteHyperliquidSnapshot {
            final_update_id: self.generation.final_update_id,
            bids,
            asks,
        }
    }
}

#[derive(Debug)]
struct CompleteHyperliquidSnapshot {
    final_update_id: i64,
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
}

#[derive(Debug)]
enum HyperliquidSnapshotAssembly {
    Pending,
    Complete(CompleteHyperliquidSnapshot),
    Conflict,
}

#[derive(Debug, Default)]
struct HyperliquidSnapshotAssembler {
    pending: Option<PendingHyperliquidSnapshot>,
    last_applied_final_update_id: Option<i64>,
    rejected_through_final_update_id: Option<i64>,
}

impl HyperliquidSnapshotAssembler {
    #[allow(clippy::too_many_arguments)]
    fn add_chunk(
        &mut self,
        first_update_id: i64,
        final_update_id: i64,
        timestamp: i64,
        chunk_index: u8,
        is_last: bool,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>,
    ) -> HyperliquidSnapshotAssembly {
        if self
            .last_applied_final_update_id
            .is_some_and(|last_applied| final_update_id <= last_applied)
            || self
                .rejected_through_final_update_id
                .is_some_and(|rejected_through| final_update_id <= rejected_through)
        {
            return HyperliquidSnapshotAssembly::Pending;
        }

        let generation = HyperliquidSnapshotGeneration {
            first_update_id,
            final_update_id,
            timestamp,
        };
        match self.pending.as_ref() {
            Some(pending) if final_update_id < pending.generation.final_update_id => {
                return HyperliquidSnapshotAssembly::Pending;
            }
            Some(pending) if final_update_id > pending.generation.final_update_id => {
                self.pending = Some(PendingHyperliquidSnapshot::new(generation));
            }
            Some(pending) if pending.generation != generation => {
                return self.reject_generation(final_update_id);
            }
            Some(_) => {}
            None => self.pending = Some(PendingHyperliquidSnapshot::new(generation)),
        }

        let incoming = HyperliquidSnapshotChunk {
            is_last,
            bids,
            asks,
        };
        let pending = self
            .pending
            .as_ref()
            .expect("Hyperliquid pending snapshot initialized");
        if let Some(existing) = pending.chunks.get(&chunk_index) {
            return if existing.matches(&incoming) {
                HyperliquidSnapshotAssembly::Pending
            } else {
                self.reject_generation(final_update_id)
            };
        }

        let invalid_layout = if is_last {
            pending
                .last_chunk_index
                .is_some_and(|existing| existing != chunk_index)
                || pending.chunks.keys().any(|index| *index > chunk_index)
        } else {
            pending
                .last_chunk_index
                .is_some_and(|last_chunk_index| chunk_index >= last_chunk_index)
        };
        if invalid_layout {
            return self.reject_generation(final_update_id);
        }

        let pending = self
            .pending
            .as_mut()
            .expect("Hyperliquid pending snapshot initialized");
        if is_last {
            pending.last_chunk_index = Some(chunk_index);
        }
        pending.chunks.insert(chunk_index, incoming);
        if !pending.is_complete() {
            return HyperliquidSnapshotAssembly::Pending;
        }

        let complete = self
            .pending
            .take()
            .expect("complete Hyperliquid snapshot must exist")
            .into_complete();
        self.last_applied_final_update_id = Some(complete.final_update_id);
        HyperliquidSnapshotAssembly::Complete(complete)
    }

    fn reject_generation(&mut self, final_update_id: i64) -> HyperliquidSnapshotAssembly {
        self.pending = None;
        self.rejected_through_final_update_id = Some(
            self.rejected_through_final_update_id
                .map_or(final_update_id, |current| current.max(final_update_id)),
        );
        HyperliquidSnapshotAssembly::Conflict
    }
}

#[derive(Debug, Default)]
struct SymbolTlenCache {
    price_tick: Option<f64>,
    amount_scale: f64,
    bids: FastHashMap<i64, LevelEntry>,
    asks: FastHashMap<i64, LevelEntry>,
    bbo: BboEntry,
    hyperliquid_snapshots: HyperliquidSnapshotAssembler,
}

#[derive(Debug)]
struct LocalTlenStore {
    venue: TradingVenue,
    table: VenueMinQtyTable,
    symbol_scope: SymbolScope,
    online_symbols: FastHashSet<String>,
    symbols: FastHashMap<String, SymbolTlenCache>,
    last_online_refresh: Instant,
    inc_updates: u64,
    bbo_updates: u64,
    query_count: u64,
    query_missing_count: u64,
    query_bbo_hit_count: u64,
    last_stats_log: Instant,
}

impl LocalTlenStore {
    fn new(venue: TradingVenue, table: VenueMinQtyTable, symbol_scope: SymbolScope) -> Self {
        let online_symbols = match symbol_scope {
            SymbolScope::Online => load_online_symbol_set(),
            SymbolScope::All => fast_hash_set(),
        };
        Self {
            venue,
            table,
            symbol_scope,
            online_symbols,
            symbols: fast_hash_map(),
            last_online_refresh: Instant::now(),
            inc_updates: 0,
            bbo_updates: 0,
            query_count: 0,
            query_missing_count: 0,
            query_bbo_hit_count: 0,
            last_stats_log: Instant::now(),
        }
    }

    fn maybe_refresh_online_symbols(&mut self) {
        if matches!(self.symbol_scope, SymbolScope::All) {
            return;
        }
        if self.last_online_refresh.elapsed() < ONLINE_REFRESH_INTERVAL {
            return;
        }
        self.last_online_refresh = Instant::now();

        let online_symbols = load_online_symbol_set();
        self.symbols
            .retain(|symbol, _| online_symbols.contains(symbol));
        self.online_symbols = online_symbols;
    }

    fn maybe_log_stats(&mut self) {
        if self.last_stats_log.elapsed() < STATS_LOG_INTERVAL {
            return;
        }
        info!(
            "local_tlen[{}] stats symbol_scope={} online_symbols={} cached_symbols={} inc_updates={} bbo_updates={} queries={} missing={} bbo_hits={}",
            self.venue.data_pub_slug(),
            self.symbol_scope.label(),
            self.online_symbols.len(),
            self.symbols.len(),
            self.inc_updates,
            self.bbo_updates,
            self.query_count,
            self.query_missing_count,
            self.query_bbo_hit_count
        );
        self.inc_updates = 0;
        self.bbo_updates = 0;
        self.query_count = 0;
        self.query_missing_count = 0;
        self.query_bbo_hit_count = 0;
        self.last_stats_log = Instant::now();
    }

    fn is_online(&self, symbol: &str) -> bool {
        matches!(self.symbol_scope, SymbolScope::All) || self.online_symbols.contains(symbol)
    }

    fn symbol_cache_mut(&mut self, symbol: &str) -> &mut SymbolTlenCache {
        if !self.symbols.contains_key(symbol) {
            let price_tick = price_tick_for_symbol(&self.table, self.venue, symbol);
            let amount_scale = amount_scale_for_symbol(&self.table, self.venue, symbol);
            self.symbols.insert(
                symbol.to_owned(),
                SymbolTlenCache {
                    price_tick,
                    amount_scale,
                    bids: fast_hash_map(),
                    asks: fast_hash_map(),
                    bbo: BboEntry::default(),
                    hyperliquid_snapshots: HyperliquidSnapshotAssembler::default(),
                },
            );
        }
        self.symbols
            .get_mut(symbol)
            .expect("local_tlen symbol cache was just initialized")
    }

    fn apply_bbo(
        &mut self,
        symbol: &str,
        timestamp_us: i64,
        bid_price: f64,
        bid_amount: f64,
        ask_price: f64,
        ask_amount: f64,
    ) {
        if !self.is_online(symbol) {
            return;
        }
        let cache = self.symbol_cache_mut(symbol);
        let Some(price_tick) = cache.price_tick else {
            return;
        };

        cache.bbo = BboEntry {
            bid_tick: price_to_tick_index(bid_price, price_tick),
            bid_amount: bid_amount * cache.amount_scale,
            ask_tick: price_to_tick_index(ask_price, price_tick),
            ask_amount: ask_amount * cache.amount_scale,
            timestamp_us,
        };
        self.bbo_updates = self.bbo_updates.saturating_add(1);
    }

    fn apply_incremental(
        &mut self,
        symbol: &str,
        update_id: i64,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
    ) {
        if !self.is_online(symbol) {
            return;
        }
        let cache = self.symbol_cache_mut(symbol);
        let Some(price_tick) = cache.price_tick else {
            return;
        };
        let amount_scale = cache.amount_scale;

        for &(price, amount) in bids {
            if let Some(tick_index) = price_to_tick_index(price, price_tick) {
                if let Some((old_qty, new_qty)) = apply_level_update(
                    &mut cache.bids,
                    tick_index,
                    amount * amount_scale,
                    update_id,
                ) {
                    super::queue_position::apply_level_update(
                        symbol,
                        BookSide::Bid,
                        tick_index,
                        old_qty,
                        new_qty,
                    );
                }
            }
        }
        for &(price, amount) in asks {
            if let Some(tick_index) = price_to_tick_index(price, price_tick) {
                if let Some((old_qty, new_qty)) = apply_level_update(
                    &mut cache.asks,
                    tick_index,
                    amount * amount_scale,
                    update_id,
                ) {
                    super::queue_position::apply_level_update(
                        symbol,
                        BookSide::Ask,
                        tick_index,
                        old_qty,
                        new_qty,
                    );
                }
            }
        }
        self.inc_updates = self.inc_updates.saturating_add(1);
    }

    fn apply_incremental_message(&mut self, update: IncrementalUpdate<'_>) {
        let IncrementalUpdate {
            symbol,
            first_update_id,
            final_update_id,
            timestamp,
            is_snapshot,
            is_last,
            chunk_index,
            bids,
            asks,
        } = update;
        let symbol = normalize_symbol_key_cow(symbol);
        if is_snapshot
            && matches!(
                self.venue,
                TradingVenue::HyperliquidMargin | TradingVenue::HyperliquidFutures
            )
        {
            self.apply_hyperliquid_snapshot_chunk(
                symbol.as_ref(),
                first_update_id,
                final_update_id,
                timestamp,
                chunk_index,
                is_last,
                bids,
                asks,
            );
        } else {
            self.apply_incremental(symbol.as_ref(), final_update_id, &bids, &asks);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_hyperliquid_snapshot_chunk(
        &mut self,
        symbol: &str,
        first_update_id: i64,
        final_update_id: i64,
        timestamp: i64,
        chunk_index: u8,
        is_last: bool,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>,
    ) {
        if !self.is_online(symbol) {
            return;
        }

        let assembly = {
            let cache = self.symbol_cache_mut(symbol);
            if cache.price_tick.is_none() {
                return;
            }
            cache.hyperliquid_snapshots.add_chunk(
                first_update_id,
                final_update_id,
                timestamp,
                chunk_index,
                is_last,
                bids,
                asks,
            )
        };

        match assembly {
            HyperliquidSnapshotAssembly::Pending => {}
            HyperliquidSnapshotAssembly::Conflict => warn!(
                "local_tlen[{}] rejected conflicting Hyperliquid snapshot symbol={} update_id={}",
                self.venue.data_pub_slug(),
                symbol,
                final_update_id,
            ),
            HyperliquidSnapshotAssembly::Complete(snapshot) => {
                self.replace_hyperliquid_snapshot(symbol, snapshot);
            }
        }
    }

    fn replace_hyperliquid_snapshot(
        &mut self,
        symbol: &str,
        snapshot: CompleteHyperliquidSnapshot,
    ) {
        let (bid_changes, ask_changes) = {
            let cache = self.symbol_cache_mut(symbol);
            let price_tick = cache
                .price_tick
                .expect("Hyperliquid snapshot assembly requires a price tick");
            let new_bids = build_snapshot_side(
                &snapshot.bids,
                price_tick,
                cache.amount_scale,
                snapshot.final_update_id,
            );
            let new_asks = build_snapshot_side(
                &snapshot.asks,
                price_tick,
                cache.amount_scale,
                snapshot.final_update_id,
            );
            let bid_changes = snapshot_level_changes(&cache.bids, &new_bids);
            let ask_changes = snapshot_level_changes(&cache.asks, &new_asks);
            cache.bids = new_bids;
            cache.asks = new_asks;
            (bid_changes, ask_changes)
        };

        for (tick_index, old_qty, new_qty) in bid_changes {
            super::queue_position::apply_level_update(
                symbol,
                BookSide::Bid,
                tick_index,
                old_qty,
                new_qty,
            );
        }
        for (tick_index, old_qty, new_qty) in ask_changes {
            super::queue_position::apply_level_update(
                symbol,
                BookSide::Ask,
                tick_index,
                old_qty,
                new_qty,
            );
        }
        self.inc_updates = self.inc_updates.saturating_add(1);
    }

    fn query_batch(&mut self, symbol: &str, tick_indices: &[i64]) -> Vec<f64> {
        self.query_count = self.query_count.saturating_add(1);
        let Some(cache) = self.symbols.get(symbol) else {
            self.query_missing_count = self.query_missing_count.saturating_add(1);
            return vec![TLEN_QUERY_AMOUNT_EMPTY; tick_indices.len()];
        };

        let mut bbo_hits = 0u64;
        let out = tick_indices
            .iter()
            .map(|tick_index| {
                if let Some(amount) = query_bbo_amount(cache.bbo, *tick_index) {
                    bbo_hits = bbo_hits.saturating_add(1);
                    return amount;
                }
                query_level_amount(&cache.bids, *tick_index)
                    .or_else(|| query_level_amount(&cache.asks, *tick_index))
                    .unwrap_or(TLEN_QUERY_AMOUNT_EMPTY)
            })
            .collect();
        self.query_bbo_hit_count = self.query_bbo_hit_count.saturating_add(bbo_hits);
        out
    }
}

enum LocalTlenRuntime {
    Uninitialized,
    Remote,
    Local(LocalTlenStore),
}

thread_local! {
    static LOCAL_TLEN: RefCell<LocalTlenRuntime> = const { RefCell::new(LocalTlenRuntime::Uninitialized) };
}

pub fn startup_mode_from_env(force_remote: bool) -> TlenQueryMode {
    startup_mode_from_env_with_default(force_remote, false)
}

pub fn queue_position_enabled_from_env() -> bool {
    let Ok(raw) = std::env::var(QUEUE_POSITION_ENABLED_ENV) else {
        return false;
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        other => {
            warn!(
                "invalid {}='{}'; queue-position messages disabled",
                QUEUE_POSITION_ENABLED_ENV, other
            );
            false
        }
    }
}

fn should_initialize_runtime(exec_branch: bool, queue_position_enabled: bool) -> bool {
    !exec_branch || queue_position_enabled
}

fn startup_mode_from_env_with_default(force_remote: bool, default_local: bool) -> TlenQueryMode {
    if let Ok(raw) = std::env::var(LOCAL_TLEN_MODE_ENV) {
        match raw.trim().to_ascii_lowercase().as_str() {
            "local" => return TlenQueryMode::Local,
            "remote" | "query" | "uds" => return TlenQueryMode::Remote,
            other => {
                warn!(
                    "invalid {}='{}'; using remote tlen query",
                    LOCAL_TLEN_MODE_ENV, other
                );
                return TlenQueryMode::Remote;
            }
        }
    }

    if force_remote {
        return TlenQueryMode::Remote;
    }

    if default_local {
        TlenQueryMode::Local
    } else {
        TlenQueryMode::Remote
    }
}

pub async fn init_for_trade_signal(open_venue: TradingVenue, force_remote: bool) -> Result<bool> {
    let exec_branch = matches!(decision_branch(), Some(DecisionBranch::Exec));
    let queue_position_enabled = queue_position_enabled_from_env();
    if !should_initialize_runtime(exec_branch, queue_position_enabled) {
        super::queue_position::disable();
        LOCAL_TLEN.with(|state| {
            *state.borrow_mut() = LocalTlenRuntime::Uninitialized;
        });
        info!(
            "Exec local_tlen disabled with queue-position runtime: venue={} env={}",
            open_venue.data_pub_slug(),
            QUEUE_POSITION_ENABLED_ENV
        );
        return Ok(false);
    }
    let mode = startup_mode_from_env_with_default(force_remote, exec_branch);
    match mode {
        TlenQueryMode::Remote => {
            super::queue_position::disable();
            LOCAL_TLEN.with(|state| {
                *state.borrow_mut() = LocalTlenRuntime::Remote;
            });
            info!(
                "local_tlen disabled: mode=remote venue={} force_remote={}",
                open_venue.data_pub_slug(),
                force_remote
            );
            Ok(false)
        }
        TlenQueryMode::Local => {
            let mut table = VenueMinQtyTable::new(open_venue);
            table
                .refresh()
                .await
                .with_context(|| format!("refresh local_tlen table for {:?}", open_venue))?;
            LOCAL_TLEN.with(|state| {
                let symbol_scope = if exec_branch {
                    SymbolScope::All
                } else {
                    SymbolScope::Online
                };
                *state.borrow_mut() =
                    LocalTlenRuntime::Local(LocalTlenStore::new(open_venue, table, symbol_scope));
            });
            if queue_position_enabled {
                super::queue_position::init_local(open_venue)?;
            } else {
                super::queue_position::disable();
                info!(
                    "queue-position messages disabled: venue={} env={} exec_branch={}",
                    open_venue.data_pub_slug(),
                    QUEUE_POSITION_ENABLED_ENV,
                    exec_branch
                );
            }
            spawn_incremental_listener(open_venue);
            info!(
                "local_tlen enabled: mode=local venue={} symbol_scope={} source=trade_signal_startup",
                open_venue.data_pub_slug(),
                if exec_branch { "all" } else { "online" }
            );
            Ok(true)
        }
    }
}

pub fn update_bbo(
    venue: TradingVenue,
    symbol: &str,
    timestamp_us: i64,
    bid_price: f64,
    bid_amount: f64,
    ask_price: f64,
    ask_amount: f64,
) {
    LOCAL_TLEN.with(|state| {
        if let LocalTlenRuntime::Local(store) = &mut *state.borrow_mut() {
            if store.venue == venue {
                let symbol = normalize_symbol_key_cow(symbol);
                store.apply_bbo(
                    symbol.as_ref(),
                    timestamp_us,
                    bid_price,
                    bid_amount,
                    ask_price,
                    ask_amount,
                );
                store.maybe_refresh_online_symbols();
                store.maybe_log_stats();
            }
        }
    });
}

pub fn query_batch_local(
    source: &str,
    venue_slug: &str,
    symbol: &str,
    tick_indices: &[i64],
) -> Option<Vec<f64>> {
    if tick_indices.is_empty() {
        return Some(Vec::new());
    }
    let symbol_key = normalize_symbol_key(symbol);
    LOCAL_TLEN.with(|state| {
        let mut state = state.borrow_mut();
        match &mut *state {
            LocalTlenRuntime::Local(store) => {
                if store.venue.data_pub_slug() != venue_slug {
                    warn!(
                        "{source}: local_tlen mode fixed to venue={} but requested venue={}; returning empty tlen in local-only mode",
                        store.venue.data_pub_slug(),
                        venue_slug
                    );
                    return Some(vec![TLEN_QUERY_AMOUNT_EMPTY; tick_indices.len()]);
                }
                Some(store.query_batch(&symbol_key, tick_indices))
            }
            LocalTlenRuntime::Remote | LocalTlenRuntime::Uninitialized => None,
        }
    })
}

pub fn query_batch_local_only_for_cancel(
    source: &str,
    venue_slug: &str,
    symbol: &str,
    tick_indices: &[i64],
) -> Option<Option<Vec<f64>>> {
    if tick_indices.is_empty() {
        return Some(Some(Vec::new()));
    }
    let symbol_key = normalize_symbol_key(symbol);
    LOCAL_TLEN.with(|state| {
        let mut state = state.borrow_mut();
        match &mut *state {
            LocalTlenRuntime::Local(store) => {
                if store.venue.data_pub_slug() != venue_slug {
                    warn!(
                        "{source}: local_tlen mode fixed to venue={} but requested venue={}; skipping tlen cancel query",
                        store.venue.data_pub_slug(),
                        venue_slug
                    );
                    return Some(None);
                }
                if !store.symbols.contains_key(&symbol_key) {
                    store.query_count = store.query_count.saturating_add(1);
                    store.query_missing_count = store.query_missing_count.saturating_add(1);
                    return Some(None);
                }
                Some(Some(store.query_batch(&symbol_key, tick_indices)))
            }
            LocalTlenRuntime::Remote | LocalTlenRuntime::Uninitialized => None,
        }
    })
}

fn spawn_incremental_listener(venue: TradingVenue) {
    let fast_poll = crate::runtime_flags::enable_ipc_fast_poll();
    tokio::task::spawn_local(async move {
        let result: Result<()> = async move {
            let venue_slug = venue.data_pub_slug();
            let service_name = format!("dat_pbs/{}/incremental", venue_slug);
            let node_name = format!(
                "trade_signal_local_tlen_{}_incremental",
                venue_slug.replace('-', "_")
            );
            let node = NodeBuilder::new()
                .name(&NodeName::new(&node_name)?)
                .create::<ipc::Service>()?;
            let service = node
                .service_builder(&ServiceName::new(&service_name)?)
                .publish_subscribe::<[u8; INC_PAYLOAD]>()
                .max_publishers(1)
                .max_subscribers(10)
                .history_size(100)
                .subscriber_max_buffer_size(8192)
                .open_or_create()?;
            let subscriber: Subscriber<ipc::Service, [u8; INC_PAYLOAD], ()> =
                service.subscriber_builder().create()?;
            info!("local_tlen subscribed incremental: {}", service_name);

            let mut drained = 0usize;
            loop {
                match subscriber.receive() {
                    Ok(Some(sample)) => {
                        drained += 1;
                        process_incremental_payload(sample.payload());
                        if drained >= INC_DRAIN_BUDGET {
                            drained = 0;
                            local_tlen_housekeeping();
                            tokio::task::yield_now().await;
                        }
                    }
                    Ok(None) => {
                        drained = 0;
                        local_tlen_housekeeping();
                        crate::runtime_flags::idle_poll_wait(fast_poll).await;
                    }
                    Err(err) => {
                        drained = 0;
                        local_tlen_housekeeping();
                        warn!("local_tlen incremental receive failed: {err:#}");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        }
        .await;

        if let Err(err) = result {
            warn!("local_tlen incremental listener exited: {err:#}");
        }
    });
}

fn process_incremental_payload(payload: &[u8]) {
    let Some(update) = parse_incremental_payload(payload) else {
        return;
    };
    LOCAL_TLEN.with(|state| {
        if let LocalTlenRuntime::Local(store) = &mut *state.borrow_mut() {
            store.apply_incremental_message(update);
        }
    });
}

fn local_tlen_housekeeping() {
    LOCAL_TLEN.with(|state| {
        if let LocalTlenRuntime::Local(store) = &mut *state.borrow_mut() {
            store.maybe_refresh_online_symbols();
            store.maybe_log_stats();
        }
    });
    super::queue_position::housekeeping();
}

struct IncrementalUpdate<'a> {
    symbol: &'a str,
    first_update_id: i64,
    final_update_id: i64,
    timestamp: i64,
    is_snapshot: bool,
    is_last: bool,
    chunk_index: u8,
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
}

fn parse_incremental_payload(payload: &[u8]) -> Option<IncrementalUpdate<'_>> {
    if payload.len() < 8 || get_msg_type(payload) != MktMsgType::OrderBookInc {
        return None;
    }
    let symbol = parse_symbol(payload)?;
    let mut offset = 8 + symbol.len();
    if payload.len() < offset + 32 {
        return None;
    }

    let first_update_id = read_i64(payload, &mut offset)?;
    let final_update_id = read_i64(payload, &mut offset)?;
    let timestamp = read_i64(payload, &mut offset)?;
    let is_snapshot = *payload.get(offset)? != 0;
    let is_last = *payload.get(offset.checked_add(1)?)? != 0;
    let chunk_index = *payload.get(offset.checked_add(2)?)?;
    offset = offset.checked_add(8)?;

    let bids_count = read_u32(payload, &mut offset)? as usize;
    let asks_count = read_u32(payload, &mut offset)? as usize;
    let levels_len = bids_count.checked_add(asks_count)?.checked_mul(16)?;
    if payload.len() < offset + levels_len {
        return None;
    }

    let mut bids = Vec::with_capacity(bids_count);
    let mut asks = Vec::with_capacity(asks_count);
    for _ in 0..bids_count {
        let price = read_f64(payload, &mut offset)?;
        let amount = read_f64(payload, &mut offset)?;
        bids.push((price, amount));
    }
    for _ in 0..asks_count {
        let price = read_f64(payload, &mut offset)?;
        let amount = read_f64(payload, &mut offset)?;
        asks.push((price, amount));
    }

    Some(IncrementalUpdate {
        symbol,
        first_update_id,
        final_update_id,
        timestamp,
        is_snapshot,
        is_last,
        chunk_index,
        bids,
        asks,
    })
}

fn parse_symbol(payload: &[u8]) -> Option<&str> {
    if payload.len() < 8 {
        return None;
    }
    let symbol_len = u32::from_le_bytes(payload.get(4..8)?.try_into().ok()?) as usize;
    let start = 8usize;
    let end = start.checked_add(symbol_len)?;
    std::str::from_utf8(payload.get(start..end)?).ok()
}

fn read_u32(payload: &[u8], offset: &mut usize) -> Option<u32> {
    let end = offset.checked_add(4)?;
    let value = u32::from_le_bytes(payload.get(*offset..end)?.try_into().ok()?);
    *offset = end;
    Some(value)
}

fn read_i64(payload: &[u8], offset: &mut usize) -> Option<i64> {
    let end = offset.checked_add(8)?;
    let value = i64::from_le_bytes(payload.get(*offset..end)?.try_into().ok()?);
    *offset = end;
    Some(value)
}

fn read_f64(payload: &[u8], offset: &mut usize) -> Option<f64> {
    let end = offset.checked_add(8)?;
    let value = f64::from_le_bytes(payload.get(*offset..end)?.try_into().ok()?);
    *offset = end;
    Some(value)
}

fn build_snapshot_side(
    levels: &[(f64, f64)],
    price_tick: f64,
    amount_scale: f64,
    update_id: i64,
) -> FastHashMap<i64, LevelEntry> {
    let mut out = fast_hash_map();
    for &(price, amount) in levels {
        let Some(tick_index) = price_to_tick_index(price, price_tick) else {
            continue;
        };
        let amount = amount * amount_scale;
        if amount.is_finite() && amount > 0.0 {
            out.insert(tick_index, LevelEntry { amount, update_id });
        }
    }
    out
}

fn snapshot_level_changes(
    old: &FastHashMap<i64, LevelEntry>,
    new: &FastHashMap<i64, LevelEntry>,
) -> Vec<(i64, f64, f64)> {
    let mut changes = Vec::with_capacity(old.len() + new.len());
    for (&tick_index, old_entry) in old {
        let old_qty = positive_level_amount(old_entry.amount);
        let new_qty = new
            .get(&tick_index)
            .map_or(0.0, |entry| positive_level_amount(entry.amount));
        if old_qty != new_qty {
            changes.push((tick_index, old_qty, new_qty));
        }
    }
    for (&tick_index, new_entry) in new {
        if old.contains_key(&tick_index) {
            continue;
        }
        let new_qty = positive_level_amount(new_entry.amount);
        if new_qty > 0.0 {
            changes.push((tick_index, 0.0, new_qty));
        }
    }
    changes
}

fn positive_level_amount(amount: f64) -> f64 {
    if amount.is_finite() && amount > 0.0 {
        amount
    } else {
        0.0
    }
}

fn apply_level_update(
    levels: &mut FastHashMap<i64, LevelEntry>,
    tick_index: i64,
    amount: f64,
    update_id: i64,
) -> Option<(f64, f64)> {
    let new_qty = if amount > 0.0 && amount.is_finite() {
        amount
    } else {
        0.0
    };
    match levels.entry(tick_index) {
        Entry::Occupied(mut entry) => {
            if update_id <= entry.get().update_id {
                return None;
            }
            let old_qty = entry.get().amount;
            entry.insert(LevelEntry {
                amount: new_qty,
                update_id,
            });
            Some((old_qty, new_qty))
        }
        Entry::Vacant(entry) => {
            entry.insert(LevelEntry {
                amount: new_qty,
                update_id,
            });
            Some((0.0, new_qty))
        }
    }
}

pub(crate) fn queue_level_context(
    venue: TradingVenue,
    symbol: &str,
    side: Side,
    price: f64,
) -> Option<super::queue_position::LocalLevelContext> {
    queue_level_context_by_book_side(venue, symbol, BookSide::from_order_side(side), price)
}

pub(crate) fn queue_level_context_by_book_side(
    venue: TradingVenue,
    symbol: &str,
    side: BookSide,
    price: f64,
) -> Option<super::queue_position::LocalLevelContext> {
    let symbol = normalize_symbol_key_cow(symbol);
    LOCAL_TLEN.with(|state| {
        let state = state.borrow();
        let LocalTlenRuntime::Local(store) = &*state else {
            return None;
        };
        if store.venue != venue {
            return None;
        }
        let cache = store.symbols.get(symbol.as_ref())?;
        let price_key = price_to_tick_index(price, cache.price_tick?)?;
        let levels = match side {
            BookSide::Bid => &cache.bids,
            BookSide::Ask => &cache.asks,
        };
        let visible_qty = query_bbo_amount(cache.bbo, price_key)
            .or_else(|| levels.get(&price_key).map(|entry| entry.amount))
            .unwrap_or(0.0);
        Some(super::queue_position::LocalLevelContext {
            price_key,
            visible_qty,
            amount_scale: cache.amount_scale,
        })
    })
}

fn query_bbo_amount(bbo: BboEntry, tick_index: i64) -> Option<f64> {
    if bbo.timestamp_us <= 0 {
        return None;
    }
    if bbo.bid_tick == Some(tick_index) && bbo.bid_amount.is_finite() && bbo.bid_amount > 0.0 {
        return Some(bbo.bid_amount);
    }
    if bbo.ask_tick == Some(tick_index) && bbo.ask_amount.is_finite() && bbo.ask_amount > 0.0 {
        return Some(bbo.ask_amount);
    }
    None
}

fn query_level_amount(levels: &FastHashMap<i64, LevelEntry>, tick_index: i64) -> Option<f64> {
    levels
        .get(&tick_index)
        .map(|entry| entry.amount)
        .filter(|amount| amount.is_finite() && *amount > 0.0)
}

fn load_online_symbol_set() -> FastHashSet<String> {
    fast_hash_set_from_iter(
        SymbolList::instance()
            .get_online_symbols()
            .into_iter()
            .map(|symbol| normalize_symbol_key(&symbol)),
    )
}

fn normalize_symbol_key(symbol: &str) -> String {
    normalize_symbol_for_whitelist(symbol, TradingVenue::OkexFutures)
}

pub(crate) fn normalize_symbol_key_cow(symbol: &str) -> Cow<'_, str> {
    let already_canonical = symbol.is_ascii()
        && !symbol
            .bytes()
            .any(|byte| byte.is_ascii_lowercase() || matches!(byte, b'-' | b'_'))
        && !symbol.ends_with("SWAP");
    if already_canonical {
        Cow::Borrowed(symbol)
    } else {
        Cow::Owned(normalize_symbol_key(symbol))
    }
}

fn symbol_key_for_table(venue: TradingVenue, symbol: &str) -> String {
    match venue {
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
            symbol.to_uppercase().replace("-SWAP", "").replace('-', "")
        }
        TradingVenue::GateMargin | TradingVenue::GateFutures => {
            symbol.to_uppercase().replace(['_', '-'], "")
        }
        _ => symbol.to_uppercase(),
    }
}

fn price_tick_for_symbol(
    table: &VenueMinQtyTable,
    venue: TradingVenue,
    symbol: &str,
) -> Option<f64> {
    table.price_tick(&symbol_key_for_table(venue, symbol))
}

fn amount_scale_for_symbol(table: &VenueMinQtyTable, venue: TradingVenue, symbol: &str) -> f64 {
    if !venue.is_futures()
        || matches!(
            venue,
            TradingVenue::BinanceFutures
                | TradingVenue::BinanceCoinFutures
                | TradingVenue::BitgetCoinFutures
        )
    {
        return 1.0;
    }
    table
        .contract_multiplier_opt(&symbol_key_for_table(venue, symbol))
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::mkt_msg::{IncMsg, Level};
    use std::sync::{Mutex, OnceLock};

    fn env_test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    fn test_store(venue: TradingVenue) -> LocalTlenStore {
        LocalTlenStore {
            venue,
            table: VenueMinQtyTable::new(venue),
            symbol_scope: SymbolScope::Online,
            online_symbols: fast_hash_set(),
            symbols: fast_hash_map(),
            last_online_refresh: Instant::now(),
            inc_updates: 0,
            bbo_updates: 0,
            query_count: 0,
            query_missing_count: 0,
            query_bbo_hit_count: 0,
            last_stats_log: Instant::now(),
        }
    }

    fn test_store_with_symbol(venue: TradingVenue, symbol: &str) -> LocalTlenStore {
        let mut store = test_store(venue);
        store.symbol_scope = SymbolScope::All;
        store.symbols.insert(
            symbol.to_string(),
            SymbolTlenCache {
                price_tick: Some(1.0),
                amount_scale: 1.0,
                ..SymbolTlenCache::default()
            },
        );
        store
    }

    #[test]
    fn startup_mode_defaults_to_remote_query() {
        let _guard = env_test_lock();
        std::env::remove_var(LOCAL_TLEN_MODE_ENV);

        assert_eq!(startup_mode_from_env(false), TlenQueryMode::Remote);
        assert_eq!(startup_mode_from_env(true), TlenQueryMode::Remote);
    }

    #[test]
    fn startup_mode_can_default_to_local_for_exec() {
        let _guard = env_test_lock();
        std::env::remove_var(LOCAL_TLEN_MODE_ENV);

        assert_eq!(
            startup_mode_from_env_with_default(false, true),
            TlenQueryMode::Local
        );
        assert_eq!(
            startup_mode_from_env_with_default(true, true),
            TlenQueryMode::Remote
        );
    }

    #[test]
    fn queue_position_messages_require_explicit_enable() {
        let _guard = env_test_lock();
        std::env::remove_var(QUEUE_POSITION_ENABLED_ENV);

        assert!(!queue_position_enabled_from_env());

        for enabled in ["1", "true", "yes", "on"] {
            std::env::set_var(QUEUE_POSITION_ENABLED_ENV, enabled);
            assert!(queue_position_enabled_from_env());
        }
        for disabled in ["0", "false", "no", "off"] {
            std::env::set_var(QUEUE_POSITION_ENABLED_ENV, disabled);
            assert!(!queue_position_enabled_from_env());
        }

        std::env::set_var(QUEUE_POSITION_ENABLED_ENV, "invalid");
        assert!(!queue_position_enabled_from_env());

        std::env::remove_var(QUEUE_POSITION_ENABLED_ENV);
    }

    #[test]
    fn exec_runtime_requires_queue_position_but_other_branches_do_not() {
        assert!(!should_initialize_runtime(true, false));
        assert!(should_initialize_runtime(true, true));
        assert!(should_initialize_runtime(false, false));
        assert!(should_initialize_runtime(false, true));
    }

    #[test]
    fn all_symbol_scope_accepts_unlisted_symbols() {
        let mut store = test_store(TradingVenue::BinanceFutures);
        assert!(!store.is_online("BTCUSDT"));
        store.symbol_scope = SymbolScope::All;
        assert!(store.is_online("BTCUSDT"));
    }

    #[test]
    fn startup_mode_parses_explicit_local_and_remote_modes() {
        let _guard = env_test_lock();

        std::env::set_var(LOCAL_TLEN_MODE_ENV, "local");
        assert_eq!(startup_mode_from_env(false), TlenQueryMode::Local);

        for mode in ["remote", "query", "uds"] {
            std::env::set_var(LOCAL_TLEN_MODE_ENV, mode);
            assert_eq!(startup_mode_from_env(false), TlenQueryMode::Remote);
        }

        std::env::remove_var(LOCAL_TLEN_MODE_ENV);
    }

    #[test]
    fn startup_mode_explicit_local_overrides_force_remote() {
        let _guard = env_test_lock();

        std::env::set_var(LOCAL_TLEN_MODE_ENV, "local");
        assert_eq!(startup_mode_from_env(true), TlenQueryMode::Local);

        std::env::remove_var(LOCAL_TLEN_MODE_ENV);
    }

    #[test]
    fn newer_zero_update_tombstones_level() {
        let mut levels = fast_hash_map();
        apply_level_update(&mut levels, 100, 2.0, 10);
        apply_level_update(&mut levels, 100, 0.0, 11);
        apply_level_update(&mut levels, 100, 3.0, 9);
        assert_eq!(query_level_amount(&levels, 100), None);
        apply_level_update(&mut levels, 100, 4.0, 12);
        assert_eq!(query_level_amount(&levels, 100), Some(4.0));
    }

    #[test]
    fn bbo_amount_has_priority() {
        let bbo = BboEntry {
            bid_tick: Some(10),
            bid_amount: 1.5,
            ask_tick: Some(11),
            ask_amount: 2.5,
            timestamp_us: 100,
        };
        assert_eq!(query_bbo_amount(bbo, 10), Some(1.5));
        assert_eq!(query_bbo_amount(bbo, 11), Some(2.5));
        assert_eq!(query_bbo_amount(bbo, 12), None);
    }

    #[test]
    fn parses_incremental_snapshot_chunk_fields() {
        let mut msg = IncMsg::create("HYPEUSDC".to_string(), 10, 11, 12_000, true, 1, 1);
        msg.set_is_last(false);
        msg.set_chunk_index(3);
        msg.set_bid_level(0, Level::from_values(100.0, 2.0));
        msg.set_ask_level(0, Level::from_values(101.0, 3.0));

        let bytes = msg.to_bytes();
        let parsed = parse_incremental_payload(&bytes).expect("incremental payload");
        assert_eq!(parsed.symbol, "HYPEUSDC");
        assert_eq!(parsed.first_update_id, 10);
        assert_eq!(parsed.final_update_id, 11);
        assert_eq!(parsed.timestamp, 12_000);
        assert!(parsed.is_snapshot);
        assert!(!parsed.is_last);
        assert_eq!(parsed.chunk_index, 3);
        assert_eq!(parsed.bids, vec![(100.0, 2.0)]);
        assert_eq!(parsed.asks, vec![(101.0, 3.0)]);
    }

    #[test]
    fn hyperliquid_snapshot_replaces_local_book_only_after_all_chunks() {
        let mut store = test_store_with_symbol(TradingVenue::HyperliquidMargin, "HYPEUSDC");
        store.apply_incremental("HYPEUSDC", 900, &[(90.0, 9.0)], &[(110.0, 11.0)]);

        let terminal_chunk = || IncrementalUpdate {
            symbol: "HYPEUSDC",
            first_update_id: 1_000,
            final_update_id: 1_000,
            timestamp: 1_000_000,
            is_snapshot: true,
            is_last: true,
            chunk_index: 1,
            bids: vec![(99.0, 2.0)],
            asks: vec![(102.0, 4.0)],
        };
        store.apply_incremental_message(terminal_chunk());
        store.apply_incremental_message(terminal_chunk());
        let cache = store.symbols.get("HYPEUSDC").unwrap();
        assert_eq!(query_level_amount(&cache.bids, 90), Some(9.0));
        assert_eq!(store.inc_updates, 1);

        store.apply_incremental_message(IncrementalUpdate {
            symbol: "HYPEUSDC",
            first_update_id: 1_000,
            final_update_id: 1_000,
            timestamp: 1_000_000,
            is_snapshot: true,
            is_last: false,
            chunk_index: 0,
            bids: vec![(100.0, 1.0)],
            asks: vec![(101.0, 3.0)],
        });
        let cache = store.symbols.get("HYPEUSDC").unwrap();
        assert_eq!(query_level_amount(&cache.bids, 90), None);
        assert_eq!(query_level_amount(&cache.asks, 110), None);
        assert_eq!(query_level_amount(&cache.bids, 100), Some(1.0));
        assert_eq!(query_level_amount(&cache.bids, 99), Some(2.0));
        assert_eq!(query_level_amount(&cache.asks, 101), Some(3.0));
        assert_eq!(query_level_amount(&cache.asks, 102), Some(4.0));
        assert_eq!(store.inc_updates, 2);

        store.apply_incremental_message(IncrementalUpdate {
            symbol: "HYPEUSDC",
            first_update_id: 1_001,
            final_update_id: 1_001,
            timestamp: 1_001_000,
            is_snapshot: true,
            is_last: true,
            chunk_index: 0,
            bids: Vec::new(),
            asks: Vec::new(),
        });
        let cache = store.symbols.get("HYPEUSDC").unwrap();
        assert!(cache.bids.is_empty());
        assert!(cache.asks.is_empty());
        assert_eq!(store.inc_updates, 3);
    }

    #[test]
    fn hyperliquid_snapshot_conflict_is_rejected_until_newer_sequence() {
        let mut assembler = HyperliquidSnapshotAssembler::default();
        assert!(matches!(
            assembler.add_chunk(10, 10, 10_000, 0, false, vec![(100.0, 1.0)], vec![]),
            HyperliquidSnapshotAssembly::Pending
        ));
        assert!(matches!(
            assembler.add_chunk(10, 10, 10_000, 0, false, vec![(100.0, 2.0)], vec![]),
            HyperliquidSnapshotAssembly::Conflict
        ));
        assert!(matches!(
            assembler.add_chunk(10, 10, 10_000, 0, true, vec![(100.0, 1.0)], vec![]),
            HyperliquidSnapshotAssembly::Pending
        ));
        assert!(matches!(
            assembler.add_chunk(
                11,
                11,
                11_000,
                0,
                true,
                vec![(100.0, 2.0)],
                vec![(101.0, 2.0)]
            ),
            HyperliquidSnapshotAssembly::Complete(_)
        ));
        assert!(matches!(
            assembler.add_chunk(11, 11, 11_000, 0, true, vec![], vec![]),
            HyperliquidSnapshotAssembly::Pending
        ));
        assert!(matches!(
            assembler.add_chunk(10, 10, 10_000, 0, true, vec![], vec![]),
            HyperliquidSnapshotAssembly::Pending
        ));
    }

    #[test]
    fn non_hyperliquid_snapshot_chunks_keep_delta_semantics() {
        let mut store = test_store_with_symbol(TradingVenue::BinanceFutures, "BTCUSDT");
        store.apply_incremental("BTCUSDT", 10, &[(90.0, 1.0)], &[]);

        store.apply_incremental_message(IncrementalUpdate {
            symbol: "BTCUSDT",
            first_update_id: 20,
            final_update_id: 20,
            timestamp: 20_000,
            is_snapshot: true,
            is_last: false,
            chunk_index: 1,
            bids: vec![(100.0, 2.0)],
            asks: vec![(101.0, 3.0)],
        });

        let cache = store.symbols.get("BTCUSDT").unwrap();
        assert_eq!(query_level_amount(&cache.bids, 90), Some(1.0));
        assert_eq!(query_level_amount(&cache.bids, 100), Some(2.0));
        assert_eq!(query_level_amount(&cache.asks, 101), Some(3.0));
        assert_eq!(store.inc_updates, 2);
    }

    #[test]
    fn snapshot_level_changes_include_removals_additions_and_quantity_changes() {
        let mut old = fast_hash_map();
        old.insert(
            99,
            LevelEntry {
                amount: 1.0,
                update_id: 10,
            },
        );
        old.insert(
            100,
            LevelEntry {
                amount: 2.0,
                update_id: 10,
            },
        );
        old.insert(
            101,
            LevelEntry {
                amount: 3.0,
                update_id: 10,
            },
        );
        let mut new = fast_hash_map();
        new.insert(
            100,
            LevelEntry {
                amount: 2.0,
                update_id: 20,
            },
        );
        new.insert(
            101,
            LevelEntry {
                amount: 4.0,
                update_id: 20,
            },
        );
        new.insert(
            102,
            LevelEntry {
                amount: 5.0,
                update_id: 20,
            },
        );

        let mut changes = snapshot_level_changes(&old, &new);
        changes.sort_by_key(|change| change.0);
        assert_eq!(
            changes,
            vec![(99, 1.0, 0.0), (101, 3.0, 4.0), (102, 0.0, 5.0)]
        );
    }

    #[test]
    fn cancel_query_skips_when_local_cache_missing() {
        LOCAL_TLEN.with(|state| {
            *state.borrow_mut() = LocalTlenRuntime::Local(test_store(TradingVenue::BinanceMargin));
        });

        assert_eq!(
            query_batch_local_only_for_cancel("test", "binance-margin", "BTCUSDT", &[100, 101],),
            Some(None)
        );

        LOCAL_TLEN.with(|state| {
            *state.borrow_mut() = LocalTlenRuntime::Uninitialized;
        });
    }

    #[test]
    fn open_query_stays_local_on_venue_mismatch() {
        LOCAL_TLEN.with(|state| {
            *state.borrow_mut() = LocalTlenRuntime::Local(test_store(TradingVenue::BinanceMargin));
        });

        assert_eq!(
            query_batch_local("test", "gate-margin", "BTCUSDT", &[100, 101]),
            Some(vec![TLEN_QUERY_AMOUNT_EMPTY, TLEN_QUERY_AMOUNT_EMPTY])
        );

        LOCAL_TLEN.with(|state| {
            *state.borrow_mut() = LocalTlenRuntime::Uninitialized;
        });
    }

    #[test]
    fn cancel_query_returns_cached_local_values() {
        LOCAL_TLEN.with(|state| {
            let mut store = test_store(TradingVenue::BinanceMargin);
            let mut bids = fast_hash_map();
            bids.insert(
                100,
                LevelEntry {
                    amount: 2.0,
                    update_id: 1,
                },
            );
            store.symbols.insert(
                normalize_symbol_key("BTCUSDT"),
                SymbolTlenCache {
                    price_tick: Some(0.01),
                    amount_scale: 1.0,
                    bids,
                    asks: fast_hash_map(),
                    bbo: BboEntry::default(),
                    hyperliquid_snapshots: HyperliquidSnapshotAssembler::default(),
                },
            );
            *state.borrow_mut() = LocalTlenRuntime::Local(store);
        });

        assert_eq!(
            query_batch_local_only_for_cancel("test", "binance-margin", "BTCUSDT", &[100, 101],),
            Some(Some(vec![2.0, 0.0]))
        );

        LOCAL_TLEN.with(|state| {
            *state.borrow_mut() = LocalTlenRuntime::Uninitialized;
        });
    }
}
