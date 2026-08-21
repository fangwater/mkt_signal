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
use std::collections::hash_map::Entry;
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

#[derive(Debug, Default)]
struct SymbolTlenCache {
    price_tick: Option<f64>,
    amount_scale: f64,
    bids: FastHashMap<i64, LevelEntry>,
    asks: FastHashMap<i64, LevelEntry>,
    bbo: BboEntry,
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
    let symbol = normalize_symbol_key_cow(update.symbol);
    LOCAL_TLEN.with(|state| {
        if let LocalTlenRuntime::Local(store) = &mut *state.borrow_mut() {
            store.apply_incremental(
                symbol.as_ref(),
                update.final_update_id,
                &update.bids,
                &update.asks,
            );
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
    final_update_id: i64,
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

    let _first_update_id = read_i64(payload, &mut offset)?;
    let final_update_id = read_i64(payload, &mut offset)?;
    let _timestamp = read_i64(payload, &mut offset)?;
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
        final_update_id,
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
