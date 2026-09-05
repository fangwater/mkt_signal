//! Depth Publisher 应用主模块
//!
//! 订阅 mkt_pub 的 incremental 数据，维护订单簿，发布深度快照

use anyhow::{anyhow, Result};
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use indexmap::IndexSet;
use log::{debug, info, warn};
use runtime_common::fast_hash::{fast_hash_map, fast_hash_set, FastHashMap};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use super::depth_msg::DepthMsg;
use super::orderbook::OrderBook;
use super::publisher::DepthMsgPublisher;
use super::query_logic::{build_query_response, DepthQuerySource};
use super::query_snapshot::{QuerySnapshotStore, SymbolQuerySnapshot};
use depth_pub_common::query_server::{DepthQueryConnection, DepthQuerySocketServer};
use order_common::TradingVenue;
use signal_common::venue_min_qty_table::VenueMinQtyTable;

/// IceOryx 增量消息缓冲区大小 (与 mkt_pub 一致)
const INC_MAX_BYTES: usize = 2048;
const TIMER_CHECK_EVERY_INCS: u64 = 500;
const IDLE_SLEEP_MICROS: u64 = 100;
/// 滑动窗口大小：用于去重的最近 update_id 数量
const DEDUP_WINDOW_SIZE: usize = 4096 * 2;
const KEEPALIVE_PUSH_INTERVAL_MS: u64 = 1000;
const BTC_DEPTH25_LOG_INTERVAL_SECS: u64 = 30;
const PUBLISH_OUTCOME_LOG_INTERVAL_SECS: u64 = 10;
const MAX_INC_DRAIN_PER_POLL: usize = 256;
const MAX_QUERY_ACCEPTS_PER_POLL: usize = 8;
const MAX_QUERY_REQUESTS_PER_POLL: usize = 64;
const STATS_INTERVAL_SECS: u64 = 60;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HyperliquidSnapshotKey {
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

fn hyperliquid_levels_equal(left: &[(f64, f64)], right: &[(f64, f64)]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.0.to_bits() == right.0.to_bits() && left.1.to_bits() == right.1.to_bits()
        })
}

impl HyperliquidSnapshotChunk {
    fn exactly_matches(&self, other: &Self) -> bool {
        self.is_last == other.is_last
            && hyperliquid_levels_equal(&self.bids, &other.bids)
            && hyperliquid_levels_equal(&self.asks, &other.asks)
    }
}

#[derive(Debug)]
struct PendingHyperliquidSnapshot {
    key: HyperliquidSnapshotKey,
    chunks: BTreeMap<u8, HyperliquidSnapshotChunk>,
    last_chunk_index: Option<u8>,
}

impl PendingHyperliquidSnapshot {
    fn new(key: HyperliquidSnapshotKey) -> Self {
        Self {
            key,
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
            key: self.key,
            bids,
            asks,
        }
    }
}

#[derive(Clone, Debug)]
struct CompleteHyperliquidSnapshot {
    key: HyperliquidSnapshotKey,
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
}

impl CompleteHyperliquidSnapshot {
    fn exactly_matches(&self, other: &Self) -> bool {
        self.key == other.key
            && hyperliquid_levels_equal(&self.bids, &other.bids)
            && hyperliquid_levels_equal(&self.asks, &other.asks)
    }
}

#[derive(Debug, Default)]
struct HyperliquidSnapshotAssembler {
    pending: Option<PendingHyperliquidSnapshot>,
    last_applied: Option<CompleteHyperliquidSnapshot>,
    rejected_update_id: Option<i64>,
}

impl HyperliquidSnapshotAssembler {
    fn reject_generation(&mut self, final_update_id: i64) {
        self.pending = None;
        self.rejected_update_id = Some(final_update_id);
    }

    fn add_chunk(
        &mut self,
        key: HyperliquidSnapshotKey,
        chunk_index: u8,
        is_last: bool,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>,
    ) -> Option<CompleteHyperliquidSnapshot> {
        if self.last_applied.as_ref().is_some_and(|last_applied| {
            key.final_update_id < last_applied.key.final_update_id
                || (key.final_update_id == last_applied.key.final_update_id
                    && key != last_applied.key)
        }) || self
            .rejected_update_id
            .is_some_and(|rejected| key.final_update_id <= rejected)
        {
            return None;
        }

        match self.pending.as_ref() {
            Some(pending) if pending.key == key => {}
            Some(pending) if key.final_update_id > pending.key.final_update_id => {
                self.pending = Some(PendingHyperliquidSnapshot::new(key));
                self.rejected_update_id = None;
            }
            Some(pending) if key.final_update_id == pending.key.final_update_id => {
                self.reject_generation(key.final_update_id);
                return None;
            }
            Some(_) => return None,
            None => {
                self.pending = Some(PendingHyperliquidSnapshot::new(key));
                self.rejected_update_id = None;
            }
        }

        let incoming = HyperliquidSnapshotChunk {
            is_last,
            bids,
            asks,
        };
        let invalid_generation = {
            let pending = self.pending.as_mut().expect("pending snapshot initialized");
            if let Some(existing) = pending.chunks.get(&chunk_index) {
                !existing.exactly_matches(&incoming)
            } else {
                let invalid_layout = if is_last {
                    match pending.last_chunk_index {
                        Some(existing) if existing != chunk_index => true,
                        _ => pending.chunks.keys().any(|index| *index > chunk_index),
                    }
                } else {
                    pending
                        .last_chunk_index
                        .is_some_and(|last_chunk_index| chunk_index >= last_chunk_index)
                };
                if !invalid_layout {
                    if is_last {
                        pending.last_chunk_index = Some(chunk_index);
                    }
                    pending.chunks.insert(chunk_index, incoming);
                }
                invalid_layout
            }
        };
        if invalid_generation {
            self.reject_generation(key.final_update_id);
            return None;
        }

        let pending = self.pending.as_mut().expect("pending snapshot initialized");
        if !pending.is_complete() {
            return None;
        }

        let complete = self
            .pending
            .take()
            .expect("complete pending snapshot must exist")
            .into_complete();
        if self
            .last_applied
            .as_ref()
            .is_some_and(|last_applied| last_applied.exactly_matches(&complete))
        {
            return None;
        }
        self.last_applied = Some(complete.clone());
        Some(complete)
    }
}

fn uses_full_snapshot_replacement(venue: TradingVenue) -> bool {
    matches!(
        venue,
        TradingVenue::HyperliquidMargin | TradingVenue::HyperliquidFutures
    )
}

/// 每个 symbol 的状态
struct SymbolState {
    orderbook: OrderBook,
    hyperliquid_snapshots: HyperliquidSnapshotAssembler,
    last_push_time: Instant,
    query_snapshot_dirty: bool,
    /// 有序去重集合：保存最近处理过的 (update_id, chunk_index)
    /// - Set 语义：O(1) 判重
    /// - 保留插入顺序：窗口超限时移除最旧 key
    dedup_msg_keys: IndexSet<(i64, u8)>,
}

impl SymbolState {
    fn new() -> Self {
        Self {
            orderbook: OrderBook::new(),
            hyperliquid_snapshots: HyperliquidSnapshotAssembler::default(),
            last_push_time: Instant::now(),
            query_snapshot_dirty: true,
            dedup_msg_keys: IndexSet::with_capacity(DEDUP_WINDOW_SIZE),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_hyperliquid_snapshot_chunk(
        &mut self,
        first_update_id: i64,
        final_update_id: i64,
        timestamp: i64,
        chunk_index: u8,
        is_last: bool,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>,
    ) -> bool {
        let key = HyperliquidSnapshotKey {
            first_update_id,
            final_update_id,
            timestamp,
        };
        let Some(snapshot) =
            self.hyperliquid_snapshots
                .add_chunk(key, chunk_index, is_last, bids, asks)
        else {
            return false;
        };

        self.orderbook.replace_snapshot(
            &snapshot.bids,
            &snapshot.asks,
            snapshot.key.final_update_id,
            snapshot.key.timestamp,
        );
        self.query_snapshot_dirty = true;
        true
    }

    /// 检查 (update_id, chunk_index) 是否重复
    /// 返回 true 表示是重复的，应该跳过
    #[inline]
    fn is_duplicate(&mut self, update_id: i64, chunk_index: u8) -> bool {
        let key = (update_id, chunk_index);

        // 已存在 => 重复
        if !self.dedup_msg_keys.insert(key) {
            return true;
        }

        // 窗口超限时淘汰最旧 key（FIFO）
        if self.dedup_msg_keys.len() > DEDUP_WINDOW_SIZE {
            let _ = self.dedup_msg_keys.shift_remove_index(0);
        }

        false
    }
}

/// Depth Publisher 应用
pub struct DepthPubApp {
    venue: TradingVenue,
    venue_slug: String,
    publisher: DepthMsgPublisher,
    subscriber: Subscriber<ipc::Service, [u8; INC_MAX_BYTES], ()>,
    query_snapshots: Arc<QuerySnapshotStore>,
    query_server: DepthQuerySocketServer,
    query_connections: Vec<DepthQueryConnection>,
    min_qty_table: VenueMinQtyTable,
    /// symbol -> SymbolState
    symbols: FastHashMap<String, SymbolState>,
    /// 推送间隔
    push_interval: Duration,
    /// 统计
    update_count: u64,
    push_count: u64,
    publish_success_count: u64,
    publish_fail_invalid_count: u64,
    publish_fail_send_count: u64,
    publish_fail_missing_side_count: u64,
    publish_fail_crossed_book_count: u64,
    timer_check_counter: u64,
    idle_check_counter: u64,
    idle_check_every: u64,
    last_btc_depth25_log: Instant,
    last_publish_outcome_log: Instant,
}

struct DepthQueryAppSource {
    snapshots: Arc<QuerySnapshotStore>,
}

impl DepthQuerySource for DepthQueryAppSource {
    fn venue_slug(&self) -> &str {
        self.snapshots.venue_slug()
    }

    fn resolve_snapshot(&self, symbol: &str) -> Option<Arc<SymbolQuerySnapshot>> {
        self.snapshots.load(symbol)
    }
}

pub struct DepthPubRunner {
    apps: Vec<DepthPubApp>,
}

impl DepthPubRunner {
    pub async fn new(venues: Vec<TradingVenue>) -> Result<Self> {
        let mut seen = fast_hash_set();
        let mut apps = Vec::with_capacity(venues.len());
        for venue in venues {
            if !seen.insert(venue) {
                warn!(
                    "duplicate depth_pub venue ignored: {}",
                    venue.data_pub_slug()
                );
                continue;
            }
            apps.push(DepthPubApp::new(venue).await?);
        }
        if apps.is_empty() {
            return Err(anyhow!("depth_pub requires at least one venue"));
        }
        let venues: Vec<&str> = apps.iter().map(|app| app.venue_slug.as_str()).collect();
        info!("DepthPubRunner created: venues={}", venues.join(","));
        Ok(Self { apps })
    }

    pub fn run(&mut self) -> Result<()> {
        let venues: Vec<&str> = self
            .apps
            .iter()
            .map(|app| app.venue_slug.as_str())
            .collect();
        info!(
            "DepthPubRunner starting main loop: venues={}",
            venues.join(",")
        );
        let mut last_stats_time = Instant::now();
        loop {
            let mut has_message = false;
            for app in &mut self.apps {
                has_message |= app.poll_once()?;
            }
            if !has_message {
                thread::sleep(Duration::from_micros(IDLE_SLEEP_MICROS));
            }
            if last_stats_time.elapsed() >= Duration::from_secs(STATS_INTERVAL_SECS) {
                for app in &mut self.apps {
                    app.log_stats();
                }
                last_stats_time = Instant::now();
            }
        }
    }
}

impl DepthPubApp {
    /// 创建应用实例
    /// venue: 例如 TradingVenue::BinanceFutures
    pub async fn new(venue: TradingVenue) -> Result<Self> {
        let venue_slug = venue.data_pub_slug();
        let push_interval = Duration::from_millis(KEEPALIVE_PUSH_INTERVAL_MS);
        let idle_check_every = std::cmp::max(
            1,
            (push_interval.as_micros() / IDLE_SLEEP_MICROS as u128) as u64,
        );

        let mut min_qty_table = VenueMinQtyTable::new(venue);
        min_qty_table.refresh().await?;

        // 创建发布器
        let publisher = DepthMsgPublisher::new(venue_slug)?;

        // 创建订阅器
        let subscriber = Self::create_subscriber(publisher.node(), venue_slug)?;
        info!(
            "Subscribed to incremental channel: dat_pbs/{}/incremental",
            venue_slug
        );
        let query_snapshots = Arc::new(QuerySnapshotStore::new(venue_slug));
        let query_server = DepthQuerySocketServer::bind(venue_slug)?;

        info!(
            "DepthPubApp created for {}: keepalive_push_interval={}ms, depth25=true",
            venue_slug, KEEPALIVE_PUSH_INTERVAL_MS
        );

        Ok(Self {
            venue,
            venue_slug: venue_slug.to_string(),
            publisher,
            subscriber,
            query_snapshots,
            query_server,
            query_connections: Vec::new(),
            min_qty_table,
            symbols: fast_hash_map(),
            push_interval,
            update_count: 0,
            push_count: 0,
            publish_success_count: 0,
            publish_fail_invalid_count: 0,
            publish_fail_send_count: 0,
            publish_fail_missing_side_count: 0,
            publish_fail_crossed_book_count: 0,
            timer_check_counter: 0,
            idle_check_counter: 0,
            idle_check_every,
            last_btc_depth25_log: Instant::now(),
            last_publish_outcome_log: Instant::now(),
        })
    }

    /// 创建 IceOryx 订阅器
    fn create_subscriber(
        node: &Node<ipc::Service>,
        venue: &str,
    ) -> Result<Subscriber<ipc::Service, [u8; INC_MAX_BYTES], ()>> {
        let service_name = format!("dat_pbs/{}/incremental", venue);
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; INC_MAX_BYTES]>()
            .open()?;

        let subscriber = service.subscriber_builder().create()?;
        Ok(subscriber)
    }

    /// 主循环
    pub fn run(&mut self) -> Result<()> {
        info!("DepthMsgApp[{}] starting main loop", self.venue_slug);
        let mut last_stats_time = Instant::now();
        loop {
            let has_message = self.poll_once()?;
            if !has_message {
                thread::sleep(Duration::from_micros(IDLE_SLEEP_MICROS));
            }
            if last_stats_time.elapsed() >= Duration::from_secs(STATS_INTERVAL_SECS) {
                self.log_stats();
                last_stats_time = Instant::now();
            }
        }
    }

    pub(crate) fn poll_once(&mut self) -> Result<bool> {
        let mut has_message = false;
        let mut inc_drained = 0usize;
        while inc_drained < MAX_INC_DRAIN_PER_POLL {
            let Some(sample) = self.subscriber.receive()? else {
                break;
            };
            has_message = true;
            inc_drained += 1;
            let data = sample.payload().to_vec();
            self.process_message(&data);
        }

        has_message |= self.poll_query_server()?;

        if !has_message {
            self.idle_check_counter += 1;
            if self.idle_check_counter >= self.idle_check_every {
                self.idle_check_counter = 0;
                self.check_timer_push();
            }
        } else {
            self.idle_check_counter = 0;
        }

        Ok(has_message)
    }

    fn poll_query_server(&mut self) -> Result<bool> {
        let source = DepthQueryAppSource {
            snapshots: Arc::clone(&self.query_snapshots),
        };
        let activity = self.query_server.poll(
            &mut self.query_connections,
            MAX_QUERY_ACCEPTS_PER_POLL,
            MAX_QUERY_REQUESTS_PER_POLL,
            |payload, resp| build_query_response(&source, payload, resp),
        )?;
        Ok(activity > 0)
    }

    /// 处理增量消息
    fn process_message(&mut self, data: &[u8]) {
        // 解析消息类型
        if data.len() < 8 {
            return;
        }

        let msg_type = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        // OrderBookInc = 1005
        if msg_type != 1005 {
            return;
        }

        // 解析 symbol
        let symbol_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        if data.len() < 8 + symbol_len + 32 {
            return;
        }

        let symbol = match std::str::from_utf8(&data[8..8 + symbol_len]) {
            Ok(s) => s.to_string(),
            Err(_) => return,
        };

        // 解析 update_id 和 timestamp
        let mut offset = 8 + symbol_len;
        let first_update_id = i64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);
        offset += 8;

        let final_update_id = i64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);
        offset += 8;

        let timestamp = i64::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);
        offset += 8;

        // is_snapshot (1 byte) + padding (7 bytes, padding[0] is is_last, padding[1] is chunk_index)
        let is_snapshot = data[offset] != 0;
        let is_last = data[offset + 1] != 0;
        let chunk_index = data[offset + 2];
        offset += 8;

        // bids_count 和 asks_count
        if data.len() < offset + 8 {
            return;
        }
        let bids_count = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;
        let asks_count = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        // 解析 levels
        let total_levels = bids_count + asks_count;
        if data.len() < offset + total_levels * 16 {
            return;
        }

        let mut bids = Vec::with_capacity(bids_count);
        let mut asks = Vec::with_capacity(asks_count);

        for _ in 0..bids_count {
            let price = f64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);
            offset += 8;
            let amount = f64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);
            offset += 8;
            bids.push((price, amount));
        }

        for _ in 0..asks_count {
            let price = f64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);
            offset += 8;
            let amount = f64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);
            offset += 8;
            asks.push((price, amount));
        }

        // 更新订单簿
        let state = self
            .symbols
            .entry(symbol.clone())
            .or_insert_with(SymbolState::new);

        let should_push = if is_snapshot && uses_full_snapshot_replacement(self.venue) {
            let committed = state.apply_hyperliquid_snapshot_chunk(
                first_update_id,
                final_update_id,
                timestamp,
                chunk_index,
                is_last,
                bids,
                asks,
            );
            if committed {
                debug!(
                    "Complete Hyperliquid snapshot replaced for {}: update_id={} timestamp={}",
                    symbol, final_update_id, timestamp
                );
                self.update_count += 1;
            }
            committed
        } else {
            // 滑动窗口去重：检查 (update_id, chunk_index) 是否已处理过
            if state.is_duplicate(final_update_id, chunk_index) {
                debug!(
                    "Duplicate msg (update_id={}, chunk_index={}) for {}, skipping",
                    final_update_id, chunk_index, symbol
                );
                return;
            }

            if is_snapshot {
                state
                    .orderbook
                    .apply_snapshot(&bids, &asks, final_update_id, timestamp);
                debug!(
                    "Snapshot applied for {}: {} bids, {} asks",
                    symbol, bids_count, asks_count
                );
            } else {
                state
                    .orderbook
                    .apply_update(&bids, &asks, final_update_id, timestamp);
            }
            state.query_snapshot_dirty = true;

            self.update_count += 1;
            is_last
        };

        if should_push {
            // 立即推送 (change-driven)
            self.push_depth(&symbol);
        }

        self.timer_check_counter += 1;
        if self.timer_check_counter >= TIMER_CHECK_EVERY_INCS {
            self.timer_check_counter = 0;
            self.check_timer_push();
        }
    }

    /// 检查定时推送
    fn check_timer_push(&mut self) {
        self.log_btc_depth25();
        self.log_publish_outcome_10s();

        let now = Instant::now();
        let symbols_to_push: Vec<String> = self
            .symbols
            .iter()
            .filter(|(_, state)| now.duration_since(state.last_push_time) >= self.push_interval)
            .map(|(symbol, _)| symbol.clone())
            .collect();

        for symbol in symbols_to_push {
            self.push_depth(&symbol);
        }
    }

    fn log_btc_depth25(&mut self) {
        if self.last_btc_depth25_log.elapsed() < Duration::from_secs(BTC_DEPTH25_LOG_INTERVAL_SECS)
        {
            return;
        }
        self.last_btc_depth25_log = Instant::now();

        for (symbol, state) in &self.symbols {
            let is_btc = symbol
                .get(0..3)
                .map(|s| s.eq_ignore_ascii_case("BTC"))
                .unwrap_or(false);
            if !is_btc {
                continue;
            }
            let amount_scale = self.depth_amount_scale(symbol);
            let (bids, asks) = scaled_depth_levels(&state.orderbook, 25, amount_scale);
            info!(
                "DepthPubApp[{}] BTC depth25 {} bids={:?} asks={:?}",
                self.venue_slug, symbol, bids, asks
            );
        }
    }

    /// 推送深度快照
    fn push_depth(&mut self, symbol: &str) {
        let price_tick = self.lookup_price_tick(symbol);
        let amount_scale = self.depth_amount_scale(symbol);
        let publishes_full_snapshots = uses_full_snapshot_replacement(self.venue);
        let mut snapshot_to_publish = None;
        let mut depth25_msg = None;
        let mut attempted_channels = 0u8;
        let mut should_return_early = false;

        {
            let state = match self.symbols.get_mut(symbol) {
                Some(s) => s,
                None => return,
            };

            if !state.orderbook.is_valid() {
                let missing_side =
                    state.orderbook.bids.is_empty() || state.orderbook.asks.is_empty();
                if publishes_full_snapshots && missing_side {
                    debug!(
                        "Publishing one-sided/empty full snapshot: venue={} symbol={}",
                        self.venue_slug, symbol
                    );
                } else {
                    let pruned_levels = state.orderbook.prune_crossed_by_best_update_id();
                    if pruned_levels > 0 && state.orderbook.is_valid() {
                        debug!(
                        "Crossed-book pruned before publish: venue={} symbol={} strategy=best_level_update_id pruned_levels={}",
                        self.venue_slug, symbol, pruned_levels
                    );
                    } else {
                        self.publish_fail_invalid_count =
                            self.publish_fail_invalid_count.saturating_add(1);
                        if state.orderbook.bids.is_empty() || state.orderbook.asks.is_empty() {
                            self.publish_fail_missing_side_count =
                                self.publish_fail_missing_side_count.saturating_add(1);
                        } else {
                            self.publish_fail_crossed_book_count =
                                self.publish_fail_crossed_book_count.saturating_add(1);
                        }
                        should_return_early = true;
                    }
                }
            }

            if state.query_snapshot_dirty {
                snapshot_to_publish = Some(SymbolQuerySnapshot::from_orderbook_with_amount_scale(
                    &state.orderbook,
                    price_tick,
                    amount_scale,
                ));
                state.query_snapshot_dirty = false;
            }

            if !should_return_early {
                let timestamp = state.orderbook.timestamp;

                attempted_channels = attempted_channels.saturating_add(1);
                let (bids, asks) = scaled_depth_levels(&state.orderbook, 25, amount_scale);
                depth25_msg = Some(DepthMsg::depth25(symbol.to_string(), timestamp, bids, asks));

                state.last_push_time = Instant::now();
            }
        }

        if let Some(snapshot) = snapshot_to_publish {
            self.query_snapshots.publish(symbol, snapshot);
        }

        if should_return_early {
            return;
        }

        let mut sent_channels = 0u8;
        if let Some(msg) = depth25_msg.as_ref() {
            if self.publisher.publish_depth25(msg) {
                sent_channels = sent_channels.saturating_add(1);
            }
        }

        if attempted_channels == 0 || sent_channels > 0 {
            self.publish_success_count = self.publish_success_count.saturating_add(1);
        } else {
            self.publish_fail_send_count = self.publish_fail_send_count.saturating_add(1);
        }

        self.push_count += 1;
    }

    fn log_publish_outcome_10s(&mut self) {
        if self.last_publish_outcome_log.elapsed()
            < Duration::from_secs(PUBLISH_OUTCOME_LOG_INTERVAL_SECS)
        {
            return;
        }

        let fail_total = self
            .publish_fail_invalid_count
            .saturating_add(self.publish_fail_send_count);
        info!(
            "DepthMsgApp[{}] publish_outcome_10s: success={} fail_total={} fail_invalid={} fail_send={} fail_missing_side={} fail_crossed_book={}",
            self.venue_slug,
            self.publish_success_count,
            fail_total,
            self.publish_fail_invalid_count,
            self.publish_fail_send_count,
            self.publish_fail_missing_side_count,
            self.publish_fail_crossed_book_count
        );

        self.last_publish_outcome_log = Instant::now();
        self.publish_success_count = 0;
        self.publish_fail_invalid_count = 0;
        self.publish_fail_send_count = 0;
        self.publish_fail_missing_side_count = 0;
        self.publish_fail_crossed_book_count = 0;
    }

    fn symbol_key_for_table(&self, symbol: &str) -> String {
        match self.venue {
            TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
                symbol.to_uppercase().replace("-SWAP", "").replace('-', "")
            }
            TradingVenue::GateMargin | TradingVenue::GateFutures => {
                symbol.to_uppercase().replace(['_', '-'], "")
            }
            _ => symbol.to_uppercase(),
        }
    }

    fn lookup_price_tick(&self, symbol: &str) -> Option<f64> {
        let table_symbol_key = self.symbol_key_for_table(symbol);
        self.min_qty_table.price_tick(&table_symbol_key)
    }

    fn depth_amount_scale(&self, symbol: &str) -> f64 {
        if !self.venue.is_futures()
            || matches!(
                self.venue,
                TradingVenue::BinanceFutures
                    | TradingVenue::BinanceCoinFutures
                    | TradingVenue::BitgetCoinFutures
            )
        {
            return 1.0;
        }

        let table_symbol_key = self.symbol_key_for_table(symbol);
        self.min_qty_table
            .contract_multiplier_opt(&table_symbol_key)
            .filter(|value| value.is_finite() && *value > 0.0)
            .unwrap_or(1.0)
    }

    /// 打印统计
    fn log_stats(&mut self) {
        info!(
            "DepthMsgApp[{}] stats: symbols={}, updates={}, pushes={}",
            self.venue_slug,
            self.symbols.len(),
            self.update_count,
            self.push_count
        );
        self.publisher.log_stats();
        self.update_count = 0;
        self.push_count = 0;
    }
}

fn scale_depth_amounts(levels: &mut [(f64, f64)], amount_scale: f64) {
    for (_, amount) in levels.iter_mut() {
        *amount *= amount_scale;
    }
}

fn scaled_depth_levels(
    orderbook: &OrderBook,
    levels: usize,
    amount_scale: f64,
) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
    let (mut bids, mut asks) = orderbook.get_depth(levels);
    if (amount_scale - 1.0).abs() <= f64::EPSILON {
        return (bids, asks);
    }

    scale_depth_amounts(&mut bids, amount_scale);
    scale_depth_amounts(&mut asks, amount_scale);
    (bids, asks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hyperliquid_chunks_commit_only_after_contiguous_out_of_order_snapshot() {
        let mut state = SymbolState::new();
        state
            .orderbook
            .apply_update(&[(90.0, 9.0)], &[(110.0, 11.0)], 900, 900_000);

        assert!(!state.apply_hyperliquid_snapshot_chunk(
            1_000,
            1_000,
            1_000_000,
            1,
            true,
            vec![(99.0, 2.0)],
            vec![(102.0, 4.0)],
        ));
        assert!(!state.apply_hyperliquid_snapshot_chunk(
            1_000,
            1_000,
            1_000_000,
            1,
            true,
            vec![(99.0, 2.0)],
            vec![(102.0, 4.0)],
        ));
        assert_eq!(
            state.orderbook.get_depth(5),
            (vec![(90.0, 9.0)], vec![(110.0, 11.0)])
        );

        assert!(state.apply_hyperliquid_snapshot_chunk(
            1_000,
            1_000,
            1_000_000,
            0,
            false,
            vec![(100.0, 1.0)],
            vec![(101.0, 3.0)],
        ));
        assert_eq!(
            state.orderbook.get_depth(5),
            (
                vec![(100.0, 1.0), (99.0, 2.0)],
                vec![(101.0, 3.0), (102.0, 4.0)]
            )
        );
        assert_eq!(state.orderbook.amount_at_price(90.0), None);
        assert_eq!(state.orderbook.amount_at_price(110.0), None);
    }

    #[test]
    fn empty_hyperliquid_snapshot_clears_the_book() {
        let mut state = SymbolState::new();
        state
            .orderbook
            .apply_update(&[(100.0, 1.0)], &[(101.0, 1.0)], 1, 1_000);

        assert!(state.apply_hyperliquid_snapshot_chunk(
            2,
            2,
            2_000,
            0,
            true,
            Vec::new(),
            Vec::new(),
        ));
        assert_eq!(state.orderbook.get_depth(5), (Vec::new(), Vec::new()));
        assert_eq!(state.orderbook.last_update_id, 2);
        assert_eq!(state.orderbook.timestamp, 2_000);
    }

    #[test]
    fn distinct_hyperliquid_snapshots_with_same_metadata_replace_the_book() {
        let mut state = SymbolState::new();
        assert!(state.apply_hyperliquid_snapshot_chunk(
            1_000,
            1_000,
            1_000_000,
            0,
            true,
            vec![(100.0, 1.0)],
            vec![(101.0, 1.0)],
        ));

        state.query_snapshot_dirty = false;
        assert!(state.apply_hyperliquid_snapshot_chunk(
            1_000,
            1_000,
            1_000_000,
            0,
            true,
            vec![(99.0, 2.0)],
            vec![(102.0, 3.0)],
        ));

        assert_eq!(
            state.orderbook.get_depth(5),
            (vec![(99.0, 2.0)], vec![(102.0, 3.0)])
        );
        assert_eq!(state.orderbook.amount_at_price(100.0), None);
        assert_eq!(state.orderbook.amount_at_price(101.0), None);
        assert!(state.query_snapshot_dirty);
    }

    #[test]
    fn exact_duplicate_hyperliquid_snapshot_is_not_a_change() {
        let mut state = SymbolState::new();
        assert!(state.apply_hyperliquid_snapshot_chunk(
            1_000,
            1_000,
            1_000_000,
            0,
            true,
            vec![(100.0, 1.0)],
            vec![(101.0, 1.0)],
        ));

        state.query_snapshot_dirty = false;
        assert!(!state.apply_hyperliquid_snapshot_chunk(
            1_000,
            1_000,
            1_000_000,
            0,
            true,
            vec![(100.0, 1.0)],
            vec![(101.0, 1.0)],
        ));

        assert_eq!(
            state.orderbook.get_depth(5),
            (vec![(100.0, 1.0)], vec![(101.0, 1.0)])
        );
        assert!(!state.query_snapshot_dirty);
    }

    #[test]
    fn newer_hyperliquid_snapshot_discards_older_pending_chunks() {
        let mut state = SymbolState::new();
        assert!(!state.apply_hyperliquid_snapshot_chunk(
            1_000,
            1_000,
            1_000_000,
            0,
            false,
            vec![(98.0, 1.0)],
            vec![],
        ));

        assert!(!state.apply_hyperliquid_snapshot_chunk(
            2_000,
            2_000,
            2_000_000,
            1,
            true,
            vec![(99.0, 2.0)],
            vec![(102.0, 2.0)],
        ));
        assert!(!state.apply_hyperliquid_snapshot_chunk(
            1_000,
            1_000,
            1_000_000,
            1,
            true,
            vec![(97.0, 3.0)],
            vec![(103.0, 3.0)],
        ));
        assert!(state.apply_hyperliquid_snapshot_chunk(
            2_000,
            2_000,
            2_000_000,
            0,
            false,
            vec![(100.0, 1.0)],
            vec![(101.0, 1.0)],
        ));

        assert_eq!(
            state.orderbook.get_depth(5),
            (
                vec![(100.0, 1.0), (99.0, 2.0)],
                vec![(101.0, 1.0), (102.0, 2.0)]
            )
        );
        assert_eq!(state.orderbook.amount_at_price(98.0), None);
        assert_eq!(state.orderbook.amount_at_price(97.0), None);
    }

    #[test]
    fn conflicting_hyperliquid_chunk_rejects_generation_until_newer_sequence() {
        let mut assembler = HyperliquidSnapshotAssembler::default();
        let key = HyperliquidSnapshotKey {
            first_update_id: 1_000,
            final_update_id: 1_000,
            timestamp: 1_000_000,
        };
        assert!(assembler
            .add_chunk(key, 0, false, vec![(100.0, 1.0)], vec![])
            .is_none());
        assert!(assembler
            .add_chunk(key, 0, false, vec![(100.0, 2.0)], vec![])
            .is_none());
        assert!(assembler
            .add_chunk(key, 1, true, vec![(99.0, 1.0)], vec![(101.0, 1.0)])
            .is_none());

        let newer = HyperliquidSnapshotKey {
            first_update_id: 1_001,
            final_update_id: 1_001,
            timestamp: 1_001_000,
        };
        let complete = assembler
            .add_chunk(newer, 0, true, vec![(100.0, 3.0)], vec![(101.0, 4.0)])
            .expect("strictly newer snapshot should recover rejected generation");
        assert_eq!(complete.bids, vec![(100.0, 3.0)]);
        assert_eq!(complete.asks, vec![(101.0, 4.0)]);
    }

    #[test]
    fn applied_hyperliquid_sequence_rejects_equal_or_older_metadata_variants() {
        let mut assembler = HyperliquidSnapshotAssembler::default();
        let key = HyperliquidSnapshotKey {
            first_update_id: 1_000,
            final_update_id: 1_000,
            timestamp: 1_000_000,
        };
        assert!(assembler
            .add_chunk(key, 0, true, vec![(100.0, 1.0)], vec![(101.0, 1.0)])
            .is_some());

        for stale in [
            HyperliquidSnapshotKey {
                first_update_id: 999,
                final_update_id: 999,
                timestamp: 999_000,
            },
            HyperliquidSnapshotKey {
                first_update_id: 1_001,
                final_update_id: 1_000,
                timestamp: 1_001_000,
            },
        ] {
            assert!(assembler
                .add_chunk(stale, 0, true, vec![(98.0, 1.0)], vec![(102.0, 1.0)])
                .is_none());
        }
    }
}
