//! Depth Publisher 应用主模块
//!
//! 订阅 mkt_pub 的 incremental 数据，维护订单簿，发布深度快照

use anyhow::Result;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use indexmap::IndexSet;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use super::cfg::DepthPubConfig;
use super::depth_msg::DepthMsg;
use super::orderbook::OrderBook;
use super::publisher::DepthMsgPublisher;
use super::query_logic::build_query_response;
use super::query_server::DepthQuerySocketServer;
use super::query_snapshot::{QuerySnapshotStore, SymbolQuerySnapshot};
use crate::signal::common::TradingVenue;
use crate::signal::venue_min_qty_table::VenueMinQtyTable;

/// IceOryx 增量消息缓冲区大小 (与 mkt_pub 一致)
const INC_MAX_BYTES: usize = 2048;
const TIMER_CHECK_EVERY_INCS: u64 = 500;
const IDLE_SLEEP_MICROS: u64 = 100;
/// 滑动窗口大小：用于去重的最近 update_id 数量
const DEDUP_WINDOW_SIZE: usize = 4096 * 2;
const KEEPALIVE_PUSH_INTERVAL_MS: u64 = 1000;
const BTC_DEPTH25_LOG_INTERVAL_SECS: u64 = 30;
const PUBLISH_OUTCOME_LOG_INTERVAL_SECS: u64 = 10;

/// 每个 symbol 的状态
struct SymbolState {
    orderbook: OrderBook,
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
            last_push_time: Instant::now(),
            query_snapshot_dirty: true,
            dedup_msg_keys: IndexSet::with_capacity(DEDUP_WINDOW_SIZE),
        }
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
    config: DepthPubConfig,
    venue: TradingVenue,
    venue_slug: String,
    publisher: DepthMsgPublisher,
    subscriber: Subscriber<ipc::Service, [u8; INC_MAX_BYTES], ()>,
    query_snapshots: Arc<QuerySnapshotStore>,
    min_qty_table: VenueMinQtyTable,
    /// symbol -> SymbolState
    symbols: HashMap<String, SymbolState>,
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

impl DepthPubApp {
    /// 创建应用实例
    /// venue: 例如 TradingVenue::BinanceFutures
    pub async fn new(config: DepthPubConfig, venue: TradingVenue) -> Result<Self> {
        let venue_slug = venue.data_pub_slug();
        let push_interval = Duration::from_millis(KEEPALIVE_PUSH_INTERVAL_MS);
        let idle_check_every = std::cmp::max(
            1,
            (push_interval.as_micros() / IDLE_SLEEP_MICROS as u128) as u64,
        );

        let mut min_qty_table = VenueMinQtyTable::new(venue);
        min_qty_table.refresh().await?;

        // 创建发布器
        let publisher = DepthMsgPublisher::new(
            venue_slug,
            config.depth_levels.enable_depth25,
            config.depth_levels.enable_depth50,
        )?;

        // 创建订阅器
        let subscriber = Self::create_subscriber(publisher.node(), venue_slug)?;
        info!(
            "Subscribed to incremental channel: dat_pbs/{}/incremental",
            venue_slug
        );

        let query_snapshots = Arc::new(QuerySnapshotStore::new(venue_slug));

        info!(
            "DepthPubApp created for {}: keepalive_push_interval={}ms, depth25={}, depth50={}",
            venue_slug,
            KEEPALIVE_PUSH_INTERVAL_MS,
            config.depth_levels.enable_depth25,
            config.depth_levels.enable_depth50
        );

        Ok(Self {
            config,
            venue,
            venue_slug: venue_slug.to_string(),
            publisher,
            subscriber,
            query_snapshots,
            min_qty_table,
            symbols: HashMap::new(),
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
        let _query_thread = self.spawn_query_thread()?;
        info!("DepthMsgApp[{}] starting main loop", self.venue_slug);
        let mut last_stats_time = Instant::now();
        let stats_interval = Duration::from_secs(60);

        loop {
            // 处理订阅器的消息
            let mut has_message = false;
            while let Some(sample) = self.subscriber.receive()? {
                has_message = true;
                // 复制数据以避免借用冲突
                let data = sample.payload().to_vec();
                self.process_message(&data);
            }

            // 如果没有消息，短暂休眠避免 CPU 空转
            if !has_message {
                self.idle_check_counter += 1;
                if self.idle_check_counter >= self.idle_check_every {
                    self.idle_check_counter = 0;
                    self.check_timer_push();
                }
                std::thread::sleep(Duration::from_micros(IDLE_SLEEP_MICROS));
            } else {
                self.idle_check_counter = 0;
            }

            // 定期打印统计
            if last_stats_time.elapsed() >= stats_interval {
                self.log_stats();
                last_stats_time = Instant::now();
            }
        }
    }

    fn spawn_query_thread(&self) -> Result<thread::JoinHandle<()>> {
        let venue_slug = self.venue_slug.clone();
        let snapshots = Arc::clone(&self.query_snapshots);
        let query_server = DepthQuerySocketServer::bind(&venue_slug)?;
        let thread_name = format!("depth_query_{}", venue_slug.replace('-', "_"));
        let handle = thread::Builder::new().name(thread_name).spawn(move || {
            info!(
                "Depth query thread started: venue={} socket={}",
                venue_slug,
                snapshots.venue_slug()
            );
            loop {
                let Some(stream) = (match query_server.accept() {
                    Ok(stream) => stream,
                    Err(err) => {
                        warn!("Depth query accept failed: {err:#}");
                        thread::sleep(Duration::from_micros(IDLE_SLEEP_MICROS));
                        continue;
                    }
                }) else {
                    thread::sleep(Duration::from_micros(IDLE_SLEEP_MICROS));
                    continue;
                };

                let snapshots = Arc::clone(&snapshots);
                if let Err(err) = thread::Builder::new()
                    .name("depth_query_conn".to_string())
                    .spawn(move || {
                        if let Err(err) =
                            DepthQuerySocketServer::serve_stream(stream, |payload, resp| {
                                build_query_response(snapshots.as_ref(), payload, resp)
                            })
                        {
                            warn!("Depth query stream handling failed: {err:#}");
                        }
                    })
                {
                    warn!("Depth query worker spawn failed: {err:#}");
                }
            }
        })?;
        Ok(handle)
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
        let _first_update_id = i64::from_le_bytes([
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

        if is_last {
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
        let mut snapshot_to_publish = None;
        let mut depth25_msg = None;
        let mut depth50_msg = None;
        let mut attempted_channels = 0u8;
        let mut should_return_early = false;

        {
            let state = match self.symbols.get_mut(symbol) {
                Some(s) => s,
                None => return,
            };

            if !state.orderbook.is_valid() {
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

                if self.config.depth_levels.enable_depth25 {
                    attempted_channels = attempted_channels.saturating_add(1);
                    let (bids, asks) = scaled_depth_levels(&state.orderbook, 25, amount_scale);
                    depth25_msg =
                        Some(DepthMsg::depth25(symbol.to_string(), timestamp, bids, asks));
                }

                if self.config.depth_levels.enable_depth50 {
                    attempted_channels = attempted_channels.saturating_add(1);
                    let (bids, asks) = scaled_depth_levels(&state.orderbook, 50, amount_scale);
                    depth50_msg =
                        Some(DepthMsg::depth50(symbol.to_string(), timestamp, bids, asks));
                }

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
        if let Some(msg) = depth50_msg.as_ref() {
            if self.publisher.publish_depth50(msg) {
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
                symbol.to_uppercase().replace('_', "").replace('-', "")
            }
            _ => symbol.to_uppercase(),
        }
    }

    fn lookup_price_tick(&self, symbol: &str) -> Option<f64> {
        let table_symbol_key = self.symbol_key_for_table(symbol);
        self.min_qty_table.price_tick(&table_symbol_key)
    }

    fn depth_amount_scale(&self, symbol: &str) -> f64 {
        if !self.venue.is_futures() || matches!(self.venue, TradingVenue::BinanceFutures) {
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
