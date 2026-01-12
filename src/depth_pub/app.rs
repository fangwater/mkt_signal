//! Depth Publisher 应用主模块
//!
//! 订阅 mkt_pub 的 incremental 数据，维护订单簿，发布深度快照

use anyhow::Result;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{debug, info};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use super::cfg::DepthPubConfig;
use super::depth_msg::DepthMsg;
use super::orderbook::OrderBook;
use super::publisher::DepthMsgPublisher;

/// IceOryx 增量消息缓冲区大小 (与 mkt_pub 一致)
const INC_MAX_BYTES: usize = 16384;
const TIMER_CHECK_EVERY_INCS: u64 = 500;
const IDLE_SLEEP_MICROS: u64 = 100;
/// 滑动窗口大小：用于去重的最近 update_id 数量
const DEDUP_WINDOW_SIZE: usize = 256;

/// 每个 symbol 的状态
struct SymbolState {
    orderbook: OrderBook,
    last_push_time: Instant,
    /// 滑动窗口：存储最近处理过的 (update_id, chunk_index)，用于去重
    /// VecDeque 维护插入顺序（用于淘汰最旧的）
    recent_msg_keys: VecDeque<(i64, u8)>,
    /// HashSet 用于 O(1) 快速查找
    msg_key_set: HashSet<(i64, u8)>,
}

impl SymbolState {
    fn new() -> Self {
        Self {
            orderbook: OrderBook::new(),
            last_push_time: Instant::now(),
            recent_msg_keys: VecDeque::with_capacity(DEDUP_WINDOW_SIZE),
            msg_key_set: HashSet::with_capacity(DEDUP_WINDOW_SIZE),
        }
    }

    /// 检查 (update_id, chunk_index) 是否重复
    /// 返回 true 表示是重复的，应该跳过
    #[inline]
    fn is_duplicate(&mut self, update_id: i64, chunk_index: u8) -> bool {
        let key = (update_id, chunk_index);

        // O(1) 查找
        if self.msg_key_set.contains(&key) {
            return true;
        }

        // 不重复，加入窗口
        if self.recent_msg_keys.len() >= DEDUP_WINDOW_SIZE {
            // 淘汰最旧的
            if let Some(old_key) = self.recent_msg_keys.pop_front() {
                self.msg_key_set.remove(&old_key);
            }
        }
        self.recent_msg_keys.push_back(key);
        self.msg_key_set.insert(key);
        false
    }
}

/// Depth Publisher 应用
pub struct DepthPubApp {
    config: DepthPubConfig,
    venue_slug: String,
    publisher: DepthMsgPublisher,
    subscriber: Subscriber<ipc::Service, [u8; INC_MAX_BYTES], ()>,
    /// symbol -> SymbolState
    symbols: HashMap<String, SymbolState>,
    /// 推送间隔
    push_interval: Duration,
    /// 统计
    update_count: u64,
    push_count: u64,
    timer_check_counter: u64,
    idle_check_counter: u64,
    idle_check_every: u64,
}

impl DepthPubApp {
    /// 创建应用实例
    /// venue_slug: 例如 "binance-futures", "okex-margin"
    pub fn new(config: DepthPubConfig, venue_slug: &str) -> Result<Self> {
        let push_interval = Duration::from_millis(config.push_config.min_push_interval_ms);
        let idle_check_every = std::cmp::max(
            1,
            (push_interval.as_micros() / IDLE_SLEEP_MICROS as u128) as u64,
        );

        // 创建发布器
        let publisher = DepthMsgPublisher::new(
            venue_slug,
            config.depth_levels.enable_depth5,
            config.depth_levels.enable_depth20,
            config.depth_levels.enable_depth50,
        )?;

        // 创建订阅器
        let subscriber = Self::create_subscriber(publisher.node(), venue_slug)?;
        info!("Subscribed to incremental channel: data_pubs/{}/incremental", venue_slug);

        info!(
            "DepthPubApp created for {}: push_interval={}ms, depth5={}, depth20={}, depth50={}",
            venue_slug,
            config.push_config.min_push_interval_ms,
            config.depth_levels.enable_depth5,
            config.depth_levels.enable_depth20,
            config.depth_levels.enable_depth50
        );

        Ok(Self {
            config,
            venue_slug: venue_slug.to_string(),
            publisher,
            subscriber,
            symbols: HashMap::new(),
            push_interval,
            update_count: 0,
            push_count: 0,
            timer_check_counter: 0,
            idle_check_counter: 0,
            idle_check_every,
        })
    }

    /// 创建 IceOryx 订阅器
    fn create_subscriber(
        node: &Node<ipc::Service>,
        venue: &str,
    ) -> Result<Subscriber<ipc::Service, [u8; INC_MAX_BYTES], ()>> {
        let service_name = format!("data_pubs/{}/incremental", venue);
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
        let bids_count =
            u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]])
                as usize;
        offset += 4;
        let asks_count =
            u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]])
                as usize;
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
        let state = self.symbols.entry(symbol.clone()).or_insert_with(SymbolState::new);

        // 滑动窗口去重：检查 (update_id, chunk_index) 是否已处理过
        if state.is_duplicate(final_update_id, chunk_index) {
            debug!(
                "Duplicate msg (update_id={}, chunk_index={}) for {}, skipping",
                final_update_id, chunk_index, symbol
            );
            return;
        }

        if is_snapshot {
            state.orderbook.apply_snapshot(&bids, &asks, final_update_id, timestamp);
            debug!("Snapshot applied for {}: {} bids, {} asks", symbol, bids_count, asks_count);
        } else {
            state.orderbook.apply_update(&bids, &asks, final_update_id, timestamp);
        }

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
        self.log_btc_depth5();

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

    fn log_btc_depth5(&self) {
        for (symbol, state) in &self.symbols {
            let is_btc = symbol
                .get(0..3)
                .map(|s| s.eq_ignore_ascii_case("BTC"))
                .unwrap_or(false);
            if !is_btc {
                continue;
            }
            let (bids, asks) = state.orderbook.get_depth(5);
            info!(
                "DepthPubApp[{}] BTC depth5 {} bids={:?} asks={:?}",
                self.venue_slug, symbol, bids, asks
            );
        }
    }

    /// 推送深度快照
    fn push_depth(&mut self, symbol: &str) {
        let state = match self.symbols.get_mut(symbol) {
            Some(s) => s,
            None => return,
        };

        if !state.orderbook.is_valid() {
            return;
        }

        let timestamp = state.orderbook.timestamp;

        // Depth5
        if self.config.depth_levels.enable_depth5 {
            let (bids, asks) = state.orderbook.get_depth(5);
            let msg = DepthMsg::depth5(symbol.to_string(), timestamp, bids, asks);
            self.publisher.publish_depth5(&msg);
        }

        // Depth20
        if self.config.depth_levels.enable_depth20 {
            let (bids, asks) = state.orderbook.get_depth(20);
            let msg = DepthMsg::depth20(symbol.to_string(), timestamp, bids, asks);
            self.publisher.publish_depth20(&msg);
        }

        // Depth50
        if self.config.depth_levels.enable_depth50 {
            let (bids, asks) = state.orderbook.get_depth(50);
            let msg = DepthMsg::depth50(symbol.to_string(), timestamp, bids, asks);
            self.publisher.publish_depth50(&msg);
        }

        state.last_push_time = Instant::now();
        self.push_count += 1;
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
