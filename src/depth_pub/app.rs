//! Depth Publisher 应用主模块
//!
//! 订阅 mkt_pub 的 incremental 数据，维护订单簿，发布深度快照

use anyhow::{anyhow, Result};
use iceoryx2::port::publisher::Publisher;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use indexmap::IndexSet;
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use super::cfg::{
    DepthAccountSubscriptionConfig, DepthAccountSubscriptionsConfig,
    DEFAULT_ACCOUNT_SUBSCRIPTIONS_PATH,
};
use super::depth_msg::DepthMsg;
use super::order_queue_msg::{OrderQueuePositionMsg, ORDER_QUEUE_POSITION_MAX_BYTES};
use super::orderbook::{price_to_key, OrderBook};
use super::publisher::DepthMsgPublisher;
use super::query_logic::{build_query_response, DepthQuerySource};
use super::query_server::{DepthQueryConnection, DepthQuerySocketServer};
use super::query_snapshot::{QuerySnapshotStore, SymbolQuerySnapshot};
use super::queue_position::{
    book_side_from_orderbook, QueuePositionPublishEvent, QueuePositionSnapshot, QueuePositionState,
    QueuePositionStats,
};
use crate::common::trade_msg_parser::{parse_trade, TradeSide};
use crate::portfolio_margin::pm_forwarder::{
    PM_HISTORY_SIZE, PM_MAX_SUBSCRIBERS, PM_SUBSCRIBER_MAX_BUFFER_SIZE,
};
use order_common::TradingVenue;
use signal_common::venue_min_qty_table::VenueMinQtyTable;

/// IceOryx 增量消息缓冲区大小 (与 mkt_pub 一致)
const INC_MAX_BYTES: usize = 2048;
const TRADE_MAX_BYTES: usize = 128;
const ACCOUNT_PAYLOAD: usize = 16_384;
const TIMER_CHECK_EVERY_INCS: u64 = 500;
const IDLE_SLEEP_MICROS: u64 = 100;
/// 滑动窗口大小：用于去重的最近 update_id 数量
const DEDUP_WINDOW_SIZE: usize = 4096 * 2;
const KEEPALIVE_PUSH_INTERVAL_MS: u64 = 1000;
const BTC_DEPTH25_LOG_INTERVAL_SECS: u64 = 30;
const PUBLISH_OUTCOME_LOG_INTERVAL_SECS: u64 = 10;
const QUEUE_POSITION_CLEANUP_INTERVAL_SECS: u64 = 60;
const MAX_INC_DRAIN_PER_POLL: usize = 256;
const MAX_TRADE_DRAIN_PER_POLL: usize = 256;
const MAX_ACCOUNT_DRAIN_PER_POLL: usize = 256;
const MAX_QUERY_ACCEPTS_PER_POLL: usize = 8;
const MAX_QUERY_REQUESTS_PER_POLL: usize = 64;
const STATS_INTERVAL_SECS: u64 = 60;

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

struct AccountSubscription {
    account_id: String,
    service_name: String,
    order_queue_service_name: String,
    venue: TradingVenue,
    amount_scale: f64,
    subscriber: Subscriber<ipc::Service, [u8; ACCOUNT_PAYLOAD], ()>,
    order_queue_publisher: Publisher<ipc::Service, [u8; ORDER_QUEUE_POSITION_MAX_BYTES], ()>,
    order_queue_publish_count: u64,
    order_queue_drop_count: u64,
}

impl AccountSubscription {
    fn matches_config(&self, cfg: &DepthAccountSubscriptionConfig) -> bool {
        self.account_id == cfg.account_id
            && self.service_name == cfg.service_name
            && derive_order_pos_service_name(cfg)
                .map(|service_name| self.order_queue_service_name == service_name)
                .unwrap_or(false)
            && self.venue == cfg.venue
            && (self.amount_scale - cfg.amount_scale).abs() <= f64::EPSILON
    }

    fn publish_order_queue_position(&mut self, msg: &OrderQueuePositionMsg) -> bool {
        let bytes = msg.to_bytes();
        if bytes.len() > ORDER_QUEUE_POSITION_MAX_BYTES {
            self.order_queue_drop_count = self.order_queue_drop_count.saturating_add(1);
            return false;
        }

        let mut buffer = [0u8; ORDER_QUEUE_POSITION_MAX_BYTES];
        buffer[..bytes.len()].copy_from_slice(&bytes);
        match self.order_queue_publisher.loan_uninit() {
            Ok(sample) => {
                let sample = sample.write_payload(buffer);
                if sample.send().is_ok() {
                    self.order_queue_publish_count =
                        self.order_queue_publish_count.saturating_add(1);
                    true
                } else {
                    self.order_queue_drop_count = self.order_queue_drop_count.saturating_add(1);
                    false
                }
            }
            Err(_) => {
                self.order_queue_drop_count = self.order_queue_drop_count.saturating_add(1);
                false
            }
        }
    }
}

#[derive(Default)]
struct QueuePositionAccounts {
    states: HashMap<String, QueuePositionState>,
}

impl QueuePositionAccounts {
    fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    fn resolve_order_snapshot(
        &self,
        account_id: Option<&str>,
        client_order_id: i64,
    ) -> Option<QueuePositionSnapshot> {
        if let Some(account_id) = account_id {
            return self
                .states
                .get(account_id)
                .and_then(|state| state.order_snapshot(client_order_id));
        }

        self.states
            .values()
            .find_map(|state| state.order_snapshot(client_order_id))
    }

    fn apply_level_qty_all(
        &mut self,
        symbol: &str,
        side: queue_position_engine::BookSide,
        price_key: i64,
        qty: f64,
        update_tp: i64,
        local_tp: i64,
    ) -> Vec<QueuePositionPublishEvent> {
        let mut events = Vec::new();
        for state in self.states.values_mut() {
            events.extend(state.apply_level_qty(symbol, side, price_key, qty, update_tp, local_tp));
        }
        events
    }

    fn apply_public_trade_all(
        &mut self,
        symbol: &str,
        side: queue_position_engine::Side,
        price_key: i64,
        qty: f64,
        update_tp: i64,
        local_tp: i64,
    ) -> Vec<QueuePositionPublishEvent> {
        let mut events = Vec::new();
        for state in self.states.values_mut() {
            events.extend(
                state.apply_public_trade(symbol, side, price_key, qty, update_tp, local_tp),
            );
        }
        events
    }

    fn process_account_payload(
        &mut self,
        account_id: &str,
        payload: &[u8],
        now_ms: i64,
        local_tp: i64,
    ) -> Vec<QueuePositionPublishEvent> {
        if let Some(state) = self.states.get_mut(account_id) {
            state.process_account_payload(payload, now_ms, local_tp)
        } else {
            Vec::new()
        }
    }

    fn clear_orders_older_than_ms(&mut self, now_ms: i64, ttl_ms: i64) -> usize {
        self.states
            .values_mut()
            .map(|state| state.clear_orders_older_than_ms(now_ms, ttl_ms))
            .sum()
    }

    fn collect_and_reset_stats(&mut self) -> Vec<(String, usize, QueuePositionStats)> {
        let mut stats = Vec::with_capacity(self.states.len());
        for (account_id, state) in self.states.iter_mut() {
            let len = state.len();
            let account_stats = state.stats();
            state.reset_interval_stats();
            stats.push((account_id.clone(), len, account_stats));
        }
        stats
    }
}

/// Depth Publisher 应用
pub struct DepthPubApp {
    venue: TradingVenue,
    venue_slug: String,
    publisher: DepthMsgPublisher,
    subscriber: Subscriber<ipc::Service, [u8; INC_MAX_BYTES], ()>,
    trade_subscriber: Option<Subscriber<ipc::Service, [u8; TRADE_MAX_BYTES], ()>>,
    account_subscriptions_path: String,
    account_subscriptions: Vec<AccountSubscription>,
    query_snapshots: Arc<QuerySnapshotStore>,
    query_server: DepthQuerySocketServer,
    query_connections: Vec<DepthQueryConnection>,
    queue_positions: Option<Arc<Mutex<QueuePositionAccounts>>>,
    account_reload_interval: Duration,
    order_ttl: Duration,
    last_account_reload: Instant,
    last_queue_cleanup: Instant,
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

struct DepthQueryAppSource {
    snapshots: Arc<QuerySnapshotStore>,
    queue_positions: Option<Arc<Mutex<QueuePositionAccounts>>>,
}

impl DepthQuerySource for DepthQueryAppSource {
    fn venue_slug(&self) -> &str {
        self.snapshots.venue_slug()
    }

    fn resolve_snapshot(&self, symbol: &str) -> Option<Arc<SymbolQuerySnapshot>> {
        self.snapshots.load(symbol)
    }

    fn resolve_order_queue_position(
        &self,
        account_id: Option<&str>,
        client_order_id: i64,
    ) -> Option<QueuePositionSnapshot> {
        let queue_positions = self.queue_positions.as_ref()?;
        queue_positions
            .lock()
            .ok()
            .and_then(|state| state.resolve_order_snapshot(account_id, client_order_id))
    }
}

pub struct DepthPubRunner {
    apps: Vec<DepthPubApp>,
}

impl DepthPubRunner {
    pub async fn new(venues: Vec<TradingVenue>) -> Result<Self> {
        let mut seen = HashSet::new();
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
        let trade_subscriber = match Self::create_trade_subscriber(publisher.node(), venue_slug) {
            Ok(subscriber) => Some(subscriber),
            Err(err) => {
                warn!(
                    "DepthPubApp[{}] public trade subscription disabled: {err:#}",
                    venue_slug
                );
                None
            }
        };
        let account_subscriptions_path = DEFAULT_ACCOUNT_SUBSCRIPTIONS_PATH.to_string();
        let account_config = Self::load_account_subscriptions_config(&account_subscriptions_path);
        let account_reload_interval =
            Duration::from_secs(account_config.runtime.reload_interval_secs);
        let order_ttl = Duration::from_secs(account_config.runtime.order_ttl_secs);
        let (account_subscriptions, queue_accounts) = if account_config.runtime.enabled {
            Self::build_account_subscriptions(publisher.node(), venue, &account_config.accounts)
        } else {
            (Vec::new(), QueuePositionAccounts::default())
        };
        if !account_config.runtime.enabled {
            info!(
                "DepthPubApp[{}] account subscriptions disabled by config",
                venue_slug
            );
        }
        let queue_positions = Some(Arc::new(Mutex::new(queue_accounts)));

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
            trade_subscriber,
            account_subscriptions_path,
            account_subscriptions,
            query_snapshots,
            query_server,
            query_connections: Vec::new(),
            queue_positions,
            account_reload_interval,
            order_ttl,
            last_account_reload: Instant::now(),
            last_queue_cleanup: Instant::now(),
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

    fn create_trade_subscriber(
        node: &Node<ipc::Service>,
        venue: &str,
    ) -> Result<Subscriber<ipc::Service, [u8; TRADE_MAX_BYTES], ()>> {
        let service_name = format!("dat_pbs/{}/trade", venue);
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; TRADE_MAX_BYTES]>()
            .open()?;

        let subscriber = service.subscriber_builder().create()?;
        info!("Subscribed to public trade channel: {}", service_name);
        Ok(subscriber)
    }

    fn create_account_subscriber(
        node: &Node<ipc::Service>,
        service_name: &str,
    ) -> Result<Subscriber<ipc::Service, [u8; ACCOUNT_PAYLOAD], ()>> {
        let service = node
            .service_builder(&ServiceName::new(&service_name)?)
            .publish_subscribe::<[u8; ACCOUNT_PAYLOAD]>()
            .max_publishers(1)
            .max_subscribers(PM_MAX_SUBSCRIBERS)
            .history_size(PM_HISTORY_SIZE)
            .subscriber_max_buffer_size(PM_SUBSCRIBER_MAX_BUFFER_SIZE)
            .open()?;

        let subscriber = service.subscriber_builder().create()?;
        info!("Subscribed to account monitor channel: {}", service_name);
        Ok(subscriber)
    }

    fn create_order_queue_publisher(
        node: &Node<ipc::Service>,
        service_name: &str,
    ) -> Result<Publisher<ipc::Service, [u8; ORDER_QUEUE_POSITION_MAX_BYTES], ()>> {
        let service = node
            .service_builder(&ServiceName::new(service_name)?)
            .publish_subscribe::<[u8; ORDER_QUEUE_POSITION_MAX_BYTES]>()
            .max_publishers(1)
            .max_subscribers(10)
            .history_size(1024)
            .open_or_create()?;

        let publisher = service.publisher_builder().create()?;
        info!("Publishing order queue position channel: {}", service_name);
        Ok(publisher)
    }

    fn load_account_subscriptions_config(path: &str) -> DepthAccountSubscriptionsConfig {
        match DepthAccountSubscriptionsConfig::load_sync(path) {
            Ok(config) => config,
            Err(err) => {
                warn!(
                    "depth account subscriptions config unavailable, account queue positions disabled until reload succeeds: path={} err={:#}",
                    path, err
                );
                DepthAccountSubscriptionsConfig::default()
            }
        }
    }

    fn build_account_subscriptions(
        node: &Node<ipc::Service>,
        venue: TradingVenue,
        configs: &[DepthAccountSubscriptionConfig],
    ) -> (Vec<AccountSubscription>, QueuePositionAccounts) {
        let mut subscriptions = Vec::new();
        let mut accounts = QueuePositionAccounts::default();
        let mut seen = HashSet::new();

        for cfg in configs {
            if cfg.venue != venue {
                continue;
            }
            if !seen.insert(cfg.account_id.clone()) {
                warn!(
                    "duplicate depth account subscription ignored: account_id={} service={}",
                    cfg.account_id, cfg.service_name
                );
                continue;
            }
            let Some(state) = QueuePositionState::new_for_account(
                cfg.account_id.clone(),
                cfg.venue,
                cfg.amount_scale,
            ) else {
                warn!(
                    "unsupported depth account subscription venue: account_id={} venue={:?}",
                    cfg.account_id, cfg.venue
                );
                continue;
            };

            match Self::create_account_subscription(node, cfg) {
                Ok(subscription) => {
                    accounts.states.insert(cfg.account_id.clone(), state);
                    subscriptions.push(subscription);
                }
                Err(err) => {
                    warn!(
                        "DepthPubApp account monitor subscription unavailable: account_id={} service={} err={:#}",
                        cfg.account_id, cfg.service_name, err
                    );
                }
            }
        }

        info!(
            "DepthPubApp account subscriptions active: count={}",
            subscriptions.len()
        );
        (subscriptions, accounts)
    }

    fn create_account_subscription(
        node: &Node<ipc::Service>,
        cfg: &DepthAccountSubscriptionConfig,
    ) -> Result<AccountSubscription> {
        let subscriber = Self::create_account_subscriber(node, &cfg.service_name)?;
        let order_queue_service_name = derive_order_pos_service_name(cfg)?;
        let order_queue_publisher =
            Self::create_order_queue_publisher(node, &order_queue_service_name)?;
        Ok(AccountSubscription {
            account_id: cfg.account_id.clone(),
            service_name: cfg.service_name.clone(),
            order_queue_service_name,
            venue: cfg.venue,
            amount_scale: cfg.amount_scale,
            subscriber,
            order_queue_publisher,
            order_queue_publish_count: 0,
            order_queue_drop_count: 0,
        })
    }

    fn maybe_reload_account_subscriptions(&mut self) {
        if self.last_account_reload.elapsed() < self.account_reload_interval {
            return;
        }
        self.last_account_reload = Instant::now();

        let cfg = Self::load_account_subscriptions_config(&self.account_subscriptions_path);
        self.account_reload_interval = Duration::from_secs(cfg.runtime.reload_interval_secs);
        self.order_ttl = Duration::from_secs(cfg.runtime.order_ttl_secs);
        let accounts: Vec<DepthAccountSubscriptionConfig> = cfg
            .accounts
            .into_iter()
            .filter(|account| account.venue == self.venue)
            .collect();

        if !cfg.runtime.enabled {
            if !self.account_subscriptions.is_empty() {
                info!(
                    "DepthPubApp[{}] account subscriptions disabled on reload; dropping {} subscribers",
                    self.venue_slug,
                    self.account_subscriptions.len()
                );
            }
            self.account_subscriptions.clear();
            return;
        }

        if self.queue_positions.is_none() {
            self.queue_positions = Some(Arc::new(Mutex::new(QueuePositionAccounts::default())));
        }
        self.reload_account_subscriptions(&accounts);
    }

    fn reload_account_subscriptions(&mut self, configs: &[DepthAccountSubscriptionConfig]) {
        let desired: HashMap<String, DepthAccountSubscriptionConfig> = configs
            .iter()
            .map(|cfg| (cfg.account_id.clone(), cfg.clone()))
            .collect();

        let before = self.account_subscriptions.len();
        let mut removed = 0usize;
        self.account_subscriptions.retain(|sub| {
            let keep = desired
                .get(&sub.account_id)
                .map(|cfg| sub.matches_config(cfg))
                .unwrap_or(false);
            if !keep {
                removed += 1;
            }
            keep
        });
        let active: HashSet<String> = self
            .account_subscriptions
            .iter()
            .map(|sub| sub.account_id.clone())
            .collect();

        let mut added = 0usize;
        for cfg in desired.values() {
            if active.contains(&cfg.account_id) {
                continue;
            }
            let Some(state) = QueuePositionState::new_for_account(
                cfg.account_id.clone(),
                cfg.venue,
                cfg.amount_scale,
            ) else {
                warn!(
                    "unsupported depth account subscription venue on reload: account_id={} venue={:?}",
                    cfg.account_id, cfg.venue
                );
                continue;
            };
            match Self::create_account_subscription(self.publisher.node(), cfg) {
                Ok(subscription) => {
                    if let Some(queue_positions) = self.queue_positions.as_ref() {
                        if let Ok(mut accounts) = queue_positions.lock() {
                            accounts
                                .states
                                .entry(cfg.account_id.clone())
                                .or_insert(state);
                        }
                    }
                    self.account_subscriptions.push(subscription);
                    added += 1;
                }
                Err(err) => {
                    warn!(
                        "DepthPubApp account monitor subscription unavailable on reload: account_id={} service={} err={:#}",
                        cfg.account_id, cfg.service_name, err
                    );
                }
            }
        }

        if let Some(queue_positions) = self.queue_positions.as_ref() {
            if let Ok(mut accounts) = queue_positions.lock() {
                for cfg in desired.values() {
                    if !accounts.states.contains_key(&cfg.account_id) {
                        if let Some(state) = QueuePositionState::new_for_account(
                            cfg.account_id.clone(),
                            cfg.venue,
                            cfg.amount_scale,
                        ) {
                            accounts.states.insert(cfg.account_id.clone(), state);
                        }
                    }
                }
            }
        } else {
            self.queue_positions = Some(Arc::new(Mutex::new(QueuePositionAccounts::default())));
        }

        if added > 0 || removed > 0 || before != self.account_subscriptions.len() {
            info!(
                "DepthPubApp[{}] account subscriptions reloaded: before={} after={} desired={} added={} removed_or_changed={}",
                self.venue_slug,
                before,
                self.account_subscriptions.len(),
                desired.len(),
                added,
                removed
            );
        } else {
            debug!(
                "DepthPubApp[{}] account subscriptions unchanged on config reload: active={} desired={}",
                self.venue_slug,
                self.account_subscriptions.len(),
                desired.len()
            );
        }
    }

    fn maybe_cleanup_expired_queue_orders(&mut self) {
        if self.last_queue_cleanup.elapsed()
            < Duration::from_secs(QUEUE_POSITION_CLEANUP_INTERVAL_SECS)
        {
            return;
        }
        self.last_queue_cleanup = Instant::now();

        let Some(queue_positions) = self.queue_positions.as_ref() else {
            return;
        };
        let ttl_ms = self.order_ttl.as_millis() as i64;
        let now_ms = crate::common::time_util::get_timestamp_us() / 1_000;
        if let Ok(mut accounts) = queue_positions.lock() {
            let expired = accounts.clear_orders_older_than_ms(now_ms, ttl_ms);
            if expired > 0 {
                info!(
                    "DepthPubApp[{}] expired queue position orders cleared: count={} ttl_secs={}",
                    self.venue_slug,
                    expired,
                    self.order_ttl.as_secs()
                );
            }
        }
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

    fn poll_once(&mut self) -> Result<bool> {
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

        has_message |= self.drain_public_trades(MAX_TRADE_DRAIN_PER_POLL)?;
        has_message |= self.drain_account_updates(MAX_ACCOUNT_DRAIN_PER_POLL)?;
        has_message |= self.poll_query_server()?;
        self.maybe_reload_account_subscriptions();
        self.maybe_cleanup_expired_queue_orders();

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
            queue_positions: self.queue_positions.as_ref().map(Arc::clone),
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

        let mut queue_level_updates = Vec::with_capacity(bids.len() + asks.len());
        for (price, amount) in &bids {
            queue_level_updates.push((
                book_side_from_orderbook(true),
                price_to_key(*price),
                state.orderbook.bids.amount_at_price(*price).unwrap_or(0.0),
                *amount,
            ));
        }
        for (price, amount) in &asks {
            queue_level_updates.push((
                book_side_from_orderbook(false),
                price_to_key(*price),
                state.orderbook.asks.amount_at_price(*price).unwrap_or(0.0),
                *amount,
            ));
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
        self.apply_queue_level_updates(&symbol, queue_level_updates, timestamp_to_us(timestamp));

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

    fn apply_queue_level_updates(
        &mut self,
        symbol: &str,
        updates: Vec<(queue_position_engine::BookSide, i64, f64, f64)>,
        update_tp: i64,
    ) {
        let mut events = Vec::new();
        let Some(queue_positions) = self.queue_positions.as_ref() else {
            return;
        };
        let Ok(mut state) = queue_positions.lock() else {
            return;
        };
        if state.is_empty() {
            return;
        }
        for (side, price_key, old_qty, new_qty) in updates {
            if (old_qty - new_qty).abs() <= f64::EPSILON {
                continue;
            }
            let local_tp = crate::common::time_util::get_timestamp_us();
            events.extend(
                state.apply_level_qty_all(symbol, side, price_key, new_qty, update_tp, local_tp),
            );
        }
        drop(state);
        self.publish_order_queue_events(events);
    }

    fn drain_public_trades(&mut self, max_messages: usize) -> Result<bool> {
        let mut events = Vec::new();
        let mut has_message = false;
        {
            let Some(subscriber) = self.trade_subscriber.as_ref() else {
                return Ok(false);
            };
            let mut drained = 0usize;
            while drained < max_messages {
                let Some(sample) = subscriber.receive()? else {
                    break;
                };
                has_message = true;
                drained += 1;
                let payload = sample.payload();
                let Some(trade) = parse_trade(payload, self.venue) else {
                    continue;
                };
                let side = match trade.side {
                    TradeSide::Buy => queue_position_engine::Side::Buy,
                    TradeSide::Sell => queue_position_engine::Side::Sell,
                };
                if let Some(queue_positions) = self.queue_positions.as_ref() {
                    if let Ok(mut state) = queue_positions.lock() {
                        let local_tp = crate::common::time_util::get_timestamp_us();
                        events.extend(state.apply_public_trade_all(
                            &trade.symbol,
                            side,
                            price_to_key(trade.price),
                            trade.amount,
                            trade.timestamp_us,
                            local_tp,
                        ));
                    }
                }
            }
        }
        self.publish_order_queue_events(events);
        Ok(has_message)
    }

    fn drain_account_updates(&mut self, max_messages: usize) -> Result<bool> {
        let mut events = Vec::new();
        let mut has_message = false;
        let mut drained = 0usize;
        for subscription in &self.account_subscriptions {
            while drained < max_messages {
                let Some(sample) = subscription.subscriber.receive()? else {
                    break;
                };
                has_message = true;
                drained += 1;
                if let Some(queue_positions) = self.queue_positions.as_ref() {
                    if let Ok(mut state) = queue_positions.lock() {
                        let local_tp = crate::common::time_util::get_timestamp_us();
                        let now_ms = local_tp / 1_000;
                        events.extend(state.process_account_payload(
                            &subscription.account_id,
                            sample.payload(),
                            now_ms,
                            local_tp,
                        ));
                    }
                }
            }
        }
        self.publish_order_queue_events(events);
        Ok(has_message)
    }

    fn publish_order_queue_events(&mut self, events: Vec<QueuePositionPublishEvent>) {
        for event in events {
            if let Some(subscription) = self
                .account_subscriptions
                .iter_mut()
                .find(|subscription| subscription.account_id == event.account_id)
            {
                subscription.publish_order_queue_position(&event.msg);
            }
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
        let queue_stats = self.queue_positions.as_ref().and_then(|queue_positions| {
            queue_positions
                .lock()
                .ok()
                .map(|mut accounts| accounts.collect_and_reset_stats())
        });
        info!(
            "DepthMsgApp[{}] stats: symbols={}, updates={}, pushes={}",
            self.venue_slug,
            self.symbols.len(),
            self.update_count,
            self.push_count
        );
        if let Some(queue_stats) = queue_stats {
            for (account_id, tracked_orders, stats) in queue_stats {
                info!(
                    "QueuePosition[{}] stats: account_id={} tracked_orders={} public_trades={} level_updates={} order_updates={} trade_updates={} add_orders={} fill_updates={} remove_orders={} expired_orders={} account_decode_fail={} account_filtered={}",
                    self.venue_slug,
                    account_id,
                    tracked_orders,
                    stats.public_trade_count,
                    stats.level_update_count,
                    stats.order_update_count,
                    stats.trade_update_count,
                    stats.add_order_count,
                    stats.fill_update_count,
                    stats.remove_order_count,
                    stats.expired_order_count,
                    stats.account_decode_fail_count,
                    stats.account_filtered_count
                );
            }
        }
        for subscription in &mut self.account_subscriptions {
            info!(
                "OrderQueuePublisher[{}] stats: account_id={} service={} published={} dropped={}",
                self.venue_slug,
                subscription.account_id,
                subscription.order_queue_service_name,
                subscription.order_queue_publish_count,
                subscription.order_queue_drop_count
            );
            subscription.order_queue_publish_count = 0;
            subscription.order_queue_drop_count = 0;
        }
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

fn derive_order_pos_service_name(cfg: &DepthAccountSubscriptionConfig) -> Result<String> {
    let Some((namespace, _)) = cfg.service_name.split_once("/account_pubs/") else {
        return Err(anyhow!(
            "account monitor service_name must include namespace/account_pubs: account_id={} service_name={}",
            cfg.account_id,
            cfg.service_name
        ));
    };
    if namespace.trim().is_empty() {
        return Err(anyhow!(
            "account monitor service_name namespace is empty: account_id={} service_name={}",
            cfg.account_id,
            cfg.service_name
        ));
    }
    Ok(format!(
        "{}/order_pos_pub/{}",
        namespace,
        cfg.venue.data_pub_slug()
    ))
}

fn timestamp_to_us(timestamp: i64) -> i64 {
    if timestamp <= 0 {
        return crate::common::time_util::get_timestamp_us();
    }
    if timestamp >= 1_000_000_000_000_000 {
        timestamp
    } else if timestamp >= 1_000_000_000_000 {
        timestamp.saturating_mul(1_000)
    } else {
        timestamp
    }
}
