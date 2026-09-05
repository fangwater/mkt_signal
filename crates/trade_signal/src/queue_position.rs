use std::cell::RefCell;
use std::collections::VecDeque;
use std::hash::{BuildHasher, Hash, Hasher};
use std::time::{Duration, Instant};

use ahash::RandomState;
use anyhow::{Context, Result};
use iceoryx2::port::publisher::Publisher;
use iceoryx2::port::subscriber::Subscriber;
use iceoryx2::prelude::*;
use iceoryx2::service::ipc;
use log::{info, warn};
use mkt_parsers::msg::basic_account_msg::{
    split_basic_account_event, BasicAccountEventType, BasicAccountScope, BinanceBasicOrderMsg,
    GateBasicOrderMsg, OkexOrderMsg, BASIC_ACCOUNT_EVENT_HEADER_LEN,
};
use mkt_parsers::msg::bitget_account_msg::BitgetBasicOrderMsg;
use mkt_parsers::msg::bybit_account_msg::BybitBasicOrderMsg;
use mkt_parsers::msg::mkt_msg::MktMsgType;
use order_common::{
    ExecutionType, OrderStatus, OrderType, OrderUpdate, Side as OrderSide, TradingVenue,
};
use persist_common::{
    OrderQueuePositionAction, OrderQueuePositionMsg, OrderQueuePositionRecord,
    ORDER_QUEUE_POSITION_MAX_BYTES, ORDER_QUEUE_POSITION_RECORD_CHANNEL,
    ORDER_QUEUE_POSITION_RECORD_MAX_PUBLISHERS,
};
use queue_position_engine::{
    AddOrder, BookSide, FillUpdate, FrontQtyMode, LevelUpdate, OrderSnapshot, PublicTrade,
    QueuePositionEngine, Side, TrackedOrder,
};
use runtime_common::exchange::Exchange;
use runtime_common::fast_hash::{fast_hash_map, fast_hash_set, FastHashMap, FastHashSet};
use runtime_common::ipc_service_name::build_service_name;
use runtime_common::symbol_util::normalize_symbol_for_internal;
use runtime_common::time_util::get_timestamp_us;

use account_common::pm_ipc::{
    PM_HISTORY_SIZE, PM_MAX_BYTES, PM_MAX_SUBSCRIBERS, PM_SUBSCRIBER_MAX_BUFFER_SIZE,
};
use ipc_common::iceoryx_publisher::SIGNAL_PAYLOAD;

const TRADE_MAX_BYTES: usize = 128;
const TRADE_MAX_PUBLISHERS: usize = 1;
const TRADE_MAX_SUBSCRIBERS: usize = 64;
const TRADE_HISTORY_SIZE: usize = 100;
const TRADE_SUBSCRIBER_MAX_BUFFER: usize = 8192;
const ORDER_POSITION_MAX_PUBLISHERS: usize = 1;
const ORDER_POSITION_MAX_SUBSCRIBERS: usize = 10;
const ORDER_POSITION_HISTORY_SIZE: usize = 1024;
const TRADE_DRAIN_BUDGET: usize = 1024;
const ACCOUNT_DRAIN_BUDGET: usize = 256;
const DEDUP_CAPACITY: usize = 8192;
const ORDER_TTL: Duration = Duration::from_secs(30 * 60);
const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalLevelContext {
    pub price_key: i64,
    pub visible_qty: f64,
    pub amount_scale: f64,
}

#[derive(Debug, Clone)]
struct QueuePositionSnapshot {
    create_tp: i64,
    order_id: i64,
    tlen: f64,
    backlen: f64,
    inpos: f64,
}

impl QueuePositionSnapshot {
    fn from_order_snapshot(create_tp: i64, value: OrderSnapshot) -> Self {
        Self {
            create_tp,
            order_id: value.order_id,
            tlen: value.tlen,
            backlen: value.backlen,
            inpos: value.inpos,
        }
    }
}

#[derive(Default)]
struct DedupCache {
    set: FastHashSet<u64>,
    order: VecDeque<u64>,
}

impl DedupCache {
    fn new() -> Self {
        Self {
            set: fast_hash_set(),
            order: VecDeque::with_capacity(DEDUP_CAPACITY),
        }
    }

    fn insert_check(&mut self, key: u64) -> bool {
        if !self.set.insert(key) {
            return false;
        }
        self.order.push_back(key);
        if self.order.len() > DEDUP_CAPACITY {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
        true
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct QueuePositionStats {
    public_trade_count: u64,
    level_update_count: u64,
    order_update_count: u64,
    trade_update_count: u64,
    add_order_count: u64,
    fill_update_count: u64,
    remove_order_count: u64,
    expired_order_count: u64,
    account_decode_fail_count: u64,
    account_filtered_count: u64,
    raw_publish_count: u64,
    raw_drop_count: u64,
    persist_publish_count: u64,
    persist_drop_count: u64,
}

struct QueuePositionState {
    account_id: String,
    venue: TradingVenue,
    exchange: Exchange,
    engine: QueuePositionEngine,
    account_dedup: DedupCache,
    account_hash_state: RandomState,
    tracked_levels: FastHashMap<String, FastHashMap<(BookSide, i64), usize>>,
    order_first_seen_ms: FastHashMap<i64, i64>,
    order_create_tp: FastHashMap<i64, i64>,
    order_amount_scale: FastHashMap<i64, f64>,
    stats: QueuePositionStats,
}

impl QueuePositionState {
    fn new(account_id: String, venue: TradingVenue, exchange: Exchange) -> Self {
        Self {
            account_id,
            venue,
            exchange,
            engine: QueuePositionEngine::new(),
            account_dedup: DedupCache::new(),
            account_hash_state: RandomState::new(),
            tracked_levels: fast_hash_map(),
            order_first_seen_ms: fast_hash_map(),
            order_create_tp: fast_hash_map(),
            order_amount_scale: fast_hash_map(),
            stats: QueuePositionStats::default(),
        }
    }

    fn order_snapshot(&self, order_id: i64) -> Option<QueuePositionSnapshot> {
        let create_tp = self.order_create_tp.get(&order_id).copied().unwrap_or(0);
        self.engine
            .order_snapshot(order_id)
            .map(|snapshot| QueuePositionSnapshot::from_order_snapshot(create_tp, snapshot))
    }

    #[inline]
    fn tracks_symbol(&self, symbol: &str) -> bool {
        self.tracked_levels.contains_key(symbol)
    }

    #[inline]
    fn tracks_level(&self, symbol: &str, side: BookSide, price_key: i64) -> bool {
        self.tracked_levels
            .get(symbol)
            .is_some_and(|levels| levels.contains_key(&(side, price_key)))
    }

    fn index_level(&mut self, symbol: &str, side: BookSide, price_key: i64) {
        let levels = self
            .tracked_levels
            .entry(symbol.to_owned())
            .or_insert_with(fast_hash_map);
        let count = levels.entry((side, price_key)).or_insert(0);
        *count = count.saturating_add(1);
    }

    fn unindex_level(&mut self, symbol: &str, side: BookSide, price_key: i64) {
        let remove_symbol = if let Some(levels) = self.tracked_levels.get_mut(symbol) {
            let key = (side, price_key);
            if let Some(count) = levels.get_mut(&key) {
                if *count > 1 {
                    *count -= 1;
                } else {
                    levels.remove(&key);
                }
            }
            levels.is_empty()
        } else {
            false
        };
        if remove_symbol {
            self.tracked_levels.remove(symbol);
        }
    }

    fn finish_removed_order(&mut self, order: TrackedOrder) {
        let order_id = order.order_id;
        self.unindex_level(
            &order.symbol,
            BookSide::from_order_side(order.side),
            order.price_key,
        );
        self.order_first_seen_ms.remove(&order_id);
        self.order_create_tp.remove(&order_id);
        self.order_amount_scale.remove(&order_id);
        self.stats.remove_order_count = self.stats.remove_order_count.saturating_add(1);
    }

    fn remove_engine_order(&mut self, order_id: i64) -> bool {
        let Some(order) = self.engine.remove_order(order_id) else {
            return false;
        };
        self.finish_removed_order(order);
        true
    }

    fn apply_level_update(
        &mut self,
        symbol: &str,
        side: BookSide,
        price_key: i64,
        old_qty: f64,
        new_qty: f64,
    ) {
        if (old_qty - new_qty).abs() <= f64::EPSILON || !self.tracks_level(symbol, side, price_key)
        {
            return;
        }
        self.engine.apply_level_update(LevelUpdate {
            symbol: symbol.to_owned(),
            side,
            price_key,
            old_qty,
            new_qty,
        });
        self.stats.level_update_count = self.stats.level_update_count.saturating_add(1);
    }

    fn apply_public_trade(&mut self, symbol: &str, side: Side, price_key: i64, qty: f64) {
        if !self.tracks_level(symbol, BookSide::consumed_by_trade_side(side), price_key) {
            return;
        }
        self.engine.apply_public_trade(PublicTrade {
            symbol: symbol.to_owned(),
            side,
            price_key,
            qty,
        });
        self.stats.public_trade_count = self.stats.public_trade_count.saturating_add(1);
    }

    fn process_account_payload<F>(
        &mut self,
        payload: &[u8],
        now_ms: i64,
        local_tp: i64,
        mut level_context: F,
    ) -> Option<OrderQueuePositionMsg>
    where
        F: FnMut(&str, Side, f64) -> Option<LocalLevelContext>,
    {
        let Some((event_type, account_scope, data)) = split_basic_account_event(payload) else {
            self.stats.account_decode_fail_count =
                self.stats.account_decode_fail_count.saturating_add(1);
            return None;
        };
        let event_len = BASIC_ACCOUNT_EVENT_HEADER_LEN + data.len();
        let mut hasher = self.account_hash_state.build_hasher();
        payload[..event_len].hash(&mut hasher);
        if !self.account_dedup.insert_check(hasher.finish()) {
            return None;
        }
        if event_type != BasicAccountEventType::OrderUpdate {
            return None;
        }
        if !scope_can_match_venue(account_scope, self.venue) {
            self.stats.account_filtered_count = self.stats.account_filtered_count.saturating_add(1);
            return None;
        }

        match self.exchange {
            Exchange::Binance => self.process_decoded_order_update(
                BinanceBasicOrderMsg::from_bytes(data),
                now_ms,
                local_tp,
                &mut level_context,
            ),
            Exchange::Okex => self.process_decoded_order_update(
                OkexOrderMsg::from_bytes(data),
                now_ms,
                local_tp,
                &mut level_context,
            ),
            Exchange::Gate => self.process_decoded_order_update(
                GateBasicOrderMsg::from_bytes(data),
                now_ms,
                local_tp,
                &mut level_context,
            ),
            Exchange::Bitget => self.process_decoded_order_update(
                BitgetBasicOrderMsg::from_bytes(data),
                now_ms,
                local_tp,
                &mut level_context,
            ),
            Exchange::Bybit => self.process_decoded_order_update(
                BybitBasicOrderMsg::from_bytes(data),
                now_ms,
                local_tp,
                &mut level_context,
            ),
            _ => {
                self.stats.account_decode_fail_count =
                    self.stats.account_decode_fail_count.saturating_add(1);
                None
            }
        }
    }

    fn process_decoded_order_update<T, E, F>(
        &mut self,
        decoded: std::result::Result<T, E>,
        now_ms: i64,
        local_tp: i64,
        level_context: &mut F,
    ) -> Option<OrderQueuePositionMsg>
    where
        T: OrderUpdate,
        F: FnMut(&str, Side, f64) -> Option<LocalLevelContext>,
    {
        let update = match decoded {
            Ok(update) => update,
            Err(_) => {
                self.stats.account_decode_fail_count =
                    self.stats.account_decode_fail_count.saturating_add(1);
                return None;
            }
        };
        self.process_order_update(&update, now_ms, local_tp, level_context)
    }

    fn process_order_update<T, F>(
        &mut self,
        update: &T,
        now_ms: i64,
        local_tp: i64,
        level_context: &mut F,
    ) -> Option<OrderQueuePositionMsg>
    where
        T: OrderUpdate,
        F: FnMut(&str, Side, f64) -> Option<LocalLevelContext>,
    {
        if update.trading_venue() != self.venue {
            self.stats.account_filtered_count = self.stats.account_filtered_count.saturating_add(1);
            return None;
        }
        if matches!(
            update.status(),
            OrderStatus::PartiallyFilled | OrderStatus::Filled
        ) || update.execution_type() == ExecutionType::Trade
        {
            self.apply_trade_update(update, now_ms, local_tp, level_context)
        } else {
            self.apply_order_update(update, now_ms, local_tp, level_context)
        }
    }

    fn apply_order_update<F>(
        &mut self,
        update: &dyn OrderUpdate,
        now_ms: i64,
        local_tp: i64,
        level_context: &mut F,
    ) -> Option<OrderQueuePositionMsg>
    where
        F: FnMut(&str, Side, f64) -> Option<LocalLevelContext>,
    {
        let update_tp = valid_timestamp_us(update.event_time(), local_tp);
        self.stats.order_update_count = self.stats.order_update_count.saturating_add(1);
        let order_id = update.client_order_id();
        if order_id <= 0 {
            return None;
        }

        if is_terminal_order_update(update) {
            let snapshot = self.order_snapshot(order_id);
            self.remove_engine_order(order_id);
            let action =
                lifecycle_terminal_action(update).unwrap_or(OrderQueuePositionAction::Canceled);
            return snapshot.map(|snapshot| {
                queue_position_msg_from_snapshot(action, update_tp, local_tp, snapshot)
            });
        }

        let action = if update.execution_type() == ExecutionType::Replaced {
            self.remove_engine_order(order_id);
            OrderQueuePositionAction::Replaced
        } else {
            if self.engine.contains_order(order_id) {
                return None;
            }
            OrderQueuePositionAction::New
        };

        if !self.add_trackable_order(update, now_ms, update_tp, level_context) {
            return None;
        }
        self.order_snapshot(order_id)
            .map(|snapshot| queue_position_msg_from_snapshot(action, update_tp, local_tp, snapshot))
    }

    fn apply_trade_update<F>(
        &mut self,
        update: &dyn OrderUpdate,
        now_ms: i64,
        local_tp: i64,
        level_context: &mut F,
    ) -> Option<OrderQueuePositionMsg>
    where
        F: FnMut(&str, Side, f64) -> Option<LocalLevelContext>,
    {
        let update_tp = valid_timestamp_us(update.event_time(), local_tp);
        self.stats.trade_update_count = self.stats.trade_update_count.saturating_add(1);
        let order_id = update.client_order_id();
        if order_id <= 0 {
            return None;
        }

        let terminal_action = lifecycle_terminal_action(update);
        let terminal = terminal_action.is_some();
        if !self.engine.contains_order(order_id)
            && !terminal
            && !self.add_trackable_order(update, now_ms, update_tp, level_context)
        {
            return None;
        }

        let before_fill = self.order_snapshot(order_id)?;
        let amount_scale = self
            .order_amount_scale
            .get(&order_id)
            .copied()
            .unwrap_or(1.0);
        let cumulative = update.cumulative_filled_quantity() * amount_scale;
        if !cumulative.is_finite() || cumulative < 0.0 {
            return None;
        }

        let removed = self.engine.apply_fill_update(FillUpdate {
            order_id,
            cumulative_filled_qty: cumulative,
        });
        let removed_by_fill = removed.is_some();
        self.stats.fill_update_count = self.stats.fill_update_count.saturating_add(1);
        if let Some(order) = removed {
            self.finish_removed_order(order);
        }
        if terminal {
            self.remove_engine_order(order_id);
        }

        let action = if terminal || removed_by_fill {
            terminal_action.unwrap_or(OrderQueuePositionAction::Filled)
        } else {
            OrderQueuePositionAction::PartiallyFilled
        };
        let snapshot = self.order_snapshot(order_id).unwrap_or(before_fill);
        Some(queue_position_msg_from_snapshot(
            action, update_tp, local_tp, snapshot,
        ))
    }

    fn add_trackable_order<F>(
        &mut self,
        update: &dyn OrderUpdate,
        now_ms: i64,
        create_tp: i64,
        level_context: &mut F,
    ) -> bool
    where
        F: FnMut(&str, Side, f64) -> Option<LocalLevelContext>,
    {
        if self.engine.contains_order(update.client_order_id())
            || !matches!(update.order_type(), OrderType::Limit)
            || is_terminal_order_update(update)
        {
            return false;
        }

        let symbol = normalize_symbol_for_internal(update.symbol());
        let side = side_from_order_side(update.side());
        if symbol.is_empty() || update.price() <= 0.0 || update.quantity() <= 0.0 {
            return false;
        }
        let Some(context) = level_context(&symbol, side, update.price()) else {
            return false;
        };
        let qty = update.quantity() * context.amount_scale;
        let order_id = update.client_order_id();
        if !self.engine.add_order(AddOrder {
            order_id,
            symbol: symbol.clone(),
            side,
            price_key: context.price_key,
            qty,
            visible_level_qty: context.visible_qty,
            front_qty_mode: FrontQtyMode::LevelExcludesOwnOrder,
        }) {
            return false;
        }

        self.index_level(&symbol, BookSide::from_order_side(side), context.price_key);
        self.order_first_seen_ms.insert(order_id, now_ms);
        self.order_create_tp.insert(order_id, create_tp);
        self.order_amount_scale
            .insert(order_id, context.amount_scale);
        self.stats.add_order_count = self.stats.add_order_count.saturating_add(1);
        true
    }

    fn clear_expired(&mut self, now_ms: i64) -> usize {
        let ttl_ms = ORDER_TTL.as_millis() as i64;
        let expired: Vec<i64> = self
            .order_first_seen_ms
            .iter()
            .filter_map(|(order_id, first_seen_ms)| {
                (now_ms.saturating_sub(*first_seen_ms) >= ttl_ms).then_some(*order_id)
            })
            .collect();
        let count = expired
            .into_iter()
            .filter(|order_id| self.remove_engine_order(*order_id))
            .count();
        self.stats.expired_order_count =
            self.stats.expired_order_count.saturating_add(count as u64);
        count
    }
}

struct QueuePublishers {
    raw: Publisher<ipc::Service, [u8; ORDER_QUEUE_POSITION_MAX_BYTES], ()>,
    persist: Publisher<ipc::Service, [u8; SIGNAL_PAYLOAD], ()>,
    raw_service: String,
    persist_service: String,
}

impl QueuePublishers {
    fn new(venue: TradingVenue) -> Result<Self> {
        let venue_slug = venue.data_pub_slug();
        let node_name = format!(
            "trade_signal_queue_position_{}",
            venue_slug.replace('-', "_")
        );
        let node = NodeBuilder::new()
            .name(&NodeName::new(&node_name)?)
            .create::<ipc::Service>()
            .context("create trade_signal queue-position iceoryx node")?;

        let raw_service = build_service_name(&format!("order_pos_pub/{venue_slug}"));
        let raw = node
            .service_builder(&ServiceName::new(&raw_service)?)
            .publish_subscribe::<[u8; ORDER_QUEUE_POSITION_MAX_BYTES]>()
            .max_publishers(ORDER_POSITION_MAX_PUBLISHERS)
            .max_subscribers(ORDER_POSITION_MAX_SUBSCRIBERS)
            .history_size(ORDER_POSITION_HISTORY_SIZE)
            .open_or_create()
            .with_context(|| format!("open queue-position service {raw_service}"))?
            .publisher_builder()
            .create()
            .with_context(|| format!("create queue-position publisher {raw_service}"))?;

        let persist_service = build_service_name(&format!(
            "persist_pubs/{ORDER_QUEUE_POSITION_RECORD_CHANNEL}"
        ));
        let persist = node
            .service_builder(&ServiceName::new(&persist_service)?)
            .publish_subscribe::<[u8; SIGNAL_PAYLOAD]>()
            .max_publishers(ORDER_QUEUE_POSITION_RECORD_MAX_PUBLISHERS)
            .max_subscribers(32)
            .history_size(128)
            .subscriber_max_buffer_size(256)
            .open_or_create()
            .with_context(|| format!("open queue-position persistence service {persist_service}"))?
            .publisher_builder()
            .create()
            .with_context(|| {
                format!("create queue-position persistence publisher {persist_service}")
            })?;

        Ok(Self {
            raw,
            persist,
            raw_service,
            persist_service,
        })
    }

    fn publish(
        &self,
        account_id: &str,
        venue: TradingVenue,
        msg: &OrderQueuePositionMsg,
        stats: &mut QueuePositionStats,
    ) {
        let raw = msg.to_bytes();
        match self.raw.send_copy(raw) {
            Ok(_) => stats.raw_publish_count = stats.raw_publish_count.saturating_add(1),
            Err(err) => {
                stats.raw_drop_count = stats.raw_drop_count.saturating_add(1);
                warn!(
                    "queue-position publish failed: service={} order_id={} err={err}",
                    self.raw_service, msg.client_order_id
                );
            }
        }

        let record = OrderQueuePositionRecord {
            recv_ts_us: get_timestamp_us(),
            account_id: account_id.to_owned(),
            venue: venue.to_u8(),
            action: msg.action,
            create_tp: msg.create_tp,
            update_tp: msg.update_tp,
            local_tp: msg.local_tp,
            client_order_id: msg.client_order_id,
            tlen: msg.tlen,
            backlen: msg.backlen,
            inpos: msg.inpos,
        };
        let payload = match record.to_bytes() {
            Ok(payload) if payload.len() <= SIGNAL_PAYLOAD => payload,
            Ok(payload) => {
                stats.persist_drop_count = stats.persist_drop_count.saturating_add(1);
                warn!(
                    "queue-position persistence payload too large: order_id={} bytes={} max={}",
                    msg.client_order_id,
                    payload.len(),
                    SIGNAL_PAYLOAD
                );
                return;
            }
            Err(err) => {
                stats.persist_drop_count = stats.persist_drop_count.saturating_add(1);
                warn!(
                    "queue-position persistence encode failed: order_id={} err={err}",
                    msg.client_order_id
                );
                return;
            }
        };
        let mut buffer = [0u8; SIGNAL_PAYLOAD];
        buffer[..payload.len()].copy_from_slice(&payload);
        match self.persist.send_copy(buffer) {
            Ok(_) => stats.persist_publish_count = stats.persist_publish_count.saturating_add(1),
            Err(err) => {
                stats.persist_drop_count = stats.persist_drop_count.saturating_add(1);
                warn!(
                    "queue-position persistence publish failed: service={} order_id={} err={err}",
                    self.persist_service, msg.client_order_id
                );
            }
        }
    }
}

struct QueuePositionRuntime {
    state: QueuePositionState,
    publishers: QueuePublishers,
    last_housekeeping: Instant,
}

enum QueuePositionRuntimeState {
    Disabled,
    Enabled(QueuePositionRuntime),
}

thread_local! {
    static QUEUE_POSITION: RefCell<QueuePositionRuntimeState> = const {
        RefCell::new(QueuePositionRuntimeState::Disabled)
    };
}

pub(crate) fn init_local(venue: TradingVenue) -> Result<()> {
    let Some(exchange) = exchange_for_venue(venue) else {
        warn!(
            "queue-position disabled for unsupported local_tlen venue={}",
            venue.data_pub_slug()
        );
        return Ok(());
    };
    let account_id = std::env::var("IPC_NAMESPACE")
        .context("IPC_NAMESPACE is required for local queue-position tracking")?;
    let publishers = QueuePublishers::new(venue)?;
    QUEUE_POSITION.with(|runtime| {
        *runtime.borrow_mut() = QueuePositionRuntimeState::Enabled(QueuePositionRuntime {
            state: QueuePositionState::new(account_id.clone(), venue, exchange),
            publishers,
            last_housekeeping: Instant::now(),
        });
    });

    spawn_public_trade_listener(venue);
    spawn_account_listener(exchange);
    info!(
        "queue-position enabled with local_tlen: account_id={} venue={} hash=ahash",
        account_id,
        venue.data_pub_slug()
    );
    Ok(())
}

pub(crate) fn disable() {
    QUEUE_POSITION.with(|runtime| {
        *runtime.borrow_mut() = QueuePositionRuntimeState::Disabled;
    });
}

pub(crate) fn apply_level_update(
    symbol: &str,
    side: BookSide,
    price_key: i64,
    old_qty: f64,
    new_qty: f64,
) {
    QUEUE_POSITION.with(|runtime| {
        if let QueuePositionRuntimeState::Enabled(runtime) = &mut *runtime.borrow_mut() {
            runtime
                .state
                .apply_level_update(symbol, side, price_key, old_qty, new_qty);
        }
    });
}

fn apply_public_trade(symbol: &str, side: Side, price_key: i64, qty: f64) {
    QUEUE_POSITION.with(|runtime| {
        if let QueuePositionRuntimeState::Enabled(runtime) = &mut *runtime.borrow_mut() {
            runtime
                .state
                .apply_public_trade(symbol, side, price_key, qty);
        }
    });
}

fn tracks_symbol(symbol: &str) -> bool {
    QUEUE_POSITION.with(|runtime| {
        let runtime = runtime.borrow();
        match &*runtime {
            QueuePositionRuntimeState::Enabled(runtime) => runtime.state.tracks_symbol(symbol),
            QueuePositionRuntimeState::Disabled => false,
        }
    })
}

fn process_account_payload(payload: &[u8]) {
    let local_tp = get_timestamp_us();
    QUEUE_POSITION.with(|runtime| {
        let mut runtime = runtime.borrow_mut();
        let QueuePositionRuntimeState::Enabled(runtime) = &mut *runtime else {
            return;
        };
        let venue = runtime.state.venue;
        let event = runtime.state.process_account_payload(
            payload,
            local_tp / 1_000,
            local_tp,
            |symbol, side, price| {
                super::local_tlen::queue_level_context(venue, symbol, side, price)
            },
        );
        if let Some(event) = event {
            let account_id = runtime.state.account_id.clone();
            runtime
                .publishers
                .publish(&account_id, venue, &event, &mut runtime.state.stats);
        }
    });
}

pub(crate) fn housekeeping() {
    QUEUE_POSITION.with(|runtime| {
        let mut runtime = runtime.borrow_mut();
        let QueuePositionRuntimeState::Enabled(runtime) = &mut *runtime else {
            return;
        };
        if runtime.last_housekeeping.elapsed() < HOUSEKEEPING_INTERVAL {
            return;
        }
        runtime.last_housekeeping = Instant::now();
        let expired = runtime.state.clear_expired(get_timestamp_us() / 1_000);
        let stats = std::mem::take(&mut runtime.state.stats);
        info!(
            "queue-position[{}] stats account_id={} tracked_orders={} public_trades={} level_updates={} order_updates={} trade_updates={} add_orders={} fill_updates={} remove_orders={} expired_orders={} decode_fail={} filtered={} raw_published={} raw_dropped={} persist_published={} persist_dropped={}",
            runtime.state.venue.data_pub_slug(),
            runtime.state.account_id,
            runtime.state.engine.len(),
            stats.public_trade_count,
            stats.level_update_count,
            stats.order_update_count,
            stats.trade_update_count,
            stats.add_order_count,
            stats.fill_update_count,
            stats.remove_order_count,
            stats.expired_order_count,
            stats.account_decode_fail_count,
            stats.account_filtered_count,
            stats.raw_publish_count,
            stats.raw_drop_count,
            stats.persist_publish_count,
            stats.persist_drop_count,
        );
        if expired > 0 {
            info!(
                "queue-position[{}] expired stale orders: count={} ttl_secs={}",
                runtime.state.venue.data_pub_slug(),
                expired,
                ORDER_TTL.as_secs()
            );
        }
    });
}

fn spawn_public_trade_listener(venue: TradingVenue) {
    let fast_poll = crate::runtime_flags::enable_ipc_fast_poll();
    tokio::task::spawn_local(async move {
        let result: Result<()> = async move {
            let venue_slug = venue.data_pub_slug();
            let service_name = format!("dat_pbs/{venue_slug}/trade");
            let node_name = format!(
                "trade_signal_queue_position_{}_trade",
                venue_slug.replace('-', "_")
            );
            let node = NodeBuilder::new()
                .name(&NodeName::new(&node_name)?)
                .create::<ipc::Service>()?;
            let service = node
                .service_builder(&ServiceName::new(&service_name)?)
                .publish_subscribe::<[u8; TRADE_MAX_BYTES]>()
                .max_publishers(TRADE_MAX_PUBLISHERS)
                .max_subscribers(TRADE_MAX_SUBSCRIBERS)
                .history_size(TRADE_HISTORY_SIZE)
                .subscriber_max_buffer_size(TRADE_SUBSCRIBER_MAX_BUFFER)
                .open_or_create()
                .with_context(|| format!("open or create public trade service {service_name}"))?;
            let subscriber: Subscriber<ipc::Service, [u8; TRADE_MAX_BYTES], ()> =
                service.subscriber_builder().create()?;
            info!("queue-position subscribed public trades: {service_name}");

            let mut drained = 0usize;
            loop {
                match subscriber.receive() {
                    Ok(Some(sample)) => {
                        drained += 1;
                        process_public_trade_payload(venue, sample.payload());
                        if drained >= TRADE_DRAIN_BUDGET {
                            drained = 0;
                            housekeeping();
                            tokio::task::yield_now().await;
                        }
                    }
                    Ok(None) => {
                        drained = 0;
                        housekeeping();
                        crate::runtime_flags::idle_poll_wait(fast_poll).await;
                    }
                    Err(err) => {
                        drained = 0;
                        housekeeping();
                        warn!("queue-position public trade receive failed: {err}");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        }
        .await;
        if let Err(err) = result {
            warn!("queue-position public trade listener exited: {err:#}");
        }
    });
}

fn spawn_account_listener(exchange: Exchange) {
    let fast_poll = crate::runtime_flags::enable_ipc_fast_poll();
    tokio::task::spawn_local(async move {
        let result: Result<()> = async move {
            let service_name = build_service_name(&format!(
                "account_pubs/{}_pm",
                exchange.as_str()
            ));
            let node_name = format!("trade_signal_queue_position_{}_account", exchange.as_str());
            let node = NodeBuilder::new()
                .name(&NodeName::new(&node_name)?)
                .create::<ipc::Service>()?;
            let service_name_obj = ServiceName::new(&service_name)?;
            let service_builder = || {
                let builder = node
                    .service_builder(&service_name_obj)
                    .publish_subscribe::<[u8; PM_MAX_BYTES]>()
                    .max_publishers(1)
                    .max_subscribers(PM_MAX_SUBSCRIBERS)
                    .history_size(PM_HISTORY_SIZE)
                    .subscriber_max_buffer_size(PM_SUBSCRIBER_MAX_BUFFER_SIZE);
                if exchange == Exchange::Hyperliquid {
                    builder.enable_safe_overflow(false)
                } else {
                    builder
                }
            };
            let service = match service_builder().open() {
                Ok(service) => service,
                Err(err) => {
                    warn!(
                        "queue-position account service not ready; open_or_create fallback: service={} err={err:?}",
                        service_name
                    );
                    service_builder().open_or_create()?
                }
            };
            let subscriber: Subscriber<ipc::Service, [u8; PM_MAX_BYTES], ()> =
                service.subscriber_builder().create()?;
            info!("queue-position subscribed account events: {service_name}");

            let mut drained = 0usize;
            loop {
                match subscriber.receive() {
                    Ok(Some(sample)) => {
                        drained += 1;
                        process_account_payload(sample.payload());
                        if drained >= ACCOUNT_DRAIN_BUDGET {
                            drained = 0;
                            housekeeping();
                            tokio::task::yield_now().await;
                        }
                    }
                    Ok(None) => {
                        drained = 0;
                        housekeeping();
                        crate::runtime_flags::idle_poll_wait(fast_poll).await;
                    }
                    Err(err) => {
                        drained = 0;
                        housekeeping();
                        warn!("queue-position account receive failed: {err}");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        }
        .await;
        if let Err(err) = result {
            warn!("queue-position account listener exited: {err:#}");
        }
    });
}

fn process_public_trade_payload(venue: TradingVenue, payload: &[u8]) {
    let Some(trade) = parse_public_trade(payload) else {
        return;
    };
    let symbol = super::local_tlen::normalize_symbol_key_cow(trade.symbol);
    if !tracks_symbol(symbol.as_ref()) {
        return;
    }
    let book_side = BookSide::consumed_by_trade_side(trade.side);
    let Some(context) = super::local_tlen::queue_level_context_by_book_side(
        venue,
        symbol.as_ref(),
        book_side,
        trade.price,
    ) else {
        return;
    };
    apply_public_trade(
        symbol.as_ref(),
        trade.side,
        context.price_key,
        trade.amount * context.amount_scale,
    );
}

struct PublicTradeTick<'a> {
    symbol: &'a str,
    side: Side,
    price: f64,
    amount: f64,
}

fn parse_public_trade(payload: &[u8]) -> Option<PublicTradeTick<'_>> {
    if payload.len() < 8
        || u32::from_le_bytes(payload[0..4].try_into().ok()?) != MktMsgType::TradeInfo as u32
    {
        return None;
    }
    let symbol_len = u32::from_le_bytes(payload[4..8].try_into().ok()?) as usize;
    let symbol_end = 8usize.checked_add(symbol_len)?;
    let fields_end = symbol_end.checked_add(40)?;
    if payload.len() < fields_end {
        return None;
    }
    let symbol = std::str::from_utf8(&payload[8..symbol_end]).ok()?;
    let side = match payload[symbol_end + 16] as char {
        'B' | 'b' => Side::Buy,
        'S' | 's' => Side::Sell,
        _ => return None,
    };
    let price = f64::from_le_bytes(payload[symbol_end + 24..symbol_end + 32].try_into().ok()?);
    let amount = f64::from_le_bytes(payload[symbol_end + 32..symbol_end + 40].try_into().ok()?);
    if !price.is_finite() || price <= 0.0 || !amount.is_finite() || amount <= 0.0 {
        return None;
    }
    Some(PublicTradeTick {
        symbol,
        side,
        price,
        amount,
    })
}

fn queue_position_msg_from_snapshot(
    action: OrderQueuePositionAction,
    update_tp: i64,
    local_tp: i64,
    snapshot: QueuePositionSnapshot,
) -> OrderQueuePositionMsg {
    OrderQueuePositionMsg {
        action,
        create_tp: snapshot.create_tp,
        update_tp,
        local_tp,
        client_order_id: snapshot.order_id,
        tlen: snapshot.tlen,
        backlen: snapshot.backlen,
        inpos: snapshot.inpos,
    }
}

#[inline]
fn side_from_order_side(side: OrderSide) -> Side {
    match side {
        OrderSide::Buy => Side::Buy,
        OrderSide::Sell => Side::Sell,
    }
}

#[inline]
fn valid_timestamp_us(value: i64, fallback: i64) -> i64 {
    if value > 0 {
        value
    } else {
        fallback
    }
}

fn is_terminal_order_update(update: &dyn OrderUpdate) -> bool {
    update.status().is_finished() || update.execution_type().is_terminal()
}

fn lifecycle_terminal_action(update: &dyn OrderUpdate) -> Option<OrderQueuePositionAction> {
    match update.status() {
        OrderStatus::Filled => Some(OrderQueuePositionAction::Filled),
        OrderStatus::Canceled => Some(OrderQueuePositionAction::Canceled),
        OrderStatus::Expired | OrderStatus::ExpiredInMatch => {
            Some(OrderQueuePositionAction::Expired)
        }
        _ => match update.execution_type() {
            ExecutionType::Rejected => Some(OrderQueuePositionAction::Rejected),
            ExecutionType::Canceled | ExecutionType::TradePrevention => {
                Some(OrderQueuePositionAction::Canceled)
            }
            ExecutionType::Expired => Some(OrderQueuePositionAction::Expired),
            _ => None,
        },
    }
}

fn exchange_for_venue(venue: TradingVenue) -> Option<Exchange> {
    match venue {
        TradingVenue::BinanceMargin
        | TradingVenue::BinanceFutures
        | TradingVenue::BinanceCoinFutures => Some(Exchange::Binance),
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => Some(Exchange::Okex),
        TradingVenue::BybitMargin | TradingVenue::BybitFutures => Some(Exchange::Bybit),
        TradingVenue::BitgetMargin
        | TradingVenue::BitgetFutures
        | TradingVenue::BitgetCoinFutures => Some(Exchange::Bitget),
        TradingVenue::GateMargin | TradingVenue::GateFutures => Some(Exchange::Gate),
        _ => None,
    }
}

fn scope_can_match_venue(scope: BasicAccountScope, venue: TradingVenue) -> bool {
    match venue {
        TradingVenue::BinanceMargin => matches!(
            scope,
            BasicAccountScope::BinanceUnified | BasicAccountScope::BinanceStdSpot
        ),
        TradingVenue::BinanceFutures => matches!(
            scope,
            BasicAccountScope::BinanceUnified | BasicAccountScope::BinanceStdUm
        ),
        TradingVenue::BinanceCoinFutures => matches!(
            scope,
            BasicAccountScope::BinanceUnifiedCm | BasicAccountScope::BinanceStdCm
        ),
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
            scope == BasicAccountScope::OkexUnified
        }
        TradingVenue::GateMargin | TradingVenue::GateFutures => {
            scope == BasicAccountScope::GateUnified
        }
        TradingVenue::BitgetMargin | TradingVenue::BitgetFutures => {
            scope == BasicAccountScope::BitgetUnified
        }
        TradingVenue::BitgetCoinFutures => scope == BasicAccountScope::BitgetUnifiedCoinFutures,
        TradingVenue::BybitMargin | TradingVenue::BybitFutures => {
            scope == BasicAccountScope::BybitUnified
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::basic_account_msg::{BasicAccountEventMsg, BinanceBasicOrderMsg};
    use order_common::{OrderStatus, Side as OrderSide, TimeInForce};

    fn level_context(_symbol: &str, _side: Side, price: f64) -> Option<LocalLevelContext> {
        Some(LocalLevelContext {
            price_key: price.round() as i64,
            visible_qty: 10.0,
            amount_scale: 1.0,
        })
    }

    #[test]
    fn account_lifecycle_emits_and_removes_queue_position() {
        let mut state = QueuePositionState::new(
            "acct".to_string(),
            TradingVenue::BinanceFutures,
            Exchange::Binance,
        );
        let new_order = test_binance_order(1_000, ExecutionType::New, OrderStatus::New, 0.0);
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            BasicAccountScope::BinanceUnified,
            new_order.to_bytes(),
        );
        let msg = state
            .process_account_payload(&event.to_bytes(), 10_000, 10_000_000, level_context)
            .unwrap();
        assert_eq!(msg.action, OrderQueuePositionAction::New);
        assert_eq!(msg.tlen, 10.0);
        assert_eq!(msg.inpos, 10.0);
        assert!(state.tracks_level("BTCUSDT", BookSide::Bid, 100));

        state.apply_public_trade("BTCUSDT", Side::Sell, 100, 3.0);
        assert_eq!(state.order_snapshot(42).unwrap().inpos, 7.0);

        let filled = test_binance_order(1_001, ExecutionType::Trade, OrderStatus::Filled, 2.0);
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            BasicAccountScope::BinanceUnified,
            filled.to_bytes(),
        );
        let msg = state
            .process_account_payload(&event.to_bytes(), 10_001, 10_001_000, level_context)
            .unwrap();
        assert_eq!(msg.action, OrderQueuePositionAction::Filled);
        assert!(state.order_snapshot(42).is_none());
        assert!(!state.tracks_symbol("BTCUSDT"));
    }

    #[test]
    fn local_level_decrease_updates_tracked_inpos() {
        let mut state = QueuePositionState::new(
            "acct".to_string(),
            TradingVenue::BinanceFutures,
            Exchange::Binance,
        );
        let order = test_binance_order(1_000, ExecutionType::New, OrderStatus::New, 0.0);
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            BasicAccountScope::BinanceUnified,
            order.to_bytes(),
        );
        state.process_account_payload(&event.to_bytes(), 10_000, 10_000_000, level_context);
        state.apply_level_update("BTCUSDT", BookSide::Bid, 101, 10.0, 1.0);
        assert_eq!(state.stats.level_update_count, 0);
        assert_eq!(state.order_snapshot(42).unwrap().inpos, 10.0);

        state.apply_level_update("BTCUSDT", BookSide::Bid, 100, 10.0, 8.0);
        assert_eq!(state.stats.level_update_count, 1);
        assert_eq!(state.order_snapshot(42).unwrap().inpos, 8.0);
    }

    #[test]
    fn first_seen_partial_fill_registers_order() {
        let mut state = QueuePositionState::new(
            "acct".to_string(),
            TradingVenue::BinanceFutures,
            Exchange::Binance,
        );
        let partial = test_binance_order(
            1_001,
            ExecutionType::Trade,
            OrderStatus::PartiallyFilled,
            0.5,
        );
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            BasicAccountScope::BinanceUnified,
            partial.to_bytes(),
        );

        let msg = state
            .process_account_payload(&event.to_bytes(), 10_001, 10_001_000, level_context)
            .unwrap();

        assert_eq!(msg.action, OrderQueuePositionAction::PartiallyFilled);
        assert_eq!(msg.create_tp, 1_001_000);
        assert_eq!(state.engine.order_snapshot(42).unwrap().remaining_qty, 1.5);
        assert!(state.tracks_level("BTCUSDT", BookSide::Bid, 100));
    }

    #[test]
    fn cancel_removes_order_and_level_index() {
        let mut state = QueuePositionState::new(
            "acct".to_string(),
            TradingVenue::BinanceFutures,
            Exchange::Binance,
        );
        let new_order = test_binance_order(1_000, ExecutionType::New, OrderStatus::New, 0.0);
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            BasicAccountScope::BinanceUnified,
            new_order.to_bytes(),
        );
        state.process_account_payload(&event.to_bytes(), 10_000, 10_000_000, level_context);

        let canceled =
            test_binance_order(1_001, ExecutionType::Canceled, OrderStatus::Canceled, 0.0);
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            BasicAccountScope::BinanceUnified,
            canceled.to_bytes(),
        );
        let msg = state
            .process_account_payload(&event.to_bytes(), 10_001, 10_001_000, level_context)
            .unwrap();

        assert_eq!(msg.action, OrderQueuePositionAction::Canceled);
        assert!(state.engine.is_empty());
        assert!(!state.tracks_symbol("BTCUSDT"));
        assert_eq!(state.stats.remove_order_count, 1);
    }

    #[test]
    fn ttl_cleanup_removes_order_and_level_index() {
        let mut state = QueuePositionState::new(
            "acct".to_string(),
            TradingVenue::BinanceFutures,
            Exchange::Binance,
        );
        let new_order = test_binance_order(1_000, ExecutionType::New, OrderStatus::New, 0.0);
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            BasicAccountScope::BinanceUnified,
            new_order.to_bytes(),
        );
        state.process_account_payload(&event.to_bytes(), 10_000, 10_000_000, level_context);

        let ttl_ms = ORDER_TTL.as_millis() as i64;
        assert_eq!(state.clear_expired(10_000 + ttl_ms - 1), 0);
        assert_eq!(state.clear_expired(10_000 + ttl_ms), 1);
        assert!(state.engine.is_empty());
        assert!(!state.tracks_symbol("BTCUSDT"));
        assert_eq!(state.stats.expired_order_count, 1);
        assert_eq!(state.stats.remove_order_count, 1);
    }

    #[test]
    fn public_trade_parser_reads_trade_wire_format() {
        let msg = mkt_parsers::msg::mkt_msg::TradeMsg::create(
            "BTCUSDT".to_string(),
            11,
            12,
            'S',
            100.5,
            2.25,
        );
        let payload = msg.to_bytes();
        let trade = parse_public_trade(&payload).unwrap();
        assert_eq!(trade.symbol, "BTCUSDT");
        assert_eq!(trade.side, Side::Sell);
        assert_eq!(trade.price, 100.5);
        assert_eq!(trade.amount, 2.25);
    }

    fn test_binance_order(
        event_time: i64,
        execution_type: ExecutionType,
        order_status: OrderStatus,
        cumulative_filled_quantity: f64,
    ) -> BinanceBasicOrderMsg {
        BinanceBasicOrderMsg::create(
            BinanceBasicOrderMsg::VENUE_UM,
            event_time,
            event_time,
            "BTCUSDT".to_string(),
            11,
            42,
            1,
            OrderSide::Buy.to_u8(),
            OrderType::Limit.to_u8(),
            TimeInForce::GTC.to_u8(),
            execution_type.to_u8(),
            order_status.to_u8(),
            false,
            100.0,
            2.0,
            cumulative_filled_quantity,
            cumulative_filled_quantity,
            100.0,
            100.0,
            0.0,
            0.0,
            "USDT".to_string(),
        )
    }
}
