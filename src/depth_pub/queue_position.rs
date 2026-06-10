use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};

use log::debug;
use queue_position_engine::{
    AddOrder, BookSide, FillUpdate, FrontQtyMode, LevelKey, OrderSnapshot, PublicTrade,
    QueuePositionEngine, Side,
};

use crate::depth_pub::order_queue_msg::{OrderQueuePositionAction, OrderQueuePositionMsg};
use mkt_parsers::msg::basic_account_msg::{
    split_basic_account_event, BasicAccountEventType, BasicAccountScope, BinanceBasicOrderMsg,
    GateBasicOrderMsg, OkexOrderMsg,
};
use mkt_parsers::msg::bitget_account_msg::BitgetBasicOrderMsg;
use mkt_parsers::msg::bybit_account_msg::BybitBasicOrderMsg;
use order_common::{ExecutionType, OrderStatus, TimeInForce, TradingVenue};
use order_common::{OrderType, Side as OrderSide};
use order_common::{OrderUpdate, TradeUpdate};
use runtime_common::exchange::Exchange;
use runtime_common::symbol_util::normalize_symbol_for_internal;

const DEDUP_CAPACITY: usize = 8192;

#[derive(Debug, Clone)]
pub struct QueuePositionSnapshot {
    pub account_id: String,
    pub order_id: i64,
    pub create_tp: i64,
    pub symbol: String,
    pub side: Side,
    pub price_key: i64,
    pub initial_qty: f64,
    pub remaining_qty: f64,
    pub queue_remaining_qty: f64,
    pub public_consumed_own_qty: f64,
    pub inpos: f64,
    pub backlen: f64,
    pub tlen: f64,
}

impl QueuePositionSnapshot {
    fn from_order_snapshot(account_id: &str, create_tp: i64, value: OrderSnapshot) -> Self {
        Self {
            account_id: account_id.to_string(),
            order_id: value.order_id,
            create_tp,
            symbol: value.symbol,
            side: value.side,
            price_key: value.price_key,
            initial_qty: value.initial_qty,
            remaining_qty: value.remaining_qty,
            queue_remaining_qty: value.queue_remaining_qty,
            public_consumed_own_qty: value.public_consumed_own_qty,
            inpos: value.inpos,
            backlen: value.backlen,
            tlen: value.tlen,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueuePositionPublishEvent {
    pub account_id: String,
    pub msg: OrderQueuePositionMsg,
}

#[derive(Default)]
struct DedupCache {
    set: HashSet<u64>,
    order: VecDeque<u64>,
}

impl DedupCache {
    fn insert_check(&mut self, key: u64) -> bool {
        if self.set.contains(&key) {
            return false;
        }
        self.set.insert(key);
        self.order.push_back(key);
        if self.order.len() > DEDUP_CAPACITY {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
        true
    }
}

pub struct QueuePositionState {
    account_id: String,
    venue: TradingVenue,
    exchange: Exchange,
    amount_scale: f64,
    engine: QueuePositionEngine,
    account_dedup: DedupCache,
    order_symbols: HashMap<i64, String>,
    order_first_seen_ms: HashMap<i64, i64>,
    order_create_tp: HashMap<i64, i64>,
    stats: QueuePositionStats,
}

#[derive(Debug, Clone, Copy)]
enum QueuePositionEventSource {
    Cancel,
    Trade,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct QueuePositionStats {
    pub public_trade_count: u64,
    pub level_update_count: u64,
    pub order_update_count: u64,
    pub trade_update_count: u64,
    pub add_order_count: u64,
    pub fill_update_count: u64,
    pub remove_order_count: u64,
    pub expired_order_count: u64,
    pub account_decode_fail_count: u64,
    pub account_filtered_count: u64,
}

impl QueuePositionState {
    pub fn new(venue: TradingVenue, amount_scale: f64) -> Option<Self> {
        Self::new_for_account("", venue, amount_scale)
    }

    pub fn new_for_account(
        account_id: impl Into<String>,
        venue: TradingVenue,
        amount_scale: f64,
    ) -> Option<Self> {
        let exchange = exchange_for_venue(venue)?;
        Some(Self {
            account_id: account_id.into(),
            venue,
            exchange,
            amount_scale: valid_amount_scale(amount_scale),
            engine: QueuePositionEngine::new(),
            account_dedup: DedupCache::default(),
            order_symbols: HashMap::new(),
            order_first_seen_ms: HashMap::new(),
            order_create_tp: HashMap::new(),
            stats: QueuePositionStats::default(),
        })
    }

    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    pub fn len(&self) -> usize {
        self.engine.len()
    }

    pub fn stats(&self) -> QueuePositionStats {
        self.stats
    }

    pub fn reset_interval_stats(&mut self) {
        self.stats = QueuePositionStats::default();
    }

    pub fn order_snapshot(&self, order_id: i64) -> Option<QueuePositionSnapshot> {
        self.engine
            .order_snapshot(order_id)
            .map(|snapshot| self.snapshot_from_order_snapshot(snapshot))
    }

    pub fn order_snapshots_for_symbol(&self, symbol: &str) -> Vec<QueuePositionSnapshot> {
        let normalized = normalize_symbol_for_internal(symbol);
        self.engine
            .order_snapshots()
            .into_iter()
            .filter(|snapshot| snapshot.symbol == normalized)
            .map(|snapshot| self.snapshot_from_order_snapshot(snapshot))
            .collect()
    }

    pub fn clear_orders_older_than_ms(&mut self, now_ms: i64, ttl_ms: i64) -> usize {
        if ttl_ms <= 0 {
            return 0;
        }
        let expired: Vec<i64> = self
            .order_first_seen_ms
            .iter()
            .filter_map(|(order_id, first_seen_ms)| {
                if now_ms.saturating_sub(*first_seen_ms) >= ttl_ms {
                    Some(*order_id)
                } else {
                    None
                }
            })
            .collect();

        for order_id in &expired {
            self.engine.remove_order(*order_id);
            self.order_symbols.remove(order_id);
            self.order_first_seen_ms.remove(order_id);
            self.order_create_tp.remove(order_id);
        }

        let count = expired.len();
        if count > 0 {
            self.stats.remove_order_count =
                self.stats.remove_order_count.saturating_add(count as u64);
            self.stats.expired_order_count =
                self.stats.expired_order_count.saturating_add(count as u64);
        }
        count
    }

    pub fn apply_level_qty(
        &mut self,
        symbol: &str,
        side: BookSide,
        price_key: i64,
        qty: f64,
        update_tp: i64,
        local_tp: i64,
    ) -> Vec<QueuePositionPublishEvent> {
        let before = self.snapshot_map();
        let qty = self.scale_qty(qty);
        self.engine.apply_level_qty(symbol, side, price_key, qty);
        self.stats.level_update_count = self.stats.level_update_count.saturating_add(1);
        self.diff_events(
            before,
            QueuePositionEventSource::Cancel,
            update_tp,
            local_tp,
        )
    }

    pub fn apply_public_trade(
        &mut self,
        symbol: &str,
        side: Side,
        price_key: i64,
        qty: f64,
        update_tp: i64,
        local_tp: i64,
    ) -> Vec<QueuePositionPublishEvent> {
        let symbol = normalize_symbol_for_internal(symbol);
        if symbol.is_empty() {
            return Vec::new();
        }
        let before = self.snapshot_map();
        self.engine.apply_public_trade(PublicTrade {
            symbol,
            side,
            price_key,
            qty: self.scale_qty(qty),
        });
        self.stats.public_trade_count = self.stats.public_trade_count.saturating_add(1);
        self.diff_events(before, QueuePositionEventSource::Trade, update_tp, local_tp)
    }

    pub fn process_account_payload(
        &mut self,
        payload: &[u8],
        now_ms: i64,
        local_tp: i64,
    ) -> Vec<QueuePositionPublishEvent> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        payload.hash(&mut hasher);
        if !self.account_dedup.insert_check(hasher.finish()) {
            return Vec::new();
        }

        let Some((event_type, account_scope, data)) = split_basic_account_event(payload) else {
            self.stats.account_decode_fail_count =
                self.stats.account_decode_fail_count.saturating_add(1);
            return Vec::new();
        };

        if event_type != BasicAccountEventType::OrderUpdate {
            return Vec::new();
        }
        if !scope_can_match_venue(account_scope, self.venue) {
            self.stats.account_filtered_count = self.stats.account_filtered_count.saturating_add(1);
            return Vec::new();
        }

        let events = match self.exchange {
            Exchange::Binance => BinanceBasicOrderMsg::from_bytes(data)
                .map(|msg| self.process_order_payload(&msg, now_ms, local_tp))
                .ok(),
            Exchange::Okex => OkexOrderMsg::from_bytes(data)
                .map(|msg| self.process_order_payload(&msg, now_ms, local_tp))
                .ok(),
            Exchange::Gate => GateBasicOrderMsg::from_bytes(data)
                .map(|msg| self.process_order_payload(&msg, now_ms, local_tp))
                .ok(),
            Exchange::Bitget => BitgetBasicOrderMsg::from_bytes(data)
                .map(|msg| self.process_order_payload(&msg, now_ms, local_tp))
                .ok(),
            Exchange::Bybit => BybitBasicOrderMsg::from_bytes(data)
                .map(|msg| self.process_order_payload(&msg, now_ms, local_tp))
                .ok(),
            _ => None,
        };

        if let Some(events) = events {
            events
        } else {
            self.stats.account_decode_fail_count =
                self.stats.account_decode_fail_count.saturating_add(1);
            Vec::new()
        }
    }

    fn process_order_payload<T>(
        &mut self,
        update: &T,
        now_ms: i64,
        local_tp: i64,
    ) -> Vec<QueuePositionPublishEvent>
    where
        T: OrderUpdate + TradeUpdate,
    {
        let update = NormalizedUpdate::new(update);
        if OrderUpdate::trading_venue(&update) != self.venue {
            self.stats.account_filtered_count = self.stats.account_filtered_count.saturating_add(1);
            return Vec::new();
        }

        match update.execution_type() {
            ExecutionType::Trade => self.apply_trade_update(&update, local_tp),
            _ => self.apply_order_update(&update, now_ms, local_tp),
        }
    }

    fn apply_order_update(
        &mut self,
        update: &dyn OrderUpdate,
        now_ms: i64,
        local_tp: i64,
    ) -> Vec<QueuePositionPublishEvent> {
        let before = self.snapshot_map();
        let update_tp = valid_timestamp_us(OrderUpdate::event_time(update), local_tp);
        self.stats.order_update_count = self.stats.order_update_count.saturating_add(1);
        let client_order_id = update.client_order_id();
        if client_order_id <= 0 {
            return Vec::new();
        }

        if is_terminal_order_update(update) {
            if self.engine.remove_order(client_order_id).is_some() {
                self.order_symbols.remove(&client_order_id);
                self.order_first_seen_ms.remove(&client_order_id);
                self.stats.remove_order_count = self.stats.remove_order_count.saturating_add(1);
            }
            let events = self.diff_events(
                before,
                QueuePositionEventSource::Cancel,
                update_tp,
                local_tp,
            );
            self.order_create_tp.remove(&client_order_id);
            return events;
        }

        if self.engine.contains_order(client_order_id) {
            return Vec::new();
        }
        if !is_trackable_new_order(update) {
            return Vec::new();
        }

        let symbol = normalize_symbol_for_internal(update.symbol());
        if symbol.is_empty() || update.price() <= 0.0 || update.quantity() <= 0.0 {
            return Vec::new();
        }
        let side = side_from_order_side(update.side());
        let price_key = crate::depth_pub::orderbook::price_to_key(update.price());
        let level = LevelKey::from_order(symbol.clone(), side, price_key);
        let visible_level_qty = self.engine.level_qty(&level);

        let added = self.engine.add_order(AddOrder {
            order_id: client_order_id,
            symbol: symbol.clone(),
            side,
            price_key,
            qty: self.scale_qty(update.quantity()),
            visible_level_qty,
            front_qty_mode: FrontQtyMode::LevelExcludesOwnOrder,
        });
        if added {
            self.order_symbols.insert(client_order_id, symbol);
            self.order_first_seen_ms.insert(client_order_id, now_ms);
            self.order_create_tp.insert(client_order_id, update_tp);
            self.stats.add_order_count = self.stats.add_order_count.saturating_add(1);
        }
        self.diff_events(
            before,
            QueuePositionEventSource::Cancel,
            update_tp,
            local_tp,
        )
    }

    fn apply_trade_update(
        &mut self,
        trade: &dyn TradeUpdate,
        local_tp: i64,
    ) -> Vec<QueuePositionPublishEvent> {
        let before = self.snapshot_map();
        let update_tp = valid_timestamp_us(TradeUpdate::event_time(trade), local_tp);
        self.stats.trade_update_count = self.stats.trade_update_count.saturating_add(1);
        let client_order_id = trade.client_order_id();
        if client_order_id <= 0 {
            return Vec::new();
        }
        let cumulative = self.scale_qty(trade.cumulative_filled_quantity());
        if cumulative < 0.0 {
            return Vec::new();
        }
        if self
            .engine
            .apply_fill_update(FillUpdate {
                order_id: client_order_id,
                cumulative_filled_qty: cumulative,
            })
            .is_some()
        {
            self.order_symbols.remove(&client_order_id);
            self.order_first_seen_ms.remove(&client_order_id);
            self.stats.remove_order_count = self.stats.remove_order_count.saturating_add(1);
        }
        self.stats.fill_update_count = self.stats.fill_update_count.saturating_add(1);

        if is_terminal_trade_update(trade) {
            if self.engine.remove_order(client_order_id).is_some() {
                self.order_symbols.remove(&client_order_id);
                self.order_first_seen_ms.remove(&client_order_id);
                self.stats.remove_order_count = self.stats.remove_order_count.saturating_add(1);
            }
        }
        let events = self.diff_events(before, QueuePositionEventSource::Trade, update_tp, local_tp);
        if self.engine.contains_order(client_order_id) {
            events
        } else {
            self.order_create_tp.remove(&client_order_id);
            events
        }
    }

    fn snapshot_map(&self) -> HashMap<i64, QueuePositionSnapshot> {
        self.engine
            .order_snapshots()
            .into_iter()
            .map(|snapshot| {
                (
                    snapshot.order_id,
                    self.snapshot_from_order_snapshot(snapshot),
                )
            })
            .collect()
    }

    fn snapshot_from_order_snapshot(&self, snapshot: OrderSnapshot) -> QueuePositionSnapshot {
        let create_tp = self
            .order_create_tp
            .get(&snapshot.order_id)
            .copied()
            .unwrap_or(0);
        QueuePositionSnapshot::from_order_snapshot(&self.account_id, create_tp, snapshot)
    }

    fn diff_events(
        &self,
        before: HashMap<i64, QueuePositionSnapshot>,
        source: QueuePositionEventSource,
        update_tp: i64,
        local_tp: i64,
    ) -> Vec<QueuePositionPublishEvent> {
        let after = self.snapshot_map();
        let mut events = Vec::new();

        for (order_id, snapshot) in &after {
            if let Some(prev) = before.get(order_id) {
                if !queue_snapshot_changed(prev, snapshot) {
                    continue;
                }
                events.push(queue_position_msg_from_snapshot(
                    action_for_source(source),
                    update_tp,
                    local_tp,
                    snapshot,
                ));
            } else {
                events.push(queue_position_msg_from_snapshot(
                    OrderQueuePositionAction::New,
                    update_tp,
                    local_tp,
                    snapshot,
                ));
            }
        }

        for (order_id, snapshot) in before {
            if !after.contains_key(&order_id) {
                events.push(queue_position_msg_from_snapshot(
                    OrderQueuePositionAction::Del,
                    update_tp,
                    local_tp,
                    &snapshot,
                ));
            }
        }
        events
    }

    fn scale_qty(&self, qty: f64) -> f64 {
        qty * self.amount_scale
    }
}

struct NormalizedUpdate<'a, T> {
    inner: &'a T,
    symbol: String,
}

impl<'a, T> NormalizedUpdate<'a, T>
where
    T: OrderUpdate + TradeUpdate,
{
    fn new(inner: &'a T) -> Self {
        Self {
            inner,
            symbol: normalize_symbol_for_internal(OrderUpdate::symbol(inner)),
        }
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
        &self.symbol
    }

    fn order_id(&self) -> i64 {
        OrderUpdate::order_id(self.inner)
    }

    fn client_order_id(&self) -> i64 {
        OrderUpdate::client_order_id(self.inner)
    }

    fn side(&self) -> OrderSide {
        OrderUpdate::side(self.inner)
    }

    fn order_type(&self) -> OrderType {
        OrderUpdate::order_type(self.inner)
    }

    fn time_in_force(&self) -> TimeInForce {
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
        &self.symbol
    }

    fn order_id(&self) -> i64 {
        TradeUpdate::order_id(self.inner)
    }

    fn client_order_id(&self) -> i64 {
        TradeUpdate::client_order_id(self.inner)
    }

    fn side(&self) -> OrderSide {
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

fn side_from_order_side(side: OrderSide) -> Side {
    match side {
        OrderSide::Buy => Side::Buy,
        OrderSide::Sell => Side::Sell,
    }
}

fn action_for_source(source: QueuePositionEventSource) -> OrderQueuePositionAction {
    match source {
        QueuePositionEventSource::Cancel => OrderQueuePositionAction::UpdateByCancel,
        QueuePositionEventSource::Trade => OrderQueuePositionAction::UpdateByTrade,
    }
}

fn queue_snapshot_changed(prev: &QueuePositionSnapshot, next: &QueuePositionSnapshot) -> bool {
    (prev.inpos - next.inpos).abs() > f64::EPSILON
        || (prev.backlen - next.backlen).abs() > f64::EPSILON
        || (prev.tlen - next.tlen).abs() > f64::EPSILON
        || (prev.remaining_qty - next.remaining_qty).abs() > f64::EPSILON
        || (prev.queue_remaining_qty - next.queue_remaining_qty).abs() > f64::EPSILON
        || (prev.public_consumed_own_qty - next.public_consumed_own_qty).abs() > f64::EPSILON
}

fn queue_position_msg_from_snapshot(
    action: OrderQueuePositionAction,
    update_tp: i64,
    local_tp: i64,
    snapshot: &QueuePositionSnapshot,
) -> QueuePositionPublishEvent {
    QueuePositionPublishEvent {
        account_id: snapshot.account_id.clone(),
        msg: OrderQueuePositionMsg {
            action,
            create_tp: snapshot.create_tp,
            update_tp,
            local_tp,
            client_order_id: snapshot.order_id,
            tlen: snapshot.tlen,
            backlen: snapshot.backlen,
            inpos: snapshot.inpos,
        },
    }
}

fn valid_timestamp_us(value: i64, fallback: i64) -> i64 {
    if value > 0 {
        value
    } else {
        fallback
    }
}

pub fn book_side_from_orderbook(is_bid: bool) -> BookSide {
    if is_bid {
        BookSide::Bid
    } else {
        BookSide::Ask
    }
}

fn is_trackable_new_order(update: &dyn OrderUpdate) -> bool {
    matches!(update.order_type(), OrderType::Limit)
        && matches!(
            update.execution_type(),
            ExecutionType::New | ExecutionType::Replaced
        )
        && !matches!(
            update.status(),
            OrderStatus::Canceled | OrderStatus::Expired
        )
}

fn is_terminal_order_update(update: &dyn OrderUpdate) -> bool {
    update.status().is_finished() || update.execution_type().is_terminal()
}

fn is_terminal_trade_update(trade: &dyn TradeUpdate) -> bool {
    matches!(trade.order_status(), Some(status) if status.is_finished())
}

fn valid_amount_scale(value: f64) -> f64 {
    if value.is_finite() && value > 0.0 {
        value
    } else {
        1.0
    }
}

fn exchange_for_venue(venue: TradingVenue) -> Option<Exchange> {
    match venue {
        TradingVenue::BinanceMargin | TradingVenue::BinanceFutures => Some(Exchange::Binance),
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => Some(Exchange::Okex),
        TradingVenue::BybitMargin | TradingVenue::BybitFutures => Some(Exchange::Bybit),
        TradingVenue::BitgetMargin | TradingVenue::BitgetFutures => Some(Exchange::Bitget),
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
        TradingVenue::OkexMargin | TradingVenue::OkexFutures => {
            scope == BasicAccountScope::OkexUnified
        }
        TradingVenue::GateMargin | TradingVenue::GateFutures => {
            scope == BasicAccountScope::GateUnified
        }
        TradingVenue::BitgetMargin | TradingVenue::BitgetFutures => {
            scope == BasicAccountScope::BitgetUnified
        }
        TradingVenue::BybitMargin | TradingVenue::BybitFutures => {
            scope == BasicAccountScope::BybitUnified
        }
        _ => false,
    }
}

pub fn log_queue_position_snapshot(prefix: &str, snapshot: &QueuePositionSnapshot) {
    debug!(
        "{} account_id={} order_id={} symbol={} side={:?} price_key={} initial={:.8} remaining={:.8} queue_remaining={:.8} inpos={:.8} backlen={:.8} tlen={:.8}",
        prefix,
        snapshot.account_id,
        snapshot.order_id,
        snapshot.symbol,
        snapshot.side,
        snapshot.price_key,
        snapshot.initial_qty,
        snapshot.remaining_qty,
        snapshot.queue_remaining_qty,
        snapshot.inpos,
        snapshot.backlen,
        snapshot.tlen
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use mkt_parsers::msg::basic_account_msg::{BasicAccountEventMsg, BasicAccountScope};

    #[test]
    fn account_order_and_trade_updates_close_queue_position_loop() {
        let mut state = QueuePositionState::new(TradingVenue::BinanceFutures, 1.0).unwrap();
        let price_key = crate::depth_pub::orderbook::price_to_key(100.0);

        state.apply_level_qty("BTCUSDT", BookSide::Bid, price_key, 10.0, 9_000, 9_100);

        let new_order = test_binance_order(1_000, 0, ExecutionType::New, OrderStatus::New, 0.0);
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            BasicAccountScope::BinanceUnified,
            new_order.to_bytes(),
        );
        let events = state.process_account_payload(&event.to_bytes(), 10_000, 10_000_000);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].account_id, "");
        assert_eq!(events[0].msg.action, OrderQueuePositionAction::New);
        assert_eq!(events[0].msg.create_tp, 1_000_000);
        assert_eq!(events[0].msg.update_tp, 1_000_000);
        assert_eq!(events[0].msg.local_tp, 10_000_000);

        let snapshot = state.order_snapshot(42).expect("tracked");
        assert_eq!(snapshot.create_tp, 1_000_000);
        assert_eq!(snapshot.inpos, 10.0);
        assert_eq!(snapshot.backlen, 0.0);

        let events = state.apply_public_trade(
            "BTCUSDT",
            Side::Sell,
            price_key,
            3.0,
            10_000_500,
            10_000_600,
        );
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].msg.action,
            OrderQueuePositionAction::UpdateByTrade
        );
        assert_eq!(events[0].msg.create_tp, 1_000_000);
        assert_eq!(events[0].msg.update_tp, 10_000_500);
        assert_eq!(events[0].msg.local_tp, 10_000_600);
        let snapshot = state.order_snapshot(42).expect("tracked");
        assert_eq!(snapshot.inpos, 7.0);

        let trade_update = test_binance_order(
            1_001,
            1,
            ExecutionType::Trade,
            OrderStatus::PartiallyFilled,
            0.5,
        );
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            BasicAccountScope::BinanceUnified,
            trade_update.to_bytes(),
        );
        let events = state.process_account_payload(&event.to_bytes(), 10_001, 10_001_000);
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].msg.action,
            OrderQueuePositionAction::UpdateByTrade
        );
        assert_eq!(events[0].msg.create_tp, 1_000_000);
        assert_eq!(events[0].msg.update_tp, 1_001_000);
        assert_eq!(events[0].msg.local_tp, 10_001_000);

        let snapshot = state
            .order_snapshot(42)
            .expect("tracked after partial fill");
        assert_eq!(snapshot.remaining_qty, 1.5);
        assert_eq!(snapshot.queue_remaining_qty, 1.5);

        let filled_update =
            test_binance_order(1_002, 2, ExecutionType::Trade, OrderStatus::Filled, 2.0);
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            BasicAccountScope::BinanceUnified,
            filled_update.to_bytes(),
        );
        let events = state.process_account_payload(&event.to_bytes(), 10_002, 10_002_000);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].msg.action, OrderQueuePositionAction::Del);
        assert_eq!(events[0].msg.create_tp, 1_000_000);
        assert_eq!(events[0].msg.update_tp, 1_002_000);
        assert_eq!(events[0].msg.local_tp, 10_002_000);

        assert!(state.order_snapshot(42).is_none());
    }

    #[test]
    fn terminal_order_update_clears_tracked_order_without_ttl() {
        let mut state =
            QueuePositionState::new_for_account("acct", TradingVenue::BinanceFutures, 1.0).unwrap();
        let price_key = crate::depth_pub::orderbook::price_to_key(100.0);
        state.apply_level_qty("BTCUSDT", BookSide::Bid, price_key, 10.0, 9_000, 9_100);

        let new_order = test_binance_order(1_000, 0, ExecutionType::New, OrderStatus::New, 0.0);
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            BasicAccountScope::BinanceUnified,
            new_order.to_bytes(),
        );
        state.process_account_payload(&event.to_bytes(), 10_000, 10_000_000);
        assert!(state.order_snapshot(42).is_some());

        let cancel_update = test_binance_order(
            1_001,
            0,
            ExecutionType::Canceled,
            OrderStatus::Canceled,
            0.0,
        );
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            BasicAccountScope::BinanceUnified,
            cancel_update.to_bytes(),
        );
        let events = state.process_account_payload(&event.to_bytes(), 10_001, 10_001_000);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].account_id, "acct");
        assert_eq!(events[0].msg.action, OrderQueuePositionAction::Del);
        assert_eq!(events[0].msg.create_tp, 1_000_000);

        assert!(state.order_snapshot(42).is_none());
        assert_eq!(state.stats().remove_order_count, 1);
        assert_eq!(state.stats().expired_order_count, 0);
    }

    #[test]
    fn ttl_fallback_removes_stale_tracked_order() {
        let mut state =
            QueuePositionState::new_for_account("acct", TradingVenue::BinanceFutures, 1.0).unwrap();
        let price_key = crate::depth_pub::orderbook::price_to_key(100.0);
        state.apply_level_qty("BTCUSDT", BookSide::Bid, price_key, 10.0, 9_000, 9_100);

        let new_order = test_binance_order(1_000, 0, ExecutionType::New, OrderStatus::New, 0.0);
        let event = BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            BasicAccountScope::BinanceUnified,
            new_order.to_bytes(),
        );
        state.process_account_payload(&event.to_bytes(), 10_000, 10_000_000);
        assert!(state.order_snapshot(42).is_some());

        assert_eq!(
            state.clear_orders_older_than_ms(10_000 + 30 * 60 * 1_000 - 1, 30 * 60 * 1_000),
            0
        );
        assert_eq!(
            state.clear_orders_older_than_ms(10_000 + 30 * 60 * 1_000, 30 * 60 * 1_000),
            1
        );
        assert!(state.order_snapshot(42).is_none());
        assert_eq!(state.stats().expired_order_count, 1);
    }

    #[test]
    fn okex_unified_account_stream_filters_orders_by_depth_venue() {
        let mut margin_state =
            QueuePositionState::new_for_account("okex", TradingVenue::OkexMargin, 1.0).unwrap();
        let mut futures_state =
            QueuePositionState::new_for_account("okex", TradingVenue::OkexFutures, 1.0).unwrap();
        let margin_price_key = crate::depth_pub::orderbook::price_to_key(100.0);
        let futures_price_key = crate::depth_pub::orderbook::price_to_key(200.0);
        margin_state.apply_level_qty(
            "BTC-USDT",
            BookSide::Bid,
            margin_price_key,
            10.0,
            9_000,
            9_100,
        );
        futures_state.apply_level_qty(
            "BTC-USDT-SWAP",
            BookSide::Bid,
            futures_price_key,
            10.0,
            9_000,
            9_100,
        );

        let margin_order = test_okex_order(OkexOrderMsg::inst_type_to_u8("SPOT"), "BTC-USDT", 101);
        let margin_event = BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            BasicAccountScope::OkexUnified,
            margin_order.to_bytes(),
        );
        assert_eq!(
            futures_state
                .process_account_payload(&margin_event.to_bytes(), 10_000, 10_000_000)
                .len(),
            0
        );
        let margin_events =
            margin_state.process_account_payload(&margin_event.to_bytes(), 10_000, 10_000_000);
        assert_eq!(margin_events.len(), 1);
        assert_eq!(margin_events[0].msg.action, OrderQueuePositionAction::New);
        assert!(margin_state.order_snapshot(101).is_some());
        assert!(futures_state.order_snapshot(101).is_none());

        let futures_order =
            test_okex_order(OkexOrderMsg::inst_type_to_u8("SWAP"), "BTC-USDT-SWAP", 202);
        let futures_event = BasicAccountEventMsg::create(
            BasicAccountEventType::OrderUpdate,
            BasicAccountScope::OkexUnified,
            futures_order.to_bytes(),
        );
        assert_eq!(
            margin_state
                .process_account_payload(&futures_event.to_bytes(), 10_001, 10_001_000)
                .len(),
            0
        );
        let futures_events =
            futures_state.process_account_payload(&futures_event.to_bytes(), 10_001, 10_001_000);
        assert_eq!(futures_events.len(), 1);
        assert_eq!(futures_events[0].msg.action, OrderQueuePositionAction::New);
        assert!(margin_state.order_snapshot(202).is_none());
        assert!(futures_state.order_snapshot(202).is_some());
    }

    fn test_binance_order(
        event_time: i64,
        trade_id: i64,
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
            trade_id,
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

    fn test_okex_order(inst_type: u8, inst_id: &str, client_order_id: i64) -> OkexOrderMsg {
        OkexOrderMsg {
            msg_type: BasicAccountEventType::OrderUpdate,
            inst_id: inst_id.to_string(),
            inst_type,
            ord_id: client_order_id + 10_000,
            cl_ord_id: client_order_id,
            trade_id: 0,
            state: OkexOrderMsg::state_to_u8("live"),
            side: OrderSide::Buy.to_u8(),
            ord_type: 1,
            cancel_source: 0,
            amend_source: 0,
            price: if inst_type == OkexOrderMsg::inst_type_to_u8("SWAP") {
                200.0
            } else {
                100.0
            },
            quantity: 2.0,
            cumulative_filled_quantity: 0.0,
            create_time: 1_000,
            update_time: 1_000,
            fill_time: 0,
        }
    }
}
