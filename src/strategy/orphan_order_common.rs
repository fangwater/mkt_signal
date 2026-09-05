use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::PreTradeOrderRequestExt;
use crate::pre_trade::{QueryEngHub, TradeEngHub};
use crate::strategy::manager::{ExecOrphanTerminal, OrphanSourceKind, OrphanStrategyRole};
use crate::strategy::order_query_builder::build_order_query_request;
use crate::strategy::uniform_order_helper::{
    publish_uniform_new_order, publish_uniform_terminal_order, publish_uniform_trade_order,
    publish_uniform_trade_order_from_order_update, UniformPublishCtx,
};
use log::{info, warn};
use order_common::OrderQueryOrderUpdate;
use order_common::OrderUpdate;
use order_common::TradeUpdate;
use order_common::{
    hyperliquid_time_in_force, ExecutionType, OrderStatus, TimeInForce, TradingVenue,
};
use order_common::{Order, OrderExecutionStatus};
use runtime_common::fast_hash::{fast_hash_map, fast_hash_set, FastHashMap, FastHashSet};
use runtime_common::symbol_util::normalize_symbol_for_internal;
use runtime_common::time_util::get_timestamp_us;
use signal_common::hyperliquid::{
    HYPERLIQUID_ACTION_COMMIT_CLOCK_MARGIN_MS, MAX_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS,
};

pub(crate) const ORPHAN_QUERY_LOG_THRESHOLD: u8 = 25;
pub(crate) const COMMIT_QUERY_MAX_ATTEMPTS: u8 = 3;
pub(crate) const COMMIT_QUERY_BASE_TICKS: u32 = 50;
/// Commit 查询每档 ×4（`4^query_count`）；非 commit orphan 仍 ×2。
pub(crate) const COMMIT_QUERY_BACKOFF_SHIFT: u32 = 2;
pub(crate) const ORPHAN_QUERY_BACKOFF_SHIFT: u32 = 1;
pub(crate) const BINANCE_PM_ORPHAN_INITIAL_QUERY_TICKS: u32 = 100;
pub(crate) const BINANCE_PM_COMMIT_QUERY_MAX_ATTEMPTS: u8 = 6;
pub(crate) const BINANCE_PM_COMMIT_QUERY_BASE_TICKS: u32 = 500;
pub(crate) const EXEC_COMMIT_NOT_FOUND_GRACE_US: i64 = 15_000_000;
const FILL_EPS: f64 = 1e-12;

pub(crate) fn orphan_initial_query_ticks_for(
    venue: TradingVenue,
    binance_is_standard: bool,
    default_ticks: u32,
) -> u32 {
    if matches!(
        venue,
        TradingVenue::BinanceMargin
            | TradingVenue::BinanceFutures
            | TradingVenue::BinanceCoinFutures
    ) && !binance_is_standard
    {
        BINANCE_PM_ORPHAN_INITIAL_QUERY_TICKS
    } else {
        default_ticks
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CommitQueryPolicy {
    pub base_ticks: u32,
    pub max_attempts: u8,
}

pub(crate) fn commit_query_policy_for(
    venue: TradingVenue,
    binance_is_standard: bool,
) -> CommitQueryPolicy {
    if matches!(
        venue,
        TradingVenue::BinanceMargin
            | TradingVenue::BinanceFutures
            | TradingVenue::BinanceCoinFutures
    ) && !binance_is_standard
    {
        CommitQueryPolicy {
            base_ticks: BINANCE_PM_COMMIT_QUERY_BASE_TICKS,
            max_attempts: BINANCE_PM_COMMIT_QUERY_MAX_ATTEMPTS,
        }
    } else {
        CommitQueryPolicy {
            base_ticks: COMMIT_QUERY_BASE_TICKS,
            max_attempts: COMMIT_QUERY_MAX_ATTEMPTS,
        }
    }
}

pub(crate) const fn standard_commit_query_policy() -> CommitQueryPolicy {
    CommitQueryPolicy {
        base_ticks: COMMIT_QUERY_BASE_TICKS,
        max_attempts: COMMIT_QUERY_MAX_ATTEMPTS,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrphanOrderOwner {
    pub source_strategy_id: i32,
    pub source_kind: OrphanSourceKind,
    pub source_role: OrphanStrategyRole,
    pub uniform_ctx: UniformPublishCtx,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OrphanQueryState {
    query_count: u8,
    ticks_until_next_query: u32,
    consecutive_not_found: u8,
    first_not_found_at_us: i64,
}

impl OrphanQueryState {
    fn record_not_found(&mut self, now_us: i64) {
        if self.consecutive_not_found == 0 {
            self.first_not_found_at_us = now_us;
        }
        self.consecutive_not_found = self.consecutive_not_found.saturating_add(1);
    }

    fn reset_not_found(&mut self) {
        self.consecutive_not_found = 0;
        self.first_not_found_at_us = 0;
    }

    fn has_confirmed_not_found(
        &self,
        required_confirmations: u8,
        now_us: i64,
        terminal_not_before_us: i64,
    ) -> bool {
        self.query_count >= required_confirmations
            && self.consecutive_not_found >= required_confirmations
            && self.first_not_found_at_us > 0
            && now_us.saturating_sub(self.first_not_found_at_us) >= EXEC_COMMIT_NOT_FOUND_GRACE_US
            && now_us >= terminal_not_before_us
    }
}

fn hyperliquid_not_found_terminal_barrier_us(
    create_time_us: i64,
    expires_after_ms: u64,
) -> Option<i64> {
    if create_time_us <= 0 {
        return None;
    }
    let protected_ms = expires_after_ms.checked_add(HYPERLIQUID_ACTION_COMMIT_CLOCK_MARGIN_MS)?;
    let protected_us = i64::try_from(protected_ms.checked_mul(1_000)?).ok()?;
    create_time_us.checked_add(protected_us)
}

pub struct OrphanOrderTracker {
    order_ids: FastHashSet<i64>,
    order_owners: FastHashMap<i64, OrphanOrderOwner>,
    query_states: FastHashMap<i64, OrphanQueryState>,
    initial_query_ticks: u32,
    query_base_ticks: u32,
    query_max_ticks: u32,
}

impl OrphanOrderTracker {
    pub fn new(initial_query_ticks: u32, query_base_ticks: u32, query_max_ticks: u32) -> Self {
        Self {
            order_ids: fast_hash_set(),
            order_owners: fast_hash_map(),
            query_states: fast_hash_map(),
            initial_query_ticks,
            query_base_ticks,
            query_max_ticks,
        }
    }

    pub fn len(&self) -> usize {
        self.order_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.order_ids.is_empty()
    }

    pub fn contains(&self, client_order_id: i64) -> bool {
        self.order_ids.contains(&client_order_id)
    }

    pub fn tracked_order_ids(&self) -> Vec<i64> {
        self.order_ids.iter().copied().collect()
    }

    pub fn owner(&self, client_order_id: i64) -> Option<OrphanOrderOwner> {
        self.order_owners.get(&client_order_id).cloned()
    }

    pub fn uniform_ctx(&self, client_order_id: i64) -> Option<UniformPublishCtx> {
        self.order_owners
            .get(&client_order_id)
            .map(|owner| owner.uniform_ctx.clone())
    }

    fn initial_query_ticks_for_order(&self, client_order_id: i64) -> u32 {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return self.initial_query_ticks;
        };
        let mgr = order_mgr.borrow();
        let Some(order) = mgr.get(client_order_id) else {
            return self.initial_query_ticks;
        };
        let binance_is_standard = mgr.binance_is_standard();
        if order.status == OrderExecutionStatus::Commit {
            return commit_query_policy_for(order.venue, binance_is_standard).base_ticks;
        }
        orphan_initial_query_ticks_for(order.venue, binance_is_standard, self.initial_query_ticks)
    }

    fn commit_query_policy_for_order(&self, client_order_id: i64) -> CommitQueryPolicy {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return standard_commit_query_policy();
        };
        let mgr = order_mgr.borrow();
        let Some(order) = mgr.get(client_order_id) else {
            return standard_commit_query_policy();
        };
        commit_query_policy_for(order.venue, mgr.binance_is_standard())
    }

    fn track_order_id(&mut self, client_order_id: i64) {
        if client_order_id <= 0 {
            return;
        }
        self.order_ids.insert(client_order_id);
        self.ensure_query_state(client_order_id);
    }

    pub fn adopt_order_owner(&mut self, client_order_id: i64, owner: OrphanOrderOwner) {
        if client_order_id <= 0 {
            return;
        }
        self.track_order_id(client_order_id);
        self.order_owners.insert(client_order_id, owner);
    }

    pub fn forget_order_id(
        &mut self,
        strategy_role: &str,
        strategy_id: i32,
        client_order_id: i64,
        reason: &str,
    ) -> bool {
        let removed = self.order_ids.remove(&client_order_id);
        if removed {
            self.order_owners.remove(&client_order_id);
            self.query_states.remove(&client_order_id);
            info!(
                "{}: strategy_id={} forgot order_id client_order_id={} reason={}",
                strategy_role, strategy_id, client_order_id, reason
            );
        }
        removed
    }

    pub fn query_due_now(&mut self, client_order_id: i64) -> bool {
        let query_base_ticks = self.query_base_ticks;
        let query_max_ticks = self.query_max_ticks;
        let Some(query_state) = self.query_states.get_mut(&client_order_id) else {
            return false;
        };
        if query_state.ticks_until_next_query > 0 {
            query_state.ticks_until_next_query -= 1;
            return false;
        }
        let next_query_count = query_state.query_count.saturating_add(1);
        query_state.query_count = next_query_count;
        query_state.ticks_until_next_query = query_backoff_ticks(
            query_base_ticks,
            query_max_ticks,
            next_query_count,
            ORPHAN_QUERY_BACKOFF_SHIFT,
        );
        true
    }

    pub fn query_count(&self, client_order_id: i64) -> Option<u8> {
        self.query_states
            .get(&client_order_id)
            .map(|state| state.query_count)
    }

    pub fn reset_order_query_not_found(&mut self, client_order_id: i64) {
        if let Some(state) = self.query_states.get_mut(&client_order_id) {
            state.reset_not_found();
        }
    }

    pub fn record_order_query_not_found(
        &mut self,
        strategy_role: &str,
        strategy_id: i32,
        client_order_id: i64,
    ) {
        let Some(owner) = self.order_owners.get(&client_order_id) else {
            return;
        };
        if owner.source_role != OrphanStrategyRole::Exec {
            return;
        }
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };
        let snapshot = {
            let mgr = order_mgr.borrow();
            let Some(order) = mgr.get(client_order_id) else {
                return;
            };
            (
                order.status,
                order.cumulative_filled_quantity,
                order.exchange_order_id,
                order.symbol.clone(),
                order.venue,
                order.timestamp.create_t,
                commit_query_policy_for(order.venue, mgr.binance_is_standard()),
            )
        };
        let (status, cumulative_fill, exchange_order_id, symbol, venue, create_time_us, policy) =
            snapshot;
        if status != OrderExecutionStatus::Commit
            || cumulative_fill > FILL_EPS
            || exchange_order_id.is_some_and(|id| id > 0)
        {
            self.reset_order_query_not_found(client_order_id);
            return;
        }

        let terminal_not_before_us = if matches!(
            venue,
            TradingVenue::HyperliquidMargin | TradingVenue::HyperliquidFutures
        ) {
            // This process may restart with a different env value than the trade_engine that
            // signed the ambiguous action. Use the maximum accepted TTL, not the current env.
            let Some(barrier_us) = hyperliquid_not_found_terminal_barrier_us(
                create_time_us,
                MAX_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS,
            ) else {
                warn!(
                    "{}: strategy_id={} refusing to terminalize Hyperliquid Exec orphan without a valid create-time expiry barrier client_order_id={} create_time_us={} max_expires_after_ms={}",
                    strategy_role,
                    strategy_id,
                    client_order_id,
                    create_time_us,
                    MAX_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS
                );
                return;
            };
            barrier_us
        } else {
            i64::MIN
        };

        let now_us = get_timestamp_us();
        let Some(state) = self.query_states.get_mut(&client_order_id) else {
            return;
        };
        state.record_not_found(now_us);
        let confirmed =
            state.has_confirmed_not_found(policy.max_attempts, now_us, terminal_not_before_us);
        let count = state.consecutive_not_found;
        let elapsed_us = now_us.saturating_sub(state.first_not_found_at_us);
        if !confirmed {
            info!(
                "{}: strategy_id={} Exec orphan not-found evidence client_order_id={} symbol={} venue={:?} queries={} confirmations={}/{} elapsed_ms={} grace_ms={} terminal_barrier_remaining_ms={}",
                strategy_role,
                strategy_id,
                client_order_id,
                symbol,
                venue,
                state.query_count,
                count,
                policy.max_attempts,
                elapsed_us / 1_000,
                EXEC_COMMIT_NOT_FOUND_GRACE_US / 1_000,
                terminal_not_before_us.saturating_sub(now_us).max(0) / 1_000,
            );
            return;
        }

        self.close_exec_commit_after_confirmed_not_found(
            strategy_role,
            strategy_id,
            client_order_id,
            count,
            elapsed_us,
        );
    }

    fn commit_query_due_now(&mut self, client_order_id: i64) -> Option<CommitQueryAction> {
        let policy = self.commit_query_policy_for_order(client_order_id);
        let query_max_ticks = self.query_max_ticks;
        let keep_querying_when_unresolved = self
            .order_owners
            .get(&client_order_id)
            .is_some_and(|owner| owner.source_role == OrphanStrategyRole::Exec);
        let Some(query_state) = self.query_states.get_mut(&client_order_id) else {
            return None;
        };
        if query_state.query_count == 0 {
            query_state.ticks_until_next_query =
                query_state.ticks_until_next_query.min(policy.base_ticks);
        }
        if query_state.ticks_until_next_query > 0 {
            query_state.ticks_until_next_query -= 1;
            return None;
        }
        let budget_exhausted = query_state.query_count >= policy.max_attempts;
        if budget_exhausted && !keep_querying_when_unresolved {
            return Some(CommitQueryAction::Close);
        }

        query_state.query_count = query_state.query_count.saturating_add(1);
        query_state.ticks_until_next_query = query_backoff_ticks(
            policy.base_ticks,
            query_max_ticks,
            query_state.query_count,
            COMMIT_QUERY_BACKOFF_SHIFT,
        );
        Some(CommitQueryAction::Query {
            query_count: query_state.query_count,
            budget_exhausted,
        })
    }

    pub fn log_orders_over_query_threshold(&self, strategy_role: &str, strategy_id: i32) {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };
        let now_us = get_timestamp_us();
        let order_mgr = order_mgr.borrow();
        let mut rows: Vec<(i64, String)> = self
            .query_states
            .iter()
            .filter(|(_, state)| state.query_count > ORPHAN_QUERY_LOG_THRESHOLD)
            .filter_map(|(client_order_id, _)| {
                order_mgr
                    .get(*client_order_id)
                    .map(|order| (*client_order_id, order_query_time_utc(&order, now_us)))
            })
            .collect();
        rows.sort_by_key(|(client_order_id, _)| *client_order_id);
        if rows.is_empty() {
            return;
        }
        warn!(
            "{}: strategy_id={} orphan orders query_count>{}\n{}",
            strategy_role,
            strategy_id,
            ORPHAN_QUERY_LOG_THRESHOLD,
            format_orphan_query_table(&rows)
        );
    }

    pub fn send_order_query(
        &mut self,
        strategy_role: &str,
        strategy_id: i32,
        client_order_id: i64,
    ) -> bool {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return false;
        };
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "{}: strategy_id={} send_order_query missing local order client_order_id={}",
                strategy_role, strategy_id, client_order_id
            );
            return false;
        };
        let request_query_id = client_order_id;
        match build_order_query_request(&order, request_query_id, client_order_id) {
            Ok((exchange, req_bytes)) => {
                if let Err(err) = QueryEngHub::publish_query_request_for(
                    client_order_id,
                    exchange.as_str(),
                    &req_bytes,
                ) {
                    warn!(
                        "{}: strategy_id={} publish query failed client_order_id={} request_query_id={} err={:#}",
                        strategy_role, strategy_id, client_order_id, request_query_id, err
                    );
                    return false;
                }
                info!(
                    "{}: strategy_id={} query sent client_order_id={} request_query_id={}",
                    strategy_role, strategy_id, client_order_id, request_query_id
                );
                true
            }
            Err(err) => {
                warn!(
                    "{}: strategy_id={} build query failed client_order_id={} err={}",
                    strategy_role, strategy_id, client_order_id, err
                );
                false
            }
        }
    }

    pub fn apply_order_update(
        &mut self,
        strategy_role: &str,
        strategy_id: i32,
        update: &dyn OrderUpdate,
    ) -> bool {
        let client_order_id = update.client_order_id();
        if !self.contains(client_order_id) {
            return false;
        }
        let Some(ctx) = self.uniform_ctx(client_order_id) else {
            return false;
        };
        let prev_cumulative_filled_qty = MonitorChannel::try_order_manager()
            .and_then(|order_mgr| {
                order_mgr
                    .borrow()
                    .get(client_order_id)
                    .map(|order| order.cumulative_filled_quantity)
            })
            .unwrap_or(0.0);

        if let Some(order_mgr) = MonitorChannel::try_order_manager() {
            let incoming_cum = update.cumulative_filled_quantity();
            let incoming_order_id = update.order_id();
            let incoming_price = update.price();
            let event_time = update.event_time();
            let status = update.status();
            let _ = order_mgr
                .borrow_mut()
                .apply_remote_update(client_order_id, |order| {
                    if incoming_cum > order.cumulative_filled_quantity {
                        order.cumulative_filled_quantity = incoming_cum;
                    }
                    if incoming_order_id > 0 {
                        order.set_exchange_order_id(incoming_order_id);
                    }
                    if incoming_price > 0.0 {
                        order.price = incoming_price;
                    }
                    match status {
                        OrderStatus::New | OrderStatus::PartiallyFilled => {
                            if !order.status.is_terminal() {
                                order.status = OrderExecutionStatus::Create;
                            }
                        }
                        OrderStatus::Canceled => {
                            order.status = OrderExecutionStatus::Cancelled;
                            order.set_end_time(event_time);
                        }
                        OrderStatus::Filled => {
                            order.status = OrderExecutionStatus::Filled;
                            order.set_end_time(event_time);
                        }
                        OrderStatus::Expired | OrderStatus::ExpiredInMatch => {
                            order.status = OrderExecutionStatus::Rejected;
                            order.set_end_time(event_time);
                        }
                    }
                });
        }

        let updated_order = MonitorChannel::try_order_manager()
            .and_then(|order_mgr| order_mgr.borrow().get(client_order_id));
        if let Some(order) = updated_order {
            if update.status() == OrderStatus::New {
                publish_uniform_new_order(
                    update,
                    &order,
                    prev_cumulative_filled_qty,
                    &ctx,
                    strategy_role,
                    strategy_id,
                );
            }
            if !update.status().is_finished()
                && matches!(
                    update.status(),
                    OrderStatus::PartiallyFilled | OrderStatus::Filled
                )
            {
                publish_uniform_trade_order_from_order_update(
                    update,
                    &order,
                    prev_cumulative_filled_qty,
                    &ctx,
                    strategy_role,
                    strategy_id,
                );
            }
        }
        let terminal_publish_snapshot = MonitorChannel::try_order_manager()
            .and_then(|order_mgr| order_mgr.borrow().get(client_order_id))
            .map(|order| (order, ctx.clone()));

        if matches!(
            update.status(),
            OrderStatus::Canceled
                | OrderStatus::Filled
                | OrderStatus::Expired
                | OrderStatus::ExpiredInMatch
        ) {
            self.finalize_terminal_order(
                strategy_role,
                strategy_id,
                client_order_id,
                update.event_time(),
                "terminal order update",
                0.0,
            );
        } else {
            let _ = self.request_cancel_from_order_update(strategy_role, strategy_id, update);
        }
        if let Some((order, ctx)) = terminal_publish_snapshot.as_ref() {
            if matches!(
                update.status(),
                OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::ExpiredInMatch
            ) {
                publish_uniform_terminal_order(
                    update,
                    order,
                    prev_cumulative_filled_qty,
                    ctx,
                    strategy_role,
                    strategy_id,
                );
            } else if matches!(update.status(), OrderStatus::Filled) {
                publish_uniform_trade_order_from_order_update(
                    update,
                    order,
                    prev_cumulative_filled_qty,
                    ctx,
                    strategy_role,
                    strategy_id,
                );
            }
        }
        info!(
            "{}: strategy_id={} adopted order_update symbol={} client_order_id={} order_id={} venue={:?} x={:?} X={:?}",
            strategy_role,
            strategy_id,
            update.symbol(),
            update.client_order_id(),
            update.order_id(),
            update.trading_venue(),
            update.execution_type(),
            update.status()
        );
        true
    }

    pub fn apply_trade_update(
        &mut self,
        strategy_role: &str,
        strategy_id: i32,
        trade: &dyn TradeUpdate,
    ) -> bool {
        let client_order_id = trade.client_order_id();
        if !self.contains(client_order_id) {
            return false;
        }
        let Some(ctx) = self.uniform_ctx(client_order_id) else {
            return false;
        };
        let prev_cumulative_filled_qty = MonitorChannel::try_order_manager()
            .and_then(|order_mgr| {
                order_mgr
                    .borrow()
                    .get(client_order_id)
                    .map(|order| order.cumulative_filled_quantity)
            })
            .unwrap_or(0.0);

        if let Some(order_mgr) = MonitorChannel::try_order_manager() {
            let cumulative_qty = trade.cumulative_filled_quantity();
            let event_time = trade.event_time();
            let order_id = trade.order_id();
            let price = trade.price();
            let terminal_status = trade.order_status();
            let _ = order_mgr
                .borrow_mut()
                .apply_remote_update(client_order_id, |order| {
                    if cumulative_qty > order.cumulative_filled_quantity {
                        order.cumulative_filled_quantity = cumulative_qty;
                    }
                    if order_id > 0 {
                        order.set_exchange_order_id(order_id);
                    }
                    if price > 0.0 {
                        order.price = price;
                    }
                    match terminal_status {
                        Some(OrderStatus::Filled) => {
                            order.status = OrderExecutionStatus::Filled;
                            order.set_end_time(event_time);
                        }
                        Some(OrderStatus::PartiallyFilled) => {
                            if !order.status.is_terminal() {
                                order.status = OrderExecutionStatus::Create;
                            }
                        }
                        Some(OrderStatus::Canceled) => {
                            order.status = OrderExecutionStatus::Cancelled;
                            order.set_end_time(event_time);
                        }
                        Some(OrderStatus::Expired | OrderStatus::ExpiredInMatch) => {
                            order.status = OrderExecutionStatus::Rejected;
                            order.set_end_time(event_time);
                        }
                        Some(OrderStatus::New) | None => {}
                    }
                });
        }

        let trade_publish_snapshot = trade.order_status().and_then(|status| {
            MonitorChannel::try_order_manager()
                .and_then(|order_mgr| order_mgr.borrow().get(client_order_id))
                .map(|order| (order, ctx.clone(), status))
        });
        if let Some((order, ctx, status)) = trade_publish_snapshot.as_ref() {
            if !status.is_finished() {
                publish_uniform_trade_order(
                    trade,
                    order,
                    prev_cumulative_filled_qty,
                    *status,
                    ctx,
                    strategy_role,
                    strategy_id,
                );
            }
        }
        if trade.order_status().is_some_and(|status| {
            matches!(
                status,
                OrderStatus::Canceled
                    | OrderStatus::Filled
                    | OrderStatus::Expired
                    | OrderStatus::ExpiredInMatch
            )
        }) {
            self.finalize_terminal_order(
                strategy_role,
                strategy_id,
                client_order_id,
                trade.event_time(),
                "terminal trade update",
                0.0,
            );
        }
        if let Some((order, ctx, status)) = trade_publish_snapshot.as_ref() {
            if status.is_finished() {
                publish_uniform_trade_order(
                    trade,
                    order,
                    prev_cumulative_filled_qty,
                    *status,
                    ctx,
                    strategy_role,
                    strategy_id,
                );
            }
        }
        info!(
            "{}: strategy_id={} adopted trade_update symbol={} client_order_id={} order_id={} venue={:?} cumulative_qty={:.8} status={:?}",
            strategy_role,
            strategy_id,
            trade.symbol(),
            trade.client_order_id(),
            trade.order_id(),
            trade.trading_venue(),
            trade.cumulative_filled_quantity(),
            trade.order_status()
        );
        true
    }

    pub fn request_cancel_from_order_update(
        &self,
        strategy_role: &str,
        strategy_id: i32,
        update: &dyn OrderUpdate,
    ) -> bool {
        if update.execution_type() == ExecutionType::Trade {
            return false;
        }
        if !matches!(
            update.status(),
            OrderStatus::New | OrderStatus::PartiallyFilled
        ) {
            return false;
        }

        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return false;
        };
        let Some(order) = order_mgr.borrow().get(update.client_order_id()) else {
            return false;
        };
        if order.status.is_terminal() {
            return false;
        }

        let client_order_id = order.client_order_id;
        let symbol = order.symbol.clone();
        let venue = order.venue;
        let exchange = venue.trade_engine_exchange();
        let cancel_bytes = match order.get_order_cancel_bytes() {
            Ok(bytes) => bytes,
            Err(err) => {
                warn!(
                    "{}: strategy_id={} failed to build cancel client_order_id={} symbol={} venue={:?}: {}",
                    strategy_role, strategy_id, client_order_id, symbol, venue, err
                );
                return false;
            }
        };
        drop(order);

        match TradeEngHub::publish_order_request_for(client_order_id, exchange, &cancel_bytes) {
            Ok(()) => {
                warn!(
                    "{}: strategy_id={} sent cancel client_order_id={} order_id={} symbol={} venue={:?} x={:?} X={:?}",
                    strategy_role,
                    strategy_id,
                    update.client_order_id(),
                    update.order_id(),
                    update.symbol(),
                    update.trading_venue(),
                    update.execution_type(),
                    update.status()
                );
                true
            }
            Err(err) => {
                warn!(
                    "{}: strategy_id={} failed to send cancel client_order_id={} order_id={} symbol={} venue={:?}: {:#}",
                    strategy_role,
                    strategy_id,
                    update.client_order_id(),
                    update.order_id(),
                    update.symbol(),
                    update.trading_venue(),
                    err
                );
                false
            }
        }
    }

    pub fn finalize_terminal_order(
        &mut self,
        strategy_role: &str,
        strategy_id: i32,
        client_order_id: i64,
        event_time: i64,
        reason: &str,
        eps: f64,
    ) {
        let owner = self.owner(client_order_id);
        let had_owner = owner.is_some();
        let retain_until_source_applies = owner
            .as_ref()
            .is_some_and(|owner| owner.source_role == OrphanStrategyRole::Exec);
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            if retain_until_source_applies {
                warn!(
                    "{}: strategy_id={} retain Exec orphan without order manager client_order_id={} reason={}",
                    strategy_role, strategy_id, client_order_id, reason
                );
            } else {
                self.forget_order_id(strategy_role, strategy_id, client_order_id, reason);
            }
            return;
        };
        let snapshot = {
            let mgr = order_mgr.borrow();
            mgr.get(client_order_id).map(|order| {
                (
                    order.venue,
                    order.symbol.clone(),
                    order.side,
                    order.quantity,
                    order.cumulative_filled_quantity,
                    order.price,
                )
            })
        };
        let Some((venue, symbol, side, order_qty, cumulative_qty, price)) = snapshot else {
            if retain_until_source_applies {
                warn!(
                    "{}: strategy_id={} retain Exec orphan with missing local order client_order_id={} reason={}",
                    strategy_role, strategy_id, client_order_id, reason
                );
            } else {
                self.forget_order_id(strategy_role, strategy_id, client_order_id, reason);
            }
            return;
        };

        let source_applied = if let Some(owner) = owner {
            let order_base_qty = MonitorChannel::instance()
                .qty_to_base_at_price(venue, &symbol, order_qty, price)
                .unwrap_or(0.0);
            let cumulative_base_qty = MonitorChannel::instance()
                .qty_to_base_at_price(venue, &symbol, cumulative_qty, price)
                .unwrap_or(0.0);
            let should_record = owner.source_role == OrphanStrategyRole::Exec
                || match owner.source_kind {
                    OrphanSourceKind::Open => cumulative_base_qty > eps,
                    OrphanSourceKind::Hedge => order_base_qty > eps || cumulative_base_qty > eps,
                };
            let recorded = if should_record {
                let strategy_mgr = MonitorChannel::instance().strategy_mgr();
                let mut strategy_mgr = strategy_mgr.borrow_mut();
                let normalized_symbol = normalize_symbol_for_internal(&symbol);
                let recorded = if owner.source_role == OrphanStrategyRole::Exec {
                    strategy_mgr.apply_exec_orphan_terminal(
                        owner.source_strategy_id,
                        &ExecOrphanTerminal {
                            client_order_id,
                            source_kind: owner.source_kind,
                            terminal_ts: event_time,
                            side,
                            order_base_qty,
                            filled_base_qty: cumulative_base_qty,
                            price,
                        },
                    )
                } else {
                    match owner.source_kind {
                        OrphanSourceKind::Open => strategy_mgr.record_open_order_terminal(
                            &normalized_symbol,
                            side,
                            order_base_qty,
                            cumulative_base_qty,
                            event_time,
                            price,
                            0,
                            client_order_id,
                        ),
                        OrphanSourceKind::Hedge => strategy_mgr.record_hedge_order_terminal(
                            &normalized_symbol,
                            side,
                            order_base_qty,
                            cumulative_base_qty,
                            event_time,
                            price,
                            0,
                            client_order_id,
                        ),
                    }
                };
                if !recorded {
                    warn!(
                        "{}: strategy_id={} record order terminal failed client_order_id={} symbol={} source_strategy_id={} source_role={:?} source_kind={:?} cumulative_base_qty={:.8} reason={}",
                        strategy_role,
                        strategy_id,
                        client_order_id,
                        normalized_symbol,
                        owner.source_strategy_id,
                        owner.source_role,
                        owner.source_kind,
                        cumulative_base_qty,
                        reason
                    );
                }
                recorded
            } else {
                true
            };
            info!(
                "{}: strategy_id={} finalized order client_order_id={} source_strategy_id={} source_role={:?} source_kind={:?} symbol={} venue={:?} side={:?} order_qty={:.8} cumulative_qty={:.8} order_base_qty={:.8} cumulative_base_qty={:.8} reason={}",
                strategy_role,
                strategy_id,
                client_order_id,
                owner.source_strategy_id,
                owner.source_role,
                owner.source_kind,
                symbol,
                venue,
                side,
                order_qty,
                cumulative_qty,
                order_base_qty,
                cumulative_base_qty,
                reason
            );
            recorded
        } else {
            warn!(
                "{}: strategy_id={} finalize terminal order missing owner client_order_id={} reason={}",
                strategy_role, strategy_id, client_order_id, reason
            );
            false
        };

        if retain_until_source_applies && !source_applied {
            return;
        }
        if had_owner {
            let _ = order_mgr.borrow_mut().remove(client_order_id);
        }
        self.forget_order_id(strategy_role, strategy_id, client_order_id, reason);
    }

    pub fn handle_period_clock(&mut self, strategy_role: &str, strategy_id: i32) {
        let tracked_order_ids = self.tracked_order_ids();
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return;
        };

        for client_order_id in tracked_order_ids {
            let order_opt = order_mgr.borrow().get(client_order_id);
            let Some(order) = order_opt else {
                if self
                    .order_owners
                    .get(&client_order_id)
                    .is_some_and(|owner| owner.source_role == OrphanStrategyRole::Exec)
                {
                    if self.query_due_now(client_order_id) {
                        warn!(
                            "{}: strategy_id={} retain unresolved Exec orphan with missing local order client_order_id={} query_count={}",
                            strategy_role,
                            strategy_id,
                            client_order_id,
                            self.query_count(client_order_id).unwrap_or_default()
                        );
                    }
                } else {
                    self.forget_order_id(
                        strategy_role,
                        strategy_id,
                        client_order_id,
                        "missing local order on period clock",
                    );
                }
                continue;
            };
            if order.status.is_terminal() {
                drop(order);
                self.finalize_terminal_order(
                    strategy_role,
                    strategy_id,
                    client_order_id,
                    get_timestamp_us(),
                    "terminal local order on period clock",
                    0.0,
                );
                continue;
            }

            if order.status == OrderExecutionStatus::Commit {
                drop(order);
                match self.commit_query_due_now(client_order_id) {
                    Some(CommitQueryAction::Query {
                        query_count,
                        budget_exhausted,
                    }) => {
                        if budget_exhausted {
                            warn!(
                                "{}: strategy_id={} Exec orphan commit state remains unknown after query budget; keeping source blocked client_order_id={} query_count={}",
                                strategy_role, strategy_id, client_order_id, query_count
                            );
                        }
                        let _ = self.send_order_query(strategy_role, strategy_id, client_order_id);
                        if query_count > ORPHAN_QUERY_LOG_THRESHOLD {
                            self.log_orders_over_query_threshold(strategy_role, strategy_id);
                        }
                    }
                    Some(CommitQueryAction::Close) => {
                        self.close_commit_order_after_query_budget(
                            strategy_role,
                            strategy_id,
                            client_order_id,
                        );
                    }
                    None => {}
                }
                continue;
            }

            drop(order);
            if self.query_due_now(client_order_id) {
                let query_count = self.query_count(client_order_id).unwrap_or_default();
                let _ = self.send_order_query(strategy_role, strategy_id, client_order_id);
                if query_count > ORPHAN_QUERY_LOG_THRESHOLD {
                    self.log_orders_over_query_threshold(strategy_role, strategy_id);
                }
            }
        }
    }

    fn ensure_query_state(&mut self, client_order_id: i64) {
        let initial_query_ticks = self.initial_query_ticks_for_order(client_order_id);
        self.query_states
            .entry(client_order_id)
            .or_insert_with(|| OrphanQueryState {
                query_count: 0,
                ticks_until_next_query: initial_query_ticks,
                consecutive_not_found: 0,
                first_not_found_at_us: 0,
            });
    }

    fn close_commit_order_after_query_budget(
        &mut self,
        strategy_role: &str,
        strategy_id: i32,
        client_order_id: i64,
    ) {
        if self
            .order_owners
            .get(&client_order_id)
            .is_some_and(|owner| owner.source_role == OrphanStrategyRole::Exec)
        {
            warn!(
                "{}: strategy_id={} refusing to synthesize terminal state for unresolved Exec orphan client_order_id={}",
                strategy_role, strategy_id, client_order_id
            );
            return;
        }
        self.synthesize_commit_terminal(
            strategy_role,
            strategy_id,
            client_order_id,
            "commit query budget exhausted",
        );
    }

    fn close_exec_commit_after_confirmed_not_found(
        &mut self,
        strategy_role: &str,
        strategy_id: i32,
        client_order_id: i64,
        confirmations: u8,
        elapsed_us: i64,
    ) {
        warn!(
            "{}: strategy_id={} Exec orphan confirmed absent; synthesizing terminal client_order_id={} confirmations={} elapsed_ms={}",
            strategy_role,
            strategy_id,
            client_order_id,
            confirmations,
            elapsed_us / 1_000,
        );
        self.synthesize_commit_terminal(
            strategy_role,
            strategy_id,
            client_order_id,
            "confirmed order-query not found",
        );
    }

    fn synthesize_commit_terminal(
        &mut self,
        strategy_role: &str,
        strategy_id: i32,
        client_order_id: i64,
        reason: &str,
    ) {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            self.forget_order_id(
                strategy_role,
                strategy_id,
                client_order_id,
                &format!("{reason} without order manager"),
            );
            return;
        };
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            self.forget_order_id(
                strategy_role,
                strategy_id,
                client_order_id,
                &format!("{reason} missing local order"),
            );
            return;
        };
        if order.status != OrderExecutionStatus::Commit {
            return;
        }

        let query_count = self.query_count(client_order_id).unwrap_or_default();
        warn!(
            "{}: strategy_id={} closing local commit orphan client_order_id={} symbol={} venue={:?} query_count={} reason={}",
            strategy_role, strategy_id, client_order_id, order.symbol, order.venue, query_count, reason
        );
        let update = OrderQueryOrderUpdate::new(
            &order,
            order.exchange_order_id.unwrap_or(order.client_order_id),
            get_timestamp_us(),
            OrderStatus::Expired,
            ExecutionType::Rejected,
            order.cumulative_filled_quantity,
            infer_query_time_in_force(&order),
        );
        let _ = self.apply_order_update(strategy_role, strategy_id, &update);
    }
}

/// `min(base * 2^(query_count * shift_per_attempt), max)`。
/// `shift_per_attempt=1` → ×2，`shift_per_attempt=2` → ×4。
pub(crate) fn query_backoff_ticks(
    query_base_ticks: u32,
    query_max_ticks: u32,
    query_count: u8,
    shift_per_attempt: u32,
) -> u32 {
    let shift = (query_count as u32)
        .saturating_mul(shift_per_attempt)
        .min(31);
    let multiplier = 1_u32.checked_shl(shift).unwrap_or(u32::MAX);
    query_base_ticks
        .saturating_mul(multiplier)
        .min(query_max_ticks)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitQueryAction {
    Query {
        query_count: u8,
        budget_exhausted: bool,
    },
    Close,
}

pub(crate) fn infer_query_time_in_force(order: &Order) -> TimeInForce {
    if let Some(time_in_force) = hyperliquid_time_in_force(order.venue, order.order_type) {
        return time_in_force;
    }
    if !order.order_type.is_limit() {
        return TimeInForce::GTC;
    }
    match order.venue {
        TradingVenue::BinanceFutures
        | TradingVenue::BinanceCoinFutures
        | TradingVenue::BybitMargin
        | TradingVenue::BybitFutures
        | TradingVenue::OkexMargin
        | TradingVenue::OkexFutures
        | TradingVenue::GateMargin
        | TradingVenue::GateFutures
        | TradingVenue::BitgetFutures
        | TradingVenue::BitgetCoinFutures => TimeInForce::GTX,
        _ => TimeInForce::GTC,
    }
}

pub(crate) fn order_query_time_utc(order: &Order, fallback_us: i64) -> String {
    let ts = [
        order.timestamp.create_t,
        order.timestamp.local_t,
        order.timestamp.submit_t,
    ]
    .into_iter()
    .find(|ts| *ts > 0)
    .unwrap_or(fallback_us);
    format_epoch_utc(ts)
}

pub(crate) fn format_orphan_query_table(rows: &[(i64, String)]) -> String {
    let id_width = rows
        .iter()
        .map(|(id, _)| id.to_string().len())
        .max()
        .unwrap_or(0)
        .max("id".len());
    let time_width = rows
        .iter()
        .map(|(_, time)| time.len())
        .max()
        .unwrap_or(0)
        .max("time_utc".len());
    let rule = format!(
        "{:-<id_width$}  {:-<time_width$}",
        "",
        "",
        id_width = id_width,
        time_width = time_width
    );
    let mut table = format!(
        "{}\n{:id_width$}  {:time_width$}\n{}\n",
        rule,
        "id",
        "time_utc",
        rule,
        id_width = id_width,
        time_width = time_width
    );
    for (client_order_id, time_utc) in rows {
        table.push_str(&format!(
            "{:id_width$}  {:time_width$}\n",
            client_order_id,
            time_utc,
            id_width = id_width,
            time_width = time_width
        ));
    }
    table.push_str(&rule);
    table
}

fn format_epoch_utc(ts: i64) -> String {
    let ts_us = normalize_epoch_to_us(ts);
    let secs = ts_us.div_euclid(1_000_000);
    let nanos = ts_us.rem_euclid(1_000_000) as u32 * 1_000;
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos)
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true))
        .unwrap_or_else(|| ts.to_string())
}

fn normalize_epoch_to_us(ts: i64) -> i64 {
    let abs_ts = ts.unsigned_abs();
    if abs_ts >= 1_000_000_000_000_000 {
        ts
    } else if abs_ts >= 1_000_000_000_000 {
        ts.saturating_mul(1_000)
    } else if abs_ts >= 1_000_000_000 {
        ts.saturating_mul(1_000_000)
    } else {
        ts
    }
}

#[cfg(test)]
mod tests {
    use super::{
        commit_query_policy_for, format_orphan_query_table,
        hyperliquid_not_found_terminal_barrier_us, infer_query_time_in_force,
        orphan_initial_query_ticks_for, CommitQueryAction, OrphanOrderOwner, OrphanOrderTracker,
        OrphanQueryState, BINANCE_PM_COMMIT_QUERY_BASE_TICKS, BINANCE_PM_COMMIT_QUERY_MAX_ATTEMPTS,
        BINANCE_PM_ORPHAN_INITIAL_QUERY_TICKS, COMMIT_QUERY_BASE_TICKS, COMMIT_QUERY_MAX_ATTEMPTS,
        EXEC_COMMIT_NOT_FOUND_GRACE_US,
    };
    use crate::strategy::manager::{OrphanSourceKind, OrphanStrategyRole};
    use crate::strategy::uniform_order_helper::UniformPublishCtx;
    use order_common::{Order, OrderType, Side, TimeInForce, TradingVenue};
    use signal_common::hyperliquid::{
        HYPERLIQUID_ACTION_COMMIT_CLOCK_MARGIN_MS, MAX_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS,
    };

    fn hyperliquid_test_order(venue: TradingVenue, order_type: OrderType) -> Order {
        Order::new(
            venue,
            42,
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
    fn hyperliquid_orphan_tif_matches_original_order_intent() {
        for venue in [
            TradingVenue::HyperliquidMargin,
            TradingVenue::HyperliquidFutures,
        ] {
            assert_eq!(
                infer_query_time_in_force(&hyperliquid_test_order(venue, OrderType::Limit)),
                TimeInForce::GTX
            );
            assert_eq!(
                infer_query_time_in_force(&hyperliquid_test_order(venue, OrderType::Market)),
                TimeInForce::IOC
            );
        }
    }

    #[test]
    fn orphan_query_table_uses_three_lines() {
        let rows = vec![
            (42_i64, "2026-05-07T02:31:38.000000Z".to_string()),
            (7_i64, "2026-05-07T02:32:00.123456Z".to_string()),
        ];

        let table = format_orphan_query_table(&rows);

        let lines: Vec<&str> = table.lines().collect();
        assert_eq!(lines.len(), 6);
        assert!(lines[0].chars().all(|c| c == '-' || c == ' '));
        assert!(lines[2].chars().all(|c| c == '-' || c == ' '));
        assert!(lines[5].chars().all(|c| c == '-' || c == ' '));
        assert!(lines[1].contains("id"));
        assert!(lines[1].contains("time_utc"));
    }

    #[test]
    fn commit_query_budget_uses_x4_backoff_from_one_second() {
        let client_order_id = 42;
        let mut tracker =
            OrphanOrderTracker::new(COMMIT_QUERY_BASE_TICKS, COMMIT_QUERY_BASE_TICKS, 3_200);
        tracker.track_order_id(client_order_id);

        let expected_waits = [
            COMMIT_QUERY_BASE_TICKS,
            COMMIT_QUERY_BASE_TICKS * 4,
            COMMIT_QUERY_BASE_TICKS * 16,
        ];
        for (expected_query_count, wait_ticks) in
            (1..=COMMIT_QUERY_MAX_ATTEMPTS).zip(expected_waits)
        {
            for _ in 0..wait_ticks {
                assert_eq!(tracker.commit_query_due_now(client_order_id), None);
            }
            assert_eq!(
                tracker.commit_query_due_now(client_order_id),
                Some(CommitQueryAction::Query {
                    query_count: expected_query_count,
                    budget_exhausted: false,
                })
            );
        }

        for _ in 0..COMMIT_QUERY_BASE_TICKS * 64 {
            assert_eq!(tracker.commit_query_due_now(client_order_id), None);
        }
        assert_eq!(
            tracker.commit_query_due_now(client_order_id),
            Some(CommitQueryAction::Close)
        );
    }

    #[test]
    fn exec_commit_order_keeps_querying_after_budget_instead_of_closing() {
        let client_order_id = 43;
        let mut tracker =
            OrphanOrderTracker::new(COMMIT_QUERY_BASE_TICKS, COMMIT_QUERY_BASE_TICKS, 3_200);
        tracker.adopt_order_owner(
            client_order_id,
            OrphanOrderOwner {
                source_strategy_id: 7,
                source_kind: OrphanSourceKind::Hedge,
                source_role: OrphanStrategyRole::Exec,
                uniform_ctx: UniformPublishCtx {
                    signal_ts: 1,
                    signal_bbo: None,
                    from_key: b"cta_alpha".to_vec(),
                    price_offset: 0.0,
                },
            },
        );

        let waits = [
            COMMIT_QUERY_BASE_TICKS,
            COMMIT_QUERY_BASE_TICKS * 4,
            COMMIT_QUERY_BASE_TICKS * 16,
        ];
        for (expected_query_count, wait_ticks) in (1..=COMMIT_QUERY_MAX_ATTEMPTS).zip(waits) {
            for _ in 0..wait_ticks {
                assert_eq!(tracker.commit_query_due_now(client_order_id), None);
            }
            assert_eq!(
                tracker.commit_query_due_now(client_order_id),
                Some(CommitQueryAction::Query {
                    query_count: expected_query_count,
                    budget_exhausted: false,
                })
            );
        }

        for _ in 0..COMMIT_QUERY_BASE_TICKS * 64 {
            assert_eq!(tracker.commit_query_due_now(client_order_id), None);
        }
        assert_eq!(
            tracker.commit_query_due_now(client_order_id),
            Some(CommitQueryAction::Query {
                query_count: COMMIT_QUERY_MAX_ATTEMPTS + 1,
                budget_exhausted: true,
            })
        );
        tracker.close_commit_order_after_query_budget("test", 1, client_order_id);
        assert!(tracker.contains(client_order_id));
    }

    #[test]
    fn exec_not_found_requires_confirmations_and_grace() {
        let first_not_found_at_us = 1_000_000;
        let mut state = OrphanQueryState {
            query_count: 2,
            ticks_until_next_query: 0,
            consecutive_not_found: 0,
            first_not_found_at_us: 0,
        };

        state.record_not_found(first_not_found_at_us);
        state.record_not_found(first_not_found_at_us + 1_000_000);
        assert!(!state.has_confirmed_not_found(
            COMMIT_QUERY_MAX_ATTEMPTS,
            first_not_found_at_us + EXEC_COMMIT_NOT_FOUND_GRACE_US,
            i64::MIN,
        ));

        state.record_not_found(first_not_found_at_us + 2_000_000);
        assert!(!state.has_confirmed_not_found(
            COMMIT_QUERY_MAX_ATTEMPTS,
            first_not_found_at_us + EXEC_COMMIT_NOT_FOUND_GRACE_US,
            i64::MIN,
        ));

        state.query_count = COMMIT_QUERY_MAX_ATTEMPTS;
        assert!(!state.has_confirmed_not_found(
            COMMIT_QUERY_MAX_ATTEMPTS,
            first_not_found_at_us + EXEC_COMMIT_NOT_FOUND_GRACE_US - 1,
            i64::MIN,
        ));
        assert!(state.has_confirmed_not_found(
            COMMIT_QUERY_MAX_ATTEMPTS,
            first_not_found_at_us + EXEC_COMMIT_NOT_FOUND_GRACE_US,
            i64::MIN,
        ));
    }

    #[test]
    fn hyperliquid_not_found_waits_past_signed_expiry_and_commit_margin() {
        let create_time_us = 1_000_000;
        let terminal_not_before_us = hyperliquid_not_found_terminal_barrier_us(
            create_time_us,
            MAX_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS,
        )
        .unwrap();
        assert_eq!(
            terminal_not_before_us,
            create_time_us
                + (MAX_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS
                    + HYPERLIQUID_ACTION_COMMIT_CLOCK_MARGIN_MS) as i64
                    * 1_000
        );

        let mut state = OrphanQueryState {
            query_count: COMMIT_QUERY_MAX_ATTEMPTS,
            ticks_until_next_query: 0,
            consecutive_not_found: 0,
            first_not_found_at_us: 0,
        };
        state.record_not_found(create_time_us + 1_000_000);
        state.record_not_found(create_time_us + 2_000_000);
        state.record_not_found(create_time_us + 3_000_000);

        assert!(!state.has_confirmed_not_found(
            COMMIT_QUERY_MAX_ATTEMPTS,
            terminal_not_before_us - 1,
            terminal_not_before_us,
        ));
        assert!(state.has_confirmed_not_found(
            COMMIT_QUERY_MAX_ATTEMPTS,
            terminal_not_before_us,
            terminal_not_before_us,
        ));
        assert!(hyperliquid_not_found_terminal_barrier_us(
            0,
            MAX_HYPERLIQUID_ACTION_EXPIRES_AFTER_MS
        )
        .is_none());
    }

    #[test]
    fn non_not_found_response_resets_exec_convergence_evidence() {
        let mut state = OrphanQueryState {
            query_count: 3,
            ticks_until_next_query: 0,
            consecutive_not_found: COMMIT_QUERY_MAX_ATTEMPTS,
            first_not_found_at_us: 1_000_000,
        };

        state.reset_not_found();

        assert_eq!(state.consecutive_not_found, 0);
        assert_eq!(state.first_not_found_at_us, 0);
        assert!(!state.has_confirmed_not_found(
            COMMIT_QUERY_MAX_ATTEMPTS,
            1_000_000 + EXEC_COMMIT_NOT_FOUND_GRACE_US,
            i64::MIN,
        ));
    }

    #[test]
    fn non_commit_query_uses_exponential_backoff() {
        let client_order_id = 7;
        let mut tracker = OrphanOrderTracker::new(25, 25, 3_200);
        tracker.track_order_id(client_order_id);

        for _ in 0..25 {
            assert!(!tracker.query_due_now(client_order_id));
        }
        assert!(tracker.query_due_now(client_order_id));
        assert_eq!(tracker.query_count(client_order_id), Some(1));

        for _ in 0..50 {
            assert!(!tracker.query_due_now(client_order_id));
        }
        assert!(tracker.query_due_now(client_order_id));
        assert_eq!(tracker.query_count(client_order_id), Some(2));
    }

    #[test]
    fn binance_pm_orphan_initial_query_starts_at_two_seconds() {
        assert_eq!(
            orphan_initial_query_ticks_for(TradingVenue::BinanceFutures, false, 25),
            BINANCE_PM_ORPHAN_INITIAL_QUERY_TICKS
        );
        assert_eq!(
            orphan_initial_query_ticks_for(TradingVenue::BinanceMargin, false, 25),
            BINANCE_PM_ORPHAN_INITIAL_QUERY_TICKS
        );
        assert_eq!(
            orphan_initial_query_ticks_for(TradingVenue::BinanceFutures, true, 25),
            25
        );
        assert_eq!(
            orphan_initial_query_ticks_for(TradingVenue::GateFutures, false, 25),
            25
        );
    }

    #[test]
    fn binance_pm_commit_query_uses_longer_budget() {
        let pm_policy = commit_query_policy_for(TradingVenue::BinanceFutures, false);
        assert_eq!(pm_policy.base_ticks, BINANCE_PM_COMMIT_QUERY_BASE_TICKS);
        assert_eq!(pm_policy.max_attempts, BINANCE_PM_COMMIT_QUERY_MAX_ATTEMPTS);
        assert_eq!(pm_policy.base_ticks, 500);
        assert_eq!(pm_policy.max_attempts, 6);

        let standard_policy = commit_query_policy_for(TradingVenue::BinanceFutures, true);
        assert_eq!(standard_policy.base_ticks, COMMIT_QUERY_BASE_TICKS);
        assert_eq!(standard_policy.max_attempts, COMMIT_QUERY_MAX_ATTEMPTS);

        let non_binance_policy = commit_query_policy_for(TradingVenue::GateFutures, false);
        assert_eq!(non_binance_policy.base_ticks, COMMIT_QUERY_BASE_TICKS);
        assert_eq!(non_binance_policy.max_attempts, COMMIT_QUERY_MAX_ATTEMPTS);
    }
}
