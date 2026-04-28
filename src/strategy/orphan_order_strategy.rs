use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::signal::trade_signal::TradeSignal;
use crate::strategy::manager::{OrphanHandoff, Strategy};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::orphan_order_common::{OrphanOrderOwner, OrphanOrderTracker};
use crate::strategy::trade_update::TradeUpdate;
use log::{info, warn};
use std::any::Any;

const ORPHAN_QUERY_BASE_TICKS: u32 = 25;
const ORPHAN_QUERY_MAX_TICKS: u32 = 3_200;
const ORPHAN_ROLE: &str = "OrphanOrderStrategy";

#[derive(Debug, Clone)]
pub struct OrphanOrderSnapshot {
    pub symbol: String,
    pub tracked_orders: usize,
}

pub struct OrphanOrderStrategy {
    strategy_id: i32,
    symbol: String,
    orders: OrphanOrderTracker,
    active: bool,
}

impl OrphanOrderStrategy {
    pub fn new(strategy_id: i32, symbol: impl Into<String>) -> Self {
        Self {
            strategy_id,
            symbol: normalize_symbol_for_internal(&symbol.into()),
            orders: OrphanOrderTracker::new(
                ORPHAN_QUERY_BASE_TICKS,
                ORPHAN_QUERY_BASE_TICKS,
                ORPHAN_QUERY_MAX_TICKS,
            ),
            active: true,
        }
    }

    pub(crate) fn adopt_orphan_order_id(&mut self, handoff: &OrphanHandoff) -> bool {
        if handoff.client_order_id <= 0 {
            return false;
        }
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return false;
        };
        let Some(order) = order_mgr.borrow().get(handoff.client_order_id) else {
            warn!(
                "{}: strategy_id={} adopt missing local order client_order_id={} reason={}",
                ORPHAN_ROLE, self.strategy_id, handoff.client_order_id, handoff.reason
            );
            return false;
        };
        let symbol = normalize_symbol_for_internal(&order.symbol);
        let venue = order.venue;
        let status = order.status;
        if symbol != self.symbol {
            return false;
        }
        drop(order);

        self.orders.adopt_order_owner(
            handoff.client_order_id,
            OrphanOrderOwner {
                source_strategy_id: handoff.source_strategy_id,
                source_kind: handoff.source_kind,
                uniform_ctx: handoff.uniform_ctx.clone(),
            },
        );
        info!(
            "{}: strategy_id={} adopted order symbol={} client_order_id={} venue={:?} status={:?} source_strategy_id={} source_kind={:?} reason={}",
            ORPHAN_ROLE,
            self.strategy_id,
            symbol,
            handoff.client_order_id,
            venue,
            status,
            handoff.source_strategy_id,
            handoff.source_kind,
            handoff.reason
        );
        true
    }

    pub fn snapshot(&self) -> OrphanOrderSnapshot {
        OrphanOrderSnapshot {
            symbol: self.symbol.clone(),
            tracked_orders: self.orders.len(),
        }
    }
}

impl Strategy for OrphanOrderStrategy {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn get_id(&self) -> i32 {
        self.strategy_id
    }

    fn is_strategy_order(&self, order_id: i64) -> bool {
        self.orders.contains(order_id)
    }

    fn handle_signal(&mut self, _signal: &TradeSignal) {}

    fn apply_order_update(&mut self, update: &dyn OrderUpdate) {
        if normalize_symbol_for_internal(update.symbol()) != self.symbol {
            return;
        }
        let _ = self
            .orders
            .apply_order_update(ORPHAN_ROLE, self.strategy_id, update);
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) {
        if normalize_symbol_for_internal(trade.symbol()) != self.symbol {
            return;
        }
        let _ = self
            .orders
            .apply_trade_update(ORPHAN_ROLE, self.strategy_id, trade);
    }

    fn handle_period_clock(&mut self, _current_tp: i64) {
        self.orders
            .handle_period_clock(ORPHAN_ROLE, self.strategy_id);
    }

    fn is_active(&self) -> bool {
        self.active
    }

    fn symbol(&self) -> Option<&str> {
        Some(&self.symbol)
    }
}
