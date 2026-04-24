use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::{PersistChannel, TradeEngHub};
use crate::signal::common::{ExecutionType, OrderStatus, TradingVenue};
use crate::signal::trade_signal::TradeSignal;
use crate::strategy::manager::{ForceCloseControl, Strategy};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::query_engine_response::QueryEngineResponse;
use crate::strategy::trade_engine_response::TradeEngineResponse;
use crate::strategy::trade_update::TradeUpdate;
use log::{debug, info, warn};
use std::any::Any;
use std::collections::HashSet;

pub struct MmOrphanOrderStrategy {
    strategy_id: i32,
    symbol: String,
    order_ids: HashSet<i64>,
    active: bool,
}

impl MmOrphanOrderStrategy {
    pub fn new(strategy_id: i32, symbol: impl Into<String>) -> Self {
        Self {
            strategy_id,
            symbol: normalize_symbol_for_internal(&symbol.into()),
            order_ids: HashSet::new(),
            active: true,
        }
    }

    pub fn len(&self) -> usize {
        self.order_ids.len()
    }

    pub fn should_adopt_order_update(update: &dyn OrderUpdate) -> bool {
        let monitor = MonitorChannel::instance();
        let venue = update.trading_venue();
        (venue == monitor.open_venue() || venue == monitor.hedge_venue())
            && update.client_order_id() > 0
            && !matches!(update.execution_type(), ExecutionType::Trade)
    }

    pub fn should_adopt_trade_update(trade: &dyn TradeUpdate) -> bool {
        let monitor = MonitorChannel::instance();
        let venue = trade.trading_venue();
        (venue == monitor.open_venue() || venue == monitor.hedge_venue())
            && trade.client_order_id() > 0
    }

    fn adopt_order_id_inner(&mut self, client_order_id: i64, reason: &str) -> bool {
        if client_order_id <= 0 {
            return false;
        }
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return false;
        };
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} adopt missing local order client_order_id={} reason={}",
                self.strategy_id, client_order_id, reason
            );
            return false;
        };
        let symbol = normalize_symbol_for_internal(&order.symbol);
        let venue = order.venue;
        if symbol != self.symbol {
            return false;
        }
        drop(order);
        self.remember_order(client_order_id);
        info!(
            "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} adopted order_id symbol={} client_order_id={} venue={:?} reason={}",
            self.strategy_id, symbol, client_order_id, venue, reason
        );
        true
    }

    pub fn forget_order_id(&mut self, client_order_id: i64, reason: &str) -> bool {
        let removed = self.order_ids.remove(&client_order_id);
        if removed {
            info!(
                "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} forgot order_id client_order_id={} reason={}",
                self.strategy_id, client_order_id, reason
            );
        }
        removed
    }

    fn remember_order(&mut self, client_order_id: i64) {
        self.order_ids.insert(client_order_id);
    }

    fn request_cancel_if_needed(&mut self, update: &dyn OrderUpdate) {
        if update.execution_type() == ExecutionType::Trade {
            return;
        }
        if !matches!(
            update.status(),
            OrderStatus::New | OrderStatus::PartiallyFilled
        ) {
            return;
        }

        let venue = update.trading_venue();
        if !matches!(
            venue,
            TradingVenue::BinanceMargin | TradingVenue::BinanceFutures
        ) {
            debug!(
                "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} skip cancel unsupported venue={:?} symbol={} client_order_id={}",
                self.strategy_id,
                venue,
                update.symbol(),
                update.client_order_id()
            );
            return;
        }

        let client_order_id = update.client_order_id();
        let symbol = normalize_symbol_for_internal(update.symbol());
        let cancel_bytes = {
            let order_mgr = MonitorChannel::instance().order_manager();
            let mgr = order_mgr.borrow();
            match mgr.build_unmatched_cancel_bytes(venue, &symbol, client_order_id) {
                Ok(bytes) => bytes,
                Err(err) => {
                    warn!(
                        "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} failed to build cancel client_order_id={} symbol={} venue={:?}: {}",
                        self.strategy_id,
                        client_order_id,
                        symbol,
                        venue,
                        err
                    );
                    return;
                }
            }
        };

        let exchange = venue.trade_engine_exchange();
        match TradeEngHub::publish_order_request(exchange, &cancel_bytes) {
            Ok(()) => {
                warn!(
                    "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} sent cancel client_order_id={} order_id={} symbol={} venue={:?} x={:?} X={:?}",
                    self.strategy_id,
                    client_order_id,
                    update.order_id(),
                    symbol,
                    venue,
                    update.execution_type(),
                    update.status()
                );
            }
            Err(err) => warn!(
                "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} failed to send cancel client_order_id={} order_id={} symbol={} venue={:?}: {:#}",
                self.strategy_id,
                client_order_id,
                update.order_id(),
                symbol,
                venue,
                err
            ),
        }
    }

    fn persist_order_update(update: &dyn OrderUpdate) {
        PersistChannel::with(|ch| ch.publish_order_update_unmatched(update));
    }

    fn persist_trade_update(trade: &dyn TradeUpdate) {
        PersistChannel::with(|ch| ch.publish_trade_update_unmatched(trade));
    }
}

impl ForceCloseControl for MmOrphanOrderStrategy {
    fn set_force_close_mode(&mut self, _enabled: bool) {}

    fn is_force_close_mode(&self) -> bool {
        false
    }
}

impl Strategy for MmOrphanOrderStrategy {
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
        self.order_ids.contains(&order_id)
    }

    fn handle_signal(&mut self, _signal: &TradeSignal) {}

    fn apply_order_update(&mut self, update: &dyn OrderUpdate) {
        if !Self::should_adopt_order_update(update)
            || normalize_symbol_for_internal(update.symbol()) != self.symbol
        {
            return;
        }
        self.remember_order(update.client_order_id());
        self.request_cancel_if_needed(update);
        Self::persist_order_update(update);
        info!(
            "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} adopted order_update symbol={} client_order_id={} order_id={} venue={:?} x={:?} X={:?}",
            self.strategy_id,
            update.symbol(),
            update.client_order_id(),
            update.order_id(),
            update.trading_venue(),
            update.execution_type(),
            update.status()
        );
    }

    fn apply_trade_update(&mut self, trade: &dyn TradeUpdate) {
        if !Self::should_adopt_trade_update(trade)
            || normalize_symbol_for_internal(trade.symbol()) != self.symbol
        {
            return;
        }
        self.remember_order(trade.client_order_id());
        Self::persist_trade_update(trade);
        info!(
            "MmOrphanOrderStrategy: strategy_role=mm_orphan strategy_id={} adopted trade_update symbol={} client_order_id={} order_id={} venue={:?} cumulative_qty={:.8} status={:?}",
            self.strategy_id,
            trade.symbol(),
            trade.client_order_id(),
            trade.order_id(),
            trade.trading_venue(),
            trade.cumulative_filled_quantity(),
            trade.order_status()
        );
    }

    fn apply_trade_engine_response(&mut self, _response: &dyn TradeEngineResponse) {}

    fn apply_query_engine_response(&mut self, _response: &dyn QueryEngineResponse) {}

    fn adopt_order_id(&mut self, client_order_id: i64, reason: &str) -> bool {
        self.adopt_order_id_inner(client_order_id, reason)
    }

    fn handle_period_clock(&mut self, _current_tp: i64) {}

    fn is_active(&self) -> bool {
        self.active
    }

    fn symbol(&self) -> Option<&str> {
        Some(&self.symbol)
    }
}
