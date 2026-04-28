use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::signal::common::TradingVenue;
use crate::signal::trade_signal::TradeSignal;
use crate::strategy::manager::{OrderTerminalRecorder, Strategy};
use crate::strategy::net_qty_queue::{NetQtyQueue, TimedNetQtyQueue};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;
use log::debug;
use std::any::Any;

/// Arb 对冲策略的只读状态快照。
///
/// 调用方可以通过它观察双 venue 合并后的净敞口、待对冲数量，以及各队列的批次数量。
#[derive(Debug, Clone)]
pub struct ArbHedgeSnapshot {
    pub symbol: String,
    pub open_venue: TradingVenue,
    pub hedge_venue: TradingVenue,
    pub net_qty: f64,
    pub pending_hedge_qty: f64,
    pub due_hedge_qty: f64,
    pub net_lot_count: usize,
    pub pending_hedge_lot_count: usize,
}

/// Arb 对冲状态策略。
///
/// 这一阶段只维护记录接口和队列状态，不负责生成对冲订单。
pub struct ArbHedgeStrategy {
    pub(super) strategy_id: i32,
    pub(super) symbol: String,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    /// open/hedge 两个 venue 合并后的实时净敞口，使用 base qty 口径互相冲销。
    pub(super) net_qty_queue: NetQtyQueue,
    /// 尚需由对冲腿覆盖的开仓成交队列，到期时间来自开仓记录的 close_ts。
    pub(super) pending_hedge_queue: TimedNetQtyQueue,
    alive_flag: bool,
}

impl ArbHedgeStrategy {
    pub fn new(
        strategy_id: i32,
        symbol: impl Into<String>,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Self {
        Self {
            strategy_id,
            symbol: normalize_symbol_for_internal(&symbol.into()),
            open_venue,
            hedge_venue,
            net_qty_queue: NetQtyQueue::new(),
            pending_hedge_queue: TimedNetQtyQueue::new(),
            alive_flag: true,
        }
    }

    pub fn snapshot(&self, now_ts: i64) -> ArbHedgeSnapshot {
        ArbHedgeSnapshot {
            symbol: self.symbol.clone(),
            open_venue: self.open_venue,
            hedge_venue: self.hedge_venue,
            net_qty: self.net_qty_queue.net_qty(),
            pending_hedge_qty: self.pending_hedge_queue.net_qty(),
            due_hedge_qty: self.pending_hedge_queue.due_qty(now_ts),
            net_lot_count: self.net_qty_queue.len(),
            pending_hedge_lot_count: self.pending_hedge_queue.len(),
        }
    }

    pub fn net_qty(&self) -> f64 {
        self.net_qty_queue.net_qty()
    }

    // pending_hedge_qty 包含了所有开仓成交形成的对冲需求，无论是否已到期；
    pub fn pending_hedge_qty(&self) -> f64 {
        self.pending_hedge_queue.net_qty()
    }
    // due_hedge_qty 则只计算已到期的部分。
    pub fn due_hedge_qty(&self, now_ts: i64) -> f64 {
        self.pending_hedge_queue.due_qty(now_ts)
    }
}

impl Strategy for ArbHedgeStrategy {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn get_id(&self) -> i32 {
        self.strategy_id
    }

    fn is_strategy_order(&self, _order_id: i64) -> bool {
        false
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        debug!(
            "ArbHedgeStrategy: strategy_id={} ignore signal {:?}",
            self.strategy_id, signal.signal_type
        );
    }

    fn apply_order_update(&mut self, _update: &dyn OrderUpdate) {}

    fn apply_trade_update(&mut self, _trade: &dyn TradeUpdate) {}

    fn handle_period_clock(&mut self, _current_tp: i64) {}

    fn is_active(&self) -> bool {
        self.alive_flag
    }

    fn symbol(&self) -> Option<&str> {
        Some(&self.symbol)
    }

    fn has_order_terminal_recorder(&self) -> bool {
        true
    }

    fn order_terminal_recorder_mut(&mut self) -> Option<&mut dyn OrderTerminalRecorder> {
        Some(self)
    }
}

#[cfg(test)]
mod tests {
    use super::ArbHedgeStrategy;
    use crate::signal::common::TradingVenue;
    use crate::strategy::manager::OrderTerminalRecorder;

    #[test]
    fn open_fill_records_net_and_pending_hedge() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "BTCUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        );

        strategy.record_open_order_terminal(10, 2.0, 100.0, 1_000);

        assert_eq!(strategy.net_qty(), 2.0);
        assert_eq!(strategy.pending_hedge_qty(), 2.0);
        assert_eq!(strategy.due_hedge_qty(999), 0.0);
        assert_eq!(strategy.due_hedge_qty(1_000), 2.0);
    }

    #[test]
    fn hedge_fill_offsets_pending_and_base_net() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "BTCUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        );

        strategy.record_open_order_terminal(10, 2.0, 100.0, 1_000);
        strategy.record_hedge_order_terminal(20, -1.25, 101.0);

        assert_eq!(strategy.net_qty(), 0.75);
        assert_eq!(strategy.pending_hedge_qty(), 2.0);
    }
}
