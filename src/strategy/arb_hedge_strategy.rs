use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::signal::common::TradingVenue;
use crate::signal::trade_signal::TradeSignal;
use crate::strategy::manager::{ForceCloseControl, OrderTerminalRecorder, Strategy};
use crate::strategy::net_qty_queue::TimedNetQtyQueue;
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;
use log::{debug, info};
use std::any::Any;

const ARB_HEDGE_EPS: f64 = 1e-12;

/// Arb 对冲策略的只读状态快照。
///
/// 调用方可以通过它观察两条腿当前净敞口、待对冲数量，以及各队列的批次数量。
#[derive(Debug, Clone)]
pub struct ArbHedgeSnapshot {
    pub symbol: String,
    pub open_venue: TradingVenue,
    pub hedge_venue: TradingVenue,
    pub open_net_qty: f64,
    pub hedge_net_qty: f64,
    pub pending_hedge_qty: f64,
    pub due_hedge_qty: f64,
    pub open_net_lot_count: usize,
    pub hedge_net_lot_count: usize,
    pub pending_hedge_lot_count: usize,
}

/// Arb 对冲状态策略。
///
/// 这一阶段只维护记录接口和队列状态，不负责生成对冲订单。
pub struct ArbHedgeStrategy {
    strategy_id: i32,
    symbol: String,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    /// 开仓腿累计净敞口。买入记正，卖出记负。
    open_net_queue: TimedNetQtyQueue,
    /// 对冲腿累计净敞口。单独维护，便于核对真实对冲成交。
    hedge_net_queue: TimedNetQtyQueue,
    /// 尚需由对冲腿覆盖的开仓成交队列，到期时间来自开仓记录的 close_ts。
    pending_hedge_queue: TimedNetQtyQueue,
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
            open_net_queue: TimedNetQtyQueue::new(),
            hedge_net_queue: TimedNetQtyQueue::new(),
            pending_hedge_queue: TimedNetQtyQueue::new(),
            alive_flag: true,
        }
    }

    pub fn snapshot(&self, now_ts: i64) -> ArbHedgeSnapshot {
        ArbHedgeSnapshot {
            symbol: self.symbol.clone(),
            open_venue: self.open_venue,
            hedge_venue: self.hedge_venue,
            open_net_qty: self.open_net_queue.net_qty(),
            hedge_net_qty: self.hedge_net_queue.net_qty(),
            pending_hedge_qty: self.pending_hedge_queue.net_qty(),
            due_hedge_qty: self.pending_hedge_queue.due_qty(now_ts),
            open_net_lot_count: self.open_net_queue.len(),
            hedge_net_lot_count: self.hedge_net_queue.len(),
            pending_hedge_lot_count: self.pending_hedge_queue.len(),
        }
    }

    pub fn open_net_qty(&self) -> f64 {
        self.open_net_queue.net_qty()
    }

    pub fn hedge_net_qty(&self) -> f64 {
        self.hedge_net_queue.net_qty()
    }
    // pending_hedge_qty 包含了所有开仓成交形成的对冲需求，无论是否已到期；
    pub fn pending_hedge_qty(&self) -> f64 {
        self.pending_hedge_queue.net_qty()
    }
    // due_hedge_qty 则只计算已到期的部分。
    pub fn due_hedge_qty(&self, now_ts: i64) -> f64 {
        self.pending_hedge_queue.due_qty(now_ts)
    }

    fn apply_open_order_terminal(
        &mut self,
        fill_ts: i64,
        qv: f64,
        price: f64,
        close_ts: i64,
    ) -> bool {
        if qv.abs() <= ARB_HEDGE_EPS {
            return false;
        }
        self.open_net_queue.apply_fill(fill_ts, 0, qv, price);
        // 开仓订单 terminal 后的累计成交量形成待对冲需求，close_ts 决定这笔需求何时进入 due 数量。
        self.pending_hedge_queue
            .apply_fill(fill_ts, close_ts, qv, price);
        info!(
            "ArbHedgeRecord: strategy_id={} symbol={} leg=open qv={:.8} price={:.8} fill_ts={} close_ts={} open_net={:.8} pending_hedge={:.8}",
            self.strategy_id,
            self.symbol,
            qv,
            price,
            fill_ts,
            close_ts,
            self.open_net_queue.net_qty(),
            self.pending_hedge_queue.net_qty()
        );
        true
    }

    fn apply_hedge_order_terminal(&mut self, fill_ts: i64, qv: f64, price: f64) -> bool {
        if qv.abs() <= ARB_HEDGE_EPS {
            return false;
        }
        self.hedge_net_queue.apply_fill(fill_ts, 0, qv, price);
        // 对冲订单 terminal 后的累计成交量立即抵消待对冲队列；若方向相同，则会增加待处理的净需求。
        self.pending_hedge_queue.apply_fill(fill_ts, 0, qv, price);
        info!(
            "ArbHedgeRecord: strategy_id={} symbol={} leg=hedge qv={:.8} price={:.8} fill_ts={} hedge_net={:.8} pending_hedge={:.8}",
            self.strategy_id,
            self.symbol,
            qv,
            price,
            fill_ts,
            self.hedge_net_queue.net_qty(),
            self.pending_hedge_queue.net_qty()
        );
        true
    }
}

impl OrderTerminalRecorder for ArbHedgeStrategy {
    fn record_open_order_terminal(
        &mut self,
        fill_ts: i64,
        signed_base_qty: f64,
        price: f64,
        close_ts: i64,
    ) -> bool {
        self.apply_open_order_terminal(fill_ts, signed_base_qty, price, close_ts)
    }

    fn record_hedge_order_terminal(
        &mut self,
        fill_ts: i64,
        signed_base_qty: f64,
        price: f64,
    ) -> bool {
        self.apply_hedge_order_terminal(fill_ts, signed_base_qty, price)
    }
}

impl ForceCloseControl for ArbHedgeStrategy {
    fn set_force_close_mode(&mut self, _enabled: bool) {}

    fn is_force_close_mode(&self) -> bool {
        false
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
    fn open_fill_records_open_net_and_pending_hedge() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "BTCUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        );

        strategy.record_open_order_terminal(10, 2.0, 100.0, 1_000);

        assert_eq!(strategy.open_net_qty(), 2.0);
        assert_eq!(strategy.hedge_net_qty(), 0.0);
        assert_eq!(strategy.pending_hedge_qty(), 2.0);
        assert_eq!(strategy.due_hedge_qty(999), 0.0);
        assert_eq!(strategy.due_hedge_qty(1_000), 2.0);
    }

    #[test]
    fn hedge_fill_offsets_pending_but_keeps_separate_venue_net() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "BTCUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        );

        strategy.record_open_order_terminal(10, 2.0, 100.0, 1_000);
        strategy.record_hedge_order_terminal(20, -1.25, 101.0);

        assert_eq!(strategy.open_net_qty(), 2.0);
        assert_eq!(strategy.hedge_net_qty(), -1.25);
        assert_eq!(strategy.pending_hedge_qty(), 0.75);
    }
}
