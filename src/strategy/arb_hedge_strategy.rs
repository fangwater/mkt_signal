use crate::common::symbol_util::normalize_symbol_for_internal;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::params_load::PreTradeParamsLoader;
use crate::pre_trade::signal_channel::SignalChannel;
use crate::signal::arb_signal::ArbBackwardQueryMsg;
use crate::signal::common::TradingVenue;
use crate::signal::hedge_signal::ArbHedgeStateQueryMsg;
use crate::signal::trade_signal::TradeSignal;
use crate::strategy::hedge_strategy_common::HEDGE_QUERY_INTERVAL_US;
use crate::strategy::manager::{OrderTerminalRecorder, Strategy};
use crate::strategy::net_qty_queue::{NetQtyQueue, TimedNetQtyQueue};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::trade_update::TradeUpdate;
use log::{debug, warn};
use std::any::Any;

const ARB_HEDGE_QTY_EPS: f64 = 1e-12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArbHedgeMode {
    Trigger,
    Period,
}

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
    hedge_mode: ArbHedgeMode,
    hedge_request_seq: u64,
    next_query_ts_us: i64,
    alive_flag: bool,
}

impl ArbHedgeStrategy {
    pub fn new(
        strategy_id: i32,
        symbol: impl Into<String>,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Self {
        Self::new_with_mode(
            strategy_id,
            symbol,
            open_venue,
            hedge_venue,
            ArbHedgeMode::Trigger,
        )
    }

    pub fn new_with_mode(
        strategy_id: i32,
        symbol: impl Into<String>,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
        hedge_mode: ArbHedgeMode,
    ) -> Self {
        if hedge_mode == ArbHedgeMode::Period {
            panic!("ArbHedgeStrategy period hedge mode is not implemented");
        }
        Self {
            strategy_id,
            symbol: normalize_symbol_for_internal(&symbol.into()),
            open_venue,
            hedge_venue,
            net_qty_queue: NetQtyQueue::new(),
            pending_hedge_queue: TimedNetQtyQueue::new(),
            hedge_mode,
            hedge_request_seq: 0,
            next_query_ts_us: 0,
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

    fn next_hedge_request_seq(&mut self) -> u64 {
        self.hedge_request_seq = self.hedge_request_seq.wrapping_add(1);
        if self.hedge_request_seq == 0 {
            self.hedge_request_seq = 1;
        }
        self.hedge_request_seq
    }

    fn send_hedge_state_query(&mut self, now_ts: i64, due_hedge_qty: f64) {
        let risk_loader = PreTradeParamsLoader::instance();
        let symbol_exposure_u = risk_loader
            .max_pos_u_for_symbol(self.open_venue, &self.symbol)
            .max(0.0)
            * risk_loader.max_symbol_exposure_ratio().max(0.0);
        let request_seq = self.next_hedge_request_seq();
        let query_msg = ArbHedgeStateQueryMsg::new(
            self.strategy_id,
            &self.symbol,
            self.net_qty_queue.net_qty(),
            due_hedge_qty,
            self.pending_hedge_queue.net_qty(),
            symbol_exposure_u,
            self.net_qty_queue.weighted_avg_price().unwrap_or(0.0),
            request_seq,
        );
        let payload = ArbBackwardQueryMsg::HedgeState(query_msg).to_bytes();
        match SignalChannel::with(|ch| ch.publish_backward(&payload)) {
            Ok(true) => {
                self.next_query_ts_us = now_ts.saturating_add(HEDGE_QUERY_INTERVAL_US);
                debug!(
                    "ArbHedgeStrategy: strategy_id={} symbol={} send hedge state query ok request_seq={} net_qty={:.8} due_hedge_qty={:.8} pending_hedge_qty={:.8} next_query_ts_us={}",
                    self.strategy_id,
                    self.symbol,
                    request_seq,
                    self.net_qty_queue.net_qty(),
                    due_hedge_qty,
                    self.pending_hedge_queue.net_qty(),
                    self.next_query_ts_us
                );
            }
            Ok(false) => {
                warn!(
                    "ArbHedgeStrategy: backward publisher 未配置，无法发送对冲状态查询 strategy_id={} symbol={} request_seq={}",
                    self.strategy_id, self.symbol, request_seq
                );
            }
            Err(err) => {
                warn!(
                    "ArbHedgeStrategy: 发送对冲状态查询失败 strategy_id={} symbol={} request_seq={} err={:#}",
                    self.strategy_id, self.symbol, request_seq, err
                );
            }
        }
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

    fn handle_period_clock(&mut self, current_tp: i64) {
        match self.hedge_mode {
            ArbHedgeMode::Trigger => return,
            ArbHedgeMode::Period => {}
        }
        let now_ts = if current_tp > 0 {
            current_tp
        } else {
            get_timestamp_us()
        };
        if self.next_query_ts_us > 0 && now_ts < self.next_query_ts_us {
            return;
        }
        let due_hedge_qty = self.pending_hedge_queue.due_qty(now_ts);
        if due_hedge_qty.abs() <= ARB_HEDGE_QTY_EPS {
            return;
        }
        self.send_hedge_state_query(now_ts, due_hedge_qty);
    }

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
    use super::{ArbHedgeMode, ArbHedgeStrategy};
    use crate::pre_trade::order_manager::Side;
    use crate::signal::common::TradingVenue;
    use crate::strategy::manager::{OrderTerminalRecorder, Strategy};

    #[test]
    fn open_fill_records_net_and_pending_hedge() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "BTCUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        );

        strategy.record_open_order_terminal(10, Side::Buy, 2.0, 2.0, 100.0, 1_000);

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

        strategy.record_open_order_terminal(10, Side::Buy, 2.0, 2.0, 100.0, 1_000);
        let borrowed = strategy.pending_hedge_queue.borrow(1_000, 2.0);
        assert_eq!(borrowed.qv, 2.0);
        strategy.record_hedge_order_terminal(1_000, Side::Sell, 2.0, 1.25, 101.0);

        assert_eq!(strategy.net_qty(), 0.75);
        assert_eq!(strategy.pending_hedge_qty(), 0.75);
        assert_eq!(strategy.due_hedge_qty(1_000), 0.75);
    }

    #[test]
    fn trigger_mode_does_not_send_period_query() {
        let mut strategy = ArbHedgeStrategy::new(
            1,
            "BTCUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
        );

        strategy.record_open_order_terminal(10, Side::Buy, 2.0, 2.0, 100.0, 1_000);
        strategy.handle_period_clock(1_000);

        assert_eq!(strategy.pending_hedge_qty(), 2.0);
        assert_eq!(strategy.due_hedge_qty(1_000), 2.0);
        assert_eq!(strategy.hedge_request_seq, 0);
    }

    #[test]
    #[should_panic(expected = "ArbHedgeStrategy period hedge mode is not implemented")]
    fn period_mode_panics_until_implemented() {
        let _strategy = ArbHedgeStrategy::new_with_mode(
            1,
            "BTCUSDT",
            TradingVenue::BinanceMargin,
            TradingVenue::BinanceFutures,
            ArbHedgeMode::Period,
        );
    }
}
