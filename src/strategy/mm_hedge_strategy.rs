use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::signal_channel::SignalChannel;
use crate::pre_trade::PersistChannel;
use crate::signal::common::SignalBytes;
use crate::signal::hedge_signal::{MmHedgeCtx, MmHedgeSignalQueryMsg};
use crate::signal::record::SignalRecordMessage;
use crate::signal::trade_signal::{SignalType, TradeSignal};
use crate::strategy::manager::{ForceCloseControl, Strategy};
use crate::strategy::order_update::OrderUpdate;
use crate::strategy::query_engine_response::QueryEngineResponse;
use crate::strategy::trade_engine_response::TradeEngineResponse;
use crate::strategy::trade_update::TradeUpdate;
use log::{debug, info, warn};
use std::any::Any;

const HEDGE_QUERY_INTERVAL_US: i64 = 30_000_000;
const HEDGE_QUERY_WATCHDOG_US: i64 = 30_000;

#[derive(Debug, Clone)]
pub struct MmHedgeSnapshot {
    pub symbol: String,
    pub net_qty: f64,
    pub buy_qty: f64,
    pub sell_qty: f64,
}

/// 做市对冲策略（每个 symbol 仅一个实例）
pub struct MarketMakerHedgeStrategy {
    strategy_id: i32,
    symbol: String,
    net_qty: f64,
    period_buy_qty: f64,
    period_sell_qty: f64,
    signal_ts: i64,
    alive_flag: bool,
    next_query_ts_us: i64,
    pending_query: bool,
    query_watchdog_due_ts: i64,
}

impl MarketMakerHedgeStrategy {
    pub fn new(strategy_id: i32, symbol: String) -> Self {
        let now = get_timestamp_us();
        Self {
            strategy_id,
            symbol,
            net_qty: 0.0,
            period_buy_qty: 0.0,
            period_sell_qty: 0.0,
            signal_ts: 0,
            alive_flag: true,
            next_query_ts_us: now.saturating_add(HEDGE_QUERY_INTERVAL_US),
            pending_query: false,
            query_watchdog_due_ts: 0,
        }
    }

    /// 累加成交（使用 base qty 口径）
    pub fn record_fill(&mut self, signed_qty: f64, buy_qty: f64, sell_qty: f64) {
        self.net_qty += signed_qty;
        self.period_buy_qty += buy_qty;
        self.period_sell_qty += sell_qty;
    }

    pub fn snapshot(&self) -> MmHedgeSnapshot {
        MmHedgeSnapshot {
            symbol: self.symbol.clone(),
            net_qty: self.net_qty,
            buy_qty: self.period_buy_qty,
            sell_qty: self.period_sell_qty,
        }
    }

    fn handle_mm_hedge_signal(&mut self, ctx: MmHedgeCtx) {
        self.signal_ts = ctx.signal_ts;
        self.next_query_ts_us = ctx.signal_ts.saturating_add(HEDGE_QUERY_INTERVAL_US);
        self.pending_query = false;
        self.query_watchdog_due_ts = 0;

        // 打印所有 MM 对冲策略的累积头寸（净头寸 + 买/卖累计）
        let strategy_mgr = MonitorChannel::instance().strategy_mgr();
        let snapshots = strategy_mgr.borrow().mm_hedge_snapshots();
        for snap in snapshots {
            info!(
                "MMHedge snapshot: symbol={} net={:.8} buy={:.8} sell={:.8}",
                snap.symbol, snap.net_qty, snap.buy_qty, snap.sell_qty
            );
        }
        let from_key = String::from_utf8_lossy(&ctx.from_key).to_string();
        info!(
            "MMHedge ctx: symbol={} offsets_len={} from_key='{}'",
            ctx.get_opening_symbol(),
            ctx.price_offsets.len(),
            from_key
        );

        // 清空期间累计多空成交
        self.period_buy_qty = 0.0;
        self.period_sell_qty = 0.0;
    }

    fn handle_query_timer(&mut self) {
        if self.next_query_ts_us <= 0 {
            return;
        }

        let now = get_timestamp_us();
        if now < self.next_query_ts_us {
            return;
        }

        self.send_hedge_query();
        self.next_query_ts_us = 0;
    }

    fn handle_query_watchdog(&mut self) {
        if !self.pending_query {
            return;
        }
        let now = get_timestamp_us();
        if now < self.query_watchdog_due_ts {
            return;
        }
        warn!(
            "MarketMakerHedgeStrategy: strategy_id={} symbol={} query watchdog timeout, retry send",
            self.strategy_id, self.symbol
        );
        self.send_hedge_query();
    }

    fn send_hedge_query(&mut self) {
        // 定时发送对冲查询（只携带 symbol + 期间累计买/卖成交）
        let query_msg = MmHedgeSignalQueryMsg::new(
            &self.symbol,
            self.period_buy_qty,
            self.period_sell_qty,
        );
        let send_result = SignalChannel::with(|ch| ch.publish_backward(&query_msg.to_bytes()));
        match send_result {
            Ok(true) => {
                debug!(
                    "MarketMakerHedgeStrategy: strategy_id={} send hedge query ok symbol={}",
                    self.strategy_id, self.symbol
                );
                let now = get_timestamp_us();
                self.pending_query = true;
                self.query_watchdog_due_ts = now.saturating_add(HEDGE_QUERY_WATCHDOG_US);
            }
            Ok(false) => {
                warn!(
                    "MarketMakerHedgeStrategy: backward publisher 未配置，无法发送对冲查询 symbol={}",
                    self.symbol
                );
                self.pending_query = false;
                self.query_watchdog_due_ts = 0;
            }
            Err(err) => {
                warn!(
                    "MarketMakerHedgeStrategy: 发送对冲查询失败 symbol={} err={:#}",
                    self.symbol, err
                );
                self.pending_query = false;
                self.query_watchdog_due_ts = 0;
            }
        }
    }

    fn handle_signal(&mut self, signal: &TradeSignal) {
        match &signal.signal_type {
            SignalType::MMHedge => match MmHedgeCtx::from_bytes(signal.context.clone()) {
                Ok(ctx) => self.handle_mm_hedge_signal(ctx),
                Err(err) => {
                    warn!(
                        "MarketMakerHedgeStrategy: strategy_id={} decode MMHedge failed: {}",
                        self.strategy_id, err
                    );
                }
            },
            _ => {
                debug!(
                    "MarketMakerHedgeStrategy: strategy_id={} ignore signal {:?}",
                    self.strategy_id, signal.signal_type
                );
            }
        }
    }
}

impl ForceCloseControl for MarketMakerHedgeStrategy {
    fn set_force_close_mode(&mut self, _enabled: bool) {}

    fn is_force_close_mode(&self) -> bool {
        false
    }
}

impl Strategy for MarketMakerHedgeStrategy {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn get_id(&self) -> i32 {
        self.strategy_id
    }

    fn symbol(&self) -> Option<&str> {
        Some(&self.symbol)
    }

    fn is_strategy_order(&self, _order_id: i64) -> bool {
        false
    }

    fn handle_signal_with_record(&mut self, signal: &TradeSignal) {
        self.handle_signal(signal);

        let record = SignalRecordMessage::new(
            self.strategy_id,
            signal.signal_type.clone(),
            signal.context.clone().to_vec(),
            signal.generation_time,
        );
        PersistChannel::with(|ch| ch.publish_signal_record(&record));
    }

    fn apply_order_update_with_record(&mut self, _update: &dyn OrderUpdate) {}

    fn apply_trade_update_with_record(&mut self, _trade: &dyn TradeUpdate) {}

    fn apply_trade_engine_response(&mut self, _response: &dyn TradeEngineResponse) {}

    fn apply_query_engine_response(&mut self, _response: &dyn QueryEngineResponse) {}

    fn handle_period_clock(&mut self, _current_tp: i64) {
        if self.is_active() {
            self.handle_query_timer();
            self.handle_query_watchdog();
        }
    }

    fn is_active(&self) -> bool {
        self.alive_flag
    }
}
