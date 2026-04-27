use crate::common::tick_math::QuantizedValue;
use crate::common::time_util::get_timestamp_us;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::{OrderExecutionStatus, Side};
use crate::pre_trade::QueryEngHub;
use crate::signal::common::TradingVenue;
use crate::strategy::order_query_builder::build_order_query_request;
use crate::strategy::order_reconcile::ORDER_QUERY_WATCHDOG_DELAY_US;
use log::{debug, info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingOrderQueryReason {
    OrderWatchdog,
    CancelWatchdog,
    CancelRejected,
}

impl PendingOrderQueryReason {
    pub fn is_cancel_rejected(self) -> bool {
        matches!(self, Self::CancelRejected)
    }

    pub fn watchdog_hint(self) -> &'static str {
        if self.is_cancel_rejected() {
            "CancelRejectedWatchdog触发"
        } else {
            "CancelWatchdog触发"
        }
    }

    pub fn query_send_failed_trigger(self) -> &'static str {
        if self.is_cancel_rejected() {
            "cancel_rejected_query_send_failed"
        } else {
            "cancel_query_send_failed"
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryWatchdog {
    pub client_order_id: i64,
    pub due_ts_us: i64,
    pub reason: PendingOrderQueryReason,
}

#[derive(Debug, Clone, Default)]
pub struct OpenOrderState {
    pub open_order_id: i64,
    pub open_expire_ts: Option<i64>,
    pub open_side: Option<Side>,
    pub pending_order_query: Option<PendingOrderQueryReason>,
    pub order_query_watchdog: Option<QueryWatchdog>,
    pub cancel_query_watchdog: Option<QueryWatchdog>,
    pub last_cancel_trigger_ts: Option<i64>,
    pub last_open_cancel_reason: Option<&'static str>,
}

#[derive(Debug, Clone)]
pub struct OpenStrategyState {
    pub strategy_id: i32,
    pub open_symbol: String,
    pub open_venue: Option<TradingVenue>,
    pub order: OpenOrderState,
    pub signal_ts: i64,
    pub from_key: String,
    pub price_qv: QuantizedValue,
    pub price_offset: f64,
    pub alive: bool,
}

impl OpenStrategyState {
    pub fn new(strategy_id: i32) -> Self {
        Self {
            strategy_id,
            open_symbol: String::new(),
            open_venue: None,
            order: OpenOrderState::default(),
            signal_ts: 0,
            from_key: String::new(),
            price_qv: QuantizedValue::zero(),
            price_offset: 0.0,
            alive: true,
        }
    }
}

pub trait OpenStrategyCommon {
    fn strategy_name(&self) -> &'static str;
    fn open_state(&self) -> &OpenStrategyState;
    fn open_state_mut(&mut self) -> &mut OpenStrategyState;

    fn handoff_open_order_after_query_failure(
        &mut self,
        client_order_id: i64,
        marker: &'static str,
    );

    fn strategy_id(&self) -> i32 {
        self.open_state().strategy_id
    }

    fn open_order_state(&self) -> &OpenOrderState {
        &self.open_state().order
    }

    fn open_order_state_mut(&mut self) -> &mut OpenOrderState {
        &mut self.open_state_mut().order
    }

    fn open_order_id(&self) -> i64 {
        self.open_order_state().open_order_id
    }

    fn open_side(&self) -> Option<Side> {
        self.open_order_state().open_side
    }

    fn pending_order_query(&self) -> Option<PendingOrderQueryReason> {
        self.open_order_state().pending_order_query
    }

    fn set_pending_order_query(&mut self, reason: Option<PendingOrderQueryReason>) {
        self.open_order_state_mut().pending_order_query = reason;
    }

    fn order_query_watchdog(&self) -> Option<QueryWatchdog> {
        self.open_order_state().order_query_watchdog
    }

    fn set_order_query_watchdog(&mut self, watchdog: Option<QueryWatchdog>) {
        self.open_order_state_mut().order_query_watchdog = watchdog;
    }

    fn cancel_query_watchdog(&self) -> Option<QueryWatchdog> {
        self.open_order_state().cancel_query_watchdog
    }

    fn set_cancel_query_watchdog(&mut self, watchdog: Option<QueryWatchdog>) {
        self.open_order_state_mut().cancel_query_watchdog = watchdog;
    }

    /// 组合订单ID：高32位为策略ID，低32位为序列号。
    fn compose_order_id(strategy_id: i32) -> i64 {
        ((strategy_id as i64) << 32) | 1
    }

    fn extract_strategy_id(order_id: i64) -> i32 {
        (order_id >> 32) as i32
    }

    fn preview_text(raw: &str, max_chars: usize) -> String {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return "-".to_string();
        }
        let mut out = String::new();
        for (idx, ch) in trimmed.chars().enumerate() {
            if idx >= max_chars {
                out.push_str("...");
                break;
            }
            out.push(ch);
        }
        out
    }

    fn schedule_order_query_watchdog(&mut self, client_order_id: i64) {
        self.set_order_query_watchdog(Some(QueryWatchdog {
            client_order_id,
            due_ts_us: get_timestamp_us().saturating_add(ORDER_QUERY_WATCHDOG_DELAY_US),
            reason: PendingOrderQueryReason::OrderWatchdog,
        }));
    }

    fn schedule_cancel_query_watchdog(&mut self, client_order_id: i64) {
        self.set_cancel_query_watchdog(Some(QueryWatchdog {
            client_order_id,
            due_ts_us: get_timestamp_us().saturating_add(ORDER_QUERY_WATCHDOG_DELAY_US),
            reason: PendingOrderQueryReason::CancelWatchdog,
        }));
        if self
            .order_query_watchdog()
            .is_some_and(|w| w.client_order_id == client_order_id)
        {
            self.set_order_query_watchdog(None);
        }
    }

    fn clear_query_watchdogs(&mut self, client_order_id: i64) {
        if client_order_id == self.open_order_id() {
            self.set_pending_order_query(None);
        }
        if self
            .order_query_watchdog()
            .is_some_and(|w| w.client_order_id == client_order_id)
        {
            self.set_order_query_watchdog(None);
        }
        if self
            .cancel_query_watchdog()
            .is_some_and(|w| w.client_order_id == client_order_id)
        {
            self.set_cancel_query_watchdog(None);
        }
    }

    fn send_order_query(&mut self, client_order_id: i64, reason: PendingOrderQueryReason) -> bool {
        if let Some(existing) = self.pending_order_query() {
            if reason.is_cancel_rejected() && !existing.is_cancel_rejected() {
                self.set_pending_order_query(Some(PendingOrderQueryReason::CancelRejected));
            }
            return true;
        }

        let order_mgr = MonitorChannel::instance().order_manager();
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "{}: strategy_id={} send_order_query but local order missing: client_order_id={} reason={:?}",
                self.strategy_name(),
                self.strategy_id(),
                client_order_id,
                reason
            );
            return false;
        };

        match build_order_query_request(&order, client_order_id, client_order_id) {
            Ok((exchange, req_bytes)) => {
                if let Err(err) = QueryEngHub::publish_query_request(exchange.as_str(), &req_bytes)
                {
                    warn!(
                        "{}: strategy_id={} publish order query failed: exchange={} client_order_id={} reason={:?} err={:#}",
                        self.strategy_name(),
                        self.strategy_id(),
                        exchange,
                        client_order_id,
                        reason,
                        err
                    );
                    return false;
                }
                self.set_pending_order_query(Some(reason));
                debug!(
                    "{}: strategy_id={} order query sent: exchange={} client_order_id={} reason={:?}",
                    self.strategy_name(),
                    self.strategy_id(),
                    exchange,
                    client_order_id,
                    reason
                );
                true
            }
            Err(err) => {
                warn!(
                    "{}: strategy_id={} build order query failed: client_order_id={} reason={:?} err={}",
                    self.strategy_name(),
                    self.strategy_id(),
                    client_order_id,
                    reason,
                    err
                );
                false
            }
        }
    }

    fn handle_query_watchdogs(&mut self) {
        let now = get_timestamp_us();

        if let Some(w) = self.cancel_query_watchdog() {
            if now >= w.due_ts_us {
                self.set_cancel_query_watchdog(None);
                let order_mgr = MonitorChannel::instance().order_manager();
                let order_opt = order_mgr.borrow().get(w.client_order_id);
                if let Some(order) = order_opt.as_ref().filter(|o| !o.status.is_terminal()) {
                    let scheduled_at = w.due_ts_us.saturating_sub(ORDER_QUERY_WATCHDOG_DELAY_US);
                    let waited_ms = now.saturating_sub(scheduled_at).saturating_div(1_000);
                    info!(
                        "{}: strategy_id={} client_order_id={} symbol={} status={:?} exch_ord_id={:?} 等待{}ms仍未收到撤单/终态回报，发送order query回补 reason={:?}",
                        w.reason.watchdog_hint(),
                        self.strategy_id(),
                        w.client_order_id,
                        order.symbol,
                        order.status,
                        order.exchange_order_id,
                        waited_ms,
                        w.reason
                    );
                    if !self.send_order_query(w.client_order_id, w.reason) {
                        self.handoff_open_order_after_query_failure(
                            w.client_order_id,
                            w.reason.query_send_failed_trigger(),
                        );
                    }
                }
            }
        }

        if let Some(w) = self.order_query_watchdog() {
            if now >= w.due_ts_us {
                self.set_order_query_watchdog(None);
                let order_mgr = MonitorChannel::instance().order_manager();
                let order_opt = order_mgr.borrow().get(w.client_order_id);
                if let Some(order) = order_opt.as_ref().filter(|o| !o.status.is_terminal()) {
                    let scheduled_at = w.due_ts_us.saturating_sub(ORDER_QUERY_WATCHDOG_DELAY_US);
                    let waited_ms = now.saturating_sub(scheduled_at).saturating_div(1_000);
                    let since_submit_ms = now
                        .saturating_sub(order.timestamp.submit_t)
                        .saturating_div(1_000);
                    let hint = if order.status == OrderExecutionStatus::Commit {
                        "（下单后未收到New/成交推送）"
                    } else {
                        ""
                    };
                    info!(
                        "OrderWatchdog触发{}: strategy_id={} client_order_id={} symbol={} status={:?} exch_ord_id={:?} 等待{}ms仍未收到回报，发送order query回补 (since_submit={}ms)",
                        hint,
                        self.strategy_id(),
                        w.client_order_id,
                        order.symbol,
                        order.status,
                        order.exchange_order_id,
                        waited_ms,
                        since_submit_ms
                    );
                    if !self.send_order_query(w.client_order_id, w.reason) {
                        self.handoff_open_order_after_query_failure(
                            w.client_order_id,
                            "order query send failed",
                        );
                    }
                }
            }
        }
    }
}
