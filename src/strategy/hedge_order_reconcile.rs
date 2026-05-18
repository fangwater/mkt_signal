use crate::common::time_util::get_timestamp_us;
use crate::common::trade_error_code::describe_trade_error_code;
use crate::pre_trade::monitor_channel::MonitorChannel;
use crate::pre_trade::order_manager::OrderExecutionStatus;
use crate::pre_trade::QueryEngHub;
use crate::strategy::manager::Strategy;
use crate::strategy::order_query_builder::build_order_query_request;
use crate::strategy::order_reconcile::{
    order_query_watchdog_delay_us, PendingOrderQueryReason, ORDER_QUERY_WATCHDOG_DELAY_US,
};
use crate::strategy::trade_engine_response::{TradeEngineResponse, TradeRequestKind};
use crate::strategy::ws_order_update::prepare_failed_trade_engine_response_for_strategy;
use log::{debug, warn};
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct HedgeOrderReconcileState {
    pub pending_order_queries: HashMap<i64, PendingOrderQueryReason>,
    pub order_query_watchdogs: HashMap<i64, (i64, PendingOrderQueryReason)>,
}

impl HedgeOrderReconcileState {
    pub fn is_cancel_reconcile_reason(reason: PendingOrderQueryReason) -> bool {
        matches!(
            reason,
            PendingOrderQueryReason::CancelFailed | PendingOrderQueryReason::CancelRejected
        )
    }

    pub fn is_cancel_reconciling(&self, client_order_id: i64) -> bool {
        self.pending_order_queries
            .get(&client_order_id)
            .copied()
            .is_some_and(Self::is_cancel_reconcile_reason)
            || self
                .order_query_watchdogs
                .get(&client_order_id)
                .map(|(_, reason)| *reason)
                .is_some_and(Self::is_cancel_reconcile_reason)
    }
}

pub trait HedgeOrderReconcileCommon: Strategy {
    fn hedge_reconcile_strategy_name(&self) -> &'static str;
    fn hedge_reconcile_strategy_id(&self) -> i32;
    fn hedge_reconcile_state(&self) -> &HedgeOrderReconcileState;
    fn hedge_reconcile_state_mut(&mut self) -> &mut HedgeOrderReconcileState;
    fn is_hedge_order_tracked(&self, client_order_id: i64) -> bool;

    fn handoff_hedge_order_after_query_failure(
        &mut self,
        client_order_id: i64,
        reason: &str,
    ) -> bool;

    fn handle_hedge_open_failed(
        &mut self,
        response: &dyn TradeEngineResponse,
        code_desc: &str,
        client_order_id: i64,
    );

    fn handle_hedge_cancel_failed(
        &mut self,
        response: &dyn TradeEngineResponse,
        code_desc: &str,
        client_order_id: i64,
    ) {
        let reason = if response.is_cancel_not_cancellable() {
            PendingOrderQueryReason::CancelRejected
        } else {
            PendingOrderQueryReason::CancelFailed
        };
        warn!(
            "{}: strategy_id={} hedge cancel failed: req_type={} status={} code={}({}) client_order_id={} is_cancel_not_cancellable={} reason={:?} {}",
            self.hedge_reconcile_strategy_name(),
            self.hedge_reconcile_strategy_id(),
            response.req_type(),
            response.status(),
            response.error_code(),
            code_desc,
            client_order_id,
            response.is_cancel_not_cancellable(),
            reason,
            self.hedge_order_trace_snapshot(client_order_id)
        );
        self.clear_order_query_state(client_order_id);
        if !self.send_order_query(client_order_id, reason) {
            self.handoff_hedge_order_after_query_failure(
                client_order_id,
                "trade cancel query send failed",
            );
        }
    }

    fn handle_hedge_other_failed(
        &mut self,
        response: &dyn TradeEngineResponse,
        code_desc: &str,
        client_order_id: i64,
    ) {
        warn!(
            "{}: strategy_id={} hedge other failed: req_type={} status={} code={}({}) client_order_id={} {}",
            self.hedge_reconcile_strategy_name(),
            self.hedge_reconcile_strategy_id(),
            response.req_type(),
            response.status(),
            response.error_code(),
            code_desc,
            client_order_id,
            self.hedge_order_trace_snapshot(client_order_id)
        );
    }

    fn apply_hedge_trade_engine_response_common(&mut self, response: &dyn TradeEngineResponse)
    where
        Self: Sized,
    {
        let Some(client_order_id) =
            prepare_failed_trade_engine_response_for_strategy(self, response)
        else {
            return;
        };

        let exchange = response.exchange_enum();
        let code_desc = exchange
            .and_then(|ex| describe_trade_error_code(ex, response.error_code()))
            .unwrap_or("unknown");

        match response.request_kind() {
            TradeRequestKind::Open => {
                self.handle_hedge_open_failed(response, code_desc, client_order_id)
            }
            TradeRequestKind::Cancel => {
                self.handle_hedge_cancel_failed(response, code_desc, client_order_id)
            }
            TradeRequestKind::Other => {
                self.handle_hedge_other_failed(response, code_desc, client_order_id)
            }
        }
    }

    fn is_cancel_reconciling(&self, client_order_id: i64) -> bool {
        self.hedge_reconcile_state()
            .is_cancel_reconciling(client_order_id)
    }

    fn hedge_order_trace_snapshot(&self, client_order_id: i64) -> String {
        let state = self.hedge_reconcile_state();
        let pending_reason = state.pending_order_queries.get(&client_order_id).copied();
        let watchdog = state
            .order_query_watchdogs
            .get(&client_order_id)
            .map(|(due_ts, reason)| (*due_ts, *reason));
        let tracked = self.is_hedge_order_tracked(client_order_id);

        let order_desc = if let Some(order_mgr) = MonitorChannel::try_order_manager() {
            let mgr = order_mgr.borrow();
            if let Some(order) = mgr.get(client_order_id) {
                format!(
                    "local=present venue={:?} symbol={} side={:?} status={:?} exch_ord_id={:?} cum_fill={:.8} qty={:.8} px={:.8}",
                    order.venue,
                    order.symbol,
                    order.side,
                    order.status,
                    order.exchange_order_id,
                    order.cumulative_filled_quantity,
                    order.quantity,
                    order.price
                )
            } else {
                "local=missing".to_string()
            }
        } else {
            "local=order_manager_unavailable".to_string()
        };

        format!(
            "client_order_id={} tracked={} pending_query={:?} watchdog={:?} {}",
            client_order_id, tracked, pending_reason, watchdog, order_desc
        )
    }

    fn schedule_order_query_watchdog(
        &mut self,
        client_order_id: i64,
        reason: PendingOrderQueryReason,
    ) {
        let delay_us = self.order_query_watchdog_delay_us_for_order_id(client_order_id);
        let due = get_timestamp_us().saturating_add(delay_us);
        self.hedge_reconcile_state_mut()
            .order_query_watchdogs
            .insert(client_order_id, (due, reason));
        debug!(
            "{}Reconcile: strategy_id={} schedule_watchdog due_ts={} reason={:?} {}",
            self.hedge_reconcile_strategy_name(),
            self.hedge_reconcile_strategy_id(),
            due,
            reason,
            self.hedge_order_trace_snapshot(client_order_id)
        );
    }

    fn order_query_watchdog_delay_us_for_order_id(&self, client_order_id: i64) -> i64 {
        let Some(order_mgr) = MonitorChannel::try_order_manager() else {
            return ORDER_QUERY_WATCHDOG_DELAY_US;
        };
        let mgr = order_mgr.borrow();
        let Some(order) = mgr.get(client_order_id) else {
            return ORDER_QUERY_WATCHDOG_DELAY_US;
        };
        order_query_watchdog_delay_us(&order, mgr.binance_is_standard())
    }

    fn clear_order_query_state(&mut self, client_order_id: i64) {
        let (removed_watchdog, removed_pending) = {
            let state = self.hedge_reconcile_state_mut();
            (
                state.order_query_watchdogs.remove(&client_order_id),
                state.pending_order_queries.remove(&client_order_id),
            )
        };
        if removed_watchdog.is_some() || removed_pending.is_some() {
            debug!(
                "{}Reconcile: strategy_id={} clear_query_state removed_pending={:?} removed_watchdog={:?} {}",
                self.hedge_reconcile_strategy_name(),
                self.hedge_reconcile_strategy_id(),
                removed_pending,
                removed_watchdog,
                self.hedge_order_trace_snapshot(client_order_id)
            );
        }
    }

    fn handle_order_query_watchdogs(&mut self) {
        if self
            .hedge_reconcile_state()
            .order_query_watchdogs
            .is_empty()
        {
            return;
        }
        let now = get_timestamp_us();
        let due_entries: Vec<(i64, PendingOrderQueryReason)> = self
            .hedge_reconcile_state()
            .order_query_watchdogs
            .iter()
            .filter_map(|(client_order_id, (due_ts, reason))| {
                (now >= *due_ts).then_some((*client_order_id, *reason))
            })
            .collect();
        if due_entries.is_empty() {
            return;
        }

        for (client_order_id, reason) in due_entries {
            let pending_removed = {
                let state = self.hedge_reconcile_state_mut();
                state.order_query_watchdogs.remove(&client_order_id);
                state
                    .pending_order_queries
                    .remove(&client_order_id)
                    .is_some()
            };
            if pending_removed {
                self.handoff_hedge_order_after_query_failure(
                    client_order_id,
                    "query response timeout",
                );
                continue;
            }

            let order_opt = MonitorChannel::instance()
                .order_manager()
                .borrow()
                .get(client_order_id);
            if let Some(order) = order_opt.as_ref().filter(|o| !o.status.is_terminal()) {
                let delay_us = self.order_query_watchdog_delay_us_for_order_id(client_order_id);
                let scheduled_at = now.saturating_sub(delay_us);
                let waited_ms = now.saturating_sub(scheduled_at).saturating_div(1_000);
                let since_submit_ms = now
                    .saturating_sub(order.timestamp.submit_t)
                    .saturating_div(1_000);
                let hint = if order.status == OrderExecutionStatus::Commit {
                    " after submit without NEW/trade"
                } else {
                    ""
                };
                debug!(
                    "{} OrderWatchdog{}: strategy_id={} client_order_id={} symbol={} status={:?} exch_ord_id={:?} waited_ms={} since_submit_ms={} reason={:?}",
                    self.hedge_reconcile_strategy_name(),
                    hint,
                    self.hedge_reconcile_strategy_id(),
                    client_order_id,
                    order.symbol,
                    order.status,
                    order.exchange_order_id,
                    waited_ms,
                    since_submit_ms,
                    reason
                );
                if !self.send_order_query(client_order_id, reason) {
                    self.handoff_hedge_order_after_query_failure(
                        client_order_id,
                        "query send failed",
                    );
                }
            }
        }
    }

    fn send_order_query(&mut self, client_order_id: i64, reason: PendingOrderQueryReason) -> bool {
        if let Some(existing) = self
            .hedge_reconcile_state()
            .pending_order_queries
            .get(&client_order_id)
            .copied()
        {
            let upgraded = matches!(
                (existing, reason),
                (
                    PendingOrderQueryReason::OrderWatchdog,
                    PendingOrderQueryReason::CancelFailed
                ) | (
                    PendingOrderQueryReason::OrderWatchdog,
                    PendingOrderQueryReason::CancelRejected
                ) | (
                    PendingOrderQueryReason::CancelFailed,
                    PendingOrderQueryReason::CancelRejected
                )
            );
            if upgraded {
                let state = self.hedge_reconcile_state_mut();
                state.pending_order_queries.insert(client_order_id, reason);
                if let Some((due_ts, _)) =
                    state.order_query_watchdogs.get(&client_order_id).copied()
                {
                    state
                        .order_query_watchdogs
                        .insert(client_order_id, (due_ts, reason));
                }
            }
            debug!(
                "{}Reconcile: strategy_id={} skip_send_order_query because pending already exists: reason={:?} upgraded={} {}",
                self.hedge_reconcile_strategy_name(),
                self.hedge_reconcile_strategy_id(),
                reason,
                upgraded,
                self.hedge_order_trace_snapshot(client_order_id)
            );
            return true;
        }

        let order_mgr = MonitorChannel::instance().order_manager();
        let Some(order) = order_mgr.borrow().get(client_order_id) else {
            warn!(
                "{}: strategy_id={} send_order_query but local order missing: client_order_id={} reason={:?}",
                self.hedge_reconcile_strategy_name(),
                self.hedge_reconcile_strategy_id(),
                client_order_id,
                reason
            );
            return false;
        };

        let query_by_exchange_order_id = order.exchange_order_id.is_some_and(|id| id > 0);
        match build_order_query_request(&order, client_order_id, client_order_id) {
            Ok((exchange, req_bytes)) => {
                if let Err(err) = QueryEngHub::publish_query_request_for(
                    client_order_id,
                    exchange.as_str(),
                    &req_bytes,
                ) {
                    warn!(
                        "{}: strategy_id={} publish order query failed: exchange={} client_order_id={} reason={:?} err={:#}",
                        self.hedge_reconcile_strategy_name(),
                        self.hedge_reconcile_strategy_id(),
                        exchange,
                        client_order_id,
                        reason,
                        err
                    );
                    return false;
                }
                self.hedge_reconcile_state_mut()
                    .pending_order_queries
                    .insert(client_order_id, reason);
                self.schedule_order_query_watchdog(client_order_id, reason);
                debug!(
                    "{}Reconcile: strategy_id={} order_query_sent exchange={} reason={:?} by_exchange_order_id={} payload_len={} {}",
                    self.hedge_reconcile_strategy_name(),
                    self.hedge_reconcile_strategy_id(),
                    exchange,
                    reason,
                    query_by_exchange_order_id,
                    req_bytes.len(),
                    self.hedge_order_trace_snapshot(client_order_id)
                );
                true
            }
            Err(err) => {
                warn!(
                    "{}: strategy_id={} build order query failed: client_order_id={} reason={:?} err={}",
                    self.hedge_reconcile_strategy_name(),
                    self.hedge_reconcile_strategy_id(),
                    client_order_id,
                    reason,
                    err
                );
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::HedgeOrderReconcileState;
    use crate::strategy::order_reconcile::PendingOrderQueryReason;

    #[test]
    fn cancel_reconcile_reasons_are_classified() {
        assert!(HedgeOrderReconcileState::is_cancel_reconcile_reason(
            PendingOrderQueryReason::CancelFailed
        ));
        assert!(HedgeOrderReconcileState::is_cancel_reconcile_reason(
            PendingOrderQueryReason::CancelRejected
        ));
        assert!(!HedgeOrderReconcileState::is_cancel_reconcile_reason(
            PendingOrderQueryReason::OrderWatchdog
        ));
    }
}
