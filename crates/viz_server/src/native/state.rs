//! Observation derives from durable facts; it never advances the trading OMS.
use anyhow::{ensure, Context, Result};
use persist_common::cn_futures::{
    NativeOrderRecord, OrderStatus, Position, SendState, Side, Venue,
};
use persist_manager::cn_futures::Evidence;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderPhase {
    Prepared,
    Pending,
    Working,
    PartiallyFilled,
    CancelPending,
    Settling,
    Filled,
    Cancelled,
    Rejected,
    Uncertain,
}
#[derive(Debug, Clone, Serialize)]
pub struct ObservedOrder {
    pub phase: OrderPhase,
    pub exchange_terminal: bool,
    pub facts_complete: bool,
    pub record: NativeOrderRecord,
}
pub fn observe(record: NativeOrderRecord) -> Result<ObservedOrder> {
    let o = &record.order;
    let lots = o.request.lots;
    ensure!(
        lots > 0
            && o.filled >= 0
            && o.reported_filled >= 0
            && o.cancelled >= 0
            && o.reported_filled <= lots
            && i64::from(o.filled) + i64::from(o.cancelled) <= i64::from(lots),
        "inconsistent native order quantities"
    );
    ensure!(
        record.identity.native == o.key && record.identity.venue == o.request.venue,
        "inconsistent native order identity"
    );
    // Order reports may precede their trades, and trades may precede reports.
    let facts_complete = o.filled == o.reported_filled;
    let phase = if record.send_state == SendState::Uncertain {
        OrderPhase::Uncertain
    } else if o.status.terminal() && !facts_complete {
        OrderPhase::Settling
    } else if o.status == OrderStatus::Filled {
        ensure!(
            o.filled == lots,
            "filled terminal without complete factual fills"
        );
        OrderPhase::Filled
    } else if o.status == OrderStatus::Cancelled {
        OrderPhase::Cancelled
    } else if o.status == OrderStatus::Rejected || record.send_state == SendState::RejectedLocally {
        OrderPhase::Rejected
    } else if o.cancel_pending {
        OrderPhase::CancelPending
    } else if o.filled > 0 {
        OrderPhase::PartiallyFilled
    } else if record.send_state == SendState::Prepared {
        OrderPhase::Prepared
    } else if o.status == OrderStatus::Working {
        OrderPhase::Working
    } else {
        OrderPhase::Pending
    };
    Ok(ObservedOrder {
        phase,
        exchange_terminal: o.status.terminal() && record.send_state == SendState::Observed,
        facts_complete,
        record,
    })
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PositionRow {
    pub venue: Venue,
    pub symbol: String,
    pub side: Side,
    pub position: Position,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StateSnapshot {
    pub ready: bool,
    pub connected: bool,
    pub reason: String,
    pub trading_day: String,
    pub available_local_budget: f64,
    pub positions: Vec<PositionRow>,
}
#[derive(Debug, Default)]
pub struct Lifecycle {
    pub sequence: u64,
    pub last_event_us: Option<u64>,
    pub state_us: Option<u64>,
    pub state_sequence: u64,
    pub state: Option<StateSnapshot>,
    pub projection_errors: usize,
    pub execute: Option<bool>,
    pub can_trade: Option<bool>,
    pub account: Option<Value>,
    pub account_recorded_us: Option<u64>,
    start: u64,
    stopped: bool,
    guard: Option<(u64, &'static str, String)>,
    transport: Option<(u64, bool, u64)>,
    last_projection_error: u64,
    reconciled: u64,
}
#[derive(Debug, Serialize)]
pub struct LifecycleView<'a> {
    pub status: &'static str,
    pub reason: &'a str,
    pub last_event_us: Option<u64>,
    pub state_observed_us: Option<u64>,
    pub data_stale: bool,
    pub state_stale: bool,
    pub snapshot_current_runtime: bool,
    pub reported_can_trade: Option<bool>,
    pub execution_enabled_at_start: Option<bool>,
    pub transport_ready: Option<bool>,
    pub transport_observed_us: Option<u64>,
    pub projection_errors: usize,
    pub state: &'a Option<StateSnapshot>,
    pub account: &'a Option<Value>,
    pub account_recorded_us: Option<u64>,
}
pub fn stale(now: u64, observed: Option<u64>, after_ms: u64) -> bool {
    observed.is_none_or(|ts| {
        ts > now.saturating_add(5_000_000) || now.saturating_sub(ts) > after_ms.saturating_mul(1000)
    })
}
impl Lifecycle {
    pub fn apply(&mut self, e: &Evidence) -> Result<()> {
        ensure!(
            e.sequence > self.sequence,
            "non-monotonic evidence sequence"
        );
        let v = &e.value;
        let kind = v["type"].as_str().context("missing evidence type")?;
        if e.projection_error.is_some() {
            self.projection_errors += 1;
            self.last_projection_error = e.sequence;
        }
        match kind {
            "runtime_start" => {
                self.start = e.sequence;
                self.stopped = false;
                self.execute = v["execute"].as_bool();
                self.can_trade = None;
                self.transport = None;
            }
            "runtime_stopped" => {
                self.stopped = true;
            }
            "state" | "reconciled" => {
                let state: StateSnapshot = serde_json::from_value(v["state"].clone())
                    .context("decode native state snapshot")?;
                ensure!(
                    state.available_local_budget.is_finite() && state.available_local_budget >= 0.0,
                    "invalid local execution budget"
                );
                let mut identities = std::collections::HashSet::new();
                for p in &state.positions {
                    p.position.validate()?;
                    ensure!(
                        !p.symbol.is_empty() && identities.insert((p.venue, &p.symbol, p.side)),
                        "invalid/duplicate native position"
                    );
                }
                self.state = Some(state);
                self.state_us = Some(e.received_us);
                self.state_sequence = e.sequence;
                self.can_trade = v["can_trade"].as_bool();
                if kind == "reconciled" {
                    self.reconciled = e.sequence;
                }
                if let Some(account) = v["account"].as_object() {
                    // Expose financial fields only, not arbitrary runtime/raw SDK data.
                    let safe = [
                        "trading_day",
                        "currency",
                        "balance",
                        "available",
                        "curr_margin",
                        "frozen_margin",
                        "frozen_cash",
                        "commission",
                        "close_profit",
                        "position_profit",
                    ]
                    .into_iter()
                    .filter_map(|k| account.get(k).map(|v| (k.to_string(), v.clone())))
                    .collect();
                    self.account = Some(Value::Object(safe));
                    self.account_recorded_us = Some(e.received_us);
                }
                if let Some(ready) = v["transport_ready"].as_bool() {
                    self.transport = Some((e.sequence, ready, e.received_us));
                }
            }
            "transport_status" => {
                let ready = v["ready"]
                    .as_bool()
                    .context("missing transport readiness")?;
                self.transport = Some((e.sequence, ready, e.received_us));
                if !ready {
                    // A recovered transport does not itself release the core's
                    // block. Wait for a newer execution snapshot/reconciliation.
                    self.guard = Some((
                        e.sequence,
                        "transport_unavailable",
                        "native trading transport unavailable; awaiting execution state".into(),
                    ));
                }
            }
            "trading_blocked" | "restart_required" => {
                self.guard = Some((
                    e.sequence,
                    "blocked",
                    v["error"]
                        .as_str()
                        .or(v["reason"].as_str())
                        .unwrap_or("execution blocked")
                        .into(),
                ));
            }
            "reconciling" => {
                self.guard = Some((e.sequence, "recovering", String::new()));
            }
            "ctp_event" | "yida_event" => {
                let raw = &v["event"];
                if kind == "ctp_event" || raw["replay"].as_i64() == Some(0) {
                    let phase = match raw["kind"].as_i64() {
                        Some(1) => Some(("recovering", "native connection recovering")),
                        Some(2) => Some(("disconnected", "native connection disconnected")),
                        Some(14 | 16) if kind == "yida_event" => {
                            Some(("blocked", "native adapter invalidated or failed"))
                        }
                        _ => None,
                    };
                    if let Some((phase, reason)) = phase {
                        self.guard = Some((e.sequence, phase, reason.into()));
                        self.transport = None;
                    }
                }
            }
            _ => {}
        }
        self.sequence = e.sequence;
        self.last_event_us = Some(e.received_us);
        Ok(())
    }
    pub fn view(&self, now: u64, stale_ms: u64) -> LifecycleView<'_> {
        let data_stale = stale(now, self.last_event_us, stale_ms);
        let state_stale = stale(now, self.state_us, stale_ms);
        let current = self.state.is_some() && self.state_sequence >= self.start;
        let guard = self
            .guard
            .as_ref()
            .filter(|g| g.0 > self.state_sequence.max(self.start));
        let transport = self.transport.filter(|t| t.0 >= self.start);
        let status = if self.stopped {
            "stopped"
        } else if self.last_event_us.is_none() {
            "waiting"
        } else if let Some(g) = guard {
            g.1
        } else if !current {
            "recovering"
        } else if self.last_projection_error > self.reconciled {
            "blocked"
        } else if self.state.as_ref().is_some_and(|s| !s.connected) {
            "disconnected"
        } else if self.state.as_ref().is_some_and(|s| !s.ready) {
            "blocked"
        } else if transport.is_some_and(|t| !t.1) {
            "transport_unavailable"
        } else if data_stale || state_stale {
            "stale"
        } else {
            "observing"
        };
        LifecycleView {
            status,
            reason: guard
                .map(|g| g.2.as_str())
                .unwrap_or_else(|| self.state.as_ref().map_or("", |s| s.reason.as_str())),
            last_event_us: self.last_event_us,
            state_observed_us: self.state_us,
            data_stale,
            state_stale,
            snapshot_current_runtime: current,
            reported_can_trade: if current && guard.is_none() && !self.stopped {
                self.can_trade
            } else {
                None
            },
            execution_enabled_at_start: self.execute,
            transport_ready: if self.stopped {
                None
            } else {
                transport.map(|t| t.1)
            },
            transport_observed_us: transport.map(|t| t.2),
            projection_errors: self.projection_errors,
            state: &self.state,
            account: &self.account,
            account_recorded_us: self.account_recorded_us,
        }
    }
}
