//! In-process strategy hooks. Every hook runs on the same thread as risk and OMS.
use super::market_data::TickRecord;
use crate::core::{SubmitReceipt, TradingCore};
use crate::model::{Intent, OrderKey};
use anyhow::{ensure, Result};
use smallvec::SmallVec;
use std::sync::Arc;

pub const MAX_ACTIONS_PER_TICK: usize = 8;
pub struct StrategyContext<'a> {
    pub core: &'a TradingCore,
    /// Includes execution permission, recovery durability, MD readiness and ingress backlog.
    pub can_trade: bool,
}
pub enum Action {
    Submit {
        tag: u64,
        intent: Intent,
    },
    SubmitExec {
        tag: u64,
        intent: Intent,
        target: std::sync::Arc<super::exec::Target>,
    },
    Cancel {
        tag: u64,
        key: OrderKey,
    },
}
#[derive(Default)]
pub struct Actions {
    values: SmallVec<[Action; MAX_ACTIONS_PER_TICK]>,
}
impl Actions {
    pub fn push(&mut self, action: Action) -> Result<()> {
        ensure!(
            self.values.len() < MAX_ACTIONS_PER_TICK,
            "strategy action budget exceeded"
        );
        self.values.push(action);
        Ok(())
    }
    /// Actions still allowed in this decision.
    pub fn remaining(&self) -> usize {
        MAX_ACTIONS_PER_TICK - self.values.len()
    }
    #[cfg(any(feature = "ctp-sdk", feature = "yida-sdk", test))]
    pub(crate) fn drain(&mut self) -> impl Iterator<Item = Action> + '_ {
        self.values.drain(..)
    }
}
pub enum ActionOutcome {
    Submitted {
        tag: u64,
        receipt: SubmitReceipt,
    },
    CancelledLocally {
        tag: u64,
        key: OrderKey,
    },
    Rejected {
        tag: u64,
        error: String,
    },
    /// A cancel the OMS refused locally (already terminal or pending); the
    /// order's own reports settle it, so execution continues.
    CancelRejected {
        tag: u64,
        key: OrderKey,
        error: String,
    },
    #[cfg(any(feature = "ctp-sdk", test))]
    CtpDispatchBlocked {
        action: Action,
        checks: CtpDispatchChecks,
        quote_received_us: u64,
        quote_generation: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CtpDispatchChecks {
    pub strategy_can_trade: bool,
    pub execution_enabled: bool,
    pub account_ready: bool,
    pub market_data_ready: bool,
    /// A newer quote for the same contract is queued behind this one.
    pub newer_contract_quote_queued: bool,
    pub trade_event_backlog: bool,
    pub journal_failed: bool,
    pub trade_ingress_failed: bool,
}

#[cfg(any(feature = "ctp-sdk", test))]
impl CtpDispatchChecks {
    fn healthy(self) -> bool {
        self.strategy_can_trade
            && self.execution_enabled
            && self.account_ready
            && self.market_data_ready
            && !self.journal_failed
            && !self.trade_ingress_failed
    }

    pub(crate) fn can_dispatch(self) -> bool {
        self.healthy() && !self.newer_contract_quote_queued && !self.trade_event_backlog
    }

    pub(crate) fn can_retry(self) -> bool {
        self.healthy() && (self.newer_contract_quote_queued || self.trade_event_backlog)
    }
}
/// Hooks must do bounded CPU work only: no network/disk I/O, sleeps or waits.
/// Submitted outcomes report local SDK acceptance, never venue acceptance/fills.
pub trait Strategy {
    fn on_target(
        &mut self,
        _target: super::exec::Target,
        _context: StrategyContext<'_>,
    ) -> Result<Arc<super::exec::Target>> {
        anyhow::bail!("target execution is not configured")
    }
    fn on_tick(
        &mut self,
        tick: &TickRecord,
        context: StrategyContext<'_>,
        actions: &mut Actions,
    ) -> Result<()>;
    fn on_market_state(&mut self, _ready: bool, _generation: u64) -> Result<()> {
        Ok(())
    }
    /// The admitted target that orders currently work towards.
    fn standing_target(&self) -> Option<Arc<super::exec::Target>> {
        None
    }
    /// Latest change in why a target quote did or did not emit an action.
    fn take_wait_note(&mut self) -> Option<super::exec::WaitNote> {
        None
    }
    fn on_update(
        &mut self,
        _update: &crate::model::DecodedUpdate,
        _context: StrategyContext<'_>,
    ) -> Result<()> {
        Ok(())
    }
    /// Durable strategy attribution read at startup.
    fn restore_ledger(
        &mut self,
        _rows: Vec<(String, cn_futures_target::ledger::Row)>,
    ) -> Result<()> {
        Ok(())
    }
    /// A newly applied live factual fill; `own_order` marks this process's order.
    fn on_fill(&mut self, _fill: &crate::model::Fill, _own_order: bool) {}
    fn take_ledger_events(&mut self) -> Vec<super::exec::LedgerEvent> {
        Vec::new()
    }
    /// Whether any strategy still holds attribution on this contract.
    fn attributes(&self, _market: &str) -> bool {
        false
    }
    /// Operator cutover: credit unallocated holdings to a strategy at a cost.
    fn assign_position(
        &mut self,
        _core: &TradingCore,
        _strategy: &str,
        _market: &str,
        _lots: i64,
        _price: f64,
        _now_us: u64,
    ) -> Result<()> {
        anyhow::bail!("position assignment requires target execution")
    }
}
pub struct NoStrategy;
impl Strategy for NoStrategy {
    fn on_tick(&mut self, _: &TickRecord, _: StrategyContext<'_>, _: &mut Actions) -> Result<()> {
        Ok(())
    }
}

pub struct ConfiguredStrategy(pub Option<super::exec::Exec>);
impl Strategy for ConfiguredStrategy {
    fn on_tick(
        &mut self,
        tick: &TickRecord,
        context: StrategyContext<'_>,
        actions: &mut Actions,
    ) -> Result<()> {
        if let Some(strategy) = &mut self.0 {
            strategy.on_tick(tick, context, actions)?;
        }
        Ok(())
    }
    fn on_target(
        &mut self,
        target: super::exec::Target,
        context: StrategyContext<'_>,
    ) -> Result<Arc<super::exec::Target>> {
        self.0
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("exec is not configured"))?
            .on_target(target, context)
    }
    fn on_market_state(&mut self, ready: bool, generation: u64) -> Result<()> {
        if let Some(strategy) = &mut self.0 {
            strategy.on_market_state(ready, generation)?;
        }
        Ok(())
    }
    fn take_wait_note(&mut self) -> Option<super::exec::WaitNote> {
        self.0.as_mut().and_then(super::exec::Exec::take_wait_note)
    }
    fn standing_target(&self) -> Option<Arc<super::exec::Target>> {
        self.0.as_ref().and_then(|exec| exec.standing_target())
    }
    fn restore_ledger(
        &mut self,
        rows: Vec<(String, cn_futures_target::ledger::Row)>,
    ) -> Result<()> {
        match &mut self.0 {
            Some(strategy) => strategy.restore_ledger(rows),
            None => Ok(()),
        }
    }
    fn on_fill(&mut self, fill: &crate::model::Fill, own_order: bool) {
        if let Some(strategy) = &mut self.0 {
            strategy.on_fill(fill, own_order);
        }
    }
    fn take_ledger_events(&mut self) -> Vec<super::exec::LedgerEvent> {
        self.0
            .as_mut()
            .map(super::exec::Exec::take_ledger_events)
            .unwrap_or_default()
    }
    fn attributes(&self, market: &str) -> bool {
        self.0.as_ref().is_some_and(|c| c.attributes(market))
    }
    fn assign_position(
        &mut self,
        core: &TradingCore,
        strategy: &str,
        market: &str,
        lots: i64,
        price: f64,
        now_us: u64,
    ) -> Result<()> {
        self.0
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("exec is not configured"))?
            .assign_position(core, strategy, market, lots, price, now_us)
    }
}

#[cfg(any(feature = "ctp-sdk", feature = "yida-sdk", test))]
pub(crate) fn dispatch<T: crate::model::Trader>(
    runtime: &mut crate::core::SingleLoop<T>,
    journal: &crate::persist::PersistChannel,
    action: Action,
    can_trade: bool,
) -> Result<ActionOutcome> {
    let day = runtime.core.trading_day();
    let tag = match &action {
        Action::Submit { tag, .. }
        | Action::SubmitExec { tag, .. }
        | Action::Cancel { tag, .. } => *tag,
    };
    if !can_trade || !runtime.execute || !runtime.core.is_ready() {
        return Ok(ActionOutcome::Rejected {
            tag,
            error: "strategy execution gate closed".into(),
        });
    }
    let target = match &action {
        Action::SubmitExec { target, .. } => Some(target.clone()),
        _ => None,
    };
    let outcome = match action {
        Action::Submit { tag, intent } | Action::SubmitExec { tag, intent, .. } => {
            match runtime
                .submit_planned_with(intent, |p| journal.prepared(day, p, target.as_deref()))
            {
                Ok(receipt) => {
                    let keys: SmallVec<[OrderKey; 2]> =
                        receipt.children.iter().map(|c| c.key).collect();
                    journal.orders(&runtime.core, &keys)?;
                    ActionOutcome::Submitted { tag, receipt }
                }
                Err(error) => {
                    journal.state(&runtime.core)?;
                    // The full chain: the context alone hides the cause.
                    ActionOutcome::Rejected {
                        tag,
                        error: format!("{error:#}"),
                    }
                }
            }
        }
        Action::Cancel { tag, key } => {
            match runtime.cancel_with(&key, |p| journal.prepared_cancel(p)) {
                Ok(()) => {
                    journal.orders(&runtime.core, std::slice::from_ref(&key))?;
                    ActionOutcome::CancelledLocally { tag, key }
                }
                Err(error) => ActionOutcome::CancelRejected {
                    tag,
                    key,
                    error: format!("{error:#}"),
                },
            }
        }
    };
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn action_budget_is_bounded_without_spilling_to_the_heap() {
        let mut actions = Actions::default();
        for tag in 0..MAX_ACTIONS_PER_TICK as u64 {
            actions
                .push(Action::Cancel {
                    tag,
                    key: OrderKey {
                        session: "ctp:1:2".into(),
                        order_ref: "1".into(),
                    },
                })
                .unwrap();
        }
        assert!(!actions.values.spilled());
        assert!(actions
            .push(Action::Cancel {
                tag: 99,
                key: OrderKey {
                    session: "ctp:1:2".into(),
                    order_ref: "1".into()
                }
            })
            .is_err());
        assert_eq!(actions.drain().count(), MAX_ACTIONS_PER_TICK);
        assert!(actions.values.is_empty());
    }
}
