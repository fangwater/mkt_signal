//! In-process strategy hooks. Every hook runs on the same thread as risk and OMS.
use super::market_data::TickRecord;
use crate::core::{SubmitReceipt, TradingCore};
use crate::model::{Intent, OrderKey};
use anyhow::{ensure, Result};
use smallvec::SmallVec;

pub const MAX_ACTIONS_PER_TICK: usize = 8;
pub struct StrategyContext<'a> {
    pub core: &'a TradingCore,
    /// Includes execution permission, recovery durability, MD readiness and ingress backlog.
    pub can_trade: bool,
}
pub enum Action {
    Submit { tag: u64, intent: Intent },
    Cancel { tag: u64, key: OrderKey },
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
    #[cfg(any(feature = "ctp-sdk", test))]
    pub(crate) fn drain(&mut self) -> impl Iterator<Item = Action> + '_ {
        self.values.drain(..)
    }
}
pub enum ActionOutcome {
    Submitted { tag: u64, receipt: SubmitReceipt },
    CancelledLocally { tag: u64, key: OrderKey },
    Rejected { tag: u64, error: String },
}
/// Hooks must do bounded CPU work only: no network/disk I/O, sleeps or waits.
/// Submitted outcomes report local SDK acceptance, never venue acceptance/fills.
pub trait Strategy {
    fn on_tick(
        &mut self,
        tick: &TickRecord,
        context: StrategyContext<'_>,
        actions: &mut Actions,
    ) -> Result<()>;
    fn on_market_state(&mut self, _ready: bool, _generation: u64) -> Result<()> {
        Ok(())
    }
    fn on_result(&mut self, _outcome: &ActionOutcome, _context: StrategyContext<'_>) -> Result<()> {
        Ok(())
    }
    fn on_update(
        &mut self,
        _update: &crate::model::DecodedUpdate,
        _context: StrategyContext<'_>,
    ) -> Result<()> {
        Ok(())
    }
}
pub struct NoStrategy;
impl Strategy for NoStrategy {
    fn on_tick(&mut self, _: &TickRecord, _: StrategyContext<'_>, _: &mut Actions) -> Result<()> {
        Ok(())
    }
}

#[cfg(feature = "ctp-sdk")]
pub(crate) fn dispatch<T: crate::model::Trader>(
    runtime: &mut crate::core::SingleLoop<T>,
    journal: &crate::runtime::Journal,
    action: Action,
    can_trade: bool,
    now: std::time::Instant,
) -> Result<ActionOutcome> {
    let day = runtime.core.trading_day().to_owned();
    let tag = match &action {
        Action::Submit { tag, .. } | Action::Cancel { tag, .. } => *tag,
    };
    if !can_trade || !runtime.execute || !runtime.core.is_ready() {
        return Ok(ActionOutcome::Rejected {
            tag,
            error: "strategy execution gate closed".into(),
        });
    }
    let outcome = match action {
        Action::Submit { tag, intent } => {
            match runtime.submit_planned_with(intent, now, |p| journal.prepared_submit(&day, p)) {
                Ok(receipt) => {
                    let keys: SmallVec<[OrderKey; 2]> =
                        receipt.children.iter().map(|c| c.key.clone()).collect();
                    journal.orders(&runtime.core, &keys, &[])?;
                    ActionOutcome::Submitted { tag, receipt }
                }
                Err(error) => {
                    journal.state(&runtime.core, &[])?;
                    ActionOutcome::Rejected {
                        tag,
                        error: error.to_string(),
                    }
                }
            }
        }
        Action::Cancel { tag, key } => {
            match runtime.cancel_with(&key, now, |p| journal.prepared_cancel(p)) {
                Ok(()) => {
                    journal.orders(&runtime.core, std::slice::from_ref(&key), &[])?;
                    ActionOutcome::CancelledLocally { tag, key }
                }
                Err(error) => ActionOutcome::Rejected {
                    tag,
                    error: error.to_string(),
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
