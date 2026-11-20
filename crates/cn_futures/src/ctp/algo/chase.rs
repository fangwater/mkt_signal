//! `chase`: joins the near touch with one Day limit, amends it (cancel then
//! re-place; domestic venues have no amend) once the touch has moved and the
//! order has rested `maker_amend_cooldown_ms`, and after the gap has stayed
//! open `maker_timeout_ms` cancels and crosses with FAK. Parameter names follow
//! the crypto chase exec.
use super::{Command, Commands, LiveOrder, Step};
use crate::model::TimeInForce;
use anyhow::{ensure, Result};
use serde::Deserialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChaseParameters {
    pub maker_amend_cooldown_ms: u64,
    pub maker_timeout_ms: u64,
}
impl Default for ChaseParameters {
    fn default() -> Self {
        Self {
            maker_amend_cooldown_ms: 500,
            maker_timeout_ms: 5_000,
        }
    }
}
impl ChaseParameters {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (100..=60_000).contains(&self.maker_amend_cooldown_ms)
                && (self.maker_amend_cooldown_ms..=3_600_000).contains(&self.maker_timeout_ms),
            "chase requires maker_amend_cooldown_ms 100..60000 and maker_timeout_ms cooldown..3600000"
        );
        Ok(())
    }
}

pub(crate) struct Chase {
    retry_us: u64,
    amend_cooldown_us: u64,
    maker_timeout_us: u64,
    next_attempt_us: u64,
    /// When the current gap was first seen open.
    opened_us: Option<u64>,
    /// When the resting order was last placed.
    placed_us: u64,
}
impl Chase {
    pub fn new(parameters: &ChaseParameters, retry_us: u64) -> Self {
        Self {
            retry_us,
            amend_cooldown_us: parameters.maker_amend_cooldown_ms * 1000,
            maker_timeout_us: parameters.maker_timeout_ms * 1000,
            next_attempt_us: 0,
            opened_us: None,
            placed_us: 0,
        }
    }
    pub fn on_target(&mut self) {
        self.next_attempt_us = 0;
        self.opened_us = None;
    }
    pub fn step(&mut self, s: &Step<'_>, out: &mut Commands) -> &'static str {
        if s.live.iter().any(|o| !o.owned) {
            return "working_order_unresolved";
        }
        if s.delta == 0 {
            self.opened_us = None;
            return cancel_resting(s.live, out);
        }
        let side = s.side();
        let opened = *self.opened_us.get_or_insert(s.now_us);
        let timed_out = s.now_us.saturating_sub(opened) >= self.maker_timeout_us;
        let passive = s.passive(side);
        let mut resting = None;
        for order in s.live {
            if order.cancel_pending {
                return "cancel_pending";
            }
            if !order.resting() {
                return "working_order_unresolved";
            }
            if resting.is_some() || order.side != side || i64::from(order.remaining) > s.delta.abs()
            {
                out.push(Command::Cancel(order.key));
            } else {
                resting = Some(order);
            }
        }
        if !out.is_empty() {
            return "chase_cancel_stale";
        }
        if let Some(order) = resting {
            if timed_out {
                out.push(Command::Cancel(order.key));
                return "chase_maker_timeout";
            }
            if order.price_ticks != passive
                && s.now_us.saturating_sub(self.placed_us) >= self.amend_cooldown_us
            {
                out.push(Command::Cancel(order.key));
                return "chase_amend";
            }
            return "maker_working";
        }
        if self.next_attempt_us > s.now_us {
            return "retry_interval";
        }
        if s.tradable == 0 {
            return "lots_below_minimum";
        }
        let (price_ticks, time_in_force, reason) = if timed_out {
            (s.aggressive(side), TimeInForce::Fak, "chase_taker")
        } else {
            (passive, TimeInForce::Day, "maker_placed")
        };
        out.push(Command::Place {
            side,
            lots: s.tradable,
            price_ticks,
            time_in_force,
        });
        self.placed_us = s.now_us;
        self.next_attempt_us = s.now_us.saturating_add(self.retry_us);
        reason
    }
}

/// Target reached: withdraw resting makers; FAK and terminal reports settle alone.
fn cancel_resting(live: &[LiveOrder], out: &mut Commands) -> &'static str {
    let mut waiting = false;
    for order in live {
        if order.cancel_pending || !order.resting() {
            waiting = true;
        } else {
            out.push(Command::Cancel(order.key));
        }
    }
    if !out.is_empty() {
        "chase_cancel_reached"
    } else if waiting {
        "working_order_unresolved"
    } else {
        "target_reached"
    }
}
