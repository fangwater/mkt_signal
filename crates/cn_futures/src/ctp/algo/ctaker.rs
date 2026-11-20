//! `ctaker`: crosses the whole tradable gap at once, like the crypto ctaker's
//! single market order. Domestic venues have no usable market order, so it is
//! a limit FAK at the opposite touch; one order in flight per contract.
use super::{Command, Commands, Step};
use crate::model::TimeInForce;

pub(crate) struct Ctaker {
    retry_us: u64,
    next_attempt_us: u64,
}
impl Ctaker {
    pub fn new(retry_us: u64) -> Self {
        Self {
            retry_us,
            next_attempt_us: 0,
        }
    }
    pub fn on_target(&mut self) {
        self.next_attempt_us = 0;
    }
    pub fn step(&mut self, s: &Step<'_>, out: &mut Commands) -> &'static str {
        if !s.live.is_empty() {
            return "working_order_unresolved";
        }
        if s.delta == 0 {
            return "target_reached";
        }
        if self.next_attempt_us > s.now_us {
            return "retry_interval";
        }
        if s.tradable == 0 {
            return "lots_below_minimum";
        }
        let side = s.side();
        out.push(Command::Place {
            side,
            lots: s.tradable,
            price_ticks: s.aggressive(side),
            time_in_force: TimeInForce::Fak,
        });
        self.next_attempt_us = s.now_us.saturating_add(self.retry_us);
        "order_action_emitted"
    }
}
