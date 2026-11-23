//! Execution algorithms below [`super::exec`], named and parameterized like the
//! crypto exec `ExecutionAlgorithm` (`ctaker`, `chase`; `grid`, `batch` and
//! `pov` fit the same interface). Exec decides *what* the account still has to
//! trade on one contract (the net gap and how many lots one order may carry);
//! an algorithm decides *how*: price, time in force, cancel and replace. It
//! reads the contract's live OMS orders, never a predicted position, and emits
//! bounded commands.
mod chase;
mod ctaker;

use crate::model::{OrderKey, Side, TimeInForce};
use anyhow::Result;
use serde::Deserialize;
use smallvec::SmallVec;

pub use chase::ChaseParameters;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionAlgorithm {
    /// Limit FAK at the opposite touch for the whole tradable gap.
    #[default]
    Ctaker,
    /// Maker at the near touch, amended as the touch moves, crossing after
    /// `maker_timeout_ms`.
    Chase,
}
impl ExecutionAlgorithm {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ctaker => "ctaker",
            Self::Chase => "chase",
        }
    }
}

/// Algorithm selection and parameters of one Exec instance.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct AlgoConfig {
    pub algorithm: ExecutionAlgorithm,
    pub chase: ChaseParameters,
}
impl AlgoConfig {
    pub fn validate(&self) -> Result<()> {
        match self.algorithm {
            ExecutionAlgorithm::Ctaker => Ok(()),
            ExecutionAlgorithm::Chase => self.chase.validate(),
        }
    }
}

/// One live order of the contract as the OMS sees it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LiveOrder {
    pub key: OrderKey,
    pub side: Side,
    pub price_ticks: i64,
    pub remaining: i32,
    pub time_in_force: TimeInForce,
    /// Terminal report waiting for its factual fills; nothing left to cancel.
    pub terminal: bool,
    pub cancel_pending: bool,
    /// Placed by this process; foreign orders are never touched.
    pub owned: bool,
}
impl LiveOrder {
    /// FAK/FOK and terminal reports resolve on their own.
    fn resting(&self) -> bool {
        self.time_in_force == TimeInForce::Day && !self.terminal
    }
}

pub(crate) const MAX_LIVE: usize = 8;
pub(crate) type LiveOrders = SmallVec<[LiveOrder; MAX_LIVE]>;

/// Inputs of one decision; prices are validated, positive tick counts.
pub(crate) struct Step<'a> {
    /// Target net minus account net, signed lots.
    pub delta: i64,
    /// Lots one new order on the gap's side may carry now (risk, inventory,
    /// rule bounds already applied); zero when below the contract minimum.
    pub tradable: i64,
    pub bid_ticks: i64,
    pub ask_ticks: i64,
    pub live: &'a [LiveOrder],
    pub now_us: u64,
}
impl Step<'_> {
    fn side(&self) -> Side {
        if self.delta > 0 {
            Side::Buy
        } else {
            Side::Sell
        }
    }
    /// Opposite touch: crosses the spread.
    fn aggressive(&self, side: Side) -> i64 {
        match side {
            Side::Buy => self.ask_ticks,
            Side::Sell => self.bid_ticks,
        }
    }
    /// Near touch: joins the queue.
    fn passive(&self, side: Side) -> i64 {
        match side {
            Side::Buy => self.bid_ticks,
            Side::Sell => self.ask_ticks,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Command {
    Place {
        side: Side,
        lots: i64,
        price_ticks: i64,
        time_in_force: TimeInForce,
    },
    Cancel(OrderKey),
}
pub(crate) type Commands = SmallVec<[Command; 4]>;

/// Longest wait after repeated venue rejections of a contract's orders.
const MAX_REJECTION_BACKOFF_US: u64 = 300_000_000;

/// Per-contract algorithm state.
pub(crate) struct Algo {
    inner: Inner,
    retry_us: u64,
    /// Doubles from `retry_us` with each venue rejection, up to
    /// [`MAX_REJECTION_BACKOFF_US`]; a fill or a changed target clears it.
    rejection_backoff_us: u64,
    blocked_until_us: u64,
}
enum Inner {
    Ctaker(ctaker::Ctaker),
    Chase(chase::Chase),
}
impl Algo {
    pub fn new(config: &AlgoConfig, retry_us: u64) -> Self {
        Self {
            inner: match config.algorithm {
                ExecutionAlgorithm::Ctaker => Inner::Ctaker(ctaker::Ctaker::new(retry_us)),
                ExecutionAlgorithm::Chase => {
                    Inner::Chase(chase::Chase::new(&config.chase, retry_us))
                }
            },
            retry_us,
            rejection_backoff_us: 0,
            blocked_until_us: 0,
        }
    }

    /// The contract's target changed: act immediately and restart any maker clock.
    pub fn on_target(&mut self) {
        self.clear_backoff();
        match &mut self.inner {
            Inner::Ctaker(algo) => algo.on_target(),
            Inner::Chase(algo) => algo.on_target(),
        }
    }

    /// The venue or counter rejected one of this contract's orders. A repeated
    /// rejection must not turn into an order every retry interval.
    pub fn on_rejected(&mut self, now_us: u64) {
        self.rejection_backoff_us = if self.rejection_backoff_us == 0 {
            self.retry_us
        } else {
            (self.rejection_backoff_us * 2).min(MAX_REJECTION_BACKOFF_US)
        };
        self.blocked_until_us = now_us.saturating_add(self.rejection_backoff_us);
    }

    pub fn on_fill(&mut self) {
        self.clear_backoff();
    }

    fn clear_backoff(&mut self) {
        self.rejection_backoff_us = 0;
        self.blocked_until_us = 0;
    }

    /// Returns the stable reason recorded in the trader log.
    pub fn step(&mut self, step: &Step<'_>, out: &mut Commands) -> &'static str {
        if step.delta != 0 && step.live.is_empty() && step.now_us < self.blocked_until_us {
            return "rejection_backoff";
        }
        match &mut self.inner {
            Inner::Ctaker(algo) => algo.step(step, out),
            Inner::Chase(algo) => algo.step(step, out),
        }
    }
}

#[cfg(test)]
mod tests;
