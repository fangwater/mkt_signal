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

/// Per-contract algorithm state.
pub(crate) enum Algo {
    Ctaker(ctaker::Ctaker),
    Chase(chase::Chase),
}
impl Algo {
    pub fn new(config: &AlgoConfig, retry_us: u64) -> Self {
        match config.algorithm {
            ExecutionAlgorithm::Ctaker => Self::Ctaker(ctaker::Ctaker::new(retry_us)),
            ExecutionAlgorithm::Chase => Self::Chase(chase::Chase::new(&config.chase, retry_us)),
        }
    }

    /// A new target batch: act immediately and restart any maker clock.
    pub fn on_target(&mut self) {
        match self {
            Self::Ctaker(algo) => algo.on_target(),
            Self::Chase(algo) => algo.on_target(),
        }
    }

    /// Returns the stable reason recorded in the trader log.
    pub fn step(&mut self, step: &Step<'_>, out: &mut Commands) -> &'static str {
        match self {
            Self::Ctaker(algo) => algo.step(step, out),
            Self::Chase(algo) => algo.step(step, out),
        }
    }
}

#[cfg(test)]
mod tests;
