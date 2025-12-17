//! Decision router for trade_signal.
//!
//! Dispatches event-driven decision triggers to either `FrDecision` (single-venue FR)
//! or `XarbDecision` (cross-venue xarb), based on a startup-selected branch.

use anyhow::Result;
use log::warn;
use std::cell::OnceCell;

use crate::signal::common::TradingVenue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionBranch {
    Fr,
    Xarb,
}

thread_local! {
    static DECISION_BRANCH: OnceCell<DecisionBranch> = OnceCell::new();
}

pub fn init_decision_branch(branch: DecisionBranch) -> Result<()> {
    DECISION_BRANCH.with(|cell| {
        if cell.get().is_some() {
            return Ok(());
        }
        cell.set(branch)
            .map_err(|_| anyhow::anyhow!("DecisionBranch already initialized (race)"))?;
        Ok(())
    })
}

pub fn decision_branch() -> Option<DecisionBranch> {
    DECISION_BRANCH.with(|cell| cell.get().copied())
}

pub fn trigger_decision(
    open_symbol: &str,
    hedge_symbol: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) {
    let Some(branch) = decision_branch() else {
        warn!("DecisionBranch not initialized; skipping decision trigger");
        return;
    };

    match branch {
        DecisionBranch::Fr => {
            use super::fr_decision::FrDecision;
            FrDecision::with_mut(|decision| {
                let _ = decision.make_combined_decision(
                    open_symbol,
                    hedge_symbol,
                    open_venue,
                    hedge_venue,
                );
            });
        }
        DecisionBranch::Xarb => {
            use super::xarb_decision::XarbDecision;
            XarbDecision::with_mut(|decision| {
                let _ = decision.make_spread_only_decision(
                    open_symbol,
                    hedge_symbol,
                    open_venue,
                    hedge_venue,
                );
            });
        }
    }
}
