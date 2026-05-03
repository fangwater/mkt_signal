//! Decision router for trade_signal.
//!
//! Dispatches event-driven decision triggers to one of:
//! - `ArbDecision` (mode-driven arb core)
//! - `MmDecision` (market-making framework)
//! based on a startup-selected branch.

use anyhow::Result;
use log::{info, warn};
use std::cell::{OnceCell, RefCell};
use std::time::{Duration, Instant};

use crate::funding_rate::RateFetcher;
use crate::funding_rate::{ArbDecision, ArbMode};
use crate::signal::common::TradingVenue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionBranch {
    Arb,
    Mm,
}

thread_local! {
    static DECISION_BRANCH: OnceCell<DecisionBranch> = const { OnceCell::new() };
    static DECISION_SKIP_NOT_READY: RefCell<DecisionSkipNotReadyStats> =
        RefCell::new(DecisionSkipNotReadyStats::default());
}

#[derive(Debug)]
struct DecisionSkipNotReadyStats {
    last_log: Instant,
    skipped: u64,
    last_open_symbol: String,
    last_hedge_symbol: String,
    last_open_venue: Option<TradingVenue>,
    last_hedge_venue: Option<TradingVenue>,
    last_detail: String,
}

impl Default for DecisionSkipNotReadyStats {
    fn default() -> Self {
        Self {
            last_log: Instant::now(),
            skipped: 0,
            last_open_symbol: String::new(),
            last_hedge_symbol: String::new(),
            last_open_venue: None,
            last_hedge_venue: None,
            last_detail: String::new(),
        }
    }
}

fn log_skip_not_ready(
    open_symbol: &str,
    hedge_symbol: &str,
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
) {
    DECISION_SKIP_NOT_READY.with(|cell| {
        let mut stats = cell.borrow_mut();
        stats.skipped += 1;
        stats.last_open_symbol.clear();
        stats.last_open_symbol.push_str(open_symbol);
        stats.last_hedge_symbol.clear();
        stats.last_hedge_symbol.push_str(hedge_symbol);
        stats.last_open_venue = Some(open_venue);
        stats.last_hedge_venue = Some(hedge_venue);
        stats.last_detail = RateFetcher::not_ready_detail(hedge_venue).unwrap_or_else(|| {
            "reason=unknown symbol=-".to_string()
        });

        if stats.last_log.elapsed() >= Duration::from_secs(10) {
            info!(
                "DecisionRouter: degraded mode (RateFetcher not fully ready) count={} last_open_symbol={} last_hedge_symbol={} open_venue={:?} hedge_venue={:?} {}",
                stats.skipped,
                stats.last_open_symbol,
                stats.last_hedge_symbol,
                stats.last_open_venue,
                stats.last_hedge_venue,
                stats.last_detail
            );
            stats.last_log = Instant::now();
            stats.skipped = 0;
        }
    });
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
        DecisionBranch::Arb => {
            let mode = ArbDecision::mode();
            if matches!(mode, Some(ArbMode::FundingArb))
                && !RateFetcher::is_initial_ready(hedge_venue)
            {
                log_skip_not_ready(open_symbol, hedge_symbol, open_venue, hedge_venue);
            }
            ArbDecision::trigger_decision(open_symbol, hedge_symbol, open_venue, hedge_venue);
        }
        DecisionBranch::Mm => {
            let _ = (open_symbol, hedge_symbol, open_venue, hedge_venue);
        }
    }
}
