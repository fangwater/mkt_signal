//! MM decision skeleton.
//!
//! This module only provides routing framework for `namespace=mm`.
//! Strategy details will be filled in later.

use anyhow::Result;
use log::info;
use std::cell::{OnceCell, RefCell};
use std::collections::HashMap;

use crate::common::time_util::get_timestamp_us;
use crate::signal::common::TradingVenue;
use crate::signal::trade_signal::SignalType;
use crate::symbol_match::normalize_symbol_for_whitelist;

thread_local! {
    static MM_DECISION: OnceCell<RefCell<MmDecision>> = OnceCell::new();
}

pub struct MmDecision {
    open_venue: TradingVenue,
    hedge_venue: TradingVenue,
    order_interval_ms: u64,
    last_report_ts_us: HashMap<String, i64>,
}

impl MmDecision {
    pub fn is_initialized() -> bool {
        MM_DECISION.with(|cell| cell.get().is_some())
    }

    pub fn with<F, R>(f: F) -> R
    where
        F: FnOnce(&MmDecision) -> R,
    {
        MM_DECISION.with(|cell| {
            let decision_ref = cell
                .get()
                .expect("MmDecision not initialized. Call init_singleton() first");
            f(&decision_ref.borrow())
        })
    }

    pub fn with_mut<F, R>(f: F) -> R
    where
        F: FnOnce(&mut MmDecision) -> R,
    {
        MM_DECISION.with(|cell| {
            let decision_ref = cell
                .get()
                .expect("MmDecision not initialized. Call init_singleton() first");
            f(&mut decision_ref.borrow_mut())
        })
    }

    pub fn try_with_mut<F, R>(f: F) -> Option<R>
    where
        F: FnOnce(&mut MmDecision) -> R,
    {
        MM_DECISION.with(|cell| {
            let decision_ref = cell.get()?;
            Some(f(&mut decision_ref.borrow_mut()))
        })
    }

    pub async fn init_singleton(open_venue: TradingVenue, hedge_venue: TradingVenue) -> Result<()> {
        MM_DECISION.with(|cell| {
            if cell.get().is_some() {
                return Ok(());
            }
            let decision = MmDecision {
                open_venue,
                hedge_venue,
                order_interval_ms: 5_000,
                last_report_ts_us: HashMap::new(),
            };
            cell.set(RefCell::new(decision))
                .map_err(|_| anyhow::anyhow!("Failed to initialize MmDecision singleton"))?;
            info!(
                "MmDecision singleton initialized, open={:?} hedge={:?}",
                open_venue, hedge_venue
            );
            Ok(())
        })
    }

    pub fn update_order_interval_ms(&mut self, interval_ms: u64) {
        if interval_ms == 0 {
            panic!("MmDecision: order_interval_ms must be > 0");
        }
        self.order_interval_ms = interval_ms;
        info!(
            "MmDecision: order interval updated interval_ms={}",
            self.order_interval_ms
        );
    }

    /// Framework hook for market-making decision path.
    /// Triggering is time-based (per symbol): emit once per configured interval.
    /// Current skeleton does not publish signal payload yet.
    pub fn make_mm_decision(
        &mut self,
        open_symbol: &str,
        hedge_symbol: &str,
        open_venue: TradingVenue,
        hedge_venue: TradingVenue,
    ) -> Result<Option<SignalType>> {
        let _ = hedge_symbol;
        if open_venue != self.open_venue || hedge_venue != self.hedge_venue {
            return Ok(None);
        }

        let now_us = get_timestamp_us();
        let interval_us = (self.order_interval_ms as i64).saturating_mul(1_000);
        let symbol_key = normalize_symbol_for_whitelist(open_symbol, TradingVenue::OkexFutures);
        let last_ts = self
            .last_report_ts_us
            .get(&symbol_key)
            .copied()
            .unwrap_or(0);
        if last_ts > 0 && now_us.saturating_sub(last_ts) < interval_us {
            return Ok(None);
        }

        self.last_report_ts_us.insert(symbol_key.clone(), now_us);
        info!(
            "MmDecision: interval trigger symbol={} now_us={} interval_ms={}",
            symbol_key, now_us, self.order_interval_ms
        );
        Ok(None)
    }
}
