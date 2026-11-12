//! Admission protection for FR hedges with repeated factual failures.
//! A submitted/acknowledged order is not progress; only a fill or resolved work
//! clears the latch. Intentional waiting and known dust are filtered by caller.

use std::collections::HashMap;

pub(crate) const HEDGE_FAILURE_WINDOW_US: i64 = 30_000_000;
const MIN_FAILURES: u32 = 3;

#[derive(Debug)]
struct FailureStreak {
    first_us: i64,
    last_us: i64,
    failures: u32,
    blocked: bool,
}

#[derive(Debug, Default)]
pub(crate) struct HedgeProgressGuard {
    failures: HashMap<String, FailureStreak>,
}

impl HedgeProgressGuard {
    /// Returns true only on the transition into the blocked state.
    pub fn failure(&mut self, symbol: &str, now_us: i64) -> bool {
        let entry = self
            .failures
            .entry(symbol.to_owned())
            .or_insert(FailureStreak {
                first_us: now_us,
                last_us: now_us,
                failures: 0,
                blocked: false,
            });
        if entry.failures > 0 && now_us <= entry.last_us {
            return false;
        }
        entry.last_us = now_us;
        entry.failures = entry.failures.saturating_add(1);
        if !entry.blocked
            && entry.failures >= MIN_FAILURES
            && now_us.saturating_sub(entry.first_us) >= HEDGE_FAILURE_WINDOW_US
        {
            entry.blocked = true;
            return true;
        }
        false
    }

    pub fn blocked(&self, symbol: &str) -> bool {
        self.failures.get(symbol).is_some_and(|entry| entry.blocked)
    }

    pub fn clear(&mut self, symbol: &str) -> bool {
        self.failures
            .remove(symbol)
            .is_some_and(|entry| entry.blocked)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requires_repeated_failures_over_window_and_does_not_expire_on_its_own() {
        let mut guard = HedgeProgressGuard::default();
        assert!(!guard.failure("ONUSDT", 1));
        assert!(!guard.failure("ONUSDT", 1)); // duplicate observation
        assert!(!guard.failure("ONUSDT", 2));
        assert!(!guard.blocked("ONUSDT"));
        assert!(guard.failure("ONUSDT", 1 + HEDGE_FAILURE_WINDOW_US));
        assert!(!guard.failure("ONUSDT", 1_000_000_000));
        assert!(guard.blocked("ONUSDT"));
        assert!(!guard.blocked("BTCUSDT"));
    }

    #[test]
    fn progress_or_resolved_work_starts_a_new_observation_window() {
        let mut guard = HedgeProgressGuard::default();
        guard.failure("ONUSDT", 1);
        guard.failure("ONUSDT", 10_000_001);
        guard.clear("ONUSDT");
        assert!(!guard.failure("ONUSDT", 30_000_001));
        assert!(!guard.blocked("ONUSDT"));
        guard.failure("ONUSDT", 40_000_001);
        assert!(guard.failure("ONUSDT", 60_000_001));
        assert!(guard.clear("ONUSDT"));
        assert!(!guard.blocked("ONUSDT"));
    }
}
