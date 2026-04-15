use crate::trade_engine::query_request::QueryRequestType;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

const OKEX_QUERY_WINDOW: Duration = Duration::from_secs(2);
const OKEX_ORDER_QUERY_LIMIT_PER_WINDOW: usize = 60;
const OKEX_ACCOUNT_BALANCE_QUERY_LIMIT_PER_WINDOW: usize = 10;
const OKEX_POSITIONS_QUERY_LIMIT_PER_WINDOW: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OkexQueryLimit {
    max_requests: usize,
    window: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OkexQueryRateBlock {
    pub wait_for: Duration,
    pub queued_in_window: usize,
    pub max_requests: usize,
    pub window: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OkexQueryRateSnapshot {
    pub queued_in_window: usize,
    pub max_requests: usize,
    pub window: Duration,
}

#[derive(Default)]
pub struct OkexQueryRateLimiter {
    windows: HashMap<u32, VecDeque<Instant>>,
}

impl OkexQueryRateLimiter {
    pub fn should_block(
        &mut self,
        req_type: QueryRequestType,
        now: Instant,
    ) -> Option<OkexQueryRateBlock> {
        let limit = limit_for_req_type(req_type)?;
        let queue = self.windows.entry(req_type as u32).or_default();
        prune_expired(queue, now, limit.window);
        if queue.len() < limit.max_requests {
            return None;
        }

        let oldest = queue.front().copied()?;
        let elapsed = now.checked_duration_since(oldest).unwrap_or_default();
        let wait_for = limit.window.saturating_sub(elapsed);
        if wait_for.is_zero() {
            return None;
        }

        Some(OkexQueryRateBlock {
            wait_for,
            queued_in_window: queue.len(),
            max_requests: limit.max_requests,
            window: limit.window,
        })
    }

    pub fn record(
        &mut self,
        req_type: QueryRequestType,
        now: Instant,
    ) -> Option<OkexQueryRateSnapshot> {
        let limit = limit_for_req_type(req_type)?;
        let queue = self.windows.entry(req_type as u32).or_default();
        prune_expired(queue, now, limit.window);
        queue.push_back(now);
        Some(OkexQueryRateSnapshot {
            queued_in_window: queue.len(),
            max_requests: limit.max_requests,
            window: limit.window,
        })
    }
}

fn limit_for_req_type(req_type: QueryRequestType) -> Option<OkexQueryLimit> {
    match req_type {
        QueryRequestType::OkexMarginQuery | QueryRequestType::OkexUMQuery => Some(OkexQueryLimit {
            max_requests: OKEX_ORDER_QUERY_LIMIT_PER_WINDOW,
            window: OKEX_QUERY_WINDOW,
        }),
        QueryRequestType::OkexAccountBalanceSnapshot => Some(OkexQueryLimit {
            max_requests: OKEX_ACCOUNT_BALANCE_QUERY_LIMIT_PER_WINDOW,
            window: OKEX_QUERY_WINDOW,
        }),
        QueryRequestType::OkexPositionsSnapshot => Some(OkexQueryLimit {
            max_requests: OKEX_POSITIONS_QUERY_LIMIT_PER_WINDOW,
            window: OKEX_QUERY_WINDOW,
        }),
        _ => None,
    }
}

fn prune_expired(queue: &mut VecDeque<Instant>, now: Instant, window: Duration) {
    while let Some(oldest) = queue.front().copied() {
        let elapsed = now.checked_duration_since(oldest).unwrap_or_default();
        if elapsed < window {
            break;
        }
        queue.pop_front();
    }
}

#[cfg(test)]
mod tests {
    use super::OkexQueryRateLimiter;
    use crate::trade_engine::query_request::QueryRequestType;
    use std::time::{Duration, Instant};

    #[test]
    fn order_query_blocks_after_sixty_requests_in_two_seconds() {
        let mut limiter = OkexQueryRateLimiter::default();
        let base = Instant::now();
        for _ in 0..60 {
            limiter.record(QueryRequestType::OkexUMQuery, base);
        }

        let block = limiter
            .should_block(QueryRequestType::OkexUMQuery, base)
            .expect("must block after hitting the window limit");
        assert_eq!(block.queued_in_window, 60);
        assert_eq!(block.max_requests, 60);
        assert_eq!(block.window, Duration::from_secs(2));
    }

    #[test]
    fn different_query_types_have_independent_windows() {
        let mut limiter = OkexQueryRateLimiter::default();
        let base = Instant::now();
        for _ in 0..60 {
            limiter.record(QueryRequestType::OkexUMQuery, base);
        }

        assert!(limiter
            .should_block(QueryRequestType::OkexUMQuery, base)
            .is_some());
        assert!(limiter
            .should_block(QueryRequestType::OkexAccountBalanceSnapshot, base)
            .is_none());
    }

    #[test]
    fn expired_requests_are_pruned_on_next_check() {
        let mut limiter = OkexQueryRateLimiter::default();
        let base = Instant::now();
        for _ in 0..10 {
            limiter.record(QueryRequestType::OkexAccountBalanceSnapshot, base);
        }

        assert!(limiter
            .should_block(QueryRequestType::OkexAccountBalanceSnapshot, base)
            .is_some());
        assert!(limiter
            .should_block(
                QueryRequestType::OkexAccountBalanceSnapshot,
                base + Duration::from_secs(2),
            )
            .is_none());
    }
}
