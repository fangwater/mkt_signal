use crate::trade_engine::query_request::QueryRequestType;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

const BITGET_QUERY_WINDOW: Duration = Duration::from_secs(1);
const BITGET_ORDER_QUERY_LIMIT_PER_WINDOW: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BitgetQueryLimit {
    max_requests: usize,
    window: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitgetQueryRateBlock {
    pub wait_for: Duration,
    pub queued_in_window: usize,
    pub max_requests: usize,
    pub window: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitgetQueryRateSnapshot {
    pub queued_in_window: usize,
    pub max_requests: usize,
    pub window: Duration,
}

#[derive(Default)]
pub struct BitgetQueryRateLimiter {
    window: VecDeque<Instant>,
}

impl BitgetQueryRateLimiter {
    pub fn should_block(
        &mut self,
        req_type: QueryRequestType,
        now: Instant,
    ) -> Option<BitgetQueryRateBlock> {
        let limit = limit_for_req_type(req_type)?;
        prune_expired(&mut self.window, now, limit.window);
        if self.window.len() < limit.max_requests {
            return None;
        }

        let oldest = self.window.front().copied()?;
        let elapsed = now.checked_duration_since(oldest).unwrap_or_default();
        let wait_for = limit.window.saturating_sub(elapsed);
        if wait_for.is_zero() {
            return None;
        }

        Some(BitgetQueryRateBlock {
            wait_for,
            queued_in_window: self.window.len(),
            max_requests: limit.max_requests,
            window: limit.window,
        })
    }

    pub fn record(
        &mut self,
        req_type: QueryRequestType,
        now: Instant,
    ) -> Option<BitgetQueryRateSnapshot> {
        let limit = limit_for_req_type(req_type)?;
        prune_expired(&mut self.window, now, limit.window);
        self.window.push_back(now);
        Some(BitgetQueryRateSnapshot {
            queued_in_window: self.window.len(),
            max_requests: limit.max_requests,
            window: limit.window,
        })
    }
}

fn limit_for_req_type(req_type: QueryRequestType) -> Option<BitgetQueryLimit> {
    match req_type {
        QueryRequestType::BitgetMarginQuery
        | QueryRequestType::BitgetUMQuery
        | QueryRequestType::BitgetAccountBalanceSnapshot
        | QueryRequestType::BitgetPositionsSnapshot => {
            Some(BitgetQueryLimit {
                max_requests: BITGET_ORDER_QUERY_LIMIT_PER_WINDOW,
                window: BITGET_QUERY_WINDOW,
            })
        }
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
