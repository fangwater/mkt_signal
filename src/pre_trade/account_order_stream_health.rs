use log::{error, warn};
use mkt_parsers::msg::basic_account_msg::BasicAccountScope;
use runtime_common::time_util::get_timestamp_us;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

const BINANCE_ORDER_DECODE_ERROR_THRESHOLD: u64 = 1;
const DECODE_ERROR_LOG_INTERVAL_US: i64 = 30_000_000;

static ACCOUNT_ORDER_STREAM_HEALTHY: AtomicBool = AtomicBool::new(true);
static CONSECUTIVE_DECODE_ERRORS: AtomicU64 = AtomicU64::new(0);
static TOTAL_DECODE_ERRORS: AtomicU64 = AtomicU64::new(0);
static LAST_DECODE_ERROR_LOG_US: AtomicI64 = AtomicI64::new(0);

pub fn is_healthy() -> bool {
    ACCOUNT_ORDER_STREAM_HEALTHY.load(Ordering::Acquire)
}

pub fn record_binance_decode_success() {
    if is_healthy() {
        CONSECUTIVE_DECODE_ERRORS.store(0, Ordering::Release);
    }
}

pub fn record_binance_decode_failure(
    account_scope: BasicAccountScope,
    payload_len: usize,
    decode_error: &anyhow::Error,
) {
    let consecutive = CONSECUTIVE_DECODE_ERRORS.fetch_add(1, Ordering::AcqRel) + 1;
    let total = TOTAL_DECODE_ERRORS.fetch_add(1, Ordering::Relaxed) + 1;
    let now_us = get_timestamp_us();
    let should_log = consecutive <= BINANCE_ORDER_DECODE_ERROR_THRESHOLD
        || now_us.saturating_sub(LAST_DECODE_ERROR_LOG_US.load(Ordering::Relaxed))
            >= DECODE_ERROR_LOG_INTERVAL_US;
    if should_log {
        LAST_DECODE_ERROR_LOG_US.store(now_us, Ordering::Relaxed);
        warn!(
            "Binance account OrderUpdate decode failed: scope={} payload_len={} consecutive={} account_order_decode_errors_total={} error={:#}",
            account_scope.as_str(),
            payload_len,
            consecutive,
            total,
            decode_error
        );
    }

    if should_trip_binance_order_stream(consecutive)
        && ACCOUNT_ORDER_STREAM_HEALTHY.swap(false, Ordering::AcqRel)
    {
        error!(
            "Binance account order stream marked unhealthy after {} consecutive decode errors; new ArbOpen/ArbClose/MMOpen signals are blocked while cancel, query, and hedge processing remain enabled",
            consecutive
        );
    }
}

pub(crate) fn should_trip_binance_order_stream(consecutive_errors: u64) -> bool {
    consecutive_errors >= BINANCE_ORDER_DECODE_ERROR_THRESHOLD
}

#[cfg(test)]
mod tests {
    use super::should_trip_binance_order_stream;

    #[test]
    fn trips_on_first_decode_error() {
        assert!(!should_trip_binance_order_stream(0));
        assert!(should_trip_binance_order_stream(1));
    }
}
