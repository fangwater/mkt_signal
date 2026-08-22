pub const INIT_TP_MS: i64 = 1_704_067_200_000;
pub const DEFAULT_PERIOD_MS: i64 = 3_000;
pub const DEFAULT_DELAY_MS: i64 = 5;

#[inline]
pub fn normalize_timestamp_ms(timestamp: i64) -> i64 {
    if timestamp >= 10_000_000_000_000 {
        timestamp / 1_000
    } else {
        timestamp
    }
}

#[inline]
pub fn period_for_timestamp_ms(timestamp_ms: i64, period_ms: i64) -> i64 {
    // Match the Kafka PeriodMessage reference observed on the spread_pbs path:
    // exact upper-bound timestamps belong to the closing period.
    (timestamp_ms - INIT_TP_MS - 1).div_euclid(period_ms)
}

#[inline]
pub fn period_lower_bound_ms(period: i64, period_ms: i64) -> i64 {
    INIT_TP_MS + period * period_ms
}

#[inline]
pub fn period_upper_bound_ms(period: i64, period_ms: i64) -> i64 {
    INIT_TP_MS + (period + 1) * period_ms
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn period_boundaries_match_cpp_epoch() {
        assert_eq!(
            period_for_timestamp_ms(INIT_TP_MS + 1, DEFAULT_PERIOD_MS),
            0
        );
        assert_eq!(
            period_for_timestamp_ms(INIT_TP_MS + DEFAULT_PERIOD_MS - 1, DEFAULT_PERIOD_MS),
            0
        );
        assert_eq!(
            period_for_timestamp_ms(INIT_TP_MS + DEFAULT_PERIOD_MS, DEFAULT_PERIOD_MS),
            0
        );
        assert_eq!(
            period_upper_bound_ms(0, DEFAULT_PERIOD_MS),
            INIT_TP_MS + 3_000
        );
    }

    #[test]
    fn upper_bound_timestamp_belongs_to_closing_period() {
        let period = 42;
        let upper = period_upper_bound_ms(period, DEFAULT_PERIOD_MS);
        assert_eq!(
            period_for_timestamp_ms(upper - 1, DEFAULT_PERIOD_MS),
            period
        );
        assert_eq!(period_for_timestamp_ms(upper, DEFAULT_PERIOD_MS), period);
        assert_eq!(
            period_for_timestamp_ms(upper + 1, DEFAULT_PERIOD_MS),
            period + 1
        );
    }

    #[test]
    fn normalizes_microseconds_to_milliseconds() {
        assert_eq!(
            normalize_timestamp_ms(1_704_067_200_123_456),
            1_704_067_200_123
        );
        assert_eq!(normalize_timestamp_ms(1_704_067_200_123), 1_704_067_200_123);
    }
}
