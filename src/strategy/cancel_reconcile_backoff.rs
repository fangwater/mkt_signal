pub const CANCEL_RECONCILE_QUERY_DELAYS_US: [i64; 3] = [300_000, 600_000, 1_200_000];

pub fn cancel_reconcile_query_max_attempts() -> u8 {
    CANCEL_RECONCILE_QUERY_DELAYS_US.len() as u8
}

pub fn cancel_reconcile_query_delay_us(sent_attempts: u8) -> i64 {
    CANCEL_RECONCILE_QUERY_DELAYS_US
        .get(sent_attempts as usize)
        .copied()
        .unwrap_or(*CANCEL_RECONCILE_QUERY_DELAYS_US.last().unwrap())
}

pub fn cancel_reconcile_attempts_exhausted(sent_attempts: u8) -> bool {
    sent_attempts >= cancel_reconcile_query_max_attempts()
}

#[cfg(test)]
mod tests {
    use super::{
        cancel_reconcile_attempts_exhausted, cancel_reconcile_query_delay_us,
        cancel_reconcile_query_max_attempts,
    };

    #[test]
    fn cancel_reconcile_backoff_keeps_existing_short_progression() {
        assert_eq!(cancel_reconcile_query_max_attempts(), 3);
        assert_eq!(cancel_reconcile_query_delay_us(0), 300_000);
        assert_eq!(cancel_reconcile_query_delay_us(1), 600_000);
        assert_eq!(cancel_reconcile_query_delay_us(2), 1_200_000);
        assert_eq!(cancel_reconcile_query_delay_us(3), 1_200_000);
        assert!(!cancel_reconcile_attempts_exhausted(2));
        assert!(cancel_reconcile_attempts_exhausted(3));
    }
}
