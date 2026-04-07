#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AmountThreshold {
    pub medium_notional_threshold: f64,
    pub large_notional_threshold: f64,
}

impl AmountThreshold {
    pub fn is_online(&self) -> bool {
        is_online_amount_threshold(
            self.medium_notional_threshold,
            self.large_notional_threshold,
        )
    }
}

pub fn is_online_amount_threshold(
    medium_notional_threshold: f64,
    large_notional_threshold: f64,
) -> bool {
    medium_notional_threshold.is_finite()
        && large_notional_threshold.is_finite()
        && medium_notional_threshold > 0.0
        && large_notional_threshold > 0.0
        && medium_notional_threshold <= large_notional_threshold
}

#[cfg(test)]
mod tests {
    use super::{is_online_amount_threshold, AmountThreshold};

    #[test]
    fn online_threshold_requires_finite_positive_ordered_values() {
        assert!(is_online_amount_threshold(10.0, 20.0));
        assert!(is_online_amount_threshold(10.0, 10.0));

        assert!(!is_online_amount_threshold(0.0, 10.0));
        assert!(!is_online_amount_threshold(10.0, 0.0));
        assert!(!is_online_amount_threshold(20.0, 10.0));
        assert!(!is_online_amount_threshold(f64::NAN, 10.0));
        assert!(!is_online_amount_threshold(10.0, f64::INFINITY));
    }

    #[test]
    fn amount_threshold_struct_uses_same_online_rule() {
        assert!(AmountThreshold {
            medium_notional_threshold: 1.0,
            large_notional_threshold: 2.0,
        }
        .is_online());

        assert!(!AmountThreshold {
            medium_notional_threshold: 3.0,
            large_notional_threshold: 2.0,
        }
        .is_online());
    }
}
