use std::error::Error;
use std::fmt::{Display, Formatter};

pub const DEFAULT_LOCAL_LEVEL_KALMAN_Q: f64 = 0.02;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct InvalidKalmanQ(pub f64);

impl Display for InvalidKalmanQ {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Kalman q must be finite and non-negative: {}", self.0)
    }
}

impl Error for InvalidKalmanQ {}

/// Causal scalar local-level Kalman filter with observation noise normalized to 1.
///
/// The state follows a random walk. `q` is the process/observation noise ratio Q/R.
/// The first finite observation initializes the level and is returned unchanged.
#[derive(Debug, Clone)]
pub struct LocalLevelKalmanFilter {
    q: f64,
    level: f64,
    variance: f64,
    initialized: bool,
}

impl LocalLevelKalmanFilter {
    pub fn new(q: f64) -> Result<Self, InvalidKalmanQ> {
        validate_q(q)?;
        Ok(Self {
            q,
            level: 0.0,
            variance: 1.0,
            initialized: false,
        })
    }

    pub fn q(&self) -> f64 {
        self.q
    }

    /// Update q without resetting the current level or covariance.
    pub fn set_q(&mut self, q: f64) -> Result<(), InvalidKalmanQ> {
        validate_q(q)?;
        self.q = q;
        Ok(())
    }

    pub fn value(&self) -> Option<f64> {
        self.initialized.then_some(self.level)
    }

    /// Advance the filter by one observation.
    ///
    /// A non-finite observation is treated as missing: no value is emitted, while an
    /// initialized filter advances its uncertainty by one process-noise step.
    pub fn update(&mut self, observation: f64) -> Option<f64> {
        if !observation.is_finite() {
            if self.initialized {
                self.variance += self.q;
            }
            return None;
        }

        if !self.initialized {
            self.level = observation;
            self.variance = 1.0;
            self.initialized = true;
            return Some(self.level);
        }

        let predicted_variance = self.variance + self.q;
        let gain = predicted_variance / (predicted_variance + 1.0);
        self.level += gain * (observation - self.level);
        self.variance = (1.0 - gain) * predicted_variance;
        Some(self.level)
    }
}

fn validate_q(q: f64) -> Result<(), InvalidKalmanQ> {
    if q.is_finite() && q >= 0.0 {
        Ok(())
    } else {
        Err(InvalidKalmanQ(q))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_value_initializes_and_next_value_is_filtered() {
        let mut filter = LocalLevelKalmanFilter::new(0.02).expect("valid q");

        assert_eq!(filter.update(0.5), Some(0.5));
        let second = filter.update(-0.5).expect("filtered value");
        assert!((second - -0.004_950_495_049_504_955).abs() < 1e-12);
        assert_eq!(filter.value(), Some(second));
    }

    #[test]
    fn missing_observation_emits_none_and_increases_next_gain() {
        let mut without_gap = LocalLevelKalmanFilter::new(0.02).expect("valid q");
        let mut with_gap = LocalLevelKalmanFilter::new(0.02).expect("valid q");
        without_gap.update(0.0);
        with_gap.update(0.0);

        let direct = without_gap.update(1.0).expect("direct update");
        assert_eq!(with_gap.update(f64::NAN), None);
        let after_gap = with_gap.update(1.0).expect("update after gap");

        assert!(after_gap > direct);
    }

    #[test]
    fn q_can_change_without_resetting_state() {
        let mut filter = LocalLevelKalmanFilter::new(0.02).expect("valid q");
        filter.update(1.0);
        filter.set_q(0.30).expect("valid replacement q");

        assert_eq!(filter.q(), 0.30);
        assert!(filter.update(0.0).expect("filtered value") < 0.5);
    }

    #[test]
    fn invalid_q_is_rejected() {
        assert!(LocalLevelKalmanFilter::new(-0.01).is_err());
        assert!(LocalLevelKalmanFilter::new(f64::NAN).is_err());
        assert!(LocalLevelKalmanFilter::new(f64::INFINITY).is_err());
    }
}
