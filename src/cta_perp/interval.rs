use serde::{Deserialize, Serialize};

/// Bar duration is fixed for a publisher/signal process, including replay.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum BarInterval {
    #[default]
    #[serde(rename = "1m")]
    OneMinute,
    #[serde(rename = "5m")]
    FiveMinutes,
    #[serde(rename = "15m")]
    FifteenMinutes,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fifteen_minute_windows_keep_their_wall_clock_horizons() {
        let interval: BarInterval = serde_json::from_str("\"15m\"").unwrap();
        assert_eq!(interval, BarInterval::FifteenMinutes);
        assert_eq!(interval.millis(), 900_000);
        assert_eq!(interval.factor_quantile_window(), 192);
        assert_eq!(interval.factor_min_samples(), 96);
        assert_eq!(interval.nq_window_samples(), 120);
        assert_eq!(interval.max_model_age_ms(), 1_800_000);
        assert_eq!(serde_json::to_string(&interval).unwrap(), "\"15m\"");
    }

    #[test]
    fn nq_keeps_120_bars_while_factor_ranks_keep_two_days() {
        for (interval, rank_window, rank_min) in [
            (BarInterval::OneMinute, 2880, 1440),
            (BarInterval::FiveMinutes, 576, 288),
            (BarInterval::FifteenMinutes, 192, 96),
        ] {
            assert_eq!(interval.nq_window_samples(), 120);
            assert_eq!(interval.factor_quantile_window(), rank_window);
            assert_eq!(interval.factor_min_samples(), rank_min);
            assert_eq!(rank_window as i64 * interval.millis(), 172_800_000);
        }
    }
}

impl BarInterval {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OneMinute => "1m",
            Self::FiveMinutes => "5m",
            Self::FifteenMinutes => "15m",
        }
    }

    pub fn millis(self) -> i64 {
        match self {
            Self::OneMinute => 60_000,
            Self::FiveMinutes => 300_000,
            Self::FifteenMinutes => 900_000,
        }
    }

    pub fn max_model_age_ms(self) -> u64 {
        (2 * self.millis()) as u64
    }

    pub fn uses_linear_nq(self) -> bool {
        self != Self::OneMinute
    }

    /// NQ extrema and change quantiles use 120 bars at every frequency.
    pub fn nq_window_samples(self) -> usize {
        120
    }

    pub fn factor_quantile_window(self) -> usize {
        (2 * 24 * 60 * 60 * 1000 / self.millis()) as usize
    }

    pub fn factor_min_samples(self) -> usize {
        self.factor_quantile_window() / 2
    }
}
