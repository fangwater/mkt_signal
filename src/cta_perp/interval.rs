use serde::{Deserialize, Serialize};

/// Bar duration is fixed for a publisher/signal process, including replay.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum BarInterval {
    #[default]
    #[serde(rename = "1m")]
    OneMinute,
    #[serde(rename = "5m")]
    FiveMinutes,
}

impl BarInterval {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OneMinute => "1m",
            Self::FiveMinutes => "5m",
        }
    }

    pub fn millis(self) -> i64 {
        match self {
            Self::OneMinute => 60_000,
            Self::FiveMinutes => 300_000,
        }
    }

    pub fn max_model_age_ms(self) -> u64 {
        (2 * self.millis()) as u64
    }

    pub fn factor_quantile_window(self) -> usize {
        (2 * 24 * 60 * 60 * 1000 / self.millis()) as usize
    }

    pub fn factor_min_samples(self) -> usize {
        self.factor_quantile_window() / 2
    }
}
