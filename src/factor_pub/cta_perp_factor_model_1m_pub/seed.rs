//! Offline warm-up uses the existing trade-flow payload and full-book metrics.
use crate::cta_perp::interval::BarInterval;
use crate::factor_pub::depth_book::DepthBookBar;
use anyhow::{ensure, Context, Result};
use bincode::Options;
use mkt_parsers::msg::trade_flow_feature_msg::TradeFlowFeatureMsg;
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

#[derive(Serialize, Deserialize)]
pub struct BaselineSeed {
    pub bar_interval: BarInterval,
    pub symbol: String,
    pub rows: Vec<BaselineSeedRow>,
}

#[derive(Serialize, Deserialize)]
pub struct BaselineSeedRow {
    pub payload: Vec<u8>,
    pub depth: Option<DepthBookBar>,
}

impl BaselineSeed {
    pub fn replace(&self, path: &Path) -> Result<()> {
        let bytes = bincode::DefaultOptions::new().serialize(self)?;
        let temporary = path.with_extension("seed.next");
        fs::write(&temporary, bytes)?;
        fs::rename(temporary, path)?;
        Ok(())
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        ensure!(
            !path.exists(),
            "baseline seed already exists: {}",
            path.display()
        );
        self.replace(path)
    }

    pub fn read(
        path: &Path,
        interval: BarInterval,
        symbol: &str,
        venue: u8,
        now: i64,
    ) -> Result<Self> {
        ensure!(
            fs::metadata(path)?.len() < 64 * 1024 * 1024,
            "oversized baseline seed"
        );
        let seed: Self = bincode::DefaultOptions::new()
            .with_limit(64 * 1024 * 1024)
            .deserialize(&fs::read(path)?)
            .with_context(|| format!("decode baseline seed {}", path.display()))?;
        ensure!(
            seed.bar_interval == interval && seed.symbol == symbol,
            "baseline seed scope mismatch"
        );
        ensure!(!seed.rows.is_empty(), "empty baseline seed for {symbol}");
        let mut previous = None;
        for row in &seed.rows {
            let msg = TradeFlowFeatureMsg::from_bytes(&row.payload)?;
            ensure!(
                msg.symbol == symbol && msg.venue == venue && msg.values.len() == 112,
                "baseline seed message identity/width mismatch"
            );
            ensure!(
                msg.ts > 0 && msg.ts <= now && msg.ts % interval.millis() == 0,
                "baseline seed contains an unfinished or misaligned bar"
            );
            ensure!(
                previous.is_none_or(|ts| msg.ts == ts + interval.millis()),
                "noncontiguous baseline seed for {symbol}"
            );
            previous = Some(msg.ts);
        }
        Ok(seed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn seed_preserves_missing_values_and_rejects_gaps_and_wrong_scope() {
        let root = std::env::temp_dir().join(format!("cta-seed-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        let path = root.join("BTCUSDT.seed");
        let row = |ts| BaselineSeedRow {
            payload: TradeFlowFeatureMsg::from_indexed_values(
                "BTCUSDT".into(),
                1,
                ts,
                &[f64::NAN; 112],
            )
            .unwrap()
            .to_bytes()
            .unwrap()
            .to_vec(),
            depth: Some(DepthBookBar::default()),
        };
        let mut seed = BaselineSeed {
            bar_interval: BarInterval::FifteenMinutes,
            symbol: "BTCUSDT".into(),
            rows: vec![row(900000), row(1800000)],
        };
        seed.write(&path).unwrap();
        let got = BaselineSeed::read(&path, seed.bar_interval, "BTCUSDT", 1, 1800000).unwrap();
        assert!(TradeFlowFeatureMsg::from_bytes(&got.rows[0].payload)
            .unwrap()
            .values[0]
            .is_nan());
        assert!(BaselineSeed::read(&path, seed.bar_interval, "ETHUSDT", 1, 1800000).is_err());
        assert!(BaselineSeed::read(&path, seed.bar_interval, "BTCUSDT", 1, 900000).is_err());
        fs::remove_file(&path).unwrap();
        seed.rows.push(row(3600000));
        seed.write(&path).unwrap();
        assert!(BaselineSeed::read(&path, seed.bar_interval, "BTCUSDT", 1, 3600000).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
