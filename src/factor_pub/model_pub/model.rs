use anyhow::{Context, Result};
use std::time::Instant;
use xgb::{Booster, DMatrix};

pub struct XgbModel {
    booster: Booster,
    n_features: usize,
}

pub struct PredictOneOutput {
    pub score: f64,
    pub dmatrix_us: u64,
    pub xgb_predict_us: u64,
}

impl XgbModel {
    pub fn load(model_path: &str, n_features: usize) -> Result<Self> {
        if n_features == 0 {
            anyhow::bail!("n_features must be > 0");
        }

        let booster = Booster::load(model_path)
            .with_context(|| format!("load xgboost model failed: {}", model_path))?;

        Ok(Self {
            booster,
            n_features,
        })
    }

    pub fn load_from_bytes(model_bytes: &[u8], n_features: usize) -> Result<Self> {
        if n_features == 0 {
            anyhow::bail!("n_features must be > 0");
        }
        if model_bytes.is_empty() {
            anyhow::bail!("model bytes must not be empty");
        }

        let booster = Booster::load_buffer(model_bytes)
            .context("load xgboost model from in-memory bytes failed")?;

        Ok(Self {
            booster,
            n_features,
        })
    }

    pub fn predict_one(&self, features: &[f32]) -> Result<f64> {
        let output = self.predict_one_timed(features)?;
        Ok(output.score)
    }

    pub fn predict_one_timed(&self, features: &[f32]) -> Result<PredictOneOutput> {
        if features.len() != self.n_features {
            anyhow::bail!(
                "feature dim mismatch: got {}, expect {}",
                features.len(),
                self.n_features
            );
        }

        let dmatrix_start = Instant::now();
        let dmat = DMatrix::from_dense(features, 1)
            .context("build xgboost dmatrix from feature vector failed")?;
        let dmatrix_us = dmatrix_start.elapsed().as_micros() as u64;

        let xgb_predict_start = Instant::now();
        let pred = self
            .booster
            .predict(&dmat)
            .context("xgboost predict failed")?;
        let xgb_predict_us = xgb_predict_start.elapsed().as_micros() as u64;

        let Some(score) = pred.first() else {
            anyhow::bail!("xgboost predict returned empty result");
        };

        Ok(PredictOneOutput {
            score: *score as f64,
            dmatrix_us,
            xgb_predict_us,
        })
    }
}
