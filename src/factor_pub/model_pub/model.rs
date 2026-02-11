use anyhow::{Context, Result};
use xgb::{Booster, DMatrix};

pub struct XgbModel {
    booster: Booster,
    n_features: usize,
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

    pub fn predict_one(&self, features: &[f32]) -> Result<f64> {
        if features.len() != self.n_features {
            anyhow::bail!(
                "feature dim mismatch: got {}, expect {}",
                features.len(),
                self.n_features
            );
        }

        let dmat = DMatrix::from_dense(features, 1)
            .context("build xgboost dmatrix from feature vector failed")?;
        let pred = self
            .booster
            .predict(&dmat)
            .context("xgboost predict failed")?;

        let Some(score) = pred.first() else {
            anyhow::bail!("xgboost predict returned empty result");
        };

        Ok(*score as f64)
    }
}
