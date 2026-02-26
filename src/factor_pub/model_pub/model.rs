use anyhow::{Context, Result};
use std::path::Path;
use std::time::Instant;
use tract_onnx::prelude::*;

type OnnxRunnable = SimplePlan<TypedFact, Box<dyn TypedOp>, TypedModel>;

pub struct OnnxModel {
    model: OnnxRunnable,
    n_features: usize,
}

pub struct PredictOneOutput {
    pub score: f64,
    pub marshal_us: u64,
    pub onnx_predict_us: u64,
}

impl OnnxModel {
    pub fn load_from_path(path: &Path, expected_features: Option<usize>) -> Result<Self> {
        let n_features = expected_features.ok_or_else(|| {
            anyhow::anyhow!(
                "expected feature dim is required for ONNX model loading: path={}",
                path.display()
            )
        })?;
        if n_features == 0 {
            anyhow::bail!("invalid ONNX model feature dim=0: {}", path.display());
        }

        let model = tract_onnx::onnx()
            .model_for_path(path)
            .with_context(|| format!("load ONNX model failed: {}", path.display()))?
            .with_input_fact(0, f32::fact([1, n_features]).into())
            .with_context(|| {
                format!(
                    "set ONNX input fact failed: path={} n_features={}",
                    path.display(),
                    n_features
                )
            })?
            .into_optimized()
            .with_context(|| format!("optimize ONNX model failed: {}", path.display()))?
            .into_runnable()
            .with_context(|| format!("build runnable ONNX model failed: {}", path.display()))?;

        Ok(Self { model, n_features })
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

        let marshal_start = Instant::now();
        let input = tract_ndarray::Array2::<f32>::from_shape_vec((1, self.n_features), features.to_vec())
            .context("build ONNX input tensor failed")?;
        let input_tensor: Tensor = input.into_tensor();
        let marshal_us = marshal_start.elapsed().as_micros() as u64;

        let predict_start = Instant::now();
        let outputs = self
            .model
            .run(tvec!(input_tensor.into()))
            .context("run ONNX model failed")?;
        let onnx_predict_us = predict_start.elapsed().as_micros() as u64;

        let first = outputs
            .get(0)
            .ok_or_else(|| anyhow::anyhow!("ONNX model output is empty"))?;
        let score = extract_first_scalar(first).context("extract ONNX score failed")?;
        if !score.is_finite() {
            anyhow::bail!("ONNX output is non-finite: {}", score);
        }

        Ok(PredictOneOutput {
            score,
            marshal_us,
            onnx_predict_us,
        })
    }
}

fn extract_first_scalar(value: &TValue) -> Result<f64> {
    if let Ok(view) = value.to_array_view::<f32>() {
        if let Some(v) = view.iter().next() {
            return Ok(*v as f64);
        }
    }
    if let Ok(view) = value.to_array_view::<f64>() {
        if let Some(v) = view.iter().next() {
            return Ok(*v);
        }
    }
    anyhow::bail!(
        "unsupported ONNX output tensor type: {:?}",
        value.datum_type()
    );
}
