use anyhow::{Context, Result};
use ort::session::{builder::GraphOptimizationLevel, Session};
use ort::value::{DynValue, Tensor};
use std::cell::RefCell;
use std::path::Path;
use std::time::Instant;

pub struct OnnxModel {
    session: RefCell<Session>,
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

        let session = Session::builder()
            .context("build ONNX Runtime session builder failed")?
            .with_parallel_execution(false)
            .context("set ONNX Runtime parallel execution mode failed")?
            .with_intra_threads(1)
            .context("set ONNX Runtime intra-op threads failed")?
            .with_inter_threads(1)
            .context("set ONNX Runtime inter-op threads failed")?
            .with_optimization_level(GraphOptimizationLevel::Level3)
            .context("set ONNX Runtime graph optimization level failed")?
            .commit_from_file(path)
            .with_context(|| {
                format!(
                    "load ONNX model with ONNX Runtime failed: {}",
                    path.display()
                )
            })?;

        if session.inputs().is_empty() {
            anyhow::bail!("ONNX model has no inputs: path={}", path.display());
        }
        if session.outputs().is_empty() {
            anyhow::bail!("ONNX model has no outputs: path={}", path.display());
        }

        Ok(Self {
            session: RefCell::new(session),
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

        let marshal_start = Instant::now();
        let input = Tensor::<f32>::from_array(([1usize, self.n_features], features.to_vec()))
            .context("build ONNX Runtime input tensor failed")?;
        let marshal_us = marshal_start.elapsed().as_micros() as u64;

        let mut session = self.session.borrow_mut();
        let predict_start = Instant::now();
        let outputs = session
            .run(ort::inputs![input])
            .context("run ONNX Runtime model failed")?;
        let onnx_predict_us = predict_start.elapsed().as_micros() as u64;
        if outputs.len() == 0 {
            anyhow::bail!("ONNX model output is empty");
        }
        let score = extract_first_scalar(&outputs[0]).context("extract ONNX score failed")?;
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

fn extract_first_scalar(value: &DynValue) -> Result<f64> {
    if let Ok((_shape, view)) = value.try_extract_tensor::<f32>() {
        if let Some(v) = view.first() {
            return Ok(*v as f64);
        }
    }
    if let Ok((_shape, view)) = value.try_extract_tensor::<f64>() {
        if let Some(v) = view.first() {
            return Ok(*v);
        }
    }
    anyhow::bail!("unsupported ONNX output tensor type: only f32/f64 tensor outputs are supported");
}

#[cfg(test)]
mod tests {
    use super::OnnxModel;
    use anyhow::{Context, Result};
    use ort::session::Session;
    use std::env;
    use std::fs;
    use std::path::{Path, PathBuf};

    const DEMO_PATH_ENV: &str = "MODEL_PUB_DEMO_ONNX_PATH";
    const DEMO_FEATURE_DIM_ENV: &str = "MODEL_PUB_DEMO_FEATURE_DIM";
    const DEFAULT_CACHE_DIR: &str = "/tmp/mkt_signal_model_pub_onnx";

    #[test]
    fn demo_ort_predict_from_local_onnx() -> Result<()> {
        let Some(model_path) = resolve_demo_model_path() else {
            eprintln!(
                "skip demo_ort_predict_from_local_onnx: set {} or place .onnx under {}",
                DEMO_PATH_ENV, DEFAULT_CACHE_DIR
            );
            return Ok(());
        };

        let feature_dim = resolve_feature_dim(&model_path)?;
        let model = OnnxModel::load_from_path(&model_path, Some(feature_dim))
            .with_context(|| format!("load demo ONNX failed: {}", model_path.display()))?;

        let features = vec![0.0f32; feature_dim];
        let output = model.predict_one_timed(&features).with_context(|| {
            format!(
                "run demo ONNX predict failed: path={} feature_dim={}",
                model_path.display(),
                feature_dim
            )
        })?;

        assert!(
            output.score.is_finite(),
            "demo predict score must be finite: {}",
            output.score
        );
        eprintln!(
            "demo_ort_predict_from_local_onnx ok: path={} feature_dim={} score={:.8} marshal_us={} onnx_predict_us={}",
            model_path.display(),
            feature_dim,
            output.score,
            output.marshal_us,
            output.onnx_predict_us
        );
        Ok(())
    }

    fn resolve_demo_model_path() -> Option<PathBuf> {
        if let Some(p) = env::var_os(DEMO_PATH_ENV) {
            let path = PathBuf::from(p);
            if path.is_file() {
                return Some(path);
            }
            eprintln!(
                "{} is set but file not found: {}",
                DEMO_PATH_ENV,
                path.display()
            );
            return None;
        }
        find_first_onnx(Path::new(DEFAULT_CACHE_DIR))
    }

    fn resolve_feature_dim(model_path: &Path) -> Result<usize> {
        if let Ok(raw) = env::var(DEMO_FEATURE_DIM_ENV) {
            let parsed = raw
                .trim()
                .parse::<usize>()
                .with_context(|| format!("parse {} failed: {}", DEMO_FEATURE_DIM_ENV, raw))?;
            if parsed == 0 {
                anyhow::bail!("{} must be > 0", DEMO_FEATURE_DIM_ENV);
            }
            return Ok(parsed);
        }
        infer_feature_dim_from_model_input(model_path)
    }

    fn infer_feature_dim_from_model_input(model_path: &Path) -> Result<usize> {
        let session = Session::builder()
            .context("build ORT session builder failed when inferring feature_dim")?
            .commit_from_file(model_path)
            .with_context(|| {
                format!(
                    "load ONNX failed when inferring feature_dim: {}",
                    model_path.display()
                )
            })?;
        let input = session.inputs().first().ok_or_else(|| {
            anyhow::anyhow!(
                "ONNX has no inputs when inferring feature_dim: {}",
                model_path.display()
            )
        })?;
        let shape = input.dtype().tensor_shape().ok_or_else(|| {
            anyhow::anyhow!(
                "first input is not tensor when inferring feature_dim: {}",
                model_path.display()
            )
        })?;

        let candidate = shape.iter().rev().find(|&&d| d > 0).copied().unwrap_or(-1);
        if candidate <= 0 {
            anyhow::bail!(
                "cannot infer positive feature_dim from input shape {:?}: path={} (set {} manually)",
                shape,
                model_path.display(),
                DEMO_FEATURE_DIM_ENV
            );
        }

        Ok(candidate as usize)
    }

    fn find_first_onnx(root: &Path) -> Option<PathBuf> {
        if !root.is_dir() {
            return None;
        }

        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let entries = fs::read_dir(&dir).ok()?;
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path
                    .extension()
                    .and_then(|s| s.to_str())
                    .map(|ext| ext.eq_ignore_ascii_case("onnx"))
                    .unwrap_or(false)
                {
                    return Some(path);
                }
            }
        }
        None
    }
}
