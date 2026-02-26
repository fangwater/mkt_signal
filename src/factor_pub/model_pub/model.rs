use anyhow::{Context, Result};
use libloading::{Library, Symbol};
use std::cell::RefCell;
use std::path::Path;
use std::time::Instant;

#[repr(C)]
#[derive(Clone, Copy)]
union Entry {
    missing: i32,
    fvalue: f32,
}

type PredictFn = unsafe extern "C" fn(*mut Entry, i32) -> f32;
type GetNumFeatureFn = unsafe extern "C" fn() -> usize;

pub struct Tl2cgenModel {
    _library: Library,
    predict_fn: PredictFn,
    n_features: usize,
    input_buffer: RefCell<Vec<Entry>>,
}

pub struct PredictOneOutput {
    pub score: f64,
    pub dmatrix_us: u64,
    pub tl2cgen_predict_us: u64,
}

impl Tl2cgenModel {
    pub fn load_from_path(path: &Path, expected_features: Option<usize>) -> Result<Self> {
        let library = unsafe { Library::new(path) }
            .with_context(|| format!("load tl2cgen shared library failed: {}", path.display()))?;

        let predict_fn: PredictFn = unsafe {
            let symbol: Symbol<PredictFn> = library.get(b"predict").with_context(|| {
                format!(
                    "resolve symbol 'predict' failed from shared library: {}",
                    path.display()
                )
            })?;
            *symbol
        };

        let get_num_feature_fn: GetNumFeatureFn = unsafe {
            let symbol: Symbol<GetNumFeatureFn> =
                library.get(b"get_num_feature").with_context(|| {
                    format!(
                        "resolve symbol 'get_num_feature' failed from shared library: {}",
                        path.display()
                    )
                })?;
            *symbol
        };

        let n_features = unsafe { get_num_feature_fn() };
        if n_features == 0 {
            anyhow::bail!(
                "invalid tl2cgen model feature dim=0 from shared library: {}",
                path.display()
            );
        }
        if let Some(expected) = expected_features {
            if expected != n_features {
                anyhow::bail!(
                    "feature dim mismatch from tl2cgen shared library: expected={} got={} path={}",
                    expected,
                    n_features,
                    path.display()
                );
            }
        }

        let input_buffer = vec![Entry { missing: -1 }; n_features];
        Ok(Self {
            _library: library,
            predict_fn,
            n_features,
            input_buffer: RefCell::new(input_buffer),
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
        let mut buffer = self.input_buffer.borrow_mut();
        for (i, &value) in features.iter().enumerate() {
            buffer[i] = Entry { fvalue: value };
        }
        let marshal_us = marshal_start.elapsed().as_micros() as u64;

        let predict_start = Instant::now();
        let score = unsafe { (self.predict_fn)(buffer.as_mut_ptr(), 0) };
        let predict_us = predict_start.elapsed().as_micros() as u64;

        Ok(PredictOneOutput {
            score: score as f64,
            dmatrix_us: marshal_us,
            tl2cgen_predict_us: predict_us,
        })
    }
}
