//! Run ONNX inference in Rust from a CSV feature matrix.
//!
//! Usage:
//!   cargo run --release --bin onnx_csv_predict -- --config config/onnx_csv_predict.toml

use anyhow::{Context, Result};
use clap::Parser;
use log::info;
use mkt_model_runtime::OnnxModel;
use serde::Deserialize;
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

const DEFAULT_CONFIG_PATH: &str = "config/onnx_csv_predict.toml";

#[derive(Parser, Debug)]
#[command(name = "onnx_csv_predict")]
#[command(about = "Run ONNX inference on CSV features and export idx/onnx predictions")]
struct Args {
    #[arg(long, default_value = DEFAULT_CONFIG_PATH)]
    config: String,
}

#[derive(Debug, Deserialize)]
struct OnnxCsvPredictConfig {
    #[serde(default)]
    onnx_path: Option<String>,
    #[serde(default)]
    model_manager_base_url: Option<String>,
    #[serde(default)]
    model_onnx_path: Option<String>,
    #[serde(default)]
    model_name: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default = "default_model_manager_timeout_sec")]
    model_manager_timeout_sec: f64,
    input_csv: String,
    output_csv: String,
    #[serde(default = "default_has_header")]
    has_header: bool,
    #[serde(default = "default_delimiter")]
    delimiter: String,
    #[serde(default)]
    expected_feature_dim: Option<usize>,
    #[serde(default = "default_log_every")]
    log_every: usize,
}

#[derive(Debug, Clone, Copy)]
struct InferenceStats {
    rows: u64,
    feature_dim: usize,
    total_us: u64,
    marshal_total_us: u64,
    marshal_max_us: u64,
    onnx_total_us: u64,
    onnx_max_us: u64,
}

fn default_has_header() -> bool {
    true
}

fn default_delimiter() -> String {
    ",".to_string()
}

fn default_log_every() -> usize {
    50_000
}

fn default_model_manager_timeout_sec() -> f64 {
    15.0
}

impl OnnxCsvPredictConfig {
    fn load(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("read config failed: {}", path.display()))?;
        let cfg: Self = toml::from_str(&content)
            .with_context(|| format!("parse TOML failed: {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        if self.model_manager_timeout_sec <= 0.0 {
            anyhow::bail!("model_manager_timeout_sec must be > 0");
        }
        if self.input_csv.trim().is_empty() {
            anyhow::bail!("input_csv must not be empty");
        }
        if self.output_csv.trim().is_empty() {
            anyhow::bail!("output_csv must not be empty");
        }
        if self.delimiter.chars().count() != 1 {
            anyhow::bail!("delimiter must be exactly one character");
        }
        if matches!(self.expected_feature_dim, Some(0)) {
            anyhow::bail!("expected_feature_dim must be > 0 when provided");
        }

        let has_remote = self
            .model_manager_base_url
            .as_deref()
            .unwrap_or("")
            .trim()
            .len()
            > 0
            || self.model_name.as_deref().unwrap_or("").trim().len() > 0
            || self.symbol.as_deref().unwrap_or("").trim().len() > 0;
        if has_remote {
            if self
                .model_manager_base_url
                .as_deref()
                .unwrap_or("")
                .trim()
                .is_empty()
            {
                anyhow::bail!("model_manager_base_url must not be empty when using remote ONNX");
            }
            if self.model_name.as_deref().unwrap_or("").trim().is_empty() {
                anyhow::bail!("model_name must not be empty when using remote ONNX");
            }
            if self.symbol.as_deref().unwrap_or("").trim().is_empty() {
                anyhow::bail!("symbol must not be empty when using remote ONNX");
            }
        } else if self.onnx_path.as_deref().unwrap_or("").trim().is_empty() {
            anyhow::bail!("must provide either remote ONNX source or onnx_path");
        }
        Ok(())
    }

    fn delimiter_char(&self) -> char {
        self.delimiter.chars().next().unwrap_or(',')
    }

    fn is_remote_mode(&self) -> bool {
        self.model_manager_base_url
            .as_deref()
            .unwrap_or("")
            .trim()
            .len()
            > 0
    }

    fn render_model_onnx_url(&self) -> Result<String> {
        let base_url = self
            .model_manager_base_url
            .as_deref()
            .unwrap_or("")
            .trim_end_matches('/');
        let model_name = self.model_name.as_deref().unwrap_or("").trim();
        let symbol = self.symbol.as_deref().unwrap_or("").trim();
        let path_tpl = self
            .model_onnx_path
            .as_deref()
            .unwrap_or("/api/models/{model_name}/model_onnx/{symbol}")
            .trim();

        let path = path_tpl
            .replace("{model_name}", &urlencoding::encode(model_name))
            .replace("{symbol}", &urlencoding::encode(symbol));
        if path.is_empty() {
            anyhow::bail!("rendered model_onnx_path is empty");
        }
        if path.starts_with("http://") || path.starts_with("https://") {
            return Ok(path);
        }
        Ok(format!("{}/{}", base_url, path.trim_start_matches('/')))
    }
}

fn main() -> Result<()> {
    env_logger::init();
    set_onnx_env_fixed();

    let args = Args::parse();
    let cfg_path = PathBuf::from(args.config.trim());
    let cfg = OnnxCsvPredictConfig::load(&cfg_path)?;
    let stats = run_predict(&cfg)?;

    let avg_total_us = if stats.rows > 0 {
        stats.total_us / stats.rows
    } else {
        0
    };
    let avg_marshal_us = if stats.rows > 0 {
        stats.marshal_total_us / stats.rows
    } else {
        0
    };
    let avg_onnx_us = if stats.rows > 0 {
        stats.onnx_total_us / stats.rows
    } else {
        0
    };

    println!("config={}", cfg_path.display());
    println!("rows={}", stats.rows);
    println!("feature_dim={}", stats.feature_dim);
    println!("output_csv={}", cfg.output_csv);
    println!("avg_total_us={}", avg_total_us);
    println!("avg_marshal_us={}", avg_marshal_us);
    println!("avg_onnx_us={}", avg_onnx_us);
    println!("max_marshal_us={}", stats.marshal_max_us);
    println!("max_onnx_us={}", stats.onnx_max_us);
    Ok(())
}

fn run_predict(cfg: &OnnxCsvPredictConfig) -> Result<InferenceStats> {
    let onnx_local = load_onnx_local_path(cfg)?;
    let onnx_path = onnx_local.path.as_path();
    let input_csv = Path::new(cfg.input_csv.trim());
    if !input_csv.is_file() {
        anyhow::bail!("input csv not found: {}", input_csv.display());
    }
    let output_csv = Path::new(cfg.output_csv.trim());
    if let Some(parent) = output_csv.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create output dir failed: {}", parent.display()))?;
        }
    }

    let file = File::open(input_csv)
        .with_context(|| format!("open input csv failed: {}", input_csv.display()))?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines().enumerate();

    if cfg.has_header {
        let _ = lines.next();
    }

    let delim = cfg.delimiter_char();
    let (first_line_no, first_features) = loop {
        let Some((line_no, line_res)) = lines.next() else {
            anyhow::bail!("input csv has no data rows: {}", input_csv.display());
        };
        let line = line_res.with_context(|| {
            format!(
                "read input csv line failed: path={} line_no={}",
                input_csv.display(),
                line_no + 1
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let parsed = parse_feature_line(&line, delim, line_no + 1, cfg.expected_feature_dim)?;
        break (line_no + 1, parsed);
    };

    let feature_dim = first_features.len();
    let model = OnnxModel::load_from_path(onnx_path, Some(feature_dim)).with_context(|| {
        format!(
            "load ONNX model failed: path={} feature_dim={}",
            onnx_path.display(),
            feature_dim
        )
    })?;

    let out_file = File::create(output_csv)
        .with_context(|| format!("create output csv failed: {}", output_csv.display()))?;
    let mut out = BufWriter::new(out_file);
    writeln!(out, "idx,onnx").context("write output header failed")?;

    let started = Instant::now();
    let mut rows: u64 = 0;
    let mut marshal_total_us: u64 = 0;
    let mut marshal_max_us: u64 = 0;
    let mut onnx_total_us: u64 = 0;
    let mut onnx_max_us: u64 = 0;

    let mut run_one = |idx: u64, features: &[f32]| -> Result<()> {
        let output = model.predict_one_timed(features).with_context(|| {
            format!(
                "ONNX predict failed: idx={} feature_dim={} onnx_path={}",
                idx,
                features.len(),
                onnx_path.display()
            )
        })?;
        writeln!(out, "{},{}", idx, output.score).context("write output row failed")?;
        marshal_total_us = marshal_total_us.saturating_add(output.marshal_us);
        marshal_max_us = marshal_max_us.max(output.marshal_us);
        onnx_total_us = onnx_total_us.saturating_add(output.onnx_predict_us);
        onnx_max_us = onnx_max_us.max(output.onnx_predict_us);
        Ok(())
    };

    run_one(0, &first_features)?;
    rows += 1;
    if cfg.log_every > 0 && rows as usize % cfg.log_every == 0 {
        info!(
            "onnx_csv_predict progress: rows={} feature_dim={} source_line={}",
            rows, feature_dim, first_line_no
        );
    }

    for (line_no, line_res) in lines {
        let line = line_res.with_context(|| {
            format!(
                "read input csv line failed: path={} line_no={}",
                input_csv.display(),
                line_no + 1
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let features = parse_feature_line(&line, delim, line_no + 1, Some(feature_dim))?;
        run_one(rows, &features)?;
        rows += 1;

        if cfg.log_every > 0 && rows as usize % cfg.log_every == 0 {
            info!(
                "onnx_csv_predict progress: rows={} feature_dim={} source_line={}",
                rows,
                feature_dim,
                line_no + 1
            );
        }
    }
    out.flush().context("flush output csv failed")?;

    let total_us = started.elapsed().as_micros() as u64;
    info!(
        "onnx_csv_predict done: rows={} feature_dim={} total_us={} output={}",
        rows,
        feature_dim,
        total_us,
        output_csv.display()
    );

    if onnx_local.cleanup {
        let _ = fs::remove_file(&onnx_local.path);
    }

    Ok(InferenceStats {
        rows,
        feature_dim,
        total_us,
        marshal_total_us,
        marshal_max_us,
        onnx_total_us,
        onnx_max_us,
    })
}

struct OnnxLocalPath {
    path: PathBuf,
    cleanup: bool,
}

fn load_onnx_local_path(cfg: &OnnxCsvPredictConfig) -> Result<OnnxLocalPath> {
    if cfg.is_remote_mode() {
        let url = cfg.render_model_onnx_url()?;
        let timeout = std::time::Duration::from_secs_f64(cfg.model_manager_timeout_sec);
        let client = reqwest::blocking::Client::builder()
            .timeout(timeout)
            .build()
            .context("build reqwest client for remote ONNX failed")?;

        let resp = client
            .get(&url)
            .send()
            .with_context(|| format!("request remote ONNX failed: {}", url))?
            .error_for_status()
            .with_context(|| format!("remote ONNX returned error status: {}", url))?;
        let bytes = resp
            .bytes()
            .with_context(|| format!("read remote ONNX body failed: {}", url))?;
        if bytes.is_empty() {
            anyhow::bail!("remote ONNX response empty: {}", url);
        }

        let root = Path::new("/tmp/mkt_signal_onnx_csv_predict");
        fs::create_dir_all(root)
            .with_context(|| format!("create tmp dir failed: {}", root.display()))?;
        let model_name_safe = sanitize_token(cfg.model_name.as_deref().unwrap_or("model"));
        let symbol_safe = sanitize_token(cfg.symbol.as_deref().unwrap_or("symbol"));
        let file_name = format!(
            "{}.{}.{}.onnx",
            model_name_safe,
            symbol_safe,
            now_millis().max(0)
        );
        let path = root.join(file_name);
        fs::write(&path, &bytes)
            .with_context(|| format!("write downloaded ONNX failed: {}", path.display()))?;
        info!(
            "downloaded ONNX from model_manager: url={} bytes={} local_path={}",
            url,
            bytes.len(),
            path.display()
        );
        return Ok(OnnxLocalPath {
            path,
            cleanup: true,
        });
    }

    let onnx_path = PathBuf::from(cfg.onnx_path.as_deref().unwrap_or("").trim());
    if !onnx_path.is_file() {
        anyhow::bail!("onnx model not found: {}", onnx_path.display());
    }
    Ok(OnnxLocalPath {
        path: onnx_path,
        cleanup: false,
    })
}

fn sanitize_token(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "x".to_string()
    } else {
        out
    }
}

fn now_millis() -> i64 {
    let Ok(duration) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) else {
        return 0;
    };
    let ms = duration.as_millis();
    if ms > i64::MAX as u128 {
        i64::MAX
    } else {
        ms as i64
    }
}

fn parse_feature_line(
    line: &str,
    delimiter: char,
    line_no: usize,
    expected_feature_dim: Option<usize>,
) -> Result<Vec<f32>> {
    let cols: Vec<&str> = line.split(delimiter).collect();
    if cols.is_empty() {
        anyhow::bail!("empty feature row: line_no={}", line_no);
    }
    if let Some(dim) = expected_feature_dim {
        if cols.len() != dim {
            anyhow::bail!(
                "feature dim mismatch at line_no={}: got {} expect {}",
                line_no,
                cols.len(),
                dim
            );
        }
    }

    let mut out = Vec::with_capacity(cols.len());
    for (i, raw) in cols.iter().enumerate() {
        let parsed = raw.trim().parse::<f32>().with_context(|| {
            format!(
                "parse feature float failed: line_no={} col={} raw={:?}",
                line_no, i, raw
            )
        })?;
        out.push(parsed);
    }
    Ok(out)
}

fn set_onnx_env_fixed() {
    env::set_var("OMP_NUM_THREADS", "1");
    env::set_var("OMP_WAIT_POLICY", "PASSIVE");
    info!("onnx_csv_predict ONNX env fixed: OMP_NUM_THREADS=1 OMP_WAIT_POLICY=PASSIVE");
}
