//! Model Publisher 入口
//!
//! 使用方式: cargo run --bin model_pub -- --config config/model_pub.toml

use anyhow::{Context, Result};
use clap::Parser;
use log::info;

use mkt_signal::factor_pub::model_pub::app::ModelPubApp;
use mkt_signal::factor_pub::model_pub::cfg::ModelPubConfig;
use mkt_signal::factor_pub::model_pub::model::XgbModel;

#[derive(Parser)]
#[command(name = "model_pub")]
#[command(about = "Model Publisher - vector in, model inference, vector out")]
struct Args {
    /// Path to model pub TOML config
    #[arg(short, long, default_value = "config/model_pub.toml")]
    config: String,

    /// Run a one-shot local inference test and exit (no IPC subscribe/publish)
    #[arg(long, default_value_t = false)]
    test_mode: bool,

    /// Optional model path override for test mode
    #[arg(long)]
    test_model_path: Option<String>,

    /// Optional feature dimension override for test mode
    #[arg(long)]
    test_n_features: Option<usize>,

    /// Optional model name override for test mode (used in logs only)
    #[arg(long)]
    test_model_name: Option<String>,

    /// Optional comma-separated feature values for test mode.
    /// If omitted, uses all-zero vector with length n_features.
    #[arg(long)]
    test_features: Option<String>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    if args.test_mode {
        run_test_mode(&args)
    } else {
        info!("Loading config from: {}", args.config);
        let mut app = ModelPubApp::new(&args.config)?;
        app.run()
    }
}

fn run_test_mode(args: &Args) -> Result<()> {
    let cfg = ModelPubConfig::load(&args.config)
        .with_context(|| format!("load config failed: {}", args.config))?;

    let model_name = args
        .test_model_name
        .clone()
        .unwrap_or_else(|| cfg.model_name.clone());
    let model_path = args
        .test_model_path
        .clone()
        .unwrap_or_else(|| cfg.model_path.clone());
    let n_features = args.test_n_features.unwrap_or(cfg.n_features);

    if model_path.trim().is_empty() {
        anyhow::bail!("test_mode: model_path is empty");
    }
    if n_features == 0 {
        anyhow::bail!("test_mode: n_features must be > 0");
    }

    let features = build_test_features(args.test_features.as_deref(), n_features)?;
    let model = XgbModel::load(&model_path, n_features)?;
    let score = model.predict_one(&features)?;

    info!(
        "test_mode inference ok: model_name={} model_path={} n_features={} score={}",
        model_name, model_path, n_features, score
    );
    println!(
        "TEST_OK model_name={} n_features={} score={}",
        model_name, n_features, score
    );
    Ok(())
}

fn build_test_features(raw: Option<&str>, n_features: usize) -> Result<Vec<f32>> {
    let Some(raw) = raw else {
        return Ok(vec![0.0; n_features]);
    };

    let parsed: Vec<f32> = raw
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(|token| {
            token
                .parse::<f32>()
                .with_context(|| format!("invalid test feature value: '{}'", token))
        })
        .collect::<Result<Vec<_>>>()?;

    if parsed.len() != n_features {
        anyhow::bail!(
            "test_features dim mismatch: got {}, expect {}",
            parsed.len(),
            n_features
        );
    }
    Ok(parsed)
}
